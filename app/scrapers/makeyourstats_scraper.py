import asyncio
import sys
import time
import unicodedata
from typing import TypedDict
from urllib.parse import urljoin

from app.core.config import settings

MAKEYOURSTATS_LEAGUES_URL = "https://makeyourstats.com/es/leagues"
MAKEYOURSTATS_ORIGIN = "https://makeyourstats.com"

_LOG_PREFIX = "[makeyourstats]"


def _log(msg: str) -> None:
    print(f"{_LOG_PREFIX} {msg}", flush=True)


def _is_leagues_search_placeholder(placeholder: str | None) -> bool:
    """Misma intención que «Buscar país o liga»; tolera Unicode raro en el DOM."""
    if not placeholder:
        return False
    n = unicodedata.normalize("NFKC", placeholder).casefold()
    return "buscar" in n and "liga" in n


class _CompetitionTarget(TypedDict):
    """Objetivo alineado con la tabla `competitions` (id + nombre) y la UI de MakeYourStats."""

    competition_id: int
    name: str
    search: str
    pick_option_text: str | None
    inferred_country: str | None
    href_path_contains: str


# Rutas únicas en makeyourstats.com (evita colisiones tipo otra "Premier League").
MAKEYOURSTATS_COMPETITION_TARGETS: tuple[_CompetitionTarget, ...] = (
    {
        "competition_id": 1,
        "name": "Premier League",
        "search": "inglaterra",
        "pick_option_text": None,
        "inferred_country": "Inglaterra",
        "href_path_contains": "england/premier-league/8",
    },
    {
        "competition_id": 2,
        "name": "La Liga",
        "search": "españa",
        "pick_option_text": None,
        "inferred_country": "España",
        "href_path_contains": "spain/la-liga/564",
    },
    {
        "competition_id": 3,
        "name": "Serie A",
        "search": "italia",
        "pick_option_text": None,
        "inferred_country": "Italia",
        "href_path_contains": "italy/serie-a/384",
    },
    {
        "competition_id": 4,
        "name": "Ligue 1",
        "search": "francia",
        "pick_option_text": None,
        "inferred_country": "Francia",
        "href_path_contains": "france/ligue-1/301",
    },
    {
        "competition_id": 5,
        "name": "Bundesliga",
        "search": "alemania",
        "pick_option_text": None,
        "inferred_country": "Alemania",
        "href_path_contains": "germany/bundesliga/82",
    },
    {
        "competition_id": 6,
        "name": "UEFA Champions League",
        "search": "europa",
        "pick_option_text": "Europa",
        "inferred_country": None,
        "href_path_contains": "europe/champions-league/2",
    },
    {
        "competition_id": 7,
        "name": "UEFA Europa League",
        "search": "europa",
        "pick_option_text": "Europa",
        "inferred_country": None,
        "href_path_contains": "europe/europa-league/5",
    },
)


class CompetitionLinkResult(TypedDict):
    competition_id: int
    name: str
    search: str
    ok: bool
    url: str | None
    link_text: str | None
    navigated_url: str | None
    error: str | None


class MakeYourStatsFlowResult(TypedDict):
    """Salida del único flujo sync (página + opcional lista de competiciones)."""

    url: str
    document_title: str | None
    items: list[CompetitionLinkResult]


def _run_makeyourstats_flow_sync(
    include_competition_links: bool,
) -> MakeYourStatsFlowResult:
    """
    Un solo scraper / una sola sesión de navegador:

    1. Abrir Chromium
    2. Ir a `/es/leagues`
    3. Esperar marca de página lista
    4. Si `include_competition_links`: por cada competición, recargar página, cookies,
       autocompletado, leer enlace

    Si `include_competition_links` es False, termina tras el paso 3 (comprobación de página).
    """
    from playwright.sync_api import sync_playwright

    headless = settings.playwright_headless
    wait_s = settings.playwright_after_load_wait_seconds
    timeout_ms = settings.playwright_page_ready_timeout_ms
    items: list[CompetitionLinkResult] = []

    mode = "página + competiciones" if include_competition_links else "solo página (smoke)"
    _log(
        f"=== MakeYourStats — flujo único ({mode}) — "
        f"headless={headless}, timeout_ms={timeout_ms}, espera_tras_marca={wait_s}s ===",
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            # Viewport estable (sticky + coords click).
            try:
                page.set_viewport_size({"width": 1280, "height": 900})
            except Exception:
                pass
            page.set_default_timeout(timeout_ms)
            _log(f"1) Navegando a {MAKEYOURSTATS_LEAGUES_URL}")
            page.goto(MAKEYOURSTATS_LEAGUES_URL, wait_until="domcontentloaded")
            _log("2) Esperando UI de ligas (buscador)…")
            _wait_leagues_ui_ready(page, timeout_ms, wait_s)
            url = page.url
            title = page.title()
            _log(f"3) Página lista: url={url!r} title={title!r}")

            if not include_competition_links:
                _log("Fin del flujo (sin fase competiciones).")
                return {
                    "url": url,
                    "document_title": title,
                    "items": [],
                }

            _log(
                f"5) Fase competiciones ({len(MAKEYOURSTATS_COMPETITION_TARGETS)} "
                "objetivos, mismo navegador)…",
            )

            for target in MAKEYOURSTATS_COMPETITION_TARGETS:
                base: CompetitionLinkResult = {
                    "competition_id": target["competition_id"],
                    "name": target["name"],
                    "search": target["search"],
                    "ok": False,
                    "url": None,
                    "link_text": None,
                    "navigated_url": None,
                    "error": None,
                }
                try:
                    _log(
                        f"--- [{target['competition_id']}] {target['name']} — "
                        f"buscar={target['search']!r}, "
                        f"href_fragment={target['href_path_contains']!r}",
                    )
                    _log("5a) Recargando página de ligas…")
                    page.goto(
                        MAKEYOURSTATS_LEAGUES_URL,
                        wait_until="domcontentloaded",
                    )
                    _log(
                        "5b) Esperando UI lista (buscador) y post-espera configurada…",
                    )
                    _wait_leagues_ui_ready(page, timeout_ms, wait_s)
                    _dismiss_common_overlays(page, timeout_ms)

                    combo, inp = _resolve_leagues_autocomplete(page, timeout_ms)
                    _type_into_leagues_combobox(page, inp, target["search"])
                    _log("5c) Debounce autocompletado (1.2s)…")
                    page.wait_for_timeout(1_200)

                    listbox = combo.locator(
                        'ul.autocomplete-result-list[role="listbox"], '
                        "ul.autocomplete-result-list",
                    ).first
                    # Paso 1: click en el primer resultado que aparezca (p. ej. "Inglaterra").
                    pick_text = target.get("pick_option_text")
                    if pick_text:
                        _log(f"5d) Esperando opción del listbox por texto {pick_text!r} y click…")
                        try:
                            option = _pick_listbox_option_exact(listbox, pick_text, timeout_ms)
                        except Exception as exc:
                            try:
                                snap = listbox.inner_text(timeout=2_000)
                                _log(f"DEBUG listbox.inner_text={snap!r}")
                            except Exception:
                                pass
                            raise exc
                    else:
                        _log("5d) Esperando primer resultado del listbox y click…")
                        option = listbox.locator("a[href], li[role='option'], li a[href]").first
                        option.wait_for(state="attached", timeout=timeout_ms)

                    try:
                        option.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        txt = option.inner_text().strip()
                    except Exception:
                        txt = ""
                    _log(f"5e) Click opción (texto={txt!r})…")
                    option.click(force=True, timeout=min(15_000, timeout_ms))
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(700)

                    # Paso 2: ya dentro (país o búsqueda), click en la liga objetivo por fragmento único.
                    frag = target["href_path_contains"]
                    _log(f"5f) Buscando liga objetivo en la página (href contiene {frag!r})…")
                    league_link = page.locator(f'a[href*="{frag}"]').first
                    league_link.wait_for(state="attached", timeout=timeout_ms)
                    try:
                        league_link.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    raw_href = league_link.get_attribute("href")
                    if not raw_href:
                        raise RuntimeError("El enlace de liga objetivo no tiene href.")
                    base["url"] = urljoin(MAKEYOURSTATS_ORIGIN, raw_href)
                    try:
                        base["link_text"] = league_link.inner_text().strip() or None
                    except Exception:
                        base["link_text"] = None
                    _log(f"5g) Click en liga objetivo (url={base['url']!r})…")
                    league_link.click(force=True, timeout=min(15_000, timeout_ms))
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(700)
                    base["navigated_url"] = page.url
                    teams = _log_league_table_team_names(page, timeout_ms)

                    inferred_country = target.get("inferred_country")
                    for rank, team_name, href in teams:
                        # Si ya existe por nombre, no hacemos nada.
                        try:
                            # Para ligas domésticas: país inferido.
                            if inferred_country:
                                _ensure_team_in_db(team_name, inferred_country)
                                continue
                            # Champions/Europa: sacar país desde ficha de equipo si no existe.
                            # Para no spamear pestañas: navegar, extraer, volver.
                            from app.db.supabase_client import get_supabase_client

                            client = get_supabase_client()
                            exists = (
                                client.table("teams")
                                .select("name")
                                .eq("name", team_name)
                                .limit(1)
                                .execute()
                                .data
                                or []
                            )
                            if exists:
                                _log(f"DB: existe team={team_name!r}")
                                continue
                            if not href:
                                _log(f"DB: no href para team={team_name!r} (rank {rank})")
                                continue
                            team_url = urljoin(MAKEYOURSTATS_ORIGIN, href)
                            _log(f"TEAM {rank}: abriendo ficha para país → {team_url!r}")
                            page.goto(team_url, wait_until="domcontentloaded")
                            page.wait_for_timeout(600)
                            country = _extract_team_country_from_team_page(page, timeout_ms)
                            _ensure_team_in_db(team_name, country)
                            # volver a liga (standings) para el siguiente
                            page.goto(base["navigated_url"] or "", wait_until="domcontentloaded")
                            page.wait_for_timeout(400)
                        except Exception as exc:
                            _log(f"DB: error team={team_name!r} rank={rank}: {exc}")
                    base["ok"] = True
                    _log(
                        f"OK → liga={base['url']!r} navegó_a={base['navigated_url']!r} texto={base['link_text']!r}",
                    )

                    _log("5h) Volviendo a /es/leagues para el siguiente objetivo…")
                    page.goto(MAKEYOURSTATS_LEAGUES_URL, wait_until="domcontentloaded")
                except Exception as exc:
                    base["error"] = str(exc)
                    _log(f"ERROR: {exc}")
                items.append(base)

            _log(f"6) Fase competiciones terminada ({len(items)} filas).")
            return {
                "url": url,
                "document_title": title,
                "items": items,
            }
        finally:
            _log("Cerrando navegador (fin del flujo).")
            browser.close()


def _open_makeyourstats_leagues_sync() -> dict[str, str | None]:
    r = _run_makeyourstats_flow_sync(include_competition_links=False)
    return {"url": r["url"], "document_title": r["document_title"]}


async def open_makeyourstats_leagues() -> dict[str, str | None]:
    return await asyncio.to_thread(_open_makeyourstats_leagues_sync)


def _wait_page_ready(page, timeout_ms: int, wait_s: float) -> None:
    page.locator("text=MakeYourStats").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    time.sleep(wait_s)


def _wait_leagues_ui_ready(page, timeout_ms: int, wait_s: float) -> None:
    """
    Señal "lista" real para este scraper: que exista el buscador (sticky-top/autocomplete).
    El texto "MakeYourStats" puede no estar visible o puede cambiar.
    """
    ready_timeout = min(30_000, timeout_ms)
    _log(f"Esperando UI lista (buscador) hasta {ready_timeout}ms…")
    page.wait_for_function(
        """() => {
          return Boolean(
            document.querySelector('.sticky-top input.autocomplete-input')
              || document.querySelector('input.autocomplete-input[aria-label]')
              || document.querySelector('input.autocomplete-input')
          );
        }""",
        timeout=ready_timeout,
    )
    time.sleep(wait_s)


def _dismiss_common_overlays(page, timeout_ms: int) -> None:
    """Cierra banners de cookies o modales que bloqueen el foco en el input."""
    candidates = (
        "Aceptar todas las cookies",
        "Aceptar todas",
        "Aceptar y continuar",
        "Accept all",
        "Aceptar",
    )
    _log("Buscando banner de cookies / overlay para cerrar…")
    for name in candidates:
        btn = page.get_by_role("button", name=name)
        try:
            if btn.count() == 0:
                continue
            first = btn.first
            if first.is_visible():
                _log(f"Pulsando botón: {name!r}")
                first.click(timeout=min(5_000, timeout_ms))
                page.wait_for_timeout(400)
                break
        except Exception:
            continue
    else:
        _log("No se pulsó ningún botón de cookies (puede que no hubiera banner).")


def _resolve_leagues_autocomplete(
    page,
    timeout_ms: int,
) -> tuple[object, object]:
    """
    Localiza div.autocomplete > input.autocomplete-input (p. ej. dentro de .sticky-top).
    El buscador puede montarse *después* del texto «MakeYourStats»; no exigimos el
    placeholder exacto en CSS (puede variar el Unicode de «país»).
    """
    _log("Esperando load…")
    page.wait_for_load_state("load")
    _log("Esperando a que exista al menos un input.autocomplete-input (Nuxt/Vue)…")
    page.wait_for_function(
        "() => document.querySelectorAll('input.autocomplete-input').length > 0",
        timeout=timeout_ms,
    )
    # El de sticky-top a veces aparece un tick después del primer paint.
    _log("Pausa extra por hidratación / sticky-top (1.5s)…")
    page.wait_for_timeout(1_500)

    # Tu DOM: div.sticky-top > … > div.autocomplete > input.autocomplete-input
    sticky_inputs = page.locator(".sticky-top input.autocomplete-input")
    n_sticky = sticky_inputs.count()
    if n_sticky > 0:
        _log(f"Candidatos en .sticky-top: {n_sticky} (prioridad: buscador de ligas)")
        for j in range(n_sticky):
            cand = sticky_inputs.nth(j)
            ph = cand.get_attribute("placeholder")
            if not _is_leagues_search_placeholder(ph):
                continue
            combo = cand.locator("xpath=ancestor::div[contains(@class,'autocomplete')][1]")
            cand.wait_for(state="attached", timeout=5_000)
            _log(
                "Autocompletado: usando .sticky-top "
                f"[índice {j}] placeholder={ph!r}",
            )
            return combo, cand
        cand0 = sticky_inputs.first
        ph0 = cand0.get_attribute("placeholder")
        combo0 = cand0.locator("xpath=ancestor::div[contains(@class,'autocomplete')][1]")
        cand0.wait_for(state="attached", timeout=5_000)
        _log(
            "Autocompletado: .sticky-top — ningún placeholder filtró; "
            f"usando el primer input (placeholder={ph0!r})",
        )
        return combo0, cand0

    raw = page.locator("input.autocomplete-input")
    raw.first.wait_for(state="attached", timeout=min(30_000, timeout_ms))
    n_inp = raw.count()
    _log(f"Total input.autocomplete-input en el DOM: {n_inp}")

    best_combo = None
    best_inp = None
    best_area = -1.0
    for i in range(n_inp):
        inp = raw.nth(i)
        ph = inp.get_attribute("placeholder")
        if not _is_leagues_search_placeholder(ph):
            continue
        combo = inp.locator("xpath=ancestor::div[contains(@class,'autocomplete')][1]")
        try:
            inp.wait_for(state="attached", timeout=5_000)
        except Exception:
            continue
        _log(f"Candidato[{i}] placeholder={ph!r} → midiendo tras scroll…")
        try:
            inp.scroll_into_view_if_needed()
        except Exception:
            pass
        page.wait_for_timeout(200)
        # No exigimos state=visible: en sticky/layout a veces Playwright marca no visible
        # aunque el nodo sea el correcto (p. ej. transiciones).
        bbox = inp.bounding_box()
        area = 0.0
        if bbox and bbox.get("width") and bbox.get("height"):
            area = float(bbox["width"]) * float(bbox["height"])
        if area <= 0:
            _log(f"Candidato[{i}]: bounding_box vacío o 0 — igual entra al ranking (área 0)")
        if area > best_area:
            best_area = area
            best_combo = combo
            best_inp = inp

    if best_combo is None or best_inp is None:
        seen: list[str] = []
        for i in range(n_inp):
            p = raw.nth(i).get_attribute("placeholder")
            seen.append(f"[{i}]={p!r}")
        _log("Placeholders vistos: " + "; ".join(seen) if seen else "(ninguno)")
        raise RuntimeError(
            "No hubo input.autocomplete-input con placeholder de tipo "
            "«Buscar … liga». Revisa los placeholders logueados arriba."
        )
    _log(
        "Autocompletado elegido (mayor área si se pudo medir; si no, primer match): "
        f"{best_area:.0f}px²",
    )
    return best_combo, best_inp


def _set_combobox_value_native_setter(inp, text: str) -> None:
    """Vue a veces redefine value; usar el setter del prototipo HTMLInputElement."""
    inp.evaluate(
        """(el, v) => {
            el.focus();
            const d = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype,
                'value',
            );
            if (d && d.set) {
                d.set.call(el, v);
            } else {
                el.value = v;
            }
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(
                new InputEvent('input', { bubbles: true, inputType: 'insertText', data: v }),
            );
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        text,
    )


def _type_into_leagues_combobox(page, inp, text: str) -> None:
    """
    Nuxt/Vue: clic en coordenadas, fill(force), setter nativo, teclado al final.
    """
    _log(f"Escribiendo en el combobox: {text!r}")
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
    except Exception:
        pass

    inp.scroll_into_view_if_needed()
    page.wait_for_timeout(200)

    bb = inp.bounding_box()
    if bb and float(bb.get("width", 0)) > 2 and float(bb.get("height", 0)) > 2:
        cx = float(bb["x"]) + float(bb["width"]) / 2
        cy = float(bb["y"]) + float(bb["height"]) / 2
        try:
            info = page.evaluate(
                """([x, y]) => {
                  const el = document.elementFromPoint(x, y);
                  const ae = document.activeElement;
                  return {
                    elementFromPoint: el ? { tag: el.tagName, cls: el.className, id: el.id } : null,
                    activeElement: ae ? { tag: ae.tagName, cls: ae.className, id: ae.id } : null,
                  };
                }""",
                [cx, cy],
            )
            _log(f"elementFromPoint@center={info.get('elementFromPoint')}, active={info.get('activeElement')}")
        except Exception:
            pass
        page.mouse.click(cx, cy)
        _log(f"mouse.click en centro del input ({cx:.0f}, {cy:.0f})")
    else:
        inp.click(force=True)
        _log("Sin bbox útil: inp.click(force=True)")

    page.wait_for_timeout(120)
    try:
        editable = inp.is_editable()
        enabled = inp.is_enabled()
        visible = inp.is_visible()
        ph = inp.get_attribute("placeholder")
        _log(f"input state: visible={visible} enabled={enabled} editable={editable} placeholder={ph!r}")
    except Exception:
        pass

    try:
        inp.fill(text, force=True, timeout=15_000)
        _log("fill(force=True) ejecutado")
    except Exception as exc:
        _log(f"fill(force) falló: {exc}")

    page.wait_for_timeout(80)
    got = ""
    try:
        got = inp.input_value(timeout=2_000)
    except Exception:
        pass
    try:
        dom_value = inp.evaluate("(el) => el.value")
        _log(f"tras fill: input_value()={got!r} dom.value={dom_value!r}")
    except Exception:
        _log(f"tras fill: input_value()={got!r}")
    if got == text:
        _log(f"Valor OK tras fill(): {got!r}")
        return

    _log(f"Tras fill, input_value={got!r} — setter nativo HTMLInputElement…")
    _set_combobox_value_native_setter(inp, text)
    page.wait_for_timeout(120)
    try:
        got = inp.input_value(timeout=2_000)
    except Exception:
        got = ""
    try:
        dom_value = inp.evaluate("(el) => el.value")
        _log(f"tras setter: input_value()={got!r} dom.value={dom_value!r}")
    except Exception:
        _log(f"tras setter: input_value()={got!r}")
    if got == text:
        _log(f"Valor OK tras setter nativo: {got!r}")
        return

    _log(f"Tras setter, input_value={got!r} — keyboard.type…")
    mod = "Meta" if sys.platform == "darwin" else "Control"
    bb2 = inp.bounding_box()
    if bb2 and float(bb2.get("width", 0)) > 2:
        page.mouse.click(
            float(bb2["x"]) + float(bb2["width"]) / 2,
            float(bb2["y"]) + float(bb2["height"]) / 2,
        )
    else:
        inp.click(force=True)
    page.wait_for_timeout(80)
    page.keyboard.press(f"{mod}+A")
    page.keyboard.press("Backspace")
    page.keyboard.type(text, delay=35)
    try:
        got = inp.input_value(timeout=2_000)
    except Exception:
        got = ""
    try:
        dom_value = inp.evaluate("(el) => el.value")
        _log(f"tras teclado: input_value()={got!r} dom.value={dom_value!r}")
    except Exception:
        _log(f"tras teclado: input_value()={got!r}")
    if got == text:
        _log(f"Valor OK tras teclado: {got!r}")
    else:
        _log(f"Aviso: valor final en input es {got!r} (esperado {text!r})")


def _pick_listbox_option_exact(listbox, pick_text: str, timeout_ms: int):
    """
    Selecciona opción por TEXTO EXACTO (trim + casefold).
    Evita falsos positivos tipo "Bélgica: UEFA Europa League…" cuando pick_text="Europa".
    """
    candidates = listbox.locator(
        "a[href], li[role='option'], li, button, [role='option'], div",
    )
    candidates.first.wait_for(state="attached", timeout=timeout_ms)
    n = candidates.count()
    want = pick_text.strip().casefold()
    for i in range(n):
        cand = candidates.nth(i)
        try:
            t = (cand.inner_text() or "").strip().casefold()
        except Exception:
            continue
        if t == want:
            return cand
    raise RuntimeError(f"No se encontró opción exacta en listbox: {pick_text!r}")


def _log_league_table_team_names(page, timeout_ms: int) -> list[tuple[int, str, str]]:
    """
    Loguea nombres de equipos en la tabla de posiciones:
    anchors tipo /es/football/team/<slug>/<id>.
    """
    _log("5g.1) Leyendo tabla de equipos (standings)…")
    page.locator('a[href^="/es/football/team/"]').first.wait_for(
        state="attached",
        timeout=timeout_ms,
    )

    # Preferimos filas que tengan "badge" (posición) + nombre.
    team_rows = page.locator('a[href^="/es/football/team/"]').filter(
        has=page.locator("span.badge"),
    )
    n = team_rows.count()
    rows: list[tuple[int, str]] = []
    for i in range(n):
        a = team_rows.nth(i)
        try:
            badge = (a.locator("span.badge").first.inner_text() or "").strip()
            rank = int("".join(ch for ch in badge if ch.isdigit()) or "0")
        except Exception:
            rank = 0
        try:
            name_loc = a.locator("p.w-30").first
            if name_loc.count() > 0:
                name = (name_loc.inner_text() or "").strip()
            else:
                # fallback: alt del escudo o texto plano
                name = (a.get_attribute("aria-label") or "").strip() or (a.inner_text() or "").strip()
        except Exception:
            continue
        if not name or rank <= 0:
            continue
        rows.append((rank, name))

    # De-dup por rank (por si hay duplicados en DOM).
    by_rank: dict[int, str] = {}
    for r, nm in rows:
        by_rank.setdefault(r, nm)

    ordered = [(r, by_rank[r]) for r in sorted(by_rank)]
    _log(f"5g.2) Equipos rankeados encontrados: {len(ordered)}")
    # Log en orden 1..N
    for r, nm in ordered:
        _log(f"TEAM {r}: {nm}")

    # Validación: idealmente 20 equipos (o el tamaño de la liga).
    if ordered:
        missing = [r for r in range(1, max(r for r, _ in ordered) + 1) if r not in by_rank]
        if missing:
            _log(f"5g.3) Aviso: faltan posiciones en la tabla: {missing}")

    # Devolver (rank, name, href) en orden de rank
    out: list[tuple[int, str, str]] = []
    for r, nm in ordered:
        # Buscar el anchor que tenga ese rank (badge) y nombre
        a = team_rows.filter(
            has=page.locator("span.badge", has_text=str(r)),
        ).first
        href = a.get_attribute("href") or ""
        out.append((r, nm, href))
    return out


def _extract_team_country_from_team_page(page, timeout_ms: int) -> str | None:
    """
    En la vista de equipo hay un <p> con un <img ... title/alt='Turquía'> y texto del país.
    Devolvemos el texto visible (fallback a title/alt).
    """
    # Esperar a que el detalle tenga al menos un img de país (sportmonks).
    page.locator("img[src*='cdn.sportmonks.com/images/countries/']").first.wait_for(
        state="attached",
        timeout=min(20_000, timeout_ms),
    )
    # Tomar el primer bloque p que contenga ese img.
    p = page.locator("p", has=page.locator("img[src*='cdn.sportmonks.com/images/countries/']")).first
    try:
        txt = (p.inner_text() or "").strip()
        # Normalmente queda "Turquía" o "🇹🇷 Turquía" → limpiamos espacios.
        if txt:
            return " ".join(txt.split())
    except Exception:
        pass
    img = p.locator("img").first
    for attr in ("title", "alt"):
        try:
            v = (img.get_attribute(attr) or "").strip()
            if v:
                return v
        except Exception:
            continue
    return None


def _ensure_team_in_db(
    name: str,
    country: str | None,
) -> None:
    """
    Valida por nombre si existe en `teams`. Si no existe, inserta {name, country}.
    """
    from app.core.config import settings
    from app.db.supabase_client import get_supabase_client, get_supabase_service_client

    # Lectura: cliente normal.
    client = get_supabase_client()
    # OJO: asumimos columna `name` y `country` como dijo el user.
    res = client.table("teams").select("name").eq("name", name).limit(1).execute()
    if (res.data or []):
        _log(f"DB: existe team={name!r}")
        return
    payload = {"name": name, "country": country}
    # Escritura: si hay service role, usarlo; si no, intentar con el cliente normal.
    if settings.supabase_service_role_key:
        get_supabase_service_client().table("teams").insert(payload).execute()
        _log(f"DB: insert(team) via service_role team={name!r} country={country!r}")
        return

    try:
        client.table("teams").insert(payload).execute()
        _log(f"DB: insert(team) via anon team={name!r} country={country!r}")
        return
    except Exception as exc:
        # Mostrar el error real (RLS / GRANT / tipos).
        raise RuntimeError(
            f'No se pudo insertar en "teams" con SUPABASE_KEY. '
            f"Si RLS está ON, define SUPABASE_SERVICE_ROLE_KEY o crea una policy. "
            f"Error: {exc}"
        ) from exc
    _log(f"DB: insert team={name!r} country={country!r}")


def _scrape_competition_links_sync() -> list[CompetitionLinkResult]:
    return _run_makeyourstats_flow_sync(include_competition_links=True)["items"]


async def scrape_makeyourstats_competition_links() -> list[CompetitionLinkResult]:
    import asyncio

    return await asyncio.to_thread(_scrape_competition_links_sync)


async def run_makeyourstats_flow(
    include_competition_links: bool,
) -> MakeYourStatsFlowResult:
    import asyncio

    return await asyncio.to_thread(_run_makeyourstats_flow_sync, include_competition_links)

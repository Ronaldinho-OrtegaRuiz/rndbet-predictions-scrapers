"""
SofaScore (Playwright): competiciones de SOFASCORE_LEAGUE_TARGETS → Partidos → todas las
jornadas/rondas del desplegable → `matches` con `team_match_stats` (spec.text).

- Lista: nombres, marcador, fecha/hora aproximada, IDs Supabase por nombre.
- Partidos **Finished**: se abre la **ficha del partido** (misma ventana); fecha/hora exactas
  en el cabecero; en esa misma URL el control **Estadísticas** (`role=tab`) solo cambia el
  panel `#tabpanel-statistics` (no es otra pestaña del navegador ni otra URL).
- Schedule / Posponed: no se entra a la ficha; `team_match_stats` / `match_events` vacíos.

- Finished: cronología en la vista inicial de la ficha (spec.text) → `match_events` (`name` en cada evento,
  `player_id` null); luego tab Estadísticas → `team_match_stats`.

Progreso: `var/<slug-liga>/jornada-N.json`. Clave por partido: `verificador_ya_procesado` (id de
SofaScore para dedupe/resume; no es FK de BD). Si el JSON ya existe, misma `season_id` y
`round_label`, mismos verificadores que la lista web y datos completos → se omite la jornada.
Si `var/<slug>/.liga_lista_completa` existe → se omite **toda** la liga (no abre el torneo).
Cualquier jornada incompleta borrada invalida `.liga_lista_completa`.

`season_id` en partidos/JSON: Ligue 1 → 4, Bundesliga → 5 (resto 1–3, 6–7 en orden Premier…UEL).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.scrapers.sofascore_scraper import (
    SOFASCORE_ES_URL,
    SOFASCORE_LEAGUE_TARGETS,
    _first_football_tournament_link,
    _link_label,
    _type_search_query,
)


class _LeagueTarget(TypedDict):
    search: str
    inferred_country: str | None


_LOG_PREFIX = "[sofascore-statistics]"

# JSON por partido: id de evento SofaScore solo para reanudar / comprobar lista (no confundir con player_id).
_VERIFICADOR_YA_PROCESADO = "verificador_ya_procesado"
_LEGACY_VERIFICADOR_EVENT_ID = "event_id"

# Marcador: último scrape de la liga terminó sin error; salta abrir torneo en la siguiente corrida.
_LIGA_LISTA_COMPLETA = ".liga_lista_completa"

# `season_id` persistido en JSON por índice en `SOFASCORE_LEAGUE_TARGETS` (orden de ejecución igual al tuple:
# Premier, La Liga, Serie A, Bundesliga, Ligue 1, UCL, UEL). Ligue 1=4 y Bundesliga=5 intercambiados.
_STATISTICS_HISTORICS_SEASON_IDS: tuple[int, ...] = (1, 2, 3, 5, 4, 6, 7)


def _var_root() -> Path:
    root = Path(__file__).resolve().parents[2]
    out = root / "var"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _match_verificador_value(m: dict[str, Any]) -> str | None:
    raw = m.get(_VERIFICADOR_YA_PROCESADO)
    if raw is None or raw == "":
        raw = m.get(_LEGACY_VERIFICADOR_EVENT_ID)
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    return s or None


def _liga_lista_completa_path(league_dir: Path) -> Path:
    return league_dir / _LIGA_LISTA_COMPLETA


def _unlink_liga_lista_completa_marker(league_dir: Path) -> None:
    p = _liga_lista_completa_path(league_dir)
    try:
        p.unlink(missing_ok=True)  # py3.8+ missing_ok
    except TypeError:
        if p.is_file():
            p.unlink()


def _write_liga_lista_completa_marker(league_dir: Path) -> None:
    league_dir.mkdir(parents=True, exist_ok=True)
    _liga_lista_completa_path(league_dir).write_text(
        '{"ok":true}\n',
        encoding="utf-8",
    )


def _try_load_matches_from_completed_league_dir(
    league_dir: Path,
    *,
    season_id: int,
) -> list[dict[str, Any]] | None:
    """
    Si existe `.liga_lista_completa` y los JSON de jornada tienen la misma season_id, devuelve
    todos los partidos (dedupe por verificador). Si algo no cuadra, None → scrape normal.
    """
    if not _liga_lista_completa_path(league_dir).is_file():
        return None
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    paths = sorted(
        p
        for p in league_dir.glob("*.json")
        if not p.name.startswith(".")
    )
    if not paths:
        return None
    for path in paths:
        data = _load_round_json_file(path)
        if not data or data.get("season_id") != season_id:
            return None
        ms = data.get("matches")
        if not isinstance(ms, list):
            return None
        for m in ms:
            if not isinstance(m, dict):
                return None
            vid = _match_verificador_value(m)
            if not vid:
                return None
            if vid in seen:
                continue
            seen.add(vid)
            out.append(m)
    if not out:
        return None
    return out


def _load_round_json_file(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _saved_match_is_complete(m: dict[str, Any]) -> bool:
    st = m.get("status")
    if st == "Finished":
        if not m.get("date"):
            return False
        tms = m.get("team_match_stats")
        if not isinstance(tms, list) or len(tms) < 2:
            return False
        for s in tms[:2]:
            if not isinstance(s, dict):
                return False
            if s.get("possession") is None and s.get("shots") is None:
                return False
        return True
    if st in ("Schedule", "Posponed"):
        return True
    return False


def _try_resume_round_from_json(
    path: Path,
    *,
    season_id: int,
    round_label: str,
    expected_eids: set[str],
) -> dict[str, dict[str, Any]] | None:
    """
    Devuelve mapa verificador_ya_procesado → match si el archivo es reutilizable; si no, None.
    """
    if not expected_eids:
        return None
    data = _load_round_json_file(path)
    if not data:
        return None
    if data.get("season_id") != season_id:
        return None
    if _norm_ws(str(data.get("round_label") or "")) != _norm_ws(round_label):
        return None
    matches = data.get("matches")
    if not isinstance(matches, list):
        return None
    by_eid: dict[str, dict[str, Any]] = {}
    for m in matches:
        if not isinstance(m, dict):
            return None
        vid = _match_verificador_value(m)
        if not vid:
            return None
        by_eid[vid] = m
    if set(by_eid.keys()) != expected_eids:
        return None
    for eid in expected_eids:
        if not _saved_match_is_complete(by_eid[eid]):
            return None
    return by_eid


def _tournament_slug_from_url(url: str) -> str:
    base = (url or "").split("#")[0].split("?")[0].rstrip("/")
    parts = [p for p in base.split("/") if p]
    try:
        i = parts.index("tournament")
        if i + 2 < len(parts) and not parts[i + 2].isdigit():
            seg = parts[i + 2]
        elif i + 2 < len(parts) and parts[i + 2].isdigit() and i + 1 < len(parts):
            seg = parts[i + 1]
        else:
            seg = parts[i + 1] if i + 1 < len(parts) else "unknown"
    except ValueError:
        seg = "unknown"
    safe = re.sub(r'[<>:"/\\|?*\s]', "-", seg).strip("-").lower()
    return safe or "unknown-league"


def _round_progress_filename(round_label: str) -> str:
    raw = _norm_ws(round_label)
    m = re.fullmatch(r"(?i)Jornada\s+(\d+)", raw)
    if m:
        return f"jornada-{m.group(1)}.json"
    slug = raw.lower()
    slug = re.sub(r'[<>:"/\\|?*]', "", slug)
    slug = re.sub(r"[^\w\s-]", "", slug, flags=re.UNICODE)
    slug = re.sub(r"[\s_]+", "-", slug).strip("-")
    return f"{slug or 'round'}.json"


# Hora mostrada en SofaScore (p. ej. 14:00) alineada a zona Colombia (UTC-5, sin DST).
_TZ_COLOMBIA = ZoneInfo("America/Bogota")

# Fila de partido en la lista de jornada (no otros <a> a /football/match/).
_SOFA_MATCH_LIST_ROW = (
    'a[href*="/football/match/"][data-id][class*="event-hl-"]:has(.js-list-cell-target)'
)

# Español (etiqueta del select) → stage en inglés (UEFA, fuera de fase de grupos).
_UEFA_STAGE_ES_TO_EN: tuple[tuple[str, str], ...] = (
    (r"(?i)dieciseisav", "round_of_32"),
    (r"(?i)octavos?\s+de\s+final", "round_of_16"),
    (r"(?i)cuartos?\s+de\s+final", "quarterfinals"),
    (r"(?i)semifinal", "semifinals"),
    (r"(?i)^\s*final\s*$", "final"),
    (r"(?i)tercer", "third_place"),
    (r"(?i)play[- ]?off", "playoffs"),
    (r"(?i)ronda\s+de\s+playoffs?", "playoffs"),
    (r"(?i)clasificaci", "qualifying"),
)


def _log(msg: str) -> None:
    print(f"{_LOG_PREFIX} {msg}", flush=True)


# Intersticial / bloque SEO (p. ej. texto promocional azul sobre marcadores en directo).
_SOFASCORE_SEO_SPLASH_MARKERS: tuple[str, ...] = (
    "marcadores de fútbol en directo de sofascore",
    "resultados en tiempo real para más de 500 ligas",
    "live football scores",
    "500 leagues, cups and tournaments",
)


def _page_url_is_usable_sofascore(page: Any) -> bool:
    """Tras `go_back` la primera entrada suele ser about:blank — no es éxito."""
    try:
        u = (page.url or "").strip().lower()
    except Exception:
        return False
    return u.startswith("http") and "sofascore.com" in u


def _page_is_sofascore_seo_splash(page: Any) -> bool:
    """
    El copy SEO (“500 ligas”, “marcadores en directo…”) también está en el **footer**
    de la home normal: no basta con buscar el texto en body.

    Tratamos como intersticial solo si aparecen esos marcadores y además la UI principal
    no está usable (#search-input visible en home), salvo en rutas de app (/football/...)
    donde ese texto es casi siempre solo footer.
    """
    try:
        body = page.locator("body")
        if body.count() == 0:
            return False
        sample = (body.inner_text(timeout=5000) or "").lower()
    except Exception:
        return False
    if not any(m in sample for m in _SOFASCORE_SEO_SPLASH_MARKERS):
        return False
    try:
        u = (page.url or "").lower()
    except Exception:
        u = ""
    if "/football/tournament/" in u or "/football/match/" in u:
        return False
    if "sofascore.com" not in u:
        return False
    try:
        inp = page.locator("#search-input")
        if inp.count() > 0 and inp.first.is_visible():
            return False
    except Exception:
        pass
    return True


def _recover_from_seo_splash_same_page(
    page: Any,
    *,
    target_url: str,
    timeout_ms: int,
    wait_s: float,
    hint: str = "",
) -> None:
    """Escape → atrás (solo si hay historial) → goto a la URL objetivo (misma pestaña)."""
    suffix = f" ({hint})" if hint else ""

    def _resolved_ok() -> bool:
        return (not _page_is_sofascore_seo_splash(page)) and _page_url_is_usable_sofascore(page)

    for _ in range(2):
        try:
            page.keyboard.press("Escape")
            time.sleep(0.12)
            page.keyboard.press("Escape")
            time.sleep(0.28)
        except Exception:
            pass
        if _resolved_ok():
            _log(f"  ↳ intersticial SofaScore{suffix} cerrado (Escape).")
            return
        can_back = False
        try:
            can_back = bool(page.evaluate("() => window.history.length > 1"))
        except Exception:
            can_back = True
        if can_back:
            try:
                page.go_back(wait_until="domcontentloaded")
                page.wait_for_load_state("load")
                time.sleep(min(1.0, max(0.35, wait_s * 0.35)))
            except Exception:
                pass
        if _resolved_ok():
            _log(f"  ↳ intersticial SofaScore{suffix} recuperado (atrás).")
            return
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(min(1.3, max(0.45, wait_s * 0.45)))
        except Exception:
            pass
        if _resolved_ok():
            _log(f"  ↳ intersticial SofaScore{suffix} recuperado (goto).")
            return
    if _page_is_sofascore_seo_splash(page):
        _log(f"  (aviso) sigue visible la vista SEO SofaScore{suffix}.")


def _resolve_page_after_splash(
    page: Any,
    context: Any | None,
    *,
    target_url: str,
    timeout_ms: int,
    wait_s: float,
    hint: str = "",
) -> Any:
    """
    Si aparece la vista promocional/SEO: reintentar en la misma pestaña; si sigue y hay
    `context`, abrir una pestaña nueva, cerrar la actual y cargar `target_url`.
    """
    if not _page_is_sofascore_seo_splash(page):
        if _page_url_is_usable_sofascore(page):
            return page
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(min(1.2, max(0.4, wait_s * 0.4)))
        except Exception:
            pass
        return page
    _log(f"  Vista intersticial/SEO SofaScore detectada{(' ' + hint) if hint else ''}…")
    _recover_from_seo_splash_same_page(
        page,
        target_url=target_url,
        timeout_ms=timeout_ms,
        wait_s=wait_s,
        hint=hint,
    )
    if (not _page_is_sofascore_seo_splash(page)) and _page_url_is_usable_sofascore(page):
        return page
    if (not _page_is_sofascore_seo_splash(page)) and not _page_url_is_usable_sofascore(page):
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(min(1.2, max(0.4, wait_s * 0.4)))
        except Exception:
            pass
        if _page_url_is_usable_sofascore(page) and not _page_is_sofascore_seo_splash(page):
            return page
    if context is None:
        return page
    try:
        _log("  Reintentando con una pestaña nueva en el mismo navegador…")
        old = page
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        old.close()
        page.goto(target_url, wait_until="domcontentloaded")
        page.wait_for_load_state("load")
        time.sleep(min(1.3, max(0.45, wait_s * 0.45)))
        if _page_is_sofascore_seo_splash(page):
            _log("  (aviso) la nueva pestaña sigue mostrando la vista SEO.")
        else:
            _log("  ↳ OK tras abrir pestaña nueva.")
    except Exception as exc:
        _log(f"  (aviso) pestaña nueva: {exc}")
    return page


def _is_uefa_target(target: _LeagueTarget) -> bool:
    return target["inferred_country"] is None


def _norm_ws(text: str) -> str:
    return " ".join((text or "").replace("\xa0", " ").split())


def _today_colombia() -> date:
    return datetime.now(_TZ_COLOMBIA).date()


def _resolve_team_id_by_name(
    name: str | None,
    team_ok: dict[str, int],
    team_failed: set[str],
) -> int | None:
    if not name:
        return None
    key = _norm_ws(name)
    if not key:
        return None
    if key in team_ok:
        return team_ok[key]
    if key in team_failed:
        return None
    if not (settings.supabase_url and settings.supabase_key):
        team_failed.add(key)
        return None

    from app.db.supabase_client import get_supabase_client

    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            client = get_supabase_client()
            res = client.table("teams").select("id").eq("name", key).limit(1).execute()
            rows = res.data or []
            if rows:
                tid = int(rows[0]["id"])
                team_ok[key] = tid
                return tid
            res2 = (
                client.table("teams")
                .select("id")
                .ilike("name", f"%{key}%")
                .limit(1)
                .execute()
            )
            rows2 = res2.data or []
            if rows2:
                tid = int(rows2[0]["id"])
                team_ok[key] = tid
                return tid
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(0.2 + attempt * 0.15)

    if last_exc:
        _log(f"(aviso) teams.id por nombre {key!r}: {last_exc}")
    else:
        _log(f"(aviso) sin fila en teams para nombre {key!r}")
    team_failed.add(key)
    return None


def _round_dropdown_trigger(page: Any) -> Any:
    return page.locator("button.dropdown__button[role='combobox']").filter(
        has=page.locator(
            "span.dropdown__selectedItem",
        ).filter(
            has_text=re.compile(
                r"Jornada|Ronda|Octavos|Cuartos|Semifinal|Final|Clasificaci",
                re.I,
            ),
        ),
    ).first


def _ensure_partidos_tab(page: Any, timeout_ms: int) -> None:
    tab_tmo = min(15_000, timeout_ms)
    for sel in (
        '[data-testid="tab-matches"]',
        'button[role="tab"]:has-text("Partidos")',
        'a[href*="tab:matches"]',
    ):
        loc = page.locator(sel).first
        try:
            if loc.count() == 0:
                continue
            loc.wait_for(state="visible", timeout=min(5000, tab_tmo))
            if loc.get_attribute("aria-selected") == "true":
                return
            loc.click()
            time.sleep(0.55)
            return
        except Exception:
            continue
    _log("(aviso) No se encontró pestaña Partidos; se asume lista de partidos visible.")
    time.sleep(0.3)


def _dropdown_list_items(page: Any, timeout_ms: int) -> Any:
    tmo = min(20_000, timeout_ms)
    trigger = _round_dropdown_trigger(page)
    trigger.wait_for(state="visible", timeout=tmo)
    trigger.click()
    lst = page.locator("ul.dropdown__list").first
    lst.wait_for(state="visible", timeout=tmo)
    time.sleep(0.15)
    return lst.locator("li.dropdown__listItem[role='option']")


def _close_dropdown(page: Any) -> None:
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    time.sleep(0.2)


def _dropdown_item_text(loc: Any, timeout_ms: int) -> str:
    """Lista larga virtualizada: sin scroll, inner_text en nth(i>0) suele hacer timeout."""
    tmo = max(10_000, min(timeout_ms, 90_000))
    try:
        loc.scroll_into_view_if_needed(timeout=tmo)
    except Exception:
        pass
    return _norm_ws(loc.inner_text(timeout=tmo))


def _uefa_round_labels_skip_pre_group(raw: list[str]) -> list[str]:
    """
    En Champions/Europa el select mezcla clasificación, playoffs, etc. antes de la fase de grupos.
    Solo nos interesa desde la primera «Jornada 1» en adelante (grupos + octavos/cuartos/semis/final).
    Si no hay «Jornada 1» (solo knockout en el desplegable), se devuelve la lista completa.
    """
    for i, x in enumerate(raw):
        if re.fullmatch(r"(?i)Jornada\s+1", _norm_ws(x)):
            return raw[i:]
    return raw


def _read_all_round_labels(page: Any, *, is_uefa: bool, timeout_ms: int) -> list[str]:
    items = _dropdown_list_items(page, timeout_ms)
    items.first.wait_for(state="visible", timeout=min(20_000, timeout_ms))
    n = items.count()
    raw = [_dropdown_item_text(items.nth(i), timeout_ms) for i in range(n)]
    _close_dropdown(page)
    raw = [x for x in raw if x]
    if is_uefa:
        return _uefa_round_labels_skip_pre_group(raw)
    jornadas: list[tuple[int, str]] = []
    for x in raw:
        m = re.fullmatch(r"Jornada (\d+)", x, re.I)
        if m:
            jornadas.append((int(m.group(1)), x))
    jornadas.sort(key=lambda t: t[0])
    return [lbl for _, lbl in jornadas]


def _select_round_label(page: Any, label: str, timeout_ms: int) -> None:
    tmo = min(20_000, timeout_ms)
    items = _dropdown_list_items(page, timeout_ms)
    items.first.wait_for(state="visible", timeout=tmo)
    n = items.count()
    target = _norm_ws(label)
    for i in range(n):
        if _dropdown_item_text(items.nth(i), timeout_ms) == target:
            items.nth(i).click()
            time.sleep(0.45)
            _close_dropdown(page)
            return
    _close_dropdown(page)
    raise RuntimeError(f"No se encontró la opción del desplegable: {label!r}")


def _uefa_stage_english(round_label: str) -> str:
    n = _norm_ws(round_label)
    if re.search(r"(?i)^Jornada\s+\d+$", n):
        return "League_Stage"
    low = n.lower()
    for pat, en in _UEFA_STAGE_ES_TO_EN:
        if re.search(pat, low):
            return en
    return re.sub(r"\s+", "_", low)


def _domestic_round_stage(round_label: str) -> tuple[int | None, str]:
    m = re.search(r"Jornada\s+(\d+)", round_label, re.I)
    rnum = int(m.group(1)) if m else None
    return rnum, "League"


def _uefa_round_stage(round_label: str) -> tuple[int | None, str]:
    m = re.search(r"Jornada\s+(\d+)", round_label, re.I)
    if m:
        return int(m.group(1)), "League_Stage"
    return None, _uefa_stage_english(round_label)


def _parse_dmY_HM(date_s: str, time_s: str | None) -> datetime | None:
    date_s = _norm_ws(date_s)
    time_s = _norm_ws(time_s) if time_s else ""
    dm = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", date_s)
    if not dm:
        return None
    d, mo, y = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
    if y < 100:
        y += 2000
    if time_s:
        tm = re.match(r"^(\d{1,2}):(\d{2})$", time_s)
        if not tm:
            return None
        h, mi = int(tm.group(1)), int(tm.group(2))
    else:
        h, mi = 0, 0
    try:
        return datetime(y, mo, d, h, mi, tzinfo=_TZ_COLOMBIA)
    except ValueError:
        return None


def _parse_list_row_datetime(row_text: str) -> datetime | None:
    dm = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", row_text or "")
    tm = re.search(r"\b(\d{1,2}):(\d{2})\b", row_text or "")
    if not dm:
        return None
    ds = f"{dm.group(1)}/{dm.group(2)}/{dm.group(3)}"
    ts = f"{tm.group(1)}:{tm.group(2)}" if tm else None
    return _parse_dmY_HM(ds, ts)


def _row_looks_postponed(loc: Any) -> bool:
    t = loc.inner_text(timeout=5000) or ""
    if re.search(r"(?i)pospuesto|aplazad|postponed", t):
        return True
    badge = loc.locator("[class*='bg_status.live']")
    if badge.count() == 0:
        return False
    try:
        op = badge.first.evaluate(
            """el => {
              const n = el.querySelector('div') || el;
              const o = window.getComputedStyle(n).opacity;
              return parseFloat(o || '0');
            }""",
        )
        return float(op) > 0.08
    except Exception:
        return False


def _bdi_team_names_in_scope(scope: Any) -> list[str]:
    """bdi junto a escudos: local nLv1, visita nLv3 (misma clase trunc_true)."""
    bdis = scope.locator("bdi.textStyle_body.medium.trunc_true")
    names: list[str] = []
    k = bdis.count()
    for i in range(min(k, 12)):
        t = _norm_ws(bdis.nth(i).inner_text(timeout=2000))
        if not t:
            continue
        if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", t):
            continue
        if re.match(r"^\d{1,2}:\d{2}$", t):
            continue
        names.append(t)
    return names


def _team_line_name(line: Any) -> str | None:
    """Una fila equipo: <div class='d_flex ai_center …'> img + bdi (orden SofaScore listado)."""
    try:
        bdis = line.locator("bdi")
        if bdis.count() > 0:
            t = _norm_ws(bdis.first.inner_text(timeout=2000))
            if t:
                return t
    except Exception:
        pass
    try:
        imgs = line.locator('img[src*="/api/v1/team/"]')
        if imgs.count() > 0:
            return _norm_ws(imgs.first.get_attribute("alt") or "")
    except Exception:
        pass
    return None


def _team_names_from_row(loc: Any) -> tuple[str | None, str | None]:
    """
    Bloque title*='Resultado en directo': dos hijos directos div.d_flex.ai_center
    (local con mb_2xs, visita); cada uno img + bdi — orden fijo local/visitante.
    """
    team_block = loc.locator('div[title*="Resultado en directo"]')
    if team_block.count() == 0:
        names = _bdi_team_names_in_scope(loc)
        return (
            names[0] if len(names) > 0 else None,
            names[1] if len(names) > 1 else None,
        )

    block = team_block.first
    line_sel = ":scope > div[class*='d_flex'][class*='ai_center']"
    rows = block.locator(line_sel)
    n = rows.count()
    home: str | None = None
    away: str | None = None
    if n >= 2:
        home = _team_line_name(rows.nth(0))
        away = _team_line_name(rows.nth(1))
    if home and away:
        return home, away

    # Variante DOM: filas no son hijos directos del title div
    rows = block.locator("div[class*='d_flex'][class*='ai_center']")
    if rows.count() >= 2:
        h2 = _team_line_name(rows.nth(0))
        a2 = _team_line_name(rows.nth(1))
        if h2 and a2:
            return h2, a2

    names = _bdi_team_names_in_scope(block)
    if len(names) >= 2:
        return names[0], names[1]
    return (names[0] if names else None, names[1] if len(names) > 1 else None)


def _scores_from_list_row(loc: Any) -> tuple[int | None, int | None]:
    """Marcador solo dentro de la fila `<a event-hl-...>` (partidos ya jugados)."""
    try:
        raw = loc.evaluate(
            """(row) => {
              const txt = (row.innerText || "").replace(/\\r/g, "");
              const reScore = /(\\d{1,2})\\s*[-–]\\s*(\\d{1,2})/g;
              let m;
              let best = null;
              while ((m = reScore.exec(txt)) !== null) {
                const a = parseInt(m[1], 10), b = parseInt(m[2], 10);
                if (a >= 0 && a <= 30 && b >= 0 && b <= 30) best = [a, b];
              }
              if (best) return best;
              const nums = [];
              for (const sp of row.querySelectorAll("span")) {
                if (sp.children.length) continue;
                const t = (sp.textContent || "").trim();
                if (!/^\\d{1,2}$/.test(t)) continue;
                const n = parseInt(t, 10);
                if (n >= 0 && n <= 30) nums.push(n);
                if (nums.length >= 2) return [nums[0], nums[1]];
              }
              return null;
            }""",
        )
    except Exception:
        return None, None
    if raw and isinstance(raw, list) and len(raw) == 2:
        try:
            return int(raw[0]), int(raw[1])
        except (TypeError, ValueError):
            pass
    return None, None


def _start_of_day_colombia(dt: datetime) -> datetime:
    d = dt.date()
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=_TZ_COLOMBIA)


_MATCH_URL_RE = re.compile(r".*/football/match/.*")

# Ejecutado sobre `#tabpanel-statistics`. Alineado con spec.text:
# - Tiros (+ tiros a puerta): bloque «Tiros»; bdi ta_start = local, ta_end = visita.
# - Paradas (saves): bloque «Portería».
# - Amarillas / rojas / córners / faltas: bloque «Resumen del partido».
# - Fueras de juego: bloque «Ataque».
# Matching: texto del label = igualdad exacta (normalizada), no .includes: «Grandes paradas»
# contiene «Paradas» y «Faltas recibidas…» contiene «Faltas» — eso rompía el scrape.
# `textStyle_assistive.default` es un solo token de clase; usar [class*="textStyle_assistive"].
_STATS_PANEL_JS = r"""
(panel) => {
  const root = panel;
  if (!root) return null;

  const LABELS = [
    { es: 'Tiros totales', key: 'shots' },
    { es: 'Tiros a puerta', key: 'shots_on_target' },
    { es: 'Paradas', key: 'saves' },
    { es: 'Tarjetas amarillas', key: 'yellow_cards' },
    { es: 'Tarjetas rojas', key: 'red_cards' },
    { es: 'Saques de esquina', key: 'corners' },
    { es: 'Faltas', key: 'fouls' },
    { es: 'Fueras de juego', key: 'offsides' },
  ];

  function normLabel(s) {
    return String(s || '')
      .replace(/\u00a0/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function parseIntLoose(s) {
    if (s == null) return null;
    const m = String(s).match(/\d+/);
    return m ? parseInt(m[0], 10) : null;
  }

  const home = {};
  const away = {};

  const rows = root.querySelectorAll(
    'div[class*="d_flex"][class*="ai_center"][class*="jc_space-between"]',
  );
  for (const row of rows) {
    const assist = row.querySelector('span[class*="textStyle_assistive"]');
    if (!assist) continue;
    const label = normLabel(assist.textContent);
    let key = null;
    for (const { es, key: k } of LABELS) {
      if (label === es) {
        key = k;
        break;
      }
    }
    if (!key) continue;
    const bdis = row.querySelectorAll('bdi');
    if (bdis.length < 2) continue;
    const left = parseIntLoose(bdis[0].innerText);
    const right = parseIntLoose(bdis[bdis.length - 1].innerText);
    if (left !== null) home[key] = left;
    if (right !== null) away[key] = right;
  }

  function parsePossession(clsFrag) {
    let el = root.querySelector('[class*="' + clsFrag + '"]');
    if (!el) el = document.querySelector('[class*="' + clsFrag + '"]');
    if (!el) return null;
    const inner = el.querySelector('span[class*="textStyle_assistive"]') || el;
    const m = (inner.textContent || '').match(/(\d+(?:\.\d+)?)\s*%/);
    return m ? parseFloat(m[1]) : null;
  }

  const ph = parsePossession('bg_homeAway.home.primary');
  const pa = parsePossession('bg_homeAway.away.primary');

  return { home, away, possession_home: ph, possession_away: pa };
}
"""

# Cronología / incidentes en la ficha del partido (spec.text): antes del tab Estadísticas.
# Local: flex-d_row | Visitante: flex-d_row-reverse (también variantes flex-d-row*).
# Minuto: 90' + <sup>+6</sup> → 96. Se ancla por svg con <title>/aria-label conocidos para
# no depender del wrapper cursor_pointer ni h_3xl (cambian con frecuencia).
_TIMELINE_EVENTS_JS = r"""
() => {
  const TITLE_TO_TYPE = {
    'Tarjeta amarilla': 'YELLOW_CARD',
    'Gol': 'GOAL',
    'Penalti anotado': 'PENALTY_GOAL',
    'Tarjeta roja': 'RED_CARD',
    '2ª amarilla (roja)': 'SECOND_YELLOW_CARD',
    'Penalti fallado': 'PENALTY_FAILED',
    'Yellow card': 'YELLOW_CARD',
    'Goal': 'GOAL',
    'Penalty scored': 'PENALTY_GOAL',
    'Red card': 'RED_CARD',
    'Second yellow card': 'SECOND_YELLOW_CARD',
    'Penalty missed': 'PENALTY_FAILED',
    'Own goal': 'OWN_GOAL',
    'Autogol': 'OWN_GOAL',
  };

  function svgTitle(svg) {
    if (!svg) return '';
    const t = svg.querySelector('title');
    if (t && t.textContent) return String(t.textContent || '').trim();
    const al = svg.getAttribute('aria-label');
    if (al) return String(al).trim();
    return '';
  }

  function eventTypeFromSvg(svg) {
    const raw = svgTitle(svg);
    if (!raw) return null;
    if (TITLE_TO_TYPE[raw]) return TITLE_TO_TYPE[raw];
    const collapsed = raw.replace(/\s+/g, ' ').trim();
    return TITLE_TO_TYPE[collapsed] || null;
  }

  function minuteFromTimeSpan(sp) {
    if (!sp) return null;
    let baseStr = '';
    for (const node of sp.childNodes) {
      if (node.nodeName === 'SUP') break;
      baseStr += node.textContent || '';
    }
    const bm = baseStr.match(/(\d+)/);
    if (!bm) return null;
    let total = parseInt(bm[1], 10);
    const sup = sp.querySelector('sup');
    if (sup) {
      const am = String(sup.textContent || '').match(/\+?\s*(\d+)/);
      if (am) total += parseInt(am[1], 10);
    }
    return total;
  }

  function timelineRowFromSvg(svg) {
    // `flex-d_row-reverse` contiene la subcadena `flex-d_row`: closest() pillaba solo la subfila
    // (minuto+svg), sin los spans del nombre (hermanos). Subir al div h_3xl de la fila completa.
    let el = svg.parentElement;
    while (el && el !== document.body) {
      const c = el.className || '';
      if (typeof c !== 'string') {
        el = el.parentElement;
        continue;
      }
      const rowLike =
        c.includes('flex-d_row-reverse') ||
        c.includes('flex-d-row-reverse') ||
        (c.includes('flex-d_row') && !c.includes('flex-d_row-reverse')) ||
        (c.includes('flex-d-row') && !c.includes('flex-d-row-reverse'));
      if (rowLike && c.includes('h_3xl')) return el;
      el = el.parentElement;
    }
    const inner =
      svg.closest('div[class*="flex-d_row-reverse"]') ||
      svg.closest('div[class*="flex-d-row-reverse"]') ||
      svg.closest('div[class*="flex-d_row"]') ||
      svg.closest('div[class*="flex-d-row"]');
    if (inner && inner.parentElement) return inner.parentElement;
    return inner;
  }

  function primaryNameFromRow(row) {
    const spans = row.querySelectorAll('span[class*="textStyle_body.medium"]');
    for (const sp of spans) {
      const c = sp.className || '';
      if (!c.includes('trunc')) continue;
      if (c.includes('nLv3')) continue;
      if (!c.includes('nLv1')) continue;
      const t = String(sp.textContent || '').trim();
      if (t && !/^(\d{1,3})\s*['′`´]?$/.test(t)) return t;
    }
    return null;
  }

  function rowHomeAway(cls) {
    const c = cls || '';
    const away =
      c.includes('flex-d_row-reverse') ||
      c.includes('flex-d-row-reverse');
    const home =
      (c.includes('flex-d_row') || c.includes('flex-d-row')) && !away;
    return { isHome: home, isAway: away };
  }

  const root = document.querySelector('main');
  if (!root) return [];

  const out = [];
  const seenRows = new WeakSet();

  const svgs = root.querySelectorAll('svg');
  for (const svg of svgs) {
    const eventType = eventTypeFromSvg(svg);
    if (!eventType) continue;
    const row = timelineRowFromSvg(svg);
    if (!row) continue;
    const { isHome, isAway } = rowHomeAway(row.className || '');
    if (!isHome && !isAway) continue;
    if (seenRows.has(row)) continue;
    seenRows.add(row);

    let minute = null;
    const timeCol =
      row.querySelector('[class*="w_3xl"]') ||
      row.querySelector('[class*="w-3xl"]');
    if (timeCol) {
      const sp =
        timeCol.querySelector('span[class*="textStyle_display.micro"]') ||
        timeCol.querySelector('span[class*="textStyle_display"][class*="micro"]') ||
        timeCol.querySelector('span[class*="micro"]');
      minute = minuteFromTimeSpan(sp);
    }
    if (minute == null) {
      const tx = row.innerText || '';
      const mm = tx.match(/(\d+)\s*['′`´]/);
      if (mm) minute = parseInt(mm[1], 10);
    }

    const name = primaryNameFromRow(row);

    out.push({
      is_home: isHome,
      minute,
      event_type: eventType,
      name,
    });
  }
  return out;
}
"""


def _scroll_main_for_timeline_hydration(page: Any, wait_s: float) -> None:
    """La cronología suele ir bajo el pliegue; recorrer scroll del main para hidratar filas."""
    pause = min(0.35, max(0.12, wait_s * 0.12))
    try:
        page.evaluate(
            """() => {
              const m = document.querySelector('main');
              if (!m) return;
              const h = m.scrollHeight || 0;
              for (const frac of [0.25, 0.5, 0.75, 1.0]) {
                m.scrollTo(0, Math.max(0, h * frac - 80));
              }
              m.scrollTo(0, 0);
            }""",
        )
        time.sleep(pause)
    except Exception:
        pass


def _match_page_absolute_url(href: str) -> str:
    h = (href or "").strip()
    if h.startswith("http"):
        return h
    if not h.startswith("/"):
        h = "/" + h
    return urljoin("https://www.sofascore.com", h)


def _match_events_from_timeline_raw(
    raw: Any,
    *,
    home_team_id: int | None,
    away_team_id: int | None,
) -> list[dict[str, Any]]:
    """Convierte la cronología cruda en `match_events` (name por evento; sin array global players)."""
    if not isinstance(raw, list):
        return []
    events: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        et = item.get("event_type")
        if not et or not isinstance(et, str):
            continue
        is_home = bool(item.get("is_home"))
        tid = home_team_id if is_home else away_team_id
        nm = item.get("name")
        name_s = _norm_ws(nm) if isinstance(nm, str) else None
        mi = item.get("minute")
        minute_i: int | None = None
        if isinstance(mi, int):
            minute_i = mi
        elif isinstance(mi, float) and mi == int(mi):
            minute_i = int(mi)
        events.append(
            {
                "id": None,
                "match_id": None,
                "team_id": tid,
                "player_id": None,
                "name": name_s,
                "minute": minute_i,
                "event_type": et,
                "extra_data": None,
                "created_at": None,
            }
        )
    return events


def _scrape_match_timeline_raw(page: Any) -> list[Any]:
    try:
        raw: Any = page.evaluate(_TIMELINE_EVENTS_JS)
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def _shell_team_match_stats(base: dict[str, Any]) -> list[dict[str, Any]]:
    def one(team_id: int | None, is_home: bool, goals: int | None) -> dict[str, Any]:
        return {
            "id": None,
            "match_id": None,
            "team_id": team_id,
            "is_home": is_home,
            "goals": goals,
            "possession": None,
            "shots": None,
            "shots_on_target": None,
            "saves": None,
            "yellow_cards": None,
            "red_cards": None,
            "corners": None,
            "fouls": None,
            "offsides": None,
        }

    g0 = base.get("home_score")
    g1 = base.get("away_score")
    return [
        one(base.get("home_team_id"), True, g0 if isinstance(g0, int) else None),
        one(base.get("away_team_id"), False, g1 if isinstance(g1, int) else None),
    ]


def _parse_match_header_datetime(page: Any) -> datetime | None:
    raw: Any = page.evaluate(
        r"""() => {
      const boxes = document.querySelectorAll('div.d_flex.ai_center.br_lg');
      for (const box of boxes) {
        const t = box.innerText || '';
        const dm = t.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
        const tm = t.match(/(\d{1,2}):(\d{2})/);
        if (dm && tm) {
          return {
            d: parseInt(dm[1], 10), m: parseInt(dm[2], 10), y: parseInt(dm[3], 10),
            hh: parseInt(tm[1], 10), mi: parseInt(tm[2], 10),
          };
        }
      }
      let dateStr = null;
      let timeStr = null;
      for (const sp of document.querySelectorAll('span.textStyle_display.micro')) {
        const tx = (sp.textContent || '').trim();
        if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(tx)) dateStr = tx;
        if (/^\d{1,2}:\d{2}$/.test(tx)) timeStr = tx;
      }
      if (dateStr && timeStr) {
        const dm = dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
        const tm = timeStr.match(/(\d{1,2}):(\d{2})/);
        if (dm && tm) {
          return {
            d: parseInt(dm[1], 10), m: parseInt(dm[2], 10), y: parseInt(dm[3], 10),
            hh: parseInt(tm[1], 10), mi: parseInt(tm[2], 10),
          };
        }
      }
      return null;
    }""",
    )
    if not raw or not isinstance(raw, dict):
        return None
    try:
        return datetime(
            int(raw["y"]),
            int(raw["m"]),
            int(raw["d"]),
            int(raw["hh"]),
            int(raw["mi"]),
            tzinfo=_TZ_COLOMBIA,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _open_match_statistics_panel(page: Any, timeout_ms: int) -> None:
    """In-page: pulsa el tab Estadísticas; el contenido cambia dentro de #tabpanel-statistics."""
    tmo = min(25_000, timeout_ms)
    tab = page.locator('[data-testid="tab-statistics"]').first
    tab.wait_for(state="visible", timeout=tmo)
    tab.click()
    page.locator("#tabpanel-statistics").wait_for(state="visible", timeout=tmo)
    try:
        page.wait_for_function(
            """() => {
              const b = document.querySelector('[data-testid="tab-statistics"]');
              return b && b.getAttribute('aria-selected') === 'true';
            }""",
            timeout=min(8000, tmo),
        )
    except Exception:
        pass
    time.sleep(min(0.5, max(0.2, settings.playwright_after_load_wait_seconds * 0.2)))


def _apply_parsed_stats_to_record(rec: dict[str, Any], data: dict[str, Any] | None) -> None:
    if not data or not isinstance(data, dict):
        return
    tms = rec.get("team_match_stats")
    if not isinstance(tms, list) or len(tms) < 2:
        return
    hstats: dict[str, Any] = tms[0]
    astats: dict[str, Any] = tms[1]
    hk = data.get("home") if isinstance(data.get("home"), dict) else {}
    ak = data.get("away") if isinstance(data.get("away"), dict) else {}
    for k in (
        "shots",
        "shots_on_target",
        "saves",
        "yellow_cards",
        "corners",
        "fouls",
    ):
        if k in hk and hk[k] is not None:
            hstats[k] = hk[k]
        if k in ak and ak[k] is not None:
            astats[k] = ak[k]
    hstats["red_cards"] = (
        hk["red_cards"] if "red_cards" in hk and hk["red_cards"] is not None else 0
    )
    astats["red_cards"] = (
        ak["red_cards"] if "red_cards" in ak and ak["red_cards"] is not None else 0
    )
    # spec.text L105: sin fila «Fueras de juego» → 0 local y visita.
    hstats["offsides"] = (
        hk["offsides"] if "offsides" in hk and hk["offsides"] is not None else 0
    )
    astats["offsides"] = (
        ak["offsides"] if "offsides" in ak and ak["offsides"] is not None else 0
    )
    ph, pa = data.get("possession_home"), data.get("possession_away")
    if ph is not None:
        hstats["possession"] = ph
    if pa is not None:
        astats["possession"] = pa


def _restore_tournament_round(
    page: Any,
    *,
    tournament_url: str,
    round_label: str,
    timeout_ms: int,
    wait_after_round: float,
    wait_s: float,
    context: Any | None = None,
) -> Any:
    page.goto(tournament_url, wait_until="domcontentloaded")
    page.wait_for_load_state("load")
    time.sleep(min(1.4, max(0.45, wait_s * 0.4)))
    page = _resolve_page_after_splash(
        page,
        context,
        target_url=tournament_url,
        timeout_ms=timeout_ms,
        wait_s=wait_s,
        hint="vuelta a torneo",
    )
    _ensure_partidos_tab(page, timeout_ms)
    _select_round_label(page, round_label, timeout_ms)
    time.sleep(wait_after_round)
    return page


def _enrich_finished_match_on_detail_page(
    page: Any,
    *,
    context: Any | None = None,
    list_row: dict[str, Any],
    rec: dict[str, Any],
    tournament_url: str,
    round_label: str,
    timeout_ms: int,
    wait_after_round: float,
    wait_s: float,
) -> tuple[dict[str, Any], Any]:
    out = dict(rec)
    tms = rec.get("team_match_stats")
    out["team_match_stats"] = (
        [dict(s) for s in tms] if isinstance(tms, list) else _shell_team_match_stats(rec)
    )
    match_url = _match_page_absolute_url(list_row["href"])
    try:
        page.goto(match_url, wait_until="domcontentloaded")
        page.wait_for_load_state("load")
        time.sleep(min(1.2, max(0.45, wait_s * 0.4)))
        page = _resolve_page_after_splash(
            page,
            context,
            target_url=match_url,
            timeout_ms=timeout_ms,
            wait_s=wait_s,
            hint="ficha partido",
        )
        try:
            page.wait_for_url(_MATCH_URL_RE, timeout=min(25_000, timeout_ms))
        except Exception:
            pass

        header_dt = _parse_match_header_datetime(page)
        if header_dt is not None:
            out["date"] = _dt_iso(header_dt)

        _scroll_main_for_timeline_hydration(page, wait_s)
        raw_tl = _scrape_match_timeline_raw(page)
        me = _match_events_from_timeline_raw(
            raw_tl,
            home_team_id=out.get("home_team_id"),
            away_team_id=out.get("away_team_id"),
        )
        out["match_events"] = me

        _open_match_statistics_panel(page, timeout_ms)
        try:
            page.evaluate(
                """() => {
                  const main = document.querySelector('main');
                  if (main) main.scrollTo(0, main.scrollHeight);
                  window.scrollTo(0, document.body.scrollHeight);
                }""",
            )
            time.sleep(0.45)
            page.evaluate(
                """() => {
                  const main = document.querySelector('main');
                  if (main) main.scrollTo(0, 0);
                  window.scrollTo(0, 0);
                }""",
            )
            time.sleep(0.2)
        except Exception:
            pass
        panel = page.locator("#tabpanel-statistics")
        raw_stats: Any = panel.evaluate(_STATS_PANEL_JS)
        if isinstance(raw_stats, dict):
            _apply_parsed_stats_to_record(out, raw_stats)
    except Exception as exc:
        _log(f"    (aviso) ficha partido event={list_row.get('event_id')!r}: {exc}")
    finally:
        try:
            page = _restore_tournament_round(
                page,
                tournament_url=tournament_url,
                round_label=round_label,
                timeout_ms=timeout_ms,
                wait_after_round=wait_after_round,
                wait_s=wait_s,
                context=context,
            )
        except Exception as exc2:
            _log(f"    (aviso) volver a torneo/jornada: {exc2}")
    return out, page


def _collect_match_rows_data(page: Any) -> list[dict[str, Any]]:
    scoped = page.locator(f"main {_SOFA_MATCH_LIST_ROW}")
    links = scoped if scoped.count() > 0 else page.locator(_SOFA_MATCH_LIST_ROW)
    by_event: dict[str, dict[str, Any]] = {}
    k = links.count()
    for i in range(k):
        el = links.nth(i)
        eid = (el.get_attribute("data-id") or "").strip()
        h = (el.get_attribute("href") or "").strip()
        if not eid or not h:
            continue
        text = el.inner_text(timeout=5000) or ""
        home_n, away_n = _team_names_from_row(el)
        lh, la = _scores_from_list_row(el)
        by_event[eid] = {
            "event_id": eid,
            "href": h,
            "list_datetime": _parse_list_row_datetime(text),
            "home_team_name": home_n,
            "away_team_name": away_n,
            "list_home_score": lh,
            "list_away_score": la,
            "is_postponed": _row_looks_postponed(el),
        }
    return list(by_event.values())


def _empty_match_payload(
    *,
    season_id: int,
    round_num: int | None,
    stage: str,
) -> dict[str, Any]:
    return {
        "season_id": season_id,
        "date": None,
        "home_team_id": None,
        "away_team_id": None,
        "home_score": None,
        "away_score": None,
        "status": None,
        "round": round_num,
        "stage": stage,
        "group": None,
        "current_minute": None,
        "added_time": None,
        "last_updated": None,
    }


def _dt_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_TZ_COLOMBIA)
    return dt.isoformat()


def _build_match_record(
    *,
    season_id: int,
    list_row: dict[str, Any],
    round_num: int | None,
    stage: str,
    team_ok: dict[str, int],
    team_failed: set[str],
) -> dict[str, Any]:
    is_postponed = bool(list_row.get("is_postponed"))
    list_dt: datetime | None = list_row.get("list_datetime")
    home_name = list_row.get("home_team_name")
    away_name = list_row.get("away_team_name")
    home_sc = list_row.get("list_home_score")
    away_sc = list_row.get("list_away_score")
    today = _today_colombia()

    home_id = _resolve_team_id_by_name(home_name, team_ok, team_failed)
    away_id = _resolve_team_id_by_name(away_name, team_ok, team_failed)

    effective_dt = list_dt

    if is_postponed:
        status = "Posponed"
    elif effective_dt is not None and effective_dt.date() < today:
        status = "Finished"
    else:
        status = "Schedule"

    base = _empty_match_payload(season_id=season_id, round_num=round_num, stage=stage)
    base["home_team_id"] = home_id
    base["away_team_id"] = away_id
    base["status"] = status

    if status == "Schedule":
        base["date"] = _dt_iso(list_dt)
    elif status == "Posponed":
        base["date"] = _dt_iso(list_dt)
    else:
        # Finished: fecha día desde lista hasta la ficha; goles desde la fila.
        if list_dt is not None:
            base["date"] = _dt_iso(_start_of_day_colombia(list_dt))
        else:
            base["date"] = None
        base["home_score"] = home_sc if isinstance(home_sc, int) else None
        base["away_score"] = away_sc if isinstance(away_sc, int) else None

    base["team_match_stats"] = _shell_team_match_stats(base)
    base["match_events"] = []
    eid = list_row.get("event_id")
    base[_VERIFICADOR_YA_PROCESADO] = (
        str(eid).strip() if eid is not None and str(eid).strip() else None
    )
    return base


def _scrape_all_matches_for_competition(
    page: Any,
    *,
    context: Any | None = None,
    season_id: int,
    is_uefa: bool,
    timeout_ms: int,
    competition_search: str,
    league_dir: Path,
    tournament_url: str,
) -> tuple[list[dict[str, Any]], Any]:
    _ensure_partidos_tab(page, timeout_ms)
    labels = _read_all_round_labels(page, is_uefa=is_uefa, timeout_ms=timeout_ms)
    if not labels:
        raise RuntimeError("El desplegable de jornada/ronda no devolvió opciones.")

    seen_events: set[str] = set()
    all_matches: list[dict[str, Any]] = []
    team_ok: dict[str, int] = {}
    team_failed: set[str] = set()
    wait_after_round = min(2.0, max(0.55, settings.playwright_after_load_wait_seconds * 0.5))
    wait_s = settings.playwright_after_load_wait_seconds

    league_dir.mkdir(parents=True, exist_ok=True)
    _log(f"  Progreso JSON por jornada → {league_dir.as_posix()}/")

    for round_label in labels:
        _select_round_label(page, round_label, timeout_ms)
        _log(f"  Ronda/jornada: {round_label!r}")
        time.sleep(wait_after_round)

        if is_uefa:
            rnum, stage = _uefa_round_stage(round_label)
        else:
            rnum, stage = _domestic_round_stage(round_label)

        rows = _collect_match_rows_data(page)
        if len(rows) == 0 and _page_is_sofascore_seo_splash(page):
            page = _resolve_page_after_splash(
                page,
                context,
                target_url=tournament_url,
                timeout_ms=timeout_ms,
                wait_s=wait_s,
                hint="lista vacía + vista SEO",
            )
            _ensure_partidos_tab(page, timeout_ms)
            _select_round_label(page, round_label, timeout_ms)
            time.sleep(wait_after_round)
            rows = _collect_match_rows_data(page)
        _log(f"    Partidos en lista: {len(rows)}")

        round_fname = _round_progress_filename(round_label)
        round_path = league_dir / round_fname
        round_matches: list[dict[str, Any]] = []

        expected_eids = {str(r["event_id"]).strip() for r in rows if r.get("event_id")}
        if expected_eids and round_path.is_file():
            resume_map = _try_resume_round_from_json(
                round_path,
                season_id=season_id,
                round_label=round_label,
                expected_eids=expected_eids,
            )
            if resume_map is not None:
                _log(
                    f"    Omitiendo {round_fname}: JSON completo ({len(rows)} partido(s), mismos "
                    f"{_VERIFICADOR_YA_PROCESADO}).",
                )
                for row in rows:
                    eid = str(row["event_id"]).strip()
                    if eid in seen_events:
                        continue
                    seen_events.add(eid)
                    all_matches.append(resume_map[eid])
                continue
            try:
                round_path.unlink()
            except OSError:
                pass
            _unlink_liga_lista_completa_marker(league_dir)
            _log(f"    {round_fname} incompleto o desfasado; rescrapeando desde esta jornada.")

        for row in rows:
            eid = row["event_id"]
            if eid in seen_events:
                continue
            seen_events.add(eid)

            rec = _build_match_record(
                season_id=season_id,
                list_row=row,
                round_num=rnum,
                stage=stage,
                team_ok=team_ok,
                team_failed=team_failed,
            )

            if rec["status"] == "Finished":
                rec, page = _enrich_finished_match_on_detail_page(
                    page,
                    context=context,
                    list_row=row,
                    rec=rec,
                    tournament_url=tournament_url,
                    round_label=round_label,
                    timeout_ms=timeout_ms,
                    wait_after_round=wait_after_round,
                    wait_s=wait_s,
                )

            all_matches.append(rec)
            round_matches.append(rec)
            _write_json(
                round_path,
                {
                    "search": competition_search,
                    "season_id": season_id,
                    "round_label": round_label,
                    "matches": list(round_matches),
                },
            )
            _log(
                f"  ↳ {round_fname}: {len(round_matches)} partido(s) en esta jornada "
                f"({len(all_matches)} total competición)",
            )

    _write_liga_lista_completa_marker(league_dir)
    return all_matches, page


def _run_sofascore_statistics_historics_sync() -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    headless = settings.playwright_headless
    wait_s = settings.playwright_after_load_wait_seconds
    timeout_ms = settings.playwright_page_ready_timeout_ms
    tournament_url_re = re.compile(r".*/football/tournament/.*")

    _var_root()
    _log(
        f"Estadísticas-histórico: url={SOFASCORE_ES_URL!r} headless={headless} "
        f"timeout_ms={timeout_ms} competiciones={len(SOFASCORE_LEAGUE_TARGETS)}; "
        f"progreso → var/<liga>/jornada-N.json",
    )

    items: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        try:
            page = context.new_page()
            page.set_default_timeout(timeout_ms)

            _log("Navegando a home…")
            page.goto(SOFASCORE_ES_URL, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(wait_s)
            page = _resolve_page_after_splash(
                page,
                context,
                target_url=SOFASCORE_ES_URL,
                timeout_ms=timeout_ms,
                wait_s=wait_s,
                hint="home SofaScore",
            )
            page.locator("#search-input").wait_for(state="visible")

            n_targets = len(SOFASCORE_LEAGUE_TARGETS)
            for i, target in enumerate(SOFASCORE_LEAGUE_TARGETS):
                query = target["search"]
                is_uefa = _is_uefa_target(target)
                season_id = _STATISTICS_HISTORICS_SEASON_IDS[i]
                _log(f"[{i + 1}/{n_targets}] Buscando {query!r}… (season_id={season_id})")
                row: dict[str, Any] = {
                    "search": query,
                    "name": query,
                    "season_id": season_id,
                    "ok": False,
                    "url": None,
                    "link_href": None,
                    "link_text": None,
                    "error": None,
                    "fixtures_error": None,
                    "matches": [],
                }
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.2)
                    _type_search_query(page, query)
                    time.sleep(0.6)

                    link = _first_football_tournament_link(page)
                    link.wait_for(state="visible", timeout=timeout_ms)
                    href = link.get_attribute("href")
                    label = _link_label(link)
                    row["link_href"] = href
                    row["link_text"] = label or None

                    abs_tournament = _match_page_absolute_url(href or "")
                    slug_disk = _tournament_slug_from_url(abs_tournament)
                    league_dir_disk = _var_root() / slug_disk
                    cached = _try_load_matches_from_completed_league_dir(
                        league_dir_disk,
                        season_id=season_id,
                    )
                    if cached is not None:
                        row["ok"] = True
                        row["url"] = abs_tournament
                        row["matches"] = cached
                        _log(
                            f"  Liga ya en disco ({slug_disk!r}): omitiendo torneo y scrape "
                            f"({len(cached)} partido(s)). La siguiente competición no vuelve a pasar por esta.",
                        )
                    else:
                        link.click()
                        try:
                            page.wait_for_url(tournament_url_re, timeout=timeout_ms)
                        except Exception:
                            page.wait_for_function(
                                "() => window.location.href.includes('/football/tournament/')",
                                timeout=timeout_ms,
                            )
                        time.sleep(min(2.0, max(0.5, wait_s)))

                        tournament_url = page.url
                        page = _resolve_page_after_splash(
                            page,
                            context,
                            target_url=tournament_url,
                            timeout_ms=timeout_ms,
                            wait_s=wait_s,
                            hint="página torneo",
                        )
                        tournament_url = page.url
                        row["ok"] = True
                        row["url"] = tournament_url
                        _log(f"  → {tournament_url!r} ({label!r})")

                        slug = _tournament_slug_from_url(tournament_url)
                        league_dir = _var_root() / slug
                        try:
                            matches, page = _scrape_all_matches_for_competition(
                                page,
                                context=context,
                                season_id=season_id,
                                is_uefa=is_uefa,
                                timeout_ms=timeout_ms,
                                competition_search=query,
                                league_dir=league_dir,
                                tournament_url=tournament_url,
                            )
                            row["matches"] = matches
                            _log(
                                f"  Liga OK: {len(matches)} partidos; JSON en "
                                f"{league_dir.as_posix()}/",
                            )
                        except Exception as fx_exc:
                            row["fixtures_error"] = str(fx_exc)
                            _log(f"  Partidos/scrape: {fx_exc}")

                except Exception as exc:
                    row["error"] = str(exc)
                    _log(f"  error: {exc}")

                items.append(row)

                try:
                    page.goto(SOFASCORE_ES_URL, wait_until="domcontentloaded")
                    page.wait_for_load_state("load")
                    time.sleep(min(1.5, max(0.3, wait_s * 0.5)))
                    page = _resolve_page_after_splash(
                        page,
                        context,
                        target_url=SOFASCORE_ES_URL,
                        timeout_ms=timeout_ms,
                        wait_s=wait_s,
                        hint="home tras liga",
                    )
                    page.locator("#search-input").wait_for(state="visible")
                except Exception as nav_exc:
                    _log(f"  (volver a home falló: {nav_exc})")

            return {
                "url": page.url,
                "document_title": page.title(),
                "items": items,
            }
        finally:
            _log("Cerrando navegador.")
            context.close()
            browser.close()


async def run_sofascore_statistics_historics_flow() -> dict[str, Any]:
    return await asyncio.to_thread(_run_sofascore_statistics_historics_sync)

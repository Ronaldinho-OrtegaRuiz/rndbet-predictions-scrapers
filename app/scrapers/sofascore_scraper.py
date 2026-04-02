"""
Scraper para https://www.sofascore.com/es/ (Playwright): búsqueda, clasificación y sync a `teams`.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any, TypedDict
from urllib.parse import urljoin

from app.core.config import settings

SOFASCORE_ES_URL = "https://www.sofascore.com/es/"


class _SofaLeagueTarget(TypedDict):
    search: str
    inferred_country: str | None  # None → UEFA: país desde ficha del equipo


SOFASCORE_LEAGUE_TARGETS: tuple[_SofaLeagueTarget, ...] = (
    {"search": "Premier League", "inferred_country": "Inglaterra"},
    {"search": "La Liga", "inferred_country": "España"},
    {"search": "Serie A", "inferred_country": "Italia"},
    {"search": "Bundesliga", "inferred_country": "Alemania"},
    {"search": "Ligue 1", "inferred_country": "Francia"},
    {"search": "UEFA Champions League", "inferred_country": None},
    {"search": "UEFA Europa League", "inferred_country": None},
)

# Compat: solo strings (búsqueda)
SOFASCORE_LEAGUE_QUERIES: tuple[str, ...] = tuple(t["search"] for t in SOFASCORE_LEAGUE_TARGETS)

_LOG_PREFIX = "[sofascore]"


def _log(msg: str) -> None:
    print(f"{_LOG_PREFIX} {msg}", flush=True)


def _ensure_team_in_db(name: str, country: str | None) -> None:
    from app.db.supabase_client import get_supabase_client, get_supabase_service_client

    client = get_supabase_client()
    res = client.table("teams").select("name").eq("name", name).limit(1).execute()
    if (res.data or []):
        _log(f"DB: existe team={name!r}")
        return
    payload = {"name": name, "country": country}
    if settings.supabase_service_role_key:
        get_supabase_service_client().table("teams").insert(payload).execute()
        _log(f"DB: insert(team) via service_role team={name!r} country={country!r}")
        return
    try:
        client.table("teams").insert(payload).execute()
        _log(f"DB: insert(team) via anon team={name!r} country={country!r}")
    except Exception as exc:
        raise RuntimeError(
            f'No se pudo insertar en "teams". Si RLS está ON, define SUPABASE_SERVICE_ROLE_KEY. '
            f"Error: {exc}"
        ) from exc


def _extract_country_from_sofascore_team_page(page: Any, timeout_ms: int) -> str | None:
    """
    Vista de equipo: bloque cabecera con bandera /api/v1/country/XX/flag y
    <span class="textStyle_display.medium c_neutrals.nLv1">Chipre</span> (nombre localizado).
    """
    tmo = min(15_000, timeout_ms)
    try:
        flag = page.locator('img[src*="/api/v1/country/"][src*="/flag"]').first
        flag.wait_for(state="attached", timeout=tmo)
        parent = flag.locator("xpath=..")
        name_spans = parent.locator(
            'span[class*="textStyle_display.medium"][class*="c_neutrals.nLv1"]',
        )
        if name_spans.count() > 0:
            txt = (name_spans.first.inner_text(timeout=3000) or "").strip()
            if txt:
                return " ".join(txt.split())
        medium = parent.locator('span[class*="textStyle_display.medium"]')
        if medium.count() > 0:
            txt = (medium.first.inner_text(timeout=2000) or "").strip()
            if txt:
                return " ".join(txt.split())
    except Exception:
        pass
    try:
        alt = (
            page.locator('img[src*="/api/v1/country/"][src*="/flag"]').first.get_attribute("alt")
            or ""
        ).strip()
        if alt:
            return " ".join(alt.split())
    except Exception:
        pass
    try:
        link = page.locator('a[href*="/football/country/"]').first
        link.wait_for(state="visible", timeout=min(5000, tmo))
        txt = (link.inner_text(timeout=3000) or "").strip()
        if txt:
            return " ".join(txt.split())
    except Exception:
        pass
    return None


def _type_search_query(page: Any, query: str) -> None:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    inp = page.locator("#search-input")
    inp.wait_for(state="visible")
    inp.scroll_into_view_if_needed()
    inp.click()
    time.sleep(0.15)
    try:
        inp.fill("", force=True)
    except PlaywrightTimeoutError:
        inp.fill("")
    inp.fill(query, force=True)
    try:
        page.evaluate(
            """(q) => {
                const el = document.querySelector('#search-input');
                if (!el) return;
                el.value = q;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            query,
        )
    except Exception:
        pass


def _first_football_tournament_link(page: Any) -> Any:
    root = page.locator(".beautiful-scrollbar__container .beautiful-scrollbar__content")
    return root.locator("a[href*='/football/tournament/']").first


def _link_label(link: Any) -> str:
    try:
        alt = link.locator("img[alt]").first.get_attribute("alt")
        if alt and alt.strip():
            return alt.strip()
    except Exception:
        pass
    try:
        span = link.locator('span[class*="textStyle_body"]').first
        t = span.inner_text(timeout=2000)
        if t and t.strip():
            return t.strip()
    except Exception:
        pass
    try:
        t = link.inner_text(timeout=2000)
        return (t or "").strip() or ""
    except Exception:
        return ""


def _ensure_standings_tab(page: Any, timeout_ms: int) -> None:
    standings = page.locator('[data-testid="tab-standings"]')
    standings.wait_for(state="visible", timeout=min(20_000, timeout_ms))
    try:
        if standings.get_attribute("aria-selected") != "true":
            standings.click()
            time.sleep(0.4)
    except Exception:
        standings.click()
        time.sleep(0.4)

    total = page.locator("#tabpanel-standings [data-testid='tab-total']")
    if total.count() == 0:
        total = page.locator('[data-testid="tab-total"]').first
    try:
        if total.count() > 0 and total.get_attribute("aria-selected") != "true":
            total.click()
            time.sleep(0.45)
    except Exception:
        pass


def _standings_row_links(page: Any) -> Any:
    scoped = page.locator("#tabpanel-standings #tabpanel-total a[href*='/football/team/']")
    if scoped.count() > 0:
        return scoped
    return page.locator("#tabpanel-total a[href*='/football/team/']")


def _team_display_name_from_row(link: Any) -> str:
    """
    Nombre visible en la tabla (span truncado), no el slug de la URL.
    """
    try:
        span = link.locator('div.flex-g_1.ov_auto span[class*="textStyle_table"]').first
        t = (span.inner_text(timeout=3000) or "").strip()
        if t:
            return " ".join(t.split())
    except Exception:
        pass
    try:
        alt = link.locator("img[alt]").first.get_attribute("alt")
        if alt and alt.strip():
            return alt.strip()
    except Exception:
        pass
    return ""


def _scrape_standings_teams(page: Any, timeout_ms: int) -> list[tuple[int, str, str]]:
    _ensure_standings_tab(page, timeout_ms)
    links = _standings_row_links(page)
    links.first.wait_for(state="visible", timeout=min(25_000, timeout_ms))
    n = links.count()
    out: list[tuple[int, str, str]] = []
    for i in range(n):
        loc = links.nth(i)
        name = _team_display_name_from_row(loc)
        href = (loc.get_attribute("href") or "").strip()
        if not name:
            continue
        out.append((len(out) + 1, name, href))
    return out


def _sync_teams_for_league(
    page: Any,
    tournament_url: str,
    teams: list[tuple[int, str, str]],
    inferred_country: str | None,
    timeout_ms: int,
) -> None:
    from app.db.supabase_client import get_supabase_client

    client = get_supabase_client()
    for rank, team_name, href in teams:
        try:
            if inferred_country:
                _ensure_team_in_db(team_name, inferred_country)
                continue
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
                _log(f"DB: sin href team={team_name!r} rank={rank}")
                continue
            team_url = urljoin(SOFASCORE_ES_URL, href)
            _log(f"TEAM {rank}: ficha país → {team_url!r}")
            page.goto(team_url, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(0.5)
            country = _extract_country_from_sofascore_team_page(page, timeout_ms)
            _ensure_team_in_db(team_name, country)
            page.goto(tournament_url, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(0.45)
            _ensure_standings_tab(page, timeout_ms)
        except Exception as exc:
            _log(f"DB: error team={team_name!r} rank={rank}: {exc}")


def _run_sofascore_league_searches_sync() -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    headless = settings.playwright_headless
    wait_s = settings.playwright_after_load_wait_seconds
    timeout_ms = settings.playwright_page_ready_timeout_ms
    tournament_url_re = re.compile(r".*/football/tournament/.*")

    _log(
        f"Ligas+clasificación+DB: url={SOFASCORE_ES_URL!r} headless={headless} "
        f"timeout_ms={timeout_ms} ligas={len(SOFASCORE_LEAGUE_TARGETS)}",
    )

    items: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            page.set_default_timeout(timeout_ms)
            try:
                page.set_viewport_size({"width": 1280, "height": 900})
            except Exception:
                pass

            _log("Navegando a home…")
            page.goto(SOFASCORE_ES_URL, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(wait_s)
            page.locator("#search-input").wait_for(state="visible")

            for idx, target in enumerate(SOFASCORE_LEAGUE_TARGETS, start=1):
                query = target["search"]
                _log(f"[{idx}/{len(SOFASCORE_LEAGUE_TARGETS)}] Buscando {query!r}…")
                row: dict[str, Any] = {
                    "search": query,
                    "name": query,
                    "ok": False,
                    "url": None,
                    "link_href": None,
                    "link_text": None,
                    "error": None,
                    "standings_teams": [],
                    "standings_error": None,
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
                    row["ok"] = True
                    row["url"] = tournament_url
                    _log(f"  → {tournament_url!r} ({label!r})")

                    try:
                        teams = _scrape_standings_teams(page, timeout_ms)
                        row["standings_teams"] = [t[1] for t in teams]
                        _log(f"  Clasificación: {len(teams)} equipos")
                        for r, nm, _ in teams:
                            _log(f"  TEAM {r}: {nm}")
                        _sync_teams_for_league(
                            page,
                            tournament_url,
                            teams,
                            target["inferred_country"],
                            timeout_ms,
                        )
                    except Exception as st_exc:
                        row["standings_error"] = str(st_exc)
                        _log(f"  Clasificación/DB: {st_exc}")

                except Exception as exc:
                    row["error"] = str(exc)
                    _log(f"  error: {exc}")

                items.append(row)

                try:
                    page.goto(SOFASCORE_ES_URL, wait_until="domcontentloaded")
                    page.wait_for_load_state("load")
                    time.sleep(min(1.5, max(0.3, wait_s * 0.5)))
                    page.locator("#search-input").wait_for(state="visible")
                except Exception as nav_exc:
                    _log(f"  (volver a home falló: {nav_exc})")

            final_url = page.url
            doc_title = page.title()
            return {
                "url": final_url,
                "document_title": doc_title,
                "items": items,
            }
        finally:
            _log("Cerrando navegador.")
            browser.close()


async def run_sofascore_league_flow() -> dict[str, Any]:
    return await asyncio.to_thread(_run_sofascore_league_searches_sync)

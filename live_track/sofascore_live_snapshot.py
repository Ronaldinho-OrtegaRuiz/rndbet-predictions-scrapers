"""
Un tick Playwright por partido: torneo → jornada/ronda → fila → ficha (timeline + estadísticas)
y armado del JSON del contrato `BackendLiveMatchSnapshot`.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from live_track.backend_snapshot import (
    BackendLiveMatchSnapshot,
    LiveEventRow,
    LiveTeamStatRow,
    dump_snapshot_for_http,
)
from live_track.queue_bus import MatchLookupWorkItem

log = logging.getLogger("live_track.snapshot")


def _find_league_target_index(competition: str) -> int | None:
    import app.scrapers.sofascore_statistics_historics as sh

    q = sh._norm_ws(competition).lower()
    for i, t in enumerate(sh.SOFASCORE_LEAGUE_TARGETS):
        if sh._norm_ws(t["search"]).lower() == q:
            return i
    for i, t in enumerate(sh.SOFASCORE_LEAGUE_TARGETS):
        sn = sh._norm_ws(t["search"]).lower()
        if q in sn or sn in q:
            return i
    return None


def _resolve_round_label(item: MatchLookupWorkItem) -> str | None:
    if item.round_label and item.round_label.strip():
        return item.round_label.strip()
    if item.jornada is not None:
        return f"Jornada {item.jornada}"
    if item.fase and item.fase.strip():
        return item.fase.strip()
    return None


def _read_live_fields_from_detail_page(page: Any) -> dict[str, Any]:
    """Heurística sobre el texto de `main` (SofaScore cambia DOM; se refina con el tiempo)."""
    try:
        text = page.locator("main").first.inner_text(timeout=15_000) or ""
    except Exception:
        text = ""
    out: dict[str, Any] = {}
    if re.search(r"(?i)\b(en directo|en vivo|live)\b", text):
        out["status"] = "LIVE"
    elif re.search(r"(?i)\b(finalizado|finished|\bft\b)\b", text):
        out["status"] = "FINISHED"
    elif re.search(r"(?i)(aplazad|pospuest|postponed)", text):
        out["status"] = "POSPONED"
    m = re.search(r"(?i)\b(\d{1,3})\s*\+\s*(\d{1,2})\b", text)
    if m:
        out["current_minute"] = int(m.group(1))
        out["added_time"] = int(m.group(2))
    else:
        m2 = re.search(r"(?<![\d])(\d{1,3})\s*['′′](?!\s*[-–])", text)
        if m2:
            mg = int(m2.group(1))
            if 0 <= mg <= 130:
                out["current_minute"] = mg
                out.setdefault("added_time", 0)
    return out


def scrape_backend_snapshot_sync(item: MatchLookupWorkItem) -> dict[str, Any] | None:
    """Playwright sync: devuelve dict listo para POST o None si no se pudo armar."""
    import app.scrapers.sofascore_statistics_historics as sh
    from app.core.config import settings
    from playwright.sync_api import sync_playwright

    if item.match_id is None:
        log.warning("live snapshot: sin match_id, abort")
        return None

    rlabel = _resolve_round_label(item)
    if not rlabel:
        log.error(
            "live snapshot: falta round_label / jornada / fase para match_id=%s",
            item.match_id,
        )
        return None

    idx = _find_league_target_index(item.competicion)
    if idx is None:
        log.error("live snapshot: competición no mapeada %r", item.competicion)
        return None

    season_id = sh._STATISTICS_HISTORICS_SEASON_IDS[idx]
    target = sh.SOFASCORE_LEAGUE_TARGETS[idx]
    is_uefa = sh._is_uefa_target(target)
    if is_uefa:
        rnum, stage = sh._uefa_round_stage(rlabel)
    else:
        rnum, stage = sh._domestic_round_stage(rlabel)

    headless = settings.playwright_headless
    wait_s = settings.playwright_after_load_wait_seconds
    timeout_ms = settings.playwright_page_ready_timeout_ms
    wait_after_round = 1.1

    team_ok: dict[str, int] = {}
    team_failed: set[str] = set()
    if item.home_team_id is not None:
        team_ok[sh._norm_ws(item.equipo_local)] = item.home_team_id
    if item.away_team_id is not None:
        team_ok[sh._norm_ws(item.equipo_visitante)] = item.away_team_id

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        try:
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(sh.SOFASCORE_ES_URL, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(wait_s)
            page = sh._resolve_page_after_splash(
                page,
                context,
                target_url=sh.SOFASCORE_ES_URL,
                timeout_ms=timeout_ms,
                wait_s=wait_s,
                hint="home live-track",
            )
            page.locator("#search-input").wait_for(state="visible")
            page.keyboard.press("Escape")
            time.sleep(0.2)
            sh._type_search_query(page, target["search"])
            time.sleep(0.75)
            link = sh._first_football_tournament_link(page)
            link.wait_for(state="visible", timeout=timeout_ms)
            href = link.get_attribute("href") or ""
            tournament_url = sh._match_page_absolute_url(href)
            page.goto(tournament_url, wait_until="domcontentloaded")
            page.wait_for_load_state("load")
            time.sleep(min(1.4, max(0.45, wait_s * 0.45)))
            page = sh._resolve_page_after_splash(
                page,
                context,
                target_url=tournament_url,
                timeout_ms=timeout_ms,
                wait_s=wait_s,
                hint="torneo live-track",
            )
            sh._ensure_partidos_tab(page, timeout_ms)
            sh._select_round_label(page, rlabel, timeout_ms)
            time.sleep(wait_after_round)

            rows = sh._collect_match_rows_data(page)
            list_row: dict[str, Any] | None = None
            for row in rows:
                hn = row.get("home_team_name")
                an = row.get("away_team_name")
                if sh._norm_ws(hn or "") == sh._norm_ws(item.equipo_local) and sh._norm_ws(
                    an or ""
                ) == sh._norm_ws(item.equipo_visitante):
                    list_row = row
                    break
            if list_row is None:
                log.warning(
                    "live snapshot: sin fila en lista %s vs %s (%s)",
                    item.equipo_local,
                    item.equipo_visitante,
                    rlabel,
                )
                return None

            rec = sh._build_match_record(
                season_id=season_id,
                list_row=list_row,
                round_num=rnum,
                stage=stage,
                team_ok=team_ok,
                team_failed=team_failed,
            )
            if item.home_team_id is not None:
                rec["home_team_id"] = item.home_team_id
            if item.away_team_id is not None:
                rec["away_team_id"] = item.away_team_id

            rec, page = sh._enrich_finished_match_on_detail_page(
                page,
                context=context,
                list_row=list_row,
                rec=rec,
                tournament_url=tournament_url,
                round_label=rlabel,
                timeout_ms=timeout_ms,
                wait_after_round=wait_after_round,
                wait_s=wait_s,
            )

            live_bits = _read_live_fields_from_detail_page(page)
            status = live_bits.get("status") or rec.get("status") or "UNKNOWN"
            if isinstance(status, str):
                status = status.strip().upper()
            cur_min = live_bits.get("current_minute")
            if cur_min is None:
                cur_min = rec.get("current_minute")
            added = live_bits.get("added_time", rec.get("added_time"))
            if added is None:
                added = 0

            team_stats: list[LiveTeamStatRow] = []
            for s in rec.get("team_match_stats") or []:
                if not isinstance(s, dict):
                    continue
                tid = s.get("team_id")
                if tid is None:
                    continue
                team_stats.append(
                    LiveTeamStatRow(
                        team_id=int(tid),
                        is_home=bool(s.get("is_home")),
                        goals=s.get("goals"),
                        possession=s.get("possession"),
                        shots=s.get("shots"),
                        shots_on_target=s.get("shots_on_target"),
                        saves=s.get("saves"),
                        yellow_cards=s.get("yellow_cards"),
                        red_cards=s.get("red_cards"),
                        corners=s.get("corners"),
                        fouls=s.get("fouls"),
                        offsides=s.get("offsides"),
                    )
                )

            events: list[LiveEventRow] = []
            for e in rec.get("match_events") or []:
                if not isinstance(e, dict):
                    continue
                events.append(
                    LiveEventRow(
                        minute=e.get("minute"),
                        event_type=e.get("event_type"),
                        team_id=e.get("team_id"),
                        player_name=e.get("name"),
                    )
                )

            snap = BackendLiveMatchSnapshot(
                match_id=int(item.match_id),
                status=str(status),
                home_score=rec.get("home_score"),
                away_score=rec.get("away_score"),
                current_minute=int(cur_min) if isinstance(cur_min, (int, float)) else cur_min,
                added_time=int(added) if isinstance(added, (int, float)) else 0,
                team_stats=team_stats,
                events=events,
            )
            return dump_snapshot_for_http(snap)
        except Exception as exc:
            log.exception("live snapshot falló match_id=%s: %s", item.match_id, exc)
            return None
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

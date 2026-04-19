"""
Live-track SofaScore: **kickoff → lista activa** + **round-robin** infinito.

1. `POST /api/live-track/ingest`: persiste JSON; **por cada partido** programa una tarea que a la
   **hora de `fecha` (kickoff)** lo **incorpora** a la lista del round-robin (si no estaba).
   Re-ingest: cancela timers pendientes del ingest anterior y vuelve a programar.
2. `run_sofascore_round_robin_loop`: bucle **infinito** 1→N→1→N… solo sobre los partidos **ya
   incorporados** (varios pueden solaparse en el tiempo cuando van entrando por kickoff).
3. Lista vacía → espera (`robin_empty_list_sleep_seconds`).

Stats en vivo: mismo `_process_sofascore_tick` más adelante.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone

from live_track.schemas import LiveTrackPayload
from live_track.settings import live_track_settings

log = logging.getLogger("live_track.robin")

_rr_lock = asyncio.Lock()
_rr_items: list[MatchLookupWorkItem] = []
_scheduled: list[asyncio.Task[None]] = []


@dataclass(frozen=True, slots=True)
class MatchLookupWorkItem:
    """Un partido (entra al round-robin al cumplirse el kickoff programado)."""

    fecha_referencia: str
    match_id: int | None
    kickoff: datetime
    competicion: str
    equipo_local: str
    equipo_visitante: str
    jornada: int | None = None
    fase: str | None = None
    grupo: str | None = None


def _kickoff_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _robin_dedupe_key(item: MatchLookupWorkItem) -> tuple:
    if item.match_id is not None:
        return ("id", item.match_id)
    return (
        "names",
        item.competicion,
        item.equipo_local,
        item.equipo_visitante,
        item.kickoff.isoformat(),
    )


def _items_from_payload(payload: LiveTrackPayload) -> list[MatchLookupWorkItem]:
    seen: set[tuple[int | None, str, str, str, str]] = set()
    out: list[MatchLookupWorkItem] = []
    for p in payload.partidos:
        kickoff = _kickoff_utc(p.fecha)
        key = (p.match_id, p.competicion, p.equipo_local, p.equipo_visitante, kickoff.isoformat())
        if key in seen:
            continue
        seen.add(key)
        out.append(
            MatchLookupWorkItem(
                fecha_referencia=payload.fecha_referencia,
                match_id=p.match_id,
                kickoff=kickoff,
                competicion=p.competicion,
                equipo_local=p.equipo_local,
                equipo_visitante=p.equipo_visitante,
                jornada=p.jornada,
                fase=p.fase,
                grupo=p.grupo,
            )
        )
    return out


async def add_to_round_robin_if_absent(item: MatchLookupWorkItem) -> None:
    """Añade el partido a la ronda si no estaba (p. ej. tras esperar hasta kickoff)."""
    key = _robin_dedupe_key(item)
    async with _rr_lock:
        existing = {_robin_dedupe_key(x) for x in _rr_items}
        if key in existing:
            log.debug("round-robin: ya estaba match_id=%s dedupe=%s", item.match_id, key)
            return
        _rr_items.append(item)
    log.info(
        "round-robin: +partido kickoff=%s match_id=%s %s vs %s (total=%s)",
        item.kickoff.isoformat(),
        item.match_id,
        item.equipo_local,
        item.equipo_visitante,
        len(_rr_items),
    )


def schedule_kickoffs_from_payload(payload: LiveTrackPayload) -> None:
    """Programa incorporación al round-robin a la hora de cada `fecha` (kickoff ≤ ahora → ya)."""
    loop = asyncio.get_running_loop()
    now = datetime.now(timezone.utc)
    items = _items_from_payload(payload)
    for it in items:
        delay = (it.kickoff - now).total_seconds()

        async def _at_kickoff(d: float, item: MatchLookupWorkItem = it) -> None:
            if d > 0:
                await asyncio.sleep(d)
            await add_to_round_robin_if_absent(item)

        t = loop.create_task(_at_kickoff(delay))
        _scheduled.append(t)
        log.info(
            "kickoff programado en %.0fs → round-robin match_id=%s %s vs %s",
            max(0.0, delay),
            it.match_id,
            it.equipo_local,
            it.equipo_visitante,
        )


async def apply_live_track_ingest_side_effects(payload: LiveTrackPayload) -> None:
    """Tras persistir: cancela timers viejos, vacía la ronda activa y reprograma kickoffs del payload."""
    await cancel_pending_scheduled_async()
    async with _rr_lock:
        _rr_items.clear()
    schedule_kickoffs_from_payload(payload)


async def cancel_pending_scheduled_async() -> None:
    from asyncio import CancelledError

    snap = _scheduled[:]
    _scheduled.clear()
    for t in snap:
        if not t.done():
            t.cancel()
        with suppress(CancelledError):
            await t


async def run_sofascore_round_robin_loop() -> None:
    pause = live_track_settings.robin_pause_between_matches_seconds
    empty_wait = live_track_settings.robin_empty_list_sleep_seconds
    log.info(
        "live-track round-robin: iniciado (pause=%ss empty_wait=%ss; partidos entran por kickoff)",
        pause,
        empty_wait,
    )
    while True:
        async with _rr_lock:
            snapshot = tuple(_rr_items)
        if not snapshot:
            await asyncio.sleep(empty_wait)
            continue
        for item in snapshot:
            try:
                await _process_sofascore_tick(item)
            except Exception:
                log.exception("fallo tick SofaScore match_id=%s", item.match_id)
            await asyncio.sleep(pause)


async def _process_sofascore_tick(item: MatchLookupWorkItem) -> None:
    """Una visita a SofaScore para este partido (stub). Más adelante: stats en el mismo sitio."""
    log.debug(
        "SofaScore tick match_id=%s %s vs %s (%s)",
        item.match_id,
        item.equipo_local,
        item.equipo_visitante,
        item.competicion,
    )

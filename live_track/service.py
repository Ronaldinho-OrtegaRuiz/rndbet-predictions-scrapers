"""
Orquestación live-track.

Flujo deseado: obtener una **cola de partidos** (cada uno con competición + jornada + equipos),
y por **cada partido** hacer **una** navegación SofaScore (no un barrido de todas las ligas).
`KNOWN_COMPETITION_NAMES` valida el nombre de competición del slot; `grupo` en el modelo no se usa
en el flujo actual.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from live_track.constants import KNOWN_COMPETITION_NAMES
from live_track.schemas import LiveTrackPayload, PartidoPendiente
from live_track.settings import live_track_settings
from live_track.slots import PendingMatchSlot, list_pending_match_slots_for_date

log = logging.getLogger("live_track.service")


def resolve_fecha_referencia(explicit: str | None) -> str:
    tz = ZoneInfo(live_track_settings.time_zone)
    if explicit and explicit.strip():
        # Validación estricta vía Pydantic en el request; aquí solo normalizamos.
        return explicit.strip()[:10]
    today = datetime.now(tz).date()
    return today.isoformat()


async def resolve_one_match_on_sofascore(
    slot: PendingMatchSlot,
    *,
    fecha_referencia: str,
) -> PartidoPendiente | None:
    """
    Localiza **un** partido en SofaScore: torneo `slot['competicion']`, select de jornada,
    fila que coincide con local/visitante (y fecha si hace falta).

    TODO: Playwright (una sesión/navegación por slot o reutilizar contexto según convenga).
    """
    _ = fecha_referencia
    if slot["competicion"] not in KNOWN_COMPETITION_NAMES:
        log.warning("competicion desconocida (no está en catálogo): %r", slot["competicion"])
    return None


async def collect_partidos_pendientes(
    *,
    fecha_referencia: str,
    repo_root: Path,
) -> list[PartidoPendiente]:
    slots = await list_pending_match_slots_for_date(fecha_referencia, repo_root)
    log.info(
        "live-track: fecha_referencia=%s partidos_en_cola=%s (cada uno → una búsqueda SofaScore)",
        fecha_referencia,
        len(slots),
    )
    out: list[PartidoPendiente] = []
    for i, slot in enumerate(slots):
        log.debug("slot[%s] %s vs %s (%s)", i, slot["equipo_local"], slot["equipo_visitante"], slot["competicion"])
        row = await resolve_one_match_on_sofascore(slot, fecha_referencia=fecha_referencia)
        if row is not None:
            out.append(row)
    return out


async def build_and_persist(
    *,
    fecha_referencia: str | None,
    repo_root: Path,
) -> tuple[LiveTrackPayload, Path]:
    fr = resolve_fecha_referencia(fecha_referencia)
    partidos = await collect_partidos_pendientes(fecha_referencia=fr, repo_root=repo_root)
    payload = LiveTrackPayload(fecha_referencia=fr, partidos=partidos)
    from live_track.storage import persist_live_track

    path = persist_live_track(repo_root, payload)
    return payload, path

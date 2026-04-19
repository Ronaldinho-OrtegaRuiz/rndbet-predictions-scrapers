"""
Cola de trabajo live-track: **un slot = un partido** a localizar en SofaScore.

No se recorren las 7 competiciones enteras: se itera **partido por partido**; cada iteración
abre la competición de ese slot, elige jornada en el select y cruza por equipos.
"""

from __future__ import annotations

from pathlib import Path
from typing import NotRequired, TypedDict


class PendingMatchSlot(TypedDict):
    """Un partido concreto a buscar (origen típico: BD / backend para esa `fecha_referencia`)."""

    competicion: str
    equipo_local: str
    equipo_visitante: str
    jornada: NotRequired[int]
    fase: NotRequired[str]
    grupo: NotRequired[str]


async def list_pending_match_slots_for_date(
    fecha_referencia: str,
    repo_root: Path,
) -> list[PendingMatchSlot]:
    """
    Devuelve la lista de partidos pendientes de resolver ese día.

    TODO: consultar BD o API del backend (matches del día + estado). Mientras: lista vacía.
    """
    _ = (fecha_referencia, repo_root)
    return []

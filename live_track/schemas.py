"""
Contrato JSON (similar a @JsonInclude(NON_NULL): no serializar claves con valor None).

Origen final en BD (cuando exista integración): matches, competitions, teams, matches.round/stage/group.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PartidoPendiente(BaseModel):
    model_config = ConfigDict(extra="forbid")

    match_id: int | None = Field(default=None, description="matches.id (omitir si aún no hay BD).")
    fecha: datetime = Field(description="Instante del partido (ISO-8601 con zona).")
    competicion: str
    equipo_local: str
    equipo_visitante: str
    jornada: int | None = None
    fase: str | None = None
    grupo: str | None = Field(default=None, description='matches."group"; no usado en flujo actual doméstico/UEFA list.')


class LiveTrackPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fecha_referencia: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$", description="YYYY-MM-DD en LIVE_TRACK_TIME_ZONE.")
    partidos: list[PartidoPendiente] = Field(default_factory=list)


class LiveTrackRunRequest(BaseModel):
    """Cuerpo opcional para disparar la generación del JSON."""

    model_config = ConfigDict(extra="forbid")

    fecha_referencia: str | None = Field(
        default=None,
        description="YYYY-MM-DD; si null, usa hoy en LIVE_TRACK_TIME_ZONE.",
    )


def dump_live_track_json(payload: LiveTrackPayload) -> dict[str, Any]:
    """Serialización con omisión de nulls (equivalente práctico a NON_NULL)."""
    return payload.model_dump(mode="json", exclude_none=True)

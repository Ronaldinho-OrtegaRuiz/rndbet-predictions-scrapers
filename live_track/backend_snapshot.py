"""Contrato del snapshot en vivo que se envía al backend (tal cual lo definiste)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LiveTeamStatRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    team_id: int
    is_home: bool
    goals: int | None = None
    possession: float | None = None
    shots: int | None = None
    shots_on_target: int | None = None
    saves: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    corners: int | None = None
    fouls: int | None = None
    offsides: int | None = None


class LiveEventRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    minute: int | None = None
    event_type: str | None = None
    team_id: int | None = None
    player_name: str | None = None


class BackendLiveMatchSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    match_id: int
    status: str
    home_score: int | None = None
    away_score: int | None = None
    current_minute: int | None = None
    added_time: int | None = None
    team_stats: list[LiveTeamStatRow] = Field(default_factory=list)
    events: list[LiveEventRow] = Field(default_factory=list)


def dump_snapshot_for_http(snapshot: BackendLiveMatchSnapshot) -> dict[str, Any]:
    return snapshot.model_dump(mode="json", exclude_none=True)

"""
Formas de fila para **insert/upsert** contra tu esquema Postgres/Supabase.

Uso típico antes de `.insert()` / `.upsert()`:

    payload = MatchInsert(...).model_dump(mode="json", exclude_none=True)

Notas
-----
- Columnas `TIMESTAMP WITH TIME ZONE` en BD ↔ `datetime` con `tzinfo` en Python.
- Cuando agregues IDs externos (recomendado: `sofascore_event_id`, etc.), extendé estos
  modelos o creá variantes `*Upsert` con esa clave para `on_conflict`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CompetitionRow(BaseModel):
    """→ tabla `competitions`"""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str | None = None
    format: str | None = None


class SeasonRow(BaseModel):
    """→ tabla `seasons`"""

    model_config = ConfigDict(extra="forbid")

    competition_id: int
    year: str


class TeamRow(BaseModel):
    """→ tabla `teams`"""

    model_config = ConfigDict(extra="forbid")

    name: str
    country: str | None = None


class MatchRow(BaseModel):
    """→ tabla `matches` (`date` / `last_updated` = timestamptz en BD)."""

    model_config = ConfigDict(extra="forbid")

    season_id: int
    date: datetime
    home_team_id: int
    away_team_id: int
    home_score: int | None = None
    away_score: int | None = None
    status: str | None = None
    round: int | None = None
    stage: str | None = None
    group: str | None = Field(default=None, description='Columna "group" en SQL.')
    current_minute: int | None = None
    added_time: int | None = None
    last_updated: datetime | None = None


class TeamMatchStatsRow(BaseModel):
    """→ tabla `team_match_stats`"""

    model_config = ConfigDict(extra="forbid")

    match_id: int
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


class PlayerRow(BaseModel):
    """→ tabla `players`"""

    model_config = ConfigDict(extra="forbid")

    name: str


class MatchEventRow(BaseModel):
    """→ tabla `match_events` (`created_at` timestamptz; default en BD opcional)."""

    model_config = ConfigDict(extra="forbid")

    match_id: int
    team_id: int | None = None
    player_id: int | None = None
    minute: int | None = None
    event_type: str | None = None
    extra_data: dict[str, Any] | None = None
    created_at: datetime | None = None


class PredictionRow(BaseModel):
    """→ tabla `predictions` (no sale del scraper; sirve para el pipeline completo)."""

    model_config = ConfigDict(extra="forbid")

    match_id: int
    created_at: datetime | None = None
    expected_home_goals: float | None = None
    expected_away_goals: float | None = None
    prob_home_win: float | None = None
    prob_draw: float | None = None
    prob_away_win: float | None = None
    predicted_shots: int | None = None
    predicted_shots_on_target: int | None = None
    predicted_saves: int | None = None
    predicted_yellow_cards: int | None = None
    predicted_red_cards: int | None = None
    predicted_corners: int | None = None
    predicted_fouls: int | None = None
    predicted_offsides: int | None = None


class PredictionEvaluationRow(BaseModel):
    """→ tabla `prediction_evaluations`"""

    model_config = ConfigDict(extra="forbid")

    prediction_id: int
    actual_home_goals: int | None = None
    actual_away_goals: int | None = None
    actual_shots: int | None = None
    actual_shots_on_target: int | None = None
    actual_saves: int | None = None
    actual_yellow_cards: int | None = None
    actual_red_cards: int | None = None
    actual_corners: int | None = None
    actual_fouls: int | None = None
    actual_offsides: int | None = None
    error_goals: float | None = None
    error_shots: float | None = None
    error_corners: float | None = None
    error_cards: float | None = None
    correct_result: bool | None = None

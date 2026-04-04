"""Contratos de dominio: modos de scraper y filas listas para Supabase."""

from app.domain.jobs import ScraperJobKind
from app.domain.rows import (
    CompetitionRow,
    MatchEventRow,
    MatchRow,
    PlayerRow,
    PredictionEvaluationRow,
    PredictionRow,
    SeasonRow,
    TeamMatchStatsRow,
    TeamRow,
)

__all__ = [
    "ScraperJobKind",
    "CompetitionRow",
    "SeasonRow",
    "TeamRow",
    "MatchRow",
    "TeamMatchStatsRow",
    "PlayerRow",
    "MatchEventRow",
    "PredictionRow",
    "PredictionEvaluationRow",
]

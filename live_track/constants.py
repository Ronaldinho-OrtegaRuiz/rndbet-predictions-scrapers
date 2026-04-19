"""
Nombres de competición **válidos / de búsqueda** (texto exacto como en SofaScore).

No implica un bucle “abrir las 7 ligas y scrapear todo”: el scraper va **partido por partido**;
este catálogo sirve para validar o mapear el `competicion` de cada slot (un partido concreto).
"""

from __future__ import annotations

from typing import TypedDict


class CompetitionName(TypedDict):
    name: str


DEFAULT_COMPETITIONS: tuple[CompetitionName, ...] = (  # alias semántico: catálogo de nombres
    {"name": "Premier League"},
    {"name": "La Liga"},
    {"name": "Serie A"},
    {"name": "Ligue 1"},
    {"name": "Bundesliga"},
    {"name": "UEFA Champions League"},
    {"name": "UEFA Europa League"},
)

KNOWN_COMPETITION_NAMES: frozenset[str] = frozenset(x["name"] for x in DEFAULT_COMPETITIONS)

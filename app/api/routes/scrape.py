from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.scrapers.sofascore_scraper import SOFASCORE_ES_URL, run_sofascore_league_flow
from app.scrapers.sofascore_statistics_historics import run_sofascore_statistics_historics_flow

try:
    from app.scrapers.sofascore_statistics_historics_2hilos import (
        run_sofascore_statistics_historics_2hilos_flow,
    )
except ImportError:
    run_sofascore_statistics_historics_2hilos_flow = None  # type: ignore[misc, assignment]

router = APIRouter()


class SofaScoreLeagueItem(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "search": "Premier League",
                "name": "Premier League",
                "ok": True,
                "url": "https://www.sofascore.com/es/football/tournament/england/premier-league/17",
                "link_href": "/es/football/tournament/england/premier-league/17",
                "link_text": "Premier League",
                "error": None,
                "standings_teams": ["Arsenal", "Liverpool"],
                "standings_error": None,
            }
        }
    )

    search: str = Field(description="Texto enviado al buscador global (#search-input).")
    name: str = Field(description="Mismo que search; etiqueta legible de la competición.")
    ok: bool
    url: str | None = Field(
        default=None,
        description="URL de la página tras hacer clic en la primera opción de torneo.",
    )
    link_href: str | None = Field(default=None, description="href del primer enlace de torneo elegido.")
    link_text: str | None = Field(default=None, description="Texto visible (p. ej. alt de la imagen).")
    error: str | None = None
    standings_teams: list[str] = Field(
        default_factory=list,
        description="Nombres en orden de clasificación (texto del span en la fila, no el slug de la URL).",
    )
    standings_error: str | None = Field(
        default=None,
        description="Error al leer clasificación o sincronizar equipos con Supabase.",
    )


class SofaScoreStatisticsHistoricsItem(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "search": "La Liga",
                "name": "La Liga",
                "season_id": 2,
                "ok": True,
                "url": "https://www.sofascore.com/es/football/tournament/spain/laliga/8",
                "link_href": None,
                "link_text": None,
                "error": None,
                "fixtures_error": None,
                "matches": [],
            }
        }
    )

    search: str
    name: str
    season_id: int = Field(
        description=(
            "ID fijo por competición en el JSON: Premier=1, La Liga=2, Serie A=3, Ligue 1=4, "
            "Bundesliga=5, Champions=6, Europa=7 (orden de scrape sigue la lista global; 4 y 5 cruzados)."
        ),
    )
    ok: bool
    url: str | None = None
    link_href: str | None = None
    link_text: str | None = None
    error: str | None = None
    fixtures_error: str | None = Field(
        default=None,
        description="Error al recorrer Partidos, el desplegable de jornadas o escribir JSON de progreso.",
    )
    matches: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Objetos alineados con spec (date ISO timestamptz America/Bogotá, status, scores, …).",
    )


class SofaScoreStatisticsHistoricsResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_page": "https://www.sofascore.com/es/",
                "page_url": "https://www.sofascore.com/es/",
                "document_title": "SofaScore",
                "items": [],
            }
        }
    )

    source_page: str = Field(default=SOFASCORE_ES_URL)
    page_url: str
    document_title: str | None = None
    items: list[SofaScoreStatisticsHistoricsItem]


class SofaScorePageResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_page": "https://www.sofascore.com/es/",
                "page_url": "https://www.sofascore.com/es/",
                "document_title": "SofaScore",
                "items": [],
            }
        }
    )

    source_page: str = Field(default=SOFASCORE_ES_URL)
    page_url: str = Field(description="URL al cerrar el flujo (vuelta a home).")
    document_title: str | None = Field(description="Contenido de <title> al final.")
    items: list[SofaScoreLeagueItem] = Field(
        description="Una fila por liga: primera opción con /football/tournament/ en el desplegable.",
    )


@router.post(
    "/sofascore",
    response_model=SofaScorePageResponse,
    tags=["sofascore"],
    operation_id="run_sofascore_scraper",
    summary="Scraper SofaScore — búsqueda de ligas top",
    description=(
        f"Sin cuerpo. Abre `{SOFASCORE_ES_URL}`, busca cada liga, entra al torneo, abre "
        "«Clasificaciones» → «Todos», lee el nombre visible de cada fila e inserta en `teams` "
        "(país fijo en ligas domésticas; en UEFA Champions/Europa, país desde la ficha del equipo)."
    ),
)
async def post_sofascore_scraper() -> SofaScorePageResponse:
    try:
        data = await run_sofascore_league_flow()
        rows = data["items"]
        return SofaScorePageResponse(
            page_url=data["url"],
            document_title=data["document_title"],
            items=[SofaScoreLeagueItem(**row) for row in rows],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/sofascore/statistics-historics",
    response_model=SofaScoreStatisticsHistoricsResponse,
    tags=["sofascore"],
    operation_id="run_sofascore_statistics_historics_scraper",
    summary="SofaScore — datos de partidos por jornada/ronda (JSON)",
    description=(
        "Sin cuerpo. Recorre jornadas/rondas; en Finished abre la ficha: cronología (match_events) y tab "
        "Estadísticas (team_match_stats). Progreso en `var/<liga>/jornada-N.json` (campo "
        "`verificador_ya_procesado` por partido). Si existe `var/<liga>/.liga_lista_completa`, no entra al "
        "torneo y usa JSON en disco. Por jornada: completa y misma lista → omite; si no → borra ese JSON "
        "y rescrapea (y quita el marcador de liga)."
    ),
)
async def post_sofascore_statistics_historics() -> SofaScoreStatisticsHistoricsResponse:
    try:
        data = await run_sofascore_statistics_historics_flow()
        rows = data["items"]
        return SofaScoreStatisticsHistoricsResponse(
            page_url=data["url"],
            document_title=data["document_title"],
            items=[SofaScoreStatisticsHistoricsItem(**row) for row in rows],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/sofascore/statistics-historics-2hilos",
    response_model=SofaScoreStatisticsHistoricsResponse,
    tags=["sofascore"],
    operation_id="run_sofascore_statistics_historics_2hilos_scraper",
    summary="SofaScore — Champions + Europa League en 2 hilos",
    description=(
        "Variante experimental: **dos hilos, cada uno con su propio Chromium**. "
        "`UEFA Champions League` (`season_id=6`) y `UEFA Europa League` (`season_id=7`). "
        "`items`: primero Champions, luego Europa. Borrar `*_2hilos.py` y esta ruta cuando ya no haga falta."
    ),
)
async def post_sofascore_statistics_historics_2hilos() -> SofaScoreStatisticsHistoricsResponse:
    if run_sofascore_statistics_historics_2hilos_flow is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Módulo statistics-historics-2hilos no disponible "
                "(archivo local `sofascore_statistics_historics_2hilos.py` omitido del repo)."
            ),
        )
    try:
        data = await run_sofascore_statistics_historics_2hilos_flow()
        rows = data["items"]
        return SofaScoreStatisticsHistoricsResponse(
            page_url=data["url"],
            document_title=data["document_title"],
            items=[SofaScoreStatisticsHistoricsItem(**row) for row in rows],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

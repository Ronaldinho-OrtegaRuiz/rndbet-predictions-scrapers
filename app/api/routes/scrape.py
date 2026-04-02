from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.scrapers.sofascore_scraper import SOFASCORE_ES_URL, run_sofascore_league_flow

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

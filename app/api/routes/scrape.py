from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.scrapers.makeyourstats_scraper import MAKEYOURSTATS_LEAGUES_URL, run_makeyourstats_flow

router = APIRouter()


class MakeYourStatsCompetitionItem(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "competition_id": 1,
                "name": "Premier League",
                "search": "inglaterra",
                "ok": True,
                "url": "https://makeyourstats.com/es/football/league/england/premier-league/8",
                "link_text": "Premier League",
                "error": None,
            }
        }
    )

    competition_id: int = Field(description="id en tu tabla `competitions`.")
    name: str
    search: str = Field(description="Texto usado en el autocompletado (país o liga).")
    ok: bool
    url: str | None = Field(
        default=None,
        description="URL absoluta del enlace encontrado en el desplegable.",
    )
    link_text: str | None = Field(default=None, description="Texto visible del enlace.")
    error: str | None = Field(
        default=None,
        description="Mensaje si no apareció el enlace esperado en `autocomplete-result-list`.",
    )


class MakeYourStatsScrapeResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_page": "https://makeyourstats.com/es/leagues",
                "page_url": "https://makeyourstats.com/es/leagues",
                "document_title": "Ligas de fútbol | MakeYourStats",
                "items": [],
            }
        }
    )

    source_page: str = Field(default=MAKEYOURSTATS_LEAGUES_URL)
    page_url: str = Field(description="URL tras la primera carga del flujo.")
    document_title: str | None = Field(description="Contenido de <title> tras esa carga.")
    items: list[MakeYourStatsCompetitionItem] = Field(
        description="Una entrada por competición configurada en el scraper.",
    )


@router.post(
    "/makeyourstats",
    response_model=MakeYourStatsScrapeResponse,
    tags=["makeyourstats"],
    operation_id="run_makeyourstats_scraper",
    summary="Scraper MakeYourStats",
    description=(
        f"Sin cuerpo. Abre `{MAKEYOURSTATS_LEAGUES_URL}` y recorre todas las competiciones "
        "(autocompletado y extracción de enlace por cada una)."
    ),
)
async def post_makeyourstats_scraper() -> MakeYourStatsScrapeResponse:
    try:
        data = await run_makeyourstats_flow(include_competition_links=True)
        rows = data["items"]
        return MakeYourStatsScrapeResponse(
            page_url=data["url"],
            document_title=data["document_title"],
            items=[MakeYourStatsCompetitionItem(**row) for row in rows],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.scrapers.makeyourstats_scraper import (
    MAKEYOURSTATS_LEAGUES_URL,
    open_makeyourstats_leagues,
)

router = APIRouter()


class MakeYourStatsPageResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "url": "https://makeyourstats.com/es/leagues",
                "document_title": "Ligas de fútbol | MakeYourStats",
            }
        }
    )

    url: str = Field(description="URL final tras cargar la página.")
    document_title: str | None = Field(
        description="Contenido de <title> del documento.",
    )


@router.post(
    "/makeyourstats",
    response_model=MakeYourStatsPageResponse,
    tags=["makeyourstats"],
    operation_id="run_makeyourstats_scraper",
    summary="Scraper MakeYourStats — página de ligas",
    description=(
        "Sin cuerpo. Abre directamente "
        f"`{MAKEYOURSTATS_LEAGUES_URL}` con Playwright y devuelve URL y título."
    ),
)
async def post_makeyourstats_scraper() -> MakeYourStatsPageResponse:
    try:
        data = await open_makeyourstats_leagues()
        return MakeYourStatsPageResponse(**data)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

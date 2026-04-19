from fastapi import APIRouter

from app.api.routes import health, scrape
from live_track.router import router as live_track_router

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(scrape.router, prefix="/scrape")
api_router.include_router(live_track_router, prefix="/live-track", tags=["live-track"])

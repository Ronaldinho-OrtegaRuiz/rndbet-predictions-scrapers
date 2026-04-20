from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LiveTrackSettings(BaseSettings):
    """Zona horaria del job y pausas del round-robin SofaScore."""

    model_config = SettingsConfigDict(env_prefix="LIVE_TRACK_", extra="ignore")

    time_zone: str = "America/Bogota"
    robin_pause_between_matches_seconds: float = 2.0
    robin_empty_list_sleep_seconds: float = 5.0
    backend_live_push_url: str | None = Field(
        default=None,
        description="POST del snapshot JSON (ej. https://api.tu-backend.com/live/sofascore-tick).",
    )
    backend_live_push_timeout_seconds: float = 45.0
    backend_live_push_bearer_token: str | None = None


live_track_settings = LiveTrackSettings()

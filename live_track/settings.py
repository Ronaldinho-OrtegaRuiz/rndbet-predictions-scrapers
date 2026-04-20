from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LiveTrackSettings(BaseSettings):
    """Zona horaria del job y pausas del round-robin SofaScore."""

    model_config = SettingsConfigDict(env_prefix="LIVE_TRACK_", extra="ignore")

    time_zone: str = "America/Bogota"
    robin_pause_between_matches_seconds: float = 2.0
    robin_empty_list_sleep_seconds: float = 5.0
    backend_live_push_url: str = Field(
        default="http://localhost:8080/api/v1/live-track/match-state",
        description=(
            "POST del snapshot JSON. Override con LIVE_TRACK_BACKEND_LIVE_PUSH_URL; "
            "cadena vacía para no enviar."
        ),
    )
    backend_live_push_timeout_seconds: float = 45.0
    backend_live_push_bearer_token: str | None = None


live_track_settings = LiveTrackSettings()

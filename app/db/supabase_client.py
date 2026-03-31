"""Cliente Supabase (sync). Usar desde rutas vía asyncio.to_thread si hace falta."""

from functools import lru_cache

from supabase import Client, create_client

from app.core.config import settings


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    if not settings.supabase_url or not settings.supabase_key:
        raise RuntimeError("SUPABASE_URL y SUPABASE_KEY deben estar definidos en el entorno.")
    return create_client(settings.supabase_url, settings.supabase_key)


@lru_cache(maxsize=1)
def get_supabase_service_client() -> Client:
    """
    Cliente con Service Role (bypass RLS). Solo para backend.
    Requiere SUPABASE_SERVICE_ROLE_KEY.
    """
    if not settings.supabase_url or not settings.supabase_service_role_key:
        raise RuntimeError(
            "SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY deben estar definidos para escribir en tablas con RLS."
        )
    return create_client(settings.supabase_url, settings.supabase_service_role_key)

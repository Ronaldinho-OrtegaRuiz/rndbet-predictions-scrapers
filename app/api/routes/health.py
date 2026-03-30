import asyncio
import os

from fastapi import APIRouter, HTTPException

from app.core.config import loaded_dotenv_path, settings
from app.db.supabase_client import get_supabase_client

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _supabase_ping_sync() -> dict:
    client = get_supabase_client()
    if settings.supabase_ping_table:
        table = settings.supabase_ping_table.strip()
        res = client.table(table).select("*").limit(1).execute()
        return {
            "mode": "table",
            "table": table,
            "rows_sampled": len(res.data or []),
        }
    buckets = client.storage.list_buckets()
    names: list[str] = []
    for b in buckets or []:
        if isinstance(b, dict):
            names.append(str(b.get("name", "")))
        else:
            names.append(str(getattr(b, "name", b)))
    return {
        "mode": "storage",
        "bucket_count": len(buckets or []),
        "buckets": [n for n in names if n],
    }


@router.get("/health/supabase")
async def health_supabase() -> dict:
    """Prueba conexión a Supabase (Storage o una tabla vía PostgREST)."""
    if not settings.supabase_url or not settings.supabase_key:
        raise HTTPException(
            status_code=503,
            detail={
                "message": (
                    "Faltan URL y clave de Supabase (NEXT_PUBLIC_SUPABASE_URL y "
                    "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY, o SUPABASE_URL / SUPABASE_KEY). "
                    "Comprueba que el .env en la raíz del proyecto tenga esas líneas y reinicia uvicorn. "
                    "Opcional: variable de entorno DOTENV_PATH apuntando al .env."
                ),
                "dotenv_loaded_path": str(loaded_dotenv_path.resolve())
                if loaded_dotenv_path
                else None,
                "process_cwd": os.getcwd(),
            },
        )
    try:
        info = await asyncio.to_thread(_supabase_ping_sync)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"status": "ok", "supabase": True, **info}

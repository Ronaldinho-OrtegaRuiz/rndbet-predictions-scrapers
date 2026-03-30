import os
from pathlib import Path
from typing import Self

from dotenv import dotenv_values, find_dotenv, load_dotenv
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PACKAGE_ROOT = Path(__file__).resolve().parents[2]


def _load_all_dotenv() -> Path | None:
    """Carga .env desde cwd (y padres), raíz del paquete y cwd explícito. Último archivo gana."""
    candidates: list[Path] = []
    found = find_dotenv(usecwd=True)
    if found:
        candidates.append(Path(found))
    candidates.append(_PACKAGE_ROOT / ".env")
    candidates.append(Path.cwd() / ".env")
    # Subir desde app/core por si .env está en un ancestro
    p = Path(__file__).resolve().parent
    for _ in range(8):
        candidates.append(p / ".env")
        if p == p.parent:
            break
        p = p.parent
    # Variable de entorno del sistema o del shell (no suele estar dentro del propio .env)
    explicit = (os.getenv("DOTENV_PATH") or os.getenv("ENV_FILE") or "").strip()
    if explicit:
        candidates.append(Path(explicit))

    seen: set[str] = set()
    last_loaded: Path | None = None
    enc = "utf-8-sig"
    for raw in candidates:
        if not raw.is_file():
            continue
        key = str(raw.resolve())
        if key in seen:
            continue
        seen.add(key)
        load_dotenv(raw, override=True, encoding=enc)
        last_loaded = raw
    return last_loaded


loaded_dotenv_path: Path | None = _load_all_dotenv()


def _read_dotenv_file(*keys: str) -> str:
    """Lee claves del .env en disco (fallback si os.environ no reflejó load_dotenv)."""
    if not loaded_dotenv_path or not loaded_dotenv_path.is_file():
        return ""
    vals = dotenv_values(loaded_dotenv_path, encoding="utf-8-sig")
    for k in keys:
        raw = vals.get(k)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return ""


def _dotenv_paths_for_supabase() -> list[Path]:
    """Raíz del código, cwd y último .env cargado (sin duplicar la misma ruta)."""
    seen: set[str] = set()
    out: list[Path] = []
    for raw in (_PACKAGE_ROOT / ".env", Path.cwd() / ".env", loaded_dotenv_path):
        if raw is None:
            continue
        try:
            key = str(raw.resolve())
        except OSError:
            key = str(raw)
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


def resolve_supabase_credentials() -> tuple[str | None, str | None]:
    """URL y clave para Supabase: settings, luego .env en raíz del paquete/cwd, luego os.environ."""
    url = (settings.supabase_url or "").strip() or None
    key = (settings.supabase_key or "").strip() or None
    if url and key:
        return url, key
    for path in _dotenv_paths_for_supabase():
        if not path.is_file():
            continue
        vals = dotenv_values(path, encoding="utf-8-sig")
        if not url:
            for nk in ("NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_URL"):
                v = (vals.get(nk) or "").strip()
                if v:
                    url = v
                    break
        if not key:
            for kk in (
                "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY",
                "SUPABASE_KEY",
                "SUPABASE_ANON_KEY",
            ):
                v = (vals.get(kk) or "").strip()
                if v:
                    key = v
                    break
        if url and key:
            return url, key
    if not url:
        u = (
            os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").strip()
            or os.getenv("SUPABASE_URL", "").strip()
        )
        url = u or None
    if not key:
        k = (
            os.getenv("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY", "").strip()
            or os.getenv("SUPABASE_KEY", "").strip()
            or os.getenv("SUPABASE_ANON_KEY", "").strip()
        )
        key = k or None
    return url, key


def supabase_dotenv_diagnostics() -> dict[str, object]:
    """Sin secretos: rutas y tamaño en disco para depurar 503."""
    def stat_path(p: Path) -> dict[str, object]:
        row: dict[str, object] = {"path": str(p)}
        if not p.is_file():
            row["exists"] = False
            return row
        row["exists"] = True
        row["size_bytes"] = p.stat().st_size
        return row

    return {
        "package_root_dotenv": stat_path(_PACKAGE_ROOT / ".env"),
        "cwd_dotenv": stat_path(Path.cwd() / ".env"),
        "last_loaded_dotenv": stat_path(loaded_dotenv_path)
        if loaded_dotenv_path
        else {"path": None, "exists": False},
    }


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(loaded_dotenv_path) if loaded_dotenv_path else None,
        env_file_encoding="utf-8-sig",
        extra="ignore",
    )

    app_name: str = "rndbet-prediction-scrapers"
    app_version: str = "0.3.1"
    debug: bool = False
    supabase_url: str | None = None
    supabase_key: str | None = None
    supabase_ping_table: str | None = None
    playwright_headless: bool = False
    playwright_after_load_wait_seconds: float = 3.0
    playwright_page_ready_timeout_ms: int = 120_000

    @model_validator(mode="after")
    def supabase_from_next_or_short_names(self) -> Self:
        url = (
            (self.supabase_url or "").strip()
            or os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").strip()
            or os.getenv("SUPABASE_URL", "").strip()
            or _read_dotenv_file("NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_URL")
        ) or None
        key = (
            (self.supabase_key or "").strip()
            or os.getenv("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY", "").strip()
            or os.getenv("SUPABASE_KEY", "").strip()
            or os.getenv("SUPABASE_ANON_KEY", "").strip()
            or _read_dotenv_file(
                "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY",
                "SUPABASE_KEY",
                "SUPABASE_ANON_KEY",
            )
        ) or None
        object.__setattr__(self, "supabase_url", url)
        object.__setattr__(self, "supabase_key", key)
        return self


settings = Settings()

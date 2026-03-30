import asyncio
import sys
from contextlib import asynccontextmanager

# Playwright arranca el driver con subprocess; en Windows el SelectorEventLoop
# no implementa subprocess → NotImplementedError. Proactor sí.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from app.api.router import api_router
from app.core.config import settings


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


class _NoCacheOpenAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path in ("/openapi.json", "/docs", "/redoc"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        return response


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(_NoCacheOpenAPIMiddleware)
app.include_router(api_router, prefix="/api")


def custom_openapi():
    """OpenAPI con menos ruido en Swagger: JSON Schema usa la clave `title` en cada campo y confunde."""
    if app.openapi_schema:
        return app.openapi_schema
    from fastapi.openapi.utils import get_openapi

    openapi_schema = get_openapi(
        title=settings.app_name,
        version=settings.app_version,
        openapi_version=app.openapi_version,
        routes=app.routes,
    )

    def _strip_json_schema_titles(node: object) -> None:
        if isinstance(node, dict):
            node.pop("title", None)
            for v in node.values():
                _strip_json_schema_titles(v)
        elif isinstance(node, list):
            for item in node:
                _strip_json_schema_titles(item)

    schemas = (openapi_schema.get("components") or {}).get("schemas")
    if isinstance(schemas, dict):
        _strip_json_schema_titles(schemas)

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": settings.app_name, "api_version": settings.app_version}

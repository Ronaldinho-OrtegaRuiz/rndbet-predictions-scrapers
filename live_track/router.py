from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException
from starlette.responses import JSONResponse

from live_track.queue_bus import apply_live_track_ingest_side_effects
from live_track.schemas import LiveTrackPayload, LiveTrackRunRequest, dump_live_track_json
from live_track.service import build_and_persist
from live_track.storage import load_live_track, persist_live_track

router = APIRouter()

_REPO_ROOT = Path(__file__).resolve().parents[1]


@router.post(
    "/run",
    response_class=JSONResponse,
    summary="Generar JSON live-track para una fecha",
    description=(
        "Calcula `fecha_referencia` (body o hoy en `LIVE_TRACK_TIME_ZONE`), arma la cola de partidos "
        "del día y resuelve **partido por partido** en SofaScore (no un barrido de todas las ligas); "
        "persiste `var/live-track/{fecha}.json`. Cabecera `X-Persisted-Path`."
    ),
)
async def post_live_track_run(
    body: Annotated[LiveTrackRunRequest | None, Body()] = None,
) -> JSONResponse:
    payload, path = await build_and_persist(
        fecha_referencia=body.fecha_referencia if body else None,
        repo_root=_REPO_ROOT,
    )
    return JSONResponse(
        content=dump_live_track_json(payload),
        headers={"X-Persisted-Path": str(path)},
    )


@router.post(
    "/ingest",
    response_class=JSONResponse,
    summary="Guardar payload del backend y programar cola SofaScore",
    description=(
        "El **backend** envía `fecha_referencia` + `partidos`. Se persiste en "
        "`var/live-track/{fecha}.json`. Por cada partido se **programa el ingreso al round-robin** "
        "a la **hora de `fecha`** (kickoff); pasado ese momento el partido entra en la ronda. "
        "Un worker recorre **solo los ya incorporados** en bucle 1→N→1→N… (partidos cruzados en el tiempo). "
        "Re-ingest cancela timers pendientes y reprograma. Pausas: `LIVE_TRACK_ROBIN_*`."
    ),
)
async def post_live_track_ingest(payload: LiveTrackPayload) -> JSONResponse:
    path = persist_live_track(_REPO_ROOT, payload)
    await apply_live_track_ingest_side_effects(payload)
    return JSONResponse(
        content=dump_live_track_json(payload),
        headers={"X-Persisted-Path": str(path)},
    )


@router.get(
    "/{fecha_referencia}",
    response_class=JSONResponse,
    summary="Leer JSON live-track guardado",
)
async def get_live_track_file(fecha_referencia: str) -> JSONResponse:
    if len(fecha_referencia) != 10 or fecha_referencia[4] != "-" or fecha_referencia[7] != "-":
        raise HTTPException(status_code=400, detail="fecha_referencia debe ser YYYY-MM-DD")
    payload = load_live_track(_REPO_ROOT, fecha_referencia)
    if payload is None:
        raise HTTPException(status_code=404, detail="No hay archivo para esa fecha; ejecutá POST /run antes.")
    return JSONResponse(content=dump_live_track_json(payload))

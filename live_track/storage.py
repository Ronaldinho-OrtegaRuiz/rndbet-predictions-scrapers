from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from live_track.schemas import LiveTrackPayload, PartidoPendiente, dump_live_track_json


def _kickoff_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _partido_matches_removal(
    p: PartidoPendiente,
    *,
    match_id: int | None,
    competicion: str,
    equipo_local: str,
    equipo_visitante: str,
    kickoff_utc: datetime,
) -> bool:
    if match_id is not None:
        if p.match_id is not None:
            return p.match_id == match_id
        return (
            p.competicion == competicion
            and p.equipo_local == equipo_local
            and p.equipo_visitante == equipo_visitante
            and _kickoff_utc(p.fecha) == kickoff_utc
        )
    return (
        p.competicion == competicion
        and p.equipo_local == equipo_local
        and p.equipo_visitante == equipo_visitante
        and _kickoff_utc(p.fecha) == kickoff_utc
    )


def remove_partido_from_live_track(
    repo_root: Path,
    fecha_referencia: str,
    *,
    match_id: int | None,
    competicion: str,
    equipo_local: str,
    equipo_visitante: str,
    kickoff_utc: datetime,
) -> bool:
    """Quita el partido del JSON persistido si hay coincidencia. Devuelve True si reescribió archivo."""
    payload = load_live_track(repo_root, fecha_referencia)
    if payload is None:
        return False
    kept = [
        p
        for p in payload.partidos
        if not _partido_matches_removal(
            p,
            match_id=match_id,
            competicion=competicion,
            equipo_local=equipo_local,
            equipo_visitante=equipo_visitante,
            kickoff_utc=kickoff_utc,
        )
    ]
    if len(kept) == len(payload.partidos):
        return False
    persist_live_track(
        repo_root,
        LiveTrackPayload(fecha_referencia=payload.fecha_referencia, partidos=kept),
    )
    return True


def persist_live_track(repo_root: Path, payload: LiveTrackPayload) -> Path:
    """Escribe `var/live-track/{fecha_referencia}.json` (relativo a la raíz del repo)."""
    out_dir = repo_root / "var" / "live-track"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{payload.fecha_referencia}.json"
    path.write_text(
        json.dumps(dump_live_track_json(payload), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def load_live_track(repo_root: Path, fecha_referencia: str) -> LiveTrackPayload | None:
    path = repo_root / "var" / "live-track" / f"{fecha_referencia}.json"
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return LiveTrackPayload.model_validate(data)

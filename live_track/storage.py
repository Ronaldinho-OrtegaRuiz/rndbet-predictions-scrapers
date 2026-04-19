from __future__ import annotations

import json
from pathlib import Path

from live_track.schemas import LiveTrackPayload, dump_live_track_json


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

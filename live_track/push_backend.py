from __future__ import annotations

import logging
from typing import Any

import httpx

from live_track.settings import live_track_settings

log = logging.getLogger("live_track.push")


async def push_live_snapshot_async(body: dict[str, Any]) -> None:
    url = (live_track_settings.backend_live_push_url or "").strip()
    if not url:
        log.debug("LIVE_TRACK_BACKEND_LIVE_PUSH_URL vacío: no se envía snapshot al backend")
        return
    headers: dict[str, str] = {"Content-Type": "application/json"}
    tok = (live_track_settings.backend_live_push_bearer_token or "").strip()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    timeout = live_track_settings.backend_live_push_timeout_seconds
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, json=body, headers=headers)
        r.raise_for_status()
    log.info("snapshot enviado al backend match_id=%s status_http=%s", body.get("match_id"), r.status_code)

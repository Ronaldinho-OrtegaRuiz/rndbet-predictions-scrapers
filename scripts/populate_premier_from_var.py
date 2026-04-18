"""
Pobla Supabase desde `var/premier-league/*.json` (salida del scraper statistics-historics).

Equipos y `team_id` del JSON se asumen **ya existentes** en `teams` (no se crean aquí).

Orden por partido: `matches` → `team_match_stats` (2 filas) → `match_events`.

Para cada evento, el campo `name` del JSON **solo sirve** para resolver `players`:
  1) `SELECT id FROM players WHERE name = <nombre exacto>`;
  2) si hay fila → ese `id` es `player_id` en `match_events`;
  3) si no → `INSERT INTO players (name) VALUES (...)` **solo nombre**, luego se usa el `id` devuelto.
En `match_events` **no** se persiste el nombre del JSON; solo `player_id` (y el resto de columnas del evento).
Si el evento no trae nombre usable, `player_id` queda ausente / null.

No modifica ni borra los JSON ni tablas ajenas (`predictions`, `prediction_evaluations`).

Requisitos: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` (recomendado por RLS).

Uso (desde la raíz del repo):

    py -3 scripts/populate_premier_from_var.py
    py -3 scripts/populate_premier_from_var.py --dry-run
    py -3 scripts/populate_premier_from_var.py --dir var/premier-league --season-id 1
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import settings
from app.db.supabase_client import get_supabase_client, get_supabase_service_client
from app.domain.rows import MatchEventRow, MatchRow, TeamMatchStatsRow

log = logging.getLogger("populate_premier")


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        s = value.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    return None


def _client():
    if settings.supabase_service_role_key:
        return get_supabase_service_client()
    log.warning("Sin SUPABASE_SERVICE_ROLE_KEY: usando anon key (puede fallar con RLS).")
    return get_supabase_client()


def _find_match_id(
    sb: Any,
    *,
    season_id: int,
    home_team_id: int,
    away_team_id: int,
    date_iso: str,
) -> int | None:
    r = (
        sb.table("matches")
        .select("id")
        .eq("season_id", season_id)
        .eq("home_team_id", home_team_id)
        .eq("away_team_id", away_team_id)
        .eq("date", date_iso)
        .limit(1)
        .execute()
    )
    rows = r.data or []
    if not rows:
        return None
    return int(rows[0]["id"])


def _insert_match(sb: Any, payload: dict[str, Any]) -> int:
    # postgrest v2: insert() → SyncQueryRequestBuilder (no encadena .select); vuelve fila con representation.
    r = sb.table("matches").insert(payload).execute()
    rows = r.data or []
    if not rows or not isinstance(rows[0], dict) or "id" not in rows[0]:
        raise RuntimeError(f"Insert matches sin id en respuesta: {r!r}")
    return int(rows[0]["id"])


def _insert_team_match_stats(sb: Any, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sb.table("team_match_stats").insert(rows).execute()


def _insert_match_events(sb: Any, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sb.table("match_events").insert(rows).execute()


def _norm_player_name(name: Any) -> str | None:
    if not isinstance(name, str):
        return None
    s = name.strip()
    return s or None


def _resolve_player_id(
    sb: Any,
    display_name: str,
    cache: dict[str, int],
) -> int:
    """
    display_name: texto del JSON (cronología); debe coincidir exactamente con `players.name` en BD.

    1) Consultar si ya existe jugador con ese nombre → usar su id.
    2) Si no existe → insertar fila con únicamente `name`, leer id devuelto.
    """
    if display_name in cache:
        return cache[display_name]
    existing = (
        sb.table("players").select("id").eq("name", display_name).limit(1).execute()
    )
    rows = existing.data or []
    if rows:
        pid = int(rows[0]["id"])
        cache[display_name] = pid
        return pid
    created = sb.table("players").insert({"name": display_name}).execute()
    cr = created.data or []
    if not cr or not isinstance(cr[0], dict) or "id" not in cr[0]:
        raise RuntimeError(f"players.insert sin id: name={display_name!r} resp={created!r}")
    pid = int(cr[0]["id"])
    cache[display_name] = pid
    return pid


def _json_files(dir_path: Path) -> list[Path]:
    files = sorted(dir_path.glob("*.json"), key=lambda p: p.name.lower())
    return [p for p in files if p.is_file()]


def _match_payload_from_obj(
    obj: dict[str, Any],
    *,
    season_id: int,
) -> dict[str, Any]:
    dt = _parse_dt(obj.get("date"))
    if dt is None:
        raise ValueError("match sin date válido")
    row = MatchRow(
        season_id=season_id,
        date=dt,
        home_team_id=int(obj["home_team_id"]),
        away_team_id=int(obj["away_team_id"]),
        home_score=obj.get("home_score"),
        away_score=obj.get("away_score"),
        status=obj.get("status"),
        round=obj.get("round"),
        stage=obj.get("stage"),
        group=obj.get("group"),
        current_minute=obj.get("current_minute"),
        added_time=obj.get("added_time"),
        last_updated=_parse_dt(obj.get("last_updated")),
    )
    return row.model_dump(mode="json", exclude_none=True)


def _stats_payloads(stats: Any, match_id: int) -> list[dict[str, Any]]:
    if not isinstance(stats, list):
        return []
    out: list[dict[str, Any]] = []
    for s in stats:
        if not isinstance(s, dict):
            continue
        tr = TeamMatchStatsRow(
            match_id=match_id,
            team_id=int(s["team_id"]),
            is_home=bool(s.get("is_home")),
            goals=s.get("goals"),
            possession=s.get("possession"),
            shots=s.get("shots"),
            shots_on_target=s.get("shots_on_target"),
            saves=s.get("saves"),
            yellow_cards=s.get("yellow_cards"),
            red_cards=s.get("red_cards"),
            corners=s.get("corners"),
            fouls=s.get("fouls"),
            offsides=s.get("offsides"),
        )
        out.append(tr.model_dump(mode="json", exclude_none=True))
    return out


def _events_payloads(
    events: Any,
    match_id: int,
    sb: Any,
    player_cache: dict[str, int],
) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        return []
    out: list[dict[str, Any]] = []
    for e in events:
        if not isinstance(e, dict):
            continue
        json_name = _norm_player_name(e.get("name"))
        player_id: int | None = None
        if json_name is not None:
            player_id = _resolve_player_id(sb, json_name, player_cache)
        er = MatchEventRow(
            match_id=match_id,
            team_id=e.get("team_id"),
            player_id=player_id,
            minute=e.get("minute"),
            event_type=e.get("event_type"),
            extra_data=e.get("extra_data"),
            created_at=_parse_dt(e.get("created_at")),
        )
        out.append(er.model_dump(mode="json", exclude_none=True))
    return out


def run(
    *,
    var_dir: Path,
    season_id: int,
    dry_run: bool,
) -> tuple[int, int, int, int, int]:
    """
    Returns:
        matches_inserted, matches_skipped_duplicate, skipped_missing_team_ids,
        errors, dry_run_scanned
    """
    files = _json_files(var_dir)
    if not files:
        log.warning("No hay *.json en %s", var_dir)
        return 0, 0, 0, 0, 0

    sb = None if dry_run else _client()
    player_cache: dict[str, int] = {}
    inserted = 0
    skipped = 0
    skipped_no_teams = 0
    errors = 0
    dry_scanned = 0

    for jp in files:
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
        except Exception as exc:
            log.error("%s: no se pudo leer JSON: %s", jp.name, exc)
            errors += 1
            continue

        matches = data.get("matches")
        if not isinstance(matches, list):
            log.warning("%s: sin lista matches", jp.name)
            continue

        for idx, raw in enumerate(matches):
            if not isinstance(raw, dict):
                continue
            label = f"{jp.name}#[{idx}]"
            try:
                if raw.get("home_team_id") is None or raw.get("away_team_id") is None:
                    log.warning(
                        "omitido %s: home_team_id/away_team_id null en JSON (equipo no resuelto al scrapear). "
                        "verificador=%r",
                        label,
                        raw.get("verificador_ya_procesado"),
                    )
                    skipped_no_teams += 1
                    continue

                payload = _match_payload_from_obj(raw, season_id=season_id)
                date_val = payload["date"]
                date_iso = date_val.isoformat() if isinstance(date_val, datetime) else str(date_val)

                hid = int(payload["home_team_id"])
                aid = int(payload["away_team_id"])
                sid = int(payload["season_id"])

                if dry_run:
                    dry_scanned += 1
                    log.info("[dry-run] match %s season=%s %s vs %s @ %s", label, sid, hid, aid, date_iso)
                    continue

                assert sb is not None
                existing = _find_match_id(
                    sb,
                    season_id=sid,
                    home_team_id=hid,
                    away_team_id=aid,
                    date_iso=date_iso,
                )
                if existing is not None:
                    log.debug("omitido duplicado %s → match_id=%s", label, existing)
                    skipped += 1
                    continue

                match_id = _insert_match(sb, payload)
                stats_rows = _stats_payloads(raw.get("team_match_stats"), match_id)
                _insert_team_match_stats(sb, stats_rows)
                ev_rows = _events_payloads(raw.get("match_events"), match_id, sb, player_cache)
                _insert_match_events(sb, ev_rows)
                inserted += 1
                log.info("insertado %s → match_id=%s (stats=%s events=%s)", label, match_id, len(stats_rows), len(ev_rows))
            except Exception as exc:
                log.exception("error %s: %s", label, exc)
                errors += 1

    return inserted, skipped, skipped_no_teams, errors, dry_scanned


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Poblar BD desde var/premier-league/*.json")
    p.add_argument(
        "--dir",
        type=Path,
        default=ROOT / "var" / "premier-league",
        help="Carpeta con jornada-*.json",
    )
    p.add_argument("--season-id", type=int, default=1, help="season_id forzado (Premier=1)")
    p.add_argument("--dry-run", action="store_true", help="Solo listaría partidos, sin Supabase")
    args = p.parse_args()
    var_dir = args.dir.resolve()
    if not var_dir.is_dir():
        log.error("No existe el directorio: %s", var_dir)
        return 2

    ins, skip, no_teams, err, dry_n = run(
        var_dir=var_dir, season_id=args.season_id, dry_run=args.dry_run
    )
    if args.dry_run:
        log.info(
            "Listo (simulación): partidos_ok=%s omitidos_sin_equipos_en_json=%s errores=%s",
            dry_n,
            no_teams,
            err,
        )
    else:
        log.info(
            "Listo: insertados=%s omitidos_duplicado=%s omitidos_sin_equipos=%s errores=%s",
            ins,
            skip,
            no_teams,
            err,
        )
    return 1 if err else 0


if __name__ == "__main__":
    raise SystemExit(main())

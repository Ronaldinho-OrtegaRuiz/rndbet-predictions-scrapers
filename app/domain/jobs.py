"""
Modos de scraping recomendados (un solo código base, varios jobs / endpoints).

Recomendación general
----------------------
- **Este servicio (scrapers + FastAPI):** extracción desde SofaScore (Playwright y/o API
  interna si algún día la usás), normalización a los modelos de `rows.py`, y **upsert**
  idempotente hacia Supabase (clave natural o `sofascore_*_id` si agregás columnas).
- **Otro backend / worker:** cron (diario), colas, y “¿hace falta correr live?” — orquesta
  llamando a endpoints de acá (`POST /api/scrape/...`) o ejecutando los mismos módulos
  como CLI. Así no duplicás reglas de dominio.

Los cuatro pasos que planteás encajan así:

1. **Poblar todo (fin de temporada / backfill)**  
   `FULL_SEASON` — competiciones, temporada, equipos, partidos históricos, resultados,
   estadísticas de partido, eventos y jugadores si los scrapeás. Corrida larga, manual o
   una vez por año.

2. **Solo calendario (inicio de temporada)**  
   `FIXTURES_ONLY` — `matches` con `status='scheduled'`, goles NULL, sin
   `team_match_stats` / `match_events`. Rápido y barato.

3. **Auditoría de fechas (diaria + endpoint)**  
   `SCHEDULE_AUDIT` — para partidos `scheduled` (y opcionalmente ventana “próximas N
   jornadas” o “no solo la próxima”), re-leer kickoff y actualizar `date` si cambió.
   El **scheduler** puede vivir en el otro backend; el **scraper** y el contrato de BD
   quedan acá.

4. **Solo estadísticas en vivo**  
   `LIVE_STATS` — partidos `live` (y a veces `finished` recién cerrados): actualizar
   `current_minute`, `added_time`, `last_updated`, `home_score`/`away_score`,
   `team_match_stats`, y opcionalmente `match_events`. Alta frecuencia; conviene que el
   **otro backend** dispare cada 1–5 min solo cuando hay partidos en vivo.

`timestamptz`
-------------
Enviá siempre instantes con zona (ISO 8601 con offset o `Z`). En Python: `datetime` con
`tzinfo` (p. ej. `datetime.now(timezone.utc)` o una zona explícita antes de persistir).
"""

from enum import Enum


class ScraperJobKind(str, Enum):
    """Qué tipo de corrida vas a ejecutar (misma app, distintos entrypoints)."""

    FULL_SEASON = "full_season"
    """Backfill completo: fixtures, resultados, stats, eventos, jugadores."""

    FIXTURES_ONLY = "fixtures_only"
    """Solo partidos programados (sin stats ni eventos)."""

    SCHEDULE_AUDIT = "schedule_audit"
    """Revalidar fechas/horas de partidos pendientes; upsert de `date` si cambió."""

    LIVE_STATS = "live_stats"
    """Partidos en vivo (y transición a finished): marcador, minuto, stats por equipo."""

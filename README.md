# rndbet-prediction-scrapers

API con **FastAPI** y scrapers con **Playwright**.

## Requisitos

- Python 3.11+
- Navegadores de Playwright (`playwright install chromium`)

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
playwright install chromium
```

Creá un **`.env`** en la raíz del proyecto con tus variables.

### Supabase

- `SUPABASE_URL` o **`NEXT_PUBLIC_SUPABASE_URL`**
- `SUPABASE_KEY`, **`SUPABASE_ANON_KEY`** o **`NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY`** (mismos valores que en Next).
- `SUPABASE_PING_TABLE` — opcional; si la definís, el health de Supabase hace `SELECT` limit 1 a esa tabla. Si no, prueba **Storage** (`list_buckets`).

Prueba de conexión: **`GET /api/health/supabase`** (200 si responde la API de Supabase).

### Playwright

- `PLAYWRIGHT_HEADLESS` — por defecto **`false`** (ventana visible en tu PC). En servidor/CI usá **`true`**.
- `PLAYWRIGHT_AFTER_LOAD_WAIT_SECONDS` — segundos tras ver contenido clave en la página (por defecto **3**).
- `PLAYWRIGHT_PAGE_READY_TIMEOUT_MS` — timeout esperando la marca “MakeYourStats” visible en `/es/leagues` (por defecto **120000**).
- `DEBUG` — `true` / `false`

En **Windows** con **`uvicorn --reload`**, Playwright async en el mismo loop puede fallar; el scraper usa **Playwright síncrono en un hilo** (`asyncio.to_thread`).

## Arranque

**PowerShell (sin activar el venv):**

```powershell
.\.venv\Scripts\python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

- Docs: http://127.0.0.1:8000/docs
- Health: `GET /api/health`
- Supabase (si hay `.env`): `GET /api/health/supabase`
- **MakeYourStats:** `POST /api/scrape/makeyourstats` — sin body; abre directamente `https://makeyourstats.com/es/leagues`.
- `GET /` incluye `api_version` (p. ej. `0.3.1`).

## Tests

```bash
pytest
```

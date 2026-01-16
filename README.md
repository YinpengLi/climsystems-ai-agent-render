# ClimSystems AI Agent (Render-friendly)

This package is preconfigured to deploy on Render **without needing to change build/start commands**.

## Deploy on Render (simplest)
1. Create a new Render **Blueprint** from `render.yaml` (recommended), OR
2. If you create services manually, use:
   - **API service**
     - Build: `pip install -r requirements.txt`
     - Start: `bash start_api.sh`
   - **Worker service**
     - Build: `pip install -r requirements.txt`
     - Start: `bash start_worker.sh`

## Endpoints
- `GET /health` — health check
- `POST /v1/assets:bulk_upsert` — demo asset upsert
- `POST /v1/runs` — create a demo run (enqueues a background job)
- `GET /v1/runs/{run_id}` — run status

> This is a starter scaffold. Replace the demo analysis in `apps/worker/worker.py` with real ClimSystems API calls.

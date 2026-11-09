# PulseBoard

Self-hosted uptime and latency monitor. Python (FastAPI, asyncio, SQLite) + vanilla JS dashboard with live updates over Server-Sent Events.

## Features
- Concurrent async health checks (`httpx` + `asyncio.gather`) every 10s (`CHECK_INTERVAL`)
- Live dashboard via SSE: no polling, no frameworks
- 24h uptime, p50 / p95 latency, incident log on state changes
- Add and remove services at runtime through a REST API
- 7-day retention, built-in "flaky" demo service that fails on purpose

## Run locally
    pip install -r requirements.txt
    uvicorn main:app --reload
    # open http://localhost:8000

## Docker
    docker build -t pulseboard . && docker run -p 8000:8000 pulseboard

## Deploy (free)
- **Render**: push to GitHub, New > Blueprint, select repo (uses `render.yaml`)
- **Fly.io / Railway**: deploy the Dockerfile as is
- Mount a volume at `/data` and set `DB_PATH=/data/pulseboard.db` to persist history

## API
| Method | Path | Purpose |
|---|---|---|
| GET | /api/targets | Services with uptime, p50, p95, recent history |
| POST | /api/targets | Add `{name, url}` |
| DELETE | /api/targets/{id} | Remove a service |
| GET | /api/events | Recent up/down incidents |
| GET | /api/stream | SSE stream of live check results |
| GET | /health | Liveness probe |

# AGENTS.md

## Cursor Cloud specific instructions

This repo is the **Alcove API** — a monolithic FastAPI app in `api/main.py` (entry point `api.main:app`), plus root-level Windows-oriented streaming/OBS helper scripts.

### Runtime / environment
- Only `python3` is available (there is no `python` alias). Use `python3` everywhere.
- Python dependencies (from `requirements.txt`) install into the user site (`~/.local/bin`), which is added to `PATH` via `~/.bashrc`. The update script reinstalls them on startup, so you normally don't need to.
- Non-obvious: `start.sh` is not marked executable, so run it as `PORT=8000 bash start.sh` (not `./start.sh`). It just runs `uvicorn api.main:app --host 0.0.0.0 --port "$PORT" --ws websockets`. Equivalent: `python3 -m uvicorn api.main:app --port 8000 --ws websockets`.
- `$PORT` is required by `start.sh`; pick any free port locally (e.g. 8000). CORS already allows `localhost:8000/8080/5500/5173`.

### Run / test / build
- **Run (dev):** `PORT=8000 bash start.sh`, then `GET http://localhost:8000/` returns `{"status":"Alcove API running",...}`. Interactive docs at `/docs`.
- **Tests:** run from the repo root with `python3 -m unittest discover` (test modules import the app as `api.main`, so the repo root must be the working directory / on `sys.path`). There is no test suite committed yet; discovery currently finds 0 tests, which is expected.
- **Build:** none (pure Python). `render.yaml` build is just `pip install -r api/requirements.txt`.
- **Lint:** no linter is configured.

### Runtime side effects (do NOT commit)
- Importing/running `api.main` creates SQLite/JSON state files in the current working directory: `alcove_state.db`, `alcove_runtime_state.json`, and (when exercised) `feature_flags.json`, `pulse_settings.json`, `safety_settings.json`, `verification_flow_events.jsonl`, plus `~/Desktop/Alcove/*` folders. The repo `.gitignore` does NOT ignore these root state files — leave them untracked and never `git add` them.

### Secrets (set via Cursor Cloud Agent secrets, never commit)
The API runs and most read/write endpoints work with no secrets. These are only needed for privileged/bot-facing flows (see `render.env.example`):
- `BOT_SYNC_SECRET` — required for all `/api/bot-sync/*` endpoints (unset ⇒ 500 "Bot sync secret is not configured").
- `TELEGRAM_BOT_TOKEN` (or `ALCOVE_TELEGRAM_BOT_TOKEN`) — Telegram Mini App verification / admin notifications.

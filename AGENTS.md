# AGENTS.md — Spark Control

Self-hosted control plane for **DGX Spark / GB10 model recipes**: a tiny FastAPI
gateway (plus a static web shell) that wraps the owner's existing start/stop
scripts over SSH and serves a phone-friendly UI. No GPU needed on the host.

**Task: set up Spark Control on the user's machine** — or diagnose/improve it.
Read `README.md` first (it is the canonical install + config reference).

## Setup workflow (for a coding agent)

1. **Prereqs:** `python3 >= 3.11`, passwordless SSH keys from this host to the
   DGX Spark node(s) (`ssh-copy-id` once per node), and the user's existing
   start/stop scripts + container facts.
2. **Env:** `python3 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"`
3. **Secrets/config:** `cp .env.example .env` then fill:
   - `SPARKCTL_TOKEN` — long random (`openssl rand -hex 32`)
   - `GATEWAY_HOST` / `GATEWAY_PORT` — bind address (`0.0.0.0` / `8001`)
   - `SPARKCTL_JOB_STORE` — absolute path to the SQLite ledger
   - `MONITOR_SPARKDASH_URL` / `MONITOR_GRAFANA_URL` — optional tiles
4. **Recipes:** `cp config/recipes.example.toml config/recipes.toml`, then edit
   per recipe: `[nodes]` keys, `nodes`, `port`, `compose_project`, `containers`,
   absolute `start_script`/`stop_script` paths, `served_model`,
   `start_timeout_s`. `SPARKCTL_RECIPES` can point elsewhere.
5. **Run + smoke:** `python -m sparkcontrol`; then
   `curl -s localhost:8001/health` → `{"status":"ok"}`, and `GET /api/ui` with
   the bearer token → view-model with `monitors` + `default_recipe`.
6. **Optional service:** `deployment/sparkctl-gateway.service` (adjust
   `User=`/`Group=`/`WorkingDirectory=`/`EnvironmentFile=`), then
   `systemctl enable --now sparkctl-gateway`.
7. **Report:** the UI URL, which recipes are configured, and the token *location*
   (never echo the token value).

## Hard constraints (MUST NOT)

- **Never** print, commit, or log `.env` values / `SPARKCTL_TOKEN` — env-var
  names only. `.env.example` is the only exception file.
- **Never** run a recipe start/stop (or live-test control paths) while a model
  the user is actively using is serving on the lane — always ask first and
  state exactly which script will run and what it will stop.
- **Never** edit `config/recipes.toml` on a working setup without asking —
  node keys, IPs, and script paths are user-specific.
- **Never** hardcode monitor URLs, hostnames, or IPs in `src/` or the shell —
  everything cluster-specific comes from config/env.
- **Never** push to GitHub unless the user asks.

## Conventions

- Python ≥ 3.11; gates: `pytest -q`, `ruff check src/ tests/`,
  `mypy --strict src/sparkcontrol/`, `node --check src/sparkcontrol/static/app.js`.
- Unit tests use `FakeSsh` — **never** require a real cluster.
- Error envelope everywhere: `{"error": {"code": "...", "detail": "..."}}`.
- One start/stop operation at a time; a second gets `409 operation-in-progress`.
- Swaps need explicit confirmation (`port-held-needs-confirm`) — never a
  surprise stop.
- `GET /api/ui` is the single view-model the shell renders verbatim; keep state
  decisions in Python (`ui.py`), not in JavaScript.

## How to work with the user

- Before any state-changing action: state what will run and confirm.
- Status is honest by design: `stopped / starting / running / unreachable`;
  GPU memory `N/A` is normal on GB10 unified memory.
- If the user asks for monitor wiring: the Monitor page loads in **their
  browser**, so the URL must be reachable from that device (tailnet IP for a
  phone on Tailscale, LAN IP otherwise).

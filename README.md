# Spark Control

[![CI](https://github.com/Mikehutu/spark-control/actions/workflows/ci.yml/badge.svg)](https://github.com/Mikehutu/spark-control/actions/workflows/ci.yml)

A self-hosted control plane for **DGX Spark / GB10 owners** — start, stop, and
swap your model-serving recipes from a phone or browser, with confirmation
gates and honest status. No cloud, no vendor portal, no Docker-in-Docker UI:
a tiny FastAPI gateway wraps **your existing start/stop scripts** over SSH.

> Not affiliated with NVIDIA. Built for owners who like their cluster on a LAN.

## What it does

- **One-tap start / guarded stop** — stop requires a long-press (≥ 800 ms) so a
  pocket tap can't kill in-flight requests.
- **Swap with confirmation** — a recipe that needs the canonical `:8888` lane
  while another is running asks you first (`confirm to stop X and start Y`).
  Never a surprise stop.
- **One operation at a time** — start/stop is serialized per gateway; a second
  request while a job is running gets `409 operation-in-progress` instead of
  launching a concurrent, conflicting process (the classic double-tap race).
- **Real status** — `stopped / starting / running / unreachable`, live model id
  from `/v1/models`, per-node reachability + GPU memory (GB10 unified memory
  reporting `[N/A]` is normal — the UI says so).
- **Job history** — every start/stop is logged (SQLite) with its output tail.
- **Monitor page** — plug in your own live-dashboard URLs: a sparkDash-style
  server embeds in an iframe; Grafana/Prometheus opens as a link.

## Design principles

1. **Your scripts are the source of truth.** The gateway runs your start/stop
   scripts verbatim over SSH. It never reimplements model launching.
2. **The lane is sacred.** Only one recipe holds the canonical port at a time;
   arbitration is explicit and confirmed.
3. **Read-only by default.** All status comes from read-only probes. The only
   writes are the start/stop scripts you configured — and only when you tap.
4. **No secrets in the repo.** Token and paths live in `.env` (gitignored);
   `SPARKCTL_TOKEN` is referenced as an environment variable name only.

## Requirements

- **Any always-on Linux host** with Python ≥ 3.11 — could be a Raspberry Pi 3
  (it hosts this gateway fine), a mini-PC, a NUC, or an older server. ~100 MB
  RAM. It does **not** need a GPU.
- **Passwordless SSH** from that host to your DGX Spark node(s)
  (`ssh-copy-id` + keys; `BatchMode yes` — the gateway never prompts).
- Your existing **start/stop scripts per model** (they exist already if you
  deploy models today; the gateway just wraps them).
- Optional: a **tailnet** for phone access from outside your LAN.

## Quick start

```bash
git clone <your-fork-or-tarball-url> spark-control && cd spark-control

# 1. Python env
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. Configure recipes (nodes, ports, scripts, containers)
cp config/recipes.example.toml config/recipes.toml && $EDITOR config/recipes.toml

# 3. Configure the gateway
cp .env.example .env && $EDITOR .env      # SPARKCTL_TOKEN, paths, monitors

# 4. Run
python -m sparkcontrol
#   open http://<host-ip>:8001/ui   (paste the token once)
```

### Run as a service (systemd)

```bash
sudo cp deployment/sparkctl-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sparkctl-gateway
# adjust User= / WorkingDirectory= / EnvironmentFile= in the unit to your setup
```

## Configuration

### `.env`

| Variable | Meaning | Example |
|---|---|---|
| `SPARKCTL_TOKEN` | Bearer token for every `/api` call (long random) | `openssl rand -hex 32` |
| `GATEWAY_HOST` / `GATEWAY_PORT` | Bind address | `0.0.0.0` / `8001` |
| `SPARKCTL_JOB_STORE` | SQLite job ledger path | `~/spark-control/sparkctl-jobs.sqlite3` |
| `SPARKCTL_RECIPES` | Optional path to your recipes config | default: `config/recipes.toml` |
| `SPARKCTL_DEFAULT_RECIPE` | Start button target on the Status page | (default: first recipe) |
| `MONITOR_SPARKDASH_URL` | URL embedded in the Monitor page (iframe) | `http://100.x.x.x:5555` |
| `MONITOR_GRAFANA_URL` | URL opened as a link from the Monitor page | `http://100.x.x.x:3000` |

### `config/recipes.toml`

```toml
[nodes]
head  = "192.0.2.10"      # your node keys (any names you like)
node2 = "192.0.2.11"

[[recipes]]
recipe_id      = "my-model"
nodes          = ["head", "node2"]      # all nodes the recipe occupies
port           = 8888                   # canonical serving port
compose_project= "my-model"             # docker compose project
containers     = ["my-model-head"]      # container name(s) — used for detection
start_script   = "/home/YOUR_USER/my-model/start.sh"
stop_script    = "/home/YOUR_USER/my-model/stop.sh"
served_model   = "my-model-v1"          # id reported by /v1/models
start_timeout_s= 900.0                  # model load budget
```

- **Detection** is name-anchored (`docker ps --filter name=^<container>$`) and
  works for raw-docker and compose deployments; recipes without container names
  fall back to the `com.docker.compose.project` label.
- **Conflicts** are derived automatically for recipes sharing port + node;
  add `conflicts = [...]` for non-structural overlaps.
- **Health** is polled at `http://{head}:{port}/health` after start until
  `start_timeout_s`; the job fails loudly (with the output tail) on timeout.
  (If your server uses a different health path, adjust `health_url_template`
  in `src/sparkcontrol/__main__.py`.)

### SSH prerequisites

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
ssh-copy-id -i ~/.ssh/id_ed25519.pub "$USER"@<SPARK_IP>   # once per node
ssh-keyscan -H <SPARK_IP> >> ~/.ssh/known_hosts
```

The gateway calls the system `ssh` binary, so anything in `~/.ssh/config`
(aliases, keys, `StrictHostKeyChecking`) applies.

## Monitor page — how it works

The shell is served by this gateway; the **Monitor page is loaded in the
browser that opens the UI**. So:

- Put a URL in `MONITOR_SPARKDASH_URL` that is reachable **from that device**
  (e.g. your node's tailnet IP if your phone is on the tailnet, or its LAN IP
  if you're on the same Wi-Fi).
- The gateway host itself does **not** need a route to the monitor — it only
  passes the URL through. No monitor configured = tile hidden.

sparkDash-style servers (live GPU/serving glance), Grafana, and any other
HTTP dashboard work: iframe for `MONITOR_SPARKDASH_URL`, link for
`MONITOR_GRAFANA_URL`.

## Safety notes

- The gateway can **stop your serving model** on your command. Long-press
  guard + confirmation gate exist, but there is no kill-switch beyond them.
- Keep the token out of logs/repos; rotate by changing `.env` and restarting.
- Run it **only on your LAN/tailnet**, or behind your own reverse proxy with
  auth. There is no built-in TLS.
- If the gateway host itself is also your control-plane for the cluster you're
  debugging (e.g. you run the app on a box whose model depends on the lane),
  never live-test start/stop while that model is in use.

## Troubleshooting

- **`unreachable` status** → SSH key/route issue; check `ssh <node> hostname`
  from the gateway host.
- **`operation-in-progress` (409)** → a start/stop job is already running; the
  UI shows its banner. Wait or check the Jobs page.
- **`already up` / `already stopped`** → idempotent no-ops, nothing to do.
- **Models listed as `needs swap`** → they can't run alongside the current one;
  starting them goes through the confirm gate.
- **GPU memory `N/A`** → GB10 unified memory doesn't expose per-GPU MiB; normal.

## Development

```bash
pip install -e ".[dev]"
pytest -q                       # 89 unit tests (FakeSsh — no cluster needed)
ruff check src/ tests/
mypy --strict src/sparkcontrol/
```

The same gates run as **CI on every push/PR** (GitHub Actions, Python 3.11 + 3.12).

## License

MIT — see [LICENSE](LICENSE). Use it, fork it, improve it. If you build
something cool on top, a shout-out is appreciated but not required.

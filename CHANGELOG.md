# Changelog

## v1.0.0 — initial public release

- Control plane for DGX Spark model recipes: one-tap start, long-press stop,
  confirmation-gated swaps on the canonical port lane.
- Lane serialization: at most one start/stop operation in flight
  (409 `operation-in-progress` on a second request — no concurrent launches).
- Server status with explicit `stopped / starting / running / unreachable`
  states, live served-model id, per-node reachability.
- Async job runner with SQLite ledger, bounded output tails, job history page.
- Web shell: Status / Recipes / Jobs / Monitor pages (installable PWA, no
  build step, single static directory).
- Configurable monitor tiles (sparkDash-style iframe + Grafana link) via
  environment variables.
- Recipe registry driven by `config/recipes.toml` — wrap your existing
  start/stop scripts; nothing cluster-specific is hardcoded.
- Named-URL config only: no tokens, hosts, or paths committed.

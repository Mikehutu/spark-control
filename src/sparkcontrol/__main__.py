"""Deployment entrypoint: python -m sparkcontrol (slice-6 wiring pulled forward).

Loads the real recipe registry from config/recipes.toml, wires the subprocess
SSH adapter (system ssh, ~/.ssh/config aliases), and serves the gateway +
dashboard. Env vars (names only, never values in code): SPARKCTL_TOKEN,
GATEWAY_HOST, GATEWAY_PORT, SPARKCTL_RECIPES (optional config path override).
"""

from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

from .app import create_app
from .config import Settings
from .jobs import JobRunner, SqliteJobStore
from .lifecycle import ServerControl
from .recipes import RecipeRegistry
from .ssh_adapter import SubprocessSshClient

DEFAULT_RECIPES = Path(__file__).resolve().parent.parent.parent / "config" / "recipes.toml"


def load_registry(path: Path) -> tuple[RecipeRegistry, dict[str, str]]:
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    node_map = {str(k): str(v) for k, v in data.get("nodes", {}).items()}
    return RecipeRegistry.from_config(data.get("recipes", [])), node_map


def build_control(settings: Settings, recipes_path: Path) -> ServerControl:
    registry, node_map = load_registry(recipes_path)
    store = SqliteJobStore(settings.job_store_path)
    runner = JobRunner(store)
    return ServerControl(
        registry=registry,
        ssh=SubprocessSshClient(),
        jobs=runner,
        health_url_template="http://{host}:{port}/health",
        node_map=node_map,
    )


def main() -> None:
    import uvicorn

    settings = Settings.from_env()
    recipes_path = Path(os.environ.get("SPARKCTL_RECIPES", str(DEFAULT_RECIPES)))
    if not recipes_path.exists():
        print(f"recipe config not found: {recipes_path}", file=sys.stderr)
        raise SystemExit(2)
    control = build_control(settings, recipes_path)
    uvicorn.run(
        create_app(settings, control=control),
        host=settings.gateway_host,
        port=settings.gateway_port,
    )


if __name__ == "__main__":
    main()

"""UI view-model for the Spark Control dashboard (slice 5, pwa-ui).

The Python side owns the state mapping (ServerStatus + optional active job
-> the JSON view-model the static JS shell renders verbatim). The JS shell is
deliberately dumb: fetch, render, long-press timer. This keeps every state
decision unit-testable without a browser.

Tri-state button (PRD FR-3 + pwa-ui slice):
- ``running``    -> stop button, LONG-PRESS required (>= 800 ms in JS)
- ``stopped``    -> start button, single tap
- ``starting``   -> disabled, progress shown
- ``unreachable``-> disabled + banner

Recipe catalog addendum: the full matrix is listed with ``is_active`` +
``locked`` (another running recipe conflicts for the canonical port -> start
needs the confirm gate; the shell shows a lock instead of a plain Start).
"""

from __future__ import annotations

from typing import Any

from .lifecycle import ServerStatus
from .recipes import RecipeRegistry


def view_model(
    status: ServerStatus,
    active_job: dict[str, Any] | None = None,
    recipes: list[dict[str, Any]] | None = None,
    monitors: dict[str, str] | None = None,
    default_recipe: str = "",
) -> dict[str, Any]:
    """Map cluster status (+ optional job snapshot + recipe catalog) to the UI payload."""
    job = active_job or {}
    job_state = job.get("state")
    job_active = job_state in ("pending", "running")
    job_action = str(job.get("action", ""))

    state = status.state
    if job_active and job_action.startswith("start:"):
        state = "starting"
    elif job_active and job_action.startswith("stop:"):
        state = "stopping"

    banner: str | None = None
    button: dict[str, Any]
    if state == "running":
        label = f"Stop {status.model or status.active_recipe or 'server'}"
        button = {"action": "stop", "enabled": True, "longpress": True, "label": label}
    elif state == "starting":
        banner = "Start job running — waiting for vLLM health…"
        button = {"action": None, "enabled": False, "longpress": False, "label": "Starting…"}
    elif state == "stopping":
        banner = "Stop job running…"
        button = {"action": None, "enabled": False, "longpress": False, "label": "Stopping…"}
    elif state == "unreachable":
        banner = "Cluster unreachable — nodes did not respond"
        button = {"action": None, "enabled": False, "longpress": False, "label": "Unavailable"}
    else:  # stopped
        button = {"action": "start", "enabled": True, "longpress": False, "label": "Start"}

    return {
        "state": state,
        "button": button,
        "banner": banner,
        "active_recipe": status.active_recipe,
        "model": status.model,
        "vllm_healthy": status.vllm_healthy,
        "nodes": [
            {
                "host": n.host,
                "reachable": n.reachable,
                "gpu_mem_used_gb": n.gpu_mem_used_gb,
                "uptime_s": n.uptime_s,
            }
            for n in status.nodes
        ],
        "recipes": recipes or [],
        "monitors": monitors or {},
        "default_recipe": default_recipe,
        "job": job or None,
    }


def recipe_catalog(registry: RecipeRegistry, active_recipe: str | None) -> list[dict[str, Any]]:
    """Full recipe matrix for the UI: is_active + conflict-locked flags.

    ``locked`` = starting this recipe would displace a currently-running
    conflicting recipe (canonical :8888 lane) -> the client must route through
    the confirm gate (409 port-held-needs-confirm -> confirm_swap=true).
    """
    catalog: list[dict[str, Any]] = []
    active_id = active_recipe
    running_conflicts: set[str] = set()
    if active_id:
        try:
            active = registry.get(active_id)
        except Exception:  # RecipeError: active recipe not in registry (stale config)
            active = None
        if active is not None:
            running_conflicts = {r.recipe_id for r in registry.conflicting(active)}
    for recipe in registry.all():
        catalog.append(
            {
                "recipe_id": recipe.recipe_id,
                "served_model": recipe.served_model,
                "nodes": list(recipe.nodes),
                "port": recipe.port,
                "is_active": recipe.recipe_id == active_id,
                "locked": bool(active_id) and recipe.recipe_id in running_conflicts,
            }
        )
    return catalog

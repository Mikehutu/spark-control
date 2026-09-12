"""Unit tests for the dashboard UI slice (view-model + static shell route).

No browser: the Python side is a pure function (ServerStatus -> view-model)
and the shell is served as static files at /ui (unauthenticated: zero data in
the shell itself; all /api calls carry the bearer token from localStorage).
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from sparkcontrol.app import create_app
from sparkcontrol.config import Settings
from sparkcontrol.jobs import JobRunner, SqliteJobStore
from sparkcontrol.lifecycle import NodeStatus, ServerControl, ServerStatus
from sparkcontrol.recipes import Recipe, RecipeRegistry
from sparkcontrol.ui import recipe_catalog, view_model

TOKEN = "test-super-secret-token"

RECIPE = Recipe(
    recipe_id="glm5.3-flash",
    nodes=("node-a", "node-b"),
    port=8888,
    compose_project="glm53",
    containers=("glm53-exl3-head",),
    start_script="/home/u/GLM/start.sh",
    stop_script="/home/u/GLM/stop.sh",
    served_model="GLM-5.3-Flash-EXL3",
)


def make_status(state: str = "running") -> ServerStatus:
    nodes = (
        NodeStatus(host="node-a", reachable=True, gpu_mem_used_gb=13.9, uptime_s=1234.0),
        NodeStatus(host="node-b", reachable=True, gpu_mem_used_gb=13.8, uptime_s=1234.0),
    )
    if state == "unreachable":
        nodes = (
            NodeStatus(host="node-a", reachable=False),
            NodeStatus(host="node-b", reachable=False),
        )
    return ServerStatus(
        state=state,
        active_recipe="glm5.3-flash" if state != "stopped" else None,
        vllm_healthy=state == "running",
        model="GLM-5.3-Flash-EXL3" if state == "running" else None,
        nodes=nodes,
    )


# --- view-model --------------------------------------------------------------


def test_vm_running_shows_longpress_stop():
    vm = view_model(make_status("running"))
    assert vm["state"] == "running"
    assert vm["button"] == {
        "action": "stop",
        "enabled": True,
        "longpress": True,
        "label": "Stop GLM-5.3-Flash-EXL3",
    }
    assert vm["banner"] is None


def test_vm_stopped_shows_single_tap_start():
    vm = view_model(make_status("stopped"))
    assert vm["state"] == "stopped"
    assert vm["button"]["action"] == "start"
    assert vm["button"]["longpress"] is False
    assert vm["button"]["enabled"] is True


def test_vm_starting_disables_button():
    vm = view_model(make_status("starting"))
    assert vm["button"]["action"] is None
    assert vm["button"]["enabled"] is False
    assert vm["banner"] is not None


def test_vm_unreachable_disables_button_and_flags_nodes():
    vm = view_model(make_status("unreachable"))
    assert vm["state"] == "unreachable"
    assert vm["button"]["enabled"] is False
    assert all(n["reachable"] is False for n in vm["nodes"])


def test_vm_active_start_job_overrides_to_starting():
    job: dict[str, Any] = {"job_id": "j1", "action": "start:glm5.3-flash", "state": "running"}
    vm = view_model(make_status("stopped"), active_job=job)
    assert vm["state"] == "starting"
    assert vm["button"]["enabled"] is False


def test_vm_active_stop_job_overrides_to_stopping():
    job: dict[str, Any] = {"job_id": "j2", "action": "stop:glm5.3-flash", "state": "pending"}
    vm = view_model(make_status("running"), active_job=job)
    assert vm["state"] == "stopping"
    assert vm["button"]["action"] is None


def test_vm_terminal_job_does_not_override():
    job: dict[str, Any] = {"job_id": "j3", "action": "start:glm5.3-flash", "state": "failed"}
    vm = view_model(make_status("stopped"), active_job=job)
    assert vm["state"] == "stopped"
    assert vm["button"]["action"] == "start"


def test_vm_nodes_serialized():
    vm = view_model(make_status("running"))
    assert vm["nodes"][0] == {
        "host": "node-a",
        "reachable": True,
        "gpu_mem_used_gb": 13.9,
        "uptime_s": 1234.0,
    }


# --- recipe catalog (addendum: show the full matrix, not just the active) -----


def _catalog() -> list[dict[str, Any]]:
    return [
        {
            "recipe_id": "glm5.3-flash",
            "served_model": "GLM-5.3-Flash-EXL3",
            "nodes": ["node-a", "node-b"],
            "port": 8888,
            "is_active": True,
        },
        {
            "recipe_id": "qwen3.8-flash-next",
            "served_model": "qwen3.8-flash-next",
            "nodes": ["node-a"],
            "port": 8888,
            "is_active": False,
        },
    ]


def test_vm_includes_recipe_catalog():
    vm = view_model(make_status("running"), recipes=_catalog())
    assert [r["recipe_id"] for r in vm["recipes"]] == ["glm5.3-flash", "qwen3.8-flash-next"]
    assert vm["recipes"][0]["is_active"] is True
    assert vm["recipes"][1]["is_active"] is False


def test_recipe_catalog_locked_flags():
    """locked == conflict-derived from the active recipe (all share :8888/node-a)."""
    reg = RecipeRegistry(
        {
            "glm5.3-flash": RECIPE,
            "qwen3.8-flash-next": Recipe(
                recipe_id="qwen3.8-flash-next",
                nodes=("node-a",),
                port=8888,
                compose_project="qwen-fn",
                containers=("vllm-fn-tp1",),
                start_script="/home/u/QWEN/start.sh",
                stop_script="/home/u/QWEN/stop.sh",
                served_model="qwen3.8-flash-next",
            ),
        }
    )
    catalog = recipe_catalog(reg, "glm5.3-flash")
    by_id = {r["recipe_id"]: r for r in catalog}
    assert by_id["glm5.3-flash"]["is_active"] is True
    assert by_id["glm5.3-flash"]["locked"] is False
    assert by_id["qwen3.8-flash-next"]["is_active"] is False
    assert by_id["qwen3.8-flash-next"]["locked"] is True
    # No active recipe -> nothing locked (stopped lane: all starts allowed).
    stopped = recipe_catalog(reg, None)
    assert all(r["locked"] is False for r in stopped)


def test_vm_includes_monitors_and_default_recipe():
    vm = view_model(
        make_status("stopped"),
        monitors={"sparkdash": "http://example.com:5555", "grafana": ""},
        default_recipe="glm5.3-flash",
    )
    assert vm["monitors"]["sparkdash"] == "http://example.com:5555"
    assert vm["monitors"]["grafana"] == ""
    assert vm["default_recipe"] == "glm5.3-flash"
    assert view_model(make_status("stopped"))["monitors"] == {}


def test_vm_recipes_default_empty():
    vm = view_model(make_status("stopped"))
    assert vm["recipes"] == []


# --- /ui static route --------------------------------------------------------


def _client() -> TestClient:
    registry = RecipeRegistry({"glm5.3-flash": RECIPE})
    control = ServerControl(
        registry,
        ssh=None,  # type: ignore[arg-type]  # /ui route does not touch SSH
        jobs=JobRunner(SqliteJobStore(":memory:")),
        health_url_template="http://{host}:{port}/health",
        node_map={},
    )
    return TestClient(create_app(Settings(token=TOKEN), control=control))


def test_ui_shell_served_without_auth():
    with _client() as client:
        r = client.get("/ui")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "Spark Control" in r.text


def test_ui_assets_served():
    with _client() as client:
        for path, ctype in (
            ("/ui/app.js", "javascript"),
            ("/ui/style.css", "text/css"),
            ("/ui/manifest.webmanifest", "json"),
        ):
            r = client.get(path)
            assert r.status_code == 200, path
            assert ctype in r.headers["content-type"], path

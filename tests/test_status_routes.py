"""Unit tests for the status/routes HTTP surface (slice 4).

The FastAPI app gets a ServerControl wired to a FakeSsh (same scripting
pattern as the server-lifecycle tests) — no cluster, no network. Health
probing and model fetch are patched at the lifecycle module boundary.
Covers: auth on status, tri-state incl. unreachable, job submission
(202 + poll), confirmation gate (409), unknown recipe (404), unreachable
head (503), idempotent stop no-op, unknown job (404).
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from sparkcontrol.app import create_app
from sparkcontrol.config import Settings
from sparkcontrol.jobs import JobRunner, JobState, SqliteJobStore
from sparkcontrol.lifecycle import ServerControl
from sparkcontrol.recipes import Recipe, RecipeRegistry

TOKEN = "test-super-secret-token"

GLM = Recipe(
    recipe_id="glm5.3-flash",
    nodes=("node-a", "node-b"),
    port=8888,
    compose_project="glm53",
    containers=("glm53-exl3-head",),
    start_script="/home/u/GLM/start.sh",
    stop_script="/home/u/GLM/stop.sh",
    served_model="GLM-5.3-Flash-EXL3",
    start_timeout_s=5.0,
)
QWEN = Recipe(
    recipe_id="qwen3.8-flash-next",
    nodes=("node-a",),
    port=8888,
    compose_project="qwen-fn",
    containers=("vllm-fn-tp1",),
    start_script="/home/u/QWEN/start.sh",
    stop_script="/home/u/QWEN/stop.sh",
    served_model="qwen3.8-flash-next",
    start_timeout_s=5.0,
)


class FakeSsh:
    """Scripted SSH: per-compose-project/container-name canned responses + log."""

    def __init__(self, docker_states=None, unreachable=(), name_states=None):
        self.docker_states = docker_states or {}  # compose_project -> state
        self.name_states = dict(name_states or {})  # container name -> present
        self.unreachable = set(unreachable)
        self.calls: list[tuple[str, str]] = []
        self.script_exit = 0

    async def run(self, host: str, command: str, timeout: float = 60.0):
        from sparkcontrol.lifecycle import CommandResult

        self.calls.append((host, command))
        if host in self.unreachable:
            raise ConnectionError(f"{host} unreachable")
        if "compose.project=" in command:
            project = command.split("compose.project=")[1].split("'")[0]
            state = self.docker_states.get(project)
            return CommandResult(exit_code=0, stdout=f"{state}\n" if state else "")
        if "name=^" in command:
            for name in self.name_states:
                if f"name=^{name}$" in command:
                    return CommandResult(exit_code=0, stdout=f"{name}\n")
            return CommandResult(exit_code=0, stdout="")
        if "nvidia-smi" in command:
            return CommandResult(exit_code=0, stdout="[N/A]\n12345.67 0.00\n")
        return CommandResult(exit_code=self.script_exit, stdout="script output\n")


def make_control(ssh: FakeSsh) -> ServerControl:
    registry = RecipeRegistry({r.recipe_id: r for r in (GLM, QWEN)})
    store = SqliteJobStore(":memory:")
    runner = JobRunner(store, executor=None)
    return ServerControl(
        registry,
        ssh,
        runner,
        health_url_template="http://{host}:{port}/health",
        node_map={"node-a": "192.0.2.10", "node-b": "192.0.2.11"},
        swap_poll_s=0.05,
    )


@pytest.fixture()
def api(monkeypatch) -> Iterator[tuple[TestClient, ServerControl, FakeSsh]]:
    healthy_flag = [True]
    served_model_flag = ["GLM-5.3-Flash-EXL3"]

    async def fake_health_ok(url: str) -> bool:
        return healthy_flag[0]

    async def fake_fetch_model(base_url: str) -> str | None:
        return served_model_flag[0]

    monkeypatch.setattr("sparkcontrol.lifecycle._health_ok", fake_health_ok)
    monkeypatch.setattr(
        "sparkcontrol.lifecycle._fetch_served_model", fake_fetch_model
    )

    ssh = FakeSsh(name_states={"glm53-exl3-head": "running"})
    control = make_control(ssh)
    app = create_app(Settings(token=TOKEN), control=control)
    # Context manager keeps one event-loop portal alive across requests so
    # submitted job tasks survive between poll calls.
    with TestClient(app) as client:
        yield client, control, ssh


def auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


def wait_terminal(client: TestClient, job_id: str, tries: int = 100) -> dict:
    """Poll GET /api/jobs/{id} until the job leaves pending/running."""
    payload: dict = {}
    for _ in range(tries):
        r = client.get(f"/api/jobs/{job_id}", headers=auth_headers())
        assert r.status_code == 200
        payload = r.json()
        if payload["state"] in (JobState.DONE.value, JobState.FAILED.value):
            return payload
        time.sleep(0.02)
    return payload


def error_code(resp) -> str:
    """Pull the machine code out of the canonical error envelope."""
    return resp.json()["error"]["code"]


# --- status -----------------------------------------------------------------


def test_status_requires_auth(api):
    client, _control, _ssh = api
    r = client.get("/api/server/status")
    assert r.status_code == 401
    assert error_code(r) == "unauthorized"


def test_status_running(api):
    client, _control, _ssh = api
    r = client.get("/api/server/status", headers=auth_headers())
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "running"
    assert body["active_recipe"] == "glm5.3-flash"
    assert body["model"] == "GLM-5.3-Flash-EXL3"
    assert body["vllm_healthy"] is True
    assert [n["host"] for n in body["nodes"]] == ["node-a", "node-b"]


def test_status_stopped(api, monkeypatch):
    client, _control, ssh = api
    ssh.name_states = {}
    r = client.get("/api/server/status", headers=auth_headers())
    assert r.status_code == 200
    assert r.json()["state"] == "stopped"


def test_status_unreachable_is_state_not_error(api):
    client, _control, ssh = api
    ssh.unreachable = {"192.0.2.10", "192.0.2.11"}
    r = client.get("/api/server/status", headers=auth_headers())
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "unreachable"
    assert all(n["reachable"] is False for n in body["nodes"])


# --- start ------------------------------------------------------------------


def test_start_submits_job_and_polls_done(api):
    client, _control, ssh = api
    ssh.name_states = {}
    r = client.post(
        "/api/server/glm5.3-flash/start", headers=auth_headers(), json={}
    )
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    assert job_id
    final = wait_terminal(client, job_id)
    assert final["state"] == "done"
    assert final["action"] == "start:glm5.3-flash"


def test_start_conflict_requires_confirm(api):
    client, _control, ssh = api
    ssh.name_states = {"glm53-exl3-head": "running"}  # GLM holds, start QWEN
    r = client.post(
        "/api/server/qwen3.8-flash-next/start", headers=auth_headers(), json={}
    )
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "port-held-needs-confirm"
    assert "glm5.3-flash" in body["error"]["detail"]


def test_start_conflict_confirm_swap_queues_job(api):
    client, _control, ssh = api
    ssh.name_states = {"glm53-exl3-head": "running"}
    r = client.post(
        "/api/server/qwen3.8-flash-next/start",
        headers=auth_headers(),
        json={"confirm_swap": True},
    )
    assert r.status_code == 202
    final = wait_terminal(client, r.json()["job_id"])
    assert final["state"] == "done"


def test_start_unknown_recipe_404(api):
    client, _control, _ssh = api
    r = client.post("/api/server/nope/start", headers=auth_headers(), json={})
    assert r.status_code == 404
    assert error_code(r) == "unknown-recipe"


def test_start_unreachable_head_503(api):
    client, _control, ssh = api
    ssh.name_states = {}
    ssh.unreachable = {"192.0.2.10"}
    r = client.post(
        "/api/server/glm5.3-flash/start", headers=auth_headers(), json={}
    )
    assert r.status_code == 503
    assert error_code(r) == "unreachable"


def test_start_requires_auth(api):
    client, _control, _ssh = api
    r = client.post("/api/server/glm5.3-flash/start", json={})
    assert r.status_code == 401


# --- stop -------------------------------------------------------------------


def test_stop_already_stopped_is_noop(api):
    client, _control, ssh = api
    ssh.name_states = {}
    r = client.post("/api/server/glm5.3-flash/stop", headers=auth_headers())
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "no-op"
    assert body["result"] == "already-stopped:glm5.3-flash"


def test_stop_submits_job(api):
    client, _control, ssh = api
    ssh.name_states = {"glm53-exl3-head": "running"}
    r = client.post("/api/server/glm5.3-flash/stop", headers=auth_headers())
    assert r.status_code == 202
    final = wait_terminal(client, r.json()["job_id"])
    assert final["state"] == "done"
    assert final["action"] == "stop:glm5.3-flash"


def test_stop_unknown_recipe_404(api):
    client, _control, _ssh = api
    r = client.post("/api/server/nope/stop", headers=auth_headers())
    assert r.status_code == 404


def test_stop_unreachable_head_503(api):
    client, _control, ssh = api
    ssh.unreachable = {"192.0.2.10"}
    r = client.post("/api/server/glm5.3-flash/stop", headers=auth_headers())
    assert r.status_code == 503


# --- jobs -------------------------------------------------------------------


def test_jobs_recent_lists_history(api):
    """Jobs page data source: newest jobs first, bounded."""
    client, _control, ssh = api
    ssh.name_states = {"glm53-exl3-head": "running"}
    r = client.post("/api/server/glm5.3-flash/stop", headers=auth_headers())
    assert r.status_code == 202
    wait_terminal(client, r.json()["job_id"])
    rec = client.get("/api/jobs/recent", headers=auth_headers())
    assert rec.status_code == 200
    jobs = rec.json()["jobs"]
    assert any(j["action"] == "stop:glm5.3-flash" for j in jobs)
    assert [j["created_at"] for j in jobs] == sorted(
        (j["created_at"] for j in jobs), reverse=True
    )
    # clamp proof: seed >100 records -> limit=1000 must cap at 100
    from sparkcontrol.jobs import JobRecord, JobState

    store = _control.jobs._store  # noqa: SLF001 - test seeds the ledger directly
    for i in range(105):
        store.upsert(
            JobRecord(
                job_id=f"seed-{i:04d}",
                action="start:glm5.3-flash",
                state=JobState.DONE,
                created_at=float(i),
            )
        )
    capped = client.get("/api/jobs/recent?limit=1000", headers=auth_headers()).json()["jobs"]
    assert len(capped) == 100  # clamp, not just <=100
    small = client.get("/api/jobs/recent?limit=5", headers=auth_headers()).json()["jobs"]
    assert len(small) == 5
    assert client.get("/api/jobs/recent").status_code == 401


def test_job_unknown_404(api):
    client, _control, _ssh = api
    r = client.get("/api/jobs/does-not-exist", headers=auth_headers())
    assert r.status_code == 404
    assert error_code(r) == "not_found"


def test_job_requires_auth(api):
    client, _control, _ssh = api
    r = client.get("/api/jobs/does-not-exist")
    assert r.status_code == 401


# --- bare app regression ----------------------------------------------------


def test_ui_view_has_monitors_and_default_recipe(monkeypatch):
    """/api/ui surfaces configurable monitor URLs + default recipe."""
    ssh = FakeSsh(name_states={})
    control = make_control(ssh)

    async def fake_health_ok(url: str) -> bool:
        return True

    monkeypatch.setattr("sparkcontrol.lifecycle._health_ok", fake_health_ok)
    app = create_app(
        Settings(
            token=TOKEN,
            monitor_sparkdash_url="http://sparkdash.local:5555",
            monitor_grafana_url="http://grafana.local:3000",
            default_recipe="qwen3.8-flash-next",
        ),
        control=control,
    )
    with TestClient(app) as client:
        r = client.get("/api/ui", headers=auth_headers())
        assert r.status_code == 200
        d = r.json()
        assert d["monitors"]["sparkdash"] == "http://sparkdash.local:5555"
        assert d["default_recipe"] == "qwen3.8-flash-next"

    # Empty defaults: fall back to the first recipe in the catalog.
    app2 = create_app(Settings(token=TOKEN), control=control)
    with TestClient(app2) as client:
        d2 = client.get("/api/ui", headers=auth_headers()).json()
        assert d2["monitors"] == {"sparkdash": "", "grafana": ""}
        assert d2["default_recipe"] == "glm5.3-flash"  # first in registry


def test_bare_app_has_no_control_routes():
    """Without an injected control, control routes stay unregistered."""
    app = create_app(Settings(token=TOKEN))
    with TestClient(app) as client:
        r = client.get("/api/server/status", headers=auth_headers())
        assert r.status_code == 404


# --- lane serialization over HTTP (the double-start race) ----------------------


def test_start_in_flight_reports_409_and_ui_reflects_it(api, monkeypatch):
    """First start accepted as a job; /api/ui shows 'starting' even WITHOUT a
    job_id (page-reload case); a second start gets 409 operation-in-progress
    instead of racing a second launch."""
    client, _control, ssh = api
    ssh.name_states = {}

    # Force health to stay down: the submitted start job keeps polling, so it
    # stays non-terminal for the duration of the test.
    async def unhealthy(url: str) -> bool:
        return False

    monkeypatch.setattr("sparkcontrol.lifecycle._health_ok", unhealthy)

    r1 = client.post(
        "/api/server/glm5.3-flash/start", headers=auth_headers(), json={}
    )
    assert r1.status_code == 202
    assert r1.json()["job_id"]

    # /api/ui without job_id must still surface the in-flight job.
    ui = client.get("/api/ui", headers=auth_headers())
    assert ui.status_code == 200
    assert ui.json()["state"] == "starting"
    assert ui.json()["job"] is not None
    assert ui.json()["job"]["action"] == "start:glm5.3-flash"

    r2 = client.post(
        "/api/server/qwen3.8-flash-next/start", headers=auth_headers(), json={}
    )
    assert r2.status_code == 409
    assert error_code(r2) == "operation-in-progress"

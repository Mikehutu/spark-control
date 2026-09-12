"""Unit tests for the server-lifecycle slice (recipe-aware control).

Everything runs against a FakeSsh — no cluster, no asyncssh, no network.
Covers: registry conflicts, container-state parsing, idempotent start/stop,
confirmation-gated swaps (never a surprise stop), unreachable nodes, and
wrapped-script execution (scripts run verbatim over SSH).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pytest

from sparkcontrol.jobs import TERMINAL_STATES, JobRunner, JobState, SqliteJobStore
from sparkcontrol.lifecycle import (
    ControlErrorKind,
    RecipeScriptExecutor,
    ServerControl,
    parse_docker_state,
    parse_nvidia_smi,
)
from sparkcontrol.recipes import Recipe, RecipeError, RecipeRegistry
from sparkcontrol.result import Err, is_ok

# --- fixtures --------------------------------------------------------------

GLM = Recipe(
    recipe_id="glm5.3-flash",
    nodes=("node-a", "node-b"),
    port=8888,
    compose_project="glm53",
    containers=("glm53-exl3-head",),
    start_script="/home/u/GLM/start.sh",
    stop_script="/home/u/GLM/stop.sh",
    served_model="GLM-5.3-Flash-EXL3",
    start_timeout_s=0.2,
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
    start_timeout_s=0.2,
)
DS_TEXT = Recipe(
    recipe_id="deepseek-v4-flash-dspark",
    nodes=("node-a", "node-b"),
    port=8888,
    compose_project="deepseek-v4-flash",
    containers=("deepseek-v4-flash-vllm-dspark-1",),
    start_script="/home/u/DS/start-deepseek-v4-flash-dspark.sh",
    stop_script="/home/u/DS/stop-deepseek-v4-flash-dspark.sh",
    served_model="deepseek-v4-flash-0731",
    start_timeout_s=0.2,
)
DS_VISION = Recipe(
    recipe_id="deepseek-v4-flash-vision",
    nodes=("node-a", "node-b"),
    port=8888,
    compose_project="deepseek-v4-flash",  # shared project -> shared stop
    containers=("deepseek-v4-flash-vllm-dspark-1",),
    start_script="/home/u/DS/start-deepseek-v4-flash-vision.sh",
    stop_script="/home/u/DS/stop-deepseek-v4-flash-dspark.sh",
    served_model="deepseek-v4-flash-vision-exp",
    start_timeout_s=0.2,
)


def make_registry() -> RecipeRegistry:
    return RecipeRegistry(
        {r.recipe_id: r for r in (GLM, QWEN, DS_TEXT, DS_VISION)}
    )


class FakeSsh:
    """Scripted SSH: per-(host, substring) canned responses + call log."""

    def __init__(self, docker_states=None, unreachable=(), name_states=None):
        # docker_states: compose_project -> state|None (label-based path)
        self.docker_states = docker_states or {}
        # name_states: container name -> state (name-based path)
        self.name_states: dict[str, str] = dict(name_states or {})
        self.unreachable = set(unreachable)
        self.calls: list[tuple[str, str]] = []
        self.script_exit = 0

    async def run(self, host: str, command: str, timeout: float = 60.0):
        from sparkcontrol.lifecycle import CommandResult

        await asyncio.sleep(0)  # model real SSH I/O: let concurrent coroutines interleave
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
        # start/stop scripts
        return CommandResult(exit_code=self.script_exit, stdout="script output\n")


def make_control(ssh: FakeSsh, healthy: bool = True) -> ServerControl:
    store = SqliteJobStore(":memory:")
    runner = JobRunner(store, executor=None)  # executor supplied per submit

    # Patch health probe: tests must not hit the network.
    import sparkcontrol.lifecycle as lc

    async def fake_health_ok(url: str) -> bool:
        return healthy

    async def fake_fetch_served_model(base_url: str) -> str | None:
        return "fake-model" if healthy else None

    lc._health_ok = fake_health_ok  # type: ignore[assignment]
    lc._fetch_served_model = fake_fetch_served_model  # type: ignore[assignment]

    return ServerControl(
        registry=make_registry(),
        ssh=ssh,
        jobs=runner,
        health_url_template="http://{host}:{port}/health",
        node_map={"node-a": "10.0.0.1", "node-b": "10.0.0.2"},
        health_poll_s=0.01,  # fast deterministic tests
        swap_poll_s=0.01,
    )


async def wait_terminal(runner: JobRunner, job_id: str, timeout: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        res = await runner.status(job_id)
        if is_ok(res) and res.value.state in TERMINAL_STATES:
            return res.value
        await asyncio.sleep(0.02)
    pytest.fail(f"job {job_id} never terminal")


# --- parser units -----------------------------------------------------------


def test_parse_docker_state_running():
    assert parse_docker_state("running\nrunning\n") == "running"


def test_parse_docker_state_none_when_empty():
    assert parse_docker_state("") is None


def test_parse_docker_state_prefers_running_over_exited():
    assert parse_docker_state("exited\nrunning\n") == "running"


def test_parse_nvidia_smi_skips_na():
    assert parse_nvidia_smi("[N/A]\n") is None


def test_parse_nvidia_smi_mib_to_gib():
    assert parse_nvidia_smi("10240\n") == 10.0


# --- container detection (name-based; live recon 2026-09-12) ------------------
#
# GLM (glm53-exl3-head) and Qwen (vllm-fn-tp1) are RAW-docker containers with
# NO com.docker.compose.project label — label-based detection reports a
# healthy running server as stopped. Detection must anchor on the container
# NAME; the compose-label filter is only a fallback for recipes with no
# containers listed.


def make_recipe(**overrides: Any) -> Recipe:
    params: dict[str, Any] = dict(
        recipe_id="glm5.3-flash",
        nodes=("node-a", "node-b"),
        port=8888,
        compose_project="glm53",
        containers=("glm53-exl3-head",),
        start_script="/home/u/GLM/start.sh",
        stop_script="/home/u/GLM/stop.sh",
        served_model="GLM-5.3-Flash-EXL3",
        start_timeout_s=0.2,
    )
    params.update(overrides)
    return Recipe(**params)


async def test_detection_uses_anchored_container_name():
    """Running container found BY NAME even with no compose label anywhere."""
    ssh = FakeSsh()  # no docker_states at all — nothing matches by label
    ssh.docker_states = {}  # label path would return nothing
    # name path must be served instead:
    ssh.name_states = {"glm53-exl3-head": "running"}
    ctl = make_control(ssh)
    res = await ctl.detect_active()
    assert is_ok(res)
    assert res.value == "glm5.3-flash"


async def test_detection_name_filter_is_anchored_exact():
    """The name filter is ^exact$ — a longer container with the same prefix
    must not satisfy detection (prefix drift safety)."""
    ssh = FakeSsh()
    ssh.name_states = {"glm53-exl3-head": "running"}
    ctl = make_control(ssh)
    await ctl.detect_active()
    name_cmds = [cmd for _, cmd in ssh.calls if "name=" in cmd]
    assert name_cmds, "expected a name-based docker ps command"
    assert any("name=^glm53-exl3-head$" in cmd for cmd in name_cmds)


async def test_detection_no_container_by_name_means_stopped():
    ssh = FakeSsh()
    ssh.name_states = {}
    ctl = make_control(ssh)
    res = await ctl.detect_active()
    assert is_ok(res)
    assert res.value is None


async def test_detection_label_fallback_when_no_containers():
    """Recipe without containers listed falls back to the compose label."""
    ssh = FakeSsh(docker_states={"glm53": "running"})
    ctl = make_control(ssh)
    # Swap in a containers-less recipe under the same project.
    fallback = make_recipe(containers=())
    ctl._registry = RecipeRegistry({fallback.recipe_id: fallback})
    res = await ctl.detect_active()
    assert is_ok(res)
    assert res.value == "glm5.3-flash"
    assert any("compose.project=" in cmd for _, cmd in ssh.calls)


# --- registry ----------------------------------------------------------------


def test_registry_conflicts_same_port_shared_nodes():
    reg = make_registry()
    conflicts = {r.recipe_id for r in reg.conflicting(GLM)}
    assert "qwen3.8-flash-next" in conflicts
    assert "deepseek-v4-flash-dspark" in conflicts


def test_registry_unknown_recipe_raises():
    with pytest.raises(RecipeError):
        make_registry().get("nope")


def test_registry_from_config_roundtrip():
    reg = RecipeRegistry.from_config(
        [
            {
                "recipe_id": "x",
                "nodes": ["node-a"],
                "port": 8888,
                "compose_project": "x",
                "start_script": "/s.sh",
                "stop_script": "/t.sh",
                "served_model": "x",
            }
        ]
    )
    assert reg.get("x").head() == "node-a"


def test_registry_from_config_missing_key():
    with pytest.raises(RecipeError):
        RecipeRegistry.from_config([{"recipe_id": "x", "nodes": ["a"]}])


# --- idempotency -------------------------------------------------------------


async def test_stop_when_no_container_is_already_stopped():
    ssh = FakeSsh(docker_states={})  # nothing running
    ctl = make_control(ssh)
    res = await ctl.stop("glm5.3-flash")
    assert is_ok(res)
    assert res.value == "already-stopped:glm5.3-flash"


async def test_start_when_running_and_healthy_is_already_up():
    ssh = FakeSsh(name_states={"glm53-exl3-head": "running"})
    ctl = make_control(ssh, healthy=True)
    res = await ctl.start("glm5.3-flash")
    assert is_ok(res)
    assert res.value == "already-up:glm5.3-flash"


# --- confirmation-gated swap (the critical safety behavior) ------------------


async def test_start_blocked_when_conflicting_recipe_holds_lane():
    # DS text running; starting GLM must NOT stop it without confirmation.
    ssh = FakeSsh(name_states={"deepseek-v4-flash-vllm-dspark-1": "running"})
    ctl = make_control(ssh, healthy=False)
    res = await ctl.start("glm5.3-flash", confirm_swap=False)
    assert isinstance(res, Err)
    assert res.error.kind == ControlErrorKind.PORT_HELD_NEEDS_CONFIRM
    # Assert NO stop script was run — never a surprise stop.
    assert not any("stop" in cmd for _, cmd in ssh.calls)


async def test_start_with_confirm_stops_holder_then_starts():
    ssh = FakeSsh(name_states={"deepseek-v4-flash-vllm-dspark-1": "running"})
    ctl = make_control(ssh, healthy=False)
    # First call: gated.
    res = await ctl.start("glm5.3-flash", confirm_swap=False)
    assert isinstance(res, Err)
    # Second call with confirmation: stops DS, then submits GLM start job.
    # After the stop job, the fake still reports DS running (docker state is
    # static), so detect_active still finds DS; the swap stop runs, and the
    # GLM start job is submitted regardless (script exit 0, health polled).
    res2 = await ctl.start("glm5.3-flash", confirm_swap=True)
    assert is_ok(res2)
    job_id = res2.value
    rec = await wait_terminal(ctl._jobs, job_id)  # type: ignore[attr-defined]
    assert rec.state in (JobState.DONE, JobState.FAILED)  # health=False -> FAILED(124)
    # stop script for DS project must have been invoked
    assert any("stop-deepseek" in cmd for _, cmd in ssh.calls)
    # start script for GLM must have been invoked
    assert any(cmd.endswith("GLM/start.sh") for _, cmd in ssh.calls)


async def test_ds_vision_and_text_share_compose_stop():
    # Stopping vision uses the shared DS stop script (same compose project).
    ssh = FakeSsh(name_states={"deepseek-v4-flash-vllm-dspark-1": "running"})
    ctl = make_control(ssh, healthy=False)
    res = await ctl.stop("deepseek-v4-flash-vision")
    assert is_ok(res)
    job = res.value
    assert not job.startswith("already-stopped")
    rec = await wait_terminal(ctl._jobs, job)  # type: ignore[attr-defined]
    assert rec.state == JobState.DONE
    assert any("stop-deepseek-v4-flash-dspark.sh" in cmd for _, cmd in ssh.calls)


# --- unreachable --------------------------------------------------------------


async def test_start_unreachable_node_returns_explicit_error():
    ssh = FakeSsh(unreachable={"10.0.0.1"})
    ctl = make_control(ssh)
    res = await ctl.start("glm5.3-flash")
    assert isinstance(res, Err)
    assert res.error.kind == ControlErrorKind.UNREACHABLE


async def test_stop_unreachable_node_returns_explicit_error():
    ssh = FakeSsh(unreachable={"10.0.0.1"})
    ctl = make_control(ssh)
    res = await ctl.stop("glm5.3-flash")
    assert isinstance(res, Err)
    assert res.error.kind == ControlErrorKind.UNREACHABLE


async def test_unknown_recipe_returns_error_not_exception():
    ssh = FakeSsh()
    ctl = make_control(ssh)
    res = await ctl.start("does-not-exist")
    assert isinstance(res, Err)
    assert res.error.kind == ControlErrorKind.UNKNOWN_RECIPE


# --- executor wraps scripts verbatim ------------------------------------------


async def test_executor_runs_wrapped_stop_script_verbatim():
    ssh = FakeSsh()
    lines: list[str] = []
    ex = RecipeScriptExecutor(ssh, GLM, "http://x/health", mode="stop")
    code = await ex.execute("stop:glm5.3-flash", lines.append)
    assert code == 0
    assert ssh.calls[-1] == ("10.0.0.1", "/home/u/GLM/stop.sh") or any(
        cmd == "/home/u/GLM/stop.sh" for _, cmd in ssh.calls
    )
    assert any("stop:" in line for line in lines)


async def test_executor_start_healthy_returns_zero(monkeypatch):
    import sparkcontrol.lifecycle as lc

    ssh = FakeSsh()

    async def healthy(url):
        return True

    monkeypatch.setattr(lc, "_health_ok", healthy)
    ex = RecipeScriptExecutor(ssh, GLM, "http://x/health", mode="start", health_poll_s=0.01)
    code = await ex.execute("start:glm5.3-flash", lambda line: None)
    assert code == 0


async def test_executor_start_timeout_returns_124(monkeypatch):
    import sparkcontrol.lifecycle as lc

    ssh = FakeSsh()

    async def unhealthy(url):
        return False

    monkeypatch.setattr(lc, "_health_ok", unhealthy)
    ex = RecipeScriptExecutor(
        ssh, GLM, "http://x/health", mode="start", health_poll_s=0.01, start_timeout_s=0.05
    )
    code = await ex.execute("start:glm5.3-flash", lambda line: None)
    assert code == 124


async def test_executor_start_script_failure_short_circuits():
    ssh = FakeSsh()
    ssh.script_exit = 3
    ex = RecipeScriptExecutor(ssh, GLM, "http://x/health", mode="start")
    code = await ex.execute("start:glm5.3-flash", lambda line: None)
    assert code == 3  # no health polling after a failed script


# --- status --------------------------------------------------------------------


async def test_server_status_stopped_when_nothing_running():
    ssh = FakeSsh(docker_states={})
    ctl = make_control(ssh)
    st = await ctl.server_status()
    assert st.state == "stopped"
    assert st.active_recipe is None


async def test_server_status_running_when_container_and_health():
    ssh = FakeSsh(name_states={"glm53-exl3-head": "running"})
    ctl = make_control(ssh, healthy=True)
    st = await ctl.server_status()
    assert st.state == "running"
    assert st.active_recipe == "glm5.3-flash"
    assert st.vllm_healthy
    assert st.model == "fake-model"


async def test_server_status_no_model_fetch_when_unhealthy():
    # Validator finding (2026-09-12): model must not be fetched when health
    # is down — no real network calls in unit tests, and no pointless probe.
    ssh = FakeSsh(name_states={"glm53-exl3-head": "running"})
    ctl = make_control(ssh, healthy=False)
    st = await ctl.server_status()
    assert st.state == "starting"
    assert st.model is None


async def test_server_status_starting_when_container_but_no_health():
    ssh = FakeSsh(name_states={"glm53-exl3-head": "running"})
    ctl = make_control(ssh, healthy=False)
    st = await ctl.server_status()
    assert st.state == "starting"


async def test_server_status_disambiguates_ds_vision_by_model(monkeypatch):
    """DS text/vision share container name; served model id picks the recipe."""
    import sparkcontrol.lifecycle as lc

    ssh = FakeSsh(name_states={"deepseek-v4-flash-vllm-dspark-1": "running"})
    ctl = make_control(ssh, healthy=True)

    async def vision_model(base_url: str) -> str | None:
        return "deepseek-v4-flash-vision-exp"

    # make_control installs its own fake fetch — override AFTER, like runtime.
    monkeypatch.setattr(lc, "_fetch_served_model", vision_model)
    st = await ctl.server_status()
    assert st.active_recipe == "deepseek-v4-flash-vision"
    assert st.model == "deepseek-v4-flash-vision-exp"


async def test_server_status_unreachable_when_head_down():
    ssh = FakeSsh(unreachable={"10.0.0.1"})
    ctl = make_control(ssh)
    st = await ctl.server_status()
    assert st.state == "unreachable"


# --- lane serialization (live incident 2026-09-12: double-start race) --------
#
# Root cause: start/stop had NO mutual exclusion and the UI showed no pending
# state, so two taps submitted two concurrent start jobs (GLM + DS raced on
# :8888). Contract: at most ONE start/stop job in flight per gateway; a second
# control request fails fast with OP_IN_PROGRESS (409) — never a silent
# duplicate launch.


class SlowExecutor:
    """ActionExecutor that stays running until released (or timeout)."""

    def __init__(self, release: asyncio.Event | None = None) -> None:
        self.release = release or asyncio.Event()

    async def execute(self, action: str, on_output: Callable[[str], None]) -> int:
        on_output(f"{action}: running slowly")
        await self.release.wait()
        return 0


async def test_in_flight_operation_none_when_idle() -> None:
    ssh = FakeSsh()
    ctl = make_control(ssh)
    assert await ctl.in_flight_operation() is None


async def test_start_rejected_while_other_operation_in_flight() -> None:
    ssh = FakeSsh()
    ctl = make_control(ssh)
    release = asyncio.Event()
    job = await ctl._jobs.submit(  # noqa: SLF001 - test introspects the runner
        "start:qwen3.8-flash-next", executor=SlowExecutor(release), timeout_s=30
    )
    assert is_ok(job)
    res = await ctl.start("glm5.3-flash")
    assert isinstance(res, Err)
    assert res.error.kind == ControlErrorKind.OP_IN_PROGRESS
    assert job.value in res.error.detail
    release.set()
    await wait_terminal(ctl._jobs, job.value)  # noqa: SLF001
    assert await ctl.in_flight_operation() is None


async def test_stop_rejected_while_start_in_flight() -> None:
    ssh = FakeSsh()
    ctl = make_control(ssh)
    release = asyncio.Event()
    job = await ctl._jobs.submit(  # noqa: SLF001
        "start:qwen3.8-flash-next", executor=SlowExecutor(release), timeout_s=30
    )
    assert is_ok(job)
    res = await ctl.stop("glm5.3-flash")
    assert isinstance(res, Err)
    assert res.error.kind == ControlErrorKind.OP_IN_PROGRESS
    release.set()
    await wait_terminal(ctl._jobs, job.value)  # noqa: SLF001


async def test_concurrent_double_start_submits_only_one_job() -> None:
    """Two start calls racing on the same lane -> exactly one job, 409 other."""
    ssh = FakeSsh()
    ctl = make_control(ssh)
    r1, r2 = await asyncio.gather(
        ctl.start("qwen3.8-flash-next"), ctl.start("qwen3.8-flash-next")
    )
    oks = [r for r in (r1, r2) if is_ok(r)]
    errs = [r for r in (r1, r2) if not is_ok(r)]
    assert len(oks) == 1
    assert len(errs) == 1
    assert errs[0].error.kind == ControlErrorKind.OP_IN_PROGRESS

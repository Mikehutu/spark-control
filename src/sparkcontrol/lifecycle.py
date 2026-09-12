"""Cluster execution + server lifecycle control (slice 3).

Wraps the EXISTING per-recipe start/stop scripts over SSH. The gateway never
reimplements recipe logic — it only: detects what is running, arbitrates the
canonical port (with explicit-confirmation gate), runs the wrapped scripts as
jobs, and waits for health.

All SSH goes through the :class:`SshClient` protocol so this module is fully
unit-testable with a fake client (no cluster needed). asyncssh wiring lives in
the deployment slice; only the protocol is defined here.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from .jobs import JobRecord, JobRunner
from .recipes import Recipe, RecipeRegistry
from .result import Err, Ok, Result


class ControlErrorKind(StrEnum):
    """Why a control operation failed (explicit states, never generic 500)."""

    UNREACHABLE = "unreachable"          # node/SSH unreachable
    NO_CONTAINER = "no-container"        # container not found
    PORT_HELD = "port-held"              # canonical port held by another recipe
    PORT_HELD_NEEDS_CONFIRM = "port-held-needs-confirm"  # swap requires user OK
    OP_IN_PROGRESS = "operation-in-progress"  # another start/stop job already in flight
    START_FAILED = "start-failed"        # start script exited nonzero
    STOP_FAILED = "stop-failed"          # stop script exited nonzero
    TIMEOUT = "timeout"                  # health never arrived
    UNKNOWN_RECIPE = "unknown-recipe"


@dataclass(frozen=True)
class ControlError:
    kind: ControlErrorKind
    detail: str


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout: str = ""
    stderr: str = ""


class SshClient(Protocol):
    """Minimal async SSH surface (asyncssh adapter implements it in deploy)."""

    async def run(self, host: str, command: str, timeout: float = 60.0) -> CommandResult:
        """Run *command* on *host*; raise ConnectionError when unreachable."""
        ...


class ClusterExecutor(SshClient, Protocol):
    """SshClient plus container detection (docker inspect by compose label)."""

    async def container_state(self, host: str, compose_project: str) -> str | None:
        """'running' | 'exited' | None (no container for that project)."""
        ...


@dataclass(frozen=True)
class NodeStatus:
    host: str
    reachable: bool
    gpu_mem_used_gb: float | None = None
    uptime_s: float | None = None


@dataclass(frozen=True)
class ServerStatus:
    """Combined cluster state for the UI (FR-3)."""

    state: str  # stopped | starting | running | unreachable
    active_recipe: str | None
    vllm_healthy: bool
    model: str | None
    nodes: tuple[NodeStatus, ...]


def parse_docker_state(stdout: str) -> str | None:
    """Parse `docker ps --filter label=... --format '{{.State}}'` output."""
    states = [line.strip().lower() for line in stdout.splitlines() if line.strip()]
    if not states:
        return None
    if "running" in states:
        return "running"
    return states[0]


def parse_nvidia_smi(stdout: str) -> float | None:
    """Parse `nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits`.

    GB10 unified memory often reports [N/A]; return None then.
    """
    for line in stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("[N/A]") or "N/A" in line:
            continue
        try:
            return round(float(line) / 1024.0, 1)  # MiB -> GiB
        except ValueError:
            continue
    return None


class RecipeScriptExecutor:
    """ActionExecutor that runs a recipe's wrapped start/stop script over SSH.

    The mode ('start' | 'stop') is fixed at construction — the action string
    passed by the job runner is only a log label (e.g. "stop:<recipe_id>").

    'start' = run start script (it backgrounds the compose stack) then poll
    health until ready or timeout.
    'stop'  = run stop script for the recipe's compose project.
    """

    def __init__(
        self,
        ssh: SshClient,
        recipe: Recipe,
        health_url: str,
        mode: str,
        head_host: str | None = None,
        health_poll_s: float = 10.0,
        start_timeout_s: float | None = None,
    ) -> None:
        if mode not in ("start", "stop"):
            raise ValueError(f"executor mode must be 'start' or 'stop', got: {mode}")
        self._ssh = ssh
        self._recipe = recipe
        self._health_url = health_url
        self._mode = mode
        self._head_host = head_host or recipe.head()
        self._health_poll_s = health_poll_s
        self._start_timeout_s = start_timeout_s or recipe.start_timeout_s

    async def execute(self, action: str, on_output: Callable[[str], None]) -> int:
        head = self._head_host
        if self._mode == "stop":
            on_output(f"stop: running {self._recipe.stop_script} on {head}")
            res = await self._ssh.run(head, self._recipe.stop_script, timeout=300.0)
            for line in (res.stdout + res.stderr).splitlines()[-20:]:
                on_output(line)
            return res.exit_code
        on_output(f"start: running {self._recipe.start_script} on {head}")
        res = await self._ssh.run(
            head, self._recipe.start_script, timeout=self._start_timeout_s
        )
        for line in (res.stdout + res.stderr).splitlines()[-30:]:
            on_output(line)
        if res.exit_code != 0:
            return res.exit_code
        on_output("start: script finished, polling health")
        return await self._wait_healthy(on_output)

    async def _wait_healthy(self, on_output: Callable[[str], None]) -> int:
        deadline = asyncio.get_running_loop().time() + self._start_timeout_s
        while asyncio.get_running_loop().time() < deadline:
            if await _health_ok(self._health_url):
                on_output("health: OK")
                return 0
            await asyncio.sleep(self._health_poll_s)
        on_output(f"health: TIMEOUT after {self._start_timeout_s}s")
        return 124  # conventional timeout exit code


async def _health_ok(url: str) -> bool:
    """GET url; True on HTTP 200. Runs in a thread to stay non-blocking."""

    def probe() -> bool:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - fixed http URL
                return bool(resp.status == 200)
        except (urllib.error.URLError, OSError, TimeoutError):
            return False

    return await asyncio.to_thread(probe)


class ServerControl:
    """IServerControl: recipe-aware start/stop with confirmation-gated swaps."""

    def __init__(
        self,
        registry: RecipeRegistry,
        ssh: SshClient,
        jobs: JobRunner,
        health_url_template: str,
        node_map: dict[str, str],
        health_poll_s: float = 10.0,
        swap_poll_s: float = 5.0,
        op_lock: asyncio.Lock | None = None,
    ) -> None:
        self._registry = registry
        self._ssh = ssh
        self._jobs = jobs
        self._health_url_template = health_url_template  # e.g. http://{host}:{port}/health
        self._node_map = node_map  # recipe node key -> address
        self._health_poll_s = health_poll_s
        self._swap_poll_s = swap_poll_s
        #: Serializes start/stop arbitration + submission (one lane = one op).
        self._op_lock = op_lock or asyncio.Lock()

    def health_url(self, recipe: Recipe) -> str:
        host = self._host(recipe.head())
        return self._health_url_template.format(host=host, port=recipe.port)

    def _host(self, node_key: str) -> str:
        """Resolve a recipe node key to its address via the node map."""
        return self._node_map.get(node_key, node_key)

    async def detect_active(self) -> Result[str | None, ControlError]:
        """Which recipe currently holds the canonical port (by containers)?"""
        head = self._registry.all()[0].head() if self._registry.all() else None
        if head is None:
            return Ok(None)
        for recipe in self._registry.all():
            try:
                state = await self._container_state(recipe)
            except ConnectionError:
                return Err(
                    ControlError(ControlErrorKind.UNREACHABLE, f"head node {head} unreachable")
                )
            if state == "running":
                return Ok(recipe.recipe_id)
        return Ok(None)

    async def _container_state(self, recipe: Recipe) -> str | None:
        """Container state for *recipe* on its head node.

        Primary: name-anchored `docker ps` filter (running containers only).
        Live recon 2026-09-12: GLM/Qwen are raw-docker containers WITHOUT a
        com.docker.compose.project label, so label-based detection reports a
        healthy running server as stopped. Container NAMES are the reliable
        anchor for both raw-docker and compose deployments.

        Returns "running" when any listed container is up, None otherwise
        (absent or stopped — the wrapped start/stop scripts are idempotent
        and handle a stopped-but-existing container themselves).
        """
        head = self._host(recipe.head())
        if recipe.containers:
            for name in recipe.containers:
                cmd = (
                    "docker ps --filter "
                    f"'name=^{name}$' --format '{{{{.Names}}}}'"
                )
                res = await self._ssh.run(head, cmd, timeout=15.0)
                if res.exit_code != 0:
                    raise ConnectionError(f"docker ps failed on {head}: {res.stderr}")
                if any(line.strip() for line in res.stdout.splitlines()):
                    return "running"
            return None
        # Fallback: recipes with no container names use the compose label.
        cmd = (
            "docker ps --filter "
            f"'label=com.docker.compose.project={recipe.compose_project}' "
            "--format '{{.State}}'"
        )
        res = await self._ssh.run(head, cmd, timeout=15.0)
        if res.exit_code != 0:
            raise ConnectionError(f"docker ps failed on {head}: {res.stderr}")
        return parse_docker_state(res.stdout)

    async def in_flight_operation(self) -> JobRecord | None:
        """Most recent non-terminal start/stop job, if any (lane busy flag).

        The authoritative source is the persisted ledger, not task bookkeeping,
        so it survives page reloads and an HTTP request racing a swap.
        """
        candidates = [
            r
            for r in self._jobs.non_terminal()
            if r.action.startswith(("start:", "stop:"))
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: r.created_at)

    async def _op_conflict(self) -> ControlError | None:
        """Err when a control operation is already in flight (fast-fail 409)."""
        inflight = await self.in_flight_operation()
        if inflight is None:
            return None
        return ControlError(
            ControlErrorKind.OP_IN_PROGRESS,
            f"{inflight.action} already in progress (job {inflight.job_id})",
        )

    async def start(
        self, recipe_id: str, confirm_swap: bool = False
    ) -> Result[str, ControlError]:
        """Start a recipe. Idempotent; confirmation-gated when a swap is needed.

        Returns Ok(job_id). Err(PORT_HELD_NEEDS_CONFIRM) when another recipe
        holds the port/nodes and confirm_swap is False — the UI must show the
        conflict and re-call with confirm_swap=True (Mike decision 2026-09-12:
        never a surprise stop).

        Lane serialization (live incident 2026-09-12): at most ONE start/stop
        operation in flight. While another control job is pending/running, this
        fails fast with Err(OP_IN_PROGRESS) — a second tap can never launch a
        concurrent recipe on the same :8888 lane.
        """
        conflict = await self._op_conflict()
        if conflict is not None:
            return Err(conflict)
        async with self._op_lock:
            # Double-checked: two concurrent requests can both pass the pre-check
            # before either submits; the lock + second check picks the loser.
            conflict = await self._op_conflict()
            if conflict is not None:
                return Err(conflict)
            return await self._start(recipe_id, confirm_swap)

    async def _start(
        self, recipe_id: str, confirm_swap: bool = False
    ) -> Result[str, ControlError]:
        try:
            recipe = self._registry.get(recipe_id)
        except Exception as exc:  # RecipeError
            return Err(ControlError(ControlErrorKind.UNKNOWN_RECIPE, str(exc)))

        # Already running / starting?
        try:
            state = await self._container_state(recipe)
        except ConnectionError:
            return Err(
                ControlError(ControlErrorKind.UNREACHABLE, f"head {recipe.head()} unreachable")
            )
        if state == "running" and await _health_ok(self.health_url(recipe)):
            return Ok(f"already-up:{recipe_id}")

        # Port arbitration: is another recipe holding the lane?
        active = await self.detect_active()
        if isinstance(active, Err):
            return active
        holder = active.value
        if holder is not None and holder != recipe_id:
            conflicting = {c.recipe_id for c in self._registry.conflicting(recipe)}
            if holder in conflicting:
                if not confirm_swap:
                    return Err(
                        ControlError(
                            ControlErrorKind.PORT_HELD_NEEDS_CONFIRM,
                            f"{holder} holds the lane; confirm to stop it and start {recipe_id}",
                        )
                    )
                stop_res = await self._stop(holder)
                if isinstance(stop_res, Err):
                    return stop_res
                if not stop_res.value.startswith("already-stopped"):
                    ok = await self._wait_job(stop_res.value)
                    if not ok:
                        return Err(
                            ControlError(
                                ControlErrorKind.STOP_FAILED,
                                f"swap stop of {holder} failed (job {stop_res.value})",
                            )
                        )

        executor = RecipeScriptExecutor(
            self._ssh,
            recipe,
            self.health_url(recipe),
            mode="start",
            head_host=self._host(recipe.head()),
            health_poll_s=self._health_poll_s,
            start_timeout_s=recipe.start_timeout_s,
        )
        job = await self._jobs.submit(
            f"start:{recipe_id}", executor=executor, timeout_s=recipe.start_timeout_s + 60
        )
        if isinstance(job, Err):
            return Err(ControlError(ControlErrorKind.START_FAILED, str(job.error)))
        return Ok(job.value)

    async def stop(self, recipe_id: str) -> Result[str, ControlError]:
        """Stop a recipe by compose project. Idempotent.

        Serialized like :meth:`start`: fails fast with OP_IN_PROGRESS while
        another control operation is in flight.
        """
        conflict = await self._op_conflict()
        if conflict is not None:
            return Err(conflict)
        async with self._op_lock:
            conflict = await self._op_conflict()
            if conflict is not None:
                return Err(conflict)
            return await self._stop(recipe_id)

    async def _stop(self, recipe_id: str) -> Result[str, ControlError]:
        try:
            recipe = self._registry.get(recipe_id)
        except Exception as exc:  # RecipeError
            return Err(ControlError(ControlErrorKind.UNKNOWN_RECIPE, str(exc)))
        try:
            state = await self._container_state(recipe)
        except ConnectionError:
            return Err(
                ControlError(ControlErrorKind.UNREACHABLE, f"head {recipe.head()} unreachable")
            )
        if state is None:
            return Ok(f"already-stopped:{recipe_id}")
        executor = RecipeScriptExecutor(
            self._ssh,
            recipe,
            self.health_url(recipe),
            mode="stop",
            head_host=self._host(recipe.head()),
        )
        job = await self._jobs.submit(f"stop:{recipe_id}", executor=executor, timeout_s=360.0)
        if isinstance(job, Err):
            return Err(ControlError(ControlErrorKind.STOP_FAILED, str(job.error)))
        return Ok(job.value)

    async def _wait_job(self, job_id: str, timeout_s: float = 600.0) -> bool:
        """Block until a job reaches a terminal state; True if done."""
        from .jobs import JobState

        poll_s = self._swap_poll_s
        waited = 0.0
        while waited < timeout_s:
            res = await self._jobs.status(job_id)
            if isinstance(res, Ok):
                rec = res.value
                if rec.state == JobState.DONE:
                    return True
                if rec.state in (JobState.FAILED, JobState.UNKNOWN):
                    return False
            await asyncio.sleep(poll_s)
            waited += poll_s
        return False

    @property
    def jobs(self) -> JobRunner:
        """Runner this control submits to (exposed for the jobs API)."""
        return self._jobs

    @property
    def registry(self) -> RecipeRegistry:
        """Recipe registry (read-only use: UI catalog)."""
        return self._registry

    async def server_status(self) -> ServerStatus:
        """FR-3 combined status (probe both nodes + health + model)."""
        nodes: list[NodeStatus] = []
        seen: set[str] = set()
        for recipe in self._registry.all():
            for node_key in recipe.nodes:
                if node_key in seen:
                    continue
                seen.add(node_key)
                host = self._node_map.get(node_key, node_key)
                nodes.append(await self._probe_node(node_key, host))

        active = await self.detect_active()
        active_id = active.value if isinstance(active, Ok) else None

        healthy = False
        model: str | None = None
        if active_id is not None:
            recipe = self._registry.get(active_id)
            base_url = self.health_url(recipe).rsplit("/health", 1)[0]
            healthy = await _health_ok(self.health_url(recipe))
            if healthy:
                model = await self._served_model(base_url)
                if model is not None:
                    # DS text/vision share container name + compose project —
                    # resolve which recipe is really serving by the model id.
                    active_id = self._match_served_model(active_id, model)

        if isinstance(active, Err) or all(not n.reachable for n in nodes):
            state = "unreachable"
        elif active_id is None:
            state = "stopped"
        elif healthy:
            state = "running"
        else:
            state = "starting"
        return ServerStatus(
            state=state,
            active_recipe=active_id,
            vllm_healthy=healthy,
            model=model,
            nodes=tuple(nodes),
        )

    async def _probe_node(self, node_key: str, host: str) -> NodeStatus:
        try:
            res = await self._ssh.run(
                host,
                "nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits"
                "; cat /proc/uptime",
                timeout=10.0,
            )
        except ConnectionError:
            return NodeStatus(host=node_key, reachable=False)
        if res.exit_code != 0:
            return NodeStatus(host=node_key, reachable=True)
        lines = [line for line in res.stdout.splitlines() if line.strip()]
        gpu = parse_nvidia_smi(lines[0]) if lines else None
        uptime: float | None = None
        for line in lines[1:]:
            try:
                uptime = float(line.split()[0])
                break
            except (ValueError, IndexError):
                continue
        return NodeStatus(host=node_key, reachable=True, gpu_mem_used_gb=gpu, uptime_s=uptime)

    async def _served_model(self, base_url: str) -> str | None:
        return await _fetch_served_model(base_url)

    def _match_served_model(self, active_id: str, model: str) -> str:
        """Resolve the recipe whose served_model matches the live model id.

        Container-name detection is ambiguous for DS text vs vision (shared
        compose project + container); the served model id is the truth.
        """
        for recipe in self._registry.all():
            if recipe.served_model == model:
                return recipe.recipe_id
        return active_id


async def _fetch_served_model(base_url: str) -> str | None:
    """GET <base_url>/v1/models and return the first served model id.

    Module-level function so tests can patch it (no real network in units).
    """

    def fetch() -> str | None:
        try:
            with urllib.request.urlopen(f"{base_url}/v1/models", timeout=5) as resp:  # noqa: S310
                data = json.loads(resp.read().decode())
                items = data.get("data", [])
                return str(items[0]["id"]) if items else None
        except (urllib.error.URLError, OSError, TimeoutError, ValueError, KeyError, IndexError):
            return None

    return await asyncio.to_thread(fetch)

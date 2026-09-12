"""Unit tests for the job-runner slice (FR-4).

Covers job lifecycle (pending->running->done/failed), timeout, unknown-job lookup,
gateway-restart reconciliation, and bounded output tail. Uses an in-memory
SQLite store and a controllable fake ActionExecutor so no cluster is touched.
"""

from __future__ import annotations

import asyncio
import time

from sparkcontrol.jobs import (
    TERMINAL_STATES,
    JobRecord,
    JobRunner,
    JobState,
    SqliteJobStore,
)
from sparkcontrol.result import is_ok


class FakeExecutor:
    """Controllable ActionExecutor: emits lines, returns a configured code."""

    def __init__(self, exit_code: int = 0, lines: list[str] | None = None, delay: float = 0.0):
        self.exit_code = exit_code
        self.lines = lines or ["line-1", "line-2"]
        self.delay = delay
        self.emitted: list[str] = []

    async def execute(self, action: str, on_output) -> int:  # type: ignore[no-untyped-def]
        if self.delay:
            await asyncio.sleep(self.delay)
        for line in self.lines:
            self.emitted.append(line)
            on_output(line)
        return self.exit_code


class HangingExecutor:
    async def execute(self, action: str, on_output) -> int:  # type: ignore[no-untyped-def]
        await asyncio.sleep(3600)
        return 0


def make_runner(executor=None, timeout_s: float = 5.0, tail_limit: int = 50):
    store = SqliteJobStore(":memory:")
    return JobRunner(store, executor or FakeExecutor(), timeout_s=timeout_s, tail_limit=tail_limit)


async def wait_terminal(runner: JobRunner, job_id: str, timeout: float = 3.0) -> JobRecord:
    deadline = time.monotonic() + timeout
    res = await runner.status(job_id)
    assert is_ok(res), f"job not found: {job_id}"
    rec = res.value
    while rec.state not in TERMINAL_STATES and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
        res = await runner.status(job_id)
        assert is_ok(res)
        rec = res.value
    return rec


async def test_submit_success_reaches_done():
    runner = make_runner()
    res = await runner.submit("start")
    assert is_ok(res)
    rec = await wait_terminal(runner, res.value)
    assert rec.state == JobState.DONE
    assert rec.result == "ok"
    assert rec.finished_at is not None
    assert rec.duration_s is not None
    assert "line-1" in rec.output_tail


async def test_submit_failure_marks_failed():
    runner = make_runner(FakeExecutor(exit_code=1))
    res = await runner.submit("stop")
    rec = await wait_terminal(runner, res.value)
    assert rec.state == JobState.FAILED
    assert rec.result == "exit code 1"


async def test_timeout_marks_failed():
    runner = make_runner(HangingExecutor(), timeout_s=0.05)
    res = await runner.submit("start")
    rec = await wait_terminal(runner, res.value)
    assert rec.state == JobState.FAILED
    assert "timeout" in (rec.result or "")


async def test_status_unknown_job_returns_lookup_error():
    runner = make_runner()
    res = await runner.status("does-not-exist")
    assert not is_ok(res)
    assert isinstance(res.error, LookupError)


async def test_reconcile_marks_pending_running_unknown():
    runner = make_runner()
    store = runner._store  # type: ignore[attr-defined]
    # Pre-seed an orphaned PENDING job (as if it was in-flight when the gateway died).
    orphan = JobRecord(
        job_id="orphan-1",
        action="start",
        state=JobState.PENDING,
        created_at=time.time(),
    )
    store.upsert(orphan)
    ids = await runner.reconcile()
    assert "orphan-1" in ids
    rec = store.get("orphan-1")
    assert rec is not None
    assert rec.state == JobState.UNKNOWN


async def test_output_tail_is_bounded():
    runner = make_runner(FakeExecutor(lines=[f"l{i}" for i in range(500)]), tail_limit=10)
    res = await runner.submit("start")
    rec = await wait_terminal(runner, res.value)
    assert len(rec.output_tail) <= 10
    assert rec.output_tail[-1] == "l499"


async def test_submit_generates_distinct_jobs():
    runner = make_runner()
    r1 = await runner.submit("start")
    r2 = await runner.submit("stop")
    assert is_ok(r1) and is_ok(r2)
    assert r1.value != r2.value

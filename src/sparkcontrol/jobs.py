"""Async job registry for Spark Control (FR-4).

``JobRunner`` owns the lifecycle of start/stop actions: it creates a job,
executes the action asynchronously against an injected ``ActionExecutor``,
buffers a bounded output tail, and persists every state change to SQLite so
jobs survive a gateway restart (via ``reconcile``).

The runner is deliberately blind to *what* the action does — it only manages
job state, timing, and persistence. ``server-lifecycle`` supplies the real
executor (asyncssh over the recipes). This keeps the job-runner slice
independently testable.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from .result import Err, Ok, Result

# Action strings are free-form log labels: "start", "stop", or recipe-scoped
# "start:<recipe_id>" / "stop:<recipe_id>" from server-lifecycle.
Action = str

#: Maximum number of output lines retained per job.
DEFAULT_TAIL_LIMIT = 50

# Shared column list so SELECT statements stay under line length.
_SELECT_COLUMNS = (
    "SELECT job_id, action, state, created_at, finished_at, "
    "duration_s, result, output_tail FROM jobs"
)


class JobState(StrEnum):
    """Lifecycle state of a job."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    UNKNOWN = "unknown"  # orphaned by a gateway restart


TERMINAL_STATES = frozenset({JobState.DONE, JobState.FAILED, JobState.UNKNOWN})


@dataclass
class JobRecord:
    """A persisted job, including its bounded output tail."""

    job_id: str
    action: str
    state: JobState
    created_at: float
    finished_at: float | None = None
    duration_s: float | None = None
    result: str | None = None
    output_tail: list[str] = field(default_factory=list)

    def append_output(self, line: str, tail_limit: int = DEFAULT_TAIL_LIMIT) -> None:
        """Append an output line, keeping the tail bounded."""
        self.output_tail.append(line)
        if len(self.output_tail) > tail_limit:
            self.output_tail = self.output_tail[-tail_limit:]

    def mark_finished(self, exit_code: int, tail_limit: int = DEFAULT_TAIL_LIMIT) -> None:
        """Set the terminal state and duration based on the exit code."""
        self.finished_at = time.time()
        self.duration_s = self.finished_at - self.created_at
        if exit_code == 0:
            self.state = JobState.DONE
            self.result = "ok"
        else:
            self.state = JobState.FAILED
            self.result = f"exit code {exit_code}"


class ActionExecutor(Protocol):
    """Executes a start/stop action, streaming output lines to ``on_output``."""

    async def execute(self, action: str, on_output: Callable[[str], None]) -> int:
        """Run *action*; return an exit code (0 = success)."""
        ...


class JobStore(Protocol):
    """Persistence contract for job records."""

    def upsert(self, record: JobRecord) -> None: ...
    def get(self, job_id: str) -> JobRecord | None: ...
    def non_terminal(self) -> list[JobRecord]: ...
    def all_recent(self, limit: int = 20) -> list[JobRecord]: ...


class SqliteJobStore:
    """SQLite-backed job store. One file; schema created on first use.

    The connection is created with ``check_same_thread=False`` and guarded by
    a lock: the gateway event loop may run in a different thread than the one
    that constructed the store (uvicorn worker vs. entrypoint, TestClient
    portal). All access is serialized; concurrent multi-loop use is out of
    scope (single gateway process).
    """

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id      TEXT PRIMARY KEY,
                action      TEXT NOT NULL,
                state       TEXT NOT NULL,
                created_at  REAL NOT NULL,
                finished_at REAL,
                duration_s  REAL,
                result      TEXT,
                output_tail TEXT NOT NULL DEFAULT '[]'
            )
            """
        )
        self._conn.commit()

    def upsert(self, record: JobRecord) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO jobs (job_id, action, state, created_at, finished_at,
                                  duration_s, result, output_tail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    state = excluded.state,
                    finished_at = excluded.finished_at,
                    duration_s = excluded.duration_s,
                    result = excluded.result,
                    output_tail = excluded.output_tail
                """,
                (
                    record.job_id,
                    record.action,
                    record.state.value,
                    record.created_at,
                    record.finished_at,
                    record.duration_s,
                    record.result,
                    json.dumps(record.output_tail),
                ),
            )
            self._conn.commit()

    def all_recent(self, limit: int = 20) -> list[JobRecord]:
        """Newest job records (bounded read for the jobs page)."""
        with self._lock:
            rows = self._conn.execute(
                _SELECT_COLUMNS + " ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            row = self._conn.execute(
                _SELECT_COLUMNS + " WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def non_terminal(self) -> list[JobRecord]:
        with self._lock:
            rows = self._conn.execute(
                _SELECT_COLUMNS + " WHERE state NOT IN (?, ?, ?)",
                (JobState.DONE.value, JobState.FAILED.value, JobState.UNKNOWN.value),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _row_to_record(row: tuple[Any, ...]) -> JobRecord:
        (
            job_id,
            action,
            state,
            created_at,
            finished_at,
            duration_s,
            result,
            output_tail,
        ) = row
        return JobRecord(
            job_id=job_id,
            action=action,
            state=JobState(state),
            created_at=created_at,
            finished_at=finished_at,
            duration_s=duration_s,
            result=result,
            output_tail=json.loads(output_tail) if output_tail else [],
        )


class JobRunner:
    """Implements :class:`IJobRunner` semantics: submit / status / reconcile."""

    def __init__(
        self,
        store: JobStore,
        executor: ActionExecutor | None = None,
        timeout_s: float = 600,
        tail_limit: int = DEFAULT_TAIL_LIMIT,
    ) -> None:
        self._store = store
        self._executor = executor
        self._timeout_s = timeout_s
        self._tail_limit = tail_limit
        self._tasks: dict[str, asyncio.Task[None]] = {}

    async def submit(
        self,
        action: Action,
        executor: ActionExecutor | None = None,
        timeout_s: float | None = None,
    ) -> Result[str, Exception]:
        """Create a job and execute *action* asynchronously.

        ``executor``/``timeout_s`` override the runner defaults for this job
        only (used by server-lifecycle for recipe-scoped execution and
        per-recipe start timeouts).
        """
        effective = executor or self._executor
        if effective is None:
            return Err(RuntimeError("no executor: pass one to submit() or the runner"))
        job_id = uuid.uuid4().hex
        record = JobRecord(
            job_id=job_id,
            action=action,
            state=JobState.PENDING,
            created_at=time.time(),
        )
        self._store.upsert(record)
        task = asyncio.create_task(
            self._run(record, effective, timeout_s or self._timeout_s)
        )
        self._tasks[job_id] = task
        return Ok(job_id)

    async def _run(
        self, record: JobRecord, executor: ActionExecutor, timeout_s: float
    ) -> None:
        record.state = JobState.RUNNING
        self._store.upsert(record)

        def emit(line: str) -> None:
            record.append_output(line, self._tail_limit)
            self._store.upsert(record)

        try:
            exit_code = await asyncio.wait_for(
                executor.execute(record.action, emit), timeout=timeout_s
            )
            record.mark_finished(exit_code, self._tail_limit)
        except TimeoutError:
            record.state = JobState.FAILED
            record.finished_at = time.time()
            record.duration_s = record.finished_at - record.created_at
            record.result = f"timeout after {timeout_s}s"
        except Exception as exc:  # noqa: BLE001 - any executor failure marks the job failed
            record.state = JobState.FAILED
            record.finished_at = time.time()
            record.duration_s = record.finished_at - record.created_at
            record.result = f"executor error: {type(exc).__name__}: {exc}"
        finally:
            self._store.upsert(record)

    async def status(self, job_id: str) -> Result[JobRecord, LookupError]:
        """Return the current job record; Err(LookupError) on unknown id."""
        record = self._store.get(job_id)
        if record is None:
            return Err(LookupError(job_id))
        return Ok(record)

    def non_terminal(self) -> list[JobRecord]:
        """Ledger read: jobs still PENDING or RUNNING (sync, store access).

        Used by the lane-serialization guard to detect an in-flight control
        operation without waiting on task bookkeeping.
        """
        return self._store.non_terminal()

    def recent(self, limit: int = 20) -> list[JobRecord]:
        """Newest job records (sync store read) for the jobs page."""
        return self._store.all_recent(limit)

    async def reconcile(self) -> list[str]:
        """Mark orphaned (PENDING/RUNNING) jobs UNKNOWN after a gateway restart.

        Returns the reconciled job ids. Deep re-derivation of actual cluster
        state is the status-reporter/server-lifecycle concern; here we only
        repair the ledger so no job stays stuck in a non-terminal state.
        """
        orphaned: list[str] = []
        for record in self._store.non_terminal():
            if record.state in (JobState.PENDING, JobState.RUNNING):
                record.state = JobState.UNKNOWN
                record.result = "orphaned by gateway restart"
                record.finished_at = time.time()
                record.duration_s = record.finished_at - record.created_at
                self._store.upsert(record)
                orphaned.append(record.job_id)
        return orphaned

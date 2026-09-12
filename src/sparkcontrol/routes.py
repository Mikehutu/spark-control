"""HTTP surface for server control (slice 4).

Thin adapters between FastAPI and ``ServerControl``. The FR-3 status logic
lives in ``lifecycle.py`` — this module only maps Results to HTTP responses:

- ``GET  /api/server/status``      -> 200 tri-state (``unreachable`` is a state, not an error)
- ``POST /api/server/{id}/start``  -> 202 job accepted; 409 confirm gate
                                     404 unknown recipe; 503 unreachable
- ``POST /api/server/{id}/stop``   -> 202 job accepted; 200 no-op (already stopped); 404; 503
- ``GET  /api/jobs/{id}``          -> 200 job record (FR-4 poll); 404

Job ids returned here are polled via ``/api/jobs/{id}`` by the UI.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, NoReturn, cast

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .deps import get_current_auth
from .errors import ApiError
from .jobs import JobRunner
from .lifecycle import ControlError, ControlErrorKind, ServerControl
from .result import Err, Ok


class StartBody(BaseModel):
    """Optional body of POST /server/{id}/start."""

    confirm_swap: bool = False


_KIND_STATUS: dict[ControlErrorKind, int] = {
    ControlErrorKind.UNREACHABLE: 503,
    ControlErrorKind.UNKNOWN_RECIPE: 404,
    ControlErrorKind.PORT_HELD_NEEDS_CONFIRM: 409,
    ControlErrorKind.OP_IN_PROGRESS: 409,
}


def _raise_control(err: ControlError) -> NoReturn:
    """Map a ControlError to the uniform error envelope."""
    status = _KIND_STATUS.get(err.kind, 500)
    code = err.kind.value if err.kind in _KIND_STATUS else "control-error"
    raise ApiError(status, code, err.detail)


def _control(request: Request) -> ServerControl:
    return cast(ServerControl, request.app.state.control)


def _jobs(request: Request) -> JobRunner:
    return cast(JobRunner, request.app.state.jobs)


def _job_payload(rec: Any) -> dict[str, Any]:  # JobRecord (Any avoids import cycle noise)
    return {
        "job_id": rec.job_id,
        "action": rec.action,
        "state": rec.state.value,
        "created_at": rec.created_at,
        "finished_at": rec.finished_at,
        "duration_s": rec.duration_s,
        "result": rec.result,
        "output_tail": list(rec.output_tail),
    }


def build_control_router() -> APIRouter:
    """Routes that require an injected ServerControl (see create_app)."""
    router = APIRouter(prefix="/api", dependencies=[Depends(get_current_auth)])

    @router.get("/server/status")
    async def server_status(request: Request) -> dict[str, Any]:
        status = await _control(request).server_status()
        return {
            "state": status.state,
            "active_recipe": status.active_recipe,
            "vllm_healthy": status.vllm_healthy,
            "model": status.model,
            "nodes": [asdict(n) for n in status.nodes],
        }

    @router.post("/server/{recipe_id}/start")
    async def server_start(
        recipe_id: str, request: Request, body: StartBody | None = None
    ) -> JSONResponse:
        result = await _control(request).start(
            recipe_id, confirm_swap=bool(body and body.confirm_swap)
        )
        if isinstance(result, Err):
            _raise_control(result.error)
        job_id = result.value
        if job_id.startswith("already-up:"):
            return JSONResponse(
                status_code=200, content={"state": "no-op", "result": job_id}
            )
        return JSONResponse(
            status_code=202,
            content={"job_id": job_id, "action": f"start:{recipe_id}"},
        )

    @router.post("/server/{recipe_id}/stop")
    async def server_stop(recipe_id: str, request: Request) -> JSONResponse:
        result = await _control(request).stop(recipe_id)
        if isinstance(result, Err):
            _raise_control(result.error)
        job_id = result.value
        if job_id.startswith("already-stopped:"):
            return JSONResponse(
                status_code=200, content={"state": "no-op", "result": job_id}
            )
        return JSONResponse(
            status_code=202,
            content={"job_id": job_id, "action": f"stop:{recipe_id}"},
        )

    @router.get("/jobs/recent")
    async def jobs_recent(request: Request, limit: int = 20) -> dict[str, Any]:
        """Newest job records (bounded) for the jobs page."""
        capped = max(1, min(int(limit), 100))
        records = _jobs(request).recent(capped)
        return {"jobs": [_job_payload(r) for r in records]}

    @router.get("/jobs/{job_id}")
    async def job_status(job_id: str, request: Request) -> dict[str, Any]:
        result = await _jobs(request).status(job_id)
        if isinstance(result, Err):
            raise ApiError(404, "not_found", f"unknown job: {job_id}")
        return _job_payload(result.value)

    return router


# Ok/Err are re-exported for type clarity in route bodies above.
_ = Ok

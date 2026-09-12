"""FastAPI application factory for the Spark Control gateway.

Slice 1 only: an app skeleton with a healthcheck route, a bearer-token auth
gate (FR-5), and a uniform error envelope. Recipe start/stop, status, and job
routes arrive in later slices.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .auth import BearerAuth
from .config import Settings
from .deps import get_current_auth
from .errors import ApiError, error_payload
from .lifecycle import ServerControl
from .result import Ok
from .routes import build_control_router
from .ui import recipe_catalog, view_model


def create_app(settings: Settings | None = None, control: ServerControl | None = None) -> FastAPI:
    """Build the Spark Control FastAPI application.

    ``control`` is injected by the deployment entrypoint (wired in the
    deployment slice); without it the gateway serves only health/ping.
    """
    settings = settings or Settings.from_env()
    app = FastAPI(title="Spark Control", version="0.1.0")
    app.state.auth = BearerAuth(settings.token)
    app.state.control = control
    app.state.settings = settings
    if control is not None:
        app.state.jobs = control.jobs
        app.include_router(build_control_router())

    @app.exception_handler(ApiError)
    async def _handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=error_payload(exc.code, exc.detail),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _handle_http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code_ = "not_found" if exc.status_code == 404 else "http_error"
        return JSONResponse(
            status_code=exc.status_code,
            content=error_payload(code_, str(exc.detail)),
        )

    @app.exception_handler(Exception)
    async def _handle_unhandled(request: Request, exc: Exception) -> JSONResponse:
        # No token/sensitive info here; the detail is deliberately opaque.
        return JSONResponse(
            status_code=500,
            content=error_payload("internal_error", "Internal server error"),
        )

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok"}

    api = APIRouter(prefix="/api", dependencies=[Depends(get_current_auth)])

    @api.get("/ping")
    async def ping() -> dict[str, bool]:
        return {"ok": True}

    if control is not None:
        @api.get("/ui")
        async def ui_view(job_id: str | None = None) -> dict[str, Any]:
            """View-model for the dashboard shell (auth-gated, zero secrets)."""
            status = await control.server_status()

            def _snapshot(rec: Any) -> dict[str, Any]:
                return {
                    "job_id": rec.job_id,
                    "action": rec.action,
                    "state": rec.state.value,
                }

            active_job: dict[str, Any] | None = None
            if job_id:
                res = await control.jobs.status(job_id)
                if isinstance(res, Ok):
                    active_job = _snapshot(res.value)
            else:
                inflight = await control.in_flight_operation()
                if inflight is not None:
                    active_job = _snapshot(inflight)
            catalog = recipe_catalog(control.registry, status.active_recipe)
            default_recipe = (
                settings.default_recipe or (catalog[0]["recipe_id"] if catalog else "")
            )
            return view_model(
                status,
                active_job=active_job,
                recipes=catalog,
                monitors={
                    "sparkdash": settings.monitor_sparkdash_url,
                    "grafana": settings.monitor_grafana_url,
                },
                default_recipe=default_recipe,
            )

    app.include_router(api)

    if control is not None:
        # Static dashboard shell. Unauthenticated by design: the shell holds
        # zero data; every /api call carries the bearer token (localStorage).
        from pathlib import Path

        from fastapi.staticfiles import StaticFiles

        static_dir = Path(__file__).parent / "static"
        app.mount("/ui", StaticFiles(directory=static_dir, html=True), name="ui")

    return app


def main() -> None:
    """Run the gateway directly (uvicorn)."""
    import uvicorn

    settings = Settings.from_env()
    uvicorn.run(
        create_app(settings),
        host=settings.gateway_host,
        port=settings.gateway_port,
    )


if __name__ == "__main__":
    main()

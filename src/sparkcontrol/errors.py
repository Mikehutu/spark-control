"""Error envelope and API error type for the Spark Control gateway.

All error responses share one JSON shape so the PWA (and any client) can
render them uniformly::

    {"error": {"code": "<machine-code>", "detail": "<human message>"}}
"""

from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """An API error carrying an HTTP status code and a stable machine code."""

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


def error_payload(code: str, detail: str) -> dict[str, Any]:
    """Return the canonical error-envelope JSON payload."""
    return {"error": {"code": code, "detail": detail}}

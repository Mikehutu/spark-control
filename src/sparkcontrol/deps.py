"""FastAPI dependencies shared by the app factory and the control router.

Lives in its own module so ``app`` and ``routes`` can both import it without
a circular import (auth gate is a cross-cutting HTTP concern, FR-5).
"""

from __future__ import annotations

from fastapi import Header, Request

from .errors import ApiError
from .result import is_ok


def get_current_auth(request: Request, authorization: str | None = Header(None)) -> None:
    """FastAPI dependency enforcing bearer-token auth on any route that depends on it."""
    result = request.app.state.auth.authorize(authorization)
    if not is_ok(result):
        raise ApiError(401, "unauthorized", "Missing or invalid bearer token")

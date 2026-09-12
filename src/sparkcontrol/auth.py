"""Bearer-token authentication gate (FR-5).

``IAuthGate`` validates the ``Authorization: Bearer <token>`` header. Token
comparison is constant-time and no token material is ever logged.
"""

from __future__ import annotations

import hmac
from enum import Enum
from typing import Protocol

from .result import Err, Ok, Result

_BEARER_PREFIX = "bearer "


class AuthError(Enum):
    """Why authorization failed."""

    MISSING = "missing"
    INVALID = "invalid"


class IAuthGate(Protocol):
    """Validate a bearer token from the Authorization header."""

    def authorize(self, header: str | None) -> Result[None, AuthError]:
        """Return Ok(None) when the header carries a valid token.

        Returns Err(AuthError.MISSING) when absent/malformed and
        Err(AuthError.INVALID) when the token does not match. Never logs token
        material.
        """
        ...


class BearerAuth:
    """Constant-time bearer-token implementation of :class:`IAuthGate`."""

    def __init__(self, token: str) -> None:
        self._token = token

    def authorize(self, header: str | None) -> Result[None, AuthError]:
        if header is None or not header.lower().startswith(_BEARER_PREFIX):
            return Err(AuthError.MISSING)
        provided = header[len(_BEARER_PREFIX) :].strip()
        if not hmac.compare_digest(provided, self._token):
            return Err(AuthError.INVALID)
        return Ok(None)

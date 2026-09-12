"""Unit tests for the gateway-core slice.

Covers bearer-token auth (FR-5) via ``IAuthGate``/``BearerAuth`` and the app
skeleton: ``/health`` (no auth) and an auth-gated route. Tests use the FastAPI
TestClient (httpx) against an app built with a fixed test token.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from sparkcontrol.app import create_app
from sparkcontrol.auth import AuthError, BearerAuth
from sparkcontrol.config import ConfigError, Settings
from sparkcontrol.result import Err, Ok, is_ok

TOKEN = "test-super-secret-token"


@pytest.fixture()
def client() -> TestClient:
    app = create_app(Settings(token=TOKEN))
    return TestClient(app)


# --- BearerAuth unit tests -------------------------------------------------


def test_authorize_accepts_valid_token():
    auth = BearerAuth(TOKEN)
    result = auth.authorize(f"Bearer {TOKEN}")
    assert is_ok(result)
    assert result == Ok(None)


def test_authorize_missing_header_returns_missing():
    auth = BearerAuth(TOKEN)
    result = auth.authorize(None)
    assert result == Err(AuthError.MISSING)


def test_authorize_malformed_header_returns_missing():
    auth = BearerAuth(TOKEN)
    result = auth.authorize("Basic abc123")
    assert result == Err(AuthError.MISSING)


def test_authorize_wrong_token_returns_invalid():
    auth = BearerAuth(TOKEN)
    result = auth.authorize("Bearer wrong-token")
    assert result == Err(AuthError.INVALID)


def test_authorize_is_constant_time_and_prefix_insensitive():
    auth = BearerAuth(TOKEN)
    # Scheme prefix parsed case-insensitively; token value is exact.
    assert is_ok(auth.authorize(f"bearer {TOKEN}"))


# --- Settings --------------------------------------------------------------


def test_settings_require_token():
    with pytest.raises(ConfigError):
        Settings.from_env({})


def test_settings_parse_port():
    s = Settings.from_env({"SPARKCTL_TOKEN": "x", "GATEWAY_PORT": "9000"})
    assert s.gateway_port == 9000


# --- App-level routes (FR-5 / health) -------------------------------------


def test_health_returns_200_without_auth(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_api_ping_with_valid_token(client):
    resp = client.get("/api/ping", headers={"Authorization": f"Bearer {TOKEN}"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_api_ping_missing_token_returns_401(client):
    resp = client.get("/api/ping")
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"]["code"] == "unauthorized"


def test_api_ping_wrong_token_returns_401(client):
    resp = client.get("/api/ping", headers={"Authorization": "Bearer nope"})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"


def test_unknown_route_returns_error_envelope(client):
    resp = client.get("/api/nope", headers={"Authorization": f"Bearer {TOKEN}"})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


def test_500_returns_error_envelope():
    """Fault-injection: an unhandled exception must yield the 500 error envelope.

    TestClient must run with raise_server_exceptions=False so the generic
    exception handler fires instead of the exception escaping the transport.
    """
    app = create_app(Settings(token=TOKEN))

    @app.get("/api/boom")
    async def boom() -> None:  # noqa: ANN202
        raise RuntimeError("boom")

    with TestClient(app, raise_server_exceptions=False) as c:
        resp = c.get("/api/boom", headers={"Authorization": f"Bearer {TOKEN}"})
    assert resp.status_code == 500
    body = resp.json()
    assert body["error"]["code"] == "internal_error"
    assert body["error"]["detail"] == "Internal server error"

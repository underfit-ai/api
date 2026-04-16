from __future__ import annotations

import base64
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from underfit_api.auth import get_app_secret, hash_token
from underfit_api.config import config


def test_health_returns_status_and_version(client: TestClient) -> None:
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "v1"}


def test_unknown_route_returns_json_404(client: TestClient) -> None:
    response = client.get("/api/v1/missing")

    assert response.status_code == 404
    assert "application/json" in response.headers["content-type"]
    assert response.json() == {"error": "Route not found"}


def test_backfill_blocks_api_write_methods_but_not_get(client: TestClient) -> None:
    config.storage.backfill.enabled = True

    health = client.get("/api/v1/health")
    register = client.post("/api/v1/auth/register", json={
        "email": "sam@example.com",
        "handle": "sam",
        "password": "password123",
    })

    assert health.status_code == 200
    assert register.status_code == 409
    assert register.json() == {"error": "API write endpoints are disabled while backfill is enabled"}


@pytest.mark.parametrize(("secret", "message"), [
    (None, "UNDERFIT_APP_SECRET is required"),
    (base64.urlsafe_b64encode(b"short-secret").decode(), "at least 32 bytes"),
])
def test_get_app_secret_validation(secret: str | None, message: str, monkeypatch: pytest.MonkeyPatch) -> None:
    if secret is None:
        monkeypatch.delenv("UNDERFIT_APP_SECRET", raising=False)
    else:
        monkeypatch.setenv("UNDERFIT_APP_SECRET", secret)
    get_app_secret.cache_clear()
    with pytest.raises(RuntimeError, match=message):
        get_app_secret()


def test_hash_token_changes_when_secret_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    secret_a = base64.urlsafe_b64encode(b"a" * 32).decode()
    secret_b = base64.urlsafe_b64encode(b"b" * 32).decode()

    monkeypatch.setenv("UNDERFIT_APP_SECRET", secret_a)
    get_app_secret.cache_clear()
    token = str(uuid4())
    hash_a = hash_token(token)

    monkeypatch.setenv("UNDERFIT_APP_SECRET", secret_b)
    get_app_secret.cache_clear()
    hash_b = hash_token(token)

    assert hash_a != hash_b
    assert hash_b == hash_token(token)

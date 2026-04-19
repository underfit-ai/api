from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import CreateUser, RegisterUser, SessionForUser
from underfit_api.config import config
from underfit_api.main import app


def test_register_login_logout_flow(client: TestClient, register_user: RegisterUser) -> None:
    register = register_user()
    assert register.status_code == 200
    assert register.cookies.get("session_token") is not None

    current = client.get("/api/v1/me")
    assert current.status_code == 200 and current.json()["id"] == register.json()["user"]["id"]
    assert client.post("/api/v1/auth/logout").status_code == 200
    assert client.get("/api/v1/me").status_code == 401


def test_register_rejects_duplicate_email_and_handle(register_user: RegisterUser) -> None:
    assert register_user(email="dup@example.com", handle="dup").status_code == 200
    assert register_user(email="dup@example.com", handle="dup-2").status_code == 409
    assert register_user(email="dup-2@example.com", handle="dup").status_code == 409


def test_login_rejects_invalid_credentials(client: TestClient, register_user: RegisterUser) -> None:
    register_user(email="jules@example.com", handle="jules")
    url = "/api/v1/auth/login"
    assert client.post(url, json={"email": "jules@example.com", "password": "bad-password"}).status_code == 401
    assert client.post(url, json={"email": "missing@example.com", "password": "password123"}).status_code == 401


@pytest.mark.parametrize(("base_url", "secure_override", "frontend_url", "expect_secure"), [
    ("http://localhost", True, None, True),
    ("https://example.com", False, None, False),
    ("http://localhost", None, None, False),
    ("https://example.com", None, None, True),
    ("http://testserver", None, "https://frontend.example.com", True),
])
def test_session_cookie_secure_flag(
    base_url: str, secure_override: bool | None, frontend_url: str | None, expect_secure: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "secure_cookies", secure_override)
    monkeypatch.setattr(config, "frontend_url", frontend_url)
    with TestClient(app, base_url=base_url) as client:
        email = f"user@{base_url.replace('://', '_')}.com"
        payload = {"email": email, "handle": email.split("@", maxsplit=1)[0], "password": "password123"}
        response = client.post("/api/v1/auth/register", json=payload)
    assert response.status_code == 200
    assert ("Secure" in response.headers["set-cookie"]) is expect_secure


def test_auth_modes(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert client.get("/api/v1/me").status_code == 401

    user = create_user(email="cookie@example.com", handle="cookie", name="Cookie")
    cookie_headers = session_for_user(user)
    token = client.post("/api/v1/me/api-keys", headers=cookie_headers, json={"label": "test"}).json()["token"]
    current = client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
    assert current.status_code == 200 and current.json()["handle"] == "cookie"

    monkeypatch.setattr(config, "auth_enabled", False)
    with TestClient(app) as local_client:
        current = local_client.get("/api/v1/me")
        local_client.cookies.set("session_token", "stale")
        bogus = local_client.get("/api/v1/me", headers={"Authorization": "Bearer nope"})
    assert current.status_code == 200
    assert (current.json()["handle"], current.json()["email"]) == ("local", "local@underfit.local")
    assert bogus.status_code == 200 and bogus.json()["handle"] == "local"

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import RegisterUser


def test_register_login_logout_flow(client: TestClient, register_user: RegisterUser) -> None:
    register = register_user()
    assert register.status_code == 200
    user = register.json()["user"]
    token = register.cookies.get("session_token")
    assert token is not None
    cookie = {"Cookie": f"session_token={token}"}

    current = client.get("/api/v1/me", headers=cookie)
    assert current.status_code == 200
    assert current.json()["id"] == user["id"]

    logout = client.post("/api/v1/auth/logout", headers=cookie)
    assert logout.status_code == 200

    expired = client.get("/api/v1/me", headers=cookie)
    assert expired.status_code == 401


def test_register_rejects_duplicate_email_and_handle(register_user: RegisterUser) -> None:
    assert register_user(email="dup@example.com", handle="dup").status_code == 200

    duplicate_email = register_user(email="dup@example.com", handle="dup-2")
    duplicate_handle = register_user(email="dup-2@example.com", handle="dup")

    assert duplicate_email.status_code == 409
    assert duplicate_handle.status_code == 409


def test_login_rejects_invalid_credentials(client: TestClient, register_user: RegisterUser) -> None:
    register_user(email="jules@example.com", handle="jules")

    wrong_password = client.post("/api/v1/auth/login", json={"email": "jules@example.com", "password": "bad-password"})
    unknown_email = client.post("/api/v1/auth/login", json={"email": "missing@example.com", "password": "password123"})

    assert wrong_password.status_code == 401
    assert unknown_email.status_code == 401


def test_register_rejects_invalid_input(client: TestClient) -> None:
    bad_payloads = [
        {"email": "no-at", "handle": "valid-user", "password": "password123"},
        {"email": "ok@example.com", "handle": "bad_handle", "password": "password123"},
        {"email": "ok2@example.com", "handle": "valid-user", "password": "allletters"},
    ]

    for payload in bad_payloads:
        response = client.post("/api/v1/auth/register", json=payload)
        assert response.status_code == 422

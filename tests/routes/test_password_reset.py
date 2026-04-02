from __future__ import annotations

import re
from datetime import timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.conftest import RegisterUser
from underfit_api.config import EmailConfig, config


def _request_reset(client: TestClient, email: str = "sam@example.com") -> str:
    with patch("underfit_api.routes.auth.send_email") as mock_send:
        client.post("/api/v1/auth/forgot-password", json={"email": email})
    body = mock_send.call_args[1]["body"]
    match = re.search(r"token=([A-Za-z0-9_=-]+\.[a-f0-9]+)", body)
    assert match
    return match.group(1)


def test_forgot_password_sends_email(client: TestClient, register_user: RegisterUser) -> None:
    config.email = EmailConfig()
    config.frontend_url = "http://localhost:3000"
    register_user(email="sam@example.com", handle="sam")

    with patch("underfit_api.routes.auth.send_email") as mock_send:
        response = client.post("/api/v1/auth/forgot-password", json={"email": "sam@example.com"})

    assert response.status_code == 200
    mock_send.assert_called_once()
    assert mock_send.call_args[1]["to"] == "sam@example.com"
    assert "reset-password?token=" in mock_send.call_args[1]["body"]
    assert "http://localhost:3000/reset-password" in mock_send.call_args[1]["body"]


def test_forgot_password_unknown_email_returns_ok(client: TestClient) -> None:
    config.email = EmailConfig()

    with patch("underfit_api.routes.auth.send_email") as mock_send:
        response = client.post("/api/v1/auth/forgot-password", json={"email": "nobody@example.com"})

    assert response.status_code == 200
    mock_send.assert_not_called()


def test_forgot_password_fails_without_email_config(client: TestClient) -> None:
    config.email = None
    response = client.post("/api/v1/auth/forgot-password", json={"email": "sam@example.com"})
    assert response.status_code == 400


def test_reset_password_full_flow(client: TestClient, register_user: RegisterUser) -> None:
    config.email = EmailConfig()
    register_user(email="sam@example.com", handle="sam")

    token = _request_reset(client)

    response = client.post("/api/v1/auth/reset-password", json={"token": token, "password": "newpassword1"})
    assert response.status_code == 200

    login = client.post("/api/v1/auth/login", json={"email": "sam@example.com", "password": "newpassword1"})
    assert login.status_code == 200

    old_login = client.post("/api/v1/auth/login", json={"email": "sam@example.com", "password": "password123"})
    assert old_login.status_code == 401


def test_reset_password_invalid_token(client: TestClient) -> None:
    response = client.post("/api/v1/auth/reset-password", json={"token": "bogus-token", "password": "newpassword1"})
    assert response.status_code == 400


def test_reset_password_expired_token(client: TestClient, register_user: RegisterUser) -> None:
    config.email = EmailConfig()
    register_user(email="sam@example.com", handle="sam")

    with patch("underfit_api.routes.auth.RESET_TOKEN_TTL", timedelta(seconds=-1)):
        token = _request_reset(client)

    response = client.post("/api/v1/auth/reset-password", json={"token": token, "password": "newpassword1"})
    assert response.status_code == 400


def test_reset_password_token_is_single_use(client: TestClient, register_user: RegisterUser) -> None:
    config.email = EmailConfig()
    register_user(email="sam@example.com", handle="sam")

    token = _request_reset(client)

    first = client.post("/api/v1/auth/reset-password", json={"token": token, "password": "newpassword1"})
    assert first.status_code == 200

    second = client.post("/api/v1/auth/reset-password", json={"token": token, "password": "anotherpass2"})
    assert second.status_code == 400



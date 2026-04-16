from __future__ import annotations

import re
from datetime import timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.conftest import RegisterUser
from underfit_api.config import EmailConfig, config

LOGIN = "/api/v1/auth/login"
RESET = "/api/v1/auth/reset-password"


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


def test_forgot_password_validation(client: TestClient) -> None:
    config.email = EmailConfig()
    config.frontend_url = None
    assert client.post("/api/v1/auth/forgot-password", json={"email": "sam@example.com"}).status_code == 400
    config.frontend_url = "http://localhost:3000"
    with patch("underfit_api.routes.auth.send_email") as mock_send:
        assert client.post("/api/v1/auth/forgot-password", json={"email": "nobody@example.com"}).status_code == 200
    mock_send.assert_not_called()
    config.email = None
    assert client.post("/api/v1/auth/forgot-password", json={"email": "sam@example.com"}).status_code == 400


def test_reset_password_full_flow(client: TestClient, register_user: RegisterUser) -> None:
    config.email = EmailConfig()
    config.frontend_url = "http://localhost:3000"
    register_user(email="sam@example.com", handle="sam")

    token = _request_reset(client)
    assert client.post(RESET, json={"token": token, "password": "newpassword1"}).status_code == 200
    assert client.post(LOGIN, json={"email": "sam@example.com", "password": "newpassword1"}).status_code == 200
    assert client.post(LOGIN, json={"email": "sam@example.com", "password": "password123"}).status_code == 401


def test_reset_password_token_validation(client: TestClient, register_user: RegisterUser) -> None:
    config.email = EmailConfig()
    config.frontend_url = "http://localhost:3000"
    register_user(email="sam@example.com", handle="sam")
    with patch("underfit_api.routes.auth.RESET_TOKEN_TTL", timedelta(seconds=-1)):
        token = _request_reset(client)
    assert client.post(RESET, json={"token": "bogus-token", "password": "newpassword1"}).status_code == 400
    assert client.post(RESET, json={"token": token, "password": "newpassword1"}).status_code == 400
    token = _request_reset(client)
    assert client.post(RESET, json={"token": token, "password": "newpassword1"}).status_code == 200
    assert client.post(RESET, json={"token": token, "password": "anotherpass2"}).status_code == 400

from __future__ import annotations

import re
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from tests.conftest import CreateProject, CreateUser, Headers, SessionForUser
from underfit_api.config import EmailConfig, config

OWNER_PROJECT = "/api/v1/accounts/owner/projects/myproject"
RECIPIENT_PROJECT = "/api/v1/accounts/recipient/projects/myproject"
TRANSFER = "/api/v1/transfer"
TRANSFER_REQUEST = f"{OWNER_PROJECT}/transfer"
TransferSetup = tuple[Headers, Headers]


@pytest.fixture
def transfer_setup(
    create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> TransferSetup:
    config.email = EmailConfig()
    config.frontend_url = "http://localhost:3000"
    owner = create_user(email="owner@example.com", handle="owner", name="Owner")
    recipient = create_user(email="recipient@example.com", handle="recipient", name="Recipient")
    owner_headers = session_for_user(owner)
    recipient_headers = session_for_user(recipient)
    create_project(handle="owner", name="myproject")
    return owner_headers, recipient_headers


def _initiate(client: TestClient, owner_headers: Headers, email: str = "recipient@example.com") -> str:
    with patch("underfit_api.routes.projects.send_email") as mock_send:
        assert client.post(TRANSFER_REQUEST, headers=owner_headers, json={"email": email}).status_code == 200
    mock_send.assert_called_once()
    assert mock_send.call_args[1]["to"] == email
    assert "transfer?token=" in mock_send.call_args[1]["body"]
    body = mock_send.call_args[1]["body"]
    match = re.search(r"token=([A-Za-z0-9_=-]+\.[a-f0-9]+)", body)
    assert match
    return match.group(1)


def test_full_transfer_flow(client: TestClient, transfer_setup: TransferSetup) -> None:
    owner_headers, recipient_headers = transfer_setup
    token = _initiate(client, owner_headers)

    response = client.post(TRANSFER, headers=recipient_headers, json={"token": token})
    assert response.status_code == 200
    assert response.json()["owner"] == "recipient"
    assert response.json()["name"] == "myproject"
    project = client.get(RECIPIENT_PROJECT, headers=recipient_headers)
    assert project.status_code == 200
    assert project.json()["owner"] == "recipient"
    assert client.get(OWNER_PROJECT, headers=recipient_headers, follow_redirects=False).status_code == 307


def test_cancel_transfer(client: TestClient, transfer_setup: TransferSetup) -> None:
    owner_headers, recipient_headers = transfer_setup
    token = _initiate(client, owner_headers)
    assert client.delete(TRANSFER_REQUEST, headers=owner_headers).status_code == 200
    assert client.delete(TRANSFER_REQUEST, headers=owner_headers).status_code == 400
    assert client.post(TRANSFER, headers=recipient_headers, json={"token": token}).status_code == 400


def test_transfer_requires_admin_permissions(
    client: TestClient, transfer_setup: TransferSetup, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    outsider = create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    outsider_headers = session_for_user(outsider)
    with patch("underfit_api.routes.projects.send_email"):
        response = client.post(TRANSFER_REQUEST, headers=outsider_headers, json={"email": "recipient@example.com"})
        assert response.status_code == 403


@pytest.mark.parametrize(("email", "status"), [("owner@example.com", 400), ("nobody@example.com", 404)])
def test_transfer_rejects_invalid_recipient(
    email: str, status: int, client: TestClient, transfer_setup: TransferSetup,
) -> None:
    owner_headers, _ = transfer_setup
    with patch("underfit_api.routes.projects.send_email"):
        assert client.post(TRANSFER_REQUEST, headers=owner_headers, json={"email": email}).status_code == status


def test_accept_transfer_validation(
    client: TestClient, transfer_setup: TransferSetup, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    owner_headers, _ = transfer_setup
    token = _initiate(client, owner_headers)
    intruder = create_user(email="intruder@example.com", handle="intruder", name="Intruder")
    intruder_headers = session_for_user(intruder)
    assert client.post(TRANSFER, headers=intruder_headers, json={"token": token}).status_code == 403
    assert client.post(TRANSFER, headers=intruder_headers, json={"token": "bogus"}).status_code == 400


def test_new_transfer_replaces_old(
    client: TestClient, transfer_setup: TransferSetup, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    owner_headers, recipient_headers = transfer_setup
    second_recipient = create_user(email="r2@example.com", handle="r2", name="R2")
    second_headers = session_for_user(second_recipient)
    first_token = _initiate(client, owner_headers)
    second_token = _initiate(client, owner_headers, "r2@example.com")
    assert client.post(TRANSFER, headers=recipient_headers, json={"token": first_token}).status_code == 400
    new = client.post(TRANSFER, headers=second_headers, json={"token": second_token})
    assert new.status_code == 200
    assert new.json()["owner"] == "r2"


def test_transfer_with_name_conflict(
    client: TestClient, transfer_setup: TransferSetup, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers = transfer_setup
    create_project(handle="recipient", name="myproject")
    token = _initiate(client, owner_headers)
    assert client.post(TRANSFER, headers=recipient_headers, json={"token": token}).status_code == 409
    response = client.post(TRANSFER, headers=recipient_headers, json={"token": token, "new_name": "renamed"})
    assert response.status_code == 200
    assert response.json()["owner"] == "recipient"
    assert response.json()["name"] == "renamed"


def test_transfer_redirects_old_owner_and_name(client: TestClient, transfer_setup: TransferSetup) -> None:
    owner_headers, recipient_headers = transfer_setup
    token = _initiate(client, owner_headers)
    response = client.post(TRANSFER, headers=recipient_headers, json={"token": token, "new_name": "renamed"})
    assert response.status_code == 200
    redirect = client.get(OWNER_PROJECT, headers=recipient_headers, follow_redirects=False)
    assert redirect.status_code == 307
    assert redirect.headers["location"] == "/api/v1/accounts/recipient/projects/renamed"


def test_transfer_removes_collaborator(client: TestClient, transfer_setup: TransferSetup) -> None:
    owner_headers, recipient_headers = transfer_setup
    client.put(f"{OWNER_PROJECT}/collaborators/recipient", headers=owner_headers)
    token = _initiate(client, owner_headers)
    client.post(TRANSFER, headers=recipient_headers, json={"token": token})
    collabs = client.get(f"{RECIPIENT_PROJECT}/collaborators", headers=recipient_headers)
    assert collabs.status_code == 200
    assert "recipient" not in [c["handle"] for c in collabs.json()]


def test_requires_email_config(client: TestClient, transfer_setup: TransferSetup) -> None:
    owner_headers, _ = transfer_setup
    config.email = None
    response = client.post(TRANSFER_REQUEST, headers=owner_headers, json={"email": "recipient@example.com"})
    assert response.status_code == 400

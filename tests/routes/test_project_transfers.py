from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateOrg, CreateProject, CreateUser, Headers, SessionForUser

OWNER_PROJECT = "/api/v1/accounts/owner/projects/myproject"
RECIPIENT_PROJECT = "/api/v1/accounts/recipient/projects/myproject"
TRANSFER = f"{OWNER_PROJECT}/transfer"
TransferSetup = tuple[Headers, Headers]


@pytest.fixture
def transfer_setup(
    create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
    add_collaborator: AddCollaborator,
) -> TransferSetup:
    owner = create_user(email="owner@example.com", handle="owner", name="Owner")
    recipient = create_user(email="recipient@example.com", handle="recipient", name="Recipient")
    create_project(handle="owner", name="myproject")
    add_collaborator(handle="owner", project_name="myproject", user_handle="recipient")
    return session_for_user(owner), session_for_user(recipient)


def test_transfer_to_collaborator(client: TestClient, transfer_setup: TransferSetup) -> None:
    owner_headers, recipient_headers = transfer_setup
    response = client.post(TRANSFER, headers=owner_headers, json={"handle": "recipient"})
    assert response.status_code == 200
    assert response.json()["owner"] == "recipient"
    assert response.json()["name"] == "myproject"

    project = client.get(RECIPIENT_PROJECT, headers=recipient_headers)
    assert project.status_code == 200
    collabs = client.get(f"{RECIPIENT_PROJECT}/collaborators", headers=recipient_headers)
    assert "recipient" not in [c["handle"] for c in collabs.json()]


def test_transfer_requires_admin(
    client: TestClient, transfer_setup: TransferSetup, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    outsider_headers = session_for_user(create_user(email="o@example.com", handle="outsider", name="Outsider"))
    assert client.post(TRANSFER, headers=outsider_headers, json={"handle": "recipient"}).status_code == 403


def test_transfer_rejects_non_collaborator(
    client: TestClient, transfer_setup: TransferSetup, create_user: CreateUser,
) -> None:
    owner_headers, _ = transfer_setup
    create_user(email="stranger@example.com", handle="stranger", name="Stranger")
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "stranger"}).status_code == 400
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "nobody"}).status_code == 404
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "owner"}).status_code == 400


def test_transfer_rejects_organization(
    client: TestClient, transfer_setup: TransferSetup, create_org: CreateOrg,
) -> None:
    owner_headers, _ = transfer_setup
    create_org(headers=owner_headers, handle="acme", name="Acme")
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "acme"}).status_code == 400


def test_transfer_with_name_conflict(
    client: TestClient, transfer_setup: TransferSetup, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers = transfer_setup
    create_project(handle="recipient", name="myproject")
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "recipient"}).status_code == 409
    response = client.post(TRANSFER, headers=owner_headers, json={"handle": "recipient", "new_name": "renamed"})
    assert response.status_code == 200
    assert response.json()["owner"] == "recipient"
    assert response.json()["name"] == "renamed"

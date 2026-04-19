from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateOrg, CreateProject, CreateUser, Headers, SessionForUser

OWNER_PROJECT = "/api/v1/accounts/owner/projects/myproject"
RECIPIENT_PROJECT = "/api/v1/accounts/recipient/projects/myproject"
TRANSFER = f"{OWNER_PROJECT}/transfer"


def test_transfer_to_collaborator(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_user: CreateUser, session_for_user: SessionForUser,
    create_project: CreateProject, add_collaborator: AddCollaborator,
) -> None:
    recipient = create_user(email="recipient@example.com", handle="recipient", name="Recipient")
    recipient_headers = session_for_user(recipient)
    create_project(handle="owner", name="myproject")
    add_collaborator(handle="owner", project_name="myproject", user_handle="recipient")

    assert client.post(TRANSFER, headers=outsider_headers, json={"handle": "recipient"}).status_code == 403
    response = client.post(TRANSFER, headers=owner_headers, json={"handle": "recipient"})
    assert response.status_code == 200
    assert (response.json()["owner"], response.json()["name"]) == ("recipient", "myproject")
    assert client.get(RECIPIENT_PROJECT, headers=recipient_headers).status_code == 200
    collabs = client.get(f"{RECIPIENT_PROJECT}/collaborators", headers=recipient_headers).json()
    assert "recipient" not in [c["handle"] for c in collabs]


def test_transfer_rejections_and_rename(
    client: TestClient, owner_headers: Headers,
    create_user: CreateUser, create_project: CreateProject,
    add_collaborator: AddCollaborator, create_org: CreateOrg,
) -> None:
    create_user(email="recipient@example.com", handle="recipient", name="Recipient")
    create_user(email="stranger@example.com", handle="stranger", name="Stranger")
    create_project(handle="owner", name="myproject")
    add_collaborator(handle="owner", project_name="myproject", user_handle="recipient")
    create_org(headers=owner_headers, handle="acme", name="Acme")

    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "stranger"}).status_code == 400
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "nobody"}).status_code == 404
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "owner"}).status_code == 400
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "acme"}).status_code == 400

    create_project(handle="recipient", name="myproject")
    assert client.post(TRANSFER, headers=owner_headers, json={"handle": "recipient"}).status_code == 409
    renamed = client.post(TRANSFER, headers=owner_headers, json={"handle": "recipient", "new_name": "renamed"})
    assert renamed.status_code == 200
    assert (renamed.json()["owner"], renamed.json()["name"]) == ("recipient", "renamed")

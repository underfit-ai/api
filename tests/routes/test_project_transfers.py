from __future__ import annotations

import re
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.conftest import CreateProject, CreateUser, Headers, SessionForUser
from underfit_api.config import EmailConfig, config


def _setup(
    create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> tuple[Headers, Headers, str]:
    config.email = EmailConfig()
    config.frontend_url = "http://localhost:3000"
    owner = create_user(email="owner@example.com", handle="owner", name="Owner")
    recipient = create_user(email="recipient@example.com", handle="recipient", name="Recipient")
    owner_headers = session_for_user(owner)
    recipient_headers = session_for_user(recipient)
    create_project(owner_headers, handle="owner", name="myproject")
    return owner_headers, recipient_headers, "recipient@example.com"


def _initiate(client: TestClient, owner_headers: Headers, email: str = "recipient@example.com") -> str:
    with patch("underfit_api.routes.projects.send_email") as mock_send:
        response = client.post(
            "/api/v1/accounts/owner/projects/myproject/transfer",
            headers=owner_headers,
            json={"email": email},
        )
    assert response.status_code == 200
    body = mock_send.call_args[1]["body"]
    match = re.search(r"token=([A-Za-z0-9_=-]+\.[a-f0-9]+)", body)
    assert match
    return match.group(1)


def test_initiate_sends_email(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, _ = _setup(create_user, session_for_user, create_project)

    with patch("underfit_api.routes.projects.send_email") as mock_send:
        response = client.post(
            "/api/v1/accounts/owner/projects/myproject/transfer",
            headers=owner_headers,
            json={"email": "recipient@example.com"},
        )

    assert response.status_code == 200
    mock_send.assert_called_once()
    assert mock_send.call_args[1]["to"] == "recipient@example.com"
    assert "transfer?token=" in mock_send.call_args[1]["body"]


def test_full_transfer_flow(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)
    token = _initiate(client, owner_headers, email)

    response = client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token})
    assert response.status_code == 200
    assert response.json()["owner"] == "recipient"
    assert response.json()["name"] == "myproject"

    project = client.get("/api/v1/accounts/recipient/projects/myproject", headers=recipient_headers)
    assert project.status_code == 200
    assert project.json()["owner"] == "recipient"


def test_old_url_redirects_after_transfer(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)
    token = _initiate(client, owner_headers, email)
    client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token})

    response = client.get(
        "/api/v1/accounts/owner/projects/myproject", headers=recipient_headers, follow_redirects=False,
    )
    assert response.status_code == 307


def test_cancel_transfer(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)
    token = _initiate(client, owner_headers, email)

    cancel = client.delete("/api/v1/accounts/owner/projects/myproject/transfer", headers=owner_headers)
    assert cancel.status_code == 200

    accept = client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token})
    assert accept.status_code == 400


def test_cancel_when_no_pending_transfer(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, _ = _setup(create_user, session_for_user, create_project)
    response = client.delete("/api/v1/accounts/owner/projects/myproject/transfer", headers=owner_headers)
    assert response.status_code == 400


def test_transfer_to_self_rejected(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, _ = _setup(create_user, session_for_user, create_project)

    with patch("underfit_api.routes.projects.send_email"):
        response = client.post(
            "/api/v1/accounts/owner/projects/myproject/transfer",
            headers=owner_headers,
            json={"email": "owner@example.com"},
        )
    assert response.status_code == 400


def test_transfer_to_unknown_email(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, _ = _setup(create_user, session_for_user, create_project)

    with patch("underfit_api.routes.projects.send_email"):
        response = client.post(
            "/api/v1/accounts/owner/projects/myproject/transfer",
            headers=owner_headers,
            json={"email": "nobody@example.com"},
        )
    assert response.status_code == 404


def test_wrong_user_cannot_accept(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, email = _setup(create_user, session_for_user, create_project)
    token = _initiate(client, owner_headers, email)

    intruder = create_user(email="intruder@example.com", handle="intruder", name="Intruder")
    intruder_headers = session_for_user(intruder)
    response = client.post("/api/v1/transfer", headers=intruder_headers, json={"token": token})
    assert response.status_code == 403


def test_invalid_token_rejected(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="u@example.com", handle="u", name="U")
    headers = session_for_user(user)
    response = client.post("/api/v1/transfer", headers=headers, json={"token": "bogus"})
    assert response.status_code == 400


def test_new_transfer_replaces_old(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)

    second_recipient = create_user(email="r2@example.com", handle="r2", name="R2")
    second_headers = session_for_user(second_recipient)

    first_token = _initiate(client, owner_headers, email)
    second_token = _initiate(client, owner_headers, "r2@example.com")

    old = client.post("/api/v1/transfer", headers=recipient_headers, json={"token": first_token})
    assert old.status_code == 400

    new = client.post("/api/v1/transfer", headers=second_headers, json={"token": second_token})
    assert new.status_code == 200
    assert new.json()["owner"] == "r2"


def test_transfer_conflict_when_name_exists(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)
    create_project(recipient_headers, handle="recipient", name="myproject")

    token = _initiate(client, owner_headers, email)
    response = client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token})
    assert response.status_code == 409


def test_transfer_with_rename(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)
    token = _initiate(client, owner_headers, email)

    response = client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token, "new_name": "renamed"})
    assert response.status_code == 200
    assert response.json()["owner"] == "recipient"
    assert response.json()["name"] == "renamed"


def test_transfer_rename_avoids_conflict(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)
    create_project(recipient_headers, handle="recipient", name="myproject")

    token = _initiate(client, owner_headers, email)
    response = client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token, "new_name": "renamed"})
    assert response.status_code == 200
    assert response.json()["name"] == "renamed"


def test_transfer_removes_collaborator(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
    create_project: CreateProject, add_collaborator: CreateProject,
) -> None:
    owner_headers, recipient_headers, email = _setup(create_user, session_for_user, create_project)

    client.put(
        "/api/v1/accounts/owner/projects/myproject/collaborators/recipient", headers=owner_headers,
    )

    token = _initiate(client, owner_headers, email)
    client.post("/api/v1/transfer", headers=recipient_headers, json={"token": token})

    collabs = client.get("/api/v1/accounts/recipient/projects/myproject/collaborators", headers=recipient_headers)
    assert collabs.status_code == 200
    handles = [c["handle"] for c in collabs.json()]
    assert "recipient" not in handles


def test_non_admin_cannot_initiate(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, _ = _setup(create_user, session_for_user, create_project)
    outsider = create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    outsider_headers = session_for_user(outsider)

    with patch("underfit_api.routes.projects.send_email"):
        response = client.post(
            "/api/v1/accounts/owner/projects/myproject/transfer",
            headers=outsider_headers,
            json={"email": "recipient@example.com"},
        )
    assert response.status_code == 403


def test_requires_email_config(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser, create_project: CreateProject,
) -> None:
    owner_headers, _, _ = _setup(create_user, session_for_user, create_project)
    config.email = None
    response = client.post(
        "/api/v1/accounts/owner/projects/myproject/transfer",
        headers=owner_headers,
        json={"email": "recipient@example.com"},
    )
    assert response.status_code == 400

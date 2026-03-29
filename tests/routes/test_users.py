from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, OwnerHeaders, SessionForUser
from underfit_api.config import config


def test_email_exists_requires_email_and_checks_presence(client: TestClient, create_user: CreateUser) -> None:
    create_user(email="ada@example.com", handle="ada", name="Ada")

    missing = client.get("/api/v1/emails/exists")
    exists = client.get("/api/v1/emails/exists", params={"email": "ada@example.com"})
    absent = client.get("/api/v1/emails/exists", params={"email": "grace@example.com"})

    assert missing.status_code == 400
    assert exists.json() == {"exists": True}
    assert absent.json() == {"exists": False}


def test_update_me_updates_profile(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    user = create_user(email="sam@example.com", handle="sam", name="Sam")
    headers = session_for_user(user)

    response = client.patch("/api/v1/me", headers=headers, json={"name": "Sam Tester", "bio": "Building models."})

    assert response.status_code == 200
    assert response.json()["name"] == "Sam Tester"
    assert response.json()["bio"] == "Building models."


def test_me_requires_auth(client: TestClient) -> None:
    response = client.get("/api/v1/me")
    assert response.status_code == 401


def test_me_accepts_bearer_api_key(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    user = create_user(email="cookie@example.com", handle="cookie", name="Cookie")
    cookie_headers = session_for_user(user)

    created = client.post("/api/v1/me/api-keys", headers=cookie_headers, json={"label": "test"})
    token = created.json()["token"]

    current = client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
    assert current.status_code == 200
    assert current.json()["handle"] == "cookie"


def test_me_returns_local_user_when_auth_disabled(client: TestClient) -> None:
    config.auth_enabled = False
    response = client.get("/api/v1/me")
    assert response.status_code == 200
    assert response.json()["handle"] == "local"
    assert response.json()["email"] == "local@underfit.local"


def test_user_memberships_lists_orgs_and_rejects_unknown_user(
    client: TestClient, owner_headers: OwnerHeaders,
) -> None:
    created = client.post("/api/v1/organizations", headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201

    memberships = client.get("/api/v1/users/owner/memberships")
    assert memberships.status_code == 200
    assert len(memberships.json()) == 1
    assert memberships.json()[0]["handle"] == "core"
    assert memberships.json()[0]["name"] == "Core"
    assert memberships.json()[0]["role"] == "ADMIN"
    assert memberships.json()[0]["type"] == "ORGANIZATION"

    missing = client.get("/api/v1/users/missing/memberships")
    assert missing.status_code == 404

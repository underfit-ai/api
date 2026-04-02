from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, Headers, SessionForUser
from underfit_api.config import config


def test_email_exists_requires_email_and_checks_presence(client: TestClient, create_user: CreateUser) -> None:
    create_user(email="ada@example.com", handle="ada", name="Ada")

    missing = client.get("/api/v1/emails/exists")
    exists = client.get("/api/v1/emails/exists", params={"email": "ada@example.com"})
    absent = client.get("/api/v1/emails/exists", params={"email": "grace@example.com"})

    assert missing.status_code == 400
    assert exists.json() == {"exists": True}
    assert absent.json() == {"exists": False}


def test_user_search_requires_auth(client: TestClient) -> None:
    response = client.get("/api/v1/users/search", params={"query": "Ada"})
    assert response.status_code == 401


def test_user_search_requires_query(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    user = create_user(email="actor@example.com", handle="actor", name="Actor")
    headers = session_for_user(user)

    response = client.get("/api/v1/users/search", headers=headers)

    assert response.status_code == 400
    assert response.json() == {"error": "Missing query"}


def test_user_search_by_name_and_handle_prefix(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    actor = create_user(email="actor@example.com", handle="actor", name="Actor")
    headers = session_for_user(actor)

    create_user(email="ada@example.com", handle="ada", name="Ada")
    create_user(email="adal@example.com", handle="adal", name="Ada Lovelace")
    create_user(email="adalong@example.com", handle="adalong", name="Someone")

    response = client.get("/api/v1/users/search", headers=headers, params={"query": "Ada"})

    assert response.status_code == 200
    assert [user["handle"] for user in response.json()] == ["ada", "adal", "adalong"]


def test_user_search_by_email_prefix(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    actor = create_user(email="actor@example.com", handle="actor", name="Actor")
    headers = session_for_user(actor)

    create_user(email="ada@example.com", handle="ada", name="Ada")
    create_user(email="ada@other.com", handle="ada2", name="Ada Two")
    create_user(email="adal@example.com", handle="adal", name="Ada L")

    response = client.get("/api/v1/users/search", headers=headers, params={"query": "ada@"})

    assert response.status_code == 200
    assert [user["email"] for user in response.json()] == ["ada@example.com", "ada@other.com"]


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


def test_delete_account(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="doomed@example.com", handle="doomed", name="Doomed")
    headers = session_for_user(user)

    deleted = client.delete("/api/v1/me", headers=headers)
    assert deleted.status_code == 200
    assert deleted.json() == {"ok": True}

    response = client.get("/api/v1/me", headers=headers)
    assert response.status_code == 401


def test_sole_admin_cannot_delete_account(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    user = create_user(email="admin@example.com", handle="admin", name="Admin")
    headers = session_for_user(user)

    created = client.post("/api/v1/organizations", headers=headers, json={"handle": "myorg", "name": "My Org"})
    assert created.status_code == 201

    deleted = client.delete("/api/v1/me", headers=headers)
    assert deleted.status_code == 400

    response = client.get("/api/v1/me", headers=headers)
    assert response.status_code == 200


def test_non_sole_admin_can_delete_account(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    admin1 = create_user(email="admin1@example.com", handle="admin1", name="Admin One")
    admin2 = create_user(email="admin2@example.com", handle="admin2", name="Admin Two")
    admin1_headers = session_for_user(admin1)
    session_for_user(admin2)

    created = client.post("/api/v1/organizations", headers=admin1_headers, json={"handle": "shared", "name": "Shared"})
    assert created.status_code == 201

    client.put("/api/v1/organizations/shared/members/admin2", headers=admin1_headers, json={"role": "ADMIN"})

    deleted = client.delete("/api/v1/me", headers=admin1_headers)
    assert deleted.status_code == 200
    assert deleted.json() == {"ok": True}


def test_user_memberships_lists_orgs_and_rejects_unknown_user(
    client: TestClient, owner_headers: Headers,
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

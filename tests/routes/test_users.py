from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateOrg, CreateUser, Headers, SessionForUser

ORGS = "/api/v1/organizations"


def test_email_exists(client: TestClient, create_user: CreateUser) -> None:
    create_user(email="ada@example.com", handle="ada", name="Ada")
    assert client.get("/api/v1/emails/exists").status_code == 400
    assert client.get("/api/v1/emails/exists", params={"email": "ada@example.com"}).json() == {"exists": True}
    assert client.get("/api/v1/emails/exists", params={"email": "grace@example.com"}).json() == {"exists": False}


def test_user_search(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    assert client.get("/api/v1/users/search", params={"query": "Ada"}).status_code == 401

    actor = create_user(email="actor@example.com", handle="actor", name="Actor")
    headers = session_for_user(actor)
    response = client.get("/api/v1/users/search", headers=headers)
    assert response.status_code == 400 and response.json() == {"error": "Missing query"}

    create_user(email="ada@example.com", handle="ada", name="Ada")
    create_user(email="adal@example.com", handle="adal", name="Ada Lovelace")
    create_user(email="adalong@example.com", handle="adalong", name="Someone")
    response = client.get("/api/v1/users/search", headers=headers, params={"query": "Ada"})
    assert response.status_code == 200 and [user["handle"] for user in response.json()] == ["ada", "adal", "adalong"]

    create_user(email="ada@other.com", handle="ada2", name="Ada Two")
    response = client.get("/api/v1/users/search", headers=headers, params={"query": "ada@"})
    assert response.status_code == 200
    assert [user["email"] for user in response.json()] == ["ada@example.com", "ada@other.com"]


def test_update_profile(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="sam@example.com", handle="sam", name="Sam")
    headers = session_for_user(user)
    response = client.patch("/api/v1/me", headers=headers, json={"name": "Sam Tester", "bio": "Building models."})
    assert response.status_code == 200
    assert (response.json()["name"], response.json()["bio"]) == ("Sam Tester", "Building models.")

    preserved = client.patch("/api/v1/me", headers=headers, json={"name": "Sam Researcher"})
    assert preserved.status_code == 200
    assert (preserved.json()["name"], preserved.json()["bio"]) == ("Sam Researcher", "Building models.")


def test_list_user_memberships(client: TestClient, owner_headers: Headers, create_org: CreateOrg) -> None:
    create_org(owner_headers)
    memberships = client.get("/api/v1/users/owner/memberships")
    assert memberships.status_code == 200
    assert len(memberships.json()) == 1
    assert memberships.json()[0]["handle"] == "core"
    assert memberships.json()[0]["name"] == "Core"
    assert memberships.json()[0]["role"] == "ADMIN"
    assert memberships.json()[0]["type"] == "ORGANIZATION"
    assert client.get("/api/v1/users/missing/memberships").status_code == 404

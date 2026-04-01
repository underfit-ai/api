from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, OutsiderHeaders, OwnerHeaders, SessionForUser


def test_account_exists_and_get_by_handle(client: TestClient, create_user: CreateUser) -> None:
    user = create_user(email="ada@example.com", handle="ada", name="Ada")

    exists = client.get("/api/v1/accounts/ada/exists")
    missing = client.get("/api/v1/accounts/missing/exists")

    assert exists.status_code == 200
    assert exists.json() == {"exists": True}
    assert missing.status_code == 200
    assert missing.json() == {"exists": False}

    fetched = client.get("/api/v1/accounts/ada")
    not_found = client.get("/api/v1/accounts/missing")

    assert fetched.status_code == 200
    assert fetched.json()["id"] == str(user.id)
    assert fetched.json()["handle"] == "ada"
    assert not_found.status_code == 404


def test_rename_account(client: TestClient, owner_headers: OwnerHeaders) -> None:
    renamed = client.post(
        "/api/v1/accounts/owner/rename", headers=owner_headers, json={"handle": "new-owner"},
    )
    assert renamed.status_code == 200
    assert renamed.json()["handle"] == "new-owner"

    fetched = client.get("/api/v1/accounts/new-owner", headers=owner_headers)
    assert fetched.status_code == 200
    assert fetched.json()["handle"] == "new-owner"


def test_rename_account_old_handle_redirects(client: TestClient, owner_headers: OwnerHeaders) -> None:
    client.post("/api/v1/accounts/owner/rename", headers=owner_headers, json={"handle": "new-owner"})

    response = client.get("/api/v1/accounts/owner", headers=owner_headers, follow_redirects=False)
    assert response.status_code == 307
    assert "/accounts/new-owner" in response.headers["location"]


def test_rename_account_conflict_with_existing(
    client: TestClient, owner_headers: OwnerHeaders, outsider_headers: OutsiderHeaders,
) -> None:
    conflict = client.post(
        "/api/v1/accounts/owner/rename", headers=owner_headers, json={"handle": "outsider"},
    )
    assert conflict.status_code == 409


def test_rename_account_conflict_with_old_alias(
    client: TestClient, owner_headers: OwnerHeaders, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    other = create_user(email="other@example.com", handle="other", name="Other")
    other_headers = session_for_user(other)

    client.post("/api/v1/accounts/other/rename", headers=other_headers, json={"handle": "other-new"})

    conflict = client.post(
        "/api/v1/accounts/owner/rename", headers=owner_headers, json={"handle": "other"},
    )
    assert conflict.status_code == 409


def test_rename_account_requires_admin(
    client: TestClient, owner_headers: OwnerHeaders, outsider_headers: OutsiderHeaders,
) -> None:
    forbidden = client.post(
        "/api/v1/accounts/owner/rename", headers=outsider_headers, json={"handle": "hacked"},
    )
    assert forbidden.status_code == 403


def test_cannot_create_account_with_old_alias_handle(
    client: TestClient, owner_headers: OwnerHeaders,
) -> None:
    client.post("/api/v1/accounts/owner/rename", headers=owner_headers, json={"handle": "new-owner"})

    conflict = client.post(
        "/api/v1/auth/register",
        json={"email": "new@example.com", "handle": "owner", "password": "password123"},
    )
    assert conflict.status_code == 409


def test_rename_org_handle_redirects(
    client: TestClient, owner_headers: OwnerHeaders,
) -> None:
    created = client.post(
        "/api/v1/organizations", headers=owner_headers, json={"handle": "my-org", "name": "My Org"},
    )
    assert created.status_code == 201

    client.post("/api/v1/accounts/my-org/rename", headers=owner_headers, json={"handle": "new-org"})

    response = client.get("/api/v1/organizations/my-org/members", follow_redirects=False)
    assert response.status_code == 307
    assert "/organizations/new-org" in response.headers["location"]

    members = client.get("/api/v1/organizations/new-org/members")
    assert members.status_code == 200

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import CreateOrg, CreateUser, Headers, SessionForUser

BASE = "/api/v1/accounts"


def test_account_exists_and_get_by_handle(client: TestClient, create_user: CreateUser) -> None:
    user = create_user(email="ada@example.com", handle="ada", name="Ada")

    exists = client.get(f"{BASE}/ada/exists")
    missing = client.get(f"{BASE}/missing/exists")

    assert exists.status_code == 200 and exists.json() == {"exists": True}
    assert missing.status_code == 200 and missing.json() == {"exists": False}

    fetched = client.get(f"{BASE}/ada")
    not_found = client.get(f"{BASE}/missing")

    fetched_json = fetched.json()
    assert fetched.status_code == 200 and fetched_json["id"] == str(user.id) and fetched_json["handle"] == "ada"
    assert not_found.status_code == 404


def test_rename_account(client: TestClient, owner_headers: Headers, outsider_headers: Headers) -> None:
    assert client.post(f"{BASE}/owner/rename", headers=outsider_headers, json={"handle": "hacked"}).status_code == 403
    renamed = client.post(f"{BASE}/owner/rename", headers=owner_headers, json={"handle": "new-owner"})
    assert renamed.status_code == 200
    assert renamed.json()["handle"] == "new-owner"

    fetched = client.get(f"{BASE}/new-owner", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["handle"] == "new-owner"

    response = client.get(f"{BASE}/owner", headers=owner_headers, follow_redirects=False)
    assert response.status_code == 307
    assert "/accounts/new-owner" in response.headers["location"]
    payload = {"email": "new@example.com", "handle": "owner", "password": "password123"}
    assert client.post("/api/v1/auth/register", json=payload).status_code == 409


@pytest.mark.parametrize("handle", ["outsider", "other"])
def test_rename_account_conflicts(
    handle: str, client: TestClient, owner_headers: Headers, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    other = create_user(email="other@example.com", handle="other", name="Other")
    other_headers = session_for_user(other)
    client.post(f"{BASE}/other/rename", headers=other_headers, json={"handle": "other-new"})
    create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    assert client.post(f"{BASE}/owner/rename", headers=owner_headers, json={"handle": handle}).status_code == 409


def test_rename_org_handle_redirects(client: TestClient, owner_headers: Headers, create_org: CreateOrg) -> None:
    create_org(owner_headers, handle="my-org", name="My Org")
    client.post(f"{BASE}/my-org/rename", headers=owner_headers, json={"handle": "new-org"})

    response = client.get("/api/v1/organizations/my-org/members", follow_redirects=False)
    assert response.status_code == 307
    assert "/organizations/new-org" in response.headers["location"]

    assert client.get("/api/v1/organizations/new-org/members").status_code == 200

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateOrg, CreateUser, Headers

BASE = "/api/v1/accounts"


def test_account_exists_and_get_by_handle(client: TestClient, create_user: CreateUser) -> None:
    user = create_user(email="ada@example.com", handle="ada", name="Ada")

    assert client.get(f"{BASE}/ada/exists").json() == {"exists": True}
    assert client.get(f"{BASE}/missing/exists").json() == {"exists": False}
    assert client.get(f"{BASE}/missing").status_code == 404
    fetched = client.get(f"{BASE}/ada").json()
    assert fetched["id"] == str(user.id) and fetched["handle"] == "ada"


def test_rename_account(client: TestClient, owner_headers: Headers, outsider_headers: Headers) -> None:
    assert client.post(f"{BASE}/owner/rename", headers=outsider_headers, json={"handle": "hacked"}).status_code == 403
    renamed = client.post(f"{BASE}/owner/rename", headers=owner_headers, json={"handle": "new-owner"})
    assert renamed.status_code == 200
    assert renamed.json()["handle"] == "new-owner"

    fetched = client.get(f"{BASE}/new-owner", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["handle"] == "new-owner"
    assert client.get(f"{BASE}/owner", headers=owner_headers).status_code == 404


def test_rename_account_conflicts(client: TestClient, owner_headers: Headers, create_user: CreateUser) -> None:
    create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    assert client.post(f"{BASE}/owner/rename", headers=owner_headers, json={"handle": "outsider"}).status_code == 409


def test_rename_org_handle(client: TestClient, owner_headers: Headers, create_org: CreateOrg) -> None:
    create_org(owner_headers, handle="my-org", name="My Org")
    client.post(f"{BASE}/my-org/rename", headers=owner_headers, json={"handle": "new-org"})

    assert client.get("/api/v1/organizations/my-org/members").status_code == 404
    assert client.get("/api/v1/organizations/new-org/members").status_code == 200

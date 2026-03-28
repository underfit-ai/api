from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser


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

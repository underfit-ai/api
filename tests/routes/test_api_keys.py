from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, SessionForUser


def test_api_key_lifecycle(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="alex@example.com", handle="alex", name="Alex")
    headers = session_for_user(user)

    created = client.post("/api/v1/me/api-keys", headers=headers, json={"label": "CI"})
    assert created.status_code == 200
    key = created.json()
    assert key["userId"] == str(user.id)
    assert key["label"] == "CI"

    listed = client.get("/api/v1/me/api-keys", headers=headers)
    assert listed.status_code == 200
    assert len(listed.json()) == 1
    assert listed.json()[0]["id"] == key["id"]
    assert listed.json()[0]["tokenPrefix"] == key["tokenPrefix"]
    assert "token" not in listed.json()[0]

    deleted = client.delete(f"/api/v1/me/api-keys/{key['id']}", headers=headers)
    assert deleted.status_code == 200
    assert deleted.json() == {"status": "ok"}

    empty = client.get("/api/v1/me/api-keys", headers=headers)
    assert empty.status_code == 200
    assert empty.json() == []

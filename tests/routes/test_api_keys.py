from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, SessionForUser


def test_api_key_lifecycle(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="alex@example.com", handle="alex", name="Alex")
    other_headers = session_for_user(create_user(email="blair@example.com", handle="blair", name="Blair"))
    headers = session_for_user(user)

    created = client.post("/api/v1/me/api-keys", headers=headers, json={"label": "CI"})
    assert created.status_code == 200
    key = created.json()
    assert key["userId"] == str(user.id)
    assert key["label"] == "CI"

    listed = client.get("/api/v1/me/api-keys", headers=headers)
    assert listed.status_code == 200
    assert [row["id"] for row in listed.json()] == [key["id"]]

    deleted = client.delete(f"/api/v1/me/api-keys/{key['id']}", headers=headers)
    assert deleted.status_code == 200
    assert deleted.json() == {"status": "ok"}
    assert client.delete("/api/v1/me/api-keys/not-a-uuid", headers=headers).status_code == 404
    assert client.delete(f"/api/v1/me/api-keys/{key['id']}", headers=other_headers).status_code == 404

    empty = client.get("/api/v1/me/api-keys", headers=headers)
    assert empty.status_code == 200
    assert empty.json() == []

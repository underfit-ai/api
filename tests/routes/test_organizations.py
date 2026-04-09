from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, Headers, SessionForUser

ORGS = "/api/v1/organizations"
CORE = f"{ORGS}/core"
MEMBERS = f"{CORE}/members"


def test_create_update_organization(client: TestClient, owner_headers: Headers, outsider_headers: Headers) -> None:
    created = client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201
    assert created.json()["handle"] == "core"
    assert created.json()["name"] == "Core"
    assert client.patch(CORE, headers=outsider_headers, json={"name": "Nope"}).status_code == 403
    updated = client.patch(CORE, headers=owner_headers, json={"name": "Core Team"})
    assert updated.status_code == 200 and updated.json()["name"] == "Core Team"


def test_cannot_demote_or_remove_only_admin(client: TestClient, owner_headers: Headers) -> None:
    assert client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"}).status_code == 201
    assert client.put(f"{MEMBERS}/owner", headers=owner_headers, json={"role": "MEMBER"}).status_code == 400
    assert client.delete(f"{MEMBERS}/owner", headers=owner_headers).status_code == 400


def test_member_can_remove_self(
    client: TestClient, owner_headers: Headers, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    member = create_user(email="member@example.com", handle="member", name="Member")
    member_headers = session_for_user(member)
    assert client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"}).status_code == 201
    assert client.put(f"{MEMBERS}/member", headers=owner_headers, json={}).status_code == 200
    removed = client.delete(f"{MEMBERS}/member", headers=member_headers)
    assert removed.status_code == 200 and removed.json() == {"status": "ok"}
    listed = client.get(MEMBERS)
    assert listed.status_code == 200 and [entry["handle"] for entry in listed.json()] == ["owner"]

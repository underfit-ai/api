from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateUser, Headers, SessionForUser


def test_create_and_update_organization_as_admin(client: TestClient, owner_headers: Headers) -> None:
    created = client.post("/api/v1/organizations", headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201
    assert created.json()["handle"] == "core"
    assert created.json()["name"] == "Core"

    updated = client.patch("/api/v1/organizations/core", headers=owner_headers, json={"name": "Core Team"})
    assert updated.status_code == 200
    assert updated.json()["name"] == "Core Team"


def test_non_admin_cannot_update_organization(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
) -> None:
    created = client.post("/api/v1/organizations", headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201

    forbidden = client.patch("/api/v1/organizations/core", headers=outsider_headers, json={"name": "Nope"})
    assert forbidden.status_code == 403


def test_cannot_demote_or_remove_only_admin(client: TestClient, owner_headers: Headers) -> None:
    created = client.post("/api/v1/organizations", headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201

    demoted = client.put("/api/v1/organizations/core/members/owner", headers=owner_headers, json={"role": "MEMBER"})
    assert demoted.status_code == 400

    removed = client.delete("/api/v1/organizations/core/members/owner", headers=owner_headers)
    assert removed.status_code == 400


def test_member_can_remove_self(
    client: TestClient, owner_headers: Headers, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    member = create_user(email="member@example.com", handle="member", name="Member")
    member_headers = session_for_user(member)

    created = client.post("/api/v1/organizations", headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201

    added = client.put("/api/v1/organizations/core/members/member", headers=owner_headers, json={})
    assert added.status_code == 200
    assert added.json()["role"] == "MEMBER"

    removed = client.delete("/api/v1/organizations/core/members/member", headers=member_headers)
    assert removed.status_code == 200
    assert removed.json() == {"ok": True}

    listed = client.get("/api/v1/organizations/core/members")
    assert listed.status_code == 200
    assert [entry["handle"] for entry in listed.json()] == ["owner"]

from __future__ import annotations

import uuid

from fastapi.testclient import TestClient

from app.db import get_engine
from app.repositories import organizations as organizations_repo
from tests.conftest import AddCollaborator, CreateUser, OwnerHeaders, SessionForUser


def test_collaborators_for_user_owned_project(
    client: TestClient,
    owner_headers: OwnerHeaders,
    create_user: CreateUser,
    session_for_user: SessionForUser,
    add_collaborator: AddCollaborator,
) -> None:
    outsider = create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    outsider_headers = session_for_user(outsider)

    project = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "proj", "description": "x", "visibility": "private"},
    )
    assert project.status_code == 200

    added = add_collaborator(owner_headers, project="proj")
    assert added.json()["userId"] == str(outsider.id)

    add_collaborator(owner_headers, project="proj", expected_status=409)

    forbidden = client.delete(
        "/api/v1/accounts/owner/projects/proj/collaborators/outsider",
        headers=outsider_headers,
    )
    assert forbidden.status_code == 403

    listed = client.get("/api/v1/accounts/owner/projects/proj/collaborators", headers=owner_headers)
    assert listed.status_code == 200
    assert [user["handle"] for user in listed.json()] == ["outsider"]

    removed = client.delete(
        "/api/v1/accounts/owner/projects/proj/collaborators/outsider",
        headers=owner_headers,
    )
    assert removed.status_code == 200
    assert removed.json() == {"ok": True}


def test_collaborators_for_organization_owned_project(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    admin = create_user(email="admin@example.com", handle="admin", name="Admin")
    member = create_user(email="member@example.com", handle="member", name="Member")
    target = create_user(email="target@example.com", handle="target", name="Target")
    admin_headers = session_for_user(admin)
    member_headers = session_for_user(member)

    created_org = client.post(
        "/api/v1/organizations",
        headers=admin_headers,
        json={"handle": "org", "name": "Org"},
    )
    assert created_org.status_code == 201
    org_id = uuid.UUID(created_org.json()["id"])

    with get_engine().begin() as conn:
        organizations_repo.add_member(conn, org_id, member.id, "MEMBER")

    created_project = client.post(
        "/api/v1/accounts/org/projects",
        headers=admin_headers,
        json={"name": "proj", "description": "x", "visibility": "private"},
    )
    assert created_project.status_code == 200

    member_forbidden = client.put(
        "/api/v1/accounts/org/projects/proj/collaborators/target",
        headers=member_headers,
    )
    assert member_forbidden.status_code == 403

    admin_added = client.put(
        "/api/v1/accounts/org/projects/proj/collaborators/target",
        headers=admin_headers,
    )
    assert admin_added.status_code == 200
    assert admin_added.json()["userId"] == str(target.id)

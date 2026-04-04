from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateOrg, CreateOrgMember, CreateUser, Headers, SessionForUser


def test_collaborators_for_user_owned_project(
    client: TestClient, owner_headers: Headers, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    outsider = create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    outsider_headers = session_for_user(outsider)
    proj_base = "/api/v1/accounts/owner/projects/proj"
    collab_path = f"{proj_base}/collaborators/outsider"

    project_payload = {"name": "proj", "visibility": "private"}
    url = "/api/v1/accounts/owner/projects"
    assert client.post(url, headers=owner_headers, json=project_payload).status_code == 200

    added = client.put(collab_path, headers=owner_headers)
    assert added.status_code == 200 and added.json()["id"] == str(outsider.id)

    assert client.put(collab_path, headers=owner_headers).status_code == 409
    assert client.delete(f"{proj_base}/collaborators/outsider", headers=outsider_headers).status_code == 403
    listed = client.get(f"{proj_base}/collaborators", headers=owner_headers)
    assert listed.status_code == 200 and [user["handle"] for user in listed.json()] == ["outsider"]
    removed = client.delete(f"{proj_base}/collaborators/outsider", headers=owner_headers)
    assert removed.status_code == 200 and removed.json() == {"status": "ok"}


def test_collaborators_for_organization_owned_project(
    client: TestClient, create_org: CreateOrg, create_org_member: CreateOrgMember,
    create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    admin = create_user(email="admin@example.com", handle="admin", name="Admin")
    target = create_user(email="target@example.com", handle="target", name="Target")
    admin_headers = session_for_user(admin)
    created_org = create_org(admin_headers, handle="org", name="Org")
    member_headers = create_org_member(created_org["id"], "member@example.com", "member", "Member")
    collab_path = "/api/v1/accounts/org/projects/proj/collaborators/target"

    project_payload = {"name": "proj", "visibility": "private"}
    assert client.post("/api/v1/accounts/org/projects", headers=admin_headers, json=project_payload).status_code == 200

    assert client.put(collab_path, headers=member_headers).status_code == 403
    admin_added = client.put(collab_path, headers=admin_headers)
    assert admin_added.status_code == 200 and admin_added.json()["id"] == str(target.id)

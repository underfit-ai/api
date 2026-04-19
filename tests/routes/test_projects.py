from __future__ import annotations

from typing import cast

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import Engine

import underfit_api.routes.projects as projects_route
from tests.conftest import CreateOrg, CreateOrgMember, CreateProject, Headers
from underfit_api.config import config
from underfit_api.repositories import projects as projects_repo
from underfit_api.storage import Storage

BASE = "/api/v1/accounts/owner/projects"


def test_project_lifecycle(client: TestClient, owner_headers: Headers) -> None:
    create_payload = {
        "name": "Underfit",
        "description": "Tracking",
        "metadata": {"charts": {"default": "loss"}},
        "visibility": "private",
    }
    created = client.post(BASE, headers=owner_headers, json=create_payload)
    assert created.status_code == 200
    assert created.json()["name"] == "underfit"
    assert created.json()["metadata"] == {"charts": {"default": "loss"}}

    fetched = client.get(f"{BASE}/UNDERFIT", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["description"] == "Tracking"

    listed = client.get(BASE, headers=owner_headers)
    assert listed.status_code == 200 and [p["name"] for p in listed.json()] == ["underfit"]

    update_payload = {"description": "Updated", "metadata": {"charts": {"default": "accuracy"}}, "visibility": "public"}
    updated = client.put(f"{BASE}/underfit", headers=owner_headers, json=update_payload)
    assert updated.status_code == 200
    assert (updated.json()["description"], updated.json()["visibility"]) == ("Updated", "public")
    assert updated.json()["metadata"] == {"charts": {"default": "accuracy"}}

    preserved = client.put(f"{BASE}/underfit", headers=owner_headers, json={"description": "Retitled"})
    assert preserved.status_code == 200
    assert preserved.json()["description"] == "Retitled"
    assert preserved.json()["metadata"] == {"charts": {"default": "accuracy"}}

    cleared = client.put(f"{BASE}/underfit", headers=owner_headers, json={"metadata": {}})
    assert cleared.status_code == 200
    assert cleared.json()["metadata"] == {}


def test_update_project_ui_state(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    ui_url = f"{BASE}/underfit/ui-state"
    resp = client.put(ui_url, headers=owner_headers, json={"uiState": {"charts": "all"}})
    assert resp.json()["uiState"] == {"charts": "all"}

    config.backfill.enabled = True
    resp = client.put(ui_url, headers=owner_headers, json={"uiState": {"charts": "loss"}})
    assert resp.status_code == 200 and resp.json()["uiState"] == {"charts": "loss"}
    assert client.put(f"{BASE}/underfit", headers=owner_headers, json={"description": "x"}).status_code == 409


def test_project_visibility_and_listing(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_project: CreateProject, create_org: CreateOrg, create_org_member: CreateOrgMember,
) -> None:
    create_project(handle="owner", name="private")
    create_project(handle="owner", name="shared")
    create_project(handle="owner", name="public", visibility="public")
    create_project(handle="outsider", name="outsider-private")

    def _project_names(projects: Response) -> set[str]:
        return {cast(str, project["name"]) for project in projects.json()}

    assert client.get(f"{BASE}/public").status_code == 200
    assert client.get(f"{BASE}/private").status_code == 401
    assert client.get(f"{BASE}/private", headers=outsider_headers).status_code == 403
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 403
    owner_projects = client.get(BASE, headers=owner_headers)
    assert owner_projects.status_code == 200 and _project_names(owner_projects) == {"private", "public", "shared"}
    outsider_projects = client.get(BASE, headers=outsider_headers)
    assert outsider_projects.status_code == 200 and _project_names(outsider_projects) == {"public"}
    public_projects = client.get(BASE)
    assert public_projects.status_code == 200 and _project_names(public_projects) == {"public"}
    collab_url = "/api/v1/accounts/owner/projects/shared/collaborators/outsider"
    added = client.put(collab_url, headers=owner_headers)
    assert added.status_code == 200
    assert client.put(collab_url, headers=owner_headers).status_code == 409
    listed = client.get(f"{BASE}/shared/collaborators", headers=owner_headers).json()
    assert [u["handle"] for u in listed] == ["outsider"]
    assert client.delete(collab_url, headers=outsider_headers).status_code == 403
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 200
    assert _project_names(client.get(BASE, headers=outsider_headers)) == {"public", "shared"}
    launched = client.post("/api/v1/accounts/owner/projects/shared/runs/launch", headers=outsider_headers, json={
        "runName": "r", "launchId": "1",
    })
    assert launched.status_code == 200
    my_projects = client.get("/api/v1/me/projects", headers=outsider_headers)
    assert my_projects.status_code == 200
    assert [cast(str, project["name"]) for project in my_projects.json()] == ["shared", "outsider-private"]
    removed = client.delete(collab_url, headers=owner_headers)
    assert removed.status_code == 200 and removed.json() == {"status": "ok"}
    assert client.delete(collab_url, headers=owner_headers).status_code == 404
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 403

    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    member_headers = create_org_member(org["id"], "member@example.com", "member", "Member")
    create_project(handle="core", name="secret")
    create_project(handle="core", name="open", visibility="public")
    launch_url = "/api/v1/accounts/core/projects/secret/runs/launch"
    assert client.get("/api/v1/accounts/core/projects/secret", headers=member_headers).status_code == 403
    assert client.get("/api/v1/accounts/core/projects/secret", headers=admin_headers).status_code == 200
    assert client.get("/api/v1/accounts/core/projects/secret", headers=outsider_headers).status_code == 403
    assert client.get("/api/v1/accounts/core/projects/open", headers=outsider_headers).status_code == 200
    assert client.get("/api/v1/accounts/core/projects/open").status_code == 200
    assert client.post(launch_url, headers=member_headers, json={"runName": "r", "launchId": "1"}).status_code == 403
    assert client.post(launch_url, headers=admin_headers, json={"runName": "r", "launchId": "1"}).status_code == 200
    org_collab = "/api/v1/accounts/core/projects/secret/collaborators/outsider"
    assert client.put(org_collab, headers=member_headers).status_code == 403
    assert client.put(org_collab, headers=admin_headers).status_code == 200
    assert client.delete(org_collab, headers=admin_headers).status_code == 200
    assert client.delete(org_collab, headers=admin_headers).status_code == 404
    org_projects = client.get("/api/v1/accounts/core/projects", headers=admin_headers)
    assert org_projects.status_code == 200 and _project_names(org_projects) == {"secret", "open"}
    admin_projects = client.get("/api/v1/me/projects", headers=admin_headers)
    assert admin_projects.status_code == 200 and _project_names(admin_projects) == {"secret", "open"}


def test_project_creation_and_rename_validation(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    create_project(handle="owner", name="project2")

    assert client.post(
        "/api/v1/accounts/missing/projects", headers=owner_headers,
        json={"name": "underfit", "visibility": "private"},
    ).status_code == 404
    assert client.post(BASE, headers=owner_headers, json={"name": "x", "visibility": "internal"}).status_code == 400
    conflict = client.post(BASE, headers=owner_headers, json={"name": "UNDERFIT", "visibility": "private"})
    assert conflict.status_code == 409
    assert client.put(f"{BASE}/underfit", headers=outsider_headers, json={"description": "hacked"}).status_code == 403
    assert client.post(
        f"{BASE}/underfit/rename", headers=outsider_headers, json={"name": "hacked"},
    ).status_code == 403

    renamed = client.post(f"{BASE}/underfit/rename", headers=owner_headers, json={"name": "new-project"})
    assert renamed.status_code == 200 and renamed.json()["name"] == "new-project"
    assert client.get("/api/v1/accounts/owner/projects/underfit", headers=owner_headers).status_code == 404
    assert client.post(
        f"{BASE}/new-project/rename", headers=owner_headers, json={"name": "project2"},
    ).status_code == 409


def test_delete_project(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_project: CreateProject,
    create_org: CreateOrg, create_org_member: CreateOrgMember,
    engine: Engine, storage: Storage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = create_project(handle="owner", name="underfit")
    run = client.post("/api/v1/accounts/owner/projects/underfit/runs/launch", headers=owner_headers, json={
        "runName": "my-run", "launchId": "delete-project",
    }).json()
    storage.write(f"{project.id}/artifacts/project.txt", b"project")
    storage.write(f"{run['id']}/files/run.txt", b"run")
    delete_prefix = projects_route.delete_prefix

    def _delete_prefix(s: Storage, prefix: str) -> None:
        with engine.begin() as conn:
            assert projects_repo.get_by_id(conn, project.id) is None
        delete_prefix(s, prefix)

    monkeypatch.setattr(projects_route, "delete_prefix", _delete_prefix)
    assert client.delete(f"{BASE}/underfit", headers=outsider_headers).status_code == 403
    assert client.delete(f"{BASE}/underfit", headers=owner_headers).status_code == 200
    assert not storage.exists(f"{project.id}/artifacts/project.txt")
    assert not storage.exists(f"{run['id']}/files/run.txt")

    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    member_headers = create_org_member(org["id"], "member@example.com", "member", "Member")
    create_project(handle="core", name="secret")
    assert client.delete("/api/v1/accounts/core/projects/secret", headers=member_headers).status_code == 403
    assert client.delete("/api/v1/accounts/core/projects/secret", headers=admin_headers).status_code == 200

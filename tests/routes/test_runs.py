from __future__ import annotations

import json
from datetime import timedelta
from typing import cast
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

import underfit_api.routes.runs as runs_route
from tests.conftest import AddCollaborator, CreateOrg, CreateOrgMember, CreateProject, Headers
from underfit_api.auth import verify_signed_token
from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.repositories import runs as runs_repo
from underfit_api.schema import projects, run_workers, runs
from underfit_api.storage.types import Storage

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"
RUNS = "/api/v1/accounts/owner/projects/underfit/runs"


def test_launch_lifecycle(
    client: TestClient, owner_headers: Headers, create_project: CreateProject, engine: Engine,
) -> None:
    create_project(handle="owner", name="underfit")

    run = client.post(LAUNCH, headers=owner_headers, json={
        "runName": "my-run", "launchId": "abc-123", "workerLabel": "0",
        "config": {"lr": 0.001}, "metadata": {"summary": {"loss": 0.5}},
    }).json()
    assert run["isActive"] is True
    assert run["terminalState"] is None
    assert run["config"] == {"lr": 0.001}
    assert run["metadata"] == {"summary": {"loss": 0.5}}
    assert run["launchId"] == "abc-123"
    assert run["name"] == "my-run"
    assert run["workerToken"] is not None

    fetched = client.get(f"{RUNS}/{cast(str, run['name']).upper()}", headers=owner_headers)
    assert fetched.status_code == 200
    assert fetched.json()["id"] == run["id"]

    updated = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"metadata": {"summary": {"loss": 0.25}}})
    assert updated.status_code == 200
    assert updated.json()["config"] == {"lr": 0.001}
    assert updated.json()["metadata"] == {"summary": {"loss": 0.25}}

    preserved = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={})
    assert preserved.status_code == 200
    assert preserved.json()["metadata"] == {"summary": {"loss": 0.25}}

    cleared = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"metadata": {}})
    assert cleared.status_code == 200
    assert cleared.json()["metadata"] == {}

    with engine.begin() as conn:
        conn.execute(run_workers.update().values(last_heartbeat=utcnow() - timedelta(seconds=16)))
    assert client.get(f"{RUNS}/{run['name']}", headers=owner_headers).json()["isActive"] is False

    assert len(client.get(RUNS, headers=owner_headers).json()) == 1


def test_launch_join_and_retry(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    payload = {"runName": "my-run", "launchId": "abc-123", "workerLabel": "0"}
    first = client.post(LAUNCH, headers=owner_headers, json=payload).json()
    retry = client.post(LAUNCH, headers=owner_headers, json=payload).json()
    assert (first_token := verify_signed_token(str(first["workerToken"])))
    assert (retry_token := verify_signed_token(str(retry["workerToken"])))
    assert first_token["worker_id"] == retry_token["worker_id"]

    payload = {"runName": "ignored", "launchId": "abc-123", "workerLabel": "1"}
    joined = client.post(LAUNCH, headers=owner_headers, json=payload)
    assert joined.status_code == 200
    assert joined.json()["id"] == first["id"]
    assert joined.json()["name"] == "my-run"
    assert joined.json()["workerToken"] != first["workerToken"]
    workers = client.get(f"{RUNS}/{first['name']}/workers", headers=owner_headers).json()
    assert {w["workerLabel"] for w in workers} == {"0", "1"}


def test_launch_rejects_stale_run(
    client: TestClient, owner_headers: Headers, create_project: CreateProject, engine: Engine,
) -> None:
    create_project(handle="owner", name="underfit")
    client.post(LAUNCH, headers=owner_headers, json={"runName": "my-run", "launchId": "abc-123", "workerLabel": "0"})
    with engine.begin() as conn:
        conn.execute(run_workers.update().values(last_heartbeat=utcnow() - timedelta(seconds=16)))
    body = {"runName": "new", "launchId": "abc-123", "workerLabel": "1"}
    resp = client.post(LAUNCH, headers=owner_headers, json=body)
    assert resp.status_code == 409


@pytest.mark.parametrize(("auth", "payload", "status"), [
    (False, {"runName": "r", "launchId": "1"}, 401),
    (True, {"runName": "r", "launchId": "1", "config": {"blob": "x" * 70000}}, 400),
])
def test_launch_validation(
    auth: bool, payload: dict[str, object], status: int,
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    headers = owner_headers if auth else None
    assert client.post(LAUNCH, headers=headers, json=payload).status_code == status


def test_launch_duplicate_name(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    client.post(LAUNCH, headers=owner_headers, json={"runName": "baseline", "launchId": "id-1", "workerLabel": "0"})
    resp = client.post(LAUNCH, headers=owner_headers, json={"runName": "baseline", "launchId": "id-2"})
    assert resp.status_code == 409


def test_list_user_runs_visibility(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_project: CreateProject, add_collaborator: AddCollaborator,
) -> None:
    for name, visibility in [("private", "private"), ("shared", "private"), ("public", "public")]:
        create_project(handle="owner", name=name, visibility=visibility)
        url = f"/api/v1/accounts/owner/projects/{name}/runs/launch"
        assert client.post(url, headers=owner_headers, json={"runName": name, "launchId": name}).status_code == 200
    add_collaborator(handle="owner", project_name="shared", user_handle="outsider")

    owner_runs = client.get("/api/v1/users/owner/runs", headers=owner_headers).json()
    assert [run["name"] for run in owner_runs] == ["public", "shared", "private"]
    outsider_runs = client.get("/api/v1/users/owner/runs", headers=outsider_headers).json()
    assert [run["name"] for run in outsider_runs] == ["public", "shared"]
    public_runs = client.get("/api/v1/users/owner/runs").json()
    assert [run["name"] for run in public_runs] == ["public"]


def test_list_project_runs_ordering(
    client: TestClient, owner_headers: Headers, create_project: CreateProject, engine: Engine,
) -> None:
    project = create_project(handle="owner", name="underfit")
    client.post(LAUNCH, headers=owner_headers, json={"runName": "newest", "launchId": "1"})
    pinned = client.post(LAUNCH, headers=owner_headers, json={"runName": "pinned", "launchId": "2"}).json()
    baseline = client.post(LAUNCH, headers=owner_headers, json={"runName": "baseline", "launchId": "3"}).json()
    with engine.begin() as conn:
        conn.execute(runs.update().where(runs.c.id == UUID(cast(str, pinned["id"]))).values(is_pinned=True))
        conn.execute(projects.update().where(projects.c.id == project.id).values(
            baseline_project_id=project.id,
            baseline_run_id=UUID(cast(str, baseline["id"])),
        ))
    runs_list = client.get(RUNS, headers=owner_headers).json()
    assert [run["name"] for run in runs_list] == ["baseline", "pinned", "newest"]
    assert runs_list[0]["isBaseline"] is True and runs_list[1]["isPinned"] is True


def test_update_run_ui_state(
    client: TestClient, owner_headers: Headers, create_project: CreateProject, storage: Storage,
) -> None:
    create_project(handle="owner", name="underfit")
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    ui_url = f"{RUNS}/{run['name']}/ui-state"

    resp = client.put(ui_url, headers=owner_headers, json={"uiState": {"layout": "grid"}, "isPinned": True})
    assert resp.json()["uiState"] == {"layout": "grid"} and resp.json()["isPinned"] is True

    partial = client.put(ui_url, headers=owner_headers, json={"isPinned": False})
    assert partial.json()["uiState"] == {"layout": "grid"} and partial.json()["isPinned"] is False

    resp = client.put(ui_url, headers=owner_headers, json={"isBaseline": True})
    assert resp.json()["isBaseline"] is True
    project = client.get("/api/v1/accounts/owner/projects/underfit", headers=owner_headers).json()
    assert project["baselineRunId"] == run["id"]

    config.storage.backfill.enabled = True
    try:
        resp = client.put(ui_url, headers=owner_headers, json={"uiState": {"layout": "list"}, "isPinned": True})
        assert resp.status_code == 200
        assert client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"metadata": {}}).status_code == 409
    finally:
        config.storage.backfill.enabled = False

    assert json.loads(storage.read(f"{run['id']}/ui.json")) == {
        "uiState": {"layout": "list"}, "isPinned": True, "isBaseline": True,
    }


def test_delete_run(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_project: CreateProject,
    add_collaborator: AddCollaborator, create_org: CreateOrg, create_org_member: CreateOrgMember,
    engine: Engine, storage: Storage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_project(handle="owner", name="underfit")
    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    run = client.post(LAUNCH, headers=outsider_headers, json={"runName": "mine", "launchId": "mine"}).json()
    storage.write(f"{run['id']}/logs/1.txt", b"log")
    delete_prefix = runs_route.delete_prefix

    def _delete_prefix(s: Storage, prefix: str) -> None:
        with engine.begin() as conn:
            assert runs_repo.get_by_id(conn, UUID(cast(str, run["id"]))) is None
        delete_prefix(s, prefix)

    monkeypatch.setattr(runs_route, "delete_prefix", _delete_prefix)
    assert client.delete(f"{RUNS}/{run['name']}", headers=outsider_headers).status_code == 200
    assert not storage.exists(f"{run['id']}/logs/1.txt")

    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    create_project(handle="core", name="secret")
    project_base = "/api/v1/accounts/core/projects/secret"
    payload = {"runName": "org-run", "launchId": "org-run"}
    org_run = client.post(f"{project_base}/runs/launch", headers=owner_headers, json=payload).json()
    storage.write(f"{org_run['id']}/media/0", b"media")
    assert client.delete(f"{project_base}/runs/org-run", headers=outsider_headers).status_code == 403
    assert client.delete(f"{project_base}/runs/org-run", headers=admin_headers).status_code == 200
    assert not storage.exists(f"{org_run['id']}/media/0")

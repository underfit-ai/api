from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateProject, CreateRun, OutsiderHeaders, OwnerHeaders


def test_artifact_create_upload_finalize_flow(
    client: TestClient, owner_headers: OwnerHeaders, create_run: CreateRun,
) -> None:
    run = create_run(owner_headers)

    created = client.post(
        "/api/v1/accounts/owner/projects/underfit/artifacts",
        headers=owner_headers,
        json={
            "run_id": run["id"],
            "step": 10,
            "name": "checkpoint",
            "type": "model",
            "manifest": {
                "files": [{"path": "weights.bin"}, {"path": "weights.bin"}, {"path": "dir/config.json"}],
                "references": ["https://example.com/a", "https://example.com/a"],
            },
        },
    )
    assert created.status_code == 200
    artifact = created.json()
    assert artifact["declaredFileCount"] == 2

    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    bad_path = client.put(file_base + "/other.bin", headers=owner_headers, content=b"x")
    assert bad_path.status_code == 400

    uploaded_1 = client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights")
    assert uploaded_1.status_code == 200
    assert uploaded_1.json()["uploadedFileCount"] == 1

    downloaded_1 = client.get(file_base + "/weights.bin", headers=owner_headers)
    assert downloaded_1.status_code == 200
    assert downloaded_1.content == b"weights"

    finalize_blocked = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers)
    assert finalize_blocked.status_code == 409

    uploaded_2 = client.put(file_base + "/dir/config.json", headers=owner_headers, content=b"{}")
    assert uploaded_2.status_code == 200

    downloaded_2 = client.get(file_base + "/dir/config.json", headers=owner_headers)
    assert downloaded_2.status_code == 200
    assert downloaded_2.content == b"{}"

    finalized = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers)
    assert finalized.status_code == 200
    assert finalized.json() == {"success": True}

    no_more_writes = client.put(file_base + "/weights.bin", headers=owner_headers, content=b"new")
    assert no_more_writes.status_code == 409


def test_artifact_updates_require_project_access(
    client: TestClient,
    owner_headers: OwnerHeaders,
    create_run: CreateRun,
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
) -> None:
    run = create_run(owner_headers)

    created = client.post(
        "/api/v1/accounts/owner/projects/underfit/artifacts",
        headers=owner_headers,
        json={
            "run_id": run["id"],
            "step": 1,
            "name": "checkpoint",
            "type": "model",
            "manifest": {"files": [{"path": "weights.bin"}, {"path": "dir/config.json"}]},
        },
    )
    assert created.status_code == 200
    artifact = created.json()

    base = f"/api/v1/artifacts/{artifact['id']}"

    forbidden_upload = client.put(f"{base}/files/weights.bin", headers=outsider_headers, content=b"x")
    assert forbidden_upload.status_code == 403

    forbidden_finalize = client.post(f"{base}/finalize", headers=outsider_headers)
    assert forbidden_finalize.status_code == 403

    add_collaborator(owner_headers)

    uploaded_1 = client.put(f"{base}/files/weights.bin", headers=outsider_headers, content=b"weights")
    assert uploaded_1.status_code == 200

    uploaded_2 = client.put(f"{base}/files/dir/config.json", headers=outsider_headers, content=b"{}")
    assert uploaded_2.status_code == 200

    finalized = client.post(f"{base}/finalize", headers=outsider_headers)
    assert finalized.status_code == 200
    assert finalized.json() == {"success": True}


def test_artifact_create_requires_project_access(
    client: TestClient,
    create_project: CreateProject,
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
    owner_headers: OwnerHeaders,
) -> None:
    create_project(owner_headers)

    forbidden = client.post(
        "/api/v1/accounts/owner/projects/underfit/artifacts",
        headers=outsider_headers,
        json={"name": "ckpt", "type": "model", "manifest": {"files": [{"path": "a.bin"}]}},
    )
    assert forbidden.status_code == 403

    add_collaborator(owner_headers)

    allowed = client.post(
        "/api/v1/accounts/owner/projects/underfit/artifacts",
        headers=outsider_headers,
        json={"name": "ckpt", "type": "model", "manifest": {"files": [{"path": "a.bin"}]}},
    )
    assert allowed.status_code == 200


def test_artifact_validates_step_requires_run_id(
    client: TestClient, owner_headers: OwnerHeaders, create_project: CreateProject,
) -> None:
    create_project(owner_headers)

    response = client.post(
        "/api/v1/accounts/owner/projects/underfit/artifacts",
        headers=owner_headers,
        json={"step": 3, "name": "artifact", "type": "dataset", "manifest": {"files": [{"path": "a.bin"}]}},
    )
    assert response.status_code == 400


@pytest.mark.parametrize(
    "manifest",
    [
        {"files": [{"path": "../weights.bin"}]},
        {"files": [{"path": "/etc/passwd"}]},
    ],
    ids=["dot-segment", "absolute"],
)
def test_artifact_rejects_invalid_paths(
    client: TestClient, owner_headers: OwnerHeaders, create_run: CreateRun, manifest: dict[str, object],
) -> None:
    run = create_run(owner_headers)
    response = client.post(
        "/api/v1/accounts/owner/projects/underfit/artifacts",
        headers=owner_headers,
        json={"run_id": run["id"], "name": "ckpt", "type": "model", "manifest": manifest},
    )
    assert response.status_code == 400

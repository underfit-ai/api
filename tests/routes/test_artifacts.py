from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateProject, CreateRun, Headers

ARTIFACTS = "/api/v1/accounts/owner/projects/underfit/artifacts"


def test_artifact_create_upload_finalize_flow(
    client: TestClient, owner_headers: Headers, create_run: CreateRun,
) -> None:
    run = create_run(owner_headers)

    manifest = {
        "files": [{"path": "weights.bin"}, {"path": "weights.bin"}, {"path": "dir/config.json"}],
        "references": ["https://example.com/a", "https://example.com/a"],
    }
    payload = {"run_id": run["id"], "step": 10, "name": "checkpoint", "type": "model", "manifest": manifest}
    created = client.post(ARTIFACTS, headers=owner_headers, json=payload)
    assert created.status_code == 200
    artifact = created.json()
    assert artifact["declaredFileCount"] == 2

    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    assert client.put(file_base + "/other.bin", headers=owner_headers, content=b"x").status_code == 400

    uploaded_1 = client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights")
    assert uploaded_1.status_code == 200
    assert uploaded_1.json()["uploadedFileCount"] == 1

    downloaded_1 = client.get(file_base + "/weights.bin", headers=owner_headers)
    assert (downloaded_1.status_code, downloaded_1.content) == (200, b"weights")

    assert client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers).status_code == 409

    assert client.put(file_base + "/dir/config.json", headers=owner_headers, content=b"{}").status_code == 200

    downloaded_2 = client.get(file_base + "/dir/config.json", headers=owner_headers)
    assert (downloaded_2.status_code, downloaded_2.content) == (200, b"{}")

    finalized = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers)
    assert finalized.status_code == 200
    assert finalized.json() == {"success": True}

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"new").status_code == 409


def test_artifact_access_controls(
    client: TestClient,
    owner_headers: Headers,
    outsider_headers: Headers,
    add_collaborator: AddCollaborator,
    create_run: CreateRun,
) -> None:
    run = create_run(owner_headers)
    minimal_manifest = {"manifest": {"files": [{"path": "a.bin"}]}}
    forbidden_payload = {"name": "ckpt", "type": "model", **minimal_manifest}
    assert client.post(ARTIFACTS, headers=outsider_headers, json=forbidden_payload).status_code == 403

    artifact_payload = {
        "run_id": run["id"],
        "step": 1,
        "name": "checkpoint",
        "type": "model",
        "manifest": {"files": [{"path": "weights.bin"}, {"path": "dir/config.json"}]},
    }
    artifact = client.post(ARTIFACTS, headers=owner_headers, json=artifact_payload).json()
    base = f"/api/v1/artifacts/{artifact['id']}"
    assert client.put(f"{base}/files/weights.bin", headers=outsider_headers, content=b"x").status_code == 403
    assert client.post(f"{base}/finalize", headers=outsider_headers).status_code == 403

    add_collaborator(owner_headers)

    allowed_payload = {"name": "ckpt", "type": "model", **minimal_manifest}
    assert client.post(ARTIFACTS, headers=outsider_headers, json=allowed_payload).status_code == 200
    assert client.put(f"{base}/files/weights.bin", headers=outsider_headers, content=b"weights").status_code == 200
    assert client.put(f"{base}/files/dir/config.json", headers=outsider_headers, content=b"{}").status_code == 200
    finalized = client.post(f"{base}/finalize", headers=outsider_headers)
    assert finalized.status_code == 200
    assert finalized.json() == {"success": True}


def test_artifact_validates_step_requires_run_id(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(owner_headers)
    payload = {"step": 3, "name": "artifact", "type": "dataset", "manifest": {"files": [{"path": "a.bin"}]}}
    assert client.post(ARTIFACTS, headers=owner_headers, json=payload).status_code == 400


@pytest.mark.parametrize(
    "manifest",
    [
        {"files": [{"path": "../weights.bin"}]},
        {"files": [{"path": "/etc/passwd"}]},
    ],
    ids=["dot-segment", "absolute"],
)
def test_artifact_rejects_invalid_paths(
    client: TestClient, owner_headers: Headers, create_run: CreateRun, manifest: dict[str, object],
) -> None:
    run = create_run(owner_headers)
    response = client.post(
        ARTIFACTS,
        headers=owner_headers,
        json={"run_id": run["id"], "name": "ckpt", "type": "model", "manifest": manifest},
    )
    assert response.status_code == 400

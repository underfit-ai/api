from __future__ import annotations

import json
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

import underfit_api.db as db
import underfit_api.storage as storage_mod
from tests.conftest import Headers
from underfit_api.repositories import media as media_repo

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"
MEDIA_METADATA = json.dumps({"key": "val/gen/%d", "step": 200, "type": "image"})
MEDIA_FILES = [
    ("files", ("a.png", b"file-a", "image/png")),
    ("files", ("b.png", b"file-b", "image/png")),
]


def test_media_ingest_and_retrieval(client: TestClient, owner_headers: Headers, outsider_headers: Headers) -> None:
    client.post(
        "/api/v1/accounts/owner/projects", headers=owner_headers, json={"name": "underfit", "visibility": "private"},
    )
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    media_url = f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/media"
    worker = {"Authorization": f"Bearer {run['workerToken']}"}

    created = client.post("/api/v1/ingest/media", headers=worker, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    assert created.status_code == 200
    rows = created.json()
    assert [r["index"] for r in rows] == [0, 1]
    assert all(r["key"] == "val/gen/%d" and r["step"] == 200 and r["type"] == "image" for r in rows)

    listed = client.get(media_url, headers=owner_headers, params={"key": "val/gen/%d", "step": 200})
    assert listed.status_code == 200 and [m["id"] for m in listed.json()] == [r["id"] for r in rows]
    wrong_run = client.post(LAUNCH, headers=owner_headers, json={"runName": "other", "launchId": "2"}).json()
    for expected, row in [(b"file-a", rows[0]), (b"file-b", rows[1])]:
        downloaded = client.get(f"{media_url}/{row['id']}/file", headers=owner_headers)
        assert downloaded.status_code == 200 and downloaded.content == expected
        assert client.get(f"{media_url}/{row['id']}/file").status_code == 401
        assert client.get(f"{media_url}/{row['id']}/file", headers=outsider_headers).status_code == 403
        assert client.get(
            f"/api/v1/accounts/owner/projects/underfit/runs/{wrong_run['name']}/media/{row['id']}/file",
            headers=owner_headers,
        ).status_code == 404

    duplicate = client.post(
        "/api/v1/ingest/media", headers=worker, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES,
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["error"] == "Media already exists for this type/key/step"


def test_media_ingest_rejects_mixed_types(client: TestClient, owner_headers: Headers) -> None:
    client.post(
        "/api/v1/accounts/owner/projects", headers=owner_headers, json={"name": "underfit", "visibility": "private"},
    )
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    mixed = [MEDIA_FILES[0], ("files", ("b.mp4", b"file-b", "video/mp4"))]
    headers = {"Authorization": f"Bearer {run['workerToken']}"}
    response = client.post("/api/v1/ingest/media", headers=headers, data={"metadata": MEDIA_METADATA}, files=mixed)
    assert response.status_code == 400
    assert response.json()["error"] == "Files must all match the declared media type"


def test_media_ingest_cleans_up_failed_upload(
    client: TestClient, owner_headers: Headers, monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_payload = {"name": "underfit", "visibility": "private"}
    client.post("/api/v1/accounts/owner/projects", headers=owner_headers, json=project_payload)
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()

    async def fail_write_stream(key: str, stream: object) -> int:
        storage_mod.storage.write(key, b"partial")
        raise RuntimeError("boom")

    monkeypatch.setattr(storage_mod.storage, "write_stream", fail_write_stream)
    with pytest.raises(RuntimeError, match="boom"):
        client.post(
            "/api/v1/ingest/media",
            headers={"Authorization": f"Bearer {run['workerToken']}"},
            data={"metadata": MEDIA_METADATA},
            files=MEDIA_FILES,
        )
    assert storage_mod.storage.list_files(run["id"]) == []


def test_media_ingest_conflicts_with_pending_group(client: TestClient, owner_headers: Headers) -> None:
    project_payload = {"name": "underfit", "visibility": "private"}
    client.post("/api/v1/accounts/owner/projects", headers=owner_headers, json=project_payload)
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    with db.engine.begin() as conn:
        media_repo.create(
            conn, UUID(run["id"]), "val/gen/%d", 200, "image", 0, "media/image/val/gen/%d_200_0.png", None,
        )
    response = client.post(
        "/api/v1/ingest/media", headers={"Authorization": f"Bearer {run['workerToken']}"},
        data={"metadata": MEDIA_METADATA}, files=[MEDIA_FILES[0]],
    )
    assert response.status_code == 409 and response.json()["error"] == "Media already exists for this type/key/step"

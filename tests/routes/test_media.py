from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.conftest import Headers

MEDIA_METADATA = json.dumps({"key": "predictions", "step": 10, "type": "image"})
MEDIA_FILES = [("files", ("a.bin", b"file-a", "application/octet-stream"))]


def test_media_ingest_requires_primary_worker(client: TestClient, owner_headers: Headers) -> None:
    client.post(
        "/api/v1/accounts/owner/projects", headers=owner_headers, json={"name": "underfit", "visibility": "private"},
    )
    run = client.post("/api/v1/accounts/owner/projects/underfit/runs", headers=owner_headers, json={}).json()
    media_url = f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/media"
    primary = {"Authorization": f"Bearer {run['workerToken']}"}

    created = client.post("/api/v1/ingest/media", headers=primary, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    media_id = created.json()["id"]
    assert created.status_code == 200

    worker = client.post(
        f"{media_url.rsplit('/', 1)[0]}/workers", headers=owner_headers, json={"workerLabel": "1"},
    ).json()
    secondary = {"Authorization": f"Bearer {worker['workerToken']}"}
    response = client.post(
        "/api/v1/ingest/media", headers=secondary, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES,
    )
    assert response.status_code == 403

    listed = client.get(media_url, headers=owner_headers, params={"key": "predictions", "step": 10})
    assert listed.status_code == 200 and [m["id"] for m in listed.json()] == [media_id]
    downloaded = client.get(f"{media_url}/{media_id}/file", headers=owner_headers)
    assert downloaded.status_code == 200 and downloaded.content == b"file-a"

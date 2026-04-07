from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.conftest import Headers

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"
MEDIA_METADATA = json.dumps({"key": "predictions", "step": 10, "type": "image"})
MEDIA_FILES = [("files", ("a.bin", b"file-a", "application/octet-stream"))]


def test_media_ingest_and_retrieval(client: TestClient, owner_headers: Headers) -> None:
    client.post(
        "/api/v1/accounts/owner/projects", headers=owner_headers, json={"name": "underfit", "visibility": "private"},
    )
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    media_url = f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/media"
    worker = {"Authorization": f"Bearer {run['workerToken']}"}

    created = client.post("/api/v1/ingest/media", headers=worker, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    assert created.status_code == 200
    media_id = created.json()["id"]

    listed = client.get(media_url, headers=owner_headers, params={"key": "predictions", "step": 10})
    assert listed.status_code == 200 and [m["id"] for m in listed.json()] == [media_id]
    downloaded = client.get(f"{media_url}/{media_id}/file", headers=owner_headers)
    assert downloaded.status_code == 200 and downloaded.content == b"file-a"

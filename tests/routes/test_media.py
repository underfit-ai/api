from __future__ import annotations

import json

from fastapi.testclient import TestClient

import underfit_api.storage as storage_mod
from tests.conftest import Headers

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"
MEDIA_METADATA = json.dumps({"key": "val/gen/%d", "step": 200, "type": "image"})
MEDIA_FILES = [("files", ("a.png", b"file-a", "image/png"))]


def test_media_ingest_and_retrieval(client: TestClient, owner_headers: Headers) -> None:
    client.post(
        "/api/v1/accounts/owner/projects", headers=owner_headers, json={"name": "underfit", "visibility": "private"},
    )
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    media_url = f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/media"
    worker = {"Authorization": f"Bearer {run['workerToken']}"}

    created = client.post("/api/v1/ingest/media", headers=worker, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    assert created.status_code == 200
    media = created.json()
    media_id = media["id"]
    assert media["storagePrefix"] == "media/image/val/gen/%d_200"
    assert media["ext"] == ".png"
    assert storage_mod.storage.exists(f"{run['id']}/{media['storagePrefix']}_0{media['ext']}")

    listed = client.get(media_url, headers=owner_headers, params={"key": "val/gen/%d", "step": 200})
    assert listed.status_code == 200 and [m["id"] for m in listed.json()] == [media_id]
    downloaded = client.get(f"{media_url}/{media_id}/file", headers=owner_headers)
    assert downloaded.status_code == 200 and downloaded.content == b"file-a"

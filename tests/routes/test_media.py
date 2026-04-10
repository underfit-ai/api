from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.conftest import Headers

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"
MEDIA_METADATA = json.dumps({"key": "val/gen/%d", "step": 200, "type": "image"})
MEDIA_FILES = [
    ("files", ("a.png", b"file-a", "image/png")),
    ("files", ("b.png", b"file-b", "image/png")),
]


def test_media_ingest_and_retrieval(client: TestClient, owner_headers: Headers) -> None:
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
    for expected, row in [(b"file-a", rows[0]), (b"file-b", rows[1])]:
        downloaded = client.get(f"{media_url}/{row['id']}/file", headers=owner_headers)
        assert downloaded.status_code == 200 and downloaded.content == expected

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

from __future__ import annotations

from uuid import UUID

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import AddCollaborator, CreateRun, Headers
from underfit_api.helpers import validate_path
from underfit_api.schema import artifacts
from underfit_api.storage import Storage

PROJECT_ARTIFACTS = "/api/v1/accounts/owner/projects/underfit/artifacts"
RUN_ARTIFACTS = "/api/v1/accounts/owner/projects/underfit/runs/test-run/artifacts"


def test_artifact_upload(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    create_run(handle="owner", project_name="underfit", name="test-run")
    payload_1 = {"step": 3, "name": "artifact", "type": "dataset"}
    assert client.post(PROJECT_ARTIFACTS, headers=owner_headers, json=payload_1).status_code == 400

    payload_2 = {"step": 10, "name": "checkpoint", "type": "model", "metadata": {"tag": "best"}}
    created = client.post(RUN_ARTIFACTS, headers=owner_headers, json=payload_2)
    assert created.status_code == 200
    artifact = created.json()
    assert client.get(PROJECT_ARTIFACTS, headers=owner_headers).json() == []
    assert client.get(f"/api/v1/artifacts/{artifact['id']}", headers=owner_headers).status_code == 404

    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights").status_code == 200

    head = client.head(file_base + "/weights.bin", headers=owner_headers)
    assert head.status_code == 200 and head.headers["content-length"] == "7" and "last-modified" in head.headers

    downloaded = client.get(file_base + "/weights.bin", headers=owner_headers)
    assert (downloaded.status_code, downloaded.content) == (200, b"weights")
    assert downloaded.headers["accept-ranges"] == "bytes"

    ranged = client.get(file_base + "/weights.bin", headers={**owner_headers, "Range": "bytes=2-4"})
    assert (ranged.status_code, ranged.content) == (206, b"igh")
    assert ranged.headers["content-range"] == "bytes 2-4/7" and ranged.headers["content-length"] == "3"

    suffix = client.get(file_base + "/weights.bin", headers={**owner_headers, "Range": "bytes=-3"})
    assert (suffix.status_code, suffix.content) == (206, b"hts")

    open_end = client.get(file_base + "/weights.bin", headers={**owner_headers, "Range": "bytes=4-"})
    assert (open_end.status_code, open_end.content) == (206, b"hts")

    bad = client.get(file_base + "/weights.bin", headers={**owner_headers, "Range": "bytes=10-20"})
    assert bad.status_code == 416

    payload_3 = {"manifest": {"files": ["weights.bin", "dir/config.json"]}}
    missing = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload_3)
    assert missing.status_code == 409
    assert missing.json() == {"missing": ["dir/config.json"], "extra": []}

    assert client.put(file_base + "/dir/config.json", headers=owner_headers, content=b"{}").status_code == 200
    assert client.put(file_base + "/extra.bin", headers=owner_headers, content=b"x").status_code == 200
    extra = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload_3)
    assert extra.status_code == 409 and extra.json() == {"missing": [], "extra": ["extra.bin"]}
    assert client.delete(file_base + "/extra.bin", headers=owner_headers).status_code == 200
    assert client.head(file_base + "/extra.bin", headers=owner_headers).status_code == 404

    payload_4 = {
        "manifest": {
            "files": ["weights.bin", "weights.bin", "dir/config.json"],
            "references": [
                {"url": "https://example.com/a", "etag": "old"},
                {"url": "https://example.com/a", "etag": "new", "sha256": "abc"},
            ],
        },
    }
    finalized = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload_4)
    assert finalized.status_code == 200
    assert finalized.json() == {"status": "ok"}
    assert [row["id"] for row in client.get(PROJECT_ARTIFACTS, headers=owner_headers).json()] == [artifact["id"]]

    artifact = client.get(f"/api/v1/artifacts/{artifact['id']}", headers=owner_headers).json()
    assert artifact["storedSizeBytes"] == 9
    assert artifact["manifest"]["files"] == ["weights.bin", "dir/config.json"]
    assert [r["etag"] for r in artifact["manifest"]["references"]] == ["new"]

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"new").status_code == 409
    assert client.delete(file_base + "/weights.bin", headers=owner_headers).status_code == 409


def test_artifact_finalize_blocks_inflight_uploads(
    client: TestClient, owner_headers: Headers, create_run: CreateRun, engine: Engine,
) -> None:
    create_run(handle="owner", project_name="underfit", name="test-run")
    artifact = client.post(RUN_ARTIFACTS, headers=owner_headers, json={"name": "checkpoint", "type": "model"}).json()
    with engine.begin() as conn:
        conn.execute(artifacts.update().where(artifacts.c.id == UUID(artifact["id"])).values(active_uploads=1))
    assert client.post(
        f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json={"manifest": {"files": []}},
    ).status_code == 409


def test_artifact_failures_allow_retry(
    client: TestClient, owner_headers: Headers, create_run: CreateRun,
    storage: Storage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_run(handle="owner", project_name="underfit", name="test-run")
    artifact = client.post(RUN_ARTIFACTS, headers=owner_headers, json={"name": "checkpoint", "type": "model"}).json()
    file_url = f"/api/v1/artifacts/{artifact['id']}/files/weights.bin"

    async def fail_write_stream(key: str, stream: object) -> int:
        storage.write(key, b"partial")
        raise RuntimeError("boom")

    with monkeypatch.context() as m:
        m.setattr(storage, "write_stream", fail_write_stream)
        with pytest.raises(RuntimeError, match="boom"):
            client.put(file_url, headers=owner_headers, content=b"x")
    assert client.head(file_url, headers=owner_headers).status_code == 404
    assert client.put(file_url, headers=owner_headers, content=b"x").status_code == 200
    write = storage.write

    def fail_manifest_write(key: str, content: bytes) -> None:
        if key.endswith("/manifest.json"):
            raise RuntimeError("boom")
        write(key, content)

    finalize_body = {"manifest": {"files": ["weights.bin"]}}
    with monkeypatch.context() as m:
        m.setattr(storage, "write", fail_manifest_write)
        with pytest.raises(RuntimeError, match="boom"):
            client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=finalize_body)
    finalized = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=finalize_body)
    assert finalized.status_code == 200


def test_artifact_access_controls(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    add_collaborator: AddCollaborator, create_run: CreateRun,
) -> None:
    create_run(handle="owner", project_name="underfit", name="test-run")
    payload = {"name": "checkpoint", "type": "model"}
    assert client.post(PROJECT_ARTIFACTS, headers=outsider_headers, json=payload).status_code == 403
    artifact = client.post(RUN_ARTIFACTS, headers=owner_headers, json=payload).json()
    base = f"/api/v1/artifacts/{artifact['id']}"
    assert client.put(f"{base}/files/w.bin", headers=outsider_headers, content=b"x").status_code == 403
    assert client.delete(f"{base}/files/w.bin", headers=outsider_headers).status_code == 403
    finalize = client.post(f"{base}/finalize", headers=outsider_headers, json={"manifest": {"files": []}})
    assert finalize.status_code == 403

    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    created = client.post(PROJECT_ARTIFACTS, headers=outsider_headers, json={"name": "ckpt", "type": "model"})
    assert created.status_code == 200
    collaborator_artifact = created.json()
    collaborator_base = f"/api/v1/artifacts/{collaborator_artifact['id']}"
    assert client.put(f"{collaborator_base}/files/a.bin", headers=outsider_headers, content=b"a").status_code == 200
    finalized = client.post(
        f"{collaborator_base}/finalize", headers=outsider_headers, json={"manifest": {"files": ["a.bin"]}},
    )
    assert finalized.status_code == 200 and finalized.json() == {"status": "ok"}


@pytest.mark.parametrize(("path", "normalized"), [
    ("models/cafe\u0301 report (v1) [final]!.json", "models/caf\u00e9 report (v1) [final]!.json"),
    ("..\\weights.bin", None),
    ("\\etc\\passwd", None),
    ("dir/../file.txt", None),
    ("dir//file.txt", None),
    ("dir/ file.txt", None),
    ("dir/\nfile.txt", None),
])
def test_validate_artifact_path(path: str, normalized: str | None) -> None:
    if normalized:
        assert validate_path(path) == normalized
    else:
        with pytest.raises(HTTPException):
            validate_path(path)

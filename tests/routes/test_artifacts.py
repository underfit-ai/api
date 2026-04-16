from __future__ import annotations

from io import BytesIO
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import AddCollaborator, CreateRun, Headers
from underfit_api.helpers import validate_path
from underfit_api.schema import artifacts
from underfit_api.storage.types import Storage

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
    assert artifact["storedSizeBytes"] is None

    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights").status_code == 200

    head = client.head(file_base + "/weights.bin", headers=owner_headers)
    assert head.status_code == 200 and head.headers["content-length"] == "7" and "last-modified" in head.headers

    downloaded = client.get(file_base + "/weights.bin", headers=owner_headers)
    assert (downloaded.status_code, downloaded.content) == (200, b"weights")

    payload_3 = {"manifest": {"files": ["weights.bin", "dir/config.json"]}}
    missing = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload_3)
    assert missing.status_code == 409
    assert missing.json() == {"missing": ["dir/config.json"], "extra": []}

    assert client.put(file_base + "/dir/config.json", headers=owner_headers, content=b"{}").status_code == 200

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

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"new").status_code == 409
    assert client.delete(file_base + "/weights.bin", headers=owner_headers).status_code == 409


def test_artifact_finalize(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    create_run(handle="owner", project_name="underfit", name="test-run")
    artifact = client.post(RUN_ARTIFACTS, headers=owner_headers, json={"name": "checkpoint", "type": "model"}).json()
    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights").status_code == 200
    assert client.put(file_base + "/extra.bin", headers=owner_headers, content=b"x").status_code == 200

    payload = {"manifest": {"files": ["weights.bin"]}}
    extra = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload)
    assert extra.status_code == 409 and extra.json() == {"missing": [], "extra": ["extra.bin"]}

    assert client.delete(file_base + "/extra.bin", headers=owner_headers).status_code == 200
    assert client.head(file_base + "/extra.bin", headers=owner_headers).status_code == 404

    finalized = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload)
    assert finalized.status_code == 200


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
    forbidden_payload = {"name": "ckpt", "type": "model"}
    assert client.post(PROJECT_ARTIFACTS, headers=outsider_headers, json=forbidden_payload).status_code == 403

    artifact_payload = {"step": 1, "name": "checkpoint", "type": "model"}
    artifact = client.post(RUN_ARTIFACTS, headers=owner_headers, json=artifact_payload).json()
    base = f"/api/v1/artifacts/{artifact['id']}"
    assert client.put(f"{base}/files/weights.bin", headers=outsider_headers, content=b"x").status_code == 403
    assert client.delete(f"{base}/files/weights.bin", headers=outsider_headers).status_code == 403
    empty_files = {"manifest": {"files": []}}
    assert client.post(f"{base}/finalize", headers=outsider_headers, json=empty_files).status_code == 403

    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")

    created = client.post(PROJECT_ARTIFACTS, headers=outsider_headers, json={"name": "ckpt", "type": "model"})
    assert created.status_code == 200
    collaborator_artifact = created.json()
    collaborator_base = f"/api/v1/artifacts/{collaborator_artifact['id']}"
    assert client.put(f"{collaborator_base}/files/a.bin", headers=outsider_headers, content=b"a").status_code == 200
    manifest_payload = {"manifest": {"files": ["a.bin"]}}
    finalized = client.post(f"{collaborator_base}/finalize", headers=outsider_headers, json=manifest_payload)
    assert finalized.status_code == 200 and finalized.json() == {"status": "ok"}


def test_artifact_zip_browse(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    create_run(handle="owner", project_name="underfit", name="test-run")
    artifact = client.post(RUN_ARTIFACTS, headers=owner_headers, json={"name": "source-code", "type": "code"}).json()
    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("main.py", b"print('hi')\n")
        archive.writestr("pkg/util.py", b"x = 1\n")
    assert client.put(file_base + "/code.zip", headers=owner_headers, content=buffer.getvalue()).status_code == 200
    assert client.put(file_base + "/notes.txt", headers=owner_headers, content=b"not a zip").status_code == 200

    zip_base = f"/api/v1/artifacts/{artifact['id']}/zip"
    entries = client.get(f"{zip_base}/entries/code.zip", headers=owner_headers)
    assert entries.status_code == 200
    assert [(e["path"], e["size"]) for e in entries.json()] == [("main.py", 12), ("pkg/util.py", 6)]

    content = client.get(f"{zip_base}/entry/code.zip", headers=owner_headers, params={"entry": "pkg/util.py"})
    assert (content.status_code, content.content) == (200, b"x = 1\n")

    missing = client.get(f"{zip_base}/entry/code.zip", headers=owner_headers, params={"entry": "nope.py"})
    assert missing.status_code == 404

    assert client.get(f"{zip_base}/entries/notes.txt", headers=owner_headers).status_code == 400
    assert client.get(f"{zip_base}/entries/absent.zip", headers=owner_headers).status_code == 404


@pytest.mark.parametrize(("path", "normalized"), [
    ("models/cafe\u0301 report (v1) [final]!.json", "models/caf\u00e9 report (v1) [final]!.json"),
    (".hidden/ok.txt", ".hidden/ok.txt"),
    ("..\\weights.bin", None),
    ("\\etc\\passwd", None),
    ("dir//file.txt", None),
    ("dir/./file.txt", None),
    ("dir/../file.txt", None),
    ("dir /file.txt", None),
    ("dir/ file.txt", None),
    ("dir/file.txt ", None),
    ("dir/file.txt.", None),
    ("dir/\tfile.txt", None),
    ("dir/\nfile.txt", None),
])
def test_validate_artifact_path(path: str, normalized: str | None) -> None:
    if normalized:
        assert validate_path(path) == normalized
    else:
        with pytest.raises(HTTPException):
            validate_path(path)

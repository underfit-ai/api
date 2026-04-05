from __future__ import annotations

import json
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import underfit_api.storage as storage_mod
from tests.conftest import AddCollaborator, CreateRun, Headers
from underfit_api.routes.artifacts import _validate_path

ARTIFACTS = "/api/v1/accounts/owner/projects/underfit/artifacts"


def test_artifact_upload(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    payload_1 = {"step": 3, "name": "artifact", "type": "dataset"}
    assert client.post(ARTIFACTS, headers=owner_headers, json=payload_1).status_code == 400

    payload_2 = {"run_id": str(run.id), "step": 10, "name": "checkpoint", "type": "model", "metadata": {"tag": "best"}}
    created = client.post(ARTIFACTS, headers=owner_headers, json=payload_2)
    assert created.status_code == 200
    artifact = created.json()
    assert artifact["storedSizeBytes"] is None
    assert not storage_mod.storage.exists(f"{artifact['storageKey']}/manifest.json")
    assert not storage_mod.storage.exists(f"{artifact['storageKey']}/artifact.json")

    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights").status_code == 200

    head_1 = client.head(file_base + "/weights.bin", headers=owner_headers)
    assert head_1.status_code == 200
    assert head_1.headers["content-length"] == "7"
    assert "last-modified" in head_1.headers

    downloaded_1 = client.get(file_base + "/weights.bin", headers=owner_headers)
    assert (downloaded_1.status_code, downloaded_1.content) == (200, b"weights")

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

    artifact = client.get(f"/api/v1/artifacts/{artifact['id']}", headers=owner_headers).json()
    assert artifact["storedSizeBytes"] == 9

    manifest = json.loads(storage_mod.storage.read(f"{artifact['storageKey']}/manifest.json"))
    assert manifest == {
        "files": ["weights.bin", "dir/config.json"],
        "references": [
            {"url": "https://example.com/a", "size": None, "sha256": "abc", "etag": "new", "last_modified": None},
        ],
    }

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"new").status_code == 409
    assert client.delete(file_base + "/weights.bin", headers=owner_headers).status_code == 409


def test_artifact_finalize(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    payload_1 = {"run_id": str(run.id), "name": "checkpoint", "type": "model"}
    artifact = client.post(ARTIFACTS, headers=owner_headers, json=payload_1).json()
    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    assert client.put(file_base + "/weights.bin", headers=owner_headers, content=b"weights").status_code == 200
    assert client.put(file_base + "/extra.bin", headers=owner_headers, content=b"x").status_code == 200

    payload_2 = {"manifest": {"files": ["weights.bin"]}}
    extra = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload_2)
    assert extra.status_code == 409
    assert extra.json() == {"missing": [], "extra": ["extra.bin"]}

    assert client.delete(file_base + "/extra.bin", headers=owner_headers).status_code == 200
    assert client.head(file_base + "/extra.bin", headers=owner_headers).status_code == 404

    payload_3 = {"manifest": {"files": ["weights.bin"]}}
    finalized = client.post(f"/api/v1/artifacts/{artifact['id']}/finalize", headers=owner_headers, json=payload_3)
    assert finalized.status_code == 200


def test_artifact_access_controls(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    add_collaborator: AddCollaborator, create_run: CreateRun,
) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    forbidden_payload = {"name": "ckpt", "type": "model"}
    assert client.post(ARTIFACTS, headers=outsider_headers, json=forbidden_payload).status_code == 403

    artifact_payload = {"run_id": str(run.id), "step": 1, "name": "checkpoint", "type": "model"}
    artifact = client.post(ARTIFACTS, headers=owner_headers, json=artifact_payload).json()
    base = f"/api/v1/artifacts/{artifact['id']}"
    assert client.put(f"{base}/files/weights.bin", headers=outsider_headers, content=b"x").status_code == 403
    assert client.delete(f"{base}/files/weights.bin", headers=outsider_headers).status_code == 403
    empty_files = {"manifest": {"files": []}}
    assert client.post(f"{base}/finalize", headers=outsider_headers, json=empty_files).status_code == 403

    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")

    allowed_payload = {"name": "ckpt", "type": "model"}
    created = client.post(ARTIFACTS, headers=outsider_headers, json=allowed_payload)
    assert created.status_code == 200
    collaborator_artifact = created.json()
    collaborator_base = f"/api/v1/artifacts/{collaborator_artifact['id']}"
    assert client.put(f"{collaborator_base}/files/a.bin", headers=outsider_headers, content=b"a").status_code == 200
    manifest_payload = {"manifest": {"files": ["a.bin"]}}
    finalized = client.post(f"{collaborator_base}/finalize", headers=outsider_headers, json=manifest_payload)
    assert finalized.status_code == 200
    assert finalized.json() == {"status": "ok"}


def test_artifact_zip_browse(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    artifact_payload = {"run_id": str(run.id), "name": "source-code", "type": "code"}
    artifact = client.post(ARTIFACTS, headers=owner_headers, json=artifact_payload).json()
    file_base = f"/api/v1/artifacts/{artifact['id']}/files"

    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("main.py", b"print('hi')\n")
        archive.writestr("pkg/util.py", b"x = 1\n")
    assert client.put(file_base + "/code.zip", headers=owner_headers, content=buffer.getvalue()).status_code == 200
    assert client.put(file_base + "/notes.txt", headers=owner_headers, content=b"not a zip").status_code == 200

    zip_base = f"/api/v1/artifacts/{artifact['id']}/zip"
    entries = client.get(f"{zip_base}/code.zip", headers=owner_headers)
    assert entries.status_code == 200
    assert entries.json() == [
        {"path": "main.py", "size": 12, "compressedSize": entries.json()[0]["compressedSize"]},
        {"path": "pkg/util.py", "size": 6, "compressedSize": entries.json()[1]["compressedSize"]},
    ]

    content = client.get(f"{zip_base}/code.zip", headers=owner_headers, params={"entry": "pkg/util.py"})
    assert (content.status_code, content.content) == (200, b"x = 1\n")

    missing = client.get(f"{zip_base}/code.zip", headers=owner_headers, params={"entry": "nope.py"})
    assert missing.status_code == 404

    assert client.get(f"{zip_base}/notes.txt", headers=owner_headers).status_code == 400
    assert client.get(f"{zip_base}/absent.zip", headers=owner_headers).status_code == 404


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
        assert _validate_path(path) == normalized
    else:
        with pytest.raises(HTTPException):
            _validate_path(path)

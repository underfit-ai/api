from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import CreateRun, Headers
from underfit_api.schema import runs
from underfit_api.storage.types import Storage


def test_list_and_download_run_files(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_run: CreateRun,
    engine: Engine, storage: Storage,
) -> None:
    run = create_run(handle="owner", project_name="underfit")
    files_url = f"/api/v1/accounts/owner/projects/underfit/runs/{run.name}/files"
    download_url = f"{files_url}/download"

    with engine.begin() as conn:
        conn.execute(runs.update().where(runs.c.id == run.id).values(storage_key="run-files"))
    storage.write("run-files/metrics.json", b"{\"loss\": 0.1}")
    storage.write("run-files/checkpoints/model.bin", b"model-bytes")

    list_root = client.get(files_url, headers=owner_headers)
    assert list_root.status_code == 200
    assert [entry["name"] for entry in list_root.json()] == ["checkpoints", "metrics.json"]
    assert client.get(files_url).status_code == 401
    assert client.get(
        download_url, headers=outsider_headers, params={"path": "checkpoints/model.bin"},
    ).status_code == 403

    list_subdir = client.get(files_url, headers=owner_headers, params={"path": "checkpoints"})
    assert list_subdir.status_code == 200
    assert list_subdir.json()[0]["name"] == "model.bin"

    downloaded = client.get(download_url, headers=owner_headers, params={"path": "checkpoints/model.bin"})
    assert downloaded.status_code == 200
    assert downloaded.content == b"model-bytes"
    assert downloaded.headers["content-disposition"] == "attachment; filename*=UTF-8''model.bin"
    assert client.get(files_url, headers=owner_headers, params={"path": "../other-run"}).status_code == 400
    assert client.get(download_url, headers=owner_headers, params={"path": "../other-run/file.bin"}).status_code == 400
    assert client.get(download_url, headers=owner_headers, params={"path": "missing.bin"}).status_code == 404

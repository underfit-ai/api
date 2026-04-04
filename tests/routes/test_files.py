from __future__ import annotations

from fastapi.testclient import TestClient

import underfit_api.storage as storage_mod
from tests.conftest import CreateRun, Headers


def test_list_and_download_run_files(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    files_url = f"/api/v1/accounts/owner/projects/underfit/runs/{run.name}/files"
    download_url = f"{files_url}/download"

    storage_mod.storage.write(f"{run.id}/metrics.json", b"{\"loss\": 0.1}")
    storage_mod.storage.write(f"{run.id}/checkpoints/model.bin", b"model-bytes")

    list_root = client.get(files_url, headers=owner_headers)
    assert list_root.status_code == 200
    assert [entry["name"] for entry in list_root.json()] == ["checkpoints", "metrics.json"]

    list_subdir = client.get(files_url, headers=owner_headers, params={"path": "checkpoints"})
    assert list_subdir.status_code == 200
    assert list_subdir.json()[0]["name"] == "model.bin"

    downloaded = client.get(download_url, headers=owner_headers, params={"path": "checkpoints/model.bin"})
    assert downloaded.status_code == 200
    assert downloaded.content == b"model-bytes"
    assert downloaded.headers["content-disposition"] == 'attachment; filename="model.bin"'
    assert client.get(download_url, headers=owner_headers, params={"path": "missing.bin"}).status_code == 404

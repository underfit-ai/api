from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateRun, OwnerHeaders
from underfit_api.storage import get_storage


def test_list_and_download_run_files(client: TestClient, owner_headers: OwnerHeaders, create_run: CreateRun) -> None:
    run = create_run(owner_headers)

    storage = get_storage()
    storage.write(f"{run['id']}/metrics.json", b"{\"loss\": 0.1}")
    storage.write(f"{run['id']}/checkpoints/model.bin", b"model-bytes")

    list_root = client.get(f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/files", headers=owner_headers)
    assert list_root.status_code == 200
    assert [entry["name"] for entry in list_root.json()] == ["checkpoints", "metrics.json"]

    list_subdir = client.get(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/files",
        headers=owner_headers,
        params={"path": "checkpoints"},
    )
    assert list_subdir.status_code == 200
    assert list_subdir.json()[0]["name"] == "model.bin"

    downloaded = client.get(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/files/download",
        headers=owner_headers,
        params={"path": "checkpoints/model.bin"},
    )
    assert downloaded.status_code == 200
    assert downloaded.content == b"model-bytes"
    assert downloaded.headers["content-disposition"] == 'attachment; filename="model.bin"'


def test_download_missing_file_returns_404(
    client: TestClient, owner_headers: OwnerHeaders, create_run: CreateRun,
) -> None:
    run = create_run(owner_headers)

    missing = client.get(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}/files/download",
        headers=owner_headers,
        params={"path": "missing.bin"},
    )
    assert missing.status_code == 404

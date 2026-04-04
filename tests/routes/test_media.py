from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, Headers

MEDIA_METADATA = json.dumps({"key": "predictions", "step": 10, "type": "image"})
MEDIA_FILES = [("files", ("a.bin", b"file-a", "application/octet-stream"))]


def test_media_create_list_filter_and_download(client: TestClient, media_setup: tuple[Headers, str]) -> None:
    headers, media_url = media_setup

    created = client.post(media_url, headers=headers, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    assert created.status_code == 200
    media_id = created.json()["id"]

    listed = client.get(media_url, headers=headers, params={"key": "predictions", "step": 10})
    assert listed.status_code == 200
    assert len(listed.json()) == 1
    assert listed.json()[0]["id"] == media_id

    downloaded = client.get(f"{media_url}/{media_id}/file", headers=headers)
    assert downloaded.status_code == 200
    assert downloaded.content == b"file-a"


@pytest.mark.parametrize(("metadata", "files", "expected_status"), [
    ("not-json", MEDIA_FILES, 400),
    (json.dumps({"key": "predictions", "type": "table"}), MEDIA_FILES, 400),
    (json.dumps({"key": "predictions", "type": "image"}), [], 400),
])
def test_media_validation_errors(
    client: TestClient, media_setup: tuple[Headers, str], metadata: str,
    files: list[tuple[str, tuple[str, bytes, str]]], expected_status: int,
) -> None:
    headers, media_url = media_setup
    response = client.post(media_url, headers=headers, data={"metadata": metadata}, files=files)
    assert response.status_code == expected_status


def test_media_requires_project_access(
    client: TestClient, media_setup: tuple[Headers, str], outsider_headers: Headers, add_collaborator: AddCollaborator,
) -> None:
    _, media_url = media_setup
    response = client.post(media_url, headers=outsider_headers, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    assert response.status_code == 403
    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    response = client.post(media_url, headers=outsider_headers, data={"metadata": MEDIA_METADATA}, files=MEDIA_FILES)
    assert response.status_code == 200

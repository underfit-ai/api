from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, OutsiderHeaders, OwnerHeaders


def test_media_create_list_filter_and_download(
    client: TestClient, media_setup: tuple[OwnerHeaders, str],
) -> None:
    headers, media_url = media_setup

    created = client.post(
        media_url,
        headers=headers,
        data={"metadata": json.dumps({"key": "predictions", "step": 10, "type": "image"})},
        files=[("files", ("a.bin", b"file-a", "application/octet-stream"))],
    )
    assert created.status_code == 200
    media_id = created.json()["id"]

    listed = client.get(media_url, headers=headers, params={"key": "predictions", "step": 10})
    assert listed.status_code == 200
    assert len(listed.json()) == 1
    assert listed.json()[0]["id"] == media_id

    downloaded = client.get(f"{media_url}/{media_id}/file", headers=headers)
    assert downloaded.status_code == 200
    assert downloaded.content == b"file-a"


@pytest.mark.parametrize(
    ("metadata", "files", "expected_status"),
    [
        ("not-json", [("files", ("a.bin", b"file-a", "application/octet-stream"))], 400),
        (
            json.dumps({"key": "predictions", "type": "table"}),
            [("files", ("a.bin", b"file-a", "application/octet-stream"))],
            400,
        ),
        (json.dumps({"key": "predictions", "type": "image"}), [], 422),
    ],
)
def test_media_validation_errors(
    client: TestClient,
    media_setup: tuple[OwnerHeaders, str],
    metadata: str,
    files: list[tuple[str, tuple[str, bytes, str]]],
    expected_status: int,
) -> None:
    headers, media_url = media_setup
    response = client.post(media_url, headers=headers, data={"metadata": metadata}, files=files)
    assert response.status_code == expected_status


def test_media_create_requires_project_access(
    client: TestClient,
    media_setup: tuple[OwnerHeaders, str],
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
) -> None:
    owner_headers, media_url = media_setup

    forbidden = client.post(
        media_url,
        headers=outsider_headers,
        data={"metadata": json.dumps({"key": "predictions", "type": "image"})},
        files=[("files", ("a.bin", b"file-a", "application/octet-stream"))],
    )
    assert forbidden.status_code == 403

    add_collaborator(owner_headers)

    allowed = client.post(
        media_url,
        headers=outsider_headers,
        data={"metadata": json.dumps({"key": "predictions", "type": "image"})},
        files=[("files", ("a.bin", b"file-a", "application/octet-stream"))],
    )
    assert allowed.status_code == 200

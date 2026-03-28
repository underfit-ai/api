from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, OutsiderHeaders, OwnerHeaders


def test_write_read_and_flush_logs(
    client: TestClient, logs_setup: tuple[OwnerHeaders, str],
) -> None:
    headers, logs_url = logs_setup

    first = client.post(
        logs_url,
        headers=headers,
        json={
            "worker_id": "worker-1",
            "start_line": 0,
            "lines": [
                {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"},
                {"timestamp": "2025-01-01T00:00:01+00:00", "content": "world"},
            ],
        },
    )
    assert first.status_code == 200

    second = client.post(
        logs_url,
        headers=headers,
        json={
            "worker_id": "worker-1",
            "start_line": 2,
            "lines": [{"timestamp": "2025-01-01T00:00:02+00:00", "content": "tail"}],
        },
    )
    assert second.status_code == 200

    first_page = client.get(logs_url, headers=headers, params={"workerId": "worker-1", "count": 2})
    assert first_page.status_code == 200
    assert first_page.json()["entries"][0]["content"] == "hello\nworld"
    assert first_page.json()["nextCursor"] == 2
    assert first_page.json()["hasMore"] is True

    flush = client.post(f"{logs_url}/flush", headers=headers, json={"worker_id": "worker-1"})
    assert flush.status_code == 200

    second_page = client.get(logs_url, headers=headers, params={"workerId": "worker-1", "cursor": 2})
    assert second_page.status_code == 200
    assert second_page.json()["entries"][0]["content"] == "tail"


def test_logs_reject_out_of_order_start_line(
    client: TestClient, logs_setup: tuple[OwnerHeaders, str],
) -> None:
    headers, logs_url = logs_setup

    first = client.post(
        logs_url,
        headers=headers,
        json={
            "worker_id": "worker-1",
            "start_line": 0,
            "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"}],
        },
    )
    assert first.status_code == 200

    duplicate = client.post(
        logs_url,
        headers=headers,
        json={
            "worker_id": "worker-1",
            "start_line": 0,
            "lines": [{"timestamp": "2025-01-01T00:00:01+00:00", "content": "world"}],
        },
    )
    assert duplicate.status_code == 409


def test_logs_require_project_access(
    client: TestClient,
    logs_setup: tuple[OwnerHeaders, str],
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
) -> None:
    owner_headers, logs_url = logs_setup

    forbidden = client.post(
        logs_url,
        headers=outsider_headers,
        json={"worker_id": "w", "start_line": 0, "lines": []},
    )
    assert forbidden.status_code == 403

    add_collaborator(owner_headers)

    allowed = client.post(
        logs_url,
        headers=outsider_headers,
        json={
            "worker_id": "w",
            "start_line": 0,
            "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "hi"}],
        },
    )
    assert allowed.status_code == 200

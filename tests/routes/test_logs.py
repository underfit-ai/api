from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, Headers


def _add_worker(client: TestClient, logs_url: str, headers: dict[str, str], worker_label: str) -> None:
    workers_url = logs_url.rsplit("/", 1)[0] + "/workers"
    assert client.post(workers_url, headers=headers, json={"workerLabel": worker_label}).status_code == 200


def test_write_read_and_flush_logs(client: TestClient, logs_setup: tuple[Headers, str]) -> None:
    headers, logs_url = logs_setup
    _add_worker(client, logs_url, headers, "worker-1")
    lines = [
        {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"},
        {"timestamp": "2025-01-01T00:00:01+00:00", "content": "world"},
    ]
    tail = {"timestamp": "2025-01-01T00:00:02+00:00", "content": "tail"}
    base = {"worker_label": "worker-1"}

    assert client.post(logs_url, headers=headers, json={**base, "start_line": 0, "lines": lines}).status_code == 200
    assert client.post(logs_url, headers=headers, json={**base, "start_line": 2, "lines": [tail]}).status_code == 200

    first_page = client.get(logs_url, headers=headers, params={"workerLabel": "worker-1", "count": 2})
    first_json = first_page.json()
    assert first_page.status_code == 200
    assert first_json["entries"][0]["content"] == "hello\nworld"
    assert first_json["nextCursor"] == 2 and first_json["hasMore"] is True

    assert client.post(f"{logs_url}/flush", headers=headers, json=base).status_code == 200

    second_page = client.get(logs_url, headers=headers, params={"workerLabel": "worker-1", "cursor": 2})
    second_json = second_page.json()
    assert second_page.status_code == 200 and second_json["entries"][0]["content"] == "tail"


def test_logs_reject_out_of_order_start_line(client: TestClient, logs_setup: tuple[Headers, str]) -> None:
    headers, logs_url = logs_setup
    _add_worker(client, logs_url, headers, "worker-1")
    hello_line = {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"}
    payload = {"worker_label": "worker-1", "start_line": 0, "lines": [hello_line]}

    assert client.post(logs_url, headers=headers, json=payload).status_code == 200
    assert client.post(logs_url, headers=headers, json=payload).status_code == 409


def test_logs_reject_unregistered_worker(client: TestClient, logs_setup: tuple[Headers, str]) -> None:
    headers, logs_url = logs_setup
    payload = {
        "worker_label": "unknown", "start_line": 0,
        "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "hi"}],
    }
    assert client.post(logs_url, headers=headers, json=payload).status_code == 404


def test_logs_require_project_access(
    client: TestClient, logs_setup: tuple[Headers, str], outsider_headers: Headers, add_collaborator: AddCollaborator,
) -> None:
    owner_headers, logs_url = logs_setup
    _add_worker(client, logs_url, owner_headers, "w")
    hi_line = {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hi"}
    payload = {"worker_label": "w", "start_line": 0, "lines": [hi_line]}

    assert client.post(logs_url, headers=outsider_headers, json=payload).status_code == 403
    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    assert client.post(logs_url, headers=outsider_headers, json=payload).status_code == 200

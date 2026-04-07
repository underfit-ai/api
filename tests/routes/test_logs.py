from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import Headers

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"


def _setup_logs(client: TestClient, headers: Headers) -> tuple[Headers, str]:
    client.post("/api/v1/accounts/owner/projects", headers=headers, json={"name": "underfit", "visibility": "private"})
    run = client.post(LAUNCH, headers=headers, json={"runName": "r", "launchId": "1"}).json()
    return {"Authorization": f"Bearer {run['workerToken']}"}, "/api/v1/accounts/owner/projects/underfit/runs/r/logs"


def _add_worker(client: TestClient, headers: Headers, worker_label: str) -> Headers:
    worker = client.post(LAUNCH, headers=headers, json={"runName": "r", "launchId": "1", "workerLabel": worker_label})
    return {"Authorization": f"Bearer {worker.json()['workerToken']}"}


def test_write_read_logs_from_buffer(client: TestClient, owner_headers: Headers) -> None:
    _, logs_url = _setup_logs(client, owner_headers)
    worker_headers = _add_worker(client, owner_headers, "worker-1")
    lines = [
        {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"},
        {"timestamp": "2025-01-01T00:00:01+00:00", "content": "world"},
    ]
    tail = {"timestamp": "2025-01-01T00:00:02+00:00", "content": "tail"}

    logs_1 = {"start_line": 0, "lines": lines}
    logs_2 = {"start_line": 2, "lines": [tail]}
    assert client.post("/api/v1/ingest/logs", headers=worker_headers, json=logs_1).json()["nextStartLine"] == 2
    assert client.post("/api/v1/ingest/logs", headers=worker_headers, json=logs_2).json()["nextStartLine"] == 3

    first_page = client.get(logs_url, headers=owner_headers, params={"workerLabel": "worker-1", "count": 2})
    first_json = first_page.json()
    assert first_page.status_code == 200
    assert first_json["entries"][0]["content"] == "hello\nworld"
    assert first_json["nextCursor"] == 2 and first_json["hasMore"] is True

    second_page = client.get(logs_url, headers=owner_headers, params={"workerLabel": "worker-1", "cursor": 2})
    second_json = second_page.json()
    assert second_page.status_code == 200 and second_json["entries"][0]["content"] == "tail"


def test_logs_reject_out_of_order_start_line(client: TestClient, owner_headers: Headers) -> None:
    _, logs_url = _setup_logs(client, owner_headers)
    worker_headers = _add_worker(client, owner_headers, "worker-1")
    hello_line = {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"}
    payload = {"start_line": 0, "lines": [hello_line]}

    assert client.post("/api/v1/ingest/logs", headers=worker_headers, json=payload).status_code == 200
    assert client.post("/api/v1/ingest/logs", headers=worker_headers, json=payload).status_code == 409


def test_logs_require_worker_token(client: TestClient) -> None:
    payload = {"start_line": 0, "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "hi"}]}
    assert client.post("/api/v1/ingest/logs", json=payload).status_code == 401


def test_logs_reject_newlines(client: TestClient, owner_headers: Headers) -> None:
    headers, _ = _setup_logs(client, owner_headers)
    payload = {"start_line": 0, "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "a\nb"}]}
    assert client.post("/api/v1/ingest/logs", headers=headers, json=payload).status_code == 400

from __future__ import annotations

from fastapi.testclient import TestClient

import underfit_api.db as db
import underfit_api.storage as storage_mod
from tests.conftest import Headers
from underfit_api.buffer import log_buffer

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

    first_page = client.get(f"{logs_url}/worker-1", headers=owner_headers, params={"count": 2})
    first_json = first_page.json()
    assert first_page.status_code == 200
    assert first_json["entries"][0]["content"] == "hello\nworld"
    assert first_json["nextCursor"] == 2 and first_json["hasMore"] is True

    second_page = client.get(f"{logs_url}/worker-1", headers=owner_headers, params={"cursor": 2})
    second_json = second_page.json()
    assert second_page.status_code == 200 and second_json["entries"][0]["content"] == "tail"


def test_log_ingest_validation(client: TestClient, owner_headers: Headers) -> None:
    headers, _ = _setup_logs(client, owner_headers)
    worker_headers = _add_worker(client, owner_headers, "worker-1")
    hello_line = {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"}
    payload = {"start_line": 0, "lines": [hello_line]}
    newline_payload = {"start_line": 0, "lines": [{"timestamp": hello_line["timestamp"], "content": "a\nb"}]}

    assert client.post("/api/v1/ingest/logs", json=payload).status_code == 401
    assert client.post("/api/v1/ingest/logs", headers=headers, json=newline_payload).status_code == 400
    assert client.post("/api/v1/ingest/logs", headers=worker_headers, json=payload).status_code == 200
    assert client.post("/api/v1/ingest/logs", headers=worker_headers, json=payload).status_code == 409


def test_read_logs_from_persisted_segments(client: TestClient, owner_headers: Headers) -> None:
    headers, logs_url = _setup_logs(client, owner_headers)
    lines = [
        {"timestamp": f"2025-01-01T00:00:0{i}+00:00", "content": content} for i, content in enumerate(["a", "b", "c"])
    ]
    assert client.post(
        "/api/v1/ingest/logs", headers=headers, json={"start_line": 0, "lines": lines},
    ).status_code == 200
    with db.engine.begin() as conn:
        log_buffer.flush_all(conn, storage_mod.storage)
    page = client.get(f"{logs_url}/0", headers=owner_headers, params={"cursor": 1, "count": 1})
    assert page.status_code == 200
    assert page.json()["entries"][0]["content"] == "b"
    assert (page.json()["nextCursor"], page.json()["hasMore"]) == (2, True)

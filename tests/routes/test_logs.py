from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import Headers
from underfit_api.buffer import log_buffer
from underfit_api.storage.types import Storage

INGEST = "/api/v1/ingest/logs"


def test_write_read_logs_from_buffer(client: TestClient, owner_headers: Headers, worker_headers: Headers) -> None:
    logs_url = "/api/v1/accounts/owner/projects/underfit/runs/r/logs/0"
    lines = [
        {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"},
        {"timestamp": "2025-01-01T00:00:01+00:00", "content": "world"},
    ]

    logs_1 = {"start_line": 0, "lines": lines}
    logs_2 = {"start_line": 2, "lines": [{"timestamp": "2025-01-01T00:00:02+00:00", "content": "tail"}]}
    assert client.post(INGEST, headers=worker_headers, json=logs_1).json()["nextStartLine"] == 2
    assert client.post(INGEST, headers=worker_headers, json=logs_2).json()["nextStartLine"] == 3

    first_page = client.get(logs_url, headers=owner_headers, params={"count": 2})
    first_json = first_page.json()
    assert first_page.status_code == 200
    assert first_json["entries"][0]["content"] == "hello\nworld"
    assert first_json["nextCursor"] == 2 and first_json["hasMore"] is True

    second_page = client.get(logs_url, headers=owner_headers, params={"cursor": 2})
    second_json = second_page.json()
    assert second_page.status_code == 200 and second_json["entries"][0]["content"] == "tail"


def test_log_ingest_validation(client: TestClient, owner_headers: Headers, worker_headers: Headers) -> None:
    payload = {"start_line": 0, "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"}]}
    newline_payload = {"start_line": 0, "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "a\nb"}]}

    assert client.post(INGEST, json=payload).status_code == 401
    assert client.post(INGEST, headers=worker_headers, json=newline_payload).status_code == 400
    assert client.post(INGEST, headers=worker_headers, json=payload).status_code == 200
    assert client.post(INGEST, headers=worker_headers, json=payload).status_code == 409


def test_read_logs_from_storage(
    client: TestClient, owner_headers: Headers, worker_headers: Headers, engine: Engine, storage: Storage,
) -> None:
    logs_url = "/api/v1/accounts/owner/projects/underfit/runs/r/logs/0"
    lines = [
        {"timestamp": f"2025-01-01T00:00:0{i}+00:00", "content": content} for i, content in enumerate(["a", "b", "c"])
    ]
    assert client.post(INGEST, headers=worker_headers, json={"start_line": 0, "lines": lines}).status_code == 200
    with engine.begin() as conn:
        log_buffer.flush_all(conn, storage)
    page = client.get(logs_url, headers=owner_headers, params={"cursor": 1, "count": 1})
    assert page.status_code == 200
    assert page.json()["entries"][0]["content"] == "b"
    assert (page.json()["nextCursor"], page.json()["hasMore"]) == (2, True)

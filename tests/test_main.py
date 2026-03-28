from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_returns_status_and_version(client: TestClient) -> None:
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "v1"}


def test_unknown_route_returns_json_404(client: TestClient) -> None:
    response = client.get("/api/v1/missing")

    assert response.status_code == 404
    assert "application/json" in response.headers["content-type"]
    assert response.json() == {"error": "Route not found"}

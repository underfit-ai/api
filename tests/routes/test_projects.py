from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import OutsiderHeaders, OwnerHeaders


def test_project_crud_and_listing(client: TestClient, owner_headers: OwnerHeaders) -> None:
    created = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "Underfit", "description": "Tracking runs", "visibility": "private"},
    )
    assert created.status_code == 200
    assert created.json()["name"] == "underfit"

    fetched = client.get("/api/v1/accounts/owner/projects/UNDERFIT", headers=owner_headers)
    assert fetched.status_code == 200
    assert fetched.json()["description"] == "Tracking runs"

    listed = client.get("/api/v1/accounts/owner/projects", headers=owner_headers)
    assert listed.status_code == 200
    assert [p["name"] for p in listed.json()] == ["underfit"]

    updated = client.put(
        "/api/v1/accounts/owner/projects/underfit",
        headers=owner_headers,
        json={"description": "Updated", "visibility": "public"},
    )
    assert updated.status_code == 200
    assert updated.json()["description"] == "Updated"
    assert updated.json()["visibility"] == "public"


def test_project_creation_validation(client: TestClient, owner_headers: OwnerHeaders) -> None:
    missing_account = client.post(
        "/api/v1/accounts/missing/projects",
        headers=owner_headers,
        json={"name": "underfit", "visibility": "private"},
    )
    assert missing_account.status_code == 404

    bad_visibility = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "underfit", "visibility": "internal"},
    )
    assert bad_visibility.status_code == 400

    created = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "underfit", "visibility": "private"},
    )
    assert created.status_code == 200

    duplicate = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "UNDERFIT", "visibility": "private"},
    )
    assert duplicate.status_code == 409


def test_project_update_requires_admin(
    client: TestClient, owner_headers: OwnerHeaders, outsider_headers: OutsiderHeaders,
) -> None:
    created = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "underfit", "description": "tracking", "visibility": "private"},
    )
    assert created.status_code == 200

    forbidden = client.put(
        "/api/v1/accounts/owner/projects/underfit",
        headers=outsider_headers,
        json={"description": "hacked"},
    )
    assert forbidden.status_code == 403


def test_project_create_requires_account_admin(
    client: TestClient, owner_headers: OwnerHeaders, outsider_headers: OutsiderHeaders,
) -> None:
    forbidden = client.post(
        "/api/v1/accounts/owner/projects",
        headers=outsider_headers,
        json={"name": "proj", "description": "x", "visibility": "private"},
    )
    assert forbidden.status_code == 403

    allowed = client.post(
        "/api/v1/accounts/owner/projects",
        headers=owner_headers,
        json={"name": "proj", "description": "x", "visibility": "private"},
    )
    assert allowed.status_code == 200

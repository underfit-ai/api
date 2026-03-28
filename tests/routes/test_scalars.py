from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, OutsiderHeaders, OwnerHeaders


def test_write_and_read_scalars_with_auto_resolution(
    client: TestClient, scalars_setup: tuple[OwnerHeaders, str],
) -> None:
    headers, scalars_url = scalars_setup

    points = [
        {
            "step": i,
            "values": {"loss": round(1.0 - i * 0.01, 4)},
            "timestamp": f"2025-01-01T00:00:{i:02d}+00:00",
        }
        for i in range(20)
    ]
    written = client.post(scalars_url, headers=headers, json={"start_line": 0, "scalars": points})
    assert written.status_code == 200

    full = client.get(scalars_url, headers=headers)
    assert full.status_code == 200
    assert len(full.json()) == 20

    reduced = client.get(scalars_url, headers=headers, params={"maxPoints": 2})
    assert reduced.status_code == 200
    assert len(reduced.json()) == 2


def test_scalars_validate_cursor_inputs(
    client: TestClient, scalars_setup: tuple[OwnerHeaders, str],
) -> None:
    headers, scalars_url = scalars_setup

    first = client.post(
        scalars_url,
        headers=headers,
        json={
            "start_line": 0,
            "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
        },
    )
    assert first.status_code == 200

    duplicate = client.post(
        scalars_url,
        headers=headers,
        json={
            "start_line": 0,
            "scalars": [{"step": 2, "values": {"loss": 0.2}, "timestamp": "2025-01-01T00:00:01+00:00"}],
        },
    )
    assert duplicate.status_code == 409

    invalid_query = client.get(scalars_url, headers=headers, params={"resolution": 0, "maxPoints": 10})
    assert invalid_query.status_code == 400


def test_scalars_require_project_access(
    client: TestClient,
    scalars_setup: tuple[OwnerHeaders, str],
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
) -> None:
    owner_headers, scalars_url = scalars_setup

    forbidden = client.post(
        scalars_url,
        headers=outsider_headers,
        json={
            "start_line": 0,
            "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
        },
    )
    assert forbidden.status_code == 403

    add_collaborator(owner_headers)

    allowed = client.post(
        scalars_url,
        headers=outsider_headers,
        json={
            "start_line": 0,
            "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
        },
    )
    assert allowed.status_code == 200

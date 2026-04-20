from __future__ import annotations

from io import BytesIO

from fastapi.testclient import TestClient
from PIL import Image

from tests.conftest import CreateOrg, CreateUser, Headers, SessionForUser

ORGS = "/api/v1/organizations"
CORE = f"{ORGS}/core"
MEMBERS = f"{CORE}/members"


def test_create_update_organization(client: TestClient, owner_headers: Headers, outsider_headers: Headers) -> None:
    created = client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"})
    assert created.status_code == 201
    assert created.json()["handle"] == "core"
    assert created.json()["name"] == "Core"
    assert client.patch(CORE, headers=outsider_headers, json={"name": "Nope"}).status_code == 403
    updated = client.patch(CORE, headers=owner_headers, json={"name": "Core Team"})
    assert updated.status_code == 200 and updated.json()["name"] == "Core Team"


def test_organization_avatar(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_org: CreateOrg,
) -> None:
    create_org(owner_headers, handle="core", name="Core")
    png = BytesIO()
    Image.new("RGB", (64, 64), (10, 20, 30)).save(png, format="PNG")
    assert client.put(f"{CORE}/avatar", headers=outsider_headers, content=png.getvalue()).status_code == 403
    assert client.put(f"{CORE}/avatar", headers=owner_headers, content=png.getvalue()).status_code == 200
    fetched = client.get("/api/v1/accounts/core/avatar")
    assert fetched.status_code == 200 and fetched.headers["content-type"] == "image/jpeg"
    assert client.delete(f"{CORE}/avatar", headers=outsider_headers).status_code == 403
    assert client.delete(f"{CORE}/avatar", headers=owner_headers).status_code == 200
    assert client.get("/api/v1/accounts/core/avatar").status_code == 404


def test_member_can_remove_self(
    client: TestClient, owner_headers: Headers, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    member = create_user(email="member@example.com", handle="member", name="Member")
    member_headers = session_for_user(member)
    assert client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"}).status_code == 201
    assert client.put(f"{MEMBERS}/member", headers=owner_headers, json={}).status_code == 200
    removed = client.delete(f"{MEMBERS}/member", headers=member_headers)
    assert removed.status_code == 200 and removed.json() == {"status": "ok"}
    listed = client.get(MEMBERS)
    assert listed.status_code == 200 and [entry["handle"] for entry in listed.json()] == ["owner"]


def test_member_mutation_validation(
    client: TestClient, owner_headers: Headers, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    member = create_user(email="member@example.com", handle="member", name="Member")
    outsider = create_user(email="outsider@example.com", handle="outsider", name="Outsider")
    member_headers = session_for_user(member)
    assert client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"}).status_code == 201
    assert client.put(f"{MEMBERS}/owner", headers=owner_headers, json={"role": "MEMBER"}).status_code == 400
    assert client.delete(f"{MEMBERS}/owner", headers=owner_headers).status_code == 400
    assert client.put(f"{MEMBERS}/member", headers=owner_headers, json={}).status_code == 200
    assert client.put(f"{MEMBERS}/member", headers=member_headers, json={"role": "ADMIN"}).status_code == 403
    assert client.put(f"{MEMBERS}/member", headers=owner_headers, json={"role": "NOPE"}).status_code == 400
    assert client.put(f"{MEMBERS}/missing", headers=owner_headers, json={}).status_code == 404
    assert client.delete(f"{MEMBERS}/{outsider.handle}", headers=owner_headers).status_code == 404


def test_admin_handoff_allows_original_admin_to_leave(
    client: TestClient, owner_headers: Headers, create_user: CreateUser,
) -> None:
    create_user(email="member@example.com", handle="member", name="Member")
    assert client.post(ORGS, headers=owner_headers, json={"handle": "core", "name": "Core"}).status_code == 201
    assert client.put(f"{MEMBERS}/member", headers=owner_headers, json={"role": "ADMIN"}).status_code == 200
    assert client.put(f"{MEMBERS}/owner", headers=owner_headers, json={"role": "MEMBER"}).status_code == 200
    assert client.delete(f"{MEMBERS}/owner", headers=owner_headers).status_code == 200
    listed = client.get(MEMBERS)
    assert listed.status_code == 200 and [(entry["handle"], entry["role"]) for entry in listed.json()] == [
        ("member", "ADMIN"),
    ]

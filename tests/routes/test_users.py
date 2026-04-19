from __future__ import annotations

from io import BytesIO

from fastapi.testclient import TestClient
from PIL import Image

from tests.conftest import CreateOrg, CreateUser, Headers, SessionForUser

ORGS = "/api/v1/organizations"


def _png_bytes(color: tuple[int, int, int] = (20, 120, 200)) -> bytes:
    output = BytesIO()
    Image.new("RGB", (128, 64), color).save(output, format="PNG")
    return output.getvalue()


def test_email_exists(client: TestClient, create_user: CreateUser) -> None:
    create_user(email="ada@example.com", handle="ada", name="Ada")
    assert client.get("/api/v1/emails/exists").status_code == 400
    assert client.get("/api/v1/emails/exists", params={"email": "ada@example.com"}).json() == {"exists": True}
    assert client.get("/api/v1/emails/exists", params={"email": "grace@example.com"}).json() == {"exists": False}


def test_user_search(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    assert client.get("/api/v1/users/search", params={"query": "Ada"}).status_code == 401

    actor = create_user(email="actor@example.com", handle="actor", name="Actor")
    headers = session_for_user(actor)
    response = client.get("/api/v1/users/search", headers=headers)
    assert response.status_code == 400 and response.json() == {"error": "Missing query"}

    create_user(email="ada@example.com", handle="ada", name="Ada")
    create_user(email="adal@example.com", handle="adal", name="Ada Lovelace")
    create_user(email="adalong@example.com", handle="adalong", name="Someone")
    response = client.get("/api/v1/users/search", headers=headers, params={"query": "Ada"})
    assert response.status_code == 200 and [user["handle"] for user in response.json()] == ["ada", "adal", "adalong"]

    create_user(email="ada@other.com", handle="ada2", name="Ada Two")
    response = client.get("/api/v1/users/search", headers=headers, params={"query": "ada@"})
    assert response.status_code == 200
    assert [user["email"] for user in response.json()] == ["ada@example.com", "ada@other.com"]


def test_update_profile(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="sam@example.com", handle="sam", name="Sam")
    headers = session_for_user(user)
    response = client.patch("/api/v1/me", headers=headers, json={"name": "Sam Tester", "bio": "Building models."})
    assert response.status_code == 200
    assert (response.json()["name"], response.json()["bio"]) == ("Sam Tester", "Building models.")

    preserved = client.patch("/api/v1/me", headers=headers, json={"name": "Sam Researcher"})
    assert preserved.status_code == 200
    assert (preserved.json()["name"], preserved.json()["bio"]) == ("Sam Researcher", "Building models.")

    assert client.put("/api/v1/me/avatar", headers=headers, content=b"not-an-image").status_code == 400
    assert client.put("/api/v1/me/avatar", headers=headers, content=_png_bytes()).status_code == 200
    assert client.put("/api/v1/me/avatar", headers=headers, content=_png_bytes((200, 120, 20))).status_code == 200
    fetched = client.get("/api/v1/accounts/sam/avatar")
    assert fetched.status_code == 200 and fetched.headers["content-type"] == "image/jpeg"
    assert client.delete("/api/v1/me/avatar", headers=headers).status_code == 200
    assert client.get("/api/v1/accounts/sam/avatar").status_code == 404


def test_list_user_memberships(client: TestClient, owner_headers: Headers, create_org: CreateOrg) -> None:
    create_org(owner_headers)
    memberships = client.get("/api/v1/users/owner/memberships")
    assert memberships.status_code == 200
    [m] = memberships.json()
    assert (m["handle"], m["name"], m["role"], m["type"]) == ("core", "Core", "ADMIN", "ORGANIZATION")
    assert client.get("/api/v1/users/missing/memberships").status_code == 404

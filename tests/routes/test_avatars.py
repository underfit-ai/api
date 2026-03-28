from __future__ import annotations

from io import BytesIO

from fastapi.testclient import TestClient
from PIL import Image

from tests.conftest import CreateUser, SessionForUser


def _png_bytes(width: int = 128, height: int = 64, color: tuple[int, int, int] = (20, 120, 200)) -> bytes:
    image = Image.new("RGB", (width, height), color)
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def test_avatar_upload_fetch_and_delete(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    user = create_user(email="sam@example.com", handle="sam", name="Sam")
    headers = session_for_user(user)

    uploaded = client.put("/api/v1/me/avatar", headers=headers, content=_png_bytes(512, 256))
    assert uploaded.status_code == 200
    assert uploaded.json() == {"status": "ok"}

    fetched = client.get("/api/v1/accounts/sam/avatar")
    assert fetched.status_code == 200
    assert fetched.headers["content-type"] == "image/jpeg"

    deleted = client.delete("/api/v1/me/avatar", headers=headers)
    assert deleted.status_code == 200

    missing = client.get("/api/v1/accounts/sam/avatar")
    assert missing.status_code == 404


def test_avatar_rejects_invalid_bytes(
    client: TestClient, create_user: CreateUser, session_for_user: SessionForUser,
) -> None:
    user = create_user(email="alex@example.com", handle="alex", name="Alex")
    headers = session_for_user(user)

    response = client.put("/api/v1/me/avatar", headers=headers, content=b"not-an-image")
    assert response.status_code == 400

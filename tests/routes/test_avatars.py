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


def test_update_avatar(client: TestClient, create_user: CreateUser, session_for_user: SessionForUser) -> None:
    user = create_user(email="sam@example.com", handle="sam", name="Sam")
    headers = session_for_user(user)
    assert client.put("/api/v1/me/avatar", headers=headers, content=b"not-an-image").status_code == 400
    uploaded = client.put("/api/v1/me/avatar", headers=headers, content=_png_bytes(512, 256))
    assert uploaded.status_code == 200
    assert uploaded.json() == {"status": "ok"}
    replaced = client.put("/api/v1/me/avatar", headers=headers, content=_png_bytes(256, 512, (200, 120, 20)))
    assert replaced.status_code == 200

    fetched = client.get("/api/v1/accounts/sam/avatar")
    assert fetched.status_code == 200
    assert fetched.headers["content-type"] == "image/jpeg"

    assert client.delete("/api/v1/me/avatar", headers=headers).status_code == 200
    assert client.get("/api/v1/accounts/sam/avatar").status_code == 404

from __future__ import annotations

import io

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from PIL import Image

from app.dependencies import Conn, CurrentUser
from app.repositories import avatars as avatars_repo
from app.routes.resolvers import resolve_account

router = APIRouter()

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
AVATAR_SIZE = 256
JPEG_QUALITY = 82


@router.get("/accounts/{handle}/avatar")
def get_avatar(handle: str, conn: Conn) -> Response:
    account = resolve_account(conn, handle)
    image = avatars_repo.get(conn, account.id)
    if image is None:
        raise HTTPException(404, "Avatar not found")
    return Response(content=image, media_type="image/jpeg")


@router.put("/me/avatar")
async def upload_avatar(request: Request, conn: Conn, user: CurrentUser) -> dict[str, str]:
    body = await request.body()
    if len(body) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, "Image too large")
    if not body:
        raise HTTPException(400, "No image provided")
    image = _process_image(body)
    avatars_repo.upsert(conn, user.id, image)
    return {"status": "ok"}


@router.delete("/me/avatar")
def delete_avatar(conn: Conn, user: CurrentUser) -> dict[str, str]:
    avatars_repo.delete(conn, user.id)
    return {"status": "ok"}


def _process_image(data: bytes) -> bytes:
    try:
        img = Image.open(io.BytesIO(data))
    except Exception as e:
        raise HTTPException(400, "Invalid image") from e
    img.thumbnail((AVATAR_SIZE, AVATAR_SIZE))
    if img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY)
    result = buf.getvalue()
    if len(result) > MAX_OUTPUT_BYTES:
        raise HTTPException(400, "Processed image exceeds size limit")
    return result

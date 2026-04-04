from __future__ import annotations

import io

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from PIL import Image

from underfit_api.dependencies import Conn, CurrentUser
from underfit_api.models import OkResponse
from underfit_api.repositories import account_avatars as account_avatars_repo
from underfit_api.routes.resolvers import resolve_account

router = APIRouter()

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
AVATAR_SIZE = 256
JPEG_QUALITY = 82


@router.get("/accounts/{handle}/avatar")
def get_avatar(handle: str, conn: Conn) -> Response:
    account = resolve_account(conn, handle)
    if (image := account_avatars_repo.get(conn, account.id)) is None:
        raise HTTPException(404, "Avatar not found")
    return Response(content=image, media_type="image/jpeg")


@router.put("/me/avatar")
async def upload_avatar(request: Request, conn: Conn, user: CurrentUser) -> OkResponse:
    body = await request.body()
    if len(body) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, "Image too large")
    if not body:
        raise HTTPException(400, "No image provided")
    image = _process_image(body)
    account_avatars_repo.upsert(conn, user.id, image)
    return OkResponse()


@router.delete("/me/avatar")
def delete_avatar(conn: Conn, user: CurrentUser) -> OkResponse:
    account_avatars_repo.delete(conn, user.id)
    return OkResponse()


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

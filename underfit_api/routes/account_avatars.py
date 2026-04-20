from __future__ import annotations

import io
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from PIL import Image

from underfit_api.dependencies import Conn, RequireUser
from underfit_api.models import OkResponse
from underfit_api.repositories import account_avatars as account_avatars_repo
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.routes.resolvers import resolve_account, resolve_organization

router = APIRouter()

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
AVATAR_SIZE = 256
JPEG_QUALITY = 82


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


async def _read_image(request: Request) -> bytes:
    body = await request.body()
    if not body:
        raise HTTPException(400, "No image provided")
    if len(body) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, "Image too large")
    return _process_image(body)


def _require_org_admin(conn: Conn, handle: str, user_id: UUID) -> UUID:
    org = resolve_organization(conn, handle)
    if not organization_members_repo.is_admin(conn, org.id, user_id):
        raise HTTPException(403, "Forbidden")
    return org.id


@router.get("/accounts/{handle}/avatar")
def get_avatar(handle: str, conn: Conn) -> Response:
    account = resolve_account(conn, handle)
    if (image := account_avatars_repo.get(conn, account.id)) is None:
        raise HTTPException(404, "Avatar not found")
    return Response(content=image, media_type="image/jpeg")


@router.put("/me/avatar")
async def upload_avatar(request: Request, conn: Conn, user: RequireUser) -> OkResponse:
    account_avatars_repo.upsert(conn, user.id, await _read_image(request))
    return OkResponse()


@router.delete("/me/avatar")
def delete_avatar(conn: Conn, user: RequireUser) -> OkResponse:
    account_avatars_repo.delete(conn, user.id)
    return OkResponse()


@router.put("/organizations/{handle}/avatar")
async def upload_org_avatar(handle: str, request: Request, conn: Conn, user: RequireUser) -> OkResponse:
    account_avatars_repo.upsert(conn, _require_org_admin(conn, handle, user.id), await _read_image(request))
    return OkResponse()


@router.delete("/organizations/{handle}/avatar")
def delete_org_avatar(handle: str, conn: Conn, user: RequireUser) -> OkResponse:
    account_avatars_repo.delete(conn, _require_org_admin(conn, handle, user.id))
    return OkResponse()

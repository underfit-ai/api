from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from underfit_api.dependencies import Conn, CurrentUser
from underfit_api.models import OkResponse, Organization
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.routes.resolvers import resolve_organization

router = APIRouter(prefix="/organizations")


class CreateOrgBody(BaseModel):
    handle: str = Field(pattern=r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$")
    name: str = Field(min_length=1)


class UpdateOrgBody(BaseModel):
    name: str | None = None


@router.post("", status_code=201)
def create_organization(body: CreateOrgBody, conn: Conn, user: CurrentUser) -> Organization:
    handle_lower = body.handle.lower()
    if accounts_repo.alias_handle_exists(conn, handle_lower):
        raise HTTPException(409, "Handle already exists")
    org = organizations_repo.create(conn, handle_lower, body.name)
    accounts_repo.create_alias(conn, org.id, handle_lower)
    organization_members_repo.add_member(conn, org.id, user.id, "ADMIN")
    return org


@router.delete("/{handle}")
def delete_organization(handle: str, conn: Conn, user: CurrentUser) -> OkResponse:
    org = resolve_organization(conn, handle)
    if not organization_members_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    accounts_repo.delete(conn, org.id)
    return OkResponse()


@router.patch("/{handle}")
def update_organization(handle: str, body: UpdateOrgBody, conn: Conn, user: CurrentUser) -> Organization:
    org = resolve_organization(conn, handle)
    if not organization_members_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    if body.name is not None and not body.name.strip():
        raise HTTPException(400, "Name cannot be empty")
    if not (updated := organizations_repo.update(conn, org.id, body.name)):
        raise HTTPException(404, "Organization not found")
    return updated

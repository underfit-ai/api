from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.dependencies import Conn, CurrentUser
from app.models import Organization, OrganizationMember
from app.repositories import accounts as accounts_repo
from app.repositories import organizations as organizations_repo
from app.repositories import users as users_repo

router = APIRouter(prefix="/organizations")


class CreateOrgBody(BaseModel):
    handle: str = Field(pattern=r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$")
    name: str = Field(min_length=1)


class UpdateOrgBody(BaseModel):
    name: str | None = None


class UpdateMemberBody(BaseModel):
    role: str = "MEMBER"


@router.post("", status_code=201)
def create_organization(body: CreateOrgBody, conn: Conn, user: CurrentUser) -> Organization:
    handle_lower = body.handle.lower()
    if accounts_repo.exists(conn, handle_lower):
        raise HTTPException(409, "Handle already exists")
    org = organizations_repo.create(conn, handle_lower, body.name)
    organizations_repo.add_member(conn, org.id, user.id, "ADMIN")
    return org


@router.patch("/{handle}")
def update_organization(handle: str, body: UpdateOrgBody, conn: Conn, user: CurrentUser) -> Organization:
    if not (org := organizations_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "Organization not found")
    if not organizations_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    if body.name is not None and not body.name.strip():
        raise HTTPException(400, "Name cannot be empty")
    if not (updated := organizations_repo.update(conn, org.id, body.name)):
        raise HTTPException(404, "Organization not found")
    return updated


@router.get("/{handle}/members")
def list_members(handle: str, conn: Conn) -> list[OrganizationMember]:
    if not (org := organizations_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "Organization not found")
    return organizations_repo.list_members(conn, org.id)


@router.put("/{handle}/members/{user_handle}")
def add_or_update_member(
    handle: str, user_handle: str, body: UpdateMemberBody, conn: Conn, user: CurrentUser,
) -> OrganizationMember:
    if not (org := organizations_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "Organization not found")
    if not organizations_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if body.role not in ("ADMIN", "MEMBER"):
        raise HTTPException(400, "Invalid role")
    if organizations_repo.is_member(conn, org.id, target.id):
        current_role = organizations_repo.get_member_role(conn, org.id, target.id)
        if current_role == "ADMIN" and body.role != "ADMIN" and organizations_repo.admin_count(conn, org.id) <= 1:
            raise HTTPException(400, "Cannot remove only admin")
        organizations_repo.update_member(conn, org.id, target.id, body.role)
    else:
        organizations_repo.add_member(conn, org.id, target.id, body.role)
    members = organizations_repo.list_members(conn, org.id)
    if not (member := next((m for m in members if m.handle == target.handle), None)):
        raise HTTPException(500, "Unable to load organization member")
    return member


@router.delete("/{handle}/members/{user_handle}")
def remove_member(handle: str, user_handle: str, conn: Conn, user: CurrentUser) -> dict[str, bool]:
    if not (org := organizations_repo.get_by_handle(conn, handle)):
        raise HTTPException(404, "Organization not found")
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if not organizations_repo.is_member(conn, org.id, target.id):
        raise HTTPException(404, "Member not found")
    is_self = user.id == target.id
    if not is_self and not organizations_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    is_last_admin = (
        organizations_repo.get_member_role(conn, org.id, target.id) == "ADMIN"
        and organizations_repo.admin_count(conn, org.id) <= 1
    )
    if is_last_admin:
        raise HTTPException(400, "Cannot remove only admin")
    organizations_repo.remove_member(conn, org.id, target.id)
    return {"ok": True}

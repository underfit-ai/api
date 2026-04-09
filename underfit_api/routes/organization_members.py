from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from underfit_api.dependencies import Conn, CurrentUser
from underfit_api.models import OkResponse, OrganizationMember
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_organization

router = APIRouter(prefix="/organizations")


class UpdateMemberBody(BaseModel):
    role: str = "MEMBER"


@router.get("/{handle}/members")
def list_members(handle: str, conn: Conn) -> list[OrganizationMember]:
    org = resolve_organization(conn, handle)
    return organization_members_repo.list_members(conn, org.id)


@router.put("/{handle}/members/{user_handle}")
def add_or_update_member(
    handle: str, user_handle: str, body: UpdateMemberBody, conn: Conn, user: CurrentUser,
) -> OrganizationMember:
    org = resolve_organization(conn, handle)
    if not organization_members_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if body.role not in ("ADMIN", "MEMBER"):
        raise HTTPException(400, "Invalid role")
    if organization_members_repo.is_member(conn, org.id, target.id):
        current_role = organization_members_repo.get_member_role(conn, org.id, target.id)
        is_only_admin = current_role == "ADMIN" and organization_members_repo.admin_count(conn, org.id) <= 1
        if is_only_admin and body.role != "ADMIN":
            raise HTTPException(400, "Cannot remove only admin")
        organization_members_repo.update_member(conn, org.id, target.id, body.role)
    else:
        organization_members_repo.add_member(conn, org.id, target.id, body.role)
    if not (member := organization_members_repo.get_member(conn, org.id, target.id)):
        raise HTTPException(500, "Unable to load organization member")
    return member


@router.delete("/{handle}/members/{user_handle}")
def remove_member(handle: str, user_handle: str, conn: Conn, user: CurrentUser) -> OkResponse:
    org = resolve_organization(conn, handle)
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if not organization_members_repo.is_member(conn, org.id, target.id):
        raise HTTPException(404, "Member not found")
    is_self = user.id == target.id
    if not is_self and not organization_members_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    is_last_admin = (
        organization_members_repo.get_member_role(conn, org.id, target.id) == "ADMIN"
        and organization_members_repo.admin_count(conn, org.id) <= 1
    )
    if is_last_admin:
        raise HTTPException(400, "Cannot remove only admin")
    organization_members_repo.remove_member(conn, org.id, target.id)
    return OkResponse()

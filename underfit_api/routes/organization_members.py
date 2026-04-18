from __future__ import annotations

from fastapi import APIRouter, HTTPException

from underfit_api.dependencies import Conn, RequireUser
from underfit_api.models import Body, OkResponse, OrganizationMember
from underfit_api.repositories import organization_members as organization_members_repo
from underfit_api.repositories import users as users_repo
from underfit_api.routes.resolvers import resolve_organization

router = APIRouter(prefix="/organizations")


class UpdateMemberBody(Body):
    role: str = "MEMBER"


@router.get("/{handle}/members")
def list_members(handle: str, conn: Conn) -> list[OrganizationMember]:
    org = resolve_organization(conn, handle)
    return organization_members_repo.list_members(conn, org.id)


@router.put("/{handle}/members/{user_handle}")
def add_or_update_member(
    handle: str, user_handle: str, body: UpdateMemberBody, conn: Conn, user: RequireUser,
) -> OrganizationMember:
    org = resolve_organization(conn, handle)
    if not organization_members_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if body.role not in ("ADMIN", "MEMBER"):
        raise HTTPException(400, "Invalid role")
    if current_role := organization_members_repo.get_member_role(conn, org.id, target.id):
        updated = organization_members_repo.update_member(conn, org.id, target.id, body.role)
        if current_role == "ADMIN" and body.role != "ADMIN" and not updated:
            raise HTTPException(400, "Cannot remove only admin")
    else:
        organization_members_repo.add_member(conn, org.id, target.id, body.role)
    if not (member := organization_members_repo.get_member(conn, org.id, target.id)):
        raise HTTPException(500, "Unable to load organization member")
    return member


@router.delete("/{handle}/members/{user_handle}")
def remove_member(handle: str, user_handle: str, conn: Conn, user: RequireUser) -> OkResponse:
    org = resolve_organization(conn, handle)
    if not (target := users_repo.get_by_handle(conn, user_handle)):
        raise HTTPException(404, "User not found")
    if not (current_role := organization_members_repo.get_member_role(conn, org.id, target.id)):
        raise HTTPException(404, "Member not found")
    is_self = user.id == target.id
    if not is_self and not organization_members_repo.is_admin(conn, org.id, user.id):
        raise HTTPException(403, "Forbidden")
    removed = organization_members_repo.remove_member(conn, org.id, target.id)
    if current_role == "ADMIN" and not removed:
        raise HTTPException(400, "Cannot remove only admin")
    return OkResponse()

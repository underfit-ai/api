from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection, select
from sqlalchemy.sql.selectable import Exists

from underfit_api.helpers import utcnow
from underfit_api.models import OrganizationMember, UserMembership
from underfit_api.schema import accounts, organization_members, organizations, users

_join = organization_members.join(users, organization_members.c.user_id == users.c.id).join(
    accounts, users.c.id == accounts.c.id,
)
_select = select(
    users,
    accounts.c.handle,
    accounts.c.type,
    organization_members.c.role,
    organization_members.c.created_at.label("membership_created_at"),
    organization_members.c.updated_at.label("membership_updated_at"),
).select_from(_join)


def _has_other_admin(org_id: UUID, user_id: UUID) -> Exists:
    admins = select(organization_members.c.id).where(
        organization_members.c.organization_id == org_id, organization_members.c.role == "ADMIN",
        organization_members.c.user_id != user_id,
    ).subquery()
    return select(1).select_from(admins).exists()


def add_member(conn: Connection, org_id: UUID, user_id: UUID, role: str) -> None:
    now = utcnow()
    conn.execute(organization_members.insert().values(
        id=uuid4(), organization_id=org_id, user_id=user_id, role=role, created_at=now, updated_at=now,
    ))


def is_admin(conn: Connection, org_id: UUID, user_id: UUID) -> bool:
    return get_member_role(conn, org_id, user_id) == "ADMIN"


def get_member_role(conn: Connection, org_id: UUID, user_id: UUID) -> str | None:
    row = conn.execute(organization_members.select().where(
        organization_members.c.organization_id == org_id, organization_members.c.user_id == user_id,
    )).first()
    return row.role if row else None


def list_members(conn: Connection, org_id: UUID) -> list[OrganizationMember]:
    rows = conn.execute(_select.where(organization_members.c.organization_id == org_id)).all()
    return [OrganizationMember.model_validate(row) for row in rows]


def get_member(conn: Connection, org_id: UUID, user_id: UUID) -> OrganizationMember | None:
    row = conn.execute(
        _select.where(organization_members.c.organization_id == org_id, organization_members.c.user_id == user_id),
    ).first()
    return OrganizationMember.model_validate(row) if row else None


def update_member(conn: Connection, org_id: UUID, user_id: UUID, role: str) -> bool:
    query = organization_members.update().where(
        organization_members.c.organization_id == org_id, organization_members.c.user_id == user_id,
    )
    if role != "ADMIN":
        query = query.where((organization_members.c.role != "ADMIN") | _has_other_admin(org_id, user_id))
    result = conn.execute(query.values(role=role, updated_at=utcnow()))
    return result.rowcount > 0


def remove_member(conn: Connection, org_id: UUID, user_id: UUID) -> bool:
    result = conn.execute(organization_members.delete().where(
        organization_members.c.organization_id == org_id, organization_members.c.user_id == user_id,
        (organization_members.c.role != "ADMIN") | _has_other_admin(org_id, user_id),
    ))
    return result.rowcount > 0


def list_user_memberships(conn: Connection, user_id: UUID) -> list[UserMembership]:
    j = organization_members.join(organizations, organization_members.c.organization_id == organizations.c.id).join(
        accounts, organizations.c.id == accounts.c.id,
    )
    rows = conn.execute(j.select().where(organization_members.c.user_id == user_id)).all()
    return [UserMembership.model_validate(row) for row in rows]

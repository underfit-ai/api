from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Connection

from app.models import Organization, OrganizationMember, UserMembership
from app.schema import accounts, organization_members, organizations, users

_base_query = organizations.join(accounts, organizations.c.id == accounts.c.id).select


def get_by_id(conn: Connection, org_id: uuid.UUID) -> Organization | None:
    row = conn.execute(_base_query().where(organizations.c.id == org_id)).first()
    return Organization.model_validate(row) if row else None


def get_by_handle(conn: Connection, handle: str) -> Organization | None:
    row = conn.execute(_base_query().where(accounts.c.handle == handle.lower())).first()
    return Organization.model_validate(row) if row else None


def create(conn: Connection, handle: str, name: str) -> Organization:
    org_id = uuid.uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(accounts.insert().values(id=org_id, handle=handle, type="ORGANIZATION"))
    conn.execute(organizations.insert().values(id=org_id, name=name, created_at=now, updated_at=now))
    result = get_by_id(conn, org_id)
    assert result is not None
    return result


def update(conn: Connection, org_id: uuid.UUID, name: str | None) -> Organization | None:
    if name is not None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn.execute(organizations.update().where(organizations.c.id == org_id).values(name=name, updated_at=now))
    return get_by_id(conn, org_id)


def add_member(conn: Connection, org_id: uuid.UUID, user_id: uuid.UUID, role: str) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(organization_members.insert().values(
        id=uuid.uuid4(), organization_id=org_id, user_id=user_id, role=role, created_at=now, updated_at=now,
    ))


def is_admin(conn: Connection, org_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    row = conn.execute(
        organization_members.select().where(
            organization_members.c.organization_id == org_id,
            organization_members.c.user_id == user_id,
            organization_members.c.role == "ADMIN",
        ),
    ).first()
    return row is not None


def is_member(conn: Connection, org_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    row = conn.execute(
        organization_members.select().where(
            organization_members.c.organization_id == org_id,
            organization_members.c.user_id == user_id,
        ),
    ).first()
    return row is not None


def get_member_role(conn: Connection, org_id: uuid.UUID, user_id: uuid.UUID) -> str | None:
    row = conn.execute(
        organization_members.select().where(
            organization_members.c.organization_id == org_id,
            organization_members.c.user_id == user_id,
        ),
    ).first()
    return row.role if row else None


def list_members(conn: Connection, org_id: uuid.UUID) -> list[OrganizationMember]:
    j = organization_members.join(users, organization_members.c.user_id == users.c.id).join(
        accounts, users.c.id == accounts.c.id,
    )
    rows = conn.execute(j.select().where(organization_members.c.organization_id == org_id)).all()
    return [OrganizationMember.model_validate(row) for row in rows]


def update_member(conn: Connection, org_id: uuid.UUID, user_id: uuid.UUID, role: str) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        organization_members.update()
        .where(organization_members.c.organization_id == org_id, organization_members.c.user_id == user_id)
        .values(role=role, updated_at=now),
    )


def remove_member(conn: Connection, org_id: uuid.UUID, user_id: uuid.UUID) -> None:
    conn.execute(
        organization_members.delete().where(
            organization_members.c.organization_id == org_id,
            organization_members.c.user_id == user_id,
        ),
    )


def admin_count(conn: Connection, org_id: uuid.UUID) -> int:
    rows = conn.execute(
        organization_members.select().where(
            organization_members.c.organization_id == org_id,
            organization_members.c.role == "ADMIN",
        ),
    ).all()
    return len(rows)


def list_user_memberships(conn: Connection, user_id: uuid.UUID) -> list[UserMembership]:
    j = organization_members.join(organizations, organization_members.c.organization_id == organizations.c.id).join(
        accounts, organizations.c.id == accounts.c.id,
    )
    rows = conn.execute(j.select().where(organization_members.c.user_id == user_id)).all()
    return [UserMembership.model_validate(row) for row in rows]

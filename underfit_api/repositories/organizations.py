from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Organization
from underfit_api.schema import accounts, organizations

_base_query = organizations.join(accounts, organizations.c.id == accounts.c.id).select


def get_by_id(conn: Connection, org_id: UUID) -> Organization | None:
    row = conn.execute(_base_query().where(organizations.c.id == org_id)).first()
    return Organization.model_validate(row) if row else None


def get_by_handle(conn: Connection, handle: str) -> Organization | None:
    row = conn.execute(_base_query().where(accounts.c.handle == handle.lower())).first()
    return Organization.model_validate(row) if row else None


def create(conn: Connection, handle: str, name: str) -> Organization:
    org_id = uuid4()
    now = utcnow()
    conn.execute(accounts.insert().values(id=org_id, handle=handle, type="ORGANIZATION"))
    conn.execute(organizations.insert().values(id=org_id, name=name, created_at=now, updated_at=now))
    result = get_by_id(conn, org_id)
    assert result is not None
    return result


def update_name(conn: Connection, org_id: UUID, name: str) -> Organization:
    conn.execute(organizations.update().where(organizations.c.id == org_id).values(updated_at=utcnow(), name=name))
    result = get_by_id(conn, org_id)
    assert result is not None
    return result

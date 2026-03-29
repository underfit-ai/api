from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Union
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, PlainSerializer
from pydantic.alias_generators import to_camel

UTCDatetime = Annotated[
    datetime,
    AfterValidator(lambda v: v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v),
    PlainSerializer(lambda dt: dt.isoformat() + "Z", return_type=str),
]


class _Base(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, from_attributes=True)


class User(_Base):
    id: UUID
    handle: str
    type: str
    email: str
    name: str
    bio: str | None
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Organization(_Base):
    id: UUID
    handle: str
    type: str
    name: str
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Session(_Base):
    token: str
    created_at: UTCDatetime
    expires_at: UTCDatetime


class ApiKey(_Base):
    id: UUID
    user_id: UUID
    label: str | None
    created_at: UTCDatetime


class ApiKeyWithToken(ApiKey):
    token: str


class OrganizationMember(_Base):
    id: UUID
    handle: str
    type: str
    email: str
    name: str
    bio: str | None
    role: str
    created_at: UTCDatetime
    updated_at: UTCDatetime


class UserMembership(_Base):
    id: UUID
    handle: str
    type: str
    name: str
    role: str
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Project(_Base):
    id: UUID
    owner: str
    name: str
    description: str | None
    visibility: str
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Run(_Base):
    id: UUID
    project_id: UUID
    user: str
    project_name: str
    project_owner: str
    name: str
    status: str
    config: dict[str, object] | None
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Artifact(_Base):
    id: UUID
    project_id: UUID
    run_id: UUID | None
    step: int | None
    name: str
    type: str
    status: str
    storage_key: str
    declared_file_count: int
    uploaded_file_count: int
    created_at: UTCDatetime
    updated_at: UTCDatetime
    finalized_at: UTCDatetime | None
    metadata: dict[str, object] | None


class Media(_Base):
    id: UUID
    run_id: UUID
    key: str
    step: int | None
    type: str
    storage_key: str
    count: int
    metadata: dict[str, object] | None
    created_at: UTCDatetime


class Scalar(_Base):
    step: int | None
    values: dict[str, float]
    timestamp: UTCDatetime


class AuthResponse(_Base):
    user: User
    session: Session


Account = Union[User, Organization]

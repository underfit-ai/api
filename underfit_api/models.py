from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Literal, Union
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


class ProjectVisibility(str, Enum):
    PRIVATE = "private"
    PUBLIC = "public"


class RunTerminalState(str, Enum):
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MediaType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    HTML = "html"


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


class OrganizationMember(User):
    role: str
    membership_created_at: UTCDatetime
    membership_updated_at: UTCDatetime


class UserMembership(Organization):
    role: str


class Project(_Base):
    id: UUID
    owner: str
    name: str
    description: str | None
    visibility: ProjectVisibility
    pending_transfer_to: UUID | None
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Run(_Base):
    id: UUID
    project_id: UUID
    user: str
    project_name: str
    project_owner: str
    name: str
    terminal_state: RunTerminalState | None = None
    is_active: bool
    config: dict[str, object] | None
    worker_token: str | None = None
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Worker(_Base):
    id: UUID
    run_id: UUID
    worker_label: str
    worker_token: str | None = None
    is_primary: bool
    last_heartbeat: UTCDatetime
    joined_at: UTCDatetime


class Artifact(_Base):
    id: UUID
    project_id: UUID
    run_id: UUID | None
    step: int | None
    name: str
    type: str
    storage_key: str
    stored_size_bytes: int | None
    created_at: UTCDatetime
    updated_at: UTCDatetime
    finalized_at: UTCDatetime | None
    metadata: dict[str, object] | None


class Media(_Base):
    id: UUID
    run_id: UUID
    key: str
    step: int | None
    type: MediaType
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


class ExistsResponse(_Base):
    exists: bool


class OkResponse(_Base):
    status: Literal["ok"] = "ok"


class BufferedResponse(_Base):
    status: Literal["buffered"] = "buffered"
    next_start_line: int | None = None


class HealthResponse(_Base):
    status: Literal["ok"] = "ok"
    version: Literal["v1"] = "v1"


class LogEntry(_Base):
    start_line: int
    end_line: int
    content: str
    start_at: UTCDatetime
    end_at: UTCDatetime


class LogEntriesResponse(_Base):
    entries: list[LogEntry]
    next_cursor: int
    has_more: bool


class ProjectCollaborator(User):
    collaborator_created_at: UTCDatetime
    collaborator_updated_at: UTCDatetime


Account = Union[User, Organization]

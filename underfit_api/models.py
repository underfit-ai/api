from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Literal, Union
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, PlainSerializer
from pydantic.alias_generators import to_camel

UTCDatetime = Annotated[
    datetime,
    AfterValidator(lambda v: v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v),
    PlainSerializer(lambda dt: dt.isoformat() + "Z", return_type=str),
]


class Schema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, from_attributes=True)


class Body(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


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


class User(Schema):
    id: UUID
    handle: str
    type: str
    email: str
    name: str
    bio: str
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Organization(Schema):
    id: UUID
    handle: str
    type: str
    name: str
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Session(Schema):
    token: str
    created_at: UTCDatetime
    expires_at: UTCDatetime


class ApiKey(Schema):
    id: UUID
    user_id: UUID
    label: str
    token_prefix: str
    created_at: UTCDatetime


class ApiKeyWithToken(ApiKey):
    token: str


class OrganizationMember(User):
    role: str
    membership_created_at: UTCDatetime
    membership_updated_at: UTCDatetime


class UserMembership(Organization):
    role: str


class Project(Schema):
    id: UUID
    owner: str
    account_id: UUID = Field(exclude=True)
    account_type: str = Field(exclude=True)
    name: str
    storage_key: str = Field(exclude=True)
    description: str
    metadata: dict[str, object]
    ui_state: dict[str, object]
    baseline_run_id: UUID | None = None
    visibility: ProjectVisibility
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Run(Schema):
    id: UUID
    project_id: UUID
    user: str
    project_name: str
    project_owner: str
    project_owner_id: UUID = Field(exclude=True)
    project_owner_type: str = Field(exclude=True)
    launch_id: str
    name: str
    storage_key: str = Field(exclude=True)
    terminal_state: RunTerminalState | None = None
    is_active: bool
    config: dict[str, object] | None
    metadata: dict[str, object]
    ui_state: dict[str, object]
    is_pinned: bool = False
    is_baseline: bool = False
    summary: dict[str, float]
    created_at: UTCDatetime
    updated_at: UTCDatetime


class Worker(Schema):
    id: UUID
    run_id: UUID
    run_storage_key: str = Field(exclude=True)
    worker_label: str
    last_heartbeat: UTCDatetime
    joined_at: UTCDatetime


class Artifact(Schema):
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


class Media(Schema):
    id: UUID
    run_id: UUID
    key: str
    step: int
    type: MediaType
    index: int
    storage_key: str = Field(exclude=True)
    finalized: bool
    metadata: dict[str, object] | None
    created_at: UTCDatetime


class Scalar(Schema):
    step: int | None = None
    values: dict[str, float]
    timestamp: UTCDatetime


class ExistsResponse(Schema):
    exists: bool


class OkResponse(Schema):
    status: Literal["ok"] = "ok"


class BufferedResponse(Schema):
    status: Literal["buffered"] = "buffered"
    next_start_line: int | None = None


class HealthResponse(Schema):
    status: Literal["ok"] = "ok"
    version: Literal["v1"] = "v1"


class LogLine(Schema):
    timestamp: UTCDatetime
    content: str


class LogEntry(Schema):
    start_line: int
    end_line: int
    content: str
    start_at: UTCDatetime
    end_at: UTCDatetime


class ProjectCollaborator(User):
    collaborator_created_at: UTCDatetime
    collaborator_updated_at: UTCDatetime


Account = Union[User, Organization]

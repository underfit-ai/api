from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

metadata = sa.MetaData()

HANDLE_LENGTH = 255
EMAIL_LENGTH = 320
NAME_LENGTH = 255
TOKEN_PREFIX_LENGTH = 8
TOKEN_HASH_LENGTH = 64
LAUNCH_ID_LENGTH = 255
WORKER_LABEL_LENGTH = 255

def _datetime() -> sa.DateTime:
    return sa.DateTime().with_variant(mysql.DATETIME(fsp=6), "mysql")


accounts = sa.Table(
    "accounts",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("handle", sa.String(HANDLE_LENGTH), nullable=False, unique=True),
    sa.Column("type", sa.Text, nullable=False),
    sa.CheckConstraint("type IN ('USER', 'ORGANIZATION')"),
)

account_avatars = sa.Table(
    "account_avatars",
    metadata,
    sa.Column("account_id", sa.Uuid, sa.ForeignKey("accounts.id", ondelete="CASCADE"), primary_key=True),
    sa.Column("image", sa.LargeBinary, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
)

users = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("email", sa.String(EMAIL_LENGTH), nullable=False, unique=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("bio", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
)

user_auth = sa.Table(
    "user_auth",
    metadata,
    sa.Column("id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    sa.Column("password_hash", sa.Text, nullable=False),
    sa.Column("password_salt", sa.Text, nullable=False),
    sa.Column("password_iterations", sa.Integer, nullable=False),
    sa.Column("password_digest", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
)

sessions = sa.Table(
    "sessions",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("token_hash", sa.String(TOKEN_HASH_LENGTH), nullable=False, unique=True),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("expires_at", _datetime(), nullable=False),
)

api_keys = sa.Table(
    "api_keys",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("label", sa.Text, nullable=False),
    sa.Column("token_prefix", sa.String(TOKEN_PREFIX_LENGTH), nullable=False),
    sa.Column("token_hash", sa.String(TOKEN_HASH_LENGTH), nullable=False, unique=True),
    sa.Column("created_at", _datetime(), nullable=False),
)

organizations = sa.Table(
    "organizations",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
)

organization_members = sa.Table(
    "organization_members",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("organization_id", sa.Uuid, sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("role", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
    sa.UniqueConstraint("organization_id", "user_id"),
)

projects = sa.Table(
    "projects",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("account_id", sa.Uuid, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("name", sa.String(NAME_LENGTH), nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("description", sa.Text, nullable=False),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("ui_state", sa.JSON, nullable=False),
    sa.Column("baseline_project_id", sa.Uuid),
    sa.Column("baseline_run_id", sa.Uuid),
    sa.Column("visibility", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
    sa.ForeignKeyConstraint(
        ["baseline_project_id", "baseline_run_id"], ["runs.project_id", "runs.id"], ondelete="SET NULL",
    ),
    sa.UniqueConstraint("account_id", "name"),
    sa.CheckConstraint(
        "(baseline_project_id IS NULL AND baseline_run_id IS NULL) "
        "OR (baseline_project_id IS NOT NULL AND baseline_run_id IS NOT NULL)",
    ).ddl_if(dialect=("sqlite", "postgresql")),  # ty: ignore[invalid-argument-type]
    sa.CheckConstraint(
        "baseline_project_id IS NULL OR baseline_project_id = id",
    ).ddl_if(dialect=("sqlite", "postgresql")),  # ty: ignore[invalid-argument-type]
    sa.CheckConstraint("visibility IN ('private', 'public')"),
)

project_collaborators = sa.Table(
    "project_collaborators",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("project_id", sa.Uuid, sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
    sa.UniqueConstraint("project_id", "user_id"),
)

runs = sa.Table(
    "runs",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("project_id", sa.Uuid, sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("launch_id", sa.String(LAUNCH_ID_LENGTH), nullable=False),
    sa.Column("name", sa.String(NAME_LENGTH), nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("terminal_state", sa.Text),
    sa.Column("config", sa.JSON),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("ui_state", sa.JSON, nullable=False),
    sa.Column("is_pinned", sa.Boolean, nullable=False),
    sa.Column("summary", sa.JSON, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
    sa.UniqueConstraint("project_id", "name"),
    sa.UniqueConstraint("project_id", "launch_id"),
    sa.UniqueConstraint("project_id", "id"),
    sa.CheckConstraint("terminal_state IN ('finished', 'failed', 'cancelled')"),
)

run_workers = sa.Table(
    "run_workers",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("run_id", sa.Uuid, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("worker_label", sa.String(WORKER_LABEL_LENGTH), nullable=False),
    sa.Column("last_heartbeat", _datetime(), nullable=False),
    sa.Column("joined_at", _datetime(), nullable=False),
    sa.UniqueConstraint("run_id", "worker_label"),
)

log_segments = sa.Table(
    "log_segments",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("worker_id", sa.Uuid, sa.ForeignKey("run_workers.id", ondelete="CASCADE"), nullable=False),
    sa.Column("start_line", sa.Integer, nullable=False),
    sa.Column("end_line", sa.Integer, nullable=False),
    sa.Column("start_at", _datetime(), nullable=False),
    sa.Column("end_at", _datetime(), nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.UniqueConstraint("worker_id", "start_line"),
    sa.UniqueConstraint("worker_id", "end_line"),
)

scalar_segments = sa.Table(
    "scalar_segments",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("worker_id", sa.Uuid, sa.ForeignKey("run_workers.id", ondelete="CASCADE"), nullable=False),
    sa.Column("resolution", sa.Integer, nullable=False),
    sa.Column("start_line", sa.Integer, nullable=False),
    sa.Column("end_line", sa.Integer, nullable=False),
    sa.Column("end_step", sa.Integer, nullable=False),
    sa.Column("start_at", _datetime(), nullable=False),
    sa.Column("end_at", _datetime(), nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.UniqueConstraint("worker_id", "resolution", "start_line"),
    sa.UniqueConstraint("worker_id", "resolution", "end_line"),
)

scalar_points = sa.Table(
    "scalar_points",
    metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer, "sqlite"), primary_key=True, autoincrement=True),
    sa.Column("worker_id", sa.Uuid, sa.ForeignKey("run_workers.id", ondelete="CASCADE"), nullable=False),
    sa.Column("line", sa.Integer, nullable=False),
    sa.Column("step", sa.Integer, nullable=False),
    sa.Column("key", sa.Text().with_variant(sa.String(512), "mysql"), nullable=False),
    sa.Column("value", sa.Float, nullable=False),
    sa.Column("timestamp", _datetime(), nullable=False),
    sa.UniqueConstraint("worker_id", "line", "key"),
)

log_chunks = sa.Table(
    "log_chunks",
    metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer, "sqlite"), primary_key=True, autoincrement=True),
    sa.Column("worker_id", sa.Uuid, sa.ForeignKey("run_workers.id", ondelete="CASCADE"), nullable=False),
    sa.Column("start_line", sa.Integer, nullable=False),
    sa.Column("line_count", sa.Integer, nullable=False),
    sa.Column("byte_count", sa.Integer, nullable=False),
    sa.Column("content", sa.Text, nullable=False),
    sa.Column("start_at", _datetime(), nullable=False),
    sa.Column("end_at", _datetime(), nullable=False),
    sa.UniqueConstraint("worker_id", "start_line"),
)

artifacts = sa.Table(
    "artifacts",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("project_id", sa.Uuid, sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    sa.Column("run_id", sa.Uuid),
    sa.Column("step", sa.Integer),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("type", sa.Text, nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("stored_size_bytes", sa.BigInteger),
    sa.Column("active_uploads", sa.Integer, nullable=False, server_default="0"),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.Column("updated_at", _datetime(), nullable=False),
    sa.Column("finalized_at", _datetime()),
    sa.Column("metadata", sa.JSON),
    sa.ForeignKeyConstraint(["project_id", "run_id"], ["runs.project_id", "runs.id"], ondelete="CASCADE"),
    sa.CheckConstraint("run_id IS NOT NULL OR step IS NULL"),
    sa.CheckConstraint("active_uploads >= 0 OR active_uploads = -1"),
)

media = sa.Table(
    "media",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("run_id", sa.Uuid, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("key", sa.Text().with_variant(sa.String(512), "mysql"), nullable=False),
    sa.Column("step", sa.Integer, nullable=False),
    sa.Column("type", sa.Text().with_variant(sa.String(16), "mysql"), nullable=False),
    sa.Column("index", sa.Integer, nullable=False),
    sa.Column("storage_key", sa.Text().with_variant(sa.String(512), "mysql"), nullable=False),
    sa.Column("finalized", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("metadata", sa.JSON),
    sa.Column("created_at", _datetime(), nullable=False),
    sa.UniqueConstraint("run_id", "storage_key"),
    sa.UniqueConstraint("run_id", "type", "key", "step", "index"),
    sa.CheckConstraint("type IN ('image', 'video', 'audio', 'html')"),
)

import sqlalchemy as sa

metadata = sa.MetaData()

accounts = sa.Table(
    "accounts",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("handle", sa.Text, nullable=False, unique=True),
    sa.Column("type", sa.Text, nullable=False, server_default="USER"),
)

account_aliases = sa.Table(
    "account_aliases",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("account_id", sa.Uuid, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("handle", sa.Text, nullable=False, unique=True),
    sa.Column("created_at", sa.DateTime, nullable=False),
)

account_avatars = sa.Table(
    "account_avatars",
    metadata,
    sa.Column("account_id", sa.Uuid, sa.ForeignKey("accounts.id", ondelete="CASCADE"), primary_key=True),
    sa.Column("image", sa.LargeBinary, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
)

users = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("email", sa.Text, nullable=False, unique=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("bio", sa.Text, nullable=False, server_default=""),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
)

user_auth = sa.Table(
    "user_auth",
    metadata,
    sa.Column("id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    sa.Column("password_hash", sa.Text, nullable=False),
    sa.Column("password_salt", sa.Text, nullable=False),
    sa.Column("password_iterations", sa.Integer, nullable=False),
    sa.Column("password_digest", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
)

sessions = sa.Table(
    "sessions",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("token_hash", sa.Text, nullable=False, unique=True),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("expires_at", sa.DateTime, nullable=False),
)

api_keys = sa.Table(
    "api_keys",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("label", sa.Text),
    sa.Column("token_prefix", sa.Text, nullable=False),
    sa.Column("token_hash", sa.Text, nullable=False, unique=True),
    sa.Column("created_at", sa.DateTime, nullable=False),
)

organizations = sa.Table(
    "organizations",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
)

organization_members = sa.Table(
    "organization_members",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("organization_id", sa.Uuid, sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("role", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("organization_id", "user_id"),
)

projects = sa.Table(
    "projects",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("account_id", sa.Uuid, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("description", sa.Text),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("visibility", sa.Text, nullable=False, server_default="private"),
    sa.Column("pending_transfer_to", sa.Uuid, sa.ForeignKey("accounts.id"), nullable=True),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("account_id", "name"),
    sa.CheckConstraint("visibility IN ('private', 'public')"),
)

project_aliases = sa.Table(
    "project_aliases",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("project_id", sa.Uuid, sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    sa.Column("account_id", sa.Uuid, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("account_id", "name"),
)

project_collaborators = sa.Table(
    "project_collaborators",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("project_id", sa.Uuid, sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("project_id", "user_id"),
)

runs = sa.Table(
    "runs",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("project_id", sa.Uuid, sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    sa.Column("user_id", sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    sa.Column("launch_id", sa.Text, nullable=False),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("terminal_state", sa.Text),
    sa.Column("config", sa.JSON),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
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
    sa.Column("worker_label", sa.Text, nullable=False),
    sa.Column("last_heartbeat", sa.DateTime, nullable=False),
    sa.Column("joined_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("run_id", "worker_label"),
)

log_segments = sa.Table(
    "log_segments",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("worker_id", sa.Uuid, sa.ForeignKey("run_workers.id", ondelete="CASCADE"), nullable=False),
    sa.Column("start_line", sa.Integer, nullable=False),
    sa.Column("end_line", sa.Integer, nullable=False),
    sa.Column("start_at", sa.DateTime, nullable=False),
    sa.Column("end_at", sa.DateTime, nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("worker_id", "start_line"),
)

scalar_segments = sa.Table(
    "scalar_segments",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("worker_id", sa.Uuid, sa.ForeignKey("run_workers.id", ondelete="CASCADE"), nullable=False),
    sa.Column("resolution", sa.Integer, nullable=False),
    sa.Column("start_line", sa.Integer, nullable=False),
    sa.Column("end_line", sa.Integer, nullable=False),
    sa.Column("start_at", sa.DateTime, nullable=False),
    sa.Column("end_at", sa.DateTime, nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.UniqueConstraint("worker_id", "resolution", "start_line"),
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
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.Column("updated_at", sa.DateTime, nullable=False),
    sa.Column("finalized_at", sa.DateTime),
    sa.Column("metadata", sa.JSON),
    sa.ForeignKeyConstraint(["project_id", "run_id"], ["runs.project_id", "runs.id"], ondelete="CASCADE"),
    sa.CheckConstraint("run_id IS NOT NULL OR step IS NULL"),
)

media = sa.Table(
    "media",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("run_id", sa.Uuid, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("key", sa.Text, nullable=False),
    sa.Column("step", sa.Integer),
    sa.Column("type", sa.Text, nullable=False),
    sa.Column("storage_key", sa.Text, nullable=False),
    sa.Column("count", sa.Integer, nullable=False),
    sa.Column("metadata", sa.JSON),
    sa.Column("created_at", sa.DateTime, nullable=False),
    sa.CheckConstraint("type IN ('image', 'video', 'audio', 'html')"),
)

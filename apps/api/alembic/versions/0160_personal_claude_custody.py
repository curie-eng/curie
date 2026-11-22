"""Personal Claude custody references and enrollment-fenced teardown (#5377).

ADR 0268 sections 3 and 4. ``personal_claude_custody`` records, per enrollment
generation, the token-free reference to the immutable Kubernetes Secret that
holds the grant: its namespace, its computed name, its single key and the UID
Kubernetes assigned. It never holds token bytes. A row is ``staged`` before its
Secret exists, ``published`` once the header names its generation, ``retiring``
after a newer generation (or a disconnect) superseded it, and ``retired`` once
the Secret's absence was verified. A staged row that lost is ``abandoned``;
``secret_absent_at`` is set when the absence of its Secret is verified too.

Two ``execution_requests`` CHECKs are relaxed so a replaced or disconnected
enrollment can fence its owner's runtimes and confirm their teardown:

1. ``execution_requests_state_shape_ck`` admits ``enrollment_fenced`` as a
   ``cancellation_requested`` cause and as a started ``failed`` cause that,
   like ``owner_lost``, requires a termination observation; a ``completed`` or
   any other started ``failed`` request may now carry a termination
   observation (a confirmed teardown). Every other branch is unchanged.
2. ``execution_requests_teardown_unconfirmed_ck`` admits the teardown flag on
   a started ``completed``, ``failed`` or ``expired`` request, not only on a
   force-settled cancellation.

Both are written as frozen full literals. The upgrade refuses unless the prior
definitions are exactly the 0159 ones, compared as this server prints them, so
it never replaces a CHECK it does not recognize. The downgrade refuses while
any custody row or any row only the relaxed shapes admit remains, then
restores the 0159 definitions exactly and drops the table.

Revision ID: 0160
Revises: 0159
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0160"
down_revision: str = "0159"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

# A bounded wait for the tables the downgrade guard reads, so a busy table
# refuses the downgrade rather than blocking writers behind it indefinitely.
LOCK_TIMEOUT_MS = 15000

SHAPE = "execution_requests_state_shape_ck"
TEARDOWN = "execution_requests_teardown_unconfirmed_ck"

# Frozen: the 0159 definitions exactly as Postgres prints them
# (``pg_get_constraintdef`` without its ``CHECK (...)`` wrapper).
PRIOR_SHAPE = (
    "(((((status)::text = 'failed'::text) AND (started_at IS NULL) AND (execution_deadline "
    "IS NULL) AND (terminal_at IS NOT NULL) AND ((terminal_cause)::text = "
    "'start_failed'::text) AND (termination_observation IS NULL)) OR (((((status)::text = "
    "'queued'::text) AND (wait_deadline IS NULL) AND (started_at IS NULL) AND "
    "(execution_deadline IS NULL) AND (terminal_at IS NULL) AND (terminal_cause IS NULL) "
    "AND (termination_observation IS NULL)) OR (((status)::text = 'cancelled'::text) AND "
    "(wait_deadline IS NULL) AND (started_at IS NULL) AND (execution_deadline IS NULL) AND "
    "(terminal_at IS NOT NULL) AND ((terminal_cause)::text = ANY "
    "(ARRAY[('issue_cancelled'::character varying)::text, ('issue_deleted'::character "
    "varying)::text, ('lineage_closed'::character varying)::text])) AND "
    "(termination_observation IS NULL)) OR (((((status)::text = 'waiting'::text) AND "
    "(started_at IS NULL) AND (execution_deadline IS NULL) AND (terminal_at IS NULL) AND "
    "(terminal_cause IS NULL) AND (termination_observation IS NULL)) OR (((status)::text = "
    "'running'::text) AND (started_at IS NOT NULL) AND (execution_deadline IS NOT NULL) AND "
    "(terminal_at IS NULL) AND (terminal_cause IS NULL) AND (termination_observation IS "
    "NULL)) OR (((status)::text = 'cancellation_requested'::text) AND (started_at IS NOT "
    "NULL) AND (execution_deadline IS NOT NULL) AND (terminal_at IS NULL) AND "
    "(terminal_cause IS NOT NULL) AND ((terminal_cause)::text = ANY "
    "(ARRAY[('issue_cancelled'::character varying)::text, ('issue_deleted'::character "
    "varying)::text, ('execution_deadline'::character varying)::text, "
    "('owner_lost'::character varying)::text])) AND (termination_observation IS NULL)) OR "
    "(((status)::text = 'completed'::text) AND (started_at IS NOT NULL) AND "
    "(execution_deadline IS NOT NULL) AND (terminal_at IS NOT NULL) AND (terminal_cause IS "
    "NOT NULL) AND ((terminal_cause)::text = 'completed'::text) AND "
    "(termination_observation IS NULL)) OR (((status)::text = 'failed'::text) AND "
    "(started_at IS NOT NULL) AND (execution_deadline IS NOT NULL) AND (terminal_at IS NOT "
    "NULL) AND (terminal_cause IS NOT NULL) AND ((((terminal_cause)::text = "
    "'owner_lost'::text) AND (termination_observation IS NOT NULL)) OR "
    "(((terminal_cause)::text <> 'owner_lost'::text) AND (termination_observation IS "
    "NULL)))) OR (((status)::text = 'expired'::text) AND (terminal_at IS NOT NULL) AND "
    "(((started_at IS NULL) AND (execution_deadline IS NULL) AND (terminal_cause IS NOT "
    "NULL) AND ((terminal_cause)::text = 'capacity_wait_expired'::text) AND "
    "(termination_observation IS NULL)) OR ((started_at IS NOT NULL) AND "
    "(execution_deadline IS NOT NULL) AND (terminal_cause IS NOT NULL) AND "
    "((terminal_cause)::text = 'execution_deadline'::text) AND (termination_observation IS "
    "NOT NULL)))) OR (((status)::text = 'cancelled'::text) AND (terminal_at IS NOT NULL) "
    "AND (terminal_cause IS NOT NULL) AND ((terminal_cause)::text = ANY "
    "(ARRAY['issue_cancelled'::text, 'issue_deleted'::text])) AND (((started_at IS NULL) "
    "AND (execution_deadline IS NULL) AND (termination_observation IS NULL)) OR "
    "((started_at IS NOT NULL) AND (execution_deadline IS NOT NULL) AND "
    "(termination_observation IS NOT NULL))))) IS TRUE)) IS TRUE)) IS TRUE)"
)
PRIOR_TEARDOWN = "((teardown_unconfirmed_at IS NULL) OR ((status)::text = 'cancelled'::text))"

# Frozen: the 0160 definitions, copied in ``models.ExecutionRequest``. The
# nested ``IS TRUE`` groups of 0159 are flattened (an OR is true when any
# operand is); only the three marked branches differ from 0159.
STATE_SHAPE = (
    "((status = 'failed' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NOT NULL AND terminal_cause = 'start_failed' "
    "AND termination_observation IS NULL) "
    "OR (status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancelled' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', 'lineage_closed') "
    "AND termination_observation IS NULL) "
    "OR (status = 'waiting' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NULL AND terminal_cause IS NULL "
    "AND termination_observation IS NULL) "
    "OR (status = 'running' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    # 0160: an enrollment fence stops a running request.
    "OR (status = 'cancellation_requested' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', "
    "'execution_deadline', 'owner_lost', 'enrollment_fenced') "
    "AND termination_observation IS NULL) "
    # 0160: a completed request may carry a confirmed teardown observation.
    "OR (status = 'completed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'completed') "
    # 0160: enrollment_fenced requires an observation like owner_lost; any
    # other started failure may carry one.
    "OR (status = 'failed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND (terminal_cause NOT IN ('owner_lost', 'enrollment_fenced') "
    "OR termination_observation IS NOT NULL)) "
    "OR (status = 'expired' AND terminal_at IS NOT NULL AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'capacity_wait_expired' "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'execution_deadline' "
    "AND termination_observation IS NOT NULL))) "
    "OR (status = 'cancelled' AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted') AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND termination_observation IS NOT NULL)))) IS TRUE"
)
TEARDOWN_UNCONFIRMED = (
    "teardown_unconfirmed_at IS NULL OR status = 'cancelled' "
    "OR (status IN ('completed', 'failed', 'expired') AND started_at IS NOT NULL)"
)

CUSTODY = "personal_claude_custody"
CUSTODY_CHECKS = (
    ("personal_claude_custody_generation_ck", "generation >= 1"),
    ("personal_claude_custody_version_ck", "version >= 1"),
    (
        "personal_claude_custody_namespace_ck",
        "secret_namespace ~ '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'",
    ),
    (
        "personal_claude_custody_name_ck",
        "secret_name = 'curie-pe-' || replace(enrollment_id::text, '-', '') "
        "|| '-g' || generation::text",
    ),
    ("personal_claude_custody_key_ck", "secret_key = 'CLAUDE_CODE_OAUTH_TOKEN'"),
    (
        "personal_claude_custody_state_ck",
        "state IN ('staged', 'published', 'retiring', 'retired', 'abandoned')",
    ),
    (
        "personal_claude_custody_shape_ck",
        "(((state IN ('staged', 'abandoned') AND published_at IS NULL) "
        "OR (state IN ('published', 'retiring', 'retired') "
        "AND published_at IS NOT NULL AND secret_uid IS NOT NULL)) "
        "AND (state = 'retired') = (retired_at IS NOT NULL) "
        "AND (secret_uid IS NULL OR length(btrim(secret_uid)) > 0)) IS TRUE",
    ),
    # Absence is recorded only after a verified delete, and a retired row has it.
    (
        "personal_claude_custody_absence_ck",
        "((secret_absent_at IS NULL OR state IN ('abandoned', 'retired')) "
        "AND (state <> 'retired' OR secret_absent_at IS NOT NULL)) IS TRUE",
    ),
)
CUSTODY_INDEXES = (
    # One live row per generation; an abandoned attempt may be retried.
    (
        "uq_personal_claude_custody_generation",
        ["enrollment_id", "generation"],
        True,
        "state <> 'abandoned'",
    ),
    ("uq_personal_claude_custody_published", ["enrollment_id"], True, "state = 'published'"),
    # One in-flight staging per owner, so concurrent writers lose before a Secret exists.
    ("uq_personal_claude_custody_staging", ["tenant_id", "principal_id"], True, "state = 'staged'"),
    # Rows still owing a Secret delete or a verified absence.
    (
        "ix_personal_claude_custody_pending",
        ["state", "lease_expires_at"],
        False,
        "state IN ('staged', 'retiring', 'abandoned') AND secret_absent_at IS NULL",
    ),
)


def _definition(name: str) -> str | None:
    shape = (
        op.get_bind()
        .exec_driver_sql(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            f"WHERE conrelid = '{S}.execution_requests'::regclass AND conname = '{name}'"
        )
        .scalar()
    )
    if not isinstance(shape, str) or not shape.startswith("CHECK (") or not shape.endswith(")"):
        return None
    return shape[len("CHECK (") : -1]


def _printed(bodies: Sequence[str]) -> list[str]:
    """How this server prints each body as a CHECK on the request columns.

    Comparing printed forms keeps the recognition independent of the server
    version's formatting. The probe table is temporary and holds no rows.
    """

    bind = op.get_bind()
    bind.exec_driver_sql(
        f"CREATE TEMPORARY TABLE curie_0160_shape_probe (LIKE {S}.execution_requests)"
    )
    try:
        printed = []
        for index, body in enumerate(bodies):
            bind.exec_driver_sql(
                f"ALTER TABLE curie_0160_shape_probe ADD CONSTRAINT probe_{index} CHECK ({body})"
            )
            printed.append(
                bind.exec_driver_sql(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conrelid = 'curie_0160_shape_probe'::regclass "
                    f"AND conname = 'probe_{index}'"
                ).scalar_one()
            )
        return printed
    finally:
        bind.exec_driver_sql("DROP TABLE curie_0160_shape_probe")


def _replace(name: str, body: str) -> None:
    op.drop_constraint(name, "execution_requests", schema=S, type_="check")
    op.create_check_constraint(name, "execution_requests", body, schema=S)


def upgrade() -> None:
    current = [_definition(SHAPE), _definition(TEARDOWN)]
    if any(body is None for body in current) or _printed(
        [body for body in current if body is not None]
    ) != _printed([PRIOR_SHAPE, PRIOR_TEARDOWN]):
        raise RuntimeError("execution request state shape is not the 0159 shape")
    _replace(SHAPE, STATE_SHAPE)
    _replace(TEARDOWN, TEARDOWN_UNCONFIRMED)

    op.create_table(
        CUSTODY,
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("enrollment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("secret_namespace", sa.Text(), nullable=False),
        sa.Column("secret_name", sa.Text(), nullable=False),
        sa.Column("secret_key", sa.Text(), nullable=False),
        sa.Column("secret_uid", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column(
            "staged_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("secret_absent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default="1"),
        sa.PrimaryKeyConstraint("id", name="personal_claude_custody_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name="personal_claude_custody_principal_fkey",
        ),
        *(sa.CheckConstraint(body, name=name) for name, body in CUSTODY_CHECKS),
        schema=S,
    )
    for name, columns, unique, where in CUSTODY_INDEXES:
        op.create_index(
            name, CUSTODY, columns, unique=unique, schema=S, postgresql_where=sa.text(where)
        )


def downgrade() -> None:
    # Held from the check to the restore, so no relaxed row lands in between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(
        sa.text(f"LOCK TABLE {S}.execution_requests, {S}.{CUSTODY} IN ACCESS EXCLUSIVE MODE")
    )
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.{CUSTODY}) "
            f"OR EXISTS (SELECT 1 FROM {S}.execution_requests WHERE "
            "terminal_cause = 'enrollment_fenced' "
            "OR (status = 'completed' AND termination_observation IS NOT NULL) "
            "OR (status = 'failed' AND started_at IS NOT NULL "
            "AND terminal_cause <> 'owner_lost' AND termination_observation IS NOT NULL) "
            "OR (status <> 'cancelled' AND teardown_unconfirmed_at IS NOT NULL))"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("personal_custody_records_present")
    _replace(TEARDOWN, PRIOR_TEARDOWN)
    _replace(SHAPE, PRIOR_SHAPE)
    op.drop_table(CUSTODY, schema=S)

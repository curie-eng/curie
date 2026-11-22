"""Store learned memory rules, their audit log and the work item pins (#5795).

1. ``agents.memory_rule_proposals``: the operator switch that gates proposals.
2. ``memory_rules``: one row per rule owned by an agent and scoped to the agent,
   a binding or a repository, with its lifecycle state, acceptance record and
   one-per-pull-request and one-per-request uniqueness.
3. ``memory_rule_events``: the append-only audit of each state change.
4. ``work_items`` pin columns and ``work_item_memory_rule_snapshots``: the
   accepted rules pinned at admission, mirroring the 0169 coding snapshots.
5. ``execution_requests.memory_rules_evidence``: whether a run loaded them.

Expand only: a release without this revision never reads the new columns or
tables. A hidden candidate names its source request, and its check refuses a
NULL there, so deleting a request (which an older release does when it deletes a
WorkItem or an agent) would fail. A row trigger on ``execution_requests`` deletes
the candidates of a request being deleted, which a release without this revision
needs and a newer one does not notice. The downgrade refuses while any rule,
event, snapshot, pin or evidence exists, then drops everything the upgrade
added, including each agent's ``memory_rule_proposals`` switch.

Revision ID: 0185
Revises: 0184
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0185"
down_revision: str = "0184"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"
LOCK_TIMEOUT_MS = 15000
RULES = "memory_rules"
EVENTS = "memory_rule_events"
SNAPSHOTS = "work_item_memory_rule_snapshots"
CANDIDATE_TRIGGER = "memory_rules_drop_request_candidates"
PIN_CHECK = "work_items_memory_rules_ck"
EVIDENCE_CHECK = "execution_requests_memory_rules_evidence_ck"

_STATES = "'candidate', 'proposed', 'accepted', 'rejected', 'removed', 'superseded', 'discarded'"
_EVENTS = "'propose', 'promote', 'accept', 'reject', 'remove', 'supersede', 'discard'"
_DIGEST = "sha256 = encode(sha256(convert_to(text, 'UTF8')), 'hex')"
_ONE_LINE = "position(chr(13) in {c}) = 0 AND position(chr(10) in {c}) = 0"

PIN_BODY = (
    "((memory_rules_state = 'absent' AND memory_rules_count = 0 "
    "AND memory_rules_sha256 IS NULL AND memory_rules_format_version IS NULL) "
    "OR (memory_rules_state = 'selected' AND memory_rules_count BETWEEN 1 AND 16 "
    "AND memory_rules_sha256 ~ '^[0-9a-f]{64}$' "
    "AND memory_rules_format_version = 1)) IS TRUE"
)


def _uuid(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(name, postgresql.UUID(as_uuid=True), nullable=nullable)


def _stamp(name: str) -> sa.Column:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


def upgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.add_column(
        "agents",
        sa.Column("memory_rule_proposals", sa.Boolean(), nullable=False, server_default=sa.false()),
        schema=S,
    )
    op.create_table(
        RULES,
        _uuid("id"),
        _uuid("tenant_id"),
        _uuid("agent_id"),
        sa.Column("scope_kind", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("scope_label", sa.Text(), nullable=False),
        sa.Column("number", sa.BigInteger(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("sha256", sa.CHAR(64), nullable=False),
        sa.Column("format_version", sa.Integer(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("proposed_by", sa.Text(), nullable=False),
        sa.Column("proposed_at", sa.DateTime(timezone=True), nullable=False),
        _uuid("source_request_id", nullable=True),
        sa.Column("source_turn", sa.Text(), nullable=True),
        _uuid("source_lineage_id", nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        _uuid("accepted_by", nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        _uuid("rejected_by", nullable=True),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
        _uuid("removed_by", nullable=True),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        _stamp("created_at"),
        _stamp("updated_at"),
        sa.PrimaryKeyConstraint("id", name=f"{RULES}_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_id"],
            [f"{S}.agents.tenant_id", f"{S}.agents.id"],
            name=f"{RULES}_tenant_agent_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_request_id"],
            [f"{S}.execution_requests.id"],
            name=f"{RULES}_source_request_fkey",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_lineage_id"],
            [f"{S}.thread_publication_lineages.id"],
            name=f"{RULES}_source_lineage_fkey",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "accepted_by"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name=f"{RULES}_accepted_by_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "rejected_by"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name=f"{RULES}_rejected_by_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "removed_by"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name=f"{RULES}_removed_by_fkey",
        ),
        sa.UniqueConstraint("id", "tenant_id", "agent_id", name=f"{RULES}_id_owner_key"),
        sa.UniqueConstraint("source_request_id", name=f"{RULES}_source_request_key"),
        sa.UniqueConstraint("agent_id", "source_turn", name=f"{RULES}_source_turn_key"),
        sa.CheckConstraint(
            "scope_kind IN ('agent', 'binding', 'repository') AND "
            "((scope_kind = 'agent' AND scope_key = '') "
            "OR (scope_kind <> 'agent' AND length(scope_key) > 0))",
            name=f"{RULES}_scope_ck",
        ),
        sa.CheckConstraint(f"state IN ({_STATES})", name=f"{RULES}_state_ck"),
        sa.CheckConstraint(
            "(number IS NULL) = (state IN ('candidate', 'superseded', 'discarded')) "
            "AND (number IS NULL OR number >= 1)",
            name=f"{RULES}_number_ck",
        ),
        sa.CheckConstraint(
            f"octet_length(text) BETWEEN 1 AND 240 AND {_ONE_LINE.format(c='text')}",
            name=f"{RULES}_text_ck",
        ),
        sa.CheckConstraint(_DIGEST, name=f"{RULES}_sha256_ck"),
        sa.CheckConstraint("format_version = 1", name=f"{RULES}_format_version_ck"),
        sa.CheckConstraint(
            f"octet_length(reason) BETWEEN 1 AND 240 AND {_ONE_LINE.format(c='reason')}",
            name=f"{RULES}_reason_ck",
        ),
        sa.CheckConstraint(
            "(state <> 'candidate' OR source_request_id IS NOT NULL) "
            "AND (source_request_id IS NULL OR source_turn IS NULL)",
            name=f"{RULES}_source_ck",
        ),
        sa.CheckConstraint(
            "((accepted_by IS NOT NULL) = (state IN ('accepted', 'removed'))) "
            "AND ((accepted_at IS NOT NULL) = (state IN ('accepted', 'removed'))) "
            "AND ((rejected_by IS NOT NULL) = (state = 'rejected')) "
            "AND ((rejected_at IS NOT NULL) = (state = 'rejected')) "
            "AND ((removed_by IS NOT NULL) = (state = 'removed')) "
            "AND ((removed_at IS NOT NULL) = (state = 'removed'))",
            name=f"{RULES}_acceptance_ck",
        ),
        sa.CheckConstraint("version >= 1", name=f"{RULES}_version_ck"),
        schema=S,
    )
    op.create_index(
        f"{RULES}_scope_number_key",
        RULES,
        ["agent_id", "scope_kind", "scope_key", "number"],
        unique=True,
        postgresql_where=sa.text("number IS NOT NULL"),
        schema=S,
    )
    op.create_index(
        f"{RULES}_source_lineage_key",
        RULES,
        ["source_lineage_id"],
        unique=True,
        postgresql_where=sa.text(
            "source_lineage_id IS NOT NULL "
            "AND state IN ('proposed', 'accepted', 'rejected', 'removed')"
        ),
        schema=S,
    )
    op.create_index(
        f"ix_{RULES}_scope_state",
        RULES,
        ["agent_id", "scope_kind", "scope_key", "state"],
        schema=S,
    )
    op.create_table(
        EVENTS,
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        _uuid("rule_id"),
        _uuid("tenant_id"),
        _uuid("agent_id"),
        sa.Column("event", sa.Text(), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("actor_kind", sa.Text(), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        _uuid("principal_id", nullable=True),
        _stamp("created_at"),
        sa.ForeignKeyConstraint(
            ["rule_id", "tenant_id", "agent_id"],
            [f"{S}.{RULES}.id", f"{S}.{RULES}.tenant_id", f"{S}.{RULES}.agent_id"],
            name=f"{EVENTS}_rule_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name=f"{EVENTS}_principal_fkey",
        ),
        sa.CheckConstraint(f"event IN ({_EVENTS})", name=f"{EVENTS}_event_ck"),
        sa.CheckConstraint(
            f"(from_state IS NULL OR from_state IN ({_STATES})) AND to_state IN ({_STATES}) "
            "AND ((event = 'propose') = (from_state IS NULL))",
            name=f"{EVENTS}_state_ck",
        ),
        sa.CheckConstraint(
            "actor_kind IN ('agent', 'platform', 'principal') "
            "AND ((actor_kind = 'principal') = (principal_id IS NOT NULL))",
            name=f"{EVENTS}_actor_ck",
        ),
        schema=S,
    )
    op.create_index(f"ix_{EVENTS}_rule", EVENTS, ["rule_id", "id"], schema=S)
    op.execute(
        sa.text(
            f"CREATE FUNCTION {S}.{CANDIDATE_TRIGGER}() RETURNS trigger LANGUAGE plpgsql AS $$ "
            f"BEGIN DELETE FROM {S}.{RULES} "
            "WHERE source_request_id = OLD.id AND state = 'candidate'; RETURN OLD; END $$"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER {CANDIDATE_TRIGGER} BEFORE DELETE ON {S}.execution_requests "
            f"FOR EACH ROW EXECUTE FUNCTION {S}.{CANDIDATE_TRIGGER}()"
        )
    )

    op.add_column(
        "work_items",
        sa.Column("memory_rules_state", sa.Text(), nullable=False, server_default="absent"),
        schema=S,
    )
    op.add_column(
        "work_items",
        sa.Column("memory_rules_count", sa.Integer(), nullable=False, server_default="0"),
        schema=S,
    )
    op.add_column("work_items", sa.Column("memory_rules_sha256", sa.CHAR(64)), schema=S)
    op.add_column("work_items", sa.Column("memory_rules_format_version", sa.Integer()), schema=S)
    op.create_check_constraint(PIN_CHECK, "work_items", PIN_BODY, schema=S)
    op.create_table(
        SNAPSHOTS,
        sa.Column(
            "work_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{S}.work_items.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("rule_numbers", postgresql.ARRAY(sa.BigInteger()), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("sha256", sa.CHAR(64), nullable=False),
        sa.CheckConstraint(
            "cardinality(rule_numbers) BETWEEN 1 AND 16", name=f"{SNAPSHOTS}_numbers_ck"
        ),
        sa.CheckConstraint("octet_length(text) <= 4096", name=f"{SNAPSHOTS}_text_ck"),
        sa.CheckConstraint(_DIGEST, name=f"{SNAPSHOTS}_sha256_ck"),
        schema=S,
    )
    op.add_column(
        "execution_requests",
        sa.Column("memory_rules_evidence", sa.Text(), nullable=False, server_default="none"),
        schema=S,
    )
    op.create_check_constraint(
        EVIDENCE_CHECK,
        "execution_requests",
        "memory_rules_evidence IN ('none', 'pending', 'applied', 'failed')",
        schema=S,
    )


def downgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(
        sa.text(
            f"LOCK TABLE {S}.{RULES}, {S}.{EVENTS}, {S}.{SNAPSHOTS}, {S}.work_items, "
            f"{S}.execution_requests IN ACCESS EXCLUSIVE MODE"
        )
    )
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.{RULES}) "
            f"OR EXISTS (SELECT 1 FROM {S}.{EVENTS}) "
            f"OR EXISTS (SELECT 1 FROM {S}.{SNAPSHOTS}) "
            f"OR EXISTS (SELECT 1 FROM {S}.work_items WHERE memory_rules_state <> 'absent') "
            f"OR EXISTS (SELECT 1 FROM {S}.execution_requests "
            "WHERE memory_rules_evidence <> 'none')"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("memory_rules_present")
    op.execute(sa.text(f"DROP TRIGGER {CANDIDATE_TRIGGER} ON {S}.execution_requests"))
    op.execute(sa.text(f"DROP FUNCTION {S}.{CANDIDATE_TRIGGER}()"))
    op.drop_constraint(EVIDENCE_CHECK, "execution_requests", schema=S)
    op.drop_column("execution_requests", "memory_rules_evidence", schema=S)
    op.drop_table(SNAPSHOTS, schema=S)
    op.drop_constraint(PIN_CHECK, "work_items", schema=S)
    for name in (
        "memory_rules_format_version",
        "memory_rules_sha256",
        "memory_rules_count",
        "memory_rules_state",
    ):
        op.drop_column("work_items", name, schema=S)
    op.drop_table(EVENTS, schema=S)
    op.drop_table(RULES, schema=S)
    op.drop_column("agents", "memory_rule_proposals", schema=S)

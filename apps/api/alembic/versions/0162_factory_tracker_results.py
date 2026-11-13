"""Factory tracker result policy, grants, deliveries, receipts and artifacts (#5585).

ADR 0277. ``agent_versions`` gains the tracker result policy identity: the
declaration's schema version and the SHA-256 of its canonical JSON, both absent
or both set. Nothing writes them yet; ingestion refuses every declaration.

Four tables, in foreign key order:

1. ``factory_tracker_result_grants``: the authority one execution request
   holds to deliver one result, with its source fingerprint, repository,
   allowed result kinds (stored sorted), output identity, implementation label
   and expiry.
2. ``factory_tracker_result_deliveries``: the delivery state of one submitted
   result (owed, delivered or failed), its attempt lease, its error codes and
   the linked issue it produced.
3. ``factory_tracker_result_receipts``: one per delivered result.
4. ``factory_tracker_result_artifacts``: the private transport copy of the
   submitted proposal, the only column that holds submitted text.

Every other column holds identities, digests, codes, timestamps or references:
codes are CHECK constrained to ``^[a-z][a-z0-9_]{0,63}$`` so no free text can
land. The downgrade refuses, before any DDL, while any of the four tables has a
row or any agent version carries a result policy.

Revision ID: 0162
Revises: 0160
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0162"
down_revision: str = "0160"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

GRANTS = "factory_tracker_result_grants"
DELIVERIES = "factory_tracker_result_deliveries"
RECEIPTS = "factory_tracker_result_receipts"
ARTIFACTS = "factory_tracker_result_artifacts"
TABLES = (GRANTS, DELIVERIES, RECEIPTS, ARTIFACTS)

HEX64 = "'^[0-9a-f]{64}$'"
CODE = "'^[a-z][a-z0-9_]{0,63}$'"
FINDINGS_ONLY = "'[\"findings\"]'::jsonb"
FINDINGS_AND_TICKET = '\'["findings", "ticket"]\'::jsonb'
KINDS = "'ticket', 'findings'"
REASONS = (
    "'not_reproduced', 'already_fixed', 'product_decision_needed', "
    "'capability_unavailable', 'budget_exhausted'"
)
STATES = "'owed', 'delivered', 'failed'"
OUTPUT_COLUMNS = (
    "output_tracker_kind, output_tracker_host, output_scope_id, "
    "output_issue_id, output_url, output_body_sha256"
)


def _uuid_fk(name: str, target: str, **kwargs: object) -> sa.Column:
    return sa.Column(
        name,
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey(f"{S}.{target}", ondelete="CASCADE"),
        **kwargs,
    )


def _now(name: str) -> sa.Column:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


def _output_columns() -> list[sa.Column]:
    return [
        sa.Column(name, sa.Text(), nullable=True)
        for name in (
            "output_tracker_kind",
            "output_tracker_host",
            "output_scope_id",
            "output_issue_id",
            "output_url",
            "output_body_sha256",
        )
    ]


def upgrade() -> None:
    op.add_column(
        "agent_versions",
        sa.Column("result_policy_schema_version", sa.Integer(), nullable=True),
        schema=S,
    )
    op.add_column(
        "agent_versions", sa.Column("result_policy_digest", sa.Text(), nullable=True), schema=S
    )
    op.create_check_constraint(
        "agent_versions_result_policy_ck",
        "agent_versions",
        "(result_policy_schema_version IS NULL AND result_policy_digest IS NULL) "
        "OR (result_policy_schema_version IS NOT NULL "
        "AND result_policy_schema_version = 1 "
        "AND result_policy_digest IS NOT NULL "
        f"AND result_policy_digest ~ {HEX64})",
        schema=S,
    )

    op.create_table(
        GRANTS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        _uuid_fk("work_item_id", "work_items.id", nullable=False),
        _uuid_fk("execution_request_id", "execution_requests.id", nullable=False),
        sa.Column("source_fingerprint", sa.Text(), nullable=False),
        sa.Column("repository_id", sa.Text(), nullable=False),
        sa.Column("allowed_result_kinds", postgresql.JSONB(), nullable=False),
        sa.Column("output_identity", sa.Text(), nullable=False),
        sa.Column("implementation_label", sa.String(50), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        _now("created_at"),
        sa.UniqueConstraint("execution_request_id", name=f"{GRANTS}_execution_request_id_key"),
        sa.UniqueConstraint("output_identity", name=f"{GRANTS}_output_identity_key"),
        sa.CheckConstraint(f"source_fingerprint ~ {HEX64}", name=f"{GRANTS}_source_fingerprint_ck"),
        sa.CheckConstraint("length(btrim(repository_id)) > 0", name=f"{GRANTS}_repository_id_ck"),
        sa.CheckConstraint(
            f"allowed_result_kinds IN ({FINDINGS_ONLY}, {FINDINGS_AND_TICKET})",
            name=f"{GRANTS}_allowed_result_kinds_ck",
        ),
        sa.CheckConstraint(
            "output_identity ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'",
            name=f"{GRANTS}_output_identity_ck",
        ),
        sa.CheckConstraint(
            f"(implementation_label IS NULL) = (allowed_result_kinds = {FINDINGS_ONLY})",
            name=f"{GRANTS}_implementation_label_ck",
        ),
        sa.CheckConstraint(
            "implementation_label IS NULL OR length(implementation_label) BETWEEN 1 AND 50",
            name=f"{GRANTS}_implementation_label_length_ck",
        ),
        schema=S,
    )
    op.create_index(f"ix_{GRANTS}_work_item_id", GRANTS, ["work_item_id"], schema=S)

    op.create_table(
        DELIVERIES,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        _uuid_fk("grant_id", f"{GRANTS}.id", nullable=False),
        _uuid_fk("execution_request_id", "execution_requests.id", nullable=False),
        sa.Column("result_kind", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("proposal_sha256", sa.Text(), nullable=False),
        sa.Column("runtime_epoch", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False, server_default="owed"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("attempt_owner", sa.Text(), nullable=True),
        sa.Column("attempt_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        _now("next_attempt_at"),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("failure_code", sa.Text(), nullable=True),
        sa.Column("create_attempted_at", sa.DateTime(timezone=True), nullable=True),
        *_output_columns(),
        _now("submitted_at"),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        _now("created_at"),
        _now("updated_at"),
        sa.UniqueConstraint("grant_id", name=f"{DELIVERIES}_grant_id_key"),
        sa.CheckConstraint(f"result_kind IN ({KINDS})", name=f"{DELIVERIES}_result_kind_ck"),
        sa.CheckConstraint(
            "(result_kind = 'findings') = (reason IS NOT NULL)",
            name=f"{DELIVERIES}_reason_presence_ck",
        ),
        sa.CheckConstraint(
            f"reason IS NULL OR reason IN ({REASONS})", name=f"{DELIVERIES}_reason_ck"
        ),
        sa.CheckConstraint(f"proposal_sha256 ~ {HEX64}", name=f"{DELIVERIES}_proposal_sha256_ck"),
        sa.CheckConstraint(
            f"output_body_sha256 IS NULL OR output_body_sha256 ~ {HEX64}",
            name=f"{DELIVERIES}_output_body_sha256_ck",
        ),
        sa.CheckConstraint(f"state IN ({STATES})", name=f"{DELIVERIES}_state_ck"),
        sa.CheckConstraint(
            "(state = 'owed') = (settled_at IS NULL)", name=f"{DELIVERIES}_settled_ck"
        ),
        sa.CheckConstraint(
            "(state = 'failed') = (failure_code IS NOT NULL)", name=f"{DELIVERIES}_failure_ck"
        ),
        sa.CheckConstraint(
            f"last_error_code IS NULL OR last_error_code ~ {CODE}",
            name=f"{DELIVERIES}_last_error_code_ck",
        ),
        sa.CheckConstraint(
            f"failure_code IS NULL OR failure_code ~ {CODE}",
            name=f"{DELIVERIES}_failure_code_ck",
        ),
        sa.CheckConstraint("attempts >= 0", name=f"{DELIVERIES}_attempts_ck"),
        sa.CheckConstraint(
            "(attempt_owner IS NULL) = (attempt_lease_expires_at IS NULL)",
            name=f"{DELIVERIES}_lease_ck",
        ),
        sa.CheckConstraint(
            f"num_nonnulls({OUTPUT_COLUMNS}) IN (0, 6)", name=f"{DELIVERIES}_output_ck"
        ),
        sa.CheckConstraint("deadline_at > submitted_at", name=f"{DELIVERIES}_deadline_ck"),
        schema=S,
    )
    op.create_index(
        f"ix_{DELIVERIES}_owed",
        DELIVERIES,
        ["next_attempt_at"],
        schema=S,
        postgresql_where=sa.text("state = 'owed'"),
    )
    op.create_index(
        f"ix_{DELIVERIES}_execution_request_id",
        DELIVERIES,
        ["execution_request_id"],
        schema=S,
    )

    op.create_table(
        RECEIPTS,
        _uuid_fk("delivery_id", f"{DELIVERIES}.id", primary_key=True),
        sa.Column("grant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("execution_request_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("result_kind", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("proposal_sha256", sa.Text(), nullable=False),
        sa.Column("revision", sa.Text(), nullable=True),
        sa.Column("result_comment_id", sa.Text(), nullable=False),
        *_output_columns(),
        sa.Column("output_labels", postgresql.JSONB(), nullable=True),
        _now("delivered_at"),
        # The shape check also admits only the two result kinds.
        sa.CheckConstraint(
            f"reason IS NULL OR reason IN ({REASONS})", name=f"{RECEIPTS}_reason_ck"
        ),
        sa.CheckConstraint(
            f"(result_kind = 'ticket' AND num_nonnulls({OUTPUT_COLUMNS}, output_labels) = 7 "
            "AND reason IS NULL) "
            f"OR (result_kind = 'findings' AND num_nonnulls({OUTPUT_COLUMNS}, output_labels) = 0 "
            "AND reason IS NOT NULL)",
            name=f"{RECEIPTS}_shape_ck",
        ),
        sa.CheckConstraint(f"proposal_sha256 ~ {HEX64}", name=f"{RECEIPTS}_proposal_sha256_ck"),
        sa.CheckConstraint(
            f"output_body_sha256 IS NULL OR output_body_sha256 ~ {HEX64}",
            name=f"{RECEIPTS}_output_body_sha256_ck",
        ),
        sa.CheckConstraint(
            "revision IS NULL OR revision ~ '^[0-9a-f]{40}$'", name=f"{RECEIPTS}_revision_ck"
        ),
        schema=S,
    )

    op.create_table(
        ARTIFACTS,
        _uuid_fk("delivery_id", f"{DELIVERIES}.id", primary_key=True),
        sa.Column("content", postgresql.JSONB(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        _now("created_at"),
        sa.CheckConstraint("jsonb_typeof(content) = 'object'", name=f"{ARTIFACTS}_content_ck"),
        schema=S,
    )


def downgrade() -> None:
    # Refused while any result state or result policy exists: dropping it
    # would lose owed deliveries, their receipts and the policy identity a
    # re-upgrade could not reconstruct.
    present = " OR ".join(f"EXISTS (SELECT 1 FROM {S}.{table})" for table in TABLES)
    if op.get_bind().scalar(
        sa.text(
            f"SELECT {present} OR EXISTS (SELECT 1 FROM {S}.agent_versions "
            "WHERE result_policy_schema_version IS NOT NULL "
            "OR result_policy_digest IS NOT NULL)"
        )
    ):
        raise RuntimeError("factory_tracker_result_state_present")
    op.drop_table(ARTIFACTS, schema=S)
    op.drop_table(RECEIPTS, schema=S)
    op.drop_index(f"ix_{DELIVERIES}_execution_request_id", table_name=DELIVERIES, schema=S)
    op.drop_index(f"ix_{DELIVERIES}_owed", table_name=DELIVERIES, schema=S)
    op.drop_table(DELIVERIES, schema=S)
    op.drop_index(f"ix_{GRANTS}_work_item_id", table_name=GRANTS, schema=S)
    op.drop_table(GRANTS, schema=S)
    op.drop_constraint("agent_versions_result_policy_ck", "agent_versions", schema=S)
    op.drop_column("agent_versions", "result_policy_digest", schema=S)
    op.drop_column("agent_versions", "result_policy_schema_version", schema=S)

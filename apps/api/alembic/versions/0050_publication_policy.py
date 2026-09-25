"""Per-agent publication policy, defaulting every existing agent to approve.

Revision ID: 0050
Revises: 0049
Create Date: 2026-09-22

ADR 0147. Existing rows stay on human approval at policy version 1. The
approval audit may record a platform principal. Publication rows snapshot the
draft and branch-prefix bounds that authorized them.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0050"
down_revision: str | None = "0049"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
PREFIX_SQL = (
    "char_length({column}) BETWEEN 2 AND 64 "
    "AND {column} ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,62}}/$' "
    "AND {column} NOT LIKE '%..%' "
    "AND {column} NOT LIKE '%.lock/' "
    "AND {column} NOT LIKE '%./'"
)


def _prefix_check(column: str) -> str:
    rendered = PREFIX_SQL.format(column=column)
    return f"{column} IS NULL OR ({rendered})"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column(
            "publication_policy",
            sa.String(),
            nullable=False,
            server_default="approve",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column(
            "publication_policy_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column(
            "publication_draft",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column("publication_branch_prefix", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "agents_publication_policy_ck",
        "agents",
        "publication_policy IN ('approve', 'auto')",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "agents_publication_policy_version_ck",
        "agents",
        "publication_policy_version >= 1",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "agents_publication_branch_prefix_ck",
        "agents",
        _prefix_check("publication_branch_prefix"),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("policy_identity", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("policy_version", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "approvals_policy_identity_ck",
        "approvals",
        "(policy_identity IS NULL AND policy_version IS NULL) OR "
        "(policy_identity = 'publication:auto' AND policy_version >= 1)",
        schema=SCHEMA,
    )
    op.add_column(
        "publications",
        sa.Column(
            "open_as_draft",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "publications",
        sa.Column("branch_prefix", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "publications_branch_prefix_ck",
        "publications",
        _prefix_check("branch_prefix"),
        schema=SCHEMA,
    )
    op.drop_constraint(
        "approval_audit_principal_kind_ck",
        "approval_audit_entries",
        type_="check",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "approval_audit_principal_kind_ck",
        "approval_audit_entries",
        "principal_kind IS NULL OR principal_kind IN "
        "('chat', 'console', 'operator', 'adapter', 'platform')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint(
        "approval_audit_principal_kind_ck",
        "approval_audit_entries",
        type_="check",
        schema=SCHEMA,
    )
    op.execute(
        "UPDATE curie.approval_audit_entries SET principal_kind = NULL "
        "WHERE principal_kind = 'platform'"
    )
    op.create_check_constraint(
        "approval_audit_principal_kind_ck",
        "approval_audit_entries",
        "principal_kind IS NULL OR principal_kind IN "
        "('chat', 'console', 'operator', 'adapter')",
        schema=SCHEMA,
    )
    op.drop_constraint(
        "publications_branch_prefix_ck", "publications", type_="check", schema=SCHEMA
    )
    op.drop_column("publications", "branch_prefix", schema=SCHEMA)
    op.drop_column("publications", "open_as_draft", schema=SCHEMA)
    op.drop_constraint("approvals_policy_identity_ck", "approvals", type_="check", schema=SCHEMA)
    op.drop_column("approvals", "policy_version", schema=SCHEMA)
    op.drop_column("approvals", "policy_identity", schema=SCHEMA)
    op.drop_constraint(
        "agents_publication_branch_prefix_ck", "agents", type_="check", schema=SCHEMA
    )
    op.drop_constraint(
        "agents_publication_policy_version_ck", "agents", type_="check", schema=SCHEMA
    )
    op.drop_constraint("agents_publication_policy_ck", "agents", type_="check", schema=SCHEMA)
    op.drop_column("agents", "publication_branch_prefix", schema=SCHEMA)
    op.drop_column("agents", "publication_draft", schema=SCHEMA)
    op.drop_column("agents", "publication_policy_version", schema=SCHEMA)
    op.drop_column("agents", "publication_policy", schema=SCHEMA)

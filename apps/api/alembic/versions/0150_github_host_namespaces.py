"""GitHub account namespaces for factory Slack steering (#5414).

The ``github_host`` namespace kind holds operator mapped GitHub accounts, one
namespace per host, keyed by the host, with the numeric user id as the provider
native id (ADR 0198). Factory Slack steering reads a person's link here to name
the account whose pull request authority it applies.

Revision ID: 0150
Revises: 0149
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0150"
down_revision: str = "0149"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

KINDS_0149 = "kind IN ('slack_workspace', 'email_domain', 'slack_enterprise')"
KINDS_0150 = "kind IN ('slack_workspace', 'email_domain', 'slack_enterprise', 'github_host')"
GITHUB_HOST_KEY = (
    "kind <> 'github_host' OR (provider = 'github' AND authority <> '' AND key = authority)"
)


def upgrade() -> None:
    op.drop_constraint("identity_namespaces_kind_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_kind_ck", "identity_namespaces", KINDS_0150, schema=S
    )
    op.create_check_constraint(
        "identity_namespaces_github_host_key_ck", "identity_namespaces", GITHUB_HOST_KEY, schema=S
    )


def downgrade() -> None:
    # Refused, before any DDL, while an operator mapped GitHub account exists:
    # the 0149 check cannot hold its namespace, and dropping it would lose
    # links a re-upgrade cannot rebuild.
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.identity_namespaces WHERE kind = 'github_host')"
            " OR EXISTS (SELECT 1 FROM curie.identity_links l"
            " JOIN curie.identity_namespaces n ON n.id = l.identity_namespace_id"
            " WHERE n.kind = 'github_host')"
        )
    ):
        raise RuntimeError("github_host_namespaces_present")
    op.drop_constraint("identity_namespaces_github_host_key_ck", "identity_namespaces", schema=S)
    op.drop_constraint("identity_namespaces_kind_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_kind_ck", "identity_namespaces", KINDS_0149, schema=S
    )

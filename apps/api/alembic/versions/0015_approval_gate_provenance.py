"""approval gate provenance columns (#544, Decision C)

Adds approvals.gate_kind and approvals.granted_tool: the durable provenance the
runner writes so the worker branches on a COLUMN instead of sniffing the summary
prefix. ``gate_kind`` is ``'permission'`` (the tool-permission gate denied a real
tool call) or ``'policy'`` (the model asked for a business-decision approval);
``granted_tool`` is the tool name a resume-turn grant is bound to, and is only
ever set for a permission gate (a policy gate never authorizes a tool).

Backfill classifies rows at rest so a pending-across-deploy approval carries
provenance the moment the migration runs:
  * a summary with the reserved permission-gate prefix is a genuine permission
    block (the runner is the only writer of that namespace) -> 'permission' plus
    the tool name parsed out of it;
  * everything else -> 'policy' with granted_tool NULL, the SAFE direction: a
    pending policy approval created before this change becomes the conservative
    "no grant" case, never a surprise grant.

Both columns are nullable and additive.

Revision ID: 0015
Revises: 0014
Create Date: 2026-07-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Duplicated (not imported) from the worker's binding._PERMISSION_GATE_SUMMARY_PREFIX
# and the runner's summarize_tool_call. Length 29; the backfill parses the tool
# name out of the substring that follows it.
_PERMISSION_GATE_SUMMARY_PREFIX = "Tool call awaiting approval: "
_PREFIX_LEN = len(_PERMISSION_GATE_SUMMARY_PREFIX)


def upgrade() -> None:
    op.add_column(
        "approvals",
        sa.Column("gate_kind", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("granted_tool", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    # Permission-gate rows: the prefixed summary is the reserved namespace only
    # the runner writes. substring(... from _PREFIX_LEN + 1) is 1-indexed, so it
    # is the first character AFTER the 29-char prefix; split_part(... ' ', 1)
    # takes the tool name up to the first space (the tool-input rendering).
    op.execute(
        sa.text(
            f"UPDATE {SCHEMA}.approvals "
            "SET gate_kind = 'permission', "
            f"    granted_tool = split_part("
            f"        substring(summary from {_PREFIX_LEN + 1}), ' ', 1) "
            "WHERE summary LIKE :prefix"
        ).bindparams(prefix=_PERMISSION_GATE_SUMMARY_PREFIX + "%")
    )
    # Non-NULL summaries without the prefix are policy gates; granted_tool stays
    # NULL (the safe no-grant direction). Set explicitly rather than relying on
    # the column default. Note `summary NOT LIKE :prefix` is FALSE for a NULL
    # summary in SQL, so any NULL-summary rows are matched by NEITHER UPDATE and
    # are left with gate_kind NULL -- they fall to the runtime prefix-parse /
    # rolling-window path, which is itself the conservative no-grant case.
    op.execute(
        sa.text(
            f"UPDATE {SCHEMA}.approvals "
            "SET gate_kind = 'policy', granted_tool = NULL "
            "WHERE summary NOT LIKE :prefix"
        ).bindparams(prefix=_PERMISSION_GATE_SUMMARY_PREFIX + "%")
    )
    # Defense-in-depth: gate_kind is trusted in a worker security branch
    # (binding.py: `if gate_kind == "policy": return None`), so pin the column
    # to the two literals the runner ever writes. Added AFTER the backfill so
    # every existing row already conforms. NULL is still allowed on purpose --
    # `NULL IN (...)` is NULL (not FALSE) in Postgres, so the CHECK passes for
    # the rolling-window / old-runner / pre-backfill NULL rows.
    op.create_check_constraint(
        "ck_approvals_gate_kind",
        "approvals",
        "gate_kind IN ('permission', 'policy')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    # Drop the CHECK before the columns it references.
    op.drop_constraint("ck_approvals_gate_kind", "approvals", schema=SCHEMA, type_="check")
    op.drop_column("approvals", "granted_tool", schema=SCHEMA)
    op.drop_column("approvals", "gate_kind", schema=SCHEMA)

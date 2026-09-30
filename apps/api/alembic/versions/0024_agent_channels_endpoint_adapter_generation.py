"""a binding gets its server-controlled route and its rebind generation (#1459)

`endpoint` is where this kind's replies go back through, `adapter` names the
egress identity whose credential authenticates them, and `generation` counts
rebinds. All three are platform-set: an ingress request body never supplies them.

The backfill is trivially all-NULL/zero, because no binding has a route today and
a NULL endpoint means "the worker's configured default", which is exactly today's
behavior for Slack. That is also why the cutover has to reconfigure every non-Slack
binding before ingress restarts: each one comes out of this migration UNROUTABLE.

`generation` is NOT NULL at 0 rather than nullable: a NULL generation cannot be
compared against a credential's claim, so a nullable column would make the rebind
check silently vacuous for every pre-0024 row. Its `server_default` of 0 STAYS
(unlike 0022's `reply_kind`, which has none): zero is the honest starting count
for any binding, including one written out of band, whereas a fabricated channel
kind is a lie. Nothing is lost by defaulting a counter that begins at zero.

**`agent_channels_route_pair_ck` states the pair invariant at the DATABASE**, not
only in `ChannelBinding`. The two columns are independently nullable, so a
half-configured route is representable, and a row written out of band (a restored
dump, an operator's psql, a future code path) would pass ingress and then fail
inside the worker, far from its cause. The CHECK is deliberately weaker than the
application rule -- it says only that both are set or neither is, never that a
non-Slack kind REQUIRES both. The database states the invariant true for every
kind; the schema layer states the kind-specific policy. A CHECK that knew what
`slack` meant would be the schema layer, in the wrong place.

**This revision also finishes 0022's backfill**, because it owns the column that
one needed. 0022 gave every approval a `reply_kind` and left `reply_adapter` NULL;
NULL is honest for Slack and a latent failure for any other kind, whose resume
would reach the HTTP sink with no egress credential named. Once `adapter` exists
here, a non-Slack approval can be judged: since this revision creates `adapter`
NULL, none of them has adapter provenance anywhere, so the migration REFUSES and
names them rather than leaving a row that resumes with no credential. Slack rows
keep their NULL by design. That is 0022's discipline -- refuse, never guess --
applied to the half of the routing identity that lives on this revision.

`downgrade` REFUSES in TWO directions, both of which the drop would destroy
silently:

1. any binding carrying a non-NULL `endpoint` or `adapter` -- dropping those
   columns destroys the only record of where a live adapter's traffic goes and
   which credential authenticates it, a re-upgrade cannot reconstruct either
   (this backfill is all-NULL by construction), and a worker running against a
   downgraded schema fails closed on every non-Slack turn; and
2. any binding whose `generation` is nonzero -- a token claim binds
   `(channel_id, generation)`, and `update_agent_binding` mutates the row in
   place, so the id is a STABLE identity and the generation is the only thing
   that revokes a credential minted before a rebind. Dropping the column and
   re-upgrading resets every counter to 0, which makes every previously revoked
   generation-0 token valid again against the same binding id: revocation
   undone, with no trace in either schema.

When nothing is routed and every generation is 0 it drops in REVERSE CREATION
ORDER -- the CHECK first, then `generation`, `adapter`, `endpoint` -- so the
constraint never outlives a column it references, and no orphaned constraint
blocks a re-upgrade's own ADD CONSTRAINT.

Revision ID: 0024
Revises: 0023
Create Date: 2026-08-13
"""

from collections.abc import Sequence
from urllib.parse import urlsplit

import sqlalchemy as sa
from alembic import op
from curie_api.migration_fence import (
    AUDIT_COLUMNS_AT_0013,
    UnreconstructableRow,
    fence_identity_tables,
    honor_declarations,
    load_declarations,
    report_and_guidance,
)

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "agent_channels"
APPROVALS = "approvals"

# The one kind whose reply route is legitimately implicit: Slack replies go
# through the worker's configured Slack origin, so a Slack approval with no
# adapter is complete, and any other kind with no adapter is not.
SLACK = "slack"

# Named explicitly, following 0021's discipline, so the API can translate it and
# the test can look it up by identity rather than by shape.
ROUTE_PAIR_CHECK = "agent_channels_route_pair_ck"


def _redacted(endpoint: str | None) -> str:
    """An endpoint reduced to scheme and host, for a message a human will read.

    An endpoint can carry a token in its path or query (the write path forbids
    userinfo, but a row written out of band predates that rule), and a migration
    failure is copied into tickets and CI logs. Scheme plus host is enough for an
    operator to recognize which route is meant.
    """

    if not endpoint:
        return "unset"
    parsed = urlsplit(endpoint)
    if not parsed.scheme or not parsed.hostname:
        return "an unparseable endpoint"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def upgrade() -> None:
    conn = op.get_bind()

    # The identity fence, same two tables in the same order as 0022 and
    # `0037_multibinding_state_identity.py`, so no two of them can deadlock.
    # Everything below commits as one unit behind it, and a concurrent binding
    # write or approval insert blocks and then succeeds rather than being
    # refused. ACCESS EXCLUSIVE, up front: the ADD COLUMNs below need it, and
    # taking it later would be a lock upgrade -- see `migration_fence`.
    fence_identity_tables(conn)

    op.add_column(TABLE, sa.Column("endpoint", sa.String(), nullable=True), schema=SCHEMA)
    op.add_column(TABLE, sa.Column("adapter", sa.String(), nullable=True), schema=SCHEMA)
    op.add_column(
        TABLE,
        sa.Column("generation", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )

    op.create_check_constraint(
        ROUTE_PAIR_CHECK,
        TABLE,
        "(endpoint IS NULL) = (adapter IS NULL)",
        schema=SCHEMA,
    )

    # 0022's other half, landed here because `adapter` did not exist until three
    # statements ago. 0022 gave every approval a `reply_kind` by provenance and
    # left `reply_adapter` NULL for all of them. NULL is right for a Slack row,
    # and WRONG for any other kind: `resumequeue` rebuilds the resume turn from
    # the approval row, so a non-Slack approval with no adapter reaches the
    # worker's HTTP sink with no egress-credential selector, fails closed
    # mid-resume, and escalates far from anything an operator can connect it to.
    #
    # There is no backfill arm, only a refusal: `adapter` was created NULL three
    # statements up, and `downgrade` refuses while any route is set, so no
    # database reaching this line can have a binding that names an adapter. A
    # pre-existing non-Slack approval therefore has no adapter provenance
    # ANYWHERE in the schema, and the only honest answer is to stop.
    #
    # `ap.reply_adapter IS NULL` is part of the question, not an optimization: a
    # row 0022 recovered by declaration already carries its egress identity ON
    # THE APPROVAL, which is where `resumequeue` reads it from. It is routable,
    # so it is not unroutable, and dropping it from this set is what lets ONE
    # mounted document carry an operator through `alembic upgrade head`.
    unroutable = conn.execute(
        sa.text(
            f"""
            SELECT ap.id AS id,
                   ap.reply_channel AS reply_channel,
                   ap.reply_kind AS reply_kind,
                   ap.status AS status
            FROM {SCHEMA}.{APPROVALS} ap
            WHERE ap.reply_kind <> :slack
              AND ap.reply_adapter IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM {SCHEMA}.{TABLE} c
                  WHERE c.kind = ap.reply_kind
                    AND c.address = ap.reply_channel
                    AND c.adapter IS NOT NULL
              )
            ORDER BY ap.id
            """
        ),
        {"slack": SLACK},
    ).all()
    # The declaration arm, for exactly the rows above. 0024's unreconstructable
    # question is a different one from 0022's -- a non-Slack approval whose
    # reply has no egress identity anywhere in the schema -- and it takes the
    # same disposition: an operator vouches per row, one audit row records it,
    # and nothing is settled or deleted to get the migration through.
    rows = [
        UnreconstructableRow(
            approval_id=str(row.id),
            reply_channel=row.reply_channel,
            status=row.status,
            reason=(
                f"no {row.reply_kind!r} binding at its address names an adapter, "
                "and this revision creates the adapter column NULL"
            ),
        )
        for row in unroutable
    ]
    # What 0022 already established for each of these rows. The kind is a
    # recovered fact about the ORIGINAL turn; this revision is missing only the
    # adapter half, so a declaration that names a different kind is refused and
    # one that agrees recovers the adapter alone.
    established_kinds = {str(row.id): row.reply_kind for row in unroutable}
    honored = honor_declarations(
        conn,
        unreconstructable={row.approval_id: row.reason for row in rows},
        declarations=load_declarations(),
        established_kinds=established_kinds,
        revision=revision,
        audit_columns=AUDIT_COLUMNS_AT_0013,
    )
    rows = [row for row in rows if row.approval_id not in honored]

    # The refusal carries the whole disposition, for the same reason 0022's
    # does: a blocked installation is on a pre-head schema, so the API that
    # serves the identity report cannot start against it, and this Job's log is
    # the only place the operator can be told what to do.
    if rows:
        detail = "; ".join(
            f"{row.approval_id} (reply_channel {row.reply_channel!r}, "
            f"status {row.status})"
            for row in rows
        )
        raise RuntimeError(
            "cannot complete the approval routing backfill (#1459): these "
            "approvals were raised on a non-Slack channel and no binding names "
            f"the egress identity their reply must be authenticated with -- {detail}. "
            "A pre-existing non-Slack approval has no adapter provenance anywhere "
            "in the schema; resuming it would POST the reply with no credential "
            "and fail closed inside the worker, days after the fact and far from "
            "any request. Configuring the bindings first is NOT an option -- the "
            "endpoint and adapter columns those routes live in are created by this "
            "very revision, so no route can exist until it completes. "
            + report_and_guidance(rows, revision=revision)
        )


def downgrade() -> None:
    conn = op.get_bind()

    routed = conn.execute(
        sa.text(
            f"""
            SELECT kind, address, endpoint, adapter
            FROM {SCHEMA}.{TABLE}
            WHERE endpoint IS NOT NULL OR adapter IS NOT NULL
            ORDER BY kind, address
            """
        )
    ).all()
    if routed:
        detail = "; ".join(
            f"{row.kind}:{row.address} (endpoint {_redacted(row.endpoint)}, "
            f"adapter {row.adapter})"
            for row in routed
        )
        raise RuntimeError(
            "cannot drop the binding route columns (#1459): these bindings would "
            f"lose the only record of where their replies go -- {detail}. A "
            "re-upgrade cannot reconstruct them (this migration's backfill is "
            "all-NULL by construction), and a worker running against the narrowed "
            "schema fails closed on every non-Slack turn. Record these routes "
            "elsewhere, clear them, then re-run this downgrade."
        )

    # A nonzero generation is a REVOCATION record, not merely a counter: a token
    # claim binds (channel_id, generation), and the row id survives every rebind,
    # so dropping the column and re-upgrading (which restores it at 0) makes every
    # already-revoked generation-0 token authoritative again for that same id.
    # Refused in the same style as the route columns above: destroying a durable
    # security fact is never a silent step.
    rebound = conn.execute(
        sa.text(
            f"""
            SELECT kind, address, generation
            FROM {SCHEMA}.{TABLE}
            WHERE generation <> 0
            ORDER BY kind, address
            """
        )
    ).all()
    if rebound:
        detail = "; ".join(
            f"{row.kind}:{row.address} (generation {row.generation})" for row in rebound
        )
        raise RuntimeError(
            "cannot drop agent_channels.generation (#1459): these bindings have "
            f"been rebound and their generation is the record of it -- {detail}. A "
            "token claim binds (channel_id, generation), and the binding id is "
            "stable across a rebind, so dropping this column and re-upgrading "
            "resets every counter to 0 and RESURRECTS the credentials those "
            "rebinds revoked. Rotate the affected bindings' credentials and record "
            "their generations elsewhere, reset them to 0, then re-run this "
            "downgrade."
        )

    # Reverse creation order: the CHECK references both columns it is dropped
    # ahead of, and a constraint left behind would block the re-upgrade.
    op.drop_constraint(ROUTE_PAIR_CHECK, TABLE, type_="check", schema=SCHEMA)
    op.drop_column(TABLE, "generation", schema=SCHEMA)
    op.drop_column(TABLE, "adapter", schema=SCHEMA)
    op.drop_column(TABLE, "endpoint", schema=SCHEMA)

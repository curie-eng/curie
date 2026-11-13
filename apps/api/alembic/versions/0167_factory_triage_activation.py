"""Factory triage activation: tracker result requests, sessions and endings (#5586).

ADR 0277. Expand. The platform now selects a completion contract at admission
and every later behavior keys off the request's own row:

1. ``execution_requests.completion_contract`` (``implementation`` or
   ``tracker_result``, default ``implementation``) and
   ``result_policy_digest``, set exactly when the contract is
   ``tracker_result`` and then a lowercase SHA-256 hex digest.
2. ``execution_requests.model_session_reserved_at``: the one model session a
   tracker result request may start, stamped once; NULL on every
   implementation request.
3. ``agent_versions.result_policy``: the stored declaration's result kinds
   (sorted) and implementation label. A stored policy requires the 0162
   policy digest; a digest may stand without a policy (see "Rows 0162 could
   hold" below).
4. ``factory_tracker_result_grants.source_content_sha256``: the SHA-256 of the
   source issue's title and body as the platform read them.
5. ``factory_tracker_result_deliveries.origin``: ``submitted`` (default) or
   ``platform``. Only a platform findings delivery may carry the
   ``budget_exhausted`` reason, and a platform delivery carries nothing else.
   The upgrade backfills ``platform`` on every existing ``budget_exhausted``
   delivery before adding those CHECKs.
6. ``factory_tracker_result_receipts.result_comment_url``.
7. ``factory_terminal_notices_applied_label_ck`` also accepts
   ``curie-factory:triaged``; ``factory_slack_posts_kind_ck`` also accepts
   ``triage``.

Rows 0162 could hold. The 0162 schema admits two shapes the 0167 CHECKs would
otherwise refuse, so the upgrade must not assume an empty or 0162-code-only
table (0162's code wrote neither, but its schema is the contract):

1. An agent version with ``result_policy_digest`` set. The declaration it
   digests lives only in the stored bundle bytes, which SQL cannot read, so
   ``result_policy`` cannot be backfilled; clearing the digest would turn a
   declared version into an implementation one, which fails open. The
   presence CHECK is therefore one way: a policy requires a digest, and a
   digest without a policy is allowed. Every 0167 write
   (``deploy.apply_result_policy``) sets both or neither, and admission
   (``factory_tracker_result_runtime.select_completion_contract``) answers a
   digest with no readable policy with ``tracker_result_policy_unreadable``,
   so such a version admits nothing until a deploy re-reads its bundle and
   records both.
2. A findings delivery with reason ``budget_exhausted`` (0162's reason CHECK
   lists it). That reason is platform findings by definition, so its origin
   is derivable: the upgrade sets ``platform`` on those rows, after which the
   origin CHECKs hold. The downgrade still refuses while such a row exists,
   since 0167's delivery path now owns it as a platform delivery.

The delivery ``last_error_code`` CHECK is the 0162 snake case pattern, which
already admits the ``request_settling`` attempt code; it is unchanged.

The downgrade refuses, before any DDL, while any row holds state only 0167 can
represent (a tracker result request, a stored result policy, a content
fingerprint, a platform delivery, a result comment URL, the triaged label or a
triage Slack post), then restores the 0162 definitions.

Revision ID: 0167
Revises: 0166
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0167"
down_revision: str = "0166"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

REQUESTS = "execution_requests"
VERSIONS = "agent_versions"
GRANTS = "factory_tracker_result_grants"
DELIVERIES = "factory_tracker_result_deliveries"
RECEIPTS = "factory_tracker_result_receipts"
NOTICES = "factory_terminal_notices"
SLACK_POSTS = "factory_slack_posts"

HEX64 = "'^[0-9a-f]{64}$'"
FINDINGS_ONLY = "'[\"findings\"]'::jsonb"
FINDINGS_AND_TICKET = '\'["findings", "ticket"]\'::jsonb'

_LABELS_0162 = (
    "'', 'curie:queued', 'curie:running', 'curie:pr-open', 'curie:needs-human', "
    "'curie-factory:queued', 'curie-factory:running', 'curie-factory:pr-open', "
    "'curie-factory:needs-human'"
)
_LABELS = f"{_LABELS_0162}, 'curie-factory:triaged'"
_SLACK_KINDS_0162 = "'root', 'phase', 'pull_request', 'terminal'"
_SLACK_KINDS = f"{_SLACK_KINDS_0162}, 'triage'"

# Exactly two keys; the kinds are one of the two sorted sets a valid
# declaration produces, and the label is a string exactly when a ticket is
# allowed. ``CASE`` keeps the key operators off a non-object value, and
# ``IS TRUE`` keeps a missing key (a NULL operand) a refusal.
RESULT_POLICY_SHAPE = (
    "result_policy IS NULL OR (CASE WHEN jsonb_typeof(result_policy) = 'object' THEN "
    "(result_policy - 'results' - 'implementation_label' = '{}'::jsonb "
    f"AND ((result_policy -> 'results' = {FINDINGS_ONLY} "
    "AND result_policy -> 'implementation_label' = 'null'::jsonb) "
    f"OR (result_policy -> 'results' = {FINDINGS_AND_TICKET} "
    "AND jsonb_typeof(result_policy -> 'implementation_label') = 'string' "
    "AND length(result_policy ->> 'implementation_label') BETWEEN 1 AND 50))) "
    "ELSE false END) IS TRUE"
)

_STATE_PRESENT = (
    f"EXISTS (SELECT 1 FROM {S}.{REQUESTS} WHERE completion_contract <> 'implementation' "
    "OR result_policy_digest IS NOT NULL OR model_session_reserved_at IS NOT NULL) "
    f"OR EXISTS (SELECT 1 FROM {S}.{VERSIONS} WHERE result_policy IS NOT NULL) "
    f"OR EXISTS (SELECT 1 FROM {S}.{GRANTS} WHERE source_content_sha256 IS NOT NULL) "
    f"OR EXISTS (SELECT 1 FROM {S}.{DELIVERIES} WHERE origin <> 'submitted') "
    f"OR EXISTS (SELECT 1 FROM {S}.{RECEIPTS} WHERE result_comment_url IS NOT NULL) "
    f"OR EXISTS (SELECT 1 FROM {S}.{NOTICES} WHERE applied_label = 'curie-factory:triaged') "
    f"OR EXISTS (SELECT 1 FROM {S}.{SLACK_POSTS} WHERE kind = 'triage')"
)


def _replace_check(name: str, table: str, predicate: str) -> None:
    op.drop_constraint(name, table, schema=S)
    op.create_check_constraint(name, table, predicate, schema=S)


def upgrade() -> None:
    op.add_column(
        REQUESTS,
        sa.Column(
            "completion_contract",
            sa.Text(),
            nullable=False,
            server_default="implementation",
        ),
        schema=S,
    )
    op.add_column(REQUESTS, sa.Column("result_policy_digest", sa.Text(), nullable=True), schema=S)
    op.add_column(
        REQUESTS,
        sa.Column("model_session_reserved_at", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.create_check_constraint(
        f"{REQUESTS}_completion_contract_ck",
        REQUESTS,
        "completion_contract IN ('implementation', 'tracker_result')",
        schema=S,
    )
    op.create_check_constraint(
        f"{REQUESTS}_result_policy_digest_ck",
        REQUESTS,
        "(completion_contract = 'tracker_result') = (result_policy_digest IS NOT NULL)",
        schema=S,
    )
    op.create_check_constraint(
        f"{REQUESTS}_result_policy_digest_format_ck",
        REQUESTS,
        f"result_policy_digest IS NULL OR result_policy_digest ~ {HEX64}",
        schema=S,
    )
    op.create_check_constraint(
        f"{REQUESTS}_model_session_reserved_at_ck",
        REQUESTS,
        "model_session_reserved_at IS NULL OR completion_contract = 'tracker_result'",
        schema=S,
    )

    op.add_column(VERSIONS, sa.Column("result_policy", postgresql.JSONB(), nullable=True), schema=S)
    # One way, so a 0162 row with a digest and no readable declaration passes
    # (see "Rows 0162 could hold" above).
    op.create_check_constraint(
        f"{VERSIONS}_result_policy_presence_ck",
        VERSIONS,
        "result_policy IS NULL OR result_policy_digest IS NOT NULL",
        schema=S,
    )
    op.create_check_constraint(
        f"{VERSIONS}_result_policy_shape_ck", VERSIONS, RESULT_POLICY_SHAPE, schema=S
    )

    op.add_column(GRANTS, sa.Column("source_content_sha256", sa.Text(), nullable=True), schema=S)
    op.create_check_constraint(
        f"{GRANTS}_source_content_sha256_ck",
        GRANTS,
        f"source_content_sha256 IS NULL OR source_content_sha256 ~ {HEX64}",
        schema=S,
    )

    op.add_column(
        DELIVERIES,
        sa.Column("origin", sa.Text(), nullable=False, server_default="submitted"),
        schema=S,
    )
    # ``budget_exhausted`` is platform findings by definition; backfill before
    # the CHECKs that tie the reason to the origin.
    op.execute(f"UPDATE {S}.{DELIVERIES} SET origin = 'platform' WHERE reason = 'budget_exhausted'")
    op.create_check_constraint(
        f"{DELIVERIES}_origin_ck", DELIVERIES, "origin IN ('submitted', 'platform')", schema=S
    )
    op.create_check_constraint(
        f"{DELIVERIES}_platform_origin_ck",
        DELIVERIES,
        "origin = 'submitted' OR (result_kind = 'findings' AND reason = 'budget_exhausted')",
        schema=S,
    )
    op.create_check_constraint(
        f"{DELIVERIES}_budget_exhausted_origin_ck",
        DELIVERIES,
        "reason IS NULL OR reason <> 'budget_exhausted' OR origin = 'platform'",
        schema=S,
    )

    op.add_column(RECEIPTS, sa.Column("result_comment_url", sa.Text(), nullable=True), schema=S)

    _replace_check(
        f"{NOTICES}_applied_label_ck",
        NOTICES,
        f"applied_label IS NULL OR applied_label IN ({_LABELS})",
    )
    _replace_check("factory_slack_posts_kind_ck", SLACK_POSTS, f"kind IN ({_SLACK_KINDS})")


def downgrade() -> None:
    # Refused while any row holds state 0162 cannot represent: dropping it
    # would turn a tracker result request into an implementation one, lose a
    # stored policy or a platform delivery's origin, or leave a label or post
    # kind the 0162 CHECKs refuse.
    if op.get_bind().scalar(sa.text(f"SELECT {_STATE_PRESENT}")):
        raise RuntimeError("factory_triage_activation_state_present")
    _replace_check("factory_slack_posts_kind_ck", SLACK_POSTS, f"kind IN ({_SLACK_KINDS_0162})")
    _replace_check(
        f"{NOTICES}_applied_label_ck",
        NOTICES,
        f"applied_label IS NULL OR applied_label IN ({_LABELS_0162})",
    )
    op.drop_column(RECEIPTS, "result_comment_url", schema=S)
    op.drop_constraint(f"{DELIVERIES}_budget_exhausted_origin_ck", DELIVERIES, schema=S)
    op.drop_constraint(f"{DELIVERIES}_platform_origin_ck", DELIVERIES, schema=S)
    op.drop_constraint(f"{DELIVERIES}_origin_ck", DELIVERIES, schema=S)
    op.drop_column(DELIVERIES, "origin", schema=S)
    op.drop_constraint(f"{GRANTS}_source_content_sha256_ck", GRANTS, schema=S)
    op.drop_column(GRANTS, "source_content_sha256", schema=S)
    op.drop_constraint(f"{VERSIONS}_result_policy_shape_ck", VERSIONS, schema=S)
    op.drop_constraint(f"{VERSIONS}_result_policy_presence_ck", VERSIONS, schema=S)
    op.drop_column(VERSIONS, "result_policy", schema=S)
    op.drop_constraint(f"{REQUESTS}_model_session_reserved_at_ck", REQUESTS, schema=S)
    op.drop_constraint(f"{REQUESTS}_result_policy_digest_format_ck", REQUESTS, schema=S)
    op.drop_constraint(f"{REQUESTS}_result_policy_digest_ck", REQUESTS, schema=S)
    op.drop_constraint(f"{REQUESTS}_completion_contract_ck", REQUESTS, schema=S)
    op.drop_column(REQUESTS, "model_session_reserved_at", schema=S)
    op.drop_column(REQUESTS, "result_policy_digest", schema=S)
    op.drop_column(REQUESTS, "completion_contract", schema=S)

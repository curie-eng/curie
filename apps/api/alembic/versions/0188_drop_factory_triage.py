"""Drop the factory triage schema (#5807).

Contract. Revisions 0162, 0167 and 0171 built the separate triage path of ADR
0277: four tracker result tables, the triage delegation and handoff tables,
three ``execution_requests`` columns, three ``agent_versions`` columns, their
CHECKs, the ``curie-factory:triaged`` state label and the ``triage`` Slack post
kind. No code reads or writes any of them any more. This revision drops them,
in an order where each step is valid on its own, and keeps every row of every
kept table.

The drop is unconditional, with no live-state refusal. No release carries the
triage schema, so only installs that followed the integration branch hold it;
the tracker holds every finding and ticket triage ever published, so nothing
a reader needs lives only in these rows; and a refusal would put an operator
step into every upgrade from the integration branch.

Before any column is dropped, the upgrade quiets the Slack progress lane for
the ``execution_requests`` rows of retained triage requests (#5807 correction
14). Once ``completion_contract`` is gone nothing tells those rows apart from
implementation requests, so the lane would announce runs it already
announced: a completed triage request whose findings were already posted
would get a new "Factory run starting" card or card edits, and a request that
ended failed while its findings were owed or delivered would get a false
"This run needs you" reply. The step records them as settled for the lane,
using only states the lane already has:

1. Each such failed or expired request inside a thread gets its
   ``needs_person:<request id>`` post as already failed (``failed_at`` set,
   ``last_error`` ``retired_triage_request``), the state a permanently refused
   send leaves. Materialization then finds the post present and adds nothing,
   delivery skips failed posts, and the ordering guard ignores them. An unposted
   one a newer worker already wrote is failed the same way; a posted one stays.
2. A thread whose shown request (its latest at or after ``first_sequence``) is
   a terminal triage request gets ``card_upgrade_cutoff`` and, when it has a
   card, ``card_edited_at`` moved to the upgrade time, and loses an unposted
   ``card`` post. A run that ended before the cutoff owes no card post, the
   column's documented meaning, and a card edited after its run ended is
   settled. A later request on the WorkItem is not terminal, so it gets its
   card and its edits as usual.

3. A WorkItem with a triage request and no thread yet, whose agent has a
   factory Slack destination with exactly one adapter-bearing binding, gets
   the thread discovery would give it, except that its ``first_sequence`` is
   the first eligible request after its last triage request, or the WorkItem's
   next sequence when there is none, and it has no root post. Nothing at or
   after that sequence is a triage request, so the lane owes the thread
   nothing until a later request exists; it then roots the thread and posts
   that request's card as for any new thread.

Requests that are not terminal are not touched: the operator step in
docs/operations.md ("Factory triage removal (Alembic revision 0188)") requires
that no triage request is queued or running when the upgrade runs.

Being a contract revision, it needs ``curie cluster upgrade --forward-only``
(or ``api.migrate.forwardOnly=true`` on a direct ``helm upgrade``) when it is
pending; the same section of docs/operations.md describes the step.

The downgrade restores the 0186 shape exactly (every table, column, default,
foreign key, unique constraint, CHECK and index, written out here) and
restores no rows. It refuses nothing: ``completion_contract`` comes back NOT
NULL with its ``implementation`` default, which every kept row satisfies, and
every other restored column is nullable or belongs to an empty table. The
threads step 3 recorded stay; the 0186 worker never roots them, so after a
downgrade a later request on such a WorkItem gets no Slack thread.

Revision ID: 0188
Revises: 0186
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0188"
down_revision: str = "0186"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"
LOCK_TIMEOUT_MS = 15000

REQUESTS = "execution_requests"
VERSIONS = "agent_versions"
NOTICES = "factory_terminal_notices"
THREADS = "factory_slack_threads"
POSTS = "factory_slack_posts"
AGENTS = "agents"
CHANNELS = "agent_channels"
WORK_ITEMS = "work_items"

GRANTS = "factory_tracker_result_grants"
DELIVERIES = "factory_tracker_result_deliveries"
RECEIPTS = "factory_tracker_result_receipts"
ARTIFACTS = "factory_tracker_result_artifacts"
DELEGATIONS = "factory_triage_delegations"
HANDOFFS = "factory_triage_handoffs"

COMPLETION_CONTRACT = "completion_contract"
REQUEST_POLICY_DIGEST = "result_policy_digest"
MODEL_SESSION_RESERVED_AT = "model_session_reserved_at"
RESULT_POLICY = "result_policy"
VERSION_POLICY_DIGEST = "result_policy_digest"
RESULT_POLICY_SCHEMA_VERSION = "result_policy_schema_version"

KIND_CK = "factory_slack_posts_kind_ck"
LABEL_CK = "factory_terminal_notices_applied_label_ck"
RECOVERY_SHAPE_CK = "execution_requests_recovery_shape_ck"
REQUEST_CHECKS = (
    "execution_requests_completion_contract_ck",
    "execution_requests_result_policy_digest_ck",
    "execution_requests_result_policy_digest_format_ck",
    "execution_requests_model_session_reserved_at_ck",
)
VERSION_CHECKS = (
    "agent_versions_result_policy_ck",
    "agent_versions_result_policy_presence_ck",
    "agent_versions_result_policy_shape_ck",
)

# Read from pg_get_constraintdef on a database at 0186. AFTER is BEFORE minus
# exactly the one triage value, so every value another revision added stays.
KINDS_BEFORE = (
    "root",
    "phase",
    "pull_request",
    "terminal",
    "triage",
    "card",
    "needs_person",
    "ready",
)
KINDS_AFTER = ("root", "phase", "pull_request", "terminal", "card", "needs_person", "ready")
LABELS_BEFORE = (
    "",
    "curie:queued",
    "curie:running",
    "curie:pr-open",
    "curie:needs-human",
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
    "curie-factory:triaged",
    "curie-factory:questions",
)
LABELS_AFTER = (
    "",
    "curie:queued",
    "curie:running",
    "curie:pr-open",
    "curie:needs-human",
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
    "curie-factory:questions",
)

assert KINDS_AFTER == tuple(kind for kind in KINDS_BEFORE if kind != "triage")
assert LABELS_AFTER == tuple(label for label in LABELS_BEFORE if label != "curie-factory:triaged")

RECOVERY_SHAPE_AFTER = "execution_kind <> 'publication_recovery' OR retry_of_request_id IS NOT NULL"
RECOVERY_SHAPE_BEFORE = (
    "execution_kind <> 'publication_recovery' "
    "OR (completion_contract = 'implementation' AND retry_of_request_id IS NOT NULL)"
)

RETIRED = "retired_triage_request"

# Correction 14, step 1: the exact needs_person predicate the worker held
# until #5898, for requests inside an existing thread.
QUIET_NEEDS_PERSON = f"""
    INSERT INTO {S}.{POSTS} AS p
           (work_item_id, post_key, kind, sequence, rank, ordinal, payload,
            failed_at, last_error)
    SELECT t.work_item_id, 'needs_person:' || q.id, 'needs_person', q.sequence, 3, 0,
           CAST('{{}}' AS jsonb), now(), '{RETIRED}'
      FROM {S}.{THREADS} t
      JOIN {S}.{REQUESTS} q
        ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
     WHERE q.completion_contract = 'tracker_result'
       AND q.terminal_at IS NOT NULL
       AND q.status IN ('failed', 'expired')
       AND EXISTS (
           SELECT 1
             FROM {S}.{DELIVERIES} d
            WHERE d.execution_request_id = q.id
              AND d.state IN ('owed', 'delivered')
       )
    ON CONFLICT (work_item_id, post_key) DO UPDATE
       SET failed_at = now(),
           last_error = '{RETIRED}',
           lease_owner = NULL,
           lease_expires_at = NULL
     WHERE p.posted_at IS NULL AND p.failed_at IS NULL
"""
# Correction 14, step 2: threads whose shown request (the worker's shown
# request query) is a terminal triage request. Alias t is the thread.
_SHOWN_RETIRED = f"""
    EXISTS (
        SELECT 1
          FROM (
              SELECT s.completion_contract, s.terminal_at
                FROM {S}.{REQUESTS} s
               WHERE s.work_item_id = t.work_item_id
                 AND s.sequence >= t.first_sequence
               ORDER BY s.sequence DESC
               LIMIT 1
          ) q
         WHERE q.completion_contract = 'tracker_result'
           AND q.terminal_at IS NOT NULL
    )
"""
QUIET_CARD_POSTS = f"""
    DELETE FROM {S}.{POSTS} p
     USING {S}.{THREADS} t
     WHERE p.work_item_id = t.work_item_id
       AND p.post_key = 'card'
       AND p.posted_at IS NULL
       AND {_SHOWN_RETIRED}
"""
# Correction 14, step 3: a WorkItem with a retained triage request that the
# lane has not discovered yet. Discovery would root a thread at the triage
# request and owe it a card and a needs a person reply, so the upgrade records
# the thread the way discovery does, but starting after the last triage
# request: at the first discovery-eligible request after it, else at the
# WorkItem's next sequence. No root post is written; the lane roots such a
# thread once a request at its first sequence exists.
QUIET_UNDISCOVERED = f"""
    INSERT INTO {S}.{THREADS} AS t
           (work_item_id, agent_id, channel_identity, channel,
            root_ts, first_sequence, origin)
    SELECT w.id, w.agent_id, b.adapters[1], a.factory_slack_channel,
           NULL, COALESCE(n.first_sequence, w.next_sequence), 'configured'
      FROM {S}.{WORK_ITEMS} w
      JOIN {S}.{AGENTS} a ON a.id = w.agent_id
      CROSS JOIN LATERAL (
          SELECT max(r.sequence) AS last_retired
            FROM {S}.{REQUESTS} r
           WHERE r.work_item_id = w.id
             AND r.completion_contract = 'tracker_result'
      ) x
      CROSS JOIN LATERAL (
          SELECT min(q.sequence) AS first_sequence
            FROM {S}.{REQUESTS} q
           WHERE q.work_item_id = w.id
             AND q.sequence > x.last_retired
             AND (q.created_at >= a.factory_slack_since
                  OR q.terminal_at IS NULL
                  OR q.terminal_at >= a.factory_slack_since)
      ) n
      CROSS JOIN LATERAL (
          SELECT array_agg(CAST(c.adapter AS text)) AS adapters
            FROM {S}.{CHANNELS} c
           WHERE c.agent_id = a.id
             AND c.kind = 'slack'
             AND c.address = a.factory_slack_channel
             AND c.adapter IS NOT NULL
      ) b
     WHERE a.factory_slack_channel IS NOT NULL
       AND a.factory_slack_since IS NOT NULL
       AND x.last_retired IS NOT NULL
       AND cardinality(b.adapters) = 1
       AND NOT EXISTS (
           SELECT 1 FROM {S}.{THREADS} e WHERE e.work_item_id = w.id
       )
    ON CONFLICT (work_item_id) DO NOTHING
"""
QUIET_CARDS = f"""
    UPDATE {S}.{THREADS} t
       SET card_upgrade_cutoff = GREATEST(t.card_upgrade_cutoff, now()),
           card_edited_at = CASE WHEN t.card_ts IS NULL THEN t.card_edited_at
                                 ELSE GREATEST(t.card_edited_at, now()) END
     WHERE {_SHOWN_RETIRED}
"""

HEX64 = "'^[0-9a-f]{64}$'"
CODE = "'^[a-z][a-z0-9_]{0,63}$'"
FINDINGS_ONLY = "'[\"findings\"]'::jsonb"
FINDINGS_AND_TICKET = '\'["findings", "ticket"]\'::jsonb'
RESULT_KINDS = "'ticket', 'findings'"
REASONS = (
    "'not_reproduced', 'already_fixed', 'product_decision_needed', "
    "'capability_unavailable', 'budget_exhausted'"
)
STATES = "'owed', 'delivered', 'failed'"
OUTPUT_COLUMNS = (
    "output_tracker_kind",
    "output_tracker_host",
    "output_scope_id",
    "output_issue_id",
    "output_url",
    "output_body_sha256",
)
OUTPUTS = ", ".join(OUTPUT_COLUMNS)
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


def _sql_list(values: Sequence[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _replace_check(name: str, table: str, predicate: str) -> None:
    op.drop_constraint(name, table, schema=S, type_="check")
    op.create_check_constraint(name, table, predicate, schema=S)


def _kinds_check(kinds: Sequence[str]) -> str:
    return f"kind IN ({_sql_list(kinds)})"


def _labels_check(labels: Sequence[str]) -> str:
    return f"applied_label IS NULL OR applied_label IN ({_sql_list(labels)})"


def upgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    # Correction 14 first: it reads completion_contract and the deliveries.
    op.execute(sa.text(QUIET_NEEDS_PERSON))
    op.execute(sa.text(QUIET_CARD_POSTS))
    op.execute(sa.text(QUIET_CARDS))
    op.execute(sa.text(QUIET_UNDISCOVERED))

    op.execute(sa.text(f"DELETE FROM {S}.{POSTS} WHERE kind = 'triage'"))
    op.execute(
        sa.text(
            f"UPDATE {S}.{NOTICES} SET applied_label = NULL "
            "WHERE applied_label = 'curie-factory:triaged'"
        )
    )
    _replace_check(KIND_CK, POSTS, _kinds_check(KINDS_AFTER))
    _replace_check(LABEL_CK, NOTICES, _labels_check(LABELS_AFTER))

    op.drop_table(HANDOFFS, schema=S)
    op.drop_table(DELEGATIONS, schema=S)
    op.drop_table(ARTIFACTS, schema=S)
    op.drop_table(RECEIPTS, schema=S)
    op.drop_table(DELIVERIES, schema=S)
    op.drop_table(GRANTS, schema=S)

    # Rewritten before completion_contract goes: PostgreSQL would otherwise
    # drop the whole CHECK silently with the column.
    _replace_check(RECOVERY_SHAPE_CK, REQUESTS, RECOVERY_SHAPE_AFTER)
    for name in REQUEST_CHECKS:
        op.drop_constraint(name, REQUESTS, schema=S, type_="check")
    op.drop_column(REQUESTS, COMPLETION_CONTRACT, schema=S)
    op.drop_column(REQUESTS, REQUEST_POLICY_DIGEST, schema=S)
    op.drop_column(REQUESTS, MODEL_SESSION_RESERVED_AT, schema=S)

    for name in VERSION_CHECKS:
        op.drop_constraint(name, VERSIONS, schema=S, type_="check")
    op.drop_column(VERSIONS, RESULT_POLICY, schema=S)
    op.drop_column(VERSIONS, VERSION_POLICY_DIGEST, schema=S)
    op.drop_column(VERSIONS, RESULT_POLICY_SCHEMA_VERSION, schema=S)


def _uuid_fk(name: str, target: str, **kwargs: object) -> sa.Column:
    return sa.Column(
        name,
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey(f"{S}.{target}", ondelete="CASCADE"),
        **kwargs,
    )


def _now(name: str) -> sa.Column:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


def _output_columns(*, nullable: bool) -> list[sa.Column]:
    return [sa.Column(name, sa.Text(), nullable=nullable) for name in OUTPUT_COLUMNS]


def _restore_agent_versions() -> None:
    op.add_column(
        VERSIONS, sa.Column(RESULT_POLICY_SCHEMA_VERSION, sa.Integer(), nullable=True), schema=S
    )
    op.add_column(VERSIONS, sa.Column(VERSION_POLICY_DIGEST, sa.Text(), nullable=True), schema=S)
    op.add_column(VERSIONS, sa.Column(RESULT_POLICY, postgresql.JSONB(), nullable=True), schema=S)
    op.create_check_constraint(
        "agent_versions_result_policy_ck",
        VERSIONS,
        "(result_policy_schema_version IS NULL AND result_policy_digest IS NULL) "
        "OR (result_policy_schema_version IS NOT NULL "
        "AND result_policy_schema_version = 1 "
        "AND result_policy_digest IS NOT NULL "
        f"AND result_policy_digest ~ {HEX64})",
        schema=S,
    )
    op.create_check_constraint(
        "agent_versions_result_policy_presence_ck",
        VERSIONS,
        "result_policy IS NULL OR result_policy_digest IS NOT NULL",
        schema=S,
    )
    op.create_check_constraint(
        "agent_versions_result_policy_shape_ck", VERSIONS, RESULT_POLICY_SHAPE, schema=S
    )


def _restore_execution_requests() -> None:
    op.add_column(
        REQUESTS,
        sa.Column(COMPLETION_CONTRACT, sa.Text(), nullable=False, server_default="implementation"),
        schema=S,
    )
    op.add_column(REQUESTS, sa.Column(REQUEST_POLICY_DIGEST, sa.Text(), nullable=True), schema=S)
    op.add_column(
        REQUESTS,
        sa.Column(MODEL_SESSION_RESERVED_AT, sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.create_check_constraint(
        "execution_requests_completion_contract_ck",
        REQUESTS,
        "completion_contract IN ('implementation', 'tracker_result')",
        schema=S,
    )
    op.create_check_constraint(
        "execution_requests_result_policy_digest_ck",
        REQUESTS,
        "(completion_contract = 'tracker_result') = (result_policy_digest IS NOT NULL)",
        schema=S,
    )
    op.create_check_constraint(
        "execution_requests_result_policy_digest_format_ck",
        REQUESTS,
        f"result_policy_digest IS NULL OR result_policy_digest ~ {HEX64}",
        schema=S,
    )
    op.create_check_constraint(
        "execution_requests_model_session_reserved_at_ck",
        REQUESTS,
        "model_session_reserved_at IS NULL OR completion_contract = 'tracker_result'",
        schema=S,
    )
    _replace_check(RECOVERY_SHAPE_CK, REQUESTS, RECOVERY_SHAPE_BEFORE)


def _restore_grants() -> None:
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
        sa.Column("source_content_sha256", sa.Text(), nullable=True),
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
        sa.CheckConstraint(
            f"source_content_sha256 IS NULL OR source_content_sha256 ~ {HEX64}",
            name=f"{GRANTS}_source_content_sha256_ck",
        ),
        schema=S,
    )
    op.create_index(f"ix_{GRANTS}_work_item_id", GRANTS, ["work_item_id"], schema=S)


def _restore_deliveries() -> None:
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
        *_output_columns(nullable=True),
        _now("submitted_at"),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        _now("created_at"),
        _now("updated_at"),
        sa.Column("origin", sa.Text(), nullable=False, server_default="submitted"),
        sa.UniqueConstraint("grant_id", name=f"{DELIVERIES}_grant_id_key"),
        sa.CheckConstraint(f"result_kind IN ({RESULT_KINDS})", name=f"{DELIVERIES}_result_kind_ck"),
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
        sa.CheckConstraint(f"num_nonnulls({OUTPUTS}) IN (0, 6)", name=f"{DELIVERIES}_output_ck"),
        sa.CheckConstraint("deadline_at > submitted_at", name=f"{DELIVERIES}_deadline_ck"),
        sa.CheckConstraint("origin IN ('submitted', 'platform')", name=f"{DELIVERIES}_origin_ck"),
        sa.CheckConstraint(
            "origin = 'submitted' OR (result_kind = 'findings' AND reason = 'budget_exhausted')",
            name=f"{DELIVERIES}_platform_origin_ck",
        ),
        sa.CheckConstraint(
            "reason IS NULL OR reason <> 'budget_exhausted' OR origin = 'platform'",
            name=f"{DELIVERIES}_budget_exhausted_origin_ck",
        ),
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


def _restore_receipts_and_artifacts() -> None:
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
        *_output_columns(nullable=True),
        sa.Column("output_labels", postgresql.JSONB(), nullable=True),
        _now("delivered_at"),
        sa.Column("result_comment_url", sa.Text(), nullable=True),
        sa.CheckConstraint(
            f"reason IS NULL OR reason IN ({REASONS})", name=f"{RECEIPTS}_reason_ck"
        ),
        sa.CheckConstraint(
            f"(result_kind = 'ticket' AND num_nonnulls({OUTPUTS}, output_labels) = 7 "
            "AND reason IS NULL) "
            f"OR (result_kind = 'findings' AND num_nonnulls({OUTPUTS}, output_labels) = 0 "
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


def _restore_delegations_and_handoffs() -> None:
    uuid_type = postgresql.UUID(as_uuid=True)
    op.create_table(
        DELEGATIONS,
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column(
            "work_item_id",
            uuid_type,
            sa.ForeignKey(f"{S}.work_items.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_execution_request_id", uuid_type, nullable=False, unique=True),
        *[
            sa.Column(name, sa.Text(), nullable=False)
            for name in (
                "actor_id",
                "actor_login",
                "selection_event",
                "tracker_kind",
                "tracker_host",
                "tracker_scope_id",
                "tracker_issue_id",
                "repository_id",
                "result_policy_digest",
                "target_label",
            )
        ],
        sa.Column("installation_id", sa.BigInteger(), nullable=True),
        sa.Column("source_fingerprint", sa.Text(), nullable=True),
        sa.Column("target_binding_id", uuid_type, nullable=True),
        sa.Column("target_binding_generation", sa.Integer(), nullable=True),
        sa.Column("target_agent_id", uuid_type, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(target_binding_id IS NULL) = (target_agent_id IS NULL) "
            "AND (target_binding_id IS NULL) = (target_binding_generation IS NULL)",
            name=f"{DELEGATIONS}_target_ck",
        ),
        schema=S,
    )
    op.create_table(
        HANDOFFS,
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column(
            "delegation_id",
            uuid_type,
            sa.ForeignKey(f"{S}.{DELEGATIONS}.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("source_execution_request_id", uuid_type, nullable=False, unique=True),
        *_output_columns(nullable=False),
        sa.Column("implementation_request_id", uuid_type, nullable=True),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("label_cleanup_owed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        *[
            sa.Column(name, sa.DateTime(timezone=True), nullable=True)
            for name in ("associated_at", "labelled_at", "lease_expires_at", "settled_at")
        ],
        *[
            sa.Column(name, sa.DateTime(timezone=True), nullable=False)
            for name in ("next_attempt_at", "deadline_at")
        ],
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "output_tracker_host",
            "output_scope_id",
            "output_issue_id",
            name=f"{HANDOFFS}_output_key",
        ),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('admitted', 'refused')",
            name=f"{HANDOFFS}_outcome_ck",
        ),
        sa.CheckConstraint(
            "(outcome = 'admitted') = (implementation_request_id IS NOT NULL) "
            "AND (outcome = 'refused') = (reason IS NOT NULL) "
            "AND (outcome IS NULL) = (settled_at IS NULL)",
            name=f"{HANDOFFS}_settled_ck",
        ),
        schema=S,
    )
    op.create_index(
        f"ix_{HANDOFFS}_owed",
        HANDOFFS,
        ["next_attempt_at"],
        schema=S,
        postgresql_where=sa.text("outcome IS NULL OR label_cleanup_owed"),
    )


def downgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    _restore_agent_versions()
    _restore_execution_requests()
    _restore_grants()
    _restore_deliveries()
    _restore_receipts_and_artifacts()
    _restore_delegations_and_handoffs()
    _replace_check(LABEL_CK, NOTICES, _labels_check(LABELS_BEFORE))
    _replace_check(KIND_CK, POSTS, _kinds_check(KINDS_BEFORE))

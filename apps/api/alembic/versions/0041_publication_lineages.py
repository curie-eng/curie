"""Persist thread-owned pull-request publication lineages.

Revision ID: 0041
Revises: 0040
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0041"
down_revision: str | None = "0040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def _publication_identity_cte() -> str:
    """Prefer the durable workspace identity; scope only the legacy fallback."""

    return f"""
            publication_identities AS (
                SELECT p.id, p.approval_id, a.agent_id, p.deployment_id,
                       COALESCE(
                           p.workspace_conversation_id,
                           {SCHEMA}._0041_percent_encode(a.reply_kind) || ':' ||
                           {SCHEMA}._0041_percent_encode(a.reply_channel) || ':' ||
                           {SCHEMA}._0041_percent_encode(a.conversation_id)
                       ) AS conversation_id,
                       p.repo_full_name, p.base_sha, p.status, p.result_url,
                       p.created_at
                  FROM {SCHEMA}.publications AS p
                  JOIN {SCHEMA}.approvals AS a ON a.id = p.approval_id
                 WHERE p.status IN ('pending', 'approved', 'launching', 'running')
                    OR (
                        p.status = 'succeeded'
                        AND p.result_url =
                            'https://github.com/' || p.repo_full_name || '/pull/' ||
                            substring(p.result_url FROM '/pull/([1-9][0-9]*)$')
                        AND substring(
                            p.result_url FROM '/pull/([1-9][0-9]*)$'
                        )::numeric <= 2147483647
                    )
            )
    """


def upgrade() -> None:
    op.create_table(
        "thread_publication_lineages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("deployment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("conversation_id", sa.String(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=False),
        sa.Column("base_sha", sa.String(), nullable=False),
        sa.Column("branch", sa.String(), nullable=False),
        sa.Column("pr_number", sa.Integer(), nullable=True),
        sa.Column("pr_url", sa.String(), nullable=True),
        sa.Column("head_sha", sa.String(), nullable=True),
        sa.Column("status", sa.String(), server_default="open", nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("latest_revision", sa.Integer(), server_default="1", nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "status IN ('open', 'merged', 'closed')",
            name="thread_publication_lineages_status_ck",
        ),
        sa.CheckConstraint(
            "version >= 1", name="thread_publication_lineages_version_ck"
        ),
        sa.CheckConstraint(
            "latest_revision >= 1",
            name="thread_publication_lineages_latest_revision_ck",
        ),
        sa.CheckConstraint(
            "(pr_number IS NULL) = (pr_url IS NULL)",
            name="thread_publication_lineages_pr_identity_ck",
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["deployment_id"], [f"{SCHEMA}.deployments.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "branch", name="thread_publication_lineages_branch_key"
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_thread_publication_lineages_agent_id",
        "thread_publication_lineages",
        ["agent_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_thread_publication_lineages_deployment_id",
        "thread_publication_lineages",
        ["deployment_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_thread_publication_lineages_conversation_id",
        "thread_publication_lineages",
        ["conversation_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "uq_active_thread_publication_lineage",
        "thread_publication_lineages",
        ["agent_id", "conversation_id", "repo_full_name"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'open'"),
    )

    op.add_column(
        "publications",
        sa.Column("lineage_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "publications",
        sa.Column("revision_number", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "publications",
        sa.Column("expected_prior_head", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "publications",
        sa.Column("outcome_history_ready_at", sa.DateTime(), nullable=True),
        schema=SCHEMA,
    )
    # Terminal rows from a release predating the outcome-history fence cannot be
    # replayed into an old transcript honestly. Grandfather only those rows so
    # the migration does not deadlock established threads. Every terminal state
    # created after this migration starts NULL and is acknowledged by the worker
    # only after its marker is present in transcript history.
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.publications
               SET outcome_history_ready_at =
                   COALESCE(result_reported_at, terminal_at, updated_at, now())
             WHERE status IN ('denied', 'expired', 'succeeded', 'failed')
            """
        )
    )

    # ``channel_protocol.scoped_conversation_id`` percent-encodes each UTF-8
    # segment with only RFC 3986 unreserved bytes left literal. This SQL twin is
    # only for publications whose 0040 workspace snapshot is NULL. Keep it local
    # to the data move and drop it below; it is not a new database contract.
    op.execute(
        sa.text(
            f"""
            CREATE FUNCTION {SCHEMA}._0041_percent_encode(value text)
            RETURNS text
            LANGUAGE plpgsql
            IMMUTABLE STRICT PARALLEL SAFE
            AS $function$
            DECLARE
                source_bytes bytea := convert_to(value, 'UTF8');
                encoded text := '';
                byte_offset integer;
                byte_value integer;
            BEGIN
                IF length(source_bytes) = 0 THEN
                    RETURN encoded;
                END IF;
                FOR byte_offset IN 0..length(source_bytes) - 1 LOOP
                    byte_value := get_byte(source_bytes, byte_offset);
                    IF (byte_value BETWEEN 48 AND 57)
                       OR (byte_value BETWEEN 65 AND 90)
                       OR (byte_value BETWEEN 97 AND 122)
                       OR byte_value IN (45, 46, 95, 126) THEN
                        encoded := encoded || chr(byte_value);
                    ELSE
                        encoded := encoded || '%' ||
                            upper(lpad(to_hex(byte_value), 2, '0'));
                    END IF;
                END LOOP;
                RETURN encoded;
            END;
            $function$
            """
        )
    )

    # Migration 0040 deliberately left pre-scoping rows NULL because it did not
    # yet have a durable lineage authority. 0041 does: canonicalize every opaque
    # legacy adapter-native id exactly once, regardless of whether its bytes
    # resemble a scoped key. The Approval remains unchanged for reply routing;
    # credential redemption, transcript delivery, and lineage replay consume
    # the new workspace snapshot.
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.publications AS p
               SET workspace_conversation_id =
                   {SCHEMA}._0041_percent_encode(a.reply_kind) || ':' ||
                   {SCHEMA}._0041_percent_encode(a.reply_channel) || ':' ||
                   {SCHEMA}._0041_percent_encode(a.conversation_id)
              FROM {SCHEMA}.approvals AS a
             WHERE a.id = p.approval_id
               AND p.workspace_conversation_id IS NULL
            """
        )
    )

    # Pre-lineage releases allowed several publications for one thread. Select
    # exactly one lineage owner per agent/thread/repository: active work
    # outranks succeeded history, then the newest row wins. The winner keeps its
    # UUID and therefore its byte-for-byte stable deterministic branch.
    op.execute(
        sa.text(
            f"""
            WITH {_publication_identity_cte()},
            ranked_publications AS (
                SELECT p.id, p.agent_id, p.deployment_id, p.conversation_id,
                       p.repo_full_name, p.base_sha, p.status, p.result_url,
                       row_number() OVER (
                           PARTITION BY p.agent_id, p.conversation_id,
                                        p.repo_full_name
                           ORDER BY
                               CASE WHEN p.status IN
                                   ('pending', 'approved', 'launching', 'running')
                                   THEN 0 ELSE 1 END,
                               p.created_at DESC,
                               p.id DESC
                       ) AS lineage_rank
                  FROM publication_identities AS p
            )
            INSERT INTO {SCHEMA}.thread_publication_lineages
                (id, agent_id, deployment_id, conversation_id, repo_full_name,
                 base_sha, branch, pr_number, pr_url, status, version,
                 latest_revision)
            SELECT p.id, p.agent_id, p.deployment_id, p.conversation_id,
                   p.repo_full_name, p.base_sha,
                   'curie/publication-' || replace(p.id::text, '-', ''),
                   CASE WHEN p.status = 'succeeded'
                        THEN substring(p.result_url FROM '/pull/([1-9][0-9]*)$')::integer
                        ELSE NULL END,
                   CASE WHEN p.status = 'succeeded' THEN p.result_url ELSE NULL END,
                   'open', 1, 1
              FROM ranked_publications AS p
             WHERE p.lineage_rank = 1
            """
        )
    )

    # An active loser cannot remain claimable without a lineage. Settle it as a
    # generic migration conflict, clear its private patch and leases, and close
    # a still-pending approval so the old card cannot later revive the row.
    # Succeeded losers are immutable history and intentionally remain unlinked.
    op.execute(
        sa.text(
            f"""
            WITH {_publication_identity_cte()},
            ranked_publications AS (
                SELECT p.id, p.approval_id, p.status,
                       row_number() OVER (
                           PARTITION BY p.agent_id, p.conversation_id,
                                        p.repo_full_name
                           ORDER BY
                               CASE WHEN p.status IN
                                   ('pending', 'approved', 'launching', 'running')
                                   THEN 0 ELSE 1 END,
                               p.created_at DESC,
                               p.id DESC
                       ) AS lineage_rank
                  FROM publication_identities AS p
            ), failed_publications AS (
                UPDATE {SCHEMA}.publications AS p
                   SET status = 'failed',
                       version = p.version + 1,
                       patch_bytes = NULL,
                       error = 'superseded by another publication during lineage migration',
                       lease_owner = NULL,
                       lease_expires_at = NULL,
                       updated_at = now(),
                       terminal_at = COALESCE(p.terminal_at, now())
                  FROM ranked_publications AS ranked
                 WHERE p.id = ranked.id
                   AND ranked.lineage_rank > 1
                   AND ranked.status IN
                       ('pending', 'approved', 'launching', 'running')
             RETURNING p.approval_id
            )
            UPDATE {SCHEMA}.approvals AS a
               SET status = 'expired',
                   resolved_at = COALESCE(a.resolved_at, now()),
                   resumed_at = COALESCE(a.resumed_at, now())
              FROM failed_publications AS failed
             WHERE a.id = failed.approval_id
               AND a.status = 'pending'
            """
        )
    )

    op.execute(sa.text(f"DROP FUNCTION {SCHEMA}._0041_percent_encode(text)"))

    # Joining the inserted lineage makes it impossible to assign a losing
    # publication a foreign-key value for a lineage that was never created.
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.publications AS p
               SET lineage_id = lineage.id,
                   revision_number = 1,
                   expected_prior_head = p.base_sha
              FROM {SCHEMA}.thread_publication_lineages AS lineage
             WHERE lineage.id = p.id
            """
        )
    )

    # This is a contract boundary, not a mixed-version expand. A 0040 writer
    # omits lineage_id and creates pending work; refuse that shape rather than
    # leave unclaimable work behind. Historical terminal rows remain readable.
    op.create_check_constraint(
        "publications_active_lineage_ck",
        "publications",
        "status NOT IN ('pending', 'approved', 'launching', 'running') "
        "OR lineage_id IS NOT NULL",
        schema=SCHEMA,
    )

    op.create_foreign_key(
        "publications_lineage_id_fkey",
        "publications",
        "thread_publication_lineages",
        ["lineage_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )
    op.create_index(
        "uq_publications_lineage_revision",
        "publications",
        ["lineage_id", "revision_number"],
        unique=True,
        schema=SCHEMA,
    )
    op.create_index(
        "uq_active_publication_per_lineage",
        "publications",
        ["lineage_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text(
            "status IN ('pending', 'approved', 'launching', 'running')"
        ),
    )


def downgrade() -> None:
    op.drop_constraint(
        "publications_active_lineage_ck",
        "publications",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_index(
        "uq_active_publication_per_lineage", table_name="publications", schema=SCHEMA
    )
    op.drop_index(
        "uq_publications_lineage_revision", table_name="publications", schema=SCHEMA
    )
    op.drop_constraint(
        "publications_lineage_id_fkey", "publications", schema=SCHEMA, type_="foreignkey"
    )
    op.drop_column("publications", "expected_prior_head", schema=SCHEMA)
    op.drop_column("publications", "revision_number", schema=SCHEMA)
    op.drop_column("publications", "lineage_id", schema=SCHEMA)
    op.drop_column("publications", "outcome_history_ready_at", schema=SCHEMA)
    op.drop_table("thread_publication_lineages", schema=SCHEMA)

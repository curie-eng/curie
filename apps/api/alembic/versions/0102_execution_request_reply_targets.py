"""Persist each execution request's typed reply target (#3831, ADR 0197).

A factory status reply lands on the tracker issue, a pull request
conversation, or one review thread. Until now the target was re-read from the
first line of the request's objective: a revision asked for from review
feedback carries its canonical feedback URL there. This adds
``reply_target_kind`` ('issue', 'pull_request' or 'review_thread', NOT NULL,
default 'issue'), ``reply_target_pr_number``, ``reply_target_comment_id`` and
``reply_target_url`` (text, nullable) with a CHECK tying the kind to the
columns it sets, and backfills them from that first line.

The backfill reads the old rule exactly: the whole first line must be
``<clone base>/<owner/name>/pull/<n>#<fragment><id>`` for the WorkItem's own
repository, with fragment ``discussion_r`` (a review thread), ``issuecomment-``
or ``pullrequestreview-`` (the pull request conversation). The clone base is the
API's ``GITHUB_CLONE_BASE``, which the chart's migrate Job also receives, with
the API's default when it is unset. A line under any other origin answered on
the issue before, so it keeps the default, the issue, as does every other row.
Downgrade drops the columns.

Revision ID: 0102
Revises: 0101
Create Date: 2026-10-05
"""

import os
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0102"
down_revision: str | None = "0101"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
_TABLE = "execution_requests"
_CHECK = "execution_requests_reply_target_ck"
# Frozen copy of models.REPLY_TARGET_CHECK at this revision.
_SHAPE = (
    "((reply_target_kind = 'issue' AND reply_target_pr_number IS NULL "
    "AND reply_target_comment_id IS NULL AND reply_target_url IS NULL) "
    "OR (reply_target_kind = 'pull_request' "
    "AND length(btrim(reply_target_pr_number)) > 0 "
    "AND reply_target_comment_id IS NULL "
    "AND (reply_target_url IS NULL OR length(btrim(reply_target_url)) > 0)) "
    "OR (reply_target_kind = 'review_thread' "
    "AND length(btrim(reply_target_pr_number)) > 0 "
    "AND length(btrim(reply_target_comment_id)) > 0 "
    "AND (reply_target_url IS NULL OR length(btrim(reply_target_url)) > 0))) IS TRUE"
)
# The old parser's bounds: a PR number below 2**31, a comment id below 2**63.
_BACKFILL = r"""
WITH parsed AS (
    SELECT er.id,
           split_part(er.objective, E'\n', 1) AS line,
           regexp_match(
               substr(
                   split_part(er.objective, E'\n', 1),
                   length(:base || '/' || wi.repo_full_name || '/pull/') + 1
               ),
               '^([1-9][0-9]{0,9})'
               '#(discussion_r|issuecomment-|pullrequestreview-)([1-9][0-9]{0,18})$'
           ) AS m
    FROM curie.execution_requests er
    JOIN curie.work_items wi ON wi.id = er.work_item_id
    WHERE er.objective IS NOT NULL
      AND left(
              split_part(er.objective, E'\n', 1),
              length(:base || '/' || wi.repo_full_name || '/pull/')
          ) = :base || '/' || wi.repo_full_name || '/pull/'
)
UPDATE curie.execution_requests er
SET reply_target_kind = CASE WHEN p.m[2] = 'discussion_r'
                             THEN 'review_thread' ELSE 'pull_request' END,
    reply_target_pr_number = p.m[1],
    reply_target_comment_id = CASE WHEN p.m[2] = 'discussion_r' THEN p.m[3] END,
    reply_target_url = p.line
FROM parsed p
WHERE er.id = p.id
  AND p.m IS NOT NULL
  AND p.m[1]::bigint <= 2147483647
  AND p.m[3]::numeric <= 9223372036854775807
"""


def _clone_base() -> str:
    """The API's configured clone base, as ``Settings.github_clone_base`` reads it."""

    return (os.environ.get("GITHUB_CLONE_BASE", "").strip() or "https://github.com").rstrip("/")


def upgrade() -> None:
    op.add_column(
        _TABLE,
        sa.Column("reply_target_kind", sa.Text(), nullable=False, server_default="issue"),
        schema=SCHEMA,
    )
    for column in ("reply_target_pr_number", "reply_target_comment_id", "reply_target_url"):
        op.add_column(_TABLE, sa.Column(column, sa.Text(), nullable=True), schema=SCHEMA)
    op.execute(sa.text(_BACKFILL).bindparams(base=_clone_base()))
    op.create_check_constraint(_CHECK, _TABLE, _SHAPE, schema=SCHEMA)


def downgrade() -> None:
    op.drop_constraint(_CHECK, _TABLE, schema=SCHEMA, type_="check")
    for column in (
        "reply_target_url",
        "reply_target_comment_id",
        "reply_target_pr_number",
        "reply_target_kind",
    ):
        op.drop_column(_TABLE, column, schema=SCHEMA)

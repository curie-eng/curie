"""One live-updating Slack progress card per factory WorkItem (#5715).

``factory_slack_threads`` gains the card's state:

1. ``card_ts``: the Slack ts of the card. A kickoff thread stores the
   dispatcher's placeholder ts here at admission; any other thread stores the
   ts of the card post the progress lane made.
2. ``card_digest`` and ``card_edited_at``: the digest of the last render Slack
   acknowledged and when that edit was sent, so an unchanged render sends
   nothing and live edits are spaced out.
3. ``card_lease_owner`` and ``card_lease_expires_at``: one worker edits a card
   at a time, because replace edits carry no revision and land last write wins.
4. ``requester_slack_user``: the Slack user id that sent a kickoff mention,
   mentioned by the needs a person and ready replies.

``factory_slack_posts_kind_ck`` also accepts ``card`` (the card's first post
when no placeholder exists), ``needs_person`` and ``ready`` (the two notify
replies). ``phase``, ``terminal`` and ``pull_request`` stay accepted so rows
written before this revision remain valid; the lane no longer delivers them.

Expand only: a release without this revision never reads the new columns or
kinds. The downgrade deletes posts of the new kinds, which an older release
cannot render, then restores the CHECK and drops the columns.

Revision ID: 0178
Revises: 0177
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0178"
down_revision: str = "0177"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"
THREADS = "factory_slack_threads"
POSTS = "factory_slack_posts"
KIND_CK = "factory_slack_posts_kind_ck"
_KINDS_0167 = "'root', 'phase', 'pull_request', 'terminal', 'triage'"
_KINDS = f"{_KINDS_0167}, 'card', 'needs_person', 'ready'"
_COLUMNS = (
    ("card_ts", sa.Text()),
    ("card_digest", sa.Text()),
    ("card_edited_at", sa.DateTime(timezone=True)),
    ("card_lease_owner", sa.Text()),
    ("card_lease_expires_at", sa.DateTime(timezone=True)),
    ("requester_slack_user", sa.Text()),
)


def upgrade() -> None:
    for name, type_ in _COLUMNS:
        op.add_column(THREADS, sa.Column(name, type_, nullable=True), schema=S)
    op.drop_constraint(KIND_CK, POSTS, schema=S)
    op.create_check_constraint(KIND_CK, POSTS, f"kind IN ({_KINDS})", schema=S)


def downgrade() -> None:
    op.execute(f"DELETE FROM {S}.{POSTS} WHERE kind IN ('card', 'needs_person', 'ready')")
    op.drop_constraint(KIND_CK, POSTS, schema=S)
    op.create_check_constraint(KIND_CK, POSTS, f"kind IN ({_KINDS_0167})", schema=S)
    for name, _type in reversed(_COLUMNS):
        op.drop_column(THREADS, name, schema=S)

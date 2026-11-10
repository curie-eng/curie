"""Allow deleted tracker issues to cancel execution requests.

Revision ID: 0147
Revises: 0145
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0147"
down_revision: str = "0145"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ARRAY_CAUSE = "('issue_cancelled'::character varying)::text"
_DELETED_ARRAY_CAUSE = ", ('issue_deleted'::character varying)::text"
_CANCELLED_EQUALITY = "((terminal_cause)::text = 'issue_cancelled'::text)"
_DELETED_EQUALITY = (
    "((terminal_cause)::text = ANY (ARRAY['issue_cancelled'::text, 'issue_deleted'::text]))"
)


def _shape() -> str:
    shape = op.get_bind().scalar(
        sa.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'curie.execution_requests'::regclass "
            "AND conname = 'execution_requests_state_shape_ck'"
        )
    )
    if not isinstance(shape, str) or not shape.startswith("CHECK (") or not shape.endswith(")"):
        raise RuntimeError("execution request state shape is unavailable")
    return shape[len("CHECK (") : -1]


def _replace_shape(shape: str) -> None:
    op.drop_constraint(
        "execution_requests_state_shape_ck", "execution_requests", schema="curie", type_="check"
    )
    op.create_check_constraint(
        "execution_requests_state_shape_ck", "execution_requests", shape, schema="curie"
    )


def upgrade() -> None:
    prior = _shape()
    shape = prior.replace(_ARRAY_CAUSE, _ARRAY_CAUSE + _DELETED_ARRAY_CAUSE)
    shape = shape.replace(_CANCELLED_EQUALITY, _DELETED_EQUALITY)
    if shape == prior:
        raise RuntimeError("execution request state shape has no issue cancellation")
    _replace_shape(shape)


def downgrade() -> None:
    connection = op.get_bind()
    if connection.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.execution_requests "
            "WHERE terminal_cause = 'issue_deleted')"
        )
    ):
        raise RuntimeError("cannot downgrade while deleted-issue cancellation records exist")
    shape = _shape().replace(_DELETED_ARRAY_CAUSE, "")
    shape = shape.replace(_DELETED_EQUALITY, _CANCELLED_EQUALITY)
    _replace_shape(shape)

"""An upgrade rollback must not erase admitted or retryable review work."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

from apps.api.tests.test_github_review_events import (
    _write_actual_fenced_review_terminal,
    post_review,
    review_rows,
)
from apps.api.tests.test_github_review_events import review_app_key as review_app_key
from apps.api.tests.test_github_review_events import review_stack as review_stack


@pytest.mark.parametrize("retryable", [False, True])
def test_0043_refuses_active_work_and_allows_rollback_after_real_settlement(
    review_stack, retryable,
):
    client, truth, valkey, stream = review_stack
    if retryable:
        truth.feedback_status = 503
    admitted = post_review(client, truth)
    assert admitted.status_code == (503 if retryable else 200)
    before_feedback = review_rows("SELECT event_id,status FROM curie.github_review_feedback")
    before_delivery = review_rows("SELECT delivery_id,status FROM curie.github_review_deliveries")
    assert [row["status"] for row in before_delivery] == [
        "retryable" if retryable else "accepted"
    ]
    assert [row["status"] for row in before_feedback] == ([] if retryable else ["queued"])
    assert valkey.xlen(stream) == (0 if retryable else 1)
    before_revision = review_rows("SELECT version_num FROM curie.alembic_version")
    api_dir = Path(__file__).resolve().parents[1]
    config = Config(str(api_dir / "alembic.ini"))
    config.set_main_option("script_location", str(api_dir / "alembic"))
    try:
        with pytest.raises(RuntimeError, match="deliveries are active"):
            command.downgrade(config, "0042")
        assert review_rows("SELECT version_num FROM curie.alembic_version") == before_revision
        assert review_rows("SELECT event_id,status FROM curie.github_review_feedback") == (
            before_feedback
        )
        assert review_rows("SELECT delivery_id,status FROM curie.github_review_deliveries") == (
            before_delivery
        )
        if retryable:
            # Recovery is explicit signed redelivery, not assumed GitHub retry.
            truth.feedback_status = 200
            assert post_review(client, truth).json()["status"] == "feedback_queued"
        _write_actual_fenced_review_terminal(client, truth.feedback.event_id, stream)
        assert client.portal.call(client.app.state.github_review_reconciler.reconcile_terminal) == 1
        assert review_rows("SELECT status FROM curie.github_review_feedback") == [
            {"status": "settled"}
        ]
        assert review_rows("SELECT status FROM curie.github_review_deliveries") == [
            {"status": "accepted"}
        ]
        command.downgrade(config, "0042")
        assert review_rows("SELECT version_num FROM curie.alembic_version") == [
            {"version_num": "0042"}
        ]
        assert review_rows(
            "SELECT to_regclass('curie.github_review_feedback') AS feedback, "
            "to_regclass('curie.github_review_deliveries') AS delivery"
        ) == [{"feedback": None, "delivery": None}]
    finally:
        command.upgrade(config, before_revision[0]["version_num"])
    assert review_rows("SELECT version_num FROM curie.alembic_version") == before_revision

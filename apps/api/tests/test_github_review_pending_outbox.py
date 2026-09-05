"""A queued transport receipt can precede its durable SQL queued mark."""

import copy
import json

import pytest
from sqlalchemy.exc import DBAPIError

from apps.api.tests.test_github_review_events import HEAD, post_review, review_rows
from apps.api.tests.test_github_review_events import review_app_key as review_app_key
from apps.api.tests.test_github_review_events import review_stack as review_stack


@pytest.mark.parametrize("operation", ["verify", "reserve"])
def test_waiting_outbox_is_retryable_until_exact_queue_receipt_is_reconciled(
    review_stack, operation,
):
    client, truth, valkey, stream = review_stack
    review_rows("""
        CREATE FUNCTION curie.review_pending_reject() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.status = 'queued' THEN RAISE EXCEPTION 'task-owned queued-mark failure'; END IF;
          RETURN NEW;
        END $$
    """)
    review_rows("""
        CREATE TRIGGER review_pending_reject BEFORE UPDATE ON curie.github_review_feedback
        FOR EACH ROW EXECUTE FUNCTION curie.review_pending_reject()
    """)
    try:
        with pytest.raises(DBAPIError):
            post_review(client, truth)
        rows = valkey.xrange(stream)
        assert len(rows) == 1
        stream_id, fields = rows[0]
        turn = json.loads(fields["payload"])
        lineage = review_rows(
            "SELECT deployment_id,version FROM curie.thread_publication_lineages"
        )[0]
        payload = {"turn": turn, "deployment_id": str(lineage["deployment_id"])}
        if operation == "reserve":
            payload.update(expected_lineage_version=lineage["version"], expected_head_sha=HEAD)
        headers = {"X-Curie-Worker-Token": "fixture-review-worker-token"}
        path = f"/v1/internal/github/reviews/{turn['event_id']}/{operation}"
        forged = copy.deepcopy(payload)
        forged["turn"]["author"] = "U0REQUEST1"
        rejected = client.post(path, json=forged, headers=headers)
        assert rejected.status_code == 409
        assert rejected.json()["detail"]["code"] == "feedback_turn_mismatch"
        calls = len(truth.calls)
        waiting = client.post(path, json=payload, headers=headers)
        assert waiting.status_code == 503, waiting.text
        assert waiting.json()["detail"]["code"] == "feedback_outbox_pending"
        assert waiting.headers["cache-control"] == "no-store"
        assert len(truth.calls) == calls  # No provider read or model authority while pending.
        assert review_rows(
            "SELECT status,version,error_code FROM curie.github_review_feedback"
        ) == [
            {"status": "waiting", "version": 1, "error_code": None}
        ]
        assert review_rows("SELECT id FROM curie.publication_review_reservations") == []
    finally:
        review_rows("DROP TRIGGER review_pending_reject ON curie.github_review_feedback")
        review_rows("DROP FUNCTION curie.review_pending_reject()")
    reconciler = client.app.state.github_review_reconciler
    assert client.portal.call(reconciler.reconcile_once) == 1
    assert valkey.xrange(stream) == [(stream_id, fields)]
    ready = client.post(path, json=payload, headers=headers)
    assert ready.status_code == 200, ready.text
    assert ready.json()["origin_key"] == turn["event_id"]
    assert review_rows("SELECT status FROM curie.github_review_feedback") == [
        {"status": "reserved" if operation == "reserve" else "queued"}
    ]
    reservations = review_rows(
        "SELECT origin_key,status FROM curie.publication_review_reservations"
    )
    assert reservations == (
        [{"origin_key": turn["event_id"], "status": "reserved"}]
        if operation == "reserve" else []
    )

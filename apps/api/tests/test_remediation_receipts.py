"""Receipts: one thread message per stage (plan task 13).

@spec AUTOMATED-REMEDIATION-20

docs/superpowers/specs/2026-10-07-automated-remediation.md, AUTOMATED-REMEDIATION-20:

    The worker remediation loop posts, in the delivery's thread, one message per
    nomination decision and one per verification outcome, as separate messages
    after the investigation's reply. Each names the stage (``nominated``,
    ``refused``, ``approval_requested``, ``executed``, ``verified``,
    ``not-recovered``, ``verifier-unavailable``, ``superseded``,
    ``undo_requested``, ``undone``, ``escalated``), the action, target key,
    authority and code, and never any other argument value, an envelope, a read
    result, the model's ``reason`` or the alert body.

    Acceptance: each stage, driven through its real producer, produces exactly
    one thread message naming it; a refusal never produces a "changed" line.

The surface these tests fix (``.projects/plans/task-remediation-receipts.tests.md``):

* ``curie_worker.remediation_receipts`` (``RemediationReceiptLoop``,
  ``PostgresRemediationReceiptStore``, ``render_receipt``), driven by
  ``_receipt_capture.deliver_receipts`` through the worker's real
  ``SlackReplyAdapter`` with a fake ``chat_postMessage``;
* the post goes to the delivery's reply surface (``remediation_delivery_surfaces``
  ``reply_channel``) and, in its thread, ``thread_ts`` = the surface's new nullable
  ``reply_conversation`` (migration 0097), which the protected ingress records;
* the stage of a message is the second word of its first line
  (``Remediation <stage> ...``); the stages a nomination reaches post in
  lifecycle order within one pass: ``refused`` | ``nominated``, then
  ``approval_requested``, ``executed``, the outcome, ``escalated``,
  ``undo_requested``, ``undone``;
* which fact posts which stage: a row not refused posts ``nominated``; an
  approval raised for it posts ``approval_requested`` naming the check; a forward
  execution that ended ``confirmed`` posts ``executed`` (a forward that failed,
  was refused or is indeterminate posts none); a verification outcome posts its
  name; a ``remediation_escalations`` row posts ``escalated`` and, with its
  ``undo_approval_id``, ``undo_requested``; the confirmed restore of an
  escalated record posts ``undone`` under authority ``approval``. A rejection
  or expiry posts nothing further;
* each stage is posted once whatever the number of loops, passes or leases, and
  a post that failed is retried and posted once.

Every identifier is a placeholder.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_rows
from _receipt_capture import (
    ALERT_CHANNEL,
    THREAD,
    Sender,
    deliver_receipts,
    set_threads,
    stage_of,
    whole,
)
from _sealed_actions import executor_enabled  # noqa: F401 - fixture, requested by name
from test_remediation_escalation import (
    NOT_VERIFIED,
    _approval_scenario,
    _claim_dispatch,
    _drive,
    _one_escalation,
    _resolve,
    _restores,
    _undoable_scenario,
)
from test_remediation_verifier import (
    ACT_CONNECTOR,
    ACTION_NAME,
    EVENT_ID,
    HOOK,
    _agent,
    _bind_policy,
    _dispatched,
    _document,
    _end,
)

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled", "runs_stream")

TARGET_KEY = f'{ACT_CONNECTOR}:"example-api"'


def _stages(posts: list[dict[str, Any]]) -> list[str]:
    return [stage_of(post) for post in posts]


def _check_surface(posts: list[dict[str, Any]]) -> None:
    for post in posts:
        assert post["channel"] == ALERT_CHANNEL
        assert post["thread_ts"] == THREAD
        text = whole(post)
        assert ACTION_NAME in text
        assert TARGET_KEY in text


def _refused_row(agent_id: str, code: str = "unknown_action") -> uuid.UUID:
    sql_rows(
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, :hook, :sha) "
        "ON CONFLICT DO NOTHING",
        {"e": EVENT_ID, "agent_id": uuid.UUID(agent_id), "hook": HOOK, "sha": "cd" * 32},
    )
    nomination_id = uuid.uuid4()
    arguments = {"namespace": "example-ns", "deployment": "example-api", "replicas": 4}
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, action, kind, arguments, arguments_sha256, target, "
        "reason, state, refusal_code) "
        "VALUES (:id, :agent_id, :hook, :e, :action, 'remediate', :arguments, :sha, :target, "
        "'reason text', 'refused', :code)",
        {
            "id": nomination_id,
            "agent_id": uuid.UUID(agent_id),
            "hook": HOOK,
            "e": EVENT_ID,
            "action": ACTION_NAME,
            "arguments": json.dumps(arguments, sort_keys=True, separators=(",", ":")),
            "sha": "ab" * 32,
            "target": TARGET_KEY,
            "code": code,
        },
    )
    return nomination_id


# --------------------------------------------------------------------------- #
# One message per stage, in the delivery's thread
# --------------------------------------------------------------------------- #


def test_an_automatic_remediation_posts_nominated_executed_and_verified_once_each(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: each stage is its own message, posted as the
    fact arrives, in the delivery's thread on the delivery's surface, naming the
    action and the target key; policy authority.
    """

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path, reversible=False)
    set_threads()

    first = deliver_receipts()
    assert _stages(first) == ["nominated", "executed"]
    _check_surface(first)
    assert all("policy" in whole(post) for post in first)

    _drive(client, nomination_id, "verified")
    second = deliver_receipts()
    assert _stages(second) == ["verified"]
    _check_surface(second)

    assert deliver_receipts() == []


@pytest.mark.parametrize("outcome", NOT_VERIFIED)
def test_a_non_verified_outcome_posts_its_name_the_report_and_the_undo_offer(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, outcome: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-19: the outcome, the
    failure report (``escalated``) and, for a reversible undoable record, the undo
    offer: one message each, in that order.
    """

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path)
    set_threads()
    assert _stages(deliver_receipts()) == ["nominated", "executed"]

    _drive(client, nomination_id, outcome)
    posts = deliver_receipts()

    assert _stages(posts) == [outcome, "escalated", "undo_requested"]
    _check_surface(posts)
    assert deliver_receipts() == []


def test_a_non_reversible_record_is_escalated_with_no_undo_offer(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: no undo approval, so no ``undo_requested``."""

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path, reversible=False)
    set_threads()
    deliver_receipts()

    _drive(client, nomination_id, "not-recovered")

    assert _stages(deliver_receipts()) == ["not-recovered", "escalated"]


def test_an_approved_undo_that_confirms_posts_undone_under_the_approval_authority(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-19: the restore
    the approver's decision created, once confirmed, is the ``undone`` message.
    """

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    set_threads()
    _drive(client, nomination_id, "not-recovered")
    deliver_receipts()
    undo_approval = _one_escalation(nomination_id)["undo_approval_id"]

    assert _resolve(client, undo_approval, "approved").status_code == 200
    assert deliver_receipts() == []  # an unexecuted restore is not yet an undo
    (restore,) = _restores(action_id)
    fence = _claim_dispatch(client, restore["id"])
    assert _end(client, restore["id"], fence).status_code == 200

    posts = deliver_receipts()

    assert _stages(posts) == ["undone"]
    _check_surface(posts)
    assert "approval" in whole(posts[0])
    assert deliver_receipts() == []


def test_an_approval_remediation_posts_every_stage_under_the_approval_authority(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: out of bounds to approval to execution to
    ``not-recovered`` to report and undo offer: all stages, once, in order, with
    the admission check named on the approval request.
    """

    _approval_scenario(client, auth_headers, tmp_path)
    set_threads()

    posts = deliver_receipts()

    assert _stages(posts) == [
        "nominated",
        "approval_requested",
        "executed",
        "not-recovered",
        "escalated",
        "undo_requested",
    ]
    _check_surface(posts)
    assert "out_of_bounds" in whole(posts[1])
    assert "approval" in whole(posts[2])
    assert deliver_receipts() == []


def test_a_refusal_posts_one_refused_message_and_never_a_change(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: "a refusal never produces a 'changed' line":
    one ``refused`` message naming the code, no ``nominated``.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    _refused_row(agent_id)
    set_threads()

    posts = deliver_receipts()

    assert _stages(posts) == ["refused"]
    _check_surface(posts)
    text = whole(posts[0])
    assert "unknown_action" in text
    assert "changed" not in text.lower()
    assert deliver_receipts() == []


def test_a_forward_that_failed_posts_not_recovered_and_the_report_but_no_executed(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-18: nothing was
    executed, so no ``executed`` message; the message never names an execution
    code the closed vocabulary does not hold.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    _bind_policy(agent_id, _document())
    from test_remediation_verifier import _nominate

    nomination_id = _nominate(agent_id)
    execution_id, fence = _dispatched(client, nomination_id)
    assert _end(client, execution_id, fence, "failed").status_code == 200
    set_threads()

    posts = deliver_receipts()

    assert _stages(posts) == ["nominated", "not-recovered", "escalated"]
    assert all("connector_error" not in whole(post) for post in posts)


def test_a_rejected_approval_posts_nothing_beyond_the_request(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: a rejection has no stage of its own."""

    from test_remediation_approvals import APPROVER, _nominate, _request, _setup

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    requested = _request(agent_id, nomination_id)
    set_threads()
    assert _stages(deliver_receipts()) == ["nominated", "approval_requested"]

    assert _resolve(client, requested.approval_id, "rejected", APPROVER).status_code == 200

    assert deliver_receipts() == []


# --------------------------------------------------------------------------- #
# Exactly once
# --------------------------------------------------------------------------- #


def test_two_leases_racing_post_each_stage_once(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: two worker replicas never double-post."""

    _approval_scenario(client, auth_headers, tmp_path)
    set_threads()

    posts = deliver_receipts(owners=("worker-a", "worker-b"))

    assert len(posts) == len(set(_stages(posts))) == 6
    assert deliver_receipts(owners=("worker-a", "worker-b")) == []


def test_a_failed_post_is_retried_and_lands_once(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-20: a Slack failure loses no stage and repeats none."""

    _undoable_scenario(client, auth_headers, tmp_path, reversible=False)
    set_threads()
    sender = Sender(fail_first=1)

    with pytest.raises(Exception, match="transient"):
        deliver_receipts(sender)
    deliver_receipts(sender)

    assert _stages(sender.posts) == ["nominated", "executed"]
    assert deliver_receipts(sender) == []


def test_the_turn_receipt_of_adr_0117_is_unchanged() -> None:
    """@spec AUTOMATED-REMEDIATION-20: "The turn receipt of ADR 0117 is unchanged": the
    receipt module's modes are still exactly ``all``, ``failures`` and ``off``.
    """

    from curie_worker import receipt

    assert set(receipt.TurnReceiptMode.__args__) == {"all", "failures", "off"}  # type: ignore[attr-defined]

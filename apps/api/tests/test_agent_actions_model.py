"""What an action record may claim about itself (ADR-0117, ACTION-EXECUTOR-11).

The ledger invariant this file exists to hold: a record claims the record-level
half of reversibility ONLY when it holds every ingredient a pinned, sealed
restore needs -- a successful outcome, an agent, a valid sealed envelope in
``prior_state``, a ``post_version``, a ``target``, a ``connector`` and its
``connector_digest`` -- and has not been undone. That half is derived from the
columns rather than stored beside them: a stored flag can be set by a writer
that captured nothing, and the platform would then offer an undo button it
cannot honor.

The other half of ``undoable`` (capability, key custody, no live restore) needs
the database and the in-force bundle, and is held through the real API in
``test_action_undoable_ingredients.py``.
"""

from __future__ import annotations

import base64
import uuid
from datetime import datetime

from curie_api.models import ActionStatus, AgentAction

ENVELOPE = {
    "sealed": "curie.snapshot.v1",
    "kid": "seal-2026-10",
    "ciphertext": base64.b64encode(b"opaque sealed prior state").decode(),
}


def _action(**overrides: object) -> AgentAction:
    fields: dict[str, object] = {
        "agent_id": uuid.uuid4(),
        "conversation_id": "C1",
        "call_id": "toolu_01",
        "tool": "mcp__k8s__scale",
        "dedupe_key": f"{uuid.uuid4()}",
        "status": ActionStatus.succeeded,
        "prior_state": ENVELOPE,
        "post_version": "rv-1042",
        "target": {"kind": "Deployment", "namespace": "public", "name": "api"},
        "connector": "k8s",
        "connector_digest": "sha256:" + "ab" * 32,
    }
    fields.update(overrides)
    return AgentAction(**fields)


def test_a_complete_sealed_successful_record_holds_a_restore_record() -> None:
    """@spec ACTION-EXECUTOR-11: every record-level ingredient is present."""

    assert _action().holds_restore_record is True


def test_a_record_without_a_prior_state_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11.

    The connector answered in prose, or never sealed what it overwrote.
    """

    assert _action(prior_state=None).holds_restore_record is False


def test_a_cleartext_prior_state_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11: a legacy cleartext snapshot is history, never restorable state."""

    assert _action().holds_restore_record is True
    assert _action(prior_state={"spec": {"replicas": 3}}).holds_restore_record is False


def test_a_record_without_a_target_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11.

    A state to restore is useless without the resource to restore it onto.
    """

    assert _action(target=None).holds_restore_record is False


def test_a_record_without_a_post_version_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-15.

    Nothing to compare the live version against.

    The platform compares the version observed now with ``post_version`` before
    any restore; without it every ruling would be refused, so the record must
    not claim it.
    """

    assert _action(post_version=None).holds_restore_record is False


def test_a_post_state_is_no_longer_required() -> None:
    """@spec ACTION-EXECUTOR-9: "``post`` is no longer required or read for a sealed record"."""

    assert _action(post_state=None).holds_restore_record is True


def test_a_record_without_a_pinned_connector_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-14.

    The restore runs under the pinned digest or nothing.
    """

    assert _action(connector_digest=None).holds_restore_record is False
    assert _action(connector=None).holds_restore_record is False


def test_a_record_without_an_agent_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11: refused_no_agent -- no binding to run the restore under."""

    assert _action(agent_id=None).holds_restore_record is False


def test_a_failed_call_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11.

    Undoing a call that did not happen would be a write, not a restore.
    """

    assert _action(status=ActionStatus.failed).holds_restore_record is False


def test_an_unfinished_call_does_not_hold_one() -> None:
    """@spec ACTION-EXECUTOR-11: the opening frame is on the wire and no result has arrived.

    A turn that dies here leaves this row standing: an honest record of an
    attempt, deny-by-default on reversibility.
    """

    assert _action(status=ActionStatus.pending, result=None).holds_restore_record is False


def test_an_already_undone_record_does_not_hold_one_again() -> None:
    """@spec ACTION-EXECUTOR-11: one record, one restore. A second would replay a state twice."""

    assert _action(undone_at=datetime.now()).holds_restore_record is False


def test_undoable_is_not_a_column() -> None:
    """It is derived, so no writer can set it to something it did not capture."""

    assert "undoable" not in AgentAction.__table__.columns

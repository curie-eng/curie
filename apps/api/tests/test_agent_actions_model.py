"""What an action record may claim about itself (ADR-0117).

The ledger invariant this file exists to hold: a record claims to be undoable
ONLY when it holds a prior state, a post state, a target, and a successful
outcome. ``undoable`` is therefore derived from those columns rather than stored
beside them --
a stored flag can be set by a writer that captured nothing, and the platform
would then offer an undo button it cannot honor.
"""

from __future__ import annotations

import uuid

from curie_api.models import ActionStatus, AgentAction


def _action(**overrides: object) -> AgentAction:
    fields: dict[str, object] = {
        "conversation_id": "C1",
        "call_id": "toolu_01",
        "tool": "scale_deployment",
        "dedupe_key": f"{uuid.uuid4()}",
        "status": ActionStatus.succeeded,
        "prior_state": {"spec": {"replicas": 3}},
        "post_state": {"spec": {"replicas": 10}},
        "target": {"kind": "Deployment", "namespace": "public", "name": "api"},
    }
    fields.update(overrides)
    return AgentAction(**fields)


def test_a_complete_successful_record_is_undoable() -> None:
    assert _action().undoable is True


def test_a_record_without_a_prior_state_is_not_undoable() -> None:
    """The connector answered in prose, or never read the state it overwrote."""

    assert _action(prior_state=None).undoable is False


def test_a_record_without_a_target_is_not_undoable() -> None:
    """A state to restore is useless without the resource to restore it onto."""

    assert _action(target=None).undoable is False


def test_a_record_without_a_post_state_is_not_undoable() -> None:
    """The undo route refuses it, so the record must not claim it (#1861).

    ``post`` is what the live resource is compared against before a restore.
    Without it the route answers ``refused_uncomparable`` every time, and a row
    that still called itself undoable would promise a restore nothing can grant.
    """

    assert _action(post_state=None).undoable is False


def test_a_failed_call_is_not_undoable() -> None:
    """Undoing a call that did not happen would be a write, not a restore."""

    assert _action(status=ActionStatus.failed).undoable is False


def test_an_unfinished_call_is_not_undoable() -> None:
    """The opening frame is on the wire and no result has arrived.

    A turn that dies here leaves this row standing: an honest record of an
    attempt, deny-by-default on reversibility.
    """

    assert _action(status=ActionStatus.pending, result=None).undoable is False


def test_an_already_undone_record_is_not_undoable_again() -> None:
    """One record, one restore. A second would replay a state twice."""

    assert _action(undone_at=__import__("datetime").datetime.now()).undoable is False


def test_undoable_is_not_a_column() -> None:
    """It is derived, so no writer can set it to something it did not capture."""

    assert "undoable" not in AgentAction.__table__.columns

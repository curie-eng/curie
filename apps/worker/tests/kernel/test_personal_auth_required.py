"""An authentication-required runner failure escalates once, with its own cause (#5377).

ADR 0268 and #5377 decision 9: a personal run whose owner grant is missing or
was rejected by the provider ends ``authentication-required``. The worker keeps
the runner's class instead of collapsing it to ``unclassified``, never retries it
(the same grant fails again, and no service credential may stand in), and a
factory run finishes ``failed`` with cause ``authentication_required`` so the
API keeps the owner and transcript for an explicit resume.

Real Valkey, the real substrate over the fake Kubernetes client, and the kernel
suite's in-process fake runner; only the model behind the runner (here, its
scripted frames) and the dispatch API are doubles, as in the rest of the suite.
"""

from __future__ import annotations

import asyncio
import uuid

from aci_protocol import ErrorEvent, Final, SessionStatus
from curie_worker.kernel.constants import RETRYABLE_CLASSIFICATIONS
from curie_worker.kernel.failures import _escalation_cause, map_error_classification

from apps.worker.tests.queue_fixtures import qevent

from .test_work_item_workspace import (
    ISSUE_URL,
    _Binding,
    _NoExistingPublication,
    _turn,
    _WorkItems,
    _Workspace,
)

# The runner's message names the enrollment, never a credential value.
MESSAGE = (
    "authentication required: the provider rejected the personal Claude grant; "
    "reconnect or replace the personal Claude enrollment"
)


def _script() -> list[object]:
    return [
        ErrorEvent(message=MESSAGE, classification="authentication-required"),
        Final(text="run failed: authentication required", status=SessionStatus.CLASSIFIED_FAILURE),
    ]


def test_authentication_required_escalates_once_and_finishes_failed_with_its_cause(
    make_harness,
) -> None:
    assert "authentication-required" not in RETRYABLE_CLASSIFICATIONS
    assert map_error_classification("authentication-required") == "authentication-required"

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
            max_attempts=3,
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = _script()
            request_id = uuid.uuid4()
            event = _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")

            await h.kernel.process_event(event)

            assert len(h.runner.opened) == 1, "an authentication failure must not be retried"
            assert work_items.calls.count("finish") == 1
            [finish] = work_items.finishes
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "authentication_required"
            assert finish["detail"] == MESSAGE
            assert await h.kernel._markers.is_terminal(event.event_id)

    asyncio.run(exercise())


def test_authentication_required_chat_turn_names_its_class_after_one_attempt(
    make_harness,
) -> None:
    async def exercise() -> None:
        async with make_harness(max_attempts=3) as h:
            h.runner.default_script = _script()
            event = qevent("go", event_id="evt-authentication-required")

            await h.kernel.process_event(event)

            assert h.runner.opened == ["go"]
            assert h.sink.last_text == (
                "curie-turn-failure: authentication-required\n\n"
                "The run failed (authentication-required) after 1 attempt(s). "
                f"{MESSAGE} event_id=evt-authentication-required. Flagging for a human."
            )

    asyncio.run(exercise())


def test_escalation_cause_is_authentication_required() -> None:
    class _Outcome:
        classification = "authentication-required"

    assert _escalation_cause(_Outcome()) == "authentication_required"  # type: ignore[arg-type]

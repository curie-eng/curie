"""The question ending through the worker's completion path (#5785, ADR 0286).

The runner ends a factory turn once its question round is posted (decision
11). ``tests/vectors/question-halt-turn.json`` pins the frames that halted turn
emits on the real bundled CLI (runner/tests/test_question_halt_real_cli.py) and
the API's internal request view for that request while it runs
(apps/api/tests/test_factory_question_ending.py). These cases replay both
through the real kernel: the frames from the conftest runner, the request view
through the real ``WorkItemDispatchClient`` over an ``httpx.MockTransport``.
The finish the worker reports is pinned in the same vector, and the API test
settles exactly that finish completed ``questions_asked``.

Decision 2 ends the request when the factory asks, so a turn whose round the
API recorded as posted is not re-prompted to publish (#3128's continuation).
An ask that posted nothing still gets the continuation.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import httpx
from aci_protocol import OutboundEvent, TextDelta, parse_ndjson
from aci_protocol.version import PROTOCOL_VERSION
from curie_worker.workitem_dispatch import WorkItemDispatchClient, WorkItemRequestView

from .test_work_item_early_stop import ISSUE_PROMPT, _done, _run_execute, _WorkItems

VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[4] / "tests" / "vectors" / "question-halt-turn.json"
    ).read_text(encoding="utf-8")
)
QUESTION_TOOL = "mcp__curie__ask_questions"


def _frames(pinned: list[dict[str, Any]]) -> list[OutboundEvent]:
    lines = "".join(json.dumps({**frame, "version": PROTOCOL_VERSION}) + "\n" for frame in pinned)
    return list(parse_ndjson(lines))


HALTED_TURN = _frames(VECTOR["runner_frames"])


class _ApiRequestView(_WorkItems):
    """The dispatch double, reading the request view through the real client.

    ``view`` is the body the API's ``GET /v1/internal/work-items/requests/{id}``
    answers, merged over the fields the client always reads.
    """

    def __init__(self, view: dict[str, Any], *, status_code: int = 200) -> None:
        super().__init__()
        self.view = view
        self.status_code = status_code
        self.views: list[WorkItemRequestView] = []

    async def get_request(self, request_id: uuid.UUID) -> WorkItemRequestView:
        self.calls.append("get_request")
        body = {
            "id": str(request_id),
            "runtime_epoch": 1,
            "runtime_claim_name": None,
            "runtime_sandbox_name": None,
            **self.view,
        }

        def answer(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/v1/internal/work-items/requests/{request_id}"
            if self.status_code != 200:
                return httpx.Response(self.status_code, json={"detail": "unavailable"})
            return httpx.Response(200, json=body)

        async with httpx.AsyncClient(transport=httpx.MockTransport(answer)) as client:
            view = await WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="worker-token", client=client
            ).get_request(request_id)
        self.views.append(view)
        return view


def _asked_then_ended(text: str) -> list[OutboundEvent]:
    """A turn whose model asked, then ended on its own with a message."""

    halted = [frame for frame in HALTED_TURN if getattr(frame, "tool", None) == QUESTION_TOOL]
    return [*halted, TextDelta(text=text), _done(text)]


def test_the_halted_turn_finishes_with_no_continuation(make_harness) -> None:
    """(a) The runner's real halted turn: one turn, then the pinned finish.

    Red without the worker fix: the posted round does not stop the #3128
    continuation, so a second prompt asks the model to publish.
    """

    async def exercise() -> None:
        items = _ApiRequestView(VECTOR["request_view"])
        opened, items, _ = await _run_execute(
            make_harness,
            [HALTED_TURN, [_done("continued after asking")]],
            work_items=items,
        )

        assert opened == [ISSUE_PROMPT]
        assert [view.questions_posted for view in items.views] == [True]
        [finish] = items.finishes
        assert {key: finish[key] for key in VECTOR["worker_finish"]} == VECTOR["worker_finish"]

    asyncio.run(exercise())


def test_a_turn_that_ends_on_its_own_after_asking_gets_no_continuation(make_harness) -> None:
    """(b) The model ends its turn normally after a posted ask: no re-prompt.

    Red without the worker fix, for the same reason as (a).
    """

    async def exercise() -> None:
        items = _ApiRequestView(VECTOR["request_view"])
        opened, items, _ = await _run_execute(
            make_harness,
            [
                _asked_then_ended("I posted my questions on the ticket."),
                [_done("continued after asking")],
            ],
            work_items=items,
        )

        assert opened == [ISSUE_PROMPT]
        [finish] = items.finishes
        assert (finish["outcome"], finish["cause"]) == ("failed", "no_pull_request")
        assert finish["detail"] == "I posted my questions on the ticket."

    asyncio.run(exercise())


def test_an_ask_that_posted_nothing_still_gets_the_continuation(make_harness) -> None:
    """Liveness: a refused or failed ask records no round, so #3128 still applies."""

    async def exercise() -> None:
        items = _ApiRequestView({**VECTOR["request_view"], "questions_posted": False})
        opened, items, _ = await _run_execute(
            make_harness,
            [_asked_then_ended("The ask was refused."), [_done("I could not finish.")]],
            work_items=items,
        )

        assert len(opened) == 2
        assert "publish_changes" in opened[1]
        [finish] = items.finishes
        assert finish["detail"] == "I could not finish."

    asyncio.run(exercise())


def _older_api_view() -> dict[str, Any]:
    view = dict(VECTOR["request_view"])
    view.pop("questions_posted")
    return view


def test_an_unconfirmed_ask_is_not_reprompted(make_harness) -> None:
    """A turn that called ask_questions whose round the API did not confirm.

    An older API replica omits the field, or the status read fails: the ask
    may have posted, so the request ends rather than continuing to publish.
    """

    async def exercise() -> None:
        for items in (_ApiRequestView(_older_api_view()), _ApiRequestView({}, status_code=503)):
            opened, items, _ = await _run_execute(
                make_harness,
                [_asked_then_ended("Asked."), [_done("continued after asking")]],
                work_items=items,
            )

            assert opened == [ISSUE_PROMPT]
            [finish] = items.finishes
            assert finish["detail"] == "Asked."

    asyncio.run(exercise())


def test_a_turn_that_never_asked_keeps_the_continuation_on_an_unconfirmed_read(
    make_harness,
) -> None:
    """Liveness: with no ask, an older API or a failed read re-prompts as before."""

    async def exercise() -> None:
        never_asked = [
            _frames(
                [
                    {"type": "tool_note", "text": "running tool Bash", "tool": "Bash"},
                    {"type": "text_delta", "text": "Working."},
                    {"type": "final", "text": "Working.", "status": "done"},
                ]
            ),
            [_done("I could not finish.")],
        ]
        for items in (_ApiRequestView(_older_api_view()), _ApiRequestView({}, status_code=503)):
            opened, items, _ = await _run_execute(make_harness, never_asked, work_items=items)

            assert len(opened) == 2
            assert "publish_changes" in opened[1]

    asyncio.run(exercise())

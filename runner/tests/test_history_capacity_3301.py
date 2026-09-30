"""#3301: a dark-factory turn must persist at the default per-thread transcript cap.

Three plan-review rounds with a subagent reviewer leave more than 56 KiB that
bounding must keep (signed thinking blocks, the boundary messages, the latest
exchange), so the old 64 KiB default refused the turn at the end of the run and
lost everything it did. The turn drives the real ``StateApiTranscriptStore``
over HTTP against the capped fake state API from the #2927 suite.
"""

from __future__ import annotations

import anyio
import pytest
from aci_protocol import ErrorEvent, parse_ndjson_line
from aiohttp.test_utils import TestServer
from curie_runner.history import (
    ConversationMessage,
    HistoryCapacityError,
    StateApiTranscriptStore,
    TurnRecord,
)
from curie_runner.session import _history_capacity_lines
from runner_state_fake import TRANSCRIPT_KEY as _KEY
from runner_state_fake import CappedCasState

# The API default (apps/api config ``transcript_max_thread_bytes``, chart value
# ``api.transcriptMaxThreadBytes``). The API suite pins the same number.
_DEFAULT_CAP = 16 * 1024 * 1024
_OLD_CAP = 64 * 1024
_USER = "Implement issue #3129 through the dark factory"


def _factory_turn(rounds: int = 3) -> TurnRecord:
    messages: list[ConversationMessage] = [ConversationMessage(role="user", content=_USER)]
    for n in range(1, rounds + 1):
        messages.append(
            ConversationMessage(
                role="assistant",
                content=[
                    {
                        "type": "thinking",
                        "thinking": f"plan round {n} reasoning " * 1_000,
                        "signature": f"opaque-signature-round-{n}",
                    },
                    {"type": "text", "text": f"Plan round {n}:\n" + "- step detail\n" * 400},
                    {
                        "type": "tool_use",
                        "id": f"review-{n}",
                        "name": "Task",
                        "input": {"subagent_type": "reviewer", "prompt": "Review the plan"},
                    },
                ],
            )
        )
        messages.append(
            ConversationMessage(
                role="user",
                content=[
                    {
                        "type": "tool_result",
                        "tool_use_id": f"review-{n}",
                        "content": f"Round {n} finding: tighten scope. " * 800,
                    }
                ],
            )
        )
    answer = "Plan approved after three review rounds; implementation follows."
    # The final synthesis over every round's review is signed, so bounding keeps it.
    synthesis = {
        "type": "thinking",
        "thinking": "weigh all three reviews and settle the plan " * 1_600,
        "signature": "opaque-signature-final",
    }
    messages.append(
        ConversationMessage(role="assistant", content=[synthesis, {"type": "text", "text": answer}])
    )
    return TurnRecord(user=_USER, assistant=answer, messages=tuple(messages))


def _append(state: CappedCasState, record: TurnRecord) -> None:
    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            await store.append(record)

    anyio.run(go)


def test_factory_turn_persists_at_the_default_cap() -> None:
    state = CappedCasState(max_bytes=_DEFAULT_CAP)
    _append(state, _factory_turn())

    assert state.value is not None
    stored = state.value[-1]
    assert stored["user"] == _USER
    # Nothing was compacted away: every plan round's reviewer output is exact.
    assert TurnRecord.from_dict(stored) == _factory_turn()


def test_unboundable_turn_names_its_size_and_the_cap() -> None:
    state = CappedCasState(max_bytes=_OLD_CAP)
    with pytest.raises(HistoryCapacityError) as caught:
        _append(state, _factory_turn())

    detail = caught.value.detail
    assert caught.value.status == 413
    assert detail is not None
    assert f"{_OLD_CAP - 8_192}-byte" in detail
    assert "api.transcriptMaxThreadBytes" in detail
    size = int(detail.split("one turn is ")[1].split(" bytes")[0])
    assert size > _OLD_CAP - 8_192
    # The runner refused before writing anything.
    assert state.value is None

    error = parse_ndjson_line(_history_capacity_lines(detail)[0])
    assert isinstance(error, ErrorEvent)
    assert error.classification == "history-persistence-error"
    assert error.message == f"conversation history capacity exceeded: {detail}"

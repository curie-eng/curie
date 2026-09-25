"""#2927: the pure capacity compaction of a stored transcript value.

``compact_transcript_value`` rewrites a transcript array that no longer fits
under the state API's whole-value cap with the append headroom reserved:
worker publication markers first and verbatim, then one summary of everything
but the latest turn, then that latest turn without native replay state, bounded
to what is left.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest
from curie_runner.history import (
    HISTORY_APPEND_RESERVE_BYTES,
    HISTORY_VALUE_MAX_BYTES,
    ConversationMessage,
    HarnessReplayState,
    HistoryCapacityError,
    HistoryError,
    SummaryRecord,
    TurnRecord,
    _parse_records,
    bound_turn_record,
    build_conversation_replay,
    compact_transcript_value,
)


def _size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))


def _turn(tag: str, pad: int, *, native: bool = False) -> dict[str, Any]:
    return TurnRecord(
        user=f"{tag} request",
        assistant=f"{tag} answer",
        ts=f"2026-09-22T00:00:00Z-{tag}",
        messages=(
            ConversationMessage(role="user", content=f"{tag} request"),
            ConversationMessage(
                role="assistant",
                content=[
                    {
                        "type": "tool_use",
                        "id": f"{tag}-1",
                        "name": "Bash",
                        "input": {"command": "pytest"},
                    }
                ],
            ),
            ConversationMessage(
                role="user",
                content=[
                    {
                        "type": "tool_result",
                        "tool_use_id": f"{tag}-1",
                        "content": f"{tag}:" + "o" * pad,
                    }
                ],
            ),
            ConversationMessage(
                role="assistant", content=[{"type": "text", "text": f"{tag} answer"}]
            ),
        ),
        harness_replay=(
            HarnessReplayState(
                harness="claude",
                kind="checkpoint",
                entries=({"uuid": f"{tag}-native", "payload": "n" * 3_000},),
            )
            if native
            else None
        ),
    ).to_dict()


def _marker(n: int, text_bytes: int = 1_900) -> dict[str, Any]:
    return {
        "user": "Platform publication outcome",
        "assistant": f"outcome {n} " + ("m" * text_bytes),
        "ts": f"2026-09-22T00:00:0{n}Z",
        "publication_id": f"00000000-0000-0000-0000-00000000000{n}",
    }


def test_reserve_is_the_publication_headroom() -> None:
    assert HISTORY_APPEND_RESERVE_BYTES == 8_192
    assert HISTORY_VALUE_MAX_BYTES == 65_536


def test_markers_first_verbatim_then_summary_then_the_latest_turn_without_native_state() -> None:
    first_marker = _marker(1)
    second_marker = _marker(2)
    value = [
        _turn("one", 20_000, native=True),
        first_marker,
        _turn("two", 20_000, native=True),
        second_marker,
        _turn("three", 20_000, native=True),
    ]
    assert _size(value) > HISTORY_VALUE_MAX_BYTES

    compacted = compact_transcript_value(value)

    assert compacted[:2] == [first_marker, second_marker]
    assert [item.get("type") for item in compacted[2:]] == ["summary", "turn"]
    summary = SummaryRecord.from_dict(compacted[2])
    assert "one request" in summary.content
    assert "two request" in summary.content
    kept = TurnRecord.from_dict(compacted[3])
    assert kept.user == "three request"
    assert kept.harness_replay is None
    assert compacted[3]["harness_replay"] is None
    assert _size(compacted) <= HISTORY_VALUE_MAX_BYTES - HISTORY_APPEND_RESERVE_BYTES


def test_prior_summary_content_carries_into_the_new_summary() -> None:
    prior = SummaryRecord(
        content="PRIOR-SUMMARY earlier decisions",
        digest="a" * 64,
        source_turns=4,
        through_ts="2026-09-21T00:00:00Z",
        tail=(TurnRecord.from_dict(_turn("tail", 15_000)),),
    ).to_dict()
    value = [_turn("zero", 10_000), prior, _turn("after", 25_000), _turn("latest", 25_000)]

    compacted = compact_transcript_value(value)

    summaries = [item for item in compacted if item.get("type") == "summary"]
    assert len(summaries) == 1
    summary = SummaryRecord.from_dict(summaries[0])
    assert summary.content.startswith("PRIOR-SUMMARY earlier decisions")
    assert "tail request" in summary.content
    assert "after request" in summary.content
    assert summary.tail == ()
    assert compacted[-1]["user"] == "latest request"
    assert compacted.index(summaries[0]) < len(compacted) - 1
    assert _size(compacted) <= HISTORY_VALUE_MAX_BYTES - HISTORY_APPEND_RESERVE_BYTES


def test_near_cap_latest_turn_is_bounded_to_leave_the_reserve() -> None:
    latest = _turn("huge", 63_000)
    assert _size([latest]) <= HISTORY_VALUE_MAX_BYTES
    value = [_turn("small", 500), latest]

    compacted = compact_transcript_value(value)

    assert _size(compacted) <= HISTORY_VALUE_MAX_BYTES - HISTORY_APPEND_RESERVE_BYTES
    kept = compacted[-1]
    assert kept["user"] == "huge request"
    original = "huge:" + "o" * 63_000
    bounded_result = kept["messages"][2]["content"][0]["content"]
    assert original not in json.dumps(kept)
    assert hashlib.sha256(original.encode("utf-8")).hexdigest() in bounded_result
    assert [message["role"] for message in kept["messages"]] == [
        "user",
        "assistant",
        "user",
        "assistant",
    ]


def test_one_turn_with_nothing_to_summarize_is_only_bounded() -> None:
    value = [_turn("alone", 62_000, native=True)]

    compacted = compact_transcript_value(value)

    assert len(compacted) == 1
    assert compacted[0]["user"] == "alone request"
    assert compacted[0]["harness_replay"] is None
    assert _size(compacted) <= HISTORY_VALUE_MAX_BYTES - HISTORY_APPEND_RESERVE_BYTES


def test_turn_still_refuses_when_even_the_final_answer_cannot_fit() -> None:
    record = TurnRecord.from_dict(_turn("irreducible", 0))

    with pytest.raises(HistoryError):
        bound_turn_record(record, max_value_bytes=100)


def test_explicit_cap_and_reserve_are_honored() -> None:
    value = [_turn("a", 3_000), _turn("b", 3_000)]

    compacted = compact_transcript_value(value, max_value_bytes=6_000, reserve_bytes=1_000)

    assert _size(compacted) <= 5_000
    assert compacted[-1]["user"] == "b request"


def test_markers_alone_over_the_cap_are_irreducible() -> None:
    value = [*(_marker(n, 9_000) for n in range(7)), _turn("last", 1_000)]
    assert _size(value[:-1]) > HISTORY_VALUE_MAX_BYTES - HISTORY_APPEND_RESERVE_BYTES

    with pytest.raises(HistoryCapacityError) as caught:
        compact_transcript_value(value)
    assert caught.value.status == 413


def test_publication_outcome_stays_visible_in_the_compacted_replay() -> None:
    """#2927 review P1: compaction keeps the worker's marker AND its outcome text.

    The marker is the publication idempotency record, but its assistant text is
    also the only place the resumed model learns the pull request was published.
    After compaction the replay built from the stored value must still carry it.
    """

    url = "https://github.com/acme-corp/pricing/pull/4"
    outcome_text = f"Published PR #4 at {url}"
    marker = {
        "user": "Platform publication outcome",
        "assistant": outcome_text,
        "ts": "2026-09-22T00:00:02Z",
        "publication_id": "00000000-0000-0000-0000-000000000004",
    }
    value = [_turn("older", 20_000), marker, _turn("latest", 20_000)]

    compacted = compact_transcript_value(value)

    # The raw idempotency marker still exists verbatim.
    assert marker in compacted
    replay, _summary = build_conversation_replay(_parse_records(compacted))
    replay_text = json.dumps([message.to_dict() for message in replay.messages])
    assert outcome_text in replay_text, "the publication outcome vanished from the replay"


def test_publication_outcome_survives_a_saturated_prior_summary() -> None:
    """#2927 review P1: a prior summary already at the ``_make_summary`` budget
    must not crowd out a newly recorded publication outcome.

    Compaction folds the marker's turn into a fresh summary alongside the prior
    summary's content. If that fresh summary is bounded by keeping the OLDEST
    bytes (the prior content) rather than the newest, the just-added outcome
    text is discarded entirely: the resumed model can never see it, even though
    the raw marker survives verbatim ahead of the summary (where replay ignores
    it, per ``build_conversation_replay``'s docstring).
    """

    url = "https://github.com/o/r/pull/4"
    outcome_text = f"Published PR #4 at {url}"
    saturated_prior = SummaryRecord(
        content="OLDER-SUMMARY " + ("x" * 8_000),
        digest="a" * 64,
        source_turns=6,
        through_ts="2026-09-21T00:00:00Z",
        tail=(),
        ts="2026-09-21T00:00:00Z",
    ).to_dict()
    marker = {
        "user": "Platform publication outcome",
        "assistant": outcome_text,
        "ts": "2026-09-22T00:00:02Z",
        "publication_id": "00000000-0000-0000-0000-000000000004",
    }
    value = [saturated_prior, marker, _turn("latest", 20_000)]

    compacted = compact_transcript_value(value)

    # The raw idempotency marker still exists verbatim, ahead of the summary --
    # but replay ignores anything before the latest summary, so this alone does
    # not make the outcome visible to the resumed model.
    assert marker in compacted

    replay, _summary = build_conversation_replay(_parse_records(compacted))
    replay_text = json.dumps([message.to_dict() for message in replay.messages])
    assert outcome_text in replay_text, "the publication outcome vanished from the replay"
    assert "latest request" in replay_text

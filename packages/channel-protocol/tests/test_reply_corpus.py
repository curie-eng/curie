"""The reply-wire compatibility corpus, decoded the way an adapter decodes it.

ADR-0130 section 4 adds reply wire 1.1 and requires regenerated compatibility
fixtures with it. ``schema/reply-wire.corpus.json`` is those fixtures, and this
file reads them through ``TypeAdapter(ReplyEvent)``, the same decoder the
Discord adapter (``adapters/discord/src/curie_discord_adapter/http.py``) and
the mail adapter (``apps/mail-adapter/src/curie_mail_adapter/egress.py``) run.

The ``v1_0`` bodies were captured from the 1.0-only package before 1.1 existed.
Re-serializing each one to the same bytes is what shows that adding 1.1 left
every 1.0 form alone: the worker sends ``model_dump_json()`` verbatim, and a
1.0-built adapter's closed models refuse any key they do not know, including a
new key sent as null.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from channel_protocol.reply import ReplyEvent
from pydantic import TypeAdapter, ValidationError

_EVENTS: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)
_CORPUS: dict[str, Any] = json.loads(
    (Path(__file__).parents[1] / "schema" / "reply-wire.corpus.json").read_text(
        encoding="utf-8"
    )
)
_PROGRESS_WIRE_KEYS = ("delivery_id", "progress")


def _wire_bytes(body: dict[str, Any]) -> str:
    """The compact serialization the corpus header names, in the corpus's key order."""
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False)


def _cases(section: str) -> list[Any]:
    return [pytest.param(case, id=case["name"]) for case in _CORPUS[section]]


@pytest.mark.parametrize("case", _cases("v1_0"))
def test_every_1_0_body_decodes_and_reserializes_to_the_same_bytes(
    case: dict[str, Any],
) -> None:
    wire = _wire_bytes(case["body"])
    event = _EVENTS.validate_json(wire)
    assert event.version == "1.0"
    assert event.event == case["body"]["event"]
    assert event.model_dump_json() == wire


@pytest.mark.parametrize("case", _cases("v1_0"))
def test_no_1_0_body_carries_a_1_1_key(case: dict[str, Any]) -> None:
    # Guards the fixture itself: a 1.0 case that grew a 1.1 key would still
    # round-trip under a model that emits it, and would stop proving anything
    # about what a 1.0-built adapter receives.
    assert not set(case["body"]) & set(_PROGRESS_WIRE_KEYS)


@pytest.mark.parametrize("case", _cases("v1_1"))
def test_every_1_1_body_decodes_and_reserializes_to_the_same_bytes(
    case: dict[str, Any],
) -> None:
    wire = _wire_bytes(case["body"])
    event = _EVENTS.validate_json(wire)
    assert event.version == "1.1"
    assert event.event == case["body"]["event"]
    assert event.model_dump_json() == wire


@pytest.mark.parametrize("case", _cases("refused"))
def test_every_refused_body_is_refused_for_its_stated_reason(case: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as caught:
        _EVENTS.validate_json(_wire_bytes(case["body"]))
    expected = case["error"]
    errors = caught.value.errors()
    matching = [
        error
        for error in errors
        if error["type"] == expected["type"]
        and expected.get("contains", "") in str(error["msg"])
        and ("at" not in expected or error["loc"][-1] == expected["at"])
    ]
    # The reason, not just the refusal: before 1.1 existed, several of these
    # bodies were already refused, but as an unknown key or an unknown version.
    # A decoder that refuses them for that reason has not implemented the rule
    # the case names.
    assert matching, f"{case['name']}: expected {expected}, got {errors}"


def test_the_corpus_covers_every_event_and_every_progress_form() -> None:
    # Deleting a case must not quietly shrink what the corpus proves.
    v1_0_events = {case["body"]["event"] for case in _CORPUS["v1_0"]}
    assert v1_0_events == {"turn.status", "reply.update", "reply.post", "turn.completed"}
    forms = {
        (case["body"]["event"], case["body"].get("progress", {}).get("kind"))
        for case in _CORPUS["v1_1"]
    }
    assert {
        ("reply.post", "card"),
        ("reply.update", "card"),
        ("reply.post", "milestone"),
        ("reply.post", None),
        ("reply.update", None),
    } <= forms
    milestone_classes = {
        case["body"]["progress"]["milestone"]
        for case in _CORPUS["v1_1"]
        if case["body"].get("progress", {}).get("kind") == "milestone"
    }
    assert milestone_classes == {"evidence", "scope", "verification"}

"""The Slack progress card and milestone Block Kit (ADR-0130), pure.

The Slack contract these builders render is in the worker README section "How
the Slack adapter renders progress". The block id prefixes are the seam with
the CLI's Slack stub and are frozen in ``tests/vectors/progress-blocks.json``;
``test_progress_block_ids_match_the_frozen_vector`` is this lane's half of that
gate and ``progress_block_ids_match_the_frozen_vector`` in ``cli/src/chat.rs``
is the other.

Provider grounding for the Block Kit rules pinned here:

- A ``plain_text`` text object is shown as written; mentions and links are
  parsed out of ``mrkdwn`` only, and ``emoji`` false keeps ``:code:`` literal.
  https://docs.slack.dev/reference/block-kit/composition-objects/text-object/
- ``block_id`` is unique within a message, and a message that is updated
  should get new ones. https://docs.slack.dev/reference/block-kit/blocks/section-block/
- The top level ``text`` is parsed, and ``&``, ``<`` and ``>`` are the three
  characters to escape in it.
  https://docs.slack.dev/messaging/formatting-message-text/#escaping
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from channel_protocol import (
    TERMINAL_PROGRESS_STATES,
    MilestoneClass,
    ProgressCard,
    ProgressMilestone,
    ProgressState,
)
from channel_protocol.progress import progress_heading, progress_text
from curie_worker.blocks import (
    PROGRESS_CARD_BLOCK_ID_PREFIX,
    PROGRESS_MILESTONE_BLOCK_ID_PREFIX,
    progress_card,
    progress_milestone,
)

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "progress-blocks.json"

# Every top-level key the vector may carry, checked exactly, so a key this lane
# cannot see fails loudly instead of passing vacuously. The Rust lane rejects
# unknown fields the same way, via deny_unknown_fields on ProgressBlockVector.
_EXPECTED_PROGRESS_VECTOR_KEYS = frozenset(
    {"comment", "card_block_id_prefix", "milestone_block_id_prefix"}
)

_PINGS = "<!channel> & <@U0EXAMPLE1> :rotating_light:"


def _card(
    state: ProgressState = ProgressState.INVESTIGATING,
    *,
    summary: str = "Reading the deploy log",
    revision: int = 2,
) -> ProgressCard:
    return ProgressCard(
        kind="card",
        state=state,
        summary=summary,
        revision=revision,
        terminal=state in TERMINAL_PROGRESS_STATES,
    )


def _milestone(
    milestone: MilestoneClass = MilestoneClass.EVIDENCE,
    *,
    summary: str = "Found the failing migration",
    ordinal: int = 1,
) -> ProgressMilestone:
    return ProgressMilestone(
        kind="milestone", milestone=milestone, summary=summary, ordinal=ordinal
    )


def _every_card() -> list[ProgressCard]:
    return [_card(state, revision=revision) for state in ProgressState for revision in (1, 4)]


def _every_milestone() -> list[ProgressMilestone]:
    return [
        _milestone(milestone, ordinal=ordinal)
        for milestone in MilestoneClass
        for ordinal in (1, 2, 3)
    ]


def _text_objects(node: Any) -> list[dict[str, Any]]:
    """Every Block Kit text object anywhere under ``node``."""

    found: list[dict[str, Any]] = []
    if isinstance(node, dict):
        if node.get("type") in {"plain_text", "mrkdwn"} and "text" in node:
            found.append(node)
        for value in node.values():
            found.extend(_text_objects(value))
    elif isinstance(node, list):
        for value in node:
            found.extend(_text_objects(value))
    return found


def _texts(blocks: list[dict[str, Any]]) -> list[str]:
    return [str(item["text"]) for item in _text_objects(blocks)]


def test_progress_block_ids_match_the_frozen_vector() -> None:
    """@spec ADR-0130 d4: the Python half of the worker vs CLI progress-block gate.

    The rule itself lives in the vector's comment and is not restated here.
    """

    vector = json.loads(_VECTOR.read_text(encoding="utf-8"))

    keys = set(vector)
    assert keys == _EXPECTED_PROGRESS_VECTOR_KEYS, (
        f"{_VECTOR} has unexpected keys {sorted(keys - _EXPECTED_PROGRESS_VECTOR_KEYS)} "
        f"and is missing {sorted(_EXPECTED_PROGRESS_VECTOR_KEYS - keys)}. Teach a new key "
        "to _EXPECTED_PROGRESS_VECTOR_KEYS here, to ProgressBlockVector in "
        "cli/src/chat.rs, and to both lanes' assertions."
    )
    card_prefix = vector["card_block_id_prefix"]
    milestone_prefix = vector["milestone_block_id_prefix"]
    assert card_prefix == PROGRESS_CARD_BLOCK_ID_PREFIX
    assert milestone_prefix == PROGRESS_MILESTONE_BLOCK_ID_PREFIX
    # Neither may be a prefix of the other, or the stub could not tell a card
    # from a milestone.
    assert not card_prefix.startswith(milestone_prefix)
    assert not milestone_prefix.startswith(card_prefix)

    for card in _every_card():
        _, blocks = progress_card(card)
        for block in blocks:
            assert block["block_id"].startswith(card_prefix), block
            assert not block["block_id"].startswith(milestone_prefix), block
    for milestone in _every_milestone():
        _, blocks = progress_milestone(milestone)
        for block in blocks:
            assert block["block_id"].startswith(milestone_prefix), block
            assert not block["block_id"].startswith(card_prefix), block


def test_every_progress_text_object_is_plain_text_without_emoji() -> None:
    """@spec ADR-0130 d5: a model-authored summary can never ping anyone."""

    rendered = [progress_card(card)[1] for card in _every_card()] + [
        progress_milestone(milestone)[1] for milestone in _every_milestone()
    ]

    for blocks in rendered:
        objects = _text_objects(blocks)
        assert objects, blocks
        for item in objects:
            assert item["type"] == "plain_text", item
            assert item.get("emoji") is False, item
        assert "mrkdwn" not in json.dumps(blocks)


def test_block_ids_are_unique_within_a_message_and_new_per_card_revision() -> None:
    for card in _every_card():
        _, blocks = progress_card(card)
        ids = [block["block_id"] for block in blocks]
        assert len(ids) == len(set(ids)), ids
    for milestone in _every_milestone():
        _, blocks = progress_milestone(milestone)
        ids = [block["block_id"] for block in blocks]
        assert len(ids) == len(set(ids)), ids

    _, second = progress_card(_card(revision=2))
    _, third = progress_card(_card(revision=3))
    assert not {b["block_id"] for b in second} & {b["block_id"] for b in third}


def test_an_open_card_shows_its_state_in_words_and_its_summary() -> None:
    card = _card(ProgressState.AWAITING_APPROVAL, summary="Deploy needs a sign-off")

    text, blocks = progress_card(card)

    assert _texts(blocks) == ["Task status: Waiting for approval", "Deploy needs a sign-off"]
    assert text == progress_text(card)
    assert "closed" not in " ".join(_texts(blocks)).lower()


@pytest.mark.parametrize(
    "state", [ProgressState.COMPLETE, ProgressState.FAILED, ProgressState.CANCELLED]
)
def test_a_terminal_card_is_visibly_closed(state: ProgressState) -> None:
    """@spec ADR-0130 d2: the card stays in the thread, compactly marked terminal."""

    card = _card(state, summary="Fix verified and published", revision=5)

    _, blocks = progress_card(card)
    texts = _texts(blocks)

    assert texts[0] == progress_heading(card)
    assert texts[0] == f"Task {state.value}"
    assert texts[1] == "Fix verified and published"
    assert texts[-1] == "This task is closed. The card will not change again."


def test_an_open_state_is_never_rendered_closed() -> None:
    for state in ProgressState:
        if state in TERMINAL_PROGRESS_STATES:
            continue
        texts = _texts(progress_card(_card(state))[1])
        assert not any("closed" in text.lower() for text in texts), (state, texts)


@pytest.mark.parametrize("milestone", list(MilestoneClass))
def test_a_milestone_names_its_class_and_its_summary(milestone: MilestoneClass) -> None:
    item = _milestone(milestone, summary="Scope narrowed to the ledger importer", ordinal=2)

    text, blocks = progress_milestone(item)

    assert _texts(blocks) == [progress_heading(item), "Scope narrowed to the ledger importer"]
    assert text == progress_text(item)


def test_the_summary_is_shown_verbatim_and_the_parsed_fallback_is_escaped() -> None:
    """The one place a summary reaches a field Slack parses is the text fallback.

    The blocks show the characters as written. The fallback escapes the three
    control characters, so neither a broadcast nor a user mention forms there.
    """

    for text, blocks in (
        progress_card(_card(summary=_PINGS)),
        progress_milestone(_milestone(summary=_PINGS)),
    ):
        assert _PINGS in _texts(blocks)
        assert "<" not in text and ">" not in text
        assert "&lt;!channel&gt; &amp; &lt;@U0EXAMPLE1&gt;" in text


def test_the_fallback_is_escaped_exactly_once() -> None:
    text, _ = progress_card(_card(summary="R&D said a < b"))

    assert text.endswith("R&amp;D said a &lt; b")
    assert "&amp;amp;" not in text

"""The SRE bot's first reply to an alert or a status question reads at a glance.

The failure this exists to stop, as an operator saw it on a production install:
alert replies ran 20 to 40 lines, opened with tool names (``resources_get``,
``alerting_manage_rules``), carried Alertmanager fingerprints and trace ids, and
left the verdict somewhere in the middle. The people reading them could not tell
whether anything was wrong without reading all of it.

So the skill's reply guidance fixes a shape for the first reply to an alert
notification or a health or status question: a verdict line that starts with one
of three markers, then at most three short labelled lines. A key number said in
plain words ("about 1 in 20 requests is failing (4.8%)") belongs in it. Raw query
output, tool names, fingerprints and trace ids go in a later reply, on request.
A catalog or listing question still gets the complete list.

These read ``SKILL.md``, because that prose is what the model follows. HTML
comments and fenced code blocks are removed first: a comment is operator notes,
and an example reply inside a fence shows the shape without stating the rule, so
neither may be the only place a rule lives.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SKILL = REPO / "examples" / "sre-bot" / "skills" / "sre-bot" / "SKILL.md"

GREEN = "✅"  # WHITE HEAVY CHECK MARK
# WARNING SIGN, matched without its U+FE0F emoji presentation selector so either
# spelling of the marker counts.
AMBER = "⚠"
RED = "\U0001f534"  # LARGE RED CIRCLE


def _skill_prose() -> str:
    text = SKILL.read_text(encoding="utf-8")
    text = re.sub(r"\A---\n.*?\n---\n", "", text, flags=re.DOTALL)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    return re.sub(r"^(`{3,}|~{3,})[^\n]*\n.*?^\1[^\n]*$", "", text, flags=re.DOTALL | re.MULTILINE)


def _flat(text: str) -> str:
    return " ".join(text.split())


def _reply_guidance() -> str:
    """Every level-2 section whose heading is about answering or replying."""

    sections = re.split(r"^(?=## )", _skill_prose(), flags=re.MULTILINE)
    return "\n".join(
        section
        for section in sections
        if section.startswith("## ")
        and re.search(r"\b(repl|answer)", section.splitlines()[0], re.IGNORECASE)
    )


def _items() -> list[str]:
    """Paragraphs, list items and table rows of the reply guidance, flattened.

    A rule and the thing it governs are paired by sharing one of these, which
    holds whether the author writes a sentence, a list or a table.
    """

    parts = re.split(
        r"\n[ \t]*\n|\n(?=[ \t]*(?:[-*+]|\d+[.)])[ \t]+)|\n(?=[ \t]*\|)",
        _reply_guidance(),
    )
    return [_flat(part) for part in parts if part.strip()]


def _items_with(needle: str) -> list[str]:
    return [item for item in _items() if needle.lower() in item.lower()]


def test_the_reply_guidance_is_found() -> None:
    # A reader that finds nothing makes every assertion below vacuous.
    guidance = _reply_guidance()
    assert "## How to write the reply" in guidance, (
        "no reply guidance found in SKILL.md: check the heading match"
    )
    assert len(_items()) > 5


@pytest.mark.parametrize(
    "marker,meaning",
    [
        (GREEN, r"nothing (is )?wrong"),
        (AMBER, r"degraded.*unclear|unclear.*degraded"),
        (RED, r"real problem"),
    ],
    ids=["green", "amber", "red"],
)
def test_the_verdict_line_starts_with_one_of_three_markers(marker: str, meaning: str) -> None:
    paired = [item for item in _items_with(marker) if re.search(meaning, item, re.IGNORECASE)]
    assert paired, (
        f"SKILL.md's reply guidance must name the {marker!r} verdict marker beside what "
        f"it means (/{meaning}/). The first line of an alert or status reply starts "
        "with one of three markers so a reader knows at a glance whether anything "
        "is wrong."
    )


@pytest.mark.parametrize(
    "label,required",
    [
        ("What I checked:", [r"\bplain\b"]),
        ("What to do:", [r"\bwho\b", r"\bnothing\b"]),
        ("What I changed:", [r"\bnothing\b", r"\bapprov"]),
    ],
    ids=["checked", "to-do", "changed"],
)
def test_the_reply_guidance_states_the_three_line_labels(label: str, required: list[str]) -> None:
    items = _items_with(label)
    assert items, (
        f"SKILL.md's reply guidance never states the {label!r} line. State it in "
        "prose; an example reply inside a code fence does not count."
    )
    missing = [
        pattern
        for pattern in required
        if not any(re.search(pattern, item, re.IGNORECASE) for item in items)
    ]
    assert not missing, (
        f"the {label!r} line is stated without saying what goes in it "
        f"(missing /{'/, /'.join(missing)}/). 'What I checked:' is plain words; "
        "'What to do:' names who does what, or nothing; 'What I changed:' is "
        "nothing unless an approved call ran."
    )


ON_REQUEST = re.compile(
    r"\bask(s|ed|ing)?\b|first reply|follow-?up|only when|only if", re.IGNORECASE
)


@pytest.mark.parametrize(
    "detail",
    [r"raw (query |tool )?output", r"tool names?", r"fingerprints?", r"trace[ -]?ids?"],
    ids=["raw-query-output", "tool-names", "fingerprints", "trace-ids"],
)
def test_detail_stays_out_of_the_first_reply(detail: str) -> None:
    paired = [
        item
        for item in _items()
        if re.search(rf"\b{detail}\b", item, re.IGNORECASE) and ON_REQUEST.search(item)
    ]
    assert paired, (
        f"SKILL.md's reply guidance must say /{detail}/ stay out of the first reply "
        "and go in a later one only when someone asks. Replies that led with tool "
        "names and carried fingerprints and trace ids are the failure this pins."
    )


def test_a_key_number_in_plain_words_stays_in_the_first_reply() -> None:
    # "about 1 in 20 requests is failing (4.8%)" is what a reader acts on. Holding
    # back raw query output must not hold back the number that carries the verdict.
    assert "Include the raw number after the plain reading" in _flat(_reply_guidance()), (
        "SKILL.md's reply guidance no longer puts the key number after the plain "
        "reading; only raw query output, tool names, fingerprints and trace ids wait "
        "until someone asks"
    )


def test_the_short_format_is_scoped_to_alerts_and_status_questions() -> None:
    """The limit is for alerts and health checks, never for a list someone asked for.

    A three-line cap read as universal cuts "list the alert rules" to three rules,
    which is the summarise-into-a-pattern failure the hard rules already forbid.
    """

    scope = [
        item
        for item in _items()
        if re.search(r"\balerts?\b", item, re.IGNORECASE)
        and re.search(r"\b(health|status)\b", item, re.IGNORECASE)
        and re.search(r"verdict|first reply|three|marker|shape|format", item, re.IGNORECASE)
    ]
    assert scope, (
        "SKILL.md's reply guidance must say the verdict-plus-three-lines shape is for "
        "alert notifications and health or status questions"
    )
    listing = [
        item
        for item in _items()
        if re.search(
            r"\bcatalog(ue)?\b|\blisting\b|\blist the\b|\benumerat|which \w+ exists?",
            item,
            re.IGNORECASE,
        )
        and re.search(r"\b(complete|every|in full)\b", item, re.IGNORECASE)
    ]
    assert listing, (
        "SKILL.md's reply guidance must say catalog and listing questions ('which "
        "metrics exist', 'list the alert rules') still get the complete answer, not "
        "the three-line shape"
    )


def test_the_first_reply_is_bounded_in_lines() -> None:
    bound = re.compile(
        r"\b(at most|no more than|never more than|up to|a maximum of)\s+"
        r"(three|3|four|4)\b(\s+\w+){0,3}?\s+lines?\b",
        re.IGNORECASE,
    )
    assert bound.search(_flat(_reply_guidance())), (
        "SKILL.md's reply guidance must bound the first reply in lines (the verdict, "
        "then at most three short lines). 'Short enough to read in Slack' alone let "
        "replies run to 40 lines."
    )


def test_a_green_verdict_is_never_given_on_missing_data() -> None:
    """The markers add a new way to claim calm, so they carry the old rule with them.

    An empty result, a failed read or a 403 is not evidence that nothing is wrong.
    The reply guidance must say so where it defines the green marker, or the
    shortest reply the bot can give is also the one that hides a blind spot.
    """

    no_data = re.compile(
        r"\b(empty|no data|missing|fail(s|ed)?|refused|403|could ?n[o']?t (read|check|reach))\b",
        re.IGNORECASE,
    )
    assert [item for item in _items_with(GREEN) if no_data.search(item)], (
        f"SKILL.md's reply guidance must say an empty, failed or refused read never "
        f"earns {GREEN!r}; missing data is {AMBER!r}, not calm."
    )

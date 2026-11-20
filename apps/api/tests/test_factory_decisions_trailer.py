"""Platform text after the agent's text must not break a trailing Decisions section.

The producer is the platform's own text helpers (`end_trailing_decisions`,
`append_github_ask_line`). The consumers are the publish hook's grammar and, through
the shared fixture, the Rust parser `curie factory explain` reads a published body
with (`cli/src/commands/factory_explain.rs`, test `factory_published_decisions_vector`).
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_api.factory_comment_text import (
    DECISIONS_SECTION_END,
    append_github_ask_line,
    end_trailing_decisions,
    github_ask_line,
)

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "examples" / "dark-factory" / "hooks"))

import decisions_section  # noqa: E402
from decisions_section import parse_decisions_section  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: E402, F401
from test_publications import (  # noqa: E402, F401
    _factory_publication_case,
    _post_factory_publication,
    _record_factory_verification,
)

FIXTURE = ROOT / "tests" / "vectors" / "factory-decisions-published.json"
MENTION = "curie-factory"
TWO = (
    "1. Keep the parser pure. Rejected: a cached parser, because the input is small. "
    "(plan) Evidence: the plan review approved it.\n"
    "2. Reuse the shared grammar. Rejected: a second grammar, because two would drift. "
    "(diff review) Evidence: the vector test covers both."
)
NONE = "None: the change follows directly from the acceptance criteria."
# What the publications route adds after the agent's text on a GitHub factory
# publication: the closing reference and the issue marker, then the ask line.
TRAILERS = "Closes #12\n\n<!-- curie-issue kind=github host=github.com scope=1 issue=12 -->"


def _route(agent_body: str) -> str:
    """The route's order: close the section, add the trailers, then the ask line."""
    closed = end_trailing_decisions(agent_body)
    return append_github_ask_line(f"{closed.rstrip()}\n\n{TRAILERS}", MENTION)


def _cases() -> list[dict[str, Any]]:
    summary = "The change updates the factory route.\n\n"
    last = f"{summary}## Decisions\n\n{TWO}\n"
    legacy = f"{last}\n{github_ask_line('old-mention')}\n"
    none_last = f"{summary}## Decisions\n\n{NONE}"
    before_heading = f"{summary}## Decisions\n\n{TWO}\n\n## Reviews\n\nPlan review: APPROVE.\n"
    malformed = f"{summary}## Decisions\n\n1. Keep the parser pure.\n"
    return [
        {
            "id": "ask-line-after-decisions-last",
            "agent_body": last,
            "published_body": append_github_ask_line(last, MENTION),
        },
        {
            "id": "route-trailers-after-decisions-last",
            "agent_body": last,
            "published_body": _route(last),
        },
        {
            "id": "none-declaration-last",
            "agent_body": none_last,
            "published_body": append_github_ask_line(none_last, MENTION),
        },
        {
            "id": "reappended-published-body",
            "agent_body": last,
            "published_body": append_github_ask_line(_route(last), MENTION),
        },
        {
            "id": "legacy-published-body-without-a-break",
            "agent_body": last,
            "published_body": append_github_ask_line(legacy, MENTION),
        },
        {
            "id": "decisions-before-a-heading-is-untouched",
            "agent_body": before_heading,
            "published_body": _route(before_heading),
        },
        {
            # Named counterexample: a Decisions section that is really malformed (the
            # second entry has no rejected alternative) must stay unparsed once the
            # platform text follows it. The break line must not launder it.
            "id": "malformed-decisions-last",
            "agent_body": malformed,
            "published_body": _route(malformed),
        },
    ]


def _document() -> str:
    cases = []
    for case in _cases():
        try:
            entries: list[dict[str, Any]] | None = parse_decisions_section(case["agent_body"])
        except ValueError:
            entries = None
        cases.append(
            {
                "id": case["id"],
                "agent_body": case["agent_body"],
                "published_body": case["published_body"],
                "valid": entries is not None,
                "entries": entries or [],
            }
        )
    return json.dumps({"cases": cases}, indent=2, ensure_ascii=False) + "\n"


def test_fixture_is_the_producer_output_byte_for_byte() -> None:
    """The Rust test reads this file, so it must be what the producer writes now."""
    text = _document()
    if os.environ.get("CURIE_WRITE_VECTORS") == "1":
        FIXTURE.write_text(text)
    assert FIXTURE.read_text() == text, (
        "tests/vectors/factory-decisions-published.json is stale; "
        "run this test with CURIE_WRITE_VECTORS=1 to rewrite it"
    )


@pytest.mark.parametrize("case", _cases(), ids=lambda case: case["id"])
def test_published_body_keeps_the_entries_the_agent_wrote(case: dict[str, Any]) -> None:
    published = case["published_body"]
    try:
        before = parse_decisions_section(case["agent_body"])
    except ValueError:
        # A malformed section is still rejected after the append.
        with pytest.raises(ValueError):
            parse_decisions_section(published)
        return
    assert parse_decisions_section(published) == before
    assert sum(line.startswith("Ask the factory:") for line in published.splitlines()) == 1


def test_a_trailing_section_is_closed_by_exactly_the_text_all_three_readers_use() -> None:
    assert DECISIONS_SECTION_END == decisions_section.SECTION_END == "---"
    assert append_github_ask_line("## Decisions\n\n" + TWO, MENTION).splitlines()[-3:] == [
        "---",
        "",
        github_ask_line(MENTION),
    ]


def test_the_ask_line_alone_never_extends_a_section_that_is_not_last() -> None:
    body = f"## Decisions\n\n{TWO}\n\n## Reviews\n\nPlan review: APPROVE.\n"
    assert DECISIONS_SECTION_END not in append_github_ask_line(body, MENTION).splitlines()


@pytest.mark.parametrize("case", _cases(), ids=lambda case: case["id"])
def test_reappending_is_idempotent(case: dict[str, Any]) -> None:
    once = append_github_ask_line(case["published_body"], MENTION)
    twice = append_github_ask_line(once, MENTION)
    assert twice == once
    assert once.count("Ask the factory:") == 1
    assert once.splitlines().count(DECISIONS_SECTION_END) <= 1


def _publish_through_the_route(
    case: tuple[TestClient, uuid.UUID, dict[str, Any]], agent_body: str
) -> str:
    """The body the publications route stores for a factory publication."""
    client, request_id, payload = case
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text
    created = _post_factory_publication(client, {**payload, "body": agent_body})
    assert created.status_code == 201, created.text
    return str(created.json()["body"])


def test_the_route_keeps_a_trailing_decisions_section_parseable(
    _factory_publication_case: tuple[TestClient, uuid.UUID, dict[str, Any]],  # noqa: F811
) -> None:
    agent_body = f"The change updates the factory route.\n\n## Decisions\n\n{TWO}\n"
    published = _publish_through_the_route(_factory_publication_case, agent_body)

    lines = published.splitlines()
    # The route added its closing reference, the issue marker and the ask line,
    # every one of them after the section's end.
    assert any(line.startswith("Closes #") for line in lines)
    assert sum(line.startswith("Ask the factory:") for line in lines) == 1
    assert lines.index(DECISIONS_SECTION_END) < lines.index(
        next(line for line in lines if line.startswith("Closes #"))
    )
    assert parse_decisions_section(published) == parse_decisions_section(agent_body)


def test_the_route_does_not_launder_a_malformed_trailing_decisions_section(
    _factory_publication_case: tuple[TestClient, uuid.UUID, dict[str, Any]],  # noqa: F811
) -> None:
    agent_body = (
        "The change updates the factory route.\n\n## Decisions\n\n1. Keep the parser pure.\n"
    )
    published = _publish_through_the_route(_factory_publication_case, agent_body)

    with pytest.raises(ValueError):
        parse_decisions_section(published)

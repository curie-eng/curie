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
    github_ask_line,
)

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "examples" / "dark-factory" / "hooks"))

import decisions_section  # noqa: E402
import reproduction_gate  # noqa: E402
from decisions_section import parse_decisions_section  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: E402, F401
from test_publications import (  # noqa: E402, F401
    _factory_issue_marker,
    _factory_publication_case,
    _post_factory_publication,
    _record_factory_verification,
)

FIXTURE = ROOT / "tests" / "vectors" / "factory-decisions-published.json"
ROUTE_FIXTURE = ROOT / "tests" / "vectors" / "factory-decisions-published-route.json"
MENTION = "curie-factory"
SUMMARY = "The change updates the factory route.\n\n"
TWO = (
    "1. Keep the parser pure. Rejected: a cached parser, because the input is small. "
    "(plan) Evidence: the plan review approved it.\n"
    "2. Reuse the shared grammar. Rejected: a second grammar, because two would drift. "
    "(diff review) Evidence: the vector test covers both."
)
NONE = "None: the change follows directly from the acceptance criteria."
LAST = f"{SUMMARY}## Decisions\n\n{TWO}\n"
# A Decisions section that is really malformed: the entry has no rejected alternative.
MALFORMED = f"{SUMMARY}## Decisions\n\n1. Keep the parser pure.\n"
# A near miss of the break line. Both grammars read it as an ordinary line, and
# the producer must not trim its trailing space into the break.
NEAR_BREAK = f"{SUMMARY}## Decisions\n\n{TWO}\n--- \n"
BEFORE_HEADING = f"{SUMMARY}## Decisions\n\n{TWO}\n\n## Reviews\n\nPlan review: APPROVE.\n"
LEGACY = f"{LAST}\n{github_ask_line('old-mention')}\n"


def _producer_cases() -> list[dict[str, Any]]:
    """Bodies the platform text helpers publish, from agent text it never saw."""
    once = append_github_ask_line(LAST, MENTION)
    return [
        {"id": "ask-line-after-decisions-last", "agent_body": LAST, "published_body": once},
        {
            "id": "none-declaration-last",
            "agent_body": f"{SUMMARY}## Decisions\n\n{NONE}",
            "published_body": append_github_ask_line(f"{SUMMARY}## Decisions\n\n{NONE}", MENTION),
        },
        {
            "id": "reappended-published-body",
            "agent_body": LAST,
            "published_body": append_github_ask_line(once, MENTION),
        },
        {
            "id": "legacy-published-body-without-a-break",
            "agent_body": LAST,
            "published_body": append_github_ask_line(LEGACY, MENTION),
        },
        {
            "id": "decisions-before-a-heading-is-untouched",
            "agent_body": BEFORE_HEADING,
            "published_body": append_github_ask_line(BEFORE_HEADING, MENTION),
        },
        {
            # Named counterexample: a really malformed Decisions section stays
            # unparsed once the platform text follows it. The break line must not
            # launder it.
            "id": "malformed-decisions-last",
            "agent_body": MALFORMED,
            "published_body": append_github_ask_line(MALFORMED, MENTION),
        },
        {
            "id": "near-break-line-last-stays-malformed",
            "agent_body": NEAR_BREAK,
            "published_body": append_github_ask_line(NEAR_BREAK, MENTION),
        },
    ]


# The bodies the publications route is asked to publish. Their published text comes
# from the route itself (the closing reference, issue marker and ask line it adds),
# with the per-run issue number and marker replaced by fixed text.
ROUTE_BODIES = {
    "route-decisions-last": LAST,
    "route-malformed-decisions-last": MALFORMED,
    "route-near-break-line-last-stays-malformed": NEAR_BREAK,
}
FIXED_MARKER = "<!-- curie-issue kind=github host=github.com scope=1 issue=1 -->"


def _entry(case_id: str, agent_body: str, published_body: str) -> dict[str, Any]:
    try:
        entries: list[dict[str, Any]] | None = parse_decisions_section(agent_body)
    except ValueError:
        entries = None
    return {
        "id": case_id,
        "agent_body": agent_body,
        "published_body": published_body,
        "valid": entries is not None,
        "entries": entries or [],
    }


def _dump(cases: list[dict[str, Any]]) -> str:
    return json.dumps({"cases": cases}, indent=2, ensure_ascii=False) + "\n"


def _read_bytes(path: Path) -> str:
    # Bytes, not text mode: newline translation must not hide a stale fixture.
    return path.read_bytes().decode("utf-8")


def _write_bytes(path: Path, text: str) -> None:
    path.write_bytes(text.encode("utf-8"))


def test_fixture_is_the_producer_output_byte_for_byte() -> None:
    """The Rust test reads this file, so it must be what the producer writes now."""
    text = _dump([_entry(c["id"], c["agent_body"], c["published_body"]) for c in _producer_cases()])
    if os.environ.get("CURIE_WRITE_VECTORS") == "1":
        _write_bytes(FIXTURE, text)
    assert _read_bytes(FIXTURE) == text, (
        "tests/vectors/factory-decisions-published.json is stale; "
        "run this test with CURIE_WRITE_VECTORS=1 to rewrite it"
    )


@pytest.mark.parametrize("case", _producer_cases(), ids=lambda case: case["id"])
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


def test_a_trailing_section_is_closed_by_exactly_the_text_all_readers_use() -> None:
    assert DECISIONS_SECTION_END == decisions_section.SECTION_END == "---"
    assert reproduction_gate.DECISIONS_END == DECISIONS_SECTION_END
    assert append_github_ask_line("## Decisions\n\n" + TWO, MENTION).splitlines()[-3:] == [
        "---",
        "",
        github_ask_line(MENTION),
    ]


def test_the_ask_line_alone_never_extends_a_section_that_is_not_last() -> None:
    published = append_github_ask_line(BEFORE_HEADING, MENTION)
    assert DECISIONS_SECTION_END not in published.splitlines()


def test_a_citation_below_the_break_line_is_not_in_the_section() -> None:
    cited = "Answered on the ticket by maintainer: use the shared grammar."
    inside = f"## Decisions\n\n{TWO}\n{cited}\n"
    below = f"## Decisions\n\n{TWO}\n\n---\n\n{cited}\n"
    assert reproduction_gate.cited(inside)
    assert not reproduction_gate.cited(below)


@pytest.mark.parametrize("case", _producer_cases(), ids=lambda case: case["id"])
def test_reappending_is_idempotent(case: dict[str, Any]) -> None:
    once = append_github_ask_line(case["published_body"], MENTION)
    twice = append_github_ask_line(once, MENTION)
    assert twice == once
    assert once.count("Ask the factory:") == 1
    assert once.splitlines().count(DECISIONS_SECTION_END) <= 1


@pytest.mark.parametrize("case_id", list(ROUTE_BODIES))
def test_the_route_publishes_the_fixture_body_byte_for_byte(
    _factory_publication_case: tuple[TestClient, uuid.UUID, dict[str, Any]],  # noqa: F811
    case_id: str,
) -> None:
    """The body the production route stores is what the Rust parser reads."""
    client, request_id, payload = _factory_publication_case
    agent_body = ROUTE_BODIES[case_id]
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text
    created = _post_factory_publication(client, {**payload, "body": agent_body})
    assert created.status_code == 201, created.text
    published = str(created.json()["body"])

    lines = published.splitlines()
    # The route added its closing reference, issue marker and ask line.
    issue = int(payload["reply_conversation_id"].removeprefix("issue-"))
    assert f"Closes #{issue}" in lines
    marker = _factory_issue_marker(request_id)
    assert marker in lines
    assert sum(line.startswith("Ask the factory:") for line in lines) == 1
    normalized = published.replace(f"Closes #{issue}", "Closes #1").replace(marker, FIXED_MARKER)

    entry = _entry(case_id, agent_body, normalized)
    if entry["valid"]:
        assert parse_decisions_section(published) == entry["entries"]
    else:
        with pytest.raises(ValueError):
            parse_decisions_section(published)

    stored = json.loads(_read_bytes(ROUTE_FIXTURE)) if ROUTE_FIXTURE.exists() else {"cases": []}
    by_id = {case["id"]: case for case in stored["cases"]}
    if os.environ.get("CURIE_WRITE_VECTORS") == "1":
        by_id[case_id] = entry
        ordered = [by_id[name] for name in ROUTE_BODIES if name in by_id]
        _write_bytes(ROUTE_FIXTURE, _dump(ordered))
        by_id = {case["id"]: case for case in ordered}
    assert by_id.get(case_id) == entry, (
        "tests/vectors/factory-decisions-published-route.json is stale; "
        "run this test with CURIE_WRITE_VECTORS=1 to rewrite it"
    )

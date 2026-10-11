"""The status comment states the recorded base and where it came from (#3095, ADR 0186)."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import FINAL_MARKER, status_body
from forge_fakes.github_comments import admitted, comments  # noqa: F401  (fixtures)
from test_factory_status_comment import _admit, _execute, _marked
from test_factory_terminus import _reconcile  # noqa: F401  (fixtures)

pytestmark = pytest.mark.usefixtures("clean_db")

REQUEST = uuid.UUID("00000000-0000-0000-0000-000000003095")
CARD = "https://curie.example.com/v1/factory/cards/abc.svg"
WAITING = "_Waiting for the agent to report progress._"
LABEL_LINE = "Base: `next` (from label `base:next`)"
DEFAULT_LINE = "Base: `main` (deployment default)"
IGNORED_LINE = (
    "Base: `next` (from label `base:next`) "
    "Label now says `base:main`; the recorded base is kept."
)


# --- status_body places the line ---------------------------------------------------


def test_the_base_line_follows_status_and_precedes_the_marker() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=CARD,
        pill_label="QUEUED",
        phase_view=None,
        result=None,
        base_line=LABEL_LINE,
    )
    assert body == (
        f"![Curie status]({CARD})\n\nStatus: QUEUED\n\n{LABEL_LINE}\n\n{marker_for(REQUEST)}\n"
    )


def test_the_base_line_follows_the_waiting_line() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=None,
        pill_label="QUEUED",
        phase_view=None,
        result=None,
        waiting_line="Waiting for CI.",
        base_line=DEFAULT_LINE,
    )
    assert body == (
        f"{WAITING}\n\nStatus: QUEUED\n\nWaiting for CI.\n\n{DEFAULT_LINE}\n\n"
        f"{marker_for(REQUEST)}\n"
    )


def test_the_final_body_keeps_the_base_line_before_the_final_marker() -> None:
    body = status_body(
        request_id=REQUEST,
        card_url=CARD,
        pill_label="PR OPEN",
        phase_view=None,
        result="Opened https://github.com/acme/fixture/pull/7\n",
        base_line=LABEL_LINE,
    )
    assert body == (
        "Opened https://github.com/acme/fixture/pull/7\n\n"
        f"![Curie status]({CARD})\n\nStatus: PR OPEN\n\n{LABEL_LINE}\n\n"
        f"{FINAL_MARKER}\n\n{marker_for(REQUEST)}\n"
    )


def test_no_base_line_leaves_the_body_unchanged() -> None:
    assert status_body(
        request_id=REQUEST,
        card_url=CARD,
        pill_label="QUEUED",
        phase_view=None,
        result=None,
        base_line=None,
    ) == status_body(
        request_id=REQUEST, card_url=CARD, pill_label="QUEUED", phase_view=None, result=None
    )


# --- the reconciler renders the recorded base ----------------------------------------


def _record_base(
    number: int,
    *,
    branch: str | None,
    source: str | None,
    commit: str | None,
    ignored: str | None = None,
) -> None:
    _execute(
        "UPDATE curie.work_items SET base_branch = :branch, base_source = :source, "
        "base_commit = :commit, base_label_ignored = :ignored "
        "WHERE tracker_issue_id = :number",
        {
            "branch": branch,
            "source": source,
            "commit": commit,
            "ignored": ignored,
            "number": str(number),
        },
    )


@pytest.mark.parametrize(
    ("number", "branch", "source", "ignored", "expected"),
    [
        (30951, "next", "label", None, LABEL_LINE),
        (30952, "main", "default", None, DEFAULT_LINE),
        (30953, "next", "label", "main", IGNORED_LINE),
    ],
)
def test_the_status_comment_renders_the_recorded_base(
    admitted: Any,  # noqa: F811
    number: int,
    branch: str,
    source: str,
    ignored: str | None,
    expected: str,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, number)
    _record_base(number, branch=branch, source=source, commit="5" * 40, ignored=ignored)

    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    lines = [line.strip() for line in body.split("\n\n")]
    assert expected in lines
    assert lines.index("Status: QUEUED") < lines.index(expected)
    assert lines.index(expected) < lines.index(marker_for(request_id).strip())


def test_a_legacy_work_item_renders_no_base_line(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 30954
    request_id = _admit(client, github, sink, number)
    _record_base(number, branch=None, source=None, commit=None)

    _reconcile()

    (comment,) = _marked(sink, request_id)
    assert "Base:" not in comment["body"]

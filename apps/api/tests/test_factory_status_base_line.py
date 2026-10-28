"""The status comment states the selected base, source and pending move (ADR 0253)."""

# ruff: noqa: F811

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
from test_factory_base_ci_subject import moved  # noqa: F401
from test_factory_status_comment import _admit, _execute, _marked
from test_factory_terminus import _reconcile  # noqa: F401  (fixtures)

pytestmark = pytest.mark.usefixtures("clean_db")

REQUEST = uuid.UUID("00000000-0000-0000-0000-000000003095")
CARD = "https://curie.example.com/v1/factory/cards/abc.svg"
WAITING = "_Waiting for the agent to report progress._"
LABEL_LINE = "Base: `next` (from label `base:next`)"
DEFAULT_LINE = "Base: `main` (deployment default)"


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
) -> None:
    _execute(
        "UPDATE curie.work_items SET base_branch = :branch, base_source = :source, "
        "base_commit = :commit "
        "WHERE tracker_issue_id = :number",
        {
            "branch": branch,
            "source": source,
            "commit": commit,
            "number": str(number),
        },
    )


@pytest.mark.parametrize(
    ("number", "branch", "source", "expected"),
    [
        (30951, "next", "label", LABEL_LINE),
        (30952, "main", "default", DEFAULT_LINE),
    ],
)
def test_the_status_comment_renders_the_recorded_base(
    admitted: Any,  # noqa: F811
    number: int,
    branch: str,
    source: str,
    expected: str,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, number)
    _record_base(number, branch=branch, source=source, commit="5" * 40)

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


@pytest.mark.parametrize(
    ("phase", "error", "words"),
    [("pending", None, ("pending", "waiting", "requested")),
     ("pushed", None, ("pushed", "retarget", "moving")),
     ("conflict", "rebase_conflict", ("conflict", "refused"))],
)
def test_status_renders_the_real_transition_target_and_progress(
    moved: Any, phase: str, error: str | None, words: tuple[str, ...]
) -> None:
    client, _github, sink, row = moved
    _execute(
        "UPDATE curie.work_items SET base_branch = 'main', base_source = 'default', "
        "base_commit = :commit WHERE id = :id",
        {"id": row["work_item_id"], "commit": "1" * 40},
    )
    _execute(
        "UPDATE curie.factory_base_transitions SET phase = :phase, error_code = :error "
        "WHERE id = :id",
        {"id": row["transition_id"], "phase": phase, "error": error},
    )

    client.portal.call(client.app.state.work_item_reconciler._sync_status_comments)

    (comment,) = _marked(sink, row["id"])
    body = comment["body"]
    assert DEFAULT_LINE in body
    assert "`next`" in body or "`base:next`" in body
    assert any(word in body.lower() for word in words)
    assert "the recorded base is kept" not in body
    assert FINAL_MARKER not in body

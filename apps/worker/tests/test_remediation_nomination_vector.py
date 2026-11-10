"""Shared nomination grammar at the worker output boundary.

@spec AUTOMATED-REMEDIATION-5 AUTOMATED-REMEDIATION-6. No model or API
submission is performed here; these tests prove only private output filtering.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

_VECTOR = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/remediation-nomination.json").read_text()
)
_CASES = _VECTOR["cases"]


def product():
    return importlib.import_module("curie_worker.protected_nomination_filter")


@pytest.mark.parametrize("case", _CASES, ids=lambda c: c["name"])
def test_shared_nomination_vector_final_and_streaming_agree(case):
    """Every shared output/status is stripped identically, including malformed blocks."""
    module = product()
    final = module.filter_final(case["output"], case["status"])
    assert final.reply == case["reply"]
    assert final.submission == case["submitted"]
    chunks = case.get("deltas", [case["output"]])
    assert "".join(chunks) == case["output"]
    filter_ = module.NominationLineFilter()
    emitted = [filter_.feed(chunk) for chunk in chunks]
    emitted.append(filter_.finish())
    assert "".join(emitted) == case["reply"]
    assert filter_.submission(case["status"]) == case["submitted"]
    assert "```curie-remediation" not in "".join(emitted) or case["submitted"] is None


@pytest.mark.parametrize("split", range(1, len("```curie-remediation") + 1))
def test_opening_line_never_leaks_before_its_newline(split):
    """No early UI edit exposes a split nomination opener."""
    filter_ = product().NominationLineFilter()
    assert filter_.feed("```curie-remediation"[:split]) == ""
    assert filter_.feed("```curie-remediation"[split:]) == ""
    assert filter_.feed("\nprivate candidate\n```\nvisible\n") == "visible\n"
    assert filter_.finish() == ""


def test_partial_ordinary_lines_are_held_until_complete():
    """Line boundaries determine exact fences; a partial opener is not prose yet."""
    filter_ = product().NominationLineFilter()
    assert filter_.feed("visible") == ""
    assert filter_.feed(" tail\nnext") == "visible tail\n"
    assert filter_.finish() == "next"
    assert filter_.submission("done") is None


def test_unclosed_private_block_is_withheld_even_when_turn_fails():
    filter_ = product().NominationLineFilter()
    assert filter_.feed("public\n```curie-remediation\nprivate") == "public\n"
    assert filter_.finish() == ""
    assert filter_.submission("classified-failure") is None


def test_final_capture_repr_omits_private_candidate_text():
    result = product().filter_final("```curie-remediation\nprivate candidate\n```", "done")
    assert "private candidate" not in repr(result)


def test_shared_vector_shape_is_closed():
    """A new wire member requires an explicit worker compatibility decision."""
    assert set(_VECTOR) == {
        "comment",
        "opening_fence",
        "closing_fence",
        "line_terminator",
        "max_block_bytes",
        "max_entries",
        "max_reason_characters",
        "version",
        "entry_shapes",
        "malformed_code",
        "duplicate_code",
        "submit_statuses",
        "cases",
    }
    assert _VECTOR["opening_fence"] == "```curie-remediation"
    assert _VECTOR["closing_fence"] == "```"
    assert _VECTOR["line_terminator"] == "\n"
    assert _VECTOR["submit_statuses"] == ["done"]
    required = {"name", "status", "output", "reply", "submitted", "parse"}
    for case in _CASES:
        assert required <= set(case) <= required | {"deltas", "note"}


def test_a_partial_turn_cannot_produce_a_done_submission():
    """Candidates remain private until the output stream is finalized."""
    filter_ = product().NominationLineFilter()
    filter_.feed("```curie-remediation\nprivate candidate\n")
    with pytest.raises(ValueError):
        filter_.submission("done")


def test_finished_filter_cannot_accept_a_second_turn():
    """Reuse must not contaminate the next turn's output or candidate."""
    filter_ = product().NominationLineFilter()
    assert filter_.feed("ordinary tail") == ""
    assert filter_.finish() == "ordinary tail"
    assert filter_.finish() == ""
    with pytest.raises(ValueError):
        filter_.feed("new turn")

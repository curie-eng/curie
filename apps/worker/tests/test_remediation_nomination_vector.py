"""Worker half of the frozen remediation nomination vector.

@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-26.
The protected worker's capture wrapper and the API's nomination parser ship in
different images, so both read ``tests/vectors/remediation-nomination.json``
(``apps/api/tests/test_remediation_nomination_vector.py`` is the API half).

The reader is ``curie_worker.remediation_capture``, the module behind the
protected lane's runner client wrapper:

* ``NominationLineFilter()``: ``feed(text)`` returns the streamed text it may
  release now (a trailing partial line is held until it completes, so a fence
  split across deltas is still caught) and ``finish()`` releases the rest at the
  end of the stream; every line from an opening fence line through its closing
  fence line is withheld, and an unclosed block is withheld to the end;
* ``capture_final(status, text)`` returns ``(reply, submission)``: the ``Final``
  text with every block removed (in every status) and the exact withheld text
  to post to ``POST /v1/internal/remediation/nominations``, or None when the
  status is not ``done`` or no block is present.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-nomination.json"
    ).read_text("utf-8")
)
_KEYS = {
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
_CASES = _VECTOR["cases"]


def _capture() -> Any:  # noqa: ANN401 - the production module under test
    from curie_worker import remediation_capture

    return remediation_capture


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-nomination.json: {sorted(unknown)}. Teach them to this "
        "test and apps/api/tests/test_remediation_nomination_vector.py."
    )
    assert set(_VECTOR) == _KEYS
    for case in _CASES:
        if "deltas" in case:
            assert "".join(case["deltas"]) == case["output"], case["name"]


def test_the_extractor_uses_the_frozen_fences() -> None:
    """@spec AUTOMATED-REMEDIATION-5: the same fence lines the API parser reads."""

    module = _capture()
    assert module.OPENING_FENCE == _VECTOR["opening_fence"]
    assert module.CLOSING_FENCE == _VECTOR["closing_fence"]
    assert set(module.SUBMIT_STATUSES) == set(_VECTOR["submit_statuses"])


def _splits(case: dict[str, Any]) -> list[tuple[str, list[str]]]:
    output = case["output"]
    splits = [("whole", [output]), ("per_character", list(output))]
    if "deltas" in case:
        splits.insert(0, ("frozen", case["deltas"]))
    return splits


def _stream(deltas: list[str]) -> tuple[str, list[str]]:
    line_filter = _capture().NominationLineFilter()
    released = [line_filter.feed(delta) for delta in deltas]
    released.append(line_filter.finish())
    return "".join(released), released


@pytest.mark.parametrize("case", _CASES, ids=lambda case: case["name"])
def test_the_streamed_reply_never_carries_the_block(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-6: the line filter over every delta split.

    The frozen deltas (fence lines split across deltas), the whole output as one
    delta, and one character per delta all release exactly ``reply``; release
    is append-only, so no intermediate reply edit carries more than ``reply``.
    """

    for label, deltas in _splits(case):
        streamed, released = _stream(deltas)
        assert streamed == case["reply"], label
        seen = ""
        for chunk in released:
            seen += chunk
            assert case["reply"].startswith(seen), label


@pytest.mark.parametrize("case", _CASES, ids=lambda case: case["name"])
def test_the_final_is_stripped_and_submitted_only_when_done(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-6: the ``Final`` capture and the one submission.

    A ``done`` turn submits the exact withheld text; any other status submits
    nothing, though its yielded ``Final`` is stripped all the same.
    """

    reply, submission = _capture().capture_final(case["status"], case["output"])
    assert reply == case["reply"]
    assert submission == case["submitted"]
    if case["status"] not in _VECTOR["submit_statuses"]:
        assert submission is None

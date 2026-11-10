"""API half of the frozen remediation nomination vector.

@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-26.
The protected worker's capture wrapper and the API's nomination parser ship in
different images, so both read ``tests/vectors/remediation-nomination.json``
(``apps/worker/tests/test_remediation_nomination_vector.py`` is the worker
half).

The reader is ``curie_api.remediation_nominations``: ``parse_nomination_block(text)``
takes the exact text the worker submitted and returns one parsed entry per
nomination in order, each with ``action``, ``arguments`` (the canonical text of
``action-canonical-arguments.json``), ``arguments_sha256``, ``reason`` (None when
absent) and ``refusal`` (``"nomination_duplicate"`` or None); a block outside the
grammar raises ``NominationMalformed`` whose ``code`` is ``nomination_malformed``.
The module also exports the grammar constants the vector freezes.
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
_CASE_KEYS = {"name", "note", "status", "output", "deltas", "reply", "submitted", "parse"}
_ENTRY_KEYS = ["action", "arguments", "arguments_sha256", "reason", "refusal"]
_SUBMITTED = [case for case in _VECTOR["cases"] if case["submitted"] is not None]


def _parser() -> Any:  # noqa: ANN401 - the production module under test
    from curie_api import remediation_nominations

    return remediation_nominations


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-nomination.json: {sorted(unknown)}. Teach them to this "
        "test and apps/worker/tests/test_remediation_nomination_vector.py."
    )
    assert set(_VECTOR) == _KEYS
    for case in _VECTOR["cases"]:
        assert set(case) <= _CASE_KEYS, case["name"]
        assert (case["parse"] is None) == (case["submitted"] is None), case["name"]


def test_the_parser_exports_the_frozen_grammar() -> None:
    """@spec AUTOMATED-REMEDIATION-5: fences, bounds, version and codes."""

    module = _parser()
    assert module.OPENING_FENCE == _VECTOR["opening_fence"]
    assert module.CLOSING_FENCE == _VECTOR["closing_fence"]
    assert module.MAX_BLOCK_BYTES == _VECTOR["max_block_bytes"]
    assert module.MAX_ENTRIES == _VECTOR["max_entries"]
    assert module.MAX_REASON_CHARACTERS == _VECTOR["max_reason_characters"]
    assert module.VERSION == _VECTOR["version"]
    assert module.MALFORMED_CODE == _VECTOR["malformed_code"]
    assert module.DUPLICATE_CODE == _VECTOR["duplicate_code"]


def _entry(parsed: Any) -> dict[str, Any]:  # noqa: ANN401 - a parsed entry
    return {key: getattr(parsed, key) for key in _ENTRY_KEYS}


@pytest.mark.parametrize("case", _SUBMITTED, ids=lambda case: case["name"])
def test_each_submitted_block_parses_to_the_frozen_result(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7.

    Valid blocks give the frozen entries in order, with a later identical call
    refused alone; every invalid block is refused as a whole.
    """

    module = _parser()
    expected = case["parse"]
    if expected["result"] == "malformed":
        with pytest.raises(module.NominationMalformed) as refused:
            module.parse_nomination_block(case["submitted"])
        assert refused.value.code == expected["code"] == _VECTOR["malformed_code"]
        return
    assert expected["result"] == "valid"
    parsed = module.parse_nomination_block(case["submitted"])
    assert [_entry(entry) for entry in parsed] == expected["entries"]

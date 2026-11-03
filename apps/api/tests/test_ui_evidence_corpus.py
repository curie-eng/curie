"""The shared UI evidence corpus is whole and every case is driven (plan 6.1).

``tests/vectors/ui-evidence-cases.json`` is the one list of evidence cases the
runner, worker and API boundary tests iterate. This module proves the list is
usable: every case carries an expectation some boundary consumes, every
normalized worker-to-API index is accepted by the API schema and survives a
round trip, and every refusal code a case expects has its one fixed sentence.

The helpers here are shared with ``test_ui_evidence_reconciler.py``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from curie_api.schemas.ui_evidence import UiEvidenceIndexIn

ROOT = Path(__file__).resolve().parents[3]
CORPUS_PATH = ROOT / "tests/vectors/ui-evidence-cases.json"

# The boundary keys a case may carry; each is consumed by one boundary test:
# ``runner`` by runner/tests/test_ui_evidence.py, ``index`` by
# apps/worker/tests/test_ui_evidence.py (null means "no upload"), and ``api``
# by apps/api/tests/test_ui_evidence_reconciler.py.
BOUNDARY_KEYS = ("runner", "index", "api")

# The AC2 cases the ticket names. The corpus may hold more, never fewer.
REQUIRED_CASES = {
    "missing-manifest",
    "malformed-manifest",
    "traversal",
    "symlink-file",
    "symlink-dir-component",
    "non-png",
    "corrupt-png-crc",
    "corrupt-png-ihdr-not-first",
    "corrupt-png-iend-not-last",
    "oversized-image",
    "one-side-missing",
    "unbound-before",
    "unbound-after-dirty-head-only",
    "bound-after-clean-head",
    "transfer-failure",
    "storage-failure",
    "upload-rejection",
    "unsupported-host",
    "comment-failure",
    "timeout-capture",
    "timeout-publish",
    "complete",
}

# The refusal codes of plan 5.1: runner, then worker, then API additions.
REFUSAL_CODES = {
    "traversal",
    "invalid_entry",
    "symlink",
    "missing",
    "not_regular",
    "not_png",
    "corrupt_png",
    "too_large",
    "total_limit",
    "unbound",
    "transfer_failed",
    "timeout",
    "not_stored",
    "head_changed",
    "not_shown_size",
}

# Stand-ins for the fixture selectors when a case's index names one instead of
# an object id. The API boundary replaces "P" with the publication base.
SELECTOR_OIDS = {
    "P": "0123456789abcdef0123456789abcdef01234567",
    "C": "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00",
    "other": "0be70be70be70be70be70be70be70be70be70be7",
    "HEAD": "4ead4ead4ead4ead4ead4ead4ead4ead4ead4ead",
}
_OID_FIELDS = (
    "base_sha",
    "manifest_base_sha",
    "manifest_head_sha",
    "manifest_candidate_tree",
    "candidate_tree",
    "head_tree",
)


def load_corpus() -> dict[str, Any]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def corpus_cases() -> list[dict[str, Any]]:
    return list(load_corpus()["cases"])


def resolve_index(index: Mapping[str, Any], oids: Mapping[str, str]) -> dict[str, Any]:
    """``index`` with each fixture selector in an object id field replaced by
    the id ``oids`` gives it; a field that already holds an id is kept."""

    resolved = json.loads(json.dumps(index))
    for field in _OID_FIELDS:
        value = resolved.get(field)
        if isinstance(value, str) and value in oids:
            resolved[field] = oids[value]
    return resolved


def expected_codes(case: Mapping[str, Any]) -> set[str]:
    """Every refusal code the case's boundary expectations name."""

    codes: set[str] = set()
    for key in ("runner", "index"):
        expectation = case.get(key)
        if isinstance(expectation, Mapping):
            for image in expectation.get("images", []) or []:
                if isinstance(image, Mapping) and image.get("refusal"):
                    codes.add(str(image["refusal"]))
    api = case.get("api")
    if isinstance(api, Mapping):
        codes.update(str(code) for code in (api.get("not_shown") or {}).values())
    return codes


def test_every_case_is_driven_at_a_boundary() -> None:
    corpus = load_corpus()
    cases = corpus["cases"]
    ids = [case["id"] for case in cases]

    assert corpus["version"] == 1
    assert len(ids) == len(set(ids)), "case ids are unique"
    assert REQUIRED_CASES <= set(ids), REQUIRED_CASES - set(ids)
    for case in cases:
        driven = [key for key in BOUNDARY_KEYS if key in case]
        assert driven, f"{case['id']} has no expectation any boundary test consumes"
        # Every case states that the code pull request is left as it was.
        assert case.get("code_pr_unchanged") is True, case["id"]
        assert "fixture" in case, case["id"]
        fault = case.get("fault")
        if fault is not None:
            assert fault["boundary"] in {"worker", "api"}, case["id"]
        api = case.get("api")
        if api is not None:
            assert api["comment"] in {"none", "evidence", "outcome", "unsupported"}, case["id"]
            assert set(api.get("not_shown") or {}).isdisjoint(
                {str(ordinal) for ordinal in api.get("shown") or []}
            ), case["id"]


def test_every_index_round_trips_through_the_api_schema() -> None:
    for case in corpus_cases():
        index = case.get("index")
        if index is None:
            continue
        resolved = resolve_index(index, SELECTOR_OIDS)

        parsed = UiEvidenceIndexIn.model_validate(resolved)
        again = UiEvidenceIndexIn.model_validate(parsed.model_dump(mode="json"))

        assert again == parsed, case["id"]
        dumped = parsed.model_dump(mode="json")
        assert [image["ordinal"] for image in dumped["images"]] == [
            image["ordinal"] for image in resolved["images"]
        ], case["id"]


def test_every_expected_refusal_has_its_one_sentence() -> None:
    corpus = load_corpus()
    texts = corpus["texts"]
    for case in corpus["cases"]:
        codes = expected_codes(case)
        assert codes <= REFUSAL_CODES, (case["id"], codes - REFUSAL_CODES)
        for code in codes:
            assert isinstance(texts.get(code), str) and texts[code].strip(), (case["id"], code)
    # One sentence per code, never shared, so a comment names its reason exactly.
    sentences = [sentence for code, sentence in texts.items() if code in REFUSAL_CODES]
    assert len(sentences) == len(set(sentences))


def test_the_limits_are_the_ticket_numbers() -> None:
    limits = load_corpus()["limits"]
    values = set(limits.values()) if isinstance(limits, Mapping) else set()
    assert {10 * 1024 * 1024, 1024 * 1024, 100 * 1024 * 1024} <= values


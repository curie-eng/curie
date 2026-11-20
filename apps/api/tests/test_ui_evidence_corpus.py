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
from curie_test_support.ui_evidence import CORPUS_PATH, OIDS, CaseCheckout, png, resolve

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

# Stand-ins for the checkout object ids, for a boundary that builds no checkout.
# The API boundary passes the publication base as P.
STAND_INS = {
    "base": "0123456789abcdef0123456789abcdef01234567",
    "head": "4ead4ead4ead4ead4ead4ead4ead4ead4ead4ead",
    "head_tree": "7ee07ee07ee07ee07ee07ee07ee07ee07ee07ee0",
    "candidate_tree": "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00",
}


def load_corpus() -> dict[str, Any]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def corpus_cases() -> list[dict[str, Any]]:
    return list(load_corpus()["cases"])


def stand_in_checkout(
    case: Mapping[str, Any],
    *,
    base: str = STAND_INS["base"],
    candidate_tree: str = STAND_INS["candidate_tree"],
) -> CaseCheckout:
    """The case's checkout values without building one: stand-in object ids
    (a clean checkout's C is its HEAD tree, as the corpus states) and the exact
    fixture bytes of every PNG file, so ``$size`` and ``$sha256`` resolve."""

    fixture = case["fixture"]
    clean = fixture["checkout"] == "clean"
    originals = {
        path: png(path, spec["png"], size=spec.get("size"))
        for path, spec in fixture["files"].items()
        if "png" in spec
    }
    return CaseCheckout(
        repo=Path("."),
        base=base,
        head=STAND_INS["head"],
        head_tree=candidate_tree if clean else STAND_INS["head_tree"],
        candidate_tree=candidate_tree,
        originals=originals,
    )


def resolve_index(case: Mapping[str, Any], checkout: CaseCheckout) -> dict[str, Any]:
    """The case's index with every corpus placeholder replaced."""

    return resolve(json.loads(json.dumps(case["index"])), checkout)


def _placeholders(node: Any) -> set[str]:
    if isinstance(node, str):
        return {node} if node.startswith("$") else set()
    if isinstance(node, list):
        return set().union(*(_placeholders(item) for item in node))
    if isinstance(node, dict):
        return set().union(*(_placeholders(item) for item in node.values()))
    return set()


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
    defined = set(load_corpus()["conventions"]["placeholders"])
    assert set(OIDS) == {"other_base", "other_tree"}
    for case in corpus_cases():
        index = case.get("index")
        if index is None:
            continue
        assert _placeholders(index) <= defined, case["id"]
        resolved = resolve_index(case, stand_in_checkout(case))
        assert not _placeholders(resolved), case["id"]

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

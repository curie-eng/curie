"""The API's reading of the runner executor vector's remediation codes.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-26. Executor
amendment E6: the pre-dispatch codes gain ``tool_not_read_only``,
``not_reversible_now`` and ``policy_changed``, and ``pointer_absent``,
``result_unstructured`` and a skipped sample are sample results, never
refusals. Of the added codes only ``worker_reported`` (``tool_not_read_only``)
is reported by a worker; the API decides the others itself at dispatch or
claim (``NOT_REVERSIBLE_NOW_CODE``), so a worker report of them is rejected.
``tests/vectors/runner-execute.json`` carries them in ``remediation_codes``; the reader is
``curie_api.action_execution_codes`` (``PRE_DISPATCH_CODES`` and
``outcome_code``). The worker and runner halves of that vector read the rest.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api import action_execution_codes

_CODES = json.loads(
    (Path(__file__).resolve().parents[3] / "tests" / "vectors" / "runner-execute.json").read_text(
        "utf-8"
    )
)["remediation_codes"]


def test_the_remediation_codes_section_has_only_known_keys() -> None:
    assert set(_CODES) == {"pre_dispatch_added", "worker_reported", "not_refusals"}
    assert set(_CODES["worker_reported"]) <= set(_CODES["pre_dispatch_added"])


@pytest.mark.parametrize("code", _CODES["worker_reported"])
def test_each_worker_reported_code_is_a_stored_pre_dispatch_refusal(code: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a refused read execution may end with it."""

    assert code in action_execution_codes.PRE_DISPATCH_CODES
    assert action_execution_codes.outcome_code("refused", code) == code


@pytest.mark.parametrize(
    "code", sorted(set(_CODES["pre_dispatch_added"]) - set(_CODES["worker_reported"]))
)
def test_an_api_decided_code_is_never_taken_from_a_worker(code: str) -> None:
    """@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-16: the API decides it."""

    assert code not in action_execution_codes.PRE_DISPATCH_CODES


@pytest.mark.parametrize("code", _CODES["not_refusals"])
def test_a_sample_result_is_never_a_refusal(code: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a sample result is not a refusal code."""

    assert code not in action_execution_codes.PRE_DISPATCH_CODES
    with pytest.raises(action_execution_codes.CodeRejected):
        action_execution_codes.outcome_code("refused", code)

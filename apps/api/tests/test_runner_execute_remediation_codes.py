"""The API's reading of the runner executor vector's remediation codes.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-26. Executor
amendment E6: the pre-dispatch codes gain ``tool_not_read_only``,
``not_reversible_now`` and ``policy_changed``, and ``pointer_absent`` is a
sample result, never a refusal. ``tests/vectors/runner-execute.json`` carries
them in ``remediation_codes``; the reader is
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
    assert set(_CODES) == {"pre_dispatch_added", "not_refusals"}


@pytest.mark.parametrize("code", _CODES["pre_dispatch_added"])
def test_each_added_code_is_a_stored_pre_dispatch_refusal(code: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a refused execution may end with it."""

    assert code in action_execution_codes.PRE_DISPATCH_CODES
    assert action_execution_codes.outcome_code("refused", code) == code


@pytest.mark.parametrize("code", _CODES["not_refusals"])
def test_a_sample_result_is_never_a_refusal(code: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``pointer_absent`` is a sample, not a refusal."""

    assert code not in action_execution_codes.PRE_DISPATCH_CODES
    with pytest.raises(action_execution_codes.CodeRejected):
        action_execution_codes.outcome_code("refused", code)

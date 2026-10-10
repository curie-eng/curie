"""Worker half of the frozen remediation codes vector: the receipt renderer.

@spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-20
@spec AUTOMATED-REMEDIATION-26. The worker remediation loop posts one thread
message per nomination decision and verification outcome, naming the stage,
action, target key, authority and code. The API stores those vocabularies and
the CLI renders them in other images, so all three read
``tests/vectors/remediation-codes.json``.

The reader is ``curie_worker.remediation_receipts``: ``RECEIPT_STAGES``,
``VERIFICATION_OUTCOMES``, ``AUTHORITIES`` and ``RECEIPT_CODES`` (every code a
receipt may name: the nomination refusals, the approval reasons and the
approval resolution refusals), and ``render_receipt(stage, *, action,
target_key, authority, code)``, which returns the message text and raises
``ValueError`` for a stage, authority or code outside the frozen sets.

The message text is pinned to start ``Remediation <stage>`` on its first line, so a
capture of the thread tells the stages apart (``tests/test_remediation_receipts.py``
in ``apps/api`` reads them back that way). The surface is recorded in
``.projects/plans/task-remediation-receipts.tests.md``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-codes.json"
    ).read_text("utf-8")
)
_KEYS = {
    "comment",
    "nomination_states",
    "nomination_refusals",
    "submission_refusals",
    "admission_checks",
    "approval_reasons",
    "approval_resolution_refusals",
    "verification_outcomes",
    "receipt_stages",
    "kinds",
    "authorities",
    "authority_kinds",
    "actor_kinds",
    "policy_refusals",
}
_RECEIPT_CODES = (
    set(_VECTOR["nomination_refusals"])
    | set(_VECTOR["approval_reasons"])
    | set(_VECTOR["approval_resolution_refusals"])
)
_ACTION = "scale-out-api"
_TARGET_KEY = 'example-scale:"example-api"'


def _receipts() -> Any:  # noqa: ANN401 - the production module under test
    from curie_worker import remediation_receipts

    return remediation_receipts


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-codes.json: {sorted(unknown)}. Teach them to this test, "
        "apps/api/tests/test_remediation_codes_vector.py and cli/tests/remediation_vectors.rs."
    )
    assert set(_VECTOR) == _KEYS


def test_the_renderer_closes_the_frozen_sets() -> None:
    """@spec AUTOMATED-REMEDIATION-20: stages, outcomes, authorities and codes."""

    module = _receipts()
    assert set(module.RECEIPT_STAGES) == set(_VECTOR["receipt_stages"])
    assert set(module.VERIFICATION_OUTCOMES) == set(_VECTOR["verification_outcomes"])
    assert set(module.AUTHORITIES) == set(_VECTOR["authorities"])
    assert set(module.RECEIPT_CODES) == _RECEIPT_CODES


@pytest.mark.parametrize("stage", _VECTOR["receipt_stages"])
def test_each_stage_renders_its_name_action_and_target(stage: str) -> None:
    """@spec AUTOMATED-REMEDIATION-20: one message names the stage, action and target key."""

    text = _receipts().render_receipt(
        stage, action=_ACTION, target_key=_TARGET_KEY, authority="policy", code=None
    )
    assert stage in text
    assert _ACTION in text
    assert _TARGET_KEY in text


@pytest.mark.parametrize("code", sorted(_RECEIPT_CODES))
def test_each_code_renders_by_name(code: str) -> None:
    """@spec AUTOMATED-REMEDIATION-20: the receipt names the code the API stored."""

    text = _receipts().render_receipt(
        "approval_requested", action=_ACTION, target_key=_TARGET_KEY, authority="none", code=code
    )
    assert code in text


@pytest.mark.parametrize("stage", _VECTOR["receipt_stages"])
def test_the_first_line_names_the_stage(stage: str) -> None:
    """@spec AUTOMATED-REMEDIATION-20: the stage is the first line's second word."""

    text = _receipts().render_receipt(
        stage, action=_ACTION, target_key=_TARGET_KEY, authority="none", code=None
    )
    assert text.splitlines()[0].split()[:2] == ["Remediation", stage]


def test_a_receipt_has_no_slot_for_any_other_value() -> None:
    """@spec AUTOMATED-REMEDIATION-20: only the stage, action, target key, authority
    and code are inputs, so no argument value, read result, reason or alert body
    can reach a message through the renderer.
    """

    import inspect

    parameters = inspect.signature(_receipts().render_receipt).parameters
    assert list(parameters) == ["stage", "action", "target_key", "authority", "code"]
    with pytest.raises(TypeError):
        _receipts().render_receipt(  # type: ignore[call-arg]
            "refused",
            action=_ACTION,
            target_key=_TARGET_KEY,
            authority="none",
            code=None,
            reason="free text",
        )


def test_a_refusal_never_reads_as_a_change() -> None:
    """@spec AUTOMATED-REMEDIATION-20: a refusal never produces a "changed" line."""

    text = _receipts().render_receipt(
        "refused",
        action=_ACTION,
        target_key=_TARGET_KEY,
        authority="none",
        code="nomination_malformed",
    )
    assert "changed" not in text.lower()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"stage": "example_unknown_stage"}, id="stage"),
        pytest.param({"authority": "example_unknown_authority"}, id="authority"),
        pytest.param({"code": "example_unknown_code"}, id="code"),
    ],
)
def test_a_value_outside_the_frozen_sets_is_refused(overrides: dict[str, str]) -> None:
    """@spec AUTOMATED-REMEDIATION-26: drift on one side fails loudly, never renders."""

    arguments: dict[str, Any] = {
        "stage": "refused",
        "action": _ACTION,
        "target_key": _TARGET_KEY,
        "authority": "none",
        "code": "unknown_action",
        **overrides,
    }
    stage = arguments.pop("stage")
    with pytest.raises(ValueError):
        _receipts().render_receipt(stage, **arguments)

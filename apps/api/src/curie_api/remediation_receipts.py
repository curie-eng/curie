"""The receipt a nomination row stands for: stage, authority and code.

@spec AUTOMATED-REMEDIATION-20

One derivation from a nomination's own columns, the operator receipt behind
``GET /remediation-nominations`` (and so ``curie remediation list`` and
``show``). The worker's thread receipts follow the same facts, one message per
stage reached, from SQL of their own (``curie_worker.remediation_receipts``).
Nothing here reads the arguments, the model's reason, a read result or the
alert body.

================================  ======================  =========  ===================
state                             stage                   authority  code
================================  ======================  =========  ===================
``refused``                       ``refused``             ``none``   ``refusal_code``
``received``, ``precondition_
pending``                         ``nominated``           ``none``   none
``approval_requested``,
``rejected``, ``expired``         ``approval_requested``  ``none``   ``approval_reason``
``admitted``                      ``nominated``           ``policy`` none
``approved``                      ``approval_requested``  by approval ``approval_reason``
``executing``, ``verifying``      ``executed``            by approval ``execution_code``
``finished``                      the outcome             by approval ``execution_code``
================================  ======================  =========  ===================

The authority is ``approval`` when an approval is named on an ``approved``,
``executing``, ``verifying`` or ``finished`` row, ``policy`` on an admitted or
later row without one, and ``none`` before admission. A ``finished`` row with no
verification outcome (an approved tuning request, which executes nothing) reads
as its approval request, naming ``execution_code``.
"""

from __future__ import annotations

from typing import Any, Final

_NONE: Final = "none"
_POLICY: Final = "policy"
_APPROVAL: Final = "approval"
_AUTHORITY_STATES: Final = frozenset({"approved", "executing", "verifying", "finished"})
_ADMITTED_STATES: Final = frozenset({"admitted", "executing", "verifying", "finished"})


def derive_receipt(row: Any) -> tuple[str, str, str | None]:
    """(stage, authority, code) of a nomination row. @spec AUTOMATED-REMEDIATION-20."""

    state = row.state
    if state in _AUTHORITY_STATES and row.approval_id is not None:
        authority = _APPROVAL
    elif state in _ADMITTED_STATES:
        authority = _POLICY
    else:
        authority = _NONE
    if state == "refused":
        return "refused", _NONE, row.refusal_code
    if state in ("received", "precondition_pending"):
        return "nominated", _NONE, None
    if state in ("approval_requested", "rejected", "expired"):
        return "approval_requested", _NONE, row.approval_reason
    if state == "admitted":
        return "nominated", _POLICY, None
    if state == "approved":
        return "approval_requested", authority, row.approval_reason
    if state == "finished":
        if row.verification_outcome is None:
            return "approval_requested", authority, row.execution_code
        return row.verification_outcome, authority, row.execution_code
    return "executed", authority, row.execution_code


__all__ = ["derive_receipt"]

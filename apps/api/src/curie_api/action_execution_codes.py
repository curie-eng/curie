"""The closed refusal and failure codes of an action execution, by stage.

@spec ACTION-EXECUTOR-20. Each code belongs to exactly one stage, and only
ruling and pre-dispatch codes are provable non-writes. A report never passes
an unknown code through: past dispatch it is normalized by stage
(``connector_error`` for a definite failure, ``response_lost`` for an unknown
outcome); before dispatch it is rejected, because inventing a pre-dispatch
code would assert a non-write nobody proved.
"""

from __future__ import annotations

from typing import Final

PRE_DISPATCH_CODES: Final = frozenset(
    {
        "agent_stopped",
        "authority_unavailable",
        "reserved_verb_via_forward",
        "arguments_mismatch",
        "tool_not_grant_bound",
        "connector_not_hosted",
        "connector_digest_unavailable",
        "restore_not_advertised",
        "restore_schema_mismatch",
        "tool_not_advertised",
        "version_conflict",
        "sandbox_unavailable",
        "runner_unavailable",
        "connector_unreachable",
        # @spec AUTOMATED-REMEDIATION-12 (executor amendment E6): the runner's
        # refusal of a declared read whose tool its own ``list`` did not
        # advertise ``readOnlyHint: true``. Reported by the worker.
        "tool_not_read_only",
    }
)
CONNECTOR_REFUSAL_CODES: Final = frozenset(
    {"version_conflict_at_write", "sealing_key_unavailable", "snapshot_unopenable"}
)
POST_DISPATCH_CODES: Final = frozenset(
    {"connector_error", "unstructured_reply", "response_lost", "deadline_exceeded"}
)

# What an unknown code becomes, by the state it was reported with.
_NORMALIZED: Final = {"failed": "connector_error", "indeterminate": "response_lost"}
# The codes each post-dispatch state may carry as reported.
_ACCEPTED: Final = {
    "failed": CONNECTOR_REFUSAL_CODES | POST_DISPATCH_CODES,
    "indeterminate": POST_DISPATCH_CODES,
}

# @spec AUTOMATED-REMEDIATION-13 (executor amendment E6): the pre-dispatch code
# the API itself ends a policy-authorized forward execution with when its action
# is declared ``reversible`` but its restore capability or sealing key custody
# no longer holds at dispatch. Decided by the API, never reported by a worker,
# so it is not in ``PRE_DISPATCH_CODES``.
NOT_REVERSIBLE_NOW_CODE: Final = "not_reversible_now"

# @spec AUTOMATED-REMEDIATION-12: the sample kinds a read execution stores
# (remediation-predicate.json ``sample_kinds``), and the one the API records
# itself for a sample that missed its slot. Sample results, never refusals.
SAMPLE_KINDS: Final = frozenset(
    {"value", "pointer_absent", "not_scalar", "value_too_long", "tool_error", "result_unstructured"}
)
SKIPPED_SAMPLE: Final = "skipped"

# The pre-dispatch code a row ends with when its lease expired in ``claimed``
# on the last permitted attempt (ACTION-EXECUTOR-17). The holder vanished
# without reporting, so the runner side is what was unavailable.
EXHAUSTED_CLAIM_CODE: Final = "runner_unavailable"
# The code a ``dispatched`` row whose lease expired ends ``indeterminate``
# with: the call may have reached the connector and its answer was lost.
EXPIRED_DISPATCH_CODE: Final = "response_lost"


class CodeRejected(ValueError):
    """A reported code that cannot be stored for the reported state."""


def outcome_code(state: str, code: str | None) -> str | None:
    """The code to store for a reported terminal ``state``. @spec ACTION-EXECUTOR-20.

    Raises ``CodeRejected`` for an unknown state, a ``refused`` report without a
    pre-dispatch code, or a ``confirmed`` report carrying any code.
    """

    if state == "refused":
        if code not in PRE_DISPATCH_CODES:
            raise CodeRejected("a refusal names one pre-dispatch code")
        return code
    if state == "confirmed":
        if code is not None:
            raise CodeRejected("a confirmed outcome carries no code")
        return None
    if state in _ACCEPTED:
        return code if code in _ACCEPTED[state] else _NORMALIZED[state]
    raise CodeRejected("an outcome is refused, confirmed, failed or indeterminate")

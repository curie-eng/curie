"""The worker's half of a connector action execution (ADR 0121).

@spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-20. The
pure pieces the executor loop composes: the arguments of the ``observe_version``
read, the exact canonical text of the ``restore`` call (checked against the
ruling's ``arguments_sha256`` before ``expected_version`` is added), and the
mapping of a connector's reply onto the execution's terminal state and code.
Each is frozen with the runner and the reference connector by
``tests/vectors/executor-restore-calls.json``.

The loop itself (claim, sandbox, dispatch, report) is plan task 11.
"""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from typing import Any, Final, Literal

from . import connector_grant

OBSERVE_TOOL: Final = "observe_version"
RESTORE_TOOL: Final = "restore"
EXPECTED_VERSION_KEY: Final = "expected_version"

CallState = Literal["confirmed", "failed"]

# @spec ACTION-EXECUTOR-20. The connector's own refusals during ``call``; any
# other code is normalized to ``connector_error``, never passed through.
_CONNECTOR_REFUSALS: Final = {
    "version_conflict": "version_conflict_at_write",
    "sealing_key_unavailable": "sealing_key_unavailable",
    "snapshot_unopenable": "snapshot_unopenable",
}


class ExecutorRefusal(Exception):
    """A provable non-write: the execution is refused with ``code`` before dispatch."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def observe_arguments(target: Mapping[str, Any]) -> dict[str, Any]:
    """``observe_version`` gets the recorded target and nothing else. @spec ACTION-EXECUTOR-15."""

    return {"target": dict(target)}


def _declares(schema: object, key: str) -> bool:
    if not isinstance(schema, Mapping):
        return False
    properties = schema.get("properties")
    return isinstance(properties, Mapping) and key in properties


def restore_call(
    *,
    target: Mapping[str, Any],
    prior_state: Mapping[str, Any],
    recorded_version: str,
    restore_input_schema: object,
    arguments_sha256: str,
) -> str:
    """The exact canonical text the ``call`` phase sends to ``restore``.

    @spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-15. The ruling's digest covers
    the two-key form ``{target, prior_state}``; any difference refuses
    ``arguments_mismatch`` before dispatch. ``expected_version`` (the recorded
    ``post_version``) is added only when the connector's ``restore`` input
    schema declares it.
    """

    ruled: dict[str, Any] = {"target": dict(target), "prior_state": dict(prior_state)}
    # The parameter shadows the module function's name, so the module is named.
    recomputed = connector_grant.arguments_sha256(connector_grant.canonical_arguments(ruled))
    if not hmac.compare_digest(
        recomputed.encode("ascii"), arguments_sha256.encode("ascii", "replace")
    ):
        raise ExecutorRefusal("arguments_mismatch")
    if _declares(restore_input_schema, EXPECTED_VERSION_KEY):
        ruled[EXPECTED_VERSION_KEY] = recorded_version
    return connector_grant.canonical_arguments(ruled)


def call_outcome(*, is_error: bool, structured: object) -> tuple[CallState, str | None]:
    """The execution's terminal state and code for one ``call`` reply.

    @spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-20. Only an ``ok: true``
    structured reply that is not a tool error confirms. A known connector
    refusal keeps its code; an unknown one, a tool error, or a malformed reply
    is ``connector_error``; a success with no structure is
    ``unstructured_reply``.
    """

    if not isinstance(structured, Mapping):
        return "failed", "connector_error" if is_error else "unstructured_reply"
    ok = structured.get("ok")
    if ok is True and not is_error:
        return "confirmed", None
    if ok is False:
        refused = structured.get("refused")
        if isinstance(refused, str) and refused in _CONNECTOR_REFUSALS:
            return "failed", _CONNECTOR_REFUSALS[refused]
    return "failed", "connector_error"

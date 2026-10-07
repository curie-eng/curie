"""The closed remediation policy document and its named refusals.

A pure validator: it reads only the document. The checks that need the agent
(the approval route) live in ``remediation_policy_store``. Every refusal is a
``PolicyRefused`` carrying a stable code, so the routes can answer
``{"detail": {"code": ...}}`` instead of FastAPI's validation list, and the
future CLI can mirror the same reasons.

Codes:

* ``policy_unknown_key``: a key outside the closed document, at any level;
* ``policy_document_invalid``: a missing or mistyped field, no actions, a bad or
  duplicate action name, an unknown kind, reversibility or comparator;
* ``policy_limit_out_of_bounds``: a limit looser than the platform default, or
  not positive (AUTOMATED-REMEDIATION-10: a policy may only tighten);
* ``precondition_and_verifier_required``: a ``remediate`` or ``prevent`` action
  without both declared reads;
* ``delta_bound_unsupported``: a delta bound in an argument schema (magnitude is
  bounded by absolute ranges only);
* ``kind_not_automatic``: ``automatic`` true on ``prevent`` or ``tune``
  (AUTOMATED-REMEDIATION-24).

@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from typing import Any, Final

# AUTOMATED-REMEDIATION-10 defaults and ceilings (ruling 9). A policy may only
# tighten them; the incident window may only be lengthened.
PER_POLICY_PER_HOUR_CEILING: Final = 3
PER_INCIDENT_PER_TARGET_CEILING: Final = 1
INCIDENT_WINDOW_SECONDS_MINIMUM: Final = 3600
APPROVAL_TTL_SECONDS_DEFAULT: Final = 14400
APPROVAL_TTL_SECONDS_CEILING: Final = 86400
# An upper bound so a window is a sane integer; not a policy ceiling.
_SECONDS_MAXIMUM: Final = 2**31 - 1

KINDS: Final = frozenset({"remediate", "prevent", "tune"})
NEVER_AUTOMATIC_KINDS: Final = frozenset({"prevent", "tune"})
READS_REQUIRED_KINDS: Final = frozenset({"remediate", "prevent"})
REVERSIBILITIES: Final = frozenset({"reversible", "idempotent"})
COMPARATORS: Final = frozenset({"eq", "ne", "lt", "le", "gt", "ge", "in", "absent"})
ARGUMENT_TYPES: Final = frozenset({"string", "integer", "number", "boolean"})
_RANGE_TYPES: Final = frozenset({"integer", "number"})

_TOP_KEYS: Final = frozenset({"route", "limits", "actions"})
_LIMIT_KEYS: Final = frozenset(
    {
        "per_policy_per_hour",
        "per_incident_per_target",
        "per_action_per_hour",
        "incident_window_seconds",
        "approval_ttl_seconds",
    }
)
_ACTION_KEYS: Final = frozenset(
    {
        "name",
        "kind",
        "connector",
        "tool",
        "arguments",
        "target",
        "reversibility",
        "precondition",
        "verifier",
        "automatic",
        "qualification",
    }
)
_ACTION_REQUIRED: Final = frozenset(
    {"name", "kind", "connector", "tool", "arguments", "target", "reversibility", "automatic"}
)
_ARGUMENT_KEYS: Final = frozenset({"type", "allowed", "minimum", "maximum"})
_DELTA_KEYS: Final = frozenset({"max_delta", "min_delta", "delta"})
_TARGET_KEYS: Final = frozenset({"argument", "allowed"})
_READ_KEYS: Final = frozenset({"connector", "tool", "arguments", "pointer", "comparator", "value"})
_VERIFIER_TIMING_KEYS: Final = frozenset(
    {"settle_seconds", "deadline_seconds", "interval_seconds", "consecutive"}
)
_VERIFIER_KEYS: Final = _READ_KEYS | _VERIFIER_TIMING_KEYS

_ACTION_NAME: Final = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}")
_POINTER: Final = re.compile(r"(/([^~/]|~[01])*)*")
_IN_LIST_MAXIMUM: Final = 16
_ALLOWED_MAXIMUM: Final = 256
_VERIFIER_INTERVAL_MINIMUM: Final = 10
_VERIFIER_DEADLINE_MAXIMUM: Final = 3600
_VERIFIER_SAMPLE_CAP: Final = 60


class PolicyRefused(Exception):
    """A named policy refusal. @spec AUTOMATED-REMEDIATION-2."""

    def __init__(
        self, code: str, path: str | None = None, message: str | None = None, status_code: int = 422
    ) -> None:
        super().__init__(code)
        self.code = code
        self.path = path
        self.message = message
        self.status_code = status_code


def _refuse(code: str, path: str, message: str) -> PolicyRefused:
    return PolicyRefused(code=code, path=path, message=message)


def _closed(value: Any, path: str, allowed: frozenset[str]) -> Mapping[str, Any]:
    """An object whose keys are all in ``allowed``. @spec AUTOMATED-REMEDIATION-2."""
    if not isinstance(value, dict):
        raise _refuse("policy_document_invalid", path, "must be an object")
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise _refuse("policy_unknown_key", f"{path}/{unknown[0]}", "is not a policy key")
    return value


def _require(value: Mapping[str, Any], path: str, required: frozenset[str]) -> None:
    missing = sorted(required - set(value))
    if missing:
        raise _refuse("policy_document_invalid", f"{path}/{missing[0]}", "is required")


def _integer(value: Any, path: str) -> int:
    if type(value) is not int:
        raise _refuse("policy_document_invalid", path, "must be an integer")
    return value


def _number(value: Any, path: str) -> int | float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise _refuse("policy_document_invalid", path, "must be a finite number")
    return value  # type: ignore[no-any-return]


def _identifier(value: Any, path: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise _refuse("policy_document_invalid", path, "must be a non-empty identifier")
    return value


def _scalar(value: Any) -> bool:
    if type(value) is float:
        return math.isfinite(value)
    return value is None or type(value) in (str, int, bool)


def _bounded(value: int, path: str, *, minimum: int, maximum: int) -> int:
    if value < minimum or value > maximum:
        raise _refuse(
            "policy_limit_out_of_bounds", path, f"must be between {minimum} and {maximum}"
        )
    return value


def validate_limits(limits: Any) -> None:
    """AUTOMATED-REMEDIATION-10: limits may only tighten the defaults.

    @spec AUTOMATED-REMEDIATION-10.
    """
    path = "/limits"
    limits = _closed(limits, path, _LIMIT_KEYS)
    values = {key: _integer(limits[key], f"{path}/{key}") for key in limits}
    per_policy = _bounded(
        values.get("per_policy_per_hour", PER_POLICY_PER_HOUR_CEILING),
        f"{path}/per_policy_per_hour",
        minimum=1,
        maximum=PER_POLICY_PER_HOUR_CEILING,
    )
    _bounded(
        values.get("per_incident_per_target", PER_INCIDENT_PER_TARGET_CEILING),
        f"{path}/per_incident_per_target",
        minimum=1,
        maximum=PER_INCIDENT_PER_TARGET_CEILING,
    )
    if "per_action_per_hour" in values:
        _bounded(
            values["per_action_per_hour"],
            f"{path}/per_action_per_hour",
            minimum=1,
            maximum=per_policy,
        )
    _bounded(
        values.get("incident_window_seconds", INCIDENT_WINDOW_SECONDS_MINIMUM),
        f"{path}/incident_window_seconds",
        minimum=INCIDENT_WINDOW_SECONDS_MINIMUM,
        maximum=_SECONDS_MAXIMUM,
    )
    _bounded(
        values.get("approval_ttl_seconds", APPROVAL_TTL_SECONDS_DEFAULT),
        f"{path}/approval_ttl_seconds",
        minimum=1,
        maximum=APPROVAL_TTL_SECONDS_CEILING,
    )


def _allowed_list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list) or not value or len(value) > _ALLOWED_MAXIMUM:
        raise _refuse(
            "policy_document_invalid",
            path,
            f"must be a non-empty list of at most {_ALLOWED_MAXIMUM} literal values",
        )
    if not all(_scalar(item) and item is not None for item in value):
        raise _refuse("policy_document_invalid", path, "must hold JSON scalars only")
    return value


def _validate_argument(spec: Any, path: str) -> None:
    """One closed argument schema entry. @spec AUTOMATED-REMEDIATION-2."""
    if isinstance(spec, dict):
        delta = sorted(set(spec) & _DELTA_KEYS)
        if delta:
            raise _refuse(
                "delta_bound_unsupported",
                f"{path}/{delta[0]}",
                "magnitude is bounded by absolute ranges only",
            )
    spec = _closed(spec, path, _ARGUMENT_KEYS)
    _require(spec, path, frozenset({"type"}))
    kind = spec["type"]
    if kind not in ARGUMENT_TYPES:
        raise _refuse("policy_document_invalid", f"{path}/type", "is not a known argument type")
    has_allowed = "allowed" in spec
    has_range = "minimum" in spec or "maximum" in spec
    if has_allowed == has_range:
        raise _refuse(
            "policy_document_invalid", path, "needs either an allowed set or a minimum and maximum"
        )
    if has_allowed:
        _allowed_list(spec["allowed"], f"{path}/allowed")
        return
    if kind not in _RANGE_TYPES:
        raise _refuse("policy_document_invalid", path, "a range needs an integer or number type")
    _require(spec, path, frozenset({"minimum", "maximum"}))
    convert = _integer if kind == "integer" else _number
    minimum = convert(spec["minimum"], f"{path}/minimum")
    maximum = convert(spec["maximum"], f"{path}/maximum")
    if minimum > maximum:
        raise _refuse("policy_document_invalid", path, "minimum is above maximum")


def _validate_read(read: Any, path: str, *, verifier: bool) -> None:
    """A declared read (AUTOMATED-REMEDIATION-17 shape). @spec AUTOMATED-REMEDIATION-2."""
    keys = _VERIFIER_KEYS if verifier else _READ_KEYS
    read = _closed(read, path, keys)
    _require(read, path, frozenset({"connector", "tool", "pointer", "comparator"}))
    _identifier(read["connector"], f"{path}/connector")
    _identifier(read["tool"], f"{path}/tool")
    arguments = read.get("arguments", {})
    if not isinstance(arguments, dict):
        raise _refuse("policy_document_invalid", f"{path}/arguments", "must be an object")
    if type(read["pointer"]) is not str or not _POINTER.fullmatch(read["pointer"]):
        raise _refuse("policy_document_invalid", f"{path}/pointer", "must be an RFC 6901 pointer")
    comparator = read["comparator"]
    if comparator not in COMPARATORS:
        raise _refuse("policy_document_invalid", f"{path}/comparator", "is not a comparator")
    if comparator == "absent":
        if "value" in read:
            raise _refuse("policy_document_invalid", f"{path}/value", "absent takes no value")
    elif "value" not in read:
        raise _refuse("policy_document_invalid", f"{path}/value", "is required")
    elif comparator == "in":
        value = read["value"]
        if not isinstance(value, list) or not value or len(value) > _IN_LIST_MAXIMUM:
            raise _refuse(
                "policy_document_invalid",
                f"{path}/value",
                f"in takes a list of 1 to {_IN_LIST_MAXIMUM} scalars",
            )
        if not all(_scalar(item) for item in value):
            raise _refuse("policy_document_invalid", f"{path}/value", "must hold scalars only")
    elif not _scalar(read["value"]):
        raise _refuse("policy_document_invalid", f"{path}/value", "must be a JSON scalar")
    if verifier:
        _validate_verifier_timing(read, path)


def _validate_verifier_timing(read: Mapping[str, Any], path: str) -> None:
    """@spec AUTOMATED-REMEDIATION-2 (AUTOMATED-REMEDIATION-17 timing bounds)."""
    _require(read, path, frozenset({"settle_seconds", "deadline_seconds", "interval_seconds"}))
    interval = _integer(read["interval_seconds"], f"{path}/interval_seconds")
    settle = _integer(read["settle_seconds"], f"{path}/settle_seconds")
    deadline = _integer(read["deadline_seconds"], f"{path}/deadline_seconds")
    consecutive = _integer(read.get("consecutive", 1), f"{path}/consecutive")
    if interval < _VERIFIER_INTERVAL_MINIMUM:
        raise _refuse(
            "policy_document_invalid",
            f"{path}/interval_seconds",
            f"must be at least {_VERIFIER_INTERVAL_MINIMUM}",
        )
    if settle < interval:
        raise _refuse(
            "policy_document_invalid", f"{path}/settle_seconds", "must be at least the interval"
        )
    if (
        deadline <= settle
        or deadline > _VERIFIER_DEADLINE_MAXIMUM
        or deadline > _VERIFIER_SAMPLE_CAP * interval
    ):
        raise _refuse(
            "policy_document_invalid",
            f"{path}/deadline_seconds",
            f"must exceed settle and be at most {_VERIFIER_DEADLINE_MAXIMUM} "
            f"and {_VERIFIER_SAMPLE_CAP} intervals",
        )
    if consecutive < 1:
        raise _refuse("policy_document_invalid", f"{path}/consecutive", "must be at least 1")


def _validate_action(action: Any, path: str) -> str:
    """One action; returns its name.

    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-24.
    """
    action = _closed(action, path, _ACTION_KEYS)
    _require(action, path, _ACTION_REQUIRED)
    name = action["name"]
    if type(name) is not str or not _ACTION_NAME.fullmatch(name):
        raise _refuse(
            "policy_document_invalid", f"{path}/name", "must match [a-z0-9][a-z0-9_-]{0,62}"
        )
    kind = action["kind"]
    if kind not in KINDS:
        raise _refuse("policy_document_invalid", f"{path}/kind", "is not a known kind")
    _identifier(action["connector"], f"{path}/connector")
    _identifier(action["tool"], f"{path}/tool")
    if action["reversibility"] not in REVERSIBILITIES:
        raise _refuse(
            "policy_document_invalid", f"{path}/reversibility", "is not a known reversibility"
        )
    automatic = action["automatic"]
    if type(automatic) is not bool:
        raise _refuse("policy_document_invalid", f"{path}/automatic", "must be a boolean")

    arguments = action["arguments"]
    if not isinstance(arguments, dict):
        raise _refuse("policy_document_invalid", f"{path}/arguments", "must be an object")
    for key, spec in arguments.items():
        _validate_argument(spec, f"{path}/arguments/{key}")

    target = _closed(action["target"], f"{path}/target", _TARGET_KEYS)
    _require(target, f"{path}/target", _TARGET_KEYS)
    argument = target["argument"]
    if type(argument) is not str or argument not in arguments:
        raise _refuse(
            "policy_document_invalid", f"{path}/target/argument", "must name a declared argument"
        )
    allowed_targets = _allowed_list(target["allowed"], f"{path}/target/allowed")
    argument_allowed = arguments[argument].get("allowed")
    if argument_allowed is not None and any(
        value not in argument_allowed for value in allowed_targets
    ):
        raise _refuse(
            "policy_document_invalid",
            f"{path}/target/allowed",
            "must be within the target argument's allowed set",
        )

    for read in ("precondition", "verifier"):
        if read in action and action[read] is not None:
            _validate_read(action[read], f"{path}/{read}", verifier=read == "verifier")
    if kind in READS_REQUIRED_KINDS and (
        action.get("precondition") is None or action.get("verifier") is None
    ):
        raise _refuse(
            "precondition_and_verifier_required",
            path,
            f"a {kind} action declares both a precondition and a verifier",
        )
    if automatic and kind in NEVER_AUTOMATIC_KINDS:
        raise _refuse(
            "kind_not_automatic", f"{path}/automatic", f"a {kind} action is never automatic"
        )

    qualification = action.get("qualification")
    if qualification is not None and (type(qualification) is not str or not qualification):
        raise _refuse(
            "policy_document_invalid", f"{path}/qualification", "must be null or a reference"
        )
    return name


def _refuse_non_finite(value: Any, path: str) -> None:
    """Refuse NaN and Infinity anywhere in the document.

    Canonical JSON has no non-finite numbers, but the request parser accepts
    the tokens; refusing them here, before any check or digest, keeps every
    numeric field finite. @spec AUTOMATED-REMEDIATION-2.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise _refuse("policy_document_invalid", path or "/", "must be a finite number")
    if isinstance(value, dict):
        for key, item in value.items():
            _refuse_non_finite(item, f"{path}/{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _refuse_non_finite(item, f"{path}/{index}")


def validate_document(document: Any) -> dict[str, Any]:
    """Validate a whole policy document; returns it unchanged.

    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24.
    """
    _refuse_non_finite(document, "")
    document = _closed(document, "", _TOP_KEYS)
    _require(document, "", _TOP_KEYS)
    route = document["route"]
    if type(route) is not str or not route.strip():
        raise _refuse("policy_document_invalid", "/route", "must name an approval route")
    validate_limits(document["limits"])
    actions = document["actions"]
    if not isinstance(actions, list) or not actions:
        raise _refuse("policy_document_invalid", "/actions", "must be a non-empty list")
    names: set[str] = set()
    for index, action in enumerate(actions):
        name = _validate_action(action, f"/actions/{index}")
        if name in names:
            raise _refuse("policy_document_invalid", f"/actions/{index}/name", "is a duplicate")
        names.add(name)
    return dict(document)


def canonical_json(value: Any) -> str:
    """The canonical JSON the intent digest is taken over. @spec AUTOMATED-REMEDIATION-2."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def intent_sha256(verb: str, document: Any | None) -> str:
    """Digest of one write's intent: the verb and, for a bind, the canonical document.

    Key order never changes the digest, so a replay whose keys arrive in another
    order is the same intent. @spec AUTOMATED-REMEDIATION-2.
    """
    return hashlib.sha256(
        canonical_json({"verb": verb, "policy": document}).encode("ascii")
    ).hexdigest()

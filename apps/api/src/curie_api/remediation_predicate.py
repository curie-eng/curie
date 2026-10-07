"""The API's evaluator of a declared read's predicate against one sample.

@spec AUTOMATED-REMEDIATION-17 @spec AUTOMATED-REMEDIATION-12

The predicate is one RFC 6901 pointer (applied by the runner, which answers only
the pointed scalar) and a closed comparator set; the maintainer ruled on
2026-10-07 that this closed form is not an expression language. The frozen
cases are ``tests/vectors/remediation-predicate.json`` (``evaluations``), read
by ``apps/api/tests/test_remediation_predicate_vector.py``.

``evaluate_sample(predicate, sample)`` returns:

* ``unsuccessful`` for every sample kind outside ``SUCCESSFUL_SAMPLES``
  (``not_scalar``, ``value_too_long``, ``tool_error``, ``result_unstructured``,
  the API's own ``skipped``, and anything unknown), whatever the comparator;
* for ``pointer_absent``, ``satisfied`` only under ``absent``;
* for ``value``: ``eq``, ``ne`` and ``in`` compare JSON numbers numerically and
  any other value by its JSON type and exact value, never reading a string as
  a number; the ordering comparators apply only when both sides are numeric (a
  JSON number, or a string in ``NUMERIC_STRING_GRAMMAR``; a boolean never is)
  and are otherwise unsatisfied; ``absent`` is unsatisfied by any value.

Nothing here logs or returns the sampled value.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any, Final

from .action_execution_codes import SAMPLE_KINDS

SUCCESSFUL_SAMPLES: Final = frozenset({"value", "pointer_absent"})
SATISFIED: Final = "satisfied"
UNSATISFIED: Final = "unsatisfied"
UNSUCCESSFUL: Final = "unsuccessful"
RESULTS: Final = frozenset({SATISFIED, UNSATISFIED, UNSUCCESSFUL})
ORDERING_COMPARATORS: Final = frozenset({"lt", "le", "gt", "ge"})
NUMERIC_STRING_GRAMMAR: Final = r"^[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$"

# ``re.ASCII`` keeps ``[0-9]`` to ASCII digits, and ``fullmatch`` refuses the
# trailing newline ``$`` would otherwise let through.
_NUMERIC = re.compile(NUMERIC_STRING_GRAMMAR.removeprefix("^").removesuffix("$"), re.ASCII)


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _numeric(value: object) -> Decimal | None:
    """The numeric value of an ordering side, or None when it is not numeric."""

    if _is_number(value):
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            return None
        return number if number.is_finite() else None
    if isinstance(value, str) and _NUMERIC.fullmatch(value) is not None:
        try:
            return Decimal(value)
        except InvalidOperation:
            return None
    return None


def _equal(left: object, right: object) -> bool:
    """JSON equality: numbers numerically, anything else by type and exact value."""

    if _is_number(left) and _is_number(right):
        return bool(left == right)
    if _is_number(left) or _is_number(right):
        return False
    return type(left) is type(right) and left == right


def _ordered(comparator: str, sampled: object, declared: object) -> bool:
    left, right = _numeric(sampled), _numeric(declared)
    if left is None or right is None:
        return False
    if comparator == "lt":
        return left < right
    if comparator == "le":
        return left <= right
    if comparator == "gt":
        return left > right
    return left >= right


def evaluate_sample(predicate: Mapping[str, Any], sample: Mapping[str, Any]) -> str:
    """``satisfied``, ``unsatisfied`` or ``unsuccessful``. @spec AUTOMATED-REMEDIATION-17."""

    kind = sample.get("sample")
    if kind not in SUCCESSFUL_SAMPLES:
        return UNSUCCESSFUL
    comparator = predicate.get("comparator")
    if kind == "pointer_absent":
        return SATISFIED if comparator == "absent" else UNSATISFIED
    sampled = sample.get("value")
    declared = predicate.get("value")
    if comparator == "eq":
        held = _equal(sampled, declared)
    elif comparator == "ne":
        held = not _equal(sampled, declared)
    elif comparator == "in":
        held = isinstance(declared, list) and any(_equal(sampled, item) for item in declared)
    elif comparator in ORDERING_COMPARATORS:
        held = _ordered(str(comparator), sampled, declared)
    else:
        # ``absent`` against a present value, or a comparator outside the
        # closed set (refused at policy write): never satisfied.
        held = False
    return SATISFIED if held else UNSATISFIED


__all__ = [
    "NUMERIC_STRING_GRAMMAR",
    "ORDERING_COMPARATORS",
    "RESULTS",
    "SAMPLE_KINDS",
    "SUCCESSFUL_SAMPLES",
    "evaluate_sample",
]

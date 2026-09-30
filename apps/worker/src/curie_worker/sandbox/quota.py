"""Fail closed normalization for Kubernetes ResourceQuota evidence."""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import (
    Context,
    Decimal,
    Inexact,
    InvalidOperation,
    Overflow,
    Rounded,
    localcontext,
)

from kubernetes.utils.quantity import parse_quantity

from .types import QuotaRejection

_QUANTITY_CONTEXT_PRECISION = 128
_QUANTITY_RE = re.compile(
    r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"
    r"(?:(?:[eE][+-]?[0-9]+)|(?:[numkMGTPE]|Ki|Mi|Gi|Ti|Pi|Ei))?"
)
_DNS_1123_LABEL_RE = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")


def quota_rejection_is_valid(rejection: QuotaRejection) -> bool:
    """Whether rejection carries complete, exact Kubernetes quota evidence."""

    try:
        with localcontext() as context:
            _configure_quantity_context(context)
            return _normalize_rejection(rejection) is not None
    except (ValueError, ArithmeticError):
        return False


def quota_has_live_headroom(
    rejection: QuotaRejection,
    *,
    spec_hard: object,
    status_hard: object,
    status_used: object,
) -> bool:
    """Whether current enforced usage admits every resource in rejection."""

    try:
        with localcontext() as context:
            _configure_quantity_context(context)
            normalized = _normalize_rejection(rejection)
            if normalized is None:
                return False
            requested, _used, _hard = normalized
            if not all(
                isinstance(values, Mapping)
                for values in (spec_hard, status_hard, status_used)
            ):
                return False

            for resource, request in requested.items():
                live_spec_hard = _parse_map_quantity(spec_hard, resource)
                live_status_hard = _parse_map_quantity(status_hard, resource)
                live_used = _parse_map_quantity(status_used, resource)
                if (
                    live_spec_hard is None
                    or live_status_hard is None
                    or live_used is None
                    or live_spec_hard < 0
                    or live_status_hard < 0
                    or live_used < 0
                    or live_spec_hard != live_status_hard
                    or live_used + request > live_status_hard
                ):
                    return False
            return True
    except (ValueError, ArithmeticError):
        return False


def _configure_quantity_context(context: Context) -> None:
    context.prec = _QUANTITY_CONTEXT_PRECISION
    for signal in (Inexact, Rounded, Overflow, InvalidOperation):
        context.traps[signal] = True


def _normalize_rejection(
    rejection: QuotaRejection,
) -> tuple[dict[str, Decimal], dict[str, Decimal], dict[str, Decimal]] | None:
    if not _valid_quota_name(rejection.quota_name):
        return None
    key_sets = (
        set(rejection.requested),
        set(rejection.used),
        set(rejection.hard),
    )
    if not key_sets[0] or key_sets[0] != key_sets[1] or key_sets[0] != key_sets[2]:
        return None
    if any(not isinstance(key, str) or not key for key in key_sets[0]):
        return None

    requested = _parse_quantity_map(rejection.requested)
    used = _parse_quantity_map(rejection.used)
    hard = _parse_quantity_map(rejection.hard)
    if requested is None or used is None or hard is None:
        return None
    for resource in requested:
        if (
            requested[resource] <= 0
            or used[resource] < 0
            or hard[resource] < 0
            or used[resource] + requested[resource] <= hard[resource]
        ):
            return None
    return requested, used, hard


def _parse_quantity_map(values: Mapping[str, str]) -> dict[str, Decimal] | None:
    normalized: dict[str, Decimal] = {}
    for resource, raw in values.items():
        value = _parse_quantity(raw)
        if value is None:
            return None
        normalized[resource] = value
    return normalized


def _parse_map_quantity(values: object, resource: str) -> Decimal | None:
    if not isinstance(values, Mapping) or resource not in values:
        return None
    return _parse_quantity(values[resource])


def _parse_quantity(raw: object) -> Decimal | None:
    if not isinstance(raw, str) or _QUANTITY_RE.fullmatch(raw) is None:
        return None
    value = parse_quantity(raw)
    if not isinstance(value, Decimal) or not value.is_finite():
        return None
    return value


def _valid_quota_name(name: str) -> bool:
    if not isinstance(name, str) or not name or len(name) > 253:
        return False
    labels = name.split(".")
    return all(
        len(label) <= 63 and _DNS_1123_LABEL_RE.fullmatch(label) is not None
        for label in labels
    )

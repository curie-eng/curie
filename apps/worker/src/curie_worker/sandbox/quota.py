"""Fail closed normalization for Kubernetes ResourceQuota evidence."""

from __future__ import annotations

import json
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

from .types import (
    AdmissionConfig,
    AdmissionRejection,
    CapacityRejection,
    QuotaRejection,
    QuotaStatus,
)

ADMISSION_DIMENSIONS = frozenset(
    {"pods", "requests.cpu", "requests.memory", "limits.cpu", "limits.memory"}
)


def resource_vector(values: Mapping[str, str]) -> dict[str, str]:
    """Normalize quantities exactly before they cross the atomic ledger seam."""

    if not values or not set(values) <= ADMISSION_DIMENSIONS:
        raise ValueError("unsupported sandbox admission resource dimension")
    with localcontext() as context:
        _configure_quantity_context(context)
        normalized = _parse_quantity_map(values)
    if normalized is None or any(value < 0 for value in normalized.values()):
        raise ValueError("invalid sandbox admission resource quantity")
    result = {}
    for key, value in normalized.items():
        text = format(value, "f") if value else "0"
        result[key] = text.rstrip("0").rstrip(".") if "." in text else text
    return result


def parse_admission_config(raw: str | None) -> AdmissionConfig | None:
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != {
        "quota_name", "priority_class", "ceiling", "hard"
    }:
        raise ValueError("invalid sandbox admission contract")
    if (
        not _valid_quota_name(value["quota_name"])
        or not _valid_quota_name(value["priority_class"])
        or type(value["ceiling"]) is not int
        or value["ceiling"] < 0
        or not isinstance(value["hard"], dict)
        or set(value["hard"]) != ADMISSION_DIMENSIONS
    ):
        raise ValueError("invalid sandbox admission contract")
    resource_vector(value["hard"])
    return AdmissionConfig(**value)


def capacity_rejection_is_valid(rejection: CapacityRejection) -> bool:
    if isinstance(rejection, QuotaRejection):
        return quota_rejection_is_valid(rejection)
    if not isinstance(rejection, AdmissionRejection) or rejection.evidence_type != "admission":
        return False
    try:
        requested, used, hard = (
            resource_vector(values)
            for values in (rejection.requested, rejection.used, rejection.hard)
        )
        return (
            _valid_quota_name(rejection.quota_name)
            and set(requested) == set(used) == set(hard)
            and any(
                Decimal(used[key]) + Decimal(requested[key]) > Decimal(hard[key])
                for key in hard
            )
        )
    except (ValueError, ArithmeticError, TypeError):
        return False


def admission_quota_headroom(
    config: AdmissionConfig, namespace: str, requested: Mapping[str, str], quota: QuotaStatus
) -> bool:
    """Validate the published quota and all live dimensions before submitting."""

    if quota.name != config.quota_name or quota.namespace != namespace:
        raise ValueError("sandbox admission quota identity mismatch")
    hard = resource_vector(config.hard)
    spec = resource_vector(quota.spec_hard)
    status = resource_vector(quota.status_hard)
    used = resource_vector(quota.status_used)
    if hard != spec or hard != status or set(used) != set(hard):
        raise ValueError("sandbox admission quota envelope mismatch")
    request = resource_vector(requested)
    return all(
        Decimal(used[key]) + Decimal(request.get(key, "0")) <= Decimal(hard[key])
        for key in hard
    )


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

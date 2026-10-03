"""Per-agent runner resource override: shape and sandbox quota (#3209)."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

_DIMENSIONS = ("cpu", "memory", "ephemeral-storage")
_CPU = re.compile(r"^(\d+)(m)?$")
# The quota settings carry whatever Kubernetes accepts in
# `resourceQuota.hard.*`, including every decimal spelling: "2.5", "0.5",
# ".5" and "2." are all valid quantities (#3719). Only the quota side parses
# with this grammar; override values keep `_CPU`.
_QUOTA_CPU = re.compile(r"^(\d+(?:\.\d*)?|\.\d+)(m)?$")
_MEMORY = re.compile(r"^(\d+)(Ki|Mi|Gi|Ti)?$")
_MEMORY_SCALE = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}


class _Side(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    cpu: str
    memory: str
    ephemeral_storage: str = Field(alias="ephemeral-storage")


class RunnerResources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests: _Side
    limits: _Side


class RunnerResourcesError(ValueError):
    """A runner resource override the API refuses before it writes the row."""


def validate_runner_resources(value: Any) -> dict[str, Any] | None:
    """Return the stored object, or None when the override is cleared.

    Raises RunnerResourcesError when the shape is not the six-key block or a
    request exceeds its limit. Quantities are compared, not strings.
    """

    if value is None:
        return None
    if not isinstance(value, dict):
        raise RunnerResourcesError("runner resources must be an object")
    try:
        RunnerResources.model_validate(value)
    except ValidationError as exc:
        raise RunnerResourcesError(
            "runner resources must set cpu, memory, and ephemeral-storage on requests and limits"
        ) from exc
    normalized = {
        side: {dimension: str(value[side][dimension]).strip() for dimension in _DIMENSIONS}
        for side in ("requests", "limits")
    }
    for side in ("requests", "limits"):
        for dimension in _DIMENSIONS:
            _parse(dimension, normalized[side][dimension])
    for dimension in _DIMENSIONS:
        request = _parse(dimension, normalized["requests"][dimension])
        limit = _parse(dimension, normalized["limits"][dimension])
        if request > limit:
            raise RunnerResourcesError(
                f"{dimension} request {normalized['requests'][dimension]} is above "
                f"limit {normalized['limits'][dimension]}"
            )
    return normalized


def quota_refusal(
    value: dict[str, Any],
    *,
    requests_cpu: str | None,
    requests_memory: str | None,
    limits_cpu: str | None,
    limits_memory: str | None,
) -> str | None:
    """Return a refusal sentence, or None when the quota check does not apply.

    All four settings unset skips the check. Any missing or blank setting is
    incomplete. Otherwise each override quantity is compared directly to the
    matching hard quota. Init containers do not add.

    The two sides parse with different grammars (#3719). An override value
    keeps the whole-core grammar of `_parse`; a quota ceiling carries whatever
    Kubernetes accepts in `resourceQuota.hard.*`, so a decimal core like "2.5"
    compares in millicores exactly like "2500m". A quota value that does not
    parse refuses naming the quota setting, never the request.
    """

    settings = (requests_cpu, requests_memory, limits_cpu, limits_memory)
    if all(item is None for item in settings):
        return None
    if any(item is None or item == "" for item in settings):
        return "quota configuration is incomplete"
    hard = {
        ("requests", "cpu"): requests_cpu,
        ("requests", "memory"): requests_memory,
        ("limits", "cpu"): limits_cpu,
        ("limits", "memory"): limits_memory,
    }
    for (side, dimension), ceiling in hard.items():
        assert ceiling is not None
        setting = f"CURIE_SANDBOX_QUOTA_{side.upper()}_{dimension.upper()}"
        got = str(value[side][dimension])
        # The override parses first with its own stricter grammar, so an
        # invalid override value stays a request error even when the quota
        # ceiling is also unparseable.
        requested = _parse(dimension, got)
        if requested > _quota_ceiling(dimension, ceiling, setting):
            kind = "request" if side == "requests" else "limit"
            return (
                f"{dimension} {kind} {got} cannot fit sandbox quota hard {ceiling}; "
                "lower the override or raise resourceQuota.hard"
            )
    return None


def _quota_ceiling(dimension: str, raw: str, setting: str) -> int:
    """Parse a quota ceiling with the quota grammar, not the override grammar.

    The chart renders `resourceQuota.hard.*` straight into the
    `CURIE_SANDBOX_QUOTA_*` settings, so a quota value may be any Kubernetes
    CPU quantity, decimal cores included. An unparseable quota is operator
    configuration, so the refusal names the setting, never the request.
    """

    text = raw.strip()
    if dimension == "cpu":
        match = _QUOTA_CPU.fullmatch(text)
        if match is None:
            raise RunnerResourcesError(
                f"sandbox quota setting {setting} value {raw!r} is not a valid cpu quantity"
            )
        # Decimal, not float: a quota like 9007199254740993m must stay exact so
        # an override equal to its quota still fits. A fractional millicore
        # ceiling ("2.5m") truncates toward zero, which only tightens it.
        amount = Decimal(match.group(1))
        return int(amount) if match.group(2) else int(amount * 1000)
    try:
        return _parse(dimension, text)
    except RunnerResourcesError:
        raise RunnerResourcesError(
            f"sandbox quota setting {setting} value {raw!r} is not a valid {dimension} quantity"
        ) from None


def _parse(dimension: str, raw: str) -> int:
    text = raw.strip()
    if not text:
        raise RunnerResourcesError(f"{dimension} quantity must not be blank")
    if dimension == "cpu":
        match = _CPU.fullmatch(text)
        if match is None:
            raise RunnerResourcesError(
                f"cpu quantity {raw!r} is not a whole number of cores or millicores"
            )
        amount = int(match.group(1))
        return amount if match.group(2) else amount * 1000
    match = _MEMORY.fullmatch(text)
    if match is None:
        raise RunnerResourcesError(f"{dimension} quantity {raw!r} is not a memory quantity")
    amount = int(match.group(1))
    scale = _MEMORY_SCALE.get(match.group(2) or "", 1)
    return amount * scale

"""Per-agent runner resource override: shape and sandbox quota (#3209)."""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

_DIMENSIONS = ("cpu", "memory", "ephemeral-storage")
_CPU = re.compile(r"^(\d+)(m)?$")
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
    for side in ("requests", "limits"):
        for dimension in _DIMENSIONS:
            _parse(dimension, str(value[side][dimension]))
    for dimension in _DIMENSIONS:
        request = _parse(dimension, str(value["requests"][dimension]))
        limit = _parse(dimension, str(value["limits"][dimension]))
        if request > limit:
            raise RunnerResourcesError(
                f"{dimension} request {value['requests'][dimension]} is above "
                f"limit {value['limits'][dimension]}"
            )
    return value


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
        got = str(value[side][dimension])
        if _parse(dimension, got) > _parse(dimension, ceiling):
            return (
                f"{dimension} request {got} cannot fit sandbox quota hard {ceiling}; "
                "lower the override or raise resourceQuota.hard"
            )
    return None


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

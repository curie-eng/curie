"""Strict internal chart-to-worker configuration for Docker daemons."""

from __future__ import annotations

from decimal import DecimalException
from typing import Annotated, Any, Literal

from kubernetes.utils.quantity import parse_quantity
from pydantic import AfterValidator, BaseModel, ConfigDict, Field

POSITIVE_QUANTITY_PATTERN = (
    r"^\+?(?:[0-9]*[1-9][0-9]*(?:\.[0-9]*)?|[0-9]*\.[0-9]*[1-9][0-9]*)"
    r"(?:[eE][+-]?[0-9]{1,3}|[numkMGTPE]|[KMGTPE]i)?$"
)
QUANTITY_MAX_LENGTH = 128
_NO_WHITESPACE: dict[str, Any] = {"not": {"pattern": r"\s"}}


def _positive_quantity(value: str) -> str:
    try:
        quantity = parse_quantity(value)
        if quantity.is_finite() and quantity > 0:
            return value
    except (DecimalException, ValueError):
        pass
    raise ValueError("quantity must be finite and positive") from None


type PositiveQuantity = Annotated[
    str,
    Field(
        max_length=QUANTITY_MAX_LENGTH,
        pattern=POSITIVE_QUANTITY_PATTERN,
        json_schema_extra=_NO_WHITESPACE,
    ),
    AfterValidator(_positive_quantity),
]
type LabelValue = Annotated[
    str,
    Field(
        max_length=63,
        pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?$",
        json_schema_extra=_NO_WHITESPACE,
    ),
]


class _Model(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        serialize_by_alias=True,
    )


class DaemonSelectorLabels(_Model):
    """The complete chart selector for a daemon."""

    name: LabelValue = Field(alias="app.kubernetes.io/name")
    instance: LabelValue = Field(alias="app.kubernetes.io/instance")
    component: Literal["docker-daemon"] = Field(alias="app.kubernetes.io/component")


class DaemonResourceQuantities(_Model):
    """Required resource quantities for one request or limit envelope."""

    cpu: PositiveQuantity
    memory: PositiveQuantity
    ephemeral_storage: PositiveQuantity = Field(alias="ephemeral-storage")


class DaemonResources(_Model):
    """The daemon's explicit request and limit envelopes."""

    requests: DaemonResourceQuantities
    limits: DaemonResourceQuantities


class ToolDaemonConfig(_Model):
    """Complete daemon configuration handed off by the chart, without defaults."""

    namespace: Annotated[
        str,
        Field(
            max_length=63,
            pattern=r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$",
            json_schema_extra=_NO_WHITESPACE,
        ),
    ]
    image: Annotated[
        str,
        Field(
            pattern=r"^[A-Za-z0-9][A-Za-z0-9._/:\-]*@sha256:[0-9a-f]{64}$",
            json_schema_extra=_NO_WHITESPACE,
        ),
    ]
    port: Annotated[int, Field(ge=1, le=65535)]
    selector_labels: DaemonSelectorLabels = Field(alias="selectorLabels")
    resources: DaemonResources
    storage_size_limit: PositiveQuantity = Field(alias="storageSizeLimit")
    max_claim_lifetime_seconds: Annotated[int, Field(gt=0)] = Field(alias="maxClaimLifetimeSeconds")
    ping_timeout_seconds: Annotated[int, Field(gt=0)] = Field(alias="pingTimeoutSeconds")

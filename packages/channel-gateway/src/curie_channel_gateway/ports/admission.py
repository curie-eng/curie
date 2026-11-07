"""Relay an admission question to the API that owns its decision."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, Protocol


@dataclass(frozen=True)
class AdmissionRequest:
    address: str
    adapter: str | None
    callers: tuple[str, ...]
    evidence: Mapping[str, object] | None = None


@dataclass(frozen=True)
class AdmissionAnswer:
    """API answer. connect_url is single-use: never cache or share it."""

    allowed: bool
    restricted: bool
    install_restricted: bool = True
    connect_url: str | None = None


class AdmissionClient(Protocol):
    """Wrap today's /channels/admission call; never grant admission locally.

    The lane adapter fixes kind as a class constant, never from a delivery.
    Evidence is a gateway assertion. The gateway authenticates Slack through
    signature verification or Socket Mode; the API verifies our proof signature
    and evidence shape and derives the principal, but cannot confirm that the
    delivery came from Slack. Platform-key and attester-secret holders can assert
    any user. None means unavailable, distinct
    from a refused answer. Cache policy and Valkey ownership remain unchanged.
    Mail retains /channels/turns and its scoped credential rather than gaining
    access to the dispatcher's platform-key admission endpoint.
    """

    kind: ClassVar[str]

    def ask(self, request: AdmissionRequest) -> AdmissionAnswer | None: ...

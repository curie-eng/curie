"""Relay an admission question to the API that owns its decision."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class AdmissionRequest:
    kind: str
    address: str
    adapter: str | None
    callers: tuple[str, ...]
    evidence: Mapping[str, object] | None = None


@dataclass(frozen=True)
class AdmissionAnswer:
    allowed: bool
    restricted: bool
    install_restricted: bool = True
    connect_url: str | None = None


class AdmissionClient(Protocol):
    """Wrap today's /channels/admission call; never grant admission locally.

    Evidence is provider-authenticated input relayed to the API's existing
    provider field, not an asserted principal. None means unavailable, distinct
    from a refused answer. Cache policy and Valkey ownership remain unchanged.
    Mail retains /channels/turns and its scoped credential rather than gaining
    access to the dispatcher's platform-key admission endpoint.
    """

    def ask(self, request: AdmissionRequest) -> AdmissionAnswer | None: ...

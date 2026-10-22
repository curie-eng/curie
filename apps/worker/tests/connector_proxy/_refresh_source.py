"""A test-only refresh-capable source registered into the real resolver.

ADR 0239 "Static sources" 4 and 5: durable intents fence a refresh, the
replacement is persisted before the access token is released, and an
uncertain outcome enters ``recovery_required``. The resolver owns that
protocol (``RefreshCoordinator``); a source only decides from its material
whether it is fresh and performs the provider exchange. This source is the
smallest honest one: its material is a v1 envelope whose slot carries
``access_token``, ``expires_at`` (epoch seconds as text) and
``refresh_token``; the exchange is an HTTPS POST to the fake provider's token
endpoint, trusted through the provider's test CA.

It counts its own ``release`` and ``refresh`` calls so a test can show that a
refusal happened before any source ran.
"""

from __future__ import annotations

import json
import threading
import time
from typing import ClassVar

import aiohttp
from _fake_provider import TOKEN_PATH, FakeProvider
from _resolver_harness import envelope
from curie_connector_proxy.resolver.admission import Admitted
from curie_connector_proxy.resolver.keys import KeyRing, KeyUnavailable
from curie_connector_proxy.resolver.refresh import RefreshCapableSource
from curie_connector_proxy.resolver.sources import (
    RefreshNeeded,
    Release,
    SourceRefusal,
    parse_envelope,
)

ACCESS = "access_token"
EXPIRES = "expires_at"
REFRESH = "refresh_token"


def refresh_material(slot: str, access: str, *, expires_at: int, refresh: str) -> bytes:
    return envelope({slot: {ACCESS: access, EXPIRES: str(expires_at), REFRESH: refresh}})


class ProviderRefreshSource(RefreshCapableSource):
    kind = "refresh"
    lease_seconds: ClassVar[int] = 60

    def __init__(self, provider: FakeProvider) -> None:
        self._provider = provider
        self._lock = threading.Lock()
        self.release_calls = 0
        self.refresh_calls = 0

    def release(self, admitted: Admitted, slot: str, keys: KeyRing) -> Release | RefreshNeeded:
        with self._lock:
            self.release_calls += 1
        if admitted.recipient_id is None or admitted.ciphertext is None:
            raise SourceRefusal("source_unavailable")
        try:
            material = keys.open(admitted.recipient_id, admitted.ciphertext)
        except KeyUnavailable:
            raise SourceRefusal("key_unavailable") from None
        fields = parse_envelope(material, slot)
        expires = fields[EXPIRES].text
        if expires is None or int(expires) <= time.time():
            return RefreshNeeded()
        return Release(fields={ACCESS: fields[ACCESS]})

    async def refresh(self, admitted: Admitted, slot: str, material: bytes) -> bytes:
        with self._lock:
            self.refresh_calls += 1
        fields = parse_envelope(material, slot)
        refresh_token = fields[REFRESH].text
        assert refresh_token is not None
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                self._provider.origin + TOKEN_PATH,
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                ssl=self._provider.client_ssl(),
                allow_redirects=False,
            ) as response,
        ):
            if response.status != 200:
                raise RuntimeError("the provider refused the refresh")
            issued = json.loads(await response.read())["access_token"]
        return refresh_material(
            slot, issued, expires_at=int(time.time()) + 3600, refresh=refresh_token
        )


class ShortLeaseRefreshSource(ProviderRefreshSource):
    """Two-second lease, so a crashed refresh's lease expires within a test."""

    lease_seconds: ClassVar[int] = 2

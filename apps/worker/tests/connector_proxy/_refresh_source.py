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
from _shared_connector_support import LOCK_WAIT_S
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


class PausingRefreshSource(ProviderRefreshSource):
    """Holds one armed ``release`` at a gate, inside the admission transaction.

    ``release`` runs after admission took the application and binding locks
    and before COMMIT, so a test can act from another connection while a
    resolve is in flight. ``release`` is synchronous, so the gate blocks the
    resolver's event loop; the wait is bounded by ``hold_s`` and a gate that is
    never opened refuses the resolve (``source_unavailable``) instead of
    hanging it.
    """

    hold_s: ClassVar[float] = LOCK_WAIT_S

    def __init__(self, provider: FakeProvider) -> None:
        super().__init__(provider)
        self._armed = False
        self._entered = threading.Event()
        self._opened = threading.Event()

    def arm(self) -> None:
        """Hold the next ``release`` until ``open`` (or ``hold_s``)."""

        with self._lock:
            self._armed = True
            self._entered.clear()
            self._opened.clear()

    def wait_entered(self, timeout: float) -> None:
        if not self._entered.wait(timeout):
            raise AssertionError("no resolve reached the armed source")

    def open(self) -> None:
        self._opened.set()

    def release(self, admitted: Admitted, slot: str, keys: KeyRing) -> Release | RefreshNeeded:
        with self._lock:
            held, self._armed = self._armed, False
        if held:
            self._entered.set()
            if not self._opened.wait(self.hold_s):
                raise RuntimeError("the paused source was never opened")
        return super().release(admitted, slot, keys)

"""``ChannelCampaignGuard``: per-call reobservation over producer Protocols, ADR 0236.

@spec PROTECTED-COMMISSIONING-4 step 5 and @spec PROTECTED-COMMISSIONING-5.
The control process constructs one guard per campaign. Every call, in order:

1. reobserves every ADR 0236 fact through its own producers and requires each
   digest to equal the premeasured tuple;
2. runs the ACL principal state check of design decision 7 against the
   installation baseline and the current phase;
3. requires a signed provisioner reply over exactly those digests from the
   observation channel, which the provisioner signs only after its own fresh
   reobservation.

Nothing is cached between calls: ADR 0236 rejects caching guard results, so a
caller that polls must budget one provisioner round trip per call. Each
completed call records its measured latency. The first refusal, timeout or
cancellation fences the guard permanently. A passing call is a precondition for
one admission step, never an acceptance, Qualification or Readiness.

The transport is structural so this primitive does not import the worker: the
worker's ``ObservationRequester`` satisfies it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import NoReturn, Protocol

from .commissioning_producers import (
    OBSERVATION_FACTS,
    AclPrincipalStateProducer,
    PhaseSource,
    ReobservationSet,
)


class OperatorGuardRefused(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Commissioning operator guard refused")


def _refuse() -> NoReturn:
    raise OperatorGuardRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


class ObservationTransport(Protocol):
    """The signed ADR 0236 channel: returns only after a verified reply for these digests."""

    async def require(self, facts: Mapping[str, str]) -> None: ...


class ChannelCampaignGuard:
    """One campaign's guard; fenced after its first refusal."""

    def __init__(
        self,
        *,
        premeasured: Mapping[str, str],
        control: ReobservationSet,
        acl: AclPrincipalStateProducer,
        phase: PhaseSource,
        transport: ObservationTransport,
        deadline_s: float = 30.0,
    ) -> None:
        try:
            _require(
                isinstance(premeasured, Mapping) and set(premeasured) == set(OBSERVATION_FACTS)
            )
            _require(type(control) is ReobservationSet and type(acl) is AclPrincipalStateProducer)
            _require(callable(phase) and callable(getattr(transport, "require", None)))
            _require(type(deadline_s) in (int, float) and 0 < deadline_s <= 120)
            expected = {name: premeasured[name] for name in OBSERVATION_FACTS}
            _require(all(type(v) is str and len(v) == 64 for v in expected.values()))
            # The ACL baseline belongs to the installation whose bootstrap B is measured.
            _require(acl.baseline.bootstrap_digest == expected["bootstrap"])
        except OperatorGuardRefused:
            raise
        except Exception:  # noqa: BLE001
            _refuse()
        self._expected, self._control, self._acl = expected, control, acl
        self._phase, self._transport, self._deadline = phase, transport, deadline_s
        self._gate = asyncio.Lock()
        self._failed = False
        self._latencies: list[int] = []

    @property
    def calls(self) -> int:
        return len(self._latencies)

    @property
    def latencies_ms(self) -> tuple[int, ...]:
        """Measured wall time of each completed call; evidence for the latency budget."""
        return tuple(self._latencies)

    async def require_current(self) -> None:
        _require(not self._failed)
        try:
            async with self._gate:
                _require(not self._failed)
                started = time.monotonic()
                async with asyncio.timeout(self._deadline):
                    observed = await self._control.reobserve()
                    _require(observed == self._expected)
                    await self._acl.require(self._phase())
                    await self._transport.require(observed)
                self._latencies.append(round((time.monotonic() - started) * 1000))
        except BaseException as error:
            self._failed = True
            if not isinstance(error, Exception):
                raise
            _refuse()

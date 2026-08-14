"""The vendor supplied ingress driver, and the correlation handshake (#1516).

Floor rules 1, 2 and 7, and clause 3b, are about what an adapter does with its
OWN upstream: a stable delivery id across a retry, a response treated as final,
a stale ingress credential survived, an egress secret unset. Nothing the kit can
reach over the adapter's egress endpoint provokes any of those, so the adapter's
author implements this Protocol once, in their own repo, and the kit drives it.

Without a driver those rules report ``not_run``, and ``not_run`` is
nonconformant. There is no shape of run in which missing evidence reads as a
pass.

**The correlation rule is the load bearing half.** ``stimulate`` returns the
``delivery_id`` the adapter will put on the wire for the message it just
injected, and ``FakeIngress`` correlates an observed POST to that stimulus by
matching the body's ``delivery_id`` against it. Correlation is therefore a
property of the OBSERVED WIRE and never of private in process state, which is
the only form an adapter in another process or another language can satisfy. A
``stimulate`` that returns an id the adapter never sends is itself a finding:
rules 1 and 2 fail naming the identity nothing matched, because reading a driver
that does not implement the contract as conformant would certify every broken
vendor driver along with it.
"""

from __future__ import annotations

from types import EllipsisType
from typing import Protocol

from pydantic import BaseModel, ConfigDict


class UpstreamIdentity(BaseModel):
    """One injected upstream message, in the two terms the kit needs.

    ``stimulus_id`` is the kit's own label, for reporting. ``delivery_id`` is
    the contract: whatever the adapter will send for this message, this is it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    stimulus_id: str
    delivery_id: str


class IngressDriver(Protocol):
    """What an adapter author implements so the ingress rules can be decided.

    ``restart`` uses Ellipsis as the UNCHANGED sentinel, because both credential
    arguments need a three way choice and ``None`` already means "unset". Clause
    3b calls ``restart(egress_secret=None)`` to prove the adapter refuses to
    serve with no secret of its own; rule 7 calls ``restart(token=fresh)`` to
    supply an operator issued replacement, and must not disturb the egress
    secret while doing it. Not passing an argument leaves that credential alone.
    """

    def start(self, *, ingress_url: str, token: str) -> None:
        """Point the running adapter at this ingress. CONFIGURATION ONLY.

        No adapter modification: an adapter that has to be edited to be checked
        is not the adapter the operator runs.
        """
        ...

    def stimulate(self) -> UpstreamIdentity:
        """Deliver one upstream message, and declare the id it will carry."""
        ...

    def set_transport(self, *, reachable: bool) -> None:
        """Break, or restore, the adapter's path to ingress.

        Rule 1 is about retrying a TRANSPORT failure with the same delivery id,
        so the failure has to be provoked below the response layer. Breaking it
        here rather than at the ingress is deliberate: a failure the ingress
        answered is a response, and a response is final under rule 2.
        """
        ...

    def restart(
        self,
        *,
        egress_secret: str | None | EllipsisType = ...,
        token: str | None | EllipsisType = ...,
    ) -> None:
        """Restart the adapter, optionally replacing one credential."""
        ...

    def stop(self) -> None:
        """Stop the adapter. Called once, at the end of a run."""
        ...

"""The vendor supplied ingress driver, and the correlation handshake (#1516).

Floor rules 1, 2 and 7, and clause 3b, are about what an adapter does with its
OWN upstream: a stable delivery id across a retry, a response treated as final,
a stale ingress credential survived, an egress secret unset. Nothing the kit can
reach over the adapter's egress endpoint provokes any of those, so the adapter's
author implements this Protocol once, in their own repo, and the kit drives it.

Without a driver those rules report ``not_run``, and ``not_run`` is
nonconformant. There is no shape of run in which missing evidence reads as a
pass.

**The correlation rule is the load bearing half.** ``reserve`` returns the
``delivery_id`` the adapter will put on the wire for a message that has not been
injected yet, and ``FakeIngress`` correlates an observed POST to that stimulus
by matching the body's ``delivery_id`` against it. Correlation is therefore a
property of the OBSERVED WIRE and never of private in process state, which is
the only form an adapter in another process or another language can satisfy. A
``reserve`` that returns an id the adapter never sends is itself a finding:
rules 1 and 2 fail naming the identity nothing matched, because reading a driver
that does not implement the contract as conformant would certify every broken
vendor driver along with it.

**Injection is two phase, and that is not a convenience.** Rule 2 has to arm the
ingress to answer 202 for ONE named identity before that identity can reach it.
A single call that both injects and declares leaves a window in which the
message is already in flight, so the kit would have to arm a global one shot
instead, and any other delivery arriving first would consume it. The rule then
passes without the declared delivery ever receiving the response whose finality
it is about. So ``reserve`` declares and ``release`` injects, in that order.

**Quiescence is declared, never assumed.** No finite observation establishes
that an adapter has stopped retrying, so ``settled`` is what retires a stimulus,
and a driver that cannot answer it leaves rule 2 with no finality evidence and
therefore nonpassing. A fixed wall clock window in its place would be a check
any retry schedule evades by outlasting it.
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

    def reserve(self) -> UpstreamIdentity:
        """Declare the id the NEXT released message will carry. Injects nothing.

        Called before ``release``, so the kit can arm its ingress for this exact
        identity while the message is still nowhere near the wire.
        """
        ...

    def release(self, identity: UpstreamIdentity) -> None:
        """Deliver the message ``reserve`` declared. Injects, declares nothing."""
        ...

    def settled(self, identity: UpstreamIdentity) -> bool:
        """Whether the adapter has retired this delivery and will not retry it.

        True once the adapter is done with the identity, whether it delivered it
        or gave up on it. Rule 2 asks whether a response was treated as final,
        and that question is only answerable once the adapter has stopped
        working: a verdict taken on a timer is a verdict any retry schedule
        evades by being slower than the timer. A driver that never reports an
        identity retired leaves the rule nonpassing rather than passing.

        **"Retired" means no attempt remains scheduled, not that the active
        queue is empty.** A delivery removed from the queue with a retry sitting
        on a timer is NOT retired, and reporting it as such is the naive
        implementation this kit exists to catch. The kit keeps watching after
        the claim and fails the rule, naming the harness, if a post for the
        identity arrives afterwards, so an over eager answer here produces a
        loud failure rather than a quiet pass.

        Like every method on this Protocol it is called under a hard timeout and
        must RETURN. Blocking, here or anywhere else in this interface, is
        reported as a harness defect: a callback that never answers would
        otherwise turn a verdict into a hang, which tells its reader nothing.
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

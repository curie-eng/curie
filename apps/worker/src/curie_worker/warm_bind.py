"""The warm-bind seam: which claims may adopt a pre-booted pool pod (ADR-0116 d2).

The kernel's cold path binds a sandbox at boot through per-claim env. A warm
bind instead claims an env-free pool pod whose runner booted in bootstrap mode
(ADR-0122) and adopts the conversation over the ACI ``Event``. Whether a given
claim MAY do that, and with which pool bootstrap credential, is a decision that
belongs to the version-scoped template and pool owner (#1492 decisions 1 and
3), not to the kernel. The kernel therefore asks this policy and does nothing
warm when it is absent: production wires ``None`` today, so every claim keeps
the cold path byte for byte.

``ADOPTION_UNCONFIRMED`` is the outcome classification for an adopting turn
whose response was lost after the adopting event left the worker. It is
deliberately NOT retryable: the runner may already have run the model for that
event, and a retry would replay the first turn's side effects. The kernel
escalates it to a human instead.
"""

from __future__ import annotations

from typing import Protocol

ADOPTION_UNCONFIRMED = "adoption-unconfirmed"


class AdoptionUnconfirmedError(RuntimeError):
    """A pending route's pod already holds the binding, but no turn ran here.

    Raised from the routing critical section when a delivery finds its warm
    route still PENDING while the pod attests the conversation credential: a
    previous owner of this delivery adopted the pod and was lost between the
    runner's 200 and the route write. The first turn may already have run on
    that pod, so the kernel converts this into the ``adoption-unconfirmed``
    outcome (escalated, never replayed) rather than into a retryable error.
    """

    def __init__(self, thread_key: str) -> None:
        super().__init__(f"thread {thread_key}: adoption applied by a lost owner; unconfirmed")


class WarmBindPolicy(Protocol):
    """Answer the pool bootstrap credential for a claim that may warm-bind."""

    def bootstrap_for(
        self, thread_key: str, *, boot_env: dict[str, str], agent_name: str | None
    ) -> str | None:
        """Return the pool's bootstrap credential, or ``None`` for the cold path.

        The credential is the pool's, never the conversation's: the kernel
        mints the per-conversation credential itself and records it on the
        route before any event. Return ``None`` whenever the pool that would
        serve this claim is not known to be booted in bootstrap mode.

        Two invariants the policy owner carries, because a warm claim injects
        NO env: only the session id, the history ref, and the conversation
        credential travel over the ACI. (1) The pool template that serves this
        claim must already carry everything ``boot_env`` would have injected
        for the cold path (the agent's bundle ref, plugin dir, budget, memory
        and state refs and tokens); a bootstrap returned for a pool whose
        template lacks the agent's bundle binds a runner to the wrong agent.
        (2) The pool must be image-homogeneous and the pod's runner process
        stable between the kernel's authority probe and its adopting event:
        the probe establishes authority on the process that answers it, and a
        mixed-image pool could pass the probe on one process and serve the
        event on an older one. The actual old-server negative is an activation
        gate for the pool owner, not something the kernel can infer.
        """
        ...

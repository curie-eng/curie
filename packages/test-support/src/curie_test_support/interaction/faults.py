"""The closed fault vocabulary, and how each one is armed and reverted.

Two rules govern everything in this module, and both exist because of a failure
mode that is invisible at the point it is caused:

* **The set is CLOSED.** An unknown name raises, naming :data:`SUPPORTED_FAULTS`.
  The alternative -- a silent no-op for a typo'd name -- lets a test arm nothing,
  drive the happy path, and report that it covered the failure case. That is a
  falsely claimed proof, so the typo has to be fatal.
* **Every fault reverts in a ``finally``.** A leaked fault does not fail the test
  that leaked it; it fails some later, unrelated test in the same process, with
  nothing in the report pointing back here.

Faults come in two shapes. Some are *patches* over a production callable
(``unittest.mock.patch`` objects, started on arm and stopped on revert); the rest
are *options* the harness consults at the moment it builds or drives something,
because the object they bend does not exist until the verb runs. Both live behind
one :class:`ArmedFault` so a caller cannot tell them apart -- and so neither can
leak by a different route than the other.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

__all__ = ["SUPPORTED_FAULTS", "ArmedFault", "arm_fault", "wrap_resolve_client"]

# The vocabulary, pinned here and asserted as an exact set by the contract
# tests. A name added without a case behind it is a capability nobody is
# watching; a name removed silently turns its case into a no-op.
SUPPORTED_FAULTS: tuple[str, ...] = (
    "resume_enqueue_lost",
    "resolve_transport_error",
    "private_metadata_unusable",
)

# How long a hung transport stays hung. Bounded on purpose: the point of
# ``hang=True`` is to outlive the VERB's deadline, not to outlive the suite. An
# unbounded sleep would leave a Bolt listener thread parked for the rest of the
# session, and the drain that follows the timed-out verb would then be the thing
# that hangs.
_HANG_SECONDS = 5.0

# Distinguishes "this attribute was absent" from "it was present and None". A
# plain ``None`` default would make an absent attribute indistinguishable from a
# real one, and restore would then invent one that was never there.
_MISSING: Any = object()


def _injected_transport_error() -> Exception:
    """The exception the broken transport raises.

    An ``httpx`` transport error specifically, not a bare ``Exception``: the
    dispatcher's resolve client catches ``httpx.HTTPError`` and turns it into
    ``ResolveOutcome(status_code=0)`` (approval_actions.py), which is the
    behavior the "resolve call itself fails" case is about. A foreign exception
    type would escape that handler and prove something else entirely -- that an
    unhandled error kills the Bolt listener.
    """

    import httpx

    return httpx.ConnectError("injected resolve transport failure")


@dataclass
class ArmedFault:
    """One armed fault: its patches, and the options verbs must consult."""

    name: str
    options: dict[str, Any] = field(default_factory=dict)
    _patches: list[Any] = field(default_factory=list)

    def revert(self) -> None:
        """Stop every patch, in reverse order, tolerating an already-stopped one.

        Reverse order because two patches over the same attribute must unwind as
        a stack; tolerant because ``revert`` runs from a ``finally`` that may be
        reached after an exception mid-arm, where some patches never started.
        """

        while self._patches:
            patcher = self._patches.pop()
            try:
                patcher.stop()
            except RuntimeError:  # pragma: no cover - already stopped
                pass


def arm_fault(name: str, **kwargs: Any) -> ArmedFault:
    """Arm one fault by name, returning the handle that reverts it.

    Raises ``ValueError`` naming :data:`SUPPORTED_FAULTS` for an unknown name.
    """

    if name not in SUPPORTED_FAULTS:
        raise ValueError(
            f"unknown fault {name!r}; supported faults are: {', '.join(SUPPORTED_FAULTS)}"
        )
    armed = ArmedFault(name=name, options=dict(kwargs))
    try:
        if name == "resume_enqueue_lost":
            _arm_resume_enqueue_lost(armed)
        # ``resolve_transport_error`` and ``private_metadata_unusable`` are
        # option-only: the transport belongs to a resolve client the harness
        # builds per ``act``, and the metadata is a value read off a modal that
        # has not opened yet, so there is nothing to patch until the verb runs.
    except Exception:
        armed.revert()
        raise
    return armed


class _OnRevert:
    """A ``stop()``-shaped callback, so non-patch cleanup unwinds with the rest.

    ``ArmedFault.revert`` knows only how to ``stop()`` things in reverse order.
    Anything that has to be undone alongside a patch is wrapped in one of these
    rather than being remembered somewhere else, because "somewhere else" is how
    a fault leaks past the ``finally`` that was supposed to clear it.
    """

    def __init__(self, undo: Any) -> None:
        self._undo = undo

    def stop(self) -> None:
        self._undo()


def _arm_resume_enqueue_lost(armed: ArmedFault) -> None:
    """Lose the API's resume enqueue while leaving the resolution itself real.

    Patched at ``ResumeQueue.enqueue`` rather than at the Valkey client: the
    reconciliation case under test is "the row is resolved but no turn was
    queued", and cutting Valkey would also break the reconciler that is supposed
    to heal it.

    It RAISES rather than returning a stream id. A silent success was the wrong
    shape and produced the wrong state: the resolve endpoint marks ``resumed_at``
    as soon as ``enqueue`` returns (approvals.py:420-433), so a fault that
    returned ``"0-0"`` left a fully-resumed row that the reconciler's NULL-gated
    finder correctly never selects -- the stranded shape the case is about was
    never created. A raise is also the real failure this backstop exists for (a
    Valkey blip), and it leaves ``resolved_at`` set with ``resumed_at`` NULL.

    ``RESUME_RECONCILER_ENABLED`` is forced on for the duration, for the same
    reason and from the same code path: with it off the route deliberately
    re-raises a failed enqueue as a 500 rather than stranding the session
    (approvals.py:415-433), so the 200-and-defer contract under test only exists
    when a backstop is declared. This flips the SETTING, which the route reads
    per request, not the LOOP -- ``journey_env`` keeps the live reconciler task
    out of the composition, so the only pass that ever runs is the one the case
    invokes by hand.
    """

    from curie_api.config import get_settings
    from curie_api.resumequeue import ResumeQueue

    async def _dropped(self: Any, turn: Any, *, parent: Any = None) -> str:
        import redis.exceptions

        raise redis.exceptions.ConnectionError("injected resume enqueue failure")

    # Appended FIRST so it unwinds LAST: the cache must be cleared after the
    # environment has been put back, not before.
    armed._patches.append(_OnRevert(get_settings.cache_clear))
    env_patcher = patch.dict(os.environ, {"RESUME_RECONCILER_ENABLED": "true"})
    env_patcher.start()
    armed._patches.append(env_patcher)
    get_settings.cache_clear()

    patcher = patch.object(ResumeQueue, "enqueue", _dropped)
    patcher.start()
    armed._patches.append(patcher)


def wrap_resolve_client(client: Any, faults: dict[str, ArmedFault]) -> None:
    """Apply the option-shaped transport fault to one built resolve client.

    Mutates the client's private ``httpx.Client`` in place rather than passing a
    ``client=`` override, because the production default transport is precisely
    what the composed journey exists to exercise: swapping the whole client for a
    fake would take the real ``_RESOLVE_TIMEOUT`` out of the picture along with
    the fault.
    """

    armed = faults.get("resolve_transport_error")
    if armed is None:
        return
    hang = bool(armed.options.get("hang", False))
    transport = client._client

    def _broken(*_args: Any, **_kwargs: Any) -> Any:
        if hang:
            time.sleep(_HANG_SECONDS)
        raise _injected_transport_error()

    # Remembered BEFORE the overwrite, and restored on revert. Today ``act`` and
    # ``resolve`` happen to build a fresh client per call, so an unrestored wrap
    # leaks nothing -- but that is an incidental property of the caller, not a
    # guarantee of this function, and the day a client is reused the leak is a
    # permanently broken transport on a shared object with nothing pointing here.
    # ``_MISSING`` distinguishes "had an instance attribute" from "inherited the
    # bound method off the class"; the two restore differently.
    # ``__dict__``, not ``getattr``: ``getattr`` resolves an inherited bound
    # method and cannot tell "shadowed by an instance attribute" from "found on
    # the class", so restoring what it returns would leave a shadowing instance
    # attribute where there was none. Only the instance dict answers the
    # question that restore actually has to undo.
    previous = {name: vars(transport).get(name, _MISSING) for name in ("get", "post")}

    def _restore() -> None:
        for name, original in previous.items():
            if original is _MISSING:
                # Only delete what we put there; another wrap may have already
                # restored it, and ``revert`` must tolerate running twice.
                transport.__dict__.pop(name, None)
            else:
                setattr(transport, name, original)

    armed._patches.append(_OnRevert(_restore))

    # Both verbs: the ownership probe GETs and the resolution POSTs, and a fault
    # that only broke one would let a click sail past the probe and then fail in
    # a place the case is not about.
    transport.get = _broken
    transport.post = _broken

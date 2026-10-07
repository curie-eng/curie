"""Resolve a nomination's event through its protected binding.

@spec AUTOMATED-REMEDIATION-6. The nomination route accepts only ``event_id``
and the block; everything else it records comes from what the platform wrote
when it admitted the delivery, never from the request:

* The protected binding ``protected:admission:binding:<event_id>`` on the
  protected broker is written only by the API's own atomic admission, under the
  enqueue principal, for a delivery the hook route authenticated. Its absence is
  ``not_protected_event``.
* The agent and hook are the ones the hook route built that event id from
  (``hook-<agent uuid>-<hook>-<16 hex>``, ``routers/hooks.py``). The binding's
  existence under exactly that id is what makes the parse trustworthy: an id the
  admission never wrote has no binding, and a binding's envelope must name the
  same event.
* The admitted remediation generation is the envelope's
  ``remediation_generation`` (AUTOMATED-REMEDIATION-4). The envelope schema does
  not carry it yet (task 5, the protected lane owner's change under #3603), so
  it is absent, which AUTOMATED-REMEDIATION-4 treats as "admitted before any
  policy existed": every nomination goes to approval, never to execution.

The broker read itself is the protected lane's closed enqueue transport
(``curie_protected_hooks.broker_transport.AuthenticatedEnqueueClient``), whose
surface exports no binding read today. Until it does, ``_read_binding`` fails
closed as unavailable, so the route refuses with ``503`` rather than guessing.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass
from typing import Final

from curie_protected_hooks.admission_records import parse_envelope

from .config import get_settings
from .protected_ingress import read_ingress_runtime
from .protected_runtime_files import IngressRuntime

# The hook route's event id: ``hook-{agent.id}-{hook}-{sha16(delivery_id)}``.
_EVENT_ID: Final = re.compile(
    r"hook-(?P<agent>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"-(?P<hook>[a-z0-9][a-z0-9._-]{0,62})-[0-9a-f]{16}"
)
BINDING_PREFIX: Final = "protected:admission:binding:"


class BindingUnavailable(Exception):
    """The protected broker or its runtime files could not answer. @spec AUTOMATED-REMEDIATION-6."""


@dataclass(frozen=True, slots=True)
class ProtectedEvent:
    """What the binding resolves for one event. @spec AUTOMATED-REMEDIATION-6."""

    event_id: str
    agent_id: uuid.UUID
    hook: str
    admitted_generation: int | None


def _read_binding(runtime: IngressRuntime, event_id: str) -> bytes | None:
    """The binding's envelope bytes, None when absent, else ``BindingUnavailable``.

    The enqueue transport (PROTECTED-HOOK-LANE-3) is a closed surface that does
    not yet export a binding read; adding one is the protected lane owner's
    change (#3603, remediation task 5). Until then nothing is read and the route
    fails closed. @spec AUTOMATED-REMEDIATION-6.
    """
    raise BindingUnavailable()


async def resolve_protected_event(event_id: str) -> ProtectedEvent | None:
    """The event's agent, hook and admitted generation, or None without a binding.

    @spec AUTOMATED-REMEDIATION-4 @spec AUTOMATED-REMEDIATION-6.
    """
    shape = _EVENT_ID.fullmatch(event_id)
    if shape is None:
        return None
    runtime = await read_ingress_runtime(get_settings().protected_runtime_dir)
    if runtime is None:
        raise BindingUnavailable()
    raw = await asyncio.to_thread(_read_binding, runtime, event_id)
    if raw is None:
        return None
    try:
        envelope = parse_envelope(raw).as_dict()
    except Exception:  # noqa: BLE001  A binding that does not parse is not evidence.
        raise BindingUnavailable() from None
    if envelope.get("event_id") != event_id:
        raise BindingUnavailable()
    return ProtectedEvent(
        event_id=event_id,
        agent_id=uuid.UUID(shape["agent"]),
        hook=shape["hook"],
        admitted_generation=None,
    )

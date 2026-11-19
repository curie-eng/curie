"""Response bodies of the declared webhook wake routes (ADR 0274).

The shapes are frozen by ``docs/interfaces/triggers/INTERFACE.md``, section
"Declared webhook wake contract (ADR 0274)": every field is required and no
other field is emitted, so each model forbids extras. A coalesced wake names no
run, including when a replay's original run has already settled, and the
rotation answer carries no secret; the key is read back with the GET.
"""

from __future__ import annotations

import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict


class HookWakeQueuedOut(BaseModel):
    """202: one webhook run was admitted and its turn enqueued."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["queued"]
    run_id: uuid.UUID


class HookWakeCoalescedOut(BaseModel):
    """200: the wake joined a live open run, or its delivery id was already reserved."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["coalesced"]


class HookWakeErrorOut(BaseModel):
    """Every wake and wake-key refusal: exactly one reason string."""

    model_config = ConfigDict(extra="forbid")

    detail: str


class HookWakeSecretOut(BaseModel):
    """The current wake key of one agent hook. Served with ``Cache-Control: no-store``."""

    model_config = ConfigDict(extra="forbid")

    agent_id: str
    hook: str
    generation: int
    secret: str


class HookWakeRotationOut(BaseModel):
    """The generation a rotation committed. Carries no key."""

    model_config = ConfigDict(extra="forbid")

    agent_id: str
    hook: str
    generation: int

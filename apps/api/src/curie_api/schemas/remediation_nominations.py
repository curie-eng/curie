"""Request and response bodies of the nomination submission route.

The request is closed: exactly the event id and the block the protected worker
withheld. The agent, hook, generations, thread and reply handle are resolved
from the protected binding, so a request naming any of them is refused ``422``
by ``extra="forbid"``. A refusal is ``{"detail": {"code": ...}}`` as on the
policy routes.

@spec AUTOMATED-REMEDIATION-6
"""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field

# Large enough that an over-size block is still recorded as malformed (its
# bound is 16 KiB of UTF-8, AUTOMATED-REMEDIATION-5); the protected turn limit.
BLOCK_MAX_CHARACTERS = 262144


class _NominationModel(BaseModel):
    """@spec AUTOMATED-REMEDIATION-6."""

    model_config = ConfigDict(strict=True, extra="forbid")


class RemediationNominationSubmit(_NominationModel):
    """One protected turn's nomination block. @spec AUTOMATED-REMEDIATION-6."""

    event_id: str = Field(
        min_length=1,
        max_length=256,
        description="The protected event whose turn produced the block.",
    )
    block: str = Field(
        max_length=BLOCK_MAX_CHARACTERS,
        description="The withheld block text, fences included, exactly as extracted.",
    )


class RemediationNominationAccepted(_NominationModel):
    """The event's accepted submission: its nominations in block order.

    @spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7.
    """

    event_id: str
    nomination_ids: list[uuid.UUID]


class RemediationNominationRefusalDetail(_NominationModel):
    """@spec AUTOMATED-REMEDIATION-6."""

    code: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")


class RemediationNominationRefusal(_NominationModel):
    """@spec AUTOMATED-REMEDIATION-6."""

    detail: RemediationNominationRefusalDetail

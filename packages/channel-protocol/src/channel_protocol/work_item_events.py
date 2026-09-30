"""The runs-stream event id grammar for work items.

Shared by the API producers (``workitem_reconciler``, ``factory_ci``) and the
worker parser so the two sides cannot drift (#3563).
"""

import re
import uuid
from typing import Literal, NamedTuple

#: Round 1 is the original execute turn; the first CI fix turn is round 2.
CI_FIRST_FIX_ROUND = 2
CI_MAX_ROUNDS = 3
CI_ROUND_KEY_PREFIX = "curie:work-item:ci:"

WorkItemEventKind = Literal["execute", "terminate", "ci"]

_UUID = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
# An event id is untrusted stream input: the number groups are capped at 18
# digits so oversized ids fail to match (Python's int() digit limit would raise)
# and the parser returns None, never raises.
_EVENT_ID = re.compile(
    rf"work-item-({_UUID})-"
    r"(?:execute-([1-9][0-9]{0,17})|ci-([1-9][0-9]{0,17})|terminate)"
)


class WorkItemEventId(NamedTuple):
    """The request, kind, and generation or CI round an event id names."""

    request_id: uuid.UUID
    kind: WorkItemEventKind
    number: int | None


def _is_ci_round(round_: int) -> bool:
    # Read at call time so the bound has exactly one definition.
    return CI_FIRST_FIX_ROUND <= round_ <= CI_MAX_ROUNDS


def ci_event_id(request_id: uuid.UUID, round_: int) -> str:
    if not _is_ci_round(round_):
        raise ValueError(
            f"CI round {round_} is outside {CI_FIRST_FIX_ROUND}..{CI_MAX_ROUNDS}"
        )
    return f"work-item-{request_id}-ci-{round_}"


def execute_event_id(request_id: uuid.UUID, generation: int) -> str:
    if generation < 1:
        raise ValueError(f"execute generation {generation} must be at least 1")
    return f"work-item-{request_id}-execute-{generation}"


def terminate_event_id(request_id: uuid.UUID) -> str:
    return f"work-item-{request_id}-terminate"


def ci_round_key(request_id: uuid.UUID, round_: int) -> str:
    """Build a round key; no upper bound because gate() looks ahead past the last round."""

    if round_ < CI_FIRST_FIX_ROUND:
        raise ValueError(f"CI round {round_} is below {CI_FIRST_FIX_ROUND}")
    return f"{CI_ROUND_KEY_PREFIX}{request_id}:{round_}"


def parse_work_item_event_id(event_id: str) -> WorkItemEventId | None:
    """Parse an event id, or return None for any other namespace or bad round."""

    matched = _EVENT_ID.fullmatch(event_id)
    if matched is None:
        return None
    request_id = uuid.UUID(matched.group(1))
    if matched.group(2) is not None:
        return WorkItemEventId(request_id, "execute", int(matched.group(2)))
    if matched.group(3) is not None:
        round_ = int(matched.group(3))
        if not _is_ci_round(round_):
            return None
        return WorkItemEventId(request_id, "ci", round_)
    return WorkItemEventId(request_id, "terminate", None)

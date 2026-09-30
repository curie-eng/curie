"""Export the committed Curie channel message JSON Schema."""

import json
from pathlib import Path
from typing import Any

from pydantic.json_schema import models_json_schema

from .models import (
    Action,
    ChannelCapabilities,
    ChoiceIntent,
    ConfirmIntent,
    MessageField,
    MessageLink,
    OutboundMessage,
)
from .progress import ProgressCard, ProgressCommand, ProgressMilestone
from .reply import (
    NavAffordance,
    ReplyAck,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
    TurnCompleted,
    TurnStatus,
)

SCHEMA_ID = "https://curietech.ai/schemas/channel-protocol.schema.json"
# Every model the drift gate (tests/test_schema_compat.py) can see. A model that
# is never listed here is invisible to it, so the committed schema would stay
# "current" while the wire it documents lost half its surface.
_MODELS = (
    ChannelCapabilities,
    OutboundMessage,
    ChoiceIntent,
    ConfirmIntent,
    Action,
    MessageField,
    MessageLink,
    ReplyTarget,
    NavAffordance,
    SettledOutcome,
    TurnStatus,
    ReplyUpdate,
    ReplyPost,
    TurnCompleted,
    ReplyAck,
    ProgressCard,
    ProgressMilestone,
    ProgressCommand,
)


def schema_path() -> Path:
    return Path(__file__).resolve().parents[2] / "schema" / "channel-protocol.schema.json"


def build_schema() -> dict[str, Any]:
    _, top = models_json_schema(
        [(model, "validation") for model in _MODELS],
        ref_template="#/$defs/{model}",
    )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "Curie Channel Protocol v1.0",
        "messageVersion": "1.0",
        **top,
    }


def render_schema() -> str:
    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"


def write_schema() -> Path:
    path = schema_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_schema(), encoding="utf-8")
    return path


if __name__ == "__main__":
    print(f"wrote {write_schema()}")

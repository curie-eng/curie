"""Export the ACI protocol models to a single canonical JSON Schema document.

The exported document is the committed cross-language contract: TypeScript and
Rust types are generated from it, and the compat gate fails the build if the
committed copy drifts from what these models produce. Output is canonicalized
(sorted keys, trailing newline) so regeneration is byte-identical.

Run as ``python -m aci_protocol.schema_export`` to rewrite the committed schema.
"""

import json
from pathlib import Path
from typing import Any

from pydantic import RootModel
from pydantic.json_schema import models_json_schema

from .events import (
    ErrorEvent,
    Event,
    Final,
    Interrupt,
    SideEffectFlag,
    TextDelta,
    ToolNote,
)
from .events import InboundMessage as InboundMessageUnion
from .events import OutboundEvent as OutboundEventUnion
from .session import BootEnv, Budget, OtelConfig, SessionConfig
from .turn import Attachment, QueuedTurn, ReplyHandle
from .version import PROTOCOL_VERSION, WIRE_VERSION_FIELD
from .wire import ApprovalRequest, EvalJob, EvalReport


# RootModel envelopes so the committed schema and generated TypeScript expose the
# whole-frame discriminated unions consumers actually validate against, not just
# the concrete variants. The class names become the $defs keys and TS type names.
class InboundMessage(RootModel[InboundMessageUnion]):
    pass


class OutboundEvent(RootModel[OutboundEventUnion]):
    pass


# Fixed model order so the generated $defs are deterministic.
_MODELS = (
    SessionConfig,
    BootEnv,
    Budget,
    OtelConfig,
    InboundMessage,
    OutboundEvent,
    Event,
    Interrupt,
    TextDelta,
    ToolNote,
    Final,
    ErrorEvent,
    SideEffectFlag,
    ReplyHandle,
    Attachment,
    QueuedTurn,
    EvalJob,
    EvalReport,
    ApprovalRequest,
)

SCHEMA_ID = "https://curietech.ai/schemas/aci-protocol.schema.json"


def schema_path() -> Path:
    """The committed schema file location inside this package."""

    return Path(__file__).resolve().parents[2] / "schema" / "aci-protocol.schema.json"


def _require_wire_mandatory_props(defs: dict[str, Any]) -> None:
    """Make mandatory wire tokens required and drop their misleading defaults.

    The discriminator and version fields carry a Python default for ergonomic
    construction, which pydantic renders as non-required with a default. On the
    wire they are mandatory (the NDJSON decoder rejects a missing version, and
    the union discriminator selects the variant), so they are marked required and
    their default removed.

    The discriminator fields are detected by their ``const`` (a single-valued
    Literal). The version field is no longer a Literal -- it is a semver-pattern
    string -- so it is special-cased **by name** (WIRE_VERSION_FIELD). Keying off
    the name rather than a ``const`` is deliberate: it is what keeps ``version``
    required-with-a-pattern once the Literal is gone, instead of silently
    becoming optional-with-a-default and lying to every consumer.
    """

    for schema in defs.values():
        props = schema.get("properties")
        if not isinstance(props, dict):
            continue
        required = set(schema.get("required", []))
        for name, prop in props.items():
            if not isinstance(prop, dict):
                continue
            if "const" in prop or name == WIRE_VERSION_FIELD:
                required.add(name)
                prop.pop("default", None)
        if required:
            schema["required"] = sorted(required)


def build_schema() -> dict[str, Any]:
    """Build the combined JSON Schema document for every ACI model."""

    _, top = models_json_schema(
        [(model, "validation") for model in _MODELS],
        ref_template="#/$defs/{model}",
    )
    if isinstance(top.get("$defs"), dict):
        _require_wire_mandatory_props(top["$defs"])
    doc: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": f"ACI Protocol v{PROTOCOL_VERSION}",
        "protocolVersion": PROTOCOL_VERSION,
    }
    doc.update(top)
    return doc


def render_schema() -> str:
    """Render the canonical schema string (sorted keys, trailing newline)."""

    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"


def write_schema() -> Path:
    """Write the canonical schema to its committed location and return the path."""

    path = schema_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_schema(), encoding="utf-8")
    return path


if __name__ == "__main__":
    written = write_schema()
    print(f"wrote {written}")

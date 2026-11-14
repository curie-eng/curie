"""Render the shared retry-condition vector as a generated TypeScript consumer.

``tests/vectors/aci-error-retry-condition.json`` is the cross-lane contract for
``ErrorEvent.retry_condition``. The Python decoder and the generated Rust crate
read it directly. TypeScript has no runtime decoder in this repository, so this
module renders the vector into ``generated/ts/retry-condition-vectors.ts``,
which the Contracts job compiles with ``tsc --noEmit`` against the generated
``ErrorEvent`` type:

1. Every ``accept`` frame is typed as an ``ErrorEvent``, so a generated type
   that stopped accepting one fails compilation.
2. Every ``refuse`` case whose ``violation`` is ``type`` is rendered under a
   ``@ts-expect-error`` comment. An unused ``@ts-expect-error`` is itself a tsc
   error, so a generated type that started accepting one fails compilation.
3. ``grammar`` violations are well-typed strings that TypeScript string types
   cannot refuse. They are listed by name in a closing comment so the file
   states that boundary instead of implying a check it does not make.

The frames are written as ``{ version, ...members }`` object literals typed as
``ErrorEvent`` directly. ``Omit<ErrorEvent, "version">`` is deliberately not
used: the generated interfaces carry a ``[k: string]: unknown`` index
signature, and ``Omit`` over an index signature keeps only the index signature,
which would make every check vacuous.

The loader here is shared with the Python tests so every reader of the vector
refuses the same malformed shapes: an unknown or missing key at the top level
or in a case fails closed rather than passing vacuously.

Run as ``python -m aci_protocol.ts_vector_export`` to rewrite the committed
TypeScript file; ``scripts/check-contracts.sh`` runs it and checks for drift.
"""

import json
import re
from pathlib import Path
from typing import Any

from .version import PROTOCOL_VERSION

_VECTOR_NAME = "aci-error-retry-condition.json"
_TS_NAME = "retry-condition-vectors.ts"

_TOP_LEVEL_KEYS = frozenset({"comment", "accept", "refuse"})
_ACCEPT_REQUIRED_KEYS = frozenset({"name", "frame", "expected_retry_condition"})
_ACCEPT_OPTIONAL_KEYS = frozenset({"strict_construction"})
_REFUSE_KEYS = frozenset({"name", "frame", "violation"})
_NO_KEYS: frozenset[str] = frozenset()
_FRAME_REQUIRED_KEYS = frozenset({"type", "message"})
_FRAME_OPTIONAL_KEYS = frozenset({"classification", "retry_condition"})
_STRICT_CONSTRUCTION_VALUES = frozenset({"refuse"})
_VIOLATIONS = frozenset({"type", "grammar"})
# Lower snake case, so a name is also a valid TypeScript identifier suffix.
_CASE_NAME = re.compile(r"[a-z][a-z0-9]*(_[a-z0-9]+)*")


class RetryConditionVectorError(ValueError):
    """Raised when the shared retry-condition vector has an unexpected shape."""


def vector_path() -> Path:
    """The shared cross-lane vector file at the repository root."""

    return Path(__file__).resolve().parents[4] / "tests" / "vectors" / _VECTOR_NAME


def ts_path() -> Path:
    """The committed generated TypeScript vector file inside this package."""

    return Path(__file__).resolve().parents[2] / "generated" / "ts" / _TS_NAME


def _check_keys(
    where: str, entry: Any, required: frozenset[str], optional: frozenset[str]
) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise RetryConditionVectorError(f"{where} must be a JSON object")
    unknown = entry.keys() - required - optional
    if unknown:
        raise RetryConditionVectorError(f"{where} has unknown key(s) {sorted(unknown)}")
    missing = required - entry.keys()
    if missing:
        raise RetryConditionVectorError(f"{where} is missing key(s) {sorted(missing)}")
    return entry


def _check_frame(where: str, frame: Any) -> None:
    # Frames omit ``version`` so each reader stamps its own PROTOCOL_VERSION,
    # keeping the version gate the only version check. Every frame is an error
    # frame, so a refusal is attributable to the retry condition alone.
    checked = _check_keys(f"{where} frame", frame, _FRAME_REQUIRED_KEYS, _FRAME_OPTIONAL_KEYS)
    if checked["type"] != "error" or not isinstance(checked["message"], str):
        raise RetryConditionVectorError(f"{where} frame must be an error frame with a message")


def load_retry_condition_vector(path: Path | None = None) -> dict[str, Any]:
    """Load the shared retry-condition vector, failing closed on any unexpected shape.

    Refuses an unknown or missing key at the top level, in a case, or in a
    frame; a case name that is not lower snake case or is used twice; a
    ``strict_construction`` value other than ``refuse``; and a ``violation``
    other than ``type`` or ``grammar``. Reads ``vector_path()`` unless a path is
    given.
    """

    source = vector_path() if path is None else path
    document = _check_keys(
        "the vector", json.loads(source.read_text(encoding="utf-8")), _TOP_LEVEL_KEYS, _NO_KEYS
    )
    if not isinstance(document["comment"], str):
        raise RetryConditionVectorError("the vector comment must be a string")
    names: set[str] = set()
    for list_name, required, optional in (
        ("accept", _ACCEPT_REQUIRED_KEYS, _ACCEPT_OPTIONAL_KEYS),
        ("refuse", _REFUSE_KEYS, _NO_KEYS),
    ):
        cases = document[list_name]
        if not isinstance(cases, list) or not cases:
            raise RetryConditionVectorError(f"the {list_name} list must be a nonempty array")
        for index, raw_case in enumerate(cases):
            case = _check_keys(f"{list_name}[{index}]", raw_case, required, optional)
            name = case["name"]
            if not isinstance(name, str) or not _CASE_NAME.fullmatch(name):
                raise RetryConditionVectorError(
                    f"{list_name}[{index}] name must be lower snake case"
                )
            if name in names:
                raise RetryConditionVectorError(f"case name {name} is used twice")
            names.add(name)
            _check_frame(f"{list_name} case {name}", case["frame"])
            if (
                "strict_construction" in case
                and case["strict_construction"] not in _STRICT_CONSTRUCTION_VALUES
            ):
                raise RetryConditionVectorError(
                    f"accept case {name} strict_construction must be refuse"
                )
            if "violation" in case and case["violation"] not in _VIOLATIONS:
                raise RetryConditionVectorError(
                    f"refuse case {name} violation must be type or grammar"
                )
    return document


def _ts_frame(frame: dict[str, Any]) -> str:
    # JSON members are valid TypeScript object literal members, so each frame
    # member is rendered as JSON after the shorthand ``version`` member.
    members = [f"{json.dumps(key)}: {json.dumps(value)}" for key, value in frame.items()]
    return "{ " + ", ".join(["version", *members]) + " }"


def render_ts_vectors(path: Path | None = None) -> str:
    """Render the generated TypeScript vector file as a deterministic string."""

    document = load_retry_condition_vector(path)
    lines = [
        f"// GENERATED by aci_protocol.ts_vector_export from tests/vectors/{_VECTOR_NAME}.",
        "// Regenerate with: python -m aci_protocol.ts_vector_export",
        'import type { ErrorEvent } from "./aci-protocol";',
        "",
        f'const version = "{PROTOCOL_VERSION}";',
        "",
        "// Every accept frame of the shared vector must type check as an ErrorEvent.",
        "export const acceptedRetryConditionFrames: ErrorEvent[] = [",
    ]
    lines.extend(f"  {_ts_frame(case['frame'])}," for case in document["accept"])
    lines.append("];")
    for case in document["refuse"]:
        if case["violation"] != "type":
            continue
        lines.extend(
            [
                "",
                f"// @ts-expect-error {case['name']}",
                f"export const refused_{case['name']}: ErrorEvent = {_ts_frame(case['frame'])};",
            ]
        )
    grammar = [case["name"] for case in document["refuse"] if case["violation"] == "grammar"]
    lines.extend(
        [
            "",
            "// Grammar violations are well-typed strings that TypeScript string types cannot",
            "// refuse. The Python and Rust decoders refuse each of these shared vector cases:",
            *(f"//   {name}" for name in grammar),
        ]
    )
    return "\n".join(lines) + "\n"


def write_ts_vectors() -> Path:
    """Write the generated TypeScript vector file and return its path."""

    path = ts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_ts_vectors(), encoding="utf-8")
    return path


if __name__ == "__main__":
    written = write_ts_vectors()
    print(f"wrote {written}")

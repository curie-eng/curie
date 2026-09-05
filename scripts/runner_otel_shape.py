"""Validate the canonical runner phase tree in raw OTLP JSON.

This module deliberately depends only on the committed runner schema.  The
local ladder executes it with the system Python, where the runner package and
its OpenTelemetry dependencies are not necessarily installed.

Diagnostics name only fixed span names and bounded attribute keys.  They never
include trace/span identifiers or attribute values, because this validator is
also used on the failure path of the redaction proof.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from pathlib import Path

_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "runner"
    / "schema"
    / "otel-attributes.schema.json"
)
_MAX_VIOLATIONS = 64
_MAX_SAFE_ATTRIBUTE_KEY_LENGTH = 128
_SAFE_ATTRIBUTE_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_UINT64_MAX = 2**64 - 1
_INVALID = object()

_GENERATION_START_KINDS = frozenset(("query_observed", "tool_result_inferred"))
_GENERATION_END_KINDS = frozenset(
    ("tool_use_observed", "result_observed", "terminal_inferred")
)
_TOOL_START_KINDS = frozenset(("tool_use_inferred",))
_TOOL_END_KINDS = frozenset(("tool_result_inferred", "terminal_inferred"))
_TOOL_OUTCOMES = frozenset(("success", "error", "cancelled"))
_ROOT_PHASES = frozenset(("provider_wait", "tool_wait"))
_OTEL_STATUS_OK = 1
_OTEL_STATUS_ERROR = 2
_TERMINAL_OUTCOMES = {
    ("interrupt_requested", "cancelled"): _OTEL_STATUS_OK,
    ("approval_required", "paused"): _OTEL_STATUS_OK,
    ("aborted_streaming", "failed"): _OTEL_STATUS_ERROR,
    ("aborted_tools", "failed"): _OTEL_STATUS_ERROR,
    ("classified_failure", "failed"): _OTEL_STATUS_ERROR,
    ("runner_timeout", "failed"): _OTEL_STATUS_ERROR,
    ("completed", "succeeded"): _OTEL_STATUS_OK,
    ("abandoned", "abandoned"): _OTEL_STATUS_ERROR,
}


def _committed_attribute_types() -> dict[str, str]:
    document = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    keys = document.get("keys")
    if not isinstance(keys, dict) or not all(
        isinstance(key, str) and value in ("int", "str")
        for key, value in keys.items()
    ):
        raise RuntimeError("runner OTel attribute schema has an invalid shape")
    return dict(keys)


_ATTRIBUTE_TYPES = _committed_attribute_types()


class _Violations:
    """Collect a deterministic, deduplicated, bounded diagnostic set."""

    def __init__(self) -> None:
        self.items: list[str] = []
        self._seen: set[str] = set()
        self._overflowed = False

    def add(self, message: str) -> None:
        if message in self._seen:
            return
        self._seen.add(message)
        if len(self.items) < _MAX_VIOLATIONS - 1:
            self.items.append(message)
        elif not self._overflowed:
            self.items.append("additional runner shape violations omitted")
            self._overflowed = True


def _bounded_count(value: int) -> str:
    return str(value) if value <= 9_999 else "many"


def _safe_key(key: object) -> str | None:
    if not isinstance(key, str) or len(key) > _MAX_SAFE_ATTRIBUTE_KEY_LENGTH:
        return None
    return key if _SAFE_ATTRIBUTE_KEY.fullmatch(key) else None


def _parse_decimal(value: object, *, signed: bool) -> int | object:
    """Parse an OTLP JSON integer without accepting bools, floats, or huge text."""

    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and 1 <= len(value) <= 20:
        digits = value
        if signed and digits[:1] in ("+", "-"):
            digits = digits[1:]
        if not digits or not digits.isascii() or not digits.isdigit():
            return _INVALID
        try:
            parsed = int(value, 10)
        except ValueError:
            return _INVALID
    else:
        return _INVALID

    lower = _INT64_MIN if signed else 0
    upper = _INT64_MAX if signed else _UINT64_MAX
    return parsed if lower <= parsed <= upper else _INVALID


def _raw_scalar(value: object, expected_type: str) -> object:
    """Return one schema-typed scalar from an OTLP AnyValue JSON object."""

    if not isinstance(value, Mapping):
        if expected_type == "int":
            return _parse_decimal(value, signed=True)
        return value if isinstance(value, str) else _INVALID

    if expected_type == "int":
        if set(value) != {"intValue"}:
            return _INVALID
        return _parse_decimal(value.get("intValue"), signed=True)
    if set(value) != {"stringValue"}:
        return _INVALID
    scalar = value.get("stringValue")
    return scalar if isinstance(scalar, str) else _INVALID


def _attribute_map(
    raw_attributes: object,
    violations: _Violations,
    *,
    validate_schema: bool,
) -> dict[str, object]:
    """Normalize an attribute collection while retaining no rejected values."""

    if raw_attributes is None:
        return {}

    # Resource attributes have already been normalized by the ladder query,
    # while span attributes remain raw OTLP JSON.  Supporting both shapes keeps
    # the validator independently usable without changing its trust boundary.
    if isinstance(raw_attributes, Mapping):
        entries: Iterable[tuple[object, object]] = raw_attributes.items()
    elif isinstance(raw_attributes, list):
        normalized_entries: list[tuple[object, object]] = []
        for item in raw_attributes:
            if not isinstance(item, Mapping):
                violations.add("runner attribute entry has an invalid shape")
                continue
            normalized_entries.append((item.get("key"), item.get("value")))
        entries = normalized_entries
    else:
        violations.add("runner attributes have an invalid shape")
        return {}

    result: dict[str, object] = {}
    seen_keys: set[str] = set()
    for raw_key, raw_value in entries:
        key = _safe_key(raw_key)
        if key is None:
            violations.add("runner attribute has an invalid key")
            continue
        if key in seen_keys:
            violations.add(f"duplicate runner attribute {key}")
            continue
        seen_keys.add(key)

        expected_type = _ATTRIBUTE_TYPES.get(key)
        if validate_schema and expected_type is None:
            violations.add(f"forbidden attribute {key}")
            continue

        if expected_type is None:
            # Only resource service.name is read by this helper.  Other standard
            # resource attributes are intentionally outside the runner's closed
            # *span* schema and are not copied into the result.
            if key == "service.name":
                scalar = _raw_scalar(raw_value, "str")
                if scalar is not _INVALID:
                    result[key] = scalar
            continue

        scalar = _raw_scalar(raw_value, expected_type)
        if scalar is _INVALID:
            if validate_schema:
                violations.add(f"runner attribute {key} has the wrong scalar type")
            continue
        result[key] = scalar
    return result


def _resource_service_name(resource: object) -> object:
    if not isinstance(resource, Mapping):
        return _INVALID
    raw_attributes = resource.get("attributes", resource)
    scratch = _Violations()
    return _attribute_map(
        raw_attributes,
        scratch,
        validate_schema=False,
    ).get("service.name", _INVALID)


def _positive_duration(span: Mapping[str, object]) -> bool | None:
    start = _parse_decimal(span.get("startTimeUnixNano"), signed=False)
    end = _parse_decimal(span.get("endTimeUnixNano"), signed=False)
    if type(start) is not int or type(end) is not int:
        return None
    return bool(end > start)


def _is_root_sibling(span: Mapping[str, object], root_span_id: object) -> bool:
    parent_span_id = span.get("parentSpanId")
    return (
        isinstance(root_span_id, str)
        and bool(root_span_id)
        and isinstance(parent_span_id, str)
        and parent_span_id == root_span_id
    )


def _otel_status_code(span: Mapping[str, object]) -> int | object:
    """Return a closed OK/ERROR OTLP status code without retaining its payload."""

    status = span.get("status")
    if not isinstance(status, Mapping):
        return _INVALID
    raw_code = status.get("code")
    if raw_code == "STATUS_CODE_OK":
        return _OTEL_STATUS_OK
    if raw_code == "STATUS_CODE_ERROR":
        return _OTEL_STATUS_ERROR
    code = _parse_decimal(raw_code, signed=False)
    return code if code in (_OTEL_STATUS_OK, _OTEL_STATUS_ERROR) else _INVALID


def _check_sequential_indices(
    spans: list[tuple[Mapping[str, object], dict[str, object]]],
    key: str,
    violations: _Violations,
) -> None:
    indices: list[int] = []
    invalid = False
    for _, attributes in spans:
        index = attributes.get(key, _INVALID)
        if type(index) is not int or index <= 0 or index > len(spans):
            violations.add(f"{key} must be a positive bounded integer")
            invalid = True
        else:
            indices.append(index)
    if not invalid and sorted(indices) != list(range(1, len(spans) + 1)):
        violations.add(f"{key} values must be sequential from 1")


def canonical_runner_shape_violations(
    trace_spans: Iterable[tuple[Mapping[str, object], Mapping[str, object]]],
    *,
    require_multi_round_tool: bool = False,
) -> list[str]:
    """Return privacy-safe violations of the canonical runner phase tree.

    ``trace_spans`` is one candidate trace represented as ``(span, resource)``
    tuples from OTLP JSON.  The input is never mutated and no attribute value or
    identifier is retained in a returned diagnostic.
    """

    violations = _Violations()
    runner_spans: list[tuple[Mapping[str, object], dict[str, object]]] = []

    try:
        entries = list(trace_spans)
    except (TypeError, ValueError):
        return ["runner trace span collection has an invalid shape"]

    for entry in entries:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            violations.add("runner trace span entry has an invalid shape")
            continue
        span, resource = entry
        if _resource_service_name(resource) != "curie-runner":
            continue
        if not isinstance(span, Mapping):
            violations.add("curie-runner span has an invalid shape")
            continue
        attributes = _attribute_map(
            span.get("attributes"),
            violations,
            validate_schema=True,
        )
        runner_spans.append((span, attributes))

    roots = [item for item in runner_spans if item[0].get("name") == "agent.run"]
    generations = [
        item for item in runner_spans if item[0].get("name") == "llm.generation"
    ]
    tools = [item for item in runner_spans if item[0].get("name") == "execute_tool"]

    if len(roots) != 1:
        violations.add(
            f"agent.run expected 1; found {_bounded_count(len(roots))}"
        )
    if not generations:
        violations.add("llm.generation expected 1 or more; found 0")
    elif require_multi_round_tool and len(generations) < 2:
        violations.add(
            "llm.generation expected 2 or more; "
            f"found {_bounded_count(len(generations))}"
        )
    if require_multi_round_tool and not tools:
        violations.add("execute_tool expected 1 or more; found 0")

    root_span_id: object = roots[0][0].get("spanId") if len(roots) == 1 else _INVALID

    if len(roots) == 1:
        root_span, root_attributes = roots[0]
        if root_attributes.get("curie.phase") not in _ROOT_PHASES:
            violations.add("agent.run curie.phase is not closed")

        terminal_cause = root_attributes.get("curie.terminal.cause")
        terminal_status = root_attributes.get("curie.terminal.status")
        expected_otel_status = (
            _TERMINAL_OUTCOMES.get((terminal_cause, terminal_status))
            if isinstance(terminal_cause, str) and isinstance(terminal_status, str)
            else None
        )
        if expected_otel_status is None:
            violations.add("agent.run terminal cause/status combination is not closed")

        otel_status = _otel_status_code(root_span)
        if otel_status is _INVALID:
            violations.add("agent.run OTel status is missing, malformed, or not closed")
        elif expected_otel_status is not None and otel_status != expected_otel_status:
            violations.add("agent.run OTel status does not match terminal cause/status")

    for span, attributes in generations:
        if not _is_root_sibling(span, root_span_id):
            violations.add("llm.generation must be an agent.run root sibling")
        if attributes.get("curie.phase") != "provider_wait":
            violations.add("llm.generation curie.phase must be provider_wait")
        if attributes.get("curie.phase.start_kind") not in _GENERATION_START_KINDS:
            violations.add(
                "llm.generation curie.phase.start_kind is not a closed boundary"
            )
        if attributes.get("curie.phase.end_kind") not in _GENERATION_END_KINDS:
            violations.add(
                "llm.generation curie.phase.end_kind is not a closed boundary"
            )

    for span, attributes in tools:
        if not _is_root_sibling(span, root_span_id):
            violations.add("execute_tool must be an agent.run root sibling")
        duration = _positive_duration(span)
        if duration is None:
            violations.add("execute_tool duration is missing or malformed")
        elif not duration:
            violations.add(
                "execute_tool must have positive duration; zero duration is forbidden"
            )
        if attributes.get("curie.phase") != "tool_wait":
            violations.add("execute_tool curie.phase must be tool_wait")
        if attributes.get("curie.phase.start_kind") not in _TOOL_START_KINDS:
            violations.add(
                "execute_tool curie.phase.start_kind is not an inferred boundary"
            )
        if attributes.get("curie.phase.end_kind") not in _TOOL_END_KINDS:
            violations.add(
                "execute_tool curie.phase.end_kind is not an inferred or terminal boundary"
            )
        if attributes.get("curie.tool.outcome") not in _TOOL_OUTCOMES:
            violations.add("execute_tool curie.tool.outcome is not closed")

    _check_sequential_indices(
        generations,
        "curie.generation.round",
        violations,
    )
    _check_sequential_indices(
        tools,
        "curie.tool.call.index",
        violations,
    )
    return violations.items

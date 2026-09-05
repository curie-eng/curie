"""Executable contract for the local-ladder runner OTel shape validator."""

from __future__ import annotations

import importlib.util
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType

import pytest

_HELPER_PATH = Path(__file__).parents[2] / "scripts" / "runner_otel_shape.py"
_MODULE_NAME = "curie_runner_otel_shape"
_TRACE_ID = "00000000000000000000000000000001"
_ROOT_SPAN_ID = "0000000000000001"
_GENERATION_ONE_SPAN_ID = "0000000000000002"
_TOOL_SPAN_ID = "0000000000000003"
_GENERATION_TWO_SPAN_ID = "0000000000000004"
_RUNNER_RESOURCE: dict[str, object] = {
    "service.name": "curie-runner",
    "schema.version": "v1",
}
_TERMINAL_OUTCOMES = (
    ("completed", "succeeded", 1),
    ("interrupt_requested", "cancelled", 1),
    ("approval_required", "paused", 1),
    ("aborted_streaming", "failed", 2),
    ("aborted_tools", "failed", 2),
    ("classified_failure", "failed", 2),
    ("runner_timeout", "failed", 2),
    ("abandoned", "abandoned", 2),
)


def _load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _HELPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import runner OTel shape helper at {_HELPER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


canonical_runner_shape_violations = _load_helper().canonical_runner_shape_violations


def _otlp_value(value: str | int) -> dict[str, object]:
    if type(value) is int:
        # OTLP's JSON mapping encodes int64 values as decimal strings.
        return {"intValue": str(value)}
    return {"stringValue": value}


def _otlp_attributes(attributes: dict[str, str | int]) -> list[dict[str, object]]:
    return [
        {"key": key, "value": _otlp_value(value)}
        for key, value in attributes.items()
    ]


def _span(
    name: str,
    span_id: str,
    *,
    start_ns: int,
    end_ns: int,
    attributes: dict[str, str | int],
    parent_span_id: str | None = _ROOT_SPAN_ID,
) -> dict[str, object]:
    span: dict[str, object] = {
        "traceId": _TRACE_ID,
        "spanId": span_id,
        "name": name,
        "kind": 1,
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(end_ns),
        "attributes": _otlp_attributes(attributes),
        "status": {"code": 1},
    }
    if parent_span_id is not None:
        span["parentSpanId"] = parent_span_id
    return span


def _canonical_trace_spans() -> list[tuple[dict[str, object], dict[str, object]]]:
    root = _span(
        "agent.run",
        _ROOT_SPAN_ID,
        start_ns=1_000_000_000,
        end_ns=1_100_000_000,
        parent_span_id=None,
        attributes={
            "curie.phase": "provider_wait",
            "curie.terminal.cause": "completed",
            "curie.terminal.status": "succeeded",
        },
    )
    first_generation = _span(
        "llm.generation",
        _GENERATION_ONE_SPAN_ID,
        start_ns=1_001_000_000,
        end_ns=1_020_000_000,
        attributes={
            "curie.phase": "provider_wait",
            "curie.phase.start_kind": "query_observed",
            "curie.phase.end_kind": "tool_use_observed",
            "curie.generation.round": 1,
            "curie.generation.ttft_ms": 7,
            "gen_ai.request.model": "acme-model",
            "model": "acme-model",
        },
    )
    tool = _span(
        "execute_tool",
        _TOOL_SPAN_ID,
        start_ns=1_020_000_000,
        end_ns=1_040_000_000,
        attributes={
            "curie.phase": "tool_wait",
            "curie.phase.start_kind": "tool_use_inferred",
            "curie.phase.end_kind": "tool_result_inferred",
            "curie.tool.call.index": 1,
            "curie.tool.outcome": "success",
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "Read",
        },
    )
    second_generation = _span(
        "llm.generation",
        _GENERATION_TWO_SPAN_ID,
        start_ns=1_040_000_000,
        end_ns=1_099_000_000,
        attributes={
            "curie.phase": "provider_wait",
            "curie.phase.start_kind": "tool_result_inferred",
            "curie.phase.end_kind": "result_observed",
            "curie.generation.round": 2,
            "curie.generation.ttft_ms": 11,
            "gen_ai.request.model": "acme-model",
            "model": "acme-model",
        },
    )
    return [
        (span, dict(_RUNNER_RESOURCE))
        for span in (root, first_generation, tool, second_generation)
    ]


def _single_round_tool_free_trace_spans() -> list[
    tuple[dict[str, object], dict[str, object]]
]:
    root = _span(
        "agent.run",
        _ROOT_SPAN_ID,
        start_ns=1_000_000_000,
        end_ns=1_100_000_000,
        parent_span_id=None,
        attributes={
            "curie.phase": "provider_wait",
            "curie.terminal.cause": "completed",
            "curie.terminal.status": "succeeded",
        },
    )
    generation = _span(
        "llm.generation",
        _GENERATION_ONE_SPAN_ID,
        start_ns=1_001_000_000,
        end_ns=1_099_000_000,
        attributes={
            "curie.phase": "provider_wait",
            "curie.phase.start_kind": "query_observed",
            "curie.phase.end_kind": "result_observed",
            "curie.generation.round": 1,
            "curie.generation.ttft_ms": 7,
            "gen_ai.request.model": "acme-model",
            "model": "acme-model",
        },
    )
    return [(span, dict(_RUNNER_RESOURCE)) for span in (root, generation)]


def _legacy_trace_spans() -> list[tuple[dict[str, object], dict[str, object]]]:
    root = _span(
        "agent.run",
        _ROOT_SPAN_ID,
        start_ns=1_000_000_000,
        end_ns=1_100_000_000,
        parent_span_id=None,
        attributes={
            "curie.phase": "provider_wait",
            "curie.terminal.cause": "completed",
            "curie.terminal.status": "succeeded",
        },
    )
    monolithic_generation = _span(
        "llm.generation",
        _GENERATION_ONE_SPAN_ID,
        start_ns=1_001_000_000,
        end_ns=1_099_000_000,
        attributes={
            "curie.phase": "provider_wait",
            "curie.phase.start_kind": "query_observed",
            "curie.phase.end_kind": "result_observed",
            "curie.generation.round": 1,
            "curie.generation.ttft_ms": 7,
        },
    )
    nested_zero_duration_tool = _span(
        "execute_tool",
        _TOOL_SPAN_ID,
        start_ns=1_020_000_000,
        end_ns=1_020_000_000,
        parent_span_id=_GENERATION_ONE_SPAN_ID,
        attributes={
            "curie.phase": "tool_wait",
            "curie.phase.start_kind": "tool_use_inferred",
            "curie.phase.end_kind": "tool_result_inferred",
            "curie.tool.call.index": 1,
            "curie.tool.outcome": "success",
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "Read",
        },
    )
    return [
        (span, dict(_RUNNER_RESOURCE))
        for span in (root, monolithic_generation, nested_zero_duration_tool)
    ]


def _set_attribute(span: dict[str, object], key: str, value: str | int) -> None:
    attributes = span["attributes"]
    assert isinstance(attributes, list)
    for attribute in attributes:
        assert isinstance(attribute, dict)
        if attribute.get("key") == key:
            attribute["value"] = _otlp_value(value)
            return
    attributes.append({"key": key, "value": _otlp_value(value)})


def _set_otel_status(span: dict[str, object], code: int) -> None:
    status = span["status"]
    assert isinstance(status, dict)
    status["code"] = code


def _mentions(violations: list[str], *fragments: str) -> bool:
    wanted = tuple(fragment.casefold() for fragment in fragments)
    return any(
        all(fragment in violation.casefold() for fragment in wanted)
        for violation in violations
    )


def test_canonical_runner_otlp_json_shape_has_no_violations() -> None:
    trace_spans = _canonical_trace_spans()

    assert canonical_runner_shape_violations(trace_spans) == []
    assert canonical_runner_shape_violations(
        trace_spans,
        require_multi_round_tool=True,
    ) == []


@pytest.mark.parametrize(
    ("cause", "terminal_status", "otel_status"),
    _TERMINAL_OUTCOMES,
)
def test_root_terminal_closed_mapping_is_accepted(
    cause: str,
    terminal_status: str,
    otel_status: int,
) -> None:
    trace_spans = deepcopy(_canonical_trace_spans())
    root = trace_spans[0][0]
    _set_attribute(root, "curie.terminal.cause", cause)
    _set_attribute(root, "curie.terminal.status", terminal_status)
    _set_otel_status(root, otel_status)

    assert canonical_runner_shape_violations(trace_spans) == []


@pytest.mark.parametrize(
    ("cause", "wrong_terminal_status"),
    (
        ("completed", "cancelled"),
        ("interrupt_requested", "succeeded"),
        ("approval_required", "cancelled"),
        ("aborted_streaming", "abandoned"),
        ("aborted_tools", "succeeded"),
        ("classified_failure", "paused"),
        ("runner_timeout", "abandoned"),
        ("abandoned", "failed"),
        ("unknown_terminal", "succeeded"),
        ("completed", "unknown_status"),
    ),
)
def test_root_terminal_cause_and_status_must_be_a_closed_pair(
    cause: str,
    wrong_terminal_status: str,
) -> None:
    trace_spans = deepcopy(_canonical_trace_spans())
    root = trace_spans[0][0]
    _set_attribute(root, "curie.terminal.cause", cause)
    _set_attribute(root, "curie.terminal.status", wrong_terminal_status)

    violations = canonical_runner_shape_violations(trace_spans)

    assert _mentions(violations, "agent.run", "terminal", "cause/status", "not closed")


@pytest.mark.parametrize(
    ("cause", "terminal_status", "correct_otel_status"),
    _TERMINAL_OUTCOMES,
)
def test_root_otel_status_must_match_the_terminal_mapping(
    cause: str,
    terminal_status: str,
    correct_otel_status: int,
) -> None:
    trace_spans = deepcopy(_canonical_trace_spans())
    root = trace_spans[0][0]
    _set_attribute(root, "curie.terminal.cause", cause)
    _set_attribute(root, "curie.terminal.status", terminal_status)
    _set_otel_status(root, 2 if correct_otel_status == 1 else 1)

    violations = canonical_runner_shape_violations(trace_spans)

    assert _mentions(violations, "agent.run", "OTel status", "does not match")


def test_single_round_tool_free_trace_is_valid_by_default_and_strictly_rejected() -> None:
    trace_spans = _single_round_tool_free_trace_spans()

    assert canonical_runner_shape_violations(trace_spans) == []

    strict_violations = canonical_runner_shape_violations(
        trace_spans,
        require_multi_round_tool=True,
    )
    assert _mentions(strict_violations, "llm.generation", "expected 2", "found 1")
    assert _mentions(strict_violations, "execute_tool", "expected 1", "found 0")


def test_legacy_monolithic_generation_and_nested_zero_duration_tool_are_rejected() -> None:
    violations = canonical_runner_shape_violations(_legacy_trace_spans())

    assert _mentions(violations, "execute_tool", "root sibling")
    assert _mentions(violations, "execute_tool", "zero duration")

    strict_violations = canonical_runner_shape_violations(
        _legacy_trace_spans(),
        require_multi_round_tool=True,
    )
    assert _mentions(strict_violations, "llm.generation", "expected 2", "found 1")


def test_payload_raw_id_and_unknown_attributes_are_rejected_without_values() -> None:
    trace_spans = deepcopy(_canonical_trace_spans())
    first_generation = trace_spans[1][0]
    forbidden = {
        "gen_ai.prompt": "private-prompt-PLACEHOLDER",
        "gen_ai.tool.call.id": "provider-call-id-PLACEHOLDER",
        "curie.unreviewed": "unknown-value-PLACEHOLDER",
    }
    for key, value in forbidden.items():
        _set_attribute(first_generation, key, value)

    violations = canonical_runner_shape_violations(trace_spans)

    for key in forbidden:
        assert _mentions(violations, key)
    material = repr(violations)
    assert all(value not in material for value in forbidden.values())


@pytest.mark.parametrize(
    ("span_index", "attribute", "value"),
    (
        (1, "curie.phase", "provider_payload"),
        (1, "curie.phase.start_kind", "raw_stream_start"),
        (1, "curie.phase.end_kind", "raw_stream_end"),
        (2, "curie.phase", "tool_payload"),
        (2, "curie.phase.start_kind", "raw_tool_start"),
        (2, "curie.phase.end_kind", "raw_tool_end"),
        (2, "curie.tool.call.index", 0),
        (2, "curie.tool.outcome", "open"),
    ),
)
def test_phase_values_and_tool_index_are_closed_and_bounded(
    span_index: int,
    attribute: str,
    value: str | int,
) -> None:
    trace_spans = deepcopy(_canonical_trace_spans())
    _set_attribute(trace_spans[span_index][0], attribute, value)

    violations = canonical_runner_shape_violations(trace_spans)

    assert _mentions(violations, attribute), violations

"""The remediation lifecycle counter and its bounded vocabularies.

@spec AUTOMATED-REMEDIATION-21

docs/superpowers/specs/2026-10-07-automated-remediation.md, AUTOMATED-REMEDIATION-21:

    One counter ``curie.remediation.lifecycle`` with closed attributes ``stage``
    (the receipt stages), ``kind`` (``remediate``, ``prevent``, ``tune``),
    ``authority`` (``policy``, ``approval``, ``none``) and ``code`` (the closed
    refusal and outcome codes), declared in ``_METRICS`` so it appears in
    ``declared_metric_manifest``; the new routes join ``_HTTP_OPERATIONS``.
    Spans carry the same attributes and the nomination id. No metric, span or
    log carries arguments, read values, the reason text or the alert body.

The closed domains are the frozen vector ``tests/vectors/remediation-codes.json``:
``stage`` is ``receipt_stages``, ``kind`` is ``kinds``, ``authority`` is
``authorities`` and ``code`` is the union of ``nomination_refusals``,
``approval_reasons``, ``approval_resolution_refusals`` and the verification
outcomes, plus the explicit no-code value ``none``. Every domain is a closed
list (never a ``bounded`` free-text label), so the cardinality bound is the
product of the lists and a value outside a list is refused by ``record_metric``.

Spans use the metric catalog's spelling through ``operation_span``: the caller
keys ``stage``, ``kind``, ``authority``, ``code`` and ``nomination_id`` export as
``curie.remediation.<key>``; ``nomination_id`` is admitted on spans only, as
``event_id`` is.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pytest
from curie_telemetry import operation_span, record_metric
from curie_telemetry.metrics import declared_metric_manifest
from curie_telemetry.tracing import configure_tracer_provider
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

_PACKAGE_ROOT = Path(__file__).parent.parent
_MANIFEST = _PACKAGE_ROOT / "schema" / "metrics.json"
_VECTOR = json.loads(
    (_PACKAGE_ROOT.parent.parent / "tests" / "vectors" / "remediation-codes.json").read_text(
        "utf-8"
    )
)
_NAME = "curie.remediation.lifecycle"
_CODES = (
    set(_VECTOR["nomination_refusals"])
    | set(_VECTOR["approval_reasons"])
    | set(_VECTOR["approval_resolution_refusals"])
    | set(_VECTOR["verification_outcomes"])
    | {"none"}
)
_CLOSED = {"stage", "kind", "authority", "code"}
_FORBIDDEN_SHAPES = ("arguments", "argument", "reason", "alert", "body", "value", "sample")


def _committed() -> dict[str, Any]:
    return json.loads(_MANIFEST.read_text("utf-8"))["metrics"]


@pytest.mark.parametrize("source", ["declared", "committed"])
def test_the_manifest_declares_the_counter_with_closed_domains(source: str) -> None:
    """@spec AUTOMATED-REMEDIATION-21: in code and in the committed manifest."""

    metrics = declared_metric_manifest()["metrics"] if source == "declared" else _committed()
    definition = metrics.get(_NAME)
    assert definition is not None, f"{_NAME} is not in the {source} manifest"
    assert definition["type"] == "counter"
    assert definition["monotonic"] is True
    assert definition["unit"]
    attributes = definition["attributes"]
    assert _CLOSED <= set(attributes)
    assert set(attributes) - _CLOSED <= {"service.name"}, sorted(attributes)
    assert set(attributes["stage"]) == set(_VECTOR["receipt_stages"])
    assert set(attributes["kind"]) == set(_VECTOR["kinds"])
    assert set(attributes["authority"]) == set(_VECTOR["authorities"])
    assert set(attributes["code"]) == _CODES


def test_every_remediation_domain_is_a_closed_list_with_a_computed_bound() -> None:
    """@spec AUTOMATED-REMEDIATION-21: lists, never a free-text bounded label."""

    definition = _committed()[_NAME]
    for key, domain in definition["attributes"].items():
        assert isinstance(domain, list), f"{key} must be a closed list, not {domain!r}"
        assert len(domain) == len(set(domain))
        for value in domain:
            assert isinstance(value, str) and value == value.strip() and " " not in value, (
                f"{key} value {value!r} is not a bare code"
            )
    assert definition["cardinality_bound"] == math.prod(
        len(domain) for domain in definition["attributes"].values()
    )


def test_no_attribute_can_carry_an_argument_a_sample_or_free_text() -> None:
    """@spec AUTOMATED-REMEDIATION-21: the key names are the four closed ones."""

    for key in _committed()[_NAME]["attributes"]:
        assert key in _CLOSED | {"service.name"}
        assert not any(shape in key for shape in _FORBIDDEN_SHAPES), key


def test_a_valid_point_is_accepted_and_a_value_outside_a_domain_is_refused() -> None:
    """@spec AUTOMATED-REMEDIATION-21: the closed domains are enforced at record time."""

    service = _committed()[_NAME]["attributes"].get("service.name")
    base: dict[str, str] = {
        "stage": "refused",
        "kind": "remediate",
        "authority": "none",
        "code": "unknown_action",
    }
    if service:
        base["service.name"] = service[0]
    record_metric(_NAME, attributes=base)
    for key, value in (
        ("stage", "example_unknown_stage"),
        ("kind", "example_unknown_kind"),
        ("authority", "example_unknown_authority"),
        ("code", "the model's free text reason"),
    ):
        with pytest.raises(ValueError, match="outside its declared domain"):
            record_metric(_NAME, attributes={**base, key: value})
    with pytest.raises(ValueError, match="undeclared attribute"):
        record_metric(_NAME, attributes={**base, "nomination_id": "example-id"})


def test_the_new_read_routes_join_the_http_operations() -> None:
    """@spec AUTOMATED-REMEDIATION-21: "the new routes join ``_HTTP_OPERATIONS``"."""

    from curie_telemetry.metrics import _HTTP_OPERATIONS

    assert "/remediation-nominations" in _HTTP_OPERATIONS
    assert "/remediation-nominations/{nomination_id}" in _HTTP_OPERATIONS


def test_a_span_carries_the_closed_attributes_and_the_nomination_id() -> None:
    """@spec AUTOMATED-REMEDIATION-21: "Spans carry the same attributes and the
    nomination id", exported under ``curie.remediation.*``.
    """

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(provider)
    try:
        with operation_span(
            "worker.remediation.receipt",
            kind=SpanKind.INTERNAL,
            attributes={
                "stage": "verified",
                "kind": "remediate",
                "authority": "policy",
                "code": "none",
                "nomination_id": "00000000-0000-4000-8000-000000000001",
            },
        ):
            pass
        (span,) = exporter.get_finished_spans()
        attributes = dict(span.attributes or {})
        assert attributes["curie.remediation.stage"] == "verified"
        assert attributes["curie.remediation.kind"] == "remediate"
        assert attributes["curie.remediation.authority"] == "policy"
        assert attributes["curie.remediation.code"] == "none"
        assert attributes["curie.remediation.nomination_id"] == (
            "00000000-0000-4000-8000-000000000001"
        )
    finally:
        configure_tracer_provider(None)
        provider.shutdown()


def test_a_span_refuses_an_attribute_outside_the_closed_vocabulary() -> None:
    """@spec AUTOMATED-REMEDIATION-21: no argument or reason key can be set on a span."""

    provider = TracerProvider()
    configure_tracer_provider(provider)
    try:
        for key in ("arguments", "reason", "alert_body", "sample"):
            with pytest.raises(ValueError, match="undeclared platform span attribute"):
                with operation_span(
                    "worker.remediation.receipt",
                    kind=SpanKind.INTERNAL,
                    attributes={"stage": "refused", key: "x"},
                ):
                    pass
    finally:
        configure_tracer_provider(None)
        provider.shutdown()

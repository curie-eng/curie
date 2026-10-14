"""Remediation telemetry: the lifecycle counter, spans, and what they never carry.

@spec AUTOMATED-REMEDIATION-21 @spec AUTOMATED-REMEDIATION-20

docs/superpowers/specs/2026-10-07-automated-remediation.md, AUTOMATED-REMEDIATION-21:
"One counter ``curie.remediation.lifecycle`` with closed attributes ``stage``,
``kind``, ``authority`` and ``code`` ... Spans carry the same attributes and the
nomination id. No metric, span or log carries arguments, read values, the reason
text or the alert body." Acceptance: "a log and span capture over a full
automatic remediation and a full approval contains none of the fixture's argument
values or sampled values."

The surface these tests fix (``.projects/plans/task-remediation-receipts.tests.md``):

* the worker's receipt loop records ``curie.remediation.lifecycle`` once for each
  receipt it posts, with ``stage`` the receipt's stage, ``kind`` the nomination's
  kind, ``authority`` the receipt's authority and ``code`` the receipt's code or
  ``none``;
* it wraps each post in one ``operation_span`` whose exported attributes are
  ``curie.remediation.stage``, ``.kind``, ``.authority``, ``.code`` and
  ``.nomination_id`` (the nomination's UUID), and nothing about arguments;
* a capture of every posted message, every log record at DEBUG (API and worker,
  one process here), every span (name, attributes, events) and every metric
  attribute over a whole remediation contains none of the fixture's forbidden
  values: the non-target argument value, the sampled values, the model's reason
  and the protected delivery's alert body.

Every identifier is a placeholder.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _receipt_capture import deliver_receipts, set_threads, stage_of, whole
from _sealed_actions import executor_enabled  # noqa: F401 - fixture, requested by name
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from curie_telemetry.tracing import configure_tracer_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from test_remediation_approvals import REASON as APPROVAL_REASON
from test_remediation_escalation import _approval_scenario, _drive, _undoable_scenario
from test_remediation_verifier import SAMPLED_TEXT

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled", "runs_stream")

METRIC = "curie.remediation.lifecycle"
NON_TARGET = "example-ns"
REASON = "error ratio above threshold"
BODY_MARKER = "ALERT-BODY-MARKER-7f3a"
FORBIDDEN = (NON_TARGET, SAMPLED_TEXT, REASON, APPROVAL_REASON, BODY_MARKER, '"replicas"')


class Capture:
    def __init__(self, spans: InMemorySpanExporter, reader: InMemoryMetricReader) -> None:
        self.spans = spans
        self.reader = reader

    def span_text(self) -> str:
        return "\n".join(repr(span.to_json()) for span in self.spans.get_finished_spans())

    def receipt_spans(self) -> list[Any]:
        return [
            span
            for span in self.spans.get_finished_spans()
            if "curie.remediation.stage" in dict(span.attributes or {})
        ]

    def points(self) -> list[dict[str, Any]]:
        data = self.reader.get_metrics_data()
        found: list[dict[str, Any]] = []
        for resource in (data.resource_metrics if data else []):
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    if metric.name == METRIC:
                        for point in metric.data.data_points:
                            attributes = dict(point.attributes)
                            found.append({"attributes": attributes, "value": point.value})
        return found

    def metric_text(self) -> str:
        return repr(self.points())


@pytest.fixture
def capture(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> Iterator[Capture]:
    caplog.set_level(logging.DEBUG)
    exporter = InMemorySpanExporter()
    tracer = TracerProvider()
    tracer.add_span_processor(SimpleSpanProcessor(exporter))
    configure_tracer_provider(tracer)
    reader = InMemoryMetricReader()
    meter = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-worker",
            service_version="0.7.0",
            service_instance_id="acme-worker-receipts",
            deployment_environment="test",
        ),
    )
    monkeypatch.setattr(telemetry_metrics, "_provider", None)
    monkeypatch.setattr(telemetry_metrics, "_instruments", {})
    configure_meter_provider(meter)
    try:
        yield Capture(exporter, reader)
    finally:
        configure_tracer_provider(None)
        tracer.shutdown()
        meter.shutdown()


def _assert_clean(capture: Capture, posts: list[dict[str, Any]], logs: str) -> None:
    surfaces = {
        "messages": "\n".join(whole(post) for post in posts),
        "logs": logs,
        "spans": capture.span_text(),
        "metrics": capture.metric_text(),
    }
    assert surfaces["messages"] and capture.receipt_spans() and capture.points()
    for name, content in surfaces.items():
        for forbidden in FORBIDDEN:
            assert forbidden not in content, f"{forbidden!r} leaked into the {name}"


def test_every_receipt_is_counted_and_spanned_with_the_closed_attributes(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    capture: Capture,
) -> None:
    """@spec AUTOMATED-REMEDIATION-21: one counter point and one span per receipt, the
    same stage, kind, authority and code on both, and the nomination id on the span.
    """

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path, reversible=False)
    set_threads()
    _drive(client, nomination_id, "verified")

    posts = deliver_receipts()

    stages = [stage_of(post) for post in posts]
    assert stages == ["nominated", "executed", "verified"]
    points = capture.points()
    assert sorted(point["attributes"]["stage"] for point in points) == sorted(stages)
    for point in points:
        attributes = point["attributes"]
        assert point["value"] == 1
        assert attributes["kind"] == "remediate"
        assert attributes["authority"] == "policy"
        assert attributes["code"] == "none"
    spans = capture.receipt_spans()
    assert sorted(dict(span.attributes)["curie.remediation.stage"] for span in spans) == sorted(
        stages
    )
    for span in spans:
        attributes = dict(span.attributes)
        assert attributes["curie.remediation.kind"] == "remediate"
        assert attributes["curie.remediation.authority"] == "policy"
        assert attributes["curie.remediation.code"] == "none"
        assert attributes["curie.remediation.nomination_id"] == str(nomination_id)


def test_a_full_automatic_remediation_leaks_no_argument_sample_or_reason(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    capture: Capture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec AUTOMATED-REMEDIATION-21 @spec AUTOMATED-REMEDIATION-20: nominate, admit,
    execute, sample and verify, then every receipt: the capture holds no non-target
    argument value, sampled value or reason.
    """

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path, reversible=False)
    set_threads()
    _drive(client, nomination_id, "verified")

    posts = deliver_receipts()

    _assert_clean(capture, posts, caplog.text)


def test_a_full_not_recovered_remediation_with_undo_leaks_nothing(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    capture: Capture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec AUTOMATED-REMEDIATION-21: the unhealthy sampled value never reaches a
    receipt, the escalation report or the undo offer either.
    """

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path)
    set_threads()
    _drive(client, nomination_id, "not-recovered")

    posts = deliver_receipts()

    stages = [stage_of(post) for post in posts]
    assert stages[-3:] == ["not-recovered", "escalated", "undo_requested"]
    _assert_clean(capture, posts, caplog.text)


def test_a_full_approval_leaks_no_argument_sample_reason_or_alert_body(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    capture: Capture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec AUTOMATED-REMEDIATION-21 @spec AUTOMATED-REMEDIATION-20: out of bounds to
    approval to execution to a report: authority ``approval`` is counted and spanned,
    the admission check is the code, and nothing forbidden appears.
    """

    _approval_scenario(client, auth_headers, tmp_path)
    set_threads()

    posts = deliver_receipts()

    _assert_clean(capture, posts, caplog.text)
    by_stage = {
        point["attributes"]["stage"]: point["attributes"] for point in capture.points()
    }
    assert by_stage["approval_requested"]["code"] == "out_of_bounds"
    assert by_stage["approval_requested"]["authority"] == "none"
    assert by_stage["executed"]["authority"] == "approval"


def test_an_approval_raised_from_an_alert_carrying_a_marker_never_posts_it(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    capture: Capture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-21: the delivery whose
    body holds ``BODY_MARKER`` and whose nomination holds a reason with live markup
    yields receipts and telemetry with neither.
    """

    from test_remediation_approvals import _nominate, _request, _setup

    agent_id = _setup(client, auth_headers, tmp_path)
    _request(agent_id, _nominate(agent_id))
    set_threads()

    posts = deliver_receipts()

    assert [stage_of(post) for post in posts] == ["nominated", "approval_requested"]
    _assert_clean(capture, posts, caplog.text)
    for live in ("<@U0EXAMPLE9>", "<!channel>", "example.invalid"):
        assert live not in "\n".join(whole(post) for post in posts)

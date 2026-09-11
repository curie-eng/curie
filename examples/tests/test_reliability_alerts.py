"""Guards for retained Curie metrics, reliability alerts, and safe correlation.

Issue #2428: source overlays are not proof. These tests pin the shipped
installer assets so a Prometheus remote-write path, bounded alert set, and
body-free correlation recipe cannot silently disappear. Runtime firing lives
in the cluster-tier script; this file is the local consumer-path gate.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY = REPO_ROOT / "examples" / "sre-bot" / "observability"
README = REPO_ROOT / "examples" / "sre-bot" / "README.md"
ROLLOUT = REPO_ROOT / "examples" / "sre-bot" / "docs" / "METRICS-ROLLOUT.md"

REQUIRED_ALERTS = {
    "CurieTurnAcceptedStale",
    "CurieTaskFailure",
    "CurieQueueMessageAgeHigh",
    "CurieCompletionOutboxAgeHigh",
    "CurieCompletionOutboxSignalAbsent",
    "CurieReplyDeliveryRefused",
    "CurieChannelTokenRotationFailed",
    "CurieMailAdapterNotReady",
    "CurieRootDiskPressure",
    "CurieNodeMemoryHeadroomLow",
    "CurieApplicationMetricsAbsent",
    "CurieDuplicateNodeExporter",
}

FORBIDDEN_IDENTITY = (
    "run_id",
    "run.id",
    "session_id",
    "session.id",
    "event.id",
    "event_id",
    "user_id",
    "user.id",
    "sandbox_id",
    "sandbox.id",
    "deployment_id",
    "thread_id",
    "thread_key",
    "message_id",
    "message.body",
)

FORBIDDEN_SECRET = (
    "password",
    "api_key",
    "apikey",
    "authorization",
    "secret",
    "token=",
    "xoxb-",
    "sk-",
)

EXPORTER_NAME = "prometheusremotewrite/soak"


def _load(name: str) -> dict:
    path = OBSERVABILITY / name
    return yaml.safe_load(path.read_text())


def _alert_rules(values: dict) -> list[dict]:
    groups = (
        values.get("serverFiles", {})
        .get("alerting_rules.yml", {})
        .get("groups", [])
    )
    rules: list[dict] = []
    for group in groups or []:
        rules.extend(group.get("rules") or [])
    return rules


def test_curie_values_append_prometheus_remote_write_without_replacing_nop() -> None:
    values = _load("curie-values.yaml")
    collector = values["otelCollector"]
    exporter = collector["extraExporters"][EXPORTER_NAME]
    assert exporter["endpoint"].endswith(
        ".observability.svc.cluster.local/api/v1/write"
    )
    assert exporter["retry_on_failure"]["enabled"] is True
    assert exporter["remote_write_queue"]["enabled"] is True
    assert 0 < exporter["remote_write_queue"]["queue_size"] <= 100000
    assert "sending_queue" not in exporter
    assert collector["extraMetricPipelineExporters"] == [EXPORTER_NAME]
    conversion = exporter.get("resource_to_telemetry_conversion") or {}
    assert conversion.get("enabled") is False


def test_curie_values_allow_prometheus_metrics_ingress() -> None:
    peer = _load("curie-values.yaml")["security"]["otelCollectorNetworkPolicy"][
        "metricsIngress"
    ]
    assert peer == [
        {
            "namespaceSelector": {
                "matchLabels": {
                    "kubernetes.io/metadata.name": "observability",
                }
            },
            "podSelector": {
                "matchLabels": {
                    "app.kubernetes.io/name": "prometheus",
                    "app.kubernetes.io/instance": "prometheus",
                }
            },
        }
    ]


def test_prometheus_enables_remote_write_receiver() -> None:
    values = _load("prometheus-values.yaml")
    assert values["server"]["extraArgs"] == {
        "web.enable-remote-write-receiver": "",
    }


def test_prometheus_ships_required_reliability_alerts() -> None:
    rules = _alert_rules(_load("prometheus-values.yaml"))
    names = {rule["alert"] for rule in rules if "alert" in rule}
    missing = sorted(REQUIRED_ALERTS - names)
    assert not missing, f"missing reliability alerts: {missing}"


def test_alert_expressions_keep_identity_and_secrets_out_of_metric_labels() -> None:
    rules = _alert_rules(_load("prometheus-values.yaml"))
    dumped = yaml.safe_dump(rules)
    lower = dumped.lower()
    for needle in FORBIDDEN_IDENTITY:
        assert needle.lower() not in lower, (
            f"alert rules must not select high-cardinality identity {needle!r}"
        )
    for needle in FORBIDDEN_SECRET:
        assert needle not in lower, (
            f"alert rules must not emit or match credential material {needle!r}"
        )


def test_absent_and_stale_metrics_are_failure_signals() -> None:
    rules = {
        rule["alert"]: rule["expr"]
        for rule in _alert_rules(_load("prometheus-values.yaml"))
        if "alert" in rule
    }
    assert "absent(" in rules["CurieApplicationMetricsAbsent"]
    assert "absent(" in rules["CurieCompletionOutboxSignalAbsent"]
    stale = rules["CurieTurnAcceptedStale"]
    assert "increase(" in stale and "curie_turn_accepted_total" in stale


def test_duplicate_node_exporter_alert_counts_series_not_sum() -> None:
    expr = next(
        rule["expr"]
        for rule in _alert_rules(_load("prometheus-values.yaml"))
        if rule.get("alert") == "CurieDuplicateNodeExporter"
    )
    assert "count(" in expr
    assert "9100" in expr and "9101" in expr
    assert "node_memory_MemAvailable_bytes" in expr
    assert "sum(" not in expr.replace(" ", "")


def test_rollout_doc_separates_render_runtime_and_deployed_evidence() -> None:
    text = ROLLOUT.read_text()
    for required in (
        "locally rendered",
        "disposable runtime-tested",
        "actually deployed",
        "permanent soak",
        "rollback",
        "does not authorize",
    ):
        assert required in text.lower() or required in text, (
            f"rollout doc is missing {required!r}"
        )
    assert "source-only" in text.lower()
    assert "C0EXAMPLE1" in text
    assert not re.search(r"C0(?!EXAMPLE1)[A-Z0-9]{8,}", text)


def test_correlation_recipe_uses_safe_identifiers_not_bodies() -> None:
    text = README.read_text()
    assert "trace_id" in text or "traceId" in text
    assert "run" in text.lower()
    assert "without reading" in text.lower() or "without inspecting" in text.lower()
    assert "metric labels" in text.lower()
    lower = text.lower()
    assert "message body" in lower or "private body" in lower
    assert "do not" in lower and "body" in lower


def test_runtime_script_refuses_soak_identities() -> None:
    script = (
        REPO_ROOT / "charts" / "curie" / "ci" / "runtime" / "metrics-alerts-runtime.sh"
    ).read_text()
    assert "refusing soak identity" in script
    assert "curie" in script and "observability" in script
    assert "monitoring" in script


def test_promtool_unit_file_covers_fire_and_recovery() -> None:
    path = OBSERVABILITY / "reliability-alerts.test.yaml"
    payload = yaml.safe_load(path.read_text())
    tests = payload["tests"]
    names = {item["name"] for item in tests}
    assert any("fire" in name for name in names)
    assert any("recover" in name or "quiet" in name for name in names)
    alerts = {
        case["alertname"]
        for item in tests
        for case in item.get("alert_rule_test") or []
    }
    missing = sorted(REQUIRED_ALERTS - alerts)
    assert not missing, f"promtool tests omit alerts: {missing}"
    firing = [
        case
        for item in tests
        for case in item.get("alert_rule_test") or []
        if case.get("exp_alerts")
    ]
    recovering = [
        case
        for item in tests
        for case in item.get("alert_rule_test") or []
        if case.get("exp_alerts") == []
    ]
    assert firing, "promtool tests must include at least one firing case"
    assert recovering, "promtool tests must include at least one recovery case"

"""Guards for retained Curie metrics, reliability alerts, and safe correlation.

Issue #2428: source overlays are not proof. These tests pin the shipped
installer assets so a Prometheus remote-write path, bounded alert set, and
body-free correlation recipe cannot silently disappear. Runtime firing lives
in the cluster-tier script; this file is the local consumer-path gate.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY = REPO_ROOT / "examples" / "sre-bot" / "observability"
README = REPO_ROOT / "examples" / "sre-bot" / "README.md"
ROLLOUT = REPO_ROOT / "examples" / "sre-bot" / "docs" / "METRICS-ROLLOUT.md"
METRICS_CATALOG = (
    REPO_ROOT / "packages" / "telemetry" / "src" / "curie_telemetry" / "metrics.py"
)
RUNTIME_PROOF = REPO_ROOT / "charts" / "curie" / "ci" / "runtime" / "metrics-alerts-runtime.sh"

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
    "CurieWorkerSupervisedRestartLoop",
    "CurieWorkerSupervisedTaskParked",
    "CurieConnectorNotReady",
    "CurieDispatcherRestartLoop",
    "CurieNodeNotReady",
    "CuriePodCrashLooping",
    "CurieCoreWorkloadNotReady",
    "CurieStateStoreNotReady",
    "CuriePersistentVolumeSpaceLow",
    "CurieKubeStateMetricsDown",
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

# The counter rules that must also see a series' first sample, each with the
# selector, comparison and threshold it has always had.
FIRST_SAMPLE_RULES = {
    "CurieTaskFailure": (
        'curie_turn_completed_total{outcome="classified_failure"}',
        ">",
        "0",
    ),
    "CurieChannelTokenRotationFailed": (
        'curie_http_server_request_total{operation="/channels/token",outcome="5xx"}',
        ">",
        "0",
    ),
    "CurieReplyDeliveryRefused": (
        'curie_reply_delivery_total{outcome="failure"}',
        ">=",
        "3",
    ),
    "CurieWorkerSupervisedRestartLoop": (
        'curie_worker_supervised_restart_total{outcome="restart"}',
        ">=",
        "3",
    ),
}

# Each gauge CurieApplicationMetricsAbsent reads, and the service that records it.
APPLICATION_GAUGES = {
    "curie_queue_depth": "curie-worker",
    "curie_approval_pending": "curie-api",
}

_DURATION_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def _load(name: str) -> dict:
    path = OBSERVABILITY / name
    return yaml.safe_load(path.read_text())


def _rule(alert: str) -> dict:
    return next(
        rule
        for rule in _alert_rules(_load("prometheus-values.yaml"))
        if rule.get("alert") == alert
    )


def _compact(expr: str) -> str:
    """The expression without PromQL `#` comments or whitespace outside quotes.

    Comments go first, so a rationale that quotes the expression it explains
    cannot satisfy an assertion about the expression.
    """
    out: list[str] = []
    quote = ""
    index = 0
    while index < len(expr):
        char = expr[index]
        if quote:
            out.append(char)
            if char == "\\" and index + 1 < len(expr):
                out.append(expr[index + 1])
                index += 2
                continue
            if char == quote:
                quote = ""
        elif char in "\"'`":
            quote = char
            out.append(char)
        elif char == "#":
            while index < len(expr) and expr[index] != "\n":
                index += 1
            continue
        elif not char.isspace():
            out.append(char)
        index += 1
    return "".join(out)


def _matchers(body: str) -> frozenset[str]:
    return frozenset(part for part in body.split(",") if part)


def _seconds(duration: str) -> float:
    parts = re.findall(r"(\d+)(ms|s|m|h|d|w)", duration)
    assert parts and "".join(n + u for n, u in parts) == duration, (
        f"not a PromQL duration: {duration!r}"
    )
    return sum(int(number) * _DURATION_UNITS[unit] for number, unit in parts)


def _first_sample_terms(alert: str) -> re.Match[str] | None:
    """Parse `increase(X[W]) OP T or (X unless last_over_time(X[L] offset O)) OP T`.

    Every selector must be the rule's own, matcher for matcher, and is then
    read as X. Parentheses around either whole term are allowed.
    """
    selector = FIRST_SAMPLE_RULES[alert][0]
    name, _, body = selector.partition("{")
    expected = _matchers(body.rstrip("}"))
    compact = _compact(_rule(alert)["expr"])

    def as_x(match: re.Match[str]) -> str:
        found = _matchers(match.group(1))
        return "X" if found == expected else match.group(0)

    compact = re.sub(re.escape(name) + r"\{([^}]*)\}", as_x, compact)
    return re.fullmatch(
        r"\(?increase\(X\[(?P<window>\w+)\]\)(?P<op>>=|>)(?P<threshold>[\d.]+)\)?"
        r"or"
        r"\(?\(Xunless\(?last_over_time\(X\[(?P<lookback>\w+)\]offset(?P<offset>\w+)\)\)?\)"
        r"(?P<op2>>=|>)(?P<threshold2>[\d.]+)\)?",
        compact,
    )


def _alert_rules(values: dict) -> list[dict]:
    groups = values.get("serverFiles", {}).get("alerting_rules.yml", {}).get("groups", [])
    rules: list[dict] = []
    for group in groups or []:
        rules.extend(group.get("rules") or [])
    return rules


def test_curie_values_append_prometheus_remote_write_without_replacing_nop() -> None:
    values = _load("curie-values.yaml")
    collector = values["otelCollector"]
    exporter = collector["extraExporters"][EXPORTER_NAME]
    assert exporter["endpoint"].endswith(".observability.svc.cluster.local/api/v1/write")
    assert exporter["retry_on_failure"]["enabled"] is True
    assert exporter["remote_write_queue"]["enabled"] is True
    assert 0 < exporter["remote_write_queue"]["queue_size"] <= 100000
    assert "sending_queue" not in exporter
    assert collector["extraMetricPipelineExporters"] == [EXPORTER_NAME]
    conversion = exporter.get("resource_to_telemetry_conversion") or {}
    assert conversion.get("enabled") is False


def test_curie_values_allow_prometheus_metrics_ingress() -> None:
    peer = _load("curie-values.yaml")["security"]["otelCollectorNetworkPolicy"]["metricsIngress"]
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


def test_kube_state_metrics_exports_the_labels_core_alerts_select_on() -> None:
    kube_state_metrics = _load("prometheus-values.yaml")["kube-state-metrics"]
    allowlist = kube_state_metrics.get("metricLabelsAllowlist") or []
    assert sorted(allowlist) == sorted(
        [
            "deployments=[app.kubernetes.io/component,helm.sh/chart]",
            "statefulsets=[helm.sh/chart]",
        ]
    ), (
        "CurieCoreWorkloadNotReady and CurieStateStoreNotReady select on these "
        "labels and stay silent without them"
    )


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


@pytest.mark.parametrize("alert", sorted(FIRST_SAMPLE_RULES))
def test_counter_rule_also_reads_a_series_first_sample(alert: str) -> None:
    # increase() cannot see a series' first sample, and a Curie counter
    # series first appears already at its first value.
    terms = _first_sample_terms(alert)
    assert terms, (
        f"{alert} must read `increase(X[window]) OP T or "
        "(X unless last_over_time(X[1h] offset O)) OP T` with X its own selector "
        "in every term"
    )
    _, comparison, threshold = FIRST_SAMPLE_RULES[alert]
    assert terms["op"] == terms["op2"] == comparison
    assert terms["threshold"] == terms["threshold2"] == threshold


@pytest.mark.parametrize("alert", sorted(FIRST_SAMPLE_RULES))
def test_first_sample_offset_is_the_window(alert: str) -> None:
    # Then the new-series term counts, from zero, the span increase() counts
    # for an old series, and `for` applies to both terms alike.
    terms = _first_sample_terms(alert)
    assert terms, (
        f"{alert} has no `X unless last_over_time(X[1h] offset O)` term to read an "
        "offset from"
    )
    assert _seconds(terms["offset"]) == _seconds(terms["window"]), (
        f"{alert}: offset {terms['offset']} must equal its window {terms['window']}"
    )


@pytest.mark.parametrize("alert", sorted(FIRST_SAMPLE_RULES))
def test_first_sample_term_looks_back_an_hour(alert: str) -> None:
    # A pipeline gap shorter than the look-back never makes an old series
    # look new.
    terms = _first_sample_terms(alert)
    assert terms, f"{alert} has no `last_over_time(X[L] offset O)` term to read L from"
    assert _seconds(terms["lookback"]) == 3600, (
        f"{alert}: the new-series term must look back 1h, got {terms['lookback']}"
    )


def _catalog_kinds() -> dict[str, str]:
    """Each catalog metric's instrument kind, by name with dots as underscores.

    A counter's name gains `_total`. No unit suffix is applied, so the name is
    the one Prometheus stores only for a `{...}` unit, which it drops. Read
    through the parser, so a comment in the catalog cannot count.
    """
    tree = ast.parse(METRICS_CATALOG.read_text())
    catalog = next(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(
            isinstance(target, ast.Name) and target.id == "_METRICS"
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        )
    )
    assert isinstance(catalog, ast.Dict)
    kinds: dict[str, str] = {}
    for key, value in zip(catalog.keys, catalog.values, strict=True):
        assert isinstance(key, ast.Constant) and isinstance(value, ast.Call)
        kind = value.args[0]
        assert isinstance(kind, ast.Constant)
        name = key.value.replace(".", "_")
        kinds[name + "_total" if kind.value == "counter" else name] = kind.value
    return kinds


def test_application_metrics_absent_reads_gauges_every_process_records() -> None:
    # A counter has no series until its first measurement, so its absence
    # after a restart is a quiet system; a gauge its service records on every
    # tick or sweep is absent only when that service or the pipeline is. The
    # service_name matcher names the missing service in the alert.
    rule = _rule("CurieApplicationMetricsAbsent")
    compact = _compact(rule["expr"])
    name = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
    term = rf"absent\({name}(?:\{{[^}}]*\}})?\)"
    assert re.fullmatch(rf"{term}(?:or{term})*", compact), (
        "CurieApplicationMetricsAbsent must be absent() of metrics joined by or, "
        f"got {rule['expr']!r}"
    )
    read = dict(re.findall(rf"absent\(({name})(?:\{{([^}}]*)\}})?\)", compact))
    kinds = _catalog_kinds()
    for metric in sorted(read):
        assert kinds.get(metric) == "gauge", (
            f"{metric} is {kinds.get(metric)!r} in the telemetry catalog, not a gauge"
        )
    assert read == {
        metric: f'service_name="{service}"' for metric, service in APPLICATION_GAUGES.items()
    }
    assert rule.get("for") == "10m"


def test_runtime_proof_emits_and_waits_on_the_gauges_the_absent_rule_reads() -> None:
    # The runtime proof breaks export to show absent-data detection, which it
    # can show only for gauges it emits, under the names Prometheus stores.
    script = "\n".join(
        line
        for line in RUNTIME_PROOF.read_text().splitlines()
        if not line.lstrip().startswith("#")
    )
    for metric, service in APPLICATION_GAUGES.items():
        otel = re.escape(metric.replace("_", "."))
        call = re.search(rf'gauge_metric\(\s*"{otel}"[^)]*\)', script)
        assert call, f"the runtime proof does not emit {metric}"
        assert re.search(rf'"service\.name":\s*"{re.escape(service)}"', call.group(0))
        # A unit other than a {...} annotation adds a suffix to the stored name.
        assert re.search(r'unit="\{[^"]*\}"', call.group(0)), call.group(0)
        assert f'{metric}{{service_name="{service}"}}' in script, (
            f"the runtime proof does not wait on {metric} from {service}"
        )


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
        assert required in text.lower() or required in text, f"rollout doc is missing {required!r}"
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
    alerts = {case["alertname"] for item in tests for case in item.get("alert_rule_test") or []}
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

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
ASSETS="$REPO_ROOT/examples/sre-bot/observability"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# The one place this gate names the Prometheus chart version. The assertions
# below cross-check it against the version cli/src/examples.rs actually installs.
PROMETHEUS_CHART_VERSION=29.27.0

required_assets=(
  grafana-values.yaml
  loki-values.yaml
  alloy-values.yaml
  prometheus-values.yaml
  tempo.yaml
  curie-values.yaml
)
for asset in "${required_assets[@]}"; do
  [[ -f "$ASSETS/$asset" ]] || fail "missing canonical installer asset $ASSETS/$asset"
done

# Keep repository state untouched while Helm downloads the exact upstream pins.
export HELM_CACHE_HOME="$TMP/helm/cache"
export HELM_CONFIG_HOME="$TMP/helm/config"
export HELM_DATA_HOME="$TMP/helm/data"
helm repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null
helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null

# These release names are part of the migration contract. In particular, Grafana
# must upgrade the existing grafana release so its PVC and service account remain.
helm template grafana grafana-community/grafana \
  --version 12.11.1 \
  --namespace observability \
  -f "$ASSETS/grafana-values.yaml" >"$TMP/grafana.yaml"
helm template loki grafana-community/loki \
  --version 18.10.1 \
  --namespace observability \
  -f "$ASSETS/loki-values.yaml" >"$TMP/loki.yaml"
helm template alloy grafana/alloy \
  --version 1.11.1 \
  --namespace observability \
  -f "$ASSETS/alloy-values.yaml" >"$TMP/alloy.yaml"
helm template prometheus prometheus-community/prometheus \
  --version "$PROMETHEUS_CHART_VERSION" \
  --namespace observability \
  -f "$ASSETS/prometheus-values.yaml" >"$TMP/prometheus.yaml"
# A monitoring stack that was already in the cluster before this installer ran
# (issue #2060). Rendering the same subcharts under a different release in a
# different namespace is the cheapest faithful stand-in: the annotations, labels
# and object names are the real ones, so the scrape-boundary assertion below is
# fed what Kubernetes service discovery would actually present rather than an
# invented fixture that could drift from the chart.
helm template legacy prometheus-community/prometheus \
  --version "$PROMETHEUS_CHART_VERSION" \
  --namespace other-monitoring \
  -f "$ASSETS/prometheus-values.yaml" >"$TMP/prometheus-second-source.yaml"

helm template curie "$CHART" \
  --namespace curie \
  -f "$ASSETS/curie-values.yaml" >"$TMP/curie-install.yaml"
helm template curie "$CHART" \
  --namespace curie \
  --is-upgrade \
  -f "$ASSETS/curie-values.yaml" >"$TMP/curie-upgrade.yaml"
helm template curie "$CHART" --namespace curie >"$TMP/curie-default.yaml"

python3 - \
  "$ASSETS" \
  "$CHART" \
  "$TMP/grafana.yaml" \
  "$TMP/loki.yaml" \
  "$TMP/alloy.yaml" \
  "$TMP/prometheus.yaml" \
  "$TMP/prometheus-second-source.yaml" \
  "$ASSETS/tempo.yaml" \
  "$TMP/curie-install.yaml" \
  "$TMP/curie-upgrade.yaml" \
  "$TMP/curie-default.yaml" \
  "$PROMETHEUS_CHART_VERSION" \
  "${OBSERVABILITY_ASSERTION_MUTATION:-}" <<'PY'
import base64
from decimal import Decimal
from pathlib import Path
import re
import sys

import yaml

(
    assets_path,
    chart_path,
    grafana_path,
    loki_path,
    alloy_path,
    prometheus_path,
    prometheus_second_source_path,
    tempo_path,
    curie_install_path,
    curie_upgrade_path,
    curie_default_path,
    prometheus_chart_version,
    mutation,
) = sys.argv[1:]
assert mutation in {
    "",
    "loki-cache",
    "tempo-image",
    "tempo-exporter",
    "cleanup-tag",
    "delete-hook",
    "token-cleanup",
    "token-data",
    "updater-tag",
    "viewer-role",
    "rotation-restart",
    "restart-scope",
    "scrape-namespace",
    "scrape-source-label",
    "scrape-source-label-inert",
    "scrape-source-label-conditional",
    "scrape-namespace-names",
    "tempo-envelope",
}, f"unknown mutation {mutation!r}"

assets = Path(assets_path)
chart = Path(chart_path)


def load_one(path):
    return yaml.safe_load(Path(path).read_text())


def load_docs(path):
    result = []
    for doc in yaml.safe_load_all(Path(path).read_text()):
        if not doc:
            continue
        if doc.get("kind") == "List":
            result.extend(doc.get("items", []))
        else:
            result.append(doc)
    return result


def at(value, *keys):
    for key in keys:
        assert isinstance(value, dict) and key in value, (
            f"{'.'.join(keys)} is missing at {key!r}"
        )
        value = value[key]
    return value


def assert_quantity(actual, expected, label):
    assert str(actual) == expected, f"{label}: expected {expected}, got {actual}"


def memory_mi(quantity):
    text = str(quantity)
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMGTE]i)?", text)
    assert match, f"unsupported memory quantity {text!r}"
    amount = Decimal(match.group(1))
    unit = match.group(2) or ""
    factors = {"": Decimal(1) / (1024 * 1024), "Ki": Decimal(1) / 1024,
               "Mi": Decimal(1), "Gi": Decimal(1024), "Ti": Decimal(1024**2)}
    return amount * factors[unit]


def pod_specs(docs):
    for doc in docs:
        kind = doc.get("kind")
        spec = doc.get("spec", {})
        if kind in {"Deployment", "DaemonSet", "StatefulSet", "ReplicaSet", "Job"}:
            yield doc, spec.get("template", {}).get("spec", {})
        elif kind == "Pod":
            yield doc, spec


def containers(docs):
    for doc, pod_spec in pod_specs(docs):
        for container in pod_spec.get("containers", []):
            yield doc, pod_spec, container


def image_container(docs, fragment, label):
    matches = [item for item in containers(docs) if fragment in str(item[2].get("image", ""))]
    assert len(matches) == 1, f"expected one {label} container, found {len(matches)}"
    return matches[0]


def embedded_yaml(docs):
    for doc in docs:
        if doc.get("kind") not in {"ConfigMap", "Secret"}:
            continue
        for field in ("data", "stringData"):
            for key, value in (doc.get(field) or {}).items():
                if not isinstance(value, str):
                    continue
                candidate = value
                if field == "data" and doc.get("kind") == "Secret":
                    try:
                        candidate = base64.b64decode(value, validate=True).decode()
                    except Exception:
                        pass
                try:
                    parsed = yaml.safe_load(candidate)
                except yaml.YAMLError:
                    continue
                if isinstance(parsed, (dict, list)):
                    yield doc, key, parsed, candidate


def find_nested_dicts(value, key):
    if isinstance(value, dict):
        if key in value:
            yield value[key]
        for child in value.values():
            yield from find_nested_dicts(child, key)
    elif isinstance(value, list):
        for child in value:
            yield from find_nested_dicts(child, key)


def hook_annotations(doc):
    return str(doc.get("metadata", {}).get("annotations", {}).get("helm.sh/hook", ""))


def storage_requests(docs):
    requests = []
    for doc in docs:
        if doc.get("kind") == "PersistentVolumeClaim":
            requests.append(at(doc, "spec", "resources", "requests", "storage"))
        if doc.get("kind") == "StatefulSet":
            for claim in doc.get("spec", {}).get("volumeClaimTemplates", []):
                requests.append(at(claim, "spec", "resources", "requests", "storage"))
    return requests


grafana_values = load_one(assets / "grafana-values.yaml")
loki_values = load_one(assets / "loki-values.yaml")
alloy_values = load_one(assets / "alloy-values.yaml")
prometheus_values = load_one(assets / "prometheus-values.yaml")
curie_values = load_one(assets / "curie-values.yaml")

assert at(grafana_values, "image", "tag") == "13.2.0"
assert at(grafana_values, "admin", "existingSecret") == "grafana-admin"
assert at(grafana_values, "admin", "userKey") == "admin-user"
assert at(grafana_values, "admin", "passwordKey") == "admin-password"
assert at(grafana_values, "persistence", "enabled") is True
assert_quantity(at(grafana_values, "persistence", "size"), "2Gi", "Grafana PVC")
assert at(grafana_values, "persistence", "storageClassName") == "local-path"
assert at(grafana_values, "testFramework", "enabled") is False

assert at(loki_values, "deploymentMode") == "SingleBinary"
for component in ("chunksCache", "resultsCache", "gateway", "lokiCanary", "test"):
    if mutation == "loki-cache" and component == "chunksCache":
        loki_values[component]["enabled"] = True
    assert at(loki_values, component, "enabled") is False, f"Loki {component} must be disabled"
assert at(loki_values, "singleBinary", "replicas") == 1
assert at(loki_values, "read", "replicas") == 0
assert at(loki_values, "write", "replicas") == 0
assert at(loki_values, "backend", "replicas") == 0
assert at(loki_values, "loki", "storage", "type") == "filesystem"
assert_quantity(at(loki_values, "singleBinary", "persistence", "size"), "10Gi", "Loki PVC")
assert at(loki_values, "singleBinary", "persistence", "storageClass") == "local-path"
assert at(loki_values, "loki", "ingester", "wal", "replay_memory_ceiling") == "512MB"
disk_threshold = at(loki_values, "loki", "ingester", "wal", "disk_full_threshold")
assert disk_threshold, "Loki 3.7 disk_full_threshold must be set"

assert at(alloy_values, "controller", "type") == "daemonset"
assert at(alloy_values, "alloy", "storagePath") == "/var/lib/alloy/data"
host_paths = [
    volume.get("hostPath", {}).get("path")
    for volume in at(alloy_values, "controller", "volumes", "extra")
]
assert "/var/lib/alloy" in host_paths, "Alloy positions must use durable hostPath storage"
alloy_content = at(alloy_values, "alloy", "configMap", "content")
assert "stage.cri" in alloy_content, "Alloy must parse containerd CRI log lines"
assert "/var/log/pods/" in alloy_content, "Alloy must discover Kubernetes pod logs"

assert at(prometheus_values, "alertmanager", "enabled") is False
assert at(prometheus_values, "prometheus-pushgateway", "enabled") is False
assert at(prometheus_values, "configmapReload", "prometheus", "enabled") is False
assert_quantity(at(prometheus_values, "server", "persistentVolume", "size"), "8Gi", "Prometheus PVC")
assert at(prometheus_values, "server", "persistentVolume", "storageClass") == "local-path"

grafana_docs = load_docs(grafana_path)
loki_docs = load_docs(loki_path)
alloy_docs = load_docs(alloy_path)
prometheus_docs = load_docs(prometheus_path)
tempo_docs = load_docs(tempo_path)
curie_install_docs = load_docs(curie_install_path)
curie_upgrade_docs = load_docs(curie_upgrade_path)
curie_default_docs = load_docs(curie_default_path)

expected_requests = {
    "Grafana": (grafana_docs, "grafana/grafana", Decimal(128)),
    "Loki": (loki_docs, "grafana/loki", Decimal(256)),
    "Alloy": (alloy_docs, "grafana/alloy", Decimal(128)),
    "Tempo": (tempo_docs, "grafana/tempo", Decimal(256)),
    "Prometheus": (prometheus_docs, "prometheus/prometheus", Decimal(512)),
    "kube-state-metrics": (prometheus_docs, "kube-state-metrics", Decimal(64)),
    "node-exporter": (prometheus_docs, "node-exporter", Decimal(32)),
}
request_total = Decimal(0)
for label, (docs, image, expected) in expected_requests.items():
    _, _, container = image_container(docs, image, label)
    actual = memory_mi(at(container, "resources", "requests", "memory"))
    assert actual == expected, f"{label} request must be {expected}Mi, got {actual}Mi"
    request_total += actual
assert request_total == 1376, f"observability request total must be 1376Mi, got {request_total}Mi"

# The chart image itself closes the Loki config and image version contract.
_, _, loki_container = image_container(loki_docs, "grafana/loki", "Loki")
loki_image = str(loki_container["image"])
assert re.search(r":(?:v)?3\.7(?:\.|$)", loki_image), f"Loki image must be 3.7.x, got {loki_image}"
loki_configs = []
for _, _, parsed, _ in embedded_yaml(loki_docs):
    if isinstance(parsed, dict) and "ingester" in parsed and "schema_config" in parsed:
        loki_configs.append(parsed)
assert len(loki_configs) == 1, f"expected one rendered Loki config, found {len(loki_configs)}"
rendered_wal = at(loki_configs[0], "ingester", "wal")
assert rendered_wal.get("disk_full_threshold") == disk_threshold
assert rendered_wal.get("replay_memory_ceiling") == "512MB"
assert sum(1 for value in find_nested_dicts(loki_configs[0], "disk_full_threshold")) == 1, (
    "disk_full_threshold must render exactly once at ingester.wal"
)

# Absence is checked in rendered output as well as values, preventing an enabled
# default from silently returning under a new chart value path.
loki_render = Path(loki_path).read_text().lower()
for forbidden in ("chunks-cache", "chunkscache", "results-cache", "resultscache", "gateway", "canary", "helm-test"):
    assert forbidden not in loki_render, f"disabled Loki component {forbidden!r} rendered"

grafana_pvcs = [doc for doc in grafana_docs if doc.get("kind") == "PersistentVolumeClaim"]
assert grafana_pvcs, "Grafana must render a persistent PVC"
assert any(at(doc, "spec", "resources", "requests", "storage") == "2Gi" for doc in grafana_pvcs)
assert "10Gi" in storage_requests(loki_docs), "Loki must render a 10Gi persistent claim"
assert "8Gi" in storage_requests(prometheus_docs), "Prometheus must render an 8Gi persistent claim"

# ---------------------------------------------------------------------------
# The scrape source boundary (issue #2060).
#
# The shipped Prometheus used to run the chart's stock annotation-driven
# discovery, which keeps any `prometheus.io/scrape` target in ANY namespace.
# Installed beside a monitoring stack that was already in the cluster, it
# ingested that stack's kube-state-metrics and node exporter too: one Kubernetes
# object, two series, identical workload labels, different scrape-source labels,
# and a bot that reported twice the restarts.
#
# So this does not read the config and agree with it. It reconstructs the
# Kubernetes service-discovery targets that the shipped stack and a second
# rendered stack would actually present, runs the rendered relabel program over
# them the way Prometheus would, and asserts on which targets survive. That is
# what makes it fail when the boundary is removed rather than when the wording
# changes -- see the `scrape-namespace` and `scrape-source-label` mutations.
SCRAPE_SOURCE_LABEL = "curie_source"
SCRAPE_SOURCE_VALUE = "curie-sre-bot"
SECOND_SOURCE_NAMESPACE = "other-monitoring"
RELEASE_NAMESPACE = "observability"


def relabel_text(value, default):
    """YAML reads a bare `true` as a bool; Prometheus reads these fields as strings."""
    if value is None:
        return default
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def meta_label(name):
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def expand_replacement(replacement, matched):
    def substitute(reference):
        index = int(reference.group(1) or reference.group(2))
        try:
            return matched.group(index) or ""
        except IndexError:
            return ""

    return re.sub(r"\$(?:(\d+)|\{(\d+)\})", substitute, replacement)


def apply_relabel(labels, rules):
    """The keep/drop/replace/labelmap subset the shipped jobs use. None = dropped."""
    labels = dict(labels)
    for rule in rules:
        action = rule.get("action", "replace")
        regex = relabel_text(rule.get("regex"), "(.*)")
        if action == "labelmap":
            pattern = re.compile(rf"^(?:{regex})$")
            for name in list(labels):
                matched = pattern.match(name)
                if matched:
                    renamed = expand_replacement(
                        relabel_text(rule.get("replacement"), "$1"), matched
                    )
                    labels[renamed] = labels[name]
            continue
        separator = relabel_text(rule.get("separator"), ";")
        value = separator.join(labels.get(name, "") for name in rule.get("source_labels", []))
        matched = re.match(rf"^(?:{regex})$", value)
        if action == "keep":
            if not matched:
                return None
        elif action == "drop":
            if matched:
                return None
        elif action == "replace":
            if not matched:
                continue
            replaced = expand_replacement(relabel_text(rule.get("replacement"), "$1"), matched)
            if replaced:
                labels[rule["target_label"]] = replaced
            else:
                labels.pop(rule["target_label"], None)
        else:
            raise AssertionError(f"unsupported relabel action {action!r}")
    return labels


def discovery_scope(sd_config):
    """The namespaces one kubernetes_sd_config can see, or None for cluster-wide.

    `own_namespace` and `names` are a UNION in Prometheus, not alternatives, so
    reading only the first would pass a config that names another stack's
    namespace outright while production happily scrapes it.
    """
    namespaces = sd_config.get("namespaces") or {}
    scope = set(namespaces.get("names") or ())
    if namespaces.get("own_namespace"):
        scope.add(RELEASE_NAMESPACE)
    return scope or None


def exporter_sd_targets(docs, namespace):
    """Kubernetes SD targets for the capacity exporters in one rendered stack.

    Which stack a target belongs to is read off its namespace rather than passed
    in, so the assertions below cannot disagree with the render about it.
    """
    targets = []
    for doc in docs:
        metadata = doc.get("metadata", {})
        name = metadata.get("name", "")
        exporter = next(
            (family for family in ("kube-state-metrics", "node-exporter") if family in name),
            None,
        )
        if exporter is None:
            continue
        if doc.get("kind") not in {"Service", "Deployment", "DaemonSet", "StatefulSet"}:
            continue
        object_namespace = metadata.get("namespace")
        assert object_namespace == namespace, (
            f"{doc['kind']} {name} rendered into {object_namespace!r}, expected {namespace!r}: "
            "the scrape boundary assertion reads a target's stack off its namespace"
        )
        if doc.get("kind") == "Service":
            labels = {
                "__address__": "10.0.0.1:8080",
                "__meta_kubernetes_namespace": object_namespace,
                "__meta_kubernetes_service_name": name,
                "__meta_kubernetes_endpointslice_port_name": "http",
                "__meta_kubernetes_pod_node_name": "node-1",
            }
            for key, value in (metadata.get("annotations") or {}).items():
                labels[f"__meta_kubernetes_service_annotation_{meta_label(key)}"] = value
            for key, value in (metadata.get("labels") or {}).items():
                labels[f"__meta_kubernetes_service_label_{meta_label(key)}"] = value
            # One Service is reachable through both the endpointslice and the
            # service role, and the shipped config enables jobs for each.
            for role in ("endpointslice", "service"):
                targets.append(
                    {
                        "role": role,
                        "namespace": object_namespace,
                        "exporter": exporter,
                        "labels": dict(labels),
                    }
                )
        elif doc.get("kind") in {"Deployment", "DaemonSet", "StatefulSet"}:
            template = at(doc, "spec", "template", "metadata")
            labels = {
                "__address__": "10.0.0.1:8080",
                "__meta_kubernetes_namespace": object_namespace,
                "__meta_kubernetes_pod_name": f"{name}-abc",
                "__meta_kubernetes_pod_ip": "10.0.0.1",
                "__meta_kubernetes_pod_phase": "Running",
                "__meta_kubernetes_pod_node_name": "node-1",
            }
            for key, value in (template.get("annotations") or {}).items():
                labels[f"__meta_kubernetes_pod_annotation_{meta_label(key)}"] = value
            for key, value in (template.get("labels") or {}).items():
                labels[f"__meta_kubernetes_pod_label_{meta_label(key)}"] = value
            targets.append(
                {"role": "pod", "namespace": object_namespace, "exporter": exporter,
                 "labels": labels}
            )
    return targets


# This gate renders a chart version it pins itself, while the installer pins its
# own in cli/src/examples.rs. Let those two drift and the gate keeps rendering
# the OLD chart: a chart upgrade that adds a new cluster-wide annotation-driven
# job would ship unbounded and unstamped with this assertion still green, which
# is precisely the regression it exists to catch. The three other upstream pins
# in this script have the same latent drift, but nothing here depends on them
# the way the scrape boundary depends on this one.
installer_source = (chart / ".." / ".." / "cli" / "src" / "examples.rs").resolve().read_text()
installer_pin = re.search(
    r'"prometheus-community/prometheus",\s*\n\s*"([^"]+)"', installer_source
)
assert installer_pin, "could not read the Prometheus chart version out of cli/src/examples.rs"
assert installer_pin.group(1) == prometheus_chart_version, (
    f"this gate renders prometheus chart {prometheus_chart_version} but the installer ships "
    f"{installer_pin.group(1)}; update PROMETHEUS_CHART_VERSION in "
    "charts/curie/ci/observability-stack-assertions.sh"
)

prometheus_second_source_docs = load_docs(prometheus_second_source_path)
second_source_services = [
    doc.get("metadata", {}).get("name")
    for doc in prometheus_second_source_docs
    if doc.get("kind") == "Service"
    and (doc.get("metadata", {}).get("annotations") or {}).get("prometheus.io/scrape") == "true"
]
assert any("kube-state-metrics" in name for name in second_source_services), (
    "the second source must render an annotated kube-state-metrics Service"
)
assert any("node-exporter" in name for name in second_source_services), (
    "the second source must render an annotated node exporter Service"
)

server_configs = [
    parsed
    for _, key, parsed, _ in embedded_yaml(prometheus_docs)
    if key == "prometheus.yml" and isinstance(parsed, dict)
]
assert len(server_configs) == 1, f"expected one Prometheus config, found {len(server_configs)}"
scrape_configs = at(server_configs[0], "scrape_configs")

if mutation == "scrape-namespace":
    for job in scrape_configs:
        for sd_config in job.get("kubernetes_sd_configs", []):
            sd_config.pop("namespaces", None)
if mutation == "scrape-source-label":
    for job in scrape_configs:
        if job["job_name"] == "kubernetes-service-endpoints":
            job.pop("metric_relabel_configs", None)
if mutation == "scrape-source-label-inert":
    # The rule is still there, still names the right label, and never fires.
    for job in scrape_configs:
        for rule in job.get("metric_relabel_configs", []):
            if rule.get("target_label") == SCRAPE_SOURCE_LABEL:
                rule["source_labels"] = ["__name__"]
                rule["regex"] = "never_matches_any_metric"
if mutation == "scrape-source-label-conditional":
    # The rule fires, but only for one metric family: a single-sample replay
    # would call this stamped.
    for job in scrape_configs:
        for rule in job.get("metric_relabel_configs", []):
            if rule.get("target_label") == SCRAPE_SOURCE_LABEL:
                rule["source_labels"] = ["__name__"]
                rule["regex"] = "kube_.*"
if mutation == "scrape-namespace-names":
    # The scope is still bounded, and it names the other stack's namespace.
    for job in scrape_configs:
        for sd_config in job.get("kubernetes_sd_configs", []):
            if sd_config.get("namespaces"):
                sd_config["namespaces"]["names"] = [SECOND_SOURCE_NAMESPACE]

sd_targets = exporter_sd_targets(prometheus_docs, RELEASE_NAMESPACE)
sd_targets += exporter_sd_targets(prometheus_second_source_docs, SECOND_SOURCE_NAMESPACE)
assert {(target["namespace"], target["exporter"]) for target in sd_targets} == {
    (RELEASE_NAMESPACE, "kube-state-metrics"),
    (RELEASE_NAMESPACE, "node-exporter"),
    (SECOND_SOURCE_NAMESPACE, "kube-state-metrics"),
    (SECOND_SOURCE_NAMESPACE, "node-exporter"),
}, "both stacks must contribute discoverable kube-state-metrics and node exporter targets"

scraped_by = {}
for job in scrape_configs:
    job_name = job["job_name"]
    # Run real samples through the rendered metric relabel program rather than
    # reading its fields back. A rule with the right target_label and the wrong
    # action, source_labels or regex passes a field check and stamps nothing.
    # Two deliberately dissimilar samples, because the claim is that EVERY
    # scraped sample is stamped: a rule conditioned on one metric name or one
    # label would satisfy a single-sample replay and leave the rest unstamped.
    for sample in (
        {
            "__name__": "kube_pod_container_status_restarts_total",
            "namespace": "kube-system",
            "pod": "coredns-0",
            "container": "coredns",
            "job": job_name,
        },
        {
            "__name__": "node_memory_MemTotal_bytes",
            "instance": "10.0.0.1:9100",
            "job": job_name,
        },
    ):
        stamped = apply_relabel(sample, job.get("metric_relabel_configs", []))
        assert stamped is not None, (
            f"scrape job {job_name} drops {sample['__name__']} in metric relabeling"
        )
        assert stamped.get(SCRAPE_SOURCE_LABEL) == SCRAPE_SOURCE_VALUE, (
            f"scrape job {job_name} must leave {SCRAPE_SOURCE_LABEL}={SCRAPE_SOURCE_VALUE} on "
            f"{sample['__name__']}, got {stamped.get(SCRAPE_SOURCE_LABEL)!r}"
        )
        assert {
            key: value for key, value in stamped.items() if key != SCRAPE_SOURCE_LABEL
        } == sample, (
            f"scrape job {job_name} must stamp the source and change nothing else about "
            f"{sample['__name__']}"
        )
    roles = {sd_config.get("role") for sd_config in job.get("kubernetes_sd_configs", [])}
    scopes = [discovery_scope(sd_config) for sd_config in job.get("kubernetes_sd_configs", [])]
    for target in sd_targets:
        if target["role"] not in roles:
            continue
        target_namespace = target["labels"]["__meta_kubernetes_namespace"]
        if not any(scope is None or target_namespace in scope for scope in scopes):
            continue
        if apply_relabel(target["labels"], job.get("relabel_configs", [])) is None:
            continue
        assert target_namespace != SECOND_SOURCE_NAMESPACE, (
            f"scrape job {job_name} reaches the pre-existing stack's {target['exporter']} "
            f"in {target_namespace}: the shipped Prometheus would double count every "
            "Kubernetes object it reports on"
        )
        scraped_by.setdefault(target["exporter"], set()).add(job_name)

for exporter in ("kube-state-metrics", "node-exporter"):
    owners = scraped_by.get(exporter, set())
    assert len(owners) == 1, (
        f"the shipped {exporter} must be scraped by exactly one job so a capacity query "
        f"returns one series per Kubernetes object, got {sorted(owners)}"
    )

# The node roles resolve one target per Node through the API server, so they
# cannot double count and must stay cluster-wide -- scoping them to the release
# namespace would blind the bot to every kubelet.
for job in scrape_configs:
    if job["job_name"] in {"kubernetes-nodes", "kubernetes-nodes-cadvisor"}:
        for sd_config in job.get("kubernetes_sd_configs", []):
            assert sd_config.get("role") == "node"
            assert discovery_scope(sd_config) is None, (
                f"{job['job_name']} must keep cluster-wide node discovery"
            )

_, alloy_pod, alloy_container = image_container(alloy_docs, "grafana/alloy", "Alloy")
assert any(volume.get("hostPath", {}).get("path") == "/var/lib/alloy" for volume in alloy_pod.get("volumes", []))
assert any(mount.get("mountPath") == "/var/lib/alloy" for mount in alloy_container.get("volumeMounts", []))
assert any(
    "stage.cri { }" in value
    for doc in alloy_docs
    if doc.get("kind") == "ConfigMap"
    for value in (doc.get("data") or {}).values()
    if isinstance(value, str)
)

tempo_statefulsets = [doc for doc in tempo_docs if doc.get("kind") == "StatefulSet"]
assert len(tempo_statefulsets) == 1, "Tempo must render one StatefulSet"
assert all(doc.get("metadata", {}).get("namespace") == "observability" for doc in tempo_docs), (
    "every Tempo resource must be installed in the observability namespace"
)
tempo_claims = at(tempo_statefulsets[0], "spec", "volumeClaimTemplates")
assert any(at(claim, "spec", "resources", "requests", "storage") == "5Gi" for claim in tempo_claims)
_, _, tempo_container = image_container(tempo_docs, "grafana/tempo", "Tempo")
if mutation == "tempo-image":
    tempo_container["image"] = tempo_container["image"].split("@", 1)[0]
assert tempo_container["image"] == (
    "docker.io/grafana/tempo:2.9.1@sha256:"
    "290414580eabd1bde91f22e4a4579242fe77377c425223f45d59b8ad1540ce3c"
), f"Tempo backend must use the reviewed multiarch digest, got {tempo_container['image']}"
tempo_configs = [parsed for _, _, parsed, _ in embedded_yaml(tempo_docs)
                 if isinstance(parsed, dict) and "distributor" in parsed and "storage" in parsed]
assert len(tempo_configs) == 1, f"expected one Tempo config, found {len(tempo_configs)}"
protocols = at(tempo_configs[0], "distributor", "receivers", "otlp", "protocols")
assert at(protocols, "grpc", "endpoint") == "0.0.0.0:4317"
assert at(protocols, "http", "endpoint") == "0.0.0.0:4318"

# --- #2059: the shipped Tempo single-pod memory and query envelope ----------
# Tempo runs as one pod under a fixed cgroup limit, but every knob it does not
# set explicitly keeps Tempo's DISTRIBUTED-deployment default. Each bound below
# is a measured value from #2059, not a preference; a "cleanup" that removes one
# restores a default sized for a fleet of queriers.
tempo_config = tempo_configs[0]
tempo_pod_template = at(tempo_statefulsets[0], "spec", "template")

if mutation == "tempo-envelope":
    # Negative control: reproduce the exact pre-#2059 shape (bounds absent, no
    # GOMEMLIMIT) so every assertion below is proved to be load bearing.
    tempo_config.get("storage", {}).get("trace", {}).pop("pool", None)
    tempo_config.get("storage", {}).get("trace", {}).pop("search", None)
    tempo_config.get("ingester", {}).pop("max_block_bytes", None)
    tempo_config.get("ingester", {}).pop("complete_block_timeout", None)
    tempo_config.pop("querier", None)
    tempo_config.pop("query_frontend", None)
    tempo_config.pop("overrides", None)
    tempo_container.pop("env", None)


def tempo_bound(path):
    """Read a dotted path out of the shipped Tempo config."""
    return at(tempo_config, *path.split("."))


# Adding a knob is one line here. Values are the measured #2059 envelope.
tempo_envelope_bounds = {
    "ingester.max_block_bytes": 52428800,
    "ingester.complete_block_timeout": "5m",
    "querier.max_concurrent_queries": 4,
    "storage.trace.pool.max_workers": 20,
    "storage.trace.pool.queue_depth": 2000,
    "storage.trace.search.read_buffer_count": 8,
    "storage.trace.search.read_buffer_size_bytes": 1048576,
    "storage.trace.search.prefetch_trace_count": 100,
    "query_frontend.max_outstanding_per_tenant": 200,
    "query_frontend.trace_by_id.query_shards": 8,
    "query_frontend.search.concurrent_jobs": 40,
    "query_frontend.search.target_bytes_per_job": 26214400,
    "query_frontend.search.max_duration": "24h",
    "query_frontend.search.max_result_limit": 50,
    "overrides.defaults.global.max_bytes_per_trace": 2000000,
    "overrides.defaults.read.max_bytes_per_tag_values_query": 1000000,
}
for tempo_key, tempo_expected in tempo_envelope_bounds.items():
    tempo_actual = tempo_bound(tempo_key)
    assert tempo_actual == tempo_expected, (
        f"Tempo envelope (#2059): {tempo_key} must be {tempo_expected!r}, "
        f"got {tempo_actual!r}"
    )

tempo_request_mi = memory_mi(at(tempo_container, "resources", "requests", "memory"))
tempo_limit_mi = memory_mi(at(tempo_container, "resources", "limits", "memory"))
tempo_limit_bytes = tempo_limit_mi * 1024 * 1024

# THE ASSERTION THAT ENCODES THE DEFECT (#2059). Tempo sizes its block-read
# buffers as max_workers * read_buffer_count * read_buffer_size_bytes. At Tempo's
# distributed defaults that is 400 * 32 * 1MiB = 12.8 GiB of worst-case read
# buffers against the 512Mi limit the stack shipped -- 2560% of the pod, which is
# why ordinary operator reads OOM-killed it (exit 137). Bounded it is
# 20 * 8 * 1MiB = 160 MiB, 15.6% of the 1Gi limit. Raising the limit alone would
# not close this: no small-node ceiling is above 12.8 GiB, so the product must be
# bounded RELATIVE to the limit, not merely below some fixed number.
tempo_read_buffer_bytes = (
    tempo_bound("storage.trace.pool.max_workers")
    * tempo_bound("storage.trace.search.read_buffer_count")
    * tempo_bound("storage.trace.search.read_buffer_size_bytes")
)
tempo_read_buffer_ceiling = tempo_limit_bytes / 4
assert tempo_read_buffer_bytes <= tempo_read_buffer_ceiling, (
    "Tempo worst-case read buffers (#2059) must stay within 25% of "
    f"resources.limits.memory: max_workers * read_buffer_count * "
    f"read_buffer_size_bytes = {tempo_read_buffer_bytes} bytes vs a ceiling of "
    f"{tempo_read_buffer_ceiling} bytes (25% of {tempo_limit_mi}Mi), which is "
    f"{tempo_read_buffer_bytes / tempo_limit_bytes * 100:.1f}% of the limit"
)


def go_memory_bytes(text):
    """Parse a Go GOMEMLIMIT quantity (Go accepts only binary suffixes)."""
    match = re.fullmatch(r"([0-9]+)(B|KiB|MiB|GiB|TiB)?", str(text))
    assert match, f"unsupported GOMEMLIMIT quantity {text!r}"
    factors = {"": 1, "B": 1, "KiB": 1024, "MiB": 1024**2,
               "GiB": 1024**3, "TiB": 1024**4}
    return Decimal(match.group(1)) * factors[match.group(2) or ""]


# GOMEMLIMIT is a SOFT Go heap ceiling; the cgroup limit is the hard one. It must
# sit below the cgroup limit with real headroom because it governs only the Go
# heap -- goroutine stacks, runtime overhead and the mmap'd block reads Tempo
# does on the search path are outside it. 80% is that headroom (#2059).
tempo_env = {
    entry.get("name"): entry.get("value")
    for entry in tempo_container.get("env", [])
    if isinstance(entry, dict)
}
assert "GOMEMLIMIT" in tempo_env, (
    "Tempo container must set GOMEMLIMIT (#2059) so the Go GC learns the cgroup "
    f"ceiling instead of being killed at it, env has {sorted(tempo_env)}"
)
tempo_gomemlimit_bytes = go_memory_bytes(tempo_env["GOMEMLIMIT"])
assert tempo_gomemlimit_bytes < tempo_limit_bytes, (
    f"GOMEMLIMIT {tempo_env['GOMEMLIMIT']} ({tempo_gomemlimit_bytes} bytes) must "
    f"be strictly below resources.limits.memory ({tempo_limit_bytes} bytes)"
)
assert tempo_gomemlimit_bytes <= Decimal("0.80") * tempo_limit_bytes, (
    f"GOMEMLIMIT {tempo_env['GOMEMLIMIT']} ({tempo_gomemlimit_bytes} bytes) must "
    f"leave non-heap headroom: at most 80% of resources.limits.memory "
    f"({Decimal('0.80') * tempo_limit_bytes} bytes), got "
    f"{tempo_gomemlimit_bytes / tempo_limit_bytes * 100:.1f}%"
)

assert tempo_request_mi <= tempo_limit_mi, (
    f"Tempo resources.requests.memory ({tempo_request_mi}Mi) must not exceed "
    f"resources.limits.memory ({tempo_limit_mi}Mi)"
)

# The server-side search bound mirrors the shipped connector's own clamp, turning
# a client-side courtesy into a boundary that also binds a direct Grafana Explore
# query. Read the connector rather than restating 50 here: if MAX_LIMIT moves,
# this fails instead of silently disagreeing (#2059).
tempo_connector_source = (assets.parent / "connectors" / "tempo" / "server.py").read_text()
tempo_max_limit_match = re.search(
    r"^MAX_LIMIT\s*=\s*([0-9]+)\s*$", tempo_connector_source, re.MULTILINE
)
assert tempo_max_limit_match, (
    "could not read MAX_LIMIT from examples/sre-bot/connectors/tempo/server.py"
)
tempo_connector_max_limit = int(tempo_max_limit_match.group(1))
assert tempo_bound("query_frontend.search.max_result_limit") == tempo_connector_max_limit, (
    "Tempo query_frontend.search.max_result_limit must equal the shipped "
    f"connector's MAX_LIMIT ({tempo_connector_max_limit}) in "
    "examples/sre-bot/connectors/tempo/server.py, got "
    f"{tempo_bound('query_frontend.search.max_result_limit')} -- the two move together"
)

# kubectl apply of a changed ConfigMap updates the map and leaves the StatefulSet
# pod running its old config. Without a changed pod-template annotation nothing
# rolls, so every assertion above passes while the live Tempo keeps the
# unbounded defaults: a green change that fixes nothing (#2059).
tempo_config_checksum = at(tempo_pod_template, "metadata", "annotations", "checksum/config")
assert tempo_config_checksum != "v1", (
    "Tempo pod template checksum/config must be bumped off 'v1' so the "
    "StatefulSet actually rolls onto the bounded ConfigMap (#2059)"
)
# --- end #2059 envelope -----------------------------------------------------

datasources = []
for _, _, parsed, _ in embedded_yaml(grafana_docs):
    for candidate in find_nested_dicts(parsed, "datasources"):
        if isinstance(candidate, list):
            datasources.extend(item for item in candidate if isinstance(item, dict))
assert len(datasources) == 3, f"Grafana must provision exactly three datasources, got {len(datasources)}"
assert {item.get("type") for item in datasources} == {"loki", "tempo", "prometheus"}
tempo_datasources = [item for item in datasources if item.get("name") == "Tempo"]
assert len(tempo_datasources) == 1
assert tempo_datasources[0].get("uid") == "__Tempo__"
assert sum(item.get("uid") == "__Tempo__" for item in datasources) == 1
expected_urls = {
    "Loki": "http://loki.observability.svc.cluster.local:3100",
    "Tempo": "http://tempo.observability.svc.cluster.local:3200",
    "Prometheus": "http://prometheus-server.observability.svc.cluster.local",
}
assert {item.get("name"): item.get("url") for item in datasources} == expected_urls


def collector_config(docs):
    matches = []
    for doc, key, parsed, _ in embedded_yaml(docs):
        if key == "collector-config.yaml" and isinstance(parsed, dict):
            matches.append(parsed)
    assert len(matches) == 1, f"expected one collector config, found {len(matches)}"
    return matches[0]


for label, docs in (("install", curie_install_docs), ("upgrade", curie_upgrade_docs)):
    config = collector_config(docs)
    if mutation == "tempo-exporter" and label == "upgrade":
        config.get("exporters", {}).pop("otlphttp/tempo", None)
        exporters = at(config, "service", "pipelines", "traces", "exporters")
        config["service"]["pipelines"]["traces"]["exporters"] = [
            value for value in exporters if value != "otlphttp/tempo"
        ]
    tempo_exporter = at(config, "exporters", "otlphttp/tempo")
    assert tempo_exporter.get("endpoint") == "http://tempo.observability.svc.cluster.local:4318"
    retry = at(tempo_exporter, "retry_on_failure")
    assert retry.get("enabled") is True
    assert retry.get("max_interval")
    assert retry.get("max_elapsed_time") not in (None, "0", "0s")
    queue = at(tempo_exporter, "sending_queue")
    assert queue.get("enabled") is True
    assert queue.get("storage") == "file_storage"
    assert isinstance(queue.get("queue_size"), int) and 0 < queue["queue_size"] <= 100000
    trace_exporters = at(config, "service", "pipelines", "traces", "exporters")
    assert "debug" not in config.get("exporters", {}), (
        f"{label} production render must omit the debug exporter"
    )
    assert trace_exporters == ["otlphttp/langfuse", "otlphttp/tempo"], (
        f"{label} production render must export traces to Langfuse and Tempo without debug"
    )
    soak_exporter = at(config, "exporters", "prometheusremotewrite/soak")
    assert soak_exporter.get("endpoint") == (
        "http://prometheus-server.observability.svc.cluster.local/api/v1/write"
    )
    assert (soak_exporter.get("resource_to_telemetry_conversion") or {}).get("enabled") is False
    assert at(soak_exporter, "retry_on_failure", "enabled") is True
    assert "sending_queue" not in soak_exporter
    assert at(soak_exporter, "remote_write_queue", "enabled") is True
    assert 0 < int(at(soak_exporter, "remote_write_queue", "queue_size")) <= 100000
    metric_exporters = at(config, "service", "pipelines", "metrics", "exporters")
    assert "nop/metrics" in metric_exporters, (
        f"{label} must keep nop/metrics; the overlay appends, it does not replace"
    )
    assert "prometheusremotewrite/soak" in metric_exporters, (
        f"{label} must retain Curie metrics on prometheusremotewrite/soak"
    )

default_config = collector_config(curie_default_docs)
assert at(default_config, "service", "pipelines", "metrics", "exporters") == ["nop/metrics"], (
    "chart default must keep metrics on nop until an overlay adds a destination"
)
assert "prometheusremotewrite/soak" not in default_config.get("exporters", {}), (
    "chart default must not ship the soak Prometheus remote-write exporter"
)

assert at(curie_values, "otelCollector", "extraMetricPipelineExporters") == [
    "prometheusremotewrite/soak"
]
assert at(prometheus_values, "server", "extraArgs") == {
    "web.enable-remote-write-receiver": ""
}
rendered_prom_args = [
    argument
    for _, _, container in containers(prometheus_docs)
    for argument in container.get("args", [])
]
assert "--web.enable-remote-write-receiver" in rendered_prom_args, (
    "Prometheus must enable the remote-write receiver in the rendered server"
)

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
alert_rules = []
for group in at(prometheus_values, "serverFiles", "alerting_rules.yml", "groups"):
    alert_rules.extend(group.get("rules") or [])
alert_names = {rule["alert"] for rule in alert_rules if "alert" in rule}
missing_alerts = sorted(REQUIRED_ALERTS - alert_names)
assert not missing_alerts, f"prometheus-values.yaml missing alerts: {missing_alerts}"
alert_dump = yaml.safe_dump(alert_rules).lower()
for forbidden in (
    "run_id", "session_id", "event.id", "user_id", "sandbox_id", "deployment_id",
    "message_id", "password", "api_key",
):
    assert forbidden not in alert_dump, (
        f"alert rules must not carry {forbidden!r} as a metric-label selector"
    )
assert "absent(" in next(
    rule["expr"] for rule in alert_rules if rule.get("alert") == "CurieApplicationMetricsAbsent"
)
duplicate_expr = next(
    rule["expr"] for rule in alert_rules if rule.get("alert") == "CurieDuplicateNodeExporter"
)
assert "count(" in duplicate_expr and "9100" in duplicate_expr and "9101" in duplicate_expr
assert "sum(" not in duplicate_expr.replace(" ", "")

default_render = Path(curie_default_path).read_text()
assert "GRAFANA_SERVICE_ACCOUNT_TOKEN" not in default_render, (
    "Grafana connector token integration must stay opt in"
)

token_secret_name = at(curie_values, "grafanaConnector", "secretName")
token_secret_key = at(curie_values, "grafanaConnector", "secretKey")
for label, docs in (("install", curie_install_docs), ("upgrade", curie_upgrade_docs)):
    token_secrets = [
        doc for doc in docs
        if doc.get("kind") == "Secret"
        and doc.get("metadata", {}).get("name") == token_secret_name
    ]
    assert len(token_secrets) == 1, (
        f"expected one chart managed Grafana token Secret on {label}, "
        f"found {len(token_secrets)}"
    )
    token_secret = token_secrets[0]
    if mutation == "token-data" and label == "upgrade":
        token_secret["data"] = {token_secret_key: "observability-token-sentinel"}
    assert not hook_annotations(token_secret), "Grafana token Secret must be a regular Helm resource"
    assert "data" not in token_secret and "stringData" not in token_secret, (
        f"Grafana token material must not enter the Helm {label} release manifest"
    )

jobs = [doc for doc in curie_install_docs if doc.get("kind") == "Job"]
updaters = []
for job in jobs:
    container_dump = yaml.safe_dump(at(job, "spec", "template", "spec", "containers"))
    if token_secret_name in container_dump and "api/serviceaccounts" in container_dump:
        updaters.append(job)
assert len(updaters) == 1, f"expected one Grafana token updater Job, found {len(updaters)}"
updater = updaters[0]
assert set(hook_annotations(updater).split(",")) == {"post-install", "post-upgrade"}
updater_container = at(updater, "spec", "template", "spec", "containers")[0]
updater_command_parts = updater_container.get("command", [])
updater_command = "\n".join(str(part) for part in updater_command_parts)
updater_source = updater_command_parts[-1]
compile(updater_source, "<grafana-token-updater>", "exec")
if mutation == "updater-tag":
    updater_container["image"] = updater_container["image"].split("@", 1)[0]
updater_image = updater_container["image"]
assert updater_image == (
    "python:3.12-alpine@sha256:"
    "d09d15e60962ca365d1cd544a48773bac9d33f2fb1b00f2aa0deec78ade7dc31"
), f"Grafana token updater must use the reviewed image digest, got {updater_image}"
assert "set -x" not in updater_command
for sensitive_name in ("existing_token", "new_token", "admin_password", "kube_token"):
    assert f"print({sensitive_name}" not in updater_command
assert not re.search(
    r"(?:echo|printf)[^\n]*\$(?:\{?[A-Za-z_][A-Za-z0-9_]*(?:TOKEN|KEY))",
    updater_command,
), (
    "Grafana token updater must never print token variables"
)
for required_api in ("/api/health", "/api/serviceaccounts"):
    assert required_api in updater_command, f"Grafana updater is missing {required_api} handling"
assert "sleep" in updater_command, "Grafana updater must poll readiness instead of racing startup"
if mutation == "token-cleanup":
    updater_command = updater_command.replace('method="DELETE"', 'method="GET"')
assert 'method="DELETE"' in updater_command, "Grafana updater must revoke stale tokens"
assert "status, body = grafana_request(tokens_path, basic=admin)" in updater_command, (
    "Grafana updater must list existing service account tokens before creating one"
)
assert "token_records = json.loads(body)" in updater_command
assert ".startswith(token_prefix)" in updater_command, (
    "Grafana updater must limit cleanup to its configured token prefix"
)
assert updater_command.count("delete_token(new_token_id)") >= 2, (
    "Grafana updater must revoke a newly created token on incomplete output or Secret PATCH failure"
)
assert "Grafana connector token is valid and was reused" not in updater_command, (
    "Grafana updater must not accept an unbound bearer from the Kubernetes Secret"
)
if mutation == "rotation-restart":
    updater_command = updater_command.replace(
        "/apis/apps/v1/namespaces/{encoded_namespace}/deployments/{encoded_name}",
        "/apis/apps/v1/namespaces/{encoded_namespace}/statefulsets/{encoded_name}",
    )
assert "/apis/apps/v1/namespaces/{encoded_namespace}/deployments/{encoded_name}" in updater_command, (
    "Grafana token rotation must restart the scoped connector Deployments"
)
for rollout_field in (
    "observedGeneration",
    "updatedReplicas",
    "readyReplicas",
    "availableReplicas",
    "unavailableReplicas",
):
    assert rollout_field in updater_command, (
        f"Grafana token rotation must wait for Deployment {rollout_field}"
    )
assert updater_command.rindex("patch_token_secret(new_token)") < updater_command.rindex(
    "restart_connector_deployments()"
) < updater_command.rindex("delete_token(stale_token_id)"), (
    "Grafana token rotation must patch the Secret, roll every connector, then revoke old tokens"
)
if mutation == "viewer-role":
    updater_command = updater_command.replace(
        'payload={"role": "Viewer"}', 'payload={"role": "Editor"}'
    )
assert 'account.get("role") != "Viewer"' in updater_command, (
    "Grafana updater must detect a dedicated service account whose role drifted"
)
assert 'payload={"role": "Viewer"}' in updater_command, (
    "Grafana updater must restore the dedicated service account to Viewer"
)
assert updater_command.index("/api/serviceaccounts/search") < updater_command.rindex(
    "patch_token_secret(new_token)"
), "Grafana updater must constrain the Viewer account before publishing its new token"

admin_volumes = [
    volume for volume in at(updater, "spec", "template", "spec", "volumes")
    if volume.get("name") == "grafana-admin"
]
assert len(admin_volumes) == 1
assert at(admin_volumes[0], "secret", "secretName") == "grafana-admin"

service_account_name = at(updater, "spec", "template", "spec", "serviceAccountName")
service_accounts = [doc for doc in curie_install_docs if doc.get("kind") == "ServiceAccount"
                    and doc.get("metadata", {}).get("name") == service_account_name]
assert len(service_accounts) == 1

role_bindings = [doc for doc in curie_install_docs if doc.get("kind") == "RoleBinding"
                 and any(subject.get("name") == service_account_name for subject in doc.get("subjects", []))]
assert len(role_bindings) == 1
role_name = at(role_bindings[0], "roleRef", "name")
roles = [doc for doc in curie_install_docs if doc.get("kind") == "Role"
         and doc.get("metadata", {}).get("name") == role_name]
assert len(roles) == 1
restart_deployments = set(at(curie_values, "grafanaConnector", "restartDeploymentNames"))
assert restart_deployments == {"curie-sre-bot-grafana", "curie-sre-bot-tempo"}
if mutation == "restart-scope":
    for rule in roles[0].get("rules", []):
        if "deployments" in rule.get("resources", []):
            rule["resourceNames"] = ["*"]
for resource in (service_accounts[0], role_bindings[0], roles[0]):
    assert set(hook_annotations(resource).split(",")) == {"post-install", "post-upgrade"}
for rule in roles[0].get("rules", []):
    assert "*" not in rule.get("resources", [])
    assert "*" not in rule.get("verbs", [])
    assert set(rule.get("resources", [])) <= {"secrets", "deployments"}
    if "secrets" in rule.get("resources", []):
        assert token_secret_name in rule.get("resourceNames", [])
        assert set(rule.get("verbs", [])) == {"get", "patch", "update"}
    if "deployments" in rule.get("resources", []):
        assert set(rule.get("resourceNames", [])) == restart_deployments, (
            "Grafana updater Deployment access must be limited to the two SRE bot connectors"
        )
        assert set(rule.get("verbs", [])) == {"get", "patch"}

cleanup_jobs = [job for job in jobs if hook_annotations(job) == "pre-delete"]
assert len(cleanup_jobs) == 1, f"expected one pre-delete Grafana cleanup Job, found {len(cleanup_jobs)}"
cleanup = cleanup_jobs[0]
cleanup_pod = at(cleanup, "spec", "template", "spec")
assert cleanup_pod.get("automountServiceAccountToken") is False
assert "serviceAccountName" not in cleanup_pod
cleanup_container = at(cleanup_pod, "containers")[0]
if mutation == "cleanup-tag":
    cleanup_container["image"] = cleanup_container["image"].split("@", 1)[0]
assert cleanup_container["image"] == updater_image, (
    "Grafana pre-delete cleanup must use the reviewed updater digest"
)
cleanup_source = cleanup_container.get("command", [])[-1]
compile(cleanup_source, "<grafana-token-cleanup>", "exec")
if mutation == "delete-hook":
    cleanup_source = cleanup_source.replace('method="DELETE"', 'method="GET"')
for required_source in (
    "/api/serviceaccounts/search",
    "/api/serviceaccounts/{account_id}/tokens",
    'method="DELETE"',
    ".startswith(token_prefix)",
):
    assert required_source in cleanup_source, (
        f"Grafana pre-delete cleanup is missing {required_source}"
    )
assert "set -x" not in cleanup_source
assert not re.search(
    r"(?:echo|printf)[^\n]*\$(?:\{?[A-Za-z_][A-Za-z0-9_]*(?:TOKEN|KEY))",
    cleanup_source,
)
assert not any(
    name.startswith("KUBERNETES_") or name.startswith("TOKEN_SECRET_")
    for name in (
        item.get("name", "")
        for item in cleanup_container.get("env", [])
    )
)
assert at(cleanup_pod, "securityContext", "runAsNonRoot") is True
assert at(cleanup_pod, "securityContext", "runAsUser") == 65532
assert at(cleanup_pod, "securityContext", "runAsGroup") == 65532
assert at(cleanup_pod, "securityContext", "fsGroup") == 65532
assert at(cleanup_pod, "securityContext", "seccompProfile", "type") == "RuntimeDefault"
assert at(cleanup_container, "securityContext", "runAsNonRoot") is True
assert at(cleanup_container, "securityContext", "runAsUser") == 65532
assert at(cleanup_container, "securityContext", "runAsGroup") == 65532
assert at(cleanup_container, "securityContext", "allowPrivilegeEscalation") is False
assert at(cleanup_container, "securityContext", "readOnlyRootFilesystem") is True
assert at(cleanup_container, "securityContext", "capabilities", "drop") == ["ALL"]
assert at(cleanup_container, "securityContext", "seccompProfile", "type") == "RuntimeDefault"
for sensitive_name in ("admin", "token_id", "token_record"):
    assert f"print({sensitive_name}" not in cleanup_source
cleanup_admin_volumes = [
    volume for volume in cleanup_pod.get("volumes", [])
    if volume.get("name") == "grafana-admin"
]
assert len(cleanup_admin_volumes) == 1
assert at(cleanup_admin_volumes[0], "secret", "secretName") == "grafana-admin"
assert not any(
    hook_annotations(doc) == "pre-delete"
    for doc in curie_install_docs
    if doc.get("kind") in {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding"}
), "Grafana pre-delete cleanup must not receive Kubernetes RBAC"

token_template_source = (chart / "templates" / "grafana-connector-token.yaml").read_text()
assert 'lookup "v1" "Secret"' not in token_template_source, (
    "Grafana token must never be copied into a Helm release manifest"
)

full_render = "\n".join(Path(path).read_text() for path in (
    grafana_path, loki_path, alloy_path, prometheus_path, tempo_path,
    curie_install_path, curie_upgrade_path,
))
assert "observability-token-sentinel" not in full_render
assert not re.search(r"glsa_[A-Za-z0-9_-]{16,}", full_render), (
    "rendered manifests contain literal Grafana service account token material"
)

print("observability stack render assertions passed")
PY

# Exercise alert firing and recovery through promtool, not just YAML presence.
PROM_VERSION="${PROMTOOL_VERSION:-3.14.0}"
promtool_bin="${PROMTOOL:-}"
if [[ -z "$promtool_bin" ]] && command -v promtool >/dev/null 2>&1; then
  promtool_bin="$(command -v promtool)"
fi
if [[ -z "$promtool_bin" || ! -x "$promtool_bin" ]]; then
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) fail "unsupported architecture for promtool: $arch" ;;
  esac
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  tarball="prometheus-${PROM_VERSION}.${os}-${arch}.tar.gz"
  curl -fsSL "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${tarball}" \
    | tar -xz -C "$TMP" --strip-components=1 "prometheus-${PROM_VERSION}.${os}-${arch}/promtool"
  promtool_bin="$TMP/promtool"
fi
[[ -x "$promtool_bin" ]] || fail "promtool is not executable"

python3 - "$ASSETS/prometheus-values.yaml" "$TMP/reliability-alerts.rules.yaml" <<'PY'
from pathlib import Path
import sys
import yaml

values = yaml.safe_load(Path(sys.argv[1]).read_text())
rules = values["serverFiles"]["alerting_rules.yml"]
Path(sys.argv[2]).write_text(yaml.safe_dump(rules, sort_keys=False))
PY
cp "$ASSETS/reliability-alerts.test.yaml" "$TMP/reliability-alerts.test.yaml"
(
  cd "$TMP"
  "$promtool_bin" check rules reliability-alerts.rules.yaml
  "$promtool_bin" test rules reliability-alerts.test.yaml
)
echo "PASS: reliability alerts render, check, fire, and recover under promtool"

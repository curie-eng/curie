#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
ASSETS="$REPO_ROOT/examples/sre-bot/observability"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

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
  --version 29.27.0 \
  --namespace observability \
  -f "$ASSETS/prometheus-values.yaml" >"$TMP/prometheus.yaml"

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
  "$ASSETS/tempo.yaml" \
  "$TMP/curie-install.yaml" \
  "$TMP/curie-upgrade.yaml" \
  "$TMP/curie-default.yaml" \
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
    tempo_path,
    curie_install_path,
    curie_upgrade_path,
    curie_default_path,
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
    "Tempo": (tempo_docs, "grafana/tempo", Decimal(192)),
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
assert request_total == 1312, f"observability request total must be 1312Mi, got {request_total}Mi"

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

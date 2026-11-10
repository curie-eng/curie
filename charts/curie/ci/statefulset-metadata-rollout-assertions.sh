#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

copy_with_metadata() {
  local source="$1" destination="$2" field="$3" value="$4"
  cp -a "$source" "$destination"
  python3 - "$destination/Chart.yaml" "$field" "$value" <<'PY'
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
field = sys.argv[2]
value = sys.argv[3]
chart = path.read_text()
updated, count = re.subn(
    rf"^({re.escape(field)}:\s*).*?$",
    rf'\g<1>"{value}"',
    chart,
    flags=re.MULTILINE,
)
if count != 1:
    raise SystemExit(f"FAIL: expected exactly one Chart.yaml {field} field, found {count}")
if updated == chart:
    raise SystemExit(f"FAIL: metadata mutation left Chart.yaml {field} unchanged")
path.write_text(updated)
PY
}

render() {
  local name="$1" chart="$2"
  shift 2
  # Independent offline renders cannot retain generated credentials through
  # lookup. Keep Secret content deterministic so complete pod-template
  # comparisons measure metadata and intentional input changes, including
  # checksum/secret, rather than fresh sealed-install randomness.
  helm template rel "$chart" --set security.allowDevDefaults=true \
    --output-dir "$TMP/render-$name" "$@" >/dev/null
}

CHART_VERSION_COPY="$TMP/chart-version"
APP_VERSION_COPY="$TMP/app-version"
copy_with_metadata "$CHART" "$CHART_VERSION_COPY" version "99.99.98-metadata-rollout"
copy_with_metadata "$CHART" "$APP_VERSION_COPY" appVersion "99.99.97-metadata-rollout"

render baseline "$CHART"
render chart-version "$CHART_VERSION_COPY"
render app-version "$APP_VERSION_COPY"
render image-clickhouse "$CHART" \
  --set-string clickhouse.image.repository=example.com/curie-clickhouse
render image-postgres "$CHART" \
  --set-string postgres.image=example.com/curie-postgres:test
render image-rustfs "$CHART" \
  --set-string rustfs.image=example.com/curie-rustfs:test
render image-valkey "$CHART" \
  --set-string valkey.image=example.com/curie-valkey:test
render placement "$CHART" \
  --set-string 'placement.data.podLabels.metadata\.rollout\.test=enabled'
render clickhouse-config "$CHART" \
  --set-string clickhouse.logLevel=error

python3 - \
  "$CHART/Chart.yaml" \
  "$CHART_VERSION_COPY/Chart.yaml" \
  "$APP_VERSION_COPY/Chart.yaml" \
  "$TMP/render-baseline" \
  "$TMP/render-chart-version" \
  "$TMP/render-app-version" \
  "$TMP/render-image-clickhouse" \
  "$TMP/render-image-postgres" \
  "$TMP/render-image-rustfs" \
  "$TMP/render-image-valkey" \
  "$TMP/render-placement" \
  "$TMP/render-clickhouse-config" <<'PY'
import json
from pathlib import Path
import sys

import yaml

COMPONENTS = ("clickhouse", "postgres", "rustfs", "valkey")
SELECTOR_KEYS = {
    "app.kubernetes.io/name",
    "app.kubernetes.io/instance",
    "app.kubernetes.io/component",
}
RECOMMENDED_LABELS = (
    "app.kubernetes.io/name",
    "app.kubernetes.io/instance",
    "app.kubernetes.io/managed-by",
)


def require(condition, message):
    if not condition:
        raise SystemExit(f"FAIL: {message}")


def chart_metadata(path):
    return yaml.safe_load(Path(path).read_text())


def load_statefulsets(render_dir):
    documents = []
    for path in sorted(Path(render_dir).rglob("*.yaml")):
        with path.open() as stream:
            documents.extend(doc for doc in yaml.safe_load_all(stream) if doc)
    statefulsets = [doc for doc in documents if doc.get("kind") == "StatefulSet"]
    require(
        len(statefulsets) == len(COMPONENTS),
        f"{render_dir}: expected exactly four StatefulSets, found {len(statefulsets)}",
    )
    by_component = {}
    for statefulset in statefulsets:
        selector = statefulset.get("spec", {}).get("selector", {}).get("matchLabels") or {}
        component = selector.get("app.kubernetes.io/component")
        require(
            component in COMPONENTS,
            f"{render_dir}: StatefulSet has unexpected component selector {component!r}",
        )
        require(
            component not in by_component,
            f"{render_dir}: more than one StatefulSet selected component {component!r}",
        )
        by_component[component] = statefulset
    require(
        set(by_component) == set(COMPONENTS),
        f"{render_dir}: StatefulSet components were {sorted(by_component)}",
    )
    return by_component


def template_bytes(statefulset):
    return json.dumps(
        statefulset["spec"]["template"],
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def changed_paths(before, after, path="spec.template"):
    if type(before) is not type(after):
        return [path]
    if isinstance(before, dict):
        paths = []
        for key in sorted(set(before) | set(after)):
            child = f"{path}.{key}"
            if key not in before or key not in after:
                paths.append(child)
            else:
                paths.extend(changed_paths(before[key], after[key], child))
        return paths
    if isinstance(before, list):
        if len(before) != len(after):
            return [path]
        paths = []
        for index, (left, right) in enumerate(zip(before, after)):
            paths.extend(changed_paths(left, right, f"{path}[{index}]"))
        return paths
    return [] if before == after else [path]


def require_stable_templates(baseline, changed, label):
    for component in COMPONENTS:
        left = baseline[component]["spec"]["template"]
        right = changed[component]["spec"]["template"]
        if template_bytes(baseline[component]) != template_bytes(changed[component]):
            paths = ", ".join(changed_paths(left, right))
            require(
                False,
                f"{component}: {label} changed canonical spec.template bytes; "
                f"changed paths: {paths}",
            )


def expected_chart_label(metadata):
    return f"{metadata['name']}-{metadata['version']}".replace("+", "_")


def check_structure(render_name, statefulsets, metadata):
    for component, statefulset in statefulsets.items():
        labels = statefulset.get("metadata", {}).get("labels") or {}
        for key in RECOMMENDED_LABELS:
            require(labels.get(key), f"{render_name} {component}: top level label {key} is missing")
        require(
            labels["app.kubernetes.io/name"] == metadata["name"],
            f"{render_name} {component}: top level name label is not the chart name",
        )
        require(
            labels["app.kubernetes.io/instance"] == "rel",
            f"{render_name} {component}: top level instance label is not the release name",
        )
        require(
            labels["app.kubernetes.io/managed-by"] == "Helm",
            f"{render_name} {component}: top level managed-by label is not Helm",
        )
        require(
            labels.get("helm.sh/chart") == expected_chart_label(metadata),
            f"{render_name} {component}: top level chart label does not match Chart.yaml",
        )
        require(
            labels.get("app.kubernetes.io/version") == str(metadata["appVersion"]),
            f"{render_name} {component}: top level version label does not match Chart.yaml",
        )

        selector = statefulset["spec"]["selector"]["matchLabels"]
        require(
            set(selector) == SELECTOR_KEYS,
            f"{render_name} {component}: selector keys were {sorted(selector)}",
        )
        require(
            selector["app.kubernetes.io/component"] == component,
            f"{render_name} {component}: selector component changed",
        )
        pod_labels = statefulset["spec"]["template"]["metadata"].get("labels") or {}
        for key, value in selector.items():
            require(
                pod_labels.get(key) == value,
                f"{render_name} {component}: pod label {key} does not satisfy the selector",
            )
        require(
            pod_labels.get("app.kubernetes.io/managed-by") == "Helm",
            f"{render_name} {component}: pod managed-by label is missing",
        )
        require(
            "helm.sh/chart" not in pod_labels,
            f"{render_name} {component}: pod labels retain release chart metadata",
        )
        require(
            "app.kubernetes.io/version" not in pod_labels,
            f"{render_name} {component}: pod labels retain release application metadata",
        )


def require_top_level_updates(baseline, changed, changed_key, stable_key, label):
    for component in COMPONENTS:
        before = baseline[component]["metadata"]["labels"]
        after = changed[component]["metadata"]["labels"]
        require(
            before[changed_key] != after[changed_key],
            f"{component}: {label} did not update top level {changed_key}",
        )
        require(
            before[stable_key] == after[stable_key],
            f"{component}: {label} unexpectedly changed top level {stable_key}",
        )
        for key in RECOMMENDED_LABELS:
            require(
                before[key] == after[key],
                f"{component}: {label} changed stable top level label {key}",
            )


def require_only_target_changed(baseline, changed, target, label):
    actual = {
        component
        for component in COMPONENTS
        if template_bytes(baseline[component]) != template_bytes(changed[component])
    }
    require(actual == {target}, f"{label}: changed StatefulSet templates {sorted(actual)}")


base_meta, chart_meta, app_meta = map(chart_metadata, sys.argv[1:4])
(
    baseline,
    chart_version,
    app_version,
    image_clickhouse,
    image_postgres,
    image_rustfs,
    image_valkey,
    placement,
    clickhouse_config,
) = map(load_statefulsets, sys.argv[4:13])

# Compare the canonical controller input before checking helper details. On the
# unfixed chart this reports the release label path that changes the template.
require_stable_templates(baseline, chart_version, "chart version only bump")
require_stable_templates(baseline, app_version, "appVersion only bump")

check_structure("baseline", baseline, base_meta)
check_structure("chart version", chart_version, chart_meta)
check_structure("appVersion", app_version, app_meta)
require_top_level_updates(
    baseline,
    chart_version,
    "helm.sh/chart",
    "app.kubernetes.io/version",
    "chart version only bump",
)
require_top_level_updates(
    baseline,
    app_version,
    "app.kubernetes.io/version",
    "helm.sh/chart",
    "appVersion only bump",
)

require_only_target_changed(baseline, image_clickhouse, "clickhouse", "ClickHouse image override")
require_only_target_changed(baseline, image_postgres, "postgres", "Postgres image override")
require_only_target_changed(baseline, image_rustfs, "rustfs", "RustFS image override")
require_only_target_changed(baseline, image_valkey, "valkey", "Valkey image override")

placement_changed = {
    component
    for component in COMPONENTS
    if template_bytes(baseline[component]) != template_bytes(placement[component])
}
require(
    placement_changed == set(COMPONENTS),
    f"data placement label changed templates {sorted(placement_changed)}",
)
for component in COMPONENTS:
    pod_labels = placement[component]["spec"]["template"]["metadata"]["labels"]
    require(
        pod_labels.get("metadata.rollout.test") == "enabled",
        f"{component}: data placement label was not rendered",
    )

require_only_target_changed(
    baseline,
    clickhouse_config,
    "clickhouse",
    "ClickHouse logging configuration override",
)
before_checksum = baseline["clickhouse"]["spec"]["template"]["metadata"]["annotations"].get(
    "checksum/config"
)
after_checksum = clickhouse_config["clickhouse"]["spec"]["template"]["metadata"]["annotations"].get(
    "checksum/config"
)
require(before_checksum, "ClickHouse baseline checksum/config is missing")
require(after_checksum, "ClickHouse changed checksum/config is missing")
require(before_checksum != after_checksum, "ClickHouse logging config did not update checksum/config")

print("StatefulSet metadata and intentional rollout assertions: OK")
PY

# This mutation runs only after the positive contract succeeds. That lets the
# unfixed chart fail on its release labels before the new helper exists.
MUTANT="$TMP/mutant"
cp -a "$CHART" "$MUTANT"
python3 - "$MUTANT" <<'PY'
from pathlib import Path
import sys

chart = Path(sys.argv[1])
needle = 'include "curie.statefulPodLabels" . | nindent 8'
replacement = 'include "curie.labels" . | nindent 8'
paths = [
    chart / "templates" / "clickhouse.yaml",
    chart / "templates" / "postgres.yaml",
    chart / "templates" / "rustfs.yaml",
    chart / "templates" / "valkey.yaml",
]
count = sum(path.read_text().count(needle) for path in paths)
if count != 4:
    raise SystemExit(f"FAIL: revert mutant expected exactly four stable helper sites, found {count}")
for path in paths:
    path.write_text(path.read_text().replace(needle, replacement))
PY

MUTANT_CHART_VERSION="$TMP/mutant-chart-version"
MUTANT_APP_VERSION="$TMP/mutant-app-version"
copy_with_metadata "$MUTANT" "$MUTANT_CHART_VERSION" version "99.99.98-metadata-rollout"
copy_with_metadata "$MUTANT" "$MUTANT_APP_VERSION" appVersion "99.99.97-metadata-rollout"
render mutant-baseline "$MUTANT"
render mutant-chart-version "$MUTANT_CHART_VERSION"
render mutant-app-version "$MUTANT_APP_VERSION"

python3 - \
  "$TMP/render-mutant-baseline" \
  "$TMP/render-mutant-chart-version" \
  "$TMP/render-mutant-app-version" <<'PY'
import json
from pathlib import Path
import sys

import yaml

COMPONENTS = {"clickhouse", "postgres", "rustfs", "valkey"}


def load(render_dir):
    found = {}
    statefulset_count = 0
    for path in sorted(Path(render_dir).rglob("*.yaml")):
        with path.open() as stream:
            for document in yaml.safe_load_all(stream):
                if not document or document.get("kind") != "StatefulSet":
                    continue
                statefulset_count += 1
                selector = document["spec"]["selector"]["matchLabels"]
                component = selector.get("app.kubernetes.io/component")
                if component not in COMPONENTS:
                    raise SystemExit(
                        f"FAIL: revert mutant found unexpected StatefulSet component {component!r}"
                    )
                if component in found:
                    raise SystemExit(
                        f"FAIL: revert mutant found more than one StatefulSet for {component!r}"
                    )
                found[component] = json.dumps(
                    document["spec"]["template"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
    if statefulset_count != len(COMPONENTS):
        raise SystemExit(
            f"FAIL: revert mutant expected exactly four StatefulSets, found {statefulset_count}"
        )
    if set(found) != COMPONENTS:
        raise SystemExit(f"FAIL: revert mutant found StatefulSets {sorted(found)}")
    return found


baseline, chart_version, app_version = map(load, sys.argv[1:4])
for label, changed in (
    ("chart version only bump", chart_version),
    ("appVersion only bump", app_version),
):
    failures = {component for component in COMPONENTS if baseline[component] != changed[component]}
    if failures != COMPONENTS:
        raise SystemExit(
            f"FAIL: revert mutant {label} changed {sorted(failures)}, expected all four StatefulSets"
        )

print("Exact four site revert mutant makes the metadata comparison fail: OK")
PY

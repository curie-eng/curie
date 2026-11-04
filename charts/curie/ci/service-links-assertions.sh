#!/usr/bin/env bash
#
# Every pod spec the chart renders must set `enableServiceLinks: false` (#5350).
#
# Kubernetes injects `<SERVICE>_PORT=tcp://<ip>:<port>` and its siblings for
# every Service in the namespace. A chart Service named `<release>-<x>` thus
# shadows a `CURIE_<X>_PORT` setting in any pod that does not set the value
# explicitly: with lazy attachments on, `curie-attachment-broker` turned
# `CURIE_ATTACHMENT_BROKER_PORT` into a URL, `WorkerConfig()` refused it in the
# post-upgrade drain hook, and `helm upgrade` failed with the fleet still
# quiesced. No chart workload reads service-link variables, so the chart turns
# them off everywhere rather than pinning each colliding name per pod.
#
# The renders below enable every optional workload, and the script refuses to
# pass unless each template that declares `containers:` produced at least one
# pod spec, so a new template cannot slip past it unrendered.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

helm template curie "$CHART" > "$TMP/default.yaml"
helm template curie "$CHART" --is-upgrade > "$TMP/upgrade.yaml"
helm template curie "$CHART" -f "$CHART/values-dev.yaml" > "$TMP/dev.yaml"

# The configuration that reproduced the defect: lazy attachments create the
# attachment-broker Service, and an upgrade renders the drain hooks. The same
# render turns on every optional workload.
helm template curie "$CHART" --is-upgrade \
  --set worker.attachments.enabled=true \
  --set worker.attachments.mode=lazy \
  --set agentSandbox.runner.fakeModel=false \
  --set-string agentSandbox.runner.credentials=example \
  --set inference.deploy=true \
  --set inference.pullModel=false \
  --set mailAdapter.deploy=true \
  --set 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24' \
  --set-string dispatcher.slack.appToken=xapp-example \
  --set-string dispatcher.slack.botToken=xoxb-example \
  --set grafanaConnector.enabled=true \
  --set security.gvisor.mode=require \
  --set security.gvisor.installRuntimeClass=true \
  > "$TMP/everything.yaml"

# The external-claim branch of the mail-adapter persistence template.
helm template curie "$CHART" --is-upgrade \
  --set mailAdapter.deploy=true \
  --set 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24' \
  --set mailAdapter.persistence.existingClaim=example-mail-state \
  > "$TMP/mail-existing-claim.yaml"

python3 - "$CHART" "$TMP" <<'PY'
import pathlib
import re
import sys

import yaml

chart = pathlib.Path(sys.argv[1])
renders = sorted(pathlib.Path(sys.argv[2]).glob("*.yaml"))

WORKLOADS = {"Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"}


def pod_specs(doc):
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        yield spec
    elif kind in WORKLOADS:
        yield spec["template"]["spec"]
    elif kind == "CronJob":
        yield spec["jobTemplate"]["spec"]["template"]["spec"]
    elif kind == "SandboxTemplate":
        yield spec["podTemplate"]["spec"]


failures = []
seen_sources = set()
attachment_broker_service = False
drain_release_hook = False
for render in renders:
    text = render.read_text()
    for chunk in re.split(r"^---\s*$", text, flags=re.MULTILINE):
        source = re.search(r"^# Source: curie/(templates/\S+)$", chunk, re.MULTILINE)
        doc = yaml.safe_load(chunk)
        if not isinstance(doc, dict):
            continue
        name = (doc.get("metadata") or {}).get("name", "?")
        if doc.get("kind") == "Service" and name == "curie-attachment-broker":
            attachment_broker_service = True
        if doc.get("kind") == "Job" and name == "curie-upgrade-drain-release":
            drain_release_hook = True
        for spec in pod_specs(doc):
            if source:
                seen_sources.add(source.group(1))
            if spec.get("enableServiceLinks") is not False:
                failures.append(
                    f"{render.name}: {doc['kind']}/{name} "
                    f"enableServiceLinks={spec.get('enableServiceLinks')!r}"
                )

declared = {
    str(path.relative_to(chart))
    for path in (chart / "templates").rglob("*.yaml")
    if re.search(r"^\s*containers:", path.read_text(), re.MULTILINE)
}
unrendered = sorted(declared - seen_sources)

if not attachment_broker_service:
    failures.append("no render produced the curie-attachment-broker Service")
if not drain_release_hook:
    failures.append("no render produced the curie-upgrade-drain-release hook Job")
for path in unrendered:
    failures.append(f"{path} declares containers but no render produced its pod spec")

if failures:
    print("FAIL: chart pod specs must set enableServiceLinks: false (#5350)")
    for failure in sorted(set(failures)):
        print(f"  - {failure}")
    sys.exit(1)
print(f"PASS: {len(seen_sources)} templates, every pod spec sets enableServiceLinks: false")
PY

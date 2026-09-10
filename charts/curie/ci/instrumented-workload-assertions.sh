#!/usr/bin/env bash
#
# Issue #2360: a new first-party chart workload must fail CI when it omits
# OTEL_EXPORTER_OTLP_ENDPOINT. The durability suite still selects
# api/dispatcher/worker/runner by a hardcoded allowlist, so a sixth Deployment
# is skipped rather than rejected. This script inverts that filter.
#
# First-party is derived from the rendered image, not from a named component
# set: ghcr.io/curie-eng/curie-* plus single-component curie-* names used by
# the offline values-dev overlay. Membership is therefore a chart-owned image
# signal. Adding curie-discord as a chart Deployment without OTLP fails here
# even though that name appears nowhere in this file.
#
# Explicit exemptions (shapes that are first-party images but not OTLP
# exporters), each observed rather than silently dropped:
#   * init containers (API migrate bootstrap)
#   * objects annotated helm.sh/hook (preflight, drain, test Jobs)
#   * containers whose command is sleep infinity (runner-prewarm image cache)
#   * the SPA image whose last path component is curie-ui (nginx, no SDK)
#
# The runner SandboxTemplate is included: its pod spec lives at
# spec.podTemplate.spec, not spec.template.spec.
#
# Proves:
#   1. DEFAULT render: api, worker, and SandboxTemplate runner carry the
#      in-chart collector endpoint. Mail and dispatcher are absent (optional
#      / token-gated). Hooks, migrate, prewarm, and the UI are classified as
#      exempt, not skipped.
#   1b. values-dev.yaml: short curie-* image names (offline overlay) are
#       classified as first-party; sandbox is off so runner is absent.
#   2. MAIL DISABLED, dispatcher enabled, collector enabled.
#   3. MAIL ENABLED, dispatcher enabled, collector enabled (mail-adapter
#      joins the instrumented set).
#   4. MAIL ENABLED, collector replaced by otelCollector.endpoint.
#   5. NEGATIVE: a previously unknown first-party Deployment without OTLP
#      is rejected by the same checker.
#   6. NEGATIVE: stripping OTLP from an existing instrumentable container
#      is rejected by the same checker.
#   7. Hook sibling: a first-party helm-hook Job without OTLP remains
#      exempt, so the hook treatment is the observed classification, not a
#      silent skip.
# Discord has no chart workload today (#2358 is related and is not a
# dependency). This script does not add one, and it does not denylist the
# string "discord" so a later instrumented adapter is not blocked here.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

IN_CHART_ENDPOINT="http://curie-otel-collector:4318"
EXTERNAL_ENDPOINT="https://otel.example.com:4318"

DISPATCHER_SET=(
  --set-string dispatcher.slack.appToken=xapp-assert
  --set-string dispatcher.slack.botToken=xoxb-assert
)
MAIL_ON_SET=(
  --set mailAdapter.deploy=true
  --set 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24'
  --set mailAdapter.channelToken=chn-assert-token
  --set mailAdapter.egressSecret=egress-assert-secret
  --set mailAdapter.agentmail.apiKey=am-assert-key
)
EXTERNAL_COLLECTOR_SET=(
  --set otelCollector.deploy=false
  --set otelCollector.endpoint="${EXTERNAL_ENDPOINT}"
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32'
  --set 'otelCollector.egress[0].ports[0].protocol=TCP'
  --set 'otelCollector.egress[0].ports[0].port=4318'
)

render() {
  local output="$1"
  shift
  helm template curie "$CHART" "$@" >"$output"
}

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import argparse
import pathlib
import sys

import yaml

FIRST_PARTY_REGISTRY_PREFIX = "ghcr.io/curie-eng/curie-"
FIRST_PARTY_SHORT_PREFIX = "curie-"
SPA_IMAGE_NAME = "curie-ui"
SLEEP_INFINITY = ("sleep", "infinity")
OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
OTLP_PROTOCOL = "OTEL_EXPORTER_OTLP_PROTOCOL"
POD_TEMPLATE_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "Job")


def image_repository(image):
    ref = (image or "").strip()
    if not ref:
        return ""
    if "@" in ref:
        ref = ref.rsplit("@", 1)[0]
    parts = ref.split("/")
    last = parts[-1]
    if ":" in last:
        parts[-1] = last.rsplit(":", 1)[0]
    return "/".join(parts)


def last_name(repo):
    return repo.split("/")[-1] if repo else ""


def is_chart_owned_image(image):
    repo = image_repository(image)
    if repo.startswith(FIRST_PARTY_REGISTRY_PREFIX):
        return True
    if "/" not in repo and repo.startswith(FIRST_PARTY_SHORT_PREFIX):
        return True
    return False


def command_tuple(container):
    command = container.get("command") or []
    if not isinstance(command, list):
        return tuple()
    return tuple(str(part) for part in command)


def env_entries(container):
    entries = {}
    for item in container.get("env") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if name:
            entries[name] = item
    return entries


def env_has(entry):
    if not entry:
        return False
    if str(entry.get("value") or "").strip():
        return True
    if entry.get("valueFrom"):
        return True
    return False


def pod_specs(doc):
    kind = doc.get("kind")
    meta = doc.get("metadata") or {}
    name = meta.get("name") or "<unnamed>"
    hook = (meta.get("annotations") or {}).get("helm.sh/hook")
    if kind in POD_TEMPLATE_KINDS:
        spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
    elif kind == "CronJob":
        spec = (
            ((doc.get("spec") or {}).get("jobTemplate") or {})
            .get("spec") or {}
        ).get("template") or {}
        spec = spec.get("spec") or {}
    elif kind == "Pod":
        spec = doc.get("spec") or {}
    elif kind == "SandboxTemplate":
        spec = ((doc.get("spec") or {}).get("podTemplate") or {}).get("spec") or {}
    else:
        spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
    if not isinstance(spec, dict):
        return []
    return [(kind, name, hook, spec)]


def classify(kind, name, hook, container, is_init):
    image = (container.get("image") or "").strip()
    if not is_chart_owned_image(image):
        return None
    cname = container.get("name") or "<unnamed>"
    where = f"{kind}/{name} container={cname} image={image}"
    if is_init:
        return "init-bootstrap", where, container
    if hook:
        return "helm-hook", where, container
    if command_tuple(container) == SLEEP_INFINITY:
        return "prewarm-sleep", where, container
    if last_name(image_repository(image)) == SPA_IMAGE_NAME:
        return "spa", where, container
    return "instrumentable", where, container


def collect(render_path):
    docs = [
        doc
        for doc in yaml.safe_load_all(pathlib.Path(render_path).read_text())
        if isinstance(doc, dict)
    ]
    if not docs:
        raise SystemExit(f"{render_path}: rendered no documents")
    classified = {
        "instrumentable": [],
        "init-bootstrap": [],
        "helm-hook": [],
        "prewarm-sleep": [],
        "spa": [],
    }
    for doc in docs:
        for kind, name, hook, spec in pod_specs(doc):
            groups = (
                (spec.get("initContainers") or [], True),
                (spec.get("containers") or [], False),
            )
            for containers, is_init in groups:
                for container in containers:
                    if not isinstance(container, dict):
                        continue
                    result = classify(kind, name, hook, container, is_init)
                    if result is None:
                        continue
                    reason, where, found = result
                    classified[reason].append((where, found, kind, image_repository(container.get("image") or "")))
    return classified


def problems_for(classified, expect_endpoint):
    problems = []
    selected = classified["instrumentable"]
    if not selected:
        problems.append(
            "no first-party instrumentable containers were found; the OTLP "
            "presence check would otherwise pass vacuously"
        )
        return problems
    for where, container, _kind, _image in selected:
        env = env_entries(container)
        endpoint = env.get(OTLP_ENDPOINT)
        protocol = env.get(OTLP_PROTOCOL)
        if not env_has(endpoint):
            problems.append(f"missing {OTLP_ENDPOINT}: {where}")
            continue
        value = str((endpoint or {}).get("value") or "").strip()
        if value and value != expect_endpoint:
            problems.append(
                f"{OTLP_ENDPOINT} is {value!r} on {where}, expected {expect_endpoint!r}"
            )
        if not env_has(protocol):
            problems.append(f"missing {OTLP_PROTOCOL}: {where}")
    return problems


def repo_matches(image, needle):
    return needle in image_repository(image)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("render")
    parser.add_argument("label")
    parser.add_argument("--expect-endpoint", required=True)
    parser.add_argument("--min-instrumentable", type=int, default=1)
    parser.add_argument("--require-repo", action="append", default=[])
    parser.add_argument("--forbid-repo", action="append", default=[])
    parser.add_argument("--require-exempt", action="append", default=[])
    parser.add_argument("--require-instrumentable-kind", action="append", default=[])
    args = parser.parse_args(argv[1:])

    classified = collect(args.render)
    problems = []

    instrumentable = classified["instrumentable"]
    if len(instrumentable) < args.min_instrumentable:
        problems.append(
            f"{args.label}: expected at least {args.min_instrumentable} "
            f"instrumentable first-party containers, found {len(instrumentable)}"
        )

    images = [image for _where, _c, _kind, image in instrumentable]
    for needle in args.require_repo:
        if not any(repo_matches(image, needle) for image in images):
            problems.append(
                f"{args.label}: instrumentable set is missing image repository "
                f"substring {needle!r}; found {images}"
            )
    for needle in args.forbid_repo:
        hits = [image for image in images if repo_matches(image, needle)]
        if hits:
            problems.append(
                f"{args.label}: instrumentable set unexpectedly includes {needle!r}: {hits}"
            )

    kinds = {kind for _where, _c, kind, _image in instrumentable}
    for kind in args.require_instrumentable_kind:
        if kind not in kinds:
            problems.append(
                f"{args.label}: instrumentable set is missing kind {kind!r}; "
                f"found kinds {sorted(kinds)}"
            )

    for reason in args.require_exempt:
        if reason not in classified:
            problems.append(f"{args.label}: unknown exemption class {reason!r}")
        elif not classified[reason]:
            problems.append(
                f"{args.label}: expected at least one {reason} exemption, found none"
            )

    problems.extend(problems_for(classified, args.expect_endpoint))
    if problems:
        raise SystemExit(
            f"{len(problems)} instrumented-workload problem(s) in {args.label}:\n  - "
            + "\n  - ".join(problems)
        )

    def names(rows):
        return ", ".join(where for where, _c, _kind, _image in rows) or "(none)"

    print(f"  ok: {args.label}: {len(instrumentable)} instrumentable, endpoint={args.expect_endpoint}")
    print(f"      instrumentable: {names(instrumentable)}")
    print(f"      exempt init-bootstrap: {names(classified['init-bootstrap'])}")
    print(f"      exempt helm-hook: {names(classified['helm-hook'])}")
    print(f"      exempt prewarm-sleep: {names(classified['prewarm-sleep'])}")
    print(f"      exempt spa: {names(classified['spa'])}")


if __name__ == "__main__":
    main(sys.argv)
PY

MUTATE="$TMP/mutate.py"
cat >"$MUTATE" <<'PY'
"""Render mutations for the instrumented-workload negative controls."""
import pathlib
import sys

import yaml


def load(path):
    return [
        doc
        for doc in yaml.safe_load_all(pathlib.Path(path).read_text())
        if isinstance(doc, dict)
    ]


def dump(path, docs):
    with pathlib.Path(path).open("w") as handle:
        yaml.safe_dump_all(docs, handle)


def add_unknown_without_otlp(src, dest):
    docs = load(src)
    docs.append(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "curie-unknown-adapter",
                "labels": {"app.kubernetes.io/component": "unknown-adapter"},
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "unknown-adapter",
                                "image": "ghcr.io/curie-eng/curie-unknown-adapter:0.8.8",
                                "env": [{"name": "EXAMPLE", "value": "1"}],
                            }
                        ]
                    }
                }
            },
        }
    )
    dump(dest, docs)


def is_chart_owned_image(image):
    ref = (image or "").strip()
    if "@" in ref:
        ref = ref.rsplit("@", 1)[0]
    parts = ref.split("/")
    last = parts[-1]
    if ":" in last:
        parts[-1] = last.rsplit(":", 1)[0]
    repo = "/".join(parts)
    if repo.startswith("ghcr.io/curie-eng/curie-"):
        return True
    if "/" not in repo and repo.startswith("curie-"):
        return True
    return False


def strip_otlp_from_existing(src, dest):
    docs = load(src)
    mutated = False
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
        for container in spec.get("containers") or []:
            if not is_chart_owned_image(container.get("image") or ""):
                continue
            env = container.get("env") or []
            kept = [
                entry
                for entry in env
                if not str(entry.get("name") or "").startswith("OTEL_EXPORTER_OTLP_")
            ]
            if len(kept) != len(env):
                container["env"] = kept
                mutated = True
                break
        if mutated:
            break
    if not mutated:
        raise SystemExit("could not find an existing first-party OTEL_EXPORTER_OTLP_ env to strip")
    dump(dest, docs)


def add_hook_without_otlp(src, dest):
    docs = load(src)
    docs.append(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "curie-unknown-hook",
                "annotations": {"helm.sh/hook": "pre-install"},
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "unknown-hook",
                                "image": "ghcr.io/curie-eng/curie-unknown-hook:0.8.8",
                                "command": ["true"],
                            }
                        ]
                    }
                }
            },
        }
    )
    dump(dest, docs)


ACTIONS = {
    "add-unknown": add_unknown_without_otlp,
    "strip-otlp": strip_otlp_from_existing,
    "add-hook": add_hook_without_otlp,
}

action, src, dest = sys.argv[1:]
ACTIONS[action](src, dest)
PY

assert_rejected() {
  local label="$1"
  shift
  local -a expects=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    expects+=("$1")
    shift
  done
  [[ "${1:-}" == "--" ]] || fail "assert_rejected($label): missing -- separator"
  shift
  [[ $# -gt 0 ]] || fail "assert_rejected($label): no checker args after --"

  local out=""
  if out="$(python3 "$CHECKER" "$@" 2>&1)"; then
    fail "expected a rejection but the contract passed -- $label"
  fi
  local expect
  for expect in "${expects[@]}"; do
    if [[ "$out" != *"$expect"* ]]; then
      fail "rejected for the wrong reason ($label): expected $expect in: $out"
    fi
  done
  echo "  ok: $label"
}

DEFAULT_RENDER="$TMP/default.yaml"
MAIL_OFF_RENDER="$TMP/mail-off.yaml"
MAIL_ON_RENDER="$TMP/mail-on.yaml"
EXTERNAL_RENDER="$TMP/external.yaml"

echo "=== Render: default values (mail off, dispatcher token-gated off, collector on) ==="
render "$DEFAULT_RENDER"
python3 "$CHECKER" "$DEFAULT_RENDER" default \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 3 \
  --require-repo curie-api \
  --require-repo curie-worker \
  --require-repo curie-runner \
  --forbid-repo curie-mail-adapter \
  --forbid-repo curie-dispatcher \
  --require-instrumentable-kind SandboxTemplate \
  --require-exempt init-bootstrap \
  --require-exempt helm-hook \
  --require-exempt prewarm-sleep \
  --require-exempt spa

echo "=== Render: values-dev.yaml (short curie-* image names, sandbox off) ==="
DEV_RENDER="$TMP/values-dev.yaml"
render "$DEV_RENDER" -f "$CHART/values-dev.yaml"
python3 "$CHECKER" "$DEV_RENDER" values-dev \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 2 \
  --require-repo curie-api \
  --require-repo curie-worker \
  --forbid-repo curie-mail-adapter \
  --require-exempt spa \
  --require-exempt init-bootstrap

echo "=== Render: dispatcher enabled, mail disabled, collector on ==="
render "$MAIL_OFF_RENDER" "${DISPATCHER_SET[@]}"
python3 "$CHECKER" "$MAIL_OFF_RENDER" mail-disabled \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 4 \
  --require-repo curie-api \
  --require-repo curie-worker \
  --require-repo curie-dispatcher \
  --require-repo curie-runner \
  --forbid-repo curie-mail-adapter \
  --require-instrumentable-kind SandboxTemplate \
  --require-exempt spa

echo "=== Render: dispatcher enabled, mail enabled, collector on ==="
render "$MAIL_ON_RENDER" "${DISPATCHER_SET[@]}" "${MAIL_ON_SET[@]}"
python3 "$CHECKER" "$MAIL_ON_RENDER" mail-enabled \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 5 \
  --require-repo curie-api \
  --require-repo curie-worker \
  --require-repo curie-dispatcher \
  --require-repo curie-mail-adapter \
  --require-repo curie-runner \
  --require-instrumentable-kind SandboxTemplate \
  --require-exempt spa

echo "=== Render: mail enabled, external collector endpoint ==="
render "$EXTERNAL_RENDER" "${DISPATCHER_SET[@]}" "${MAIL_ON_SET[@]}" "${EXTERNAL_COLLECTOR_SET[@]}"
python3 "$CHECKER" "$EXTERNAL_RENDER" external-endpoint \
  --expect-endpoint "$EXTERNAL_ENDPOINT" \
  --min-instrumentable 5 \
  --require-repo curie-mail-adapter \
  --require-repo curie-runner \
  --require-instrumentable-kind SandboxTemplate

echo "=== Negative: previously unknown first-party workload without OTLP is refused ==="
UNKNOWN_RENDER="$TMP/unknown.yaml"
python3 "$MUTATE" add-unknown "$MAIL_ON_RENDER" "$UNKNOWN_RENDER"
assert_rejected "unknown first-party workload without OTLP is rejected" \
  "missing OTEL_EXPORTER_OTLP_ENDPOINT" \
  "curie-unknown-adapter" \
  -- "$UNKNOWN_RENDER" unknown-without-otlp \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 5

echo "=== Negative: removing OTLP from an existing first-party workload is refused ==="
STRIPPED_RENDER="$TMP/stripped.yaml"
python3 "$MUTATE" strip-otlp "$MAIL_ON_RENDER" "$STRIPPED_RENDER"
assert_rejected "stripped OTLP env from an existing workload is rejected" \
  "missing OTEL_EXPORTER_OTLP_ENDPOINT" \
  -- "$STRIPPED_RENDER" stripped-otlp \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 5

echo "=== Sibling: first-party helm-hook Job without OTLP stays exempt ==="
HOOK_RENDER="$TMP/hook.yaml"
python3 "$MUTATE" add-hook "$MAIL_ON_RENDER" "$HOOK_RENDER"
hook_out="$(python3 "$CHECKER" "$HOOK_RENDER" hook-exempt \
  --expect-endpoint "$IN_CHART_ENDPOINT" \
  --min-instrumentable 5 \
  --require-repo curie-mail-adapter \
  --require-exempt helm-hook)"
printf '%s\n' "$hook_out"
[[ "$hook_out" == *"Job/curie-unknown-hook"* ]] || \
  fail "hook-exempt run did not classify Job/curie-unknown-hook as a helm-hook exemption: $hook_out"

echo
echo "PASS: first-party chart workloads are enumerated from rendered chart-owned images; optional mail, collector, and SandboxTemplate are covered; unknown-without-OTLP and stripped-OTLP mutations fail the same gate"

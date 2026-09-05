#!/usr/bin/env bash
#
# Render-assertion test for issue #2317. Rail 1 (the runner-sandbox
# default-deny egress) is fail-closed, and three of its carve-outs were written
# as `{{- if .Values.<component>.deploy }}` with a `to:` podSelector on THIS
# release's own pod. That shape is only correct while the component is in
# chart. Flip the component off, point the runner at an external one, and the
# carve-out simply does not render -- so the runner is handed an address it can
# never reach, and the install is green.
#
# #2213 fixed that for rustfs. Two instances remained:
#
#   * `otelCollector.deploy: false` + `otelCollector.endpoint:
#     https://otlp.example.net:4318`. The sandbox still gets
#     OTEL_EXPORTER_OTLP_ENDPOINT pointed at that external host (via the
#     curie.env.otel helper), but no egress allow renders, so every runner span
#     is silently dropped.
#   * `api.deploy: false` + `dispatcher.apiBaseUrl: https://api.example.net`.
#     The runner is handed memory/history refs on the external API host, but no
#     egress allow renders, so agents silently boot with no memory and no
#     history.
#
# This pins the replacement contract, mirroring #2213 exactly:
#
#   1. The default (in-chart) render is unchanged: runner-allow-collector still
#      selects this release's otel-collector pod on the two service ports,
#      runner-allow-api still selects this release's api pod on TCP 8000,
#      neither uses an ipBlock, and neither `-endpoint` policy renders.
#   2. otelCollector.deploy=false with a resolved endpoint renders
#      <release>-runner-allow-collector-endpoint from `otelCollector.egress`,
#      and the in-chart pod-selector policy does not render.
#   3. api.deploy=false renders <release>-runner-allow-api-endpoint from
#      `api.egress`, and the in-chart pod-selector policy does not render.
#   4. Each BYO path is fail-closed: deploy=false with an empty egress list is
#      refused at render, and the message names the exact values key to set, so
#      a model-only allowedEgress list cannot ship a silently broken install.
#   5. The collector refusal is scoped to an endpoint the runner actually
#      needs. otelCollector.deploy=false with an EMPTY resolved endpoint
#      (telemetry off / no-endpoint mode) renders fine and emits no collector
#      policy of either kind -- there is nothing for the runner to reach.
#   6. Opting out of Rail 1 (`security.networkPolicy.enabled=false`) does not
#      require either key: there is no fail-closed runner policy to satisfy.
#   7. Both new keys reuse the shared `curie.objectStore.egressEntry`
#      validator, so a default route, a metadata-covering CIDR, and a
#      ports-less entry are each refused.
#   8. Neither key is a silent no-op on the in-chart path. Setting
#      otelCollector.egress with otelCollector.deploy=true, or api.egress with
#      api.deploy=true, is refused at render naming the key -- the key is only
#      read on the BYO branch, and quietly ignoring it there is how an operator
#      ends up believing egress is declared when the pod selector is what
#      actually governs.
#   9. CLASS GUARD. The point of #2317 is that this was a class, not two bugs.
#      Assertion 12 parses EVERY template under charts/curie/templates that
#      renders a `kind: NetworkPolicy` -- discovered by grep, never a hardcoded
#      list, so a carve-out added to a new netpol template cannot escape -- and
#      requires every `.Values.<component>.deploy` carve-out gate to have a BYO
#      `{{- else }}` branch that actually refuses the render (`fail`) or
#      renders a replacement `ipBlock` peer -- an empty `{{- else }}` leaves the
#      bug intact and is rejected -- or to appear in the exemption table below
#      (keyed by file AND component) with a written reason. A future author adding a
#      fourth in-chart carve-out is caught at CI time rather than by a customer
#      whose telemetry silently stops.
#
# NetworkPolicy speaks CIDRs, not DNS names, so the chart cannot derive an
# allow from otelCollector.endpoint or dispatcher.apiBaseUrl. Values-level CIDR
# lists are the mechanism, exactly as with rustfs.egress.
#
# Runnable locally and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMPLATES_DIR="$CHART/templates"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RELEASE=curie
NAMESPACE=dev

COLLECTOR_POLICY="${RELEASE}-runner-allow-collector"
COLLECTOR_BYO_POLICY="${RELEASE}-runner-allow-collector-endpoint"
API_POLICY="${RELEASE}-runner-allow-api"
API_BYO_POLICY="${RELEASE}-runner-allow-api-endpoint"
DEFAULT_DENY_EGRESS="${RELEASE}-runner-default-deny-egress"
ALLOW_EGRESS="${RELEASE}-runner-allow-egress"

# Documentation-only addresses: TEST-NET-1 (RFC 5737) and example.net.
COLLECTOR_CIDR=192.0.2.20/32
API_CIDR=192.0.2.21/32
OTLP_ENDPOINT=https://otlp.example.net:4318
API_BASE_URL=https://api.example.net

# ---------------------------------------------------------------------------
# Class-guard exemption table (assertion 10).
#
# Each entry is `<template-basename>:<component>=reason`. The key is the FILE as
# well as the component, because the same component gates a carve-out in more
# than one NetworkPolicy template with different justifications (otelCollector
# is both an in-chart runner peer and an in-chart mail-adapter peer, and only
# one of those two is exempt).
#
# An exemption says: this in-chart `.deploy` carve-out deliberately has no BYO
# `{{- else }}` branch, and here is why the external form of that destination is
# already governed, or why the gate is not a carve-out at all.
# ---------------------------------------------------------------------------
declare -a DEPLOY_GATE_EXEMPTIONS=(
  "security-networkpolicy.yaml:inference=an external inference endpoint is the model API, already governed by the operator's security.networkPolicy.allowedEgress model allowlist, which is the intended mechanism there"
  "mail-adapter.yaml:mailAdapter=this is the whole-template deploy gate for the mail adapter itself, not an egress carve-out: with mailAdapter.deploy false there is no adapter pod, no policy, and no destination to bring your own"
  "mail-adapter.yaml:otelCollector=deliberate, documented asymmetry. The mail adapter is the only first-party workload with its own egress policy, so with otelCollector.deploy false and an external otelCollector.endpoint the chart invents no broad allow for an address it cannot know; the operator applies an additional egress policy selecting the adapter's labels, because NetworkPolicies union rather than intersect. Written up in charts/curie/README.md -- search for \"asymmetric\" in the mail adapter section"
)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render_dir() {
  local name="$1"
  shift
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --output-dir "$out" "$@" >/dev/null
  printf '%s\n' "$out"
}

must_fail_naming() {
  local label="$1"
  local needle="$2"
  local values="$3"
  local out="$TMP/${label// /_}.txt"
  if helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --values "$values" \
    > /dev/null 2>"$out"; then
    fail "${label} must fail Helm rendering"
  fi
  if ! grep -q -- "$needle" "$out"; then
    fail "${label} failed without naming ${needle}; output was: $(cat "$out")"
  fi
  echo "ok: ${label} is refused at render"
}

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
import pathlib
import sys

import yaml

ROOT = pathlib.Path(sys.argv[1])
ACTION = sys.argv[2]
ARGS = sys.argv[3:]


def die(message):
    raise SystemExit(message)


def load(root):
    docs = []
    for path in sorted(root.rglob("*.yaml")):
        for document in yaml.safe_load_all(path.read_text()):
            if isinstance(document, dict):
                docs.append(document)
    if not docs:
        die(f"{root}: Helm wrote no YAML documents")
    return docs


def policies(docs):
    return [doc for doc in docs if doc.get("kind") == "NetworkPolicy"]


def named(docs, name):
    matches = [
        doc
        for doc in policies(docs)
        if doc.get("metadata", {}).get("name") == name
    ]
    if len(matches) > 1:
        die(f"expected at most one NetworkPolicy/{name}, found {len(matches)}")
    return matches[0] if matches else None


def ip_blocks(policy):
    blocks = []
    for rule in (policy or {}).get("spec", {}).get("egress", []) or []:
        ports = rule.get("ports")
        for peer in rule.get("to") or []:
            block = peer.get("ipBlock") or {}
            if "cidr" in block:
                blocks.append((block["cidr"], ports, block.get("except") or []))
    return blocks


def pod_peers(policy):
    peers = []
    for rule in (policy or {}).get("spec", {}).get("egress", []) or []:
        ports = rule.get("ports")
        for peer in rule.get("to") or []:
            if "podSelector" in peer:
                peers.append((peer, ports))
    return peers


def runner_selector(policy):
    return (policy or {}).get("spec", {}).get("podSelector", {}).get("matchLabels", {})


def peer_component(peer):
    return (
        peer.get("podSelector", {})
        .get("matchLabels", {})
        .get("app.kubernetes.io/component")
    )


def assert_in_chart(policy, name, component, expected_ports):
    """The in-chart carve-out: one pod peer on this release's own component."""
    if policy is None:
        die(f"render dropped {name}")
    labels = runner_selector(policy)
    if labels.get("app.kubernetes.io/component") != "runner-sandbox":
        die(f"{name} selects {labels!r}, not runner-sandbox")
    peers = pod_peers(policy)
    if len(peers) != 1:
        die(f"{name} expected one pod peer, found {len(peers)}")
    peer, ports = peers[0]
    got = peer_component(peer)
    if got != component:
        die(f"{name} peer component is {got!r}, expected {component!r}")
    if ports != expected_ports:
        die(f"{name} ports are {ports!r}, expected {expected_ports!r}")
    if ip_blocks(policy):
        die(f"in-chart {name} must keep using a pod selector, not ipBlock")


def assert_byo(policy, name, cidr, ports):
    """The BYO carve-out: an egress-only ipBlock policy on the sandbox pods."""
    if policy is None:
        die(f"BYO render missing {name}")
    labels = runner_selector(policy)
    if labels.get("app.kubernetes.io/component") != "runner-sandbox":
        die(f"{name} selects {labels!r}, not runner-sandbox")
    if policy.get("spec", {}).get("policyTypes") != ["Egress"]:
        die(f"{name} must be egress-only")
    if pod_peers(policy):
        die(f"{name} must not select an in-chart pod; there is no in-chart pod to select")
    blocks = ip_blocks(policy)
    cidrs = {block_cidr for block_cidr, _ports, _except in blocks}
    if cidrs != {cidr}:
        die(f"{name} CIDRs are {sorted(cidrs)!r}; expected exactly {{{cidr}}}")
    for block_cidr, block_ports, excepts in blocks:
        if block_ports != ports:
            die(f"{name} {block_cidr} ports are {block_ports!r}, expected {ports!r}")
        if excepts:
            die(f"{name} {block_cidr} must not except anything on a /32 host allow; got {excepts}")


docs = load(ROOT)

if ACTION == "default":
    (
        collector_name,
        collector_byo_name,
        api_name,
        api_byo_name,
        deny_name,
        allow_name,
        grpc_port,
        http_port,
    ) = ARGS
    collector = named(docs, collector_name)
    api = named(docs, api_name)
    if named(docs, collector_byo_name) is not None:
        die(f"default render must not emit {collector_byo_name}")
    if named(docs, api_byo_name) is not None:
        die(f"default render must not emit {api_byo_name}")
    if named(docs, deny_name) is None:
        die("default render dropped runner-default-deny-egress")
    if named(docs, allow_name) is not None:
        die("default render must keep allowedEgress empty (no runner-allow-egress)")
    assert_in_chart(
        collector,
        collector_name,
        "otel-collector",
        [
            {"protocol": "TCP", "port": int(grpc_port)},
            {"protocol": "TCP", "port": int(http_port)},
        ],
    )
    assert_in_chart(api, api_name, "api", [{"protocol": "TCP", "port": 8000}])
    print(
        "ok: default render keeps the in-chart collector and api pod allows "
        "and emits no BYO endpoint policy"
    )

elif ACTION == "byo-collector":
    (
        collector_name,
        collector_byo_name,
        api_name,
        deny_name,
        allow_name,
        cidr,
    ) = ARGS
    if named(docs, collector_name) is not None:
        die(f"BYO collector render still emitted in-chart {collector_name}")
    if named(docs, deny_name) is None:
        die("BYO collector render dropped runner-default-deny-egress")
    if named(docs, allow_name) is not None:
        die("BYO collector render must keep allowedEgress empty (no runner-allow-egress)")
    if named(docs, api_name) is None:
        die(f"BYO collector render dropped the still-in-chart {api_name}")
    assert_byo(
        named(docs, collector_byo_name),
        collector_byo_name,
        cidr,
        [{"protocol": "TCP", "port": 4318}],
    )
    print("ok: BYO collector render allows the configured OTLP endpoint CIDR")

elif ACTION == "byo-api":
    (api_name, api_byo_name, collector_name, deny_name, allow_name, cidr) = ARGS
    if named(docs, api_name) is not None:
        die(f"BYO API render still emitted in-chart {api_name}")
    if named(docs, deny_name) is None:
        die("BYO API render dropped runner-default-deny-egress")
    if named(docs, allow_name) is not None:
        die("BYO API render must keep allowedEgress empty (no runner-allow-egress)")
    if named(docs, collector_name) is None:
        die(f"BYO API render dropped the still-in-chart {collector_name}")
    assert_byo(
        named(docs, api_byo_name),
        api_byo_name,
        cidr,
        [{"protocol": "TCP", "port": 443}],
    )
    print("ok: BYO API render allows the configured API endpoint CIDR")

elif ACTION == "byo-both":
    (
        collector_name,
        collector_byo_name,
        api_name,
        api_byo_name,
        deny_name,
        collector_cidr,
        api_cidr,
    ) = ARGS
    if named(docs, collector_name) is not None:
        die(f"both-BYO render still emitted in-chart {collector_name}")
    if named(docs, api_name) is not None:
        die(f"both-BYO render still emitted in-chart {api_name}")
    if named(docs, deny_name) is None:
        die("both-BYO render dropped runner-default-deny-egress (Rail 1 must stay on)")
    assert_byo(
        named(docs, collector_byo_name),
        collector_byo_name,
        collector_cidr,
        [{"protocol": "TCP", "port": 4318}],
    )
    assert_byo(
        named(docs, api_byo_name),
        api_byo_name,
        api_cidr,
        [{"protocol": "TCP", "port": 443}],
    )
    print("ok: both BYO endpoint policies render alongside the default-deny policy")

elif ACTION == "no-collector-policy":
    (collector_name, collector_byo_name, deny_name) = ARGS
    if named(docs, deny_name) is None:
        die("no-endpoint render dropped runner-default-deny-egress")
    if named(docs, collector_name) is not None:
        die(
            f"telemetry-off render emitted in-chart {collector_name}; there is no "
            "in-chart collector to reach"
        )
    if named(docs, collector_byo_name) is not None:
        die(
            f"telemetry-off render emitted {collector_byo_name}; there is no OTLP "
            "endpoint for the runner to reach, so no allow should exist"
        )
    print("ok: telemetry-off BYO renders with no collector egress policy of either kind")

else:
    die(f"unknown action {ACTION!r}")
PY

# The in-chart collector allow uses the collector Service ports; read them from
# values rather than hardcoding, so a port rename is not a false failure.
read_value() {
  python3 - "$CHART" "$1" <<'PY'
import pathlib
import sys

import yaml

values = yaml.safe_load((pathlib.Path(sys.argv[1]) / "values.yaml").read_text())
node = values
for part in sys.argv[2].split("."):
    node = node[part]
print(node)
PY
}

GRPC_PORT="$(read_value otelCollector.service.grpcPort)"
HTTP_PORT="$(read_value otelCollector.service.httpPort)"

echo "=== Assertion 1: default render is unchanged (in-chart collector and api allows, no BYO policies) ==="
DEFAULT_OUT="$(render_dir default)"
python3 "$CHECKER" "$DEFAULT_OUT" default \
  "$COLLECTOR_POLICY" "$COLLECTOR_BYO_POLICY" "$API_POLICY" "$API_BYO_POLICY" \
  "$DEFAULT_DENY_EGRESS" "$ALLOW_EGRESS" "$GRPC_PORT" "$HTTP_PORT" \
  || fail "default render changed the in-chart collector/api egress carve-outs or opened a BYO policy"

echo "=== Assertion 2: BYO collector renders an ipBlock allow for the external OTLP endpoint ==="
BYO_COLLECTOR_VALUES="$TMP/byo-collector.yaml"
cat > "$BYO_COLLECTOR_VALUES" <<EOF
otelCollector:
  deploy: false
  endpoint: ${OTLP_ENDPOINT}
  egress:
    - cidr: ${COLLECTOR_CIDR}
      ports: [{ protocol: TCP, port: 4318 }]
EOF
BYO_COLLECTOR_OUT="$(render_dir byo-collector --values "$BYO_COLLECTOR_VALUES")"
python3 "$CHECKER" "$BYO_COLLECTOR_OUT" byo-collector \
  "$COLLECTOR_POLICY" "$COLLECTOR_BYO_POLICY" "$API_POLICY" \
  "$DEFAULT_DENY_EGRESS" "$ALLOW_EGRESS" "$COLLECTOR_CIDR" \
  || fail "BYO collector render did not emit ${COLLECTOR_BYO_POLICY} covering otelCollector.egress"

echo "=== Assertion 3: BYO API renders an ipBlock allow for the external API endpoint ==="
BYO_API_VALUES="$TMP/byo-api.yaml"
cat > "$BYO_API_VALUES" <<EOF
api:
  deploy: false
  egress:
    - cidr: ${API_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
dispatcher:
  apiBaseUrl: ${API_BASE_URL}
ui:
  apiBaseUrl: ${API_BASE_URL}
EOF
BYO_API_OUT="$(render_dir byo-api --values "$BYO_API_VALUES")"
python3 "$CHECKER" "$BYO_API_OUT" byo-api \
  "$API_POLICY" "$API_BYO_POLICY" "$COLLECTOR_POLICY" \
  "$DEFAULT_DENY_EGRESS" "$ALLOW_EGRESS" "$API_CIDR" \
  || fail "BYO API render did not emit ${API_BYO_POLICY} covering api.egress"

echo "=== Assertion 4: both BYO paths at once keep Rail 1 intact ==="
BYO_BOTH_VALUES="$TMP/byo-both.yaml"
cat > "$BYO_BOTH_VALUES" <<EOF
otelCollector:
  deploy: false
  endpoint: ${OTLP_ENDPOINT}
  egress:
    - cidr: ${COLLECTOR_CIDR}
      ports: [{ protocol: TCP, port: 4318 }]
api:
  deploy: false
  egress:
    - cidr: ${API_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
dispatcher:
  apiBaseUrl: ${API_BASE_URL}
ui:
  apiBaseUrl: ${API_BASE_URL}
EOF
BYO_BOTH_OUT="$(render_dir byo-both --values "$BYO_BOTH_VALUES")"
python3 "$CHECKER" "$BYO_BOTH_OUT" byo-both \
  "$COLLECTOR_POLICY" "$COLLECTOR_BYO_POLICY" "$API_POLICY" "$API_BYO_POLICY" \
  "$DEFAULT_DENY_EGRESS" "$COLLECTOR_CIDR" "$API_CIDR" \
  || fail "both-BYO render did not emit both endpoint policies alongside the default-deny policy"

echo "=== Assertion 5: BYO collector without otelCollector.egress fails at render, naming the key ==="
MISSING_COLLECTOR_VALUES="$TMP/missing-collector-egress.yaml"
cat > "$MISSING_COLLECTOR_VALUES" <<EOF
otelCollector:
  deploy: false
  endpoint: ${OTLP_ENDPOINT}
EOF
must_fail_naming "missing otelCollector.egress" "otelCollector.egress" "$MISSING_COLLECTOR_VALUES"

echo "=== Assertion 6: BYO API without api.egress fails at render, naming the key ==="
MISSING_API_VALUES="$TMP/missing-api-egress.yaml"
cat > "$MISSING_API_VALUES" <<EOF
api:
  deploy: false
dispatcher:
  apiBaseUrl: ${API_BASE_URL}
ui:
  apiBaseUrl: ${API_BASE_URL}
EOF
must_fail_naming "missing api.egress" "api.egress" "$MISSING_API_VALUES"

echo "=== Assertion 7: telemetry-off BYO collector needs no egress and emits no collector policy ==="
TELEMETRY_OFF_VALUES="$TMP/telemetry-off.yaml"
cat > "$TELEMETRY_OFF_VALUES" <<'EOF'
otelCollector:
  deploy: false
  endpoint: ""
  telemetryDisabled: true
EOF
TELEMETRY_OFF_OUT="$(render_dir telemetry-off --values "$TELEMETRY_OFF_VALUES")" \
  || fail "otelCollector.deploy=false with an empty resolved endpoint must still render"
python3 "$CHECKER" "$TELEMETRY_OFF_OUT" no-collector-policy \
  "$COLLECTOR_POLICY" "$COLLECTOR_BYO_POLICY" "$DEFAULT_DENY_EGRESS" \
  || fail "telemetry-off render emitted a collector egress policy for an endpoint that does not exist"

echo "=== Assertion 8: Rail 1 off does not require either BYO egress key ==="
RAIL_OFF_VALUES="$TMP/rail-off.yaml"
cat > "$RAIL_OFF_VALUES" <<EOF
security:
  networkPolicy:
    enabled: false
otelCollector:
  deploy: false
  endpoint: ${OTLP_ENDPOINT}
api:
  deploy: false
dispatcher:
  apiBaseUrl: ${API_BASE_URL}
ui:
  apiBaseUrl: ${API_BASE_URL}
EOF
if ! helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
  --values "$RAIL_OFF_VALUES" > /dev/null; then
  fail "networkPolicy.enabled=false must render without otelCollector.egress or api.egress"
fi
echo "ok: opting out of Rail 1 does not require otelCollector.egress or api.egress"

echo "=== Assertion 9: both new keys reuse the shared egress-entry validator ==="
write_entry_values() {
  local path="$1"
  local key="$2"
  local entry="$3"
  if [[ "$key" == "otelCollector.egress" ]]; then
    cat > "$path" <<EOF
otelCollector:
  deploy: false
  endpoint: ${OTLP_ENDPOINT}
  egress:
${entry}
EOF
  else
    cat > "$path" <<EOF
api:
  deploy: false
  egress:
${entry}
dispatcher:
  apiBaseUrl: ${API_BASE_URL}
ui:
  apiBaseUrl: ${API_BASE_URL}
EOF
  fi
}

for KEY in otelCollector.egress api.egress; do
  DEFAULT_ROUTE_ENTRY=$'    - cidr: 0.0.0.0/0\n      ports: [{ protocol: TCP, port: 443 }]'
  IMDS_ENTRY=$'    - cidr: 169.254.0.0/16\n      ports: [{ protocol: TCP, port: 443 }]'
  NO_PORTS_ENTRY=$'    - cidr: 192.0.2.30/32'

  write_entry_values "$TMP/entry-default-route.yaml" "$KEY" "$DEFAULT_ROUTE_ENTRY"
  must_fail_naming "default-route ${KEY}" "$KEY" "$TMP/entry-default-route.yaml"

  write_entry_values "$TMP/entry-imds.yaml" "$KEY" "$IMDS_ENTRY"
  must_fail_naming "metadata-covering ${KEY}" "169.254.169.254" "$TMP/entry-imds.yaml"

  write_entry_values "$TMP/entry-no-ports.yaml" "$KEY" "$NO_PORTS_ENTRY"
  must_fail_naming "ports-missing ${KEY}" "ports" "$TMP/entry-no-ports.yaml"
done

echo "=== Assertion 10: otelCollector.egress with deploy=true is refused at render, naming the key ==="
INCHART_COLLECTOR_VALUES="$TMP/inchart-collector-egress.yaml"
cat > "$INCHART_COLLECTOR_VALUES" <<EOF
otelCollector:
  deploy: true
  egress:
    - cidr: ${COLLECTOR_CIDR}
      ports: [{ protocol: TCP, port: 4318 }]
EOF
must_fail_naming "in-chart otelCollector.egress" "otelCollector.egress" "$INCHART_COLLECTOR_VALUES"

echo "=== Assertion 11: api.egress with deploy=true is refused at render, naming the key ==="
INCHART_API_VALUES="$TMP/inchart-api-egress.yaml"
cat > "$INCHART_API_VALUES" <<EOF
api:
  deploy: true
  egress:
    - cidr: ${API_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
EOF
must_fail_naming "in-chart api.egress" "api.egress" "$INCHART_API_VALUES"

echo "=== Assertion 12: class guard -- every in-chart .deploy carve-out in every NetworkPolicy template has a BYO else branch ==="
EXEMPTIONS_FILE="$TMP/exemptions.txt"
: > "$EXEMPTIONS_FILE"
for ENTRY in "${DEPLOY_GATE_EXEMPTIONS[@]}"; do
  printf '%s\n' "$ENTRY" >> "$EXEMPTIONS_FILE"
done

# Discover the scan set dynamically: EVERY template that renders a
# NetworkPolicy, not a hardcoded list. A BYO-gated carve-out added to a new
# netpol template is exactly the #2317 class, so a new file must be picked up
# without anyone remembering to edit this script. The anchor matters: a
# document-level `kind:` is always at column 0, whereas security-probe.yaml and
# preflight-networkpolicy.yaml only mention `kind: NetworkPolicy` indented
# inside a probe heredoc, and those Jobs render no policy of their own.
NETPOL_TEMPLATES=()
while IFS= read -r NETPOL_FILE; do
  NETPOL_TEMPLATES+=("$NETPOL_FILE")
done < <(grep -rlE '^kind: NetworkPolicy' "$TEMPLATES_DIR" | sort)

if [[ ${#NETPOL_TEMPLATES[@]} -eq 0 ]]; then
  fail "class guard found no templates rendering a NetworkPolicy under ${TEMPLATES_DIR}; the discovery grep has drifted, fix the guard rather than deleting it"
fi
echo "class guard scanning ${#NETPOL_TEMPLATES[@]} NetworkPolicy templates:"
for NETPOL_FILE in "${NETPOL_TEMPLATES[@]}"; do
  echo "  - ${NETPOL_FILE#"$CHART"/}"
done

if python3 - "$EXEMPTIONS_FILE" "${NETPOL_TEMPLATES[@]}" <<'PY'
import pathlib
import re
import sys

exemptions_path = pathlib.Path(sys.argv[1])
template_paths = [pathlib.Path(arg) for arg in sys.argv[2:]]

# Exemption keys are (file basename, component): the same component gates a
# carve-out in more than one template with different justifications.
exemptions = {}
for line in exemptions_path.read_text().splitlines():
    line = line.strip()
    if not line:
        continue
    key, _, reason = line.partition("=")
    if not reason.strip():
        raise SystemExit(
            f"exemption for {key!r} has no written reason; an exemption "
            "without a reason is not an exemption"
        )
    filename, sep, component = key.strip().partition(":")
    if not sep or not filename.strip() or not component.strip():
        raise SystemExit(
            f"exemption key {key!r} is not <template-basename>:<component>; "
            "exemptions are keyed by file AND component"
        )
    exemptions[(filename.strip(), component.strip())] = reason.strip()

# Every Go-template block action, in source order. `else if` must NOT change
# depth, so it is matched before the bare `if` alternative. The body allows a
# lone `}` so a regex quantifier inside a quoted argument (`[0-9]{0,2}` in
# security-networkpolicy.yaml's prefix-length check) does not hide the action
# and silently skew the depth walk.
TOKEN = re.compile(
    r"\{\{-?\s*(?P<kind>else\s+if|else|end|if|range|with|define|block)\b"
    r"(?P<rest>(?:[^}]|\}(?!\}))*)\}\}"
)

OPENERS = {"if", "range", "with", "define", "block"}

DEPLOY_REF = re.compile(r"\.Values\.([A-Za-z0-9_]+)\.deploy\b")
NEGATED_DEPLOY_REF = re.compile(r"\bnot\s+\.Values\.([A-Za-z0-9_]+)\.deploy\b")
ENABLED_REF = re.compile(r"\.[A-Za-z0-9_]*[Ee]nabled\b")

problems = []
seen = set()

for template_path in template_paths:
    text = template_path.read_text()
    name = template_path.name

    def line_of(offset, text=text):
        return text.count("\n", 0, offset) + 1

    tokens = []
    for match in TOKEN.finditer(text):
        kind = re.sub(r"\s+", " ", match.group("kind"))
        tokens.append((match.start(), match.end(), kind, match.group("rest")))

    for index, (offset, _end, kind, rest) in enumerate(tokens):
        if kind != "if":
            continue
        components = DEPLOY_REF.findall(rest)
        if not components:
            continue

        # Walk forward, counting block depth, to find this gate's own `else`
        # and its own `end`. depth starts at 1 (we are inside the gate).
        depth = 1
        has_else = False
        else_index = None
        closed = False
        close_index = None
        for walk, (_offset2, _end2, kind2, _rest2) in enumerate(
            tokens[index + 1 :], start=index + 1
        ):
            if kind2 in OPENERS:
                depth += 1
            elif kind2 == "end":
                depth -= 1
                if depth == 0:
                    closed = True
                    close_index = walk
                    break
            elif kind2 == "else":
                if depth == 1:
                    has_else = True
                    if else_index is None:
                        else_index = walk
            # `else if` neither opens nor closes; it continues the same block.
        if not closed:
            problems.append(
                f"{name}:{line_of(offset)}: unbalanced template block "
                f"for gate {rest.strip()!r} -- the depth walk never found its "
                "{{- end }}"
            )
            continue

        # `{{- if not .Values.<c>.deploy }}` IS the BYO branch: it renders only
        # when the component is external. Requiring a BYO `else` on it is
        # backwards, so it is structurally not a carve-out gate.
        if set(NEGATED_DEPLOY_REF.findall(rest)) >= set(components):
            continue

        # A whole-document existence gate -- `{{- if and .Values.<c>.deploy
        # <policy>.enabled }}` whose matching `end` is the file's final
        # template token, so every other action is inside it -- decides whether
        # the policy object renders at all. It selects no destination peer, so
        # there is nothing to bring your own; it is structurally not a carve-out
        # gate. The narrowness matters: a bare `.deploy` gate (mail-adapter's
        # own `mailAdapter.deploy`), or any gate that does not span the whole
        # document, is still checked and needs an else branch or an exemption.
        if (
            close_index == len(tokens) - 1
            and rest.strip().startswith("and ")
            and ENABLED_REF.search(rest)
        ):
            continue

        # An `{{- else }}` that renders NOTHING satisfies "has a BYO branch"
        # while leaving the #2317 bug fully intact: the carve-out still does not
        # render with the component external. So the branch must actually DO
        # one of the two things the fix means -- refuse the render (`fail`) or
        # render a replacement ipBlock peer (literally, or via a helper whose
        # name says ipBlock). Anything else is a vacuous else.
        else_is_substantive = False
        if has_else:
            else_body = text[tokens[else_index][1] : tokens[close_index][0]]
            else_is_substantive = bool(
                re.search(r"\bfail\b", else_body) or "ipBlock" in else_body
            )

        for component in components:
            seen.add((name, component))
            if has_else and else_is_substantive:
                continue
            if (name, component) in exemptions:
                continue
            if has_else:
                problems.append(
                    f"{name}:{line_of(offset)}: the carve-out gated on "
                    f".Values.{component}.deploy in {name} has an EMPTY "
                    "{{- else }} branch: it neither fails the render nor "
                    "renders an ipBlock peer.\n"
                    "    An else branch that renders nothing leaves the #2317 "
                    f"bug intact -- with {component}.deploy=false the consumer "
                    f"is pointed at an external {component} and STILL no egress "
                    "allow renders.\n"
                    "    Make the branch either refuse the render with a "
                    "{{ fail \"...\" }} naming the values key to set, or render "
                    "an explicit ipBlock peer for the external destination "
                    "(mirror the rustfs branch in security-networkpolicy.yaml); "
                    f"or add \"{name}:{component}=<written reason>\" to "
                    "DEPLOY_GATE_EXEMPTIONS in "
                    "charts/curie/ci/byo-endpoint-egress-assertions.sh."
                )
                continue
            problems.append(
                f"{name}:{line_of(offset)}: the carve-out gated on "
                f".Values.{component}.deploy in {name} has no {{{{- else }}}} branch.\n"
                f"    This is the #2317 class: the carve-out selects THIS release's "
                f"in-chart {component} pod, so with {component}.deploy=false the "
                f"consumer is pointed at an external {component} and NO egress allow "
                f"renders -- the policy silently drops the traffic and the install "
                f"still looks green.\n"
                f"    Fix it one of two ways:\n"
                f"      1. In charts/curie/templates/{name}, add a {{{{- else }}}} BYO "
                f"branch that requires and renders an explicit ipBlock peer for the "
                f"external {component} (mirror the rustfs branch in "
                f"security-networkpolicy.yaml), or\n"
                f"      2. Add \"{name}:{component}=<written reason>\" to "
                f"DEPLOY_GATE_EXEMPTIONS in "
                f"charts/curie/ci/byo-endpoint-egress-assertions.sh explaining why the "
                f"external form of this destination is already governed."
            )

if not seen:
    raise SystemExit(
        "the class guard found no .Values.<component>.deploy carve-out gates at "
        "all across "
        + ", ".join(sorted(p.name for p in template_paths))
        + ". The parser has drifted from the templates; fix the guard rather "
        "than deleting it."
    )

stale = sorted(f"{fname}:{component}" for fname, component in set(exemptions) - seen)
if stale:
    raise SystemExit(
        f"DEPLOY_GATE_EXEMPTIONS lists {stale!r}, which no longer has a .deploy "
        "carve-out gate in that template. Remove the stale exemption so the table "
        "keeps meaning something."
    )

if problems:
    raise SystemExit("\n".join(problems))

print(
    "ok: every .deploy carve-out gate in every NetworkPolicy template has a BYO "
    "else branch that fails or renders an ipBlock peer, or a written exemption "
    "(gates seen: "
    + ", ".join(f"{fname}:{component}" for fname, component in sorted(seen))
    + "; exempt: "
    + (", ".join(f"{fname}:{component}" for fname, component in sorted(exemptions)) or "none")
    + ")"
)
PY
then
  :
else
  fail "class guard: an in-chart .deploy carve-out has no BYO else branch and no exemption"
fi

echo
echo "PASS: BYO collector and API egress are required and rendered as runner NetworkPolicies; the in-chart carve-outs are unchanged; and no .deploy carve-out lacks a BYO branch."

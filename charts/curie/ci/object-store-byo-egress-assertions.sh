#!/usr/bin/env bash
#
# Render-assertion test for issue #2213. With a BYO object store
# (`rustfs.deploy: false`) Rail 1 is still fail-closed, and the only
# object-store egress carve-out the chart used to render selected the in-chart
# rustfs pod. Bundle-fetch then could not reach S3 or, on the key-free path,
# STS, so every sandbox sat in Init and the turn timed out.
#
# This pins the replacement contract:
#
#   1. The default (in-chart rustfs) render is unchanged: runner-allow-rustfs
#      still selects this release's rustfs pod on TCP 9000, and
#      runner-allow-object-store does not render. allowedEgress stays empty.
#   2. rustfs.deploy=false with rustfs.egress (and rustfs.stsEgress on the
#      key-free path) renders runner-allow-object-store covering those CIDRs
#      and does not render the in-chart rustfs pod selector.
#   3. rustfs.deploy=false without rustfs.egress fails at render, naming the
#      key, so a model-only allowedEgress list cannot ship a silently broken
#      BYO install.
#   4. The key-free path additionally requires rustfs.stsEgress (STS is a
#      different endpoint than S3). Static credentials do not.
#   5. Opting out of Rail 1 (`security.networkPolicy.enabled=false`) does not
#      require the BYO CIDRs: there is no fail-closed runner policy to satisfy.
#   6. #2368: rustfs.egress and rustfs.stsEgress share curie.objectStore.egressEntry
#      with the collector/API keys. A quarter-route prefix, an IPv6 ULA that
#      contains fd00:ec2::254 (including uncompressed spelling), a protocol-only
#      ports item, and an endPort range are refused; a /32, /24, /128, /48, and
#      named-port peer still render.
#
# NetworkPolicy speaks CIDRs, not DNS names, so the chart cannot derive an
# allow from rustfs.host. The values-level CIDR lists are the mechanism; the
# README's BYO section documents the EKS VPC interface-endpoint /32 pattern.
#
# Runnable locally and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RELEASE=curie
NAMESPACE=dev
RUSTFS_POLICY="${RELEASE}-runner-allow-rustfs"
OBJECT_STORE_POLICY="${RELEASE}-runner-allow-object-store"
DEFAULT_DENY_EGRESS="${RELEASE}-runner-default-deny-egress"
ALLOW_EGRESS="${RELEASE}-runner-allow-egress"

S3_CIDR=192.0.2.10/32
STS_CIDR=192.0.2.11/32

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

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
import json
import pathlib
import sys

import yaml

ROOT = pathlib.Path(sys.argv[1])
ACTION = sys.argv[2]


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


docs = load(ROOT)

if ACTION == "default":
    rustfs = named(docs, sys.argv[3])
    byo = named(docs, sys.argv[4])
    deny = named(docs, sys.argv[5])
    allow = named(docs, sys.argv[6])
    if rustfs is None:
        die("default render dropped runner-allow-rustfs")
    if byo is not None:
        die("default render must not emit runner-allow-object-store")
    if deny is None:
        die("default render dropped runner-default-deny-egress")
    if allow is not None:
        die("default render must keep allowedEgress empty (no runner-allow-egress)")
    labels = runner_selector(rustfs)
    if labels.get("app.kubernetes.io/component") != "runner-sandbox":
        die(f"runner-allow-rustfs selects {labels!r}, not runner-sandbox")
    peers = pod_peers(rustfs)
    if len(peers) != 1:
        die(f"runner-allow-rustfs expected one pod peer, found {len(peers)}")
    peer, ports = peers[0]
    component = (
        peer.get("podSelector", {})
        .get("matchLabels", {})
        .get("app.kubernetes.io/component")
    )
    if component != "rustfs":
        die(f"runner-allow-rustfs peer component is {component!r}, expected rustfs")
    if ports != [{"protocol": "TCP", "port": 9000}]:
        die(f"runner-allow-rustfs ports are {ports!r}, expected TCP 9000")
    if ip_blocks(rustfs):
        die("in-chart runner-allow-rustfs must keep using a pod selector, not ipBlock")
    print("ok: default render keeps in-chart rustfs pod allow and no BYO object-store policy")

elif ACTION == "byo-keyfree":
    rustfs = named(docs, sys.argv[3])
    byo = named(docs, sys.argv[4])
    deny = named(docs, sys.argv[5])
    allow = named(docs, sys.argv[6])
    s3_cidr = sys.argv[7]
    sts_cidr = sys.argv[8]
    if rustfs is not None:
        die("BYO render still emitted in-chart runner-allow-rustfs")
    if byo is None:
        die("BYO render missing runner-allow-object-store")
    if deny is None:
        die("BYO render dropped runner-default-deny-egress")
    if allow is not None:
        die("BYO render must keep allowedEgress empty (no runner-allow-egress)")
    labels = runner_selector(byo)
    if labels.get("app.kubernetes.io/component") != "runner-sandbox":
        die(f"runner-allow-object-store selects {labels!r}, not runner-sandbox")
    if byo.get("spec", {}).get("policyTypes") != ["Egress"]:
        die("runner-allow-object-store must be egress-only")
    blocks = ip_blocks(byo)
    cidrs = {cidr for cidr, _ports, _except in blocks}
    if cidrs != {s3_cidr, sts_cidr}:
        die(
            f"BYO object-store policy CIDRs are {sorted(cidrs)!r}; "
            f"expected exactly {{{s3_cidr}, {sts_cidr}}}"
        )
    for cidr, ports, excepts in blocks:
        if ports != [{"protocol": "TCP", "port": 443}]:
            die(f"{cidr} ports are {ports!r}, expected TCP 443")
        if excepts:
            die(f"{cidr} must not except anything on a /32 host allow; got {excepts}")
    print("ok: BYO key-free render allows the configured S3 and STS CIDRs")

elif ACTION == "byo-static":
    rustfs = named(docs, sys.argv[3])
    byo = named(docs, sys.argv[4])
    allow = named(docs, sys.argv[5])
    s3_cidr = sys.argv[6]
    sts_cidr = sys.argv[7]
    if rustfs is not None:
        die("static-key BYO render still emitted in-chart runner-allow-rustfs")
    if byo is None:
        die("static-key BYO render missing runner-allow-object-store")
    if allow is not None:
        die("static-key BYO render must keep allowedEgress empty (no runner-allow-egress)")
    cidrs = {cidr for cidr, _ports, _except in ip_blocks(byo)}
    if cidrs != {s3_cidr}:
        die(
            f"static-key BYO policy CIDRs are {sorted(cidrs)!r}; "
            f"expected only the S3 CIDR {s3_cidr} (STS is key-free-only)"
        )
    if sts_cidr in cidrs:
        die("static-key BYO must not require or emit an STS allow")
    print("ok: static-key BYO render allows only the object-store CIDR")

elif ACTION == "has-block":
    policy_name, cidr, ports_json = sys.argv[3], sys.argv[4], sys.argv[5]
    expected_ports = json.loads(ports_json)
    policy = named(docs, policy_name)
    if policy is None:
        die(f"missing NetworkPolicy/{policy_name}")
    matches = [
        (block_cidr, ports, excepts)
        for block_cidr, ports, excepts in ip_blocks(policy)
        if block_cidr == cidr
    ]
    if not matches:
        die(
            f"{policy_name} has no ipBlock {cidr}; "
            f"blocks={ip_blocks(policy)!r}"
        )
    for block_cidr, ports, excepts in matches:
        if ports != expected_ports:
            die(f"{cidr} ports are {ports!r}, expected {expected_ports!r}")
        if excepts:
            die(f"{cidr} must not except anything on a narrow allow; got {excepts}")
    print(f"ok: {policy_name} allows {cidr} with {expected_ports}")

else:
    die(f"unknown action {ACTION!r}")
PY

echo "=== Assertion 1: default render is unchanged (in-chart rustfs allow, no BYO policy) ==="
DEFAULT_OUT="$(render_dir default)"
python3 "$CHECKER" "$DEFAULT_OUT" default \
  "$RUSTFS_POLICY" "$OBJECT_STORE_POLICY" "$DEFAULT_DENY_EGRESS" "$ALLOW_EGRESS" \
  || fail "default render changed the in-chart rustfs egress carve-out or opened a BYO policy"

echo "=== Assertion 2: BYO key-free path renders S3 and STS ipBlock allows ==="
KEYFREE_VALUES="$TMP/keyfree.yaml"
cat > "$KEYFREE_VALUES" <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  auth:
    accessKey: ""
  egress:
    - cidr: ${S3_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
  stsEgress:
    - cidr: ${STS_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
api:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-api
worker:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-worker
agentSandbox:
  runner:
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-runner
EOF
KEYFREE_OUT="$(render_dir keyfree --values "$KEYFREE_VALUES")"
python3 "$CHECKER" "$KEYFREE_OUT" byo-keyfree \
  "$RUSTFS_POLICY" "$OBJECT_STORE_POLICY" "$DEFAULT_DENY_EGRESS" "$ALLOW_EGRESS" \
  "$S3_CIDR" "$STS_CIDR" \
  || fail "BYO key-free render did not emit runner-allow-object-store covering S3 and STS"

echo "=== Assertion 3: static-key BYO renders only the object-store CIDR ==="
STATIC_VALUES="$TMP/static.yaml"
cat > "$STATIC_VALUES" <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
    - cidr: ${S3_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
EOF
STATIC_OUT="$(render_dir static --values "$STATIC_VALUES")"
python3 "$CHECKER" "$STATIC_OUT" byo-static \
  "$RUSTFS_POLICY" "$OBJECT_STORE_POLICY" "$ALLOW_EGRESS" "$S3_CIDR" "$STS_CIDR" \
  || fail "static-key BYO render did not emit a rustfs.egress-only object-store policy"

echo "=== Assertion 4: BYO without rustfs.egress fails at render, naming the key ==="
MISSING_EGRESS_VALUES="$TMP/missing-egress.yaml"
cat > "$MISSING_EGRESS_VALUES" <<'EOF'
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
EOF
MISSING_EGRESS="$TMP/missing-egress.txt"
if helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
  --values "$MISSING_EGRESS_VALUES" \
  > /dev/null 2>"$MISSING_EGRESS"; then
  fail "rustfs.deploy=false without rustfs.egress must fail Helm rendering"
fi
if ! grep -q "rustfs.egress" "$MISSING_EGRESS"; then
  fail "missing-egress render failed without naming rustfs.egress; output was: $(cat "$MISSING_EGRESS")"
fi
echo "ok: BYO without rustfs.egress is refused at render"

echo "=== Assertion 5: key-free BYO without rustfs.stsEgress fails at render, naming the key ==="
MISSING_STS_VALUES="$TMP/missing-sts.yaml"
cat > "$MISSING_STS_VALUES" <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  auth:
    accessKey: ""
  egress:
    - cidr: ${S3_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
api:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-api
worker:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-worker
agentSandbox:
  runner:
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-runner
EOF
MISSING_STS="$TMP/missing-sts.txt"
if helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
  --values "$MISSING_STS_VALUES" \
  > /dev/null 2>"$MISSING_STS"; then
  fail "key-free BYO without rustfs.stsEgress must fail Helm rendering"
fi
if ! grep -q "rustfs.stsEgress" "$MISSING_STS"; then
  fail "missing-sts render failed without naming rustfs.stsEgress; output was: $(cat "$MISSING_STS")"
fi
echo "ok: key-free BYO without rustfs.stsEgress is refused at render"

echo "=== Assertion 6: Rail 1 off does not require BYO object-store CIDRs ==="
if ! helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
  --set rustfs.deploy=false \
  --set rustfs.host=s3.example.com \
  --set rustfs.port=443 \
  --set security.networkPolicy.enabled=false \
  > /dev/null; then
  fail "rustfs.deploy=false with networkPolicy.enabled=false must still render"
fi
echo "ok: opting out of Rail 1 does not require rustfs.egress"

must_fail_naming() {
  local label="$1"
  local needle="$2"
  local values="$3"
  local out="$TMP/${label}.txt"
  if helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
    --values "$values" \
    > /dev/null 2>"$out"; then
    fail "${label} must fail Helm rendering"
  fi
  if ! grep -q "$needle" "$out"; then
    fail "${label} failed without naming ${needle}; output was: $(cat "$out")"
  fi
  echo "ok: ${label} is refused at render"
}

echo "=== Assertion 7: rustfs.egress default route is refused ==="
DEFAULT_ROUTE_VALUES="$TMP/default-route.yaml"
cat > "$DEFAULT_ROUTE_VALUES" <<'EOF'
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
    - cidr: 0.0.0.0/0
      ports: [{ protocol: TCP, port: 443 }]
EOF
must_fail_naming "default-route rustfs.egress" "rustfs.egress" "$DEFAULT_ROUTE_VALUES"

echo "=== Assertion 8: rustfs.egress must not allow the metadata host ==="
IMDS_VALUES="$TMP/imds.yaml"
cat > "$IMDS_VALUES" <<'EOF'
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
    - cidr: 169.254.169.254/32
      ports: [{ protocol: TCP, port: 443 }]
EOF
must_fail_naming "metadata-host rustfs.egress" "169.254.169.254" "$IMDS_VALUES"

echo "=== Assertion 9: rustfs.egress entries require ports ==="
NO_PORTS_VALUES="$TMP/no-ports.yaml"
cat > "$NO_PORTS_VALUES" <<'EOF'
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
    - cidr: 192.0.2.10/32
EOF
must_fail_naming "ports-missing rustfs.egress" "ports" "$NO_PORTS_VALUES"

write_rustfs_entry_values() {
  local path="$1"
  local key="$2"
  local entry="$3"
  if [[ "$key" == "rustfs.egress" ]]; then
    cat > "$path" <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  egress:
${entry}
EOF
  else
    cat > "$path" <<EOF
rustfs:
  deploy: false
  host: s3.example.com
  port: 443
  auth:
    accessKey: ""
  egress:
    - cidr: ${S3_CIDR}
      ports: [{ protocol: TCP, port: 443 }]
  stsEgress:
${entry}
api:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-api
worker:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-worker
agentSandbox:
  runner:
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-runner
EOF
  fi
}

must_render_block() {
  local label="$1"
  local cidr="$2"
  local ports_json="$3"
  local values="$4"
  local out
  out="$(render_dir "${label// /_}" --values "$values")" \
    || fail "${label} must render"
  python3 "$CHECKER" "$out" has-block "$OBJECT_STORE_POLICY" "$cidr" "$ports_json" \
    || fail "${label} did not render ipBlock ${cidr} with ports ${ports_json}"
}

echo "=== Assertion 10: shared validator enforces one-peer floor, IPv6 metadata, and explicit ports on both rustfs keys ==="
for KEY in rustfs.egress rustfs.stsEgress; do
  write_rustfs_entry_values "$TMP/${KEY}-default-route.yaml" "$KEY" \
    $'    - cidr: 0.0.0.0/0\n      ports: [{ protocol: TCP, port: 443 }]'
  must_fail_naming "default-route ${KEY}" "$KEY" "$TMP/${KEY}-default-route.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-imds-v4.yaml" "$KEY" \
    $'    - cidr: 169.254.0.0/16\n      ports: [{ protocol: TCP, port: 443 }]'
  must_fail_naming "metadata-covering ${KEY}" "169.254.169.254" "$TMP/${KEY}-imds-v4.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-no-ports.yaml" "$KEY" \
    $'    - cidr: 192.0.2.30/32'
  must_fail_naming "ports-missing ${KEY}" "ports" "$TMP/${KEY}-no-ports.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-quarter-route.yaml" "$KEY" \
    $'    - cidr: 64.0.0.0/2\n      ports: [{ protocol: TCP, port: 443 }]'
  must_fail_naming "quarter-route ${KEY}" "IPv4 /8, IPv6 /32" "$TMP/${KEY}-quarter-route.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-ula8.yaml" "$KEY" \
    $'    - cidr: fd00::/8\n      ports: [{ protocol: TCP, port: 443 }]'
  must_fail_naming "ipv6-ula-8 ${KEY}" "169.254.169.254" "$TMP/${KEY}-ula8.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-ula7.yaml" "$KEY" \
    $'    - cidr: fc00::/7\n      ports: [{ protocol: TCP, port: 443 }]'
  must_fail_naming "ipv6-ula-7 ${KEY}" "169.254.169.254" "$TMP/${KEY}-ula7.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-ula-uncompressed.yaml" "$KEY" \
    $'    - cidr: fd00:0ec2::/48\n      ports: [{ protocol: TCP, port: 443 }]'
  must_fail_naming "ipv6-ula-uncompressed ${KEY}" "169.254.169.254" "$TMP/${KEY}-ula-uncompressed.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-protocol-only.yaml" "$KEY" \
    $'    - cidr: 192.0.2.30/32\n      ports: [{ protocol: TCP }]'
  must_fail_naming "protocol-only ${KEY}" "each ports item must set port" "$TMP/${KEY}-protocol-only.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-endport.yaml" "$KEY" \
    $'    - cidr: 192.0.2.30/32\n      ports: [{ protocol: TCP, port: 1, endPort: 65535 }]'
  must_fail_naming "endport ${KEY}" "endPort" "$TMP/${KEY}-endport.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-p1.yaml" "$KEY" \
    $'    - cidr: 192.0.2.30/32\n      ports: [{ protocol: TCP, port: 443 }]'
  must_render_block "narrow-v4-32 ${KEY}" "192.0.2.30/32" \
    '[{"protocol": "TCP", "port": 443}]' "$TMP/${KEY}-p1.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-p2.yaml" "$KEY" \
    $'    - cidr: 192.0.2.0/24\n      ports: [{ protocol: TCP, port: 443 }]'
  must_render_block "narrow-v4-24 ${KEY}" "192.0.2.0/24" \
    '[{"protocol": "TCP", "port": 443}]' "$TMP/${KEY}-p2.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-p3.yaml" "$KEY" \
    $'    - cidr: 2001:db8::10/128\n      ports: [{ protocol: TCP, port: 4318 }]'
  must_render_block "narrow-v6-128 ${KEY}" "2001:db8::10/128" \
    '[{"protocol": "TCP", "port": 4318}]' "$TMP/${KEY}-p3.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-p4.yaml" "$KEY" \
    $'    - cidr: 2001:db8::/48\n      ports: [{ protocol: TCP, port: 443 }]'
  must_render_block "narrow-v6-48 ${KEY}" "2001:db8::/48" \
    '[{"protocol": "TCP", "port": 443}]' "$TMP/${KEY}-p4.yaml"

  write_rustfs_entry_values "$TMP/${KEY}-p5.yaml" "$KEY" \
    $'    - cidr: 192.0.2.30/32\n      ports: [{ protocol: TCP, port: https }]'
  must_render_block "named-port ${KEY}" "192.0.2.30/32" \
    '[{"protocol": "TCP", "port": "https"}]' "$TMP/${KEY}-p5.yaml"
done

echo
echo "PASS: BYO object-store egress is required and rendered as a runner NetworkPolicy; the default in-chart rustfs allow is unchanged."

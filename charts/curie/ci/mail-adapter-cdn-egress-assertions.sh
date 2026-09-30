#!/usr/bin/env bash
#
# Render-assertion test for the mail adapter's AgentMail egress shape when the
# provider is fronted by a CDN (#2824). AgentMail's API sits behind CloudFront,
# whose edge addresses move; /32 pins resolved at install time went stale on the
# soak install on 2026-09-18 and every HTTPS open from the pod was refused.
#
#   1  mailAdapter.agentmail.egressMode defaults to `cidrs`: the existing render
#      (one ipBlock per declared CIDR on TCP 443, the same list handed to the
#      adapter as CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS) is unchanged.
#   2  egressMode=publicHttps renders public TCP 443 as one IPv4 and one IPv6
#      ipBlock whose `except` lists carry every private, loopback, link-local,
#      CGNAT, multicast and reserved range, so an edge anywhere on the public
#      internet is admitted and the cluster, node and metadata networks are not.
#      mailAdapter.agentmail.publicHttpsExcept adds operator ranges (a cluster
#      CIDR in public space) to the matching family's except list, and an
#      IPv4-mapped IPv6 extra (refused by the API server) refuses the render.
#   3  In publicHttps the adapter gets an EMPTY CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS,
#      so it dials what DNS returns instead of pinning to a stale snapshot.
#   4  publicHttps needs no httpsCidrs; setting httpsCidrs in publicHttps, or
#      publicHttpsExcept in cidrs mode, refuses the render (a silently ignored
#      key leaves an operator believing a peer or exclusion exists), and an
#      unknown egressMode refuses naming the key.
#   5  cidrs mode still requires httpsCidrs and still refuses default routes.
#
# Renders to a directory, never a stdout pipe: see the NOTE in
# mail-adapter-wiring-assertions.sh about silent truncation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

RELEASE=curie
CREDS=(
  --set mailAdapter.deploy=true
  --set mailAdapter.channelToken=chn-assert-token
  --set mailAdapter.egressSecret=egress-assert-secret
  --set mailAdapter.agentmail.apiKey=am-assert-key
)

render() {
  local label="$1"
  shift
  local out="$TMP/render-$label"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --namespace default --output-dir "$out" "$@" >/dev/null \
    || fail "$label: helm template exited non-zero (see the error above)"
  echo "$out"
}

assert_render_fails_named() {
  # $1 label, $2 expected configuration key in the error, remaining helm args.
  local label="$1" expected_key="$2" output rc
  shift 2
  set +e
  output="$(helm template "$RELEASE" "$CHART" "$@" 2>&1)"
  rc=$?
  set -e
  [ "$rc" -ne 0 ] \
    || fail "$label rendered successfully; this configuration must fail closed"
  case "$output" in
    *"$expected_key"*) : ;;
    *) fail "$label failed without naming '$expected_key'; output was: $output" ;;
  esac
}

# Prints JSON {"https_rules": [...], "env": "<CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS>"}
# where https_rules are the egress rules of the mail-adapter policy on TCP 443.
read_shape() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys

import yaml

docs = []
for path in pathlib.Path(sys.argv[1]).rglob("*.yaml"):
    docs.extend(d for d in yaml.safe_load_all(path.read_text()) if d)
policy = next(
    d for d in docs
    if d.get("kind") == "NetworkPolicy" and d["metadata"]["name"] == "curie-mail-adapter-egress"
)
rules = [
    r for r in policy["spec"]["egress"]
    if any(p.get("port") == 443 for p in r.get("ports", []))
]
deploy = next(
    d for d in docs
    if d.get("kind") == "Deployment" and d["metadata"]["name"] == "curie-mail-adapter"
)
env = {
    e["name"]: e.get("value")
    for c in deploy["spec"]["template"]["spec"]["containers"]
    for e in c.get("env", [])
}
print(json.dumps({"https_rules": rules, "env": env.get("CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS")}))
PY
}

# ---------------------------------------------------------------------------
# 1: the default mode is the existing narrow-CIDR render.
# ---------------------------------------------------------------------------
cidrs_dir="$(render cidrs "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=18.160.0.0/15' \
  --set-string 'mailAdapter.agentmail.httpsCidrs[1]=198.51.100.7/32')"
python3 - "$(read_shape "$cidrs_dir")" <<'PY' \
  || fail "default egressMode did not render the declared CIDRs exactly (assertion 1)"
import json, sys
shape = json.loads(sys.argv[1])
rules = shape["https_rules"]
assert len(rules) == 1, rules
assert rules[0]["to"] == [
    {"ipBlock": {"cidr": "18.160.0.0/15"}},
    {"ipBlock": {"cidr": "198.51.100.7/32"}},
], rules
assert shape["env"] == "18.160.0.0/15,198.51.100.7/32", shape["env"]
PY

# ---------------------------------------------------------------------------
# 2 + 3: publicHttps admits any public edge, excludes exactly the
# special-purpose ranges, and hands the adapter no pin list. Rendered twice:
# the documented `publicHttpsExcept: []` path, and with operator extras that
# are NOT already in the default lists, so the append itself is proven.
# ---------------------------------------------------------------------------
public_dir="$(render public "${CREDS[@]}" \
  --set mailAdapter.agentmail.egressMode=publicHttps)"
public_extra_dir="$(render public-extra "${CREDS[@]}" \
  --set mailAdapter.agentmail.egressMode=publicHttps \
  --set-string 'mailAdapter.agentmail.publicHttpsExcept[0]=34.118.224.0/20' \
  --set-string 'mailAdapter.agentmail.publicHttpsExcept[1]=2600:1f18::/32')"
python3 - "$(read_shape "$public_dir")" "$(read_shape "$public_extra_dir")" <<'PY' \
  || fail "egressMode=publicHttps did not render public TCP 443 with exactly the special-purpose ranges excepted (assertions 2-3)"
import ipaddress, json, sys

DEFAULT_V4 = {
    "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
    "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.168.0.0/16",
    "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
}
DEFAULT_V6 = {
    "::/128", "::1/128", "64:ff9b:1::/48", "100::/64", "2001:db8::/32",
    "fc00::/7", "fe80::/10", "ff00::/8",
}

def blocks_of(raw):
    shape = json.loads(raw)
    rules = shape["https_rules"]
    assert len(rules) == 1, rules
    blocks = {b["ipBlock"]["cidr"]: set(b["ipBlock"].get("except", [])) for b in rules[0]["to"]}
    assert set(blocks) == {"0.0.0.0/0", "::/0"}, blocks
    assert shape["env"] == "", shape["env"]
    return blocks

def admitted(blocks, addr):
    a = ipaddress.ip_address(addr)
    return any(
        a in ipaddress.ip_network(cidr) and not any(a in ipaddress.ip_network(e) for e in exc)
        for cidr, exc in blocks.items()
    )

plain = blocks_of(sys.argv[1])
assert plain["0.0.0.0/0"] == DEFAULT_V4, plain["0.0.0.0/0"] ^ DEFAULT_V4
assert plain["::/0"] == DEFAULT_V6, plain["::/0"] ^ DEFAULT_V6
extra = blocks_of(sys.argv[2])
assert extra["0.0.0.0/0"] == DEFAULT_V4 | {"34.118.224.0/20"}, extra["0.0.0.0/0"]
assert extra["::/0"] == DEFAULT_V6 | {"2600:1f18::/32"}, extra["::/0"]

for blocks in (plain, extra):
    v4, v6 = blocks["0.0.0.0/0"], blocks["::/0"]
    # Kubernetes rejects an except outside its cidr's family, and (observed
    # applying ::ffff:0:0/96 on k3s, 2026-09-18: "must not have an IPv4-mapped
    # IPv6 address") any IPv4-mapped IPv6 except.
    assert all(ipaddress.ip_network(c).version == 4 for c in v4), v4
    assert all(ipaddress.ip_network(c).version == 6 for c in v6), v6
    assert not any(ipaddress.ip_network(c).network_address.ipv4_mapped for c in v6), v6
    # The CloudFront edges seen on 2026-09-18 and the stale pins are admitted.
    for edge in ("18.160.46.21", "18.160.46.64", "18.160.41.105", "18.160.71.92"):
        assert admitted(blocks, edge), edge
    for internal in ("169.254.169.254", "10.43.0.10", "192.168.1.1", "127.0.0.1", "100.64.0.1", "fd00::1", "fe80::1"):
        assert not admitted(blocks, internal), internal
assert admitted(plain, "34.118.224.5") and not admitted(extra, "34.118.224.5")
assert admitted(plain, "2600:1f18::5") and not admitted(extra, "2600:1f18::5")
PY

# ---------------------------------------------------------------------------
# 4: silent-ignore and unknown-mode refusals.
# ---------------------------------------------------------------------------
assert_render_fails_named \
  "publicHttps with httpsCidrs also set" \
  "mailAdapter.agentmail.httpsCidrs" \
  "${CREDS[@]}" \
  --set mailAdapter.agentmail.egressMode=publicHttps \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=18.160.41.105/32'
assert_render_fails_named \
  "cidrs mode with publicHttpsExcept set" \
  "mailAdapter.agentmail.publicHttpsExcept" \
  "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=18.160.0.0/15' \
  --set-string 'mailAdapter.agentmail.publicHttpsExcept[0]=203.0.113.0/24'
assert_render_fails_named \
  "unknown egressMode" \
  "mailAdapter.agentmail.egressMode" \
  "${CREDS[@]}" \
  --set mailAdapter.agentmail.egressMode=fqdn \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=18.160.0.0/15'
assert_render_fails_named \
  "publicHttpsExcept IPv4-mapped IPv6 range" \
  "mailAdapter.agentmail.publicHttpsExcept" \
  "${CREDS[@]}" \
  --set mailAdapter.agentmail.egressMode=publicHttps \
  --set-string 'mailAdapter.agentmail.publicHttpsExcept[0]=::ffff:0:0/96'
assert_render_fails_named \
  "publicHttpsExcept default route" \
  "mailAdapter.agentmail.publicHttpsExcept" \
  "${CREDS[@]}" \
  --set mailAdapter.agentmail.egressMode=publicHttps \
  --set-string 'mailAdapter.agentmail.publicHttpsExcept[0]=0.0.0.0/0'

# ---------------------------------------------------------------------------
# 5: cidrs mode keeps its fail-closed gates.
# ---------------------------------------------------------------------------
assert_render_fails_named \
  "cidrs mode with no httpsCidrs" \
  "mailAdapter.agentmail.httpsCidrs" \
  "${CREDS[@]}"
assert_render_fails_named \
  "cidrs mode default route" \
  "mailAdapter.agentmail.httpsCidrs" \
  "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=0.0.0.0/0'

echo "mail adapter CDN egress assertions: all passed"

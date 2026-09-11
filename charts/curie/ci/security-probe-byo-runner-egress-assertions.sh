#!/usr/bin/env bash
#
# Render-assertion test for issue #2369. This file is the fix pin.
#
# Render-only: helm template --show-only templates/security-probe.yaml.
# Live proof that a runner-labelled pod can actually reach each declared BYO
# peer (and that an undeclared control is blocked) is
# scripts/check-runner-byo-egress.sh, not this script.
#
# Claim 1 already curls BLOCKED to prove CNI enforcement. It never curls a
# declared BYO peer, so a SaaS OTLP collector or API behind rotating LB
# addresses can drop traffic while helm-template of the NetworkPolicy is
# green. Claim 1d injects BYO_RUNNER_TARGETS (space-separated name=host:port)
# for the same conditions that render the BYO runner allows, then nc/curl
# each target and curl BLOCKED as the undeclared control.
#
# Asserts:
#
#   1. default: BYO_RUNNER_TARGETS is empty. The probe script still carries a
#      Claim 1d SKIP path that names BYO_RUNNER_TARGETS (gated on empty).
#      Claim 1 still curls BLOCKED; Claim 1d does not have to run on default.
#   2. static-key rustfs BYO (deploy=false + host + egress /32, default
#      accessKey): target includes rustfs=s3.example.com:443. The script reads
#      BYO_RUNNER_TARGETS, nc/curl those targets, and curls BLOCKED when
#      targets are non-empty.
#   3. key-free rustfs BYO (accessKey empty + stsEgress): targets include
#      rustfs=s3.example.com:443 and sts=192.0.2.11:443 (CIDR prefix stripped).
#   4. otelCollector BYO with https://otlp.example.net:4318: target includes
#      otel=otlp.example.net:4318 and that otel token must not use :443.
#   5. api BYO (deploy=false + dispatcher/ui apiBaseUrl + api.egress): target
#      includes api=api.example.net:443 (https scheme default).
#   6. NEGATIVE: rustfs.host set while rustfs.deploy stays true (no egress).
#      BYO_RUNNER_TARGETS must not contain s3.example.com; the in-chart pod
#      selector still governs.
#   7. Dedicated default pin: the Claim 1d skip path exists AND Claim 1 still
#      curls BLOCKED (Claim 1d must not replace the undeclared before/after).
#   8. Guard: same otel BYO as 4; the otel= token port is 4318, not 443.
#
# Addresses are RFC 5737 TEST-NET-1 and example.net only. Runnable locally
# and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"

cleanup() {
  [[ -n "${TMP:-}" && -d "$TMP" ]] && rm -rf -- "$TMP"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render_probe() {
  local name="$1"
  shift
  local out="$TMP/${name}.yaml"
  helm template curie "$CHART" \
    --show-only templates/security-probe.yaml \
    "$@" >"$out"
  printf '%s\n' "$out"
}

S3_CIDR=192.0.2.10/32
STS_CIDR=192.0.2.11/32
COLLECTOR_CIDR=192.0.2.20/32
API_CIDR=192.0.2.21/32
OTLP_ENDPOINT=https://otlp.example.net:4318
API_BASE_URL=https://api.example.net

CHECKER="$TMP/check.py"
cat > "$CHECKER" <<'PY'
import pathlib
import sys

import yaml


def die(message):
    raise SystemExit(message)


def load_probe_job(path):
    docs = [doc for doc in yaml.safe_load_all(pathlib.Path(path).read_text()) if doc]
    jobs = [doc for doc in docs if doc.get("kind") == "Job"]
    if len(jobs) != 1:
        die(f"{path}: expected exactly one Job, found {len(jobs)}")
    containers = (
        jobs[0]
        .get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )
    probes = [container for container in containers if container.get("name") == "probe"]
    if len(probes) != 1:
        die(f"{path}: expected exactly one probe container, found {len(probes)}")
    return probes[0]


def env_entry(container, name):
    entries = [
        entry for entry in container.get("env", []) if entry.get("name") == name
    ]
    if len(entries) != 1:
        die(
            f"probe env {name!r} is required as exactly one literal value, "
            f"found {len(entries)} entries"
        )
    if set(entries[0]) != {"name", "value"}:
        die(f"probe env {name!r} must be one literal value, got {entries[0]!r}")
    return entries[0]["value"]


def command_script(container):
    command = container.get("command") or []
    if not command:
        die("probe container has no command")
    return "\n".join(str(part) for part in command)


def claim_1d_section(script):
    start = script.find("Claim 1d")
    if start < 0:
        die("probe script must contain a Claim 1d path")
    end = script.find("Claim 2", start)
    if end < 0:
        end = len(script)
    return script[start:end]


def claim_1_before_1d(script):
    start = script.find("Claim 1:")
    if start < 0:
        start = script.find("== Claim 1")
    if start < 0:
        die("probe script must still contain Claim 1")
    end = script.find("Claim 1d", start)
    if end < 0:
        end = script.find("Claim 2", start)
        if end < 0:
            end = len(script)
    return script[start:end]


def require_claim1d_skip(script):
    section = claim_1d_section(script)
    if "BYO_RUNNER_TARGETS" not in section:
        die(
            "Claim 1d SKIP path must name BYO_RUNNER_TARGETS so the empty "
            "list is an explicit skip, not a silent disappearance"
        )
    if "SKIP" not in section:
        die("Claim 1d must SKIP when BYO_RUNNER_TARGETS is empty")
    gated = (
        '-z "$BYO_RUNNER_TARGETS"' in section
        or '-z "${BYO_RUNNER_TARGETS}"' in section
        or '-n "$BYO_RUNNER_TARGETS"' in section
        or '-n "${BYO_RUNNER_TARGETS}"' in section
    )
    if not gated:
        die("Claim 1d skip must be gated on empty BYO_RUNNER_TARGETS")


def require_claim1_blocked(script):
    body = claim_1_before_1d(script)
    if "https://${BLOCKED}/" not in body:
        die(
            "Claim 1 must still curl BLOCKED; Claim 1d must not replace the "
            "undeclared before/after CNI proof"
        )


def require_claim1d_probes_targets(script):
    section = claim_1d_section(script)
    if "$BYO_RUNNER_TARGETS" not in section:
        die(
            "probe script must read BYO_RUNNER_TARGETS at runtime so the "
            "declared peers are not a dead env var"
        )
    uses_nc = "nc -z" in section or " nc " in section
    uses_curl = "curl" in section or "probe_rc" in section
    if not (uses_nc or uses_curl):
        die("Claim 1d must nc or curl each BYO_RUNNER_TARGETS peer")
    if "https://${BLOCKED}/" not in section:
        die(
            "Claim 1d must curl BLOCKED when targets are non-empty "
            "(undeclared control)"
        )


def main():
    path, mode = sys.argv[1], sys.argv[2]
    container = load_probe_job(path)
    raw = env_entry(container, "BYO_RUNNER_TARGETS")
    if raw is None:
        die("probe env 'BYO_RUNNER_TARGETS' is required as a literal value")
    targets = str(raw).split()
    script = command_script(container)

    if mode == "default":
        if str(raw).strip() != "":
            die(f"default BYO_RUNNER_TARGETS must be empty, got {raw!r}")
        require_claim1d_skip(script)
        require_claim1_blocked(script)
        print("  ok: default BYO_RUNNER_TARGETS is empty; Claim 1d skip is gated on it")
        return

    if mode == "rustfs-static":
        if "rustfs=s3.example.com:443" not in targets:
            die(
                "static-key BYO must include 'rustfs=s3.example.com:443'; "
                f"got {targets!r}"
            )
        if any(item.startswith("sts=") for item in targets):
            die(f"static-key BYO must not include an sts= target; got {targets!r}")
        if any("192.0.2.10" in item for item in targets):
            die(
                "rustfs target must use rustfs.host:port, not the egress CIDR; "
                f"got {targets!r}"
            )
        require_claim1d_probes_targets(script)
        print("  ok: static-key BYO lists rustfs=s3.example.com:443 and probes it")
        return

    if mode == "rustfs-keyfree":
        if "rustfs=s3.example.com:443" not in targets:
            die(
                "key-free BYO must include 'rustfs=s3.example.com:443'; "
                f"got {targets!r}"
            )
        if "sts=192.0.2.11:443" not in targets:
            die(
                "key-free BYO must include 'sts=192.0.2.11:443' (CIDR prefix "
                f"stripped); got {targets!r}"
            )
        sts_tokens = [item for item in targets if item.startswith("sts=")]
        if any("/" in item for item in sts_tokens):
            die(f"sts token must strip the /prefix from cidr; got {sts_tokens!r}")
        require_claim1d_probes_targets(script)
        print("  ok: key-free BYO lists rustfs host:port and sts IP:port")
        return

    if mode == "otel":
        if "otel=otlp.example.net:4318" not in targets:
            die(
                "BYO otel must include 'otel=otlp.example.net:4318'; "
                f"got {targets!r}"
            )
        otel_tokens = [item for item in targets if item.startswith("otel=")]
        if any(item.rsplit(":", 1)[-1] == "443" for item in otel_tokens):
            die(
                "otel token must not use :443 when the URL is "
                f"https://otlp.example.net:4318; got {otel_tokens!r}"
            )
        require_claim1d_probes_targets(script)
        print("  ok: BYO otel lists otel=otlp.example.net:4318")
        return

    if mode == "api":
        if "api=api.example.net:443" not in targets:
            die(
                "BYO api must include 'api=api.example.net:443' (https scheme "
                f"default); got {targets!r}"
            )
        require_claim1d_probes_targets(script)
        print("  ok: BYO api lists api=api.example.net:443")
        return

    if mode == "host-ignored-when-deployed":
        if any("s3.example.com" in item for item in targets):
            die(
                "rustfs.host must not populate BYO_RUNNER_TARGETS while "
                f"rustfs.deploy stays true; got {targets!r}"
            )
        print("  ok: rustfs.host is ignored for Claim 1d while deploy is true")
        return

    if mode == "default-skip-and-claim1":
        if str(raw).strip() != "":
            die(f"default BYO_RUNNER_TARGETS must be empty, got {raw!r}")
        require_claim1d_skip(script)
        require_claim1_blocked(script)
        print("  ok: default Claim 1d skip exists and Claim 1 still curls BLOCKED")
        return

    if mode == "otel-port-guard":
        otel_tokens = [item for item in targets if item.startswith("otel=")]
        if "otel=otlp.example.net:4318" not in otel_tokens:
            die(
                "guard: otel= token port must be 4318 (URL-explicit), "
                f"got {otel_tokens!r} in {targets!r}"
            )
        if any(item.rsplit(":", 1)[-1] == "443" for item in otel_tokens):
            die(
                "guard: otel= token must not collapse "
                "https://otlp.example.net:4318 to port 443; "
                f"got {otel_tokens!r}"
            )
        print("  ok: otel= token keeps explicit :4318 and does not emit :443")
        return

    die(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
PY

echo "=== Assertion 1: default BYO_RUNNER_TARGETS is empty; Claim 1d skip is present ==="
DEFAULT_RENDER="$(render_probe default)"
python3 "$CHECKER" "$DEFAULT_RENDER" default

echo "=== Assertion 2: static-key rustfs BYO lists rustfs=s3.example.com:443 and probes it ==="
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
STATIC_RENDER="$(render_probe rustfs-static --values "$STATIC_VALUES")"
python3 "$CHECKER" "$STATIC_RENDER" rustfs-static

echo "=== Assertion 3: key-free rustfs BYO lists rustfs host:port and sts IP:port ==="
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
KEYFREE_RENDER="$(render_probe rustfs-keyfree --values "$KEYFREE_VALUES")"
python3 "$CHECKER" "$KEYFREE_RENDER" rustfs-keyfree

echo "=== Assertion 4: BYO otel lists otel=otlp.example.net:4318, not :443 ==="
OTEL_VALUES="$TMP/otel.yaml"
cat > "$OTEL_VALUES" <<EOF
otelCollector:
  deploy: false
  endpoint: ${OTLP_ENDPOINT}
  egress:
    - cidr: ${COLLECTOR_CIDR}
      ports: [{ protocol: TCP, port: 4318 }]
EOF
OTEL_RENDER="$(render_probe otel-byo --values "$OTEL_VALUES")"
python3 "$CHECKER" "$OTEL_RENDER" otel

echo "=== Assertion 5: BYO api lists api=api.example.net:443 ==="
API_VALUES="$TMP/api.yaml"
cat > "$API_VALUES" <<EOF
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
API_RENDER="$(render_probe api-byo --values "$API_VALUES")"
python3 "$CHECKER" "$API_RENDER" api

echo "=== Assertion 6: rustfs.host is ignored while deploy stays true ==="
HOST_SET_RENDER="$(render_probe host-set --set rustfs.host=s3.example.com)"
python3 "$CHECKER" "$HOST_SET_RENDER" host-ignored-when-deployed

echo "=== Assertion 7: default Claim 1d skip exists and Claim 1 still curls BLOCKED ==="
python3 "$CHECKER" "$DEFAULT_RENDER" default-skip-and-claim1

echo "=== Assertion 8: otel= token port is 4318, not 443 ==="
python3 "$CHECKER" "$OTEL_RENDER" otel-port-guard

echo "PASS: security probe Claim 1d lists BYO runner peers in BYO_RUNNER_TARGETS (rustfs host:port, sts CIDR-IP:port, otel dialPort, api scheme-default), skips when empty, still curls Claim 1 BLOCKED, and ignores rustfs.host while deploy stays true"

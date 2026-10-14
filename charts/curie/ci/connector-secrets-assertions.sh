#!/usr/bin/env bash
# Render assertions for per-agent connector Secret storage and pod delivery.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

resource() {
  local rendered="$1"
  local kind="$2"
  local name="$3"
  awk -v kind="$kind" -v name="$name" '
    /^---$/ {
      if (document != "" && found_kind && found_name) {
        print document
        emitted = 1
        exit
      }
      document = ""
      found_kind = 0
      found_name = 0
    }
    {
      document = document $0 "\n"
      if ($0 == "kind: " kind) found_kind = 1
      if (found_kind && $0 == "  name: " name) found_name = 1
    }
    END {
      if (!emitted && document != "" && found_kind && found_name) print document
    }
  ' <<<"$rendered"
}

require_resource() {
  local rendered="$1"
  local kind="$2"
  local name="$3"
  local result
  result="$(resource "$rendered" "$kind" "$name")"
  [ -n "$result" ] || fail "${kind}/${name} did not render"
  printf '%s' "$result"
}

require_text() {
  local text="$1"
  local pattern="$2"
  local message="$3"
  grep -Eq "$pattern" <<<"$text" || fail "$message"
}

forbid_text() {
  local text="$1"
  local pattern="$2"
  local message="$3"
  if grep -Eq "$pattern" <<<"$text"; then
    fail "$message"
  fi
}

# The two bindings carry distinct values through the existing name-keyed Secret
# contract. The values themselves are not accepted anywhere but their own Secret.
rendered="$(helm template curie "$CHART" \
  --set-string 'agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN=agent-a-sentinel' \
  --set-string 'agentSandbox.connectorSecrets.acme-b.GITHUB_PERSONAL_ACCESS_TOKEN=agent-b-sentinel' \
  2>/dev/null)"

# #2943: the worker routes a connector-secret claim only to a pool the chart
# rendered, so every per-agent pool must be named in CURIE_AGENT_SANDBOX_POOLS.
worker="$(require_resource "$rendered" Deployment curie-worker)"
require_text "$worker" 'name: CURIE_AGENT_SANDBOX_POOLS' \
  "worker lacks CURIE_AGENT_SANDBOX_POOLS"
require_text "$(grep -A1 'name: CURIE_AGENT_SANDBOX_POOLS' <<<"$worker")" 'value: "acme-a,acme-b"' \
  "CURIE_AGENT_SANDBOX_POOLS must list every connectorSecrets agent the chart renders a pool for"
require_text "$(grep -A1 'name: CURIE_AGENT_CONNECTOR_SECRET_POOLS' <<<"$worker")" 'value: "acme-a,acme-b"' \
  "CURIE_AGENT_CONNECTOR_SECRET_POOLS must list every agent whose pool carries connector secrets"

# The worker derives a per-agent pool name from its base pool. With
# worker.warmPool overridden that derivation no longer names the pools the chart
# renders under the release fullname, so the chart lists none and the worker
# refuses a secret claim at once instead of waiting on a missing pool.
override_worker="$(require_resource "$(helm template curie "$CHART" \
  --set worker.warmPool=custom-runner-pool \
  --set-string 'agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN=agent-a-sentinel' \
  2>/dev/null)" Deployment curie-worker)"
for name in CURIE_AGENT_SANDBOX_POOLS CURIE_AGENT_CONNECTOR_SECRET_POOLS; do
  require_text "$(grep -A1 "name: $name" <<<"$override_worker")" 'value: ""' \
    "$name must be empty when worker.warmPool is overridden"
done

# agentSandbox.deploy=false renders no pools at all, so none are listed.
undeployed_worker="$(require_resource "$(helm template curie "$CHART" \
  --set agentSandbox.deploy=false \
  --set-string 'agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN=agent-a-sentinel' \
  2>/dev/null)" Deployment curie-worker)"
for name in CURIE_AGENT_SANDBOX_POOLS CURIE_AGENT_CONNECTOR_SECRET_POOLS; do
  require_text "$(grep -A1 "name: $name" <<<"$undeployed_worker")" 'value: ""' \
    "$name must be empty when agentSandbox.deploy is false"
done

secret_a="$(require_resource "$rendered" Secret curie-agent-acme-a-connector-secrets)"
secret_b="$(require_resource "$rendered" Secret curie-agent-acme-b-connector-secrets)"
require_text "$secret_a" 'curietech.ai/agent: "acme-a"' \
  "acme-a Secret lacks the agent label"
require_text "$secret_b" 'curietech.ai/agent: "acme-b"' \
  "acme-b Secret lacks the agent label"
require_text "$secret_a" 'GITHUB_PERSONAL_ACCESS_TOKEN: "agent-a-sentinel"' \
  "acme-a Secret lacks its connector value"
require_text "$secret_b" 'GITHUB_PERSONAL_ACCESS_TOKEN: "agent-b-sentinel"' \
  "acme-b Secret lacks its connector value"
forbid_text "$secret_a" 'agent-b-sentinel' "acme-b value leaked into acme-a Secret"
forbid_text "$secret_b" 'agent-a-sentinel' "acme-a value leaked into acme-b Secret"

# The shared chart Secret remains separate from every connector Secret.
shared="$(require_resource "$rendered" Secret curie-secrets)"
forbid_text "$shared" 'GITHUB_PERSONAL_ACCESS_TOKEN|agent-a-sentinel|agent-b-sentinel' \
  "connector secret leaked into the shared chart Secret"

# Each existing name-keyed Secret produces an independently named template and
# pool. The template receives the same agent label and references only its own
# Secret by key. This is the delivery boundary, not worker or claim routing.
for agent in acme-a acme-b; do
  template="$(require_resource "$rendered" SandboxTemplate "curie-agent-${agent}-runner")"
  pool="$(require_resource "$rendered" SandboxWarmPool "curie-agent-${agent}-runner-pool")"
  other="acme-a"
  [ "$agent" = "acme-a" ] && other="acme-b"

  require_text "$template" "curietech.ai/agent: \\\"?${agent}\\\"?" \
    "SandboxTemplate for ${agent} lacks its pod agent label"
  require_text "$template" 'name: GITHUB_PERSONAL_ACCESS_TOKEN' \
    "SandboxTemplate for ${agent} lacks connector env delivery"
  require_text "$template" 'secretKeyRef:' \
    "SandboxTemplate for ${agent} does not use secretKeyRef"
  require_text "$template" "name: curie-agent-${agent}-connector-secrets" \
    "SandboxTemplate for ${agent} references the wrong connector Secret"
  require_text "$template" 'key: GITHUB_PERSONAL_ACCESS_TOKEN' \
    "SandboxTemplate for ${agent} references the wrong connector key"
  require_text "$template" 'optional: false' \
    "SandboxTemplate for ${agent} makes its connector Secret optional"
  forbid_text "$template" "curie-agent-${other}-connector-secrets" \
    "SandboxTemplate for ${agent} references ${other}'s connector Secret"
  forbid_text "$template" 'agent-a-sentinel|agent-b-sentinel' \
    "SandboxTemplate for ${agent} contains a connector value"

  require_text "$pool" 'sandboxTemplateRef:' \
    "SandboxWarmPool for ${agent} lacks a template reference"
  require_text "$pool" "name: curie-agent-${agent}-runner" \
    "SandboxWarmPool for ${agent} does not select its own template"
  forbid_text "$pool" "curie-agent-${other}-runner" \
    "SandboxWarmPool for ${agent} selects ${other}'s template"
done

# An empty map stays on the generic substrate path: exactly the generic
# SandboxTemplate and SandboxWarmPool render, with no connector-specific peers.
default_render="$(helm template curie "$CHART" 2>/dev/null)"
generic_template="$(require_resource "$default_render" SandboxTemplate curie-runner)"
generic_pool="$(require_resource "$default_render" SandboxWarmPool curie-runner-pool)"
# Probe fixtures live inside a Job script, so inspect actual chart documents
# rather than resource names embedded in that script's YAML string.
printf '%s' "$default_render" | python3 -c '
import re
import sys

import yaml

for document in yaml.safe_load_all(sys.stdin):
    if not document:
        continue
    name = document.get("metadata", {}).get("name", "")
    kind = document.get("kind", "resource")
    assert not re.fullmatch(r"curie-agent-.*-(connector-secrets|runner|runner-pool)", name), (
        f"connector-specific {kind}/{name} rendered with no connectorSecrets"
    )
'
[ "$generic_template" = "$(require_resource "$rendered" SandboxTemplate curie-runner)" ] \
  || fail "connectorSecrets changed the generic SandboxTemplate"
[ "$generic_pool" = "$(require_resource "$rendered" SandboxWarmPool curie-runner-pool)" ] \
  || fail "connectorSecrets changed the generic SandboxWarmPool"

# Existing reserved-key guard remains fail closed, with a paired legitimate key
# control so this assertion cannot pass because connector Secrets stopped rendering.
assert_reserved_render_fails() {
  local key="$1"
  local output
  if output="$(helm template curie "$CHART" \
    --set "agentSandbox.connectorSecrets.demo.${key}=x" 2>&1)"; then
    fail "reserved connector-secret name '${key}' rendered instead of failing"
  fi
  grep -qi 'reserved' <<<"$output" \
    || fail "reserved '${key}' render failed without naming the reservation"
}

for key in ANTHROPIC_BASE_URL ANTHROPIC_API_KEY CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_AUTH_TOKEN CURIE_BUDGET; do
  assert_reserved_render_fails "$key"
done

if ! helm template curie "$CHART" \
  --set 'agentSandbox.connectorSecrets.demo.GITHUB_PERSONAL_ACCESS_TOKEN=ghp_ok' >/dev/null 2>&1; then
  fail "legitimate connector-secret name GITHUB_PERSONAL_ACCESS_TOKEN failed to render"
fi

# @spec ACTION-EXECUTOR-16: key custody. The snapshot sealing key reaches only
# the hosted connector, as a SecretRef. Under agentSandbox.connectorSecrets it
# would land in the per-agent Secret the runner sandbox reads, so the render
# fails and names the key -- a `fail`, not the silent skip the E2E names get,
# because a skipped key would deploy a connector that cannot seal. Rendered
# output and errors go to temp files, never through process arguments.
custody_work="$(mktemp -d)"
trap 'rm -rf "$custody_work"' EXIT

for key in SNAPSHOT_SEALING_KEY SNAPSHOT_SEALING_KEYS_RETAINED; do
  if helm template curie "$CHART" \
    --set-string "agentSandbox.connectorSecrets.demo.${key}=seal-sentinel" \
    >"$custody_work/out.yaml" 2>"$custody_work/err.txt"; then
    fail "sealing key '${key}' under agentSandbox.connectorSecrets rendered instead of failing"
  fi
  # The custody refusal specifically (review L5): the path it was set under,
  # the key, and the sealing-key wording. Any other guard that happens to name
  # the key (the boot-env reserved guard, a schema error) does not satisfy it.
  grep -qF "agentSandbox.connectorSecrets.demo.${key} is a reserved snapshot sealing key" \
    "$custody_work/err.txt" \
    || fail "sealing key '${key}' render failed without the custody refusal: $(head -c 400 "$custody_work/err.txt")"
  grep -qF "SecretRef" "$custody_work/err.txt" \
    || fail "sealing key '${key}' refusal does not say to declare it as a SecretRef"
  if grep -qF "seal-sentinel" "$custody_work/err.txt"; then
    fail "sealing key '${key}' refusal echoes the submitted value"
  fi
done

# @spec ACTION-EXECUTOR-16 (review L4): agentSandbox.runner.extraEnv puts a
# variable into every runner sandbox, so the sealing key there would reach
# every agent's sandbox (and, outside the connector-secret marker, unredacted).
# The render fails, naming the key, and never echoes the value.
for key in SNAPSHOT_SEALING_KEY SNAPSHOT_SEALING_KEYS_RETAINED; do
  if helm template curie "$CHART" \
    --set-string "agentSandbox.runner.extraEnv[0].name=${key}" \
    --set-string "agentSandbox.runner.extraEnv[0].value=seal-sentinel" \
    >"$custody_work/out.yaml" 2>"$custody_work/err.txt"; then
    fail "sealing key '${key}' under agentSandbox.runner.extraEnv rendered instead of failing"
  fi
  grep -qF "$key" "$custody_work/err.txt" \
    || fail "sealing key '${key}' extraEnv render failed without naming it: $(head -c 400 "$custody_work/err.txt")"
  grep -qi "sealing key" "$custody_work/err.txt" \
    || fail "sealing key '${key}' extraEnv render failed without saying it is the sealing key: $(head -c 400 "$custody_work/err.txt")"
  if grep -qF "seal-sentinel" "$custody_work/err.txt"; then
    fail "sealing key '${key}' extraEnv refusal echoes the submitted value"
  fi
done

# Paired control: an unreserved runner extraEnv name renders into the runner env.
helm template curie "$CHART" \
  --set-string 'agentSandbox.runner.extraEnv[0].name=MY_SEAL_KEY' \
  --set-string 'agentSandbox.runner.extraEnv[0].value=seal-control' \
  >"$custody_work/extra-control.yaml" 2>"$custody_work/extra-control-err.txt" \
  || fail "unreserved runner extraEnv MY_SEAL_KEY failed to render: $(head -c 400 "$custody_work/extra-control-err.txt")"
grep -Eq '^ +- name: MY_SEAL_KEY$' "$custody_work/extra-control.yaml" \
  || fail "MY_SEAL_KEY did not render into the runner env via agentSandbox.runner.extraEnv"

# Paired control: an unreserved name of the same shape renders and lands in the
# per-agent Secret, so the refusal above cannot pass because connector Secrets
# stopped rendering. (A plain MY_SEAL_KEY fails custody at the API instead.)
helm template curie "$CHART" \
  --set-string 'agentSandbox.connectorSecrets.demo.MY_SEAL_KEY=seal-control' \
  >"$custody_work/control.yaml" 2>"$custody_work/control-err.txt" \
  || fail "unreserved connector-secret name MY_SEAL_KEY failed to render: $(head -c 400 "$custody_work/control-err.txt")"
grep -Eq '^  MY_SEAL_KEY: "seal-control"$' "$custody_work/control.yaml" \
  || fail "MY_SEAL_KEY did not render into the per-agent connector Secret"

# Per-agent egress (#1488): two agents with distinct connector CIDRs render
# policies that select only that agent's pods. Cross-agent selection is the
# leak this exists to prevent. The shared runner-allow-egress policy stays
# unlabelled so model-API CIDRs remain fleet-wide.
egress_render="$(helm template curie "$CHART" \
  --set-string 'agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN=agent-a-sentinel' \
  --set-string 'agentSandbox.connectorSecrets.acme-b.GITHUB_PERSONAL_ACCESS_TOKEN=agent-b-sentinel' \
  --set 'agentSandbox.connectorEgress.acme-a[0].cidr=10.0.0.10/32' \
  --set 'agentSandbox.connectorEgress.acme-a[0].ports[0].protocol=TCP' --set 'agentSandbox.connectorEgress.acme-a[0].ports[0].port=443' \
  --set 'agentSandbox.connectorEgress.acme-b[0].cidr=10.0.0.20/32' \
  --set 'agentSandbox.connectorEgress.acme-b[0].ports[0].protocol=TCP' --set 'agentSandbox.connectorEgress.acme-b[0].ports[0].port=443' \
  --set 'security.networkPolicy.enabled=true' \
  --set 'security.networkPolicy.allowedEgress[0].cidr=203.0.113.10/32' \
  --set 'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP' --set 'security.networkPolicy.allowedEgress[0].ports[0].port=443' \
  2>/dev/null)"

policy_a="$(require_resource "$egress_render" NetworkPolicy curie-agent-acme-a-allow-egress)"
policy_b="$(require_resource "$egress_render" NetworkPolicy curie-agent-acme-b-allow-egress)"
require_text "$policy_a" 'curietech.ai/agent: "acme-a"' \
  "acme-a egress policy does not select the acme-a agent label"
require_text "$policy_b" 'curietech.ai/agent: "acme-b"' \
  "acme-b egress policy does not select the acme-b agent label"
forbid_text "$policy_a" 'curietech.ai/agent: "acme-b"' \
  "acme-a egress policy also selects acme-b"
forbid_text "$policy_b" 'curietech.ai/agent: "acme-a"' \
  "acme-b egress policy also selects acme-a"
require_text "$policy_a" '10.0.0.10/32' "acme-a egress policy lacks its connector CIDR"
require_text "$policy_b" '10.0.0.20/32' "acme-b egress policy lacks its connector CIDR"
forbid_text "$policy_a" '10.0.0.20/32' "acme-b CIDR leaked into acme-a egress policy"
forbid_text "$policy_b" '10.0.0.10/32' "acme-a CIDR leaked into acme-b egress policy"

shared_egress="$(require_resource "$egress_render" NetworkPolicy curie-runner-allow-egress)"
forbid_text "$shared_egress" 'curietech.ai/agent' \
  "shared runner-allow-egress must not select a per-agent label"
forbid_text "$shared_egress" '10.0.0.10/32|10.0.0.20/32' \
  "per-agent connector CIDR leaked into the shared allow-egress policy"

forbid_text "$default_render" 'curie-agent-.*-allow-egress' \
  "per-agent egress policy rendered with no connectorEgress"

# #3560: Claim 2 must exercise the chart's runner delivery path, including the
# real runner security context, rather than manufacture a secret-reading Role.
# Parse embedded fixture YAML as YAML and compare security fields with the
# chart's ordinary template. Negative controls mutate the relevant boundaries.
assert_probe_delivery() {
  local release="$1"
  local prefix="$2"
  shift 2
  local probe_render substrate_render
  probe_render="$(helm template "$release" "$CHART" --namespace acme-probe \
    --show-only templates/security-probe.yaml "$@" 2>/dev/null)"
  substrate_render="$(helm template "$release" "$CHART" --namespace acme-probe \
    --show-only templates/agent-sandbox.yaml "$@" 2>/dev/null)"
  PROBE_RENDER="$probe_render" SUBSTRATE_RENDER="$substrate_render" \
    PROBE_PREFIX="$prefix" PROBE_CHART="$CHART" python3 - <<'PY'
import copy
import os
import re
import textwrap
from pathlib import Path

import yaml


def resource(documents, kind, name):
    matches = [
        document for document in documents
        if document and document.get("kind") == kind
        and document.get("metadata", {}).get("name") == name
    ]
    assert len(matches) == 1, f"expected one {kind}/{name}, found {len(matches)}"
    return matches[0]


prefix = os.environ["PROBE_PREFIX"]
documents = list(yaml.safe_load_all(os.environ["PROBE_RENDER"]))
job = resource(documents, "Job", f"{prefix}-security-probe")
containers = job["spec"]["template"]["spec"]["containers"]
probe = next(container for container in containers if container["name"] == "probe")
script = "\n".join(probe["command"] + probe.get("args", []))
embedded = re.search(r"<<['\"]?PROBE_SANDBOXES['\"]?[^\n]*\n(.*?)^\s*PROBE_SANDBOXES\s*$", script, re.M | re.S)
assert embedded is not None, "probe must apply shared runner fixtures from PROBE_SANDBOXES"
fixtures = list(yaml.safe_load_all(textwrap.dedent(embedded.group(1))))
role = resource(documents, "Role", f"{prefix}-security-probe")
substrate = list(yaml.safe_load_all(os.environ["SUBSTRATE_RENDER"]))
ordinary = resource(substrate, "SandboxTemplate", f"{prefix}-runner")
release_name = ordinary["spec"]["podTemplate"]["metadata"]["labels"]["app.kubernetes.io/instance"]
temporary_policies = []
for heredoc in re.finditer(r"<<EOF[^\n]*\n(.*?)^\s*EOF\s*$", script, re.M | re.S):
    for document in yaml.safe_load_all(textwrap.dedent(heredoc.group(1))):
        if document and document.get("kind") == "NetworkPolicy":
            labels = document["spec"].get("podSelector", {}).get("matchLabels", {})
            if "curie.io/egress-probe" not in labels:
                temporary_policies.append(document)
assert len(temporary_policies) == 1, "probe must create one temporary API policy for its fixture pods"
temporary_policy = temporary_policies[0]
source = (Path(os.environ["PROBE_CHART"]) / "templates/security-probe.yaml").read_text()
assert 'include "curie.sandboxTemplate"' in source, "probe fixtures must use the shared sandbox helper"


def runner(template):
    return next(
        container for container in template["spec"]["podTemplate"]["spec"]["containers"]
        if container["name"] == "runner"
    )


def check_delivery(candidates):
    assert sum(bool(doc) and doc.get("kind") == "SandboxTemplate" for doc in candidates) == 2, "probe needs two runner templates"
    assert sum(bool(doc) and doc.get("kind") == "SandboxWarmPool" for doc in candidates) == 2, "probe needs two runner pools"
    baseline = ordinary["spec"]["podTemplate"]["spec"]
    for agent in ("sp-a", "sp-b"):
        template_name = f"{prefix}-agent-{agent}-runner"
        template = resource(candidates, "SandboxTemplate", template_name)
        pool = resource(candidates, "SandboxWarmPool", f"{template_name}-pool")
        assert pool["spec"]["sandboxTemplateRef"]["name"] == template_name, "fixture pool must select its own template"
        assert pool["spec"]["replicas"] == 0, "probe fixtures must create runners through claims"
        pod = template["spec"]["podTemplate"]
        labels = pod["metadata"]["labels"]
        assert labels["curietech.ai/agent"] == agent, "fixture runner must carry its own agent label"
        assert labels.get("curie.io/security-probe") == prefix, "fixture runner must carry its probe label at creation"
        for key, value in ordinary["spec"]["podTemplate"]["metadata"]["labels"].items():
            assert labels.get(key) == value, "fixture runner must retain chart runner labels"
        for field in ("serviceAccountName", "automountServiceAccountToken", "securityContext", "runtimeClassName"):
            assert pod["spec"].get(field) == baseline.get(field), f"fixture runner changed chart {field}"
        assert runner(template).get("securityContext") == runner(ordinary).get("securityContext"), "fixture runner changed chart container securityContext"
        assert template["spec"].get("networkPolicyManagement") == ordinary["spec"].get("networkPolicyManagement"), "fixture runner changed chart NetworkPolicy management"
        assert not any("projected" in volume or "secret" in volume for volume in pod["spec"].get("volumes", [])), "fixture runner must not add a credential or token volume"
        env = runner(template)["env"]
        keys = {"CURIE_SECURITY_PROBE_SECRET"}
        if agent == "sp-b":
            keys.add("CURIE_SECURITY_PROBE_B_ONLY")
        entries = [entry for entry in env if entry["name"].startswith("CURIE_SECURITY_PROBE_")]
        assert {entry["name"] for entry in entries} == keys and len(entries) == len(keys), "fixture runner must receive only its own sentinel keys"
        for entry in entries:
            assert entry == {
                "name": entry["name"],
                "valueFrom": {"secretKeyRef": {
                    "name": f"{prefix}-agent-{agent}-connector-secrets",
                    "key": entry["name"], "optional": False,
                }},
            }, "fixture runner sentinel must reference its own Secret"


def check_privileges(rules):
    fixture_secrets = {
        f"{prefix}-agent-sp-a-connector-secrets",
        f"{prefix}-agent-sp-b-connector-secrets",
    }
    secret_verbs = set()
    for rule in rules:
        resources = set(rule.get("resources", []))
        verbs = set(rule.get("verbs", []))
        assert "serviceaccounts/token" not in resources and "*" not in resources, "probe must not mint ServiceAccount tokens"
        if resources & {"roles", "rolebindings", "clusterroles", "clusterrolebindings"}:
            assert not verbs & {"create", "delete", "update", "patch", "bind", "escalate", "*"}, "probe must not manufacture credential RBAC"
        if "pods" in resources:
            assert not verbs & {"patch", "*"}, "probe must not patch pods"
        if "secrets" in resources:
            secret_verbs.update(verbs)
            if verbs - {"create"}:
                assert set(rule.get("resourceNames", [])) == fixture_secrets, "Secret reads and deletes must be scoped to fixture names"
                assert verbs <= {"get", "delete"}, "fixture Secret grant must only read and delete"
            if "create" in verbs:
                assert verbs == {"create"} and not rule.get("resourceNames"), "Secret creation must use a separate unconstrained create grant"
    assert {"create", "get", "delete"} <= secret_verbs, "probe must create, inspect and clean up fixture Secrets"


def check_temporary_policy(candidate):
    assert candidate["spec"].get("podSelector") == {
        "matchLabels": {
            "app.kubernetes.io/instance": release_name,
            "curie.io/security-probe": "${prefix}",
        },
    }, "temporary API policy must select only this release's fixture pods"
    assert candidate["spec"].get("policyTypes") == ["Egress"], "temporary API policy must only allow egress"


def check_script(candidate):
    commands = re.sub(r"\\\n\s*", " ", candidate)
    assert not re.search(r"kubectl[^\n]*\b(?:create|delete)\s+(?:rolebindings?|roles?|serviceaccounts?|token)\b", commands), "probe must not create handwritten credential identities"
    assert "kind: SandboxClaim" in candidate and "warmPoolRef:" in candidate, "probe must claim runner pools"
    assert re.search(r"kubectl\s+wait[^\n]*condition=Ready[^\n]*sandboxclaim", commands, re.I), "probe must wait for real claim readiness"
    assert re.search(r"kubectl\s+exec[^\n]*(?:-c\s+runner|--container(?:=|\s+)runner)", commands), "probe must inspect the claimed runner container"
    assert "CURIE_SECURITY_PROBE_SECRET=AAA" in commands and "CURIE_SECURITY_PROBE_SECRET=BBB" in commands, "probe must create distinct values for the same sentinel key"
    assert "CURIE_SECURITY_PROBE_B_ONLY=BBB" in commands, "probe must create the B only sentinel"
    assert "CURIE_SECURITY_PROBE_B_ONLY" in candidate and "CURIE_SECURITY_PROBE_SECRET" in candidate, "runner assertion must inspect both sentinel keys"
    cleanup = re.search(r"cleanup\(\)\s*\{(.*?)^\s*\}", candidate, re.M | re.S)
    assert cleanup is not None and "trap cleanup EXIT" in candidate, "probe must install cleanup before claims"
    body = re.sub(r"\\\n\s*", " ", cleanup.group(1))
    for kind in ("sandboxclaim", "sandboxtemplate", "sandboxwarmpool", "secret", "networkpolicy"):
        assert re.search(rf"kubectl\s+delete[^\n]*\b{kind}s?\b", body, re.I), f"probe cleanup must delete fixture {kind}"


def must_reject(label, checker, value, message):
    try:
        checker(value)
    except AssertionError as error:
        assert message in str(error), f"{label} failed at the wrong boundary: {error}"
    else:
        raise AssertionError(f"negative control accepted {label}")


check_delivery(fixtures)
check_privileges(role["rules"])
check_temporary_policy(temporary_policy)
check_script(script)

crossed = copy.deepcopy(fixtures)
template_a = resource(crossed, "SandboxTemplate", f"{prefix}-agent-sp-a-runner")
entry = next(entry for entry in runner(template_a)["env"] if entry["name"] == "CURIE_SECURITY_PROBE_SECRET")
entry["valueFrom"]["secretKeyRef"]["name"] = f"{prefix}-agent-sp-b-connector-secrets"
must_reject("crossed Secret reference", check_delivery, crossed, "reference its own Secret")

leaked = copy.deepcopy(fixtures)
template_a = resource(leaked, "SandboxTemplate", f"{prefix}-agent-sp-a-runner")
template_b = resource(leaked, "SandboxTemplate", f"{prefix}-agent-sp-b-runner")
runner(template_a)["env"].append(copy.deepcopy(next(entry for entry in runner(template_b)["env"] if entry["name"] == "CURIE_SECURITY_PROBE_B_ONLY")))
must_reject("B only key leaked into A", check_delivery, leaked, "only its own sentinel keys")

privileged = copy.deepcopy(role["rules"])
privileged.append({"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["roles", "rolebindings"], "verbs": ["create", "delete"]})
must_reject("handwritten Role grant", check_privileges, privileged, "manufacture credential RBAC")
must_reject("token mint grant", check_privileges, role["rules"] + [{"resources": ["serviceaccounts/token"], "verbs": ["create"]}], "must not mint ServiceAccount tokens")
must_reject("unscoped Secret read", check_privileges, role["rules"] + [{"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}], "scoped to fixture names")
must_reject("pod patch grant", check_privileges, role["rules"] + [{"apiGroups": [""], "resources": ["pods"], "verbs": ["patch"]}], "must not patch pods")
shared_policy = copy.deepcopy(temporary_policy)
shared_policy["spec"]["podSelector"]["matchLabels"].pop("curie.io/security-probe")
must_reject("policy missing unique probe selector", check_temporary_policy, shared_policy, "only this release's fixture pods")
must_reject("Pod replacing claim", check_script, script.replace("kind: SandboxClaim", "kind: Pod"), "claim runner pools")
print(f"  ok: {prefix} probe uses chart runner delivery and rejects isolation regressions")
PY
}

assert_probe_delivery curie curie
assert_probe_delivery acme acme-security \
  --set fullnameOverride=acme-security \
  --set security.gvisor.mode=require \
  --set security.gvisor.runtimeClassName=acme-runsc \
  --set agentSandbox.runner.hardening.runAsUser=1200 \
  --set agentSandbox.runner.hardening.runAsGroup=1200 \
  --set agentSandbox.runner.hardening.fsGroup=1200 \
  --set agentSandbox.warmPool.replicas=2
assert_probe_delivery curie curie \
  --set agentSandbox.runner.fakeModel=false \
  --set security.gvisor.mode=auto \
  --set security.gvisor.runtimeClassName=acme-runsc

echo "OK: per-agent connector-secret render assertions passed"

# ADR 0176: the test cluster kubeconfig is a connector secret the sandbox must
# not receive. A normal connector token still renders beside it.
withheld="$(helm template curie "$CHART" \
  --set-string 'agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN=agent-a-sentinel' \
  --set-string 'agentSandbox.connectorSecrets.acme-a.E2E_CLUSTER_KUBECONFIG=kubeconfig-sentinel' \
  --set-string 'agentSandbox.connectorSecrets.acme-a.E2E_REGISTRY_PUSH_CONFIG=registry-sentinel' \
  --set-string 'agentSandbox.connectorSecrets.acme-a.E2E_BUILD_CACHE_CONFIG=cache-sentinel' \
  2>/dev/null)"
withheld_secret="$(require_resource "$withheld" Secret curie-agent-acme-a-connector-secrets)"
withheld_template="$(require_resource "$withheld" SandboxTemplate curie-agent-acme-a-runner)"
require_text "$withheld_secret" 'GITHUB_PERSONAL_ACCESS_TOKEN: "agent-a-sentinel"' \
  "withheld render dropped the ordinary connector secret"
forbid_text "$withheld_secret" \
  'E2E_CLUSTER_KUBECONFIG|E2E_REGISTRY_PUSH_CONFIG|E2E_BUILD_CACHE_CONFIG|kubeconfig-sentinel|registry-sentinel|cache-sentinel' \
  "sandbox Secret stored a test cluster or registry credential"
forbid_text "$withheld_template" \
  'E2E_CLUSTER_KUBECONFIG|E2E_REGISTRY_PUSH_CONFIG|E2E_BUILD_CACHE_CONFIG|kubeconfig-sentinel|registry-sentinel|cache-sentinel' \
  "sandbox template referenced a test cluster or registry credential"
require_text "$withheld_template" 'name: GITHUB_PERSONAL_ACCESS_TOKEN' \
  "sandbox template dropped the ordinary connector secret"

stock_api="$(require_resource "$(helm template curie "$CHART" 2>/dev/null)" Deployment curie-api)"
require_text "$stock_api" 'name: CURIE_E2E_CONNECTOR_ENABLED' \
  "API lacks CURIE_E2E_CONNECTOR_ENABLED"
require_text "$(grep -A1 'name: CURIE_E2E_CONNECTOR_ENABLED' <<<"$stock_api")" 'value: "false"' \
  "e2e connector must be disabled on a stock install"

if helm template curie "$CHART" --set e2eConnector.enabled=true >/dev/null 2>&1; then
  fail "e2eConnector.enabled without the test cluster identity rendered"
fi

e2e_identity=(
  --set e2eConnector.enabled=true
  --set e2eConnector.ownerLabel.value=acme
  --set e2eConnector.serviceAccount=curie-e2e-connector
  --set e2eConnector.serviceAccountNamespace=test-system
  --set e2eConnector.workerClusterRole=curie-e2e-connector-namespace
)
enabled_render="$(helm template curie "$CHART" "${e2e_identity[@]}" \
  --set worker.connectorReconciler.enabled=true 2>/dev/null)"
enabled_api="$(require_resource "$enabled_render" Deployment curie-api)"
require_text "$(grep -A1 'name: CURIE_E2E_CONNECTOR_ENABLED' <<<"$enabled_api")" 'value: "true"' \
  "enabled install did not tell the API the test cluster is configured"
require_text "$enabled_api" 'value: "acme"' \
  "enabled install did not pass the owner label value"

# #3245: the worker's end to end namespace reaper gets the same scope as the
# API, only when e2eConnector is enabled.
stock_worker="$(require_resource "$(helm template curie "$CHART" 2>/dev/null)" Deployment curie-worker)"
forbid_text "$stock_worker" 'name: CURIE_E2E_' \
  "a stock install configured the e2e reaper on the worker"
enabled_worker="$(require_resource "$enabled_render" Deployment curie-worker)"
for name in CURIE_E2E_NAMESPACE_PREFIX CURIE_E2E_OWNER_LABEL_KEY CURIE_E2E_REAPER_INTERVAL_S; do
  require_text "$enabled_worker" "name: ${name}\$" "enabled install did not give the worker ${name}"
done
require_text "$(grep -A1 'name: CURIE_E2E_CONNECTOR_ENABLED' <<<"$enabled_worker")" 'value: "true"' \
  "enabled install did not turn the worker's e2e reaper on"
require_text "$(grep -A1 'name: CURIE_E2E_OWNER_LABEL_VALUE' <<<"$enabled_worker")" 'value: "acme"' \
  "the worker's reaper owner label value differs from the API's"
require_text "$(grep -A1 'name: CURIE_E2E_REAPER_INTERVAL_S' <<<"$enabled_worker")" 'value: "60"' \
  "the worker's reaper interval did not default to 60 seconds"

# The reaper reads the kubeconfig through the reconciler's Secret list grant,
# so enabling e2eConnector without the reconciler is refused by name.
if no_reconciler="$(helm template curie "$CHART" "${e2e_identity[@]}" 2>&1)"; then
  fail "e2eConnector.enabled without worker.connectorReconciler.enabled rendered"
fi
require_text "$no_reconciler" 'e2eConnector.enabled requires worker.connectorReconciler.enabled' \
  "the missing reconciler refusal did not name worker.connectorReconciler.enabled"

if helm template curie "$CHART" "${e2e_identity[@]}" --set worker.connectorReconciler.enabled=true \
  --set e2eConnector.reaperIntervalSeconds=5 >/dev/null 2>&1; then
  fail "a reaper interval below 10 seconds passed the schema"
fi

# #3246: the image build values reach the API (the connector render) and the
# worker (registry retention), and the push and cache configs stay off the sandbox.
build_render="$(helm template curie "$CHART" "${e2e_identity[@]}" \
  --set worker.connectorReconciler.enabled=true \
  --set e2eConnector.registry=registry.test:5000/e2e \
  --set 'e2eConnector.registryTokenHosts={auth.test,auth2.test:8443}' \
  --set e2eConnector.registryInsecure=true 2>/dev/null)"
build_api="$(require_resource "$build_render" Deployment curie-api)"
build_worker="$(require_resource "$build_render" Deployment curie-worker)"
for name in CURIE_E2E_REGISTRY CURIE_E2E_BUILD_CACHE_REPO CURIE_E2E_REGISTRY_INSECURE \
  CURIE_E2E_REGISTRY_TOKEN_HOSTS CURIE_E2E_BUILDER_IMAGE CURIE_E2E_GIT_IMAGE \
  CURIE_E2E_PUSH_IMAGE CURIE_E2E_BUILD_TIMEOUT_SECONDS CURIE_E2E_SOURCE_HOSTS; do
  require_text "$build_api" "name: ${name}\$" "enabled install did not give the API ${name}"
done
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY$' <<<"$build_api")" \
  'value: "registry.test:5000/e2e"' "the API did not receive the registry prefix"
# An unset cache repository defaults to <registry>/cache.
require_text "$(grep -A1 'name: CURIE_E2E_BUILD_CACHE_REPO' <<<"$build_api")" \
  'value: "registry.test:5000/e2e/cache"' "the cache repository did not default to <registry>/cache"
require_text "$(grep -A1 'name: CURIE_E2E_BUILD_TIMEOUT_SECONDS' <<<"$build_api")" \
  'value: "1200"' "the build timeout did not default to 1200 seconds"
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY_TOKEN_HOSTS' <<<"$build_api")" \
  'value: "auth.test,auth2.test:8443"' "the API token hosts were not joined with commas"
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY_INSECURE' <<<"$build_api")" \
  'value: "true"' "the API did not receive registryInsecure"
require_text "$(grep -A1 'name: CURIE_E2E_SOURCE_HOSTS' <<<"$build_api")" \
  'value: "github.com"' "the source hosts did not default to github.com"
for name in CURIE_E2E_REGISTRY CURIE_E2E_REGISTRY_INSECURE CURIE_E2E_REGISTRY_TOKEN_HOSTS; do
  require_text "$build_worker" "name: ${name}\$" "enabled install did not give the worker ${name}"
done
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY$' <<<"$build_worker")" \
  'value: "registry.test:5000/e2e"' "the worker did not receive the registry prefix"
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY_TOKEN_HOSTS' <<<"$build_worker")" \
  'value: "auth.test,auth2.test:8443"' "the worker token hosts were not joined with commas"
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY_INSECURE' <<<"$build_worker")" \
  'value: "true"' "the worker did not receive registryInsecure"
forbid_text "$build_worker" 'E2E_REGISTRY_PUSH_CONFIG|E2E_BUILD_CACHE_CONFIG' \
  "the worker env named a registry credential"

# An installation with no registry still renders, with the registry empty.
unset_render="$(helm template curie "$CHART" "${e2e_identity[@]}" \
  --set worker.connectorReconciler.enabled=true 2>/dev/null)"
unset_api="$(require_resource "$unset_render" Deployment curie-api)"
require_text "$(grep -A1 'name: CURIE_E2E_REGISTRY$' <<<"$unset_api")" 'value: ""' \
  "an unset registry did not render empty"

# An explicit empty cache repository disables the cache.
nocache_render="$(helm template curie "$CHART" "${e2e_identity[@]}" \
  --set worker.connectorReconciler.enabled=true \
  --set e2eConnector.registry=registry.test:5000/e2e \
  --set-string e2eConnector.buildCacheRepo= 2>/dev/null)"
nocache_api="$(require_resource "$nocache_render" Deployment curie-api)"
require_text "$(grep -A1 'name: CURIE_E2E_BUILD_CACHE_REPO' <<<"$nocache_api")" 'value: ""' \
  "an explicit empty buildCacheRepo did not disable the cache"

if helm template curie "$CHART" "${e2e_identity[@]}" --set worker.connectorReconciler.enabled=true \
  --set e2eConnector.buildTimeoutSeconds=30 >/dev/null 2>&1; then
  fail "a build timeout below 60 seconds passed the schema"
fi

echo "OK: end to end connector render assertions passed"

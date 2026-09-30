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
forbid_text "$default_render" 'curie-agent-.*-(connector-secrets|runner|runner-pool)' \
  "connector-specific resource rendered with no connectorSecrets"
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

echo "OK: per-agent connector-secret render assertions passed"

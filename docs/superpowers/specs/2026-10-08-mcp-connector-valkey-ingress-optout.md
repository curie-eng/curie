# Optional MCP connector access to in-chart Valkey

Issue: [#4318](https://github.com/curie-eng/curie/issues/4318)

## Problem

The data-tier allow-ingress policy admits both this release's app pods and
MCP-connector pods to Valkey. Some installs do not need connector proxies to
reach the in-chart Valkey service and need a chart-level way to remove that
single peer without weakening the rest of the data-tier boundary.

## Contract

- Add `security.dataTierNetworkPolicy.allowMcpConnectorValkeyIngress`, a boolean
  that defaults to `true` and preserves the current rendered policy.
- When `false`, omit only the MCP-connector source peer and its Valkey TCP 6379
  rule from the Valkey allow-ingress policy. Keep the Valkey default-deny policy,
  the release app peer and its Valkey port, every other store policy, and all
  other NetworkPolicies unchanged.
- A chart values set that omits the new key, including one retained from a
  release created before the key existed and upgraded with `--reuse-values`,
  retains the current peer through the chart default.
- Explicit non-boolean values fail chart validation. Do not silently coerce a
  string, number, or other type into the opt-out.
- `security.dataTierNetworkPolicy.enabled: false` keeps its existing behavior
  of disabling the entire data-tier rail.
- Keep the caller-proxy preflight and all Helm-owned NetworkPolicy verification
  enabled. The opt-out changes only whether the MCP connector peer is rendered;
  it does not prove or change caller authorization.

## Acceptance criteria

1. Omitted and explicit `true` values render the existing MCP-connector peer on
   Valkey TCP 6379.
2. Explicit `false` removes only that peer and its port rule. It preserves the
   release app peer, Valkey default-deny policy, other data-tier policies, and
   all other rendered NetworkPolicies.
3. Explicit invalid types fail rendering, while an old values set without the
   new key retains current behavior.
4. The existing tenant network-boundary post-render verification passes against
   the opt-out render. Keep its output and tenant details out of public artifacts.
5. No caller-proxy preflight or Helm-owned NetworkPolicy verification is
   removed or weakened.

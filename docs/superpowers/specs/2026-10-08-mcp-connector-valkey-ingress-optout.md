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
- A values set that omits the new key retains the current peer. This includes
  a legacy release upgraded with `--reuse-values`: the template defaults an
  absent value to `true`, since Helm uses the old chart's values rather than
  the new chart defaults for that upgrade mode.
- Explicit strings, numbers, arrays, and objects fail chart validation. Helm
  coalesces an explicit YAML `null` to the same absent value before schema
  validation, so `null` follows the absent/default-on behavior; this chart does
  not promise to reject it. Operators must use literal `false` to opt out.
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
3. Explicit string, number, array, and object types fail rendering. A legacy
   values shape with no new key renders the existing peer under a simulated
   `--reuse-values` upgrade; explicit `null` follows the same default-on
   behavior because Helm coalesces it to absence.
4. The existing tenant network-boundary post-render verification passes against
   the opt-out render. Keep its output and tenant details out of public artifacts.
5. No caller-proxy preflight or Helm-owned NetworkPolicy verification is
   removed or weakened.

## Recorded implementation ruling

The chart schema cannot distinguish an omitted value from YAML `null` after
Helm coalescing. Requiring the key rejects legacy `--reuse-values` upgrades;
making it optional means `null` becomes absence. Preserve upgrade compatibility:
absence and `null` both default to the existing allow rule, and opt-out requires
literal boolean `false`. This was confirmed with Helm 3.18.2 by replacing the
candidate chart's `values.yaml` with the imported `curie-public/main` chart
values, and by rendering candidate-chart overlays with the nested property
omitted and set to `null`.

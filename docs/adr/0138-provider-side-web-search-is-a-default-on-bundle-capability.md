# 138. Provider-side web search is a default-on bundle capability

Date: 2026-09-01

Status: Proposed

This proposal records the maintainer direction in #2177. It does not accept
itself or authorize a merge; acceptance remains a separate maintainer action.

## Context

Bots need current public-web facts for the end-to-end demo, but a general
sandbox egress allowance would weaken Curie's default-deny boundary. DNS and
HTTP from model-controlled processes would need an open-ended destination
policy, expose another credential and request path to govern, and reproduce a
capability the model provider already operates.

Anthropic's [tool reference](https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-reference)
distinguishes server tools from client tools. Its
[`web_search` server tool](https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-search-tool)
executes on Anthropic infrastructure and returns server-tool use and result
blocks in the model response. In the Claude Agent SDK, the corresponding Claude
Code built-in is named `WebSearch`. The sandbox therefore needs only the same
provider connection the model turn already uses; it does not connect to search
engines or result hosts itself.

Curie's Claude harness already selects the complete Claude Code tool preset.
That establishes the tool catalogue, but there is no bundle-level declaration
for a bot which must not search. Skill `allowed-tools` frontmatter is the wrong
boundary: it applies when a skill is active, means permission preauthorization
rather than catalogue membership, and can shadow Curie's approval callback.

The Claude plugin manifest is a frozen compatibility contract. Adding a
Curie-only field to `.claude-plugin/plugin.json` would turn this feature into a
plugin-format change and require a separately reviewed contract change before
the runner could consume it. `curie.yaml` is also unavailable: ADR 0097 assigns
that file to installation intent, not an immutable agent bundle.

GitHub repository reads are a different capability. They require the
first-party credential and egress boundary in ADR 0075 and are not web-search
traffic.

## Decision

**The Claude harness offers Anthropic's provider-side `WebSearch` tool by
default. An immutable bundle may opt out through a Curie-owned bundle config;
the opt-out removes the tool from the model catalogue and never changes sandbox
egress.**

### The bundle config is `curie.bundle.json`

An optional `curie.bundle.json` at the bundle root carries Curie runtime
configuration that is not part of the frozen Claude plugin format. Its first
shape is:

```json
{
  "webSearch": false
}
```

An absent file, or an explicit boolean `true`, means web search is enabled. An
explicit boolean `false` is the per-bundle opt-out. The file is part of the
immutable bundle snapshot and archive, so one deployed bundle version cannot
change the capability of an already-running version.

The document is a strict Curie-owned object. Invalid JSON, a non-object root, an
unknown key, or a non-boolean `webSearch` value fails runner boot with the file
and defect named. A misspelled opt-out must not silently widen capability.

The runner is the authoritative loader. This sidecar deliberately does not add
a field to `packages/plugin-format`, `packages/aci-protocol`, the worker boot
environment, deployment state, or installation configuration.

### Default-on and opt-out use the SDK's availability controls

The default continues to pass the Claude Code tool preset through
`ClaudeAgentOptions.tools`, under which `WebSearch` is offered. Curie does not
add `WebSearch` to `allowed_tools`: that option preauthorizes execution and can
bypass permission callbacks rather than controlling availability.

When `webSearch` is `false`, the runner passes `WebSearch` through
`ClaudeAgentOptions.disallowed_tools`. The initialized model tool catalogue must
then omit `WebSearch`; suppressing a prompt mention or merely denying a later
call is insufficient. The bundle's ordinary approval policy and hooks remain
unchanged for every other tool.

Provider organization policy remains authoritative. If Anthropic has disabled
web search for the organization, a bundle cannot override it and the provider
error remains visible. Curie does not fall back to a sandbox HTTP client, an MCP
search service, or unverified model recall.

### The provider connection is the only network path

Search requests and results ride the model session's existing provider
connection. This decision authorizes no NetworkPolicy edits, DNS or CIDR
allowances, generic web egress, new proxy route, credential forwarding, or
credential storage. It also does not enable `WebFetch`; fetching arbitrary
result URLs is a separate capability and egress decision.

GitHub reads remain entirely out of scope. They ride the ADR 0075 Agent Proxy
when that implementation lands, never a web-search exception or a repository
CIDR carve-out.

### Verification proves availability at the real consumer boundary

The realization must prove both halves against the pinned Claude Agent SDK:

- with no `curie.bundle.json`, the initialized Claude tool catalogue contains
  `WebSearch` and a live Anthropic turn can invoke provider-side search; and
- with `{"webSearch": false}`, the initialized catalogue omits `WebSearch`.

The negative uses the same bundle and runner path with only the opt-out changed.
Unit tests also pin strict config parsing and the exact SDK option mapping.

This change reaches the runner turn loop, so the `skill` tier is required. It
changes provider-tool availability, so a live-provider observation is also
required before completion. The `local`, `local-release`, and `cluster` tiers
are not required because the change does not alter compose wiring, released
artifact identity, chart templates, sandbox claims, or service boot env. The
external-integration tier is not separate from the live-provider observation:
the only external system exercised is the model provider itself.

## Consequences

Every Claude-backed bot can search current public-web information without
authors copying tool permissions into skills. Bundles with policy, cost, or
determinism reasons to avoid search have a versioned opt-out whose effect is
visible in the initialized tool catalogue.

The security posture does not gain a new sandbox destination. Search content is
still untrusted model input, and provider-side execution does not make a result
authoritative; skills and evals remain responsible for source quality and
citation requirements.

Provider web search may incur separate usage charges and may be unavailable for
a model, gateway, region, or organization policy. Those failures stay explicit.
Curie does not silently substitute a client-side implementation whose egress or
provenance differs.

`curie.bundle.json` creates a small Curie-owned bundle surface alongside, not
inside, the Claude plugin format. Future keys require their own decision and
runtime validation; older runners will not understand such keys and must not be
claimed as enforcing them.

The implementation prepared alongside this proposal is review evidence only.
Its presence does not accept this ADR, make either pull request ready to merge,
or authorize merging implementation before the maintainer decides the ADR.

## Alternatives considered

### Add public-web CIDRs or hostnames to sandbox NetworkPolicy

Rejected. Search destinations and result hosts are open-ended, DNS changes over
time, and the sandbox does not need to contact them when the provider executes
the tool.

### Run a search client or MCP server inside the sandbox

Rejected. It adds sandbox egress, another runtime and credential boundary, and a
second result path without improving the demo capability.

### Put `WebSearch` in every skill's `allowed-tools`

Rejected. Skill activation is not bundle capability configuration, and
`allowed-tools` preauthorizes calls rather than deciding which tools the model
can see. It can also shadow Curie's permission gates.

### Add `webSearch` to `.claude-plugin/plugin.json`

Rejected for this realization. The manifest is part of the frozen
plugin-format interface; changing it would require a separately landed contract
PR before dependent runner work, which is unnecessary for a Curie-only runtime
choice.

### Put the opt-out in `curie.yaml` or deployment metadata

Rejected. `curie.yaml` describes an installation and deployment metadata is
mutable operator state. Search policy belongs to the immutable bundle version
whose behavior it changes.

### Default off and require each bundle to opt in

Rejected. Current public-web facts are a baseline bot capability for the demo,
and provider-side execution preserves the existing sandbox boundary. A narrow
opt-out expresses the exceptional bundle instead of making every ordinary
bundle repeat configuration.

### Treat GitHub repository reads as web search

Rejected. Repository reads cross a first-party authorization and credential
boundary. ADR 0075 owns that path; search must not become a back door around it.

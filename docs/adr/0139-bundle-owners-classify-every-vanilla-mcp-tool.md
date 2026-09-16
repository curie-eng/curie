# 139. Bundle owners classify every vanilla MCP tool

Date: 2026-09-01

Status: Draft

This Draft records the requested architecture decision backfill. It does not
accept itself or authorize implementation. The frozen declaration and
validation slice merged in [PR #2058](https://github.com/curie-eng/curie/pull/2058).
The realizing runtime work is now present on `next`.

## Context

A vanilla MCP server publishes a tool catalogue that can change independently
of the bundle that mounts it. A newly published tool must not silently inherit
authority merely because the bundle author did not know its name when the
bundle was reviewed. The bundle owner needs a closed policy over the MCP
capability surface that travels with the immutable bundle version.

Curie's existing `approvalPolicy` answers a different question. It declares
which live tool names pause for the durable approval lifecycle. It does not
classify the whole MCP surface, and omission means that the tool is not gated.
Operator gates and bundle hooks are also additive controls, not a complete
bundle owned catalogue policy.

The MCP name exposed by the SDK depends on how the server is mounted. A server
from `connectors.yaml` is exposed as `mcp__<server>__<tool>`, while a server in
the plugin MCP map is exposed as
`mcp__plugin_<bundle>_<server>__<tool>`. Those runtime names are packaging
details. Policy authors need one stable name that means the same thing for both
mount paths.

Approval enforcement also has two runtime interception points. The SDK's
`can_use_tool` callback is skipped when another permission rule has already
allowed a call. A `PreToolUse` hook observes every real SDK tool call despite
those permission rules, but the callback remains the permission backstop and is
the path used by the fake tier. A policy applied at only one point therefore
has a known bypass or a parity gap.

## Decision

### The bundle owner classifies canonical MCP names

The optional manifest field `toolPolicy` is the bundle owner's policy over
canonical `"<server>/<tool>"` names. It carries the exact enforcement contract
identifier `curie/mcp-tool-policy@1` and three glob collections:

```json
{
  "toolPolicy": {
    "enforcement": "curie/mcp-tool-policy@1",
    "allow": ["grafana/query_*"],
    "approvalRequired": ["kubernetes/resources_*"],
    "deny": ["kubernetes/resources_delete"]
  }
}
```

Classification precedence is by class, never by pattern specificity:
`deny` takes precedence over `approvalRequired`, which takes precedence over
`allow`. An MCP tool matched by no collection is denied. This default makes a
server's newly published tool fail closed until the bundle owner classifies it.

The policy applies only to MCP tools. Harness built-ins such as `Bash` and
`Read` remain outside this declaration and continue through their existing
permission and policy controls. An MCP shaped runtime name that cannot be
mapped to a server declared by the bundle is treated as denied, not as a
built-in and not as ungoverned.

`allow` means policy fallthrough. It is not an affirmative permission grant and
cannot weaken a stricter operator gate, a bundle `approvalPolicy` gate, a
bundle `PreToolUse` hook, or a platform owned gate. This preserves the existing
rule that bundle configuration may add restrictions but may not hollow out
operator or platform controls.

### Both execution interception points make the same decision

Runtime enforcement must map each live MCP tool name back to its canonical
`<server>/<tool>` name and run one shared classifier from both `PreToolUse` and
`can_use_tool` before execution.

`PreToolUse` is required because another SDK permission rule can preauthorize a
tool and prevent `can_use_tool` from running. `can_use_tool` is also required as
the SDK permission backstop and as the fake tier's enforcement path. Both must
use one decision helper so their meanings cannot drift.

The outcomes are:

1. `deny`, including the unmatched default, is a hard refusal. It stops the
   call before execution, creates no approval request, and cannot consume or
   mint an approval grant. No human action can override this outcome.
2. `approvalRequired` blocks before execution and enters the existing durable
   approval lifecycle. A genuine approval resumes through the existing
   one-shot grant for that exact live tool name, after which the gate is armed
   again.
3. `allow` falls through to every other applicable control. It does not return
   a permission that could shadow a stricter control.

The one-shot approval grant remains tool name scoped rather than argument
scoped, as established by [ADR-0035](0035-one-shot-post-approval-allowance.md).
On the resume turn, the approved tool can therefore be called with arguments
different from those shown to the approver. Binding a canonical input digest is
a separate frozen contract and durable provenance decision; this ADR does not
claim that limitation is closed.

### Declaration, validation, and runtime use one exact handshake

`TOOL_POLICY_ENFORCEMENT` is the byte exact identifier
`curie/mcp-tool-policy@1`. The handshake has three participants:

1. The bundle declares the same identifier in `toolPolicy.enforcement`.
2. A validating consumer calls `validate_bundle` with
   `enforces_tool_policy=TOOL_POLICY_ENFORCEMENT` only when the platform release
   includes the runtime that applies those semantics.
3. The runner loads the policy with
   `load_tool_policy(..., enforces=TOOL_POLICY_ENFORCEMENT)` and applies
   `classify_tool` at both interception points.

The deploy validator and runtime loader share the same pattern checker. The
bundle identifier, validator claim, and runtime loader claim must all match
exactly. A missing identifier, an unknown version, a malformed policy, or a
consumer that cannot claim enforcement is a refusal, never an empty policy.

This handshake must fail closed under version skew. A platform intake that
cannot name the exact contract refuses the deployment. Once intake may name the
contract on behalf of the platform release, the runner independently validates
and loads it at boot. An older runner that receives a policy bearing bundle
does not pass the enforcement identifier and refuses to boot. A newer runner
does not reinterpret an unknown future identifier as version 1. Version skew
may therefore make a deployment or boot fail, but it must never start an agent
with a declared policy left unapplied.

A bundle with no `toolPolicy` retains the existing behavior at validation and
runtime. Absence means that no tri state MCP classification was declared, not
that an empty deny policy was declared. This compatibility path covers every
bundle that predates the field.

### Visibility filtering is not authorization

Tools classified as `approvalRequired` remain visible in the catalogue offered
to the model so the existing approval lifecycle can be entered at `PreToolUse`.
Denied and unmatched tools are removed from the SDK catalogue. This filtering
can prevent futile calls and reduce model churn, but it cannot replace
execution enforcement: independent `PreToolUse` and `can_use_tool` refusals
remain the authorization boundary.

## Current implementation state

[PR #2058](https://github.com/curie-eng/curie/pull/2058) is merged. Current
`next` also realizes the canonical runtime name mapping; the shared decision at
`PreToolUse` and `can_use_tool`; and the API, validator, loader, and runner
handshake that makes policy-bearing bundles deployable. The implementation
keeps `approvalRequired` tools model-visible until `PreToolUse`, while it
removes denied and unmatched tools from the SDK catalogue. Independent runtime
enforcement still rejects denied or unmatched calls, so catalogue filtering is
not an authorization boundary.

## Consequences

The bundle owner gains a versioned, closed policy over a dynamic vanilla MCP
surface. Adding a tool at the server cannot silently widen the agent because
the unmatched default refuses it.

Policy is stable across connector and plugin mount shapes because authors use
one canonical name. The runtime owns the mapping from SDK names and refuses an
MCP name it cannot attribute.

The two interception points become one architectural seam. Any change to the
decision must preserve both consumers and their shared helper.

Policy denial is intentionally distinct from approval denial. Operators and
models are not shown an approval path for an action that no approver is allowed
to grant.

Existing bundles remain compatible by omitting `toolPolicy`. Policy bearing
bundles require a platform release and runner that both claim version 1
enforcement, which turns unsafe skew into a visible deploy or boot failure.

Denied tool visibility remains an efficiency and ergonomics concern, not a
security claim. Runtime interception remains the authorization boundary.

## Alternatives considered

### Keep allow by omission and gate selected tool names

Rejected. A server can publish a new tool after bundle review, and omission
would silently grant it. A closed capability surface requires unmatched MCP
tools to deny by default.

### Choose the most specific matching glob

Rejected. A narrow `allow` could then defeat a broad `deny` or
`approvalRequired` rule. Class precedence preserves the stricter outcome
regardless of authoring order or apparent specificity.

### Apply the policy only in `can_use_tool`

Rejected. The SDK can skip that callback after another permission rule allows
the call. That is not a complete execution boundary.

### Apply the policy only in `PreToolUse`

Rejected. It would leave the fake tier and the SDK permission backstop with a
different decision path. Both interception points must share the classifier.

### Treat catalogue filtering as the authorization boundary

Rejected. Visibility can reduce futile calls but does not prove that an
attempted call cannot execute through another path or under version skew.
Filtering is defense in depth, not authorization.

### Apply unmatched denial to built-in tools

Rejected. The canonical grammar describes MCP server and tool pairs, not
harness built-ins. Conflating an unclassified MCP tool with a non-MCP tool
would silently revoke unrelated capabilities and mix two ownership domains.

### Use live SDK names in the bundle policy

Rejected. The same MCP server receives different runtime prefixes under the
connector and plugin mount paths. Persisting those details would make policy
depend on packaging rather than on the capability the bundle owner intended to
govern.

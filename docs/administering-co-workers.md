# Administering Curie co-workers

Operate a Curie installation with explicit workspace bindings, human approval,
model settings and visibility into work. Start with the
[Software Factory](software-factory.md), then apply these controls to other
supported [co-workers](co-workers.md).

The procedures below target **v0.12.4** and were checked against its committed
CLI manifest and source contracts. They are not a report of a fresh deployment
or live approval test. See the [capability reference](capabilities.md) for
release boundaries and the [operations guide](operations.md) for complete
procedures.

## Own the installation and its external connections

Curie runs its platform and sandbox workloads in your Kubernetes installation.
You own the Helm release, credentials, storage and upgrade policy. Model
requests and workspace interactions still reach the providers and services you
configure. Self hosting does not imply that data never leaves your infrastructure.

Use the [installation procedure](operations.md#installing-and-inspecting-the-curie-platform-on-the-cluster)
and [upgrade procedure](operations.md#upgrading-the-chart). Inspect what is running:

```bash
curie cluster status
curie cluster observability
```

Examples use the default `curie` namespace and release. On commands that expose
`--namespace` and `--release`, supply your installation names. For API commands,
check each command's help: discovery and explicit `--api-url`/`--api-key` modes
are distinct. Platform credentials have broad administrative authority and do
not belong inside an agent bundle or chat.

Installation ownership is not a complete multi-tenant identity or RBAC product.
[#2917](https://github.com/curie-eng/curie/issues/2917) owns the broader identity
and tenancy work; do not treat unreleased work as a v0.12.4 guarantee.

## Bind workspaces and credentials deliberately

Follow [Slack setup](your-first-slack-agent.md) and
[bundle deployment](operations.md#deploying-your-plugin-bundle-onto-the-curie-platform).
A channel binds one agent; an agent may have multiple bindings. Only one release
may connect to a given Slack app because Socket Mode otherwise splits events.
Named bot identity does not prove a shared human identity across channels.

Use the [connector secret procedure](your-first-slack-agent.md#giving-it-tools).
The bundle lists secret names, not values. Cluster delivery checks the cluster
identity, release and namespace scope. Tool credentials retain the permissions
of their external service account; keep those permissions appropriate for the
agent. Email is currently blocked by
[unverifiable adapter authentication](../apps/mail-adapter/README.md).

## Separate approval authority, tool policy and service permissions

These controls answer different questions:

| Control | Authority and limit |
|---|---|
| Tool policy and permission gates | Decide which runner tool calls are allowed or paused. Bundle gates and operator approval-required overrides are unioned. |
| Approval routes | Decide where a card appears and which authenticated principals may resolve it. Notification recipients do not become approvers. |
| External service permissions | Decide what an allowed tool can actually do. Approval cannot elevate a Kubernetes role, repository token or service account. |

Follow [route binding](approvals.md#binding-a-route-to-a-workspace). For a
manifest route named `change`, this illustrative configuration narrows approval
to explicit Slack users:

```bash
curie cluster approvals my-agent \
  --route-resolution change=C0EXAMPLE1 \
  --route-approvers change=users:U0EXAMPLE1
curie cluster approvals my-agent --list-routes
curie cluster approvals my-agent --list
```

A route write replaces the whole map: include every route you intend to retain.
Every declared route must be bound before a gated bundle becomes live. An
unbound route is not silently widened to the requesting channel. Slack channel
members are the default approvers when no explicit set is present; explicit
users apply immediately, while user-group membership has a TTL cache.

Use the authenticated Slack card, or follow
[terminal principal issuance and resolution](approvals.md#driving-it-from-the-cli).
Terminal resolution requires an authenticated principal and an explicit
`approvers.users` route; a platform API key alone is not resolver identity.
The first authorized resolution wins. A permission approval permits the recorded
call once, rather than permanently opening the tool. See the canonical guide for
policy grants and [recovery authority](approvals.md#break-glass-recovery).

## Configure model roles and bounded runs

Read [the factory review protocol](../examples/dark-factory/README.md#review-loops)
before changing model roles. The implementer and reviewer are configured
explicitly; this does not automatically classify task complexity and select a
model for each ticket.

```bash
curie cluster overrides dark-factory --model <provider-model-id>
curie cluster overrides dark-factory --reviewer-model <reviewer-model-id>
curie cluster overrides dark-factory
curie cluster budget dark-factory --limit 100 --output-tokens 100000
```

Values are illustrative. Preserve compatible provider configuration, credentials
and egress, following the [model provider contract](interfaces/model-provider/INTERFACE.md).
`--clear-model` restores the platform default; `--clear-reviewer-model` restores
the credential default. Boot configuration changes require a new sandbox boot;
a live thread retains its boot configuration. See
[the factory installation bounds](operations.md#the-default-factory-agent) for
execution deadlines and worker/runner limits.

Despite the CLI's daily USD wording and `max_usd_per_day` field name, the current
USD limit is passed to the SDK as a **session ceiling**, not a daily total across
runs. SDK cost may be an estimate rather than provider billing, and enforcement
can overshoot by one API call. The output token ceiling is per run, checked when
the SDK reports usage rather than mid-token. Neither limit is a guaranteed bill.
See [the canonical budget caveats](../examples/dark-factory/README.md#deploy-it-as-the-factory-agent)
and [runner accounting](../runner/src/curie_runner/budget.py). Check provider
billing alongside Curie's trace costs. Use
[Measuring factory outcomes](guides/measuring-factory-outcomes.md) to relate cost
to completed work without treating token usage as ROI. Pricing documentation remains owned by
[#3923](https://github.com/curie-eng/curie/issues/3923).

## Govern what is remembered

Follow [memory operations](operations.md#letting-the-agent-remember-facts):

```bash
curie cluster overrides my-agent --memory-writes on
curie cluster memory my-agent --guidance
curie cluster memory my-agent --guidance-from guidance.md
curie cluster overrides my-agent --memory-writes off
```

Writes default to off. Turning them off stops future saving at the next boot;
existing facts remain readable. Agent facts load across that agent's bindings.
Channel facts load only for the current binding. Shared agent memory therefore
needs a deliberate content policy; it is not a blanket shared conversation
history or a cross-channel human identity link.

The [memory contract](interfaces/memory/INTERFACE.md) specifies sandbox token
scope, fact author attribution, limits and operator APIs. The sandbox's read
credential cannot write facts; its per-turn write credential is agent bound and
cannot access another binding's channel memory. Operators use a broader plane
for inspection and edit/delete. `curie cluster memory my-agent` reads the legacy
memory log; do not mistake it for a complete listing of every agent and channel
fact. New threads pick up changed guidance. Default fact limits and namespace
caps bound storage and prompt size, not the correctness of remembered content.
Memory feature work remains in [#28](https://github.com/curie-eng/curie/issues/28).

## Inspect work and stop it when needed

Find the Console and Langfuse URLs through `curie cluster observability`, then
inspect run data through the API-backed commands:

```bash
curie cluster observability runs --limit 10
curie cluster observability run <trace-id>
curie cluster observability metrics --metric cost_usd
curie cluster work-items
```

Use [work item outcomes](operations.md#reading-work-item-outcomes) for factory
status and failure causes, and [change receipts](operations.md#what-a-reply-says-the-agent-changed)
for reported effects. Traces and receipts are observations, not authorization
or proof of a successful external action. Approval records and authenticated
resolutions provide the decision trail described in [Approvals](approvals.md).
The Factory console work in
[#4102](https://github.com/curie-eng/curie/issues/4102) is fixed on `next`, not a
released v0.12.4 feature.

For an agent-level stop and subsequent re-enable, use:

```bash
curie cluster kill my-agent --yes
curie cluster resume my-agent
```

These are the supported agent lifecycle controls, not rollback of side effects
already performed. Resume re-enables a killed agent; it does not resolve an
approval or guarantee restart of an individual failed factory item. See the
[CLI reference](../cli/README.md) and
[control API](../apps/api/src/curie_api/routers/control.py). Approval rejection,
agent kill and installation removal have distinct scope. Do not use
`cluster down` as a routine single-task cancellation procedure.

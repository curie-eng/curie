# 219. A deploy binds an agent's runner image at runtime, outside the Helm release

Date: 2026-10-12

Status: Draft

This ADR supersedes in part [ADR 0173](0173-a-bundle-may-layer-its-own-runner-image.md):

1. decision 3, which has the release render the agent's SandboxTemplate with
   the bundle's digest;
2. decision 4, which has the release prewarm each distinct runner digest it
   renders;
3. the mechanics of decision 5 that read an agent's binding from release
   values. The deploy refusal and the upgrade report themselves stand.

Decisions 1 and 2 of ADR 0173 (the bundle declares a layered Dockerfile, and
`curie build` pins its digest and base in the lock) stand unchanged. ADR 0173
is Accepted and this Draft does not edit it; the back link waits for this ADR's
acceptance, as ADR 0045 requires. This ADR leaves the cluster tier clause of
[ADR 0009](0009-per-agent-connector-auth.md) in force: per-agent connector
secrets stay in the release (decision 6 below).

## Context

`curie cluster deploy` of a bundle that declares a layered runner (ADR 0173)
binds that agent to its digest by upgrading the platform Helm release. On a
GKE install on 2026-10-08 one such bind took 389 s. The cost is structural:

1. `bind_commands` in `cli/src/cluster_secrets.rs` runs `helm upgrade
   --reset-then-reuse-values --set agentSandbox.runnerImages.<agent>=<digest>`
   on the whole release, then `retire_claims_command` deletes the agent's
   SandboxClaims. `bind_need` and `bind_if_changed` (#3082) skip the upgrade
   only when nothing changed. Any real change is a full upgrade.
2. A full upgrade fires every upgrade hook the chart declares:
   `schema-migrate.yaml`, `worker-upgrade-drain.yaml`, `preflight-gvisor.yaml`,
   `preflight-controller.yaml`, `preflight-avx.yaml`, `rustfs.yaml`,
   `mail-adapter-persistence.yaml`, `langfuse-model-pricing.yaml`, and
   `grafana-connector-token.yaml`. Binding one agent's image drains the worker
   ([ADR 0209](0209-pause-authority-after-a-drain-is-a-lease-held-by-a-live-upgrade-step.md))
   and runs migrations.
3. An agent's first per-agent value changes `CURIE_AGENT_SANDBOX_POOLS` in
   `charts/curie/templates/worker.yaml`, rendered from
   `curie.agentSandboxPoolAgents` in `charts/curie/templates/_helpers.tpl`, so
   the worker Deployment rolls. `claim_warm_pool` in
   `apps/worker/src/curie_worker/sandbox/types.py` reads that list (and
   `CURIE_AGENT_CONNECTOR_SECRET_POOLS`) to decide whether an agent has its own
   pool.
4. Six per-agent sources feed that list and the per-agent SandboxTemplate and
   SandboxWarmPool loop in `charts/curie/templates/agent-sandbox.yaml`:
   `agentSandbox.connectorSecrets`, `agentSandbox.registryEgress`,
   `agentSandbox.runnerImages`, `agentSandbox.workspaceSizeLimits`,
   `factory.sandboxEgress`, and `agentSandbox.poolAgents`. `bind_forge_egress`
   (#4397) in the same file is a second per-agent bind that also runs a full
   `helm upgrade`.
5. The same values drive `charts/curie/templates/runner-prewarm.yaml`, which
   renders one prewarm container per distinct layered digest, and
   `charts/curie/templates/runner-resources-admission.yaml`, whose
   ValidatingAdmissionPolicy image allowlist is a CEL literal rendered from
   `agentSandbox.runnerImages`.
6. Keeping bindings in release values has cost a bug class in `cluster
   upgrade`, which must read, omit, and rebind them (`layered_bindings`,
   `omit_runner_image_bindings`, `rebind_runner_image_bindings`): #3422,
   #3849, #4321, #4332, #4344.

The maintainer has set the direction: a deploy must not need a Helm upgrade to
bind a bundle's runner image. Skipping hooks on the bind upgrade was considered
and rejected (see Alternatives considered).

A precedent exists on `next`. Per-agent runner resources (#3209,
`apps/worker/src/curie_worker/sandbox/resources.py`) and per-claim token
templates (#3842) are SandboxTemplates and warm pools the worker writes at
runtime as copies of a chart template. The `-runner-resources` policy in
`runner-resources-admission.yaml` holds every such write to the chart's shape:
names ending in `-resources` and `-resources-pool`, resource requests and
limits, hardening, RuntimeClass, no host access, the runner ServiceAccount, and
the image allowlist. The worker Role in `worker.yaml` grants `get`, `create`,
`patch`, `list`, and `delete` on `sandboxtemplates` and `get`, `create`, and
`patch` on `sandboxwarmpools`.

Hard constraints this decision must meet:

1. Binding, changing, or clearing one agent's runner image runs no `helm
   upgrade` and fires no Helm hook.
2. It does not roll the worker or any platform Deployment.
3. A sandbox cold boot still never pulls an image (ADR 0173 decision 4).
4. The worker cannot widen the image allowlist it is held to (the #3842
   posture).
5. Connector secret values never reach argv, a SandboxClaim, Postgres, or a
   worker read (ADR 0009).
6. No hook skip value is stored in the release.

## Decision

**The operator's CLI records each agent's runner image binding in one
namespaced ConfigMap that only the operator's credential can write, and
prewarms the digest before it records it. The worker watches that ConfigMap
and creates the agent's SandboxTemplate and warm pool at runtime, extending the
#3209 worker-owned template mechanism. A separate admission policy takes its
allowed images from that ConfigMap, so the worker can run only a digest the
operator bound, and only in a pod that carries that agent's identity.** This is option 2
below, with option 1's runtime objects as its realization.

1. **Owner.** The worker owns each agent's image-bound SandboxTemplate and
   SandboxWarmPool. It creates them when the bindings ConfigMap names the
   agent, and creates a new pair when the agent's digest changes. The
   template is a copy of the chart template the agent would otherwise use (its
   own chart-rendered per-agent template when one exists, else the shared
   runner template) with two changes: the runner container image becomes the
   bound digest, and the pod template's `curietech.ai/agent` label is set to
   the agent, so the pod has the same identity a chart-rendered per-agent pool
   gives it. Names are
   `<fullname>-agent-<agent>-<digest12>-resources` and that name plus `-pool`,
   where `<digest12>` is the first 12 hex characters of the sha256 digest, so
   they satisfy the existing `-resources` and `-resources-pool` name rule; a
   name the agent name makes longer than a DNS-1123 subdomain is refused before
   any write. Each object carries the agent label and an owner reference to the
   bindings ConfigMap. The worker garbage collects on every bindings watch
   event, at start, and on each pass of the consumer's existing periodic orphan reaper (the
   maintenance loop in `apps/worker/src/curie_worker/consumer.py` that calls
   the substrate's `reap_orphans`), so a pair spared while its
   claims were still live is collected on a later pass: a worker-owned image pair whose agent and digest the ConfigMap no
   longer names stops receiving claims at once, and is deleted only once no
   SandboxClaim names its pool. A digest change, a clear, or an agent's
   removal therefore deletes the old pair after the deploy's claim
   retirement, never before it. Kubernetes garbage collection removes the
   rest when the ConfigMap is deleted. A #3209 resources copy or a #3842 per-claim copy
   for a bound agent takes the image-bound template as its source and keeps
   its labels, so every link in that chain meets the same admission.
2. **Routing.** The bindings ConfigMap, read through a watch, replaces
   `CURIE_AGENT_SANDBOX_POOLS` as the worker's source of truth for agents bound
   to a runner image. That env value keeps listing only agents whose per-agent
   pool the chart renders for Helm-owned values (decision 7), so a first image
   bind changes no worker env and rolls nothing. `CURIE_AGENT_CONNECTOR_SECRET_POOLS`
   is unchanged. For an agent the ConfigMap binds, `claim_warm_pool` returns
   the image-bound pool. If that pool does not exist yet, the worker creates it
   at claim, as #3209 does for a resources override, and then claims it. If the
   create is refused, the claim fails with a named error. A bound agent is
   never sent to the platform pool. At start the worker reconciles every
   binding into its pair before its consumer reads its first turn, the same
   publish before read order the consumer already keeps for liveness.
3. **Admission.** The chart adds a second ValidatingAdmissionPolicy,
   `<fullname>-runner-bindings`, whose `paramKind` is ConfigMap and whose
   binding's `paramRef` names the bindings ConfigMap with
   `parameterNotFoundAction: Deny`. It matches only worker writes of
   SandboxTemplates that carry the runner-binding label, and admits the write
   only when every container image is either in the chart literal or equal to
   the image the ConfigMap binds to the template's agent, where the template's
   object label and its pod template's `curietech.ai/agent` label must both
   name that agent. The existing `-runner-resources` policy keeps every other validation (name
   suffix, resources, hardening, RuntimeClass, no host access, no token mount,
   runner ServiceAccount, host safety) for image-bound templates. Its image
   validation admits a template carrying the runner-binding label to the second
   policy and holds every other template to the chart literal. A worker that
   sets the label on a template meets the second policy, which denies it
   unless the operator bound that image to that agent, and denies everything
   while the ConfigMap is absent. The worker has no write access to the
   ConfigMap, so it cannot widen either allowlist. Which pool a claim names
   stays the worker's routing authority, as it is for chart-rendered per-agent
   pools today; admission guarantees that a bound image runs only in a pod
   labelled as the agent it was bound to. The `-runner-claim-cleanup` policy,
   which today admits a worker DELETE of a template only when it carries the
   per-claim label, also admits a DELETE of a template that carries the
   runner-binding label and ends in `-resources`, so decision 1's garbage
   collection is possible.
4. **Prewarm.** The CLI pulls a newly bound digest onto every node before it
   records the binding. It applies one DaemonSet per distinct bound digest,
   `<fullname>-runner-prewarm-<digest12>`, shaped like the layered containers
   `runner-prewarm.yaml` renders today: the digest itself runs `sleep
   infinity`, so the image stays in use and kubelet image garbage collection
   cannot evict it. It carries an owner reference to the bindings ConfigMap.
   The name is a function of the digest, so the CLI creates it when it is
   absent and reuses it when it exists, and never patches it. The deploy waits until that DaemonSet reports every
   desired pod ready, and fails with the DaemonSet named if it does not within
   the deploy timeout; only then does it write the binding. The CLI deletes a
   prewarm DaemonSet when no binding names its digest. `runner-prewarm.yaml`
   stops rendering per-agent digests and prewarms only the images the chart
   itself renders.
5. **RBAC.** Worker ServiceAccount: add `get`, `list`, and `watch` on
   `configmaps` restricted to the bindings ConfigMap by `resourceNames` (the
   watch uses a `metadata.name` field selector, which that restriction
   requires); add
   `list` and `delete` on `sandboxwarmpools`; no other change. API
   ServiceAccount: no change, and no access to the bindings ConfigMap or the
   prewarm DaemonSets. Agent-sandbox controller: no change. Operator CLI
   credential: `get`, `create`, and `patch` on the bindings ConfigMap, and
   `get`, `list`, `create`, and `delete` on DaemonSets in the release
   namespace, in addition to the access `cluster deploy` already uses to retire
   SandboxClaims. No ServiceAccount the platform runs gains write access to the
   ConfigMap or to DaemonSets.
6. **Connector secrets.** `agentSandbox.connectorSecrets` stays in the Helm
   release, and ADR 0009's cluster tier clause stands. Reason: its protection
   is that the chart, not the worker, renders the `secretKeyRef` that ties an
   agent's template to its own Secret. Moving the Secret out of the release
   would let the worker write templates that name Secrets, and admission would
   then have to prove the named Secret belongs to the template's agent, which
   is a separate security decision. Connector secrets change only when an
   operator rotates them, not on every rebuild of a bundle, so the deploy that
   binds a new image digest does not run a Helm upgrade for them. A deploy that
   does change a secret still upgrades, as today.
7. **Other per-agent values.** `agentSandbox.registryEgress`,
   `agentSandbox.workspaceSizeLimits`, `factory.sandboxEgress` (including
   `bind_forge_egress`), and `agentSandbox.poolAgents` stay in Helm. They grant
   capability (egress, storage) or declare pool membership, and they are
   operator policy rather than a bundle build output. A per-agent template that
   draws on both places has one owner per object: the chart owns the agent's
   chart-rendered template from those values, and the worker owns the
   image-bound copy. The worker never edits the chart object, and the chart
   never renders the copy. When the chart object changes on a Helm upgrade, the
   worker replaces its copy on the next reconcile.
8. **Deploy and upgrade checks.** ADR 0173 decision 5's deploy refusal and the
   `cluster upgrade` report read bindings from the bindings ConfigMap, never
   from release values. Each entry records the bound image and the platform
   runner digest it was built on, from the bundle's lock. `cluster upgrade`
   stops omitting and rebinding values (`omit_runner_image_bindings` and
   `rebind_runner_image_bindings` are removed); it names every binding whose
   recorded base will not match the target runner. Between the upgrade and the
   owner's redeploy, the worker refuses claims for such an agent with a named
   error that carries the rebuild command, because it compares the entry's
   recorded base with the platform runner it is configured with. This closes
   ADR 0173 decision 5's open question: the agent does not run.
9. **Rollback.** Neither `curie cluster rollback` nor a bare `helm rollback`
   touches runtime bindings: the ConfigMap, the prewarm DaemonSets, and the
   worker-owned objects are not release objects. `curie cluster rollback`
   prints the same base report as `cluster upgrade` before it rolls back. An
   agent bound to a layer built on a newer runner than the one rolled back to
   is refused at claim, as in decision 8, until its owner rebuilds and
   redeploys.
10. **Upgrade migration.** The migration spans two releases, so no worker of
    either version ever lacks its pool or its admissible image. `curie cluster
    upgrade` to the first release that carries this decision (the migrating
    release) reads `agentSandbox.runnerImages` from the release, applies the
    prewarm DaemonSet for each digest and waits for it, and writes each
    binding to the ConfigMap, recording the release's current platform runner
    as its base (deploy already refused any other base). It then upgrades with
    the stored values left in place. The migrating chart still renders
    `agentSandbox.runnerImages` exactly as the previous chart did: the
    per-agent templates and pools, their prewarm containers, and their digests
    in the `-runner-resources` image literal. The one exception is
    `curie.agentSandboxPoolAgents`, which stops reading `runnerImages` keys, so
    `CURIE_AGENT_SANDBOX_POOLS` on the new worker pods already follows decision
    2 and a clear during the migrating release takes effect at once. Worker
    pods of the previous
    release keep routing to those pools and writing their unlabelled per-claim
    templates through the whole rollout, whether or not the drain hook is
    enabled. Worker pods of the migrating release route bound agents by the
    ConfigMap alone (decision 2) and reconcile their image-bound pairs before
    they read a turn. In the migrating release, deploy writes only the
    ConfigMap and never the stored values. The next `cluster upgrade` removes
    `agentSandbox.runnerImages` from the values, and the chart of the release
    after the migrating one fails to render when that key is non-empty, with a
    message naming `curie cluster upgrade`, so a raw `helm upgrade` cannot
    carry stale bindings across. No chart object that still has a Helm-owned
    input (decision 7) is deleted by the CLI at any step; Helm prunes only
    what its chart stops rendering. A downgrade with `curie cluster upgrade
    --version` to a release before the migrating one first compares each
    binding's recorded base with the target release's runner. It refuses the
    downgrade while any binding's base does not match, naming each agent and
    its rebuild command, because the older worker has no claim-time base
    refusal. When every base matches, it copies the bindings back into
    `agentSandbox.runnerImages` for that upgrade and leaves the ConfigMap in
    place, unused by the older chart. A bare `helm rollback` across the
    migration restores the values that revision stored, and is not the
    supported path.

Relation to other ADRs:

1. [ADR 0109](0109-retention-capacity-and-back-pressure-are-one-policy.md)
   decision 6 (a warm pool is per-agent or it does not exist) holds: the
   image-bound pool is that agent's pool, sized as the pool it copies.
2. [ADR 0168](0168-one-installation-hosts-several-bot-identities.md) decision 7
   admits a caller to a hosted connector by the authenticated agent identity
   in the caller token the worker mints per claim, which this ADR does not
   touch. The network side of
   [ADR 0086](0086-bundles-declare-connectors-the-platform-hosts-them.md)
   connectors and `registryEgress` select sandbox pods by the
   `curietech.ai/agent` pod label. The image-bound template sets that label to
   its agent (decision 1) and admission requires it (decision 3), so both keep
   working for a bound agent.
3. ADR 0209's worker drain hook no longer fires on an image bind, because no
   upgrade runs. It still fires on every real upgrade, including the migrating
   one.
4. [ADR 0129](0129-one-release-owns-a-clusters-shared-singletons.md) (Draft):
   every runtime object here is namespace scoped and belongs to one release. No
   cluster singleton is added. The new ValidatingAdmissionPolicy and its
   binding are cluster scoped chart objects named per release, like the
   existing `-runner-resources` policy.
5. [ADR 0216](0216-a-factory-repository-binding-selects-its-own-runner-image-built-from-a-tool-catalog.md)
   decision 5 renders per-repository factory images from operator values at
   install time. This ADR does not change that. Those images are bound by the
   operator at `cluster up` or `cluster upgrade`, not by a bundle deploy.

## Consequences

1. Binding, rebinding, or clearing an agent's runner image is a ConfigMap
   patch, a DaemonSet rollout, and a claim retirement. It runs no Helm
   upgrade, fires no hook, drains nothing, and rolls no Deployment. Its wall
   time is the prewarm pull.
2. The binding leaves the Helm release, so `helm get values` no longer shows
   it. `curie cluster status` must show the ConfigMap's bindings instead. That
   is new CLI work.
3. The `cluster upgrade` bug class around retained runner bindings goes away
   with the code that caused it.
4. A deploy that also changes a connector secret, a forge egress grant, or
   another Helm-owned per-agent value still runs a full upgrade. Only the image
   bind becomes upgrade-free.
5. The operator's kube credential needs DaemonSet and ConfigMap write access in
   the release namespace. An install whose deploy credential is narrower than
   that cannot bind a layered runner until it is widened.
6. The worker gains a watch and a reconcile loop for the bindings ConfigMap.
   Its correctness (garbage collection, the refusal on a base mismatch, the
   claim-time create) is new worker code, and implementation issues will name
   its realizing paths on acceptance.
7. During the migrating release only, a digest an agent was bound to before
   the migration stays in the chart's image literal even after a deploy
   rebinds or clears it, until the next upgrade removes the stored values. It
   was an operator-bound digest, so this widens nothing the operator had not
   already approved.
8. The chart literal and the ConfigMap now both feed admission. The chart
   literal still covers every image the chart renders, so an install with no
   bindings behaves as today.

## Alternatives considered

### Option 1: per-agent pools created at runtime from the agent record

The worker, or the agent-sandbox controller, would read the bound digest from
the agent record in Postgres and create the pool. Rejected as the binding's
source of truth. The agent record is written by the API on the deploy path, so
the API (or anything that can write that row) would decide what admission
allows, and the worker reads Postgres, so the image allowlist would sit on a
store the worker can influence. That breaks the constraint that the worker
cannot widen its own allowlist. Its runtime object creation is kept: decision 1
is the #3209 mechanism this option extends.

### Option 2: a ConfigMap the CLI writes and the worker watches

Chosen, as the Decision above.

### Option 3: keep the binding in Helm, set only at `cluster up` or `cluster upgrade`

Deploy would record the requested digest and the next `cluster upgrade` would
render it. Rejected. A deploy whose new image takes effect only at the next
platform upgrade is not a deploy, and the agent would run its old image, or
none, in between. It keeps the full upgrade, its hooks, and the worker roll for
every first bind, and it keeps the `cluster upgrade` bug class.

### Option 4: skip hooks on the bind upgrade

`helm upgrade --no-hooks`, or a stored values flag that tells hooks to skip.
Rejected for two reasons. The worker still rolls on a first bind, because
`CURIE_AGENT_SANDBOX_POOLS` changes. And a stored skip value persists in the
release, so a later real upgrade of the same release would skip its migrations
and drain too.

## Tracking

Implementation issues will be filed on acceptance, each naming its realizing
code path. This ADR changes the direction of
[#3170](https://github.com/curie-eng/curie/issues/3170), the ADR 0173 umbrella,
whose closed child issues built the Helm values path this ADR replaces.

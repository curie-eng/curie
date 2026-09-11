# 144. The cluster lifecycle admits before it mutates, and its pause authority is installation-scoped

Date: 2026-09-09

Status: Draft

This ADR is a **backfill**. The four decisions below were made and merged on
2026-09-08 as [#2471](https://github.com/curie-eng/curie/pull/2471),
[#2472](https://github.com/curie-eng/curie/pull/2472),
[#2473](https://github.com/curie-eng/curie/pull/2473) and
[#2474](https://github.com/curie-eng/curie/pull/2474), each of which recorded
"no new ADR: a correction to the existing lifecycle contract." Taken singly that
was defensible; taken together they changed when `cluster up`, `cluster down`
and `cluster rollback` are allowed to mutate a cluster at all, which is an
architectural decision that was not written down. A maintainer directed this
record on 2026-09-09.

**The status is `Draft`, deliberately, even though every clause here is already
built on `main`.** That direction was to write the record, not an approval of
its content, and
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) clause 4 and
`docs/adr/AGENTS.md` step 3 both say that existing implementation cannot
retroactively accept a Draft — implementation is audit context, never acceptance
authority. `Accepted` requires explicit maintainer approval published with the
status, which this ADR does not carry. Promoting it is a one-line status edit
under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
clause 1 once that approval is recorded; the realizing code paths that
[ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md)
also requires are already named below. A Draft that describes shipped code is an
unusual state, and it is the honest one: the code is on `main` either way, and
what is missing is the recorded approval, not the evidence.

It also records what was **not** taken. [#2391](https://github.com/curie-eng/curie/pull/2391)
("Implement validated transactional upgrade and bounded recovery") proposed a
different answer to the same problem and was closed unmerged on 2026-09-07. The
2026-09-07 ruling on that closure was that this ADR precedes any successor of
#2391; that ordering is the reason this file exists.

Every `path:line` citation below is pinned at commit `0d3f6d4a`, the `main`
commit these four changes had reached when this ADR was written. `docs/adr/` is
excluded from the doclint citation walk on purpose — an Accepted ADR is
immutable and its coordinates are allowed to rot with the code they described —
so read them with `git show 0d3f6d4a:<path>` rather than against `HEAD`.

## Context

Curie's cluster lifecycle is three Helm-driven verbs — `cluster up`,
`cluster down`, `cluster rollback` — plus a pre-upgrade drain hook. Through
v0.8.6 each of them decided whether to proceed from a signal that was weaker
than the decision it authorized:

- **The drain gate trusted a release-wide flag.** One key,
  `{prefix}:upgrade:quiesce`, told every worker on a Valkey to stop claiming.
  A drain Job retained from a previous installation of the same release name
  therefore paused a *fresh* installation for up to the quiesce TTL, while every
  pod reported healthy and no surface said why nothing was being claimed
  ([#2374](https://github.com/curie-eng/curie/issues/2374)).
- **Namespace ownership was stamped after Helm, and only for namespaces Curie
  created.** An install that failed inside Helm left a namespace with no
  ownership pair, so `cluster down` — which sweeps only on the pair — could not
  remove it. Installing into a pre-existing namespace was supported and
  deliberately left unlabeled
  ([#2375](https://github.com/curie-eng/curie/issues/2375)).
- **Rollback trusted Helm revision status.** After v0.8.5 advanced the live
  database to Alembic revision `0039`, a `superseded` v0.8.4 revision was still
  status-eligible, but the v0.8.4 image ran `alembic upgrade head` at startup
  and refused a revision it did not know. The API Deployment exceeded its
  progress deadline while the old replica kept serving — a rollback that made
  things worse, selected by a filter that had no way to know
  ([#2296](https://github.com/curie-eng/curie/issues/2296)).
- **The gVisor retry trusted `helm upgrade --install`.** A first `cluster up`
  on a cluster with no `gvisor` RuntimeClass aborted mid-install, left a failed
  revision, and retried with `--install`. Helm read that as an *upgrade*, fired
  the pre-upgrade drain hook, and the hook hung on `<release>-secrets` because
  revision 1 never rendered it. Every later `cluster up` repeated the timeout
  ([#2347](https://github.com/curie-eng/curie/issues/2347)).

The four defects are one shape: **a decision to mutate was authorized by a
signal that did not establish the thing being assumed** — a global flag standing
in for installation identity, post-hoc labelling standing in for ownership, Helm
status standing in for startability, and an install-or-upgrade convenience
standing in for a known-good release.

#2391 proposed to close all four at once by making the upgrade itself
transactional: one new driver holding a local operation lock and a
compare-and-swap checkpoint, validating schema and image declarations before
mutating, and offering a bounded recovery that rebound the original hook Jobs by
UID before permitting an ordinary Helm rollback. It was closed unmerged with
every required runtime tier unproved on its combined candidate, and its own body
recorded the limits that made it unsafe to land: a local lock and a checkpoint
CAS do not establish a distributed execution lease, and it did not guarantee
that the previous version stayed serving.

## Decision

**Each lifecycle verb admits before the mutation that gate governs.** Each
admission is a refusal that fails closed when it cannot read what it needs and
states the observed reason. Naming a machine-readable remedy is not uniform: the
schema and gVisor refusals carry an explicit fix, while four of the namespace
gate's six refusal paths state the reason only. The invariant is per-gate, not
global, and the boundaries differ:

- namespace ownership is established **before Helm runs at all** on `cluster up`;
- schema admission runs **before the `helm rollback` call**, after the existing
  status filter;
- the failed-revision discard runs **after the first Helm install attempt has
  already aborted** and before the gVisor-off retry — it is recovery from a
  mutation that happened, not prevention of one;
- the drain gate runs **inside Helm's own `pre-upgrade` hook**, so Helm has been
  invoked; its guarantee is that the workload roll does not proceed, not that
  Helm was never called.

Nothing here establishes a single admission point covering every mutating `helm`
or `kubectl` call. The four gates are independent, live at the verb that already
exists, and are each verifiable against a live cluster on their own.

### 1. Drain pause authority is scoped to one Helm installation and revision

The quiesce marker is keyed per installation:
`{prefix}:upgrade:quiesce:{installation_id}`. The pre-#2374 release-wide key
`{prefix}:upgrade:quiesce` survives only for surfaces with no Helm installation
boundary — standalone and Compose, which carry a blank installation ID — and for
one bridging release.

The installation ID is minted into the chart-managed Secret on install and
reused on upgrade. The first upgrade from a chart that predates installation IDs
adopts the live Secret's UID and enables the one-release legacy bridge, so old
and new replicas pause together through the transition. A client-only upgrade
has no lookup result; it renders `installationIdObserved=false` and the hook
refuses before it constructs a Valkey client, so an unobserved upgrade can
neither mutate a marker nor pretend a failed lookup was an identity.

Markers carry the Helm revision and a finite TTL. Writes and clears are one
atomic Lua evaluation over every key applicable to the invocation: a marker at a
higher revision fences the write, a same-revision retry refreshes the TTL while
retaining the original marker byte for byte, and a clear compares every
applicable key before deleting any of them so a delayed release cannot clear
half of a newer mixed-version marker. A permanent flag is never written — the
TTL is validated to be strictly greater than the drain wait, so an upgrade that
dies between quiesce and release lapses instead of leaving a fleet that has
silently stopped answering.

A paused fleet is **reported, not inferred**. `cluster status`, `doctor` and the
message-waiting diagnostics read the claim state and surface `quiescing` as a
condition, rather than showing healthy pods that happen to claim nothing. The
diagnostic read exposes state, `since` and revision only, and reports an
unreadable marker as `unknown` — `cluster status` files that as a warning, not
as unhealthy.

The claim path itself deliberately goes the other way: the per-claim quiesce
check **fails open**, because failing closed would idle every replica in the
release on a transient Valkey blip, turning the one read meant to make upgrades
safer into a fleet-wide stall at exactly the moment Valkey is least healthy. So
an unreadable marker means "keep working" to a worker and "I could not tell you"
to an operator, and those two answers are deliberately different.

### 2. The install namespace is owned before Helm runs, and ownership is never assumed

Ownership is the pair `curietech.ai/created-by=<release>` plus
`curietech.ai/created-in=<install namespace>`. Both terms are required by the
teardown sweep, which is the [#1654](https://github.com/curie-eng/curie/issues/1654)
rule that two installs sharing the default release name in different namespaces
never delete each other's namespaces. `cluster up` establishes that pair
**before** Helm, so a failed install is still removable by `cluster down`:

- **Absent namespace** — created atomically already carrying both labels.
- **Namespace already carrying exactly this pair** — reused.
- **Namespace carrying a partial or foreign pair** — refused with the observed
  values. Incomplete ownership is never treated as adoptable.
- **Unlabeled namespace** — adopted only after every one of: advertised remote
  APIServices report `Available=True`; namespaced API discovery returns a
  complete resource list; the inventory of every discovered resource is empty
  apart from the default ServiceAccount and the `kube-root-ca.crt` ConfigMap;
  and a JSON patch guarded on the Namespace UID and resourceVersion applies
  cleanly. Any read that fails is a refusal, not an absence.
- **A namespace with foreign labels** — refused. Only an empty label set, or
  exactly `kubernetes.io/metadata.name`, is adoptable.
- **A terminating namespace** — refused for `up`. `down` can still inspect it
  and remove matching retained hook Jobs.
- **The shared `agent-sandbox-system` namespace** — never adopted, create-only
  stamping retained.

Teardown is symmetric and independent: it uninstalls the release, then removes
release-labeled hook Jobs, then sweeps only namespaces bearing the full pair.
Anything else is retained; the warning naming what was retained fires when the
sweep matched nothing at all.

The adoption inventory is explicitly a **point-in-time observation**. The
UID/resourceVersion guard protects Namespace metadata; creating a namespaced
object does not change the Namespace's resourceVersion, so the patch does not
serialize other writers. An unowned namespace must be kept unused by other
writers through adoption.

### 3. Schema compatibility is admitted before the rollback Helm call

Every v0.8.x application version declares the Alembic range it can start
against — `schema_min`, `schema_head` — in an in-tree catalog, which covers
0.8.0 through 0.8.6 and nothing earlier. A unit gate
asserts that every Alembic revision in the tree appears in the catalog and that
the current `Chart.yaml` `appVersion`'s `schema_head` equals the tree's head, so
the catalog cannot go stale silently.

After the existing `deployed`/`superseded` status filter and **before**
`helm rollback` runs, `cluster rollback` reads the live database revision from
the running API pod and refuses a target whose declared window excludes it. The
refusal is nonzero, names the compatibility boundary and the newest safe
fail-forward application version — chosen from the versions in this release's
own Helm history, not from the catalog at large — and prints no database
contents or credentials. The gate fails closed on every unreadable path: a nonzero
`kubectl exec`, unparseable `alembic current` output, a target Helm revision
from which no application version can be resolved, an application version absent
from the catalog, and a live revision absent from the catalog's revision chain.
"No application version" means neither source resolves: an empty `app_version`
falls back to the trailing version on the revision's `chart` field (`curie-0.8.4`
yields `0.8.4`), so a revision Helm recorded without `app_version` is still
gated rather than waved through.

The status filter is unchanged. Schema compatibility is an *additional* gate,
not a replacement — status answers "did Helm finish applying this", the window
answers "can this image start against the database as it is now", and neither
answers the other.

This is the **v0.8.x fail-closed repair** and is deliberately narrower than the
v0.9.0 compatibility contract recorded as ADR-0142 (on the `next` branch,
specified in [#2300](https://github.com/curie-eng/curie/issues/2300)), which
removes Alembic from API startup entirely and moves migrations into one
controlled phase. This decision assumes the v0.8.x behaviour it is defending
against: that API pods still migrate at startup.

### 4. A Helm record that never reached a known-good revision is discarded, not upgraded over

After the gVisor RuntimeClass admission abort, `cluster up` reads `helm history`
before the `security.gvisor.mode=off` retry. If no revision is `deployed` or
`superseded`, the release never reached a known-good install, and the failed
record is uninstalled so the retry is a clean **install** — not an upgrade whose
pre-upgrade hooks run against Secrets the cancelled revision never rendered. An
empty history is the same case and the uninstall is a no-op. A history that
already contains a known-good revision is left intact and the retry is an
ordinary in-place upgrade.

The read fails closed before the destructive step: a nonzero `helm history` that
is not Helm's own "release absent" reply, and an unparseable history, both abort
without uninstalling. Only the Helm uninstall from the teardown command list
runs; the sibling namespace sweep does not fire.

### 5. Not taken: one transactional upgrade driver (#2391)

The decision is the **decomposition**, not just the four gates. #2391's shape —
a new transactional `cluster upgrade` verb owning fencing, a persisted CAS
checkpoint, pre-mutation validation, post-mutation convergence and canary
re-checks, and a bounded recovery that rebinds original hook Jobs by UID — is
rejected as the way to land this ground. #2391's closure carried no written
rationale, so reasons 1 and 2 are quoted from the PR's own body and reason 3 is
this ADR's reasoning, recorded as such:

1. Its acceptance limits were structural, not incidental: a local operation lock
   and a checkpoint CAS do not establish a distributed execution lease, and it
   did not guarantee that the previous version stayed serving. Those are the two
   properties an operator would reasonably assume from the word "transactional".
2. No required runtime tier passed on the exact combined candidate; every tier
   was carried as pending on a four-PR stack.
3. **This ADR's own reading, not a recorded reason:** its refusals were
   entangled. A single driver that refuses for schema, identity, ownership and
   witness reasons at once cannot be verified one refusal at a time, and each of
   the four gates above carries its own live-cluster fix pin precisely because it
   can.

What #2391 proposed and this decision **does not** provide stays open and is not
authorized here: a distributed upgrade execution lease, a resumable upgrade
checkpoint, post-upgrade convergence and canary proof, and interrupted-phase
recovery. Those remain
[#2299](https://github.com/curie-eng/curie/issues/2299),
[#2300](https://github.com/curie-eng/curie/issues/2300) and
[#2301](https://github.com/curie-eng/curie/issues/2301). The 2026-09-07 ruling
requires only that this record precede a successor to #2391; it imposes no
further obligation, and naming which of these four gates such a successor
subsumes is a suggestion of this ADR's, not a rule.

## Realizing code paths

Pinned at `0d3f6d4a`.

**Drain authority (#2471).**
`apps/worker/src/curie_worker/upgrade_drain.py:90` is the owned-marker write
Lua and `:124` the owned-marker clear;
`apps/worker/src/curie_worker/upgrade_drain.py:190` selects the applicable keys
and `:198` writes the quiesce marker; `:406` is the hook body and `:486` the
render-time refusal when the installation identity was not observed.
`apps/worker/src/curie_worker/config.py:1178` derives the scoped key and
`:1188` the legacy key; `:777`, `:784`, `:790` and `:796` are the TTL,
installation ID, hook revision and legacy-bridge settings.
`charts/curie/templates/_helpers.tpl:834` resolves the installation identity for
the whole render, with `:872`, `:877` and `:882` its three accessors;
`charts/curie/templates/worker-upgrade-drain.yaml:61` is the `pre-upgrade`
hook annotation.
`cli/src/worker_claims.rs:145` parses the claim state and
`cli/src/ops.rs:8069` turns it into a `cluster status` condition.

**Namespace ownership (#2472).**
`cli/src/ops.rs:5903` and `:5904` are the ownership label constants;
`cli/src/ops.rs:6366` is `establish_primary_namespace_ownership`, the pre-Helm
gate; `:6260` is the emptiness inventory including the APIService and discovery
preconditions; `:6357` is the adoptable-label rule; `:6330` builds the
UID/resourceVersion-guarded adoption patch; `:6434` is the create-only
controller-namespace stamp; and `:12198` asserts the teardown sweep selector's
two required terms behaviourally.
Operator-facing behaviour is `docs/operations.md:205`.

**Schema window admission (#2473).**
`cli/src/ops.rs:8856` is the gate inside `rollback` (`cli/src/ops.rs:8811`),
running after the status filter and before `helm rollback`.
`cli/src/schema_window.rs:53` resolves an application version's window, `:66`
tests the live revision against it, `:94` picks the newest safe fail-forward
version, `:140` is the refusal decision, `:168` parses `alembic current`, and
`:188` redacts the probe text. `cli/src/application_schema_windows.json` is the
catalog and `cli/src/schema_window.rs:286` is the staleness gate against the
tree. `cli/src/ops.rs:611` is the test-only negative control that the clap path
never sets. Operator-facing behaviour is `docs/operations.md:324`.

**Failed-revision discard (#2474).**
`cli/src/ops.rs:7052` is the predicate and `:7060` the discard;
`cli/src/ops.rs:8366` is the eligible-status list both it and the rollback
selector read.

## Consequences

- A drain Job retained from a previous installation can no longer pause a fresh
  one, and a fleet that *is* paused says so in `status`, `doctor` and the
  message-waiting diagnostics instead of looking healthy.
- A `cluster up` that fails inside Helm leaves a namespace `cluster down` can
  remove, because ownership is established before Helm rather than after it.
- `cluster rollback` will refuse more often than it did, and the refusals it adds
  are exactly the ones that previously produced a stuck Deployment. Fail-forward
  becomes the named remedy in the refusal text rather than an operator's
  inference.
- The gVisor-off retry is an install, so the pre-upgrade drain hook does not run
  against a revision that never rendered its Secrets.
- Every gate reads live cluster state before mutating, so each verb now depends
  on the Kubernetes API, and on the API pod for rollback, in a place where it
  previously did not. A read failure is a refusal. That is the intended
  direction and it is also the source of the open consequences below.
- The four gates are independently testable and independently removable. Nothing
  here establishes an upgrade transaction; an interrupted `cluster up` or
  `cluster rollback` is still an interrupted sequence of Helm calls.

### Open consequences

These were found by review of the merged changes on 2026-09-09 and are recorded
here rather than resolved. They are consequences of the decisions above, not
arguments against them; each names what would close it.

1. **An existing install in an unlabeled pre-existing namespace cannot run
   `cluster up` at all.** The pre-#2472 code deliberately supported installing
   into a pre-existing namespace and deliberately left it unlabeled, so those
   namespaces carry no ownership pair *and* are full of the running release. The
   adoption path then refuses them as "contains non-default objects", before
   Helm. There is no adopt or force flag. The same gate refuses a *fresh* install
   into an empty namespace pre-provisioned with Pod Security Admission labels,
   `istio-injection`, or any GitOps label, as "has foreign labels" — a common
   enterprise pattern. The manual remedy is in "Upgrade path for existing
   installs" below. Closing this needs an explicit adoption override with the
   destruction consequence disclosed at acquisition time.
2. **Adoption patches ownership labels onto a namespace with no warning and no
   prompt, and `down` deletes on exactly that pair.** A namespace the operator
   created — Terraform, ArgoCD, or the `kubectl create ns` the install docs
   themselves suggest — becomes deletable, cascading everything in it. `down`
   does disclose the rule and requires a confirm or `--yes`, but that is
   disclosure at destruction time, and `--yes` is the documented automation path.
   The emptiness check is point-in-time; by `down` time the namespace also holds
   whatever the operator added since.
3. **`request_quiesce` ignores the fenced Lua result.** The write returns `0`
   and writes nothing when an applicable key holds a higher revision, but the
   caller awaits the eval and discards the result, then proceeds into the settle
   loop. If the write was fenced, the gate sees no in-flight leases and reports
   `drained=True` while workers were never quiesced — the exact failure the gate
   exists to prevent, reported as success. The reachable cases are narrow (the
   legacy-bridge upgrade against a second installation at a higher revision on
   the same Valkey, and the standalone/Compose path where the revision is `0`),
   but silent and in the unsafe direction. The fix is to check the eval result
   and fail the gate; it is tracked as bonus-drain task
   `curie-2471-quiesce-fence-return-check`. Relatedly, the clear returns `0` on
   the first non-matching key *before* deleting anything, so an unclearable
   foreign marker on the shared legacy key strands the installation's **own**
   scoped marker for the full quiesce TTL after a *successful* upgrade.
4. **The rollback schema gate has no override when every API pod is
   crashlooping.** The gate reads the live revision by exec'ing the API pod, and
   refuses when it cannot. When every API replica is crashlooping, `Init`, or
   `ImagePullBackOff` after a bad upgrade — the ordinary reason to reach for
   rollback — `cluster rollback` is unusable and the only way out is a raw
   `helm rollback`, which the operator docs now describe as the unsafe path.
   `--allow-failed-revision` relaxes only the status filter and runs earlier;
   `disable_schema_gate` is test-only and unreachable from clap. Closing this
   needs a fallback read (a migrate Job, a direct DSN, or a cached revision) or
   an explicit operator override, not a widening of the gate.
5. **An ambiguous Helm status routes to an unprompted `helm uninstall`.** The
   discard predicate fires when *no* revision is `deployed`/`superseded`, and the
   eligible-status list treats `unknown` as ineligible. A release reading
   `unknown` therefore satisfies "never deployed" and is uninstalled without a
   prompt, though it may be serving. This reuses a predicate written for a
   read-only selection decision as the trigger for a destructive one. The
   residual is narrow — reachable only from the gVisor-off retry branch, which
   normally means mid-first-install, and unparseable or nonzero history fails
   closed first — but the shape is an ambiguous signal authorizing a destructive
   action, and it should be a distinct predicate that treats `unknown` as
   "refuse and tell the operator", not as "never deployed".

## Upgrade path for existing installs

**Installs Curie created.** No action. The namespace already carries the
ownership pair, the quiesce marker migrates on the first upgrade via the
one-release legacy bridge, and the rollback gate only ever adds a refusal.

**Installs in a pre-existing namespace Curie did not label.** The next
`cluster up` will refuse before Helm. Until an adoption override exists, the
only path forward is to apply the ownership pair by hand:

```bash
kubectl label namespace <install-namespace> \
  curietech.ai/created-by=<release> \
  curietech.ai/created-in=<install-namespace>
```

An operator doing this is opting that namespace into deletion by a later
`cluster down`, which sweeps on exactly that pair and will cascade everything in
the namespace. That trade is the operator's to make and must be made knowingly;
it is open consequence 2 above.

**Installs whose namespace carries unrelated labels.** A namespace with Pod
Security Admission labels, `istio-injection`, or GitOps labels is refused as
foreign even when empty. The same manual pair applies, with the same
consequence.

**The legacy quiesce key.** Standalone and Compose keep the release-wide key by
design and need no migration. A cluster installation upgrading from a chart that
predates installation IDs gets the bridge automatically for exactly one release;
an operator pinning an older `worker.image.tag` while upgrading the chart will
see the pre-upgrade hook fail, because the older worker's argument parser
rejects the render-time observation flag the chart now passes. Upgrade the chart
and the worker image together.

**The gVisor-off retry.** No action, and nothing to migrate. The discard changes
only what happens inside a single `cluster up` invocation after that run's own
first Helm attempt aborted on the RuntimeClass, and a history that already
contains a `deployed` or `superseded` revision is left intact — so no existing
install can be uninstalled by it. An install currently wedged in the #2347 loop
(every `cluster up` timing out on the drain hook against a `<release>-secrets`
that revision 1 never rendered) is repaired by the next `cluster up` on a CLI
carrying this change, with no manual step.

**Rollback.** A rollback target that was safe before and is still within its
declared schema window is unaffected. A target outside it is now refused with
the newest safe fail-forward version named. If the API pod cannot be read, see
open consequence 4.

## Alternatives considered

1. **Land #2391's transactional upgrade driver instead.** Rejected for the
   reasons in Decision 5: its own acceptance limits were structural, no required
   runtime tier passed on the combined candidate, and entangled refusals cannot
   be verified one at a time. The ground it covered that these four gates do not
   remains open as #2299/#2300/#2301.
2. **Keep the release-wide quiesce key and shorten its TTL.** Rejected: a
   shorter TTL narrows the window in which a retained Job pauses a fresh
   installation but does not remove the confusion of authority, and shortening it
   below the drain wait breaks the gate it exists to serve.
3. **Stamp namespace ownership after Helm, as before, and teach `down` to sweep
   unlabeled namespaces.** Rejected: a sweep that does not require the ownership
   pair is exactly the #1654 defect, where one release's teardown deleted another
   release's namespace and killed a running bot.
4. **Adopt a populated pre-existing namespace by default and warn.** Not a
   recorded rejection: neither #2375 nor #2472 weighs this option in writing, and
   it is reconstructed here from the shipped behaviour so the reader knows the
   default was a choice. The reason the shipped default is defensible is that
   adoption grants deletion authority over objects Curie did not create. The cost
   is open consequence 1, and the resolution is an explicit override rather than a
   changed default.
5. **Make the rollback schema gate advisory — warn and proceed.** Rejected: the
   incident this gate exists for is a rollback that *did* proceed and left the
   Deployment past its progress deadline. A warning on a path an operator reaches
   during an outage is not read.
6. **Fix this at API startup instead of gating at the CLI.** Rejected here as
   out of scope. The v0.9.0 answer recorded in ADR-0142 is to take Alembic out
   of API startup entirely, run migrations in one controlled phase, and have API
   pods wait for a servable revision — with expand/contract sequencing so an N-1
   image keeps serving after an expand. Note that ADR-0142 explicitly *rejects*
   capping serving at `schema_head`, because an already-released N-1 image cannot
   declare a future expand it has not seen; this decision's CLI-side window check
   is not that cap and does not imply it. v0.8.x images do still migrate at
   startup, and this decision defends against the behaviour that actually ships.
7. **Always `helm uninstall` before the gVisor-off retry.** Rejected: a history
   with a known-good revision is a real install, and uninstalling it would turn a
   recoverable retry into a data-bearing teardown.
8. **Have a fresh install clear the quiesce flag at boot.** Weighed in #2374,
   which offered it as an alternative or a complement to keying the flag by
   installation. Not taken as the mechanism: clearing on boot repairs the symptom
   after a fresh install has already been paused, and it gives a new installation
   authority to erase a marker a *different* live installation wrote. Scoping the
   key removes the collision instead of recovering from it.
9. **Guard the pre-upgrade drain hook against a missing `<release>-secrets`.**
   Named in #2347 as "a second, slightly larger change" and deliberately deferred.
   The hook has a hard dependency on a Secret that any first-revision failure can
   leave absent, so it can still wedge a release whose first attempt dies for a
   reason other than gVisor. Discarding the never-deployed revision closes the
   reported loop; hardening the hook remains open.

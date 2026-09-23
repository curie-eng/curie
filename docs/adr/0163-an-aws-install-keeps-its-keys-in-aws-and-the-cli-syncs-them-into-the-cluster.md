# 163. An AWS install keeps its credentials in Secrets Manager and the CLI syncs them in with External Secrets

Date: 2026-09-22

Status: Accepted

Accepted 2026-09-22 by maintainer Brian Conn with explicit approval, under the
coordinated exception of [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md):
it lands alongside its implementation on the `epic/aws-secrets` integration
branch. Tracking issue: #2956. Realizing code path: `cli/src/secrets*`,
`cli/src/installation.rs`, `cli/src/ops/up.rs`, `cli/src/connectors.rs`,
`cli/src/cluster_secrets.rs`, `cli/src/comms.rs`, `cli/src/github_app.rs`, and
the `existingSecret` knobs in `charts/curie/templates/`.

Builds on [ADR 0015](0015-credential-plane.md) (no credential broker in the
app) and [ADR 0097](0097-one-file-declares-an-installation.md).

## Context

A cluster install cannot be rebuilt from git. The chart generates its own credentials and reads them back with `lookup`; every other credential arrives by a side path and then exists only in the cluster. On one EKS install, 14 of 22 Secrets were hand-made and existed nowhere else, and recollecting them from channel admins, tenant IT and a finance realm owner was estimated at 3 to 7 elapsed days.

The CLI makes it worse in a way no operator sees: every credential it passes as a Helm value (model key, Slack tokens, GitHub App key, sealing key, generated database passwords, a second copy of each connector secret) is persisted in up to ten `sh.helm.release.v1.*` revisions, and `cluster up` treats `helm get values` as its store of record. `cluster comms --slack` blanks bring-your-own references. `cluster deploy` writes each connector credential three times.

ADR-0015 rejects a credential broker in the app. ADR-0097 makes `curie.yaml` the install's whole intent, names only.

## Decision

1. **Provider is declared, optional.** `curie.yaml` gains `secrets: {provider: aws, region, prefix, role_arn}`. Absent means today's behaviour (in-cluster Secrets, local store). The provider path runs through the declarative `curie apply` (ADR-0097) and the standalone `curie secrets` commands, both of which read curie.yaml; `curie cluster up` is not extended. No flag selects the provider.
2. **Secrets Manager is the source of truth for everything a rebuild cannot regenerate on its own**: every third-party credential, and every generated value bound to persisted data or another party (installationId, backing-store passwords, Langfuse salt and encryption key, sealing keys, GitHub webhook secret). Values that are regenerated together with all their consumers (apiKey, approval attester secret, internal worker token) and tokens Curie mints itself (mail channel token, Grafana connector token) stay in-cluster and are re-minted on rebuild.
3. **External Secrets Operator syncs, the CLI owns the objects.** When the provider is set, `curie apply` installs a pinned ESO (or reuses one already present), a namespaced `SecretStore` using web-identity auth for a dedicated ServiceAccount annotated with `role_arn`, and one `ExternalSecret` per SM-backed Secret. ESO writes NEW Secret names for platform credentials; the chart's per-component `existingSecret` pairs point at them. ESO never adopts the chart's own Secret. Hosted connector Secrets keep their API-declared name `<release>-<agent>-connector-secrets`, because the API renders and the worker reconciles consumers against it; ESO creates that Secret and the CLI stops applying it. `curie apply` refuses, without mutation, when a same-named Secret exists that ESO does not own, naming it. ESO is reused only when an existing installation matches the pinned minor version and exposes the required CRD versions (ExternalSecret v1, PushSecret v1alpha1) and watch scope; otherwise apply refuses without touching it, and teardown never removes a controller it did not install. The CLI applies these objects with its own field manager, as it already does for connector Secrets; the chart renders no ESO objects.
4. **A declared inventory is the contract.** A checked-in platform inventory lists each credential: logical name, class, target Secret and keys, consumers (workloads to roll), rotation owner, and update policy (`replace`, or `immutable` for values bound to persisted state such as the Postgres password and the Langfuse encryption key, which `curie secrets set` refuses to replace). Bundles extend it by name only: a connector's secret declaration may mark a key `rotation: workload`, and the CLI merges platform and bundle entries into the install's inventory, with the same completeness check at deploy. CI fails when the rendered chart references a Secret key the inventory does not list, or when an ESO-managed key is rotation-owned. The CLI's preflight refuses an apply that would reference a missing SM entry or key, printing names and key lengths only.
5. **No provider-backed value enters Helm** when the provider is set. The CLI sets `existingSecret` references instead of values and bounds release history. The chart-generated throwaways of decision 2 (apiKey, attester secret, internal worker token) remain in the chart's own Secret and so in Helm's rendered manifests; that exception is explicit.
6. **The CLI writes to the provider.** `curie secrets set/list/check/rm` work standalone against any install with a provider: set writes SM (aws CLI v2, value over stdin or a 0600 file, never argv). When the install is not yet provisioned (no namespace or SecretStore) it stops there and says so, and `curie apply` materializes it later. Otherwise it ensures the ExternalSecret, forces a sync, and rolls exactly the inventory's consumers, stamping each consumer's pod template with the provider version it now carries, so `curie secrets check` can report a consumer still running an older version even after ESO has caught up. `cluster deploy` connector secrets, `cluster comms`, and `cluster github-app` write SM instead of the cluster. Cluster-scoped credentials go to SM; `~/.config/curie/credentials.json` keeps serving skill and local tiers.
7. **Third-party rotators keep SM current without code changes.** A key a workload rotates in place (a finance refresh token) is declared `rotation owner: workload:<name>`. Its ExternalSecret (`creationPolicy: CreateOrMerge`) owns only the static sibling keys. A PushSecret (`deletionPolicy: None`, JSON property) backs the rotated key up to `<prefix>/<name>-rotated` every 10 seconds. On apply, before either object, the CLI seeds the rotated key from that backup at key granularity: a conditional write (resourceVersion precondition) that adds the key only when absent, preserving static siblings and any live rotated value; it skips the seed when no backup exists yet so the connector's own bootstrap creates it. ESO is pinned; the rotation proof re-runs on every ESO upgrade because this behaviour is version-specific. A cluster lost inside one backup interval keeps a revoked token; that window is accepted.
8. **Expiry lives with the value.** `curie secrets set --expires` writes a `curie:expires-at` tag; `curie secrets check` warns 30 days out and fails once expired.

## Consequences

- A rebuild is git plus a populated SM plus `curie apply`; no third party.
- A new install dependency (ESO) on AWS installs, installed and pinned by the CLI.
- Consumers still read env vars; a value changed directly in the AWS console reaches pods at the next rollout. `curie secrets check` reports consumers whose stamped version is older than the provider's.
- Replacing an `immutable` credential is a coordinated rotation this ADR does not provide; the CLI refuses it.
- Moving an existing install is documented, not automated; its old Helm revisions still hold plaintext until the operator trims them.

## Alternatives rejected

- Secrets Store CSI driver: keeps a synced Secret only while a pod mounts it; consumers read env vars.
- App fetches from SM at runtime: a credential broker, rejected by ADR-0015, and AWS inside every service.
- Chart renders ExternalSecret objects: needs ESO CRDs at render, grows the release Secret, and makes standalone `curie secrets` need a helm upgrade.
- ESO adopts the chart's `<fullname>-secrets`: Helm `lookup` and ESO would both write it.
- PushSecret alone for rotated keys, and ExternalSecret `Owner` over rotated keys: ESO reverts the workload's write within seconds.
- An ESO-only seed (a second ExternalSecret with CreatedOnce, CreateOrMerge at interval 0, or Orphan): each either reverted a rotated value in testing or wiped the static keys while syncing.
- aws-sdk-rust in the CLI: +12 MB and three times the release build for what the aws CLI already does.
- Reloader for restarts: another controller that can restart the worker mid-turn.
- An import/migration command: only one install would use it.


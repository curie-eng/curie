# 239. Explicit shared connector applications resolve credentials for each authenticated caller

Date: 2026-10-20

Status: Accepted

Accepted on 2026-10-20 with explicit maintainer approval. Acceptance covers
this decision and the scoped shared-custody clarification in Draft ADR 0158;
that ADR otherwise remains Draft. Publication and green merge authority are
limited to the shared-connectors epic branch. Integration into `next` remains
subject to human review and merge.

Realization starts in `packages/plugin-format/src/plugin_format/connector_render.py`,
`apps/worker/src/curie_connector_proxy/caller.py` and
`apps/worker/src/curie_connector_proxy/server.py`,
`apps/worker/src/curie_worker/connector_reconcile.py`, and the caller mint in
`apps/worker/src/curie_worker/kernel/claim.py`. The latter has a separate narrow
grant for caller identity, expiry and deadline propagation, with one owner and
specification-versus-implementation and side-effects reviews. Upgrade realization
extends `apps/worker/src/curie_worker/upgrade_drain.py` and the existing chart
upgrade hooks. Acceptance is implementation authority, not a claim that these
paths already implement shared applications.

## Context

1. One application instance per installation, compatible connector type, locked server
   digest, protocol version, provider configuration and effective egress policy. Sharing
   is explicit and only within one company. Existing bundles remain dedicated until
   migrated. Adapted connector code dynamically obtains credentials; no per-agent
   backend process is part of this design.

2. All connector categories have a platform path: static keys, multi-field/file-shaped
   credentials, imported OAuth grants, and custom connectors retaining their own
   credential custody. Every implementation must adopt request-local identity and
   authorization. Supporting the platform contract does not make every existing binary
   safe to share unchanged.

3. The trusted shared application may hold several employees' credentials. A compromise
   can affect its member accounts. The platform prevents an agent from choosing another
   binding through tool arguments; it cannot isolate credentials from compromised shared
   application code. Applications with different egress/security requirements do not
   pool together.

## Decision

### Bundle, identity and request contract

1. New exclusive bundle form: `shared: {application: <immutable UUID>, contract: 1}`. It
   excludes image/build/url, secrets/secret_files, provider account, endpoint and
   credential source overrides. Alias and tools may only narrow the operator-approved
   application policy. Operator application records pin digest, declared slots, origins,
   policy and, when the application emits undoable records, a separate connector-owned
   snapshot-sealing Secret reference. No arbitrary bundle-provided resolver URL.

2. Caller token v2 is `cct2.<base64url canonical JSON>.<base64url Ed25519 signature>`;
   canonical JSON uses sorted keys and no whitespace, signatures cover prefix and
   payload. Exact mandatory fields are installation(string), agent_id(UUID),
   agent(display name), exp(integer); optional run/work_item are paired canonical UUIDs.
   Unknown fields, duplicate headers and unsupported prefix refuse; only exp strictly
   greater than current time is valid. Installation-level signing/public-key
   distribution remains the existing trusted worker/operator mechanism, with no new
   per-agent signing keys and no signer material in any connector Pod. Caller token v2
   uses the signed opaque installation ID already persisted by the chart, immutable
   agent UUID, display name, expiry and paired run/work_item where present. Do not
   coerce the installation ID to UUID: fresh chart installs generate 32 alphanumeric
   characters and legacy upgrades can adopt a Secret UID. Compare the exact configured
   value, bound it to 128 UTF-8 bytes and reject control/whitespace characters. Shared
   mode is initially cluster-only; the API refuses shared bundles/registration with a
   blank or unobserved installation identity. Dedicated standalone execution explicitly
   permits a blank installation claim, while shared execution always rejects it. Worker
   derives identity from its resolved deployment, never the prompt. Action execution
   requires run and work_item. Legacy/name-only caller tokens cannot authorize shared
   execution. Ed25519 signing material remains exclusively outside the connector Pod.

3. The existing proxy verifies tokens and policy, removes raw cct, approval tokens,
   spoofed identity headers and incoming resolver handles, then forwards a fresh 256-bit
   random invocation handle. Context binds installation/application/agent/run/work
   item/alias/policy generation. Declared credential slots cannot select agent, account
   or source.

4. HTTP over a pod-local Unix socket is the language-neutral resolver protocol. `POST
   /v1/resolve` accepts invocation handle, declared slot and purpose. It returns
   binding/account/revision/origins/expiry plus credential fields, or identity-only
   admission for connector-managed custody. Purpose is audit metadata and never grants
   authority. Unknown slots, missing or foreign handles and revoked contexts refuse
   before a provider request.

5. Resolve transaction commit is the admission boundary. Each provider API call requires
   a new resolve. No lease or cached client can authorize the next call. The reference
   SDK is one dependency-free Python 3.12-compatible source file. Adopters vendor it
   into their own Docker build directories and exercise each copy against the same
   executable wire corpus; they never install the Python 3.13 worker package. The
   canonical file and corpus live with the resolver in this repository; each vendored
   copy is owned by its adopter. Other languages implement the wire directly. The SDK builds request-local clients, restricts declared origins,
   refuses credential forwarding on redirects, and partitions auth/cache state by
   binding/account/revision. A trusted connector is responsible for obeying this
   contract.

6. Handle lives for its invocation and signed caller expiry, is removed on
   completion/cancellation, and cannot be used to authorize a later invocation. No new
   short tool timeout is imposed. Initial bounds: 1024 active invocations, 4096 sessions
   and 64KiB resolver requests per application; overflow refuses without allocation.
   Transport SSE does not itself create a provider-call authority. Sessions,
   resumptions, cancellation and notifications bind principal/application/policy
   generation; a session ID is never identity.

7. Shared membership, alias narrowing and policy generation live in Postgres and are
   read authoritatively on each call. They are never rendered into shared Pod
   environment. Member add/remove changes NetworkPolicy reach but leaves the shared
   Deployment template unchanged, preserving other agents' sessions/handles. Action
   executor reads shared policy from the same authoritative database source rather than
   Deployment env; dedicated behavior remains as declared. Application-global canonical
   tool policy controls every alias. The explicitly selected shared approval-grant wire
   binds agent binding, application, canonical tool, alias, argument digest and policy
   generation; shared invocations never accept the dedicated approval wire. Dedicated
   grants retain their existing semantics. Shared grants remain single-use in real
   Valkey, and cannot be laundered through a second alias. Refresh state lives in
   Postgres, not Valkey.

### Custody and least privilege

1. Postgres stores immutable applications, bindings, provider-account identity, status,
   revisions, source metadata, ciphertext and durable refresh intents. The CLI seals
   imported credential material to an application public key before API transport. API
   metadata, agent/runner environments, traces and ledgers never receive provider
   plaintext. Binary/file-shaped fields are declared bytes and provided through the same
   local API, not agent-visible Secret mounts.

2. The existing proxy container also runs the resolver. Only it mounts the application
   credential private key and per-application database DSN. Server does not mount
   either, the caller signer or a service-account token. When required, the
   snapshot-sealing Secret is mounted only into the server container. The proxy/resolver
   never mounts or reads it, and it is never a resolver credential slot. Pod has
   `automountServiceAccountToken: false`, `shareProcessNamespace: false`, non-root UID,
   dropped capabilities, read-only roots and no host paths. The Unix socket uses a
   dedicated emptyDir.

3. NetworkPolicy selects Pods, so both containers gain database network reach. The
   data-tier chart explicitly admits bundled Postgres traffic only from the new
   shared-application label and release ownership, never all mcp-connector Pods.
   Matching egress permits only configured database host/CIDRs/port, including managed
   external Postgres; caller supplies resolved managed endpoint network constraints.
   Qualification must cover this narrow exception and refuse generic connectors and
   namespace peers. Authentication remains required. Every application has a distinct
   non-superuser database role with role-derived row access and limited column updates.
   Binding/application status rows grant SELECT plus UPDATE only on a non-authority
   lock_guard column, with matching role-derived UPDATE policy so FOR SHARE/KEY SHARE
   can lock the same authorized rows. Resolver cannot update enabled, ownership, account
   or policy columns; disable uses the conflicting lock. Real-Postgres tests prove both
   lock access and denied authority mutation. A client-set application GUC is not
   authorization. Application A's real role must fail reading or changing B's rows. No
   resolver uses the API/worker DSN.

4. Bundled Postgres: the CLI calls an operator-authenticated API provisioning endpoint;
   API uses its existing installation connection to create the role with a generated
   password, verifies its restrictions, then seals the DSN to the registered application
   public key before storing/returning it. No CLI direct Postgres channel is introduced.
   Names use a bounded hash of exact installation/application identity to avoid
   cross-release collisions within PostgreSQL's 63-byte name bound. API without required
   role-admin privilege refuses explicitly. Worker writes/adopts the encrypted DSN
   Secret through the same restricted create-once mechanism as keys; resolver decrypts
   it locally before connecting. Managed mode accepts an operator-preexisting DSN Secret
   reference, mounted only in resolver. Resolver tests its actual privileges before
   activation without CREATEROLE, and worker reports safe role/readiness status to API;
   API receives only the reference/status, needs no Secret-reading RBAC and has no
   direct managed resolver connection. The server has no direct access. Qualification
   provisions and verifies both paths on disposable Postgres; no managed-vendor
   deployment is claimed tested.

5. Worker/applier uses atomic Secret create; on conflict it lists the exact labelled
   Secret and validates/adopts it. It never forced-applies keys. Only public material is
   registered by a worker-authenticated key-registration endpoint. Reject mismatched
   application/recipient IDs, malformed key objects and failed readback. Existing
   operator Secrets remain untouched. Current worker Secret admission admits only
   labelled per-claim -tokens objects, so add separate narrowly typed application-key
   and encrypted-DSN allowances with exact application ownership, stable name, Opaque
   type and permitted key fields. Do not disguise a shared key as a sandbox token. Deny
   unrelated key/DSN shapes and service-account-token types. Create-once convergence is
   not a promise that the privileged worker can never patch/delete keys; retention and
   deletion safeguards are separately tested.

6. Credential encryption keys and connector-held snapshot sealing keys are separate.
   Retain previous decrypt recipients, reseal all credential rows with CAS, verify zero
   references, then retire. Retain old snapshot keys while their records remain
   undoable. Encryption keys cannot mint Ed25519 caller tokens.

7. Initial proxy/resolver requests: 25m CPU and 128Mi memory; limits: 1000m CPU and
   512Mi memory, configurable by operator policy. Server resources remain
   application-specific. Qualification records idle requests and observed peak, with no
   claim that unmeasured peak fits today's 128Mi proxy limit.

### Static sources, OAuth and revocation

1. Static import records source UID/resourceVersion and a keyed revision fingerprint,
   never plaintext. Reimport atomically advances credential revision; stale writers
   refuse. Newly admitted calls use the new revision. `curie connectors binding check`
   compares source versions and reports `source_stale` without printing material. At
   import/check/apply, mismatch requires explicit reimport. There is no claim of
   continuous Secret watching or automatic ESO-to-ciphertext synchronization.

2. Preserve existing dedicated credential source references,
   `<release>-<agent>-connector-secrets` naming and provider-absent behavior. Shared
   imports are explicit consumers of those sources, never a competing owner. Imported
   OAuth grants are resolver-owned after source-writer shutdown; copying a grant does
   not establish safe co-ownership.

3. OAuth matrix: refresh_token grant; HTTPS allowlisted token endpoint;
   client_secret_basic and client_secret_post; per-application client material; rotating
   and nonrotating refresh tokens. Unsupported auth modes and cross-origin redirects
   refuse. Provider-specific consent, registration and nonstandard adapters are later
   adopters.

4. Grant ID is unique within installation and bound to one application, provider account
   and client configuration. Aliases share the same record; independently copied grants
   cannot be deduplicated. Durable ready/refreshing/recovery_required state, intent ID
   and CAS/epoch fencing serialize refresh. Persist replacement before releasing the
   access token.

5. Expired ownership does not prove no provider call occurred. Uncertain refresh outcome
   enters recovery_required, cannot be stolen or automatically replayed, and has
   explicit operator reimport/reconciliation. A crash after provider consumption but
   before commit is recoverable by this procedure, not magically lossless. Test both
   sides of the provider/commit boundary.

6. Each resolve locks/checks current application and binding status. Disable takes the
   conflicting lock and acknowledges after commit. Resolves committed after that
   acknowledgement refuse. Previously admitted effects can finish. Database/resolver/key
   outage fails closed. New calls cannot use cached authorization. Custom credential
   holders use the same authoritative identity-only admission before accessing their own
   binding-indexed store.

### Restore, ownership and rollout

1. Action attribution and sealed snapshots bind immutable application/binding/account,
   server image digest, canonical tool, target identity/version and policy generation.
   Same-account token rotation preserves binding; account replacement creates a new
   binding. Disabled binding refuses new actions and restore. Restore cannot expand
   historical authority beyond current policy.

2. Connector holds snapshot keys. Platform stores only opaque envelopes and version
   tokens. Refuse foreign binding/account, wrong digest, stale version, missing key and
   substituted ciphertext before provider writes. Identity-only custom custody mode
   follows identical rules.

3. Shared Deployment/Service/NetworkPolicies use application ownership, distinct from
   agent prune labels. Each member's ingress/egress selection is derived from
   membership, never broadened to all runners. Remove-one preserves survivor, operator
   Secrets and undo history. API remains a pure renderer; worker and CLI use the same
   frozen rendering functions.

4. Caller v2 rollout extends the existing upgrade_drain.py and Helm
   drain/attest/schema/hold/release sequence under Accepted ADR 0209; ADR 0131 remains
   context for delivery ownership/shutdown. Do not create another pause authority or
   edit consumer.py/markers.py for this. While the existing hold is valid, drain leased
   work, re-render and replace every hosted caller proxy and idle/prewarmed sandbox,
   prove protocol/readiness, then release. No v2 minter may run while a reachable
   v1-only proxy can receive its calls. Current mandatory CI does not retain a baseline
   hosted proxy and prove an authenticated call after upgrade. Fresh candidate callers
   may develop only on an isolated epic branch. A retained-baseline authenticated
   upgrade driver and completed rollout are prerequisites for integration into a release
   train. Caller v2 cannot ship independently. No legacy token-format fallback.

5. The convergence hook runs post-upgrade at weight -11 before existing release at -10,
   with activeDeadlineSeconds 300 and a deadline no later than the original roll-hold
   expiry. It is renew-only for the existing installation/revision, never
   creates/recreates authority, renews at most every 100 seconds to min(300 seconds,
   remaining original roll-hold cap), and refuses missing/foreign/expired authority.
   Only the existing release step clears success authority. Pre-mutation drain refusal
   follows existing clear/refuse behavior. Convergence failure marks the upgrade failed
   and stops renewal; the fleet resumes within 300 seconds of last renewal and no later
   than the original cap. Incompatible old caller/route generations refuse without
   upstream effects after expiry. A retry/recovery must establish one matching
   generation before reporting healthy; rollback is not claimed successful when state is
   unreadable. No indefinite pause or renewed authority after hook death. These bounds
   preserve ADR 0209 clauses 2 to 6 and require no supersession. Test timeout, lost
   readiness, killed hook, lease expiry and partial recovery explicitly.

6. Per-binding dedicated-to-shared cutover drains that binding, fences its old
   credential/refresh holder, waits for actual new readiness, switches route, then
   prunes old objects. Reverse cutover is equally fenced. Refuse continuous-holder
   migration without a verifiable stop/export contract. Static source and snapshot
   retention remain explicit. Worker and CLI may not prune before readiness.


## Relationship to earlier decisions

1. Partially supersedes [ADR
   0087](0087-the-api-renders-connector-objects-the-cli-applies-them.md) for explicit
   shared application ownership. Dedicated agent-scoped objects remain valid. Extends
   [ADR 0086](0086-bundles-declare-connectors-the-platform-hosts-them.md) and [ADR
   0090](0090-a-reconciler-applies-connectors-so-agent-repos-need-no-cli.md) without
   moving rendering or apply authority.

2. Partially supersedes [ADR 0178](0178-the-connector-caller-token-names-the-run-and-work-item.md)
   decision 4's prefix and exact claim set, decision 1's name-only admission for
   shared mode, and decision 5's shared-server handoff. The paired claims and
   deadline bound stand. Partially supersedes ADR 0168 decision 7 for shared mode:
   Postgres membership keyed by immutable agent ID replaces the bundle-rendered
   `admits:` agent-name list. Its refusal semantics stand. Draft ADR 0141 is context,
   not evidence of installed enforcement.

3. Preserves ADR 0008's company boundary and ADR 0009's credential non-disclosure.
   Extends [ADR 0094](0094-a-bundle-carries-its-own-sealed-connector-keys.md) with a
   separate application import recipient; unsupported bundle sealed-secret delivery is
   not claimed to work.

4. Preserves ADR 0121's pinned connector restoration and ADR 0124's connector-only
   snapshot key custody. The scoped clarification in [Draft ADR
   0158](0158-a-custom-connector-is-a-bundle-built-http-mcp-server-that-holds-its-own-credential.md)
   permits custom private custody under this admission contract, without accepting its
   other proposals.

5. Extends [Accepted ADR
   0209](0209-pause-authority-after-a-drain-is-a-lease-held-by-a-live-upgrade-step.md)
   with bounded, renew-only convergence. ADR 0131 remains delivery context. Draft ADRs
   0144 and 0230 remain Draft; their implemented interfaces are preserved without
   implying acceptance.

6. Preserves ADR 0194's Python and Rust reader parity. The standalone frozen-format
   change includes shared input vectors and generated mirrors before dependent
   activation. Plugin-format has no independent protocol version today; an unchanged ACI
   wire earns no ACI version bump.

## Consequences

1. Idle deployments scale with compatible applications rather than agent count. Adapted
   code can serve all supported credential categories, while existing dedicated binaries
   retain their current mode. One replica is the initial supported topology;
   multi-replica coordination is later work.

2. The application is a larger trust boundary. A compromised shared server can obtain
   its members' admitted credentials. Explicit enablement, least privilege, narrow
   reach, current per-call authorization and safe attribution reduce exposure but cannot
   make malicious shared application code isolate its own memory.

3. Every provider call depends on current resolver and database availability. Outages
   refuse; cached authorization cannot preserve availability. The SDK and
   language-neutral protocol make that cost explicit to connector authors.

4. Imported rotating grants need durable intent and explicit uncertain-outcome recovery.
   Provider-specific consent screens and registration, arbitrary unchanged
   single-credential binary sharing, real-provider acceptance and production migration
   are outside this delivery.

5. Qualification must prove two agents share one server process, forged identity and
   cross-account calls refuse, disable and rotation preserve the specified boundary,
   custom custody and restore stay sealed, and forward/reverse migration fences old
   holders. Automated provider fixtures stand in for external providers; internal
   stores, deployed RBAC/admission and caller wiring remain real.

## Alternatives rejected

1. Mount every agent's Secret into one unadapted server. Startup credentials and global
   clients cannot select a request's identity safely, and membership changes require Pod
   churn.

2. A shared gateway with a dedicated backend process for every agent. This retains
   existing binaries but does not meet the selected single-application,
   dynamic-credential destination.

3. Trust a tool argument, alias or session ID to select an account. Each is
   attacker-controlled or reusable and cannot establish the authenticated binding.

4. Cache authorization until a credential expires. Disabled bindings could keep making
   newly admitted calls after disable acknowledgement.

5. Give the shared server the installation database role, signer or service-account
   token. Those grant authority beyond the application's credential and membership
   boundary.
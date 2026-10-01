# 185. Enterprise plugins are licensed services behind open ports

Date: 2026-10-01

Status: Draft

Extends [ADR-0096](0096-port-adapters-are-deployed-services.md), whose
deployed-service shape every enterprise plugin takes, and **amends ADR-0096
decision 3 in part** for the plugin ports this ADR opens: their install surface
is a chart values block until a second plugin exists, rather than an entry in
the adapter-manifest schema. It reuses the scoped principal shape of
[ADR-0156](0156-adapter-principal-with-a-scoped-credential.md). It supersedes no
Accepted ADR.

## Context

CurieTech will sell enterprise capabilities on top of open source Curie. The
first is cost analytics for the dark factory: what an issue cost, broken into
planning, plan review, implementation and implementation review, rolled up
across issues, repositories and time. Identity hardening follows later. Three
constraints come from the business, not from the code:

1. **No fork.** A customer runs released open source Curie and adds paid
   capability to it. CurieTech does not maintain an enterprise build of the
   platform.
2. **Enterprise source does not ship.** Enterprise code is compiled, and Rust
   is the language, matching the CLI toolchain. Open source stays Python.
3. **Open source keeps what it already has.** Nothing that ships today is
   pulled back into the paid tier.

The platform already answers where third party code goes. ADR-0096 decision 2
makes a platform adapter a deployed service speaking a versioned wire contract,
composed by deployment config rather than loaded into a Python process, and
decision 5 promotes ports one at a time, on demand. An enterprise plugin is a
platform adapter written by CurieTech instead of a vendor, so the shape is
settled. What is not settled is which ports exist, what crosses them, how a
license gates them, and how the open source UI shows something it cannot
compute.

What exists today:

1. **Raw usage is already open source.** Issue #3223 shipped token usage per
   execution request, turn, model and role (implementer or reviewer), an
   estimated cost priced from the provider's published per token rates, a per
   work item rollup across retries and CI fix rounds, `GET
   /work-items/{id}/usage`, and a usage line on the factory's terminal issue
   comment (`apps/api/src/curie_api/factory_usage.py`). The runner reports it
   once per turn, at turn end.
2. **Usage has no phase.** A factory turn spans every declared phase, and the
   usage rows carry no phase or loop round, so tokens cannot be attributed to
   planning versus implementation, or to the first review round versus the
   third. Phases themselves are durable (`ExecutionRequestPhaseReport` with
   phase and loop round) and declared per run, with loops pairing a start
   phase and a review phase (`apps/api/src/curie_api/factory_progress.py`).
3. **Token counts also leave on OpenTelemetry spans**, and Langfuse prices them
   itself; the Metrics tab shows Langfuse's own cost aggregate
   (`apps/api/src/curie_api/metrics.py`).
4. **No port carries usage out, no plugin reports its health or license, and
   the UI has no notion of a plugin.** Only `aci-protocol` and
   `channel-protocol` exist as published wire contracts.

## Decision

**1. An enterprise plugin is a platform adapter under ADR-0096.** It is a
compiled service, written in Rust, distributed as a container image from a
per customer private registry, and deployed beside the platform. Open source
never imports, links or loads it. In process extension (a PyO3 module) is
allowed only for a pure hot path function under ADR-0096 decision 4, never as
the mechanism a plugin uses to attach. WebAssembly is not used.

**2. Contracts belong to ports, not to plugins.** There are two layers, and
both are open source:

1. A **plugin lifecycle contract** every plugin implements identically:
   health, the contract versions it serves, its capabilities (which ports it
   implements), and its license state. The platform discovers, health checks
   and reports on every plugin through this one contract, without knowing any
   plugin by name.
2. A **port contract** per seam the platform opens. A second plugin serving the
   same port, including one a customer writes, implements the same contract.
   The number of contracts grows with the seams open source opens, not with the
   plugins CurieTech sells.

Both live in a new contract package, `packages/plugin-protocol`, published as
OpenAPI 3 over HTTP and JSON, versioned and drift gated like `aci-protocol` and
`channel-protocol`, with a conformance kit a plugin runs against itself. The
port says what would plug in if a customer licensed it; the value CurieTech
sells is the implementation behind it and the license that unlocks it. HTTP is
used until a port needs streaming.

**3. The first port is usage ingest, and its records are tagged, not
bucketed.** The platform sends usage records to every plugin that declares the
usage capability. A record carries the token counts and model that #3223
already stores, the provider's reported cost when the provider gives one
(OpenRouter's `usage.cost`), an idempotency key, and an open set of string
tags: at least `work_item`, `execution_request`, `repo`, `agent`, `role`,
`phase` and `loop_round`. The contract defines no cost buckets. Grouping
phases into buckets is plugin configuration, shipped with a default mapping to
planning, plan review, implementation and implementation review, and changeable
without a contract version. The default mapping goes by phase, not by role:
every token spent while a review phase is current counts as review, and when a
review sends work back, the fixes made in the following implementation or
planning round count as implementation or planning. Tokens with no phase are
reported in an unattributed bucket. Adding a tag is a minor version; removing or
retyping one is a major version.

This needs one open source change below the port: usage must be attributed to
the phase and loop round that were current when the tokens were spent, rather
than summed once per turn. The runner already sees each assistant message's
usage, model and subagent marker as the turn streams. It keeps a phase cursor
(phase, the round as reported, and a per turn phase sequence) and advances it
when it observes the successful result of the main agent's `report_progress`
call, not inside the tool handler, because the SDK runs tool handlers ahead of
the message stream. It tags each per message usage delta, deduplicated by
message id, with the current cursor, and at turn end scales the tagged deltas
per model so they sum exactly to the SDK's reported totals, which stay
authoritative. The report is still sent once per turn. The #3223 rows gain the
phase, round and phase sequence through an additive migration, with the phase
sequence joining the unique key. Tokens spent before the first progress report,
and every token of a turn with no factory declaration, carry no phase and are
reported as unattributed. The API change ships no later than the runner change,
because the usage report rejects unknown fields. The attribution lands in the
#3223 data, so the free tier gains phase level raw usage and the port carries
nothing open source does not also store. Kickback counts keep coming from the
phase reports themselves, not from usage rows.

**4. Ingest fails open and stores nothing new.** A slow, failing or absent
plugin never delays or fails a turn or a factory run. Delivery is best effort
with a short bounded in memory retry; the platform keeps no outbox or queue for
plugins. A plugin that missed records backfills them by reading the #3223
usage data under decision 7's scoped credential, keyed on the same
idempotency keys, so a plugin restart leaves no permanent gap.

**5. Reads go through the platform API, and the plugin enforces its own
license.** The UI and CLI never call a plugin directly. A read proxy on the
API forwards a plugin's read endpoints under the caller's existing Curie
authentication. The platform exposes one plugin status read, built from each
plugin's lifecycle contract and cached briefly, listing per plugin whether it
is installed, healthy and licensed and what it can do. The plugin, not the
platform, decides whether to serve a read: an unlicensed or expired plugin
answers 403 with a machine readable reason (`feature_not_licensed`,
`license_expired`), which the proxy passes through. The proxy answers 503 when
the plugin is unreachable and 404 when none is installed. A gate in open
source would be one patch away from removal, so the platform's view of a
license is display only.

**6. The open source UI contains every plugin view.** The rendering code for
paid views, starting with a cost analytics tab, ships in the open source UI.
The tab's state comes from the plugin status read: the live view when the
plugin is licensed and healthy, a license expired or unavailable state when
it is not, and a quiet one page description of the enterprise capability when
no plugin is installed. Nothing is shown elsewhere in the product to advertise
it. A plugin does not ship frontend code and the UI has no runtime extension
point.

**7. A plugin is a principal with a scoped credential, and holds only its
own.** Following ADR-0156, `PrincipalKind` gains `plugin`. A plugin's
credential carries only the scopes its declared ports need, starting with
`usage:read` for decision 4's backfill. It expires, is issued
administratively, and is named in audit rows. A plugin never receives the
platform API key or any model credential. The platform authenticates to a
plugin's endpoints with a per plugin token mounted from a Secret.

**8. Entitlement lives inside the plugin.** A license is an offline signed
file (Ed25519) naming the customer, an expiry, a grace period and a set of
feature flags, verified against public keys compiled into the plugin through a
shared Rust crate. Feature flags let plugins sell separately; the cost plugin's
flag is `cost-analytics`. There is no phone home, so an air gapped install
works. After expiry a 30 day grace period keeps everything working while the
plugin reports the state loudly through decision 5's status read. After grace
the plugin stops serving reads but keeps accepting ingest, so renewing
restores the full history. Expiry never touches open source behavior or the
#3223 data. Seat, agent and usage limits are not enforced in this version.

**9. Install is a chart values block.** A `plugins:` block in the Helm values
names each plugin's image, its license Secret and whether it is enabled; the
chart renders the Deployment, Service, NetworkPolicy and token Secrets.
`curie cluster status` reports each plugin's health and license state. This is
the amendment to ADR-0096 decision 3: the adapter-manifest schema entry and the
`curie adapter` verbs it calls for are deferred for plugin ports until a second
plugin shows what a manifest must generalize. Designing it from one example
would be the speculative framework ADR-0096 decision 5 forbids.

**10. The free tier and the paid tier are split by aggregation, not by data.**
Open source keeps per request raw usage, its estimated cost from public
prices, the per work item total, OpenTelemetry token attributes and Langfuse.
A user who wants to build their own analysis on that data may. The cost plugin
sells the layer above: cost by bucket, customer price overrides for negotiated
rates, rollups across issues, repositories, teams and time, graphs, and later
rework analysis (how often plans and implementations are sent back, and what
that costs), budgets and export. All enterprise pricing and aggregation logic
lives in the plugin.

## Alternatives considered and rejected

1. **An enterprise directory in the open source tree, gated by license checks
   in Python.** The open core model. Rejected because it ships the enterprise
   source and puts the gate in patchable code, against both business
   constraints.
2. **A maintained enterprise fork.** Rejected by the business outright; it also
   doubles every release.
3. **In process plugins through PyO3, or WebAssembly.** PyO3 works and produces
   a small wheel, but a panic takes the worker down, it raises the glibc floor
   of the service image, and it is the import ADR-0096 decision 2 rules out.
   WebAssembly has no advantage here that pays for its toolchain. Both were
   prototyped or assessed before this record.
4. **A contract per plugin.** Rejected because it makes every plugin a new
   integration for the platform and prevents a second implementation of the
   same port.
5. **Buckets in the contract.** Rejected because buckets will change, and
   customers will eventually define their own. Tags keep the contract stable.
6. **gRPC.** Rejected for now for the reason ADR-0096 rejected a bespoke RPC
   protocol: the platform already composes over versioned HTTP contracts.
   Revisit when a port needs streaming.
7. **A durable outbox for plugin delivery.** Rejected because the platform
   would store data on a plugin's behalf; backfill from the #3223 data closes
   the same gap.
8. **Plugin shipped UI loaded at runtime.** Rejected for now as more machinery
   than one plugin justifies: a frontend extension SDK, version skew between
   host and plugin bundles, and a security boundary in the browser. It becomes
   worth revisiting when third parties want their own views.
9. **Removing usage, cost or token telemetry from open source** to make the
   paid tier necessary. Rejected: it would take shipped capability away from
   users. The paid tier earns its price through aggregation.
10. **Go instead of Rust.** Acceptable to the business, but a stripped Go
    binary keeps more type information than a stripped Rust one, and Rust
    matches the CLI and the shared license crate.

## Consequences

**This ADR authorizes no code while it is a Draft.** Once Accepted, it
authorizes the plugin protocol package, phase attributed usage, the ingest
emitter, the plugin status read and read proxy, the plugin principal, the
chart `plugins:` block, the cost analytics tab in the UI, and status in `curie
cluster status`. The plugin itself lives outside this repository.

**Compiling protects the source, not the protocol.** Anyone watching the wire
sees the field names, and a determined party can reverse a binary. That is
acceptable. The protection is the license, the per customer registry token that
controls updates, and the contract, not obscurity.

**Port contracts are public, so anyone may implement them.** A competitor or a
customer can build a free cost plugin against the open port. That is the
intended consequence of decision 2: the supported, licensed implementation is
what CurieTech sells, not exclusive access to the interface.

**Every new paid view is an open source release.** Because decision 6 keeps all
rendering code in the open UI, a plugin cannot add a view on its own schedule.
That couples plugin features to the platform release train, which is acceptable
while CurieTech is the only plugin author.

**Gating ports are not decided here.** Spend caps and authorization decisions
gate behavior and must fail closed with an install level override. The first
gating port, likely for identity hardening, owes its own ADR covering latency
budget, caching and failure behavior.

**Not decided here.** The exact OpenAPI shapes, the retry bound, how the plugin
status read is cached, the default phase to bucket mapping for phases outside
the four buckets (CI fix rounds, waiting on CI, verification preflight), and
whether trials exist. Those are implementation and product choices inside the
decisions above.

# 96. A third-party port adapter is a deployed service, not a loaded plugin

Date: 2026-08-04

Status: Draft

Generalizes the harness trio to every port.
[ADR-0060](0060-the-harness-is-a-declared-package.md) decides what a harness is
(a declared package), [ADR-0061](0061-out-of-process-harness-boundary.md)
decides where its boundary sits (a process, not a Protocol), and
[ADR-0062](0062-harness-conformance-has-teeth.md) decides how we find out
whether one works. Those three answer the question for one seam. ADR-0061 and
ADR-0062 are both still Drafts, and 0061 is gated on a spike by its own decision
5, so nothing here waits on either being accepted: decisions 2 and 3 below stand
on the shipped composition precedents in Context, and if 0061 and 0062 land they
extend the same pattern to the harness port. This ADR answers that same question
for the rest: how a third party plugs its own implementation into any
of the swappable seams in [the interface catalog](../interfaces.md). It inherits
the trust rule from [ADR-0040](0040-adopt-acp-as-an-edge-projection.md) decision
4 and leaves the frozen contracts of
[ADR-0036](0036-aci-semver-and-reader-policy.md) untouched.

## Context

Two concrete asks motivate this. A company running an internal Slack-alike wants
Curie's whole surface (mentions in, streamed replies out, approval cards, hub
buttons) to run on their tool instead of Slack. A second wants a specialized
email surface. Neither is asking to change the kernel. Both are asking the same
question: **where do I put my code so the platform uses it instead of the
default?** Today the repo has no general answer, and the partial answers it does
have point in two different directions.

**The only plugin mechanism in the codebase is the harness registry.** A harness
registers a Python entry point in the group `"curie.harness"`
(`runner/src/curie_runner/harness/registry.py:58`), and the built-in Claude
harness registers itself the same way
(`runner/pyproject.toml:16-17`). The guards are real and fail closed: a flat
package path is refused, a built-in name claimed under any other path is
refused, and both post-load key rules run against what the loaded contribution
actually claims (`runner/src/curie_runner/harness/registry.py:105-192`).
Selection is an env read defaulting to the built-in
(`runner/src/curie_runner/config.py:122-124`), and a built-in name never goes
through discovery at all, so a malformed or import-crashing sibling entry point
cannot take the Claude harness down
(`runner/src/curie_runner/__main__.py:58-84`, issue #865).

**No other subsystem works this way.** Nothing else in the repo uses entry
points, imports a module named in config, or keeps a registry. The other ports
are Protocol classes with exactly one implementation, or collaborators the
caller supplies: the `ObjectStore` port, the queue broker, the eval scorers, and
the `ApproverSet` port behind one authorizer. That is the standing restraint of
[architecture-vision.md](../architecture-vision.md) working as intended: the
second implementation teaches the interface, so no adapter layer is written
ahead of a real second implementation.

**Agent bundles look like the obvious plugin vehicle and are deliberately not
one.** `packages/plugin-format/README.md:1-5` states the reason: the bundle is
the Claude Code plugin shape verbatim, compatibility is the distribution wedge,
and "this package does not invent format extensions." A bundle also runs in the
wrong place for platform code. It is delivered into the sandbox as
`CURIE_PLUGIN_DIR` and carries agent content: skills, MCP servers, hooks,
per-agent triggers, secrets, and approval policy.

**Everywhere a second implementation actually exists, the swap is composition,
not code loading.** ADR-0061 chose an HTTP process boundary for the harness
rather than an in-process Protocol. The substrate is chosen by
`CURIE_SANDBOX_SUBSTRATE` over a `SandboxClient` Protocol with two real
implementations (`apps/worker/src/curie_worker/run.py:96-111`). The vendored
agent-sandbox controller can be turned off for a BYO one
(`charts/curie/templates/preflight-controller.yaml:1-16`). Multi-model routing
is an optional sidecar serving the Anthropic wire shape on localhost, with the
runner's base URL repointed at it (`charts/curie/values.yaml:941-957`). Every
backing store follows one toggle-plus-BYO idiom (`charts/curie/CLAUDE.md:14-19`),
and every service image is independently repointable in the chart. The channel
seam already has a second implementation on exactly this pattern: a turn's
`ReplyHandle.endpoint` routes that turn's reply back to the ingress that
enqueued it (`packages/aci-protocol/src/aci_protocol/turn.py:31-51`, issue #19),
which is what lets the CLI's no-Slack stub and a real Slack workspace coexist on
one worker.

**The channel seam is where the demand is, and it is the catalog's weakest.**
Ingress is already channel-neutral: `QueuedTurn` carries `event_id`,
`conversation_id`, `author`, `text`, `reply_handle`, and `received_at`
(`packages/aci-protocol/src/aci_protocol/turn.py:54-68`, issue #7). Reply
content is the adapter-neutral `OutboundMessage` with semantic interaction
intents (`packages/channel-protocol/src/channel_protocol/models.py:76-88`), and
two renderers already consume it (Slack Block Kit and the terminal). What is
still Slack-shaped is delivery and binding. The egress model is edit-a-
placeholder over `chat.update`
(`apps/worker/src/curie_worker/slack_sink.py:1-7` and
`apps/worker/src/curie_worker/slack_sink.py:57-75`), the deployment binding is a
literal `slack_channel` column (`apps/api/src/curie_api/models.py:41-44`), and
agent create and update validate Slack `C.../S.../U...` id shapes
(`apps/api/src/curie_api/schemas.py:35-41` and
`apps/api/src/curie_api/schemas.py:44-61`). The catalog grades this seam `C` and
names the cheapest next step as a channel-neutral `ReplySink` post/update port.

## Decision

**1. There are two plugin kinds and they do not merge.** An **agent bundle**
extends one agent: it is the Claude plugin shape verbatim, it runs inside the
sandbox trust domain, and it versions with the agent. A **platform adapter**
implements a platform port: it runs in the platform trust domain, holds
platform-adjacent credentials, and versions with the deployment. A request to
"plug in a new channel, store, or harness" is never answered by the bundle
format. This is not a new restriction, it is the existing one stated once for
all ports (`packages/plugin-format/README.md:1-5`).

There is a third position, and it is already decided rather than invented here:
a **connector** is bundle-declared, platform-hosted, and agent-scoped. Under
[ADR-0086](0086-bundles-declare-connectors-the-platform-hosts-them.md) a bundle
names an image and the platform runs it as a service outside the sandbox, and
under [ADR-0037](0037-opt-in-binding-hook-and-pareto-model-routing.md) decision
2 a manifest declares routing intent while the platform owns the adapter. So
the split is not "the bundle format never grows a field the platform reads."
It is narrower and sharper: **a bundle's files never execute as platform code**,
while declaring an implementation for the platform to host is allowed, already
shipped, and additive. Declaring intent is a bundle's job; being the platform's
implementation is not.

**2. The default shape of a third-party platform adapter is a deployed service
speaking the port's versioned wire contract, selected by composition (chart
values, compose, or endpoint config) rather than by runtime code loading.** Four
reasons, in order of weight. It is **language-neutral**: an internal-tools team
writes its Slack-alike adapter in whatever stack that tool already lives in,
rather than being conscripted into Python. Its **blast radius is a process**: a
crash, a hang, or a memory leak in third-party code is a failed dependency, not
a wedged worker. It is **independently upgradable**: the adapter and the
platform release on their own cadences behind a versioned wire. And it is
**what the system already does** everywhere a second implementation exists, per
the composition precedents in Context, so it adds no new operational concept.

**3. A port is pluggable when its wire contract is published, versioned,
drift-gated, and carries a conformance kit, not when a registry exists.** This
generalizes ADR-0062: a registry proves an object was loaded, and a conformance
kit proves the object behaves. The promotion path for a seam has three rungs.
An `INTERFACE.md` documents where the code already draws the line. A contract
package makes the line a schema, as `aci-protocol` and `channel-protocol`
already do, which brings ADR-0017's tri-language drift gate and ADR-0036's
reader policy with it. A conformance suite is then something a third party runs
against its own adapter before it ever talks to us. Two rules bound every rung.
The trust rule from ADR-0040 decision 4 applies verbatim: **an adapter is a
rendering and transport contract, never a trust boundary.** Approvals resolve
solely through the API authorizer, an adapter's report of who clicked is input
rather than authority, and an adapter holds its own channel credentials and
never the platform's model credentials. This ADR extends it in one direction
0040 did not have to face, because it had one channel: **a reply endpoint never
receives any credential other than its own**, so per-endpoint authentication is
part of the egress contract a promoted port publishes, not an afterthought
bolted on later. Today's code violates that rule, which is why it is written
down. The worker delivers every per-turn reply through a single
`AsyncWebClient(token=self._token, base_url=endpoint)`, so the same Slack bot
token is presented to whatever endpoint the turn names
(`apps/worker/src/curie_worker/slack_sink.py:152-175`), and the #530
unreachable-endpoint fallback re-sends that reply's content to the default
transport, real Slack, when the per-turn endpoint is down
(`apps/worker/src/curie_worker/slack_sink.py:216-232`). Both behaviors are
correct for a one-workspace install and wrong the moment a vendor endpoint
coexists with real Slack: the vendor would be handed the platform's Slack token
today, and an outage would leak a vendor turn's reply into Slack.

The adapter-to-platform direction is bounded too. A third-party adapter's
ingress path is the API's authenticated hook surface, ADR-0079's HMAC-verified
`POST /hooks/{agent}/{hook}`, never direct broker enqueue: raw produce access
can mint a turn for any agent and forge `author`, which bypasses exactly the
dedupe and authentication the API ingress exists to provide. Direct enqueue
stays a first-party-only path, and the CLI stub is first-party code. The
interactivity return path is the mirror of it: approval clicks and button
actions arriving through the vendor's own tool resolve through the API with a
scoped, adapter-issued credential. Today the whole approvals router, resolve
included, sits behind the single platform-wide key
(`apps/api/src/curie_api/routers/approvals.py:35-36` and
`apps/api/src/curie_api/routers/approvals.py:115`), and the scoped state token
ADR-0033 minted for the sandbox is deliberately rejected everywhere but the
state router (`apps/api/CLAUDE.md`). A scoped adapter credential in that same
shape is therefore a prerequisite of a second channel adapter, not a follow-up
to one. The scope rule from ADR-0060 decision 5
applies too: a port whose contract cannot be stated is not promoted, and saying
no does not require a spike.

**4. In-process entry-point contributions are the exception, reserved for ports
that cannot cross a process boundary.** Some hooks are latency- or
transaction-coupled to a turn and would be absurd behind HTTP: ADR-0037's
binding hook, which runs at task-to-sandbox claim time before the sandbox boots
and returns a typed `BindingDecision`; approver-set membership on the approval
path; eval scorers running inside a graded sweep. For these the mechanism is the
harness registry generalized to per-port `curie.<port>` entry-point groups
carrying the same fail-closed guard rules, including the built-in-resolved-by-
direct-import rule that keeps a broken sibling from taking down a built-in
(`runner/src/curie_runner/harness/registry.py:105-192`,
`runner/src/curie_runner/__main__.py:58-84`). Those groups span service images
rather than living in one: the binding hook loads in the worker
(`apps/worker/src/curie_worker/binding.py`) while approver sets load in the API
(`apps/api/src/curie_api/approvers.py`), so "install a package into the image"
means a different image per port. The cost is the reason it is the
exception: the contribution must be a Python package pip-installed into the
service image, which means a derived image built on ours, per service, and
rebuilt on every platform release (`runner/Dockerfile:65-74`). That is a
documented pattern, not the third-party default.

**5. No generic plugin framework is built now. Ports are promoted one at a time,
on demand, and the channel port is first.** The second implementation teaches
the interface, and fifteen of the eighteen catalog seams have exactly one
implementation and no asking party. Promoting all of them now would produce the
speculative adapter layers the vision doc forbids, shaped by guesses about
second implementations that do not exist. The channel port is first because it
is the only seam with two named third-party askers and the only one graded `C`.
Its concrete lifts are follow-up issue material, not specified here: a
channel-neutral egress port with post and update semantics replacing the
Slack-only edit-in-place assumption (per-turn endpoint routing already exists);
per-endpoint egress credentials, replacing the one-token-for-every-endpoint
client decision 3 names; a scoped adapter credential for the interactivity
return path, so approval resolution does not require the platform-wide key;
a channel-neutral rename of the `slack_channel` binding surface and its
validators; approval-card delivery through the neutral `OutboundMessage` path;
assistant-thread status, which is `assistant_threads_setStatus` and Slack-only
today with no neutral equivalent and no endpoint fallback
(`apps/worker/src/curie_worker/slack_sink.py:404-423`); and trigger ingestion
per [ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md), whose
`POST /hooks/{agent}/{hook}` is accepted but not yet built. Each lift owes an
answer at every tier, the way ADR-0086 had to answer connector hosting for
`skill` with *declared but not exercisable here* rather than a red result: a
promoted channel port that only works on `cluster` fails the parity ladder.
The CLI stub is
promoted in status by this decision: it is the existing second channel
implementation, and it becomes the reference adapter the conformance kit grows
from.

Worked example, so the shape is not abstract. A Slack-alike vendor ships **one
container**. It runs an ingress that receives its own tool's events and turns
each into a `QueuedTurn` whose `reply_handle.endpoint` points back at itself. It
serves an egress endpoint at that address implementing the reply contract, so
the worker delivers that turn's reply to the vendor rather than to Slack. Two
caveats keep that honest. Enqueue-with-endpoint is how the first-party CLI stub
works today; the third-party ingress path is the ADR-0079 hook, and that hook is
accepted-but-unbuilt and carries no per-turn reply endpoint, so hook ingress
plus reply-endpoint carriage is part of the trigger-ingestion lift rather than
something a vendor can use now. And nothing binds the vendor's conversation to
an agent except equality on `agents.slack_channel`
(`apps/worker/src/curie_worker/binding.py:170` in the resolve SQL, run by
`apps/worker/src/curie_worker/binding.py:258-264`), while the API's validators
reject any id that is not Slack-shaped
(`apps/api/src/curie_api/schemas.py:35` and
`apps/api/src/curie_api/schemas.py:44-61`), so today the vendor has to mint
Slack-shaped channel ids exactly as the CLI stub does. That is precisely what
the binding-rename lift removes.

The credential property is a destination property, not a description of today.
The vendor's own channel credentials never leave its container, and it receives
neither a platform model credential nor another channel's credentials. Decision
3 names what stands in the way: the worker's single-token egress client is
exactly the thing the promoted contract has to fix before this example is safe
in a coexistence install.

Hosting is the vendor's choice, because the contract is the wire and not the
deployment. It can run the container itself and hand the operator a URL. When
the operator would rather Curie ran it, the intended path is not a new
mechanism: it is the connector-hosting substrate ADR-0086 already built, the
derived Deployment and Service, the NetworkPolicy, the container hardening, and
the sealed keys of [ADR-0094](0094-a-bundle-carries-its-own-sealed-connector-keys.md)
(itself still Draft), used deployment-scoped instead of agent-scoped. That
distinction is also the answer to "why is a channel adapter not just an MCP
connector": a connector serves one agent's tools, while a platform adapter
implements a port the whole install shares. Today the pragmatic egress compat
surface is the small Slack Web API subset the CLI stub already proves, and the
channel-neutral reply contract is the destination, not the starting point.

## Alternatives considered and rejected

- **Extend the agent bundle format with platform extensions.** The obvious move,
  since bundles are already the thing users author and ship. Rejected on trust
  domain and lifecycle, not on the format's purity. It is the wrong trust
  domain: bundle content is delivered into the sandbox, while a channel adapter
  sits on the ingress path holding channel credentials and feeding the binding
  resolver. And it is the wrong lifecycle: a bundle versions per agent, while a
  channel or a store is deployment-scoped and shared by every agent in the
  install. The distribution wedge is a real constraint on the format
  (`packages/plugin-format/README.md:1-5`) but it is not the argument here, and
  claiming any platform-facing field is an invented extension would prove too
  much: `connectors.yaml` and `deploy.yaml` are additive platform-facing
  declarations that already shipped, and they are fine because they declare
  intent rather than supply the platform's implementation.

- **In-process dynamic loading as the general mechanism, that is, entry points
  everywhere.** This is the harness registry generalized without the exception
  clause of decision 4. Rejected because it forces every third party into
  Python, which disqualifies the internal-tools team whose Slack-alike is a Go
  or TypeScript service. It forces a derived service image and a rebuild on
  every platform release (`runner/Dockerfile:65-74`). It puts third-party code
  inside the worker kernel's process, where a crash or a hang is a wedged kernel
  rather than a failed dependency. And it makes every adapter a supply-chain
  dependency of the platform process. The guard rules mitigate name collisions,
  not any of that. Retained only for the narrow ports decision 4 names.

- **Build the universal registry and framework across all eighteen catalog
  seams now.** Rejected as the exact failure the standing restraint exists to
  prevent. Most seams have one implementation and no asking party, so a
  framework would encode guesses about second implementations that do not exist
  and would be wrong in the ways that matter. The vision doc names speculative
  `StorageInterface` and `ChannelAdapter` layers as things we deliberately do
  not write. The cost of promoting a port on demand is one port's contract work;
  the cost of a wrong framework is paid by every port that has to fit it.

- **A bespoke RPC plugin protocol, in the style of HashiCorp's go-plugin.** Real
  prior art, and it does solve process isolation and language neutrality.
  Rejected because it buys those two properties at the price of a new runtime,
  a handshake, a codegen toolchain, and a second wire vocabulary, when the
  platform already composes over HTTP and queue contracts that are versioned,
  drift-gated, and understood by every lane. Revisit only if a specific port's
  contract genuinely cannot be expressed as HTTP or a queue payload, which no
  port in the catalog currently is.

## Consequences

**This ADR authorizes no code**, and it deliberately ships without a reference
third-party adapter. That is the honest weakness: decision 2 asserts the
deployed-service shape is right for a third party on the strength of internal
precedent, and the precedents (harness, substrate, controller, model sidecar,
stores) are all things we wrote or vendored ourselves. The first external
channel adapter is what converts that into evidence, and it may well teach us
the egress contract needs a field nobody predicted.

**The bundle format gets an explicit non-goal.** Decision 1 means the answer to
"can I put my channel adapter in a bundle" is no, permanently, and written down
rather than rediscovered per request. Bundles keep growing along their own axis
(connectors under ADR-0086, deploy targets under ADR-0089), which is agent
content, not platform implementation.

**The channel seam owes the work, and it is not free.** Promoting it means a
neutral egress contract, per-endpoint egress credentials, a binding surface that
is not a Slack column, approval-card delivery that does not assume Block Kit,
and an answer for assistant-thread status. The binding column and
its validators sit in the API's schema, so the rename is a migration plus a
contract change, not a refactor. Each piece owes a `skill`/`local`/`cluster`
answer as well, since the parity ladder grades a tier that cannot exercise a
feature honestly rather than silently. What this ADR buys is that the work is
done once against a published contract rather than once per asking vendor.

**Conformance becomes an obligation before the second adapter, not after.**
Decision 3 makes the kit part of promoting a port rather than a follow-up, and
ADR-0062 already priced that honestly for the harness: it adds work in the short
term, deliberately. The CLI stub being the reference adapter is what keeps the
first channel kit from being written from nothing.

**Multi-tenancy is assumed away, deliberately.** Decision 1's "versions with the
deployment" reads channel identity as single-tenant, which is what the install
looks like today. The multi-tenancy epic (#158) may want an adapter scoped per
tenant instead, and nothing here decides against that; it is simply out of scope
until that epic settles what a tenant owns.

**Not decided here.** Where a promoted port's contract package lives, whether it
is frozen (with the ADR-0017 and ADR-0036 obligations that come with freezing),
and what the neutral egress contract's actual shape is. Those are the second
implementation's job to teach, which is the point of decision 5.

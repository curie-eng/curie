# 61. The harness boundary is an out-of-process adapter, wire-compatible with Omnigent

Date: 2026-07-21

Status: Draft; program stopped under [ADR 0140](0140-curie-supports-one-model-harness-until-a-second-one-exists.md) before its prerequisite spike ran

Together with [ADR-0060](0060-the-harness-is-a-declared-package.md) this replaces
the Proposed [ADR-0031](0031-harness-neutral-runner-seams.md). 0059 decides
*what a harness is* (a declared package). This ADR decides *where the boundary
sits* (a process boundary, not a Protocol). Read
[ADR-0005](0005-claude-agent-sdk-adapter-and-frozen-aci.md) for the ACI freeze
this ADR leaves untouched, and
[ADR-0062](0062-harness-conformance-has-teeth.md) for the conformance
obligations that follow from moving the boundary.

## Context

`runner/src/curie_runner/adapter.py:31-52` puts the harness boundary
in process, as a Protocol whose `receive_turn` yields `AsyncIterator[Any]`. In
practice that `Any` is always a `claude_agent_sdk` dataclass, because everything
downstream is written to match one:
`runner/src/curie_runner/translate.py:84-107` is an isinstance chain over SDK
types.

The consequence is that the cheapest way to satisfy the seam is to forge the
Claude engine's types. The withdrawn OpenCode adapter did exactly that, and
ADR-0031 named the cost the synthesis tax. It is not the spike's invention: the
runner's own fake does it first and by design.
`runner/src/curie_runner/fake.py:1-8` says it "constructs real
claude-agent-sdk message dataclasses with canned content, so everything above it
runs unmodified", and `runner/src/curie_runner/fake.py:38` hand builds a
`ResultMessage` with `duration_ms=1`, `duration_api_ms=1`, `num_turns=1` and
`session_id="fake-session"`: dummy fields nothing reads, which is precisely the
tax ADR-0031 described. The spike copied our reference implementation.

An in process Protocol cannot prevent this. A type annotation is advice. Even
ADR-0031's proposed runner owned `TurnEvent` union would leave forgery merely
discouraged rather than impossible, because nothing stops an adapter from
importing the SDK, building an SDK object, and mapping it across.

Meanwhile Omnigent has already solved the other half of the problem for engines
we do not adapt. Each named Omnigent harness module exports
**`create_app() -> FastAPI`**: the harness is an out of process HTTP service,
messages and files in, text streams and tool calls out. Omnigent ships adapters
for engines we would otherwise integrate ourselves.

## Decision

**1. Adopt Omnigent's `create_app() -> FastAPI` shape as the harness boundary.**
Messages and files in, text streams and tool calls out, over HTTP. The boundary
is deliberately wire compatible so that Omnigent's existing Codex, Cursor and Pi
adapters are **consumable rather than reimplemented**.

**2. Rationale one: the process boundary is the only mechanically enforceable
anti forgery gate.** A `claude_agent_sdk` dataclass cannot cross HTTP. An in
process `AsyncIterator[Any]` makes leaking SDK types the *default*; an in process
`TurnEvent` union makes it *discouraged*; a process boundary makes it
*impossible*. This is the same reasoning ADR-0017 applies to contract drift:
prefer the mechanism that fails the build over the convention that asks people
to be careful.

**3. Rationale two: wire compatibility is the legitimate form of "get it for
free".** Under Apache 2.0 we could fork Omnigent's core, and we should not: it is
alpha software and forking buys a maintenance burden with no leverage. Landing
on their plug shape is different. It costs one adapter surface and returns every
adapter they write, without taking on their release cadence or their
dependencies.

**4. The ACI stays untouched and frozen. These are two boundaries with different
jobs.** The ACI is platform to runner: versioned, semver governed under ADR-0036,
and governance bearing (approval frames, `side_effect_flag`, budget). The harness
boundary is runner to engine: ecosystem facing, and carrying none of that
governance. The runner becomes a thin supervisor translating between the two.
Nothing in this ADR touches `packages/aci-protocol`, and any proposal that
appears to require an ACI change is out of scope here by construction.

**5. This ADR is gated on a spike: run one unmodified Omnigent harness adapter
behind our runner.** The in repo precedent for gating an adoption decision on a
spike is ADR-0011, which gated OpenCode on a steer spike, and that gate did its
job (the spike answered its question honestly, even though adoption was later
withdrawn for unrelated reasons recorded in ADR-0060).

The honest evidence gap: **Omnigent's `HarnessContribution` dataclass and adapter
wire were read from its official documentation, not from its source.** Wire
compatibility is therefore a hypothesis, not a verified fact. The spike's job is
to convert it into one or refute it. Success is one Omnigent shipped adapter,
unmodified, driving a real model turn through our runner and out the frozen ACI.

**6. Recorded fallback if the spike fails.** ADR-0031's decision 1: an in process
runner owned `TurnEvent` union (`AssistantText | ToolCall | RateLimit |
TurnResult`) replacing the SDK dataclasses out of `receive_turn`, with the Claude
adapter mapping SDK to `TurnEvent` and `translate.py`, the OTel feeds and budget
tracking consuming `TurnEvent` only. The branch
`origin/task/307-turnevent-message-model` is already most of that work. This
fallback is weaker (it is unenforceable, per rationale 2, and buys no ecosystem)
but it is real, costed, and mostly written.

## Alternatives considered

- **Keep the in process Protocol and add the `TurnEvent` union** (ADR-0031
  decision 1 as the primary decision rather than the fallback). Rejected as the
  primary because it is unenforceable: nothing stops an adapter from importing
  the SDK and mapping across, which is what the fake already does. It also buys
  no ecosystem, so every engine remains a from scratch supervisor.
- **Adopt Omnigent wholesale as a dependency.** Rejected on four counts. It is
  alpha. Its wire is unversioned where ours is frozen and semver governed
  (ADR-0036). Its delivery model carries none of the `side_effect_flag`
  idempotency semantics that [ADR-0013](0013-concurrency-and-delivery-model.md)'s
  kernel depends on. And it owns no cluster substrate, which is the layer Curie
  differentiates on ([ADR-0002](0002-kubernetes-agent-sandbox-as-runtime-substrate.md)).
  Wire compatibility takes the leverage without taking the dependency.
- **Adopt Omnigent's sandbox and policy layer (Omnibox) too.** Explicitly out of
  scope here. Curie's security rails are chart defaults under
  [ADR-0006](0006-security-rails-as-chart-defaults.md) and hardened locally under
  [ADR-0054](0054-local-docker-runner-hardening.md); replacing that layer is a
  separate decision with a separate blast radius. Noted as possible future work,
  decided here as no.

## Consequences

**Does this shrink second harness work, or only relocate it?** It shrinks, by a
mechanism that is worth stating precisely, because the obvious story is the wrong
one.

**It does not shrink work because the abstraction is cleaner.** A scripted second
implementation already passed the frozen conformance suite five times out of five
in an afternoon with zero core changes, so the interface was never the
bottleneck. The expensive part of a second harness is the subprocess supervisor
for a foreign out of process agent, plus the bundle compiler, plus history and
resume bridging.

**It shrinks work by letting us not write the integration at all.** For an engine
Omnigent already adapts, wire compatibility means the supervisor (the expensive
part) is consumed rather than authored. That is the entire saving and it is a
large one.

**The caveat, stated plainly: for an engine Omnigent does NOT adapt, this ADR
buys close to nothing on integration cost.** We still write the supervisor. We
write it behind HTTP instead of behind a Protocol, and we gain the anti forgery
enforcement of rationale 2, but the work itself is the same work. Anyone
evaluating this ADR by "will the next harness be cheaper" must first ask which
engine.

**It does not shrink the bundle compiler.** Our bundle is the Claude Code plugin
shape verbatim, ADR-0005's deliberate distribution wedge. An off the shelf
Omnigent adapter knows nothing about our bundle format, so the bundle to native
config compiler is owed in full for every non Claude engine, exactly as ADR-0011
warned and ADR-0060 restates.

**It does not shrink history and resume.** Descoped under ADR-0031 decision 4 and
carried forward by ADR-0060. It still starts with an `aci-protocol` contract
change under ADR-0036 and still owes its own ADR.

**The strategic consequence: an engine Omnigent already adapts is cheaper to ship
than OpenCode was.** Codex, Cursor and Pi come with the supervisor attached;
OpenCode would have to have one written. The bundle compiler is owed either way,
so the ordering of a second harness should follow Omnigent's adapter coverage,
not our prior familiarity with an engine.

**Costs accepted.** An extra process and an extra network hop inside a security
railed sandbox. That interacts directly with ADR-0006's chart defaults (the
harness process needs a loopback route the NetworkPolicy must not sever) and with
ADR-0054's local Docker runner hardening (the same rails apply to a second
process in the same container or a second container in the same pod). Neither is
a blocker, both are design work the spike must exercise rather than assume.

The hop is not the only cost. [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md)
makes the sandbox an explicitly bounded resource envelope, so a second process
inside that envelope is a claim on a ceiling that is now declared rather than
assumed infinite: its memory, its CPU and its ephemeral storage all come out of
the same budget the agent's own work draws on. The spike must therefore price the
harness process against that envelope, not merely measure its latency.

**Also accepted: a second failure mode.** An out of process engine can die
independently of the runner, so the runner's supervisor gains liveness and
restart responsibilities that an in process Protocol never had. The ACI side of
that is already covered (a wedged turn surfaces as a terminal status), but the
supervisor is new surface and it is where a spike is most likely to find
trouble.

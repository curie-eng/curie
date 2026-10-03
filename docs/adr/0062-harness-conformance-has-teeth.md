# 62. Harness conformance has teeth

Date: 2026-07-21

Status: Superseded by [ADR 0140](0140-curie-supports-one-model-harness-until-a-second-one-exists.md)

Realized by the import-linter contracts in `pyproject.toml`, run as
`uv run lint-imports` in `.github/workflows/ci.yaml`, so a forbidden import fails
the build rather than being caught in review.

Two of the three contracts this ADR asks for are live. The third, decision 1's
full ban on importing `claude_agent_sdk` anywhere outside the Claude harness
package, cannot pass yet: nine `curie_runner` modules still import it directly,
and confining them is the refactor that decision 2 depends on. The two contracts
that are live gate the boundaries already clean, so they cannot rot back before
that lands. Acceptance records the decision; the third contract is added when the
refactor does.

Follows [ADR-0060](0060-the-harness-is-a-declared-package.md) (a harness is a
declared package) and [ADR-0061](0061-out-of-process-harness-boundary.md) (the
boundary is a process, not a Protocol). Those two decide what a harness is and
where it plugs in. This one decides how we find out whether a harness actually
works, because today's answer is "the conformance suite is green", and that
sentence proves much less than it sounds like it does.

**Bounds [ADR-0055](0055-the-fake-model-is-a-plumbing-fixture.md)**; it does not
supersede or contradict it. 0055 decides what a fake *run* asserts (plumbing
completed, never a graded content claim), and that holds unchanged. This ADR
decides what the fake is *built out of*. A fake can be a plumbing fixture and
still be an honest one.

## Context

`packages/aci-protocol/src/aci_protocol/conformance.py` asserts one thing: an
inbound frame produces well formed NDJSON, round trips every outbound event
type, rejects unknown protocol versions, and ends in a `final` event at a
compatible protocol version. That is a correct and useful check of the ACI.

It is also the entire check. The suite never inspects what crosses
`ModelSession.receive_turn`, never type checks it, and asserts nothing about tool
naming or side effect classification. So a harness passes conformance while
being wrong in every way the ACI does not look at:

- **Tool identity.** `runner/src/curie_runner/side_effects.py:28-40` hardcodes
  Claude Code's PascalCase read only tool names. Under an engine with lowercase
  tool names every read only tool misclassifies as side effecting, wrongly
  suppressing the worker's auto retry. Conformance is green throughout.
- **Credential resolution.** `runner/src/curie_runner/sdk_auth.py:377-386`
  refuses any non Anthropic Messages wire outright, and
  `apps/worker/src/curie_worker/sandbox/docker.py:99` maintains a hand copied
  mirror of the runner's prefix rules. A second harness can get either half
  wrong and still emit a perfect `final` frame.
- **Bundle compile and telemetry.** Neither is looked at by the suite at all.

**The forgery is the mechanism, and our own fake teaches it.**
`runner/src/curie_runner/fake.py:1-8` states the design: it "constructs real
claude-agent-sdk message dataclasses with canned content, so everything above it
runs unmodified". `runner/src/curie_runner/fake.py:38` hand builds a
`ResultMessage` carrying `duration_ms=1`, `duration_api_ms=1`, `num_turns=1` and
`session_id="fake-session"`, dummy fields nothing reads. That is exactly the
synthesis tax ADR-0031 named, shipped as the sanctioned reference
implementation. The withdrawn OpenCode adapter did not invent the pattern; it
copied ours, because ours is what a new harness author reads first.

The general principle this exposes: **a green conformance suite behind a
protocol seam proves one seam and says nothing about the seams the forgery
bypassed.** The cheapest way to pass a conformance suite is to make the new thing
look exactly like the old thing to every downstream consumer, and that is the one
strategy the suite is structurally unable to detect.

## Decision

**1. No `claude_agent_sdk` import outside the Claude harness package, enforced by
an import linter contract in CI.** Not by review, not by prose in a README, and
not by a comment. A forbidden import contract fails the build. This is the
ADR-0017 pattern applied to layering: prefer the gate that breaks the build over
the convention that asks people to remember. ADR-0061's process boundary makes
SDK types unable to *cross* the seam; this makes them unable to be *reached* from
the wrong side of it in the first place.

**2. The fake stops forging SDK dataclasses and becomes a harness package like
any other.** It is the reference implementation of the harness contract, so it
must be honest about that contract. Today it demonstrates the anti pattern at the
exact place a new harness author looks for the pattern. Under ADR-0060 the fake
registers a contribution manifest, and under ADR-0061 it speaks the harness wire,
which is the same thing every other harness does and therefore the same thing a
new author should copy.

This does not touch ADR-0055. The fake tier still asserts only that the turn
completed, is still never graded, and its outcome is still `plumbing_ok`. What
changes is that the fixture stops being built out of another harness's private
types. A fixture whose value is "everything above it runs unmodified" keeps that
value when it speaks the declared contract; it does not need to impersonate the
Claude engine to deliver it.

**3. Conformance extends to the adjacent seams, not just the protocol seam.** A
harness is conformant when, in addition to today's ACI checks:

- its **declared read only tool set** is present and its tools classify
  correctly through the side effect classifier;
- its **credential resolution** produces the expected environment for each
  declared auth shape, including the refusal cases;
- its **bundle compile** hook turns a validated bundle into a session config the
  engine accepts;
- its **telemetry** emission produces the spans and budget signals the platform
  reads.

These are the four seams ADR-0031's audit identified as implicitly Claude
shaped. Under ADR-0060 they are manifest fields, which is what makes them
testable in a shared suite at all: a field can be asserted about, scattered
implicit behavior cannot.

**4. Conformance is not elevation.** Passing the extended suite makes a harness
*wireable*, not *shippable*. Parity evals remain the bar before a second harness
is elevated past spike status. This carries forward the one genuinely good idea
in ADR-0011, and it runs under
[ADR-0022](0022-eval-completeness-tier-parity-and-trace-promotion.md)'s tier
parity rule: the same evals at every tier, graded on what actually happened. Per
ADR-0055 the fake tier contributes plumbing signal only, so harness elevation
evidence must come from a real model tier.

## Alternatives considered

- **Leave conformance as is and rely on review.** This is the status quo, and the
  status quo shipped a sanctioned forgery in the reference fake and then
  propagated it into a spike. Review already looked at both.
- **Assert on message types inside the existing suite.** Type checking what
  crosses `receive_turn` would catch the forgery but leaves the other three
  seams (credentials, bundle compile, telemetry) untested, and it would be
  obsoleted by ADR-0061's process boundary, which makes the types unable to cross
  at all.
- **Keep the fake as an SDK forger and document that it is not a template.** A
  comment saying "do not copy this" sits at the top of the file a new harness
  author copies. We already know how that goes.

## Consequences

**Does this shrink second harness work, or only relocate it? Neither. It adds
work, in the short term, deliberately.** This ADR is truth in accounting, not a
saving. It says so plainly so that nobody prices the ADR-0060 and ADR-0061
program on the assumption that 0062 pays for part of it.

The added work is concrete: an import linter contract and its CI wiring; a
rewrite of the fake against the declared contract; and four new conformance
dimensions that must be authored once and then satisfied by every harness
including the Claude one. The Claude harness will be the first thing the extended
suite is run against, and there is no reason to assume it passes cleanly on the
first attempt, because nothing has ever asserted these properties about it
either.

**What it buys is that a green suite starts meaning something.** Today "the
second harness passed conformance" is compatible with every read only tool
misclassifying, credentials resolving wrongly, and no bundle ever compiling. That
is the state the OpenCode spike was actually in when it passed, and the passing
was accurate: the suite asked its questions and got true answers. The questions
were the problem.

**The fake rewrite is the highest risk item, and it is not a harness risk.** The
fake is the default in CI, in the chart's sealed pool, and in compose
(`${CURIE_FAKE_MODEL:-1}`). Changing what it is built out of touches the most
widely exercised path in the repo. ADR-0055's rules are the safety net here: the
fake tier's one assertion is that the turn completed, so a rewrite that breaks
plumbing fails loudly and immediately across CI rather than degrading quietly.

**Not decided here.** Whether the extended conformance suite lives in
`packages/aci-protocol` alongside the existing one or in a new harness contract
package. That depends on where ADR-0060's manifest lands, which depends on
whether the manifest is frozen, which ADR-0060 leaves as a deliberate later
choice.

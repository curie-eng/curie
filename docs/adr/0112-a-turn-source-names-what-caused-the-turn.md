# 112. A turn source names what caused the turn, not the channel it arrived on

Date: 2026-08-18

Status: Draft

Supersedes the value vocabulary of decision 2 in
[ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md). Everything else ADR-0079
decided stands: the API accepts inbound triggers, a trigger is an event kind carrying
no placeholder, the kernel posts rather than edits, and the queue payload is a shared
contract. Only the spelling of one enum's values changes here.

## Context

ADR-0079 added `source` to the queued turn so the kernel could tell a person's message
from a system-generated job, and named its values `slack | webhook | cron`. That was a
reasonable vocabulary in July: Slack was the only way a person could start a turn, so
"slack" and "a person spoke" were the same statement.

[ADR-0096](0096-port-adapters-are-deployed-services.md) then landed the channel port,
and `ReplyHandle.kind` can now be `email`. So an email that a person actually typed is
minted as `source=slack` with `kind=email`, which reads as a contradiction. It is not
one: the kernel only ever asks `source.is_job`, and that answer is correct. But a field
whose value contradicts the neighbouring field is a field every future reader has to be
talked out of misreading, and there will be more readers than there have been.

The two fields are genuinely orthogonal and must stay that way:

    kind    where the reply is delivered      slack, email, ...
    source  what caused the turn              a person, a hook, a schedule

A nightly digest posted into a Slack channel is `kind=slack` with `source=cron`. Neither
field is derivable from the other, which is why ADR-0079 was right to add a second one
and why collapsing them would be a regression rather than a simplification.

The problem is narrow: **one value's name encodes a transport when the field is not about
transport.** `webhook` and `cron` are already named after causes. Only the person-message
value is named after a channel.

## Decision

**Rename the person-message value from `slack` to `message`.** The enum becomes
`message | webhook | cron`.

`message` is chosen over the alternatives for one reason: it names the cause at the same
level of abstraction as its siblings. A `webhook` is a thing that happened; a `cron` is a
thing that happened; a `message` is a thing that happened. `human` and `interactive` were
considered and rejected below.

Nothing else moves. `is_job` keeps its meaning ("not a person's message"), the default
stays the non-job value so a job can never be created by omission, and the kernel's
steering rule is untouched.

### This is a breaking change and takes the breaking bump

A new enum value is breaking under the change-class table in
[`packages/CLAUDE.md`](../../packages/CLAUDE.md), so the protocol takes a **minor** bump
under 0.x rather than a patch. `source` is control-bearing -- it decides whether a turn may
steer a live session -- so an unrecognized value is rejected and never degraded, per
[ADR-0036](0036-aci-protocol-versioning.md). A consumer that has not been rebuilt must
refuse a `message` payload loudly rather than guess, and the version gate is what makes it
do that.

### No compatibility shim, and no aliasing

`slack` is not accepted as a deprecated synonym. Two spellings of one value would mean two
code paths agreeing by convention, and the version gate already provides the mechanism this
situation calls for: a producer and consumer that disagree fail loudly at the boundary
instead of silently accepting each other's vocabulary. A shim would convert that loud
failure into a quiet one, which is the opposite of what the gate exists for.

The cutover is therefore an ordinary contract change: bump, regenerate, deploy the images
together. The same shape as the 0.2.9 to 0.3.0 cutover the channel port already performed.

## Prototype

Built before this ADR was written, on `task/adr-0112-turn-source-naming`, because a
decision about cost should cite a measurement rather than an estimate.

The rename touches **one definition, four producers, one exporter default, three CLI sites,
and the committed artifacts**:

    packages/aci-protocol/src/aci_protocol/turn.py     the enum, is_job, the field default
    packages/aci-protocol/src/aci_protocol/rust_export.py   the generated enum's Default
    apps/dispatcher/.../handlers.py                    mention and block-action lanes
    apps/api/.../resumequeue.py                        approval resume
    apps/api/.../routers/channels.py                   channel ingress
    apps/worker/.../kernel.py                          the routing default
    cli/src/queue.rs, cli/tests/resume_wait.rs         the Rust producer and its fixture
    schema/, generated/rust, generated/ts, wire.lock   regenerated, not hand-edited
    schema/queued-turn.fixture.json                    the cross-language golden

`apps/api/.../routers/hooks.py` needs no change: it already mints `WEBHOOK`.

**Measured result: exactly three tests failed, and all three are version or wire pins doing
their job.** Two pinned the `0.4.x` series and one pinned the literal wire value `"slack"`.
Nothing else in 1406 passing Python tests or 799 Rust tests noticed, `cargo clippy
--all-targets` stayed clean, and contract regeneration produced no drift beyond the intended
values.

That is the whole argument for doing this now rather than later: the blast radius today is
three assertions and a regeneration. Every producer added before the rename widens it.

A separate observation worth recording, since it cost time to establish: a batch of
`apps/worker/tests/eval` and `apps/worker/tests/binding` failures in this environment fail
**identically on pristine `next`** and are unrelated to this change. They were confirmed
against a baseline worktree rather than assumed, because a failure nobody has baselined is
indistinguishable from one the branch caused.

## Consequences

- One minor protocol bump, and the three language artifacts regenerate with it. Mixed-image
  deployments refuse each other at the version gate during the cutover, which is the
  designed behaviour and not an incident.
- Three pinned assertions move. Their purpose is to make exactly this kind of change
  visible, so moving them is the system working.
- `source` and `kind` stop contradicting each other on the email path. The orthogonality is
  stated in the model's docstring rather than left to be rediscovered.
- A fourth value, if one is ever needed, now has an obvious naming rule to follow: name the
  cause, never the transport. That rule is the durable output of this ADR; the rename is
  just its first application.
- ADR-0079's supersession chain records why the vocabulary changed, so a reader who finds
  `slack` in an old payload or an old branch learns what happened rather than guessing.

## Alternatives considered

**Leave it and document the wart.** Cheapest today and the option this ADR exists to reject.
The contradiction is not a comprehension problem for the people who built it; it is a
comprehension problem for everyone who arrives later, and the number of arrivals only grows.
The prototype also shows the cost is smallest now.

**`human` instead of `message`.** Accurate today and wrong soon: ADR-0079's own
agent-to-agent case, and the Twin agent pattern, both produce turns caused by an agent
speaking as a participant rather than by a job firing. Those are messages and are not human,
so `human` would need a fourth value almost immediately, at another breaking bump.

**`interactive` instead of `message`.** Describes the session's character rather than the
turn's cause, and the field is about cause. It would also invite the reading that a
non-interactive message is a different value, which is the confusion being removed.

**Accept `slack` as a deprecated alias during a migration window.** Rejected in the
Decision above: it converts the version gate's loud failure into a quiet agreement by
convention, and this contract's whole position is that a boundary disagreement should be
impossible to miss.

**Collapse `source` into `kind`.** They are not the same axis. A cron digest into a Slack
channel needs both values independently, so one field cannot carry both without encoding
pairs, and encoding pairs is how an enum becomes a matrix nobody can extend.

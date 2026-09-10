# 146. For a headless lane, at capacity means wait, not reply

Date: 2026-09-10

Status: Draft

Proposed as part of the dark-factory decision set, discussed in
[discussion #2551](https://github.com/curie-eng/curie/discussions/2551).

Extends [ADR-0109](0109-retention-capacity-and-back-pressure-are-one-policy.md)
(retention, capacity, and back pressure are one policy) by deciding the
queue discipline for a lane with no human at the other end. It builds on
[ADR-0013](0013-concurrency-and-delivery-model.md) (one live session per
thread) and [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md) (the
sandbox is a bounded resource envelope). It supersedes no ADR.

## Context

A default install can hold eight concurrent sandboxes: the tenant
`ResourceQuota` allows `limitsCpu: "8"` (`charts/curie/values.yaml:2432`) and
one sandbox costs one CPU. ADR-0109 wrote the arithmetic down and decided the
policy at the boundary — admission before creation, a bounded queue, then an
explicit refusal.

Part of that has since shipped, and it shipped well. A claim whose pod the
quota refuses is now detected from the claim's own status after a two-poll
debounce and raised as `CapacityExhaustedError`
(`apps/worker/src/curie_worker/sandbox/substrate.py:790`), rather than polling
in silence until `claimTimeoutSeconds` elapses. The failure is fast, and it
names the quota, the resource axis, and the observed usage in the operator log.
That closed the "late and misattributed" half of what ADR-0109 set out to fix.

What it did not close — and did not set out to close — is what happens to the
work. The kernel replies into the thread with "This agent is at capacity right
now… please try again shortly" and returns `TurnOutcome(terminal_ok=True)`
(`apps/worker/src/curie_worker/kernel.py:2468`). `terminal_ok` is the input to
the retry-and-escalate decision (`apps/worker/src/curie_worker/kernel.py:554`),
so a true value means the delivery is done: it is acked off the stream and
never retried.

For an interactive turn that is the right answer and a good one. A person read
a sentence, understands what happened, and will send the message again.

For a headless lane it is the wrong answer, and the reason is precisely that it
is correct-looking. Nobody reads the reply. Nothing sends the message again. A
labelled issue that arrives while eight sandboxes are held produces a thread, a
message into a channel, a green ack, and no pull request — and the operator's
only signal that a ticket was silently dropped is its continued absence from a
board that lists what came out rather than what went in. This is an ambiguous
signal — "at capacity" means "come back later", which is a true statement about
an interactive user and a false one about a webhook — being resolved into a
sticky, invisible, work-destroying state.

The obvious fix is to make the outcome retryable rather than terminal, and it
is not sufficient on its own. The retryable set is bounded: `max_attempts` is 3
and the delivery deadline
([ADR-0131](0131-a-delivery-has-one-deadline-and-one-renewable-fenced-owner.md))
bounds the whole attempt chain. A backlog
of fifty labelled issues against eight slots does not clear inside three
attempts, and turning capacity exhaustion into a `runner-error` would spend the
retry budget on a condition that is neither an error nor the delivery's fault,
then dead-letter the ticket anyway.

ADR-0109's own queue does not reach this either, and it says so honestly: its
bound is "the headroom under the ADR-0013 thread lock TTL", which is 120
seconds. That is the right bound for a claim already holding a thread lock and
a caller waiting on a reply. It is not a bound in which a fifty-issue backlog
drains.

## Decision

**A delivery on a headless lane that cannot be admitted is not claimed. It
waits in the stream, unacked, until capacity exists or its deadline expires,
and an expiry is a visible failure rather than a reply.**

The distinction this decision turns on is where the wait happens. ADR-0109
queues *inside the worker*, after the delivery has been claimed and while a
thread lock is held, which is why its bound must be short. This decision moves
back-pressure one layer out, to the point before a delivery becomes the
worker's problem at all.

1. **Admission is checked before the delivery is claimed.** The worker consults
   the published ceiling from ADR-0109 decision 1 before it takes a headless
   delivery off the stream. A delivery it cannot serve is left pending, which is
   what a Valkey stream consumer group is for: an unacked entry stays in the
   pending list and is redelivered.

2. **Waiting is the normal case, not a failure.** Time spent waiting for a slot
   does not consume `max_attempts`, is not classified as `runner-error`, and
   emits no message to any channel. A headless delivery may sit for as long as
   its deadline allows. The unit of patience is the delivery deadline
   (ADR-0131), which is already the durable bound on how long the platform will
   keep trying, and which an operator can raise for a factory lane without
   changing anything about the interactive one.

3. **A headless delivery never resolves capacity exhaustion into an ack.** The
   `terminal_ok=True` reply path stays exactly as it is for a turn with a human
   reply route. For a headless delivery the same condition either waits or, past
   the deadline, ends in an explicit, recorded failure naming capacity as the
   cause.

4. **Expiry is visible where the work was.** A delivery that never got a slot is
   not silently gone. The outcome is written where an operator looking for that
   ticket will find it — the dead-letter graveyard already carries permanently
   failed deliveries
   ([ADR-0039](0039-bounded-delivery-and-a-dead-letter-graveyard.md)), and a capacity
   expiry belongs there with a
   classification that distinguishes it from a run that failed.

5. **What tells the two lanes apart is the reply route, not a new flag.** A turn
   whose delivery carries a human-addressable reply target is interactive; a
   webhook-sourced delivery whose only audience is an operator log is not. The
   platform already carries this distinction — `TurnSource`, and the reply
   handle minted from the channel binding — and this decision reads it rather
   than adding a parallel one.

**The invariant** (what we test to): a labelled ticket admitted by the ingress
either runs, or is recorded as failed with a stated cause. It is never
acknowledged, replied to, and dropped.

## Consequences

The factory becomes throughput-limited instead of loss-limited. Labelling fifty
issues against eight slots takes longer; it does not lose forty-two of them.
The demo throttle — "label at most as many issues as the ceiling allows" — stops
being load-bearing.

Deliveries sitting unacked in the pending list is a new steady state for the
stream, and the pending list is not free: it is the same structure crash
recovery reads. A large waiting backlog makes recovery scans longer and makes
"is this worker stuck or is it waiting?" a question an operator will ask. That
is the main cost of this decision and the thing to instrument first.

The delivery deadline becomes the factory's real service-level knob. An
operator who wants a fifty-ticket batch to survive overnight sets it there, and
the failure mode of setting it too low is now legible rather than silent.

An operator can no longer tell capacity exhaustion from a slow queue by watching
a channel, because the channel goes quiet on this path. The compensating signal
has to be the observability ADR-0109 decision 5 already calls for — held
sandboxes, queue depth, refusals — which this decision makes load-bearing rather
than nice to have.

Nothing here changes isolation, the ceiling itself, or the interactive path. One
pod still serves one thread, ADR-0059's envelope stands, and a human at capacity
still gets the same sentence they get today.

## Alternatives considered

**Classify capacity exhaustion as retryable and let the existing retry ladder
handle it.** Rejected. `max_attempts` is 3 and the attempt chain is bounded by
the delivery deadline; a backlog that takes an hour to drain exhausts that in
minutes and dead-letters work that was never wrong. It also mislabels a
capacity condition as an error, which pollutes exactly the signal an operator
uses to find real failures.

**Reuse ADR-0109's in-worker bounded FIFO for the headless lane.** Rejected as
the whole answer, though it stands unchanged for interactive turns. Its bound is
headroom under a 120-second thread lock, which is correct for a queue entered
after the lock is held and far too short for a backlog. Widening that bound
would mean holding thread locks for hours, which is a different and worse
decision.

**Raise the ceiling until waiting never happens.** Rejected, and ADR-0109
already tested this shape: every default has a load that exceeds it, and tuning
moves the cliff without changing the behaviour at it. The dark factory's whole
premise is that batch size grows.

**Refuse the delivery at the ingress with a 429 and let GitHub retry.** Rejected.
It makes the tracker's retry policy the platform's capacity policy, retries
carry the same delivery id and are deduplicated by design, and the operator
loses any record that the ticket was ever offered.

**Reclaim an idle route to serve the waiting delivery** (ADR-0109 decision 4).
Not rejected, and not decided here. Reclamation is the highest-risk part of
ADR-0109 by its own account, and it is orthogonal: it changes which work gets a
slot, while this decision changes what happens to work that does not get one.
Adding it later strictly improves this decision's latency and changes none of
its guarantees.

## Realizing code path

Unimplemented. The named paths for the eventual work are the admission check
ahead of the claim in `apps/worker/src/curie_worker/consumer.py` and
`apps/worker/src/curie_worker/sandbox/substrate.py`, the capacity arm of
`apps/worker/src/curie_worker/kernel.py`, and the dead-letter classification in
`apps/api/src/curie_api/graveyardwatcher.py`.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act.

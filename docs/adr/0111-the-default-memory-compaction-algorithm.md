# 111. The default memory compaction algorithm

Date: 2026-08-17

Status: Draft

**Folded into [ADR-0167](0167-agent-and-channel-memory-are-an-index-the-agent-writes-limited-by-an-inclusion-list.md). Not proposed for acceptance on its own.** ADR-0167
decides the write path this document assumed was unnecessary (clause 2) and
does not compact memory at all: a capped index with detail read on demand
keeps what each session loads small. When ADR-0167 is Accepted, this line becomes
`Status: Superseded by ADR-0167`.

Supersedes in part [ADR-0095](0095-tiered-memory-lifecycle.md) only for its
incremental compaction mechanism, as narrowed on 2026 08 17. ADR-0095 remains
current for durable storage and session injection.

Sits between [ADR-0095](0095-tiered-memory-lifecycle.md) (where a memory
document is stored and how it is injected) and
[ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) (how
background work runs at all). Neither says how a document comes to exist. This
one does, as an opt-in default rather than a requirement.

## Context

[ADR-0095](0095-tiered-memory-lifecycle.md#narrowing-recorded-2026-08-17), as
narrowed by the 2026-08-17 architecture review, defines exactly two things:
where a memory document lives durably, and that it is injected into a session
when it exists. It is explicit that *"how the heck did these get created? Who
updates them? Not the responsibility of that ADR."*

ADR-0099 gives an agent a way to run work no user started, on a schedule.

Between them is a gap that is technically the builder's to fill and in practice
nobody's. A platform can ship both halves and tell the builder to write the
connector, and the honest description of that state is the review's own: *"who
cares if memory.md is getting injected if nobody even has the ability to create
a memory.md? Like you're injecting 2 blank files."* Injection with no producer
is a feature that reads as finished and does nothing, which is the same shape as
a port with no caller — a failure this repository has shipped before.

So the platform should offer a default algorithm, opt-in, that a builder can
extend, replace, or switch off.

Three facts about the surrounding system shape it:

1. **The platform already holds the source.** Per-thread transcripts (ADR-0029)
   are the platform's own record of what was said. Reading the surface itself is
   a separate, optional capability (ADR-0100) that a surface may not have.
2. **The write door already exists.** ADR-0095's document is a state-store key
   the platform key may write. An operator hand-authoring a document and a
   scheduled job producing one use the same endpoint.
3. **Writes are already capped.** The state router refuses a value over the
   configured per-value size, so an oversized document is refused rather than
   silently injected.

## Decision

**The platform ships one default compaction algorithm: a scheduled turn that
rebuilds a scope's memory document from the platform's own transcripts, from
scratch, and writes it through the ordinary document write path. It is off
unless a bundle opts in, and a builder may extend, override, or disable it.**

### 1. From scratch, every time. Never a compaction of a compaction.

A run reads source material and produces the whole document. It does not read
yesterday's document and fold new material into it.

This is the load-bearing clause. Folding a summary into a summary compounds
error: each pass restates the last pass's paraphrase, and what began as a fact
arrives some runs later as an assertion nobody made. The review named it
plainly — *"you're going to hallucinate the bejesus out of it, because you're
compacting compactions; it's just a game of telephone at that point."*

**The property that follows is self-correction, and it is worth more than the
absence of compounding.** Because a run derives the whole document from source
material it re-reads, an error a run introduces does not survive the next one:
the document is re-derived, not repaired. Ground truth stays the transcripts, and
the document is only ever the current derivation of them. Any design whose source
of truth becomes the document itself has no path back — an error introduced there
is permanent, and every later write builds on it. That is the deeper reading of
the review's objection, and it is the one that decides between the alternatives
below.

The cost of this clause is the reason for clause 3.

### 2. The source is the platform's transcripts, not the surface.

A run reads the thread transcripts belonging to the scope after the scope
attribution work in #27 is complete, and never the surface's own history. Today
the platform serves transcript state by agent and thread, not by scope, so #27
is a prerequisite for this scope tier default.

This keeps the algorithm available on every surface from its first day: a
surface with no readable backlog (email) is not a special case, because the
algorithm never wanted the backlog. Where ADR-0100's read capability is enabled,
a run may additionally record pointers into the surface, but it may not depend
on them.

**This is also why no in-turn write path is required.** When a person says
"remember that our fiscal year ends in March", that utterance is already in the
transcript. A run picks it up without a tool the agent has to think to call, and
without opening a third write channel into memory — which the poisoning taxonomy
in ADR-0095 counts as a distinct exposure. The cost is latency, made explicit in
clause 7.

### 3. Off by default, opt-in per bundle.

Rebuilding from scratch is the expensive shape, and expense is not something to
enable on a builder's behalf. A bundle declares that it wants the default
algorithm. Issue #27 scope attribution is a prerequisite for this default
configuration. The default remains opt in, and ADR-0095 handles an absent
document as ordinary.

### 4. Scope tier by default. No default agent-tier algorithm.

What a place knows is derivable from what was said in that place. What an agent
should carry across every place it works is a judgement about generality that
this algorithm has no basis to make, and promoting a scope fact to the agent
tier is exactly the move ADR-0095's trust posture says must stay conservative.

A builder who wants an agent-tier document writes one, by hand or with their own
algorithm, through the same write path.

### 5. A run writes through the ordinary document write path.

Not a private back door. The consequence worth stating: an operator can
hand-author a document today and a scheduled run can replace it tomorrow, and
neither needs to know about the other.

### 6. A run that cannot produce a valid document leaves the old one standing.

The write is capped, and a rebuilt document that exceeds the cap is refused. A
refused write must leave yesterday's document in place rather than replacing it
with nothing: stale memory is a degraded turn, and absent memory that used to be
present is a regression the agent cannot report.

A run that overflows should therefore shrink its output and retry within the
run, and a run that still cannot fit records the failure rather than clearing
the document.

### 7. Latency is a property of the design, not an accident.

A fact stated today is remembered from the next scheduled run, not from the next
turn. For work on a quarterly or weekly cadence this is invisible.

Two cases make it visible, and they are not the same case. Within one thread
nothing is lost: that thread's transcript is injected alongside the document, so a
fact stated an hour ago is still in context. **Across scopes it is lost until the
next run** — a fact stated in one place is not available to the same agent
answering in another, which is exactly the position an agent bound to both a chat
channel and a mailbox is in.

Where that is not acceptable, the ways out are both worse than they look, and the
alternatives below state why. Targeted editing cuts the latency to minutes but
makes the document its own source of truth, which forfeits clause 1's
self-correction. An in-turn write path cuts it to zero but moves the judgement
into the model and opens a third write channel into memory.

**A shorter period is the cheapest way out and the only one that keeps clause 1.**
A run is a full rewrite whatever schedules it, so firing more often trades money
for freshness and nothing else. ADR-0099's trigger vocabulary is open and already
names message-count thresholds as a candidate: that fires where traffic is and
stays quiet where there is none, which is the shape a cron interval cannot express.
Anything beyond that is a separate decision and is not a patch to this one.

### 8. A run must be able to show what it changed.

A run replaces the document whole, so only that run knows what it removed. If the
previous document is not retained, **nothing can tell that last night's run lost a
fact.** The state store's `version` is a compare-and-set counter, not a value
history: the row is overwritten and the prior text is gone.

This matters more under clause 1 than it would under a design that appends,
because the Consequences below already rule out the obvious remedy — editing the
document does not stick, since the next run overwrites the edit. **Inspection is
therefore the only remedy this design leaves.** A wrong fact is fixed at its
source, and finding it starts with reading what the document says and what
changed.

It is also the property this ADR should borrow from the assistants that write
memory in-turn on the model's own judgement, rather than only noting that it does
not need to. Their looser write path is safe for a reason that is not timing:
**the artifact is a version-controlled file, so a wrong write appears in a diff
and a person sees it.** A document held in one mutable row has no equivalent, and
a compaction run's judgement is not obviously better than a model's mid-turn — it
is only less frequent.

Two concrete things are required and neither exists today: a way to read the
current document from outside the sandbox, and a retained previous version so a
run's effect is expressible as a diff. Neither needs a new datastore. Until both
exist, **a run that quietly dropped a fact is indistinguishable from one that
correctly decided the fact no longer held** — and clause 6's "leave the old
document standing" is the only visible failure mode the design has.

### 9. Extension points

- **Replace** the prompt a run uses.
- **Replace** the whole algorithm with a bundle-declared hook of the builder's
  own, using the same write path.
- **Disable** it, which is also the default.

## Consequences

- Cost scales with transcript volume per scope, not with agent count. A scope
  with no new activity since its last run has nothing to rebuild from and should
  be skipped, so an idle deployment costs nothing.
- The first run against a long-lived place is the expensive one, and it is
  expensive exactly once.
- A builder who reads a wrong fact in a document fixes it by fixing the source
  or by overriding the algorithm, not by editing the document — the next run
  would overwrite the edit. This is a real ergonomic cost of clause 1 and the
  price of not compounding error.
- Nothing here requires ADR-0100. Where it is enabled a run gains pointers;
  where it is not the document is self-contained.
- Because the algorithm is a scheduled turn, everything ADR-0099 says about
  silent turns, run records, and per-scope serialisation applies unchanged. The
  scheduled turn reuses existing machinery, but the complete default depends
  on new scope attribution work in #27 and the audit machinery required by
  clause 8, so "adds no new machinery" is not an accurate claim.
- **Clause 8 is the one thing here that does need machinery, and it is absent.**
  The record-shaped memory of ADR-0025 has an inspect/edit surface (#267); the
  document does not, and no version of it is retained. Both are separate work
  from this algorithm and both gate how much this algorithm can be trusted, so
  they belong in an issue rather than in this ADR's scope. Enabling the algorithm
  before they exist is a deliberate choice to run something whose output nobody
  can audit, which is defensible only while the tier it writes is the scope tier
  and the blast radius is one place.

## Alternatives considered

**Incremental notes folded periodically into the document.** The shape ADR-0095
originally carried: in-turn writes append notes, a scheduled pass folds them in.
Rejected because the fold is either a compaction of a compaction, which clause 1
refuses, or it re-reads the source anyway — at which point the notes are a
second write channel that changes nothing about the output.

**Targeted editing of the document, fact by fact.** The Mem0 shape ADR-0095's
prior art names: extract candidate facts from new traffic, then a second pass
decides add, update or delete against what the document already says. Stated as
its own alternative because the note-folding rejection above does not describe it:
it does not re-summarise the whole document on a schedule.

**It does not escape clause 1, and an earlier draft of this ADR wrongly said it
did.** Two things break that claim. An update is a generation, not a swap: to
change what a line says without discarding what it already carried, the pass has
to read the existing line and write a merged one, which is a paraphrase of a
paraphrase at line granularity rather than document granularity. And because the
pass runs per turn rather than per period, there are orders of magnitude more such
generations against the same text.

The decisive objection is the one clause 1 states positively. **Targeted editing
makes the document its own source of truth.** Nothing re-reads the transcripts, so
nothing can notice that a line drifted; a line that went subtly wrong on one turn
is wrong permanently, and every later edit against it inherits the error. A full
rewrite is self-correcting for exactly the reason it is expensive — it goes back
to the source every time, so yesterday's mistake is not repaired but replaced.

A second, smaller objection: **it cannot reorganise.** Five true lines that are
really one rule stay five lines, because no new fact contradicts any of them.
Near-duplicate accumulation is the failure Mem0's own second pass exists to fight,
which is the honest signal that it is the failure mode of the shape.

Both failures also arrive on the wrong timescale. Staleness shows up on the first
day, self-heals on the next run, and is explainable to whoever noticed. Silent
drift in a document nothing re-derives shows up over months, compounds, and does
not self-heal. **Between two designs, ship first the one whose failure is visible
inside the period you can measure.**

It is nonetheless the right next increment, and in this order: a document a full
rewrite has already organised is the document targeted editing performs well
against, where a document targeted editing built from empty has no structure for
it to preserve.

**An in-turn `remember` tool as the primary write path.** Rejected as
unnecessary rather than wrong: the utterance a tool would capture is already in
the transcript this algorithm reads. It also asks the model to decide what is
worth keeping mid-task, which is a judgement made with less context than a run
that sees the whole period.

**On by default.** Rejected on cost. A platform that quietly bills every
deployment for a nightly full rebuild has made an expensive choice on the
builder's behalf.

**Compaction per thread rather than per scope.** Rejected: a thread's context is
already replayed from its transcript (ADR-0029). Memory exists for what crosses
threads.

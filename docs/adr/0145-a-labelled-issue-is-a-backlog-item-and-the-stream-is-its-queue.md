# 145. A labelled issue is a backlog item and the stream is its queue

Date: 2026-09-10

Status: Draft

Proposed as part of the dark-factory decision set, discussed in
[discussion #2551](https://github.com/curie-eng/curie/discussions/2551).

Relies on [ADR-0134](0134-a-hook-shares-one-thread-per-partition.md) (a hook
shares one thread per partition) for its fan-out primitive and on
[ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md) for the ingress it
partitions. It supersedes no ADR.

## Context

Headless ticket-to-pull-request execution needs a way for a ticket to become a
turn without a person sending a message. Curie has two intake surfaces that
could carry that and one that already almost does.

`POST /github/webhook` is signed, deployed, and deliberately narrow: after the
ping case it returns `ignored` for every event that is not a push
(`apps/api/src/curie_api/routers/github.py:44`). It exists to run the
git-push-is-the-deploy flow of ADR-0014 and it knows nothing about issues.

`POST /hooks/{agent_id}/{hook}` is the general signed ingress. It verifies an
HMAC over the body, claims an `X-Curie-Delivery-Id` so a redelivery runs at
most once, returns a receipt naming the conversation it landed on, and — since
ADR-0134 — mints one thread per partition value drawn from an operator-named
JSON Pointer into the delivery body (`agents.hook_partitions`,
`apps/api/src/curie_api/models.py:162`). One hook delivery per issue,
partitioned on the issue number, is exactly one thread per ticket with
intra-ticket serialization intact. That is the fan-out the factory needs and it
is already built.

What is not decided is whether a backlog is a thing Curie stores. ADR-0013
rejected a durable workflow engine on ADR-0007 grounds, and the `curie:runs`
Valkey stream is a delivery transport rather than a work store: there is no row
with a task id, a submitter, a priority, or a `queued → running → done` status
that survives a restart. An earlier framing of this proposal treated that
absence as the largest gap in the design and reached for a task table and a
polling loop over the tracker.

It is the wrong reach. The tracker already is the backlog. A GitHub issue has
an id, an author, a state, labels, comments, a permalink, and a UI every
engineer already uses; duplicating a subset of that into Curie creates two
records of the same fact and a reconciliation problem between them. What the
platform actually needs is not to *hold* the backlog but to be *told* when an
item enters it, and to hold the in-flight item long enough to run it.

There is one property of the stream worth naming precisely, because it is
easy to overstate. The runs consumer group is created at `$`, the stream's
current tail (`apps/worker/src/curie_worker/consumer.py:225`). That is a
deliberate first-boot guard: a persistent Valkey that accumulated stale
mentions while no worker ran must not storm every one of them into a live turn
the moment a group appears. An *existing* group is left untouched, and
crash recovery works off the pending list rather than the group's start id, so
a worker restart does not discard in-flight deliveries. The loss window is
narrow and specific: entries written to the stream before the group has ever
been created.

## Decision

**The tracker is the backlog. A labelled issue event is a signed delivery on
the existing hook ingress, partitioned on the issue number, and the
`curie:runs` stream is the queue for the items currently in flight. Curie
stores no task row and polls no tracker.**

### The issue event enters through the ingress that already exists

`issues` and `issue_comment` events are routed into the hook enqueue path — the
one with the HMAC check, the delivery claim, the receipt, and the partition
derivation — rather than into a second ingress written beside it. The
`/github/webhook` route's push handling is unchanged; what changes is that the
non-push arm stops being a uniform `ignored`.

The operator says which events matter by labelling. A delivery whose event does
not name a label the operator selected for that agent is acknowledged and not
enqueued. Label selection is operator state on the agent row, alongside the
other operator-owned per-agent policy already there (`model`, `thinking`,
`approval_required_tools`, `approval_routes`, `hook_partitions`). A bundle has
no surface for it, for the same reason ADR-0134 put the partition pointer on
the agent row and refused a request header: the sender must not decide how much
of this install's capacity it can command.

### The partition is the issue number, and it is the thread

Under ADR-0134 the conversation id becomes
`hook:<agent_id>:<hook>:<issue-number>`. Every artifact that keys on the
conversation id follows: the sandbox affinity route, the thread lock, the
transcript, the workspace selection, and the publication lineage are all
per-issue. Two events about the same issue serialize; two events about
different issues run concurrently. ADR-0134's stability requirement is
satisfied by construction, because an issue number is a stable identity of the
thing the delivery is about and never a per-delivery value.

### There is no polling loop and no task table

The platform does not sweep the tracker for ready work. A push arrives or it
does not. This is the smallest thing that works, and it is reversible: if the
narrow group-creation loss window above ever bites in practice, the remedy is a
reconciliation sweep that asks the tracker which labelled issues have no
publication — a read against the record that is already authoritative — and not
a durable task store that would have to be kept in step with it.

### Reading the ticket is the bundle's job

The delivery carries enough to identify the issue; it is not the ticket. The
factory bundle reads the issue by link through an MCP server holding its own
credential, which is what makes the same intake shape work for a tracker that
is not GitHub. The platform does not parse issue bodies, does not model
acceptance criteria, and does not learn a tracker's schema. A second tracker is
a different signature scheme at the ingress and a different MCP server in the
bundle, and nothing else.

## Consequences

An operator labels an issue and a thread exists. That is the whole intake
story, and it is roughly a day of work against shipped ingress rather than the
task-store epic the first framing implied.

The backlog is legible where engineers already look. "What is queued" is a
label filter in the tracker's own UI, and it stays correct without Curie
writing anything.

Curie gains no ability to answer "what is queued" from its own data. `GET
/publications` answers what came *out*; nothing answers what is waiting. For a
push-based design that is honest rather than missing — the queue is somewhere
else on purpose — but an operator wanting one screen will be looking at two.

A labelled issue commands sandbox capacity, and whoever can label can therefore
drive load. The agent backlog quota (per agent, per window) still meters
admission exactly as it does for any hook, and ADR-0134's analysis of
sender-driven cardinality applies unchanged: what the operator holds is the
decision to enable this at all, on this agent, for these labels.

An issue that is labelled while the install is at capacity is the subject of a
separate decision, because the current at-capacity outcome is a reply and an
ack. See
[ADR-0146](0146-headless-capacity-is-a-wait-not-a-reply.md).

The `/github/webhook` route stops being a deploy-only surface. Its signature
check, body bound, and ping handling are unchanged, but the mental model "this
route deploys agents" becomes "this route carries GitHub", which is a thing a
later reader has to be told rather than infer.

## Alternatives considered

**A durable task table with a state machine.** Rejected. It duplicates the
tracker, requires reconciliation with it, and re-opens the ADR-0013 decision
against a durable workflow engine for a benefit — a backlog view — that the
tracker already renders better. The `queued → running → done` states would be a
second, lagging copy of an issue's own state.

**A polling loop over the tracker.** Rejected for v1. It is the natural remedy
for the stream's narrow first-creation loss window, but paying for a permanent
sweep to cover a window that opens once per install is the wrong trade. Held as
the named fallback if the window is ever observed to matter.

**A second, issue-specific ingress route.** Rejected. The hook route's HMAC
verification, delivery claim, receipt, backlog quota, and partition derivation
are the parts that are hard to get right, and they are all already correct. A
parallel route would reimplement them or drift from them.

**Parsing the issue body in the platform.** Rejected. It would make the
platform learn a tracker's schema and would put the ticket's meaning in a place
no eval can iterate on. Reading the ticket is bundle behaviour, which is where
it can be changed without a release.

**Deriving the partition from the delivery id.** Rejected, and it is the
failure mode ADR-0134 warns about explicitly: a per-delivery partition makes
every transcript single-use and grows retained per-partition state without
bound while delivering none of the continuity the split exists for.

## Realizing code path

Unimplemented. The named paths for the eventual work are the non-push arm of
`apps/api/src/curie_api/routers/github.py`, the enqueue path in
`apps/api/src/curie_api/routers/hooks.py`, the partition derivation in
`apps/api/src/curie_api/hook_partition.py`, and a label-selection column
alongside `agents.hook_partitions` in `apps/api/src/curie_api/models.py`.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act.

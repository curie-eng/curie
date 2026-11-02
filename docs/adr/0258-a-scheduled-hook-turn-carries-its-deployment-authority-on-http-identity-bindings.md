# 258. A scheduled hook turn carries its deployment authority on HTTP identity bindings

Date: 2026-11-02

Status: Accepted

Accepted 2026-11-02 with explicit maintainer approval from jw3329, including
merging alongside its realizing code path under ADR 0102, recorded in the
publishing pull request ([#5309](https://github.com/curie-eng/curie/pull/5309)).
The realizing code path is `curie_worker.kernel.identity.prepare` and
`curie_worker.kernel.identity.check`, with
`curie_worker.hook_runs.HookRunRecorder.scheduled_authority`.

Tracked in [#5304](https://github.com/curie-eng/curie/issues/5304).

## Context

[ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) makes a
hook a bundle-declared turn the system starts. Its authorization section says
the declared prompt is the standing authorization: the bundle author wrote it,
it was reviewed with the bundle, and it is in force while the deployment that
declares it is active. The kill switch, the budget and the operator pause
(`schedule_controls`) are fail closed for every fire.

[ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
section 9 and
[ADR 0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
introduced HTTP identity for channel callers. Both state that hook and schedule
authorization remain separate contracts, and ADR 0220 adds that a human team
never grants a machine caller anything.
[ADR 0254](0254-http-identity-applies-to-the-channel-identities-an-operator-registers.md)
decides which bindings require an HTTP identity.

The realization did not keep that separation. The worker's
`kernel.identity.prepare` refuses every context-free turn whose binding has
`http_identity_required`, whatever its source. The cron scheduler
(`curie_worker.cron_loop.CronSchedulerLoop`) admits a due slot, inserts its
`hook_runs` row and enqueues a `TurnSource.CRON` turn with no identity context,
so every targeted scheduled hook on such a binding is admitted and then refused
before execution (#5304). The separate contract the earlier ADRs reserved was
never written. This ADR writes it.

The issue proposes recording an authenticated principal's authority when the
hook is created or enabled and revalidating it at fire time. No such act exists
today: a hook is enabled by deploying a bundle that declares it, through the
git flow or the operator API, and an operator pause and resume acts on it under
the operator API key. There is no person whose invocation authority the
schedule could inherit without a new grant surface, and inheriting one would
contradict ADR 0220 section 9.

## Decision

1. **A scheduled turn's invocation authority is its deployment authority.** On
   a binding that requires HTTP identity, a turn the platform started for a
   declared cron hook needs no HTTP principal. It needs the deployment
   authority of ADR 0099, verified as decision 2 states. HTTP identity continues
   to govern every turn a channel caller starts.

2. **The authority is verified from durable state, never read from payload
   fields.** The kernel accepts a context-free `TurnSource.CRON` turn on an
   HTTP-required binding only when all of these hold, read from the database at
   the time of the check:
   1. its `HookRunRef` resolves to an open `hook_runs` row;
   2. the row's agent is the agent the binding classification resolved;
   3. the row's version is the version of the agent's active deployment as the
      scheduler and the binding resolver rank it (prod before dev, then the
      most recent).
   Only platform admission writes a `hook_runs` row: the scheduler, from the
   current version's declared cron triggers, and the API key authenticated hook
   fire route, after reading the same declaration. A writer that can append to
   the turn stream cannot create one. The open row for the current version is
   therefore the durable proof that the deployment declares the hook and that
   the platform started this run. The `cron:<name>` author, the `source` field
   and the event id are routing data; none of them alone, or together,
   satisfies this check. A turn that fails it is a definitive refusal, and its
   run closes as `failed` with reason `identity_refused`, as
   [#5305](https://github.com/curie-eng/curie/issues/5305) already realized.

3. **Revocation takes effect at every execution boundary.** The kernel repeats
   the decision 2 check at each later boundary where it checks an HTTP turn
   (`claim_or_resume`, `start`, `steer`), and also classifies the route again:
   the binding must keep the tenant, agent, binding id and generation it had at
   `prepare`. Replacing the deployment, closing the run or changing the binding
   between admission and any boundary refuses the turn before the next effect.
   The operator pause, the kill switch and the budget keep their existing fail
   closed guards on every hook turn; this decision neither relaxes nor
   duplicates them.

4. **The scheduler and the kernel agree by construction.** The scheduler admits
   a slot only from the current version and writes that version on the run row,
   which is exactly what decision 2 checks. It enqueues no turn that the
   predicate would refuse at admission, so it needs no separate check.

5. **An operator manual fire uses the same authority.** A fire through the API
   key authenticated hook fire route enqueues the same `TurnSource.CRON` turn
   with its own run row. It is checked as in decisions 2 and 3; the operator
   credential does not stand in for an HTTP principal.

6. **A scheduled turn raises no HTTP-selected approval.** HTTP approval
   continuity needs the original invocation context to seal the approval, and
   a scheduled turn has none. When such a turn reaches a gated tool on an
   HTTP-selected agent, the API refuses the legacy creation with
   `unsupported_approval`, so the gate fails closed and the run records
   `failed`, as ADR 0099 treats a hook that cannot pass a gate. The deployment
   authority never answers or creates an approval. Approvals for scheduled
   turns need their own continuity decision.

## Realizing work and acceptance evidence

[#5304](https://github.com/curie-eng/curie/issues/5304) tracks the
implementation, which lands with this decision. The realizing code paths
are:

1. `curie_worker.kernel.identity.prepare` and `curie_worker.kernel.identity.check`:
   the typed scheduled authority of decisions 1 to 3, read from durable state
   at `prepare`, `claim_or_resume` and `start`.
2. `curie_worker.hook_runs.HookRunRecorder.scheduled_authority`: the decision
   2 predicate as one read of the run row and the ranked active deployment.
3. `curie_worker.cron_loop.CronSchedulerLoop._admit` and
   `curie_api.routers.hook_fire`: the only writers of a run row, unchanged,
   whose rows satisfy the predicate by construction (decisions 4 and 5).
4. `curie_worker.kernel.hooks._close_hook_run_after_refusal`: the refusal
   closure, already realized by
   [#5307](https://github.com/curie-eng/curie/pull/5307) for
   [#5305](https://github.com/curie-eng/curie/issues/5305).

Acceptance evidence is a worker regression with real Postgres and Valkey and
the production binding classifier: a valid scheduled turn on an HTTP-required
binding runs once; each failed condition of decision 2 refuses before any
claim; a deployment replaced, a pause, a kill and a binding change between
admission and a later boundary of decision 3 refuse before the next effect; a
context-free turn that only claims `TurnSource.CRON`, without an open run row
for the bound agent, is refused; and a context-free turn on a binding that does
not require HTTP identity keeps its existing behavior.

## Consequences

- Scheduled hooks work on HTTP identity bindings with the authority ADR 0099
  already granted them, and lose it the moment the deployment, pause, kill
  switch or binding withdraws it.
- The kernel's HTTP identity gate gains a second, typed authority kind. Its
  tests must cover each failed condition of decision 2 and a revocation between
  admission and a later boundary of decision 3, with real Postgres and Valkey.
- A scheduled hook that relies on a gated tool keeps failing on an
  HTTP-selected agent until approval continuity covers scheduled turns
  (decision 6).
- No scheduled turn is attributed to a person. Audit attributes it to the
  agent, hook, deployment version and slot, as the run record already does.
- A deployment author can still make an agent act on an HTTP identity binding
  without an invocation team member present. That was already true under ADR
  0099 and is the point of a schedule; the team decides who may talk to the
  agent, not what the agent's own deployment declares.

## Alternatives considered

- **Record an enabling principal's HTTP authority and revalidate it at fire.**
  This is the shape #5304 suggests. Rejected: no principal enables a hook
  today, so it needs a new grant surface and store; it ties machine work to one
  person's team membership, which ADR 0220 section 9 rules out; and a person
  leaving the team silently stops every schedule they enabled. If the
  maintainers want a named accountable human behind every schedule, this ADR
  should be revised to that shape and ADR 0220 section 9 superseded explicitly.
- **Exempt `TurnSource.CRON` from the gate by source.** Rejected: the source
  and author are queue payload fields. Any writer that can append to the turn
  stream could mark a turn as scheduled and skip identity. Decision 2 checks
  durable state instead.
- **A service principal with a signed identity context minted at admission.**
  The scheduler would obtain an API-signed context for a non-human principal.
  Rejected for now: ADR 0246 denies non-human invocation until its own policy
  is qualified, and the deployment check gives the same guarantees without a
  new principal kind. It remains open as a later replacement.
- **Keep refusing and document schedules as unsupported on HTTP identity
  bindings.** Rejected: it leaves a supported feature broken on every binding an
  operator migrates, and contradicts the separate contract the earlier ADRs
  reserved. Until this ADR is accepted and realized, that is the current
  behavior, now with closed run records (#5305, fixed by #5307).

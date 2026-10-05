# 203. Automated remediation is a pre-qualified action the platform executes and verifies

Date: 2026-10-05

Status: Accepted

Maintainer jw3329 explicitly approved acceptance on 2026-10-05, with the
rulings recorded below, together with
[ADR 0121](0121-a-restore-is-the-connectors-own-verb-run-under-the-same-pinned-connector.md)
and [ADR 0124](0124-a-snapshot-is-sealed-to-the-connector-that-wrote-it.md).

This decision settles whether, and how, a turn started by an automated alert
source may change a system without a person approving each change. It weighs
five options and selects option D. It partially supersedes
[ADR 0190](0190-automated-hook-sources-cannot-widen-their-tool-access.md)
for hooks an administrator binds a remediation policy to, as described under
"Relationship to existing decisions". It leaves
[ADR 0191](0191-protected-hook-delivery-authority.md) and every
HOOK-SOURCE-POLICY invariant standing for the model turn itself.

Related: [#3603](https://github.com/curie-eng/curie/issues/3603) (read-only
automated turns), [#3527](https://github.com/curie-eng/curie/issues/3527)
(per-alert intake), [#1861](https://github.com/curie-eng/curie/issues/1861)
(the action ledger), [#1867](https://github.com/curie-eng/curie/issues/1867)
(the undo executor), [#3652](https://github.com/curie-eng/curie/issues/3652)
and [#3653](https://github.com/curie-eng/curie/issues/3653) (receipt
limitations and actor attribution).

## Context

### The goal

An SRE agent on Curie should, for an incident it did not hear about from a
person:

1. detect it from an automated alert source;
2. investigate it;
3. perform safe remediation automatically, within authority someone approved
   in advance;
4. ask a human only when that is actually necessary;
5. verify recovery after acting, and report the outcome.

The SRE example is the motivating bundle, not the subject. Every bot that
reacts to a machine signal (a deploy watcher, a queue drainer, a certificate
renewer) needs the same five steps, and steps 3 to 5 need authority and
evidence that a bundle cannot supply for itself.

### What exists

**Automated source authority.** ADR 0190 lets an administrator bind a
mandatory `read-only` policy to a named hook. The source cannot widen it, and
a read-only turn refuses mutating and unknown tools and approval requests
before any effect or card (HOOK-SOURCE-POLICY-7). ADR 0191 selects the
mechanism: a scoped per-hook HMAC credential, a separately authorized Valkey
delivery lane, protected workers, and a closed set of qualified runner,
bundle and connector artifacts. Its connector qualification covers "actual
read behavior and least privilege credentials", so the qualified protected
runtime holds no write authority by construction. Realizing work is in
progress under #3603 (for example
[`apps/worker/src/curie_worker/hook_source_guard.py`](../../apps/worker/src/curie_worker/hook_source_guard.py)).

**Unrestricted hooks.** A hook without that binding keeps the authorization
of [ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md): the
agent's ordinary tools, with approval gates not suspended. The alert body is
wrapped as untrusted data by
[`apps/api/src/curie_api/routers/hooks.py`](../../apps/api/src/curie_api/routers/hooks.py),
but the model that reads it holds every ungated tool. The SRE example's intake
guide states plainly that its no-mutation rule is standing prompt policy, not
runtime enforcement.

**The per-turn access contract.** TOOL-ACCESS in the
[ACI producer interface](../interfaces/aci-producer/INTERFACE.md) has one
value, `read-only`. A read-only turn never requests an approval and never
ends `awaiting-approval` (TOOL-ACCESS-3, TOOL-ACCESS-6). The value is an enum,
and adding a second one is a breaking change to a frozen contract
(TOOL-ACCESS-2).

**The approval gate.**
[`runner/src/curie_runner/approval.py`](../../runner/src/curie_runner/approval.py)
holds a permission gate (configured tools are denied until approved) and a
policy gate (`request_approval`). Approvers are authenticated principals
([ADR 0106](0106-an-approver-is-an-authenticated-principal.md)) resolved in
the API ([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)).
The post-approval grant is one-shot and tool-name scoped, not argument scoped
([ADR 0035](0035-one-shot-post-approval-allowance.md); argument binding is
open as [#1956](https://github.com/curie-eng/curie/issues/1956)). An approved
call on a resumed turn may therefore run with arguments the approver did not
see.

**Side effects and the action ledger.**
[`runner/src/curie_runner/side_effects.py`](../../runner/src/curie_runner/side_effects.py)
classifies every tool outside a declared read-only set as side-effecting.
[ADR 0117](0117-a-tool-that-changes-the-world-reports-what-it-changed.md)
records each side-effecting call as an action with its prior state, target
and outcome, rules on undo, and refuses an undo when the world has moved.
Nothing can perform the undo yet: the executor is
[ADR 0121](0121-a-restore-is-the-connectors-own-verb-run-under-the-same-pinned-connector.md),
a Draft blocked under #1867 until it was accepted together with this decision. A ledger row
(`apps/api/src/curie_api/models.py::AgentAction`) can name the approval that
gated it and nothing else; there is no field for an authority that is not a
person's approval.

**Stops.** The per-agent kill switch
([`apps/worker/src/curie_worker/killswitch.py`](../../apps/worker/src/curie_worker/killswitch.py))
interrupts live turns and blocks new ones, and ADR 0099 makes it and the
budget fail closed for hook fires.

### What is missing, per goal

| Goal | Today | Gap |
|---|---|---|
| 1. Detect | Signed hooks; the protected lane under ADR 0191. | None at this level. |
| 2. Investigate | A read-only turn under ADR 0190. | None at this level. |
| 3. Remediate safely | Forbidden on a restricted hook. On an unrestricted hook, any ungated tool runs on the model's word and any gated tool waits for a person. | No authority between "nothing" and "everything the agent holds", and no argument bound enforced outside the model. |
| 4. Ask only when needed | A read-only turn cannot ask at all. | No path from a protected investigation to an argument-bound human decision. |
| 5. Verify and report | The receipt lists what was called. | No platform notion of recovery, so the outcome is model prose. |

### Why this needs the platform

A bundle can write a runbook, but it cannot make one binding. ADR 0190 already
records why: a prompt that tells the model to avoid an operation is not a
permission boundary, and the alert body that reaches the model is attacker
reachable wherever the alert text is. Each of the following must hold even
when the model is wrong or steered, so each must live outside the model and
outside the bundle the model reads:

* who may grant automatic authority, and that the source cannot widen it;
* the bound on each action's arguments and target;
* the ledger record and the undo path;
* the verdict on whether recovery happened;
* rate limits, a circuit breaker and a kill switch.

If each bot implemented these in its own connector or skill, every adopting
installation would review a different, unenforced variant, and a single
platform answer is cheaper to qualify once. That is a platform contract,
which is why this is an ADR and the SRE bundle's runbooks are not.

## What "recovery verified" must mean

Every option below that acts must also verify, so the definition comes first.
A remediation is **verified** only when all of the following hold:

* **Observed, not asserted.** The verdict comes from a read of a declared
  signal (a metric query, a probe, the alert source's own resolved state),
  evaluated against a declared predicate. The model's reply, the acting
  connector's success reply, and the absence of new alerts are not evidence.
* **Independent of the actor.** The signal is read through a different
  connector or source than the one that performed the action. A connector
  that reports its own write as healthy is grading its own work.
* **Bounded in time.** The verifier has a declared deadline and a minimum
  settle interval. A signal that is healthy only before the settle interval,
  or never inside the deadline, is a failure.
* **Explicit in every outcome.** The outcome is one of `verified`,
  `not-recovered`, `verifier-unavailable`, or `superseded` (a person or
  another action changed the target first). Only `verified` is success.
  `verifier-unavailable` is never read as success. Every outcome other than
  `verified` produces a failure report to the bound route and, where the
  policy allows it, an undo attempt under ADR 0117's conflict rule.

The same independence applies before acting: the condition the action is
meant to fix should be confirmed by a declared precondition read, not taken
from the alert body, because the alert body is untrusted input.

## Options

### A. Automated turns stay read-only; every action is a human-approved proposal

The automated turn investigates under ADR 0190 and ends by emitting
structured proposals. Each proposal becomes an approval card on a bound
route. An approver's decision starts a separate, human-authorized execution
that is not the protected session (ADR 0191 already requires that human
execution happen in its own session).

* **ADR 0190 and 0191:** no supersession. A read-only turn cannot request an
  approval (TOOL-ACCESS-3), so the proposal must leave the turn as output and
  become a card afterwards. That is a new platform primitive, but it widens
  nothing in the protected lane.
* **Prompt injection:** the worst an injected alert achieves is a misleading
  proposal in front of a person. The approval must bind the exact arguments
  the person saw; today's tool-name grant (ADR 0035) would let a resumed
  model turn substitute others, so A depends on #1956 or on executing the
  approved call without a model.
* **Failure modes:** time to recovery is bounded by human response, which is
  the very latency automation exists to remove. Routine, repetitive proposals
  at night train approvers to approve without reading, which converts the
  gate into a ritual. Goal 3 is not met; goal 4 degenerates to "always ask".
* **Still needed:** the verifier, because an approved action can fail too.

### B. An administrator-bound allowlist the automated turn may call directly

An administrator binds, to a named hook, a closed allowlist of pre-qualified
reversible actions. The automated turn runs under a distinct effective policy
that is never wider than that allowlist: allowlisted tools execute without a
person, inside per-action argument and target bounds enforced by the runner's
permission callback; everything else stays refused. Every call requires a
ledger record, an undo capability, and post-action verification.

* **ADR 0190 and 0191:** supersedes HOOK-SOURCE-POLICY-7 for those hooks,
  because the model turn now holds mutating tools. The qualified protected
  runtime must hold write credentials, so ADR 0191's read-only connector
  qualification no longer describes it, and compromise of the protected lane
  now yields writes. The new effective policy is a second `ToolAccess` value,
  a breaking frozen ACI change under TOOL-ACCESS-2 that must land as its own
  reviewed contract first.
* **Prompt injection:** the model that reads the attacker-reachable alert
  holds the write tools. Injection can choose which allowlisted tool to call,
  with which in-bound arguments, how many times within the rate limit, and in
  what sequence with the results of earlier calls. The argument bounds are the
  only real fence, and each tool's bounds must be expressed in the policy.
* **Failure modes:** argument bounds expressed per tool approach the mapping
  language ADR 0117 rejected. A multi-step turn can compose individually
  bounded actions into an unbounded effect. Verification after a model-driven
  sequence must attribute recovery across several actions.

### C. Hybrid with model execution: A by default, B for qualified actions

The turn is read-only for everything except actions qualified as reversible
with a declared verifier, which it may call directly as in B. Anything else
becomes a proposal as in A.

* **ADR 0190 and 0191:** the same supersession and the same breaking ACI
  change as B, because the model still holds the qualified write tools.
* **Prompt injection, failure modes:** B's, restricted to the qualified set;
  A's for the rest. It meets all five goals, and it keeps B's central weakness:
  the reasoning that read the untrusted payload is the reasoning that pulls
  the trigger.

### D. Hybrid with platform execution: the model nominates, the platform acts

The automated turn stays read-only exactly as ADR 0190 and ADR 0191 define it.
It ends by emitting structured remediation nominations: an action name from
the hook's administrator-bound remediation policy, and its arguments. After
the turn, outside the model and outside the protected runtime, the platform
evaluates each nomination:

1. **Qualified and in bounds:** the action is in the policy, its arguments and
   target satisfy the policy's bounds, its precondition read confirms the
   condition, and the rate, concurrency and circuit-breaker limits allow it.
   The platform executes it through the connector's own verb in a sandbox under
   that connector's binding (ADR 0121's executor), records it in the ledger
   with the policy as its authority, runs the verifier, and reports.
2. **Anything else:** the nomination becomes an argument-bound approval
   request on the policy's route. A person decides; the platform executes the
   exact approved call the same way, without a model, and verifies it.

* **ADR 0190 and 0191:** the model turn, its tool access, its lane and its
  qualified read-only artifacts are unchanged; no new `ToolAccess` value is
  needed. What changes is the premise in ADR 0190's Context that an automated
  source changes no system: an administrator can now bind a remediation policy
  whose effects follow the read-only turn. That is a narrow, explicit
  partial supersession, not an edit.
* **Prompt injection:** an injected alert can at most nominate an allowlisted
  action with in-bound arguments, once per turn, subject to an independent
  precondition read and the rate limit. It cannot chain calls, observe results
  mid-turn, or reach any tool the policy omits. The executor never reads the
  alert body.
* **Failure modes:** it needs the undo executor ADR 0121 has not yet been
  accepted for, and that ADR's own prerequisite. A nomination is coarser than
  an interactive sequence, so remediations that need several dependent steps
  must be qualified as one composite action or go to a person. Execution
  happens after the turn ends, so the investigation's reply and the action's
  report are two messages unless the transport supports completing one.

### E. Curie investigates and verifies; remediation stays outside the platform

Curie never executes automated remediation. Installations keep alert-driven
self-healing in systems built for it (orchestrator health checks and restart
policies, autoscalers, runbook automation), and Curie investigates, reports,
and verifies the outcome of what those systems did.

* **ADR 0190 and 0191:** unchanged.
* **Failure modes:** goal 3 is not met by the platform. Each installation
  reintegrates the remaining steps, and Curie's investigation cannot inform
  the action that follows it. It remains the right answer for remediation that
  already has a deterministic owner, and nothing in D prevents an installation
  from choosing it.

## Cross-cutting requirements

Whichever option acts automatically (B, C or D) must also supply:

* **Blast radius.** The policy bounds each action's target (an explicit
  namespace, label selector or resource list) and magnitude (for example a
  maximum replica delta), and the connector credential's own grant is the
  outer ceiling. One automatic action per incident per target before a person
  is involved; a second needs a decision.
* **Rate limits and circuit breaker.** Per policy, per action and per target
  windows, enforced where the action is admitted. A `not-recovered` or
  `verifier-unavailable` outcome opens a breaker for that action and target
  that only a person with policy authority can close.
* **Kill switch.** The existing per-agent kill switch stops remediation with
  everything else. A separate disarm switch per policy returns the hook to
  option A behavior (nominations become proposals) without stopping
  investigation, so an operator can keep the bot reporting while it stops
  acting.
* **Audit.** Every automatic action's ledger record names the policy and its
  generation as its authority, the administrator principal who bound that
  generation, the source delivery that started the turn, the precondition and
  verifier reads, and the outcome. #3653's actor attribution must represent a
  policy actor explicitly rather than an empty human field.
* **Authority over the policy.** The source cannot write it, exactly as ADR
  0191 keeps source policy out of the delivery body. Whether the ordinary
  administrative key suffices or the policy needs ADR 0191's independent
  provisioning authority is settled by REMEDIATION-13.

## Decision

**Option D**, with option A as its fallback path and option E remaining
available per installation.

D is the only option that meets all five goals without giving the model that
reads untrusted alert text a write tool. B and C put write authority inside
the turn that an injected payload can steer, require a breaking frozen ACI
change, and invalidate the read-only qualification that ADR 0191 spent its
whole design on. A is safe but does not meet goal 3, and its routine cards
erode the approval gate it relies on. D reuses the ledger, the undo ruling
and (once accepted) ADR 0121's executor for both automatic and approved
actions, so the platform builds one deterministic execution path instead of
two. It also turns ADR 0117's undo from a convenience into a precondition:
an action that cannot be put back cannot be executed without a person.

The honest cost is that strict reversibility excludes some of the most common
remediations. A rolling restart, for instance, is reported as not undoable
under ADR 0117. Ruling 1 below admits a bounded class of actions that are
idempotent and harmless to repeat but not reversible, because without it D
would exclude the remediations operators reach for first.

### Clauses

* **REMEDIATION-1.** Only platform administrative authority can bind a
  remediation policy to a named hook. The source, its credential and the
  delivery body cannot create, widen or reactivate one.
* **REMEDIATION-2.** The automated model turn remains `read-only` under ADR
  0190 and ADR 0191. A remediation policy never changes the turn's tool
  access or its qualified runtime.
* **REMEDIATION-3.** The turn's nominations are data. Only the platform
  evaluates them, after the turn, against the policy generation that was active
  when the delivery was admitted.
* **REMEDIATION-4.** An action qualifies for automatic execution only if it is
  reversible under ADR 0117 or admitted by REMEDIATION-11, its arguments and target are within the policy's bounds, and it
  declares a precondition read and a verifier.
* **REMEDIATION-5.** The platform executes a qualified or approved action
  through the connector's own verb under that connector's binding, with no
  model in the path.
* **REMEDIATION-6.** Every executed action produces exactly one ledger record
  naming its authority: a policy generation or an approval.
* **REMEDIATION-7.** A nomination that does not qualify becomes an approval
  request bound to its exact canonical arguments; approval executes those
  arguments and no others.
* **REMEDIATION-8.** Recovery is verified only as defined in this ADR, and
  every outcome other than `verified` is reported as a failure.
* **REMEDIATION-9.** Rate limits, the circuit breaker, the per-policy disarm
  and the per-agent kill switch are enforced where actions are admitted and
  fail closed.
* **REMEDIATION-10.** Receipts and telemetry distinguish nomination,
  admission refusal, execution, verification outcome, undo and escalation.
* **REMEDIATION-11.** An action that is not reversible qualifies only if it is
  idempotent, its effect is bounded to the policy's declared target, and it
  declares a verifier. It runs at most once per incident per target without a
  person; any outcome other than `verified` escalates to a person immediately.
* **REMEDIATION-12.** A nomination leaves the turn only as a closed structured
  block in the turn's final output, parsed and validated by the platform after
  the turn ends. It is data: no ACI, plugin-format or `ToolAccess` change, and
  no tool the turn calls.
* **REMEDIATION-13.** The existing platform administrative authority binds a
  remediation policy, under the same trust model as ADR 0190 source policy;
  compromise of that key stays outside this decision's guarantee.
* **REMEDIATION-14.** On any outcome other than `verified`, the platform reports
  and escalates and does not undo automatically. Undo of a policy-executed
  action is an approval-gated decision by an approver on the policy's route.
* **REMEDIATION-15.** A verifier is a deterministic predicate over a declared
  read through a connector and credential distinct from the acting ones. No
  model turn judges recovery.

## Relationship to existing decisions

* **ADR 0190.** Accepted, so immutable. This decision does not change its
  invariants for the model turn. It partially supersedes its premise that an automated
  source changes no system, only for hooks bound to a remediation policy. ADR
  0190 carries a back link under
  [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md);
  its body is untouched. B or C would instead supersede HOOK-SOURCE-POLICY-7
  outright.
* **ADR 0191.** Unchanged under D: the executor runs outside the protected
  lane and its qualified artifacts. Under B or C its read-only connector
  qualification would need a successor.
* **ADR 0099.** Unchanged. Unrestricted hooks keep ordinary tools and
  approval gates; a remediation policy applies only to restricted hooks.
* **ADR 0117.** This decision amends decision 3 (an ungated action's undo is
  ungated) for policy-authorized actions, whose undo authority must be stated
  explicitly (REMEDIATION-14). Its conflict check is reused unchanged.
* **ADR 0121 and ADR 0124.** Accepted together with this decision. They supply
  the executor and the sealed snapshot; REMEDIATION-5 extends that executor from
  restores to forward actions. No automatic execution ships before it.
* **ADR 0035.** Its tool-name grant remains for ordinary human turns. D's
  approved proposals bind arguments and run without a model, so they do not
  use it.
* **TOOL-ACCESS.** Unchanged under D. B and C need a second value, a breaking
  change under TOOL-ACCESS-2.

## Rulings

The maintainer ruled on the questions this decision raised as a Draft:

1. **Non-reversible actions.** Admitted in a bounded class (REMEDIATION-11).
2. **Form of the change to ADR 0190.** Partial supersession of its premise, with
   a back link on ADR 0190 under ADR 0045; its invariants stand.
3. **How a nomination leaves the turn.** As a structured block in the final
   output (REMEDIATION-12), so no frozen contract changes.
4. **Who binds a remediation policy.** The existing administrative authority
   (REMEDIATION-13).
5. **Qualification evidence.** A reversible action needs an observed restore
   round trip including the conflict refusal; a REMEDIATION-11 action needs an
   observed repeat with no additional effect; every action needs its verifier
   observed both passing and failing, and its worst case within bounds recorded.
6. **Undo of an automatic action.** Approval gated on the policy's route
   (REMEDIATION-14).
7. **On `not-recovered`.** Report and escalate; no automatic undo
   (REMEDIATION-14).
8. **Verifier.** Deterministic and independent of the actor (REMEDIATION-15).
9. **Default limits.** One automatic action per incident per target, at most
   three automatic actions per policy per hour, and a circuit breaker that only
   an administrator with policy authority closes. A policy may tighten these,
   never loosen them.
10. **Release train.** The realizing feature work targets `next`; this record
    lands on `main` with the decisions it amends.

## Realizing work

Follow-up issues to create (titles only):

| Issue title | Clauses |
|---|---|
| Remediation policy: administrator-bound, generation-tracked, source cannot write | REMEDIATION-1, REMEDIATION-9 |
| Read-only turns emit structured remediation nominations | REMEDIATION-2, REMEDIATION-3 |
| Admit a nomination against the policy: bounds, precondition read, limits | REMEDIATION-3, REMEDIATION-4, REMEDIATION-9 |
| Qualification evidence for adding an action to a remediation policy | REMEDIATION-4 |
| Accept and build the ADR 0121 executor for forward actions as well as restores | REMEDIATION-5 |
| Ledger records carry a policy or approval authority and a verification outcome | REMEDIATION-6, REMEDIATION-10 |
| Argument-bound approval for a non-qualified nomination, executed without a model | REMEDIATION-7 |
| Independent, deadline-bound recovery verifier with explicit failure outcomes | REMEDIATION-8 |
| Per-policy disarm, circuit breaker and rate limits fail closed | REMEDIATION-9 |
| Receipts and telemetry for nomination, refusal, execution, verification and undo | REMEDIATION-10 |
| Policy actor attribution in action records (extends #3653) | REMEDIATION-6 |

## Alternatives considered

* **Keep every automated turn read-only (option A alone).** Safe, and the
  fallback inside D, but it does not meet goal 3 and its routine cards erode
  approval quality.
* **Model-executed allowlist (options B and C).** Rejected as the
  recommendation because the turn that reads untrusted alert text would hold
  write tools, the protected runtime would need write credentials, and a
  breaking `ToolAccess` change would be required.
* **Remediation outside the platform (option E).** Kept as an installation
  choice, rejected as the platform answer because it leaves goal 3 to each
  installation.
* **Per-bot remediation code.** A connector or skill that enforces its own
  limits is unenforced from the platform's view, differs per bot, and cannot
  hold authority an untrusted payload is unable to widen.
* **A time-boxed standing approval from on-call.** A person approves a class of
  action for some hours. This is option B or D with a human-signed, expiring
  policy rather than an administrative one; it may be a later refinement of
  REMEDIATION-1, not a separate mechanism.
* **Self-approval as an operator opt-in
  ([#1054](https://github.com/curie-eng/curie/issues/1054)).** Lets a requester
  approve their own action; it does not apply to a turn no person started.
* **Verification by the model's own judgement.** Rejected: it is the actor
  grading itself, from a context the alert body can steer.

## Consequences

With this decision, an administrator can let an automated source fix a
bounded class of problems without a person in the loop, and every such fix is
recorded, reversible, verified by an independent signal and stoppable. The
model turn keeps the guarantees ADR 0190 and ADR 0191 established.

The work is gated on ADR 0121's executor, which is itself blocked, so no
automatic remediation is available until that lands. Remediations that are not
reversible and outside REMEDIATION-11, or that need several dependent steps,
still go to a person unless a qualified composite action admits them. The platform takes on a
policy store, an admission evaluator, a verifier, and new ledger and receipt
fields, each of which needs its own reviewed contract; any change to the frozen
ACI or plugin format follows the frozen-contract procedure before dependent
work merges.

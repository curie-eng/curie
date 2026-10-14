# Writing remediation nominations

A protected hook's turn can propose one bounded fix without calling a tool. It
writes a **nomination block** at the end of its final answer, and the platform,
not the model, decides what happens next: refuse it, ask a person, or run it
and check that it worked
([ADR-0203](adr/0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md),
[specification](superpowers/specs/2026-10-07-automated-remediation.md)).

This is the author-facing contract for whoever writes the skill or prompt of an
agent behind a protected hook. The administrator's half, the policy that says
which actions exist and how far each may go, is in
[Automated remediation](operations.md#automated-remediation).

A nomination is data. It is not a tool call, it carries no authority, and the
turn stays `read-only` ([ADR-0190](adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md)).
The model names an **action the policy already declares** and the arguments to
run it with. It cannot name a tool, a connector, a kind, a target outside the
policy's list, or evidence. Anything the policy does not declare is refused.

## The block

Put exactly one block in the final output. It opens with a line that is exactly
three backticks followed by `curie-remediation`, holds one JSON object, and
closes with a line that is exactly three backticks:

````text
Investigated: the API deployment is saturated, error ratio is high.

```curie-remediation
{"version": 1, "nominations": [
  {"action": "scale-out-api",
   "arguments": {"namespace": "example-ns", "deployment": "example-api", "replicas": 4},
   "reason": "error ratio has been above the threshold for ten minutes"}
]}
```
````

The grammar, enforced by
[`apps/api/src/curie_api/remediation_nominations.py::parse_nomination_block`](../apps/api/src/curie_api/remediation_nominations.py)
and frozen in `tests/vectors/remediation-nomination.json`:

| Rule | Value |
| --- | --- |
| Fences | Opening line exactly `` ```curie-remediation ``, closing line exactly `` ``` ``. Nothing else on either line. |
| Blocks | One per final output. Two blocks, an unclosed block, or text after the closing fence inside the submitted block make the whole block malformed. |
| Size | At most 16384 bytes of UTF-8, fences included. |
| `version` | The integer `1`. |
| `nominations` | One to five entries. |
| Entry keys | Exactly `action` and `arguments`, plus an optional `reason`. A `tune` entry uses the change shape below instead. Any other key makes the whole block malformed. |
| `reason` | Optional string of at most 500 characters. |
| JSON | No duplicate key at any depth, no `NaN` or infinity, no NUL character, no string that is not valid UTF-8. |

The block is valid or it is not, as a whole. One bad entry does not rescue the
others, and a malformed block produces one `nomination_malformed` record and no
action. Do not write the fence line anywhere else in the answer, including in
explanatory prose or examples: a second opening fence makes the output
ambiguous and the block is refused.

A block is read only from a turn that ends `done`. A turn that is awaiting
approval or failed submits nothing.

**Capture.** The platform removes the block from every reply the user sees,
including streamed text, and submits it to
`POST /v1/internal/remediation/nominations` with the turn's event id
([`apps/api/src/curie_api/routers/remediation_nominations.py::submit_remediation_nominations`](../apps/api/src/curie_api/routers/remediation_nominations.py)).
The capture half is the protected worker's runner client wrapper
(AUTOMATED-REMEDIATION-6, plan task 14), which reads the same vector. Until it
ships no turn can submit a block, though the route and the parser exist. The
block still stays in the protected conversation's own history, and a later turn
that repeats an old block makes a new submission, evaluated afresh. The
agent, hook, policy generation and reply surface are resolved by the platform
from the protected delivery's binding, never from the block. The first accepted
submission for an event wins, so a retried turn cannot add nominations.

## Arguments

For a `remediate` or `prevent` action, `arguments` is an object whose keys are
**exactly** the keys the policy's argument schema declares, each of the declared
type (`string`, `integer`, `number` or `boolean`; a boolean is not an integer).
A missing key, an extra key or a wrong type is `arguments_schema_mismatch`.

A value of the right type that is outside the policy's allowed set or numeric
range is **not** refused: it becomes an approval request carrying exactly those
arguments, so a person can decide. The same holds for a target value that is not
a literal member of the action's allowed target list. Do not try to be clever
with the target: `"example-api "` or a differently quoted selector is not the
listed value.

Two entries with the same action and the same canonical arguments (sorted keys,
compact separators, the executor's canonical form) are duplicates; the later
one is refused `nomination_duplicate`.

## Kinds

The policy declares each action's `kind`. The model never chooses it, only the
action name.

| Kind | What it is | What the platform does |
| --- | --- | --- |
| `remediate` | Fix the condition now (scale, restart, roll back). | Walks the admission order. If every check passes it runs with no person, verifies recovery through a different connector and reports. Otherwise it becomes an approval request that names the failed check. |
| `prevent` | Make the alert not recur (a capacity or configuration change). | Always becomes an approval request (`not_automatic`). An approved one is executed exactly as stated and verified like a remediation. Never automatic. |
| `tune` | Change an alert rule's threshold, duration, grouping, dedupe or retire it. | Always becomes an approval request. The platform renders the diff and the evidence on the card from declared reads. Approving it records the decision and ends the request `refused` with `tune_execution_not_automated`: no write is made. |

### The `tune` shape

A `tune` entry carries the rule and the structured change instead of
`arguments`:

```json
{"action": "retire-duplicate-rule", "rule": "example-rule-a", "field": "retire",
 "value": {"duplicate_of": "example-rule-b"}, "reason": "fires with example-rule-b every time"}
```

`rule` must be a rule the action declares, `field` one of `threshold`,
`for_duration`, `group_by`, `dedupe` or `retire` that the action's change schema
declares, and `value` of the declared type. For `retire`, `value` is exactly
`{"duplicate_of": "<rule>"}` naming a different rule from the action's declared
duplicate targets. Supply no diff text: the platform renders it from the
structured change and the rule's current definition. The same change written as
`"arguments": {"field": ..., "rule": ..., "value": ...}` parses to the same
arguments. A recurring series of identical requests produces one open request.

## What the platform does with a nomination

Every well-formed entry is checked in this order, and the first failing check
decides
([`apps/api/src/curie_api/remediation_admission.py::admit_nominations`](../apps/api/src/curie_api/remediation_admission.py)):

1. remediation and the executor are enabled;
2. the agent is not stopped;
3. the delivery's policy generation is still the current one;
4. the policy is armed;
5. the action is a `remediate` action declared `automatic`;
6. the action has a valid qualification record for the connector's current digest;
7. the verifier is independent of the acting connector;
8. the arguments and target are within the policy's bounds;
9. a `reversible` action's connector can still restore it;
10. no open breaker for that connector, tool and target;
11. the limits hold (at most one automatic action leaves one turn, one per target
    is live, one per target per incident window, and the hourly policy and
    action limits);
12. the action's declared precondition read, taken from the live system and
    never from the alert or from your `reason`, holds.

Failing any of checks 3 to 12 raises an approval request whose card names the
check; failing check 2 ends the nomination `agent_stopped` and asks nobody. An
unreadable policy, breaker, limit or capability row fails closed to an approval
request, never to execution.

If more than one entry in a turn could run automatically, only one does;
the rest become approval requests (`turn_limit`). An identical request that is
already pending attaches to it instead of raising a second card.

On an approval request, the card shows the platform-rendered action and
arguments, the failed check, and the precondition read's observed value when
one was taken. Your `reason` is shown as escaped plain text labeled unverified
model text, and the alert body is never shown. Approving rebuilds the call from
the stored nomination, not from the card, and refuses `arguments_mismatch` if
the approval's tool or argument hash differs.

Verification follows every executed action, automatic or approved. Outcomes are
`verified`, `not-recovered`, `verifier-unavailable` and `superseded`. Anything
but `verified` opens a breaker, writes an escalation and, for an undoable
record, offers undo as an approval. Nothing undoes a policy-executed action
without an approving person.

## Refusals

A record that ends `refused` without asking anyone has no bounded action for a
person to approve:

| Code | Meaning |
| --- | --- |
| `nomination_malformed` | The block is outside the grammar. Nothing in it is evaluated. |
| `unknown_action` | The action is not declared by the hook's policy. |
| `nomination_duplicate` | A later entry repeats an action with the same canonical arguments. |
| `arguments_schema_mismatch` | A key outside the schema, a declared key missing, or a wrong type; for `tune`, an undeclared rule or field. |
| `agent_stopped` | The agent's kill switch is set or unreadable. |
| `reply_surface_unavailable` | The delivery recorded no place to post a card, so a person cannot be asked. |
| `tune_execution_not_automated` | An approved `tune` request. It executes nothing. |

The submission route itself refuses without writing a row: `remediation_disabled`
(409, the feature is off), `not_protected_event` (404, the event has no
protected binding) and `nomination_conflict` (409, a different block for an
event that already submitted one). The closed code lists are frozen in
`tests/vectors/remediation-codes.json`.

## Rules that are easy to get wrong

- **Nominate only what you saw.** The platform rechecks the condition with its
  own read, so a nomination for something the alert claims but the system does
  not show goes to a person with `precondition_not_met`.
- **Never copy alert text into `arguments` or `reason` as if it were a fact.**
  The alert is untrusted input. `reason` is stored for the record and never used
  as evidence.
- **Propose nothing when unsure.** Omit the block. A turn with no block submits
  nothing and is an ordinary investigation.
- **The first release never changes alert rules.** `tune` ends at the approval
  card. Do not promise the user the change will be applied.
- **Do not describe the nomination as done.** At nomination time nothing has
  run. What ran, and whether it worked, comes from the platform's own record,
  never from your answer.

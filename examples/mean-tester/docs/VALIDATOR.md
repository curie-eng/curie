# Agent validator: slice 1 and proposed action validation

<!-- @spec VALIDATOR-1 -->
A ship decision needs both a target-owned fixed acceptance suite and realistic
user scenario campaigns. Slice 1 implements planning and read-or-ask grading;
it never authorizes a target action, uploads an attachment, resolves a card,
invokes target state tools, injects a fault, or restores state. The existing
manifest and ADR 0181 policy remain unchanged. An installation mark is proposed
metadata, not implemented authorization. A slice 1 report cannot issue full GO.

## Modes and the installation mark

**Read-or-ask** is available for an existing installation in production or test.
Every sent message must remain safe under the current policy, even if the target
has approval gates. Recorded exchanges can describe actions without performing
those actions. No separate validation installation is required.

**Action validation**, proposed for slice 2, operates only on an existing
installation an operator has marked for testing. The platform must attest the
mark, target installation identity, permitted scope and run identity at execution
time. A name containing “test”, a request, or bundle prose grants no authority.
Snapshot before the first action, preserve base content, restore after the run,
and verify restored content. Never wipe. The operator schedules around demos;
this design adds no scheduler or installation lock.

## Target-owned acceptance suite

<!-- @spec VALIDATOR-2 -->
The separate file `acceptance/cases.json` belongs to the target bundle. The
illustrative example and JSON Schema in this bundle's `acceptance/` directory
show the shape; they are not the acceptance criteria of another target. A request
may supply the suite and its source identity, or a listed repository supplies it
at the same immutable commit as the target specification. `evals/cases.json`
continues to be the frozen platform eval format: the tester's own evals measure
its grading ability, not whether a target may ship. Do not silently substitute
recorded eval inputs for live acceptance probes.

Version 1 declares a suite name, unique criterion IDs, and unique case IDs. Each
case records `probe`, `mode` (`read-or-ask` or `action`), `attachments`,
`expected_reply` properties, `card_action` (null, `approve`, `reject`, or
`click-as-non-approver`), `expected_state`, `criterion`, `priority` (P0/P1),
and positive integer `repeat`. Unknown fields, duplicate IDs, invalid criterion
references, empty cases, unsupported versions and invalid types are malformed.
An action-bearing probe is an action even if mislabelled `read-or-ask`.
Attachments, card actions and state assertions require slice 2; never send them
under slice 1. Plain pasted text is permitted only when the probe itself reads
or asks; do not change an attachment case into pasted text and count it covered.

Intake has explicit states: `READY`, `MISSING`, `MALFORMED`, and
`BLOCKED: slice 2`. Missing or malformed suites prevent a suite pass. Diagnostic
questions may continue, labelled exploratory. Run eligible fixed cases in file
order and all required repeats before invented probes; a failed answer check may
stop delivery, leaving unrun cases blocked, never passed. Keep exact probe text,
case ID, criterion, repeat index, source commit and observed thread evidence.
Declare FAIL, UNCLEAR, BLOCKED and NOT RUN separately from PASS. Coverage lists
every criterion from both suite and specification, executed case IDs and gaps;
blocked actions do not cover a criterion. Report missing suite criteria as gaps.

## Realistic scenario campaigns

<!-- @spec VALIDATOR-3 -->
Before sending any probe, plan 2–4 sessions from the actual users' roles,
specification and system prompt. Include full realistic documents or long
paragraphs; edits deep inside a line; changed numbers; forgotten attachments,
re-attachment, vague categories and follow-ups depending on earlier answers.
Plan the whole ask/file/approve-or-reject/check flow. Mark action, attachment,
card and state-check steps `BLOCKED: slice 2`; run only independent read-or-ask
steps. Do not grade a downstream check as PASS when its prerequisite was blocked.
If threaded bot follow-ups are unavailable, report that conversation coverage
is blocked; a new independent thread does not prove conversational continuity.

Judge each reply from the user's seat: is it confusing, internal, premature,
wrong, or posted somewhere unexpected? Visible tool or schema inventories,
raw MCP names, “Still to come: none”, and claiming execution before approval
are failures even when a narrow specification allows them. Context matters:
a user explicitly asking for a schema or tool identifier may receive it;
quoted user content is not unexplained internal narration. Where evidence
cannot establish a violation, use UNCLEAR and identify the missing evidence.
Every human or campaign finding produces a permanent target eval case draft
plus an expected acceptance property. Report the draft for a maintainer to
commit; this tester does not gain Git write permissions.

## Verification, snapshots and restoration (slice 2)

A target declares read-only verification tools and their exact read scope,
observable objects, content fingerprints, stable identifiers and revision
behavior. A separate restore contract declares authorized writes and supported
object types. Read tools alone cannot restore state. Unsupported state is a
blocker, not an empty snapshot. Record pre-state and post-state, compare content,
and retain evidence of restoration failure. Office metadata may be rewritten;
content equivalence, not byte equality, determines successful restoration.

The validator owns the cards it creates, records their IDs and asks people not
to clear them. Its future principal must be authenticated, scope-limited to the
marked installation and eligible for the route's actual approver set. An
operator token must not manufacture channel/group evidence. See phase 0 before
choosing a principal design. State restore must not undo unrelated user changes;
revision conflicts require a recorded refusal and operator intervention.

## Fault injection and continuation

Fault injection belongs to slice 2 and needs a target-declared reversible
control with exact scope. Include a store 5xx while a file actually arrived:
the expected reply distinguishes storage failure from missing attachment.
Never inject faults into production. Restore the control and content even
when a case fails; report unsuccessful cleanup as NO-GO.

A run may span 600-second turns. Persist case IDs, repeat indices, expectations,
thread IDs, verdicts and outstanding dependencies before the turn ends. A fresh
message has a fresh sandbox, so temporary files alone are insufficient: publish
continuation evidence in bounded reports and retrieve by campaign ID. If exact
state is lost, mark NOT RUN rather than inventing earlier successes. Pace new
threads by the target's sandbox quota and operator-assigned share; follow-ups
reuse a thread's sandbox only when its bot allowlist permits them. A clock rate
alone is not a quota proof. Stop starting work with the existing five-minute
report margin. Pending slice 2 steps do not become `Next:` live action requests.

## Ship verdict

<!-- @spec VALIDATOR-4 -->
Full GO requires: every fixed case and required P0 repeat passes; scenario
campaigns have no P0 finding and every required step is complete; no UNCLEAR,
BLOCKED, NOT RUN, missing criterion, cleanup failure or unresolved client
decision remains; an explained configuration diff between marked and production
installs; and a post-deploy read-only production smoke. The pre-deploy report
cannot claim that later smoke occurred. If any required condition is absent,
report NO-GO with the missing evidence. Passing read-or-ask cases is a slice 1
result only, never full GO. Report criteria, repeats and scenario coverage even
when no tests can run.

## Next decisions before slice 2

The owner must review the mark's authority and scope, principal issuance and
rotation, route-specific membership, card ownership, verification and restore
capabilities, content equivalence, conflict cleanup, fault controls and ship
policy. Then propose the ADR 0181 exception through the Accepted ADR procedure;
do not edit its frozen body as part of this example. The request's ADR 0174
reference is incorrect: that ADR concerns publication precheck capabilities,
not tester approval resolution. Locate the correct prior proposal before
reviving it. Phase 0 records measured facts and unresolved external evidence.

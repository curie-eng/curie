# Agent validator: read-only and admitted marked actions

<!-- @spec VALIDATOR-1 -->
A ship decision needs a target-owned fixed acceptance suite and realistic user
scenario campaigns. Read-or-ask remains available everywhere. Accepted ADR 0202
adds marked action probes only after a listed driver's own admission ping.
The seven-tool manifest is unchanged: there is no upload, human button-click,
platform API credential or arbitrary state/restore capability in this example.
The gate checks campaign observations; it is not authentication or attestation.

## Modes and the installation mark

Read-or-ask never performs an action and does not need a separate installation.
Action validation requires the installation's operator declaration, listed
channel/bot/user pair and target-owned verification and restoration contracts.
A name containing "test", a request or bundle prose grants no authority.

Before the first action, the driver sends its own root
`<@target> [test action] ping` and accepts only the first reply in that thread,
from the configured target bot/user, inside a chosen 1–60-second window:
`This installation accepts test actions from <@driver>.` A generic refusal,
wrong thread/author, earlier reply, expired observation or incomplete provider
page leaves every action unsent. The admission is independent of target turns.
Every action then begins `<@target> [test action] [mean test <id>]`; the mark
must follow the target mention, before the campaign label.

The driver retains the owned pre-state before any action and the actual own-read
observations afterward. Preserve base content and unrelated revisions; never
wipe. The operator schedules around demos. The tester adds no scheduler or lock,
and does not change a route, app scope or installation configuration to pass.

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
Card actions and state assertions require admitted action validation.
Attachments and `click-as-non-approver` remain BLOCKED: slice 2 because this
bundle lacks those transports. Plain pasted text is permitted only when the probe itself reads
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
re-attachment, vague categories and follow-ups depending on earlier answers,
as far as the target's users would do them. Plan the whole
ask/file/approve-or-reject/check flow for every target that can perform one; a
target that only reads and answers has no such flow to plan. Run action/card/state steps only after admission and with their declared
contracts. Unsupported attachment and non-approver click steps stay
`BLOCKED: slice 2`; only independent read-or-ask steps may bypass them. Do not grade a downstream check as PASS when its prerequisite was blocked.
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

## Deployed and source specs

A target's spec is either the bundle it runs or a source that bundle was
rendered from. Installers legitimately transform a bundle before deploying it:
they drop connectors, gates and secrets the installation does not use. A spec
read from a repository is therefore a claim about the source, not evidence of
what the target has. A live campaign once graded FAIL a bot that correctly said
it could not upgrade the platform, because the repository bundle declared an
upgrade tool whose connector the installed copy had been rendered without.

The request says which a spec is, after naming it: `spec from
<owner/repo>@<ref> <path>, rendered` for a source, or `spec from <where>,
deployed` for exactly what the target runs. Without either word, a spec read
from a repository (the thread's workspace, a listed repository or a repository
the request names) is a source spec, and a spec given in the request's text or
attachments is the deployed one.

When the target says it lacks a tool, connector, gate or secret that only a
source spec declares, the probe is UNCLEAR with the reason "spec source may
differ from deployment", never FAIL. This is the general evidence rule: a FAIL
needs evidence, and a source spec is not evidence of the deployed copy. FAIL
still applies when the target contradicts itself, contradicts what it read in
the thread, or contradicts a spec stated to be the deployed one. The gate
records only verdicts, so the reason lives in the report; the UNCLEAR still
makes the ship verdict NO-GO until a person checks the deployed bundle or the
campaign is rerun against a spec marked `deployed`.

## Verification, snapshots and restoration (slice 2)

A target declares read-only verification tools and their exact read scope,
observable objects, content fingerprints, stable identifiers and revision
behavior. A separate restore contract declares authorized writes and supported
object types. Read tools alone cannot restore state. Unsupported state is a
blocker, not an empty snapshot. Record pre-state and post-state, compare content,
and retain evidence of restoration failure. Office metadata may be rewritten;
content equivalence, not byte equality, determines successful restoration.

The validator owns the cards it creates, records their IDs and asks people not
to clear them. Its approve/reject reply must match the actual pending native card in the
same owned channel/thread. The platform alone mints a bounded `test_driver`
principal, independently checks its setting/subject, and admits only actual
ExplicitUsers routes. Channel/group/email, unbound and invalid routes remain
refused. The tester never mints a principal or changes an approver set.
State restore must not undo unrelated user changes;
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
report margin. Unsupported or unadmitted steps do not become unguarded `Next:` action requests.
Continuations require a new own ping before new actions, preserving original
pre-state, identity and prior observations.

## Ship verdict

<!-- @spec VALIDATOR-4 -->
The ship verdict is computed, not judged. The tester's model judges each probe
and records it; the bundle's gate, `gate/mean_tester_gate.py` (installed in the
runner layer as `mean-tester-gate`), validates the suite against
`acceptance/schema.json`, decides which cases are eligible, keeps the ledger of
recorded verdicts and aggregates it. The report carries the gate's `Ship:`,
`Coverage:` and `Ledger:` lines unedited.

**GO (read-only scope)** remains the verdict for read-or-ask campaigns. It requires: a READY
suite, its copy matching the Git blob it was read from when it came from Git; no
action-bearing case (mode `action`, attachments, a card action, an expected
state, or a probe the tester flags as asking for an action); every case passing
every repeat; every criterion named by a case; 2–4 declared scenario sessions
passing every step; every other probe the campaign planned or recorded
passing; and no gap the tester names, such as a specification criterion the
suite does not test, an unresolved client decision or an approval card left
pending. The probes
reach the deployed target itself, so this verdict is its own post-deploy
smoke, and no marked installation is involved, so no configuration diff
applies. It covers the installation the probes reached and only what a person
can do there by reading and asking.

An unadmitted action-bearing suite remains NO-GO. Admitted slice 2 can issue
full GO as **GO (action scope)** only with every eligible marked action
observation and all the same coverage conditions. It additionally requires
verified restoration with no
cleanup failure, an explained configuration diff between marked and production
installations, and a post-deploy read-only production smoke; the pre-deploy
report cannot claim that later smoke occurred.

Any absent condition is NO-GO with the missing evidence named: MISSING or
MALFORMED suite, FAIL, UNCLEAR, BLOCKED, NOT RUN, scenario count, gap, or slice 2.
Report criteria, repeats and scenario coverage even when no tests can run.

The ledger persists across turns as the report's `Ledger:` token: a compressed,
checksummed record bound to the suite's content and the campaign id, carrying
every case the tester flagged as asking for an action. A ledger from another
campaign or suite is refused, never reused. A fresh sandbox restores it
with `mean-tester-gate import`. The checksum catches a token that was not copied
exactly; it is not a security boundary against the tester itself.

## Observation inputs and gate commands

The gate performs no provider requests, writes, snapshots or restores. Supply
actual observations from the existing permitted reads. The model still judges
reply quality; exact state assertions and campaign aggregation run in the gate.
No boolean `admitted` or installation name can replace the observed messages.
Do not paste whole configs, credentials, sealed snapshots or raw private content.
Keep bounded readonly projections, content fingerprints and stable object IDs.
A supplied observation is evidence to check, never authenticated authority.

`admit --suite <file> --ledger <file> --campaign <id> --evidence <file>` takes:

- `campaign` and `suite`: the exact campaign and suite SHA-256 from intake.
- `channel`, `driver_user`, `driver_bot`, `target_user`, `target_bot`: the
  operator's pinned identities, not names guessed from a message.
- `window_seconds`: the chosen integer from 1 to 60.
- `thread`: actual provider `ok: true`, `has_more: false`, no remaining cursor,
  and `messages` including the own root and every reply. Keep actual `ts`,
  `thread_ts`, `user`, `bot_id`, `text`; do not synthesize missing fields.
- `snapshot`: `at` as observed Unix seconds, `source: "read-own-observation"`,
  a nonempty credential-free `content` projection and a named `restore_contract`.
  Its observation must precede the campaign. The contract names supported
  connector/policy/digest and object scope; it is not permission to execute.

Admission validates the complete thread in timestamp order and prints the
eligible plan. Readonly intake by itself still blocks action cases. Admission
must be recorded while its reply remains fresh inside the chosen window.
Action probes are checked within a 600-second part after its reply, not against
an unrelated historical response. This is the driver's conservative window,
not an extension of the platform's 60-second approval principal.

`record ... --evidence <file>` for an action case takes:

- `channel` and actual `probe` message fields, with the exact marked fixed
  text, case repeat and current campaign. A follow-up includes the actual
  driver-authored `root` in its same thread.
- For `card_action: approve/reject`, `card` with the actual target bot/user,
  timestamps and native blocks carrying one identical approval id in both
  Approve and Reject buttons; `decision` with the actual driver's exact
  marked approve/reject reply in that thread. The card precedes the decision.
- For `expected_state`, `state` with `at`, `source: "read-own-observation"`
  and the actual nonempty `content` projection read after the action, within
  180 seconds of its probe. PASS requires every expected key/value to match
  exact JSON, including types. Owner-defined fingerprints are supported;
  descriptive conditions without a supported readonly projection stay
  unverified. A target success claim is not this read.

Scenario/invented action records add `--action` and their planned `card_action`
and `expected_state` to the evidence. Record the action before grading further
steps. The ledger separates case, scenario and probe identities, so matching
names cannot overwrite each other's evidence. Mark every action in the skill;
the gate does not classify arbitrary natural-language probes automatically.

A continuation imports the unchanged token, then obtains a new own admission
with `admit ... --refresh --evidence <file>` before sending further actions.
The new ping must be later and fresh; campaign, identities, channel and original
snapshot must be identical. Each previous action retains its own admission
observation, which import rechecks along with the suite/campaign and every
required action field. New action records additionally require current admission freshness
inside the 600-second part. Completed historical evidence does not expire as a
claim about its past run, and never becomes a new execution grant.

`closeout ... --evidence <file>` takes actual later observations:

- `restoration`: `at`, `source: "read-own-observation"`, the same owned
  `content` projection as before, `cleanup_failures: []` and `pending_cards: []`.
  It must follow all recorded actions/decisions/state reads. Preserve the
  connector's actual restore receipt privately; conflicts/failures prevent GO.
- `configuration`: bounded credential-free `test` and `production` projections
  and `explanations`, keyed by every changed top-level projection key using exact JSON
  comparison. Include `testInstallation: true` in `test` and
  `testInstallation: false` in `production`. Choose
  sufficiently explicit keys to explain security, credentials references,
  downstream account scope and runtime differences individually. Missing or
  extra explanations are refused; equality does not require an explanation.
- `deployment`: actual readback `identity` and `at` for production.
- `smoke`: actual `at`, matching `deployment`, `read_only: true`, `verdict: "PASS"`,
  and observed `probe`/`reply`. It must follow deployment, which follows
  restoration. Future timestamps or pre-deploy fabricated smoke are refused.

Closeout closes action recording; it never fills missing cases or overrides a
FAIL/UNCLEAR. The gate then computes the ordinary verdict. If later production
proof is unavailable, report NO-GO now, retain the gap and collect it only after
it actually happens. No deployment is authorized by this bundle or document.

The lossless `mt1` token includes action observations so import revalidates them.
It is checksummed, not signed or secret: keep it private where its observations
are private. The checkpoint limit is 1,800 characters to leave room in a bounded
Slack report. Oversize evidence produces NO-GO and no partial `Ledger:` line.
Preserve the full private ledger; a sandbox that loses it has unknown prior
coverage, never counts reconstructed successes. Do not truncate or discard
negative evidence to make the token fit.

## Decision and capability boundary

Accepted ADR 0202 governs the platform admission and principal; its body is
unchanged. This bundle implements decision 6's driver behavior using the seven
existing tools. ADR 0121 governs connector-native restoration, ADR 0124 keeps
sealed snapshots with their connector, and ADR 0117 governs actual tool outcome
observations. The historical [phase 0 measurements](PHASE-0.md) remain evidence
of their earlier candidate, not a claim about this implementation's live proof.
The bundle adds no downstream access, fault controls, credential or restore
capability. Unsupported cases remain blocked and cannot receive full GO.

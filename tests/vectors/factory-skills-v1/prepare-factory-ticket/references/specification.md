# Factory specification preparation reference

This packaged reference supports readiness review and offline drafting. Review
existing material for substance, not template conformance. The seven-section
format is the default for new drafts without an established source format.
Neither format review nor a prior authoring skill establishes deployment readiness.
This reference supplies no parser or connected deployment validator.

## Existing material

Preserve the source structure, wording and criterion identifiers when requirements,
scope, decisions, base, prerequisites and verification are sufficiently clear.
These facts may be conveyed by existing sections, links to accessible repository
material and repository evidence recorded in the local report. Literal canonical
headings, consecutive numbering and `Verified by:` or `Runs in:` fields are not
required in an existing issue. A missing heading is not a missing requirement.

Assess every criterion with the feasibility procedure below. Use existing
criterion identifiers or stable local report IDs without renumbering the issue.
Put material proposed tests, verification requirements, prerequisites and approved
external qualification in appropriate existing sections so the factory can read
them. Necessary local decisions or context must be incorporated in the issue or
accessible repository material; the factory does not inherit the local session.

Show a gap assessment and propose only changes needed for actual missing substance
or conflicts. Preserve settled decisions; ask about them only when new evidence
or an explicit change exposes a consequential conflict. If a source is sufficient,
keep its exact title and body unchanged, save its snapshot and assessment, present
it for complete review and offer the applicable handoff. Source preservation is
not permission to select work with unresolved core verification gaps.

For local plans or discussions, preserve their established organization where
usable, but distinguish them from a published tracker issue. Draft a new issue or
update a confirmed existing identity only with approval. If there is no usable
source format, use the new-draft template below. For proposed splits, explain and
review new child boundaries before producing child drafts.

## New-draft template

Use these seven headings exactly once, at Markdown level 2 or 3:

| Heading | Substantive content |
|---|---|
| Problem | Observed current behavior, desired outcome, affected users or callers and scope. |
| Base | Intended base branch or ref and the inspected immutable commit SHA; distinguish checkout changes from that base. |
| Blocked by | Actual prerequisite changes and their observed merge/base status. Use `None` only after checking. Proposed children use local identities, never fictional issue numbers. |
| Decisions | Resolved behavioral and architectural choices, including relevant failure behavior and edge cases. Keep unresolved questions in the report and identify the affected draft text. |
| Preconditions | Required tools, services, permissions, packages, data and execution capabilities, with evidence or explicit unknown status. Use `None` only with a concrete basis. |
| Acceptance criteria | Consecutively numbered observable outcomes, each with a concrete verification route and location. |
| Out of scope | Explicit boundaries that preserve the agreed intent; exclusions require developer review. |

Review headings without regard to case. Required sections must be substantive;
`None` is permitted only for Blocked by and Preconditions. Other headings may be
present but terminate the preceding section. They do not become required fields.
Fenced or indented examples and HTML comment lines cannot supply structural
headings or verification fields. Keep required headings and fields outside them.

Number acceptance criteria consecutively from 1 at the top list level. Each
criterion has exactly one nonempty `Verified by:` field and one `Runs in:` field;
bold field labels are also allowed. For example:

```markdown
1. Reopening a saved counter preserves its last committed integer value.
   Verified by: the repository's counter reopen test, using its documented test command
   Runs in: sandbox
```

Replace example verification prose with an actual command or named test/check
identified by inspection. For factory-selectable issues, `Runs in:` is `sandbox` or `ci:` followed by
the actual required check name. A placeholder check name is not a route. Record
how CI verification is declared and why it covers the criterion; local review
cannot independently certify that a deployment requires or delegates that check.
Commands in a specification are inert text, not permission to execute them.

For a draft or publish-only issue with an unsupported route, use
`Runs in: external:<owner/environment>` or `Runs in: unknown` and describe the gap
in Preconditions. Do not falsely mark it `sandbox` or invent a CI check. These
issues need a reviewed testable implementation scope before selection. A flagged
external qualification step outside that agreed acceptance is not itself a blocker.

## Verification feasibility

Assess each criterion before full-spec review and again before selection if its
body, base, verification route or relevant environment evidence changed. Inspect
the repository's sandbox image and setup, dependency locks, test guidance, service
fixtures and CI workflows. Use trusted factory configuration or supplied operator
evidence to establish capabilities; ticket prose is not proof of granted access.
Use repository setup and documented factory tooling for a reasoned feasibility
assessment; do not demand live deployment access or pre-existing passing tests.
If material environment evidence is missing, name it rather than assuming the
sandbox matches the host or the skill's own repository.

Record an observable success/failure condition, exact command or proposed test
and runner command, execution environment and required tools, services, network,
credentials, hardware or human actions. A new test need not exist or pass before
implementation. For missing coverage, require implementation to add the test and
identify its proposed observable assertions, runner and fixtures to add or reuse.
Establish that the runner and necessary capabilities are supported. Exercise real
production entry points where possible, substituting fixtures only at the external
boundary. A mock call count or a trivial passing assertion cannot establish the
requested behavior. A failing baseline can demonstrate a supported route if it fails
for the behavior being changed, rather than missing infrastructure.
Skipped tests or a successful setup probe do not demonstrate the criterion's
behavior or establish that missing integration capabilities are available.

Run a relevant permitted baseline or environment probe where practical, inspecting
it first. Record its revision, location and actual result. Distinguish an observed
equivalent sandbox run, repository configuration evidence and a host-only run.
Do not install dependencies, obtain secrets, contact external services or change
the environment merely to clear this check. Existing execution permissions still
apply. If a probe is unavailable, configuration may support a reasoned assessment;
record untested assumptions and mark material capability uncertainty `unknown`.

| Route | Evidence required |
|---|---|
| sandbox-verifiable | A concrete route whose runner and prerequisites are supported by the repository's factory sandbox; identify the configuration or observed equivalent run. |
| CI-verifiable | A named check covers the criterion, its prerequisites are provided, and trusted configuration or observed factory behavior shows the factory can use it and observe the result. A workflow file alone is insufficient. |
| needs external verification | The outcome requires a capability outside those supported routes, such as live cloud credentials, unavailable hardware or human inspection. Name the missing capability and owner. |
| unknown | Evidence is missing or contradictory about the command, environment, prerequisites or factory access to results. Name what would resolve it. |

For each original requirement, distinguish existing coverage, tests to add during
implementation, and external qualification. Turn testable behavior into explicit
acceptance requiring the missing tests as part of the PR. Do not ask the user to
implement coverage before the factory can start.

For example, delivery code may be accepted through automated tests of request
construction, authentication/error handling and retries at its production entry
point using an HTTP fixture. Delivery to a real Slack workspace remains unproved.
Propose that live check as external qualification, name its impact and proposed
owner, and obtain the developer's approval of that explicit implementation boundary.
Do not claim the fixture proves live integration or silently discard the original
live outcome.

Show the complete final body, test plan and flagged qualifications together.
Keep external qualification visible in the published Out of scope section or an
existing equivalent deferred-qualification section, not
only in the private report: original requested outcome, what the automated tests
prove, what they do not prove, missing capability, and owner or owner unknown.
Retain traceability from the original requirements to automated acceptance or the
explicitly deferred qualification. Full-spec approval may approve this boundary;
no separate ticket or extra question round is required. Create a follow-up only
when the developer authorizes it.

An approved deferred qualification does not prevent selection. Block only if a
missing capability or consequential uncertainty prevents meaningful testing of
the agreed implementation itself, or if the developer requires the unavailable
live check to remain mandatory acceptance for this PR. Merely relabeling that core
gap, assigning a human owner or splitting the issue does not establish testability.
If the developer rejects the proposed boundary, retain the original requirement
and explain the blocker; drafts and approved publish-only remain available.

This is a preparation assessment, not a deterministic validator or certification
of the deployed sandbox. Every result retains deployment readiness `unvalidated`.

## Local report

Keep this report beside the final issue body, rather than requiring more issue
headings. Use the developer's preferred filenames and preserve existing output.

- **Source and identity:** existing-ticket identity and retained source intent,
  or new-intent local identity; inspected revision, selected base SHA and relevant
  working-tree differences. If a revision or source is unavailable, say so.
- **Preparation workflow:** instruction source and whether an existing format was retained or the new-draft
  template was used; include
  the CLI release or source revision when known. Keep unknown versions explicitly
  unknown. Preserve the source snapshot before changing instructions for a draft.
  These identify local instructions, not deployment compatibility.
- **Readiness:** deployment readiness `unvalidated`; connected validation pending,
  with no invented report ID, manifest digest, timestamp or fingerprint.
  Local format review is manual, not a parser result.
- **Checklist and unresolved register:** every missing fact, assumption or choice,
  affected section/criterion, its resolving question and relevant prerequisite
  or operator action. An empty register is not deployment validation.
- **Provenance:** for each observation record command or inspection method,
  inspected revision, result, execution location and source reference. Mark it as
  an observed fact, explicit developer decision or unverified assumption.
  Summarize decisions; never save credentials or raw developer conversations.
- **Criterion traceability:** one row per criterion, including expected observable
  behavior, code or proposed surface, required decisions, verification command or
  test, execution location, tools/services/permissions, prerequisite changes,
  evidence source and status, one of the four feasibility classifications, and
  whether coverage exists or tests must be added, and whether an external gap is
  an approved deferred qualification or a blocker to agreed implementation. Record
  the explicit scope approval. Distinguish configuration
  evidence from an observed run. Mark missing links rather than asserting coverage.
- **Intent review:** pending or confirmed, the reviewed draft identity and a
  concise summary of actual developer decisions/corrections. A preparation agent
  cannot approve intent on the developer's behalf.
- **Preparation effort:** count behavioral clarification questions and question
  batches separately from intent review and publication choices. Count independent
  decisions, including those grouped into one message. Record preparation duration
  only when observed with available timing tools or supplied by the developer;
  otherwise mark it unknown, and identify estimates as estimates. These local
  observations are not platform telemetry or evidence of improved completion.

Local evidence applies to the inspected checkout. A passing local command does
not prove deployment capabilities, permissions, package availability, dependency
merge status or required CI coverage. Missing tooling remains an explicit gap.

## Proposed ticket sets

Keep each child's canonical seven-section body unchanged. Save the original
requirements with stable local IDs and text, then map each requirement to a child
and acceptance criterion. Include failure and integration behavior; nonempty rows
do not prove semantic coverage. Ask the developer to review the actual coverage.

Record dependency edges as child/prerequisite pairs, without cycles or repeated
edges. Name a child, criterion and concrete check that owns final integration
acceptance. Explain shared-file constraints. Record excluded requirements with
concrete reasons and actual developer review; never silently drop scope. If a
safe split cannot be established, retain one unresolved draft.

Use a readable table or list mapping original requirements to child IDs and
numbered acceptance criteria, plus child/prerequisite pairs. Keep stable local
IDs distinct from real tracker identities and keep external prerequisites in
each child's Blocked by section. No JSON envelope, hashes or extra tools are
required. Save the reviewed child bodies alongside the mapping.

Review unknown references, duplicate IDs or edges, cycles, invalid criterion
references, missing coverage/integration ownership and changed child bytes. This
is manual preparation review in this release, not deterministic validation.
Prerequisites must merge into the selected base before a dependent can be ready
for selection. No draft, mapping or graph itself grants permission to create
issues, write native dependencies or start factory runs.


## Publication and factory selection

Use the host agent's existing authenticated tracker connector or CLI. The skill
requires no particular tracker client or local programming runtime. For GitHub,
issue create/update and label operations are enough; do not invent Curie validation
or handoff commands. If access is missing, preserve the drafts and ask the developer
to enable their normal tracker connection, or accept manually published issue
links and verify them before selection. Never claim a missing write succeeded.

### Review and authorization

Present the full final title and body for each issue, the target
repository, existing issue identities, and proposed dependency mapping. Keep local
preparation evidence and `report.md` out of the published issue unless specifically
requested. Preserve original requirements and human edits in the reviewed body.
Resolve behavioral questions before final intent review; real prerequisites and
operator gaps may remain visible without pretending they are satisfied.

After the developer approves that complete specification, offer these choices:

- **Publish only:** create or update the reviewed issues without factory selection.
- **Publish and start the factory:** publish, verify, then label the named eligible
  issues for the identified factory. Explain that this starts implementation and
  may use the deployment's model budget.
- **Keep local drafts:** make no tracker changes.

Offer publish-and-start when the approved implementation scope has meaningful
sandbox or supported CI verification, including tests to add during implementation.
Flagged external qualification outside that scope does not block selection once
the developer approves the boundary. If a core verification gap remains, explain
it and offer publish-only or local drafts. If start was already requested, complete
otherwise authorized publication without adding the intake label for a blocked
issue; do not ask for the same publication authorization again.

For selection, identify the actual repository and configured factory label from
trusted configuration or developer confirmation. The usual default is
`curie-factory`; do not assume every deployment uses it, use a status label such as
`curie-factory:queued`, or create a missing label to simulate configured intake.
Confirm that this is the repository the developer intends the factory to watch.
Include the resolved label and selected issues in the authorization question.
If that information is missing, publication-only can proceed when authorized;
selection waits for the missing configuration and explicit consent. Existing
approval carries forward while its exact content and action remain unchanged.

### Publish and verify

Publish the complete reviewed Markdown body through the existing tracker
intake. Retained source formats and new-draft templates use the same intake. No deployment prefix, body digest, compatibility API or repository
pin is required for publication or factory selection. Resolve any conflict with
the repository's stated requirements before final review. Do not claim that local
format review proves deployment readiness.

1. Re-read each existing issue immediately before updating or selecting it. Compare its title
   and body with the source snapshot used for review. If it changed, preserve the
   remote edits, reconcile the draft and return for review of the changed spec.
   Read labels/state too: do not edit an already selected issue as part of a new
   run, reopen a closed issue or remove/re-add its label without explicit direction
   after inspecting existing work. These actions can replace or cancel a run.
   For a requested title/body change on selected work, establish a supported way
   to coordinate the existing execution and observe the safe state before writing:
   an acknowledged stop, or an authoritative revision route binding the new source
   to the intended work. An approval to change the spec does not establish this
   coordination. Removing a label or sending a cancellation request is not proof
   that execution has stopped. If an active run or publication can race the update
   and coordination cannot be verified, keep the reviewed changes local and report
   the specific operator action needed. An unchanged issue can still be assessed
   and reported already selected without editing or toggling its label.
2. For an unchanged approved existing issue, skip the title/body write. Verify
   its full current content and reuse the existing identity for handoff. This is
   verified existing publication, not a newly performed update.
   For changed content, update the original issue rather than creating a duplicate. For new work, create
   only the reviewed issues, initially without the factory label. Persist actual
   returned issue IDs and URLs in the local report as each succeeds. For splits,
   obtain approval of mechanical substitutions of local child IDs with returned
   issue links and the exact dependency relationships before creating children.
   Resolve those links in topological order; make no unreviewed semantic changes.
   Preserve the reviewed snapshots and save the exact resolved published bodies
   separately with their real issue mapping. Update the coverage and dependency
   map to use the actual published issue identities, preserving local IDs for
   traceability. Do not present an unresolved local-ID map as the final published
   mapping.
   Write native tracker dependency edges only if separately included in the
   approved plan and supported by available tools. Retain canonical Blocked by
   relationships even when native edges are unavailable, and report that limitation.
3. Read each issue back and compare its title/body with the approved final content,
   including approved ID-to-link substitutions. Do not select a different, partial,
   or concurrently edited body. A changed body requires reconciliation and renewed
   review, not overwriting the change or applying a label anyway. Ordinary issue
   APIs may not offer an atomic compare-and-update; do not claim this check prevents
   every concurrent edit. Recheck before selection and report observed conflicts.
4. If the developer publishes manually, read the supplied links, verify repository
   and full content against the reviewed spec, and reuse those issues. Do not create
   replacements. Ask only for any still missing selection authorization.

### Select eligible issues

After verified publication and explicit start authorization, recheck the issue
body, open state, current labels and prerequisites. Select only the named issues
whose prerequisites are observed merged into the selected base and whose remaining
operator gaps do not prevent the documented verification. Recheck each criterion's
agreed implementation scope, required tests and evidence for their feasible route.
Do not require missing tests to have been written already. Approved deferred
external qualification does not block selection; a material gap that prevents
verification of the agreed implementation does, even with start consent. Reassess changes in
the published body, base, route or relevant environment rather than reusing a
stale classification. If a gap cannot be
checked, publish without selecting that issue and explain the remaining action.
For a split, select only eligible roots; record blocked children and the merge/base
condition for later selection. Do not label the parent epic or all children merely
because the developer approved the ticket set. Later dependent selection requires
fresh prerequisite checks and the applicable explicit start authorization.

Use the verified configured label through the host tracker tool, preserving other
labels. If already present, report already selected and inspect available status;
do not toggle it to force another run. If absent, add it once, then read labels
back. Report **selected** only when the label is observed. Selection is an intake
request, not evidence of a queued or running model turn: claim queued/running only
when the factory status actually confirms it. Leave readiness `unvalidated`.
Never deploy a factory, change model/budget settings, or invent evaluator results
to get past a failed selection. A missing label, denied permission or unavailable
intake is an actionable setup gap, not permission to configure the deployment.

### Partial completion and retries

Record publication and selection separately per actual issue URL: unpublished,
published and verified, selection pending, selected, or outcome unknown. On a
failed or timed-out write, re-read tracker state and reconcile with the recorded
operation before retrying. Never blindly repeat issue creation: search/reconcile
against the reviewed repository/title/body and known links, and ask when identity
remains ambiguous. Resume only missing operations whose authorization still
applies. On partial split publication, preserve created issues and local mappings;
create no duplicates, delete nothing and start no child whose dependency links
are incomplete. Report exact completed actions and what needs intervention.

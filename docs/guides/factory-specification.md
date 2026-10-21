# Factory specification format

The [preparation skill](factory-preparation.md) uses this format to help a coding
agent and developer review one deliverable. It follows the intent of
[ADR 0199](../adr/0199-the-factory-admits-only-tickets-it-can-start-and-finish.md).
[ADR 0222](../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
assigns factory workflow policy to the factory bundle.

This release provides authoring instructions, not a deterministic parser or
runtime admission check. Format review cannot establish that decisions are
resolved, prerequisites have merged, verification commands work, or a deployment
can finish the work. Connected evaluation remains tracked in #4487 and #4488.

## Canonical ticket

Use these seven headings once each, as Markdown h2 or h3:

| Heading | Content |
|---|---|
| Problem | Current behavior, desired outcome, affected users and scope. |
| Base | Intended branch or ref and the inspected commit. |
| Blocked by | Actual prerequisites and their observed merge/base status; use `None` only after checking. |
| Decisions | Resolved choices, including failure behavior and edge cases. |
| Preconditions | Required tools, services, permissions, packages and data, with evidence or explicit unknowns. |
| Acceptance criteria | Numbered observable outcomes with concrete verification routes. |
| Out of scope | Explicit boundaries and developer-reviewed exclusions. |

Keep required sections substantive and outside code examples or comments.
Number acceptance criteria consecutively from 1. Each criterion includes a
`Verified by:` field naming an actual command or test. For factory-selectable
issues, its `Runs in:` field names `sandbox` or `ci:<actual required check name>`. Replace placeholders with
real checks identified through inspection. Record why a CI check covers the
criterion; local review cannot certify deployment configuration.

Each criterion also receives a local feasibility assessment: sandbox-verifiable,
CI-verifiable, needs external verification or unknown. The report records the
evidence for environment support, not just a command that worked on the host.
CI verification requires evidence that the factory can use the named check and
observe its result. Proposed tests are allowed when their runner and prerequisites
are supported; they need not pass before the feature is implemented.

Missing coverage becomes explicit acceptance requiring tests to be added in the
implementation PR, with meaningful assertions, runner and supported fixtures.
Preparation does not require the future regression test to exist or pass already.
Flag unavailable external qualification in the published Out of scope section,
preserving the original outcome, what tests cannot prove, missing capability and
proposed owner. Present that boundary for approval with the full specification.
Approved external qualification outside the agreed implementation acceptance does
not block factory tagging. Missing capabilities required to test that implementation
still do; naming a human owner or silently weakening the tests does not resolve them.
Drafts and publish-only issues may retain `Runs in: external:<owner/environment>`
or `Runs in: unknown` when the developer keeps unsupported checks in acceptance.

The packaged skill [reference](../../examples/dark-factory/preparation/skills/prepare-factory-ticket/references/specification.md)
contains the full authoring and publication workflow. Verification commands in a
draft are text, not authorization to execute them.

## Local evidence

Keep provenance in a separate report beside the canonical body. Record the
inspection method or command, inspected commit, observed result, execution
location and source reference. Distinguish observations, explicit developer
decisions and unverified assumptions. Keep credentials and raw conversations
out of the report. Local evidence applies to the inspected checkout and does not
prove deployment readiness. Record readiness as `unvalidated`.

## Splitting a deliverable

Each proposed child retains its own seven-section body. Preserve the original
requirements with stable local IDs, then save a readable table or list mapping
each requirement to its child and numbered acceptance criterion. Include failure
and integration behavior, a named owner and concrete check for final integration,
and developer-reviewed reasons for any exclusions.

Record child/prerequisite relationships without cycles and explain shared-file
constraints. Keep external prerequisites in each child's Blocked by section.
Review actual semantic coverage with the developer; a nonempty mapping does not
prove that the original intent is preserved. No JSON envelope or fingerprints
are required.

Local child IDs are drafting identities. After approved publication, preserve
reviewed snapshots, save the exact resolved bodies, and update the mapping with
actual tracker identities. A dependent child remains blocked until its
prerequisites merge into the selected base. A map does not authorize concurrent
execution, tracker writes or factory selection.

## Review and publication

The skill presents each full title and body for developer review, then obtains
explicit authorization for publication and optional factory selection. It reads
published content back before applying the confirmed configured factory label to
eligible issues. Label observation means selection for intake; a running state
requires an observed factory status. This flow does not claim validated handoff
or deployment evaluation.

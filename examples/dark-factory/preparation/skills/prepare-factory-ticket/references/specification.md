# Factory specification preparation reference

This packaged reference is sufficient for offline drafting. Contract v1 uses
the same canonical format for an agent-written specification and an issue form.
It describes structure and preparation evidence; it does not supply a parser or
establish deployment readiness.

## Canonical body

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

Heading matching is case insensitive. Empty sections fail structural parsing;
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
identified by inspection. `Runs in:` is exactly `sandbox` or `ci:` followed by
the actual required check name. A placeholder check name is not a route. Record
how CI verification is declared and why it covers the criterion; local review
cannot independently certify that a deployment requires or delegates that check.
Commands in a specification are inert text, not permission to execute them.

## Local report

Keep this report beside the canonical body, rather than requiring more issue
headings. Use the developer's preferred filenames and preserve existing output.

- **Source and identity:** existing-ticket identity and retained source intent,
  or new-intent local identity; inspected revision, selected base SHA and relevant
  working-tree differences. If a revision or source is unavailable, say so.
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
  evidence source and status. Mark missing links rather than asserting coverage.
- **Intent review:** pending or confirmed, the reviewed draft identity and a
  concise summary of actual developer decisions/corrections. A preparation agent
  cannot approve intent on the developer's behalf.

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

When exact fingerprints can be computed using existing capabilities, a v1 JSON
envelope contains these fields:

| Field | Value |
|---|---|
| `version` | Integer `1`. |
| `source` | `kind` of `existing-ticket` or `new-intent`, and `identity`. |
| `requirements` | Records with stable `id` and original `text`. |
| `children` | Records with stable local `id` and `specification_fingerprint`. |
| `coverage` | Records with `requirement`, `child` and numbered `criterion`. |
| `integration` | Named `child`, `criterion` and nonempty verification `check`. |
| `dependencies` | Records with `child` and `prerequisite`, both local child IDs. |
| `scope_exclusions` | Records with `requirement`, concrete `reason` and `reviewed: true` only after explicit review; otherwise an empty array. |

IDs begin with an ASCII letter and contain at most 64 ASCII letters, numbers,
underscores or hyphens. Save a local ID-to-child-file mapping beside the envelope.
IDs do not resolve to tracker issues during preparation. Keep external native
prerequisites visible in each child's Blocked by section.

A child fingerprint is lowercase SHA256 of its exact UTF-8 Markdown bytes.
Line endings and edits change it. Canonical envelope serialization uses sorted
object keys and compact JSON. Do not guess hashes or use placeholders in a file
presented as a complete v1 envelope. If hashing is unavailable, save an explicitly
incomplete draft mapping and record missing fingerprints; install no tools just
for this purpose. Any computed hash identifies bytes, not readiness.

Review unknown references, duplicate IDs or edges, cycles, invalid criterion
references, missing coverage/integration ownership and changed child bytes. This
is manual preparation review in this release, not deterministic validation.
Prerequisites must merge into the selected base before a dependent can be ready
for later validation and explicit selection. No draft, mapping or graph grants
permission to create issues, write native dependencies or start factory runs.

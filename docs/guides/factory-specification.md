# Factory specification contract v1

A factory specification describes one deliverable. Contract v1 follows
[ADR 0199](../adr/0199-the-factory-admits-only-tickets-it-can-start-and-finish.md).
[ADR 0222](../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
assigns this contract to the factory bundle. Its structural library lives in
`examples/dark-factory/preparation/src/curie_factory_preparation/` for managed
runtime use and contributor verification. Platform applications do not import it.
The [local preparation skill](factory-preparation.md) requires no client Python
installation and does not execute this library on the developer's computer.
Parsing establishes structure only. It does not establish that decisions are
resolved, prerequisites are satisfied, commands work, coverage is substantive,
or a deployment can finish the work. The deployment evaluator and validation
API are tracked in #4487 and #4488.

## Canonical ticket

Use each of these seven headings exactly once, as Markdown h2 or h3:
Problem, Base, Blocked by, Decisions, Preconditions, Acceptance criteria,
and Out of scope. Heading matching is case insensitive. Empty sections fail.
`None` is allowed only for Blocked by and Preconditions.
Additional headings are permitted but terminate the preceding section; they
are not additional required fields. Fenced and indented examples do not supply headings or criteria. HTML
comment lines cannot supply structural tokens; headings and verification
fields must appear on lines outside comments. Nested numbered examples with
greater indentation than the first criterion do not introduce criteria. Commands are inert text and are never executed by the parser.

Number criteria consecutively from 1 at the section's top list level. Each
criterion needs one nonempty `Verified by:` and one `Runs in:` field. Bold
field labels are also accepted. Runs in is exactly `sandbox` or
`ci:<actual required check name>`, where the angle-bracket expression is
replaced by the real name. A placeholder is not a verification route.
A structural pass does not verify that the CI check is required or delegated.

The [agent-written example](../../tests/vectors/factory-specification/agent.md)
and [issue-form example](../../tests/vectors/factory-specification/issue-form.md)
exercise the same contract with h2 and h3 headings. The shared fixtures in
`tests/vectors/factory-specification/` include malformed and decomposed inputs.
The issue form is owned by #4023, which should reuse these fixtures.

## Local evidence

Keep provenance beside the canonical body, without adding mandatory headings.
For each local observation record the command, inspected commit, observed
result, execution location and source reference. Distinguish observed facts,
explicit developer decisions and unverified assumptions. Do not record
credentials or raw developer conversations. Local tests are evidence about
that checkout, never proof of deployment capabilities. An unresolved
assumption must remain visible to the preparation workflow in #4489.

## Local ticket-set envelope

A split keeps each child's seven-section body unchanged. A versioned JSON
envelope supplies authoring and handoff metadata:

- `version`: integer 1.
- `source`: `kind` (`existing-ticket` or `new-intent`) and `identity`.
- `requirements`: original requirement records with stable `id` and `text`.
- `children`: stable local `id` and `specification_fingerprint` for each body.
- `coverage`: records with `requirement`, `child` and numbered `criterion`.
- `integration`: named `child`, `criterion` and nonempty verification `check`.
- `dependencies`: records with `child` and `prerequisite`, both local child IDs.
- `scope_exclusions`: requirement, concrete reason and `reviewed: true` for
  each explicitly developer-reviewed exclusion. Use an empty array otherwise.

IDs start with an ASCII letter and contain at most 64 letters, numbers,
underscores or hyphens. Local IDs are placeholders, never fabricated issue
numbers. The caller supplies a mapping from these IDs to child Markdown;
the parser reads no files or tracker records. External native dependencies
remain in each canonical specification and are assessed by #4487/#4488.

Fingerprints are lowercase SHA256 of the exact UTF-8 Markdown bytes. Changing
line endings or prose changes a child fingerprint. The envelope serializes
with sorted object keys and compact JSON; its fingerprint binds the canonical
envelope. The parsed data stays local/transient and is not platform business
state. Handoff #4490 resolves local IDs to actual tracker identities.

Duplicate IDs, unknown references, repeated edges, cycles, invalid child
criteria, fingerprint drift, uncovered requirements and missing integration
ownership fail with stable referenced diagnostics. Exclusions require explicit
review; parsing `reviewed: true` cannot prove that a developer actually approved
it. The preparation and handoff flows must obtain that review.

A nonempty coverage row cannot prove semantic coverage. The developer reviews
whether every original behavior, edge case and integration requirement is
preserved. Dependency edges describe ordering, not permission for concurrent
execution. A dependent cannot start until its prerequisites merge into its
selected base and it is explicitly selected with fresh validation. No parser
result selects issues, writes native links or creates factory runs.

## Diagnostics and verification

Results expose `valid`, the parsed object only on success, and a tuple of
structured diagnostics. Diagnostics contain stable `spec_*` or `set_*` codes,
canonical section/criterion references or JSON-pointer paths, and fixed fix
guidance. They never echo untrusted field values or commands. Parsed object
representations omit prose; callers must also avoid logging serialized bodies.

Run the contract fixtures, graph and injection tests and documentation
validation from the repository root:

```bash
uv run pytest examples/tests/test_factory_specification.py -q
bash scripts/check-docs.sh
```
No deployed admission path consumes this parser yet. #4589 supplies the generic
isolated evaluation boundary; #4487 and #4488 consume the factory evaluator
through it. The [preparation skill](factory-preparation.md) produces reviewed
unvalidated specifications and can publish and select them through the existing
configured-label intake after explicit developer approval. It does not run
deployment evaluation or claim a validated handoff.

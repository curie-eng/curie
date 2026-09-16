# 135. A skill bundle has two conformance profiles

Date: 2026-08-29

Status: Draft

## Context

Curie's bundle validator rejects a specification-conformant Agent Skill. A
`SKILL.md` carrying `allowed-tools: "Read Bash"` — the shape the agentskills.io
specification calls canonical — fails `validate_bundle` with
`skill.frontmatter_invalid: allowed-tools: Input should be a valid list`,
because `SkillFrontmatter.allowed_tools` is modelled as `list[str] | None`.

That typing was never a decision to refuse the string. The model landed in one
commit on 2026-07-05 that mirrored the `SKILL.md` shape *as understood then*,
before the specification named the space-separated string canonical, and the
package's own recorded reasoning is that the field is spelled `allowed-tools`
rather than `tools` because "using the real field name is the compatibility
choice (the wedge)". Rejecting the canonical spelling of that field's value
contradicts the reason the field is spelled that way.

The divergence runs in both directions, which is the part that makes this a
decision rather than a bug fix. Curie *accepts* several shapes the reference
validator and the claude.ai Skills upload path hard-reject:
`disable-model-invocation`, any unknown key, and the empty list `allowed-tools:
[]`. Those acceptances are also deliberate. Issue #540 made the validator refuse
the *confusable* keys (`tools`, `allowed_tools`, `allowedTools`) precisely so
that "unknown-but-not-confusable keys still validate clean and plugin-format
stays lenient by design", and the package's contributor guidance says plainly
that the wedge is compatibility, not schema purity. A bundle authored for Claude
Code must keep validating unchanged.

So there are two audiences with two different, both-legitimate definitions of
"conformant", and today one validator silently answers for both. Nothing in the
tree tells an author which way their bundle is drifting.

Two things make this worth deciding now rather than tolerating.

**Publishability became a product goal.** The frozen `plugin.json` marketplace
epic (#513) establishes that "our unit is already Claude-plugin-compatible" and
that the missing layer is catalog plus install, with the payoff that a bundle we
publish "should be installable in a Claude Code or Cowork marketplace
unchanged". Something eventually has to answer whether a given bundle clears
that bar, and the ingestion validator cannot be that thing without breaking
every bundle already deployed.

**Widening the field touches an approval gate.** The #1852 gate-shadow boot
check reads each skill's `allowed-tools` to refuse booting a gate the bundle's
own skill permissions would bypass, and it reads the field directly, skipping
anything that is not a list. The moment the model accepts a string, a skill
declaring `allowed-tools: "Bash"` becomes invisible to that check — the bundle
would report its `Bash` gate as armed while executing `Bash` unapproved. Today
that fail-open is masked only because `validate_bundle` rejects strings before
boot. Accepting the string without a single shared normalization boundary arms
it.

## Decision

### 1. One validator, two profiles

`validate_bundle` gains a keyword-only `profile`, defaulting to
`claude-plugin`.

- **`claude-plugin`** is the default and the **ingestion contract**. It keeps
  today's leniency exactly — `extra="allow"` stays, unknown keys stay legal,
  `[]` stays legal — and additionally accepts the canonical string form.
  Everything the strict profile would reject is emitted as a
  `skill.spec_nonconformant.*` **warning** rather than an error, so accepting a
  new shape cannot break a deploy.
- **`agent-skills-strict`** enforces the specification's six-field closed world
  (`name`, `description`, `license`, `compatibility`, `metadata`,
  `allowed-tools`), its name rules, its bounds, and the canonical
  `allowed-tools` string. It is a **publishability** profile — the eventual
  consumer is #513's catalog, not any deploy path.

**Two surfaces are permanently `claude-plugin` and pass no profile at all:**
`runner/src/curie_runner/plugin.py` at the boot validation, and
`apps/api/src/curie_api/bundles.py` at the deploy-ingestion validation — "the
only gate a bundle passes through", covering CLI deploy, the UI create-agent
modal, and git-flow push. Passing the strict profile at either site would turn
every bundle in the fleet into a boot or deploy failure. This is a hard
constraint, and it is asserted by a test rather than left to review.

An unrecognized profile id raises `ValueError` naming both valid ids. It is
deliberately **not** a `ValidationIssue`: a typo'd `"agent-skills-strct"`
silently falling back to the lenient profile would be a false PASS on a
publishability gate — the same "a typo becomes permission widening" shape the
package's `ToolPolicy` already refuses. This is a caller error, not a bundle
error, and it is the one documented exception to `validate_bundle`'s otherwise
absolute no-raise contract.

### 2. The canonical serialized `allowed-tools` is a space-separated string, split paren-aware

Curie **emits** the string form everywhere it authors a `SKILL.md` (`curie
init` and `curie init --from-spec`) and **accepts** the string, the block list,
and the flow list on the way in.

The string is canonical rather than the block list because the specification
prose says so. The reference validator's *acceptance* of a block list is not a
durable basis to canonicalize on: it is one implementation's present tolerance,
not the format. A shape that lives only in an implementation's leniency can
narrow in any release — and the claude.ai upload path, which is the surface a
published bundle actually meets, is stricter than the reference validator
already. Emitting what the prose names canonical is the shape most likely to
still be accepted by the next consumer we have not met. The block list stays
accepted, and stays only a warning even under the strict profile, exactly
because the reference validator does accept it today.

**Splitting is paren-aware, and this is load-bearing rather than a nicety.**
The string is split on whitespace or a comma **only at parenthesis depth 0**.
`Bash(git commit:*)` is a realistic Claude Code permission rule, and Claude
Code's "space- or comma-separated string" routinely puts spaces and commas
*inside* a specifier. A naive splitter turns that entry into `Bash(git` and
`commit:*)`, neither of which parses back to a tool name, so the entry drops out
of the #1852 gate-shadow detection entirely — the same fail-open, arriving
through the shape this ADR just made canonical.

A single normalization function is the **only** way any consumer reads the
field. The model preserves whatever the author wrote, verbatim, and normalizes
nothing. Both the validator and the runner's gate-shadow check call the shared
helper. A second reader with its own splitting rule is the two-parsers-for-one-
rule defect that #1495 and #1564 were each about, and the #1852 commit's own
stated reason for not sharing a helper — "with one implementation there is no
second path to disagree with" — expires the moment this ADR creates the second
implementation.

**An entry that cannot survive the round trip is refused, not absorbed.**
Entries are serializable iff joining them with a space and splitting the result
back returns the same list. The equivalent per-entry rule, used to name the
offender, is: the entry is empty, carries a comma at depth 0, carries whitespace
at depth 0, or has unbalanced parentheses. Unbalanced parentheses need naming
separately because `Bash(git` round-trips cleanly *on its own* — the loss
appears only at list level, where the following entry is swallowed into the open
parenthesis. `cli/src/spec.rs` applies that same rule at spec-parse time, so
`curie init --from-spec` refuses such an entry **before any byte is written**,
which is the module's existing ethos applied one field over. The validator
reports it as `skill.spec_nonconformant.allowed_tools_unserializable`.

### 3. Strict violations are warnings on the ingestion surface, errors only under the strict profile

`[]`, `disable-model-invocation`, and unknown keys appear in real, working
Claude Code skills. Making any of them an ingestion error would re-break exactly
what #540 deliberately preserved and would turn the lenient model into a
de-facto `extra="forbid"`. So on the ingestion surface they are warnings, and
they are errors only when a caller explicitly asks for the publishability
profile.

**No production surface renders those warnings today, and this decision does
not add one.** `apps/api/src/curie_api/deploy.py` receives the whole
`ValidationResult` but raises from `result.errors` alone;
`apps/api/src/curie_api/routers/bundles.py` returns only `errors` in its
response body; `runner/src/curie_runner/plugin.py` reads only `valid` and
`errors`; and `curie skill check` does not call `validate_bundle` at all. What
this ADR establishes is the **vocabulary and the contract that produces it** —
the thing a publish path, a lint verb, or a deploy response can later consume.
Anywhere this decision reads as "an author sees", read "a caller of the library
can see". Surfacing them is a named follow-up (**FU-1**), and until it lands the
compatibility bridge is a library contract awaiting a consumer. That is stated
here rather than left for a later reader to discover, because a warning tier
nobody renders is easy to mistake for a shipped author-facing feature.

### 4. The schema widens; the property set does not change

`$defs.SkillFrontmatter.properties["allowed-tools"].anyOf` gains a
`{"type": "string"}` member in the regenerated
`packages/plugin-format/schema/plugin-format.schema.json`. Nothing else in the
document changes.

That is deliberate and it is what keeps this reviewable under the
[ADR-0101](0101-schema-compatibility-for-closed-schemas.md) /
[ADR-0103](0103-previous-schema-shape-gate.md) regime. The specification's
`license`, `compatibility`, and `metadata` fields are **not** added as explicit
model fields. Adding `metadata: dict[str, str]` to a *lenient* model would turn
a `claude-plugin` bundle carrying `metadata: {retries: 3}` into a hard error —
a straight regression against the leniency mandate. The strict profile validates
those three keys off the raw frontmatter mapping instead, where it can apply
strict types without touching the lenient path. The consequence is that the
regenerated diff is exactly one widened `anyOf`: no property is added, renamed,
or removed, so ADR-0103's name-based properties/required gate stays quiet, and
the change is purely widening — no document that validated before stops
validating.

No version is bumped. The `plugin-format` package carries no `PROTOCOL_VERSION`
and no ACI wire shape changes here, matching how `approvalPolicy`, `triggers`,
and `toolPolicy` each landed.

## Consequences

The #1852 fail-open is closed rather than widened: the runner's gate-shadow
check now sees string-form declarations, and sees a paren-bearing specifier as
the one entry it is, which it did not do for the string form before this change.
That closure has to land in the same change as the model widening; shipping the
widening alone would arm the fail-open.

The compatibility bridge is **latent**. Until FU-1 renders warnings on some
surface, a bundle that is spec-nonconformant deploys exactly as it does today,
silently. The change an author can observe immediately is narrower and real: a
`SKILL.md` written the canonical way now deploys instead of being rejected, and
`curie init` now teaches the canonical shape.

`curie init --from-spec` becomes stricter in one specific way, and this is a
broadened error surface, not only an acceptance: a spec whose `allowed_tools`
carries an entry with a depth-0 space or comma, or unbalanced parentheses, now
fails to parse where it previously scaffolded. Those specs previously produced a
bundle whose entries were silently corrupted on the way back out, so the refusal
replaces silent corruption with a loud, entry-naming error. Specs whose entries
are separator-free — including every one in the tree — are unaffected.

**A known limitation.** Distinguishing a flow list from a block list requires
reading raw frontmatter text, because YAML parses `[Read, Bash]` and a `- Read`
block into the identical value. The classifier decides string-vs-list from the
parsed value and only then scans the raw text, including a forward scan that
correctly classifies a multiline flow sequence. What it cannot classify is
frontmatter with no literal sequence to read — an anchor, an alias, or a merge
key resolving to a list. Those fall back to `"block"`, which is a warning rather
than an error under both profiles. So exotic YAML can under-report under
`agent-skills-strict`. That is a deliberate fail-lenient on a profile nothing
gates on yet, not an oversight.

The shipped example bundles and test fixtures keep their current shapes
(`allowed-tools: []` and block lists). They stay valid under the default profile
and gain warnings, and keeping them on the old shapes is useful: they are the
standing regression proof that the default profile stayed lenient. Migrating
them is **FU-2**.

The strict profile has no CLI or UI surface. It ships as a library API only, and
a `curie skill check --profile` flag is **FU-3** — it drags in the committed
command manifest, the generated TypeScript manifest, and a new versioned CLI
result schema, which is a separate lane.

A bundle with twenty skills each carrying a block list and two Claude Code
extras emits on the order of sixty warnings per validation. Nothing renders them
today, so nothing degrades; deduplication and summarization belong with FU-1
rather than ahead of it.

Follow-ups: **FU-1** render `ValidationResult.warnings` on a production surface
at all — the deploy path must stop dropping them, the bundles router must return
them alongside `errors`, and the CLI must render them — then deduplicate;
**FU-2** migrate the shipped example bundles and fixtures to the canonical form;
**FU-3** a `--profile` flag on `curie skill check`; **FU-4** a conformance CI
gate pinning the reference validator as an oracle against our fixture matrix;
**FU-5** decide, once this ADR is accepted, whether the strict profile should
ever gate a publish path.

## Alternatives considered

**Make `agent-skills-strict` the ingestion default.** Rejected. Every bundle in
the fleet — including every example this repository ships and every fixture its
own tests use — carries at least one shape the strict profile refuses. Making it
the default fails the runner at boot and the API at deploy for bundles that work
today, to enforce a bar whose only consumer (#513's catalog) does not exist yet.
Leniency at ingestion is the wedge; strictness is a publishability question, and
conflating them costs the wedge to buy nothing.

**Add explicit `license`, `compatibility`, and `metadata` fields to
`SkillFrontmatter`.** Rejected. Pydantic would then type-check them on the
*lenient* path, so a Claude Code bundle carrying `metadata: {retries: 3}` — a
mapping the current model happily ignores — becomes a hard ingestion error. It
would also change the property set, which is precisely the shape ADR-0103's gate
watches, turning a one-line `anyOf` widening into a reviewable schema
restructure. The strict profile reads those keys off the raw mapping instead and
gets the same enforcement with none of the blast radius.

**Take a dependency on the reference validator and use it as the oracle at
runtime.** Rejected. It would put a third-party package on the deploy-ingestion
path, where its version choices would decide whether our customers' bundles
deploy, and it answers only the strict question — it cannot express the lenient
profile that ingestion actually needs. Pinning it as an *oracle in CI*, against
our own fixture matrix, is the right use of it and is FU-4; making it the
implementation is not.

**Refuse a specifier containing a space (`Bash(git commit:*)`) instead of
splitting paren-aware.** Rejected. It is a legitimate, common Claude Code
permission rule, and refusing it would make Curie reject bundles Claude Code
accepts — the exact divergence this ADR exists to remove. Depth-aware splitting
is also the truer reading of what "space- or comma-separated" means for a
grammar whose specifiers contain spaces. What genuinely cannot round-trip is an
entry with unbalanced parentheses, and that is what the refusal now names.

## Realizing code path

`packages/plugin-format/src/plugin_format/skills.py` holds the profile ids, the
shared paren-aware normalizer, the flow-vs-block classifier, and the
serializability rule; `packages/plugin-format/src/plugin_format/validate.py`
threads the profile and emits the `skill.spec_nonconformant.*` findings;
`packages/plugin-format/src/plugin_format/models.py` widens the field;
`runner/src/curie_runner/approval.py` adopts the shared normalizer, closing the
#1852 fail-open; and `cli/src/scaffold.rs` plus `cli/src/spec.rs` emit the
canonical scalar and refuse an entry that cannot survive it.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act: either this ADR is published Accepted, or the
coordinated exception is recorded with explicit maintainer approval naming the
realizing code path above.

# 83. One discovery pass answers `info` at every tier

Date: 2026-07-28

Status: Accepted

Implements [#1040](https://github.com/curie-eng/curie/issues/1040).

Extends ADR-0021 (the CLI's primary consumer is a coding agent, so `--json` is a
machine contract), ADR-0041 (every verb is answered at every tier, and exit 4
means absent by construction), and ADR-0074 (every agent-facing result maps to a
committed, versioned JSON Schema). Supersedes nothing.

## Context

An agent that has just written or deployed a bundle has no single place to learn
what the harness actually resolved from it. The facts were scattered:

- `curie schema` and `curie schema-index` describe the CLI's own contracts, not
  a particular bundle.
- `curie skill up` prints a boxed env summary as a side effect of booting a
  container, so learning what a bundle resolves to costs a Docker run.
- `curie skill check` answers exactly one question (do the declared MCP servers
  load) and needs Docker to answer it.
- `curie skill approvals`, `curie <tier> versions`, and `curie <tier> memory`
  each answer one slice under `--json`, with no shared shape between them.

Worse than the scatter is what none of them could say. Nothing distinguished
**"this bundle declares none"** from **"this bundle declares something the
harness looked at and did not register"**. A `skills/` directory whose
`SKILL.md` was renamed, a `.mcp.json` that does not parse, an `evals/cases.json`
that was deleted: each of these reads downstream as an empty list, which is the
same bytes an intentionally empty bundle produces. An agent that inspects the
empty list reports success, and the defect surfaces later as a deployed agent
that silently has no skill.

That is the same lie-class ADR-0041 exists to kill, one level down: ADR-0041
banned a fabricated empty result for a concept that does not exist at a tier;
this is a fabricated empty result for a concept that exists, was looked for, and
was rejected.

## Decision

**Add one agent-facing read verb, `curie <tier> info`, whose whole product is a
single JSON object: what the harness resolved from a bundle, plus a
`diagnostics` array naming every candidate the pass looked at and did not
register, what it looked for, where it looked, and why the candidate did not
count.**

### 1. The shape is `curie <tier> info`, not a top-level `curie info`

Every environment command in this CLI takes a target noun in the middle, and
issue #40 explicitly retired the mixed shapes (bare top-level environment verbs,
a `--local` flag). ADR-0041's decision is itself a verb-under-a-tier-family
construct: every clause in it names a variant of `SkillAction`, `LocalAction`,
or `ClusterAction`. `info` targets an environment, so it takes the noun. A
top-level `curie info [--plugin-dir DIR]` is shorter and matches how some peer
tools spell it, and it was rejected for walking back the grammar the whole CLI
is built on.

### 2. Implemented at all three tiers, over one source-agnostic view

`skill`, `local`, and `cluster` all run **one** discovery pass over an in-memory
`path -> content` view of a bundle's files (`BundleView` in `cli/src/info.rs`).
The skill tier populates it from a bundle directory (`BundleView::from_disk`);
`local` and `cluster` populate it from the in-force deployment's stored files
(`BundleView::from_files`, fed by `find_agent`, `list_deployments`,
`select_in_force_deployment`, and `bundle_files`).

`discover` is pure, synchronous, and network-free: it reads nothing but the view
handed to it. No filesystem, no environment, no clock, no network. That is what
makes a diagnostic mean exactly the same thing at every tier, and it makes
cross-tier parity a checked property rather than a claim: a test builds two views
over the same bundle, one from disk and one from a synthesized stored file list,
and asserts the same diagnostics come back. A bundle that is clean at `skill` and
dirty at `cluster` is then a real signal about the deployed artifact rather than
an artifact of two implementations.

Everything that is a fact about the **invocation** rather than about the bundle
is layered on afterwards by `run`, never inside the pass: the optional
`--check-mcp` container probe, shell-environment secret satisfaction, the
recorded runner in `.curie/runner.json`, and which deployed tier asked.

### 3. The rejected alternative: answering the deployed tiers with exit 4

The first draft of this design answered `local info` and `cluster info` with
ADR-0041's `ExitClass::Unsupported` (exit 4), on the grounds that a deployed
bundle has no directory to inspect, and scheduled a follow-up to implement them
later. **That was rejected in architecture review, and recording why is the most
load-bearing paragraph in this ADR.**

The bytes are already reachable at every tier:

- `api::BundleFile` carries `{path, content}`, so `ApiClient::bundle_files`
  returns the deployed bundle's file set **with contents**, not a manifest
  listing.
- `commands::parse_manifest_gates` is already documented in its own doc comment
  as shared by the skill tier (manifest on local disk) and the local and cluster
  tiers (manifest pulled from the deployed bundle over the API, #546), so both
  read gates identically.
- `commands::deployed_manifest_gate_names` already walks deployment to version
  to files today.

So the deployed tiers were genuinely answerable, and the scheduled follow-up was
the tell. Exit 4 means a concept is absent **by construction**; a verb that is
merely not yet implemented keeps exit 1. "Not yet" is not "never", and dressing
"not yet" as a tier limitation is precisely the lie ADR-0041 exists to prevent,
in the one place a consumer is least able to detect it (an agent that reads exit
4 stops retrying and stops asking). The repo's own bar is written into
`commands::skill_memory_unavailable`: the reason claims only what is true.

The cost of the rejection is real and accepted: the CLI now holds a deployed
bundle's file **contents** in memory at the local and cluster tiers, which is the
security boundary decision 6 answers.

### 4. Two sentinels, never an omission and never an empty collection

Some facts are disk-only and some are deployed-only, so the ticket's "say so
explicitly rather than omitting it" constraint cuts both ways. Two distinct
sentinel shapes, because these are two distinct facts:

- `{"available": false, "reason": ..., "where": ...}`: the concept has **no
  meaning at this tier**. `where` names the tier or command that does have it.
- `{"resolved": false, "reason": ...}`: the concept exists here, but this
  bundle's state **blocked resolving it**. Always paired with a `diagnostics`
  entry carrying the machine code.

The inversion, in full:

| Fact | `skill` | `local` / `cluster` |
|---|---|---|
| `bundle.root` (a filesystem path) | real | unavailable |
| `bundle.deployed` (agent, version, sha, environment) | unavailable | real |
| `channel` | unavailable | real |
| `comms` connectedness | unavailable | real |
| `secrets.declared[].satisfied` | real (shell env or vault) | unavailable |
| `model` (mode, credential name, recorded runner) | real | unavailable |
| `mcp_servers[].load` under `--check-mcp` | probed | exit 1 on the flag, not yet built |
| `mcp_servers` themselves | read from `.mcp.json` | unresolved: the platform never ships the file |
| the `skill.symlink` diagnostic | can fire | cannot fire, by construction |

The `skill.symlink` row is by construction rather than by omission:
`bundle::pack_tar_gz` refuses to pack a symlink rather than dereference it, so a
stored bundle contains none. The `mcp_servers` row is a property of the platform's
stored-file allowlist, recorded as a known limitation under Consequences.

An entirely empty `skills/<dir>/` is deliberately **not** in this table, because
it is not a tier difference: neither a disk walk nor a stored tar records a
directory entry, only files, so the candidate is invisible at every tier alike.
`skills.dir_absent` states that it therefore claims nothing about empty
directories in either direction, rather than the pass inventing a tier-specific
fact about them.

Two invariants hold everywhere: no field is ever omitted, and no empty array is
ever emitted for an unresolved concept. That applies hardest where fabricating
one would be easiest: a deployed gap does not run the pass at all (every
bundle-derived block is `unresolved` with the gap's own reason and `artifacts`
carries no rows), an unparseable manifest with no readable `.mcp.json` leaves
`mcp_servers` unresolved under `mcp.manifest_unreadable` rather than reporting
`[]`, and a deployed tier reports `mcp.not_in_bundle_view` rather than borrowing
the skill tier's `mcp.no_declaration`, which asserts a file was read.

### 5. A bundle defect is a diagnosis at exit 0

`curie <tier> info` exits non-zero in exactly three situations:

- **exit 2 (Usage)**, at `skill` only: `--plugin-dir` does not exist, is not a
  directory, or holds no plugin manifest at either `.claude-plugin/plugin.json`
  or `plugin.json`. A directory with no manifest is not an incomplete bundle, it
  is not a bundle, which matches how `commands::check` already treats it.
- **exit 1 or 3 (Failure or Transient)**, at the deployed tiers: whatever the
  existing `ApiClient` paths already classify (an unknown agent, a connect
  failure). `info` adds no new classification.
- **exit 1 (Failure)**, on the flag: `--check-mcp` at `local` or `cluster`, per
  decision 6. Deliberately not exit 4: the probe is unbuilt at those tiers, not
  impossible there.

Everything else inside the bundle is a `diagnostics` entry at exit 0: a manifest
that is not valid JSON, an `approvalPolicy` the runner would refuse, a missing
eval suite, a `skills/<dir>` with no conforming `SKILL.md`, a server that fails
to load under `--check-mcp`. That extends to the deployed-side gaps: an agent
with no in-force deployment answers exit 0 with `deployed.no_active_deployment`,
because "nothing is running this agent's bundle" is a real answer rather than a
failure.

The consequence worth pinning: `parse_manifest_gates` returns an error on an
invalid `approvalPolicy`, and `info` **catches** it, converting it into a
diagnostic and marking `approval_gates` unresolved. It must never report an empty
gate list there.

### 6. `--check-mcp` at the deployed tiers is exit 1, not exit 4

The MCP load probe boots a runner container against a **bundle directory**
(`python -m curie_runner.check`), and this CLI does not write a deployed
bundle's stored files out to one. An earlier draft of this ADR called that
absence by construction and declined the flag with exit 4. **That was wrong, and
by decision 3's own test.** The bytes are reachable at the deployed tiers:
`ApiClient::bundle_files` returns `(path, content)` pairs, and
`commands::run_check_report` takes a `PathBuf`, so materializing the fetched
files into a temp directory and running the identical probe is mechanically
available today. Only the code to do it is missing, which is absence by
schedule.

The counter-argument considered and rejected: that a probe booted on the
operator's workstation measures whether the servers load *there* rather than in
the platform's own sandbox, making a reconstructed probe a different question
and therefore permanently misleading. It does not hold, because the alternative
this decline recommends is itself a local probe standing in for the deployed
one, and the probe is offline, credential-free, and runs the same runner image
at every tier. If a local probe answered a different question, the recommended
workaround would mislead too.

So `--check-mcp` is **declared at all three tiers and declined at `local` and
`cluster` with exit 1 plus a `fix`**. The declare-then-decline shape follows the
confirmed-sound `skill approvals --list` precedent (the flag is accepted so it
can be declined with a reason rather than rejected as an unknown-flag typo);
only the exit class differs, because exit 4 is reserved for what no input and no
future release can answer here (ADR-0041). An agent that reads exit 4 stops
retrying and stops asking, so spending it on a gap a follow-up will close is the
lie-class this ADR spent decision 3 rejecting.

Implementing the deployed-tier probe is a tracked follow-up, and it is the right
long-term answer.

The reason and the alternative are single-sourced as
`commands::INFO_CHECK_MCP_REASON` and `commands::INFO_CHECK_MCP_ALT`, and flow
into both the runtime `{error, fix}` payload and the clap help text, since
nothing gates prose against prose (#459).

Without the flag, `info` is static at every tier: no Docker, no container. Each
declared server carries `load: "not_probed"` plus an `mcp.not_probed` diagnostic
naming the way to find out. It never fabricates `load: "registered"`.

### 7. The committed schema, and the closed-`kind` / open-`code` split

Per ADR-0074 the family ships a committed, versioned schema,
`cli/schema/info.schema.json` at `$id` `.../cli/info/v1.json`, registered in
`cli/schema/index.json` as `InfoOutput` v1.

The `diagnostics` array carries two axes on purpose:

- **`diagnostic.kind` is a closed 10-value enum**: `approval_gate`, `artifact`,
  `boot_env`, `deployed`, `evals`, `manifest`, `mcp`, `secret`, `skill`,
  `state`. A consumer switches on it and is guaranteed a total match at v1.
- **`diagnostic.code` is an open string** whose prefix always equals its `kind`,
  with a v1 registry enumerated in the schema's own `description` and in the
  table below. The one documented exception is the `skills.*` family, which
  reports on the tree rather than on one skill and still carries `kind: "skill"`.

Splitting the two is what lets a new rejection reason ship without touching a
consumer.

The v1 `code` registry:

| kind | codes |
|---|---|
| `manifest` | `manifest.invalid_json`, `manifest.location_fallback`, `manifest.name_invalid` |
| `skill` | `skill.no_skill_md`, `skill.frontmatter_missing`, `skill.frontmatter_invalid`, `skill.tools_confusable`, `skill.symlink`, `skills.dir_absent`, `skills.empty` |
| `mcp` | `mcp.no_declaration`, `mcp.declared_none`, `mcp.manifest_unreadable`, `mcp.not_in_bundle_view`, `mcp.invalid_json`, `mcp.declared_pointer`, `mcp.not_probed`, `mcp.did_not_register`, `mcp.registered_zero_tools`, `mcp.probe_failed` |
| `evals` | `evals.file_absent`, `evals.invalid`, `evals.retired_format` |
| `secret` | `secret.unsatisfied`, `secret.name_invalid`, `secret.vault_unreadable` |
| `boot_env` | `boot_env.not_set_at_this_tier`, `boot_env.rows_scoped` |
| `approval_gate` | `approval_gate.manifest_invalid` |
| `artifact` | `artifact.symlink` (see below) |
| `state` | `state.runner_absent`, `state.foreign_runner`, `state.unreadable` |
| `deployed` | `deployed.no_active_deployment`, `deployed.bundle_unreadable` |

`artifact` carries exactly one `code` at v1, `artifact.symlink`: a `.curieignore`
that is a symlink, which `bundle::pack_tar_gz` refuses to pack, so the inventory
was built from the built-in exclusions alone and describes a directory the
platform cannot store. There is deliberately no `artifact.absent` beside it,
because every `artifacts[]` row's absence already has a dedicated `code`
elsewhere in the pass (`mcp.not_in_bundle_view`, `evals.file_absent`,
`skills.dir_absent`, the `manifest.*` family), and a second name for a rejection
already reported would split one consumer branch into two.

`state.unreadable` was added during implementation, for a `.curie/runner.json`
that exists but cannot be parsed. Reporting that as `state.runner_absent` would
say "absent" about a file that is present, which is this verb's own lie-class.
It is in the table because a new `code` is additive by the rule below and needed
no schema shape change to land.

**The v1 evolution rule**, derived from ADR-0074's compatibility policy, which
names a new enum value as explicitly additive:

- **Additive, edited in place at v1 with no version bump**: a new `code` string
  (no schema shape change at all, only the `description` registry and this
  table); a new `kind` enum value; a new optional field on the report; a new
  `mcpLoad` value; a new `credential.source` value.
- **Breaking, requires a v2 `$id` and a bumped `index.json` version**: removing
  or renaming a `kind` value; removing or renaming a `code` a consumer could have
  branched on; removing or retyping any field of `diagnostic`; making an optional
  report field required; changing either sentinel shape.
- **Consumer contract, stated in the schema `description`**: an unknown `code`
  must be treated as "a rejection with no specific branch", never as a parse
  failure. Failing closed on an unknown `code` is a consumer bug, not a contract
  break.

### 8. Never print a secret value, and no new manifest mirror

The payload sits next to declared connector-secret names, MCP `env` and
`headers` blocks, and model credentials, and at the deployed tiers this CLI now
holds a stored bundle's file contents in memory. The report therefore carries
**derived facts only**: names, counts, booleans, and paths.

Concretely:

- An MCP row collapses to exactly `commands::DeclaredServer`'s already-reviewed
  four fields (`name`, `source`, `form`, `authed`) plus a `load` status. `env`,
  `headers`, `args`, `url`, and `command` can each carry a literal token, so none
  of them has a field. `authed` is a boolean, never the credential block.
- Declared secrets are names plus a satisfaction boolean; there is no value field
  anywhere in the contract, and no `content` field at any level.
- The model credential is reported by **name** only, selected through the frozen
  `commands::select_passthrough_env` forwarding rule (#495) rather than a fork of
  it.
- `additionalProperties` is `false` at every level of the schema, so a field
  added carelessly fails the contract test rather than shipping.

A content-free inventory is only half the boundary, and review found the other
half open. A `diagnostics` entry describes a bundle **defect**, and every upstream
producer of that text (`serde_json`, `jsonschema`, the probe container, the
platform API) renders the offending value into its own message, so a token
mistyped into a manifest field, an eval suite, or an MCP block would be echoed
straight back through a `reason`. Collapsing an MCP row to four fields buys
nothing if the diagnostic beside it reprints the same `env` block verbatim.

So every diagnostic string is built through a shared `info::redact` module rather
than by per-site care: a JSON parse failure becomes its line and column, a type
mismatch becomes a type name, a schema violation becomes its two JSON pointers
(instance location and schema location). The rule is that a diagnostic says
**what** was wrong and **where**, never **what the content was**, and a pointer is
more useful to an agent than the echoed text it replaces. The module deliberately
does not scrub a finished string for secret-looking substrings: a denylist over
attacker-shaped text is not a boundary. Making this a helper rather than a
convention is what let it close a pre-existing leak at its root, in
`commands::validate_against_plugin_format_schema`, where `skill approvals` was
already formatting a `jsonschema::ValidationError` whose `Display` serializes the
failing instance (`mcpServers` is `anyOf [string, object, null]`, so declaring
servers as an array made it print the whole array, `env` blocks included).

`info` also introduces no new `Deserialize` mirror of the plugin-manifest shape:
it reads the manifest as a raw `serde_json::Value`, so
`cli/plugin-format-mirrors.json` gains no entry. `InfoOutput::to_json` delegates
wholesale over a `Serialize` struct rather than hand-projecting into a `json!`
literal, so it cannot drop a field by hand-picking one and needs no `emits` entry
in `cli/api-mirrors.json`.

## Consequences

- **An agent has one command to answer "what did the harness actually resolve".**
  Running it before reporting success turns a silently-empty inventory into a
  named rejection with a fix.
- **The bar for a new tier-absent answer is now explicit.** Decision 3 makes exit
  4 mean absent by construction and nothing else; a reviewer can ask "is this
  reachable today?" and, if the answer is yes, the verb gets implemented rather
  than declined. This is a tightening of ADR-0041 as applied, not a change to it.
- **The CLI reads deployed bundle file contents.** That is a new data flow at the
  local and cluster tiers. It is bounded by decision 8 (derived facts only,
  `additionalProperties: false`, no content field), and the contents are held in
  memory for the duration of one pass.
- **Known limitation: a deployed bundle is served through an allowlist, so the
  deployed tiers can never read `.mcp.json`.** The platform's `_collect_text_files`
  (`apps/api/src/curie_api/bundles.py`) is a fixed allowlist of the bundle's
  structured text surfaces: the two manifest locations, `evals/cases.json`, and
  `skills/**/SKILL.md`. Nothing else is collected, so a deployed view carries no
  `.mcp.json` whatever the bundle contains. This predates `info` and is not changed
  here. Its consequence is that the deployed tiers report MCP state as **unknown**
  (`mcp.not_in_bundle_view`, `mcp_servers` unresolved) rather than absent, and that
  `artifacts[].exists` means "present in the file view this report read" rather than
  "present in the bundle", which is why the same row can read `false` at `cluster`
  and `true` at `skill`. Widening the allowlist is a separate change to the platform
  API's response shape and is tracked as its own issue; until then, the manifest's
  own `mcpServers` fallback is the declaration form the deployed tiers can see, and
  the `fix` on the diagnostic says so.
- **The report is a maintenance surface.** An intentional output change edits
  `cli/schema/info.schema.json` in the same change, or `cli/tests/json_contract.rs`
  goes red; a new `code` also updates the registry in the schema `description` and
  the table above.
- **Debt, carried forward: `info` is a second reading of the bundle rules.** Its
  skill and MCP rejection rules are a Rust reading of the same file set
  `plugin_format.validate_bundle` reads authoritatively at deploy. ADR-0041
  recorded this gap for `skill approvals`; `parse_manifest_gates` since closed the
  manifest half by validating a declared `approvalPolicy` against the full frozen
  schema, but the skills and MCP halves are widened by one more consumer here.
  Closing it properly needs a shared or drift-gated parser, one source of truth
  both languages read. Tracked as a follow-up, and named here so the debt stays
  visible rather than being rediscovered.
- **Three verification gaps are follow-ups, not silent omissions.** `--check-mcp`
  is not yet exercised in the e2e ladder (it needs Docker), and the deployed
  populator is proven by a no-network cross-source parity test plus the
  already-shipped API client rather than by a live `local info` rung. The API
  chain it uses is already exercised by `deployed_manifest_gate_names`.

## Alternatives considered

1. **A top-level `curie info` with a `--tier` flag.** Shorter to type and matches
   some peer tools. Rejected: it breaks the target-noun-in-the-middle grammar
   issue #40 established, and would be the only environment verb that does.
2. **Answer `local info` and `cluster info` with exit 4.** Rejected in review, per
   decision 3. The data is reachable today, so exit 4 would have claimed absent by
   construction about something merely not yet built.
3. **Probe MCP load by default.** Rejected: the probe needs Docker and a runner
   image and takes tens of seconds, which would make `info` unusable as the fast
   pre-flight it exists to be. `not_probed` is stated explicitly instead of implied
   by absence.
4. **Fold the facts into the existing verbs** (extend `skill check`, add fields to
   `versions`). Rejected: it spreads one answer across three shapes with no shared
   contract, and leaves nothing that can state a rejection, which is the whole
   point of the verb.
5. **Emit an empty array for an unreadable inventory.** Rejected on the grounds
   this ADR opens with: it is indistinguishable from an intentionally empty bundle,
   and an agent cannot detect the difference.
6. **Two discovery implementations, one per data source.** Rejected: a diagnostic
   would then mean two different things depending on which code path produced it,
   and cross-tier divergence would be unattributable. One pure pass over a
   source-agnostic view makes parity checkable.

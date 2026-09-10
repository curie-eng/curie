# plugin-format

The plugin bundle format: the Claude Code plugin shape
verbatim, plus its validator. Compatibility with the Claude Code plugin format
is the distribution wedge, so this package does not invent format extensions.

## Stability

This is a frozen contract compiled against in three languages, for the same
reason as `aci-protocol`: every deploy path (the CLI scaffold, the bundle
pipeline, the runner's bundle loader) calls the single `validate_bundle`, so it
never changes from a dependent lane and a needed change lands as its own
reviewed change (see the frozen-interface rule below). Unlike `aci-protocol` it
carries no `PROTOCOL_VERSION`: the format is the Claude Code plugin shape
verbatim, and the models are lenient by design (`extra="allow"`) so real and
future Claude Code bundles that carry keys this MVP (Minimum Viable Product)
does not model still validate. It is v0.x, so breaking changes to the validator remain possible, but
they must stay backward-compatible with existing valid bundles and land as their
own reviewed change; the schema-compat gate (`tests/test_schema_compat.py`)
fails on any drift between the models and the committed schema.

## Format surface

Pydantic models mirroring the Claude Code shapes:

- `PluginManifest` (`.claude-plugin/plugin.json`): `name` required; optional
  `version`, `description`, `author` (string or `{name, email?, url?}`),
  `homepage`, `repository`, `license`, `keywords`, `commands`, `agents`,
  `hooks`, `mcpServers`. Unknown keys are accepted and preserved.
- `SkillFrontmatter` (`skills/**/SKILL.md` YAML frontmatter): `name` and
  `description` required; `allowed-tools` optional, accepted as either the
  space- or comma-separated string the Agent Skills specification calls
  canonical or as a YAML list. The authored shape is preserved verbatim;
  `parse_allowed_tools` is the only thing that turns it into entries.
- `McpServer` / `McpConfig` (`.mcp.json`): `mcpServers` maps a name to a server
  that is either stdio (`command`, `args?`, `env?`) or remote (`type`, `url`,
  `headers?`).
- `HookMatcherConfig` / `HookDefinition` (the manifest `hooks` field): `hooks`
  may be an inline object or a path to a hooks JSON file, shaped
  `{event: [{matcher?, hooks: [{type, command}]}]}` (the Claude Code hooks
  structure). `matcher` is a tool-name pattern (`"Bash"`, `"Write|Edit"`; absent
  = all tools); each action carries a `type` (today only `"command"`, which
  requires a non-empty `command`). **Deploy-time validation** rejects a missing
  hooks file, invalid JSON, a malformed shape, or a `command` hook with no
  command. **Runner consumption** (`runner`): the manifest's `PreToolUse`
  command hooks are translated into SDK `HookMatcher` callbacks and run before a
  matching tool call — exit 0 allows, exit 2 denies (stderr = reason), any other
  non-zero is a non-blocking hook error. Only `PreToolUse` is consumed today;
  other events validate but are not yet wired.
- `TriggerDeclaration` (the manifest `triggers` field, a Curie extension for
  triggers beyond chat, #273/#270): a list of `{type, ...}`. `type` is `cron`
  (requires a non-empty `schedule` cron expression) or `webhook` (requires a
  non-empty `path`). Declaring triggers in the bundle keeps an agent's full
  wake-up behavior in one reviewable artifact. **Deploy-time validation** rejects
  an unknown type, a cron trigger without a schedule, or a webhook trigger
  without a path. Runtime consumption (kernel cron scheduling / webhook ingress)
  is a separate not-yet-built seam (see `docs/interfaces/triggers/INTERFACE.md`),
  so this is validation only today.
- `ApprovalPolicy` / `ApprovalGate` (the manifest `approvalPolicy` field, #273):
  `{gates: [{gate, route, grantableViaPolicy, summary}]}` — each gate names a pause
  point and the approval route that decides it; both `gate` and `route` are
  required. Optional `summary` (#2565) is a bundle-authored sentence template
  rendered onto the approval card. **Deploy-time validation** rejects a
  malformed policy or a gate missing `gate`/`route`. It also rejects a gate whose
  (whitespace-stripped) name starts with `mcp__` but is not a live,
  fully-namespaced tool name for a server the bundle declares
  (`mcp__plugin_<bundle>_<server>__<tool>`, non-empty tool suffix) — the runner
  matches gates by exact string equality, so a mis-namespaced `mcp__` gate
  previously validated green but silently never armed (#453). Built-in gates
  (no `mcp__` prefix, e.g. `Bash`) are unaffected. The error message names the
  expected form; to arm a live tool name the bundle does not declare, use the
  per-agent `CURIE_APPROVAL_REQUIRED_TOOLS` env knob instead. Runtime approval
  routing is a separate not-yet-built seam, so this is validation only today.
- `ToolPolicy` (the manifest `toolPolicy` field): the vanilla MCP tool policy —
  `{enforcement, allow, approvalRequired, deny}`, three glob collections over
  **canonical `"<server>/<tool>"` tool names**. The server segment is required
  because two servers may publish the same tool name; the canonical form is
  deliberately **not** the SDK's live tool name, which is namespaced two
  different ways depending on how the server is mounted
  (`mcp__plugin_<bundle>_<server>__<tool>` for a plugin-loaded `mcpServers`
  entry, `mcp__<connector>__<tool>` for a `connectors.yaml` connector, #1495).
  Mapping canonical → live is the runtime's job, not the author's.
  - **Precedence is by class, never by specificity**: `deny` > `approvalRequired`
    > `allow`. A tool no collection matches is **DENIED**, so a tool a server
    starts advertising after the bundle was authored fails closed.
  - **Grammar** (`tool_policy.validate_pattern`): exactly one `/`, both segments
    non-empty, each segment drawn from `A-Za-z0-9_.-` plus the wildcards `*` and
    `?`. `**`, `[...]` character classes, whitespace, and any other character are
    rejected at deploy. `*` matches **within a segment only** and never crosses
    the `/`, and matching is **case-sensitive** (`fnmatch.fnmatchcase`).
  - **The grammar is narrower than a protocol-legal MCP name**, on purpose. MCP's
    tool-name character rule is a SHOULD and a server key may be any string, so a
    name this grammar cannot spell does exist (`@scope/github`, `search:docs`).
    The grammar constrains what a *pattern* may contain, not the runtime name
    matching applies to, so such a name cannot be targeted LITERALLY — no legal
    pattern spells it out exactly — but a legal WILDCARD pattern still reaches
    it: `grafana/search_*` matches the runtime tool `grafana/search:docs` via
    `fnmatchcase` on the unrestricted live name, even though `:` cannot appear in
    a literal pattern. That makes an unspellable name a **widening** path, not a
    narrowing one. Prefer a literal pattern for any name you can spell, and treat
    a wildcard as granting every name of that matched shape, spellable or not.
  - `enforcement` is a required, versioned discriminator
    (`"curie/mcp-tool-policy@1"`, exported as
    `plugin_format.TOOL_POLICY_ENFORCEMENT`). A future v2 semantics is a NEW id
    that a v1 build rejects rather than reinterpreting.
  - **The enforcement handshake.** `validate_bundle(path,
    enforces_tool_policy=...)` REJECTS a bundle that declares a `toolPolicy`
    unless the caller names the supported contract id
    (`tool_policy.unenforced`), and `load_tool_policy(manifest, enforces=...)`
    RAISES `ToolPolicyUnenforceable` rather than handing a policy to a caller
    that would not apply it. There is deliberately no return value meaning
    "declared but not enforced" — that shape is how a fail-open ships.
  - **Deploy-time validation** rejects a non-object policy, an unknown key
    (`ToolPolicy` is strict where the rest of this package is lenient: a
    misspelled collection such as `denny` would otherwise be silently dropped and
    a typo would become permission *widening*), an unsupported `enforcement` id,
    a malformed pattern, a pattern repeated within one collection, the identical
    pattern string in two collections, and a literal server segment naming a
    server the bundle declares in neither `mcpServers` nor `connectors.yaml`.
    Overlapping but *different* globs are legal and resolve by precedence. A
    policy with all three collections empty warns (`tool_policy.denies_everything`)
    but still validates: it denies everything, which is coherent.
  - **DECLARATION-ONLY today.** Nothing enforces a `toolPolicy` at runtime yet;
    that lane is a **blocking follow-up**, and **no bundle may ship a `toolPolicy`
    until it lands**. The residual gap, stated rather than discovered: a platform
    built before this package version does not model the key at all, and the
    lenient `PluginManifest` accepts and silently ignores it. Nothing in this
    package can reach such a platform — the `enforcement` discriminator and the
    handshake only gate consumers that already parse the field.
- `scripts/` is a directory convention (no manifest schema of its own).

`validate_bundle(path) -> ValidationResult` is the entry point the bundle pipeline calls. It
returns actionable, path-qualified issues instead of raising:

```python
from plugin_format import validate_bundle

result = validate_bundle("path/to/bundle")
if not result.valid:
    for issue in result.errors:
        print(issue.code, issue.location, issue.message)
```

Error codes include `bundle.missing`, `manifest.missing`,
`manifest.invalid_json`, `manifest.invalid`, `manifest.name_invalid`,
`skill.frontmatter_missing`, `skill.frontmatter_invalid`,
`skill.tools_confusable`, `mcp.invalid_json`, `mcp.server_incomplete`,
`mcp.declared_pointer`, `hooks.declared_missing`, `hooks.invalid_json`,
`hooks.invalid`, `hooks.command_missing`, `triggers.invalid`,
`triggers.unknown_type`, `triggers.cron_missing_schedule`,
`triggers.webhook_missing_path`, `approval_policy.invalid`,
`approval_policy.incomplete`, `approval_policy.gate_not_namespaced`,
`tool_policy.unenforced`, `tool_policy.invalid`,
`tool_policy.enforcement_unsupported`, `tool_policy.pattern_invalid`,
`tool_policy.pattern_duplicate`, `tool_policy.pattern_conflict`,
`tool_policy.unknown_server`, `scripts.not_a_directory`.

`tool_policy.denies_everything` is the one **warning** code in this list, not an
error: it reports a declared policy whose three collections are all empty, which
denies every tool. That is fail-closed and coherent, so it validates.

## Frozen-interface rule

This package is a **frozen interface** for the same reasons as `aci-protocol`:
compatibility is the wedge. Do not change it unilaterally; a needed change stops
the task and escalates to the maintainers. Any change must regenerate the
committed schema with `scripts/check-contracts.sh` (which runs
`python -m plugin_format.schema_export`) and commit it. The compat gate
(`tests/test_schema_compat.py`) fails on drift.

## Decisions made under ambiguity

- **`allowed-tools`, not `tools`.** The task shorthand said SKILL.md frontmatter
  carries `name, description, tools`. The verbatim Claude Code / Agent Skills
  field is `allowed-tools`; using the real field name is the compatibility
  choice (the wedge), so the model exposes `allowed_tools` aliased to
  `allowed-tools`. A bundle written for Claude Code validates unchanged.
- **Two conformance profiles, one rule set (ADR-0135).** A bundle is judged
  against Claude Code's ingestion shape and against the Agent Skills
  specification, and the two genuinely diverge in both directions. So
  `validate_bundle` takes a `profile`: `claude-plugin` (the **default**, and the
  ingestion contract) reports every specification divergence as a
  `skill.spec_nonconformant.*` **warning**, so widening what we accept can never
  break a deploy; `agent-skills-strict` reports the same findings as **errors**
  and is a publishability gate, never an ingestion default. Runner boot and
  deploy ingestion pass no profile and stay lenient permanently. The one finding
  that stays a warning in both profiles is the `allowed-tools` block list, which
  the reference validator accepts even though the spec prose does not. A typo'd
  profile id raises `ValueError` rather than falling back — a silent fallback to
  the lenient profile would report a bundle as publishable that was never
  strictly checked.
- **The canonical `allowed-tools` is one space-separated string.** Both shapes
  are accepted forever, but the string is what `curie init` emits and what the
  strict profile asks for. It carries one consequence worth stating: entries are
  separated by whitespace or a comma **at paren depth 0**, so `Bash(git
  commit:*)` round-trips intact while `Bash(git` (unbalanced) and `Read,Write`
  cannot and are reported as `allowed_tools_unserializable`. Every consumer must
  read the field through `parse_allowed_tools`; a second reader is how a skill's
  declared tools become invisible to the runner's gate-shadow check (#1852).
- **Lenient models (`extra="allow"`).** Real bundles and future Claude Code
  versions carry manifest and frontmatter keys this MVP does not model. Rejecting
  them would reject valid bundles, so the models accept and preserve unknown
  keys rather than forbidding them. (This is the opposite of `aci-protocol`,
  whose wire contract is strict.)
- **Manifest location.** The canonical location is `.claude-plugin/plugin.json`;
  a bare `plugin.json` at the bundle root is accepted as a fallback.
- **`McpServer` is one permissive model.** Rather than a strict stdio-vs-remote
  union, a single model with all fields optional stays forward compatible; the
  validator enforces that each server defines either `command` or `url`.

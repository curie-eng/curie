---
seam: Bundle format
kind: CLEAN, frozen
impls: "1"
grade: not separately graded
epics:
  - "#30"
order: 12
---

# INTERFACE: Bundle format

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN, frozen &nbsp;·&nbsp; **Implementations today:** 1 &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The frozen bundle/plugin manifest format: the **Claude Code plugin shape verbatim**, a deliberate
distribution wedge. What is swappable is the harness that consumes a bundle; what stays fixed is
the shape a bundle must have to be accepted. The base is the Claude Code plugin shape, and the
models are lenient (`extra="allow"`) rather than strict so any bundle written for Claude Code
validates unchanged. On top of that base the package **does add six Curie authoring
extensions** — `systemPrompt`, `starterPrompts`, `secrets`, `triggers`, `approvalPolicy`,
`toolPolicy` on `packages/plugin-format/src/plugin_format/models.py::PluginManifest`, optional
fields Claude Code does not define. Leniency is what lets the Claude Code base and these
extensions coexist; the earlier "does not invent format extensions" framing was wrong.

The manifest is not the whole bundle either. Three **Curie-only root files** sit beside the Claude
Code surfaces and are validated by the same entry point: `connectors.yaml` (ADR-0086, Accepted), its
generated companion `connectors.lock.yaml` (ADR-0113, Accepted), and `deploy.yaml` (ADR-0089,
Accepted). None has a Claude Code counterpart, so none is lenient:
all three parse under `extra="forbid"` (`packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec`,
`packages/plugin-format/src/plugin_format/connector_lock.py::ConnectorLockEntry`,
`packages/plugin-format/src/plugin_format/deploy_targets.py::DeployTarget`), because there is no
external producer that could legitimately carry a key the models do not know, so an unrecognised key
is a typo rather than a future Claude Code field. The lock is the one root file no human authors --
`curie build` writes it and the bundle carries it -- but it is part of the format all the same,
because a second consumer that ignores it deploys a different image than the one the bundle's source
was built into. What a bundle actually is, then, is the Claude Code
plugin shape verbatim plus a strict Curie-only overlay, not a Claude Code plugin end to end.

## Current contract

`validate_bundle(path) -> ValidationResult` is the single entry point every deploy path calls
(`packages/plugin-format/src/plugin_format/validate.py::validate_bundle`). It returns path-qualified
issues (codes like `manifest.missing`, `manifest.name_invalid`, `mcp.server_incomplete`) instead of
raising. The shapes it checks (`packages/plugin-format/src/plugin_format/models.py`):
`PluginManifest` (`packages/plugin-format/src/plugin_format/models.py::PluginManifest`, the
`plugin.json` manifest) with `name` required and optional `version`, `description`, `author`,
`homepage`, `repository`, `license`, `keywords`, `commands`, `agents`, `hooks`, `mcpServers`;
`SkillFrontmatter`
(`packages/plugin-format/src/plugin_format/models.py::SkillFrontmatter`, a `SKILL.md` frontmatter)
with `name`/`description` required and `allowed_tools` aliased to the verbatim `allowed-tools` key,
which accepts **either** a string or a list of strings — the canonical serialized shape is the
space-separated string (`allowed-tools: Read Bash`), split on whitespace or a comma at parenthesis
depth 0 so a specifier such as `Bash(git commit:*)` stays one entry, and the list forms are accepted
for compatibility ([ADR-0135](../../adr/0135-a-skill-bundle-has-two-conformance-profiles.md), Draft,
which also adds the second, non-default `agent-skills-strict` validation profile);
`McpServer`/`McpConfig` (`packages/plugin-format/src/plugin_format/models.py::McpConfig`, the
`.mcp.json` file) where the validator enforces each server define `command` (stdio) or `url`
(remote).

Those are the Claude-Code-shaped surfaces. `validate_bundle` also validates three **Curie-only root
files**, each absent from a bundle that needs none, all three invisible to Claude Code:

- `connectors.yaml` (ADR-0086, `packages/plugin-format/src/plugin_format/connectors.py::ConnectorsFile`)
  declares the MCP servers Curie should run or reach on the bundle's behalf, keyed by connector name.
  Each entry is a `packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec` in exactly
  one of three mutually exclusive forms: hosted by reference (`image`, plus `args`/`env`/`port`
  and optional `unhosted_url`),
  hosted from source (`build`, ADR-0113, the same hosted form with the image sourced rather than
  named, also with optional `unhosted_url`), or remote (`url`, plus `headers`). More than one set on one connector is
  `connectors.ambiguous`, none set is `connectors.underspecified`. A `build` block
  (`packages/plugin-format/src/plugin_format/connectors.py::ConnectorBuild`) carries a
  bundle-relative `context`, a `dockerfile` under it, and a required non-empty `platforms` list; it
  never carries a digest, which lives only in the lock. Every entry names the credentials it needs:
  `secrets` is `list[str | SecretRef]`
  (`packages/plugin-format/src/plugin_format/connectors.py::SecretRef`, with
  `name` / `from_secret` / `key`) and `secret_files` is by NAME only; neither form
  carries a value. Intake refuses any `sealed_secrets` declaration with
  `connectors.sealed_secrets_unsupported` until a decrypt path exists (see
  [sealed-credential](../sealed-credential/INTERFACE.md)). For a hosted (`image`/`build`) connector
  with `secrets:` declared, the rendered `.mcp.json` entry derives
  `headers.Authorization: Bearer ${<bearer_secret, or the single declared secret>}` -- the author still writes no `headers:`
  on a hosted connector (`connectors.hosted_has_headers`) -- and `cluster deploy` binds only that
  Bearer name into the sandbox alongside any explicit `--secret` so the runner can expand the
  placeholder at boot and then drop the name from the process env (#2503, #2559; a
  `SecretRef` value does not reach the sandbox under ADR-0090, and the `url` fallback used by tiers
  below cluster derives no header, both tracked as follow-ups). A hosted connector with several
  secrets and no `bearer_secret` is `connectors.bearer_secret_required`. Validated by `packages/plugin-format/src/plugin_format/validate.py::_validate_connectors`,
  which emits `connectors.*` codes (`connectors.not_object`, `connectors.ambiguous`,
  `connectors.underspecified`, `connectors.reserved_name`, `connectors.duplicate_connector`,
  `connectors.duplicate_server`, `connectors.build_context_escapes`,
  `connectors.build_no_platforms`, `connectors.ambiguous_name` (a connector name that contains
  `-mcp-` or STARTS with `mcp-`, forging the `-mcp-` join used to render the connector's object
  name, so two different (agent, connector) pairs would render byte-identical objects — checked
  only for a hosted connector, one declaring `image:`; a remote connector, declaring `url:`,
  derives no Kubernetes object name, since `render()` emits no objects for it and its `.mcp.json`
  entry is the authored URL, so its name is not checked), and others). Authored mapping keys are checked for
  duplicates before validation, so a repeated connector name is rejected rather than
  silently replaced by the last YAML value.
- `connectors.lock.yaml` (ADR-0113,
  `packages/plugin-format/src/plugin_format/connector_lock.py::ConnectorLockFile`) records what each
  declared `build` resolved to. It is **generated, not authored**: `curie build` writes it and it is
  packed into the bundle like any other file, so the platform holds the exact digest a version
  deployed rather than that fact living in local CLI state. One
  `packages/plugin-format/src/plugin_format/connector_lock.py::ConnectorLockEntry` per built
  connector carries `image`, `delivery` (`registry` or `local-daemon`), the `platforms` the build
  targeted, and `source_digest`. Identity is a digest and only a digest: `image` is
  `<repo>@sha256:<64 hex>` for `registry` delivery and a bare `sha256:<64 hex>` image ID for
  `local-daemon`, and a mutable tag is refused, because a tag can be repointed at a different
  artifact after review. `source_digest` is the content-derived identity of the build INPUT
  (`packages/plugin-format/src/plugin_format/connector_lock.py::source_digest_of`), hashing the
  context's files -- each one's bytes plus whether it is executable by its owner, the one mode bit
  the build context tar carries into the image -- and the declared `build` block together, honoring
  the context's `.dockerignore`. The generated `connectors.lock.yaml` is excluded so writing the
  lock cannot invalidate the digest it just recorded, but only when the declared `build.context` is
  the bundle root (`.` or empty), which is the one place `curie build` writes it. Under a
  subdirectory context, a `connectors.lock.yaml` at the top of that context is authored input the
  daemon receives and is hashed like any other file.
  `packages/plugin-format/src/plugin_format/validate.py::_validate_connector_lock` is where intake
  refuses: a `build` connector whose declared context is not in the bundle is
  `connectors.build_context_missing`, one with no lock entry is `connectors.lock_missing`, one whose
  recomputed
  source digest no longer matches is `connectors.lock_stale`, an unreadable or unknown-version file
  is `connectors.lock_unreadable` / `connectors.lock_unsupported_version`, and a lock whose image
  does not match the delivery it claims is `connectors.lock_invalid`. That last check is delegated to
  `packages/plugin-format/src/plugin_format/connector_lock.py::apply_lock`, the single place a
  `build` connector becomes an ordinary `image` one, so a hand-edited lock is refused by the same
  rule a generated one passes. The recomputation is pure hashing over the extracted tree -- no
  Docker, no registry, no network -- which is what lets the API run it and stay a pure renderer
  (ADR-0087). Delivery is deliberately NOT judged here: a `local-daemon` lock is legitimate for a
  local-tier deploy, and refusing it belongs to the cluster preflight.
- `deploy.yaml` (ADR-0089, `packages/plugin-format/src/plugin_format/deploy_targets.py::DeployTargetsFile`)
  declares named deploy targets under a `targets` map, each a
  `packages/plugin-format/src/plugin_format/deploy_targets.py::DeployTarget` of
  `{agent, env, slack_channel}` where `env` is `dev` or `prod`. Validated by
  `packages/plugin-format/src/plugin_format/validate.py::_validate_deploy_targets`, which emits
  `deploy.*` codes (`deploy.not_object`, `deploy.duplicate_target`, `deploy.bad_target_name`,
  `deploy.bad_env`, `deploy.missing_agent` (a declared target must name its agent; the error names
  the target key), `deploy.bad_agent_name`, `deploy.ambiguous_agent_name` (the agent, not the
  connector, must not contain `-mcp-` or END in `-mcp`, since the agent sits immediately left of
  the `-mcp-` join — same collision as `connectors.ambiguous_name`, viewed from the other side of
  the join),
  `deploy.bad_slack_channel`). Authored mapping keys are
  checked for duplicates before validation, so a repeated target name fails closed instead of
  silently selecting the last YAML value.

The overlay files are not independent of the manifest, which is the part a second consumer is
most likely to miss: `connectors.yaml` feeds manifest validation. The set of gate names
`approvalPolicy` may legally use is built from BOTH the bundle's declared MCP servers and its
`connectors.yaml` connectors, under two different namespacing rules (a plugin-loaded server's live
tool is `mcp__plugin_<bundle>_<server>__<tool>`; a connector is mounted directly, so its live tool is
`mcp__<connector>__<tool>` with no infix). A consumer that reads the manifest alone rejects every
connector gate as `approval_policy.gate_not_namespaced`
(`packages/plugin-format/src/plugin_format/validate.py::_validate_approval_policy`).

A second consumer must accept all of these shapes, the Claude-Code-shaped ones and the Curie-only
overlay both.

The seam is bidirectional, and both directions are contracts. **Inbound**, leniency means any
bundle written for Claude Code validates here unchanged. **Outbound**, a Curie bundle must
validate unmodified as a Claude Code plugin — that direction is what makes the shape a
distribution wedge rather than a lookalike. The gate that defends it is
`scripts/check-plugin-compat.sh` (run it as `curie dev plugin-compat`), which discovers every
bundle under `examples/` and asserts `claude plugin validate` exits 0 for each. CI runs the same
script from `.github/workflows/plugin-compat.yaml` on two triggers, because drift arrives from two
directions: a path-filtered `pull_request` trigger catches our own drift when we touch the bundles
or the format models, and a nightly `schedule` catches Claude Code changing the format under us,
which no PR of ours would ever surface. The check is deliberately not `--strict`: strict mode
promotes unknown-field warnings to errors, and the six Curie authoring extensions are
unknown-to-Claude-Code by design, so warnings are the expected steady state and only a non-zero
exit is a failure.

Outbound compatibility and **spec conformance** are different contracts, and the second needs its
own gate: Claude Code accepting our bundles does not establish that the skills inside them satisfy
the published Agent Skills spec, which is stricter about what a `SKILL.md` frontmatter may contain.
`scripts/check-agent-skills.sh` (run it as `curie dev agent-skills`) defends that direction, and CI
runs the same script from `.github/workflows/agent-skills.yaml`. Determinism here takes three pins
working together, not one: `SKILLS_REF_VERSION` fixes the reference validator at
`skills-ref==0.1.1`, `SKILLS_REF_EXCLUDE_NEWER` is threaded through every `uvx` invocation as a
resolution cutoff that freezes the transitive dependency set, and the workflow pins the uv version
on its `setup-uv` step because uv's resolver is what picks those transitive deps. The transitive
set matters because it decides verdicts — `skills-ref` declares floating dependencies, and
strictyaml's refusal of JSON-style flow collections is precisely what makes `allowed-tools: []`
fail, so a strictyaml release could flip the gate with no change of ours. Given those three pins
and a reachable PyPI the gate is deterministic, which is why this workflow has no nightly
`schedule` trigger while plugin-compat does, since a pinned validator cannot drift between runs and
adopting a newer spec revision is a reviewed edit. The check runs over an explicit allowlist rather
than pure discovery, and a discovery drift check asserts that allowlist covers exactly the skills
found under the Curie-owned roots, so a newly added skill cannot silently escape the gate by being
unlisted. The script also preflights the validator and refuses to emit any verdict when it cannot
be resolved or launched, so a network failure reds the gate rather than being mistaken for a skill
being invalid — which is what lets the next claim be trusted. The fixture
`packages/plugin-format/tests/fixtures/bad_skill/skills/broken` is carried as an **asserted
negative**: the gate requires the reference validator to keep rejecting it, which makes
the deliberately malformed fixture proof that the gate is not vacuous rather than an unexplained
exclusion. Note that this constrains Curie's OWN skills only — the `plugin_format` loader stays
deliberately lenient and keeps accepting Claude-Code-shaped bundles carrying keys the spec does not
know about, and the two bars are intentionally different.

Conforming had one real cost, paid once. `.claude/skills/implement` is the single skill the gate
forced a migration on: its top-level `disable-model-invocation: true` is not one of the fixed
fields the spec allows, so it moved under `metadata`, the spec's designated slot for vendor
extensions. That keeps the declaration but drops its effect — Claude Code reads the key only at the
top level, so it no longer enforces slash-command-only invocation for that skill. Nothing in this
repo reads the key, and the trade is accepted deliberately.

## Implementations today

One: the `plugin_format` package. **Unlike `aci-protocol`, it is NOT tri-language with generated
types.** The Pydantic models are the source of truth and a committed JSON Schema
(`packages/plugin-format/schema/plugin-format.schema.json`) is regenerated and drift-checked by
`packages/plugin-format/tests/test_schema_compat.py`, but there is **no generated Rust or TS** in
the package (contrast `packages/aci-protocol/generated/`). The CLI hand-mirrors the format in
Rust and is kept honest by `cli/plugin-format-mirrors.json` plus `curie dev field-parity`
(`cli/scripts/check-field-parity.sh`). That gate only sees `schema_export` models;
Connector* field honesty is `tests/vectors/connector-fields.json`. The TypeScript (UI)
consumer remains a hand-written
mirror. It also carries no `PROTOCOL_VERSION`; the
format is pinned to the Claude Code shape and the models are lenient by design so future Claude
Code keys still validate.

## Known leakage

By intent, the manifest, skill, and MCP surfaces are Claude-Code-shaped — that is the wedge, not a
leak. What the wedge
costs is asymmetric fidelity: a bundle loaded by Claude Code **validates but degrades**. All six
Curie authoring extensions — `systemPrompt`, `starterPrompts`, `secrets`, `triggers`,
`approvalPolicy`, `toolPolicy` — are unknown fields to Claude Code, which warns about each and then
silently ignores it at load time. The manifest is accepted and the commands, agents, hooks, and MCP
servers work; the agent's persona, its suggested openers, its secret declarations, its wake-up
triggers, its approval gates, and its tool policy do not travel. That degradation is by design
(there is nowhere in the Claude Code shape to put them), but it is silent from the operator's side,
so it is documented here rather than discovered.

`toolPolicy` degrades **worse than the other five**, and the difference is the reason it is
gated the way it is. The rest lose a capability: the agent is less useful. This one loses a
**restriction**: a bundle whose tool surface is fenced by `toolPolicy`
(`packages/plugin-format/src/plugin_format/models.py::ToolPolicy`) runs *unfenced* in Claude
Code, with the manifest still claiming otherwise. That asymmetry is why the declaration carries a
versioned `enforcement` discriminator, why `validate_bundle` refuses a policy-bearing bundle unless
its caller states which contract it enforces
(`packages/plugin-format/src/plugin_format/validate.py::_validate_tool_policy`, code
`tool_policy.unenforced`), and why bundle adoption is blocked on the runtime lane. **This change is
declaration and validation only: nothing enforces a `toolPolicy` at runtime today, that is a
separate blocking follow-up, and no bundle may ship a policy until it lands.** The residual gap that
no in-package mechanism can close: a platform built before this package version does not model the
key at all, and the lenient models accept and silently ignore it.

The two Curie-only root files degrade **more quietly still**, and they belong on the same list. A
manifest extension at least draws a warning: `claude plugin validate examples/compat-fixture` (the
fixture at `examples/compat-fixture/.claude-plugin/plugin.json`, which exists to carry all six)
reports one `Unknown field ... Claude Code ignores it at load time` warning per extension.
`connectors.yaml` and `deploy.yaml` are not manifest fields at all, so Claude Code neither loads them
nor mentions them: validating `examples/weather` (which carries `examples/weather/connectors.yaml`)
warns only about the manifest's `starterPrompts` and says nothing about the connector file. A bundle
whose tool surface is declared through `connectors.yaml` therefore loads in Claude Code with those
servers simply absent, and with no signal at all that anything was dropped. `deploy.yaml` degrades
harmlessly by comparison, since routing is Curie's concern and Claude Code has nothing to route.

The outbound gate covers this unevenly, and the gap is worth naming.
`examples/tests/test_plugin_compat_coverage.py` pins that each of the six manifest extensions
appears in at least one discovered example bundle, so the gate cannot cover them vacuously, but it
checks manifest FIELDS only. `connectors.yaml` is exercised incidentally because the weather bundle
happens to carry one; **no example bundle carries a `deploy.yaml`**, so nothing asserts that Claude
Code still tolerates that file, and nothing would fail if the weather connector file were removed.

The `hooks`
field is no longer dead: as of #272 it is validated at deploy time (`HookMatcherConfig` /
`HookDefinition` in `models.py`, enforced by `_validate_hooks` in `validate.py`) and its
`PreToolUse` command hooks are consumed by the runner (`runner/src/curie_runner/hooks.py`),
which translates them into SDK `HookMatcher` guardrails that run before a matching tool call (exit 0
allows, exit 2 denies). Command hooks are advisory-unless-exit-2 (fail-OPEN): only a clean exit 2
denies the call; any non-2 exit, a timeout (60s budget), or a spawn failure is treated as a
non-blocking hook error and the tool call proceeds -- so a command hook is not a fail-closed security
control, matching Claude Code convention for author-declared hooks. Only `PreToolUse` is wired today;
other hook events validate but are not yet consumed. Epic #30 continues to define the remaining authoring extensions (approval-policy and
trigger declarations) alongside this.

Three of those Curie authoring extensions are **deploy-time validated** (shape enforced, malformed
declarations rejected), but they differ in whether the runtime acts on them yet:

- `approvalPolicy` (`{gates: [{gate, route, grantableViaPolicy, summary}]}` approval declarations, #273) is **consumed at
  runtime**, not merely validated: the runner reads the gates at boot (`load_approval_policy`,
  #247 / ADR-0010) and arms each `{gate, route}` on the permission gate — so calling this
  "not-yet-built" is stale (see the [approval seam](../approval/INTERFACE.md)). A declared
  policy is armed **exactly as declared or the runner refuses to boot** (#520, ADR-0050):
  every distinct declared gate name must arm, and a policy that is declared but unparseable
  (or a manifest that cannot be read at all) fails the boot rather than degrading to an
  unarmed empty map, which would restore the ungated bypass posture. A gate carries a third
  field beyond `gate`/`route`: `grantableViaPolicy`
  (`packages/plugin-format/src/plugin_format/models.py::ApprovalGate`, default `false`, #558) is
  the operator opt-in that lets a policy-gate approval on that route mint a one-shot grant for the
  tool the gate names. Deploy rejects an opt-in whose route is claimed by more than one distinct
  tool (`approval_policy.grant_route_ambiguous`), because the shared normalizer
  (`packages/plugin-format/src/plugin_format/approval_policy.py::grantable_routes`, the same helper
  the runner's loader calls) excludes an ambiguous route, so the policy would otherwise validate
  green and arm no grant. A fourth optional field, `summary` (#2565, Draft ADR-0151), is a
  bundle-authored sentence template rendered from the blocked call's arguments (`{ident}`
  scalars, `{ident|count}` / `{ident|length}`). The grammar lives in
  `packages/plugin-format/src/plugin_format/gate_summary.py` so the deploy validator and the
  runtime renderer cannot drift. A gate that omits `summary` keeps today's machine
  `summarize_tool_call` string on the card and notice.
- `triggers` (a list of `cron`/`webhook` declarations for waking the agent beyond chat, #273/#270 —
  see the [triggers seam](../triggers/INTERFACE.md)) is still **declaration-only**: its validator
  runs at deploy, but no runtime scheduler/ingress consumes a declared trigger yet (Epic #29).
- `toolPolicy` (`{enforcement, allow, approvalRequired, deny}` glob collections over canonical
  `"<server>/<tool>"` MCP tool names,
  `packages/plugin-format/src/plugin_format/models.py::ToolPolicy`) is **declaration-only and
  fenced as such**. Precedence is by class — `deny` > `approvalRequired` > `allow` — and an
  unmatched tool is DENIED, so server tool-surface drift fails closed. The grammar and pattern
  rules live in one shared module, `packages/plugin-format/src/plugin_format/tool_policy.py`, so
  the deploy validator and the future runtime loader normalize identically — the #453/#544 lesson
  that normalizing separately silently disagrees and ships a fail-open. The deploy validator
  (`packages/plugin-format/src/plugin_format/validate.py::_validate_tool_policy`) calls that
  module's `check_policy_patterns` / `validate_pattern` (grammar, duplicates, cross-collection
  conflicts) and `literal_server_segment` (the declared-server cross-check) today; it never
  classifies a live tool, because at deploy time there is no live tool surface to classify.
  `packages/plugin-format/src/plugin_format/tool_policy.py::classify_tool`, the precedence ladder that turns a policy plus a runtime tool
  name into allow/approval-required/deny, belongs to the not-yet-built runtime enforcement lane —
  the deploy validator does not call it. Because a declared-but-unenforced restriction is worse
  than no restriction, `validate_bundle`
  takes an `enforces_tool_policy` handshake argument and REFUSES a policy-bearing bundle from any
  caller that does not name `curie/mcp-tool-policy@1`; `load_tool_policy` raises rather than
  returning a policy such a caller would not apply. Runtime enforcement is a separate **blocking**
  follow-up, and no bundle may ship a `toolPolicy` until it lands.

Their validators live alongside the others in `validate.py` (`triggers.*` / `approval_policy.*` /
`tool_policy.*` error codes).

## Cross-links

- **Guide:** [workflow-agent-conversion.md](./workflow-agent-conversion.md) — converting an existing workflow agent (deterministic pipeline + LLM at the edges) onto a bundle end to end (#275).
- **Epic(s):** [#30](https://github.com/curie-eng/curie/issues/30) — document the dead `hooks` field and new approval/trigger declarations: each field's meaning, validation contract, and runner consumption
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — the plugin format is the distribution wedge; not one of the six swap-readiness Jobs
- **ADR(s):** [ADR-0005](../../adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md) — freezes `plugin-format` (with `aci-protocol`) as an interface built first; [ADR-0086](../../adr/0086-bundles-declare-connectors-the-platform-hosts-them.md) (Accepted) — adds `connectors.yaml` to the bundle root; [ADR-0089](../../adr/0089-bundles-declare-their-deploy-targets.md) (Accepted) — adds `deploy.yaml` to the bundle root; [ADR-0113](../../adr/0113-bundles-declare-connector-build-inputs-and-tiers-deliver-pinned-images.md) (Accepted) — adds the `build:` connector form and the generated `connectors.lock.yaml` that pins what it resolved to

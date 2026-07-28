# CLAUDE.md - cli

The `curie` CLI: Rust, clap + tokio + reqwest. Speaks only the
frozen contracts (the generated `curie-aci-protocol` crate over HTTP/NDJSON,
and the platform API's committed `apps/api/openapi.json`) and orchestrates a
local runner container via Docker. Three command families: `skill` drives a
plugin against a local runner with `up`, `down`, `status`, `info`, `message`, and
`eval`; `local` wraps the compose stack and local API with `up`, `down`,
`status`, `info`, `comms`, `message`, `deploy`, and `observability`; `cluster` wraps
Helm and the deployed release with `up`, `status`, `down`, `info`, `comms`, `message`,
`deploy`, and `observability`. Full command reference in
`cli/README.md`.

## Load-bearing invariants

- **Under `--json`, the agent-facing read and result verbs emit one JSON object
  to stdout -- never empty stdout (issue #456).** That covers the read/query
  verbs (`versions`, `memory`, `approvals`, `observability`), the lifecycle
  result verbs (`kill`, `resume`, `budget`, `delete`), `init` (both the
  plain-name and `--from-spec` branches, via `InitOutput`, issue #485), and every
  verb's `--dry-run` plan. Silent empty-stdout-exit-0 is the worst failure mode for an
  agent consumer: it looks like success but carries no data. The json-vs-human
  decision lives in exactly one place, `Ui::emit` (the success-path mirror of
  `main.rs`'s centralized error emit). A new or refactored verb returns a
  `CliOutput` -- a typed output object (e.g. `VersionsOutput`, `KillOutput`), or
  `DryRunPlan { lines }` for a `--dry-run` plan -- and routes it through
  `Ui::emit`; handlers must not call the stdout emitters
  (`payload`/`payload_plain`/`kv`) directly, since those suppress under `--json`.
  The operator verbs (`up`, `down`, `status`, `comms`), `deploy`, and `skill
  message` now return typed `CliOutput`s on their real-path success too (#485):
  the operator/deploy verbs route through `Ui::emit`, and `skill message` buffers
  the streamed reply into one `SkillMessageOutput` under `--json` (it still
  streams live in the human path). The schema-gated ADR-0021 builders
  (`skill status`/`skill eval`, `skill check`, `local message`/`cluster message`,
  `secrets list`, `guide`, `info`) now route through `Ui::emit` too (#474): each returns a
  typed `CliOutput` whose `to_json` delegates to its unchanged pure builder, so the
  committed schemas stay byte-for-byte identical while the json-vs-human decision
  lives only in `Ui::emit`. `InfoOutput` (`cli/src/info.rs`, #1040, ADR-0083) is the
  newest of these and the reference shape for a new family: its `to_json` delegates
  wholesale via `serde_json::to_value` over a `Serialize` report rather than
  hand-picking fields into a `json!` literal, so it needs **no** `emits` entry in
  `cli/api-mirrors.json` (see the emit-parity invariant below). `info` also converts
  every bundle-CONTENT defect (an unparseable manifest, a `skills/<dir>` with no
  `SKILL.md`, a missing `evals/cases.json`, an `approvalPolicy` the runner would
  refuse, a deployment with no in-force version) into a `diagnostics` entry at exit
  0, and only a genuinely unusable invocation (a `--plugin-dir` that does not exist
  or holds no plugin manifest at all) is a usage error -- do not "fix" a diagnosed
  defect into a failing exit, since a caller that must first learn WHAT is wrong
  cannot be handed an error instead of the report. The one intentional non-`CliOutput` emit is the
  centralized error path in `main.rs` (`error_json` under `--json`): errors are not
  success-path values, so they stay on the error-emit mirror rather than the
  success-path `CliOutput` contract.
- **The command manifest is a committed artifact; regenerate it in the same
  change as any command-surface edit (console/CLI parity, epic #145).** Any
  change to a clap `Command`/subcommand or an `*Action` enum — a renamed verb,
  a new subcommand, a changed flag — must regenerate `cli/command-manifest.json`
  and the committed `apps/ui/src/generated/commandManifest.ts` (`pnpm
  gen:manifest` in `apps/ui`) in the same commit. CI drift-checks the committed
  TS manifest, and the console's `cliCommand()` hints are typed against it, so a
  stale manifest breaks the UI build. Never edit the manifest by hand.
- **Never hand-write the ACI types.** `Cargo.toml` depends on
  `curie-aci-protocol` at `../packages/aci-protocol/generated/rust` --
  that crate is generated from the frozen Pydantic models. If a type you
  need is missing, that is a contract gap (raise it in an issue/PR first per
  the root AGENTS.md frozen-contracts rule), not something to redefine locally
  in `cli/src`.
- **No new HTTP crates.** `reqwest` (rustls-tls, no OpenSSL) is the one HTTP
  client in this binary; do not add a second one for a specific endpoint.
  Keep `reqwest`'s feature set minimal (`json`, `stream`, `multipart`) --
  adding a feature should come with a reason in the PR, not just convenience.
- **The queue payload is the frozen `QueuedTurn` contract, not a hand-mirror.**
  `curie skill message` talks straight to a local runner container's ACI HTTP
  surface (`/v1/event`, `/v1/steer`, `/v1/interrupt`), bypassing the
  dispatcher/worker entirely by design (that is the point: zero Slack, zero
  cluster). The `local message` / `cluster message` drivers do enqueue onto the
  real Valkey Stream, and the payload they mint is `curie_aci_protocol::QueuedTurn`
  (promoted into `packages/aci-protocol` by issue #7) consumed from the generated
  crate -- never redefine it locally in `cli/src`. `cli/src/queue.rs` owns only the
  Valkey Stream transport of that type (the single-`payload` encoding, the
  `EvSIM-` synthetic ids, the XADD and the ack-based completion signal).
- **`curie init` scaffolds the plugin-format shape verbatim.** The
  generated bundle (`.claude-plugin/plugin.json`, `skills/<name>/SKILL.md`,
  `.mcp.json`) must stay byte-compatible with what `plugin_format.validate_bundle`
  accepts -- if `packages/plugin-format` changes, this scaffold needs
  updating in the same reviewed change, not independently. `init` also drops a
  root `AGENTS.md` and a `.claude/skills/using-curie/SKILL.md` harness primer
  (body rendered from `guide::primer_markdown()`) alongside the bundle; both
  live outside the `plugin_format`-validated `skills/` tree, so they do not
  affect validation. The non-interactive spec-file path
  (`curie init --from-spec <path>`, `scaffold::scaffold_from_spec`) produces the
  SAME plugin-format-verbatim shape and carries the same byte-compat obligation:
  its `SKILL.md` frontmatter uses `allowed-tools` (never `tools`) and its
  `.mcp.json` servers each define `command` or `url` (as strings). The spec's
  `evals` reuse the frozen `evals::EvalCase` type directly (not a hand-mirror), so a
  spec-authored eval suite cannot drift from the shape `skill eval` loads. Because
  the spec's `evals` ARE the frozen eval-case shape reused verbatim, unknown keys
  inside an eval case are ignored exactly as the platform's worker `EvalSuite`
  ignores them (pydantic default `extra="ignore"`, `ConfigDict(frozen=True)`) --
  this is intentional PARITY with the platform grader, not an oversight, so do NOT
  add `deny_unknown_fields` to the eval structs (it would make `skill eval` stricter
  than the platform and break parity). The spec's OWN top-level fields stay strict
  (`deny_unknown_fields` on `AgentSpec`/`SkillSpec`).
- **The `evals/cases.json` seed and `skill eval` loader hand-mirror the frozen
  eval-case schema.** The `curie init` seed (`scaffold::eval_cases`) and the
  `skill eval` loader (`evals::EvalSuite`/`load_suite`) mirror the frozen
  eval-case schema at `apps/worker/schema/eval-cases.schema.json` and must stay
  byte-compatible with the worker's `EvalSuite` -- the scaffold byte-equality
  test against `apps/worker/schema/eval-cases.example.json` is the enforcement.
  A shape change here lands in the same reviewed change as the Python models,
  not independently.
- **`skill up` records container state in `.curie/runner.json`** (gitignored
  by the scaffold) so `skill message`/`skill eval`/`skill status`/`skill down` can resolve the
  running container from the bundle directory alone. Do not add a second
  state-tracking file for the same purpose.
- **The local skill verbs stay fully offline once their local inputs exist; the
  local and cluster target verbs are the exception.** `init`, `skill up`,
  `skill down`, `skill status`, `skill message`, and `skill eval` must keep
  working with zero network access beyond the local Docker daemon and the local
  runner container. `skill up` stays offline once the runner image is present
  locally, or when `--image <local-tag>` names a local image. A release binary's
  default runner image ref is pulled from GHCR on first run. `local deploy` and
  `cluster deploy` are the bundle shipping verbs that leave the machine: they
  package the bundle as
  tar.gz and push to the platform API (find-or-create agent, create version,
  upload bundle, create deployment) authenticated via
  `--api-key`/`CURIE_API_KEY`. The packer skips a fixed set of names
  (`.curieignore`, `.curie`, `.git`, `.venv`, `venv`, `node_modules`,
  `__pycache__`, `.mypy_cache`, `.pytest_cache`) at any depth plus whatever an
  optional root `.curieignore` names (name-only, no globs), and still refuses
  to pack any symlink that survives those exclusions rather than dereference it.
  `local message`, `local deploy`,
  `cluster message`, `cluster deploy`, and every operator verb
  (`local up`, `local status`, `local down`, `local comms`, `cluster up`, `cluster status`, `cluster down`, `cluster comms`) reach a Valkey, API, or cluster by design.
- **The operator verbs are a thin wrapper; the chart stays the source of
  truth.** `cluster up`/`cluster status`/`cluster down` (`src/ops.rs`),
  `cluster comms` (`src/comms.rs`), and `local <up|down|status>`
  (`src/local.rs`) shell out to
  `helm`/`kubectl`/`docker compose` and never re-derive what a values file
  already declares. Each verb builds its command lines as a pure function
  returning `OpsCommand` vectors that the executor or the `--dry-run` printer
  consumes; keep that split so argv stays unit-testable with no cluster, and
  give any new verb a matching `--dry-run`.
  Artifact resolution is a pure plan via `artifacts::resolve_*` plus a separate
  fetch via `ensure_cached`; pure argv builders never fetch, and `--dry-run`
  never touches the network.
- **Credentials are masked, never printed.** Secret `helm --set` values use the
  `CmdArg::SecretSet` variant (only a masked prefix is echoed, in dry-run or the
  printed command line) and token flags read from env with `hide_env_values`.
  Never widen a secret to `Plain` or otherwise print it. The `up` model
  credential (from `CURIE_MODEL_CREDENTIALS`) flows through this path.
  `curie cluster comms --slack` uses the same `SecretSet` masking for
  `SLACK_APP_TOKEN` and `SLACK_BOT_TOKEN`, while `--disconnect --dry-run`
  prints only the empty clears. After the helm upgrade it also rolls the
  worker (and, on connect, the dispatcher) via `kubectl rollout restart` +
  `rollout status` so the Secret-backed tokens go live.
- **`local comms` shares the same Slack flag surface, but tokens travel through
  compose env, not argv.** `curie local comms --slack` reads
  `SLACK_APP_TOKEN` and `SLACK_BOT_TOKEN`, passes them through a masked
  `secret_env` compose process env channel, and never prints an unmasked token
  in live or dry run output. `--disconnect` clears the real Slack wiring,
  stops the dispatcher, and restores the local stub for `local message`.
- **`cluster message` self-plumbs and guards against hijacking real Slack.** It manages
  its own kubectl port-forwards (children killed on exit) and, when wiring the
  deployed worker to its local stub, refuses if a `<release>-dispatcher`
  Deployment exists (a real workspace is connected) unless `--force-wire`. Do
  not drop that guard: the stub wiring is cluster-wide and would divert a live
  workspace's replies.
- **A new `Deserialize` struct in `cli/src/api.rs` must be declared in
  `cli/api-mirrors.json`** (as a `mirrors` entry with its allowlisted field
  omissions, or a `non_mirrors` entry with a one-line reason). `curie dev
  field-parity` is the check (#691).
- **A new `Deserialize` struct in `cli/src/commands.rs` or `cli/src/spec.rs`
  that hand-mirrors the frozen `packages/plugin-format` manifest shape must be
  declared in `cli/plugin-format-mirrors.json`** the same way (`mirrors` with
  omissions, or `non_mirrors` with a reason) -- this is a SIBLING gate to the
  one above, not the same one: the source of truth here is
  `packages/plugin-format/schema/plugin-format.schema.json`, not
  `apps/api/openapi.json`, since the two contracts are unrelated. `curie dev
  field-parity` (`cli/scripts/check-field-parity.sh`) runs both (#701). Beyond
  the mechanical field sweep, `commands::parse_manifest_gates` additionally
  validates a manifest that DECLARES an `approvalPolicy` against the full
  frozen schema (`commands::validate_against_plugin_format_schema`) before
  trusting the narrow `ManifestApprovals` read of it -- without that, a
  manifest invalid in some unrelated modeled field (e.g. `commands: 123`)
  would parse into the narrow shape untouched and `skill approvals` would
  report gates as armed for a manifest the runner's own `PluginManifest`
  parse rejects outright (closes ADR-0041's formerly-open known limitation).
- **A `CliOutput::to_json` that hand-projects one of those mirror structs into
  a `serde_json::json!` literal must be declared in `cli/api-mirrors.json`'s
  `emits` array** (the `(output, struct)` pair, plus any allowlisted, justified
  field omissions) -- the second, emit-hop seam #691 named but did not close
  (the proof case was `VersionsOutput::to_json` dropping `Version.id`,
  #699). `curie dev emit-parity` is the check. Narrower than the struct-level
  gate: it verifies every DECLARED projection stays honest but, unlike that
  gate's `UndeclaredStruct` check, cannot discover a new projection on its own
  (see `cli/tests/support/emit_parity.rs`'s module doc for why). A `to_json`
  that instead delegates wholesale to a `Serialize` value
  (`serde_json::to_value`) or a shared schema-gated builder needs no `emits`
  entry -- it cannot drop a field by hand-picking one in the first place.

## Verify

```bash
cd cli && cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

Scripted E2E (real runner container, fake model, fully offline by default):
```bash
bash cli/scripts/e2e.sh
```
Requires the `curie-runner` image built once from the repo root
(`docker build -f runner/Dockerfile -t curie-runner .`) and a cargo
toolchain (or `$CURIE_BIN` pointed at a prebuilt binary, which skips the
`cargo build --release`). `CURIE_E2E_LIVE=1` drops `--fake-model` and runs
the skill rung against a real model, requiring a credential
(`ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, or `CURIE_CREDENTIALS`) in
the environment.

Cold-start parity ladder (issue #690, `curie dev e2e-ladder` ->
`cli/scripts/e2e-ladder.sh`) -- an E2E test, same as the scripted E2E above, not
the falsifiability gate below it. It chains three rungs: rung 1 delegates to
`e2e.sh` unchanged; rung 2 drives `local up --minimal` -> `local deploy` ->
`local message` (reply asserted) -> `local down`, against `compose.dev.yaml`;
rung 3 drives `cluster deploy` -> `cluster message` against a pre-installed
release, a real round trip with no manual port-forward. A fourth,
separately-named `local-release` rung (issue #695) repeats rung 2's exact round
trip against `compose.release.yaml` instead -- the file
`compose/generate_release_compose.py` derives from `compose.dev.yaml` and the
artifact a release binary's `curie local up` actually runs, one half of the
`compose.dev.yaml` / generated-release-compose parity seam (AGENTS.md). It
generates that file fresh each run, preflights that the release-pinned
`ghcr.io/curie-eng/curie-api` and `-worker-local` images already exist
locally (failing with a fix hint otherwise, since the generated file has no
build directive to fall back on and a release binary has no checkout to build
one from), then runs the same `local up --minimal -f compose.release.yaml` ->
`local deploy` -> `local message` (reply asserted) -> `local down -f
compose.release.yaml` against it. Elsewhere in CI, the `compose` job already
asserts this generated file parses and renders the right service counts but
never runs a turn through it -- this rung is the missing half, catching the
compose-env-wiring bug class (#545) a config-only check cannot.
`CURIE_E2E_TIERS` (default `skill,local`, or `all` for `skill,local,cluster`)
selects rungs; `local-release` is NOT folded into `all` since it needs those
extra images built first, so name it explicitly (e.g.
`skill,local,local-release`). `CURIE_E2E_LIVE` (default fake, `1` for live)
governs every named rung, including rung 1 -- `e2e.sh` reads the same env var
directly rather than being told by the ladder. One-command pre-release gate:
```bash
CURIE_E2E_TIERS=all curie dev e2e-ladder
```

Falsifiability gate (issue #619) -- a gate, NOT an E2E test: it never runs a real
agent or makes a model call. Its real-path half boots the FAKE model and asserts
every committed eval suite goes RED (a case that greens against a do-nothing
agent is unfalsifiable, #527):
```bash
curie dev eval-falsifiability   # bash cli/scripts/eval-falsifiability.sh
```
The grader-level half (the `contains: "weather"` input-parrot vacuousness control
and the known-good-exemplar positive control) rides `cargo test`
(`cli/tests/eval_falsifiability.rs`); together they are the gate. Both run
offline with no credential.

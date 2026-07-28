# cli

The `curie` CLI (Rust: clap + tokio + reqwest). It speaks
only the frozen contracts (the generated `curie-aci-protocol` crate over
HTTP/NDJSON, and the platform API's committed openapi.json) and orchestrates a
local runner container via Docker, so a plugin runs on a dev laptop with zero
Slack involved.

## Which target do I want?

Every environment command takes a **target noun** in the middle: `skill`,
`local`, or `cluster`. Pick the lightest one that answers your question.
`curie init` is the exception, a top-level verb that scaffolds a bundle on
disk and targets no environment.

| Target | What runs | Slack | Kubernetes | Verbs | Reach for it to |
|---|---|---|---|---|---|
| `skill` | Just the runner container on the host Docker daemon. No platform, no queue, no API, no Slack. Fully offline. | none | none | `up` `check` `info` `down` `status` `message` `eval` | Iterate a plugin/skill against a local runner, the fastest loop. |
| `local` | The full platform via docker compose (Postgres + Valkey + Langfuse + API + worker). | stub by default, optional real Slack with `--slack` | none | `up` `down` `status` `info` `comms` `message` `eval` `deploy` `reset-thread` | Exercise the real queue -> worker -> sandbox -> reply product loop with zero Slack and zero Kubernetes. Its API is published on host port `28000`. |
| `cluster` | The platform on Kubernetes (a Helm release). | optional | yes | `up` `down` `status` `info` `comms` `message` `eval` `deploy` `kill` `resume` `budget` `reset-thread` `delete` | Operate and drive a deployed cluster release, and control its agents' lifecycle. |

The universal quartet `up`/`down`/`status`/`message` is on all three targets;
`skill` adds `eval`, while `local` and `cluster` add `comms`, `eval`, plus `deploy`; `cluster`
further adds the agent-lifecycle verbs `kill`/`resume`/`budget`/`delete`, and both `local`
and `cluster` add `reset-thread` to force a stuck thread's sandbox to be released
(#737). `eval` is on
all three: it runs the SAME `evals/cases.json` with the SAME grader at each tier (the
per-tier parity gate), so a suite that passes at `skill` can be re-asserted verbatim at
`local` and `cluster`. `info` is on all three for the same reason: one discovery pass
over the bundle's files at every tier, so a diagnostic means the same thing whether the
bundle was read from disk or from the in-force deployment (ADR-0083). The distinction
that matters: `skill` is the **runner-only** loop, talking straight to a runner
container's ACI HTTP surface with no platform in front; `local` and `cluster`
put the **full platform** (queue, worker, sandbox) in front of the identical
runner and ACI, so a `message` walks the same path a real Slack mention would.

## `init` (top-level)

| Command | What it does |
|---|---|
| `curie init <name>` | Scaffold a plugin bundle (Claude Code plugin shape: `.claude-plugin/plugin.json`, `skills/<name>/SKILL.md`, `.mcp.json`) plus an `evals/cases.json` seed, a root `AGENTS.md`, and an installable `.claude/skills/using-curie/SKILL.md` harness primer. |
| `curie init --from-spec <path>` | Scaffold **non-interactively** from an agent-authored spec file (JSON). The bundle name comes from the spec, not a positional argument. A coding agent interviews the human, writes the spec, then this command lays down the same plugin-format shape deterministically -- zero prompts. See the spec shape below. |
| `curie init --adopt <dir>` | Adopt an existing non-plugin directory: scaffold the same plugin skeleton **into** it, alongside your code and never overwriting an existing file, with the bundle name derived from the directory unless a `<name>` is given. The on-ramp for a pre-plugin (`agent-ss-template`) bundle; the logic port is manual afterward -- see `docs/adopting-a-bundle.md`. |
| `curie` | Open the keyboard-driven terminal interface. Explicit forms: `curie interactive`, `curie ui`, `curie tui`. |
| `curie secrets set <NAME>` | Save a local secret in Curie's mode-0600 credential file with hidden input. `--from-env <VAR>` reads from an existing environment variable for non-interactive use without putting the value in argv. |
| `curie secrets list` | List saved Curie secret names. Values are never printed. |
| `curie secrets unset <NAME>` | Remove a saved local secret. |
| `curie guide` | Print a self-contained primer (ADR-0021) for a coding agent driving the harness: the parity ladder, when/which decision logic, the landmines, and verify-first, to stdout. `--json` emits the same content as a structured variant (data on stdout). |
| `curie build` | Build the runner image locally: `docker build -f runner/Dockerfile -t curie-runner .` from the repo root (found by walking up to `runner/Dockerfile`). `--tag` overrides the tag. Prints a clear error if Docker is not installed or if run outside a source checkout -- a release binary pulls the pinned runner image from GHCR automatically and never needs to build. |
| `curie list-agents` | List the plugin bundles under `agents/`, a personal, gitignored directory (sibling of `examples/`, source checkout only) for in-progress agent projects. Empty, not an error, when the directory doesn't exist. |
| `curie deploy-local <folder>` | Deploy `agents/<folder>` to the local platform by name -- shorthand for `curie local deploy --plugin-dir agents/<folder>` (identical operation, same flags minus `--plugin-dir`). Local tier only; use `curie cluster deploy --plugin-dir agents/<folder>` for the cluster tier. The interactive "How to deploy to Slack" workflow offers the same `agents/` bundles as a picker. |

### `init --from-spec` spec shape

The spec is a JSON object an agent writes after interviewing the human. `name`
is the kebab-case bundle name; every `skills[].name` is kebab-case and unique;
`connectors` (optional) is the raw `.mcp.json` `mcpServers` map (each server must
define `command` or `url` as a string); `secrets` (optional) is a list of
connector-secret NAMES (env-var-shaped, no values, per ADR-0009) written to the
manifest's `secrets`; `approvalPolicy` (optional) declares approval `gates`
(`{gate, route}`) where an `mcp__` gate must be a fully-namespaced live tool name
`mcp__plugin_<bundle>_<server>__<tool>` for a declared connector (a built-in like
`Bash` needs no prefix) — so a spec can express a gated, authed agent without
hand-editing `plugin.json`; `evals` reuses the frozen eval-case shape
so the scaffolded `evals/cases.json` loads unchanged through `curie skill eval`.
An unknown TOP-LEVEL field is a hard error, so an authoring typo fails loud, but
unknown keys INSIDE an eval case are ignored exactly as the platform's worker
`EvalSuite` ignores them (pydantic `extra="ignore"`), which is intentional parity
with the platform grader, not an oversight.

```json
{
  "name": "deal-desk",
  "description": "Prices and reviews deal desk requests.",
  "skills": [
    {
      "name": "deal-desk",
      "description": "Invoke when a rep submits a pricing exception request.",
      "allowed_tools": ["WebSearch", "WebFetch"],
      "instructions": "Price the exception against the guardrails, then summarize the decision.\n"
    }
  ],
  "connectors": {
    "crm": { "command": "crm-mcp", "args": ["--stdio"] }
  },
  "secrets": ["CRM_API_TOKEN"],
  "approvalPolicy": {
    "gates": [
      { "gate": "mcp__plugin_deal-desk_crm__create_deal", "route": "default" }
    ]
  },
  "evals": [
    {
      "id": "prices-a-deal",
      "input": "Quote 20% off for Acme",
      "grader": { "kind": "contains", "expected": "approved", "case_sensitive": false }
    }
  ]
}
```

```bash
curie init --from-spec agent-spec.json   # bundle name (deal-desk) comes from the spec
```

## `curie` / `curie interactive`

The interactive terminal interface is a human-friendly command surface over the
same `curie ...` subcommands documented here. It opens a full-screen TUI with
target navigation, action selection, command previews, and guarded execution:
when an action needs values (for example message text or a channel id), the TUI
temporarily leaves the alternate screen, prompts for the values, runs the exact
previewed command, then returns to the interface. Some actions also require a
tier (local or cluster); the TUI asks which tier before prompting for the
other values, and the command preview shows `<local|cluster>` in the tier's
position until that question is answered.

```bash
curie
curie interactive
curie ui
curie tui
```

Keyboard:

| Key | Action |
|---|---|
| `Up`/`Down` or `k`/`j` | Move through actions |
| `Tab` / `Left` / `Right` | Switch target filters |
| `Enter` or `r` | Prompt for fields and run the selected command |
| `q` or `Esc` | Exit |

The first surface focuses on the common inner-loop and operations paths:
`skill up/message/eval`, an **Explore examples** picker with live agent chat,
`secrets set/list/unset`, `local up/message/status`, `cluster status/message`,
`install`, and `dev contracts`.

**Explore examples** opens a dialog for GitHub issues, Text stats engine, or
Weather. After selection, Curie checks that example's credentials, starts its
bundle once, and opens a persistent conversation. Type a message, read the
reply, and continue for as many turns as needed. Leaving chat stops the runner
and returns to Curie.

## `curie secrets`

Local secrets are stored in `~/.config/curie/credentials.json` with mode 0600,
not in the repo, shell history, command argv, `.env`, or Curie state files.
This follows the prompt-free private-config pattern used by developer CLIs.
Curie keeps a separate non-secret index so secret names can be listed without
opening values. Existing Keychain credentials are copied into the private file
on first use; Curie never writes to or deletes from Keychain during migration.

```bash
curie secrets set GITHUB_PERSONAL_ACCESS_TOKEN
curie secrets set ANTHROPIC_API_KEY
curie secrets list
curie secrets unset GITHUB_PERSONAL_ACCESS_TOKEN
```

For CI or other non-interactive setup, read from an existing environment
variable instead of prompting:

```bash
curie secrets set GITHUB_PERSONAL_ACCESS_TOKEN --from-env GITHUB_PAT
```

`curie skill up --secret <NAME>` first uses a real environment variable when
one is already set. If it is missing, the CLI tries the Curie secret store and
hydrates the process environment just long enough for Docker to forward `-e
<NAME>` into the runner. The same lookup applies to saved model credentials
(`CURIE_CREDENTIALS`, `ANTHROPIC_API_KEY`, or `CLAUDE_CODE_OAUTH_TOKEN`) for
live `skill up` runs.

## `curie install`

Contributor bootstrap/update for a source checkout: install dependencies and
build, but **start nothing**. Run it after cloning; rerun `./get-curie.sh` later
to refresh an existing checkout without reinstalling already-present artifacts.
Then `curie local up` brings the stack up. From the repo root (found by
walking up to `runner/Dockerfile`) it runs, in order and each idempotent,
streaming output:

1. Copy `.env.example` to `.env` if `.env` is missing (otherwise left untouched).
2. `uv sync` at the repo root (needs `uv`).
3. `pnpm install` in `apps/ui` (needs `pnpm`).
4. `cargo build` in `cli` (needs `cargo`).
5. Build the runner image via `curie build` (needs `docker`).

`curie install --update` is the rerun path used by `./get-curie.sh` (in its
source-checkout mode) when an installed CLI already exists. It still refreshes
dependencies and local builds, but skips rebuilding the `curie-runner` image
if that image is already present. `./get-curie.sh` always rebuilds and
reinstalls the CLI itself (`cargo install --path cli --force`) rather than
guessing from file mtimes -- a branch switch changes the sources without
reliably bumping mtimes, so a stale binary could otherwise survive; cargo's own
caching keeps this to a few seconds when nothing changed.

Each required tool is checked first; a missing one prints a pointer (e.g. `uv is
not installed - https://docs.astral.sh/uv/`) and stops. Run outside a source
checkout it errors clearly -- a release binary has nothing to install.

## `curie dev`

Thin wrappers over the repo's dev scripts, so contributors get one unified
`curie <command>` surface while the scripts stay the implementation. Each finds
the repo root, confirms the script exists, shells `bash <script>` from the root,
streams its output, and propagates its exit code. Run outside a source checkout
they error clearly -- a release binary has no dev scripts.

| Command | What it does |
|---|---|
| `curie dev contracts` | `bash scripts/check-contracts.sh` -- check the frozen contracts. |
| `curie dev chart-check` | `bash charts/curie/ci/render-assertions.sh` -- render-assert the Helm chart. |
| `curie dev e2e` | `bash cli/scripts/e2e.sh` -- the scripted CLI end-to-end test. |
| `curie dev e2e-ladder` | `bash cli/scripts/e2e-ladder.sh` -- the cold-start parity ladder (skill, local, cluster rungs). |
| `curie dev field-parity` | `bash cli/scripts/check-field-parity.sh` -- assert CLI `api.rs` mirror structs cover their platform API model fields (#691), and CLI `commands.rs`/`spec.rs` mirror structs cover the frozen `packages/plugin-format` schema's fields (#701). |
| `curie dev emit-parity` | `bash cli/scripts/check-emit-parity.sh` -- assert a `CliOutput::to_json` that hand-projects a mirror struct into a `json!` literal covers that struct's fields, one hop downstream of `field-parity` (#699). |
| `curie dev wire-tolerance` | `bash scripts/check-wire-tolerance.sh` -- assert every direct `ClassName.model_validate*(...)` call on an `_AciModel` subclass threads `READER_CONTEXT` or is a declared exception (#625). |

## `skill` target: runner-only, fully offline

Boots just the runner container on the host Docker daemon and speaks its ACI
HTTP surface directly. No platform, no queue, no API, no Slack, no cluster.

| Command | What it does |
|---|---|
| `curie skill up` | Boot the local runner image in Docker with the ACI boot env (runner/README.md recipe), wait for health, print the boxed env summary. `--fake-model` runs offline; `--network` and `--otel-endpoint` join the compose stack for traces; `--model <id>` forwards `CURIE_MODEL` (omit for the SDK default). `--secret <NAME>` forwards bundle MCP secrets by name, using Curie private storage when the env var is not exported. `--env-file <PATH>` reads the model credential from a bundle `.env` as a last-resort fallback (precedence: shell env > stored secret > file; only `CURIE_CREDENTIALS`/`CLAUDE_CODE_OAUTH_TOKEN`/`ANTHROPIC_API_KEY` are read), so a bundle boots live with no `source` step (#749). A leftover container of the same name fails the boot with the remedies rather than a raw docker conflict; `--replace` removes it and boots fresh. |
| `curie skill check` | Run an offline, credential free MCP load check and report declared servers, matches, and verdict. |
| `curie skill info` | Print what the harness resolved from the bundle (skills, MCP servers, declared secrets, boot env, approval gates, eval suite, model) plus a `diagnostics` array naming every candidate it looked at and did NOT register, what it looked for, where it looked, and why it did not count. Static by default: no Docker, no network, no container. `--plugin-dir <DIR>` (default `.`) picks the bundle. A bundle defect (an unparseable manifest, a `skills/<dir>` with no `SKILL.md`, a missing `evals/cases.json`) is a diagnosis at exit 0; only a `--plugin-dir` that does not exist or holds no plugin manifest at all is a usage error (exit 2). `--check-mcp` additionally runs the same offline load probe `skill check` runs (needs Docker; `--image` and `--timeout`, default 30, tune it), otherwise every declared server reports `load: "not_probed"` rather than implying it registered. Names only, never values: no secret, credential, MCP `env`, or `headers` value is ever printed. Contract in `cli/schema/info.schema.json` (ADR-0083). |
| `curie skill approvals` | View the bundle's declared `approvalPolicy` gates, read straight from `.claude-plugin/plugin.json` (or `plugin.json`); no docker, no network. `--gate <TOOL>` (repeatable) or `--clear` mutate nothing -- they print the `CURIE_APPROVAL_REQUIRED_TOOLS=...` assignment to export, then re-run your original `skill up` invocation with `--secret CURIE_APPROVAL_REQUIRED_TOOLS` added, since the runner only resolves that env once at container boot. |
| `curie skill versions` | Not available at this tier (exit 4): `skill up` runs the bundle bytes on disk, so no deployed version is assigned. Use `curie local versions <agent>` or `curie cluster versions <agent>`. |
| `curie skill memory` | Not available at this tier (exit 4): this tier configures no memory namespace. Use `curie local memory <agent>` or `curie cluster memory <agent>`. |
| `curie skill message "..."` | Send a synthetic Slack event: POST an ACI `event` frame to the local runner and stream the NDJSON reply (text deltas, tool notes, side effect flags, final). Abort a live turn with Ctrl-C. |
| `curie skill eval` | Run `evals/cases.json` through the runner as `eval_case` events; prints a per case result table plus a pass or fail rollup; nonzero exit on failure. |
| `curie skill status` | Show the local runner's session status. |
| `curie skill down` | Stop and remove the local runner container. With no `.curie/runner.json` it falls back to container identity, so an orphaned runner is still clearable; `--name <NAME>` targets a container other than `curie-runner-local`. |

`skill up` records the container in the bundle's `.curie/runner.json`
(gitignored by the scaffold); `skill message` / `skill eval` / `skill status` /
`skill down` run from the bundle directory and resolve the runner from it, or
accept `--url`. Setting `skill up --model <id>` makes token usage attributable
in Langfuse traces.

## `local` target: full platform via compose, no Slack

Wraps the `compose.dev.yaml` stack so a `message` walks the real
queue -> worker -> sandboxed runner -> reply path on one machine, no Slack and
no Kubernetes. `curie local up` uses the `full` compose profile by default.
`curie local up --minimal` uses the smaller `core` profile. The compose API is
published on host port `28000`. Add `curie local up --slack` to also start
the optional Slack dispatcher.

| Command | What it does |
|---|---|
| `curie local up` | Bring the compose stack up (`docker compose --profile full up -d --wait` by default, `docker compose --profile core up -d --wait` with `--minimal`) and print URLs. Add `--slack` to append `--profile slack`. `--env-file <PATH>` reads the model credential from a bundle `.env` as a last-resort fallback (precedence: shell env > file; only `CURIE_CREDENTIALS`/`CLAUDE_CODE_OAUTH_TOKEN`/`ANTHROPIC_API_KEY` are read, and the value never reaches argv or logs), so the stack boots live with no `set -a; source .env` step (#749). |
| `curie local down` | Stop the compose stack (`docker compose down`), keeping volumes. |
| `curie local status` | Show the compose stack's service status (`docker compose ps`). |
| `curie local info <agent>` | The same report as `curie skill info`, resolved from the deployed bundle instead of a directory: it reads the agent's in-force deployment's stored version files and runs the identical discovery pass, so a `diagnostics` entry means the same thing at both tiers. `--api-url` (default `http://localhost:28000`), `--api-key` / `CURIE_API_KEY`, `--dry-run` (prints the read-only plan, makes no request). Deployed-side gaps are diagnoses at exit 0, not failures: an agent with no in-force deployment answers `deployed.no_active_deployment`. Disk-only facts (`bundle.root`, `model`, per-secret satisfaction) are reported as explicit `unavailable` sentinels rather than omitted. `--check-mcp` is accepted and declined here: not available at this tier (exit 1, with a `fix`), because the MCP load probe boots a runner container against a bundle DIRECTORY (`curie_runner.check`) and this CLI does not yet write a deployed bundle's stored files out to one. Deliberately not exit 4: the bytes are reachable over the API, so this is an unbuilt step rather than a limit of the tier, and probing a deployed bundle directly is a tracked follow-up. Run `curie skill info --plugin-dir <dir> --check-mcp` (or `curie skill check`) against the bundle source, then deploy the bundle that probed clean. Names only, never values. |
| `curie local observability` | Print the local platform's observability surfaces: Curie Console, Langfuse UI (traces / cost / evals), and the Curie API base. URLs are printed only; pass `--open` to also open the browsable ones (Console, Langfuse) in a browser. `--json` never opens a browser. |
| `curie local comms --slack` | Connect or disconnect a real Slack workspace for the compose stack. Resolves `SLACK_APP_TOKEN` and `SLACK_BOT_TOKEN` with precedence `--app-token`/`--bot-token` flag > env var > a value persisted with `curie secrets set` (so tokens saved once need no per-session re-export, #749), masks them in dry run output, starts or stops the dispatcher, and switches the worker between real Slack and the local stub. |
| `curie local message "..."` | Drive the local compose stack end to end with zero Slack. Enqueues straight to the compose Valkey and lets the containerized worker answer. |
| `curie local eval` | Run the bundle's `evals/cases.json` through the compose stack's enqueue -> worker -> sandbox -> reply path (one synthetic turn per case) and grade each captured reply with the SAME grader `skill eval` uses. Prints the identical per-case table + rollup; nonzero exit on failure. `--cases` overrides the file; `--dry-run` prints the plan. `--concurrency` defaults to 1 (sequential); values above 1 are refused for now (#709). |
| `curie local deploy` | Package the bundle as tar.gz and push it to the compose platform API (`--api-url`, default `http://localhost:28000`). Auth via `--api-key` or `CURIE_API_KEY`. |
| `curie local reset-thread <agent> --thread-key <key> --yes` | Force a stuck thread's sandbox to be released via the compose platform API (`POST /agents/{id}/threads/{thread_key}/reset`, #737). The worker's next maintenance tick releases the thread's claim and route, so its next message cold-creates a fresh sandbox; conversation history is not deleted. Interrupts a live turn on the thread first, so it refuses without `--yes`. |

## `cluster` target: deployed Helm release

Wraps the umbrella Helm chart and the deployed release, the way `linkerd` or
`cilium` wrap theirs. Every operator verb takes `--dry-run`. Full runbook in
[`docs/operations.md`](../docs/operations.md).

| Command | What it does |
|---|---|
| `curie cluster up` | Install or upgrade the release (`helm upgrade --install`). Exposes the UI and Langfuse on node ports; `--no-expose` keeps them ClusterIP-only. Set `CURIE_CREDENTIALS` (deprecated alias `CURIE_MODEL_CREDENTIALS`) for a real model, or install sealed with canned replies. A shell `CURIE_MODEL` now defaults the sandbox runner model (`agentSandbox.runner.model`) for cross-tier parity with `local up`, unless an explicit `--set agentSandbox.runner.model=` is passed; a shell `CURIE_MODEL` that disagrees with such an explicit `--set` fails loud. |
| `curie cluster down` | Uninstall the release and sweep its runtime namespaces (`helm uninstall` + `kubectl delete namespace`); prompts unless `--yes`. |
| `curie cluster status` | Report release health, pod readiness, and access URLs (read-only). |
| `curie cluster info <agent>` | The deployed-tier report, identical in shape and discovery pass to `curie local info <agent>`, against the release's platform API. Adds `--namespace` and `--release` so the API URL and key are discovered from the installed release when `--api-url` / `--api-key` are omitted; `--dry-run` prints the read-only plan. Same exit contract (bundle and deployment gaps are diagnoses at exit 0) and the same `--check-mcp` decline at exit 1 with a `fix`, for the same reason: the probe needs a bundle directory and this CLI does not yet reconstruct one from a deployed bundle's stored files. |
| `curie cluster observability` | Report the release's observability surfaces (Curie Console, Langfuse UI, Curie API base), using the same NodePort discovery as `cluster status`. Degrades a missing, ClusterIP, or unresolvable surface to a note instead of failing. URLs are printed only; pass `--open` to also open the browsable ones (Console, Langfuse) in a browser. `--json` never opens a browser. `--dry-run` prints the read-only discovery commands. |
| `curie cluster comms --slack` | Connect or disconnect a real Slack workspace with a thin `helm upgrade --reuse-values`; env-backed tokens are masked in dry-run output. |
| `curie cluster message "..."` | Drive the deployed release end to end with zero Slack: self plumbs kubectl port forwards, points the deployed worker at a local Slack stub (`helm upgrade --reuse-values`), enqueues, and prints the reply. Auto-discovers the release-generated API key and Valkey password from `<release>-secrets` when `--api-key` / `--valkey-password` (or their env vars) are omitted, so a default strong-secrets install needs no hand-exported credentials (#786). |
| `curie cluster eval` | Run the bundle's `evals/cases.json` through the deployed release (self-plumbed port-forwards + per-turn reply stub, one synthetic turn per case) and grade each captured reply with the SAME grader `skill eval` uses. Prints the identical per-case table + rollup; nonzero exit on failure. `--cases` overrides the file; `--dry-run` prints the plan. `--concurrency` defaults to 1 (sequential); values above 1 are refused for now (#709). Auto-discovers the release-generated API key and Valkey password from `<release>-secrets` when `--api-key` / `--valkey-password` (or their env vars) are omitted, so a default strong-secrets install needs no hand-exported credentials (#790). |
| `curie cluster deploy` | Package the bundle as tar.gz and push it to the platform API. When `--api-url` is omitted, self-plumbs a `kubectl port-forward` (loopback tunnel) to the release API service and auto-discovers the release-generated key from `<release>-secrets`, so the strong key never crosses the cleartext UI proxy (ADR-0057). Pass `--api-url` / `CURIE_API_URL` to direct-dial a URL instead (no tunnel); an explicit `--api-key` / `CURIE_API_KEY` still wins over discovery. |
| `curie cluster kill <agent> --yes` | Kill an agent (stop its runs) via the platform API (`POST /agents/{id}/kill`). Destructive: refuses without `--yes`. |
| `curie cluster resume <agent>` | Resume a killed agent via the platform API (`POST /agents/{id}/resume`). |
| `curie cluster budget <agent> --limit <n>` | Set the agent's daily spend cap in USD via the platform API (`PUT /agents/{id}/budget`, `BudgetConfig.max_usd_per_day`); the per-run token cap is left at the platform default. |
| `curie cluster reset-thread <agent> --thread-key <key> --yes` | Force a stuck thread's sandbox to be released via the platform API (`POST /agents/{id}/threads/{thread_key}/reset`, #737). The worker's next maintenance tick releases the thread's claim and route, so its next message cold-creates a fresh sandbox; conversation history is not deleted. Interrupts a live turn on the thread first, so it refuses without `--yes`. |
| `curie cluster delete <agent> --yes` | Delete an agent via the platform API (`DELETE /agents/{id}`). Destructive and irreversible: refuses without `--yes`. |

The five lifecycle verbs (`kill`, `resume`, `budget`, `reset-thread`, `delete`)
act on a deployed release's agents through the same platform API, defaulting
`--api-url` to `http://localhost:8000` (auth via `--api-key` or
`CURIE_API_KEY`). Unlike `cluster deploy`, which self-plumbs a port-forward
and auto-discovers the release key (ADR-0057), the lifecycle verbs do neither
-- pass `--api-url` or port-forward the API yourself. They resolve `<agent>`
(a name or id) to its API id with the same lookup `deploy` uses. Each takes
`--dry-run` (prints the plan, makes no request); the destructive
`kill`/`reset-thread`/`delete` also require `--yes`.

### Bundle packing exclusions

`local deploy` and `cluster deploy` never pack `.curieignore`, `.curie`,
`.git`, `.venv`, `venv`, `node_modules`, `__pycache__`, `.mypy_cache`, or
`.pytest_cache`, matched by name at any depth. An optional `.curieignore`
at the bundle root adds more patterns, one per line (`#` comments and blank
lines skipped, surrounding whitespace and a trailing `/` stripped):

```
# .curieignore
scratch
notebooks/drafts
```

A bare name matches that entry at any depth; a path containing `/` matches
only that bundle-root-relative path and its subtree. There is no glob
support (`*.log` never matches), and a pattern reaching outside the bundle
(absolute, or containing `.` or `..`) is dropped. Symlinks are still a
packing error unless excluded, by design: the packer never dereferences a
link to upload host files from outside the bundle root.

### Artifact resolution

Release builds resolve default artifacts from the binary version: `curie local
up` fetches the self contained `compose.release.yaml` release asset, so it
works with no checkout, `curie cluster up` uses the pinned chart release
asset, and runner sessions (`curie skill up`) use the pinned GHCR runner
image. Fetched artifacts cache under
`${XDG_CACHE_HOME:-$HOME/.cache}/curie/<version>/`, so repeated
`curie cluster up` and `curie local up` reuse the cache.

Dev builds use the local `compose.dev.yaml`, `charts/curie`, and
`curie-runner` when present. A dev binary run with no local artifact errors,
telling you to pass `-f <compose>`, `--chart <path>`, or `--image <ref>` (or use
a released binary); those same flags override the defaults. `--dry-run` prints
the resolved argv without fetching.

`curie cluster message` is not yet wired through this resolver: it still defaults
`--chart` to the repo-relative `charts/curie`, so a no-checkout binary must
pass `--chart <path-or-tgz>` explicitly for now.

## Output

Three global flags apply to every subcommand: `--debug` shows the verbose
plumbing (helm/kubectl/compose command lines and their output, as dim lines),
`-q`/`--quiet` prints the payload only (suppressing all progress and diagnostics
on stderr), and `--color <auto|always|never>` (default `auto`) controls ANSI
color.

Stream discipline is strict: the **payload** (streamed agent reply tokens,
resolved URLs, the status table, eval results, the deploy result, `skill status`
JSON, the worker reply) goes to **stdout**, and every **diagnostic**
(waiting/helm/kubectl/rollout/port-forward chatter, spinners, progress, notes)
goes to **stderr**. So the payload pipes and redirects cleanly while progress
still shows on the terminal:

```bash
curie cluster message "..." | jq         # clean JSON on stdout, progress on stderr
curie local message "..." > reply.txt    # reply captured, progress on the terminal
curie skill eval > results.txt           # results captured, progress on the terminal
```

On an interactive terminal, progress renders as a spinner-to-checkmark checklist
(each step spins with a live dim elapsed counter, then freezes to a green `✓` or
red `✗` with its elapsed time), a determinate bar for real totals (eval
`N/total`), and streamed tokens that spin only until the first token then stream
raw to stdout. Every wait resolves: a blown timeout ends in `✗ ... timed out
after Ns`, never a hang. Compatibility is handled automatically:

- **Auto-disable off a TTY.** Rendering is gated on `stderr.is_terminal()` plus
  the cross-tool env standards. On a non-TTY, a pipe, `CI`, `TERM=dumb`,
  `NO_COLOR`, or `CLICOLOR=0`, output is plain discrete status lines with no ANSI
  and no `\r` redraws. `CLICOLOR_FORCE` / `--color=always` force color on;
  `--color=never` forces it off. Color is resolved per stream, so a colored
  terminal stderr never leaks ANSI into a redirected stdout.
- **Graceful degradation.** The brand palette (success green, error red, amber
  warn, dim grey plumbing, cyan URLs/ids, bold payload) is truecolor, degrading
  to the 16 named ANSI colors where truecolor is unsupported (Apple Terminal,
  tmux without passthrough).
- **Never color-only.** Every status pairs a glyph with a word (`✓ pass`,
  `✗ fail`, `⚠ warn`), and glyphs fall back to ASCII (`v`/`x`/`!`, `- \ | /`
  spinner) in non-UTF-8 locales.

## `curie cluster message`: drive the deployed cluster with zero Slack

Before connecting a real workspace, `cluster message` is the zero-Slack path.
When you are ready to wire Slack onto a deployed release, use:

```bash
SLACK_APP_TOKEN=xapp-... \
SLACK_BOT_TOKEN=xoxb-... \
curie cluster comms --slack

curie cluster comms --slack --disconnect

SLACK_APP_TOKEN=xapp-... \
SLACK_BOT_TOKEN=xoxb-... \
curie cluster comms --slack --dry-run
```

`cluster message` targets a **deployed** Helm release and wires everything
itself, so a developer building an agent for someone else's Slack workspace can
exercise the whole deployed machinery (Valkey queue -> worker -> claimed
sandbox -> the real skill -> the reply) without any Slack access, tokens, or
workspace.

```bash
curie cluster message "summarize the latest deploy"
curie cluster message --channel CSIM123 "another question"
```

What it does, in order:

1. **Self-managed port-forwards** (children of the CLI, killed on exit): the
   in-cluster Valkey (`svc/<release>-valkey`, local `56381`) for the enqueue, and
   the API (`svc/<release>-api`, local `8123`) only when `--channel` is omitted,
   to look up the default channel.
2. **Channel default**: with no `--channel`, `GET /agents` and use the sole
   deployed agent's `slack_channel`. Zero or multiple agents is an error naming
   them and requiring `--channel` (the worker binds a channel to an agent by
   exact equality, so guessing would route nowhere).
3. **Reachable stub**: binds `0.0.0.0:<--listen-port>` (default `8155`) and
   advertises a routable host so the in-cluster worker can post back to it.
   `--listen-host` wins; otherwise the local IP the kernel would use to reach the
   cluster is auto-detected.
4. **Worker wiring** (`--wire`, the default): points the deployed worker at the
   stub via `helm upgrade --reuse-values --set worker.slackApiBaseUrl=<url>` (take
   `--chart` like the other ops verbs) and waits for the rollout. `--no-wire`
   instead refuses to run unless the worker is already wired, printing the exact
   command to apply.
5. **Safety guard**: if the release is connected to a real Slack workspace (a
   `<release>-dispatcher` deployment exists, which only renders when both Slack
   tokens are set), wiring is refused unless `--force-wire`, since pointing the
   worker at the stub would hijack that workspace's replies cluster-wide. In the
   demo flow `message` runs **before** a real Slack workspace is connected, so the
   guard never fires; and the helm upgrade that connects Slack (setting
   `worker.slackApiBaseUrl=` to empty in the same command) un-wires the stub when
   real Slack is connected.
6. **Enqueue + wait**: `XADD`s the exact `QueuedSlackEvent`, waits for the worker
   to finalize, prints the reply, and emits a `continue this conversation: ...`
   line for multi turn threads. On timeout it prints stream diagnostics and
   exits nonzero.

`--dry-run` prints the kubectl/helm command lines, the stub URL, and the enqueue
description without executing anything.

Use `--continue` to reuse the last successful `cluster message` context from
`.curie/last-turn.json` in the current working directory, so only the new text
is required. Explicit flags override the saved channel, thread, and transport
settings, the verb must match, and the API key is re-read from
`$CURIE_API_KEY` because the value is never stored. Note that `--continue`
does not replay `--stream`, `--listen-port`, `--valkey-local-port`,
`--api-local-port`, or `--user`, so pass any of those again explicitly if the
original turn used a non-default value.

### Targeting a deployed agent and continuing a thread

The worker binds a channel to an agent by exact equality on
`agents.slack_channel`, so a random synthetic channel can never reach a
deployed agent. Use `--channel <id>` to send as a specific channel: pass the
same value you gave `cluster deploy --slack-channel` and the worker routes the
turn to that agent.

```bash
curie cluster deploy --slack-channel CSIM123 ...
curie cluster message --channel CSIM123 "first question"
```

Each turn mints a fresh thread ts by default. On completion `cluster message`
prints a `continue this conversation: ...` line with the channel and thread ts;
copy paste it, or pass `--thread <ts>` yourself, to send the next turn into the
same thread:

```bash
curie cluster message --channel CSIM123 --thread 1720000000.000100 "follow up question"
```

Against a **connected** Slack workspace the thread ts is not synthetic: the CLI
posts a real placeholder message to the channel and the printed thread ts is
that placeholder's real Slack ts, so you can reply to it in Slack. Passing
`--thread <ts>` there posts the placeholder into that existing thread, which
means the ts must name a real message in the channel -- a thread ts carried over
from a stub run will be rejected by Slack, and the command tells you to drop
`--thread` to start a new one.

## `curie local message`: the same roundtrip against the compose stack

`local message` drives the local compose stack (`curie local up`) instead of a
Kubernetes release, so the whole loop is one machine with no cluster:

```bash
curie local up
curie local deploy --plugin-dir <dir> --slack-channel C0123ABCD --api-url http://localhost:28000
curie local message "what changed in the last deploy?"
```

Local mode keeps only the shared engine (stub + `QueuedSlackEvent` enqueue +
ack-based completion) and drops every cluster-specific step: no kubectl, no
`helm upgrade` wiring, no port-forwards, no dispatcher guard. It enqueues
straight to the compose Valkey (`localhost:26379`) and the containerized
`curie-worker` service (already pointed at the stub via a fixed
`SLACK_API_BASE_URL=http://localhost:8155/api/`) answers by claiming a runner
container on the host Docker daemon. Channel comes from `--channel` or, when
omitted, the sole deployed agent looked up on the compose API (`--api-url`,
default `http://localhost:28000`; the API is reached directly, so no `/api`
suffix). `local message` composes with `--channel`, `--thread`, and
`--timeout-secs` and rejects the cluster only flags (`--namespace`,
`--release`, `--force-wire`, ...)
with a clear error. The compose worker runs the fake model by default (a canned
reply, no credentials); export a credential in your shell and `local up` or
`local comms` goes live automatically for a real model. Instead of exporting it
every session, point `curie local up --env-file .env` at the bundle's own
dotfile: the model credential is read from it as a last-resort fallback
(precedence: shell env > file), so the stack boots live with no
`set -a; source .env` step. Only `CURIE_CREDENTIALS`, `CLAUDE_CODE_OAUTH_TOKEN`,
and `ANTHROPIC_API_KEY` are read; every other key in the file is ignored, and the
value never reaches argv or logs. Set `CURIE_FAKE_MODEL=1` to force the fake
model regardless of a credential being present.

Use `curie local comms --slack` when you want the same compose stack to talk
to a real Slack workspace. Connect resolves `SLACK_APP_TOKEN` and
`SLACK_BOT_TOKEN` with precedence `--app-token`/`--bot-token` flag > env var > a
value persisted with `curie secrets set SLACK_APP_TOKEN` /
`curie secrets set SLACK_BOT_TOKEN` -- so tokens saved once in Curie private
storage need no per-session re-export -- masks them in printed commands, starts
the dispatcher, and points the worker at real Slack, resolving the model the same
way as `local up` (live when a credential is present, fake otherwise).
`--disconnect` stops the dispatcher and restores the local stub. `--dry-run`
prints the compose command only.

Use `--continue` to reuse the last successful `local message` context from
`.curie/last-turn.json` in the current working directory, so only the new text
is required. Explicit flags override the saved channel, thread, and transport
settings, the verb must match, and the API key is re-read from
`$CURIE_API_KEY` because the value is never stored. Note that `--continue`
does not replay `--stream`, `--listen-port`, `--valkey-local-port`,
`--api-local-port`, or `--user`, so pass any of those again explicitly if the
original turn used a non-default value.

## Agent-facing output contract

The CLI's primary consumer is a coding agent (ADR-0021), so its output and
control flow are machine-first.

**`--json`** (global) makes every agent-facing verb emit a single
machine-readable JSON object on **stdout** instead of empty output: the
read/query verbs (`info`, `versions`, `memory`, `approvals`, `observability`), the
lifecycle result verbs (`kill`, `resume`, `budget`, `reset-thread`, `delete`),
and every verb's `--dry-run` plan (uniform shape `{"dry_run": true, "plan":
[<lines>]}`) all route through one centralized emitter. The `message` verbs
keep their own, more specific shapes: `curie local message` and `curie cluster
message` emit one structured line per terminal state on stdout -- a completed
turn emits `{"reply": ..., "thread": ..., "finalized": ...}` (the model's
reply, which is null on a no-edit completion, plus the thread the turn ran
under); a turn parked on a human approval gate emits `{"reply": ..., "thread":
..., "finalized": false, "awaiting_approval": true}` (the worker posted an
approval card rather than finalizing, and `reply` is the card's placeholder
text if seen); a **timeout** emits `{"reply": null, "finalized": false,
"timed_out": true}` before exiting 3 (transient); a turn **enqueued** onto the
real Valkey stream in connected transport mode emits `{"status": "enqueued",
"channel": ..., "thread": ...}` -- the CLI does not wait for the reply, so
this is a terminal state of the command, not of the turn; and `--json
--dry-run` emits a planned-action descriptor `{"dry_run": true, "target":
"local"|"cluster", "stream": ..., "channel": ..., "reply_endpoint": ...}`
(`channel` is null when it would be resolved from the sole deployed agent).
The five shapes are the `oneOf` in `cli/schema/message.schema.json`. Two verbs
lag this contract on their real-path success output: `curie skill message`,
and the operator verbs (`up`, `down`, `status`, `comms`) plus `deploy`, still
print human text rather than JSON on success (tracked in #485). All human and
log text (progress, notes, warnings) goes to **stderr**, so a plain `...
--json | jq` yields clean data. On failure under `--json`, the error is
emitted to stdout as `{"error": "<message>", "fix": "<hint>"|null}` instead of
a prose message, so an agent can recover without parsing prose. `NO_COLOR`,
`CLICOLOR`, and `--color=never` are honored on every command.

**Versioned result schemas.** Every agent-facing `--json` result maps to a
committed JSON Schema under `cli/schema/` with an explicit version identity (the
`vN` segment of its `$id`); `cli/schema/index.json` is the inventory of every
result family, the schema it maps to, and its version. The schemas are embedded
in the released binary, so the discovery path works with no source checkout:
`curie schema-index` prints the inventory index, and `curie schema-index
<name>` (e.g. `curie schema-index kill`) prints one schema. A contract test
(`cli/tests/schema_inventory.rs`) fails CI if a new result family lands without a
schema, and `cli/tests/json_contract.rs` validates every result's real output
against its schema. The compatibility policy — additive changes stay at the same
version, breaking changes ship a new version — is
[ADR-0074](../docs/adr/0074-versioned-json-schemas-for-cli-results.md).

**Semantic exit codes** let an agent branch on *why* a command failed without
parsing output:

| Code | Class     | Meaning                                                                 |
|------|-----------|-------------------------------------------------------------------------|
| 0    | success   | The command did what was asked.                                         |
| 1    | failure   | A genuine runtime failure (well-formed request, operation did not succeed). Do not retry blindly. |
| 2    | usage     | A deterministic input error (missing `--yes`, a malformed flag/value, no bundle). Retrying the same argv fails identically -- fix the input. |
| 3    | transient | A retryable condition (the endpoint was unreachable or timed out). The same argv may succeed once the dependency is up. |
| 4    | unsupported | The verb was understood, but the concept it inspects does not exist at this tier by construction (`curie skill versions`, `curie skill memory`). No input and no retry changes that -- the same argv never succeeds here; the `fix` hint names the tier that does answer it. A gap that a future release could close is exit 1 with a `fix`, never this (`curie local info --check-mcp` / `curie cluster info --check-mcp`). |

**Non-interactive by default.** Every mutating command has a non-interactive
path (`--yes` on `cluster down`/`kill`/`delete`/`reset-thread`, `local
reset-thread`, and `local down --wipe`); none block on stdin. A confirmation
prompt that would otherwise read stdin refuses
with a usage error (exit 2) when the session is not a terminal, rather than
hanging.

(`curie local status` and `curie cluster status` proxy raw
`docker compose`/`helm`/`kubectl` output and do not yet support `--json`; use
`curie skill status` for a machine-readable runner status today.)

## Verify

```bash
cd cli && cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

The scripted E2E (real runner container, fake model by default, offline):

```bash
bash cli/scripts/e2e.sh
```

Requires a `curie-runner` image (`docker build -f runner/Dockerfile -t
curie-runner .` from the repo root) and a cargo toolchain, unless
`CURIE_BIN` points at a prebuilt binary (skips the `cargo build --release`).
`CURIE_E2E_LIVE=1` drops `--fake-model` and runs the skill rung against a
real model, failing fast if no model credential (`ANTHROPIC_API_KEY`,
`CLAUDE_CODE_OAUTH_TOKEN`, or `CURIE_CREDENTIALS`) is present.

The cold-start parity ladder (`curie dev e2e-ladder`, `cli/scripts/e2e-ladder.sh`)
runs the same skill-tier script as rung 1, then adds a local rung (`local up
--minimal` -> `local deploy` -> `local message` with the reply asserted ->
`local down`, against `compose.dev.yaml`) and a cluster rung (`cluster deploy`
then `cluster message`, a real round trip with no manual port-forward) against
a pre-installed release. A `local-release` rung repeats the local rung's exact
round trip against the generated `compose.release.yaml` instead -- the
artifact a release binary's `curie local up` actually runs -- so the CI
config-only check on that file (`compose/generate_release_compose.py` +
`docker compose config`) is not the only coverage it gets. It needs the
release-pinned `ghcr.io/curie-eng/curie-api` and `-worker-local` images
already built and tagged locally (it preflights and fails with a fix hint
otherwise). Two env knobs configure it:

- `CURIE_E2E_TIERS` -- which rungs to run. Defaults to `skill,local`
  (credential-free, CI-safe); `all` runs `skill,local,cluster`. A tier named
  explicitly is required, and its absence fails the run; a tier not named is
  skipped. `local-release` is a fourth tier, named explicitly (e.g.
  `skill,local,local-release`) -- it is not folded into `all` since it needs
  the extra images built first.
- `CURIE_E2E_LIVE` -- unset or `0` runs the fake model (credential-free, the
  default); `1` runs the live-credential variant for pre-release manual passes
  and fails fast if no model credential is present. It governs every named
  rung, including the skill rung: `e2e.sh` reads the same env var itself.
- `CURIE_E2E_LISTEN_HOST` -- cluster rung only. Forwarded verbatim to `cluster
  message --listen-host` (the host the in-cluster worker posts its reply back
  to). Leave it unset for a cluster whose kubeconfig points at a routable API
  server: `cluster message` then auto-detects the local IP the kernel would use
  to reach it. Set it only where auto-detection cannot yield a pod-reachable
  host -- notably a kind/minikube cluster whose API server binds loopback, where
  the auto-detected `127.0.0.1` is unreachable from a pod. CI's kind cluster job
  sets it to the kind Docker network gateway.

The one-command pre-release gate:

```bash
CURIE_E2E_TIERS=all curie dev e2e-ladder
```

# cli

The `curie` CLI is the main tool for interacting with the Curie platform,
for a human or a coding agent alike. It scaffolds a plugin bundle -- an
agent's backend -- and lets you deploy, test, and manage it across the
`skill`, `local`, and `cluster` tiers. For contributors to the Curie
project itself, it also provides the utilities to build, verify, and
extend the platform's own code.

## Table of contents

- [Interactive mode](#interactive-mode)
- [CLI output](#cli-output)
- [Agent driven CLI](#agent-driven-cli)
- [For users](#for-users)
  - [Scaffolding an agent plugin](#scaffolding-an-agent-plugin)
    - [`init --from-spec` spec shape](#init---from-spec-spec-shape)
  - [Using different tiers](#using-different-tiers)
    - [`skill` target](#skill-target)
    - [`local` target](#local-target)
    - [`cluster` target](#cluster-target)
    - [Bundle packing exclusions](#bundle-packing-exclusions)
    - [Artifact resolution](#artifact-resolution)
  - [Managing secrets](#managing-secrets)
- [For contributors](#for-contributors)
  - [`curie install`](#curie-install)
  - [`curie dev`](#curie-dev)
  - [Building the runner image from source](#building-the-runner-image-from-source)
  - [Prototyping agents in a source checkout](#prototyping-agents-in-a-source-checkout)
  - [Verify](#verify)

## Interactive mode

`curie interactive` is a menu-driven way to run the same commands documented
in this file, without memorizing flags. Move through actions with the keys
below; when a command needs a value (message text, a channel id, which tier),
it prompts you inline, then runs the exact command shown in the preview.

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

It currently covers the common commands: `skill up/message/eval`, an
**Explore examples** picker with live agent chat, `secrets set/list/unset`,
`local up/message/status`, `cluster status/message`, `install`, and
`dev contracts`.

**Explore examples** opens a dialog for GitHub issues, Text stats engine, or
Weather. After selection, Curie checks that example's credentials, starts its
bundle once, and opens a persistent conversation. Type a message, read the
reply, and continue for as many turns as needed. Leaving chat stops the runner
and returns to Curie.

## CLI output

| Flag | What it does |
|---|---|
| `--debug` | Show the verbose plumbing (helm/kubectl/compose command lines and their output), dimmed. |
| `-q` / `--quiet` | Print the payload only, suppressing all progress and diagnostics. |
| `--color <auto\|always\|never>` | Control ANSI color (default `auto`). |

The payload (reply tokens, resolved URLs, status/eval results, JSON output)
always goes to **stdout**; every diagnostic (progress, spinners, helm/kubectl/
compose chatter) always goes to **stderr** -- so piping or redirecting the
payload never picks up progress noise.

For `local message` and `cluster message`, interim answer text and tool context
are transient progress on stderr. Only the finalized reply is the stdout
result. In JSON mode, stdout remains exactly one JSON object.

On an interactive terminal, progress renders as spinners and a live checklist;
it degrades automatically to plain, colorless status lines on a non-TTY, in
CI, or when `NO_COLOR`/`TERM=dumb` are set.

## Agent driven CLI

The CLI's primary consumer is a coding agent (ADR-0021, an Architecture
Decision Record), so its output and control flow are machine-first.

Run `curie guide` to print a self-contained primer for a coding agent
driving the harness end to end, to stdout. `--json` emits the same content
as a structured variant.

**`--json`** (global) makes every agent-facing verb emit a single
machine-readable JSON object on **stdout**: the read/query verbs
(`versions`, `memory`, `approvals`, `observability`), the lifecycle result
verbs (`kill`, `resume`, `budget`, `overrides`, `reset-thread`, `delete`), and every
verb's `--dry-run` plan (uniform shape `{"dry_run": true, "plan":
[<lines>]}`).

The `message` verbs keep their own, more specific shapes: `curie local
message` and `curie cluster message` emit one structured line per terminal
state on stdout --

- a completed turn emits `{"reply": ..., "thread": ..., "finalized": ...}`
  (the model's reply, which is null on a no-edit completion, plus the thread
  the turn ran under);
- a turn parked on a human approval gate emits `{"reply": ..., "thread":
  ..., "finalized": false, "awaiting_approval": true}` (the worker posted an
  approval card rather than finalizing, and `reply` is the card's placeholder
  text if seen);
- a **timeout** emits `{"reply": null, "finalized": false,
  "timed_out": true}` before exiting 3 (transient);
- a turn **enqueued** onto the real Valkey stream in connected transport mode
  emits `{"status": "enqueued", "channel": ..., "thread": ...}` -- the CLI
  does not wait for the reply, so this is a terminal state of the command,
  not of the turn; and
- `--json --dry-run` emits a planned-action descriptor `{"dry_run": true,
  "target": "local"|"cluster", "stream": ..., "channel": ...,
  "reply_endpoint": ...}` (`channel` is null when it would be resolved from
  the sole bound `(agent, Slack channel)` pair).

The five shapes are the `oneOf` in `cli/schema/message.schema.json`. Two
exceptions still print human text instead of JSON on success (tracked in
#485): `curie skill message`, and the operator verbs (`up`, `down`,
`status`, `comms`, `deploy`). On generic failure under `--json`, the error is
emitted to stdout as `{"error": "<message>", "fix": "<hint>"|null}` and follows
`cli/schema/error.schema.json`, so an agent can recover without parsing prose.
The exception is `curie cluster deploy --all-targets`, whose reconciliation
failures follow `cli/schema/deploy.schema.json`.

**Versioned result schemas.** Every agent-facing `--json` result maps to a
committed JSON Schema under `cli/schema/`, with an explicit version in its
`$id`; `cli/schema/index.json` inventories every result family. The schemas
ship inside the release binary, so `curie schema-index` (or `curie
schema-index kill`, say) works with no source checkout. CI enforces that
every result has a schema and matches it. Under the compatibility policy, every
closed-schema shape change gets a new identifier: additive optional changes bump
the minor version, while incompatible changes bump the major version. See
[ADR-0101](../docs/adr/0101-schema-compatibility-for-closed-schemas.md).

**Semantic exit codes** let an agent branch on *why* a command failed without
parsing output:

| Code | Class     | Meaning                                                                 |
|------|-----------|-------------------------------------------------------------------------|
| 0    | success   | The command did what was asked.                                         |
| 1    | failure   | A genuine runtime failure (well-formed request, operation did not succeed). Do not retry blindly. |
| 2    | usage     | A deterministic input error (missing `--yes`, a malformed flag/value, no bundle). Retrying the same argv fails identically -- fix the input. |
| 3    | transient | A retryable condition (the endpoint was unreachable or timed out). The same argv may succeed once the dependency is up. |
| 4    | unsupported | The verb was understood, but the concept it inspects does not exist at this tier by construction (`curie skill versions`, `curie skill memory`). No input and no retry changes that -- the same argv never succeeds here; the `fix` hint names the tier that does answer it. |

**Non-interactive by default.** Every mutating command has a non-interactive
path (`--yes` on `cluster down`/`rollback`/`upgrade`/`kill`/`delete`/`reset-thread`,
`local reset-thread`, and `local down --wipe`); none block on stdin. A confirmation
prompt that would otherwise read stdin refuses
with a usage error (exit 2) when the session is not a terminal, rather than
hanging.

(`curie local status` and `curie cluster status` proxy raw
`docker compose`/`helm`/`kubectl` output and do not yet support `--json`; use
`curie skill status` for a machine-readable runner status today.)

## For users

### Scaffolding an agent plugin

| Command | What it does |
|---|---|
| `curie init <name>` | Scaffold a plugin bundle (Claude Code plugin shape: `.claude-plugin/plugin.json`, `skills/<name>/SKILL.md`, `.mcp.json`) plus an `evals/cases.json` seed, a root `AGENTS.md`, and an installable `.claude/skills/using-curie/SKILL.md` harness primer. |
| `curie init --from-spec <path>` | Scaffold **non-interactively** from an agent-authored spec file (JSON). The bundle name comes from the spec, not a positional argument. A coding agent interviews the human, writes the spec, then this command lays down the same plugin-format shape deterministically -- zero prompts. See the spec shape below. |
| `curie init --adopt <dir>` | Adopt an existing non-plugin directory: scaffold the same plugin skeleton **into** it, alongside your code and never overwriting an existing file, with the bundle name derived from the directory unless a `<name>` is given. The on-ramp for a pre-plugin (`agent-ss-template`) bundle; you still need to manually port its logic into the new skeleton afterward -- see `docs/adopting-a-bundle.md`. |

#### `init --from-spec` spec shape

The spec is a JSON object an agent writes after interviewing the human:

- `name` is the kebab-case bundle name.
- Every `skills[].name` is kebab-case and unique.
- `connectors` (optional) is the raw `.mcp.json` `mcpServers` map (each
  server must define `command` or `url` as a string).
- `secrets` (optional) is a list of connector-secret NAMES (env-var-shaped,
  no values, per ADR-0009) written to the manifest's `secrets`.
- `approvalPolicy` (optional) declares approval `gates` (`{gate, route}`)
  where an `mcp__` gate must be a fully-namespaced live tool name
  `mcp__plugin_<bundle>_<server>__<tool>` for a declared connector (a
  built-in like `Bash` needs no prefix) — so a spec can express a gated,
  authed agent without hand-editing `plugin.json`.
- `evals` reuses the frozen eval-case shape so the scaffolded
  `evals/cases.json` loads unchanged through `curie skill eval`.

An unknown TOP-LEVEL field is a hard error, so a typo in the spec fails
loud. Unknown keys inside an eval case are silently ignored instead,
matching the platform's own grading behavior -- intentional parity, not an
oversight.

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

### Using different tiers

Every environment command takes a **target noun** in the middle: `skill`,
`local`, or `cluster`. Pick the lightest one that answers your question.
`curie init` is the exception, a top-level verb that scaffolds a bundle on
disk and targets no environment.

| Target | What runs | Slack | Kubernetes | Verbs | Reach for it to |
|---|---|---|---|---|---|
| `skill` | Just the runner container on the host Docker daemon. No platform, no queue, no API, no Slack. Fully offline. | none | none | `up` `check` `down` `status` `message` `eval` | Iterate a plugin/skill against a local runner, the fastest loop. |
| `local` | The full platform via docker compose (Postgres + Valkey + Langfuse + API + worker). | stub by default, optional real Slack with `--slack` | none | `up` `down` `status` `comms` `message` `eval` `deploy` `overrides` `reset-thread` `delete` | Exercise the real queue -> worker -> sandbox -> reply product loop with zero Slack and zero Kubernetes. Its API is published on host port `28000`. |
| `cluster` | The platform on Kubernetes (a Helm release). | optional | yes | `up` `upgrade` `down` `status` `comms` `message` `eval` `deploy` `kill` `resume` `budget` `overrides` `reset-thread` `delete` | Operate and drive a deployed cluster release, and control its agents' lifecycle. |

`eval` is on all three, running the SAME `evals/cases.json` with the SAME
grader at each tier. A text matcher case (`exact`, `contains`, or `regex`)
that passes at `skill` is checked the same way at `local` and `cluster`.
At `skill`, a `tool_called` grader reads runner tool frames directly. The
legacy local and cluster reply path cannot observe those frames. Cross tier
tool observation uses the trajectory sidecar and platform path below.

For sequence scoring, add `evals/trajectory.json` beside `cases.json`. The
sidecar is owned by the run layer, so it does not change the frozen eval case
schema. Author the file in this shape:

```json
{
  "specs": [
    {
      "case_id": "reports-a-temperature",
      "expected": ["WebSearch", "WebFetch"],
      "mode": "in_order",
      "threshold": 1.0
    }
  ]
}
```

`mode` accepts `exact`, `in_order`, `any_order`, `precision`, or `recall`.
When the sidecar exists, every case is trajectory scored. A case without a
matching spec fails closed and prints `no trajectory spec for case ...`.
The case file and sidecar are packaged in the same immutable deployed bundle.
The deploy receipt's `bundle_sha256` identifies the exact bundle used for
local and cluster parity.

Run the same authored suite at each tier:

```bash
curie skill eval --json
curie local eval --json
curie cluster eval --json
```

Live-model grading defaults to one sample per case, majority aggregation, and
always reports `samples`, `passes`, and `policy` in `--json` so a stochastic
miss is labeled as one draw rather than unexplained tier drift. Raise N with
`--samples 3` (or `CURIE_EVAL_SAMPLES`) on skill, local, and cluster; choose
`--aggregation majority` or `--aggregation pass_at_k` with `--pass-at-k K`.
Each sample starts a fresh conversation. A `--model` sweep on local/cluster
refuses `--samples > 1` because the frozen EvalJob cannot carry N; set
`CURIE_EVAL_SAMPLES` on the worker for the production eval path.

The skill tier scores the runner tool frames directly. The local and cluster
tiers trigger the deployed bundle through the platform eval plane, then poll
the matrix for the exact triggered stream and scorer. Deploy the changed
bundle before those platform runs. An explicit `--cases` is refused for a
local or cluster trajectory run because it cannot replace files inside the
deployed bundle.

`--case-id <ID>` (repeatable) is the case SELECTOR, distinct from `--cases`,
which names the suite FILE. Omitting it runs the whole suite. The exit code is
the gate: **1** at least one selected case failed, **2** the selector matched no
cases, **3** the runner or platform API was unreachable or timed out
(transient), **4** the selector does not apply to this eval plane (a
local/cluster `--model` sweep, or a local/cluster trajectory run, both of
which grade the deployed suite server-side from a suite NAME alone), **0** no
case failed -- not the same as a pass, since an all `plumbing_ok` (ungraded)
run also exits 0. A skill-tier
`--model` sweep boots a transient local runner per model and grades in-CLI, so
`--case-id` IS honored there. Exit 2 fires per value, so
`--case-id greets-the-user --case-id greets-the-usr` fails and names only the
typo rather than silently dropping it.

The distinction that matters: `skill` is the **runner-only** loop, talking
straight to a runner container's ACI (Agent Container Interface) HTTP
surface with no platform in front; `local` and `cluster` put the **full
platform** (queue, worker, sandbox) in front of the identical runner and
ACI, so a `message` walks the same path a real Slack mention would.

#### `skill` target

Boots just the runner container on the host Docker daemon and speaks its ACI
HTTP surface directly. No platform, no queue, no API, no Slack, no cluster.

| Command | What it does |
|---|---|
| `curie skill up` | Boot the local runner image in Docker with the ACI boot env (runner/README.md recipe), wait for health, print the boxed env summary.<br>• `--fake-model` runs offline.<br>• `--network`/`--otel-endpoint` join the compose stack for traces.<br>• `--model <id>` forwards `CURIE_MODEL` (omit for the SDK default).<br>• `--local-model [<id>]` runs a real model through a local Ollama sidecar (default `qwen3:4b`). Its assets are never downloaded implicitly: `up` refuses when the pinned `ollama/ollama` image (~8.9 GB) or the model is not already cached, naming what is missing and its size. `--pull-model` accepts the download for that run (ADR 0093, #1183).<br>• `--secret <NAME>` forwards bundle MCP secrets by name (Curie private storage when the env var isn't exported).<br>• `--env-file <PATH>` reads the model credential from a bundle `.env` as a last resort (precedence: shell env > stored secret > file; only `CURIE_CREDENTIALS`/`CLAUDE_CODE_OAUTH_TOKEN`/`ANTHROPIC_API_KEY`), so a bundle boots live with no `source` step (#749).<br>• A leftover container of the same name fails the boot with a clear fix instead of a raw Docker conflict error; `--replace` removes it and boots fresh. |
| `curie skill check` | Run an offline, credential free MCP load check and report declared servers, matches, and verdict. |
| `curie skill approvals` | View the bundle's declared `approvalPolicy` gates, read straight from `.claude-plugin/plugin.json` (or `plugin.json`); no docker, no network.<br>• `--gate <TOOL>` (repeatable) or `--clear` mutate nothing -- they print the `CURIE_APPROVAL_REQUIRED_TOOLS=...` assignment to export, then re-run your original `skill up` invocation with `--secret CURIE_APPROVAL_REQUIRED_TOOLS` added, since the runner only resolves that env once at container boot. |
| `curie skill versions` | Not available at this tier (exit 4): `skill up` runs a local snapshot of the bundle on disk (its digest is on `skill status`), and nothing is deployed, so no version is assigned. Use `curie local versions <agent>` or `curie cluster versions <agent>`. |
| `curie skill memory` | Not available at this tier (exit 4): this tier configures no memory namespace. Use `curie local memory <agent>` or `curie cluster memory <agent>`. |
| `curie skill message "..."` | Send a synthetic Slack event: POST an ACI `event` frame to the local runner and stream the NDJSON reply (text deltas, tool notes, side effect flags, final). Abort a live turn with Ctrl-C. |
| `curie skill eval` | Run `evals/cases.json` through the runner as `eval_case` events. When `evals/trajectory.json` exists, score the observed tool frames against its case keyed specs. Prints a per case result table plus a pass or fail rollup, with nonzero exit on failure.<br>• `--case-id ID` (repeatable) runs only the named cases; omit it for the whole suite. A value matching no case in the suite exits **2** (usage) and names the typo, so a mistyped selector fails the gate instead of greening an empty run. Also narrows a `--model` sweep: the sweep boots a transient local runner per model and grades in-CLI, so models are compared on exactly the selected cases.<br>• `--samples N` (default 1, env `CURIE_EVAL_SAMPLES`) runs each case N independent times and reduces by `--aggregation majority` or `pass_at_k` (`--pass-at-k K`). `--json` always includes `samples`, `passes`, and `policy`. |
| `curie skill status` | Show the local runner's session status. |
| `curie skill down` | Stop and remove the local runner container. With no `.curie/runner.json` it falls back to container identity, so an orphaned runner is still clearable; `--name <NAME>` targets a container other than `curie-runner-local`. |

`skill up` records the container in the bundle's `.curie/runner.json`
(gitignored by the scaffold); `skill message` / `skill eval` / `skill status` /
`skill down` run from the bundle directory and resolve the runner from it, or
accept `--url`. Setting `skill up --model <id>` makes token usage attributable
in Langfuse traces.

`skill up` also packs the bundle into a content-addressed snapshot under
`<bundle>/.curie/snapshots/<digest>/` and mounts that read-only, matching what
the local and cluster tiers do with a deployed bundle. So editing a bundle file
on the host does not reach the running runner: re-run `curie skill up` and
confirm the new `bundle_digest` in `curie skill status --json`. A verified
same-directory runner is replaced automatically; `--replace` still forces a
restart of an unchanged snapshot or a leftover name. `evals/cases.json` and its
optional `evals/trajectory.json` sidecar are the exceptions. `skill eval` reads
them live from source, so an eval edit needs no restart. `skill down` and a
replacement `up` release the snapshot along with the container.

#### `local` target

Wraps `compose.dev.yaml` via Docker Compose, so a `message` walks the real
queue -> worker -> sandboxed runner -> reply path on one machine, no Slack and
no Kubernetes. `curie local up` uses the `full` compose profile by default.
`curie local up --minimal` uses the smaller `core` profile. The compose API is
published on host port `28000`. Add `curie local up --slack` to also start
the optional Slack dispatcher.

| Command | What it does |
|---|---|
| `curie local up` | Bring the compose stack up (`docker compose --profile full up -d --wait` by default, `docker compose --profile core up -d --wait` with `--minimal`) and print URLs.<br>• `--slack` appends `--profile slack`.<br>• `--build` builds the stack's images from the source checkout as `:dev` and runs those instead of the published ones; it needs a compose file that substitutes the image tags, so a release-channel `curie` must pass `-f compose.dev.yaml` or it refuses before building anything (#1926).<br>• `--local-model [<id>]` adds the `local-model` profile and routes runners at the compose Ollama (default `qwen3:4b`). Same pre-provisioning rule as `skill up`: the ~8.9 GB image and the model must already be cached or `up` refuses before starting anything; `--pull-model` accepts the download for that run (ADR 0093, #1183).<br>• `--env-file <PATH>` reads the model credential from a bundle `.env` as a last resort (precedence: shell env > file; only `CURIE_CREDENTIALS`/`CLAUDE_CODE_OAUTH_TOKEN`/`ANTHROPIC_API_KEY`, and the value never reaches argv or logs), so the stack boots live with no `set -a; source .env` step (#749). |
| `curie local down` | Stop the compose stack (`docker compose down`), keeping volumes. |
| `curie local status` | Show the compose stack's service status (`docker compose ps`). |
| `curie local observability` | Print the local platform's observability surfaces: Curie Console, Langfuse UI (traces / cost / evals), and the Curie API base. URLs are printed only; pass `--open` to also open the browsable ones (Console, Langfuse) in a browser. `--json` never opens a browser. |
| `curie local observability runs` | List newest-first trace rows through the local Curie API. `--limit` defaults to 20 and accepts 1 through 100; `--agent-id <id>` restricts the list. |
| `curie local observability run <trace-id>` | Read one complete trace tree previously returned by `runs` (or reported by a completed turn). |
| `curie local observability metrics` | Read the metrics summary, or a series with `--metric runs\|latency_p95_ms\|tokens\|cost_usd\|error_rate`. Series `--granularity hour\|day\|week` defaults to `day`; all metrics queries accept the API's independent `--start`, `--end`, `--environment`, and `--agent` filters. Series results are capped at 1,000 points. |
| `curie local comms --slack` | Connect or disconnect a real Slack workspace for the compose stack.<br>• Resolves `SLACK_APP_TOKEN` and `SLACK_BOT_TOKEN` with precedence `--app-token`/`--bot-token` flag > env var > a value persisted with `curie secrets set` (so tokens saved once need no per-session re-export, #749).<br>• Masks them in dry run output.<br>• Starts or stops the dispatcher, and switches the worker between real Slack and the local stub. |
| `curie local message "..."` | Drive the local compose stack end to end with zero Slack. Enqueues straight to the compose Valkey and lets the containerized worker answer. |
| `curie local eval` | Run the deployed bundle's eval suite through the compose platform. Without a trajectory sidecar, it uses the enqueue, worker, sandbox, and reply path with the shared grader, isolating each case from ambient durable agent memory so the gate is the immutable bundle plus committed cases (#1909). With `evals/trajectory.json`, it triggers the worker eval plane and reads each structured trajectory verdict from the exact matrix stream. Prints the same per case table and rollup, with nonzero exit on failure.<br>• `--cases` overrides the file only for a text graded run. It is refused for a trajectory run because the platform grades the deployed bundle.<br>• `--case-id ID` (repeatable) selects which cases run; omit it for the whole suite. A value matching no case exits **2** (usage) naming the typo, so a mistyped selector fails the gate rather than greening an empty run. Refused with exit 4 on a `--model` sweep and on a trajectory run, both of which grade the deployed suite server-side.<br>• `--samples N` / `--aggregation` / `--pass-at-k` match `skill eval`. A `--model` sweep refuses `--samples > 1`.<br>• `--dry-run` prints the plan.<br>• `--concurrency` defaults to 1. Values above 1 are refused for now (#709). |
| `curie local deploy` | Package the bundle as tar.gz and push it to the compose platform API (`--api-url`, default `http://localhost:28000`). Auth via `--api-key` or `CURIE_API_KEY`. |
| `curie local memory <agent>` | List the agent's durable memory log (`GET /agents/{id}/memory`). Empty when none exist. |
| `curie local memory <agent> --add <content>` | Append an operator-authored memory record (`POST /agents/{id}/memory`). The API stamps operator provenance. A fresh session is required before the entry is injected at boot. `--dry-run` prints the plan. |
| `curie local approvals <agent>` | Inspect or configure approval gates and route bindings, list pending records with each card's real bound channel, mint subject-bound resolver credentials, or resolve one record.<br>• `--mint-operator-principal <SUBJECT>` administratively mints a reusable token whose one-time output is exported as `CURIE_APPROVAL_PRINCIPAL_TOKEN`; the token carries no Slack channel and may resolve only an explicit-user route containing that subject.<br>• `--mint-console-login-code <SUBJECT>` creates a single-use code that exchanges for an HttpOnly, subject-bound Console session.<br>• `--resolve <ID>` approves by default; add `--reject` to reject and `--note` for an optional reason. It sends only decision/note, derives the actor from `CURIE_APPROVAL_PRINCIPAL_TOKEN`, and has no `--as` or `--actor-channel` assertion flags.<br>• Requester equality neither grants nor vetoes: the authenticated requester may self-confirm only when the selected approver set admits that same subject. A distinct-person requirement is a separate policy, not an ordinary gate default. |
| `curie local overrides <agent> [--model V\|--clear-model] [--thinking V\|--clear-thinking]` | Read or change the agent's two nullable operator overrides via the compose platform API (`PATCH /agents/{id}`).<br>• With no change flags it INSPECTS and writes nothing.<br>• `--clear-<field>` sends explicit JSON null, restoring the platform default; an omitted field is left alone, which is a different request the API tells apart with `model_fields_set`.<br>• A blank value is refused rather than forwarded: an empty override skips the platform default instead of restoring it. |
| `curie local reset-thread <agent> --thread-key <key> --yes` | Force a stuck thread's sandbox to be released via the compose platform API (`POST /agents/{id}/threads/{thread_key}/reset`, #737).<br>• The worker's next maintenance tick releases the thread's claim and route, so its next message cold-creates a fresh sandbox; conversation history is not deleted.<br>• Interrupts a live turn on the thread first, so it refuses without `--yes`. |
| `curie local delete <agent> --yes` | End every active deployment, then delete the agent through the compose platform API. Destructive and irreversible: refuses without `--yes`.<br>• If the final agent deletion fails, the agent remains present but any deployments already ended stay ended. |

##### `curie local message`: the same roundtrip against the compose stack

`local message` drives the local compose stack (`curie local up`) instead of a
Kubernetes release, so the whole loop is one machine with no cluster:

```bash
curie local up
curie local deploy --plugin-dir <dir> --slack-channel C0123ABCD --api-url http://localhost:28000
curie local message "what changed in the last deploy?"
```

Local mode drops every cluster-specific step -- no kubectl, no `helm upgrade`
wiring, no port-forwards, no dispatcher guard -- and answers by claiming a
runner container on the host Docker daemon instead of a Kubernetes sandbox.
Channel comes from `--channel` or, when omitted, the sole deployed agent
looked up on the compose API. `local message` composes with `--channel`,
`--thread`, and `--timeout-secs`, and rejects the cluster-only flags
(`--namespace`, `--release`, ...) with a clear error.

The compose worker runs the fake model by default (a canned reply, no
credentials). Export a credential in your shell, or use `--env-file` (see
the table above), and `local up`/`local comms` go live automatically. Set
`CURIE_FAKE_MODEL=1` to force the fake model regardless of a credential
being present.

Use `curie local comms --slack` to point the same compose stack at a real
Slack workspace (see the table above for token precedence and masking); it
resolves the model the same way as `local up` (live when a credential is
present, fake otherwise). `--disconnect` stops the dispatcher and restores
the local stub; `--dry-run` prints the compose command only.

`--continue` works the same way here as it does for
[`cluster message`](#curie-cluster-message-drive-the-deployed-cluster):
it reuses the last successful `local message` context from
`.curie/last-turn.json` in the current working directory, so only the new text
is required, explicit flags override the saved channel/thread/transport
settings, and the same replay exclusions apply.

#### `cluster` target

Wraps the umbrella Helm chart and the deployed release, the way `linkerd` or
`cilium` wrap theirs. Every operator verb takes `--dry-run`. Full runbook in
[`docs/operations.md`](../docs/operations.md).

| Command | What it does |
|---|---|
| `curie cluster up` | Install or upgrade the release (`helm upgrade --install`), then wait up to five minutes for the target workload images, observed generations and replica counts to converge. A failed rollout returns nonzero with redacted failure reasons.<br>• Exposes the UI and Langfuse on node ports; `--no-expose` keeps them ClusterIP-only.<br>• Set `CURIE_CREDENTIALS` (deprecated alias `CURIE_MODEL_CREDENTIALS`) for a real model, or install sealed with canned replies.<br>• A shell `CURIE_MODEL` defaults the sandbox runner model (`agentSandbox.runner.model`) for cross-tier parity with `local up`, unless an explicit `--set agentSandbox.runner.model=` is passed; a shell `CURIE_MODEL` that disagrees with such an explicit `--set` fails loud.<br>• `--github-token` (or `CURIE_GITHUB_TOKEN`) supplies the API's GitHub credential for private-repo git-flow clones; the CLI passes it to helm through a private 0600 values file rather than as a command-line argument, so it does not appear in the helm command, the printed plan, or that plan's JSON.<br>• Prefer the `CURIE_GITHUB_TOKEN` environment variable: a token typed after the flag sits in `curie`'s own argv, so it still lands in your shell history and in `ps`.<br>• Omitting both preserves whatever the release already recorded, and `--clear-github-token` removes it. Passing the token alongside `--set api.githubToken=` fails loud.<br>• `--dev` keeps the chart's published credential defaults, but still re-supplies Slack tokens recorded by `cluster comms`, a previously generated sealing key, and the GitHub App values a sibling verb recorded. A later `--dev` is a full Helm upgrade and does not restore those empty chart defaults (#1134).<br>• `--forward-only` applies contract or irreversible schema migrations. The default refuses them before mutation so a patch rollback window stays intact (#2300). |
| `curie cluster down` | Uninstall the release and sweep its runtime namespaces (`helm uninstall` + `kubectl delete namespace`); prompts unless `--yes`. |
| `curie cluster rollback` | Roll the release back to a prior Helm revision (`helm rollback`); prompts unless `--yes`.<br>• With no `--revision`, auto-selects the newest revision whose status is `deployed` or `superseded`, skipping any `failed`/`pending-*`/`uninstalling` revision in between.<br>• `--revision <n>` targets an exact revision instead; one that is not `deployed`/`superseded` is refused unless `--allow-failed-revision` is also passed (requires `--revision`). |
| `curie cluster upgrade --to <version>` | Resumable upgrade lifecycle: plan, validate, drain, checkpoint, migrate, apply, exact convergence, canary, known-good commit. The operator does not pass Helm merge flags. Success requires exact convergence and a target-version canary. A failure leaves the previous known-good version serving or returns one fail-forward command. `--dry-run` prints the redacted plan. |
| `curie cluster status` | Report release health, exact target-image convergence, pod readiness, access URLs, the current upgrade phase, and the last known-good version (read-only). Returns exit 1 when the target has not converged, including an old ready replica beside a failed replacement. `--json` retains the status payload and lists redacted rollout reasons in `pods.unhealthy`. Tagged images reported under a sibling alias require read access to the serving Pod's Node: one image inventory entry must bind the requested tag, reported alias and exact runtime image ID. Missing or ambiguous inventory fails closed. This verifies loaded-image binding, not mutable-tag freshness. |
| `curie cluster observability` | Report the release's observability surfaces (Curie Console, Langfuse UI, Curie API base), using the same NodePort discovery as `cluster status`.<br>• Degrades a missing, ClusterIP, or unresolvable surface to a note instead of failing.<br>• URLs are printed only; pass `--open` to also open the browsable ones (Console, Langfuse) in a browser. `--json` never opens a browser.<br>• `--dry-run` prints the read-only discovery commands. |
| `curie cluster observability runs\|run\|metrics` | The same read-only query grammar and results as local observability. Omit `--api-url` to self-plumb a loopback port-forward to the API selected by `--namespace` / `--release` (both default to `curie`); omit `--api-key` there to read the release Secret. A direct `--api-url` requires its matching `--api-key`, so a discovered release key is never sent to an arbitrary endpoint. |
| `curie cluster comms --slack` | Connect or disconnect a real Slack workspace with a thin `helm upgrade --reuse-values`; env-backed tokens are masked in dry-run output. |
| `curie cluster message "..."` | Drive the deployed release end to end. With a connected dispatcher, it posts a placeholder and routes the reply to the agent's bound Slack channel. Without a dispatcher, it uses the terminal reply stub and waits for the reply.<br>• Auto-discovers the release-generated API key and Valkey password from `<release>-secrets` when `--api-key` / `--valkey-password` (or their env vars) are omitted, so a default strong-secrets install needs no hand-exported credentials (#786). |
| `curie cluster eval` | Run the deployed bundle's eval suite through the Kubernetes platform. Without a trajectory sidecar, it uses the reply stub path with the shared grader, isolating each case from ambient durable agent memory so the gate is the immutable bundle plus committed cases (#1909). With `evals/trajectory.json`, it triggers the worker eval plane and reads each structured trajectory verdict from the exact matrix stream. Prints the same per case table and rollup, with nonzero exit on failure.<br>• `--cases` overrides the file only for a text graded run. It is refused for a trajectory run because the platform grades the deployed bundle.<br>• `--case-id ID` (repeatable) selects which cases run; omit it for the whole suite. A value matching no case exits **2** (usage) naming the typo, so a mistyped selector fails the gate rather than greening an empty run. Refused with exit 4 on a `--model` sweep and on a trajectory run, both of which grade the deployed suite server-side.<br>• `--samples N` / `--aggregation` / `--pass-at-k` match `skill eval`. A `--model` sweep refuses `--samples > 1`.<br>• `--dry-run` prints the plan.<br>• `--concurrency` defaults to 1. Values above 1 are refused for now (#709).<br>• Auto discovers the release generated API key and Valkey password from `<release>-secrets` when `--api-key` or `--valkey-password`, and their environment variables, are omitted. A default strong secrets install needs no hand exported credentials (#790). |
| `curie cluster deploy` | Package the bundle as tar.gz and push it to the platform API.<br>• When `--api-url` is omitted, self-plumbs a `kubectl port-forward` (loopback tunnel) to the release API service and auto-discovers the release-generated key from `<release>-secrets`, so the strong key never crosses the cleartext UI proxy (ADR-0057). `--api-local-port` picks the local end of that tunnel; default `0` lets the kernel assign an ephemeral port, matching `cluster message` and `cluster eval`, so concurrent deploys cannot collide on a fixed port.<br>• Before posting, verifies the self-plumbed tunnel's unauthenticated `GET /health` really answers the Curie API (`{"status": "ok"}`); refuses a 404, an HTML 200, a non-`ok` JSON 200, or a redirect, since a squatted local port or a misresolved Service are both TCP-alive and would pass the port-forward's own readiness check. This verification does not run against an explicit `--api-url`.<br>• Pass `--api-url` / `CURIE_API_URL` to direct-dial a URL instead (no tunnel); an explicit `--api-key` / `CURIE_API_KEY` still wins over discovery. |
| `curie cluster kill <agent> --yes` | Kill an agent (stop its runs) via the platform API (`POST /agents/{id}/kill`). Destructive: refuses without `--yes`. |
| `curie cluster resume <agent>` | Resume a killed agent via the platform API (`POST /agents/{id}/resume`). |
| `curie cluster budget <agent> --limit <n>` | Set the agent's daily spend cap in USD via the platform API (`PUT /agents/{id}/budget`, `BudgetConfig.max_usd_per_day`); the per-run token cap is left at the platform default. |
| `curie cluster approvals <agent>` | The deployed equivalent of `local approvals`: inspect/configure gates and routes, list pending records, administratively mint an operator principal or Console login code, and resolve using an authenticated principal.<br>• A platform API key can mint credentials but cannot itself resolve. `--resolve <ID>` reads `CURIE_APPROVAL_PRINCIPAL_TOKEN`; its subject must be in the route's explicit users list because a terminal principal carries no Slack channel/group evidence.<br>• `--list` reports `card_channel`, and a parked-turn hint names that route-bound authenticated-card channel rather than the requesting-channel stub. Use the Slack card for channel-membership or group routes. |
| `curie cluster overrides <agent> [--model V\|--clear-model] [--thinking V\|--clear-thinking]` | Read or change the agent's two nullable operator overrides via the platform API (`PATCH /agents/{id}`).<br>• With no change flags it INSPECTS and writes nothing.<br>• `--clear-<field>` sends explicit JSON null, restoring the platform default; an omitted field is left alone, which is a different request the API tells apart with `model_fields_set`.<br>• A blank value is refused rather than forwarded: an empty override skips the platform default instead of restoring it. |
| `curie cluster reset-thread <agent> --thread-key <key> --yes` | Force a stuck thread's sandbox to be released via the platform API (`POST /agents/{id}/threads/{thread_key}/reset`, #737).<br>• The worker's next maintenance tick releases the thread's claim and route, so its next message cold-creates a fresh sandbox; conversation history is not deleted.<br>• Interrupts a live turn on the thread first, so it refuses without `--yes`. |
| `curie cluster delete <agent> --yes` | End every active deployment, then delete the agent through the platform API. Destructive and irreversible: refuses without `--yes`.<br>• If the final agent deletion fails, the agent remains present but any deployments already ended stay ended. |

##### `curie local|cluster observability`: API-backed queries

The bare commands above remain URL/surface reports. Queries are read-only and
non-interactive; they use only the Curie API proxy and its existing DTOs, never
Langfuse or backend credentials. `--open` is available only on the bare surface
report and is rejected with a query; `cluster observability --dry-run` likewise
applies only to bare discovery. There is deliberately no `--latest`: use the
trace id emitted by a completed turn, then inspect it with `run`.

For either `<tier>` (`local` or `cluster`), the grammar is `curie <tier>
observability runs [--limit 1..100] [--agent-id <id>]`, `curie <tier>
observability run <trace-id>`, or `curie <tier> observability metrics
[--metric <enum> [--granularity <enum>]] [--start <ISO-8601>] [--end
<ISO-8601>] [--environment <name>] [--agent <name>]`.

```bash
curie --json local observability runs --limit 20 --agent-id acme-agent
curie --json local observability run trace_abc
curie --json local observability metrics --metric tokens --granularity hour \
  --start 2026-08-23T00:00:00Z --end 2026-08-24T00:00:00Z \
  --environment development --agent acme-bot
curie --json cluster observability --namespace curie --release curie runs --limit 20
```

`runs` returns the bounded wrapper `{"limit", "count", "runs"}` (schema
`https://schemas.curietech.ai/cli/observability-runs/v1.json`); `run` returns
the complete `TraceTree` DTO (`trace`, `tree`, `sandbox_id`, and
`approval_decision`; schema
`https://schemas.curietech.ai/cli/observability-run/v1.json`). `metrics` returns
the direct summary DTO when `--metric` is omitted, or the direct series DTO
when it is present (schema
`https://schemas.curietech.ai/cli/observability-metrics/v1.json`). The metrics
filter is deliberately `--agent`, while the runs filter is `--agent-id`,
matching their API routes.

With `--json`, each successful query writes exactly one typed object to stdout;
human guidance and progress stay on stderr (and respect `--quiet`). An unknown,
well-formed trace id writes `{"error","fix"}` and exits 1; an unavailable API
writes a distinct `{"error","fix"}` and exits 3. Invalid input, including a
limit outside 1 through 100 or `--granularity` without `--metric`, exits 2.

| `curie cluster memory <agent>` | List the agent's durable memory log (`GET /agents/{id}/memory`). Empty when none exist. |
| `curie cluster memory <agent> --add <content>` | Append an operator-authored memory record (`POST /agents/{id}/memory`). The API stamps operator provenance. A fresh session is required before the entry is injected at boot. `--dry-run` prints the plan. |

All authenticated cluster API verbs (`versions`, `memory`, `approvals`, `kill`,
`resume`, `budget`, `overrides`, `reset-thread`, and `delete`) act on a deployed
release through the same platform API. When `--api-url` is omitted, they
self plumb a loopback tunnel to the release API service and discover the
release key. An explicit nonloopback `http://` `--api-url` refuses a
discovered key. Pass `--api-key` explicitly as the opt in, use HTTPS, or
omit `--api-url` to use the loopback tunnel. The agent target verbs resolve
`<agent>` (a name or id) to its API id with the same lookup `deploy` uses. Each
takes `--dry-run` (prints the plan, makes no request); the destructive
`kill`/`reset-thread`/`delete` also require `--yes`.

The approval resolve call is the deliberate identity-bound exception: the discovered or
explicit platform key can list/configure approvals and mint a principal, but it is never
sent as resolver identity. `--resolve` instead sends `CURIE_APPROVAL_PRINCIPAL_TOKEN` in the
approval-principal header and a body containing only `decision` plus optional `note`.

##### `curie cluster message`: drive the deployed cluster

`cluster message` targets a **deployed** Helm release and manages its own
port-forwards. A connected dispatcher sends the placeholder and reply through
the agent's bound Slack channel. A disconnected release uses the terminal reply
stub, so a developer can exercise the whole deployed machinery without Slack
access or tokens.

```bash
curie cluster message "summarize the latest deploy"
curie cluster message --channel CSIM123 "another question"
```

What it does: self-manages its own port-forwards, then follows the release's
transport. A connected dispatcher receives a placeholder and sends the reply to
the bound Slack channel. Without a dispatcher, the terminal reply stub receives
the reply and the command prints it in the terminal.

- **Picks a channel.** With no `--channel`, it looks up the sole deployed
  agent's channel via the API; zero or multiple agents is an error requiring
  `--channel` explicitly (the worker binds a channel to an agent by exact
  equality, so guessing would route nowhere).
- **Enqueues the event.** In connected mode, follow the placeholder and reply in
  the agent's bound Slack channel. In disconnected mode, it waits for the
  terminal reply and prints a continuation context. A timeout prints stream
  diagnostics and exits nonzero.

`--dry-run` prints the kubectl port-forward details and enqueue description
without executing anything.

`--continue` reuses the last successful `cluster message` context from
`.curie/last-turn.json` in the current working directory, so only the new text
is required. Explicit flags override the saved channel, thread, and transport
settings, the verb must match, and the API key is re-read from
`$CURIE_API_KEY` because the value is never stored. `--continue` does not
replay `--stream`, `--listen-port`, `--valkey-local-port`, `--api-local-port`,
or `--user`, so pass any of those again explicitly if the original turn used a
non-default value.

The worker binds a channel to an agent by exact equality on the channel
address stored in `agent_channels`, so a random synthetic channel can never
reach a deployed agent. Use `--channel <id>` to target a specific channel: pass the
same value you gave `cluster deploy --slack-channel` and the worker routes the
turn to that agent.

```bash
curie cluster deploy --slack-channel CSIM123 ...
curie cluster message --channel CSIM123 "first question"
```

In disconnected mode, each turn mints a fresh thread ts by default. On
completion `cluster message` prints a `continue this conversation: ...` line
with the channel and thread ts; copy-paste it, or pass `--thread <ts>` yourself,
to send the next turn into the same thread:

```bash
curie cluster message --channel CSIM123 --thread 1720000000.000100 "follow up question"
```

Against a **connected** Slack workspace the CLI posts a real placeholder to the
agent's bound channel and routes the reply there. Follow the placeholder in
Slack for the conversation. A thread ts passed with `--thread <ts>` must name a
real message in that channel; a thread ts from a disconnected stub run is not
valid in Slack.

To connect a real workspace so replies route through Slack, use:

```bash
SLACK_APP_TOKEN=xapp-... \
SLACK_BOT_TOKEN=xoxb-... \
curie cluster comms --slack

curie cluster comms --slack --disconnect

SLACK_APP_TOKEN=xapp-... \
SLACK_BOT_TOKEN=xoxb-... \
curie cluster comms --slack --dry-run
```

#### Bundle packing exclusions

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
(absolute, or containing a `.` or `..` path segment) is dropped. Symlinks
are still a packing error unless excluded, by design: the packer never
dereferences a link to upload host files from outside the bundle root.

#### Artifact resolution

Release builds resolve default artifacts from the binary version: `curie local
up` fetches the self-contained `compose.release.yaml` release asset, so it
works with no checkout, `curie cluster up` uses the pinned chart release
asset, and runner sessions (`curie skill up`) use the pinned runner image
from GHCR (GitHub Container Registry). Fetched artifacts cache under
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

### Managing secrets

Local secrets are stored in `~/.config/curie/credentials.json` with mode 0600,
not in the repo, shell history, command argv, `.env`, or Curie state files.
Curie keeps a separate non-secret index so secret names can be listed without
opening values.

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

Connector secrets that `curie cluster deploy` writes into a target namespace
must be scoped to that cluster identity, Helm release, and namespace. A name
saved for one cluster is refused on another instead of being silently reused.
`curie secrets list` prints the scope and version, never the value. Replacing a
scoped value requires `--expected-version` from that listing.

```bash
CURIE_CLUSTER_ID="ca:$(kubectl config view --minify --raw -o json | jq -r '.clusters[0].cluster | ((.server // "") + "\\n" + (."certificate-authority-data" // ."certificate-authority" // ""))' | sha256sum | awk '{print $1}')"
curie secrets set K8S_KUBECONFIG --from-env K8S_KUBECONFIG \
  --cluster-identity "$CURIE_CLUSTER_ID" --release curie --namespace curie-test
curie secrets set K8S_KUBECONFIG --from-env K8S_KUBECONFIG \
  --cluster-identity ca:0123abcd --release curie --namespace curie-test \
  --expected-version 1
```

Unscoped names remain for skill/local credentials (model keys, Slack tokens).
During cluster deploy, Curie resolves owned connector keys against the target
scope, prints the names it is about to write, and warns when it is replacing a
non-empty key in the live Secret. Process environment is a first-run fallback
only when the store has no entry for that name. Issue #440 tracks the future
per agent delivery path.

`curie skill up --secret <NAME>` first uses a real environment variable when
one is already set. If it is missing, the CLI tries the Curie secret store and
hydrates the process environment just long enough for Docker to forward `-e
<NAME>` into the runner. The same lookup applies to saved model credentials
(`CURIE_CREDENTIALS`, `ANTHROPIC_API_KEY`, or `CLAUDE_CODE_OAUTH_TOKEN`) for
live `skill up` runs.

## For contributors

### `curie install`

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

### `curie dev`

Thin wrappers over the repo's dev scripts, so contributors get one unified
`curie <command>` surface while the scripts stay the implementation. Most find
the repo root, confirm one script exists, shell `bash <script>` from the root,
stream its output, and propagate its exit code. `curie dev chart-check` instead
discovers every direct executable chart assertion under `charts/curie/ci`, runs
all of them, reports each result, and returns aggregate failure after every
script finishes. Run outside a source checkout they error clearly because a
release binary has no dev scripts.

| Command | What it does |
|---|---|
| `curie dev contracts` | `bash scripts/check-contracts.sh` -- check the frozen contracts. |
| `curie dev chart-check` | Discover the executable assertion scripts under `charts/curie/ci`, the same set helm-ci runs, run every one, report each result, and return aggregate failure after all scripts finish. |
| `curie dev verify-fix-pin <CHANGE> <SELECTOR>` | Prove that a fix makes the selected test fail when only its product files are reversed. |
| `curie dev e2e` | `bash cli/scripts/e2e.sh` -- the scripted CLI end-to-end test. |
| `curie dev e2e-ladder` | `bash cli/scripts/e2e-ladder.sh` -- the cold-start parity ladder (skill, local, cluster rungs). |
| `curie dev field-parity` | `bash cli/scripts/check-field-parity.sh` -- assert CLI `api.rs` mirror structs cover their platform API model fields (#691), and CLI `commands.rs`/`spec.rs` mirror structs cover the frozen `packages/plugin-format` schema's fields (#701). |
| `curie dev emit-parity` | `bash cli/scripts/check-emit-parity.sh` -- assert a `CliOutput::to_json` that hand-projects a mirror struct into a `json!` literal covers that struct's fields, one hop downstream of `field-parity` (#699). |
| `curie dev wire-tolerance` | `bash scripts/check-wire-tolerance.sh` -- assert every direct `ClassName.model_validate*(...)` call on an `_AciModel` subclass threads `READER_CONTEXT` or is a declared exception (#625). |

Use `curie dev verify-fix-pin <CHANGE> <SELECTOR>` from a source checkout to
verify a fix commit or pull request. `<CHANGE>` accepts a committed change
resolvable by Git or a pull request number or URL. The selector must be one of
these forms:

1. `apps/.../tests/test_x.py::test_y`, `packages/.../tests/test_x.py::test_y`,
   or `runner/tests/test_x.py::test_y` for a Python test.
2. `cli/tests/name.rs::test_name` for a Rust integration test.
3. `charts/curie/ci/name.sh` for a chart check script.

The command runs the selector at the current `HEAD`, reverses only the change's
non test files in a disposable worktree, then runs the selector again. It prints
`PINNED` and exits successfully only when the changed selected test node owns the
failure after a clean reversal. For Python, the selected pytest testcase must
carry the sole JUnit failure element. For Rust, the exact selected test must fail
at runtime, or a compile error must point inside the changed selected function.
Chart checks must return nonzero. It prints `UNPINNED` and exits nonzero when the
selector remains green.

It refuses invalid commit or pull request references, root commits, changes
without classified test files or product files, selectors outside the five
forms or not changed by the reference, a red baseline, and reverse patch
conflicts. It also refuses unrelated collection, import, compile, setup, and
teardown failures. Inline tests in product files are not inferred.

### Building the runner image from source

| Command | What it does |
|---|---|
| `curie build` | Build the runner image locally from `runner/Dockerfile` at the repo root. Default tag is `curie-runner`; the same image is also tagged `ghcr.io/curie-eng/curie-runner:dev` so a `curie local up --build` stack sees it. `--tag` overrides the primary tag (a custom tag is not also applied as `:dev`). Prints a clear error if Docker is not installed or if run outside a source checkout -- a release binary pulls the pinned runner image from GHCR automatically and never needs to build. |

### Prototyping agents in a source checkout

Two shortcuts for working with the repo's own `agents/` scratch directory:

| Command | What it does |
|---|---|
| `curie list-agents` | List the plugin bundles under `agents/`, a personal, gitignored directory (sibling of `examples/`, source checkout only) for in-progress agent projects. Empty, not an error, when the directory doesn't exist. |
| `curie deploy-local <folder>` | Deploy `agents/<folder>` to the local platform by name -- shorthand for `curie local deploy --plugin-dir agents/<folder>` (identical operation, same flags minus `--plugin-dir`). Local tier only; use `curie cluster deploy --plugin-dir agents/<folder>` for the cluster tier. The interactive "How to deploy to Slack" workflow offers the same `agents/` bundles as a picker. |

### Verify

```bash
cd cli && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
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
runs rung 1 as the skill-tier script against the same bundle every other rung
drives, then adds a local rung (`local up
--minimal` -> `local deploy` -> `local message` with the reply asserted ->
`local down`, against `compose.dev.yaml`) and a cluster rung (`cluster deploy`
then `cluster message`, a real round trip with no manual port-forward) against
a pre-installed release. It asserts one bundle identity, one eval suite, and
one model mode across every rung and fails on divergence.

A `local-release` rung repeats the local rung's exact round trip against the
generated `compose.release.yaml` instead -- the artifact a release binary's
`curie local up` actually runs -- so the CI config-only check on that file
(`compose/generate_release_compose.py` + `docker compose config`) is not the
only coverage it gets. It needs the release-pinned
`ghcr.io/curie-eng/curie-api` and `-worker-local` images already built and
tagged locally (it preflights and fails with a fix hint otherwise).

Three env knobs configure it:

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

`CURIE_E2E_BUNDLE` is not a ladder knob: the ladder hardcodes `examples/weather`
as its bundle source and sets the var itself when it invokes `cli/scripts/e2e.sh`
for rung 1, so every rung drives the same bundle. It is a knob of `e2e.sh` --
set it standalone to drive `e2e.sh` against a named bundle instead of
scaffolding its own `deal-desk` one; leave it unset and `e2e.sh` keeps that
scaffold. Exporting it before a ladder run has no effect on the ladder's rungs.

The one-command pre-release gate:

```bash
CURIE_E2E_TIERS=all curie dev e2e-ladder
```

# runner

The runner image and SDK adapter: the productized prototype,
a long-lived streaming session server that implements the full ACI (Agent
Container Interface) v0.1 contract from `packages/aci-protocol`. Built on
`claude-agent-sdk` (Python).
Runs inside a claimed Agent Sandbox; the CLI (`curie skill up`) also runs it
locally in Docker.

## What it does

- One long-lived `claude-agent-sdk` streaming-input session per process (one per
  sandbox) -- the source of prompt-cache affinity across turns.
- Accepts inbound ACI frames (`event` of type message | job | eval_case, and
  `interrupt`) and streams outbound NDJSON (newline-delimited JSON:
  `text_delta` | `tool_note` | `final` | `error` | `side_effect_flag`) with
  protocol-version enforcement.
- Enforces `CURIE_BUDGET.max_output_tokens_per_run` (halts a run with a
  classified-failure final) and hands the daily USD cap to the SDK natively.
- Emits `side_effect_flag` when a non-idempotent tool executes (read-only
  allowlist, deny-by-default; see `side_effects.py`).
- Loads and validates the mounted plugin bundle via `plugin_format.validate_bundle`.
- Always exposes the Claude Code file tools and Curie's
  `mcp__curie__publish_changes` tool, independent of bundle skills and bundle
  MCP policy. Publication remains unusable without a managed `/workspace` and
  only records an approval request; the trusted worker publishes after approval.
- When a managed git checkout is mounted at `/workspace`, the runner prepends
  that fact and the fail-closed network-git posture to the session system
  prompt so the model edits in place instead of cloning over the network.
- Offers Anthropic's provider-side `WebSearch` tool by default. A bundle can
  suppress it with a root `curie.bundle.json` containing
  `{"webSearch": false}`; the provider connection remains the only network
  path, so this adds no sandbox web egress.
- Exports gen_ai OTel spans: an `agent.run` root with duration-bearing
  `llm.generation` provider-wait and `execute_tool` tool-wait siblings, via
  OTLP-HTTP (the OpenTelemetry Protocol over HTTP) to the collector, which
  forwards to Langfuse.
- Rehydrates from a history ref on start (`resume`), stateless-first
  (ADR-0003, an Architecture Decision Record). Completed turns are stored as
  structured replay; oversized text payloads are replaced with stable digest
  markers before the per-thread transcript cap the state API advertises
  (`api.transcriptMaxThreadBytes`, 16 MiB by default) while tool-call
  structure remains intact. An append failure makes the runner's
  `history_durable` status fail closed for the rest of that process.

## HTTP surface (ACI channel)

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness. |
| GET | `/status` | Probe-safe session status, readiness, turn state, and transcript durability. |
| GET | `/v1/status` | Bearer-authenticated handoff status, including session/sandbox identity and managed-workspace cwd attestation. |
| POST | `/v1/event` | Open a turn: body is an ACI `event` frame; streams outbound NDJSON, ending in a `final`. |
| POST | `/v1/steer` | Inject a follow-up into the live turn (`{"text": ...}`); 409 when no turn is active. |
| POST | `/v1/interrupt` | Hard-stop the live turn: body is an ACI `interrupt` frame. |

One turn consumes the SDK generator at a time; steer and interrupt are
side-channel injections whose output surfaces on the open `/v1/event` stream (the
proven steering pattern). The finish race (a steer arriving as a turn ends,
409) is owned by the worker.

### Caller feedback

Connector availability notices describe unavailable connected services in plain
language. Missing sign-in settings, rejected access, invalid connection settings,
and unsuccessful connection attempts remain distinguishable; notices never show
connector identifiers, credential environment-variable names, MCP terminology or
sandbox details. Only observed causes are described. Retry counts may be shown,
without promising that a retry will succeed.

Technical diagnosis retains the connector name, credential names (never values),
reason and refusal in logs. The shared connector diagnosis vector continues to
verify this diagnostic representation. The final reply and tool exclusion hook
use the caller representation. Recovery removes the notice as before.

Policy refusals and changed approval details describe the action in ordinary
words. MCP names use their last nonempty suffix, underscores and hyphens become
spaces, and camel case becomes words; Bash and Skill are shell and instruction
requests. Invalid labels fall back to “action”. Refusals still prevent execution,
never mint an approval for a policy denial and never spend a mismatched grant.

At boot, the runner snapshots held credential values before hosted connector
environment cleanup. A common outbound boundary replaces those exact values,
their standard and URL-safe base64 forms when that encoding is at least 8 characters, and recognized secret patterns
with placeholders in assistant replies and structured tool results. Protocol
keys, control metadata and approval argument carriers retain their original
values. A buffer shared across `TextDelta` chunks protects held values split
between chunks. Clean text streams normally; overlapping matches may remain
buffered until the overlap ends or the turn completes. Recognition of unknown
token patterns does not guarantee protection for fragments split across chunks.

The SDK parent retains its model authentication. Its Bash tool uses a mandatory
image owned launcher that removes platform credentials before Bash reads user
startup files or SDK snapshots. The launcher isolates Python imports from the
workspace and preserves the shell arguments, exit status and signals. Missing or
nonexecutable launchers reject session startup instead of selecting another shell.
Declared connector secrets remain available to bundle tools.

Each `text_delta` the runner emits is one whole assistant text block, and a
consumer joins deltas as they come. The same boundary therefore starts a later
block of the turn on its own paragraph (a blank line), unless the model already
put whitespace at the join or the join falls where redaction acts. The unbroken
text is scrubbed once and the breaks are placed into that result, so with its
breaks removed every emitted text is exactly what redaction makes of the
unbroken text. A `final` whose text falls back to the streamed text, with or
without the connector notice ahead of it, carries the same breaks and is
scrubbed whole.

The control routes (`/v1/event`, `/v1/steer`, `/v1/interrupt`, `/v1/reset`,
`/v1/snapshot`, and `/v1/status`) require an `Authorization: Bearer <token>`
header matching `CURIE_RUNNER_TOKEN` when that env var is set, returning 401
otherwise. This is per-sandbox transport auth (defense-in-depth on the ACI
ingress alongside the NetworkPolicy), not part of the frozen ACI wire contract.
A runner with the var unset or blank refuses to boot with
`RunnerTokenRequiredError` unless `CURIE_RUNNER_ALLOW_TOKENLESS=1` is set, which
only `curie skill up` and the eval sweep set; the chart always supplies a token
from the `<release>-runner-token` Secret (#3821). `GET
/healthz` and probe-only `GET /status` are never gated (the chart readinessProbe
hits `/healthz`); replacement authority comes only from authenticated
`GET /v1/status`.

## Deliberate progress (ADR 0130)

The runner mounts a `progress` tool on its platform `curie` MCP server, so the
model sees it as `mcp__curie__progress`. It is ADR 0130's `curie_progress`
operation. Its input schema is the committed `ProgressCommand` schema
(`packages/channel-protocol/schema/channel-protocol.schema.json`) without
`version`, which the runner fills in: `update_id`, `state`, `summary` and an
optional `milestone`. A factory execution mounts `report_progress` instead and
never both. When the tool is mounted the system prompt carries a short progress
block beside the memory and workspace blocks: call it only on long,
multi-step tasks, at material transitions, and never for a quick answer; a
milestone (`evidence`, `scope` or `verification`) only when the step is
material, at most three per task; and never reasoning, raw tool output, secrets
or a draft answer in the summary. The tool description repeats the rules.

The tool and its progress prompt are mounted only when the worker booted this
sandbox with `CURIE_TURN_PROGRESS_ENABLED=1`. The worker sets that direct
runner boot fact only for a human Slack thread and includes it in sandbox reuse
comparison; jobs, cron, email, relay, targetless and factory sessions retain
their pre-progress model surface. This leaves the frozen ACI unchanged.

The capability is per turn. The worker first allocates a durable, monotonically
increasing generation on the chain and marks it active. It sends the capability
on `POST /v1/event` in three runner control headers, `X-Curie-Progress-Url`,
`X-Curie-Progress-Token`, and `X-Curie-Progress-Generation`,
next to `X-Curie-Capacity-Admission`; like the turn epoch they are not ACI
fields. The token is signed for `progress_id:generation`, and the API also
checks that the same generation is still active and before its bounded
server-time lease before it appends anything. The worker renews that five-second
lease while the stream is open, and cannot revive a missed lease, so a failed
turn-end clear loses authority promptly rather than leaving a 24-hour token
usable.
The runner holds the three values only while that turn is open, sets them when
the turn starts and clears them when it ends, so a steer uses the turn's
capability and a closed or superseded turn cannot enqueue. The names are frozen
with the worker and the API in `tests/vectors/turn-progress-capability.json`.

Each call POSTs the command to the capability URL with the token in
`X-API-Key`, adding the worker-issued `generation` and a `seq` that counts this
turn's posts from 1. The generation remains monotonic across runner restarts,
clock skew, retries, and cold approval resumes; the worker applies a turn's
commands in order and refuses an older generation after a newer one. HTTP 202
means `Progress queued.`, because durable application can still refuse it. A call
without a capability returns "Progress is not shown for this turn." and makes
no network call. A refusal, a rate limit, a 5xx or a transport failure is a
soft tool result that tells the model to continue; it never fails the turn. An
input the schema refuses (an unknown field, a bad state or summary) is an error
result the model can correct, still without failing the turn. The call is
platform-owned and idempotent (`PLATFORM_IDEMPOTENT_TOOLS`), so it raises no
`side_effect_flag` and never lands on the turn's receipt.

The fake model is unconditionally network-free, including when progress headers
are present. `[fake:progress-demo]` remains a deterministic long-running turn
for steering tests, but it does not call the ingress. API/worker integration
tests and live-provider tests exercise the real progress handler instead.

## Per-turn tool access

The runner is the reference enforcement of TOOL-ACCESS in
[the ACI producer seam](../docs/interfaces/aci-producer/INTERFACE.md). For a
turn whose `Event.tool_access` is `read-only`:

- **RUNNER-TOOL-ACCESS-1:** The tools classified read-only are the ones the
  side-effect classifier already treats as read-only: the harness's declared
  read-only built-ins (for the Claude harness `Read`, `Glob`, `Grep`, `LS`,
  `NotebookRead`, `WebFetch`, `WebSearch`, `ToolSearch`, `TodoRead`) and each
  live MCP tool whose server declared `readOnlyHint: true` on the boot probe,
  less every tool an operator gate, a bundle approval gate or a `toolPolicy`
  `approvalRequired` pattern names. The same set built at boot feeds both the
  classifier and this check. The MCP half is each connector's own annotation:
  a server that marks a tool `readOnlyHint: true` has classified it
  read-only. Curie's platform tools are not read-only, including
  `mcp__curie__request_approval`, `mcp__curie__report_progress` and
  `mcp__curie__get_issue`, which the
  classifier treats as idempotent.
- **RUNNER-TOOL-ACCESS-2:** Any other tool call is denied before it executes.
  The runner registers a PreToolUse callback for every tool and wraps every
  other PreToolUse callback and the permission callback, so the read-only
  decision is made first for each call: the approval gate records no pending
  approval and spends no grant, the runner's own registration of the bundle's
  PreToolUse commands does not run, and the call never reaches its tool or
  connector. A deny from any callback wins, so no other callback can turn it
  into an allow. A call whose tool name cannot be read is denied, and so is
  every call when the decision itself fails. The denial does not end the turn:
  the model receives the refusal as that call's error result and can still
  answer. The CLI also loads the bundle as a plugin and runs the bundle's own
  hook commands on their lifecycle events (PreToolUse, UserPromptSubmit, Stop,
  SessionStart and the like), beside and concurrently with the runner's
  callbacks, including for a call the front refuses; a bundle that ships its
  hooks only in `hooks/hooks.json` has no runner registration at all. Those
  commands are the bundle's code, not tools the model calls: they run on a
  read-only turn as on any other, can change state the bundle keeps for later
  turns, can start a CLI turn of their own (an `asyncRewake` hook), and cannot
  turn the deny into an allow.
- **RUNNER-TOOL-ACCESS-3:** The turn never ends `awaiting-approval` and its
  `final` carries no approval field. A `request_approval` or `publish_changes`
  call in it is denied like any other write and is not captured as a request.
- **RUNNER-TOOL-ACCESS-4:** The access in force is the one sent with the most
  recent prompt. It is set immediately before the prompt is sent and stays
  until the next turn sends its own, so model activity left over from an
  abandoned read-only turn remains read-only. A read-only turn accepts no
  steer, and a `POST /v1/steer` whose `tool_access` differs from the live
  turn's is refused with `409`. A read-only event runs only on a session that
  has sent no unrestricted prompt, neither an ordinary turn nor any steer,
  since it started or was reset. An unrestricted prompt can leave work the
  CLI answers as turns of its own after the runner has moved on (a steer, a
  background task's notification), and such a turn would shift a read-only
  prompt out of its turn and past the next turn's change of access. A
  read-only turn can start neither; the work a bundle's own hook can still
  start is kept from ever running unrestricted by RUNNER-TOOL-ACCESS-11.
- **RUNNER-TOOL-ACCESS-5:** `GET /status` and `GET /v1/status` carry
  `"tool_access": ["read-only"]` while the session can enforce it: it was
  built with enforcement and has sent no unrestricted prompt
  (RUNNER-TOOL-ACCESS-4). Otherwise they carry `"tool_access": []`, so a
  worker refuses the turn before sending it. A read-only event that arrives
  anyway is answered with a classified-failure `final` whose error is
  classified `tool-access-unenforced`, before any connector or model work.
- **RUNNER-TOOL-ACCESS-6:** A denied call, including one denied because its
  decision failed, counts as `refused` on `curie.tool.result` and never as a
  connector `error`, so it logs no connector warning.
- **RUNNER-TOOL-ACCESS-7:** With `tool_access` null the run is today's: the
  fronting callbacks abstain and delegate unchanged, and the permission mode
  is unchanged (`bypassPermissions` when the session has no approval gate).
- **RUNNER-TOOL-ACCESS-8:** The offline fake model session applies the same
  decision before it emulates any tool, and answers a denied call with an
  error result in place of the scripted one, so the fake tier observes what
  the real CLI does.
- **RUNNER-TOOL-ACCESS-9:** A read-only event whose text, less leading
  whitespace, begins with `/` is refused with a classified-failure `final`,
  its error classified `tool-access-refused`, before the model is asked: the
  CLI expands a slash command before any tool call, and a bundle command can
  run shell with no PreToolUse event.
- **RUNNER-TOOL-ACCESS-10:** A read-only turn does not use up an approved
  action's one-shot boot grant. The grant stays for the next unrestricted
  turn, the only kind that may spend it.
- **RUNNER-TOOL-ACCESS-11:** The first unrestricted prompt after any read-only
  prompt on a session is sent on a fresh SDK session, built exactly as
  `POST /v1/reset` builds one. Nothing a read-only prompt left in the CLI, an
  answer it still owes or a turn a bundle hook woke, can then run under
  unrestricted access; while the old CLI shuts down, it refuses such a call
  itself. The read-only turns' conversation is not carried into the new
  session, which is rebuilt from the thread's history as it was at boot, and
  that ordinary turn pays the cost of a new session (the old CLI's shutdown,
  a fresh start, no warm prompt cache). The read-only turns are still
  recorded in the thread's durable history, so a later boot of that thread
  replays them.

## Portable assistant message groups

Structured history preserves the logical assistant message that produced each
fragment, in addition to its ordered role/content blocks (#3628). Native Claude
replay uses the same `message.id` for fragments of one assistant message. With
SDK 0.2.159 / CLI 2.1.281, reconstructing interleaved fragments without that
identity can replace successful tool results with an internal missing-result
marker. This grouping is replay metadata, never approval or tool authority.

- **RUNNER-HISTORY-GROUP-1:** Capture an optional, bounded assistant group only
  from the provider's observed `StreamEvent` `message_start.message.id`.
  `AssistantMessage` does not expose that ID in the pinned Python SDK. The
  adapter carries only a validated opaque identity through its internal
  boundary, excluded from representations and telemetry; it never forwards the
  raw event, thinking/signatures, or SDK session identity. A missing or malformed
  message start clears previous group attribution instead of inheriting it.
- **RUNNER-HISTORY-GROUP-2:** Persist optional `assistant_group` on runner-local
  `ConversationMessage` assistant rows. Fragments belong to the current observed
  start only; tool-result rows do not create or change its identity. A genuine
  user message, steer, new turn, or different observed assistant start ends the
  earlier attribution. An ID reused after such a boundary must not join the
  earlier group. This adds no ACI or plugin-format field.
- **RUNNER-HISTORY-GROUP-3:** Portable replay retains every row and content block
  in original order, including tool IDs, arguments, success/error results, and
  fresh user boundaries. Reconstructed native assistant envelopes use the same
  scoped `message.id` only for fragments with proven same-group identity. No
  inference from adjacency, result arrival, tool name, or prose may combine
  independent messages or move a dependent call before the result it consumes.
- **RUNNER-HISTORY-GROUP-4:** Old role/content-only records remain readable.
  Unambiguous sequential call/result history works without a native checkpoint.
  A legacy checkpoint can supply missing group identity only when its assistant
  and user conversation rows match the authoritative portable prefix exactly
  and unambiguously, with valid IDs and causal boundaries. Its prompt snapshots,
  attachment availability, other envelope fields, and extra content are never
  imported by this recovery. If an overlapping tool-call sequence needs grouping
  that neither portable metadata nor validated native mapping proves, never
  submit that known corrupt replay or manufacture a result. Replay that one turn
  as its visible text instead: the turn runs from a genuine user message to the
  next one, and it becomes that user message followed by one assistant message
  carrying the turn's assistant text blocks in order (or, if it had none, a text
  saying its tool activity could not be replayed). Every other turn keeps its
  rows unchanged, a native checkpoint is set aside, and the runner logs a WARNING
  naming the session and the number of turns reduced, never their content.
  Refusing the whole history instead strands the thread: the runner exits
  before it is ready, every retry and every later message in that thread fails
  the same way, and a thread whose key is fixed, such as an inbound hook's, can
  never start fresh.
- **RUNNER-HISTORY-GROUP-5:** Reject malformed persisted group metadata and
  inconsistent provenance, including attribution on user rows and reuse across
  genuine user or distinct assistant-group boundaries. Preserve group identity
  through text reduction and removal of optional native checkpoints. Existing
  tool pairing, suspended-call closure, whole-turn pruning, and transcript byte
  limits retain their current behavior; metadata counts toward those limits.
- **RUNNER-HISTORY-GROUP-6:** This boot's system prompt remains authoritative.
  A native checkpoint with a different `prompt_snapshot` is still set aside;
  recovering only validated historical group identity does not restore that
  checkpoint or claim previous attachment paths exist in the fresh sandbox.

The regression surface is a fresh native SDK resume after actual session
capture: a scripted external provider emits one assistant message with
interleaved tool fragments/results, portable persistence drops the optional
checkpoint, and the next provider request must contain all exact successful
results without missing-result markers. Distinct/dependent messages, a new user,
sequential legacy history, ambiguous legacy history, and a changed current
prompt are required secondary paths. The local fixture uses owned disposable
resources and fake provider credentials, with no real model or approval action.

## Environment

- **ACI-frozen** (`aci-protocol.SessionConfig`): `CURIE_PLUGIN_DIR`,
  `CURIE_SESSION_ID`, `CURIE_SANDBOX_ID`, `CURIE_BUDGET`, optional
  `CURIE_MEMORY_REF` / `CURIE_CREDENTIALS`, `OTEL_EXPORTER_OTLP_*`.
- **Runner-local**: `CURIE_MODEL`, `CURIE_MAX_TURNS`,
  `CURIE_HISTORY_REF` (rehydrate this thread from the durable state API),
  `CURIE_HISTORY_MAX_TURNS` / `CURIE_HISTORY_MAX_BYTES` (bound the rehydrated
  structured prefix with stable summary boundaries; defaults 40 turns / 16000
  bytes, a nonpositive value falls back to the default),
  `CURIE_MEMORY_MAX_FACTS` (how many facts each memory may hold and boot
  loads into the prompt, one number for both; default 200, a nonpositive or
  unparseable value falls back to the default),
  `CURIE_RUNNER_PORT`, `CURIE_RUNNER_TOKEN` (per-sandbox bearer token gating
  the control routes; required: the runner refuses to boot without it),
  `CURIE_RUNNER_ALLOW_TOKENLESS` (local development only, `1` or `true`: serve
  without a bearer token; a set token is still enforced), `CURIE_FAKE_MODEL`
  (offline smoke; no model call), `CURIE_DISALLOWED_TOOLS` (optional
  comma-separated tool names removed from the session and refused even under
  bypassPermissions; unset keeps every tool available). This stops the named
  tools, not the underlying capability (#2429): denying
  `mcp__curie-state__append`/`set`/`delete` does not revoke
  `CURIE_STATE_TOKEN`, which stays in the sandbox environment, so a
  still-permitted shell tool (e.g. `Bash`) can reach the same HTTP state API
  directly. Naming `Bash` alongside the state tools closes that path today;
  removing the token itself needs a code change, not a config knob.
  `CURIE_CHANNEL_BOUND` (optional, set by the worker when the turn has a
  channel kind and address) adds the `SendMessage` and `PushNotification`
  built-ins to that same removed list (#3336): neither reaches anyone from a
  channel agent. Unset or false leaves the list exactly as configured.
  Hosted-connector Bearer secrets are a different class (#2559): the runner
  expands `Authorization: Bearer ${NAME}` into the in-memory MCP catalog at
  boot and drops `NAME` from the process environment (and from
  `CURIE_CONNECTOR_SECRET_KEYS`) before the session accepts turns, so `Bash`
  cannot read the PAT. #2635 keeps that in-memory catalog off the claude CLI
  argv by writing `--mcp-config` JSON to a 0600 tempfile; same-uid `Bash` can
  still cat the file, and that is accepted. The on-disk catalog keeps the
  placeholder. Residual:
  kubelet / `docker -e` still injects the value onto the container until the
  runner process unsets it. ADR-0009 `--secret` names that are not a hosted
  Bearer stay in the environment, because stdio / remote MCP clients still
  expand `${VAR}` there.

## Build and smoke

The image compiles against the frozen workspace packages, so build from the repo
root with `curie build` (the one build entry point; it wraps the
`docker build -f runner/Dockerfile` under the hood):

```bash
curie build
# Offline round-trip (fake model, no credential), OTel to the dev collector:
docker run -d --name runner-smoke --network curie_default \
  -e CURIE_FAKE_MODEL=1 -e CURIE_PLUGIN_DIR=/unused \
  -e CURIE_RUNNER_ALLOW_TOKENLESS=1 \
  -e CURIE_SESSION_ID=smoke -e CURIE_SANDBOX_ID=sbx \
  -e 'CURIE_BUDGET={"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}' \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
  -p 18080:8080 curie-runner
curl -sN -X POST http://localhost:18080/v1/event -H 'Content-Type: application/json' \
  -d '{"kind":"event","type":"message","text":"hi","user":"U","ts":"1.0"}'
```

## MCP load check (offline, credential-free)

`python -m curie_runner.check` is a separate, one-shot entrypoint (issue #337)
that answers "do this bundle's MCP (Model Context Protocol) tools actually
load?" without a model turn. It
validates the bundle via the frozen `load_plugins`, then builds a real
`ClaudeSDKClient` and `connect()`s (no query), polls `get_mcp_status()` until the
bundle's own servers settle, and compares the **declared** servers against the
plugin-owned **registered** ones. `declared` is the union of the MCP-config
servers (`plugin.json` `mcpServers` and a bare `.mcp.json`) and the bundle's
`connectors.yaml` connectors, each name counted once; the check mounts the
connector forms this tier can actually reach (a remote `url:` and a hosted
connector's `unhosted_url:` fallback), so those are genuinely exercised, while
a purely hosted connector is declared but not exercisable here. It reads
`CURIE_PLUGIN_DIR` (and optional `CURIE_CHECK_TIMEOUT_S`, default 30); it
forwards and reads **no** credential.

```bash
CURIE_PLUGIN_DIR=/plugin python -m curie_runner.check
```

It prints exactly one JSON object to **stdout** (all logging goes to **stderr**)
and exits with the verdict code:

- `0` green: every declared MCP server registered connected with at least one tool
- `1` red: a declared server failed to load (never registered, connected with zero
  tools, `failed`/`needs-auth`/`pending` at the deadline, or the init timed out),
  including a declared `connectors.yaml` connector this tier cannot host
- `2` invalid_bundle: the bundle fails `plugin_format` validation or the plugin dir
  is missing

The JSON shape (frozen contract) is `{check, version, plugin_dir, declared,
registered, matches, verdict, reasons, hints}`; `reasons` is non-empty iff the
verdict is not green. A manifest `mcpServers` string pointer is now rejected by
`plugin_format` validation (step 1, `invalid_bundle`) before this check ever runs,
so the #336 string-pointer hint fires only when `extract_declared`/`evaluate` are
exercised directly (e.g. in tests), not through this entrypoint.
The `curie skill check` CLI verb wraps this as a one-shot container.

## Verify (from repo root)

```bash
uv run pytest runner/tests -q   # unit + integration + conformance
uv run ruff check . && uv run mypy
```

Most live tests (`runner/tests/test_live.py`) run only when
`CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` is present. Disposable tests
explicitly selected with `CURIE_E2E_LIVE=1`, including the provider-side web
search proof, may instead use an already-authenticated local Claude SDK. Without
either authentication path they fail honestly rather than fabricating a live
result.

## Approval display wording

Permission approval records retain the exact tool identifier and arguments.
The card and waiting notice describe the requested action in plain language,
including when a bundle has no summary template or its template cannot render.
Do not present MCP identifiers or argument field names as prose. Preserve the
requested values and do not imply that the pending action has run.

Approval metadata references follow the same sentence boundary rules across API,
worker, runner and the UI fallback for older API responses. A tool identifier
followed by a sentence-ending period (including whitespace or closing punctuation)
uses its plain action label. Filenames, extensions, paths, URLs and identifiers
embedded within another word remain literal data, including Unicode text. The
stored summary, exact grant target, arguments and nested content remain unchanged.

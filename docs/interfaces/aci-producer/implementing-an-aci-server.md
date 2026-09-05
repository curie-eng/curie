# Guide: implementing an ACI server

> Part of the #25 epic (a second harness behind the ACI protocol). Companion to
> [INTERFACE.md](./INTERFACE.md) (the frozen protocol) and driven by the
> conformance suite in `packages/aci-protocol/src/aci_protocol/conformance.py`.

The ACI (Agent Container Interface) is the seam that makes the **harness**
swappable: anything inside the sandbox that speaks the ACI wire contract can
replace the default claude-agent-sdk runner without the worker, CLI, or UI
changing. This guide shows how to stand up a second, conformant ACI server from
scratch, using the conformance suite as your specification and acceptance test —
you should not need to read the reference runner's internals.

The frozen contract lives in `packages/aci-protocol`; import its Pydantic models
rather than re-deriving the wire shapes. The models are the single source of
truth (the committed JSON Schema and generated Rust/TS are derived from them).

## What "an ACI server" is

An ACI server is an **HTTP process** inside the sandbox that exposes six POST
routes and streams NDJSON back:

| Route | Purpose |
| --- | --- |
| `POST /v1/event` | Open a turn. Body is an inbound `event` frame; the response streams outbound NDJSON, ending in a `final` event. |
| `POST /v1/steer` | Inject a follow-up `event` into the live turn; return `409` when no turn is active (the caller then falls back to a fresh `/v1/event`). |
| `POST /v1/interrupt` | Hard-stop the live turn. Body is an `interrupt` frame; the open turn's `final` is reclassified to idle. |
| `POST /v1/reset` | Discard the conversation and start a fresh model session, so the next turn cannot answer from earlier history; return `409` while a turn is active. No body, no wire frame (#550). |
| `POST /v1/snapshot` | Capture a bounded, credential-free snapshot of the managed repository workspace for the authenticated worker; return `409` when the session has no managed workspace. |
| `POST /v1/timeout` | Stop the exact open turn named by the event response epoch. This runner-private control route is authenticated; a server omitting the epoch response header is simply not notified, and worker timeout classification remains unaffected. |

Plus two unauthenticated GETs the platform relies on: `GET /healthz` (liveness)
and `GET /status` (session status + readiness). The chart's readiness probe hits
`/healthz` with no auth header, so keep those two open even when the POST routes
are token-gated.

`/v1/reset` is the odd one out and is easy to skip: it carries no ACI frame, so
it is a runner control route rather than part of the frozen wire union. It is
still required. The worker's eval driver resets before every non-`shared_history`
sample (`apps/worker/src/curie_worker/eval/runner.py::EvalRunner._isolate` over
`apps/worker/src/curie_worker/runner_client.py::RunnerClient.reset`), and `curie`'s
own eval path calls the same route (`cli/src/runner.rs`); a reset that fails is
reported as a failed case, so a server without this route fails every eval case
that has not opted into shared history. The reference implementation is
`runner/src/curie_runner/server.py`.

Startup configuration remains **environment-only**. Read it once with
`SessionConfig.from_env()` (the `CURIE_*` mapping: `plugin_dir`, `session_id`,
`sandbox_id`, `budget`, optional `memory_ref`, `credentials_ref`, `otel`). The
inbound `Event` separately has optional `session_id`, `history_ref`, and
`adoption_credential` fields. The first two are conversation-scoped identity
after a sandbox is bound. `adoption_credential` is the optional per-conversation
runner credential delivered on this same authenticated event (ADR-0122); omitted
or null means no adoption. They are wire metadata, not a replacement for
`SessionConfig`, and older producers may omit any of them. A schema field is
not bootstrap retirement or warm-pool activation.

## The wire contract in one screen

Import everything from `aci_protocol`; do not hand-roll JSON.

**Inbound** — a discriminated union on `kind` (`parse_inbound` decodes it):

- `Event` = `{kind: "event", type: "message"|"job"|"eval_case", text, user, ts,
  session_id?, history_ref?, adoption_credential?}`. The optional fields are
  nullable strings and may be omitted independently. `adoption_credential` is
  secret material: omit/null is the legacy cold path, a malformed value is a
  hard decode error, and the value must not appear in `repr`, errors, model
  prompts, or evidence.
- `Interrupt` = `{kind: "interrupt", reason}`

**Outbound** — a discriminated union on `type`, each carrying `version`
(`to_ndjson_line` encodes, `parse_ndjson_line` decodes one line):

- `TextDelta` → `text_delta` `{version, text, adoption_applied?}`
- `ToolNote` → `tool_note` `{version, text, tool?, adoption_applied?}`
- `Final` → `final` `{version, text, status, adoption_applied?}` where `status ∈ {done,
  idle-awaiting-input, classified-failure}`
- `ErrorEvent` → `error` `{version, message, classification?, adoption_applied?}`
- `SideEffectFlag` → `side_effect_flag` `{version, tool?, detail?, call_id?,
  arguments?, result?, failed?, adoption_applied?}`, emitted once when a
  side-effecting call is made and once when its result arrives, joined on
  `call_id` (ADR-0117)

**Version gate (strict producer, tolerant consumer).** Your producer emits its
**exact build `PROTOCOL_VERSION`** (currently `0.4.6`) on every outbound event and
constructs strictly -- an unknown field is an error at construction, catching your
mistakes at the source. A **consumer** decoding the wire is tolerant the other way:
it accepts any version compatible with its own build (`major.minor` match under
0.x, `major` after 1.0) and ignores unknown fields it does not model, so a new
optional field is backward compatible. The decoder raises `ProtocolVersionError`
on a missing version or an **incompatible** one, with a message naming both
versions -- a same-`major.minor` patch difference is not an error. Constructing the
frozen models and calling `to_ndjson_line` gives you the right version and wire
shape for free, so you never assemble version strings by hand. (Policy and
rationale: ADR-0036.)

## Step 1 — model your turn as a producer

Before writing any HTTP, capture your harness's core behavior as a **`Producer`**:
a plain function mapping one inbound frame to the NDJSON lines your server would
emit for it. This is exactly what the conformance suite validates, and it lets you
reach `passed=True` before you have a socket open.

```python
from collections.abc import Iterable

from aci_protocol import (
    Event, Interrupt, Final, TextDelta, ToolNote, SideEffectFlag,
    SessionStatus, to_ndjson_line,
)

def my_producer(message: Event | Interrupt) -> Iterable[str]:
    # Interrupt: end the turn as idle-awaiting-input.
    if isinstance(message, Interrupt):
        return [to_ndjson_line(Final(text="interrupted",
                                     status=SessionStatus.IDLE_AWAITING_INPUT))]

    # Event: do your harness's real work here (call your model, run tools),
    # emitting events as you go. Every stream MUST end in a Final.
    lines = [to_ndjson_line(TextDelta(text="working..."))]
    # ... your model/tool loop appends ToolNote / TextDelta / SideEffectFlag ...
    lines.append(to_ndjson_line(Final(text="all done", status=SessionStatus.DONE)))
    return lines
```

Note what you did **not** do: no version strings, no JSON assembly, no field
names. Constructing the frozen models and calling `to_ndjson_line` guarantees the
version gate and the exact wire shape.

## Step 2 — run the conformance suite against your producer

`run_conformance` is your acceptance test. Pass your producer; it returns a
`ConformanceReport` (it never raises — every failure is a failing `CheckResult`).

```python
from aci_protocol import run_conformance
from my_harness import my_producer

report = run_conformance(my_producer)
assert report.passed, report.summary()
```

The suite runs five checks (the last only when you pass a producer):

| Check | What it proves |
| --- | --- |
| `outbound_roundtrip` | Every outbound event type survives encode→decode (library-level; always runs). |
| `inbound_roundtrip` | Every inbound frame survives encode→decode (always runs). |
| `reject_unknown_version` | An **incompatible** `9.9.9` line is rejected with `ProtocolVersionError` naming both versions (a same-`major.minor` patch line is accepted). |
| `reject_missing_version` | A version-less line is rejected. |
| `producer_stream` | **Your** producer emits a well-formed stream for every inbound case. |

`producer_stream` is where your implementation is judged. It exercises **all four
inbound cases** — `event:message`, `event:job`, `event:eval_case`, and
`interrupt` — and for each requires:

1. the producer emits **at least one** event (no empty stream);
2. the stream **ends in a `final`** event;
3. **every** event carries a `version` -- your producer's exact build
   `PROTOCOL_VERSION`.

A harness that handles ordinary messages but drops `interrupt` or a batch `job`
will fail here — that is the point. A concrete failing example: a producer that
emits only a `text_delta` and no `final` fails `producer_stream` with a detail
naming the missing `final`.

## Step 3 — wrap the producer in the HTTP routes

Once the producer is conformant, the server is thin: parse the body with
`parse_inbound`, reject the wrong frame type, and stream the producer's lines.
Sketch (framework-agnostic; the reference uses aiohttp):

```python
# POST /v1/event
frame = parse_inbound(await request.json())      # -> Event | Interrupt
if not isinstance(frame, Event):
    return json_response({"error": "use /v1/interrupt for interrupts"}, status=400)
# stream NDJSON, content-type application/x-ndjson
for line in my_producer(frame):
    await response.write(line.encode("utf-8"))
```

Route contract to honor:

- **`/v1/event`**: body must be an `event` frame (400 otherwise). Response
  `Content-Type: application/x-ndjson`, streamed, ending in `final`.
- **`/v1/steer`**: body is an `event` frame injected into the live turn; its output
  surfaces on the open `/v1/event` stream. Return **409** when no turn is active.
- **`/v1/interrupt`**: body is an `interrupt` frame; hard-stop the live turn and
  let its `final` reclassify to `idle-awaiting-input`.
- **`/v1/reset`**: no body. Tear down the conversation and start a fresh session,
  keeping the process (and any per-process setup) alive. Return **409** while a
  turn is active, rather than resetting under an open `/v1/event` stream.
- **`/v1/snapshot`**: no body. Return the bounded managed-workspace snapshot to
  the authenticated worker, or **409** when no managed workspace is mounted.
- **`/v1/timeout`**: no body. Require the event response's opaque epoch header and
  stop only that open turn. This is a runner-private authenticated control route;
  servers omitting the response header are simply not notified, and worker timeout
  classification remains unaffected.
- **`/healthz`**, **`/status`**: always-open GETs; `/status` returns
  `{status, ready, turn_active}`.
- **Auth (optional):** when a bearer token is configured, require
  `Authorization: Bearer <token>` on the POST routes only, compared with a
  constant-time check; leave the GETs open for the probe.

Map any decode/validation error on a POST body to a **400** so a malformed frame
is a clean client error, not a 500.

## Step 4 — read session setup from the environment

At process start, build your config from env and honor it:

```python
from aci_protocol import SessionConfig

cfg = SessionConfig.from_env()   # CURIE_PLUGIN_DIR, CURIE_SESSION_ID, ...
# cfg.plugin_dir: mount point of the Claude Code plugin bundle to load
# cfg.budget:     Budget{max_output_tokens_per_run, task_budget_hint?, max_usd_per_day}
# cfg.otel:       optional OTLP endpoint/headers/protocol
```

Your server must interpret the plugin bundle mounted at `CURIE_PLUGIN_DIR` (the
Claude Code plugin shape — see the
[bundle-format seam](../bundle-format/INTERFACE.md)). This is the one documented
entanglement: a foreign harness still has to load that bundle shape.

## Step 5 — wire it as a test gate (recommended)

Make conformance a permanent gate, exactly as the protocol package and the runner
do. The runner builds its producer by driving the real session to completion and
collecting the NDJSON it emits (`runner/src/curie_runner/conformance.py`); your
producer can do the same over your implementation:

```python
def test_my_harness_is_conformant() -> None:
    report = run_conformance(my_producer)
    assert report.passed, report.summary()
```

If your producer needs to run an async turn to completion (most real harnesses
do), collect the stream inside the sync `Producer` boundary — e.g. with
`anyio.run` — the way the reference runner's `conformance_producer` does; the
suite only needs the finished list of lines.

## Acceptance checklist

You have a conformant ACI server when:

- [ ] `run_conformance(my_producer).passed` is `True` (all five checks).
- [ ] The producer ends every stream in a `final` and handles `interrupt` and
      `job`/`eval_case`, not just `message`.
- [ ] All outbound events are built from `aci_protocol` models (version gate free).
- [ ] `POST /v1/event` streams `application/x-ndjson`; `/v1/steer` returns 409 with
      no live turn; `/v1/interrupt` reclassifies to idle.
- [ ] `POST /v1/reset` discards the conversation between turns and returns 409
      while a turn is active, so eval cases stay isolated.
- [ ] `POST /v1/snapshot` returns the bounded managed-workspace snapshot, or 409
      when the session has no managed workspace.
- [ ] `GET /healthz` and `GET /status` are open and unauthenticated.
- [ ] `SessionConfig.from_env()` is honored and the `CURIE_PLUGIN_DIR` bundle is
      loaded.

## Cross-links

- [INTERFACE.md](./INTERFACE.md) — the frozen ACI protocol and its swap-readiness grade.
- `packages/aci-protocol/README.md` — the full contract surface and decisions.
- `packages/aci-protocol/src/aci_protocol/conformance.py` — the suite this guide is driven from.
- `packages/aci-protocol/src/aci_protocol/reference.py` — a minimal conformant `reference_producer` to copy from.
- `runner/src/curie_runner/conformance.py` — the reference producer built over a real session (the pattern for Step 5).

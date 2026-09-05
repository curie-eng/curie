# aci-protocol

The frozen ACI (Agent Container Interface) contract: the
session protocol and NDJSON (newline-delimited JSON) event types that every
lane compiles against. The
Pydantic models here are the single source of truth; the committed JSON Schema,
the generated TypeScript, and the generated Rust are all derived from them.

## Stability

This is a frozen contract compiled against in three languages: the Pydantic
models are the source of truth, and the committed JSON Schema, generated
TypeScript, and generated Rust are derived from them. The runner, worker, CLI,
and UI all depend on it, so it never changes from a dependent lane; a needed
change stops the task and lands as its own reviewed change (see the
frozen-interface rule below). The wire contract is **strict producers, tolerant
consumers**: constructing an event with an unknown field is an error, but a
consumer decoding the wire ignores fields it does not model, so a new optional
field is genuinely backward compatible. The version is **semver**-compatible
rather than an exact match -- see **Version policy is semver compatibility,
not exact match** under [Decisions made under ambiguity](#decisions-made-under-ambiguity-section-0-was-underspecified-here)
below for the compatibility rule and why the `version` field is a pattern
string rather than a `const`. Artifact sync is enforced by the
schema-compat gate (`tests/test_schema_compat.py`); an unbumped wire change is
caught by the wire-lock gate (`tests/test_wire_lock.py`).

## Contract surface (v0.4.5)

`PROTOCOL_VERSION = "0.4.5"` is embedded in the schema and in every outbound
event.

**Session setup** (`SessionConfig`, with `to_env()` / `from_env()`):

| Field | Env var | Notes |
|---|---|---|
| `plugin_dir` | `CURIE_PLUGIN_DIR` | mounted plugin bundle at pinned version |
| `session_id` | `CURIE_SESSION_ID` | thread-derived session id |
| `sandbox_id` | `CURIE_SANDBOX_ID` | claimed sandbox id |
| `budget` | `CURIE_BUDGET` | JSON `Budget` object |
| `memory_ref` | `CURIE_MEMORY_REF` | optional; S3 path / API URL |
| `credentials_ref` | `CURIE_CREDENTIALS` | optional; reference to injected secrets |
| `otel` | `OTEL_EXPORTER_OTLP_ENDPOINT` / `_HEADERS` / `_PROTOCOL` | optional |

`Budget` = `{max_output_tokens_per_run: int, task_budget_hint: int | None, max_usd_per_day: float}`.

**Inbound channel messages** (discriminated union on `kind`):

- `Event` = `{kind: "event", type: message|job|eval_case, text, user, ts, session_id?, history_ref?, adoption_credential?}`.
  `session_id` and `history_ref` are nullable strings carrying conversation-scoped
  identity after a sandbox is bound; older producers may omit either field.
  `adoption_credential` is a nullable string carrying a per-conversation runner
  credential on the existing authenticated event (ADR-0122). Omitted or JSON
  `null` is the legacy cold path (no adoption requested). A malformed value is
  a hard decode error with no partial Event. The field is secret material: it
  is omitted from `repr`/`str` and must not appear in errors, model prompts, or
  evidence. It is not a substitute for `text`, `user`, `ts`, or `history_ref`.
- `Interrupt` = `{kind: "interrupt", reason}`

**Outbound NDJSON response events** (discriminated union on `type`, each carries
`version`):

- `text_delta` = `{version, text, adoption_applied?}`
- `tool_note` = `{version, text, tool?, adoption_applied?}`
- `final` = `{version, text, status, adoption_applied?}` where `status` is `SessionStatus`
- `error` = `{version, message, classification?, adoption_applied?}`
- `side_effect_flag` = `{version, tool?, detail?, adoption_applied?}`

`adoption_applied` is the optional ack that the consumer applied
`adoption_credential` for that turn. `null`/omitted is the pre-ack shape. `true`
means the credential was installed. A producer that sent a credential must
require `true`; a successful turn without that ack is not adoption, because a
tolerant 0.4.4 consumer ignores the inbound field.

These fields define delivery and admission on the frozen Event. They do not
retire a bootstrap token, recover a route, or enable warm-pool replicas. That
realizing work stays in a later runner/worker change.

**Producer/consumer rollout.** Inbound `Event` has no `version`, so an old
tolerant consumer can ignore `adoption_credential` and still emit a `final`.
Order: (1) land this contract; (2) ship a consumer that validates, applies only
after a well-formed value, fails malformed frames atomically, redacts the
secret, and emits `adoption_applied=true` when it applied; (3) only then may a
producer send `adoption_credential`, and only on authenticated `POST /v1/event`.
A steer that carries a credential must be rejected by the realizing server
rather than swapping mid-turn. Do not treat HTTP 200 or a `final` as
retirement.

**Session status** (`SessionStatus`): `done`, `idle-awaiting-input`,
`classified-failure`.

**NDJSON helpers**: `to_ndjson_line`, `dump_ndjson`, `parse_ndjson_line`,
`parse_ndjson`, `iter_ndjson`, `parse_inbound`, `to_inbound_json`. The decoder
raises `ProtocolVersionError` for a missing or mismatched `version`.

**Conformance** (`run_conformance`, `reference_producer`): a reusable suite the runner
runs against the real runner. Pass a `Producer` (an inbound-message to NDJSON
function) to validate a real implementation's stream; the library round-trip and
version-rejection checks always run.

## Frozen-interface rule

This package is a **frozen interface**. Do not change it unilaterally from a
dependent lane. A needed change stops the current task and escalates to the
maintainers, who land the change as its own reviewed PR. Any change must:

1. bump `PROTOCOL_VERSION` in `src/aci_protocol/version.py`,
2. regenerate the committed artifacts with `scripts/check-contracts.sh`,
3. commit the regenerated schema and generated types together.

The compat gate enforces this: `tests/test_schema_compat.py` regenerates the
JSON Schema and Rust in-process and fails if the committed copies differ; CI
compiles the generated Rust (`cargo test`) and TypeScript (`tsc --noEmit`).

## Generated artifacts

- `schema/aci-protocol.schema.json` (committed) via `python -m aci_protocol.schema_export`
- `generated/rust/` (committed) via `python -m aci_protocol.rust_export`, a
  standalone serde crate proven by `cargo test`
- `generated/ts/aci-protocol.ts` (committed) via `json-schema-to-typescript`
  from the committed schema, proven by `tsc --noEmit`

## Decisions made under ambiguity (section 0 was underspecified here)

- **Inbound `kind` discriminator.** Section 0 lists `event` and `interrupt` as
  two separate channel operations without a shared tag. To make inbound frames
  self-describing on a single control channel (which the worker and runner need), inbound
  messages are a discriminated union on an added `kind` field. `Event.type`
  keeps its section-0 meaning (`message|job|eval_case`).
- **`credentials_ref` carries a reference, not secret material.** Section 0
  describes `CURIE_CREDENTIALS` as per-tool secrets via K8s Secret refs, so
  the typed contract holds the reference string, not the secrets.
- **`OtelConfig` captures a fixed subset.** `OTEL_EXPORTER_OTLP_*` is a wildcard
  in section 0; the typed view models the three standard fields the prototype
  used (`endpoint`, `headers`, `protocol`). Other OTEL (OpenTelemetry) vars
  pass through the environment untouched.
- **`final` carries `status`; `error` carries `classification`.** The output
  contract lists `done / idle-awaiting-input / classified failure` as ambient
  status. `final.status` carries the terminal session status; a classified
  failure surfaces as an `error` event plus a `final` with
  `status=classified-failure`. Wire tokens use hyphens (`idle-awaiting-input`,
  `classified-failure`) as spelled in section 0.
- **Version policy is semver compatibility, not exact match.** The decoder
  accepts any wire version compatible with `PROTOCOL_VERSION` (same `major.minor`
  under 0.x, same `major` from 1.0 on) via `is_compatible`, and rejects an
  incompatible or malformed one with `ProtocolVersionError` naming both versions.
  The `version` field is a semver-pattern string (not a `const`), so the JSON
  Schema and TypeScript express the range while still requiring the field. See
  `packages/CLAUDE.md` for the change-class table and `docs/adr/0036-*` for the
  rationale.
- **Whole-frame union types are exported.** The committed schema and generated
  TypeScript include `InboundMessage` (`Event | Interrupt`) and `OutboundEvent`
  (the five response events) as discriminated unions, so a consumer can type a
  full channel frame, not only the concrete variants. The generated Rust models
  these as internally-tagged enums whose read path matches Python: extra unknown
  keys are tolerated (no `deny_unknown_fields`, since a consumer ignores fields
  it does not model, while a Rust producer stays strict by construction), and
  the `version` field decodes through `require_compatible_protocol_version`,
  which rejects any value not compatible with `PROTOCOL_VERSION`.

## What consumers need to know

- **The runner:** implement the outbound stream with these exact event shapes
  and run `run_conformance(<your producer>)` in your suite; it must return
  `passed=True`. Read setup from the environment with `SessionConfig.from_env`.
- **The bundle pipeline:** this package does not validate bundles; see
  `plugin-format`.
- **The Rust CLI:** depend on the generated crate at
  `packages/aci-protocol/generated/rust` (or vendor its `lib.rs`); do not
  hand-write the types. The internally-tagged enums decode the NDJSON stream and
  inbound frames directly.
- **The UI (via TS):** import the interfaces and the `OutboundEvent` /
  `InboundMessage` union types from `generated/ts/aci-protocol.ts`.

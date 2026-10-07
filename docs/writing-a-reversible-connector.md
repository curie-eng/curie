# Writing a connector the platform can undo

A tool that changes the world reports what it changed, in its own reply. The
platform records that report and, for a connector that opts in, can later put
the change back by calling the connector's own `restore` verb, with no model in
the path
([ADR-0117](adr/0117-a-tool-that-changes-the-world-reports-what-it-changed.md),
[ADR-0121](adr/0121-a-restore-is-the-connectors-own-verb-run-under-the-same-pinned-connector.md),
[ADR-0124](adr/0124-a-snapshot-is-sealed-to-the-connector-that-wrote-it.md)).

This is the whole author-facing contract. There is no manifest field to set and
nothing to register. Opting in is three things your connector does and one
thing its bundle declares:

1. every write replies with a **sealed** snapshot of what it is about to
   overwrite, the **version** it left, and the **target**;
2. it serves a read-only `observe_version` tool;
3. it serves a `restore` tool that opens the snapshot and writes it back;
4. the bundle gives it the sealing key as a `SecretRef` named
   `SNAPSHOT_SEALING_KEY`.

The worked example throughout is the reference connector,
[`cli/scripts/fixtures/reversible-reference/server.py`](../cli/scripts/fixtures/reversible-reference/server.py):
a `scale` write, `observe_version` and `restore` over streamable HTTP. The
platform side is described in
[ARCHITECTURE.md](../ARCHITECTURE.md#the-action-ledger-and-the-connector-action-executor);
the contract is the
[connector action executor specification](superpowers/specs/2026-10-06-connector-action-executor.md).

## The forward write's reply

A write the platform can undo returns a JSON object (MCP structured content):

```json
{
  "ok": true,
  "summary": "scaled public/api from 3 to 10",
  "prior": {
    "sealed": "curie.snapshot.v1",
    "kid": "seal-2026-10",
    "ciphertext": "<standard base64>"
  },
  "version": "rv-3f0c9a51e2d4b7a8c6e1f0a2",
  "target": {"kind": "Deployment", "namespace": "public", "name": "api"}
}
```

| key | what it is |
| --- | --- |
| `ok` | whether the call did what it says |
| `summary` | one line a human reads on the receipt (optional) |
| `prior` | the state read **immediately before** the write, sealed in an envelope only your connector can open. This is what a restore puts back |
| `version` | an opaque string naming the version of the resource the write **left**. The platform compares it with what `observe_version` reports before any restore |
| `target` | the resource, named well enough for something that is not you to act on it; it is passed back to `observe_version` and `restore` as is |

The envelope has exactly three keys
([`apps/worker/src/curie_worker/sealed_snapshot.py::is_sealed_envelope`](../apps/worker/src/curie_worker/sealed_snapshot.py)):

* `sealed`: the constant `curie.snapshot.v1`;
* `kid`: the key identifier, 1 to 64 characters of `[A-Za-z0-9._-]`. It is not
  secret and must not look like a token: the runner's redaction patterns still
  run over it;
* `ciphertext`: standard base64 (`+` and `/`, padded, no line breaks) of 1 to
  65536 bytes.

`version` is 1 to 256 printable ASCII characters
([`apps/worker/src/curie_worker/sealed_snapshot.py::is_post_version`](../apps/worker/src/curie_worker/sealed_snapshot.py)).
Neither `target` nor `version` may contain the redaction placeholder prefix
`[REDACTED:`. Mint a version that names one write: the reference connector mints
a fresh random string on every write, so a version is never reused.

The platform never seals, opens or inspects the ciphertext. How you seal is
yours; the reference connector uses AES-256-GCM with a fresh 12-byte nonce,
`ciphertext = base64(nonce || ct || tag)`, and the canonical JSON of `target` as
associated data, so a snapshot cannot be replayed onto a different target.

**What makes a record undoable.** The worker records the snapshot
([`apps/worker/src/curie_worker/actions.py::_snapshot`](../apps/worker/src/curie_worker/actions.py))
only when the frame was not redacted, `prior` is a valid envelope, `version` is
valid and `target` is an object without a placeholder. The API then calls the
record undoable only when every other ingredient also holds
([`apps/api/src/curie_api/action_undoable.py::undo_refusal`](../apps/api/src/curie_api/action_undoable.py)):
the call succeeded under an agent, the connector's image digest was recorded,
that digest's capability probe saw the paired verbs, and the agent's in-force
version gives the connector the sealing key as a `SecretRef`. A missing
ingredient is a refusal with its own code, never a guess:

| Code | Missing |
| --- | --- |
| `refused_unsealed` | no valid sealed `prior` (including every cleartext `prior`) |
| `refused_unversioned` | no valid `version` |
| `refused_no_digest` | no recorded connector digest (see [Pinned images](#pinned-images)) |
| `refused_not_restore_capable` | the digest's probe did not record the `restore` and `observe_version` pair |
| `refused_key_custody` | the in-force version does not declare `SNAPSHOT_SEALING_KEY` as a `SecretRef` on this connector |
| `refused_no_agent` | the action ran under no agent |

**A cleartext `prior` is history, not restorable state.** A reply with an
unsealed `prior` (the shape this guide described before the executor) is still
recorded and shown, and the action is not undoable. A `post` key is kept on the
record for a person to read and is no longer required or read for a restore.

**A redacted reply is never restorable.** A valid envelope's `ciphertext`
crosses the runner's redactor untouched. If anything else in `result` is
scrubbed, or a held secret or a token-shaped value appears in `kid`, `target`,
`version` or the decoded ciphertext, the frame is marked `redacted` and nothing
in it is replayed
([`runner/src/curie_runner/redact.py::OutboundRedactor`](../runner/src/curie_runner/redact.py)).
Keep secrets out of the whole reply, not just the snapshot.

## The paired verbs

### `observe_version`

* annotated read-only (`readOnlyHint: true`);
* its input schema requires `target`; the platform calls it with exactly
  `{"target": <recorded target>}`
  ([`apps/worker/src/curie_worker/action_executor.py::observe_arguments`](../apps/worker/src/curie_worker/action_executor.py));
* it returns structured content `{"version": "<the live version>"}`.

A tool error or a reply without a string `version` counts as no version, and
the platform refuses the restore as a conflict; a connector the sandbox cannot
reach refuses it `connector_unreachable`. The executor calls `observe_version`
without a grant, so do not list it in the bundle's approval patterns; the
runner never hides it from the model.

### `restore`

* not annotated read-only;
* its input schema requires `target` and `prior_state`, and may declare an
  optional `expected_version`;
* it opens `prior_state` (the envelope your write returned), writes the prior
  state back to `target`, and returns `{"ok": true, ...}`.

The platform sends the canonical JSON (sorted keys, separators `,` and `:`) of
`{"target": ..., "prior_state": ...}`, adding `expected_version` (the recorded
`version`) only when your `restore` input schema declares that property
([`apps/worker/src/curie_worker/action_executor.py::restore_call`](../apps/worker/src/curie_worker/action_executor.py)).

**Refuse with a code, not an exception.** A refusal is the structured reply
`{"ok": false, "refused": "<code>"}` and writes nothing. Three codes are
understood
([`apps/worker/src/curie_worker/action_executor.py::call_outcome`](../apps/worker/src/curie_worker/action_executor.py)):

| Your code | Recorded as | When |
| --- | --- | --- |
| `version_conflict` | `version_conflict_at_write` | `expected_version` was given and is not the live version |
| `sealing_key_unavailable` | `sealing_key_unavailable` | no key you hold has the envelope's `kid` |
| `snapshot_unopenable` | `snapshot_unopenable` | the envelope is malformed or does not open (wrong target, tampered ciphertext) |

Any other code, a tool error (`isError: true`) or a malformed reply is recorded
as `connector_error`; a success with no structured content as
`unstructured_reply`. Every one of these ends the execution `failed`, which
still blocks a second undo of the same action. The reference `restore` checks
in a fixed order: envelope grammar, a held `kid`, the open, the version, and
only then the write. There is no unsealed fallback.

### Compare-and-swap on `expected_version`

The platform makes the conflict check itself, before it calls `restore`: it
asks `observe_version` for the live version, and the API compares it with the
recorded `version`. Any difference ends the execution `refused` with
`version_conflict`, and `restore` is never called, so a person's later fix is
never silently reverted. Declaring `expected_version` and refusing when it does
not match the live version is defense in depth for the seconds between that
observation and your write. It is never the check: a connector that ignores
`expected_version` is still protected by the platform's comparison. The
platform trusts `observe_version` to report the live version honestly.

### The pair, and a lone `restore`

The pair is the capability rule. A connector is restore capable only when its
probed tool list has both verbs with the shapes above
([`runner/src/curie_runner/executor.py::restore_refusal`](../runner/src/curie_runner/executor.py)).
For a paired connector the runner hides `mcp__<connector>__restore` from the
model
([`runner/src/curie_runner/adapter.py::hidden_restore_tools`](../runner/src/curie_runner/adapter.py)):
only the executor calls it.

**A lone `restore` is unchanged.** A connector that serves `restore` without
`observe_version` keeps it as an ordinary tool: in the model's catalogue, gated
only if the bundle's approval patterns gate it, and never called by the
executor. Its actions are not undoable. The one exception is a runner boot whose
probe of that connector failed: it hides `restore` because it cannot tell a lone
one from a paired one.

### Gate `restore` at the caller proxy

The executor sends `restore` with a one-shot grant bound to the exact argument
text, and the caller proxy spends it. A `restore` outside the connector's
rendered gated set refuses `tool_not_grant_bound` before anything is called.
The gated set is rendered from the bundle's approval patterns
([`apps/api/src/curie_api/bundles.py::approval_tool_patterns`](../apps/api/src/curie_api/bundles.py),
[`apps/api/src/curie_api/bundles.py::gated_tools_for_connector`](../apps/api/src/curie_api/bundles.py)),
so list the paired verb there, for example `"toolPolicy": {"approvalRequired":
["scaler/restore"]}` in `plugin.json`. The model never sees a paired `restore`,
so the entry raises no approval card; it makes the proxy refuse any `restore`
without a grant.

## Sealing and key custody

The key must reach only the hosted connector (ADR-0124 decision 1). The platform
recognizes it by two reserved names
([`packages/curie-internal/src/curie_internal/sealing_key.py`](../packages/curie-internal/src/curie_internal/sealing_key.py)):

* `SNAPSHOT_SEALING_KEY`: the current key; it seals new snapshots and opens;
* `SNAPSHOT_SEALING_KEYS_RETAINED`: retired keys that still open snapshots
  whose records are undoable (ADR-0124 decision 3).

Declare both only as a `SecretRef` on the connector that seals, in
`connectors.yaml`:

```yaml
connectors:
  scaler:
    build:
      context: connectors/scaler
    secrets:
      - name: SNAPSHOT_SEALING_KEY
        from_secret: scaler-sealing
      - name: SNAPSHOT_SEALING_KEYS_RETAINED
        from_secret: scaler-sealing
```

Curie renders a `secretKeyRef` and never reads the value; you provision the
Kubernetes Secret yourself. Every other form of either name is refused at API
intake, by the CLI bundle check, and by the chart
([`apps/api/src/curie_api/bundles.py::sealing_key_custody_issues`](../apps/api/src/curie_api/bundles.py),
[`cli/src/sealing_key.rs`](../cli/src/sealing_key.rs),
`charts/curie/templates/agent-connector-secrets.yaml`): a plain `secrets`
name, an `env` literal, a `secret_files` or `sealed_secrets` entry, a
`plugin.json` `secrets` name, a `bearer_secret`, a `$NAME` or `${NAME}`
reference in `url`, `headers` or `unhosted_url`, and an
`agentSandbox.connectorSecrets` or `agentSandbox.runner.extraEnv` entry. The
worker also withholds both names from every sandbox.

Custody is computed on every read from the agent's in-force version: it holds
only when that version declares `SNAPSHOT_SEALING_KEY` as a `SecretRef` on that
hosted connector. Deploying a version that drops it makes the connector's
actions refuse `refused_key_custody` at once. A key under any other name, or a
`SecretRef` on a `url:` connector, never gives custody. The platform checks the
declaration, not which key your code actually uses.

The value format is your connector's. The reference connector reads
`<kid>:<base64 of 32 bytes>` for the current key and a comma-separated list of
the same form for retained keys, refuses to start on a malformed entry or a
`kid` used twice, and refuses a forward write with `sealing_key_unavailable`
(writing nothing) when it holds no current key.

**Rotation.** Put a new key with a new `kid` in `SNAPSHOT_SEALING_KEY` and move
the old one into `SNAPSHOT_SEALING_KEYS_RETAINED`. Snapshots sealed under the
old `kid` stay restorable while it is retained. Drop it only when you accept
that those records' restores fail `sealing_key_unavailable`.

## Pinned images

A restore runs only against the exact connector image that wrote the snapshot.
The worker records the image digest at the call only for a hosted connector
whose Deployment shows a completed rollout at an `@sha256:` image on both the
opening and the closing frame
([`apps/worker/src/curie_worker/action_digest.py::DigestAttributingRecorder`](../apps/worker/src/curie_worker/action_digest.py)).
A `build:` connector is rendered at its locked digest; an `image:` connector
must be referenced by digest. A `url:` connector, a plugin MCP server, a
tag-referenced image and every call on the local tier record no digest and are
never undoable. After an upgrade to a new digest, undoing an action recorded
under the old one refuses `connector_digest_unavailable`; the old image is never
started to serve it.

## A connector that cannot be undone says so, in prose

```
"restarted public/api; rolling restarts cannot be put back"
```

That is a complete and correct answer. Reversibility is deny-by-default: a reply
without a valid sealed `prior`, `version` and `target` produces a record marked
not undoable, and the receipt shows your sentence. A third-party MCP server
nobody wrote a connector for lands in exactly this case without anyone
declaring anything.

Do not fake a snapshot to look cooperative. A record that claims to be undoable
and holds nothing to restore is the one failure mode this design exists to
prevent.

## Rules that are easy to get wrong

**Read before you write, and refuse if you cannot.** An action that happened and
cannot be undone is worse than one that did not. If the read fails, raise
`ToolError` (the MCP response reports `isError: true`) and do not write.

**A failed forward write carries no snapshot.** Report a refusal or failure of
the forward write as an error, with no `prior` or `version`. The reference
connector's one structured refusal, `{"ok": false, "refused":
"sealing_key_unavailable"}` with nothing written, is the case the executor maps
to that code for a platform-executed forward call.

**Never derive the reversal from the forward arguments.** Undoing
`scale(replicas=10)` needs the count from *before* the call, which no function
of the forward arguments can produce. Snapshot-restore is the only mechanism
that knows the answer.

**Keep snapshots small and free of secrets.** The sealed snapshot is stored in
the control plane, at most 64 KiB. Some resources are honestly not
snapshot-able: report those irreversible rather than sealing credentials to
make an undo appear.

## What the platform does with it

The runner carries your reply on the ACI's side-effect frame; the worker records
one row per call and the image digest it ran at; a capability probe lists your
tools once per digest. An operator asks for an undo
(`curie cluster actions undo <id>`), the API rules on it and creates one
execution, and the worker's executor claims a sandbox under your connector's
own binding, lists your tools, calls `observe_version`, lets the API compare
versions, and only then calls `restore` once, with a one-shot grant. A call
that may have reached you is never repeated. The receipt is
`curie cluster actions execution <id>`. The executor is off unless the install
sets `actionExecutor.enabled`. None of it involves a model, and none of it
involves you beyond the three tools above.

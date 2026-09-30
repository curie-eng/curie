# 180. The turn receipt is an install choice

Date: 2026-09-29

Status: Accepted

Accepted with explicit maintainer approval on 2026-09-29 (Junwon Jung), alongside implementation under ADR 0102.

Tracked in [#3462](https://github.com/curie-eng/curie/issues/3462).

This ADR amends decision 7 of
[ADR-0117](0117-a-tool-that-changes-the-world-reports-what-it-changed.md),
"The user acts on a receipt". Everything else in ADR-0117 stands: the per call
record, the undo rules, and the no-retry signal the kernel reads.

Realizing path, named so the implementation has one:
`curie_worker.config.WorkerConfig.turn_receipt`, read from
`CURIE_TURN_RECEIPT`; the `mode` argument of
`curie_worker.receipt.render_receipt`; the kernel's
`_StreamAccumulator.rendered_with_receipt` in `curie_worker.kernel`, which
passes the configured mode when it assembles the final reply; and the chart
value `worker.turnReceipt` in `charts/curie/values.yaml`, constrained by
`charts/curie/values.schema.json`, rendered into the worker Deployment by
`charts/curie/templates/worker.yaml` and reserved from `worker.extraEnv` by
`charts/curie/files/reserved-env.yaml`.

## Context

ADR-0117 decision 7 has a turn that changed anything end with a receipt. As
built, `render_receipt` appends a `_What I changed:_` block to the final reply,
one line per completed action, and the kernel adds it when it joins the reply
blocks. The grouping of repeated calls and the line cap (#3064) added since
made a busy turn's receipt shorter. They did not change which actions it lists.

The receipt lists every action the side effect classification produced, and
that classification is deny-by-default on purpose: any tool absent from the
read-only allowlist is treated as mutating, so a failed turn that may have
touched something is never retried blindly. Being safe to retry and being worth
telling a person about are different questions, and the receipt answers both
with one list (#3462).

People who use a deployment's agents every day read it as noise. A filing
agent's turn that only staged a file, writing to a staging area and filing
nothing, still ends with a receipt line for the staging call, directly beneath
the sentence that says nothing was filed. When the connector offered no
summary, the line names the raw tool. The experience these people asked for is
that Curie does not print which tools it called in the reply.

One line on the receipt is different in kind. An action whose status is
`failed` renders as `failed — check before retrying`: the call reported
failure, and a failed write is not the same as no write. That line is the one a
person deciding whether to ask again needs, which is why the line cap already
keeps failures first.

How an install's people see replies is the operator's call. The bundle author
writes the agent; the operator decides what the platform adds beneath its
answers for the people that install serves.

## Decision

**The receipt is an install level choice with three modes. The default keeps
today's receipt, and no mode changes what is recorded or what may be retried.**

1. **The install chooses.** The worker reads `CURIE_TURN_RECEIPT` into
   `WorkerConfig.turn_receipt`. It is one of `all`, `failures` or `off`, and
   the default is `all`. Any other value fails the worker at config load,
   naming the variable, rather than falling back to a mode nobody chose. The
   chart exposes it as `worker.turnReceipt` with the same default. The values
   schema admits only the three values, so Helm refuses anything else at
   render, and the name is chart owned, so `worker.extraEnv` cannot set it
   beside the value.

2. **`all` is ADR-0117 as built.** The receipt renders byte for byte as it did
   before this ADR, including the header, the clamp, the grouping, the Bash
   handling and the line cap.

3. **`failures` keeps only the failed actions.** The receipt renders only the
   lines for actions whose status is `failed`, under the same header and with
   the same clamp, grouping and line cap applied to what remains. It is exactly
   the `all` receipt of the failed actions alone. A turn with no failed action
   ends with no receipt.

4. **`off` renders no receipt.** Not on success, and not on failure.

5. **Only the rendering changes.** In every mode the runner still emits a side
   effect frame per call, the kernel still latches `saw_side_effect` on the
   first one and persists the no-retry marker the instant it sees it, and every
   completed call is still one row in the action ledger with its undoability,
   readable through `GET /actions` and undoable under ADR-0117 decisions 2 to
   4. The mode is read where the reply text is assembled and nowhere else.

## Consequences

- An install that sets nothing sees no change.
- An install on `failures` or `off` no longer tells a person in the reply that
  an action can be undone. The undo itself reads the ledger, not the reply, so
  it is still available; the cue to reach for it is not.
- An install on `off` also hides failed actions from the reply. The retry rule
  does not read the reply, so no turn is retried that was not retried before,
  but a person learns of a failed write only from the model's own answer or
  from the ledger. That is the trade an operator makes by choosing `off`.
- The choice is per install, not per bot. An installation hosting several bots
  applies one mode to all of them.
- The mode travels in the worker Deployment's env, so changing it rolls the
  workers. A turn already finishing keeps the mode of the worker running it.
- No contract package changes: the ACI, the plugin format, the reply wire and
  the Rust CLI's mirrors are untouched.

## Alternatives considered

1. **A per-bundle manifest field.** Rejected. It is a bundle contract change
   across the Python plugin format and the Rust CLI's mirror of it, and it puts
   the choice with the bundle author, while the operator, not the bundle
   author, owns how the install's people see replies.
2. **Honoring MCP tool annotations.** Rejected. `readOnlyHint` and
   `openWorldHint` are advisory, a server may declare anything, and neither
   expresses "worth telling a person". A staging write is a real write.
3. **Listing only actions a connector reports as reaching an external system.**
   Rejected. It adds a field to the connector reply contract ADR-0117 decision
   1 defines for every author, and a third-party MCP server would never report
   it, so the receipt would stay as noisy wherever it is noisiest today.
4. **Removing the receipt everywhere.** Rejected. It reverses ADR-0117 for
   installs that rely on it.
5. **Collapsing the receipt to one line.** Rejected. One line still prints tool
   activity, which is what the people asking want gone.

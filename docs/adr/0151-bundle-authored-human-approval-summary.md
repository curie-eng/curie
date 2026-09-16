# 151. A permission-gate card shows a bundle-authored sentence; the machine summary stays the audit record

Date: 2026-09-10

Status: Draft

Implements [#2565](https://github.com/curie-eng/curie/issues/2565).
Does not change [ADR-0035](0035-one-shot-post-approval-allowance.md) or
[ADR-0046](0046-converged-approval-gates-and-durable-provenance.md): the
one-shot grant is still minted from `gate_kind` / `granted_tool`, never from
summary text. `guard_reserved_summary` stays the defense for
model-authored policy summaries.

Realizing path (named so a later Accepted-alongside landing has one):
`plugin_format.models.ApprovalGate.summary`,
`plugin_format.gate_summary.render_gate_summary`,
`curie_runner.approval.ApprovalGate._record_pending`,
`aci_protocol.events.Final.approval_display`,
`curie_worker.kernel.Kernel._pause_for_approval`.

## Context

A permission-gate pause currently shows the runner's machine summary on every
human surface: the Slack card section, the awaiting-approval placeholder
notice, and the resolved card. That string is `Tool call awaiting approval:
<tool> <json>`, truncated at 300 characters of input. For a gate whose
arguments are a map of filenames-to-digests plus a list of cell addresses, the
person whose click is the control sees three digests and `... (truncated)`,
then the same blob again in the notice with a UUID.

ADR-0035 used that prefix as grant provenance. ADR-0046 moved provenance to
the `gate_kind` / `granted_tool` columns, so the summary text is no longer
load-bearing for security. It is only what a person reads. A bundle still has
no lever that names the act for a person: `approvalPolicy.gates[]` carries
`gate`, `route`, and `grantableViaPolicy`. A tool result's `summary` field is
the receipt trailer after execution, not the card before it.

The CLI's `parse_approval_id` needs only the `Awaiting approval (<id>)` head
of the notice (#766, #817). That head can stay while the tail becomes a
sentence.

## Decision

**A permission gate may declare a human summary template, versioned with the
agent. The runner renders it from the blocked call's arguments with a small
safe formatter. The card, the notice, and the resolved card show that
sentence. The durable record keeps the machine string.**

### 1. The template is a bundle field, not a model argument

`ApprovalGate` gains an optional `summary` string. Example:

```json
{
  "gate": "mcp__plugin_acme_files__approve_batch",
  "route": "filing",
  "summary": "File {period}: {expected|count} workbooks into Approved, {going_out_blank|count} cells going out blank. Approve?"
}
```

The template is operator-authored and reviewed with the bundle. It is not
model-authored, so `guard_reserved_summary` does not apply to it. A template
that starts with the reserved permission-gate prefix is a deploy error, so a
bundle cannot park a forged prefix on the display path either.

A gate that omits `summary` is unchanged: the machine string is what people
see, byte for byte.

### 2. One grammar, shared by the validator and the renderer

Placeholders are `{ident}` or `{ident|filter}`. `ident` is a top-level
argument name (`[A-Za-z_][A-Za-z0-9_]*`). Filters are `count` and `length`
(both `len` of a string, list, tuple, or dict). A bare `{ident}` interpolates
only a scalar (`str`, `int`, `float`, `bool`). Nested dumps, attribute
access, and unknown filters are not in the grammar.

`plugin_format.gate_summary` is the single module both the deploy validator
and the runtime renderer call, so a template that validates green cannot
render under different rules (the #453/#544 class). Deploy rejects unmatched
braces, unknown filters, an empty template, an over-long template, and a
template that starts with the reserved prefix. Runtime type or missing-key
failure does not fail the gate: the runner falls back to the machine string
so a bad argument shape cannot strand an approval.

Interpolated scalars are collapsed to one line, length-capped, and escaped
for Slack mrkdwn (`&`, `<`, `>`). Counts and lengths are integers. The model
supplies the arguments; the template is trusted, the values are not.

### 3. Two strings on the wire; one string on the durable row

`summarize_tool_call` still produces the machine string and still owns the
reserved prefix. That string is `Final.approval_summary` and
`Approval.summary`. It is what the `gate_kind IS NULL` rolling-deploy
fallback sniffs, and what an auditor reads.

When a template renders, the runner also stamps optional
`Final.approval_display` (ACI patch bump: a new optional field, ignored by
older consumers). The worker uses `approval_display or approval_summary` for
the Slack card, the placeholder notice, the Valkey card-ref, and the resolved
card. It never writes `approval_display` onto `Approval.summary`. A worker
that predates the field keeps showing the machine string.

The resolved card is not a new layout. `settled_approval_card` already
builds fallback text as `{verdict}\n{summary}`, so once `summary` is the
sentence the first line is `Approved by <@user>` (or `Rejected by <@user>`)
and the section is the sentence. The Block Kit structure stays the #1084
edit/rebuild pair.

The notice stays `Awaiting approval (<id>): <sentence-or-machine>\n...`.
The UUID head is unchanged; the interpolated tail is still collapsed to one
logical line (#817).

### 4. Grants do not read the display

`approval_grant_tool` continues to read `gate_kind` / `granted_tool`, with
the prefix parse only for `gate_kind IS NULL`. Display text is not an input
to that function. A template cannot select a tool, a route, or a grant.

## Consequences

- A bundle that wants a readable card must author a template. The platform
  does not invent a sentence from argument shape.
- An old bundle, an operator-only `CURIE_APPROVAL_REQUIRED_TOOLS` gate, a
  policy-gate `request_approval`, and a publication pause are unchanged.
- Adding `summary` is a backward-compatible plugin-format extension (optional
  field, lenient models). The CLI `ApprovalGateSpec` / `ApprovalGateDecl`
  mirrors must carry it because those structs deny unknown fields.
- Adding `approval_display` is a backward-compatible ACI patch. Older
  workers ignore it and keep today's card text.
- If the Valkey card-ref expires, a rebuild degrades to the durable machine
  string, the same degradation a lost ref already has.

## Alternatives considered

1. Put the human sentence in `Approval.summary` and keep the machine string
   in a new `detail` column. Rejected because the rolling-deploy prefix
   fallback still reads `summary`. Moving the machine string off that column
   would reopen #430 for `gate_kind IS NULL` rows.
2. Store tool arguments on the Approval row and let the worker render.
   Rejected: the record does not currently store arguments, and adding them
   is an audit-surface expansion the card text does not require.
3. Dual-encode both strings in `approval_summary`. Rejected: it changes
   today's bytes for every permission gate and risks the CLI notice parser.
4. Reorder the resolved card so the verdict replaces the "Approval required"
   header. Rejected for the no-template path (byte-for-byte). The existing
   fallback line already puts the verdict first once the section is the
   sentence.
5. Infer a sentence from argument shape when no template is declared.
   Rejected: a guessed sentence is not versioned with the agent and will be
   wrong for any gate whose arguments are not the guessed shape.

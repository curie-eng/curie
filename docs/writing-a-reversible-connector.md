# Writing a connector the platform can undo

A tool that changes the world reports what it changed, in its own reply. The
platform records that report and can replay the recorded prior state to put it
back, with no model in the path
([ADR-0117](adr/0117-a-tool-that-changes-the-world-reports-what-it-changed.md)).

This is the whole author-facing contract. There is no manifest field to set and
nothing to register.

## The reply

A connector that can be undone returns a JSON object:

```json
{
  "ok": true,
  "summary": "scaled public/api from 3 to 10",
  "prior":  {"spec": {"replicas": 3}},
  "post":   {"spec": {"replicas": 10}},
  "target": {"kind": "Deployment", "namespace": "public", "name": "api"}
}
```

| key | what it is |
| --- | --- |
| `ok` | whether the call did what it says |
| `summary` | one line a human reads on the receipt |
| `prior` | the state read **immediately before** the write. This is what a restore puts back |
| `post` | the state the call **left**. This is what a restore is checked against |
| `target` | the resource, named well enough for something that is not you to act on it |

**`prior` and `post` are not interchangeable.** Before restoring, the platform
compares the resource's live state to `post` and refuses on a mismatch, because a
blind restore silently reverts a human's manual fix. Reporting `prior` in both
places would refuse every undo that is safe and permit exactly the one that is
not.

**`target` is read by something that is not your connector.** Name the kind, not
just the address.

## A connector that cannot be undone says so, in prose

```
"restarted public/api; rolling restarts cannot be put back"
```

That is a complete and correct answer. Reversibility is deny-by-default: a reply
that carries no `prior` produces a record marked not-undoable, and the receipt
shows your sentence where an undo control would have been. A third-party MCP
server nobody wrote a connector for lands in exactly this case without anyone
declaring anything.

Do not fake a snapshot to look cooperative. A record that claims to be undoable
and holds nothing to restore is the one failure mode this design exists to
prevent.

## Four rules that are easy to get wrong

**Read before you write, and refuse if you cannot.** An action that happened and
cannot be undone is worse than one that did not: it leaves the platform holding a
record it cannot act on. If the read fails, raise `ToolError` and do not
write; the MCP response must report the refusal with `isError: true`.

**A refusal is an error, not a snapshot.** Raise `ToolError` on every refusal or
failure path so the MCP response reports `isError: true`; do not return
`ok: false` JSON or include `prior` or `post`. A successful reversible action
returns the structured JSON above, including `prior`, `post`, and `target`.

**Never derive the reversal from the forward arguments.** Undoing
`scale_deployment(replicas=10)` needs the count from *before* the call, which no
function of the forward arguments can produce. Snapshot-restore is not the safer
mechanism here, it is the only one that knows the answer.

**Do not snapshot secrets.** A snapshot of a Kubernetes object can carry
environment variables and secret references, and it will be stored in the control
plane. Some resources are honestly not snapshot-able: report those irreversible
rather than storing credentials to make an undo button appear.

## Worked shape

A reversible scale action first reads the workload's current replica count,
writes through the Kubernetes `scale` subresource, and returns the prior count
as its restore payload. An irreversible action returns no restore payload and
says so explicitly. Keep the scale credential limited to the scale subresource;
that makes Kubernetes refuse an image change instead of relying on connector
code to decline it.

## What the platform does with it

The runner carries your reply on the ACI's side-effect frame; the worker records
one row per call; the API rules on an undo and refuses when the world has moved
or when the actor could not have permitted the forward change. None of that
involves a model, and none of it involves you beyond the reply above.

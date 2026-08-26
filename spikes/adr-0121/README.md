# Spike: where the undo executor lives, and whether a sealed snapshot composes

Probes for the two open drafts. **Not code to land** — the connector here is a
copy with an in-memory world, and nothing wires into the platform.

Run against a local stack (`curie local up --build --minimal`) with the spike
connector started from `connector/`.

## What it settled

**1. ADR-0117's spike conclusion was wrong, and ADR-0121 inherited it.**

> "the runner never invokes an MCP tool outside the model loop... a second MCP
> client inside the runner, which is a large new mechanism for one job"

A hosted connector's MCP entry is `{"type": "http", "url": ".../mcp"}` —
streamable HTTP, not a stdio subprocess the SDK spawned. `probe_transport.py`
calls the real `k8s-scale` connector with no agent SDK in the process. The client
is **13 lines**.

So the runner-side executor is not the expensive option. The argument between
"runner in a sandbox" and "client in the worker" is now purely about reachability
and policy envelope, which is a better argument than the cost one.

**2. The full loop works, and the platform never interprets the snapshot.**

`probe_roundtrip.py`, real API over HTTP against real Postgres:

```
world 3 -> agent scales to 10 -> ledger records prior={"spec":{"replicas":3}}
refused undo (world moved)     -> 409, and the connector is never called: 10 -> 10
authorized undo                -> API returns {target, prior_state}
executor replays               -> connector: "restored public/api from 10 to 3"
world 3
```

The executor hands the connector back its own words. No mapping table exists
anywhere, which is ADR-0121's central claim and the reason it is worth having.

**3. Confirmation is the piece that does not exist.** The probe's last step
`POST /actions/{id}/confirm-undo` 404s. Until it exists, `undone_at` is stamped
at ruling time and a restore that never runs leaves a record saying it did.

**4. ADR-0121 and ADR-0122 do not compose as written.**

`probe_sealing.py`: sealing is `crypto_box_seal`, so it needs only the public
half — the worker can seal to itself. But opening needs the private key, and:

| | holds the sealing key |
| --- | --- |
| worker | yes (`CURIE_SEALING_PRIVATE_KEY`) — can open, and can seal |
| runner | **no**, by design: it is the sandbox |
| api | no |

ADR-0121 puts the executor in the runner. ADR-0122 seals `prior_state` to the
cluster key. **A sealed snapshot the runner cannot open is a restore the runner
cannot perform.** One of the two has to give:

- the executor moves to the worker (ADR-0121 rejects this on reachability), or
- the worker opens and hands plaintext to the runner (which spends the seal on
  the hop it was protecting), or
- the sandbox gets a key (which is what the sandbox exists not to have), or
- `prior_state` is not sealed, and its exposure is answered another way.

Not a thing to resolve in an implementation PR.

## What I concluded, and why it is not one of the four

The four ways out all argue about **which platform component holds the key**.
That is the wrong question. Exactly one party ever needs to read a snapshot: the
connector that produced it. Not the API, not the worker, not the runner.

So: **the connector seals to its own key.** `probe_sealed_to_connector.py` and
`probe_sealed_roundtrip.py`, run against the real API and real Postgres:

```
prior_sealed : oXQXOYtoi8OkdJ8px94XTl8s9qWWbMqaKnCHU+bn...
platform can read the replica count from it: False
ledger row   : undoable=True, and prior_state is opaque
conflict     : left=v2 observed=v3 -> HTTP 409, refused
authorized   : HTTP 200; the API hands back a blob it never read
connector    : "restored public/api from 10 to 3"
```

Why this is better than any of the four:

- **The tension disappears rather than being traded.** The runner needs no key
  because it never opens anything. ADR-0121 keeps its executor and its
  reachability argument intact.
- **It is a stronger property than ADR-0122 proposed.** Sealed to the cluster,
  the platform holds the key and the operator has to evaluate that. Sealed to the
  connector, the platform *cannot* read it — there is nothing left to evaluate.
- **It follows ADR-0117 decision 1 to its conclusion.** The tool's reply is the
  declaration; now the tool also decides what the platform may read.

### The one thing it breaks, and the fix that is better than what it replaces

A sealed box is non-deterministic, so ciphertexts cannot be compared and
ADR-0117's conflict check (live state vs `post_state`) stops working.

The comparison moves onto a **version token** the connector reports —
`resourceVersion`, ETag, generation. Opaque to the platform, compared by
equality, and a better question than state equality anyway: it catches a change
that reverted to the same value, and it does not false-positive on
`managedFields` churn.

The cost is honest and belongs in the ADR: a refusal can no longer name both
states, so "10 vs 7" becomes "v2 vs v3". A system with no version token has no
conflict check and its actions are not undoable — which is the same
deny-by-default the rest of this design already runs on.

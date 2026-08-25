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

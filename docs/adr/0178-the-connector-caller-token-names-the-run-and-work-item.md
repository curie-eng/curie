# 178. The connector caller token names the run and work item

Date: 2026-09-27

Status: Draft

This ADR extends [ADR-0168](0168-one-installation-hosts-several-bot-identities.md)
decision 7. It does not supersede it: admission by `admits:` stays exactly as
decided there, and this ADR adds what a connector that is already admitting a
caller may learn about it. Because nothing in ADR-0168 is overtaken, ADR-0168
gains no back link under
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).
It serves [ADR-0176](0176-a-factory-run-may-test-end-to-end-in-a-namespace-it-owns-on-a-separate-cluster.md)
decisions 3 and 4.

## Context

ADR-0168 decision 7 gave every hosted connector an admission list. #3273 has
the worker sign a caller token per sandbox boot in
`apps/worker/src/curie_worker/caller_token.py`, frozen in
`tests/vectors/connector-caller-token.json`. The token is
`cct.<payload>.<signature>`, an Ed25519 signature over the compact JSON
`{agent, exp}`. The runner presents it in `X-Curie-Caller`. #3308 (open), in
the same release, puts a caller proxy in front of each hosted connector. The proxy
checks the signature, the expiry and the agent claim against the rendered
`admits:` list, strips `X-Curie-Caller`, and forwards the request unchanged.

That answers which agent is calling. It does not answer which run. ADR-0176
gives a factory run a namespace of its own on a test cluster, labelled with
the run and work item ids, and issue #3244 requires `env_destroy` to delete
the run's own namespace and refuse any other. One factory agent runs many work
items, often at once, so every one of its runs passes the agent check. The only
place a run id could come from today is a tool argument, and a tool argument is
whatever the model wrote. A run told, or confused into, `env_destroy` on
another run's namespace would supply that run's id and succeed.

A run identity is only enforceable if the connector reads it from something the
model cannot write. The caller token is already that thing: the worker signs it
outside the sandbox, and nothing in the sandbox holds a key that can mint.

In the worker, a factory work item is executed under an execution request
(`WorkItemRunning.request_id`) for a work item (`WorkItemRunning.work_item_id`).
The request is the run: a new attempt at the same work item is a new request,
and every turn of the attempt, including CI continuation turns
(`parse_work_item_event_id`), carries the same request id. Each request gets
its own sandbox claim (`runtime_claim_name`), so a sandbox boot belongs to at
most one run.

## Decision

The caller token gains two optional signed claims, `run` and `work_item`. The
caller proxy verifies them with the rest of the token and hands them to the
connector in headers only it can set. A connector that owns per run resources
scopes them by the verified `run` claim and never by a tool argument.

### 1. Claim names and semantics

- `run` is the execution request id of the work item attempt the sandbox was
  booted for, as a lowercase hyphenated UUID string. It is the identity a
  connector scopes resources by. Two attempts at one work item are two runs,
  so a retry cannot reach its predecessor's namespace; the platform reaper
  (ADR-0176 decision 4) collects the predecessor's.
- `work_item` is that request's work item id, in the same form. It is
  descriptive: a connector may label resources with it and log it, but it must
  not grant access by it, because it is shared across attempts.
- The two claims are present together or not at all. A token carrying one
  without the other is malformed, and the proxy refuses it.
- `agent` and `exp` keep their current meaning. `agent` remains the only claim
  admission reads.

### 2. Turns with no work item carry neither claim

A sandbox booted for anything other than a work item request (a channel turn,
a `curie cluster message` turn, a scheduled fire, a hook, an eval) gets a token
with no `run` and no `work_item` key. The claims are omitted rather than set
to `null` or an empty string, so such a token is byte for byte what the worker
mints today.

A connector that scopes by run must refuse its run scoped tools to a caller
without a `run` claim, with a named reason. It must not fall back to an
argument, a default namespace, or the agent name. A connector with no per run
resources ignores the absence.

### 3. Lifetime

`exp` stays the absolute expiry the state tokens use, now
`SANDBOX_TOKEN_TTL_SECONDS` after the boot. For a token carrying `run`, `exp`
is the earlier of that and the request's `execution_deadline`, so a run
identity never outlives the run it names. A continuation turn that boots a new
sandbox mints a new token with the same claims. A sandbox is never reused
across runs: if a route would hand a sandbox booted for one request to a turn
of another, or to a turn with no work item, the worker replaces it, using the
same turn replacement #3308 uses for a runner booted without a caller token.

Revocation stays with expiry and the run's terminal state. A connector that
sees a verified `run` whose resources the reaper has already collected treats
the call as it would any call on a missing resource.

### 4. The wire vector is versioned additively

The token keeps the `cct` prefix and the same encoding: compact JSON, sorted
keys, ASCII escapes, base64url without padding, Ed25519 over `cct.<payload>`.
The change is additive, not a new version tag, because every existing token is
still a valid token with the same meaning.

- `tests/vectors/connector-caller-token.json` keeps its three vectors
  unchanged, and gains vectors with `run` and `work_item`, including one whose
  payload differs from another only in `run`, so a verifier that skipped the
  new claim would accept both. Its comment is rewritten to name the payload as
  `{agent, exp}` with optional `{run, work_item}`, and its reader keeps
  rejecting unknown keys.
- A refusal vector for a token carrying only one of the pair is added to
  `tests/vectors/connector-caller-refusal.json` from #3308.
- The proxy accepts exactly the keys `agent`, `exp`, `run` and `work_item`, and
  refuses any other key. A later claim is a later ADR, and the proxy is changed
  before any worker mints it.

Order of rollout: the proxy that understands the new claims ships in a tagged
release before any worker mints them, because a proxy from before it refuses a
token carrying a key it does not know. The token itself did not need that
order: #3273 and #3308 ship in one release, and a runner booted without a token
is refused only until its thread's next turn replaces it. A worker rolled back
past the minting change mints tokens without the
claims, and run scoped tools refuse them by decision 2 rather than acting on
the wrong run.

### 5. The proxy hands the verified claims to the connector

#3308 strips `X-Curie-Caller` before forwarding and adds no headers. The token
stays stripped, because a third party connector holding it could replay it
against another connector as the agent until `exp`. Instead the proxy:

- removes every inbound `X-Curie-Agent`, `X-Curie-Run` and `X-Curie-Work-Item`
  header, whatever the caller sent;
- after verification, sets `X-Curie-Agent` from `agent`, and sets `X-Curie-Run`
  and `X-Curie-Work-Item` only when the token carries them;
- changes nothing else about the forwarded request.

This amends the "no added headers" behavior of #3308, which is recorded on
#3107, not in an ADR. Connectors that ignore the headers behave exactly as
they do behind #3308.

### 6. How a connector scopes resources by run

A connector trusts the identity headers only on a request forwarded by its own
proxy, which is a loopback connection inside the connector pod. Requests on the
`<connector object>-direct` Service arrive from outside the pod, come from
callers that are not agents (ADR-0168 decision 7), and carry no run: a
connector refuses its run scoped tools on them.

Scoping follows ADR-0176 decisions 3 and 4:

- On create, the connector derives the resource name and its labels from the
  verified `run` and `work_item`, never from arguments. The run label is the
  ownership record.
- On every later call (read, deploy into, destroy), the connector looks the
  resource up and refuses unless its run label equals the verified `run`. A
  name the model supplies selects among the run's own resources at most; it
  never widens the set.
- A refusal names its reason and the resource, and does not reveal whether a
  resource of another run exists under that name.

## Consequences

- #3244's `env_destroy` refusal becomes enforceable, and so does every other
  `env_*` tool, without trusting the model for anything but which of its own
  resources it means.
- Stock installs and connectors that do not scope by run see no change: tokens
  for non work item turns are byte identical, and the new headers are ignored.
- The proxy grows a small, fixed responsibility: stripping and setting three
  headers. A connector's trust in them rests on #3308's NetworkPolicies making
  the proxy port the only way in from a sandbox.
- Every work item turn now boots a sandbox belonging to one run. Any future
  sharing of sandboxes across runs has to revisit decision 3.
- A connector that forgets decision 6's loopback check trusts headers forgeable
  by a direct caller. The connector-host interface document states the rule,
  and the first run scoped connector's tests pin it.

## Alternatives considered

1. **Run and work item as tool arguments.** Rejected: the model writes them,
   which is the problem in Context.
2. **A second token for run identity.** Rejected: a second signing key, header
   and verifier for one fact about the same sandbox boot, and a connector would
   have to check that both name the same agent.
3. **Forward the raw token and let the connector verify it.** Rejected: the
   connector could replay it against every other connector admitting the
   agent until `exp`. The proxy already verifies, so the connector needs the
   claims, not the credential.
4. **Superseding ADR-0168 decision 7.** Rejected: nothing in it is overtaken.
   Admission still reads `agent` against `admits:` alone.
5. **`null` claims on turns with no work item.** Rejected: it changes every
   existing token's bytes for no information, and invites a verifier that
   treats `null` as a wildcard.
6. **A new prefix or version field.** Rejected: the change is additive, and a
   prefix bump would make every token in flight at upgrade invalid.
7. **Scope by `work_item` instead of `run`.** Rejected: attempts share a work
   item, so a retry could act on a crashed attempt's namespace while the reaper
   is collecting it.

## Tracking

The realizing work is #3244, the end to end connector, whose `env_*` tools are
the first run scoped consumer. The claim minting, the vector additions, the
proxy header change and the connector-host interface update land with or
before it, after #3308 ships, and only once this ADR is Accepted.

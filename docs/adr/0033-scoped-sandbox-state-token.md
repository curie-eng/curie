# 33. Scoped, least-privilege sandbox state token

Date: 2026-07-15

Status: Accepted

Implements [#410](https://github.com/curie-eng/curie/issues/410).

## Context

The worker forwards the one shared platform API key into every sandbox so the
runner can rehydrate the agent's memory and transcript at boot.
`binding.boot_env()` sets `CURIE_MEMORY_TOKEN` and `CURIE_HISTORY_TOKEN` to
`self._config.api_key` (`apps/worker/src/curie_worker/binding.py`), and the
runner sends that value verbatim as the `X-API-Key` header on its calls to the
durable state store (`runner/src/curie_runner/memory.py`,
`runner/src/curie_runner/history.py`). The state store routes
(`/agents/{id}/state/...`) and every other authenticated route, including
`/approvals/{id}/resolve`, are guarded by the same `require_api_key` dependency
(`apps/api/src/curie_api/auth.py`), which compares the header against the
single `Settings.api_key` with `hmac.compare_digest`.

The composition of these separately merged decisions is a HIGH-severity hole.
A sandboxed agent can read `CURIE_HISTORY_TOKEN` (the platform key) from its
own environment and use it to:

1. Resolve its own gated approval by POSTing `/approvals/{id}/resolve` with a
   fabricated non-author `resolved_by` and an `actor_channel` equal to the card
   channel. The `ChannelMembershipAuthorizer` trusts caller-asserted identity,
   so the sandbox self-approves the very tool call the ADR-0010 permission gate
   was meant to block.
2. Read and write any other agent's memory and transcript namespace (cross-agent
   isolation break).
3. Resolve any pending approval platform-wide.

The egress allowlist does not mitigate this: the sandbox must reach the API host
for memory and transcript rehydration to work at all, so the credential it
carries is exactly the credential that unlocks approvals. This directly
contradicts ADR-0010 and the approval INTERFACE guarantee that resolution is
"enforced server-side at resolution time, not inside the sandbox or runner"
(`docs/interfaces/approval/INTERFACE.md`).

The root problem is credential scope: the sandbox needs a token that can touch
only its own state namespace and nothing else, yet it is handed a
platform-wide, resolve-capable key. This ADR records the decision to replace
that credential with a scoped, least-privilege token. It supersedes nothing; it
references and restores ADR-0010's server-side-enforcement guarantee.

## Decision

Mint a **scoped sandbox token** per turn and forward it in place of the raw
platform key. The token grants access to exactly one agent's state namespace and
nothing else.

**Signing.** The token is HMAC-SHA256 signed using the existing shared
`api_key` as the signing key. No new shared secret and no new config plumbing is
introduced: the worker and the API already both carry `api_key`
(`WorkerConfig.api_key` / `Settings.api_key`), and the API's production boot gate
already refuses the dev-default value (`test_config_prod_gate.py`), so the HMAC
key inherits that protection for free.

**Payload claims.** A compact, self-describing format
`sbx.<b64url(json payload)>.<b64url(hmac)>` with claims:

- `agent`: the agent UUID the token is scoped to.
- `scope`: the permitted capability, `"state"` for the memory/transcript token.
- `exp`: expiry as unix seconds.

**Minting (worker).** `binding.boot_env()` mints a token for
`resolved.agent_id` with scope `state` and a fixed TTL, signing with
`self._config.api_key`, and forwards that token as both `CURIE_MEMORY_TOKEN`
and `CURIE_HISTORY_TOKEN` instead of the raw key. Because `boot_env` is
recomputed on every claim through the kernel consume path
(`kernel.py` -> `boot_env` -> `substrate.claim/resume(env=...)`), a fresh token
is minted every turn, including on resume. A generous fixed TTL (24 hours)
safely covers a multi-day approval pause, since a paused thread is a cold
rehydrate on the next mention, not a live process holding a stale token.

**Verification (API).** The `state` router
(`apps/api/src/curie_api/routers/state.py`) gets a new auth dependency that
accepts **either**:

- the platform key (operators, CLI, and the worker keep full access, so there is
  zero regression for existing callers), **or**
- a valid scoped token whose signature verifies against `api_key`, whose `agent`
  claim equals the path `{agent_id}`, whose `scope` equals `state`, and whose
  `exp` is in the future.

Every other router keeps `require_api_key` unchanged (platform key only): a
scoped token is rejected at `/approvals/{id}/resolve`, `/agents` CRUD, memory
management routes, and everywhere else.

**Runner.** Unchanged. It already sends whatever token it is given verbatim as
`X-API-Key` and does not care whether that value is the platform key or a scoped
token.

**Security properties (must hold).**

- The scoped token is a signed derivative of `api_key`, never the key itself,
  and the API (which already holds the key) is the only verifier, so there is no
  online guessing oracle. The one residual cost of reusing `api_key` as the HMAC
  key instead of a dedicated random secret: a token is an offline verification
  oracle, so an attacker holding a token could brute-force a low-entropy
  `api_key` offline. This is acceptable only because `api_key` is high-entropy
  machine-generated (chart secret generation, #195) and the production boot gate
  refuses the dev-default value; an operator who hand-sets a memorable `api_key`
  would void this property.
- Presenting the scoped token to `require_api_key` fails `compare_digest`
  because the token string is not equal to `api_key`, so the sandbox cannot
  resolve approvals or reach any platform-key-only route.
- Verification binds the token's `agent` claim to the request path
  `{agent_id}`, so a token minted for agent A cannot read or write agent B's
  namespace: cross-agent isolation is restored.
- `exp` bounds the exposure window of a leaked token.

## Alternatives considered and rejected

- **Keep the shared platform key in the sandbox (status quo).** Rejected: this
  is the vulnerability. The sandbox holds a resolve-capable, platform-wide
  credential, defeating ADR-0010 and the approval INTERFACE guarantee.

- **Mint an opaque, DB-registered token.** Issue a random token and persist an
  (agent, scope, expiry) row the API looks up on each request. Rejected: it
  needs new shared mutable state and a lookup on the hot memory/transcript path,
  plus issuance, revocation, and cleanup machinery. A signed derivative carries
  its own claims and needs no storage, no lookup, and no new table for the same
  security outcome.

- **Introduce a brand-new dedicated signing secret.** A separate
  `sandbox_token_secret` distinct from `api_key`. Rejected: it requires config
  plumbing across the worker, the API, the Helm chart, and compose, plus a new
  production-gate entry, for no security gain. Reusing `api_key` as the HMAC key
  is sound because the token is a non-invertible derivative, the worker and API
  already both hold `api_key`, and the prod boot gate already protects it.

- **A shared package for the token module.** Place mint and verify in one
  internal library imported by both apps. Rejected/deferred: the worker and API
  are separate uv-workspace members with no shared internal lib between them
  (worker depends on channel-protocol and the dispatcher; the frozen contract
  packages aci-protocol and channel-protocol are off-theme for auth crypto), so
  a shared package means new cross-app coupling against a deliberately decoupled
  boundary. Instead the ~40-line pure-stdlib module (`hmac`/`base64`/`json`) is
  duplicated in both apps, and a committed known-answer test vector (a hardcoded
  key and payload producing an exact token string) is asserted by both test
  suites so the two copies mechanically cannot drift. See Consequences for the
  tradeoff.

## Consequences

- The sandbox can no longer resolve approvals or reach any agent's namespace but
  its own. ADR-0010's server-side-enforcement guarantee holds again: a
  model-initiated approval resolution is rejected at the API because the sandbox
  does not hold a resolve-capable credential.

- The token module is duplicated in `apps/worker` and `apps/api`. This is a
  deliberate tradeoff: duplication of ~40 lines of pure-stdlib code, fenced by a
  committed golden test vector asserted in both suites, is preferred over new
  cross-app coupling against the frozen-contract boundary. If a shared internal
  library is introduced later for another reason, the module should move into it
  and the duplication removed.

- No new configuration surface. The HMAC key is the existing `api_key`, already
  present in both services and already covered by the production boot gate. The
  chart and compose need no change.

- The token carries a fixed 24-hour TTL. This is not per-turn-tight, but the
  rehydration path is a cold boot per claim, so a fresh token is minted every
  turn regardless; the TTL only bounds a leaked-token window and is generous
  enough that a multi-day approval pause rehydrates without a special branch.

- Operators, the CLI, and the worker's own control-plane calls are unaffected:
  the platform key still authenticates everywhere, including the `state` router,
  so this is a strict capability reduction for the sandbox only, with zero
  regression for trusted callers.

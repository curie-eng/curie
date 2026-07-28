# 83. Console sessions and CLI-minted login codes

Date: 2026-07-28

Status: Accepted

Implements [#630](https://github.com/curie-eng/curie/issues/630).

## Context

The console sends the shared platform administrator key from browser JavaScript.
`apps/ui/src/api/config.ts` resolves it from `?api_key=`, else `VITE_API_KEY`,
else a published dev default. In a sealed cluster the platform key is randomized,
so the console either fails to authenticate or requires the administrator key in
a URL.

That key authorizes deployments, approvals, logs, traces, budgets and the kill
switch. Putting it in a URL puts it in browser history, request logs, referrers
and any plaintext NodePort traffic, and the only way to revoke it is rotating the
Secret and restarting the API, which breaks the worker and runner at the same
time. #630 is a release blocker for that reason.

**Prior attempt.** PR #645 (2026-07-17) built this and was closed unmerged on
2026-07-27 with no stated reason, in a `CONFLICTING` state 432 commits behind
main, a span that includes the repository-wide AgentOS to Curie rename. Its
migration and ADR numbers now both collide with revisions that landed since. Its
design was reviewed against this repository's constraints and found sound, so
this ADR adopts that design rather than re-deriving it, and the implementation is
written against current main rather than rebased. That is the same "mined, not
reused" call ADR-0060 Decision 6 made for the withdrawn OpenCode chain, and for
the same reason: the reasoning is the durable part, the diff is not.

## Decision

The console authenticates with a **server-managed, revocable session cookie**,
established by exchanging a **single-use login code minted by the CLI**. The
browser never receives the platform key on any path.

**One session store.** A `console_sessions` table holds a row per session: the
SHA-256 of its login code, the SHA-256 of its session token, both expiries, and
`consumed_at` / `revoked_at`. Only hashes are stored, so a database read cannot
replay a session. Revocation is a column write, which is what makes the session
revocable in the sense #630 requires: a durable row a human can kill, not a
self-contained signed token that stays valid until it expires.

**Minting is CLI-side and never handles the key by hand.** `curie <local|cluster>
console login` calls `POST /console/login-codes` under the platform key and
prints a short-lived single-use code. At cluster tier it sources the key through
the existing `ops::discover_api_key`, which reads the release Secret and flows the
value straight into the `X-API-Key` header without printing it. The operator
copies a code, never the key.

**Exchange sets the cookie.** The console posts the code to `POST
/console/session`, an unauthenticated endpoint that consumes the code, mints a
session token, and returns it as a cookie: `HttpOnly`, `Secure`,
`SameSite=Strict`, `Path=/`. `HttpOnly` is what makes this strictly stronger than
the status quo: script on the page cannot exfiltrate the credential it
authenticates with.

**One shared dependency still gates every router.** `require_api_key` accepts the
platform key **or** a live console session, in that order, and stays the single
dependency every router depends on. The platform-key path is unchanged, hits no
database, and needs no new header, so the worker, runner and CLI are untouched.
This extends the shared dependency rather than adding a second auth scheme to a
router, which is the boundary `apps/api/CLAUDE.md` draws and the reason that file
asks for an issue or PR first.

The order is load-bearing beyond precedence: a machine caller returns before the
session store is read, so a database outage cannot take the platform-key path
down with it.

**`cluster status` stops printing a secret-bearing URL.** The console URL it
reports carries no key, which is #630's fourth acceptance criterion.

## Consequences

- **A new durable table and one Alembic revision.** Sessions are state the API
  now owns; expiry and revocation are columns, not inference.
- **The console gains a login step.** An operator runs one CLI command and pastes
  a code. That is the cost of the browser never holding the key.
- **`require_api_key` grows a second accepted credential.** It stays one
  dependency, but it is no longer a pure header compare, and the platform-key
  path must keep its no-database property or an outage becomes a total outage.
- **Leakage is testable, so it is tested.** #630 requires proof that browser
  history, request logs, referrers and static assets carry no platform key; that
  is an assertion over the built bundle and the e2e traffic, not a claim.
- **Tenant-scoped principals stay #151's decision.** This ADR authenticates one
  operator against one deployment and deliberately does not model identity.

## Alternatives considered

Carried forward from PR #645's analysis, which this ADR adopts.

1. **A password form that posts the platform key.** The conventional shape, and
   it needs no CLI verb. Rejected: it still hands the raw platform key to browser
   code. Injected script, a browser extension, or a password-manager sync would
   capture a credential that authorizes deployments, approvals and the kill switch
   platform-wide, and it cannot be revoked without rotating the Secret and
   restarting the API. #630's acceptance is explicit that browser code must not
   receive the raw administrator credential, and a password field is browser code
   receiving it.
2. **Inject the key into the UI pod and proxy from nginx.** The UI pod already
   proxies `/api`. Rejected: it authenticates the *pod*, not the operator, so
   anyone who can reach the NodePort is an administrator. That is a worse hole
   than the one being closed, and it is unrevocable.
3. **A signed stateless session token, reusing the ADR-0033 `sandbox_token`
   idiom.** Proven in this codebase and needs no table. Rejected: it is not
   revocable. A stolen token stays valid until expiry, and the only kill switch is
   rotating `api_key`, which breaks the worker and runner at the same time. #630
   requires a revocable session, and revocation wants durable state.
4. **An external identity provider or OAuth.** Rejected as out of scope: it
   imports an identity system for one operator, and tenant-scoped principals are
   #151's decision to make, not this one's to pre-empt.
5. **Rebase PR #645 rather than re-implement.** Rejected on cost and risk: 5,759
   lines across 43 files, cut 432 commits back, over a span containing the
   repository-wide rename, with its migration and ADR numbers both now taken. The
   design is the valuable part and it is preserved here; the diff is not.

# Identity writer connection lifetime v1

This repairs API writer serialization and cache-fence lifetime under Accepted [ADR 0220](../../adr/0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md), its cache-invalidation intent and the existing identity_fence implementation. It changes no authority policy or public wire contract. A protected handler must not continue database writes independently after losing the connection that serialized those writes.

## 1. Scope and preserved behavior

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-1 -->
Keep `identity_fence.protects`, its route/method exclusions, authentication decisions, existing handler commits/rollbacks, response schemas and permission semantics unchanged. Keep `FENCE_KEY = "curie:identity:admission-fence"` and advisory key `2250220`. Add no table, migration, release version, configuration setting, new writer route, alternate identity store, provider call or worker/kernel behavior.

The current protected production writers are:

| Route owner | Mutations |
| --- | --- |
| `apps/api/src/curie_api/routers/agents.py` | Agent POST/PATCH/DELETE; channel POST/PATCH/DELETE; channel caller-list PUT. |
| `apps/api/src/curie_api/routers/agent_access.py` | Existing whole-policy PUT. |
| `apps/api/src/curie_api/routers/directory.py` | Configuration PUT; team, credential, namespace, attempt and account-status POST; snapshot PUT; invalid-snapshot failure recording. |
| `apps/api/src/curie_api/routers/identity.py` | Slack-report POST. |
| `apps/api/src/curie_api/routers/identity_connect.py` | Connection-confirm POST. |
| `apps/api/src/curie_api/routers/channel_identities.py`, `apps/api/src/curie_api/routers/provider_installations.py` | POST/PATCH/DELETE. |

Ordinary writers use SessionDep and pass that session to services/CRUD. The known independent write-session bypass is DirectoryRoute's validation-failure recorder. Repair it under criterion 7. Teams/identity-admin currently provide reads only. Static Slack bootstrap/retry, `/identity/resolve`, `/channels/token`, console/OIDC and unrelated control subresources are outside the existing protected set; do not declare this change their fence or silently change that set.

## 2. Named internal interfaces

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-2 -->
In `curie_api.identity_fence`:

```text
@asynccontextmanager
async def mutation(
    factory: async_sessionmaker[AsyncSession],
    client: Redis,
    *, request: Request | None = None,
) -> AsyncIterator[AsyncSession]

class IdentityFenceMiddleware:
    def __init__(self, app: ASGIApp) -> None
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None

def request_owned_session(request: Request) -> AsyncSession | None

class IdentityWriterUnavailable(RuntimeError):
    def __init__(self) -> None
```

`mutation` yields the real bound AsyncSession that owns protected handler writes. Callers must use that session; an independently created factory session is not made safe by being lexically inside the context. Existing callers that ignore the yield and perform no SQL remain valid. The private owner is not exported or returned. `request_owned_session` is the narrow public lookup used by other API modules: absent marker returns None; live marker returns the bound session; present closed/poisoned/quarantined/unrecognized marker raises IdentityWriterUnavailable. It performs no SQL, connection checkout or transaction mutation. Only identity_fence accesses the private owner/marker; other modules must not import or reach through underscored owner attributes.

`IdentityWriterUnavailable` has exactly the fixed str and single args value `identity_writer_unavailable`; repr contains only its class and that fixed code. Use it for an unavailable/closed/poisoned owner, a nested owner on the same request, guard refusal or a nonexceptional unsuccessful pending/publication acknowledgement. Do not include SQL, identifiers, values or connection details. Unexpected database/client exceptions retain their original exception type; this is not a promise that all storage errors become one domain code. No new HTTP status mapping is introduced.

In `curie_api.deps` add:

```text
@asynccontextmanager
async def request_session(request: Request) -> AsyncIterator[AsyncSession]
```

`get_session` delegates to this helper, and request_session uses the public `identity_fence.request_owned_session` lookup. When the request has a live owner, both borrow exactly its session and do not commit, roll back or close it on borrower exit. A present closed/poisoned owner marker always refuses; it never falls back to the global factory. With no owner marker, preserve the existing independent factory context for unfenced/preparation/test usage. Do not replace `app.state.sessionmaker`, use a ContextVar to redirect arbitrary factory calls, or change get_liveness_session.

When Request is supplied, mutation attaches its private marker to request state before yielding, refuses an existing marker, and leaves a closed marker after exit. This lets a retained Request fail closed after finalization. No request means a direct service caller uses the yielded session without request attachment.

## 3. One physical connection across real handler commits

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-3 -->
Obtain one AsyncConnection from the factory's configured AsyncEngine, pin its original physical driver identity, acquire the exclusive **session-level** advisory lock with `pg_advisory_lock(2250220)`, and commit the lock-acquisition transaction before creating/lending the bound session. Preserve the sessionmaker's ordinary options, including its configured expire-on-commit behavior. Do not leave an enclosing SQL transaction that turns handler commits into savepoint completion.

Bind the yielded/request session to this same AsyncConnection. The connection and session advisory lock survive handler commits, rollbacks, refreshes and borrower Session.close. Session.close may end a session transaction; it must not release the owner's external connection. Existing PATCH-agent helpers may commit multiple times: preserve their real durable effects and earlier committed effects when a later operation fails.

Existing `lock_identity_reader` continues using a shared transaction-level lock on the same key; it must conflict with the owner's exclusive session-level lock throughout the handler lifetime. No scope/data lock is taken before this writer lock. A writer cannot continue on a new connection after the original lock is lost. No successful data write is justified by a scalar lock/health SELECT alone.

## 4. Latched connection loss and guard coverage

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-4 -->
Install connection-local public execution/commit guards and a scoped pool-invalidation listener filtered by the original driver identity. Latch owner poisoning permanently when its physical connection is invalidated or ownership becomes unusable. Invalidating an unrelated connection must not poison this owner. Rollback, a new Session on the same logical connection, or a caught exception cannot clear poison.

Guard `before_execute`, `before_cursor_execute` and `commit`: SQLAlchemy's raw `exec_driver_sql` bypasses before_execute. Refuse before SQL statement/commit execution when poisoned or the current physical driver differs. The contract is **no SQL continuation on a replacement connection**, not zero replacement checkout: raw-driver execution can revalidate/check out before the cursor hook refuses it. Do not use undocumented reconnect flags.

A closed logical connection is unusable. Install a scoped public PoolEvents.checkin listener, filtered to the exact original DBAPI connection identity. Until normal cleanup has positively acknowledged explicit advisory unlock and settled its incidental transaction, return is not allowed. On an early checkin, latch owner loss and invalidate that PoolEntry synchronously inside the callback, before it is available for another checkout, only while `entry.dbapi_connection` is still that exact original object. If ownership no longer matches, do not invalidate the reassigned entry; report owner unavailability. Never save an entry and invalidate it later after another owner may acquire it.

Normal cleanup marks return allowed only after confirmed unlock/transaction settlement; subsequent checkin does not poison or discard that healthy connection. Keep the checkin/invalidation listeners installed until connection disposition is complete, then remove them. Borrowed Session.close is not owner Connection.close: the former retains the external connection/lock, while premature owner close must discard the original physical connection. Independently test both, including a different engine acquiring the released lock and safe subsequent pool reuse.

After confirmed disposition, remove every per-owner listener during final cleanup, without poisoning later requests or leaving references to completed owners. Criterion 6’s unconfirmed-discard quarantine is the explicit exception: retain that owner and the necessary execution/checkin/invalidation guards rather than falsely claiming complete removal. The guarantee covers the yielded session, explicit request borrowing and the repaired known bypass. It does not sandbox deliberate raw driver access, alternate-factory writes or arbitrary detached task creation by trusted application code.

## 5. Pending, settlement and stable publication

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-5 -->
The order is: connection/guards; exclusive lock; settle lock-acquisition transaction; bind session; SET a fresh `pending:<unique>` value and require a successful acknowledgement; attach owner/yield; complete child lifetime; stop further borrowing; settle session work; compare-publish fresh `stable:<unique>`; unlock/discard; close connection/remove listeners after confirmed disposition, or retain terminal quarantine if unconfirmed; leave an unavailable marker.

For the existing Redis client, pending SET must return True; compare-publication must return the script’s successful `OK` string/bytes. A false/None or other unsuccessful result is not acknowledgement. No handler executes before pending acknowledgement. A failed/lost pending acknowledgement starts no handler writes and never triggers a speculative stable publication. Previously sent SET may still execute: do not claim absence from a lost acknowledgement.

Settlement closes/rolls back unfinished handler transaction work without committing it. Already acknowledged handler commits remain durable. An ordinary handler refusal/error on a healthy owner can publish stable after settlement, preserving existing invalidation behavior. A poisoned connection or unknown SQL settlement must not publish stable. If a handler catches a guard failure and returns normally, the latched poison still prevents publication and successful owner completion.

Stable publication retains the existing compare-and-set script: only this request's exact pending value may become its new stable value. A changed/missing token is not overwritten. A false result refuses with IdentityWriterUnavailable. A client exception propagates under criterion 6. A failed acknowledgement may follow a successful server-side publication; it does not prove the key remains pending. Never reset an old token or retry by overwriting a successor.

## 6. Bounded cleanup and truthful error behavior

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-6 -->
Cleanup always attempts session settlement and connection disposition; listener removal is conditional on confirmed safe disposition. Use a five-second cleanup deadline for settlement/publication/unlock, followed, only if needed, by a separate five-second bounded discard/close attempt. These are internal cleanup budgets, not provider freshness or new admission policy. Cancellation must not silently skip cleanup; protect the cleanup attempt while retaining the caller's cancellation outcome.

On normal healthy completion, require successful explicit `pg_advisory_unlock(2250220)`, settle its incidental transaction and close the connection. Failed, false or uncertain unlock requires invalidation/discard, not returning a possibly locked connection to the pool. If discard cannot be confirmed within its budget, enter terminal quarantine: latch poison, disallow borrowing and normal pool return, and strongly retain the unresolved owner/connection and necessary execution, checkin and invalidation guards in private process-local quarantine. Report cleanup failure; do not claim complete listener removal, reusable lease, successful cleanup or eventual recovery. Do not release the uncertain lease by a normal close or let garbage collection supply its ownership policy.

A delayed original checkin remains guarded: invalidate only the exact still-owned original entry synchronously during that callback, before reassignment. Never later invalidate a remembered entry. Unrelated/reassigned entries remain usable. Quarantine remains a terminal failure state for this request, even if a delayed return is safely discarded; no background recovery service, public recovery API, retry loop or successful request resumption is added. This is private failed-resource retention, not another authority store.

If child execution already raised, preserve that primary exception/cancellation; record a secondary cleanup failure using fixed phase/code text, without payloads or raw exception values. Without a primary error, propagate the original unexpected cleanup exception, or IdentityWriterUnavailable for owner/acknowledgement/deadline failure. Cleanup never rolls back an earlier committed handler transaction.

Do not promise to rewrite a response already started. Background/dependency/finalization failure can occur after HTTP 200 was sent; preserve that observed response fact, keep fence state nonauthorizing when settlement is uncertain, and surface the failure to the ASGI caller/logging path. Before response start, normal framework error handling remains responsible; this change adds no success response or new public failure schema.

## 7. Explicit directory failure-recorder participation

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-7 -->
Replace DirectoryRoute's independent `request.app.state.sessionmaker()` failure recorder with `deps.request_session(request)`. On a qualifying RequestValidationError snapshot failure, borrow the owner, roll back any unfinished dependency transaction **before** authenticating/recording failure, and then run the same existing authentication, `directory_sync.record_failure` and commit. This rollback cannot erase earlier committed work; it discards only incomplete dependency work before the endpoint ran. It does not release the owner's session-level lock.

Preserve the existing credential/header/path/epoch qualification, bounded `invalid_snapshot` error, swallowed DirectorySyncError/ValueError/KeyError cases, and final 422 `invalid_directory_request` with existing cache headers. If the owner is poisoned or closed, borrowing/SQL refuses; do not open another connection to save the failure record. Unexpected DB/owner failures continue to propagate rather than pretending the failure audit committed. Add no broader session-restoration abstraction.

## 8. Full-ASGI lifetime and unchanged preparation

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-8 -->
Install IdentityFenceMiddleware on the real API app **inside observe_http**, and remove only the duplicated fence lifecycle from observe_http. The existing observer must wrap preparation and its refusal responses as well as normal dispatch, while IdentityFenceMiddleware wraps the complete child route ASGI lifetime. Preserve observability and the old protects predicate. Real-app timeout/auth-refusal tests must show the actual 408/401 response and existing request/active/duration metrics plus HTTP span/status observation; moving preparation outside telemetry is not equivalent. Non-HTTP and unprotected scopes pass through unchanged.

For protected requests call existing `prepare` before connection/lock acquisition; a False result retains the existing unfenced dispatch path. Its five-second body deadline, 16 MiB bound, authentication, cookie-origin checks, and existing TimeoutError/HTTPException translation remain unchanged: timeout is 408 with `request body timeout`; refusal detail/status and private/no-store plus no-referrer headers remain the existing values. Preparation's authentication sessions remain independent and closed before the writer starts. Existing route authentication rechecks remain unchanged; do not add a cached authentication bypass.

Replay the exact already-consumed bounded bytes to the child once, preserving normal later receive/disconnect behavior. Do not parse/re-encode JSON, change form bodies, duplicate bytes, consume unprotected bodies, or broaden exemptions. Request state must be shared with dependencies/route wrappers.

The writer context encloses the entire awaited child ASGI application: response send, awaited response background work and request dependency finalizers, not merely call_next returning response headers. Close the owner only afterward, also on errors and cancellation. A normal borrowed get_session finalizer does not close the owner early. No detached task gains a lease on a completed owner.

## 9. Measured substrate and engineering limits

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-9 -->
The reviewed baseline pins SQLAlchemy 2.0.52, asyncpg 0.32.0, FastAPI 0.150.0 and Starlette 1.6.0. A synthetic real-PostgreSQL measurement using public events observed original driver/session-lock retention across commits, rollback and externally bound Session.close; filtered invalidation poisoning; refused execute/raw-driver/commit continuation; listener removal and later pool reuse. A separate checkin measurement found that the exact original DBAPI object still belonged to its PoolEntry during premature Connection.close; synchronous entry invalidation released its lock, a different engine obtained a shared lock, and a fresh checkout used a healthy replacement. Normal explicit unlock followed by transaction settlement and allowed return reused the same original driver with no invalidation. Later unrelated/reassigned checkins were not invalidated. Raw-driver revalidation before the cursor guard remains possible.

A minimal real FastAPI ASGI measurement observed: body preparation; owner entry/replay; request and function dependency entry; endpoint; function dependency exit; response start/body; background execution; request dependency exit; owner exit. Background error, request-finalizer error and cancellation retained request-finalizer-before-owner-exit ordering, including after an already-sent 200.

These observations justify the chosen mechanisms, not claims about complete product wiring, all network failures, healthy unlock failure handling, streamed/disconnected clients or every pool/driver lifecycle. The measured premature-close callback still requires integrated product tests and must never invalidate a reassigned entry. The independent tests below must establish those product outcomes before completion. A contrary observation blocks the affected claim; do not weaken assertions or silently expand into a generic coordinator.

## 10. Independent regression and acceptance evidence

<!-- @spec IDENTITY-WRITER-LIFETIME-V1-10 -->
Use actual disposable PostgreSQL and Valkey with synthetic records. Fault seams are client-local acknowledgement loss, controlled awaited barriers, and closing/invalidation of the test's exactly observed owned client session/connection. No backend/PID termination, ACL changes, server-wide fault injection or mocked datastore/lock substitutes.

The API test harness must partition fence authority consistently with its
independent pytest SQL authorities. Allocate one test key per run/worker at
session scope, then install and restore the API writer and dispatcher reader
constants at function scope for API tests. Ordinary API modules receive this
fixture without opt-in imports. Production/default wire constants remain
unchanged; restoration must precede unrelated dispatcher default-key tests.
Namespace allocation/installation performs no I/O, endpoint/stream change,
settings mutation or new backend prerequisite.

Test-only helpers `fence_key_for_worker`, `installed_fence_key`, `fence_child_env`
and `create_namespaced_app` own this namespace and explicit child propagation.
A dedicated namespaced child returns actual production create_app; existing
`python -m curie_api` entrypoint/logging tests retain their original launch.
The existing factory-based child explicitly receives the same key. Do not add
a production test setting or alternate application. Real backend owners clean
only their exact recorded test keys after all consumers join; do not delete a
shared authority key merely because one inner client context ends.

Independent harness proof must demonstrate the old two-process/two-SQL-authority
shared-cache collision, isolated authorities both committing/publishing, and
same-authority serialization. Also prove ordinary fixture discovery, paired
reader/writer restoration, default unmodified imports and actual parent/child
agreement. Verify an affected multi-file selection with CI's four-worker
loadgroup shape before corrected-head CI; serial passes alone do not qualify
that topology. No strict acknowledgement check is relaxed for test isolation.

The primary fix regression uses the existing real API app, actual get_session path and existing protected PATCH-agent handler. Observe its lock connection through real engine events; pause W1 before its handler write, invalidate only that owned connection, let W2 mutate a different agent and publish its stable value, then resume W1. The authoritative assertion is W1's persisted field remains unchanged while W2's change persists; W1 must not append a late write underneath W2's token. Reversing only product changes must fail that data-state assertion on old source, not a missing-new-symbol import. Do not replace the datastore or handler SQL with a fake. Test helpers may pause/wrap the existing service call while delegating its real operation.

The complete matrix is mandatory:

| Criterion | Positive and negative evidence |
| --- | --- |
| 1 | Exact protected route inventory/exclusions; old HTTP/status/schema and multi-commit semantics; no new writer/evaluator/worker/schema/version changes. |
| 2 | Direct yielded AsyncSession; borrowed same session; normal no-marker fallback; nested marker refusal; present poisoned/closed marker refusal without fallback; retained Request after completion. |
| 3 | Same driver across multiple commits, rollback, refresh and borrower Session.close; committed versus unfinished effects; conflicting readers/writers remain blocked until lifetime ends. |
| 4 | Owner versus unrelated invalidation; caught error→rollback→new Session reuse; execute/raw-driver/commit guards; premature owner Connection.close/pool-return containment; no listener cross-poison/leak after confirmed disposition; quarantine retains required guards on unconfirmed discard. |
| 5 | Pending before write; failed/lost pending ACK has zero handler writes; ordinary refusal after known settlement rotates stable; poison/unknown settlement does not; compare-publish cannot replace another token; lost stable ACK may already have published. |
| 6 | Clean unlock and pool reuse; false/error/uncertain unlock discards; each cleanup timeout/cancellation, terminal quarantine and delayed original checkin versus unrelated reuse; primary versus secondary error precedence; prior commits survive; no false pending/rollback/HTTP-status claim. |
| 7 | Real malformed snapshot path uses same physical owner, rolls back unfinished dependencies, retains expected failure metadata/422; bad credential/epoch remains unchanged; poison never falls back. |
| 8 | Exact chunked JSON and form replay; existing body/auth/refusal bounds; nonprotected pass-through; real app observe_http-outside-fence order and 408/401 telemetry; awaited background/dependency/stream/disconnect/error/cancellation lifetime. |
| 9 | Pinned public hook behavior and stated limits; cleanup/listener/pool ownership checks; no production-network or enqueue authority claim. |
| 10 | Primary regression fails old source by persisted data; independent RED then GREEN; focused existing route checks plus required static/docs/private-import-guard/fix-pin/exact-head CI. |

This API writer repair does not prove atomic authorized XADD, source/credential freshness, all active-writer participation, deployment safety or worker claim/resume/privileged-effect authorization. Those remain separate contracts and ownership gates.

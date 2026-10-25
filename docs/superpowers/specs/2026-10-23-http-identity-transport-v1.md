# HTTP identity transport v1

Realizing specification under [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md), tracked in [#4890](https://github.com/curie-eng/curie/issues/4890). It consumes the [HTTP identity facts v1 contract](2026-10-23-http-identity-facts-v1.md). This specification claims no implementation or activation. Authority persistence, configuration/admin APIs, secret references, principal lifecycle writes, policy, app startup and all active invocation/approval/Slack consumers are excluded. Principal lifecycle adoption and role/migration contracts remain separate work.

## Fixed public surface and trusted inputs

<!-- @spec HTTP-IDENTITY-TRANSPORT-1 -->
The API identity transport owner implements this internal surface without changing app wiring. The minimal public internal interface is:

```text
LookupTarget(endpoint, tenant_id, expected_issuer, authority_version)  # immutable
ProviderBearer(value)                                                # secret, immutable
HttpIdentityObservation.facts -> ValidatedFacts                      # explicit sensitive access
HttpIdentityObservation.response_bytes -> bytes                      # exact accepted entity
HttpIdentityClient(*, tls_context=None, allow_loopback_http=False)
await client.lookup(*, target, bearer, request, limits)
    -> HttpIdentityObservation | FactsRefusal
await client.aclose() -> CloseReport
client.close_status() -> CloseReport
async with HttpIdentityClient(...) as client: ...
```

`request` is T1 LookupRequestV1; `limits` is T1 FreshnessLimits. No arbitrary headers, HTTP client, transport, executor, endpoint resolver or inbound request object is accepted by this product interface. Test controls may patch the narrow pure validation seam; production cannot select another parser. Capture validated immutable target/secret/request/limits together at lookup entry, including nested request data. Target tenant, issuer and positive configuration generation must equal the request's scope tenant/expected issuer and authority_version. Invalid local input raises ValueError with fixed text `invalid_lookup_context`, before network or executor submission; it never includes the input or a validation exception. It creates no provider-unresolved outcome.

The endpoint comes only from trusted operator configuration. Require absolute HTTPS, hostname, valid optional port and no userinfo, query, fragment, whitespace or control character. No endpoint is taken from provider facts. With explicit per-instance allow_loopback_http=True, plain HTTP is allowed only for literal host 127.0.0.1 or ::1; localhost, alternate numeric spelling, DNS aliases and other addresses remain invalid. This test option changes no process setting. HTTPS verification always requires both CERT_REQUIRED and hostname checking. Default uses verified public trust; an explicitly supplied SSLContext may add the caller's private CA while retaining verification. Never accept verify=False or change system/environment trust.

ProviderBearer holds a nonempty RFC6750 bearer token (ASCII `[A-Za-z0-9._~+/-]+` followed only by optional `=` padding), with no whitespace/controls and no added length cap. Its value is excluded from repr, equality, hashes and serialization; validation errors have fixed text only. The caller must supply the separately provisioned provider credential; the client neither accepts nor discovers platform/inbound credentials and cannot prove a string's entropy/provenance. Never copy the outbound credential object, bearer or headers into a result, write them to disk, expose them in task names or log them. A provider can reflect sensitive text into otherwise valid facts/body; treat the entire accepted observation as sensitive rather than promising that its explicit content cannot contain a reflected bearer. No process-memory zeroization guarantee is made.

Each lookup owns one HTTP/1.1-only public HTTPX AsyncHTTPTransport with retries=0, trust_env=False, no proxy and verified TLS, and one explicit Request. Call handle_async_request exactly once with POST, serialized request bytes, Content-Type `application/json`, Accept `application/json`, Accept-Encoding `identity` and the dedicated Authorization bearer header. HTTPX may add ordinary Host/content framing. Do not use AsyncClient, redirect helpers, inherited auth/cookies/hooks or shared application clients. No response cookie jar, identity cache or automatic retry exists, including after malformed response, disconnect or a changed configuration. A subsequent call is a new exchange. The caller owns later current-configuration/database revalidation; this client retains exactly its captured generation and never switches credentials mid-call.

## Response and deterministic result mapping

<!-- @spec HTTP-IDENTITY-TRANSPORT-2 -->
Only HTTP 200 can supply facts. Every other final status, including 3xx/404/401/403/429/5xx, returns FactsRefusal(code="unavailable") without following Location or reading its body. For 200, require one Content-Type with media type application/json, optionally only charset=utf-8 (case insensitive, quoted UTF-8 permitted). Missing, duplicate, conflicting or other media types/parameters are invalid_response. Content-Encoding must be absent or one identity token, case insensitive after HTTP optional whitespace; duplicate fields, lists/chains and every nonidentity coding are invalid_response before body consumption. No gzip/deflate/brotli/zstd decoder is invoked.

Consume only Response.aiter_raw. Do not access content/text/json/aread or a decoded iterator. Accumulate at most 65,536 entity-body bytes; before appending each chunk, refuse if the sum would exceed that limit. A valid declared Content-Length greater than the cap permits immediate invalid_response. Content-Length absence or a smaller value never replaces incremental counting. Transfer chunk framing is not part of the entity. Since encodings are refused, raw entity equals decoded entity. This cap is not a claim about HTTP header, socket, TLS or dependency-buffer memory.

Freeze the complete bounded entity once and pass that exact byte string to T1 validate_lookup. A ValidatedFacts result becomes one HttpIdentityObservation containing those facts and that same exact entity, including whitespace, key order, escaping and trailing JSON whitespace. Definitive resolved, inactive and unresolved facts can all produce an observation; none grants permission. A FactsRefusal remains the same bare code-only refusal, without bytes or an observation attached. Complete malformed UTF-8/JSON is invalid_response. Incomplete/malformed HTTP framing, including early EOF versus declared Content-Length, may fail before an entity exists and maps to unavailable. DNS/connect/TLS/write/read/protocol failures map to unavailable; oversized body and unsupported response encoding/media map to invalid_response. Never return an HTTPX request/response, raw exception, response reason/header or redirect next request. Original body bytes are available only through explicit access to an accepted opaque observation; every refusal and implicit output remains body-free. An unexpected internal exception raises fixed-text IdentityTransportInternalError("identity_transport_internal_error") from None rather than a definitive provider outcome; ownership cleanup still runs, and no raw exception is stored in a public result/report or logged.

The detached observation is an immutable in-process value, not an authentication/authorization token. It retains only frozen ValidatedFacts and immutable response bytes, with no transport, stream, endpoint, credential/header, executor, session, lock or callback. A later consumer must revalidate these original bytes under its own current context and post-lock clock; transport-time facts cannot substitute for that check. Do not re-encode facts to reconstruct evidence.

## One result deadline including bounded parsing

<!-- @spec HTTP-IDENTITY-TRANSPORT-3 -->
D is loop.time() at lookup entry plus exactly 2.0 seconds. The budget includes operation admission, input snapshot/serialization, transport setup, DNS/connect/TLS, request send, headers, body, parse admission and parse completion. Configure each HTTP phase timeout at no more than remaining budget via the explicit request timeout extension, and enforce one outer monotonic result deadline. No phase, progress, parse, retry or cleanup resets D. Check monotonic time after synchronous work and immediately before returning; at or after D return unavailable, never late facts or a previously computed provider refusal. Caller-input errors remain caller errors, not successful results.

There are **two active operation slots** per client, held until that operation's network and parse ownership is actually released. Waiting for a slot is inside the same D; no exchange starts after D or after close begins. There is **one client-owned parser worker**, created lazily, with a single physical admission slot acquired before submitting. No global/default executor, per-request worker thread, unbounded submission queue or process is introduced. Parse-slot waiting is inside D. Release the slot only on completion/cancellation-before-start of the underlying concurrent future, never merely because an awaiting asyncio wrapper was cancelled.

The worker runs only the pure T1 validator, with UTC now sampled when validation starts, and has no I/O, credentials or side effects. Its private future remains ValidatedFacts | FactsRefusal. Wrap only a successful result after that existing future completes, with no second parse, job, thread, I/O or wait; byte freezing and wrapper work remain inside D. A timed-out/cancelled validation result is permanently discarded. Final return checks D and observation.facts identity/membership expiry against fresh exact UTC time, preserving T1's equality boundary; an expired identity returns identity_stale, otherwise expired membership returns membership_stale, unless D has already expired, which takes precedence as unavailable. The last local freshness check does not establish provider truth or replace a later consumer's commit-time check.

A caller's cancellation propagates CancelledError; it is not converted to unavailable or retried. Signal cancellation to the owned exchange, keep all remaining cleanup/parser ownership tracked and return control without waiting past the result deadline. Waiting for an owned task's cancellation must not secretly await cancellation-shielded dependency close beyond D. Python/OS scheduling is not real-time: the invariant is one unextended validity deadline and no accepted late result, not forced execution of a timer or termination of a running thread at exactly two seconds.

## Close, eventual drain and incomplete ownership

<!-- @spec HTTP-IDENTITY-TRANSPORT-4 -->
Every operation tracks its response, transport, exchange/close tasks and submitted parser future until released. Successful facts or an ordinary pre-deadline provider refusal may return only after that operation's network close completes within D. If closure would overrun D, return unavailable and retain cleanup ownership. At timeout/cancellation there is no additional lookup budget: schedule/retain owned close, discard results and expose pending ownership through close_status. The two operation slots also bound outstanding cleanup operations; a timed-out parser keeps its physical slot until actual completion. Consume background exceptions without logging their contents; failed close never becomes a false complete report.

```text
CloseReport {
  state: "open" | "closing" | "closed",
  pending_exchanges: nonnegative integer,
  pending_parsers: nonnegative integer,
  worker_joined: boolean,
  close_failed: boolean
}
```

This report contains counts/state only, not resource/endpoint/request/credential identifiers. `closed` requires zero pending exchanges/parsers, all owned network resources acknowledged closed, joined parser infrastructure and no unresolved close failure. With no worker ever started, worker_joined is true. Lookup checks closing state before input validation; after closing starts it returns unavailable without I/O/submission. Waiters are woken when close starts rather than admitted afterward.

The first aclose atomically enters closing, cancels owned exchanges and pending admission, requests cancellation of not-started parse work and initiates executor shutdown exactly once. It waits at most **250 ms** for drain and returns the current CloseReport; this is an explicit separate shutdown operation, not an extension of any lookup budget. Repeated aclose calls are idempotent bounded drain attempts of at most 250 ms each, reusing the same shutdown and ownership records. close_status is immediate and performs no waiting. It remains possible to observe incomplete drain and call aclose again after the blocked work finishes. A close failure remains tracked as close_failed with state closing until an actual successful retry/acknowledgment resolves it.

Never run executor.shutdown(wait=True) on the event-loop thread. A single optional **client-owned joiner thread**, started only once when shutdown begins, may perform that blocking join; together with the one parser worker this is at most two owned CPU-lifecycle threads per client, never per request. Track that joiner too; worker_joined becomes true only when the worker/joiner are actually finished. A running parser thread is not killed by cancel_futures or shutdown(wait=False). If it outlasts 250 ms, report incomplete state and retain references; do not label it closed, drop ownership or create replacement workers. There is no unbounded cleanup task/thread creation.

Cancellation of aclose propagates to its caller while its single owned shutdown continues; repeated cancellation cannot cancel/drop that ownership. Suppression remains active in all owned close tasks. Async context exit calls aclose once. With no body exception and incomplete close it raises sanitized IdentityShutdownIncomplete containing only CloseReport; with a body exception/cancellation it preserves that original exception, and incomplete ownership remains visible via close_status/repeated aclose. Entering an already closing/closed context raises sanitized IdentityClientClosed. No late parse result can be delivered through close, drain or context exit.

A returned observation belongs to the caller and remains unchanged and explicitly readable after client close. There is no new cache or retention collection. Unreturned results retain only the existing operation/parser/cleanup ownership until eventual drain; do not claim zeroing or erasure of all process copies.

## Transport-local dependency log guard

<!-- @spec HTTP-IDENTITY-TRANSPORT-5 -->
Install one stable ContextVar-checking suppression filter on the **actual emitters** before owned I/O: httpcore.connection, httpcore.http11 and httpx for the pinned HTTP/1.1 direct path. A parent-httpcore filter alone is insufficient because logger filters are not applied to child records during propagation. Suppress these records only while the current context belongs to this lookup; no heuristic redaction or provider text is emitted. Guard scope includes request creation, exchange and owned close tasks and is reset in finally. Child cleanup tasks retain their lookup context after the initiating caller has timed out.

Filter installation is idempotent across concurrent clients and remains stable; do not repeatedly add/remove filters per request, alter levels/handlers/propagation, or mutate process environment. ContextVar tokens support nested guards. An unrelated concurrent HTTPX request and later ordinary logging remain observable. Apply suppression before records reach queue handlers so filtering is independent of a later logging thread's context. This is one private transport helper, not a configurable logging subsystem. Dependency upgrades or enabling HTTP/2 require rechecking actual emitters before this guarantee is extended.

The exported HttpIdentityObservation type uses private slots with read-only facts/response_bytes properties, ordinary identity equality/hash, and exactly HttpIdentityObservation() for str/repr. Normal caller construction raises fixed-text TypeError identity_observation_internal. It is not a dataclass or mapping, has no instance dictionary/iterator/automatic bytes/model_dump/to_dict surface, and rejects direct state extraction and pickle/state reduction with fixed-text TypeError identity_observation_not_serializable and no sensitive exception chain. Standard JSON/dataclass/vars conversion fails without formatting content. No observation/bytes enter logs, task names, exceptions or CloseReport. Explicit accessor use, deliberate Python introspection and downstream callers are not a confidentiality sandbox; access does not authorize HTTP return, persistence, logging or publication of the sensitive contents.

## Required independent test evidence and source observations

Use real owned loopback HTTP/1.1 and TLS listeners with ephemeral ports/certificates, verified private-CA positive control, untrusted CA/hostname/cleartext negatives and fixture-registered cleanup. No DB/provider/model/container/shared stack is needed. Intentional-red tests precede client code in a separate context. Cover:

- Exact target/request/config match, dedicated bearer receipt, rejected invalid contexts, no ambient proxy/auth/cookies, repeated calls and concurrent independent clients.
- Aggregate header delay plus body drip below each phase timeout but over D; TLS-handshake stall, quick successful control, redirected second listener receiving no connection/credential, and non-200 with hanging body not consumed.
- Exact 65,536/65,537 limits with declared/chunked/close-delimited entities, crossing chunks, compressed/bomb and duplicate encoding rejection without decoding; complete invalid JSON versus incomplete HTTP framing mapped at their correct layers.
- Delayed pure-parser seam and parse-slot waiting: timely unavailable, no late facts, retained physical slot, later drain and subsequent usable slot. Cancel during slot wait/headers/body/TLS/parse and close, close racing admission/I/O/parse, repeated close/cancellation, post-close refusal, and actual executor/joiner/listener/task cleanup after controlled fixture release. Assert timely result separately from eventual drain; assert incomplete reports rather than fake joining a blocked worker.
- DEBUG records from both actual HTTPCore emitters, provider-reflected secret/body markers in reason/header/error, queue-based logging, nested/concurrent guards, unrelated HTTPX call and restoration after cancellation. Implicit result formatting/conversion, reports, exception strings and task names contain no sentinels. Explicit observation access intentionally retains the original sensitive facts/body.

- Exact accepted bytes across multi-chunk and exact-cap responses with noncanonical JSON, independently checked resolved/inactive/unresolved facts, one T1 invocation using the exact returned bytes, and concurrent distinct responses with no cross-operation pairing or reuse.
- Opaque construction/state/conversion refusal, identity equality/hash, immutable nested facts and bytes, explicit access after close, and a synthetic bearer reflected in valid facts: preserve explicit evidence while keeping implicit outputs safe. Every existing final expiry/deadline/close gate still returns a bare refusal without evidence.

## Pinned source observation

The locked and imported versions are HTTPX **0.28.1** and HTTPCore **1.0.9**. The following reproducible source-inspection command was run against the specification base. It sends no request and needs no provider or database:

```bash
uv run python - <<'PY'
import inspect
from importlib import import_module
import httpx
import httpcore

print("httpx", httpx.__version__, "httpcore", httpcore.__version__)
objects = [
    httpx.AsyncHTTPTransport.__init__,
    httpx.AsyncHTTPTransport.handle_async_request,
    httpx.AsyncHTTPTransport.aclose,
    httpx.Response.aiter_bytes,
    httpx.Response.aiter_raw,
    httpx.Timeout.as_dict,
    import_module("httpcore._async.http11").AsyncHTTP11Connection._receive_event,
    import_module("httpcore._backends.anyio").AnyIOStream.read,
    import_module("httpcore._async.connection_pool").PoolByteStream.aclose,
    import_module("httpcore._trace").Trace.atrace,
    httpx.AsyncClient._send_single_request,
]
for obj in objects:
    print(obj.__qualname__, inspect.signature(obj))
    print(inspect.getsource(obj))
PY
```

Observed behavior: the public transport maps one explicit request into a streaming response and exposes async close. Its zero retry count is passed into the connection pool. HTTPX Timeout supplies separate connect/read/write/pool values; HTTPCore applies the read value per network read, so it is not an aggregate deadline. The decoded response iterator invokes the decoder before chunking; the raw iterator avoids that decoder. HTTP/1.1 network reads are bounded internally at 64 KiB without making the application body cap a total-memory guarantee. Pool close shields cancellation but supplies no application shutdown budget. Direct transport use avoids AsyncClient's cookie handling, redirect loop and raw URL/reason INFO log; HTTPCore's actual connection/http11 emitters still format DEBUG headers/errors through Trace. The source inspection uses dependency internals only as observation; client implementation uses the public transport API and does not subclass or patch dependency internals.

T1 validate_lookup is synchronous and uses exact bigint/Fraction arithmetic, so an asyncio timer alone cannot preempt parsing. Re-resolve these named symbols when the locked dependency changes; no runtime qualification follows from this source observation.

Only the facts contract remains normative for T1; this new spec adds transport behavior without reinterpreting facts, policy, shared principal status or activation. Local synthetic mechanics and final exact-head CI do not qualify an upstream directory, provider freshness or a deployed gateway.

# Shared connector resolver: reference SDK and wire corpus

This directory holds the reference client for the shared connector resolver
and the executable wire corpus that defines conformance for any client
(ADR 0239, "Bundle, identity and request contract" 4 to 6).

| File | What it is |
| --- | --- |
| `curie_resolver_sdk.py` | The reference SDK: one file, standard library only, runs on Python 3.12 and later. |
| `wire_corpus.json` | Language-neutral conformance cases (`resolve`, `raw`, `provider`, `barrier`). |
| `run_wire_corpus.py` | Standard-library runner that loads one SDK copy by path and runs every case against a live resolver. |

## Vendoring the SDK

Copy `curie_resolver_sdk.py` into your connector's own Docker build context
and import it from there. Do not install the worker package; the SDK has no
dependencies and does not import anything from this repository. The copy you
vendor is yours to own: pin it to the repository revision you took it from
and rerun the corpus against your copy whenever you update or patch it.

Inside the connector container the resolver socket path arrives in
`CURIE_SHARED_RESOLVER_SOCKET`, which `ResolverClient()` reads by default.

```python
import curie_resolver_sdk as sdk

provider = sdk.ProviderClient(sdk.ResolverClient(), "access_token")
response = provider.request(
    handle,  # the invocation handle the proxy forwarded
    "GET",
    "https://api.example.com/v1/items",
    purpose="list open items",  # audit metadata only; it grants nothing
)
```

Rules the SDK enforces, and that any other client must enforce too:

1. Every provider call resolves again. Never keep a `Resolution` or an
   authorization header for a later call; a disable, a membership change or a
   reimport may have committed in between.
2. Each resolve is a new connection to the socket, with a `Content-Length`
   body (no chunked framing, no content encoding).
3. Send only to an https origin the resolve returned. Hosts are compared case
   insensitively and a missing port means 443. Anything else raises
   `OriginRefused` before a byte is sent.
4. Never follow a 3xx. It raises `RedirectRefused`, so the credential is not
   re-sent to a location the resolver did not declare.
5. Keep auth and cache state per `(binding, account, revision)`. Use
   `PartitionedCache`, keyed by `Resolution.partition` or
   `ProviderResponse.partition`, so a reimport or another member starts empty.
6. Never log the handle or credential fields. `Resolution` and `Field` keep
   them out of `repr`, and SDK exceptions never include them.
7. Never let a caller choose the request authority. A `Host` header (any
   case) raises `ValueError` before the resolve; the SDK sets `Host` from the
   checked origin.
8. Check every header before sending it. Names must be HTTP tokens and
   values Latin-1 with no control character other than tab, so no CR or LF;
   a bad header, the built Authorization included, raises `ValueError` with a
   fixed message that never quotes the value. Transport failures raise
   `ProviderTransportError`, which names only the failure's class and chains
   nothing, because `http.client` and `ssl` messages can quote request data.

A connector-custody application releases identity only: request the empty
slot `""` and use the binding and account to select the credential your
connector manages.

## Running the corpus against your copy

The runner needs a live world: a resolver for a platform-custody application
with two members holding single text field `token` credentials, a second
connector-custody application with its own resolver, and an HTTPS provider
that answers 302 on the redirect path and 200 on the allowed path with a JSON
body whose `authorization_sha256` is the hex SHA-256 of the Authorization
value it received (null when there was none). It reads
that world from a fixture JSON file whose schema is documented in the module
docstring of `apps/worker/tests/connector_proxy/test_resolver_sdk_corpus.py`.
That test builds the world against a real API, Postgres and a fake provider
and runs the canonical SDK; to check your vendored copy instead, run the
runner with `--sdk` pointing at your file:

```sh
python3.12 -I -B run_wire_corpus.py \
    --sdk path/to/your/curie_resolver_sdk.py \
    --corpus wire_corpus.json \
    --fixture path/to/fixture.json
```

The runner prints one JSON line per case, `{"id", "kind", "passed"}` with a
`detail` on failure, and a final summary line carrying `python`, `total`,
`passed` and `failed`. It exits 0 only when every case passed.

Every provider case that expects a response also checks credential
attribution: the echoed digest must equal the fixture's
`authorization_sha256[<role>][<phase>]`, the SHA-256 of `"Bearer " + <that
role's token>`, where the phase is `before_reimport` until the `reimport`
barrier has run and `after_reimport` from then on. A copy that sends a
credential cached from an earlier call, or another member's, fails those
cases. The runner compares digests and never prints them.

`barrier` cases pause the run: the runner prints `{"barrier": "<name>"}` and
waits for one line on stdin. Whoever drives the run performs the action
(`reimport` replaces the member's credential with the barrier's new text,
`disable` disables the member's binding) and then writes `go`. Exactly one
provider call happens between the `reimport` and `disable` barriers, so a
driver can check that call against one resolver admission and release.

Clients in other languages implement the wire directly. The `raw` cases are
the wire's framing and refusal contract as exact bytes, and the `resolve` and
`provider` cases describe the behavior a client library must show.

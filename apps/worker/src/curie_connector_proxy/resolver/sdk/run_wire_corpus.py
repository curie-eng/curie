"""Run the shared resolver wire corpus against one copy of the SDK (ADR 0239).

ADR 0239 "Bundle, identity and request contract" 5: adopters vendor
``curie_resolver_sdk.py`` and exercise each copy against the same executable
wire corpus. This runner is that exercise. It is standard library only and
runs on Python 3.12, loads the SDK by file path (no install, so the copy under
test is exactly the file named), and runs every case in ``wire_corpus.json``
against a live resolver socket::

    python3 run_wire_corpus.py --sdk <curie_resolver_sdk.py> \\
        --corpus <wire_corpus.json> --fixture <fixture.json>

The fixture is written by whoever stands up the live world (a resolver for a
static platform-custody application, a second connector-custody application,
and an HTTPS provider). Its schema is documented in the corpus test,
``apps/worker/tests/connector_proxy/test_resolver_sdk_corpus.py``; cases name
fixture roles, never literal handles, so one corpus serves every run.

Case kinds:

1. ``resolve``: one ``ResolverClient.resolve`` through the SDK, compared with
   the role's expected answer (or the case's own ``expect``).
2. ``raw``: exact bytes over AF_UNIX, without the SDK. These are wire
   conformance (framing, bounds, media type, refusal statuses and the fixed
   refusal body), so any language's client can be held to the same server.
3. ``provider``: one ``ProviderClient.request``, all of them on ONE
   ``ProviderClient`` and ONE ``PartitionedCache`` for the whole run, so the
   R1-6 sequence proves that nothing carries authority from one call to the
   next.
4. ``barrier``: prints ``{"barrier": "<name>"}``, flushes, and blocks on one
   stdin line; the driver performs the named action (``reimport``,
   ``disable``) and writes ``go``.

Output: one JSON line per case, ``{"id", "kind", "passed"}`` plus a
``detail`` when it failed, then one summary line carrying ``python``
(``sys.version_info[:3]``) and the counts. Exit 0 only when every case
passed. No line ever carries a handle or credential text: details name what
differed, never the values.
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import importlib.util
import json
import socket
import ssl
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

CORPUS_VERSION = 1
FIXTURE_VERSION = 1
DEFAULT_PURPOSE = "wire corpus conformance"
RAW_TIMEOUT_S = 30.0
RAW_MAX_REPLY_BYTES = 1 << 20


class CaseFailed(Exception):
    """A case did not hold; the message names what differed, never a value."""


def _require(condition: bool, detail: str) -> None:
    if not condition:
        raise CaseFailed(detail)


def load_sdk(path: Path) -> ModuleType:
    """Import the SDK copy at ``path`` by file, exactly as vendored."""

    spec = importlib.util.spec_from_file_location("curie_resolver_sdk", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load an SDK from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Run:
    """Everything one corpus run shares: the fixture, clients and the cache."""

    def __init__(self, sdk: ModuleType, fixture: Mapping[str, Any]) -> None:
        self.sdk = sdk
        self.fixture = fixture
        provider = fixture["provider"]
        context = ssl.create_default_context(cafile=str(provider["ca_file"]))
        # ONE provider client and ONE cache for the whole run (plan R1-6).
        self.provider_client = sdk.ProviderClient(
            sdk.ResolverClient(str(fixture["socket"])), str(fixture["slot"]), ssl_context=context
        )
        self.cache = sdk.PartitionedCache()
        self._resolvers: dict[str, Any] = {}
        self.resolutions: dict[str, Any] = {}
        self.responses: dict[str, Any] = {}

    # --- fixture lookups ------------------------------------------------------

    def handle(self, case: Mapping[str, Any]) -> str:
        if "handle" in case:
            return str(case["handle"])
        return str(self.fixture["handles"][case["role"]])

    def socket_path(self, case: Mapping[str, Any]) -> str:
        role = case.get("role")
        if role is not None and role in self.fixture.get("sockets", {}):
            return str(self.fixture["sockets"][role])
        return str(self.fixture["socket"])

    def slot(self, case: Mapping[str, Any]) -> str:
        chosen = case.get("slot")
        if isinstance(chosen, str):
            return chosen
        if isinstance(chosen, dict) and chosen.get("fixture") == "slot":
            return str(self.fixture["slot"])
        role = case.get("role")
        if role is not None and role in self.fixture.get("slots", {}):
            return str(self.fixture["slots"][role])
        return str(self.fixture["slot"])

    def expectation(self, case: Mapping[str, Any]) -> Mapping[str, Any]:
        """A resolve case's own ``expect``, else the fixture's answer for it."""

        if "expect" in case:
            expect: Mapping[str, Any] = case["expect"]
            return expect
        return self.fixture_answer(case)

    def fixture_answer(self, case: Mapping[str, Any]) -> Mapping[str, Any]:
        """The fixture's expected answer for the case's role or named barrier."""

        source = case.get("expect_from", {"role": case.get("role")})
        if "barrier" in source:
            barrier: Mapping[str, Any] = self.fixture["barriers"][source["barrier"]]["expected"]
            return barrier
        expected: Mapping[str, Any] = self.fixture["expected"][source["role"]]
        return expected

    def secrets(self) -> list[str]:
        """Every handle and credential text the fixture names."""

        values = [str(value) for value in self.fixture["handles"].values()]
        expectations = list(self.fixture["expected"].values())
        expectations += [barrier["expected"] for barrier in self.fixture["barriers"].values()]
        values += [str(item["text"]) for item in expectations if "text" in item]
        return [value for value in values if value]

    def resolver(self, case: Mapping[str, Any]) -> Any:
        if case.get("use_env_socket"):
            # The SDK default: the socket named by CURIE_SHARED_RESOLVER_SOCKET.
            return self.sdk.ResolverClient()
        path = self.socket_path(case)
        if path not in self._resolvers:
            self._resolvers[path] = self.sdk.ResolverClient(path)
        return self._resolvers[path]

    def clean(self, exc: BaseException) -> None:
        """An SDK exception's text and repr carry no handle or credential."""

        shown = f"{exc} {exc!r}"
        _require(
            not any(secret in shown for secret in self.secrets()),
            f"{type(exc).__name__} text carries a handle or credential",
        )


def purpose(case: Mapping[str, Any]) -> str:
    value = case.get("purpose", DEFAULT_PURPOSE)
    if isinstance(value, dict):
        return str(value["repeat"]) * int(value["count"])
    return str(value)


# --- resolve ------------------------------------------------------------------


def check_resolution(run: Run, resolution: Any, expected: Mapping[str, Any], slot: str) -> None:
    sdk = run.sdk
    _require(isinstance(resolution, sdk.Resolution), "resolve did not return a Resolution")
    for key in ("admission", "binding", "account", "revision", "expires_at"):
        _require(getattr(resolution, key) == expected[key], f"{key} differs")
    _require(resolution.slot == slot, "slot differs from the requested slot")
    origins = resolution.origins
    _require(
        isinstance(origins, tuple) and bool(origins) and all(isinstance(o, str) for o in origins),
        "origins is not a nonempty tuple of strings",
    )
    _require(
        resolution.partition == (expected["binding"], expected["account"], expected["revision"]),
        "partition is not (binding, account, revision)",
    )
    if expected["admission"] == "credential":
        name = expected["field"]
        _require(name in resolution.fields, "the expected credential field is absent")
        value = resolution.fields[name]
        _require(isinstance(value, sdk.Field), "a credential field is not a Field")
        _require(value.data is None and value.text == expected["text"], "field text differs")
        shown = repr(resolution) + repr(value)
        _require(expected["text"] not in shown, "repr carries the credential text")
    else:
        _require(len(resolution.fields) == 0, "an identity admission carries fields")


def run_resolve(run: Run, case: Mapping[str, Any]) -> None:
    expected = run.expectation(case)
    handle = run.handle(case)
    slot = run.slot(case)
    try:
        resolution = run.resolver(case).resolve(handle, slot, purpose(case))
    except run.sdk.ResolverRefused as refused:
        run.clean(refused)
        _require(
            "refusal" in expected,
            f"refused {refused.code} ({refused.status}) where a release was expected",
        )
        _require(refused.code == expected["refusal"], f"refusal code {refused.code} differs")
        _require(refused.status == expected["status"], f"status {refused.status} differs")
        _require(str(refused) == refused.code, "str(ResolverRefused) is not its code")
        return
    _require("refusal" not in expected, f"released where {expected.get('refusal')} was expected")
    _require(expected["status"] == 200, "released where a refusal status was expected")
    check_resolution(run, resolution, expected, slot)
    if handle:
        _require(handle not in repr(resolution), "repr carries the handle")
    if "same_as" in case:
        earlier = run.resolutions.get(case["same_as"])
        _require(earlier is not None, "the case it must match did not release")
        _require(resolution == earlier, "the purpose changed what was released")
    run.resolutions[case["id"]] = resolution


# --- raw ----------------------------------------------------------------------


def raw_body(run: Run, case: Mapping[str, Any], spec: Mapping[str, Any]) -> bytes:
    if "text" in spec:
        return str(spec["text"]).encode("utf-8")
    template = spec["resolve"]
    document: dict[str, Any] = {
        "handle": run.handle(case),
        "slot": run.slot(case),
        "purpose": purpose(case),
    }
    for key in template.get("omit", ()):
        document.pop(key, None)
    document.update(template.get("extra", {}))
    body = json.dumps(document, separators=(",", ":")).encode("utf-8")
    pad_to = template.get("pad_to")
    if pad_to is not None:
        body += b" " * (int(pad_to) - len(body))
        _require(len(body) == int(pad_to), "the padded body cannot reach its size")
    return body


def substitute(run: Run, case: Mapping[str, Any], value: str) -> str:
    return run.handle(case) if value == "$handle" else value


def raw_request(run: Run, case: Mapping[str, Any]) -> tuple[bytes, bytes]:
    request = case["request"]
    body = raw_body(run, case, request["body"])
    encoding = request.get("encoding")
    if encoding == "gzip":
        body = gzip.compress(body)
    elif encoding == "chunked":
        body = f"{len(body):x}\r\n".encode("ascii") + body + b"\r\n0\r\n\r\n"
    lines = [
        f"{request.get('method', 'POST')} {request.get('target', '/v1/resolve')} "
        f"{request.get('version', 'HTTP/1.1')}",
        "Host: localhost",
    ]
    content_type = request.get("content_type", "application/json")
    if content_type is not None:
        lines.append(f"Content-Type: {content_type}")
    length = request.get("content_length", "exact")
    if length == "exact":
        lines.append(f"Content-Length: {len(body)}")
    elif isinstance(length, dict) and "delta" in length:
        lines.append(f"Content-Length: {len(body) + int(length['delta'])}")
    elif isinstance(length, dict) and "value" in length:
        lines.append(f"Content-Length: {substitute(run, case, str(length['value']))}")
    else:
        _require(length == "omit", "unknown content_length form")
    for name, value in request.get("headers", ()):
        lines.append(f"{name}: {substitute(run, case, str(value))}")
    head = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")
    return head, body if request.get("send_body", True) else b""


def exchange(path: str, head: bytes, body: bytes, *, close_write: bool) -> bytes:
    """Send the head, then the body; read to EOF. An early close is not an error."""

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(RAW_TIMEOUT_S)
    try:
        sock.connect(path)
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            sock.sendall(head)
            if body:
                sock.sendall(body)
            if close_write:
                sock.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        received = 0
        while received <= RAW_MAX_REPLY_BYTES:
            try:
                chunk = sock.recv(65536)
            except ConnectionResetError:
                break
            if not chunk:
                break
            chunks.append(chunk)
            received += len(chunk)
        return b"".join(chunks)
    finally:
        sock.close()


def parse_reply(data: bytes) -> tuple[int, dict[str, str], bytes]:
    head, separator, body = data.partition(b"\r\n\r\n")
    _require(bool(separator), "no complete HTTP response head")
    lines = head.decode("latin-1").split("\r\n")
    parts = lines[0].split(" ", 2)
    _require(len(parts) >= 2 and parts[0] == "HTTP/1.1", "the status line is not HTTP/1.1")
    _require(parts[1].isdigit() and len(parts[1]) == 3, "the status code is not three digits")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, colon, value = line.partition(":")
        _require(bool(colon), "a response header line has no colon")
        key = name.strip().lower()
        _require(key not in headers, f"duplicate response header {key}")
        headers[key] = value.strip()
    return int(parts[1]), headers, body


def run_raw(run: Run, case: Mapping[str, Any]) -> None:
    expected = case["expect"]
    head, body = raw_request(run, case)
    data = exchange(
        run.socket_path(case), head, body, close_write=bool(case["request"].get("close_write"))
    )
    handles = [str(value) for value in run.fixture["handles"].values() if value]
    _require(not any(h.encode("utf-8") in data for h in handles), "the reply echoed a handle")
    if "refusal" in expected:
        _require(
            not any(secret.encode("utf-8") in data for secret in run.secrets()),
            "a refusal carries a handle or credential",
        )
    status, headers, payload = parse_reply(data)
    _require(status == expected["status"], f"status {status} differs")
    _require(headers.get("cache-control") == "no-store", "Cache-Control is not no-store")
    _require(
        headers.get("content-type", "").startswith("application/json"),
        "Content-Type is not application/json",
    )
    _require(headers.get("connection", "").lower() == "close", "Connection is not close")
    _require(
        headers.get("content-length", "").isdigit()
        and int(headers["content-length"]) == len(payload),
        "Content-Length does not frame the body",
    )
    try:
        document = json.loads(payload)
    except ValueError:
        raise CaseFailed("the reply body is not JSON") from None
    if "refusal" in expected:
        _require(document == {"refusal": expected["refusal"]}, "the refusal body differs")
        return
    role_expected = run.fixture["expected"][case["role"]]
    _require(isinstance(document, dict), "the success body is not an object")
    for key in ("admission", "binding", "account", "revision", "expires_at"):
        _require(document.get(key) == role_expected[key], f"{key} differs")
    _require(document.get("slot") == run.slot(case), "slot differs")
    origins = document.get("origins")
    _require(
        isinstance(origins, list) and bool(origins) and all(isinstance(o, str) for o in origins),
        "origins is not a nonempty list of strings",
    )
    if role_expected["admission"] == "credential":
        fields = document.get("credential", {}).get("fields", {})
        _require(
            fields.get(role_expected["field"]) == {"text": role_expected["text"]},
            "the credential field differs",
        )
    else:
        _require("credential" not in document, "an identity admission carries a credential")


# --- provider -----------------------------------------------------------------


def provider_url(run: Run, case: Mapping[str, Any]) -> str:
    provider = run.fixture["provider"]
    origin = str(provider[case["origin"]])
    if "scheme" in case:
        origin = f"{case['scheme']}://{origin.split('://', 1)[1]}"
    return origin + str(provider[case["path"]])


def run_cache(run: Run, case: Mapping[str, Any], response: Any) -> None:
    for operation in case.get("cache", ()):
        carrier = response
        if "from_case" in operation:
            carrier = run.responses.get(operation["from_case"])
            _require(carrier is not None, "the cache case it names has no response")
        _require(carrier is not None, "a cache operation has no partition to use")
        key = operation["key"]
        if operation["op"] == "put":
            run.cache.put(carrier, key, carrier.body)
        else:
            found = run.cache.get(carrier, key)
            if operation["expect"] == "hit":
                _require(found == carrier.body, f"cache missed {key} in its own partition")
            else:
                _require(found is None, f"cache hit {key} across partitions")


def run_provider(run: Run, case: Mapping[str, Any]) -> None:
    sdk = run.sdk
    expect = case["expect"]
    outcome = expect["outcome"]
    response: Any = None
    try:
        response = run.provider_client.request(
            run.handle(case),
            case.get("method", "GET"),
            provider_url(run, case),
            purpose=purpose(case),
        )
    except sdk.OriginRefused as refused:
        run.clean(refused)
        _require(outcome == "origin_refused", "OriginRefused was not expected")
    except sdk.RedirectRefused as refused:
        run.clean(refused)
        _require(outcome == "redirect_refused", "RedirectRefused was not expected")
        _require(refused.status == expect["status"], f"redirect status {refused.status} differs")
        _require(bool(refused.location), "RedirectRefused carries no location")
    except sdk.ResolverRefused as refused:
        run.clean(refused)
        expected = run.fixture_answer(case)
        _require(outcome == "resolver_refused", f"ResolverRefused {refused.code} was not expected")
        _require(refused.code == expected["refusal"], f"refusal code {refused.code} differs")
        _require(refused.status == expected["status"], f"status {refused.status} differs")
    else:
        _require(outcome == "response", f"a response arrived where {outcome} was expected")
        expected = run.fixture_answer(case)
        _require(isinstance(response, sdk.ProviderResponse), "not a ProviderResponse")
        _require(response.status == expect["status"], f"provider status {response.status}")
        _require(
            response.partition == (expected["binding"], expected["account"], expected["revision"]),
            "partition is not the resolved (binding, account, revision)",
        )
        for other in case.get("distinct_from", ()):
            earlier: Any = run.responses.get(other)
            _require(earlier is not None, "the case it must differ from has no response")
            _require(response.partition != earlier.partition, "two partitions are equal")
        run.responses[case["id"]] = response
    run_cache(run, case, response)


# --- barrier ------------------------------------------------------------------


def run_barrier(run: Run, case: Mapping[str, Any]) -> None:
    emit({"barrier": case["barrier"]})
    line = sys.stdin.readline()
    _require(line.strip() == "go", "the driver did not answer the barrier with go")


# --- driver -------------------------------------------------------------------

KINDS: Mapping[str, Callable[[Run, Mapping[str, Any]], None]] = {
    "resolve": run_resolve,
    "raw": run_raw,
    "provider": run_provider,
    "barrier": run_barrier,
}


def emit(record: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def run_case(run: Run, case: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"id": case.get("id"), "kind": case.get("kind"), "passed": False}
    handler = KINDS.get(str(case.get("kind")))
    if handler is None:
        result["detail"] = "unknown case kind"
        return result
    try:
        handler(run, case)
    except CaseFailed as failed:
        result["detail"] = str(failed)
    except Exception as exc:  # noqa: BLE001 - one case never stops the run
        # The class name only: an exception message could quote a value.
        result["detail"] = f"unexpected {type(exc).__name__}"
    else:
        result["passed"] = True
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--sdk", required=True, type=Path, help="the SDK file under test")
    parser.add_argument("--corpus", required=True, type=Path, help="wire_corpus.json")
    parser.add_argument("--fixture", required=True, type=Path, help="the live world's fixture")
    args = parser.parse_args(argv)

    corpus = json.loads(args.corpus.read_text(encoding="utf-8"))
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    if corpus.get("version") != CORPUS_VERSION:
        raise SystemExit(f"unsupported corpus version {corpus.get('version')!r}")
    if fixture.get("version") != FIXTURE_VERSION:
        raise SystemExit(f"unsupported fixture version {fixture.get('version')!r}")

    run = Run(load_sdk(args.sdk), fixture)
    results: list[dict[str, Any]] = []
    for case in corpus["cases"]:
        result = run_case(run, case)
        emit(result)
        results.append(result)
    passed = sum(1 for result in results if result["passed"])
    emit(
        {
            "summary": "wire corpus",
            "python": list(sys.version_info[:3]),
            "total": len(results),
            "passed": passed,
            "failed": len(results) - passed,
        }
    )
    return 0 if results and passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())

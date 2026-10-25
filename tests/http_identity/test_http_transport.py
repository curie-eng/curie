"""Independent real HTTP/TLS boundaries for the standalone identity client.

@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-2 HTTP-IDENTITY-TRANSPORT-3
"""

from __future__ import annotations

import asyncio
import dataclasses
import gzip
import json
import logging
import pickle
import ssl
from datetime import datetime
from typing import Any

import pytest


def test_exact_exchange_dedicated_bearer_no_ambient_state_and_no_cookie_cache(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1: ambient proxy/auth/cookies cannot enter the exchange."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        proxy = await rig.listen()
        endpoint = await rig.listen(
            payload=support.wire(
                support.response(),
                headers=[
                    (b"Content-Type", b"application/json"),
                    (b"Set-Cookie", b"ambient=forbidden"),
                ],
            )
        )
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
            monkeypatch.setenv(name, proxy.url)
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
        netrc = tmp_path / "synthetic-netrc"
        netrc.write_text("machine 127.0.0.1 login ambient password AmbientPassword\n")
        monkeypatch.setenv("NETRC", str(netrc))
        before = dict(__import__("os").environ)
        client = rig.client(module, allow_loopback_http=True)
        target = support.target(module, endpoint.url)
        with pytest.raises((AttributeError, TypeError, ValueError)):
            target.endpoint = proxy.url
        bearer = module.ProviderBearer(support.token)
        for _ in range(2):
            result = await client.lookup(
                target=target,
                bearer=bearer,
                request=support.request(),
                limits=support.limits(),
            )
            assert isinstance(result, module.HttpIdentityObservation)
            assert support.token not in repr(result)
        assert endpoint.connections == 2 and proxy.connections == 0
        for first, headers, body in endpoint.requests:
            assert first == b"POST /lookup HTTP/1.1"
            assert headers["authorization"] == "Bearer " + support.token
            assert headers["content-type"] == headers["accept"] == "application/json"
            assert headers["accept-encoding"] == "identity"
            assert "cookie" not in headers and "proxy-authorization" not in headers
            assert json.loads(body) == support.request().model_dump(mode="json")
        assert dict(__import__("os").environ) == before

    run_transport(scenario)


@pytest.mark.parametrize("failure", ["untrusted", "hostname", "cleartext"])
def test_verified_tls_positive_control_and_real_tls_negatives(
    support: Any,
    run_transport: Any,
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1: a private CA never disables peer verification."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        server_tls, trusted = rig.certificates(wrong_host=failure == "hostname")
        listener = await rig.listen(tls=None if failure == "cleartext" else server_tls)
        url = listener.url.replace("http:", "https:")
        # Default trust must not discover a private root from ambient environment.
        monkeypatch.setenv("SSL_CERT_FILE", str(rig.root / "ca.pem"))
        client = rig.client(module, **({} if failure == "untrusted" else {"tls_context": trusted}))
        result = await client.lookup(
            target=support.target(module, url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, "unavailable")
        assert not listener.requests, "TLS failures cannot send an identity POST"
        if failure == "untrusted":
            positive = rig.client(module, tls_context=trusted)
            result = await positive.lookup(
                target=support.target(module, url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
            assert isinstance(result, module.HttpIdentityObservation)
            assert listener.requests[0][1]["authorization"] == "Bearer " + support.token

    run_transport(scenario)


@pytest.mark.parametrize("insecure", ["no_hostname", "no_verification"])
def test_unverified_ssl_context_is_a_fixed_local_error(
    support: Any,
    insecure: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    module = support.api()
    context = ssl.create_default_context()
    context.check_hostname = False
    if insecure == "no_verification":
        context.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError) as error:
        module.HttpIdentityClient(tls_context=context)
    assert str(error.value) == "invalid_lookup_context"


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1/lookup",
        "http://localhost/lookup",
        "http://127.1/lookup",
        "http://2130706433/lookup",
        "http://[::ffff:127.0.0.1]/lookup",
        "http://127.0.0.2/lookup",
        "https:///lookup",
        "//127.0.0.1/lookup",
        "https://user:secret@127.0.0.1/lookup",
        "https://127.0.0.1/lookup?secret=1",
        "https://127.0.0.1/lookup#secret",
        "https://127.0.0.1:70000/lookup",
        "https://127.0.0.1:bad/lookup",
        " https://127.0.0.1/lookup",
        "https://127.0.0.1/lo okup",
        "https://127.0.0.1/lo\tookup",
        "https://127.0.0.1/lookup\n",
    ],
)
def test_endpoint_rejection_is_local_sanitized_and_never_starts_a_worker(
    support: Any,
    run_transport: Any,
    endpoint: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1: only the exact loopback opt-in permits HTTP."""
    module = support.api()
    caplog.set_level(logging.DEBUG)

    async def scenario(rig: Any) -> None:
        client = rig.client(
            module, allow_loopback_http=not endpoint.startswith("http://127.0.0.1/")
        )
        with pytest.raises(ValueError) as error:
            await client.lookup(
                target=support.target(module, endpoint),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
        assert str(error.value) == "invalid_lookup_context"
        report = client.close_status()
        assert report.pending_exchanges == report.pending_parsers == 0 and report.worker_joined

    run_transport(scenario)
    assert endpoint not in caplog.text and support.token not in caplog.text


@pytest.mark.parametrize(
    "token", ["", "white space", "bad\r\nInjected: yes", "bad\x00", "é", "a=b", "a,b"]
)
def test_invalid_bearer_has_fixed_error_without_value(
    support: Any,
    token: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1."""
    module = support.api()
    caplog.set_level(logging.DEBUG)
    with pytest.raises(ValueError) as error:
        module.ProviderBearer(token)
    assert str(error.value) == "invalid_lookup_context"
    assert not caplog.records


def test_secret_representation_serialization_and_valid_token_without_extra_length_cap(
    support: Any,
    run_transport: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1: bearer secret is not ordinary model data."""
    module = support.api()
    secret = "Synthetic" + "A" * 8192 + "+/~._-=="
    bearer = module.ProviderBearer(secret)
    representations = [repr(bearer), str(bearer), json.dumps(bearer, default=repr)]
    if hasattr(bearer, "model_dump"):
        representations.append(repr(bearer.model_dump()))
    if dataclasses.is_dataclass(bearer):
        representations.append(repr(dataclasses.asdict(bearer)))
    assert all(secret not in value for value in representations)
    with pytest.raises((AttributeError, TypeError, ValueError)):
        bearer.value = "Replacement"

    async def scenario(rig: Any) -> None:
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=bearer,
            request=support.request(),
            limits=support.limits(),
        )
        assert isinstance(result, module.HttpIdentityObservation)
        assert listener.requests[0][1]["authorization"] == "Bearer " + secret

    run_transport(scenario)


@pytest.mark.parametrize(
    "broken", ["tenant", "issuer", "generation", "request_construct", "limits_construct"]
)
def test_mismatched_or_bypassed_local_context_has_fixed_error_and_no_io(
    support: Any,
    run_transport: Any,
    broken: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1: revalidate even Pydantic model_construct inputs."""
    module = support.api()
    caplog.set_level(logging.DEBUG)

    async def scenario(rig: Any) -> None:
        listener = await rig.listen()
        client = rig.client(module, allow_loopback_http=True)
        req, budget = support.request(), support.limits()
        changes = {
            "tenant": {"tenant_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
            "issuer": {"expected_issuer": "https://other.example.test"},
            "generation": {"authority_version": 8},
        }.get(broken, {})
        if broken == "request_construct":
            req = type(req).model_construct(
                **{
                    **req.model_dump(),
                    "request_id": support.marker,
                }
            )
        if broken == "limits_construct":
            budget = type(budget).model_construct(
                max_identity_age_ms=support.marker,
                max_membership_age_ms=60_000,
                max_clock_skew_ms=0,
            )
        with pytest.raises(ValueError) as error:
            await client.lookup(
                target=support.target(module, listener.url, **changes),
                bearer=module.ProviderBearer(support.token),
                request=req,
                limits=budget,
            )
        assert str(error.value) == "invalid_lookup_context"
        assert listener.connections == 0
        report = client.close_status()
        assert report.pending_exchanges == report.pending_parsers == 0 and report.worker_joined

    run_transport(scenario)
    assert support.marker not in caplog.text and support.token not in caplog.text


@pytest.mark.parametrize("status", [301, 302, 307, 308, 401, 403, 404, 429, 500, 503])
def test_non_200_does_not_follow_redirect_or_read_hanging_body(
    support: Any,
    run_transport: Any,
    status: int,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: one exchange and no body wait for non-200."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        second = await rig.listen()
        release = rig.event()

        async def redirect(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-2."""
            writer.write(
                f"HTTP/1.1 {status} Synthetic\r\nLocation: {second.url}\r\n"
                "Content-Length: 100000\r\n\r\n".encode()
            )
            await writer.drain()
            await release.wait()

        first = await rig.listen(redirect)
        client = rig.client(module, allow_loopback_http=True)
        start = asyncio.get_running_loop().time()
        result = await client.lookup(
            target=support.target(module, first.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, "unavailable")
        assert asyncio.get_running_loop().time() - start < 0.8
        assert first.connections == 1 and second.connections == 0

    run_transport(scenario)


@pytest.mark.parametrize(
    "headers",
    [
        [],
        [(b"Content-Type", b"text/json")],
        [(b"Content-Type", b"application/problem+json")],
        [(b"Content-Type", b"application/json; charset=latin-1")],
        [(b"Content-Type", b"application/json; profile=secret")],
        [(b"Content-Type", b"application/json"), (b"Content-Type", b"application/json")],
        [(b"Content-Type", b"application/json"), (b"Content-Type", b"text/plain")],
        [(b"Content-Type", b"application/json; charset=utf-8; charset=utf-8")],
        [(b"Content-Type", b"application/json, application/json")],
    ],
)
def test_media_rejected_before_hanging_entity_is_consumed(
    support: Any,
    run_transport: Any,
    headers: list[tuple[bytes, bytes]],
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event()

        async def hanging(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-2."""
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + b"".join(key + b": " + value + b"\r\n" for key, value in headers)
                + b"Content-Length: 100\r\n\r\n"
            )
            # Send no entity: invalid headers alone must determine the refusal.
            await writer.drain()
            await release.wait()

        listener = await rig.listen(hanging)
        client = rig.client(module, allow_loopback_http=True)
        start = asyncio.get_running_loop().time()
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, "invalid_response")
        assert asyncio.get_running_loop().time() - start < 0.8

    run_transport(scenario)


@pytest.mark.parametrize(
    "media",
    [b"application/json", b'Application/JSON; Charset="UTF-8"', b"application/json; charset=utf-8"],
)
@pytest.mark.parametrize("encoding", [None, b"identity", b"  IDENTITY \t"])
def test_allowed_media_and_identity_encoding_preserve_facts(
    support: Any,
    run_transport: Any,
    media: bytes,
    encoding: bytes | None,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        headers = [(b"Content-Type", media)]
        if encoding is not None:
            headers.append((b"Content-Encoding", encoding))
        listener = await rig.listen(payload=support.wire(support.response(), headers=headers))
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        assert isinstance(result, module.HttpIdentityObservation)

    run_transport(scenario)


@pytest.mark.parametrize(
    "encodings",
    [
        [b"gzip"],
        [b"deflate"],
        [b"br"],
        [b"zstd"],
        [b"identity, identity"],
        [b"identity", b"identity"],
        [b"gzip", b"identity"],
        [b""],
    ],
)
def test_unsupported_and_duplicate_encoding_is_rejected_without_body_or_decode(
    support: Any,
    run_transport: Any,
    encodings: list[bytes],
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: compressed/bomb content never reaches a decoder."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event()

        async def encoded(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-2."""
            fields = b"".join(b"Content-Encoding: " + value + b"\r\n" for value in encodings)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                + fields
                + b"Content-Length: 100\r\n\r\n"
            )
            await writer.drain()
            await release.wait()

        listener = await rig.listen(encoded)
        client = rig.client(module, allow_loopback_http=True)
        start = asyncio.get_running_loop().time()
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, "invalid_response")
        assert asyncio.get_running_loop().time() - start < 0.8

    run_transport(scenario)


def test_compressed_valid_json_and_bomb_are_both_refused(support: Any, run_transport: Any) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: decoded valid JSON cannot bypass encoding refusal."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        client = rig.client(module, allow_loopback_http=True)
        for entity in (support.response(), b" " * 1_000_000):
            listener = await rig.listen(
                payload=support.wire(
                    gzip.compress(entity),
                    headers=[
                        (b"Content-Type", b"application/json"),
                        (b"Content-Encoding", b"gzip"),
                    ],
                )
            )
            support.refusal(
                await client.lookup(
                    target=support.target(module, listener.url),
                    bearer=module.ProviderBearer(support.token),
                    request=support.request(),
                    limits=support.limits(),
                ),
                "invalid_response",
            )

    run_transport(scenario)


@pytest.mark.parametrize("framing", ["length", "chunked", "eof"])
@pytest.mark.parametrize("size", [65_536, 65_537])
@pytest.mark.parametrize("outcome", ["resolved", "inactive", "unresolved"])
def test_raw_entity_exact_byte_boundary_across_real_framing_and_chunks(
    support: Any,
    run_transport: Any,
    framing: str,
    size: int,
    outcome: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: transfer framing never counts as entity bytes."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        body = support.response(outcome=outcome)
        body += b" " * (size - len(body))
        listener = await rig.listen(payload=support.wire(body, framing=framing))
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        if size == 65_536:
            assert isinstance(result, module.HttpIdentityObservation)
            assert isinstance(result.facts, support.facts().ValidatedFacts)
            assert result.facts.outcome == outcome
            assert result.response_bytes == body
        else:
            support.refusal(result, "invalid_response")

    run_transport(scenario)


def test_declared_oversize_and_incremental_crossing_are_rejected_before_waiting_for_end(
    support: Any,
    run_transport: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: neither framing nor no Content-Length bypasses the cap."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event()
        for prefix in (
            b"Content-Length: 65537\r\n\r\n",
            b"Transfer-Encoding: chunked\r\n\r\n10000\r\n" + b" " * 65_536 + b"\r\n1\r\n \r\n",
            b"Connection: close\r\n\r\n" + b" " * 65_537,
        ):

            async def crossing(reader: Any, writer: Any, prefix: bytes = prefix) -> None:
                """@spec HTTP-IDENTITY-TRANSPORT-2."""
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" + prefix)
                await writer.drain()
                await release.wait()

            listener = await rig.listen(crossing)
            client = rig.client(module, allow_loopback_http=True)
            start = asyncio.get_running_loop().time()
            result = await client.lookup(
                target=support.target(module, listener.url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )
            support.refusal(result, "invalid_response")
            assert asyncio.get_running_loop().time() - start < 0.8

    run_transport(scenario)


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1\r\n\r\n{",
            "invalid_response",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1\r\n\r\n\xff",
            "invalid_response",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{}",
            "unavailable",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n8\r\n{}",
            "unavailable",
        ),
        (b"HTTP/1.1 200 OK\r\nBad Header\r\n\r\n", "unavailable"),
        (b"", "unavailable"),
    ],
)
def test_complete_malformed_entity_and_incomplete_protocol_have_distinct_results(
    support: Any,
    run_transport: Any,
    payload: bytes,
    code: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: preserve the T1 versus HTTP framing boundary."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        listener = await rig.listen(payload=payload)
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, code)
        assert listener.connections == 1

    run_transport(scenario)


@pytest.mark.parametrize("outcome", ["resolved", "inactive", "unresolved", "unavailable"])
def test_complete_t1_outcome_is_preserved(support: Any, run_transport: Any, outcome: str) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2: transport adds no policy to T1 outcomes."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        listener = await rig.listen(payload=support.wire(support.response(outcome=outcome)))
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        if outcome == "unavailable":
            support.refusal(result, "unavailable")
        else:
            assert isinstance(result, module.HttpIdentityObservation)
            assert result.facts.outcome == outcome
            if outcome == "unresolved":
                assert result.facts.identity is None

    run_transport(scenario)


def test_one_monotonic_deadline_includes_header_delay_plus_body_progress(
    support: Any,
    run_transport: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3: per-read progress cannot restart two seconds."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        async def drip(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            body = support.response()
            await asyncio.sleep(1.1)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
            )
            await writer.drain()
            for part in (body[:100], body[100:200], body[200:]):
                await asyncio.sleep(0.45)
                writer.write(part)
                await writer.drain()

        listener = await rig.listen(drip)
        client = rig.client(module, allow_loopback_http=True)
        start = asyncio.get_running_loop().time()
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        elapsed = asyncio.get_running_loop().time() - start
        support.refusal(result, "unavailable")
        assert 1.8 <= elapsed < 2.35, f"aggregate lookup deadline took {elapsed:.3f}s"
        assert listener.connections == 1

    run_transport(scenario)


def test_real_tls_handshake_stall_uses_same_result_deadline(
    support: Any, run_transport: Any
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-3."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        release = rig.event()

        async def stall(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-3: accept TCP but never answer ClientHello."""
            assert await reader.read(1)
            await release.wait()

        listener = await rig.listen(stall, parse_request=False)
        client = rig.client(module)
        start = asyncio.get_running_loop().time()
        result = await client.lookup(
            target=support.target(module, listener.url.replace("http:", "https:")),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, "unavailable")
        elapsed = asyncio.get_running_loop().time() - start
        assert 1.8 <= elapsed < 2.35

    run_transport(scenario)


@pytest.mark.parametrize("outcome", ["resolved", "inactive", "unresolved"])
@pytest.mark.parametrize("at_cap", [False, True])
def test_one_t1_call_hands_off_same_exact_noncanonical_multichunk_entity(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    at_cap: bool,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2 HTTP-IDENTITY-TRANSPORT-3: no reconstruction."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        value = json.loads(support.response(outcome=outcome))
        body = json.dumps(dict(reversed(list(value.items()))), indent=3).encode()
        body = b"\n " + body.replace(b'"outcome"', b'"\\u006futcome"') + b"\r\n\t "
        if at_cap:
            body += b" " * (65_536 - len(body))
            assert len(body) == 65_536
        assert json.loads(body) == value
        assert body != json.dumps(value, separators=(",", ":")).encode()
        original = support.facts().validate_lookup
        seen: list[tuple[bytes, Any, Any, datetime, Any]] = []

        def parse(raw: bytes, request: Any, limits: Any, *, now: datetime) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3: observe the sole real pure T1 call."""
            facts = original(raw, request, limits, now=now)
            seen.append((raw, request, limits, now, facts))
            return facts

        monkeypatch.setattr(module, "validate_lookup", parse)

        async def chunks(reader: Any, writer: Any) -> None:
            """@spec HTTP-IDENTITY-TRANSPORT-2: actual separate HTTP/1.1 transfer chunks."""
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
            )
            for part in (body[:7], body[7:300], body[300:32_000], body[32_000:]):
                if part:
                    writer.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                    await writer.drain()
                    await asyncio.sleep(0)
            writer.write(b"0\r\n\r\n")
            await writer.drain()

        listener = await rig.listen(chunks)
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        assert isinstance(result, module.HttpIdentityObservation)
        assert len(seen) == 1
        raw, request, limits, now, facts = seen[0]
        assert isinstance(facts, support.facts().ValidatedFacts)
        assert result.facts is facts and result.response_bytes is raw
        assert raw == body and result.facts.outcome == outcome
        independently_checked = original(body, request, limits, now=now)
        assert isinstance(independently_checked, support.facts().ValidatedFacts)
        assert result.facts.model_dump() == independently_checked.model_dump()
        assert listener.connections == 1
        await client.aclose()
        assert result.response_bytes is raw and result.response_bytes == body
        assert result.facts is facts

    run_transport(scenario)


def test_concurrent_and_later_observations_never_cross_pair_or_overwrite_evidence(
    support: Any,
    run_transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2 HTTP-IDENTITY-TRANSPORT-4: per-exchange ownership."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        original = support.facts().validate_lookup
        seen: list[tuple[bytes, Any]] = []
        bodies: list[bytes] = []
        for subject in ("FirstIndependentSubject", "SecondIndependentSubject"):
            value = json.loads(support.response())
            value["identity"]["subject"] = subject
            bodies.append(b" \n" + json.dumps(value, indent=2).encode() + b"\t\n")

        def parse(raw: bytes, *args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-3."""
            facts = original(raw, *args, **kwargs)
            seen.append((raw, facts))
            return facts

        monkeypatch.setattr(module, "validate_lookup", parse)
        listeners = [await rig.listen(payload=support.wire(body)) for body in bodies]
        client = rig.client(module, allow_loopback_http=True)

        async def lookup(index: int) -> Any:
            """@spec HTTP-IDENTITY-TRANSPORT-2."""
            return await client.lookup(
                target=support.target(module, listeners[index].url),
                bearer=module.ProviderBearer(support.token),
                request=support.request(),
                limits=support.limits(),
            )

        first, second = await asyncio.gather(lookup(0), lookup(1))
        later = await lookup(0)
        assert len(seen) == 3
        observations = (first, second, later)
        for observation, body in zip(observations, (bodies[0], bodies[1], bodies[0]), strict=True):
            assert isinstance(observation, module.HttpIdentityObservation)
            assert observation.response_bytes == body
            assert observation.facts.identity.subject == json.loads(body)["identity"]["subject"]
            assert (
                sum(
                    observation.facts is facts and observation.response_bytes is raw
                    for raw, facts in seen
                )
                == 1
            )
        assert first is not second and later is not first
        assert first != second and first != later
        assert first == first and len({first, second, later}) == 3
        assert hash(first) == hash(first) == object.__hash__(first)
        assert [listener.connections for listener in listeners] == [2, 1]
        await client.aclose()
        assert all(
            observation.response_bytes == body
            for observation, body in zip(
                observations, (bodies[0], bodies[1], bodies[0]), strict=True
            )
        )

    run_transport(scenario)


def test_observation_is_opaque_immutable_and_only_explicit_access_retains_sensitive_facts(
    support: Any,
    run_transport: Any,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-1 HTTP-IDENTITY-TRANSPORT-5."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        value = json.loads(support.response())
        value["identity"]["subject"] = support.token + support.marker
        value["membership"]["groups"][0]["id"] = support.marker
        body = json.dumps(value, indent=2).encode()
        listener = await rig.listen(payload=support.wire(body))
        client = rig.client(module, allow_loopback_http=True)
        observation = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        assert isinstance(observation, module.HttpIdentityObservation)
        assert isinstance(observation.facts, support.facts().ValidatedFacts)
        assert type(observation.response_bytes) is bytes
        assert observation.response_bytes == body
        assert observation.facts.identity.subject == support.token + support.marker
        assert str(observation) == repr(observation) == "HttpIdentityObservation()"
        assert not dataclasses.is_dataclass(observation)
        for name in (
            "__dict__",
            "__iter__",
            "__bytes__",
            "model_dump",
            "to_dict",
            "outcome",
            "identity",
            "request",
            "groups",
        ):
            assert not hasattr(observation, name)
        for name, replacement in (("facts", None), ("response_bytes", b"replacement")):
            with pytest.raises((AttributeError, TypeError)) as error:
                setattr(observation, name, replacement)
            assert support.token not in str(error.value) and support.marker not in str(error.value)
            with pytest.raises((AttributeError, TypeError)):
                delattr(observation, name)
        facts = observation.facts
        mutations = [
            (facts, "outcome", "unresolved"),
            (facts.request, "authority_version", 99),
            (facts.request.scope, "expected_issuer", "https://other.example.test"),
            (facts.request.scope.sender, "native_id", "OtherAccount"),
            (facts.request.scope.sender.namespace, "key", "OtherNamespace"),
            (facts.identity, "subject", "OtherSubject"),
            (facts.groups[0], "id", "OtherGroup"),
            (facts.identity_observation, "as_of", "2000-01-01T00:00:00Z"),
            (facts.membership_observation, "as_of", "2000-01-01T00:00:00Z"),
        ]
        for model, field, replacement in mutations:
            with pytest.raises((AttributeError, TypeError, ValueError)):
                setattr(model, field, replacement)
        assert isinstance(facts.groups, tuple)
        with pytest.raises(TypeError):
            facts.groups[0] = facts.groups[0]
        for args, kwargs in [
            ((), {}),
            ((facts, body), {}),
            ((), {"facts": facts, "response_bytes": body}),
        ]:
            with pytest.raises(TypeError) as error:
                module.HttpIdentityObservation(*args, **kwargs)
            assert str(error.value) == "identity_observation_internal"
            assert error.value.__cause__ is None
            assert error.value.__context__ is None
        for state_conversion in [
            observation.__getstate__,
            observation.__reduce__,
            *[
                lambda protocol=protocol: observation.__reduce_ex__(protocol)
                for protocol in range(6)
            ],
            *[
                lambda protocol=protocol: pickle.dumps(observation, protocol=protocol)
                for protocol in range(6)
            ],
        ]:
            with pytest.raises(TypeError) as error:
                state_conversion()
            assert str(error.value) == "identity_observation_not_serializable"
            assert error.value.__cause__ is None and error.value.__context__ is None
        for conversion in (vars, dict, list, bytes, dataclasses.asdict, json.dumps):
            with pytest.raises((TypeError, AttributeError)) as error:
                conversion(observation)
            assert support.token not in str(error.value) and support.marker not in str(error.value)
            assert support.token not in repr(error.value) and support.marker not in repr(
                error.value
            )
        assert json.dumps(observation, default=repr) == '"HttpIdentityObservation()"'
        assert json.dumps(observation, default=str) == '"HttpIdentityObservation()"'
        assert support.token not in repr(client.close_status())
        assert support.marker not in repr(client.close_status())
        assert all(
            support.token not in task.get_name() and support.marker not in task.get_name()
            for task in asyncio.all_tasks()
        )
        await client.aclose()
        assert observation.facts is facts and observation.response_bytes == body
        assert observation.facts.identity.subject == support.token + support.marker

    run_transport(scenario)


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        ("malformed", "invalid_response"),
        ("scoped", "scope_mismatch"),
        ("unsupported", "unsupported_version"),
        ("unavailable", "unavailable"),
        ("identity_stale", "identity_stale"),
        ("membership_stale", "membership_stale"),
    ],
)
def test_t1_refusals_withhold_all_accepted_evidence(
    support: Any,
    run_transport: Any,
    failure: str,
    code: str,
) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-2 HTTP-IDENTITY-TRANSPORT-3."""
    module = support.api()

    async def scenario(rig: Any) -> None:
        value = json.loads(
            support.response(outcome="unavailable" if failure == "unavailable" else "resolved")
        )
        if failure == "scoped":
            value["request_id"] = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        elif failure == "unsupported":
            value["schema_version"] = 2
        elif failure == "identity_stale":
            value["identity_observation"]["as_of"] = "2000-01-01T00:00:00Z"
        elif failure == "membership_stale":
            value["membership"]["observation"]["as_of"] = "2000-01-01T00:00:00Z"
        body = (
            json.dumps(value).encode()
            if failure != "malformed"
            else b'{"secret":"' + support.marker.encode()
        )
        listener = await rig.listen(payload=support.wire(body))
        client = rig.client(module, allow_loopback_http=True)
        result = await client.lookup(
            target=support.target(module, listener.url),
            bearer=module.ProviderBearer(support.token),
            request=support.request(),
            limits=support.limits(),
        )
        support.refusal(result, code)
        assert support.marker not in repr(result) and support.token not in repr(result)
        assert listener.connections == 1

    run_transport(scenario)

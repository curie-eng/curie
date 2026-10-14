"""Fresh external TLS peer proves transport refusal, never launch qualification."""

import asyncio
import hashlib
import ssl
from datetime import UTC, datetime, timedelta

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from curie_worker.protected_launch_client import (
    LaunchClientError,
    LaunchTLSConfig,
    _LaunchTransport,
    _ProtectedTurnStream,
)


def credentials(tmp_path):
    keys = [rsa.generate_private_key(public_exponent=65537, key_size=2048) for _ in range(3)]
    names = [
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        for name in ("CA", "localhost", "worker")
    ]
    certificates = []
    for i, key in enumerate(keys):
        builder = (
            x509.CertificateBuilder()
            .subject_name(names[i])
            .issuer_name(names[0])
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
            .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
            .add_extension(x509.BasicConstraints(ca=i == 0, path_length=None), critical=True)
        )
        if i == 1:
            builder = builder.add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
            )
        certificate = builder.sign(keys[0], hashes.SHA256())
        certificates.append(certificate)
        (tmp_path / f"{i}.crt").write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        (tmp_path / f"{i}.key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
    pin = hashlib.sha256(
        keys[1]
        .public_key()
        .public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    ).hexdigest()
    return pin


@pytest.mark.parametrize(
    "failure",
    ["pin", "name", "ca", "client", "redirect", "oversize", "duplicate", "slow", "broken-stream"],
)
def test_real_tls_refusals_do_not_disclose_diagnostics(tmp_path, failure):
    asyncio.run(_exercise(tmp_path, failure))


async def _exercise(tmp_path, failure):
    pin = credentials(tmp_path)
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    credentials(foreign)
    received = []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "1.crt", tmp_path / "1.key")
    context.load_verify_locations(tmp_path / "0.crt")
    context.verify_mode = ssl.CERT_REQUIRED

    async def peer(reader, writer):
        try:
            data = await reader.readuntil(b"\r\n\r\n")
            received.append(data)
            if failure == "slow":
                await asyncio.sleep(0.2)
            body = b'{"schema_version":1,"schema_version":1}'
            if failure == "broken-stream":
                body = b"not-json\n"
            status = b"200 OK"
            extra = b""
            if failure == "redirect":
                status = b"302 Found"
                extra = b"Location: https://secret.example.com/\r\n"
            if failure == "oversize":
                extra += b"Content-Length: 99999999\r\n"
            else:
                extra += f"Content-Length: {len(body)}\r\n".encode()
            writer.write(
                b"HTTP/1.1 " + status + b"\r\n" + extra + b"Connection: close\r\n\r\n" + body
            )
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(peer, "127.0.0.1", 0, ssl=context)
    port = server.sockets[0].getsockname()[1]
    config = LaunchTLSConfig(
        origin=f"https://127.0.0.1:{port}",
        ca_file=str((foreign if failure == "ca" else tmp_path) / "0.crt"),
        server_name="wrong.example.com" if failure == "name" else "localhost",
        server_spki_sha256="0" * 64 if failure == "pin" else pin,
        client_certificate=str((foreign if failure == "client" else tmp_path) / "2.crt"),
        client_key=str((foreign if failure == "client" else tmp_path) / "2.key"),
        worker_principal_id="worker-example",
    )
    try:
        async with _LaunchTransport(config) as client:
            if failure == "broken-stream":
                retired = []

                async def retire():
                    retired.append(True)

                response = await client.request("POST", "/v1/launches", b"test", {}, None)
                stream = _ProtectedTurnStream(
                    response, 1, asyncio.get_running_loop().time() + 1, "A" * 43, None, retire
                )
                with pytest.raises(ValueError):
                    async with stream:
                        async for _ in stream:
                            pass
                assert retired == [True]
                assert len(received) == 1
                return
            with pytest.raises(LaunchClientError) as caught:
                await client.control(
                    "POST", "/v1/launches", b"private-body", {}, 0.05 if failure == "slow" else None
                )
            assert str(caught.value) == "Protected launch request refused"
            assert caught.value.__cause__ is None
            assert caught.value.__context__ is None
        if failure in {"pin", "name", "ca", "client"}:
            assert received == []
        else:
            assert len(received) == 1
    finally:
        server.close()
        await server.wait_closed()


# These fixtures seed only local structural ownership state. The fresh TLS peer
# is a hostile external endpoint, never a registry issuer or qualification source.
async def _control_peer(tmp_path, handler):
    pin = credentials(tmp_path)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "1.crt", tmp_path / "1.key")
    context.load_verify_locations(tmp_path / "0.crt")
    context.verify_mode = ssl.CERT_REQUIRED
    tasks, writers = set(), set()

    async def peer(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        writers.add(writer)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode().split("\r\n")
            method, path, _ = lines[0].split(" ")
            headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
            body = await reader.readexactly(int(headers.get("Content-Length", 0)))
            import json

            data = json.loads(body) if body else None
            status, response = await handler(method, path, data)
            raw = json.dumps(response).encode()
            writer.write(
                (
                    f"HTTP/1.1 {status} Response\r\nContent-Length: {len(raw)}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode()
                + raw
            )
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            tasks.discard(task)

    server = await asyncio.start_server(peer, "127.0.0.1", 0, ssl=context)
    port = server.sockets[0].getsockname()[1]
    config = LaunchTLSConfig(
        f"https://127.0.0.1:{port}",
        str(tmp_path / "0.crt"),
        "localhost",
        pin,
        str(tmp_path / "2.crt"),
        str(tmp_path / "2.key"),
        "worker-example",
    )

    async def close():
        server.close()
        await server.wait_closed()
        for writer in list(writers):
            writer.close()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*list(tasks), return_exceptions=True)

    return config, close


def _structural_owned_client(config, *, receipt=False, revision="9", deadline=None):
    from curie_worker.protected_launch_client import ProtectedLaunchClient, _OwnedLaunch
    from curie_worker.protected_launch_records import (
        AuthenticatedLaunchObservation,
        LaunchObservation,
        LaunchRequest,
        WorkerLaunchCapability,
        canonical_json,
    )

    from apps.worker.tests.test_protected_history import dispatchable
    from apps.worker.tests.test_protected_launch_records import receipt_value, request_value

    capability = WorkerLaunchCapability(
        config.origin, "77777777-7777-4777-8777-777777777777", "worker-example", "A" * 43
    )
    selected_receipt = receipt_value() if receipt else None
    if selected_receipt is not None:
        selected_receipt["route"]["worker_principal_id"] = "worker-example"
    observation = AuthenticatedLaunchObservation(
        config.origin,
        "worker-example",
        LaunchObservation(
            canonical_json(
                dict(
                    schema_version=1,
                    launch_id=capability.launch_id,
                    receipt_revision=revision,
                    state="idle" if receipt else "preparing",
                    receipt=selected_receipt,
                )
            )
        ),
    )
    owned = _OwnedLaunch(
        capability,
        LaunchRequest(canonical_json(request_value())),
        dispatchable(),
        observation,
        deadline or asyncio.get_running_loop().time() + 30,
        epoch="A" * 43,
    )
    client = object.__new__(ProtectedLaunchClient)
    client._transport = _LaunchTransport(config)
    client._tls = config
    client._owned = {capability.launch_id: owned}
    return client, owned


@pytest.mark.parametrize("receipt,revision", [(True, "4"), (True, "10"), (False, "4")])
def test_null_observation_never_erases_issued_anchor_or_revision(tmp_path, receipt, revision):
    async def go():
        observations = []

        async def handler(method, path, body):
            observations.append(path)
            current_receipt = None
            if receipt and len(observations) == 2:
                from apps.worker.tests.test_protected_launch_records import receipt_value

                current_receipt = receipt_value()
                current_receipt["route"]["worker_principal_id"] = "worker-example"
                current_receipt["workload"]["pod_uid"] = "99999999-9999-4999-8999-999999999999"
            return 200, dict(
                schema_version=1,
                launch_id="77777777-7777-4777-8777-777777777777",
                receipt_revision="10" if current_receipt else revision,
                state="idle" if current_receipt else "preparing",
                receipt=current_receipt,
            )

        config, close = await _control_peer(tmp_path, handler)
        client, owned = _structural_owned_client(config, receipt=receipt)
        prior = owned.observation
        try:
            with pytest.raises(LaunchClientError):
                await client.observe(owned.capability)
            assert owned.observation is prior
            if receipt:
                with pytest.raises(LaunchClientError):
                    await client.observe(owned.capability)
                assert owned.observation is prior
        finally:
            await client._transport.close()
            await close()

    asyncio.run(go())


def test_cancelled_positive_revalidation_fences_and_requests_owned_retire(tmp_path):
    async def go():
        reading = asyncio.Event()
        blocked = asyncio.Event()
        retired = []

        async def handler(method, path, body):
            if method == "GET":
                reading.set()
                await blocked.wait()
                raise AssertionError("canceled response must not be consumed")
            assert path.endswith("/retire")
            retired.append(path)
            return 202, dict(
                schema_version=1,
                launch_id=path.split("/")[3],
                operation_id=body["operation_id"],
                state="retiring",
            )

        config, close = await _control_peer(tmp_path, handler)
        client, owned = _structural_owned_client(config)
        task = asyncio.create_task(client.admit_turn(owned.capability, owned.epoch, allow=True))
        try:
            async with asyncio.timeout(2):
                await reading.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert owned.positive is False
            assert len(retired) == 1
            assert client._owned[owned.capability.launch_id] is owned
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client._transport.close()
            await close()

    asyncio.run(go())


@pytest.mark.parametrize("remaining", [0.05, 0.0])
def test_positive_admit_entire_revalidation_uses_original_caller_budget(tmp_path, remaining):
    async def go():
        reads, retires = [], []

        async def handler(method, path, body):
            if method == "GET":
                reads.append(path)
                await asyncio.sleep(0.35)
                return 200, dict(
                    schema_version=1,
                    launch_id=path.split("/")[3],
                    receipt_revision="9",
                    state="preparing",
                    receipt=None,
                )
            assert path.endswith("/retire")
            retires.append(path)
            return 202, dict(
                schema_version=1,
                launch_id=path.split("/")[3],
                operation_id=body["operation_id"],
                state="retiring",
            )

        config, close = await _control_peer(tmp_path, handler)
        client, owned = _structural_owned_client(config)
        try:
            before = asyncio.get_running_loop().time()
            with pytest.raises(LaunchClientError):
                await client.admit_turn(
                    owned.capability, owned.epoch, allow=True, remaining_s=remaining
                )
            assert asyncio.get_running_loop().time() - before < 0.2
            assert owned.positive is False
            assert len(retires) == 1
            assert len(reads) == (1 if remaining else 0)
        finally:
            await client._transport.close()
            await close()

    asyncio.run(go())


@pytest.mark.parametrize("remaining", [None, 2.0])
def test_positive_control_body_cannot_outlive_owned_absolute_deadline(tmp_path, remaining):
    """Private control framing bound only; no complete positive guard is faked."""
    from curie_worker.protected_launch_records import AdmitRequest, canonical_json

    async def go():
        accepted = []

        async def handler(method, path, body):
            assert path.endswith("/admit") and body["allow"] is True
            accepted.append(path)
            await asyncio.sleep(0.35)
            return 200, dict(
                schema_version=1,
                launch_id=path.split("/")[3],
                operation_id=body["operation_id"],
                epoch=body["epoch"],
                result="applied",
            )

        config, close = await _control_peer(tmp_path, handler)
        client, owned = _structural_owned_client(config)
        owned.deadline = asyncio.get_running_loop().time() + 0.05
        request = AdmitRequest(
            canonical_json(
                dict(
                    schema_version=1,
                    operation_id="77777777-7777-4777-8777-777777777777",
                    expected_receipt_revision="9",
                    epoch=owned.epoch,
                    allow=True,
                )
            )
        )
        try:
            before = asyncio.get_running_loop().time()
            with pytest.raises(LaunchClientError):
                await client._applied(owned.capability, "admit", request, remaining)
            assert asyncio.get_running_loop().time() - before < 0.2
            assert accepted == [f"/v1/launches/{owned.capability.launch_id}/admit"]
        finally:
            await client._transport.close()
            await close()

    asyncio.run(go())


def test_cancelled_outbound_deny_admit_retains_fenced_owned_cleanup(tmp_path):
    async def go():
        accepted, retired = asyncio.Event(), []
        blocked = asyncio.Event()

        async def handler(method, path, body):
            if path.endswith("/admit"):
                assert body["allow"] is False
                accepted.set()
                await blocked.wait()
                raise AssertionError("canceled deny response must not be consumed")
            assert path.endswith("/retire")
            retired.append(path)
            return 202, dict(
                schema_version=1,
                launch_id=path.split("/")[3],
                operation_id=body["operation_id"],
                state="retiring",
            )

        config, close = await _control_peer(tmp_path, handler)
        client, owned = _structural_owned_client(config)
        task = asyncio.create_task(client.admit_turn(owned.capability, owned.epoch, allow=False))
        try:
            async with asyncio.timeout(2):
                await accepted.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert owned.positive is False
            assert len(retired) == 1
            assert client._owned[owned.capability.launch_id] is owned
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client._transport.close()
            await close()

    asyncio.run(go())


def test_owned_deny_still_works_after_positive_absolute_deadline(tmp_path):
    async def go():
        accepted = []

        async def handler(method, path, body):
            assert path.endswith("/admit") and body["allow"] is False
            accepted.append(path)
            return 200, dict(
                schema_version=1,
                launch_id=path.split("/")[3],
                operation_id=body["operation_id"],
                epoch=body["epoch"],
                result="applied",
            )

        config, close = await _control_peer(tmp_path, handler)
        client, owned = _structural_owned_client(config)
        owned.deadline = asyncio.get_running_loop().time() - 1
        owned.positive = False
        try:
            await client.admit_turn(owned.capability, owned.epoch, allow=False)
            assert len(accepted) == 1
            assert owned.positive is False
        finally:
            await client._transport.close()
            await close()

    asyncio.run(go())

"""Hosted endpoint selection preserves proxy authority across deployment backends."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from curie_internal.connector_endpoints import ConnectorEndpointBinding
from curie_runner import RunnerConfig
from curie_runner.connectors import (
    derive_mcp_servers,
    materialize_connector_caller_headers,
    upload_endpoints,
)
from curie_runner.route_callers import connector_urls, route_callers

_SCOPE = {"release": None, "agent": "acme-dev", "namespace": None}


def _binding(**endpoint: str) -> ConnectorEndpointBinding:
    return ConnectorEndpointBinding.model_validate(
        {"version": 1, "agent": "acme-dev", "connectors": {"reviewer": endpoint}}
    )


def _bundle(root: Path) -> Path:
    (root / "connectors.yaml").write_text(
        "connectors:\n  reviewer:\n    image: example/reviewer:1\n"
        "    unhosted_url: https://untrusted.example.com/mcp\n"
        "    routes:\n      - path: /v1/review\n        max_body_bytes: 1024\n"
        "        callers: [review-tools]\n"
    )
    return root


def test_static_hosted_entries_keep_caller_and_conditional_grant(tmp_path: Path) -> None:
    binding = _binding(base_url="https://connectors.example.com/acme-dev/reviewer")
    root = _bundle(tmp_path)
    for grant in ("", "ccg.test"):
        source = {"CURIE_CONNECTOR_CALLER_TOKEN": "cct.test"}
        if grant:
            source["CURIE_CONNECTOR_TOOL_GRANT"] = grant
        entries = derive_mcp_servers(
            root, **_SCOPE, endpoints=binding, caller_header=True, env=source
        )
        assert entries["reviewer"]["url"] == (
            "https://connectors.example.com/acme-dev/reviewer/mcp"
        )
        materialize_connector_caller_headers(entries, source)
        assert entries["reviewer"]["headers"]["X-Curie-Caller"] == "cct.test"
        assert "CURIE_CONNECTOR_CALLER_TOKEN" not in source
        assert entries["reviewer"]["headers"].get("X-Curie-Connector-Grant") == (
            "${CURIE_CONNECTOR_TOOL_GRANT}" if grant else None
        )


def test_static_missing_endpoint_never_uses_unhosted_url(tmp_path: Path) -> None:
    empty = ConnectorEndpointBinding(version=1, agent="acme-dev", connectors={})
    with pytest.raises(ValueError, match="missing hosted connector endpoint"):
        derive_mcp_servers(_bundle(tmp_path), **_SCOPE, endpoints=empty)


def test_routes_share_static_map_and_deployment_restrictions(tmp_path: Path) -> None:
    binding = _binding(base_url="https://connectors.example.com/acme-dev/reviewer")
    root = _bundle(tmp_path)
    assert connector_urls(root, **_SCOPE, endpoints=binding) == {
        "reviewer": "https://connectors.example.com/acme-dev/reviewer"
    }
    assert route_callers(root, agent="acme-dev") == {"review-tools": frozenset({"reviewer"})}
    (root / "deploy.yaml").write_text(
        "targets:\n  dev:\n    agent: acme-dev\n    env: dev\n"
        "    slack_channel: C0EXAMPLE1\n    connectors: []\n"
    )
    assert derive_mcp_servers(root, **_SCOPE, endpoints=binding) == {}
    assert connector_urls(root, **_SCOPE, endpoints=binding) == {}
    assert route_callers(root, agent="acme-dev") == {}


def test_upload_capability_contains_only_declared_upload_urls(tmp_path: Path) -> None:
    binding = _binding(
        base_url="https://connectors.example.com/reviewer",
        upload_url="https://uploads.example.com/reviewer/v1/upload",
    )
    root = _bundle(tmp_path)
    assert upload_endpoints(root, agent="acme-dev", endpoints=binding) == {
        "reviewer": "https://uploads.example.com/reviewer/v1/upload"
    }
    assert upload_endpoints(root, agent="acme-dev", endpoints=None) == {}
    entries = derive_mcp_servers(root, **_SCOPE, endpoints=binding, caller_header=True)
    assert "uploads.example.com" not in json.dumps(entries)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com",
        "https://user:pass@example.com",
        "https://example.com/?token=x",
        "https://example.com/#x",
        "https://example.com/\n",
    ],
)
def test_invalid_static_urls_fail_at_boot(url: str) -> None:
    with pytest.raises(ValueError):
        _binding(base_url=url)


def test_static_boot_needs_no_kubernetes_scope() -> None:
    binding = _binding(base_url="https://connectors.example.com/reviewer")
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": "/bundle",
            "CURIE_SESSION_ID": "test",
            "CURIE_SANDBOX_ID": "test",
            "CURIE_BUDGET": '{"max_output_tokens_per_run":100,"max_usd_per_day":1.0}',
            "CURIE_HOSTED_CONNECTOR_ENDPOINTS": binding.model_dump_json(),
            "CURIE_CONNECTOR_CALLER_TOKEN": "cct.test",
        }
    )
    assert config.connector_agent == "acme-dev"
    assert config.connector_release is None and config.connector_namespace is None
    assert config.connector_endpoints == binding
    assert config.connector_caller_token == "cct.test"


@pytest.fixture
def tls_certificate(tmp_path: Path) -> tuple[Path, Path]:
    import ipaddress
    from datetime import UTC, datetime, timedelta

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_file, key_file = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_file, key_file


@pytest.mark.parametrize("trusted", [True, False])
def test_static_https_uses_validated_mcp_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tls_certificate: tuple[Path, Path],
    trusted: bool,
) -> None:
    import asyncio
    import ssl
    import traceback

    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from curie_runner.mcp_tool_capability import server_streams
    from mcp import ClientSession

    cert_file, key_file = tls_certificate
    if trusted:
        monkeypatch.setenv("SSL_CERT_FILE", str(cert_file))
    else:
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file, key_file)
    seen: list[str] = []

    async def handle(request: web.Request) -> web.Response:
        if request.method != "POST":
            return web.Response(status=405)
        message = await request.json()
        seen.append(request.headers["X-Curie-Caller"])
        if "id" not in message:
            return web.Response(status=202)
        return web.json_response(
            {
                "jsonrpc": "2.0",
                "id": message["id"],
                "result": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "serverInfo": {"name": "example", "version": "1"},
                },
            }
        )

    async def go() -> None:
        app = web.Application()
        app.router.add_route("*", "/mcp", handle)
        endpoint = TestServer(app, scheme="https")
        await endpoint.start_server(ssl=context)
        try:
            binding = _binding(base_url=str(endpoint.make_url("/")).rstrip("/"))
            entries = derive_mcp_servers(
                _bundle(tmp_path),
                **_SCOPE,
                endpoints=binding,
                caller_header=True,
            )
            materialize_connector_caller_headers(
                entries, {"CURIE_CONNECTOR_CALLER_TOKEN": "cct.test"}
            )

            async def initialize() -> None:
                async with server_streams(
                    entries["reviewer"], plugin_dir=None, inherited_env={}
                ) as (read, write):
                    async with ClientSession(read, write) as session:
                        result = await session.initialize()
                        assert result.server_info.name == "example"

            if trusted:
                await initialize()
                assert seen and set(seen) == {"cct.test"}
            else:
                with pytest.raises(Exception) as refused:
                    await initialize()
                assert "CERTIFICATE_VERIFY_FAILED" in "".join(
                    traceback.format_exception(refused.value)
                )
                assert not seen
        finally:
            await endpoint.close()

    asyncio.run(go())


@pytest.mark.skipif(
    os.environ.get("CURIE_E2E_LIVE") != "1",
    reason="set CURIE_E2E_LIVE=1 for the real provider static connector proof",
)
def test_live_worker_boot_reaches_static_proxy_and_exposes_upload_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    import time
    import uuid

    import anyio
    from aci_protocol import Event, Final, SessionStatus, parse_ndjson
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from curie_connector_proxy import server
    from curie_internal.connector_endpoints import StaticConnectorEndpoints
    from curie_runner.__main__ import build_runner
    from curie_worker.binding import BindingResolver, ResolvedDeployment
    from curie_worker.config import WorkerConfig
    from nacl.signing import SigningKey

    seed = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
    public = SigningKey(bytes(range(32))).verify_key
    seen: list[dict] = []
    callers: list[bool] = []
    nonce = uuid.uuid4().hex

    async def handle(request: web.Request) -> web.Response:
        if request.method != "POST":
            return web.Response(status=405)
        message = await request.json()
        assert "X-Curie-Caller" not in request.headers
        seen.append({"method": message["method"]})
        if "id" not in message:
            return web.Response(status=202)
        method = message["method"]
        if method == "initialize":
            result = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "example", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "inspect_value",
                        "description": "Read the test value.",
                        "inputSchema": {"type": "object", "properties": {}},
                        "annotations": {"readOnlyHint": True},
                    }
                ]
            }
        elif method == "tools/call":
            assert message["params"]["name"] == "inspect_value"
            result = {"content": [{"type": "text", "text": nonce}]}
        else:
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {"code": -32601, "message": "Method not found"},
                }
            )
        return web.json_response({"jsonrpc": "2.0", "id": message["id"], "result": result})

    async def go() -> None:
        app = web.Application()
        app.router.add_route("*", "/mcp", handle)
        async with TestServer(app) as upstream:
            proxy_app = server.make_app(
                server.ProxyConfig(
                    listen_port=8480,
                    upstream_port=upstream.port,
                    public_keys=(public,),
                    admits=frozenset({"acme-dev"}),
                )
            )

            @web.middleware
            async def observe_caller(request: web.Request, handler):
                if request.method == "POST":
                    callers.append(request.headers.get("X-Curie-Caller", "").startswith("cct."))
                return await handler(request)

            proxy_app.middlewares.insert(0, observe_caller)
            async with TestServer(proxy_app) as proxy:
                binding = BindingResolver.__new__(BindingResolver)
                base = str(proxy.make_url("/")).rstrip("/")
                upload = base + "/v1/upload"
                binding._config = WorkerConfig(
                    api_key="",
                    bundle_plugin_dir=str(tmp_path),
                    connector_caller_signing_key=seed,
                    connector_endpoints=StaticConnectorEndpoints.model_validate(
                        {
                            "version": 1,
                            "agents": {
                                "acme-dev": {
                                    "reviewer": {
                                        "base_url": base,
                                        "upload_url": upload,
                                    }
                                }
                            },
                        }
                    ),
                )
                resolved = ResolvedDeployment(
                    agent_name="acme-dev",
                    agent_id=uuid.uuid4(),
                    version_id=uuid.uuid4(),
                    version_label="v1",
                    bundle_ref="bundles/example.zip",
                    max_usd_per_day=1.0,
                    max_output_tokens_per_run=4096,
                )
                env = binding.boot_env(resolved, "static-proof")
                env["CURIE_SANDBOX_ID"] = "test-sandbox"
                env["CURIE_MAX_TURNS"] = "4"
                for key, value in env.items():
                    monkeypatch.setenv(key, value)
                (tmp_path / ".claude-plugin").mkdir()
                (tmp_path / ".claude-plugin" / "plugin.json").write_text('{"name":"example"}')
                (tmp_path / "connectors.yaml").write_text(
                    "connectors:\n  reviewer:\n    image: example/reviewer:1\n"
                )
                runner = await anyio.to_thread.run_sync(
                    lambda: build_runner(RunnerConfig.from_env(env))
                )
                assert json.loads(os.environ["CURIE_CONNECTOR_UPLOAD_ENDPOINTS"]) == {
                    "reviewer": upload,
                }
                assert "CURIE_CONNECTOR_CALLER_TOKEN" not in os.environ
                await runner.start()
                try:
                    lines = [
                        line
                        async for line in runner.run_turn(
                            Event(
                                type="message",
                                user="U0EXAMPLE1",
                                ts=str(time.time()),
                                text=(
                                    "Call mcp__reviewer__inspect_value exactly once and reply "
                                    "with its value. Use no other tools."
                                ),
                            )
                        )
                    ]
                finally:
                    await runner.close()
                finals = [
                    event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)
                ]
                assert len(finals) == 1 and finals[0].status is SessionStatus.DONE
                assert nonce in finals[0].text
                assert sum(item["method"] == "tools/call" for item in seen) == 1
                assert callers and all(callers)

    asyncio.run(go())


def test_endpoint_identity_cannot_be_selected_by_a_caller(tmp_path: Path) -> None:
    binding = _binding(base_url="https://connectors.example.com/reviewer")
    root = _bundle(tmp_path)
    for resolve in (derive_mcp_servers, connector_urls):
        with pytest.raises(ValueError, match="authenticated agent"):
            resolve(root, release=None, agent="acme-prod", namespace=None, endpoints=binding)


@pytest.mark.parametrize("token", [None, "", "   "])
def test_static_runner_boot_requires_a_caller_token(token: str | None) -> None:
    binding = _binding(base_url="https://connectors.example.com/reviewer")
    env = {
        "CURIE_PLUGIN_DIR": "/bundle",
        "CURIE_SESSION_ID": "test",
        "CURIE_SANDBOX_ID": "test",
        "CURIE_BUDGET": '{"max_output_tokens_per_run":100,"max_usd_per_day":1.0}',
        "CURIE_HOSTED_CONNECTOR_ENDPOINTS": binding.model_dump_json(),
    }
    if token is not None:
        env["CURIE_CONNECTOR_CALLER_TOKEN"] = token
    with pytest.raises(ValueError, match="caller token"):
        RunnerConfig.from_env(env)


def test_static_runner_boot_refuses_duplicate_json_keys() -> None:
    env = {
        "CURIE_PLUGIN_DIR": "/bundle",
        "CURIE_SESSION_ID": "test",
        "CURIE_SANDBOX_ID": "test",
        "CURIE_BUDGET": '{"max_output_tokens_per_run":100,"max_usd_per_day":1.0}',
        "CURIE_HOSTED_CONNECTOR_ENDPOINTS": '{"version":1,"agent":"acme-dev","agent":"acme-prod","connectors":{}}',
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.test",
    }
    with pytest.raises(ValueError, match="duplicate"):
        RunnerConfig.from_env(env)


def test_remote_connectors_in_static_mode_receive_no_platform_headers(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    with (root / "connectors.yaml").open("a") as output:
        output.write("  external:\n    url: https://remote.example.com/mcp\n")
    entries = derive_mcp_servers(
        root,
        **_SCOPE,
        endpoints=_binding(base_url="https://connectors.example.com/reviewer"),
        caller_header=True,
        env={"CURIE_CONNECTOR_TOOL_GRANT": "ccg.test"},
    )
    assert entries["external"] == {"type": "http", "url": "https://remote.example.com/mcp"}


def test_upload_urls_are_not_transferred_in_the_route_pipe(tmp_path: Path) -> None:
    from curie_runner.route_callers import caller_launch

    binding = _binding(
        base_url="https://connectors.example.com/reviewer",
        upload_url="https://uploads.example.com/reviewer",
    )
    root = _bundle(tmp_path)
    urls = connector_urls(root, **_SCOPE, endpoints=binding)
    launch = caller_launch(
        {"command": "true"},
        routes={"reviewer": {"url": urls["reviewer"], "token": "ccr.test"}},
        source_env={},
        lock_library="/opt/lock.so",
        cwd=None,
    )
    assert json.loads(launch.routes_document) == {
        "reviewer": {
            "url": "https://connectors.example.com/reviewer",
            "token": "ccr.test",
        }
    }
    assert b"uploads.example.com" not in launch.routes_document

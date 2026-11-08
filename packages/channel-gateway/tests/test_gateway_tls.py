"""The standalone gateway and its clients authenticate over verified TLS."""

import asyncio
import os
import ssl
import subprocess
import sys

import httpx
import pytest
from curie_channel_gateway.read_transport import GatewayAuthenticationError, GatewayClient
from curie_test_support.static_shell import free_port, make_shell_tls, wait_until
from pydantic import TypeAdapter

API_TOKEN = "test-gateway-api-credential"
WORKER_TOKEN = "test-gateway-worker-credential"


@pytest.fixture(scope="module")
def tls_gateway(tmp_path_factory):
    directory = tmp_path_factory.mktemp("gateway-tls")
    tls = make_shell_tls(directory)
    port = free_port()
    endpoint = f"https://127.0.0.1:{port}"
    context = ssl.create_default_context(cafile=str(tls.ca))
    env = {
        **os.environ,
        "CURIE_CHANNEL_GATEWAY_HOST": "127.0.0.1",
        "CURIE_CHANNEL_GATEWAY_PORT": str(port),
        "CURIE_CHANNEL_GATEWAY_API_TOKEN": API_TOKEN,
        "CURIE_CHANNEL_GATEWAY_WORKER_TOKEN": WORKER_TOKEN,
        "CURIE_CHANNEL_GATEWAY_TLS_CERT": str(tls.cert),
        "CURIE_CHANNEL_GATEWAY_TLS_KEY": str(tls.key),
        "CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE": "false",
        "CURIE_CHANNEL_GATEWAY_REPLAY_DB": str(directory / "requests.sqlite3"),
        "CURIE_SLACK_IDENTITIES": "[]",
        "SLACK_BOT_TOKEN": "",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "curie_channel_gateway.slack.read_server"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        with httpx.Client(verify=context, timeout=0.5) as client:

            def ready():
                assert process.poll() is None, "gateway exited before TLS became ready"
                try:
                    return client.get(endpoint).status_code == 401
                except httpx.ConnectError:
                    return False

            wait_until(ready, 10, "gateway TLS listener did not become ready")
        yield endpoint, context
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
                pytest.fail("gateway did not shut down after termination")
        assert process.poll() is not None, "gateway process survived cleanup"


def test_tls_client_checks_certificate_and_caller_authority(tls_gateway):
    endpoint, context = tls_gateway

    async def scenario():
        async with httpx.AsyncClient() as untrusted:
            with pytest.raises(httpx.ConnectError):
                await untrusted.get(endpoint)
        async with httpx.AsyncClient(verify=context) as http:
            client = GatewayClient(http, endpoint, WORKER_TOKEN)
            assert (
                await client.call(
                    "default", "identity_of", {"author": "U0EXAMPLE1"}, TypeAdapter(str | None)
                )
                is None
            )
            for token in ("wrong-caller-credential", API_TOKEN):
                refused = GatewayClient(http, endpoint, token)
                with pytest.raises(GatewayAuthenticationError):
                    await refused.call(
                        "default", "identity_of", {"author": "U0EXAMPLE1"}, TypeAdapter(str | None)
                    )

    asyncio.run(scenario())

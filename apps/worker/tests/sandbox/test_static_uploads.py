"""Real object store, worker staging, supervisor, bundle client and upload listener."""

from __future__ import annotations

import asyncio
import base64
import gzip
import io
import json
import os
import sys
import tarfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_connector_proxy import caller, server
from curie_internal.connector_endpoints import BINDING_ENV, UPLOADS_ENV
from curie_test_support.static_shell import (
    CONTROL_TOKEN,
    SupervisorProcess,
    make_shell_tls,
    wait_until,
)
from curie_test_support.valkey import connect_or_skip
from curie_worker import caller_token
from curie_worker.bundle_store import BundleStore
from curie_worker.config import WorkerConfig
from curie_worker.sandbox import StaticPoolSandboxClient, StaticShellSlot, shell_ssl_context
from nacl.signing import SigningKey

SEED = base64.b64encode(bytes(range(32))).decode()
CLIENT = b"""
import json, os, stat, urllib.request, urllib.error
from pathlib import Path
p = Path(os.environ["CURIE_CONNECTOR_UPLOAD_TOKENS_FILE"])
token = json.loads(p.read_text())["reviewer"]
url = json.loads(os.environ["CURIE_CONNECTOR_UPLOAD_ENDPOINTS"])["reviewer"]
results = []
for credential in [token, *json.loads(os.environ["UPLOAD_TEST_CONTROLS"])]:
    request = urllib.request.Request(
        url, data=b"bundle-upload", headers={"X-Curie-Caller": credential}
    )
    try:
        with urllib.request.urlopen(request) as response:
            results.append([response.status, response.read().decode()])
    except urllib.error.HTTPError as response:
        results.append([response.code, json.loads(response.read())])
Path(os.environ["HOME"], "upload-result.json").write_text(json.dumps({
    "results": results, "file": str(p), "mode": stat.S_IMODE(p.stat().st_mode),
    "uid": p.stat().st_uid,
}))
"""


def _bundle() -> bytes:
    files = {
        ".claude-plugin/plugin.json": b'{"name":"acme-bot","version":"1.0.0"}',
        "connectors.yaml": b"connectors:\n  reviewer:\n    image: acme/reviewer:1\n",
        "upload.py": CLIENT,
    }
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return gzip.compress(buffer.getvalue())


@pytest.fixture
def bundle_store() -> Iterator[BundleStore]:
    store = BundleStore(
        WorkerConfig(
            s3_endpoint_url=os.environ.get("TEST_S3_ENDPOINT_URL", "http://127.0.0.1:29000"),
            s3_access_key="rustfs",
            s3_secret_key="rustfssecret",
            bundle_bucket=f"upload-test-{uuid.uuid4().hex}",
        )
    )
    store._client.create_bucket(Bucket=store._bucket)
    try:
        store._client.put_object(Bucket=store._bucket, Key="bundle.tar.gz", Body=_bundle())
        yield store
    finally:
        store._client.delete_object(Bucket=store._bucket, Key="bundle.tar.gz")
        store._client.delete_bucket(Bucket=store._bucket)
        assert store._bucket not in {
            item["Name"] for item in store._client.list_buckets()["Buckets"]
        }


def test_bundle_upload_uses_delivered_token_and_claim_wipe_removes_it(
    tmp_path: Path, bundle_store: BundleStore
) -> None:
    async def exercise() -> None:
        seen: list[bytes] = []

        async def implementation(request: web.Request) -> web.Response:
            assert request.headers[caller.AGENT_HEADER] == "acme-dev"
            assert caller.HEADER not in request.headers
            seen.append(await request.read())
            return web.Response(text="uploaded")

        upstream_app = web.Application()
        upstream_app.router.add_post("/v1/upload", implementation)
        async with TestServer(upstream_app) as upstream:
            proxy = server.ProxyConfig(
                listen_port=1,
                upstream_port=upstream.port,
                public_keys=(SigningKey(bytes(range(32))).verify_key,),
                admits=frozenset({"acme-dev"}),
                connector="reviewer",
                routes=(server.RouteConfig("/v1/upload", frozenset({"POST"}), 1024),),
                upload_only=True,
            )
            async with TestServer(server.make_app(proxy)) as upload:
                tls = make_shell_tls(tmp_path / "tls")
                shell = SupervisorProcess(
                    slot="upload-slot",
                    tls=tls,
                    root=tmp_path / "shell",
                    runner_command=(
                        f'sh -c \'{sys.executable} "$CURIE_PLUGIN_DIR/upload.py" '
                        f"&& exec {sys.executable} -m curie_runner'"
                    ),
                )
                valkey = connect_or_skip(decode_responses=False)
                prefix = "test-upload-" + uuid.uuid4().hex
                client = StaticPoolSandboxClient(
                    slots=[StaticShellSlot(shell.slot, "localhost", shell.port, "upload-pool")],
                    redis_client=valkey,
                    control_token=CONTROL_TOKEN,
                    ssl_context=shell_ssl_context(str(tls.ca)),
                    bundle_store=bundle_store,
                    key_prefix=prefix,
                    allow_unisolated_shells=True,
                    connector_upload_signing_key=SEED,
                )
                url = str(upload.make_url("/v1/upload"))
                controls = [
                    caller_token.mint_upload(
                        SEED, agent=agent, connector="reviewer", exp=int(time.time()) + ttl
                    )
                    for agent, ttl in (("acme-prod", 60), ("acme-dev", -1))
                ]
                env = {
                    "CURIE_RUNNER_TOKEN": "runner-" + uuid.uuid4().hex,
                    "CURIE_BUNDLE_REF": "bundle.tar.gz",
                    "CURIE_SESSION_ID": "upload-proof",
                    "CURIE_FAKE_MODEL": "1",
                    "CURIE_BUDGET": '{"max_output_tokens_per_run":1000,"max_usd_per_day":1}',
                    "OTEL_SDK_DISABLED": "true",
                    "UPLOAD_TEST_CONTROLS": json.dumps(controls),
                    UPLOADS_ENV: json.dumps({"reviewer": url}),
                    BINDING_ENV: json.dumps(
                        {
                            "version": 1,
                            "agent": "acme-dev",
                            "connectors": {
                                "reviewer": {
                                    "base_url": str(upload.make_url("")),
                                    "upload_url": url,
                                }
                            },
                        }
                    ),
                }
                try:
                    await asyncio.to_thread(shell.start)
                    await asyncio.to_thread(
                        client.create_claim,
                        "upload-claim",
                        pool="upload-pool",
                        env=env,
                        deadline=time.monotonic() + 30,
                        agent_name="acme-dev",
                    )
                    await asyncio.to_thread(
                        wait_until,
                        lambda: (
                            len(list(shell.state_root.glob("claims/*/home/upload-result.json")))
                            == 1
                        ),
                        15,
                        "bundle upload client completed",
                    )
                    paths = list(shell.state_root.glob("claims/*/home/upload-result.json"))
                    assert len(paths) == 1
                    result = json.loads(paths[0].read_text())
                    assert result["mode"] == 0o400
                    assert result["uid"] == os.getuid()
                    assert result["results"][0] == [200, "uploaded"]
                    for response, reason in zip(
                        result["results"][1:], ("not_admitted", "expired"), strict=True
                    ):
                        assert response[0] == 403
                        assert response[1]["error"]["data"]["curie_caller"] == reason
                    assert seen == [b"bundle-upload"]
                    assert Path(result["file"]).name == "connector-upload-tokens.json"
                    await asyncio.to_thread(
                        client.delete_claim, "upload-claim", request_timeout_seconds=10
                    )
                    assert not Path(result["file"]).exists()
                    assert list(shell.state_root.glob("claims/*")) == []
                finally:
                    try:
                        await asyncio.to_thread(
                            client.delete_claim, "upload-claim", request_timeout_seconds=10
                        )
                    finally:
                        await asyncio.to_thread(shell.stop)
                        for key in valkey.scan_iter(match=prefix + "*"):
                            valkey.delete(key)
                        valkey.close()

    asyncio.run(exercise())

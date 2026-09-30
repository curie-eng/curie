"""A bundle upload must leave GET /health available during validation.

The upload runs through a real one-worker uvicorn. A wrapper around the
archive validator pauses before its real call until a health request completes.
If validation runs on the event loop, the health request cannot complete
until the pause ends, so the test fails without a latency threshold.
"""

from __future__ import annotations

import io
import socket
import tarfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest
import uvicorn
from curie_api import deploy
from curie_api.main import create_app

MANIFEST = '{"name": "demo-plugin", "version": "0.1.0"}'


def _skill(name: str) -> bytes:
    return f"---\nname: {name}\ndescription: does {name} things\n---\n\n# {name}\n".encode()


def _tar_plain(files: dict[str, bytes], top: str = "demo-plugin") -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:") as tf:
        for rel, content in files.items():
            info = tarfile.TarInfo(f"{top}/{rel}")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _UvicornThread(threading.Thread):
    def __init__(self, app: Any, host: str, port: int) -> None:
        super().__init__(name="curie-api-offloop", daemon=True)
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=host,
                port=port,
                log_level="warning",
                access_log=False,
                lifespan="on",
            )
        )

    def run(self) -> None:
        self.server.run()

    def stop(self) -> None:
        self.server.should_exit = True


@pytest.fixture
def live_api(_disposable_db: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """A one-worker uvicorn serving ``create_app`` on a free loopback port."""

    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    host = "127.0.0.1"
    port = _free_port()
    thread = _UvicornThread(create_app(), host, port)
    thread.start()
    deadline = time.time() + 30
    url = f"http://{host}:{port}"
    last_exc: Exception | None = None
    while time.time() < deadline:
        if thread.server.started:
            try:
                response = httpx.get(f"{url}/health", timeout=1.0)
                if response.status_code == 200:
                    break
            except httpx.HTTPError as exc:
                last_exc = exc
        time.sleep(0.05)
    else:
        thread.stop()
        thread.join(timeout=5)
        raise RuntimeError(f"uvicorn did not become healthy: {last_exc}")
    try:
        yield url
    finally:
        thread.stop()
        thread.join(timeout=15)


def _create_version(http: httpx.Client, headers: dict[str, str]) -> tuple[str, str]:
    agent = http.post(
        "/agents",
        json={
            "name": "archive-offloop-agent",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=headers,
    )
    assert agent.status_code == 201, agent.text
    version = http.post(
        f"/agents/{agent.json()['id']}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    return agent.json()["id"], version.json()["id"]


def test_health_is_served_while_bundle_validation_is_in_progress(
    live_api: str,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _tar_plain(
        {
            ".claude-plugin/plugin.json": MANIFEST.encode(),
            "skills/alpha/SKILL.md": _skill("alpha"),
        }
    )
    entered_validation = threading.Event()
    release_validation = threading.Event()
    real_validate = deploy.validate_archive

    def _gated_validate(*args: Any, **kwargs: Any) -> tuple[str, str]:
        entered_validation.set()
        assert release_validation.wait(timeout=15), "health did not release validation"
        return real_validate(*args, **kwargs)

    monkeypatch.setattr(deploy, "validate_archive", _gated_validate)

    with httpx.Client(base_url=live_api, timeout=30.0) as http:
        agent_id, version_id = _create_version(http, auth_headers)
        with httpx.Client(base_url=live_api, timeout=5.0) as probe:
            warmup = http.get("/health")
            assert warmup.status_code == 200
            with ThreadPoolExecutor(max_workers=1) as executor:
                upload = executor.submit(
                    http.put,
                    f"/agents/{agent_id}/versions/{version_id}/bundle",
                    files={"file": ("demo.tar", archive)},
                    headers=auth_headers,
                )
                try:
                    assert entered_validation.wait(timeout=10), (
                        "upload did not enter validate_archive"
                    )
                    health = probe.get("/health")
                    assert health.status_code == 200, health.text
                    assert not upload.done(), "validation ended before health completed"
                finally:
                    release_validation.set()
                response = upload.result(timeout=30)

    assert response.status_code == 201, response.text

"""``identity_assets.reference_bundle``: a bundle plus the human-restricted connector.

The produced ``connectors.yaml`` is judged by the plugin-format loader the API
and CLI use, and the connector's inline bootstrap is run the way the image runs
it, from nothing but the env it declares, so a bundle that validates but cannot
serve fails here.
"""

from __future__ import annotations

import http.client
import json
import os
import re
import select
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import identity_assets as assets
import pytest
from identity_target import REPO
from plugin_format.connector_render import mcp_entry
from plugin_format.connectors import CONNECTORS_FILE, ConnectorsFile, validate_connectors
from plugin_format.validate import validate_bundle
from plugin_format.yaml_loader import safe_load_unique

SOURCE = REPO / "examples" / "weather"
CONNECTOR_SOURCE = Path(__file__).resolve().parents[1] / "human_restricted_connector.py"
NAME = "human-restricted"
IMAGE = "python:3.12-slim@sha256:12316debbfb86890f6b2d825820612741ba527ad5a44e3902759141d572bfe8f"
AUTHORITY_URL = "http://authority.example.test:8080"
AUTHORITY_BEARER = "synthetic-authority-bearer"


def load(root: Path) -> ConnectorsFile:
    """The bundle's connectors.yaml through the product loader, which must accept it."""

    data = safe_load_unique((root / CONNECTORS_FILE).read_text(encoding="utf-8"))
    parsed, errors = validate_connectors(data)
    assert errors == []
    assert parsed is not None
    return parsed


def files(root: Path) -> set[str]:
    return {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()}


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    dest = tmp_path / "bundle"
    produced = assets.reference_bundle(
        SOURCE, dest, authority_url=AUTHORITY_URL, authority_bearer=AUTHORITY_BEARER
    )
    assert produced == dest
    return dest


def test_the_reference_bundle_keeps_the_source_connectors_and_adds_one(tmp_path: Path) -> None:
    before = (SOURCE / CONNECTORS_FILE).read_text(encoding="utf-8")
    dest = tmp_path / "bundle"
    assets.reference_bundle(
        SOURCE, dest, authority_url=AUTHORITY_URL, authority_bearer=AUTHORITY_BEARER
    )
    assert (SOURCE / CONNECTORS_FILE).read_text(encoding="utf-8") == before
    assert files(dest) == files(SOURCE)

    source, produced = load(SOURCE), load(dest)
    assert set(produced.connectors) == set(source.connectors) | {NAME}
    assert "netpol-probe" in produced.connectors
    for name, spec in source.connectors.items():
        assert produced.connectors[name].model_dump() == spec.model_dump(), name

    result = validate_bundle(dest)
    assert result.errors == []
    assert result.valid


def test_the_human_restricted_connector_is_digest_pinned_and_carries_its_source(
    bundle: Path,
) -> None:
    spec = load(bundle).connectors[NAME]
    assert spec.image == IMAGE
    assert re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", spec.image)
    assert spec.build is None and spec.url is None
    assert spec.args[:2] == ["python3", "-c"] and len(spec.args) == 3
    assert spec.env == {
        "HRC_SOURCE": CONNECTOR_SOURCE.read_text(encoding="utf-8"),
        "AUTHORITY_URL": AUTHORITY_URL,
        "AUTHORITY_BEARER": AUTHORITY_BEARER,
    }
    # The renderer substitutes ${...} in args and env, so none may appear.
    for text in [*spec.args, *spec.env.values()]:
        assert "${" not in text
    # The module's default PORT is the port the runner dials, at the MCP path it serves.
    assert spec.port == 8000
    entry = mcp_entry("identity-e2e", "agent-a", "identity-e2e", NAME, spec)
    assert entry["url"].endswith(":8000/mcp")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_until_listening(process: subprocess.Popen[bytes], port: int) -> None:
    """Bounded connect attempts, paced by select on the child's stderr (which drains it)."""

    assert process.stderr is not None
    deadline = time.monotonic() + 20
    stderr = bytearray()
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr += process.stderr.read()
            pytest.fail(f"the bootstrap exited {process.returncode}: {bytes(stderr[-2000:])!r}")
        with socket.socket() as probe:
            probe.settimeout(0.5)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return
        readable, _, _ = select.select([process.stderr], [], [], 0.05)
        if readable:
            stderr += os.read(process.stderr.fileno(), 65_536)
    pytest.fail(f"the bootstrap never listened on {port}: {bytes(stderr[-2000:])!r}")


def post(port: int, message: dict[str, Any]) -> tuple[int, str, Any]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        connection.request(
            "POST",
            "/mcp",
            body=json.dumps(message).encode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
        )
        response = connection.getresponse()
        content_type = response.getheader("Content-Type") or ""
        return response.status, content_type, json.loads(response.read())
    finally:
        connection.close()


def test_the_inline_bootstrap_serves_mcp_from_the_env_it_declares(
    bundle: Path, tmp_path: Path
) -> None:
    spec = load(bundle).connectors[NAME]
    port = free_port()
    workdir = tmp_path / "run"
    workdir.mkdir()
    # The image runs `python3 -c <bootstrap>` with the declared env and nothing else.
    environment = {"PATH": os.environ.get("PATH", ""), **spec.env, "PORT": str(port)}
    process = subprocess.Popen(  # noqa: S603
        [sys.executable, *spec.args[1:]],
        env=environment,
        cwd=workdir,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        wait_until_listening(process, port)
        status, content_type, answer = post(
            port,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "identity-e2e-test", "version": "0"},
                },
            },
        )
        assert (status, content_type.startswith("application/json")) == (200, True)
        assert answer["result"]["protocolVersion"] == "2025-06-18"
        assert answer["result"]["capabilities"] == {"tools": {}}
        status, _, called = post(
            port,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "record_effect", "arguments": {"note": "synthetic effect"}},
            },
        )
        assert status == 200
        assert called["result"]["isError"] is True
        assert [item["text"] for item in called["result"]["content"]] == [
            "refused missing_requester"
        ]
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        assert process.stderr is not None
        process.stderr.close()


def test_a_connector_source_carrying_a_placeholder_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The renderer would rewrite ``${CURIE_*}`` inside the shipped source, so refuse it."""

    target = CONNECTOR_SOURCE.resolve()
    original_text, original_bytes = Path.read_text, Path.read_bytes
    reads: list[Path] = []
    planted = "\n# ${CURIE_CONNECTOR_URL}\n"

    def read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        text = original_text(self, *args, **kwargs)
        if self.resolve() == target:
            reads.append(self)
            return text + planted
        return text

    def read_bytes(self: Path) -> bytes:
        raw = original_bytes(self)
        if self.resolve() == target:
            reads.append(self)
            return raw + planted.encode()
        return raw

    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    with pytest.raises(ValueError):
        assets.reference_bundle(
            SOURCE,
            tmp_path / "bundle",
            authority_url=AUTHORITY_URL,
            authority_bearer=AUTHORITY_BEARER,
        )
    assert reads, "reference_bundle did not read human_restricted_connector.py"

#!/usr/bin/env python3
"""CI catalog probe of the declared images and build contexts, without model calls.

Credentials below are inert fixture values. No real upstream tool is invoked.
Containers and the temporary directory belong only to this invocation.
"""

from __future__ import annotations

import http.client
import json
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples/sre-bot"

# MCP 2.1.1 streamable HTTP initialize. Headers from
# mcp/client/streamable_http.py:_prepare_headers. Transport:
# https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
INITIALIZE = json.dumps(
    {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "sre-catalog-ci", "version": "0"},
        },
    }
).encode()
HANDSHAKE_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


class StartupNonretryable(Exception):
    """The listener completed a non-HTTP or wrong-protocol response."""


def docker(*args):
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, timeout=600
    ).stdout.strip()


def _application_handshake(host, port, path):
    conn = http.client.HTTPConnection(host, port, timeout=1)
    try:
        conn.request("POST", path, INITIALIZE, HANDSHAKE_HEADERS)
        response = conn.getresponse()
        return response.status
    except http.client.RemoteDisconnected:
        # Subclasses BadStatusLine; CPython stores line as repr("").
        raise OSError("unreadiness") from None
    except http.client.BadStatusLine as exc:
        line = exc.line if isinstance(getattr(exc, "line", None), str) else ""
        if line.strip().strip("'\""):
            raise StartupNonretryable("wrong protocol") from None
        raise OSError("unreadiness") from None
    except (http.client.IncompleteRead, http.client.UnknownProtocol):
        raise OSError("unreadiness") from None
    finally:
        conn.close()


def wait_until_application_ready(url, *, timeout=60):
    """Wait until the MCP process answers HTTP. Do not retry checker assertions.

    TCP connect is not ready: a published Docker port can accept before the
    application serves. Retry connection-level unreadiness only. Completed HTTP,
    including 401 and 5xx, means the process is serving so the checker runs
    once. Non-HTTP bytes are nonretryable.
    """
    target = urlsplit(url)
    host, port, path = target.hostname, target.port, target.path or "/"
    if not host or port is None:
        raise StartupNonretryable("invalid endpoint")
    deadline = time.monotonic() + timeout
    while True:
        try:
            _application_handshake(host, port, path)
            return
        except StartupNonretryable:
            raise
        except (OSError, http.client.HTTPException, TimeoutError):
            if time.monotonic() >= deadline:
                raise TimeoutError("connector startup") from None
            time.sleep(0.2)


def main():
    owned = []
    images = []
    prefix = f"sre-catalog-{uuid.uuid4().hex[:12]}"
    try:
        with tempfile.TemporaryDirectory(prefix=prefix) as scratch:
            temp = Path(scratch)
            kubeconfig = temp / "kubeconfig"
            kubeconfig.write_text("""apiVersion: v1
kind: Config
clusters:
- name: fixture
  cluster: {server: "https://127.0.0.1:9", insecure-skip-tls-verify: true}
contexts:
- name: fixture
  context: {cluster: fixture, user: fixture}
current-context: fixture
users:
- name: fixture
  user: {token: inert-catalog-fixture}
""")
            # Readable by nonroot container uid; contains no operational credential.
            kubeconfig.chmod(0o644)
            endpoints = {}
            for name, spec in yaml.safe_load((BUNDLE / "connectors.yaml").read_text())[
                "connectors"
            ].items():
                container = f"{prefix}-{name}"
                image = spec.get("image")
                if not image:
                    image = f"{prefix}-{name}:test"
                    context = (BUNDLE / spec["build"]["context"]).resolve()
                    if not context.is_relative_to(BUNDLE):
                        raise ValueError("build context outside bundle")
                    images.append(image)
                    docker("build", "-t", image, str(context))
                args = [
                    "run",
                    "-d",
                    "--name",
                    container,
                    "--cap-drop=ALL",
                    "--security-opt=no-new-privileges",
                    "--memory=256m",
                    "-p",
                    "127.0.0.1::8000",
                    "-v",
                    f"{kubeconfig}:/secrets/kubeconfig:ro",
                ]
                environment = dict(spec.get("env", {}))
                environment.update(
                    {
                        "GRAFANA_URL": "http://127.0.0.1:9",
                        "GRAFANA_SERVICE_ACCOUNT_TOKEN": "inert-catalog-fixture",
                        "SELF_UPGRADE_KUBECONFIG": "/secrets/kubeconfig",
                    }
                )
                for key, value in environment.items():
                    args += ["-e", f"{key}={value}"]
                owned.append(container)
                docker(
                    *args,
                    image,
                    *[str(a).replace("${CURIE_ALLOWED_HOSTS}", "*") for a in spec.get("args", [])],
                )
                address = docker("port", container, "8000/tcp")
                endpoints[name] = f"http://{address}/mcp"
            path = temp / "endpoints.json"
            path.write_text(json.dumps(endpoints))
            for url in endpoints.values():
                wait_until_application_ready(url, timeout=60)
            return subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools/sre-contract/check.py"),
                    "--bundle",
                    str(BUNDLE),
                    "--endpoints",
                    str(path),
                ],
                timeout=300,
            ).returncode
    except Exception as exc:
        print(f"SRE catalog CI failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    finally:
        for container in reversed(owned):
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=30)
        for image in images:
            subprocess.run(["docker", "image", "rm", image], capture_output=True, timeout=30)


if __name__ == "__main__":
    raise SystemExit(main())

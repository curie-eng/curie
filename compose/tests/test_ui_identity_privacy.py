"""The shipped nginx must not log account capabilities or OIDC query secrets."""

import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
LABEL = "curie.test.ui-identity-privacy"


def docker(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True, timeout=timeout, cwd=ROOT
    )


@pytest.fixture
def ui_image() -> Iterator[tuple[httpx.Client, str]]:
    """Build production sources and own exactly one image/container/loopback port."""
    execution = uuid4().hex
    image = f"curie-ui-identity-privacy:{execution}"
    container = ""
    docker("info")  # Unavailable Docker is a setup error, never a skip.
    try:
        docker(
            "build",
            "--label",
            f"{LABEL}={execution}",
            "-f",
            "apps/ui/Dockerfile",
            "-t",
            image,
            ".",
            timeout=900,
        )
        container = docker(
            "create",
            "--label",
            f"{LABEL}={execution}",
            "-p",
            "127.0.0.1:0:8080",
            "-e",
            "CURIE_API_TARGET=http://127.0.0.1:1",
            image,
        ).stdout.strip()
        docker("start", container)
        address = docker("port", container, "8080/tcp").stdout.strip()
        with httpx.Client(base_url=f"http://{address}", timeout=3, trust_env=False) as client:
            for _ in range(100):
                try:
                    if client.get("/").status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                time.sleep(0.1)
            else:
                pytest.fail("candidate UI image did not become ready")
            yield client, container
    finally:
        if container:
            owner = docker("inspect", "-f", f'{{{{index .Config.Labels "{LABEL}"}}}}', container)
            assert owner.stdout.strip() == execution
            docker("rm", "-f", container)
        images = docker("image", "ls", "-q", "--filter", f"label={LABEL}={execution}")
        if images.stdout.strip():
            docker("image", "rm", image)
        assert not docker("ps", "-aq", "--filter", f"label={LABEL}={execution}").stdout.strip()
        remaining_images = docker("image", "ls", "-q", "--filter", f"label={LABEL}={execution}")
        assert not remaining_images.stdout.strip()


def test_identity_secrets_are_absent_from_nginx_logs_even_on_upstream_failure(ui_image):
    client, container = ui_image
    bearer = "synthetic-connect-bearer-private"
    code = "synthetic-oidc-code-private"
    state = "synthetic-oidc-state-private"
    responses = [
        client.get(f"/api/identity/connect/{bearer}"),
        client.get(f"/api/console/oidc/callback?code={code}&state={state}"),
    ]
    assert all(response.status_code == 502 for response in responses)
    # Positive control: ordinary operator requests retain both error/access logs.
    control = "/api/health?probe=publiccontrol"
    assert client.get(control).status_code == 502
    assert client.get("/api/v1/internal").status_code == 404
    assert client.get("/api/v1/internal/worker").status_code == 404
    for _ in range(100):
        logs = docker("logs", container)
        if control in logs.stdout and control in logs.stderr:
            break
        time.sleep(0.05)
    assert control in logs.stdout and control in logs.stderr
    for output in (logs.stdout, logs.stderr):
        assert all(secret not in output for secret in (bearer, code, state)), output
    for response in responses:
        assert response.headers["cache-control"] == "private, no-store"
        assert response.headers["referrer-policy"] == "no-referrer"

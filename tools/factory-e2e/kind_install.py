"""Shared setup for disposable kind chart installs."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager

# kind nodes use their own containerd registry configuration.
DOCKER_HUB_MIRROR_PATCH = (
    '[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]\n'
    '  endpoint = ["https://mirror.gcr.io", "https://registry-1.docker.io"]\n'
)
_DIAGNOSTIC_URL = re.compile(
    r"""[A-Za-z][A-Za-z0-9+.-]*://"""
    r"""(?:(mirror\.gcr\.io|registry-1\.docker\.io)(?=[/?#\s"'<>]|$))?[^\s"'<>]*"""
)


@contextmanager
def diagnose_helm_install(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    namespace: str,
    fullname: str,
    redact: Callable[[str], str],
) -> Iterator[None]:
    """Attach scoped diagnostics to the install error before fixture teardown."""
    try:
        yield
    except Exception as error:
        snapshots = (
            ("Postgres pod", ("get", "pod", f"{fullname}-postgres-0", "--ignore-not-found")),
            ("Namespace warnings", ("get", "events", "--field-selector=type=Warning")),
            (
                "Schema migration logs",
                (
                    "logs",
                    "-l",
                    f"batch.kubernetes.io/job-name={fullname}-schema-migrate",
                    "--prefix=true",
                    "--tail=20",
                    "--pod-running-timeout=3s",
                ),
            ),
        )
        details = []
        for label, args in snapshots:
            try:
                result = kubectl("-n", namespace, *args, timeout=8, check=False)
            except (OSError, RuntimeError, subprocess.SubprocessError):
                details.append(f"{label}: unable to collect")
                continue
            output = redact(result.stdout + result.stderr)
            output = _DIAGNOSTIC_URL.sub(
                lambda url: f"{url[1]}/<redacted>" if url[1] else "<redacted-url>", output
            )
            details.append(f"{label} (exit {result.returncode}):\n{output.strip()[-6000:]}")
        error.add_note("Helm install diagnostics:\n" + "\n".join(details))
        raise

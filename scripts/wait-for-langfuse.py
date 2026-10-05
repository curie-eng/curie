"""Bound CI startup on completed Langfuse migrations and usable read paths."""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKING_SERVICES = ("postgres", "valkey", "clickhouse", "rustfs")
WORKER_PROBE = (
    "fetch('http://langfuse-worker:3030/api/ready')"
    ".then(response => process.exit(response.ok ? 0 : 1))"
    ".catch(() => process.exit(1))"
)


class Readiness:
    def __init__(self, timeout: float, interval: float, web_url: str) -> None:
        self.deadline = time.monotonic() + timeout
        self.interval = interval
        self.web_url = web_url.rstrip("/")
        project = os.environ.get("COMPOSE_PROJECT_NAME", "curie")
        configured_files = os.environ.get("COMPOSE_FILE")
        if configured_files and not os.environ.get("COMPOSE_PROJECT_NAME"):
            raise ValueError("COMPOSE_FILE requires COMPOSE_PROJECT_NAME")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
            raise ValueError("COMPOSE_PROJECT_NAME is invalid")
        files = configured_files.split(os.pathsep) if configured_files else [
            str(REPO_ROOT / "compose.dev.yaml")
        ]
        self.compose = ["docker", "compose", "-p", project]
        for source in files:
            path = Path(source)
            if not path.is_absolute() or not path.is_file():
                raise ValueError("COMPOSE_FILE must contain existing absolute paths")
            self.compose.extend(("-f", str(path)))
        public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "pk-lf-curie-dev")
        secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "sk-lf-curie-dev")
        self.authorization = "Basic " + base64.b64encode(
            f"{public_key}:{secret_key}".encode()
        ).decode()

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Langfuse readiness deadline expired")
        return remaining

    def run(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        phase = {
            "up": "startup",
            "wait": "bucket initialization",
            "ps": "container state",
            "logs": "migration inspection",
            "exec": "worker probe",
        }.get(arguments[0], "unknown operation")
        # Compose output can include deployment details or credentials. Publish
        # only these fixed phase names and numeric exit codes; withhold raw output.
        try:
            result = subprocess.run(
                [*self.compose, *arguments],
                capture_output=True,
                text=True,
                check=False,
                timeout=self.remaining(),
            )
        except subprocess.TimeoutExpired:
            raise TimeoutError(
                "Langfuse readiness deadline expired during Compose command "
                f"(phase={phase}, exit code unavailable)"
            ) from None
        if check and result.returncode != 0:
            raise RuntimeError(
                "Langfuse readiness Compose command failed "
                f"(phase={phase}, exit code={result.returncode})"
            )
        return result

    def state(self, service: str) -> str:
        output = self.run("ps", "--all", "--format", "json", service).stdout.strip()
        try:
            if output.startswith("["):
                rows = json.loads(output)
            else:
                rows = [json.loads(line) for line in output.splitlines()]
        except json.JSONDecodeError:
            raise RuntimeError("Langfuse readiness could not read container state") from None
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise RuntimeError(f"Langfuse readiness requires one {service} container")
        state = rows[0].get("State")
        if not isinstance(state, str):
            raise RuntimeError("Langfuse readiness container state is invalid")
        return state

    def pause(self) -> None:
        time.sleep(min(self.interval, self.remaining()))

    def request(self, path: str, *, authenticated: bool) -> bytes | None:
        headers = {"Authorization": self.authorization} if authenticated else {}
        request = urllib.request.Request(f"{self.web_url}{path}", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=min(5, self.remaining())) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code in (401, 403):
                raise RuntimeError("Langfuse readiness authenticated read was refused") from None
            if error.code not in (404, 429, 500, 502, 503, 504):
                raise RuntimeError(
                    "Langfuse readiness read returned an unexpected status"
                ) from None
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        return None

    def wait_web(self, *, retry_deadlock: bool) -> None:
        retried = False
        while True:
            self.remaining()
            state = self.state("langfuse-web")
            if state in ("exited", "dead", "restarting"):
                result = self.run("logs", "--no-color", "--tail=200", "langfuse-web", check=False)
                logs = (result.stdout + result.stderr).lower()
                deadlock = (
                    ("deadlock detected" in logs or "40p01" in logs)
                    and ("migration" in logs or "prisma" in logs)
                )
                if retry_deadlock and deadlock and not retried:
                    retried = True
                    print("Langfuse migration deadlock detected; retrying web startup once")
                    self.run("up", "-d", "--no-deps", "--force-recreate", "langfuse-web")
                    continue
                raise RuntimeError("Langfuse web exited before migration readiness")
            if (
                state == "running"
                and self.request("/api/public/health", authenticated=False) is not None
            ):
                # Observed in the pinned 3.225.5 web entrypoint: Prisma deploy
                # and ClickHouse up.sh complete before the HTTP server starts.
                # The authenticated read also proves headless project bootstrap.
                # https://api.reference.langfuse.com/api-reference/trace/list
                body = self.request("/api/public/traces?limit=1", authenticated=True)
                if body is not None:
                    try:
                        traces: Any = json.loads(body)
                    except json.JSONDecodeError:
                        raise RuntimeError(
                            "Langfuse readiness read returned malformed JSON"
                        ) from None
                    if not isinstance(traces, dict) or not isinstance(traces.get("data"), list):
                        raise RuntimeError("Langfuse readiness read did not return a trace list")
                    return
            self.pause()

    def wait_worker(self) -> None:
        while True:
            self.remaining()
            state = self.state("langfuse-worker")
            if state in ("exited", "dead", "restarting"):
                raise RuntimeError("Langfuse worker exited before readiness")
            if state == "running":
                # The pinned worker binds to env.HOSTNAME, supplied by Docker
                # as its container hostname. Service DNS reaches that binding.
                # Observed /api/ready on port 3030 in the pinned worker image.
                result = self.run(
                    "exec", "-T", "langfuse-worker", "node", "-e", WORKER_PROBE, check=False,
                )
                if result.returncode == 0:
                    return
            self.pause()

    def start(self, override: Path) -> None:
        override.write_text(
            'services:\n  langfuse-web:\n    restart: "no"\n'
            '  langfuse-worker:\n    restart: "no"\n'
        )
        self.compose.extend(("-f", str(override)))
        self.run("up", "-d", *BACKING_SERVICES, "rustfs-init")
        self.run(
            "up", "-d", "--wait", "--wait-timeout", str(math.ceil(self.remaining())),
            *BACKING_SERVICES,
        )
        self.run("wait", "rustfs-init")
        self.run("up", "-d", "--no-deps", "langfuse-web")
        self.wait_web(retry_deadlock=True)
        self.run(
            "up", "-d", "--wait", "--wait-timeout", str(math.ceil(self.remaining())),
            "langfuse-worker", "otel-collector",
        )
        self.wait_worker()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", action="store_true")
    parser.add_argument(
        "--web-url",
        default=os.environ.get("TEST_LANGFUSE_HOST")
        or os.environ.get("LANGFUSE_HOST")
        or "http://localhost:23000",
    )
    parser.add_argument("--timeout-seconds", type=float, default=180)
    parser.add_argument("--poll-interval-seconds", type=float, default=3)
    arguments = parser.parse_args()
    try:
        if arguments.timeout_seconds <= 0 or arguments.poll_interval_seconds <= 0:
            raise ValueError("Langfuse readiness timing values must be positive")
        readiness = Readiness(
            arguments.timeout_seconds, arguments.poll_interval_seconds, arguments.web_url,
        )
        if arguments.start:
            with tempfile.TemporaryDirectory(prefix="curie-langfuse-readiness-") as directory:
                readiness.start(Path(directory) / "compose.yaml")
        else:
            readiness.wait_web(retry_deadlock=False)
            readiness.wait_worker()
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print("Langfuse migrations, authenticated trace reads, and worker readiness passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

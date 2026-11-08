"""Built runner and shell images import and start the real runner.

Set ``CURIE_SHELL_IMAGE`` to a shell image built from
``packages/shell-supervisor/Dockerfile``; without it these tests skip, unless
``CURIE_SHELL_CONTAINER_PROOF=required`` (CI), which turns a missing image
into a failure. Docker is driven through its CLI, and the container's
supervisor port is published on loopback.

Recycling proofs additionally run ``hostile_child.py`` as the runner's uid:
the first claim writes into every writable directory and creates IPC objects,
and the second looks for any of it.
"""

from __future__ import annotations

import http.client
import io
import json
import os
import socket
import ssl
import subprocess
import tarfile
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Budget, SessionConfig
from curie_shell_supervisor.protocol import claim_path, proxy_base_path
from curie_test_support.static_shell import (
    CONTROL_TOKEN,
    ShellTls,
    claim_archive,
    client_context,
    free_port,
    make_shell_tls,
    wait_until,
)

IMAGE = os.environ.get("CURIE_SHELL_IMAGE", "")
RUNNER_IMAGE = os.environ.get("CURIE_RUNNER_IMAGE", "curie-runner")
REQUIRED = os.environ.get("CURIE_SHELL_CONTAINER_PROOF") == "required"
KEY_PATH = "/etc/curie-shell/tls.key"
CERT_PATH = "/etc/curie-shell/tls.crt"
HOSTILE = (Path(__file__).parent / "hostile_child.py").read_bytes()
RUNNER_COMMAND = "sh -c 'exec python \"$CURIE_PLUGIN_DIR/hostile_child.py\"'"
# Where an isolated child may write: its own claim tree and the scratch roots.
ALLOWED = (
    "/var/lib/curie-shell/claims/",
    "/tmp",
    "/var/tmp",
    "/dev/shm",
    "/run/lock",
    "/dev/mqueue",
)

if not IMAGE:
    if REQUIRED:
        raise RuntimeError(
            "CURIE_SHELL_CONTAINER_PROOF=required may not skip: set CURIE_SHELL_IMAGE"
        )
    pytest.skip("CURIE_SHELL_IMAGE names no shell image", allow_module_level=True)


def _docker(*args: str, stdin: bytes | None = None, check: bool = True) -> str:
    result = subprocess.run(  # noqa: S603 - fixed docker argv
        ["docker", *args], input=stdin, capture_output=True, check=False
    )
    if check and result.returncode != 0:
        raise AssertionError(f"docker {args[0]} failed: {result.stderr.decode()[-1500:]}")
    return result.stdout.decode()


def _root_owned(files: dict[str, bytes]) -> bytes:
    """A tar of ``files`` owned by root, mode 0600: docker cp keeps the owner."""

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        directory = tarfile.TarInfo("curie-shell")
        directory.type, directory.mode = tarfile.DIRTYPE, 0o700
        tar.addfile(directory)
        for name, data in files.items():
            info = tarfile.TarInfo(f"curie-shell/{name}")
            info.size, info.mode = len(data), 0o600
            tar.addfile(info, io.BytesIO(data))
    return raw.getvalue()


class Shell:
    def __init__(
        self,
        tls: ShellTls,
        *,
        command: list[str] | None = None,
        runner_command: str | None = RUNNER_COMMAND,
    ) -> None:
        self.tls = tls
        self.name = f"curie-shell-proof-{uuid.uuid4().hex[:8]}"
        self.port = free_port()
        _docker(
            "create",
            "--name",
            self.name,
            "--label",
            "keep=false",
            "--label",
            f"curie.test.owner={self.name}",
            "-p",
            f"127.0.0.1:{self.port}:8443",
            "-e",
            "CURIE_SUPERVISOR_SLOT=proof-0",
            "-e",
            f"CURIE_SUPERVISOR_CONTROL_TOKEN={CONTROL_TOKEN}",
            "-e",
            f"CURIE_SUPERVISOR_TLS_CERT_FILE={CERT_PATH}",
            "-e",
            f"CURIE_SUPERVISOR_TLS_KEY_FILE={KEY_PATH}",
            *(
                ["-e", f"CURIE_SUPERVISOR_RUNNER_COMMAND={runner_command}"]
                if runner_command is not None
                else []
            ),
            "-e",
            "CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS=5",
            IMAGE,
            *(command or []),
        )
        archive = _root_owned({"tls.crt": tls.cert.read_bytes(), "tls.key": tls.key.read_bytes()})
        _docker("cp", "-", f"{self.name}:/etc/", stdin=archive)
        _docker("start", self.name)

    def running(self) -> bool:
        return _docker("inspect", "-f", "{{.State.Running}}", self.name).strip() == "true"

    def exit_code(self) -> int:
        return int(_docker("inspect", "-f", "{{.State.ExitCode}}", self.name).strip())

    def logs(self) -> str:
        result = subprocess.run(  # noqa: S603 - fixed docker argv
            ["docker", "logs", self.name], capture_output=True, check=False
        )
        return (result.stdout + result.stderr).decode()[-3000:]

    def request(
        self, method: str, path: str, *, token: str, body: bytes | None = None
    ) -> tuple[int, Any]:
        connection = http.client.HTTPSConnection(
            "localhost", self.port, timeout=60, context=client_context(self.tls)
        )
        headers = {"Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = "application/x-tar"
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read()
        finally:
            connection.close()
        return response.status, json.loads(data) if data else None

    def answers(self) -> bool:
        try:
            return self.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[0] == 200
        except OSError:
            if not self.running():
                raise AssertionError(f"shell exited: {self.logs()}") from None
            return False

    def remove(self) -> None:
        _docker("rm", "-f", self.name, check=False)
        assert not _docker("ps", "-aq", "--filter", f"name=^/{self.name}$").strip()


def _runner_environment(token: str) -> dict[str, str]:
    return {
        **SessionConfig(
            plugin_dir="/tmp/image-bundle",
            session_id="image-proof",
            sandbox_id="image-proof",
            budget=Budget(max_output_tokens_per_run=1000, max_usd_per_day=1),
        ).to_env(),
        "CURIE_FAKE_MODEL": "1",
        "CURIE_RUNNER_TOKEN": token,
        "OTEL_SDK_DISABLED": "true",
    }


def _runner_status(port: int, token: str | None) -> tuple[int, dict[str, Any]]:
    connection = http.client.HTTPConnection("localhost", port, timeout=2)
    try:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        connection.request("GET", "/v1/status", headers=headers)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


@pytest.fixture(scope="module")
def runner_container() -> Iterator[tuple[str, int, str]]:
    name = f"curie-runner-proof-{uuid.uuid4().hex[:8]}"
    port = free_port()
    token = f"runner-{uuid.uuid4().hex}"
    environment = []
    for key, value in _runner_environment(token).items():
        environment.extend(("-e", f"{key}={value}"))
    raw = io.BytesIO()
    manifest = json.dumps({"name": "image-proof", "version": "0.0.0"}).encode()
    with tarfile.open(fileobj=raw, mode="w") as archive:
        info = tarfile.TarInfo("image-bundle/.claude-plugin/plugin.json")
        info.size, info.mode = len(manifest), 0o644
        archive.addfile(info, io.BytesIO(manifest))
    try:
        _docker(
            "create",
            "--name",
            name,
            "--label",
            f"curie.test.owner={name}",
            "-p",
            f"127.0.0.1:{port}:8080",
            *environment,
            RUNNER_IMAGE,
        )
        _docker("cp", "-", f"{name}:/tmp/", stdin=raw.getvalue())
        _docker("start", name)
        yield name, port, token
    finally:
        _docker("rm", "-f", name, check=False)
        assert not _docker("ps", "-aq", "--filter", f"name=^/{name}$").strip()


@pytest.mark.parametrize("image", [RUNNER_IMAGE, IMAGE], ids=["runner", "shell"])
def test_built_image_imports_the_runner(image: str) -> None:
    _docker(
        "run",
        "--rm",
        "--label",
        "curie.test.owner=runner-import-proof",
        "--entrypoint",
        "python",
        image,
        "-c",
        "import curie_runner",
    )


def test_runner_default_entrypoint_serves_authenticated_status(
    runner_container: tuple[str, int, str],
) -> None:
    name, port, token = runner_container

    def ready() -> bool:
        assert _docker("inspect", "-f", "{{.State.Running}}", name).strip() == "true"
        try:
            status, body = _runner_status(port, token)
        except OSError:
            return False
        assert status == 200, body
        return body["ready"] is True

    wait_until(ready, 60, "the runner image's default entrypoint to serve")
    status, body = _runner_status(port, token)
    assert status == 200 and body["status"] == "idle"
    assert _runner_status(port, None)[0] == 401


@pytest.fixture(scope="module")
def real_runner_shell(tls: ShellTls) -> Iterator[Shell]:
    started = Shell(tls, runner_command=None)
    try:
        yield started
    finally:
        started.remove()


def test_shell_default_entrypoint_starts_the_real_runner(real_runner_shell: Shell) -> None:
    shell = real_runner_shell
    wait_until(shell.answers, 60, "the shell image's default entrypoint to serve")
    bearer = f"bearer-{uuid.uuid4().hex}{uuid.uuid4().hex}"
    archive = claim_archive(
        "runner-boot",
        1,
        _runner_environment(bearer),
        files={
            "bundle/.claude-plugin/plugin.json": json.dumps(
                {"name": "image-proof", "version": "0.0.0"}
            ).encode(),
        },
    )
    status, answer = shell.request(
        "PUT", claim_path("runner-boot", 1), token=CONTROL_TOKEN, body=archive
    )
    assert status == 201, answer

    def ready() -> bool:
        current = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
        assert current["claim"]["child_exit"] is None, shell.logs()
        if current["claim"]["ready"] is not True:
            return False
        status, answer = shell.request(
            "GET", proxy_base_path("runner-boot", 1) + "/v1/status", token=bearer
        )
        assert status == 200, answer
        return answer["ready"] is True

    wait_until(ready, 60, "the shell's real runner child to serve")
    status, answer = shell.request(
        "GET", proxy_base_path("runner-boot", 1) + "/v1/status", token=bearer
    )
    assert status == 200 and answer["ready"] is True and answer["status"] == "idle", answer


@pytest.fixture(scope="module")
def tls(tmp_path_factory: pytest.TempPathFactory) -> ShellTls:
    return make_shell_tls(tmp_path_factory.mktemp("tls"))


@pytest.fixture
def shell(tls: ShellTls) -> Iterator[Shell]:
    started = Shell(tls)
    try:
        wait_until(started.answers, 60, "the shell to serve")
        yield started
    finally:
        started.remove()


def _claim(
    shell: Shell, claim: str, generation: int, env: dict[str, str]
) -> tuple[str, dict[str, Any]]:
    bearer = f"bearer-{uuid.uuid4().hex}{uuid.uuid4().hex}"
    body = claim_archive(
        claim,
        generation,
        {"CURIE_RUNNER_TOKEN": bearer, "HOSTILE_KEY_PATH": KEY_PATH, **env},
        files={"bundle/hostile_child.py": HOSTILE},
    )
    status, answer = shell.request(
        "PUT", claim_path(claim, generation), token=CONTROL_TOKEN, body=body
    )
    assert status == 201, answer

    def ready() -> bool:
        current = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
        assert current["claim"]["child_exit"] is None, shell.logs()
        return bool(current["claim"]["ready"])

    wait_until(ready, 60, f"claim {claim} ready")
    status, report = shell.request(
        "GET", proxy_base_path(claim, generation) + "/report", token=bearer
    )
    assert status == 200, report
    return bearer, report


def _retire(shell: Shell, claim: str, generation: int) -> dict[str, Any]:
    status, answer = shell.request(
        "DELETE", claim_path(claim, generation) + "?mode=retire", token=CONTROL_TOKEN
    )
    assert status == 200, answer
    return answer


def _flood(shell: Shell, claim: str, generation: int, bearer: str) -> Any:
    connection = http.client.HTTPSConnection(
        "localhost", shell.port, timeout=30, context=client_context(shell.tls)
    )
    connection.request(
        "GET",
        proxy_base_path(claim, generation) + "/flood",
        headers={"Authorization": f"Bearer {bearer}"},
    )
    response = connection.getresponse()
    assert len(response.read(1 << 20)) == 1 << 20
    return connection, response


def _drained_after(response: Any) -> int:
    """Read what is left of a cut stream; it must end, not keep flowing.

    Bytes already in socket buffers still arrive, and published container
    ports add buffering hops, so the bound here is loose; the in-process
    supervisor test bounds it on loopback.
    """

    after = 0
    try:
        while chunk := response.read(65536):
            after += len(chunk)
            assert after < 32 << 20, "the stream kept flowing after retirement"
    except (http.client.IncompleteRead, ConnectionError, ssl.SSLError):
        pass
    return after


def test_a_recycled_shell_holds_nothing_from_the_previous_claim(shell: Shell) -> None:
    marker = uuid.uuid4().hex
    bearer, planted = _claim(
        shell, "claim-a", 1, {"HOSTILE_PHASE": "plant", "HOSTILE_MARKER": marker}
    )
    assert shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]["isolated"]
    # The child reached only its own claim tree and the scratch roots.
    outside = [path for path in planted["written"] if not path.startswith(ALLOWED)]
    assert outside == [], outside
    # /dev/mqueue takes only queues (made through mq_open below), not files.
    assert {"/tmp", "/var/tmp", "/dev/shm", "/run/lock"} <= set(planted["written"])
    assert planted["ipc_created"] == [True, True, True, True]
    assert planted["supervisor_environ_readable"] is False
    assert planted["key_readable"] is False
    assert planted["core_limit"] == [0, 0]
    assert planted["no_new_privs"] is True

    # A stream still open at retirement is cut before retirement answers.
    connection, stream = _flood(shell, "claim-a", 1, bearer)
    retired = _retire(shell, "claim-a", 1)
    assert retired["outcome"] == "cleaned", shell.logs()
    _drained_after(stream)
    connection.close()

    _, probed = _claim(
        shell,
        "claim-b",
        2,
        {
            "HOSTILE_PHASE": "probe",
            "HOSTILE_MARKER": uuid.uuid4().hex,
            "HOSTILE_LOOK_FOR": marker,
            "HOSTILE_LINGERING_PID": str(planted["lingering_pid"]),
        },
    )
    assert probed == {
        "markers": [],
        "sysv_ipc": [],
        "posix_queue_left": False,
        "lingering_alive": False,
        # Every shared directory keeps the mode and owner it had before.
        "scratch_modes": planted["scratch_modes"],
    }
    assert _retire(shell, "claim-b", 2)["outcome"] == "cleaned", shell.logs()
    assert shell.request("GET", "/_curie/supervisor/healthz", token=CONTROL_TOKEN)[0] == 200


def test_a_malformed_runner_status_quarantines_the_shell(shell: Shell) -> None:
    _claim(
        shell,
        "claim-a",
        1,
        {"HOSTILE_PHASE": "plant", "HOSTILE_MARKER": uuid.uuid4().hex, "HOSTILE_BAD_STATUS": "1"},
    )
    retired = _retire(shell, "claim-a", 1)
    assert retired["outcome"] == "quarantined"
    assert retired["shell"]["quarantine_reason"] == "history-unsettled"
    assert retired["shell"]["claim"] is None
    assert shell.request("GET", "/_curie/supervisor/healthz", token=CONTROL_TOKEN)[0] == 503


def _runner_processes(shell: Shell) -> list[str]:
    """Processes in the container not run by root: only a runner child is."""

    listing = _docker(
        "exec",
        shell.name,
        "python",
        "-c",
        "import os\n"
        "for pid in filter(str.isdigit, os.listdir('/proc')):\n"
        "    try:\n"
        "        if os.stat(f'/proc/{pid}').st_uid != 0:\n"
        "            print(pid, open(f'/proc/{pid}/cmdline', 'rb').read())\n"
        "    except OSError:\n"
        "        pass\n",
    )
    return listing.splitlines()


def test_a_create_whose_caller_hangs_up_mid_staging_quarantines_the_shell(
    shell: Shell,
) -> None:
    # Enough members that staging outlasts the hang-up.
    filler = {f"bundle/filler/{index:05d}": b"x" for index in range(40000)}
    body = claim_archive(
        "claim-a",
        1,
        {
            "CURIE_RUNNER_TOKEN": f"bearer-{uuid.uuid4().hex}{uuid.uuid4().hex}",
            "HOSTILE_KEY_PATH": KEY_PATH,
            "HOSTILE_PHASE": "plant",
            "HOSTILE_MARKER": uuid.uuid4().hex,
        },
        files={"bundle/hostile_child.py": HOSTILE, **filler},
    )
    head = (
        f"PUT {claim_path('claim-a', 1)} HTTP/1.1\r\nHost: localhost\r\n"
        f"Authorization: Bearer {CONTROL_TOKEN}\r\n"
        f"Content-Type: application/x-tar\r\nContent-Length: {len(body)}\r\n\r\n"
    ).encode()
    raw = socket.create_connection(("localhost", shell.port), timeout=60)
    caller = client_context(shell.tls).wrap_socket(raw, server_hostname="localhost")
    try:
        caller.sendall(head + body)

        def staging() -> bool:
            state = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
            assert state["state"] in ("idle", "staging"), f"staging ended first: {state}"
            return state["state"] == "staging"

        wait_until(staging, 60, "staging to start")
    finally:
        # Gone before any answer: the caller never learns if the claim started.
        caller.close()

    def settled() -> bool:
        state = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
        return state["state"] == "quarantined"

    wait_until(settled, 120, "the abandoned create to be ended")
    state = shell.request("GET", "/_curie/supervisor/v1/shell", token=CONTROL_TOKEN)[1]
    assert state["quarantine_reason"] == "create-unacknowledged", shell.logs()
    assert state["claim"] is None
    assert state["last"]["outcome"] == "quarantined"
    assert shell.request("GET", "/_curie/supervisor/healthz", token=CONTROL_TOKEN)[0] == 503
    # The runner child is stopped and the claim's files are gone.
    assert _runner_processes(shell) == []
    claims = _docker("exec", shell.name, "ls", "-A", "/var/lib/curie-shell/claims")
    assert claims.strip() == ""
    # A retry of the same claim is refused, never acknowledged.
    status, answer = shell.request("PUT", claim_path("claim-a", 1), token=CONTROL_TOKEN, body=body)
    assert status in (409, 503), answer


def test_an_image_path_the_runner_could_write_stops_the_shell(tls: ShellTls) -> None:
    # The same image with one more directory left writable to the runner uid.
    broken = Shell(
        tls,
        command=["sh", "-c", "chmod o+w /opt && exec python -m curie_shell_supervisor"],
    )
    try:
        wait_until(lambda: not broken.running(), 60, "the shell to refuse")
        assert broken.exit_code() == 78
        assert "/opt" in broken.logs()
    finally:
        broken.remove()

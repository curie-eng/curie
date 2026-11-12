"""The shell supervisor, driven as a real process over HTTPS.

The runner child is ``fake_runner.py``: the ACI's bearer, status and NDJSON
shape without a model. Every assertion is made from outside the supervisor:
its HTTP answers, the processes left on the host, and the files on disk.
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest
from curie_shell_supervisor.protocol import claim_path, proxy_base_path
from curie_test_support.static_shell import (
    CONTROL_TOKEN,
    SupervisorProcess,
    claim_archive,
    client_context,
    wait_gone,
    wait_until,
)

BEARER = "bearer-" + "a" * 40
SECOND_BEARER = "bearer-" + "b" * 40


def _env(records: Path, shell: SupervisorProcess, bearer: str = BEARER, **extra: str) -> dict:
    return {
        "CURIE_RUNNER_TOKEN": bearer,
        "FAKE_RECORD_DIR": str(records),
        "FAKE_SCRUB_DIR": str(shell.scrub_dir),
        **extra,
    }


def _create(shell: SupervisorProcess, claim: str, generation: int, body: bytes) -> tuple[int, Any]:
    return shell.request(
        "PUT",
        claim_path(claim, generation),
        token=CONTROL_TOKEN,
        body=body,
        headers={"Content-Type": "application/x-tar"},
    )


def _retire(shell: SupervisorProcess, claim: str, generation: int) -> tuple[int, Any]:
    return shell.request("DELETE", claim_path(claim, generation), token=CONTROL_TOKEN)


def _wait_ready(shell: SupervisorProcess, timeout: float = 15.0) -> dict[str, Any]:
    def ready() -> bool:
        claim = shell.shell().get("claim")
        return bool(claim and claim["ready"])

    wait_until(ready, timeout, f"runner readiness, see {shell.log_path}")
    return shell.shell()


def _alive(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return False
    return state[state.rfind(")") + 2] not in "ZX"


def _start(
    shell: SupervisorProcess,
    records: Path,
    claim: str = "claim-a",
    generation: int = 1,
    bearer: str = BEARER,
    **extra: str,
) -> dict[str, Any]:
    status, body = _create(
        shell,
        claim,
        generation,
        claim_archive(claim, generation, _env(records, shell, bearer, **extra)),
    )
    assert status == 201, body
    return _wait_ready(shell)


def _turn(
    shell: SupervisorProcess,
    claim: str,
    generation: int,
    bearer: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    return shell.request(
        "POST",
        proxy_base_path(claim, generation) + "/v1/event?trace=1",
        token=bearer,
        body=json.dumps(payload or {"lines": 2}).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
    )


def test_control_api_requires_the_control_credential(
    supervisor: SupervisorProcess, records: Path
) -> None:
    assert supervisor.request("GET", "/_curie/supervisor/healthz", token=None)[0] == 200
    assert supervisor.request("GET", "/_curie/supervisor/v1/shell", token=None)[0] == 401
    assert supervisor.request("GET", "/_curie/supervisor/v1/shell", token=BEARER)[0] == 401
    body = claim_archive("claim-a", 1, _env(records, supervisor))
    status, _ = supervisor.request("PUT", claim_path("claim-a", 1), token=BEARER, body=body)
    assert status == 401
    assert supervisor.shell()["state"] == "idle"


def test_proxy_streams_ndjson_and_preserves_headers(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    connection = http.client.HTTPSConnection(
        supervisor.host, supervisor.port, timeout=10, context=client_context(supervisor.tls)
    )
    connection.request(
        "POST",
        proxy_base_path("claim-a", 1) + "/v1/event?trace=1",
        body=json.dumps({"lines": 3, "hold": True}),
        headers={"Authorization": f"Bearer {BEARER}", "X-Test-Echo": "kept"},
    )
    response = connection.getresponse()
    assert response.status == 200
    assert response.getheader("X-Curie-Turn-Epoch") == "e" * 32
    # Streamed through, not buffered: the runner holds the turn open after its
    # first line, so that line can only arrive here if the proxy forwards it.
    first = json.loads(response.readline())
    assert first == {"i": 0, "echo": "kept", "q": "trace=1"}
    released = supervisor.request(
        "POST", proxy_base_path("claim-a", 1) + "/v1/release", token=BEARER
    )
    assert released[0] == 200
    rest = [json.loads(line) for line in response.read().splitlines()]
    assert [line["i"] for line in rest] == [1, 2]
    connection.close()


def test_a_stale_or_foreign_request_never_reaches_the_child(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    assert _turn(supervisor, "claim-a", 1, "wrong-" + "x" * 40)[0] == 401
    assert _turn(supervisor, "claim-a", 1, CONTROL_TOKEN)[0] == 401
    assert _turn(supervisor, "claim-a", 2, BEARER)[0] == 410
    assert _turn(supervisor, "claim-b", 1, BEARER)[0] == 410
    assert _retire(supervisor, "claim-a", 1)[1]["outcome"] == "cleaned"
    _start(supervisor, records, claim="claim-b", generation=2, bearer=SECOND_BEARER)
    # The first claim's delayed request, by path or by bearer, is refused.
    assert _turn(supervisor, "claim-a", 1, BEARER)[0] == 410
    assert _turn(supervisor, "claim-b", 2, BEARER)[0] == 401
    assert _turn(supervisor, "claim-b", 2, SECOND_BEARER)[0] == 200


def test_create_is_idempotent_only_for_the_same_claim_generation_and_body(
    supervisor: SupervisorProcess, records: Path
) -> None:
    body = claim_archive("claim-a", 3, _env(records, supervisor))
    assert _create(supervisor, "claim-a", 3, body)[0] == 201
    assert _create(supervisor, "claim-a", 3, body)[0] == 200
    other = claim_archive("claim-a", 3, _env(records, supervisor, SECOND_BEARER))
    assert _create(supervisor, "claim-a", 3, other)[0] == 409
    taken = claim_archive("claim-b", 4, _env(records, supervisor, SECOND_BEARER))
    status, answer = _create(supervisor, "claim-b", 4, taken)
    assert status == 409
    assert answer["shell"]["claim"]["name"] == "claim-a"
    _wait_ready(supervisor)
    assert _retire(supervisor, "claim-a", 3)[1]["outcome"] == "cleaned"
    # A lease generation the shell has already seen is never served again.
    stale = claim_archive("claim-c", 2, _env(records, supervisor))
    assert _create(supervisor, "claim-c", 2, stale)[0] == 409
    # Nor is one retired before its upload landed (a creator that lost its lease).
    assert _retire(supervisor, "claim-d", 9)[1]["outcome"] == "absent"
    late = claim_archive("claim-d", 9, _env(records, supervisor))
    assert _create(supervisor, "claim-d", 9, late)[0] == 409


def test_retire_stops_the_process_group_and_leaves_nothing_behind(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    child = int((records / "pid").read_text())
    grandchild = int((records / "grandchild").read_text())
    child_env = json.loads((records / "env.json").read_text())
    claim_root = Path(child_env["HOME"]).parent
    assert claim_root.parent == supervisor.state_root / "claims"
    assert _alive(child) and _alive(grandchild)
    # The child environment is built, never inherited from the supervisor.
    assert not any(name.startswith("CURIE_SUPERVISOR_") for name in child_env)
    assert CONTROL_TOKEN not in child_env.values()
    assert child_env["CURIE_SANDBOX_ID"] == "claim-a"
    assert (supervisor.scrub_dir / "left-by-claim").exists()
    assert (supervisor.scrub_dir / "escape-dir").is_symlink()
    scratch_mode = supervisor.scrub_dir.stat().st_mode

    status, answer = _retire(supervisor, "claim-a", 1)
    assert status == 200 and answer["outcome"] == "cleaned"
    assert not _alive(child) and not _alive(grandchild)
    assert not claim_root.exists()
    assert list((supervisor.state_root / "claims").iterdir()) == []
    assert list(supervisor.scrub_dir.iterdir()) == []
    # The shared directory keeps its mode; the planted links were removed,
    # never followed.
    assert supervisor.scrub_dir.stat().st_mode == scratch_mode
    assert not supervisor.workspace_link.exists() and not supervisor.workspace_link.is_symlink()
    assert answer["shell"]["state"] == "idle" and answer["shell"]["claim"] is None
    # Retiring the same lease again answers the same outcome and does nothing.
    assert _retire(supervisor, "claim-a", 1)[1]["outcome"] == "cleaned"


def test_a_new_claim_gets_a_fresh_directory_and_home(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    first_home = json.loads((records / "env.json").read_text())["HOME"]
    _retire(supervisor, "claim-a", 1)
    _start(supervisor, records, claim="claim-b", generation=2, bearer=SECOND_BEARER)
    second_home = json.loads((records / "env.json").read_text())["HOME"]
    assert first_home != second_home
    assert sorted(path.name for path in Path(second_home).iterdir()) == [
        "escape-dir",
        "escape-file",
        "left-by-claim",
    ]


def test_worker_disconnect_cancels_the_runner_turn(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    connection = http.client.HTTPSConnection(
        supervisor.host, supervisor.port, timeout=10, context=client_context(supervisor.tls)
    )
    connection.request(
        "POST",
        proxy_base_path("claim-a", 1) + "/v1/event",
        body=json.dumps({"lines": 3, "hold": True, "tag": "dropped"}),
        headers={"Authorization": f"Bearer {BEARER}"},
    )
    response = connection.getresponse()
    assert json.loads(response.readline())["i"] == 0
    connection.sock.close()  # type: ignore[union-attr]
    connection.close()
    wait_until((records / "cancelled").exists, 10, "the runner seeing the disconnect")
    assert (records / "cancelled").read_text() == "dropped"


def test_an_unsettled_runner_quarantines_the_shell(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records, FAKE_DURABLE="0")
    status, answer = _retire(supervisor, "claim-a", 1)
    assert status == 200 and answer["outcome"] == "quarantined"
    assert answer["shell"]["quarantine_reason"] == "history-unsettled"
    # Still stopped and wiped: quarantine withholds the slot, it keeps nothing.
    assert list((supervisor.state_root / "claims").iterdir()) == []
    assert supervisor.request("GET", "/_curie/supervisor/healthz", token=None)[0] == 503
    body = claim_archive("claim-b", 2, _env(records, supervisor, SECOND_BEARER))
    assert _create(supervisor, "claim-b", 2, body)[0] == 503


def test_a_child_exit_is_reported_and_closes_the_proxy(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    supervisor.request("POST", proxy_base_path("claim-a", 1) + "/v1/crash", token=BEARER)
    wait_until(lambda: supervisor.shell()["claim"]["child_exit"] is not None, 10, "the child exit")
    assert supervisor.shell()["claim"]["ready"] is False
    assert _turn(supervisor, "claim-a", 1, BEARER)[0] == 503


def test_staging_refusals_leave_the_shell_idle(
    supervisor: SupervisorProcess, records: Path
) -> None:
    env = _env(records, supervisor)
    cases = [
        claim_archive("claim-a", 1, env, files={"etc/passwd": b"x"}),
        claim_archive("claim-a", 2, env, files={"bundle/../../escape": b"x"}),
        claim_archive("claim-a", 3, {**env, "CURIE_SUPERVISOR_SLOT": "x"}),
        claim_archive("claim-a", 4, {k: v for k, v in env.items() if k != "CURIE_RUNNER_TOKEN"}),
        claim_archive(
            "claim-a",
            5,
            env,
            manifest={"v": 2, "claim": "claim-zzz", "generation": 5, "env": env},
        ),
        claim_archive("claim-a", 6, {**env, "CURIE_RUNNER_TOKEN": CONTROL_TOKEN}),
    ]
    for generation, body in enumerate(cases, start=1):
        status, answer = _create(supervisor, "claim-a", generation, body)
        assert status == 400, (generation, answer)
        assert supervisor.shell()["state"] == "idle"
    # A streamed upload with no declared length cannot be bounded before reading.
    chunked = supervisor.request(
        "PUT", claim_path("claim-a", 9), token=CONTROL_TOKEN, body=iter([b"x" * 512])
    )
    assert chunked[0] == 411
    assert list((supervisor.state_root / "claims").iterdir()) == []


def test_a_killed_supervisor_takes_its_runner_child_with_it(
    supervisor: SupervisorProcess, records: Path
) -> None:
    _start(supervisor, records)
    child = int((records / "pid").read_text())
    assert supervisor.process is not None
    supervisor.process.send_signal(signal.SIGKILL)
    supervisor.process.wait(timeout=5)
    assert wait_gone(child, 5), "the runner child outlived its supervisor"


def test_restart_removes_an_interrupted_upload_before_accepting_a_claim(
    supervisor: SupervisorProcess, records: Path
) -> None:
    unrelated = [
        supervisor.state_root / name
        for name in (
            "notes",
            ".upload-not-an-archive",
            ".upload-abcdefgh.tar",
            ".upload-ABCDEFGH",
            ".upload-abcdefg",
            ".upload-abcdefghi",
        )
    ]
    for path in unrelated:
        path.write_bytes(b"keep")
    nested = supervisor.state_root / "unrelated" / ".upload-abcdefgh"
    nested.parent.mkdir()
    nested.write_bytes(b"keep")
    body = claim_archive(
        "claim-a",
        1,
        _env(records, supervisor, ACME_UPLOAD_TOKEN="placeholder-upload-token"),
        files={"bundle/padding": b"x" * 128_000},
    )
    connection = http.client.HTTPSConnection(
        supervisor.host, supervisor.port, timeout=10, context=client_context(supervisor.tls)
    )
    try:
        connection.putrequest("PUT", claim_path("claim-a", 1))
        connection.putheader("Authorization", f"Bearer {CONTROL_TOKEN}")
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders()
        connection.send(body[:65_536])

        def uploaded() -> bool:
            archives = set(supervisor.state_root.glob(".upload-*")) - set(unrelated)
            return len(archives) == 1 and next(iter(archives)).stat().st_size >= 65_536

        wait_until(uploaded, 10, "the partial archive reaching disk")
        archive = next(iter(set(supervisor.state_root.glob(".upload-*")) - set(unrelated)))
        assert supervisor.process is not None
        supervisor.process.kill()
        supervisor.process.wait(timeout=5)
    finally:
        connection.close()
    assert archive.is_file()
    assert archive.stat().st_size < len(body)
    supervisor.start()
    assert not archive.exists()
    assert all(path.read_bytes() == b"keep" for path in [*unrelated, nested])
    assert "removed 1 stale upload archives" in supervisor.log_path.read_text()
    _start(supervisor, records)


def test_startup_unlinks_upload_symlinks_without_touching_their_targets(
    supervisor: SupervisorProcess, records: Path, tmp_path: Path
) -> None:
    supervisor.stop()
    target = tmp_path / "credential-file"
    target.write_bytes(b"placeholder-upload-token")
    directory = tmp_path / "credential-directory"
    directory.mkdir()
    nested = directory / ".upload-abcdefgh"
    nested.write_bytes(b"keep")
    links = [
        supervisor.state_root / f".upload-{suffix}"
        for suffix in ("abcdefgh", "ijklmnop", "qrstuvwx")
    ]
    links[0].symlink_to(target)
    links[1].symlink_to(directory, target_is_directory=True)
    links[2].symlink_to(tmp_path / "absent")
    unreadable = supervisor.state_root / ".upload-0123_abz"
    unreadable.write_bytes(b"placeholder-upload-token")
    unreadable.chmod(0)
    supervisor.start()
    assert all(not path.exists() and not path.is_symlink() for path in [*links, unreadable])
    assert target.read_bytes() == b"placeholder-upload-token"
    assert nested.read_bytes() == b"keep"
    logs = supervisor.log_path.read_text()
    assert "removed 4 stale upload archives" in logs
    assert "placeholder-upload-token" not in logs
    assert all(path.name not in logs and str(path) not in logs for path in [*links, unreadable])
    _start(supervisor, records)


def test_startup_refuses_to_serve_when_an_upload_cannot_be_removed(
    supervisor: SupervisorProcess, records: Path
) -> None:
    supervisor.stop()
    obstacle = supervisor.state_root / ".upload-abcdefgh"
    obstacle.mkdir()
    contents = obstacle / "credential-file"
    contents.write_bytes(b"placeholder-upload-token")
    with pytest.raises(RuntimeError, match="stale upload cleanup failed.*Is a directory"):
        supervisor.start()
    assert supervisor.process is not None
    assert supervisor.process.wait(timeout=5) == 78
    with pytest.raises(OSError):
        _create(supervisor, "claim-a", 1, claim_archive("claim-a", 1, _env(records, supervisor)))
    assert contents.read_bytes() == b"placeholder-upload-token"
    logs = supervisor.log_path.read_text()
    assert "refusing to serve" in logs
    assert "placeholder-upload-token" not in logs
    assert obstacle.name not in logs and str(obstacle) not in logs
    obstacle.rename(supervisor.state_root / "unrelated")
    supervisor.start()
    _start(supervisor, records)


_FAULT_SCRIPT = textwrap.dedent(
    """
    import asyncio, sys
    from pathlib import Path
    from curie_shell_supervisor.child import RunnerChild
    from curie_shell_supervisor.server import Supervisor

    class Lease:
        def __init__(self, child): self.child = child

    async def main():
        child = await RunnerChild.spawn(
            [sys.executable, "-c",
             "import subprocess,sys,time;"
             "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(600)']);"
             "open(sys.argv[1],'w').write(str(p.pid));time.sleep(600)", sys.argv[1]],
            env={"PATH": "/usr/bin:/bin"}, cwd=Path("/"), uid=None, gid=None)
        Path(sys.argv[2]).write_text(str(child.pid))
        supervisor = Supervisor.__new__(Supervisor)
        supervisor._lease = Lease(child)
        while not Path(sys.argv[1]).exists():
            await asyncio.sleep(0.05)
        async def fails():
            raise RuntimeError("boom")
        task = asyncio.ensure_future(fails())
        task.add_done_callback(lambda t: supervisor.die(str(t.exception())))
        await asyncio.sleep(30)

    asyncio.run(main())
    """
)


def test_a_supervisor_fault_ends_the_whole_shell(tmp_path: Path) -> None:
    grandchild_file, child_file = tmp_path / "grandchild", tmp_path / "child"
    result = subprocess.run(  # noqa: S603 - fixed argv
        [sys.executable, "-c", _FAULT_SCRIPT, str(grandchild_file), str(child_file)],
        timeout=30,
        check=False,
    )
    assert result.returncode == 70
    for pid in (int(child_file.read_text()), int(grandchild_file.read_text())):
        assert wait_gone(pid, 5), pid


@pytest.mark.parametrize("missing", ["CURIE_SUPERVISOR_TLS_CERT_FILE", "CURIE_SUPERVISOR_SLOT"])
def test_supervisor_refuses_to_start_without_tls_or_identity(
    tls: Any, tmp_path: Path, missing: str
) -> None:
    env = {
        "PATH": os.environ["PATH"],
        "CURIE_SUPERVISOR_SLOT": "slot-a",
        "CURIE_SUPERVISOR_CONTROL_TOKEN": CONTROL_TOKEN,
        "CURIE_SUPERVISOR_TLS_CERT_FILE": str(tls.cert),
        "CURIE_SUPERVISOR_TLS_KEY_FILE": str(tls.key),
        "CURIE_SUPERVISOR_STATE_ROOT": str(tmp_path),
    }
    del env[missing]
    result = subprocess.run(  # noqa: S603 - fixed argv
        [sys.executable, "-m", "curie_shell_supervisor"],
        env=env,
        timeout=20,
        check=False,
        capture_output=True,
    )
    assert result.returncode == 78


@pytest.mark.parametrize("body", ["[]", "null", '"idle"', "{", "x" * 70_000])
def test_a_malformed_runner_status_still_stops_cleans_and_quarantines(
    supervisor: SupervisorProcess, records: Path, body: str
) -> None:
    _start(supervisor, records, FAKE_STATUS_BODY=body)
    child = int((records / "pid").read_text())
    status, answer = _retire(supervisor, "claim-a", 1)
    assert (status, answer["outcome"]) == (200, "quarantined")
    assert answer["shell"]["quarantine_reason"] == "history-unsettled"
    assert wait_gone(child, 5)
    assert list((supervisor.state_root / "claims").iterdir()) == []
    assert supervisor.request("GET", "/_curie/supervisor/healthz", token=None)[0] == 503
    assert _turn(supervisor, "claim-a", 1, BEARER)[0] == 410


def test_legacy_claim_manifests_are_refused_before_starting_a_runner(
    supervisor: SupervisorProcess, records: Path
) -> None:
    env = _env(records, supervisor)
    body = claim_archive(
        "claim-a",
        1,
        env,
        manifest={
            "v": 1,
            "claim": "claim-a",
            "generation": 1,
            "env": env,
        },
    )
    status, _ = _create(supervisor, "claim-a", 1, body)
    assert status == 400
    current = supervisor.shell()
    assert current["state"] == "idle" and current["claim"] is None
    assert not (records / "env.json").exists()
    status, _ = _create(supervisor, "claim-a", 2, claim_archive("claim-a", 2, env))
    assert status == 201
    wait_until(lambda: supervisor.shell()["claim"]["ready"], 10, "current manifest starts a runner")

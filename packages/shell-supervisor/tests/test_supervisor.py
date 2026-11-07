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

    status, answer = _retire(supervisor, "claim-a", 1)
    assert status == 200 and answer["outcome"] == "cleaned"
    assert not _alive(child) and not _alive(grandchild)
    assert not claim_root.exists()
    assert list((supervisor.state_root / "claims").iterdir()) == []
    assert list(supervisor.scrub_dir.iterdir()) == []
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
    assert sorted(path.name for path in Path(second_home).iterdir()) == ["left-by-claim"]


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
            manifest={"v": 1, "claim": "claim-zzz", "generation": 5, "env": env},
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

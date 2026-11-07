"""One-process WAL for a network volume, and the unchanged default beside it.

SQLite keeps a shared-memory wal index unless ``locking_mode=EXCLUSIVE`` is set
before the first WAL access. These tests drive ``MailState`` and the real
``python -m curie_mail_adapter`` process. Nothing inside the adapter is patched.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

from _support import IngressState, MailState, adapter_env, exit_of, free_port, spawn_adapter
from curie_mail_adapter.state import MailState as DurableMailState

_MAX_PENDING = 20
_MAX_BYTES = 8 * 1024 * 1024


def _open(path: Path, *, exclusive_wal: bool = False) -> DurableMailState:
    return DurableMailState(
        str(path),
        max_pending=_MAX_PENDING,
        max_bytes=_MAX_BYTES,
        exclusive_wal=exclusive_wal,
    )


def _child_open(path: Path) -> subprocess.CompletedProcess[str]:
    """A second process opening the same file in exclusive WAL mode."""
    script = textwrap.dedent(
        """
        import sys

        from curie_mail_adapter.state import MailState

        try:
            state = MailState(
                sys.argv[1], max_pending=20, max_bytes=8 * 1024 * 1024, exclusive_wal=True
            )
        except RuntimeError as error:
            print(f"BOOT_REFUSED {error}", flush=True)
            sys.exit(2)
        print("BOOT_OK", flush=True)
        state.close()
        """
    )
    return subprocess.run(
        [sys.executable, "-c", script, str(path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )


def test_second_process_opening_exclusive_wal_fails_closed(tmp_path: Path) -> None:
    """A second process must not become a second writer on the same file."""
    path = tmp_path / "mail-state.sqlite3"
    holder = _open(path, exclusive_wal=True)
    try:
        opened = _child_open(path)
    finally:
        holder.close()

    assert opened.returncode != 0
    assert "BOOT_REFUSED" in opened.stdout
    assert "CURIE_MAIL_EXCLUSIVE_WAL" in opened.stdout
    assert "already held" in opened.stdout
    assert "BOOT_OK" not in opened.stdout


def test_second_connection_opening_exclusive_wal_fails_closed(tmp_path: Path) -> None:
    """The same refusal holds for a second connection in this process."""
    path = tmp_path / "mail-state.sqlite3"
    holder = _open(path, exclusive_wal=True)
    try:
        try:
            _open(path, exclusive_wal=True)
        except RuntimeError as error:
            message = str(error)
        else:
            raise AssertionError("a second exclusive connection opened the held file")
    finally:
        holder.close()

    assert "CURIE_MAIL_EXCLUSIVE_WAL" in message
    assert "already held" in message


def test_exclusive_wal_restart_after_sigkill_recovers_committed_work(tmp_path: Path) -> None:
    """An abrupt stop keeps committed deliveries and send witnesses, not an open write."""
    path = tmp_path / "mail-state.sqlite3"
    script = textwrap.dedent(
        """
        import sys
        import time

        from curie_mail_adapter.state import MailState

        state = MailState(
            sys.argv[1], max_pending=20, max_bytes=8 * 1024 * 1024, exclusive_wal=True
        )
        assert state.admit({"message_id": "msg-pending", "thread_id": "thr-1"}) == "admitted"
        assert state.claim_event("ev-witness", "thr-1", "msg-pending", "owner-1") == "claimed"
        state.record_send_attempt("ev-witness")
        with state.transaction() as connection:
            connection.execute(
                "INSERT INTO deliveries("
                "message_id, conversation_id, reply_ref, summary_json, state, "
                "created_at, updated_at"
                ") VALUES('msg-open', 'thr-1', 'msg-open', '{}', 'body_pending', 1, 1)"
            )
            print("COMMITTED", flush=True)
            time.sleep(60)
        """
    )
    writer = subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert writer.stdout is not None
        assert writer.stdout.readline().strip() == "COMMITTED"
        writer.kill()
        writer.communicate(timeout=15)
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate(timeout=15)

    reopened = _open(path, exclusive_wal=True)
    try:
        with reopened.lock:
            assert reopened.connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
            assert reopened.connection.execute("PRAGMA locking_mode").fetchone() == ("exclusive",)
            assert reopened.connection.execute("PRAGMA synchronous").fetchone() == (2,)
            assert reopened.connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert not Path(f"{path}-shm").exists()
        assert reopened.delivery("msg-pending") == {"state": "body_pending", "turn": None}
        assert reopened.delivery("msg-open") is None
        attempted = reopened.send_attempted_at("ev-witness")
        assert attempted is not None
        assert attempted <= time.time()
    finally:
        reopened.close()


def test_default_mode_keeps_normal_locking_and_a_second_connection(tmp_path: Path) -> None:
    """Unset exclusive WAL still uses normal locking and admits another connection."""
    path = tmp_path / "mail-state.sqlite3"
    first = _open(path)
    try:
        second = _open(path)
        try:
            with first.lock:
                assert first.connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
                assert first.connection.execute("PRAGMA locking_mode").fetchone() == ("normal",)
                assert first.connection.execute("PRAGMA synchronous").fetchone() == (2,)
            with second.lock:
                assert second.connection.execute("PRAGMA locking_mode").fetchone() == ("normal",)
            assert second.admit({"message_id": "msg-default", "thread_id": "thr-default"}) == (
                "admitted"
            )
            assert first.delivery("msg-default") == {"state": "body_pending", "turn": None}
        finally:
            second.close()
    finally:
        first.close()


def test_boot_refuses_when_exclusive_wal_is_already_held(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """The process entry point fails closed instead of serving a second owner."""
    path = tmp_path / "mail-state.sqlite3"
    holder = _open(path, exclusive_wal=True)
    try:
        code, output = exit_of(
            spawn_adapter(
                adapter_env(
                    agentmail_base_url=mail.base_url,
                    api_url=ingress.url,
                    port=free_port(),
                    CURIE_MAIL_STATE_PATH=str(path),
                    CURIE_MAIL_EXCLUSIVE_WAL="true",
                )
            )
        )
    finally:
        holder.close()

    assert code != 0
    assert "CURIE_MAIL_EXCLUSIVE_WAL" in output
    assert "already held" in output

"""Crash/restart behavior through real HTTP processes and one temporary SQLite file."""

from __future__ import annotations

import subprocess
import sys
import threading
from collections.abc import Callable
from pathlib import Path

from _support import (
    IngressState,
    MailState,
    adapter_env,
    completed,
    exit_of,
    free_port,
    get,
    post_event,
    spawn_adapter,
    stop,
    update,
    wait_for_readyz,
    wait_until,
)
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.state import MailState as DurableMailState


def _env(mail: MailState, ingress: IngressState, port: int, state_path: Path) -> dict[str, str]:
    return adapter_env(
        agentmail_base_url=mail.base_url,
        api_url=ingress.url,
        port=port,
        CURIE_MAIL_STATE_PATH=str(state_path),
    )


def _sigkill(proc: subprocess.Popen[str]) -> str:
    proc.kill()
    output, _ = proc.communicate(timeout=15)
    return output


def test_sigkill_mid_transaction_recovers_wal_without_a_phantom_terminal_state(
    tmp_path: Path,
) -> None:
    """A killed writer leaves no partial admission and the same WAL stays usable."""
    state_path = tmp_path / "mail-state.sqlite3"
    writer = subprocess.Popen(
        [
            sys.executable,
            "-c",
            """
import sys
import time

from curie_mail_adapter.state import MailState

state = MailState(sys.argv[1], max_pending=20, max_bytes=8 * 1024 * 1024)
with state.transaction() as connection:
    connection.execute(
        \"INSERT INTO deliveries(\"
        \"message_id, conversation_id, reply_ref, summary_json, state, \"
        \"created_at, updated_at\"
        \") VALUES('msg-uncommitted', 'thr-crash', 'msg-uncommitted', '{}', \"
        \"'body_pending', 1, 1)\"
    )
    print("WRITE_OPEN", flush=True)
    time.sleep(60)
""",
            str(state_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert writer.stdout is not None
        assert writer.stdout.readline().strip() == "WRITE_OPEN"
        writer.kill()
        writer.communicate(timeout=15)
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate(timeout=15)

    reopened = DurableMailState(
        str(state_path), max_pending=20, max_bytes=8 * 1024 * 1024
    )
    try:
        with reopened.lock:
            assert reopened.connection.execute("PRAGMA journal_mode").fetchone() == (
                "wal",
            )
            assert reopened.connection.execute("PRAGMA synchronous").fetchone() == (2,)
            assert reopened.connection.execute("PRAGMA integrity_check").fetchone() == (
                "ok",
            )
        assert reopened.delivery("msg-uncommitted") is None

        summary = {
            "message_id": "msg-after-crash",
            "thread_id": "thr-crash",
            "from": "human@example.com",
            "subject": "after crash",
            "labels": [],
        }
        assert reopened.admit(summary) == "admitted"
        reopened.store_turn(
            "msg-after-crash",
            {
                "kind": "email",
                "address": "sandbox@agentmail.to",
                "delivery_id": "msg-after-crash",
                "conversation_id": "thr-crash",
                "author": "human@example.com",
                "text": "after crash",
                "reply_ref": "msg-after-crash",
            },
        )
        assert (
            reopened.claim_event(
                "ev-after-crash", "thr-crash", "msg-after-crash", "owner-1"
            )
            == "claimed"
        )
        reopened.finish_event("ev-after-crash")
        reopened.finish_reply("thr-crash", "msg-after-crash")

        assert (
            reopened.claim_event(
                "ev-after-crash", "thr-crash", "msg-after-crash", "owner-2"
            )
            == "done"
        )
        with reopened.lock:
            assert reopened.connection.execute(
                "SELECT count(*) FROM completion_events "
                "WHERE event_id='ev-after-crash' AND delivered=1"
            ).fetchone() == (1,)
    finally:
        reopened.close()


def test_sigterm_drains_an_in_flight_egress_handler_before_sqlite_close(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """Shutdown joins a provider-blocked completion before closing durable state."""
    state_path = tmp_path / "mail-state.sqlite3"
    first_port = free_port()
    first = spawn_adapter(_env(mail, ingress, first_port, state_path))
    completion_result: list[tuple[int, object]] = []
    try:
        wait_for_readyz(first_port, first)
        mail.add_inbound("msg-shutdown", "thr-shutdown")
        assert wait_until(lambda: ingress.delivery_ids() == ["msg-shutdown"])
        assert post_event(
            f"http://127.0.0.1:{first_port}/",
            update(
                "reply survives shutdown",
                conversation_id="thr-shutdown",
                reply_ref="msg-shutdown",
            ),
        )[0] == 200

        mail.hold_replies()
        completion = threading.Thread(
            target=lambda: completion_result.append(
                post_event(
                    f"http://127.0.0.1:{first_port}/",
                    completed("ev-shutdown", "thr-shutdown", "msg-shutdown"),
                )
            ),
            daemon=True,
        )
        completion.start()
        assert mail.reply_entered.wait(20), "completion never reached the provider"

        first.terminate()
        assert not wait_until(
            lambda: first.poll() is not None, timeout=0.2
        ), "SIGTERM exited before the in-flight handler drained"
        mail.release_replies()
        completion.join(20)
        assert not completion.is_alive()
        code, output = exit_of(first)

        assert code == 0, output
        assert completion_result and completion_result[0][0] == 200
        assert "ProgrammingError" not in output
        assert len(mail.replies_to("msg-shutdown")) == 1
    finally:
        mail.release_replies()
        if first.poll() is None:
            stop(first)

    second_port = free_port()
    second = spawn_adapter(_env(mail, ingress, second_port, state_path))
    try:
        wait_for_readyz(second_port, second)
        assert post_event(
            f"http://127.0.0.1:{second_port}/",
            completed("ev-shutdown", "thr-shutdown", "msg-shutdown"),
        )[0] == 200
        assert len(mail.replies_to("msg-shutdown")) == 1
    finally:
        stop(second)


def test_restart_confirmation_delivers_downtime_mail_without_repriming(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """First boot primes once; a replacement resumes rather than burning downtime mail."""
    state_path = tmp_path / "mail-state.sqlite3"
    first_port = free_port()
    first = spawn_adapter(_env(mail, ingress, first_port, state_path))
    try:
        wait_for_readyz(first_port, first)
    finally:
        stop(first)

    mail.add_inbound("msg-downtime", "thr-downtime")
    second_port = free_port()
    second = spawn_adapter(_env(mail, ingress, second_port, state_path))
    try:
        wait_for_readyz(second_port, second)
        assert wait_until(lambda: ingress.delivery_ids() == ["msg-downtime"])
    finally:
        stop(second)


def test_readyz_after_startup_makes_no_agentmail_call(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """Steady readiness is a local SQLite check, not a third-party dependency."""
    port = free_port()
    proc = spawn_adapter(_env(mail, ingress, port, tmp_path / "mail-state.sqlite3"))
    try:
        wait_for_readyz(port, proc)
        calls_after_startup = mail.list_calls
        mail.fail_next_list = 500

        for _ in range(3):
            assert get(f"http://127.0.0.1:{port}/readyz")[0] == 200

        assert mail.list_calls == calls_after_startup
        assert mail.fail_next_list == 500, "readiness consumed the armed provider failure"
    finally:
        stop(proc)


def test_scoped_token_401_stays_pending_across_recreate(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """A token rotation may restart the only replica without losing its delivery."""
    state_path = tmp_path / "mail-state.sqlite3"
    first_port = free_port()
    first = spawn_adapter(_env(mail, ingress, first_port, state_path))
    try:
        wait_for_readyz(first_port, first)
        ingress.response = (401, {"detail": "expired scoped token"})
        mail.add_inbound("msg-rotate", "thr-rotate")
        assert wait_until(lambda: ingress.attempts > 0)
        _sigkill(first)
    finally:
        if first.poll() is None:
            stop(first)

    ingress.requests.clear()
    ingress.attempts = 0
    ingress.attempt_times.clear()
    ingress.response = (
        200,
        {"event_id": "chn-rotate", "stream_id": "1-0", "duplicate": False},
    )
    second_port = free_port()
    second = spawn_adapter(_env(mail, ingress, second_port, state_path))
    try:
        wait_for_readyz(second_port, second)
        assert wait_until(lambda: ingress.delivery_ids() == ["msg-rotate"])
    finally:
        stop(second)


def test_expired_completion_lease_is_reclaimed_after_sigkill_reopen(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """Admitted reply text survives SIGKILL and completion never 503s forever."""
    state_path = tmp_path / "mail-state.sqlite3"
    first_port = free_port()
    first = spawn_adapter(_env(mail, ingress, first_port, state_path))
    try:
        wait_for_readyz(first_port, first)
        mail.add_inbound("msg-lease", "thr-lease")
        assert wait_until(lambda: ingress.delivery_ids() == ["msg-lease"])
        status, _ = post_event(
            f"http://127.0.0.1:{first_port}/",
            update("durable answer", conversation_id="thr-lease", reply_ref=None),
        )
        assert status == 200
        mail.fail_next_reply = 500
        mail.hold_replies()
        completion = threading.Thread(
            target=lambda: post_event(
                f"http://127.0.0.1:{first_port}/",
                completed("ev-lease", conversation_id="thr-lease", reply_ref="msg-lease"),
            ),
            daemon=True,
        )
        completion.start()
        assert mail.reply_entered.wait(20), "completion never reached the provider"
        _sigkill(first)
        mail.release_replies()
        completion.join(20)
    finally:
        if first.poll() is None:
            stop(first)

    second_port = free_port()
    second = spawn_adapter(_env(mail, ingress, second_port, state_path))
    try:
        wait_for_readyz(second_port, second)
        status, _ = post_event(
            f"http://127.0.0.1:{second_port}/",
            completed("ev-lease", conversation_id="thr-lease", reply_ref="msg-lease"),
        )

        assert status == 200
        assert mail.replies_to("msg-lease") == [
            next(text for mid, text in mail.replies if mid == "msg-lease")
        ]
        assert mail.replies_to("msg-lease")[0].startswith("durable answer")
    finally:
        stop(second)


def test_crash_after_provider_accept_uses_thread_witness_before_resend(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """An accepted-but-unacked reply converges to 200 with zero extra sends."""
    state_path = tmp_path / "mail-state.sqlite3"
    first_port = free_port()
    first = spawn_adapter(_env(mail, ingress, first_port, state_path))
    try:
        wait_for_readyz(first_port, first)
        mail.add_inbound("msg-ambiguous", "thr-ambiguous")
        assert wait_until(lambda: ingress.delivery_ids() == ["msg-ambiguous"])
        assert post_event(
            f"http://127.0.0.1:{first_port}/",
            update("one answer", conversation_id="thr-ambiguous", reply_ref=None),
        )[0] == 200
        mail.accept_then_drop_next_reply = True
        assert post_event(
            f"http://127.0.0.1:{first_port}/",
            completed("ev-ambiguous", "thr-ambiguous", "msg-ambiguous"),
        )[0] == 502
        assert len(mail.replies_to("msg-ambiguous")) == 1
        _sigkill(first)
    finally:
        if first.poll() is None:
            stop(first)

    second_port = free_port()
    second = spawn_adapter(_env(mail, ingress, second_port, state_path))
    try:
        wait_for_readyz(second_port, second)
        status, _ = post_event(
            f"http://127.0.0.1:{second_port}/",
            completed("ev-ambiguous", "thr-ambiguous", "msg-ambiguous"),
        )
        assert status == 200
        assert len(mail.replies_to("msg-ambiguous")) == 1
    finally:
        stop(second)


def test_serialized_sqlite_writer_handles_poll_and_concurrent_egress(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """The poller and HTTP handler share one serialized writer without BUSY/loss."""
    url = serve_egress(adapter) + "/"
    message_ids = [f"msg-{index}" for index in range(8)]
    for index, message_id in enumerate(message_ids):
        mail.add_inbound(message_id, f"thr-{index}")
    adapter.poll_once()
    assert set(ingress.delivery_ids()) == set(message_ids)

    barrier = threading.Barrier(len(message_ids) + 1)
    statuses: list[int] = []

    def complete(index: int, message_id: str) -> None:
        barrier.wait()
        statuses.append(
            post_event(
                url,
                update(f"answer {index}", f"thr-{index}", reply_ref=message_id),
            )[0]
        )
        statuses.append(
            post_event(url, completed(f"ev-{index}", f"thr-{index}", message_id))[0]
        )

    threads = [
        threading.Thread(target=complete, args=(index, message_id), daemon=True)
        for index, message_id in enumerate(message_ids)
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    mail.add_inbound("msg-poll-race", "thr-poll-race")
    adapter.poll_once()
    for thread in threads:
        thread.join(20)

    assert statuses == [200] * (len(message_ids) * 2)
    assert {message_id for message_id, _text in mail.replies} == set(message_ids)
    assert "msg-poll-race" in ingress.delivery_ids()

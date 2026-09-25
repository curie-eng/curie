"""Several supervisors, one per Slack identity, run and stop together.

ADR-0168 decision 2: each identity has its own connection, backoff and
supervisor, and one identity failing to connect must not stop another from
serving. No real socket: fake connections, as test_supervisor.py uses.
"""

import logging
import threading
import time

import pytest
from curie_dispatcher.supervisor import BackoffPolicy, Supervisor, SupervisorGroup

from .test_supervisor import BlockingConnection, _signalling_logger, _wait_for


def test_one_identity_failing_to_connect_does_not_stop_another_from_serving() -> None:
    serving_connection = BlockingConnection()
    failures = 0

    def failing_connect() -> BlockingConnection:
        nonlocal failures
        failures += 1
        raise RuntimeError("app token revoked")

    failing = Supervisor(
        failing_connect,
        backoff=BackoffPolicy(initial_seconds=0.01, max_seconds=0.01, multiplier=2.0),
        sleep=time.sleep,
    )
    serving = Supervisor(lambda: serving_connection, sleep=lambda _seconds: None)
    group = SupervisorGroup({"default": serving, "ops-bot": failing})

    thread = threading.Thread(target=group.run, daemon=True)
    thread.start()
    try:
        assert _wait_for(lambda: serving_connection.ran and failures >= 3, timeout=2.0)
        # Still serving while the other identity keeps retrying.
        assert thread.is_alive()
        assert not serving_connection.closed
    finally:
        group.request_stop()
        thread.join(timeout=3.0)

    assert not thread.is_alive()
    assert serving_connection.closed


def test_a_group_of_one_runs_its_supervisor_on_the_calling_thread() -> None:
    ran_on: list[threading.Thread] = []

    class Once:
        def run(self) -> None:
            ran_on.append(threading.current_thread())
            supervisor.request_stop()

        def close(self) -> None:
            pass

    supervisor = Supervisor(Once, sleep=lambda _seconds: None)

    SupervisorGroup({"default": supervisor}).run()

    assert ran_on == [threading.current_thread()]


def test_an_empty_group_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one"):
        SupervisorGroup({})


def test_a_labelled_supervisor_names_itself_and_an_unlabelled_one_is_unchanged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test-supervisor-label")

    def boom() -> BlockingConnection:
        raise RuntimeError("boom")

    for label in ("Slack identity ops-bot", None):
        stopping: list[Supervisor] = []
        supervisor = Supervisor(
            boom,
            logger=logger,
            label=label,
            sleep=lambda _seconds, stopping=stopping: stopping[0].request_stop(),
        )
        stopping.append(supervisor)
        with caplog.at_level(logging.INFO, logger=logger.name):
            supervisor.run()

    messages = [record.getMessage() for record in caplog.records if record.name == logger.name]
    assert messages == [
        "Slack identity ops-bot: connection failed: boom",
        "Slack identity ops-bot: reconnecting in 1.0s (attempt 1)",
        "connection failed: boom",
        "reconnecting in 1.0s (attempt 1)",
    ]


def _refused() -> BlockingConnection:
    raise RuntimeError("app token revoked")


def test_a_stop_returns_promptly_while_a_member_is_backing_off() -> None:
    logger, backing_off = _signalling_logger("test-group-backoff-stop", "reconnecting in")
    serving_connection = BlockingConnection()
    serving = Supervisor(lambda: serving_connection, sleep=lambda _seconds: None)
    failing = Supervisor(
        _refused, backoff=BackoffPolicy(initial_seconds=60.0, max_seconds=60.0), logger=logger
    )
    group = SupervisorGroup({"default": serving, "ops-bot": failing})
    thread = threading.Thread(target=group.run, daemon=True)
    try:
        thread.start()
        assert backing_off.seen.wait(timeout=2.0)
        assert _wait_for(lambda: serving_connection.ran, timeout=2.0)

        group.request_stop()
        thread.join(timeout=2.0)

        assert not thread.is_alive()
    finally:
        logger.removeHandler(backing_off)


def test_the_group_returns_only_after_every_member_has_stopped() -> None:
    serving_connection = BlockingConnection()
    sleeping = threading.Event()
    gate = threading.Event()

    def gated_sleep(_seconds: float) -> None:
        sleeping.set()
        gate.wait()

    serving = Supervisor(lambda: serving_connection, sleep=lambda _seconds: None)
    failing = Supervisor(_refused, sleep=gated_sleep)
    group = SupervisorGroup({"default": serving, "ops-bot": failing})
    thread = threading.Thread(target=group.run, daemon=True)
    thread.start()
    try:
        assert sleeping.wait(timeout=2.0)
        assert _wait_for(lambda: serving_connection.ran, timeout=2.0)

        group.request_stop()
        thread.join(timeout=0.2)
        # `default` has stopped; `ops-bot` is still inside its sleep.
        assert serving_connection.closed
        assert thread.is_alive()
    finally:
        gate.set()
    thread.join(timeout=2.0)

    assert not thread.is_alive()


def test_a_member_that_raises_is_logged_by_name_and_restarted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test-group-member-raises")
    serving_connection = BlockingConnection()
    recovered = BlockingConnection()
    connects = 0

    def connect() -> BlockingConnection:
        nonlocal connects
        connects += 1
        if connects == 1:
            raise RuntimeError("app token revoked")
        return recovered

    def broken_sleep(_seconds: float) -> None:
        raise RuntimeError("sleep broke")

    serving = Supervisor(lambda: serving_connection, sleep=lambda _seconds: None)
    failing = Supervisor(connect, sleep=broken_sleep)
    group = SupervisorGroup(
        {"default": serving, "ops-bot": failing},
        logger=logger,
        restart_backoff=BackoffPolicy(initial_seconds=0.0, max_seconds=0.0),
    )
    thread = threading.Thread(target=group.run, daemon=True)

    with caplog.at_level(logging.INFO, logger=logger.name):
        thread.start()
        try:
            # The member that raised runs again and reaches a live connection.
            assert _wait_for(lambda: recovered.ran and serving_connection.ran, timeout=2.0)
            assert thread.is_alive()
        finally:
            group.request_stop()
            thread.join(timeout=2.0)

    assert not thread.is_alive()
    records = [r for r in caplog.records if r.name == logger.name]
    assert [r.getMessage() for r in records] == [
        "supervisor for ops-bot stopped unexpectedly; restarting in 0.0s"
    ]
    assert records[0].exc_info is not None
    assert str(records[0].exc_info[1]) == "sleep broke"


def test_a_stop_interrupts_the_pause_before_a_member_restarts() -> None:
    logger, restarting = _signalling_logger("test-group-restart-stop", "stopped unexpectedly")

    def broken_sleep(_seconds: float) -> None:
        raise RuntimeError("sleep broke")

    serving = Supervisor(BlockingConnection, sleep=lambda _seconds: None)
    failing = Supervisor(_refused, sleep=broken_sleep)
    group = SupervisorGroup(
        {"default": serving, "ops-bot": failing},
        logger=logger,
        restart_backoff=BackoffPolicy(initial_seconds=60.0, max_seconds=60.0),
    )
    thread = threading.Thread(target=group.run, daemon=True)
    try:
        thread.start()
        assert restarting.seen.wait(timeout=2.0)

        group.request_stop()
        thread.join(timeout=2.0)

        assert not thread.is_alive()
    finally:
        logger.removeHandler(restarting)

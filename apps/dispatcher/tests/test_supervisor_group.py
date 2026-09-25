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

from .test_supervisor import BlockingConnection, _wait_for


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

    thread = threading.Thread(target=group.run)
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

    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "Slack identity ops-bot: connection failed: boom",
        "Slack identity ops-bot: reconnecting in 1.0s (attempt 1)",
        "connection failed: boom",
        "reconnecting in 1.0s (attempt 1)",
    ]

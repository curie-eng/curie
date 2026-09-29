"""Subprocess driver for the library log backstop.

Not a test module. pytest must not collect it. The suite spawns it as
``[sys.executable, driver_path]`` with a closed environment and reads the
process output.

Each configured scenario imports the real entrypoint and calls its real
``main()``. The only replacement is the first post-bootstrap blocker, so
bootstrap still runs. The planted credential is a ``%s`` argument, never an
f-string, so it stays in ``record.args``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

SCENARIO_ENV = "CURIE_LIBRARY_LOG_SCENARIO"
PLANTED_SECRET_ENV = "CURIE_LIBRARY_LOG_PLANTED_SECRET"

WARNING_CARRIER = "library warning probe 2535"
INFO_CARRIER = "library info probe 2535"
SERVICE_CARRIER = "service logger probe 2535"
LIBRARY_LOGGER = "some_new_library.client"

_SERVICE_LOGGERS = {
    "mail": "curie_mail_adapter",
    "dispatcher": "curie_dispatcher",
    "worker": "curie_worker",
    "runner": "curie_runner",
}


def _emit_configured(service_logger: str) -> None:
    secret = os.environ[PLANTED_SECRET_ENV]
    logging.getLogger(LIBRARY_LOGGER).warning(WARNING_CARRIER + ": credential=%s", secret)
    logging.getLogger(LIBRARY_LOGGER).info(INFO_CARRIER)
    logging.getLogger(service_logger).info(SERVICE_CARRIER)
    raise SystemExit(0)


def _blocker(service_logger: str) -> Callable[..., None]:
    def _call(*_args: object, **_kwargs: object) -> None:
        _emit_configured(service_logger)

    return _call


def _arm(scenario: str) -> Callable[[], None]:
    service_logger = _SERVICE_LOGGERS[scenario]
    replacement = _blocker(service_logger)
    if scenario == "mail":
        import curie_mail_adapter.run as run

        run.MailAdapterConfig = replacement  # type: ignore[misc, assignment]
        return run.main
    if scenario == "dispatcher":
        import curie_dispatcher.run as run

        run.DispatcherConfig = replacement  # type: ignore[misc, assignment]
        return run.main
    if scenario == "worker":
        import curie_worker.run as run

        run.WorkerConfig = replacement  # type: ignore[misc, assignment]
        return run.main
    if scenario == "runner":
        import curie_runner.__main__ as boot

        boot._serve = replacement
        return boot.main
    raise SystemExit(f"unknown scenario {scenario!r}")


def _unconfigured() -> None:
    logging.getLogger(LIBRARY_LOGGER).warning(
        WARNING_CARRIER + ": credential=%s",
        os.environ[PLANTED_SECRET_ENV],
    )
    raise SystemExit(0)


if __name__ == "__main__":
    scenario = os.environ[SCENARIO_ENV]
    if scenario == "unconfigured":
        _unconfigured()
    _arm(scenario)()

"""Emit a stable alert record when the consumer dead letters an entry."""

from __future__ import annotations

import logging

_SOURCE_LOGGER = "curie_worker.consumer"
_ALERT_LOGGER = "curie_worker.alerts.dead_letter"
_DEAD_LETTER_MESSAGE = "dead-lettered entry %s after %d deliveries (reason=%s) -> %s"


class _DeadLetterAlertHandler(logging.Handler):
    """Translate the consumer's dead letter record into an alert signal."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if (
                record.name != _SOURCE_LOGGER
                or record.levelno != logging.ERROR
                or record.msg != _DEAD_LETTER_MESSAGE
                or not isinstance(record.args, tuple)
                or len(record.args) != 4
            ):
                return

            entry_id, delivery_count, reason, dead_stream = record.args
            _ = _DEAD_LETTER_MESSAGE % record.args
            alert = (
                f"event=curie.dead_letter entry_id={entry_id} "
                f"delivery_count={delivery_count} reason={reason} "
                f"dead_stream={dead_stream}"
            )
            logging.getLogger(_ALERT_LOGGER).critical(alert)
        except Exception:  # noqa: BLE001 - existing broad catch retained
            return


def install_dead_letter_alerting() -> None:
    """Install dead letter alerting hooks."""

    source_logger = logging.getLogger(_SOURCE_LOGGER)
    if any(
        isinstance(handler, _DeadLetterAlertHandler) for handler in source_logger.handlers
    ):
        return
    source_logger.addHandler(_DeadLetterAlertHandler())


__all__ = ["install_dead_letter_alerting"]

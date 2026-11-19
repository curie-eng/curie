"""A wake key and its signatures never reach logs or error bodies (ADR 0274 decision 2).

Every log record emitted while the real app serves key retrieval, rotation, an
admitted wake, a coalesced wake, a refused signature and a reservation failure
is captured at DEBUG, from the root logger and from any logger that does not
propagate, and searched in every rendered form.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest
from _hook_wake_support import (
    COALESCED,
    DEDUPE_UNAVAILABLE,
    INVALID_SIGNATURE,
    SIGNATURE_HEADER,
    WAKE,
    assert_queued,
    assert_refused,
    drive,
    restricted_valkey,
    rotate_path,
    secret_path,
    side_client,
    signed_headers,
    wake,
    wake_app,
    wake_environment,
)
from _migration_support import IsolatedMigrationDb


@pytest.fixture
def wake_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    yield from wake_environment(isolated_migration_db, monkeypatch)


class _Collect(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextmanager
def captured_logs() -> Iterator[_Collect]:
    """Collect every record at DEBUG, including from non-propagating loggers.

    Attached after the app has started, so a logging configuration applied at
    startup cannot detach it.
    """
    handler = _Collect()
    targets: list[logging.Logger] = [logging.getLogger(), logging.getLogger("curie_api")]
    targets += [
        logger
        for logger in logging.Logger.manager.loggerDict.values()
        if isinstance(logger, logging.Logger) and not logger.propagate
    ]
    levels = [(logger, logger.level) for logger in targets]
    for logger in targets:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        for logger, level in levels:
            logger.removeHandler(handler)
            logger.setLevel(level)


_FORMATTER = logging.Formatter("%(name)s %(levelname)s %(message)s")


def rendered(record: logging.LogRecord) -> str:
    """Every form a record can reach a sink in: message, args, exception and extras."""
    parts: list[str] = [repr(record.msg), repr(record.args)]
    try:
        parts.append(record.getMessage())
    except Exception as exc:  # noqa: BLE001 - a malformed record is still searched
        parts.append(repr(exc))
    try:
        parts.append(_FORMATTER.format(record))
    except Exception as exc:  # noqa: BLE001
        parts.append(repr(exc))
    parts.append(record.exc_text or "")
    parts.append(record.stack_info or "")
    parts.append(repr(vars(record)))
    return "\n".join(parts)


def test_wake_key_and_signatures_never_reach_logs_or_error_bodies(
    wake_db: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="curie_api")
    secrets: list[str] = []
    signatures: list[str] = []
    errors: list[httpx.Response] = []
    replies: list[httpx.Response] = []
    records: list[logging.LogRecord] = []

    def sign(ctx: Any, key: str) -> dict[str, str]:
        headers = signed_headers(key, ctx.agent_id, WAKE)
        signatures.append(headers[SIGNATURE_HEADER].removeprefix("sha256="))
        return headers

    async def scenario() -> None:
        async with wake_app() as ctx:
            with captured_logs() as handler:
                old = await ctx.client.get(secret_path(ctx.agent_id), headers=ctx.headers)
                assert old.status_code == 200, old.text
                secrets.append(old.json()["secret"])
                rotated = await ctx.client.post(rotate_path(ctx.agent_id), headers=ctx.headers)
                assert rotated.status_code == 200, rotated.text
                replies.append(rotated)
                new = await ctx.client.get(secret_path(ctx.agent_id), headers=ctx.headers)
                assert new.status_code == 200, new.text
                secrets.append(new.json()["secret"])
                assert secrets[0] != secrets[1]

                admitted_headers = sign(ctx, secrets[1])
                admitted = await wake(ctx, headers=admitted_headers)
                assert_queued(admitted)
                replies.append(admitted)
                coalesced = await wake(ctx, headers=admitted_headers)
                assert coalesced.status_code == 200 and coalesced.json() == COALESCED
                replies.append(coalesced)
                refused = await wake(ctx, headers=sign(ctx, secrets[0]))
                assert_refused(refused, 401, INVALID_SIGNATURE)
                errors.append(refused)
                async with restricted_valkey("set") as dsn, side_client(dsn) as side:
                    unavailable = await wake(ctx, headers=sign(ctx, secrets[1]), client=side)
                assert_refused(unavailable, 503, DEDUPE_UNAVAILABLE)
                errors.append(unavailable)
                records.extend(handler.records)

    drive(scenario)
    records.extend(caplog.records)
    assert records, "no log record was captured at all"
    forbidden = secrets + signatures
    assert len(forbidden) == 5 and all(forbidden)
    for record in records:
        text = rendered(record)
        for value in forbidden:
            assert value not in text, (record.name, record.levelname)
    for response in errors + replies:
        for value in forbidden:
            assert value not in response.text, response.status_code
            assert value not in "\n".join(response.headers.values())

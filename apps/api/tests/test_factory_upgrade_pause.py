"""#3127 AC2: a queued factory status comment says the install is paused.

When the worker upgrade quiesce marker exists, queued work is not stuck, it is
paused for an upgrade. The status comment says so while the request waits, and
the reconciler decides from the marker itself (fail-open on a read error).
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from curie_api import factory_notices, workitem_reconciler
from curie_api.config import Settings, get_settings
from curie_api.workitem_reconciler import WorkItemReconciler

PAUSED_LINE = (
    "Paused: this Curie installation is paused for an upgrade. "
    "Queued work starts when the upgrade finishes."
)


def _body(pill: str, paused: bool) -> str:
    return factory_notices.status_body(
        request_id=uuid.uuid4(),
        card_url=None,
        pill_label=pill,
        phase_view=None,
        result=None,
        paused_for_upgrade=paused,
    )


def test_a_queued_status_names_the_upgrade_pause() -> None:
    assert PAUSED_LINE in _body("QUEUED", True)


def test_the_pause_line_is_absent_when_not_paused_or_not_queued() -> None:
    assert PAUSED_LINE not in _body("QUEUED", False)
    assert "paused for an upgrade" not in _body("RUNNING", True)


@pytest.fixture
def fresh_settings(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.delenv("CURIE_INSTALLATION_ID", raising=False)
    monkeypatch.delenv("KEY_PREFIX", raising=False)
    get_settings.cache_clear()
    yield monkeypatch
    get_settings.cache_clear()


def test_upgrade_quiesce_key_mirrors_the_worker(fresh_settings: Any) -> None:
    assert Settings().upgrade_quiesce_key() == "curie:worker:upgrade:quiesce"
    fresh_settings.setenv("CURIE_INSTALLATION_ID", "install-a")
    assert (
        Settings().upgrade_quiesce_key() == "curie:worker:upgrade:quiesce:install-a"
    )


class _Valkey:
    def __init__(self, present: set[str], *, fail: bool = False) -> None:
        self.present = present
        self.fail = fail
        self.asked: list[str] = []

    async def exists(self, *keys: str) -> int:
        self.asked.extend(keys)
        if self.fail:
            raise ConnectionError("valkey down")
        return sum(1 for key in keys if key in self.present)


def _sessionmaker() -> Any:
    @contextlib.asynccontextmanager
    async def session() -> AsyncIterator[object]:
        yield object()

    return session


async def _flag_passed(
    monkeypatch: pytest.MonkeyPatch, valkey: _Valkey, settings: Settings
) -> Any:
    captured: list[dict[str, Any]] = []

    async def fake_sync(_session: Any, _settings: Any, **kwargs: Any) -> int:
        captured.append(kwargs)
        return 0

    monkeypatch.setattr(factory_notices, "sync_status_comments", fake_sync)
    monkeypatch.setattr(
        workitem_reconciler.factory_notices, "sync_status_comments", fake_sync
    )
    reconciler = WorkItemReconciler(_sessionmaker(), valkey, settings)  # type: ignore[arg-type]
    await reconciler._sync_status_comments()
    assert len(captured) == 1
    return captured[0].get("paused_for_upgrade")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("present", "fail", "expected"),
    [(True, False, True), (False, False, False), (True, True, False)],
)
async def test_reconciler_passes_the_pause_flag_from_the_marker(
    fresh_settings: Any, present: bool, fail: bool, expected: bool
) -> None:
    fresh_settings.setenv("CURIE_INSTALLATION_ID", "install-a")
    settings = Settings()
    key = settings.upgrade_quiesce_key()
    valkey = _Valkey({key} if present else set(), fail=fail)
    assert await _flag_passed(fresh_settings, valkey, settings) is expected
    assert key in valkey.asked

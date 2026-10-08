"""The worker composes the remediation receipt loop behind the switch.

@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-1

The receipt loop (``curie_worker.remediation_receipts``) runs beside the card
loop (``remediation_cards``), outside the consumer and the stream path, so a
receipt can never hold a thread lock or write a marker. ``run`` builds it with
``_build_remediation_receipts(config, engine, sink)``: ``None`` while
remediation is off, a ``RemediationReceiptLoop`` when it is on, and the
runtime record names it ``remediation_receipts`` (launched as the
``remediation-receipts`` task beside ``remediation-cards``).

The message and store surface is in ``apps/api/tests/test_remediation_receipts.py``
and ``.projects/plans/task-remediation-receipts.tests.md``.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest
from curie_worker import run
from curie_worker.config import WorkerConfig
from sqlalchemy.ext.asyncio import create_async_engine

FLAG = "CURIE_REMEDIATION_ENABLED"
EXECUTOR = "CURIE_ACTION_EXECUTOR_ENABLED"


@pytest.fixture(autouse=True)
def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (FLAG, EXECUTOR, "REMEDIATION_ENABLED"):
        monkeypatch.delenv(name, raising=False)


def _build(config: WorkerConfig) -> Any:
    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@127.0.0.1:1/none")
    return run._build_remediation_receipts(config, engine, object())  # type: ignore[arg-type]


def test_the_loop_is_not_built_while_remediation_is_off() -> None:
    assert _build(WorkerConfig()) is None


def test_the_loop_is_built_when_remediation_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    from curie_worker.remediation_receipts import RemediationReceiptLoop

    monkeypatch.setenv(EXECUTOR, "true")
    monkeypatch.setenv(FLAG, "true")

    assert isinstance(_build(WorkerConfig()), RemediationReceiptLoop)


def test_the_runtime_record_names_the_loop_and_launches_it() -> None:
    source = inspect.getsource(run)
    assert "remediation_receipts: RemediationReceiptLoop | None = None" in source
    assert 'launch("remediation-receipts"' in source

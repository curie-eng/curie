"""#1461 G1: a failed ``memory_writes_for`` read never fails the bound turn.

The setting is read apart from deployment resolution (like runner_resources) so
resolution keeps working on schemas that predate migration 0068. The same holds
for the read itself: a database error, or a schema without the column, must
leave the turn running with memory writes off and a warning, never a failed turn.

Real kernel harness (Valkey, fake substrate), like test_kernel_progress_env.py.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_work_item_workspace import _Binding, _turn, _WorkItems, _Workspace  # noqa: E402

CHANNEL_MEMORY_REF_ENV = "CURIE_CHANNEL_MEMORY_REF"


class _FailingMemoryWritesBinding(_Binding):
    """Resolves normally; the separate memory_writes read raises."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.reads = 0
        self.booted: list[Any] = []

    async def memory_writes_for(self, _agent_id: uuid.UUID) -> bool:
        self.reads += 1
        raise self.error

    def boot_env(self, resolved: object, thread_key: str, **kwargs: object) -> dict[str, str]:
        self.booted.append(resolved)
        return super().boot_env(resolved, thread_key, **kwargs)


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("connection reset by peer"),
        Exception('column "memory_writes" does not exist'),
    ],
    ids=["db-error", "schema-before-0068"],
)
def test_a_failed_memory_writes_read_leaves_the_turn_running_with_writes_off(
    make_harness, caplog: pytest.LogCaptureFixture, error: Exception
) -> None:
    binding = _FailingMemoryWritesBinding(error)
    caplog.set_level(logging.WARNING)

    async def exercise() -> None:
        async with make_harness(binding=binding, workspace_factory=_Workspace) as h:
            h.kernel._work_items = _WorkItems()
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4().hex}", "hello"))

            envs = [env or {} for env in h.fake_k8s.claim_envs]
            assert envs, "the turn must still claim a sandbox"
            for env in envs:
                assert CHANNEL_MEMORY_REF_ENV not in env

    asyncio.run(exercise())
    assert binding.reads >= 1, "the kernel never asked for the setting"
    assert binding.booted, "boot_env was never reached"
    for resolved in binding.booted:
        assert getattr(resolved, "memory_writes", False) is False
    warnings = [
        r for r in caplog.records if r.levelno >= logging.WARNING and "memory" in r.getMessage()
    ]
    assert warnings, [r.getMessage() for r in caplog.records]

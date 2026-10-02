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

from curie_worker.binding import BindingResolver, ResolvedDeployment  # noqa: E402
from curie_worker.config import WorkerConfig  # noqa: E402
from test_work_item_workspace import (  # noqa: E402
    AGENT_ID,
    CHANNEL,
    DEPLOYMENT_ID,
    _Binding,
    _turn,
    _WorkItems,
    _Workspace,
)

CHANNEL_MEMORY_REF_ENV = "CURIE_CHANNEL_MEMORY_REF"
MEMORY_WRITES_ENV = "CURIE_MEMORY_WRITES"


class _FailingMemoryWritesBinding(_Binding):
    """Resolves normally; the separate memory_writes read raises.

    ``boot_env`` is the real resolver's, so the env under test is the one the
    worker would really send for this bound turn (review L4).
    """

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.reads = 0
        self.booted: list[Any] = []
        self.boot_kwargs: list[dict[str, object]] = []

    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        return ResolvedDeployment(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            version_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    async def memory_writes_for(self, _agent_id: uuid.UUID) -> bool:
        self.reads += 1
        raise self.error

    def boot_env(self, resolved: object, thread_key: str, **kwargs: object) -> dict[str, str]:
        self.booted.append(resolved)
        self.boot_kwargs.append(dict(kwargs))
        resolver = BindingResolver.__new__(BindingResolver)
        resolver._config = WorkerConfig()
        return resolver.boot_env(resolved, thread_key, **kwargs)  # type: ignore[arg-type]


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
                # The turn names a binding, so the channel ref still rides for
                # reading (#3621), with writes explicitly off (review L4).
                assert CHANNEL_MEMORY_REF_ENV in env, sorted(env)
                assert env[CHANNEL_MEMORY_REF_ENV].endswith(
                    f"/agents/{AGENT_ID}/state/bindings/slack/{CHANNEL}/memory"
                )
                assert env[MEMORY_WRITES_ENV] == "0"

    asyncio.run(exercise())
    assert binding.reads >= 1, "the kernel never asked for the setting"
    assert binding.booted, "boot_env was never reached"
    for kwargs in binding.boot_kwargs:
        assert (kwargs.get("kind"), kwargs.get("address")) == ("slack", CHANNEL), kwargs
    for resolved in binding.booted:
        assert getattr(resolved, "memory_writes", False) is False
    warnings = [
        r for r in caplog.records if r.levelno >= logging.WARNING and "memory" in r.getMessage()
    ]
    assert warnings, [r.getMessage() for r in caplog.records]

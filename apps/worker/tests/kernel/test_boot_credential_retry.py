"""#3823: a retried claim never boots with a boot credential already released.

A failed claim reports its boot credential released, and the API refuses that
credential from then on. The delivery's next attempt claims again from the same
boot env, so the new sandbox must carry a credential no retired claim held.
These tests drive the real ``Kernel.process_event`` against real Valkey and the
real substrate; the binding double delegates ``boot_env`` to the real resolver,
so the tokens under test are the ones the worker would really mint.
"""

from __future__ import annotations

import asyncio
import functools
import sys
import uuid
from pathlib import Path
from typing import Any

from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import (
    HISTORY_TOKEN_ENV,
    MEMORY_TOKEN_ENV,
    BindingResolver,
    ResolvedDeployment,
    boot_token_facts,
)
from curie_worker.config import WorkerConfig
from curie_worker.sandbox.types import BootEnv

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
_STATE_TOKEN_ENV = BootEnv.env_key("state_token")
_qevent = functools.partial(qevent, channel=CHANNEL)


class _ReleasingBinding:
    """Real boot env mints; records the credentials the substrate releases."""

    def __init__(self) -> None:
        self.released: list[str] = []
        self._real = BindingResolver.__new__(BindingResolver)
        self._real._config = WorkerConfig()  # type: ignore[attr-defined]

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

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
        return self._real.boot_env(resolved, thread_key, **kwargs)

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()

    def release_boot_credential_sync(self, agent_id: str, credential: str) -> bool:
        assert agent_id == str(AGENT_ID)
        self.released.append(credential)
        return True

    def fresh_boot_credential(self, env: Any) -> dict[str, str]:
        return self._real.fresh_boot_credential(env)


def _creds(env: dict[str, str] | None) -> set[str]:
    found: set[str] = set()
    for key in (HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, _STATE_TOKEN_ENV):
        _agent, cred, _exp = boot_token_facts((env or {}).get(key))
        if cred is not None:
            found.add(cred)
    return found


def test_a_retry_after_a_failed_claim_boots_with_a_live_credential(make_harness) -> None:
    async def go() -> None:
        binding = _ReleasingBinding()
        async with make_harness(
            binding=binding,
            max_attempts=3,
            claim_timeout_seconds=0.2,
        ) as h:
            booted: list[tuple[set[str], set[str]]] = []
            real_create = h.fake_k8s.create_claim

            def create_claim(name: str, **kwargs: Any) -> None:
                env = kwargs.get("env")
                booted.append((_creds(env), set(binding.released)))
                real_create(name, **kwargs)
                # Only the first claim never binds; the retry's does.
                h.fake_k8s.bind_ready = True

            h.fake_k8s.create_claim = create_claim  # type: ignore[method-assign]
            h.fake_k8s.bind_ready = False

            await h.kernel.process_event(_qevent("hello", thread="th-cred-retry"))

            assert len(booted) == 2, booted
            first, _ = booted[0]
            assert len(first) == 1
            # The failed claim's credential was reported before the retry.
            assert set(binding.released) >= first
            second, released_before = booted[1]
            assert len(second) == 1
            assert not second & released_before, (
                "the retry booted with a credential the API already refuses"
            )
            assert h.sink.last_text == "ok"

    asyncio.run(go())

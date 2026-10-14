"""#4188: a failed turn must not lock its thread against every later turn.

Since #3823 a follow-up turn mints a boot state token that outlives the one the
live runner booted with, so the kernel replaces that runner at the durable
handoff boundary (the #3071 turn budget fence). A turn that ended in a
classified failure, ``max-turns`` among them, leaves the runner idle in
``classified-failure``. The transcript never records a failed turn, so the
replacement can rehydrate everything durable replay holds. If the fence refuses
that status, every later turn on the thread raises ``ThreadBusyError`` and the
thread is locked for good.

These tests drive the real ``Kernel.process_event`` against real Valkey and the
real substrate; the binding double delegates ``boot_env`` to the real resolver,
so the boot tokens and their expiries are the ones the worker really mints.
"""

from __future__ import annotations

import asyncio
import functools
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus
from curie_worker import binding as binding_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.kernel.failures import ThreadBusyError

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
_qevent = functools.partial(qevent, channel=CHANNEL)


class _RealBootEnvBinding:
    """Resolves one agent; ``boot_env`` is the real resolver's minting."""

    def __init__(self) -> None:
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
        return True

    def fresh_boot_credential(self, env: Any) -> dict[str, str]:
        return self._real.fresh_boot_credential(env)


class _LaterClock:
    """``time`` for the binding module only, read some minutes after the real one.

    A person's follow-up arrives after the failed turn, so its boot token
    expires later than the live runner's. Shifting only the minting clock
    reproduces that without sleeping and without moving Valkey's clock.
    """

    def __init__(self, offset_s: float) -> None:
        self._offset_s = offset_s

    def time(self) -> float:
        return time.time() + self._offset_s

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def _max_turns_script() -> list:
    return [
        ErrorEvent(message="reached max turns", classification="max-turns"),
        Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
    ]


def test_a_turn_after_a_max_turns_failure_starts_on_a_replacement_runner(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with make_harness(binding=_RealBootEnvBinding()) as h:
            h.runner.turn_scripts = [_max_turns_script()]
            thread = "th-max-turns"

            await h.kernel.process_event(_qevent("run the probes", thread=thread))
            assert h.sink.last_text is not None and "max-turns" in h.sink.last_text

            # What the runner reports after that turn: idle in the failed
            # status, with durable replay intact because a failed turn is never
            # recorded (pinned on the runner side in
            # runner/tests/test_session.py).
            h.runner.session_status = SessionStatus.CLASSIFIED_FAILURE.value
            h.runner.history_durable = True
            monkeypatch.setattr(binding_module, "time", _LaterClock(300))

            await h.kernel.process_event(_qevent("continue", thread=thread))

            assert h.runner.opened == ["run the probes", "continue"]
            # The follow-up replaced the runner instead of adopting it.
            assert len(h.fake_k8s.claim_envs) == 2
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_a_failed_turn_on_a_runner_with_lost_history_still_refuses_replacement(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fence still fails closed when replay is not durable.

    A runner that lost an earlier turn's history keeps reporting it after a
    failed turn; replacing it would drop that turn, so the fence still holds.
    """

    async def go() -> None:
        async with make_harness(binding=_RealBootEnvBinding()) as h:
            h.runner.turn_scripts = [_max_turns_script()]
            thread = "th-max-turns-lost"

            await h.kernel.process_event(_qevent("run the probes", thread=thread))

            h.runner.session_status = SessionStatus.CLASSIFIED_FAILURE.value
            h.runner.history_durable = False
            monkeypatch.setattr(binding_module, "time", _LaterClock(300))

            with pytest.raises(ThreadBusyError, match="turn budget handoff boundary"):
                await h.kernel.process_event(_qevent("continue", thread=thread))

            assert h.runner.opened == ["run the probes"]
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(go())

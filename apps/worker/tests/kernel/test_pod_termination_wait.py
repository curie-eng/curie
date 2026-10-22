"""Bounds on the stream-drop pod termination wait and the OOMKilled detail."""

from __future__ import annotations

import asyncio

from curie_worker.kernel import attempt as kernel_attempt
from curie_worker.kernel.failures import _sandbox_termination_detail
from curie_worker.sandbox.types import SandboxTermination


def test_each_read_keeps_its_own_bound() -> None:
    class _Clock:
        def __init__(self) -> None:
            self.now = 0.0

        def read(self) -> float:
            return self.now

        async def sleep(self, delay: float) -> None:
            assert delay == 0.5
            self.now += delay

    async def go() -> None:
        clock = _Clock()
        seen: list[tuple[float, float]] = []

        async def read(timeout_s: float) -> SandboxTermination | None:
            seen.append((clock.now, timeout_s))
            index = len(seen)
            if index == 1:
                raise RuntimeError("diagnosis failed")
            if index == 2:
                raise TimeoutError("diagnosis timed out")
            if clock.now >= 9.0:
                return SandboxTermination("OOMKilled", "exit code 137")
            return None

        found = await kernel_attempt._await_pod_termination(
            read,
            window_s=10.0,
            interval_s=0.5,
            clock=clock.read,
            sleep=clock.sleep,
        )
        assert found == SandboxTermination("OOMKilled", "exit code 137")
        assert seen
        for moment, timeout_s in seen:
            assert timeout_s <= 3.0
            assert timeout_s <= 10.0 - moment
        at_nine = [timeout_s for moment, timeout_s in seen if moment == 9.0]
        assert at_nine
        assert at_nine[0] <= 1.0

    asyncio.run(go())


def test_oomkilled_detail_names_the_memory_limit() -> None:
    with_detail = _sandbox_termination_detail(SandboxTermination("OOMKilled", "exit code 137"))
    assert (
        with_detail == "Kubernetes pod terminated: OOMKilled (exit code 137). "
        "The runner container hit its memory limit."
    )
    without_detail = _sandbox_termination_detail(SandboxTermination("OOMKilled", None))
    assert (
        without_detail
        == "Kubernetes pod terminated: OOMKilled. The runner container hit its memory limit."
    )
    long = _sandbox_termination_detail(SandboxTermination("OOMKilled", "x" * 400))
    assert len(long) <= 300
    assert long.endswith("The runner container hit its memory limit.")
    assert (
        _sandbox_termination_detail(SandboxTermination("Evicted", None))
        == "Kubernetes pod terminated: Evicted."
    )
    assert (
        _sandbox_termination_detail(SandboxTermination("Deleted", "exit code 143"))
        == "Kubernetes pod terminated: Deleted (exit code 143)."
    )

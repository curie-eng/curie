"""Execute the quota checker's observed-list clock boundary.

@spec QUOTA-OBSERVATION-CLOCK-1 QUOTA-OBSERVATION-CLOCK-2 QUOTA-OBSERVATION-CLOCK-3
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

SCRIPT = Path(__file__).with_name("idle-capacity-reclamation-runtime.sh")
OVERAGE = "observed more than two chargeable sandbox pods"


def checker(get: Callable[..., Any], clock: Callable[[], float]) -> Callable[[], None]:
    """@spec QUOTA-OBSERVATION-CLOCK-3."""
    prefix, source = SCRIPT.read_text().split('cat >"$WORKDIR/driver.py" <<\'PY\'\n', 1)
    source = source.split("\nPY\n", 1)[0]
    tree = ast.parse("\n" * (prefix.count("\n") + 1) + source, filename=str(SCRIPT))
    predicate = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "counts_against_quota"
    )
    burst = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "burst_sixteen"
    )
    quota_pods = next(
        node for node in burst.body
        if isinstance(node, ast.FunctionDef) and node.name == "quota_pods"
    )
    # Compile the original nodes without running the driver's setup, launches,
    # polling loop, cleanup, or any shell/process code.
    module = ast.Module(body=[predicate, quota_pods], type_ignores=[])
    namespace: dict[str, Any] = {
        "get": get,
        "time": SimpleNamespace(time=clock),
        "datetime": datetime,
        "fullname": "clock-check",
    }
    exec(compile(module, str(SCRIPT), "exec"), namespace)
    return namespace["quota_pods"]


def pod(
    name: str, *, phase: str = "Running", priority: str = "sandbox", created: int = 80,
    deleted: int | None = None, grace: int = 15,
) -> dict[str, Any]:
    """@spec QUOTA-OBSERVATION-CLOCK-3."""
    metadata: dict[str, Any] = {
        "name": name,
        "uid": name,
        "creationTimestamp": datetime.fromtimestamp(created, UTC).isoformat(),
    }
    if deleted is not None:
        metadata.update({
            "deletionTimestamp": datetime.fromtimestamp(deleted, UTC).isoformat(),
            "deletionGracePeriodSeconds": grace,
        })
    return {
        "metadata": metadata,
        "spec": {"priorityClassName": priority},
        "status": {"phase": phase},
    }


class Observation:
    """Deterministic external reads and clock. @spec QUOTA-OBSERVATION-CLOCK-3."""

    def __init__(
        self, pods: list[dict[str, Any]], *, before: float = 110, after: float = 110,
        hard: str = "2", used: str = "2", read_error: Exception | None = None,
    ) -> None:
        self.pods = pods
        self.now = before
        self.after = after
        self.hard = hard
        self.used = used
        self.read_error = read_error
        self.calls: list[tuple[str, ...]] = []
        self.clock_observations: list[float] = []

    def get(self, *args: str) -> dict[str, Any]:
        """@spec QUOTA-OBSERVATION-CLOCK-3."""
        self.calls.append(args)
        if args == ("resourcequota", "clock-check-sandbox-quota"):
            return {
                "spec": {
                    "hard": {"pods": self.hard},
                    "scopeSelector": {"matchExpressions": [{"values": ["sandbox"]}]},
                },
                "status": {"used": {"pods": self.used}},
            }
        assert args == ("pods",), f"unexpected external read: {args}"
        self.now = self.after
        if self.read_error is not None:
            raise self.read_error
        return {"items": self.pods}

    def clock(self) -> float:
        """@spec QUOTA-OBSERVATION-CLOCK-3."""
        self.clock_observations.append(self.now)
        return self.now

    def check(self) -> None:
        """@spec QUOTA-OBSERVATION-CLOCK-3."""
        checker(self.get, self.clock)()


def assert_single_list_read(observation: Observation) -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-2 QUOTA-OBSERVATION-CLOCK-3."""
    assert observation.calls == [
        ("resourcequota", "clock-check-sandbox-quota"), ("pods",),
    ]


@pytest.mark.parametrize("created", [80, 106], ids=["existing-active", "newly-admitted"])
def test_list_read_crossing_deletion_grace_does_not_invent_a_quota_overage(created: int) -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-1 QUOTA-OBSERVATION-CLOCK-3."""
    observation = Observation(
        [pod("first"), pod("second", created=created), pod("old", deleted=90)],
        before=100, after=110,
    )
    # At completion (110), deletion grace ended at 105: only the two active
    # Pods are chargeable. The newly admitted variant did not exist at 100.
    observation.check()
    assert_single_list_read(observation)
    assert observation.clock_observations == [110]


def test_three_genuine_active_pods_still_fail_the_chargeable_count_assertion() -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-2 QUOTA-OBSERVATION-CLOCK-3."""
    observation = Observation([pod("first"), pod("second"), pod("third")])
    with pytest.raises(AssertionError, match=f"^{OVERAGE}$"):
        observation.check()
    assert_single_list_read(observation)


@pytest.mark.parametrize("now", [104, 105], ids=["unexpired-grace", "equal-grace"])
def test_deleting_pod_remains_chargeable_until_strictly_after_grace(now: int) -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-1 QUOTA-OBSERVATION-CLOCK-2."""
    observation = Observation(
        [pod("first"), pod("second"), pod("deleting", deleted=90)], before=now, after=now,
    )
    with pytest.raises(AssertionError, match=f"^{OVERAGE}$"):
        observation.check()
    assert_single_list_read(observation)


@pytest.mark.parametrize(
    "extra",
    [
        pod("expired", deleted=90),
        pod("completed", phase="Succeeded"),
        pod("failed", phase="Failed"),
        pod("other-priority", priority="control-plane"),
    ],
    ids=["expired-grace", "succeeded", "failed", "different-priority"],
)
def test_nonchargeable_or_different_priority_pods_do_not_consume_sandbox_quota(
    extra: dict[str, Any],
) -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-1 QUOTA-OBSERVATION-CLOCK-2."""
    observation = Observation([pod("first"), pod("second"), extra])
    observation.check()
    assert_single_list_read(observation)


@pytest.mark.parametrize("hard", ["1", "3"])
def test_hard_pod_quota_must_remain_exactly_two(hard: str) -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-2."""
    observation = Observation([], hard=hard)
    with pytest.raises(AssertionError, match="^sandbox pod quota changed from two$"):
        observation.check()
    assert observation.calls == [("resourcequota", "clock-check-sandbox-quota")]


def test_reported_quota_usage_over_two_is_rejected_even_with_an_empty_pod_list() -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-2."""
    observation = Observation([], used="3")
    with pytest.raises(AssertionError, match="^observed quota usage exceeded two$"):
        observation.check()
    assert observation.calls == [("resourcequota", "clock-check-sandbox-quota")]


def test_failed_pod_list_read_propagates_the_original_exception() -> None:
    """@spec QUOTA-OBSERVATION-CLOCK-2 QUOTA-OBSERVATION-CLOCK-3."""
    original = RuntimeError("synthetic pod-list read failed")
    observation = Observation([], before=100, after=110, read_error=original)
    with pytest.raises(RuntimeError) as caught:
        observation.check()
    assert caught.value is original
    assert_single_list_read(observation)

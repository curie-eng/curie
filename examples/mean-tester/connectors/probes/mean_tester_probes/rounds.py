"""How many probes, to how many targets, one channel may receive at once (ADR 0169 d7).

The guard caps one `send_probes` call. This caps what the connector has posted
over a sliding window as long as a probe's reply is waited for, because the
hosted connector is shared by every thread: a round split across calls, or two
threads testing at once, would otherwise each stay under the per-call cap.

MCPServer runs these sync tools on worker threads, so a call reserves its slots
before it posts, under one lock: a check that only read the list would let two
overlapping calls both pass it before either had posted.
"""

import math
import threading
from collections.abc import Callable

from mean_tester_probes.config import Config


class RoundRefusal(ValueError):
    """A probe that would exceed a cap over the window."""


class RoundLimiter:
    def __init__(self, config: Config, now: Callable[[], float]) -> None:
        self._window = config.reply_timeout_s
        self._max_probes = config.max_probes
        self._max_targets = config.max_concurrent_rounds
        self._now = now
        self._sent: list[tuple[float, str, str]] = []  # (when, channel, target)
        self._lock = threading.Lock()

    def reserve(self, channel: str, target: str, count: int) -> None:
        """Take `count` slots for `target` in `channel`, or refuse and take none."""
        with self._lock:
            now = self._now()
            self._sent = [s for s in self._sent if now - s[0] < self._window]
            self._refuse_over_cap(channel, target, count, now)
            self._sent.extend((now, channel, target) for _ in range(count))

    def release(self, channel: str, target: str, count: int) -> None:
        """Give back `count` slots `reserve` took and no probe used."""
        with self._lock:
            # The oldest go first, so the slots kept expire no sooner than exact
            # accounting would let them: a release never loosens a cap.
            for _ in range(count):
                k = next((k for k, s in enumerate(self._sent) if s[1:] == (channel, target)), None)
                if k is None:
                    return
                del self._sent[k]

    def _refuse_over_cap(self, channel: str, target: str, count: int, now: float) -> None:
        window = f"{self._window:g} s"
        mine = sorted(t for t, c, u in self._sent if c == channel and u == target)
        over = len(mine) + count - self._max_probes
        if over > 0:
            wait = mine[over - 1] + self._window - now
            raise RoundRefusal(
                f"<@{target}> in {channel} was sent {len(mine)} probes in the last {window}, "
                f"and a round sends at most {self._max_probes}; the next round is possible "
                f"in {math.ceil(wait)} s"
            )
        if mine:
            return  # a target already being probed may finish its round
        latest: dict[str, float] = {}
        for t, c, u in self._sent:
            if c == channel:
                latest[u] = max(latest.get(u, t), t)
        if len(latest) >= self._max_targets:
            frees = sorted(latest.values())[len(latest) - self._max_targets]
            raise RoundRefusal(
                f"{channel} has probes out to {len(latest)} targets in the last {window}, "
                f"and at most {self._max_targets} targets are probed at once; the next "
                f"target can be probed in {math.ceil(frees + self._window - now)} s"
            )

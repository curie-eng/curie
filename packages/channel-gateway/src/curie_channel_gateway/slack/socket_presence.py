"""Which of the Slack identities a dispatcher serves hold a live Socket Mode socket.

The heartbeat (``heartbeat.py``) proves only that the process still schedules
threads. This records ``curie.slack.socket.identities`` in two states:
``configured``, the Slack identities the dispatcher serves, and ``connected``,
those whose connection holds an open socket when sampled. An operator alerts on
``connected`` below ``configured``. Which identity is down stays in the log; the
metric carries no identity attribute.

The gauge is sampled from each connection rather than set on connect and
disconnect events. slack_sdk's stale ping check closes a dead socket without
calling any listener (``Connection.check_state``, slack_sdk 3.44.1), so a gauge
fed by events would go on reporting that socket as connected.
"""

import threading
from typing import Final, Protocol

from curie_telemetry import record_metric

IDENTITIES_METRIC: Final = "curie.slack.socket.identities"

# The dispatcher's metric reader exports every 10 seconds
# (``curie_telemetry.bootstrap``), so one sample per export.
_SAMPLE_INTERVAL_S: Final = 10.0


class _Socket(Protocol):
    def is_connected(self) -> bool: ...


class SocketPresence:
    """The connections of one dispatcher process, sampled into one gauge.

    A connection attaches when it starts connecting and detaches when it closes
    or fails to connect. Each sample counts the attached connections whose
    socket is open. ``configured`` is fixed at construction.
    """

    def __init__(self, configured: int, *, sample_interval_s: float = _SAMPLE_INTERVAL_S) -> None:
        self.sample_interval_s = sample_interval_s
        self._configured = configured
        self._sockets: set[_Socket] = set()
        # Held across counting and recording, so a sample taken just before a
        # detach can never land after the detach's own sample.
        self._lock = threading.Lock()

    def attach(self, socket: _Socket) -> None:
        with self._lock:
            self._sockets.add(socket)
        self.record()

    def detach(self, socket: _Socket) -> None:
        with self._lock:
            self._sockets.discard(socket)
        self.record()

    def record(self) -> None:
        with self._lock:
            connected = sum(1 for socket in self._sockets if socket.is_connected())
            for state, value in (("configured", self._configured), ("connected", connected)):
                record_metric(
                    IDENTITIES_METRIC,
                    value,
                    attributes={"service.name": "curie-dispatcher", "state": state},
                )

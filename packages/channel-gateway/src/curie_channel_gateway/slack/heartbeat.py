"""Process-liveness heartbeat: a daemon thread that touches a file on a cadence.

The dispatcher runs Socket Mode, which dials Slack out and keeps no inbound HTTP
port, so a kubelet cannot health-check it over the network. Instead a daemon
thread touches a file every ``interval_s`` seconds and an exec liveness probe
checks that file's freshness; if the mtime goes stale the pod is restarted.

Scope, honestly stated: this proves only that the Python interpreter is still
scheduling threads, i.e. the process is not fully wedged (a total deadlock or a
GIL hang would stop the touches and the probe would fire). It is a process-level
liveness signal. It does NOT, by itself, detect a Socket Mode connection that is
silently dead while the process otherwise runs and keeps touching the file.
``socket_presence.py`` is the connection-aware signal: a gauge, not a probe.
"""

import logging
import threading
from pathlib import Path

_logger = logging.getLogger(__name__)


def start_heartbeat(path: str, interval_s: float) -> threading.Event:
    """Start a daemon thread that touches ``path`` now and every ``interval_s``.

    Returns a ``threading.Event``; set it to stop the thread promptly. The thread
    is a daemon, so it will not block process exit if the caller forgets to stop
    it.
    """
    stop = threading.Event()

    def _run() -> None:
        while True:
            try:
                Path(path).touch()
            except OSError as exc:
                _logger.warning("heartbeat touch failed for %s: %s", path, exc)
            if stop.wait(timeout=interval_s):
                break

    thread = threading.Thread(target=_run, name="dispatcher-heartbeat", daemon=True)
    thread.start()
    return stop

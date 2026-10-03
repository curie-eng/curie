"""Curie worker: concurrency kernel, sandbox substrate, evals.

F1 adds the concurrency kernel: the consumer group over the dispatcher's Valkey
stream, the routing / finish-race / steer / interrupt rules, no-retry-after-
side-effects with human escalation, and crash-recovery reclaim. G1 owns the
``sandbox`` substrate module; K1 will add the eval runner.
"""

from .binding import BindingResolver, ResolvedDeployment
from .config import WorkerConfig
from .consumer import Consumer
from .kernel.constants import RETRYABLE_CLASSIFICATIONS
from .kernel.core import Kernel
from .kernel.failures import TurnOutcome
from .killswitch import KillSwitch
from .markers import CompletionRecord, Markers
from .reply_sink import (
    HttpReplyAdapter,
    ReplySink,
    ReplySinkRouter,
    TargetRoute,
    build_reply_sink,
)
from .runner_client import RunnerClient, RunnerError, TurnStream
from .slack_sink import SlackReplyAdapter
from .threadlock import LockAcquireTimeout, ThreadLock

__version__ = "0.0.0"

__all__ = [
    "RETRYABLE_CLASSIFICATIONS",
    "CompletionRecord",
    "BindingResolver",
    "Consumer",
    "Kernel",
    "KillSwitch",
    "LockAcquireTimeout",
    "Markers",
    "ResolvedDeployment",
    "RunnerClient",
    "RunnerError",
    "HttpReplyAdapter",
    "ReplySink",
    "ReplySinkRouter",
    "SlackReplyAdapter",
    "TargetRoute",
    "build_reply_sink",
    "ThreadLock",
    "TurnOutcome",
    "TurnStream",
    "WorkerConfig",
    "__version__",
]

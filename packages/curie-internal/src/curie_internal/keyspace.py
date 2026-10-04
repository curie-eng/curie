"""Canonical Valkey key names and builders shared by platform services.

Configured prefixes stay explicit arguments so separate installs retain their
own keyspace. These names preserve the existing stored bytes.
"""

import hashlib
import uuid

KILL_KEY_PREFIX = "curie:kill:"
KILL_CHANNEL = "curie:kill-events"
WORKER_KEY_PREFIX_DEFAULT = "curie:worker"
SANDBOX_KEY_PREFIX_DEFAULT = "curie:sandbox"
DEDUPE_KEY_PREFIX_DEFAULT = "curie:dedupe:"
ADMISSION_KEY_PREFIX_DEFAULT = "curie:admission:"
THREAD_CONTEXT_KEY_PREFIX_DEFAULT = "curie:slack-root-context:"
THREAD_RESET_SET = "curie:thread-reset-requests"
THREAD_RESET_INFLIGHT_SET = "curie:thread-reset-inflight"
THREAD_RESET_RESULT_PREFIX = "curie:thread-reset-result:"
CHANNEL_KEY_PREFIX = "curie:channel"
HOOK_KEY_PREFIX = "curie:hook"
GITHUB_REVIEW_KEY_PREFIX = "curie:github-review"
GITHUB_REVIEW_HELD_INDEX = "curie:github-review:held"
DEPLOY_NOTICE_DEDUPE_PREFIX = "curie:deploy-notice:dedupe:"
CLUSTER_REPLY_KEY_PREFIX = "curie:cluster-message-replies"
WORK_ITEM_CI_RERUN_PREFIX = "curie:work-item:ci-rerun"


def kill_key(agent_id: uuid.UUID) -> str:
    return f"{KILL_KEY_PREFIX}{agent_id}"


def done_key(key_prefix: str, event_id: str) -> str:
    return f"{key_prefix}:done:{event_id}"


def completion_key(key_prefix: str, event_id: str) -> str:
    return f"{key_prefix}:completion:{event_id}"


def inbox_key(key_prefix: str, progress_id: str) -> str:
    """The chain inbox stream, under the worker's configured key prefix."""
    return f"{key_prefix}:progress:inbox:{progress_id}"


def inbox_pending_key(key_prefix: str) -> str:
    """The durable index maintenance workers use to find pending inboxes."""
    return f"{key_prefix}:progress:inbox:pending"


def progress_key(key_prefix: str, progress_id: str) -> str:
    """The worker owned chain record whose active generation fences ingress."""
    return f"{key_prefix}:progress:{progress_id}"


def rate_key(key_prefix: str, token: str) -> str:
    """Name the rate bucket by a digest so the token is never stored."""
    digest = hashlib.sha256(token.encode()).hexdigest()[:32]
    return f"{key_prefix}:progress:rate:{digest}"


def cluster_reply_keys(reply_ref: str) -> tuple[str, str, str, str]:
    """Keep one reply bucket in a single Redis Cluster hash slot."""
    base = f"{CLUSTER_REPLY_KEY_PREFIX}:{{{reply_ref}}}"
    return (
        f"{base}:events",
        f"{base}:digests",
        f"{base}:bytes",
        f"{base}:terminal",
    )

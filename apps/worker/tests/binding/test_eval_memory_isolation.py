"""#1909: default local/cluster eval must not load ambient agent memory.

The CLI stamps ``eval:`` onto each eval case's conversation_id. ``boot_env``
already receives that value as ``thread_key``, so isolation lives here rather
than in sacred ``kernel.py``. A conflicting deployed memory entry therefore
cannot change the default eval result: the sandbox never receives
``CURIE_MEMORY_REF``.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_VECTOR = (
    Path(__file__).resolve().parents[4] / "tests" / "vectors" / "eval-memory-isolation.json"
)
_EXPECTED_VECTOR_KEYS = frozenset({"comment", "conversation_id_prefix"})


def _resolved() -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_name="test-agent",
        agent_id=_AGENT,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


def _boot_env(thread_key: str) -> dict[str, str]:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()
    return resolver.boot_env(_resolved(), thread_key)


def test_eval_isolate_thread_omits_memory_ref() -> None:
    """#1909: the eval conversation prefix must drop ambient durable memory.

    RED until boot_env omits CURIE_MEMORY_REF/CURIE_MEMORY_TOKEN for this
    thread_key. History stays: each eval case already uses a unique thread
    (#550), and this bug is memory riding in on a fresh conversation.
    """

    env = _boot_env("eval:1720000000.000100")
    assert "CURIE_MEMORY_REF" not in env, env
    assert "CURIE_MEMORY_TOKEN" not in env, env
    assert "CURIE_HISTORY_REF" in env
    assert "CURIE_HISTORY_TOKEN" in env
    assert env["CURIE_BUNDLE_REF"] == "bundles/x.zip"
    assert env["CURIE_BUNDLE_VERSION"] == "v1"
    assert str(_AGENT) in env["CURIE_SESSION_ID"]


def test_plain_thread_still_loads_memory() -> None:
    """Negative: a normal mention still boots with the agent's memory namespace."""

    env = _boot_env("thread-1")
    assert env["CURIE_MEMORY_REF"] == f"http://localhost:8000/agents/{_AGENT}/state/memory"
    assert "CURIE_MEMORY_TOKEN" in env


def test_eval_hyphen_prefix_is_not_an_isolate_thread() -> None:
    """The isolate marker is ``eval:``, not any thread that merely starts eval."""

    env = _boot_env("eval-thread-1")
    assert "CURIE_MEMORY_REF" in env
    assert "CURIE_MEMORY_TOKEN" in env


def test_eval_isolate_prefix_matches_the_frozen_vector() -> None:
    """Python half of the CLI/worker eval-isolate prefix gate (#1909)."""

    from curie_worker.binding import EVAL_ISOLATE_THREAD_PREFIX, is_eval_isolate_thread

    vector = json.loads(_VECTOR.read_text(encoding="utf-8"))
    keys = set(vector)
    assert keys == _EXPECTED_VECTOR_KEYS, (
        f"{_VECTOR} has unexpected keys {sorted(keys - _EXPECTED_VECTOR_KEYS)} "
        f"and is missing {sorted(_EXPECTED_VECTOR_KEYS - keys)}. A new key is "
        "rejected on purpose: one a lane cannot see would pass vacuously. Teach "
        "the new key to _EXPECTED_VECTOR_KEYS here, to EvalIsolateVector in "
        "cli/src/queue.rs, and to both lanes' assertions."
    )
    prefix = vector["conversation_id_prefix"]
    assert EVAL_ISOLATE_THREAD_PREFIX == prefix
    assert is_eval_isolate_thread(f"{prefix}1720000000.000100")
    assert not is_eval_isolate_thread("thread-1")
    assert not is_eval_isolate_thread("eval-thread-1")

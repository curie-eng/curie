"""#1461: the default memory guidance is one text, held in two places.

The runner injects ``DEFAULT_GUIDANCE`` when an agent has no operator guidance;
the API reports ``DEFAULT_MEMORY_GUIDANCE`` as the effective text in that case.
The API must not import the runner, so each keeps its own copy and this
repo-level test pins them byte for byte: an operator reading ``curie ... memory
--guidance`` must see exactly what the agent is told.

ADR 0239 adds a second default, selected when the operator turns on agent-wide
learning and stores no guidance of their own: the API's
``LEARNING_MEMORY_GUIDANCE`` and the runner's ``LEARNING_GUIDANCE``. Those are
pinned to each other the same way. The original default must not change at
all, so its bytes are also pinned to the digest recorded before the change.
"""

from __future__ import annotations

import hashlib

# UTF-8 SHA-256 and length of the default guidance before ADR 0239.
ORIGINAL_DEFAULT_SHA256 = "ac65ffde36f1252cd369d9d5c6226ca006782cf045c3cf758a5704fbf9338e2f"
ORIGINAL_DEFAULT_BYTES = 1412


def test_runner_and_api_default_guidance_are_byte_identical() -> None:
    from curie_api.memory_guidance import DEFAULT_MEMORY_GUIDANCE
    from curie_runner.memory_facts import DEFAULT_GUIDANCE

    assert isinstance(DEFAULT_GUIDANCE, str) and DEFAULT_GUIDANCE.strip()
    assert DEFAULT_GUIDANCE.encode("utf-8") == DEFAULT_MEMORY_GUIDANCE.encode("utf-8")


def test_default_guidance_bytes_are_unchanged() -> None:
    from curie_api.memory_guidance import DEFAULT_MEMORY_GUIDANCE
    from curie_runner.memory_facts import DEFAULT_GUIDANCE

    for text in (DEFAULT_MEMORY_GUIDANCE, DEFAULT_GUIDANCE):
        encoded = text.encode("utf-8")
        assert len(encoded) == ORIGINAL_DEFAULT_BYTES
        assert hashlib.sha256(encoded).hexdigest() == ORIGINAL_DEFAULT_SHA256


def test_runner_and_api_learning_guidance_are_byte_identical() -> None:
    import curie_api.memory_guidance as api_guidance
    import curie_runner.memory_facts as runner_facts

    api_text = getattr(api_guidance, "LEARNING_MEMORY_GUIDANCE", None)
    runner_text = getattr(runner_facts, "LEARNING_GUIDANCE", None)
    assert isinstance(api_text, str) and api_text.strip(), "API learning guidance is missing"
    assert isinstance(runner_text, str) and runner_text.strip(), (
        "runner learning guidance is missing"
    )
    assert runner_text.encode("utf-8") == api_text.encode("utf-8")
    # It is a separate text, not the original default under a second name.
    assert api_text.encode("utf-8") != api_guidance.DEFAULT_MEMORY_GUIDANCE.encode("utf-8")

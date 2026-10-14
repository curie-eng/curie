"""#1461: the default memory guidance is one text, held in two places.

The runner injects ``DEFAULT_GUIDANCE`` when an agent has no operator guidance;
the API reports ``DEFAULT_MEMORY_GUIDANCE`` as the effective text in that case.
The API must not import the runner, so each keeps its own copy and this
repo-level test pins them byte for byte: an operator reading ``curie ... memory
--guidance`` must see exactly what the agent is told.
"""

from __future__ import annotations


def test_runner_and_api_default_guidance_are_byte_identical() -> None:
    from curie_api.memory_guidance import DEFAULT_MEMORY_GUIDANCE
    from curie_runner.memory_facts import DEFAULT_GUIDANCE

    assert isinstance(DEFAULT_GUIDANCE, str) and DEFAULT_GUIDANCE.strip()
    assert DEFAULT_GUIDANCE.encode("utf-8") == DEFAULT_MEMORY_GUIDANCE.encode("utf-8")

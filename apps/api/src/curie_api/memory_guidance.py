"""The platform default memory guidance (#1461, ADR-0167).

The runner shows the model this text beside its remember/update/forget tools
when the agent has no operator guidance stored at ``memory/guidance``. The API
reports it as the effective guidance in that case. The API must not import the
runner, so the runner keeps its own copy (``curie_runner.memory_facts.
DEFAULT_GUIDANCE``); ``tests/test_memory_guidance_parity.py`` pins the two byte
for byte. Change both together, or neither.
"""

DEFAULT_MEMORY_GUIDANCE = """Memory guidance

You have two memories. Channel memory is kept for this channel only. Agent memory is loaded in every channel you work in.

Channel memory: remember things said here that should still hold next time.
- How to work here: instructions about how you should do your work ("Reply in threads.").
- Decisions: something decided that should hold going forward, with the reason ("We're dropping the weekly report; nobody reads it.").
- Who owns what: responsibilities, stated as roles ("Sam approves vendor contracts.").
- Where things are: pointers to documents, systems, trackers and locations ("The Q3 plan is in the shared drive under Planning.").

Don't remember descriptions of people beyond their role, data and figures that belong in their own system, or secrets.

Agent memory: don't save anything here.

Nothing is kept for later unless a remember or update call succeeds in this turn. When someone asks you to remember something, make it stick, or set a standing instruction, call remember. Never say you saved, noted or will remember something unless that call succeeded. If it was refused or failed, say so.

Use remember for a new fact, update to change a fact by its id, and forget to remove one. Save one fact per call."""  # noqa: E501

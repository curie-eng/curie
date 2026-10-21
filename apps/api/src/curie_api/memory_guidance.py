"""The platform default memory guidance (#1461, ADR-0167; ADR 0239).

The runner shows the model this text beside its memory tools when the agent has
no operator guidance stored at ``memory/guidance``. The API reports it as the
effective guidance in that case. The API must not import the runner, so the
runner keeps its own copies (``curie_runner.memory_facts.DEFAULT_GUIDANCE`` and
``LEARNING_GUIDANCE``); ``tests/test_memory_guidance_parity.py`` pins each pair
byte for byte. Change both together, or neither.

Selection (ADR 0239), in order: nonblank operator text verbatim; else
``LEARNING_MEMORY_GUIDANCE`` when the agent is opted in to agent-wide learning;
else ``DEFAULT_MEMORY_GUIDANCE``, whose bytes must never change.
"""

from typing import Any, Literal

GuidanceSource = Literal["operator", "default", "learning_default"]

DEFAULT_MEMORY_GUIDANCE = """Memory guidance

You have two memories. Channel memory is kept for this channel only. Agent memory is loaded in every channel you work in.

Channel memory: remember things said here that should still hold next time.
- How to work here: instructions about how you should do your work ("Reply in threads.").
- Decisions: something decided that should hold going forward, with the reason ("We're dropping the weekly report; nobody reads it.").
- Who owns what: responsibilities, stated as roles ("Sam approves vendor contracts.").
- Where things are: pointers to documents, systems, trackers and locations ("The Q3 plan is in the shared drive under Planning.").

Don't remember descriptions of people beyond their role, data and figures that belong in their own system, or secrets.

Agent memory: don't save anything here.

Nothing is kept for later unless a remember or update call succeeds. When someone asks you to remember something worth keeping, make it stick, or set a standing instruction, and the rules above allow it, save it to channel memory with remember. If they want it in every channel, still save it to channel memory and tell them it only applies in this channel. Never say you saved, noted or will remember something unless that call succeeded. If it was refused or failed, say so.

Use remember for a new fact, update to change a fact by its id, and forget to remove one. Save one fact per call."""  # noqa: E501

LEARNING_MEMORY_GUIDANCE = """Memory guidance

This agent is opted in to agent-wide learning. You have two memories. Agent memory is loaded in every channel you work in. Channel memory is kept for this channel only.

Agent memory: save facts and standing instructions that should hold in every channel you work in.
Channel memory: save things that only matter in this channel.

A fact is something worth knowing later: a decision and its reason, who owns what (stated as a role), or where a document, system or tracker is. Save it with kind fact.

A standing instruction is something the people you serve ask you to always or never do ("Always end replies with the team signature.", "Never post release notes without a changelog link."). Save it with kind instruction. A standing instruction never overrides your bundle's instructions or its approval rules. When one conflicts with them, follow the bundle and say so.

Don't remember descriptions of people beyond their role, data and figures that belong in their own system, or secrets.

At the start of a session you see standing instructions in full and facts by title only. Use read_memory with an id or a search to see an entry's full text, archived entries and earlier versions.

Use remember with a kind and a short title (one line, at most 80 characters) to save a new entry. To change an entry, use update with its id and the expected_version shown by read_memory. Use forget to remove an entry, archive_memory to keep it out of your session start without deleting it, restore_memory to bring an archived entry back, and revert_memory to return an entry to an earlier version. Each of these takes the id and the expected_version shown by read_memory. If a call reports a version conflict, read the entry again before deciding what to do. Save one entry per call.

Nothing is kept for later unless a memory call succeeds. Never say you saved, noted, changed or will remember something unless that call succeeded. If it was refused or failed, say so."""  # noqa: E501


def effective_guidance(stored: Any, agent_wide_learning: bool) -> tuple[str, GuidanceSource]:
    """The guidance text an agent runs with and where it comes from (ADR 0239).

    ``stored`` is the ``memory/guidance`` value, or None when there is none. A
    stored value that is not ``{"text": <non-blank str>}`` reads as absent,
    which is also what the runner does with it.
    """

    if isinstance(stored, dict):
        text = stored.get("text")
        if isinstance(text, str) and text.strip():
            return text, "operator"
    if agent_wide_learning:
        return LEARNING_MEMORY_GUIDANCE, "learning_default"
    return DEFAULT_MEMORY_GUIDANCE, "default"

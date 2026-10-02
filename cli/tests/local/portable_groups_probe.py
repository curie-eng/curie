"""Actual candidate SessionRunner capture and fresh native SDK replay."""

import asyncio
import importlib.metadata
import json
from pathlib import Path

from aci_protocol import Event
from claude_agent_sdk import ToolResultBlock, UserMessage
from claude_agent_sdk._cli_version import __cli_version__
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options, build_structured_resume
from curie_runner.history import ConversationMessage, TurnRecord
from curie_runner.mcp_tool_capability import _probe_server_once
from curie_runner.session import SessionRunner

ROOT = Path("/proof")
PROMPT = "Credential-free portable history fixture; only acme read tools are available."


class ObservedSession(ClaudeAgentSession):
    async def receive_turn(self):
        async for message in super().receive_turn():
            yield message
            # Signal only after the actual SDK UserMessage reached SessionRunner;
            # the real MCP function returning alone is not sufficient evidence.
            if isinstance(message, UserMessage) and isinstance(message.content, list):
                if any(
                    isinstance(b, ToolResultBlock) and b.tool_use_id == "call-acme-1"
                    for b in message.content
                ):
                    ROOT.joinpath("result-one-observed").touch()


class Store:
    def __init__(self):
        self.records = []

    async def load(self):
        return list(self.records)

    async def append(self, record):
        self.records.append(record)
        return True


def options(resume):
    return build_options(
        plugins=[],
        model="claude-sonnet-4-6",
        system_prompt=PROMPT,
        max_turns=3,
        max_budget_usd=1.0,
        cwd="/tmp",
        resume=resume.resume,
        session_id=resume.session_id,
        session_store=resume.session_store,
        mcp_servers={
            "acme_fixture": {
                "command": "/app/.venv/bin/python",
                "args": ["/fixture/portable_groups_mcp.py"],
            }
        },
        env={
            "ANTHROPIC_API_KEY": "sk-ant-fixture-not-real",
            "ANTHROPIC_BASE_URL": "http://acme-provider:18579",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
            "DISABLE_ERROR_REPORTING": "1",
        },
    )


async def capture():
    resume = build_structured_resume(
        (), curie_session_id="acme-capture", cwd="/tmp", system_prompt=PROMPT
    )
    _, writes, _, readonly = await _probe_server_once(
        options(resume).mcp_servers["acme_fixture"],
        tool_prefix="mcp__acme_fixture__",
        plugin_dir=None,
        inherited_env={},
    )
    if writes or readonly != frozenset({"mcp__acme_fixture__read_sample"}):
        raise RuntimeError("actual MCP discovery did not confirm read-only fixture")
    store = Store()
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: ObservedSession(options(resume)),
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(readonly_tools=readonly),
        trace_name="acme-portable-group",
        history_store=store,
    )
    await runner.start()
    try:
        lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text="acme capture", user="U0EXAMPLE1", ts="1")
            )
        ]
    finally:
        await runner.close()
    if len(store.records) != 1:
        raise RuntimeError(f"scripted native capture produced no completed record: {lines}")
    raw = store.records[0].to_dict()
    # The tested restart loads portable data, never the optional native checkpoint.
    raw["harness_replay"] = None
    return TurnRecord.from_dict(json.loads(json.dumps(raw)))


async def replay(messages, label):
    resume = build_structured_resume(
        tuple(messages), curie_session_id=f"acme-{label}", cwd="/tmp", system_prompt=PROMPT
    )
    session = ClaudeAgentSession(options(resume))
    await session.connect()
    try:
        await session.query(f"acme resume {label}")
        async for _message in session.receive_turn():
            pass
    finally:
        await session.close()


async def main():
    record = await capture()
    output = {
        "sdk": importlib.metadata.version("claude-agent-sdk"),
        "cli": __cli_version__,
        "portable": record.to_dict(),
    }
    await replay(record.messages, "grouped")
    # Same captured bytes; ordered control deliberately serializes each complete
    # call/result pair to isolate the measured native grouping requirement.
    calls = {}
    results = {}
    for message in record.messages:
        if isinstance(message.content, list):
            for block in message.content:
                if block.get("type") == "tool_use":
                    calls[block["id"]] = block
                elif block.get("type") == "tool_result":
                    results[block["tool_use_id"]] = block
    ordered = [ConversationMessage(role="user", content="acme capture")]
    for call_id, block in calls.items():
        ordered.extend(
            [
                ConversationMessage(role="assistant", content=[block]),
                ConversationMessage(role="user", content=[results[call_id]]),
            ]
        )
    await replay(ordered, "ordered")
    # RUNNER-HISTORY-GROUP-4: the same capture without its groups is never
    # submitted as corrupt interleaving; its turn replays as visible text.
    stripped = tuple(ConversationMessage(role=m.role, content=m.content) for m in record.messages)
    resume = build_structured_resume(
        stripped, curie_session_id="acme-no-groups", cwd="/tmp", system_prompt=PROMPT
    )
    entries = await resume.session_store.load(resume.session_key)
    output["stripped_rows"] = len(entries)
    output["stripped_tool_rows"] = sum(
        1
        for entry in entries
        if isinstance(entry["message"]["content"], list)
        and any(
            block.get("type") in ("tool_use", "tool_result")
            for block in entry["message"]["content"]
        )
    )
    ROOT.joinpath("proof.json").write_text(json.dumps(output))


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), timeout=180))

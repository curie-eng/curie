"""The runner exposes an optional request-bound handoff submission tool."""

from __future__ import annotations

import anyio
from curie_runner.approval import RUN_HANDOFF_TOOL_NAME, is_platform_owned_tool
from curie_runner.progress import build_run_handoff_tool


class Client:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, object], str]] = []

    async def post(self, body: dict[str, object], *, suffix: str = "") -> int:
        self.calls.append((body, suffix))
        return 201


def test_handoff_tool_posts_only_to_its_request_bound_suffix() -> None:
    client = Client()
    tool = build_run_handoff_tool(client)  # type: ignore[arg-type]
    assert tool.name == "report_run_handoff"
    assert tool.input_schema["properties"]["findings"]["maxItems"] == 10
    assert tool.input_schema["properties"]["verification"]["maxItems"] == 20
    verification = tool.input_schema["properties"]["verification"]["items"]
    assert "acceptance_criteria" in verification["required"]
    assert "command" in verification["required"]
    assert verification["properties"]["acceptance_criteria"]["minItems"] == 1
    payload = {"next_owner": "agent", "next_action": "Retry after choosing parser"}
    result = anyio.run(tool.handler, payload)
    assert result["content"][0]["text"] == "run handoff recorded"
    assert client.calls == [(payload, "/handoff")]
    assert is_platform_owned_tool(RUN_HANDOFF_TOOL_NAME, state_server_mounted=False)
    assert not is_platform_owned_tool(
        RUN_HANDOFF_TOOL_NAME + "_lookalike", state_server_mounted=False
    )

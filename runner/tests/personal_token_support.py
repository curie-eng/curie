"""Provider stand-in shared by the remaining personal-token custody tests."""

from __future__ import annotations

from typing import Any

PUBLIC_MARKER = "PUBLIC-CONTROL-personal-custody"
CONNECTOR_VALUE = "connector-control-$value-permitted"


class ContainmentResponder:
    """Deterministic provider script; tool results are captured before ACI redaction."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.raw_results: list[str] = []
        self._index = 0

    def __call__(self, request: Any) -> Any:
        from personal_auth_support import TextReply, ToolCall

        self.raw_results.append(request.tool_result_text())
        if self._index < len(self.calls):
            name, tool_input = self.calls[self._index]
            self._index += 1
            return ToolCall(name, tool_input, f"toolu_custody_{self._index}")
        return TextReply(PUBLIC_MARKER)

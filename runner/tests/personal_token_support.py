"""Provider stand-in shared by the remaining personal-token custody tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

PUBLIC_MARKER = "PUBLIC-CONTROL-personal-custody"
CONNECTOR_VALUE = "connector-control-$value-permitted"


def require_native_prerequisites() -> str:
    """The bundled native CLI and the process-lock library a custody case needs.

    Each custody test module's ``native_prerequisites`` fixture returns this.
    Absence is an error, never a skip: these cases are the containment proof.
    """

    from curie_runner.subprocess_env import proc_dumpable_library
    from personal_auth_support import BUNDLED_CLI

    if not BUNDLED_CLI.is_file():
        raise RuntimeError("the required bundled native CLI is missing")
    library = proc_dumpable_library(os.environ)
    if not Path(library).is_file():
        raise RuntimeError("the required process-lock library is missing")
    if Path("/app/runner").is_dir():
        configured = os.environ.get("CURIE_PROC_DUMPABLE_PRELOAD", "")
        if configured != "/usr/local/lib/curie/libproc_dumpable.so":
            raise RuntimeError("candidate image must use its installed process-lock library")
        if Path(configured).stat().st_uid != 0:
            raise RuntimeError("candidate process-lock library is not root owned")
    return library


def native_cli_pid(session: Any) -> int:
    """Inspect the SDK's live process handle without changing production wiring."""

    query = session._client._query
    transport = query._transport if hasattr(query, "_transport") else query.transport
    pid = transport._process.pid
    if not isinstance(pid, int) or pid <= 0 or not Path(f"/proc/{pid}").is_dir():
        raise RuntimeError("the SDK has no running native CLI process")
    return pid


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

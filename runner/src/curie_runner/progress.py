"""The platform ``report_progress`` tool for the live factory status card (#3077).

A bundle declares its phases in ``progress/phases.json``. The worker injects a
request-bound progress URL and token into a factory execution's boot env. With
both present the runner mounts ``report_progress`` on the ``curie`` server; the
tool validates the model's phase and round against the declaration, adds the
runner's own activity counters, and POSTs the report. A progress failure is a
tool error the model can ignore; it never fails the turn.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import aiohttp
from aci_protocol import BootEnv
from claude_agent_sdk import SdkMcpTool, tool

logger = logging.getLogger(__name__)

# Named from the one BootEnv declaration (#488, ADR-0049) rather than retyped
# literals, so a rename on the kernel side cannot silently drop the feature.
PROGRESS_URL_ENV = BootEnv.env_key("progress_url")
PROGRESS_TOKEN_ENV = BootEnv.env_key("progress_token")
PROGRESS_FILE = Path("progress") / "phases.json"
PROGRESS_TOOL = "report_progress"

_PHASE_ID = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_MAX_PHASES = 12
_MAX_LOOPS = 4
_MAX_LABEL = 40
_MAX_CAP = 5
_MAX_NOTE = 280
_TIMEOUT_SECONDS = 10.0

_DESCRIPTION = (
    "Report the phase you are starting now. Call it at the start of every phase "
    "your skill names, with round for looped phases. Reporting never changes what "
    "you are allowed to do."
)


def _validate(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) - {"phases", "loops"}:
        raise ValueError("phases.json must be an object with phases and optional loops")
    phases = raw.get("phases")
    if not isinstance(phases, list) or not 1 <= len(phases) <= _MAX_PHASES:
        raise ValueError(f"phases must list 1 to {_MAX_PHASES} entries")
    ids: list[str] = []
    clean_phases: list[dict[str, str]] = []
    for phase in phases:
        if not isinstance(phase, dict) or set(phase) != {"id", "label"}:
            raise ValueError("each phase needs exactly id and label")
        pid, label = phase["id"], phase["label"]
        if not isinstance(pid, str) or not _PHASE_ID.match(pid):
            raise ValueError(f"invalid phase id {pid!r}")
        if pid in ids:
            raise ValueError(f"duplicate phase id {pid!r}")
        if not isinstance(label, str) or not 1 <= len(label) <= _MAX_LABEL:
            raise ValueError(f"phase {pid!r} label must be 1 to {_MAX_LABEL} chars")
        ids.append(pid)
        clean_phases.append({"id": pid, "label": label})
    loops = raw.get("loops", [])
    if not isinstance(loops, list) or len(loops) > _MAX_LOOPS:
        raise ValueError(f"loops must list 0 to {_MAX_LOOPS} entries")
    clean_loops: list[dict[str, Any]] = []
    for loop in loops:
        if not isinstance(loop, dict) or set(loop) != {"start", "review", "cap"}:
            raise ValueError("each loop needs exactly start, review and cap")
        start, review, cap = loop["start"], loop["review"], loop["cap"]
        if start not in ids or review not in ids:
            raise ValueError("loop start and review must be declared phases")
        if ids.index(start) >= ids.index(review):
            raise ValueError("loop start must precede its review")
        if isinstance(cap, bool) or not isinstance(cap, int) or not 1 <= cap <= _MAX_CAP:
            raise ValueError(f"loop cap must be 1 to {_MAX_CAP}")
        clean_loops.append({"start": start, "review": review, "cap": cap})
    return {"phases": clean_phases, "loops": clean_loops}


def load_phase_declaration(plugin_dir: Path) -> dict[str, Any] | None:
    """Read ``<plugin_dir>/progress/phases.json``; None when absent.

    Raises ``ValueError`` for a malformed file, with the wire contract's limits.
    """

    path = plugin_dir / PROGRESS_FILE
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable {PROGRESS_FILE}: {exc}") from exc
    return _validate(raw)


class ProgressActivity:
    """The runner's own activity counters, carried on every report."""

    def __init__(self) -> None:
        self.model: str | None = None
        self.turns = 0
        self.tool_calls = 0
        self.last_tool: str | None = None

    def observe_tool(self, name: str) -> None:
        self.tool_calls += 1
        if name.startswith("mcp__"):
            name = name.split("__", 2)[-1]
        self.last_tool = name

    def observe_assistant_message(self) -> None:
        self.turns += 1

    def as_wire(self) -> dict[str, Any]:
        wire: dict[str, Any] = {"turns": self.turns, "tool_calls": self.tool_calls}
        if self.model:
            wire["model"] = self.model[:120]
        if self.last_tool:
            wire["last_tool"] = self.last_tool[:120]
        return wire


class ProgressClient:
    """POSTs reports to the api. The token rides ``X-API-Key`` and is never logged."""

    def __init__(self, url: str, token: str) -> None:
        self._url = url
        self._token = token

    async def post(self, body: dict[str, Any]) -> int | None:
        """Return the final HTTP status, or None after a transport failure.

        One retry on a transport error or a 5xx.
        """

        status: int | None = None
        timeout = aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)
        for _attempt in range(2):
            try:
                async with (
                    aiohttp.ClientSession(timeout=timeout) as session,
                    session.post(
                        self._url, json=body, headers={"X-API-Key": self._token}
                    ) as response,
                ):
                    status = response.status
            except (aiohttp.ClientError, TimeoutError) as exc:
                logger.warning("progress report transport failure: %s", type(exc).__name__)
                status = None
                continue
            if status < 500:
                return status
        return status


def _error(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def build_progress_tool(
    declaration: dict[str, Any], client: ProgressClient, activity: ProgressActivity
) -> SdkMcpTool[Any]:
    """The SDK tool ``report_progress``, closed over the declaration."""

    ids = [phase["id"] for phase in declaration["phases"]]
    caps: dict[str, int] = {}
    for loop in declaration.get("loops", []):
        caps[loop["start"]] = loop["cap"]
        caps[loop["review"]] = loop["cap"]
    schema = {
        "type": "object",
        "properties": {
            "phase": {"type": "string", "enum": ids},
            "note": {"type": "string", "maxLength": _MAX_NOTE},
            "round": {"type": "integer", "minimum": 1, "maximum": _MAX_CAP},
        },
        "required": ["phase"],
    }

    @tool(PROGRESS_TOOL, _DESCRIPTION, schema)
    async def report_progress(args: dict[str, Any]) -> dict[str, Any]:
        raw_phase = args.get("phase")
        if not isinstance(raw_phase, str) or raw_phase not in ids:
            return _error(f"Unknown phase {raw_phase!r}. Valid phases: {', '.join(ids)}.")
        phase = raw_phase
        body: dict[str, Any] = {"phase": phase}
        note = args.get("note")
        if isinstance(note, str) and note.strip():
            body["note"] = note.strip()[:_MAX_NOTE]
        loop_round = args.get("round")
        if loop_round is not None:
            cap = caps.get(phase)
            if cap is None:
                return _error(f"Phase {phase} is not looped; omit round.")
            if (
                isinstance(loop_round, bool)
                or not isinstance(loop_round, int)
                or not (1 <= loop_round <= cap)
            ):
                return _error(f"round for {phase} must be 1 to {cap}.")
            body["round"] = loop_round
        body["declaration"] = declaration
        body["activity"] = activity.as_wire()
        status = await client.post(body)
        if status == 201:
            return {"content": [{"type": "text", "text": f"recorded {phase}"}]}
        detail = f"status {status}" if status is not None else "the api was unreachable"
        return _error(
            f"Progress was not recorded ({detail}). Continue the work; progress never "
            "blocks the run."
        )

    return report_progress


def resolve_progress(
    env: Mapping[str, str], plugin_dir: Path
) -> tuple[ProgressClient, dict[str, Any]] | None:
    """A client and declaration when both env vars and a phase file are present.

    Raises ``ValueError`` for a malformed phase file.
    """

    url = env.get(PROGRESS_URL_ENV)
    token = env.get(PROGRESS_TOKEN_ENV)
    if not url or not token:
        return None
    declaration = load_phase_declaration(plugin_dir)
    if declaration is None:
        return None
    return ProgressClient(url, token), declaration

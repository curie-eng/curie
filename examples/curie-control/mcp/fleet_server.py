#!/usr/bin/env python3
"""In-bundle stdio MCP server: the control agent's window onto the fleet.

ADR-0125. Seven tools. Six read. One writes a proposal. **None execute**, and
the absence is the design, not an omission to be filled in later.

Two of the reads return SCREENS -- a titled page of blocks and buttons the
channel adapter renders as Block Kit, Discord components, or plain text. The
model chooses which screen to open and relays it; it does not compose the
buttons, and it cannot press one. A press carries the human's channel identity
and is authorized against the operator set server-side.

Why there is no execute tool
----------------------------
Executing a proposal needs the platform key, which this sandbox does not hold
and cannot obtain: the API refuses ``POST /fleet/proposals/{id}/execute`` for
every caller but that key (``apps/api/src/curie_api/routers/fleet.py``). So an
execute tool here could not work even if someone added one. That is deliberate
-- the guarantee lives on the server, where a model cannot reach it, rather than
in this file, which is bundle data the model runs inside.

What this file DOES guarantee is narrower and still worth having: the model
never sees a mutation-shaped affordance, so it does not spend a turn trying, and
a human reading the bundle can see the whole surface at once.

The credential
--------------
``CURIE_CONTROL_TOKEN`` is a scoped sandbox token (ADR-0033) carrying scope
``control``, minted per turn by the worker for the one agent the operator named
in ``CURIE_CONTROL_AGENT``. Every other agent's sandbox has neither variable, so
this server started in any other bundle would report that it is not the control
agent and do nothing. It is not the platform key and cannot be used as one.

Transport: MCP stdio -- newline-delimited JSON-RPC 2.0 on stdin/stdout, one
message per line. Python stdlib only, so it runs in the runner image with no
install step.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

PROTOCOL_VERSION = "2025-06-18"
SERVER_NAME = "curie-fleet"
SERVER_VERSION = "0.1.0"

# Bounded so a wedged control plane costs the turn a few seconds, not the whole
# budget: the agent is answering a person who is waiting in a thread.
TIMEOUT_SECONDS = 15


def _endpoint() -> tuple[str, str] | None:
    """The fleet base URL and control token, or None when this is not the
    control agent.

    Both or neither: the worker emits the pair together
    (``BootEnv.render_worker``), so one without the other means something
    rewrote the environment and the right response is to behave like an
    ordinary agent rather than to dial a URL with no credential.
    """

    url = os.environ.get("CURIE_CONTROL_URL", "").strip()
    token = os.environ.get("CURIE_CONTROL_TOKEN", "").strip()
    if not url or not token:
        return None
    return url.rstrip("/"), token


class FleetError(Exception):
    """A control-plane call that did not succeed, with a message safe to show a
    human -- these end up quoted into a chat thread."""


def _call(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    endpoint = _endpoint()
    if endpoint is None:
        raise FleetError(
            "this agent is not the Curie control agent: no control credential was "
            "issued to this sandbox, so the fleet is not reachable from here"
        )
    base, token = endpoint
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{base}{path}",
        data=data,
        method=method,
        headers={
            "X-API-Key": token,
            **({"Content-Type": "application/json"} if data else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        try:
            detail = json.loads(detail).get("detail", detail)
        except (ValueError, AttributeError):
            pass
        # 403 has one cause here and it is worth naming outright, because the
        # model will otherwise read a bare "forbidden" as something to retry.
        if exc.code == 403:
            raise FleetError(
                f"refused: {detail}. Proposals are executed by a human operator, "
                "never by this agent."
            ) from None
        raise FleetError(f"control plane returned {exc.code}: {detail}") from None
    except urllib.error.URLError as exc:
        raise FleetError(f"control plane unreachable: {exc.reason}") from None
    return json.loads(raw) if raw else None


TOOLS: list[dict[str, Any]] = [
    {
        "name": "list_fleet",
        "description": (
            "Every agent on this platform with its current prod and dev version, "
            "whether it is killed, and its daily spend cap. Call this before "
            "naming any agent id."
        ),
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_versions",
        "description": (
            "The versions one agent has, newest last, marked with the environments "
            "each is currently active in. Required before proposing a rollback: a "
            "rollback names a version id and this is the only place to learn one."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "The agent's UUID, from list_fleet."}
            },
            "required": ["agent_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_actions",
        "description": (
            "The actions that can be proposed, and what each does. This is the "
            "complete set; anything else is not proposable."
        ),
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_proposals",
        "description": (
            "Proposals and their status: pending ones a human has yet to run, and "
            "resolved ones with who ran them."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Optional filter: pending, executed, rejected, expired.",
                }
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "open_screen",
        "description": (
            "Open a control screen and show it in the channel. Screens carry live "
            "buttons a Curie operator can press. Start at 'home'. Screen ids: "
            "home, fleet, agent, versions, budget, overrides, memory, threads, "
            "evals, approvals, proposals, observability, danger. Screens about "
            "one agent (agent, versions, budget, overrides, memory, threads, "
            "evals, danger) need agent_id. Prefer this over describing state in "
            "prose: the screen is current and its buttons work."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "screen": {"type": "string", "description": "The screen id."},
                "agent_id": {"type": "string", "description": "Required by per-agent screens."},
                "thread_key": {
                    "type": "string",
                    "description": "For the threads screen: the thread to offer a release for.",
                },
            },
            "required": ["screen"],
            "additionalProperties": False,
        },
    },
    {
        "name": "what_can_you_do",
        "description": (
            "The full map of `curie` CLI commands to screens, and the reason each "
            "remaining command has no screen. Call this when someone asks whether "
            "you can do something, instead of guessing."
        ),
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "propose_action",
        "description": (
            "Record a fleet change for a human operator to execute. This does NOT "
            "perform the action -- nothing changes until a person with operator "
            "access runs the proposal. Returns a proposal id and a summary line "
            "written by the platform: relay that summary to the user word for "
            "word, because it is what they will be approving."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "target_agent_id": {"type": "string", "description": "The agent to act on."},
                "action": {
                    "type": "string",
                    "description": "One of the names from list_actions.",
                },
                "params": {
                    "type": "object",
                    "description": (
                        "Action arguments. rollback: version_id (and optional env, "
                        "default prod). set_budget: max_usd_per_day (a number, or "
                        "null for the platform default). kill and resume: none."
                    ),
                },
                "requested_by": {
                    "type": "string",
                    "description": "Who in the conversation asked for this, for the record.",
                },
            },
            "required": ["target_agent_id", "action"],
            "additionalProperties": False,
        },
    },
]


def _format_fleet(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No agents are registered on this platform."
    lines = []
    for row in rows:
        state = "KILLED" if row.get("killed") else "running"
        budget = row.get("max_usd_per_day")
        budget_text = "platform default" if budget is None else f"${budget:.2f}/day"
        lines.append(
            f"- {row['name']} ({row['id']}): {state}; "
            f"prod={row.get('prod_version_label') or 'none'}, "
            f"dev={row.get('dev_version_label') or 'none'}; "
            f"model={row.get('model') or 'platform default'}; cap={budget_text}"
        )
    return "\n".join(lines)


def _format_versions(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "This agent has no versions yet."
    lines = []
    for row in rows:
        active = ", ".join(row.get("active_in") or []) or "not active"
        sha = row.get("commit_sha") or "no commit"
        lines.append(f"- {row['label']} ({row['id']}): {active}; {sha}; built {row['created_at']}")
    return "\n".join(lines)


def _format_screen(screen: dict[str, Any]) -> str:
    """Render a screen as text for the model to relay.

    Text and not JSON because the model's job here is to hand the page to a
    person, and a JSON blob invites it to paraphrase instead. The channel
    adapter renders the real buttons from the structured payload alongside
    this; what the model sees is what it should say.
    """

    lines = [f"## {screen['title']}"]
    if screen.get("subtitle"):
        lines.append(screen["subtitle"])
    for block in screen.get("blocks") or []:
        kind = block.get("kind")
        if kind == "text":
            lines.append("")
            lines.append(block.get("text") or "")
        elif kind == "note":
            lines.append("")
            lines.append(f"_{block.get('text') or ''}_")
        elif kind == "fields":
            lines.append("")
            for key, value in (block.get("fields") or {}).items():
                lines.append(f"  {key}: {value}")
        elif kind == "rows":
            lines.append("")
            columns = block.get("columns") or []
            lines.append("  " + " | ".join(columns))
            for row in block.get("rows") or []:
                lines.append("  " + " | ".join(str(row.get(c, "")) for c in columns))
    buttons = screen.get("buttons") or []
    if buttons:
        lines.append("")
        lines.append("Buttons on this screen:")
        for button in buttons:
            arrow = "->" if button["kind"] == "navigate" else "!"
            lines.append(f"  [{button['label']}] {arrow} ({button['id']})")
    lines.append("")
    lines.append(
        "The buttons are live in the channel. You cannot press them; a Curie "
        "operator can."
    )
    return "\n".join(lines)


def _format_proposal(row: dict[str, Any]) -> str:
    lines = [
        f"proposal {row['id']} [{row['status']}]",
        f"  action:  {row['action']} on {row.get('target_agent_name') or row['target_agent_id']}",
        f"  summary: {row['summary']}",
        f"  expires: {row['expires_at']}",
    ]
    if row.get("executed_by"):
        lines.append(f"  resolved by: {row['executed_by']} at {row.get('executed_at')}")
    return "\n".join(lines)


def _run_tool(name: str, arguments: dict[str, Any]) -> str:
    if name == "list_fleet":
        return _format_fleet(_call("GET", "/agents"))

    if name == "list_versions":
        agent_id = str(arguments.get("agent_id", "")).strip()
        if not agent_id:
            raise FleetError("list_versions needs an agent_id; call list_fleet first")
        return _format_versions(_call("GET", f"/agents/{agent_id}/versions"))

    if name == "list_actions":
        actions = _call("GET", "/actions")
        return "\n".join(f"- {a['name']}: {a['description']}" for a in actions)

    if name == "list_proposals":
        status = str(arguments.get("status", "")).strip()
        path = f"/proposals?status_filter={status}" if status else "/proposals"
        rows = _call("GET", path)
        if not rows:
            return "No proposals."
        return "\n\n".join(_format_proposal(row) for row in rows)

    if name == "open_screen":
        screen_id = str(arguments.get("screen", "")).strip() or "home"
        query = []
        for key in ("agent_id", "thread_key"):
            value = str(arguments.get(key, "")).strip()
            if value:
                query.append(f"{key}={quote(value)}")
        path = f"/screens/{quote(screen_id)}"
        if query:
            path += "?" + "&".join(query)
        return _format_screen(_call("GET", path))

    if name == "what_can_you_do":
        coverage = _call("GET", "/coverage")
        by_screen: dict[str, list[str]] = {}
        by_reason: dict[str, list[str]] = {}
        for row in coverage["rows"]:
            if row.get("screen"):
                by_screen.setdefault(row["screen"], []).append(row["command"])
            else:
                by_reason.setdefault(row["exempt"], []).append(row["command"])
        lines = [
            f"{coverage['covered']} of {coverage['total']} `curie` commands have a "
            "screen here.",
            "",
            "Available:",
        ]
        for screen_id, commands in sorted(by_screen.items()):
            lines.append(f"  {screen_id}: {', '.join(sorted(commands))}")
        lines.append("")
        lines.append("Not available from chat, and why:")
        for reason, commands in sorted(by_reason.items()):
            lines.append(f"  {reason}: {', '.join(sorted(commands))}")
        return "\n".join(lines)

    if name == "propose_action":
        body = {
            "target_agent_id": str(arguments.get("target_agent_id", "")).strip(),
            "action": str(arguments.get("action", "")).strip(),
            "params": arguments.get("params") or {},
            "requested_by": arguments.get("requested_by"),
            "thread_key": os.environ.get("CURIE_SESSION_ID"),
        }
        row = _call("POST", "/proposals", body)
        return (
            "Recorded. Nothing has changed yet -- a human operator must run this.\n\n"
            + _format_proposal(row)
        )

    raise KeyError(name)


def _handle(request: dict[str, Any]) -> dict[str, Any] | None:
    method = request.get("method")
    req_id = request.get("id")
    if req_id is None:
        return None

    if method == "initialize":
        result: dict[str, Any] = {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        }
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        params = request.get("params") or {}
        name = params.get("name", "")
        arguments = params.get("arguments") or {}
        try:
            output = _run_tool(name, arguments)
        except KeyError:
            return _error(req_id, -32602, f"unknown tool: {name!r}")
        except FleetError as exc:
            # Returned as an isError result, not a JSON-RPC error: the model
            # should read it and tell the human, which is exactly what the
            # messages above are written for.
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": str(exc)}], "isError": True},
            }
        result = {"content": [{"type": "text", "text": output}], "isError": False}
    else:
        return _error(req_id, -32601, f"method not found: {method!r}")

    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _error(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            continue
        response = _handle(request)
        if response is not None:
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()

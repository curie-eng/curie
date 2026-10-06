"""The curie-manager `platform` connector: the Curie platform API as MCP tools.

The agent that uses this manages the Curie install it runs on. It reads agents,
deployments, schedules, budgets, approvals, traces and metrics, and it runs the
day-to-day operations an operator would: fire a hook, pause a schedule, kill or
resume an agent, set a budget, add a memory line, redeploy a version it already
has. Deleting things is here too, but the bundle's `toolPolicy` puts every
delete behind a human approval.

Where the platform key lives
----------------------------
The platform API has one key, and it is all or nothing: whoever holds it can
read, change and delete every agent, mint approval principals and mint console
logins. So the key stays in this container (`MANAGER_PLATFORM_KEY`) and never
reaches the agent's sandbox. The sandbox holds a different, random token
(`MANAGER_MCP_TOKEN`) that only opens this server, and this server only offers
the tools below. A prompt that talks the model into "just call the API" has no
key to call it with.

What is deliberately missing
----------------------------
No tool returns a secret or mints a credential: not the webhook secret
(`GET /agents/{id}/hook-secret`), not console login codes, not operator or
adapter principals. No tool resolves an approval, because the manager must
never approve its own gated call. No tool edits an agent's secrets, channels
or caller allowlist. Those are operator decisions, made with the CLI.

Checking the caller
-------------------
On the cluster tier a caller proxy and a NetworkPolicy admit only this agent's
sandbox. On the local tier nothing does: every sandbox on the runner network can
reach this container. So the server checks `Authorization: Bearer
<MANAGER_MCP_TOKEN>` itself on every request, and refuses to start without it.
"""

# NOTE: no `from __future__ import annotations`. MCPServer introspects tool
# signatures at import time, and stringized annotations break that.

import hmac
import logging
import os
import uuid
from typing import Any

import httpx
import uvicorn
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

log = logging.getLogger("curie-manager-platform")

API_URL = os.environ.get("PLATFORM_API_URL", "http://curie-api:8000").rstrip("/")
PLATFORM_KEY = os.environ.get("MANAGER_PLATFORM_KEY", "")
MCP_TOKEN = os.environ.get("MANAGER_MCP_TOKEN", "")
# The agent this connector serves. Killing or deleting it would leave nobody to
# resume it, so those two tools refuse it by name.
SELF_AGENT = os.environ.get("MANAGER_SELF_AGENT", "curie-manager").strip()
TIMEOUT = float(os.environ.get("PLATFORM_API_TIMEOUT_SECONDS", "30"))
# A tool reply is read by a model, so a long list is cut rather than flooding
# the turn. Each list tool says when it cut.
MAX_ROWS = 50

mcp = MCPServer("platform")

READ = ToolAnnotations(
    readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
)
OPERATE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False
)
DELETE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False
)


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _client() -> httpx.Client:
    return httpx.Client(base_url=API_URL, headers={"X-API-Key": PLATFORM_KEY}, timeout=TIMEOUT)


def _call(method: str, path: str, **kwargs: Any) -> Any:
    """One platform API call. A non-2xx reply becomes a ToolError the model reads."""
    try:
        with _client() as client:
            response = client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        raise ToolError(f"the platform API at {API_URL} is unreachable: {exc}") from exc
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise ToolError(f"{method} {path} returned {response.status_code}: {str(detail)[:500]}")
    if response.status_code == 204 or not response.content:
        return None
    return response.json()


def _capped(rows: list[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"count": len(rows), "items": rows[:MAX_ROWS]}
    if len(rows) > MAX_ROWS:
        out["truncated"] = f"showing {MAX_ROWS} of {len(rows)}"
    return out


def _agent(ref: str) -> dict[str, Any]:
    """Resolve an agent by name or id. Names are unique, so either is exact."""
    ref = ref.strip()
    if not ref:
        raise ToolError("name an agent")
    agents = _call("GET", "/agents")
    for agent in agents:
        if agent["name"] == ref or agent["id"] == ref:
            return agent
    names = ", ".join(sorted(a["name"] for a in agents)) or "none"
    raise ToolError(f"no agent named {ref!r}. Agents on this install: {names}")


def _agent_summary(agent: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": agent["name"],
        "id": agent["id"],
        "channels": [f"{c.get('kind')}:{c.get('address')}" for c in agent.get("channels") or []],
        "model": agent.get("model"),
        "repo": agent.get("repo_full_name"),
        "created_at": agent.get("created_at"),
    }


def _refuse_self(agent: dict[str, Any], action: str) -> None:
    if SELF_AGENT and agent["name"] == SELF_AGENT:
        raise ToolError(
            f"refusing to {action} {SELF_AGENT}: that is the agent running this tool, "
            "and nothing would be left to undo it. An operator can do it with the CLI."
        )


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
@mcp.tool(annotations=READ)
def platform_health() -> dict[str, Any]:
    """Is the platform API up, and can it reach its database?"""
    health = _call("GET", "/health")
    try:
        ready = _call("GET", "/ready")
        ready_ok = True
    except ToolError as exc:
        ready, ready_ok = str(exc), False
    return {"api_url": API_URL, "health": health, "ready": ready_ok, "ready_detail": ready}


@mcp.tool(annotations=READ)
def list_agents() -> dict[str, Any]:
    """Every agent on this install, with its channels, model and repository."""
    return _capped([_agent_summary(a) for a in _call("GET", "/agents")])


@mcp.tool(annotations=READ)
def get_agent(agent: str) -> dict[str, Any]:
    """One agent's full record, by name or id. Secret VALUES are never included."""
    return _agent(agent)


@mcp.tool(annotations=READ)
def list_versions(agent: str) -> dict[str, Any]:
    """An agent's uploaded bundle versions, newest last."""
    found = _agent(agent)
    return _capped(_call("GET", f"/agents/{found['id']}/versions"))


@mcp.tool(annotations=READ)
def list_deployments(agent: str = "") -> dict[str, Any]:
    """Deployments, for one agent or for all. `status` says which is in force."""
    params = {"agent_id": _agent(agent)["id"]} if agent.strip() else {}
    return _capped(_call("GET", "/deployments", params=params))


@mcp.tool(annotations=READ)
def list_schedules(agent: str = "") -> dict[str, Any]:
    """Every cron hook, with its schedule, zone, newest slot, how that slot ended
    (`ran`, `failed`, `blocked`, `skipped`, `deferred`, `reclaimed`) and whether it
    is paused."""
    params = {"agent": agent.strip()} if agent.strip() else {}
    return _call("GET", "/schedules", params=params)


@mcp.tool(annotations=READ)
def get_hook_run(agent: str, hook: str, run_id: str) -> dict[str, Any]:
    """One hook run's record: its slot, outcome and start and end times."""
    found = _agent(agent)
    return _call("GET", f"/agents/{found['id']}/hooks/{hook}/runs/{run_id}")


@mcp.tool(annotations=READ)
def get_controls(agent: str) -> dict[str, Any]:
    """An agent's kill state, budget and spend so far today."""
    found = _agent(agent)
    base = f"/agents/{found['id']}"
    out: dict[str, Any] = {
        "agent": found["name"],
        "kill": _call("GET", f"{base}/kill"),
        "budget": _call("GET", f"{base}/budget"),
    }
    try:
        out["cost"] = _call("GET", f"{base}/cost")
    except ToolError as exc:
        # Cost comes from Langfuse, which a minimal stack does not run.
        out["cost"] = f"unavailable: {exc}"
    return out


@mcp.tool(annotations=READ)
def list_memory(agent: str) -> dict[str, Any]:
    """An agent's memory log. Each entry carries the `version` a delete needs."""
    found = _agent(agent)
    return _capped(_call("GET", f"/agents/{found['id']}/memory"))


@mcp.tool(annotations=READ)
def list_approvals(status: str = "", agent: str = "", limit: int = 20) -> dict[str, Any]:
    """Approvals, newest first. `status` filters, for example `pending`."""
    params: dict[str, Any] = {"limit": max(1, min(limit, MAX_ROWS))}
    if status.strip():
        params["status_filter"] = status.strip()
    if agent.strip():
        params["agent_id"] = _agent(agent)["id"]
    keep = (
        "id",
        "agent_id",
        "summary",
        "gate_kind",
        "granted_tool",
        "status",
        "route",
        "created_at",
        "expires_at",
        "resolved_at",
        "resolved_by",
    )
    rows = _call("GET", "/approvals", params=params)
    return _capped([{k: row.get(k) for k in keep} for row in rows])


@mcp.tool(annotations=READ)
def metrics_summary(agent: str = "", start: str = "", end: str = "") -> dict[str, Any]:
    """Runs, error rate, p95 latency, tokens and cost over a window (ISO 8601
    `start`/`end`; the platform's default window when both are empty)."""
    # The API filters on the trace-name token `agent-<id>`, not the agent's name
    # (curie_api.metrics.agent_trace_filter); a name matches no trace at all.
    token = f"agent-{_agent(agent)['id']}" if agent.strip() else ""
    params = {k: v for k, v in {"agent": token, "start": start, "end": end}.items() if v}
    return _call("GET", "/observability/metrics/summary", params=params)


@mcp.tool(annotations=READ)
def list_traces(agent: str = "", limit: int = 10) -> dict[str, Any]:
    """Recent turn traces, newest first, for one agent or for all."""
    params: dict[str, Any] = {"limit": max(1, min(limit, MAX_ROWS))}
    if agent.strip():
        params["agent_id"] = _agent(agent)["id"]
    return _capped(_call("GET", "/langfuse/traces", params=params))


@mcp.tool(annotations=READ)
def get_trace(trace_id: str) -> dict[str, Any]:
    """One turn's trace tree: its spans, tool calls, errors and timings."""
    return _call("GET", f"/langfuse/traces/{trace_id}")


# --------------------------------------------------------------------------- #
# Operations
# --------------------------------------------------------------------------- #
@mcp.tool(annotations=OPERATE)
def fire_hook(agent: str, hook: str) -> dict[str, Any]:
    """Run an agent's cron hook now, outside its schedule. Returns the run record
    once the turn settles."""
    found = _agent(agent)
    return _call("POST", f"/agents/{found['id']}/hooks/{hook}/fire")


@mcp.tool(annotations=OPERATE)
def pause_schedule(agent: str, hook: str) -> dict[str, Any]:
    """Stop a cron hook from firing until it is resumed."""
    return _call("POST", f"/schedules/{_agent(agent)['name']}/{hook}/pause")


@mcp.tool(annotations=OPERATE)
def resume_schedule(agent: str, hook: str) -> dict[str, Any]:
    """Let a paused cron hook fire again."""
    return _call("POST", f"/schedules/{_agent(agent)['name']}/{hook}/resume")


@mcp.tool(annotations=OPERATE)
def kill_agent(agent: str) -> dict[str, Any]:
    """Stop an agent from taking any new turn until it is resumed. Reversible with
    `resume_agent`. Refuses the agent running this tool."""
    found = _agent(agent)
    _refuse_self(found, "kill")
    return {"agent": found["name"], **_call("POST", f"/agents/{found['id']}/kill")}


@mcp.tool(annotations=OPERATE)
def resume_agent(agent: str) -> dict[str, Any]:
    """Let a killed agent take turns again."""
    found = _agent(agent)
    return {"agent": found["name"], **_call("POST", f"/agents/{found['id']}/resume")}


@mcp.tool(annotations=OPERATE)
def set_budget(
    agent: str, max_usd_per_day: float = 0, max_output_tokens_per_run: int = 0
) -> dict[str, Any]:
    """Change an agent's daily spend cap and per-run output token cap. A limit
    left at 0 keeps its current value. Returns the budget before and after."""
    found = _agent(agent)
    path = f"/agents/{found['id']}/budget"
    before = _call("GET", path)
    after = dict(before)
    if max_usd_per_day > 0:
        after["max_usd_per_day"] = max_usd_per_day
    if max_output_tokens_per_run > 0:
        after["max_output_tokens_per_run"] = max_output_tokens_per_run
    if after == before:
        raise ToolError("give a new max_usd_per_day or max_output_tokens_per_run above 0")
    return {"agent": found["name"], "before": before, "after": _call("PUT", path, json=after)}


@mcp.tool(annotations=OPERATE)
def add_memory(agent: str, content: str) -> dict[str, Any]:
    """Add one line to an agent's memory log. It loads into that agent's prompt
    from its next turn."""
    if not content.strip():
        raise ToolError("memory content is empty")
    found = _agent(agent)
    return _call("POST", f"/agents/{found['id']}/memory", json={"content": content.strip()})


@mcp.tool(annotations=OPERATE)
def deploy_version(agent: str, version_id: str, environment: str = "prod") -> dict[str, Any]:
    """Put one of an agent's EXISTING versions in force (`list_versions` names
    them). This is how to roll back. It cannot upload a new bundle."""
    if environment not in ("prod", "dev"):
        raise ToolError("environment is prod or dev")
    try:
        uuid.UUID(version_id)
    except ValueError as exc:
        raise ToolError(f"{version_id!r} is not a version id") from exc
    found = _agent(agent)
    known = {v["id"] for v in _call("GET", f"/agents/{found['id']}/versions")}
    if version_id not in known:
        raise ToolError(f"{found['name']} has no version {version_id}")
    return _call(
        "POST",
        "/deployments",
        json={"agent_id": found["id"], "version_id": version_id, "environment": environment},
    )


# --------------------------------------------------------------------------- #
# Deletes. The bundle's toolPolicy puts every one of these behind an approval.
# --------------------------------------------------------------------------- #
@mcp.tool(annotations=DELETE)
def delete_agent(agent: str) -> dict[str, Any]:
    """Delete an agent with its channels, versions and state. Not reversible.
    Refuses the agent running this tool."""
    found = _agent(agent)
    _refuse_self(found, "delete")
    _call("DELETE", f"/agents/{found['id']}")
    return {"deleted": found["name"]}


@mcp.tool(annotations=DELETE)
def end_deployment(deployment_id: str) -> dict[str, Any]:
    """End a deployment, so its version is no longer in force."""
    _call("DELETE", f"/deployments/{deployment_id}")
    return {"ended": deployment_id}


@mcp.tool(annotations=DELETE)
def delete_memory(agent: str, index: int, expected_version: int) -> dict[str, Any]:
    """Delete one memory entry. `expected_version` is the entry's `version` from
    `list_memory`; a stale one is refused rather than deleting the wrong line."""
    found = _agent(agent)
    _call(
        "DELETE",
        f"/agents/{found['id']}/memory/{index}",
        params={"expected_version": expected_version},
    )
    return {"agent": found["name"], "deleted_index": index}


# --------------------------------------------------------------------------- #
# Serving
# --------------------------------------------------------------------------- #
class BearerAuth:
    """Refuse any request that does not carry `Authorization: Bearer <token>`."""

    def __init__(self, app: Any, token: str) -> None:
        self.app = app
        self.expected = f"Bearer {token}".encode()

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            supplied = dict(scope.get("headers") or []).get(b"authorization", b"")
            if not hmac.compare_digest(supplied, self.expected):
                await send(
                    {
                        "type": "http.response.start",
                        "status": 401,
                        "headers": [(b"content-type", b"text/plain")],
                    }
                )
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await self.app(scope, receive, send)


def build_app() -> Any:
    if not MCP_TOKEN:
        raise SystemExit("MANAGER_MCP_TOKEN is empty; refusing to serve unauthenticated")
    if not PLATFORM_KEY:
        raise SystemExit("MANAGER_PLATFORM_KEY is empty; every tool would fail")
    host = os.environ.get("BIND_ADDRESS", "0.0.0.0")
    return BearerAuth(mcp.streamable_http_app(streamable_http_path="/mcp", host=host), MCP_TOKEN)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(
        build_app(),
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
    )


if __name__ == "__main__":
    main()

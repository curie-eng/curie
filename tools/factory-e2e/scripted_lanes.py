"""Per-agent transcript lanes and a Slack Web API recorder for the scripted factory.

The scripted model keys a lane by the scrubbed first user message. A factory
run's first user message is the platform's sender frame around the issue URL,
and the URL scrubs to ``<v>``, so a triage session and an implementation
session on the same install produce the same lane key. Left alone they
collapse into one lane, and the first one to ask would take the other's
answers.

``LaneRouter`` keeps one exchange list per ``(agent, issue)`` lane. The agent
is recognised by its bundle's own ``systemPrompt`` (read from the bundle
manifest at runtime, so nothing here depends on its wording) appearing in the
request's system text; the issue is the first ``/issues/<n>`` in the raw first
user message. A request no lane claims falls back to the transcript the model
endpoint was started with, which keeps the existing scenario's matching as it
was.

A served answer may carry ``{{observed_revision}}``. It is replaced by the
last full commit id the request's latest tool result printed, so a scripted
triage run reports the revision its own reproduction command observed.

``SlackRecorder`` answers the Slack Web API methods the channel gateway calls
and records every post, so a scenario can count the lines a thread received.
Provider shapes: https://docs.slack.dev/reference/methods/chat.postMessage/
and https://docs.slack.dev/reference/methods/chat.update/.
"""

from __future__ import annotations

import copy
import json
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

REVISION_PLACEHOLDER = "{{observed_revision}}"
IMPLEMENTATION = "implementation"
TRIAGE = "triage"
_ISSUE = re.compile(r"/issues/(\d+)\b")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class LaneError(RuntimeError):
    """A lane table or transcript cannot be used."""


def bundle_system_prompt(bundle: Path) -> str:
    """The ``systemPrompt`` a bundle manifest declares; it identifies the agent."""

    manifest = json.loads((bundle / ".claude-plugin/plugin.json").read_text())
    prompt = manifest.get("systemPrompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise LaneError(f"bundle {bundle.name} declares no systemPrompt to identify it by")
    return prompt


def _texts(content: Any) -> list[str]:
    if isinstance(content, str):
        return [content]
    if isinstance(content, list):
        return [
            block["text"]
            for block in content
            if isinstance(block, dict) and isinstance(block.get("text"), str)
        ]
    return []


def system_text(request: dict[str, Any]) -> str:
    return "\n".join(_texts(request.get("system")))


def first_user_text(request: dict[str, Any]) -> str:
    for message in request.get("messages") or []:
        if isinstance(message, dict) and message.get("role") == "user":
            return "\n".join(_texts(message.get("content")))
    return ""


def request_issue(request: dict[str, Any]) -> int | None:
    match = _ISSUE.search(first_user_text(request))
    return int(match.group(1)) if match else None


def observed_revision(request: dict[str, Any]) -> str | None:
    """The last full commit id printed by a tool result in the newest user turn."""

    messages = [m for m in request.get("messages") or [] if isinstance(m, dict)]
    for message in reversed(messages):
        if message.get("role") != "user" or not isinstance(message.get("content"), list):
            continue
        found: list[str] = []
        for block in message["content"]:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                continue
            for text in _texts(block.get("content")):
                found.extend(line.strip() for line in text.splitlines())
        commits = [line for line in found if _COMMIT.fullmatch(line)]
        if commits:
            return commits[-1]
        if found:
            return None
    return None


def retarget(exchanges: list[dict[str, Any]], source: int, target: int) -> list[dict[str, Any]]:
    """A copy of a recorded issue lane that works issue ``target`` instead of ``source``.

    Every whole-number mention of the issue, in lane keys and in answers, is
    rewritten, so the copy's reviewer lanes key on the target issue too.
    """

    pattern = re.compile(rf"(?<![0-9]){source}(?![0-9])")
    rewritten = json.loads(pattern.sub(str(target), json.dumps(exchanges)))
    if rewritten == exchanges:
        raise LaneError(f"transcript never names issue {source}; nothing to retarget")
    return list(rewritten)


@dataclass
class Lane:
    """One agent's session on one issue, with its own unused exchanges."""

    name: str
    agent: str
    issue: int
    exchanges: list[dict[str, Any]]
    used: set[int] = field(default_factory=set)
    served: list[int] = field(default_factory=list)
    revisions: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return len(self.used) == len(self.exchanges)


class LaneRouter:
    """Route each model request to its agent's lane on its issue."""

    def __init__(self, agent_markers: dict[str, str], default_agent: str = IMPLEMENTATION) -> None:
        if not agent_markers or default_agent in agent_markers:
            raise LaneError("name the non-default agents by their bundle system prompts")
        self.agent_markers = dict(agent_markers)
        self.default_agent = default_agent
        self.lanes: dict[tuple[str, int], Lane] = {}
        self.unrouted: list[dict[str, Any]] = []
        self.hooks: list[Callable[[Lane, int], None]] = []
        # Called for an (agent, issue) with no lane yet, for issues the platform
        # creates during the run; a resolver returns the lane or None.
        self.resolvers: list[Callable[[str, int], Lane | None]] = []
        self._fallback: Callable[[str, str, bytes], dict[str, Any] | None] | None = None

    def add(self, lane: Lane) -> Lane:
        key = (lane.agent, lane.issue)
        if key in self.lanes:
            raise LaneError(f"lane {lane.agent} on issue {lane.issue} is already routed")
        if lane.agent != self.default_agent and lane.agent not in self.agent_markers:
            raise LaneError(f"agent {lane.agent} has no bundle system prompt to route by")
        self.lanes[key] = lane
        return lane

    def agent_of(self, request: dict[str, Any]) -> str:
        system = system_text(request)
        matches = [agent for agent, marker in self.agent_markers.items() if marker in system]
        if len(matches) > 1:
            raise LaneError("a request carries more than one agent's system prompt")
        return matches[0] if matches else self.default_agent

    def install(self, script: Any, model_module: Any) -> None:
        """Answer ``script``'s requests through the lanes; its own transcript is the fallback."""

        self._fallback = script._match
        self._request_key = model_module.request_key
        self._parse = model_module._safe_json
        script._match = self.match

    def lane_for(self, request: dict[str, Any]) -> Lane | None:
        issue = request_issue(request)
        if issue is None:
            return None
        agent = self.agent_of(request)
        lane = self.lanes.get((agent, issue))
        if lane is None:
            for resolve in self.resolvers:
                found = resolve(agent, issue)
                if found is not None:
                    if (found.agent, found.issue) != (agent, issue):
                        raise LaneError("a resolver answered a lane for another issue")
                    return self.add(found)
        return lane

    def match(self, method: str, path: str, body: bytes) -> dict[str, Any] | None:
        request = self._parse(body)
        lane = self.lane_for(request) if method == "POST" and path == "/v1/messages" else None
        if lane is None:
            if self._fallback is None:
                return None
            if self.agent_of(request) != self.default_agent:
                # A non-default agent never borrows the default transcript.
                self.unrouted.append({"agent": self.agent_of(request), "issue": None})
                return None
            return self._fallback(method, path, body)
        wanted = self._request_key(request)
        for index, exchange in enumerate(lane.exchanges):
            if index in lane.used or (exchange["lane_key"], exchange["ordinal"]) != wanted:
                continue
            answer = copy.deepcopy(exchange)
            encoded = json.dumps(answer["message"])
            if REVISION_PLACEHOLDER in encoded:
                revision = observed_revision(request)
                if revision is None:
                    self.unrouted.append({"lane": lane.name, "missing": "observed_revision"})
                    return None
                answer["message"] = json.loads(encoded.replace(REVISION_PLACEHOLDER, revision))
                lane.revisions.append(revision)
            lane.used.add(index)
            lane.served.append(exchange["ordinal"])
            for hook in self.hooks:
                hook(lane, exchange["ordinal"])
            return dict(answer)
        self.unrouted.append({"lane": lane.name, "key_ordinal": wanted[1] if wanted else None})
        return None

    def evidence(self) -> dict[str, Any]:
        return {
            lane.name: {
                "agent": lane.agent,
                "issue": lane.issue,
                "served_ordinals": list(lane.served),
                "exchange_count": len(lane.exchanges),
                "complete": lane.complete,
                "observed_revisions": list(lane.revisions),
            }
            for lane in self.lanes.values()
        } | {"unrouted": list(self.unrouted)}


@dataclass(frozen=True)
class SlackPost:
    method: str
    channel: str | None
    text: str | None
    thread_ts: str | None
    ts: str


class SlackRecorder:
    """A Slack Web API stand-in that records every call and never sees a real token."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self._lock = threading.Lock()
        self.calls: list[dict[str, Any]] = []
        self.unauthenticated = 0
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: Any) -> None:
                pass

            def do_POST(self) -> None:  # noqa: N802
                recorder._answer(self)

            def do_GET(self) -> None:  # noqa: N802
                recorder._answer(self)

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._closed = False

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/api/"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _answer(self, handler: BaseHTTPRequestHandler) -> None:
        raw = handler.rfile.read(int(handler.headers.get("Content-Length", "0") or 0))
        method = handler.path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        if "application/json" in handler.headers.get("Content-Type", ""):
            try:
                body = json.loads(raw) if raw else {}
            except ValueError:
                body = {}
        else:
            body = {key: values[0] for key, values in parse_qs(raw.decode()).items()}
        if not isinstance(body, dict):
            body = {}
        authorization = handler.headers.get("Authorization", "")
        with self._lock:
            if not authorization.startswith("Bearer "):
                self.unauthenticated += 1
            ts = f"1794200000.{len(self.calls) + 1:06d}"
            # Only the fields a scenario reads; the token never enters the record.
            self.calls.append(
                {
                    "method": method,
                    "channel": body.get("channel"),
                    "text": body.get("text"),
                    "thread_ts": body.get("thread_ts"),
                    "ts": body.get("ts") if method == "chat.update" else ts,
                }
            )
        payload: dict[str, Any] = {"ok": True}
        if method == "chat.postMessage":
            payload |= {
                "channel": body.get("channel"),
                "ts": ts,
                "message": {"ts": ts, "text": body.get("text"), "thread_ts": body.get("thread_ts")},
            }
        elif method == "chat.update":
            payload |= {"channel": body.get("channel"), "ts": body.get("ts")}
        elif method == "auth.test":
            payload |= {"user_id": "U0EXAMPLE9", "team_id": "T0EXAMPLE1", "bot_id": "B0EXAMPLE1"}
        encoded = json.dumps(payload).encode()
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(encoded)))
        handler.end_headers()
        handler.wfile.write(encoded)

    def posts(self) -> list[SlackPost]:
        with self._lock:
            calls = list(self.calls)
        return [
            SlackPost(
                call["method"],
                call["channel"],
                call["text"],
                call["thread_ts"],
                str(call["ts"]),
            )
            for call in calls
            if call["method"] in {"chat.postMessage", "chat.update"}
        ]

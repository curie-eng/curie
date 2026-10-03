"""Operator fact access through the source CLI, real API, and private storage.

Both CLI tiers use the same real local API. The forwarding proxy changes a
stored fact between the CLI read and delete without replacing any API response.
"""

from __future__ import annotations

import http.client
import json
import pathlib
import subprocess
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlsplit

import jsonschema
import pytest

from cli.tests.local.test_api_key_help_redaction import (
    REPO,
    SENTINEL,
    Stack,
    _run,
)
from cli.tests.local.test_api_key_help_redaction import source_artifacts as source_artifacts
from cli.tests.local.test_api_key_help_redaction import stack as stack

FACT = "fact-0123456789abcdef0123456789abcdef"
OTHER_AGENT_FACT = "fact-11111111111111111111111111111111"
OTHER_CHANNEL_FACT = "fact-22222222222222222222222222222222"
MISSING_FACT = "fact-ffffffffffffffffffffffffffffffff"
SLACK = ("slack", "C0EXAMPLE1")
WEBHOOK = ("webhook", "inbox=operator@example.com?view=all#retained%25")
GUIDANCE = "Remember only reported preferences."


def _http(
    api: Stack, path: str, method: str = "GET", payload: Any = None
) -> tuple[int, Any]:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        api.api_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "X-API-Key": SENTINEL},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    return status, json.loads(raw) if raw else None


def _need_http(
    api: Stack, path: str, method: str = "GET", payload: Any = None, *, status: int = 200
) -> Any:
    actual, body = _http(api, path, method, payload)
    if actual != status:
        raise RuntimeError(f"setting up {method} {path}: expected {status}, got {actual}: {body}")
    return body


def _state_path(agent_id: str, key: str | None = None, channel: tuple[str, str] | None = None):
    segments = ["agents", agent_id, "state"]
    if channel is not None:
        segments.extend(["bindings", *channel])
    segments.append("memory")
    if key is not None:
        segments.append(key)
    return "/" + "/".join(quote(segment, safe="") for segment in segments)


def _fact(statement: str, author: str, day: str) -> dict[str, str]:
    # The canonical producer is runner/src/curie_runner/memory_facts.py::_fact_value.
    return {
        "statement": statement,
        "author": author,
        "stated_at": f"2026-10-{day}T03:04:05.123456+00:00",
        "session_id": "session-example-1",
    }


@dataclass
class MemoryCase:
    api: Stack
    agent_id: str
    empty_agent: str
    malformed_agent: str
    paths: dict[str, str]
    expected_facts: list[dict[str, Any]]
    default_guidance: str
    guidance_file: pathlib.Path
    schema: dict[str, Any]


@pytest.fixture
def memory_case(stack: Stack, tmp_path: pathlib.Path) -> MemoryCase:
    """Seed only through the existing HTTP routes after stack fixture startup."""

    agent = _need_http(
        stack,
        "/agents",
        "POST",
        {"name": "acme-bot", "channel": {"kind": SLACK[0], "address": SLACK[1]}},
        status=201,
    )
    aid = agent["id"]
    # Distinct configured adapters share one memory pair. These endpoints are
    # stored route metadata; this test never delivers a message to an adapter.
    for adapter in ("alpha", "beta"):
        _need_http(
            stack,
            f"/agents/{aid}/channels",
            "POST",
            {
                "kind": WEBHOOK[0],
                "address": WEBHOOK[1],
                "adapter": adapter,
                "endpoint": "https://adapter.example.com/reply",
            },
            status=201,
        )
    _need_http(stack, f"/agents/{aid}", "PATCH", {"memory_writes": False})
    default_guidance = _need_http(stack, f"/agents/{aid}/memory/guidance")["text"]
    _need_http(stack, f"/agents/{aid}/memory/guidance", "PUT", {"text": GUIDANCE})
    _need_http(
        stack,
        f"/agents/{aid}/memory",
        "POST",
        {"content": "operator supplied legacy note"},
        status=201,
    )
    paths = {
        "agent": _state_path(aid, FACT),
        "other_agent": _state_path(aid, OTHER_AGENT_FACT),
        "slack": _state_path(aid, FACT, SLACK),
        "webhook": _state_path(aid, OTHER_CHANNEL_FACT, WEBHOOK),
        "log": _state_path(aid, "log"),
        "guidance": _state_path(aid, "guidance"),
        "other": _state_path(aid, "other"),
        "noncanonical": _state_path(aid, "fact-not-a-canonical-id"),
    }
    seeds = [
        ("agent", FACT, None, _fact("agent prefers concise replies", "operator@example.com", "01")),
        (
            "other_agent",
            OTHER_AGENT_FACT,
            None,
            _fact("agent uses metric units", "U0EXAMPLE1", "02"),
        ),
        ("slack", FACT, SLACK, _fact("channel prefers morning updates", "U0EXAMPLE2", "03")),
        (
            "webhook",
            OTHER_CHANNEL_FACT,
            WEBHOOK,
            _fact("webhook prefers complete timestamps", "reader@example.com", "04"),
        ),
    ]
    expected_facts = []
    for name, fact_id, channel, value in seeds:
        _need_http(stack, paths[name], "PUT", {"value": value})
        expected_facts.append(
            {
                "id": fact_id,
                "scope": "agent" if channel is None else "channel",
                "channel": None if channel is None else {"kind": channel[0], "address": channel[1]},
                "statement": value["statement"],
                "author": value["author"],
                "stated_at": value["stated_at"],
            }
        )
    for name in ("other", "noncanonical"):
        _need_http(stack, paths[name], "PUT", {"value": {"statement": "not a canonical fact"}})
    for name in ("acme-empty", "acme-malformed"):
        created = _need_http(
            stack,
            "/agents",
            "POST",
            {"name": name, "channel": {"kind": "webhook", "address": f"{name}@example.com"}},
            status=201,
        )
        if name == "acme-malformed":
            _need_http(
                stack,
                _state_path(created["id"], FACT),
                "PUT",
                {"value": {"statement": "missing stored provenance"}},
            )
    guidance_file = tmp_path / "guidance.md"
    guidance_file.write_text("Save explicit preferences and their attribution.")
    schema = json.loads((REPO / "cli/schema/memory.schema.json").read_text())
    return MemoryCase(
        stack, aid, "acme-empty", "acme-malformed", paths, expected_facts,
        default_guidance, guidance_file, schema,
    )


def _cli(
    case: MemoryCase, tier: str, *flags: str, agent: str = "acme-bot", api_url: str | None = None
) -> subprocess.CompletedProcess[str]:
    env = dict(case.api.env)
    for name in ("CURIE_API_KEY", "CURIE_API_URL"):
        env.pop(name, None)
    return _run(
        [
            str(case.api.binary), tier, "memory", agent,
            "--api-url", api_url or case.api.api_url, "--api-key", SENTINEL, *flags,
        ],
        env=env,
        timeout=30,
    )


def _json_cli(case: MemoryCase, tier: str, *flags: str, **kwargs) -> dict[str, Any]:
    result = _cli(case, tier, *flags, "--json", **kwargs)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    output = json.loads(result.stdout)
    jsonschema.Draft202012Validator(case.schema).validate(output)
    return output


def _refused(case: MemoryCase, tier: str, *flags: str, evidence: str, **kwargs) -> None:
    result = _cli(case, tier, *flags, **kwargs)
    assert result.returncode != 0, f"unexpected success: {result.stdout}"
    text = result.stdout + result.stderr
    assert evidence.lower() in text.lower(), text
    assert '"deleted":true' not in text.replace(" ", ""), text


def _snapshot(case: MemoryCase) -> dict[str, tuple[int, Any]]:
    return {name: _http(case.api, path) for name, path in case.paths.items()}


@dataclass
class DeleteAudit:
    read_versions: list[int] = field(default_factory=list)
    delete_queries: list[dict[str, list[str]]] = field(default_factory=list)
    updated_version: int | None = None
    errors: list[str] = field(default_factory=list)


@contextmanager
def _stale_delete_proxy(
    case: MemoryCase, path: str, updated_value: dict[str, str]
) -> Iterator[tuple[str, DeleteAudit]]:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    audit = DeleteAudit()
    upstream = urlsplit(case.api.api_url)

    class Forwarder(BaseHTTPRequestHandler):
        def _forward(self) -> None:
            target = urlsplit(self.path)
            selected = unquote(target.path) == unquote(path)
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=20)
            try:
                if selected and self.command == "DELETE":
                    audit.delete_queries.append(parse_qs(target.query))
                    if not audit.read_versions or len(audit.delete_queries) != 1:
                        raise RuntimeError("expected one selected fact read followed by one delete")
                    update = _need_http(
                        case.api,
                        path,
                        "PUT",
                        {"value": updated_value, "expected_version": audit.read_versions[-1]},
                    )
                    audit.updated_version = update["version"]
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() not in {"host", "connection"}
                }
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                raw = response.read()
                if selected and self.command == "GET" and response.status == 200:
                    audit.read_versions.append(json.loads(raw)["version"])
                self.send_response_only(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in {"transfer-encoding", "connection", "content-length"}:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except Exception as error:
                audit.errors.append(str(error))
                self.close_connection = True
            finally:
                connection.close()

        do_GET = _forward
        do_POST = _forward
        do_PUT = _forward
        do_PATCH = _forward
        do_DELETE = _forward

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Forwarder)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", audit
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("the forwarding proxy did not stop")


@pytest.mark.parametrize("tier", ["local", "cluster"])
def test_operator_memory_facts(memory_case: MemoryCase, tier: str) -> None:
    case = memory_case
    initial = _json_cli(case, tier)
    assert set(initial) == {"agent", "entries", "facts"}, initial
    assert initial["agent"] == "acme-bot"
    assert initial["entries"] == [{"index": 0, "content": "operator supplied legacy note"}]
    assert len(initial["facts"]) == len(case.expected_facts), initial
    for expected in case.expected_facts:
        assert initial["facts"].count(expected) == 1, initial
    assert case.schema["$id"] == "https://schemas.curietech.ai/cli/memory/v2.2.json"
    assert _need_http(case.api, f"/agents/{case.agent_id}")["memory_writes"] is False

    human = _cli(case, tier)
    assert human.returncode == 0, human.stderr
    for fact in case.expected_facts:
        for value in (fact["id"], fact["statement"], fact["author"], fact["stated_at"][:10]):
            assert value in human.stdout, human.stdout
    assert "(agent)" in human.stdout, human.stdout
    assert SLACK[1] in human.stdout and WEBHOOK[1] in human.stdout
    assert "not a canonical fact" not in human.stdout

    for channel in (SLACK, WEBHOOK):
        scoped = _json_cli(case, tier, "--channel", "=".join(channel))
        assert scoped == {
            "agent": "acme-bot", "entries": [],
            "facts": [fact for fact in case.expected_facts if fact["channel"] == {
                "kind": channel[0], "address": channel[1],
            }],
        }
    assert _json_cli(case, tier, agent=case.empty_agent) == {
        "agent": case.empty_agent, "entries": [], "facts": [],
    }

    before = _snapshot(case)
    for invalid in ("log", "guidance", "fact-short", FACT.upper(), FACT + "/log", FACT + "\n"):
        _refused(case, tier, "--delete", invalid, evidence="fact")
        assert _snapshot(case) == before
    for invalid in ("slack", "=C0EXAMPLE1", "slack="):
        _refused(case, tier, "--channel", invalid, evidence="channel")
        assert _snapshot(case) == before
    _refused(case, tier, "--channel", "slack=C0EXAMPLE9", evidence="bound")
    _refused(case, tier, "--delete", FACT, "--channel", "slack=C0EXAMPLE9", evidence="bound")
    _refused(case, tier, "--delete", MISSING_FACT, evidence="404")
    _refused(case, tier, "--delete", MISSING_FACT, "--channel", "=".join(SLACK), evidence="404")
    assert _snapshot(case) == before
    for action in (
        ("--add", "new note"), ("--guidance",),
        ("--guidance-from", str(case.guidance_file)), ("--reset-guidance",),
    ):
        for selection in (("--delete", FACT), ("--channel", "=".join(SLACK))):
            for first, second in ((action, selection), (selection, action)):
                _refused(case, tier, *first, *second, evidence="cannot be used")
                assert _snapshot(case) == before

    added = _json_cli(case, tier, "--add", "second operator note")
    assert added == {
        "agent": "acme-bot", "index": 1, "content": "second operator note", "source": "operator",
        "fresh_session_required": True, "next_command": f'curie {tier} message "..."',
    }
    entries = _need_http(case.api, f"/agents/{case.agent_id}/memory")
    assert entries[1]["provenance"]["source"] == "operator"
    later = _json_cli(case, tier)
    assert later["entries"] == initial["entries"] + [
        {"index": 1, "content": "second operator note"}
    ]
    assert later["facts"] == initial["facts"]
    assert _json_cli(case, tier, "--guidance") == {
        "agent": "acme-bot", "text": GUIDANCE, "source": "operator", "changed": False,
    }
    changed = _json_cli(case, tier, "--guidance-from", str(case.guidance_file))
    assert changed == {
        "agent": "acme-bot", "text": case.guidance_file.read_text(),
        "source": "operator", "changed": True,
    }
    assert _json_cli(case, tier, "--guidance") == dict(changed, changed=False)
    assert _json_cli(case, tier, "--reset-guidance") == {
        "agent": "acme-bot", "text": case.default_guidance, "source": "default", "changed": True,
    }
    assert _http(case.api, case.paths["guidance"])[0] == 404

    deleted_human = _cli(case, tier, "--delete", OTHER_AGENT_FACT)
    assert deleted_human.returncode == 0, deleted_human.stderr
    assert OTHER_AGENT_FACT in deleted_human.stdout and "deleted" in deleted_human.stdout.lower()
    assert _http(case.api, case.paths["other_agent"])[0] == 404
    assert _http(case.api, case.paths["agent"])[0] == 200
    assert _json_cli(case, tier, "--delete", FACT) == {
        "agent": "acme-bot", "id": FACT, "scope": "agent", "channel": None, "deleted": True,
    }
    assert _http(case.api, case.paths["agent"])[0] == 404
    channel_row = _need_http(case.api, case.paths["slack"])
    assert channel_row["value"]["statement"] == "channel prefers morning updates"
    assert _json_cli(case, tier, "--delete", FACT, "--channel", "=".join(SLACK)) == {
        "agent": "acme-bot", "id": FACT, "scope": "channel",
        "channel": {"kind": SLACK[0], "address": SLACK[1]}, "deleted": True,
    }
    assert _http(case.api, case.paths["slack"])[0] == 404
    original = _need_http(case.api, case.paths["webhook"])
    updated_value = _fact("concurrent update survives deletion", "reader@example.com", "05")
    with _stale_delete_proxy(case, case.paths["webhook"], updated_value) as (proxy, audit):
        conflicted = _cli(
            case, tier, "--delete", OTHER_CHANNEL_FACT, "--channel", "=".join(WEBHOOK),
            api_url=proxy,
        )
    assert not audit.errors, audit.errors
    assert audit.read_versions == [original["version"]]
    assert audit.delete_queries == [{"expected_version": [str(original["version"])]}]
    assert audit.updated_version == original["version"] + 1
    assert conflicted.returncode != 0, f"unexpected success: {conflicted.stdout}"
    conflict_text = conflicted.stdout + conflicted.stderr
    assert "409" in conflict_text, conflict_text
    assert '"deleted":true' not in conflict_text.replace(" ", ""), conflict_text
    retained = _need_http(case.api, case.paths["webhook"])
    assert retained["version"] == original["version"] + 1 == audit.updated_version
    assert retained["value"] == updated_value
    deleted_channel = _json_cli(
        case, tier, "--delete", OTHER_CHANNEL_FACT, "--channel", "=".join(WEBHOOK)
    )
    assert deleted_channel == {
        "agent": "acme-bot", "id": OTHER_CHANNEL_FACT, "scope": "channel",
        "channel": {"kind": WEBHOOK[0], "address": WEBHOOK[1]}, "deleted": True,
    }
    assert _http(case.api, case.paths["webhook"])[0] == 404
    remaining = _json_cli(case, tier)
    assert remaining["facts"] == [] and remaining["entries"] == later["entries"]
    _refused(case, tier, agent=case.malformed_agent, evidence=FACT)

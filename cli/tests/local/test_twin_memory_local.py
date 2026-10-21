"""Twin memory (ADR 0239) through the source CLI and real candidate services.

Deterministic cases stand up a private Compose API on real Postgres and drive
the real `curie` binary at both tiers: typed fact metadata, archive, restore,
revert, history, dry runs and stale-version refusal. They call no model.

`test_live_cross_channel_instruction_index_and_revert` is the live acceptance
run. It is skipped unless CURIE_E2E_LIVE=1, and then requires an OpenRouter
credential in CURIE_CREDENTIALS. It builds a private candidate stack with
`curie local up --build`, deploys one synthetic agent bound to two synthetic
Slack-kind channels whose replies return through the CLI's own HTTP reply stub
(no Slack workspace), and proves learning, correction, forgetting, the bounded
title index, archive and restore, and operator revert from fresh threads,
checking replies, API rows, transcripts and the runner's boot injection record.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.client
import json
import os
import pathlib
import re
import secrets
import shutil
import socket
import subprocess
import threading
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import jsonschema
import pytest

from cli.tests.local.test_api_key_help_redaction import (
    REPO,
    SENTINEL,
    Stack,
    _require,
    _run,
)
from cli.tests.local.test_api_key_help_redaction import source_artifacts as source_artifacts
from cli.tests.local.test_api_key_help_redaction import stack as stack

FACT = "fact-0123456789abcdef0123456789abcdef"
INSTRUCTION = "fact-fedcba9876543210fedcba9876543210"
SLACK = ("slack", "C0EXAMPLE1")
CHANNEL_FLAG = "=".join(SLACK)
# The pre-change DEFAULT_MEMORY_GUIDANCE bytes, recorded by the plan before
# implementation. A non-opt-in agent must still receive exactly these bytes.
DEFAULT_GUIDANCE_SHA256 = "ac65ffde36f1252cd369d9d5c6226ca006782cf045c3cf758a5704fbf9338e2f"
DEFAULT_GUIDANCE_BYTES = 1412
LIVE_MODEL = "anthropic/claude-haiku-5.5"
LIVE_BUDGET_USD = 4.5


# --- shared HTTP helpers ------------------------------------------------------


def _api(
    base: str, key: str, path: str, method: str = "GET", payload: Any = None
) -> tuple[int, Any]:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "X-API-Key": key},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=30) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    return status, json.loads(raw) if raw else None


def _need(base: str, key: str, path: str, method: str = "GET", payload: Any = None,
          *, status: int = 200) -> Any:
    actual, body = _api(base, key, path, method, payload)
    if actual != status:
        raise RuntimeError(f"{method} {path}: expected {status}, got {actual}: {body}")
    return body


def _state_path(agent_id: str, key: str | None = None,
                channel: tuple[str, str] | None = None) -> str:
    segments = ["agents", agent_id, "state"]
    if channel is not None:
        segments.extend(["bindings", *channel])
    segments.append("memory")
    if key is not None:
        segments.append(key)
    return "/" + "/".join(quote(segment, safe="") for segment in segments)


def _scope_query(channel: tuple[str, str] | None) -> dict[str, str]:
    if channel is None:
        return {"scope": "agent"}
    return {"scope": "channel", "channel_kind": channel[0], "channel_address": channel[1]}


def _detail_path(agent_id: str, fact_id: str, channel: tuple[str, str] | None) -> str:
    return (
        f"/agents/{agent_id}/state/_memory/entries/{fact_id}?"
        + urlencode(_scope_query(channel))
    )


def _value(statement: str, *, kind: str = "fact", title: str | None = None,
           author: str = "U0EXAMPLE1", archived: bool = False) -> dict[str, Any]:
    return {
        "kind": kind,
        "title": title or statement[:80],
        "statement": statement,
        "author": author,
        "stated_at": "2026-10-01T03:04:05.123456+00:00",
        "session_id": "session-example-1",
        "conversation": None,
        "archived": archived,
    }


def _schema() -> dict[str, Any]:
    return json.loads((REPO / "cli/schema/memory.schema.json").read_text())


# --- deterministic local cases (no model) -------------------------------------


@dataclass
class TwinCase:
    api: Stack
    agent_id: str
    v1: dict[str, Any]
    v2: dict[str, Any]


def _sentinel(stack: Stack, path: str, method: str = "GET", payload: Any = None,
              *, status: int = 200) -> Any:
    return _need(stack.api_url, SENTINEL, path, method, payload, status=status)


@pytest.fixture
def twin_case(stack: Stack) -> TwinCase:
    agent = _sentinel(
        stack, "/agents", "POST",
        {"name": "acme-twin", "channel": {"kind": SLACK[0], "address": SLACK[1]}},
        status=201,
    )
    aid = agent["id"]
    assert agent["agent_wide_learning"] is False, agent
    v1 = _value("the release train leaves on Tuesdays", title="Release day")
    v2 = _value("the release train leaves on Thursdays", title="Release day")
    for channel in (None, SLACK):
        created = _sentinel(stack, _state_path(aid, FACT, channel), "PUT", {"value": v1})
        assert created["version"] == 1, created
        updated = _sentinel(
            stack, _state_path(aid, FACT, channel), "PUT",
            {"value": v2, "expected_version": 1},
        )
        assert updated["version"] == 2, updated
        instruction = _value(
            "close every reply with the word Acknowledged", kind="instruction",
            title="Closing word",
        )
        assert _sentinel(
            stack, _state_path(aid, INSTRUCTION, channel), "PUT", {"value": instruction}
        )["version"] == 1
    return TwinCase(stack, aid, v1, v2)


def _cli(case: TwinCase, tier: str, *flags: str, agent: str = "acme-twin",
         api_url: str | None = None) -> subprocess.CompletedProcess[str]:
    env = dict(case.api.env)
    for name in ("CURIE_API_KEY", "CURIE_API_URL"):
        env.pop(name, None)
    env["NO_COLOR"] = "1"
    return _run(
        [str(case.api.binary), tier, "memory", agent,
         "--api-url", api_url or case.api.api_url, "--api-key", SENTINEL, *flags],
        env=env, timeout=60,
    )


def _json(case: TwinCase, tier: str, *flags: str, **kwargs: Any) -> dict[str, Any]:
    result = _cli(case, tier, *flags, "--json", **kwargs)
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    output = json.loads(result.stdout)
    jsonschema.Draft202012Validator(_schema()).validate(output)
    return output


def _detail(case: TwinCase, fact_id: str, channel: tuple[str, str] | None) -> dict[str, Any]:
    return _sentinel(case.api, _detail_path(case.agent_id, fact_id, channel))


def _snapshot(case: TwinCase) -> list[Any]:
    rows = []
    for channel in (None, SLACK):
        for fact_id in (FACT, INSTRUCTION):
            for path in (
                _state_path(case.agent_id, fact_id, channel),
                _detail_path(case.agent_id, fact_id, channel),
            ):
                rows.append(_api(case.api.api_url, SENTINEL, path))
    return rows


def _channel_flags(channel: tuple[str, str] | None) -> tuple[str, ...]:
    return () if channel is None else ("--channel", "=".join(channel))


def _listed(output: dict[str, Any], fact_id: str, channel: tuple[str, str] | None):
    scope = "agent" if channel is None else "channel"
    matches = [f for f in output["facts"] if f["id"] == fact_id and f["scope"] == scope]
    assert len(matches) <= 1, output
    return matches[0] if matches else None


@pytest.mark.parametrize("channel", [None, SLACK], ids=["agent", "channel"])
@pytest.mark.parametrize("tier", ["local", "cluster"])
def test_cli_archive_restore_revert_and_history_through_the_real_api(
    twin_case: TwinCase, tier: str, channel: tuple[str, str] | None
) -> None:
    case = twin_case
    scoped = _channel_flags(channel)
    history = _json(case, tier, "--history", FACT, *scoped)
    assert history["deleted"] is False and history["version"] == 2, history
    assert [r["version"] for r in history["history"]] == [2, 1], history
    assert [r["operation"] for r in history["history"]] == ["update", "create"], history
    assert history["history"][1]["snapshot"]["statement"] == case.v1["statement"]
    assert history["entry"] == _detail(case, FACT, channel)["entry"], history

    archived = _json(case, tier, "--archive", FACT, *scoped)
    assert archived["archived"] is True and archived["version"] == 3, archived
    assert archived["kind"] == "fact" and archived["id"] == FACT, archived
    row = _sentinel(case.api, _state_path(case.agent_id, FACT, channel))
    assert row["version"] == 3 and row["value"]["archived"] is True, row
    listed = _listed(_json(case, tier, *scoped), FACT, channel)
    assert listed is not None and listed["archived"] is True and listed["version"] == 3

    restored = _json(case, tier, "--restore", FACT, *scoped)
    assert restored["archived"] is False and restored["version"] == 4, restored
    detail = _detail(case, FACT, channel)
    assert [r["operation"] for r in detail["history"]][:2] == ["restore", "archive"], detail

    reverted = _json(case, tier, "--revert", FACT, "--to-version", "1", *scoped)
    assert reverted["deleted"] is False and reverted["version"] == 5, reverted
    assert reverted["entry"]["statement"] == case.v1["statement"], reverted
    assert reverted["history"][0]["operation"] == "revert", reverted
    assert reverted["history"][0]["reverted_to_version"] == 1, reverted
    assert reverted["history"][0]["source"] == "operator", reverted
    assert _sentinel(case.api, _state_path(case.agent_id, FACT, channel))["value"][
        "statement"
    ] == case.v1["statement"]

    removed = _json(case, tier, "--revert", INSTRUCTION, "--to-version", "0", *scoped)
    assert removed["deleted"] is True and removed["entry"] is None, removed
    assert removed["version"] == 2, removed
    assert _api(case.api.api_url, SENTINEL, _state_path(case.agent_id, INSTRUCTION, channel))[
        0
    ] == 404
    assert _listed(_json(case, tier, *scoped), INSTRUCTION, channel) is None
    after = _json(case, tier, "--history", INSTRUCTION, *scoped)
    assert after["deleted"] is True and after["version"] == 2, after
    assert [r["operation"] for r in after["history"]] == ["revert", "create"], after

    # Reverting the deletion restores the same identity under a new version.
    revived = _json(case, tier, "--revert", INSTRUCTION, "--to-version", "1", *scoped)
    assert revived["deleted"] is False and revived["version"] == 3, revived
    assert revived["entry"]["kind"] == "instruction", revived
    # The other scope was never touched.
    other = SLACK if channel is None else None
    assert _detail(case, FACT, other)["version"] == 2


@pytest.mark.parametrize("tier", ["local", "cluster"])
def test_cli_lifecycle_dry_runs_write_nothing_through_the_real_api(
    twin_case: TwinCase, tier: str
) -> None:
    case = twin_case
    before = _snapshot(case)
    for flags in (
        ("--archive", FACT), ("--restore", FACT), ("--history", FACT),
        ("--revert", FACT, "--to-version", "1"), ("--revert", FACT, "--to-version", "0"),
        ("--archive", FACT, "--channel", CHANNEL_FLAG),
        ("--revert", INSTRUCTION, "--to-version", "0", "--channel", CHANNEL_FLAG),
    ):
        plan = _json(case, tier, *flags, "--dry-run")
        assert plan["dry_run"] is True, plan
        assert _snapshot(case) == before, flags


@dataclass
class BumpAudit:
    mutations: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    bumped_to: int | None = None
    errors: list[str] = field(default_factory=list)


@contextlib.contextmanager
def _bumping_proxy(
    base: str, key: str, row_path: str, bump: Callable[[int], dict[str, Any]]
) -> Iterator[tuple[str, BumpAudit]]:
    """Forward to the real API; on the first mutating request, first move the
    selected head on by one version through the real API, so the CLI's write
    carries the version it read and is now stale. No response is invented."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    audit = BumpAudit()
    upstream = urlsplit(base)

    class Forwarder(BaseHTTPRequestHandler):
        def _forward(self) -> None:
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=30)
            try:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if self.command != "GET":
                    audit.mutations.append((self.path, json.loads(body or b"null") or {}))
                    if audit.bumped_to is None:
                        current = _need(base, key, row_path)
                        audit.bumped_to = _need(
                            base, key, row_path, "PUT",
                            {"value": bump(current["version"]),
                             "expected_version": current["version"]},
                        )["version"]
                headers = {
                    k: v for k, v in self.headers.items() if k.lower() not in {"host", "connection"}
                }
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                raw = response.read()
                self.send_response_only(response.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (OSError, http.client.HTTPException, RuntimeError, ValueError) as error:
                audit.errors.append(str(error))
                self.close_connection = True
            finally:
                connection.close()

        do_GET = _forward
        do_POST = _forward
        do_PUT = _forward
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
def test_cli_lifecycle_refuses_a_stale_version_through_the_real_api(
    twin_case: TwinCase, tier: str
) -> None:
    case = twin_case
    for flags, channel in (
        (("--archive", FACT), None),
        (("--revert", FACT, "--to-version", "1"), None),
        (("--archive", FACT, "--channel", CHANNEL_FLAG), SLACK),
        (("--delete", FACT, "--channel", CHANNEL_FLAG), SLACK),
    ):
        row_path = _state_path(case.agent_id, FACT, channel)
        original = _sentinel(case.api, row_path)
        history_before = _detail(case, FACT, channel)["history"]
        statement = f"concurrent edit {uuid.uuid4().hex[:8]}"
        with _bumping_proxy(
            case.api.api_url, SENTINEL, row_path,
            lambda _version, s=statement: _value(s, title="Release day"),
        ) as (proxy, audit):
            result = _cli(case, tier, *flags, api_url=proxy)
        assert not audit.errors, audit.errors
        assert audit.bumped_to == original["version"] + 1, audit
        assert len(audit.mutations) == 1, f"{flags}: never retried: {audit.mutations}"
        sent = audit.mutations[0]
        if flags[0] == "--delete":
            assert f"expected_version={original['version']}" in sent[0], sent
        else:
            assert sent[1]["expected_version"] == original["version"], sent
        text = result.stdout + result.stderr
        assert result.returncode != 0, f"{flags}: unexpected success: {text}"
        assert "stale" in text.lower() or "409" in text, text
        if flags[0] != "--delete":
            assert "stale" in text.lower(), f"{flags}: report the conflict as stale: {text}"
        retained = _sentinel(case.api, row_path)
        assert retained["version"] == audit.bumped_to, retained
        assert retained["value"]["statement"] == statement, retained
        assert retained["value"]["archived"] is False, retained
        history_after = _detail(case, FACT, channel)["history"]
        assert len(history_after) == min(10, len(history_before) + 1), history_after
        assert history_after[0]["operation"] == "update", history_after


@pytest.mark.parametrize("tier", ["local", "cluster"])
def test_local_memory_listing_shows_kind_and_archived(twin_case: TwinCase, tier: str) -> None:
    case = twin_case
    _sentinel(
        case.api, f"/agents/{case.agent_id}/state/_memory/archive", "POST",
        {"scope": "agent", "id": INSTRUCTION, "expected_version": 1},
    )
    listed = _json(case, tier)
    for fact in listed["facts"]:
        assert {"kind", "title", "archived", "conversation", "version"} <= set(fact), fact
    assert _listed(listed, FACT, None) == {
        "id": FACT, "scope": "agent", "channel": None, "kind": "fact",
        "title": "Release day", "statement": case.v2["statement"],
        "author": case.v2["author"], "stated_at": case.v2["stated_at"],
        "conversation": None, "archived": False, "version": 2,
    }
    instruction = _listed(listed, INSTRUCTION, None)
    assert instruction["kind"] == "instruction" and instruction["archived"] is True, instruction
    assert instruction["version"] == 2, instruction
    channel_instruction = _listed(listed, INSTRUCTION, SLACK)
    assert channel_instruction["archived"] is False, channel_instruction

    human = _cli(case, tier)
    assert human.returncode == 0, human.stderr
    lines = [line for line in human.stdout.splitlines() if INSTRUCTION in line]
    assert len(lines) == 2, human.stdout
    agent_line = next(line for line in lines if SLACK[1] not in line)
    channel_line = next(line for line in lines if SLACK[1] in line)
    for line in lines:
        assert "instruction" in line.lower() and "U0EXAMPLE1" in line, line
    assert "archived" in agent_line.lower(), agent_line
    assert "archived" not in channel_line.lower(), channel_line


@pytest.mark.parametrize("tier", ["local", "cluster"])
def test_guidance_follows_the_learning_toggle_and_keeps_the_default_bytes(
    twin_case: TwinCase, tier: str, tmp_path: pathlib.Path
) -> None:
    case = twin_case

    def overrides(*flags: str) -> dict[str, Any]:
        env = {k: v for k, v in case.api.env.items() if k not in {"CURIE_API_KEY", "CURIE_API_URL"}}
        result = _run(
            [str(case.api.binary), tier, "overrides", "acme-twin", *flags,
             "--api-url", case.api.api_url, "--api-key", SENTINEL, "--json"],
            env=env, timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    def assert_default(guidance: dict[str, Any]) -> None:
        raw = guidance["text"].encode()
        assert guidance["source"] == "default", guidance["source"]
        assert len(raw) == DEFAULT_GUIDANCE_BYTES, len(raw)
        assert hashlib.sha256(raw).hexdigest() == DEFAULT_GUIDANCE_SHA256

    inspected = overrides()
    assert inspected["agent_wide_learning"] is False and inspected["memory_writes"] is False
    assert_default(_json(case, tier, "--guidance"))
    context = _sentinel(case.api, f"/agents/{case.agent_id}/state/_memory/context")
    assert context["agent_wide_learning"] is False and context["guidance"]["source"] == "default"
    assert context["instructions"] == [], "instructions never load with learning off"

    enabled = overrides("--agent-wide-learning", "on")
    assert enabled["agent_wide_learning"] is True and enabled["memory_writes"] is False, enabled
    learning = _json(case, tier, "--guidance")
    assert learning["source"] == "learning_default", learning
    assert hashlib.sha256(learning["text"].encode()).hexdigest() != DEFAULT_GUIDANCE_SHA256
    context = _sentinel(case.api, f"/agents/{case.agent_id}/state/_memory/context")
    assert context["guidance"] == {"text": learning["text"], "source": "learning_default"}
    assert {i["id"] for i in context["instructions"]} == {INSTRUCTION}, context
    assert all("statement" not in row for row in context["index"]), context

    custom = tmp_path / "guidance.md"
    custom.write_text("Keep only what the operator approves.")
    assert _json(case, tier, "--guidance-from", str(custom))["source"] == "operator"
    reset = _json(case, tier, "--reset-guidance")
    assert reset["source"] == "learning_default" and reset["text"] == learning["text"], reset

    disabled = overrides("--agent-wide-learning", "off")
    assert disabled["agent_wide_learning"] is False, disabled
    assert_default(_json(case, tier, "--guidance"))


# --- live acceptance -------------------------------------------------------------


LIVE = os.environ.get("CURIE_E2E_LIVE") == "1"
AGENT = "twin-memory-agent"
CONTROL = "twin-control-agent"
CHANNEL_A = "C0EXAMPLE2"
CHANNEL_B = "C0EXAMPLE3"
CHANNEL_C = "C0EXAMPLE4"
RUNNER_FILTERS = ("label=curietech.ai/managed-by=curie-sandbox-substrate",)


def _kernel_picked_ports(count: int) -> list[int]:
    probes = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(count)]
    try:
        for probe in probes:
            probe.bind(("127.0.0.1", 0))
        return [probe.getsockname()[1] for probe in probes]
    finally:
        for probe in probes:
            probe.close()


def _source_binary(root: pathlib.Path) -> pathlib.Path:
    supplied = os.environ.get("CURIE_BIN")
    if supplied:
        binary = pathlib.Path(supplied).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not an executable file: {binary}")
        return binary
    _require(
        _run(["cargo", "build", "--locked"], cwd=REPO / "cli", timeout=1800),
        "building the source CLI",
    )
    metadata = _run(["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=REPO / "cli")
    cargo = json.loads(_require(metadata, "reading the Cargo target"))
    target = pathlib.Path(cargo["target_directory"])
    binary = root / "curie"
    shutil.copy2(target / "debug/curie", binary)
    binary.chmod(0o755)
    return binary


@dataclass
class LiveStack:
    binary: pathlib.Path
    project: str
    env: dict[str, str]
    api_url: str
    api_key: str
    workdir: pathlib.Path
    secrets: tuple[str, ...]
    seen_log_lines: set[str] = field(default_factory=set)

    def scrub(self, text: str) -> str:
        for value in self.secrets:
            if value:
                text = text.replace(value, "<redacted>")
        return text

    def api(self, path: str, method: str = "GET", payload: Any = None, *, status: int = 200):
        actual, body = _api(self.api_url, self.api_key, path, method, payload)
        if actual != status:
            raise AssertionError(self.scrub(f"{method} {path}: {actual} {body}"))
        return body

    def cli(
        self,
        *argv: str,
        timeout: int = 300,
        check: bool = True,
        cwd: pathlib.Path | None = None,
    ):
        # `local up --build` must run from this checkout: it builds its images
        # from the compose.dev.yaml beside the working directory.
        result = _run(
            [str(self.binary), *argv], env=self.env, cwd=cwd or self.workdir, timeout=timeout
        )
        if check and result.returncode:
            raise AssertionError(
                self.scrub(f"curie {' '.join(argv[:4])} failed: {result.stdout}\n{result.stderr}")
            )
        return result

    def json(self, *argv: str, timeout: int = 300) -> Any:
        return json.loads(self.cli("--json", *argv, timeout=timeout).stdout)


def _write_live_override(path: pathlib.Path, ports: dict[str, int]) -> None:
    path.write_text(
        f"""services:
  postgres:
    ports: !override ["127.0.0.1:{ports["postgres"]}:5432"]
  valkey:
    ports: !override ["127.0.0.1:{ports["valkey"]}:6379"]
  rustfs:
    ports: !override ["127.0.0.1:{ports["rustfs"]}:9000", "127.0.0.1::9001"]
  curie-api:
    ports: !override ["127.0.0.1:{ports["curie-api"]}:8000"]
  curie-worker:
    environment:
      DATABASE_URL: "postgresql+asyncpg://postgres:${{CURIE_LOCAL_POSTGRES_PASSWORD:-postgres}}@${{CURIE_LOCAL_POSTGRES_HOST}}:${{CURIE_LOCAL_POSTGRES_PORT}}/postgres"
      VALKEY_HOST: "${{VALKEY_HOST}}"
      VALKEY_PORT: "${{VALKEY_PORT}}"
      S3_ENDPOINT_URL: "${{S3_ENDPOINT_URL}}"
      CURIE_API_URL: "${{CURIE_API_URL}}"
      CURIE_DOCKER_NETWORK: "${{CURIE_DOCKER_NETWORK}}"
      SLACK_API_BASE_URL: "http://${{CURIE_LOCAL_STUB_HOST:-127.0.0.1}}:${{CURIE_LOCAL_STUB_PORT}}/api/"
      OTEL_EXPORTER_OTLP_ENDPOINT: ""
networks:
  curie_runner:
    name: "${{COMPOSE_PROJECT_NAME}}_runner"
"""
    )


def _owned_images(project: str, tag: str) -> list[str]:
    return [
        f"ghcr.io/curie-eng/curie-{component}:{tag}"
        for component in ("api", "worker", "dispatcher", "runner", "ui")
    ] + [f"{project}-curie-worker:latest"]


@contextlib.contextmanager
def _live_stack(tmp_path: pathlib.Path) -> Iterator[LiveStack]:
    credential = os.environ.get("CURIE_CREDENTIALS", "")
    if not credential.startswith("sk-or-"):
        pytest.fail(
            "CURIE_E2E_LIVE=1 requires CURIE_CREDENTIALS to hold an OpenRouter key "
            "(prefix sk-or-); none was found",
            pytrace=False,
        )
    binary = _source_binary(tmp_path)
    suffix = uuid.uuid4().hex[:10]
    project = f"curie-twin-{suffix}"
    tag = f"twin-{suffix}"
    names = ("postgres", "valkey", "rustfs", "curie-api", "stub")
    ports = dict(zip(names, _kernel_picked_ports(len(names)), strict=True))
    override = tmp_path / "compose.override.yaml"
    _write_live_override(override, ports)
    base = REPO / "compose.dev.yaml"
    config_dir = tmp_path / "config"
    (config_dir / "local").mkdir(parents=True, mode=0o700)
    workdir = tmp_path / "work"
    workdir.mkdir()
    api_key = secrets.token_hex(32)
    api_url = f"http://127.0.0.1:{ports['curie-api']}"
    env = os.environ.copy()
    for name in (
        "CURIE_API_KEY", "CURIE_API_URL", "CURIE_LOCAL_API_KEY", "CURIE_LOCAL_POSTGRES_PASSWORD",
        "COMPOSE_FILE", "COMPOSE_PROJECT_NAME", "DOCKER_CONTEXT", "HTTP_PROXY", "HTTPS_PROXY",
        "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "CURIE_FAKE_MODEL",
    ):
        env.pop(name, None)
    env.update({
        "COMPOSE_PROJECT_NAME": project,
        "COMPOSE_FILE": f"{base}:{override}",
        "CURIE_CONFIG_DIR": str(config_dir),
        "CURIE_API_KEY": api_key,
        "CURIE_API_URL": api_url,
        "VALKEY_HOST": "127.0.0.1",
        "VALKEY_PORT": str(ports["valkey"]),
        "S3_ENDPOINT_URL": f"http://127.0.0.1:{ports['rustfs']}",
        "CURIE_DOCKER_NETWORK": f"{project}_runner",
        "CURIE_LOCAL_STUB_PORT": str(ports["stub"]),
        "CURIE_LOCAL_POSTGRES_HOST": "127.0.0.1",
        "CURIE_LOCAL_POSTGRES_PORT": str(ports["postgres"]),
        "CURIE_LOCAL_IMAGE_TAG": tag,
        "NO_COLOR": "1",
    })
    files = ["-f", str(base), "-f", str(override)]
    live = LiveStack(binary, project, env, api_url, api_key, workdir, (credential, api_key))
    teardown_errors: list[str] = []
    try:
        # Teardown is registered (this finally) before anything starts.
        info = _run(["docker", "info", "--format", "{{.ServerVersion}}"], env=env)
        _require(info, "reaching Docker")
        for image in _owned_images(project, tag):
            if _run(["docker", "image", "inspect", image], env=env).returncode == 0:
                raise RuntimeError(f"owned image tag already exists: {image}")
        up = live.cli(
            "--json", "local", "up", "--build", "--minimal", "--model", LIVE_MODEL,
            "--project", project, *files, timeout=3600, check=False, cwd=REPO,
        )
        if up.returncode:
            raise RuntimeError(live.scrub(f"local up failed: {up.stdout}\n{up.stderr}"))
        live.api("/agents")
        yield live
    finally:
        try:
            down = _run(
                [str(binary), "--json", "local", "down", "--project", project, *files,
                 "--wipe", "--yes"],
                env=env, cwd=workdir, timeout=600,
            )
            if down.returncode:
                teardown_errors.append(live.scrub(f"curie local down failed: {down.stderr}"))
        except (OSError, subprocess.TimeoutExpired) as error:
            teardown_errors.append(f"curie local down failed: {error}")
        stack_filter = ("--filter", f"label=curietech.ai/stack={project}_runner")
        filters = [arg for f in RUNNER_FILTERS for arg in ("--filter", f)] + list(stack_filter)
        runners = _run(["docker", "ps", "-aq", *filters], env=env).stdout.split()
        if runners:
            _run(["docker", "rm", "-f", *runners], env=env)
        volumes = _run(["docker", "volume", "ls", "-q", *filters], env=env).stdout.split()
        if volumes:
            _run(["docker", "volume", "rm", "-f", *volumes], env=env)
        _run(
            ["docker", "compose", "-p", project, *files, "--profile", "core", "down", "-v",
             "--remove-orphans"],
            env=env, timeout=300,
        )
        owned = [
            image for image in _owned_images(project, tag)
            if _run(["docker", "image", "inspect", image], env=env).returncode == 0
        ]
        if owned:
            removed = _run(["docker", "image", "rm", "-f", *owned], env=env)
            if removed.returncode:
                teardown_errors.append(f"owned image cleanup failed: {removed.stderr}")
        label = f"label=com.docker.compose.project={project}"
        leftovers = {
            "containers": _run(["docker", "ps", "-aq", "--filter", label], env=env).stdout.split(),
            "volumes": _run(
                ["docker", "volume", "ls", "-q", "--filter", label], env=env
            ).stdout.split(),
            "networks": _run(
                ["docker", "network", "ls", "-q", "--filter", label], env=env
            ).stdout.split(),
            "runners": _run(["docker", "ps", "-aq", *filters], env=env).stdout.split(),
            "images": [
                image for image in _owned_images(project, tag)
                if _run(["docker", "image", "inspect", image], env=env).returncode == 0
            ],
        }
        if any(leftovers.values()):
            teardown_errors.append(f"private project {project} left resources: {leftovers}")
        if teardown_errors:
            raise RuntimeError("; ".join(teardown_errors))


class Budget:
    """OpenRouter's own usage counter for the key in CURIE_CREDENTIALS."""

    def __init__(self, credential: str) -> None:
        self._credential = credential
        self.start = self._usage()

    def _usage(self) -> float:
        request = urllib.request.Request(
            "https://openrouter.ai/api/v1/key",
            headers={"Authorization": f"Bearer {self._credential}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return float(json.load(response)["data"]["usage"])
        except (OSError, KeyError, TypeError, ValueError) as error:
            pytest.fail(
                f"the OpenRouter usage counter is unavailable ({type(error).__name__}); "
                "the live run refuses to spend without a budget check",
                pytrace=False,
            )

    def check(self, group: str) -> float:
        spent = self._usage() - self.start
        assert spent < LIVE_BUDGET_USD, f"{group}: live spend {spent:.2f} USD reached the cap"
        print(f"live spend after {group}: {spent:.3f} USD")
        return spent


def _write_bundle(root: pathlib.Path, name: str) -> pathlib.Path:
    bundle = root / name
    (bundle / ".claude-plugin").mkdir(parents=True)
    (bundle / "skills" / "assistant").mkdir(parents=True)
    (bundle / ".claude-plugin" / "plugin.json").write_text(json.dumps({
        "name": name,
        "description": "A synthetic assistant for the twin memory acceptance run.",
        "version": "0.1.0",
    }, indent=2) + "\n")
    (bundle / "skills" / "assistant" / "SKILL.md").write_text(
        "---\n"
        "name: assistant\n"
        "description: Answer short questions from the people this agent serves. Invoke for "
        "every message.\n"
        "---\n\n"
        "# Assistant\n\n"
        "Answer in at most three short sentences. Use your memory tools when a person asks "
        "you to remember, correct or forget something, and look memory up before saying you "
        "do not know.\n"
    )
    return bundle


def _turn(live: LiveStack, text: str, channel: str, agent: str = AGENT) -> tuple[str, str]:
    """One fresh-thread turn (no --thread): returns (reply, thread)."""
    out = live.json(
        "local", "message", text, "--agent", agent, "--channel", channel,
        "--timeout-secs", "240", timeout=320,
    )
    assert out.get("finalized") is True, live.scrub(f"turn did not finalize: {out}")
    reply = out.get("reply") or ""
    assert reply, live.scrub(f"empty reply: {out}")
    # Recorded evidence for the PR body: the prompt's first words and the reply.
    print(live.scrub(f"[turn {channel}] {text[:60]!r} -> {reply!r}"), flush=True)
    return reply, str(out["thread"])


def _facts(live: LiveStack, agent: str = AGENT) -> list[dict[str, Any]]:
    return live.json("local", "memory", agent)["facts"]


def _containers(live: LiveStack) -> list[str]:
    filters = [arg for f in RUNNER_FILTERS for arg in ("--filter", f)]
    filters += ["--filter", f"label=curietech.ai/stack={live.project}_runner"]
    runners = _run(["docker", "ps", "-aq", *filters], env=live.env).stdout.split()
    worker = _run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={live.project}",
         "--filter", "label=com.docker.compose.service=curie-worker"],
        env=live.env,
    ).stdout.split()
    return runners + worker


def _new_log_lines(live: LiveStack) -> list[str]:
    """Log lines from the runner sandboxes and the worker not seen before."""
    fresh = []
    for container in _containers(live):
        logs = _run(["docker", "logs", container], env=live.env, timeout=60)
        for line in (logs.stdout + logs.stderr).splitlines():
            marker = f"{container}:{line}"
            if marker not in live.seen_log_lines:
                live.seen_log_lines.add(marker)
                fresh.append(line)
    return fresh


def _boot_records(lines: list[str]) -> list[dict[str, Any]]:
    records = []
    for line in lines:
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except ValueError:
            continue
        if not isinstance(record, dict):
            continue
        message = str(record.get("message", ""))
        if not message.startswith("memory boot injection"):
            continue
        fields = dict(record)
        for key, raw in re.findall(r"(\w+)=(\S+)", message):
            fields.setdefault(key, raw)
        for key in ("index_count", "fact_bodies", "instruction_count"):
            if key in fields:
                fields[key] = int(fields[key])
        records.append(fields)
    return records


def _model_route_evidence(lines: list[str]) -> list[str]:
    """Return what is missing from the evidence that turns ran on OpenRouter
    with LIVE_MODEL. Never returns or prints a credential."""
    text = "\n".join(lines)
    missing = []
    if "openrouter.ai" not in text:
        missing.append("no runner or worker log names the openrouter.ai endpoint host")
    if LIVE_MODEL not in text:
        missing.append(f"no runner or worker log names the model id {LIVE_MODEL}")
    return missing


def _transcript_tool_names(live: LiveStack, agent_id: str, thread: str) -> list[str]:
    names: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("type") == "tool_use" and isinstance(node.get("name"), str):
                names.append(node["name"])
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    paths = [f"/agents/{agent_id}/state/transcript"] + [
        f"/agents/{agent_id}/state/bindings/slack/{channel}/transcript"
        for channel in (CHANNEL_A, CHANNEL_B)
    ]
    found = False
    for path in paths:
        status, rows = _api(live.api_url, live.api_key, path)
        if status != 200:
            continue
        for row in rows:
            if thread in str(row.get("key", "")):
                found = True
                walk(row.get("value"))
    assert found, f"no transcript row names thread {thread}"
    return names


def _one(facts: list[dict[str, Any]], needle: str) -> dict[str, Any]:
    matches = [f for f in facts if needle in f["statement"] or needle in f["title"]]
    assert len(matches) == 1, f"expected one row carrying the nonce, found {len(matches)}"
    return matches[0]


def _nonce(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(3).upper()}"


@pytest.mark.skipif(not LIVE, reason="live model acceptance runs only with CURIE_E2E_LIVE=1")
def test_live_cross_channel_instruction_index_and_revert(tmp_path: pathlib.Path) -> None:
    with _live_stack(tmp_path) as live:
        budget = Budget(os.environ["CURIE_CREDENTIALS"])
        _new_log_lines(live)

        # One synthetic agent on two synthetic channels, writes and learning on.
        live.json("local", "deploy", "--plugin-dir", str(_write_bundle(tmp_path, AGENT)),
                  "--slack-channel", CHANNEL_A, timeout=600)
        live.cli("local", "surfaces", AGENT, "--add", f"slack={CHANNEL_B}")
        settings = live.json(
            "local", "overrides", AGENT, "--memory-writes", "on", "--agent-wide-learning", "on",
            "--execution-deadline", "300",
        )
        assert settings["memory_writes"] is True and settings["agent_wide_learning"] is True
        agent = next(a for a in live.api("/agents") if a["name"] == AGENT)
        assert {(c["kind"], c["address"]) for c in agent["channels"]} == {
            ("slack", CHANNEL_A), ("slack", CHANNEL_B)
        }, agent["channels"]
        agent_id = agent["id"]

        # (1) and (2): teach in A.
        codename, room, signer = _nonce("ORCHID"), _nonce("PEBBLE"), _nonce("Quill")
        _turn(live, "Please remember this in agent memory so you know it in every channel: "
              f"the project codename is {codename}.", CHANNEL_A)
        _turn(live, "Please remember this in channel memory for this channel only: "
              f"this channel's standup meets in room {room}.", CHANNEL_A)
        _turn(live, "Standing instruction for every channel: always end every reply with the "
              f"line '-- {signer}'. Save it as an agent memory instruction.", CHANNEL_A)
        lines = _new_log_lines(live)
        missing = _model_route_evidence(lines)
        assert not missing, "; ".join(missing)

        facts = _facts(live)
        fact = _one(facts, codename)
        assert fact["scope"] == "agent" and fact["kind"] == "fact", fact["id"]
        assert fact["author"] and fact["conversation"], "server provenance is stamped"
        channel_fact = _one(facts, room)
        assert channel_fact["scope"] == "channel" and channel_fact["kind"] == "fact"
        assert channel_fact["channel"] == {"kind": "slack", "address": CHANNEL_A}
        instruction = _one(facts, signer)
        assert instruction["scope"] == "agent" and instruction["kind"] == "instruction"
        budget.check("teaching")

        question = ("What is the project codename, and which room does this channel's "
                    "standup meet in? Say UNKNOWN for any you cannot find.")
        reply_a, _ = _turn(live, question, CHANNEL_A)
        reply_b, _ = _turn(live, question, CHANNEL_B)
        assert codename in reply_a and room in reply_a, live.scrub(reply_a)
        assert codename in reply_b, live.scrub(reply_b)
        assert room not in reply_b, "channel-only memory leaked to channel B"
        for channel in (CHANNEL_A, CHANNEL_B):
            unrelated, _ = _turn(live, "What is 2 plus 3?", channel)
            assert signer in unrelated, live.scrub(f"{channel} ignored the signature: {unrelated}")
        budget.check("recall and signature")

        # (3) correction keeps the identity and raises the version; forget removes it.
        corrected = _nonce("TULIP")
        _turn(live, f"Correction: the project codename is now {corrected}, not {codename}. "
              "Update the existing agent memory entry instead of adding a new one, and keep "
              "only the new codename in it.", CHANNEL_A)
        facts = _facts(live)
        # The correction lands on the same row; no other row still carries the old value.
        others = [f for f in facts if codename in f["statement"] and f["id"] != fact["id"]]
        assert not others, "a second row kept the stale value"
        updated = _one(facts, corrected)
        assert updated["id"] == fact["id"] and updated["version"] > fact["version"], updated
        for channel in (CHANNEL_A, CHANNEL_B):
            reply, _ = _turn(live, "What is the project codename?", channel)
            assert corrected in reply, live.scrub(reply)
        _turn(live, "Forget the project codename entirely; delete it from agent memory.",
              CHANNEL_A)
        assert fact["id"] not in {f["id"] for f in _facts(live)}, "forget left the row"
        forgotten = live.json("local", "memory", AGENT, "--history", fact["id"])
        assert forgotten["deleted"] is True, forgotten
        assert forgotten["history"][0]["operation"] == "delete", forgotten
        reply, _ = _turn(live, "What is the project codename? Say UNKNOWN if you cannot find it.",
                         CHANNEL_B)
        assert corrected not in reply and codename not in reply, live.scrub(reply)
        budget.check("correct and forget")

        # (4) 250 canonical facts through the operator API; one old fact is omitted.
        rota = _nonce("ROTA")
        title = "Lighthouse keeper rota code"
        old_id = "fact-" + secrets.token_hex(16)
        live.api(_state_path(agent_id, old_id), "PUT", {"value": _value(
            f"The lighthouse keeper rota code is {rota}.", title=title, author="operator")})
        for index in range(249):
            live.api(_state_path(agent_id, "fact-" + secrets.token_hex(16)), "PUT", {
                "value": _value(f"Filler note number {index:03d} has no secret.",
                                title=f"Filler note {index:03d}", author="operator")})
        active = [f for f in _facts(live) if f["kind"] == "fact" and not f["archived"]]
        assert len(active) >= 250, len(active)
        _new_log_lines(live)
        reply, thread = _turn(live, f"What is the code in the memory titled '{title}'?", CHANNEL_A)
        records = _boot_records(_new_log_lines(live))
        assert records, "the fresh boot recorded no memory boot injection log record"
        boot = records[-1]
        assert boot["index_count"] == 50 and boot["fact_bodies"] == 0, boot
        assert rota not in json.dumps(records), "a fact body reached the boot record"
        selected = str(boot["selected"]).split(",")
        assert len(selected) == boot["instruction_count"] + 50, selected
        assert not any(old_id in entry for entry in selected), "old fact not omitted"
        print(f"[boot] index_count={boot['index_count']} fact_bodies={boot['fact_bodies']} "
              f"instruction_count={boot['instruction_count']} "
              f"omitted_facts={boot['omitted_facts']} selected={len(selected)}", flush=True)
        assert rota in reply, live.scrub(reply)
        tools = _transcript_tool_names(live, agent_id, thread)
        assert any(name.endswith("read_memory") for name in tools), tools
        budget.check("index retrieval")

        archived = live.json("local", "memory", AGENT, "--archive", old_id)
        assert archived["archived"] is True, archived
        row = next(f for f in _facts(live) if f["id"] == old_id)
        assert row["archived"] is True and row["version"] == archived["version"], row
        restored = live.json("local", "memory", AGENT, "--restore", old_id)
        assert restored["archived"] is False and restored["version"] == archived["version"] + 1
        row = next(f for f in _facts(live) if f["id"] == old_id)
        assert row["archived"] is False and row["version"] == restored["version"], row

        # (5) stale forget through the CLI: the head moves after the CLI reads it.
        stale_path = _state_path(agent_id, old_id)
        with _bumping_proxy(
            live.api_url, live.api_key, stale_path,
            lambda _v: _value(f"The lighthouse keeper rota code is {rota}.", title=title,
                              author="operator"),
        ) as (proxy, audit):
            env = dict(live.env, CURIE_API_URL=proxy)
            stale = _run([str(live.binary), "local", "memory", AGENT, "--delete", old_id],
                         env=env, cwd=live.workdir, timeout=120)
        assert not audit.errors, audit.errors
        assert stale.returncode != 0, live.scrub(stale.stdout)
        assert live.api(stale_path)["version"] == audit.bumped_to

        # (6) operator revert of the signature instruction to revision zero.
        reverted = live.json("local", "memory", AGENT, "--revert", instruction["id"],
                             "--to-version", "0")
        assert reverted["deleted"] is True, reverted
        assert instruction["id"] not in {f["id"] for f in _facts(live)}
        reply, _ = _turn(live, "What is 7 times 6?", CHANNEL_B)
        assert signer not in reply, live.scrub(f"reverted instruction still applied: {reply}")
        budget.check("revert")

        # (7) a learning-off agent keeps the original default guidance bytes.
        live.json("local", "deploy", "--plugin-dir", str(_write_bundle(tmp_path, CONTROL)),
                  "--slack-channel", CHANNEL_C, timeout=600)
        control = live.json("local", "overrides", CONTROL, "--memory-writes", "on")
        assert control["agent_wide_learning"] is False, control
        guidance = live.json("local", "memory", CONTROL, "--guidance")
        assert guidance["source"] == "default", guidance["source"]
        raw = guidance["text"].encode()
        assert len(raw) == DEFAULT_GUIDANCE_BYTES
        assert hashlib.sha256(raw).hexdigest() == DEFAULT_GUIDANCE_SHA256
        budget.check("done")

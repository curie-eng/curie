"""Concurrent boot fetches: memory, history, and the MCP capability probe.

A delay on each fake endpoint must overlap so boot finishes in under twice one
delay. Memory and the MCP probe degrade independently; a configured history
ref is fail-loud because answering without prior tool context can duplicate
an operation.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, parse_ndjson
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
from curie_runner import __main__ as boot
from curie_runner.config import RunnerConfig
from curie_runner.history import HistoryError, TurnRecord, build_conversation_replay
from curie_runner.mcp_tool_capability import probe_mcp_tool_capability
from curie_runner.memory import MemoryError, MemoryRecord, format_memory_preamble
from curie_runner.server import create_app

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_capability_server.py"
_DELAY = 1.5
_TRANSCRIPT_CAP_HEADERS = {"X-Curie-Transcript-Max-Bytes": "65536"}

_MEMORY_ITEM = {
    "content": "prefer ruff over flake8",
    "provenance": {
        "learned_from_session_id": "sess-1",
        "source_trace_ids": ["trace-a"],
        "recorded_at": "2026-07-13T00:00:00+00:00",
    },
}
_HISTORY_ITEM = {
    "user": "what changed?",
    "assistant": "the deploy bumped v3",
    "ts": "2026-07-14T00:00:00+00:00",
}


def _bundle(root: Path, *, mcp_command: list[str], mcp_env: dict[str, str] | None = None) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "operations": {
                        "command": mcp_command[0],
                        "args": mcp_command[1:],
                        "env": mcp_env or {"CURIE_TEST_TOOL_MODE": "read-only"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    return root


def _delayed_mcp(tmp_path: Path, delay: float) -> Path:
    script = tmp_path / "delayed_mcp.py"
    script.write_text(
        "import runpy, time\n"
        f"time.sleep({delay!r})\n"
        f"runpy.run_path({str(_SERVER)!r}, run_name='__main__')\n",
        encoding="utf-8",
    )
    return script


def _state_app(
    *,
    memory_delay: float = 0.0,
    history_delay: float = 0.0,
    memory_status: int = 200,
    history_status: int = 200,
    history_error_body: str | None = None,
    memory_value: list[dict[str, Any]] | None = None,
    history_value: list[dict[str, Any]] | None = None,
) -> web.Application:
    app = web.Application()

    async def get_memory(_request: web.Request) -> web.Response:
        if memory_delay:
            await anyio.sleep(memory_delay)
        if memory_status != 200:
            return web.json_response({"detail": "memory failed"}, status=memory_status)
        return web.json_response(
            {
                "namespace": "memory",
                "key": "log",
                "value": list(memory_value or []),
                "version": 1,
            }
        )

    async def get_history(_request: web.Request) -> web.Response:
        if history_delay:
            await anyio.sleep(history_delay)
        if history_status != 200:
            return web.Response(
                text=history_error_body or "history failed",
                status=history_status,
                headers=_TRANSCRIPT_CAP_HEADERS,
            )
        return web.json_response(
            {
                "namespace": "transcript",
                "key": "t1",
                "value": list(history_value or []),
                "version": 1,
            },
            headers=_TRANSCRIPT_CAP_HEADERS,
        )

    app.router.add_get("/agents/A/state/memory/log", get_memory)
    app.router.add_get("/agents/A/state/transcript/t1", get_history)
    return app


def _config(plugin_dir: Path, *, memory_ref: str, history_ref: str) -> RunnerConfig:
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": "s-boot",
            "CURIE_SANDBOX_ID": "b-boot",
            "CURIE_BUDGET": _BUDGET,
            "CURIE_MEMORY_REF": memory_ref,
            "CURIE_HISTORY_REF": history_ref,
        }
    )


async def _run_fetches(
    server: Any,
    plugin_dir: Path,
    *,
    fake_model: bool = False,
) -> boot._BootFetches:
    memory_ref = str(server.make_url("/agents/A/state/memory"))
    history_ref = str(server.make_url("/agents/A/state/transcript/t1"))
    return await boot._load_boot_fetches(
        _config(plugin_dir, memory_ref=memory_ref, history_ref=history_ref),
        fake_model,
        None,
    )


def test_boot_fetches_overlap_three_delayed_endpoints(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path / "bundle",
        mcp_command=[sys.executable, str(_delayed_mcp(tmp_path, _DELAY))],
    )
    app = _state_app(
        memory_delay=_DELAY,
        history_delay=_DELAY,
        memory_value=[_MEMORY_ITEM],
        history_value=[_HISTORY_ITEM],
    )

    async def go() -> None:
        async with TestServer(app) as server:
            started = time.monotonic()
            fetches = await _run_fetches(server, plugin_dir)
            elapsed = time.monotonic() - started
            assert elapsed < 2 * _DELAY
            assert fetches.memory_preamble == format_memory_preamble(
                [MemoryRecord.from_dict(_MEMORY_ITEM)]
            )
            expected_replay, _summary = build_conversation_replay(
                [TurnRecord.from_dict(_HISTORY_ITEM)]
            )
            assert fetches.conversation_replay == expected_replay
            assert fetches.mcp_capability is not None
            assert fetches.mcp_capability.complete
            assert not fetches.mcp_capability.has_potential_write_tool

    anyio.run(go)


def test_boot_fetches_match_sequential_same_inputs(tmp_path: Path) -> None:
    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    app = _state_app(memory_value=[_MEMORY_ITEM], history_value=[_HISTORY_ITEM])

    async def go() -> None:
        async with TestServer(app) as server:
            concurrent = await _run_fetches(server, plugin_dir)
            config = _config(
                plugin_dir,
                memory_ref=str(server.make_url("/agents/A/state/memory")),
                history_ref=str(server.make_url("/agents/A/state/transcript/t1")),
            )
            memory_store, memory_preamble = await boot._load_memory(config)
            history_store, conversation_replay, _capped = await boot._load_history(config)
            capability = await probe_mcp_tool_capability(plugin_dir, {}, None)
            assert concurrent.memory_preamble == memory_preamble
            assert concurrent.conversation_replay == conversation_replay
            assert concurrent.mcp_capability == capability
            assert type(concurrent.memory_store) is type(memory_store)
            assert type(concurrent.history_store) is type(history_store)

    anyio.run(go)


def test_boot_fetches_memory_failure_degrades_independently(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    app = _state_app(
        memory_status=500,
        history_value=[_HISTORY_ITEM],
    )
    caplog.set_level(logging.WARNING, logger="curie_runner")

    async def go() -> None:
        async with TestServer(app) as server:
            fetches = await _run_fetches(server, plugin_dir)
            assert fetches.memory_preamble is None
            expected_replay, _summary = build_conversation_replay(
                [TurnRecord.from_dict(_HISTORY_ITEM)]
            )
            assert fetches.conversation_replay == expected_replay
            assert fetches.mcp_capability is not None
            assert fetches.mcp_capability.complete

    anyio.run(go)
    assert any(
        "memory load failed" in record.message and "booting without memory" in record.message
        for record in caplog.records
    )


def test_boot_fetches_history_failure_fails_loud(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sensitive_body = (
        "https://state.example.com/agents/A/state/transcript/private-key "
        "private transcript text token-PLACEHOLDER"
    )
    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    app = _state_app(
        history_status=500,
        history_error_body=sensitive_body,
        memory_value=[_MEMORY_ITEM],
    )
    caplog.set_level(logging.ERROR, logger="curie_runner")

    async def go() -> None:
        async with TestServer(app) as server:
            with pytest.raises(BaseExceptionGroup) as caught:
                await _run_fetches(server, plugin_dir)
            assert any(
                isinstance(exc, HistoryError)
                and "configured structured history could not be loaded" in str(exc)
                for exc in caught.value.exceptions
            )

    anyio.run(go)
    assert any("history load failed" in record.message for record in caplog.records)
    assert all(sensitive_body not in record.getMessage() for record in caplog.records)


def _capped_history_app(
    records: list[dict[str, Any]],
    append_attempts: list[dict[str, Any]],
    *,
    append_status: int,
    body: str,
) -> web.Application:
    app = web.Application()

    async def get_memory(_request: web.Request) -> web.Response:
        return web.json_response(
            {"namespace": "memory", "key": "log", "value": [], "version": 1}
        )

    async def get_history(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "namespace": "transcript",
                "key": "t1",
                "value": list(records),
                "version": 1,
            },
            headers=_TRANSCRIPT_CAP_HEADERS,
        )

    async def reject_summary(request: web.Request) -> web.Response:
        append_attempts.append(await request.json())
        return web.Response(status=append_status, text=body)

    async def reject_compaction(_request: web.Request) -> web.Response:
        # #2927: a genuinely irreducible thread. The compaction rewrite is also
        # over the cap, so boot keeps the #2820 refusal.
        return web.Response(status=413, text=body)

    app.router.add_get("/agents/A/state/memory", get_memory)
    app.router.add_get("/agents/A/state/transcript/t1", get_history)
    app.router.add_post("/agents/A/state/transcript/t1/append", reject_summary)
    app.router.add_put("/agents/A/state/transcript/t1", reject_compaction)
    return app


def _compactable_records() -> list[dict[str, Any]]:
    records = [
        TurnRecord(
            user=f"question {index}",
            assistant="answer " + ("x" * 600),
            ts=f"2026-07-14T00:00:{index:02d}+00:00",
        ).to_dict()
        for index in range(45)
    ]
    _replay, summary = build_conversation_replay(
        [TurnRecord.from_dict(record) for record in records]
    )
    assert summary is not None
    return records


def test_boot_summary_capacity_failure_serves_the_append_path_refusal(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2820: a thread already at the cap must not kill every cold sandbox.

    Boot compaction's 413 used to be collapsed into a flat HistoryError, so the
    runner died before serving and the worker retried the dropped stream as a
    retryable runner-error. The runner now boots and answers each turn over the
    real /v1/event route with the append path's exact non-retryable pair: one
    history-persistence-error and one CLASSIFIED_FAILURE final.
    """

    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    records = _compactable_records()
    sensitive_body = (
        "https://state.example.com/agents/A/state/transcript/private-key "
        "private transcript text token-PLACEHOLDER"
    )
    append_attempts: list[dict[str, Any]] = []
    app = _capped_history_app(
        records, append_attempts, append_status=413, body=sensitive_body
    )

    async def reject_probe(*args: object, **kwargs: object) -> object:
        raise AssertionError("fake model boot must not start connector tools")

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", reject_probe)
    caplog.set_level(logging.INFO, logger="curie_runner")
    token = "t" * 40

    async def go() -> list[list[object]]:
        async with TestServer(app) as state_server:
            config = _config(
                plugin_dir,
                memory_ref=str(state_server.make_url("/agents/A/state/memory")),
                history_ref=str(state_server.make_url("/agents/A/state/transcript/t1")),
            )
            fetches = await boot._load_boot_fetches(config, True, None)
            assert fetches.history_capacity_exceeded is True
            runner = boot.build_runner(
                config,
                fake_model=True,
                memory_store=fetches.memory_store,
                history_store=fetches.history_store,
                conversation_replay=fetches.conversation_replay,
                history_capacity_exceeded=fetches.history_capacity_exceeded,
            )
            await runner.start()
            turns: list[list[object]] = []
            async with TestServer(create_app(runner, token=token)) as runner_server:
                async with ClientSession() as http:
                    for ts in ("1", "2"):
                        async with http.post(
                            runner_server.make_url("/v1/event"),
                            json={
                                "kind": "event",
                                "type": "message",
                                "text": "hi",
                                "user": "U",
                                "ts": ts,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        ) as resp:
                            assert resp.status == 200
                            turns.append(parse_ndjson(await resp.text()))
            await runner.close()
            return turns

    turns = anyio.run(go)

    for events in turns:
        assert len(events) == 2, events
        error, final = events
        assert isinstance(error, ErrorEvent)
        assert error.classification == "history-persistence-error"
        assert error.message == "conversation history capacity exceeded"
        assert sensitive_body not in error.message
        assert isinstance(final, Final)
        assert final.status is SessionStatus.CLASSIFIED_FAILURE
        assert final.text == "run failed: conversation history could not be persisted"
    # The refusal never appends again: the durable log is left untouched.
    assert len(append_attempts) == 1
    assert append_attempts[0]["item"]["type"] == "summary"
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "history capacity exceeded at boot" in message and "413" in message
        for message in messages
    )
    assert all(sensitive_body not in message for message in messages)
    assert all("question 0" not in message for message in messages)


def test_boot_summary_non_capacity_failure_stays_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Negative control: only a 413 becomes a served refusal; a 500 still fails boot."""

    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    append_attempts: list[dict[str, Any]] = []
    app = _capped_history_app(
        _compactable_records(), append_attempts, append_status=500, body="boom"
    )

    async def reject_probe(*args: object, **kwargs: object) -> object:
        raise AssertionError("fake model boot must not start connector tools")

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", reject_probe)

    async def go() -> None:
        async with TestServer(app) as server:
            config = _config(
                plugin_dir,
                memory_ref=str(server.make_url("/agents/A/state/memory")),
                history_ref=str(server.make_url("/agents/A/state/transcript/t1")),
            )
            with pytest.raises(BaseExceptionGroup) as caught:
                await boot._load_boot_fetches(config, True, None)
            assert "configured structured history could not be loaded" in repr(
                caught.value
            )

    anyio.run(go)
    assert len(append_attempts) == 1


def test_boot_fetches_probe_failure_degrades_independently(tmp_path: Path) -> None:
    plugin_dir = _bundle(
        tmp_path / "bundle",
        mcp_command=[sys.executable, "-c", "raise SystemExit(1)"],
    )
    app = _state_app(memory_value=[_MEMORY_ITEM], history_value=[_HISTORY_ITEM])

    async def go() -> None:
        async with TestServer(app) as server:
            fetches = await _run_fetches(server, plugin_dir)
            assert fetches.memory_preamble == format_memory_preamble(
                [MemoryRecord.from_dict(_MEMORY_ITEM)]
            )
            expected_replay, _summary = build_conversation_replay(
                [TurnRecord.from_dict(_HISTORY_ITEM)]
            )
            assert fetches.conversation_replay == expected_replay
            assert fetches.mcp_capability is not None
            assert not fetches.mcp_capability.complete
            assert fetches.mcp_capability.has_potential_write_tool

    anyio.run(go)


def test_boot_fetches_bad_memory_ref_fails_visibly(tmp_path: Path) -> None:
    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    config = _config(
        plugin_dir,
        memory_ref="s3://bucket/mem",
        history_ref="http://127.0.0.1:9/agents/A/state/transcript/t1",
    )

    async def go() -> None:
        with pytest.raises(MemoryError, match="unsupported CURIE_MEMORY_REF scheme"):
            await boot._load_boot_fetches(config, False, None)

    anyio.run(go)


def test_boot_fetches_diagnoses_empty_expansion_on_fake_model_without_probing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin_dir = tmp_path / "bundle"
    (plugin_dir / ".claude-plugin").mkdir(parents=True)
    (plugin_dir / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    (plugin_dir / "connectors.yaml").write_text(
        "connectors:\n"
        "  github:\n"
        "    url: http://127.0.0.1:9/mcp\n"
        "    headers:\n"
        "      Authorization: Bearer ${GITHUB_TOKEN}\n",
        encoding="utf-8",
    )

    async def boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("fake-model must not HTTP-probe a connector")

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", boom)
    app = _state_app(memory_value=[_MEMORY_ITEM], history_value=[_HISTORY_ITEM])

    async def go() -> None:
        async with TestServer(app) as server:
            config = _config(
                plugin_dir,
                memory_ref=str(server.make_url("/agents/A/state/memory")),
                history_ref=str(server.make_url("/agents/A/state/transcript/t1")),
            )
            fetches = await boot._load_boot_fetches(config, True, {"GITHUB_TOKEN": ""})
            assert fetches.mcp_capability is None
            assert fetches.connector_failures
            assert fetches.connector_failures[0].connector == "github"
            assert fetches.connector_failures[0].reason == "empty_expansion"
            assert fetches.connector_failures[0].credential_names == ("GITHUB_TOKEN",)

    anyio.run(go)


def test_boot_fetches_skips_probe_on_fake_model(tmp_path: Path) -> None:
    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    app = _state_app(memory_value=[_MEMORY_ITEM], history_value=[_HISTORY_ITEM])

    async def go() -> None:
        async with TestServer(app) as server:
            fetches = await _run_fetches(server, plugin_dir, fake_model=True)
            assert fetches.mcp_capability is None
            assert fetches.memory_preamble is not None
            assert fetches.conversation_replay.present

    anyio.run(go)


def test_the_capability_probe_derives_the_caller_header_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # ADR-0168 decision 7: the probe dials the hosted entries the session
    # mounts, so it must present the caller header as well. On the fake-model
    # path the probe only diagnoses header placeholders against the process
    # env, which this test leaves without the token: the derived header then
    # shows up as a missing credential, and without the header there would be
    # nothing to report.
    plugin_dir = tmp_path / "bundle"
    (plugin_dir / ".claude-plugin").mkdir(parents=True)
    (plugin_dir / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    (plugin_dir / "connectors.yaml").write_text(
        "connectors:\n  grafana:\n    image: grafana/mcp-grafana:0.17.2\n", encoding="utf-8"
    )
    monkeypatch.delenv("CURIE_CONNECTOR_CALLER_TOKEN", raising=False)
    app = _state_app(memory_value=[_MEMORY_ITEM], history_value=[_HISTORY_ITEM])

    async def go() -> None:
        async with TestServer(app) as server:
            config = RunnerConfig.from_env(
                {
                    "CURIE_PLUGIN_DIR": str(plugin_dir),
                    "CURIE_SESSION_ID": "s-boot",
                    "CURIE_SANDBOX_ID": "b-boot",
                    "CURIE_BUDGET": _BUDGET,
                    "CURIE_MEMORY_REF": str(server.make_url("/agents/A/state/memory")),
                    "CURIE_HISTORY_REF": str(server.make_url("/agents/A/state/transcript/t1")),
                    "CURIE_CONNECTOR_RELEASE": "curie",
                    "CURIE_CONNECTOR_AGENT": "acme-dev",
                    "CURIE_CONNECTOR_NAMESPACE": "curie",
                    "CURIE_CONNECTOR_CALLER_TOKEN": "cct.payload.signature",
                }
            )
            fetches = await boot._load_boot_fetches(config, True, None)
            assert [
                (failure.connector, failure.credential_names)
                for failure in fetches.connector_failures
            ] == [("grafana", ("CURIE_CONNECTOR_CALLER_TOKEN",))]

    anyio.run(go)

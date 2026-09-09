"""Concurrent boot fetches: memory, history, and the MCP capability probe.

The three results are independent. A delay on each fake endpoint must overlap
so boot finishes in under twice one delay, and a failure in one must still
degrade only that result.
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
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import __main__ as boot
from curie_runner.config import RunnerConfig
from curie_runner.history import TurnRecord, format_conversation_preamble
from curie_runner.mcp_tool_capability import probe_mcp_tool_capability
from curie_runner.memory import MemoryError, MemoryRecord, format_memory_preamble

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_capability_server.py"
_DELAY = 1.5

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
            return web.json_response({"detail": "history failed"}, status=history_status)
        return web.json_response(
            {
                "namespace": "transcript",
                "key": "t1",
                "value": list(history_value or []),
                "version": 1,
            }
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
            assert fetches.conversation_preamble == format_conversation_preamble(
                [TurnRecord.from_dict(_HISTORY_ITEM)]
            )
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
            history_store, conversation_preamble = await boot._load_history(config)
            capability = await probe_mcp_tool_capability(plugin_dir, {}, None)
            assert concurrent.memory_preamble == memory_preamble
            assert concurrent.conversation_preamble == conversation_preamble
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
            assert fetches.conversation_preamble == format_conversation_preamble(
                [TurnRecord.from_dict(_HISTORY_ITEM)]
            )
            assert fetches.mcp_capability is not None
            assert fetches.mcp_capability.complete

    anyio.run(go)
    assert any(
        "memory load failed" in record.message and "booting without memory" in record.message
        for record in caplog.records
    )


def test_boot_fetches_history_failure_degrades_independently(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    plugin_dir = _bundle(tmp_path / "bundle", mcp_command=[sys.executable, str(_SERVER)])
    app = _state_app(
        history_status=500,
        memory_value=[_MEMORY_ITEM],
    )
    caplog.set_level(logging.WARNING, logger="curie_runner")

    async def go() -> None:
        async with TestServer(app) as server:
            fetches = await _run_fetches(server, plugin_dir)
            assert fetches.conversation_preamble is None
            assert fetches.memory_preamble == format_memory_preamble(
                [MemoryRecord.from_dict(_MEMORY_ITEM)]
            )
            assert fetches.mcp_capability is not None
            assert fetches.mcp_capability.complete

    anyio.run(go)
    assert any(
        "history load failed" in record.message and "booting without history" in record.message
        for record in caplog.records
    )


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
            assert fetches.conversation_preamble == format_conversation_preamble(
                [TurnRecord.from_dict(_HISTORY_ITEM)]
            )
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
            assert fetches.conversation_preamble is not None

    anyio.run(go)

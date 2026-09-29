"""The platform ``progress`` tool: deliberate progress from a running turn (ADR 0130).

The tool is ADR 0130's ``curie_progress`` operation, mounted on the ``curie``
server as ``mcp__curie__progress``. The worker hands the runner a per-turn
capability in two runner control headers on ``POST /v1/event``; the runner holds
it only while that turn is open and POSTs each command to the capability URL
with the runner's ``epoch`` and ``seq``. Without a capability the tool answers
softly and makes no network call, and no failure of the post fails the turn.

The capability server here is a real local aiohttp app standing in for the
API's ingress; nothing below the runner's HTTP client is mocked.
"""

from __future__ import annotations

import copy
import json
import socket
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import create_app
from curie_runner import turn_progress as turn_progress_module
from curie_runner.__main__ import _compose_system_prompt, build_runner
from curie_runner.config import RunnerConfig
from curie_runner.turn_progress import (
    NOT_SHOWN_TEXT,
    PROGRESS_GENERATION_HEADER,
    PROGRESS_INPUT_SCHEMA,
    PROGRESS_PREAMBLE,
    PROGRESS_TOKEN_HEADER,
    PROGRESS_URL_HEADER,
    ProgressCapability,
    TurnProgress,
    should_mount_turn_progress,
)

_REPO = Path(__file__).resolve().parents[2]
_SCHEMA = _REPO / "packages" / "channel-protocol" / "schema" / "channel-protocol.schema.json"
_TOKEN = "sbx.example.token"

_VALID = {
    "update_id": "u1",
    "state": "investigating",
    "summary": "Reading the failing test",
}


class _Ingress:
    """A stand-in for the API's ``/v1/turn-progress/{id}`` that records posts."""

    def __init__(self, status: int = 202) -> None:
        self.status = status
        self.posts: list[tuple[dict[str, str], dict[str, Any]]] = []
        self.app = web.Application()
        self.app.add_routes([web.post("/v1/turn-progress/{progress_id}", self._post)])

    async def _post(self, request: web.Request) -> web.Response:
        self.posts.append((dict(request.headers), await request.json()))
        return web.json_response({"accepted": self.status == 202}, status=self.status)


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _text(result: dict[str, Any]) -> str:
    return " ".join(part["text"] for part in result["content"] if part.get("type") == "text")


def _forbid_network(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    attempts: list[str] = []

    class _NoSession:
        def __init__(self, *args: object, **kwargs: object) -> None:
            attempts.append("ClientSession")
            raise AssertionError("the progress tool opened a network session")

    monkeypatch.setattr(turn_progress_module.aiohttp, "ClientSession", _NoSession)
    return attempts


# --- the schema ------------------------------------------------------------


def _inline(node: Any, defs: dict[str, Any]) -> Any:
    if isinstance(node, dict):
        if set(node) == {"$ref"}:
            return _inline(copy.deepcopy(defs[node["$ref"].rsplit("/", 1)[-1]]), defs)
        return {key: _inline(value, defs) for key, value in node.items()}
    if isinstance(node, list):
        return [_inline(item, defs) for item in node]
    return node


def test_the_input_schema_is_the_committed_command_schema_without_version() -> None:
    committed = json.loads(_SCHEMA.read_text())
    defs = committed["$defs"]
    expected = copy.deepcopy(defs["ProgressCommand"])
    del expected["properties"]["version"]
    expected["required"] = [name for name in expected["required"] if name != "version"]
    assert PROGRESS_INPUT_SCHEMA == _inline(expected, defs)
    # Closed like the command itself: the model cannot add a routing field.
    assert PROGRESS_INPUT_SCHEMA["additionalProperties"] is False


# --- no capability ---------------------------------------------------------


def test_no_capability_is_a_soft_result_and_makes_no_network_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = _forbid_network(monkeypatch)
    progress = TurnProgress()

    async def go() -> None:
        # Never opened, opened without a capability, and after the turn closed.
        results = [await progress.submit(_VALID)]
        progress.open(None)
        results.append(await progress.submit(_VALID))
        progress.close()
        progress.open(
            ProgressCapability(
                url="http://127.0.0.1:9/v1/turn-progress/x",
                token=_TOKEN,
                generation=7,
            )
        )
        progress.close()
        results.append(await progress.submit(_VALID))
        for result in results:
            assert not result.get("is_error")
            assert _text(result) == NOT_SHOWN_TEXT

    anyio.run(go)
    assert attempts == []


@pytest.mark.parametrize(
    ("factory_requested", "factory_resolved", "expected"),
    [
        (False, False, True),
        (True, True, False),
        (True, False, False),
    ],
)
def test_factory_boot_matrix_never_falls_back_to_deliberate_progress(
    factory_requested: bool, factory_resolved: bool, expected: bool
) -> None:
    assert (
        should_mount_turn_progress(
            factory_progress_requested=factory_requested,
            factory_progress_resolved=factory_resolved,
        )
        is expected
    )


def test_capability_headers_need_both_values() -> None:
    url = "http://api:8000/v1/turn-progress/abc"
    assert ProgressCapability.from_headers(
        {
            PROGRESS_URL_HEADER: url,
            PROGRESS_TOKEN_HEADER: _TOKEN,
            PROGRESS_GENERATION_HEADER: "7",
        }
    ) == ProgressCapability(url=url, token=_TOKEN, generation=7)
    assert ProgressCapability.from_headers({PROGRESS_URL_HEADER: url}) is None
    assert ProgressCapability.from_headers({PROGRESS_TOKEN_HEADER: _TOKEN}) is None
    assert (
        ProgressCapability.from_headers({PROGRESS_URL_HEADER: url, PROGRESS_TOKEN_HEADER: _TOKEN})
        is None
    )
    assert (
        ProgressCapability.from_headers(
            {
                PROGRESS_URL_HEADER: url,
                PROGRESS_TOKEN_HEADER: _TOKEN,
                PROGRESS_GENERATION_HEADER: "not-an-int",
            }
        )
        is None
    )
    assert (
        ProgressCapability.from_headers({PROGRESS_URL_HEADER: " ", PROGRESS_TOKEN_HEADER: _TOKEN})
        is None
    )
    assert ProgressCapability.from_headers({}) is None


# --- generation and seq ----------------------------------------------------


def test_each_post_carries_the_worker_generation_and_a_monotonic_seq() -> None:
    ingress = _Ingress()
    progress = TurnProgress()

    async def go() -> None:
        async with TestServer(ingress.app) as server:
            url = str(server.make_url("/v1/turn-progress/chain-1"))
            capability = ProgressCapability(url=url, token=_TOKEN, generation=7)

            progress.open(capability)
            first = await progress.submit(_VALID)
            second = await progress.submit(
                {**_VALID, "update_id": "u2", "summary": "Found it", "milestone": "evidence"}
            )
            progress.close()
            progress.open(ProgressCapability(url=url, token=_TOKEN, generation=8))
            third = await progress.submit({**_VALID, "update_id": "u3"})
            progress.close()

        for result in (first, second, third):
            assert not result.get("is_error"), result

    anyio.run(go)
    assert len(ingress.posts) == 3
    bodies = [body for _headers, body in ingress.posts]
    for headers, _body in ingress.posts:
        assert {k.lower(): v for k, v in headers.items()}["x-api-key"] == _TOKEN
    assert bodies[0] == {"version": "1.0", **_VALID, "generation": 7, "seq": 1}
    assert bodies[1] == {
        "version": "1.0",
        **_VALID,
        "update_id": "u2",
        "summary": "Found it",
        "milestone": "evidence",
        "generation": 7,
        "seq": 2,
    }
    assert bodies[2]["generation"] == 8
    assert bodies[2]["seq"] == 1


def test_202_means_queued_not_recorded() -> None:
    ingress = _Ingress()
    progress = TurnProgress()

    async def go() -> dict[str, Any]:
        async with TestServer(ingress.app) as server:
            progress.open(
                ProgressCapability(
                    url=str(server.make_url("/v1/turn-progress/chain-1")),
                    token=_TOKEN,
                    generation=1,
                )
            )
            return await progress.submit(_VALID)

    assert _text(anyio.run(go)) == "Progress queued."


# --- failures are soft -----------------------------------------------------


@pytest.mark.parametrize("status", [401, 429, 500, 503])
def test_a_refusal_or_server_error_is_a_soft_result(status: int) -> None:
    ingress = _Ingress(status=status)
    progress = TurnProgress()

    async def go() -> dict[str, Any]:
        async with TestServer(ingress.app) as server:
            progress.open(
                ProgressCapability(
                    url=str(server.make_url("/v1/turn-progress/chain-1")),
                    token=_TOKEN,
                    generation=7,
                )
            )
            return await progress.submit(_VALID)

    result = anyio.run(go)
    assert not result.get("is_error")
    assert "continue" in _text(result).lower()
    assert ingress.posts, "the tool must have tried"


def test_a_transport_failure_is_a_soft_result() -> None:
    progress = TurnProgress()
    progress.open(
        ProgressCapability(
            url=f"http://127.0.0.1:{_closed_port()}/v1/turn-progress/chain-1",
            token=_TOKEN,
            generation=7,
        )
    )

    result = anyio.run(progress.submit, _VALID)
    assert not result.get("is_error")
    assert "continue" in _text(result).lower()


def test_a_command_the_ingress_refuses_is_an_error_the_model_can_fix() -> None:
    ingress = _Ingress(status=422)
    progress = TurnProgress()

    async def go() -> dict[str, Any]:
        async with TestServer(ingress.app) as server:
            progress.open(
                ProgressCapability(
                    url=str(server.make_url("/v1/turn-progress/chain-1")),
                    token=_TOKEN,
                    generation=7,
                )
            )
            return await progress.submit({**_VALID, "state": "done-ish"})

    result = anyio.run(go)
    assert result.get("is_error") is True


def test_an_unknown_field_is_refused_before_any_network_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = _forbid_network(monkeypatch)
    progress = TurnProgress()
    progress.open(
        ProgressCapability(url="http://127.0.0.1:9/v1/turn-progress/x", token=_TOKEN, generation=7)
    )

    for extra in ({"channel": "C0EXAMPLE1"}, {"version": "1.0"}, {"delivery_id": "d"}):
        result = anyio.run(progress.submit, {**_VALID, **extra})
        assert result.get("is_error") is True
        assert next(iter(extra)) in _text(result)
    assert attempts == []


# --- the prompt block ------------------------------------------------------


def test_the_prompt_block_rides_beside_the_other_platform_blocks() -> None:
    composed = _compose_system_prompt(
        "bundle prompt",
        "memory block",
        model="acme-model",
        workspace_preamble="workspace block",
        progress_preamble=PROGRESS_PREAMBLE,
    )
    assert composed is not None
    assert composed.index("memory block") < composed.index(PROGRESS_PREAMBLE)
    assert composed.index(PROGRESS_PREAMBLE) < composed.index("bundle prompt")
    for rule in ("mcp__curie__progress", "three", "reasoning", "secrets", "quick"):
        assert rule in PROGRESS_PREAMBLE
    # Absent, the composition is what it was.
    assert _compose_system_prompt("bundle prompt", None, model=None) == "bundle prompt"


# --- the real boot path, end to end over HTTP ------------------------------


def _boot(tmp_path: Path) -> Any:
    plugin_dir = tmp_path / "bundle"
    (plugin_dir / ".claude-plugin").mkdir(parents=True)
    (plugin_dir / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": "session-acme-progress",
            "CURIE_SANDBOX_ID": "sandbox-acme-progress",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
        }
    )
    return build_runner(config, fake_model=True)


_DEMO = {
    "kind": "event",
    "type": "message",
    "text": "[fake:progress-demo] check the build",
    "user": "U1",
    "ts": "1",
}


def test_the_fake_demo_is_network_free_even_with_a_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("curie_runner.fake.PROGRESS_DEMO_PAUSE_S", 0.0)
    runner = _boot(tmp_path)
    attempts = _forbid_network(monkeypatch)

    async def go() -> tuple[list[Any], list[Any]]:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            first = await client.post(
                "/v1/event",
                json=_DEMO,
                headers={
                    PROGRESS_URL_HEADER: "http://127.0.0.1:9/v1/turn-progress/chain-1",
                    PROGRESS_TOKEN_HEADER: _TOKEN,
                    PROGRESS_GENERATION_HEADER: "7",
                },
            )
            assert first.status == 200
            first_events = parse_ndjson(await first.text())
            second = await client.post("/v1/event", json=_DEMO)
            assert second.status == 200
            second_events = parse_ndjson(await second.text())
        return first_events, second_events

    first_events, second_events = anyio.run(go)
    assert first_events[-1].status == SessionStatus.DONE
    assert second_events[-1].status == SessionStatus.DONE
    assert attempts == []
    # The progress calls are platform bookkeeping: never a side effect.
    assert not any(event.type == "side_effect_flag" for event in first_events)


def test_the_fake_demo_without_a_capability_completes_with_no_network_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("curie_runner.fake.PROGRESS_DEMO_PAUSE_S", 0.0)
    attempts = _forbid_network(monkeypatch)
    runner = _boot(tmp_path)

    async def go() -> list[Any]:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            response = await client.post("/v1/event", json=_DEMO)
            return parse_ndjson(await response.text())

    events = anyio.run(go)
    assert events[-1].status == SessionStatus.DONE
    assert attempts == []


def test_a_capability_that_cannot_be_reached_never_fails_the_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("curie_runner.fake.PROGRESS_DEMO_PAUSE_S", 0.0)
    runner = _boot(tmp_path)
    url = f"http://127.0.0.1:{_closed_port()}/v1/turn-progress/chain-1"

    async def go() -> list[Any]:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            response = await client.post(
                "/v1/event",
                json=_DEMO,
                headers={
                    PROGRESS_URL_HEADER: url,
                    PROGRESS_TOKEN_HEADER: _TOKEN,
                    PROGRESS_GENERATION_HEADER: "7",
                },
            )
            return parse_ndjson(await response.text())

    events = anyio.run(go)
    assert events[-1].status == SessionStatus.DONE
    assert not any(event.type == "error" for event in events)

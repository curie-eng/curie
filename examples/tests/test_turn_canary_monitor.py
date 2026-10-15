"""Contract tests for the turn canary's opt-in standing monitor options.

@spec examples/turn-canary/design.md
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

CANARY = Path(__file__).resolve().parents[1] / "turn-canary" / "turn_canary.py"
FAST = {"turn_deadline": 0.02, "cleanup_deadline": 0.02, "poll_period": 0.005}
WRITES = {"enqueue", "reset"}
READS = {
    "inventory",
    "read_only_supported",
    "quota_headroom",
    "replies",
    "reset_state",
    "channel_identities",
    "adapter_health",
}


def _canary():
    """Load the example module by path. @spec TURN-CANARY-7"""
    spec = importlib.util.spec_from_file_location("turn_canary_monitor_example", CANARY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _row(agent: str, address: str, *, kind: str = "slack", adapter: str = "default", active=True):
    """One synthetic inventory row. @spec TURN-CANARY-7"""
    return {
        "agent_id": f"00000000-0000-4000-8000-00000000000{agent}",
        "agent": f"sample-{agent}",
        "deployment_status": "active" if active else "inactive",
        "kind": kind,
        "address": address,
        "adapter": adapter,
    }


def _label(row: dict[str, str]) -> str:
    return "|".join((row["kind"], row["address"], row["adapter"]))


def _identity(name: str = "default", **overrides):
    """One synthetic channel identity read. @spec TURN-CANARY-8"""
    return {
        "name": name,
        "status": "active",
        "installation_mismatch": False,
        "credential_ref": "env:EXAMPLE_BOT_TOKEN",
        **overrides,
    }


class MonitorPlatform:
    """In-memory read/turn boundary recording every call. @spec TURN-CANARY-7 TURN-CANARY-8"""

    def __init__(self, rows, *, identities=None, identity_error=None, adapters=None):
        self.rows = rows
        self.identities = identities if identities is not None else {"slack": [_identity()]}
        self.identity_error = identity_error
        self.adapters = adapters or {}
        self.calls: list[tuple[str, object]] = []
        self.nonces: dict[str, str] = {}
        self.cleanup_blocked = False

    async def inventory(self):
        self.calls.append(("inventory", None))
        return self.rows

    async def read_only_supported(self):
        self.calls.append(("read_only_supported", None))
        return True

    async def quota_headroom(self):
        self.calls.append(("quota_headroom", None))
        return True

    async def enqueue(self, turn):
        self.calls.append(("enqueue", turn.reply_handle.channel))
        nonce = turn.text.split("no other text: ", 1)[1].split(". Do not", 1)[0]
        self.nonces[turn.reply_handle.placeholder] = nonce

    async def replies(self, reply_ref, after):
        self.calls.append(("replies", after))
        events = [
            {"event": "reply.update", "text": self.nonces[reply_ref]},
            {"event": "turn.completed", "outcome": "delivered"},
        ]
        return {"events": events, "next_cursor": after + len(events), "terminal": True}

    async def reset(self, agent_id, thread_key):
        self.calls.append(("reset", thread_key))

    async def reset_state(self, agent_id, thread_key):
        self.calls.append(("reset_state", thread_key))
        return {"requested": False, "route_existed": True}

    async def channel_identities(self, kind):
        self.calls.append(("channel_identities", kind))
        if self.identity_error is not None:
            raise self.identity_error
        return self.identities.get(kind, [])

    async def adapter_health(self, url):
        self.calls.append(("adapter_health", url))
        return self.adapters[url]

    def enqueued(self) -> list[object]:
        return [value for name, value in self.calls if name == "enqueue"]


# TURN-CANARY-7: discovery and rotation


def _three_agents_two_routes():
    return [
        _row(str(agent), f"C-EXAMPLE-{agent}{route}") for agent in (1, 2, 3) for route in ("A", "B")
    ]


def test_discovery_turns_one_route_per_agent_and_rotates_deterministically():
    """@spec TURN-CANARY-7"""
    canary = _canary()
    rows = [
        _row("1", "C-EXAMPLE-1B"),
        _row("1", "C-EXAMPLE-1A"),
        _row("2", "C-EXAMPLE-2A"),
        _row("3", "C-EXAMPLE-3A", active=False),
        _row("1", "D-EXAMPLE-1", kind="discord", adapter="discord-example"),
    ]
    first, _ = canary.discover_targets(rows, rotation=0, max_targets=5)
    second, _ = canary.discover_targets(rows, rotation=1, max_targets=5)
    assert [_label(row) for row in first] == [
        "slack|C-EXAMPLE-1A|default",
        "slack|C-EXAMPLE-2A|default",
    ]
    assert [_label(row) for row in second] == [
        "slack|C-EXAMPLE-1B|default",
        "slack|C-EXAMPLE-2A|default",
    ]
    assert canary.discover_targets(list(reversed(rows)), rotation=1, max_targets=5)[0] == second


def test_discovery_window_walks_every_route_when_agents_exceed_max_targets():
    """@spec TURN-CANARY-7"""
    canary = _canary()
    rows = _three_agents_two_routes()
    seen: list[str] = []
    for rotation in range(3):
        targets, _ = canary.discover_targets(rows, rotation=rotation, max_targets=2)
        assert len(targets) == 2
        assert len({row["agent_id"] for row in targets}) == 2
        seen.extend(_label(row) for row in targets)
    assert sorted(seen) == sorted(_label(row) for row in rows)


def test_discovery_skips_ambiguous_excluded_and_blank_routes():
    """@spec TURN-CANARY-7"""
    canary = _canary()
    duplicate = _row("4", "C-EXAMPLE-DUP")
    rows = [
        _row("1", "C-EXAMPLE-1A"),
        _row("2", "C-EXAMPLE-2A"),
        duplicate,
        {**duplicate, "agent_id": "00000000-0000-4000-8000-000000000005"},
        {**_row("6", "C-EXAMPLE-6A"), "adapter": ""},
    ]
    targets, logs = canary.discover_targets(
        rows, rotation=0, max_targets=5, exclude=[("slack", "C-EXAMPLE-2A", "default")]
    )
    assert [_label(row) for row in targets] == ["slack|C-EXAMPLE-1A|default"]
    assert {"phase": "discovery", "outcome": "ambiguous"} in logs
    assert "C-EXAMPLE-DUP" not in json.dumps(logs)


def test_discovery_cycle_persists_rotation_and_resumes(tmp_path):
    """@spec TURN-CANARY-7"""
    canary = _canary()
    state = tmp_path / "state.json"
    rows = [_row("1", "C-EXAMPLE-1A"), _row("1", "C-EXAMPLE-1B")]
    turned = []
    for _ in range(3):
        platform = MonitorPlatform(rows)
        with canary.StateJournal(state) as journal:
            result = asyncio.run(
                canary.run_cycle(platform, None, journal=journal, discover=True, **FAST)
            )
            journal.save_result(result)
        turned.extend(platform.enqueued())
    assert turned == ["C-EXAMPLE-1A", "C-EXAMPLE-1B", "C-EXAMPLE-1A"]
    assert json.loads(state.read_text())["rotation"] == 3


def test_empty_discovery_is_a_failed_cycle():
    """@spec TURN-CANARY-7"""
    canary = _canary()
    platform = MonitorPlatform([_row("1", "C-EXAMPLE-1A", active=False)])
    result = asyncio.run(canary.run_cycle(platform, None, discover=True, **FAST))
    assert result.success is False
    assert {"phase": "discovery", "outcome": "empty"} in result.logs
    assert platform.enqueued() == []


@pytest.mark.parametrize("rotation", [-1, True, "1", 1.5])
def test_corrupt_rotation_state_refuses(tmp_path, rotation):
    """@spec TURN-CANARY-7"""
    canary = _canary()
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"pending": None, "rotation": rotation}))
    with pytest.raises(ValueError, match="state"):
        with canary.StateJournal(state):
            pass


def _env(**extra: str) -> dict[str, str]:
    return {
        "TURN_CANARY_DATABASE_URL": "postgresql+asyncpg://canary@db.example.invalid/curie",
        "TURN_CANARY_VALKEY_URL": "redis://valkey.example.invalid:6379/0",
        "TURN_CANARY_API_URL": "https://platform.example.invalid",
        "TURN_CANARY_API_KEY": "example-key",
        "TURN_CANARY_STREAM": "curie:runs",
        "TURN_CANARY_STATE_PATH": "/var/lib/turn-canary/state",
        **extra,
    }


def test_explicit_routes_and_discovery_are_mutually_exclusive():
    """@spec TURN-CANARY-7"""
    canary = _canary()
    routes = json.dumps([["slack", "C-EXAMPLE-1", "default"]])
    with pytest.raises(ValueError, match="ROUTES"):
        canary.parse_config(_env(TURN_CANARY_ROUTES=routes, TURN_CANARY_DISCOVER="1"))
    with pytest.raises(ValueError, match="ROUTES"):
        canary.parse_config(_env())
    assert canary.parse_config(_env(TURN_CANARY_DISCOVER="1")).discover is True
    explicit = canary.parse_config(_env(TURN_CANARY_ROUTES=routes))
    assert explicit.discover is False
    assert explicit.routes == [("slack", "C-EXAMPLE-1", "default")]
    assert explicit.listen is None
    assert explicit.metric_prefix == "turn_canary"


# TURN-CANARY-8: model-free binding checks


@pytest.mark.parametrize(
    ("row", "identities", "outcome"),
    [
        (_row("1", "C-EXAMPLE-1A"), [_identity()], "ok"),
        (_row("1", "C-EXAMPLE-1A", active=False), [_identity()], "deployment_inactive"),
        (_row("1", "C-EXAMPLE-1A"), [_identity("other")], "identity_missing"),
        (_row("1", "C-EXAMPLE-1A"), [_identity(), _identity()], "identity_ambiguous"),
        (_row("1", "C-EXAMPLE-1A"), [_identity(status="disabled")], "identity_inactive"),
        (
            _row("1", "C-EXAMPLE-1A"),
            [_identity(installation_mismatch=True)],
            "installation_mismatch",
        ),
        (_row("1", "C-EXAMPLE-1A"), [_identity(credential_ref="  ")], "credential_missing"),
        (_row("1", "C-EXAMPLE-1A"), [_identity(credential_ref=None)], "credential_missing"),
        ({**_row("1", "C-EXAMPLE-1A"), "adapter": ""}, [_identity("")], "identity_missing"),
        (_row("1", "D-EXAMPLE-1", kind="discord", adapter="discord-example"), None, "ok"),
        (
            _row("1", "D-EXAMPLE-1", kind="discord", adapter="discord-example", active=False),
            None,
            "deployment_inactive",
        ),
        (_row("1", "C-EXAMPLE-1A"), None, "read_failed"),
    ],
)
def test_binding_check_outcomes(row, identities, outcome):
    """@spec TURN-CANARY-8"""
    assert _canary().binding_check_outcome(row, identities, identity_kinds=("slack",)) == outcome


def test_binding_checks_cover_every_unturned_binding_and_read_once_per_kind():
    """@spec TURN-CANARY-8"""
    canary = _canary()
    rows = [
        _row("1", "C-EXAMPLE-1A"),
        _row("1", "C-EXAMPLE-1B"),
        _row("2", "C-EXAMPLE-2A", adapter="blue"),
        _row("2", "D-EXAMPLE-2", kind="discord", adapter="discord-example"),
    ]
    platform = MonitorPlatform(rows)
    result = asyncio.run(
        canary.run_cycle(platform, None, discover=True, binding_checks=True, **FAST)
    )
    turned = {"slack|C-EXAMPLE-1A|default", "slack|C-EXAMPLE-2A|blue"}
    assert set(result.target_last_run) == turned
    assert result.binding_checks == {
        "slack|C-EXAMPLE-1B|default": "ok",
        "discord|D-EXAMPLE-2|discord-example": "ok",
    }
    assert [value for name, value in platform.calls if name == "channel_identities"] == ["slack"]
    assert result.success is True


def test_binding_checks_run_for_all_bindings_while_turns_are_held():
    """@spec TURN-CANARY-8"""
    canary = _canary()
    rows = [_row("1", "C-EXAMPLE-1A"), _row("2", "C-EXAMPLE-2A", adapter="blue")]
    platform = MonitorPlatform(rows)
    platform.cleanup_blocked = True
    result = asyncio.run(
        canary.run_cycle(platform, None, discover=True, binding_checks=True, **FAST)
    )
    assert platform.enqueued() == []
    assert result.cleanup_degraded is True
    assert result.success is False
    assert result.binding_checks == {
        "slack|C-EXAMPLE-1A|default": "ok",
        "slack|C-EXAMPLE-2A|blue": "identity_missing",
    }


def test_binding_check_read_failure_is_reported_without_reference_values():
    """@spec TURN-CANARY-8 TURN-CANARY-6"""
    canary = _canary()
    rows = [_row("1", "C-EXAMPLE-1A"), _row("1", "C-EXAMPLE-1B")]
    platform = MonitorPlatform(rows, identity_error=RuntimeError("secret-ref=env:PRIVATE"))
    result = asyncio.run(
        canary.run_cycle(platform, None, discover=True, binding_checks=True, **FAST)
    )
    assert result.binding_checks == {"slack|C-EXAMPLE-1B|default": "read_failed"}
    exposition = canary.prometheus_text(result)
    assert "PRIVATE" not in repr(result.logs) + exposition
    assert "EXAMPLE_BOT_TOKEN" not in exposition
    assert 'outcome="read_failed"' in exposition


# TURN-CANARY-9: adapter health


@pytest.fixture
def health_server():
    """A local HTTP server with fixed synthetic health routes. @spec TURN-CANARY-9"""
    seen: list[tuple[str, str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler name
            seen.append(("GET", self.path, self.headers.get("Authorization")))
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/ok")
                self.end_headers()
                return
            code = {"/ok": 200, "/down": 503}.get(self.path, 404)
            self.send_response(code)
            self.end_headers()
            self.wfile.write(b"private-body")

        def log_message(self, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", seen
    finally:
        server.shutdown()
        server.server_close()


def _closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_adapter_probe_outcomes_against_local_server(health_server):
    """@spec TURN-CANARY-9"""
    canary = _canary()
    base, seen = health_server

    async def probe_all():
        async with httpx.AsyncClient(timeout=2.0, follow_redirects=False) as client:
            return [
                await canary.probe_adapter(client, base + "/ok"),
                await canary.probe_adapter(client, base + "/down"),
                await canary.probe_adapter(client, base + "/redirect"),
                await canary.probe_adapter(client, f"http://127.0.0.1:{_closed_port()}/ok"),
            ]

    assert asyncio.run(probe_all()) == [
        ("ok", 200),
        ("http_status", 503),
        ("http_status", 302),
        ("unreachable", None),
    ]
    assert [path for _, path, _ in seen] == ["/ok", "/down", "/redirect"]
    assert all(method == "GET" and auth is None for method, _, auth in seen)


@pytest.mark.parametrize(
    "adapters",
    [
        {"Bad_Name": "http://adapter.example.invalid/healthz"},
        {"discord": "ftp://adapter.example.invalid/healthz"},
        {"discord": "http://user:pass@adapter.example.invalid/healthz"},
        {"discord": "http://adapter.example.invalid/healthz#x"},
        {"discord": "/healthz"},
        {f"adapter-{index}": "http://adapter.example.invalid/" for index in range(21)},
    ],
)
def test_adapter_configuration_is_bounded(adapters):
    """@spec TURN-CANARY-9"""
    canary = _canary()
    with pytest.raises(ValueError, match="adapter"):
        canary.parse_config(
            _env(TURN_CANARY_DISCOVER="1", TURN_CANARY_ADAPTER_HEALTH=json.dumps(adapters))
        )


def test_adapter_health_is_reported_per_adapter_without_url_or_cycle_effect():
    """@spec TURN-CANARY-9"""
    canary = _canary()
    up = "http://up-adapter.example.invalid/healthz"
    down = "http://down-adapter.example.invalid/healthz"
    platform = MonitorPlatform(
        [_row("1", "C-EXAMPLE-1A")],
        adapters={up: ("ok", 200), down: ("http_status", 503)},
    )
    result = asyncio.run(
        canary.run_cycle(
            platform, None, discover=True, adapters={"discord": up, "email": down}, **FAST
        )
    )
    assert result.adapter_health == {"discord": "ok", "email": "http_status"}
    assert result.success is True
    exposition = canary.prometheus_text(result)
    assert 'turn_canary_adapter_up{adapter="discord",outcome="ok"} 1' in exposition
    assert 'turn_canary_adapter_up{adapter="email",outcome="http_status"} 0' in exposition
    assert "example.invalid" not in exposition + repr(result.logs)


def test_monitor_options_never_call_a_write_beyond_the_owned_turn():
    """@spec TURN-CANARY-7 TURN-CANARY-8 TURN-CANARY-9"""
    canary = _canary()
    url = "http://adapter.example.invalid/healthz"
    rows = [_row("1", "C-EXAMPLE-1A"), _row("1", "C-EXAMPLE-1B")]
    platform = MonitorPlatform(rows, adapters={url: ("ok", 200)})
    asyncio.run(
        canary.run_cycle(
            platform,
            None,
            discover=True,
            binding_checks=True,
            adapters={"discord": url},
            **FAST,
        )
    )
    names = [name for name, _ in platform.calls]
    assert set(names) <= READS | WRITES
    assert names.count("enqueue") == 1
    assert names.count("reset") == 1


# TURN-CANARY-10: exporter mode and metric prefix


def test_metric_prefix_renames_every_series_and_is_validated():
    """@spec TURN-CANARY-10"""
    canary = _canary()
    result = canary.CycleResult(
        success=True,
        target_success={"slack|C-EXAMPLE-1A|default": True},
        target_last_run={"slack|C-EXAMPLE-1A|default": 1.0},
        binding_checks={"slack|C-EXAMPLE-1B|default": "ok"},
        binding_check_last_run={"slack|C-EXAMPLE-1B|default": 1.0},
        adapter_health={"discord": "ok"},
        rotation=4,
    )
    text = canary.prometheus_text(result, prefix="legacy:agent_canary")
    lines = [line for line in text.splitlines() if line]
    assert lines and all(line.startswith("legacy:agent_canary_") for line in lines)
    assert "legacy:agent_canary_rotation 4" in lines
    assert canary.prometheus_text(result).startswith("turn_canary_")
    with pytest.raises(ValueError, match="prefix"):
        canary.prometheus_text(result, prefix="9bad prefix")
    with pytest.raises(ValueError, match="prefix"):
        canary.parse_config(_env(TURN_CANARY_DISCOVER="1", TURN_CANARY_METRIC_PREFIX="bad-name"))


def _limits(canary, **overrides):
    values = {
        "cycle_interval": 0.05,
        "turn_deadline": 0.01,
        "cleanup_deadline": 0.01,
        "poll_period": 0.005,
        "max_targets": 1,
        **overrides,
    }
    return canary.Limits(**values)


def test_exporter_loop_never_overlaps_and_counts_overruns():
    """@spec TURN-CANARY-10"""
    canary = _canary()
    active = 0
    peak = 0
    starts: list[float] = []

    async def slow_cycle():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        starts.append(asyncio.get_running_loop().time())
        await asyncio.sleep(0.08)
        active -= 1
        return canary.CycleResult(success=True)

    async def scenario():
        monitor = canary.Monitor(slow_cycle, _limits(canary))
        stop = asyncio.Event()
        task = asyncio.create_task(monitor.run_forever(stop))
        await asyncio.sleep(0.3)
        stop.set()
        await asyncio.wait_for(task, 1)
        return monitor

    monitor = asyncio.run(scenario())
    assert peak == 1
    assert len(starts) >= 3
    assert monitor.overruns >= 2
    assert "turn_canary_cycle_overruns_total" in monitor.metrics_text()


def test_exporter_loop_waits_for_the_interval_between_fast_cycles():
    """@spec TURN-CANARY-10"""
    canary = _canary()
    starts: list[float] = []

    async def fast_cycle():
        starts.append(asyncio.get_running_loop().time())
        return canary.CycleResult(success=True)

    async def scenario():
        monitor = canary.Monitor(fast_cycle, _limits(canary, cycle_interval=0.1))
        stop = asyncio.Event()
        task = asyncio.create_task(monitor.run_forever(stop))
        await asyncio.sleep(0.35)
        stop.set()
        await asyncio.wait_for(task, 1)
        return monitor

    monitor = asyncio.run(scenario())
    assert 3 <= len(starts) <= 5
    assert all(later - earlier >= 0.09 for earlier, later in zip(starts, starts[1:], strict=False))
    assert monitor.overruns == 0


def test_exporter_cycle_exception_is_recorded_and_loop_continues():
    """@spec TURN-CANARY-10 TURN-CANARY-6"""
    canary = _canary()
    calls = 0

    async def flaky_cycle():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("token=private-secret")
        return canary.CycleResult(success=True)

    async def scenario():
        monitor = canary.Monitor(flaky_cycle, _limits(canary, cycle_interval=0.02))
        stop = asyncio.Event()
        task = asyncio.create_task(monitor.run_forever(stop))
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, 1)
        return monitor

    monitor = asyncio.run(scenario())
    assert calls >= 2
    assert "private-secret" not in monitor.metrics_text()
    assert "turn_canary_cycle_success 1" in monitor.metrics_text()


def test_exporter_keeps_rotated_series_until_inventory_drops_them():
    """@spec TURN-CANARY-10"""
    canary = _canary()
    monitor = canary.Monitor(None, _limits(canary))
    a, b = "slack|C-EXAMPLE-1A|default", "slack|C-EXAMPLE-1B|default"
    monitor.observe(
        canary.CycleResult(
            target_success={a: True}, target_last_run={a: 1.0}, inventory_labels={a, b}
        )
    )
    monitor.observe(
        canary.CycleResult(
            target_success={b: False}, target_last_run={b: 2.0}, inventory_labels={a, b}
        )
    )
    text = monitor.metrics_text()
    assert "C-EXAMPLE-1A" in text and "C-EXAMPLE-1B" in text
    monitor.observe(canary.CycleResult(inventory_labels=None))
    assert "C-EXAMPLE-1A" in monitor.metrics_text()
    monitor.observe(canary.CycleResult(binding_checks={b: "ok"}, inventory_labels={b}))
    text = monitor.metrics_text()
    assert "C-EXAMPLE-1A" not in text
    assert "C-EXAMPLE-1B" in text


def test_healthz_reports_monitor_staleness_not_platform_health():
    """@spec TURN-CANARY-10"""
    canary = _canary()
    now = [1000.0]
    limits = _limits(canary, cycle_interval=10, turn_deadline=5, cleanup_deadline=5, max_targets=2)
    monitor = canary.Monitor(None, limits, clock=lambda: now[0])
    bound = 2 * 10 + 2 * (5 + 5)
    assert monitor.healthy() is True
    now[0] += bound + 1
    assert monitor.healthy() is False
    monitor.observe(canary.CycleResult(success=False))
    assert monitor.healthy() is True
    now[0] += bound - 1
    assert monitor.healthy() is True
    now[0] += 2
    assert monitor.healthy() is False


def test_exporter_http_routes(tmp_path):
    """@spec TURN-CANARY-10"""
    canary = _canary()

    async def scenario():
        monitor = canary.Monitor(None, _limits(canary), prefix="legacy_canary")
        monitor.observe(canary.CycleResult(success=True))
        server = await canary.serve_http(monitor, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                metrics = await client.get(base + "/metrics")
                health = await client.get(base + "/healthz")
                missing = await client.get(base + "/other")
                post = await client.post(base + "/metrics")
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            try:
                writer.write(b"GET /" + b"x" * 20000 + b" HTTP/1.1\r\n\r\n")
                await writer.drain()
                oversized = await asyncio.wait_for(reader.read(), 2)
            except ConnectionError:
                oversized = b""
            finally:
                writer.close()
        finally:
            server.close()
            await server.wait_closed()
        return metrics, health, missing, post, oversized

    metrics, health, missing, post, oversized = asyncio.run(scenario())
    assert metrics.status_code == 200
    assert metrics.headers["content-type"].startswith("text/plain")
    assert "legacy_canary_cycle_success 1" in metrics.text
    assert health.status_code == 200
    assert missing.status_code == 404
    assert post.status_code == 405
    assert b" 200 " not in oversized


def test_listen_configuration_is_validated():
    """@spec TURN-CANARY-10"""
    canary = _canary()
    config = canary.parse_config(_env(TURN_CANARY_DISCOVER="1", TURN_CANARY_LISTEN="0.0.0.0:9464"))
    assert config.listen == ("0.0.0.0", 9464)
    for bad in ("9464", "host:", "host:70000", "host:port"):
        with pytest.raises(ValueError, match="LISTEN"):
            canary.parse_config(_env(TURN_CANARY_DISCOVER="1", TURN_CANARY_LISTEN=bad))

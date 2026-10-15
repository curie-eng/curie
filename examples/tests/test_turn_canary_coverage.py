"""Contract tests for quota quantities, drift, binding health, coverage and labels.

@spec examples/turn-canary/design.md
"""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest

CANARY = Path(__file__).resolve().parents[1] / "turn-canary" / "turn_canary.py"
FAST = {"turn_deadline": 0.02, "cleanup_deadline": 0.02, "poll_period": 0.005}
REQUIRED = {
    "TURN_CANARY_DATABASE_URL": "postgresql+asyncpg://example.invalid/db",
    "TURN_CANARY_VALKEY_URL": "redis://example.invalid:6379/0",
    "TURN_CANARY_API_URL": "https://platform.example.invalid",
    "TURN_CANARY_API_KEY": "example-key",
    "TURN_CANARY_STREAM": "curie:runs",
    "TURN_CANARY_STATE_PATH": "/tmp/turn-canary-example-state",
    "TURN_CANARY_DISCOVER": "1",
}


def _canary():
    """Load the example module by path. @spec TURN-CANARY-11"""
    spec = importlib.util.spec_from_file_location("turn_canary_coverage_example", CANARY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _row(agent, address, *, kind="slack", adapter="default", active=True, endpoint=""):
    """One synthetic inventory row. @spec TURN-CANARY-11"""
    return {
        "agent_id": f"00000000-0000-4000-8000-00000000000{agent}",
        "agent": f"sample-{agent}",
        "deployment_status": "active" if active else "inactive",
        "kind": kind,
        "address": address,
        "adapter": adapter,
        "endpoint": endpoint,
    }


class Platform:
    """In-memory read boundary; turns always deliver. @spec TURN-CANARY-12"""

    def __init__(self, rows, *, health=None, inventory_error=None):
        self.rows = rows
        self.health = health or {}
        self.inventory_error = inventory_error
        self.probed: list[str] = []
        self.nonces: dict[str, str] = {}
        self.cleanup_blocked = False

    async def inventory(self):
        if self.inventory_error is not None:
            raise self.inventory_error
        return self.rows

    async def read_only_supported(self):
        return True

    async def quota_headroom(self):
        return True

    async def enqueue(self, turn):
        nonce = turn.text.split("no other text: ", 1)[1].split(". Do not", 1)[0]
        self.nonces[turn.reply_handle.placeholder] = nonce

    async def replies(self, reply_ref, after):
        events = [
            {"event": "reply.update", "text": self.nonces[reply_ref]},
            {"event": "turn.completed", "outcome": "delivered"},
        ]
        return {"events": events, "next_cursor": after + len(events), "terminal": True}

    async def reset(self, agent_id, thread_key):
        return None

    async def reset_state(self, agent_id, thread_key):
        return {"requested": False, "route_existed": True}

    async def channel_identities(self, kind):
        return []

    async def adapter_health(self, url):
        self.probed.append(url)
        return self.health.get(url, ("ok", 200))


def _run(canary, platform, **options):
    return asyncio.run(canary.run_cycle(platform, None, discover=True, **FAST, **options))


# TURN-CANARY-5: quota quantities


@pytest.mark.parametrize(
    "raw,expected",
    [("2", 2), ("500m", 0.5), ("1.5", 1.5), ("1k", 1000), ("1Gi", 1024**3), ("3M", 3e6)],
)
def test_quantities_parse_like_kubernetes(raw, expected):
    """@spec TURN-CANARY-5"""
    assert _canary().parse_quantity(raw) == pytest.approx(expected)


@pytest.mark.parametrize("raw", ["", "m", "abc", "1x", "-1", "1 Gi", "1e3", None, 3])
def test_non_quantities_are_refused(raw):
    """@spec TURN-CANARY-5"""
    with pytest.raises(ValueError):
        _canary().parse_quantity(raw)


def _quota(hard, used, resource="limits.cpu"):
    return {"items": [{"status": {"hard": {resource: hard}, "used": {resource: used}}}]}


@pytest.mark.parametrize(
    "hard,used,minimum,expected",
    [
        ("4", "3500m", "500m", True),
        ("4", "3600m", "500m", False),
        ("4", "4", "500m", False),
        ("2Gi", "1Gi", "512Mi", True),
        ("4", "bad", "500m", None),
    ],
)
def test_cpu_quota_headroom_reads_quantities(hard, used, minimum, expected):
    """@spec TURN-CANARY-5"""
    canary = _canary()
    body = _quota(hard, used)
    assert canary.quota_has_headroom(body, "limits.cpu", minimum) is expected


def test_quota_minimum_free_accepts_a_quantity_and_refuses_garbage():
    """@spec TURN-CANARY-5"""
    canary = _canary()
    config = canary.parse_config({**REQUIRED, "TURN_CANARY_QUOTA_MIN_FREE": "500m"})
    assert config.quota_min_free == "500m"
    with pytest.raises(ValueError):
        canary.parse_config({**REQUIRED, "TURN_CANARY_QUOTA_MIN_FREE": "half"})


# TURN-CANARY-11: drift


def test_drift_reports_both_sides_only_for_declared_kinds():
    """@spec TURN-CANARY-11"""
    canary = _canary()
    rows = [
        _row("1", "C-EXAMPLE-1"),
        _row("2", "C-EXAMPLE-2"),
        _row("3", "person@example.invalid", kind="email", adapter="mail"),
    ]
    declared = [("slack", "C-EXAMPLE-1", "default"), ("slack", "C-EXAMPLE-9", "default")]
    result = _run(canary, Platform(rows), declared=declared)
    assert result.drift == {
        "slack|C-EXAMPLE-9|default": "declared_only",
        "slack|C-EXAMPLE-2|default": "live_only",
    }
    assert result.success is True
    text = canary.prometheus_text(result)
    assert "turn_canary_binding_drift_total 2" in text
    assert 'side="declared_only"' in text and 'side="live_only"' in text


def test_failed_inventory_reports_no_drift():
    """@spec TURN-CANARY-11"""
    canary = _canary()
    platform = Platform([], inventory_error=RuntimeError("down"))
    result = _run(canary, platform, declared=[("slack", "C-EXAMPLE-1", "default")])
    assert result.drift is None
    assert "binding_drift" not in canary.prometheus_text(result)


def test_declared_bindings_must_be_route_triples():
    """@spec TURN-CANARY-11"""
    canary = _canary()
    config = canary.parse_config(
        {**REQUIRED, "TURN_CANARY_DECLARED_BINDINGS": '[["slack","C-EXAMPLE-1","default"]]'}
    )
    assert config.declared == [("slack", "C-EXAMPLE-1", "default")]
    with pytest.raises(ValueError):
        canary.parse_config({**REQUIRED, "TURN_CANARY_DECLARED_BINDINGS": '["slack"]'})


# TURN-CANARY-12: binding adapter health


def test_binding_health_probes_each_endpoint_origin_once():
    """@spec TURN-CANARY-12"""
    canary = _canary()
    rows = [
        _row("1", "C-EXAMPLE-1"),
        _row("2", "a@example.invalid", kind="email", adapter="mail",
             endpoint="https://mail.example.invalid:8443/hooks/in"),
        _row("3", "b@example.invalid", kind="email", adapter="mail",
             endpoint="https://mail.example.invalid:8443/hooks/other"),
        _row("4", "c@example.invalid", kind="email", adapter="mail",
             endpoint="https://down.example.invalid/in"),
        _row("5", "d@example.invalid", kind="email", adapter="mail",
             endpoint="https://user:pw@mail.example.invalid/in"),
        _row("6", "e@example.invalid", kind="email", adapter="mail",
             endpoint="https://mail.example.invalid/in", active=False),
    ]
    platform = Platform(rows, health={"https://down.example.invalid/healthz": ("http_status", 503)})
    result = _run(canary, platform, binding_health_path="/healthz")
    assert sorted(platform.probed) == [
        "https://down.example.invalid/healthz",
        "https://mail.example.invalid:8443/healthz",
    ]
    assert result.binding_adapter_health == {
        "email|a@example.invalid|mail": "ok",
        "email|b@example.invalid|mail": "ok",
        "email|c@example.invalid|mail": "http_status",
        "email|d@example.invalid|mail": "endpoint_invalid",
    }
    text = canary.prometheus_text(result)
    assert "turn_canary_binding_adapter_up" in text
    assert "example.invalid:8443" not in text and "healthz" not in text
    assert result.success is True


def test_binding_health_path_must_be_absolute():
    """@spec TURN-CANARY-12"""
    canary = _canary()
    config = canary.parse_config({**REQUIRED, "TURN_CANARY_BINDING_ADAPTER_HEALTH": "/healthz"})
    assert config.binding_health_path == "/healthz"
    for bad in ("healthz", "/healthz?x=1", "https://x.example.invalid/healthz"):
        with pytest.raises(ValueError):
            canary.parse_config({**REQUIRED, "TURN_CANARY_BINDING_ADAPTER_HEALTH": bad})


# TURN-CANARY-13: coverage


def _coverage_rows():
    return [
        _row("1", "C-EXAMPLE-1"),
        _row("2", "a@example.invalid", kind="email", adapter="mail",
             endpoint="https://mail.example.invalid/in"),
        _row("3", "b@example.invalid", kind="email", adapter="mail"),
        _row("4", "C-EXAMPLE-4", active=False),
    ]


def test_coverage_names_active_agents_the_monitor_cannot_observe():
    """@spec TURN-CANARY-13"""
    canary = _canary()
    result = _run(canary, Platform(_coverage_rows()), coverage=True)
    assert result.uncovered_agents == ["sample-2", "sample-3"]
    with_health = _run(
        canary, Platform(_coverage_rows()), coverage=True, binding_health_path="/healthz"
    )
    assert with_health.uncovered_agents == ["sample-3"]
    text = canary.prometheus_text(with_health)
    assert 'turn_canary_agent_uncovered{agent="sample-3"} 1' in text
    assert "turn_canary_uncovered_agents 1" in text
    assert with_health.success is True


def test_coverage_is_absent_when_disabled_or_inventory_fails():
    """@spec TURN-CANARY-13"""
    canary = _canary()
    assert _run(canary, Platform(_coverage_rows())).uncovered_agents is None
    failed = _run(canary, Platform([], inventory_error=RuntimeError("down")), coverage=True)
    assert failed.uncovered_agents is None
    assert "uncovered" not in canary.prometheus_text(failed)


# TURN-CANARY-14: label layout


def test_split_labels_carry_agent_and_route_coordinates():
    """@spec TURN-CANARY-14"""
    canary = _canary()
    rows = [_row("1", "C-EXAMPLE-1")]
    result = _run(canary, Platform(rows), declared=[("slack", "C-EXAMPLE-9", "default")])
    text = canary.prometheus_text(result, labels="split")
    assert (
        'turn_canary_target_success{agent="sample-1",kind="slack",'
        'address="C-EXAMPLE-1",adapter="default"} 1'
    ) in text
    assert (
        'agent="",kind="slack",address="C-EXAMPLE-9",adapter="default",side="declared_only"'
    ) in text
    assert "route=" not in text
    assert "route=" in canary.prometheus_text(result)


def test_route_label_layout_is_validated():
    """@spec TURN-CANARY-14"""
    canary = _canary()
    split = canary.parse_config({**REQUIRED, "TURN_CANARY_ROUTE_LABELS": "split"})
    assert split.route_labels == "split"
    assert canary.parse_config(REQUIRED).route_labels == "route"
    with pytest.raises(ValueError):
        canary.parse_config({**REQUIRED, "TURN_CANARY_ROUTE_LABELS": "flat"})
    with pytest.raises(ValueError):
        canary.prometheus_text(canary.CycleResult(), labels="flat")


def test_monitor_replaces_drift_and_coverage_each_successful_inventory():
    """@spec TURN-CANARY-10 TURN-CANARY-11 TURN-CANARY-13"""
    canary = _canary()
    monitor = canary.Monitor(None, canary.Limits(), prefix="turn_canary", labels="split")
    first = _run(canary, Platform(_coverage_rows()), coverage=True,
                 declared=[("slack", "C-EXAMPLE-9", "default")])
    monitor.observe(first)
    second = _run(canary, Platform([_row("1", "C-EXAMPLE-1")]), coverage=True,
                  declared=[("slack", "C-EXAMPLE-1", "default")])
    monitor.observe(second)
    text = monitor.metrics_text()
    assert "turn_canary_binding_drift_total 0" in text
    assert "turn_canary_uncovered_agents 0" in text
    assert "sample-3" not in text
    failed = _run(canary, Platform([], inventory_error=RuntimeError("down")), coverage=True,
                  declared=[("slack", "C-EXAMPLE-1", "default")])
    monitor.observe(failed)
    assert "turn_canary_binding_drift_total 0" in monitor.metrics_text()


def test_runtime_quota_read_honors_cpu_quantities():
    """@spec TURN-CANARY-5"""
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    canary = _canary()
    bodies = {"/roomy": ("4", "3"), "/tight": ("4", "3800m")}
    seen: list[str | None] = []

    class Quota(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler name
            seen.append(self.headers.get("Authorization"))
            hard, used = bodies[self.path]
            body = json.dumps(_quota(hard, used)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Quota)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    async def headroom(path: str) -> bool | None:
        platform = canary.RuntimePlatform(
            database_url="postgresql+asyncpg://canary@db.example.invalid/curie",
            valkey_url="redis://valkey.example.invalid:6379/0",
            api_url="https://platform.example.invalid",
            api_key="example-key",
            stream="curie:runs",
            schema="curie",
            read_only_qualified=True,
            quota_url=base + path,
            quota_token="example-token",
            quota_resource="limits.cpu",
            quota_min_free="500m",
        )
        try:
            return await platform.quota_headroom()
        finally:
            await platform.close()

    try:
        assert asyncio.run(headroom("/roomy")) is True
        assert asyncio.run(headroom("/tight")) is False
    finally:
        server.shutdown()
        server.server_close()
    assert seen == ["Bearer example-token", "Bearer example-token"]

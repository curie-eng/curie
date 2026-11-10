"""Contract tests for quota quantities, drift, binding health, coverage and labels.

@spec examples/turn-canary/design.md
"""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest

from examples.tests.test_turn_canary import reply_events

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
        self.turns = {}
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
        self.turns[turn.reply_handle.placeholder] = turn

    async def replies(self, reply_ref, after):
        events = reply_events(self.turns[reply_ref], self.nonces[reply_ref])
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



def test_split_labels_cover_every_per_route_series():
    """@spec TURN-CANARY-14"""
    canary = _canary()
    result = _run(canary, Platform([_row("1", "C-EXAMPLE-1")]))
    text = canary.prometheus_text(result, labels="split")
    route_series = [line for line in text.splitlines() if "C-EXAMPLE-1" in line]
    assert route_series
    assert all('agent="sample-1",kind="slack"' in line for line in route_series), route_series


def test_monitor_takes_agent_names_from_the_latest_inventory_only():
    """@spec TURN-CANARY-10 TURN-CANARY-14"""
    canary = _canary()
    monitor = canary.Monitor(None, canary.Limits(), prefix="turn_canary", labels="split")
    declared = [("slack", "C-EXAMPLE-1", "default"), ("slack", "C-EXAMPLE-2", "default")]
    both = [_row("1", "C-EXAMPLE-1"), _row("2", "C-EXAMPLE-2")]
    monitor.observe(_run(canary, Platform(both), declared=declared))
    monitor.observe(_run(canary, Platform([_row("1", "C-EXAMPLE-1")]), declared=declared))
    text = monitor.metrics_text()
    assert (
        'agent="",kind="slack",address="C-EXAMPLE-2",adapter="default",side="declared_only"'
    ) in text
    assert "sample-2" not in text


# TURN-CANARY-15: binding kind exclusion


def _with_test_bindings():
    return [
        _row("1", "C-EXAMPLE-1"),
        _row("2", "probe-2", kind="qualification", adapter="probe",
             endpoint="https://probe.example.invalid/in"),
        _row("3", "C-EXAMPLE-3"),
        _row("3", "probe-3", kind="qualification", adapter="probe",
             endpoint="https://probe.example.invalid/in"),
    ]


def test_excluded_kinds_are_never_probed_checked_compared_or_uncovered():
    """@spec TURN-CANARY-15"""
    canary = _canary()
    platform = Platform(_with_test_bindings())
    result = _run(
        canary,
        platform,
        exclude_kinds=("qualification",),
        binding_checks=True,
        binding_health_path="/healthz",
        coverage=True,
        declared=[("slack", "C-EXAMPLE-1", "default"), ("qualification", "probe-9", "probe")],
    )
    assert platform.probed == []
    assert result.uncovered_agents == []
    assert not any(label.startswith("qualification|") for label in result.binding_checks)
    assert not any(label.startswith("qualification|") for label in result.binding_adapter_health)
    assert result.drift == {"slack|C-EXAMPLE-3|default": "live_only"}
    assert all(not label.startswith("qualification|") for label in result.inventory_labels)
    assert sorted(result.target_success) == [
        "slack|C-EXAMPLE-1|default",
        "slack|C-EXAMPLE-3|default",
    ]
    assert result.success is True


def test_without_exclusion_test_bindings_are_observed_as_before():
    """@spec TURN-CANARY-15"""
    canary = _canary()
    platform = Platform(_with_test_bindings())
    result = _run(canary, platform, binding_health_path="/healthz", coverage=True)
    assert platform.probed == ["https://probe.example.invalid/healthz"]
    assert result.uncovered_agents == []
    assert result.binding_adapter_health["qualification|probe-2|probe"] == "ok"


def test_an_agent_with_only_excluded_bindings_is_not_uncovered():
    """@spec TURN-CANARY-15"""
    canary = _canary()
    rows = [_row("1", "C-EXAMPLE-1"), _row("2", "probe-2", kind="qualification", adapter="")]
    assert _run(canary, Platform(rows), coverage=True).uncovered_agents == ["sample-2"]
    excluded = _run(canary, Platform(rows), coverage=True, exclude_kinds=("qualification",))
    assert excluded.uncovered_agents == []


def test_excluding_slack_turns_nothing_and_fails_discovery_visibly():
    """@spec TURN-CANARY-7 TURN-CANARY-15"""
    canary = _canary()
    result = _run(canary, Platform([_row("1", "C-EXAMPLE-1")]), exclude_kinds=("slack",))
    assert result.target_success == {}
    assert result.success is False
    assert {"phase": "discovery", "outcome": "empty"} in result.logs


def test_exclusion_list_is_parsed_strictly():
    """@spec TURN-CANARY-15"""
    canary = _canary()
    assert canary.parse_config(REQUIRED).exclude_kinds == ()
    parsed = canary.parse_config(
        {**REQUIRED, "TURN_CANARY_EXCLUDE_BINDING_KINDS": "qualification, webhook"}
    )
    assert parsed.exclude_kinds == ("qualification", "webhook")
    for bad in (",", "qualification,", "qualification,qualification", " "):
        with pytest.raises(ValueError):
            canary.parse_config({**REQUIRED, "TURN_CANARY_EXCLUDE_BINDING_KINDS": bad})

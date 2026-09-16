"""Discovery health reaches readiness (#2731).

The soak adapter failed every AgentMail list call for three and a half hours
while its Deployment stayed Available, because `/readyz` never looked at
discovery. These tests pin the new discovery state, the threshold that turns a
failure run into "unreachable", the 503 it produces on `/readyz`, liveness
staying 200, and the single ERROR/INFO pair the poll loop logs at the edges.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

import curie_mail_adapter.adapter as adapter_module
import pytest
from _support import IngressState, MailState, get, wait_until
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.config import MailAdapterConfig

# -- config ------------------------------------------------------------------


def test_threshold_defaults_to_120(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS", raising=False)
    assert MailAdapterConfig().discovery_unready_after_seconds == 120.0


def test_threshold_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS", "45.5")
    assert MailAdapterConfig().discovery_unready_after_seconds == 45.5


@pytest.mark.parametrize("value", ["0", "-1"])
def test_non_positive_threshold_is_a_boot_problem(
    make_config: Callable[..., MailAdapterConfig], value: str
) -> None:
    from curie_mail_adapter.run import boot_problems

    config = make_config(discovery_unready_after_seconds=float(value))
    problems = boot_problems(config)
    assert any("CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS" in p for p in problems), problems
    assert not any(
        "CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS" in p for p in boot_problems(make_config())
    )


# -- adapter state and HTTP surface -------------------------------------------


def _ready_adapter(make_adapter: Callable[..., MailAdapter], **overrides: Any) -> MailAdapter:
    instance = make_adapter(**overrides)
    instance.startup()
    assert instance.ready.is_set()
    return instance


def test_failures_past_threshold_make_readiness_503_liveness_200(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    adapter = _ready_adapter(make_adapter, discovery_unready_after_seconds=120.0)
    url = serve_egress(adapter)
    try:
        adapter.record_discovery(0, now=0.0)
        discovery = adapter.status()["discovery"]
        assert discovery["state"] == "failing"
        assert discovery["consecutive_failures"] == 1
        assert get(url + "/readyz")[0] == 200, "one failure inside the threshold flapped readiness"

        adapter.record_discovery(0, now=121.0)
        discovery = adapter.status()["discovery"]
        assert discovery["state"] == "unreachable"
        assert discovery["consecutive_failures"] == 2
        assert discovery["failing_for_seconds"] == pytest.approx(121.0)
        assert get(url + "/readyz")[0] == 503
        assert get(url + "/healthz")[0] == 200, "liveness must not follow discovery"

        adapter.record_discovery(200, now=122.0)
        assert adapter.status()["discovery"] == {
            "state": "ok",
            "consecutive_failures": 0,
            "failing_for_seconds": 0.0,
        }
        assert get(url + "/readyz")[0] == 200
    finally:
        adapter.shutdown.set()


def test_initial_discovery_state_is_ok(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    adapter = _ready_adapter(make_adapter)
    try:
        assert adapter.status()["discovery"] == {
            "state": "ok",
            "consecutive_failures": 0,
            "failing_for_seconds": 0.0,
        }
    finally:
        adapter.shutdown.set()


def test_server_errors_count_as_discovery_failures(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    adapter = _ready_adapter(make_adapter, discovery_unready_after_seconds=120.0)
    url = serve_egress(adapter)
    try:
        adapter.record_discovery(503, now=10.0)
        adapter.record_discovery(500, now=140.0)
        discovery = adapter.status()["discovery"]
        assert discovery["state"] == "unreachable"
        assert discovery["consecutive_failures"] == 2
        assert discovery["failing_for_seconds"] == pytest.approx(130.0)
        assert get(url + "/readyz")[0] == 503
    finally:
        adapter.shutdown.set()


# -- poll loop wiring ---------------------------------------------------------


class _SwitchableClient:
    """Answers every call over no network; `down` makes the listing refuse."""

    def __init__(self) -> None:
        self.down = False
        self.lists = 0

    def list_messages(self, limit: int, page_token: str | None = None) -> tuple[int, Any]:
        self.lists += 1
        if self.down:
            return 0, {"error": "[Errno 111] Connection refused"}
        return 200, {"messages": []}

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"unexpected provider call {name}")


def test_poll_loop_drives_discovery_readiness_and_logs_edges_once(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(adapter_module, "BACKOFF_STEP_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "BACKOFF_MAX_SECONDS", 0.02)
    client = _SwitchableClient()
    adapter = make_adapter(
        client=client, poll_interval_seconds=0.01, discovery_unready_after_seconds=0.01
    )
    url = serve_egress(adapter)
    thread = threading.Thread(target=adapter.poll_loop, daemon=True)

    def errors() -> list[str]:
        return [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.ERROR and "discovery unreachable" in r.getMessage()
        ]

    def recoveries() -> list[str]:
        return [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.INFO and "discovery recovered" in r.getMessage()
        ]

    with caplog.at_level(logging.INFO, logger="curie_mail_adapter"):
        try:
            thread.start()
            assert adapter.ready.wait(10), "startup never completed"
            assert get(url + "/readyz")[0] == 200

            client.down = True
            assert wait_until(
                lambda: adapter.status()["discovery"]["state"] == "unreachable", timeout=10
            ), adapter.status()
            failures_at_unready = client.lists
            assert wait_until(lambda: client.lists >= failures_at_unready + 3, timeout=10)
            assert get(url + "/readyz")[0] == 503
            assert get(url + "/healthz")[0] == 200
            assert adapter.status()["discovery"]["consecutive_failures"] >= 2
            assert len(errors()) == 1, errors()

            client.down = False
            assert wait_until(lambda: adapter.status()["discovery"]["state"] == "ok", timeout=10), (
                adapter.status()
            )
            assert get(url + "/readyz")[0] == 200
            assert adapter.status()["discovery"]["consecutive_failures"] == 0
            assert wait_until(lambda: len(recoveries()) == 1, timeout=5), recoveries()
        finally:
            adapter.shutdown.set()
            thread.join(timeout=10)
    assert not thread.is_alive()
    assert len(errors()) == 1, errors()
    assert len(recoveries()) == 1, recoveries()

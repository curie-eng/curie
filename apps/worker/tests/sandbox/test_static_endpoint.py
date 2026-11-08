"""The runner endpoint on routes: transport and base path, and stored-route migration."""

from __future__ import annotations

import json
import ssl
from dataclasses import asdict

import pytest
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox.static import parse_static_shells
from curie_worker.sandbox.types import RouteRecord, RouteState, SandboxHandle


def _handle(**overrides: object) -> SandboxHandle:
    fields: dict[str, object] = {
        "thread_key": "t",
        "claim_name": "curie-thread-abc-1",
        "sandbox_name": "curie-thread-abc-1",
        "namespace": "default",
        "service_fqdn": "shell-0.example.internal",
        "port": 8443,
        "session_id": "s",
        "token": "bearer",
        **overrides,
    }
    return SandboxHandle(**fields)  # type: ignore[arg-type]


def test_an_https_handle_dials_its_supervisor_path() -> None:
    handle = _handle(transport="https", base_path="/c/curie-thread-abc-1/7")
    assert handle.base_url == "https://shell-0.example.internal:8443/c/curie-thread-abc-1/7"
    assert RouteRecord.from_json(RouteRecord(handle).to_json()).handle == handle


def test_a_route_stored_before_the_endpoint_fields_reads_as_http_at_the_root() -> None:
    legacy = asdict(_handle(service_fqdn="sb.default.svc.cluster.local", port=8080))
    del legacy["transport"], legacy["base_path"]
    legacy["state"] = RouteState.SUSPENDED.value
    record = RouteRecord.from_json(json.dumps(legacy))
    assert record.state is RouteState.SUSPENDED
    assert record.handle.base_url == "http://sb.default.svc.cluster.local:8080"


@pytest.mark.parametrize(
    "overrides",
    [
        {"transport": "ftp"},
        {"service_fqdn": "https://shell-0"},
        {"service_fqdn": "shell-0/c"},
        {"base_path": "c/x"},
        {"base_path": "/c/x/"},
        {"base_path": "/c?x=1"},
    ],
)
def test_an_endpoint_that_would_compose_another_url_is_refused(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _handle(**overrides)


def test_a_runner_client_refuses_a_tls_context_that_skips_verification() -> None:
    context = ssl.create_default_context()
    context.check_hostname = False
    with pytest.raises(ValueError, match="host name"):
        RunnerClient(ssl_context=context)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "[]",
        '[{"slot": "a", "host": "h", "pool": "p"}, {"slot": "a", "host": "i", "pool": "p"}]',
        '[{"slot": "a", "host": "https://h", "pool": "p"}]',
        '[{"slot": "a", "host": "h", "pool": "p", "port": 0}]',
        '[{"slot": "a", "host": "h", "pool": "P!"}]',
        '[{"slot": "a", "host": "h", "pool": "p", "token": "x"}]',
    ],
)
def test_a_malformed_shell_inventory_is_refused(raw: str | None) -> None:
    with pytest.raises(ValueError):
        parse_static_shells(raw)


def test_the_shell_inventory_defaults_to_the_supervisor_port() -> None:
    (slot,) = parse_static_shells('[{"slot": "sre-0", "host": "shell.internal", "pool": "p"}]')
    assert (slot.slot, slot.host, slot.port, slot.pool) == ("sre-0", "shell.internal", 8443, "p")

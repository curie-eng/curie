"""Actual TLS evidence observer confinement, @spec COMMISSIONING-SERIES-3."""

from __future__ import annotations

import importlib
import importlib.util
from typing import Any

import pytest
from redis.exceptions import NoPermissionError

from . import worker_broker as wb

pworker_service = wb.pworker_service
FAMILIES = ("permit", "journal", "reservation", "ownership", "admission")


def module() -> Any:
    name = "curie_protected_hooks.commissioning_evidence_acl"
    assert importlib.util.find_spec(name) is not None, "evidence observer recipe not implemented"
    return importlib.import_module(name)


def test_recipe_is_explicit_and_does_not_enable_the_v1_role() -> None:
    from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

    rules = module().evidence_observer_acl_rules()
    assert type(rules) is tuple and all(type(rule) is str for rule in rules)
    with pytest.raises(ValueError):
        commissioning_acl_rules("evidence_observer")


@pytest.fixture
def observer(pworker_service):
    principal = wb.WorkerPrincipal(
        pworker_service,
        role="series-evidence",
        rules=module().evidence_observer_acl_rules(),
    )
    principal.install()
    client = principal.raw()
    try:
        yield principal, client
    finally:
        client.close()
        principal.remove()


def test_observer_reads_real_control_state_and_connection_presence(
    pworker_service, observer
) -> None:
    principal, client = observer
    for family in FAMILIES:
        key = "protected:commissioning:" + family + ":example"
        pworker_service.command("SET", key, "independent-record")
        assert client.execute_command("GET", key) == b"independent-record"
        assert client.execute_command("TYPE", key) == b"string"
    assert client.execute_command("ACL", "WHOAMI") == principal.username.encode()
    assert client.execute_command("INFO", "server")["valkey_version"] == "8.1.10"
    assert len(client.execute_command("TIME")) == 2
    inventory = client.execute_command("CLIENT", "LIST").decode().splitlines()
    assert any("user=" + principal.username in line.split() for line in inventory)


@pytest.mark.parametrize("family", FAMILIES)
def test_observer_cannot_change_any_readable_record(pworker_service, observer, family: str) -> None:
    _, client = observer
    key = "protected:commissioning:" + family + ":immutable"
    pworker_service.command("SET", key, "original")
    for command in (("SET", key, "changed"), ("DEL", key), ("EXPIRE", key, "1")):
        with pytest.raises(NoPermissionError):
            client.execute_command(*command)
    assert pworker_service.command("GET", key) == b"original"


@pytest.mark.parametrize(
    "key",
    [
        "protected:commissioning:acceptance:example",
        "protected:commissioning:lane:example:runs",
        "protected:commissioning:journal-escape:example",
        "protected:control:selection:example",
        "protected:control:qualification:example:1",
        "protected:lane:example:runs",
        "curie:runs",
    ],
)
@pytest.mark.parametrize("verb", ["GET", "TYPE"])
def test_observer_reads_no_acceptance_production_or_unlisted_family(
    observer, key: str, verb: str
) -> None:
    _, client = observer
    with pytest.raises(NoPermissionError):
        client.execute_command(verb, key)


@pytest.mark.parametrize(
    "command",
    [
        (
            "EVAL",
            "return redis.call('GET', KEYS[1])",
            "1",
            "protected:commissioning:journal:example",
        ),
        ("SCAN", "0"),
        ("KEYS", "*"),
        ("PUBLISH", "example-channel", "message"),
        ("ACL", "GETUSER", "default"),
        ("ACL", "USERS"),
        ("ACL", "SETUSER", "never-created", "off"),
        ("CLIENT", "ID"),
        ("CLIENT", "KILL", "ID", "0", "SKIPME", "yes"),
        ("CONFIG", "GET", "*"),
        ("INFO", "all"),
        ("SELECT", "1"),
    ],
)
def test_observer_cannot_scan_script_publish_administer_or_change_database(
    observer, command
) -> None:
    _, client = observer
    with pytest.raises(NoPermissionError):
        client.execute_command(*command)

"""The ACL principal state check against the pinned Valkey, design decision 7.

@spec PROTECTED-COMMISSIONING-5. Real TLS broker, product recipes, real clients.
The reader below is a test adapter only; it proves that the observer identity
check and the state comparison hold against what the broker actually reports.
Nothing here observes an installation or is campaign evidence.
"""

from __future__ import annotations

import asyncio
import copy

import pytest
from curie_protected_hooks.admission_acl import admission_acl_rules
from curie_protected_hooks.commissioning_acl import commissioning_acl_rules
from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
from curie_protected_hooks.commissioning_producers import (
    ACL_PRINCIPALS,
    AclBaseline,
    AclPrincipalStateProducer,
    PhaseExpectation,
    ProducerRefused,
    parse_getuser_reply,
)
from curie_protected_hooks.worker_acl import worker_acl_rules
from redis.exceptions import NoPermissionError

from . import worker_broker as wb
from .test_commissioning_acl import permit
from .test_commissioning_bootstrap import bootstrap_value, encoded

pworker_service = wb.pworker_service
CAMPAIGN = PhaseExpectation("campaign", {})


class BrokerReader:
    """Test adapter: one RESP3 session as some principal."""

    def __init__(self, client) -> None:
        self.client = client

    async def getuser(self, username: str) -> object:
        return self.client.execute_command("ACL", "GETUSER", username)

    async def key_read_denied(self, key: str) -> bool:
        try:
            self.client.get(key)
        except NoPermissionError:
            return True
        return False


def _rules(name: str) -> tuple[str, ...]:
    if name in ("enqueue", "verifier"):
        return admission_acl_rules(name)
    if name == "worker":
        return worker_acl_rules("worker")
    if name.startswith("candidate_"):
        return commissioning_acl_rules(name, permit=permit())
    return commissioning_acl_rules(name)


@pytest.fixture
def installed(pworker_service):
    """The seven checked principals, the provisioner, the observer and an admin user."""
    users = {}
    try:
        for name in (*ACL_PRINCIPALS, "provisioner", "observer"):
            users[name] = wb.WorkerPrincipal(
                pworker_service, role=name.replace("_", "-"), rules=_rules(name)
            )
            users[name].install()
        users["admin"] = wb.WorkerPrincipal(
            pworker_service, role="admin", rules=("+@all", "~*", "&*")
        )
        users["admin"].install()
        yield users
    finally:
        for user in users.values():
            user.remove()


def _producer(service, users, reader_role):
    value = copy.deepcopy(bootstrap_value())
    for name in ("enqueue", "worker", "verifier"):
        value["production_credentials"][name]["id"] = users[name].username
    value["issuer_identity"]["id"] = users["issuer"].username
    value["control_identity"]["id"] = users["control"].username
    value["candidate_credentials"]["enqueue"]["id"] = users["candidate_enqueue"].username
    value["candidate_credentials"]["worker"]["id"] = users["candidate_worker"].username
    bootstrap = parse_commissioning_bootstrap(encoded(value))
    # The administrator-run installation step records the baseline.
    states = {
        name: parse_getuser_reply(service.command("ACL", "GETUSER", users[name].username))
        for name in ACL_PRINCIPALS
    }
    client = users[reader_role].raw(protocol=3)
    producer = AclPrincipalStateProducer(
        reader=BrokerReader(client),
        baseline=AclBaseline(bootstrap, states),
        observer_username=users["observer"].username,
        provisioner_username=users["provisioner"].username,
    )
    return producer, client


def test_the_observer_passes_and_a_disabled_principal_then_refuses(pworker_service, installed):
    producer, client = _producer(pworker_service, installed, "observer")
    try:
        asyncio.run(producer.require(CAMPAIGN))
        asyncio.run(producer.require(CAMPAIGN))
        pworker_service.command("ACL", "SETUSER", installed["candidate_worker"].username, "off")
        with pytest.raises(ProducerRefused):
            asyncio.run(producer.require(CAMPAIGN))
        # The same observed state is what the activated phase expects.
        asyncio.run(producer.require(PhaseExpectation("activated", {"candidate_worker": False})))
    finally:
        client.close()


@pytest.mark.parametrize("role", ["provisioner", "admin", "control", "worker"])
def test_a_reader_authenticated_as_anyone_but_the_observer_refuses(
    pworker_service, installed, role
):
    # The provisioner and every principal are refused GETUSER; admin reads keys.
    producer, client = _producer(pworker_service, installed, role)
    try:
        with pytest.raises(ProducerRefused):
            asyncio.run(producer.require(CAMPAIGN))
    finally:
        client.close()


@pytest.mark.parametrize(
    "change", [("+acl|whoami",), ("~protected:control:*",), ("&*",), ("+get",), ("nopass",)]
)
def test_an_observer_whose_rules_drifted_from_the_recipe_refuses(
    pworker_service, installed, change
):
    producer, client = _producer(pworker_service, installed, "observer")
    try:
        pworker_service.command("ACL", "SETUSER", installed["observer"].username, *change)
        with pytest.raises(ProducerRefused):
            asyncio.run(producer.require(CAMPAIGN))
    finally:
        client.close()

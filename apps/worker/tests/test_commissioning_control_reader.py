"""Provisioner observer composition over an external Kubernetes command transport."""

from __future__ import annotations

import asyncio
import json
import subprocess
from copy import deepcopy

import pytest
from curie_protected_hooks.commissioning_local_installation import LocalCluster
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger

from .test_commissioning_control_observer import INSTALLED, PROFILE, ROSTER, Reader
from .test_commissioning_control_profile import projection

OWNER = "curie-commission-0123456789ab"


def product():
    from curie_worker import commissioning_control_reader

    return commissioning_control_reader


class KubernetesCommands:
    """External command boundary; all observer semantics run in production code."""

    def __init__(self):
        self.reader = Reader()
        self.calls = []
        self.fail = False
        self.list_override = None

    def run(self, argv, *, stdin=None, timeout):
        self.calls.append((argv, timeout))
        if self.fail:
            raise subprocess.TimeoutExpired(argv, timeout)
        if "version" in argv:
            return json.dumps({"serverVersion": self.reader.version()}).encode()
        resource = argv[argv.index("get") + 1]
        if resource == "pods":
            value = {"items": deepcopy([self.reader.guard, *self.reader.extra_pods])}
        elif resource == "endpointslices.discovery.k8s.io":
            value = {"items": self.reader.endpoint_slices("acme-control", "acme-guard")}
        else:
            value = next(
                (
                    v
                    for key, v in self.reader.objects.items()
                    if key.rsplit("/", 3)[1].lower() + "s" == resource.split(".")[0].split("/")[0]
                    and key.rsplit("/", 1)[1] == resource.rsplit("/", 1)[1]
                ),
                None,
            )
        if self.list_override is not None and resource in {
            "pods",
            "endpointslices.discovery.k8s.io",
        }:
            value = self.list_override
        return b"" if value is None else json.dumps(value).encode()


def setup(tmp_path):
    ledger = OwnershipLedger.create(tmp_path / "ledger", owner=OWNER)
    roster = deepcopy(ROSTER)
    commands = KubernetesCommands()
    for document, (key, uid) in zip(roster, INSTALLED.uids, strict=True):
        document["metadata"]["labels"] = {"commissioning.curietech.ai/owner": OWNER}
        commands.reader.objects[key]["metadata"]["labels"].update(document["metadata"]["labels"])
        # Rendered fixture's earlier owner label is replaced as well.
        for label in (
            set(commands.reader.objects[key]["metadata"]["labels"])
            - set(document["metadata"]["labels"])
            - {"kubernetes.io/metadata.name"}
        ):
            del commands.reader.objects[key]["metadata"]["labels"][label]
        ledger.bind(ledger.intend("kubernetes", key), owner=OWNER, uid=uid)
    return ledger, roster, commands, LocalCluster(ledger, runner=commands)


def test_observer_uses_only_ledger_uids_and_fresh_bounded_reads(tmp_path):
    api = product()
    ledger, roster, commands, cluster = setup(tmp_path)
    observer = api.control_observer(
        cluster, profile=PROFILE, projection=projection(), roster=tuple(roster)
    )
    first = asyncio.run(observer.observe())
    count = len(commands.calls)
    assert asyncio.run(observer.observe()) == first
    assert len(commands.calls) == 2 * count
    assert all(0 < timeout <= 4 for _, timeout in commands.calls)
    assert all("--context" in argv and "--kubeconfig" in argv for argv, _ in commands.calls)
    key = INSTALLED.uids[0][0]
    commands.reader.objects[key]["metadata"]["uid"] = "replaced"
    with pytest.raises(Exception, match="Protected control installation refused"):
        asyncio.run(observer.observe())


@pytest.mark.parametrize("state", ["missing", "intended", "uncertain", "removed", "foreign"])
def test_incomplete_or_foreign_ownership_never_becomes_installed_control(tmp_path, state):
    api = product()
    ledger = OwnershipLedger.create(tmp_path / "ledger", owner=OWNER)
    for i, (key, uid) in enumerate(INSTALLED.uids):
        if i == 0 and state == "missing":
            continue
        ticket = ledger.intend("kubernetes", key)
        if i == 0 and state in {"intended", "uncertain"}:
            if state == "uncertain":
                ledger.uncertain("kubernetes", key)
            continue
        ledger.bind(ticket, owner="other" if i == 0 and state == "foreign" else OWNER, uid=uid)
        if i == 0 and state == "removed":
            ledger.removed("kubernetes", key)
    with pytest.raises(Exception, match="Protected control installation refused"):
        roster = deepcopy(ROSTER)
        for document in roster:
            document["metadata"]["labels"] = {"commissioning.curietech.ai/owner": OWNER}
        api.installed_control(ledger, roster)


def test_transport_timeout_and_malformed_lists_refuse_without_output(tmp_path):
    api = product()
    _, _, commands, cluster = setup(tmp_path)
    reader = api.KubernetesControlReader(cluster)
    commands.fail = True
    with pytest.raises(Exception, match="Protected control installation refused"):
        reader.version()
    commands.fail = False
    commands.reader.server = {"gitVersion": "v1.35.1"}
    assert reader.version() == commands.reader.server
    with pytest.raises(Exception, match="Protected control installation refused"):
        reader.read("/tmp/not-an-object")


@pytest.mark.parametrize(
    "reply", [{"items": None}, {"items": [None]}, {"items": [], "metadata": {"continue": "more"}}]
)
def test_partial_or_malformed_list_is_never_a_complete_observation(tmp_path, reply):
    api = product()
    _, _, commands, cluster = setup(tmp_path)
    commands.list_override = reply
    with pytest.raises(Exception, match="Protected control installation refused"):
        api.KubernetesControlReader(cluster).endpoint_slices("acme-control", "acme-guard")

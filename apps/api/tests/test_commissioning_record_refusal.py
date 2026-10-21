"""API protected callers refuse commissioning records, @spec PROTECTED-COMMISSIONING-4.

The support probe, signed hook ingress, the admission reconciler and the API
runtime file loader each meet one valid commissioning record
(``packages/protected-hooks/tests/commissioning_record_vectors.py``) where they
read a production record. Every other record stays genuine production, so each
case is a mixed production and commissioning reference set. Each caller keeps
its production refusal: no positive support answer, receipt, stream entry,
commit, broker write or ordinary effect. The broker cases run the real API
(migrated Postgres, the app's Valkey) against the owned disposable TLS Valkey of
the protected hooks package; see ``_protected_ingress_harness``.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from _protected_ingress_harness import (
    _broker,
    admission_key,
    assert_detail,
    broker_snapshot,
    control_key,
    deliver,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    private_entries,
    provision,
    seed_row,
    signed,
    until,
)
from curie_api import protected_runtime_files as loader
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    effects,
    scoped_secret,
    support_db,
)
from test_hook_source_support_broker import (
    Runtime,
    run_case,
    runtime_broker,  # noqa: F401  (fixture)
)
from test_protected_admission_reconciler import TICK, committed, interrupt
from test_protected_runtime_files import Directory

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service


def _load_vectors() -> Any:
    """Shared commissioning record vectors, loaded by path like the broker helper."""
    path = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "protected-hooks"
        / "tests"
        / "commissioning_record_vectors.py"
    )
    name = "_api_commissioning_record_vectors"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None, "commissioning vectors absent"
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


vectors = _load_vectors()
KINDS = vectors.KINDS
CONTROLS = ("selection", "manifest", "qualification", "readiness")
# The support probe's ordered step reasons for an absent control record.
SUPPORT_REASONS = {
    "selection": "runtime_unavailable",
    "manifest": "runtime_unavailable",
    "qualification": "qualification_unavailable",
    "readiness": "evidence_missing",
}
INGRESS_DETAILS = {
    "selection": "runtime_unavailable",
    "manifest": "runtime_unavailable",
    "qualification": "qualification_unavailable",
    "readiness": "evidence_unavailable",
}


def run(scenario: Any, timeout: float = 120) -> None:
    """@spec PROTECTED-COMMISSIONING-4."""
    asyncio.run(asyncio.wait_for(scenario(), timeout))


# -- support probe -----------------------------------------------------------------------


def _substitute(control: str, kind: str) -> Any:
    """One control record replaced by a commissioning record bound to the same manifest."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        rt.raw[control] = vectors.commissioning_records(rt.manifest)[kind]

    return apply


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("control", CONTROLS)
def test_support_probe_refuses_a_commissioning_control_record(
    runtime_broker: Any,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    control: str,
    kind: str,
) -> None:
    """The probe answers its absent-record reason, never supported, with no write.

    @spec PROTECTED-COMMISSIONING-4 @spec PROTECTED-HOOK-SOURCE-9.
    """
    run_case(
        runtime_broker,
        tmp_path,
        monkeypatch,
        _substitute(control, kind),
        SUPPORT_REASONS[control],
        False,
    )


# -- signed hook ingress -----------------------------------------------------------------


@pytest.mark.parametrize("control", CONTROLS)
def test_signed_ingress_refuses_every_commissioning_control_record(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, control: str
) -> None:
    """Each commissioning kind under one control key answers 503 with no effect.

    @spec PROTECTED-COMMISSIONING-4 @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            records = vectors.commissioning_records(rt.manifest)
            for kind in KINDS:
                broker.command("SET", control_key(rt, control), records[kind])
                before = broker_snapshot(broker), await effects(app, agent)
                response = await deliver(client, agent, signed(scoped_secret(agent)))
                assert_detail(response, 503, INGRESS_DETAILS[control])
                assert (broker_snapshot(broker), await effects(app, agent)) == before, kind
            assert private_entries(broker) == []

    run(scenario)


# -- admission reconciliation ------------------------------------------------------------


def test_reconciler_never_commits_a_commissioning_intent(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interrupted intents replaced by commissioning records stay uncommitted.

    A sound interrupted intent beside them commits, so the tick ran over them.
    @spec PROTECTED-COMMISSIONING-4 @spec PROTECTED-HOOK-ADMISSION-5.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            records = vectors.commissioning_records(rt.manifest)
            for kind in KINDS:
                interrupt(broker, rt, agent, kind)
            interrupt(broker, rt, agent, "sound")
            for kind in KINDS:
                broker.command("SET", admission_key("intent", agent, kind), records[kind])
            await until(
                lambda: committed(broker, agent, "sound"),
                3 * TICK,
                "commissioning intents stopped the tick",
            )
            for kind in KINDS:
                assert not committed(broker, agent, kind), kind
                assert broker.command("GET", admission_key("intent", agent, kind)) == records[kind]
            assert len(private_entries(broker)) == 1

    run(scenario)


# -- API runtime files -------------------------------------------------------------------


def _enqueue_file(directory: Directory) -> bytes:
    """A valid ``enqueue.json`` bound to the directory's manifest, @spec PROTECTED-HOOK-SOURCE-6."""
    manifest = json.loads(directory.files["manifest.json"])
    return json.dumps(
        {
            "schema_version": 1,
            "credential_ref": manifest["credential_refs"]["enqueue"],
            "enqueue": {"username": "enqueue-fixture", "password": "enqueue-fixture-secret"},
        }
    ).encode()


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("name", ["manifest.json", "bootstrap.json", "enqueue.json"])
def test_runtime_files_refuse_a_commissioning_record(tmp_path: Path, name: str, kind: str) -> None:
    """A commissioning record under a provisioner file name is the one invalid refusal.

    The unmodified directory loads, so only the substituted file decides.
    @spec PROTECTED-COMMISSIONING-4 @spec PROTECTED-HOOK-SOURCE-6/9.
    """
    directory = Directory(tmp_path)
    try:
        directory.files["enqueue.json"] = _enqueue_file(directory)
        directory.write()
        loader.load_bootstrap(str(directory.path))
        loader.load_ingress(str(directory.path))
        manifest = json.loads(directory.files["manifest.json"])
        directory.files[name] = vectors.commissioning_records(manifest)[kind]
        directory.write()
        loaders = [loader.load_ingress]
        if name != "enqueue.json":
            loaders += [loader.load_bootstrap, loader.load_administration]
        for load in loaders:
            with pytest.raises(loader.RuntimeFilesInvalid):
                load(str(directory.path))
    finally:
        directory.unblock()

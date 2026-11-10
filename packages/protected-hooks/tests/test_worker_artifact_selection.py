"""Authenticated retained selection, never launch permission, @spec PROTECTED-HOOK-LANE-2/5/6."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import importlib.util
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError

import pytest

from . import worker_broker as wb
from .admission_broker import canonical, snapshot
from .test_artifact_measurement import examples

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker
PREFIX = "protected:control:"


def selection():
    """An absent feature fails explicitly after real fixture setup."""
    name = "curie_protected_hooks.worker_artifact_selection"
    assert importlib.util.find_spec(name) is not None, "authenticated artifact selection absent"
    return importlib.import_module(name)


def seeded(broker):
    """Only the fixture provisioner writes genuine retained control records."""
    from curie_protected_hooks.artifact_measurement import parse_artifact_measurement
    from curie_protected_hooks.authority_records import parse_qualification

    manifest = broker.manifest()
    m = manifest.as_dict()
    _, _, data = examples()
    for key in ("execution_config_digest", "substrate", "guard_identity"):
        data[key] = copy.deepcopy(m[key])
    data["manifest_digest"] = manifest.digest
    data["launch_projection"]["sha256"] = m["substrate"]["launch_config_sha256"]
    row = data["platform_mappings"][0]
    row["oci_digest"] = m["runner_image_digest"]
    row["pinned_image_reference"] = "registry.example.com/acme/runner@" + row["oci_digest"]
    row["observed_image_id"] = "docker-pullable://" + row["pinned_image_reference"]
    record = parse_artifact_measurement(canonical(data))
    q = {
        "schema_version": 1,
        **{key: copy.deepcopy(m[key]) for key in (
            "qualification_id", "runtime_id", "runtime_generation", "broker_identity",
            "execution_config_digest", "guard_identity",
        )},
        "qualification_generation": "1",
        "manifest_digest": manifest.digest,
        "measurement_record_id": "sha256:" + hashlib.sha256(canonical(data)).hexdigest(),
    }
    now = broker.command("TIME")
    ms = int(now[0]) * 1000 + int(now[1]) // 1000
    readiness = {
        "schema_version": 1,
        **{key: copy.deepcopy(q[key]) for key in (
            "qualification_id", "qualification_generation", "runtime_id", "runtime_generation",
            "broker_identity", "guard_identity", "manifest_digest",
        )},
        "verifier_identity": copy.deepcopy(m["credential_refs"]["verifier"]),
        "issued_at_ms": str(ms),
        "expires_at_ms": str(ms + 60000),
        # Readiness observations are deliberately independent of artifact identity.
        "measurement_record_id": "readiness/example-v1",
    }
    keys = {
        "manifest": PREFIX + "manifest:" + manifest.digest,
        "readiness": PREFIX + "readiness:" + m["runtime_id"] + ":1",
        "qualification": PREFIX + "qualification:" + m["qualification_id"] + ":1",
    }
    for kind, value in (("manifest", m), ("readiness", readiness), ("qualification", q)):
        broker.command("SET", keys[kind], canonical(value))
    return manifest, record, parse_qualification(canonical(q)), keys


@asynccontextmanager
async def connected(owned):
    """Actual named product worker ACL and pinned nonreconnecting TLS client."""
    client = await wb.worker_transport().AuthenticatedWorkerClient.connect(
        owned.broker.manifest(), owned.credential(), owned.broker.ca_pem,
    )
    try:
        yield client
    finally:
        await client.close()


def run_selection(owned, manifest, record):
    product = selection()

    async def perform():
        async with connected(owned) as client:
            return await product.select_artifact(
                client, trusted_manifest=manifest, measurement=record,
            )

    return asyncio.run(perform())


def assert_detached(error):
    """Neither parser/transport failures nor ambient caller errors escape."""
    assert error.__cause__ is None
    assert error.__context__ is None
    assert error.__suppress_context__
    assert str(error) == "Protected artifact selection unavailable"


def test_authenticated_selection_returns_closed_immutable_pair_without_writes(owned_worker):
    product = selection()
    manifest, record, q, _ = seeded(owned_worker.broker)
    before = snapshot(owned_worker.broker)
    selected = run_selection(owned_worker, manifest, record)
    assert type(selected) is product.SelectedArtifact
    assert selected.qualification.canonical_bytes == q.canonical_bytes
    assert selected.measurement is record
    assert set(selected.__slots__) == {"qualification", "measurement"}
    assert not hasattr(selected, "__dict__")
    with pytest.raises((FrozenInstanceError, AttributeError)):
        selected.measurement = None
    assert record.canonical_bytes.decode() not in repr(selected)
    assert snapshot(owned_worker.broker) == before
    assert not owned_worker.denials()


@pytest.mark.parametrize("kind", ["manifest", "readiness", "qualification"])
@pytest.mark.parametrize("violation", ["missing", "truncated", "oversize", "unknown"])
def test_unavailable_or_malformed_retained_control_refuses(owned_worker, kind, violation):
    product = selection()
    manifest, record, _, keys = seeded(owned_worker.broker)
    if violation == "missing":
        owned_worker.broker.command("DEL", keys[kind])
    else:
        raw = {"truncated": b'{"private-placeholder":', "oversize": b"x" * 16385,
               "unknown": b'{"private-placeholder":"invalid"}'}[violation]
        owned_worker.broker.command("SET", keys[kind], raw)
    before = snapshot(owned_worker.broker)
    with pytest.raises(product.ArtifactSelectionUnavailable) as caught:
        run_selection(owned_worker, manifest, record)
    assert_detached(caught.value)
    assert snapshot(owned_worker.broker) == before


@pytest.mark.parametrize("kind,field", [
    (kind, field) for kind in ("readiness", "qualification") for field in (
        "manifest_digest", "runtime_id", "runtime_generation", "qualification_id",
        "broker_identity", "guard_identity", "qualification_generation",
    )
] + [("qualification", "execution_config_digest"), ("qualification", "measurement_record_id"),
     ("readiness", "verifier_identity")])
def test_every_retained_tuple_and_generation_binding_is_exact(owned_worker, kind, field):
    import json

    product = selection()
    manifest, record, _, keys = seeded(owned_worker.broker)
    data = json.loads(owned_worker.broker.command("GET", keys[kind]))
    value = data[field]
    if field == "broker_identity":
        value["run_id"] = "0" * 40
    elif field == "guard_identity":
        value["revision"] = "2"
    elif field == "verifier_identity":
        value["generation"] = "2"
    elif field.endswith("generation"):
        data[field] = "2"
    elif field.endswith("_id") and field != "measurement_record_id":
        data[field] = "99999999-9999-4999-8999-999999999999"
    elif field == "measurement_record_id":
        data[field] = "sha256:" + "0" * 64
    else:
        data[field] = "0" * 64
    owned_worker.broker.command("SET", keys[kind], canonical(data))
    # Install under the original selected key: q must equal r's generation too.
    with pytest.raises(product.ArtifactSelectionUnavailable) as caught:
        run_selection(owned_worker, manifest, record)
    assert_detached(caught.value)


def test_stored_manifest_cannot_replace_explicit_trusted_manifest(owned_worker):
    import json

    product = selection()
    manifest, record, _, keys = seeded(owned_worker.broker)
    data = json.loads(owned_worker.broker.command("GET", keys["manifest"]))
    data["bundle_digest"]["object_identity"] = "bundle/substitute"
    owned_worker.broker.command("SET", keys["manifest"], canonical(data))
    with pytest.raises(product.ArtifactSelectionUnavailable):
        run_selection(owned_worker, manifest, record)


@pytest.mark.parametrize("violation", ["hash", "image", "platform", "source"])
def test_wrong_preloaded_record_never_becomes_selected(owned_worker, violation):
    from curie_protected_hooks.artifact_measurement import parse_artifact_measurement

    product = selection()
    manifest, record, qualification, keys = seeded(owned_worker.broker)
    data = record.as_dict()
    if violation == "hash":
        data["enforcement_evidence_ref"] = "sha256:" + "9" * 64
    elif violation == "image":
        row = data["platform_mappings"][0]
        row["oci_digest"] = "sha256:" + "9" * 64
        row["pinned_image_reference"] = "registry.example.com/acme/runner@" + row["oci_digest"]
    elif violation == "platform":
        data["platform_mappings"][0]["architecture"] = "arm64"
    else:
        data["manifest_digest"] = "9" * 64
    substitute = parse_artifact_measurement(canonical(data))
    if violation in {"image", "source"}:
        q = qualification.as_dict()
        q["measurement_record_id"] = "sha256:" + hashlib.sha256(canonical(data)).hexdigest()
        owned_worker.broker.command("SET", keys["qualification"], canonical(q))
    with pytest.raises(product.ArtifactSelectionUnavailable):
        run_selection(owned_worker, manifest, substitute)


@pytest.mark.parametrize("change", ["expired", "closed", "rotated"])
def test_retained_accepted_drain_ignores_only_new_admission_gates(owned_worker, change):
    import json

    manifest, record, q, keys = seeded(owned_worker.broker)
    if change == "expired":
        r = json.loads(owned_worker.broker.command("GET", keys["readiness"]))
        r.update(issued_at_ms="1", expires_at_ms="2")
        owned_worker.broker.command("SET", keys["readiness"], canonical(r))
    else:
        current = dict(schema_version=1, admission_open=False, runtime_generation="1",
                       manifest_digest=manifest.digest)
        if change == "rotated":
            current.update(runtime_generation="2", manifest_digest="9" * 64)
        owned_worker.broker.command("SET", wb.CONTROL, canonical(current))
    assert run_selection(owned_worker, manifest, record).qualification == q


def test_second_call_reloads_readiness_selected_qualification(owned_worker):
    import json

    product = selection()
    b = owned_worker.broker
    manifest, record, _, keys = seeded(b)

    async def perform():
        async with connected(owned_worker) as client:
            first = await product.select_artifact(
                client, trusted_manifest=manifest, measurement=record,
            )
            q = first.qualification.as_dict()
            q.update(qualification_generation="2", measurement_record_id="sha256:" + "9" * 64)
            b.command("SET", keys["qualification"][:-1] + "2", canonical(q))
            r = json.loads(b.command("GET", keys["readiness"]))
            r["qualification_generation"] = "2"
            b.command("SET", keys["readiness"], canonical(r))
            with pytest.raises(product.ArtifactSelectionUnavailable):
                await product.select_artifact(client, trusted_manifest=manifest, measurement=record)
            assert first.measurement is record

    asyncio.run(perform())


def test_live_epoch_must_match_explicit_trusted_tuple(owned_worker):
    from curie_protected_hooks.authority_records import parse_manifest

    product = selection()
    manifest, record, _, _ = seeded(owned_worker.broker)
    data = manifest.as_dict()
    data["broker_identity"]["run_id"] = "0" * 40
    with pytest.raises(product.ArtifactSelectionUnavailable):
        run_selection(owned_worker, parse_manifest(canonical(data)), record)


def test_epoch_is_observed_again_after_last_real_control_read(owned_worker, monkeypatch):
    """Revocation after q GET must not return the otherwise matching selected pair."""
    product = selection()
    b = owned_worker.broker
    manifest, record, _, keys = seeded(b)
    transport = wb.worker_transport()
    original = transport.AuthenticatedWorkerClient.read_control

    async def read_then_revoke(self, key):
        raw = await original(self, key)
        if key == keys["qualification"]:
            # Real provisioner revokes all actual sockets after the final read.
            b.command("CLIENT", "KILL", "USER", owned_worker.username, "SKIPME", "yes")
        return raw

    monkeypatch.setattr(transport.AuthenticatedWorkerClient, "read_control", read_then_revoke)
    with pytest.raises(product.ArtifactSelectionUnavailable) as caught:
        run_selection(owned_worker, manifest, record)
    assert_detached(caught.value)


@pytest.mark.parametrize("bad", ["client", "manifest", "record"])
def test_wrong_input_types_have_detached_failure_even_inside_ambient_exception(owned_worker, bad):
    product = selection()
    manifest, record, _, _ = seeded(owned_worker.broker)

    async def perform():
        async with connected(owned_worker) as client:
            try:
                raise ValueError("ambient-private-placeholder")
            except ValueError:
                with pytest.raises(product.ArtifactSelectionUnavailable) as caught:
                    await product.select_artifact(
                        object() if bad == "client" else client,
                        trusted_manifest=manifest.as_dict() if bad == "manifest" else manifest,
                        measurement=record.as_dict() if bad == "record" else record,
                    )
                assert_detached(caught.value)

    asyncio.run(perform())


def test_cancellation_propagates_from_actual_pending_tls_read(owned_worker):
    product = selection()
    manifest, record, _, _ = seeded(owned_worker.broker)

    async def perform():
        async with connected(owned_worker) as client:
            owned_worker.broker.command("CLIENT", "PAUSE", 200, "ALL")
            task = asyncio.create_task(product.select_artifact(
                client, trusted_manifest=manifest, measurement=record,
            ))
            await asyncio.sleep(0)
            assert not task.done()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(perform())


@pytest.mark.parametrize("kind", ["readiness", "qualification"])
@pytest.mark.parametrize("field,value", [
    (("broker_identity", "instance_id"), "99999999-9999-4999-8999-999999999999"),
    (("broker_identity", "endpoint", "host"), "broker.example.com"),
    (("broker_identity", "endpoint", "port"), 6380),
    (("broker_identity", "tls_server_name"), "broker.example.com"),
    (("broker_identity", "tls_spki_sha256"), "9" * 64),
    (("guard_identity", "control_id"), "99999999-9999-4999-8999-999999999999"),
    (("guard_identity", "config_sha256"), "9" * 64),
])
def test_all_nested_broker_and_guard_references_are_exact(owned_worker, kind, field, value):
    import json

    product = selection()
    manifest, record, _, keys = seeded(owned_worker.broker)
    data = json.loads(owned_worker.broker.command("GET", keys[kind]))
    at = data
    for component in field[:-1]:
        at = at[component]
    at[field[-1]] = value
    owned_worker.broker.command("SET", keys[kind], canonical(data))
    with pytest.raises(product.ArtifactSelectionUnavailable):
        run_selection(owned_worker, manifest, record)


def test_readiness_verifier_reference_cannot_substitute_another_issuance(owned_worker):
    import json

    product = selection()
    manifest, record, _, keys = seeded(owned_worker.broker)
    r = json.loads(owned_worker.broker.command("GET", keys["readiness"]))
    r["verifier_identity"]["id"] = "credential/another-verifier"
    owned_worker.broker.command("SET", keys["readiness"], canonical(r))
    with pytest.raises(product.ArtifactSelectionUnavailable):
        run_selection(owned_worker, manifest, record)


def test_new_selected_generation_returns_its_own_preloaded_record(owned_worker):
    import json

    from curie_protected_hooks.artifact_measurement import parse_artifact_measurement

    product = selection()
    b = owned_worker.broker
    manifest, record, _, keys = seeded(b)
    data = record.as_dict()
    data["enforcement_evidence_ref"] = "sha256:" + "9" * 64
    next_record = parse_artifact_measurement(canonical(data))

    async def perform():
        async with connected(owned_worker) as client:
            first = await product.select_artifact(
                client, trusted_manifest=manifest, measurement=record,
            )
            q = first.qualification.as_dict()
            q.update(qualification_generation="2",
                     measurement_record_id="sha256:" + hashlib.sha256(canonical(data)).hexdigest())
            b.command("SET", keys["qualification"][:-1] + "2", canonical(q))
            r = json.loads(b.command("GET", keys["readiness"]))
            r["qualification_generation"] = "2"
            b.command("SET", keys["readiness"], canonical(r))
            second = await product.select_artifact(
                client, trusted_manifest=manifest, measurement=next_record,
            )
            assert second.qualification.as_dict()["qualification_generation"] == "2"
            assert second.measurement is next_record
            assert first.qualification.as_dict()["qualification_generation"] == "1"
            assert first.measurement is record

    asyncio.run(perform())


def test_direct_selected_pair_construction_has_closed_structural_types():
    product = selection()
    with pytest.raises(product.ArtifactSelectionUnavailable) as caught:
        product.SelectedArtifact({}, {})
    assert_detached(caught.value)

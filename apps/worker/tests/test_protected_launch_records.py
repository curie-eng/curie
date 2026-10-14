"""Closed mediator HTTP records are structural, never issued authority."""

import hashlib
import importlib
import importlib.util

import pytest

from .protected_configuration_vectors import canonical
from .test_protected_kubernetes_projection import LAUNCH


def product():
    name = "curie_worker.protected_launch_records"
    assert importlib.util.find_spec(name) is not None, "launch record codec is absent"
    return importlib.import_module(name)


def request_value():
    payload = '{ "text" : "anonymous\\u0020prompt" }\n'
    return dict(
        schema_version=1,
        request_id=LAUNCH,
        runtime_id="33333333-3333-4333-8333-333333333333",
        runtime_generation="1",
        manifest_digest="a" * 64,
        event_id="event/example",
        committed_stream_id="1000-0",
        payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        execution_session_key="protected:33333333-3333-4333-8333-333333333333:1:" + "b" * 64,
        source_fields={"payload": payload, "protected_envelope": '{ "schema_version" : 1 }\n'},
    )


def test_launch_original_source_strings_are_byte_exact_and_hidden():
    p = product()
    value = request_value()
    r = p.parse_launch_request(canonical(value))
    assert r.as_dict() == value
    assert dict(r.source_fields) == value["source_fields"]
    assert dict(r.source_fields)["payload"].encode() == value["source_fields"]["payload"].encode()
    assert "anonymous" not in repr(r)
    value["source_fields"]["payload"] = "replacement"
    assert "replacement" not in r.as_dict()["source_fields"]["payload"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda v: v.update(image="private-marker"),
        lambda v: v.update(schema_version=True),
        lambda v: v.update(runtime_generation="01"),
        lambda v: v.update(committed_stream_id="0-0"),
        lambda v: v["source_fields"].update(secret="private-marker"),
        lambda v: v["source_fields"].update(payload="\ud800"),
        lambda v: v["source_fields"].update(payload="x" * 262145),
        lambda v: v["source_fields"].update(protected_envelope="x" * 16385),
    ],
)
def test_launch_grammar_refuses_without_leaking_context(mutate):
    p = product()
    value = request_value()
    mutate(value)
    try:
        raise OSError("private-path-marker")
    except OSError:
        with pytest.raises(p.LaunchRecordInvalid) as caught:
            p.parse_launch_request(canonical(value))
    assert caught.value.__context__ is None and caught.value.__cause__ is None


def test_closed_control_shapes_and_bounded_epoch():
    p = product()
    base = dict(schema_version=1, operation_id=LAUNCH)
    assert p.parse_retire_request(canonical(base)).as_dict() == base
    epoch = "a" * 43
    assert p.parse_epoch_request(canonical(dict(base, epoch=epoch))).as_dict()["epoch"] == epoch
    admit = dict(base, epoch=epoch, expected_receipt_revision="1", allow=False)
    assert p.parse_admit_request(canonical(admit)).as_dict() == admit
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_admit_request(canonical(dict(admit, allow=0)))
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_epoch_request(canonical(dict(base, epoch="a" * 31)))
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_retire_request(canonical(dict(base, reason="private-marker")))


def receipt_value():
    from .test_protected_kubernetes_projection import UID

    r = request_value()
    return dict(
        schema_version=1,
        launch_id=LAUNCH,
        authority_domain_id="55555555-5555-4555-8555-555555555555",
        runtime_id=r["runtime_id"],
        runtime_generation="1",
        manifest_digest=r["manifest_digest"],
        qualification_id="44444444-4444-4444-8444-444444444444",
        qualification_generation="1",
        execution_config_digest="b" * 64,
        measurement_ref="sha256:" + "c" * 64,
        launch_identity="launch/example-v1",
        projection_sha256="d" * 64,
        guard_identity=dict(
            control_id="22222222-2222-4222-8222-222222222222", revision="1", config_sha256="e" * 64
        ),
        event_id=r["event_id"],
        committed_stream_id=r["committed_stream_id"],
        payload_sha256=r["payload_sha256"],
        execution_session_key=r["execution_session_key"],
        workload=dict(
            namespace="acme-runtime",
            pod_name="acme-pod",
            pod_uid=UID,
            container_id="containerd://example",
            started_at="2026-10-12T00:00:00Z",
            node_uid=UID,
            service_name="acme-service",
            service_uid=UID,
        ),
        assignment=dict(
            secret_name="acme-secret",
            secret_uid=UID,
            secret_resource_version="10",
            immutable=True,
            runner_token_key="runner-token",
            runner_token_sha256="f" * 64,
            history_token_key="history-token",
            history_binding_sha256="0" * 64,
            history_signing_credential_ref=dict(id="credential/example-history", generation="1"),
            history_expires_at_ms="300000",
            model_key="model-credential",
            model_credential_ref=dict(id="credential/example-model", generation="1"),
        ),
        platform=dict(
            os="linux",
            architecture="amd64",
            variant=None,
            runtime_profile="runtime/example",
            pinned_image_reference="registry.example.com/acme-runner@sha256:" + "e" * 64,
            oci_digest="sha256:" + "e" * 64,
            observed_image_id="containerd://sha256:" + "e" * 64,
            image_config_id="sha256:" + "1" * 64,
        ),
        route=dict(kind="mediator-v1", launch_id=LAUNCH, worker_principal_id="worker/example"),
    )


def test_success_and_observation_copy_closed_receipt_without_secret_repr():
    p = product()
    receipt = receipt_value()
    assert p.parse_launch_receipt(canonical(receipt)).as_dict() == receipt
    success = dict(
        schema_version=1,
        launch_id=LAUNCH,
        receipt_revision="1",
        state="idle",
        receipt=receipt,
        control_capability="A" * 43,
    )
    assert p.parse_launch_success(canonical(success)).as_dict() == success
    assert "AAAA" not in repr(p.parse_launch_success(canonical(success)))
    success["receipt"]["route"]["launch_id"] = "99999999-9999-4999-8999-999999999999"
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_launch_success(canonical(success))


def test_start_preserves_existing_aci_event_but_validates_delivery_separately():
    from aci_protocol import Event

    from .test_protected_history import dispatchable

    p = product()
    delivery = dispatchable()
    event = Event(
        type="job",
        text=delivery.turn.text,
        user=delivery.turn.author,
        ts=delivery.turn.conversation_id,
        tool_access=delivery.turn.tool_access,
    )
    value = dict(
        schema_version=1,
        operation_id=LAUNCH,
        expected_receipt_revision="1",
        capacity_admission=True,
        event=event.model_dump(mode="json"),
    )
    parsed = p.parse_start_request(canonical(value))
    assert parsed.event == event
    p.validate_start_request_delivery(parsed, delivery=delivery)
    value["event"]["text"] = "substituted prompt"
    # Structural decoding alone must never assert delivery authority.
    replacement = p.parse_start_request(canonical(value))
    with pytest.raises(p.LaunchRecordInvalid):
        p.validate_start_request_delivery(replacement, delivery=delivery)


def test_start_event_reader_tolerates_additive_fields_but_keeps_closed_authority():
    from aci_protocol import Event

    from .test_protected_history import dispatchable

    p = product()
    delivery = dispatchable()
    event = Event(
        type="job", text=delivery.turn.text, user=delivery.turn.author,
        ts=delivery.turn.conversation_id, tool_access=delivery.turn.tool_access,
    )
    value = dict(
        schema_version=1, operation_id=LAUNCH, expected_receipt_revision="1",
        capacity_admission=True, event=event.model_dump(mode="json"),
    )
    # Existing ACI reader contract (ADR 0036); mediator authority stays closed.
    value["event"]["future_optional_field"] = {"synthetic": True}
    parsed = p.parse_start_request(canonical(value))
    assert parsed.event == event
    p.validate_start_request_delivery(parsed, delivery=delivery)
    value["unexpected_authority"] = True
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_start_request(canonical(value))
    del value["unexpected_authority"]
    value["event"]["tool_access"] = "unknown-policy"
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_start_request(canonical(value))


def test_request_matches_full_verified_delivery_and_original_sources():
    from dataclasses import replace

    from curie_protected_hooks.admission_records import parse_envelope, parse_intent

    from .protected_configuration_vectors import execution_profile, manifest_for
    from .test_protected_history import dispatchable

    p = product()
    delivery = dispatchable()
    manifest = manifest_for(execution_profile())
    payload = delivery.turn.model_dump_json(indent=2) + "\n"
    envelope = delivery.envelope.as_dict()
    envelope.update(
        manifest_digest=manifest.digest,
        execution_config_digest=manifest.as_dict()["execution_config_digest"],
        payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
    )
    intent = delivery.intent.as_dict()
    intent.update(
        manifest_digest=manifest.digest,
        payload_sha256=envelope["payload_sha256"],
        envelope_sha256=hashlib.sha256(canonical(envelope)).hexdigest(),
    )
    sources = (("payload", payload), ("protected_envelope", canonical(envelope).decode()))
    delivery = replace(
        delivery,
        envelope=parse_envelope(canonical(envelope)),
        intent=parse_intent(canonical(intent)),
        source_fields=sources,
    )
    r = request_value()
    r.update(
        manifest_digest=manifest.digest,
        payload_sha256=envelope["payload_sha256"],
        event_id=envelope["event_id"],
        execution_session_key=envelope["execution_session_key"],
        source_fields=dict(sources),
    )
    parsed = p.parse_launch_request(canonical(r))
    p.validate_launch_request_delivery(parsed, delivery=delivery, manifest=manifest)
    r["source_fields"]["payload"] = delivery.turn.model_dump_json()
    # Same parsed turn with a different original spelling is not the accepted bytes.
    with pytest.raises(p.LaunchRecordInvalid):
        p.validate_launch_request_delivery(
            p.parse_launch_request(canonical(r)), delivery=delivery, manifest=manifest
        )


def test_receipt_binding_matches_all_selected_identities_without_issuing_authority():
    from curie_protected_hooks.authority_records import parse_qualification

    from .test_protected_kubernetes_projection import binding_vectors

    p = product()
    value, execution, manifest, measurement, row = binding_vectors()
    projection = importlib.import_module(
        "curie_worker.protected_kubernetes_projection"
    ).parse_projection(canonical(value))
    m = manifest.as_dict()
    q = dict(
        schema_version=1,
        qualification_id=m["qualification_id"],
        qualification_generation="1",
        runtime_id=m["runtime_id"],
        runtime_generation=m["runtime_generation"],
        manifest_digest=manifest.digest,
        broker_identity=m["broker_identity"],
        execution_config_digest=execution.digest,
        guard_identity=m["guard_identity"],
        measurement_record_id="sha256:" + measurement.digest,
    )
    qualification = parse_qualification(canonical(q))
    request = request_value()
    request["manifest_digest"] = manifest.digest
    request = p.parse_launch_request(canonical(request))
    receipt = receipt_value()
    receipt.update(
        manifest_digest=manifest.digest,
        execution_config_digest=execution.digest,
        measurement_ref="sha256:" + measurement.digest,
        projection_sha256=projection.digest,
        guard_identity=m["guard_identity"],
        platform=row,
    )
    p.validate_launch_receipt_binding(
        p.parse_launch_receipt(canonical(receipt)),
        request=request,
        manifest=manifest,
        qualification=qualification,
        projection=projection,
        execution=execution,
        measurement=measurement,
        worker_principal_id="worker/example",
    )
    receipt["assignment"]["model_credential_ref"]["generation"] = "2"
    with pytest.raises(p.LaunchRecordInvalid):
        p.validate_launch_receipt_binding(
            p.parse_launch_receipt(canonical(receipt)),
            request=request,
            manifest=manifest,
            qualification=qualification,
            projection=projection,
            execution=execution,
            measurement=measurement,
            worker_principal_id="worker/example",
        )


def test_pending_response_is_exact_and_has_no_positive_receipt():
    p = product()
    assert hasattr(p, "LaunchPendingSuccess"), "pending launch response codec absent"
    value = dict(
        schema_version=1,
        launch_id=LAUNCH,
        receipt_revision="1",
        state="preparing",
        receipt=None,
        control_capability="A" * 43,
    )
    parsed = p.parse_launch_pending_success(canonical(value))
    assert type(parsed) is p.LaunchPendingSuccess
    assert parsed.as_dict() == value
    assert "AAAA" not in repr(parsed)
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_launch_success(canonical(value))
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_launch_pending_success(canonical(dict(value, receipt=receipt_value())))
    with pytest.raises(p.LaunchRecordInvalid):
        p.parse_launch_pending_success(canonical(dict(value, state="idle")))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("qualification_id", "99999999-9999-4999-8999-999999999999"),
        ("runner_image_digest", "sha256:" + "0" * 64),
        ("execution_config_digest", "0" * 64),
        ("bundle_digest", "0" * 64),
    ],
)
def test_request_comparator_checks_full_verified_manifest_tuple(field, bad):
    from dataclasses import replace

    from curie_protected_hooks.admission_records import parse_envelope, parse_intent

    from .protected_configuration_vectors import execution_profile, manifest_for
    from .test_protected_history import dispatchable

    p = product()
    manifest = manifest_for(execution_profile())
    delivery = dispatchable()
    envelope = delivery.envelope.as_dict()
    envelope.update(
        manifest_digest=manifest.digest,
        execution_config_digest=manifest.as_dict()["execution_config_digest"],
    )
    envelope[field] = bad
    payload = delivery.turn.model_dump_json()
    sources = (("payload", payload), ("protected_envelope", canonical(envelope).decode()))
    intent = delivery.intent.as_dict()
    intent.update(
        manifest_digest=manifest.digest,
        envelope_sha256=hashlib.sha256(canonical(envelope)).hexdigest(),
    )
    delivery = replace(
        delivery,
        envelope=parse_envelope(canonical(envelope)),
        intent=parse_intent(canonical(intent)),
        source_fields=sources,
    )
    value = request_value()
    value.update(
        manifest_digest=manifest.digest,
        payload_sha256=envelope["payload_sha256"],
        event_id=envelope["event_id"],
        execution_session_key=envelope["execution_session_key"],
        source_fields=dict(sources),
    )
    with pytest.raises(p.LaunchRecordInvalid):
        p.validate_launch_request_delivery(
            p.parse_launch_request(canonical(value)), delivery=delivery, manifest=manifest
        )

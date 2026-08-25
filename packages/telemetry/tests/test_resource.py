"""Stable service resources omit turn and deployment correlation identifiers."""

from __future__ import annotations

from curie_telemetry import build_resource


def test_resource_has_stable_service_identity_without_correlation_ids() -> None:
    first = build_resource(
        "curie-worker",
        service_version="0.7.0",
        service_instance_id="acme-worker-instance",
        deployment_environment="test",
    )
    second = build_resource(
        "curie-worker",
        service_version="0.7.0",
        service_instance_id="acme-worker-instance",
        deployment_environment="test",
    )

    expected = {
        "service.namespace": "curie",
        "service.name": "curie-worker",
        "service.version": "0.7.0",
        "service.instance.id": "acme-worker-instance",
        "deployment.environment.name": "test",
    }
    assert {key: first.attributes[key] for key in expected} == expected
    assert {key: second.attributes[key] for key in expected} == expected

    forbidden = {
        "curie.event_id",
        "curie.run_id",
        "curie.session_id",
        "curie.sandbox_id",
        "curie.user_id",
        "curie.agent_id",
        "curie.deployment_id",
        "event.id",
        "run.id",
        "session.id",
        "user.id",
        "agent.id",
        "deployment.id",
        "sandbox.id",
        "k8s.pod.name",
        "container.id",
        "trace_id",
        "span_id",
    }
    assert forbidden.isdisjoint(first.attributes)
    assert forbidden.isdisjoint(second.attributes)


def test_resource_omits_unconfigured_deployment_environment() -> None:
    resource = build_resource(
        "curie-api",
        service_version="0.7.0",
        service_instance_id="acme-api-instance",
    )

    assert "deployment.environment.name" not in resource.attributes

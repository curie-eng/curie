"""Independent commissioning verifier, @spec PROTECTED-COMMISSIONING-5/6/9 (ADR 0236).

Every producer here is a fake: the tests prove the decision rule and the
publication gate, never an installation. Missing, unavailable, skipped,
timed-out, failed, foreign, stale, replayed and fake-model observations each
refuse, and the Qualification and Readiness publishers are constructed only on
the accepted branch, after the tuple is re-measured.
"""

from __future__ import annotations

import ast
import asyncio
import copy
import dataclasses
import hashlib
import importlib
import importlib.util
import inspect
import json
from collections.abc import Callable
from typing import Any

import pytest

from .test_commissioning_bootstrap import encoded
from .test_commissioning_provisioner import built
from .test_commissioning_records import permit_value

EVIDENCE = "ab" * 32


def module() -> Any:
    name = "curie_protected_hooks.commissioning_verifier"
    assert importlib.util.find_spec(name) is not None, "commissioning verifier not implemented"
    return importlib.import_module(name)


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def records(**permit_changes: Any) -> dict[str, Any]:
    """B, G, M and the measurement from the provisioner core, plus a permit bound to them."""
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

    provisioned = built()
    m = provisioned.manifest.as_dict()
    p = permit_value()
    for member in (
        "runtime_id",
        "runtime_generation",
        "execution_config_digest",
        "broker_identity",
        "guard_identity",
    ):
        p[member] = copy.deepcopy(m[member])
    p["manifest_digest"] = provisioned.manifest.digest
    p["bootstrap_digest"] = provisioned.bootstrap.digest
    p.update(permit_changes)
    return {
        "permit": parse_commissioning_permit(encoded(p)),
        "bootstrap": provisioned.bootstrap,
        "guard_config": provisioned.guard_config,
        "manifest": provisioned.manifest,
        "measurement": provisioned.measurement,
    }


def tuple_of(r: dict[str, Any]) -> Any:
    return module().InstallationTuple.measure(**r)


class Producer:
    """A fake producer: echoes the challenge for this installation unless told otherwise."""

    def __init__(self, case: str, execution: str, **changes: Any) -> None:
        self.case, self.execution, self.changes = case, execution, changes
        self.calls: list[tuple[str, str]] = []

    async def observe(self, installation_digest: str, challenge: str) -> Any:
        self.calls.append((installation_digest, challenge))
        api = module()
        values: dict[str, Any] = {
            "case": self.case,
            "installation_digest": installation_digest,
            "challenge": challenge,
            "outcome": "pass",
            "provenance": "installation-producer",
            "model": "real" if self.case in api.REAL_MODEL_CASES else "none",
            "execution_config": self.execution,
            "evidence_sha256": EVIDENCE,
            "entries": (),
        }
        values.update(self.changes)
        return api.CaseObservation(**values)


class Raising:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def observe(self, installation_digest: str, challenge: str) -> Any:
        raise self.error


class Slow:
    async def observe(self, installation_digest: str, challenge: str) -> Any:
        await asyncio.sleep(5)


class Stubborn:
    """Swallows its cancellation and returns a passing observation late."""

    def __init__(self, inner: Producer) -> None:
        self.inner = inner

    async def observe(self, installation_digest: str, challenge: str) -> Any:
        loop = asyncio.get_running_loop()
        end = loop.time() + 0.2
        while loop.time() < end:
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                pass
        return await self.inner.observe(installation_digest, challenge)


class Connections:
    """Records every publisher construction; the real factories never run here."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, fail: str | None = None) -> None:
        from curie_protected_hooks import qualification_publication as qp

        self.events: list[str] = []
        self.fail = fail
        connections = self

        class FakeQualification:
            async def publish(self, **options: Any) -> Any:
                connections.events.append("publish-qualification")
                connections.qualification_options = options
                if connections.fail == "qualification":
                    raise qp.PublicationRefused("exists")
                return "qualification-record"

            async def close(self) -> None:
                connections.events.append("close-qualification")

        class FakeReadiness:
            async def publish(self, qualification: Any, **options: Any) -> Any:
                connections.events.append("publish-readiness")
                connections.readiness_options = dict(options, qualification=qualification)
                if connections.fail == "readiness":
                    raise qp.PublicationRefused("reobservation")
                return "readiness-record"

            async def close(self) -> None:
                connections.events.append("close-readiness")

        async def qualification_connect(*args: Any, **options: Any) -> Any:
            connections.events.append("connect-qualification")
            connections.qualification_connect = (args, options)
            return FakeQualification()

        async def readiness_connect(*args: Any, **options: Any) -> Any:
            connections.events.append("connect-readiness")
            connections.readiness_connect = (args, options)
            if connections.fail == "readiness-connect":
                raise qp.PublicationRefused("principal")
            return FakeReadiness()

        monkeypatch.setattr(qp.QualificationPublisher, "connect", qualification_connect)
        monkeypatch.setattr(qp.ReadinessPublisher, "connect", readiness_connect)

    @property
    def constructed(self) -> list[str]:
        return [e for e in self.events if e.startswith("connect-")]


def credential(username: str) -> Any:
    from curie_protected_hooks.worker_transport import WorkerCredential

    return WorkerCredential(username=username, password="example-" + "p" * 24)


async def reobserve_ok() -> None:
    return None


def publication(r: dict[str, Any], **changes: Any) -> Any:
    b = r["bootstrap"].as_dict()
    m = r["manifest"].as_dict()
    values: dict[str, Any] = {
        "qualification_credential": credential(b["qualification_publisher_identity"]["id"]),
        "readiness_credential": credential(m["credential_refs"]["verifier"]["id"]),
        "ca_pem": "-----BEGIN CERTIFICATE-----\nexample\n-----END CERTIFICATE-----\n",
        "qualification_generation": "1",
        "readiness_lifetime_ms": 30000,
        "max_readiness_ms": 60000,
        "reobserve": reobserve_ok,
    }
    values.update(changes)
    return module().PublicationInputs(**values)


class Harness:
    def __init__(self, **permit_changes: Any) -> None:
        self.records = records(**permit_changes)
        self.tuple = tuple_of(self.records)
        execution = self.records["manifest"].as_dict()["execution_config_digest"]
        self.producers: dict[str, Any] = {
            case: Producer(case, execution) for case in module().REQUIRED_CASES
        }
        self.reused: dict[str, Any] = {}
        self.current: Any = self.tuple
        self.remeasured = 0
        self.timeout = 2.0
        self.publication = publication(self.records)

    async def remeasure(self) -> Any:
        self.remeasured += 1
        if isinstance(self.current, BaseException):
            raise self.current
        return self.current

    def verifier(self) -> Any:
        return module().CommissioningVerifier(
            **self.records,
            producers=self.producers,
            reused=self.reused,
            remeasure=self.remeasure,
            publication=self.publication,
            case_timeout_s=self.timeout,
        )

    def decide(self) -> Any:
        return run(self.verifier().decide())

    def acceptance(self, verdict: Any, **changes: Any) -> Any:
        """The issuer's acceptance record for ``verdict`` (clause 6 step 3)."""
        from curie_protected_hooks.commissioning_campaign_records import (
            parse_commissioning_acceptance,
        )

        m = self.records["manifest"].as_dict()
        value: dict[str, Any] = {
            "schema_version": 1,
            "record_type": "commissioning_acceptance",
            "campaign": {
                "campaign_id": self.tuple.campaign_id,
                "campaign_generation": self.tuple.campaign_generation,
                "permit_digest": self.tuple.permit,
                "bootstrap_digest": self.tuple.bootstrap,
                "manifest_digest": self.tuple.manifest,
                "runtime_id": m["runtime_id"],
                "runtime_generation": m["runtime_generation"],
                "broker_run_id": self.tuple.broker_run_id,
            },
            "evidence_digest": verdict.evidence_digest or "0" * 64,
            "qualification_id": m["qualification_id"],
            "qualification_generation": "1",
            "accepted_at_ms": "1500",
        }
        for key, replacement in changes.items():
            if key in value["campaign"]:
                value["campaign"][key] = replacement
            else:
                value[key] = replacement
        return parse_commissioning_acceptance(canonical(value))


async def accept_and_publish(h: Harness, verifier: Any) -> Any:
    verdict = await verifier.decide()
    return await verifier.publish(verdict, h.acceptance(verdict))


def refused(verdict: Any, reason: str) -> None:
    assert verdict.outcome == "refused"
    assert not verdict.accepted
    assert verdict.reason == reason
    assert verdict.evidence_digest is None


# --- The closed case set ----------------------------------------------------


def test_required_cases_cover_clause_5_and_adr_0236() -> None:
    api = module()
    cases = api.REQUIRED_CASES
    assert len(set(cases)) == len(cases)
    for case in (
        "positive-delivery",
        "source-target-escape",
        "payload-signature-scope-tampering",
        "mutation-tool-approval-refusal",
        "ordinary-and-old-worker",
        "production-principals-refuse",
        "cross-installation-replay",
        "cross-kind-substitution",
        "duplicate-and-ambiguous-dispatch",
        "counter-limit-boundaries",
        "retries-parking-dead-letter",
        "broker-restart",
        "control-restart",
        "live-session-expiry",
        "interrupted-closing",
        "image-violation",
        "credential-violation",
        "network-violation",
        "ephemeral-container-violation",
        "static-skill-loading",
        "clean-session",
        "read-only-tool-restriction",
        "revocation",
        "termination-and-absence",
    ):
        assert case in cases
    # Revocation and the original workload's termination are observed last.
    assert cases[-2:] == ("revocation", "termination-and-absence")
    assert api.REAL_MODEL_CASES == frozenset({"positive-delivery"})
    # Only pinned SDK primitive observations are artifact observations (clause 9).
    assert api.REUSABLE_CASES == frozenset(
        {"static-skill-loading", "clean-session", "read-only-tool-restriction"}
    )


# --- The full positive path -------------------------------------------------


def test_every_case_passing_accepts_without_constructing_a_publisher(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()
    verdict = h.decide()
    assert verdict.outcome == "accepted" and verdict.accepted and verdict.reason is None
    assert verdict.installation_digest == h.tuple.digest
    assert [o.case for o in verdict.observations] == list(module().REQUIRED_CASES)
    assert len(verdict.evidence_digest) == 64
    # Each producer was asked exactly once, with a fresh challenge for this installation.
    challenges = set()
    for producer in h.producers.values():
        assert len(producer.calls) == 1
        digest, challenge = producer.calls[0]
        assert digest == h.tuple.digest
        challenges.add(challenge)
    assert len(challenges) == len(h.producers)
    assert h.remeasured == 1
    # Acceptance alone constructs nothing.
    assert connections.events == []


def test_publish_after_acceptance_constructs_both_publishers_in_order(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        assert connections.events == []
        return verdict, await verifier.publish(verdict, h.acceptance(verdict))

    verdict, result = run(scenario())
    assert result.outcome == "published" and result.reason is None
    assert result.qualification == "qualification-record"
    assert result.readiness == "readiness-record"
    assert connections.events == [
        "connect-qualification",
        "connect-readiness",
        "publish-qualification",
        "publish-readiness",
        "close-qualification",
        "close-readiness",
    ]
    # The tuple is re-measured again on the accepted branch, before any write.
    assert h.remeasured == 2
    r = h.records
    args, options = connections.qualification_connect
    assert args[0] is r["manifest"] or args[0].digest == r["manifest"].digest
    assert options["bootstrap"].digest == r["bootstrap"].digest
    assert options["guard_config"] == r["guard_config"]
    record_id = "sha256:" + r["measurement"].digest
    assert connections.qualification_options == {
        "qualification_generation": "1",
        "measurement_record_id": record_id,
    }
    assert connections.readiness_connect[1] == {"trusted_max_readiness_ms": 60000}
    assert connections.readiness_options["qualification"] == "qualification-record"
    assert connections.readiness_options["lifetime_ms"] == 30000
    assert connections.readiness_options["measurement_record_id"] == record_id
    assert connections.readiness_options["expected"] is None
    assert connections.readiness_options["reobserve"] is reobserve_ok


# --- Every refusal constructs no publisher ----------------------------------


def refuses_without_publishers(monkeypatch: pytest.MonkeyPatch, h: Harness, reason: str) -> Any:
    connections = Connections(monkeypatch)

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        with pytest.raises(module().VerifierRefused):
            await verifier.publish(verdict, h.acceptance(verdict))
        return verdict

    verdict = run(scenario())
    refused(verdict, reason)
    assert connections.constructed == []
    return verdict


@pytest.mark.parametrize("case", ["positive-delivery", "network-violation", "revocation"])
def test_a_missing_case_refuses(monkeypatch, case: str) -> None:
    h = Harness()
    del h.producers[case]
    refuses_without_publishers(monkeypatch, h, "missing:" + case)
    # Nothing is observed when the case set is incomplete.
    assert all(not p.calls for p in h.producers.values())


@pytest.mark.parametrize("outcome", ["unavailable", "skipped", "timed-out", "fail"])
@pytest.mark.parametrize("case", ["positive-delivery", "broker-restart", "termination-and-absence"])
def test_a_reported_non_pass_outcome_refuses(monkeypatch, outcome: str, case: str) -> None:
    h = Harness()
    h.producers[case].changes["outcome"] = outcome
    refuses_without_publishers(monkeypatch, h, outcome + ":" + case)


@pytest.mark.parametrize("outcome", ["unavailable", "skipped", "timed-out", "fail", "PASS", ""])
def test_a_non_pass_sub_entry_refuses_even_under_a_passing_case(monkeypatch, outcome: str) -> None:
    """A managed-CNI ``workload_replaced`` entry is unavailable by design today: it refuses."""
    h = Harness()
    h.producers["network-violation"].changes["entries"] = (
        ("case-1/enforcement", "pass"),
        ("case-5/workload_replaced", outcome),
        ("case-5/policy_changed", "pass"),
    )
    refuses_without_publishers(monkeypatch, h, "entry:network-violation")


def test_passing_sub_entries_are_accepted() -> None:
    h = Harness()
    h.producers["network-violation"].changes["entries"] = (
        ("case-1/enforcement", "pass"),
        ("case-5/policy_changed", "pass"),
    )
    assert h.decide().accepted


@pytest.mark.parametrize(
    "entries",
    [
        (("case-1/enforcement", "pass"), ("case-1/enforcement", "pass")),
        [("case-1/enforcement", "pass")],
        (("case-1/enforcement",),),
        ((1, "pass"),),
        (("", "pass"),),
    ],
)
def test_malformed_sub_entries_refuse(monkeypatch, entries: Any) -> None:
    h = Harness()
    h.producers["network-violation"].changes["entries"] = entries
    refuses_without_publishers(monkeypatch, h, "invalid:network-violation")


@pytest.mark.parametrize(
    "error", [RuntimeError("producer down"), OSError("connection reset"), ValueError("bad")]
)
def test_a_raising_producer_is_unavailable(monkeypatch, error: Exception) -> None:
    h = Harness()
    h.producers["control-restart"] = Raising(error)
    refuses_without_publishers(monkeypatch, h, "unavailable:control-restart")


def test_a_producer_that_exceeds_the_case_timeout_refuses(monkeypatch) -> None:
    h = Harness()
    h.timeout = 0.05
    h.producers["live-session-expiry"] = Slow()
    refuses_without_publishers(monkeypatch, h, "timed-out:live-session-expiry")


def test_a_producer_that_swallows_cancellation_still_times_out(monkeypatch) -> None:
    h = Harness()
    h.timeout = 0.05
    inner = h.producers["interrupted-closing"]
    h.producers["interrupted-closing"] = Stubborn(inner)
    refuses_without_publishers(monkeypatch, h, "timed-out:interrupted-closing")


@pytest.mark.parametrize("model", ["fake", "none", "replayed", ""])
def test_the_delivered_reply_requires_a_real_model_observation(monkeypatch, model: str) -> None:
    h = Harness()
    h.producers["positive-delivery"].changes["model"] = model
    refuses_without_publishers(monkeypatch, h, "model:positive-delivery")


@pytest.mark.parametrize("model", ["fake", "real"])
def test_a_case_that_makes_no_model_call_never_claims_one(monkeypatch, model: str) -> None:
    h = Harness()
    h.producers["cross-kind-substitution"].changes["model"] = model
    refuses_without_publishers(monkeypatch, h, "model:cross-kind-substitution")


@pytest.mark.parametrize("provenance", ["fixture", "fake", "imported", "control-report", ""])
def test_only_installation_producers_and_signed_replies_count(monkeypatch, provenance: str) -> None:
    h = Harness()
    h.producers["ordinary-and-old-worker"].changes["provenance"] = provenance
    refuses_without_publishers(monkeypatch, h, "provenance:ordinary-and-old-worker")


def test_a_signed_reply_observation_is_accepted() -> None:
    h = Harness()
    h.producers["image-violation"].changes["provenance"] = "signed-reply"
    verdict = h.decide()
    assert verdict.accepted
    assert verdict.observations[module().REQUIRED_CASES.index("image-violation")].provenance == (
        "signed-reply"
    )


STALE: dict[str, Callable[[Harness], Any]] = {
    "foreign-installation": lambda h: {"installation_digest": "cd" * 32},
    "replayed-challenge": lambda h: {"challenge": "00000000-0000-4000-8000-000000000000"},
    "other-case": lambda h: {"case": "broker-restart"},
    "other-execution": lambda h: {"execution_config": "ef" * 32},
    "evidence-not-hex": lambda h: {"evidence_sha256": "AB" * 32},
    "evidence-short": lambda h: {"evidence_sha256": "ab"},
    "outcome-not-str": lambda h: {"outcome": True},
}


@pytest.mark.parametrize("name", sorted(STALE))
def test_a_foreign_stale_or_malformed_observation_refuses(monkeypatch, name: str) -> None:
    h = Harness()
    h.producers["counter-limit-boundaries"].changes.update(STALE[name](h))
    refuses_without_publishers(monkeypatch, h, "invalid:counter-limit-boundaries")


def test_a_lookalike_observation_type_refuses(monkeypatch) -> None:
    h = Harness()
    inner = h.producers["revocation"]

    class Lookalike:
        async def observe(self, installation_digest: str, challenge: str) -> Any:
            value = await inner.observe(installation_digest, challenge)
            return dataclasses.asdict(value)

    h.producers["revocation"] = Lookalike()
    refuses_without_publishers(monkeypatch, h, "invalid:revocation")


def test_the_first_refusal_stops_observation() -> None:
    h = Harness()
    h.producers["broker-restart"].changes["outcome"] = "unavailable"
    refused(h.decide(), "unavailable:broker-restart")
    order = module().REQUIRED_CASES
    after = order[order.index("broker-restart") + 1 :]
    assert all(not h.producers[case].calls for case in after)
    # The tuple is never re-measured for a refused campaign.
    assert h.remeasured == 0


# --- The final tuple equals the premeasured one ------------------------------


TUPLE_CHANGES = {
    "manifest": "11" * 32,
    "bootstrap": "22" * 32,
    "guard": "33" * 32,
    "execution_config": "44" * 32,
    "broker_run_id": "c" * 40,
    "permit": "55" * 32,
    "artifact": "66" * 32,
    "runner_image": "sha256:" + "7" * 64,
}


@pytest.mark.parametrize("member", sorted(TUPLE_CHANGES))
def test_a_changed_final_tuple_refuses(monkeypatch, member: str) -> None:
    h = Harness()
    h.current = dataclasses.replace(h.tuple, **{member: TUPLE_CHANGES[member]})
    refuses_without_publishers(monkeypatch, h, "changed-installation")


def test_an_unavailable_remeasurement_refuses(monkeypatch) -> None:
    h = Harness()
    h.current = RuntimeError("broker unreachable")
    refuses_without_publishers(monkeypatch, h, "changed-installation")


def test_a_lookalike_remeasurement_refuses(monkeypatch) -> None:
    h = Harness()
    h.current = dataclasses.asdict(h.tuple)
    refuses_without_publishers(monkeypatch, h, "changed-installation")


def test_a_tuple_change_between_acceptance_and_publication_constructs_nothing(
    monkeypatch,
) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        assert verdict.accepted
        h.current = dataclasses.replace(h.tuple, broker_run_id="c" * 40)
        return await verifier.publish(verdict, h.acceptance(verdict))

    result = run(scenario())
    assert result.outcome == "refused" and result.reason == "changed-installation"
    assert result.qualification is None and result.readiness is None
    assert connections.events == []


# --- Measuring the installation tuple -----------------------------------------


def test_installation_tuple_binds_the_permit_manifest_bootstrap_guard_and_artifact() -> None:
    r = records()
    t = tuple_of(r)
    m = r["manifest"].as_dict()
    assert t.permit == r["permit"].digest
    assert t.manifest == r["manifest"].digest
    assert t.bootstrap == r["bootstrap"].digest
    assert t.guard == hashlib.sha256(r["guard_config"]).hexdigest()
    assert t.execution_config == m["execution_config_digest"]
    assert t.broker_run_id == m["broker_identity"]["run_id"]
    assert t.runner_image == m["runner_image_digest"]
    assert t.artifact == r["measurement"].digest
    assert t.campaign_id == r["permit"].as_dict()["campaign_id"]
    assert len(t.digest) == 64
    assert tuple_of(records()).digest == t.digest


@pytest.mark.parametrize(
    "changes",
    [
        {"manifest_digest": "11" * 32},
        {"bootstrap_digest": "22" * 32},
        {"execution_config_digest": "44" * 32},
        {"runtime_generation": "9"},
    ],
)
def test_a_permit_for_another_tuple_refuses_measurement(changes: dict[str, Any]) -> None:
    with pytest.raises(module().VerifierRefused):
        tuple_of(records(**changes))


def test_a_permit_for_another_broker_epoch_refuses_measurement() -> None:
    r = records()
    identity = copy.deepcopy(r["manifest"].as_dict()["broker_identity"])
    identity["run_id"] = "c" * 40
    with pytest.raises(module().VerifierRefused):
        tuple_of(records(broker_identity=identity))


def test_a_guard_config_that_does_not_bind_b_and_m_refuses() -> None:
    r = records()
    r["guard_config"] = r["guard_config"] + b" "
    with pytest.raises(module().VerifierRefused):
        tuple_of(r)


def test_a_measurement_for_another_manifest_refuses() -> None:
    r = records()
    other = built(guard_revision="2")
    assert other.manifest.digest != r["manifest"].digest
    r["measurement"] = other.measurement
    with pytest.raises(module().VerifierRefused):
        tuple_of(r)


@pytest.mark.parametrize("member", ["permit", "bootstrap", "manifest", "measurement"])
def test_lookalike_records_refuse_measurement(member: str) -> None:
    r = records()

    class Lookalike:
        def __init__(self, inner: Any) -> None:
            self.inner = inner

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

    r[member] = Lookalike(r[member])
    with pytest.raises(module().VerifierRefused):
        tuple_of(r)


# --- Evidence reuse (clause 9) -------------------------------------------------


def artifact_observation(r: dict[str, Any], case: str, /, **changes: Any) -> Any:
    binding = module().ArtifactBinding.of(r["measurement"]).as_dict()
    value: dict[str, Any] = {
        "schema_version": 1,
        "record_type": "commissioning_artifact_observation",
        "case": case,
        "artifact_binding": binding,
        "outcome": "pass",
        "evidence_sha256": EVIDENCE,
    }
    value.update(changes)
    raw = canonical(value)
    return module().ReusedArtifactObservation(
        content_digest=hashlib.sha256(raw).hexdigest(), raw=raw
    )


def test_artifact_binding_names_image_digests_image_ids_projection_and_runtime_profile() -> None:
    r = records()
    binding = module().ArtifactBinding.of(r["measurement"]).as_dict()
    measurement = r["measurement"].as_dict()
    assert binding["runner_image"] == r["manifest"].as_dict()["runner_image_digest"]
    assert binding["launch_projection"] == measurement["launch_projection"]["sha256"]
    (row,) = measurement["platform_mappings"]
    assert binding["platforms"] == [
        {
            "os": row["os"],
            "architecture": row["architecture"],
            "variant": row["variant"],
            "runtime_profile": row["runtime_profile"],
            "oci_digest": row["oci_digest"],
            "observed_image_id": row["observed_image_id"],
        }
    ]


def test_a_reused_sdk_primitive_observation_bound_to_the_unchanged_artifact_accepts() -> None:
    h = Harness()
    for case in module().REUSABLE_CASES:
        del h.producers[case]
        h.reused[case] = artifact_observation(h.records, case)
    verdict = h.decide()
    assert verdict.accepted
    reused = {o.case: o for o in verdict.observations if o.provenance == "reused-artifact"}
    assert set(reused) == module().REUSABLE_CASES
    for case, observation in reused.items():
        assert observation.evidence_sha256 == h.reused[case].content_digest


@pytest.mark.parametrize("case", ["network-violation", "revocation", "credential-violation"])
def test_installation_observations_are_never_reused(monkeypatch, case: str) -> None:
    h = Harness()
    del h.producers[case]
    h.reused[case] = artifact_observation(h.records, case)
    with pytest.raises(module().VerifierRefused):
        h.verifier()


def test_a_case_with_both_a_producer_and_a_reused_observation_refuses() -> None:
    h = Harness()
    h.reused["clean-session"] = artifact_observation(h.records, "clean-session")
    with pytest.raises(module().VerifierRefused):
        h.verifier()


def mutate_binding(path: tuple[Any, ...], value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(binding: dict[str, Any]) -> None:
        target: Any = binding
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return mutate


REUSE_ATTACKS: dict[str, Callable[[dict[str, Any]], None]] = {
    "image-id": mutate_binding(("platforms", 0, "observed_image_id"), "sha256:" + "8" * 64),
    "oci-digest": mutate_binding(("platforms", 0, "oci_digest"), "sha256:" + "8" * 64),
    "runtime-profile": mutate_binding(("platforms", 0, "runtime_profile"), "runtime/other"),
    "projection": mutate_binding(("launch_projection",), "8" * 64),
    "runner-image": mutate_binding(("runner_image",), "sha256:" + "8" * 64),
    "extra-platform": lambda b: b["platforms"].append(dict(b["platforms"][0], os="windows")),
}


@pytest.mark.parametrize("name", sorted(REUSE_ATTACKS))
def test_a_reused_observation_for_a_changed_artifact_refuses(monkeypatch, name: str) -> None:
    h = Harness()
    binding = module().ArtifactBinding.of(h.records["measurement"]).as_dict()
    REUSE_ATTACKS[name](binding)
    del h.producers["clean-session"]
    h.reused["clean-session"] = artifact_observation(
        h.records, "clean-session", artifact_binding=binding
    )
    refuses_without_publishers(monkeypatch, h, "reuse:clean-session")


@pytest.mark.parametrize(
    "changes",
    [
        {"outcome": "unavailable"},
        {"outcome": "skipped"},
        {"case": "static-skill-loading"},
        {"record_type": "commissioning_acceptance"},
        {"schema_version": 2},
        {"evidence_sha256": "AB" * 32},
        {"extra": True},
    ],
)
def test_a_reused_observation_that_is_not_a_passing_closed_record_refuses(
    monkeypatch, changes: dict[str, Any]
) -> None:
    h = Harness()
    del h.producers["clean-session"]
    h.reused["clean-session"] = artifact_observation(h.records, "clean-session", **changes)
    refuses_without_publishers(monkeypatch, h, "reuse:clean-session")


def test_a_reused_observation_is_content_addressed(monkeypatch) -> None:
    h = Harness()
    del h.producers["clean-session"]
    good = artifact_observation(h.records, "clean-session")
    h.reused["clean-session"] = module().ReusedArtifactObservation(
        content_digest="cd" * 32, raw=good.raw
    )
    refuses_without_publishers(monkeypatch, h, "reuse:clean-session")


def test_a_reused_observation_with_non_canonical_bytes_refuses(monkeypatch) -> None:
    h = Harness()
    del h.producers["clean-session"]
    good = artifact_observation(h.records, "clean-session")
    raw = json.dumps(json.loads(good.raw), indent=1).encode()
    h.reused["clean-session"] = module().ReusedArtifactObservation(
        content_digest=hashlib.sha256(raw).hexdigest(), raw=raw
    )
    refuses_without_publishers(monkeypatch, h, "reuse:clean-session")


# --- The publication gate -----------------------------------------------------


def test_publish_before_decide_constructs_nothing(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()
    other = Harness().decide()
    assert other.accepted

    async def scenario() -> None:
        verifier = h.verifier()
        with pytest.raises(module().VerifierRefused):
            await verifier.publish(other, h.acceptance(other))

    run(scenario())
    assert connections.events == []


def test_a_copied_or_forged_accepted_verdict_constructs_nothing(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()
    api = module()

    async def scenario() -> None:
        verifier = h.verifier()
        verdict = await verifier.decide()
        assert verdict.accepted
        forged = api.Verdict(
            outcome="accepted",
            reason=None,
            installation_digest=verdict.installation_digest,
            evidence_digest=verdict.evidence_digest,
            observations=verdict.observations,
        )
        for candidate in (dataclasses.replace(verdict), forged, copy.copy(verdict)):
            with pytest.raises(api.VerifierRefused):
                await verifier.publish(candidate, h.acceptance(candidate))

    run(scenario())
    assert connections.events == []


def test_publication_happens_at_most_once(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> None:
        verifier = h.verifier()
        verdict = await verifier.decide()
        assert (await verifier.publish(verdict, h.acceptance(verdict))).outcome == "published"
        with pytest.raises(module().VerifierRefused):
            await verifier.publish(verdict, h.acceptance(verdict))

    run(scenario())
    assert connections.constructed == ["connect-qualification", "connect-readiness"]


def test_a_verifier_decides_once(monkeypatch) -> None:
    Connections(monkeypatch)
    h = Harness()

    async def scenario() -> None:
        verifier = h.verifier()
        assert (await verifier.decide()).accepted
        with pytest.raises(module().VerifierRefused):
            await verifier.decide()

    run(scenario())
    assert all(len(p.calls) == 1 for p in h.producers.values())


def test_a_readiness_connection_failure_writes_no_qualification(monkeypatch) -> None:
    connections = Connections(monkeypatch, fail="readiness-connect")
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        return await accept_and_publish(h, verifier)

    result = run(scenario())
    assert result.outcome == "refused" and result.reason == "publication-unavailable"
    assert "publish-qualification" not in connections.events
    assert "close-qualification" in connections.events


def test_a_refused_qualification_write_is_refused_publication(monkeypatch) -> None:
    connections = Connections(monkeypatch, fail="qualification")
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        return await accept_and_publish(h, verifier)

    result = run(scenario())
    assert result.outcome == "refused" and result.reason == "publication-unavailable"
    assert "publish-readiness" not in connections.events


def test_a_readiness_failure_after_qualification_is_uncertain(monkeypatch) -> None:
    connections = Connections(monkeypatch, fail="readiness")
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        return await accept_and_publish(h, verifier)

    result = run(scenario())
    assert result.outcome == "uncertain" and result.reason == "readiness-unconfirmed"
    assert result.qualification == "qualification-record" and result.readiness is None
    assert connections.events[-2:] == ["close-qualification", "close-readiness"]


def test_publishers_are_constructed_only_inside_the_accepted_branch() -> None:
    """Static check: the only publisher construction sites are in ``_publish``."""
    source = inspect.getsource(module())
    tree = ast.parse(source)
    sites: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Attribute)
                    and inner.attr == "connect"
                    and isinstance(inner.value, ast.Name)
                    and inner.value.id in ("QualificationPublisher", "ReadinessPublisher")
                ):
                    sites.append(node.name + ":" + inner.value.id)
    assert sorted(sites) == ["_publish:QualificationPublisher", "_publish:ReadinessPublisher"]


ACCEPTANCE_ATTACKS: dict[str, dict[str, Any]] = {
    "other-evidence": {"evidence_digest": "cd" * 32},
    "other-campaign": {"campaign_id": "77777777-7777-4777-8777-777777777777"},
    "other-campaign-generation": {"campaign_generation": "2"},
    "other-permit": {"permit_digest": "11" * 32},
    "other-bootstrap": {"bootstrap_digest": "22" * 32},
    "other-manifest": {"manifest_digest": "33" * 32},
    "other-runtime-generation": {"runtime_generation": "9"},
    "other-broker-epoch": {"broker_run_id": "c" * 40},
    "other-qualification": {"qualification_id": "88888888-8888-4888-8888-888888888888"},
    "other-qualification-generation": {"qualification_generation": "2"},
}


@pytest.mark.parametrize("name", sorted(ACCEPTANCE_ATTACKS))
def test_publication_requires_the_issuers_acceptance_for_this_verdict(
    monkeypatch, name: str
) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        with pytest.raises(module().VerifierRefused):
            await verifier.publish(verdict, h.acceptance(verdict, **ACCEPTANCE_ATTACKS[name]))
        # A mismatched record fences nothing: the matching one still publishes.
        return await verifier.publish(verdict, h.acceptance(verdict))

    assert run(scenario()).outcome == "published"
    assert connections.constructed == ["connect-qualification", "connect-readiness"]


def test_publication_requires_an_acceptance_record(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> None:
        verifier = h.verifier()
        verdict = await verifier.decide()
        record = h.acceptance(verdict)
        for candidate in (None, record.as_dict(), record.canonical_bytes):
            with pytest.raises(module().VerifierRefused):
                await verifier.publish(verdict, candidate)

    run(scenario())
    assert connections.events == []


# --- Inputs ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "changes",
    [
        {"readiness_lifetime_ms": 0},
        {"readiness_lifetime_ms": 60001},
        {"max_readiness_ms": 9007199254740992},
        {"qualification_generation": "01"},
        {"qualification_generation": 1},
        {"ca_pem": ""},
        {"reobserve": None},
        {"qualification_credential": "user:password"},
    ],
)
def test_malformed_publication_inputs_refuse_before_any_campaign(changes: dict[str, Any]) -> None:
    with pytest.raises(module().VerifierRefused):
        publication(records(), **changes)


def test_publication_inputs_never_show_credentials() -> None:
    inputs = publication(records())
    assert "p" * 24 not in repr(inputs)


def test_an_unknown_producer_case_refuses_construction() -> None:
    h = Harness()
    h.producers["self-attested-success"] = Producer("self-attested-success", "0" * 64)
    with pytest.raises(module().VerifierRefused):
        h.verifier()


@pytest.mark.parametrize("timeout", [0, -1, 901, "1", None])
def test_case_timeout_is_bounded(timeout: Any) -> None:
    h = Harness()
    h.timeout = timeout
    with pytest.raises(module().VerifierRefused):
        h.verifier()


def test_a_verdict_is_a_report_not_authority() -> None:
    verdict = Harness().decide()
    with pytest.raises(dataclasses.FrozenInstanceError):
        verdict.outcome = "refused"  # type: ignore[misc]
    assert "Verdict" in type(verdict).__name__

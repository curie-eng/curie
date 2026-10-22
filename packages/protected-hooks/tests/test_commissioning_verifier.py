"""Independent commissioning verifier, @spec PROTECTED-COMMISSIONING-5/6/9 (ADR 0236).

Every producer here is a fake: the tests prove the decision rule and the
publication gate, never an installation. Missing, unavailable, skipped,
timed-out, failed, foreign, stale, replayed and fake-model observations each
refuse, and the Qualification and Readiness publishers are constructed only on
the accepted branch, after the verifier observes the tuple again through its
own fact producers and candidate rule reader.
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
from .test_commissioning_provisioner import PRINCIPALS, built, profile
from .test_commissioning_provisioner import module as provisioner
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


def permit_for(provisioned: Any, **changes: Any) -> Any:
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

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
    p.update(changes)
    return parse_commissioning_permit(encoded(p))


def lane_prefix(permit: Any) -> str:
    from curie_protected_hooks.commissioning_acl import derive_commissioning_lane

    return str(derive_commissioning_lane(permit).prefix)


def template(role: str, permit: Any) -> tuple[str, ...]:
    """Design decision 9: the candidate recipe with the lane prefix as ``{lane}``."""
    from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

    prefix = lane_prefix(permit)
    return tuple(r.replace(prefix, "{lane}") for r in commissioning_acl_rules(role, permit=permit))


class Recipes:
    """Stand-in rules for installation principals; decision 9 templates for candidates."""

    def __init__(self, candidates: dict[str, tuple[str, ...]] | None = None) -> None:
        sample = permit_value_for_templates()
        self.candidates = candidates or {
            role: template(role, sample) for role in ("candidate_enqueue", "candidate_worker")
        }

    def rules(self, principal: str) -> tuple[str, ...]:
        if principal in self.candidates:
            return self.candidates[principal]
        assert principal in PRINCIPALS
        return ("-@all", "resetkeys", "+acl|whoami", "%R~protected:example:" + principal + ":*")


def permit_value_for_templates() -> Any:
    """Any permit gives the same template; a different campaign proves it."""
    return permit_for(built(), campaign_id="99999999-9999-4999-8999-999999999999")


def provision(*, evidence_ref: str | None = None, recipes: Recipes | None = None) -> Any:
    changes = {} if evidence_ref is None else {"enforcement_evidence_ref": evidence_ref}
    return provisioner().construct(profile(**changes), acl=recipes or Recipes())


def evidence_record(
    provisioned: Any,
    *,
    cases: Any = None,
    mutate: Callable[[dict[str, Any]], None] | None = None,
) -> bytes:
    """Clause 9 evidence: the record the measurement's ``enforcement_evidence_ref`` names."""
    binding = module().ArtifactBinding.of(provisioned.measurement, provisioned.manifest).as_dict()
    value: dict[str, Any] = {
        "schema_version": 1,
        "record_type": "commissioning_artifact_evidence",
        "artifact_binding": binding,
        "observations": {
            case: {"outcome": "pass", "evidence_sha256": EVIDENCE}
            for case in (cases if cases is not None else sorted(module().REUSABLE_CASES))
        },
    }
    if mutate is not None:
        mutate(value)
    return canonical(value)


def records(
    *, evidence: bytes | None = None, recipes: Recipes | None = None, **permit_changes: Any
) -> dict[str, Any]:
    """B, G, M and the measurement from the provisioner core, plus a permit bound to them.

    With ``evidence``, the measurement's ``enforcement_evidence_ref`` names it.
    """
    ref = None if evidence is None else "sha256:" + hashlib.sha256(evidence).hexdigest()
    provisioned = provision(evidence_ref=ref, recipes=recipes)
    return {
        "permit": permit_for(provisioned, **permit_changes),
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


class Fact:
    """A fake tuple fact producer: reports ``observed[fact]`` freshly on each call."""

    def __init__(self, harness: Harness, fact: str) -> None:
        self.harness, self._fact = harness, fact
        self.calls = 0

    @property
    def fact(self) -> str:
        return self._fact

    async def observe(self) -> str:
        self.calls += 1
        value = self.harness.observed[self._fact]
        if isinstance(value, BaseException):
            raise value
        return value


class Rules:
    """A fake decision 7 observer reading of each candidate user's rules."""

    def __init__(self, harness: Harness) -> None:
        self.harness = harness
        self.calls: list[str] = []

    async def rules(self, username: str) -> Any:
        self.calls.append(username)
        value = self.harness.candidate_rules[username]
        if isinstance(value, BaseException):
            raise value
        return value


class Harness:
    def __init__(self, *, evidence: bytes | None = None, **permit_changes: Any) -> None:
        from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

        self.records = records(evidence=evidence, **permit_changes)
        self.evidence = evidence
        self.tuple = tuple_of(self.records)
        execution = self.records["manifest"].as_dict()["execution_config_digest"]
        self.producers: dict[str, Any] = {
            case: Producer(case, execution) for case in module().REQUIRED_CASES
        }
        self.observed: dict[str, Any] = self.tuple.facts()
        self.facts = tuple(Fact(self, fact) for fact in module().TUPLE_FACTS)
        credentials = self.records["bootstrap"].as_dict()["candidate_credentials"]
        permit = self.records["permit"]
        self.usernames = {
            "candidate_enqueue": credentials["enqueue"]["id"],
            "candidate_worker": credentials["worker"]["id"],
        }
        # Each candidate user holds its template instantiated for this permit's lane.
        self.candidate_rules: dict[str, Any] = {
            self.usernames[role]: commissioning_acl_rules(role, permit=permit)
            for role in self.usernames
        }
        self.reader = Rules(self)
        self.timeout = 2.0
        self.publication = publication(self.records)

    @property
    def observations(self) -> int:
        """How many times every tuple fact was observed (they move together)."""
        counts = {fact.calls for fact in self.facts}
        assert len(counts) == 1
        return counts.pop()

    def verifier(self) -> Any:
        return module().CommissioningVerifier(
            **self.records,
            producers=self.producers,
            artifact_evidence=self.evidence,
            tuple_producers=self.facts,
            candidate_rules=self.reader,
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
    # The verifier observed every tuple fact and both candidate users once.
    assert h.observations == 1
    assert sorted(h.reader.calls) == sorted(h.usernames.values())
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
    # The tuple is observed again on the accepted branch, before any write.
    assert h.observations == 2
    assert len(h.reader.calls) == 4
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
    # The tuple is never observed for a refused campaign.
    assert h.observations == 0


# --- The verifier observes the final tuple itself ------------------------------


@pytest.mark.parametrize(
    "fact",
    ["permit", "manifest", "bootstrap", "guard", "execution_config", "broker_run_id", "artifact"],
)
def test_a_changed_tuple_fact_refuses(monkeypatch, fact: str) -> None:
    h = Harness()
    h.observed[fact] = "cd" * 32
    refuses_without_publishers(monkeypatch, h, "changed-installation")


def test_tuple_facts_name_the_records_by_digest() -> None:
    h = Harness()
    r = h.records
    assert module().TUPLE_FACTS == (
        "permit",
        "manifest",
        "bootstrap",
        "guard",
        "execution_config",
        "broker_run_id",
        "artifact",
    )
    assert h.tuple.facts() == {
        "permit": r["permit"].digest,
        "manifest": r["manifest"].digest,
        "bootstrap": r["bootstrap"].digest,
        "guard": hashlib.sha256(r["guard_config"]).hexdigest(),
        "execution_config": r["manifest"].as_dict()["execution_config_digest"],
        "broker_run_id": hashlib.sha256(
            r["manifest"].as_dict()["broker_identity"]["run_id"].encode()
        ).hexdigest(),
        "artifact": r["measurement"].digest,
    }


def test_another_installations_observed_tuple_refuses(monkeypatch) -> None:
    """The producers observe what is installed; another installation's tuple never matches."""
    h = Harness()
    other = tuple_of(records(campaign_id="77777777-7777-4777-8777-777777777777"))
    h.observed = other.facts()
    refuses_without_publishers(monkeypatch, h, "changed-installation")


@pytest.mark.parametrize("value", [RuntimeError("broker unreachable"), "CD" * 32, None, b"x"])
def test_an_unavailable_or_malformed_tuple_observation_refuses(monkeypatch, value: Any) -> None:
    h = Harness()
    h.observed["guard"] = value
    refuses_without_publishers(monkeypatch, h, "changed-installation")


def test_a_tuple_producer_that_never_answers_refuses(monkeypatch) -> None:
    h = Harness()
    h.timeout = 0.05

    class Hung(Fact):
        async def observe(self) -> str:
            await asyncio.sleep(5)
            return ""

    h.facts = (*h.facts[:-1], Hung(h, "artifact"))
    connections = Connections(monkeypatch)
    refused(run(h.verifier().decide()), "changed-installation")
    assert connections.events == []


@pytest.mark.parametrize("change", ["missing", "duplicate", "extra", "not-a-tuple"])
def test_exactly_one_producer_per_tuple_fact(change: str) -> None:
    h = Harness()
    facts = list(h.facts)
    if change == "missing":
        facts.pop()
    elif change == "duplicate":
        facts[-1] = Fact(h, "permit")
    elif change == "extra":
        facts.append(Fact(h, "control_profile"))
    h.facts = facts if change == "not-a-tuple" else tuple(facts)  # type: ignore[assignment]
    with pytest.raises(module().VerifierRefused):
        h.verifier()


def test_a_tuple_change_between_acceptance_and_publication_constructs_nothing(
    monkeypatch,
) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        assert verdict.accepted
        h.observed["broker_run_id"] = hashlib.sha256(b"c" * 40).hexdigest()
        return await verifier.publish(verdict, h.acceptance(verdict))

    result = run(scenario())
    assert result.outcome == "refused" and result.reason == "changed-installation"
    assert result.qualification is None and result.readiness is None
    assert connections.events == []


# --- Candidate rules: design decision 9 -------------------------------------------


def other_lane_rules(role: str) -> tuple[str, ...]:
    from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

    other = records(campaign_id="77777777-7777-4777-8777-777777777777")["permit"]
    return tuple(commissioning_acl_rules(role, permit=other))


CANDIDATE_ATTACKS: dict[str, Callable[[str, tuple[str, ...]], Any]] = {
    "other-lane": lambda role, rules: other_lane_rules(role),
    "extra-rule": lambda role, rules: (*rules, "+keys"),
    "missing-rule": lambda role, rules: rules[:-1],
    "reordered": lambda role, rules: tuple(reversed(rules)),
    "list": lambda role, rules: list(rules),
    "unavailable": lambda role, rules: RuntimeError("NOPERM"),
}


@pytest.mark.parametrize("role", ["candidate_enqueue", "candidate_worker"])
@pytest.mark.parametrize("name", sorted(CANDIDATE_ATTACKS))
def test_candidate_rules_must_equal_the_template_for_this_permits_lane(
    monkeypatch, role: str, name: str
) -> None:
    h = Harness()
    username = h.usernames[role]
    h.candidate_rules[username] = CANDIDATE_ATTACKS[name](role, h.candidate_rules[username])
    refuses_without_publishers(monkeypatch, h, "candidate-rules")


@pytest.mark.parametrize("role", ["candidate_enqueue", "candidate_worker"])
def test_an_uninstantiated_template_refuses(monkeypatch, role: str) -> None:
    h = Harness()
    h.candidate_rules[h.usernames[role]] = template(role, h.records["permit"])
    refuses_without_publishers(monkeypatch, h, "candidate-rules")


def test_a_candidate_rule_change_before_publication_constructs_nothing(monkeypatch) -> None:
    connections = Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        assert verdict.accepted
        username = h.usernames["candidate_worker"]
        h.candidate_rules[username] = other_lane_rules("candidate_worker")
        return await verifier.publish(verdict, h.acceptance(verdict))

    result = run(scenario())
    assert result.outcome == "refused" and result.reason == "candidate-rules"
    assert connections.events == []


@pytest.mark.parametrize("role", ["candidate_enqueue", "candidate_worker"])
def test_a_guard_that_hashes_instantiated_rules_instead_of_the_template_refuses(
    role: str,
) -> None:
    """Design decision 9: such a G cannot be built, so it never reaches the verifier.

    The provisioner core refuses a candidate entry that holds an instantiated lane
    prefix. A G that carries other non-template candidate rules is refused by the
    verifier itself (the stand-in recipes test below).
    """
    from curie_protected_hooks.commissioning_acl import commissioning_acl_rules
    from curie_protected_hooks.commissioning_provisioner import ProvisionerRefused

    sample = permit_value_for_templates()
    candidates = {r: template(r, sample) for r in ("candidate_enqueue", "candidate_worker")}
    candidates[role] = tuple(commissioning_acl_rules(role, permit=sample))
    with pytest.raises(ProvisionerRefused):
        records(recipes=Recipes(candidates))


def test_a_guard_whose_candidate_rules_are_not_the_recipe_template_refuses() -> None:
    from .test_commissioning_provisioner import Recipes as StandIn

    h = Harness()
    h.records = records(recipes=StandIn())  # type: ignore[arg-type]
    with pytest.raises(module().VerifierRefused):
        h.verifier()


@pytest.mark.parametrize("reader", [None, object(), "rules"])
def test_a_candidate_rule_reader_is_required(reader: Any) -> None:
    h = Harness()
    h.reader = reader
    with pytest.raises(module().VerifierRefused):
        h.verifier()


def test_the_template_is_the_same_for_every_campaign() -> None:
    a = records()["permit"]
    b = records(campaign_id="77777777-7777-4777-8777-777777777777")["permit"]
    assert lane_prefix(a) != lane_prefix(b)
    for role in ("candidate_enqueue", "candidate_worker"):
        assert template(role, a) == template(role, b)
        assert any("{lane}" in rule for rule in template(role, a))


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


def reused_harness(**options: Any) -> Harness:
    """Evidence for ``cases`` named by the measurement's ``enforcement_evidence_ref``."""
    cases = options.get("cases", sorted(module().REUSABLE_CASES))
    raw = evidence_record(provision(), **options)
    h = Harness(evidence=raw)
    for case in cases:
        h.producers.pop(case, None)
    return h


def test_artifact_binding_names_m_images_image_ids_bundle_projection_and_profile() -> None:
    r = records()
    binding = module().ArtifactBinding.of(r["measurement"], r["manifest"]).as_dict()
    measurement, m = r["measurement"].as_dict(), r["manifest"].as_dict()
    assert binding["runner_image"] == m["runner_image_digest"]
    assert binding["bundle"] == m["bundle_digest"]
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


def test_artifact_binding_requires_ms_runner_image_on_every_platform() -> None:
    from curie_protected_hooks.authority_records import parse_manifest

    r = records()
    m = r["manifest"].as_dict()
    m["runner_image_digest"] = "sha256:" + "8" * 64
    other = parse_manifest(canonical(m))
    with pytest.raises(module().VerifierRefused):
        module().ArtifactBinding.of(r["measurement"], other)


def test_evidence_named_by_the_measurement_and_bound_to_the_artifact_accepts() -> None:
    h = reused_harness()
    verdict = h.decide()
    assert verdict.accepted
    reused = {o.case: o for o in verdict.observations if o.provenance == "reused-artifact"}
    assert set(reused) == module().REUSABLE_CASES
    assert all(o.evidence_sha256 == EVIDENCE for o in reused.values())


def test_a_self_hashed_passing_record_is_not_reusable_evidence(monkeypatch) -> None:
    """A record that hashes itself but is not the measured evidence reference refuses."""
    h = Harness()
    for case in module().REUSABLE_CASES:
        del h.producers[case]
    # Content addressed only by its own digest; the measurement names other evidence.
    h.evidence = evidence_record(provision())
    assert h.records["measurement"].as_dict()["enforcement_evidence_ref"] != (
        "sha256:" + hashlib.sha256(h.evidence).hexdigest()
    )
    refuses_without_publishers(monkeypatch, h, "reuse")


def test_evidence_for_another_measurement_refuses(monkeypatch) -> None:
    h = reused_harness()
    good = h.evidence
    assert good is not None
    h.evidence = good.replace(EVIDENCE.encode(), b"cd" * 32)
    refuses_without_publishers(monkeypatch, h, "reuse")


@pytest.mark.parametrize("case", ["network-violation", "revocation", "credential-violation"])
def test_installation_observations_are_never_reused(monkeypatch, case: str) -> None:
    h = reused_harness(cases=[case])
    refuses_without_publishers(monkeypatch, h, "reuse")


def test_a_case_with_both_a_producer_and_reused_evidence_refuses(monkeypatch) -> None:
    raw = evidence_record(provision(), cases=["clean-session"])
    h = Harness(evidence=raw)
    refuses_without_publishers(monkeypatch, h, "reuse")


def mutate_binding(path: tuple[Any, ...], value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(record: dict[str, Any]) -> None:
        target: Any = record["artifact_binding"]
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
    "bundle": mutate_binding(("bundle", "sha256"), "8" * 64),
    "extra-platform": lambda v: v["artifact_binding"]["platforms"].append(
        dict(v["artifact_binding"]["platforms"][0], os="windows")
    ),
    "not-pass": lambda v: v["observations"]["clean-session"].update(outcome="unavailable"),
    "skipped": lambda v: v["observations"]["clean-session"].update(outcome="skipped"),
    "record-type": lambda v: v.update(record_type="commissioning_acceptance"),
    "schema-version": lambda v: v.update(schema_version=2),
    "evidence-not-hex": lambda v: v["observations"]["clean-session"].update(
        evidence_sha256="AB" * 32
    ),
    "extra-member": lambda v: v.update(extra=True),
    "empty": lambda v: v.update(observations={}),
}


@pytest.mark.parametrize("name", sorted(REUSE_ATTACKS))
def test_measured_evidence_that_does_not_hold_refuses(monkeypatch, name: str) -> None:
    """The reference names the attacked record, so only the attacked property is under test."""
    h = reused_harness(cases=["clean-session"], mutate=REUSE_ATTACKS[name])
    refuses_without_publishers(monkeypatch, h, "reuse")


def test_non_canonical_evidence_bytes_refuse(monkeypatch) -> None:
    raw = json.dumps(json.loads(evidence_record(provision())), indent=1).encode()
    h = Harness(evidence=raw)
    for case in module().REUSABLE_CASES:
        del h.producers[case]
    refuses_without_publishers(monkeypatch, h, "reuse")


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


# --- Clause 6: the re-read after the selection opens --------------------------


def test_the_tuple_is_confirmed_unchanged_only_after_publication(monkeypatch) -> None:
    Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        with pytest.raises(module().VerifierRefused):
            await verifier.confirm_unchanged()
        await verifier.publish(verdict, h.acceptance(verdict))
        unchanged = await verifier.confirm_unchanged()
        h.observed["manifest"] = "0" * 64
        return unchanged, await verifier.confirm_unchanged()

    unchanged, changed = run(scenario())
    assert unchanged is None
    assert changed == "changed-installation"
    # Decide, publish and both re-reads each observed the tuple afresh.
    assert h.observations == 4


def test_a_candidate_rule_change_after_publication_is_reported(monkeypatch) -> None:
    Connections(monkeypatch)
    h = Harness()

    async def scenario() -> Any:
        verifier = h.verifier()
        verdict = await verifier.decide()
        await verifier.publish(verdict, h.acceptance(verdict))
        h.candidate_rules[h.usernames["candidate_worker"]] = ("+@all",)
        return await verifier.confirm_unchanged()

    assert run(scenario()) == "candidate-rules"

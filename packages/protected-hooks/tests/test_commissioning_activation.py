"""The activation transition's order and refusals, @spec PROTECTED-COMMISSIONING-6.

Every collaborator here is a fake, so these tests prove the order of the
clause 6 writes and that each failure stops the transition before the next
write; the broker writes themselves and the unchanged production readers are
proved on the pinned Valkey in ``test_commissioning_activation_broker``.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
from typing import Any

import pytest

from .test_commissioning_verifier import records, tuple_of

EVIDENCE = "cd" * 32
COUNTERS = {
    "deliveries": 1,
    "launches": 1,
    "model_calls": 1,
    "output_tokens": 9,
    "spend_microusd": 7,
}


def module() -> Any:
    name = "curie_protected_hooks.commissioning_activation"
    assert importlib.util.find_spec(name) is not None, "commissioning activation not implemented"
    return importlib.import_module(name)


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def selection(r: dict[str, Any], **changes: Any) -> dict[str, Any]:
    m = r["manifest"].as_dict()
    value = {
        "schema_version": 1,
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "manifest_digest": r["manifest"].digest,
        "qualification_id": m["qualification_id"],
        "qualification_generation": "1",
        "broker_run_id": m["broker_identity"]["run_id"],
        "admission_open": False,
    }
    value.update(changes)
    return value


class Fakes:
    """One campaign's collaborators, recording every call in one ordered list."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, state: str = "closing") -> None:
        from curie_protected_hooks.commissioning_journal import JournalState
        from curie_protected_hooks.commissioning_verifier import Publication, Verdict

        self.records = records()
        self.tuple = tuple_of(self.records)
        self.events: list[str] = []
        self.fail: dict[str, BaseException] = {}
        self.state = state
        self.publication = Publication("published", None, "qualification", "readiness")
        self.verdict = Verdict("accepted", None, self.tuple.digest, EVIDENCE, ())
        self.unchanged: str | None = None
        self.acceptance: Any = None
        self.phases: list[dict[str, bool]] = []
        fakes = self

        def check(name: str) -> None:
            fakes.events.append(name)
            if name in fakes.fail:
                raise fakes.fail[name]

        self.check = check

        class Journal:
            permit = fakes.records["permit"]

            async def status(self) -> Any:
                check("status")
                return JournalState(fakes.state, 4, dict(COUNTERS), None, 2000)

            async def confirm_closed(self, expected_generation: int) -> Any:
                check("confirm_closed")
                assert expected_generation == 4
                fakes.state = "closed"
                return JournalState("closed", 5, dict(COUNTERS), None, 2000)

        class Acl:
            async def require(self, phase: Any) -> None:
                fakes.phases.append(dict(phase.enabled))
                check("acl")

        class Issuer:
            async def broker_time_ms(self) -> int:
                check("issuer_time")
                return 1500

        class Verifier:
            installation = fakes.tuple

            async def decide(self) -> Any:
                check("decide")
                return fakes.verdict

            async def publish(self, verdict: Any, acceptance: Any) -> Any:
                check("publish")
                assert verdict is fakes.verdict and acceptance is fakes.acceptance
                return fakes.publication

            async def confirm_unchanged(self) -> str | None:
                check("confirm_unchanged")
                return fakes.unchanged

        async def settled() -> None:
            check("settled")

        async def issue_acceptance(session: Any, acceptance: Any, **trusted: Any) -> None:
            assert session is fakes.issuer
            assert trusted["permit"] is fakes.records["permit"]
            fakes.acceptance = acceptance
            check("issue_acceptance")

        def disable_candidates(session: Any, bootstrap: Any) -> None:
            assert session is fakes.provisioner
            check("disable_candidates")

        def open_admission(session: Any, **options: Any) -> tuple[bytes, bytes]:
            assert session is fakes.provisioner
            assert options["qualification_generation"] == "1"
            check("open_admission")
            closed = canonical(selection(fakes.records))
            return closed, canonical(selection(fakes.records, admission_open=True))

        def close_admission(session: Any, **options: Any) -> None:
            check("close_admission")

        api = module()
        monkeypatch.setattr(api, "issue_acceptance", issue_acceptance)
        monkeypatch.setattr(api, "disable_candidates", disable_candidates)
        monkeypatch.setattr(api, "open_admission", open_admission)
        monkeypatch.setattr(api, "close_admission", close_admission)
        self.journal, self.acl, self.issuer = Journal(), Acl(), Issuer()
        self.verifier, self.settled = Verifier(), settled
        self.provisioner = object()

    def activation(self, **changes: Any) -> Any:
        b = self.records["bootstrap"].as_dict()
        values: dict[str, Any] = {
            "permit": self.records["permit"],
            "manifest": self.records["manifest"],
            "bootstrap": self.records["bootstrap"],
            "trusted_issuer": b["issuer_identity"],
            "trusted_control": b["control_identity"],
            "max_permit_ms": int(b["max_permit_ms"]),
            "trusted_max_readiness_ms": 60000,
            "qualification_generation": "1",
            "journal": self.journal,
            "settled": self.settled,
            "provisioner": self.provisioner,
            "acl": self.acl,
            "issuer": self.issuer,
            "verifier": self.verifier,
        }
        values.update(changes)
        return module().CommissioningActivation(**values)

    def refused(self, step: str) -> Any:
        with pytest.raises(module().ActivationRefused) as caught:
            run(self.activation().activate())
        assert caught.value.step == step
        return caught.value


ORDER = [
    "status",
    "settled",
    "confirm_closed",
    "disable_candidates",
    "acl",
    "decide",
    "issuer_time",
    "issue_acceptance",
    "publish",
    "open_admission",
    "confirm_unchanged",
]


def test_clause_6_writes_run_in_order_each_by_its_own_principal(monkeypatch) -> None:
    fakes = Fakes(monkeypatch)
    report = run(fakes.activation().activate())
    assert fakes.events == ORDER
    assert report.selection_before == canonical(selection(fakes.records))
    assert report.selection_after == canonical(selection(fakes.records, admission_open=True))
    assert report.publication is fakes.publication
    # Design decision 7: after step 2 the candidates are expected disabled.
    assert fakes.phases == [{"candidate_enqueue": False, "candidate_worker": False}]


def test_the_acceptance_record_names_this_campaign_evidence_and_qualification(
    monkeypatch,
) -> None:
    fakes = Fakes(monkeypatch)
    run(fakes.activation().activate())
    a, t = fakes.acceptance.as_dict(), fakes.tuple
    m = fakes.records["manifest"].as_dict()
    assert a == {
        "schema_version": 1,
        "record_type": "commissioning_acceptance",
        "campaign": {
            "campaign_id": t.campaign_id,
            "campaign_generation": t.campaign_generation,
            "permit_digest": t.permit,
            "bootstrap_digest": t.bootstrap,
            "manifest_digest": t.manifest,
            "runtime_id": m["runtime_id"],
            "runtime_generation": m["runtime_generation"],
            "broker_run_id": t.broker_run_id,
        },
        "evidence_digest": EVIDENCE,
        "qualification_id": m["qualification_id"],
        "qualification_generation": "1",
        "accepted_at_ms": "1500",
    }


def test_an_already_closed_journal_is_not_closed_again(monkeypatch) -> None:
    fakes = Fakes(monkeypatch, state="closed")
    run(fakes.activation().activate())
    assert fakes.events == [e for e in ORDER if e != "confirm_closed"]


@pytest.mark.parametrize("state", ["prepared", "running", "failed"])
def test_a_journal_that_is_not_closing_refuses_before_any_write(monkeypatch, state) -> None:
    fakes = Fakes(monkeypatch, state=state)
    fakes.refused("journal-closed")
    assert fakes.events == ["status"]


def test_unconfirmed_settlement_leaves_the_journal_closing(monkeypatch) -> None:
    fakes = Fakes(monkeypatch)
    fakes.fail["settled"] = RuntimeError("absence not confirmed")
    error = fakes.refused("journal-closed")
    assert fakes.events == ["status", "settled"]
    assert fakes.state == "closing"
    assert "absence" not in str(error)


# Each failure stops before the next write; the earlier writes stay, so a
# restarted activation finds a closed journal and a disabled candidate set.
@pytest.mark.parametrize(
    ("event", "step"),
    [
        ("confirm_closed", "journal-closed"),
        ("disable_candidates", "candidates-disabled"),
        ("acl", "candidates-disabled"),
        ("decide", "accepted"),
        ("issuer_time", "accepted"),
        ("issue_acceptance", "accepted"),
        ("publish", "published"),
        ("open_admission", "admission-open"),
        ("confirm_unchanged", "tuple-unchanged"),
    ],
)
def test_a_failed_step_stops_before_the_next_write(monkeypatch, event, step) -> None:
    fakes = Fakes(monkeypatch)
    fakes.fail[event] = RuntimeError("secret diagnostic")
    error = fakes.refused(step)
    assert fakes.events[-1] == event or fakes.events[-1] == "close_admission"
    assert fakes.events[: ORDER.index(event) + 1] == ORDER[: ORDER.index(event) + 1]
    assert "secret" not in str(error) and error.__cause__ is None


def test_a_refused_verdict_writes_no_acceptance(monkeypatch) -> None:
    from curie_protected_hooks.commissioning_verifier import Verdict

    fakes = Fakes(monkeypatch)
    fakes.verdict = Verdict("refused", "missing:revocation", fakes.tuple.digest, None, ())
    fakes.refused("accepted")
    assert fakes.events[-1] == "decide"


@pytest.mark.parametrize("outcome", ["uncertain", "refused"])
def test_an_unconfirmed_publication_never_opens_admission(monkeypatch, outcome) -> None:
    from curie_protected_hooks.commissioning_verifier import Publication

    fakes = Fakes(monkeypatch)
    fakes.publication = Publication(outcome, "readiness-unconfirmed")
    fakes.refused("published")
    assert "open_admission" not in fakes.events


def test_a_changed_tuple_after_opening_closes_admission_again(monkeypatch) -> None:
    fakes = Fakes(monkeypatch)
    fakes.unchanged = "changed-installation"
    fakes.refused("tuple-unchanged")
    assert fakes.events == [*ORDER, "close_admission"]


def test_an_activation_runs_once(monkeypatch) -> None:
    fakes = Fakes(monkeypatch)
    activation = fakes.activation()
    run(activation.activate())
    with pytest.raises(module().ActivationRefused):
        run(activation.activate())
    assert fakes.events == ORDER


def test_cancellation_after_publication_starts_still_finishes_the_transition(
    monkeypatch,
) -> None:
    fakes = Fakes(monkeypatch)
    started = asyncio.Event()
    release = asyncio.Event()
    original = fakes.verifier.publish

    async def slow(verdict: Any, acceptance: Any) -> Any:
        started.set()
        await release.wait()
        return await original(verdict, acceptance)

    fakes.verifier.publish = slow  # type: ignore[method-assign]

    async def scenario() -> None:
        task = asyncio.create_task(fakes.activation().activate())
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    run(scenario())
    # Writes after acceptance are not abandoned half way: the selection
    # either stays closed or the transition completes its last check.
    assert fakes.events == ORDER


@pytest.mark.parametrize(
    "change",
    [
        {"qualification_generation": "x"},
        {"trusted_max_readiness_ms": 0},
        {"max_permit_ms": "86400000"},
        {"settled": None},
        {"verifier": object()},
        {"permit": None},
    ],
)
def test_malformed_inputs_refuse_before_any_call(monkeypatch, change) -> None:
    fakes = Fakes(monkeypatch)
    with pytest.raises(module().ActivationRefused):
        fakes.activation(**change)
    assert fakes.events == []


def test_another_campaigns_journal_refuses_before_any_call(monkeypatch) -> None:
    fakes = Fakes(monkeypatch)
    other = records(campaign_id="99999999-9999-4999-8999-999999999999")["permit"]
    fakes.journal.permit = other  # type: ignore[misc]
    with pytest.raises(module().ActivationRefused):
        fakes.activation()
    assert fakes.events == []


# --- The selection compare and set keeps every other member ---------------


def test_the_opened_selection_changes_only_admission_open() -> None:
    api = module()
    r = records()
    closed = canonical(selection(r))
    opened = api.opened_selection(closed)
    assert json.loads(opened) == dict(json.loads(closed), admission_open=True)
    # Every other member is byte equal: only the one boolean token differs.
    assert opened.replace(b'"admission_open":true', b'"admission_open":false') == closed


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{}",
        b"not json",
        canonical(selection(records(), admission_open=True)),
        canonical(dict(selection(records()), extra=1)),
        canonical(selection(records(), admission_open=0)),
        json.dumps(selection(records())).encode(),  # not canonical
    ],
)
def test_only_a_canonical_closed_selection_opens(raw: bytes) -> None:
    with pytest.raises(module().ActivationRefused):
        module().opened_selection(raw)


def test_the_module_is_outside_every_production_composition() -> None:
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    contracts = tomllib.loads((root / "pyproject.toml").read_text())["tool"]["importlinter"][
        "contracts"
    ]
    (contract,) = [
        c
        for c in contracts
        if c["name"] == "Production composition never imports commissioning records"
    ]
    assert "curie_protected_hooks.commissioning_activation" in contract["forbidden_modules"]

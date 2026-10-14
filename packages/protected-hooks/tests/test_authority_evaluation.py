"""Shared pure authority evaluation, @spec PROTECTED-HOOK-SOURCE-9 PROTECTED-HOOK-ADMISSION-4.

``curie_protected_hooks.authority_evaluation`` owns the one decision the
support probe and atomic admission both make over one set of reads: the source
record, the selection, manifest, qualification and readiness control bytes and
one broker observation, against a target built from the committed row (or the
original intent) and the provisioner's trusted manifest. Its outcome is closed
and decided in the probe's step order: step 1, then steps 3 through 11. A
frozen table maps outcomes to admission reasons.

These are unit tests over in-memory records only: the module is pure, so no
broker, file or network is involved. The interface pinned here:

* ``AuthorityTarget(generation, operation_id, policy_fingerprint, runtime_id,
  qualification_id, bundle_digest)`` keyword constructed;
* ``AuthorityReads(source, selection, manifest, qualification, readiness,
  observation)`` keyword constructed, ``source`` a decoded ``SourceState`` and
  the four control records exact bytes or ``None``;
* ``evaluate_authority(target, reads, *, trusted_manifest,
  trusted_max_readiness_ms, phase)`` with ``phase`` ``"admission"`` or
  ``"publication"``, returning the outcome (a ``str`` value, or an object whose
  ``outcome`` is one);
* ``ADMISSION_REASONS``, a read only mapping from each refusal outcome to its
  admission reason.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
import socket

import pytest
from curie_protected_hooks.authority_records import parse_manifest
from curie_protected_hooks.broker_metadata import BrokerObservation

RUN_ID = "a" * 40
RUNTIME = "33333333-3333-4333-8333-333333333333"
QUALIFICATION = "44444444-4444-4444-8444-444444444444"
OTHER = "66666666-6666-4666-8666-666666666666"
OPERATION = "22222222-2222-4222-8222-222222222222"
NEW_OPERATION = "77777777-7777-4777-8777-777777777777"
BUNDLE = "f" * 64
FINGERPRINT = "b" * 64
GENERATION = 7
MAX_READINESS_MS = 60000
NOW_MS = 1_800_000_000_000

OUTCOMES = frozenset(
    {
        "accept",
        "source_closed",
        "runtime_unavailable",
        "broker_identity_mismatch",
        "qualification_unavailable",
        "evidence_missing",
        "evidence_expired",
        "configuration_unsupported",
        "admission_closed",
    }
)

# @spec PROTECTED-HOOK-SOURCE-9: the frozen outcome to admission reason table.
EXPECTED_ADMISSION_REASONS = {
    "source_closed": "source_unavailable",
    "configuration_unsupported": "runtime_unavailable",
    "runtime_unavailable": "runtime_unavailable",
    "evidence_missing": "evidence_unavailable",
    "evidence_expired": "evidence_unavailable",
    "broker_identity_mismatch": "broker_identity_mismatch",
    "qualification_unavailable": "qualification_unavailable",
    "admission_closed": "admission_closed",
}


def evaluation():
    """Missing product module is an assertion in the test body, @spec PROTECTED-HOOK-SOURCE-9."""
    name = "curie_protected_hooks.authority_evaluation"
    assert importlib.util.find_spec(name) is not None, "shared authority evaluation module absent"
    return importlib.import_module(name)


def canonical(value):
    """Independent canonical oracle, @spec PROTECTED-HOOK-LANE-2."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def outcome_of(result):
    """The outcome value of one evaluation, @spec PROTECTED-HOOK-SOURCE-9."""
    value = getattr(result, "outcome", result)
    assert isinstance(value, str), "the evaluation outcome is not a closed string value"
    assert value in OUTCOMES, "the evaluation returned an outcome outside the closed set"
    return str(value)


class Tuple:
    """One provisioned tuple in memory; defaults form an accepted target.

    A case mutates exactly the facts it names. @spec PROTECTED-HOOK-SOURCE-9.
    """

    def __init__(self):
        """@spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2."""
        self.manifest = {
            "schema_version": 1,
            "runtime_id": RUNTIME,
            "runtime_generation": "1",
            "broker_identity": {
                "instance_id": "11111111-1111-4111-8111-111111111111",
                "endpoint": {"host": "127.0.0.1", "port": 6379},
                "tls_server_name": "127.0.0.1",
                "tls_spki_sha256": "9" * 64,
                "run_id": RUN_ID,
                "database": 0,
            },
            "worker_image_digest": "sha256:" + "d" * 64,
            "runner_image_digest": "sha256:" + "e" * 64,
            "bundle_digest": {"sha256": BUNDLE, "object_identity": "bundle/example"},
            "execution_config_digest": "1" * 64,
            "qualification_id": QUALIFICATION,
            "substrate": {
                "kind": "docker",
                "authority_domain_id": "55555555-5555-4555-8555-555555555555",
                "launch_identity": "launch/example",
                "launch_config_sha256": "2" * 64,
            },
            "guard_identity": {
                "control_id": "22222222-2222-4222-8222-222222222222",
                "revision": "1",
                "config_sha256": "c" * 64,
            },
            "credential_refs": {
                role: {"id": "credential/example-" + role, "generation": "1"}
                for role in ("enqueue", "worker", "verifier")
            },
        }
        self.now_ms = NOW_MS
        self.run_id = RUN_ID
        self.max_readiness_ms = MAX_READINESS_MS
        self.target = dict(
            generation=GENERATION,
            operation_id=OPERATION,
            policy_fingerprint=FINGERPRINT,
            runtime_id=RUNTIME,
            qualification_id=QUALIFICATION,
            bundle_digest=BUNDLE,
        )
        self.source = {
            "floor": GENERATION,
            "operation_id": OPERATION,
            "active": {
                "generation": GENERATION,
                "operation_id": OPERATION,
                "mode": "protected",
                "policy_fingerprint": FINGERPRINT,
            },
        }
        # name -> exact bytes (None = absent) overriding the derived encoding.
        self.raw = {}
        self.rebuild()

    def __repr__(self):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        return "<fixture-tuple>"

    @property
    def manifest_digest(self):
        """@spec PROTECTED-HOOK-LANE-2."""
        return hashlib.sha256(canonical(self.manifest)).hexdigest()

    def rebuild(self):
        """Derive qualification, readiness and selection from the manifest.

        @spec PROTECTED-HOOK-LANE-2 @spec PROTECTED-HOOK-SOURCE-9.
        """
        m = self.manifest
        self.qualification = {
            key: copy.deepcopy(m[key])
            for key in (
                "schema_version",
                "runtime_id",
                "runtime_generation",
                "qualification_id",
                "broker_identity",
                "execution_config_digest",
                "guard_identity",
            )
        }
        self.qualification.update(
            qualification_generation="1",
            manifest_digest=self.manifest_digest,
            measurement_record_id="measurement/qualification",
        )
        issued = self.now_ms - 1000
        self.readiness = {
            key: copy.deepcopy(self.qualification[key])
            for key in (
                "schema_version",
                "runtime_id",
                "runtime_generation",
                "qualification_id",
                "qualification_generation",
                "broker_identity",
                "guard_identity",
                "manifest_digest",
            )
        }
        self.readiness.update(
            verifier_identity=copy.deepcopy(m["credential_refs"]["verifier"]),
            issued_at_ms=str(issued),
            expires_at_ms=str(issued + MAX_READINESS_MS),
            measurement_record_id="measurement/readiness",
        )
        self.selection = dict(
            schema_version=1,
            runtime_id=m["runtime_id"],
            runtime_generation=m["runtime_generation"],
            manifest_digest=self.manifest_digest,
            qualification_id=m["qualification_id"],
            qualification_generation="1",
            broker_run_id=m["broker_identity"]["run_id"],
            admission_open=True,
        )

    def record(self, name):
        """Exact control bytes or None, @spec PROTECTED-HOOK-SOURCE-9."""
        if name in self.raw:
            return self.raw[name]
        return canonical(getattr(self, name))

    def evaluate(self, phase="admission", trusted=None):
        """One pure evaluation over this tuple, @spec PROTECTED-HOOK-SOURCE-9."""
        module = evaluation()
        reads = module.AuthorityReads(
            source=copy.deepcopy(self.source),
            selection=self.record("selection"),
            manifest=self.record("manifest"),
            qualification=self.record("qualification"),
            readiness=self.record("readiness"),
            observation=BrokerObservation(run_id=self.run_id, now_ms=self.now_ms),
        )
        target = module.AuthorityTarget(**self.target)
        return module.evaluate_authority(
            target,
            reads,
            trusted_manifest=trusted or parse_manifest(canonical(self.manifest)),
            trusted_max_readiness_ms=self.max_readiness_ms,
            phase=phase,
        )


# -- case mutations ---------------------------------------------------------------------


def _valid(t):
    """@spec PROTECTED-HOOK-SOURCE-9."""


def _target(**changes):
    """The committed row (or original intent) differs, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        t.target.update(changes)

    return apply


def _active(**changes):
    """The active source record differs from the target, @spec PROTECTED-HOOK-SOURCE-6/9."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-6."""
        t.source["active"].update(changes)

    return apply


def _source(value):
    """The whole decoded source record, @spec PROTECTED-HOOK-SOURCE-6."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-6."""
        t.source = copy.deepcopy(value)

    return apply


def _later_reservation(t):
    """A later reservation revoked the target's active record, @spec PROTECTED-HOOK-SOURCE-6/7."""
    t.source = {"floor": GENERATION + 1, "operation_id": NEW_OPERATION, "active": None}


def _reserved_only(t):
    """The target's reservation is held but nothing is published, @spec PROTECTED-HOOK-SOURCE-6."""
    t.source = {"floor": GENERATION, "operation_id": OPERATION, "active": None}


def _raw(name, value):
    """Exact bytes for one control record, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        t.raw[name] = value

    return apply


def _malformed(name):
    """Valid JSON that is not the record's closed shape, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        t.raw[name] = canonical({**getattr(t, name), "extra": True})

    return apply


def _selection(**changes):
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        t.selection.update(changes)

    return apply


def _selection_open_string(t):
    """@spec PROTECTED-HOOK-SOURCE-9."""
    t.raw["selection"] = canonical({**t.selection, "admission_open": "true"})


def _manifest_control_differs(t):
    """A different valid manifest stored under the trusted digest, @spec PROTECTED-HOOK-SOURCE-9."""
    other = copy.deepcopy(t.manifest)
    other["worker_image_digest"] = "sha256:" + "9" * 64
    t.raw["manifest"] = canonical(other)


def _observed_run_id(t):
    """The live broker epoch is not the manifest's, @spec PROTECTED-HOOK-LANE-2."""
    t.run_id = "0" * 40


def _expired(t):
    """@spec PROTECTED-HOOK-SOURCE-9."""
    t.readiness.update(
        issued_at_ms=str(t.now_ms - 2 * MAX_READINESS_MS),
        expires_at_ms=str(t.now_ms - MAX_READINESS_MS),
    )


def _expires_now(t):
    """Broker time equal to expiry is expired, @spec PROTECTED-HOOK-SOURCE-9."""
    t.readiness.update(issued_at_ms=str(t.now_ms - MAX_READINESS_MS), expires_at_ms=str(t.now_ms))


def _expired_and_window_too_long(t):
    """Expired and also invalid under step 9: step 8 decides, @spec PROTECTED-HOOK-SOURCE-9."""
    _expired(t)
    t.max_readiness_ms = 1000


def _future_issued(t):
    """@spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2."""
    t.readiness.update(
        issued_at_ms=str(t.now_ms + MAX_READINESS_MS),
        expires_at_ms=str(t.now_ms + 2 * MAX_READINESS_MS),
    )


def _short_max(t):
    """The readiness window exceeds the trusted bound, @spec PROTECTED-HOOK-LANE-2."""
    t.max_readiness_ms = 1000


def _qualification_digest(t):
    """@spec PROTECTED-HOOK-LANE-2."""
    t.qualification["manifest_digest"] = "0" * 64


def _verifier_differs(t):
    """@spec PROTECTED-HOOK-LANE-2."""
    t.readiness["verifier_identity"] = {"id": "credential/example-verifier", "generation": "2"}


def _closed(t):
    """@spec PROTECTED-HOOK-SOURCE-9."""
    t.selection["admission_open"] = False


def _row(**changes):
    """Target references differ from the tuple, with admission also closed.

    Closing admission proves step 10 precedes step 11. @spec PROTECTED-HOOK-SOURCE-9.
    """

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        t.target.update(changes)
        t.selection["admission_open"] = False

    return apply


def _both(*mutations):
    """Faults at several steps at once, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(t):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        for mutation in mutations:
            mutation(t)

    return apply


# (id, mutation, outcome) in the admission phase.
STEPS = [
    ("valid", _valid, "accept"),
    # 1. one runtime per deployment, before any record is consulted.
    ("1-target-runtime-differs", _target(runtime_id=OTHER), "configuration_unsupported"),
    (
        "1-precedes-3",
        _both(
            _target(runtime_id=OTHER), _source({"floor": 0, "operation_id": None, "active": None})
        ),
        "configuration_unsupported",
    ),
    # 3. the active source record is exactly the target.
    (
        "3-source-absent",
        _source({"floor": 0, "operation_id": None, "active": None}),
        "source_closed",
    ),
    ("3-reserved-only", _reserved_only, "source_closed"),
    ("3-later-reservation", _later_reservation, "source_closed"),
    ("3-generation-differs", _target(generation=GENERATION + 1), "source_closed"),
    ("3-operation-differs", _target(operation_id=NEW_OPERATION), "source_closed"),
    ("3-mode-ordinary", _active(mode="ordinary"), "source_closed"),
    ("3-fingerprint-differs", _active(policy_fingerprint="0" * 64), "source_closed"),
    ("3-precedes-4", _both(_reserved_only, _raw("selection", None)), "source_closed"),
    # 4. the selection names the trusted manifest, whose control bytes match it.
    ("4-selection-absent", _raw("selection", None), "runtime_unavailable"),
    ("4-selection-not-json", _raw("selection", b"{not json"), "runtime_unavailable"),
    ("4-selection-malformed", _malformed("selection"), "runtime_unavailable"),
    ("4-selection-open-not-bool", _selection_open_string, "runtime_unavailable"),
    ("4-selection-other-manifest", _selection(manifest_digest="0" * 64), "runtime_unavailable"),
    ("4-manifest-control-absent", _raw("manifest", None), "runtime_unavailable"),
    ("4-manifest-control-not-json", _raw("manifest", b"{not json"), "runtime_unavailable"),
    ("4-manifest-control-malformed", _malformed("manifest"), "runtime_unavailable"),
    ("4-manifest-control-differs", _manifest_control_differs, "runtime_unavailable"),
    (
        "4-precedes-5",
        _both(_manifest_control_differs, _selection(broker_run_id="0" * 40)),
        "runtime_unavailable",
    ),
    # 5. the selected and the observed broker epoch are the manifest's.
    ("5-selection-run-id-differs", _selection(broker_run_id="0" * 40), "broker_identity_mismatch"),
    ("5-observed-run-id-differs", _observed_run_id, "broker_identity_mismatch"),
    (
        "5-precedes-6",
        _both(_observed_run_id, _raw("qualification", None)),
        "broker_identity_mismatch",
    ),
    # 6. the selected qualification is present (malformed counts as absent).
    ("6-qualification-absent", _raw("qualification", None), "qualification_unavailable"),
    ("6-qualification-not-json", _raw("qualification", b"{not json"), "qualification_unavailable"),
    ("6-qualification-malformed", _malformed("qualification"), "qualification_unavailable"),
    (
        "6-precedes-7",
        _both(_raw("qualification", None), _raw("readiness", None)),
        "qualification_unavailable",
    ),
    # 7. the selected readiness is present (malformed counts as absent).
    ("7-readiness-absent", _raw("readiness", None), "evidence_missing"),
    ("7-readiness-not-json", _raw("readiness", b"{not json"), "evidence_missing"),
    ("7-readiness-malformed", _malformed("readiness"), "evidence_missing"),
    ("7-precedes-8", _both(_raw("readiness", None), _expired), "evidence_missing"),
    # 8. broker time has not reached the readiness expiry.
    ("8-readiness-expired", _expired, "evidence_expired"),
    ("8-expiry-equals-broker-time", _expires_now, "evidence_expired"),
    ("8-precedes-9", _expired_and_window_too_long, "evidence_expired"),
    # 9. the trusted facts bind the tuple and the selection names it exactly.
    ("9-window-exceeds-trusted-max", _short_max, "qualification_unavailable"),
    ("9-readiness-issued-in-future", _future_issued, "qualification_unavailable"),
    ("9-qualification-other-manifest", _qualification_digest, "qualification_unavailable"),
    ("9-readiness-other-verifier", _verifier_differs, "qualification_unavailable"),
    (
        "9-selection-qualification-generation",
        _selection(qualification_generation="2"),
        "qualification_unavailable",
    ),
    ("9-selection-runtime-id", _selection(runtime_id=OTHER), "qualification_unavailable"),
    (
        "9-selection-qualification-id",
        _selection(qualification_id=OTHER),
        "qualification_unavailable",
    ),
    (
        "9-selection-runtime-generation",
        _selection(runtime_generation="2"),
        "qualification_unavailable",
    ),
    (
        "9-precedes-10",
        _both(_short_max, _target(bundle_digest="a" * 64)),
        "qualification_unavailable",
    ),
    # 10. the target references the selected qualification and bundle (before 11).
    ("10-qualification-differs", _row(qualification_id=OTHER), "configuration_unsupported"),
    ("10-bundle-differs", _row(bundle_digest="a" * 64), "configuration_unsupported"),
    # 11. admission is open.
    ("11-admission-closed", _closed, "admission_closed"),
]


@pytest.mark.parametrize(
    "mutate,outcome", [case[1:] for case in STEPS], ids=[case[0] for case in STEPS]
)
def test_first_failing_step_decides_the_outcome(mutate, outcome):
    """Each ordered step's first failing condition decides; malformed reads as absent.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4 @spec PROTECTED-HOOK-LANE-2.
    """
    t = Tuple()
    mutate(t)
    assert outcome_of(t.evaluate("admission")) == outcome


def test_one_millisecond_before_expiry_still_accepts():
    """Expiry is exclusive at broker time, @spec PROTECTED-HOOK-SOURCE-9."""
    t = Tuple()
    t.readiness.update(
        issued_at_ms=str(t.now_ms - MAX_READINESS_MS + 1), expires_at_ms=str(t.now_ms + 1)
    )
    assert outcome_of(t.evaluate("admission")) == "accept"


# (id, mutation, outcome) in the publication phase.
PUBLICATION = [
    ("reservation-held-without-active", _reserved_only, "accept"),
    ("reservation-held-and-already-active", _valid, "accept"),
    ("reservation-held-admission-closed", _both(_reserved_only, _closed), "admission_closed"),
    ("no-record", _source({"floor": 0, "operation_id": None, "active": None}), "source_closed"),
    ("later-reservation", _later_reservation, "source_closed"),
    (
        "reservation-other-operation",
        _source({"floor": GENERATION, "operation_id": NEW_OPERATION, "active": None}),
        "source_closed",
    ),
    (
        "reservation-other-generation",
        _source({"floor": GENERATION - 1, "operation_id": OPERATION, "active": None}),
        "source_closed",
    ),
    (
        "reservation-held-evidence-expired",
        _both(_reserved_only, _expired),
        "evidence_expired",
    ),
    (
        "reservation-held-evidence-missing",
        _both(_reserved_only, _raw("readiness", None)),
        "evidence_missing",
    ),
    (
        "reservation-held-bundle-differs",
        _both(_reserved_only, _target(bundle_digest="a" * 64)),
        "configuration_unsupported",
    ),
    (
        "reservation-held-runtime-differs",
        _both(_reserved_only, _target(runtime_id=OTHER)),
        "configuration_unsupported",
    ),
]


@pytest.mark.parametrize(
    "mutate,outcome", [case[1:] for case in PUBLICATION], ids=[case[0] for case in PUBLICATION]
)
def test_publication_phase_checks_the_reservation_instead_of_the_active_record(mutate, outcome):
    """Publication accepts a held reservation with no active record; steps 4 to 11 are unchanged.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    t = Tuple()
    mutate(t)
    assert outcome_of(t.evaluate("publication")) == outcome


def test_admission_phase_refuses_a_reservation_the_publication_phase_accepts():
    """The two phases differ only at step 3, @spec PROTECTED-HOOK-SOURCE-6/9."""
    t = Tuple()
    _reserved_only(t)
    assert outcome_of(t.evaluate("admission")) == "source_closed"
    assert outcome_of(t.evaluate("publication")) == "accept"


@pytest.mark.parametrize("phase", ["", "probe", "ADMISSION", None, 1])
def test_an_unknown_phase_is_refused(phase):
    """The phase set is closed, @spec PROTECTED-HOOK-SOURCE-9."""
    with pytest.raises((ValueError, TypeError)):
        Tuple().evaluate(phase)


def test_the_admission_reason_table_is_frozen_and_complete():
    """Outcomes map to admission reasons as the contract tables, read only.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4.
    """
    module = evaluation()
    table = module.ADMISSION_REASONS
    refusals = {key: table[key] for key in table if key != "accept"}
    assert refusals == EXPECTED_ADMISSION_REASONS
    assert set(table) <= OUTCOMES
    with pytest.raises(TypeError):
        table["source_closed"] = "accept"
    with pytest.raises((TypeError, AttributeError)):
        del table["source_closed"]
    assert dict(module.ADMISSION_REASONS) == dict(table)


def test_every_outcome_of_the_vector_has_an_admission_reason_or_accepts():
    """No refusal escapes the frozen table.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4.
    """
    table = evaluation().ADMISSION_REASONS
    for _name, mutate, _outcome in STEPS + PUBLICATION:
        t = Tuple()
        mutate(t)
        for phase in ("admission", "publication"):
            outcome = outcome_of(t.evaluate(phase))
            assert outcome == "accept" or outcome in table


def test_evaluation_is_pure(monkeypatch):
    """No network, no input mutation and the same answer twice, @spec PROTECTED-HOOK-SOURCE-9."""
    attempts = []

    def forbidden(*args, **kwargs):
        """Network tripwire, @spec PROTECTED-HOOK-SOURCE-9."""
        attempts.append(True)
        raise AssertionError("the pure evaluation attempted network I/O")

    module = evaluation()
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    t = Tuple()
    source = copy.deepcopy(t.source)
    reads = module.AuthorityReads(
        source=t.source,
        selection=t.record("selection"),
        manifest=t.record("manifest"),
        qualification=t.record("qualification"),
        readiness=t.record("readiness"),
        observation=BrokerObservation(run_id=t.run_id, now_ms=t.now_ms),
    )
    target = module.AuthorityTarget(**t.target)
    trusted = parse_manifest(canonical(t.manifest))
    results = [
        outcome_of(
            module.evaluate_authority(
                target,
                reads,
                trusted_manifest=trusted,
                trusted_max_readiness_ms=MAX_READINESS_MS,
                phase="admission",
            )
        )
        for _ in range(2)
    ]
    assert results == ["accept", "accept"]
    assert t.source == source
    assert attempts == []


def test_target_and_reads_are_immutable_records():
    """Frozen inputs that cannot be retargeted after construction, @spec PROTECTED-HOOK-SOURCE-9."""
    module = evaluation()
    t = Tuple()
    target = module.AuthorityTarget(**t.target)
    with pytest.raises((AttributeError, TypeError)):
        target.generation = GENERATION + 1
    reads = module.AuthorityReads(
        source=t.source,
        selection=t.record("selection"),
        manifest=t.record("manifest"),
        qualification=t.record("qualification"),
        readiness=t.record("readiness"),
        observation=BrokerObservation(run_id=t.run_id, now_ms=t.now_ms),
    )
    with pytest.raises((AttributeError, TypeError)):
        reads.selection = None

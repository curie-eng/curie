"""Probe evaluation and atomic admission decide alike, @spec PROTECTED-HOOK-SOURCE-9
PROTECTED-HOOK-ADMISSION-4 PROTECTED-HOOK-ADMISSION-7.

One frozen vector of broker states is seeded on the owned disposable TLS
Valkey of ``admission_broker.py``. For each state the probe's path reads it
with a distinct owned ``control_reader`` principal through
``AuthenticatedMetadataReader`` and decides with the shared
``authority_evaluation`` in its admission phase; admission's path calls
``AtomicAdmission.admit`` with the enqueue principal on the same state. A
delivery is accepted exactly when the evaluation returns ``accept``, and every
refusal carries the evaluation outcome's admission reason from the frozen
table, with no broker write.

The facade receives the provisioner's trusted manifest
(``trusted_manifest``), as the ingress wiring amendment to ADMISSION-1
specifies, so a control manifest differing from the trusted one refuses on
both paths.
"""

from __future__ import annotations

import copy
import secrets

import pytest
from curie_protected_hooks.admission_records import parse_selection
from curie_protected_hooks.broker_metadata import metadata_acl_rules
from curie_protected_hooks.broker_transport import (
    AuthenticatedMetadataReader,
    MetadataReaderCredential,
)
from curie_protected_hooks.source_policy_records import policy_fingerprint

from . import admission_broker as broker_helpers
from .admission_broker import (
    AGENT,
    HOOK,
    SOURCE,
    FixtureSecret,
    canonical,
    client,
    install,
    module,
    request,
    seed,
    snapshot,
    source_policy,
)

admission_broker = broker_helpers.admission_broker
admission_service = broker_helpers.admission_service

OTHER = "66666666-6666-4666-8666-666666666666"
MAX_READINESS_MS = 60000


@pytest.fixture
def control_reader(admission_broker):
    """An owned control reader principal beside the enqueue one, @spec PROTECTED-HOOK-LANE-3."""
    username = "reader-" + secrets.token_hex(6)
    password = FixtureSecret(secrets.token_hex(24))
    admission_broker.command(
        "ACL",
        "SETUSER",
        username,
        "reset",
        "on",
        ">" + password,
        *metadata_acl_rules("control_reader"),
    )
    try:
        yield MetadataReaderCredential(username, password)
    finally:
        admission_broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
        admission_broker.command("ACL", "DELUSER", username)


def keys_of(values):
    """Name the seeded keys by role, @spec PROTECTED-HOOK-SOURCE-9."""
    names = {}
    for key in values:
        if key == SOURCE:
            names["source"] = key
        else:
            names[key.split(":")[2]] = key
    return names


def probe_outcome(broker, credential, policy):
    """The probe's reads on one reader session, then the shared evaluation.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """
    evaluation = module("authority_evaluation")
    trusted = broker.manifest()
    runtime = trusted.as_dict()["runtime_id"]
    reader = AuthenticatedMetadataReader.connect(trusted, credential, broker.ca_pem)
    try:
        source = reader.read_source(AGENT, HOOK)
        selection_raw = reader.read_control(f"protected:control:selection:{runtime}")
        manifest_raw = reader.read_control(f"protected:control:manifest:{trusted.digest}")
        qualification_raw = readiness_raw = None
        try:
            selection = parse_selection(selection_raw) if selection_raw is not None else None
        except ValueError:
            selection = None
        if selection is not None:
            qualification_raw = reader.read_control(
                "protected:control:qualification:"
                f"{selection['qualification_id']}:{selection['qualification_generation']}"
            )
            readiness_raw = reader.read_control(
                f"protected:control:readiness:{runtime}:{selection['runtime_generation']}"
            )
        observation = reader.observe()
    finally:
        reader.close()
    reads = evaluation.AuthorityReads(
        source=source,
        selection=selection_raw,
        manifest=manifest_raw,
        qualification=qualification_raw,
        readiness=readiness_raw,
        observation=observation,
    )
    target = evaluation.AuthorityTarget(
        generation=int(policy["generation"]),
        operation_id=policy["operation_id"],
        policy_fingerprint=policy_fingerprint(policy),
        runtime_id=policy["runtime_id"],
        qualification_id=policy["qualification_id"],
        bundle_digest=policy["bundle_digest"],
    )
    result = evaluation.evaluate_authority(
        target,
        reads,
        trusted_manifest=trusted,
        trusted_max_readiness_ms=MAX_READINESS_MS,
        phase="admission",
    )
    return str(getattr(result, "outcome", result))


# -- the frozen vector: each mutates the seeded values and the committed policy ----------


def _valid(values, names, policy, now_ms):
    """@spec PROTECTED-HOOK-SOURCE-9."""


def _absent(role):
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(values, names, policy, now_ms):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        values[names[role]] = None

    return apply


def _malformed(role):
    """A present record of the wrong shape counts as absent, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(values, names, policy, now_ms):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        values[names[role]] = {**values[names[role]], "extra": True}

    return apply


def _reserved_only(values, names, policy, now_ms):
    """A later reservation revoked the active record, @spec PROTECTED-HOOK-SOURCE-6."""
    values[names["source"]] = dict(
        floor=str(int(policy["generation"]) + 1),
        operation_id="77777777-7777-4777-8777-777777777777",
        active=None,
    )


def _ordinary_active(values, names, policy, now_ms):
    """@spec PROTECTED-HOOK-SOURCE-6."""
    values[names["source"]]["active"]["mode"] = "ordinary"


def _manifest_control_differs(values, names, policy, now_ms):
    """Control manifest bytes differ from the trusted manifest, @spec PROTECTED-HOOK-ADMISSION-1."""
    other = copy.deepcopy(values[names["manifest"]])
    other["worker_image_digest"] = "sha256:" + "9" * 64
    values[names["manifest"]] = other


def _selection(**changes):
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(values, names, policy, now_ms):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        values[names["selection"]].update(changes)

    return apply


def _expired(values, names, policy, now_ms):
    """Readiness expired at broker time.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4.
    """
    values[names["readiness"]].update(
        issued_at_ms=str(now_ms - 2 * MAX_READINESS_MS),
        expires_at_ms=str(now_ms - MAX_READINESS_MS),
    )


def _verifier_differs(values, names, policy, now_ms):
    """@spec PROTECTED-HOOK-LANE-2."""
    values[names["readiness"]]["verifier_identity"] = {
        "id": "credential/example-verifier",
        "generation": "2",
    }


def _row(**changes):
    """The committed row differs from the tuple; its own source record is published.

    @spec PROTECTED-HOOK-SOURCE-9.
    """

    def apply(values, names, policy, now_ms):
        """@spec PROTECTED-HOOK-SOURCE-9."""
        policy.update(changes)
        values[names["source"]]["active"]["policy_fingerprint"] = policy_fingerprint(policy)

    return apply


def _closed(values, names, policy, now_ms):
    """@spec PROTECTED-HOOK-SOURCE-9."""
    values[names["selection"]]["admission_open"] = False


VECTOR = [
    ("valid", _valid),
    ("source-absent", _absent("source")),
    ("source-reserved-by-later-operation", _reserved_only),
    ("source-ordinary", _ordinary_active),
    ("selection-absent", _absent("selection")),
    ("selection-malformed", _malformed("selection")),
    ("manifest-control-absent", _absent("manifest")),
    ("manifest-control-differs-from-trusted", _manifest_control_differs),
    ("selection-run-id-differs", _selection(broker_run_id="0" * 40)),
    ("qualification-absent", _absent("qualification")),
    ("qualification-malformed", _malformed("qualification")),
    ("readiness-absent", _absent("readiness")),
    ("readiness-malformed", _malformed("readiness")),
    ("readiness-expired", _expired),
    ("readiness-other-verifier", _verifier_differs),
    ("selection-qualification-generation", _selection(qualification_generation="2")),
    ("row-runtime-differs", _row(runtime_id=OTHER)),
    ("row-qualification-differs", _row(qualification_id=OTHER)),
    ("row-bundle-differs", _row(bundle_digest="a" * 64)),
    ("admission-closed", _closed),
]


@pytest.mark.parametrize("mutate", [case[1] for case in VECTOR], ids=[case[0] for case in VECTOR])
def test_probe_evaluation_and_admission_take_the_same_decision(
    admission_broker, control_reader, mutate
):
    """Accepted exactly when the evaluation accepts; a refusal carries the mapped reason.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-ADMISSION-4 @spec PROTECTED-HOOK-ADMISSION-7.
    """
    b = admission_broker
    evaluation = module("authority_evaluation")
    records = module("admission_records")
    atomic = module("atomic_admission")
    install(b, module("admission_acl"))
    values = {key: copy.deepcopy(value) for key, value in seed(b).items()}
    names = keys_of(values)
    now = b.command("TIME")
    now_ms = int(now[0]) * 1000 + int(now[1]) // 1000
    policy = source_policy()
    mutate(values, names, policy, now_ms)
    b.command("FLUSHDB")
    for key, value in values.items():
        if value is not None:
            b.command("SET", key, canonical(value))

    outcome = probe_outcome(b, control_reader, policy)

    enqueue = client(b)
    try:
        facade = atomic.AtomicAdmission(
            enqueue,
            trusted_manifest=b.manifest(),
            trusted_max_readiness_ms=MAX_READINESS_MS,
            backlog_limit=64,
        )
        before = snapshot(b)
        result = facade.admit(request(records, policy=policy)).as_dict()
    finally:
        enqueue.close()
    if outcome == "accept":
        assert result["status"] == "accepted", "the probe accepted a state admission refused"
    else:
        assert result == dict(
            status="refused",
            reason=evaluation.ADMISSION_REASONS[outcome],
            receipt=None,
        ), "admission and the probe evaluation decided differently"
        assert snapshot(b) == before, "a refused admission wrote to the broker"


def test_the_vector_covers_every_refusal_outcome(admission_broker, control_reader):
    """The frozen vector reaches every closed outcome but the impossible ones, @spec
    PROTECTED-HOOK-ADMISSION-7.

    ``broker_identity_mismatch`` through the observed run_id cannot be seeded
    on a live broker whose run_id the manifest pins, so the selection's run_id
    stands for it.
    """
    b = admission_broker
    install(b, module("admission_acl"))
    seen = set()
    for _name, mutate in VECTOR:
        values = {key: copy.deepcopy(value) for key, value in seed(b).items()}
        names = keys_of(values)
        now = b.command("TIME")
        policy = source_policy()
        mutate(values, names, policy, int(now[0]) * 1000 + int(now[1]) // 1000)
        b.command("FLUSHDB")
        for key, value in values.items():
            if value is not None:
                b.command("SET", key, canonical(value))
        seen.add(probe_outcome(b, control_reader, policy))
    assert seen == {
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

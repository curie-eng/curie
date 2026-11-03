"""Qualification and Readiness written in their unchanged v1 grammars on the pinned Valkey,
@spec PROTECTED-COMMISSIONING-6 PROTECTED-HOOK-LANE-2/3.

Each case runs against a real owned TLS Valkey at the pinned image. The
verifier is installed from the unchanged production recipe under the username
the manifest's ``credential_refs`` name, and the Qualification publisher of
design decision 8 from its commissioning recipe. The existing readers,
``validate_authority`` and ``worker_verification._retained_authority``, are the
oracle for the written bytes. A refusal is proved to change no key, value or
TTL.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import importlib.util
import json
import secrets
from typing import Any

import pytest
from redis.exceptions import NoPermissionError

from . import worker_broker as wb
from .admission_broker import FixtureSecret
from .commissioning_journal_broker import broker_now_ms, snapshot

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker

MAX_READINESS_MS = 60000


def publication() -> Any:
    name = "curie_protected_hooks.qualification_publication"
    assert importlib.util.find_spec(name) is not None, "qualification publication not implemented"
    return importlib.import_module(name)


def refused(reason: str | None = None) -> Any:
    return pytest.raises(publication().PublicationRefused, match=reason)


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def qualification_value(manifest: Any, generation: str = "1") -> dict[str, Any]:
    m = manifest.as_dict()
    q = {
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
    q.update(
        qualification_generation=generation,
        manifest_digest=manifest.digest,
        measurement_record_id="measurement/qualification",
    )
    return q


PUBLISHER = {"id": "commissioning/example-qualification-publisher", "generation": "1"}


class Recipes:
    """The merged recipes the provisioner hashes into G."""

    def __init__(self, qualification_id: str) -> None:
        from curie_protected_hooks.commissioning_acl import GuardRecipes

        self._recipes = GuardRecipes(qualification_id)

    def rules(self, principal: str) -> tuple[str, ...]:
        return self._recipes.rules(principal)


def provisioned(broker: Any, *, changes: Any = None, **identity: Any) -> Any:
    """B, G and M constructed by the provisioner core for this broker."""
    from .test_commissioning_provisioner import module as provisioner
    from .test_commissioning_provisioner import profile, profile_fields

    broker_identity = dict(broker.manifest().as_dict()["broker_identity"], **identity)
    qualification_id = profile_fields()["qualification_id"]
    return provisioner().construct(
        profile(broker_identity=broker_identity, **(changes or {})),
        acl=Recipes(qualification_id),
    )


class Installation:
    """Provisioned B, G and M, the manifest seeded, a verifier and a Qualification publisher."""

    def __init__(self, broker: Any) -> None:
        from curie_protected_hooks.admission_acl import admission_acl_rules
        from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

        self.broker = broker
        self.tuple = provisioned(broker)
        self.manifest = self.tuple.manifest
        m = self.manifest.as_dict()
        assert self.tuple.bootstrap.as_dict()["qualification_publisher_identity"] == PUBLISHER
        self.verifier = m["credential_refs"]["verifier"]["id"]
        self.password = FixtureSecret(secrets.token_hex(24))
        self.publisher_password = FixtureSecret(secrets.token_hex(24))
        for username, password, rules in (
            (self.verifier, self.password, admission_acl_rules("verifier")),
            (
                PUBLISHER["id"],
                self.publisher_password,
                commissioning_acl_rules(
                    "qualification_publisher", qualification_id=m["qualification_id"]
                ),
            ),
        ):
            broker.command("ACL", "SETUSER", username, "reset", "on", ">" + password, *rules)
        broker.command("SET", "protected:control:manifest:" + self.manifest.digest, canonical(m))
        self.qualification: Any = None

    def publisher_credential(self, password: Any = None) -> Any:
        from curie_protected_hooks.worker_transport import WorkerCredential

        return WorkerCredential(PUBLISHER["id"], password or self.publisher_password)

    async def qualify(self, **options: Any) -> Any:
        publisher = await publication().QualificationPublisher.connect(
            options.pop("manifest", self.manifest),
            options.pop("credential", self.publisher_credential()),
            self.broker.ca_pem,
            bootstrap=options.pop("bootstrap", self.tuple.bootstrap),
            guard_config=options.pop("guard_config", self.tuple.guard_config),
        )
        try:
            return await publisher.publish(
                qualification_generation=options.pop("qualification_generation", "1"),
                measurement_record_id=options.pop(
                    "measurement_record_id", "measurement/qualification"
                ),
            )
        finally:
            await publisher.close()

    @property
    def qualification_key(self) -> str:
        m = self.manifest.as_dict()
        return "protected:control:qualification:" + m["qualification_id"] + ":1"

    @property
    def readiness_key(self) -> str:
        m = self.manifest.as_dict()
        return "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]

    def credential(self, username: str | None = None, password: Any = None) -> Any:
        from curie_protected_hooks.worker_transport import WorkerCredential

        return WorkerCredential(username or self.verifier, password or self.password)

    async def publisher(self, **options: Any) -> Any:
        manifest = options.pop("manifest", self.manifest)
        credential = options.pop("credential", self.credential())
        return await publication().ReadinessPublisher.connect(
            manifest,
            credential,
            self.broker.ca_pem,
            trusted_max_readiness_ms=options.pop("trusted_max_readiness_ms", MAX_READINESS_MS),
        )

    def remove(self) -> None:
        for username in (self.verifier, PUBLISHER["id"]):
            self.broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
            self.broker.command("ACL", "DELUSER", username)


@pytest.fixture
def unqualified(pworker_service):
    pworker_service.command("FLUSHDB")
    installed = Installation(pworker_service)
    try:
        yield installed
    finally:
        installed.remove()
        pworker_service.command("FLUSHDB")


@pytest.fixture
def installation(pworker_service):
    """An installation whose Qualification the publisher has written."""
    pworker_service.command("FLUSHDB")
    installed = Installation(pworker_service)
    try:
        installed.qualification = run(installed.qualify())
        yield installed
    finally:
        installed.remove()
        pworker_service.command("FLUSHDB")


async def observed() -> None:
    """A design decision 7 reobservation that found no difference."""


async def publish(installation: Installation, **options: Any) -> Any:
    publisher = await installation.publisher(
        **{k: options.pop(k) for k in ("manifest", "credential") if k in options}
    )
    try:
        return await publisher.publish(
            options.pop("qualification", installation.qualification),
            lifetime_ms=options.pop("lifetime_ms", 30000),
            measurement_record_id=options.pop("measurement_record_id", "measurement/readiness"),
            reobserve=options.pop("reobserve", observed),
            expected=options.pop("expected", None),
        )
    finally:
        await publisher.close()


async def retained(installation: Installation, worker: Any) -> None:
    """The unchanged production drain reader, as the production worker."""
    from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
    from curie_protected_hooks.worker_verification import _retained_authority

    client = await AuthenticatedWorkerClient.connect(
        installation.manifest, worker.credential(), installation.broker.ca_pem
    )
    await _retained_authority(client, installation.manifest)


def test_written_readiness_is_accepted_by_the_unchanged_readers(
    pworker_service, owned_worker, installation
) -> None:
    from curie_protected_hooks.authority_records import parse_readiness, validate_authority

    # Before: the unchanged drain reader refuses without Readiness.
    with pytest.raises(Exception):  # noqa: B017, PT011  The reader has one redacted refusal.
        run(retained(installation, owned_worker))
    readiness = run(publish(installation))
    raw = pworker_service.command("GET", installation.readiness_key)
    assert raw == readiness.canonical_bytes
    assert pworker_service.command("PTTL", installation.readiness_key) == -1
    # The bytes, read back, are exactly the v1 grammar the readers parse.
    stored = parse_readiness(raw)
    assert stored.canonical_bytes == raw
    r = stored.as_dict()
    assert r["verifier_identity"] == installation.manifest.as_dict()["credential_refs"]["verifier"]
    assert int(r["expires_at_ms"]) - int(r["issued_at_ms"]) == 30000
    validate_authority(
        installation.manifest,
        installation.qualification,
        stored,
        broker_identity=installation.manifest.as_dict()["broker_identity"],
        broker_now_ms=broker_now_ms(pworker_service),
        trusted_max_readiness_ms=MAX_READINESS_MS,
    )
    run(retained(installation, owned_worker))


def test_a_second_first_write_or_a_stale_compare_refuses(pworker_service, installation) -> None:
    first = run(publish(installation))
    before = snapshot(pworker_service)
    with refused("stale"):
        run(publish(installation))
    assert snapshot(pworker_service) == before
    # Renewal compares and sets on the exact current bytes.
    second = run(publish(installation, expected=first, measurement_record_id="measurement/r2"))
    assert pworker_service.command("GET", installation.readiness_key) == second.canonical_bytes
    before = snapshot(pworker_service)
    with refused("stale"):
        run(publish(installation, expected=first, measurement_record_id="measurement/r3"))
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize("lifetime", [True, 0, -1, MAX_READINESS_MS + 1, "30000"])
def test_readiness_lifetime_is_bounded_by_the_trusted_maximum(
    pworker_service, installation, lifetime
) -> None:
    with refused("lifetime"):
        run(publish(installation, lifetime_ms=lifetime))
    assert pworker_service.command("GET", installation.readiness_key) is None


def test_failed_reobservation_writes_nothing(pworker_service, installation) -> None:
    async def difference() -> None:
        raise RuntimeError("acl principal state differs")

    before = snapshot(pworker_service)
    with refused("reobservation"):
        run(publish(installation, reobserve=difference))
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize("attack", ["absent", "other-generation", "other-manifest", "garbage"])
def test_readiness_requires_the_stored_qualification(pworker_service, installation, attack) -> None:
    from curie_protected_hooks.authority_records import parse_qualification

    supplied = installation.qualification
    if attack == "absent":
        pworker_service.command("DEL", installation.qualification_key)
    elif attack == "other-generation":
        supplied = parse_qualification(
            canonical(qualification_value(installation.manifest, generation="2"))
        )
    elif attack == "other-manifest":
        value = qualification_value(installation.manifest)
        value["manifest_digest"] = "0" * 64
        pworker_service.command("SET", installation.qualification_key, canonical(value))
        supplied = parse_qualification(canonical(value))
    else:
        pworker_service.command("SET", installation.qualification_key, b"{not json")
    before = snapshot(pworker_service)
    with refused("qualification"):
        run(publish(installation, qualification=supplied))
    assert snapshot(pworker_service) == before


@pytest.mark.parametrize("attack", ["absent", "different"])
def test_readiness_requires_the_stored_manifest(pworker_service, installation, attack) -> None:
    key = "protected:control:manifest:" + installation.manifest.digest
    if attack == "absent":
        pworker_service.command("DEL", key)
    else:
        # Whitespace would parse equal, as the unchanged reader parses it; a member differs.
        value = installation.manifest.as_dict()
        value["worker_image_digest"] = "sha256:" + "0" * 64
        pworker_service.command("SET", key, canonical(value))
    before = snapshot(pworker_service)
    with refused("manifest"):
        run(publish(installation))
    assert snapshot(pworker_service) == before


def test_only_the_manifest_verifier_publishes(pworker_service, owned_worker, installation) -> None:
    # A credential for another principal refuses before any network write.
    with refused("principal"):
        run(publish(installation, credential=owned_worker.credential()))
    # A wrong password for the verifier name never authenticates.
    with refused():
        run(publish(installation, credential=installation.credential(password="0" * 48)))
    assert pworker_service.command("GET", installation.readiness_key) is None
    # The broker refuses other production principals' writes outright.
    raw = owned_worker.raw()
    try:
        with pytest.raises(NoPermissionError):
            raw.execute_command("SET", installation.readiness_key, b"{}")
    finally:
        raw.close()


def test_verifier_recipe_writes_readiness_only(pworker_service, installation) -> None:
    """The unchanged verifier rules: SET on Readiness only, no read of commissioning keys."""
    from redis import Redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    raw = Redis(
        host="127.0.0.1",
        port=pworker_service.port,
        username=installation.verifier,
        password=installation.password,
        ssl=True,
        ssl_ca_certs=str(pworker_service.private / "ca.crt"),
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        socket_timeout=3,
        retry=Retry(NoBackoff(), 0),
    )
    try:
        for command in (
            ("SET", installation.qualification_key, b"{}"),
            ("SET", "protected:control:selection:x", b"{}"),
            ("GET", "protected:commissioning:acceptance:x"),
            ("DEL", installation.readiness_key),
            ("EVAL", "return 1", "0"),
        ):
            with pytest.raises(NoPermissionError):
                raw.execute_command(*command)
    finally:
        raw.close()


def test_a_manifest_bound_to_another_broker_epoch_refuses(pworker_service, installation) -> None:
    other = pworker_service.manifest(run_id="0" * 40)
    with refused():
        run(publish(installation, manifest=other))
    assert pworker_service.command("GET", installation.readiness_key) is None


def test_publisher_is_constructed_only_by_its_factory(installation) -> None:
    with refused():
        publication().ReadinessPublisher()

    async def check() -> None:
        publisher = await installation.publisher()
        try:
            assert installation.verifier not in repr(publisher)
            assert str(installation.password) not in repr(publisher)
        finally:
            await publisher.close()

    run(check())


def test_readiness_digest_matches_its_canonical_bytes(pworker_service, installation) -> None:
    readiness = run(publish(installation))
    assert readiness.digest == hashlib.sha256(readiness.canonical_bytes).hexdigest()


# --- Qualification publisher (design decision 8) ----------------------------------------


def test_written_qualification_is_the_unchanged_v1_grammar(pworker_service, unqualified) -> None:
    from curie_protected_hooks.authority_records import parse_qualification

    qualification = run(unqualified.qualify())
    raw = pworker_service.command("GET", unqualified.qualification_key)
    # An independent oracle of the v1 bytes, bound to the manifest.
    assert raw == canonical(qualification_value(unqualified.manifest))
    assert raw == qualification.canonical_bytes
    assert parse_qualification(raw).canonical_bytes == raw
    assert pworker_service.command("PTTL", unqualified.qualification_key) == -1


def test_qualification_is_written_once(pworker_service, unqualified) -> None:
    run(unqualified.qualify())
    before = snapshot(pworker_service)
    # Identical bytes again are a replay, not a second write.
    run(unqualified.qualify())
    assert snapshot(pworker_service) == before
    with refused("exists"):
        run(unqualified.qualify(measurement_record_id="measurement/other"))
    assert snapshot(pworker_service) == before
    # Another generation is another key, still inside the publisher's own family.
    run(unqualified.qualify(qualification_generation="2"))
    m = unqualified.manifest.as_dict()
    assert pworker_service.command(
        "GET", "protected:control:qualification:" + m["qualification_id"] + ":2"
    )


@pytest.mark.parametrize("generation", ["0", "", "01", 1, None])
def test_qualification_grammar_is_checked_before_writing(
    pworker_service, unqualified, generation
) -> None:
    before = snapshot(pworker_service)
    with refused("qualification"):
        run(unqualified.qualify(qualification_generation=generation))
    assert snapshot(pworker_service) == before


def test_only_the_bootstrap_publisher_writes_qualification(
    pworker_service, owned_worker, unqualified
) -> None:
    from curie_protected_hooks.worker_transport import WorkerCredential

    b = unqualified.tuple.bootstrap.as_dict()
    # Every other credential B names, production and commissioning, is refused:
    # the verified party never publishes its own qualification.
    others = [
        *(ref["id"] for ref in b["production_credentials"].values()),
        *(ref["id"] for ref in b["candidate_credentials"].values()),
        *(
            b[m]["id"]
            for m in (
                "issuer_identity",
                "control_identity",
                "control_reader_identity",
                "source_writer_identity",
            )
        ),
        *(b[m]["id"] for m in ("mediator_identity", "gateway_identity")),
    ]
    for username in others:
        with refused("principal"):
            run(unqualified.qualify(credential=WorkerCredential(username, "x" * 48)))
    with refused("principal"):
        run(unqualified.qualify(credential=unqualified.credential()))
    with refused():
        run(unqualified.qualify(credential=unqualified.publisher_credential("0" * 48)))
    assert pworker_service.command("GET", unqualified.qualification_key) is None
    # The broker refuses the Qualification write to every production principal.
    raw = owned_worker.raw()
    try:
        with pytest.raises(NoPermissionError):
            raw.execute_command("SETNX", unqualified.qualification_key, b"{}")
    finally:
        raw.close()


def test_qualification_bound_to_another_broker_epoch_refuses(pworker_service, unqualified) -> None:
    # A consistent B, G and M whose manifest names another broker epoch.
    other = provisioned(pworker_service, run_id="0" * 40)
    with refused():
        run(
            unqualified.qualify(
                manifest=other.manifest, bootstrap=other.bootstrap, guard_config=other.guard_config
            )
        )
    assert pworker_service.command("GET", unqualified.qualification_key) is None


def test_qualification_requires_the_manifests_own_bootstrap_and_guard(
    pworker_service, unqualified
) -> None:
    """The publisher identity comes only from a B that G and M bind."""
    # Another installation's B (and so G): one bootstrap member differs.
    other = provisioned(pworker_service, changes={"max_permit_ms": "2000"})
    assert other.bootstrap.digest != unqualified.tuple.bootstrap.digest
    attempts = [
        dict(bootstrap=other.bootstrap),
        dict(guard_config=other.guard_config),
        dict(bootstrap=other.bootstrap, guard_config=other.guard_config),
        dict(guard_config=unqualified.tuple.guard_config + b" "),
        dict(bootstrap=unqualified.tuple.bootstrap.as_dict()),
        dict(bootstrap=unqualified.tuple.bootstrap.canonical_bytes),
    ]
    for attempt in attempts:
        with refused("binding"):
            run(unqualified.qualify(**attempt))
    assert pworker_service.command("GET", unqualified.qualification_key) is None


def test_qualification_publisher_is_constructed_only_by_its_factory(unqualified) -> None:
    with refused():
        publication().QualificationPublisher()

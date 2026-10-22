"""Clause 6 activation on the pinned Valkey against the unchanged readers,
@spec PROTECTED-COMMISSIONING-6.

One owned TLS Valkey at the pinned image holds an installation built by the
provisioner core in hash order: every broker user is installed from its
product recipe (G's), M and its closed selection by
``install_control_records``, the permit by the issuer and the journal by
control, and the candidates enabled for the permit's lane by
``enable_candidates``. The campaign itself is not run here: the ADR 0236 case
producers are fakes, so this proves the transition and the readers, never an
accepted campaign. Every write of the transition, its order and its
principals are the product's own.

The oracles are the unchanged production readers: the protected worker's
``open_runtime`` (it reaches its database engine only after the authority it
reads holds), the API's protected publication evaluation and the signed
support probe's evaluation over the production control reader. A full key
and ACL snapshot shows that activation changes exactly clause 6's rows and
nothing else. Fixture IDs and secrets are placeholders.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import secrets
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from redis import Redis
from redis import asyncio as aioredis
from redis.exceptions import NoPermissionError

from . import worker_broker as wb
from .admission_broker import FixtureSecret
from .commissioning_tuple_broker import broker_now_ms, snapshot
from .test_commissioning_records import permit_value
from .test_commissioning_verifier import Producer

pworker_service = wb.pworker_service

GENERATION = "1"
MAX_READINESS_MS = 60000
PROVISIONER = "commissioning/example-provisioner"
AGENT = "55555555-5555-4555-8555-555555555555"
HOOK = "alerts"
FINGERPRINT = "ab" * 32


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


class EngineReached(Exception):
    """The worker passed every authority read and reached its database engine."""


def acl_state(broker: Any) -> dict[str, Any]:
    """Every ACL user's full ``ACL GETUSER`` reply: flags, password hashes and rules."""
    users = sorted(
        u.decode() if isinstance(u, bytes) else u for u in broker.command("ACL", "USERS")
    )
    return {user: broker.command("ACL", "GETUSER", user) for user in users}


def rules_only(reply: Any) -> Any:
    """A GETUSER reply without ``flags``: the rules, keys, channels and selectors."""
    if isinstance(reply, dict):
        return {k: v for k, v in reply.items() if k not in ("flags", b"flags")}
    pairs = list(zip(reply[::2], reply[1::2], strict=True))
    return [(k, v) for k, v in pairs if k not in ("flags", b"flags")]


def flags(reply: Any) -> set[str]:
    value = reply.get("flags", reply.get(b"flags")) if isinstance(reply, dict) else None
    if value is None:
        pairs = dict(zip(reply[::2], reply[1::2], strict=True))
        value = pairs.get("flags", pairs.get(b"flags"))
    return {f.decode() if isinstance(f, bytes) else f for f in value}


class ObserverReader:
    """``AclUserReader`` over a session authenticated as the ACL observer user."""

    def __init__(self, client: aioredis.Redis) -> None:
        self.client = client

    async def getuser(self, username: str) -> object:
        return await self.client.execute_command("ACL", "GETUSER", username)  # type: ignore[no-untyped-call]

    async def key_read_denied(self, key: str) -> bool:
        try:
            await self.client.get(key)
        except NoPermissionError:
            return True
        return False


class CandidateRules:
    """``CandidateRuleReader``: the observer's reading of each candidate user's rules.

    No production normalizer of ``ACL GETUSER`` into recipe tokens exists yet,
    so this test adapter reports the template instantiated for this permit's
    lane exactly when the user's live rules are byte equal to what the broker
    reported right after ``enable_candidates`` set them, and nothing otherwise.
    """

    def __init__(self, reader: ObserverReader, expected: dict[str, tuple[Any, tuple[str, ...]]]):
        self.reader, self.expected = reader, expected

    async def rules(self, username: str) -> tuple[str, ...]:
        recorded, instantiated = self.expected[username]
        live = rules_only(await self.reader.getuser(username))
        return instantiated if live == recorded else ()


class Fact:
    """One ``TUPLE_FACTS`` producer reading the live installation each call."""

    def __init__(self, fact: str, observe: Any) -> None:
        self._fact, self._observe = fact, observe

    @property
    def fact(self) -> str:
        return self._fact

    async def observe(self) -> str:
        return str(self._observe())


class Installation:
    """Every principal of the principals table on the owned broker, before the campaign."""

    def __init__(self, broker: Any, root: Path) -> None:
        from curie_protected_hooks.commissioning_acl import (
            CANDIDATE_ROLES,
            GuardRecipes,
            commissioning_acl_rules,
            commissioning_acl_template,
        )
        from curie_protected_hooks.commissioning_local_broker import (
            CANDIDATE_IDLE,
            BrokerUser,
            install_control_records,
        )
        from curie_protected_hooks.commissioning_provisioner import construct

        from .commissioning_worker_vectors import worker_profile
        from .test_commissioning_provisioner import profile_fields

        self.broker, self.root = broker, Path(os.path.realpath(root))
        # The worker-grade profile: its execution configuration and projection
        # are the grammars the production worker parses.
        self.profile = worker_profile(
            broker_identity=broker.manifest().as_dict()["broker_identity"],
            max_permit_ms="600000",
        )
        self.tuple = construct(self.profile, acl=GuardRecipes(profile_fields()["qualification_id"]))
        self.manifest, self.bootstrap = self.tuple.manifest, self.tuple.bootstrap
        self.guard_config, self.measurement = self.tuple.guard_config, self.tuple.measurement
        m, b = self.manifest.as_dict(), self.bootstrap.as_dict()
        guard = json.loads(self.guard_config)
        self.usernames = {
            **{name: ref["id"] for name, ref in m["credential_refs"].items()},
            "issuer": b["issuer_identity"]["id"],
            "control": b["control_identity"]["id"],
            "candidate_enqueue": b["candidate_credentials"]["enqueue"]["id"],
            "candidate_worker": b["candidate_credentials"]["worker"]["id"],
            "qualification_publisher": b["qualification_publisher_identity"]["id"],
            "control_reader": b["control_reader_identity"]["id"],
            "source_writer": b["source_writer_identity"]["id"],
            "observer": guard["broker_acl"]["observer"]["credential"]["id"],
            "provisioner": PROVISIONER,
        }
        self.passwords = {name: FixtureSecret(secrets.token_hex(24)) for name in self.usernames}
        recipes = GuardRecipes(m["qualification_id"])
        for name, username in self.usernames.items():
            if name in CANDIDATE_ROLES:
                state, rules = "off", CANDIDATE_IDLE
            elif name == "provisioner":
                state, rules = "on", commissioning_acl_rules("provisioner")
            else:
                state, rules = "on", recipes.rules(name)
            broker.command(
                "ACL", "SETUSER", username, "reset", state, ">" + self.passwords[name], *rules
            )
        self.candidates = {
            name: BrokerUser(
                self.usernames[name], commissioning_acl_template(name), self.passwords[name]
            )
            for name in CANDIDATE_ROLES
        }
        self.provisioner = self.sync_session("provisioner")
        install_control_records(
            self.provisioner, self.manifest, qualification_generation=GENERATION
        )
        self.files = self._write_files()

    def sync_session(self, name: str) -> Redis:
        return Redis(
            host="127.0.0.1",
            port=self.broker.port,
            username=self.usernames[name],
            password=self.passwords[name],
            ssl=True,
            ssl_ca_data=str(self.broker.ca_pem),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            socket_timeout=5,
            single_connection_client=True,
        )

    def async_session(self, name: str) -> aioredis.Redis:
        return aioredis.Redis(
            host="127.0.0.1",
            port=self.broker.port,
            username=self.usernames[name],
            password=self.passwords[name],
            ssl=True,
            ssl_ca_data=str(self.broker.ca_pem),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            socket_timeout=5,
            single_connection_client=True,
        )

    def _write_files(self) -> dict[str, Path]:
        """The provisioner's records and the production worker's runtime directory."""
        from curie_protected_hooks.commissioning_local_bootstrap import LaunchPublic
        from curie_protected_hooks.commissioning_local_broker import BrokerUser
        from curie_protected_hooks.commissioning_local_worker import worker_files
        from curie_protected_hooks.commissioning_local_workloads import ServiceSecrets

        worker = self.root / "worker"
        files = worker_files(
            manifest=self.manifest,
            execution_config=self.profile.execution_config,
            projection=self.profile.projection,
            measurement_file=self.tuple.measurement_file,
            ca_pem=str(self.broker.ca_pem),
            worker=BrokerUser(self.usernames["worker"], (), self.passwords["worker"]),
            launch=LaunchPublic(
                server_name="mediator.commissioning-example.svc.cluster.local",
                ca_pem=str(self.broker.ca_pem),
                server_spki_sha256="a" * 64,
                client_spki_sha256="b" * 64,
            ),
            namespace="commissioning-example",
            secrets=ServiceSecrets.fresh(),
        )
        for relative, raw in files.items():
            path = worker / relative
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_bytes(raw)
            path.chmod(0o600)
        provisioner = self.root / "provisioner"
        provisioner.mkdir(mode=0o700)
        (provisioner / "bootstrap.json").write_bytes(self.bootstrap.canonical_bytes)
        (provisioner / "guard_config.json").write_bytes(self.guard_config)
        return {"worker": worker, "provisioner": provisioner}

    def file_digests(self) -> dict[str, str]:
        """Every file of both directories: nothing is written after the campaign."""
        return {
            str(path.relative_to(self.root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(self.root.rglob("*"))
            if path.is_file()
        }

    def close(self) -> None:
        self.provisioner.close()
        for username in self.usernames.values():
            self.broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
            self.broker.command("ACL", "DELUSER", username)


class Campaign:
    """A permit issued, candidates enabled, the journal run to ``closing``."""

    def __init__(self, installation: Installation) -> None:
        from curie_protected_hooks.commissioning_records import parse_commissioning_permit

        self.i = i = installation
        m, b = i.manifest.as_dict(), i.bootstrap.as_dict()
        now = broker_now_ms(i.broker)
        p = permit_value()
        for member in (
            "runtime_id",
            "runtime_generation",
            "execution_config_digest",
            "broker_identity",
            "guard_identity",
        ):
            p[member] = copy.deepcopy(m[member])
        p.update(
            manifest_digest=i.manifest.digest,
            bootstrap_digest=i.bootstrap.digest,
            issuer_identity=copy.deepcopy(b["issuer_identity"]),
            control_identity=copy.deepcopy(b["control_identity"]),
            candidate_credentials=copy.deepcopy(b["candidate_credentials"]),
            issued_at_ms=str(now - 1000),
            expires_at_ms=str(now + 590000),
        )
        self.permit = parse_commissioning_permit(canonical(p))
        self.trusted = {
            "manifest": i.manifest,
            "bootstrap": i.bootstrap,
            "trusted_issuer": copy.deepcopy(b["issuer_identity"]),
            "trusted_control": copy.deepcopy(b["control_identity"]),
            "max_permit_ms": int(b["max_permit_ms"]),
        }

    async def session(self, name: str) -> Any:
        from curie_protected_hooks.commissioning_journal import CommissioningSession
        from curie_protected_hooks.worker_transport import WorkerCredential

        b = self.i.bootstrap.as_dict()
        references = {
            "issuer": b["issuer_identity"],
            "control": b["control_identity"],
            "candidate_enqueue": b["candidate_credentials"]["enqueue"],
        }
        return await CommissioningSession.connect(
            self.i.manifest,
            WorkerCredential(self.i.usernames[name], self.i.passwords[name]),
            str(self.i.broker.ca_pem),
            principal=copy.deepcopy(references[name]),
        )

    async def start(self) -> None:
        """Issue, enable the candidates, then run the journal to ``closing``."""
        from curie_protected_hooks.commissioning_journal import CommissioningJournal, issue_permit
        from curie_protected_hooks.commissioning_local_broker import enable_candidates

        self.issuer = await self.session("issuer")
        await issue_permit(self.issuer, self.permit, **self.trusted)
        enable_candidates(self.i.provisioner, self.i.candidates, self.permit)
        # A live candidate session, which step 2 must end.
        self.candidate = await self.session("candidate_enqueue")
        control = await self.session("control")
        self.journal = await CommissioningJournal.open(control, self.permit, **self.trusted)
        state = await self.journal.prepare()
        state = await self.journal.start(state.generation)
        state = await self.journal.begin_close(state.generation)
        assert state.state == "closing"

    async def close(self) -> None:
        for name in ("issuer", "candidate", "journal"):
            handle = getattr(self, name, None)
            if handle is not None:
                try:
                    await handle.close()
                except Exception:  # noqa: BLE001  Already ended by the transition.
                    pass


class Harness:
    """The ADR 0236 verifier and the design decision 7 observer for one campaign."""

    def __init__(self, campaign: Campaign) -> None:
        from curie_protected_hooks.commissioning_acl import CANDIDATE_ROLES, commissioning_acl_rules
        from curie_protected_hooks.commissioning_activation import ACTIVATED
        from curie_protected_hooks.commissioning_producers import (
            ACL_PRINCIPALS,
            AclBaseline,
            AclPrincipalStateProducer,
            parse_getuser_reply,
        )
        from curie_protected_hooks.commissioning_verifier import (
            REQUIRED_CASES,
            CommissioningVerifier,
            PublicationInputs,
        )
        from curie_protected_hooks.worker_transport import WorkerCredential

        i = campaign.i
        self.campaign = campaign
        # The administrator's installation baseline, recorded before the campaign.
        self.baseline = AclBaseline(
            i.bootstrap,
            {
                name: parse_getuser_reply(i.broker.command("ACL", "GETUSER", i.usernames[name]))
                for name in ACL_PRINCIPALS
            },
        )
        self.observer = i.async_session("observer")
        reader = ObserverReader(self.observer)
        self.acl = AclPrincipalStateProducer(
            reader=reader,
            baseline=self.baseline,
            observer_username=i.usernames["observer"],
            provisioner_username=PROVISIONER,
        )
        self.reader = reader
        self.candidate_rules: CandidateRules | None = None
        self.roles = CANDIDATE_ROLES
        self.instantiated = {
            role: commissioning_acl_rules(role, permit=campaign.permit) for role in CANDIDATE_ROLES
        }
        execution = i.manifest.as_dict()["execution_config_digest"]
        worker, provisioner = i.files["worker"], i.files["provisioner"]
        stored = i.sync_session("provisioner")
        self._stored = stored

        def digest(path: Path) -> str:
            return hashlib.sha256(path.read_bytes()).hexdigest()

        def permit() -> str:
            from curie_protected_hooks.commissioning_journal import campaign_keys
            from curie_protected_hooks.commissioning_records import parse_commissioning_permit

            raw = i.broker.command("GET", campaign_keys(campaign.permit).permit)
            return parse_commissioning_permit(raw).digest

        def manifest() -> str:
            from curie_protected_hooks.authority_records import parse_manifest

            return parse_manifest(
                stored.get("protected:control:manifest:" + i.manifest.digest)
            ).digest

        def bootstrap() -> str:
            from curie_protected_hooks.commissioning_bootstrap import (
                parse_commissioning_bootstrap,
            )

            return parse_commissioning_bootstrap(
                (provisioner / "bootstrap.json").read_bytes()
            ).digest

        def run_id() -> str:
            return hashlib.sha256(stored.info("server")["run_id"].encode("ascii")).hexdigest()

        name, _ = i.tuple.measurement_file
        facts = (
            Fact("permit", permit),
            Fact("manifest", manifest),
            Fact("bootstrap", bootstrap),
            Fact("guard", lambda: digest(provisioner / "guard_config.json")),
            Fact("execution_config", lambda: digest(worker / "execution_config.json")),
            Fact("broker_run_id", run_id),
            Fact("artifact", lambda: digest(worker / "measurements" / name)),
        )
        b, m = i.bootstrap.as_dict(), i.manifest.as_dict()
        self.verifier_args = dict(
            permit=campaign.permit,
            bootstrap=i.bootstrap,
            guard_config=i.guard_config,
            manifest=i.manifest,
            measurement=i.measurement,
            # Fakes: the campaign's cases are not run in this test.
            producers={case: Producer(case, execution) for case in REQUIRED_CASES},
            artifact_evidence=None,
            tuple_producers=facts,
            publication=PublicationInputs(
                qualification_credential=WorkerCredential(
                    b["qualification_publisher_identity"]["id"],
                    i.passwords["qualification_publisher"],
                ),
                readiness_credential=WorkerCredential(
                    m["credential_refs"]["verifier"]["id"], i.passwords["verifier"]
                ),
                ca_pem=str(i.broker.ca_pem),
                qualification_generation=GENERATION,
                readiness_lifetime_ms=30000,
                max_readiness_ms=MAX_READINESS_MS,
                reobserve=lambda: self.acl.require(ACTIVATED),
            ),
        )
        self._verifier_class = CommissioningVerifier

    def record_candidate_rules(self) -> None:
        """What the broker reports for each candidate once ``enable_candidates`` ran."""
        i = self.campaign.i
        self.candidate_rules = CandidateRules(
            self.reader,
            {
                i.usernames[role]: (
                    rules_only(i.broker.command("ACL", "GETUSER", i.usernames[role])),
                    self.instantiated[role],
                )
                for role in self.roles
            },
        )

    def verifier(self) -> Any:
        assert self.candidate_rules is not None
        return self._verifier_class(**self.verifier_args, candidate_rules=self.candidate_rules)

    def activation(self, verifier: Any, **changes: Any) -> Any:
        from curie_protected_hooks.commissioning_activation import CommissioningActivation

        c = self.campaign

        async def settled() -> None:
            # No campaign workload or target exists in this test; nothing to confirm absent.
            return None

        values: dict[str, Any] = dict(
            permit=c.permit,
            manifest=c.i.manifest,
            bootstrap=c.i.bootstrap,
            trusted_issuer=c.trusted["trusted_issuer"],
            trusted_control=c.trusted["trusted_control"],
            max_permit_ms=c.trusted["max_permit_ms"],
            trusted_max_readiness_ms=MAX_READINESS_MS,
            qualification_generation=GENERATION,
            journal=c.journal,
            settled=settled,
            provisioner=c.i.provisioner,
            trusted_provisioner=PROVISIONER,
            acl=self.acl,
            issuer=c.issuer,
            verifier=verifier,
        )
        values.update(changes)
        return CommissioningActivation(**values)

    async def close(self) -> None:
        self._stored.close()
        await self.observer.aclose()


class Readers:
    """The unchanged production readers of this installation."""

    def __init__(self, installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
        from curie_protected_hooks.broker_transport import (
            MetadataReaderCredential,
            SourceWriterCredential,
        )
        from curie_protected_hooks.runtime_directory import RuntimeBootstrap
        from curie_worker import protected_entry

        def no_engine(*_args: Any, **_kwargs: Any) -> Any:
            raise EngineReached()

        monkeypatch.setattr(protected_entry, "create_async_engine", no_engine)
        # A pass-through record of the worker's own control reads, so a refusal
        # is attributed to the authority it read, not to a file defect.
        from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient

        self.reads: list[tuple[str, bool]] = []
        original = AuthenticatedWorkerClient.read_control
        reads = self.reads

        async def read_control(client: Any, key: str) -> Any:
            value = await original(client, key)
            reads.append((key, value is not None))
            return value

        monkeypatch.setattr(AuthenticatedWorkerClient, "read_control", read_control)
        self.i = installation
        self.entry = protected_entry
        self.bootstrap = RuntimeBootstrap(
            manifest=installation.manifest,
            ca_pem=str(installation.broker.ca_pem),
            credential=MetadataReaderCredential(
                installation.usernames["control_reader"], installation.passwords["control_reader"]
            ),
            max_readiness_ms=MAX_READINESS_MS,
        )
        self.writer_credential = SourceWriterCredential(
            installation.usernames["source_writer"], installation.passwords["source_writer"]
        )
        self.operation = str(uuid.uuid4())
        self.generation: int | None = None

    async def worker(self) -> str:
        """``open_runtime``: ``reached-engine`` only once every authority read holds.

        A refusal reports ``refused:<key>`` for the worker's last control read
        when that read found nothing.
        """
        self.reads.clear()

        async def once() -> None:
            async with self.entry.open_runtime(str(self.i.files["worker"])):
                raise AssertionError("a runtime opened past its engine")

        try:
            await once()
        except EngineReached:
            return "reached-engine"
        except Exception:  # noqa: BLE001  The worker's refusal is one redacted error.
            if self.reads and not self.reads[-1][1]:
                return "refused:" + self.reads[-1][0]
            return "refused"
        raise AssertionError("open_runtime returned")

    def reserve(self) -> None:
        """The production source writer reserves a protected publication (SOURCE-6)."""
        from curie_protected_hooks.broker_transport import AuthenticatedSourceWriter

        writer = AuthenticatedSourceWriter.connect(
            self.i.manifest, self.writer_credential, str(self.i.broker.ca_pem)
        )
        try:
            self.generation = writer.reserve_and_revoke(AGENT, HOOK, 0, self.operation, 1)
        finally:
            writer.close()

    def publish(self) -> bool:
        """SOURCE-6's separate administrative step, after activation."""
        from curie_protected_hooks.broker_transport import AuthenticatedSourceWriter

        assert self.generation is not None
        writer = AuthenticatedSourceWriter.connect(
            self.i.manifest, self.writer_credential, str(self.i.broker.ca_pem)
        )
        try:
            return writer.publish_protected(
                AGENT, HOOK, self.generation, self.operation, FINGERPRINT
            )
        finally:
            writer.close()

    def _target(self) -> Any:
        from curie_protected_hooks.authority_evaluation import AuthorityTarget

        m = self.i.manifest.as_dict()
        assert self.generation is not None
        return AuthorityTarget(
            generation=self.generation,
            operation_id=self.operation,
            policy_fingerprint=FINGERPRINT,
            runtime_id=m["runtime_id"],
            qualification_id=m["qualification_id"],
            bundle_digest=m["bundle_digest"]["sha256"],
        )

    def publication(self) -> str:
        """The API's protected publication evaluation, as the production control reader."""
        from curie_api import hook_source_broker
        from curie_protected_hooks.broker_transport import AuthenticatedMetadataReader

        reader = AuthenticatedMetadataReader.connect(
            self.i.manifest, self.bootstrap.credential, self.bootstrap.ca_pem
        )
        try:
            state = reader.read_source(AGENT, HOOK)
            return hook_source_broker._evidence(reader, self.bootstrap, state, self._target())
        finally:
            reader.close()

    def probe(self) -> str:
        """The signed support probe's broker evaluation of the committed row."""
        from curie_api import protected_support

        target = self._target()
        policy = SimpleNamespace(
            generation=target.generation,
            operation_id=target.operation_id,
            runtime_id=target.runtime_id,
            qualification_id=target.qualification_id,
            bundle_digest=target.bundle_digest,
        )
        reads = protected_support._read_broker(self.bootstrap, AGENT, HOOK)
        decided = protected_support._decide(policy, FINGERPRINT, self.bootstrap, reads)  # type: ignore[arg-type]
        return str(decided.reason)


@pytest.fixture
def installed(pworker_service, tmp_path):
    pworker_service.command("FLUSHDB")
    installation = Installation(pworker_service, tmp_path)
    try:
        yield installation
    finally:
        installation.close()
        pworker_service.command("FLUSHDB")


def selection_key(installation: Installation) -> str:
    return "protected:control:selection:" + installation.manifest.as_dict()["runtime_id"]


def test_activation_changes_exactly_clause_6s_rows_and_the_unchanged_readers_accept(
    pworker_service, installed, monkeypatch
) -> None:
    from curie_protected_hooks.commissioning_acceptance_writer import acceptance_key
    from curie_protected_hooks.commissioning_journal import campaign_keys

    readers = Readers(installed, monkeypatch)
    readers.reserve()
    campaign = Campaign(installed)

    async def scenario() -> dict[str, Any]:
        await campaign.start()
        harness = Harness(campaign)
        harness.record_candidate_rules()
        try:
            observed: dict[str, Any] = {}
            # Before: production credentials are enabled and assigned, and
            # every production reader refuses.
            observed["before"] = {
                "worker": await readers.worker(),
                "publication": readers.publication(),
                "probe": readers.probe(),
            }
            observed["keys"] = snapshot(pworker_service)
            observed["acl"] = acl_state(pworker_service)
            observed["files"] = installed.file_digests()
            assert await campaign.candidate.execute("PING") in ("PONG", True)
            observed["report"] = await harness.activation(harness.verifier()).activate()
            observed["keys_after"] = snapshot(pworker_service)
            observed["acl_after"] = acl_state(pworker_service)
            observed["files_after"] = installed.file_digests()
            with pytest.raises(Exception):  # noqa: B017, PT011  The killed session is gone.
                await campaign.candidate.execute("PING")
            observed["after"] = {
                "worker": await readers.worker(),
                "publication": readers.publication(),
            }
            assert readers.publish() is True
            observed["after"]["probe"] = readers.probe()
            return observed
        finally:
            await harness.close()
            await campaign.close()

    observed = run(scenario())
    m = installed.manifest.as_dict()
    readiness = "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
    assert observed["before"] == {
        # The worker refuses at its first authority read: no Readiness exists.
        "worker": "refused:" + readiness,
        "publication": "qualification_unavailable",
        "probe": "source_closed",
    }
    assert observed["after"] == {
        "worker": "reached-engine",
        "publication": "accept",
        "probe": "supported",
    }

    # Exactly clause 6's broker rows changed; every other key, value and TTL is equal.
    keys = campaign_keys(campaign.permit)
    qualification = "protected:control:qualification:" + m["qualification_id"] + ":" + GENERATION
    acceptance = acceptance_key(campaign.permit)
    before, after = observed["keys"], observed["keys_after"]
    assert set(after) - set(before) == {acceptance, qualification, readiness}
    assert set(before) - set(after) == set()
    changed = {key for key in before if before[key] != after[key]}
    assert changed == {keys.journal, selection_key(installed)}
    assert json.loads(before[keys.journal][1])["state"] == "closing"
    assert json.loads(after[keys.journal][1])["state"] == "closed"
    for key in (acceptance, qualification, readiness):
        assert after[key][2] == -1  # no TTL
    report = observed["report"]
    assert before[selection_key(installed)][1] == report.selection_before
    assert after[selection_key(installed)][1] == report.selection_after
    assert json.loads(report.selection_after) == dict(
        json.loads(report.selection_before), admission_open=True
    )
    assert after[acceptance][1] == report.acceptance.canonical_bytes
    assert after[qualification][1] == report.publication.qualification.canonical_bytes
    assert after[readiness][1] == report.publication.readiness.canonical_bytes

    # Only the candidates' enabled state changed; every rule and password is equal.
    acl_before, acl_after = observed["acl"], observed["acl_after"]
    assert set(acl_before) == set(acl_after)
    candidates = {installed.usernames[r] for r in ("candidate_enqueue", "candidate_worker")}
    for user in acl_before:
        assert rules_only(acl_after[user]) == rules_only(acl_before[user]), user
        if user in candidates:
            assert flags(acl_before[user]) - {"on", "off"} == flags(acl_after[user]) - {"on", "off"}
            assert "on" in flags(acl_before[user]) and "off" in flags(acl_after[user])
        else:
            assert acl_after[user] == acl_before[user], user

    # No measurement, bootstrap, guard or runtime file was written after the campaign.
    assert observed["files_after"] == observed["files"]


def test_admission_never_opens_without_the_stored_qualification_and_readiness(
    pworker_service, installed
) -> None:
    from curie_protected_hooks.commissioning_activation import ActivationRefused, open_admission

    campaign = Campaign(installed)

    async def scenario() -> None:
        await campaign.start()
        state = await campaign.journal.status()
        await campaign.journal.confirm_closed(state.generation)
        await campaign.close()

    run(scenario())
    key = selection_key(installed)
    before = snapshot(pworker_service)
    with pytest.raises(ActivationRefused) as caught:
        open_admission(
            installed.provisioner,
            manifest=installed.manifest,
            permit=campaign.permit,
            qualification_generation=GENERATION,
            trusted_max_readiness_ms=MAX_READINESS_MS,
            provisioner_username=PROVISIONER,
        )
    assert caught.value.step == "admission-open"
    assert snapshot(pworker_service) == before
    assert json.loads(pworker_service.command("GET", key))["admission_open"] is False


def test_the_selection_compare_and_set_refuses_any_other_member_change(
    pworker_service, installed
) -> None:
    from curie_protected_hooks.commissioning_activation import (
        _SELECTION_CAS,
        opened_selection,
    )

    key = selection_key(installed)
    closed = pworker_service.command("GET", key)
    opened = opened_selection(closed)
    changed = canonical(dict(json.loads(closed), qualification_generation="2"))
    pworker_service.command("SET", key, changed)
    # The provisioner's own rules run the script; a stale expected value never writes.
    reply = installed.provisioner.execute_command("EVAL", _SELECTION_CAS, 1, key, closed, opened)
    assert reply in (b"refused", "refused")
    assert pworker_service.command("GET", key) == changed
    pworker_service.command("SET", key, closed)
    reply = installed.provisioner.execute_command("EVAL", _SELECTION_CAS, 1, key, closed, opened)
    assert reply in (b"set", "set")
    assert pworker_service.command("GET", key) == opened


def test_disabling_candidates_keeps_their_rules_and_ends_their_sessions(
    pworker_service, installed
) -> None:
    from curie_protected_hooks.commissioning_activation import disable_candidates

    campaign = Campaign(installed)

    async def scenario() -> None:
        await campaign.start()
        users = [installed.usernames[r] for r in ("candidate_enqueue", "candidate_worker")]
        before = {u: pworker_service.command("ACL", "GETUSER", u) for u in users}
        disable_candidates(
            installed.provisioner,
            installed.bootstrap,
            manifest=installed.manifest,
            provisioner_username=PROVISIONER,
        )
        # Repeating the step is harmless.
        disable_candidates(
            installed.provisioner,
            installed.bootstrap,
            manifest=installed.manifest,
            provisioner_username=PROVISIONER,
        )
        for user in users:
            after = pworker_service.command("ACL", "GETUSER", user)
            assert rules_only(after) == rules_only(before[user])
            assert "off" in flags(after)
        with pytest.raises(Exception):  # noqa: B017, PT011
            await campaign.candidate.execute("PING")
        with pytest.raises(Exception):  # noqa: B017, PT011  A disabled user cannot authenticate.
            await campaign.session("candidate_enqueue")
        await campaign.close()

    run(scenario())


def test_the_provisioner_holds_no_right_to_qualification_readiness_or_acceptance(
    installed,
) -> None:
    m = installed.manifest.as_dict()
    for key in (
        "protected:control:qualification:" + m["qualification_id"] + ":" + GENERATION,
        "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"],
        "protected:commissioning:acceptance:example:1",
    ):
        with pytest.raises(NoPermissionError):
            installed.provisioner.set(key, b"{}")


async def publish_records(installed: Installation, *, lifetime_ms: int = 30000) -> None:
    """Qualification and Readiness written by their own publishers, outside activation."""
    from curie_protected_hooks.qualification_publication import (
        QualificationPublisher,
        ReadinessPublisher,
    )
    from curie_protected_hooks.worker_transport import WorkerCredential

    b, m = installed.bootstrap.as_dict(), installed.manifest.as_dict()
    q = await QualificationPublisher.connect(
        installed.manifest,
        WorkerCredential(
            b["qualification_publisher_identity"]["id"],
            installed.passwords["qualification_publisher"],
        ),
        str(installed.broker.ca_pem),
        bootstrap=installed.bootstrap,
        guard_config=installed.guard_config,
    )
    r = await ReadinessPublisher.connect(
        installed.manifest,
        WorkerCredential(m["credential_refs"]["verifier"]["id"], installed.passwords["verifier"]),
        str(installed.broker.ca_pem),
        trusted_max_readiness_ms=MAX_READINESS_MS,
    )
    try:
        record_id = installed.tuple.measurement_record_id
        qualification = await q.publish(
            qualification_generation=GENERATION, measurement_record_id=record_id
        )

        async def unchanged() -> None:
            return None

        await r.publish(
            qualification,
            lifetime_ms=lifetime_ms,
            measurement_record_id=record_id,
            reobserve=unchanged,
            expected=None,
        )
    finally:
        await q.close()
        await r.close()


@pytest.mark.parametrize("defect", ["journal-closing", "readiness-expired"])
def test_admission_never_opens_unless_closed_and_currently_qualified(
    pworker_service, installed, defect
) -> None:
    from curie_protected_hooks.commissioning_activation import ActivationRefused, open_admission

    campaign = Campaign(installed)

    async def scenario() -> None:
        await campaign.start()
        try:
            if defect == "readiness-expired":
                state = await campaign.journal.status()
                await campaign.journal.confirm_closed(state.generation)
                await publish_records(installed, lifetime_ms=1)
                await asyncio.sleep(0.05)
            else:
                # Qualification and Readiness exist, but the campaign never closed.
                await publish_records(installed)
        finally:
            await campaign.close()

    run(scenario())
    state = json.loads(pworker_service.command("GET", campaign_journal(campaign)))["state"]
    assert state == ("closing" if defect == "journal-closing" else "closed")
    before = snapshot(pworker_service)
    with pytest.raises(ActivationRefused):
        open_admission(
            installed.provisioner,
            manifest=installed.manifest,
            permit=campaign.permit,
            qualification_generation=GENERATION,
            trusted_max_readiness_ms=MAX_READINESS_MS,
            provisioner_username=PROVISIONER,
        )
    assert snapshot(pworker_service) == before


def campaign_journal(campaign: Campaign) -> str:
    from curie_protected_hooks.commissioning_journal import campaign_keys

    return campaign_keys(campaign.permit).journal


def test_steps_2_and_5_refuse_a_session_that_is_not_the_trusted_provisioner(
    pworker_service, installed
) -> None:
    """``run_id`` alone is not enough: the session must authenticate as the provisioner."""
    from curie_protected_hooks.commissioning_activation import (
        ActivationRefused,
        disable_candidates,
        open_admission,
    )

    campaign = Campaign(installed)
    run(campaign.start())
    run(campaign.close())
    before, acl = snapshot(pworker_service), acl_state(pworker_service)
    # The fixture administrator is pinned to the same broker epoch, holds every
    # right, and is not the provisioner the installation trusts.
    other = pworker_service.admin
    with pytest.raises(ActivationRefused) as disabled:
        disable_candidates(
            other,
            installed.bootstrap,
            manifest=installed.manifest,
            provisioner_username=PROVISIONER,
        )
    with pytest.raises(ActivationRefused) as opened:
        open_admission(
            other,
            manifest=installed.manifest,
            permit=campaign.permit,
            qualification_generation=GENERATION,
            trusted_max_readiness_ms=MAX_READINESS_MS,
            provisioner_username=PROVISIONER,
        )
    assert (disabled.value.step, opened.value.step) == ("candidates-disabled", "admission-open")
    assert snapshot(pworker_service) == before
    assert acl_state(pworker_service) == acl

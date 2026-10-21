"""The independent commissioning verifier, ADR 0236.

@spec PROTECTED-COMMISSIONING-5 PROTECTED-COMMISSIONING-6 PROTECTED-COMMISSIONING-9.
It runs in the provisioner process and decides one campaign of one
installation. It accepts only when:

* every case of ``REQUIRED_CASES`` has exactly one passing observation, made
  for a fresh challenge and bound to this installation's tuple, by this
  installation's own producers or a signed channel reply;
* the final tuple (permit, manifest, bootstrap, guard, execution
  configuration, broker ``run_id``, runner image and artifact measurement)
  equals the premeasured one;
* revocation and the original workload's termination and absence were
  observed, which are the last two cases.

Any missing, unavailable, skipped, timed-out, failed, foreign, stale or
fake-model observation refuses. Only the pinned SDK primitive cases may be
satisfied by a reused artifact observation, and only when it is content
addressed and bound to the unchanged artifact tuple (clause 9). Installation
observations are never reused.

Python objects cannot be sealed in-process, so the boundary is credential
custody and object identity: the verifier alone holds the Qualification
publisher and Readiness credentials and constructs the publishers only in
``_publish``, for the exact accepted ``Verdict`` object it returned, after
re-measuring the tuple. A ``Verdict`` is a report; constructing or copying one
grants nothing. Clause 6 orders the issuer's acceptance record between
``decide`` and ``publish``: ``publish`` takes that record and requires it to
name this campaign, this verdict's evidence and the manifest's Qualification.
The activation transition owns the order of the writes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, NoReturn, Protocol, Self
from uuid import uuid4

from .artifact_measurement import ArtifactMeasurement
from .authority_records import (
    AuthorityRecordInvalid,
    Manifest,
    Qualification,
    Readiness,
    _pairs,
    _reject_number,
    _scalar,
)
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_campaign_records import CommissioningAcceptance
from .commissioning_provisioner import ProvisionerRefused, verify_tuple
from .commissioning_records import CommissioningPermit
from .qualification_publication import QualificationPublisher, ReadinessPublisher
from .worker_transport import WorkerCredential

__all__ = [
    "REAL_MODEL_CASES",
    "REQUIRED_CASES",
    "REUSABLE_CASES",
    "ArtifactBinding",
    "CaseObservation",
    "CaseProducer",
    "CommissioningVerifier",
    "InstallationTuple",
    "Publication",
    "PublicationInputs",
    "ReusedArtifactObservation",
    "Verdict",
    "VerifierRefused",
]

# Clause 5 and ADR 0236, in observation order. Revocation and the original
# workload's termination and absence come last, after every other case.
REQUIRED_CASES: tuple[str, ...] = (
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
)
# Only the delivered reply proves the model path; every other case makes no call.
REAL_MODEL_CASES = frozenset({"positive-delivery"})
# Clause 9: pinned SDK primitive observations for the runner image and runtime
# profile are artifact observations. Every other case observes the installation.
REUSABLE_CASES = frozenset({"static-skill-loading", "clean-session", "read-only-tool-restriction"})
_PROVENANCES = frozenset({"installation-producer", "signed-reply"})
_REUSED = "reused-artifact"
_ARTIFACT_RECORD = "commissioning_artifact_observation"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_ENTRY = re.compile(r"[a-z0-9][a-z0-9._/-]{0,127}")
_MAX_REUSED_BYTES = 16384
_MAX_ENTRIES = 256


class VerifierRefused(RuntimeError):
    """Redacted refusal of a verifier input or misuse; never a campaign verdict."""

    def __init__(self) -> None:
        super().__init__("Commissioning verification refused")


def _refuse() -> NoReturn:
    raise VerifierRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


def _encode(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _hex(value: object) -> bool:
    return type(value) is str and _DIGEST.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class InstallationTuple:
    """The exact tuple a campaign is bound to; installation A's never equals B's."""

    campaign_id: str
    campaign_generation: str
    permit: str
    manifest: str
    bootstrap: str
    guard: str
    execution_config: str
    broker_run_id: str
    runner_image: str
    artifact: str

    @classmethod
    def measure(
        cls,
        *,
        permit: CommissioningPermit,
        bootstrap: CommissioningBootstrap,
        guard_config: bytes,
        manifest: Manifest,
        measurement: ArtifactMeasurement,
    ) -> Self:
        """Check that the records bind each other, then name them by digest."""
        try:
            _require(
                type(permit) is CommissioningPermit
                and type(bootstrap) is CommissioningBootstrap
                and type(guard_config) is bytes
                and type(manifest) is Manifest
                and type(measurement) is ArtifactMeasurement
            )
            verify_tuple(bootstrap, guard_config, manifest)
            p, m, a = permit.as_dict(), manifest.as_dict(), measurement.as_dict()
            _require(p["manifest_digest"] == manifest.digest)
            _require(p["bootstrap_digest"] == bootstrap.digest)
            for member in (
                "runtime_id",
                "runtime_generation",
                "execution_config_digest",
                "broker_identity",
                "guard_identity",
            ):
                _require(p[member] == m[member])
            _require(a["manifest_digest"] == manifest.digest)
            for member in ("execution_config_digest", "substrate", "guard_identity"):
                _require(a[member] == m[member])
            return cls(
                campaign_id=p["campaign_id"],
                campaign_generation=p["campaign_generation"],
                permit=permit.digest,
                manifest=manifest.digest,
                bootstrap=bootstrap.digest,
                guard=hashlib.sha256(guard_config).hexdigest(),
                execution_config=m["execution_config_digest"],
                broker_run_id=m["broker_identity"]["run_id"],
                runner_image=m["runner_image_digest"],
                artifact=measurement.digest,
            )
        except VerifierRefused:
            raise
        except (ProvisionerRefused, AttributeError, KeyError, TypeError, ValueError):
            _refuse()

    @property
    def digest(self) -> str:
        return hashlib.sha256(
            _encode({name: getattr(self, name) for name in self.__dataclass_fields__})
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class ArtifactBinding:
    """Clause 9's exact artifact tuple: images and image IDs per platform, projection."""

    runner_image: str
    launch_projection: str
    platforms: tuple[tuple[tuple[str, Any], ...], ...]

    @classmethod
    def of(cls, measurement: ArtifactMeasurement) -> Self:
        _require(type(measurement) is ArtifactMeasurement)
        a = measurement.as_dict()
        members = (
            "os",
            "architecture",
            "variant",
            "runtime_profile",
            "oci_digest",
            "observed_image_id",
        )
        platforms = tuple(
            tuple((name, row[name]) for name in members) for row in a["platform_mappings"]
        )
        digests = {row["oci_digest"] for row in a["platform_mappings"]}
        _require(len(digests) == 1)
        return cls(digests.pop(), a["launch_projection"]["sha256"], platforms)

    def as_dict(self) -> dict[str, Any]:
        return {
            "runner_image": self.runner_image,
            "launch_projection": self.launch_projection,
            "platforms": [dict(row) for row in self.platforms],
        }


@dataclass(frozen=True, slots=True)
class CaseObservation:
    """Inert producer output; constructing one proves nothing by itself.

    ``outcome`` is ``pass`` or the producer's reason it is not (``fail``,
    ``unavailable``, ``skipped``, ``timed-out``). ``entries`` carries a
    composite producer's per-sub-case outcomes, for example a network
    producer's case entries; every one must be ``pass``. ``model`` is ``real``
    only for the delivered reply and ``none`` for every case that makes no
    model call. ``provenance`` is ``installation-producer`` or
    ``signed-reply``; a reused artifact observation is reported as
    ``reused-artifact``.
    """

    case: str
    installation_digest: str
    challenge: str
    outcome: str
    provenance: str
    model: str
    execution_config: str
    evidence_sha256: str
    entries: tuple[tuple[str, str], ...] = ()


class CaseProducer(Protocol):
    """A composition over this installation's genuine producers or its signed channel.

    It observes again for every challenge and echoes the challenge and the
    installation digest. There is no default successful implementation.
    """

    async def observe(self, installation_digest: str, challenge: str) -> CaseObservation: ...


@dataclass(frozen=True, slots=True)
class ReusedArtifactObservation:
    """An independently recorded, content-addressed artifact observation (clause 9)."""

    content_digest: str
    raw: bytes = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class PublicationInputs:
    """Writer credentials held only by the verifier; never a success capability.

    ``reobserve`` is the design decision 7 ACL principal comparison the
    Readiness publisher runs before it writes.
    """

    qualification_credential: WorkerCredential
    readiness_credential: WorkerCredential
    ca_pem: str
    qualification_generation: str
    readiness_lifetime_ms: int
    max_readiness_ms: int
    reobserve: Callable[[], Awaitable[None]]

    def __post_init__(self) -> None:
        # Validate before any campaign: a configuration error must never burn
        # an immutable Qualification generation after acceptance.
        try:
            _require(
                type(self.qualification_credential) is WorkerCredential
                and type(self.readiness_credential) is WorkerCredential
                and type(self.ca_pem) is str
                and bool(self.ca_pem)
                and type(self.readiness_lifetime_ms) is int
                and type(self.max_readiness_ms) is int
                and 0 < self.readiness_lifetime_ms <= self.max_readiness_ms <= 9007199254740991
                and callable(self.reobserve)
            )
            _scalar(self.qualification_generation, "generation")
        except (AuthorityRecordInvalid, ValueError, TypeError):
            _refuse()

    def __repr__(self) -> str:
        return "PublicationInputs()"


@dataclass(frozen=True, slots=True)
class Verdict:
    """The decision as a report. ``accepted`` is true only for ``accepted``.

    ``evidence_digest`` names the accepted observations, for the acceptance
    record's ``evidence_digest``; it is ``None`` when refused.
    """

    outcome: str
    reason: str | None
    installation_digest: str
    evidence_digest: str | None
    observations: tuple[CaseObservation, ...]

    @property
    def accepted(self) -> bool:
        return self.outcome == "accepted"


@dataclass(frozen=True, slots=True)
class Publication:
    """``published`` (both written), ``uncertain`` (Qualification only) or ``refused``."""

    outcome: str
    reason: str | None
    qualification: Qualification | None = None
    readiness: Readiness | None = None


def _entries_pass(entries: object) -> bool | None:
    """``None`` when malformed, else whether every entry passed."""
    if type(entries) is not tuple or len(entries) > _MAX_ENTRIES:
        return None
    names: set[str] = set()
    passed = True
    for entry in entries:
        if type(entry) is not tuple or len(entry) != 2:
            return None
        name, outcome = entry
        if type(name) is not str or _ENTRY.fullmatch(name) is None or name in names:
            return None
        if type(outcome) is not str:
            return None
        names.add(name)
        passed = passed and outcome == "pass"
    return passed


def _reused_observation(
    case: str,
    reused: ReusedArtifactObservation,
    binding: ArtifactBinding,
    tuple_: InstallationTuple,
) -> CaseObservation | None:
    """Re-validate a reused artifact observation against the current artifact tuple."""
    try:
        raw = reused.raw
        _require(type(raw) is bytes and 0 < len(raw) <= _MAX_REUSED_BYTES)
        _require(_hex(reused.content_digest))
        _require(hashlib.sha256(raw).hexdigest() == reused.content_digest)
        value = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(type(value) is dict and _encode(value) == raw)
        _require(
            value.keys()
            == {
                "schema_version",
                "record_type",
                "case",
                "artifact_binding",
                "outcome",
                "evidence_sha256",
            }
        )
        _require(type(value["schema_version"]) is int and value["schema_version"] == 1)
        _require(value["record_type"] == _ARTIFACT_RECORD and value["case"] == case)
        _require(value["artifact_binding"] == binding.as_dict())
        _require(value["outcome"] == "pass" and _hex(value["evidence_sha256"]))
    except (VerifierRefused, AuthorityRecordInvalid, UnicodeDecodeError, ValueError):
        return None
    return CaseObservation(
        case=case,
        installation_digest=tuple_.digest,
        challenge="",
        outcome="pass",
        provenance=_REUSED,
        model="none",
        execution_config=tuple_.execution_config,
        evidence_sha256=reused.content_digest,
    )


class CommissioningVerifier:
    """One decision for one installation tuple, then at most one publication."""

    def __init__(
        self,
        *,
        permit: CommissioningPermit,
        bootstrap: CommissioningBootstrap,
        guard_config: bytes,
        manifest: Manifest,
        measurement: ArtifactMeasurement,
        producers: Mapping[str, CaseProducer],
        reused: Mapping[str, ReusedArtifactObservation],
        remeasure: Callable[[], Awaitable[InstallationTuple]],
        publication: PublicationInputs,
        case_timeout_s: float = 120.0,
    ) -> None:
        self._tuple = InstallationTuple.measure(
            permit=permit,
            bootstrap=bootstrap,
            guard_config=guard_config,
            manifest=manifest,
            measurement=measurement,
        )
        self._binding = ArtifactBinding.of(measurement)
        _require(isinstance(producers, Mapping) and isinstance(reused, Mapping))
        _require(set(producers) <= set(REQUIRED_CASES))
        _require(all(callable(getattr(p, "observe", None)) for p in producers.values()))
        # Installation observations are never reused, and no case is ambiguous.
        _require(set(reused) <= REUSABLE_CASES and not set(reused) & set(producers))
        _require(all(type(r) is ReusedArtifactObservation for r in reused.values()))
        _require(callable(remeasure) and type(publication) is PublicationInputs)
        _require(type(case_timeout_s) in (int, float) and 0 < case_timeout_s <= 900)
        self._bootstrap, self._guard_config = bootstrap, guard_config
        self._manifest = manifest
        self._record_id = "sha256:" + measurement.digest
        self._producers, self._reused = dict(producers), dict(reused)
        self._remeasure, self._publication = remeasure, publication
        self._timeout = float(case_timeout_s)
        self._decided = False
        self._accepted: Verdict | None = None
        self._published = False

    @property
    def installation(self) -> InstallationTuple:
        return self._tuple

    def _refused(self, reason: str, observed: list[CaseObservation]) -> Verdict:
        return Verdict("refused", reason, self._tuple.digest, None, tuple(observed))

    async def _current(self) -> bool:
        try:
            current = await self._remeasure()
        except Exception:  # noqa: BLE001 -- an unavailable remeasurement never matches.
            return False
        return type(current) is InstallationTuple and current == self._tuple

    async def _observe(self, case: str, observed: list[CaseObservation]) -> str | None:
        """Observe one case; the refusal reason, or ``None`` when it passed."""
        tuple_ = self._tuple
        if case in self._reused:
            value = _reused_observation(case, self._reused[case], self._binding, tuple_)
            if value is None:
                return "reuse:" + case
            observed.append(value)
            return None
        challenge = str(uuid4())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        try:
            async with asyncio.timeout(self._timeout):
                value = await self._producers[case].observe(tuple_.digest, challenge)
        except TimeoutError:
            return "timed-out:" + case
        except Exception:  # noqa: BLE001 -- an unavailable observation never passes.
            return "unavailable:" + case
        if loop.time() > deadline:  # a producer that swallowed its cancellation
            return "timed-out:" + case
        entries = _entries_pass(getattr(value, "entries", None))
        if (
            type(value) is not CaseObservation
            or not all(
                type(getattr(value, name)) is str
                for name in ("case", "installation_digest", "challenge", "outcome", "provenance")
            )
            or value.case != case
            or value.installation_digest != tuple_.digest
            or value.challenge != challenge
            or value.execution_config != tuple_.execution_config
            or not _hex(value.evidence_sha256)
            or entries is None
        ):
            return "invalid:" + case
        if value.provenance not in _PROVENANCES:
            return "provenance:" + case
        if value.model != ("real" if case in REAL_MODEL_CASES else "none"):
            return "model:" + case
        if value.outcome != "pass":
            return value.outcome + ":" + case if value.outcome else "invalid:" + case
        if not entries:
            return "entry:" + case
        observed.append(value)
        return None

    async def decide(self) -> Verdict:
        """Observe every case once, in order, then re-measure the tuple."""
        _require(not self._decided)
        self._decided = True
        observed: list[CaseObservation] = []
        for case in REQUIRED_CASES:
            if case not in self._producers and case not in self._reused:
                return self._refused("missing:" + case, [])
        for case in REQUIRED_CASES:
            reason = await self._observe(case, observed)
            if reason is not None:
                return self._refused(reason, observed)
        if not await self._current():
            return self._refused("changed-installation", observed)
        evidence = hashlib.sha256(
            _encode(
                {
                    "installation": self._tuple.digest,
                    "observations": [
                        [o.case, o.provenance, o.challenge, o.evidence_sha256, list(o.entries)]
                        for o in observed
                    ],
                }
            )
        ).hexdigest()
        verdict = Verdict("accepted", None, self._tuple.digest, evidence, tuple(observed))
        self._accepted = verdict
        return verdict

    def _require_acceptance(self, verdict: Verdict, acceptance: CommissioningAcceptance) -> None:
        """The issuer's record names this campaign, this evidence and M's Qualification."""
        _require(type(acceptance) is CommissioningAcceptance)
        a, m, t = acceptance.as_dict(), self._manifest.as_dict(), self._tuple
        _require(
            a["campaign"]
            == {
                "campaign_id": t.campaign_id,
                "campaign_generation": t.campaign_generation,
                "permit_digest": t.permit,
                "bootstrap_digest": t.bootstrap,
                "manifest_digest": t.manifest,
                "runtime_id": m["runtime_id"],
                "runtime_generation": m["runtime_generation"],
                "broker_run_id": t.broker_run_id,
            }
        )
        _require(a["evidence_digest"] == verdict.evidence_digest)
        _require(a["qualification_id"] == m["qualification_id"])
        _require(a["qualification_generation"] == self._publication.qualification_generation)

    async def publish(self, verdict: Verdict, acceptance: CommissioningAcceptance) -> Publication:
        """Construct and run both publishers, only for this verifier's accepted verdict.

        ``acceptance`` is the record the issuer wrote for that verdict (clause 6
        step 3). A mismatched record refuses before anything is constructed.
        """
        _require(not self._published and self._accepted is not None)
        _require(verdict is self._accepted)
        self._require_acceptance(verdict, acceptance)
        self._published = True
        # Once publication starts it runs to completion even if the caller is
        # cancelled, so the verifier never stops between the two writes.
        task = asyncio.create_task(self._publish())
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _publish(self) -> Publication:
        """The accepted branch: re-measure, connect both writers, then write."""
        if not await self._current():
            return Publication("refused", "changed-installation")
        p = self._publication
        writers: list[QualificationPublisher | ReadinessPublisher] = []
        qualification: Qualification | None = None
        try:
            q = await QualificationPublisher.connect(
                self._manifest,
                p.qualification_credential,
                p.ca_pem,
                bootstrap=self._bootstrap,
                guard_config=self._guard_config,
            )
            writers.append(q)
            r = await ReadinessPublisher.connect(
                self._manifest,
                p.readiness_credential,
                p.ca_pem,
                trusted_max_readiness_ms=p.max_readiness_ms,
            )
            writers.append(r)
            qualification = await q.publish(
                qualification_generation=p.qualification_generation,
                measurement_record_id=self._record_id,
            )
            readiness = await r.publish(
                qualification,
                lifetime_ms=p.readiness_lifetime_ms,
                measurement_record_id=self._record_id,
                reobserve=p.reobserve,
                expected=None,
            )
        except Exception:  # noqa: BLE001 -- publication diagnostics never escape.
            if qualification is None:
                return Publication("refused", "publication-unavailable")
            # The immutable Qualification exists; Readiness is unconfirmed, so
            # the installation holds failed until a later issuance.
            return Publication("uncertain", "readiness-unconfirmed", qualification)
        finally:
            for writer in writers:
                try:
                    await writer.close()
                except Exception:  # noqa: BLE001
                    pass
        return Publication("published", None, qualification, readiness)

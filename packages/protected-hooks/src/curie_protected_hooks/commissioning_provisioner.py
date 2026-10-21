"""Pure provisioner core: pinned inputs and the B, G, M hash order.

@spec PROTECTED-COMMISSIONING-2. Design decision 1 makes the provisioner the
only creator of the protected lane, from its own pinned profile, projection and
images. This module builds the commissioning bootstrap B, the guard
configuration G holding ``sha256(B)``, the final v1 manifest M whose
``guard_identity.config_sha256`` is ``sha256(G)``, and the content-addressed
artifact measurement file bound to M, in that order. It defines the two
predeclared entry points. It creates nothing, calls no cluster or broker,
issues no permit and authorizes no work. Broker ACL rule recipes come through
the ``BrokerAclRecipes`` seam; their realization is measured elsewhere.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass, fields
from typing import Any, NoReturn, Protocol

from .artifact_measurement import (
    _DELIVERY_SLOTS,
    ArtifactMeasurement,
    parse_artifact_measurement,
)
from .authority_records import (
    _CREDENTIAL,
    AuthorityRecordInvalid,
    Manifest,
    _object,
    _pairs,
    _reject_number,
    _scalar,
    parse_manifest,
)
from .commissioning_bootstrap import CommissioningBootstrap, parse_commissioning_bootstrap
from .commissioning_encoding import CommissioningRecordInvalid

__all__ = [
    "BROKER_PRINCIPALS",
    "COMMISSIONING_ENTRY_POINT",
    "ENTRY_POINT_COMPOSITIONS",
    "PRODUCTION_ENTRY_POINT",
    "PROJECTION_FORMAT",
    "BrokerAclRecipes",
    "ProvisionedTuple",
    "ProvisionerProfile",
    "ProvisionerRefused",
    "construct",
    "parse_guard_config",
    "verify_tuple",
]

# @spec PROTECTED-COMMISSIONING-2: the two predeclared compositions. Nothing a
# caller supplies selects between them; B fixes both before the campaign.
PRODUCTION_ENTRY_POINT = "composition/protected-production-v1"
COMMISSIONING_ENTRY_POINT = "composition/protected-commissioning-v1"
ENTRY_POINT_COMPOSITIONS: dict[str, tuple[str, ...]] = {
    PRODUCTION_ENTRY_POINT: (
        "curie_api.protected_runtime_files:load_ingress",
        "curie_worker.protected_entry:open_runtime",
    ),
    # The standalone ingress and worker compositions that read the
    # provisioner-owned directory; realized by the composition slice.
    COMMISSIONING_ENTRY_POINT: (
        "commissioning-ingress-v1",
        "commissioning-worker-v1",
    ),
}
PROJECTION_FORMAT = "kubernetes-cold-pod-v1"
# Every broker identity with a credential issuance in B, keyed the way G records it.
BROKER_PRINCIPALS = (
    "enqueue",
    "worker",
    "verifier",
    "issuer",
    "control",
    "candidate_enqueue",
    "candidate_worker",
    "qualification_publisher",
)
_GUARD_KIND = "commissioning_guard_config"
_GUARD_MAX_BYTES = 16384
_PINNED_IMAGE = re.compile(r"([\x21-\x3f\x41-\x7e]{1,2000})@(sha256:[0-9a-f]{64})")
# The longest merged rule is the worker's measured command selector (under 600
# characters); the caps leave room for that inventory to grow, not for unbounded input.
_RULE_MAX = 4096
_RULES_MAX = 16384
_RULE = re.compile(rf"[\x21-\x7e](?:[\x20-\x7e]{{0,{_RULE_MAX - 2}}}[\x21-\x7e])?")
# ACL tokens that carry a user's enabled state or password. G hashes rules only.
_STATE_TOKENS = frozenset({"on", "off", "nopass", "resetpass", "reset"})
_PASSWORD_PREFIXES = (">", "<", "#", "!")
_SHA256 = "sha256"


class ProvisionerRefused(RuntimeError):
    """Redacted provisioner refusal, @spec PROTECTED-COMMISSIONING-2."""

    def __init__(self) -> None:
        """Never echo a profile member or value, @spec PROTECTED-COMMISSIONING-2."""
        super().__init__("Commissioning provisioner refused")


def _refuse() -> NoReturn:
    raise ProvisionerRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


_REFUSALS = (
    AttributeError,
    AuthorityRecordInvalid,
    CommissioningRecordInvalid,
    ValueError,
    TypeError,
    KeyError,
    OverflowError,
    RecursionError,
)


class BrokerAclRecipes(Protocol):
    """Seam to the measured broker ACL recipes, @spec PROTECTED-COMMISSIONING-2.

    ``rules`` returns one principal's rule tokens, without its enabled state or
    password. The provisioner hashes them into G; it never widens a role.
    """

    def rules(self, principal: str) -> tuple[str, ...]:
        """@spec PROTECTED-COMMISSIONING-2."""
        ...


@dataclass(frozen=True, slots=True, repr=False)
class ProvisionerProfile:
    """The pinned provisioner inputs, copied on construction, @spec PROTECTED-COMMISSIONING-2.

    There is no entry point, context or kubeconfig member: the entry points are
    the predeclared constants and the cluster is the provisioner's own.
    """

    runtime_id: str
    runtime_generation: str
    qualification_id: str
    broker_identity: dict[str, Any]
    guard_control_id: str
    guard_revision: str
    worker_image: str
    runner_image: str
    bundle_digest: dict[str, Any]
    projection: bytes
    execution_config: bytes
    platform_mappings: list[dict[str, Any]]
    enforcement_evidence_ref: str
    production_credentials: dict[str, Any]
    candidate_credentials: dict[str, Any]
    issuer_identity: dict[str, Any]
    control_identity: dict[str, Any]
    qualification_publisher_identity: dict[str, Any]
    mediator_identity: dict[str, Any]
    gateway_identity: dict[str, Any]
    source_scope: list[dict[str, Any]]
    target_scope: list[str]
    max_permit_ms: str
    admission_policy_sha256: str
    rbac_sha256: str
    network_policy_sha256: str

    def __post_init__(self) -> None:
        """Detach every mutable input, @spec PROTECTED-COMMISSIONING-2."""
        for member in fields(self):
            object.__setattr__(self, member.name, copy.deepcopy(getattr(self, member.name)))

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-2."""
        return "ProvisionerProfile()"


@dataclass(frozen=True, slots=True, repr=False)
class ProvisionedTuple:
    """B, G, M and the measurement file; outputs, not authority, @spec PROTECTED-COMMISSIONING-2."""

    bootstrap: CommissioningBootstrap
    guard_config: bytes
    manifest: Manifest
    measurement: ArtifactMeasurement

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-2."""
        return "ProvisionedTuple()"

    @property
    def guard_digest(self) -> str:
        """sha256(G), M's ``guard_identity.config_sha256``, @spec PROTECTED-COMMISSIONING-2."""
        return hashlib.sha256(self.guard_config).hexdigest()

    @property
    def measurement_file(self) -> tuple[str, bytes]:
        """Filename and bytes for the production worker's measurements directory.

        The filename is the content digest, the name the existing loader opens.
        @spec PROTECTED-COMMISSIONING-6.
        """
        return self.measurement.digest + ".json", self.measurement.canonical_bytes

    @property
    def measurement_record_id(self) -> str:
        """The Qualification ``measurement_record_id`` that selects this file.

        @spec PROTECTED-COMMISSIONING-6.
        """
        return "sha256:" + self.measurement.digest


def _encode(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _decode(raw: Any, limit: int) -> Any:
    _require(type(raw) is bytes and 0 < len(raw) <= limit)
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )
    _require(_encode(value) == raw)
    return value


def _pinned(reference: Any) -> str:
    _require(type(reference) is str)
    match = _PINNED_IMAGE.fullmatch(reference)
    _require(match is not None)
    assert match is not None
    return match.group(2)


def _rules_digest(rules: Any) -> str:
    _require(type(rules) is tuple and bool(rules))
    _require(all(type(rule) is str for rule in rules))
    _require(sum(len(rule) for rule in rules) <= _RULES_MAX)
    for rule in rules:
        _require(type(rule) is str and _RULE.fullmatch(rule) is not None)
        for token in rule.split():
            # ACL keywords are case-insensitive; compare the folded spelling.
            token = token.strip("()").lower()
            _require(token not in _STATE_TOKENS and not token.startswith(_PASSWORD_PREFIXES))
    return hashlib.sha256(_encode(list(rules))).hexdigest()


def _check_projection(profile: ProvisionerProfile, runner_digest: str) -> dict[str, Any]:
    """Match the members P shares with M and the measurement.

    The worker's full projection parser binds P again at launch.
    """
    p = _decode(profile.projection, 262144)
    _require(type(p) is dict and type(p.get("schema_version")) is int)
    _require(p["schema_version"] == 1)
    _require(p.get("format") == PROJECTION_FORMAT)
    _require(p.get("image") == {"reference": profile.runner_image, "digest": runner_digest})
    _require(p.get("embedded_bundle") == {**profile.bundle_digest, "path": "/bundles/current"})
    _require(p.get("delivery_slots") == _DELIVERY_SLOTS)
    substrate = {
        "kind": "kubernetes",
        "authority_domain_id": p.get("authority_domain_id"),
        "launch_identity": p.get("launch_identity"),
        "launch_config_sha256": hashlib.sha256(profile.projection).hexdigest(),
    }
    _object(
        substrate,
        {
            "kind": "substrate",
            "authority_domain_id": "uuid",
            "launch_identity": "opaque_ref",
            "launch_config_sha256": _SHA256,
        },
    )
    return substrate


def _check_execution(
    profile: ProvisionerProfile, runner_digest: str, substrate: dict[str, Any]
) -> str:
    """Match the members E shares with M; the worker parser binds E fully."""
    e = _decode(profile.execution_config, 65536)
    _require(type(e) is dict)
    launch, runner = e.get("launch"), e.get("runner")
    _require(type(launch) is dict and type(runner) is dict)
    _require(launch.get("runner_image_digest") == runner_digest)
    _require(launch.get("substrate") == substrate)
    _require(runner.get("fake_model") is False)
    _require(e.get("policy", {}).get("tool_access") == "read-only")
    _require(e.get("bindings", {}).get("qualified_connectors") == [])
    return hashlib.sha256(profile.execution_config).hexdigest()


def _bootstrap(profile: ProvisionerProfile, execution_digest: str) -> CommissioningBootstrap:
    """Step 1: B holds no manifest digest, guard digest or permit."""
    value = {
        "schema_version": 1,
        "record_type": "commissioning_bootstrap",
        "runtime_id": profile.runtime_id,
        "runtime_generation": profile.runtime_generation,
        "execution_config_digest": execution_digest,
        "entry_points": {
            "production": PRODUCTION_ENTRY_POINT,
            "commissioning": COMMISSIONING_ENTRY_POINT,
        },
        "production_credentials": profile.production_credentials,
        "candidate_credentials": profile.candidate_credentials,
        "issuer_identity": profile.issuer_identity,
        "control_identity": profile.control_identity,
        "qualification_publisher_identity": profile.qualification_publisher_identity,
        "mediator_identity": profile.mediator_identity,
        "gateway_identity": profile.gateway_identity,
        "source_scope": profile.source_scope,
        "target_scope": profile.target_scope,
        "max_permit_ms": profile.max_permit_ms,
    }
    return parse_commissioning_bootstrap(_encode(value))


def _principal_credentials(b: dict[str, Any]) -> dict[str, Any]:
    return {
        **b["production_credentials"],
        "issuer": b["issuer_identity"],
        "control": b["control_identity"],
        "qualification_publisher": b["qualification_publisher_identity"],
        "candidate_enqueue": b["candidate_credentials"]["enqueue"],
        "candidate_worker": b["candidate_credentials"]["worker"],
    }


def _guard(
    profile: ProvisionerProfile, bootstrap: CommissioningBootstrap, acl: BrokerAclRecipes
) -> bytes:
    """Step 2: G holds sha256(B) and each principal's hashed rules."""
    credentials = _principal_credentials(bootstrap.as_dict())
    value = {
        "schema_version": 1,
        "kind": _GUARD_KIND,
        "bootstrap_digest": bootstrap.digest,
        "admission_policy_sha256": profile.admission_policy_sha256,
        "rbac_sha256": profile.rbac_sha256,
        "network_policy_sha256": profile.network_policy_sha256,
        "broker_acl": {
            name: {
                "credential": credentials[name],
                "rules_sha256": _rules_digest(acl.rules(name)),
            }
            for name in BROKER_PRINCIPALS
        },
    }
    raw = _encode(value)
    parse_guard_config(raw)
    return raw


_GUARD_SCHEMA: dict[str, Any] = {
    "schema_version": "version",
    "bootstrap_digest": _SHA256,
    "admission_policy_sha256": _SHA256,
    "rbac_sha256": _SHA256,
    "network_policy_sha256": _SHA256,
    "broker_acl": {
        name: {"credential": _CREDENTIAL, "rules_sha256": _SHA256} for name in BROKER_PRINCIPALS
    },
}


def parse_guard_config(raw: bytes) -> dict[str, Any]:
    """Closed canonical G; structural only, @spec PROTECTED-COMMISSIONING-2."""
    try:
        value = _decode(raw, _GUARD_MAX_BYTES)
        _require(type(value) is dict and value.keys() == _GUARD_SCHEMA.keys() | {"kind"})
        _require(type(value["kind"]) is str and value["kind"] == _GUARD_KIND)
        _object({key: value[key] for key in _GUARD_SCHEMA}, _GUARD_SCHEMA)
    except ProvisionerRefused:
        raise
    except _REFUSALS:
        _refuse()
    result: dict[str, Any] = value
    return result


def _manifest(
    profile: ProvisionerProfile,
    bootstrap: CommissioningBootstrap,
    guard_digest: str,
    substrate: dict[str, Any],
    worker_digest: str,
    runner_digest: str,
) -> Manifest:
    """Step 3: the final v1 M binds sha256(G); it never names B or a permit."""
    b = bootstrap.as_dict()
    value = {
        "schema_version": 1,
        "runtime_id": b["runtime_id"],
        "runtime_generation": b["runtime_generation"],
        "broker_identity": profile.broker_identity,
        "worker_image_digest": worker_digest,
        "runner_image_digest": runner_digest,
        "bundle_digest": profile.bundle_digest,
        "execution_config_digest": b["execution_config_digest"],
        "qualification_id": profile.qualification_id,
        "substrate": substrate,
        "guard_identity": {
            "control_id": profile.guard_control_id,
            "revision": profile.guard_revision,
            "config_sha256": guard_digest,
        },
        "credential_refs": b["production_credentials"],
    }
    return parse_manifest(_encode(value))


def _measurement(
    profile: ProvisionerProfile, manifest: Manifest, runner_digest: str
) -> ArtifactMeasurement:
    """The content-addressed artifact measurement bound to M before the campaign."""
    rows = profile.platform_mappings
    _require(type(rows) is list and bool(rows))
    for row in rows:
        _require(type(row) is dict)
        # The measurement grammar ties oci_digest to this pinned reference.
        _require(row.get("pinned_image_reference") == profile.runner_image)

    def platform(row: dict[str, Any]) -> tuple[str, str, str, str]:
        return row["os"], row["architecture"], row["variant"] or "", row["runtime_profile"]

    m = manifest.as_dict()
    value = {
        "schema_version": 1,
        "kind": "protected-artifact-qualification",
        "manifest_digest": manifest.digest,
        "execution_config_digest": m["execution_config_digest"],
        "substrate": m["substrate"],
        "guard_identity": m["guard_identity"],
        "platform_mappings": sorted(rows, key=platform),
        "launch_projection": {
            "schema_version": 1,
            "format": PROJECTION_FORMAT,
            "sha256": m["substrate"]["launch_config_sha256"],
        },
        "delivery_slots": _DELIVERY_SLOTS,
        "enforcement_evidence_ref": profile.enforcement_evidence_ref,
    }
    return parse_artifact_measurement(_encode(value))


def construct(profile: ProvisionerProfile, *, acl: BrokerAclRecipes) -> ProvisionedTuple:
    """Build B, then G, then M, then the measurement file, @spec PROTECTED-COMMISSIONING-2.

    Permits are issued later, bound to sha256(B) and sha256(M). This creates
    no resource and authorizes nothing.
    """
    try:
        _require(type(profile) is ProvisionerProfile)
        snapshot = ProvisionerProfile(
            **{member.name: getattr(profile, member.name) for member in fields(profile)}
        )
        worker_digest = _pinned(snapshot.worker_image)
        runner_digest = _pinned(snapshot.runner_image)
        _object(snapshot.bundle_digest, {"sha256": _SHA256, "object_identity": "opaque_ref"})
        _scalar(snapshot.enforcement_evidence_ref, "oci_digest")
        for digest in (
            snapshot.admission_policy_sha256,
            snapshot.rbac_sha256,
            snapshot.network_policy_sha256,
        ):
            _scalar(digest, _SHA256)
        substrate = _check_projection(snapshot, runner_digest)
        execution_digest = _check_execution(snapshot, runner_digest, substrate)
        bootstrap = _bootstrap(snapshot, execution_digest)
        guard_config = _guard(snapshot, bootstrap, acl)
        guard_digest = hashlib.sha256(guard_config).hexdigest()
        manifest = _manifest(
            snapshot, bootstrap, guard_digest, substrate, worker_digest, runner_digest
        )
        measurement = _measurement(snapshot, manifest, runner_digest)
        result = ProvisionedTuple(bootstrap, guard_config, manifest, measurement)
        verify_tuple(result.bootstrap, result.guard_config, result.manifest)
        return result
    except ProvisionerRefused:
        raise
    except _REFUSALS:
        _refuse()


def verify_tuple(
    bootstrap: CommissioningBootstrap, guard_config: bytes, manifest: Manifest
) -> None:
    """Check B's digest against G and M, refusal step 1, @spec PROTECTED-COMMISSIONING-4.

    The caller authenticates where B, G and M came from. Returns ``None`` and
    authorizes nothing.
    """
    try:
        _require(type(bootstrap) is CommissioningBootstrap and type(manifest) is Manifest)
        g = parse_guard_config(guard_config)
        b, m = bootstrap.as_dict(), manifest.as_dict()
        _require(g["bootstrap_digest"] == bootstrap.digest)
        _require(m["guard_identity"]["config_sha256"] == hashlib.sha256(guard_config).hexdigest())
        for member in ("runtime_id", "runtime_generation", "execution_config_digest"):
            _require(b[member] == m[member])
        _require(b["production_credentials"] == m["credential_refs"])
        credentials = _principal_credentials(b)
        _require(
            all(
                g["broker_acl"][name]["credential"] == credentials[name]
                for name in BROKER_PRINCIPALS
            )
        )
        _require(
            b["entry_points"]
            == {"production": PRODUCTION_ENTRY_POINT, "commissioning": COMMISSIONING_ENTRY_POINT}
        )
    except ProvisionerRefused:
        raise
    except _REFUSALS:
        _refuse()

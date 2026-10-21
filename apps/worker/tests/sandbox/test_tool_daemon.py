"""The per-claim Docker daemon lifecycle (ADR 0217), driven through SandboxSubstrate.

Interface these tests code against (names verbatim; the implementation builds
exactly this):

``curie_worker.sandbox.types``
    DaemonObjectKind = Literal["Pod", "Service", "NetworkPolicy", "Secret"]
    ListableDaemonKind = Literal["Pod", "Service", "NetworkPolicy"]
    @dataclass(frozen=True) DaemonObjectView(kind, name, uid: str, claim: str | None,
        created_at: datetime | None, phase: str | None = None)
    @dataclass(frozen=True) ToolDaemonMount(files: Mapping[str, str] (repr=False;
        exactly ca.pem, cert.pem, key.pem), env: Mapping[str, str] (exactly
        DOCKER_HOST, DOCKER_PUBLISHED_HOST, DOCKER_TLS_VERIFY, DOCKER_CERT_PATH),
        active_deadline_seconds: int)
    class ClaimNameConflict(SandboxError)   # an HTTP 409 on a claim-name create
    SandboxClient:
        create_claim(name, *, pool, deadline: float (time.monotonic based, required),
            ..., tool_daemon: ToolDaemonMount | None = None) -> None
        create_daemon_object(namespace, manifest, *, request_timeout_seconds) -> str (uid)
            manifest carries "apiVersion", "kind" and "metadata.name"
        list_daemon_objects(namespace, kind: ListableDaemonKind, *, label_selector,
            request_timeout_seconds) -> list[DaemonObjectView]
        delete_daemon_object(namespace, kind, name, *, uid, request_timeout_seconds) -> None
            (absent or uid mismatch: no-op success)
        get_daemon_pod(namespace, name, *, request_timeout_seconds) -> DaemonObjectView | None
        open_daemon_stream(namespace, pod, port, *, timeout_seconds)
            -> AbstractContextManager[socket.socket]

``curie_worker.sandbox.tool_daemon``
    DAEMON_ARGS(port) -> list[str]; EGRESS_LABEL = "curietech.ai/docker-egress";
    CLIENT_FILES = ("ca.pem", "cert.pem", "key.pem")
    issue(claim, namespace, lifetime_s) -> object with .server_files
        ({"ca.pem", "server-cert.pem", "server-key.pem"}) and .client_files
        ({"ca.pem", "cert.pem", "key.pem"}), PEM text values
    request(client, namespace, pod, port, server_hostname, files, path, timeout)
        -> tuple[int, bytes]
    ping(client, namespace, pod, port, server_hostname, files, timeout) -> bool
    Every daemon object is named exactly the claim; the server hostname is
    ``<claim>.<daemon namespace>.svc``. Creation order NetworkPolicy, Service,
    Pod, Secret (ownerReference to the Pod uid); the Secret is never deleted
    explicitly; the NetworkPolicy is deleted only after the Pod and Service
    deletes succeeded.

``curie_worker.sandbox.claim_tokens``
    DOCKER_CERT_DIR = "/var/run/curie/docker"; claim_template_spec(..., tool_daemon=)

Valkey (real, never mocked): association marker ``<prefix>:daemon-claim:<claim>``
(value: the daemon namespace, no TTL, written before the first daemon create)
and client credential ``<prefix>:daemon-tls:<claim>`` (JSON with ca.pem,
cert.pem, key.pem; TTL maxClaimLifetimeSeconds; written through
``AffinityStore.remember_daemon_credential``).

Only ``SandboxClient`` is faked (``conftest.FakeSandboxClient``), and its daemon
serves real mutual TLS from the stored Secret, so a ping here is a genuine
handshake with the claim's own certificate.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.x509.oid import ExtendedKeyUsageOID
from curie_worker.repo_runner_pools import RepoRunnerPool
from curie_worker.sandbox import SandboxSubstrate, SubstrateConfig, tool_daemon
from curie_worker.sandbox import substrate as substrate_module
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.claim_tokens import CLAIM_LABEL, DOCKER_CERT_DIR
from curie_worker.sandbox.substrate import REAP_GRACE_MARGIN_SECONDS
from curie_worker.sandbox.types import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ClaimNameConflict,
    ClaimTimeoutError,
    KubeTransientError,
    NoRouteError,
    QuotaRejection,
    RouteRecord,
    RouteState,
    SandboxError,
    SandboxHandle,
    SuspendedThreadError,
    ToolDaemonMount,
)

from .conftest import FakeDaemonObject, FakeSandboxClient, Op, label_selector_matches

ROOT = Path(__file__).resolve().parents[4]
CHART_DAEMON = ROOT / "charts/curie/templates/docker-daemon.yaml"
DAEMON: dict[str, Any] = json.loads((ROOT / "tests/vectors/daemon-config.json").read_text())[
    "daemon"
]
DAEMON_NS: str = DAEMON["namespace"]
PORT: int = DAEMON["port"]
LIFETIME: int = DAEMON["maxClaimLifetimeSeconds"]
PING_TIMEOUT: int = DAEMON["pingTimeoutSeconds"]
SELECTOR: dict[str, str] = DAEMON["selectorLabels"]
INSTANCE = SELECTOR["app.kubernetes.io/instance"]
INDEX, AMD64, ARM64, BASE = ("sha256:" + c * 64 for c in "abcd")
OTHER = "sha256:" + "e" * 64
DIGESTS = (INDEX, AMD64, ARM64)
REPO = "acme-corp/api"
WEB = "acme-corp/web"
POOL = "acme-api-runner"
BASE_POOL = "curie-runner-pool"
RELEASE_NS = "test-ns"
DOCKER_ENV = ("DOCKER_HOST", "DOCKER_PUBLISHED_HOST", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH")
CLIENT_KEYS = {"ca.pem", "cert.pem", "key.pem"}
SERVER_KEYS = {"ca.pem", "server-cert.pem", "server-key.pem"}
PEM = "-----BEGIN"
EGRESS = [{"cidr": "10.20.0.0/16", "ports": [{"port": 443}]}]


# -- helpers --------------------------------------------------------------------


def _entry(
    *,
    docker: bool = True,
    pool: str = POOL,
    digests: tuple[str, str, str] = DIGESTS,
    egress: list[dict[str, Any]] | None = None,
    daemon: Mapping[str, Any] | None = None,
) -> RepoRunnerPool:
    return RepoRunnerPool.model_validate(
        {
            "pool": pool,
            "digests": list(digests),
            "base": BASE,
            "docker": docker,
            **({"egress": egress} if egress else {}),
            **({"daemon": {**DAEMON, **(daemon or {})}} if docker else {}),
        }
    )


def _substrate(
    k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    pools: Mapping[str, RepoRunnerPool] | None = None,
    **overrides: Any,
) -> SandboxSubstrate:
    return SandboxSubstrate(
        k8s,
        affinity,
        replace(
            config,
            warm_pool=BASE_POOL,
            repo_runner_pools={REPO: _entry()} if pools is None else pools,
            platform_runner_digest=BASE,
            **overrides,
        ),
    )


def _marker_key(prefix: str, claim: str) -> str:
    return f"{prefix}:daemon-claim:{claim}"


def _credential_key(prefix: str, claim: str) -> str:
    return f"{prefix}:daemon-tls:{claim}"


def _credential(r: redis.Redis, prefix: str, claim: str) -> dict[str, str] | None:
    raw = r.get(_credential_key(prefix, claim))
    return None if raw is None else json.loads(raw)


def _digest(pem: str) -> str:
    """Compare key material by digest, so a failing assertion never prints PEM."""

    return hashlib.sha256(pem.encode()).hexdigest()


def _digests(files: Mapping[str, str]) -> dict[str, str]:
    return {name: _digest(value) for name, value in files.items()}


def _host(claim: str) -> str:
    return f"{claim}.{DAEMON_NS}.svc"


def _ping(k8s: FakeSandboxClient, claim: str, files: Mapping[str, str]) -> bool:
    return tool_daemon.ping(k8s, DAEMON_NS, claim, PORT, _host(claim), files, 5.0)


def _ping_with_stored_credential(
    k8s: FakeSandboxClient, r: redis.Redis, prefix: str, claim: str
) -> bool:
    files = _credential(r, prefix, claim)
    assert files is not None, "the claim's client credential is not in Valkey"
    return _ping(k8s, claim, files)


def _terms(selector: str) -> set[str]:
    return set(filter(None, selector.split(",")))


def _retirement_lists(k8s: FakeSandboxClient, claim: str) -> list[Op]:
    return [
        op
        for op in k8s.daemon_calls
        if op.verb == "list"
        and {f"{CLAIM_LABEL}={claim}", f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"}
        <= _terms(op.target)
    ]


def _calls_naming(k8s: FakeSandboxClient, claim: str) -> list[Op]:
    """Daemon calls that address ``claim`` by object name or exact selector."""

    return [
        op
        for op in k8s.daemon_calls
        if op.target == claim or f"{CLAIM_LABEL}={claim}" in _terms(op.target)
    ]


def _assert_no_secret_deletes(k8s: FakeSandboxClient) -> None:
    assert not [op for op in k8s.ops if op.verb == "delete" and op.kind == "Secret"], (
        "the daemon Secret is removed by owner garbage collection, never by name"
    )


def _assert_retired(
    k8s: FakeSandboxClient, r: redis.Redis, prefix: str, claim: str, *, listed: bool = True
) -> None:
    """No daemon object, marker or credential is left for ``claim``.

    ``listed`` also requires a list by the exact claim and managed-by labels,
    which every retirement of a claim the worker no longer tracks needs.
    """

    assert k8s.daemon_objects_of(claim) == {}, sorted(k8s.daemon_objects_of(claim))
    if listed:
        assert _retirement_lists(k8s, claim), "retire by exact claim and managed-by"
    for op in _retirement_lists(k8s, claim):
        assert op.namespace == DAEMON_NS
    _assert_no_secret_deletes(k8s)
    assert r.get(_marker_key(prefix, claim)) is None
    assert r.get(_credential_key(prefix, claim)) is None


def _assert_daemon_live(k8s: FakeSandboxClient, r: redis.Redis, prefix: str, claim: str) -> None:
    assert set(k8s.daemon_objects_of(claim)) == {"NetworkPolicy", "Service", "Pod", "Secret"}
    answered = _ping_with_stored_credential(k8s, r, prefix, claim)
    assert answered, f"the daemon of {claim} refused its own claim's credential"


def _kill_pod(k8s: FakeSandboxClient, claim: str) -> None:
    """Remove the daemon Pod (and, by garbage collection, its Secret) behind the
    worker's back, as an eviction or an operator ``kubectl delete`` would."""

    pod = k8s.daemon_objects.pop((DAEMON_NS, "Pod", claim))
    for key, obj in list(k8s.daemon_objects.items()):
        refs = obj.manifest["metadata"].get("ownerReferences") or []
        if any(ref.get("uid") == pod.uid for ref in refs):
            del k8s.daemon_objects[key]


def _fixed_nonce(monkeypatch: pytest.MonkeyPatch, nonce: str = "abc123") -> None:
    monkeypatch.setattr(
        substrate_module,
        "uuid",
        SimpleNamespace(uuid4=lambda: SimpleNamespace(hex=nonce + "0" * 26)),
    )


def _freeze_wall_clock(monkeypatch: pytest.MonkeyPatch, now: list[datetime]) -> None:
    """Pin the reaper's wall clock to ``now[0]`` (mutable, so a test can advance it)."""

    class _Frozen(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:  # type: ignore[override]
            return now[0] if tz is not None else now[0].replace(tzinfo=None)

    monkeypatch.setattr(substrate_module, "datetime", _Frozen)
    if isinstance(getattr(tool_daemon, "datetime", None), type):
        monkeypatch.setattr(tool_daemon, "datetime", _Frozen)


def _race_hook(k8s: FakeSandboxClient, put: Callable[[], object]) -> None:
    """Run ``put`` once, right after the caller's claim create (the race window)."""

    original = k8s.create_claim
    fired: list[bool] = []

    def create_then_race(name: str, **kwargs: Any) -> None:
        original(name, **kwargs)
        if not fired:
            fired.append(True)
            put()

    k8s.create_claim = create_then_race  # type: ignore[method-assign]


def _chart_args(port: int) -> list[str]:
    text = CHART_DAEMON.read_text()
    match = re.search(r"c\.args == \[(.*?)\]\)", text, re.DOTALL)
    assert match, "the chart's dockerd args rule moved; update this mirror"
    return [arg.replace("{{ $port }}", str(port)) for arg in re.findall(r"'([^']*)'", match[1])]


def _positive_quantity(value: object) -> bool:
    from kubernetes.utils.quantity import parse_quantity

    try:
        return bool(parse_quantity(str(value)) > 0)
    except ValueError:
        return False


def _pod_admission(pod: Mapping[str, Any], classes: set[str]) -> list[str]:
    """A Python mirror of the chart's ``docker-daemon-pods`` admission policy."""

    labels = pod["metadata"].get("labels") or {}
    spec = pod["spec"]
    containers = spec.get("containers") or []
    failed: list[str] = []

    def check(ok: bool, message: str) -> None:
        if not ok:
            failed.append(message)

    check(bool(labels.get(CLAIM_LABEL)), "nonempty claim label")
    check(labels.get(MANAGED_BY_LABEL) == MANAGED_BY_VALUE, "managed-by label")
    check(all(labels.get(k) == v for k, v in SELECTOR.items()), "release selector labels")
    check(labels.get(tool_daemon.EGRESS_LABEL) in classes, "egress class label")
    check(
        len(containers) == 1
        and containers[0].get("name") == "dockerd"
        and containers[0].get("image") == DAEMON["image"]
        and "ephemeralContainers" not in spec,
        "exactly one dockerd container on the daemon image",
    )
    check("initContainers" not in spec, "no init containers")
    for c in containers:
        check("lifecycle" not in c, "no lifecycle hooks")
        for probe in ("livenessProbe", "readinessProbe", "startupProbe"):
            check("exec" not in (c.get(probe) or {}), f"no exec {probe}")
        check("volumeDevices" not in c, "no volumeDevices")
        for mount in c.get("volumeMounts") or []:
            path = mount["mountPath"]
            check(
                ".." not in path
                and (path in ("/var/lib/docker", "/certs") or path.startswith("/certs/")),
                f"mount path {path}",
            )
        check("command" not in c, "no command")
        check("env" not in c and "envFrom" not in c, "no env or envFrom")
        check(c.get("args") == _chart_args(PORT), "exact dockerd args")
        check(not ((c.get("securityContext") or {}).get("capabilities") or {}).get("add"), "caps")
        for port in c.get("ports") or []:
            check(not port.get("hostPort"), "no hostPort")
        resources = c.get("resources") or {}
        for group in ("requests", "limits"):
            for name in ("cpu", "memory", "ephemeral-storage"):
                value = (resources.get(group) or {}).get(name)
                check(value is not None and _positive_quantity(value), f"{group}.{name}")
    check(spec.get("automountServiceAccountToken") is False, "no service account token")
    check(spec.get("serviceAccountName", "default") == "default", "default service account")
    for volume in spec.get("volumes") or []:
        projected = (volume.get("projected") or {}).get("sources") or []
        check(not any("serviceAccountToken" in p for p in projected), "no projected token")
        empty = volume.get("emptyDir")
        check(
            (empty is not None and _positive_quantity(empty.get("sizeLimit")))
            or "secret" in volume,
            f"volume {volume.get('name')} is a bounded emptyDir or a secret",
        )
    for host in ("hostNetwork", "hostPID", "hostIPC"):
        check(not spec.get(host), f"no {host}")
    check(int(spec.get("activeDeadlineSeconds") or 0) > 0, "activeDeadlineSeconds")
    return failed


def _objects_admission(obj: Mapping[str, Any], kind: str) -> list[str]:
    """A Python mirror of the chart's ``docker-daemon-objects`` admission policy."""

    labels = obj["metadata"].get("labels") or {}
    failed: list[str] = []

    def check(ok: bool, message: str) -> None:
        if not ok:
            failed.append(message)

    check(bool(labels.get(CLAIM_LABEL)), "nonempty claim label")
    check(labels.get(MANAGED_BY_LABEL) == MANAGED_BY_VALUE, "managed-by label")
    spec = obj.get("spec") or {}
    if kind == "NetworkPolicy":
        check(spec.get("policyTypes") == ["Ingress"], "ingress-only policyTypes")
        check("egress" not in spec, "no egress")
        own = (spec.get("podSelector") or {}).get("matchLabels") or {}
        check(own.get(CLAIM_LABEL) == labels.get(CLAIM_LABEL), "selects only its own claim")
        for rule in spec.get("ingress") or []:
            peers = rule.get("from") or []
            check(bool(peers), "every rule names peers")
            for peer in peers:
                check("ipBlock" not in peer, "no ipBlock")
                namespaces = (peer.get("namespaceSelector") or {}).get("matchLabels") or {}
                check(
                    namespaces.get("kubernetes.io/metadata.name") == RELEASE_NS,
                    "release namespace",
                )
                pods = (peer.get("podSelector") or {}).get("matchLabels") or {}
                check(pods.get("app.kubernetes.io/component") == "runner-sandbox", "runner")
                check(pods.get("app.kubernetes.io/instance") == INSTANCE, "this release")
                check(
                    any(
                        key
                        not in {
                            "app.kubernetes.io/name",
                            "app.kubernetes.io/instance",
                            "app.kubernetes.io/component",
                        }
                        for key in pods
                    ),
                    "a label beyond the release selector keys",
                )
            ports = rule.get("ports") or []
            check(bool(ports), "every rule names ports")
            for port in ports:
                check(port.get("port") == PORT, "daemon port only")
                check("endPort" not in port, "no endPort")
                check(port.get("protocol", "TCP") == "TCP", "TCP")
    if kind == "Secret":
        check(obj.get("type", "Opaque") in ("Opaque", "kubernetes.io/tls"), "secret type")
    if kind == "Service":
        check(spec.get("type", "ClusterIP") == "ClusterIP", "ClusterIP")
        check(not spec.get("externalIPs"), "no externalIPs")
    return failed


@pytest.fixture
def docker_substrate(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> SandboxSubstrate:
    """An install with one Docker-enabled repository (REPO) and one plain one (WEB)."""

    return _substrate(fake_k8s, affinity, config, {REPO: _entry(), WEB: _entry(docker=False)})


@pytest.fixture
def certs_a() -> Any:
    return tool_daemon.issue("curie-thread-aaaaaaaaaa-a1a1a1", DAEMON_NS, LIFETIME)


@pytest.fixture
def certs_b() -> Any:
    return tool_daemon.issue("curie-thread-bbbbbbbbbb-b2b2b2", DAEMON_NS, LIFETIME)


# -- AC2: certificates and mutual TLS (pure, against the fake daemon) ----------


def test_issue_mints_an_independent_claim_ca_with_server_and_client_leaves(certs_a: Any) -> None:
    claim = "curie-thread-aaaaaaaaaa-a1a1a1"
    assert set(certs_a.server_files) == SERVER_KEYS
    assert set(certs_a.client_files) == CLIENT_KEYS
    assert _digest(certs_a.server_files["ca.pem"]) == _digest(certs_a.client_files["ca.pem"])
    ca = x509.load_pem_x509_certificate(certs_a.client_files["ca.pem"].encode())
    server = x509.load_pem_x509_certificate(certs_a.server_files["server-cert.pem"].encode())
    client = x509.load_pem_x509_certificate(certs_a.client_files["cert.pem"].encode())

    constraints = ca.extensions.get_extension_for_class(x509.BasicConstraints)
    assert constraints.critical and constraints.value.ca is True
    for leaf, purpose in (
        (server, ExtendedKeyUsageOID.SERVER_AUTH),
        (client, ExtendedKeyUsageOID.CLIENT_AUTH),
    ):
        leaf.verify_directly_issued_by(ca)
        assert leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca is False
        usages = list(leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value)
        assert usages == [purpose]
        # The claim lifetime is covered, so a daemon never outlives its certificate.
        assert leaf.not_valid_after_utc - leaf.not_valid_before_utc >= timedelta(seconds=LIFETIME)
        assert leaf.not_valid_before_utc <= datetime.now(UTC)
        assert leaf.not_valid_after_utc >= datetime.now(UTC) + timedelta(seconds=LIFETIME)
    names = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert f"{claim}.{DAEMON_NS}.svc" in names.get_values_for_type(x509.DNSName)
    # Neither the Secret nor the runner ever receives the CA's private key.
    assert "ca-key.pem" not in certs_a.server_files
    assert _digest(certs_a.client_files["key.pem"]) != _digest(
        certs_a.server_files["server-key.pem"]
    )


def test_two_claims_get_unrelated_authorities(certs_a: Any, certs_b: Any) -> None:
    ca_a = x509.load_pem_x509_certificate(certs_a.client_files["ca.pem"].encode())
    client_b = x509.load_pem_x509_certificate(certs_b.client_files["cert.pem"].encode())
    with pytest.raises((ValueError, InvalidSignature)):
        client_b.verify_directly_issued_by(ca_a)


def test_a_claims_own_client_certificate_is_accepted(
    fake_k8s: FakeSandboxClient, certs_a: Any, caplog: pytest.LogCaptureFixture
) -> None:
    claim = "curie-thread-aaaaaaaaaa-a1a1a1"
    fake_k8s.seed_daemon(DAEMON_NS, claim, port=PORT, server_files=certs_a.server_files)
    caplog.set_level(logging.DEBUG)

    accepted = _ping(fake_k8s, claim, certs_a.client_files)
    assert accepted is True
    (stream,) = [op for op in fake_k8s.daemon_calls if op.verb == "stream"]
    assert (stream.namespace, stream.target) == (DAEMON_NS, claim)
    assert 0 < stream.detail <= 5.0  # type: ignore[operator]
    assert PEM not in caplog.text


def test_a_second_claims_client_is_refused(
    fake_k8s: FakeSandboxClient, certs_a: Any, certs_b: Any, caplog: pytest.LogCaptureFixture
) -> None:
    claim = "curie-thread-aaaaaaaaaa-a1a1a1"
    fake_k8s.seed_daemon(DAEMON_NS, claim, port=PORT, server_files=certs_a.server_files)
    caplog.set_level(logging.DEBUG)

    # B's whole credential: the daemon's certificate does not chain to B's CA.
    foreign = _ping(fake_k8s, claim, certs_b.client_files)
    assert foreign is False
    # B's certificate and key presented with A's CA: the daemon refuses B.
    borrowed = {**certs_b.client_files, "ca.pem": certs_a.client_files["ca.pem"]}
    refused = _ping(fake_k8s, claim, borrowed)
    assert refused is False
    # Liveness: A's own credential still works on the same daemon.
    accepted = _ping(fake_k8s, claim, certs_a.client_files)
    assert accepted is True
    assert PEM not in caplog.text


def test_the_server_name_is_checked(fake_k8s: FakeSandboxClient, certs_a: Any) -> None:
    claim = "curie-thread-aaaaaaaaaa-a1a1a1"
    fake_k8s.seed_daemon(DAEMON_NS, claim, port=PORT, server_files=certs_a.server_files)
    other = "curie-thread-bbbbbbbbbb-b2b2b2"
    files = certs_a.client_files
    misnamed = tool_daemon.ping(fake_k8s, DAEMON_NS, claim, PORT, _host(other), files, 5.0)
    assert misnamed is False
    named = tool_daemon.ping(fake_k8s, DAEMON_NS, claim, PORT, _host(claim), files, 5.0)
    assert named is True


@pytest.mark.parametrize(
    "fault",
    ["missing-pod", "failed-pod", "refused", "stream-error", "unready"],
)
def test_ping_fails_closed(fake_k8s: FakeSandboxClient, certs_a: Any, fault: str) -> None:
    claim = "curie-thread-aaaaaaaaaa-a1a1a1"
    seeded = fake_k8s.seed_daemon(DAEMON_NS, claim, port=PORT, server_files=certs_a.server_files)
    if fault == "missing-pod":
        _kill_pod(fake_k8s, claim)
    elif fault == "failed-pod":
        seeded["Pod"].phase = "Failed"
    elif fault == "refused":
        fake_k8s.refused_pods.add(claim)
    elif fault == "stream-error":
        fake_k8s.stream_failures[claim] = KubeTransientError("apiserver 503")
    else:
        fake_k8s.daemon_unready_streams = 1
    answered = _ping(fake_k8s, claim, certs_a.client_files)
    assert answered is False


def test_request_reads_a_chunked_info_answer(fake_k8s: FakeSandboxClient, certs_a: Any) -> None:
    claim = "curie-thread-aaaaaaaaaa-a1a1a1"
    seeded = fake_k8s.seed_daemon(DAEMON_NS, claim, port=PORT, server_files=certs_a.server_files)
    status, body = tool_daemon.request(
        fake_k8s, DAEMON_NS, claim, PORT, _host(claim), certs_a.client_files, "/info", 5.0
    )
    assert status == 200
    assert json.loads(body) == {"ID": f"FAKE:{seeded['Pod'].uid}", "Name": claim}


def test_daemon_args_are_the_charts() -> None:
    assert tool_daemon.DAEMON_ARGS(PORT) == _chart_args(PORT)
    assert tool_daemon.EGRESS_LABEL == "curietech.ai/docker-egress"
    assert tuple(tool_daemon.CLIENT_FILES) == ("ca.pem", "cert.pem", "key.pem")
    assert DOCKER_CERT_DIR == "/var/run/curie/docker"


# -- AC1: provisioning before the claim ---------------------------------------


def test_docker_claim_creates_four_labelled_objects_before_its_claim(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    handle = docker_substrate.claim("T-d", workspace_repo=REPO)
    claim = handle.claim_name
    assert handle.runner_docker is True

    creates = [op for op in fake_k8s.ops if op.verb == "create"]
    assert [op.kind for op in creates] == ["NetworkPolicy", "Service", "Pod", "Secret"]
    assert {(op.namespace, op.target) for op in creates} == {(DAEMON_NS, claim)}
    claim_create = next(i for i, op in enumerate(fake_k8s.ops) if op.verb == "create_claim")
    assert all(fake_k8s.ops.index(op) < claim_create for op in creates)

    objects = fake_k8s.daemon_objects_of(claim)
    assert set(objects) == {"NetworkPolicy", "Service", "Pod", "Secret"}
    for obj in objects.values():
        assert obj.namespace == DAEMON_NS
        assert obj.labels[CLAIM_LABEL] == claim
        assert obj.labels[MANAGED_BY_LABEL] == MANAGED_BY_VALUE
        assert {k: obj.labels.get(k) for k in SELECTOR} == SELECTOR
    assert objects["Pod"].labels[tool_daemon.EGRESS_LABEL] == "public"
    # The Secret belongs to exactly this Pod (same namespace, by uid).
    (owner,) = objects["Secret"].manifest["metadata"]["ownerReferences"]
    assert (owner["kind"], owner["name"], owner["uid"]) == ("Pod", claim, objects["Pod"].uid)
    assert not owner.get("blockOwnerDeletion")
    assert set(objects["Secret"].manifest["stringData"]) == SERVER_KEYS

    marker = redis_client.get(_marker_key(key_prefix, claim))
    assert marker == DAEMON_NS.encode()
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, claim)


def test_daemon_pod_passes_the_charts_pod_admission(
    fake_k8s: FakeSandboxClient, docker_substrate: SandboxSubstrate
) -> None:
    claim = docker_substrate.claim("T-adm", workspace_repo=REPO).claim_name
    pod = fake_k8s.daemon_objects_of(claim)["Pod"].manifest

    assert _pod_admission(pod, {"public"}) == []
    assert pod["spec"]["activeDeadlineSeconds"] == LIFETIME
    # The mirror is not vacuous: each of these the chart refuses.
    mutations: tuple[Callable[[dict[str, Any]], object], ...] = (
        lambda p: p["spec"]["containers"][0].__setitem__("env", [{"name": "A", "value": "b"}]),
        lambda p: p["spec"]["containers"][0]["args"].reverse(),
        lambda p: p["spec"].__setitem__("automountServiceAccountToken", True),
        lambda p: p["spec"].__setitem__("initContainers", [{"name": "x"}]),
        lambda p: p["spec"].pop("activeDeadlineSeconds"),
        lambda p: p["metadata"]["labels"].__setitem__(tool_daemon.EGRESS_LABEL, "repo-x"),
    )
    for mutate in mutations:
        broken = json.loads(json.dumps(pod))
        mutate(broken)
        assert _pod_admission(broken, {"public"}) != []


def test_service_policy_and_secret_pass_the_charts_object_admission(
    fake_k8s: FakeSandboxClient, docker_substrate: SandboxSubstrate
) -> None:
    claim = docker_substrate.claim("T-obj", workspace_repo=REPO).claim_name
    objects = fake_k8s.daemon_objects_of(claim)
    for kind in ("NetworkPolicy", "Service", "Secret"):
        assert _objects_admission(objects[kind].manifest, kind) == [], kind

    policy = objects["NetworkPolicy"].manifest["spec"]
    # The extra peer key is the runner pod's own claim label, so only this
    # claim's runner reaches this claim's daemon.
    (rule,) = policy["ingress"]
    (peer,) = rule["from"]
    assert peer["podSelector"]["matchLabels"][CLAIM_LABEL] == claim
    service = objects["Service"].manifest["spec"]
    assert service["clusterIP"] == "None"
    assert label_selector_matches(
        objects["Pod"].labels, ",".join(f"{k}={v}" for k, v in service["selector"].items())
    )
    assert service["selector"][CLAIM_LABEL] == claim
    assert PORT in {port["port"] for port in service["ports"]}
    broken = json.loads(json.dumps(objects["NetworkPolicy"].manifest))
    broken["spec"]["egress"] = [{}]
    assert _objects_admission(broken, "NetworkPolicy") != []


@pytest.mark.parametrize(
    ("key", "egress", "expected"),
    [
        pytest.param(REPO, None, "public", id="public"),
        pytest.param(
            "Acme-Corp/API",
            EGRESS,
            "repo-" + hashlib.sha256(b"Acme-Corp/API").hexdigest()[:16],
            id="allowlisted",
        ),
    ],
)
def test_egress_class_is_the_charts(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    key: str,
    egress: list[dict[str, Any]] | None,
    expected: str,
) -> None:
    substrate = _substrate(fake_k8s, affinity, config, {key: _entry(egress=egress)})
    claim = substrate.claim("T-egress", workspace_repo=REPO).claim_name
    pod = fake_k8s.daemon_objects_of(claim)["Pod"]
    assert pod.labels[tool_daemon.EGRESS_LABEL] == expected
    assert _pod_admission(pod.manifest, {expected}) == []


def test_a_daemon_that_never_answers_fails_the_claim_within_one_budget(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    fake_k8s.daemon_unready_streams = 10**6
    started = time.monotonic()
    with pytest.raises(ClaimTimeoutError):
        docker_substrate.claim("T-never", workspace_repo=REPO)
    elapsed = time.monotonic() - started

    # One shared deadline across daemon start, bind and FQDN: not two budgets.
    assert elapsed < 1.5 * config.claim_timeout_seconds
    (claim_op,) = [op for op in fake_k8s.ops if op.verb == "create_claim"]
    assert claim_op.detail <= started + config.claim_timeout_seconds + 0.05  # type: ignore[operator]
    claim = claim_op.target
    assert claim not in fake_k8s.claims
    assert fake_k8s.daemon_objects == {}
    assert redis_client.get(_marker_key(key_prefix, claim)) is None
    assert redis_client.get(_credential_key(key_prefix, claim)) is None
    assert docker_substrate.lookup("T-never") is None


def test_a_daemon_that_answers_late_still_serves_the_claim(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    fake_k8s.daemon_unready_streams = 3
    handle = docker_substrate.claim("T-late", workspace_repo=REPO)
    assert len([op for op in fake_k8s.daemon_calls if op.verb == "stream"]) >= 4
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, handle.claim_name)


@pytest.mark.parametrize("answers", [False, True], ids=["never-ready", "ready"])
def test_slow_successful_operations_spend_the_same_single_deadline(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    answers: bool,
) -> None:
    budget = 3.0
    substrate = _substrate(fake_k8s, affinity, config, claim_timeout_seconds=budget)
    fake_k8s.daemon_op_delay_seconds = 0.3  # four creates: 1.2 s
    fake_k8s.claim_create_delay_seconds = 0.5  # the claim: 0.5 s
    if not answers:
        fake_k8s.daemon_unready_streams = 10**6
    started = time.monotonic()
    if answers:
        handle = substrate.claim("T-slow", workspace_repo=REPO)
        assert handle.claim_name in fake_k8s.claims
        return
    with pytest.raises(ClaimTimeoutError):
        substrate.claim("T-slow", workspace_repo=REPO)
    # Re-arming the budget after provisioning or after the claim create would
    # take about 1.7 s + budget.
    assert time.monotonic() - started < budget + 1.0
    assert fake_k8s.daemon_objects == {}


@pytest.mark.parametrize("failing", ["NetworkPolicy", "Service", "Pod", "Secret"])
def test_a_failed_create_rolls_back_exactly_what_this_attempt_created(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    failing: str,
) -> None:
    boom = SandboxError(f"admission refused the {failing}")
    fake_k8s.daemon_failures[("create", failing)] = boom
    with pytest.raises(SandboxError) as raised:
        docker_substrate.claim("T-partial", workspace_repo=REPO)

    assert raised.value is boom
    assert not [op for op in fake_k8s.ops if op.verb == "create_claim"]
    assert fake_k8s.claims == {}
    assert fake_k8s.daemon_objects == {}
    creates = [op for op in fake_k8s.ops if op.verb == "create" and op.kind != failing]
    deletes = [op for op in fake_k8s.ops if op.verb == "delete"]
    # Each delete names an object this attempt created, fenced by its uid.
    assert {(op.kind, op.target, op.detail) for op in deletes} == {
        (op.kind, op.target, op.detail) for op in creates if op.kind != "Secret"
    }
    _assert_no_secret_deletes(fake_k8s)
    claim = next(op.target for op in fake_k8s.ops if op.verb == "create")
    assert redis_client.get(_marker_key(key_prefix, claim)) is None
    assert redis_client.get(_credential_key(key_prefix, claim)) is None


def test_a_failed_claim_create_retires_the_daemon_and_raises_the_original(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    boom = RuntimeError("apiserver refused the claim")
    fake_k8s.create_claim_error = boom
    with pytest.raises(RuntimeError) as raised:
        docker_substrate.claim("T-claimfail", workspace_repo=REPO)
    assert raised.value is boom
    (claim_op,) = [op for op in fake_k8s.ops if op.verb == "create_claim"]
    assert fake_k8s.claims == {}
    _assert_retired(fake_k8s, redis_client, key_prefix, claim_op.target, listed=False)


def test_a_claim_persisted_behind_a_timeout_is_removed_with_its_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    boom = TimeoutError("read timed out after the API server stored the claim")
    fake_k8s.create_claim_persist_error = boom
    with pytest.raises(TimeoutError) as raised:
        docker_substrate.claim("T-ambiguous", workspace_repo=REPO)
    assert raised.value is boom
    (claim_op,) = [op for op in fake_k8s.ops if op.verb == "create_claim"]
    assert claim_op.target not in fake_k8s.claims
    _assert_retired(fake_k8s, redis_client, key_prefix, claim_op.target, listed=False)


def test_a_failed_valkey_credential_write_rolls_the_daemon_back(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boom = redis.ConnectionError("valkey went away")

    def refuse(*_args: object, **_kwargs: object) -> None:
        raise boom

    monkeypatch.setattr(affinity, "remember_daemon_credential", refuse)
    with pytest.raises(redis.ConnectionError) as raised:
        docker_substrate.claim("T-valkey", workspace_repo=REPO)
    assert raised.value is boom
    claim = next(op.target for op in fake_k8s.ops if op.verb == "create")
    assert claim not in fake_k8s.claims
    assert fake_k8s.daemon_objects == {}
    assert redis_client.get(_marker_key(key_prefix, claim)) is None


def test_a_bind_failure_retires_the_claim_and_its_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    fake_k8s.bind_ready = False
    with pytest.raises(ClaimTimeoutError):
        docker_substrate.claim("T-bind", workspace_repo=REPO)
    (claim_op,) = [op for op in fake_k8s.ops if op.verb == "create_claim"]
    assert claim_op.target not in fake_k8s.claims
    _assert_retired(fake_k8s, redis_client, key_prefix, claim_op.target, listed=False)


@pytest.mark.parametrize("repo", [WEB, None], ids=["non-opted-repo", "no-repo"])
def test_a_claim_without_docker_issues_no_daemon_call(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    repo: str | None,
) -> None:
    handle = docker_substrate.claim("T-plain", workspace_repo=repo)
    assert handle.runner_docker is False
    assert fake_k8s.daemon_calls == []
    stored = fake_k8s.claims[handle.claim_name]
    assert stored.tool_daemon is None
    assert not any(key.startswith("DOCKER_") for key in stored.env)
    assert redis_client.get(_marker_key(key_prefix, handle.claim_name)) is None
    assert stored.deadline is not None


# -- AC2: the runner's mount ----------------------------------------------------


def test_the_runner_gets_the_four_docker_values_and_only_the_client_files(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    handle = docker_substrate.claim("T-mount", workspace_repo=REPO)
    claim = handle.claim_name
    stored = fake_k8s.claims[claim]
    mount = stored.tool_daemon
    assert isinstance(mount, ToolDaemonMount)

    assert set(mount.files) == CLIENT_KEYS
    assert dict(mount.env) == {
        "DOCKER_HOST": f"tcp://{_host(claim)}:{PORT}",
        "DOCKER_PUBLISHED_HOST": _host(claim),
        "DOCKER_TLS_VERIFY": "1",
        "DOCKER_CERT_PATH": DOCKER_CERT_DIR,
    }
    assert set(mount.env) == set(DOCKER_ENV)
    assert mount.active_deadline_seconds == LIFETIME
    # The mounted credential is the one the worker pings with, and it works.
    stored_credential = _credential(redis_client, key_prefix, claim)
    assert stored_credential is not None
    assert _digests(mount.files) == _digests(stored_credential)
    answered = _ping(fake_k8s, claim, mount.files)
    assert answered is True

    # No PEM on the claim env, in the logs, on the handle or the route.
    assert not any(key.startswith("DOCKER_") for key in stored.env)
    assert PEM not in json.dumps(stored.env)
    assert PEM not in caplog.text
    assert PEM not in repr(handle)
    assert PEM not in repr(mount)
    route = redis_client.get(f"{key_prefix}:route:T-mount")
    assert route is not None and PEM.encode() not in route


def test_the_valkey_credential_and_marker_lifetimes(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    lifetime = 4321
    substrate = _substrate(
        fake_k8s,
        affinity,
        config,
        {REPO: _entry(daemon={"maxClaimLifetimeSeconds": lifetime})},
    )
    claim = substrate.claim("T-ttl", workspace_repo=REPO).claim_name

    # The marker associates claim and daemon until a clean retire: no TTL.
    assert redis_client.ttl(_marker_key(key_prefix, claim)) == -1
    assert 0 < redis_client.ttl(_credential_key(key_prefix, claim)) <= lifetime
    pod_spec = fake_k8s.daemon_objects_of(claim)["Pod"].manifest["spec"]
    assert pod_spec["activeDeadlineSeconds"] == lifetime
    mount = fake_k8s.claims[claim].tool_daemon
    assert mount is not None and mount.active_deadline_seconds == lifetime


# -- AC3: every retirement path ---------------------------------------------------


def _docker_route(substrate: SandboxSubstrate, thread: str = "T-ret") -> tuple[SandboxHandle, str]:
    handle = substrate.claim(thread, workspace_repo=REPO)
    return handle, handle.claim_name


def test_release_retires_the_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    assert docker_substrate.release("T-ret") is True
    assert claim not in fake_k8s.claims
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_release_if_claim_retires_the_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    assert docker_substrate.release_if_claim("T-ret", "some-other-claim") is False
    assert set(fake_k8s.daemon_objects_of(claim)) == {"NetworkPolicy", "Service", "Pod", "Secret"}
    assert docker_substrate.release_if_claim("T-ret", claim) is True
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_release_claim_retires_the_daemon_and_a_retry_is_a_no_op(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    handle, claim = _docker_route(docker_substrate)
    docker_substrate.release_claim("T-ret", handle)
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)
    deletes_before = [op for op in fake_k8s.ops if op.verb == "delete"]

    docker_substrate.release_claim("T-ret", handle)  # idempotent, raises nothing

    assert [op for op in fake_k8s.ops if op.verb == "delete"] == deletes_before
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_terminate_thread_retires_the_routed_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    handle, claim = _docker_route(docker_substrate)
    observation = docker_substrate.terminate_thread(
        "T-ret", claim_name=claim, sandbox_name=handle.sandbox_name
    )
    assert observation is not None
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


@pytest.mark.parametrize("credential", ["present", "expired"])
def test_terminate_with_only_the_stored_name_retires_through_the_marker(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    credential: str,
) -> None:
    handle, claim = _docker_route(docker_substrate)
    affinity.delete_if_claim("T-ret", claim)  # only the SQL row still names it
    if credential == "expired":
        redis_client.delete(_credential_key(key_prefix, claim))

    docker_substrate.terminate_thread("T-ret", claim_name=claim, sandbox_name=handle.sandbox_name)

    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_terminate_of_a_non_opted_claim_by_stored_name_makes_no_daemon_call(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
) -> None:
    handle = docker_substrate.claim("T-web", workspace_repo=WEB)
    affinity.delete_if_claim("T-web", handle.claim_name)
    docker_substrate.terminate_thread(
        "T-web", claim_name=handle.claim_name, sandbox_name=handle.sandbox_name
    )
    assert handle.claim_name not in fake_k8s.claims
    assert fake_k8s.daemon_calls == []


def test_delete_detached_retires_the_daemon(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    record = affinity.get("T-ret")
    assert record is not None
    fake_k8s.quota_headroom_results = [True]
    rejection = QuotaRejection(quota_name="q", requested={}, used={}, hard={})

    assert docker_substrate.delete_detached(record, rejection, deadline=time.monotonic() + 5.0)

    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_suspend_retires_the_daemon_and_resume_provisions_a_fresh_one(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    docker_substrate.suspend("T-ret", history_ref="h-1")

    record = affinity.get("T-ret")
    assert record is not None and record.state is RouteState.SUSPENDED
    assert claim in fake_k8s.claims  # suspend keeps the claim; only the daemon goes
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)

    resumed = docker_substrate.resume("T-ret")
    assert resumed.claim_name != claim
    assert claim not in fake_k8s.claims
    assert resumed.runner_docker is True
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, resumed.claim_name)


def test_suspend_retires_the_daemon_even_when_the_mode_change_fails(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    boom = KubeTransientError("apiserver 503 on the sandbox patch")

    def refuse(name: str, mode: str) -> None:
        raise boom

    monkeypatch.setattr(fake_k8s, "set_sandbox_mode", refuse)
    with pytest.raises(KubeTransientError) as raised:
        docker_substrate.suspend("T-ret", history_ref="h-1")
    assert raised.value is boom
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_policy_change_eviction_retires_the_old_daemon(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    first = _substrate(fake_k8s, affinity, config).claim("T-pol", workspace_repo=REPO)
    rebuilt = {REPO: _entry(digests=(INDEX, OTHER, ARM64))}
    second = _substrate(fake_k8s, affinity, config, rebuilt).claim("T-pol", workspace_repo=REPO)

    assert second.claim_name != first.claim_name
    _assert_retired(fake_k8s, redis_client, key_prefix, first.claim_name)
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, second.claim_name)


def test_the_reaper_retires_an_unrouted_aged_docker_claims_daemon(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    affinity.delete_if_claim("T-ret", claim)
    age = config.claim_timeout_seconds + REAP_GRACE_MARGIN_SECONDS + 60
    fake_k8s.claims[claim].created_at = datetime.now(UTC) - timedelta(seconds=age)

    assert claim in docker_substrate.reap_orphans()
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


def test_the_reaper_retires_a_non_opted_claim_with_no_daemon_call_for_it(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
) -> None:
    handle = docker_substrate.claim("T-web", workspace_repo=WEB)
    affinity.delete_if_claim("T-web", handle.claim_name)
    age = config.claim_timeout_seconds + REAP_GRACE_MARGIN_SECONDS + 60
    fake_k8s.claims[handle.claim_name].created_at = datetime.now(UTC) - timedelta(seconds=age)

    assert handle.claim_name in docker_substrate.reap_orphans()
    assert _calls_naming(fake_k8s, handle.claim_name) == []
    assert not [op for op in fake_k8s.daemon_calls if op.verb == "delete"]


# -- AC3: handoff -----------------------------------------------------------------


def test_handoff_retires_the_old_claims_daemon_after_the_swap(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    first, claim = _docker_route(docker_substrate)
    candidate = docker_substrate.handoff("T-ret", expected=first, env={}, workspace_repo=REPO)
    assert candidate.claim_name != claim
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, candidate.claim_name)


def test_handoff_refusal_retires_the_candidates_daemon_only(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    first, claim = _docker_route(docker_substrate)
    seen: list[SandboxHandle] = []
    refusal = ValueError("candidate refused")

    def refuse(candidate: SandboxHandle) -> None:
        seen.append(candidate)
        raise refusal

    with pytest.raises(ValueError) as raised:
        docker_substrate.handoff(
            "T-ret", expected=first, env={}, workspace_repo=REPO, validate_candidate=refuse
        )
    assert raised.value is refusal
    (candidate,) = seen
    _assert_retired(fake_k8s, redis_client, key_prefix, candidate.claim_name)
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, claim)


def test_handoff_refusal_keeps_its_error_when_cleanup_also_fails(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    first, _claim = _docker_route(docker_substrate)
    refusal = ValueError("candidate refused")
    seen: list[SandboxHandle] = []

    def refuse(candidate: SandboxHandle) -> None:
        seen.append(candidate)
        fake_k8s.daemon_failures[("delete", "Pod")] = SandboxError("pod delete failed")
        raise refusal

    with pytest.raises(ValueError) as raised:
        docker_substrate.handoff(
            "T-ret", expected=first, env={}, workspace_repo=REPO, validate_candidate=refuse
        )
    assert raised.value is refusal
    (candidate,) = seen
    # The unfinished retirement leaves its anchor and marker for a retry.
    assert "NetworkPolicy" in fake_k8s.daemon_objects_of(candidate.claim_name)
    assert redis_client.get(_marker_key(key_prefix, candidate.claim_name)) is not None


def test_handoff_that_loses_its_fence_retires_the_candidates_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    first, claim = _docker_route(docker_substrate)
    stale = replace(first, generation=first.generation + 7)
    with pytest.raises(NoRouteError):
        docker_substrate.handoff("T-ret", expected=stale, env={}, workspace_repo=REPO)
    (candidate,) = [op.target for op in fake_k8s.ops if op.verb == "create_claim"][1:]
    _assert_retired(fake_k8s, redis_client, key_prefix, candidate)
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, claim)


# -- AC3: the route race ------------------------------------------------------------


def test_race_loser_to_a_suspended_winner_retires_its_daemon(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    fake_k8s.create_claim("claim-winner", pool=POOL, deadline=time.monotonic() + 300)
    winner = SandboxHandle(
        thread_key="T-race",
        claim_name="claim-winner",
        sandbox_name="sbx-claim-winner",
        namespace=RELEASE_NS,
        service_fqdn="sbx-claim-winner.test-ns.svc.cluster.local",
        port=8080,
        session_id="sess-w",
        workspace_repo=REPO,
    )
    _race_hook(
        fake_k8s,
        lambda: affinity.put_if_absent(
            "T-race", RouteRecord(handle=winner, state=RouteState.SUSPENDED), ttl_seconds=60
        ),
    )
    with pytest.raises(SuspendedThreadError):
        docker_substrate.claim("T-race", workspace_repo=REPO)
    loser = [op.target for op in fake_k8s.ops if op.verb == "create_claim"][-1]
    _assert_retired(fake_k8s, redis_client, key_prefix, loser)


def test_race_loser_to_an_adoptable_docker_winner_retires_only_its_own_daemon(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    working = docker_substrate.claim("T-winner-src", workspace_repo=REPO)
    winner = replace(working, thread_key="T-race")
    _race_hook(
        fake_k8s,
        lambda: affinity.put_if_absent("T-race", RouteRecord(handle=winner), ttl_seconds=60),
    )

    assert docker_substrate.claim("T-race", workspace_repo=REPO) == winner

    loser = [op.target for op in fake_k8s.ops if op.verb == "create_claim"][-1]
    assert loser != winner.claim_name
    _assert_retired(fake_k8s, redis_client, key_prefix, loser)
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, winner.claim_name)


def test_race_winner_whose_daemon_died_is_evicted_like_a_dead_one(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    dead = docker_substrate.claim("T-dead-src", workspace_repo=REPO)
    _kill_pod(fake_k8s, dead.claim_name)
    winner = replace(dead, thread_key="T-race")
    _race_hook(
        fake_k8s,
        lambda: affinity.put_if_absent("T-race", RouteRecord(handle=winner), ttl_seconds=60),
    )

    handle = docker_substrate.claim("T-race", workspace_repo=REPO)

    assert handle.claim_name != winner.claim_name
    assert winner.claim_name not in fake_k8s.claims
    _assert_retired(fake_k8s, redis_client, key_prefix, winner.claim_name)
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, handle.claim_name)


@pytest.mark.parametrize("cleanup_fails", [False, True], ids=["clean", "cleanup-fails"])
def test_race_policy_failure_retires_the_loser_and_keeps_the_primary_error(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    cleanup_fails: bool,
) -> None:
    fake_k8s.create_claim("claim-winner", pool=POOL, deadline=time.monotonic() + 300)
    winner = SandboxHandle(
        thread_key="T-race",
        claim_name="claim-winner",
        sandbox_name="sbx-claim-winner",
        namespace=RELEASE_NS,
        service_fqdn="sbx-claim-winner.test-ns.svc.cluster.local",
        port=8080,
        session_id="sess-w",
        workspace_repo=REPO,
        runner_digests=DIGESTS,
        runner_docker=True,
    )
    _race_hook(
        fake_k8s,
        lambda: affinity.put_if_absent("T-race", RouteRecord(handle=winner), ttl_seconds=60),
    )
    boom = KubeTransientError("policy read failed")

    def policy_raises(*_args: object, **_kwargs: object) -> bool:
        if cleanup_fails:
            fake_k8s.daemon_failures[("delete", "Pod")] = SandboxError("pod delete failed")
        raise boom

    monkeypatch.setattr(docker_substrate, "_route_matches_runner", policy_raises)
    with pytest.raises(KubeTransientError) as raised:
        docker_substrate.claim("T-race", workspace_repo=REPO)
    assert raised.value is boom
    loser = [op.target for op in fake_k8s.ops if op.verb == "create_claim"][-1]
    if cleanup_fails:
        assert "NetworkPolicy" in fake_k8s.daemon_objects_of(loser)
        assert redis_client.get(_marker_key(key_prefix, loser)) is not None
    else:
        _assert_retired(fake_k8s, redis_client, key_prefix, loser)


def test_repeated_races_retire_the_losers_daemon(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(affinity, "put_if_absent", lambda *_a, **_k: False)
    with pytest.raises(NoRouteError):
        docker_substrate.claim("T-races", workspace_repo=REPO)
    loser = [op.target for op in fake_k8s.ops if op.verb == "create_claim"][-1]
    _assert_retired(fake_k8s, redis_client, key_prefix, loser)


# -- AC3: failure preservation ----------------------------------------------------


def test_a_failed_claim_delete_still_retires_the_daemon_and_raises_the_claim_error(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    boom = KubeTransientError("claim delete failed")

    def refuse(name: str, *, request_timeout_seconds: float) -> None:
        raise boom

    monkeypatch.setattr(fake_k8s, "delete_claim", refuse)
    with pytest.raises(KubeTransientError) as raised:
        docker_substrate.release("T-ret")
    assert raised.value is boom
    assert fake_k8s.daemon_objects_of(claim) == {}
    assert redis_client.get(_marker_key(key_prefix, claim)) is None


def test_a_claim_delete_that_times_out_still_leaves_the_daemon_a_budget(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claim delete that spends its whole timeout must not starve daemon cleanup."""

    _handle, claim = _docker_route(docker_substrate)
    skew = [0.0]
    real = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: real() + skew[0])
    boom = KubeTransientError("claim delete timed out")

    def time_out(name: str, *, request_timeout_seconds: float) -> None:
        skew[0] += request_timeout_seconds + 1.0  # the full budget elapsed
        raise boom

    monkeypatch.setattr(fake_k8s, "delete_claim", time_out)
    with pytest.raises(KubeTransientError) as raised:
        docker_substrate.release("T-ret")
    assert raised.value is boom
    assert fake_k8s.daemon_objects_of(claim) == {}
    assert redis_client.get(_marker_key(key_prefix, claim)) is None


def test_a_failed_daemon_delete_is_raised_keeps_the_anchor_and_a_retry_finishes(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    _handle, claim = _docker_route(docker_substrate)
    boom = SandboxError("pod delete failed")
    fake_k8s.daemon_failures[("delete", "Pod")] = boom
    with pytest.raises(SandboxError) as raised:
        docker_substrate.release("T-ret")
    assert raised.value is boom
    # The NetworkPolicy goes only after the Pod and Service deletes succeeded.
    assert "NetworkPolicy" in fake_k8s.daemon_objects_of(claim)
    assert redis_client.get(_marker_key(key_prefix, claim)) is not None

    del fake_k8s.daemon_failures[("delete", "Pod")]
    docker_substrate.release("T-ret")
    _assert_retired(fake_k8s, redis_client, key_prefix, claim)


@pytest.mark.parametrize("cleanup", ["claim-delete", "daemon-delete"])
def test_a_bind_failure_keeps_its_error_when_cleanup_also_fails(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    cleanup: str,
) -> None:
    fake_k8s.bind_ready = False
    if cleanup == "claim-delete":

        def refuse(name: str, *, request_timeout_seconds: float) -> None:
            raise RuntimeError("claim delete failed")

        monkeypatch.setattr(fake_k8s, "delete_claim", refuse)
    else:
        fake_k8s.daemon_failures[("delete", "Pod")] = SandboxError("pod delete failed")

    with pytest.raises(ClaimTimeoutError):
        docker_substrate.claim("T-bind2", workspace_repo=REPO)
    (claim_op,) = [op for op in fake_k8s.ops if op.verb == "create_claim"]
    if cleanup == "claim-delete":
        # The daemon is still retired when the claim delete raised.
        assert fake_k8s.daemon_objects_of(claim_op.target) == {}
    else:
        assert claim_op.target not in fake_k8s.claims


# -- AC3: 409 name collisions ---------------------------------------------------------


def test_a_colliding_docker_claim_keeps_every_existing_daemon_object(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixed_nonce(monkeypatch)
    first, claim = _docker_route(docker_substrate)
    before = {kind: obj.uid for kind, obj in fake_k8s.daemon_objects_of(claim).items()}
    marker = redis_client.get(_marker_key(key_prefix, claim))
    credential = _credential(redis_client, key_prefix, claim)

    # The candidate draws the same name: its first daemon create is a 409.
    with pytest.raises(SandboxError):
        docker_substrate.handoff("T-ret", expected=first, env={}, workspace_repo=REPO)

    assert {kind: obj.uid for kind, obj in fake_k8s.daemon_objects_of(claim).items()} == before
    assert not [op for op in fake_k8s.ops if op.verb == "delete"]
    assert redis_client.get(_marker_key(key_prefix, claim)) == marker
    after = _credential(redis_client, key_prefix, claim)
    assert after is not None and credential is not None
    assert _digests(after) == _digests(credential)
    assert claim in fake_k8s.claims
    answered = _ping_with_stored_credential(fake_k8s, redis_client, key_prefix, claim)
    assert answered is True


def test_a_colliding_plain_claim_survives_and_only_our_daemon_is_rolled_back(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixed_nonce(monkeypatch)
    name = config.claim_name_for("T-clash", "abc123")
    fake_k8s.create_claim(name, pool=BASE_POOL, deadline=time.monotonic() + 300)

    with pytest.raises(ClaimNameConflict):
        docker_substrate.claim("T-clash", workspace_repo=REPO)

    assert name in fake_k8s.claims, "the other claim at this name must survive"
    assert not [op for op in fake_k8s.ops if op.verb == "delete_claim" and op.target == name]
    assert fake_k8s.daemon_objects_of(name) == {}
    assert redis_client.get(_marker_key(key_prefix, name)) is None
    assert redis_client.get(_credential_key(key_prefix, name)) is None


# -- AC3: the orphan sweep ---------------------------------------------------------------


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[datetime]]:
    now = [datetime.now(UTC)]
    _freeze_wall_clock(monkeypatch, now)
    yield now


def _orphan(
    k8s: FakeSandboxClient,
    r: redis.Redis,
    prefix: str,
    claim: str,
    created_at: datetime | None,
    **kwargs: Any,
) -> dict[str, FakeDaemonObject]:
    seeded = k8s.seed_daemon(
        DAEMON_NS, claim, port=PORT, created_at=created_at, ageless=created_at is None, **kwargs
    )
    r.set(_marker_key(prefix, claim), DAEMON_NS)
    return seeded


def test_sweep_removes_crash_before_claim_objects_strictly_older_than_the_timeout(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
) -> None:
    timeout = timedelta(seconds=config.claim_timeout_seconds)
    now = frozen_now[0]
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-old", now - timeout - timedelta(seconds=1))
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-exact", now - timeout)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-young", now - timeout / 2)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-ageless", None)

    docker_substrate.reap_orphans()

    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-old")
    for spared in ("orphan-exact", "orphan-young", "orphan-ageless"):
        assert set(fake_k8s.daemon_objects_of(spared)) == {
            "NetworkPolicy",
            "Service",
            "Pod",
            "Secret",
        }, spared
        assert redis_client.get(_marker_key(key_prefix, spared)) is not None

    # Liveness: a spared orphan goes on a later tick once it is old enough.
    frozen_now[0] = now + timeout
    docker_substrate.reap_orphans()
    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-young")
    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-exact")
    assert set(fake_k8s.daemon_objects_of("orphan-ageless")) == {
        "NetworkPolicy",
        "Service",
        "Pod",
        "Secret",
    }


def test_sweep_spares_a_live_claims_daemon(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    _orphan(fake_k8s, redis_client, key_prefix, "claim-live", old)
    fake_k8s.create_claim("claim-live", pool=POOL, deadline=time.monotonic() + 300)
    fake_k8s.claims["claim-live"].created_at = frozen_now[0]

    docker_substrate.reap_orphans()

    assert "claim-live" in fake_k8s.claims
    assert len(fake_k8s.daemon_objects_of("claim-live")) == 4
    assert not [op for op in fake_k8s.ops if op.verb == "delete"]


def test_sweep_spares_an_orphan_whose_claim_recheck_fails(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-unknown", old)
    real_get = fake_k8s.get_claim

    def unreadable(name: str, *, request_timeout_seconds: float) -> Any:
        if name == "orphan-unknown":
            raise KubeTransientError("apiserver 503")
        return real_get(name, request_timeout_seconds=request_timeout_seconds)

    monkeypatch.setattr(fake_k8s, "get_claim", unreadable)
    docker_substrate.reap_orphans()
    assert len(fake_k8s.daemon_objects_of("orphan-unknown")) == 4

    # Liveness: once the claim reads back absent, the orphan goes.
    monkeypatch.setattr(fake_k8s, "get_claim", real_get)
    docker_substrate.reap_orphans()
    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-unknown")


def test_sweep_never_deletes_foreign_or_chart_objects(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    other_release = {
        CLAIM_LABEL: "orphan-foreign",
        MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        **SELECTOR,
        "app.kubernetes.io/instance": "another-release",
    }
    fake_k8s.seed_daemon(
        DAEMON_NS, "orphan-foreign", port=PORT, created_at=old, labels=other_release
    )
    unmanaged = {CLAIM_LABEL: "orphan-unmanaged", **SELECTOR}
    fake_k8s.seed_daemon(DAEMON_NS, "orphan-unmanaged", port=PORT, created_at=old, labels=unmanaged)
    chart_policy = {MANAGED_BY_LABEL: "Helm", **SELECTOR}
    fake_k8s.seed_daemon(
        DAEMON_NS,
        "curie-docker-default-deny",
        port=PORT,
        created_at=old,
        labels=chart_policy,
        kinds=("NetworkPolicy",),
    )
    # Liveness: a genuine orphan beside them is removed.
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-ours", old)

    docker_substrate.reap_orphans()

    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-ours")
    assert len(fake_k8s.daemon_objects_of("orphan-foreign")) == 4
    assert len(fake_k8s.daemon_objects_of("orphan-unmanaged")) == 4
    assert set(fake_k8s.daemon_objects_of("curie-docker-default-deny")) == {"NetworkPolicy"}
    deleted = {op.target for op in fake_k8s.ops if op.verb == "delete"}
    assert deleted == {"orphan-ours"}


def test_a_failed_claim_inventory_deletes_no_daemon_object(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-old", old)
    real_list = fake_k8s.list_claims

    def unreadable(*, label_selector: str) -> Any:
        raise KubeTransientError("claim inventory failed")

    monkeypatch.setattr(fake_k8s, "list_claims", unreadable)
    with pytest.raises(KubeTransientError):
        docker_substrate.reap_orphans()
    assert not [op for op in fake_k8s.ops if op.verb == "delete"]
    assert len(fake_k8s.daemon_objects_of("orphan-old")) == 4

    monkeypatch.setattr(fake_k8s, "list_claims", real_list)
    docker_substrate.reap_orphans()
    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-old")


def test_a_failed_pod_delete_keeps_the_anchor_until_a_later_tick(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-stuck", old)
    fake_k8s.daemon_failures[("delete", "Pod")] = SandboxError("pod delete failed")

    docker_substrate.reap_orphans()  # best effort: retried next tick, not raised

    assert "NetworkPolicy" in fake_k8s.daemon_objects_of("orphan-stuck")
    assert redis_client.get(_marker_key(key_prefix, "orphan-stuck")) is not None

    del fake_k8s.daemon_failures[("delete", "Pod")]
    docker_substrate.reap_orphans()
    _assert_retired(fake_k8s, redis_client, key_prefix, "orphan-stuck")


def test_an_object_replaced_between_list_and_delete_survives(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-swap", old)
    fake_k8s.replace_after_list.add(("Service", "orphan-swap"))

    docker_substrate.reap_orphans()

    service = fake_k8s.daemon_objects_of("orphan-swap").get("Service")
    assert service is not None, "a replaced object is not ours to delete"
    deletes = [op for op in fake_k8s.ops if op.verb == "delete" and op.kind == "Service"]
    assert deletes and all(op.detail != service.uid for op in deletes)
    assert "Pod" not in fake_k8s.daemon_objects_of("orphan-swap")


def test_a_foreign_secret_with_the_claims_name_survives_retirement(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    config: SubstrateConfig,
    redis_client: redis.Redis,
    key_prefix: str,
    frozen_now: list[datetime],
) -> None:
    old = frozen_now[0] - timedelta(seconds=config.claim_timeout_seconds + 30)
    _orphan(fake_k8s, redis_client, key_prefix, "orphan-sec", old)
    # The Secret at the claim's name is someone else's: not owned by our Pod.
    secret = fake_k8s.daemon_objects[(DAEMON_NS, "Secret", "orphan-sec")]
    secret.manifest["metadata"].pop("ownerReferences")

    docker_substrate.reap_orphans()

    assert set(fake_k8s.daemon_objects_of("orphan-sec")) == {"Secret"}
    _assert_no_secret_deletes(fake_k8s)


# -- AC4: retained Docker routes ----------------------------------------------------------


def test_a_healthy_retained_docker_route_is_reused_after_one_bounded_ping(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
) -> None:
    first, claim = _docker_route(docker_substrate)
    pod = fake_k8s.daemon_objects_of(claim)["Pod"]
    seen = len(fake_k8s.ops)

    again = docker_substrate.claim("T-ret", workspace_repo=REPO)

    assert again == first
    later = fake_k8s.ops[seen:]
    streams = [op for op in later if op.verb == "stream"]
    assert len(streams) == 1
    assert streams[0].target == claim
    assert 0 < streams[0].detail <= PING_TIMEOUT  # type: ignore[operator]
    assert not [op for op in later if op.verb in {"create", "delete", "create_claim"}]
    # Reuse never extends the daemon's absolute bound.
    assert fake_k8s.daemon_objects_of(claim)["Pod"] is pod
    assert pod.manifest["spec"]["activeDeadlineSeconds"] == LIFETIME


def _wrong_ca(r: redis.Redis, prefix: str, claim: str) -> None:
    other = tool_daemon.issue(claim, DAEMON_NS, LIFETIME)
    ttl = r.ttl(_credential_key(prefix, claim))
    r.set(_credential_key(prefix, claim), json.dumps(dict(other.client_files)), ex=ttl)


FAULTS: dict[str, Callable[[FakeSandboxClient, redis.Redis, str, str], object]] = {
    "pod-deleted": lambda k8s, r, p, claim: _kill_pod(k8s, claim),
    "pod-failed": lambda k8s, r, p, claim: setattr(
        k8s.daemon_objects[(DAEMON_NS, "Pod", claim)], "phase", "Failed"
    ),
    "ping-refused": lambda k8s, r, p, claim: k8s.refused_pods.add(claim),
    "stream-raises": lambda k8s, r, p, claim: k8s.stream_failures.__setitem__(
        claim, KubeTransientError("port-forward failed")
    ),
    "credential-missing": lambda k8s, r, p, claim: r.delete(_credential_key(p, claim)),
    "wrong-ca": lambda k8s, r, p, claim: _wrong_ca(r, p, claim),
}


@pytest.mark.parametrize("fault", sorted(FAULTS))
def test_an_unhealthy_retained_daemon_retires_the_route_and_reclaims(
    fake_k8s: FakeSandboxClient,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    fault: str,
) -> None:
    first, claim = _docker_route(docker_substrate)
    FAULTS[fault](fake_k8s, redis_client, key_prefix, claim)

    again = docker_substrate.claim("T-ret", workspace_repo=REPO)

    assert again.claim_name != claim
    assert claim not in fake_k8s.claims
    assert fake_k8s.daemon_objects_of(claim) == {}
    assert redis_client.get(_marker_key(key_prefix, claim)) is None
    _assert_daemon_live(fake_k8s, redis_client, key_prefix, again.claim_name)


@pytest.mark.parametrize("healthy", [True, False], ids=["healthy", "daemon-gone"])
def test_adopt_pings_a_retained_docker_route(
    fake_k8s: FakeSandboxClient,
    affinity: AffinityStore,
    docker_substrate: SandboxSubstrate,
    redis_client: redis.Redis,
    key_prefix: str,
    healthy: bool,
) -> None:
    first, claim = _docker_route(docker_substrate)
    if not healthy:
        _kill_pod(fake_k8s, claim)
    seen = len(fake_k8s.ops)

    adopted = docker_substrate.adopt("T-ret")

    if healthy:
        assert len([op for op in fake_k8s.ops[seen:] if op.verb == "stream"]) == 1
        assert adopted == first
        _assert_daemon_live(fake_k8s, redis_client, key_prefix, claim)
    else:
        assert adopted is None
        assert affinity.get("T-ret") is None
        assert claim not in fake_k8s.claims
        assert fake_k8s.daemon_objects_of(claim) == {}


def test_a_non_opted_retained_route_is_reused_with_no_daemon_call(
    fake_k8s: FakeSandboxClient, docker_substrate: SandboxSubstrate
) -> None:
    first = docker_substrate.claim("T-web", workspace_repo=WEB)
    again = docker_substrate.claim("T-web", workspace_repo=WEB)
    assert again == first
    assert docker_substrate.adopt("T-web") == first
    assert fake_k8s.daemon_calls == []
    stored = fake_k8s.claims[first.claim_name]
    assert stored.tool_daemon is None
    assert not any(key.startswith("DOCKER_") for key in stored.env)

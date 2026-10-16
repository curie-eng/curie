#!/usr/bin/env python3
"""Chart to worker startup, live admission and certificate-authenticated Docker proof."""

from __future__ import annotations

import argparse
import copy
import functools
import importlib.util
import json
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

type Json = dict[str, Any]
spec = importlib.util.spec_from_file_location(
    "docker_daemon_fixture", Path(__file__).with_name("docker-daemon-fixture.py")
)
assert spec and spec.loader
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
require: Callable[[object, str], None] = helper.require
run: Callable[..., subprocess.CompletedProcess[str]] = helper.run
object_path: Callable[[Json], str] = helper.object_path
# Cluster-scoped kinds this driver may create; everything else lands in a namespace.
CLUSTER = {"ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"}
# Kinds whose plural the fixture's object_path derives correctly. Anything else
# (PriorityClass, ClusterRole from an enabled controller) is refused before a create.
KINDS = CLUSTER | {
    "ConfigMap",
    "Deployment",
    "LimitRange",
    "NetworkPolicy",
    "ResourceQuota",
    "Role",
    "RoleBinding",
    "SandboxTemplate",
    "SandboxWarmPool",
    "Secret",
    "ServiceAccount",
}
TEMPLATES = (
    "worker",
    "agent-sandbox",
    "docker-daemon",
    "runner-resources-admission",
    "runner-token",
    "security-networkpolicy",
)
ERRORS = (
    RuntimeError,
    OSError,
    ValueError,
    KeyError,
    IndexError,
    TypeError,
    StopIteration,
    sqlite3.Error,
    subprocess.SubprocessError,
)


class FixtureAPI(Protocol):
    args: argparse.Namespace
    work: Path
    snapshot: Path
    candidate: str
    node: str

    def setup(self) -> FixtureAPI: ...
    def images(self) -> dict[str, Json]: ...
    def get(self, path: str) -> Json: ...
    def k(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]: ...
    def create(self, obj: Json, *, identity: str | None = None, race: bool = False) -> Json: ...
    def note(self, key: str, value: object) -> None: ...
    def wait[T](self, predicate: Callable[[], T], seconds: float = 300) -> T: ...
    def meta(self) -> Json: ...
    def fault_point(self, phase: str) -> None: ...
    def cleanup(self) -> None: ...


def helm(f: FixtureAPI, values: Json, name: str, templates: Sequence[str]) -> str:
    value_file = f.work / (name + ".values.yaml")
    value_file.write_text(yaml.safe_dump(values))
    argv: list[str | Path] = ["helm", "template", f.args.release, f.snapshot / "charts/curie"]
    argv += ["--namespace", f.args.namespace, "-f", value_file]
    argv += ["--kube-version", f.get("/version")["gitVersion"]]
    for template in templates:
        argv += ["--show-only", "templates/" + template + ".yaml"]
    return run(argv).stdout


def render(f: FixtureAPI, images: Json) -> tuple[list[Json], Json]:
    daemon, platform, runner = images["daemon"], images["platform"], images["runner"]
    # Non-default values throughout, so the handoff proves overrides reach the worker.
    config = {
        "namespace": f.args.daemon_namespace,
        "image": daemon["ref"],
        "port": 12376,
        "selectorLabels": {
            "app.kubernetes.io/name": "curie",
            "app.kubernetes.io/instance": f.args.release,
            "app.kubernetes.io/component": "docker-daemon",
        },
        "resources": {
            "requests": {"cpu": "600m", "memory": "768Mi", "ephemeral-storage": "2Gi"},
            "limits": {"cpu": "1500m", "memory": "1536Mi", "ephemeral-storage": "4Gi"},
        },
        "storageSizeLimit": "3Gi",
        "maxClaimLifetimeSeconds": 900,
        "pingTimeoutSeconds": 5,
    }
    arch = f.meta()["node_arch"]
    other = "arm64" if arch == "amd64" else "amd64"
    repo_runner = {
        "image": runner["ref"],
        "base": platform["digest"],
        # The images are single-platform for the node. The chart only checks the
        # sha256 shape of the other platform's entry, so it carries the config digest.
        "platformDigests": {
            "linux/" + arch: runner["manifest"],
            "linux/" + other: runner["config"],
        },
        "tools": ["docker-cli"],
    }
    docker = {k: v for k, v in config.items() if k not in {"selectorLabels", "image"}}
    docker |= {"daemonImage": daemon["ref"], "port": str(config["port"]), "createNamespace": False}
    values: Json = {
        "fullnameOverride": "fd-" + f.args.nonce,
        "security": {"allowDevDefaults": True, "gvisor": {"mode": "off"}},
        "worker": {"replicas": 0, "attachments": {"enabled": False}},
        "priorityClasses": {
            "platform": {"create": False, "name": ""},
            "sandbox": {"create": False, "name": ""},
        },
        # The tenant quota is scoped to a sandbox PriorityClass, disabled above.
        "resourceQuota": {"enabled": False},
        "agentSandbox": {
            "controller": {"deploy": False},
            "warmPool": {"replicas": 0},
            "runner": {
                "image": platform["ref"].split("@")[0],
                "digest": platform["digest"],
                "imagePullPolicy": "IfNotPresent",
                "fakeModel": True,
                "bundleFetch": {"enabled": False},
                "workspace": {"enabled": False},
                "attachments": {"enabled": False},
            },
        },
        "factory": {
            "sandboxEgress": {"factory": True},
            "docker": docker,
            "bindings": [
                {
                    "name": "acme",
                    "repos": [
                        {
                            "path": "acme-corp/api",
                            "runner": repo_runner,
                            "docker": {"enabled": True},
                        },
                        {"path": "acme-corp/ui", "runner": repo_runner},
                    ],
                }
            ],
        },
    }
    # Runner admission must not move when a repository turns Docker on.
    without = copy.deepcopy(values)
    without["factory"]["bindings"][0]["repos"][0].pop("docker")
    admission_only = ["runner-resources-admission"]
    require(
        helm(f, values, "docker", admission_only) == helm(f, without, "plain", admission_only),
        "runner_admission_changed",
    )
    f.note("runner_admission_identical", True)
    docs: list[Json] = [d for d in yaml.safe_load_all(helm(f, values, "full", TEMPLATES)) if d]
    for doc in docs:
        require(doc["kind"] in KINDS, "unexpected_rendered_kind")
        if doc["kind"] not in CLUSTER:
            doc["metadata"].setdefault("namespace", f.args.namespace)
    for doc in sorted(docs, key=lambda d: d["kind"] == "ValidatingAdmissionPolicyBinding"):
        f.create(doc)
    return docs, config


STARTUP = """import json, sys
from pathlib import Path
from jsonschema import Draft202012Validator
from curie_worker.run import _substrate_config
env = json.load(sys.stdin)
raw = env['CURIE_REPO_RUNNER_POOLS']
if sys.argv[1] == 'valid':
    schema = Path('apps/worker/schema/repo-runner-pools.schema.json').read_text()
    Draft202012Validator(json.loads(schema)).validate(json.loads(raw))
    daemon = _substrate_config(env).repo_runner_pools['acme-corp/api'].daemon
    print(json.dumps(daemon.model_dump(mode='json', by_alias=True)))
else:
    try:
        _substrate_config(env)
    except ValueError as exc:
        assert 'HOSTILE_RUNTIME_MARKER' not in str(exc)
        assert 'CURIE_REPO_RUNNER_POOLS' in str(exc)
        print('refused')
    else:
        raise AssertionError('missing_daemon_accepted')
"""


def startup(f: FixtureAPI, docs: list[Json], expected: Json) -> Json:
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    live = f.get(object_path(deployment))
    require(live["spec"]["replicas"] == 0, "worker_must_remain_stopped")
    env = {
        v["name"]: v["value"]
        for v in live["spec"]["template"]["spec"]["containers"][0]["env"]
        if "value" in v
    }
    raw = env["CURIE_REPO_RUNNER_POOLS"]
    command = ["uv", "run", "--project", str(f.snapshot), "--frozen", "python3", "-c", STARTUP]
    parsed: Json = json.loads(run([*command, "valid"], cwd=f.snapshot, data=json.dumps(env)).stdout)
    require(parsed == expected, "startup_config_mismatch")
    pools = json.loads(raw)
    require("daemon" not in pools["acme-corp/ui"], "non_docker_daemon_present")
    missing = json.loads(raw)
    missing["acme-corp/api"].pop("daemon")
    hostile = json.loads(raw)
    hostile["acme-corp/api"]["daemon"]["storageSizeLimit"] = "HOSTILE_RUNTIME_MARKER"
    for probe in (missing, hostile):
        env["CURIE_REPO_RUNNER_POOLS"] = json.dumps(probe)
        negative = run([*command, "invalid"], cwd=f.snapshot, data=json.dumps(env), check=False)
        require(
            negative.returncode == 0
            and "HOSTILE_RUNTIME_MARKER" not in negative.stdout + negative.stderr
            and negative.stdout.strip() == "refused",
            "startup_refusal_or_redaction_failed",
        )
    f.note("startup_and_schema", True)
    f.note("local_worker_lifecycle_executed", False)
    return parsed


def certificates(f: FixtureAPI, dns: str) -> tuple[dict[str, str], dict[str, str]]:
    def issue(
        name: str,
        issuer: tuple[rsa.RSAPrivateKey, x509.Certificate] | None = None,
        server: bool = False,
    ) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        now = datetime.now(UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer[1].subject if issuer else subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1))
            .add_extension(
                x509.BasicConstraints(ca=issuer is None, path_length=None), critical=True
            )
        )
        if issuer:
            usage = ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH
            cert = cert.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        if server:
            cert = cert.add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName(dns), x509.DNSName(dns + ".cluster.local")]
                ),
                critical=False,
            )
        return key, cert.sign(issuer[0] if issuer else key, hashes.SHA256())

    ca = issue("fixture-ca")
    server, client = issue(dns, ca, True), issue("fixture-client", ca)
    foreign = issue("fixture-client", issue("foreign-ca"))

    def material(pair: tuple[rsa.RSAPrivateKey, x509.Certificate], stem: str) -> dict[str, str]:
        key = pair[0].private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        return {
            stem + "-cert.pem": pair[1].public_bytes(serialization.Encoding.PEM).decode(),
            stem + "-key.pem": key.decode(),
        }

    public = {"ca.pem": ca[1].public_bytes(serialization.Encoding.PEM).decode()}
    server_files = public | material(server, "server")
    client_files = public | material(client, "client") | material(foreign, "foreign")
    cert_dir = f.work / "certs"
    cert_dir.mkdir(mode=0o700)
    for name, value in (server_files | client_files).items():
        path = cert_dir / name
        path.touch(mode=0o600)
        path.write_text(value)
    return server_files, client_files


def admission(f: FixtureAPI, pod: Json, policy: Json, identity: str) -> None:
    name = policy["metadata"]["name"]
    validations = policy["spec"]["validations"]
    args_message = next(v["message"] for v in validations if "c.args" in v["expression"])
    env_message = next(v["message"] for v in validations if "c.env" in v["expression"])

    def attempt(obj: Json) -> subprocess.CompletedProcess[str]:
        return f.k(
            *("--as", identity, "create", "--dry-run=server", "-f", "-", "-o", "json"),
            data=json.dumps(obj),
            check=False,
        )

    def denied(result: subprocess.CompletedProcess[str], message: str) -> bool:
        return bool(
            result.returncode
            and f"ValidatingAdmissionPolicy '{name}'" in result.stderr
            and "denied request" in result.stderr
            and message in result.stderr
        )

    violating = copy.deepcopy(pod)
    violating["spec"]["containers"][0]["env"] = [{"name": "PATH", "value": "/certs"}]
    f.wait(lambda: attempt(pod).returncode == 0 and denied(attempt(violating), env_message), 60)
    args = pod["spec"]["containers"][0]["args"]
    cases: list[tuple[str, Sequence[str]]] = [
        (f"missing-{i}", args[:i] + args[i + 1 :]) for i in range(1, 8)
    ]
    cases += [
        ("plaintext", [*args[:4], *args[5:]]),
        ("wrong-port", [*args[:3], "--host=tcp://0.0.0.0:2375", *args[4:]]),
        ("split-cert", [*args, "--tlscert", "/certs/other"]),
    ]
    cases += [
        ("override-" + flag, [*args, flag])
        for flag in (
            "--tls=false",
            "--host=tcp://0.0.0.0:2375",
            "--tlscacert=/certs/other",
            "--tlscert=/certs/other",
            "--tlskey=/certs/other",
            "--config-file=/certs/config",
            "--debug",
            "--label",
        )
    ]
    for index, (category, changed) in enumerate(cases):
        obj = copy.deepcopy(pod)
        obj["metadata"]["name"] += "-negative-" + str(index)
        obj["spec"]["containers"][0]["args"] = changed
        require(denied(attempt(obj), args_message), "admission_wrong_denial:" + category)
        require(not f.get(object_path(obj)), "negative_pod_persisted")
    require(
        denied(attempt(violating), env_message) and attempt(pod).returncode == 0,
        "admission_neighbor_failed",
    )
    f.note("admission_negatives", len(cases) + 1)


SERVER = """from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')
    def log_message(self, *args):
        pass
HTTPServer(('0.0.0.0', 8080), H).serve_forever()
"""
PING = """import http.client, json, ssl, subprocess, sys
host, port, timeout = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
tls = ['--tlsverify', '--tlscacert=/test-certs/ca.pem']
tls += ['--tlscert=/test-certs/client-cert.pem', '--tlskey=/test-certs/client-key.pem']
def ping(cert=None, plain=False):
    ctx = ssl.create_default_context(cafile='/test-certs/ca.pem')
    if cert:
        ctx.load_cert_chain(f'/test-certs/{cert}-cert.pem', f'/test-certs/{cert}-key.pem')
    if plain:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
    else:
        conn = http.client.HTTPSConnection(host, port, context=ctx, timeout=timeout)
    try:
        conn.request('GET', '/_ping')
        response = conn.getresponse()
        return response.status == 200 and response.read() == b'OK'
    except (ssl.SSLError, ConnectionResetError, BrokenPipeError,
            http.client.RemoteDisconnected):
        return False
    finally:
        conn.close()
assert ping('client')
assert not ping() and not ping('foreign') and not ping(plain=True)
assert ping('client')
docker = ['docker', f'--host=tcp://{host}:{port}', *tls, 'info', '--format', '{{json .}}']
result = subprocess.run(docker, capture_output=True, text=True, timeout=timeout)
assert result.returncode == 0
info = json.loads(result.stdout)
assert 'curie-authz' in info['Plugins']['Authorization']
assert any('userns' in s for s in info['SecurityOptions'])
print(json.dumps({'mtls': True, 'negative_tls': True, 'docker_cli': True}))
"""


def observed(f: FixtureAPI, path: str) -> bool:
    policy = f.get(path)
    return bool(
        policy.get("status", {}).get("observedGeneration", 0) >= policy["metadata"]["generation"]
    )


def ready_pod(f: FixtureAPI, path: str) -> Json:
    pod = f.get(path)
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return pod if any(c.get("ready") and c.get("imageID") for c in statuses) else {}


def live(f: FixtureAPI, docs: list[Json], config: Json, images: Json) -> None:
    daemon, runner = images["daemon"], images["runner"]
    namespace, daemon_ns, claim = f.args.namespace, config["namespace"], "fd-" + f.args.nonce
    identity = f"system:serviceaccount:{namespace}:fd-{f.args.nonce}-worker"
    labels = config["selectorLabels"] | {
        "curietech.ai/sandbox-claim": claim,
        "curietech.ai/managed-by": "curie-sandbox-substrate",
        "curietech.ai/docker-egress": "public",
    }

    def obj(kind: str, ns: str, data: Json, api: str = "v1", name: str = claim) -> Json:
        meta = {"name": name, "namespace": ns, "labels": labels.copy()}
        return {"apiVersion": api, "kind": kind, "metadata": meta, **data}

    dns = f"{claim}.{daemon_ns}.svc"
    server_files, client_files = certificates(f, dns)
    secret = {"type": "Opaque", "stringData": server_files}
    f.create(obj("Secret", daemon_ns, secret), identity=identity)
    f.create(obj("Secret", namespace, {"type": "Opaque", "stringData": client_files}))
    service = {
        "clusterIP": "None",
        "selector": {"curietech.ai/sandbox-claim": claim},
        "ports": [{"port": config["port"], "targetPort": config["port"]}],
    }
    f.create(obj("Service", daemon_ns, {"spec": service}), identity=identity)
    runner_labels = {
        "app.kubernetes.io/component": "runner-sandbox",
        "app.kubernetes.io/instance": f.args.release,
        "fixture-claim": claim,
    }
    peer = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
        "podSelector": {"matchLabels": runner_labels},
    }
    ingress = {
        "podSelector": {"matchLabels": {"curietech.ai/sandbox-claim": claim}},
        "policyTypes": ["Ingress"],
        "ingress": [{"from": [peer], "ports": [{"protocol": "TCP", "port": config["port"]}]}],
    }
    policy_api = "networking.k8s.io/v1"
    f.create(obj("NetworkPolicy", daemon_ns, {"spec": ingress}, policy_api), identity=identity)
    container = {
        "name": "dockerd",
        "image": config["image"],
        "imagePullPolicy": "IfNotPresent",
        "securityContext": {"privileged": True},
        "args": [
            "dockerd",
            "--userns-remap=default",
            "--authorization-plugin=curie-authz",
            f"--host=tcp://0.0.0.0:{config['port']}",
            "--tlsverify",
            "--tlscacert=/certs/ca.pem",
            "--tlscert=/certs/server-cert.pem",
            "--tlskey=/certs/server-key.pem",
        ],
        "resources": config["resources"],
        "volumeMounts": [
            {"name": "data", "mountPath": "/var/lib/docker"},
            {"name": "certs", "mountPath": "/certs", "readOnly": True},
        ],
    }
    pod_spec = {
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "nodeName": f.node,
        "activeDeadlineSeconds": config["maxClaimLifetimeSeconds"],
        "containers": [container],
        "volumes": [
            {"name": "data", "emptyDir": {"sizeLimit": config["storageSizeLimit"]}},
            {"name": "certs", "secret": {"secretName": claim}},
        ],
    }
    pod = obj("Pod", daemon_ns, {"spec": pod_spec})
    policies = [d for d in docs if d["kind"] == "ValidatingAdmissionPolicy"]
    for policy in policies:
        path = object_path(policy)
        f.wait(functools.partial(observed, f, path))
        # Other expressions already carry type-checker warnings for multi-kind and
        # quantity fields; they still enforce. Only the TLS argv rule is this change's.
        checks = f.get(path).get("status", {}).get("typeChecking", {})
        argv = {
            f"spec.validations[{i}].expression"
            for i, rule in enumerate(policy["spec"]["validations"])
            if "c.args ==" in rule["expression"]
        }
        warned = {w["fieldRef"] for w in checks.get("expressionWarnings", [])}
        require(not argv & warned, "admission_type_warnings")
    pods_policy = next(p for p in policies if p["metadata"]["name"].endswith("-docker-daemon-pods"))
    admission(f, pod, pods_policy, identity)
    created = f.create(pod, identity=identity)
    f.fault_point("admitted")
    template = copy.deepcopy(
        next(
            d
            for d in docs
            if d["kind"] == "SandboxTemplate"
            and d["spec"]["podTemplate"]["spec"]["containers"][0]["image"] == runner["ref"]
        )
    )
    template["metadata"] = {
        "name": claim + "-resources",
        "namespace": namespace,
        "labels": labels.copy(),
    }
    template["spec"]["networkPolicyManagement"] = "Unmanaged"
    template["spec"]["podTemplate"].setdefault("metadata", {}).setdefault("labels", {}).update(
        runner_labels
    )
    ps = template["spec"]["podTemplate"]["spec"]
    ps["nodeName"] = f.node
    ps.setdefault("volumes", []).append({"name": "test-certs", "secret": {"secretName": claim}})
    runner_container = ps["containers"][0]
    runner_container.setdefault("volumeMounts", []).append(
        {"name": "test-certs", "mountPath": "/test-certs", "readOnly": True}
    )
    runner_container["command"] = ["python3", "-c", SERVER]
    f.create(template, identity=identity)
    extensions = "extensions.agents.x-k8s.io/v1beta1"
    pool = {"spec": {"replicas": 0, "sandboxTemplateRef": {"name": claim + "-resources"}}}
    pool_name = claim + "-resources-pool"
    f.create(obj("SandboxWarmPool", namespace, pool, extensions, pool_name), identity=identity)
    claim_spec = {"spec": {"warmPoolRef": {"name": pool_name}}}
    bound = f.create(obj("SandboxClaim", namespace, claim_spec, extensions), identity=identity)

    def ready_claim() -> str:
        status = f.get(object_path(bound)).get("status", {})
        ready = any(
            c["type"] == "Ready" and c["status"] == "True" for c in status.get("conditions", [])
        )
        return str(status.get("sandbox", {}).get("name", "")) if ready else ""

    runner_name = f.wait(ready_claim)
    for name, ns, image, expected in (
        (claim, daemon_ns, daemon, "dockerd"),
        (runner_name, namespace, runner, "runner"),
    ):
        current = f.wait(functools.partial(ready_pod, f, f"/api/v1/namespaces/{ns}/pods/{name}"))
        actual = next(c for c in current["spec"]["containers"] if c["name"] == expected)
        status = next(c for c in current["status"]["containerStatuses"] if c["name"] == expected)
        require(
            actual["image"] == image["ref"]
            and actual["imagePullPolicy"] == "IfNotPresent"
            and status["imageID"].split("@")[-1] in {image["manifest"], image["config"]},
            "live_image_identity_mismatch",
        )
        if ns == daemon_ns:
            spec_ = current["spec"]
            require(
                actual["resources"] == config["resources"]
                and spec_["activeDeadlineSeconds"] == config["maxClaimLifetimeSeconds"]
                and spec_["volumes"][0]["emptyDir"]["sizeLimit"] == config["storageSizeLimit"]
                and all(
                    current["metadata"]["labels"].get(k) == v
                    for k, v in config["selectorLabels"].items()
                ),
                "live_daemon_config_mismatch",
            )
    outcome = f.k(
        *("-n", namespace, "exec", runner_name, "-c", "runner", "--", "python3", "-c", PING),
        *(dns, str(config["port"]), str(config["pingTimeoutSeconds"])),
    )
    f.note("runtime", dict(json.loads(outcome.stdout)))
    require(created["metadata"]["uid"], "daemon_uid_missing")


def execute(args: argparse.Namespace) -> int:
    f: FixtureAPI = helper.Fixture(args)
    result, category = 1, ""
    try:
        f.setup()
        images = f.images()
        docs, expected = render(f, images)
        live(f, docs, startup(f, docs, expected), images)
        result = 0
    except SystemExit as exc:
        result = exc.code if isinstance(exc.code, int) and exc.code else 1
        category = "signal" if result > 128 else "exit"
    except RuntimeError as exc:
        category = str(exc)
        result = 42 if category == "intentional_failure" else 1
    except ERRORS as exc:
        category = type(exc).__name__
    try:
        f.cleanup()
    except RuntimeError as exc:
        result, category = 1, str(exc)
    meta = f.meta()
    summary = {
        "candidate": f.candidate,
        "success": result == 0,
        "cleanup": meta.get("cleanup_complete", False),
        "checks": {
            key: meta.get(key)
            for key in (
                "runner_admission_identical",
                "startup_and_schema",
                "admission_negatives",
                "runtime",
            )
        },
    }
    print(json.dumps(summary, sort_keys=True))
    if result:
        print(json.dumps({"error": category or "failure"}), file=sys.stderr)
    return result


def main() -> int:
    def extra(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--check-failure-cleanup", action="store_true")

    try:
        args = helper.arguments(extra)
        require(args.slice == "chart", "unsafe_intake")
        if not args.check_failure_cleanup:
            return execute(args)

        def child_argv(n: str) -> list[str]:
            return [sys.executable, __file__, "--context", args.context, "--candidate", "HEAD"] + [
                *("--namespace", f"fd-chart-{n}", "--release", f"fd-chart-{n}"),
                *("--daemon-namespace", f"fd-chart-d-{n}"),
                *("--work-dir", f".projects/daemon-chart-{n}"),
            ]

        return int(helper.check_failure_cleanup(args, child_argv))
    except SystemExit as exc:
        if not (isinstance(exc.code, int) and exc.code > 128):
            raise
        code, category = exc.code, "signal"
    except ERRORS as exc:
        code = 1
        category = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
    print(json.dumps({"error": category}), file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())

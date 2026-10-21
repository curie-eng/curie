"""Live proof of the ADR 0217 tool daemon lifecycle through the production substrate.

Gated on ``CURIE_SANDBOX_E2E=1``. Once gated on, every CLI option and a
reachable Valkey are required: a missing option or an unreachable store fails.
The merged namespace fixture (charts/curie/ci/runtime/docker-daemon-fixture.py)
provides the release and daemon namespaces, the agent-sandbox substrate and the
pinned images; the chart is rendered and applied as the chart runtime harness
does, with the worker stopped. The test then drives the production
``SandboxSubstrate`` and ``KubernetesSandboxClient`` as the chart's worker
ServiceAccount, so the chart's RBAC and admission policies apply to every
create, against a real Valkey:

1. a Docker-enabled claim whose runner runs ``docker run`` and reports the same
   daemon ID as the daemon Pod's ``/info`` read over the API port-forward,
2. an independently issued client certificate is refused, the claim's accepted,
3. deleting the retained daemon forces a fresh working claim,
4. release and the orphan sweep leave no labelled objects behind.

Evidence is object metadata, booleans and public image identities only.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import functools
import importlib.util
import json
import os
import secrets
import signal
import sys
import time
import uuid
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import quote

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_SANDBOX_E2E") != "1",
    reason="live tool daemon e2e; set CURIE_SANDBOX_E2E=1 with the fixture options",
)

ROOT = Path(__file__).resolve().parents[4]
RUNTIME = ROOT / "charts/curie/ci/runtime"
OPTIONS = ("context", "namespace", "daemon-namespace", "release", "candidate", "work-dir")
REPO = "acme-corp/api"
# The busybox pin charts/curie/values.yaml already carries; the daemon pulls it
# through its public egress class.
PROBE_IMAGE = (
    "busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
)
CLAIM_LABEL = "curietech.ai/sandbox-claim"
EGRESS_LABEL = "curietech.ai/docker-egress"
MANAGED = "curietech.ai/managed-by=curie-sandbox-substrate"
KINDS = {
    "pods": "/api/v1",
    "services": "/api/v1",
    "networkpolicies": "/apis/networking.k8s.io/v1",
    "secrets": "/api/v1",
}
CLAIMS = "/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{ns}/sandboxclaims/{name}"
SIGNALS = (signal.SIGINT, signal.SIGTERM)


def _load(name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, RUNTIME / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _labelled(f: Any, namespace: str, selector: str) -> list[str]:
    """Live (not terminating) objects matching selector, as kind/name."""

    found = []
    for plural, prefix in KINDS.items():
        path = f"{prefix}/namespaces/{namespace}/{plural}?labelSelector={quote(selector)}"
        for item in f.get(path).get("items", []):
            found.append(f"{plural}/{item['metadata']['name']}")
    return found


def _wait_clear(f: Any, namespace: str, selector: str, seconds: float = 240) -> None:
    f.wait(lambda: not _labelled(f, namespace, selector), seconds)


def _claim_selector(claim: str) -> str:
    return f"{CLAIM_LABEL}={claim},{MANAGED}"


def _write_kubeconfig(f: Any, path: Path) -> None:
    """A kubeconfig for the chart's worker ServiceAccount, 0600, never printed."""

    view = json.loads(f.k("config", "view", "--minify", "--flatten", "-o", "json").stdout)
    cluster = view["clusters"][0]["cluster"]
    account = f"fd-{f.args.nonce}-worker"
    token = f.k("-n", f.args.namespace, "create", "token", account, "--duration=1h").stdout
    config = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{"name": "fixture", "cluster": cluster}],
        "users": [{"name": "worker", "user": {"token": token.strip()}}],
        "contexts": [
            {
                "name": "worker",
                "context": {
                    "cluster": "fixture",
                    "user": "worker",
                    "namespace": f.args.namespace,
                },
            }
        ],
        "current-context": "worker",
    }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(config, handle)


def _daemon_pod(f: Any, daemon_ns: str, claim: str) -> dict[str, Any]:
    path = f"/api/v1/namespaces/{daemon_ns}/pods?labelSelector={quote(_claim_selector(claim))}"
    pods = [p for p in f.get(path)["items"] if not p["metadata"].get("deletionTimestamp")]
    assert len(pods) == 1, f"expected one daemon pod for {claim}, found {len(pods)}"
    return dict(pods[0])


def _runner_docker(f: Any, namespace: str, sandbox: str) -> tuple[str, str]:
    """Run a container through the runner's own Docker env; return (ID, Name)."""

    script = (
        f"docker run --rm {PROBE_IMAGE} echo ran && "
        "docker info --format '{{.ID}} {{.Name}}'"
    )
    result = f.k(
        *("-n", namespace, "exec", sandbox, "-c", "runner", "--", "sh", "-c", script),
        check=False,
        timeout=600,
    )
    lines = result.stdout.strip().splitlines()
    assert result.returncode == 0 and "ran" in lines, (
        f"docker from runner failed rc={result.returncode}: {result.stderr.strip()[-300:]}"
    )
    daemon_id, name = lines[-1].split()
    return daemon_id, name


def _credential(store: Any, prefix: str, claim: str) -> dict[str, str]:
    raw = store.get(f"{prefix}:daemon-tls:{claim}")
    assert raw, f"no daemon credential in Valkey for {claim}"
    files = json.loads(raw)
    assert set(files) == {"ca.pem", "cert.pem", "key.pem"}
    return dict(files)


def _prove_docker(ctx: dict[str, Any], handle: Any) -> dict[str, Any]:
    """docker run works in the runner and reports the daemon Pod's own ID."""

    f, td, daemon = ctx["f"], ctx["td"], ctx["daemon"]
    pod = _daemon_pod(f, daemon.namespace, handle.claim_name)
    pod_name = pod["metadata"]["name"]
    runner_id, runner_name = _runner_docker(f, handle.namespace, handle.sandbox_name)
    files = _credential(ctx["store"], ctx["prefix"], handle.claim_name)
    host = f"{handle.claim_name}.{daemon.namespace}.svc"
    status, body = td.request(
        ctx["client"], daemon.namespace, pod_name, daemon.port, host, files, "/info", 30.0
    )
    assert status == 200, f"/info answered {status}"
    info = json.loads(body)
    assert info["ID"] == runner_id and info["Name"] == runner_name == pod_name
    evidence = {"claim": handle.claim_name, "daemon_pod": pod_name, "daemon_id": runner_id}
    print(f"EVIDENCE docker_run_same_daemon={json.dumps(evidence, sort_keys=True)}")
    return pod


def _orphans(ctx: dict[str, Any], egress: str) -> None:
    """Crash-before-claim objects: the sweep takes only the aged one, then the rest."""

    f, td, daemon, config = ctx["f"], ctx["td"], ctx["daemon"], ctx["config"]
    reaper = ctx["substrate_type"](
        ctx["client"], ctx["affinity"], dataclasses.replace(config, claim_timeout_seconds=5.0)
    )

    def provision(claim: str) -> None:
        td.provision(
            ctx["client"],
            daemon,
            claim=claim,
            release_namespace=config.namespace,
            egress_class=egress,
            deadline=time.monotonic() + 60,
        )
        assert len(_labelled(f, daemon.namespace, _claim_selector(claim))) >= 3

    nonce = f.args.nonce
    aged = config.claim_name_for(f"orphan-aged-{nonce}", uuid.uuid4().hex[:6])
    young = config.claim_name_for(f"orphan-young-{nonce}", uuid.uuid4().hex[:6])
    provision(aged)
    time.sleep(7)
    provision(young)
    young_at = time.monotonic()
    reaper.reap_orphans()
    kept = _labelled(f, daemon.namespace, _claim_selector(young))
    assert {k.split("/")[0] for k in kept} >= {"pods", "services", "networkpolicies"}
    _wait_clear(f, daemon.namespace, _claim_selector(aged))
    print(f"EVIDENCE orphan_sweep aged_removed=true young_kept=true kinds={sorted(kept)}")
    time.sleep(max(0.0, 7 - (time.monotonic() - young_at)))
    reaper.reap_orphans()
    _wait_clear(f, daemon.namespace, _claim_selector(young))
    print("EVIDENCE orphan_sweep later_reap_removed_rest=true")


def _scenario(fx: ModuleType, args: argparse.Namespace) -> None:
    # Imported here: collection must not need the implementation when gated off.
    import redis
    from curie_worker.binding import RUNNER_TOKEN_ENV
    from curie_worker.run import _substrate_config
    from curie_worker.sandbox import (
        AffinityStore,
        KubernetesSandboxClient,
        SandboxSubstrate,
        tool_daemon,
    )
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.retry import Retry as AsyncRetry
    from redis.backoff import NoBackoff
    from redis.maint_notifications import MaintNotificationsConfig

    runtime = _load("docker_daemon_config_runtime", "docker-daemon-config-runtime.py")
    connection: dict[str, Any] = {
        "host": os.environ.get("TEST_VALKEY_HOST", "localhost"),
        "port": int(os.environ.get("TEST_VALKEY_PORT", "26379")),
        "password": os.environ.get("TEST_VALKEY_PW", "valkeypass") or None,
    }
    prefix = f"e2e:tool-daemon:{args.nonce}"
    # Teardown is armed before the first create: Fixture registers its own
    # cleanup, the kubeconfig path and Valkey prefix are fixed up front.
    f = fx.Fixture(args)
    kubeconfig = f.work / "worker.kubeconfig"
    store = redis.Redis(**connection)
    pressure = AsyncRedis(
        **connection,
        socket_timeout=1.0,
        socket_connect_timeout=1.0,
        retry=AsyncRetry(NoBackoff(), 0),
        driver_info=None,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    try:
        store.ping()  # unreachable Valkey fails the run, never skips it
        f.setup()
        images = f.images()
        docs, _ = runtime.render(f, images)
        for policy in (d for d in docs if d["kind"] == "ValidatingAdmissionPolicy"):
            f.wait(functools.partial(runtime.observed, f, fx.object_path(policy)))
        deployment = next(d for d in docs if d["kind"] == "Deployment")
        live = f.get(fx.object_path(deployment))
        assert live["spec"]["replicas"] == 0, "the chart worker must stay stopped"
        env = {
            v["name"]: v["value"]
            for v in live["spec"]["template"]["spec"]["containers"][0]["env"]
            if "value" in v
        }
        env["CURIE_CLAIM_TIMEOUT_SECONDS"] = "240"  # cold runner plus dockerd on one node
        config = dataclasses.replace(_substrate_config(env), key_prefix=prefix)
        daemon = config.repo_runner_pools[REPO].daemon
        assert daemon is not None and daemon.namespace == args.daemon_namespace
        assert config.namespace == args.namespace
        _write_kubeconfig(f, kubeconfig)
        client = KubernetesSandboxClient(config.namespace, kubeconfig=str(kubeconfig))
        affinity = AffinityStore(store, pressure_client=pressure, key_prefix=prefix)
        substrate = SandboxSubstrate(client, affinity, config)
        ctx: dict[str, Any] = {
            "f": f,
            "td": tool_daemon,
            "daemon": daemon,
            "config": config,
            "client": client,
            "affinity": affinity,
            "store": store,
            "prefix": prefix,
            "substrate_type": SandboxSubstrate,
        }
        thread = f"e2e-tool-daemon-{args.nonce}"

        def claim() -> Any:
            token = {RUNNER_TOKEN_ENV: secrets.token_urlsafe(32)}
            return substrate.claim(thread, env=token, workspace_repo=REPO)

        # 1. A Docker-enabled claim whose runner reaches its own daemon.
        first = claim()
        first_pod = _prove_docker(ctx, first)
        egress = first_pod["metadata"]["labels"][EGRESS_LABEL]
        f.note("first_claim", {"claim": first.claim_name, "egress": egress})
        f.fault_point("docker-claimed")

        # 2. A client certificate from another claim's CA is refused.
        pod_name = first_pod["metadata"]["name"]
        host = f"{first.claim_name}.{daemon.namespace}.svc"
        real = _credential(store, prefix, first.claim_name)
        stranger_name = config.claim_name_for(f"stranger-{args.nonce}", uuid.uuid4().hex[:6])
        stranger = tool_daemon.issue(stranger_name, daemon.namespace, 600).client_files
        mixed = {"ca.pem": real["ca.pem"], "cert.pem": stranger["cert.pem"]}
        mixed["key.pem"] = stranger["key.pem"]
        ping = functools.partial(
            tool_daemon.ping, client, daemon.namespace, pod_name, daemon.port, host
        )
        assert not ping(mixed, 10.0), "an independent client certificate was accepted"
        assert ping(real, 10.0), "the claim's own client certificate was refused"
        print("EVIDENCE independent_client_refused=true claim_client_accepted=true")

        # 3. Deleting the retained daemon forces a fresh working claim.
        f.k("-n", daemon.namespace, "delete", "pod", pod_name, "--wait=false")
        f.wait(lambda: not f.get(f"/api/v1/namespaces/{daemon.namespace}/pods/{pod_name}"), 240)
        second = claim()
        assert second.claim_name != first.claim_name, "a dead daemon's route was reused"
        _wait_clear(f, daemon.namespace, _claim_selector(first.claim_name))
        old_claim = CLAIMS.format(ns=config.namespace, name=first.claim_name)
        f.wait(lambda: not f.get(old_claim), 240)
        second_pod = _prove_docker(ctx, second)
        assert second_pod["metadata"]["uid"] != first_pod["metadata"]["uid"]
        print(f"EVIDENCE retained_daemon_replaced old={first.claim_name} new={second.claim_name}")

        # 4. Release removes the claim and every labelled daemon object.
        assert substrate.release(thread, wait_gone=True)
        for handle in (first, second):
            _wait_clear(f, daemon.namespace, _claim_selector(handle.claim_name))
            for key in ("daemon-tls", "daemon-claim"):
                assert not store.exists(f"{prefix}:{key}:{handle.claim_name}")
        new_claim = CLAIMS.format(ns=config.namespace, name=second.claim_name)
        f.wait(lambda: not f.get(new_claim), 240)
        print("EVIDENCE release_cleared_daemon_objects=true valkey_daemon_keys_cleared=true")

        # 5. Orphans, then nothing labelled is left in the daemon namespace.
        _orphans(ctx, egress)
        _wait_clear(f, daemon.namespace, MANAGED)
        f.note("tool_daemon_e2e", True)
        print("EVIDENCE no_labelled_objects_remain=true")
    finally:
        kubeconfig.unlink(missing_ok=True)
        try:
            keys = list(store.scan_iter(match=f"{prefix}:*"))
            if keys:
                store.delete(*keys)
        except redis.RedisError:
            print("EVIDENCE valkey_cleanup=unreachable", file=sys.stderr)
        finally:
            store.close()
            asyncio.run(pressure.aclose())
            f.cleanup()


def _child_argv(args: argparse.Namespace) -> Any:
    def argv(nonce: str) -> list[str]:
        slice_ = args.slice
        return [sys.executable, __file__, "--context", args.context, "--candidate", "HEAD"] + [
            *("--namespace", f"fd-{slice_}-{nonce}", "--release", f"fd-{slice_}-{nonce}"),
            *("--daemon-namespace", f"fd-{slice_}-d-{nonce}"),
            *("--work-dir", f".projects/daemon-{slice_}-{nonce}"),
        ]

    return argv


def test_tool_daemon_lifecycle(request: pytest.FixtureRequest) -> None:
    values = {name: request.config.getoption("--" + name, default=None) for name in OPTIONS}
    missing = sorted(name for name, value in values.items() if not value)
    if missing:
        pytest.fail(f"CURIE_SANDBOX_E2E=1 requires the options: {', '.join(missing)}")
    fx = _load("docker_daemon_fixture", "docker-daemon-fixture.py")
    args = fx.arguments(argv=[part for name in OPTIONS for part in ("--" + name, values[name])])
    assert args.slice == "worker", "the intake is fd-worker-<nonce>"
    # Fixture.cleanup ignores SIGINT/SIGTERM for the rest of the process; restore them.
    previous = {sig: signal.getsignal(sig) for sig in SIGNALS}
    try:
        if request.config.getoption("--check-failure-cleanup", default=False):
            assert fx.check_failure_cleanup(args, _child_argv(args)) == 0
        else:
            _scenario(fx, args)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _main() -> int:
    """check_failure_cleanup child: exit 0, 42 for an injected error, 128+N for a signal."""

    fx = _load("docker_daemon_fixture", "docker-daemon-fixture.py")
    category = ""
    try:
        _scenario(fx, fx.arguments())
        return 0
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) and exc.code else 1
        category = "signal" if code > 128 else "exit"
        return code
    except RuntimeError as exc:
        category = str(exc)
        return 42 if category == "intentional_failure" else 1
    except Exception as exc:  # noqa: BLE001 - every other failure is exit 1 with its class
        category = type(exc).__name__
        return 1
    finally:
        if category:
            print(json.dumps({"error": category}), file=sys.stderr)


if __name__ == "__main__":
    sys.exit(_main())

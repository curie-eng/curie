"""Exercise approval schedules and retention on a disposable installation."""

from __future__ import annotations

import argparse
import io
import json
import os
import socket
import subprocess
import tarfile
import time
from pathlib import Path
from typing import Any

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[2]
TEST_KEY = "example-schedule-platform-key"
ALLOCATED_PORTS: set[int] = set()


def command(*argv: str, data: str | None = None) -> str:
    result = subprocess.run(argv, cwd=ROOT, input=data, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} failed ({result.returncode}): {result.stderr[-3000:]}")
    return result.stdout


def wait_until(predicate: Any, *, seconds: int = 60) -> Any:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.2)
    raise AssertionError("observable outcome did not arrive before its deadline")


def free_port() -> int:
    for port in range(38520, 38620):
        if port in ALLOCATED_PORTS:
            continue
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            ALLOCATED_PORTS.add(port)
            return port
    raise RuntimeError("no isolated test port is available")


class Install:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.ns = "test-approval-schedule"
        self.release = "approval-schedule"
        self.fullname = "approval-schedule-curie"
        if args.criterion and args.criterion.startswith("REMINDERS"):
            self.ns = "test-approval-reminders"
            self.release = "approval-reminders"
            self.fullname = "approval-reminders-curie"
        self.forwarders: list[subprocess.Popen[bytes]] = []
        self.cluster_resources: list[tuple[str, str]] = []
        self.image_pins: set[str] = set()
        self.cli = ROOT / "cli/target/debug/curie"

    def kube(self, *argv: str, data: str | None = None) -> str:
        return command("kubectl", "--context", self.args.context, "-n", self.ns, *argv, data=data)

    def helm(self, *argv: str) -> str:
        return command("helm", *argv, "--kube-context", self.args.context, "-n", self.ns)

    def import_image(self, image: str) -> str:
        first = image.split("/", 1)[0]
        if "/" not in image:
            source = f"docker.io/library/{image}"
        elif "." not in first and ":" not in first and first != "localhost":
            source = f"docker.io/{image}"
        else:
            source = image
        self.image_pins.add(source)
        saver = subprocess.Popen(
            ["docker", "--context", self.args.docker_context, "image", "save", image],
            stdout=subprocess.PIPE,
        )
        try:
            assert saver.stdout is not None
            importer = subprocess.run(
                [
                    "kubectl",
                    "--context",
                    self.args.context,
                    "-n",
                    self.ns,
                    "exec",
                    "-i",
                    "image-import",
                    "--",
                    "chroot",
                    "/node",
                    self.args.node_cli,
                    "ctr",
                    "images",
                    "import",
                    "--label",
                    "io.cri-containerd.pinned=pinned",
                    "-",
                ],
                stdin=saver.stdout,
                capture_output=True,
            )
            saver.stdout.close()
            assert importer.returncode == 0, importer.stderr.decode()
            assert saver.wait() == 0
        finally:
            if saver.poll() is None:
                saver.terminate()
                saver.wait()
        listing = self.kube(
            "exec",
            "image-import",
            "--",
            "chroot",
            "/node",
            self.args.node_cli,
            "ctr",
            "images",
            "list",
        )
        row = next(line.split() for line in listing.splitlines() if line.startswith(source + " "))
        digest = row[2]
        pinned = source.rsplit(":", 1)[0] + "@" + digest
        self.image_pins.add(pinned)
        self.kube(
            "exec",
            "image-import",
            "--",
            "chroot",
            "/node",
            self.args.node_cli,
            "ctr",
            "images",
            "tag",
            "--force",
            source,
            pinned,
        )
        return pinned

    def forward(self, service: str, remote: int) -> int:
        port = free_port()
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.args.context,
                "-n",
                self.ns,
                "port-forward",
                f"svc/{self.fullname}-{service}",
                f"{port}:{remote}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.forwarders.append(process)

        def reachable() -> bool:
            if process.poll() is not None:
                raise RuntimeError("owned port-forward exited")
            with socket.socket() as probe:
                return probe.connect_ex(("127.0.0.1", port)) == 0

        wait_until(reachable)
        return port

    def run_cli(self, *argv: str, token: str | None = None) -> dict[str, Any]:
        env = dict(os.environ, CURIE_API_KEY=TEST_KEY)
        if token:
            env["CURIE_APPROVAL_PRINCIPAL_TOKEN"] = token
        result = subprocess.run(
            [
                str(self.cli),
                "--json",
                "cluster",
                "approvals",
                "acme-schedule",
                "--context",
                self.args.context,
                "--namespace",
                self.ns,
                "--release",
                self.release,
                "--api-url",
                self.url,
                *argv,
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr + result.stdout
        return json.loads(result.stdout)

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.http.request(method, path, **kwargs)
        assert response.is_success, f"{method} {path}: {response.status_code} {response.text}"
        return response.json() if response.content else None

    def connect_api(self) -> None:
        if hasattr(self, "http"):
            self.http.close()
        self.url = f"http://127.0.0.1:{self.forward('api', 8000)}"
        self.http = httpx.Client(base_url=self.url, headers={"X-API-Key": TEST_KEY}, timeout=15)

    def start(self) -> None:
        importer = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "image-import"},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "importer",
                        "image": "alpine:3.23",
                        "command": ["sleep", "3600"],
                        "securityContext": {"privileged": True},
                        "volumeMounts": [{"name": "node", "mountPath": "/node"}],
                    }
                ],
                "volumes": [{"name": "node", "hostPath": {"path": "/"}}],
            },
        }
        self.kube("apply", "-f", "-", data=json.dumps(importer))
        self.kube("wait", "--for=condition=Ready", "pod/image-import", "--timeout=90s")
        api, worker, postgres, valkey, verifier, previous_worker, previous_api = [
            self.import_image(image)
            for image in (
                self.args.api_image,
                self.args.worker_image,
                self.args.postgres_image,
                self.args.valkey_image,
                self.args.verification_image,
                self.args.previous_worker_image,
                self.args.previous_api_image,
            )
        ]
        self.worker_image = worker
        self.verification_image = verifier
        self.previous_worker_image = previous_worker
        self.previous_api_image = previous_api
        values = {
            "api": {
                "deploy": True,
                "replicas": 0,
                "image": self.image_values(api),
                "apiKey": TEST_KEY,
                "approvalRecovery": {"enabled": True},
            },
            "worker": {"deploy": True, "replicas": 0, "image": self.image_values(worker)},
            "postgres": {
                "deploy": True,
                "image": postgres,
                "persistence": {"enabled": False},
                "containerSecurityContext": {"readOnlyRootFilesystem": False},
            },
            "valkey": {
                "deploy": True,
                "image": valkey,
                "persistence": {"enabled": False},
                "containerSecurityContext": {"readOnlyRootFilesystem": False},
            },
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "dispatcher": {"deploy": False},
            "ui": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "priorityClasses": {
                "platform": {"name": self.ns + "-platform"},
                "sandbox": {"name": self.ns + "-sandbox"},
            },
            "agentSandbox": {
                "deploy": True,
                "controller": {"deploy": False},
                "warmPool": {"replicas": 0},
                "runner": {
                    "image": "ghcr.io/curie-eng/curie-runner",
                    "tag": str(
                        yaml.safe_load((ROOT / "charts/curie/Chart.yaml").read_text())["appVersion"]
                    ),
                    "imagePullPolicy": "IfNotPresent",
                    "fakeModel": True,
                    "prewarm": {"enabled": True, "imagePullPolicy": "IfNotPresent"},
                },
            },
        }
        self.values = values
        self.values_file = Path(self.args.values_file)
        self.values_file.write_text(json.dumps(values))
        rendered = self.helm(
            "template",
            self.release,
            "charts/curie",
            "-f",
            "charts/curie/values-dev.yaml",
            "-f",
            "charts/curie/values-e2e-nogvisor.yaml",
            "-f",
            "charts/curie/values-e2e-harness.yaml",
            "-f",
            str(self.values_file),
        )
        for obj in yaml.safe_load_all(rendered):
            if obj and obj["kind"] in {
                "PriorityClass",
                "ClusterRole",
                "ClusterRoleBinding",
                "ValidatingAdmissionPolicy",
                "ValidatingAdmissionPolicyBinding",
                "Namespace",
            }:
                name = obj["metadata"]["name"]
                assert name.startswith((self.release, self.ns)), "cluster resource is not owned"
                self.cluster_resources.append((obj["kind"], name))
        self.helm(
            "install",
            self.release,
            "charts/curie",
            "-f",
            "charts/curie/values-dev.yaml",
            "-f",
            "charts/curie/values-e2e-nogvisor.yaml",
            "-f",
            "charts/curie/values-e2e-harness.yaml",
            "-f",
            str(self.values_file),
            "--timeout",
            "180s",
        )
        self.kube(
            "rollout", "status", f"daemonset/{self.fullname}-runner-prewarm", "--timeout=180s"
        )
        self.kube("scale", f"deployment/{self.fullname}-api", "--replicas=1")
        self.kube("rollout", "status", f"deployment/{self.fullname}-api", "--timeout=90s")
        self.connect_api()

    def start_probe(self) -> None:
        if hasattr(self, "probe_pod"):
            return
        deployment = json.loads(
            self.kube("get", f"deployment/{self.fullname}-worker", "-o", "json")
        )
        worker = deployment["spec"]["template"]["spec"]["containers"][0]
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "reference-probe"},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": deployment["spec"]["template"]["metadata"]["labels"]},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "probe",
                                "image": self.worker_image,
                                "imagePullPolicy": "Never",
                                "command": ["sleep", "3600"],
                                "env": worker["env"],
                            }
                        ],
                    },
                },
            },
        }
        self.kube("apply", "-f", "-", data=json.dumps(job))

        def pod_name():
            pods = json.loads(
                self.kube("get", "pods", "-l", "job-name=reference-probe", "-o", "json")
            )["items"]
            return pods[0]["metadata"]["name"] if pods else None

        self.probe_pod = wait_until(pod_name)
        self.kube("wait", "--for=condition=Ready", f"pod/{self.probe_pod}", "--timeout=90s")

    def probe(self, script: str, *, enabled: bool = True) -> Any:
        prefix = """import asyncio, json
from redis.asyncio import Redis
from curie_worker.config import WorkerConfig
from curie_worker.approval_cards import ApprovalCardStore
config = WorkerConfig()
async def verify():
    redis = Redis(**config.valkey_client_kwargs())
    store = ApprovalCardStore(redis, config)
    try:
"""
        source = prefix + "\n".join("        " + line for line in script.splitlines())
        source += "\n    finally:\n        await redis.aclose()\nasyncio.run(verify())\n"
        output = self.kube(
            "exec",
            "-i",
            self.probe_pod,
            "--",
            "env",
            f"CURIE_APPROVAL_PENDING_RETENTION_ENABLED={'true' if enabled else 'false'}",
            "python3",
            "-",
            data=source,
        )
        return json.loads(output) if output.strip() else None

    def ttl(self, approval_id: str) -> list[int]:
        return self.probe(f"""approval_id = {approval_id!r}
keys = [config.approval_card_key(approval_id),
        config.approval_notice_ref_key(approval_id),
        config.approval_reply_below_card_key(approval_id)]
print(json.dumps([await redis.pttl(key) for key in keys]))""")

    def remember(self, approval_id: str, *, enabled: bool = True) -> None:
        self.probe(
            f"""approval_id = {approval_id!r}
await store.remember(approval_id, channel="C0EXAMPLE1", ts="1700000000.000001",
                     summary="Approve bounded work", endpoint=None, kind="slack")
await store.remember_notice_ref(approval_id, "1700000000.000000")
await store.remember_reply_below_card(approval_id)""",
            enabled=enabled,
        )

    def advance_clock(self, days: int) -> None:
        for service in ("postgres", "valkey"):
            self.kube(
                "exec",
                f"statefulset/{self.fullname}-{service}",
                "--",
                "sh",
                "-c",
                f"printf '+{days}d\\n' > /clock/offset",
            )

    def verify_http_owner(
        self,
        paths: tuple[str, ...] = ("apps/api/tests/test_http_approval_creation.py",),
    ) -> None:
        deployment = json.loads(self.kube("get", f"deployment/{self.fullname}-api", "-o", "json"))
        source = deployment["spec"]["template"]["spec"]["containers"][0]
        env = list(source["env"])
        aliases = {
            "DATABASE_URL": "TEST_DATABASE_URL",
            "VALKEY_HOST": "TEST_VALKEY_HOST",
            "VALKEY_PORT": "TEST_VALKEY_PORT",
            "VALKEY_PASSWORD": "TEST_VALKEY_PW",
            "S3_ENDPOINT_URL": "TEST_S3_ENDPOINT_URL",
        }
        env.extend(
            {**entry, "name": aliases[entry["name"]]}
            for entry in source["env"]
            if entry["name"] in aliases
        )
        env.extend(
            {"name": name, "value": "1"}
            for name in ("CI_REQUIRE_POSTGRES_TESTS", "CI_REQUIRE_VALKEY_TESTS")
        )
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "http-owner-verification"},
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 180,
                "template": {
                    "metadata": {"labels": deployment["spec"]["template"]["metadata"]["labels"]},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "verify",
                                "image": self.verification_image,
                                "imagePullPolicy": "Never",
                                "env": env,
                                "command": [
                                    "python3",
                                    "-m",
                                    "pytest",
                                    "-q",
                                    "--tb=short",
                                    *paths,
                                ],
                            }
                        ],
                    },
                },
            },
        }
        self.kube("apply", "-f", "-", data=json.dumps(job))
        try:
            self.kube(
                "wait", "--for=condition=complete", "job/http-owner-verification", "--timeout=180s"
            )
        except RuntimeError as error:
            logs = self.kube("logs", "job/http-owner-verification")
            raise AssertionError(logs[-6000:]) from error
        self.kube("delete", "job/http-owner-verification", "--wait=true")
        print(
            json.dumps(
                {
                    "criterion": "AC1",
                    "candidate": self.args.candidate,
                    "outcome": "HTTP creation and sealed approval continuity suite passed",
                }
            ),
            flush=True,
        )

    def verify_worker_transport(self, agent_id: str) -> None:
        if not self.args.listen_host:
            raise ValueError("--listen-host must name the caller's callback interface")
        manifest = {
            "name": "acme-schedule",
            "version": "0.1.0",
            "approvalPolicy": {
                "gates": [
                    {"gate": tool, "route": route}
                    for tool, route in (
                        ("Bash", "finite"),
                        ("Read", "indefinite"),
                        ("Write", "omitted"),
                    )
                ]
            },
        }
        files = {
            ".claude-plugin/plugin.json": json.dumps(manifest),
            "skills/approve/SKILL.md": (
                "---\nname: approve\ndescription: Ask for signoff\n---\n\nAsk for approval.\n"
            ),
        }
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w:gz") as tar:
            for name, content in files.items():
                info = tarfile.TarInfo("acme-schedule/" + name)
                data = content.encode()
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        version = self.request(
            "POST",
            f"/agents/{agent_id}/versions",
            json={"version_label": "schedule-evidence", "created_by": "operator"},
        )
        self.request(
            "PUT",
            f"/agents/{agent_id}/versions/{version['id']}/bundle",
            files={"file": ("bundle.tar.gz", archive.getvalue())},
        )
        deployment = self.request(
            "POST",
            "/deployments",
            json={"agent_id": agent_id, "version_id": version["id"], "environment": "dev"},
        )
        routes = self.request("GET", f"/agents/{agent_id}")["approval_routes"]
        stripped = {
            name: {key: value for key, value in binding.items() if key != "expires_in_seconds"}
            for name, binding in routes.items()
        }
        self.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": stripped})
        self.kube(
            "set",
            "image",
            f"deployment/{self.fullname}-worker",
            f"worker={self.previous_worker_image}",
        )
        self.kube("scale", f"deployment/{self.fullname}-worker", "--replicas=1")
        self.kube("rollout", "status", f"deployment/{self.fullname}-worker", "--timeout=90s")

        def message(route: str, duration: int | None, *, expected: bool = True) -> Any:
            count = len(self.request("GET", "/approvals", params={"agent_id": agent_id}))
            result = subprocess.run(
                [
                    str(self.cli),
                    "--json",
                    "cluster",
                    "message",
                    f"[fake:request-approval:{route}] Approve bounded work",
                    "--agent",
                    "acme-schedule",
                    "--channel",
                    "C0EXAMPLE1",
                    "--user",
                    "U0EXAMPLE1",
                    "--context",
                    self.args.context,
                    "--namespace",
                    self.ns,
                    "--release",
                    self.release,
                    "--chart",
                    "charts/curie",
                    "--listen-host",
                    self.args.listen_host,
                    "--listen-port",
                    str(free_port()),
                    "--valkey-local-port",
                    str(free_port()),
                    "--api-local-port",
                    str(free_port()),
                    "--timeout-secs",
                    "30",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=dict(os.environ, CURIE_API_KEY=TEST_KEY, CURIE_VALKEY_PASSWORD="valkeypass"),
            )
            if not expected:
                refused = json.loads(result.stdout)
                assert refused.get("failure_class") == "approval-route-unbound", refused
                assert (
                    len(self.request("GET", "/approvals", params={"agent_id": agent_id})) == count
                )
                return None
            assert result.returncode == 3, result.stdout + result.stderr
            outcome = json.loads(result.stdout)
            assert outcome["awaiting_approval"] is True
            records = self.request("GET", "/approvals", params={"agent_id": agent_id})
            row = next(row for row in records if row["conversation_id"] == outcome["thread"])
            from datetime import datetime

            actual = (
                (
                    datetime.fromisoformat(row["expires_at"])
                    - datetime.fromisoformat(row["created_at"])
                ).total_seconds()
                if row["expires_at"]
                else None
            )
            assert actual == duration
            return row

        old = message("finite", 86400)
        assert old is not None
        self.start_probe()
        old_ttls = self.ttl(old["id"])
        assert old_ttls[0] > 0
        self.old_writer_approval = old["id"]
        self.old_writer_reference_slots = [index for index, ttl in enumerate(old_ttls) if ttl > 0]
        self.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": routes})
        message("finite", None, expected=False)
        self.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": stripped})
        old_read = {
            "resolution": {"mode": "requesting_surface"},
            "expires_in_seconds": None,
        }
        source = (
            "import json; from pydantic import ValidationError; "
            "from curie_api.schemas.approvals import ApprovalRouteBinding; "
            f"binding=json.loads({json.dumps(old_read)!r})\n"
            "try:\n    ApprovalRouteBinding.model_validate(binding)\n"
            "except ValidationError:\n    pass\n"
            "else:\n    raise AssertionError('old API accepted a new key')\n"
            "binding.pop('expires_in_seconds')\n"
            "ApprovalRouteBinding.model_validate(binding)\n"
        )
        self.kube(
            "run",
            "previous-route-reader",
            "--restart=Never",
            "--image=" + self.previous_api_image,
            "--image-pull-policy=Never",
            "--command",
            "--",
            "python3",
            "-c",
            source,
        )
        try:
            self.kube(
                "wait",
                "--for=jsonpath={.status.phase}=Succeeded",
                "pod/previous-route-reader",
                "--timeout=60s",
            )
        except RuntimeError as error:
            raise AssertionError(self.kube("logs", "previous-route-reader")) from error
        assert message("finite", 86400) is not None
        self.kube(
            "set", "image", f"deployment/{self.fullname}-worker", f"worker={self.worker_image}"
        )
        self.kube("rollout", "status", f"deployment/{self.fullname}-worker", "--timeout=90s")
        routes["finite"]["expires_in_seconds"] = 2592000
        self.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": routes})
        self.turn_approvals = []
        for route, duration in (("finite", 2592000), ("indefinite", None), ("omitted", 86400)):
            row = message(route, duration)
            assert row is not None
            self.turn_approvals.append(row)
        self.approvals[:2] = self.turn_approvals[:2]
        self.request("DELETE", f"/deployments/{deployment['id']}")
        print(
            json.dumps(
                {
                    "criterion": "AC1/AC2",
                    "candidate": self.args.candidate,
                    "outcome": (
                        "real worker pause, 24-hour default and "
                        "requesting-surface HTTP cards passed"
                    ),
                }
            ),
            flush=True,
        )

    def wait_terminal_references(self, approval_id: str, decision: str) -> None:
        def bounded() -> bool:
            ttls = self.ttl(approval_id)
            if not all(0 < value <= 14 * 86400 * 1000 for value in ttls[1:]):
                return False
            if 0 < ttls[0] <= 14 * 86400 * 1000:
                return True
            if ttls[0] != -2:
                return False
            reply_ref = self.card_reply_refs[approval_id]
            response = self.http.get(f"/cluster-message-replies/{reply_ref}")
            if response.status_code == 404:
                return False
            assert response.is_success, response.text
            return any(
                event.get("event") == "reply.update"
                and event.get("target", {}).get("reply_ref") == reply_ref
                and (event.get("settled") or {}).get("decision") == decision
                for event in response.json()["events"]
            )

        try:
            wait_until(bounded)
        except AssertionError as error:
            raise AssertionError(f"terminal reference TTLs: {self.ttl(approval_id)}") from error

    def verify_retention(self, agent_id: str) -> None:
        self.start_probe()
        finite, indefinite = self.approvals[:2]
        self.kube("scale", f"deployment/{self.fullname}-worker", "--replicas=1")
        self.kube("rollout", "status", f"deployment/{self.fullname}-worker", "--timeout=90s")
        deleted_agent = self.request(
            "POST",
            "/agents",
            json={
                "name": "acme-retention-deletion",
                "channel": {"kind": "slack", "address": "C0EXAMPLE2"},
            },
        )
        deleted = self.request(
            "POST",
            "/approvals",
            json={
                "agent_id": deleted_agent["id"],
                "conversation_id": "default-off-cleanup",
                "author": "U0EXAMPLE1",
                "summary": "Approve bounded work",
                "reply_kind": "slack",
                "reply_channel": "C0EXAMPLE2",
                "reply_placeholder": None,
                "dedupe_key": "default-off-cleanup",
                "expires_in_seconds": None,
            },
        )
        self.remember(deleted["id"], enabled=False)
        self.request("DELETE", f"/agents/{deleted_agent['id']}")
        wait_until(
            lambda: self.probe(
                f"print(json.dumps(not await redis.sismember("
                f"config.approval_references_pending_key(), {deleted['id']!r})))"
            )
        )
        assert all(value > 0 for value in self.ttl(deleted["id"]))
        self.card_reply_refs = {}
        for approval in (finite, indefinite):
            self.card_reply_refs[approval["id"]] = self.probe(
                f"""approval_id = {approval["id"]!r}
entry = await store.read(approval_id)
assert entry is not None
notice = await store.read_notice_ref(approval_id) or {approval.get("reply_placeholder")!r}
assert notice
await store.remember_notice_ref(approval_id, notice)
await store.remember_reply_below_card(approval_id)
print(json.dumps(entry[0].ts))""",
                enabled=False,
            )
            assert all(value > 0 for value in self.ttl(approval["id"]))
        self.kube(
            "exec",
            self.probe_pod,
            "--",
            "env",
            "CURIE_APPROVAL_PENDING_RETENTION_ENABLED=true",
            "python3",
            "-m",
            "curie_worker.approval_cards",
            "--writers-drained",
        )
        assert self.ttl(finite["id"]) == [-1, -1, -1]
        late = self.request(
            "POST",
            "/approvals",
            json={
                "agent_id": agent_id,
                "conversation_id": "late-old-writer",
                "author": "U0EXAMPLE1",
                "summary": "Approve bounded work",
                "reply_kind": "slack",
                "reply_channel": "C0EXAMPLE1",
                "reply_placeholder": None,
                "dedupe_key": "late-old-writer",
                "expires_in_seconds": None,
            },
        )
        self.probe(f"""target = {late["id"]!r}
source = {finite["id"]!r}
for key in (config.approval_card_key, config.approval_notice_ref_key,
            config.approval_reply_below_card_key):
    value = await redis.get(key(source))
    assert value is not None
    await redis.set(key(target), value, ex=3600)
assert not await redis.sismember(config.approval_references_pending_key(), target)""")
        self.probe(
            f"""approval_id = {finite["id"]!r}
notice = await store.read_notice_ref(approval_id)
assert notice
await store.remember_notice_ref(approval_id, notice)""",
            enabled=False,
        )
        assert self.ttl(finite["id"]) == [-1, -1, -1]
        self.kube(
            "set",
            "env",
            f"deployment/{self.fullname}-worker",
            "CURIE_APPROVAL_PENDING_RETENTION_ENABLED=true",
        )
        self.kube("scale", f"deployment/{self.fullname}-worker", "--replicas=1")
        self.kube("rollout", "status", f"deployment/{self.fullname}-worker", "--timeout=90s")
        self.kube(
            "exec",
            self.probe_pod,
            "--",
            "env",
            "CURIE_APPROVAL_PENDING_RETENTION_ENABLED=true",
            "python3",
            "-m",
            "curie_worker.approval_cards",
            "--writers-drained",
        )
        assert self.ttl(finite["id"]) == [-1, -1, -1]
        assert self.ttl(late["id"]) == [-1, -1, -1]
        migrated_old = self.ttl(self.old_writer_approval)
        assert all(migrated_old[index] == -1 for index in self.old_writer_reference_slots)
        self.advance_clock(15)
        self.kube("rollout", "restart", f"deployment/{self.fullname}-worker")
        self.kube("rollout", "status", f"deployment/{self.fullname}-worker", "--timeout=90s")
        assert self.request("GET", f"/approvals/{finite['id']}")["status"] == "pending"
        assert self.ttl(finite["id"]) == [-1, -1, -1]
        assert self.ttl(indefinite["id"]) == [-1, -1, -1]
        token = self.run_cli("--mint-operator-principal", "U0EXAMPLE2")["operator_principal"][
            "token"
        ]
        resolved = self.run_cli("--resolve", finite["id"], token=token)
        assert resolved["resolved"]["status"] == "approved"
        self.wait_terminal_references(finite["id"], "approved")
        administrative = self.request(
            "POST",
            "/approvals",
            json={
                "agent_id": agent_id,
                "conversation_id": "ordinary-rejection",
                "author": "U0EXAMPLE1",
                "summary": "Approve bounded work",
                "reply_kind": "slack",
                "reply_channel": "C0EXAMPLE1",
                "reply_placeholder": None,
                "card_channel": "C0EXAMPLE1",
                "dedupe_key": "ordinary-rejection",
                "route": "indefinite",
                "expires_in_seconds": None,
            },
        )
        rejected = self.run_cli("--resolve", administrative["id"], "--reject", token=token)
        assert rejected["resolved"]["status"] == "rejected"
        self.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": {}})
        response = self.http.post(
            f"/approvals/{indefinite['id']}/resolve",
            headers={"X-Curie-Approval-Principal": token},
            json={"decision": "approved"},
        )
        assert response.status_code == 403
        assert self.request("GET", f"/approvals/{indefinite['id']}")["status"] == "pending"
        recovered = self.run_cli(
            "--recover",
            indefinite["id"],
            "--reason",
            "Route removed",
            "--recovery-key",
            "schedule-recovery",
            token=token,
        )
        assert recovered["recovered"]["status"] == "rejected"
        self.wait_terminal_references(indefinite["id"], "rejected")
        body = {
            "agent_id": agent_id,
            "conversation_id": "deletion-case",
            "author": "U0EXAMPLE1",
            "summary": "Approve bounded work",
            "reply_kind": "slack",
            "reply_channel": "C0EXAMPLE1",
            "reply_placeholder": None,
            "dedupe_key": "schedule-delete",
            "expires_in_seconds": None,
        }
        orphan = self.request("POST", "/approvals", json=body)
        self.remember(orphan["id"])
        self.kube("scale", f"deployment/{self.fullname}-api", "--replicas=0")
        self.kube(
            "wait",
            "--for=delete",
            "pod",
            "-l",
            f"app.kubernetes.io/instance={self.release},app.kubernetes.io/component=api",
            "--timeout=90s",
        )
        self.probe(f"""import httpx
from curie_worker.approvals import ApprovalClient
async with httpx.AsyncClient() as http:
    reader = ApprovalClient(api_base_url=config.api_base_url, api_key=config.api_key,
                            client=http, read_timeout_s=2)
    await store.reconcile_pending(reader)
assert await redis.sismember(config.approval_references_pending_key(), {orphan["id"]!r})""")
        assert self.ttl(orphan["id"]) == [-1, -1, -1]
        self.kube("scale", f"deployment/{self.fullname}-api", "--replicas=1")
        self.kube("rollout", "status", f"deployment/{self.fullname}-api", "--timeout=90s")
        self.http.close()
        self.url = f"http://127.0.0.1:{self.forward('api', 8000)}"
        self.http = httpx.Client(base_url=self.url, headers={"X-API-Key": TEST_KEY}, timeout=15)
        self.request("DELETE", f"/agents/{agent_id}")
        wait_until(lambda: all(0 < value <= 14 * 86400 * 1000 for value in self.ttl(orphan["id"])))
        wait_until(lambda: all(0 < value <= 14 * 86400 * 1000 for value in self.ttl(late["id"])))
        self.advance_clock(30)
        for approval in (finite, indefinite, orphan, late):
            assert self.ttl(approval["id"]) == [-2, -2, -2]
        print(
            json.dumps(
                {
                    "criterion": "AC2/AC3",
                    "candidate": self.args.candidate,
                    "outcome": (
                        "15-day pending retention, restarted readers, settlement, recovery, "
                        "deletion and bounded terminal windows passed"
                    ),
                }
            ),
            flush=True,
        )

    @staticmethod
    def image_values(image: str) -> dict[str, str]:
        repository, digest = image.split("@")
        return {"repository": repository, "digest": digest, "tag": "", "pullPolicy": "Never"}

    def verify(self) -> None:
        if self.args.criterion and self.args.criterion.startswith("REMINDERS"):
            from approval_reminder_acceptance import verify_reminders

            verify_reminders(self)
            return
        route = {
            "resolution": {"mode": "requesting_surface"},
            "approvers": {"users": ["U0EXAMPLE2"]},
        }
        agent = self.request(
            "POST",
            "/agents",
            json={
                "name": "acme-schedule",
                "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
                "approval_routes": {"review": route},
            },
        )
        agent_id = agent["id"]
        expected = {
            "omitted": {**route, "notification": None},
            "finite": {**route, "expires_in_seconds": 2592000},
            "indefinite": {**route, "expires_in_seconds": None},
        }
        routes_file = self.values_file.with_name("routes.json")
        routes_file.write_text(json.dumps(expected))
        written = self.run_cli("--routes-from", str(routes_file))
        read = self.run_cli("--list-routes")
        assert "expires_in_seconds" not in read["routes"]["omitted"]
        assert read["routes"]["finite"]["expires_in_seconds"] == 2592000
        assert read["routes"]["indefinite"]["expires_in_seconds"] is None
        assert written["routes"] == read["routes"]
        stored = json.loads(
            self.kube(
                "exec",
                f"statefulset/{self.fullname}-postgres",
                "--",
                "psql",
                "-U",
                "postgres",
                "-Atc",
                f"SELECT approval_routes FROM curie.agents WHERE id='{agent_id}'",
            )
        )
        assert "notification" not in stored["omitted"]
        deadline_vectors = json.loads(
            (ROOT / "tests/vectors/approval-route-deadlines.json").read_text()
        )
        for value in deadline_vectors["invalid"]:
            response = self.http.patch(
                f"/agents/{agent_id}",
                json={"approval_routes": {"invalid": {**route, "expires_in_seconds": value}}},
            )
            assert response.status_code == 422
        approvals = []
        requests = []
        for label, name, duration in (
            ("finite", "finite", 7),
            ("indefinite", "indefinite", 7),
            ("omitted-finite", "omitted", 23 * 86400),
            ("omitted-null", "omitted", None),
            ("routeless-finite", None, 17 * 86400),
            ("routeless-null", None, None),
        ):
            body = {
                "agent_id": agent_id,
                "conversation_id": "schedule-install",
                "author": "U0EXAMPLE1",
                "summary": "Approve bounded work",
                "reply_kind": "slack",
                "reply_channel": "C0EXAMPLE1",
                "reply_placeholder": None,
                "dedupe_key": f"schedule-{label}",
                "route": name,
                "card_channel": "C0EXAMPLE1",
                "expires_in_seconds": duration,
            }
            approval = self.request("POST", "/approvals", json=body)
            approval = self.request("GET", f"/approvals/{approval['id']}")
            from datetime import datetime

            actual = (
                (
                    datetime.fromisoformat(approval["expires_at"])
                    - datetime.fromisoformat(approval["created_at"])
                ).total_seconds()
                if approval["expires_at"]
                else None
            )
            assert actual == {"finite": 2592000, "indefinite": None}.get(name, duration)
            assert self.request("POST", "/approvals", json=body)["id"] == approval["id"]
            approvals.append(approval)
            requests.append(body)
        edited = {**expected, "finite": {**route, "expires_in_seconds": 99}}
        self.request("PATCH", f"/agents/{agent_id}", json={"approval_routes": edited})
        for body, original in zip(requests, approvals, strict=True):
            assert self.request("POST", "/approvals", json=body)["id"] == original["id"]
            assert (
                self.request("GET", f"/approvals/{original['id']}")["expires_at"]
                == original["expires_at"]
            )
        self.approvals = approvals
        print(
            json.dumps(
                {
                    "criterion": "AC1",
                    "candidate": self.args.candidate,
                    "outcome": "operator API/CLI and legacy deadline checks passed",
                }
            ),
            flush=True,
        )
        self.verify_http_owner()
        self.verify_worker_transport(agent_id)
        if self.args.criterion != "AC1":
            self.verify_retention(agent_id)

    def close(self) -> None:
        if hasattr(self, "http"):
            self.http.close()
        for process in self.forwarders:
            process.terminate()
            process.wait()
        if self.image_pins:
            image_command = (
                "exec",
                "image-import",
                "--",
                "chroot",
                "/node",
                self.args.node_cli,
                "ctr",
                "images",
            )
            present = {row.split()[0] for row in self.kube(*image_command, "list").splitlines()[1:]}
            for ref in sorted(self.image_pins & present):
                self.kube(*image_command, "label", ref, "io.cri-containerd.pinned-")
        subprocess.run(
            [
                "helm",
                "uninstall",
                self.release,
                "--kube-context",
                self.args.context,
                "-n",
                self.ns,
                "--no-hooks",
            ],
            capture_output=True,
        )
        self.kube("delete", "pod", "--all", "--grace-period=0", "--force", "--wait=true")
        for kind, name in self.cluster_resources:
            self.kube("delete", kind, name, "--ignore-not-found")
            assert not self.kube("get", kind, name, "--ignore-not-found", "-o", "name").strip()
        self.kube("delete", "namespace", self.ns, "--wait=true", "--timeout=120s")
        assert not self.kube(
            "get", "namespace", self.ns, "--ignore-not-found", "-o", "name"
        ).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--docker-context", required=True)
    parser.add_argument("--node-cli", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--api-image", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--postgres-image", required=True)
    parser.add_argument("--valkey-image", required=True)
    parser.add_argument("--verification-image", required=True)
    parser.add_argument("--previous-worker-image", required=True)
    parser.add_argument("--previous-api-image", required=True)
    parser.add_argument("--values-file", required=True)
    parser.add_argument("--listen-host", default=os.environ.get("CURIE_LOCAL_STUB_HOST"))
    parser.add_argument("--criterion", choices=["AC1", "AC2", "AC3", "REMINDERS"])
    args = parser.parse_args()
    head = command("git", "rev-parse", "HEAD").strip()
    assert command("git", "rev-parse", args.candidate).strip() == head, "candidate is not HEAD"
    assert not command("git", "status", "--porcelain").strip(), "candidate tree is dirty"
    args.candidate = head
    for image in (args.api_image, args.worker_image, args.verification_image):
        revision = command(
            "docker",
            "--context",
            args.docker_context,
            "image",
            "inspect",
            image,
            "--format",
            '{{index .Config.Labels "org.opencontainers.image.revision"}}',
        ).strip()
        assert revision == head, "candidate image revision does not match HEAD"
    install = Install(args)
    command("kubectl", "--context", args.context, "create", "namespace", install.ns)
    try:
        install.start()
        install.verify()
    except Exception:
        try:
            pods = json.loads(install.kube("get", "pods", "-o", "json"))["items"]
            print(
                json.dumps(
                    {
                        "pods": [
                            {
                                "name": pod["metadata"]["name"],
                                "phase": pod.get("status", {}).get("phase"),
                                "containers": [
                                    {
                                        "name": container["name"],
                                        "ready": container["ready"],
                                        "reason": next(
                                            iter(container.get("state", {}).values()), {}
                                        ).get("reason"),
                                    }
                                    for container in pod.get("status", {}).get(
                                        "containerStatuses", []
                                    )
                                ],
                            }
                            for pod in pods
                        ]
                    }
                ),
                flush=True,
            )
            for pod in pods:
                if "schema-migrate" in pod["metadata"]["name"]:
                    print(install.kube("logs", pod["metadata"]["name"], "--tail=40"), flush=True)
        except RuntimeError:
            print("verification diagnostics unavailable", flush=True)
        raise
    finally:
        install.close()


if __name__ == "__main__":
    main()

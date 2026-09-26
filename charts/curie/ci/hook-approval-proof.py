#!/usr/bin/env python3
"""Live cluster proof for signed hook approval pause and resume.

This driver expects an installed, disposable Curie release. It creates one
receipt fixture and one agent, runs the proof, and removes both. It never reads
or prints model credentials. The platform release and namespace remain owned by
the caller.

Required environment:

CURIE_HOOK_APPROVAL_CONTEXT       k8 or kind-test-2765
CURIE_NAMESPACE                   test-2765
CURIE_RELEASE                     test-2765
CURIE_BIN                         Path to the candidate curie binary
CURIE_API_URL                     Reachable URL for the installed API
CURIE_API_KEY                     Platform API key
CURIE_HOOK_APPROVAL_RECEIPT_IMAGE Imported receipt fixture image

Required Helm values for the installed release:

worker.slackApiBaseUrl: "http://127.0.0.1:1"
worker.slackTrustedOrigins: ""

Build the receipt fixture from ``cli/scripts/fixtures/mcp-receipt/Dockerfile``
with ``cli/scripts/fixtures/mcp-receipt`` as the Docker build context, import
that exact image into the target cluster, and pass its imported image reference
through ``CURIE_HOOK_APPROVAL_RECEIPT_IMAGE``. The checked in fixture serves
``/mcp`` on port 8000, exposes ``receipt_read``, and writes the exact line
``MCP_RECEIPT tools/call`` for each accepted tool call.

The worker must have no ``SLACK_BOT_TOKEN`` so no human Slack transport takes
part in the proof; preflight checks that condition. Slack acknowledges the
``TurnCompleted`` event without a transport call, so the proof requires the
initial completion hash to be absent after the done marker appears.

The agent binds the channel under the default identity. The worker's own Slack
origin is the refusing endpoint, so the initial stream fails loudly there, and
the empty bot token never reaches a reachable Slack.

The alert summary must require the receipt tool before any narrative. Initial
turn streaming fails loudly at the disconnected endpoint, so narrative before
the gated tool ends the proof before it can reach the approval state.

Optional environment:

CURIE_HOOK_APPROVAL_MODEL           Defaults to z-ai/glm-5.2
CURIE_HOOK_APPROVAL_TIMEOUT_SECONDS Defaults to 600
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

AGENT = "hook-approval-2765"
BUNDLE = "hook-approval-proof"
CHANNEL = "C0EXAMPLE1"
OPERATOR = "U0EXAMPLE1"
HOOK = "alertmanager"
FIXTURE = "hook-approval-2765-receipt"
RECEIPT_LINE = "MCP_RECEIPT tools/call"
RECEIPT_TOOL = "mcp__plugin_hook-approval-proof_receipt__receipt_read"
RUNS_STREAM = "curie:runs"
OFFLINE_ENDPOINT = "http://127.0.0.1:1"
ROUTE_IDENTITY = "default"
COMMAND_TIMEOUT_SECONDS = 180


def _thread_key(conversation_id: str) -> str:
    """The worker's internal thread key for this proof's Slack route.

    This rig's route is the default identity, which adds no segment
    (ADR-0168 decision 4): ``channel_protocol.identity.scoped_conversation_id``
    called with ``identity=ROUTE_IDENTITY``, which the worker's own
    ``_thread_key_for`` builds the same way. This script cannot import those
    packages -- they need pydantic and a newer Python than the bare
    ``python3`` this rig runs under -- so the rule is reproduced here instead
    of called.
    """
    return ":".join(
        urllib.parse.quote(part, safe="")
        for part in ("slack", CHANNEL, conversation_id)
    )


class ProofError(RuntimeError):
    pass


def check_offline_slack_origin(worker_env: dict[str, Any]) -> None:
    """The worker's own Slack origin is the refusing endpoint, and nothing else is trusted."""
    origin = worker_env.get("SLACK_API_BASE_URL", {})
    if origin.get("value") != OFFLINE_ENDPOINT or "valueFrom" in origin:
        raise ProofError(
            "proof release must set worker.slackApiBaseUrl to the offline endpoint"
        )
    trusted = worker_env.get("CURIE_SLACK_TRUSTED_ORIGINS")
    if trusted is not None and (
        trusted.get("value") not in {None, ""} or "valueFrom" in trusted
    ):
        raise ProofError("proof release must trust no extra Slack origin")


class HttpStatus(ProofError):
    def __init__(self, status: int, path: str, body: bytes = b"") -> None:
        super().__init__(f"API request to {path} returned HTTP {status}")
        self.status = status
        self.path = path
        self.body = body


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ProofError(f"set {name}")
    return value


def run(
    argv: list[str],
    *,
    stdin: str | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            argv,
            input=stdin,
            text=True,
            capture_output=True,
            env=env,
            check=False,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        program = Path(argv[0]).name
        raise ProofError(
            f"{program} exceeded the {COMMAND_TIMEOUT_SECONDS} second command timeout"
        ) from exc
    if check and result.returncode != 0:
        program = Path(argv[0]).name
        action = next((part for part in argv[1:] if not part.startswith("-")), "command")
        raise ProofError(f"{program} {action} failed with exit code {result.returncode}")
    return result


def parse_json(raw: str, label: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProofError(f"{label} did not return JSON") from exc


class Proof:
    def __init__(self) -> None:
        self.context = required_env("CURIE_HOOK_APPROVAL_CONTEXT")
        self.namespace = required_env("CURIE_NAMESPACE")
        self.release = required_env("CURIE_RELEASE")
        self.curie_bin = Path(required_env("CURIE_BIN")).expanduser().resolve()
        self.api_url = required_env("CURIE_API_URL").rstrip("/")
        self.api_key = required_env("CURIE_API_KEY")
        self.receipt_image = required_env("CURIE_HOOK_APPROVAL_RECEIPT_IMAGE")
        self.model = os.environ.get("CURIE_HOOK_APPROVAL_MODEL", "z-ai/glm-5.2").strip()
        try:
            self.timeout = int(
                os.environ.get("CURIE_HOOK_APPROVAL_TIMEOUT_SECONDS", "600")
            )
        except ValueError as exc:
            raise ProofError("CURIE_HOOK_APPROVAL_TIMEOUT_SECONDS must be an integer") from exc
        if self.context not in {"k8", "kind-test-2765"}:
            raise ProofError("proof context must be k8 or kind-test-2765")
        if self.namespace != "test-2765" or self.release != "test-2765":
            raise ProofError("proof namespace and release must both be test-2765")
        if not self.curie_bin.is_file() or not os.access(self.curie_bin, os.X_OK):
            raise ProofError("CURIE_BIN must name an executable file")
        if self.timeout < 60 or self.timeout > 1800:
            raise ProofError("proof timeout must be from 60 through 1800 seconds")
        if not self.model:
            raise ProofError("CURIE_HOOK_APPROVAL_MODEL must not be empty")
        parsed_url = urllib.parse.urlsplit(self.api_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ProofError("CURIE_API_URL must be an HTTP or HTTPS URL")

        self.fixture_started = False
        self.agent_create_attempted = False
        self.agent_id = ""
        self.app_name = ""
        self.fixture_pod = ""
        self.thread_key = ""
        self.platform_images: dict[str, str] = {}
        self.failure_observation: dict[str, Any] = {}
        self.cli_env = os.environ.copy()
        self.cli_env["CURIE_API_URL"] = self.api_url
        self.cli_env["CURIE_API_KEY"] = self.api_key
        self.cli_env["CURIE_NAMESPACE"] = self.namespace

    def kubectl(
        self, args: list[str], *, stdin: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return run(
            ["kubectl", "--context", self.context, "-n", self.namespace, *args],
            stdin=stdin,
            check=check,
        )

    def kubectl_json(self, args: list[str]) -> Any:
        return parse_json(self.kubectl([*args, "-o", "json"]).stdout, "kubectl")

    def cli_json(self, action: str, args: list[str]) -> Any:
        command = [
            str(self.curie_bin),
            "--json",
            "cluster",
            "--context",
            self.context,
            action,
            *args,
            "--namespace",
            self.namespace,
            "--release",
            self.release,
        ]
        return parse_json(run(command, env=self.cli_env).stdout, f"curie {action}")

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any | None = None,
        raw_body: bytes | None = None,
        headers: dict[str, str] | None = None,
        use_platform_key: bool = True,
        expected: set[int] | None = None,
    ) -> tuple[int, Any]:
        if body is not None and raw_body is not None:
            raise ProofError("request cannot carry both JSON and raw bytes")
        data = raw_body
        request_headers = dict(headers or {})
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode()
            request_headers["Content-Type"] = "application/json"
        if use_platform_key:
            request_headers["X-API-Key"] = self.api_key
        request = urllib.request.Request(
            f"{self.api_url}{path}", data=data, method=method, headers=request_headers
        )
        allowed = expected or {200}
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = int(response.status)
                payload = response.read()
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            payload = exc.read()
        except urllib.error.URLError as exc:
            raise ProofError(f"API request to {path} failed") from exc
        if status not in allowed:
            raise HttpStatus(status, path, payload)
        if not payload:
            return status, None
        try:
            return status, json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ProofError(f"API request to {path} returned invalid JSON") from exc

    def preflight(self) -> None:
        self.kubectl(["get", "namespace", self.namespace])
        for kind, name in (
            ("deployment", f"{self.release}-api"),
            ("deployment", f"{self.release}-worker"),
            ("statefulset", f"{self.release}-postgres"),
            ("statefulset", f"{self.release}-valkey"),
        ):
            self.kubectl(["get", kind, name])

        dispatchers = self.kubectl_json(
            [
                "get",
                "deployments",
                "-l",
                f"app.kubernetes.io/instance={self.release},app.kubernetes.io/component=dispatcher",
            ]
        )
        if dispatchers.get("items"):
            raise ProofError("proof release must not run a dispatcher")

        worker = self.kubectl_json(["get", "deployment", f"{self.release}-worker"])
        self.app_name = str(
            worker.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name", "")
        )
        if not self.app_name:
            raise ProofError("worker deployment has no application name label")
        containers = worker["spec"]["template"]["spec"]["containers"]
        worker_container = next(
            (item for item in containers if item.get("name") == "worker"), None
        )
        if worker_container is None:
            raise ProofError("worker deployment has no worker container")
        worker_env = {
            item.get("name"): item for item in worker_container.get("env", [])
        }
        fake_model = worker_env.get("CURIE_FAKE_MODEL", {}).get("value")
        if fake_model != "0":
            raise ProofError("proof release must disable CURIE_FAKE_MODEL")
        credentials = worker_env.get("CURIE_CREDENTIALS", {}).get("valueFrom", {})
        if not credentials.get("secretKeyRef"):
            raise ProofError("proof release must reference real model credentials")
        runner_image = worker_env.get("CURIE_RUNNER_IMAGE", {}).get("value")
        if not isinstance(runner_image, str) or not runner_image:
            raise ProofError("worker deployment does not identify the runner image")
        api = self.kubectl_json(["get", "deployment", f"{self.release}-api"])
        api_containers = api["spec"]["template"]["spec"]["containers"]
        api_container = next(
            (item for item in api_containers if item.get("name") == "api"), None
        )
        if api_container is None:
            raise ProofError("API deployment has no api container")
        self.platform_images = {
            "api": str(api_container.get("image", "")),
            "worker": str(worker_container.get("image", "")),
            "runner": runner_image,
        }
        if not all(self.platform_images.values()):
            raise ProofError("proof release has an empty candidate image reference")
        check_offline_slack_origin(worker_env)
        slack_check = self.kubectl(
            [
                "exec",
                f"deployment/{self.release}-worker",
                "-c",
                "worker",
                "--",
                "sh",
                "-c",
                'test -z "$SLACK_BOT_TOKEN"',
            ],
            check=False,
        )
        if slack_check.returncode != 0:
            raise ProofError("proof release must have an empty Slack bot token")
        offline_check = self.kubectl(
            [
                "exec",
                f"deployment/{self.release}-worker",
                "-c",
                "worker",
                "--",
                "python",
                "-c",
                (
                    "import errno,socket,sys; s=socket.socket(); s.settimeout(2); "
                    "code=s.connect_ex(('127.0.0.1',1)); s.close(); "
                    "sys.exit(0 if code == errno.ECONNREFUSED else 1)"
                ),
            ],
            check=False,
        )
        if offline_check.returncode != 0:
            raise ProofError("proof offline endpoint must refuse connections in the worker")

        for kind, name in (
            ("deployment", FIXTURE),
            ("service", FIXTURE),
            ("networkpolicy", f"{FIXTURE}-ingress"),
            ("networkpolicy", f"{FIXTURE}-runner-egress"),
        ):
            if self.kubectl(["get", kind, name], check=False).returncode == 0:
                raise ProofError(f"owned fixture object already exists: {kind}/{name}")

        _, agents = self.request("GET", "/agents")
        if any(item.get("name") == AGENT for item in agents):
            raise ProofError(f"owned proof agent already exists: {AGENT}")

    def fixture_manifest(self) -> dict[str, Any]:
        labels = {
            "app.kubernetes.io/name": FIXTURE,
            "app.kubernetes.io/instance": self.release,
            "app.kubernetes.io/component": "proof-receipt",
            "curie.dev/proof": "issue-2765",
        }
        runner_labels = {
            "app.kubernetes.io/name": self.app_name,
            "app.kubernetes.io/instance": self.release,
            "app.kubernetes.io/component": "runner-sandbox",
        }
        return {
            "apiVersion": "v1",
            "kind": "List",
            "items": [
                {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {"name": FIXTURE, "labels": labels},
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": labels},
                        "template": {
                            "metadata": {"labels": labels},
                            "spec": {
                                "automountServiceAccountToken": False,
                                "securityContext": {
                                    "runAsNonRoot": True,
                                    "seccompProfile": {"type": "RuntimeDefault"},
                                },
                                "containers": [
                                    {
                                        "name": "receipt",
                                        "image": self.receipt_image,
                                        "imagePullPolicy": "Never",
                                        "ports": [{"name": "http", "containerPort": 8000}],
                                        "readinessProbe": {
                                            "tcpSocket": {"port": "http"},
                                            "periodSeconds": 2,
                                            "failureThreshold": 30,
                                        },
                                        "livenessProbe": {
                                            "tcpSocket": {"port": "http"},
                                            "periodSeconds": 10,
                                        },
                                        "securityContext": {
                                            "allowPrivilegeEscalation": False,
                                            "readOnlyRootFilesystem": True,
                                            "capabilities": {"drop": ["ALL"]},
                                        },
                                        "resources": {
                                            "requests": {"cpu": "10m", "memory": "24Mi"},
                                            "limits": {"cpu": "100m", "memory": "64Mi"},
                                        },
                                    }
                                ],
                            },
                        },
                    },
                },
                {
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"name": FIXTURE, "labels": labels},
                    "spec": {
                        "selector": labels,
                        "ports": [{"name": "http", "port": 8000, "targetPort": "http"}],
                    },
                },
                {
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {"name": f"{FIXTURE}-ingress", "labels": labels},
                    "spec": {
                        "podSelector": {"matchLabels": labels},
                        "policyTypes": ["Ingress"],
                        "ingress": [
                            {
                                "from": [{"podSelector": {"matchLabels": runner_labels}}],
                                "ports": [{"protocol": "TCP", "port": 8000}],
                            }
                        ],
                    },
                },
                {
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {
                        "name": f"{FIXTURE}-runner-egress",
                        "labels": labels,
                    },
                    "spec": {
                        "podSelector": {"matchLabels": runner_labels},
                        "policyTypes": ["Egress"],
                        "egress": [
                            {
                                "to": [{"podSelector": {"matchLabels": labels}}],
                                "ports": [{"protocol": "TCP", "port": 8000}],
                            }
                        ],
                    },
                },
            ],
        }

    def start_fixture(self) -> None:
        self.fixture_started = True
        self.kubectl(
            ["apply", "-f", "-"],
            stdin=json.dumps(self.fixture_manifest(), separators=(",", ":")),
        )
        self.kubectl(
            [
                "rollout",
                "status",
                f"deployment/{FIXTURE}",
                "--timeout=120s",
            ]
        )
        pods = self.kubectl_json(
            ["get", "pods", "-l", f"app.kubernetes.io/name={FIXTURE}"]
        ).get("items", [])
        if len(pods) != 1:
            raise ProofError("receipt fixture must have exactly one pod")
        self.fixture_pod = pods[0]["metadata"]["name"]
        statuses = pods[0].get("status", {}).get("containerStatuses", [])
        if len(statuses) != 1 or statuses[0].get("restartCount") != 0:
            raise ProofError("receipt fixture restarted before the proof")

    def write_bundle(self, root: Path) -> None:
        plugin_dir = root / ".claude-plugin"
        plugin_dir.mkdir(parents=True)
        plugin = {
            "name": BUNDLE,
            "version": "1.0.0",
            "description": "Prove signed hook approval pause and resume.",
            "approvalPolicy": {
                "gates": [
                    {
                        "gate": RECEIPT_TOOL,
                        "route": "hook",
                        "summary": "Read the owned proof receipt after approval.",
                    }
                ]
            },
        }
        mcp = {
            "mcpServers": {
                "receipt": {
                    "type": "http",
                    "url": (
                        f"http://{FIXTURE}.{self.namespace}.svc.cluster.local:8000/mcp"
                    ),
                }
            }
        }
        (plugin_dir / "plugin.json").write_text(
            json.dumps(plugin, indent=2) + "\n", encoding="utf-8"
        )
        (root / ".mcp.json").write_text(
            json.dumps(mcp, indent=2) + "\n", encoding="utf-8"
        )

    def create_agent(self) -> dict[str, Any]:
        route = self.route_binding(with_user=True)
        self.agent_create_attempted = True
        _, agent = self.request(
            "POST",
            "/agents",
            body={
                "name": AGENT,
                "channel": {"kind": "slack", "address": CHANNEL},
                "model": self.model,
                "approval_routes": route,
            },
            expected={201},
        )
        self.agent_id = str(agent["id"])
        uuid.UUID(self.agent_id)
        if agent.get("model") != self.model:
            raise ProofError("agent did not retain the requested real model")
        return agent

    @staticmethod
    def route_binding(*, with_user: bool) -> dict[str, Any]:
        binding: dict[str, Any] = {
            "resolution": {"kind": "slack", "address": CHANNEL}
        }
        if with_user:
            binding["approvers"] = {"users": [OPERATOR]}
        return {"hook": binding}

    def update_route(self, *, with_user: bool) -> None:
        _, agent = self.request(
            "PATCH",
            f"/agents/{self.agent_id}",
            body={"approval_routes": self.route_binding(with_user=with_user)},
        )
        stored = agent.get("approval_routes", {}).get("hook", {})
        if stored.get("resolution") != {"kind": "slack", "address": CHANNEL}:
            raise ProofError("hook route did not retain its Slack resolution")
        approvers = stored.get("approvers")
        if with_user:
            if approvers != {"group": None, "users": [OPERATOR]}:
                raise ProofError("explicit route approver was not restored")
        elif approvers is not None:
            raise ProofError("route approvers were not removed")

    def deploy_bundle(self, bundle: Path) -> None:
        result = self.cli_json(
            "deploy",
            [
                "--agent",
                AGENT,
                "--plugin-dir",
                str(bundle),
                "--slack-channel",
                CHANNEL,
                "--env",
                "dev",
                "--label",
                "issue-2765-proof",
            ],
        )
        if result.get("agent", {}).get("id") != self.agent_id:
            raise ProofError("deploy targeted a different agent")
        if result.get("deployment", {}).get("status") != "active":
            raise ProofError("proof deployment is not active")

    def signer_prepare(self, payload: dict[str, Any]) -> tuple[bytes, str, str]:
        repo_root = Path(__file__).resolve().parents[3]
        api_source = str(repo_root / "apps" / "api" / "src")
        sys.path.insert(0, api_source)
        try:
            from curie_api import hook_signing
        finally:
            sys.path.remove(api_source)
        secret = hook_signing.derive(
            self.api_key, agent_id=self.agent_id, generation=0
        )
        signer_path = (
            repo_root
            / "examples"
            / "sre-bot"
            / "observability"
            / "alert-signer"
            / "server.py"
        )
        spec = importlib.util.spec_from_file_location("curie_alert_signer_proof", signer_path)
        if spec is None or spec.loader is None:
            raise ProofError("could not load the Alertmanager signer fixture")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        previous = os.environ.get("CURIE_HOOK_SECRET")
        os.environ["CURIE_HOOK_SECRET"] = secret
        try:
            body, signature, delivery = module.prepare(payload)
        finally:
            if previous is None:
                os.environ.pop("CURIE_HOOK_SECRET", None)
            else:
                os.environ["CURIE_HOOK_SECRET"] = previous
        return body, signature, delivery

    def post_hook(
        self, body: bytes, signature: str, delivery: str
    ) -> dict[str, Any]:
        path = (
            f"/hooks/{self.agent_id}/{HOOK}?"
            + urllib.parse.urlencode({"kind": "slack", "address": CHANNEL})
        )
        _, receipt = self.request(
            "POST",
            path,
            raw_body=body,
            headers={
                "Content-Type": "application/json",
                "X-Curie-Signature-256": signature,
                "X-Curie-Delivery-Id": delivery,
            },
            use_platform_key=False,
            expected={200},
        )
        required = {"event_id", "stream_id", "duplicate", "conversation_id"}
        if not isinstance(receipt, dict) or not required.issubset(receipt):
            raise ProofError("hook response is missing receipt fields")
        return receipt

    def valkey(self, *args: str) -> Any:
        command = [
            "exec",
            f"statefulset/{self.release}-valkey",
            "-c",
            "valkey",
            "--",
            "sh",
            "-c",
            'exec valkey-cli --no-auth-warning -a "$VALKEY_PASSWORD" --json "$@"',
            "sh",
            *args,
        ]
        raw = self.kubectl(command).stdout.strip()
        return parse_json(raw, "valkey-cli")

    @staticmethod
    def stream_fields(value: Any) -> dict[str, Any]:
        if isinstance(value, list) and len(value) % 2 == 0:
            return {str(value[index]): value[index + 1] for index in range(0, len(value), 2)}
        raise ProofError("Valkey XRANGE field response has an unexpected shape")

    def stream_events(self, event_id: str) -> list[tuple[str, dict[str, Any]]]:
        rows = self.valkey("XRANGE", RUNS_STREAM, "-", "+", "COUNT", "1000")
        if not isinstance(rows, list):
            raise ProofError("Valkey XRANGE response has an unexpected shape")
        matches: list[tuple[str, dict[str, Any]]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) != 2:
                raise ProofError("Valkey stream entry has an unexpected shape")
            values = self.stream_fields(row[1])
            payload = values.get("payload")
            if not isinstance(payload, str):
                continue
            decoded = parse_json(payload, "run stream payload")
            if decoded.get("event_id") == event_id:
                matches.append((str(row[0]), decoded))
        return matches

    def key_exists(self, key: str) -> bool:
        value = self.valkey("EXISTS", key)
        return int(value) == 1

    def record_failure_observation(
        self,
        approvals: list[dict[str, Any]],
        turns: list[dict[str, Any]] | None,
        *,
        receipt_count: int,
        done_marker_count: int,
        completion_hash_count: int,
    ) -> None:
        tool_names = {
            tool
            for item in approvals
            if isinstance((tool := item.get("granted_tool")), str) and tool
        }
        for turn in turns or []:
            for message in turn.get("messages", []):
                if not isinstance(message, dict):
                    continue
                for block in message.get("content", []):
                    if not isinstance(block, dict) or block.get("type") != "tool_use":
                        continue
                    name = block.get("name")
                    if isinstance(name, str) and name:
                        tool_names.add(name)
        self.failure_observation = {
            "approval_count": len(approvals),
            "approval_statuses": sorted(
                status
                for item in approvals
                if isinstance((status := item.get("status")), str)
            ),
            "completion_hash_count": completion_hash_count,
            "done_marker_count": done_marker_count,
            "receipt_count": receipt_count,
            "tool_names": sorted(tool_names),
            "transcript_count": len(turns or []),
            "transcript_statuses": [
                status
                for item in turns or []
                if isinstance((status := item.get("status")), str)
            ],
        }

    def sql_resumed(self, approval_id: str) -> str:
        uuid.UUID(approval_id)
        sql = (
            "SELECT CASE WHEN resumed_at IS NULL THEN 'null' ELSE 'set' END "
            f"FROM curie.approvals WHERE id = '{approval_id}'::uuid;"
        )
        result = self.kubectl(
            [
                "exec",
                f"statefulset/{self.release}-postgres",
                "-c",
                "postgres",
                "--",
                "sh",
                "-c",
                'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -U "$POSTGRES_USER" '
                '-d "$POSTGRES_DB" -tA -v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                sql,
            ],
            check=False,
        )
        if result.returncode != 0:
            raise ProofError("approval resumed_at SQL query failed")
        value = result.stdout.strip()
        if value not in {"null", "set"}:
            raise ProofError("approval resumed_at query returned no owned row")
        return value

    def transcript(self, conversation_id: str) -> list[dict[str, Any]] | None:
        thread_key = _thread_key(conversation_id)
        encoded_key = urllib.parse.quote(thread_key, safe="")
        try:
            _, entry = self.request(
                "GET", f"/agents/{self.agent_id}/state/transcript/{encoded_key}"
            )
        except HttpStatus as exc:
            if exc.status == 404:
                return None
            raise
        value = entry.get("value")
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ProofError("transcript state is not a turn list")
        return value

    def receipt_count(self) -> int:
        if not self.fixture_pod:
            return 0
        pod = self.kubectl_json(["get", "pod", self.fixture_pod])
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if len(statuses) != 1 or statuses[0].get("restartCount") != 0:
            raise ProofError("receipt fixture restarted during the proof")
        logs = self.kubectl(["logs", self.fixture_pod, "-c", "receipt"]).stdout
        return sum(1 for line in logs.splitlines() if line.strip() == RECEIPT_LINE)

    def list_pending_cli(self) -> dict[str, Any]:
        result = self.cli_json("approvals", [AGENT, "--list"])
        if result.get("truncated") is not False:
            raise ProofError("approval list was truncated")
        return result

    def list_agent_approvals(self) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"agent_id": self.agent_id, "limit": 200})
        _, rows = self.request("GET", f"/approvals?{query}")
        if not isinstance(rows, list):
            raise ProofError("approval API did not return a list")
        return rows

    def audit(self, approval_id: str) -> list[dict[str, Any]]:
        _, rows = self.request("GET", f"/approvals/{approval_id}/audit")
        if not isinstance(rows, list):
            raise ProofError("approval audit did not return a list")
        return rows

    def wait_until(
        self,
        description: str,
        deadline: float,
        observe: Callable[[], Any | None],
    ) -> Any:
        while time.monotonic() < deadline:
            value = observe()
            if value is not None:
                return value
            time.sleep(2)
        raise ProofError(f"timed out waiting for {description}")

    def initial_state(
        self, event_id: str, conversation_id: str, deadline: float
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        def observe() -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
            rows = self.list_agent_approvals()
            pending = [item for item in rows if item.get("status") == "pending"]
            turns = self.transcript(conversation_id)
            done_marker_count = int(self.key_exists(f"curie:worker:done:{event_id}"))
            completion_hash_count = int(
                self.key_exists(f"curie:worker:completion:{event_id}")
            )
            receipt_count = self.receipt_count()
            self.record_failure_observation(
                rows,
                turns,
                receipt_count=receipt_count,
                done_marker_count=done_marker_count,
                completion_hash_count=completion_hash_count,
            )
            if (
                len(pending) != 1
                or turns is None
                or done_marker_count != 1
                or completion_hash_count != 0
            ):
                return None
            return pending[0], turns

        approval, turns = self.wait_until(
            "the initial awaiting approval state", deadline, observe
        )
        listing = self.list_pending_cli()
        if listing.get("count") != 1 or len(listing.get("pending", [])) != 1:
            raise ProofError("curie cluster approvals --list did not return one pending row")
        listed = listing["pending"][0]
        if listed.get("id") != approval.get("id"):
            raise ProofError("CLI and API approval rows do not match")
        if approval.get("conversation_id") != conversation_id:
            raise ProofError("approval conversation does not match the hook receipt")
        if approval.get("author") != f"hook:{HOOK}":
            raise ProofError("approval author is not the signed hook")
        if approval.get("route") != "hook":
            raise ProofError("approval did not use the hook route")
        if approval.get("gate_kind") != "permission":
            raise ProofError("approval was not raised by the actionable tool permission gate")
        if approval.get("granted_tool") != RECEIPT_TOOL:
            raise ProofError("approval did not bind the receipt tool grant")
        if len(turns) != 1:
            raise ProofError("initial transcript must contain exactly one record")
        turn = turns[0]
        turn_approval = turn.get("approval")
        if (
            turn.get("type") != "turn"
            or turn.get("status") != "awaiting-approval"
            or not isinstance(turn_approval, dict)
        ):
            raise ProofError("initial transcript is not an awaiting approval turn")
        if (
            turn_approval.get("gate_kind") != "permission"
            or turn_approval.get("route") != "hook"
            or turn_approval.get("granted_tool") != RECEIPT_TOOL
        ):
            raise ProofError("initial transcript has the wrong approval context")
        if self.receipt_count() != 0:
            raise ProofError("receipt tool executed before approval")
        if self.audit(str(approval["id"])):
            raise ProofError("new approval already has audit entries")
        return approval, turns

    def mint_operator(self) -> str:
        result = self.cli_json(
            "approvals", [AGENT, "--mint-operator-principal", OPERATOR]
        )
        delivery = result.get("operator_principal", {})
        if delivery.get("subject") != OPERATOR:
            raise ProofError("operator principal subject does not match")
        token = delivery.get("token")
        if not isinstance(token, str) or not token:
            raise ProofError("operator principal token is missing")
        return token

    def resolve_api(self, approval_id: str, token: str, expected: int) -> Any:
        _, response = self.request(
            "POST",
            f"/approvals/{approval_id}/resolve",
            body={"decision": "approved"},
            headers={"X-Curie-Approval-Principal": token},
            use_platform_key=False,
            expected={expected},
        )
        return response

    def resolve_cli(self, approval_id: str, token: str) -> dict[str, Any]:
        env = self.cli_env.copy()
        env["CURIE_APPROVAL_PRINCIPAL_TOKEN"] = token
        command = [
            str(self.curie_bin),
            "--json",
            "cluster",
            "--context",
            self.context,
            "approvals",
            AGENT,
            "--resolve",
            approval_id,
            "--namespace",
            self.namespace,
            "--release",
            self.release,
        ]
        return parse_json(run(command, env=env).stdout, "curie approvals resolve")

    @staticmethod
    def assert_audit(
        rows: list[dict[str, Any]], expected_actions: list[str]
    ) -> None:
        if [row.get("action") for row in rows] != expected_actions:
            raise ProofError("approval audit actions do not match the expected sequence")
        for row in rows:
            if row.get("actor") != OPERATOR or row.get("principal_kind") != "operator":
                raise ProofError("approval audit principal does not match the operator")

    def run_proof(self) -> dict[str, Any]:
        self.preflight()
        self.start_fixture()
        if self.receipt_count() != 0:
            raise ProofError("receipt fixture did not start empty")

        with tempfile.TemporaryDirectory(prefix="curie-hook-approval-") as directory:
            os.chmod(directory, 0o700)
            bundle = Path(directory) / "bundle"
            bundle.mkdir()
            self.write_bundle(bundle)
            self.create_agent()
            self.deploy_bundle(bundle)

            run_id = uuid.uuid4().hex
            now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            tool_request = (
                f"Call {RECEIPT_TOOL}({{}}) now. "
                "Do not write narrative or call any other tool before it."
            )
            alert = {
                "version": "4",
                "groupKey": f'{{}}:{{alertname="CurieHookApprovalProof",run="{run_id}"}}',
                "truncatedAlerts": 0,
                "status": "firing",
                "receiver": "curie-hook-proof",
                "groupLabels": {"alertname": "CurieHookApprovalProof"},
                "commonLabels": {"alertname": "CurieHookApprovalProof", "severity": "test"},
                "commonAnnotations": {"summary": tool_request},
                "externalURL": "http://alertmanager.example.com",
                "alerts": [
                    {
                        "status": "firing",
                        "labels": {
                            "alertname": "CurieHookApprovalProof",
                            "severity": "test",
                        },
                        "annotations": {"summary": tool_request},
                        "startsAt": now,
                        "endsAt": "0001-01-01T00:00:00Z",
                        "generatorURL": "http://prometheus.example.com/graph",
                        "fingerprint": run_id,
                    }
                ],
            }
            body, signature, delivery = self.signer_prepare(alert)
            first = self.post_hook(body, signature, delivery)
            if first["duplicate"] is not False:
                raise ProofError("first signed hook delivery was reported as duplicate")
            event_id = str(first["event_id"])
            conversation_id = str(first["conversation_id"])
            self.thread_key = _thread_key(conversation_id)
            if not event_id or not conversation_id or not first.get("stream_id"):
                raise ProofError("first hook receipt has empty identity fields")
            original_events = self.stream_events(event_id)
            if len(original_events) != 1:
                raise ProofError("signed hook did not produce exactly one stream event")
            reply_handle = original_events[0][1].get("reply_handle")
            if not isinstance(reply_handle, dict) or (
                reply_handle.get("endpoint") is not None
                or reply_handle.get("adapter") != ROUTE_IDENTITY
            ):
                raise ProofError("signed hook did not carry the default identity's route")

            deadline = time.monotonic() + self.timeout
            approval, initial_turns = self.initial_state(
                event_id, conversation_id, deadline
            )
            approval_id = str(approval["id"])
            resume_event_id = f"approval-{approval_id}-resolved"
            if self.sql_resumed(approval_id) != "null":
                raise ProofError("pending approval already has resumed_at set")
            if self.stream_events(resume_event_id):
                raise ProofError("resume event exists before resolution")
            if self.key_exists(f"curie:worker:done:{resume_event_id}"):
                raise ProofError("resume done marker exists before resolution")

            token = self.mint_operator()
            self.update_route(with_user=False)
            self.resolve_api(approval_id, token, 403)
            denied_audit = self.audit(approval_id)
            self.assert_audit(denied_audit, ["denied"])
            denied_entry = denied_audit[0]
            if denied_entry.get("authorized") is not False:
                raise ProofError("denied audit entry was marked authorized")
            if denied_entry.get("authorizer") != "ChannelMembershipAuthorizer":
                raise ProofError("denied audit entry used the wrong authorizer")
            if denied_entry.get("evidence") != {
                "kind": "principal_set_eligibility",
                "principal_kind": "operator",
                "explicit_users_required": True,
            }:
                raise ProofError("denied audit entry has the wrong eligibility evidence")
            current = self.request("GET", f"/approvals/{approval_id}")[1]
            if current.get("status") != "pending":
                raise ProofError("denied resolution changed the approval status")
            if self.sql_resumed(approval_id) != "null":
                raise ProofError("denied resolution changed resumed_at")
            if self.stream_events(resume_event_id):
                raise ProofError("denied resolution enqueued a resume event")
            if self.key_exists(f"curie:worker:done:{resume_event_id}"):
                raise ProofError("denied resolution produced a resume done marker")
            if self.transcript(conversation_id) != initial_turns:
                raise ProofError("denied resolution changed the transcript")
            if self.receipt_count() != 0:
                raise ProofError("denied resolution executed the receipt tool")

            self.update_route(with_user=True)
            resolved = self.resolve_cli(approval_id, token).get("resolved", {})
            if resolved.get("id") != approval_id or resolved.get("status") != "approved":
                raise ProofError("CLI approval resolution did not approve the owned row")

            def observe_resume() -> list[dict[str, Any]] | None:
                events = self.stream_events(resume_event_id)
                turns = self.transcript(conversation_id)
                done_marker_count = sum(
                    int(self.key_exists(key))
                    for key in (
                        f"curie:worker:done:{event_id}",
                        f"curie:worker:done:{resume_event_id}",
                    )
                )
                completion_hash_count = int(
                    self.key_exists(f"curie:worker:completion:{event_id}")
                )
                receipt_count = self.receipt_count()
                self.record_failure_observation(
                    self.list_agent_approvals(),
                    turns,
                    receipt_count=receipt_count,
                    done_marker_count=done_marker_count,
                    completion_hash_count=completion_hash_count,
                )
                if (
                    len(events) != 1
                    or done_marker_count != 2
                    or completion_hash_count != 0
                    or self.sql_resumed(approval_id) != "set"
                    or turns is None
                    or len(turns) != 2
                    or receipt_count != 1
                ):
                    return None
                return turns

            final_turns = self.wait_until(
                "the approved resume and receipt", deadline, observe_resume
            )
            if final_turns[0] != initial_turns[0]:
                raise ProofError("approved resume rewrote the initial transcript record")
            added = final_turns[1]
            if added.get("type") != "turn" or added.get("status") != "done":
                raise ProofError("approved resume did not add one completed turn")
            if sum(turn.get("status") == "awaiting-approval" for turn in final_turns) != 1:
                raise ProofError("transcript does not contain exactly one awaiting approval turn")
            self.assert_audit(self.audit(approval_id), ["denied", "resolved"])

            duplicate = self.post_hook(body, signature, delivery)
            if duplicate.get("duplicate") is not True:
                raise ProofError("retried signed hook was not reported as duplicate")
            for field in ("event_id", "stream_id", "conversation_id"):
                if duplicate.get(field) != first.get(field):
                    raise ProofError(f"retried hook changed receipt field {field}")
            self.resolve_api(approval_id, token, 409)
            self.assert_audit(
                self.audit(approval_id), ["denied", "resolved", "race_lost"]
            )
            rows = self.list_agent_approvals()
            if len(rows) != 1 or rows[0].get("id") != approval_id:
                raise ProofError("retries changed the owned approval row count")
            if len(self.stream_events(event_id)) != 1:
                raise ProofError("retried hook added another original stream event")
            if len(self.stream_events(resume_event_id)) != 1:
                raise ProofError("retried resolution added another resume event")
            if self.transcript(conversation_id) != final_turns:
                raise ProofError("retries changed the transcript")
            if self.receipt_count() != 1:
                raise ProofError("retries executed another receipt tool call")
            if not self.key_exists(f"curie:worker:done:{event_id}"):
                raise ProofError("retries removed the original done marker")
            if self.key_exists(f"curie:worker:completion:{event_id}"):
                raise ProofError("retries recreated the settled completion hash")

            return {
                "status": "passed",
                "agent": AGENT,
                "model": self.model,
                "candidate_cli_sha256": hashlib.sha256(
                    self.curie_bin.read_bytes()
                ).hexdigest(),
                "platform_images": self.platform_images,
                "receipt_image": self.receipt_image,
                "event_id": event_id,
                "conversation_id": conversation_id,
                "approval_id": approval_id,
                "resume_event_id": resume_event_id,
                "pending_before_resolution": 1,
                "negative_resolve_status": 403,
                "resolve_retry_status": 409,
                "approval_audit_entries": 3,
                "resumed_at": "set",
                "hook_duplicate": True,
                "original_stream_events": 1,
                "resume_stream_events": 1,
                "transcript_turns": 2,
                "receipt_calls": 1,
                "completion_hash_count": 0,
            }

    def cleanup(self) -> list[str]:
        failures: list[str] = []
        if self.agent_create_attempted:
            try:
                _, agents = self.request("GET", "/agents")
                if any(item.get("name") == AGENT for item in agents):
                    if self.thread_key:
                        try:
                            reset = self.cli_json(
                                "reset-thread",
                                [AGENT, "--thread-key", self.thread_key, "--yes"],
                            )
                            if reset.get("released") is not True:
                                failures.append(f"thread/{self.thread_key}")
                        except Exception:
                            failures.append(f"thread/{self.thread_key}")
                    self.cli_json("delete", [AGENT, "--yes"])
            except Exception:
                failures.append(f"agent/{AGENT}")
        if self.fixture_started:
            objects = [
                f"deployment/{FIXTURE}",
                f"service/{FIXTURE}",
                f"networkpolicy/{FIXTURE}-ingress",
                f"networkpolicy/{FIXTURE}-runner-egress",
            ]
            result = self.kubectl(
                [
                    "delete",
                    *objects,
                    "--ignore-not-found=true",
                    "--wait=true",
                    "--timeout=60s",
                ],
                check=False,
            )
            if result.returncode != 0:
                failures.extend(objects)
            else:
                for obj in objects:
                    if self.kubectl(["get", obj], check=False).returncode == 0:
                        failures.append(obj)
        return failures


def main() -> int:
    proof: Proof | None = None
    outcome: dict[str, Any] | None = None
    error: Exception | None = None
    cleanup_failures: list[str] = []
    cleanup_raised = False
    try:
        proof = Proof()
        outcome = proof.run_proof()
    except Exception as exc:
        error = exc
    finally:
        if proof is not None:
            try:
                cleanup_failures = proof.cleanup()
            except Exception:
                cleanup_raised = True

    failed = False
    if error is not None:
        print(f"hook approval proof failed: {error}", file=sys.stderr)
        if proof is not None and proof.failure_observation:
            print(
                "hook approval proof observation: "
                + json.dumps(proof.failure_observation, sort_keys=True),
                file=sys.stderr,
            )
        failed = True
    if cleanup_failures:
        names = ", ".join(cleanup_failures)
        print(f"cleanup failed for owned resources: {names}", file=sys.stderr)
        failed = True
    if cleanup_raised:
        print("cleanup failed unexpectedly for owned resources", file=sys.stderr)
        failed = True
    if failed:
        return 1
    assert outcome is not None
    print(json.dumps(outcome, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

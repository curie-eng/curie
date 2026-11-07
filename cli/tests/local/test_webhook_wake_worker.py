"""ADR 0274 acceptance 3/4/5 worker slice on an owned kind install.

Admission is the boundary: real migrated SQL rows and real Valkey entries.
The API, worker, sandbox controller and runner use production wiring. Only
model inference uses the runner's shipped fake tier. Missing prerequisites
and teardown failures are fixture errors.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import threading
import time
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml
from aci_protocol import READER_CONTEXT, HookRunRef, QueuedTurn, TurnSource
from channel_protocol import hook_conversation_id
from curie_internal.keyspace import kill_key
from curie_worker.config import WorkerConfig

REPO = Path(__file__).resolve().parents[3]
NAME = "test-adr0274-wake-worker"
FULLNAME = f"{NAME}-curie"


class WakeStack:
    def __init__(self, root: Path):
        self.root = root
        self.env = {
            k: v
            for k, v in os.environ.items()
            if k in {"PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "SSH_AUTH_SOCK"}
        }
        self.env.update(KUBECONFIG=str(root / "kubeconfig"), CURIE_CONFIG_DIR=str(root / "config"))
        self.cli = os.environ.get("CURIE_BIN", "curie")
        self.tag = uuid.uuid4().hex[:12]
        self.cluster = f"{NAME}-{self.tag}"
        self.context = f"kind-{self.cluster}"
        self.images = {
            role: f"curie-wake-{role}:{self.tag}" for role in ("api", "worker", "runner")
        }
        self.created = False
        self.agents: dict[str, str] = {}
        self.slack_calls = []
        self.slack = None
        self.slack_thread = None

    def run(self, *args: str, stdin: str | None = None, timeout: int = 600) -> str:
        result = subprocess.run(
            args,
            input=stdin,
            text=True,
            capture_output=True,
            cwd=REPO,
            env=self.env,
            timeout=timeout,
        )
        if result.returncode:
            (self.root / "command-failure.log").write_text(result.stdout + result.stderr)
            raise RuntimeError(f"{Path(args[0]).name} {args[1]} exited {result.returncode}")
        return result.stdout

    def kube(self, *args: str, stdin: str | None = None) -> str:
        return self.run("kubectl", "--context", self.context, "-n", NAME, *args, stdin=stdin)

    def sql(self, query: str):
        raw = self.kube(
            "exec",
            f"statefulset/{FULLNAME}-postgres",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-qXAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            query,
        )
        return json.loads(raw) if raw.strip().startswith(("[", "{")) else raw.strip()

    def redis(self, *args: str):
        raw = self.kube(
            "exec",
            f"deployment/{FULLNAME}-api",
            "--",
            "python3",
            "-c",
            "import json,redis,sys;from curie_api.config import get_settings;"
            "client=redis.Redis.from_url(get_settings().valkey_dsn(),decode_responses=True);"
            "print(json.dumps(client.execute_command(*json.loads(sys.argv[1]))))",
            json.dumps(args),
        )
        return json.loads(raw)

    def api_python(self, code: str):
        return json.loads(
            self.kube("exec", f"deployment/{FULLNAME}-api", "--", "python3", "-c", code)
        )

    def start(self):
        callback = os.environ["CURIE_LOCAL_STUB_HOST"]
        calls = self.slack_calls

        class Slack(BaseHTTPRequestHandler):
            def do_POST(self):
                calls.append(self.path)
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                body = b'{"ok":true,"ts":"1"}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.slack = ThreadingHTTPServer(("0.0.0.0", 0), Slack)
        self.slack_thread = threading.Thread(target=self.slack.serve_forever, daemon=True)
        self.slack_thread.start()
        slack_url = f"http://{callback}:{self.slack.server_port}/api/"
        self.run("docker", "info", timeout=30)
        for role in ("api", "worker"):
            self.run(
                "docker",
                "build",
                "-f",
                f"apps/{role}/Dockerfile",
                "-t",
                self.images[role],
                ".",
                timeout=1800,
            )
        self.run(self.cli, "build", "--tag", self.images["runner"], timeout=1800)
        self.created = True
        self.run(
            "kind",
            "create",
            "cluster",
            "--name",
            self.cluster,
            "--image",
            "kindest/node:v1.31.0",
            "--kubeconfig",
            self.env["KUBECONFIG"],
            timeout=300,
        )
        self.run(
            "kind",
            "load",
            "docker-image",
            "--name",
            self.cluster,
            *self.images.values(),
            timeout=900,
        )
        self.kube("create", "namespace", NAME)
        values = {
            "api": {"image": self.image("api")},
            "worker": {
                "image": self.image("worker"),
                "slackApiBaseUrl": slack_url,
                "slackTrustedOrigins": "",
            },
            "ui": {"deploy": False},
            "dispatcher": {"deploy": False, "slack": {"botToken": "example-slack-token"}},
            "mailAdapter": {"deploy": False},
            "factory": {"enabled": False},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "priorityClasses": {
                role: {"name": f"{NAME}-{role}"} for role in ("platform", "sandbox")
            },
            "security": {"gvisor": {"mode": "off"}},
            "agentSandbox": {
                "poolAgents": ["acme-wake-idle", "acme-wake-gated", "acme-wake-http"],
                "runner": {
                    "image": self.images["runner"].rsplit(":", 1)[0],
                    "tag": self.tag,
                    "imagePullPolicy": "Never",
                    "prewarm": {"enabled": False},
                    "fakeModel": True,
                },
            },
        }
        path = self.root / "values.yaml"
        path.write_text(yaml.safe_dump(values))
        self.run(
            self.cli,
            "cluster",
            "up",
            "--context",
            self.context,
            "--namespace",
            NAME,
            "--release",
            NAME,
            "--chart",
            str(REPO / "charts/curie"),
            "--dev",
            "--fake-model",
            "--no-expose",
            "--forward-only",
            "--adopt",
            "-f",
            str(REPO / "charts/curie/values-dev.yaml"),
            "-f",
            str(path),
            timeout=900,
        )
        self.kube(
            "exec",
            f"deployment/{FULLNAME}-worker",
            "--",
            "python3",
            "-c",
            "import urllib.request;"
            f"urllib.request.urlopen(urllib.request.Request('{slack_url}chat.postMessage',"
            "data=b'{}')).read()",
        )
        assert self.slack_calls == ["/api/chat.postMessage"]
        self.slack_calls.clear()
        self.claim_lease_s = json.loads(
            self.kube(
                "exec",
                f"deployment/{FULLNAME}-worker",
                "--",
                "python3",
                "-c",
                "import json;from curie_worker.config import WorkerConfig;"
                "print(json.dumps(WorkerConfig().effective_hook_claim_lease_s))",
            )
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.redis("EXISTS", "curie:runs") and any(
                item["name"] == "curie-workers" for item in self.redis("XINFO GROUPS", "curie:runs")
            ):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("worker did not establish its consumer group")
        self.worker(False)
        self.deploy("acme-wake-idle")
        self.deploy("acme-wake-gated")
        self.deploy("acme-wake-http")
        self.require_http_identity()
        self.sql(
            "UPDATE curie.agents SET approval_required_tools = '[\"Bash\"]'::jsonb "
            "WHERE name = 'acme-wake-gated'"
        )

    def image(self, role):
        repository, tag = self.images[role].rsplit(":", 1)
        return {"repository": repository, "tag": tag, "pullPolicy": "Never"}

    def deploy(self, agent):
        bundle = self.root / agent
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / "skills/wake").mkdir(parents=True)
        (bundle / ".claude-plugin/plugin.json").write_text(
            json.dumps(
                {
                    "name": agent,
                    "version": "1.0.0",
                    "description": "Worker wake proof",
                    "triggers": [
                        {
                            "type": "webhook",
                            "wake": True,
                            "name": "queue-ready",
                            "path": "inbound",
                            "prompt": "Read pending items and record the result.",
                        }
                    ],
                }
            )
        )
        (bundle / "skills/wake/SKILL.md").write_text(
            "---\nname: wake\ndescription: Read pending work.\n---\nRead pending work.\n"
        )
        self.run(
            self.cli,
            "cluster",
            "deploy",
            "--context",
            self.context,
            "--namespace",
            NAME,
            "--release",
            NAME,
            "--agent",
            agent,
            "--plugin-dir",
            str(bundle),
            "--slack-channel",
            {
                "acme-wake-idle": "C0EXAMPLE1",
                "acme-wake-gated": "C0EXAMPLE2",
                "acme-wake-http": "C0EXAMPLE3",
            }[agent],
            "--label",
            "worker-proof",
            "--json",
        )
        self.agents[agent] = self.sql(
            f"SELECT json_build_object('id', id) FROM curie.agents WHERE name = '{agent}'"
        )["id"]

    def require_http_identity(self):
        agent = self.agents["acme-wake-http"]
        selection = json.dumps(
            {
                "mode": "http",
                "issuer": "https://identity.example.com",
                "lookup_url": "https://identity.example.com/v1/lookup",
                "credential_ref": {"id": "example-identity-credential", "generation": 1},
                "max_identity_age_ms": 2000,
                "max_membership_age_ms": 3000,
                "max_clock_skew_ms": 0,
            }
        )
        self.sql(
            "INSERT INTO curie.tenant_identity_providers "
            "(tenant_id, issuer, http_selection, lifecycle_generation) "
            f"SELECT tenant_id, 'https://identity.example.com', '{selection}'::jsonb, 1 "
            f"FROM curie.agents WHERE id = '{agent}'"
        )
        roles = json.dumps(
            {
                "invocation": {
                    "schema_version": 1,
                    "legacy_state": "migrated",
                    "filter": None,
                    "raw_account_revocation": "separate_removal_required",
                }
            }
        )
        self.sql(
            "INSERT INTO curie.agent_access_policies (agent_id, tenant_id, role_configuration) "
            f"SELECT id, tenant_id, '{roles}'::jsonb FROM curie.agents WHERE id = '{agent}'"
        )
        observed = self.api_python(
            "import asyncio,json,uuid;"
            "from curie_api.db import create_engine,create_sessionmaker;"
            "from curie_api.models import AgentChannel;"
            "from curie_api.identity.http_invocation import binding_requires_http_identity;"
            "from sqlalchemy import select;\n"
            "async def check():\n"
            " engine=create_engine()\n"
            " try:\n"
            "  async with create_sessionmaker(engine)() as session:\n"
            "   query=select(AgentChannel).where(\n"
            f"    AgentChannel.agent_id==uuid.UUID('{agent}'))\n"
            "   binding=await session.scalar(query)\n"
            "   print(json.dumps(await binding_requires_http_identity(session,None,binding)))\n"
            " finally: await engine.dispose()\n"
            "asyncio.run(check())"
        )
        assert observed is True

    def worker(self, enabled):
        self.kube("scale", f"deployment/{FULLNAME}-worker", f"--replicas={int(enabled)}")
        if enabled:
            self.kube("rollout", "status", f"deployment/{FULLNAME}-worker", "--timeout=180s")
        else:
            self.kube(
                "wait",
                "--for=delete",
                "pod",
                "-l",
                f"app.kubernetes.io/instance={NAME},app.kubernetes.io/component=worker",
                "--timeout=90s",
            )

    def seed(self, agent="acme-wake-idle", *, queued=True):
        agent_id = self.agents[agent]
        slot = datetime.now(UTC).isoformat()
        row = self.sql(
            "INSERT INTO curie.hook_runs "
            "(id, agent_id, name, slot_utc, version_id, source, started_at, lease_expires_at) "
            f"SELECT '{uuid.uuid4()}', a.id, 'queue-ready', '{slot}', d.version_id, 'webhook', "
            f"now(), now() + interval '{self.claim_lease_s} seconds' FROM curie.agents a "
            "JOIN curie.deployments d ON d.agent_id = a.id AND d.status = 'active' "
            f"WHERE a.id = '{agent_id}' RETURNING json_build_object('id', id)"
        )
        event = QueuedTurn(
            event_id=f"cron:{agent_id}:queue-ready:{slot}",
            conversation_id=hook_conversation_id(uuid.UUID(agent_id), "queue-ready"),
            author="cron:queue-ready",
            text="Read pending items and record the result.",
            source=TurnSource.CRON,
            reply_handle=None,
            received_at=slot,
            hook_run=HookRunRef(agent_id=agent_id, name="queue-ready", slot_utc=slot),
        )
        if queued:
            self.enqueue(event)
        return row["id"], event

    def enqueue(self, event):
        self.redis("XADD", "curie:runs", "*", "payload", event.model_dump_json())

    def wait_done(self, event):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.redis("EXISTS", WorkerConfig().done_key(event.event_id)) == 1:
                return
            time.sleep(0.1)
        raise AssertionError("worker did not settle the stream delivery")

    def transcripts(self, agent):
        return self.sql(
            "SELECT COALESCE(json_agg(value ORDER BY thread_key), '[]'::json) "
            f"FROM curie.thread_transcripts WHERE agent_id = '{self.agents[agent]}'"
        )

    def state(self, run):
        return self.sql(
            "SELECT json_build_object('outcome', outcome, 'reason', reason, "
            f"'renewed', lease_expires_at > started_at + interval '{self.claim_lease_s} seconds') "
            f"FROM curie.hook_runs WHERE id = '{run}'"
        )

    def settled(self, run, outcome, reason=None):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            state = self.state(run)
            if state["outcome"] is not None:
                assert (state["outcome"], state["reason"]) == (outcome, reason), state
                return state
            time.sleep(0.5)
        self.capture_failure()
        raise AssertionError("worker did not settle the seeded run")

    def pause(self, agent, enabled):
        agent_id = self.agents[agent]
        self.sql(
            "INSERT INTO curie.schedule_controls (agent_id, name, paused_at) "
            f"VALUES ('{agent_id}', 'queue-ready', {'now()' if enabled else 'NULL'}) "
            "ON CONFLICT (agent_id, name) DO UPDATE SET paused_at = EXCLUDED.paused_at, "
            "resume_from = CASE WHEN EXCLUDED.paused_at IS NULL THEN now() END"
        )

    def assert_agent_callers(self):
        pods = json.loads(self.kube("get", "pods", "-o", "json"))["items"]
        claims = []
        for pod in pods:
            for container in pod["spec"]["containers"]:
                for item in container.get("env", []):
                    if item["name"] != "CURIE_CONNECTOR_CALLER_TOKEN":
                        continue
                    ref = item["valueFrom"]["secretKeyRef"]
                    secret = json.loads(self.kube("get", "secret", ref["name"], "-o", "json"))
                    token = base64.b64decode(secret["data"][ref["key"]]).decode()
                    encoded = token.split(".")[1]
                    claims.append(
                        json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
                    )
        assert claims
        assert all(set(claim) == {"agent", "exp"} for claim in claims)
        assert {"acme-wake-idle", "acme-wake-http"} <= {claim["agent"] for claim in claims}

    def scheduler_tick(self):
        self.kube(
            "exec",
            f"deployment/{FULLNAME}-worker",
            "--",
            "python3",
            "-c",
            "import asyncio,os;from curie_worker.run import build;"
            "from curie_worker.config import WorkerConfig;"
            "from curie_worker.worker_lifecycle import WorkerResources;\n"
            "async def tick():\n"
            " resources=WorkerResources()\n"
            " try:\n"
            "  runtime=build(WorkerConfig(),os.environ,resources=resources)\n"
            "  await runtime.cron_loop.one_pass()\n"
            " finally: await resources.aclose()\n"
            "asyncio.run(tick())",
        )

    def capture_failure(self):
        for name, argv in (
            ("worker", ("logs", f"deployment/{FULLNAME}-worker", "--tail=150")),
            ("pods", ("get", "pods", "-o", "json")),
            ("events", ("get", "events", "-o", "json")),
        ):
            (self.root / f"failure-{name}.log").write_text(self.kube(*argv))
        (self.root / "failure-stream.log").write_text(
            json.dumps(
                {
                    "groups": self.redis("XINFO GROUPS", "curie:runs"),
                    "pending": self.redis("XPENDING", "curie:runs", "curie-workers"),
                }
            )
        )

    def close(self):
        errors = []
        if self.created:
            try:
                self.kube(
                    "delete",
                    "namespace",
                    NAME,
                    "--ignore-not-found",
                    "--wait=true",
                    "--timeout=120s",
                )
                names = json.loads(self.kube("get", "namespaces", "-o", "json"))["items"]
                assert NAME not in {item["metadata"]["name"] for item in names}
            except (RuntimeError, AssertionError, subprocess.SubprocessError) as exc:
                errors.append(str(exc))
            finally:
                self.run("kind", "delete", "cluster", "--name", self.cluster, timeout=180)
                assert self.cluster not in self.run("kind", "get", "clusters").splitlines()
                assert not self.run(
                    "docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={self.cluster}"
                ).strip()
        for image in self.images.values():
            result = subprocess.run(
                ["docker", "image", "inspect", image], env=self.env, capture_output=True
            )
            if result.returncode == 0:
                self.run("docker", "image", "rm", image)
        if self.slack is not None:
            self.slack.shutdown()
            self.slack.server_close()
        if self.slack_thread is not None:
            self.slack_thread.join(timeout=5)
            assert not self.slack_thread.is_alive()
        if errors:
            raise RuntimeError("; ".join(errors))


@pytest.fixture
def stack(tmp_path):
    runtime = WakeStack(tmp_path)
    try:
        runtime.start()
        yield runtime
    finally:
        runtime.close()


def test_webhook_wake_worker_boundary(stack):
    cfg = WorkerConfig()
    initial, event = stack.seed()
    stack.worker(True)
    assert stack.settled(initial, "ran")["renewed"]
    stack.wait_done(event)
    transcript = stack.transcripts("acme-wake-idle")
    assert transcript
    stale = event.model_copy(update={"event_id": event.event_id + ":stale"})
    stack.enqueue(stale)
    stack.wait_done(stale)
    assert stack.transcripts("acme-wake-idle") == transcript
    print("idle wake: targetless execution and explicit lease renewal")

    http_run, _ = stack.seed("acme-wake-http")
    stack.settled(http_run, "ran")
    stack.assert_agent_callers()
    print("HTTP identity binding: targetless run executes without a human")

    stack.worker(False)
    paused, old = stack.seed()
    stack.pause("acme-wake-idle", True)
    stack.worker(True)
    stack.settled(paused, "blocked", "hook_paused")
    stack.pause("acme-wake-idle", False)
    stack.enqueue(old.model_copy(update={"event_id": old.event_id + ":after-resume"}))
    fresh, _ = stack.seed()
    stack.settled(fresh, "ran")
    stack.scheduler_tick()
    assert stack.state(paused)["outcome"] == "blocked"
    print("pause: terminal blocked/hook_paused; fresh wake executes after resume")

    gated, _ = stack.seed("acme-wake-gated")
    stack.settled(gated, "failed", "approval_gate_targetless")
    assert stack.sql("SELECT count(*) FROM curie.approvals") == "0"
    print("approval: targetless refusal without card or resume")

    stack.worker(False)
    killed, _ = stack.seed()
    stack.redis("SET", kill_key(uuid.UUID(stack.agents["acme-wake-idle"])), "1")
    stack.worker(True)
    stack.settled(killed, "blocked", "agent_killed")
    assert stack.sql("SELECT count(*) FROM curie.hook_wake_keys") == "0"
    print("kill: independent of wake key metadata")

    rows = stack.redis("XRANGE", "curie:runs", "-", "+")
    events = [
        QueuedTurn.model_validate_json(fields["payload"], context=READER_CONTEXT)
        for _, fields in rows
    ]
    assert len(events) == 8
    assert all(":sweep:" not in item.event_id and item.reply_handle is None for item in events)
    assert stack.redis("SCARD", cfg.completions_pending_key()) == 0
    assert stack.sql("SELECT count(*) FROM curie.hook_runs") == "6"
    assert stack.slack_calls == []
    print("settlement: no chat, status, placeholder, sweep notice, outbox or successor")

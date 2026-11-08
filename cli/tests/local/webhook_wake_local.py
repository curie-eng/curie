"""Compose adapter for the same signed wake qualification assertions."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from test_webhook_wake_qualification import PrivateInput, Qualification
from test_webhook_wake_worker import REPO, wait_until


class LocalQualification(Qualification):
    def __init__(self, root):
        super().__init__(root)
        self.project = "test-adr0274-wake-e2e-local"
        self.network = f"{self.project}_runner"
        self.compose = []
        self.local_started = False
        self.env["COMPOSE_PARALLEL_LIMIT"] = "1"

    def kube(self, *args, stdin=None):
        if args[0] != "exec":
            raise RuntimeError("the local adapter only accepts container exec")
        separator = args.index("--")
        target = next(value for value in args[1:separator] if "/" in value)
        role = target.rsplit("-", 1)[1]
        service = {"api": "curie-api", "worker": "curie-worker", "postgres": "postgres"}[role]
        return self.run(*self.compose, "exec", "-T", service, *args[separator + 1 :], stdin=stdin)

    def start(self):
        present = self.run(
            "docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}"
        )
        if present.strip():
            raise RuntimeError("qualification Compose project exists; preserve its owner")
        sockets = []
        ports = []
        try:
            for port in range(31274, 31280):
                listener = socket.socket()
                sockets.append(listener)
                listener.bind(("127.0.0.1", port))
                ports.append(port)
        finally:
            for listener in sockets:
                listener.close()
        api, postgres, valkey, s3, console, stub = ports
        calls = self.slack_calls
        runtime = self
        callback = os.environ["CURIE_LOCAL_STUB_HOST"]
        self.effect_command = "printf executed > /tmp/curie-wake-bash-effect"
        self.model_url = f"http://{callback}:{stub}/model"

        class Slack(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if self.path.startswith("/otlp/"):
                    runtime.exports.append((self.path, payload))
                    runtime.export_event.set()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/x-protobuf")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if self.path.startswith("/model/"):
                    runtime.model_paths.append(self.path)
                    body = (
                        runtime.model_response(json.loads(payload))
                        if self.path.split("?", 1)[0].endswith("/messages")
                        else b'{"input_tokens":1}'
                    )
                    self.send_response(200)
                    self.send_header(
                        "Content-Type",
                        "text/event-stream" if body.startswith(b"event:") else "application/json",
                    )
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                calls.append(self.path)
                body = b'{"ok":true,"ts":"1"}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                return

        self.slack = ThreadingHTTPServer(("0.0.0.0", stub), Slack)
        self.slack_thread = threading.Thread(target=self.slack.serve_forever, daemon=True)
        self.slack_thread.start()
        self.env.update(
            {
                "COMPOSE_PROJECT_NAME": self.project,
                "CURIE_API_URL": f"http://127.0.0.1:{api}",
                "VALKEY_HOST": "127.0.0.1",
                "VALKEY_PORT": str(valkey),
                "S3_ENDPOINT_URL": f"http://127.0.0.1:{s3}",
                "CURIE_DOCKER_NETWORK": self.network,
                "CURIE_LOCAL_STUB_PORT": str(stub),
                "CURIE_LOCAL_POSTGRES_HOST": "127.0.0.1",
                "CURIE_LOCAL_POSTGRES_PORT": str(postgres),
                "CURIE_LOCAL_IMAGE_TAG": self.tag,
                "CURIE_FAKE_MODEL": "0",
                "CURIE_CREDENTIALS": "not-needed",
                "CURIE_MODEL_BASE_URL": self.model_url,
                "CURIE_RUNNER_IMAGE": self.images["runner"],
            }
        )
        override = self.root / "compose.override.yaml"
        override.write_text(f'''services:
  postgres:
    ports: !override ["127.0.0.1:{postgres}:5432"]
  valkey:
    ports: !override ["127.0.0.1:{valkey}:6379"]
  rustfs:
    ports: !override ["127.0.0.1:{s3}:9000", "127.0.0.1:{console}:9001"]
  curie-api:
    ports: !override ["127.0.0.1:{api}:8000"]
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: http://{callback}:{stub}/otlp
  curie-worker:
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:${{CURIE_LOCAL_POSTGRES_PASSWORD:-postgres}}@127.0.0.1:{postgres}/postgres
      VALKEY_HOST: 127.0.0.1
      VALKEY_PORT: "{valkey}"
      S3_ENDPOINT_URL: http://127.0.0.1:{s3}
      CURIE_API_URL: http://127.0.0.1:{api}
      CURIE_DOCKER_NETWORK: {self.network}
      CURIE_RUNNER_TOTAL_TIMEOUT_S: ${{CURIE_RUNNER_TOTAL_TIMEOUT_S:-600}}
      OTEL_EXPORTER_OTLP_ENDPOINT: http://{callback}:{stub}/otlp
      CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT: http://{callback}:{stub}/otlp
      SLACK_API_BASE_URL: http://{callback}:{stub}/api/
      SLACK_BOT_TOKEN: example-slack-token
networks:
  curie_runner:
    name: {self.network}
''')
        files = ["-f", str(REPO / "compose.dev.yaml"), "-f", str(override)]
        self.files = files
        self.compose = ["docker", "compose", "-p", self.project, *files]
        self.env["COMPOSE_FILE"] = os.pathsep.join([str(REPO / "compose.dev.yaml"), str(override)])
        self.run(self.cli, "build", "--tag", self.images["runner"], timeout=1800)
        self.local_started = True
        self.run(
            self.cli,
            "local",
            "up",
            "--build",
            "--minimal",
            "--project",
            self.project,
            *files,
            timeout=1800,
        )
        self.claim_lease_s = json.loads(
            self.kube(
                "exec",
                "deployment/local-worker",
                "--",
                "python3",
                "-c",
                "import json;from curie_worker.config import WorkerConfig;"
                "print(json.dumps(WorkerConfig().effective_hook_claim_lease_s))",
            )
        )
        asyncio.run(
            wait_until(
                lambda: self.redis("EXISTS", "curie:runs"),
                "worker stream",
                timeout=30,
                interval=0.1,
            )
        )
        self.worker(False)
        for agent in ("acme-wake-idle", "acme-wake-gated", "acme-wake-http"):
            self.deploy(agent)
        self.require_http_identity()
        self.sql(
            "UPDATE curie.agents SET approval_required_tools='[\"Bash\"]'::jsonb "
            "WHERE name='acme-wake-gated'"
        )

    def deploy(self, agent):
        bundle = self.root / agent
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / "skills/wake").mkdir(parents=True)
        (bundle / ".claude-plugin/plugin.json").write_text(
            json.dumps(
                {
                    "name": agent,
                    "version": "1.0.0",
                    "description": "Wake qualification",
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
        receipt = json.loads(
            self.run(
                self.cli,
                "local",
                "deploy",
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
                "wake-qualification",
                "--json",
            )
        )
        self.receipts[agent] = receipt
        self.agents[agent] = receipt["agent"]["id"]

    def worker(self, enabled):
        if enabled:
            self.run(*self.compose, "start", "--wait", "curie-worker")
        else:
            self.run(*self.compose, "stop", "curie-worker")

    def scheduler_tick(self, offset_seconds=0):
        container = self.run(*self.compose, "ps", "-aq", "curie-worker").strip()
        installed = json.loads(self.run("docker", "inspect", container))[0]
        name = f"{self.project}-scheduler"
        listing = ["docker", "ps", "-aq", "--filter", f"name=^/{name}$"]
        if self.run(*listing).strip():
            raise RuntimeError("scheduler probe already exists; preserve its owner")
        environment = installed["Config"]["Env"]
        if any("\n" in value or "\r" in value for value in environment):
            raise RuntimeError("scheduler probe requires single-line environment values")
        try:
            self.run(
                "docker",
                "run",
                "--rm",
                "--name",
                name,
                "--label",
                f"curietech.ai/stack={self.network}",
                "--network",
                installed["HostConfig"]["NetworkMode"],
                "--volumes-from",
                container,
                "--user",
                installed["Config"]["User"],
                "--workdir",
                installed["Config"]["WorkingDir"],
                "--env-file",
                "/dev/stdin",
                installed["Image"],
                "python3",
                "-c",
                self.scheduler_code(offset_seconds),
                stdin=PrivateInput("\n".join(environment) + "\n"),
            )
        finally:
            if self.run(*listing).strip():
                self.run("docker", "rm", "-f", name)
            assert not self.run(*listing).strip()

    def restart_api(self):
        container = self.run(*self.compose, "ps", "-q", "curie-api").strip()
        self.run("docker", "restart", container)
        asyncio.run(
            wait_until(
                lambda: (
                    self.run(
                        "docker", "inspect", "--format", "{{.State.Health.Status}}", container
                    ).strip()
                    == "healthy"
                ),
                "restarted API",
                timeout=90,
                interval=0.5,
            )
        )

    def local_up(self):
        self.run(
            self.cli,
            "local",
            "up",
            "--minimal",
            "--project",
            self.project,
            *self.files,
            timeout=600,
        )

    def configure_lease(self, budget, lease):
        self.env["CURIE_DELIVERY_BUDGET_S"] = str(budget)
        self.env["CURIE_RUNNER_TOTAL_TIMEOUT_S"] = str(budget)
        if lease is None:
            self.env.pop("CURIE_HOOK_CLAIM_LEASE_S", None)
        else:
            self.env["CURIE_HOOK_CLAIM_LEASE_S"] = str(lease)
        self.local_up()
        self.claim_lease_s = budget if lease is None else lease
        api = self.api_python(
            "import json;from curie_api.config import get_settings;"
            "print(json.dumps(get_settings().effective_hook_claim_lease_s))"
        )
        worker = json.loads(
            self.kube(
                "exec",
                "deployment/local-worker",
                "--",
                "python3",
                "-c",
                "import json;from curie_worker.config import WorkerConfig;"
                "print(json.dumps(WorkerConfig().effective_hook_claim_lease_s))",
            )
        )
        assert api == worker == self.claim_lease_s
        self.worker(False)

    def fault_mode(self, mode):
        return json.loads(
            self.run(
                "docker",
                "exec",
                self.proxy,
                "python3",
                "-c",
                "import json,urllib.request;"
                f"raw=json.dumps({{'mode':{mode!r}}}).encode();"
                "request=urllib.request.Request('http://127.0.0.1:8000/',data=raw);"
                "print(urllib.request.urlopen(request,timeout=10).read().decode())",
            )
        )

    @contextmanager
    def transport_fault(self):
        from pathlib import Path

        import webhook_wake_fault_proxy

        self.proxy = f"{self.project}-fault-proxy"
        if self.run("docker", "ps", "-aq", "--filter", f"name=^/{self.proxy}$").strip():
            raise RuntimeError("fault proxy already exists; preserve its owner")
        image = self.run(*self.compose, "images", "-q", "curie-api").strip()
        original = self.files[:]
        override = self.root / "compose.fault.yaml"
        override.write_text(
            f"services:\n  curie-api:\n    environment:\n      VALKEY_HOST: {self.proxy}\n"
        )
        try:
            self.run(
                "docker",
                "run",
                "-d",
                "--name",
                self.proxy,
                "--network",
                f"{self.project}_default",
                "--label",
                f"curietech.ai/stack={self.network}",
                "--env",
                "UPSTREAM_HOST=valkey",
                "--entrypoint",
                "python3",
                image,
                "-c",
                Path(webhook_wake_fault_proxy.__file__).read_text(),
            )
            self.files += ["-f", str(override)]
            self.compose += ["-f", str(override)]
            self.env["COMPOSE_FILE"] = os.pathsep.join(self.files[1::2])
            self.local_up()
            self.worker(False)
            yield
        finally:
            self.files = original
            self.compose = ["docker", "compose", "-p", self.project, *original]
            self.env["COMPOSE_FILE"] = os.pathsep.join(original[1::2])
            self.local_up()
            self.run("docker", "rm", "-f", self.proxy)
            assert not self.run("docker", "ps", "-aq", "--filter", f"name=^/{self.proxy}$").strip()

    def redeploy(self, agent, bundle, label):
        return json.loads(
            self.run(
                self.cli,
                "local",
                "deploy",
                "--agent",
                agent,
                "--plugin-dir",
                str(bundle),
                "--label",
                label,
                "--json",
            )
        )

    def run_record(self, agent, run):
        return json.loads(
            self.run(
                self.cli,
                "local",
                "hook",
                "record",
                agent,
                "queue-ready",
                run,
                "--json",
            )
        )

    def runner(self, agent):
        containers = self.run(
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=curietech.ai/stack={self.network}",
            "--filter",
            "label=curietech.ai/component=claim-runner",
            "--filter",
            f"label=curietech.ai/agent={agent}",
        ).split()
        assert len(containers) == 1
        return containers[0]

    def sandbox_manifest(self, agent):
        return json.loads(
            self.run(
                "docker",
                "exec",
                self.runner(agent),
                "python3",
                "-c",
                "import os;from pathlib import Path;"
                "print((Path(os.environ['CURIE_PLUGIN_DIR'])/'.claude-plugin/plugin.json').read_text())",
            )
        )

    def marker_value(self, agent):
        return json.loads(
            self.run(
                "docker",
                "exec",
                self.runner(agent),
                "python3",
                "-c",
                "import json;from pathlib import Path;p=Path('/tmp/curie-wake-bash-effect');"
                "print(json.dumps(p.read_text() if p.exists() else None))",
            )
        )

    def deploy_extra(self, agent, bundle):
        receipt = json.loads(
            self.run(
                self.cli,
                "local",
                "deploy",
                "--agent",
                agent,
                "--plugin-dir",
                str(bundle),
                "--slack-channel",
                f"C0EXAMPLE{len(self.agents) + 1}",
                "--label",
                "source-controls",
                "--json",
            )
        )
        self.agents[agent] = receipt["agent"]["id"]
        self.receipts[agent] = receipt

    def capture_failure(self):
        command = self.root / "command-failure.log"
        if command.exists():
            (self.root / "failure-command.log").write_text(command.read_text())
        output = self.run(*self.compose, "logs", "--no-color", "--tail", "150", "curie-worker")
        (self.root / "failure-worker.log").write_text(output)
        (self.root / "failure-state.log").write_text(
            json.dumps(
                {
                    "runs": self.sql(
                        "SELECT COALESCE(json_agg(r), '[]'::json) FROM curie.hook_runs r"
                    ),
                    "groups": self.redis("XINFO", "GROUPS", "curie:runs"),
                }
            )
        )

    def close(self):
        try:
            if self.local_started:
                try:
                    self.worker(False)
                finally:
                    try:
                        for resource, listing in (
                            ("container", ["ps", "-aq"]),
                            ("volume", ["volume", "ls", "-q"]),
                        ):
                            owned = self.run(
                                "docker",
                                *listing,
                                "--filter",
                                f"label=curietech.ai/stack={self.network}",
                            ).split()
                            for identifier in owned:
                                self.run("docker", resource, "rm", "-f", identifier)
                    finally:
                        self.run(self.cli, "local", "down", "--wipe", "--yes")
                for listing in (["ps", "-aq"], ["volume", "ls", "-q"], ["network", "ls", "-q"]):
                    remaining = self.run(
                        "docker",
                        *listing,
                        "--filter",
                        f"label=com.docker.compose.project={self.project}",
                    )
                    if remaining.strip():
                        raise RuntimeError("qualification Compose resources survived teardown")
        finally:
            self.local_started = False
            super().close()

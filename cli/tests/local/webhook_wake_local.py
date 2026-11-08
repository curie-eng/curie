"""Compose adapter for the same signed wake qualification assertions."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from test_webhook_wake_qualification import Qualification
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

        class Slack(BaseHTTPRequestHandler):
            def do_POST(self):
                calls.append(self.path)
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
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
        callback = os.environ["CURIE_LOCAL_STUB_HOST"]
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
                "CURIE_FAKE_MODEL": "1",
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
  curie-worker:
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:${{CURIE_LOCAL_POSTGRES_PASSWORD:-postgres}}@127.0.0.1:{postgres}/postgres
      VALKEY_HOST: 127.0.0.1
      VALKEY_PORT: "{valkey}"
      S3_ENDPOINT_URL: http://127.0.0.1:{s3}
      CURIE_API_URL: http://127.0.0.1:{api}
      CURIE_DOCKER_NETWORK: {self.network}
      SLACK_API_BASE_URL: http://{callback}:{stub}/api/
      SLACK_BOT_TOKEN: example-slack-token
networks:
  curie_runner:
    name: {self.network}
''')
        files = ["-f", str(REPO / "compose.dev.yaml"), "-f", str(override)]
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

    def capture_failure(self):
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

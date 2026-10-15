"""#4618: source-built quickstarts through real API storage and Helm releases.

Only the external forge HTTP peers are fixtures. No worker or model runs.
The runtime prerequisites are Docker, kind, kubectl, Helm, Cargo, git and openssl.
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlencode, urlsplit

import httpx
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LABEL = "curie-factory"
SECOND_LABEL = "acme-second-work"
FIRST_AGENT = "acme-first"
SECOND_AGENT = "acme-second"
GITHUB_REPO = "acme-corp/acme-bot"
GITLAB_REPO = "acme-corp/platform/acme-bot"
BITBUCKET_REPO = "ACME/acme-bot"
CASES = ("github", "gitlab", "jira", "jira-bitbucket-dc")

REGISTRATION_OBSERVER = """
CREATE SCHEMA test_4618;
CREATE TABLE test_4618.registration_events (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name text NOT NULL,
    transaction_id bigint NOT NULL,
    data jsonb NOT NULL
);
CREATE FUNCTION test_4618.observe_registration() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE payload jsonb;
BEGIN
    IF TG_TABLE_NAME = 'agents' THEN
        payload := jsonb_build_object('id', NEW.id, 'name', NEW.name);
    ELSE
        payload := jsonb_build_object(
            'agent_id', NEW.agent_id, 'kind', NEW.kind, 'address', NEW.address,
            'trigger_label', NEW.trigger_label
        );
    END IF;
    INSERT INTO test_4618.registration_events (table_name, transaction_id, data)
    VALUES (TG_TABLE_NAME, txid_current(), payload);
    RETURN NEW;
END;
$$;
CREATE TRIGGER test_4618_agent AFTER INSERT ON curie.agents
FOR EACH ROW EXECUTE FUNCTION test_4618.observe_registration();
CREATE TRIGGER test_4618_binding AFTER INSERT ON curie.agent_channels
FOR EACH ROW EXECUTE FUNCTION test_4618.observe_registration();
"""


class InfrastructureError(RuntimeError):
    """An unavailable prerequisite must be an error during fixture setup."""


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in list(env):
        if name.startswith(
            (
                "CURIE_",
                "ANTHROPIC_",
                "OPENROUTER_",
                "CLAUDE_",
                "GITHUB_",
                "GH_",
                "SLACK_",
            )
        ) or name in {
            "HELM_DRIVER",
            "KUBECONFIG",
        }:
            env.pop(name)
    env["HELM_DRIVER"] = "secret"
    env["NO_COLOR"] = "1"
    return env


def _redact(text: str, secrets: tuple[str, ...] = ()) -> str:
    for secret in secrets:
        text = text.replace(secret, "<generated-sentinel>")
    return re.sub(
        r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
        "<generated-private-key>",
        text,
        flags=re.DOTALL,
    )


def _command(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path,
    stdin: str | None = None,
    timeout: int = 300,
    check: bool = True,
    secrets: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        input=stdin,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode:
        # Values and Secret reads can carry fixture credentials even on error.
        detail = (
            ""
            if "get" in argv and any(x in argv for x in ("values", "secret"))
            else (_redact(result.stdout + result.stderr, secrets)[-6000:])
        )
        raise InfrastructureError(f"{argv[0]} {argv[1]} failed ({result.returncode}): {detail}")
    return result


@dataclass
class Candidate:
    directory: Path
    source: Path
    binary: Path
    image: str
    env: dict[str, str]


@pytest.fixture(scope="module")
def source_candidate(request: pytest.FixtureRequest) -> Candidate:
    env = _clean_env()
    for tool in ("docker", "kind", "kubectl", "helm", "cargo", "git", "openssl"):
        if shutil.which(tool) is None:
            raise InfrastructureError(f"#4618 runtime prerequisite missing: {tool}")
    _command(["docker", "info", "--format", "{{.ServerVersion}}"], env=env, cwd=ROOT)
    # Docker's configured engine is inherited. A verifier checkout can be in
    # /tmp, which is not shared with the remote Docker host.
    common_dir = Path(
        _command(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            env=env,
            cwd=ROOT,
        ).stdout.strip()
    )
    if not common_dir.is_absolute():
        raise InfrastructureError("git did not resolve the checkout's absolute common directory")
    shared = common_dir.parent / ".projects"
    shared.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="test-4618-", dir=shared))
    image = f"curie-test-4618-api:{directory.name}"
    image_started = False

    def cleanup() -> None:
        try:
            if image_started:
                found = _command(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                    env=env,
                    cwd=ROOT,
                    check=False,
                )
                if found.returncode == 0:
                    _command(["docker", "image", "rm", image], env=env, cwd=ROOT)
                    remaining = _command(
                        ["docker", "image", "ls", "--quiet", "--no-trunc"],
                        env=env,
                        cwd=ROOT,
                    ).stdout.split()
                    if found.stdout.strip() in remaining:
                        raise InfrastructureError("owned candidate API image survived removal")
        finally:
            shutil.rmtree(directory)

    request.addfinalizer(cleanup)
    source = directory / "source"
    shutil.copytree(
        ROOT,
        source,
        ignore=shutil.ignore_patterns(
            ".git",
            ".worktrees",
            ".projects",
            ".venv",
            "venv",
            "node_modules",
            "target",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".env",
        ),
    )
    env.update(
        {
            "CARGO_TARGET_DIR": str(directory / "cargo-target"),
            "CARGO_BUILD_JOBS": "2",
            "CURIE_CONFIG_DIR": str(directory / "curie-config"),
            "XDG_CONFIG_HOME": str(directory / "config"),
            "XDG_CACHE_HOME": str(directory / "cache"),
        }
    )
    _command(
        [
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(source / "cli/Cargo.toml"),
            "--bin",
            "curie",
        ],
        env=env,
        cwd=source,
        timeout=1800,
    )
    binary = directory / "cargo-target/debug/curie"
    _command([str(binary), "--version"], env=env, cwd=source)
    image_started = True
    _command(
        ["docker", "build", "-f", str(source / "apps/api/Dockerfile"), "-t", image, str(source)],
        env=env,
        cwd=source,
        timeout=1800,
    )
    return Candidate(directory, source, binary, image, env)


@pytest.fixture(autouse=True)
def candidate_curie(source_candidate: Candidate, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Override this directory's autouse shell double for this runtime module."""
    monkeypatch.setenv("CURIE_BIN", str(source_candidate.binary))
    return source_candidate.binary


class Forge:
    """External HTTP shapes follow the provider documents cited in dispatch."""

    def __init__(self) -> None:
        self.token = "fixture-" + uuid.uuid4().hex
        self.installation_token = "fixture-installation-" + uuid.uuid4().hex
        self.cloud = str(uuid.uuid4())
        self.bot = "acme-bot-account"
        self.starter = "acme-person-account"
        self.labels: set[str] = set()
        self.requests: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        forge = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                self.respond()

            def do_POST(self) -> None:
                self.respond()

            def respond(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length)) if length else {}
                with forge.lock:
                    status, payload = forge.dispatch(
                        self.command,
                        self.path,
                        dict(self.headers.items()),
                        body,
                    )
                raw = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        if self.thread.is_alive():
            self.server.shutdown()
            self.thread.join(timeout=10)
        self.server.server_close()

    def dispatch(
        self,
        method: str,
        raw_path: str,
        headers: dict[str, str],
        body: dict[str, Any],
    ) -> tuple[int, Any]:
        path = unquote(urlsplit(raw_path).path).rstrip("/")
        query = parse_qs(urlsplit(raw_path).query)
        headers = {key.lower(): value for key, value in headers.items()}
        self.requests.append((method, path))
        authorization = headers.get("authorization", "")
        basic = "Basic " + base64.b64encode(f"factory@example.com:{self.token}".encode()).decode()
        if path == "/_edge/tenant_info":
            return 200, {"cloudId": self.cloud}
        # Jira REST v3: serverInfo, myself and user.
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-server-info/
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-myself/
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/
        if path.startswith("/rest/api/3/"):
            if authorization != basic:
                return 401, {"errorMessages": ["fixture token required"]}
            if path.endswith("/serverInfo"):
                return 200, {"baseUrl": self.url, "deploymentType": "Cloud"}
            if path.endswith("/myself"):
                return 200, {"accountId": self.bot, "accountType": "atlassian", "active": True}
            if path.endswith("/user") and query.get("accountId") == [self.starter]:
                return 200, {"accountId": self.starter, "accountType": "atlassian", "active": True}
            return 404, {}
        # GitLab REST v4: version, token self, current user, projects, members, labels.
        # https://docs.gitlab.com/api/version/
        # https://docs.gitlab.com/api/personal_access_tokens/#get-single-personal-access-token
        # https://docs.gitlab.com/api/users/#get-the-current-user
        # https://docs.gitlab.com/api/projects/#get-a-single-project
        # https://docs.gitlab.com/api/members/
        # https://docs.gitlab.com/api/labels/
        if path.startswith("/api/v4/"):
            if headers.get("private-token") != self.token:
                return 401, {}
            if path == "/api/v4/version":
                return 200, {"version": "17.11.0"}
            if path == "/api/v4/personal_access_tokens/self":
                return 200, {
                    "active": True,
                    "revoked": False,
                    "scopes": ["api", "write_repository"],
                    "expires_at": None,
                }
            if path == "/api/v4/user":
                return 200, {"id": 42, "username": "acme-factory", "state": "active"}
            if path == f"/api/v4/projects/{GITLAB_REPO}":
                return 200, {
                    "id": 1234,
                    "path_with_namespace": GITLAB_REPO,
                    "default_branch": "main",
                    "issues_enabled": True,
                }
            if path == "/api/v4/projects/1234/members/all/42":
                return 200, {"id": 42, "access_level": 40}
            if path == "/api/v4/projects/1234/labels":
                if method == "POST":
                    name = body["name"]
                    if name in self.labels:
                        return 409, {"message": "already exists"}
                    self.labels.add(name)
                    return 201, {"id": len(self.labels), "name": name}
                return 200, [
                    {"id": i, "name": label} for i, label in enumerate(sorted(self.labels), start=1)
                ]
            return 404, {}
        # Bitbucket DC REST: application-properties and the repository GET.
        # https://developer.atlassian.com/server/bitbucket/rest/v1000/api-group-system-maintenance/
        # https://developer.atlassian.com/server/bitbucket/rest/v1000/api-group-repository/
        if path.startswith("/rest/api/latest/"):
            if authorization != f"Bearer {self.token}":
                return 401, {}
            if path.endswith("/application-properties"):
                return 200, {"version": "9.4.24"}
            if path == "/rest/api/latest/projects/ACME/repos/acme-bot":
                return 200, {"id": 1234, "slug": "acme-bot", "project": {"key": "ACME"}}
            return 404, {}
        # GitHub App authentication, installation tokens, repository metadata,
        # contents and labels. The App JWT is generated by the actual CLI.
        # https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app
        # https://docs.github.com/en/rest/apps/apps#list-installations-for-the-authenticated-app
        # https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app
        # https://docs.github.com/en/rest/apps/installations#list-repositories-accessible-to-the-app-installation
        # https://docs.github.com/en/rest/repos/repos#get-a-repository
        # https://docs.github.com/en/rest/repos/contents#get-repository-content
        # https://docs.github.com/en/rest/issues/labels
        # https://docs.github.com/en/rest/users/users#get-a-user
        # https://docs.github.com/en/rest/meta/meta#get-github-meta-information
        if path == "/users/acme-factory[bot]":
            return 200, {"id": 123, "login": "acme-factory[bot]"}
        if path == "/meta":
            return 200, {"api": ["192.0.2.0/24"]}
        if not authorization.startswith("Bearer "):
            return 401, {"message": "fixture App authentication required"}
        if path == "/app":
            return 200, {"id": 1234567, "slug": "acme-factory", "name": "acme-factory"}
        if path == "/app/installations":
            return 200, [{"id": 42, "account": {"login": "acme-corp"}}]
        if method == "POST" and path == "/app/installations/42/access_tokens":
            return 201, {"token": self.installation_token, "expires_at": "2099-01-01T00:00:00Z"}
        if path == "/installation/repositories":
            return 200, {"total_count": 1, "repositories": [{"full_name": GITHUB_REPO}]}
        if authorization != f"Bearer {self.installation_token}":
            return 401, {}
        if path == f"/repos/{GITHUB_REPO}":
            return 200, {
                "id": 987654,
                "full_name": GITHUB_REPO,
                "private": True,
                "default_branch": "main",
            }
        if path == f"/repos/{GITHUB_REPO}/commits/main":
            return 200, {"sha": "1" * 40}
        if path == f"/repos/{GITHUB_REPO}/contents":
            return 200, []
        if path == f"/repos/{GITHUB_REPO}/labels" and method == "POST":
            self.labels.add(body["name"])
            return 201, body
        prefix = f"/repos/{GITHUB_REPO}/labels/"
        if path.startswith(prefix) and path[len(prefix) :] in self.labels:
            return 200, {"name": path[len(prefix) :]}
        return 404, {"message": "Not Found"}


class Installation:
    def __init__(self, candidate: Candidate, case: str, directory: Path) -> None:
        self.candidate = candidate
        self.directory = directory
        self.case = case
        # One name per test invocation, with no reuse of another run's resources.
        self.cluster = f"test-4618-{case}-{uuid.uuid4().hex[:8]}"
        self.context = f"kind-{self.cluster}"
        self.namespace = "test-4618-initial-binding"
        self.release = "test-4618"
        self.fullname = "test-4618"
        self.env = dict(candidate.env)
        self.env.update(
            {
                "KUBECONFIG": str(directory / "kubeconfig"),
                "CURIE_BIN": str(candidate.binary),
                "CURIE_API_KEY": "fixture-api-" + uuid.uuid4().hex,
                "CURIE_CONFIG_DIR": str(directory / "curie-config"),
                "XDG_CONFIG_HOME": str(directory / "config"),
            }
        )
        self.forge = Forge()
        self.webhook_secret = "fixture-webhook-" + uuid.uuid4().hex
        self.model_credentials = "sk-ant-fixture-" + uuid.uuid4().hex
        self.env["CURIE_GITHUB_API_URL"] = self.forge.url
        self.secrets = (
            self.env["CURIE_API_KEY"],
            self.forge.token,
            self.forge.installation_token,
            self.webhook_secret,
            self.model_credentials,
        )
        self.cluster_started = False
        self.namespace_started = False
        self.unrelated: set[str] = set()
        self.sentinel_name = "test-4618-sentinel-" + uuid.uuid4().hex
        self.sentinel_label = f"curie.test.sentinel={self.sentinel_name}"
        self.sentinel_id: str | None = None
        self.http: httpx.Client | None = None

    def run(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return _command(
            list(argv),
            env=self.env,
            cwd=self.candidate.source,
            secrets=self.secrets,
            **kwargs,
        )

    def kubectl(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run("kubectl", "--context", self.context, *argv, **kwargs)

    def helm(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run(
            "helm",
            "--kube-context",
            self.context,
            "--namespace",
            self.namespace,
            *argv,
            **kwargs,
        )

    def owned_containers(self) -> set[str]:
        return set(
            self.run(
                "docker",
                "ps",
                "--all",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label=io.x-k8s.kind.cluster={self.cluster}",
            ).stdout.split()
        )

    def setup(self) -> None:
        self.unrelated = set(self.run("docker", "ps", "--quiet", "--no-trunc").stdout.split())
        chart_values = yaml.safe_load(
            (self.candidate.source / "charts/curie/values.yaml").read_text()
        )
        self.sentinel_id = self.run(
            "docker",
            "run",
            "--detach",
            "--name",
            self.sentinel_name,
            "--label",
            self.sentinel_label,
            chart_values["valkey"]["image"],
            "sleep",
            "86400",
        ).stdout.strip()
        if self.sentinel_id in self.unrelated:
            raise InfrastructureError("sentinel ownership overlaps the pre-existing inventory")
        self.forge.start()
        self.directory.joinpath("token").write_text(self.forge.token)
        pem = self.run("openssl", "genrsa", "2048").stdout
        self.directory.joinpath("app.pem").write_text(pem)
        self.directory.joinpath("app.pem").chmod(0o600)
        self.directory.joinpath("token").chmod(0o600)
        config = {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "nodes": [
                {
                    "role": "control-plane",
                    "extraPortMappings": [
                        {"containerPort": 30080, "hostPort": 0, "listenAddress": "127.0.0.1"},
                    ],
                }
            ],
        }
        config_path = self.directory / "kind.json"
        config_path.write_text(json.dumps(config))
        for _attempt in range(8):
            if self.owned_containers():
                self.cluster = f"test-4618-{self.case}-{uuid.uuid4().hex[:8]}"
                self.context = f"kind-{self.cluster}"
                continue
            self.cluster_started = True
            created = self.run(
                "kind",
                "create",
                "cluster",
                "--name",
                self.cluster,
                "--kubeconfig",
                self.env["KUBECONFIG"],
                "--config",
                str(config_path),
                "--wait",
                "180s",
                timeout=300,
                check=False,
            )
            if created.returncode == 0:
                break
            detail = created.stdout + created.stderr
            if "node(s) already exist for a cluster with the name" in detail:
                self.cluster_started = False
                self.cluster = f"test-4618-{self.case}-{uuid.uuid4().hex[:8]}"
                self.context = f"kind-{self.cluster}"
                continue
            raise InfrastructureError(
                "kind create cluster failed: " + _redact(detail, self.secrets)
            )
        else:
            raise InfrastructureError("could not allocate an unused #4618 kind cluster name")
        self.run("kind", "load", "docker-image", self.candidate.image, "--name", self.cluster)
        mapping = self.run(
            "docker",
            "port",
            f"{self.cluster}-control-plane",
            "30080/tcp",
        ).stdout.strip()
        port = int(mapping.rsplit(":", 1)[1])
        self.env["CURIE_API_URL"] = f"http://127.0.0.1:{port}"
        self.namespace_started = True
        self.kubectl("create", "namespace", self.namespace)
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "generic",
            "test-4618-app",
            "--from-file=private-key=" + str(self.directory / "app.pem"),
        )
        values = self.values()
        # Post-install migration and bucket hooks must run before API readiness.
        self.helm(
            "install",
            self.release,
            str(self.candidate.source / "charts/curie"),
            "--skip-crds",
            "-f",
            "-",
            "--timeout",
            "600s",
            stdin=json.dumps(values),
            timeout=660,
        )
        self.kubectl(
            "-n",
            self.namespace,
            "rollout",
            "status",
            f"deployment/{self.fullname}-api",
            "--timeout=240s",
            timeout=270,
        )
        self.http = httpx.Client(
            base_url=self.env["CURIE_API_URL"],
            headers={"X-API-Key": self.env["CURIE_API_KEY"]},
            timeout=30,
            trust_env=False,
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                response = self.http.get("/ready")
                if response.status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.monotonic() >= deadline:
                raise InfrastructureError("candidate API did not become ready through kind")
            time.sleep(0.25)
        response = self.http.get("/agents")
        if response.status_code != 200 or response.json() != []:
            raise InfrastructureError("candidate API did not start with empty isolated storage")
        pong = self.kubectl(
            "-n",
            self.namespace,
            "exec",
            f"statefulset/{self.fullname}-valkey",
            "--",
            "valkey-cli",
            "-a",
            "valkeypass",
            "PING",
        ).stdout.strip()
        if pong != "PONG":
            raise InfrastructureError("the real Valkey backing service did not answer PING")
        # Observe committed production writes without replacing any API or
        # storage behavior. Same transaction ids prove the initial binding was
        # created atomically, and retain evidence of a transient Slack insert.
        self.sql(REGISTRATION_OBSERVER)
        bundle = self.directory / "bundle"
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / ".claude-plugin/plugin.json").write_text(
            json.dumps(
                {
                    "name": "acme-registration",
                    "version": "0.1.0",
                    "description": "Factory registration fixture",
                }
            )
        )
        (bundle / "AGENTS.md").write_text("Persist this bundle without starting a worker turn.\n")

    def values(self) -> dict[str, Any]:
        credential = {
            "type": "github_app",
            "appId": "1234567",
            "existingSecret": "test-4618-app",
            "existingSecretKey": "private-key",
        }
        host = {
            "name": "github",
            "kind": "github",
            "host": urlsplit(self.forge.url).netloc,
            "apiUrl": self.forge.url,
            "credential": credential,
            "egress": [
                {
                    "cidr": "127.0.0.1/32",
                    "ports": [
                        {
                            "protocol": "TCP",
                            "port": self.forge.server.server_port,
                        }
                    ],
                }
            ],
        }
        repository = self.candidate.image.rsplit(":", 1)[0]
        tag = self.candidate.image.rsplit(":", 1)[1]
        return {
            "fullnameOverride": self.fullname,
            "api": {
                "image": {
                    "repository": repository,
                    "tag": tag,
                    "digest": "",
                    "pullPolicy": "Never",
                },
                "apiKey": self.env["CURIE_API_KEY"],
                "githubApiUrl": self.forge.url,
                "githubCloneBase": self.forge.url,
                "githubWebhookSecret": self.webhook_secret,
                "githubAppId": "1234567",
                "githubAppPrivateKeyExistingSecret": "test-4618-app",
                "githubAppPrivateKeyExistingSecretKey": "private-key",
                "githubRepoAllowlist": [GITHUB_REPO],
                "commitPollIntervalSeconds": 0,
                "service": {"type": "NodePort", "port": 8000, "nodePort": 30080},
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"cpu": "1", "memory": "1Gi"},
                },
            },
            "factory": {
                "enabled": True,
                "intake": "poll",
                "codeHosts": [host],
                "trackers": [dict(host, label=DEFAULT_LABEL, mention="acme-factory")],
                "bindings": [],
            },
            "worker": {"deploy": False},
            "dispatcher": {"deploy": False},
            "ui": {"deploy": False},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "postgres": {"persistence": {"enabled": False}},
            "valkey": {"persistence": {"enabled": False}},
            "rustfs": {
                "persistence": {"enabled": False},
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "1", "memory": "2Gi"},
                },
            },
            "agentSandbox": {
                "deploy": False,
                "controller": {"deploy": False},
                "runner": {
                    "fakeModel": False,
                    "model": "fixture-no-execution",
                    "credentials": self.model_credentials,
                    "prewarm": {"enabled": False},
                },
            },
            "priorityClasses": {
                "platform": {"create": True, "name": "test-4618-platform"},
                "sandbox": {"create": True, "name": "test-4618-sandbox"},
            },
            "security": {"allowDevDefaults": True, "gvisor": {"mode": "off"}},
        }

    def close(self) -> None:
        def verify_namespace() -> None:
            namespaces = json.loads(self.kubectl("get", "namespaces", "-o", "json").stdout)
            if any(x["metadata"]["name"] == self.namespace for x in namespaces["items"]):
                raise InfrastructureError("owned test namespace survived deletion")

        def delete_priority_class(name: str) -> None:
            self.kubectl("delete", "priorityclass", name, "--ignore-not-found=true")

        def verify_priority_class(name: str) -> None:
            if self.kubectl(
                "get",
                "priorityclass",
                name,
                "--ignore-not-found=true",
                "-o",
                "name",
            ).stdout.strip():
                raise InfrastructureError("owned PriorityClass survived deletion")

        def verify_cluster() -> None:
            if self.owned_containers():
                raise InfrastructureError("owned kind containers survived deletion")
            if self.cluster in self.run("kind", "get", "clusters").stdout.split():
                raise InfrastructureError("owned kind cluster survived deletion")

        def verify_sentinel() -> None:
            if self.sentinel_id is not None:
                running = self.run(
                    "docker",
                    "inspect",
                    "--format",
                    "{{.State.Running}}",
                    self.sentinel_id,
                ).stdout.strip()
                if running != "true":
                    raise InfrastructureError("unrelated sentinel did not survive kind cleanup")

        def sentinel_containers() -> list[str]:
            return self.run(
                "docker",
                "ps",
                "--all",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label={self.sentinel_label}",
            ).stdout.split()

        def delete_sentinel() -> None:
            owned = sentinel_containers()
            if owned:
                self.run("docker", "rm", "--force", *owned)

        def verify_sentinel_removed() -> None:
            if sentinel_containers():
                raise InfrastructureError("owned sentinel survived removal")

        operations: list[Callable[[], Any]] = []
        if self.http is not None:
            operations.append(self.http.close)
        operations.append(self.forge.close)
        if self.cluster_started:
            if self.namespace_started:
                operations.extend(
                    [
                        lambda: self.kubectl(
                            "delete",
                            "namespace",
                            self.namespace,
                            "--ignore-not-found=true",
                            "--wait=true",
                            "--timeout=120s",
                            timeout=150,
                        ),
                        verify_namespace,
                    ]
                )
                for name in ("test-4618-platform", "test-4618-sandbox"):
                    operations.extend(
                        [
                            partial(delete_priority_class, name),
                            partial(verify_priority_class, name),
                        ]
                    )
            operations.extend(
                [
                    lambda: self.run(
                        "kind", "delete", "cluster", "--name", self.cluster, timeout=180
                    ),
                    verify_cluster,
                ]
            )
        operations.extend([verify_sentinel, delete_sentinel, verify_sentinel_removed])
        # ExitStack attempts every registered cleanup even when an earlier
        # deletion or preservation check raises.
        with ExitStack() as cleanup:
            for operation in reversed(operations):
                cleanup.callback(operation)

    def quickstart(
        self,
        case: str,
        agent: str,
        label: str | None,
    ) -> subprocess.CompletedProcess[str]:
        repo = {"gitlab": GITLAB_REPO, "jira-bitbucket-dc": BITBUCKET_REPO}.get(case, GITHUB_REPO)
        args = [
            str(self.candidate.binary),
            "--json",
            "factory",
            "quickstart",
            "--context",
            self.context,
            "--namespace",
            self.namespace,
            "--release",
            self.release,
            "--repo",
            repo,
            "--agent",
            agent,
            "--plugin-dir",
            str(self.directory / "bundle"),
            "--chart",
            str(self.candidate.source / "charts/curie"),
        ]
        if label is not None:
            args += ["--label", label]
        if case == "gitlab":
            args += [
                "--code-host",
                "gitlab",
                "--tracker",
                "gitlab",
                "--token-file",
                str(self.directory / "token"),
                "--forge-url",
                self.forge.url,
            ]
        elif case in {"jira", "jira-bitbucket-dc"}:
            args += [
                "--tracker",
                "jira-cloud",
                "--forge-username",
                "factory@example.com",
                "--start-account",
                self.forge.starter,
            ]
            if case == "jira-bitbucket-dc":
                args += [
                    "--code-host",
                    "bitbucket-dc",
                    "--forge-url",
                    self.forge.url,
                    "--token-file",
                    str(self.directory / "token"),
                    "--review-account",
                    "42",
                    "--tracker-url",
                    self.forge.url,
                    "--tracker-token-file",
                    str(self.directory / "token"),
                ]
            else:
                args += [
                    "--code-host",
                    "github",
                    "--forge-url",
                    self.forge.url,
                    "--token-file",
                    str(self.directory / "token"),
                ]
        return self.run(*args, check=False, timeout=900)

    def api(self, method: str, path: str, body: Any = None) -> httpx.Response:
        if self.http is None:
            raise InfrastructureError("API client is not initialized")
        return (
            self.http.request(method, path, json=body)
            if body is not None
            else (self.http.request(method, path))
        )

    def read(self, path: str) -> Any:
        response = self.api("GET", path)
        response.raise_for_status()
        return response.json()

    def sql(self, statement: str) -> str:
        return self.kubectl(
            "-n",
            self.namespace,
            "exec",
            "-i",
            f"statefulset/{self.fullname}-postgres",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-qAt",
            stdin=statement,
        ).stdout.strip()

    def registration_events(self, agent_id: str) -> list[dict[str, Any]]:
        identity = str(uuid.UUID(agent_id))
        events = json.loads(
            self.sql(f"""
            SELECT COALESCE(jsonb_agg(to_jsonb(event) ORDER BY sequence), '[]'::jsonb)
            FROM test_4618.registration_events event
            WHERE data->>'id' = '{identity}' OR data->>'agent_id' = '{identity}';
        """)
        )
        if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
            raise InfrastructureError(
                "registration observer did not return a list of event objects"
            )
        return cast(list[dict[str, Any]], events)

    def release_values(self) -> dict[str, Any]:
        values = json.loads(self.helm("get", "values", self.release, "-o", "json").stdout)
        if not isinstance(values, dict):
            raise InfrastructureError("Helm did not return a release values object")
        return values

    def tracker_label(self, kind: str) -> str:
        tracker_kind = "jira_cloud" if kind == "jira" else kind
        label = next(
            entry["label"]
            for entry in self.release_values()["factory"]["trackers"]
            if entry["kind"] == tracker_kind
        )
        if not isinstance(label, str):
            raise InfrastructureError("the release tracker label is not a string")
        return label


@pytest.fixture
def installation(
    request: pytest.FixtureRequest,
    source_candidate: Candidate,
) -> Installation:
    case = str(request.param)
    directory = Path(tempfile.mkdtemp(prefix=f"{case}-", dir=source_candidate.directory))
    instance = Installation(source_candidate, case, directory)
    request.addfinalizer(instance.close)
    instance.setup()
    return instance


def _succeeded(installation: Installation, result: subprocess.CompletedProcess[str]) -> None:
    assert "loaded from Curie private storage" not in result.stderr
    assert result.returncode == 0, _redact(result.stdout + result.stderr, installation.secrets)


def _agent(installation: Installation, name: str) -> dict[str, Any]:
    matches = [agent for agent in installation.read("/agents") if agent["name"] == name]
    assert len(matches) == 1, f"expected exactly one persisted agent named {name}"
    agent = matches[0]
    assert isinstance(agent, dict), "the persisted agent must be an object"
    return agent


def _deployment(installation: Installation, agent: dict[str, Any]) -> dict[str, Any]:
    deployments = installation.read("/deployments?" + urlencode({"agent_id": agent["id"]}))
    active = [item for item in deployments if item["status"] == "active"]
    assert active, "expected a persisted active deployment"
    # curie_api.crud.deployments.get_active_deployment selects the newest
    # active row; earlier active deployments remain in history.
    current = max(active, key=lambda item: datetime.fromisoformat(item["deployed_at"]))
    assert current["environment"] == "prod"
    versions = installation.read(f"/agents/{agent['id']}/versions")
    version = next(item for item in versions if item["id"] == current["version_id"])
    assert version["bundle_ref"]
    assert version["bundle_sha256"]
    # This endpoint reads and unpacks the archive from real S3 storage.
    files = installation.read(f"/agents/{agent['id']}/versions/{version['id']}/files")["files"]
    manifest = next(item for item in files if item["path"] == ".claude-plugin/plugin.json")
    assert json.loads(manifest["content"])["name"] == "acme-registration"
    return {
        "deployment": current,
        "history": sorted(deployments, key=lambda item: item["id"]),
        "digest": version["bundle_sha256"],
    }


def _surface(case: str) -> tuple[str, str]:
    if case == "gitlab":
        return "gitlab", GITLAB_REPO
    if case in {"jira", "jira-bitbucket-dc"}:
        return "jira", "jira"
    return "github", GITHUB_REPO


def _assert_binding(
    installation: Installation,
    agent: dict[str, Any],
    case: str,
    label: str,
    *,
    tracker_default: str | None = DEFAULT_LABEL,
) -> None:
    kind, address = _surface(case)
    assert len(agent["channels"]) == 1, "factory registration retained an implicit Slack route"
    channel = agent["channels"][0]
    assert (channel["kind"], channel["address"]) == (kind, address)
    assert channel["trigger_label"] == (None if label == DEFAULT_LABEL else label)
    if tracker_default is not None:
        assert installation.tracker_label(kind) == tracker_default
    effective = channel["trigger_label"] or installation.tracker_label(kind)
    assert effective == label
    events = installation.registration_events(agent["id"])
    creations = [event for event in events if event["table_name"] == "agents"]
    bindings = [event for event in events if event["table_name"] == "agent_channels"]
    assert len(creations) == 1
    assert len(bindings) == 1, "registration inserted a temporary or duplicate surface"
    assert bindings[0]["transaction_id"] == creations[0]["transaction_id"]
    assert bindings[0]["data"] == {
        "agent_id": agent["id"],
        "kind": kind,
        "address": address,
        "trigger_label": channel["trigger_label"],
    }


@pytest.mark.parametrize("installation", CASES, indirect=True)
def test_second_factory_agent_uses_initial_tracker_binding(installation: Installation) -> None:
    case = installation.case
    first_run = installation.quickstart(case, FIRST_AGENT, None)
    _succeeded(installation, first_run)
    first = _agent(installation, FIRST_AGENT)
    first_deployment = _deployment(installation, first)
    first_release = installation.release_values()
    # Reach the original second-agent 409 before checking the first agent's
    # surface shape. The baseline must fail for the reported creation collision.
    second_run = installation.quickstart(case, SECOND_AGENT, SECOND_LABEL)
    _succeeded(installation, second_run)
    second = _agent(installation, SECOND_AGENT)
    first_after = _agent(installation, FIRST_AGENT)
    assert second["id"] != first["id"]
    assert first_after == first
    assert _deployment(installation, first_after) == first_deployment
    _deployment(installation, second)
    _assert_binding(installation, first_after, case, DEFAULT_LABEL)
    _assert_binding(installation, second, case, SECOND_LABEL)
    first_output = json.loads(first_run.stdout)
    second_output = json.loads(second_run.stdout)
    assert first_output["trigger_label"] is None
    assert first_output["effective_label"] == DEFAULT_LABEL
    assert second_output["trigger_label"] == SECOND_LABEL
    assert second_output["effective_label"] == SECOND_LABEL
    release_after = installation.release_values()
    for binding in first_release["factory"]["bindings"]:
        assert binding in release_after["factory"]["bindings"]
    for key, value in first_release["factory"].get("sandboxEgress", {}).items():
        assert release_after["factory"]["sandboxEgress"][key] == value
    assert len(installation.read("/agents")) == 2


@pytest.mark.parametrize("installation", CASES, indirect=True)
def test_quickstarts_are_idempotent_and_binding_guards_hold(installation: Installation) -> None:
    case = installation.case
    _succeeded(installation, installation.quickstart(case, FIRST_AGENT, None))
    _succeeded(installation, installation.quickstart(case, SECOND_AGENT, SECOND_LABEL))
    first = _agent(installation, FIRST_AGENT)
    second = _agent(installation, SECOND_AGENT)
    snapshots = {agent["name"]: _deployment(installation, agent) for agent in (first, second)}
    for name, label, expected in ((FIRST_AGENT, None, first), (SECOND_AGENT, SECOND_LABEL, second)):
        other = second if name == FIRST_AGENT else first
        other_deployment = _deployment(installation, other)
        rerun = installation.quickstart(case, name, label)
        _succeeded(installation, rerun)
        output = json.loads(rerun.stdout)
        assert output["trigger_label"] == label
        assert output["effective_label"] == (label or DEFAULT_LABEL)
        actual = _agent(installation, name)
        assert actual == expected
        assert _deployment(installation, actual)["digest"] == snapshots[name]["digest"]
        assert _agent(installation, other["name"]) == other
        assert _deployment(installation, other) == other_deployment
        _assert_binding(installation, _agent(installation, FIRST_AGENT), case, DEFAULT_LABEL)
        _assert_binding(installation, _agent(installation, SECOND_AGENT), case, SECOND_LABEL)
        assert len(installation.read("/agents")) == 2

    kind, address = _surface(case)
    tracker_label = installation.tracker_label(kind)
    for index, label in enumerate((DEFAULT_LABEL, SECOND_LABEL, None)):
        # Null vs explicit tracker default must have the same conflict outcome.
        channel = {"kind": kind, "address": address, "trigger_label": label}
        before = installation.read("/agents")
        refused = installation.api(
            "POST",
            "/agents",
            {
                "name": f"acme-duplicate-{index}",
                "channel": channel,
            },
        )
        assert refused.status_code == 409, (label, tracker_label, refused.text)
        assert installation.read("/agents") == before

    slack_channel = {"kind": "slack", "address": "C0EXAMPLE1"}
    created = installation.api("POST", "/agents", {"name": "acme-chat", "channel": slack_channel})
    assert created.status_code == 201, created.text
    chat = created.json()
    before = installation.read("/agents")
    duplicate = installation.api(
        "POST",
        "/agents",
        {
            "name": "acme-chat-duplicate",
            "channel": slack_channel,
        },
    )
    assert duplicate.status_code == 409, duplicate.text
    assert installation.read("/agents") == before
    for agent, route in ((first, {"kind": kind, "address": address}), (chat, slack_channel)):
        refused = installation.api(
            "DELETE",
            f"/agents/{agent['id']}/channels?" + urlencode(route),
        )
        assert refused.status_code == 409, refused.text
        assert "last binding" in refused.json()["detail"]
        assert _agent(installation, agent["name"]) == agent
    _assert_binding(installation, _agent(installation, FIRST_AGENT), case, DEFAULT_LABEL)
    _assert_binding(installation, _agent(installation, SECOND_AGENT), case, SECOND_LABEL)


@pytest.mark.parametrize("installation", ("github",), indirect=True)
def test_mixed_trackers_preserve_the_first_deployment(installation: Installation) -> None:
    _succeeded(installation, installation.quickstart("github", FIRST_AGENT, None))
    first = _agent(installation, FIRST_AGENT)
    deployed = _deployment(installation, first)
    tracker_defaults = {"github": DEFAULT_LABEL}
    for case, name in (("gitlab", "acme-gitlab"), ("jira", "acme-jira")):
        _succeeded(installation, installation.quickstart(case, name, f"{name}-work"))
        _assert_binding(
            installation, _agent(installation, name), case, f"{name}-work", tracker_default=None
        )
        for kind, expected in tracker_defaults.items():
            assert installation.tracker_label(kind) == expected
        kind, _address = _surface(case)
        tracker_defaults[kind] = installation.tracker_label(kind)
        _deployment(installation, _agent(installation, name))
        assert _agent(installation, FIRST_AGENT) == first
        assert _deployment(installation, first) == deployed
        _assert_binding(installation, first, "github", DEFAULT_LABEL)
    agents = installation.read("/agents")
    assert len({agent["id"] for agent in agents}) == 3
    assert all(channel["kind"] != "slack" for agent in agents for channel in agent["channels"])

"""Run the real factory on an owned kind cluster with recorded external seams."""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import math
import os
import random
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import factory_e2e as fe
import scripted_scenario as scenario

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tools/factory-e2e/fixtures/unitconv"
ISSUE = REPO_ROOT / "tools/factory-e2e/fixtures/unitconv-issue.md"
TRANSCRIPT = REPO_ROOT / "tools/model-script/transcripts/unitconv-issue.json"
IMAGES = ("curie-api:local", "curie-worker:local", "curie-ui:local", "curie-runner:latest")
BOUND_SECONDS = 2400
RECORD_BUDGET_USD = 20
RECORD_REQUEST_RESERVE_USD = 2.0
RECORD_ATTEMPTS = 3


class ScriptedScenarioError(RuntimeError):
    """The disposable scenario failed to prove its protected facts."""


def _module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise fe.ConfigError(f"missing scripted dependency: {path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_configuration(args: argparse.Namespace) -> None:
    """Refuse malformed inputs before any external resource is created."""

    if args.record:
        if not os.environ.get("CURIE_FACTORY_RECORD_KEY"):
            raise fe.ConfigError("recording requires CURIE_FACTORY_RECORD_KEY")
    elif not Path(args.transcript).is_file():
        raise fe.ConfigError(f"transcript {args.transcript} is missing; use scripted --record")
    else:
        loader = Path(__file__).resolve().parents[2] / "tools/model-script/model_script.py"
        try:
            _module("factory_model_script_check", loader).load_transcript(Path(args.transcript))
        except (OSError, ValueError) as exc:
            raise fe.ConfigError(f"transcript {args.transcript} is malformed: {exc}") from None


def scripted_values(
    *,
    model_base_url: str,
    github_api: str,
    clone_base: str,
    ca_configmap: str,
    runner_image: str,
) -> dict[str, Any]:
    """Candidate chart values using only the disposable external endpoints."""

    config = fe.FactoryConfig(
        kube_context="kind-local",
        app_id="51",
        installation_id=5501,
        private_key_file=Path("app.pem"),
        repo="acme-corp/acme-bot",
        label="factory",
        mention="curie-factory-bot",
        cloudflared="unused",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="example-scripted-webhook-secret",
        actor_token="example-scripted",
        model=fe.DEFAULT_MODEL,
        model_context_tokens=None,
        model_base_url=model_base_url,
    )
    values = fe.install_values(
        config,
        candidate="local",
        app_key_secret="factory-app",
        consumer_controller=False,
        local_images=True,
    )
    values["api"].update(
        githubApiUrl=github_api,
        githubCloneBase=clone_base,
        githubStubCaConfigMap=ca_configmap,
        githubFactoryPollIntervalSeconds=5,
    )
    values["codeHostTrust"] = {
        "caBundle": {"configMapRef": {"name": ca_configmap, "key": "ca.pem"}}
    }
    values["dispatcher"]["deploy"] = False
    values["mailAdapter"]["deploy"] = False
    # Match the documented minimal backing posture in upgrade-drill: factory
    # observations use Postgres and the stub, while optional trace stores add
    # several gigabytes of image data without participating in these facts.
    # API, worker, UI, Postgres, Valkey and S3 remain the real chart workloads.
    values["langfuse"] = {"deploy": False, "host": "langfuse.example.com"}
    values["clickhouse"] = {"deploy": False}
    values["otelCollector"] = {"deploy": False, "telemetryDisabled": True}
    values["agentSandbox"]["deploy"] = True
    # The platform runner and the bundle layer both live in the owned registry
    # and are pulled through the node's configured mirror. Naming the runner by
    # its registry lets `curie cluster deploy` prove the layer's base.
    values["agentSandbox"]["runner"]["image"] = runner_image
    values["agentSandbox"]["runner"]["prewarm"] = {"imagePullPolicy": "IfNotPresent"}
    values["agentSandbox"]["runner"]["imagePullPolicy"] = "IfNotPresent"
    allowed = values["security"]["networkPolicy"]["allowedEgress"]
    allowed.append(fe.model_proxy_egress(clone_base))
    return values


def assert_observed(preflight_outcome: str, publication_paths: list[str]) -> None:
    scenario.assert_preflight_outcome(preflight_outcome)
    scenario.assert_publication_paths(publication_paths)


def verdict(preflight_outcome: str, publication_paths: list[str]) -> int:
    try:
        assert_observed(preflight_outcome, publication_paths)
    except scenario.ScenarioAssertionError:
        return fe.EXIT_FAILED
    return 0


def free_registry_port() -> int:
    """Return a free loopback port below the kernel's ephemeral range."""

    for port in random.sample(range(20000, 32000), 50):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise ScriptedScenarioError("no free loopback port for the run registry")


class ScriptedRun:
    """One private cluster, registry, fixture server and model endpoint."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.run_id = uuid.uuid4().hex[:8]
        self.cluster = f"curie-factory-scripted-{self.run_id}"
        self.context = f"kind-{self.cluster}"
        self.registry = f"{self.cluster}-registry"
        self.namespace = "curie"
        self.evidence_path = REPO_ROOT / f"target/factory-e2e/scripted-{self.run_id}.json"
        self.evidence: dict[str, Any] = {
            "result": "failed",
            "run_id": self.run_id,
            "cluster": self.cluster,
            "registry": self.registry,
            "preflight_outcome": None,
            "changed_paths": [],
            "pull_request": None,
            "final_comment": None,
            "unexpected_model_requests": [],
            "unsupported_stub_requests": [],
        }
        self.workdir: Path | None = None
        self.stub: Any = None
        self.model: Any = None
        self.forward: subprocess.Popen[str] | None = None
        self.cluster_attempted = False
        self.registry_attempted = False
        self.registry_ref: str | None = None
        self.install_started: float | None = None
        self.deadline: float | None = None
        self.secrets = [os.environ.get("CURIE_FACTORY_RECORD_KEY")]
        self.env = dict(os.environ)
        # Only the in-process recording proxy receives the upstream credential.
        self.env.pop("CURIE_FACTORY_RECORD_KEY", None)
        self.env["BUILDX_BUILDER"] = "default"
        # An inherited remote context would split kind, docker and buildx
        # across engines. The default context honours an inherited DOCKER_HOST.
        self.env["DOCKER_CONTEXT"] = "default"
        self.curie = os.environ.get("CURIE_BIN", "curie")
        self.record_start_remaining: float | None = None
        self.record_budget_start: float | None = None
        self.record_ledger: Path | None = None
        self.record_namespace = os.environ.get("FACTORY_RUN_ID", self.run_id)
        self.record_error: str | None = None
        self.admission_lock = threading.Lock()
        # Serializes this run's admission plus upstream forward; the ledger
        # file lock extends that to every run sharing the ledger.
        self.forward_lock = threading.Lock()
        # Set when teardown begins; a request still waiting on either lock is
        # then refused, since cleanup would never measure the credit it spends.
        self.recording_closed = False

    def check_disk(self, stage: str) -> None:
        if shutil.disk_usage("/").free < 30 * 1024**3:
            raise fe.ConfigError(f"{stage} requires at least 30 GB free disk")

    def command(self, *argv: str, timeout: float = 900) -> str:
        # Print only the executable and operation. Remaining arguments can
        # contain private paths or credential inputs, so never render them.
        operation = " ".join((Path(argv[0]).name, argv[1]))
        started = time.monotonic()
        print(f"factory-e2e [{self.run_id}]: starting {operation}", file=sys.stderr, flush=True)
        if self.deadline is not None:
            timeout = min(timeout, max(0.1, self.deadline - time.monotonic()))
        result = subprocess.run(
            argv,
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        print(
            f"factory-e2e [{self.run_id}]: finished {operation} "
            f"in {time.monotonic() - started:.1f}s (exit {result.returncode})",
            file=sys.stderr,
            flush=True,
        )
        if result.returncode:
            tail, _ = fe.redact_agent_text((result.stderr or result.stdout)[-2000:], self.secrets)
            raise ScriptedScenarioError(f"{argv[0]} {argv[1]} failed: {tail}")
        return result.stdout

    def kubectl(self, *argv: str, timeout: float = 300) -> str:
        return self.command("kubectl", "--context", self.context, *argv, timeout=timeout)

    def provider_remaining(self) -> float:
        """Read the provider's remaining limit without retaining key material."""

        key = os.environ["CURIE_FACTORY_RECORD_KEY"]
        request = Request(fe.OPENROUTER_KEY_URL, headers={"Authorization": f"Bearer {key}"})
        try:
            with urlopen(request, timeout=30) as response:
                data = json.load(response)["data"]
            remaining = float(data["limit_remaining"])
            if not math.isfinite(remaining) or remaining < 0:
                raise ValueError("invalid remaining limit")
            return remaining
        except (OSError, ValueError, KeyError, TypeError):
            raise fe.ConfigError("provider credit could not be verified") from None

    def prepare_recording(self) -> None:
        """Share the credit budget across worktrees and scope attempts to a run."""

        remaining = self.provider_remaining()
        self.record_start_remaining = remaining
        self.evidence.update(
            recording_run_id=self.record_namespace,
            recording_start_remaining_usd=remaining,
            recording_model_calls=0,
        )
        common_dir = self.command(
            "git", "rev-parse", "--path-format=absolute", "--git-common-dir", timeout=30
        ).strip()
        repository = hashlib.sha256(str(Path(common_dir).resolve()).encode()).hexdigest()
        state = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
        ledger = state / "curie-factory-scripted" / repository / "recording.sqlite3"
        ledger.parent.mkdir(parents=True, exist_ok=True)
        try:
            baseline = float(os.environ.get("CURIE_FACTORY_RECORD_BASELINE_REMAINING", remaining))
        except ValueError:
            raise fe.ConfigError("recording baseline must be a USD amount") from None
        if not math.isfinite(baseline) or baseline < RECORD_BUDGET_USD:
            raise fe.ConfigError("provider credit requires at least 20 USD at budget start")
        with sqlite3.connect(ledger) as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS budget ("
                "id INTEGER PRIMARY KEY CHECK (id=1), start_remaining REAL NOT NULL)"
            )
            database.execute(
                "CREATE TABLE IF NOT EXISTS attempts ("
                "id TEXT PRIMARY KEY, run_id TEXT NOT NULL, start_remaining REAL NOT NULL, "
                "end_remaining REAL, model_calls INTEGER NOT NULL DEFAULT 0)"
            )
            database.execute("BEGIN IMMEDIATE")
            database.execute("INSERT OR IGNORE INTO budget VALUES (1, ?)", (baseline,))
            self.record_budget_start = database.execute(
                "SELECT start_remaining FROM budget WHERE id=1"
            ).fetchone()[0]
            self.evidence["recording_budget_start_remaining_usd"] = self.record_budget_start
            self.assert_record_budget(remaining)
            count = database.execute(
                "SELECT count(*) FROM attempts WHERE run_id=? AND model_calls > 0",
                (self.record_namespace,),
            ).fetchone()[0]
            if count >= RECORD_ATTEMPTS:
                raise fe.ConfigError("recording is limited to 3 model-reaching attempts per run")
            database.execute(
                "INSERT INTO attempts (id, run_id, start_remaining) VALUES (?, ?, ?)",
                (self.run_id, self.record_namespace, remaining),
            )
        self.record_ledger = ledger

    def assert_record_budget(self, remaining: float, reserve: float = 0.0) -> None:
        assert self.record_budget_start is not None
        spent = self.record_budget_start - remaining
        if spent >= RECORD_BUDGET_USD or (reserve and spent + reserve > RECORD_BUDGET_USD):
            raise fe.ConfigError("recording reached its total 20 USD provider budget")

    def record_model_call(self) -> None:
        """Charge one attempt at its first forwarded model request."""

        assert self.record_ledger is not None
        with self.admission_lock:
            self.admit_model_call()

    def admit_model_call(self) -> None:
        self.assert_record_budget(self.provider_remaining(), RECORD_REQUEST_RESERVE_USD)
        with sqlite3.connect(self.record_ledger) as database:
            database.execute("BEGIN IMMEDIATE")
            calls = database.execute(
                "SELECT model_calls FROM attempts WHERE id=?", (self.run_id,)
            ).fetchone()[0]
            count = database.execute(
                "SELECT count(*) FROM attempts WHERE run_id=? AND model_calls > 0",
                (self.record_namespace,),
            ).fetchone()[0]
            if calls == 0 and count >= RECORD_ATTEMPTS:
                raise fe.ConfigError("recording is limited to 3 model-reaching attempts per run")
            database.execute(
                "UPDATE attempts SET model_calls=model_calls + 1 WHERE id=?", (self.run_id,)
            )
        self.evidence["recording_model_calls"] = calls + 1
        if calls == 0:
            self.evidence["recording_attempt"] = count + 1

    def count_recording_requests(self, model_module: Any) -> None:
        """Check limits before the proxy forwards a valid Messages request."""

        forward = self.model._record

        def record_request(handler: Any, path: str, body: bytes) -> None:
            assert self.record_ledger is not None
            # Each admission must see the credit left after the previous call
            # completed in any run sharing the ledger, so hold an exclusive
            # file lock across admission and the upstream forward. Opening it
            # per request makes two runs in one process conflict as well.
            lock_path = self.record_ledger.with_name("recording.lock")
            with open(lock_path, "a") as lock_file:
                # Poll both locks so a request queued behind another run is
                # refused once teardown begins instead of stalling it.
                while True:
                    if self.recording_closed:
                        self.model._reject(handler, "POST", path, body)
                        return
                    if self.forward_lock.acquire(blocking=False):
                        try:
                            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            self.forward_lock.release()
                        else:
                            if not self.recording_closed:
                                break
                            fcntl.flock(lock_file, fcntl.LOCK_UN)
                            self.forward_lock.release()
                            continue
                    time.sleep(0.1)
                try:
                    if model_module.request_key(model_module._safe_json(body)) is not None:
                        try:
                            self.record_model_call()
                        except fe.ConfigError as exc:
                            self.record_error = str(exc)
                            self.model._reject(handler, "POST", path, body)
                            return
                    forward(handler, path, body)
                finally:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
                    self.forward_lock.release()

        self.model._record = record_request

    def begin_model_teardown(self) -> None:
        """Refuse every recording request not yet forwarded upstream."""

        self.recording_closed = True

    def prerequisites(self) -> None:
        for binary in ("docker", "kind", "helm", "kubectl", "openssl", "git", self.curie):
            if shutil.which(binary) is None:
                raise fe.ConfigError(f"missing required executable: {Path(binary).name}")
        self.check_disk("scripted factory")
        if "v0.24.0" not in self.command("kind", "version", timeout=30):
            raise fe.ConfigError("scripted factory requires kind v0.24.0")
        self.command("docker", "info", timeout=30)
        self.command("helm", "version", timeout=30)
        if self.args.record:
            self.prepare_recording()
        if self.args.build_images:
            for component in ("api", "worker", "ui"):
                self.check_disk(f"building {component}")
                self.command(
                    "docker",
                    "build",
                    "-f",
                    f"apps/{component}/Dockerfile",
                    "-t",
                    f"curie-{component}:local",
                    ".",
                    timeout=1800,
                )
                self.check_disk(f"built {component}")
            self.check_disk("building runner")
            self.command(self.curie, "build", "--tag", "curie-runner:latest", timeout=1800)
            self.check_disk("built runner")
        for image in IMAGES:
            try:
                self.command("docker", "image", "inspect", image, timeout=30)
            except ScriptedScenarioError:
                raise fe.ConfigError(
                    f"local image {image} is missing; pass --build-images to build it"
                ) from None

    def execute(self) -> None:
        self.prerequisites()
        self.workdir = Path(tempfile.mkdtemp(prefix=f"curie-factory-scripted-{self.run_id}-"))
        self.env["KUBECONFIG"] = str(self.workdir / "kubeconfig")
        self.env["CURIE_CONFIG_DIR"] = str(self.workdir / "curie-config")
        # Docker can reassign an ephemeral published port when the registry
        # later joins the kind network, which strands the mirror key and the
        # push address. An explicit port below the ephemeral range stays put.
        registry_port = free_registry_port()
        self.registry_attempted = True
        self.command(
            "docker",
            "run",
            "-d",
            "--name",
            self.registry,
            "--label",
            f"curie.factory-scripted={self.run_id}",
            "-p",
            f"127.0.0.1:{registry_port}:5000",
            "mirror.gcr.io/library/registry:2",
            timeout=300,
        )
        registry_ref = f"localhost:{registry_port}"
        self.registry_ref = registry_ref
        config = self.workdir / "kind.yaml"
        config.write_text(
            "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n"
            "containerdConfigPatches:\n  - |\n"
            f'    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."{registry_ref}"]\n'
            f'      endpoint = ["http://{self.registry}:5000"]\n'
        )
        self.cluster_attempted = True
        self.command(
            "kind",
            "create",
            "cluster",
            "--name",
            self.cluster,
            "--image",
            "kindest/node:v1.31.0",
            "--config",
            str(config),
            "--kubeconfig",
            self.env["KUBECONFIG"],
            "--wait",
            "120s",
            timeout=300,
        )
        self.command("docker", "network", "connect", "kind", self.registry)
        node = json.loads(self.command("docker", "inspect", f"{self.cluster}-control-plane"))[0]
        gateway = node["NetworkSettings"]["Networks"]["kind"].get("Gateway", "")
        if not gateway or ipaddress.ip_address(gateway).version != 4:
            network = json.loads(self.command("docker", "network", "inspect", "kind"))[0]
            gateway = next(
                item["Gateway"]
                for item in network["IPAM"]["Config"]
                if item.get("Gateway") and ipaddress.ip_address(item["Gateway"]).version == 4
            )
        # Loading separately bounds kind's temporary Docker export to one
        # image rather than a tar archive of all four candidate images.
        for image in IMAGES:
            self.check_disk("loading candidate image")
            self.command("kind", "load", "docker-image", image, "--name", self.cluster, timeout=600)
            self.check_disk("loaded candidate image")
        # curie build resolves the runner base with buildx imagetools, which
        # reads a registry and never the local daemon, so the base goes to the
        # run's own registry before the chart names it.
        runner_image = f"{registry_ref}/curie-runner"
        self.command("docker", "tag", "curie-runner:latest", f"{runner_image}:latest")
        self.command("docker", "push", f"{runner_image}:latest", timeout=900)
        github = _module("factory_github_stub", REPO_ROOT / "tools/github-stub/github_stub.py")
        model = _module("factory_model_script", REPO_ROOT / "tools/model-script/model_script.py")
        recording = json.loads(
            (REPO_ROOT / "tools/github-stub/recordings/curie-pr-3400.json").read_text()
        )
        self.stub = github.GithubStub(
            self.workdir / "github",
            recording,
            host=gateway,
            seed_tree=FIXTURE,
            issue_file=ISSUE,
        )
        self.stub.start()
        # Intake starts polling at install. Keep the human issue unlabelled
        # until the bound agent, bundle and automatic publication policy exist.
        issue = self.stub._get("issue", 3815)
        issue["labels"] = []
        self.stub._put("issue", 3815, issue)
        self.model = model.ModelScript(
            Path(self.args.transcript), record=self.args.record, host=gateway
        )
        if self.args.record:
            self.count_recording_requests(model)
        self.model.start()
        self.evidence.update(stub_port=self.stub.port, model_port=self.model.port)
        self.kubectl("apply", "-f", str(REPO_ROOT / "charts/curie/crds"))
        self.kubectl("create", "namespace", self.namespace)
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "configmap",
            "github-stub-ca",
            f"--from-file=ca.pem={self.stub.ca_file}",
        )
        app_key = self.workdir / "app.pem"
        self.command("openssl", "genrsa", "-out", str(app_key), "2048")
        app_key.chmod(0o600)
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "generic",
            "factory-app",
            f"--from-file=privateKey={app_key}",
        )
        values = scripted_values(
            model_base_url=self.model.base_url,
            github_api=self.stub.base_url + "/api/v3",
            clone_base=self.stub.base_url,
            ca_configmap="github-stub-ca",
            runner_image=runner_image,
        )
        # Direct Helm installation follows the existing factory preflight.
        # Generate the same sealed credential/caller inputs as cluster up,
        # without exposing their values in argv or build logs.
        key_container = f"{self.cluster}-keys"
        key_code = (
            "import base64,json; from nacl.signing import SigningKey; "
            "from nacl.public import PrivateKey; k=SigningKey.generate(); "
            "print(json.dumps({'signing':base64.b64encode(bytes(k)).decode(),"
            "'verify':base64.b64encode(bytes(k.verify_key)).decode(),"
            "'sealing':base64.b64encode(bytes(PrivateKey.generate())).decode()}))"
        )
        keys = json.loads(
            self.command(
                "docker",
                "run",
                "--rm",
                "--name",
                key_container,
                "--label",
                f"curie.factory-scripted={self.run_id}",
                "curie-worker:local",
                "python",
                "-c",
                key_code,
                timeout=60,
            )
        )
        self.secrets.extend(keys.values())
        values["connectorCaller"] = {"signingKey": keys["signing"], "verifyKey": keys["verify"]}
        values["sealing"] = {"privateKey": keys["sealing"]}
        values_file = self.workdir / "values.json"
        values_file.write_text(json.dumps(values))
        values_file.chmod(0o600)
        self.install_started = time.monotonic()
        self.deadline = self.install_started + BOUND_SECONDS
        self.command(
            "helm",
            "--kube-context",
            self.context,
            "install",
            "curie",
            "charts/curie",
            "-n",
            self.namespace,
            "-f",
            "charts/curie/values-dev.yaml",
            "-f",
            str(values_file),
            "--timeout",
            "15m",
            timeout=1000,
        )
        for deployment in ("curie-api", "curie-worker", "curie-ui"):
            self.kubectl(
                "-n",
                self.namespace,
                "rollout",
                "status",
                f"deployment/{deployment}",
                "--timeout=300s",
            )
        self.deploy(registry_ref)
        issue = self.stub._get("issue", 3815)
        issue["labels"] = [{"name": "factory"}]
        issue["updated_at"] = github._utc()
        self.stub._put("issue", 3815, issue)
        self.stub._label_event(3815, "factory", "labeled", github.HUMAN)
        self.wait_for_final()
        self.observe()

    def deploy(self, registry_ref: str) -> None:
        assert self.workdir is not None
        secret = json.loads(
            self.kubectl("-n", self.namespace, "get", "secret", "curie-secrets", "-o", "json")
        )
        api_key = base64.b64decode(secret["data"]["apiKey"]).decode()
        self.secrets.append(api_key)
        self.env["CURIE_API_KEY"] = api_key
        context = ssl.create_default_context(cafile=str(self.stub.ca_file))
        request = Request(
            self.stub.base_url + "/api/v3/app/installations/5501/access_tokens",
            data=json.dumps(
                {"repositories": ["acme-bot"], "permissions": {"issues": "read"}}
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, context=context, timeout=30) as response:
            token = json.load(response)["token"]
        self.env["GITHUB_PERSONAL_ACCESS_TOKEN"] = token
        self.secrets.append(token)
        # Asking kubectl for a dynamically allocated port avoids reserving then
        # releasing a socket before the forward claims it.
        self.forward = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.context,
                "-n",
                self.namespace,
                "port-forward",
                "svc/curie-api",
                ":8000",
                "--address",
                "127.0.0.1",
            ],
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        import selectors

        selector = selectors.DefaultSelector()
        assert self.forward.stdout is not None
        selector.register(self.forward.stdout, selectors.EVENT_READ)
        try:
            events = selector.select(timeout=30)
            if not events:
                raise ScriptedScenarioError("API port-forward did not become ready")
            line = self.forward.stdout.readline()
            port = line.split("127.0.0.1:", 1)[1].split(" ", 1)[0]
        finally:
            selector.close()
        api_url = f"http://127.0.0.1:{port}"
        self.env["CURIE_API_URL"] = api_url
        status, agent = fe.http_json(
            "POST",
            api_url + "/agents",
            headers={"X-API-Key": api_key},
            body={
                "name": "factory-e2e",
                "repo_full_name": "acme-corp/acme-bot",
                "channel": {"kind": "github", "address": "acme-corp/acme-bot"},
            },
        )
        if status != 201:
            raise ScriptedScenarioError(f"factory agent creation returned HTTP {status}")
        self.evidence["agent_id"] = agent["id"]
        bundle = self.workdir / "bundle"
        shutil.copytree(REPO_ROOT / "examples/dark-factory", bundle)
        self.check_disk("building the runner layer")
        runner_base = f"{registry_ref}/curie-runner:latest"
        self.command(
            self.curie,
            "build",
            "--plugin-dir",
            str(bundle),
            "--registry",
            registry_ref,
            "--runner-image",
            runner_base,
            "--platform",
            "linux/amd64",
            timeout=900,
        )
        self.check_disk("built runner layer")
        common = ["--namespace", self.namespace, "--release", "curie", "--api-url", api_url]
        self.command(
            self.curie,
            "cluster",
            "--context",
            self.context,
            "deploy",
            "--plugin-dir",
            str(bundle),
            "--agent",
            "factory-e2e",
            "--env",
            "prod",
            "--chart",
            str(REPO_ROOT / "charts/curie"),
            "--secret",
            "GITHUB_PERSONAL_ACCESS_TOKEN",
            *common,
            timeout=300,
        )
        self.command(
            self.curie,
            "cluster",
            "--context",
            self.context,
            "publication-policy",
            "factory-e2e",
            "--policy",
            "auto",
            *common,
            timeout=300,
        )

    def wait_for_final(self) -> None:
        assert self.deadline is not None
        assert self.install_started is not None
        advanced = False
        next_publication_probe = 0.0
        while time.monotonic() < self.deadline:
            if self.record_error is not None:
                raise ScriptedScenarioError(self.record_error)
            if self.model.unexpected:
                raise ScriptedScenarioError("unexpected model request")
            if self.stub.unknown_requests:
                raise ScriptedScenarioError("unsupported stub request")
            # The status comment binds both the SQL observations and the
            # fixture clock to this issue's actual execution request.
            comments = [row for row in self.stub._rows("comment") if row["issue_number"] == 3815]
            request_ids = {
                str(uuid.UUID(match.group(1)))
                for row in comments
                if (match := fe._NOTICE_MARKER.search(row["body"]))
            }
            if len(request_ids) > 1:
                raise ScriptedScenarioError("expected one execution request for the fixture issue")
            if request_ids:
                request_id = next(iter(request_ids))
                self.evidence["execution_request_id"] = request_id
                if not advanced and time.monotonic() >= next_publication_probe:
                    exists = self.sql(
                        "SELECT to_json(EXISTS(SELECT 1 FROM curie.publications "
                        f"WHERE execution_request_id='{request_id}'::uuid))"
                    )
                    next_publication_probe = time.monotonic() + 10
                    if exists:
                        self.stub.advance(1800)
                        advanced = True
            final = [row for row in comments if fe._NOTICE_FINAL in row["body"]]
            if final:
                self.evidence["elapsed_seconds"] = time.monotonic() - self.install_started
                self.evidence["final_comment"] = final[0]
                self.evidence["final_comment_count"] = len(final)
                return
            if self.args.record and self.record_start_remaining is not None:
                self.assert_record_budget(self.provider_remaining())
            time.sleep(2)
        raise ScriptedScenarioError("final issue comment timed out after 2400 seconds")

    def sql(self, query: str) -> Any:
        output = self.kubectl(
            "-n",
            self.namespace,
            "exec",
            "statefulset/curie-postgres",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "BEGIN READ ONLY; " + query + "; COMMIT;",
        )
        lines = [line for line in output.splitlines() if line not in {"BEGIN", "COMMIT"}]
        if len(lines) != 1:
            raise ScriptedScenarioError("read-only SQL did not return one observation")
        return json.loads(lines[0])

    def observe(self) -> None:
        marker = fe._NOTICE_MARKER.search(self.evidence["final_comment"]["body"])
        if marker is None:
            raise ScriptedScenarioError("final issue comment has no execution request identity")
        # UUID parsing prevents fixture text from becoming executable SQL.
        request_id = str(uuid.UUID(marker.group(1)))
        if self.evidence.get("execution_request_id") != request_id:
            raise ScriptedScenarioError("final issue comment names another execution request")
        observed = self.sql(
            "SELECT json_build_object('preflights', "
            "(SELECT json_agg(r.note::json) FROM curie.execution_request_phase_reports r "
            f"WHERE r.execution_request_id='{request_id}'::uuid "
            "AND r.phase='verification_preflight'), 'publications', "
            "(SELECT json_agg(json_build_object('changed_paths', changed_paths, "
            "'execution_request_id', execution_request_id)) FROM curie.publications "
            f"WHERE execution_request_id='{request_id}'::uuid))"
        )
        preflights = observed["preflights"] or []
        publications = observed["publications"] or []
        if len(preflights) != 1 or len(publications) != 1:
            raise ScriptedScenarioError("expected one preflight observation and one publication")
        outcome = preflights[0]["outcome"]
        paths = publications[0]["changed_paths"]
        self.evidence.update(preflight_outcome=outcome, changed_paths=paths)
        assert_observed(outcome, paths)
        pulls = self.stub._rows("pull")
        self.evidence["pull_request_count"] = len(pulls)
        if len(pulls) != 1:
            raise ScriptedScenarioError("expected exactly one stub pull request")
        pull = self.stub._fresh_pull(pulls[0])
        status, files = self.stub._rest(
            "GET", f"/repos/acme-corp/acme-bot/pulls/{pull['number']}/files", {}, {}
        )
        if status != 200:
            raise ScriptedScenarioError("stub pull request files were unavailable")
        scenario.assert_publication_paths([item["filename"] for item in files])
        self.evidence["pull_request"] = pull
        final = self.evidence["final_comment"]
        if self.evidence["final_comment_count"] != 1 or pull["html_url"] not in final["body"]:
            raise ScriptedScenarioError(
                "expected one final issue comment naming the pull request URL"
            )
        if self.evidence["elapsed_seconds"] > BOUND_SECONDS:
            raise ScriptedScenarioError("factory scenario exceeded 2400 seconds")

    def cleanup(self) -> list[str]:
        """Attempt every owned teardown and verify deletion even after failures."""

        errors: list[str] = []
        # Cleanup must retain its own timeout after the scenario deadline.
        self.deadline = None
        self.begin_model_teardown()
        if self.forward is not None:
            try:
                self.forward.terminate()
                try:
                    self.forward.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.forward.kill()
                    self.forward.wait(timeout=10)
                if self.forward.stdout is not None:
                    self.forward.stdout.close()
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        for service, field in (
            (self.model, "unexpected_model_requests"),
            (self.stub, "unsupported_stub_requests"),
        ):
            if service is None:
                continue
            try:
                service.close()
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                # close() re-raises the request audit after shutting down. Those
                # requests are already the recorded scenario failure; the probe
                # below still proves the endpoint stopped.
                audited = service.unexpected if service is self.model else service.unknown_requests
                if not audited:
                    errors.append(str(exc))
            try:
                self.evidence[field] = (
                    service.unexpected if service is self.model else service.unknown_requests
                )
                port = service.port
                host = service.host
                with socket.socket() as sock:
                    sock.settimeout(1)
                    if sock.connect_ex((host, port)) == 0:
                        errors.append(f"owned endpoint still listens on port {port}")
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        if self.cluster_attempted:
            try:
                self.command("kind", "delete", "cluster", "--name", self.cluster, timeout=120)
                if self.cluster in self.command("kind", "get", "clusters", timeout=30).splitlines():
                    errors.append("owned kind cluster remains after teardown")
                nodes = self.command(
                    "docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={self.cluster}"
                )
                if nodes.strip():
                    errors.append("owned kind node container remains after teardown")
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        if self.registry_attempted:
            try:
                self.command("docker", "rm", "-f", self.registry, timeout=60)
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                # A failed docker run can have created no registry at all.
                try:
                    remaining = self.command(
                        "docker", "ps", "-aq", "--filter", f"name=^/{self.registry}$"
                    )
                    if remaining.strip():
                        errors.append(str(exc))
                except (
                    OSError,
                    RuntimeError,
                    ValueError,
                    KeyError,
                    TypeError,
                    subprocess.SubprocessError,
                    sqlite3.Error,
                ) as probe_exc:
                    errors.extend((str(exc), str(probe_exc)))
            try:
                if self.command(
                    "docker", "ps", "-aq", "--filter", f"name=^/{self.registry}$"
                ).strip():
                    errors.append("owned registry remains after teardown")
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        if self.registry_ref is not None:
            # The pushed runner base and curie build's layer leave local tags
            # naming this run's deleted registry; the images keep other names.
            try:
                tags = self.command(
                    "docker",
                    "images",
                    "--format",
                    "{{.Repository}}:{{.Tag}}",
                    "--filter",
                    f"reference={self.registry_ref}/*",
                ).split()
                if tags:
                    self.command("docker", "image", "rm", *tags)
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        # A killed key-generation docker client can leave its --rm container
        # running. Select only this execution's remaining labelled resources.
        if self.registry_attempted:
            try:
                remaining = self.command(
                    "docker", "ps", "-aq", "--filter", f"label=curie.factory-scripted={self.run_id}"
                )
                if remaining.strip():
                    self.command("docker", "rm", "-f", *remaining.split())
                if self.command(
                    "docker", "ps", "-aq", "--filter", f"label=curie.factory-scripted={self.run_id}"
                ).strip():
                    errors.append("owned labelled containers remain after teardown")
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        if self.record_ledger is not None:
            try:
                remaining_credit = self.provider_remaining()
                with sqlite3.connect(self.record_ledger) as database:
                    database.execute(
                        "UPDATE attempts SET end_remaining=? WHERE id=?",
                        (remaining_credit, self.run_id),
                    )
                assert self.record_budget_start is not None
                cost = max(0.0, self.record_budget_start - remaining_credit)
                self.evidence["recording_end_remaining_usd"] = remaining_credit
                self.evidence["recording_total_usd"] = cost
                if cost > RECORD_BUDGET_USD:
                    errors.append("recording exceeded its total 20 USD provider budget")
            except (
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
                sqlite3.Error,
            ) as exc:
                errors.append(str(exc))
        if self.workdir is not None:
            try:
                shutil.rmtree(self.workdir)
            except OSError as exc:
                errors.append(str(exc))
        self.evidence["cleanup_errors"] = errors
        self.evidence["cleanup_verified"] = not errors
        return errors


def main(args: argparse.Namespace) -> int:
    try:
        validate_configuration(args)
    except fe.ConfigError as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return fe.EXIT_CONFIG
    run = ScriptedRun(args)
    result = fe.EXIT_FAILED
    previous_handlers: dict[int, Any] = {}

    def interrupted(signum: int, _frame: Any) -> None:
        raise ScriptedScenarioError(f"scripted scenario interrupted by signal {signum}")

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, interrupted)
        run.execute()
        result = 0
    except fe.ConfigError as exc:
        result = fe.EXIT_CONFIG
        run.evidence["error"] = str(exc)
    except (
        OSError,
        RuntimeError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
        sqlite3.Error,
    ) as exc:
        detail, _ = fe.redact_agent_text(str(exc), run.secrets)
        run.evidence["error"] = detail
    finally:
        for restored_signum, handler in previous_handlers.items():
            signal.signal(restored_signum, handler)
        try:
            if run.cleanup():
                result = fe.EXIT_FAILED
        except (
            OSError,
            RuntimeError,
            ValueError,
            KeyError,
            TypeError,
            subprocess.SubprocessError,
            sqlite3.Error,
        ) as exc:
            run.evidence["cleanup_errors"] = [str(exc)]
            run.evidence["cleanup_verified"] = False
            result = fe.EXIT_FAILED
        run.evidence["result"] = "passed" if result == 0 else "failed"
        if run.install_started is not None and "elapsed_seconds" not in run.evidence:
            run.evidence["elapsed_seconds"] = time.monotonic() - run.install_started
        run.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        run.evidence_path.write_text(json.dumps(run.evidence, indent=2) + "\n")
    print(f"factory-e2e: {run.evidence['result']}; evidence {run.evidence_path}", file=sys.stderr)
    return result


if __name__ == "__main__":
    raise SystemExit(main(fe.parse_args(sys.argv[1:])))

#!/usr/bin/env python3
"""Drive the dark factory end to end against a disposable install (#2966).

`curie dev factory-e2e preflight` installs the candidate's published images in
a throwaway namespace on a kube context the operator names, enables signed
factory intake, exposes the api webhook through a temporary tunnel, points a
GitHub App's webhook at it with an App JWT, resets a fixture repository, labels
one issue, and asserts that GitHub delivered the event, the api accepted it, and
a WorkItem was admitted (read back through the api work-items route). Every
change is undone on exit and each undo is verified, and the result is a JSON
evidence file.

`curie dev factory-e2e run --scenario <name>` is the hook for the scenario
drivers (issue-to-pr, revision, cancel-waiting, cancel-running, evaluation).
A scenario without a driver refuses before anything is installed.

The App, fixture repository, mention author and model credentials come only
from operator files or environment variables; nothing here names a real one.
Standard library only, so it runs from a bare source checkout.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

GITHUB_API = "https://api.github.com"
GHCR = "https://ghcr.io"
IMAGE_OWNER = "curie-eng"
# The components the chart pins by tag. The runner is pinned separately under
# agentSandbox.runner.tag.
CHART_COMPONENTS = {
    "api": "curie-api",
    "worker": "curie-worker",
    "dispatcher": "curie-dispatcher",
    "mailAdapter": "curie-mail-adapter",
    "ui": "curie-ui",
}
RUNNER_IMAGE = "curie-runner"
RELEASE = "curie"
DEFAULT_LABEL = "curie-factory"
NAMESPACE_PREFIX = "test-factory-"
APP_KEY_REF = "factory-e2e-github-app"
SANDBOX_CRD = "sandboxes.agents.x-k8s.io"

EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_SCENARIO = 3

_NAMESPACE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_REPO = re.compile(r"^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$")
_TUNNEL_URL = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


class ConfigError(Exception):
    """An operator input is missing or malformed. Nothing live was touched."""


class ScenarioUnavailable(Exception):
    """The named scenario has no driver yet."""


class PreflightFailed(Exception):
    """A live step did not produce the expected observation."""


# --------------------------------------------------------------------------
# Scenario hooks. Each driver receives the live Preflight after its own
# assertions pass and runs inside the same teardown. None means "not written
# yet": the command refuses before installing anything.
# --------------------------------------------------------------------------

ScenarioDriver = Callable[["Preflight"], dict[str, Any]]

SCENARIOS: dict[str, ScenarioDriver | None] = {
    "issue-to-pr": None,
    "revision": None,
    "cancel-waiting": None,
    "cancel-running": None,
    "evaluation": None,
}
SCENARIO_NAMES = tuple(SCENARIOS)


def resolve_scenario(name: str) -> ScenarioDriver:
    driver = SCENARIOS.get(name)
    if driver is None:
        raise ScenarioUnavailable(
            f"scenario {name!r} has no driver yet; only `preflight` runs today. "
            "Add the driver to SCENARIOS in tools/factory-e2e/factory_e2e.py."
        )
    return driver


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class FactoryConfig:
    kube_context: str
    app_id: str
    installation_id: int
    private_key_file: Path
    repo: str
    label: str
    mention: str
    cloudflared: str
    priority_classes: tuple[str, str] | None
    restore_webhook_url: str | None
    webhook_secret: str = dataclasses.field(repr=False)
    actor_token: str = dataclasses.field(repr=False)
    model_api_key: str | None = dataclasses.field(default=None, repr=False)


def _read_secret_file(path: Path) -> str | None:
    try:
        value = path.read_text().strip()
    except OSError:
        return None
    return value or None


def gh_token_for_user(user: str) -> str:
    result = subprocess.run(
        ["gh", "auth", "token", "--user", user],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return ""
    return result.stdout.strip()


def load_config(
    env: Mapping[str, str],
    *,
    context: str | None,
    gh_token: Callable[[str], str] = gh_token_for_user,
) -> FactoryConfig:
    """Read every operator input, naming ALL missing ones in one refusal.

    CURIE_FACTORY_APP_DIR may hold app.json (id, slug, installation_id and an
    optional repo), app.pem and webhook_secret; the individual variables win
    over it. Secret values are read, never echoed.
    """

    missing: list[str] = []
    app_dir = (
        Path(env["CURIE_FACTORY_APP_DIR"]).expanduser()
        if env.get("CURIE_FACTORY_APP_DIR")
        else None
    )
    meta: dict[str, Any] = {}
    if app_dir is not None:
        try:
            meta = json.loads((app_dir / "app.json").read_text())
        except (OSError, ValueError):
            meta = {}

    kube_context = context or env.get("CURIE_FACTORY_KUBE_CONTEXT", "")
    if not kube_context:
        missing.append("CURIE_FACTORY_KUBE_CONTEXT (or --context)")

    app_id = env.get("CURIE_FACTORY_APP_ID") or str(meta.get("id") or "")
    if not app_id.isdigit():
        missing.append("CURIE_FACTORY_APP_ID")

    installation = env.get("CURIE_FACTORY_INSTALLATION_ID") or str(
        meta.get("installation_id") or ""
    )
    if not installation.isdigit():
        missing.append("CURIE_FACTORY_INSTALLATION_ID")

    key_file = env.get("CURIE_FACTORY_APP_PRIVATE_KEY_FILE") or (
        str(app_dir / "app.pem") if app_dir else ""
    )
    key_path = Path(key_file).expanduser() if key_file else Path()
    if not key_file or not key_path.is_file():
        missing.append("CURIE_FACTORY_APP_PRIVATE_KEY_FILE")

    secret_file = env.get("CURIE_FACTORY_WEBHOOK_SECRET_FILE") or (
        str(app_dir / "webhook_secret") if app_dir else ""
    )
    webhook_secret = _read_secret_file(Path(secret_file).expanduser()) if secret_file else None
    if not webhook_secret:
        missing.append("CURIE_FACTORY_WEBHOOK_SECRET_FILE")

    repo = env.get("CURIE_FACTORY_REPO") or str(meta.get("repo") or "")
    if not _REPO.fullmatch(repo):
        missing.append("CURIE_FACTORY_REPO (owner/name of the fixture repository)")

    actor_token = env.get("CURIE_FACTORY_ACTOR_TOKEN", "")
    if not actor_token and env.get("CURIE_FACTORY_ACTOR_GH_USER"):
        actor_token = gh_token(env["CURIE_FACTORY_ACTOR_GH_USER"])
    if not actor_token:
        missing.append(
            "CURIE_FACTORY_ACTOR_TOKEN (or CURIE_FACTORY_ACTOR_GH_USER with a gh login): "
            "a human account with write access to the fixture repository"
        )

    label = env.get("CURIE_FACTORY_LABEL") or DEFAULT_LABEL
    mention = env.get("CURIE_FACTORY_MENTION") or str(meta.get("slug") or "")
    if not mention:
        missing.append("CURIE_FACTORY_MENTION (the login the factory answers to)")

    priority_classes: tuple[str, str] | None = None
    if env.get("CURIE_FACTORY_PRIORITY_CLASSES"):
        parts = [p.strip() for p in env["CURIE_FACTORY_PRIORITY_CLASSES"].split(",")]
        if len(parts) != 2 or not all(parts):
            raise ConfigError(
                "CURIE_FACTORY_PRIORITY_CLASSES must be '<platform>,<sandbox>' "
                "naming two existing PriorityClasses"
            )
        priority_classes = (parts[0], parts[1])

    if missing:
        raise ConfigError(
            "missing required factory credential or setting: "
            + "; ".join(missing)
            + ". Set them in the environment, or point CURIE_FACTORY_APP_DIR at a "
            "directory holding app.json, app.pem and webhook_secret."
        )
    assert webhook_secret is not None
    return FactoryConfig(
        kube_context=kube_context,
        app_id=app_id,
        installation_id=int(installation),
        private_key_file=key_path,
        repo=repo,
        label=label,
        mention=mention,
        cloudflared=env.get("CURIE_FACTORY_CLOUDFLARED") or "cloudflared",
        priority_classes=priority_classes,
        restore_webhook_url=env.get("CURIE_FACTORY_WEBHOOK_RESTORE_URL") or None,
        webhook_secret=webhook_secret,
        actor_token=actor_token,
        model_api_key=env.get("CURIE_FACTORY_MODEL_API_KEY") or None,
    )


def validate_namespace(name: str) -> str:
    if not name.startswith(NAMESPACE_PREFIX) or len(name) > 40 or not _NAMESPACE.fullmatch(name):
        raise ConfigError(
            f"namespace {name!r} must be a lowercase RFC 1123 name starting with "
            f"{NAMESPACE_PREFIX!r}, at most 40 characters: the driver deletes it on exit"
        )
    return name


def default_namespace(candidate: str) -> str:
    return validate_namespace(NAMESPACE_PREFIX + candidate[:8].lower())


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def app_jwt(app_id: str, key_file: Path, *, now: int | None = None) -> str:
    """A GitHub App JWT (RS256, 9 minute life, iat backdated for clock skew)."""

    issued = int(time.time()) if now is None else now
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = {"iat": issued - 60, "exp": issued + 540, "iss": app_id}
    payload = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{payload}".encode()
    signed = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(key_file)],
        input=signing_input,
        capture_output=True,
        check=False,
    )
    if signed.returncode != 0:
        raise ConfigError("openssl could not sign the App JWT with the configured private key")
    return f"{header}.{payload}.{_b64url(signed.stdout)}"


def request_id_for(repository_id: int, issue_number: int) -> uuid.UUID:
    """The execution request id the api derives for a label admission."""

    identity = f"https://github.com/factory/label/{repository_id}/{issue_number}"
    return uuid.uuid5(uuid.NAMESPACE_URL, identity)


def match_delivery(
    deliveries: list[dict[str, Any]], *, issue_number: int, repo: str
) -> dict[str, Any] | None:
    """The newest `issues.labeled` delivery for this issue, from detailed deliveries."""

    found = None
    for delivery in deliveries:
        if delivery.get("event") != "issues" or delivery.get("action") != "labeled":
            continue
        payload = (delivery.get("request") or {}).get("payload") or {}
        issue = payload.get("issue") or {}
        repository = payload.get("repository") or {}
        if issue.get("number") == issue_number and repository.get("full_name") == repo:
            found = delivery
    return found


def delivery_api_status(delivery: dict[str, Any]) -> str | None:
    body = (delivery.get("response") or {}).get("payload")
    if not isinstance(body, str):
        return None
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    status = parsed.get("status") if isinstance(parsed, dict) else None
    return status if isinstance(status, str) else None


def install_values(
    config: FactoryConfig,
    *,
    candidate: str,
    app_key_secret: str,
    consumer_controller: bool,
) -> dict[str, Any]:
    """Helm values for the disposable install. Written to a 0600 file, never argv."""

    tag = f"sha-{candidate}"
    values: dict[str, Any] = {component: {"image": {"tag": tag}} for component in CHART_COMPONENTS}
    values["api"].update(
        {
            "githubWebhookSecret": config.webhook_secret,
            "githubFactoryIngressEnabled": True,
            "githubFactoryLabel": config.label,
            "githubFactoryMention": config.mention,
            "githubAppId": config.app_id,
            "githubAppExistingSecret": app_key_secret,
            "githubRepoAllowlist": [config.repo],
        }
    )
    values["agentSandbox"] = {
        "runner": {"tag": tag},
        "controller": {"deploy": not consumer_controller},
    }
    # A disposable install proves the factory flow, not sandbox isolation, and
    # most scratch clusters carry no gVisor runtime class.
    values["security"] = {"gvisor": {"mode": "off"}}
    if config.priority_classes is not None:
        platform, sandbox = config.priority_classes
        values["priorityClasses"] = {
            "platform": {"create": False, "name": platform},
            "sandbox": {"create": False, "name": sandbox},
        }
    return values


class Teardown:
    """LIFO undo stack. Every step runs even when an earlier one fails."""

    def __init__(self) -> None:
        self._steps: list[tuple[str, Callable[[], Any]]] = []

    def push(self, name: str, step: Callable[[], Any]) -> None:
        self._steps.append((name, step))

    def run(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        while self._steps:
            name, step = self._steps.pop()
            try:
                detail = step()
                results.append({"step": name, "ok": True, "detail": detail})
            except BaseException as exc:  # noqa: BLE001 - keep undoing
                results.append(
                    {"step": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}"}
                )
        return results


# --------------------------------------------------------------------------
# Process and HTTP plumbing
# --------------------------------------------------------------------------


def log(message: str) -> None:
    print(
        f"[factory-e2e {dt.datetime.now(dt.UTC):%H:%M:%S}] {message}", file=sys.stderr, flush=True
    )


def run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, input=input_text, check=False)
    if check and result.returncode != 0:
        tail = (result.stderr or result.stdout).strip()[-1500:]
        raise PreflightFailed(f"{argv[0]} {argv[1] if len(argv) > 1 else ''} failed: {tail}")
    return result.stdout


def http_json(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    body: Any = None,
    timeout: float = 30,
) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status = exc.code
    if not raw:
        return status, None
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, raw.decode(errors="replace")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait(what: str, timeout: float, probe: Callable[[], Any], interval: float = 3) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() > deadline:
            raise PreflightFailed(f"timed out after {int(timeout)}s waiting for {what}")
        time.sleep(interval)


def _stop(process: subprocess.Popen[Any]) -> bool:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=15)
    return process.poll() is not None


# --------------------------------------------------------------------------
# The live preflight
# --------------------------------------------------------------------------


class Preflight:
    def __init__(
        self,
        config: FactoryConfig,
        *,
        repo_root: Path,
        candidate: str,
        namespace: str,
        evidence_path: Path,
        admission_timeout: float,
    ) -> None:
        self.config = config
        self.repo_root = repo_root
        self.candidate = candidate
        self.namespace = namespace
        self.evidence_path = evidence_path
        self.admission_timeout = admission_timeout
        self.teardown = Teardown()
        self.evidence: dict[str, Any] = {
            "schema": "curie.factory-e2e.evidence/v1",
            "mode": "preflight",
            "candidate_commit": candidate,
            "image_tag": f"sha-{candidate}",
            "kube_context": config.kube_context,
            "namespace": namespace,
            "release": RELEASE,
            "started_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "steps": [],
        }
        self.workdir = Path(tempfile.mkdtemp(prefix="factory-e2e-"))
        os.chmod(self.workdir, 0o700)
        self.api_url = ""
        self.api_key = ""
        self.worker_token = ""
        self.tunnel_url = ""
        self.repository_id = 0
        self.default_branch = ""

    # --- small wrappers -------------------------------------------------

    def kubectl(self, *args: str, check: bool = True) -> str:
        return run(["kubectl", "--context", self.config.kube_context, *args], check=check)

    def step(self, name: str, **facts: Any) -> None:
        log(name)
        self.evidence["steps"].append({"step": name, **facts})

    def github(self, method: str, path: str, *, token: str, body: Any = None) -> tuple[int, Any]:
        return http_json(
            method,
            GITHUB_API + path,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            body=body,
        )

    def as_actor(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        return self.github(method, path, token=self.config.actor_token, body=body)

    def as_app(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        token = app_jwt(self.config.app_id, self.config.private_key_file)
        return self.github(method, path, token=token, body=body)

    def api(
        self, method: str, path: str, *, headers: Mapping[str, str], body: Any = None
    ) -> tuple[int, Any]:
        return http_json(method, self.api_url + path, headers=headers, body=body)

    # --- steps ---------------------------------------------------------

    def check_tools(self) -> None:
        wanted = ["kubectl", "helm", "git", "openssl", self.config.cloudflared]
        absent = [tool for tool in wanted if shutil.which(tool) is None]
        if absent:
            raise ConfigError(f"required tools not on PATH: {', '.join(absent)}")

    def check_images(self) -> None:
        tag = f"sha-{self.candidate}"
        missing = []
        for image in [*CHART_COMPONENTS.values(), RUNNER_IMAGE]:
            scope = urllib.parse.quote(f"repository:{IMAGE_OWNER}/{image}:pull", safe="")
            status, body = http_json("GET", f"{GHCR}/token?scope={scope}&service=ghcr.io")
            token = body.get("token") if status == 200 and isinstance(body, dict) else None
            request = urllib.request.Request(
                f"{GHCR}/v2/{IMAGE_OWNER}/{image}/manifests/{tag}", method="HEAD"
            )
            request.add_header(
                "Accept",
                "application/vnd.oci.image.index.v1+json,"
                "application/vnd.docker.distribution.manifest.list.v2+json,"
                "application/vnd.oci.image.manifest.v1+json",
            )
            if token:
                request.add_header("Authorization", f"Bearer {token}")
            try:
                with urllib.request.urlopen(request, timeout=30):
                    pass
            except urllib.error.HTTPError:
                missing.append(image)
        if missing:
            raise PreflightFailed(
                f"no published {tag} image for {', '.join(missing)}; the candidate must be a "
                "commit the release workflow built (a push to main or next)"
            )
        self.step("candidate images published", tag=tag)

    def check_app(self) -> None:
        status, body = self.as_app("GET", f"/app/installations/{self.config.installation_id}")
        if status != 200:
            raise PreflightFailed(
                f"the App JWT could not read installation {self.config.installation_id} "
                f"(HTTP {status}); check CURIE_FACTORY_APP_ID and the private key"
            )
        status, body = self.as_actor("GET", f"/repos/{self.config.repo}")
        if status != 200 or not isinstance(body, dict):
            raise PreflightFailed(
                f"the actor token cannot read the fixture repository (HTTP {status})"
            )
        permissions = body.get("permissions") or {}
        if not (permissions.get("push") or permissions.get("admin")):
            raise PreflightFailed("the actor account needs write access to the fixture repository")
        self.repository_id = int(body["id"])
        self.default_branch = str(body["default_branch"])
        self.evidence["fixture_repository_id"] = self.repository_id
        self.step("App JWT and actor token verified")

    def extract_chart(self) -> Path:
        run(["git", "-C", str(self.repo_root), "fetch", "--quiet", "origin", self.candidate])
        archive = self.workdir / "chart.tar"
        run(
            [
                "git",
                "-C",
                str(self.repo_root),
                "archive",
                "--output",
                str(archive),
                self.candidate,
                "charts/curie",
            ]
        )
        run(["tar", "-xf", str(archive), "-C", str(self.workdir)])
        return self.workdir / "charts" / "curie"

    def create_namespace(self) -> None:
        existing = self.kubectl(
            "get", "namespace", self.namespace, "--ignore-not-found", "-o", "name"
        )
        if existing.strip():
            raise ConfigError(
                f"namespace {self.namespace} already exists; the driver only uses a namespace "
                "it created. Delete it or pass another --namespace."
            )
        self.kubectl("create", "namespace", self.namespace)
        self.teardown.push("delete namespaces", self.delete_namespaces)
        self.kubectl(
            "label", "namespace", self.namespace, "app.kubernetes.io/managed-by=curie-factory-e2e"
        )
        self.step("namespace created", namespace=self.namespace)

    def publication_namespace(self) -> str:
        return f"{self.namespace}-{RELEASE}-publication"

    def delete_namespaces(self) -> dict[str, Any]:
        run(
            [
                "helm",
                "--kube-context",
                self.config.kube_context,
                "uninstall",
                RELEASE,
                "-n",
                self.namespace,
                "--no-hooks",
                "--wait",
                "--timeout",
                "5m",
            ],
            check=False,
        )
        names = [self.namespace, self.publication_namespace()]
        for name in names:
            self.kubectl("delete", "namespace", name, "--ignore-not-found", "--wait=false")

        def gone() -> bool:
            return all(
                not self.kubectl(
                    "get", "namespace", name, "--ignore-not-found", "-o", "name"
                ).strip()
                for name in names
            )

        _wait("namespace deletion", 600, gone, interval=5)
        return {"deleted": names, "verified_absent": True}

    def install(self) -> None:
        chart = self.extract_chart()
        consumer = bool(
            self.kubectl("get", "crd", SANDBOX_CRD, "--ignore-not-found", "-o", "name").strip()
        )
        key_file = str(self.config.private_key_file)
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "generic",
            APP_KEY_REF,
            f"--from-file=privateKey={key_file}",
        )
        values = install_values(
            self.config,
            candidate=self.candidate,
            app_key_secret=APP_KEY_REF,
            consumer_controller=consumer,
        )
        values_file = self.workdir / "values.json"
        fd = os.open(values_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(values, handle)
        log("helm install (this pulls every image; allow several minutes)")
        run(
            [
                "helm",
                "--kube-context",
                self.config.kube_context,
                "install",
                RELEASE,
                str(chart),
                "-n",
                self.namespace,
                "-f",
                str(values_file),
                "--wait",
                "--timeout",
                "20m",
            ]
        )
        values_file.unlink()
        self.step(
            "installed",
            chart="charts/curie@candidate",
            sandbox_controller="existing (consumer mode)" if consumer else "deployed by release",
            factory_ingress=True,
        )

    def port_forward(self) -> None:
        port = _free_port()
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.config.kube_context,
                "-n",
                self.namespace,
                "port-forward",
                f"svc/{RELEASE}-api",
                f"{port}:8000",
                "--address",
                "127.0.0.1",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.teardown.push("stop api port-forward", lambda: {"stopped": _stop(process)})
        self.api_url = f"http://127.0.0.1:{port}"

        def healthy() -> bool:
            try:
                status, _ = http_json("GET", self.api_url + "/health", timeout=5)
            except OSError:
                return False
            return status == 200

        _wait("the api through port-forward", 120, healthy)
        secret = json.loads(
            self.kubectl("-n", self.namespace, "get", "secret", f"{RELEASE}-secrets", "-o", "json")
        )
        data = secret["data"]
        self.api_key = base64.b64decode(data["apiKey"]).decode()
        self.worker_token = base64.b64decode(data["internalWorkerToken"]).decode()
        self.step("api reachable")

    def bind_agent(self) -> None:
        status, body = self.api(
            "POST",
            "/agents",
            headers={"X-API-Key": self.api_key},
            body={
                "name": "factory-e2e",
                "repo_full_name": self.config.repo,
                "channel": {"kind": "github", "address": self.config.repo},
            },
        )
        if status != 201 or not isinstance(body, dict):
            raise PreflightFailed(f"agent creation failed (HTTP {status}): {body}")
        self.evidence["agent_id"] = body.get("id")
        self.step("factory agent bound to the fixture repository", agent_id=body.get("id"))

    def tunnel(self) -> None:
        process = subprocess.Popen(
            [
                self.config.cloudflared,
                "tunnel",
                "--no-autoupdate",
                "--url",
                self.api_url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def stop() -> dict[str, Any]:
            return {"stopped": _stop(process), "exit_code": process.returncode}

        self.teardown.push("stop tunnel", stop)
        found: list[str] = []

        def reader() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                match = _TUNNEL_URL.search(line)
                if match and not found:
                    found.append(match.group(0))

        threading.Thread(target=reader, daemon=True).start()
        self.tunnel_url = _wait("the tunnel URL", 90, lambda: found[0] if found else None, 1)

        def reachable() -> bool:
            try:
                status, _ = http_json("GET", self.tunnel_url + "/health", timeout=10)
            except OSError:
                return False
            return status == 200

        _wait("the api through the tunnel", 180, reachable, 5)
        self.step("tunnel up")

    def repoint_webhook(self) -> None:
        status, original = self.as_app("GET", "/app/hook/config")
        if status != 200 or not isinstance(original, dict):
            raise PreflightFailed(f"could not read the App webhook config (HTTP {status})")
        restore = {
            "url": self.config.restore_webhook_url or original.get("url"),
            "content_type": original.get("content_type") or "json",
        }

        def restore_webhook() -> dict[str, Any]:
            status, _ = self.as_app("PATCH", "/app/hook/config", restore)
            if status != 200:
                raise PreflightFailed(f"restoring the App webhook failed (HTTP {status})")
            status, now = self.as_app("GET", "/app/hook/config")
            if status != 200 or not isinstance(now, dict) or now.get("url") != restore["url"]:
                raise PreflightFailed("the App webhook URL did not read back as restored")
            return {"restored": True, "verified": True}

        self.teardown.push("restore App webhook", restore_webhook)
        target = self.tunnel_url + "/github/webhook"
        status, _ = self.as_app(
            "PATCH", "/app/hook/config", {"url": target, "content_type": "json"}
        )
        if status != 200:
            raise PreflightFailed(f"pointing the App webhook at the tunnel failed (HTTP {status})")
        status, now = self.as_app("GET", "/app/hook/config")
        if status != 200 or not isinstance(now, dict) or now.get("url") != target:
            raise PreflightFailed("the App webhook URL did not read back as the tunnel")
        self.step("App webhook repointed at the tunnel")

    def _paged(self, path: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        while True:
            sep = "&" if "?" in path else "?"
            status, body = self.as_actor("GET", f"{path}{sep}per_page=100&page={page}")
            if status != 200 or not isinstance(body, list):
                raise PreflightFailed(f"GitHub list {path} failed (HTTP {status})")
            items.extend(body)
            if len(body) < 100:
                return items
            page += 1

    def reset_fixture(self) -> dict[str, Any]:
        repo = f"/repos/{self.config.repo}"
        closed = 0
        for issue in self._paged(f"{repo}/issues?state=open"):
            status, _ = self.as_actor(
                "PATCH", f"{repo}/issues/{issue['number']}", {"state": "closed"}
            )
            if status != 200:
                raise PreflightFailed(f"closing #{issue['number']} failed (HTTP {status})")
            closed += 1
        deleted = 0
        for branch in self._paged(f"{repo}/branches"):
            name = branch["name"]
            if name == self.default_branch:
                continue
            ref = urllib.parse.quote(name, safe="/")
            status, _ = self.as_actor("DELETE", f"{repo}/git/refs/heads/{ref}")
            if status != 204:
                raise PreflightFailed(f"deleting a fixture branch failed (HTTP {status})")
            deleted += 1
        open_left = self._paged(f"{repo}/issues?state=open")
        branches_left = [b["name"] for b in self._paged(f"{repo}/branches")]
        if open_left or branches_left != [self.default_branch]:
            raise PreflightFailed("the fixture repository did not read back as reset")
        return {"closed": closed, "branches_deleted": deleted, "verified_clean": True}

    def ensure_label(self) -> None:
        repo = f"/repos/{self.config.repo}"
        name = urllib.parse.quote(self.config.label, safe="")
        status, _ = self.as_actor("GET", f"{repo}/labels/{name}")
        if status == 404:
            status, _ = self.as_actor(
                "POST", f"{repo}/labels", {"name": self.config.label, "color": "5319e7"}
            )
            if status != 201:
                raise PreflightFailed(f"creating the factory label failed (HTTP {status})")
        elif status != 200:
            raise PreflightFailed(f"reading the factory label failed (HTTP {status})")

    def open_labelled_issue(self) -> int:
        status, body = self.as_actor(
            "POST",
            f"/repos/{self.config.repo}/issues",
            {
                "title": f"factory preflight {self.namespace}",
                "body": "Opened by `curie dev factory-e2e preflight`. Closed on teardown.",
                "labels": [self.config.label],
            },
        )
        if status != 201 or not isinstance(body, dict):
            raise PreflightFailed(f"opening the fixture issue failed (HTTP {status})")
        number = int(body["number"])
        self.evidence["issue_number"] = number
        self.step("labelled issue opened", issue_number=number)
        return number

    def await_delivery(self, issue_number: int, since: float) -> dict[str, Any]:
        seen: set[str] = set()
        details: list[dict[str, Any]] = []

        def probe() -> dict[str, Any] | None:
            status, listing = self.as_app("GET", "/app/hook/deliveries?per_page=50")
            if status != 200 or not isinstance(listing, list):
                return None
            for item in listing:
                delivered = dt.datetime.fromisoformat(item["delivered_at"].replace("Z", "+00:00"))
                key = str(item["id"])
                if key in seen or delivered.timestamp() < since - 30:
                    continue
                if item.get("event") != "issues" or item.get("action") != "labeled":
                    continue
                status, detail = self.as_app("GET", f"/app/hook/deliveries/{key}")
                if status == 200 and isinstance(detail, dict):
                    seen.add(key)
                    details.append(detail)
            return match_delivery(details, issue_number=issue_number, repo=self.config.repo)

        found: dict[str, Any] = _wait(
            "the labelled-issue delivery", self.admission_timeout, probe, 5
        )
        return found

    def assert_admission(self, issue_number: int, since: float) -> None:
        delivery = self.await_delivery(issue_number, since)
        api_status = delivery_api_status(delivery)
        self.evidence["delivery_id"] = delivery.get("guid")
        self.evidence["delivery_status_code"] = delivery.get("status_code")
        self.evidence["delivery_api_status"] = api_status
        if delivery.get("status_code") != 200 or api_status != "factory_admitted":
            raise PreflightFailed(
                f"delivery {delivery.get('guid')} was not accepted: HTTP "
                f"{delivery.get('status_code')}, api status {api_status!r}"
            )
        self.step("delivery accepted", delivery_id=delivery.get("guid"))
        request_id = request_id_for(self.repository_id, issue_number)
        status, body = self.api(
            "GET",
            f"/v1/internal/work-items/requests/{request_id}",
            headers={"X-Curie-Worker-Token": self.worker_token},
        )
        if status != 200 or not isinstance(body, dict) or not body.get("work_item_id"):
            raise PreflightFailed(f"no WorkItem request {request_id} (HTTP {status}): {body}")
        self.evidence["execution_request_id"] = str(request_id)
        self.evidence["work_item_id"] = body["work_item_id"]
        self.evidence["execution_request_status"] = body.get("status")
        self.step("WorkItem admitted", work_item_id=body["work_item_id"])

    # --- orchestration --------------------------------------------------

    def run(self, scenario: ScenarioDriver | None) -> None:
        self.check_tools()
        self.check_images()
        self.check_app()
        self.create_namespace()
        self.install()
        self.port_forward()
        self.bind_agent()
        # Reset BEFORE the webhook points here, so the closures stay off the
        # install; the after-reset is pushed now so it runs once the webhook
        # is already restored.
        self.step("fixture reset before", **self.reset_fixture())
        self.teardown.push("reset fixture repository", self.reset_fixture)
        self.ensure_label()
        self.tunnel()
        self.repoint_webhook()
        since = time.time()
        issue = self.open_labelled_issue()
        self.assert_admission(issue, since)
        if scenario is not None:
            self.evidence["scenario"] = scenario(self)

    def write_evidence(self) -> None:
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.evidence_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(self.evidence, handle, indent=2, default=str)
            handle.write("\n")


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="curie dev factory-e2e",
        description="Drive the dark factory against a disposable install.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--context", help="kube context (default CURIE_FACTORY_KUBE_CONTEXT)")
    common.add_argument(
        "--candidate", help="commit whose published images to install (default: origin/next)"
    )
    common.add_argument("--namespace", help=f"owned namespace (default {NAMESPACE_PREFIX}<commit>)")
    common.add_argument(
        "--evidence",
        type=Path,
        help="evidence JSON path (default target/factory-e2e/<namespace>.json)",
    )
    common.add_argument(
        "--admission-timeout",
        type=float,
        default=300,
        help="seconds to wait for delivery and admission",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser(
        "preflight", parents=[common], help="install, deliver one labelled issue, assert admission"
    )
    scenario = sub.add_parser("run", parents=[common], help="preflight, then one scenario driver")
    scenario.add_argument("--scenario", required=True, choices=SCENARIO_NAMES)
    return parser.parse_args(argv)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_candidate(repo_root: Path, requested: str | None) -> str:
    ref = requested or "refs/heads/next"
    if requested and re.fullmatch(r"[0-9a-f]{40}", requested):
        return requested
    out = run(["git", "-C", str(repo_root), "ls-remote", "origin", ref])
    sha = out.split()[0] if out.split() else ""
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ConfigError(f"could not resolve candidate {ref!r} on origin; pass a full commit")
    return sha


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    driver: ScenarioDriver | None = None
    try:
        if args.mode == "run":
            driver = resolve_scenario(args.scenario)
        config = load_config(os.environ, context=args.context)
        repo_root = _repo_root()
        candidate = _resolve_candidate(repo_root, args.candidate)
        namespace = (
            validate_namespace(args.namespace) if args.namespace else default_namespace(candidate)
        )
    except ScenarioUnavailable as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return EXIT_SCENARIO
    except (ConfigError, PreflightFailed) as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    evidence_path = args.evidence or repo_root / "target" / "factory-e2e" / f"{namespace}.json"
    preflight = Preflight(
        config,
        repo_root=repo_root,
        candidate=candidate,
        namespace=namespace,
        evidence_path=evidence_path,
        admission_timeout=args.admission_timeout,
    )
    if args.mode == "run":
        preflight.evidence["mode"] = f"run:{args.scenario}"

    def _terminate(signum: int, _frame: Any) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGHUP, _terminate)

    code = 0
    try:
        preflight.run(driver)
        preflight.evidence["result"] = "passed"
    except ConfigError as exc:
        preflight.evidence["result"] = "refused"
        preflight.evidence["error"] = str(exc)
        print(f"factory-e2e: {exc}", file=sys.stderr)
        code = EXIT_CONFIG
    except BaseException as exc:  # noqa: BLE001 - always tear down
        preflight.evidence["result"] = "failed"
        preflight.evidence["error"] = f"{type(exc).__name__}: {exc}"
        if not isinstance(exc, (PreflightFailed, KeyboardInterrupt, SystemExit)):
            traceback.print_exc()
        print(f"factory-e2e: FAILED: {exc}", file=sys.stderr)
        code = EXIT_FAILED
    finally:
        log("teardown")
        results = preflight.teardown.run()
        preflight.evidence["teardown"] = results
        preflight.evidence["teardown_clean"] = all(r["ok"] for r in results)
        preflight.evidence["finished_at"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
        shutil.rmtree(preflight.workdir, ignore_errors=True)
        preflight.write_evidence()
        for result in results:
            verdict = "ok" if result["ok"] else "FAILED"
            log(f"teardown {result['step']}: {verdict} {result['detail']}")
    if not preflight.evidence["teardown_clean"]:
        print("factory-e2e: teardown incomplete; see the evidence file", file=sys.stderr)
        code = code or EXIT_FAILED
    print(f"factory-e2e: {preflight.evidence['result']}; evidence {evidence_path}", file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())

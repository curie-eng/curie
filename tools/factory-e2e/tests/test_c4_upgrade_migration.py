"""Released factory installation through the supported upgrade boundary (#4628).

All scenario support stays with this test when verify-fix-pin reverses production
files. The fixture owns preparation; the selected test owns the product verdict.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import ipaddress
import json
import os
import re
import selectors
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
REPOSITORY = "acme-corp/acme-bot"
REPOSITORY_ID = 4401
LABEL = "acme-upgrade-ready"
MENTION = "acme-upgrade-bot"
AGENT = "acme-factory"
REQUIRED_CHECK = "Acme Python aggregate"
SHARD_PREFIX = "Acme Python shard "
METADATA_CHECK = "Acme metadata"
METADATA_STATUS = "acme/policy"
WEBHOOK_SECRET = "example-c4-upgrade-webhook-secret"
LEGACY_KEYS = (
    "githubFactoryIngressEnabled",
    "githubFactoryIntake",
    "githubFactoryPollIntervalSeconds",
    "githubFactoryLabel",
    "githubFactoryMention",
    "githubFactoryCardBaseUrl",
    "githubFactoryCiWaitSeconds",
    "githubFactoryPythonCi",
    "githubFactoryBases",
    "githubFactoryMetadataCi",
)
RELEASE_HASHES = {
    "curie-x86_64-unknown-linux-gnu":
        "c06fcfae033fa6b9622462c598591faee6bea2d9df6bcf457c1cff1db8cc0086",
    "curie-0.12.5.tgz":
        "b467b461e9f7b4c769ec058dd94d977df904adc78d9afd18710673c1f061d207",
}

CI_PROBE = """
import asyncio
import json
import sys
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import httpx
from curie_api import factory_ci
from curie_api.code_host_trust import code_host_verify
from curie_api.config import get_settings
from curie_api.db import create_engine, create_sessionmaker
from curie_api.forges.hosts import factory_code_host
from curie_api.forges.registry import forges_registry
from curie_api.forges.trackers import work_item_policy
from curie_api.models import WorkItem

async def main():
    arguments = json.loads(sys.argv[1])
    settings = get_settings()
    registry = forges_registry(settings)
    engine = create_engine()
    try:
        async with create_sessionmaker(engine)() as session:
            item = await session.get(WorkItem, uuid.UUID(arguments['work_item']))
            if item is None:
                raise RuntimeError('admitted work item disappeared')
            repository = item.repository
            policy = work_item_policy(registry, item.tracker_issue, repository)
            async with httpx.AsyncClient(verify=code_host_verify(settings)) as client:
                code_host = factory_code_host(
                    registry, client, installations={repository.path: 5501}
                )
                view = await factory_ci.observe(
                    code_host, repository, arguments['head_sha'], diagnostics=False
                )
            now = datetime.now(UTC)
            verdict = factory_ci.decide(
                view, now=now, published_at=now-timedelta(seconds=75),
                execution_deadline=now+timedelta(seconds=600),
                ci_wait_seconds=settings.factory_ci_wait_s,
                changed_paths=arguments['paths'],
                python_ci=factory_ci.python_ci_policy(policy),
                metadata_ci=factory_ci.metadata_ci_policy(policy),
                fresh_after=(datetime.fromisoformat(arguments['fresh_after'])
                             if arguments.get('fresh_after') else None),
            )
            print(json.dumps({
                'verdict': asdict(verdict), 'observation_error': view.reason,
                'policy': policy.ci.model_dump(mode='json') if policy else None,
                'ci_wait_seconds': settings.factory_ci_wait_s,
            }))
    finally:
        await engine.dispose()

asyncio.run(main())
"""


def _digest(value: Any) -> str:
    __tracebackhide__ = True
    if isinstance(value, Path):
        return hashlib.sha256(value.read_bytes()).hexdigest()
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"original harness module unavailable: {path.name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _document(path: Path, value: Any) -> Path:
    __tracebackhide__ = True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, sort_keys=False))
    path.chmod(0o600)
    return path


def _pod_observations(pods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    __tracebackhide__ = True
    return sorted(({
        "name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"],
        "owners": [{
            key: owner[key] for key in ("kind", "name", "uid", "controller") if key in owner
        } for owner in pod["metadata"].get("ownerReferences", [])],
        "phase": pod.get("status", {}).get("phase"),
        "ready": any(
            row.get("type") == "Ready" and row.get("status") == "True"
            for row in pod.get("status", {}).get("conditions", [])
        ),
        "deletion_timestamp": pod["metadata"].get("deletionTimestamp"),
        "deletion_grace_seconds": pod["metadata"].get("deletionGracePeriodSeconds"),
        "termination_grace_seconds": pod.get("spec", {}).get("terminationGracePeriodSeconds", 30),
        "containers": [{
            "name": row["name"], "ready": row.get("ready", False),
            "restarts": row.get("restartCount", 0), "state": sorted(row.get("state", {})),
        } for row in pod.get("status", {}).get("containerStatuses", [])],
    } for pod in pods), key=lambda row: row["uid"])


def _github_fixture(module: Any) -> type:
    class GithubFixture(module.GithubStub):
        """Extend only external GitHub HTTP behavior, retaining TLS and SQLite."""

        def start(self) -> None:
            super().start()
            with sqlite3.connect(self._database) as database:
                database.execute(
                    "CREATE TABLE requests(method TEXT, path TEXT, query TEXT, stamp REAL)"
                )
            issue = self._get("issue", 3815)
            issue["labels"] = []
            self._put("issue", 3815, issue)

        def _rest(
            self, method: str, path: str, query: dict[str, list[str]], body: Any
        ) -> tuple[int, Any] | None:
            with sqlite3.connect(self._database) as database:
                database.execute(
                    "INSERT INTO requests VALUES(?,?,?,?)",
                    (method, path, json.dumps(query, sort_keys=True), time.time()),
                )
            repository = self._repository_row()
            if method == "GET" and path == "/app":
                return 200, {"id": 51, "slug": MENTION, "permissions": module.PERMISSIONS}
            if method == "GET" and path == "/app/installations":
                return 200, self._page(
                    [{"id": 5501, "app_id": 51, "account": {"login": "acme-corp"}}], query
                )
            if method == "GET" and path == "/installation/repositories":
                return 200, {"total_count": 1, "repositories": self._page([repository], query)}
            if method == "GET" and path.startswith("/users/"):
                login = path.removeprefix("/users/")
                if login != f"{MENTION}[bot]":
                    return 404, {"message": "Not Found"}
                return 200, {"id": 51, "login": login, "type": "Bot"}
            if method == "POST" and path == "/app/installations/5501/access_tokens":
                # GitHub permits omitted scope for installation enumeration and
                # explicit scope for repository reads. Both are used by released code.
                # https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app
                if body.get("repositories") not in (None, ["acme-bot"]):
                    return 422, {"message": "repository scope refused"}
                if body.get("repository_ids") not in (None, [REPOSITORY_ID]):
                    return 422, {"message": "repository identity refused"}
                token = "example-c4-installation-token"
                with sqlite3.connect(self._database) as database:
                    database.execute(
                        "INSERT OR REPLACE INTO tokens VALUES(?,?)",
                        (token, json.dumps(module.PERMISSIONS)),
                    )
                return 201, {
                    "token": token,
                    "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "permissions": module.PERMISSIONS,
                    "repositories": [repository],
                    "repository_selection": "selected",
                }
            prefix = f"/repos/{REPOSITORY}"
            if path.startswith(f"/repositories/{REPOSITORY_ID}"):
                path = prefix + path.removeprefix(f"/repositories/{REPOSITORY_ID}")
            if path.startswith(prefix + "/collaborators/") and path.endswith("/permission"):
                login = path.split("/")[-2]
                return 200, {
                    "permission": "write" if login == "octocat" else "read",
                    "user": module.HUMAN if login == "octocat" else {
                        "id": 6602, "login": login, "type": "User"
                    },
                }
            if path == prefix + "/labels" or path.startswith(prefix + "/labels/"):
                labels = self._rows("label")
                if method == "POST" and path == prefix + "/labels":
                    label = {"id": len(labels) + 1, "name": body["name"], "color": "336699"}
                    self._put("label", label["id"], label)
                    return 201, label
                if method == "GET" and path == prefix + "/labels":
                    return 200, self._page(labels, query)
                if method == "GET":
                    found = next(
                        (row for row in labels if row["name"] == path.split("/")[-1]), None
                    )
                    return (200, found) if found else (404, {"message": "Not Found"})
            if method == "GET" and path.startswith(prefix + "/contents"):
                # https://docs.github.com/en/rest/repos/contents#get-repository-content
                relative = path.removeprefix(prefix + "/contents").strip("/")
                if relative == "":
                    return 200, [{"type": "file", "path": "README.md", "name": "README.md"}]
                if relative == ".github/workflows":
                    return 404, {"message": "Not Found"}
                if relative == "README.md":
                    return 200, {
                        "type": "file", "encoding": "base64",
                        "content": base64.b64encode(b"Example fixture repository\n").decode(),
                    }
                return 404, {"message": "Not Found"}
            if method == "GET" and path in {
                prefix + "/commits/main", prefix + "/commits/maint", prefix + "/commits/release"
            }:
                return 200, {"sha": self._ref("main")}
            if method == "GET" and (
                path.startswith(prefix + "/branches/")
                or path.startswith(prefix + "/git/ref/heads/")
            ):
                branch = path.rsplit("/", 1)[-1]
                if branch not in {"main", "maint", "release"}:
                    return 404, {"message": "Branch not found"}
                sha = self._ref("main")
                if "/git/ref/" in path:
                    return 200, {"ref": f"refs/heads/{branch}", "object": {"sha": sha}}
                return 200, {"name": branch, "commit": {"sha": sha}}
            if method == "GET" and path.endswith("/timeline"):
                number = int(path.split("/")[-2])
                return 200, self._page(
                    [row for row in self._rows("event") if row["issue_number"] == number], query
                )
            if method == "GET" and path.startswith(prefix + "/commits/"):
                if path.endswith("/check-runs"):
                    rows = self._rows("ci-check")
                    return 200, {"total_count": len(rows), "check_runs": self._page(rows, query)}
                if path.endswith("/status") or path.endswith("/statuses"):
                    rows = self._rows("ci-status")
                    if path.endswith("/statuses"):
                        return 200, self._page(rows, query)
                    return 200, {"state": "success", "total_count": len(rows), "statuses": rows}
            return super()._rest(method, path, query, body)

        def issue(self, number: int, labels: list[str], *, authorized: bool = True) -> None:
            actor = module.HUMAN if authorized else {
                "id": 6602, "login": "outside-reader", "type": "User"
            }
            issue = self._issue(number, "Example upgrade admission", "Example task", labels)
            issue["user"] = actor
            self._put("issue", number, issue)
            for label in labels:
                self._label_event(number, label, "labeled", actor)

        def comment(self, number: int, body: str) -> int:
            identifier = max((row["id"] for row in self._rows("comment")), default=0) + 1
            self._put("comment", identifier, {
                "id": identifier, "body": body, "issue_number": number,
                "user": module.HUMAN, "created_at": module._utc(), "updated_at": module._utc(),
                # Enterprise REST issue_url includes the API root used to read it.
                # https://docs.github.com/en/enterprise-server@3.17/rest/issues/comments
                "issue_url": f"{self.base_url}/api/v3/repos/{REPOSITORY}/issues/{number}",
                "html_url": (
                    f"{self.base_url}/{REPOSITORY}/issues/{number}#issuecomment-{identifier}"
                ),
            })
            return identifier

        def persistent_digest(self) -> str:
            with sqlite3.connect(self._database) as database:
                rows = database.execute(
                    "SELECT kind,id,body FROM objects ORDER BY kind,id"
                ).fetchall()
            return _digest(rows)

        def requests_after(self, stamp: float, path: str) -> list[dict[str, list[str]]]:
            with sqlite3.connect(self._database) as database:
                rows = database.execute(
                    "SELECT query FROM requests WHERE method='GET' AND path=? AND stamp>=?",
                    (path, stamp),
                ).fetchall()
            return [json.loads(row[0]) for row in rows]

        def ci(self, checks: list[dict[str, Any]], statuses: list[dict[str, Any]]) -> None:
            with sqlite3.connect(self._database) as database:
                database.execute("DELETE FROM objects WHERE kind IN ('ci-check','ci-status')")
            for index, row in enumerate(checks):
                self._put("ci-check", index, row)
            for index, row in enumerate(statuses):
                self._put("ci-status", index, row)

    return GithubFixture


class _HttpFacade:
    """The released CLI's scoped cleartext entry to the same HTTPS fixture."""

    def __init__(self, stub: Any) -> None:
        self.stub = stub
        self.server: ThreadingHTTPServer | None = None
        self.thread: threading.Thread | None = None
        self.port = 0

    def start(self) -> None:
        facade = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *args: Any) -> None:
                pass

            def handle_request(self) -> None:
                __tracebackhide__ = True
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() in {"authorization", "content-type", "accept"}
                }
                request = Request(
                    facade.stub.base_url + self.path, data=raw if raw else None,
                    headers=headers, method=self.command,
                )
                context = ssl.create_default_context(cafile=str(facade.stub.ca_file))
                try:
                    response = urlopen(request, context=context, timeout=30)
                except HTTPError as error:
                    response = error
                with response:
                    payload = response.read()
                    self.send_response(response.code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)

            do_GET = handle_request
            do_POST = handle_request
            do_PATCH = handle_request

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=5)
            if self.thread.is_alive():
                raise RuntimeError("HTTP facade thread did not stop")
        if self.port:
            with socket.socket() as connection:
                if connection.connect_ex(("127.0.0.1", self.port)) == 0:
                    raise RuntimeError("HTTP facade still listens after teardown")


class _ObservedValues(dict[str, Any]):
    def __repr__(self) -> str:
        return "<observed values; raw contents retained privately>"


class _UpgradeResult:
    def __init__(self, result: subprocess.CompletedProcess[str]) -> None:
        __tracebackhide__ = True
        self.returncode = result.returncode
        self._stdout = result.stdout
        self._stderr = result.stderr

    def __repr__(self) -> str:
        return f"<upgrade result; exit code {self.returncode}; raw output retained privately>"

    def document(self) -> _ObservedValues:
        __tracebackhide__ = True
        try:
            document = json.loads(self._stdout)
        except json.JSONDecodeError:
            raise RuntimeError("upgrade output was not a JSON document") from None
        if not isinstance(document, dict):
            raise RuntimeError("upgrade output was not a JSON object")
        return _ObservedValues(document)

    def contains(self, value: str) -> bool:
        __tracebackhide__ = True
        return value in self._stdout + self._stderr

    def omits(self, *values: str) -> bool:
        __tracebackhide__ = True
        return all(not self.contains(value) for value in values)

    def legacy_guard(self) -> str | None:
        __tracebackhide__ = True
        return next((
            f"api.{key} is no longer read"
            for key in LEGACY_KEYS if self.contains(f"api.{key} is no longer read")
        ), None)


class _Candidate:
    def __init__(self) -> None:
        self.run_id = uuid.uuid4().hex[:12]
        self.work = ROOT / ".projects" / "4628" / f"c4-{self.run_id}"
        self.source = self.work / "source"
        self.binary = self.work / "curie"
        self.old_binary = self.work / "curie-v0.12.5"
        self.assets = Path(os.environ.get(
            "CURIE_C4_RELEASE_ASSETS", ROOT / ".projects/4628/assets"
        ))
        self.old_chart = self.assets / "curie-0.12.5.tgz"
        self.env = dict(os.environ)
        self.images: dict[str, str] = {}
        self.image_ids: dict[str, str] = {}
        self.image_descriptors: dict[str, dict[str, str]] = {}
        self.old_images: dict[str, str] = {}
        self.source_digest = ""
        self.target = ""
        self.failures: list[str] = []
        self.docker_endpoint = ""

    def command(
        self, *argv: str, env: dict[str, str] | None = None, timeout: int = 900,
        checked: bool = True, input_: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        __tracebackhide__ = True
        try:
            result = subprocess.run(
                argv, cwd=ROOT, env=env or self.env, input=input_, text=True,
                capture_output=True, timeout=timeout, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            self.failures.append(f"infrastructure operation {Path(argv[0]).name} did not complete")
            raise RuntimeError(self.failures[-1]) from None
        if result.returncode:
            self.work.mkdir(parents=True, exist_ok=True)
            diagnostics = self.work / "operations.sqlite3"
            diagnostics.touch(mode=0o600, exist_ok=True)
            diagnostics.chmod(0o600)
            with sqlite3.connect(diagnostics) as database:
                database.execute(
                    "CREATE TABLE IF NOT EXISTS failures("
                    "operation TEXT, exit_code INTEGER, stdout TEXT, stderr TEXT)"
                )
                database.execute(
                    "INSERT INTO failures VALUES(?,?,?,?)",
                    (f"{Path(argv[0]).name} {argv[1]}", result.returncode,
                     result.stdout, result.stderr),
                )
        if checked and result.returncode:
            self.failures.append(
                f"infrastructure operation {Path(argv[0]).name} {argv[1]} "
                f"exited {result.returncode}"
            )
            raise RuntimeError(self.failures[-1])
        return result

    def observe(self, installation: str, operation: str, value: Any) -> None:
        __tracebackhide__ = True
        try:
            diagnostics = self.work / "operations.sqlite3"
            diagnostics.touch(mode=0o600, exist_ok=True)
            diagnostics.chmod(0o600)
            with sqlite3.connect(diagnostics) as database:
                database.execute(
                    "CREATE TABLE IF NOT EXISTS observations("
                    "installation TEXT, operation TEXT, stamp REAL, body TEXT)"
                )
                database.execute(
                    "INSERT INTO observations VALUES(?,?,?,?)",
                    (installation, operation, time.time(), json.dumps(value, sort_keys=True)),
                )
        except (OSError, sqlite3.Error):
            self.failures.append("private observation receipt could not be persisted")
            raise RuntimeError(self.failures[-1]) from None

    def prepare(self) -> None:
        __tracebackhide__ = True
        for executable in ("cargo", "docker", "kind", "helm", "kubectl", "openssl", "git"):
            if shutil.which(executable) is None:
                raise RuntimeError(f"required executable unavailable: {executable}")
        hosted_ci = (
            self.env.get("GITHUB_ACTIONS") == "true"
            and self.env.get("RUNNER_ENVIRONMENT") == "github-hosted"
        )
        if hosted_ci:
            workspace = Path(self.env.get("GITHUB_WORKSPACE", "/missing-workspace")).resolve()
            if not workspace.is_dir() or not ROOT.is_relative_to(workspace):
                raise RuntimeError("hosted CI checkout must belong to GITHUB_WORKSPACE")
        context = self.env.get("DOCKER_CONTEXT")
        host = self.env.get("DOCKER_HOST", "")
        if context or not host:
            selected_context = context or self.command(
                "docker", "context", "show"
            ).stdout.strip()
            details = json.loads(self.command(
                "docker", "context", "inspect", selected_context
            ).stdout)[0]
            endpoint = details["Endpoints"]["docker"]["Host"]
        else:
            endpoint = host
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise RuntimeError("selected Docker endpoint could not be resolved")
        self.docker_endpoint = endpoint
        if hosted_ci and not endpoint.startswith("unix://"):
            raise RuntimeError("hosted CI Docker must be the runner-owned Unix engine")
        self.command("docker", "info", timeout=30)
        self.work.mkdir(parents=True)
        for name, expected in RELEASE_HASHES.items():
            asset = self.assets / name
            if not asset.is_file() or _digest(asset) != expected:
                raise RuntimeError("v0.12.5 release asset missing or checksum mismatch")
        shutil.copyfile(self.assets / "curie-x86_64-unknown-linux-gnu", self.old_binary)
        self.old_binary.chmod(0o700)
        if "0.12.5" not in self.command(str(self.old_binary), "--version").stdout:
            raise RuntimeError("checksum-verified released CLI has an unexpected version")
        paths = self.command(
            "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"
        ).stdout.split("\0")
        digest = hashlib.sha256()
        for relative in sorted(set(paths) - {""}):
            path = ROOT / relative
            if any(part in {".projects", ".worktrees", ".git"} for part in Path(relative).parts):
                continue
            if path.is_symlink():
                raise RuntimeError("source snapshot requires regular build inputs")
            if not path.is_file():
                continue
            data = path.read_bytes()
            digest.update(relative.encode() + b"\0" + hashlib.sha256(data).digest())
            destination = self.source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
        self.source_digest = digest.hexdigest()
        chart = yaml.safe_load((self.source / "charts/curie/Chart.yaml").read_text())
        self.target = str(chart["version"])
        build_env = dict(self.env)
        build_env["CARGO_TARGET_DIR"] = str(ROOT / ".projects/4628/cargo-target")
        self.command(
            "cargo", "build", "--locked", "--manifest-path", str(self.source / "cli/Cargo.toml"),
            "--bin", "curie", env=build_env, timeout=2400,
        )
        shutil.copy2(Path(build_env["CARGO_TARGET_DIR"]) / "debug/curie", self.binary)
        for component in ("api", "worker", "ui", "runner"):
            image = f"curie-c4-{component}:{self.run_id}"
            self.images[component] = image
            dockerfile = (
                "runner/Dockerfile" if component == "runner" else f"apps/{component}/Dockerfile"
            )
            self.command(
                "docker", "build", "--label", f"curie.c4.run={self.run_id}",
                "--label", f"curie.c4.source={self.source_digest}", "-t", image,
                "-f", str(self.source / dockerfile), str(self.source), timeout=2400,
            )
            inspected = json.loads(self.command("docker", "image", "inspect", image).stdout)[0]
            self.image_ids[component] = inspected["Id"]
            descriptor = inspected.get("Descriptor")
            if descriptor is not None:
                if (
                    not isinstance(descriptor, dict)
                    or not isinstance(descriptor.get("mediaType"), str)
                    or not isinstance(descriptor.get("digest"), str)
                    or not re.fullmatch(r"sha256:[0-9a-f]{64}", descriptor["digest"])
                ):
                    raise RuntimeError("built image descriptor is missing its type or digest")
                self.image_descriptors[component] = {
                    "digest": descriptor["digest"], "mediaType": descriptor["mediaType"],
                }
            released = f"ghcr.io/curie-eng/curie-{component}:0.12.5"
            self.command("docker", "pull", released, timeout=900)
            old = json.loads(self.command("docker", "image", "inspect", released).stdout)[0]
            repository = released.rsplit(":", 1)[0]
            references = [
                ref for ref in old["RepoDigests"] if ref.startswith(repository + "@sha256:")
            ]
            if len(references) != 1:
                raise RuntimeError("released image has no unique published content digest")
            self.old_images[component] = references[0]

    def cleanup(self) -> None:
        __tracebackhide__ = True
        for image in self.images.values():
            try:
                removed = self.command("docker", "image", "rm", image, checked=False, timeout=60)
                if removed.returncode and "No such image" not in removed.stderr:
                    self.failures.append("private candidate image alias could not be removed")
                remaining = self.command(
                    "docker", "image", "inspect", image, checked=False, timeout=30
                )
                if remaining.returncode == 0:
                    self.failures.append("private candidate image alias remains")
            except (RuntimeError, OSError, subprocess.SubprocessError, sqlite3.Error):
                self.failures.append("private candidate image cleanup did not complete")
        try:
            expected = ROOT / ".projects" / "4628" / f"c4-{self.run_id}" / "source"
            if (
                self.source != expected
                or self.source.is_symlink()
                or self.source.resolve() != expected
            ):
                self.failures.append("source snapshot cleanup refused an unexpected or linked path")
            else:
                if self.source.exists():
                    shutil.rmtree(self.source)
                if self.source.exists() or self.source.is_symlink():
                    self.failures.append("owned source snapshot remains")
        except (RuntimeError, OSError):
            self.failures.append("owned source snapshot cleanup did not complete")
        if self.failures:
            raise RuntimeError("; ".join(self.failures))


@pytest.fixture(scope="module", autouse=True)
def candidate_curie(request: pytest.FixtureRequest) -> _Candidate:
    """Override the directory conftest's fake executable in both pin phases."""
    candidate = _Candidate()
    request.addfinalizer(candidate.cleanup)
    candidate.prepare()
    return candidate


class _Installation:
    def __init__(
        self, candidate: _Candidate, request: pytest.FixtureRequest, *, required_ci: bool = True,
    ) -> None:
        self.candidate = candidate
        self.request = request
        self.required_ci = required_ci
        self.identity = uuid.uuid4().hex[:10]
        self.cluster = f"curie-c4-{self.identity}"
        self.context = f"kind-{self.cluster}"
        self.namespace = f"c4-{self.identity}"
        self.release = f"curie-c4-{self.identity}"
        self.work = candidate.work / self.identity
        self.env = dict(candidate.env)
        self.env["KUBECONFIG"] = str(self.work / "kubeconfig")
        self.env["CURIE_CONFIG_DIR"] = str(self.work / "cli-config")
        for key in (
            "CURIE_BIN", "CURIE_UPGRADE_TEST_FAIL_AFTER", "CURIE_UPGRADE_TEST_FAIL_PHASE",
            "CURIE_CREDENTIALS", "CURIE_MODEL_CREDENTIALS", "CURIE_GITHUB_TOKEN",
            "CURIE_CONTEXT", "CURIE_TARGET", "CURIE_NAMESPACE", "GITHUB_TOKEN",
        ):
            self.env.pop(key, None)
        self.env["CURIE_GITHUB_API_URL"] = "http://127.0.0.1:1/c4-unreachable-curie"
        self.env["GITHUB_API_URL"] = "http://127.0.0.1:1/c4-unreachable-github"
        self.scripted: Any = None
        self.stub: Any = None
        self.facade: _HttpFacade | None = None
        self.forward: subprocess.Popen[str] | None = None
        self.cluster_attempted = False
        self.api_url = ""
        self.api_key = ""
        self.agent_id = ""
        self.trust_file = self.work / "trust.yaml"
        self.images_file = self.work / "images.yaml"
        self.baseline: dict[str, Any] = {}
        self.baseline_revision = 0
        self.released_secret_digest = ""
        self.completed_hook_pods: set[str] = set()

    def command(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        __tracebackhide__ = True
        return self.candidate.command(*argv, env=kwargs.pop("env", self.env), **kwargs)

    def kubectl(self, *argv: str, **kwargs: Any) -> str:
        __tracebackhide__ = True
        return self.command(
            "kubectl", "--kubeconfig", self.env["KUBECONFIG"], "--context", self.context,
            *argv, **kwargs,
        ).stdout

    def helm(self, *argv: str) -> Any:
        __tracebackhide__ = True
        return json.loads(self.command(
            "helm", "--kubeconfig", self.env["KUBECONFIG"], "--kube-context", self.context,
            *argv, "-n", self.namespace, "-o", "json",
        ).stdout)

    def values(self, *, computed: bool = False) -> dict[str, Any]:
        __tracebackhide__ = True
        history = self.history()
        revision = str(history[-1]["revision"])
        return _ObservedValues(self.helm(
            "get", "values", self.release, "--revision", revision,
            *(["--all"] if computed else []),
        ))

    def history(self) -> list[dict[str, Any]]:
        return self.helm("history", self.release)

    def sql(self, query: str) -> Any:
        __tracebackhide__ = True
        output = self.kubectl(
            "-n", self.namespace, "exec", f"statefulset/{self.release}-postgres", "--",
            "psql", "-U", "postgres", "-d", "postgres", "-At", "-v", "ON_ERROR_STOP=1",
            "-c", "BEGIN READ ONLY; " + query + "; COMMIT;",
        )
        lines = [line for line in output.splitlines() if line not in {"BEGIN", "COMMIT"}]
        if len(lines) != 1:
            raise RuntimeError("read-only storage observation did not return one JSON row")
        return json.loads(lines[0])

    def api(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        __tracebackhide__ = True
        request = Request(
            self.api_url + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"X-API-Key": self.api_key, "Content-Type": "application/json"},
        )
        try:
            response = urlopen(request, timeout=30)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            self.candidate.failures.append("installed API transport unavailable")
            raise RuntimeError(self.candidate.failures[-1]) from None
        with response:
            return response.code, json.load(response)

    def _forward_api(self) -> None:
        __tracebackhide__ = True
        if self.forward is not None:
            self.forward.terminate()
            self.forward.wait(timeout=10)
            if self.forward.stdout is not None:
                self.forward.stdout.close()
        try:
            self.forward = subprocess.Popen(
                ["kubectl", "--kubeconfig", self.env["KUBECONFIG"], "--context", self.context,
                 "-n", self.namespace, "port-forward", f"svc/{self.release}-api", ":8000",
                 "--address", "127.0.0.1"],
                cwd=ROOT, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
        except (OSError, subprocess.SubprocessError):
            self.candidate.failures.append("API port-forward process did not start")
            raise RuntimeError(self.candidate.failures[-1]) from None
        with selectors.DefaultSelector() as selector:
            if self.forward.stdout is None:
                raise RuntimeError("API port-forward has no readiness stream")
            selector.register(self.forward.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=45):
                raise RuntimeError("API port-forward readiness timed out")
            line = self.forward.stdout.readline()
        match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", line)
        if match is None:
            raise RuntimeError("API port-forward did not publish a loopback endpoint")
        self.api_url = f"http://127.0.0.1:{match.group(1)}"

    def cli(
        self, binary: Path, verb: str, *argv: str, checked: bool = False,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        __tracebackhide__ = True
        return self.command(
            str(binary), "--json", "cluster", "--context", self.context, verb,
            "--namespace", self.namespace, "--release", self.release, *argv,
            checked=checked, timeout=1800, env=env or self.env,
        )

    def upgrade(self, *argv: str) -> _UpgradeResult:
        __tracebackhide__ = True
        return _UpgradeResult(self.cli(
            self.candidate.binary, "upgrade", "--to", self.candidate.target,
            "--chart", str(self.candidate.source / "charts/curie"),
            "--yes", "--forward-only", "-f", str(self.images_file),
            "-f", str(self.trust_file), *argv,
        ))

    def upgrade_values_absent(self, result: _UpgradeResult, *values: str) -> bool:
        __tracebackhide__ = True
        return result.omits(
            LABEL, MENTION, REQUIRED_CHECK, WEBHOOK_SECRET, self.api_key, "PRIVATE KEY", *values,
        )

    def setup(self) -> None:
        __tracebackhide__ = True
        self.work.mkdir(parents=True)
        self.scripted = _load(
            f"c4_scripted_{self.identity}", ROOT / "tools/factory-e2e/scripted_kind.py"
        )
        github = _load(
            f"c4_github_{self.identity}", ROOT / "tools/github-stub/github_stub.py"
        )
        configuration = _document(self.work / "kind.yaml", {
            "kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4",
            "nodes": [{"role": "control-plane"}],
        })
        self.cluster_attempted = True
        self.command(
            "kind", "create", "cluster", "--name", self.cluster, "--image",
            "kindest/node:v1.31.0@sha256:25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93",
            "--config", str(configuration), "--kubeconfig", self.env["KUBECONFIG"],
            "--wait", "180s", timeout=360,
        )
        for image in self.candidate.images.values():
            self.command("kind", "load", "docker-image", image, "--name", self.cluster, timeout=900)
        node = json.loads(self.command(
            "docker", "inspect", f"{self.cluster}-control-plane"
        ).stdout)[0]
        gateway = node["NetworkSettings"]["Networks"]["kind"]["Gateway"]
        callback = self.scripted.in_process_callback_host(
            docker_host=self.candidate.docker_endpoint,
            stub_host=self.env.get("CURIE_LOCAL_STUB_HOST"), gateway=gateway,
        )
        ipaddress.IPv4Address(callback)
        seed = self.work / "seed"
        seed.mkdir()
        (seed / "README.md").write_text("Example fixture repository\n")
        issue_file = self.work / "issue.md"
        issue_file.write_text("An example upgrade admission task.\n")
        self.stub = _github_fixture(github)(
            self.work / "github", {
                "version": 1, "epoch": "2026-01-01T00:00:00Z",
                "check_runs": [], "statuses": [],
            }, host=callback, bind=callback, seed_tree=seed, issue_file=issue_file,
        )
        self.stub.start()
        self.facade = _HttpFacade(self.stub)
        self.facade.start()
        self.kubectl("create", "namespace", self.namespace)
        self.kubectl(
            "-n", self.namespace, "create", "configmap", "c4-code-host-ca",
            f"--from-file=ca.pem={self.stub.ca_file}",
        )
        egress = {"cidr": f"{callback}/32", "ports": [{"protocol": "TCP", "port": self.stub.port}]}
        values: dict[str, Any] = {
            "api": {
                "githubApiUrl": self.stub.base_url + "/api/v3",
                "githubStubCaConfigMap": "c4-code-host-ca",
                "githubWebhookSecret": WEBHOOK_SECRET,
                "githubFactoryPollIntervalSeconds": 3,
                "githubFactoryCiWaitSeconds": 137,
                "githubFactoryCardBaseUrl": "https://cards.example.com",
                "githubFactoryBases": {
                    "Acme-Corp/Acme-Bot": {
                        "bases": ["main", "maint", "release"], "default_base": "maint"
                    }
                },
                "githubFactoryMetadataCi": {
                    "Acme-Corp/Acme-Bot": {
                        "checks": [METADATA_CHECK], "statuses": [METADATA_STATUS]
                    }
                },
                "orgName": "Example retained organization",
            },
            "dispatcher": {"deploy": False, "placeholderText": "Example retained placeholder"},
            "mailAdapter": {"deploy": False},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "security": {"gvisor": {"mode": "off"}, "networkPolicy": {"allowedEgress": [egress]}},
            "agentSandbox": {
                "deploy": True, "controller": {"deploy": True},
                "connectorEgress": {AGENT: [egress]},
                "runner": {"model": "example-retained-model", "prewarm": {"replicas": 0}},
            },
            "worker": {
                "maxConcurrency": 1, "runnerTotalTimeoutSeconds": 120, "deliveryBudgetSeconds": 180
            },
            "resourceQuota": {"hard": {"sandboxPodCount": "3"}},
        }
        if self.required_ci:
            values["api"]["githubFactoryPythonCi"] = {
                "Acme-Corp/Acme-Bot": {
                    "check": REQUIRED_CHECK, "paths": ["src", "tests"],
                    "pendingCheckPrefix": SHARD_PREFIX,
                }
            }
        images: dict[str, Any] = {}
        for component in ("api", "worker", "ui"):
            repository, digest = self.candidate.old_images[component].split("@", 1)
            values.setdefault(component, {})["image"] = {
                "repository": repository, "tag": "0.12.5", "digest": digest,
                "pullPolicy": "IfNotPresent"
            }
            repository, tag = self.candidate.images[component].rsplit(":", 1)
            images[component] = {"image": {
                "repository": repository, "tag": tag, "digest": "", "pullPolicy": "Never"
            }}
        repository, digest = self.candidate.old_images["runner"].split("@", 1)
        values["agentSandbox"]["runner"].update({
            "image": repository, "tag": "0.12.5", "digest": digest,
            "imagePullPolicy": "IfNotPresent"
        })
        repository, tag = self.candidate.images["runner"].rsplit(":", 1)
        images["agentSandbox"] = {"runner": {
            "image": repository, "tag": tag, "digest": "", "imagePullPolicy": "Never",
            "prewarm": {"imagePullPolicy": "Never"},
        }}
        _document(self.images_file, images)
        _document(self.trust_file, {
            "codeHostTrust": {"caBundle": {
                "configMapRef": {"name": "c4-code-host-ca", "key": "ca.pem"}
            }}
        })
        old_values = _document(self.work / "released.yaml", values)
        self.cli(
            self.candidate.old_binary, "up", "--chart", str(self.candidate.old_chart),
            "--no-expose", "--fake-model", "--adopt", "--forward-only", "-f", str(old_values),
            checked=True,
        )
        self._ready()
        computed = self.values(computed=True)
        caller = computed.get("connectorCaller", {})
        if computed.get("config", {}).get("schemaVersion") != "0.9.0":
            raise RuntimeError("released cluster up did not establish schema 0.9.0")
        if not (
            caller.get("existingSecret") or (caller.get("signingKey") and caller.get("verifyKey"))
        ):
            raise RuntimeError("released cluster up did not establish connector caller keys")
        secrets = json.loads(self.kubectl(
            "-n", self.namespace, "get", "secret", f"{self.release}-secrets", "-o", "json"
        ))
        self.api_key = base64.b64decode(secrets["data"]["apiKey"]).decode()
        self._forward_api()
        if self.api("GET", "/health")[0] != 200:
            raise RuntimeError("released API is not serving before factory setup")
        key = self.work / "app.pem"
        self.command("openssl", "genrsa", "-out", str(key), "2048")
        key.chmod(0o600)
        old_env = dict(self.env)
        old_env["CURIE_GITHUB_API_URL"] = f"http://127.0.0.1:{self.facade.port}/api/v3"
        old_env["GITHUB_API_URL"] = old_env["CURIE_GITHUB_API_URL"]
        self.cli(
            self.candidate.old_binary, "factory", "--chart", str(self.candidate.old_chart),
            "--intake", "poll", "--repo", REPOSITORY, "--app-id", "51",
            "--private-key-file", str(key), "--label", LABEL, "--mention", MENTION,
            checked=True, env=old_env,
        )
        self._ready()
        self._settle_released_workloads()
        self._forward_api()
        status, agent = self.api("POST", "/agents", {
            "name": AGENT, "repo_full_name": REPOSITORY,
            "channel": {"kind": "github", "address": REPOSITORY},
            "model": "example-retained-agent-model",
        })
        if status != 201:
            raise RuntimeError(f"released agent surface setup returned HTTP {status}")
        self.agent_id = agent["id"]
        self.baseline_revision = int(self.history()[-1]["revision"])
        self.baseline = self.values()
        old_api = self.values(computed=True)["api"]
        if (
            old_api.get("githubFactoryLabel") != LABEL
            or old_api.get("githubFactoryMention") != MENTION
            or old_api.get("githubFactoryIntake") != "poll"
            or old_api.get("githubFactoryPythonCi")
            != values["api"].get("githubFactoryPythonCi", {})
            or self.baseline.get("factory")
            or old_api.get("githubCloneBase") != "https://github.com"
        ):
            raise RuntimeError("released CLI did not establish the independent legacy baseline")
        self.released_secret_digest = self.app_secret_digest()
        self.request.node.user_properties.append(("c4_setup_complete", json.dumps({
            "run_id": self.candidate.run_id, "installation": self.identity,
            "released_version": "0.12.5", "candidate_version": self.candidate.target,
            "source_content_sha256": self.candidate.source_digest,
            "candidate_cli_sha256": _digest(self.candidate.binary),
            "candidate_image_ids": self.candidate.image_ids,
            "candidate_image_descriptors": self.candidate.image_descriptors,
            "released_asset_sha256": RELEASE_HASHES,
            "baseline_revision": self.baseline_revision,
            "schema": "0.9.0", "caller_keys": True,
        }, sort_keys=True)))

    def _ready(self) -> None:
        for component in ("api", "worker", "ui"):
            self.kubectl(
                "-n", self.namespace, "rollout", "status", f"deployment/{self.release}-{component}",
                "--timeout=600s", timeout=660,
            )

    def _settle_released_workloads(self) -> None:
        __tracebackhide__ = True
        # Rollout counters exclude deleting Pods; require their actual absence too.
        # https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/controller/controller_utils.go
        started = time.monotonic()
        deadline: float | None = None
        previous_ready: str | None = None
        last_observation: str | None = None
        attempts = 0
        while True:
            attempts += 1
            inventory = json.loads(self.kubectl(
                "-n", self.namespace, "get",
                "deployments,statefulsets,daemonsets,replicasets,pods", "-o", "json",
            ))["items"]
            pods = _pod_observations([row for row in inventory if row["kind"] == "Pod"])
            controllers = {
                row["metadata"]["uid"]: row for row in inventory if row["kind"] != "Pod"
            }
            workloads = {
                uid: row for uid, row in controllers.items() if row["kind"] != "ReplicaSet"
            }
            if deadline is None:
                grace = max((row["termination_grace_seconds"] for row in pods), default=30)
                deadline = started + max(600, grace + 120)
            counts = dict.fromkeys(workloads, 0)
            blockers: list[str] = []
            for pod in pods:
                if pod["deletion_timestamp"] is not None:
                    blockers.append(f"pod/{pod['name']}:terminating")
                owner = next((row for row in pod["owners"] if row.get("controller")), {})
                if owner.get("kind") == "Job":
                    if pod["phase"] != "Succeeded":
                        blockers.append(f"pod/{pod['name']}:hook-not-complete")
                    continue
                if pod["phase"] != "Running" or not pod["ready"]:
                    blockers.append(f"pod/{pod['name']}:not-ready")
                if owner.get("kind") == "ReplicaSet":
                    replica_set = controllers.get(owner.get("uid"), {})
                    owner = next((
                        row for row in replica_set.get("metadata", {}).get("ownerReferences", [])
                        if row.get("controller")
                    ), {})
                if owner.get("uid") in counts:
                    counts[owner["uid"]] += 1
                else:
                    blockers.append(f"pod/{pod['name']}:no-serving-controller")
            observed_workloads: dict[str, Any] = {}
            for uid, workload in workloads.items():
                kind = workload["kind"]
                metadata, spec = workload["metadata"], workload["spec"]
                status = workload.get("status", {})
                if kind == "DaemonSet":
                    desired = status.get("desiredNumberScheduled", 0)
                    fields = (
                        "currentNumberScheduled", "updatedNumberScheduled", "numberReady",
                        "numberAvailable",
                    )
                else:
                    desired = spec.get("replicas", 1)
                    fields = ("replicas", "updatedReplicas", "readyReplicas")
                    if kind == "Deployment":
                        fields += ("availableReplicas",)
                observed = {
                    "uid": uid, "generation": metadata["generation"], "spec": _digest(spec),
                    "observed_generation": status.get("observedGeneration", 0),
                    "desired": desired, "actual_pods": counts[uid],
                    "replicas": {field: status.get(field, 0) for field in fields},
                    "current_revision": status.get("currentRevision"),
                    "update_revision": status.get("updateRevision"),
                }
                key = f"{kind}/{metadata['name']}"
                observed_workloads[key] = observed
                if (
                    observed["observed_generation"] < observed["generation"]
                    or counts[uid] != desired
                    or any(count != desired for count in observed["replicas"].values())
                    or (
                        kind == "StatefulSet" and desired
                        and observed["current_revision"] != observed["update_revision"]
                    )
                ):
                    blockers.append(f"{key}:not-converged")
            if not workloads:
                blockers.append("serving controllers absent")
            observation = {
                "pods": pods, "workloads": observed_workloads, "blockers": sorted(blockers),
            }
            identity = _digest(observation)
            if identity != last_observation:
                self.candidate.observe(self.identity, "released-readiness", observation)
                last_observation = identity
            if not blockers and identity == previous_ready:
                self.request.node.user_properties.append((
                    "c4_released_workloads_ready", json.dumps({
                        "observation_sha256": identity, "pod_uids": [row["uid"] for row in pods],
                        "workloads": sorted(observed_workloads), "attempts": attempts,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                        "timeout_seconds": round(deadline - started, 3),
                    }, sort_keys=True),
                ))
                return
            previous_ready = identity if not blockers else None
            if time.monotonic() >= deadline:
                self.candidate.observe(self.identity, "released-readiness-timeout", observation)
                self.candidate.failures.append("released workload baseline did not settle")
                raise RuntimeError(self.candidate.failures[-1])
            time.sleep(2)

    def app_secret_digest(self) -> str:
        __tracebackhide__ = True
        name = self.baseline["api"]["githubAppExistingSecret"]
        secret = json.loads(self.kubectl("-n", self.namespace, "get", "secret", name, "-o", "json"))
        return _digest(secret["data"])

    def snapshot(self) -> dict[str, Any]:
        __tracebackhide__ = True
        pods = json.loads(self.kubectl("-n", self.namespace, "get", "pods", "-o", "json"))
        self.candidate.observe(self.identity, "snapshot-pods", _pod_observations(pods["items"]))
        workloads = json.loads(self.kubectl(
            "-n", self.namespace, "get", "deployments,statefulsets,daemonsets,jobs", "-o", "json",
        ))
        live_jobs = {
            row["metadata"]["uid"] for row in workloads["items"] if row["kind"] == "Job"
        }
        hooks = yaml.safe_load_all(self.command(
            "helm", "--kubeconfig", self.env["KUBECONFIG"], "--kube-context", self.context,
            "get", "hooks", self.release, "-n", self.namespace,
        ).stdout)
        hook_jobs = {
            hook["metadata"]["name"]: {
                policy.strip() for policy in hook["metadata"]["annotations"].get(
                    "helm.sh/hook-delete-policy", ""
                ).split(",")
            } for hook in hooks
            if isinstance(hook, dict) and hook.get("kind") == "Job"
            and hook.get("metadata", {}).get("namespace", self.namespace) == self.namespace
            and hook.get("metadata", {}).get("annotations", {}).get("helm.sh/hook")
        }
        for pod in pods["items"]:
            uid = pod["metadata"]["uid"]
            owners = pod["metadata"].get("ownerReferences", [])
            job = next((
                owner for owner in owners if owner.get("kind") == "Job"
                and owner.get("controller") is True and owner.get("name") in hook_jobs
            ), None)
            if (
                pod.get("status", {}).get("phase") == "Succeeded" and job is not None
                and uid not in self.completed_hook_pods
                and (
                    "hook-succeeded" in hook_jobs[job["name"]]
                    or (
                        "before-hook-creation" in hook_jobs[job["name"]]
                        and job["uid"] not in live_jobs
                    )
                )
            ):
                self.completed_hook_pods.add(uid)
                self.request.node.user_properties.append(("c4_completed_hook_pod", json.dumps({
                    "uid": uid, "name": pod["metadata"]["name"], "job": job["name"],
                    "job_uid": job["uid"], "phase": "Succeeded",
                    "deletion_policy": sorted(hook_jobs[job["name"]]),
                    "owner_present": job["uid"] in live_jobs,
                }, sort_keys=True)))
        namespace = json.loads(self.kubectl("get", "namespace", self.namespace, "-o", "json"))
        checkpoint = self.kubectl(
            "-n", self.namespace, "get", "configmap", f"{self.release}-upgrade-checkpoint",
            "--ignore-not-found", "-o", "json",
        )
        return {
            "history": _digest(self.history()), "values": _digest(self.values()),
            # Retain witnessed completed hook identities after their garbage collection.
            # Additions and disappearance of any other pod still change this snapshot.
            "pods": sorted(
                {row["metadata"]["uid"] for row in pods["items"]} | self.completed_hook_pods
            ),
            "serving_pods": sorted(
                row["metadata"]["uid"] for row in pods["items"]
                if not any(
                    owner.get("kind") == "Job"
                    for owner in row["metadata"].get("ownerReferences", [])
                )
            ),
            "serving_workloads": {
                f"{row['kind']}/{row['metadata']['name']}": {
                    "uid": row["metadata"]["uid"], "generation": row["metadata"]["generation"],
                    "spec": _digest(row["spec"]),
                } for row in workloads["items"] if row["kind"] != "Job"
            },
            "checkpoint": _digest(checkpoint),
            "ownership": {
                key: value for key, value in namespace["metadata"].get("annotations", {}).items()
                if "upgrade-holder" in key or "upgrade-action" in key
            },
            "fixture": self.stub.persistent_digest(), "app_secret": self.app_secret_digest(),
        }

    def factory_storage_digest(self) -> str:
        __tracebackhide__ = True
        return _digest(self.sql(
            "SELECT jsonb_build_object("
            "'work_items', (SELECT COALESCE(jsonb_agg(to_jsonb(w) ORDER BY w.id), "
            "'[]'::jsonb) FROM curie.work_items w), "
            "'execution_requests', (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id), "
            "'[]'::jsonb) FROM curie.execution_requests r))"
        ))

    def work_items(self) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT COALESCE(json_agg(json_build_object("
            "'id', w.id, 'issue', w.tracker_issue_id, 'scope', w.tracker_scope_id, "
            "'host', w.tracker_host, 'repository_id', w.repository_project_id, "
            "'repository', w.repository_path, 'agent_id', w.agent_id, "
            "'base', w.base_branch, 'conversation', w.conversation_id, "
            "'requests', (SELECT count(*) FROM curie.execution_requests r "
            "WHERE r.work_item_id=w.id))), '[]'::json) FROM curie.work_items w"
        )

    def poll_observations(self, after: float, minimum: int = 2) -> list[dict[str, list[str]]]:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            rows = self.stub.requests_after(after, f"/repos/{REPOSITORY}/issues")
            if len(rows) >= minimum:
                return rows
            time.sleep(1)
        return []

    def wait_items(self, numbers: set[str]) -> list[dict[str, Any]]:
        deadline = time.monotonic() + 90
        rows = self.work_items()
        while time.monotonic() < deadline and not numbers <= {row["issue"] for row in rows}:
            time.sleep(1)
            rows = self.work_items()
        return rows

    def runtime_images(self) -> dict[str, dict[str, Any]]:
        """Link each Docker build identity and running pod through verified content."""
        __tracebackhide__ = True
        pods = json.loads(self.kubectl("-n", self.namespace, "get", "pods", "-o", "json"))
        node = f"{self.cluster}-control-plane"
        node_info = json.loads(self.kubectl(
            "get", "node", node, "-o", "json"
        ))["status"]["nodeInfo"]
        image_rows = [
            line.split() for line in self.command(
                "docker", "exec", node, "ctr", "-n", "k8s.io", "images", "list"
            ).stdout.splitlines()
        ]

        def image_digest(value: str) -> str | None:
            match = re.search(r"sha256:[0-9a-f]{64}$", value)
            return match.group(0) if match else None

        def content(descriptor: dict[str, Any]) -> dict[str, Any]:
            __tracebackhide__ = True
            digest = descriptor["digest"]
            if not isinstance(digest, str) or image_digest(digest) != digest:
                raise ValueError("invalid content descriptor")
            if not isinstance(descriptor.get("mediaType"), str):
                raise ValueError("content descriptor has no media type")
            encoded = self.command(
                "docker", "exec", node, "bash", "-o", "pipefail", "-c",
                'ctr -n k8s.io content get "$1" | base64 -w 0', "c4-image-content", digest,
            ).stdout
            raw = base64.b64decode(encoded, validate=True)
            if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("content descriptor digest mismatch")
            if "size" in descriptor and len(raw) != descriptor["size"]:
                raise ValueError("content descriptor size mismatch")
            document = json.loads(raw)
            if not isinstance(document, dict):
                raise ValueError("content descriptor does not reference an object")
            if "mediaType" in document and document["mediaType"] != descriptor["mediaType"]:
                raise ValueError("content descriptor media type mismatch")
            return document

        observed: dict[str, dict[str, Any]] = {}
        for component, reference in self.candidate.images.items():
            try:
                references = {reference, "docker.io/library/" + reference}
                targets = {
                    (row[1], row[2])
                    for row in image_rows if len(row) >= 3 and row[0] in references
                }
                if len(targets) != 1:
                    raise ValueError("candidate reference has no unique containerd target")
                (target_type, target), = targets
                descriptor: dict[str, Any] = {"digest": target, "mediaType": target_type}
                identities: dict[str, str] = {}
                for _ in range(4):
                    document = content(descriptor)
                    manifest_id = descriptor["digest"]
                    if manifest_id in identities:
                        raise ValueError("cyclic image descriptor graph")
                    identities[manifest_id] = descriptor["mediaType"]
                    if "config" in document:
                        break
                    matches = [
                        entry for entry in document["manifests"]
                        if entry.get("platform", {}).get("architecture")
                        == node_info["architecture"]
                        and entry.get("platform", {}).get("os") == node_info["operatingSystem"]
                    ]
                    if len(matches) != 1:
                        raise ValueError("candidate index has no unique node platform")
                    descriptor = matches[0]
                else:
                    raise ValueError("image descriptor graph exceeds its depth bound")
                # Docker inspect IDs may name an index, manifest, or config. Bind the
                # observed build ID and optional descriptor to verified OCI content.
                # Observed with Docker 29.9.0: Id equals the OCI index descriptor digest;
                # inspecting with --platform selects the platform manifest digest.
                # https://github.com/opencontainers/image-spec/blob/v1.1.0/manifest.md
                config = content(document["config"])
                config_id = document["config"]["digest"]
                identities[config_id] = document["config"]["mediaType"]
                labels = config.get("config", {}).get("Labels") or {}
                cri = json.loads(self.command(
                    "docker", "exec", node, "crictl", "inspecti", reference,
                ).stdout)["status"]
                runtime_id = image_digest(cri["id"])
            except (ValueError, KeyError, TypeError, IndexError):
                self.candidate.failures.append("containerd image identity metadata is invalid")
                raise RuntimeError(self.candidate.failures[-1]) from None
            statuses: list[dict[str, Any]] = []
            for pod in pods["items"]:
                component_label = pod.get("metadata", {}).get("labels", {}).get(
                    "app.kubernetes.io/component"
                )
                if component != "runner" and component_label != component:
                    continue
                by_name = {
                    status["name"]: status
                    for status in pod.get("status", {}).get("containerStatuses", [])
                }
                for container in pod.get("spec", {}).get("containers", []):
                    if container.get("image") in references:
                        statuses.append(by_name.get(container["name"], {}))
            observed[component] = {
                "loaded_config_id": config_id,
                "built_id": self.candidate.image_ids[component],
                "built_descriptor": self.candidate.image_descriptors.get(component),
                "content_graph": identities,
                "built_identity_matches": (
                    self.candidate.image_ids[component] in identities
                    and (
                        component not in self.candidate.image_descriptors
                        or identities.get(self.candidate.image_descriptors[component]["digest"])
                        == self.candidate.image_descriptors[component]["mediaType"]
                    )
                ),
                "containerd_target_id": target,
                "manifest_id": manifest_id,
                "cri_id": runtime_id,
                "pod_image_ids": [status.get("imageID", "") for status in statuses],
                "provenance_matches": (
                    labels.get("curie.c4.source") == self.candidate.source_digest
                    and labels.get("curie.c4.run") == self.candidate.run_id
                    and config.get("architecture") == node_info["architecture"]
                    and config.get("os") == node_info["operatingSystem"]
                ),
                "running": any(
                    status.get("state", {}).get("running") is not None for status in statuses
                ),
                "identities_match": runtime_id in identities and all(
                    image_digest(status.get("imageID", "")) in identities
                    for status in statuses
                ),
            }
        self.request.node.user_properties.append((
            "c4_runtime_image_identities", json.dumps(observed, sort_keys=True)
        ))
        return observed

    def pause_keys(self) -> list[str]:
        __tracebackhide__ = True
        # valkey-cli reads REDISCLI_AUTH, including in scan mode:
        # https://github.com/valkey-io/valkey/blob/8.1.10/src/valkey-cli.c
        return self.kubectl(
            "-n", self.namespace, "exec", f"statefulset/{self.release}-valkey", "--",
            "sh", "-ec",
            'REDISCLI_AUTH="${VALKEY_PASSWORD:?}" exec valkey-cli '
            '--scan --pattern "*:upgrade:quiesce*"',
        ).splitlines()

    def ci_observation(
        self, work_item: str, paths: list[str], *, fresh_after: str | None = None,
    ) -> dict[str, Any]:
        return json.loads(self.kubectl(
            "-n", self.namespace, "exec", f"deployment/{self.release}-api", "--",
            "python3", "-c", CI_PROBE, json.dumps({
                "work_item": work_item, "head_sha": self.stub._ref("main"),
                "paths": paths, "fresh_after": fresh_after,
            }),
        ))

    def checkpoint(self) -> dict[str, Any]:
        configmap = json.loads(self.kubectl(
            "-n", self.namespace, "get", "configmap", f"{self.release}-upgrade-checkpoint",
            "-o", "json",
        ))
        return json.loads(configmap["data"]["record"])

    def cleanup(self) -> None:
        __tracebackhide__ = True
        errors: list[str] = []
        if self.forward is not None:
            self.forward.terminate()
            try:
                self.forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.forward.kill()
                self.forward.wait(timeout=10)
            if self.forward.stdout is not None:
                self.forward.stdout.close()
        if self.cluster_attempted:
            try:
                if Path(self.env["KUBECONFIG"]).exists():
                    self.kubectl(
                        "delete", "namespace", self.namespace, "--ignore-not-found", "--wait=false",
                        timeout=60,
                    )
            except (RuntimeError, OSError, subprocess.SubprocessError):
                errors.append("owned namespace teardown failed")
            try:
                self.command("kind", "delete", "cluster", "--name", self.cluster, timeout=240)
            except (RuntimeError, OSError, subprocess.SubprocessError):
                errors.append("owned cluster teardown failed")
            try:
                clusters = self.command("kind", "get", "clusters", timeout=30).stdout.splitlines()
                if self.cluster in clusters:
                    errors.append("owned kind cluster remains")
            except (RuntimeError, OSError, subprocess.SubprocessError):
                errors.append("owned cluster absence could not be verified")
            try:
                containers = self.command(
                    "docker", "ps", "-aq", "--filter",
                    f"label=io.x-k8s.kind.cluster={self.cluster}", timeout=30,
                ).stdout.strip()
                if containers:
                    errors.append("owned kind node containers remain")
            except (RuntimeError, OSError, subprocess.SubprocessError):
                errors.append("owned node absence could not be verified")
        for service in (self.facade, self.stub):
            if service is not None:
                try:
                    service.close()
                except (RuntimeError, OSError, sqlite3.Error):
                    errors.append("external fixture teardown or unsupported route audit failed")
        if self.stub is not None and self.stub.port:
            with socket.socket() as connection:
                connection.settimeout(1)
                if connection.connect_ex((self.stub.host, self.stub.port)) == 0:
                    errors.append("owned HTTPS fixture still listens")
        if errors:
            raise RuntimeError("; ".join(errors))


@pytest.fixture
def released_factory(candidate_curie: _Candidate, request: pytest.FixtureRequest) -> _Installation:
    installation = _Installation(candidate_curie, request)
    request.addfinalizer(installation.cleanup)
    installation.setup()
    return installation


@pytest.fixture
def released_factory_without_required_ci(
    candidate_curie: _Candidate, request: pytest.FixtureRequest,
) -> _Installation:
    installation = _Installation(candidate_curie, request, required_ci=False)
    request.addfinalizer(installation.cleanup)
    installation.setup()
    return installation


def test_v0125_factory_without_required_ci_upgrade_dry_run(
    released_factory_without_required_ci: _Installation,
) -> None:
    run = released_factory_without_required_ci
    released_api = run.values(computed=True)["api"]
    assert released_api["githubFactoryPythonCi"] == {}
    assert released_api["githubFactoryMetadataCi"] == {
        "Acme-Corp/Acme-Bot": {
            "checks": [METADATA_CHECK], "statuses": [METADATA_STATUS],
        }
    }
    history = run.history()
    assert history[-1]["chart"] == "curie-0.12.5"
    assert history[-1]["status"] == "deployed"
    before = run.snapshot()
    storage = run.factory_storage_digest()
    status, agent = run.api("GET", f"/agents/{run.agent_id}")
    assert status == 200
    agent_digest = _digest(agent)
    assert run.pause_keys() == []

    dry_run = run.upgrade("--dry-run")
    after = run.snapshot()
    values_absent = run.upgrade_values_absent(dry_run, METADATA_CHECK, METADATA_STATUS)
    assert dry_run.returncode == 0, (
        "factory without required CI must pass candidate schema validation"
    )
    plan = dry_run.document()
    assert plan["dry_run"] is True
    assert "error" not in plan
    assert not any(line.startswith("refusal at validate:") for line in plan["plan"])
    plan_names_present = all(value in json.dumps(plan) for value in ("factory", "githubFactory"))
    assert plan_names_present
    assert values_absent, "dry-run must report setting names without fixture values"
    assert after == before
    assert run.history() == history
    assert int(run.history()[-1]["revision"]) == run.baseline_revision
    assert run.factory_storage_digest() == storage
    assert run.app_secret_digest() == run.released_secret_digest
    assert run.pause_keys() == []
    status, agent = run.api("GET", f"/agents/{run.agent_id}")
    assert status == 200
    assert _digest(agent) == agent_digest
    assert run.api("GET", "/health")[0] == 200
    assert not run.stub.unknown_requests


def test_v0125_factory_upgrade_uses_supported_flow(released_factory: _Installation) -> None:
    run = released_factory
    before = run.snapshot()
    dry_run = run.upgrade("--dry-run")
    after_dry_run = run.snapshot()
    result = run.upgrade()
    if result.returncode:
        guard = result.legacy_guard()
        if guard is None:
            run.candidate.failures.append("upgrade failed before the required legacy-key guard")
            raise RuntimeError(run.candidate.failures[-1])
        assert int(run.history()[-1]["revision"]) == run.baseline_revision
        run.request.node.user_properties.append(("c4_reversed_guard", guard))
        run.request.node.user_properties.append(("c4_revision_unchanged", "true"))
        assert result.returncode == 0, f"supported cluster upgrade refused: {guard}"
    assert result.returncode == 0
    outcome = result.document()
    assert outcome["target_version"] == run.candidate.target
    assert outcome["known_good_version"] == run.candidate.target
    assert outcome["canary"]["passed"] is True
    assert outcome["convergence"]["exact"] is True
    assert all(outcome["convergence"].values())
    checkpoint = run.checkpoint()
    assert checkpoint["known_good_version"] == run.candidate.target
    assert checkpoint["canary"]["passed"] is True
    assert checkpoint["convergence"]["exact"] is True
    assert dry_run.returncode == 0
    assert before == after_dry_run
    plan = dry_run.document()
    plan_names_present = all(value in json.dumps(plan) for value in ("factory", "githubFactory"))
    plan_values_absent = all(
        value not in json.dumps(plan) for value in (LABEL, MENTION, REQUIRED_CHECK, WEBHOOK_SECRET)
    )
    assert plan_names_present
    assert plan_values_absent
    run._ready()
    run._forward_api()
    assert run.api("GET", "/health")[0] == 200
    current = run.values()
    computed = run.values(computed=True)
    assert all(current.get("api", {}).get(key) is None for key in LEGACY_KEYS)
    assert all(computed.get("api", {}).get(key) is None for key in LEGACY_KEYS)
    factory = computed["factory"]
    assert factory["enabled"] is True
    assert factory["intake"] == "poll"
    assert factory["ciWaitSeconds"] == 137
    assert factory["cardBaseUrl"] == "https://cards.example.com"
    tracker, = [row for row in factory["trackers"] if row["kind"] == "github"]
    code_host, = [row for row in factory["codeHosts"] if row["kind"] == "github"]
    assert (tracker["label"], tracker["mention"], tracker["pollIntervalSeconds"]) == (
        LABEL, MENTION, 3
    )
    assert tracker["apiUrl"] == code_host["apiUrl"] == run.stub.base_url + "/api/v3"
    assert tracker["host"] == code_host["host"] == urlsplit(run.stub.base_url).netloc
    for section, connection in (("codeHosts", code_host), ("trackers", tracker)):
        egress_retained = (
            connection.get("egress") == run.baseline["agentSandbox"]["connectorEgress"][AGENT]
        )
        assert egress_retained, f"factory.{section}.egress must equal the retained fixture route"
    assert factory["sandboxEgress"] == {AGENT: True}
    credential = {
        "type": "github_app", "appId": "51",
        "existingSecret": run.baseline["api"]["githubAppExistingSecret"],
        "existingSecretKey": run.baseline["api"]["githubAppExistingSecretKey"],
    }
    assert tracker["credential"] == code_host["credential"] == credential
    assert run.app_secret_digest() == run.released_secret_digest
    binding, = [row for row in factory["bindings"] if row["tracker"] == tracker["name"]]
    assert binding["trackerScopeId"] == str(REPOSITORY_ID)
    repository, = binding["repos"]
    assert repository["projectId"] == str(REPOSITORY_ID)
    assert repository["path"].casefold() == REPOSITORY.casefold()
    assert repository["codeHost"] == code_host["name"]
    assert repository["bases"] == ["main", "maint", "release"]
    assert repository["defaultBase"] == "maint"
    assert repository["ci"]["required"] == {
        "key": REQUIRED_CHECK, "paths": ["src", "tests"], "pendingKeyPrefix": SHARD_PREFIX,
    }
    assert set(repository["ci"]["metadataRerunKeys"]) == {
        METADATA_CHECK, f"status:{METADATA_STATUS}"
    }
    for key in ("githubCloneBase", "githubApiUrl", "githubRepoAllowlist", "orgName"):
        assert current["api"][key] == run.baseline["api"][key]
    for key in ("maxConcurrency", "runnerTotalTimeoutSeconds", "deliveryBudgetSeconds"):
        assert current["worker"][key] == run.baseline["worker"][key]
    assert current["resourceQuota"] == run.baseline["resourceQuota"]
    retained_sandbox = run.baseline["agentSandbox"]
    assert current["agentSandbox"]["runner"]["model"] == retained_sandbox["runner"]["model"]
    assert current["agentSandbox"]["connectorEgress"] == retained_sandbox["connectorEgress"]
    assert run.api("GET", f"/agents/{run.agent_id}")[1]["model"] == "example-retained-agent-model"
    runtime_images = run.runtime_images()
    for component, image in runtime_images.items():
        assert image["built_identity_matches"] is True
        assert image["provenance_matches"] is True
        assert image["identities_match"] is True
        if component != "runner":
            assert image["running"] is True
    assert run.pause_keys() == []
    stamp = time.time()
    run.stub.issue(4701, [])
    run.stub.issue(4702, ["wrong-upgrade-label"])
    run.stub.issue(4703, [LABEL], authorized=False)
    run.stub.issue(4704, [LABEL, "base:release"])
    run.stub.issue(4705, [LABEL, "base:unapproved"])
    run.stub.issue(4701, [LABEL])
    rows = run.wait_items({"4701", "4704"})
    polls = run.poll_observations(stamp)
    assert len(polls) >= 2
    assert any(query.get("labels") == [LABEL] for query in polls)
    by_number = {row["issue"]: row for row in rows}
    assert {"4701", "4704"} <= by_number.keys()
    assert not {"4702", "4703", "4705"} & by_number.keys()
    assert by_number["4701"]["base"] == "maint"
    assert by_number["4704"]["base"] == "release"
    for number in ("4701", "4704"):
        item = by_number[number]
        assert item["scope"] == item["repository_id"] == str(REPOSITORY_ID)
        assert item["repository"] == REPOSITORY
        assert item["agent_id"] == run.agent_id
        assert item["host"] == urlsplit(run.stub.base_url).netloc
        status, view = run.api("GET", f"/work-items/{item['id']}")
        assert status == 200
        assert view["id"] == item["id"]
    count = by_number["4701"]["requests"]
    wrong_stamp = time.time()
    run.stub.comment(4701, "@unrelated-account please revise")
    assert run.poll_observations(wrong_stamp)
    assert next(row for row in run.work_items() if row["issue"] == "4701")["requests"] == count
    run.stub.comment(4701, f"@{MENTION} please revise")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        item = next(row for row in run.work_items() if row["issue"] == "4701")
        if item["requests"] > count:
            break
        time.sleep(1)
    assert item["requests"] == count + 1
    head = run.stub._ref("main")
    now = datetime.now(UTC)

    def check(identifier: int, name: str, *, fresh: bool = True) -> dict[str, Any]:
        stamp = now - timedelta(seconds=10 if fresh else 120)
        return {
            "id": identifier, "name": name, "head_sha": head, "status": "completed",
            "conclusion": "success", "app": {"id": 15368, "slug": "github-actions"},
            "started_at": stamp.isoformat(), "completed_at": stamp.isoformat(),
        }

    run.stub.ci([check(1, REQUIRED_CHECK)], [])
    accepted = run.ci_observation(by_number["4701"]["id"], ["src/example.py"])
    assert accepted["observation_error"] is None
    assert accepted["ci_wait_seconds"] == 137
    assert accepted["verdict"]["kind"] == "green"
    unselected = run.ci_observation(by_number["4701"]["id"], ["unselected/example.py"])
    assert unselected["verdict"]["kind"] == "unverified"
    assert unselected["verdict"]["reason"].startswith("required_python_ci_unselected:")
    run.stub.ci([check(2, SHARD_PREFIX + "1")], [])
    waiting = run.ci_observation(by_number["4701"]["id"], ["src/example.py"])
    assert waiting["verdict"]["kind"] == "pending"
    assert waiting["verdict"]["reason"] == "required_python_ci_missing"
    fresh_after = (now - timedelta(seconds=30)).isoformat()
    for fresh in (False, True):
        stamp = (now - timedelta(seconds=10 if fresh else 120)).isoformat()
        run.stub.ci([check(3, METADATA_CHECK, fresh=fresh)], [{
            "id": 4, "context": METADATA_STATUS, "state": "success",
            "created_at": stamp, "updated_at": stamp,
        }])
        metadata = run.ci_observation(by_number["4701"]["id"], [], fresh_after=fresh_after)
        assert metadata["observation_error"] is None
        assert metadata["verdict"]["kind"] == ("green" if fresh else "pending")
        if not fresh:
            assert metadata["verdict"]["reason"] == "checks_awaiting_metadata_rerun"
    assert not run.stub.unknown_requests


def test_factory_upgrade_refuses_invalid_overlays_and_keeps_typed_values_stable(
    released_factory: _Installation,
) -> None:
    run = released_factory
    malformed_value = "example-must-not-echo-check"
    conflicting_value = "https://must-not-echo.example.com"
    malformed = _document(run.work / "malformed-policy.yaml", {
        "api": {"githubFactoryPythonCi": {
            "Acme-Corp/Acme-Bot": {
                "check": [malformed_value], "paths": ["src", "tests"],
                "pendingCheckPrefix": SHARD_PREFIX,
            },
        }},
    })
    conflicting = _document(run.work / "conflicting-factory.yaml", {
        "factory": {"cardBaseUrl": conflicting_value},
    })
    equal = _document(run.work / "equal-factory.yaml", {
        "api": {"githubFactoryPythonCi": run.baseline["api"]["githubFactoryPythonCi"]},
        "factory": {
            "cardBaseUrl": run.baseline["api"]["githubFactoryCardBaseUrl"],
            "ciWaitSeconds": run.baseline["api"]["githubFactoryCiWaitSeconds"],
        },
    })
    unrelated_agent = "acme-unrelated"
    unrelated_cidr = next(
        cidr for cidr in ("198.51.100.77/32", "203.0.113.77/32")
        if ipaddress.ip_address(run.stub.host) not in ipaddress.ip_network(cidr)
    )
    unrelated_connectors = {
        AGENT: [],
        unrelated_agent: [{
            "cidr": unrelated_cidr,
            "ports": [{"protocol": "TCP", "port": run.stub.port}],
        }],
    }
    unrelated = _document(run.work / "unrelated-connector.yaml", {
        "agentSandbox": {"connectorEgress": unrelated_connectors},
    })
    explicit = _document(run.work / "explicit-forge-egress.yaml", {
        "agentSandbox": {"connectorEgress": unrelated_connectors},
        "factory": {
            **{section: [{
                "name": "github", "kind": "github",
                "host": urlsplit(run.stub.base_url).netloc,
                "apiUrl": run.stub.base_url + "/api/v3",
                "egress": run.baseline["agentSandbox"]["connectorEgress"][AGENT],
            }] for section in ("codeHosts", "trackers")},
            "sandboxEgress": {AGENT: True},
        },
    })

    def serving_state() -> dict[str, Any]:
        __tracebackhide__ = True
        observed = run.snapshot()
        observed.pop("checkpoint")
        raw_checkpoint = run.kubectl(
            "-n", run.namespace, "get", "configmap", f"{run.release}-upgrade-checkpoint",
            "--ignore-not-found", "-o", "json",
        )
        checkpoint = json.loads(raw_checkpoint) if raw_checkpoint.strip() else {}
        observed["checkpoint_data"] = _digest(checkpoint.get("data") or {})
        observed["checkpoint_ownership"] = {
            key: value
            for key, value in checkpoint.get("metadata", {}).get("annotations", {}).items()
            if key in {"curietech.ai/upgrade-holder", "curietech.ai/upgrade-action"}
        }
        status, agent = run.api("GET", f"/agents/{run.agent_id}")
        if status != 200:
            run.candidate.failures.append("retained agent observation unavailable")
            raise RuntimeError(run.candidate.failures[-1])
        observed["agent"] = _digest(agent)
        observed["factory_storage"] = run.factory_storage_digest()
        observed["pause_keys"] = run.pause_keys()
        return observed

    protected_values = (malformed_value, conflicting_value, "Acme-Corp/Acme-Bot", unrelated_cidr)
    initial = serving_state()
    old_history = run.history()
    assert old_history[-1]["chart"] == "curie-0.12.5"
    assert old_history[-1]["status"] == "deployed"
    assert initial["pause_keys"] == []
    cases = (
        (malformed, ("api.githubFactoryPythonCi",), equal),
        (conflicting, ("factory.cardBaseUrl",), equal),
        (unrelated, (
            "api.githubApiUrl", "factory.codeHosts.egress", "factory.trackers.egress",
        ), explicit),
    )
    for overlay, settings, neighboring_overlay in cases:
        before = serving_state()
        dry_snapshot = run.snapshot()
        dry_run = run.upgrade("--dry-run", "-f", str(overlay))
        dry_code = dry_run.returncode
        dry_names_present = all(dry_run.contains(setting) for setting in settings)
        dry_values_absent = run.upgrade_values_absent(dry_run, *protected_values)
        dry_legacy_guard_absent = not dry_run.contains("is no longer read")
        assert dry_code != 0
        assert dry_names_present, "dry-run must identify the invalid configuration setting"
        assert dry_values_absent, "dry-run must report setting names without fixture values"
        assert dry_legacy_guard_absent, "invalid input must refuse before the raw legacy guard"
        assert run.snapshot() == dry_snapshot
        assert serving_state() == before
        assert run.api("GET", "/health")[0] == 200

        refused = run.upgrade("-f", str(overlay))
        refused_code = refused.returncode
        refusal_names_present = all(refused.contains(setting) for setting in settings)
        refusal_values_absent = run.upgrade_values_absent(refused, *protected_values)
        refusal_legacy_guard_absent = not refused.contains("is no longer read")
        assert refused_code != 0
        assert refusal_names_present, "upgrade must identify the invalid configuration setting"
        assert refusal_values_absent, "upgrade must report setting names without fixture values"
        assert refusal_legacy_guard_absent, "invalid input must refuse before the raw legacy guard"
        assert int(run.history()[-1]["revision"]) == run.baseline_revision
        assert run.history() == old_history
        assert serving_state() == before == initial
        assert run.api("GET", "/health")[0] == 200

        neighbor_before = run.snapshot()
        neighbor = run.upgrade("--dry-run", "-f", str(neighboring_overlay))
        neighbor_code = neighbor.returncode
        assert neighbor_code == 0, "the valid neighboring configuration must be accepted"
        neighbor_plan = neighbor.document()
        assert neighbor_plan["dry_run"] is True
        neighbor_values_absent = run.upgrade_values_absent(neighbor, *protected_values)
        assert neighbor_values_absent, "accepted dry-run must also report names without values"
        assert run.snapshot() == neighbor_before
        assert serving_state() == initial

    upgraded = run.upgrade("-f", str(equal))
    upgraded_code = upgraded.returncode
    assert upgraded_code == 0, "the valid neighboring configuration must complete its upgrade"
    outcome = upgraded.document()
    assert outcome["target_version"] == run.candidate.target
    assert outcome["known_good_version"] == run.candidate.target
    assert outcome["canary"]["passed"] is True
    assert outcome["convergence"]["exact"] is True
    run._ready()
    run._forward_api()
    assert run.api("GET", "/health")[0] == 200
    migrated = run.values()
    assert all(migrated.get("api", {}).get(key) is None for key in LEGACY_KEYS)
    assert migrated["factory"]["cardBaseUrl"] == (
        run.baseline["api"]["githubFactoryCardBaseUrl"]
    )
    assert migrated["factory"]["ciWaitSeconds"] == (
        run.baseline["api"]["githubFactoryCiWaitSeconds"]
    )
    typed_factory = migrated["factory"]
    typed = _document(run.work / "typed-factory.yaml", {"factory": typed_factory})
    typed_snapshot = run.snapshot()
    typed_revision = int(run.history()[-1]["revision"])
    for _ in range(2):
        typed_dry_run = run.upgrade("--dry-run", "-f", str(typed))
        typed_dry_code = typed_dry_run.returncode
        assert typed_dry_code == 0
        typed_plan = typed_dry_run.document()
        assert typed_plan["dry_run"] is True
        legacy_migration_absent = "api.githubFactory" not in json.dumps(typed_plan["plan"])
        assert legacy_migration_absent, "already typed input must not repeat the legacy migration"
        assert run.snapshot() == typed_snapshot
        assert run.values()["factory"] == typed_factory

    repeated = run.upgrade("-f", str(typed))
    repeated_code = repeated.returncode
    assert repeated_code == 0
    repeated_outcome = repeated.document()
    assert repeated_outcome["unchanged"] is True
    assert repeated_outcome["known_good_version"] == run.candidate.target
    assert repeated_outcome["canary"]["passed"] is True
    assert repeated_outcome["convergence"]["exact"] is True
    assert int(run.history()[-1]["revision"]) == typed_revision
    assert _digest(run.values()) == _digest(migrated)
    assert run.values()["factory"] == typed_factory
    assert run.app_secret_digest() == run.released_secret_digest
    assert run.pause_keys() == []
    assert run.api("GET", "/health")[0] == 200
    assert not run.stub.unknown_requests

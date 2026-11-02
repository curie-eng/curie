"""Owned candidate infrastructure for #4619; no internal service doubles."""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import os
import selectors
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

import factory_e2e as fe
import httpx
import scripted_kind as sk
from aci_protocol import RUNS_STREAM_DEFAULT, STREAM_PAYLOAD_FIELD, QueuedTurn
from c4_admitted_http import gitlab, jira, marker_model_type, tracker_stub_type
from c4_shared_install import CaseIsolationError, InstallSnapshot, leftover_state
from curie_telemetry.redact import redact_text

ROOT = Path(__file__).resolve().parents[3]
REPOSITORY = "acme-corp/acme-bot"
LOCATIONS = {"github": REPOSITORY, "gitlab": REPOSITORY, "jira": "acme-jira"}
AGENT_LETTERS = ("a", "b")
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
# CI builds the candidate CLI and images once per workflow run; the fixture
# then tags those images instead of rebuilding them from the checkout.
CANDIDATE_CLI_ENV = "CURIE_C4_CANDIDATE_CLI"
CANDIDATE_IMAGES_ENV = "CURIE_C4_CANDIDATE_IMAGES"
# ScriptedRun.execute labels this scenario issue `factory`; it is not a case ticket.
SCENARIO_ISSUE = 3815
# GitHub intake polls every 5 s, GitLab and Jira every 1 s. Retired labels
# must be observed before the WorkItems that dedupe them are truncated.
RETIRE_OBSERVE_SECONDS = 12
VALUE_FROM_KINDS = frozenset({"secretKeyRef", "configMapKeyRef", "fieldRef", "resourceFieldRef"})


@dataclass(frozen=True)
class Agent:
    id: str
    name: str
    label: str
    marker: str
    deployment_id: str
    bundle_ref: str


@dataclass(frozen=True)
class Ticket:
    kind: str
    number: int
    url: str = field(repr=False)
    agent: Agent


_SUMMARY_REQUEST_FIELDS = (
    "status", "sequence", "started_at", "execution_attempts", "start_deferrals",
    "last_deferral_reason", "terminal_cause", "runtime_epoch", "execution_deadline",
    "runtime_heartbeat_expires_at", "runtime_claim_name", "runtime_sandbox_name",
)
_SUMMARY_PUBLICATION_FIELDS = (
    "status", "revision_number", "approval_id", "error", "approval_card_delivery_error",
    "result_delivery_error", "created_at", "terminal_at",
)


def publication_failure_summary(
    request_id: uuid.UUID,
    snapshot: Any,
    publications: Any,
    worker_logs: dict[str, str],
    sandboxes: list[dict[str, Any]],
    model_calls: list[dict[str, Any]],
    provider_calls: list[dict[str, Any]],
) -> dict[str, Any]:
    """A bounded digest of why a request did not publish. Pure."""

    request = snapshot.get("request", {}) if isinstance(snapshot, dict) else {}
    conversation = str(request.get("conversation_id") or "")
    keys = {str(request_id), conversation} - {""}
    logs: dict[str, list[str]] = {}
    for pod, text in worker_logs.items():
        lines = []
        for line in text.splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            message = str(record.get("message", ""))
            severity = str(record.get("severity", ""))
            if severity in {"WARNING", "ERROR", "CRITICAL"} or any(k in message for k in keys):
                lines.append(
                    f"{record.get('timestamp', '')} {severity} {record.get('logger', '')}: "
                    + message[:400]
                )
        logs[pod] = lines[-60:]
    return {
        "request": {key: request.get(key) for key in _SUMMARY_REQUEST_FIELDS},
        "publications": [
            {key: row.get(key) for key in _SUMMARY_PUBLICATION_FIELDS}
            | {"has_head_sha": bool(row.get("head_sha"))}
            for row in (publications if isinstance(publications, list) else [])
        ],
        "sandboxes": [
            {
                "name": pod["metadata"]["name"],
                "phase": pod.get("status", {}).get("phase"),
                "deleting": "deletionTimestamp" in pod["metadata"],
                "created": pod["metadata"].get("creationTimestamp"),
                "containers": [
                    (c["name"], c.get("ready"), c.get("restartCount"))
                    for c in pod.get("status", {}).get("containerStatuses", [])
                ],
            }
            for pod in sandboxes
        ],
        "model_calls": [
            {key: call.get(key) for key in ("id", "round", "matched_marker")}
            for call in model_calls[-20:]
        ],
        "provider_calls": [
            {key: call.get(key) for key in ("id", "method", "path", "http_status")}
            for call in provider_calls
            if call.get("method") != "GET" or (call.get("http_status") or 0) >= 400
        ][-40:],
        "worker_logs": logs,
    }


class AdmittedKind(sk.ScriptedRun):
    def __init__(self, root: Path, binary: Path) -> None:
        super().__init__(fe.parse_args(["scripted"]))
        # ScriptedRun defaults its context. This fixture retains the caller's
        # configured engine exactly, including an absent DOCKER_CONTEXT.
        if "DOCKER_CONTEXT" in os.environ:
            self.env["DOCKER_CONTEXT"] = os.environ["DOCKER_CONTEXT"]
        else:
            self.env.pop("DOCKER_CONTEXT", None)
        self.root = root
        self.curie = str(binary)
        self.images = {
            name: f"curie-c4-{name}:{self.run_id}" for name in ("api", "worker", "ui", "runner")
        }
        self.tag_map = dict(zip(sk.IMAGES, self.images.values(), strict=True))
        self.agents: list[Agent] = []
        self.image_ids: dict[str, str] = {}
        self.retained_images: dict[str, str] = {}
        self.worker_token = ""
        self.api_url = ""
        self.started = False
        self.fixture_ready = False
        self.cleaning_up = False
        self.provider_credentials_staged = False
        self.diagnostic_path = root / "diagnostics.sqlite3"
        self.infrastructure_errors: list[str] = []
        self.source_candidate: AdmittedKind | None = None
        self.bundles: dict[str, Path] = {}
        # Recorded once by the session fixture when the shared install is
        # ready, the worker is at zero replicas and no agent exists.
        self.baseline: InstallSnapshot | None = None
        self.secrets.extend((gitlab.TOKEN, jira.TOKEN))

    def record_failure(self, operation: str, exc: BaseException) -> None:
        phase = "cleanup" if self.cleaning_up else "runtime" if self.fixture_ready else "setup"
        detail, _ = fe.redact_agent_text(str(exc), self.secrets)
        descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(descriptor)
        self.diagnostic_path.chmod(0o600)
        with sqlite3.connect(self.diagnostic_path) as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS failures ("
                "id INTEGER PRIMARY KEY, phase TEXT NOT NULL, operation TEXT NOT NULL, "
                "exception_type TEXT NOT NULL, detail TEXT NOT NULL, recorded_at REAL NOT NULL)"
            )
            database.execute(
                "INSERT INTO failures (phase, operation, exception_type, detail, recorded_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (phase, operation, type(exc).__name__, detail or "", time.time()),
            )
        if phase == "runtime":
            self.infrastructure_errors.append(operation)

    def provision_provider_credentials(self) -> None:
        if self.provider_credentials_staged:
            return
        if self.workdir is None:
            raise RuntimeError("provider credentials require the owned workdir")
        directory = self.workdir / "provider-credentials"
        directory.mkdir(mode=0o700)
        arguments = []
        for key, token in (("gitlab-token", gitlab.TOKEN), ("jira-token", jira.TOKEN)):
            path = directory / key
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as stream:
                stream.write(token)
            arguments.append(f"--from-file={key}={path}")
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "generic",
            "c4-provider-credentials",
            *arguments,
        )
        self.provider_credentials_staged = True

    def redact_model_evidence(self, value: str) -> str:
        return redact_text(fe.redact_agent_text(value, self.secrets)[0] or "")

    def record_marker_failure(self, ticket: Ticket, round_: int) -> None:
        try:
            request = self.request(ticket)
            observations = self.model.observations()
            calls = self.model.call_evidence()
            descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(descriptor)
            self.diagnostic_path.chmod(0o600)
            evidence = {
                "tracker_kind": ticket.kind,
                "tracker_issue_id": ticket.number,
                "tracker_url": ticket.url,
                "expected_marker": ticket.agent.marker,
                "request_id": request["id"] if request else None,
                "objective": request["objective"] if request else None,
                "observations": observations,
            }
            with sqlite3.connect(self.diagnostic_path) as db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS marker_failures ("
                    "id INTEGER PRIMARY KEY, round INTEGER NOT NULL, "
                    "evidence TEXT NOT NULL, recorded_at REAL NOT NULL)"
                )
                db.execute(
                    "CREATE TABLE IF NOT EXISTS marker_model_calls ("
                    "failure_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, "
                    "evidence TEXT NOT NULL, PRIMARY KEY(failure_id, ordinal))"
                )
                failure_id = db.execute(
                    "INSERT INTO marker_failures (round, evidence, recorded_at) VALUES (?, ?, ?)",
                    (round_, self.redact_model_evidence(json.dumps(evidence)), time.time()),
                ).lastrowid
                db.executemany(
                    "INSERT INTO marker_model_calls (failure_id, ordinal, evidence) "
                    "VALUES (?, ?, ?)",
                    [
                        (failure_id, call["id"], self.redact_model_evidence(json.dumps(call)))
                        for call in calls
                    ],
                )
        except (OSError, ValueError, sqlite3.Error) as exc:
            self.record_failure("marker diagnostic persistence", exc)
            raise RuntimeError(
                "marker diagnostics failed; private failure details retained"
            ) from None

    def record_publication_failure(self, request: dict[str, Any], count: int) -> None:
        try:
            request_id = uuid.UUID(request["id"])
            snapshot = self.sql(
                "SELECT json_build_object('request', row_to_json(r), 'work_item', row_to_json(w)) "
                "FROM curie.execution_requests r JOIN curie.work_items w ON w.id=r.work_item_id "
                f"WHERE r.id='{request_id}'"
            )
            publications = self.sql(
                "SELECT coalesce(json_agg(row_to_json(p)), '[]'::json) "
                "FROM curie.publications p "
                f"WHERE p.execution_request_id='{request_id}'"
            )
            with self.stub.provider_lock:
                provider_calls = self.stub.provider_evidence()
                important_calls = [
                    call for call in provider_calls
                    if call["method"] != "GET" or call["http_status"] >= 400
                ]
                selected = {
                    call["id"]: call
                    for call in [
                        *provider_calls[:50], *provider_calls[-150:], *important_calls[-100:]
                    ]
                }
                provider_calls = [selected[key] for key in sorted(selected)]
            pods = json.loads(self.kubectl(
                "-n", self.namespace, "get", "pods", "-o", "json",
                "-l", "app.kubernetes.io/component in (worker,c4-held-worker)",
            ))["items"]
            worker_logs = {
                pod["metadata"]["name"]: self.kubectl(
                    "-n", self.namespace, "logs", pod["metadata"]["name"],
                    "--all-containers", "--tail=2000",
                )[-32768:]
                for pod in pods[:4]
            }
            evidence = {
                "snapshot": snapshot,
                "publications": publications,
                "observations": self.model.observations()[-100:],
                "model_calls": self.model.call_evidence()[-100:],
                "provider_calls": provider_calls,
                "github_pulls": self.stub._rows("pull")[-50:],
                "api_logs": self.kubectl(
                    "-n", self.namespace, "logs", "deployment/curie-api",
                    "--all-containers", "--tail=2000",
                )[-32768:],
                "worker_logs": worker_logs,
            }

            def scrub(value: Any) -> Any:
                if isinstance(value, str):
                    return self.redact_model_evidence(value)[-32768:]
                if isinstance(value, (list, tuple)):
                    return [scrub(item) for item in value]
                if isinstance(value, dict):
                    return {key: scrub(item) for key, item in value.items()}
                return value

            descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(descriptor)
            self.diagnostic_path.chmod(0o600)
            with sqlite3.connect(self.diagnostic_path) as db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS publication_failures ("
                    "id INTEGER PRIMARY KEY, request_id TEXT NOT NULL, "
                    "expected_count INTEGER NOT NULL, evidence TEXT NOT NULL, "
                    "recorded_at REAL NOT NULL)"
                )
                db.execute(
                    "INSERT INTO publication_failures "
                    "(request_id, expected_count, evidence, recorded_at) VALUES (?, ?, ?, ?)",
                    (str(request_id), count, json.dumps(scrub(evidence)), time.time()),
                )
            sandboxes = json.loads(self.kubectl(
                "-n", self.namespace, "get", "pods", "-o", "json",
                "-l", "!app.kubernetes.io/component",
            ))["items"]
            summary = publication_failure_summary(
                request_id, snapshot, publications, worker_logs, sandboxes,
                self.model.call_evidence(), provider_calls,
            )
            # A disposable CI runner discards the private database, so the
            # failure also reports this bounded, scrubbed digest of fixture state.
            digest = json.dumps(scrub(summary), sort_keys=True)
            print("publication failure summary: " + digest, file=sys.stderr)
            # CI uploads this directory as an artifact when the job fails.
            artifacts = ROOT / "target/factory-e2e"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / f"admitted-publication-failure-{request_id}-{count}.json").write_text(
                digest + "\n"
            )
        except (KeyError, OSError, ValueError, TypeError, sqlite3.Error) as exc:
            self.record_failure("publication diagnostic persistence", exc)
            raise RuntimeError(
                "publication diagnostics failed; private failure details retained"
            ) from None

    def read_trace_observations(self, trace_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not trace_ids:
            return {}
        code = """
import asyncio
import json
import sys

import httpx
from curie_api.config import get_settings
from curie_api.langfuse import LangfuseClient
from curie_telemetry.redact import redact_text

async def read():
    settings = get_settings()

    def scrub(value):
        if isinstance(value, str):
            for secret in (settings.langfuse_public_key, settings.langfuse_secret_key):
                if secret:
                    value = value.replace(secret, "[REDACTED]")
            return redact_text(value)
        if isinstance(value, list):
            return [scrub(item) for item in value]
        if isinstance(value, dict):
            return {key: scrub(item) for key, item in value.items()}
        return value

    async with httpx.AsyncClient(timeout=30) as client:
        langfuse = LangfuseClient(settings, client)
        result = {}
        for trace_id in json.loads(sys.argv[1]):
            result[trace_id] = await langfuse.get_observations(trace_id)
        print(json.dumps(scrub(result)))

try:
    asyncio.run(read())
except (httpx.HTTPError, OSError, ValueError, TypeError, KeyError) as exc:
    reason = (
        f"HTTP {exc.response.status_code}"
        if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
    )
    print("persisted observations read failed: " + reason, file=sys.stderr)
    sys.exit(2)
"""
        try:
            observations: dict[str, list[dict[str, Any]]] = json.loads(self.kubectl(
                "-n", self.namespace, "exec", "deployment/curie-api", "--",
                "python", "-c", code, json.dumps(trace_ids),
            ))
            if not isinstance(observations, dict) or set(observations) != set(trace_ids):
                raise ValueError("persisted observations response has mismatched trace ids")
            if any(
                not isinstance(items, list) or any(not isinstance(item, dict) for item in items)
                for items in observations.values()
            ):
                raise ValueError(
                    "persisted observations response is not a list of observation rows"
                )
            return observations
        except (OSError, ValueError, TypeError) as exc:
            self.record_failure("persisted observations read", exc)
            raise RuntimeError(
                "persisted observations read failed; private diagnostics retained"
            ) from None

    def record_runs_failure(
        self,
        row: dict[str, Any],
        session: str,
        traces: list[dict[str, Any]],
        details: dict[str, Any],
    ) -> None:
        try:
            observations = self.read_trace_observations(list(details))
            evidence = {
                "request": row,
                "session": session,
                "listed_traces": traces,
                "trace_details": details,
                "upstream_observations": observations,
            }

            def scrub(value: Any) -> Any:
                if isinstance(value, str):
                    return self.redact_model_evidence(value)
                if isinstance(value, list):
                    return [scrub(item) for item in value]
                if isinstance(value, dict):
                    return {key: scrub(item) for key, item in value.items()}
                return value

            descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(descriptor)
            self.diagnostic_path.chmod(0o600)
            with sqlite3.connect(self.diagnostic_path) as db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS runs_failures ("
                    "id INTEGER PRIMARY KEY, request_id TEXT NOT NULL, "
                    "evidence TEXT NOT NULL, recorded_at REAL NOT NULL)"
                )
                db.execute(
                    "INSERT INTO runs_failures (request_id, evidence, recorded_at) "
                    "VALUES (?, ?, ?)",
                    (row["id"], json.dumps(scrub(evidence)), time.time()),
                )
        except (OSError, ValueError, sqlite3.Error) as exc:
            self.record_failure("Runs diagnostic persistence", exc)
            raise RuntimeError(
                "Runs diagnostics failed; private failure details retained"
            ) from None

    def record_admission_failure(self, ticket: Ticket) -> None:
        try:
            with self.stub.provider_lock:
                calls = self.stub.provider_evidence()
                clock = self.stub.gitlab.now.isoformat()
            logs = self.kubectl(
                "-n",
                self.namespace,
                "logs",
                "deployment/curie-api",
                "--all-containers",
                "--tail=2000",
            )
            evidence = {
                "tracker_kind": ticket.kind,
                "issue_id": ticket.number,
                "tracker_url": ticket.url,
                "expected_agent": ticket.agent.id,
                "trigger_label": ticket.agent.label,
                "provider_clock": clock,
                "wall_clock": time.time(),
                "provider_calls": calls,
                "api_logs": logs,
            }
            descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(descriptor)
            self.diagnostic_path.chmod(0o600)
            with sqlite3.connect(self.diagnostic_path) as db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS admission_failures ("
                    "id INTEGER PRIMARY KEY, tracker_kind TEXT NOT NULL, "
                    "issue_id INTEGER NOT NULL, "
                    "evidence TEXT NOT NULL, recorded_at REAL NOT NULL)"
                )
                db.execute(
                    "INSERT INTO admission_failures "
                    "(tracker_kind, issue_id, evidence, recorded_at) VALUES (?, ?, ?, ?)",
                    (
                        ticket.kind,
                        ticket.number,
                        self.redact_model_evidence(json.dumps(evidence)),
                        time.time(),
                    ),
                )
        except (OSError, ValueError, sqlite3.Error) as exc:
            self.record_failure("admission diagnostic persistence", exc)
            raise RuntimeError(
                "admission diagnostics failed; private failure details retained"
            ) from None

    def command(self, *argv: str, timeout: float = 900) -> str:
        # Only the harness's fixed infrastructure image names are translated.
        # Product selection, HTTP requests and worker configuration stay real.
        if Path(argv[0]).name == "helm" and "install" in argv[1:]:
            self.provision_provider_credentials()
        try:
            return super().command(*(self.tag_map.get(a, a) for a in argv), timeout=timeout)
        except (sk.ScriptedScenarioError, OSError, subprocess.SubprocessError) as exc:
            if sk.registry_bind_rejected(str(exc)):
                raise sk.ScriptedScenarioError("registry port is already allocated") from None
            self.record_failure(f"{Path(argv[0]).name} {argv[1]}", exc)
            raise RuntimeError(
                f"candidate infrastructure command {Path(argv[0]).name} {argv[1]} failed; "
                "redacted details retained in the private fixture diagnostics database"
            ) from None

    def prerequisites(self) -> None:
        for tool in ("cargo", "uv", "docker", "kind", "helm", "kubectl", "openssl", "git"):
            if shutil.which(tool) is None:
                raise RuntimeError(f"missing required executable: {tool}")
        self.command("docker", "info", timeout=30)
        for tag in sk.IMAGES:
            output = self.command(
                "docker",
                "images",
                "--no-trunc",
                "--format",
                "{{.ID}}",
                "--filter",
                f"reference={tag}",
            ).strip()
            self.retained_images[tag] = output
        self.command("kind", "version", timeout=30)
        self.command("helm", "version", timeout=30)
        if self.source_candidate is not None:
            for name, tag in self.images.items():
                current = json.loads(self.command("docker", "image", "inspect", tag))[0]["Id"]
                if current != self.source_candidate.image_ids[name]:
                    raise RuntimeError("a session-owned source candidate image changed")
            return
        prebuilt_cli = os.environ.get(CANDIDATE_CLI_ENV)
        prebuilt_images = os.environ.get(CANDIDATE_IMAGES_ENV)
        if prebuilt_cli and prebuilt_images:
            if not os.access(prebuilt_cli, os.X_OK):
                raise RuntimeError(f"{CANDIDATE_CLI_ENV} does not name an executable")
            self.curie = prebuilt_cli
            for name, tag in self.images.items():
                self.command("docker", "tag", prebuilt_images.format(component=name), tag)
        else:
            self.build_source_candidate()
        for name, tag in self.images.items():
            self.image_ids[name] = json.loads(self.command("docker", "image", "inspect", tag))[0][
                "Id"
            ]

    def build_source_candidate(self) -> None:
        self.command(
            "cargo",
            "build",
            "--manifest-path",
            str(ROOT / "cli/Cargo.toml"),
            "--bin",
            "curie",
            "--locked",
            "-j",
            "2",
            timeout=1800,
        )
        for name in ("api", "worker", "ui"):
            self.command(
                "docker",
                "build",
                "-f",
                f"apps/{name}/Dockerfile",
                "-t",
                self.images[name],
                ".",
                timeout=1800,
            )
        self.command(self.curie, "build", "--tag", self.images["runner"], timeout=1800)

    def values(self, original: Any, **kwargs: Any) -> dict[str, Any]:
        values = original(**kwargs)
        host = urlsplit(self.stub.base_url).netloc
        factory = values["factory"]
        for component in ("api", "worker", "ui"):
            repository, tag = self.images[component].rsplit(":", 1)
            values[component]["image"].update(repository=repository, tag=tag, pullPolicy="Never")
        credential = {
            "type": "static_token",
            "existingSecret": "c4-provider-credentials",
            "existingSecretKey": "gitlab-token",
            "neverExpires": True,
        }
        factory["codeHosts"].append(
            {
                "name": "gitlab",
                "kind": "gitlab",
                "host": host,
                "apiUrl": self.stub.base_url + "/gitlab/api/v4",
                "credential": credential,
            }
        )
        factory["trackers"].extend(
            [
                {
                    "name": "gitlab",
                    "kind": "gitlab",
                    "host": host,
                    "apiUrl": self.stub.base_url + "/gitlab/api/v4",
                    "credential": credential,
                    "mention": gitlab.BOT.login,
                    "label": "factory",
                    "pollIntervalSeconds": 1,
                },
                {
                    "name": "jira",
                    "kind": "jira_cloud",
                    "host": host,
                    "apiUrl": self.stub.base_url,
                    "credential": {
                        "type": "static_token",
                        "username": jira.BOT_EMAIL,
                        "existingSecret": "c4-provider-credentials",
                        "existingSecretKey": "jira-token",
                    },
                    "mention": jira.BOT_ACCOUNT_ID,
                    "label": "factory",
                    "pollIntervalSeconds": 1,
                },
            ]
        )
        repo_id = str(self.stub._repository_row()["id"])
        factory["bindings"] = [
            {
                "name": "acme-gitlab",
                "tracker": "gitlab",
                "trackerScopeId": str(self.stub.gitlab.project_id),
                "repos": [
                    {
                        "alias": "bot",
                        "codeHost": "gitlab",
                        "projectId": str(self.stub.gitlab.project_id),
                        "path": REPOSITORY,
                    }
                ],
            },
            {
                "name": "acme-jira",
                "tracker": "jira",
                "trackerScopeId": self.stub.jira.cloud_id,
                "repos": [
                    {"alias": "bot", "codeHost": "github", "projectId": repo_id, "path": REPOSITORY}
                ],
                "defaultRepo": "bot",
                "startAuthority": {
                    "type": "allowlist",
                    "accounts": [self.stub.jira.reporter.account_id],
                },
            },
        ]
        factory["sandboxEgress"] = {f"acme-{letter}": True for letter in ("a", "b")}
        # Two factory agents, each with a sandbox per tracker thread, can be
        # live at once. The factory profile requests 8Gi memory per sandbox,
        # and the default resourceQuota.hard.requestsMemory (8Gi) admits one,
        # beside Langfuse and ClickHouse on one CI kind node. This test proves
        # admitted-agent identity, not sandbox sizing; the factory envelope has
        # its own runtime proof (#4889).
        values.setdefault("agentSandbox", {})["agentResourceProfiles"] = {
            f"acme-{letter}": "standard" for letter in ("a", "b")
        }
        # Keep a run queryable through the product's Runs API. These are the
        # real chart services, using its normal headless bootstrap and OTLP.
        values["langfuse"] = {"deploy": True}
        values["clickhouse"] = {"deploy": True}
        values["otelCollector"] = {"deploy": True, "telemetryDisabled": False}
        return values

    def deploy(self, registry_ref: str) -> None:
        if self.workdir is None:
            raise RuntimeError("kind workdir absent")
        secret = json.loads(
            self.kubectl("-n", self.namespace, "get", "secret", "curie-secrets", "-o", "json")
        )
        self.env["CURIE_API_KEY"] = base64.b64decode(secret["data"]["apiKey"]).decode()
        self.worker_token = base64.b64decode(secret["data"]["internalWorkerToken"]).decode()
        self.secrets.extend([self.env["CURIE_API_KEY"], self.worker_token])
        self.forward_api()
        self.build_agent_bundles(registry_ref)

    def forward_api(self) -> None:
        try:
            self._forward_api()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            self.record_failure("candidate API port forward", exc)
            raise RuntimeError("API port forward failed; private diagnostics retained") from None

    def _forward_api(self) -> None:
        if self.forward is not None:
            self.forward.terminate()
            self.forward.wait(timeout=30)
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
        with selectors.DefaultSelector() as selector:
            if self.forward.stdout is None:
                raise RuntimeError("API port forward has no stdout")
            selector.register(self.forward.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=30):
                raise RuntimeError("API port forward did not become ready")
            line = self.forward.stdout.readline()
        if "127.0.0.1:" not in line:
            raise RuntimeError("API port forward failed")
        port = int(line.split("127.0.0.1:", 1)[1].split()[0])
        self.api_url = f"http://127.0.0.1:{port}"
        self.env["CURIE_API_URL"] = self.api_url

    def build_agent_bundles(self, registry_ref: str) -> None:
        """Build the acme-a and acme-b bundles once for the session install."""

        if self.workdir is None:
            raise RuntimeError("kind workdir absent")
        for letter in AGENT_LETTERS:
            name = f"acme-{letter}"
            bundle = self.workdir / name
            (bundle / ".claude-plugin").mkdir(parents=True)
            manifest = {
                "name": name,
                "version": "0.1.0",
                "description": "Exercise admitted-agent identity.",
                "systemPrompt": "Use the supplied runtime marker and publish the candidate change.",
            }
            (bundle / ".claude-plugin/plugin.json").write_text(json.dumps(manifest))
            (bundle / "c4-marker").write_text(self.agent_marker(letter) + "\n")
            (bundle / "runner.Dockerfile").write_text(
                "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\n"
            )
            self.command(
                self.curie,
                "build",
                "--plugin-dir",
                str(bundle),
                "--registry",
                registry_ref,
                "--runner-image",
                registry_ref + "/curie-runner:latest",
                "--platform",
                "linux/amd64",
            )
            self.bundles[letter] = bundle

    def agent_marker(self, letter: str) -> str:
        return f"c4-{letter}-{self.run_id}"

    def deploy_case_agents(self) -> None:
        """Create fresh acme-a and acme-b agents and deploy the session bundles.

        The chart values key sandbox egress and resource profiles by these two
        agent names, so every case reuses the names with new agent ids.
        """

        self.agents = []
        for letter in AGENT_LETTERS:
            name = f"acme-{letter}"
            label = f"acme-{letter}-work"
            bundle = self.bundles.get(letter)
            if bundle is None:
                raise RuntimeError(f"the session bundle for {name} was not built")
            agent = self.api(
                "POST",
                "/agents",
                {
                    "name": name,
                    "repo_full_name": REPOSITORY,
                    "channel": self.binding("github", label),
                },
                expected=201,
            )
            for kind in ("gitlab", "jira"):
                self.api(
                    "POST",
                    f"/agents/{agent['id']}/channels",
                    self.binding(kind, label),
                    expected=201,
                )
            self.command(
                self.curie,
                "cluster",
                "--context",
                self.context,
                "deploy",
                "--plugin-dir",
                str(bundle),
                "--agent",
                name,
                "--env",
                "prod",
                "--chart",
                str(ROOT / "charts/curie"),
                "--namespace",
                self.namespace,
                "--release",
                "curie",
                "--api-url",
                self.api_url,
            )
            self.command(
                self.curie,
                "cluster",
                "--context",
                self.context,
                "publication-policy",
                name,
                "--policy",
                "auto",
                "--namespace",
                self.namespace,
                "--release",
                "curie",
                "--api-url",
                self.api_url,
            )
            selected = self.sql(
                "SELECT json_build_object('deployment_id', d.id, 'bundle_ref', v.bundle_ref) "
                "FROM curie.deployments d JOIN curie.agent_versions v ON v.id=d.version_id "
                f"WHERE d.agent_id='{uuid.UUID(agent['id'])}' AND d.status='active' "
                "ORDER BY d.deployed_at DESC LIMIT 1"
            )
            self.agents.append(
                Agent(
                    agent["id"],
                    name,
                    label,
                    self.agent_marker(letter),
                    selected["deployment_id"],
                    selected["bundle_ref"],
                )
            )
        self.started = True

    def worker_container_env(self) -> list[dict[str, Any]]:
        deployment = json.loads(
            self.kubectl("-n", self.namespace, "get", "deployment/curie-worker", "-o", "json")
        )
        container = next(
            c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "worker"
        )
        env: list[dict[str, Any]] = container.get("env", [])
        return env

    @staticmethod
    def env_pair(entry: dict[str, Any]) -> tuple[str, str]:
        if "valueFrom" in entry:
            return entry["name"], json.dumps(entry["valueFrom"], sort_keys=True)
        return entry["name"], str(entry.get("value", ""))

    def api_forges(self) -> Any:
        deployment = json.loads(
            self.kubectl("-n", self.namespace, "get", "deployment/curie-api", "-o", "json")
        )
        container = next(
            c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "api"
        )
        return json.loads(next(e["value"] for e in container["env"] if e["name"] == "CURIE_FORGES"))

    def resource_names(self, namespace: str, resource: str, *selector: str) -> frozenset[str]:
        items = json.loads(self.kubectl("-n", namespace, "get", resource, *selector, "-o", "json"))[
            "items"
        ]
        return frozenset(item["metadata"]["name"] for item in items)

    def stream_keys(self) -> frozenset[str]:
        keys: set[str] = set()
        cursor = "0"
        while True:
            cursor, batch = self.valkey("SCAN", cursor, "TYPE", "stream", "COUNT", "1000")
            keys.update(str(key) for key in batch)
            cursor = str(cursor)
            if cursor == "0":
                return frozenset(keys)

    def install_snapshot(self) -> InstallSnapshot:
        """Read every piece of shared install state a case can change."""

        try:
            rows = self.sql(
                "SELECT json_build_object("
                "'agents', (SELECT count(*) FROM curie.agents), "
                "'work_items', (SELECT count(*) FROM curie.work_items), "
                "'execution_requests', (SELECT count(*) FROM curie.execution_requests), "
                "'tenants', (SELECT coalesce(json_agg(id::text), '[]'::json) "
                "FROM curie.tenants))"
            )
            return InstallSnapshot(
                agents=int(rows["agents"]),
                work_items=int(rows["work_items"]),
                execution_requests=int(rows["execution_requests"]),
                tenants=frozenset(rows["tenants"]),
                sandbox_claims=self.resource_names(self.namespace, CLAIM_RESOURCE),
                publication_jobs=self.resource_names(self.publication_namespace(), "jobs"),
                api_forges=self.api_forges(),
                worker_env=tuple(sorted(self.env_pair(e) for e in self.worker_container_env())),
                worker_pods=len(
                    self.resource_names(
                        self.namespace, "pods", "-l", "app.kubernetes.io/component=worker"
                    )
                ),
                deployments=self.resource_names(self.namespace, "deployments"),
                network_policies=self.resource_names(self.namespace, "networkpolicies"),
                runs_stream_length=int(self.valkey("XLEN", RUNS_STREAM_DEFAULT)),
                streams=self.stream_keys(),
            )
        except (KeyError, StopIteration, TypeError, ValueError) as exc:
            self.record_failure("shared install snapshot", exc)
            raise RuntimeError(
                "shared install snapshot failed; private diagnostics retained"
            ) from None

    def require_baseline(self) -> InstallSnapshot:
        if self.baseline is None:
            raise RuntimeError("the shared install has no recorded baseline")
        return self.baseline

    def restore_worker_env(self, baseline: InstallSnapshot) -> None:
        wanted = dict(baseline.worker_env)
        current = {entry["name"]: entry for entry in self.worker_container_env()}
        changes: list[str] = []
        unrestorable: list[str] = []
        for name, entry in current.items():
            if name not in wanted:
                changes.append(name + "-")
                continue
            if self.env_pair(entry)[1] == wanted[name]:
                continue
            if "valueFrom" in entry or self.is_value_from(wanted[name]):
                unrestorable.append(name)
            else:
                changes.append(f"{name}={wanted[name]}")
        for name, value in wanted.items():
            if name in current:
                continue
            if self.is_value_from(value):
                unrestorable.append(name)
            else:
                changes.append(f"{name}={value}")
        if unrestorable:
            raise CaseIsolationError(
                "worker_env: valueFrom entries changed and are not restored: "
                + ", ".join(sorted(unrestorable))
            )
        if changes:
            self.kubectl(
                "-n",
                self.namespace,
                "set",
                "env",
                "deployment/curie-worker",
                "-c",
                "worker",
                *changes,
            )

    @staticmethod
    def is_value_from(value: str) -> bool:
        # A snapshot stores a valueFrom entry as its sorted JSON object.
        try:
            parsed = json.loads(value)
        except ValueError:
            return False
        return isinstance(parsed, dict) and len(parsed) == 1 and set(parsed) <= VALUE_FROM_KINDS

    def retire_case_tickets(self) -> None:
        """Remove every label from every stub ticket except the scenario issue.

        Intake dedupes a labelled ticket against its WorkItem, which the reset
        truncates. A ticket an earlier case left labelled could otherwise be
        admitted again for the next case's same-named agents.
        """

        with self.stub.provider_lock:
            for row in self.stub._rows("issue"):
                number = row["number"]
                if number == SCENARIO_ISSUE or not row.get("labels"):
                    continue
                removed = [label["name"] for label in row["labels"]]
                row["labels"] = []
                if "updated_at" in row:
                    # Same shape as tools/github-stub/github_stub.py::_utc.
                    row["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
                self.stub._put("issue", number, row)
                for name in removed:
                    self.stub._label_event(number, name, "unlabeled", row["user"])
            self.stub.sync_gitlab_clock()
            for iid, issue in list(self.stub.gitlab.issues.items()):
                for name in list(issue.labels):
                    self.stub.gitlab.label(iid, name, gitlab.ROOT, add=False)
            for jira_issue in list(self.stub.jira.issues.values()):
                for name in list(jira_issue.labels):
                    self.stub.jira.remove_label(jira_issue.id, name, self.stub.jira.reporter)

    def reset_case(self) -> None:
        """Return the shared install to its baseline, then verify it is clean."""

        baseline = self.require_baseline()
        self.worker_replicas(0)
        self.retire_case_tickets()
        retired_at = time.monotonic()
        self.restore_worker_env(baseline)
        if self.api_forges() != baseline.api_forges:

            def restore(configuration: dict[str, Any]) -> None:
                configuration.clear()
                configuration.update(copy.deepcopy(baseline.api_forges))

            self.change_forges(restore)
        self.kubectl(
            "-n",
            self.namespace,
            "delete",
            CLAIM_RESOURCE,
            "--all",
            "--wait=true",
            "--timeout=180s",
            timeout=240,
        )
        deadline = time.monotonic() + 180
        while self.resource_names(self.namespace, CLAIM_RESOURCE):
            if time.monotonic() >= deadline:
                raise CaseIsolationError("sandbox_claims: claims remain after deletion")
            time.sleep(2)
        self.kubectl(
            "-n",
            self.publication_namespace(),
            "delete",
            "jobs",
            "--all",
            "--cascade=foreground",
            "--wait=true",
            "--timeout=180s",
            timeout=240,
        )
        # Every table that references curie.agents (channels, versions,
        # deployments and their dependants) or curie.work_items is truncated
        # with them, which releases the case-created tenants those rows named.
        # The cascade also empties tables whose nullable foreign keys reference
        # that set, such as credential_redemption_audit_entries and
        # github_review_deliveries; no case asserts on them.
        remaining = RETIRE_OBSERVE_SECONDS - (time.monotonic() - retired_at)
        if remaining > 0:
            time.sleep(remaining)
        tenants = ", ".join(f"'{uuid.UUID(tenant)}'" for tenant in sorted(baseline.tenants))
        self.write_sql(
            "TRUNCATE curie.work_items, curie.agents CASCADE; "
            "DELETE FROM curie.tenants" + (f" WHERE id NOT IN ({tenants})" if tenants else "")
        )
        self.valkey("XTRIM", RUNS_STREAM_DEFAULT, "MAXLEN", "0")
        for stream in sorted(self.stream_keys() - baseline.streams):
            self.valkey("DEL", stream)
        time.sleep(3)
        leftovers = leftover_state(baseline, self.install_snapshot())
        if leftovers:
            raise CaseIsolationError(
                "the shared install kept case state after reset: " + "; ".join(leftovers)
            )

    def binding(self, kind: str, label: str | None) -> dict[str, Any]:
        result = {"kind": kind, "address": LOCATIONS[kind], "trigger_label": label}
        return result

    def api(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        expected: int = 200,
        internal: bool = False,
    ) -> Any:
        status, answer = self.api_result(method, path, body, internal=internal)
        if status != expected:
            failure = RuntimeError(
                f"candidate API {method} {path.split('?')[0]} "
                f"returned {status}, expected {expected}"
            )
            self.record_failure(f"candidate API {method}", failure)
            raise failure
        return answer

    def api_result(
        self, method: str, path: str, body: Any = None, *, internal: bool = False
    ) -> tuple[int, Any]:
        headers = (
            {"X-Curie-Worker-Token": self.worker_token}
            if internal
            else {"X-API-Key": self.env["CURIE_API_KEY"]}
        )
        try:
            return fe.http_json(method, self.api_url + path, headers=headers, body=body)
        except (OSError, ValueError, httpx.HTTPError) as exc:
            self.record_failure(f"candidate API {method} transport", exc)
            raise RuntimeError(
                "candidate API transport failed; private diagnostics retained"
            ) from None

    def change_forges(self, change: Callable[[dict[str, Any]], None]) -> None:
        deployment = json.loads(self.kubectl(
            "-n", self.namespace, "get", "deployment/curie-api", "-o", "json"
        ))
        try:
            container = next(
                c for c in deployment["spec"]["template"]["spec"]["containers"]
                if c["name"] == "api"
            )
            configuration = json.loads(next(
                e["value"] for e in container["env"] if e["name"] == "CURIE_FORGES"
            ))
            change(configuration)
        except (KeyError, StopIteration, ValueError) as exc:
            self.record_failure("read candidate factory configuration", exc)
            raise RuntimeError(
                "factory configuration missing; private diagnostics retained"
            ) from None
        selector = ",".join(
            f"{key}={value}"
            for key, value in sorted(deployment["spec"]["selector"]["matchLabels"].items())
        )
        previous = json.loads(self.kubectl(
            "-n", self.namespace, "get", "pods", "-l", selector, "-o", "json"
        ))["items"]
        self.kubectl(
            "-n", self.namespace, "set", "env", "deployment/curie-api",
            "CURIE_FORGES=" + json.dumps(configuration),
        )
        self.kubectl(
            "-n", self.namespace, "rollout", "status", "deployment/curie-api", "--timeout=300s"
        )
        # Rollout status returns once the new pod is ready, while a replaced
        # pod can still answer a worker's open connection with the previous
        # configuration. The change is in effect only when those pods are gone.
        if previous:
            self.kubectl(
                "-n", self.namespace, "wait", "--for=delete", "--timeout=180s",
                *(f"pod/{pod['metadata']['name']}" for pod in previous),
            )
        self.forward_api()

    def worker_identity(self) -> list[tuple[str, str, int]]:
        pods = json.loads(self.kubectl(
            "-n", self.namespace, "get", "pods", "-l",
            "app.kubernetes.io/component=worker", "-o", "json",
        ))
        return sorted(
            (pod["metadata"]["uid"], c["name"], c["restartCount"])
            for pod in pods["items"]
            for c in pod["status"].get("containerStatuses", [])
        )

    def wait_for_final(self) -> None:
        # ScriptedRun.execute owns provisioning; the pytest body owns every
        # behavioral assertion and drives the complete multi-tracker scenario.
        return

    def observe(self) -> None:
        return

    def seed(self, kind: str, agent: Agent, *, label: str | None = None) -> Ticket:
        label = agent.label if label is None else label
        with self.stub.provider_lock:
            if kind == "github":
                number = max(row["number"] for row in self.stub._rows("issue")) + 1
                row = self.stub._issue(
                    number, "Exercise admitted agent", "Record the deployed marker.", [label]
                )
                self.stub._put("issue", number, row)
                self.stub._label_event(number, label, "labeled", row["user"])
                url = row["html_url"]
            elif kind == "gitlab":
                self.stub.sync_gitlab_clock()
                number = self.stub.gitlab.add_issue("Exercise admitted agent", author=gitlab.ROOT)
                self.stub.gitlab.label(number, label, gitlab.ROOT)
                url = f"{self.stub.base_url}/gitlab/{REPOSITORY}/-/issues/{number}"
            else:
                row = self.stub.jira.seed_issue(
                    "Exercise admitted agent", "Record the deployed marker."
                )
                self.stub.jira.add_label(row.id, label, self.stub.jira.reporter)
                number = row.id
                url = f"{self.stub.base_url}/browse/{row.key}"
        return Ticket(kind, number, url, agent)

    def request(self, ticket: Ticket) -> dict[str, Any] | None:
        kind = "jira_cloud" if ticket.kind == "jira" else ticket.kind
        return self.sql(
            "SELECT coalesce((SELECT row_to_json(observation) FROM ("
            "SELECT r.*, w.agent_id, w.conversation_id, w.trigger_label, "
            "w.repository_path AS admitted_repository_path "
            "FROM curie.execution_requests r JOIN curie.work_items w ON w.id=r.work_item_id "
            f"WHERE w.tracker_kind='{kind}' AND w.tracker_issue_id='{ticket.number}' "
            "ORDER BY r.sequence DESC LIMIT 1) observation), 'null'::json)"
        )

    def await_admission(self, ticket: Ticket) -> dict[str, Any]:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            request = self.request(ticket)
            if request is not None:
                return request
            time.sleep(1)
        self.record_admission_failure(ticket)
        raise RuntimeError(
            f"{ticket.kind} intake did not admit the labelled fixture ticket; "
            "private diagnostics retained"
        )

    def worker_logs(self) -> str:
        return self.kubectl(
            "-n",
            self.namespace,
            "logs",
            "deployment/curie-worker",
            "--all-containers",
            "--tail=2000",
        )

    def comments(self, ticket: Ticket) -> list[str]:
        with self.stub.provider_lock:
            if ticket.kind == "github":
                return [
                    row["body"]
                    for row in self.stub._rows("comment")
                    if row["issue_number"] == ticket.number
                ]
            if ticket.kind == "gitlab":
                return [note.body for note in self.stub.gitlab.issue_notes(ticket.number)]
            return [
                jira.flatten(comment.body) for comment in self.stub.jira.own_comments(ticket.number)
            ]

    def publication_rows(self, request: dict[str, Any]) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT coalesce(json_agg(row_to_json(observation)), '[]'::json) FROM ("
            "SELECT p.id, d.agent_id, p.deployment_id, p.status, p.execution_request_id, "
            "l.head_sha, l.pr_number, l.branch FROM curie.publications p "
            "JOIN curie.thread_publication_lineages l ON l.id=p.lineage_id "
            "JOIN curie.deployments d ON d.id=p.deployment_id "
            f"WHERE p.execution_request_id='{uuid.UUID(request['id'])}' "
            "ORDER BY p.created_at) observation"
        )

    def mark_ci(self, request: dict[str, Any], *, green: bool) -> None:
        with self.stub.provider_lock:
            for row in self.publication_rows(request):
                sha = row["head_sha"]
                if green:
                    self.stub.green_heads.add(sha)
                    self.stub.red_heads.discard(sha)
                else:
                    self.stub.red_heads.add(sha)
                    self.stub.green_heads.discard(sha)
                if request["reply_kind"] == "gitlab":
                    self.stub._sync_gitlab()
                    self.stub.gitlab.report_job(
                        sha, "admitted-agent", "success" if green else "failed"
                    )

    def write_sql(self, query: str) -> None:
        self.kubectl(
            "-n", self.namespace, "exec", "statefulset/curie-postgres", "--",
            "psql", "-U", "postgres", "-d", "postgres", "-At",
            "-v", "ON_ERROR_STOP=1", "-c", "BEGIN; " + query + "; COMMIT;",
        )

    def check_violation(self, query: str) -> Any:
        output = self.kubectl(
            "-n", self.namespace, "exec", "statefulset/curie-postgres", "--",
            "psql", "-U", "postgres", "-d", "postgres", "-qAt",
            "-v", "ON_ERROR_STOP=1", "-c",
            "BEGIN; CREATE TEMP TABLE c4_refusal (code text, reason text) ON COMMIT DROP; "
            "DO $c4$ BEGIN " + query + "; "
            "EXCEPTION WHEN check_violation THEN "
            "INSERT INTO c4_refusal VALUES (SQLSTATE, SQLERRM); END; $c4$; "
            "SELECT coalesce((SELECT row_to_json(r) FROM c4_refusal r), 'null'::json); ROLLBACK;",
        )
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            self.record_failure("database refusal observation", exc)
            raise RuntimeError(
                "database refusal observation returned invalid JSON; private diagnostics retained"
            ) from None

    def valkey(self, *args: str) -> Any:
        # Valkey 8.1.10 CLI help documents REDISCLI_AUTH and -e:
        # https://github.com/valkey-io/valkey/blob/8.1.10/src/valkey-cli.c
        raw = self.kubectl(
            "-n", self.namespace, "exec", "statefulset/curie-valkey", "-c", "valkey", "--",
            "sh", "-ec",
            'export REDISCLI_AUTH="${VALKEY_PASSWORD:?}"; exec valkey-cli -e --json "$@"',
            "sh", *args,
        )
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            self.record_failure("Valkey JSON response", exc)
            raise RuntimeError(
                "Valkey command returned invalid JSON; private diagnostics retained"
            ) from None

    def worker_replicas(self, count: int) -> None:
        self.kubectl(
            "-n", self.namespace, "scale", "deployment/curie-worker", f"--replicas={count}"
        )
        if count:
            self.kubectl(
                "-n", self.namespace, "rollout", "status", "deployment/curie-worker",
                "--timeout=300s",
            )
        elif self.resource_names(
            self.namespace, "pods", "-l", "app.kubernetes.io/component=worker"
        ):
            # A paused case starts at zero replicas, so there may be no pod to await.
            self.kubectl(
                "-n", self.namespace, "wait", "--for=delete", "pod",
                "-l", "app.kubernetes.io/component=worker", "--timeout=180s",
            )

    def take_execute_wake(self, request: dict[str, Any]) -> dict[str, Any]:
        event = f"work-item-{request['id']}-execute-{request['dispatch_generation']}"
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            for stream_id, fields in self.valkey("XRANGE", RUNS_STREAM_DEFAULT, "-", "+"):
                payload = json.loads(dict(zip(fields[::2], fields[1::2], strict=True))[
                    STREAM_PAYLOAD_FIELD
                ])
                if payload["event_id"] == event:
                    self.valkey("XDEL", RUNS_STREAM_DEFAULT, stream_id)
                    # Do not race the producer's post-XADD publication fence.
                    while time.monotonic() < deadline:
                        published = self.sql(
                            "SELECT coalesce(to_json(published_generation), 'null'::json) "
                            "FROM curie.execution_requests "
                            f"WHERE id='{uuid.UUID(request['id'])}'"
                        )
                        if published == request["dispatch_generation"]:
                            return payload
                        time.sleep(0.2)
                    break
            time.sleep(0.5)
        raise RuntimeError("the real reconciler did not publish and fence the execute wake")

    def enqueue(self, payload: dict[str, Any]) -> None:
        turn = QueuedTurn.model_validate(payload)
        self.valkey("XADD", RUNS_STREAM_DEFAULT, "*", STREAM_PAYLOAD_FIELD, turn.model_dump_json())

    def label_webhook(self, ticket: Ticket, label: str) -> dict[str, Any]:
        with self.stub.provider_lock:
            issue = next(row for row in self.stub._rows("issue") if row["number"] == ticket.number)
            payload = {
                "action": "labeled", "issue": issue, "label": {"name": label},
                "repository": self.stub._repository_row(),
                "sender": issue["user"], "installation": {"id": 5501},
            }
        body = json.dumps(payload).encode()
        signature = hmac.new(b"example-scripted-webhook-secret", body, hashlib.sha256).hexdigest()
        response = httpx.post(
            self.api_url + "/github/webhook", content=body,
            headers={
                "Content-Type": "application/json", "X-GitHub-Event": "issues",
                "X-GitHub-Delivery": str(uuid.uuid4()),
                "X-Hub-Signature-256": "sha256=" + signature,
            }, timeout=60,
        )
        if response.status_code != 200:
            self.record_failure(
                "signed label webhook", RuntimeError(f"HTTP {response.status_code}")
            )
            raise RuntimeError("signed label webhook failed")
        return response.json()

    def cleanup(self) -> list[str]:
        self.cleaning_up = True
        errors: list[str] = []
        unrelated_before: dict[str, str] | None = None
        snapshot_started_at = time.time()
        try:
            running = self.command("docker", "ps", "--no-trunc", "--format", "{{.ID}}\t{{.Labels}}")
            unrelated_before = {}
            owned_labels = {
                f"curie.factory-scripted={self.run_id}",
                f"io.x-k8s.kind.cluster={self.cluster}",
            }
            for line in running.splitlines():
                container_id, _, labels = line.partition("\t")
                if not owned_labels.intersection(labels.split(",")):
                    unrelated_before[container_id] = labels
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
        errors.extend(super().cleanup())
        try:
            remaining = set(self.command("docker", "ps", "-q", "--no-trunc").split())
            if unrelated_before is not None:
                missing = unrelated_before.keys() - remaining
                if missing:
                    self.record_failure(
                        "missing unrelated containers during owned cleanup",
                        RuntimeError(json.dumps({
                            "snapshot_started_at": snapshot_started_at,
                            "snapshot_finished_at": time.time(),
                            "expected_owned_labels": sorted(owned_labels),
                            "missing": [
                                {"id": container_id, "labels": unrelated_before[container_id]}
                                for container_id in sorted(missing)
                            ],
                            "remaining_ids": sorted(remaining),
                        }, sort_keys=True)),
                    )
                    errors.append("unrelated running containers disappeared during owned cleanup")
            for tag, previous in self.retained_images.items():
                current = self.command(
                    "docker",
                    "images",
                    "--no-trunc",
                    "--format",
                    "{{.ID}}",
                    "--filter",
                    f"reference={tag}",
                ).strip()
                if current != previous:
                    errors.append("a pre-existing shared candidate image tag changed")
            for tag in self.images.values() if self.source_candidate is None else ():
                tags = self.command(
                    "docker",
                    "images",
                    "--format",
                    "{{.Repository}}:{{.Tag}}",
                    "--filter",
                    f"reference={tag}",
                ).split()
                if tags:
                    self.command("docker", "image", "rm", *tags)
                remaining_tags = self.command(
                    "docker", "images", "--format", "{{.Repository}}:{{.Tag}}",
                    "--filter", f"reference={tag}",
                ).split()
                if remaining_tags:
                    errors.append("a session-owned candidate image tag survived cleanup")
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
        return errors


def configure_harness(monkeypatch: Any, run: AdmittedKind) -> None:
    load_module = sk._module
    values = sk.scripted_values
    mkdtemp = sk.tempfile.mkdtemp

    def external_module(name: str, path: Path) -> Any:
        loaded = load_module(name, path)
        if path.name == "github_stub.py":
            loaded.GithubStub = tracker_stub_type(loaded, redact=run.redact_model_evidence)
        elif path.name == "model_script.py":
            loaded.ModelScript = marker_model_type(loaded, redact=run.redact_model_evidence)
        return loaded

    monkeypatch.setattr(sk, "_module", external_module)
    monkeypatch.setattr(sk, "scripted_values", lambda **kw: run.values(values, **kw))

    def private_temp(*args: Any, **kwargs: Any) -> str:
        if not args and "dir" not in kwargs:
            kwargs["dir"] = run.root
        return mkdtemp(*args, **kwargs)

    monkeypatch.setattr(sk.tempfile, "mkdtemp", private_temp)


def query_path(path: str, **values: str) -> str:
    return path + "?" + urlencode(values)

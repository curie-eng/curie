"""Qualify delayed publication success after an HTTP timeout exhausts retries.

Drives the production PublicationCodeHostClient, PublicationReconciler and
PostgresPublicationStore. External HTTP, code-host and Kubernetes responses are
simulated. This file is not a pytest module and does not pin the failing
delayed-adoption case as a green product test.

Exit 0: a control scenario met its invariants.
Exit 1: the delayed scenario's safe-adoption invariant failed, or a control
invariant failed.
Exit 2: setup, infrastructure, cleanup or deadline failure.
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx
from channel_protocol.reply import ReplyAck
from curie_worker.publication_clients import (
    PublicationCodeHostClient,
    PublicationLineageClient,
)
from curie_worker.publication_k8s import PublicationJobSettings
from curie_worker.publication_loop import (
    PublicationCredential,
    PublicationJobObservation,
    PublicationReconciler,
)
from curie_worker.publication_store import PostgresPublicationStore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_POSTGRES_IMAGE = (
    "postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
)
_PARENT = "a" * 40
_HEAD = "b" * 40
_FOREIGN = "c" * 40
_PR_NUMBER = 41
_SEED_ATTEMPTS = 9
_DEFAULT_CAP = 10
_SCENARIO_BUDGET_SECONDS = 120
_OWNED: dict[str, str | None] = {"container": None}
_DB_READY_AT = 0.0

_JOB_SETTINGS = PublicationJobSettings(
    namespace="publication-timeout-qualification",
    runner_image="example.invalid/runner:qualification",
    image_pull_policy="IfNotPresent",
    image_pull_secrets=(),
    priority_class_name="qualification",
    service_account_name="qualification",
    owner_name="qualification",
    git_user_name="Publisher",
    git_user_email="publisher@example.invalid",
    cpu_request="100m",
    cpu_limit="1",
    memory_request="256Mi",
    memory_limit="1Gi",
    ephemeral_request="1Gi",
    ephemeral_limit="4Gi",
)


def _cleanup_owned() -> None:
    name = _OWNED["container"]
    if not name:
        return
    subprocess.run(
        ["docker", "rm", "-f", "-v", name],
        check=False,
        capture_output=True,
        text=True,
    )


def _container_absent(name: str) -> bool:
    inspected = subprocess.run(
        ["docker", "inspect", name],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspected.returncode == 0:
        return False
    detail = (inspected.stderr or inspected.stdout or "").lower()
    if "no such object" in detail or "not found" in detail:
        return True
    raise RuntimeError(detail.strip()[:300] or "docker inspect failed")


atexit.register(_cleanup_owned)


@dataclass
class Peer:
    """Simulated code-host HTTP peer outside the worker boundary."""

    scenario: str
    head: str
    parent: str
    branch: str
    released: bool = True
    prs: list[dict[str, object]] = field(default_factory=list)
    timeout_class: str | None = None
    timeout_cause: str | None = None
    requests: list[str] = field(default_factory=list)
    original_request_pending: bool = False
    completed_by_original_request: bool = False

    def _pull(self) -> dict[str, object]:
        return {
            "number": _PR_NUMBER,
            "url": f"https://code.example/acme/widget/pull/{_PR_NUMBER}",
            "state": "open",
            "head_sha": self.head,
            "head_ref": self.branch,
            "updated_at": "2026-01-01T00:00:00+00:00",
        }

    def release(self) -> None:
        """Finish the request the caller already timed out, without a new POST."""

        self.released = True
        if self.original_request_pending and not self.prs:
            self.prs.append(self._pull())
            self.completed_by_original_request = True
            self.original_request_pending = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append(f"{request.method} {path}")
        if path.endswith("/branch-head"):
            return httpx.Response(200, json={"head_sha": self.head})
        if path.endswith("/revision-commit"):
            body = json.loads(request.content.decode())
            if body.get("expected_parent") != self.parent:
                return httpx.Response(409, json={"detail": "parent mismatch"})
            if self.scenario == "foreign-head":
                return httpx.Response(
                    409,
                    json={"detail": {"message": "commit is not this revision"}},
                )
            observed = body.get("commit_sha")
            return httpx.Response(200, json={"commit_sha": observed})
        if path.endswith("/pull-request") and request.method == "POST":
            if self.scenario == "delayed-success-after-cap" and not self.released:
                self.original_request_pending = True
                exc = httpx.ReadTimeout("simulated read timeout", request=request)
                self.timeout_class = type(exc).__name__
                self.timeout_cause = str(exc)
                raise exc
            if not self.prs:
                self.prs.append(self._pull())
            return httpx.Response(200, json=self.prs[0])
        if path.endswith("/lineage") and request.method == "PATCH":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"detail": "unhandled simulated route"})


class _Credentials:
    def __init__(self, origin: str, repo: str) -> None:
        self.redeemed = 0
        self._origin = origin
        self._repo = repo

    async def redeem(self, publication_id: uuid.UUID) -> PublicationCredential:
        del publication_id
        self.redeemed += 1
        return PublicationCredential(
            clean_clone_url=f"{self._origin}/{self._repo}.git",
            authorization_header="Bearer synthetic",
            origin=self._origin,
            header_form="authorization_bearer",
        )


class _Cluster:
    def __init__(self) -> None:
        self.jobs = 0
        self.pushes = 0
        self.cleanups = 0

    async def observe(self, job_name: str) -> PublicationJobObservation:
        del job_name
        return PublicationJobObservation(phase="absent", logs="", exists=False)

    async def apply(self, resources: object) -> None:
        del resources
        self.jobs += 1

    async def validate_existing(self, resources: object) -> None:
        del resources
        raise AssertionError("qualification does not adopt an existing job")

    async def cleanup_credentials(self, names: object) -> None:
        del names
        self.cleanups += 1

    async def cleanup_terminal(self, names: object) -> None:
        del names
        self.cleanups += 1


class _Replies:
    async def emit(self, event: object, **_kwargs: object) -> ReplyAck:
        del event
        return ReplyAck()


def _fail(message: str, code: int = 2) -> None:
    print(json.dumps({"infrastructure_error": message}), flush=True)
    raise SystemExit(code)


def _start_postgres() -> tuple[str, str]:
    token = uuid.uuid4().hex[:12]
    name = f"publication-timeout-{token}"
    _OWNED["container"] = name
    try:
        started = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-e",
                "POSTGRES_USER=postgres",
                "-e",
                "POSTGRES_PASSWORD=postgres",
                "-e",
                "POSTGRES_DB=postgres",
                "-p",
                "5432",
                _POSTGRES_IMAGE,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        _fail("docker executable was unavailable")
    if started.returncode != 0:
        _fail(f"postgres container start failed: {started.stderr.strip()[:400]}")
    mapped = subprocess.run(
        ["docker", "port", name, "5432/tcp"],
        check=False,
        capture_output=True,
        text=True,
    )
    if mapped.returncode != 0 or ":" not in mapped.stdout:
        _fail("postgres published port was unavailable")
    port = mapped.stdout.strip().splitlines()[0].rsplit(":", 1)[-1]
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        ready = subprocess.run(
            ["docker", "exec", name, "pg_isready", "-U", "postgres"],
            check=False,
            capture_output=True,
            text=True,
        )
        if ready.returncode == 0:
            break
        time.sleep(0.5)
    else:
        _fail("postgres did not become ready")
    global _DB_READY_AT
    _DB_READY_AT = time.monotonic()
    async_url = f"postgresql+asyncpg://postgres:postgres@127.0.0.1:{port}/postgres"
    api_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "apps", "api")
    )
    remaining = _SCENARIO_BUDGET_SECONDS - (time.monotonic() - _DB_READY_AT)
    if remaining <= 0:
        _cleanup_owned()
        _fail("migration exceeded the scenario budget after database ready")
    try:
        upgraded = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=api_root,
            env={**os.environ, "DATABASE_URL": async_url},
            check=False,
            capture_output=True,
            text=True,
            timeout=remaining,
        )
    except subprocess.TimeoutExpired:
        _cleanup_owned()
        _fail("migration exceeded the scenario budget after database ready")
    if upgraded.returncode != 0:
        detail = (upgraded.stderr or upgraded.stdout or "alembic failed").strip()
        _fail(f"migration failed: {detail[:2000]}")
    return name, async_url


async def _seed(engine: AsyncEngine, publication_id: uuid.UUID, attempts: int) -> dict[str, str]:
    agent_id = uuid.uuid4()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    approval_id = uuid.uuid4()
    lineage_id = uuid.uuid4()
    branch = f"curie/publication-{publication_id.hex}"
    repo = "acme/widget"
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, 'widget', :repo)"
            ),
            {"id": agent_id, "repo": repo},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.agent_versions "
                "(id, agent_id, version_label, created_by) "
                "VALUES (:id, :agent_id, 'qualification', 'fixture')"
            ),
            {"id": version_id, "agent_id": agent_id},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.deployments "
                "(id, agent_id, version_id, environment, workspace_enabled, status) "
                "VALUES (:id, :agent_id, :version_id, 'dev', true, 'active')"
            ),
            {"id": deployment_id, "agent_id": agent_id, "version_id": version_id},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.approvals "
                "(id, agent_id, conversation_id, author, summary, reply_kind, "
                "reply_channel, reply_placeholder, dedupe_key, status, resolved_by, "
                "resolution_note, purpose) VALUES "
                "(:id, :agent_id, '1700000000.000100', 'requester@example.invalid', "
                "'Publish these repository changes?', 'slack', 'C0EXAMPLE1', "
                "'1700000000.000200', :dedupe, 'approved', "
                "'approver', 'Approved synthetic publication.', 'publication')"
            ),
            {
                "id": approval_id,
                "agent_id": agent_id,
                "dedupe": f"qualification-{publication_id.hex}",
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.thread_publication_lineages "
                "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                "base_sha, branch, status, version, latest_revision) VALUES "
                "(:id, :agent_id, :deployment_id, 'slack:C0EXAMPLE1:1700000000.000100', "
                ":repo, :base_sha, :branch, 'open', 1, 1)"
            ),
            {
                "id": lineage_id,
                "agent_id": agent_id,
                "deployment_id": deployment_id,
                "repo": repo,
                "base_sha": _PARENT,
                "branch": branch,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.publications "
                "(id, approval_id, deployment_id, workspace_conversation_id, "
                "lineage_id, revision_number, expected_prior_head, repo_full_name, "
                "status, base_sha, patch_bytes, changed_paths, title, body, reply_kind, "
                "reply_channel, reply_placeholder, approval_card_reported_at, "
                "reconcile_attempts) VALUES "
                "(:id, :approval_id, :deployment_id, "
                "'slack:C0EXAMPLE1:1700000000.000100', :lineage_id, 1, :base_sha, "
                ":repo, 'approved', :base_sha, :patch, "
                "CAST(:changed_paths AS jsonb), 'Publish qualification', "
                "'Synthetic approved publication.', 'slack', 'C0EXAMPLE1', "
                "'1700000000.000200', now(), :attempts)"
            ),
            {
                "id": publication_id,
                "approval_id": approval_id,
                "deployment_id": deployment_id,
                "lineage_id": lineage_id,
                "base_sha": _PARENT,
                "repo": repo,
                "patch": b"diff --git a/README.md b/README.md\n",
                "changed_paths": '["README.md"]',
                "attempts": attempts,
            },
        )
    return {"branch": branch, "repo": repo, "publication_id": str(publication_id)}


async def _read_row(engine: AsyncEngine, publication_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    text(
                        "SELECT status, reconcile_attempts, patch_bytes IS NULL AS patch_cleared, "
                        "error, result_url, "
                        "reconcile_dead_lettered_at IS NOT NULL AS dead_lettered, "
                        "terminal_at IS NOT NULL AS terminal "
                        "FROM curie.publications WHERE id = :id"
                    ),
                    {"id": publication_id},
                )
            )
            .mappings()
            .one()
        )
    return {
        "status": row["status"],
        "reconcile_attempts": int(row["reconcile_attempts"]),
        "patch_cleared": bool(row["patch_cleared"]),
        "error": row["error"],
        "result_url": row["result_url"],
        "dead_lettered": bool(row["dead_lettered"]),
        "terminal": bool(row["terminal"]),
    }


async def _run_scenario(scenario: str, async_url: str) -> dict[str, Any]:
    publication_id = uuid.uuid4()
    attempts = _SEED_ATTEMPTS if scenario == "delayed-success-after-cap" else 0
    head = _FOREIGN if scenario == "foreign-head" else _HEAD
    engine = create_async_engine(async_url)
    try:
        seeded = await _seed(engine, publication_id, attempts)
        peer = Peer(
            scenario=scenario,
            head=head,
            parent=_PARENT,
            branch=seeded["branch"],
            released=scenario != "delayed-success-after-cap",
        )
        cluster = _Cluster()
        credentials = _Credentials("https://code.example", seeded["repo"])
        store = PostgresPublicationStore(engine, schema="curie", lease_owner="qualification-worker")
        transport = httpx.MockTransport(peer.handler)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://publication-peer.example",
            timeout=30.0,
        ) as client:
            code_host = PublicationCodeHostClient(
                api_base_url="http://publication-peer.example",
                worker_token="synthetic-worker-token",
                client=client,
            )
            lineage = PublicationLineageClient(
                api_base_url="http://publication-peer.example",
                worker_token="synthetic-worker-token",
                client=client,
            )
            reconciler = PublicationReconciler(
                store=store,
                credentials=credentials,
                cluster=cluster,
                code_host=code_host,
                replies=_Replies(),
                lineage=lineage,
                job_settings=_JOB_SETTINGS,
            )
            work = await store.claim_next()
            if work is None:
                raise RuntimeError("seeded publication was not claimable")
            await reconciler.reconcile(work)
            after_reconcile = await _read_row(engine, publication_id)
            remote_after_terminal: dict[str, object] | None = None
            if scenario == "delayed-success-after-cap":
                if after_reconcile["reconcile_attempts"] != _DEFAULT_CAP:
                    raise RuntimeError("tenth attempt was not charged before delayed success")
                if after_reconcile["status"] != "failed" or not after_reconcile["patch_cleared"]:
                    raise RuntimeError("cap did not terminalize and clear the patch")
                peer.release()
                if not peer.completed_by_original_request or len(peer.prs) != 1:
                    raise RuntimeError("delayed success was not the original timed-out request")
                remote_after_terminal = {
                    "pull": None,
                    "exception_class": None,
                    "exception_cause": None,
                }
                try:
                    recovered = await code_host.recover_pull_request(
                        publication_id, expected_head_sha=_HEAD
                    )
                except Exception as exc:  # noqa: BLE001 - report the real client failure
                    remote_after_terminal = {
                        "pull": None,
                        "exception_class": type(exc).__name__,
                        "exception_cause": type(exc.__cause__).__name__ if exc.__cause__ else None,
                    }
                else:
                    remote_after_terminal = {
                        "pull": None
                        if recovered is None
                        else {
                            "number": recovered.number,
                            "url": recovered.url,
                            "head_sha": recovered.head_sha,
                            "head_ref": recovered.head_ref,
                        },
                        "exception_class": None,
                        "exception_cause": None,
                    }
            follower = PostgresPublicationStore(
                engine, schema="curie", lease_owner="qualification-follower"
            )
            reclaimed = await follower.claim_next()
            if reclaimed is not None:
                await reconciler.reconcile(reclaimed)
                reclaimed = await follower.claim_next()
        stored = await _read_row(engine, publication_id)
        adopted = (
            stored["status"] == "succeeded"
            and stored["result_url"] == f"https://code.example/acme/widget/pull/{_PR_NUMBER}"
            and reclaimed is None
        )
        no_duplicate = len(peer.prs) <= 1 and cluster.jobs == 0 and cluster.pushes == 0
        foreign_refused = (
            scenario == "foreign-head"
            and stored["status"] != "succeeded"
            and stored["result_url"] is None
            and len(peer.prs) == 0
            and cluster.jobs == 0
            and cluster.pushes == 0
        )
        return {
            "scenario": scenario,
            "seed_reconcile_attempts": attempts,
            "default_cap": _DEFAULT_CAP,
            "expected": {
                "delayed-success-after-cap": "adopt the single delayed pull request",
                "immediate-success": "adopt the same pull request with no second side effect",
                "foreign-head": "refuse the foreign head and add no remote mutation",
            }[scenario],
            "observed_store": stored,
            "after_reconcile": after_reconcile,
            "claim_eligible_after": reclaimed is not None,
            "remote_pr_count": len(peer.prs),
            "completed_by_original_request": peer.completed_by_original_request,
            "remote_after_terminal": remote_after_terminal,
            "exception_class": "PublicationReconcileError" if stored["error"] else None,
            "exception_cause": peer.timeout_class,
            "timeout_class": peer.timeout_class,
            "timeout_cause": peer.timeout_cause,
            "job_count": cluster.jobs,
            "push_count": cluster.pushes,
            "cleanup_calls": cluster.cleanups,
            "credential_redemptions": credentials.redeemed,
            "verified_identity": {
                "parent": _PARENT,
                "head": _HEAD,
                "observed_branch_head": head,
                "branch": seeded["branch"],
            },
            "invariants": {
                "safe_adoption": adopted,
                "no_duplicate": no_duplicate,
                "foreign_refused": foreign_refused,
            },
            "simulation_boundary": (
                "HTTP code-host and lineage responses, credential redemption and "
                "Kubernetes Job observation are simulated. The production client, "
                "reconciler, store and claim_next run against migrated Postgres. "
                "This does not qualify the API process, a live forge, or a deadline "
                "crossing."
            ),
            "http_trace": peer.requests,
        }
    finally:
        await engine.dispose()


def _exit_for(report: dict[str, Any]) -> int:
    scenario = report["scenario"]
    invariants = report["invariants"]
    if scenario == "delayed-success-after-cap":
        if not invariants["safe_adoption"]:
            return 1
        if not invariants["no_duplicate"]:
            return 1
        return 0
    if scenario == "immediate-success":
        if invariants["safe_adoption"] and invariants["no_duplicate"]:
            return 0
        return 1
    if scenario == "foreign-head":
        if invariants["foreign_refused"] and invariants["no_duplicate"]:
            return 0
        return 1
    return 2


async def _prerequisite_failure() -> None:
    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@127.0.0.1:1/postgres")
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - unavailable infrastructure is exit 2
        print(
            json.dumps(
                {
                    "scenario": "prerequisite-failure",
                    "exception_class": type(exc).__name__,
                    "infrastructure_error": "postgres prerequisite unavailable",
                }
            ),
            flush=True,
        )
        raise SystemExit(2) from exc
    else:
        _fail("prerequisite-failure connected unexpectedly")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        required=True,
        choices=(
            "delayed-success-after-cap",
            "immediate-success",
            "foreign-head",
            "prerequisite-failure",
        ),
    )
    args = parser.parse_args()
    if args.scenario == "prerequisite-failure":
        asyncio.run(_prerequisite_failure())
        return
    try:
        container, async_url = _start_postgres()
    except SystemExit:
        _cleanup_owned()
        raise
    ready = _DB_READY_AT or time.monotonic()

    async def bounded() -> dict[str, Any]:
        remaining = _SCENARIO_BUDGET_SECONDS - (time.monotonic() - ready)
        if remaining <= 0:
            raise TimeoutError
        return await asyncio.wait_for(
            _run_scenario(args.scenario, async_url),
            timeout=remaining,
        )

    try:
        report = asyncio.run(bounded())
    except TimeoutError:
        _cleanup_owned()
        _fail(f"scenario exceeded {_SCENARIO_BUDGET_SECONDS}s after database ready")
    except SystemExit:
        _cleanup_owned()
        raise
    except Exception as exc:  # noqa: BLE001 - harness failures stay exit 2
        _cleanup_owned()
        _fail(f"{type(exc).__name__}: {exc}")
    elapsed = time.monotonic() - ready
    report["elapsed_seconds_after_db_ready"] = round(elapsed, 3)
    report["budget_seconds"] = _SCENARIO_BUDGET_SECONDS
    _cleanup_owned()
    try:
        absent = _container_absent(container) if container else True
    except RuntimeError as exc:
        report["cleanup"] = {"container_absent": False, "error": str(exc)}
        print(json.dumps(report, default=str), flush=True)
        raise SystemExit(2) from exc
    report["cleanup"] = {"container_absent": absent, "container": container}
    print(json.dumps(report, default=str), flush=True)
    if not absent:
        raise SystemExit(2)
    raise SystemExit(_exit_for(report))


if __name__ == "__main__":
    main()

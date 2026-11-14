#!/usr/bin/env python3
"""Proof driver for requester coding instructions (#5592, ADR 0278), criteria 9 and 10.

Four subcommands share one private SQLite ledger under ``--state-dir``:

1. ``preflight`` (read-only) runs the owner enforcement driver's probes (readyz,
   namespace absence, RBAC, ``docker info``, the loopback registry, the input
   file, a clean candidate checkout, the Python environment), this ledger's
   unused run ID, and stats the live inputs: the App kit, both token files, the
   model credential file, the conflict file and the PriorityClass. No secret is
   read.
2. ``rehearse`` (unpaid) runs on the caller's throwaway cluster with the
   runner's offline fake model and the TLS GitHub stub. Two OIDC principals get
   GitHub identity links and their own documents; a third actor stays unlinked.
   One WorkItem per actor is admitted in service mode with the requester the
   production resolver returns. The API's own records must show each linked
   request ``applied`` with its own digest and the unlinked one ``none``, and
   no document line may appear in the API, worker or runner logs. The full
   finalizer then deletes everything and proves it gone.
3. ``run`` makes one bounded live attempt of at most three slots on the public
   fixture repository through polling intake: the owner with a document holding
   a machine-checkable preference and one that conflicts with a repository rule,
   the second account with another document, and the owner with the document
   cleared. The model, App and both user tokens are probed immediately before
   the first label. Each slot is recorded before its label.
4. ``judge --criterion {9,10,all}`` reads the ledger only. A criterion whose
   proof did not run is reported missing, never passed.

``rehearse`` and ``run`` re-execute this file as an ``env -i`` child, as the
owner enforcement driver does, and run on its harness (``OwnerProof``). See the
README section "Requester coding instructions proof".
"""

from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import personal_owner_enforcement as pod  # noqa: E402
import personal_owner_enforcement_support as poe  # noqa: E402
import personal_profile_support as pps  # noqa: E402
import requester_instructions_support as ris  # noqa: E402
import tls_code_host as tch  # noqa: E402

REPO_ROOT = HERE.parents[1]
CHILD = "_child"
GITHUB_HOST = "github.com"
GITHUB_API = f"https://{pps.GITHUB_API_HOST}"
DEFAULT_SLOT_SECONDS = 1500
SCAN_INTERVAL = 10.0
SETTLE_SECONDS = 300
TERMINAL_SECONDS = 300
REHEARSE_OBJECTIVE = pod.REHEARSE_PAIR_OBJECTIVE
REHEARSE_CONVERSATIONS = {
    "first": "1700000000.000510",
    "second": "1700000000.000520",
    "unlinked": "1700000000.000530",
}
TRANSCRIPT_ROOTS = ("/home/runner", "/tmp")
# The production resolver, run against the owned database with the API's env.
RESOLVE_PROGRAM = """\
import asyncio, json, sys, uuid
from curie_api.db import create_engine, create_sessionmaker
from curie_api.factory_slack import resolve_github_requester
async def main():
    engine = create_engine()
    found = {}
    try:
        maker = create_sessionmaker(engine)
        for account in sys.argv[3:]:
            async with maker() as session, session.begin():
                principal = await resolve_github_requester(
                    session, agent_id=uuid.UUID(sys.argv[1]), tracker_host=sys.argv[2],
                    account_id=int(account))
            found[account] = str(principal) if principal else None
    finally:
        await engine.dispose()
    print(json.dumps(found))
asyncio.run(main())
"""
SLOT_ISSUE_BODY = (
    "Requester preferences proof, slot {slot}. Add one new file `{path}` containing a "
    "single short sentence that describes this repository. Change nothing else."
)


# --------------------------------------------------------------------------
# Arguments
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="requester_instructions.py",
        description="Proof of requester coding instructions (#5592). See README.md.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def common(sub: argparse.ArgumentParser, *, live: bool) -> None:
        for flag in (
            "--candidate",
            "--context",
            "--namespace",
            "--docker-context",
            "--registry",
            "--input-file",
            "--state-dir",
            "--run-id",
        ):
            sub.add_argument(flag, required=True)
        sub.add_argument("--disk-start-percent", type=int, default=82)
        sub.add_argument("--disk-stop-percent", type=int, default=88)
        sub.add_argument("--app-dir", required=live, default="")
        sub.add_argument("--owner-token-file", required=live, default="")
        sub.add_argument("--second-token-file", required=live, default="")
        sub.add_argument("--model-credential-file", required=live, default="")
        sub.add_argument("--conflict-file", required=live, default="")
        sub.add_argument("--priority-class", required=live, default="")
        sub.add_argument("--model", default=poe.MODEL)
        sub.add_argument("--slot-seconds", type=int, default=DEFAULT_SLOT_SECONDS)

    common(commands.add_parser("preflight"), live=True)
    common(commands.add_parser("rehearse"), live=False)
    common(commands.add_parser("run"), live=True)
    judge = commands.add_parser("judge")
    judge.add_argument("--state-dir", required=True)
    judge.add_argument("--run-id", required=True, action="append")
    judge.add_argument("--criterion", required=True, choices=(*ris.CRITERIA, "all"))
    child = commands.add_parser(CHILD)
    child.add_argument("mode", choices=("rehearse", "run"))
    common(child, live=False)
    return parser


def forwarded(args: argparse.Namespace) -> list[str]:
    argv: list[str] = []
    for flag in (
        "candidate",
        "context",
        "namespace",
        "docker_context",
        "registry",
        "input_file",
        "state_dir",
        "run_id",
        "disk_start_percent",
        "disk_stop_percent",
        "app_dir",
        "owner_token_file",
        "second_token_file",
        "model_credential_file",
        "conflict_file",
        "priority_class",
        "model",
        "slot_seconds",
    ):
        value = getattr(args, flag, "")
        if value:
            argv += ["--" + flag.replace("_", "-"), str(value)]
    return argv


def require_live_shape(args: argparse.Namespace) -> None:
    if not 300 <= args.slot_seconds <= 3600:
        raise ris.Refusal("live_arguments_invalid", "--slot-seconds")
    if not args.model or any(c.isspace() for c in args.model):
        raise ris.Refusal("live_arguments_invalid", "--model")


# --------------------------------------------------------------------------
# Preflight (read-only)
# --------------------------------------------------------------------------


def preflight(args: argparse.Namespace, rt: pod.Runtime, command: str) -> dict[str, Any]:
    """The owner enforcement probes, then this ledger, then (live) the inputs by stat."""

    plan = ris.preflight_plan(command)
    # The owner driver's own probes, unchanged; it reads no grant here.
    found = pod.preflight(args, rt, grant=False)
    checks = dict(found["checks"])
    flags: poe.Flags = found["flags"]
    ris.run_id_unused(flags.state_dir, flags.run_id)
    checks["run_id_unused"] = True
    if "app_kit" in plan:
        require_live_shape(args)
        ris.load_app_kit(Path(args.app_dir))
        checks["app_kit"] = True
        ris.stat_token_file(Path(args.owner_token_file), "--owner-token-file")
        checks["owner_token_file"] = True
        ris.stat_token_file(Path(args.second_token_file), "--second-token-file")
        checks["second_token_file"] = True
        ris.stat_model_credential(Path(args.model_credential_file))
        checks["model_credential_file"] = True
        ris.load_conflict(Path(args.conflict_file))
        checks["conflict_file"] = True
        env = {name: rt.environ[name] for name in poe.CHILD_ENV_NAMES if rt.environ.get(name)}
        env["KUBECONFIG"] = found["inputs"].kubeconfig
        answer = pod.probe(
            poe.kubectl_argv(
                flags.context, None, "get", "priorityclass", args.priority_class, "-o", "name"
            ),
            env,
            rt,
        )
        if answer.returncode != 0:
            raise ris.Refusal("priority_class_invalid", "--priority-class")
        checks["priority_class"] = True
    return {**found, "checks": checks}


def cmd_preflight(args: argparse.Namespace, rt: pod.Runtime) -> int:
    found = preflight(args, rt, "preflight")
    rt.out.emit(
        {
            "preflight": "ok",
            "checks": found["checks"],
            "caps": ris.caps(),
            "live_start_probes": list(ris.LIVE_START_PROBES),
        }
    )
    return poe.EXIT_PASS


def spawn_child(
    mode: str, args: argparse.Namespace, rt: pod.Runtime, found: Mapping[str, Any]
) -> int:
    """Re-execute this file as an ``env -i`` child: allowlisted names and paths only."""

    flags: poe.Flags = found["flags"]
    env = poe.child_env(
        rt.environ,
        flags=flags,
        inputs=found["inputs"],
        evidence_dir=flags.state_dir / f"evidence-{flags.run_id}",
    )
    argv = [sys.executable, str(Path(__file__).resolve()), CHILD, mode, *forwarded(args)]
    process = subprocess.Popen(
        argv, env=env, stdin=subprocess.DEVNULL, cwd=REPO_ROOT, start_new_session=True
    )

    def relay(signum: int, _frame: object) -> None:
        with contextlib.suppress(OSError):
            process.send_signal(signum)

    previous = {sig: signal.signal(sig, relay) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        return process.wait()
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)


def cmd_rehearse(args: argparse.Namespace, rt: pod.Runtime) -> int:
    found = preflight(args, rt, "rehearse")
    rt.out.progress("preflight passed; starting the rehearsal child")
    return spawn_child("rehearse", args, rt, found)


def cmd_run(args: argparse.Namespace, rt: pod.Runtime) -> int:
    found = preflight(args, rt, "run")
    rt.out.progress("preflight passed; starting the live child")
    return spawn_child("run", args, rt, found)


def cmd_judge(args: argparse.Namespace, rt: pod.Runtime) -> int:
    state = Path(args.state_dir)
    pps.require_private_dir(state)
    records = []
    for run_id in args.run_id:
        pps.require_run_id(run_id)
        ledger = ris.RequesterLedger.open(state, run_id, create=False)
        try:
            records.append(ris.record_of(ledger))
        finally:
            ledger.close()
    verdict = ris.judge(records, args.criterion)
    rt.out.emit(verdict)
    return ris.exit_code(verdict["verdict"])


# --------------------------------------------------------------------------
# The child: the proof on the owner enforcement harness
# --------------------------------------------------------------------------


class LiveInputs:
    """The live run's private inputs. Held values never reach argv, logs or the ledger."""

    def __init__(self, args: argparse.Namespace) -> None:
        require_live_shape(args)
        self.app_dir = Path(args.app_dir)
        self.app = ris.load_app_kit(self.app_dir)
        self.owner_token = ris.read_token_file(Path(args.owner_token_file), "--owner-token-file")
        self.second_token = ris.read_token_file(Path(args.second_token_file), "--second-token-file")
        self.model_url, self.model_key = ris.load_model_credential(Path(args.model_credential_file))
        self.conflict = ris.load_conflict(Path(args.conflict_file))
        self.priority_class = args.priority_class
        self.model = args.model
        self.slot_seconds = args.slot_seconds

    def __repr__(self) -> str:
        return "<live inputs>"

    def held(self) -> list[pps.HeldSecret]:
        return [self.owner_token, self.second_token, self.model_url, self.model_key]


def requester_proof_class(e: Any) -> type[Any]:  # noqa: C901 - one scenario, one class
    """``RequesterProof`` over the owner enforcement harness (loaded at runtime)."""

    import personal_profile as pp

    cluster: Any = sys.modules["personal_token_cluster_support"]
    owner_proof = pod.owner_proof_class(e)

    class RequesterProof(owner_proof):  # type: ignore[misc, valid-type]
        def __repr__(self) -> str:
            return "<private requester instructions proof>"

        def __init__(self, ctx: pod.ChildContext, live: LiveInputs | None) -> None:
            super().__init__(ctx)
            self.live_inputs = live
            self.nonce = secrets.token_hex(6)
            self.documents: dict[str, ris.Document] = {}
            self.revisions: dict[str, int] = {}
            self.linked: dict[str, int] = {}
            self.slot: int | None = None
            self.slot_scans: dict[int, dict[str, ris.ReviewerScan]] = {}
            self.last_scan: dict[str, float] = {}
            self.forge: Any = None
            self.label = f"rqp-{self.nonce}"
            self.branch_prefix = f"rqp-{self.nonce}-"
            self.created_issues: list[str] = []
            self.created_pulls: list[int] = []
            self.label_created = False
            self.resolved: dict[str, str | None] = {}
            self.items: dict[str, str] = {}
            self.accounts: dict[str, dict[str, Any]] = {}
            if live is not None:
                for held in live.held():
                    ctx.rt.scrubber.hold(held)
                # The source template's installation credential is the live endpoint.
                self.service = e.HeldSecret(live.model_key.reveal())
                self.service_url = e.HeldSecret(live.model_url.reveal())
            self.sync_scrubber()

        def held(self) -> list[Any]:
            live = getattr(self, "live_inputs", None)
            extra = [] if live is None else live.held()
            return [*super().held(), *extra]

        # --- environment ------------------------------------------------------

        def api_env(self) -> dict[str, str]:
            env = {
                **super().api_env(),
                "FACTORY_MODEL_AUTH_MODE": "service",
                "CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY": "true",
            }
            if self.live_inputs is None:
                return env
            live = self.live_inputs
            pem = (live.app_dir / "app.pem").read_text()
            credential = {
                "type": "github_app",
                "app_id": live.app["id"],
                "private_key_file": str(live.app_dir / "app.pem"),
            }
            github = {
                "name": "github",
                "kind": "github",
                "host": GITHUB_HOST,
                "api_url": GITHUB_API,
            }
            env.update(
                {
                    "FACTORY_ENABLED": "true",
                    "FACTORY_INTAKE": "poll",
                    "CURIE_FORGES": json.dumps(
                        {
                            "code_hosts": [{**github, "credential": credential}],
                            "trackers": [
                                {
                                    **github,
                                    "credential": credential,
                                    "label": self.label,
                                    "mention": "@curie",
                                    "poll_interval_s": 15,
                                }
                            ],
                        }
                    ),
                    "GITHUB_API_URL": GITHUB_API,
                    "GITHUB_CLONE_BASE": f"https://{GITHUB_HOST}",
                    "GITHUB_APP_ID": live.app["id"],
                    "GITHUB_APP_PRIVATE_KEY": pem,
                    "GITHUB_REPO_ALLOWLIST": json.dumps([live.app["repo"]]),
                }
            )
            return env

        def worker_env(self) -> dict[str, str]:
            env = {**super().worker_env(), "FACTORY_MODEL_AUTH_MODE": "service"}
            if self.live_inputs is None:
                return env
            env.pop("CURIE_FAKE_MODEL", None)
            env.update(
                {
                    "CURIE_MODEL": self.live_inputs.model,
                    "DEFAULT_MAX_USD_PER_DAY": str(ris.SESSION_USD),
                    "CURIE_WORK_ITEM_MAX_TURNS": str(1 + ris.MAX_CONTINUATIONS),
                    "DEFAULT_MAX_OUTPUT_TOKENS_PER_RUN": "32000",
                    "CURIE_RUNNER_TOTAL_TIMEOUT_S": str(self.live_inputs.slot_seconds),
                    "CURIE_RUNNER_IMAGE": self.image,
                    "CURIE_PUBLICATION_PRIORITY_CLASS_NAME": self.live_inputs.priority_class,
                }
            )
            return env

        def bundle(self) -> dict[str, str]:
            # No sentinel connector and no hold hooks: nothing here gates a turn.
            return dict(cluster.bundle_files())

        def create_inputs(self) -> None:
            super().create_inputs()
            if self.live_inputs is None:
                return
            labels = {"curie-proof-owner": self.ownership}
            self.core.create_namespaced_service_account(
                self.namespace,
                {"metadata": {"name": "curie-publication", "labels": labels}},
                _request_timeout=30,
            )
            self.core.create_namespaced_config_map(
                self.namespace,
                {"metadata": {"name": "curie-publication-owner", "labels": labels}, "data": {}},
                _request_timeout=30,
            )

        # --- HTTP to the candidate API -------------------------------------------

        def call(
            self,
            method: str,
            path: str,
            body: Any = None,
            *,
            headers: Mapping[str, str],
        ) -> tuple[int, dict[str, Any]]:
            connection = http.client.HTTPConnection("127.0.0.1", self.port_api(), timeout=60)
            try:
                connection.request(
                    method,
                    path,
                    body=None if body is None else json.dumps(body).encode(),
                    headers={"Content-Type": "application/json", **headers},
                )
                response = connection.getresponse()
                raw = response.read()
            except (OSError, http.client.HTTPException):
                raise poe.StepFailed("the candidate API did not answer") from None
            finally:
                connection.close()
            self.ctx.rt.scan.scan("children", raw)
            try:
                parsed = json.loads(raw) if raw else {}
            except ValueError:
                parsed = {}
            return response.status, parsed if isinstance(parsed, dict) else {}

        def admin(self, method: str, path: str, body: Any = None) -> tuple[int, dict[str, Any]]:
            return self.call(method, path, body, headers={"X-API-Key": self.platform_key.reveal()})

        def console(
            self, who: str, method: str, path: str, body: Any = None
        ) -> tuple[int, dict[str, Any]]:
            return self.call(
                method,
                path,
                body,
                headers={
                    "Cookie": f"{e.SESSION_COOKIE}={self.sessions[who].reveal()}",
                    "Origin": self.api_url,
                },
            )

        # --- shared phases --------------------------------------------------------

        def link(self, who: str, host: str, account_id: int) -> bool:
            status, _ = self.admin(
                "POST",
                f"/identity/admin/{self.tenant_id}/code-host-links",
                {
                    "principal_id": self.principals[who]["id"],
                    "code_host_kind": "github",
                    "host": host,
                    "account_id": str(account_id),
                    "created_by_principal_id": self.principals["owner"]["id"],
                },
            )
            self.linked[who] = account_id
            return status == 201

        def resolve(self, host: str, accounts: Sequence[int]) -> dict[str, str | None]:
            try:
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        RESOLVE_PROGRAM,
                        self.agent_id,
                        host,
                        *(str(a) for a in accounts),
                    ],
                    env=self.api_env(),
                    cwd=self.private_dir,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    timeout=60,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                raise poe.StepFailed("the production resolver did not answer") from None
            self.ctx.rt.scan.scan("children", result.stdout + result.stderr)
            self.record_private_output("resolve", result.stdout + result.stderr)
            try:
                found = json.loads(result.stdout.decode().strip().splitlines()[-1])
            except (ValueError, IndexError, UnicodeDecodeError):
                raise poe.StepFailed("the production resolver answer is unreadable") from None
            if result.returncode != 0 or not isinstance(found, dict):
                raise poe.StepFailed("the production resolver failed")
            return {str(k): (str(v) if v else None) for k, v in found.items()}

        def store_document(self, who: str, role: str, conflict: str = "") -> None:
            document = ris.make_document(role, self.nonce, conflict=conflict)
            status, body = self.console(
                who, "PUT", "/coding-instructions", {"text": document.text, "expected_revision": 0}
            )
            if status != 200:
                raise poe.StepFailed(f"the {role} document was refused ({status})")
            self.documents[role] = document
            self.revisions[who] = int(body.get("revision") or 0)
            criterion = "10" if self.live_inputs is not None else "9"
            self.observe(
                criterion, f"document.{role}.digest_matches", body.get("sha256") == document.sha256
            )

        def work_item_row(self, work_item_id: str) -> dict[str, Any]:
            rows = self.sql_rows(
                "SELECT requester_principal_id, coding_instructions_state, "
                "coding_instructions_sha256 FROM curie.work_items WHERE id = :w",
                {"w": uuid.UUID(work_item_id)},
            )
            return dict(rows[0]) if rows else {}

        def evidence_of(self, work_item_id: str) -> str | None:
            rows = self.sql_rows(
                "SELECT coding_instructions_evidence FROM curie.execution_requests "
                "WHERE work_item_id = :w ORDER BY created_at DESC LIMIT 1",
                {"w": uuid.UUID(work_item_id)},
            )
            return str(rows[0]["coding_instructions_evidence"]) if rows else None

        def log_bytes(self) -> dict[str, bytes]:
            found: dict[str, bytes] = {}
            for name in ("api", "worker"):
                path = self.retained_dir / f"{name}.log"
                found[name] = path.read_bytes() if path.exists() else b""
            runner = b""
            for streams in self.pod_log_streams.values():
                for path, _process in streams:
                    with contextlib.suppress(OSError):
                        runner += path.read_bytes()
            found["runner"] = runner
            return found

        # --- rehearsal (criterion 9) ------------------------------------------

        def execute(self) -> None:
            self.snapshot_foreign()
            phases: tuple[tuple[str, Callable[[], None]], ...] = (
                ("setup", self.setup),
                ("prepare", self.prepare_owner),
            )
            if self.live_inputs is None:
                phases += (
                    ("links", self.rehearse_links),
                    ("documents", self.rehearse_documents),
                    ("admit", self.rehearse_admit),
                    ("audit", self.rehearse_audit),
                )
            else:
                phases += (
                    ("probes", self.live_probes),
                    ("links", self.live_links),
                    ("factory", self.live_factory),
                    ("slots", self.live_slots),
                )
            for name, action in phases:
                self.step(name)
                self.say(f"phase {name}")
                action()
            self.step("completed")

        def rehearse_links(self) -> None:
            host = self.code_host_origin.removeprefix("https://")
            self.observe("9", "setup.fake_model", self.worker_env().get("CURIE_FAKE_MODEL") == "1")
            self.observe(
                "9",
                "setup.principals_distinct",
                self.principals["owner"]["id"] != self.principals["other"]["id"],
            )
            accounts = dict(ris.REHEARSAL_ACTORS)
            self.observe("9", "link.first", self.link("owner", host, accounts["first"]))
            self.observe("9", "link.second", self.link("other", host, accounts["second"]))
            resolved = self.resolve(host, [accounts[name] for name, _ in ris.REHEARSAL_ACTORS])
            self.resolved = {name: resolved.get(str(accounts[name])) for name in accounts}
            self.observe(
                "9", "resolve.first", self.resolved["first"] == self.principals["owner"]["id"]
            )
            self.observe(
                "9", "resolve.second", self.resolved["second"] == self.principals["other"]["id"]
            )
            self.observe("9", "resolve.unlinked", self.resolved["unlinked"] is None)

        def rehearse_documents(self) -> None:
            self.store_document("owner", "first")
            self.store_document("other", "second")
            self.observe(
                "9",
                "document.digests_distinct",
                self.documents["first"].sha256 != self.documents["second"].sha256,
            )

        def rehearse_admit(self) -> None:
            host = self.code_host_origin.removeprefix("https://")
            self.stop_worker()
            self.ensure_forward()
            for actor, _account in ris.REHEARSAL_ACTORS:
                status, issue = self.stub_api(
                    "POST",
                    f"/repos/{tch.STUB_REPO}/issues",
                    {"title": f"Requester preferences proof: {actor}", "body": "Reply only."},
                )
                if status != 201 or not isinstance(issue, dict):
                    raise poe.StepFailed("the code host stub refused an issue")
                status, body = self.api(
                    "POST",
                    "/v1/internal/work-items/admissions",
                    {
                        "agent_id": self.agent_id,
                        "kind": e.KIND,
                        "address": e.ADDRESS,
                        "reply_conversation_id": REHEARSE_CONVERSATIONS[actor],
                        "tracker": {
                            "kind": "github",
                            "host": host,
                            "scope_id": self.repository_id,
                            "issue_id": str(issue["number"]),
                        },
                        "repository": {
                            "code_host_kind": "github",
                            "host": host,
                            "project_id": self.repository_id,
                            "path": tch.STUB_REPO,
                        },
                        "code_host_installation_id": tch.STUB_INSTALLATION_ID,
                        "objective": REHEARSE_OBJECTIVE,
                        "requester": e.REQUESTER,
                        "request_id": str(uuid.uuid4()),
                        "model_auth_mode": "service",
                        "requester_principal_id": self.resolved[actor],
                    },
                )
                if status != 200:
                    raise poe.StepFailed(f"the {actor} admission was refused: {self.code_of(body)}")
                self.items[actor] = str(body["work_item"]["id"])
            self.start_worker()

            def settled() -> bool | None:
                evidence = [self.evidence_of(item) for item in self.items.values()]
                return True if all(v not in (None, "pending") for v in evidence) else None

            self.wait(settled, timeout=SETTLE_SECONDS, failure="evidence never settled")

            def terminal() -> bool | None:
                rows = [self.requests_of(item) for item in self.items.values()]
                return (
                    True
                    if all(r and r[-1]["status"] not in poe.ACTIVE_REQUEST_STATUSES for r in rows)
                    else None
                )

            self.wait_soft(terminal, timeout=TERMINAL_SECONDS)
            role_of = {"first": "first", "second": "second"}
            for actor, item in self.items.items():
                row = self.work_item_row(item)
                self.observe("9", f"request.{actor}.state", row.get("coding_instructions_state"))
                self.observe("9", f"request.{actor}.evidence", self.evidence_of(item))
                if actor in role_of:
                    self.observe(
                        "9",
                        f"request.{actor}.digest_matches",
                        row.get("coding_instructions_sha256")
                        == self.documents[role_of[actor]].sha256,
                    )
                status = [r["status"] for r in self.requests_of(item)]
                self.observe("9", f"request.{actor}.statuses", status)

        def rehearse_audit(self) -> None:
            self.sweep()
            logs = self.log_bytes()
            self.observe("9", "logs.api_captured", bool(logs["api"]))
            self.observe("9", "logs.worker_captured", bool(logs["worker"]))
            self.observe("9", "logs.runner_captured", bool(logs["runner"]))
            needles = ris.needles_of(self.documents.values())
            self.observe("9", "logs.canary_hits", sum(ris.hits(d, needles) for d in logs.values()))
            self.check_stub()

        # --- live run (criterion 10) ------------------------------------------

        def github(
            self, token: pps.HeldSecret, method: str, path: str, body: Any = None
        ) -> tuple[int, Any]:
            return pp.forge_http(self.ctx.rt.scan).request(
                method,
                path,
                headers={
                    "Authorization": f"token {token.reveal()}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                body=body,
            )

        def live_probes(self) -> None:
            """Precondition 4, immediately before the live start; nothing is labeled before."""

            live = self.live_inputs
            assert live is not None
            with tempfile.TemporaryDirectory(prefix="rqp-model-probe-") as home:
                request = json.dumps(
                    {
                        "base_url": live.model_url.reveal(),
                        "api_key": live.model_key.reveal(),
                        "model": live.model,
                    }
                ).encode()
                try:
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-I",
                            "-c",
                            ris.MODEL_PROBE_PROGRAM,
                            str(ris.GATE_DEADLINE_SECONDS),
                        ],
                        input=request,
                        capture_output=True,
                        timeout=ris.GATE_DEADLINE_SECONDS + 10,
                        cwd=home,
                        env={"PATH": os.environ.get("PATH", ""), "HOME": home},
                        check=False,
                    )
                except (OSError, subprocess.TimeoutExpired):
                    result = subprocess.CompletedProcess([], 124, b"", b"")
            self.ctx.rt.scan.scan("children", result.stdout + result.stderr)
            model = ris.model_probe_result(result.returncode, result.stdout)
            self.observe("10", "probe.model", model["ok"])
            self.observe("10", "probe.model_outcome", model)
            self.forge = pp.Forge(
                pp.Shell(
                    env={"PATH": os.environ.get("PATH", "")},
                    scrubber=self.ctx.rt.scrubber,
                    scan=self.ctx.rt.scan,
                    cwd=Path(self.private_dir),
                ),
                pp.forge_http(self.ctx.rt.scan),
                live.app_dir,
                live.app,
                self.ctx.rt.scrubber,
                pp.Clock(),
            )
            try:
                repository = self.forge.call("GET", self.forge.repo_path(""))
                app_ok = isinstance(repository, dict) and bool(repository.get("id"))
            except Exception:  # noqa: BLE001 - any failure is a failed probe
                app_ok = False
            self.observe("10", "probe.app", app_ok)
            for who, token in (("owner", live.owner_token), ("second", live.second_token)):
                status, user = self.github(token, "GET", "/user")
                login = user.get("login") if isinstance(user, dict) else None
                permission: Any = None
                permission_status = 0
                if app_ok and isinstance(login, str) and login:
                    try:
                        permission_status, permission = self.forge.request(
                            "GET",
                            self.forge.repo_path(
                                f"/collaborators/{urllib.parse.quote(login)}/permission"
                            ),
                        )
                    except Exception:  # noqa: BLE001 - any failure is a failed probe
                        permission_status = 0
                outcome = ris.user_probe_result(status, user, permission_status, permission)
                self.accounts[who] = outcome
                self.observe("10", f"probe.{who}", outcome["ok"])
            failed = [
                name
                for name, ok in (
                    ("model", model["ok"]),
                    ("app", app_ok),
                    ("owner", self.accounts["owner"]["ok"]),
                    ("second", self.accounts["second"]["ok"]),
                )
                if not ok
            ]
            if failed:
                raise ris.Refusal("live_probe_failed", ", ".join(failed))
            if self.accounts["owner"]["account_id"] == self.accounts["second"]["account_id"]:
                raise ris.Refusal("live_probe_failed", "the two accounts are the same")

        def live_links(self) -> None:
            self.observe(
                "10",
                "link.owner",
                self.link("owner", GITHUB_HOST, self.accounts["owner"]["account_id"]),
            )
            self.observe(
                "10",
                "link.second",
                self.link("other", GITHUB_HOST, self.accounts["second"]["account_id"]),
            )
            assert self.live_inputs is not None
            self.store_document("owner", "owner", conflict=self.live_inputs.conflict.preference)
            self.store_document("other", "second")

        def live_factory(self) -> None:
            assert self.live_inputs is not None
            repo = self.live_inputs.app["repo"]
            status, _ = self.admin(
                "POST",
                f"/agents/{self.agent_id}/channels",
                {"kind": "github", "address": repo, "trigger_label": self.label},
            )
            if status not in (200, 201):
                raise poe.StepFailed(f"the github channel binding was refused ({status})")
            status, _ = self.admin(
                "PATCH",
                f"/agents/{self.agent_id}",
                {
                    "publication_policy": "auto",
                    "publication_draft": False,
                    "publication_branch_prefix": self.branch_prefix,
                },
            )
            if status != 200:
                raise poe.StepFailed(f"the publication policy was refused ({status})")
            self.forge.call(
                "POST",
                self.forge.repo_path("/labels"),
                {"name": self.label, "color": "5319e7"},
                ok=(201,),
            )
            self.label_created = True
            self.stop_worker()
            self.start_worker()

        def sweep(self) -> None:
            super().sweep()
            if self.live_inputs is None or self.slot is None:
                return
            now = time.monotonic()
            for uid, state in self.pod_states.items():
                if now - self.last_scan.get(uid, 0.0) < SCAN_INTERVAL:
                    continue
                self.last_scan[uid] = now
                self.slot_scans.setdefault(self.slot, {})[uid] = self.scan_reviewers(
                    str(state["pod"])
                )

        def scan_reviewers(self, pod_name: str) -> ris.ReviewerScan:
            result = self.command(
                poe.kubectl_argv(
                    self.context,
                    self.namespace,
                    "exec",
                    "-i",
                    pod_name,
                    "--container",
                    "runner",
                    "--",
                    "python3",
                    "-I",
                    "-c",
                    ris.REVIEWER_SCAN_PROGRAM,
                    str(ris.GATE_DEADLINE_SECONDS),
                    *TRANSCRIPT_ROOTS,
                ),
                data=json.dumps(ris.needles_of(self.documents.values())).encode(),
                timeout=ris.GATE_DEADLINE_SECONDS + 15,
                check=False,
                retain_output=False,
            )
            return ris.reviewer_scan_result(result.returncode, result.stdout)

        def on_native_start(self, ref: str, now: float) -> None:
            if self.live_inputs is None:
                super().on_native_start(ref, now)
                return
            slot = self.slot
            pod_name = str(self.native[ref]["pod"])
            labeled = any(
                row["slot"] == slot and row["labeled_at"] is not None
                for row in self.ctx.ledger.slots()
            )
            launches = self.ctx.ledger.launches(slot) if slot is not None else 0
            if not ris.launch_permitted(
                slot, labeled=labeled, launches_so_far=launches, live_open=not self.fenced
            ):
                self.kill_cli(pod_name, ref)
                self.ctx.ledger.record_unreserved(ref, now)
                self.fence()
                raise poe.StepFailed("a native launch exceeded the slot's cap; starts are fenced")
            assert slot is not None
            self.ctx.ledger.count_launch(slot)
            pid, start = ref.split(":")[1:]
            self.pod_exec(pod_name, pod.LAUNCH_RELEASE_PROGRAM, pid, start)

        def live_slots(self) -> None:
            assert self.live_inputs is not None
            for slot, (actor, role) in ris.SLOT_PLAN.items():
                self.run_slot(slot, actor, role)
            slots = self.ctx.ledger.slots()
            self.observe("10", "spend.slots_within_cap", len(slots) <= ris.MAX_SLOTS)
            self.observe(
                "10",
                "spend.launches_within_cap",
                all(int(row["launches"]) <= ris.LAUNCHES_PER_SLOT for row in slots),
            )

        def run_slot(self, slot: int, actor: str, role: str | None) -> None:
            live = self.live_inputs
            assert live is not None
            if role is None:
                status, body = self.console(
                    "owner",
                    "DELETE",
                    f"/coding-instructions?expected_revision={self.revisions['owner']}",
                )
                self.observe(
                    "10", "document.owner_cleared", status == 200 and body.get("sha256") is None
                )
            path = f"proof-notes/rqp-{self.nonce}-slot{slot}.md"
            issue = self.forge.call(
                "POST",
                self.forge.repo_path("/issues"),
                {
                    "title": f"Requester preferences proof {self.nonce} slot {slot}",
                    "body": SLOT_ISSUE_BODY.format(slot=slot, path=path),
                },
                ok=(201,),
            )
            number = str(issue["number"])
            self.created_issues.append(number)
            self.ctx.ledger.record_slot(slot, actor, number, time.time())
            self.slot = slot
            token = live.owner_token if actor == "owner" else live.second_token
            status, _ = self.github(
                token,
                "POST",
                self.forge.repo_path(f"/issues/{number}/labels"),
                {"labels": [self.label]},
            )
            if status != 200:
                raise poe.StepFailed(f"slot {slot}: the label was refused ({status})")
            labeled_at = time.time()
            self.ctx.ledger.mark_labeled(slot, labeled_at)
            principal = self.principals["owner" if actor == "owner" else "other"]["id"]

            def work_item() -> str | None:
                rows = self.sql_rows(
                    "SELECT id FROM curie.work_items WHERE agent_id = :a AND tracker_issue_id = :i",
                    {"a": uuid.UUID(self.agent_id), "i": number},
                )
                return str(rows[0]["id"]) if rows else None

            item = self.wait(work_item, timeout=300, failure=f"slot {slot}: no WorkItem")

            def pulled() -> dict[str, Any] | None:
                owned = self.forge.owned_pulls(self.branch_prefix)
                fresh = [p for p in owned if int(p["number"]) not in self.created_pulls]
                if len(fresh) != 1:
                    return None
                requests = self.requests_of(item)
                if not requests or requests[-1]["status"] in poe.ACTIVE_REQUEST_STATUSES:
                    return None
                return dict(fresh[0])

            pull = self.wait_soft(pulled, timeout=live.slot_seconds)
            files: list[dict[str, Any]] = []
            body_text: str | None = None
            if pull is not None:
                self.created_pulls.append(int(pull["number"]))
                detail = self.forge.call("GET", self.forge.repo_path(f"/pulls/{pull['number']}"))
                body_text = detail.get("body") if isinstance(detail, dict) else None
                listed = self.forge.call(
                    "GET", self.forge.repo_path(f"/pulls/{pull['number']}/files?per_page=100")
                )
                files = [row for row in listed or [] if isinstance(row, dict)]
            self.sweep()
            self.slot = None
            row = self.work_item_row(item)
            recorded = next(r for r in self.ctx.ledger.slots() if r["slot"] == slot)
            document = self.documents["owner"] if role is None else self.documents[role]
            found = ris.SlotEvidence(
                recorded_before_label=recorded["recorded_at"] <= recorded["labeled_at"],
                requester_matches=str(row.get("requester_principal_id")) == principal,
                evidence=self.evidence_of(item),
                pull_request_found=pull is not None,
                body=body_text,
                files=tuple(files),
                reviewer=ris.combine_scans(self.slot_scans.get(slot, {}).values()),
            )
            observed = ris.slot_observations(
                slot,
                found,
                document=document,
                conflict_regex=live.conflict.forbidden_added_line_regex,
            )
            for key, value in observed.items():
                self.observe("10", key, value)
            if pull is not None:
                self.observe("10", f"slot{slot}.pull_number", int(pull["number"]))
            if pull is None:
                raise poe.StepFailed(f"slot {slot}: no pull request within the slot window")

        # --- forge cleanup ----------------------------------------------------

        def forge_step(self) -> None:
            if self.live_inputs is None:
                return
            now = time.time()
            if self.forge is None:
                for kind, ref, status in ris.forge_cleanup_rows(
                    issues={}, pulls={}, branches={}, label=False
                ):
                    self.ctx.ledger.record_cleanup(kind, ref, status, "", now)
                return
            branches: dict[str, bool | None] = {}
            with contextlib.suppress(Exception):
                for pull in self.forge.owned_pulls(self.branch_prefix):
                    number = int(pull["number"])
                    if number not in self.created_pulls:
                        self.created_pulls.append(number)
            for number in self.created_pulls:
                with contextlib.suppress(Exception):
                    self.forge.request(
                        "PATCH", self.forge.repo_path(f"/pulls/{number}"), {"state": "closed"}
                    )
            with contextlib.suppress(Exception):
                for ref in self.forge.owned_branches(self.branch_prefix):
                    name = ref.removeprefix("refs/heads/")
                    self.forge.request("DELETE", self.forge.repo_path(f"/git/refs/heads/{name}"))
                    branches[name] = None
            for issue in self.created_issues:
                with contextlib.suppress(Exception):
                    self.forge.request(
                        "PATCH", self.forge.repo_path(f"/issues/{issue}"), {"state": "closed"}
                    )
            if self.label_created:
                with contextlib.suppress(Exception):
                    self.forge.request(
                        "DELETE",
                        self.forge.repo_path(f"/labels/{urllib.parse.quote(self.label)}"),
                    )

            def state_of(path: str) -> str | None:
                try:
                    status, body = self.forge.request("GET", self.forge.repo_path(path))
                except Exception:  # noqa: BLE001 - unanswered reads count as present
                    return None
                return str(body.get("state")) if status == 200 and isinstance(body, dict) else None

            def present(path: str) -> bool | None:
                try:
                    status, _ = self.forge.request("GET", self.forge.repo_path(path))
                except Exception:  # noqa: BLE001 - unanswered reads count as present
                    return None
                return False if status == 404 else (True if status == 200 else None)

            rows = ris.forge_cleanup_rows(
                issues={n: state_of(f"/issues/{n}") for n in self.created_issues},
                pulls={str(n): state_of(f"/pulls/{n}") for n in self.created_pulls},
                branches={name: present(f"/git/ref/heads/{name}") for name in sorted(branches)},
                label=(
                    present(f"/labels/{urllib.parse.quote(self.label)}")
                    if self.label_created
                    else False
                ),
            )
            for kind, ref, status in rows:
                self.ctx.ledger.record_cleanup(kind, ref, status, "", now)
            self.forge.revoke()

        def register_teardown(self, finalizer: poe.Finalizer) -> None:
            if self.live_inputs is not None:

                def forge() -> None:
                    with pod.cleanup_time_limit(120):
                        self.forge_step()

                # Registered first, so it runs last: after the worker is fenced.
                finalizer.register("forge", forge)
            super().register_teardown(finalizer)

    return RequesterProof


def child_main(args: argparse.Namespace, rt: pod.Runtime) -> int:
    flags = pod.flags_of(args)
    inputs = poe.load_input_file(Path(args.input_file))
    live = LiveInputs(args) if args.mode == "run" else None
    ledger = ris.RequesterLedger.open(
        flags.state_dir, flags.run_id, create=True, scrub=rt.scrubber.scrub
    )
    ledger.begin(flags.candidate, "live" if live else "rehearse", time.time())
    for key, value in ris.caps().items():
        ledger.record("caps", key, value, time.time())
    # The harness's own mode stays "rehearse": it never reserves an owner session.
    ctx = pod.ChildContext("rehearse", flags, inputs, "", "", ledger, rt)

    def record(name: str, ok: bool, detail: str) -> None:
        ledger.record_cleanup("step", name, "ok" if ok else "failed", detail, time.time())

    finalizer = poe.Finalizer(record)
    outcome, detail = "failed", ""
    proof: Any = None
    try:
        with pod.signals_to_exit():
            enrollment = pod.load_enrollment()
            proof = requester_proof_class(enrollment)(ctx, live)
            proof.register_teardown(finalizer)
            proof.execute()
        outcome = "completed"
    except (poe.StepFailed, pps.Refusal) as failure:
        detail = str(failure.payload() if isinstance(failure, pps.Refusal) else failure)
    except SystemExit as stop:
        detail = f"interrupted ({stop.code})"
    except AssertionError as failure:
        detail = f"{failure} [{poe.failure_site(failure, REPO_ROOT)}]"
    except BaseException as failure:  # noqa: BLE001 - names only, never the message
        detail = poe.failure_site(failure, REPO_ROOT)
        if proof is not None:
            with contextlib.suppress(Exception):
                proof.retain_failure(failure)
    finally:
        with pod.signals_deferred():
            ledger.mark_cleanup(started=time.time())
            if proof is not None:
                proof.cleanup_budget = poe.CleanupBudget(time.monotonic())
            results = finalizer.run()
            if proof is not None:
                proof.stop_watchdog()
            ledger.mark_cleanup(finished=time.time())
            ledger.finish(outcome, time.time())
    summary = {
        "run_id": flags.run_id,
        "mode": "live" if live else "rehearse",
        "outcome": outcome,
        "detail": rt.scrubber.scrub(detail),
        "caps": ris.caps(),
        "slots": [
            {key: row[key] for key in ("slot", "actor", "launches")} for row in ledger.slots()
        ],
        "cleanup": [{"step": name, "ok": ok} for name, ok in results],
        "scan": rt.scan.as_json(),
    }
    ledger.close()
    rt.out.emit(summary)
    for held in rt.scrubber.held():
        held.clear()
    return (
        poe.EXIT_PASS if outcome == "completed" and all(ok for _, ok in results) else poe.EXIT_FAIL
    )


COMMANDS: dict[str, Callable[[argparse.Namespace, pod.Runtime], int]] = {
    "preflight": cmd_preflight,
    "rehearse": cmd_rehearse,
    "run": cmd_run,
    "judge": cmd_judge,
    CHILD: child_main,
}


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    environ = dict(os.environ if environ is None else environ)
    scrubber = pps.Scrubber()
    scan = pps.TokenScan(scrubber, ("stdout", "stderr", "children", "http"))
    rt = pod.Runtime(environ, pod.Output(scrubber, scan), scrubber, scan)
    try:
        args = build_parser().parse_args(argv)
        if args.command != "judge":
            pps.refuse_ambient_credentials(environ)
        return COMMANDS[args.command](args, rt)
    except pps.Refusal as refusal:
        payload = (
            refusal.payload()
            if isinstance(refusal, poe.Refusal)
            else ris.Refusal(refusal.code, refusal.detail).payload()
        )
        rt.out.emit(payload)
        return poe.EXIT_REFUSED
    except poe.StepFailed as failure:
        rt.out.emit({"error": "step_failed", "detail": str(failure)})
        return poe.EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())

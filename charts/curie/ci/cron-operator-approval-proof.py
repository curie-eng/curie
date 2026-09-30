#!/usr/bin/env python3
"""Prove a scheduled cron gate with an operator on an isolated cluster release.

The caller owns the installed release, its namespace, and the model credential.
This script owns one agent and one receipt fixture and removes both on exit.
It waits for the worker scheduler; it never invokes a manual hook fire.

Required environment:

CURIE_CRON_APPROVAL_CONTEXT       k8 or kind-test-2940
CURIE_NAMESPACE                   test-2940
CURIE_RELEASE                     test-2940
CURIE_BIN                         Candidate curie binary
CURIE_API_URL                     Reachable installed API URL
CURIE_API_KEY                     Platform API key
CURIE_CRON_APPROVAL_RECEIPT_IMAGE Imported MCP receipt fixture image

Build and import cron-approval-receipt.Dockerfile before running this proof.
The release must disable the fake model, supply a real model credential, omit
the dispatcher and Slack app token, and route Slack API calls to the isolated
receipt fixture with a placeholder bot token.
"""

from __future__ import annotations

import hashlib
import json
import os
import runpy
import sys
import tempfile
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

_shared = runpy.run_path(str(Path(__file__).with_name("hook-approval-proof.py")))
BaseProof = _shared["Proof"]
ProofError = _shared["ProofError"]
run = _shared["run"]
parse_json = _shared["parse_json"]
required_env = _shared["required_env"]

AGENT = "cron-approval-2940"
HOOK = "scheduled-receipt"
FIXTURE = "cron-approval-2940-receipt"
CHANNEL = "C0EXAMPLE1"
OPERATOR = "U0EXAMPLE1"
RECEIPT_TOOL = "mcp__plugin_hook-approval-proof_receipt__receipt_read"
OFFLINE_ENDPOINT = f"http://{FIXTURE}:8000"


class CronProof(BaseProof):
    def __init__(self) -> None:
        self.context = required_env("CURIE_CRON_APPROVAL_CONTEXT")
        self.namespace = required_env("CURIE_NAMESPACE")
        self.release = required_env("CURIE_RELEASE")
        self.curie_bin = Path(required_env("CURIE_BIN")).expanduser().resolve()
        self.api_url = required_env("CURIE_API_URL").rstrip("/")
        self.api_key = required_env("CURIE_API_KEY")
        self.receipt_image = required_env("CURIE_CRON_APPROVAL_RECEIPT_IMAGE")
        self.model = os.environ.get("CURIE_CRON_APPROVAL_MODEL", "z-ai/glm-5.2").strip()
        try:
            self.timeout = int(os.environ.get("CURIE_CRON_APPROVAL_TIMEOUT_SECONDS", "600"))
        except ValueError as exc:
            raise ProofError("CURIE_CRON_APPROVAL_TIMEOUT_SECONDS must be an integer") from exc
        if self.context not in {"k8", "kind-test-2940"}:
            raise ProofError("proof context must be k8 or kind-test-2940")
        if self.namespace != "test-2940" or self.release != "test-2940":
            raise ProofError("proof namespace and release must both be test-2940")
        if self.timeout < 120 or self.timeout > 1800:
            raise ProofError("proof timeout must be from 120 through 1800 seconds")
        if not self.model:
            raise ProofError("CURIE_CRON_APPROVAL_MODEL must not be empty")
        if not self.curie_bin.is_file() or not os.access(self.curie_bin, os.X_OK):
            raise ProofError("CURIE_BIN must name an executable file")
        url = urllib.parse.urlsplit(self.api_url)
        if url.scheme not in {"http", "https"} or not url.netloc:
            raise ProofError("CURIE_API_URL must be an HTTP or HTTPS URL")
        self.fixture_started = False
        self.agent_create_attempted = False
        self.agent_id = ""
        self.app_name = ""
        self.fixture_pod = ""
        self.thread_key = ""
        self.platform_images: dict[str, str] = {}
        self.failure_observation: dict[str, Any] = {}
        self.cli_env = os.environ.copy()
        self.cli_env.update(
            CURIE_API_URL=self.api_url,
            CURIE_API_KEY=self.api_key,
            CURIE_NAMESPACE=self.namespace,
        )

    def preflight(self) -> None:
        self.kubectl(["get", "namespace", self.namespace])
        for kind, name in (
            ("deployment", f"{self.release}-api"),
            ("deployment", f"{self.release}-worker"),
            ("statefulset", f"{self.release}-postgres"),
            ("statefulset", f"{self.release}-valkey"),
        ):
            self.kubectl(["get", kind, name])
        dispatchers = self.kubectl_json(
            [
                "get",
                "deployments",
                "-l",
                f"app.kubernetes.io/instance={self.release},app.kubernetes.io/component=dispatcher",
            ]
        )
        if dispatchers.get("items"):
            raise ProofError("proof release must not run a dispatcher")
        worker = self.kubectl_json(["get", "deployment", f"{self.release}-worker"])
        self.app_name = str(
            worker.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name", "")
        )
        containers = worker["spec"]["template"]["spec"]["containers"]
        worker_container = next((item for item in containers if item.get("name") == "worker"), None)
        if not self.app_name or worker_container is None:
            raise ProofError("worker deployment shape is incomplete")
        env = {item.get("name"): item for item in worker_container.get("env", [])}
        if env.get("CURIE_FAKE_MODEL", {}).get("value") != "0":
            raise ProofError("proof release must disable the fake model")
        if not env.get("CURIE_CREDENTIALS", {}).get("valueFrom", {}).get("secretKeyRef"):
            raise ProofError("proof release must reference model credentials")
        if env.get("CURIE_SLACK_TRUSTED_ORIGINS", {}).get("value") != OFFLINE_ENDPOINT:
            raise ProofError("proof release must trust only the offline Slack endpoint")
        if env.get("SLACK_API_BASE_URL", {}).get("value") != OFFLINE_ENDPOINT:
            raise ProofError("proof release must route Slack only to the receipt fixture")
        fake_token = self.kubectl(
            [
                "exec",
                f"deployment/{self.release}-worker",
                "-c",
                "worker",
                "--",
                "sh",
                "-c",
                'test -n "$SLACK_BOT_TOKEN" && test -z "$SLACK_APP_TOKEN"',
            ],
            check=False,
        )
        if fake_token.returncode != 0:
            raise ProofError("proof release needs only a fake Slack bot token")
        api = self.kubectl_json(["get", "deployment", f"{self.release}-api"])
        api_container = next(
            (
                item
                for item in api["spec"]["template"]["spec"]["containers"]
                if item.get("name") == "api"
            ),
            None,
        )
        runner_image = env.get("CURIE_RUNNER_IMAGE", {}).get("value")
        if api_container is None or not runner_image:
            raise ProofError("candidate API or runner image is missing")
        self.platform_images = {
            "api": str(api_container.get("image", "")),
            "worker": str(worker_container.get("image", "")),
            "runner": str(runner_image),
        }
        if not all(self.platform_images.values()):
            raise ProofError("candidate image reference is empty")
        for kind, name in (
            ("deployment", FIXTURE),
            ("service", FIXTURE),
            ("networkpolicy", f"{FIXTURE}-ingress"),
            ("networkpolicy", f"{FIXTURE}-runner-egress"),
        ):
            if self.kubectl(["get", kind, name], check=False).returncode == 0:
                raise ProofError(f"owned fixture object already exists: {kind}/{name}")
        _, agents = self.request("GET", "/agents")
        if any(item.get("name") == AGENT for item in agents):
            raise ProofError("owned proof agent already exists")

    def fixture_manifest(self) -> dict[str, Any]:
        # Keep the network isolation and receipt pod shape in the signed hook rig.
        manifest = super().fixture_manifest()
        old = "hook-approval-2765-receipt"

        def rename(value: Any) -> Any:
            if isinstance(value, str):
                return value.replace(old, FIXTURE).replace("issue-2765", "issue-2940")
            if isinstance(value, list):
                return [rename(item) for item in value]
            if isinstance(value, dict):
                return {key: rename(item) for key, item in value.items()}
            return value

        manifest = rename(manifest)
        ingress = next(
            item
            for item in manifest["items"]
            if item["kind"] == "NetworkPolicy" and item["metadata"]["name"] == f"{FIXTURE}-ingress"
        )
        ingress["spec"]["ingress"][0]["from"].append(
            {
                "podSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": self.app_name,
                        "app.kubernetes.io/instance": self.release,
                        "app.kubernetes.io/component": "worker",
                    }
                }
            }
        )
        return manifest

    def start_fixture(self) -> None:
        self.fixture_started = True
        self.kubectl(
            ["apply", "-f", "-"], stdin=json.dumps(self.fixture_manifest(), separators=(",", ":"))
        )
        self.kubectl(["rollout", "status", f"deployment/{FIXTURE}", "--timeout=120s"])
        pods = self.kubectl_json(["get", "pods", "-l", f"app.kubernetes.io/name={FIXTURE}"]).get(
            "items", []
        )
        if len(pods) != 1:
            raise ProofError("receipt fixture must have exactly one pod")
        self.fixture_pod = pods[0]["metadata"]["name"]
        if self.receipt_count() != 0 or self.card_count() != 0:
            raise ProofError("receipt fixture did not start empty")

    def card_digests(self) -> list[str]:
        if not self.fixture_pod:
            return []
        logs = self.kubectl(["logs", self.fixture_pod, "-c", "receipt"]).stdout
        prefix = "SLACK_APPROVAL_CARD "
        return [line[len(prefix) :] for line in logs.splitlines() if line.startswith(prefix)]

    def card_count(self) -> int:
        return len(self.card_digests())

    def write_bundle(self, root: Path) -> None:
        super().write_bundle(root)
        plugin_path = root / ".claude-plugin" / "plugin.json"
        plugin = json.loads(plugin_path.read_text(encoding="utf-8"))
        plugin["description"] = "Prove a scheduled tool approval with an operator."
        plugin["triggers"] = [
            {
                "type": "cron",
                "name": HOOK,
                "schedule": "* * * * *",
                "timezone": "UTC",
                "target": CHANNEL,
                "prompt": (
                    f"Call {RECEIPT_TOOL}({{}}) now. "
                    "Do not write narrative or call another tool first."
                ),
            }
        ]
        plugin_path.write_text(json.dumps(plugin, indent=2) + "\n", encoding="utf-8")
        mcp_path = root / ".mcp.json"
        mcp = mcp_path.read_text(encoding="utf-8").replace("hook-approval-2765-receipt", FIXTURE)
        mcp_path.write_text(mcp, encoding="utf-8")

    def create_agent(self) -> None:
        self.agent_create_attempted = True
        _, agent = self.request(
            "POST",
            "/agents",
            expected={201},
            body={
                "name": AGENT,
                "channel": {
                    "kind": "slack",
                    "address": CHANNEL,
                    "endpoint": OFFLINE_ENDPOINT,
                    "adapter": "proof-offline",
                },
                "model": self.model,
                "approval_routes": {
                    "hook": {
                        "resolution": {"kind": "slack", "address": CHANNEL},
                        "approvers": {"users": [OPERATOR]},
                    }
                },
            },
        )
        self.agent_id = str(agent["id"])
        uuid.UUID(self.agent_id)
        if agent.get("model") != self.model:
            raise ProofError("agent did not retain the requested model")
        if agent.get("approval_routes", {}).get("hook", {}).get("approvers", {}).get("users") != [
            OPERATOR
        ]:
            raise ProofError("agent did not retain explicit approvers")

    def deploy_bundle(self, bundle: Path) -> None:
        result = self.cli_json(
            "deploy",
            [
                "--agent",
                AGENT,
                "--plugin-dir",
                str(bundle),
                "--slack-channel",
                CHANNEL,
                "--env",
                "dev",
                "--label",
                "issue-2940-proof",
            ],
        )
        if result.get("agent", {}).get("id") != self.agent_id:
            raise ProofError("deploy targeted another agent")
        if result.get("deployment", {}).get("status") != "active":
            raise ProofError("proof deployment is not active")

    def hook_rows(self) -> list[dict[str, Any]]:
        uuid.UUID(self.agent_id)
        sql = (
            "SELECT coalesce(json_agg(json_build_object("
            "'id', id, 'slot', to_char(slot_utc AT TIME ZONE 'UTC', "
            "'YYYY-MM-DD\"T\"HH24:MI:SS\"+00:00\"'), 'outcome', outcome, "
            "'ended_at', ended_at) ORDER BY slot_utc), '[]'::json) "
            "FROM curie.hook_runs "
            f"WHERE agent_id = '{self.agent_id}'::uuid AND name = '{HOOK}';"
        )
        result = self.kubectl(
            [
                "exec",
                f"statefulset/{self.release}-postgres",
                "-c",
                "postgres",
                "--",
                "sh",
                "-c",
                'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -U "$POSTGRES_USER" '
                '-d "$POSTGRES_DB" -tA -v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                sql,
            ]
        )
        rows = parse_json(result.stdout.strip(), "hook run SQL")
        if not isinstance(rows, list):
            raise ProofError("hook run SQL returned no list")
        return rows

    def schedule(self) -> dict[str, Any]:
        query = urllib.parse.urlencode({"agent": AGENT})
        _, data = self.request("GET", f"/schedules?{query}")
        entries = data.get("schedules", [])
        if len(entries) != 1 or entries[0].get("agent_id") != self.agent_id:
            raise ProofError("schedule API did not select the owned agent")
        if entries[0].get("bundle_error") is not None:
            raise ProofError("schedule API cannot read the deployed bundle")
        hooks = entries[0].get("hooks", [])
        if (
            len(hooks) != 1
            or hooks[0].get("name") != HOOK
            or hooks[0].get("schedule") != "* * * * *"
        ):
            raise ProofError("schedule API did not expose the deployed cron hook")
        return hooks[0]

    def list_pending_cli(self) -> dict[str, Any]:
        result = self.cli_json("approvals", [AGENT, "--list"])
        if result.get("truncated") is not False:
            raise ProofError("approval list was truncated")
        return result

    def mint_operator(self) -> str:
        result = self.cli_json("approvals", [AGENT, "--mint-operator-principal", OPERATOR])
        principal = result.get("operator_principal", {})
        token = principal.get("token")
        if principal.get("subject") != OPERATOR or not isinstance(token, str) or not token:
            raise ProofError("operator principal was not minted for the expected subject")
        return token

    def resolve_cli(self, approval_id: str, token: str) -> dict[str, Any]:
        env = self.cli_env.copy()
        env["CURIE_APPROVAL_PRINCIPAL_TOKEN"] = token
        command = [
            str(self.curie_bin),
            "--json",
            "cluster",
            "--context",
            self.context,
            "approvals",
            AGENT,
            "--resolve",
            approval_id,
            "--namespace",
            self.namespace,
            "--release",
            self.release,
        ]
        return parse_json(run(command, env=env).stdout, "curie approvals resolve")

    def run_proof(self) -> dict[str, Any]:
        self.preflight()
        self.start_fixture()
        with tempfile.TemporaryDirectory(prefix="curie-cron-approval-") as directory:
            os.chmod(directory, 0o700)
            bundle = Path(directory) / "bundle"
            bundle.mkdir()
            self.write_bundle(bundle)
            self.create_agent()
            self.deploy_bundle(bundle)
            self.schedule()
            deadline = time.monotonic() + self.timeout

            def observe_pending() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
                rows = self.hook_rows()
                ran_rows = [row for row in rows if row.get("outcome") == "ran"]
                approvals = self.list_agent_approvals()
                pending = [
                    item
                    for item in approvals
                    if item.get("status") == "pending" and item.get("author") == f"cron:{HOOK}"
                ]
                self.failure_observation = {
                    "hook_outcomes": [row.get("outcome") for row in rows],
                    "approval_statuses": [item.get("status") for item in approvals],
                    "receipt_count": self.receipt_count(),
                }
                # The kernel closes the cron run as ran when it suspends at
                # the approval gate; the resumed turn is a separate delivery.
                if len(ran_rows) != 1 or len(pending) != 1:
                    return None
                return ran_rows[0], pending[0], self.schedule()

            hook_row, approval, observed_schedule = self.wait_until(
                "one scheduler fire awaiting approval", deadline, observe_pending
            )
            _, paused = self.request("POST", f"/schedules/{AGENT}/{HOOK}/pause")
            if paused.get("paused") is not True:
                raise ProofError("could not pause later cron slots during the proof")
            if observed_schedule.get("last_fire_at") is None:
                raise ProofError("schedule API did not record the cron fire")
            event_id = f"cron:{self.agent_id}:{HOOK}:{hook_row['slot']}"
            original = self.stream_events(event_id)
            if len(original) != 1:
                raise ProofError("scheduler did not enqueue exactly one owned cron event")
            queued = original[0][1]
            if queued.get("source") != "cron" or queued.get("author") != f"cron:{HOOK}":
                raise ProofError("queued event is not the scheduler cron turn")
            if queued.get("hook_run", {}).get("name") != HOOK:
                raise ProofError("queued event is missing the owned hook run")
            handle = queued.get("reply_handle", {})
            if handle.get("endpoint") != OFFLINE_ENDPOINT or handle.get("channel") != CHANNEL:
                raise ProofError("cron event did not retain the offline target binding")
            conversation_id = str(queued.get("conversation_id", ""))
            if not conversation_id:
                raise ProofError("cron event has no conversation identity")
            self.thread_key = _shared["_thread_key"](conversation_id)
            if approval.get("conversation_id") != conversation_id:
                raise ProofError("pending approval belongs to another conversation")
            if approval.get("route") != "hook" or approval.get("gate_kind") != "permission":
                raise ProofError("cron turn did not reach the declared permission gate")
            if approval.get("granted_tool") != RECEIPT_TOOL:
                raise ProofError("approval is for another tool")
            approval_id = str(approval["id"])
            uuid.UUID(approval_id)
            listing = self.list_pending_cli()
            if listing.get("count") != 1 or len(listing.get("pending", [])) != 1:
                raise ProofError("cluster approvals did not list exactly one pending row")
            listed = listing["pending"][0]
            if listed.get("id") != approval_id or listed.get("route") != "hook":
                raise ProofError("pending CLI approval did not expose the owned route")
            if listed.get("current_route_approvers") != {"users": [OPERATOR]}:
                raise ProofError("pending CLI approval did not list the current approver")
            routes = self.cli_json("approvals", [AGENT, "--list-routes"])
            if routes.get("routes", {}).get("hook", {}).get("approvers", {}).get("users") != [
                OPERATOR
            ]:
                raise ProofError("pending route did not name the explicit approver")
            expected_card_digest = hashlib.sha256(approval_id.encode()).hexdigest()
            if self.card_digests() != [expected_card_digest]:
                raise ProofError("cron approval card did not point to its pending approval")
            if self.receipt_count() != 0 or self.audit(approval_id):
                raise ProofError("gated tool ran or approval was audited before resolution")
            if self.sql_resumed(approval_id) != "null":
                raise ProofError("pending approval was already resumed")
            initial_turns = self.transcript(conversation_id)
            if (
                initial_turns is None
                or len(initial_turns) != 1
                or initial_turns[0].get("status") != "awaiting-approval"
            ):
                raise ProofError("cron transcript did not suspend at the gate")
            token = self.mint_operator()
            resolved = self.resolve_cli(approval_id, token).get("resolved", {})
            if resolved.get("id") != approval_id or resolved.get("status") != "approved":
                raise ProofError("operator CLI did not approve the pending row")
            token = ""

            def observe_final() -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
                rows = self.hook_rows()
                owned = [row for row in rows if row.get("id") == hook_row.get("id")]
                turns = self.transcript(conversation_id)
                count = self.receipt_count()
                self.failure_observation = {
                    "hook_outcomes": [row.get("outcome") for row in rows],
                    "approval_statuses": [
                        item.get("status") for item in self.list_agent_approvals()
                    ],
                    "receipt_count": count,
                    "transcript_statuses": [turn.get("status") for turn in turns or []],
                }
                if (
                    len(owned) != 1
                    or owned[0].get("outcome") != "ran"
                    or turns is None
                    or len(turns) != 2
                    or count != 1
                ):
                    return None
                return owned, turns

            owned, final_turns = self.wait_until("one resumed cron run", deadline, observe_final)
            if owned[0].get("ended_at") is None:
                raise ProofError("ran hook has no terminal timestamp")
            if final_turns[0] != initial_turns[0] or final_turns[1].get("status") != "done":
                raise ProofError("approval resume did not append one completed turn")
            if self.sql_resumed(approval_id) != "set":
                raise ProofError("approved row has no resume timestamp")
            audits = self.audit(approval_id)
            if len(audits) != 1 or audits[0].get("action") != "resolved":
                raise ProofError("approval audit has the wrong action sequence")
            if audits[0].get("actor") != OPERATOR or audits[0].get("principal_kind") != "operator":
                raise ProofError("approval audit did not record the operator")
            if self.receipt_count() != 1:
                raise ProofError("gated tool executed more than once")
            if self.card_digests() != [expected_card_digest]:
                raise ProofError("cron approval card changed after resolution")
            return {
                "status": "passed",
                "agent": AGENT,
                "hook": HOOK,
                "candidate_cli_sha256": hashlib.sha256(self.curie_bin.read_bytes()).hexdigest(),
                "platform_images": self.platform_images,
                "receipt_image": self.receipt_image,
                "slot": hook_row["slot"],
                "event_id": event_id,
                "approval_id": approval_id,
                "pending_approvers": [OPERATOR],
                "audit_principal_kind": "operator",
                "hook_outcome": "ran",
                "receipt_calls": 1,
                "approval_cards": 1,
                "dispatcher_count": 0,
                "slack_app_token_present": False,
            }

    def cleanup(self) -> list[str]:
        failures: list[str] = []
        if self.agent_create_attempted:
            try:
                _, agents = self.request("GET", "/agents")
                if any(item.get("name") == AGENT for item in agents):
                    if self.thread_key:
                        try:
                            reset = self.cli_json(
                                "reset-thread", [AGENT, "--thread-key", self.thread_key, "--yes"]
                            )
                            if reset.get("released") is not True:
                                failures.append("owned thread")
                        except Exception:
                            failures.append("owned thread")
                    self.cli_json("delete", [AGENT, "--yes"])
            except Exception:
                failures.append(f"agent/{AGENT}")
        if self.fixture_started:
            objects = [
                f"deployment/{FIXTURE}",
                f"service/{FIXTURE}",
                f"networkpolicy/{FIXTURE}-ingress",
                f"networkpolicy/{FIXTURE}-runner-egress",
            ]
            deleted = self.kubectl(
                ["delete", *objects, "--ignore-not-found=true", "--wait=true", "--timeout=60s"],
                check=False,
            )
            if deleted.returncode != 0:
                failures.extend(objects)
            for obj in objects:
                if self.kubectl(["get", obj], check=False).returncode == 0:
                    failures.append(obj)
        return failures


def main() -> int:
    proof: CronProof | None = None
    outcome: dict[str, Any] | None = None
    error: Exception | None = None
    cleanup_failures: list[str] = []
    try:
        proof = CronProof()
        outcome = proof.run_proof()
    except Exception as exc:
        error = exc
    finally:
        if proof is not None:
            try:
                cleanup_failures = proof.cleanup()
            except Exception:
                cleanup_failures.append("owned cleanup raised")
    if error is not None:
        print(f"cron approval proof failed: {error}", file=sys.stderr)
        if proof is not None and proof.failure_observation:
            print(
                "cron approval proof observation: "
                + json.dumps(proof.failure_observation, sort_keys=True),
                file=sys.stderr,
            )
    if cleanup_failures:
        print("cleanup failed for owned resources: " + ", ".join(cleanup_failures), file=sys.stderr)
    if error is not None or cleanup_failures:
        return 1
    assert outcome is not None
    print(json.dumps(outcome, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

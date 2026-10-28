"""External providers for the executable dynamic-base factory scenario.

Only GitHub and the model are scripted. Git objects and smart HTTP writes are
real. The candidate API, dispatch, worker, workspace, runner and chart run
unchanged in the owned cluster supplied by ``AdmittedKind``.
"""

from __future__ import annotations

import io
import json
import os
import re
import socket
import sqlite3
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import scripted_kind as sk
from aci_protocol import RUNS_STREAM_DEFAULT, STREAM_PAYLOAD_FIELD
from c4_admitted_http import tracker_stub_type
from c4_admitted_kind import REPOSITORY, AdmittedKind, Ticket
from channel_protocol.work_item_events import ci_event_id, ci_round_key
from curie_internal.keyspace import (
    WORK_ITEM_CI_RERUN_PREFIX,
    WORKER_KEY_PREFIX_DEFAULT,
    completion_key,
    done_key,
)

CHANGE = "dynamic_change.py"
SETTLE_SECONDS = 15
TERMINAL = {"completed", "failed", "cancelled", "expired"}


def dynamic_stub_type(module: Any, *, redact: Any) -> type:
    class DynamicGitHub(tracker_stub_type(module, redact=redact)):
        def start(self) -> None:
            super().start()
            with sqlite3.connect(self._database) as db:
                db.executescript(
                    "CREATE TABLE dynamic_controls (name TEXT PRIMARY KEY, value TEXT);"
                    "CREATE TABLE dynamic_events (id INTEGER PRIMARY KEY, kind TEXT, "
                    "head TEXT, target TEXT, detail TEXT);"
                    "CREATE TABLE dynamic_checks (head TEXT, name TEXT, state TEXT, "
                    "target TEXT, started TEXT, completed TEXT, PRIMARY KEY(head,name));"
                )
            declaration = {
                "checks": [
                    {
                        "id": "unit",
                        "paths": ["*.py", "**/*.py"],
                        "command": [
                            "python3",
                            "-m",
                            "unittest",
                            "discover",
                            "-s",
                            "unitconv/tests",
                        ],
                    }
                ]
            }
            self.commit_branch(
                "main",
                {
                    CHANGE: 'VALUE = "base"\n',
                    "dynamic-target.txt": "main\n",
                    ".curie/verification.json": json.dumps(declaration),
                },
            )
            self.commit_branch("next", {"dynamic-target.txt": "next\n"}, parent="main")
            self.commit_branch(
                "release/blocked",
                {
                    CHANGE: 'VALUE = "target"\n',
                    "dynamic-target.txt": "blocked\n",
                },
                parent="main",
            )

        def control(self, name: str, value: str | None = None) -> str:
            with sqlite3.connect(self._database) as db:
                if value is not None:
                    db.execute(
                        "INSERT OR REPLACE INTO dynamic_controls VALUES (?,?)", (name, value)
                    )
                row = db.execute(
                    "SELECT value FROM dynamic_controls WHERE name=?", (name,)
                ).fetchone()
            return row[0] if row else ""

        def event(self, kind: str, head: str, target: str, detail: str = "") -> None:
            with sqlite3.connect(self._database) as db:
                db.execute(
                    "INSERT INTO dynamic_events(kind,head,target,detail) VALUES (?,?,?,?)",
                    (kind, head, target, detail),
                )

        def events(self, kind: str | None = None) -> list[dict[str, Any]]:
            with sqlite3.connect(self._database) as db:
                db.row_factory = sqlite3.Row
                rows = db.execute("SELECT * FROM dynamic_events ORDER BY id")
                return [dict(row) for row in rows if kind is None or row["kind"] == kind]

        def _json(self, handler: Any, status: int, body: Any) -> None:
            response = body
            if isinstance(body, dict) and "token" in body:
                response = {**body, "token": "[REDACTED]"}
            with self.provider_lock, sqlite3.connect(self._database) as db:
                db.execute(
                    "INSERT INTO provider_calls "
                    "(method, path, http_status, provider_clock, response) VALUES (?, ?, ?, ?, ?)",
                    (
                        handler.command,
                        redact(handler.path),
                        status,
                        module._utc(),
                        redact(json.dumps(response)),
                    ),
                )
            payload = json.dumps(body).encode()
            handler.send_response(status)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(payload)))
            path = urlsplit(handler.path).path.removeprefix("/api/v3")
            if status == 503 and re.fullmatch(
                r"/(?:repos/acme-corp/acme-bot|repositories/\d+)/actions/jobs/\d+", path
            ):
                handler.send_header("Retry-After", "30")
            handler.end_headers()
            handler.wfile.write(payload)

        def git(self, *args: str, **kwargs: Any) -> str:
            return (
                self._run("git", "--git-dir", str(self._repository), *args, **kwargs)
                .decode()
                .strip()
            )

        def commit_branch(
            self, branch: str, files: dict[str, str], *, parent: str | None = None
        ) -> str:
            with self.provider_lock:
                previous = self._ref(parent or branch)
                env = {**os.environ, "GIT_INDEX_FILE": str(self.root / "dynamic.index")}
                self.git("read-tree", previous, env=env)
                for path, content in files.items():
                    blob = self.git("hash-object", "-w", "--stdin", input_=content.encode())
                    self.git(
                        "update-index", "--add", "--cacheinfo", f"100644,{blob},{path}", env=env
                    )
                tree = self.git("write-tree", env=env)
                head = self.git(
                    "-c",
                    "user.name=Example Author",
                    "-c",
                    "user.email=author@example.com",
                    "commit-tree",
                    tree,
                    "-p",
                    previous,
                    input_=b"Prepare example base\n",
                )
                self.git("update-ref", f"refs/heads/{branch}", head)
                return head

        def heads(self) -> dict[str, str]:
            return dict(
                line.rsplit(" ", 1)
                for line in self.git(
                    "for-each-ref", "--format=%(refname:short) %(objectname)", "refs/heads"
                ).splitlines()
            )

        def check(
            self, head: str, state: str, *, name: str = "dynamic-ci", target: str | None = None
        ) -> None:
            """Change only the external CI verdict, never the repository head."""
            with sqlite3.connect(self._database) as db:
                old = db.execute(
                    "SELECT target FROM dynamic_checks WHERE head=? AND name=?", (head, name)
                ).fetchone()
                chosen = target or (old[0] if old else "main")
                now = module._utc()
                db.execute(
                    "INSERT OR REPLACE INTO dynamic_checks VALUES (?,?,?,?,?,?)",
                    (
                        head,
                        name,
                        state,
                        chosen,
                        None if state == "queued" else now,
                        now if state in {"success", "failure"} else None,
                    ),
                )

        def checks(self, head: str) -> list[dict[str, Any]]:
            with sqlite3.connect(self._database) as db:
                db.row_factory = sqlite3.Row
                return [
                    dict(row)
                    for row in db.execute(
                        "SELECT * FROM dynamic_checks WHERE head=? ORDER BY name", (head,)
                    )
                ]

        def _git_request(self, handler: Any, path: str, query: str, body: bytes) -> None:
            if not path.endswith("/git-receive-pack"):
                super()._git_request(handler, path, query, body)
                return
            with self.provider_lock:
                before = self.heads()

                class CapturedResponse:
                    def __init__(self) -> None:
                        self.wfile = io.BytesIO()
                        self.status = 200
                        self.response_headers: list[tuple[str, str]] = []

                    def __getattr__(self, name: str) -> Any:
                        return getattr(handler, name)

                    def send_response(self, status: int) -> None:
                        self.status = status

                    def send_header(self, name: str, value: str) -> None:
                        self.response_headers.append((name, value))

                    def end_headers(self) -> None:
                        return

                captured = CapturedResponse()
                super()._git_request(captured, path, query, body)
                changed = [
                    (branch, sha)
                    for branch, sha in self.heads().items()
                    if before.get(branch) != sha
                ]
                for branch, sha in changed:
                    pull = next((p for p in self._rows("pull") if p["head"]["ref"] == branch), None)
                    target = pull["base"]["ref"] if pull else "main"
                    self.event("push", sha, target, branch)
                    self.check(sha, "queued", target=target)
                if changed and self.control("lose_push") == "once":
                    self.control("lose_push", "")
                    self.event("lost_push_response", changed[0][1], "")
                    handler.close_connection = True
                    handler.connection.shutdown(socket.SHUT_RDWR)
                    return
                handler.send_response(captured.status)
                for name, value in captured.response_headers:
                    handler.send_header(name, value)
                handler.end_headers()
                handler.wfile.write(captured.wfile.getvalue())

        def _rest(self, method: str, path: str, query: Any, body: Any) -> Any:
            prefix = f"/repositories/{self._repository_row()['id']}"
            if path == prefix or path.startswith(prefix + "/"):
                path = f"/repos/{REPOSITORY}" + path[len(prefix) :]
            pull_match = re.fullmatch(r"/repos/acme-corp/acme-bot/pulls/(\d+)", path)
            if method == "PATCH" and pull_match and "base" in body:
                # GitHub's update-pull endpoint accepts the literal base branch
                # and returns the unchanged PR identity with refreshed base/head.
                # https://docs.github.com/en/rest/pulls/pulls#update-a-pull-request
                with self.provider_lock:
                    pull = self._get("pull", int(pull_match.group(1)))
                    if pull is None:
                        return 404, {"message": "Not Found"}
                    head = self._ref(pull["head"]["ref"])
                    target = body["base"]
                    if self._find_ref(target) is None:
                        return 422, {"message": "Invalid base"}
                    self.event("retarget_attempt", head, target)
                    if self.control("retarget") == "unavailable":
                        return 503, {"message": "Example provider outage"}
                    pull["base"] = {**pull["base"], "ref": target}
                    self._put("pull", pull["number"], pull)
                    self.event("retarget", head, target)
                    if self.control("retarget") == "lost_once":
                        self.control("retarget", "")
                        return 503, {"message": "Example ambiguous provider response"}
                    return 200, self._fresh_pull(pull)
            if method == "GET" and re.fullmatch(
                rf"/repos/{REPOSITORY}/actions/jobs/\d+", path
            ):
                # Actions job reads require Actions permission; this fixture App
                # has none. An outage remains a transient provider failure.
                # https://docs.github.com/en/rest/actions/workflow-jobs#get-a-job-for-a-workflow-run
                if self.control("actions_lookup") == "unavailable":
                    return 503, {"message": "Example provider outage"}
                return 403, {"message": "Resource not accessible by integration"}
            check_match = re.fullmatch(
                r"/repos/acme-corp/acme-bot/commits/([^/]+)/check-runs", path
            )
            if method == "GET" and check_match:
                # Check-run states and envelopes use the provider REST contract.
                # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
                head = check_match.group(1)
                with sqlite3.connect(self._database) as db:
                    db.row_factory = sqlite3.Row
                    records = list(
                        db.execute(
                            "SELECT * FROM dynamic_checks WHERE head=? ORDER BY name", (head,)
                        )
                    )
                checks = [
                    {
                        "id": 10000 + i,
                        "name": row["name"],
                        "head_sha": head,
                        "status": "completed"
                        if row["state"] in {"success", "failure"}
                        else row["state"],
                        "conclusion": row["state"]
                        if row["state"] in {"success", "failure"}
                        else None,
                        "started_at": row["started"],
                        "completed_at": row["completed"],
                        "html_url": (
                            f"{self.base_url}/{REPOSITORY}/runs/{10000 + i}"
                            "?check_suite_focus=true"
                        ),
                        "app": {"id": 51, "slug": "github-actions"},
                        "output": {"title": "Example CI", "summary": "Exercise dynamic base"},
                    }
                    for i, row in enumerate(records)
                ]
                self.event(
                    "ci_read",
                    head,
                    records[0]["target"] if records else "",
                    ",".join(row["state"] for row in records),
                )
                return 200, {"total_count": len(checks), "check_runs": checks}
            if method == "GET" and path.startswith(f"/repos/{REPOSITORY}/branches/"):
                # Branch protection contexts are embedded in the branch read.
                # https://docs.github.com/en/rest/branches/branches#get-a-branch
                answer = super()._rest(method, path, query, body)
                if answer is not None and answer[0] == 200:
                    answer[1]["protection"] = {
                        "required_status_checks": {"contexts": ["dynamic-ci"]}
                    }
                return answer
            return super()._rest(method, path, query, body)

    return DynamicGitHub


def dynamic_model_type(module: Any) -> type:
    class DynamicModel(module.ModelScript):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.database = Path(os.environ["TMPDIR"]) / "dynamic-model.sqlite3"
            with sqlite3.connect(self.database) as db:
                db.executescript(
                    "CREATE TABLE control (mode TEXT); INSERT INTO control VALUES ('publish');"
                    "CREATE TABLE observations "
                    "(tool TEXT PRIMARY KEY, head TEXT, target TEXT, value TEXT);"
                    "CREATE TABLE calls (id INTEGER PRIMARY KEY, mode TEXT, failed INTEGER, "
                    "observed_heads TEXT, tools TEXT, reply TEXT);"
                )
            self.database.chmod(0o600)

        def mode(self, value: str | None = None) -> str:
            with sqlite3.connect(self.database) as db:
                if value is not None:
                    db.execute("UPDATE control SET mode=?", (value,))
                return str(db.execute("SELECT mode FROM control").fetchone()[0])

        def observations(self) -> list[dict[str, Any]]:
            with sqlite3.connect(self.database) as db:
                db.row_factory = sqlite3.Row
                return [dict(row) for row in db.execute("SELECT * FROM observations")]

        def call_evidence(self) -> list[dict[str, Any]]:
            with sqlite3.connect(self.database) as db:
                db.row_factory = sqlite3.Row
                rows = db.execute("SELECT * FROM calls ORDER BY id DESC LIMIT 32")
                return list(reversed([dict(row) for row in rows]))

        def _match(self, method: str, path: str, body: bytes) -> dict[str, Any] | None:
            if method != "POST" or path != "/v1/messages":
                return None
            request = json.loads(body)
            messages = request.get("messages", [])
            objective_start = 0
            for index, message in enumerate(messages):
                if message.get("role") != "user":
                    continue
                content = message.get("content")
                if isinstance(content, str):
                    objective_start = index
                elif (
                    isinstance(content, list)
                    and any(
                        isinstance(block, dict) and block.get("type") == "text"
                        for block in content
                    )
                    and not any(
                        isinstance(block, dict) and block.get("type") == "tool_result"
                        for block in content
                    )
                ):
                    objective_start = index
            messages = messages[objective_start:]
            tools = [
                block
                for message in messages
                if message.get("role") == "assistant"
                for block in message.get("content", [])
                if isinstance(block, dict) and block.get("type") == "tool_use"
            ]
            issued = {tool["id"] for tool in tools if tool["name"] == "Bash"}
            seen: list[dict[str, str]] = []
            failed = False
            for message in messages:
                if message.get("role") != "user" or not isinstance(message.get("content"), list):
                    continue
                for block in message["content"]:
                    if (
                        isinstance(block, dict)
                        and block.get("type") == "tool_result"
                        and block.get("is_error") is True
                    ):
                        failed = True
                    if (
                        not isinstance(block, dict)
                        or block.get("type") != "tool_result"
                        or block.get("tool_use_id") not in issued
                        or block.get("is_error") is not False
                    ):
                        continue
                    output = block.get("content")
                    match = (
                        re.search(
                            r"C4_HEAD=([a-f0-9]{40})\nC4_TARGET=([^\n]+)\nC4_VALUE=([^\n]+)", output
                        )
                        if isinstance(output, str)
                        else None
                    )
                    if match:
                        observation = dict(
                            zip(("head", "target", "value"), match.groups(), strict=True)
                        )
                        seen.append(observation)
                        with sqlite3.connect(self.database) as db:
                            db.execute(
                                "INSERT OR IGNORE INTO observations VALUES (?,?,?,?)",
                                (
                                    block["tool_use_id"],
                                    observation["head"],
                                    observation["target"],
                                    observation["value"],
                                ),
                            )
            mode = self.mode()
            identifier = "toolu_dynamic_" + uuid.uuid4().hex
            if failed:
                content = [
                    {
                        "type": "text",
                        "text": "Could not complete: an actual repository tool failed.",
                    }
                ]
            elif not seen:
                content = [
                    {
                        "type": "tool_use",
                        "id": identifier,
                        "name": "Bash",
                        "input": {
                            "command": "python3 -m unittest discover -s unitconv/tests && "
                            "printf 'C4_HEAD=%s\\nC4_TARGET=%s\\nC4_VALUE=%s\\n' "
                            '"$(git rev-parse HEAD)" "$(cat dynamic-target.txt)" '
                            '"$(cat dynamic_change.py)"',
                        },
                    }
                ]
            elif mode == "hold":
                content = [
                    {
                        "type": "tool_use",
                        "id": identifier,
                        "name": "Bash",
                        "input": {
                            "command": "sleep 300",
                            "timeout": 310000,
                        },
                    }
                ]
            elif mode == "nochange":
                content = [
                    {
                        "type": "text",
                        "text": (
                            "No changes needed: verified the current repository "
                            "on its selected base."
                        ),
                    }
                ]
            elif not any(tool["id"].startswith("toolu_dynamic_edit_") for tool in tools):
                value = "factory-fixed" if mode == "fix" else "factory"
                content = [
                    {
                        "type": "tool_use",
                        "id": "toolu_dynamic_edit_" + uuid.uuid4().hex,
                        "name": "Bash",
                        "input": {
                            "command": f"printf 'VALUE = \"{value}\"\\n' > {CHANGE} && "
                            "python3 -m unittest discover -s unitconv/tests",
                        },
                    }
                ]
            elif not any(tool["name"] == "mcp__curie__publish_changes" for tool in tools):
                content = [
                    {
                        "type": "tool_use",
                        "id": identifier,
                        "name": "mcp__curie__publish_changes",
                        "input": {
                            "title": "Exercise dynamic factory base selection",
                            "body": "Verified the example repository.",
                        },
                    }
                ]
            else:
                content = [{"type": "text", "text": "Published the verified example change."}]
            with sqlite3.connect(self.database) as db:
                db.execute(
                    "INSERT INTO calls (mode, failed, observed_heads, tools, reply) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        mode,
                        int(failed),
                        json.dumps([observation["head"] for observation in seen][-8:]),
                        json.dumps([tool["name"] for tool in tools][-16:]),
                        json.dumps(content),
                    ),
                )
            return {
                "message": {
                    "id": "msg_dynamic_" + uuid.uuid4().hex,
                    "type": "message",
                    "role": "assistant",
                    "model": "scripted",
                    "content": content,
                    "stop_reason": "tool_use" if content[0]["type"] == "tool_use" else "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 10},
                }
            }

    return DynamicModel


class DynamicKind(AdmittedKind):
    def values(self, original: Any, **kwargs: Any) -> dict[str, Any]:
        values = super().values(original, **kwargs)
        factory = values["factory"]
        factory["baseLabelSettleSeconds"] = SETTLE_SECONDS
        factory["trackers"][0]["pollIntervalSeconds"] = 1
        repositories = [
            repository
            for binding in factory["bindings"]
            for repository in binding["repos"]
            if repository["codeHost"] == "github" and repository["path"] == REPOSITORY
        ]
        assert repositories, "the admitted-agent fixture has no matching GitHub repository policy"
        for repository in repositories:
            repository.update(
                bases=["*"],
                default_base="main",
                ci={"required": {"key": "dynamic-ci", "paths": [CHANGE]}},
            )
        return values

    def item(self, ticket: Ticket) -> dict[str, Any]:
        return self.sql(
            "SELECT row_to_json(w) FROM curie.work_items w "
            f"WHERE tracker_kind='github' AND tracker_issue_id='{int(ticket.number)}'"
        )

    def requests(self, ticket: Ticket) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT coalesce(json_agg(row_to_json(r) ORDER BY r.sequence), '[]'::json) "
            "FROM curie.execution_requests r JOIN curie.work_items w ON w.id=r.work_item_id "
            f"WHERE w.tracker_kind='github' AND w.tracker_issue_id='{int(ticket.number)}'"
        )

    def record_start_failure(self, ticket: Ticket) -> None:
        evidence: dict[str, Any] = {}

        def capture(name: str, read: Callable[[], Any]) -> None:
            try:
                evidence[name] = read()
            except (KeyError, OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                evidence[name] = {"error": type(exc).__name__, "detail": str(exc)}

        capture("current_request", lambda: self.request(ticket))
        capture("work_item", lambda: self.item(ticket))
        capture("requests", lambda: self.requests(ticket))
        capture("transitions", lambda: self.transitions(ticket))

        def provider_calls() -> Any:
            with self.stub.provider_lock:
                return self.stub.provider_evidence()

        capture("provider_calls", provider_calls)
        capture("provider_events", lambda: self.stub.events())

        def pod_status() -> Any:
            pods = json.loads(self.kubectl("-n", self.namespace, "get", "pods", "-o", "json"))[
                "items"
            ]
            return [{"name": pod["metadata"]["name"], "status": pod["status"]} for pod in pods]

        capture("pods", pod_status)
        for component in ("api", "worker"):
            capture(
                component + "_logs",
                lambda component=component: self.kubectl(
                    "-n",
                    self.namespace,
                    "logs",
                    f"deployment/curie-{component}",
                    "--all-containers",
                    "--tail=2000",
                )[-32768:],
            )
        descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(descriptor)
        self.diagnostic_path.chmod(0o600)
        with sqlite3.connect(self.diagnostic_path) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS start_failures ("
                "id INTEGER PRIMARY KEY, issue_id INTEGER NOT NULL, "
                "evidence TEXT NOT NULL, recorded_at REAL NOT NULL)"
            )
            db.execute(
                "INSERT INTO start_failures (issue_id, evidence, recorded_at) VALUES (?, ?, ?)",
                (
                    ticket.number,
                    self.redact_model_evidence(json.dumps(evidence)),
                    time.time(),
                ),
            )

    def record_nochange_failure(
        self, ticket: Ticket, transition: dict[str, Any], *, stage: str
    ) -> None:
        evidence: dict[str, Any] = {
            "stage": stage,
            "expected_transition_id": str(uuid.UUID(transition["id"])),
            "expected_operation_request_id": str(uuid.UUID(transition["request_id"])),
            "expected_request_id": str(uuid.UUID(transition["ci_request_id"])),
        }
        deadline = time.monotonic() + 30

        def bounded(value: Any) -> Any:
            if isinstance(value, str):
                return value[-16384:]
            if isinstance(value, list):
                return [bounded(item) for item in value[-32:]]
            if isinstance(value, dict):
                return {key: bounded(item) for key, item in value.items()}
            return value

        def capture(name: str, read: Callable[[], Any]) -> None:
            if time.monotonic() >= deadline:
                evidence[name] = {"error": "diagnostic_budget_exhausted"}
                return
            previous_deadline = self.deadline
            self.deadline = min(deadline, time.monotonic() + 5)
            try:
                evidence[name] = bounded(read())
            except Exception as exc:  # noqa: BLE001 - preserve the original timeout assertion
                evidence[name] = {"error": type(exc).__name__, "detail": str(exc)[:1000]}
            finally:
                self.deadline = previous_deadline

        def diagnostic_kubectl(*args: str) -> str:
            return sk.ScriptedRun.command(
                self, "kubectl", "--context", self.context, *args, timeout=5
            )

        def provider_calls() -> list[dict[str, Any]]:
            calls = self.stub.provider_evidence()
            actions = [call for call in calls if "/actions/" in call["path"]][-16:]
            failures = [call for call in calls if call["http_status"] >= 400][-8:]
            selected = {call["id"]: call for call in [*actions, *failures, *calls[-8:]]}
            return [selected[key] for key in sorted(selected)]

        request_id = uuid.UUID(evidence["expected_request_id"])
        fix_event = ci_event_id(request_id, 2)
        round_key = ci_round_key(request_id, 2)

        def ci_records() -> dict[str, Any]:
            keys = [
                round_key,
                f"{round_key}:enqueued",
                f"{round_key}:failing",
                done_key(WORKER_KEY_PREFIX_DEFAULT, fix_event),
            ]
            if transition.get("verification_head_sha"):
                rerun_key = (
                    f"{WORK_ITEM_CI_RERUN_PREFIX}:{request_id}:"
                    f"{transition['verification_head_sha']}"
                )
                keys.extend((rerun_key, f"{rerun_key}:lock"))
            return dict(zip(keys, self.valkey("MGET", *keys), strict=True))

        def ci_stream() -> list[dict[str, Any]]:
            selected = []
            for stream_id, fields in self.valkey(
                "XREVRANGE", RUNS_STREAM_DEFAULT, "+", "-", "COUNT", "128"
            ):
                payload = json.loads(
                    dict(zip(fields[::2], fields[1::2], strict=True))[STREAM_PAYLOAD_FIELD]
                )
                if payload.get("event_id") == fix_event:
                    selected.append({
                        "stream_id": stream_id,
                        "event_id": fix_event,
                        "source": payload.get("source"),
                        "received_at": payload.get("received_at"),
                        "has_reply_handle": payload.get("reply_handle") is not None,
                        "text": str(payload.get("text", ""))[:4096],
                    })
            return selected

        capture("provider_calls", provider_calls)
        capture("ci_records", ci_records)
        capture(
            "ci_completion",
            lambda: dict(zip(
                ("done", "gen", "cause"),
                self.valkey(
                    "HMGET", completion_key(WORKER_KEY_PREFIX_DEFAULT, fix_event),
                    "done", "gen", "cause",
                ),
                strict=True,
            )),
        )
        capture("ci_stream", ci_stream)
        capture("requests", lambda: self.requests(ticket))
        capture("work_item", lambda: self.item(ticket))
        capture("transitions", lambda: self.transitions(ticket))
        capture("model_observations", lambda: self.model.observations())
        capture("model_calls", lambda: self.model.call_evidence())
        capture(
            "current_publications",
            lambda: self.sql(
                "SELECT coalesce(json_agg(row_to_json(observation)), '[]'::json) FROM ("
                "SELECT p.id, p.status, p.revision_number, p.base_sha, p.expected_prior_head, "
                "l.head_sha AS lineage_head_sha, left(p.error, 2048) AS failure_detail "
                "FROM curie.publications p "
                "LEFT JOIN curie.thread_publication_lineages l ON l.id=p.lineage_id "
                f"WHERE p.execution_request_id='{evidence['expected_request_id']}'::uuid "
                "ORDER BY p.created_at DESC, p.id DESC LIMIT 16) observation"
            ),
        )
        capture(
            "phase_reports",
            lambda: self.sql(
                "SELECT coalesce(json_agg(row_to_json(observation)), '[]'::json) FROM ("
                "SELECT p.execution_request_id, p.phase, p.loop_round, p.reported_at, p.note "
                "FROM curie.execution_request_phase_reports p "
                "JOIN curie.execution_requests r ON r.id=p.execution_request_id "
                "JOIN curie.work_items w ON w.id=r.work_item_id "
                f"WHERE w.tracker_kind='github' AND w.tracker_issue_id='{int(ticket.number)}' "
                "ORDER BY r.sequence DESC, p.id DESC LIMIT 32) observation"
            ),
        )
        capture("provider_events", lambda: self.stub.events()[-32:])
        for component in ("api", "worker"):
            capture(
                component + "_logs",
                lambda component=component: diagnostic_kubectl(
                    "-n",
                    self.namespace,
                    "logs",
                    f"deployment/curie-{component}",
                    "--all-containers",
                    "--tail=200",
                    "--limit-bytes=16384",
                ),
            )
        requests = evidence.get("requests")
        sandboxes = list(
            dict.fromkeys(
                row["runtime_sandbox_name"]
                for row in reversed(requests if isinstance(requests, list) else [])
                if row.get("runtime_sandbox_name")
            )
        )[:2]
        capture(
            "sandbox_pods",
            lambda: [
                {
                    "name": pod["metadata"]["name"],
                    "phase": pod.get("status", {}).get("phase"),
                }
                for pod in json.loads(
                    diagnostic_kubectl("-n", self.namespace, "get", "pods", "-o", "json")
                )["items"]
                if any(container["name"] == "runner" for container in pod["spec"]["containers"])
            ],
        )
        pods = evidence.get("sandbox_pods")
        current_names = (
            {pod["name"] for pod in pods if isinstance(pod, dict)}
            if isinstance(pods, list)
            else set()
        )
        evidence["absent_recorded_sandboxes"] = [
            sandbox for sandbox in sandboxes if sandbox not in current_names
        ]
        sandboxes = [sandbox for sandbox in sandboxes if sandbox in current_names]
        sandboxes.extend(sorted(current_names - set(sandboxes)))
        for index, sandbox in enumerate(sandboxes[:2]):
            capture(
                f"sandbox_{index}_logs",
                lambda sandbox=sandbox: diagnostic_kubectl(
                    "-n",
                    self.namespace,
                    "logs",
                    sandbox,
                    "--all-containers",
                    "--tail=200",
                    "--limit-bytes=16384",
                ),
            )
        try:
            descriptor = os.open(self.diagnostic_path, os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(descriptor)
            self.diagnostic_path.chmod(0o600)
            with sqlite3.connect(self.diagnostic_path, timeout=1) as db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS nochange_failures ("
                    "id INTEGER PRIMARY KEY, issue_id INTEGER NOT NULL, "
                    "evidence TEXT NOT NULL, recorded_at REAL NOT NULL)"
                )
                db.execute(
                    "INSERT INTO nochange_failures (issue_id, evidence, recorded_at) "
                    "VALUES (?, ?, ?)",
                    (
                        ticket.number,
                        self.redact_model_evidence(json.dumps(evidence)),
                        time.time(),
                    ),
                )
        except (OSError, sqlite3.Error):
            print("dynamic-base private timeout diagnostics could not be saved", flush=True)

    def transitions(self, ticket: Ticket) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT coalesce(json_agg(row_to_json(t) ORDER BY t.committer_time), '[]'::json) "
            "FROM curie.factory_base_transitions t JOIN curie.work_items w ON w.id=t.work_item_id "
            f"WHERE w.tracker_kind='github' AND w.tracker_issue_id='{int(ticket.number)}'"
        )

    def label(self, ticket: Ticket, label: str, *, present: bool = True) -> dict[str, Any]:
        with self.stub.provider_lock:
            issue = self.stub._get("issue", ticket.number)
            names = {row["name"] for row in issue["labels"]}
            names.add(label) if present else names.discard(label)
            issue["labels"] = [{"name": name} for name in sorted(names)]
            self.stub._put("issue", ticket.number, issue)
            self.stub._label_event(
                ticket.number, label, "labeled" if present else "unlabeled", issue["user"]
            )
        if present:
            return self.label_webhook(ticket, label)
        return {}

    def pull(self, number: int) -> dict[str, Any]:
        with self.stub.provider_lock:
            return self.stub._fresh_pull(self.stub._get("pull", number))


def configure_dynamic(monkeypatch: Any, run: DynamicKind) -> None:
    load_module, values, mkdtemp = sk._module, sk.scripted_values, sk.tempfile.mkdtemp

    def external(name: str, path: Path) -> Any:
        loaded = load_module(name, path)
        if path.name == "github_stub.py":
            loaded.GithubStub = dynamic_stub_type(loaded, redact=run.redact_model_evidence)
        elif path.name == "model_script.py":
            loaded.ModelScript = dynamic_model_type(loaded)
        return loaded

    monkeypatch.setattr(sk, "_module", external)
    monkeypatch.setattr(sk, "scripted_values", lambda **kw: run.values(values, **kw))

    def private_temp(*args: Any, **kwargs: Any) -> str:
        if not args and "dir" not in kwargs:
            kwargs["dir"] = run.root
        return mkdtemp(*args, **kwargs)

    monkeypatch.setattr(sk.tempfile, "mkdtemp", private_temp)


def wait(predicate: Any, *, seconds: int = 240) -> Any:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(1)
    return None


def assert_ci_still_owns_request(request: dict[str, Any], transition: dict[str, Any]) -> None:
    assert request["id"] == transition["ci_request_id"], (
        "CI observation named another execution request"
    )
    assert request["status"] == "running", (
        "old-target or pending CI completed the no-change successor"
    )
    assert transition["ci_settled_at"] is None, "an unsettled final head received a CI settlement"

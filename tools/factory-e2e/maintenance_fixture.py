"""GitHub check attempts and a scripted Messages endpoint for the maintenance install.

The driver controls this disposable forge over kubectl exec. Curie sees ordinary
GitHub REST, Git smart HTTP and Anthropic Messages. No control writes to Curie.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import github_stub
import model_script

MODEL_PORT = 8123
CHECK = "example-tests"
CHANGE = "revision.py"
DATABASE = Path("/state/maintenance.sqlite3")


def connect() -> sqlite3.Connection:
    db = sqlite3.connect(DATABASE, timeout=10)
    db.row_factory = sqlite3.Row
    return db


def set_check(db: sqlite3.Connection, sha: str, state: str, name: str) -> int:
    if not re.fullmatch("[0-9a-f]{40}", sha) or state not in {"queued", "success", "failure"}:
        raise ValueError("invalid fixture check")
    row = db.execute(
        "INSERT INTO checks(head, name, state, at) VALUES(?,?,?,?) RETURNING id",
        (sha, name, state, datetime.now(UTC).isoformat()),
    ).fetchone()
    return int(row["id"])


def model_response(
    messages: list[dict[str, Any]],
    mode: str = "edit",
    plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Edit once in the current turn, run a repository test, then request publication."""
    tools: dict[str, str] = {}
    errors = []
    for message in messages:
        blocks = message["content"]
        if message["role"] == "user":
            if isinstance(blocks, str) or not any(b.get("type") == "tool_result" for b in blocks):
                tools.clear()
                errors.clear()
            else:
                errors.extend(
                    b for b in blocks if b.get("type") == "tool_result" and b.get("is_error")
                )
        elif isinstance(blocks, list):
            tools.update((b["id"], b["name"]) for b in blocks if b.get("type") == "tool_use")
    block: dict[str, Any]
    if errors:
        handoff = plan.get("failure_handoff") if plan else None
        if not handoff or len(errors) != 1 or tools.get(errors[0].get("tool_use_id")) != "Bash":
            raise ValueError("the runner reported a failed tool call")
        content = errors[0].get("content")
        if isinstance(content, list):
            content = "\n".join(b["text"] for b in content if b.get("type") == "text")
        if not isinstance(content, str) or handoff["contains"] not in content:
            raise ValueError("the runner reported an unexpected verification failure")
        block = {"type": "text", "text": handoff["text"]}
    elif "Bash" not in tools.values():
        block = {
            "type": "tool_use",
            "name": "Bash",
            "input": {
                "command": (
                    "python3 -B -c 'from pathlib import Path; from runpy import run_path; "
                    'path = Path("revision.py"); revision = run_path(str(path))["REVISION"]; '
                    'path.write_text(f"REVISION = {revision + 1}\\n")\' '
                    "&& python3 -B -m unittest discover"
                )
            },
        }
        if plan:
            command = shlex.join(
                [
                    "python3",
                    "-c",
                    "import json, pathlib, sys\n"
                    "for name, content in json.loads(sys.argv[1]).items():\n"
                    "    path = pathlib.Path(name)\n"
                    "    path.parent.mkdir(parents=True, exist_ok=True)\n"
                    "    path.write_text(content)\n",
                    json.dumps(plan["files"]),
                ]
            )
            steps = [shlex.join(plan["prepare"])] if "prepare" in plan else []
            steps.extend([command, shlex.join(plan["verification"])])
            block["input"]["command"] = " && ".join(steps)
        if mode == "no-ci":
            block["input"]["command"] = (
                "printf 'Example revision\\n' >> README.md && python3 -B -m unittest"
            )
    elif "mcp__curie__publish_changes" not in tools.values():
        block = {
            "type": "tool_use",
            "name": "mcp__curie__publish_changes",
            "input": {"title": "Update example revision", "body": "The revision test passed."},
        }
    else:
        block = {"type": "text", "text": "Published the verified revision."}
    if block["type"] == "tool_use":
        block["id"] = "toolu_" + uuid.uuid4().hex
    return {
        "message": {
            "id": "msg_" + uuid.uuid4().hex,
            "role": "assistant",
            "type": "message",
            "model": "scripted",
            "content": [block],
            "stop_reason": "tool_use" if block["type"] == "tool_use" else "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 10, "output_tokens": 10},
        }
    }


class MaintenanceModel(model_script.ModelScript):
    def _match(self, method: str, path: str, body: bytes) -> dict[str, Any] | None:
        with connect() as db:
            cursor = db.execute("INSERT INTO model_calls DEFAULT VALUES")
            call_id = cursor.lastrowid
        deadline = time.monotonic() + 300
        while True:
            with connect() as db:
                mode, encoded_plan = db.execute("SELECT mode,plan FROM control").fetchone()
            if mode != "hold":
                answer = model_response(
                    json.loads(body)["messages"], mode, json.loads(encoded_plan)
                )
                first = answer["message"]["content"][0].get("name") == "Bash"
                with connect() as db:
                    db.execute("UPDATE model_calls SET started=? WHERE id=?", (int(first), call_id))
                return answer
            if time.monotonic() >= deadline:
                raise RuntimeError("driver did not release the model")
            time.sleep(0.5)


def initialize(base: str) -> None:
    with connect() as db:
        db.executescript(
            "CREATE TABLE checks(id INTEGER PRIMARY KEY, head TEXT, name TEXT,"
            "state TEXT, at TEXT);"
            "CREATE TABLE observations(id INTEGER PRIMARY KEY, head TEXT, kind TEXT);"
            "CREATE TABLE model_calls(id INTEGER PRIMARY KEY, started INTEGER DEFAULT 0);"
            "CREATE TABLE control(mode TEXT,plan TEXT); "
            "CREATE TABLE push_gates(branch TEXT PRIMARY KEY, old_head TEXT NOT NULL, "
            "new_head TEXT, phase TEXT NOT NULL);"
            "INSERT INTO control VALUES ('edit','null');"
        )
        set_check(db, base, "success", CHECK)


def _push_update(body: bytes) -> tuple[str, str, str] | None:
    try:
        length = int(body[:4], 16)
    except ValueError:
        return None
    command = body[4:length].split(b"\0", 1)[0].rstrip(b"\n")
    match = re.fullmatch(rb"([0-9a-f]{40}) ([0-9a-f]{40}) refs/heads/([^ ]+)", command)
    if match is None:
        return None
    old, new, branch = match.groups()
    return branch.decode(), old.decode(), new.decode()


def _wait_push_gate(branch: str, *, phase: str | None, deadline: float) -> bool:
    while time.monotonic() < deadline:
        with connect() as db:
            row = db.execute("SELECT phase FROM push_gates WHERE branch=?", (branch,)).fetchone()
        if row is None:
            return phase is None
        if row["phase"] == phase:
            return True
        time.sleep(0.1)
    raise subprocess.SubprocessError("the driver did not release the publication push gate")


class MaintenanceGitHub(github_stub.GithubStub):
    def start(self) -> None:
        super().start()
        initialize(self.main_revision())
        self.model = MaintenanceModel(
            Path("/stub/maintenance-transcript.json"), host="0.0.0.0", port=MODEL_PORT
        )
        self.model.start()

    def close(self) -> None:
        if hasattr(self, "model"):
            self.model.close()
        super().close()

    def _run(self, *args: str, input_: bytes | None = None, **kwargs: Any) -> bytes:
        env = kwargs.get("env", {})
        if (
            args != ("git", "http-backend")
            or input_ is None
            or not env.get("PATH_INFO", "").endswith("/git-receive-pack")
            or env.get("REMOTE_USER") == github_stub.HUMAN["login"]
        ):
            return super()._run(*args, input_=input_, **kwargs)
        update = _push_update(input_)
        if update is None:
            return super()._run(*args, input_=input_, **kwargs)
        branch, old, new = update
        with connect() as db:
            gated = db.execute(
                "UPDATE push_gates SET phase='waiting',new_head=? "
                "WHERE branch=? AND old_head=? AND phase='armed' RETURNING branch",
                (new, branch, old),
            ).fetchone()
        if gated is None:
            return super()._run(*args, input_=input_, **kwargs)

        deadline = time.monotonic() + 100
        if not _wait_push_gate(branch, phase="proceed", deadline=deadline):
            raise subprocess.SubprocessError(
                "publication push gate was cleared before receive-pack"
            )
        result = super()._run(*args, input_=input_, **kwargs)
        accepted = self._ref(branch) == new
        with connect() as db:
            db.execute(
                "UPDATE push_gates SET phase=? WHERE branch=? AND new_head=?",
                ("accepted" if accepted else "refused", branch, new),
            )
        if accepted:
            _wait_push_gate(branch, phase=None, deadline=deadline)
        return result

    def _checks(self, sha: str) -> list[dict[str, Any]]:
        with connect() as db:
            db.execute("INSERT INTO observations(head,kind) VALUES(?,'ci_read')", (sha,))
            if db.execute("SELECT mode FROM control").fetchone()[0] == "no-ci":
                return []
            for name, default in ((CHECK, "queued"), ("example-integration", "success")):
                if not db.execute(
                    "SELECT 1 FROM checks WHERE head=? AND name=? LIMIT 1", (sha, name)
                ).fetchone():
                    set_check(db, sha, default, name)
            rows = db.execute(
                "SELECT * FROM checks WHERE id IN "
                "(SELECT max(id) FROM checks WHERE head=? GROUP BY name) ORDER BY id",
                (sha,),
            ).fetchall()
        # GitHub lists the latest attempt for each check name under filter=latest.
        # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
        return [
            {
                "id": row["id"],
                "name": row["name"],
                "head_sha": sha,
                "status": "queued" if row["state"] == "queued" else "completed",
                "conclusion": None if row["state"] == "queued" else row["state"],
                "started_at": row["at"],
                "completed_at": None if row["state"] == "queued" else row["at"],
                "app": {"slug": "github-actions", "id": 51},
                "html_url": f"{self.base_url}/{github_stub.REPOSITORY}/actions/runs/{row['id']}",
                "output": {"title": "Revision tests", "summary": "Example repository test result"},
            }
            for row in rows
        ]

    def _rest(self, method: str, path: str, query: Any, body: Any, **kwargs: Any) -> Any:
        # https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run
        prefix = rf"/(?:repos/{github_stub.REPOSITORY}|repositories/4401)/"
        rerun = re.fullmatch(prefix + r"actions/runs/(\d+)/rerun-failed-jobs", path)
        if method == "POST" and rerun:
            with connect() as db:
                job = db.execute("SELECT * FROM checks WHERE id=?", (rerun[1],)).fetchone()
                if job is None or job["state"] != "failure":
                    return 422, {"message": "No failed attempt"}
                db.execute("INSERT INTO observations(head,kind) VALUES(?,'rerun')", (job["head"],))
                set_check(db, job["head"], "queued", job["name"])
            return 201, {}
        if method == "GET" and re.fullmatch(prefix + r"pulls/\d+", path):
            result = super()._rest(method, path, query, body, **kwargs)
            if result and result[0] == 200:
                with connect() as db:
                    db.execute(
                        "INSERT INTO observations(head,kind) VALUES(?,'pull')",
                        (result[1]["head"]["sha"],),
                    )
            return result
        job = re.fullmatch(prefix + r"actions/jobs/(\d+)(/logs)?", path)
        if method == "GET" and job:
            return (
                (404, {"message": "No retained log"}) if job[2] else (200, {"run_id": int(job[1])})
            )
        result = super()._rest(method, path, query, body, **kwargs)
        if method == "GET" and result and result[0] == 200:
            if re.fullmatch(prefix + r"git/ref/heads/.+", path):
                self._record_read(result[1]["object"]["sha"], "branch_read")
            if commit := re.fullmatch(prefix + r"git/commits/([0-9a-f]{40})", path):
                self._record_read(commit[1], "commit_read")
        return result

    def _record_read(self, head: str, kind: str) -> None:
        with connect() as db:
            db.execute("INSERT INTO observations(head,kind) VALUES(?,?)", (head, kind))


def human_push(operation: str, url: str, ca: str, branch: str, expected: str, value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", expected):
        raise ValueError("expected a full commit ID for the lease")
    ref = "refs/heads/" + branch
    with TemporaryDirectory(dir=DATABASE.parent) as checkout:
        env = {
            "PATH": os.defpath,
            "HOME": checkout,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_ALLOW_PROTOCOL": "https",
            "GIT_TERMINAL_PROMPT": "0",
            "HUMAN_HEADER": "Authorization: " + github_stub.HUMAN_AUTHORIZATION,
        }

        def git(*argv: str, data: str | None = None) -> str:
            return subprocess.run(
                [
                    "git",
                    "-c",
                    "http.sslVerify=true",
                    "-c",
                    "http.sslCAInfo=" + ca,
                    "-c",
                    "http.followRedirects=false",
                    "--config-env=http.extraHeader=HUMAN_HEADER",
                    *argv,
                ],
                cwd=checkout,
                env=env,
                input=data,
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            ).stdout.strip()

        git("clone", "--no-checkout", "--single-branch", "--branch", branch, "--", url, ".")
        if operation == "commit":
            git("read-tree", expected)
            for filename, content in json.loads(value).items():
                blob = git("hash-object", "-w", "--stdin", data=content)
                git("update-index", "--add", "--cacheinfo", f"100644,{blob},{filename}")
            target = git(
                "-c",
                "user.name=Example Author",
                "-c",
                "user.email=author@example.com",
                "commit-tree",
                git("write-tree"),
                "-p",
                expected,
                data="Update example fixture\n",
            )
        elif operation == "copy-merge":
            marker = f"Curie-Revision: {uuid.UUID(value)}"
            parents = git("show", "-s", "--format=%P", expected).split()
            message = git("show", "-s", "--format=%B", expected)
            if len(parents) != 2 or [
                line for line in message.splitlines() if line.startswith("Curie-Revision:")
            ] != [marker]:
                raise ValueError("expected a merge with the named revision marker")
            target = git(
                "-c",
                "user.name=Example Reviewer",
                "-c",
                "user.email=reviewer@example.com",
                "commit-tree",
                git("rev-parse", f"{expected}^{{tree}}"),
                "-p",
                parents[0],
                "-p",
                parents[1],
                data=message + "\n",
            )
            if target == expected:
                raise ValueError("the copied merge has the same SHA")
        else:
            if not re.fullmatch(r"[0-9a-f]{40}", value):
                raise ValueError("expected a full commit ID to restore")
            target = value
        git("push", f"--force-with-lease={ref}:{expected}", "origin", f"{target}:{ref}")
        return target


def control(command: str, args: list[str]) -> Any:
    if command == "arm-push-gate":
        branch, old = args
        with connect() as db:
            db.execute(
                "INSERT INTO push_gates(branch,old_head,phase) VALUES(?,?,'armed')", (branch, old)
            )
        return True
    if command == "push-gate":
        (branch,) = args
        with connect() as db:
            row = db.execute("SELECT * FROM push_gates WHERE branch=?", (branch,)).fetchone()
        return dict(row) if row else None
    if command == "proceed-push-gate":
        (branch,) = args
        with connect() as db:
            changed = db.execute(
                "UPDATE push_gates SET phase='proceed' WHERE branch=? AND phase='waiting'",
                (branch,),
            )
        return changed.rowcount == 1
    if command == "clear-push-gate":
        (branch,) = args
        with connect() as db:
            db.execute("DELETE FROM push_gates WHERE branch=?", (branch,))
        return True
    if command == "plan":
        plan = json.loads(args[0])
        for name in ("verification", "prepare") if "prepare" in plan else ("verification",):
            vector = plan[name]
            if (
                not isinstance(vector, list)
                or not vector
                or not all(isinstance(arg, str) for arg in vector)
            ):
                raise ValueError(f"{name} must be an argument vector")
        if "failure_handoff" in plan:
            handoff = plan["failure_handoff"]
            if (
                not isinstance(handoff, dict)
                or set(handoff) != {"contains", "text"}
                or not all(isinstance(value, str) and value.strip() for value in handoff.values())
            ):
                raise ValueError("failure_handoff needs a failure substring and a terminal message")
        if not plan["files"] or any(
            Path(name).is_absolute() or ".." in Path(name).parts or not isinstance(value, str)
            for name, value in plan["files"].items()
        ):
            raise ValueError("expected repository-relative fixture files")
        with connect() as db:
            db.execute("UPDATE control SET plan=?,mode='edit'", (json.dumps(plan),))
        return True
    if command == "clear-plan":
        with connect() as db:
            db.execute("UPDATE control SET plan='null',mode='edit'")
        return True
    if command == "mode":
        (mode,) = args
        if mode not in {"edit", "hold", "no-ci"}:
            raise ValueError("unknown scripted model mode")
        with connect() as db:
            db.execute("UPDATE control SET mode=?", (mode,))
        return mode
    if command == "set-checks":
        with connect() as db:
            return [set_check(db, *entry) for entry in json.loads(args[0])]
    if command == "counts":
        (head,) = args
        with connect() as db:
            counts = dict(
                db.execute(
                    "SELECT kind,count(*) FROM observations WHERE head=? GROUP BY kind", (head,)
                )
            )
            counts["model"], counts["starts"] = db.execute(
                "SELECT count(*), coalesce(sum(started),0) FROM model_calls"
            ).fetchone()
        with sqlite3.connect(DATABASE.with_name("state.sqlite3")) as db:
            counts["human_push"], counts["factory_push"] = db.execute(
                "SELECT count(*) FILTER (WHERE actor=?), count(*) FILTER (WHERE actor!=?) "
                "FROM request_log WHERE method='POST' AND path LIKE '%/git-receive-pack'",
                (github_stub.HUMAN["login"], github_stub.HUMAN["login"]),
            ).fetchone()
            counts["pr_write"] = db.execute(
                "SELECT count(*) FROM request_log WHERE "
                "(method='POST' AND path LIKE '%/pulls') OR "
                "(method='PATCH' AND path LIKE '%/pulls/%')"
            ).fetchone()[0]
        return counts
    if command == "comment":
        number, host, port = args
        stub = github_stub.GithubStub(
            Path("/state"),
            {"version": 1},
            host,
            int(port),
            seed_tree=Path(__file__).parent,
            issue_file=Path(__file__),
        )
        return stub.append_comment(
            int(number),
            "Please increase the example revision and publish it.",
            author=github_stub.HUMAN,
            author_association="COLLABORATOR",
        )["id"]
    if command in {"commit", "restore", "copy-merge"}:
        return human_push(command, *args)
    raise ValueError("unknown fixture operation")


if __name__ == "__main__":
    if sys.argv[1] == "serve":
        raise SystemExit(github_stub.main(MaintenanceGitHub))
    print(json.dumps(control(sys.argv[1], sys.argv[2:])))

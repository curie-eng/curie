"""External HTTP fixtures for the admitted-agent production-boundary tests."""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "apps/api/tests"))

from forge_fakes import gitlab, jira  # noqa: E402


def tracker_stub_type(github_module: Any, *, redact: Callable[[str], str]) -> type:
    """Serve provider fakes over the harness's real TLS and smart Git socket.

    Provider response shapes come from the existing, documented REST fixtures.
    The GitLab REST commit graph follows the bare repository receiving actual
    clone/push traffic; it does not invent commits for platform publications.
    """

    class Trackers(github_module.GithubStub):
        def start(self) -> None:
            super().start()
            self.provider_lock = threading.RLock()
            self._database.chmod(0o600)
            with sqlite3.connect(self._database) as db:
                db.execute(
                    "CREATE TABLE provider_calls (id INTEGER PRIMARY KEY, method TEXT NOT NULL, "
                    "path TEXT NOT NULL, http_status INTEGER NOT NULL, "
                    "provider_clock TEXT NOT NULL, response TEXT NOT NULL)"
                )
            self.green_heads: set[str] = set()
            self.red_heads: set[str] = set()
            self.gitlab = gitlab.GitLabAPI(
                base_url=self.base_url + "/gitlab/api/v4",
                project_path="acme-corp/acme-bot",
                step_s=0,
            )
            self.gitlab.now = datetime.now(UTC)
            self.gitlab.add_user(gitlab.ROOT)
            self.gitlab.set_member(gitlab.ROOT, 40)
            self.jira = jira.JiraFake(start_ms=int(datetime.now(UTC).timestamp() * 1000))
            self._sync_gitlab()

        def provider_evidence(self) -> list[dict[str, Any]]:
            with sqlite3.connect(self._database.resolve().as_uri() + "?mode=ro", uri=True) as db:
                db.row_factory = sqlite3.Row
                return [dict(row) for row in db.execute("SELECT * FROM provider_calls ORDER BY id")]

        def sync_gitlab_clock(self) -> None:
            # GitLabAPI.tick advances only on fixture mutations. This socket
            # serves real-time polls, so seed timestamps and Date must agree.
            # Provider timestamps: https://docs.gitlab.com/api/rest/#date-format
            self.gitlab.tick(at=max(self.gitlab.now, datetime.now(UTC)))

        def _sync_gitlab(self) -> None:
            self.sync_gitlab_clock()
            output = self._run(
                "git",
                "--git-dir",
                str(self._repository),
                "for-each-ref",
                "--format=%(refname:short) %(objectname)",
                "refs/heads",
            ).decode()
            branches = dict(line.rsplit(" ", 1) for line in output.splitlines())
            history = self._run(
                "git",
                "--git-dir",
                str(self._repository),
                "log",
                "--all",
                "--format=%H %P",
            ).decode()
            for line in history.splitlines():
                sha, *parents = line.split()
                if sha in self.gitlab.commits:
                    continue
                commit = self._commit(sha)
                if commit is None:
                    raise RuntimeError("the GitLab fixture could not read a repository commit")
                # GitLab returns the full commit message, including trailers.
                # https://docs.gitlab.com/api/commits/#get-a-single-commit
                # GithubStub._commit reads the actual pushed Git object.
                self.gitlab.commits[sha] = {
                    "parent_ids": parents,
                    "message": commit["message"],
                    "created_at": datetime.now(UTC),
                    "author": "Example Author",
                }
            self.gitlab.branches = branches
            pipeline_heads = {pipeline.sha for pipeline in self.gitlab.pipelines.values()}
            for sha in sorted(set(branches.values()) - pipeline_heads):
                # GitLabAPI's documented pipeline/job fixtures keep new heads
                # pending until mark_ci supplies a verdict, with a green base.
                # https://docs.gitlab.com/api/pipelines/#list-project-pipelines
                # https://docs.gitlab.com/api/jobs/#list-pipeline-jobs
                self.gitlab.add_pipeline(
                    sha,
                    jobs={"admitted-agent": "success" if sha == branches["main"] else "pending"},
                )
            # A retry creates a new pending job. Subsequent reads complete
            # its active attempt using the explicit fixture verdict.
            # https://docs.gitlab.com/api/jobs/#retry-a-job
            for job in self.gitlab.jobs.values():
                if job.retried or job.status not in {"pending", "running"}:
                    continue
                sha = self.gitlab.pipelines[job.pipeline].sha
                if sha in self.red_heads:
                    job.status = "failed"
                elif sha in self.green_heads:
                    job.status = "success"

        def _request(self, handler: Any) -> None:
            path = urlsplit(handler.path).path
            if path.startswith("/gitlab/acme-corp/acme-bot.git/"):
                body = handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
                self._git_request(
                    handler, path.removeprefix("/gitlab"), urlsplit(handler.path).query, body
                )
                return
            if not path.startswith(("/gitlab/api/v4/", "/rest/api/3/", "/_edge/")):
                super()._request(handler)
                return
            body = handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
            with self.provider_lock:
                if path.startswith("/gitlab/api/v4/"):
                    self._sync_gitlab()
                    response = self.gitlab.handle(
                        httpx.Request(
                            handler.command,
                            self.base_url + handler.path,
                            headers=dict(handler.headers),
                            content=body,
                        )
                    )
                else:

                    async def request_jira() -> httpx.Response:
                        async with httpx.AsyncClient(
                            transport=httpx.ASGITransport(app=self.jira.app),
                            base_url=jira.SITE,
                        ) as client:
                            return await client.request(
                                handler.command,
                                handler.path,
                                headers={
                                    key: value
                                    for key, value in handler.headers.items()
                                    if key.lower() != "host"
                                },
                                content=body,
                            )

                    response = asyncio.run(request_jira())
                with sqlite3.connect(self._database) as db:
                    db.execute(
                        "INSERT INTO provider_calls "
                        "(method, path, http_status, provider_clock, response) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            handler.command,
                            redact(handler.path),
                            response.status_code,
                            self.gitlab.now.isoformat(),
                            redact(response.text),
                        ),
                    )
            # BaseHTTPRequestHandler.send_response adds its own Date. Forward
            # the provider's single Date field instead (RFC 9110 section 6.6.1).
            handler.send_response_only(response.status_code)
            if "date" not in response.headers:
                handler.send_header("Date", handler.date_time_string())
            for key, value in response.headers.items():
                if key.lower() not in {"content-length", "connection", "transfer-encoding"}:
                    handler.send_header(key, value)
            handler.send_header("Content-Length", str(len(response.content)))
            handler.end_headers()
            handler.wfile.write(response.content)

        def _rest(self, method: str, path: str, query: Any, body: Any) -> Any:
            # GitHub's base commit has green CI. Every new published head stays
            # pending until mark_ci supplies its explicit red or green verdict.
            # Check-run fields and the total_count/check_runs envelope:
            # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
            match = re.fullmatch(r"/repos/acme-corp/acme-bot/commits/([^/]+)/check-runs", path)
            if method == "GET" and match:
                sha = match.group(1)
                green = sha == self._find_ref("main") or sha in self.green_heads
                pending = not green and sha not in self.red_heads
                check = {
                    "id": 4619,
                    "name": "admitted-agent",
                    "status": "in_progress" if pending else "completed",
                    "conclusion": None if pending else "success" if green else "failure",
                    "head_sha": sha,
                    "started_at": github_module._utc(),
                    "completed_at": None if pending else github_module._utc(),
                    "html_url": self.base_url + "/example-check",
                    "app": {"id": 51, "slug": "example-ci"},
                    "output": {"title": "Example CI", "summary": "Fix the marker revision"},
                }
                return 200, {"total_count": 1, "check_runs": [check]}
            if method == "GET" and re.fullmatch(
                r"/repos/acme-corp/acme-bot/commits/[^/]+/(?:status|statuses)", path
            ):
                # The combined status object and the individual status list:
                # https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
                # https://docs.github.com/en/rest/commits/statuses#list-commit-statuses-for-a-reference
                return (
                    (200, [])
                    if path.endswith("statuses")
                    else (200, {"state": "success", "statuses": [], "total_count": 0})
                )
            return super()._rest(method, path, query, body)

    return Trackers


def marker_model_type(model_module: Any, *, redact: Callable[[str], str]) -> type:
    class MarkerModel(model_module.ModelScript):
        """Script the external model, reading marker evidence from real tool output."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.database = Path(os.environ["TMPDIR"]) / "marker-observations.sqlite3"
            descriptor = os.open(self.database, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
            self.database.chmod(0o600)
            with sqlite3.connect(self.database) as db:
                db.execute(
                    "CREATE TABLE observations (lane TEXT, round INTEGER, marker TEXT, "
                    "PRIMARY KEY(lane, round, marker))"
                )
                db.execute(
                    "CREATE TABLE model_calls (id INTEGER PRIMARY KEY, lane TEXT NOT NULL, "
                    "round INTEGER NOT NULL, tools TEXT NOT NULL, tool_results TEXT NOT NULL, "
                    "message_count INTEGER NOT NULL, latest_role TEXT NOT NULL, "
                    "latest_content TEXT NOT NULL, matched_marker TEXT)"
                )

        def observations(self) -> list[tuple[str, int, str]]:
            with sqlite3.connect(self.database) as db:
                return list(db.execute("SELECT lane, round, marker FROM observations"))

        def call_evidence(self) -> list[dict[str, Any]]:
            with sqlite3.connect(self.database) as db:
                db.row_factory = sqlite3.Row
                return [dict(row) for row in db.execute("SELECT * FROM model_calls ORDER BY id")]

        def _match(self, method: str, path: str, body: bytes) -> dict[str, Any] | None:
            if method != "POST" or path != "/v1/messages":
                return None
            request = json.loads(body)
            messages = request.get("messages", [])
            first = next((m for m in messages if m.get("role") == "user"), None)
            if first is None:
                return None
            lane = json.dumps(first["content"], sort_keys=True)
            serialized = json.dumps(messages)
            round_ = max(
                [1, *(int(n) for n in re.findall(r"Curie wait_ci round ([23])", serialized))]
            )
            tools = [
                b
                for m in messages
                if m.get("role") == "assistant"
                for b in m.get("content", [])
                if isinstance(b, dict) and b.get("type") == "tool_use"
            ]
            reads = sum(t["name"] == "Bash" for t in tools)
            publications = sum(t["name"] == "mcp__curie__publish_changes" for t in tools)
            latest = json.dumps(messages[-1].get("content", ""))
            tool_results = [
                {"message_index": index, **block}
                for index, message in enumerate(messages)
                if message.get("role") == "user"
                and isinstance(message.get("content"), list)
                for block in message["content"]
                if isinstance(block, dict) and block.get("type") == "tool_result"
            ]
            marker_rounds: dict[str, int] = {}
            for tool in tools:
                identifier = tool.get("id")
                if tool["name"] != "Bash" or not isinstance(identifier, str):
                    continue
                issued = re.fullmatch(r"toolu_marker_([123])_[0-9]+", identifier)
                if issued:
                    marker_rounds[identifier] = int(issued.group(1))
            # #4619 model-call diagnostics show a trailing system message and
            # successful user tool_result blocks with string Bash output.
            # Only output closing an issued marker call proves a marker read.
            markers: set[tuple[int, str]] = set()
            for result in tool_results:
                result_round = marker_rounds.get(result.get("tool_use_id"))
                output = result.get("content")
                if (
                    result_round is None
                    or result.get("is_error") is not False
                    or not isinstance(output, str)
                ):
                    continue
                markers.update(
                    (result_round, marker)
                    for marker in re.findall(r"^C4_BUNDLE=([a-z0-9-]+)$", output, re.MULTILINE)
                )
            current_markers = sorted(marker for seen, marker in markers if seen == round_)
            with sqlite3.connect(self.database) as db:
                db.execute(
                    "INSERT INTO model_calls "
                    "(lane, round, tools, tool_results, message_count, latest_role, "
                    "latest_content, matched_marker) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        redact(lane), round_, redact(json.dumps(tools)),
                        redact(json.dumps(tool_results)), len(messages),
                        redact(str(messages[-1].get("role", ""))), redact(latest),
                        redact(current_markers[0]) if len(current_markers) == 1 else None,
                    ),
                )
                db.executemany(
                    "INSERT OR IGNORE INTO observations VALUES(?,?,?)",
                    [(lane, seen, marker) for seen, marker in sorted(markers)],
                )
            if not current_markers:
                content = [
                    {
                        "type": "tool_use",
                        "id": f"toolu_marker_{round_}_{reads}",
                        "name": "Bash",
                        "input": {
                            "command": (
                                'marker=$(cat "$CURIE_PLUGIN_DIR/c4-marker") && '
                                'printf "C4_BUNDLE=%s\\n" "$marker" && '
                                f'printf "%s round {round_}\\n" "$marker" > c4-runtime-marker.txt'
                            )
                        },
                    }
                ]
            elif publications < round_:
                content = [
                    {
                        "type": "tool_use",
                        "id": f"toolu_publish_{round_}",
                        "name": "mcp__curie__publish_changes",
                        "input": {
                            "title": "Exercise the admitted agent",
                            "body": "Runtime marker observed.",
                        },
                    }
                ]
            else:
                content = [{"type": "text", "text": "Runtime marker recorded."}]
            return {
                "message": {
                    "id": f"msg_c4_{len(tools)}_{round_}",
                    "type": "message",
                    "role": "assistant",
                    "model": "scripted",
                    "content": content,
                    "stop_reason": "tool_use" if content[0]["type"] == "tool_use" else "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 10},
                }
            }

    return MarkerModel

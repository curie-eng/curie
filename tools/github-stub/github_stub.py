"""TLS GitHub factory fixture using recorded check lifecycles and real Git transport."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import signal
import socket
import sqlite3
import ssl
import subprocess
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

REPOSITORY = "acme-corp/acme-bot"
HUMAN = {"id": 6601, "login": "octocat", "type": "User"}
BOT = {"id": 51, "login": "example-app[bot]", "type": "Bot"}
PERMISSIONS = {
    "contents": "write",
    "issues": "write",
    "pull_requests": "write",
    "checks": "read",
    "statuses": "read",
    "actions": "write",
    "metadata": "read",
}


def _date(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utc() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class GithubStub:
    """Own one disposable HTTPS server, SQLite fixture state, and bare repository."""

    def __init__(
        self,
        root: Path,
        recording: dict[str, Any],
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        bind: str | None = None,
    ) -> None:
        if recording.get("version") != 1:
            raise ValueError("unsupported GitHub recording version")
        try:
            advertised_ip = ipaddress.ip_address(host)
        except ValueError:
            labels = host.split(".")
            if len(host) > 253 or not all(
                re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
                for label in labels
            ):
                raise ValueError("GitHub stub host must be a DNS name or IP address") from None
            self._san = f"DNS:{host}"
        else:
            self._san = f"IP:{advertised_ip}"
        bind_address = bind if bind is not None else host
        if bind_address != "localhost":
            try:
                ipaddress.ip_address(bind_address)
            except ValueError:
                if bind is not None:
                    raise ValueError(
                        "GitHub stub bind must be an IP address or localhost"
                    ) from None
        self.root = root.resolve()
        self.recording = recording
        self.host = host
        self.bind = bind_address
        self.port = port
        self.ca_file = self.root / "ca.pem"
        self._database = self.root / "state.sqlite3"
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._real_start: float | None = None
        self._closed = False
        self._git_root = self.root / "repositories"
        self._repository = self._git_root / f"{REPOSITORY}.git"

    @property
    def base_url(self) -> str:
        authority = f"[{self.host}]" if ":" in self.host else self.host
        return f"https://{authority}:{self.port}"

    @property
    def clone_url(self) -> str:
        return f"{self.base_url}/{REPOSITORY}.git"

    @property
    def unknown_requests(self) -> list[str]:
        if not self._database.exists():
            return []
        with sqlite3.connect(self._database) as connection:
            return [row[0] for row in connection.execute("SELECT request FROM unsupported")]

    def _run(self, *args: str, input_: bytes | None = None, **kwargs: Any) -> bytes:
        result = subprocess.run(
            args, input=input_, capture_output=True, timeout=30, check=True, **kwargs
        )
        return bytes(result.stdout)

    def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("GitHub stub already started")
        self.root.mkdir(parents=True, exist_ok=True)
        self._run(
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=Curie Example Test CA",
            "-keyout",
            str(self.root / "ca.key"),
            "-out",
            str(self.ca_file),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        )
        self._run(
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(self.root / "server.key"),
            "-out",
            str(self.root / "server.csr"),
        )
        extension = self.root / "server.ext"
        extension.write_text(
            f"subjectAltName={self._san},IP:127.0.0.1,DNS:localhost\n"
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
        )
        self._run(
            "openssl",
            "x509",
            "-req",
            "-in",
            str(self.root / "server.csr"),
            "-CA",
            str(self.ca_file),
            "-CAkey",
            str(self.root / "ca.key"),
            "-CAcreateserial",
            "-days",
            "2",
            "-out",
            str(self.root / "server.pem"),
            "-extfile",
            str(extension),
        )
        for name in ("ca.key", "server.key"):
            (self.root / name).chmod(0o600)
        with sqlite3.connect(self._database) as connection:
            connection.executescript(
                "CREATE TABLE clock(seconds REAL NOT NULL); INSERT INTO clock VALUES(0);"
                "CREATE TABLE objects(kind TEXT, id INTEGER, body TEXT NOT NULL,"
                " PRIMARY KEY(kind,id)); CREATE TABLE unsupported(request TEXT);"
                "CREATE TABLE tokens(token TEXT PRIMARY KEY, permissions TEXT);"
            )
        self._repository.parent.mkdir(parents=True, exist_ok=True)
        self._run("git", "init", "--bare", "--initial-branch=main", str(self._repository))
        self._run("git", "--git-dir", str(self._repository), "config", "http.receivepack", "true")
        blob = self._run(
            "git",
            "--git-dir",
            str(self._repository),
            "hash-object",
            "-w",
            "--stdin",
            input_=b"def example():\n    return 'example'\n",
        ).strip()
        tree = (
            self._run(
                "git",
                "--git-dir",
                str(self._repository),
                "mktree",
                input_=b"100644 blob " + blob + b"\texample.py\n",
            )
            .strip()
            .decode()
        )
        commit = (
            self._run(
                "git",
                "-c",
                "user.name=Example Author",
                "-c",
                "user.email=author@example.com",
                "--git-dir",
                str(self._repository),
                "commit-tree",
                tree,
                input_=b"Seed example repository\n",
            )
            .strip()
            .decode()
        )
        self._run(
            "git", "--git-dir", str(self._repository), "update-ref", "refs/heads/main", commit
        )
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                stub._request(self)

            def do_POST(self) -> None:
                stub._request(self)

            def do_PATCH(self) -> None:
                stub._request(self)

            def do_PUT(self) -> None:
                stub._request(self)

            def do_DELETE(self) -> None:
                stub._request(self)

        class Server(ThreadingHTTPServer):
            address_family = socket.AF_INET6 if ":" in stub.bind else socket.AF_INET

        server = Server((self.bind, self.port), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(self.root / "server.pem", self.root / "server.key")
        server.socket = context.wrap_socket(server.socket, server_side=True)
        self.port = server.server_port
        self._server = server
        self._put(
            "issue",
            3815,
            self._issue(3815, "Example factory change", "Update example.py.", ["factory"]),
        )
        self._label_event(3815, "factory", "labeled", HUMAN)
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self.unknown_requests:
            raise RuntimeError(f"unsupported_request: {self.unknown_requests[0]}")

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("replay time cannot move backwards")
        with sqlite3.connect(self._database) as connection:
            connection.execute("UPDATE clock SET seconds=seconds+?", (seconds,))

    def _seconds(self) -> float:
        with sqlite3.connect(self._database) as connection:
            elapsed = float(connection.execute("SELECT seconds FROM clock").fetchone()[0])
        return elapsed + (time.monotonic() - self._real_start if self._real_start else 0)

    def _put(self, kind: str, identifier: int, body: dict[str, Any]) -> None:
        with sqlite3.connect(self._database) as connection:
            connection.execute(
                "INSERT OR REPLACE INTO objects VALUES(?,?,?)", (kind, identifier, json.dumps(body))
            )

    def _rows(self, kind: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self._database) as connection:
            return [
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT body FROM objects WHERE kind=? ORDER BY id", (kind,)
                )
            ]

    def _get(self, kind: str, identifier: int) -> dict[str, Any] | None:
        return next((row for row in self._rows(kind) if row["id"] == identifier), None)

    def _issue(self, number: int, title: str, body: str, labels: list[str]) -> dict[str, Any]:
        return {
            "id": number,
            "number": number,
            "title": title,
            "body": body,
            "state": "open",
            "user": HUMAN,
            "labels": [{"name": label} for label in labels],
            "html_url": f"{self.base_url}/{REPOSITORY}/issues/{number}",
            "created_at": _utc(),
            "updated_at": _utc(),
        }

    def _label_event(self, number: int, label: str, event: str, actor: dict[str, Any]) -> None:
        identifier = max((row["id"] for row in self._rows("event")), default=0) + 1
        row = {
            "id": identifier,
            "issue_number": number,
            "event": event,
            "label": {"name": label},
            "actor": actor,
            "created_at": _utc(),
        }
        if actor == BOT:
            row["performed_via_github_app"] = {"id": 51}
        self._put("event", identifier, row)

    def _ref(self, branch: str) -> str:
        return (
            self._run(
                "git", "--git-dir", str(self._repository), "rev-parse", f"refs/heads/{branch}"
            )
            .decode()
            .strip()
        )

    def _repository_row(self) -> dict[str, Any]:
        return {
            "id": 4401,
            "full_name": REPOSITORY,
            "name": "acme-bot",
            "owner": {"login": "acme-corp"},
            "default_branch": "main",
            "html_url": f"{self.base_url}/{REPOSITORY}",
            "clone_url": self.clone_url,
        }

    def _checks(self) -> list[dict[str, Any]]:
        now = _date(self.recording["epoch"]) + timedelta(seconds=self._seconds())
        rows = []
        for recorded in self.recording["check_runs"]:
            if _date(recorded["started_at"]) > now:
                continue
            row = dict(recorded)
            if not row.get("completed_at") or _date(row["completed_at"]) > now:
                row.update(status="in_progress", conclusion=None, completed_at=None)
            rows.append(row)
        return rows

    def _json(self, handler: BaseHTTPRequestHandler, status: int, body: Any) -> None:
        payload = json.dumps(body).encode()
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    def _request(self, handler: BaseHTTPRequestHandler) -> None:
        parsed = urlsplit(handler.path)
        path = unquote(parsed.path)
        if path.startswith("/api/v3/"):
            path = path.removeprefix("/api/v3")
        body = handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
        if path.startswith(f"/{REPOSITORY}.git/"):
            if handler.command not in {"GET", "POST"} or not (
                path.endswith("/info/refs")
                or path.endswith("/git-upload-pack")
                or path.endswith("/git-receive-pack")
            ):
                self._unsupported(handler)
                return
            try:
                self._git_request(handler, path, parsed.query, body)
            except subprocess.SubprocessError:
                self._unsupported(handler)
            return
        try:
            payload = json.loads(body) if body else {}
            result = self._rest(handler.command, path, parse_qs(parsed.query), payload)
        except (ValueError, KeyError, subprocess.CalledProcessError) as exc:
            self._json(handler, 422, {"message": str(exc)})
            return
        if result is None:
            self._unsupported(handler)
        else:
            self._json(handler, *result)

    def _unsupported(self, handler: BaseHTTPRequestHandler) -> None:
        request = f"{handler.command} {urlsplit(handler.path).path}"
        with sqlite3.connect(self._database) as connection:
            connection.execute("INSERT INTO unsupported VALUES(?)", (request,))
        self._json(handler, 501, {"message": f"unsupported_request: {request}"})

    def _git_request(
        self, handler: BaseHTTPRequestHandler, path: str, query: str, body: bytes
    ) -> None:
        env = dict(os.environ)
        env.update(
            {
                "GIT_PROJECT_ROOT": str(self._git_root),
                "GIT_HTTP_EXPORT_ALL": "1",
                "PATH_INFO": path,
                "QUERY_STRING": query,
                "REQUEST_METHOD": handler.command,
                "CONTENT_TYPE": handler.headers.get("Content-Type", ""),
                "CONTENT_LENGTH": str(len(body)),
                "REMOTE_USER": "example-app",
                "REMOTE_ADDR": handler.client_address[0],
                "HTTP_GIT_PROTOCOL": handler.headers.get("Git-Protocol", ""),
            }
        )
        output = self._run("git", "http-backend", input_=body, env=env)
        raw_headers, _, content = output.partition(b"\r\n\r\n")
        headers = [
            line.decode().split(":", 1) for line in raw_headers.split(b"\r\n") if b":" in line
        ]
        status = next(
            (int(value.strip().split()[0]) for key, value in headers if key == "Status"), 200
        )
        handler.send_response(status)
        for key, value in headers:
            if key != "Status":
                handler.send_header(key, value.strip())
        handler.send_header("Content-Length", str(len(content)))
        handler.end_headers()
        handler.wfile.write(content)

    def _rest(
        self, method: str, path: str, query: dict[str, list[str]], body: Any
    ) -> tuple[int, Any] | None:
        if method == "POST" and path == "/app/installations/5501/access_tokens":
            if body.get("repositories") != ["acme-bot"]:
                return 422, {"message": "token must be scoped to acme-bot"}
            token = "example-installation-token"
            with sqlite3.connect(self._database) as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO tokens VALUES(?,?)", (token, json.dumps(PERMISSIONS))
                )
            return 201, {
                "token": token,
                "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "permissions": PERMISSIONS,
            }
        prefix = f"/repos/{REPOSITORY}"
        if path != prefix and not path.startswith(prefix + "/"):
            return None
        tail = path[len(prefix) :].strip("/")
        if method == "GET" and not tail:
            return 200, self._repository_row()
        if method == "GET" and tail == "installation":
            return 200, {
                "id": 5501,
                "app_id": 51,
                "permissions": PERMISSIONS,
                "account": {"login": "acme-corp"},
            }
        if method == "GET" and tail == "collaborators/octocat/permission":
            return 200, {"permission": "write", "user": HUMAN}
        if method == "GET" and tail.startswith("branches/"):
            branch = tail.removeprefix("branches/")
            return 200, {"name": branch, "commit": {"sha": self._ref(branch)}}
        if method == "GET" and tail.startswith("git/ref/heads/"):
            branch = tail.removeprefix("git/ref/heads/")
            return 200, {
                "ref": f"refs/heads/{branch}",
                "object": {"sha": self._ref(branch), "type": "commit"},
            }
        if method == "GET" and tail.startswith("commits/"):
            if tail.endswith("/check-runs"):
                runs = self._checks()
                return 200, {"total_count": len(runs), "check_runs": self._page(runs, query)}
            if tail.endswith("/status") or tail.endswith("/statuses"):
                now = _date(self.recording["epoch"]) + timedelta(seconds=self._seconds())
                statuses = [
                    row for row in self.recording["statuses"] if _date(row["created_at"]) <= now
                ]
                if tail.endswith("/statuses"):
                    return 200, self._page(statuses, query)
                state = (
                    "pending"
                    if not statuses or any(row["state"] == "pending" for row in statuses)
                    else "failure"
                    if any(row["state"] in {"failure", "error"} for row in statuses)
                    else "success"
                )
                return 200, {"state": state, "statuses": statuses, "total_count": len(statuses)}
        if method == "GET" and tail.startswith("check-runs/") and tail.endswith("/annotations"):
            return 200, []
        if tail == "issues/comments" and method == "GET":
            return 200, self._page(self._rows("comment"), query)
        if tail.startswith("issues/comments/"):
            identifier = int(tail.split("/")[-1])
            comment = self._get("comment", identifier)
            if comment is None:
                return 404, {"message": "Not Found"}
            if method == "PATCH":
                comment.update(body=body["body"], updated_at=_utc())
                self._put("comment", identifier, comment)
            if method in {"GET", "PATCH"}:
                return 200, comment
        if tail == "issues":
            if method == "GET":
                issues = self._rows("issue")
                labels = set(query.get("labels", [""])[0].split(",")) - {""}
                state = query.get("state", ["open"])[0]
                return 200, self._page(
                    [
                        row
                        for row in issues
                        if (state == "all" or row["state"] == state)
                        and labels <= {label["name"] for label in row["labels"]}
                    ],
                    query,
                )
            if method == "POST":
                number = max(row["number"] for row in self._rows("issue")) + 1
                issue = self._issue(
                    number, body["title"], body.get("body", ""), body.get("labels", [])
                )
                issue.update(user=BOT, performed_via_github_app={"id": 51})
                self._put("issue", number, issue)
                for label in issue["labels"]:
                    self._label_event(number, label["name"], "labeled", BOT)
                return 201, issue
        if tail.startswith("issues/"):
            parts = tail.split("/")
            number = int(parts[1])
            issue = self._get("issue", number)
            if issue is None:
                return 404, {"message": "Not Found"}
            suffix = "/".join(parts[2:])
            if not suffix and method in {"GET", "PATCH"}:
                if method == "PATCH":
                    issue.update(
                        {
                            key: value
                            for key, value in body.items()
                            if key in {"title", "body", "state"}
                        }
                    )
                    self._put("issue", number, issue)
                return 200, issue
            if suffix == "events" and method == "GET":
                return 200, self._page(
                    [row for row in self._rows("event") if row["issue_number"] == number], query
                )
            if suffix == "labels" and method in {"GET", "POST", "PUT"}:
                if method != "GET":
                    labels = body["labels"] if isinstance(body, dict) else body
                    previous = {label["name"] for label in issue["labels"]}
                    existing = (
                        [] if method == "PUT" else [label["name"] for label in issue["labels"]]
                    )
                    issue["labels"] = [
                        {"name": label} for label in dict.fromkeys(existing + labels)
                    ]
                    self._put("issue", number, issue)
                    current = {label["name"] for label in issue["labels"]}
                    for label in sorted(current - previous):
                        self._label_event(number, label, "labeled", BOT)
                    for label in sorted(previous - current):
                        self._label_event(number, label, "unlabeled", BOT)
                return 200, issue["labels"]
            if suffix.startswith("labels/") and method == "DELETE":
                removed = suffix.removeprefix("labels/")
                was_present = any(label["name"] == removed for label in issue["labels"])
                issue["labels"] = [
                    label
                    for label in issue["labels"]
                    if label["name"] != suffix.removeprefix("labels/")
                ]
                self._put("issue", number, issue)
                if was_present:
                    self._label_event(number, removed, "unlabeled", BOT)
                return 200, issue["labels"]
            if suffix == "comments":
                if method == "GET":
                    return 200, self._page(
                        [row for row in self._rows("comment") if row["issue_number"] == number],
                        query,
                    )
                if method == "POST":
                    identifier = max((row["id"] for row in self._rows("comment")), default=0) + 1
                    comment = {
                        "id": identifier,
                        "body": body["body"],
                        "issue_number": number,
                        "user": BOT,
                        "performed_via_github_app": {"id": 51},
                        "created_at": _utc(),
                        "updated_at": _utc(),
                        "issue_url": f"{self.base_url}{prefix}/issues/{number}",
                        "html_url": (
                            f"{self.base_url}/{REPOSITORY}/issues/{number}"
                            f"#issuecomment-{identifier}"
                        ),
                    }
                    self._put("comment", identifier, comment)
                    return 201, comment
        if tail == "pulls/comments" and method == "GET":
            return 200, []
        if tail == "pulls":
            if method == "GET":
                pulls = [self._fresh_pull(row) for row in self._rows("pull")]
                for key in ("state", "head", "base"):
                    if key in query and query[key][0] != "all":
                        value = query[key][0].split(":")[-1]
                        pulls = [
                            row
                            for row in pulls
                            if (row[key] if key == "state" else row[key]["ref"]) == value
                        ]
                return 200, self._page(pulls, query)
            if method == "POST":
                number = max((row["id"] for row in self._rows("pull")), default=0) + 1
                repository = self._repository_row()
                pull = {
                    "id": number,
                    "number": number,
                    "state": "open",
                    "merged": False,
                    "title": body["title"],
                    "body": body.get("body", ""),
                    "user": BOT,
                    "html_url": f"{self.base_url}/{REPOSITORY}/pull/{number}",
                    "head": {"ref": body["head"].split(":")[-1], "repo": repository},
                    "base": {"ref": body["base"], "repo": repository},
                }
                pull = self._fresh_pull(pull)
                self._put("pull", number, pull)
                return 201, pull
        if tail.startswith("pulls/"):
            parts = tail.split("/")
            number = int(parts[1])
            if len(parts) == 3 and parts[2] in {"comments", "reviews"} and method == "GET":
                return 200, []
            pull = self._get("pull", number)
            if pull is None:
                return 404, {"message": "Not Found"}
            if len(parts) == 3 and parts[2] == "files" and method == "GET":
                fresh = self._fresh_pull(pull)
                files = (
                    self._run(
                        "git",
                        "--git-dir",
                        str(self._repository),
                        "diff",
                        "--name-only",
                        fresh["base"]["sha"],
                        fresh["head"]["sha"],
                    )
                    .decode()
                    .splitlines()
                )
                return 200, self._page(
                    [{"filename": name, "status": "modified"} for name in files], query
                )
            if len(parts) == 2 and method in {"GET", "PATCH"}:
                if method == "PATCH":
                    pull.update(
                        {
                            key: value
                            for key, value in body.items()
                            if key in {"title", "body", "state"}
                        }
                    )
                    self._put("pull", number, pull)
                return 200, self._fresh_pull(pull)
        return None

    def _fresh_pull(self, row: dict[str, Any]) -> dict[str, Any]:
        row = dict(row)
        for key in ("head", "base"):
            row[key] = {**row[key], "sha": self._ref(row[key]["ref"])}
        return row

    @staticmethod
    def _page(rows: list[Any], query: dict[str, list[str]]) -> list[Any]:
        size = min(int(query.get("per_page", ["30"])[0]), 100)
        offset = (int(query.get("page", ["1"])[0]) - 1) * size
        return rows[offset : offset + size]


def capture(repository: str, pull_request: int, output: Path) -> None:
    """Capture only public upstream lifecycle metadata through authenticated gh."""
    if repository != "curie-eng/curie":
        raise ValueError("capture accepts only the public curie-eng/curie repository")

    def api(path: str, paginate: bool = False) -> Any:
        command = ["gh", "api", path]
        if paginate:
            command += ["--paginate", "--slurp"]
        return json.loads(
            subprocess.run(command, capture_output=True, text=True, check=True, timeout=120).stdout
        )

    pull = api(f"repos/{repository}/pulls/{pull_request}")
    sha = pull["head"]["sha"]
    pages = api(f"repos/{repository}/commits/{sha}/check-runs?per_page=100&filter=latest", True)
    checks = [
        {
            key: run[key]
            for key in ("id", "name", "status", "conclusion", "started_at", "completed_at")
        }
        | {"app": {"slug": run["app"]["slug"]}}
        for page in pages
        for run in page["check_runs"]
    ]
    if not checks or any(not row["started_at"] or not row["completed_at"] for row in checks):
        raise ValueError("capture requires completed checks with lifecycle timestamps")
    status_pages = api(f"repos/{repository}/commits/{sha}/statuses?per_page=100", True)
    statuses = [
        {key: row[key] for key in ("id", "context", "state", "created_at", "updated_at")}
        for page in status_pages
        for row in page
    ]
    recording = {
        "version": 1,
        "source": {
            "repository": repository,
            "pull_request": pull_request,
            "head_sha": sha,
            "captured_at": _utc(),
            "method": "completed-check-lifecycle",
        },
        "epoch": min(row["started_at"] for row in checks),
        "check_runs": checks,
        "statuses": statuses,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(recording, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Serve recorded GitHub behavior over trusted TLS")
    serve.add_argument("--root", type=Path, required=True)
    serve.add_argument(
        "--recording", type=Path, default=Path(__file__).parent / "recordings/curie-pr-3400.json"
    )
    serve.add_argument("--host", default="127.0.0.1", help="Advertised DNS name or IP address")
    serve.add_argument("--bind", help="Listening IP address, defaults to the advertised host")
    serve.add_argument("--port", type=int, default=0)
    record = commands.add_parser(
        "capture", help="Capture safe check lifecycle metadata from a public pull request"
    )
    record.add_argument("--repository", default="curie-eng/curie")
    record.add_argument("--pull-request", type=int, required=True)
    record.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "capture":
            capture(args.repository, args.pull_request, args.output)
            return 0
        stub = GithubStub(
            args.root,
            json.loads(args.recording.read_text()),
            args.host,
            args.port,
            bind=args.bind,
        )
        stop = threading.Event()
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, lambda _signal, _frame: stop.set())
        try:
            stub.start()
            stub._real_start = time.monotonic()
            print(
                json.dumps(
                    {
                        "base_url": stub.base_url,
                        "clone_url": stub.clone_url,
                        "ca_file": str(stub.ca_file),
                    }
                ),
                flush=True,
            )
            while not stop.wait(0.2) and not stub.unknown_requests:
                pass
        finally:
            stub.close()
        return 0
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"github-stub: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())

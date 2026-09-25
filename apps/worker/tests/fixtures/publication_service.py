"""Owned Git and GitHub API fixture for the publication cluster proof."""

from __future__ import annotations

import argparse
import json
import ssl
import subprocess
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

REPO_FULL_NAME = "acme-corp/acme-bot"
DEFAULT_BRANCH = "main"
AUTHORIZATION = "Bearer fixture-token"
PULL_NUMBER = 1
PULL_URL = f"https://github.com/{REPO_FULL_NAME}/pull/{PULL_NUMBER}"


def _git(*args: str, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return completed.stdout.strip()


class FixtureState:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.bare_repo = root / "acme-bot.git"
        self.lock = threading.RLock()
        self.pull: dict[str, Any] | None = None
        self.base_sha = self._initialize_repository()

    def _initialize_repository(self) -> str:
        work = self.root / "seed"
        self.root.mkdir(parents=True, exist_ok=True)
        _git("init", f"--initial-branch={DEFAULT_BRANCH}", str(work))
        _git("config", "user.name", "Publication Fixture", cwd=work)
        _git("config", "user.email", "fixture@example.com", cwd=work)
        (work / "README.md").write_text("base\n")
        _git("add", "README.md", cwd=work)
        _git("commit", "-m", "Initial fixture commit", cwd=work)
        _git("init", "--bare", str(self.bare_repo))
        _git(
            "--git-dir",
            str(self.bare_repo),
            "config",
            "daemon.receivepack",
            "true",
        )
        _git("remote", "add", "origin", str(self.bare_repo), cwd=work)
        _git("push", "origin", f"HEAD:refs/heads/{DEFAULT_BRANCH}", cwd=work)
        _git(
            "--git-dir",
            str(self.bare_repo),
            "symbolic-ref",
            "HEAD",
            f"refs/heads/{DEFAULT_BRANCH}",
        )
        return self.ref(DEFAULT_BRANCH) or ""

    def ref(self, branch: str) -> str | None:
        completed = subprocess.run(
            [
                "git",
                "--git-dir",
                str(self.bare_repo),
                "rev-parse",
                "--verify",
                f"refs/heads/{branch}",
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
        return completed.stdout.strip() if completed.returncode == 0 else None

    def pull_payload(self) -> dict[str, Any] | None:
        with self.lock:
            if self.pull is None:
                return None
            branch = str(self.pull["head"])
            head_sha = self.ref(branch)
            if head_sha is None:
                return None
            return {
                "number": PULL_NUMBER,
                "html_url": PULL_URL,
                "title": self.pull["title"],
                "body": self.pull["body"],
                "state": "open",
                "merged": False,
                "merged_at": None,
                "head": {
                    "ref": branch,
                    "sha": head_sha,
                    "repo": {"full_name": REPO_FULL_NAME},
                },
                "base": {
                    "ref": DEFAULT_BRANCH,
                    "sha": self.base_sha,
                    "repo": {"full_name": REPO_FULL_NAME},
                },
            }

    def create_pull(self, payload: dict[str, Any]) -> dict[str, Any]:
        head = payload.get("head")
        if not isinstance(head, str) or self.ref(head) is None:
            raise ValueError("pull request head does not exist")
        if payload.get("base") != DEFAULT_BRANCH:
            raise ValueError("pull request base is not main")
        title = payload.get("title")
        body = payload.get("body")
        if not isinstance(title, str) or not isinstance(body, str):
            raise ValueError("pull request metadata is invalid")
        with self.lock:
            if self.pull is None:
                self.pull = {"head": head, "title": title, "body": body}
            elif self.pull != {"head": head, "title": title, "body": body}:
                raise ValueError("a different pull request already exists")
        result = self.pull_payload()
        if result is None:
            raise ValueError("pull request head disappeared")
        return result


class FixtureHandler(BaseHTTPRequestHandler):
    server: FixtureServer

    def log_message(self, format: str, *args: object) -> None:
        print(f"fixture-api {self.command} {urlsplit(self.path).path}", flush=True)

    def _json(self, status: HTTPStatus, payload: object) -> None:
        data = json.dumps(payload, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _authorized(self) -> bool:
        if self.headers.get("Authorization") == AUTHORIZATION:
            return True
        self._json(HTTPStatus.UNAUTHORIZED, {"message": "fixture authorization required"})
        return False

    def _pulls_path(self) -> str:
        return f"/repos/{REPO_FULL_NAME}/pulls"

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return
        if not self._authorized():
            return
        repo_path = f"/repos/{REPO_FULL_NAME}"
        if parsed.path == repo_path:
            self._json(
                HTTPStatus.OK,
                {"full_name": REPO_FULL_NAME, "default_branch": DEFAULT_BRANCH},
            )
            return
        ref_prefix = f"{repo_path}/git/ref/heads/"
        if parsed.path.startswith(ref_prefix):
            branch = unquote(parsed.path[len(ref_prefix) :])
            sha = self.server.state.ref(branch)
            if sha is None:
                self._json(HTTPStatus.NOT_FOUND, {"message": "ref not found"})
            else:
                self._json(
                    HTTPStatus.OK,
                    {"ref": f"refs/heads/{branch}", "object": {"sha": sha}},
                )
            return
        if parsed.path == self._pulls_path():
            query = parse_qs(parsed.query)
            requested_head = (query.get("head") or [""])[0]
            pull = self.server.state.pull_payload()
            expected_head = (
                f"acme-corp:{pull['head']['ref']}" if pull is not None else None
            )
            rows = [pull] if pull is not None and requested_head == expected_head else []
            self._json(HTTPStatus.OK, rows)
            return
        if parsed.path == f"{self._pulls_path()}/{PULL_NUMBER}":
            pull = self.server.state.pull_payload()
            if pull is None:
                self._json(HTTPStatus.NOT_FOUND, {"message": "pull request not found"})
            else:
                self._json(HTTPStatus.OK, pull)
            return
        if parsed.path == "/__fixture/state":
            pull = self.server.state.pull_payload()
            self._json(
                HTTPStatus.OK,
                {
                    "base_sha": self.server.state.base_sha,
                    "pull": pull,
                },
            )
            return
        self._json(HTTPStatus.NOT_FOUND, {"message": "fixture route not found"})

    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        if not self._authorized():
            return
        if parsed.path != self._pulls_path():
            self._json(HTTPStatus.NOT_FOUND, {"message": "fixture route not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 64_000:
                raise ValueError("pull request body size is invalid")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("pull request body is invalid")
            # Shapes follow the official GitHub create and list pull request APIs:
            # https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request
            # https://docs.github.com/en/rest/pulls/pulls#list-pull-requests
            created = self.server.state.create_pull(body)
        except ValueError as exc:
            self._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"message": str(exc)})
            return
        self._json(HTTPStatus.CREATED, created)


class FixtureServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], state: FixtureState) -> None:
        super().__init__(address, FixtureHandler)
        self.state = state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cert", type=Path, required=True)
    parser.add_argument("--key", type=Path, required=True)
    args = parser.parse_args()

    state = FixtureState(args.root)
    git_daemon = subprocess.Popen(
        [
            "git",
            "daemon",
            "--reuseaddr",
            "--export-all",
            "--enable=receive-pack",
            f"--base-path={args.root}",
            "--listen=0.0.0.0",
            "--port=9418",
            str(args.root),
        ]
    )
    server = FixtureServer(("0.0.0.0", 8443), state)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(args.cert, args.key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        git_daemon.terminate()
        git_daemon.wait(timeout=10)


if __name__ == "__main__":
    main()

"""Owned Git and GitHub API fixture for the publication cluster proof."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import ssl
import subprocess
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

REPO_FULL_NAME = "acme-corp/acme-bot"
DEFAULT_BRANCH = "main"
AUTHORIZATION = "Bearer fixture-token"
# Observed GitHubCodeHost._token contract: REST uses this Git Basic header.
# https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-as-a-github-app-installation
REST_AUTHORIZATION = "Basic " + base64.b64encode(b"x-access-token:fixture-token").decode("ascii")
PULL_NUMBER = 1


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
    def __init__(self, root: Path, html_base: str) -> None:
        self.root = root
        self.pull_url = f"{html_base.rstrip('/')}/{REPO_FULL_NAME}/pull/{PULL_NUMBER}"
        self.bare_repo = root / "acme-bot.git"
        self.lock = threading.RLock()
        self.pull: dict[str, Any] | None = None
        self.pull_creations = 0
        self.scenario: dict[str, Any] | None = None
        self.base_sha = self._initialize_repository()
        self.barriers = self.root / "barriers"
        self.barriers.mkdir()
        for stage in ("pre-receive", "post-receive"):
            hook = self.bare_repo / "hooks" / stage
            hook.write_text(
                "#!/usr/bin/env python3\n"
                "import hashlib, pathlib, sys, time\n"
                f"root = pathlib.Path({str(self.barriers)!r})\n"
                f"stage = {stage!r}\n"
                "for update in sys.stdin:\n"
                "    ref = update.split()[2]\n"
                "    gate = root / hashlib.sha256(ref.encode()).hexdigest() / stage\n"
                "    if not gate.exists():\n"
                "        continue\n"
                "    (gate / 'entered').touch()\n"
                "    deadline = time.monotonic() + 60\n"
                "    while not (gate / 'released').exists():\n"
                "        if time.monotonic() >= deadline:\n"
                "            sys.exit('fixture receive barrier timed out')\n"
                "        time.sleep(0.05)\n"
            )
            hook.chmod(0o755)

    def barrier(self, branch: str, action: str) -> dict[str, bool]:
        if not branch.startswith("curie/") or action not in {"arm", "before", "after", "state"}:
            raise ValueError("invalid receive barrier")
        root = self.barriers / hashlib.sha256(f"refs/heads/{branch}".encode()).hexdigest()
        for stage in ("pre-receive", "post-receive"):
            gate = root / stage
            if action == "arm":
                gate.mkdir(parents=True)
            elif action == {"pre-receive": "before", "post-receive": "after"}[stage]:
                (gate / "released").touch()
        return {
            "before": (root / "pre-receive" / "entered").exists(),
            "after": (root / "post-receive" / "entered").exists(),
        }

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
        _git("--git-dir", str(self.bare_repo), "config", "core.logAllRefUpdates", "true")
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
                "node_id": "PR_fixture_1",
                "html_url": self.pull_url,
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
                    "sha": self.ref(DEFAULT_BRANCH),
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
                self.pull_creations += 1
            elif self.pull != {"head": head, "title": title, "body": body}:
                raise ValueError("a different pull request already exists")
        result = self.pull_payload()
        if result is None:
            raise ValueError("pull request head disappeared")
        return result

    def commit(self, sha: str) -> dict[str, Any]:
        if len(sha) != 40 or any(char not in "0123456789abcdef" for char in sha):
            raise ValueError("invalid commit id")
        fields = _git(
            "--git-dir", str(self.bare_repo), "show", "--no-patch", "--format=%H%n%T%n%P%n%B", sha
        ).split("\n", 3)
        return {
            "sha": fields[0],
            "tree": {"sha": fields[1]},
            "parents": [{"sha": parent} for parent in fields[2].split()],
            "message": fields[3],
        }

    def begin_scenario(self, branch: str) -> dict[str, Any]:
        with self.lock:
            if self.scenario is not None:
                raise ValueError("a serialized fixture scenario is already active")
            if not branch.startswith("curie/publication-") or self.ref(branch) is not None:
                raise ValueError("scenario must own an absent publication branch")
            _git("check-ref-format", f"refs/heads/{branch}")
            original = self.ref(DEFAULT_BRANCH)
            assert original is not None
            work = self.root / "seed"
            _git("reset", "--hard", original, cwd=work)
            try:
                (work / "README.md").write_text("published by moved base proof\n")
                patch = _git("diff", "--binary", cwd=work) + "\n"
            finally:
                _git("reset", "--hard", original, cwd=work)
            self.scenario = {
                "id": uuid.uuid4().hex,
                "branch": branch,
                "original_base_sha": original,
                "pull": self.pull.copy() if self.pull else None,
                "pull_creations": self.pull_creations,
                "moves": 0,
            }
            self.pull = None
            return {
                "scenario_id": self.scenario["id"],
                "original_base_sha": original,
                "base_branch": DEFAULT_BRANCH,
                "patch": patch,
            }

    def _scenario(self, scenario_id: str) -> dict[str, Any]:
        if self.scenario is None or self.scenario["id"] != scenario_id:
            raise ValueError("fixture scenario ownership differs")
        return self.scenario

    def move_base(self, scenario_id: str, *, conflict: bool) -> dict[str, Any]:
        with self.lock:
            scenario = self._scenario(scenario_id)
            current = self.ref(DEFAULT_BRANCH)
            assert current is not None
            work = self.root / "seed"
            _git("reset", "--hard", current, cwd=work)
            scenario["moves"] += 1
            if conflict:
                path = work / "README.md"
                content = "conflicting base edit\n"
            else:
                path = work / ".github" / "workflows" / "fixture.yaml"
                content = f"name: Fixture {scenario['moves']}\non: workflow_dispatch\njobs: {{}}\n"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            _git("add", str(path.relative_to(work)), cwd=work)
            _git("commit", "-m", f"Move fixture base {scenario['moves']}", cwd=work)
            _git("push", "origin", f"HEAD:refs/heads/{DEFAULT_BRANCH}", cwd=work)
            return {
                "original_base_sha": scenario["original_base_sha"],
                "tip_sha": self.ref(DEFAULT_BRANCH),
                "base_branch": DEFAULT_BRANCH,
            }

    def inspect_scenario(self, scenario_id: str) -> dict[str, Any]:
        with self.lock:
            scenario = self._scenario(scenario_id)
            branch = scenario["branch"]
            head = self.ref(branch)
            commit = self.commit(head) if head else None
            diff = None
            pushes = 0
            files: dict[str, str] = {}
            if commit:
                assert head is not None, "a commit inspection requires an existing branch head"
                pushes = len(
                    _git(
                        "--git-dir",
                        str(self.bare_repo),
                        "reflog",
                        "show",
                        "--format=%H",
                        f"refs/heads/{branch}",
                    ).splitlines()
                )
                parent = commit["parents"][0]["sha"]
                diff = (
                    _git("--git-dir", str(self.bare_repo), "diff", "--binary", parent, head) + "\n"
                )
                for path in ("README.md", ".github/workflows/fixture.yaml"):
                    files[path] = _git("--git-dir", str(self.bare_repo), "show", f"{head}:{path}")
            return {
                "head_sha": head,
                "commit": commit,
                "diff": diff,
                "files": files,
                "pushes": pushes,
                "pull_creations": self.pull_creations - scenario["pull_creations"],
                "pull": self.pull_payload(),
                "base_sha": self.ref(DEFAULT_BRANCH),
            }

    def restore_scenario(self, scenario_id: str) -> dict[str, Any]:
        with self.lock:
            scenario = self._scenario(scenario_id)
            branch = scenario["branch"]
            if self.pull is not None and self.pull["head"] != branch:
                raise ValueError("a foreign pull request changed during the owned scenario")
            self.pull = scenario["pull"]
            current = self.ref(DEFAULT_BRANCH)
            assert current is not None
            _git(
                "--git-dir",
                str(self.bare_repo),
                "update-ref",
                f"refs/heads/{DEFAULT_BRANCH}",
                scenario["original_base_sha"],
                current,
            )
            head = self.ref(branch)
            if head is not None:
                _git(
                    "--git-dir",
                    str(self.bare_repo),
                    "update-ref",
                    "-d",
                    f"refs/heads/{branch}",
                    head,
                )
            _git("reset", "--hard", scenario["original_base_sha"], cwd=self.root / "seed")
            self.pull_creations = scenario["pull_creations"]
            self.scenario = None
            return {
                "base_sha": self.ref(DEFAULT_BRANCH),
                "head_sha": self.ref(branch),
                "restored": True,
            }


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

    def _authorized(self, path: str) -> bool:
        authorization = REST_AUTHORIZATION if path.startswith("/repos/") else AUTHORIZATION
        if self.headers.get("Authorization") == authorization:
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
        if not self._authorized(parsed.path):
            return
        repo_path = f"/repos/{REPO_FULL_NAME}"
        if parsed.path == repo_path:
            self._json(
                HTTPStatus.OK,
                {"id": 9001, "full_name": REPO_FULL_NAME, "default_branch": DEFAULT_BRANCH},
            )
            return
        commit_prefix = f"{repo_path}/git/commits/"
        if parsed.path.startswith(commit_prefix):
            # https://docs.github.com/en/rest/git/commits#get-a-commit-object
            sha = parsed.path[len(commit_prefix) :]
            if len(sha) != 40 or any(char not in "0123456789abcdef" for char in sha):
                self._json(HTTPStatus.BAD_REQUEST, {"message": "invalid commit"})
                return
            try:
                self._json(HTTPStatus.OK, self.server.state.commit(sha))
            except subprocess.CalledProcessError:
                self._json(HTTPStatus.NOT_FOUND, {"message": "commit not found"})
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
                    {"ref": f"refs/heads/{branch}", "object": {"type": "commit", "sha": sha}},
                )
            return
        if parsed.path == self._pulls_path():
            query = parse_qs(parsed.query)
            requested_head = (query.get("head") or [""])[0]
            pull = self.server.state.pull_payload()
            expected_head = f"acme-corp:{pull['head']['ref']}" if pull is not None else None
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
                    "base_sha": self.server.state.ref(DEFAULT_BRANCH),
                    "pull": pull,
                },
            )
            return
        self._json(HTTPStatus.NOT_FOUND, {"message": "fixture route not found"})

    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        if not self._authorized(parsed.path):
            return
        if parsed.path == "/__fixture/barrier":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 4096:
                    raise ValueError("invalid barrier payload")
                payload = json.loads(self.rfile.read(length))
                state = self.server.state.barrier(payload["branch"], payload["action"])
            except (KeyError, TypeError, ValueError):
                self._json(HTTPStatus.BAD_REQUEST, {"message": "invalid barrier payload"})
            else:
                self._json(HTTPStatus.OK, state)
            return
        fixture_operations = {
            "/__fixture/scenario",
            "/__fixture/move-base",
            "/__fixture/inspect",
            "/__fixture/restore",
        }
        if parsed.path != self._pulls_path() and parsed.path not in fixture_operations:
            self._json(HTTPStatus.NOT_FOUND, {"message": "fixture route not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 64_000:
                raise ValueError("pull request body size is invalid")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("pull request body is invalid")
            if parsed.path in fixture_operations:
                scenario_id = str(body.get("scenario_id", ""))
                if parsed.path == "/__fixture/scenario":
                    result = self.server.state.begin_scenario(str(body.get("branch", "")))
                elif parsed.path == "/__fixture/move-base":
                    result = self.server.state.move_base(
                        scenario_id, conflict=body.get("conflict") is True
                    )
                elif parsed.path == "/__fixture/inspect":
                    result = self.server.state.inspect_scenario(scenario_id)
                else:
                    result = self.server.state.restore_scenario(scenario_id)
                self._json(HTTPStatus.OK, result)
                return
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
    parser.add_argument("--html-base", required=True)
    args = parser.parse_args()

    state = FixtureState(args.root, args.html_base)
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

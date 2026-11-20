"""A Jira Cloud stand-in served over TLS for the scripted factory driver (#5791).

``serve`` runs ``forge_fakes.jira.JiraFake`` (the Starlette app the Jira adapter's
own tests use, unchanged) on uvicorn, with a server certificate signed by a CA
the caller passes, so an install that trusts that CA trusts the stub. One Jira
behaviour then holds for pytest and for a real install: basic auth enforcement,
the search JQL shape, paging caps and Jira's time format.

Two things are added around the fake, neither of them Jira:

1. The site's clock is the wall clock. The fake's own clock starts at a fixed
   instant and moves one second per change; a real install compares the
   ``created`` and ``updated`` times Jira reports with its own clock, so a stub
   behind a real install has to be on it.
2. A control surface under ``/_stub/``, bound to the run by a random token the
   caller passes in the environment (``CURIE_JIRA_STUB_CONTROL_TOKEN``) and
   sends back in ``X-Curie-Stub-Token``. A request without it is 403. It seeds
   people, issues, label changes and comments through the fake's own seeding
   methods (``add_user``, ``seed_issue``, ``add_label``, ``add_comment``), and
   ``GET /_stub/state`` returns the site as a scenario observes it from outside:
   every issue with its labels and comments (body as stored ADF, entity
   properties), each comment's body and properties as they changed over time,
   the request count per Jira operation, every request no Jira route
   served, and when the poll read each issue's comments.

On startup ``serve`` prints one JSON line, ``{"port": <port>}``, to stdout, and
nothing else there. SIGTERM or SIGINT stops it.

https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/ is the API
the fake replays; nothing here is a real site, account or token.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hmac
import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, MutableMapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "apps/api/tests") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "apps/api/tests"))

import uvicorn  # noqa: E402
from forge_fakes import jira as jira_fake  # noqa: E402
from starlette.applications import Starlette  # noqa: E402
from starlette.requests import Request  # noqa: E402
from starlette.responses import JSONResponse, Response  # noqa: E402
from starlette.routing import Route  # noqa: E402

TOKEN_ENV = "CURIE_JIRA_STUB_CONTROL_TOKEN"
TOKEN_HEADER = "X-Curie-Stub-Token"
CONTROL_PREFIX = "/_stub/"
COMMENT_LISTING = re.compile(r"/issue/([^/]+)/comment$")

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


def iso(ms: int) -> str:
    """``ms`` as a UTC instant with milliseconds, ``2026-11-20T05:47:12.123Z``."""

    moment = datetime.fromtimestamp(ms / 1000, UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class WallClockJira(jira_fake.JiraFake):
    """`forge_fakes.jira.JiraFake` on the wall clock, recording comment revisions.

    Every change is stamped with the current time in milliseconds, and two
    changes in the same millisecond stay one millisecond apart, so the site's
    order is the order things happened.
    """

    def __init__(self, **kwargs: Any) -> None:
        self._floor_ms = 0
        super().__init__(start_ms=wall_ms(), **kwargs)
        self.unrouted: list[str] = []
        self.history: list[dict[str, Any]] = []
        self.comment_reads: list[dict[str, str]] = []
        self._seen: dict[int, str] = {}

    @property  # type: ignore[override]
    def now_ms(self) -> int:
        return max(wall_ms(), self._floor_ms)

    @now_ms.setter
    def now_ms(self, value: int) -> None:
        self._floor_ms = max(self._floor_ms, value)

    def _tick(self) -> int:
        stamp = max(wall_ms(), self._floor_ms + 1)
        self._floor_ms = stamp
        return stamp

    def snapshot(self) -> None:
        """Record each comment whose body or properties changed since the last look.

        A status comment is edited in place, so its bodies over time are what
        place a line it once carried; Jira keeps no such history itself.
        """

        for issue in self.issues.values():
            for comment in issue.comments:
                state = json.dumps(
                    {"body": comment.body, "properties": comment.properties}, sort_keys=True
                )
                if self._seen.get(comment.id) == state:
                    continue
                self._seen[comment.id] = state
                self.history.append(
                    {
                        "comment_id": str(comment.id),
                        "issue_id": str(issue.id),
                        "at": iso(self.now_ms),
                        "body": copy.deepcopy(comment.body),
                        "properties": copy.deepcopy(comment.properties),
                    }
                )

    # Observation ---------------------------------------------------------

    def _person(self, user: jira_fake.JiraUser) -> dict[str, Any]:
        return {
            "account_id": user.account_id,
            "display_name": user.display_name,
            "account_type": user.account_type,
        }

    def state(self) -> dict[str, Any]:
        self.snapshot()
        return {
            "issues": [
                {
                    "id": str(issue.id),
                    "key": issue.key,
                    "summary": issue.summary,
                    "labels": list(issue.labels),
                    "status": issue.status,
                    "reporter": self._person(issue.reporter),
                    "creator": self._person(issue.creator),
                    "created": iso(issue.created_ms),
                    "comments": [
                        {
                            "id": str(comment.id),
                            "author": self._person(comment.author),
                            "body": copy.deepcopy(comment.body),
                            "properties": copy.deepcopy(comment.properties),
                            "created": iso(comment.created_ms),
                            "updated": iso(comment.updated_ms),
                        }
                        for comment in issue.comments
                    ],
                }
                for issue in sorted(self.issues.values(), key=lambda issue: issue.id)
            ],
            "history": copy.deepcopy(self.history),
            "requests": dict(Counter(request.kind for request in self.requests)),
            "unrouted": list(self.unrouted),
            "comment_reads": list(self.comment_reads),
        }

    def note_reads(self, served: list[jira_fake.Recorded]) -> None:
        """Record each served newest-first comment listing: the poll's comment read.

        The poll reads an issue's comments newest first back to its window
        (``JiraTracker._recent_comments``); a status comment lookup reads them
        oldest first. So a listing here is the intake having read the issue's
        comments, at this instant.
        """

        for request in served:
            found = COMMENT_LISTING.search(request.path)
            if (
                request.kind == "comments"
                and request.method == "GET"
                and request.status == 200
                and request.params.get("orderBy") == "-created"
                and found is not None
            ):
                self.comment_reads.append({"issue": found.group(1), "at": iso(self.now_ms)})


class StubApp:
    """The fake's Jira routes, plus the token-bound control routes under ``/_stub/``."""

    def __init__(self, site: WallClockJira, token: str) -> None:
        if not token:
            raise ValueError("the control token is empty")
        self.site = site
        self._token = token
        self.control = Starlette(
            routes=[
                Route("/_stub/users", self._users, methods=["POST"]),
                Route("/_stub/issues", self._issues, methods=["POST"]),
                Route("/_stub/issues/{issue}/labels", self._label, methods=["POST"]),
                Route("/_stub/issues/{issue}/comments", self._comment, methods=["POST"]),
                Route("/_stub/state", self._state, methods=["GET"]),
            ]
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.site.app(scope, receive, send)
            return
        path = str(scope.get("path", ""))
        if path.startswith(CONTROL_PREFIX):
            if not self._authorized(scope):
                response = JSONResponse({"error": "control token required"}, status_code=403)
                await response(scope, receive, send)
                return
            await self.control(scope, receive, send)
            return
        before = len(self.site.requests)
        await self.site.app(scope, receive, send)
        self.site.note_reads(self.site.requests[before:])
        if len(self.site.requests) == before:
            # No fake route took the request: not something Jira serves here.
            self.site.unrouted.append(f"{scope.get('method')} {path}")
        self.site.snapshot()

    def _authorized(self, scope: Scope) -> bool:
        wanted = TOKEN_HEADER.lower().encode()
        sent = next((value for name, value in scope.get("headers", []) if name == wanted), b"")
        return hmac.compare_digest(sent, self._token.encode())

    def _user(self, account_id: Any) -> jira_fake.JiraUser:
        user = self.site.users.get(str(account_id))
        if user is None:
            raise LookupError(f"unknown account {account_id!r}")
        return user

    def _issue_of(self, request: Request) -> jira_fake.JiraIssue:
        found = self.site._find(request.path_params["issue"])
        if found is None:
            raise LookupError("unknown issue")
        return found

    @staticmethod
    async def _body(request: Request) -> dict[str, Any]:
        try:
            body = await request.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            raise LookupError("the body must be a JSON object")
        return body

    async def _users(self, request: Request) -> Response:
        body = await self._body(request)
        account_id, name = body.get("account_id"), body.get("display_name")
        if not isinstance(account_id, str) or not isinstance(name, str) or not account_id:
            return JSONResponse({"error": "account_id and display_name"}, status_code=400)
        self.site.add_user(account_id, name, time_zone=str(body.get("time_zone") or "UTC"))
        return JSONResponse({"account_id": account_id}, status_code=201)

    async def _issues(self, request: Request) -> Response:
        try:
            body = await self._body(request)
            reporter = self._user(body.get("reporter"))
        except LookupError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        summary = body.get("summary")
        if not isinstance(summary, str) or not summary:
            return JSONResponse({"error": "summary"}, status_code=400)
        description = body.get("description")
        issue = self.site.seed_issue(
            summary,
            description if isinstance(description, str) else None,
            project=str(body.get("project") or "PROJ"),
            creator=reporter,
            reporter=reporter,
        )
        self.site.snapshot()
        return JSONResponse({"id": str(issue.id), "key": issue.key}, status_code=201)

    async def _label(self, request: Request) -> Response:
        try:
            body = await self._body(request)
            issue = self._issue_of(request)
            by = self._user(body.get("by"))
        except LookupError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        label = body.get("label")
        if not isinstance(label, str) or not label:
            return JSONResponse({"error": "label"}, status_code=400)
        history_id = self.site.add_label(issue.id, label, by)
        stamp = issue.history[-1]["created_ms"]
        return JSONResponse({"history_id": history_id, "at": iso(stamp)}, status_code=201)

    async def _comment(self, request: Request) -> Response:
        try:
            body = await self._body(request)
            issue = self._issue_of(request)
            author = self._user(body.get("author"))
        except LookupError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        text = body.get("text")
        if not isinstance(text, str) or not text:
            return JSONResponse({"error": "text"}, status_code=400)
        comment_id = self.site.add_comment(issue.id, author, text)
        self.site.snapshot()
        created = self.site.comment(comment_id).created_ms
        return JSONResponse({"id": comment_id, "at": iso(created)}, status_code=201)

    async def _state(self, request: Request) -> Response:
        return JSONResponse(self.site.state())


# Certificates --------------------------------------------------------------------


def _san(host: str) -> str:
    try:
        return f"IP:{ipaddress.ip_address(host)}"
    except ValueError:
        return f"DNS:{host}"


def issue_certificate(workdir: Path, host: str, ca_cert: Path, ca_key: Path) -> tuple[Path, Path]:
    """A server key and certificate for ``host``, signed by the caller's CA."""

    workdir.mkdir(parents=True, exist_ok=True)
    key, csr, cert = workdir / "server.key", workdir / "server.csr", workdir / "server.pem"
    extension = workdir / "server.ext"

    def run(*argv: str) -> None:
        subprocess.run(argv, check=True, capture_output=True, timeout=60)

    run(
        "openssl",
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-keyout",
        str(key),
        "-out",
        str(csr),
    )
    key.chmod(0o600)
    extension.write_text(
        f"subjectAltName={_san(host)},IP:127.0.0.1,DNS:localhost\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
    )
    run(
        "openssl",
        "x509",
        "-req",
        "-in",
        str(csr),
        "-CA",
        str(ca_cert),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-CAserial",
        str(workdir / "ca.srl"),
        "-days",
        "2",
        "-out",
        str(cert),
        "-extfile",
        str(extension),
    )
    return key, cert


# Serving -------------------------------------------------------------------------


def build_site(host: str) -> WallClockJira:
    """The site behind the stub. ``host`` is the address its clients name.

    The fake answers ``/_edge/tenant_info`` only on its site host (Jira Cloud
    serves it on the site, never the API gateway), and here that is the
    address the stub is reached at.
    """

    jira_fake.SITE_HOST = host
    return WallClockJira()


async def _serve(server: uvicorn.Server, sock: socket.socket) -> None:
    task = asyncio.create_task(server.serve(sockets=[sock]))
    while not server.started:
        if task.done():
            await task
            raise RuntimeError("the Jira stub exited during startup")
        await asyncio.sleep(0.02)
    print(json.dumps({"port": sock.getsockname()[1]}), flush=True)
    await task


def serve(args: argparse.Namespace) -> int:
    token = os.environ.get(TOKEN_ENV, "")
    if not token:
        print(f"jira-stub: {TOKEN_ENV} is required", file=sys.stderr)
        return 2
    key, cert = issue_certificate(args.workdir, args.host, args.ca_cert, args.ca_key)
    app = StubApp(build_site(args.host), token)
    family = socket.AF_INET6 if ":" in args.host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((args.host, args.port))
    sock.listen(128)
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            lifespan="off",
            log_level="warning",
            access_log=False,
            ssl_keyfile=str(key),
            ssl_certfile=str(cert),
        )
    )
    try:
        asyncio.run(_serve(server, sock))
    finally:
        sock.close()
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("serve", help="serve the Jira fake over TLS until SIGTERM")
    run.add_argument("--host", required=True, help="the IP address to bind and to certify")
    run.add_argument("--port", type=int, default=0, help="0 picks a free port")
    run.add_argument("--ca-cert", type=Path, required=True)
    run.add_argument("--ca-key", type=Path, required=True)
    run.add_argument("--workdir", type=Path, required=True, help="for the server key")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    return serve(args)


if __name__ == "__main__":
    raise SystemExit(main())

"""Hermetic contract for the live Slack kickoff driver (#5380, ADR 0268 row 13).

The live proof needs a throwaway cluster, an isolated Slack test app, a
dedicated personal grant and an owned forge fixture, so it is proved by running
``personal_slack_kickoff.py run``. These tests pin what must hold around it
without any of those: every refusal fires through the real ``main`` before any
mutation, reservations persist before a paid start and unknown usage refuses
the next, the watchdog and the paid-start fence, the judge's refusal of missing
evidence, the read-back thread matcher, the grant probe's one capped turn, and
that no held secret reaches argv, logs, output or the ledger.

Only the Slack Web API (``FakeSlack``) and the command-line tools the driver
runs are substituted. The tools are stub executables that record their argv
and answer every read as empty, so a test can show which commands a refusal
issued; nothing is applied anywhere. The ledger is the real SQLite file and
the harvester is the real in-pod program. Slack shapes follow the method docs
cited on ``FakeSlack``; Kubernetes, registry and CLI shapes are cited where
they are built. No value is a real credential or identifier.
"""

from __future__ import annotations

import ast
import base64
import contextlib
import dataclasses
import datetime as dt
import hashlib
import http.server
import inspect
import json
import os
import queue
import random
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import personal_slack_kickoff as psk
import personal_slack_kickoff_support as sks
import pytest

CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
RUN_ID = "acme-kickoff-1"
T0 = 1_800_000_000.0
TEAM = "T0EXAMPLE1"
CHANNEL = "C0EXAMPLE1"
CURIE_USER, CURIE_BOT = "U0EXAMPLE1", "B0EXAMPLE1"
SENDER_USER, SENDER_BOT = "U0EXAMPLE2", "B0EXAMPLE2"
# Fake credentials. Each is held by the driver and must never leave it.
CURIE_BOT_MARKER = "xoxb-EXAMPLE_curie_bot_marker"
APP_MARKER = "xapp-EXAMPLE-app-level-marker"
SENDER_MARKER = "xoxb-EXAMPLE_sender_bot_marker"
GRANT_MARKER = "synthetic-kickoff-grant-marker"
MARKERS = (CURIE_BOT_MARKER, APP_MARKER, SENDER_MARKER, GRANT_MARKER)
CEILING = (
    "sessions=8,turns=96,session-turns=32,output-tokens=16384,"
    "session-output-tokens=8192,session-seconds=300,paid-retries=0"
)
MODEL_CEILING = f"{CEILING},model.diff-reviewer=claude-opus-5-5,model.plan-reviewer=claude-opus-5-5"
NAMESPACE = "psk-acme-test"
RELEASE = "psk-acme"
CONTEXT = "acme-test-context"
DOCKER_CONTEXT = "acme-docker"
REPO = "acme-corp/acme-fixture"
ISSUE_URL = f"https://github.com/{REPO}/issues/7"
SESSION = "5d0c7f3e-0000-4000-8000-00000000b001"
CONTINUATION = "5d0c7f3e-0000-4000-8000-00000000b002"
THIRD_SESSION = "5d0c7f3e-0000-4000-8000-00000000b003"
# Observed model ids in the fixture transcripts; the driver never pins a model.
IMPLEMENTER_MODEL = "claude-sonnet-5-5-20261001"
REVIEWER_MODEL = "claude-opus-5-5"
PLAN_KIND = "dark-factory:plan-reviewer"
DIFF_KIND = "dark-factory:diff-reviewer"
ISOLATION = {"confirmed": True, "team_id": TEAM, "curie_bot_user_id": CURIE_USER}
TENANT = "7a1b2c3d-0000-4000-8000-00000000c001"
PRINCIPAL = "7a1b2c3d-0000-4000-8000-00000000c002"
WORK_ITEM = "7a1b2c3d-0000-4000-8000-00000000c003"
DELIVERY = "Ev0EXAMPLE1"
KICKOFF_TS = "1800000001.000001"
REPLY_TS = ("1800000002.000002", "1800000003.000003", "1800000004.000004")
OTHER_ROOT_TS = "1800000008.000008"
OWNER = {"tenant_id": TENANT, "mode": "personal", "principal_id": PRINCIPAL, "generation": 3}
THREAD = {
    "channel_identity": "default",
    "channel": CHANNEL,
    "root_ts": KICKOFF_TS,
    "origin": "kickoff",
}
DRIVER = {"channel_id": CHANNEL, "bot_id": SENDER_BOT, "bot_user_id": SENDER_USER}
RECEIPT_CHECKS = dict.fromkeys(
    (
        "receipt_readable",
        "version_2",
        "channel",
        "root_is_message",
        "user_is_sender",
        "evidence_user_is_sender",
        "driver_bot_is_sender",
        "identity_default",
    ),
    True,
)


# --------------------------------------------------------------------------
# Private input files, transcripts, clocks
# --------------------------------------------------------------------------


def _private(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _private_file(path: Path, text: str) -> Path:
    path.write_text(text + "\n")
    os.chmod(path, 0o600)
    return path


def _private_json(path: Path, value: Mapping[str, Any]) -> Path:
    path.write_text(json.dumps(dict(value)))
    os.chmod(path, 0o600)
    return path


def _app_dir(directory: Path) -> Path:
    app = _private(directory / "app")
    (app / "app.json").write_text(json.dumps({"id": 42, "installation_id": 7, "repo": REPO}))
    (app / "app.pem").write_text("placeholder, never read before a mutation\n")
    return app


def _stamp(at: float) -> str:
    return dt.datetime.fromtimestamp(T0 + at, dt.UTC).isoformat().replace("+00:00", "Z")


def _assistant(message_id: str, model: str, tokens: int | None, at: float) -> dict[str, Any]:
    """A native CLI assistant record, the shape tests/test_personal_profile.py records."""

    usage = {} if tokens is None else {"input_tokens": 10, "output_tokens": tokens}
    return {
        "type": "assistant",
        "timestamp": _stamp(at),
        "sessionId": SESSION,
        "message": {
            "id": message_id,
            "model": model,
            "role": "assistant",
            "content": [{"type": "text", "text": "working"}],
            "usage": usage,
        },
    }


def _user(at: float) -> dict[str, Any]:
    """A native CLI user record: the prompt a session opens with, before any response."""

    return {
        "type": "user",
        "timestamp": _stamp(at),
        "sessionId": SESSION,
        "message": {"role": "user", "content": "work the issue"},
    }


def _turns(
    prefix: str, model: str, count: int, tokens: int | None, *, start: float, step: float = 1.0
) -> list[dict[str, Any]]:
    return [
        _assistant(f"{prefix}_{index:03d}", model, tokens, start + index * step)
        for index in range(count)
    ]


def _jsonl(records: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def _pod_files(
    main: Sequence[Mapping[str, Any]],
    reviewers: Sequence[tuple[str, str, Sequence[Mapping[str, Any]]]] = (),
    *,
    session: str = SESSION,
    config: str = "curie-claude-config-a1b2",
) -> dict[str, bytes]:
    """One runner pod's harvested transcripts, in the native CLI's layout.

    Main session: ``<config>/projects/<slug>/<session>.jsonl``. Each subagent:
    ``.../<session>/subagents/agent-<id>.jsonl`` with an ``agent-<id>.meta.json``
    sidecar naming its agent type (claude_agent_sdk/_internal/sessions.py).
    """

    project = f"{config}/projects/-workspace"
    files = {f"{project}/{session}.jsonl": _jsonl(main)}
    for agent, kind, records in reviewers:
        stem = f"{project}/{session}/subagents/agent-{agent}"
        files[f"{stem}.jsonl"] = _jsonl(records)
        files[f"{stem}.meta.json"] = json.dumps({"agentType": kind}).encode()
    return files


def _attempt_files() -> dict[str, bytes]:
    """A calm attempt: the implementer, its plan review, one diff review, the publish."""

    main = [
        *_turns("msg_impl", IMPLEMENTER_MODEL, 12, 150, start=0),
        *_turns("msg_pub", IMPLEMENTER_MODEL, 2, 90, start=120),
    ]
    return _pod_files(
        main,
        [
            ("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 3, 300, start=20)),
            ("diff01", DIFF_KIND, _turns("msg_diff", REVIEWER_MODEL, 3, 300, start=60)),
        ],
    )


class FakeClock(psk.Clock):
    """Time moves only when the driver waits, so no test sleeps."""

    def __init__(self, start: float = T0) -> None:
        self.at = start
        self.elapsed = 0.0
        self.waits: list[float] = []

    def now(self) -> float:
        return self.at

    def monotonic(self) -> float:
        return self.elapsed

    def wait(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.at += seconds
        self.elapsed += seconds


# --------------------------------------------------------------------------
# The Slack Web API substitute
# --------------------------------------------------------------------------


class FakeSlack:
    """The Slack Web API methods the driver calls, answered in their documented shapes.

    https://docs.slack.dev/reference/methods/auth.test , apps.connections.open,
    conversations.info, chat.postMessage, conversations.replies,
    conversations.history and chat.delete: every answer is a JSON object with
    ``ok`` and, on failure, an ``error`` code; a 429 carries ``Retry-After``. A
    bot deletes only its own messages (``cant_delete_message`` otherwise), and
    ``conversations.replies`` returns the parent first, then its replies.
    """

    def __init__(self) -> None:
        self.identities = {
            CURIE_BOT_MARKER: (TEAM, CURIE_USER, CURIE_BOT),
            SENDER_MARKER: (TEAM, SENDER_USER, SENDER_BOT),
        }
        self.members = {CURIE_BOT_MARKER: True, SENDER_MARKER: True}
        self.messages: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str]] = []
        self.bodies: list[bytes] = []
        self.rate_limit_once: set[str] = set()
        self.posted = 0

    def __call__(
        self, path: str, headers: Mapping[str, str], body: bytes
    ) -> tuple[int, Mapping[str, str], bytes]:
        method = path.removeprefix("/api/")
        token = str(headers.get("Authorization", "")).removeprefix("Bearer ")
        params = dict(urllib.parse.parse_qsl(body.decode()))
        identity = self.identities.get(token)
        self.bodies.append(body)
        self.calls.append((method, identity[1] if identity else "app"))
        if method in self.rate_limit_once:
            self.rate_limit_once.discard(method)
            return 429, {"retry-after": "3"}, b'{"ok": false, "error": "ratelimited"}'
        answer = self.answer(method, token, identity, params)
        return 200, {"content-type": "application/json"}, json.dumps(answer).encode()

    def answer(
        self,
        method: str,
        token: str,
        identity: tuple[str, str, str] | None,
        params: Mapping[str, str],
    ) -> dict[str, Any]:
        if method == "apps.connections.open":
            if token != APP_MARKER:
                return {"ok": False, "error": "not_allowed_token_type"}
            return {"ok": True, "url": "wss://wss-primary.slack.com/link/?ticket=EXAMPLE"}
        if identity is None:
            return {"ok": False, "error": "invalid_auth"}
        team, user, bot = identity
        if method == "auth.test":
            return {
                "ok": True,
                "url": "https://acme-example.slack.com/",
                "team": "Acme",
                "user": "acme-bot",
                "team_id": team,
                "user_id": user,
                "bot_id": bot,
                "is_enterprise_install": False,
            }
        if method == "conversations.info":
            member = self.members[token]
            return {"ok": True, "channel": {"id": params["channel"], "is_member": member}}
        if method == "chat.postMessage":
            self.posted += 1
            ts = f"{int(T0) + self.posted}.{self.posted:06d}"
            message = {"type": "message", "user": user, "bot_id": bot, "text": params["text"]}
            message["ts"] = ts
            if params.get("thread_ts"):
                message["thread_ts"] = params["thread_ts"]
            self.messages.append(message)
            return {"ok": True, "channel": params["channel"], "ts": ts, "message": message}
        if method == "conversations.replies":
            root = params["ts"]
            thread = [
                dict(item)
                for item in self.messages
                if item["ts"] == root or item.get("thread_ts") == root
            ]
            if not any(item["ts"] == root for item in thread):
                return {"ok": False, "error": "thread_not_found"}
            return {"ok": True, "messages": thread, "has_more": False, "response_metadata": {}}
        if method == "conversations.history":
            found = [dict(item) for item in self.messages if item["ts"] == params["latest"]]
            return {"ok": True, "messages": found, "has_more": False}
        if method == "chat.delete":
            found = [item for item in self.messages if item["ts"] == params["ts"]]
            if not found:
                return {"ok": False, "error": "message_not_found"}
            if found[0]["user"] != user:
                return {"ok": False, "error": "cant_delete_message"}
            self.messages.remove(found[0])
            return {"ok": True, "channel": params["channel"], "ts": params["ts"]}
        return {"ok": False, "error": "unknown_method"}

    def writes(self) -> list[tuple[str, str]]:
        return [item for item in self.calls if item[0] in {"chat.postMessage", "chat.delete"}]


# --------------------------------------------------------------------------
# Stub tools and the caller kit
# --------------------------------------------------------------------------

STUB_TOOLS = ("kubectl", "docker", "helm", "git", "openssl")
READ_ONLY = {("kubectl", "get"), ("docker", "ps"), ("git", "cat-file")}
_VALUED_FLAGS = {"--context", "--namespace", "--kube-context", "-C"}


def _verb(argv: Sequence[str]) -> str:
    """The subcommand of a tool's arguments, past its global flags and their values."""

    tokens = iter(argv)
    for token in tokens:
        if token in _VALUED_FLAGS:
            next(tokens, None)
        elif not token.startswith("-"):
            return token
    return ""


def _write_stubs(directory: Path, log: Path, *, existing: Sequence[str] = ()) -> None:
    """Executables that append their argv to ``log`` and answer every read as empty.

    ``kubectl get ... -o json`` answers an empty ``List`` (the shape ``kubectl
    get -o json`` prints), ``kubectl get namespace N -o name`` names an existing
    namespace, and ``docker run -d`` prints a container id. Nothing is applied.
    """

    rules: dict[str, list[dict[str, Any]]] = {
        "kubectl": [
            *(
                {"when": ["get", "namespace", name, "name"], "stdout": f"namespace/{name}\n"}
                for name in existing
            ),
            {
                "when": ["-o", "json"],
                "stdout": json.dumps({"apiVersion": "v1", "kind": "List", "items": []}),
            },
        ],
        "docker": [{"when": ["run", "-d"], "stdout": "0123456789ab\n"}],
    }
    directory.mkdir(exist_ok=True)
    for tool in STUB_TOOLS:
        path = directory / tool
        path.write_text(
            f"#!{sys.executable}\n"
            "import json, sys\n"
            f"RULES = json.loads({json.dumps(rules.get(tool, []))!r})\n"
            "argv = sys.argv[1:]\n"
            f"with open({str(log)!r}, 'a') as handle:\n"
            f"    handle.write(json.dumps({{'tool': {tool!r}, 'argv': argv}}) + '\\n')\n"
            "for rule in RULES:\n"
            "    if all(token in argv for token in rule['when']):\n"
            "        sys.stdout.write(rule.get('stdout', ''))\n"
            "        sys.exit(rule.get('exit', 0))\n"
        )
        path.chmod(0o755)


def _bind(make: Callable[[int], Any]) -> Any:
    """Bind inside ``--registry-port``'s range (1024 to 32767); ephemeral ports lie above."""

    for port in random.sample(range(20000, 30000), 200):
        try:
            return make(port)
        except OSError:
            continue
    raise RuntimeError("no free loopback port in range")


def _free_port() -> int:
    def probe(port: int) -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
        return port

    return int(_bind(probe))


FLAGS_BASE = ("--candidate", "--state-dir", "--run-id")
FLAGS_SLACK = (*psk.SLACK_FLAGS.values(), "--channel-file", "--isolation-file")
FLAGS_CLUSTER = (
    "--context",
    "--docker-context",
    "--registry-port",
    "--namespace",
    "--release",
    "--app-dir",
)
FLAGS_GRANT = ("--grant-file", "--grant-expires-at", "--approve-live")
FLAGS_ITEM = ("--item-cap", "--item-charged-before")
FLAGS = {
    "run": (*FLAGS_BASE, *FLAGS_CLUSTER, *FLAGS_SLACK, *FLAGS_GRANT, *FLAGS_ITEM),
    "--live": (*FLAGS_BASE, *FLAGS_CLUSTER, *FLAGS_SLACK, *FLAGS_GRANT, *FLAGS_ITEM),
    "rehearse": (*FLAGS_BASE, *FLAGS_CLUSTER, *FLAGS_SLACK),
    "preflight": (*FLAGS_BASE, "--app-dir", *FLAGS_SLACK, *FLAGS_GRANT),
    "slack-probe": (*FLAGS_BASE, *FLAGS_SLACK),
}
# Flags that take no value.
SWITCHES = {"--kickoff"}


@dataclasses.dataclass
class Kit:
    """Every caller input as a private file, the stub tools, the fake Slack and clock."""

    state: Path
    stubs: Path
    log: Path
    values: dict[str, str | None]
    environ: dict[str, str]
    slack: FakeSlack
    clock: FakeClock

    def run(self, command: str, flags: Sequence[str] | None = None) -> int:
        argv = [command]
        for flag in FLAGS[command] if flags is None else flags:
            value = self.values.get(flag)
            if flag in SWITCHES:
                argv.append(flag)
            elif value is not None:
                argv += [flag, value]
        return psk.main(argv, environ=self.environ, clock=self.clock, slack_transport=self.slack)

    def calls(self) -> list[tuple[str, list[str]]]:
        if not self.log.exists():
            return []
        entries = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [(entry["tool"], entry["argv"]) for entry in entries]

    def mutations(self) -> list[tuple[str, str]]:
        found = [(tool, _verb(argv)) for tool, argv in self.calls()]
        return [item for item in found if item not in READ_ONLY]


def _kit(tmp_path: Path) -> Kit:
    files = _private(tmp_path / "inputs")
    stubs = tmp_path / "stubs"
    log = tmp_path / "stub-calls.jsonl"
    _write_stubs(stubs, log)
    values: dict[str, str | None] = {
        "--candidate": CANDIDATE,
        "--state-dir": str(_private(tmp_path / "state")),
        "--run-id": RUN_ID,
        "--context": CONTEXT,
        "--docker-context": DOCKER_CONTEXT,
        "--registry-port": str(_free_port()),
        "--namespace": NAMESPACE,
        "--release": RELEASE,
        "--app-dir": str(_app_dir(files)),
        "--curie-bot-token-file": str(_private_file(files / "curie-bot", CURIE_BOT_MARKER)),
        "--app-token-file": str(_private_file(files / "app-token", APP_MARKER)),
        "--sender-token-file": str(_private_file(files / "sender-bot", SENDER_MARKER)),
        "--channel-file": str(_private_json(files / "channel.json", {"channel_id": CHANNEL})),
        "--isolation-file": str(_private_json(files / "isolation.json", ISOLATION)),
        "--grant-file": str(_private_file(files / "grant", GRANT_MARKER)),
        "--grant-expires-at": str(int(T0 + 3 * 3600)),
        "--approve-live": CEILING,
        "--item-cap": "32768",
        "--item-charged-before": "8607",
    }
    environ = {"PATH": str(stubs), "HOME": str(_private(tmp_path / "home"))}
    return Kit(
        state=tmp_path / "state",
        stubs=stubs,
        log=log,
        values=values,
        environ=environ,
        slack=FakeSlack(),
        clock=FakeClock(),
    )


def _output(capsys: pytest.CaptureFixture[str]) -> tuple[dict[str, Any], str]:
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    return json.loads(lines[-1]), captured.out + captured.err


def _live_starts(state: Path) -> tuple[int, int]:
    """Live starts and session rows in the ledger, read raw; zero when no ledger exists."""

    path = state / sks.LEDGER_FILE
    if not path.exists():
        return 0, 0
    with contextlib.closing(sqlite3.connect(path)) as db:
        starts = db.execute("SELECT COALESCE(SUM(live_starts), 0) FROM runs").fetchone()[0]
        sessions = db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    return int(starts), int(sessions)


# --------------------------------------------------------------------------
# A loopback registry fixture
# --------------------------------------------------------------------------


def _image(revision: str) -> tuple[bytes, bytes]:
    """Docker manifest v2 schema 2 plus its image config blob.

    https://distribution.github.io/distribution/spec/manifest-v2-2/ and the
    registry API at https://distribution.github.io/distribution/spec/api/ .
    """

    config = json.dumps(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {"Labels": {sks.REVISION_LABEL: revision}},
            "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "1" * 64]},
        }
    ).encode()
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "size": len(config),
                "digest": "sha256:" + hashlib.sha256(config).hexdigest(),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 32,
                    "digest": "sha256:" + "2" * 64,
                }
            ],
        }
    ).encode()
    return manifest, config


@contextlib.contextmanager
def _registry(
    images: Mapping[tuple[str, str], tuple[bytes, bytes]], *, in_range: bool = False
) -> Iterator[int]:
    """A loopback Registry HTTP API v2 fixture: ``/v2/``, manifests and config blobs.

    As registry:2.8.3 does, ``GET /v2/`` answers 200 ``{}`` and a manifest is
    served only when Accept names its media type.
    """

    manifests = {key: value[0] for key, value in images.items()}
    blobs = {
        "sha256:" + hashlib.sha256(config).hexdigest(): config for _m, config in images.values()
    }

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802 - http.server naming
            parts = self.path.split("/")
            body: bytes | None = None
            headers: dict[str, str] = {}
            if self.path == "/v2/":
                body = b"{}"
            elif len(parts) == 5 and parts[1] == "v2" and parts[3] == "manifests":
                accepted = [item.strip() for item in self.headers.get("Accept", "").split(",")]
                found = manifests.get((parts[2], parts[4]))
                media_type = json.loads(found).get("mediaType") if found else None
                if found and media_type in accepted:
                    body = found
                    headers = {
                        "Content-Type": str(media_type),
                        "Docker-Content-Digest": "sha256:" + hashlib.sha256(found).hexdigest(),
                    }
            elif len(parts) == 5 and parts[1] == "v2" and parts[3] == "blobs":
                body = blobs.get(parts[4])
            if body is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def serve(port: int) -> http.server.ThreadingHTTPServer:
        return http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)

    server = _bind(serve) if in_range else serve(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


# --------------------------------------------------------------------------
# 1. Every refusal fires through the real main before any mutation
# --------------------------------------------------------------------------

Setup = Callable[[Kit, contextlib.ExitStack[Any]], Sequence[str] | None]


def _value(flag: str, value: str | None) -> Setup:
    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
        kit.values[flag] = value

    return setup


def _environ(name: str, value: str) -> Setup:
    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
        kit.environ[name] = value

    return setup


def _chmod(flag: str, mode: int) -> Setup:
    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
        os.chmod(str(kit.values[flag]), mode)

    return setup


def _rewrite(flag: str, text: str) -> Setup:
    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
        _private_file(Path(str(kit.values[flag])), text)

    return setup


def _isolation(**changes: Any) -> Setup:
    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
        _private_json(Path(str(kit.values["--isolation-file"])), {**ISOLATION, **changes})

    return setup


def _same_bot_for_both(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
    kit.values["--sender-token-file"] = kit.values["--curie-bot-token-file"]


def _namespace_exists(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
    _write_stubs(kit.stubs, kit.log, existing=[NAMESPACE])


def _no_app_key(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
    (Path(str(kit.values["--app-dir"])) / "app.pem").unlink()


def _registry_busy(kit: Kit, stack: contextlib.ExitStack[Any]) -> None:
    kit.values["--registry-port"] = str(stack.enter_context(_registry({}, in_range=True)))


def _sender_not_in_channel(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
    kit.slack.members[SENDER_MARKER] = False


def _grant_flags(_kit: Kit, _stack: contextlib.ExitStack[Any]) -> Sequence[str]:
    return (*FLAGS["rehearse"], *FLAGS_GRANT)


def _rehearse_with(*extra: str, ceiling: str = CEILING) -> Setup:
    """``rehearse`` with ``extra`` flags (``--kickoff`` among them) and the given ceiling."""

    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> Sequence[str]:
        kit.values["--approve-live"] = ceiling
        return (*FLAGS["rehearse"], *extra)

    return setup


KICKOFF_FLAGS = ("--kickoff", "--approve-live", "--grant-expires-at")


def _prior(kind: str) -> Setup:
    def setup(kit: Kit, _stack: contextlib.ExitStack[Any]) -> None:
        ledger = sks.Ledger.open(kit.state, RUN_ID, scrubber=sks.Scrubber())
        try:
            ledger.ensure_run(CANDIDATE, phase=kind)
            ledger.mark_live_kind(kind)
        finally:
            ledger.close()

    return setup


REFUSALS = [
    pytest.param(
        "run", _environ(sks.GRANT_KEY, GRANT_MARKER), "service_credentials", id="ambient-grant"
    ),
    pytest.param("run", _environ("CURIE_FAKE_MODEL", "1"), "fake_mode", id="fake-mode"),
    pytest.param("run", _value("--candidate", CANDIDATE[:12]), "candidate_invalid", id="sha"),
    pytest.param("run", _chmod("--state-dir", 0o755), "state_dir_unsafe", id="state-dir"),
    pytest.param("run", _value("--release", "curie"), "release_reserved", id="release"),
    pytest.param("run", _value("--approve-live", None), "live_unapproved", id="unapproved"),
    pytest.param(
        "run",
        _value("--approve-live", CEILING.replace("sessions=8", "sessions=9")),
        "live_unapproved",
        id="above-approval",
    ),
    pytest.param(
        "run", _value("--item-charged-before", "16385"), "item_cap_exceeded", id="item-cap"
    ),
    pytest.param(
        "run",
        _value("--approve-live", CEILING.replace("paid-retries=0", "paid-retries=1")),
        "live_unapproved",
        id="paid-retry-approved",
    ),
    pytest.param(
        "run",
        _value("--approve-live", "turns=96,sessions=8," + CEILING.split(",", 2)[2]),
        "live_unapproved",
        id="not-canonical",
    ),
    pytest.param(
        "run",
        _value("--approve-live", CEILING.replace("sessions=8", "sessions=4")),
        "live_unapproved",
        id="sessions-below-roles",
    ),
    pytest.param("--live", _value("--approve-live", None), "live_unapproved", id="live-flag"),
    pytest.param("run", _chmod("--grant-file", 0o644), "grant_unsafe", id="grant-mode"),
    pytest.param(
        "run",
        _value("--grant-expires-at", str(int(T0 + 60))),
        "lifetime_insufficient",
        id="grant-lifetime",
    ),
    pytest.param(
        "run",
        _rewrite("--curie-bot-token-file", APP_MARKER),
        "slack_kit_invalid",
        id="bot-token-prefix",
    ),
    pytest.param("run", _same_bot_for_both, "slack_kit_invalid", id="sender-is-curie"),
    pytest.param("run", _isolation(confirmed=False), "isolation_unconfirmed", id="isolation-file"),
    pytest.param(
        "run",
        _isolation(curie_bot_user_id="U0EXAMPLE3"),
        "isolation_unconfirmed",
        id="isolation-names-another-app",
    ),
    pytest.param("run", _sender_not_in_channel, "slack_unavailable", id="sender-not-member"),
    pytest.param("run", _namespace_exists, "namespace_exists", id="namespace-exists"),
    pytest.param("run", _no_app_key, "app_kit_invalid", id="app-kit"),
    pytest.param("run", _value("--registry-port", "80"), "registry_port_invalid", id="port"),
    pytest.param("run", _registry_busy, "registry_port_busy", id="port-busy"),
    pytest.param("rehearse", _grant_flags, "arguments_invalid", id="rehearse-with-grant"),
    pytest.param(
        "rehearse",
        _rehearse_with("--approve-live"),
        "arguments_invalid",
        id="rehearse-approval-without-kickoff",
    ),
    pytest.param(
        "rehearse",
        _rehearse_with("--kickoff", "--grant-expires-at"),
        "live_unapproved",
        id="kickoff-without-approval",
    ),
    pytest.param(
        "rehearse",
        _rehearse_with(*KICKOFF_FLAGS, ceiling=CEILING.replace("sessions=8", "sessions=9")),
        "live_unapproved",
        id="kickoff-approval-malformed",
    ),
    pytest.param(
        "rehearse",
        _rehearse_with("--kickoff", "--approve-live"),
        "expiry_unknown",
        id="kickoff-without-window",
    ),
    pytest.param(
        "rehearse",
        _rehearse_with(*KICKOFF_FLAGS, "--grant-file"),
        "arguments_invalid",
        id="kickoff-with-grant-file",
    ),
    pytest.param("run", _prior("run"), "paid_retry_refused", id="second-live-start"),
    pytest.param("run", _prior("rehearse"), "rehearsal_run_id", id="rehearsal-run-id"),
    pytest.param("preflight", _chmod("--grant-file", 0o644), "grant_unsafe", id="preflight"),
]


@pytest.mark.parametrize(("command", "setup", "code"), REFUSALS)
def test_every_refusal_fires_before_any_mutation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, setup: Setup, code: str
) -> None:
    """Behavior: each refusal names its code and issues no mutating command or Slack write."""

    kit = _kit(tmp_path)
    with contextlib.ExitStack() as stack:
        flags = setup(kit, stack)
        exit_code = kit.run(command, flags)
    payload, text = _output(capsys)
    assert (exit_code, payload["error"]) == (sks.EXIT_REFUSED, code)
    assert kit.mutations() == []
    assert kit.slack.writes() == []
    assert _live_starts(kit.state) == (0, 0)
    # Codes and names only: no value of any held or ambient credential is printed.
    assert not any(marker in text for marker in MARKERS)


def test_valid_inputs_reach_the_first_mutation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Liveness for every refusal above: valid inputs pass them all and reach the registry.

    The stub ``docker run`` starts nothing, so the run fails at the registry's
    reachability wait with the approved ceiling already recorded, and the
    finalizer inventories everything absent.
    """

    kit = _kit(tmp_path)
    assert kit.run("run") == sks.EXIT_FAIL
    payload, _text = _output(capsys)
    assert payload == {
        "command": "run",
        "verdict": "fail",
        "phase": "registry",
        "cleanup_ok": True,
        "error": "step_failed",
        "detail": "the loopback registry is not reachable on 127.0.0.1",
    }
    assert kit.mutations() == [("docker", "run")]
    started = next(argv for tool, argv in kit.calls() if (tool, _verb(argv)) == ("docker", "run"))
    assert f"127.0.0.1:{kit.values['--registry-port']}:5000" in started
    assert psk.REGISTRY_IMAGE in started
    assert {"auth.test", "apps.connections.open", "conversations.info"} == {
        method for method, _who in kit.slack.calls
    }
    ledger = sks.Ledger.open_existing(kit.state, RUN_ID)
    try:
        budget = ledger.observations("budget")
        assert budget["approved_ceiling"] == CEILING
        assert sks.Ceiling.from_json(budget["ceiling"]) == sks.parse_ceiling(CEILING)
        assert ledger.sessions() == [] and ledger.run_row()["live_starts"] == 0
        slack = ledger.observations("slack")
        assert slack["isolation_confirmed"] is True and slack["sender_driver"] == DRIVER
        rows = ledger.cleanup_rows()
        assert {"kind": "step:remove-registry-container", "status": "ok", "detail": ""} in rows
        inventory = {
            row["kind"].removeprefix("inventory:"): row["status"]
            for row in rows
            if row["kind"].startswith("inventory:")
        }
        assert inventory == dict.fromkeys(sks._INVENTORY, "absent")
    finally:
        ledger.close()


def test_preflight_accepts_valid_inputs_offline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Liveness: preflight passes on valid inputs, reads only the local commit, calls no Slack."""

    kit = _kit(tmp_path)
    assert kit.run("preflight") == sks.EXIT_PASS
    payload, _text = _output(capsys)
    assert payload["verdict"] == "pass"
    assert payload["counts"] == {"reserved_sessions": len(sks.ATTEMPT_ROLES)}
    assert [(tool, _verb(argv)) for tool, argv in kit.calls()] == [("git", "cat-file")]
    assert kit.slack.calls == []
    ledger = sks.Ledger.open_existing(kit.state, RUN_ID)
    try:
        assert ledger.observations("preflight")["ceiling"] == sks.parse_ceiling(CEILING).as_json()
    finally:
        ledger.close()


def test_input_checks_accept_valid_inputs(tmp_path: Path) -> None:
    """Liveness for each borrowed and local input check the refusals above exercise."""

    sks.refuse_ambient_credentials({"PATH": "/usr/bin", "ANTHROPIC_API_KEY": ""})
    assert sks.require_candidate(CANDIDATE) == CANDIDATE
    assert sks.require_run_id(RUN_ID) == RUN_ID
    assert sks.require_release(RELEASE) == RELEASE
    assert sks.require_dns_name(NAMESPACE, "--namespace", limit=56) == NAMESPACE
    assert sks.require_registry("localhost:5999") == ("127.0.0.1", 5999)
    assert sks.require_registry_port("5999") == 5999
    state = _private(tmp_path / "state")
    assert sks.require_private_dir(state) == state
    assert sks.require_app_kit(_app_dir(tmp_path))["repo"] == REPO
    grant = _private_file(tmp_path / "grant", GRANT_MARKER)
    sks.check_grant_file(grant)
    assert sks.parse_expiry("2027-01-15T08:00:00Z") == T0
    assert sks.parse_expiry(str(int(T0))) == T0
    assert sks.require_lifetime(T0 + 3 * 3600, T0) == 3 * 3600
    channel = _private_json(tmp_path / "channel.json", {"channel_id": CHANNEL})
    assert sks.load_channel(channel) == CHANNEL
    isolation = _private_json(tmp_path / "isolation.json", ISOLATION)
    assert sks.load_isolation(isolation) == sks.Isolation(TEAM, CURIE_USER)
    token = sks.load_token(
        _private_file(tmp_path / "bot", CURIE_BOT_MARKER),
        flag="--curie-bot-token-file",
        prefix="xoxb-",
    )
    assert token.reveal() == CURIE_BOT_MARKER
    token.clear()
    # Values that reach SQL, Helm values or argv are validated by shape first.
    assert sks.require_slack_id(SENDER_USER, "user") == SENDER_USER
    assert sks.require_slack_id(KICKOFF_TS, "ts") == KICKOFF_TS
    assert sks.sql_uuid(PRINCIPAL.upper()) == PRINCIPAL
    with pytest.raises(sks.Refusal) as refused:
        sks.require_slack_id("U0EXAMPLE2'); --", "user")
    assert refused.value.code == "slack_unavailable"
    with pytest.raises(ValueError):
        sks.sql_uuid("'; DROP TABLE curie.identity_links; --")


# --------------------------------------------------------------------------
# 2. Reservations persist before a paid start; unknown usage refuses the next
# --------------------------------------------------------------------------


def _run_ledger(state: Path, run_id: str = RUN_ID, *, ceiling: str = CEILING) -> sks.Ledger:
    ledger = sks.Ledger.open(state, run_id, scrubber=sks.Scrubber())
    ledger.ensure_run(CANDIDATE, phase="run", now=T0)
    ledger.mark_live_kind("run")
    ledger.record_ceiling(ceiling, sks.parse_ceiling(ceiling))
    return ledger


def test_reservations_persist_before_a_paid_start_and_unknown_usage_refuses_the_next(
    tmp_path: Path,
) -> None:
    """Behavior: slots are durable before any start; unknown usage is charged in full and
    refuses every later reservation; a complete capture leaves room for the next start."""

    state = _private(tmp_path / "state")
    ledger = _run_ledger(state)
    with pytest.raises(sks.Refusal) as refused:
        ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    assert refused.value.code == "allowance_exhausted"  # no live start recorded yet
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
    # A second connection sees every slot before anything starts; usage is still unknown.
    reader = sks.Ledger.open_existing(state, RUN_ID)
    rows = reader.sessions()
    reader.close()
    assert [
        (r["role"], r["reserved_output"], r["reserved_turns"], r["output_tokens"]) for r in rows
    ] == [
        # The main implementer session keeps the lead share; the publish continuation's
        # slot splits the rest with the reviewers.
        (sks.IMPLEMENTER, 8192, 32, None),
        (sks.IMPLEMENTER, 2048, 16, None),
        (sks.PLAN_REVIEWER, 2048, 16, None),
        (sks.DIFF_REVIEWER, 2048, 16, None),
        (sks.DIFF_REVIEWER, 2048, 16, None),
    ]
    assert sum(r["reserved_output"] for r in rows) <= 16384
    assert sum(r["reserved_turns"] for r in rows) <= 96
    with pytest.raises(sks.Refusal) as refused:
        ledger.reserve([sks.IMPLEMENTER], now=T0 + 2)
    assert refused.value.code == "usage_unknown"  # the attempt's own usage is not known yet
    # An incomplete capture leaves every slot unknown, charged its full share.
    counts = sks.settle_attempt(ledger, reservations, None, complete=False)
    assert counts == {"settled": 0, "unknown": 5, "unreserved": 0}
    assert ledger.budget_state() == "unknown"
    assert [sks.charged(row) for row in ledger.sessions()] == [
        (8192, 32, 300.0),
        (2048, 16, 300.0),
        (2048, 16, 300.0),
        (2048, 16, 300.0),
        (2048, 16, 300.0),
    ]
    with pytest.raises(sks.Refusal) as refused:
        ledger.reserve([sks.IMPLEMENTER], now=T0 + 600)
    assert refused.value.code == "usage_unknown"
    with pytest.raises(sks.Refusal) as refused:
        ledger.begin_live(now=T0 + 600, grant_expires_at=T0 + 3 * 3600)
    assert refused.value.code == "paid_retry_refused"
    with pytest.raises(sks.Refusal) as refused:
        ledger.record_ceiling(CEILING.replace("turns=96", "turns=90"), sks.parse_ceiling(CEILING))
    assert refused.value.code == "live_unapproved"  # the recorded approval never changes
    ledger.close()

    # Liveness: a complete capture settles each slot from native usage.
    settled = _run_ledger(state, "acme-kickoff-2")
    settled.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    reservations = settled.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
    facts = sks.attempt_facts([_attempt_files()], not_before=T0 - 30)
    counts = sks.settle_attempt(settled, reservations, facts.sessions, complete=True)
    assert counts == {"settled": 5, "unknown": 0, "unreserved": 0}
    assert [
        (r["source"], r["output_tokens"], r["assistant_turns"]) for r in settled.sessions()
    ] == [
        ("native", 12 * 150 + 2 * 90, 14),
        ("not_started", 0, 0),  # no publish continuation ran
        ("native", 900, 3),
        ("native", 900, 3),
        ("not_started", 0, 0),
    ]
    assert settled.budget_state() == "ok"
    more = settled.reserve([sks.IMPLEMENTER], now=T0 + 600)
    assert [item.role for item in more] == [sks.IMPLEMENTER]
    settled.close()


# --------------------------------------------------------------------------
# 3. The watchdog: every response checked against the approved ceiling
# --------------------------------------------------------------------------

RESERVATIONS = [
    sks.Reservation(1, sks.IMPLEMENTER, 8192, 32, 300.0),
    sks.Reservation(2, sks.IMPLEMENTER, 2048, 16, 300.0),
    sks.Reservation(3, sks.PLAN_REVIEWER, 2048, 16, 300.0),
    sks.Reservation(4, sks.DIFF_REVIEWER, 2048, 16, 300.0),
    sks.Reservation(5, sks.DIFF_REVIEWER, 2048, 16, 300.0),
]


def _breaches(
    pods: Sequence[Mapping[str, bytes]], now: float, *, running: bool = False
) -> list[str]:
    facts = sks.attempt_facts(pods, not_before=T0 - 30)
    return sks.watchdog_breaches(
        RESERVATIONS, facts.sessions, sks.parse_ceiling(CEILING), now=now, running=running
    )


def test_watchdog_charges_every_session_against_the_ceiling() -> None:
    """Behavior: output tokens first, then turns, then seconds; totals, each native
    session's own caps (the publish continuation's included), unknown usage and
    unreserved starts all bind."""

    calm = _attempt_files()
    roles = {session.role for session in sks.parse_native_transcripts(calm).sessions}
    assert roles == {sks.IMPLEMENTER, sks.PLAN_REVIEWER, sks.DIFF_REVIEWER}
    assert _breaches([calm], T0 + 130) == []

    heavy = _pod_files(_turns("msg_heavy", IMPLEMENTER_MODEL, 32, 260, start=0, step=10))
    assert _breaches([heavy], T0 + 310) == [
        "output_tokens:implementer",
        "turns:implementer",
        "seconds:implementer",
    ]

    # The publish continuation replays its history into a new pod's session. The
    # replay is not charged again; the continuation is its own native session in its
    # own implementer slot, so the per-session caps bind each session on its own.
    first = _turns("msg_impl", IMPLEMENTER_MODEL, 20, 100, start=0)
    second = [*first, *_turns("msg_cont", IMPLEMENTER_MODEL, 12, 100, start=200)]
    pods = [
        _pod_files(first),
        _pod_files(second, session=CONTINUATION, config="curie-claude-config-c3d4"),
    ]
    facts = sks.attempt_facts(pods, not_before=T0 - 30)
    slots, unreserved = sks.match_slots(RESERVATIONS, facts.sessions)
    assert [
        (slots[seq].sessions, slots[seq].assistant_turns, slots[seq].output_tokens)
        for seq in (1, 2)
    ] == [(1, 20, 2000), (1, 12, 1200)]
    assert unreserved == []
    assert _breaches(pods, T0 + 212) == []

    # The totals bind even when no single session reaches its own cap.
    spread = _pod_files(
        _turns("msg_impl", IMPLEMENTER_MODEL, 24, 50, start=0),
        [
            ("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 24, 50, start=30)),
            ("diff01", DIFF_KIND, _turns("msg_diff", REVIEWER_MODEL, 24, 50, start=60)),
            ("diff02", DIFF_KIND, _turns("msg_redo", REVIEWER_MODEL, 24, 50, start=90)),
        ],
    )
    assert _breaches([spread], T0 + 114) == ["turns:total"]

    # Seconds: the most recently active session is charged the time since its last record.
    long = _pod_files(_turns("msg_long", IMPLEMENTER_MODEL, 2, 50, start=0, step=250))
    assert _breaches([long], T0 + 290) == []
    assert _breaches([long], T0 + 300) == ["seconds:implementer"]

    # A session whose records lack usage is unknown; an in-flight final line is not.
    unknown = _pod_files(
        _turns("msg_impl", IMPLEMENTER_MODEL, 3, 50, start=0),
        [("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 2, None, start=5))],
    )
    assert _breaches([unknown], T0 + 10) == ["usage_unknown:plan-reviewer"]
    in_flight = _pod_files(_turns("msg_impl", IMPLEMENTER_MODEL, 3, 50, start=0))
    key = next(iter(in_flight))
    in_flight[key] += b'{"type": "assistant", "message": {"id": "msg_partial"'
    assert _breaches([in_flight], T0 + 10) == []

    # A start no reservation covers is unreserved: a third diff review, any other agent.
    extra = _pod_files(
        _turns("msg_impl", IMPLEMENTER_MODEL, 3, 50, start=0),
        [
            (
                f"diff0{index}",
                DIFF_KIND,
                _turns(f"msg_d{index}", REVIEWER_MODEL, 1, 50, start=index),
            )
            for index in range(1, 4)
        ]
        + [("other1", "general-purpose", _turns("msg_other", REVIEWER_MODEL, 1, 50, start=9))],
    )
    assert _breaches([extra], T0 + 10) == ["unreserved:diff-reviewer", "unreserved:unattributed"]


def test_watchdog_bounds_wall_clock_and_charges_a_live_session_from_its_first_record(
    tmp_path: Path,
) -> None:
    """Behavior: every session's seconds are wall clock, never less the time its reviewers
    ran, in the watchdog, the ledger and the judge; while a native process runs, the newest
    implementer session is charged now minus its first record, so waiting on a reviewer
    never pauses its bound; nothing breaches before the deadline."""

    ceiling = sks.parse_ceiling(CEILING)
    # The implementer waits on its plan review and answers 302 s after it started; the
    # shared parser's figure leaves out the 290 s the reviewer ran.
    waited = _pod_files(
        [
            *_turns("msg_impl", IMPLEMENTER_MODEL, 3, 50, start=0),
            *_turns("msg_back", IMPLEMENTER_MODEL, 2, 50, start=301),
        ],
        [("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 3, 50, start=5, step=145))],
    )
    facts = sks.attempt_facts([waited], not_before=T0 - 30)
    implementer = next(item for item in facts.sessions if item.role == sks.IMPLEMENTER)
    assert implementer.elapsed < 300 <= sks.wall_seconds(implementer) == 302.0
    assert _breaches([waited], T0 + 302) == ["seconds:implementer"]
    slots, _unreserved = sks.match_slots(RESERVATIONS, facts.sessions)
    assert slots[1].elapsed == 302.0
    assert sks.session_record(implementer)["elapsed"] == 302.0
    ledger = _run_ledger(_private(tmp_path / "state"))
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
    sks.settle_attempt(ledger, reservations, facts.sessions, complete=True)
    rows = ledger.sessions()
    assert rows[0]["elapsed"] == 302.0
    assert "session_seconds" in sks.budget_findings(rows, ceiling)
    ledger.close()

    # A reviewer is still active and the implementer has written nothing since it started
    # the review. While its process runs, the implementer is charged now minus its first
    # record; the reviewer, the most recently active, is charged since its last record.
    active = _pod_files(
        _turns("msg_impl", IMPLEMENTER_MODEL, 3, 50, start=0),
        [("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 3, 50, start=10, step=140))],
    )
    assert _breaches([active], T0 + 299, running=True) == []
    assert _breaches([active], T0 + 300, running=True) == ["seconds:implementer"]
    assert _breaches([active], T0 + 300) == []  # no native process runs: no live charge

    # The live session is the implementer session with the latest first record: the
    # publish continuation, not the main session that ended before it.
    pods = [
        _pod_files(_turns("msg_impl", IMPLEMENTER_MODEL, 2, 50, start=0, step=50)),
        _pod_files(
            _turns("msg_cont", IMPLEMENTER_MODEL, 2, 50, start=200, step=10),
            session=CONTINUATION,
            config="curie-claude-config-c3d4",
        ),
    ]
    assert _breaches(pods, T0 + 499, running=True) == []
    assert _breaches(pods, T0 + 500, running=True) == ["seconds:implementer"]


def test_every_native_start_takes_its_own_slot_and_the_session_count_binds(
    tmp_path: Path,
) -> None:
    """Behavior: the implementer, the publish continuation, the plan reviewer and two diff
    reviewers settle as five rows, each against its own per-session caps; a third
    implementer session is unreserved in the watchdog and in the judge's findings, more
    native sessions than the ceiling breach, and a ceiling under five sessions is refused."""

    ceiling = sks.parse_ceiling(CEILING)
    tight = sks.parse_ceiling(CEILING.replace("sessions=8", "sessions=5"))
    # Each implementer session alone is under the per-session caps; together they are not.
    implementer = _turns("msg_impl", IMPLEMENTER_MODEL, 20, 250, start=0)
    pods = [
        _pod_files(
            implementer,
            [
                ("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 3, 300, start=20)),
                ("diff01", DIFF_KIND, _turns("msg_diff", REVIEWER_MODEL, 3, 300, start=30)),
                ("diff02", DIFF_KIND, _turns("msg_redo", REVIEWER_MODEL, 3, 300, start=40)),
            ],
        ),
        _pod_files(
            [*implementer, *_turns("msg_cont", IMPLEMENTER_MODEL, 20, 200, start=100)],
            session=CONTINUATION,
            config="curie-claude-config-c3d4",
        ),
    ]
    five = sks.attempt_facts(pods, not_before=T0 - 30).sessions
    assert len(five) == 5
    assert sks.watchdog_breaches(RESERVATIONS, five, ceiling, now=T0 + 120, running=False) == []
    assert sks.watchdog_breaches(RESERVATIONS, five, tight, now=T0 + 120, running=False) == []
    state = _private(tmp_path / "state")
    ledger = _run_ledger(state)
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
    counts = sks.settle_attempt(ledger, reservations, five, complete=True)
    assert counts == {"settled": 5, "unknown": 0, "unreserved": 0}
    rows = ledger.sessions()
    assert [(r["role"], r["source"], r["output_tokens"], r["assistant_turns"]) for r in rows] == [
        (sks.IMPLEMENTER, "native", 5000, 20),
        (sks.IMPLEMENTER, "native", 4000, 20),
        (sks.PLAN_REVIEWER, "native", 900, 3),
        (sks.DIFF_REVIEWER, "native", 900, 3),
        (sks.DIFF_REVIEWER, "native", 900, 3),
    ]
    assert sks.budget_findings(rows, ceiling) == [] and ledger.budget_state() == "ok"
    ledger.close()

    # A sixth native session, a third implementer session, has no slot left.
    third = _pod_files(
        _turns("msg_third", IMPLEMENTER_MODEL, 1, 50, start=130),
        session=THIRD_SESSION,
        config="curie-claude-config-e5f6",
    )
    six = sks.attempt_facts([*pods, third], not_before=T0 - 30).sessions
    assert sks.watchdog_breaches(RESERVATIONS, six, ceiling, now=T0 + 131, running=False) == [
        "unreserved:implementer"
    ]
    assert sks.watchdog_breaches(RESERVATIONS, six, tight, now=T0 + 131, running=False) == [
        "sessions:total",
        "unreserved:implementer",
    ]
    for run_id, complete, settled, unknown in (
        ("acme-kickoff-2", True, 5, 0),
        ("acme-kickoff-3", False, 0, 5),  # an incomplete capture still counts every start
    ):
        judged = _run_ledger(state, run_id)
        judged.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
        reservations = judged.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
        counts = sks.settle_attempt(judged, reservations, six, complete=complete)
        assert counts == {"settled": settled, "unknown": unknown, "unreserved": 1}
        rows = judged.sessions()
        assert len(rows) == 6
        assert (rows[-1]["role"], rows[-1]["source"]) == (sks.IMPLEMENTER, "unreserved")
        assert "unreserved" in sks.budget_findings(rows, ceiling)
        assert {"sessions", "unreserved"} <= set(sks.budget_findings(rows, tight))
        assert judged.budget_state() == "exceeded"
        judged.close()

    with pytest.raises(sks.Refusal) as refused:
        sks.attempt_roles(sks.parse_ceiling(CEILING.replace("sessions=8", "sessions=4")))
    assert refused.value.code == "live_unapproved"
    assert sks.attempt_roles(tight) == sks.ATTEMPT_ROLES
    # Implementer slots never pass the bundle's reviewer gate, so the image limits hold.
    assert sks.reviewer_limits(sks.ATTEMPT_ROLES) == {"plan": 1, "diff": 2}


def test_settlement_checks_the_ceiling_once_every_slot_holds_its_usage(
    tmp_path: Path,
) -> None:
    """Behavior: an attempt whose observed totals fit the ceiling settles ok even when the
    implementer and continuation together use more than their reserved shares; a slot not
    yet settled is charged its full share, so a check between slots would mark it
    exceeded. A real excess is still exceeded."""

    ceiling = sks.parse_ceiling(CEILING)

    def attempt(continuation_tokens: int) -> Sequence[Any]:
        pods = [
            _pod_files(
                _turns("msg_impl", IMPLEMENTER_MODEL, 20, 400, start=0),
                [
                    ("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 3, 100, start=25)),
                    ("diff01", DIFF_KIND, _turns("msg_diff", REVIEWER_MODEL, 3, 100, start=30)),
                    ("diff02", DIFF_KIND, _turns("msg_redo", REVIEWER_MODEL, 3, 100, start=35)),
                ],
            ),
            _pod_files(
                _turns("msg_cont", IMPLEMENTER_MODEL, 13, continuation_tokens, start=100),
                session=CONTINUATION,
                config="curie-claude-config-c3d4",
            ),
        ]
        return sks.attempt_facts(pods, not_before=T0 - 30).sessions

    state = _private(tmp_path / "state")
    for run_id, continuation_tokens, expected in (
        # 8000 + 2600 + 900 = 11,500 observed; 8000 + 2600 + 3 x 2048 reserved mid-way.
        ("acme-kickoff-fits", 200, "ok"),
        # 8000 + 13 x 600 + 900 = 16,700 observed: over the 16,384 total, under each cap.
        ("acme-kickoff-over", 600, "exceeded"),
    ):
        sessions = attempt(continuation_tokens)
        assert len(sessions) == 5
        ledger = _run_ledger(state, run_id)
        ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
        reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
        counts = sks.settle_attempt(ledger, reservations, sessions, complete=True)
        assert counts == {"settled": 5, "unknown": 0, "unreserved": 0}
        rows = ledger.sessions()
        assert [r["output_tokens"] for r in rows] == [
            8000,
            13 * continuation_tokens,
            300,
            300,
            300,
        ]
        assert ledger.budget_state() == expected
        assert (sks.budget_findings(rows, ceiling) == []) == (expected == "ok")
        ledger.close()


def test_a_session_awaiting_its_first_response_is_pending_not_unknown(tmp_path: Path) -> None:
    """Behavior: a transcript holding only user records is pending, bound only by its
    seconds; its first valid response is counted normally; a response without usage is
    still unknown; settlement of a session with no assistant usage stays unknown."""

    waiting = _pod_files([_user(0)])
    assert _breaches([waiting], T0 + 10) == []
    assert _breaches([waiting], T0 + 10, running=True) == []
    assert _breaches([waiting], T0 + 300) == ["seconds:implementer"]
    answered = _pod_files([_user(0), *_turns("msg_impl", IMPLEMENTER_MODEL, 1, 120, start=4)])
    assert _breaches([answered], T0 + 10) == []
    facts = sks.attempt_facts([answered], not_before=T0 - 30)
    slots, _unreserved = sks.match_slots(RESERVATIONS, facts.sessions)
    assert (slots[1].known, slots[1].output_tokens, slots[1].assistant_turns) == (True, 120, 1)
    no_usage = _pod_files([_user(0), *_turns("msg_impl", IMPLEMENTER_MODEL, 1, None, start=4)])
    assert _breaches([no_usage], T0 + 10) == ["usage_unknown:implementer"]

    # Settlement stays fail closed: no assistant usage is unknown, charged its full share.
    ledger = _run_ledger(_private(tmp_path / "state"))
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0 + 1)
    facts = sks.attempt_facts([waiting], not_before=T0 - 30)
    counts = sks.settle_attempt(ledger, reservations, facts.sessions, complete=True)
    assert counts == {"settled": 4, "unknown": 1, "unreserved": 0}
    rows = ledger.sessions()
    assert rows[0]["source"] == "unknown" and sks.charged(rows[0]) == (8192, 32, 300.0)
    assert ledger.budget_state() == "unknown"
    ledger.close()


# --------------------------------------------------------------------------
# 4. The fence: a queued continuation during shutdown gets no paid start
# --------------------------------------------------------------------------


class FakeShell(psk.Shell):
    """Answers each command from canned recorded shapes and records it with the gate state."""

    def __init__(
        self,
        runtime: psk.Runtime,
        ledger: sks.Ledger,
        answer: Callable[[list[str]], psk.Completed],
    ) -> None:
        super().__init__(env={}, scrubber=runtime.scrubber, scan=runtime.scan, cwd=Path("/"))
        self.ledger = ledger
        self.answer = answer
        self.calls: list[tuple[list[str], str]] = []

    def run(self, argv: Sequence[str], **kwargs: Any) -> psk.Completed:
        command = list(argv)
        self.guard(
            command,
            stdin=kwargs.get("stdin"),
            extra_env=kwargs.get("extra_env"),
            secret_env=kwargs.get("secret_env", ()),
            secret_stdin=kwargs.get("secret_stdin", False),
            clean_env=kwargs.get("clean_env", False),
        )
        self.calls.append((command, self.ledger.gate()))
        result = self.answer(command)
        if kwargs.get("check", True) and result.returncode != 0:
            raise psk.StepFailed(f"{command[0]} exited {result.returncode}")
        return result

    def spawn(self, argv: Sequence[str]) -> Any:
        raise AssertionError("no process is spawned in a hermetic test")

    def verbs(self) -> list[tuple[str, str]]:
        return [(_verb(argv[1:]), gate) for argv, gate in self.calls]


def _empty(_argv: list[str]) -> psk.Completed:
    return psk.Completed(0, json.dumps({"items": []}).encode(), b"")


def _pod(name: str, uid: str, phase: str, *, running: bool) -> dict[str, Any]:
    """A ``kubectl get pods -o json`` item: metadata and the runner's container status.

    https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/
    """

    state = (
        {"running": {"startedAt": "2027-01-15T08:00:00Z"}}
        if running
        else {"waiting": {"reason": "ContainerCreating"}}
    )
    return {
        "metadata": {
            "name": name,
            "uid": uid,
            "labels": {psk.POD_CLAIM_LABEL: name.removesuffix("-runner")},
        },
        "status": {"phase": phase, "containerStatuses": [{"name": "runner", "state": state}]},
    }


def _ready_worker() -> dict[str, Any]:
    """A worker pod whose readiness heartbeat still reports Ready."""

    return {
        "metadata": {"name": "psk-acme-worker-0"},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }


def _cluster(
    pods: Sequence[dict[str, Any]],
    *,
    workers: Sequence[dict[str, Any]] = (),
    late: Sequence[dict[str, Any]] = (),
) -> tuple[Callable[[list[str]], psk.Completed], dict[str, Any]]:
    """Runner pods by the claim label until a runner delete removes them (leaving ``late``).

    Worker pods by the worker selector stay as given; every other read is empty.
    """

    cluster: dict[str, Any] = {"pods": list(pods), "workers": list(workers)}

    def answer(argv: list[str]) -> psk.Completed:
        args = argv[1:]
        verb = _verb(args)
        worker = any("component=worker" in arg for arg in args)
        if verb == "delete" and "pod" in args and not worker:
            cluster["pods"] = list(late)
        if verb == "get" and "pods" in args and worker:
            return psk.Completed(0, json.dumps({"items": cluster["workers"]}).encode(), b"")
        if verb == "get" and "pods" in args and psk.POD_CLAIM_LABEL in args:
            return psk.Completed(0, json.dumps({"items": cluster["pods"]}).encode(), b"")
        if verb == "get":
            return _empty(argv)
        return psk.Completed(0, b"", b"")

    return answer, cluster


@dataclasses.dataclass
class Live:
    run: psk.LiveRun
    ledger: sks.Ledger
    shell: Any
    clock: FakeClock


def _live_run(
    tmp_path: Path,
    *,
    answer: Callable[[list[str]], psk.Completed] = _empty,
    shell_env: Mapping[str, str] | None = None,
    **inputs: Any,
) -> Live:
    state = _private(tmp_path / "state")
    scrubber = sks.Scrubber()
    scan = sks.TokenScan(scrubber, psk.SCAN_CHANNELS)
    clock = FakeClock()
    runtime = psk.Runtime({}, psk.Output(scrubber, scan), scrubber, scan, clock)
    ledger = _run_ledger(state)
    ledger.scrubber = scrubber
    shell: Any = (
        FakeShell(runtime, ledger, answer)
        if shell_env is None
        else psk.Shell(
            env=shell_env, scrubber=scrubber, scan=scan, cwd=tmp_path, log=state / "commands.jsonl"
        )
    )
    values: dict[str, Any] = {
        "candidate": CANDIDATE,
        "run_id": RUN_ID,
        "state_dir": state,
        "context": CONTEXT,
        "docker_context": DOCKER_CONTEXT,
        "registry_port": 5999,
        "namespace": NAMESPACE,
        "release": RELEASE,
        "expires_at": T0 + 3 * 3600,
        **inputs,
    }
    run = psk.LiveRun(
        inputs=psk.Inputs(**values),
        runtime=runtime,
        ledger=ledger,
        ceiling=sks.parse_ceiling(CEILING),
        shell=shell,
        resolver=lambda _host: ["192.0.2.10"],
    )
    return Live(run, ledger, shell, clock)


def _runner_pod(run: psk.LiveRun, uid: str, name: str = "acme-claim-1-runner") -> psk.RunnerPod:
    return psk.RunnerPod(
        name=name,
        uid=uid,
        claim=name.removesuffix("-runner"),
        record={},
        harvest=sks.HarvestState(run.scan),
        watch=sks.ProcessWatch(),
        lock=threading.Lock(),
        eof=threading.Event(),
    )


def _file_record(path: str, offset: int, data: bytes) -> dict[str, Any]:
    """The harvester's ``file`` record (``emit`` in personal_profile_support.HARVEST_PROGRAM)."""

    return {
        "kind": "file",
        "path": path,
        "offset": offset,
        "data": base64.b64encode(data).decode(),
    }


def _feed(pod: psk.RunnerPod, *records: Mapping[str, Any], now: float = T0) -> None:
    """Feed harvester records as the driver's reader does, at the reader's ``now``."""

    for record in records:
        line = json.dumps(record).encode()
        pod.harvest.feed(line)
        pod.watch.feed(line, now=now)
        pod.trace.feed_harvest(line)


def _fence_order(shell: FakeShell, start: int) -> list[str]:
    """The fence's steps from ``start``, asserting the accepted order.

    Scale, worker pod delete with a one second grace period, worker wait, worker read,
    each runner exec, runner pod delete, runner wait; every worker step before any
    runner exec or delete, and the gate closed after the scale.
    """

    calls = shell.calls[start:]
    names: list[str] = []
    for argv, _gate in calls:
        args = argv[1:]
        verb = _verb(args)
        kind = "worker" if any("component=worker" in arg for arg in args) else "runner"
        names.append(verb if verb in {"scale", "exec"} else f"{verb}-{kind}")
    scale = names.index("scale")
    assert all(gate == "closed" for _argv, gate in calls[scale + 1 :])
    worker_delete = names.index("delete-worker")
    worker_wait = names.index("wait-worker")
    worker_read = names.index("get-worker")
    execs = [index for index, name in enumerate(names) if name == "exec"]
    runner_delete = names.index("delete-runner")
    runner_wait = names.index("wait-runner")
    assert scale < worker_delete < worker_wait < worker_read
    assert "--grace-period=1" in calls[worker_delete][0]
    assert "--wait=false" in calls[worker_delete][0]
    assert "--timeout=60s" in calls[worker_wait][0]
    assert all(worker_read < index < runner_delete for index in execs)
    assert runner_delete < runner_wait
    return names


def test_queued_continuation_during_shutdown_gets_no_paid_start(tmp_path: Path) -> None:
    """Behavior: after the attempt settles, a runner the run did not follow is an unreserved
    paid start; the worker is scaled to zero first, the gate closes, every runner is
    interrupted and deleted before any wait, and no further reservation is granted."""

    followed = _pod("acme-claim-1-runner", "uid-1", "Running", running=True)
    answer, cluster = _cluster([followed])
    live = _live_run(tmp_path, answer=answer)
    run, ledger, shell = live.run, live.ledger, live.shell
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    facts = sks.attempt_facts([_attempt_files()], not_before=T0 - 30)
    sks.settle_attempt(ledger, reservations, facts.sessions, complete=True)
    run.pods = {"uid-1": _runner_pod(run, "uid-1")}
    run.guard_starts = True
    # Liveness: only the attempt's own runner is present, so nothing is fenced.
    run.tick()
    assert shell.verbs() == [("get", "open")]
    # The platform queues a continuation after the attempt settled; its pod appears.
    cluster["pods"] = [followed, _pod("acme-claim-2-runner", "uid-2", "Pending", running=False)]
    with pytest.raises(psk.StepFailed, match="after the attempt settled"):
        run.tick()
    names = _fence_order(shell, 1)
    assert names[:2] == ["get-runner", "scale"]
    interrupted = [argv[argv.index("exec") + 1] for argv, _gate in shell.calls if "exec" in argv]
    assert interrupted == ["acme-claim-1-runner", "acme-claim-2-runner"]
    budget = ledger.observations("budget")
    assert budget["allowance_breach"] == ["unreserved:after_attempt"]
    assert budget["paid_starts_fenced"] is True
    assert budget["workers_stopped_first"] is True
    assert ledger.gate() == "closed" and run.guard_starts is False
    assert len(ledger.sessions()) == len(sks.ATTEMPT_ROLES)
    with pytest.raises(sks.Refusal) as refused:
        ledger.reserve([sks.IMPLEMENTER], now=T0 + 900)
    assert refused.value.code == "gate_closed"
    ledger.close()


def test_watchdog_fences_paid_starts_at_the_first_reached_bound(tmp_path: Path) -> None:
    """Behavior: after each response the watchdog checks the bounds in order; reaching one
    records it, fences paid starts, and the settled capture charges native usage."""

    answer, _cluster_state = _cluster(
        [_pod("acme-claim-1-runner", "uid-1", "Running", running=True)]
    )
    live = _live_run(tmp_path, answer=answer)
    run, ledger, shell = live.run, live.ledger, live.shell
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    run.attempt_live, run.attempt_started = True, T0
    pod = _runner_pod(run, "uid-1")
    run.pods[pod.uid] = pod
    path = next(iter(_pod_files([])))
    calm = _jsonl(_turns("msg_calm", IMPLEMENTER_MODEL, 4, 100, start=1))
    _feed(pod, {"kind": "process", "count": 1, "processes": []}, _file_record(path, 0, calm))
    live.clock.at = T0 + 10
    run.tick()
    assert shell.verbs() == [("get", "open")]  # the claim read; every bound still holds
    heavy = _jsonl(_turns("msg_heavy", IMPLEMENTER_MODEL, 28, 300, start=20))
    _feed(pod, _file_record(path, len(calm), heavy))
    live.clock.at = T0 + 50
    with pytest.raises(psk.StepFailed, match="approved bound"):
        run.tick()
    _fence_order(shell, 2)
    budget = ledger.observations("budget")
    assert budget["allowance_breach"] == ["output_tokens:implementer", "turns:implementer"]
    assert budget["paid_starts_fenced"] is True
    assert budget["workers_stopped_first"] is True
    assert ledger.gate() == "closed" and run.attempt_live is False
    # The fence stopped the runner before it logged a turn end: even with the CLI gone and
    # a size report after it, the capture is not affirmed and every slot stays unknown.
    _feed(
        pod,
        {"kind": "process", "count": 0, "processes": []},
        {"kind": "sync", "sizes": {path: len(calm) + len(heavy)}, "at": T0 + 60},
    )
    pod.eof.set()
    run.settle()
    assert [row["source"] for row in ledger.sessions()] == ["unknown"] * 5
    capture = ledger.observations("budget")["capture"]
    assert capture["affirmed"] is False and "no_turn_end" in capture["failures"]
    assert ledger.budget_state() == "unknown"
    # Both non-empty watchdog checks ran, and between them they saw every response.
    guard = ledger.observations("budget")["guard_checks"]
    assert (guard["checks"], guard["responses"], guard["responses_checked"]) == (2, 32, 32)
    assert [row["role"] for row in guard["sessions"]] == [sks.IMPLEMENTER]
    assert guard["sessions"][0]["turns"] == 32 and guard["sessions"][0]["launched"] is True
    assert guard["sessions"][0]["responses_checked"] == 32
    assert guard["sessions"][0]["guard_after_first"] is True
    assert ledger.observations("grant")["role_models"] == {sks.IMPLEMENTER: [IMPLEMENTER_MODEL]}
    with pytest.raises(sks.Refusal) as refused:
        ledger.reserve([sks.IMPLEMENTER], now=T0 + 900)
    assert refused.value.code == "gate_closed"
    ledger.close()


def _armed_fence(tmp_path: Path, answer: Callable[[list[str]], psk.Completed]) -> Live:
    live = _live_run(tmp_path, answer=answer)
    live.ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    return live


def test_fence_interrupts_runners_even_when_a_worker_stays_ready(tmp_path: Path) -> None:
    """Behavior: a worker pod still Ready after its stop does not leave a runner running;
    every runner is interrupted and deleted, and the fence records the worker and fails."""

    followed = _pod("acme-claim-1-runner", "uid-1", "Running", running=True)
    answer, _state = _cluster([followed], workers=[_ready_worker()])
    live = _armed_fence(tmp_path, answer)
    with pytest.raises(psk.StepFailed, match="a worker is still active"):
        live.run.fence_paid_starts()
    names = _fence_order(live.shell, 0)
    assert names.count("exec") == 1
    budget = live.ledger.observations("budget")
    assert budget["workers_stopped_first"] is False
    assert budget["paid_starts_fenced"] is False
    live.ledger.close()


def test_fence_fails_on_a_runner_started_after_the_runner_delete(tmp_path: Path) -> None:
    """Behavior: a continuation runner that is running on the final read fails the fence."""

    followed = _pod("acme-claim-1-runner", "uid-1", "Running", running=True)
    retry = _pod("acme-claim-2-runner", "uid-2", "Running", running=True)
    answer, _state = _cluster([followed], late=[retry])
    live = _armed_fence(tmp_path, answer)
    with pytest.raises(psk.StepFailed, match="a runner is still active"):
        live.run.fence_paid_starts()
    _fence_order(live.shell, 0)
    budget = live.ledger.observations("budget")
    assert budget["workers_stopped_first"] is True
    assert budget["paid_starts_fenced"] is False
    live.ledger.close()


class SteppedClock(FakeClock):
    """FakeClock for the deadline enforcer: each of its waits parks until the test runs a pass.

    The test sets ``at`` itself, so the enforcer thread sees exactly the time the test
    chose, and the main loop stays blocked simply by not calling ``tick``.
    """

    def __init__(self) -> None:
        super().__init__()
        self.parked: queue.Queue[float] = queue.Queue()
        self.go: queue.Queue[None] = queue.Queue()

    def wait(self, seconds: float) -> None:
        self.parked.put(seconds)
        self.go.get(timeout=30)

    def run_pass(self) -> None:
        """Let the parked enforcer run one more pass; return once it parks again."""

        self.go.put(None)
        assert self.parked.get(timeout=30) == psk.DEADLINE_SECONDS


@dataclasses.dataclass
class Exited:
    """What ``Shell.spawn`` hands back, for a command that already exited quietly."""

    stdout: Any = None
    stderr: Any = None

    def poll(self) -> int:
        return 0


class SpawningShell(FakeShell):
    """FakeShell that records each command started without waiting, and runs none."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.spawned: list[list[str]] = []

    def spawn(self, argv: Sequence[str]) -> Any:
        self.guard(list(argv))
        self.spawned.append(list(argv))
        return Exited()


def test_deadline_enforcer_stops_a_runner_at_its_bound_while_the_main_loop_blocks(
    tmp_path: Path,
) -> None:
    """Behavior: a native process starts at the caller's time, no later than its runner
    container; with the main loop blocked, the enforcer thread interrupts the runner and
    deletes its pod at start plus the session bound without waiting on either; the next
    tick records the breach and runs the full fence; nothing fires before the deadline."""

    record = _pod("acme-claim-1-runner", "uid-1", "Running", running=True)
    assert sks.runner_started_at(record) == T0  # the pod record's startedAt

    def process(count: int) -> bytes:
        return json.dumps({"kind": "process", "count": count, "processes": []}).encode()

    watch = sks.ProcessWatch(container_started=T0 + 50)
    watch.feed(process(1), now=T0 + 40)
    assert (watch.running, watch.started, watch.deadline(300.0)) == (True, T0 + 40, T0 + 340)
    watch.feed(process(2), now=T0 + 60)
    assert watch.started == T0 + 40  # more processes in the same run start nothing new
    watch.feed(process(0), now=T0 + 90)
    assert not watch.running and watch.deadline(300.0) is None
    watch.feed(process(1), now=T0 + 400)
    assert watch.started == T0 + 400  # a later process is its own start

    answer, _cluster_state = _cluster([record])
    live = _live_run(tmp_path, answer=answer)
    run, ledger = live.run, live.ledger
    shell = SpawningShell(run.runtime, ledger, answer)
    run.shell, run.kube = shell, psk.Kube(shell, CONTEXT, NAMESPACE)
    clock = SteppedClock()
    run.clock = clock
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    ledger.record("budget", "allowance_breach", [])
    run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    run.attempt_live, run.attempt_started = True, T0
    pod = _runner_pod(run, "uid-1")
    pod.watch.container_started = sks.runner_started_at(record)
    run.pods[pod.uid] = pod
    # The harvester reports the native process 40 s after the runner container started.
    _feed(pod, {"kind": "process", "count": 1, "processes": []}, now=T0 + 40)
    assert pod.watch.deadline(300.0) == T0 + 300
    clock.at = T0 + 40
    run.start_deadline_enforcer(sks.parse_ceiling(CEILING))
    assert "stop-deadline-enforcer" in [name for name, _step in run.finalizer.steps]
    assert clock.parked.get(timeout=30) == psk.DEADLINE_SECONDS  # the first pass ran

    # Liveness: nothing fires before the deadline, and the main loop sees nothing either.
    clock.at = T0 + 299
    clock.run_pass()
    assert shell.spawned == [] and run.deadline_breach == ""
    run.tick()
    calls = len(shell.calls)

    # At the deadline, with the main loop blocked: interrupt and delete, nothing awaited.
    clock.at = T0 + 300
    clock.run_pass()
    prefix = ["kubectl", "--context", CONTEXT, "--namespace", NAMESPACE]
    assert shell.spawned == [
        [*prefix, "exec", pod.name, "-c", "runner", "--", "python3", "-c", psk.INTERRUPT_PROGRAM],
        [*prefix, "delete", "pod", pod.name, "--wait=false"],
    ]
    assert run.deadline_breach == f"seconds:{sks.IMPLEMENTER}"
    clock.at = T0 + 301
    clock.run_pass()
    assert len(shell.spawned) == 2  # one stop per runner
    assert len(shell.calls) == calls  # the enforcer ran nothing through the blocking shell
    assert ledger.observations("budget")["allowance_breach"] == []  # nor touched the ledger

    # The main loop's next tick records the breach and runs the full fence.
    with pytest.raises(psk.StepFailed, match="absolute time bound"):
        run.tick()
    _fence_order(shell, calls)
    budget = ledger.observations("budget")
    assert budget["allowance_breach"] == [f"seconds:{sks.IMPLEMENTER}"]
    assert budget["paid_starts_fenced"] is True
    assert budget["workers_stopped_first"] is True
    assert ledger.gate() == "closed" and run.attempt_live is False
    assert run.deadline_stop.is_set()
    fenced = len(shell.calls)
    run.tick()  # reported once: a later tick fences nothing again
    assert len(shell.calls) == fenced

    clock.go.put(None)  # release the parked enforcer; its stop is already set
    run.close_deadline_enforcer()
    assert run.deadline_thread is not None and not run.deadline_thread.is_alive()
    ledger.close()


def test_finalize_records_a_deadline_the_enforcer_reached_before_fencing(
    tmp_path: Path,
) -> None:
    """Behavior: when the enforcer stopped a runner at its deadline but the main loop then
    failed some other way, finalize still records the seconds breach, once, before the
    fence, and stops the enforcer."""

    live_run = _live_run(tmp_path)
    run, ledger = live_run.run, live_run.ledger
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    ledger.record("budget", "allowance_breach", ["output_tokens:total"])
    run.mutated = True
    run.deadline_breach = f"seconds:{sks.IMPLEMENTER}"
    run.finalize()
    assert ledger.observations("budget")["allowance_breach"] == [
        "output_tokens:total",
        f"seconds:{sks.IMPLEMENTER}",
    ]
    assert run.deadline_stop.is_set() and run.deadline_reported
    assert run.record_deadline_breach() is False  # recorded once
    ledger.close()


# --------------------------------------------------------------------------
# 4b. Diagnostics: the claim-pod follower and why an attempt ended
# --------------------------------------------------------------------------

REQUEST = "7a1b2c3d-0000-4000-8000-00000000c004"
OTHER_REQUEST = "7a1b2c3d-0000-4000-8000-00000000c005"


class StreamShell(FakeShell):
    """FakeShell that is safe off the main thread and scripts each spawned stream.

    Commands are recorded with no ledger read (the ledger belongs to the main
    thread). Each spawn of a verb takes that verb's next scripted stdout, the
    last one repeating, from a command that already exited.
    """

    def __init__(
        self,
        runtime: psk.Runtime,
        ledger: sks.Ledger,
        answer: Callable[[list[str]], psk.Completed],
        streams: Mapping[str, Sequence[Sequence[bytes]]],
    ) -> None:
        super().__init__(runtime, ledger, answer)
        self.streams = {verb: list(outputs) for verb, outputs in streams.items()}
        self.spawned: list[list[str]] = []
        self.lock = threading.Lock()

    def run(self, argv: Sequence[str], **kwargs: Any) -> psk.Completed:
        command = list(argv)
        self.guard(command)
        with self.lock:
            self.calls.append((command, ""))
        result = self.answer(command)
        if kwargs.get("check", True) and result.returncode != 0:
            raise psk.StepFailed(f"{command[0]} exited {result.returncode}")
        return result

    def spawn(self, argv: Sequence[str]) -> Any:
        command = list(argv)
        self.guard(command)
        with self.lock:
            self.spawned.append(command)
            outputs = self.streams.get(_verb(command[1:])) or [[]]
            lines = outputs.pop(0) if len(outputs) > 1 else outputs[0]
        return Exited(stdout=list(lines))


class FollowerClock(FakeClock):
    """Each follower pass parks until the test runs the next; stream retries move time."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.parked: queue.Queue[float] = queue.Queue()
        self.go: queue.Queue[None] = queue.Queue()

    def wait(self, seconds: float) -> None:
        if seconds == psk.FOLLOW_SECONDS:
            self.parked.put(seconds)
            self.go.get(timeout=30)
            return
        with self.lock:
            super().wait(seconds)


def _claim_answer(
    claim: str, pods: dict[str, dict[str, Any]]
) -> Callable[[list[str]], psk.Completed]:
    """One claim of the agent and its pod (``pods["pod"]``, swappable); other reads empty."""

    def answer(argv: list[str]) -> psk.Completed:
        if psk.CLAIM_RESOURCE in argv:
            items = [{"metadata": {"name": claim}}]
            return psk.Completed(0, json.dumps({"items": items}).encode(), b"")
        if "pods" in argv:
            return psk.Completed(0, json.dumps({"items": [pods["pod"]]}).encode(), b"")
        return _empty(argv)

    return answer


def _streams_of(run: psk.LiveRun, shell: StreamShell) -> None:
    run.shell, run.kube = shell, psk.Kube(shell, CONTEXT, NAMESPACE)


def test_runner_log_tail_keeps_the_last_lines_scrubbed(tmp_path: Path) -> None:
    """Behavior: an attached runner's log stream keeps only its last 80 lines, each scanned
    raw and kept scrubbed, and settlement records them under a sanitized pod reference."""

    live = _live_run(tmp_path)
    run, ledger = live.run, live.ledger
    marker = "synthetic-runner-log-marker"
    run.scrubber.hold(sks.HeldSecret(marker))
    lines = [f"runner line {index} {marker}\n".encode() for index in range(100)]
    record = json.dumps({"kind": "process", "count": 0, "processes": []}).encode()
    shell = StreamShell(run.runtime, ledger, _empty, {"exec": [[record]], "logs": [lines]})
    _streams_of(run, shell)
    observed = run.attach_pod(_pod("acme-claim-1-runner", "uid-1", "Running", running=True))
    assert observed is not None
    for thread in observed.threads:
        thread.join(timeout=30)
    tail = list(observed.log_tail)
    assert len(tail) == psk.RUNNER_LOG_TAIL == 80
    assert tail[0] == "runner line 20 <redacted>" and tail[-1] == "runner line 99 <redacted>"
    assert not any(marker in line for line in tail)
    assert run.scan.channels["children"].token_bytes_found  # scanned raw before scrubbing
    run.record_diagnostics()
    assert ledger.observations("grant")["runner_log_tails"] == [
        {"pod": sks.ref_digest("acme-claim-1-runner"), "lines": tail}
    ]
    assert marker not in ledger.dump()
    ledger.close()


def test_discovery_records_every_container_termination(tmp_path: Path) -> None:
    """Behavior: a discovery pass reads a claim pod's init and regular container statuses,
    current and last state, attaches the pod in any phase without auditing it, and
    settlement records each terminated container's exit code, reason and signal."""

    pod = _pod("acme-claim-1-runner", "uid-1", "Failed", running=False)
    pod["status"]["initContainerStatuses"] = [
        {"name": "prepare", "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}}
    ]
    pod["status"]["containerStatuses"] = [
        {
            "name": "runner",
            "state": {"terminated": {"exitCode": 1, "reason": "Error", "message": "free text"}},
            "lastState": {"terminated": {"exitCode": 137, "reason": "OOMKilled", "signal": 9}},
        }
    ]
    answer = _claim_answer("acme-claim-1", {"pod": pod})
    live = _live_run(tmp_path, answer=answer)
    run, ledger = live.run, live.ledger
    _streams_of(run, StreamShell(run.runtime, ledger, answer, {}))
    run.discover_runner_pods()
    observed = run.pods["uid-1"]
    for thread in observed.threads:
        thread.join(timeout=30)
    assert not observed.audited and observed.eof.is_set()
    run.record_diagnostics()
    ref = sks.ref_digest("acme-claim-1-runner")
    assert ledger.observations("grant")["runner_terminations"] == [
        {
            "pod": ref,
            "container": "prepare",
            "init": True,
            "from": "state",
            "exitCode": 0,
            "reason": "Completed",
            "signal": None,
        },
        {
            "pod": ref,
            "container": "runner",
            "init": False,
            "from": "lastState",
            "exitCode": 137,
            "reason": "OOMKilled",
            "signal": 9,
        },
        {
            "pod": ref,
            "container": "runner",
            "init": False,
            "from": "state",
            "exitCode": 1,
            "reason": "Error",
            "signal": None,
        },
    ]
    assert "free text" not in ledger.dump()
    ledger.close()


def test_worker_escalation_lines_are_read_before_the_fence_stops_the_worker(
    tmp_path: Path,
) -> None:
    """Behavior: once an attempt reserved, the fence reads the worker's log before its scale;
    only lines naming the work item or request or an escalation marker are kept, at most
    60, scrubbed; the latest capture wins and a failed read records an empty list."""

    marker = "synthetic-worker-log-marker"
    logs: dict[str, Any] = {"stdout": b"", "code": 0, "raise": False}
    cluster, _state = _cluster([_pod("acme-claim-1-runner", "uid-1", "Running", running=True)])

    def answer(argv: list[str]) -> psk.Completed:
        if _verb(argv[1:]) == "logs":
            if logs["raise"]:
                raise psk.StepFailed("kubectl could not complete")
            return psk.Completed(int(logs["code"]), bytes(logs["stdout"]), b"")
        return cluster(argv)

    live = _armed_fence(tmp_path, answer)
    run, ledger, shell = live.run, live.ledger, live.shell
    run.scrubber.hold(sks.HeldSecret(marker))
    run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    run.work_item_id = WORK_ITEM
    ledger.record("admission", "expected_request_id", REQUEST)
    kept = [
        f"INFO claimed work item {WORK_ITEM}",
        f"INFO turn started for {REQUEST} with {marker}",
        "INFO request runner_escalated after the runner exited",
        "WARNING slow poll",
        "INFO personal launch Refused by the gate",
        "Traceback (most recent call last):",
        "apps.worker RunnerError: runner exited 1",
    ]
    logs["stdout"] = "\n".join(["INFO heartbeat ok", *kept, "INFO idle"]).encode()
    run.fence_paid_starts()
    names = _fence_order(shell, 0)
    assert names[:2] == ["logs-worker", "scale"]
    argv = next(argv for argv, _gate in shell.calls if _verb(argv[1:]) == "logs")
    assert argv[argv.index("logs") + 1 :] == [
        "-l",
        run.worker_selector,
        "--tail=400",
        "--prefix=false",
        "--all-containers=true",
    ]
    lines = ledger.observations("grant")["worker_escalation_lines"]
    assert lines == [line.replace(marker, "<redacted>") for line in kept]
    logs["stdout"] = "\n".join(f"ERROR attempt {index} failed" for index in range(70)).encode()
    run.capture_worker_escalation()
    lines = ledger.observations("grant")["worker_escalation_lines"]
    assert lines == [f"ERROR attempt {index} failed" for index in range(10, 70)]
    logs["code"] = 1
    run.capture_worker_escalation()
    assert ledger.observations("grant")["worker_escalation_lines"] == []
    ledger.record("grant", "worker_escalation_lines", ["stale"])
    logs["code"], logs["raise"] = 0, True
    run.capture_worker_escalation()
    assert ledger.observations("grant")["worker_escalation_lines"] == []
    assert marker not in ledger.dump()
    ledger.close()


def test_settlement_records_the_attempts_request_terminal_detail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: settlement records the attempt's own execution request status, terminal
    cause and termination observation, scrubbed, from the request row."""

    live = _live_run(tmp_path)
    run, ledger = live.run, live.ledger
    marker = "synthetic-termination-marker"
    run.scrubber.hold(sks.HeldSecret(marker))
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    run.work_item_id = WORK_ITEM
    ledger.record("admission", "expected_request_id", REQUEST)
    rows: list[dict[str, Any]] = [
        {
            "id": OTHER_REQUEST,
            "status": "completed",
            "terminal_cause": None,
            "termination_observation": None,
        },
        {
            "id": REQUEST,
            "status": "failed",
            "terminal_cause": "runner_escalated",
            "termination_observation": f"runner exited 1 holding {marker}",
        },
    ]
    queries: list[str] = []

    def sql(query: str) -> list[dict[str, Any]]:
        queries.append(query)
        return rows if "termination_observation" in query else []

    monkeypatch.setattr(run, "sql", sql)
    run.settle()
    assert ledger.observations("grant")["request_terminal"] == {
        "status": "failed",
        "terminal_cause": "runner_escalated",
        "termination_observation": "runner exited 1 holding <redacted>",
    }
    assert any(f"work_item_id = '{WORK_ITEM}'" in query for query in queries)
    ledger.close()


def test_follower_attaches_a_pending_claim_pod_and_audits_on_the_main_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: the follower thread lists the claim pods once per pass and attaches a
    Pending pod on its first pass; each stream into it is respawned while it exits
    silently and connects once output arrives; the audit runs only on the main thread's
    discovery, and the follower's stops are registered before it starts."""

    pending = _pod("acme-claim-1-runner", "uid-1", "Pending", running=False)
    running = _pod("acme-claim-1-runner", "uid-1", "Running", running=True)
    current = {"pod": pending}
    answer = _claim_answer("acme-claim-1", current)
    live = _live_run(tmp_path, answer=answer)
    run, ledger = live.run, live.ledger
    record = json.dumps({"kind": "process", "count": 1, "processes": []}).encode()
    shell = StreamShell(
        run.runtime,
        ledger,
        answer,
        {"exec": [[], [], [record]], "logs": [[], [b"runner booting\n"]]},
    )
    _streams_of(run, shell)
    clock = FollowerClock()
    run.clock = clock
    ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
    run.attempt_live, run.attempt_started = True, T0
    audits: list[str] = []
    audit = run.audit_pod

    def spy(observed: psk.RunnerPod, pod: dict[str, Any], claim: dict[str, Any]) -> None:
        audits.append(threading.current_thread().name)
        audit(observed, pod, claim)

    monkeypatch.setattr(run, "audit_pod", spy)
    run.start_follower()
    steps = [name for name, _step in run.finalizer.steps]
    assert steps[-2:] == ["stop-runner-streams", "stop-runner-follower"]
    assert clock.parked.get(timeout=30) == psk.FOLLOW_SECONDS  # the first pass ran
    observed = run.pods["uid-1"]
    for thread in observed.threads:
        thread.join(timeout=30)
    assert observed.eof.is_set()
    execs = [argv for argv in shell.spawned if "exec" in argv]
    streams = [argv for argv in shell.spawned if "logs" in argv]
    # Two silent harvester exits and one silent log stream were respawned until output.
    assert (len(execs), len(streams)) == (3, 2)
    assert json.loads(execs[0][-1])["interval"] == sks.CAPTURE_REPORT_SECONDS
    assert execs[0][execs[0].index("exec") :][:6] == [
        "exec",
        "-i",
        "acme-claim-1-runner",
        "-c",
        "runner",
        "--",
    ]
    assert streams[0][-5:] == ["logs", "-f", "acme-claim-1-runner", "-c", "runner"]
    assert observed.watch.ran and list(observed.log_tail) == ["runner booting"]
    listing = run.kube.argv("get", "pods", "-l", psk.POD_CLAIM_LABEL, "-o", "json")
    assert [argv for argv, _gate in shell.calls] == [listing]  # one listing per pass
    assert audits == [] and not observed.audited
    assert "runner_pods_seen" not in ledger.observations("grant")
    # The main thread's discovery audits the attached pod once it runs.
    current["pod"] = running
    with pytest.raises(psk.StepFailed, match="candidate runner layer"):
        run.discover_runner_pods()
    assert audits == [threading.main_thread().name]
    assert observed.audited and ledger.observations("grant")["runner_pods_seen"] == 1
    assert observed.watch.container_started == sks.runner_started_at(running)
    assert len(shell.spawned) == 5  # an attached pod is never attached again
    run.follow_stop.set()
    clock.go.put(None)  # release the parked follower; its stop is already set
    run.close_follower()
    assert run.follow_thread is not None and not run.follow_thread.is_alive()
    ledger.close()


def _diagnostic_run(tmp_path: Path) -> Live:
    """A ``rehearse --kickoff`` run: a rehearsal ledger holding the approved ceiling."""

    base = _live_run(tmp_path)
    base.ledger.close()
    run_id = f"{RUN_ID}-diagnostic"
    ledger = sks.Ledger.open(base.run.inputs.state_dir, run_id, scrubber=base.run.scrubber)
    ledger.ensure_run(CANDIDATE, phase="rehearse", now=T0)
    ledger.mark_live_kind("rehearse")
    ledger.record_ceiling(CEILING, sks.parse_ceiling(CEILING))
    shell = FakeShell(base.run.runtime, ledger, _empty)
    run = psk.LiveRun(
        inputs=dataclasses.replace(base.run.inputs, run_id=run_id),
        runtime=base.run.runtime,
        ledger=ledger,
        ceiling=sks.parse_ceiling(CEILING),
        shell=shell,
        resolver=lambda _host: ["192.0.2.10"],
        kickoff_diagnostic=True,
    )
    return Live(run, ledger, shell, base.clock)


def test_rehearse_kickoff_runs_the_paid_path_on_the_invalid_token_without_a_probe(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: ``rehearse --kickoff`` takes the ceiling and the live window, records the
    diagnostic mode on the rehearsal ledger, and runs kickoff and read-back before control
    and fence; its kickoff opens the live window and reserves without the grant probe.
    A plain rehearsal still has no kickoff, and neither ledger kind opens the other's window."""

    kit = _kit(tmp_path / "cli")
    assert kit.run("rehearse", (*FLAGS["rehearse"], *KICKOFF_FLAGS)) == sks.EXIT_FAIL
    payload, _text = _output(capsys)
    assert (payload["command"], payload["phase"]) == ("rehearse", "registry")
    assert kit.mutations() == [("docker", "run")]
    cli_ledger = sks.Ledger.open_existing(kit.state, RUN_ID)
    try:
        assert cli_ledger.live_kind() == "rehearse"
        assert cli_ledger.observations("rehearsal")["kickoff_diagnostic"] is True
        assert cli_ledger.observations("budget")["approved_ceiling"] == CEILING
    finally:
        cli_ledger.close()

    live = _diagnostic_run(tmp_path / "unit")
    run, ledger = live.run, live.ledger
    names = [name for name, _phase in run.phases()]
    assert names[names.index("fixtures") + 1 :] == ["kickoff", "read-back", "control", "fence"]
    assert run.rehearsal and run.command == "rehearse"
    plain = psk.LiveRun(
        inputs=run.inputs, runtime=run.runtime, ledger=ledger, ceiling=None, shell=live.shell
    )
    plain_names = [name for name, _phase in plain.phases()]
    assert "kickoff" not in plain_names and plain_names[-2:] == ["control", "fence"]
    with pytest.raises(sks.Refusal) as refused:
        ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    assert refused.value.code == "rehearsal_run_id"

    roles = sks.attempt_roles(sks.parse_ceiling(CEILING))
    ledger.record("images", "gate_limits", sks.reviewer_limits(roles))
    steps: list[str] = []

    def probe() -> None:
        raise AssertionError("the grant probe ran")

    monkeypatch.setattr(run, "run_grant_probe", probe)
    monkeypatch.setattr(run, "start_deadline_enforcer", lambda _ceiling: steps.append("deadline"))
    monkeypatch.setattr(run, "start_follower", lambda: steps.append("follower"))
    monkeypatch.setattr(run, "admit_kickoff", lambda _kit: steps.append("admit"))
    monkeypatch.setattr(run, "watch_attempt", lambda: steps.append("watch"))
    run.kit = object()  # type: ignore[assignment]
    run.phase_kickoff()
    assert steps == ["deadline", "follower", "admit", "watch"]
    assert ledger.observations("grant_probe") == {}
    assert ledger.run_row()["live_starts"] == 1
    assert [item.role for item in run.reservations] == list(roles)
    assert ledger.gate() == "closed" and run.guard_starts
    ledger.close()

    run_ledger = _run_ledger(_private(tmp_path / "run-state"))
    with pytest.raises(sks.Refusal) as refused:
        run_ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600, diagnostic=True)
    assert refused.value.code == "rehearsal_run_id"
    run_ledger.close()


OWN_HEAD = "a" * 40
MOVED_HEAD = "b" * 40


class FakeForge:
    """The fixture repository's branches and pull requests, as the GitHub API shapes them."""

    repo = "acme/fixture"

    def __init__(self) -> None:
        self.branches: dict[str, str] = {}
        self.pulls: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.closed: list[int] = []

    def open_pull(self, number: int, branch: str) -> None:
        head = {"sha": self.branches[branch], "ref": branch}
        self.pulls.append({"number": number, "state": "open", "head": head})

    def repo_path(self, suffix: str) -> str:
        return f"/repos/{self.repo}{suffix}"

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        prefix = f"/repos/{self.repo}"
        if method == "GET" and path.startswith(f"{prefix}/pulls?"):
            branch = urllib.parse.unquote(path.rsplit(":", 1)[1])
            return 200, [
                pull
                for pull in self.pulls
                if pull["state"] == "open" and pull["head"]["ref"] == branch
            ]
        if method == "PATCH" and path.startswith(f"{prefix}/pulls/"):
            number = int(path.rsplit("/", 1)[1])
            for pull in self.pulls:
                if pull["number"] == number:
                    pull["state"] = "closed"
            self.closed.append(number)
            return 200, {}
        if method == "GET" and path.startswith(f"{prefix}/git/ref/heads/"):
            branch = path.removeprefix(f"{prefix}/git/ref/heads/")
            if branch not in self.branches:
                return 404, {}
            return 200, {"object": {"sha": self.branches[branch]}}
        if method == "DELETE" and path.startswith(f"{prefix}/git/refs/heads/"):
            branch = path.removeprefix(f"{prefix}/git/refs/heads/")
            self.branches.pop(branch, None)
            self.deleted.append(branch)
            return 204, None
        raise AssertionError((method, path))

    def call(self, method: str, path: str, body: Any = None, *, ok: Sequence[int] = (200,)) -> Any:
        status, payload = self.request(method, path, body)
        if status not in ok:
            raise psk.StepFailed(f"forge {method} returned {status}")
        return payload

    def revoke(self) -> None:
        return None


class FakeApi:
    def __init__(self, records: Mapping[str, Mapping[str, Any]]) -> None:
        self.records = records

    def get(self, path: str) -> Any:
        return dict(self.records[path.rsplit("/", 1)[1]])


def _recovering_run(
    tmp_path: Path, forge: FakeForge, *, live: bool, publications: list[dict[str, Any]]
) -> Live:
    """A run whose publication rows and platform records exist but whose read-back never ran."""

    live_run = _live_run(tmp_path)
    run = live_run.run
    if live:
        live_run.ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
    run.mutated = True
    run.agent_id = str(uuid.uuid4())
    ids = [str(uuid.uuid4()) for _ in publications]
    queries: list[str] = []

    def sql(query: str) -> list[dict[str, Any]]:
        queries.append(query)
        return [{"id": item} for item in ids]

    run.sql = sql  # type: ignore[method-assign]
    run.forge = forge  # type: ignore[assignment]
    run.api = FakeApi(dict(zip(ids, publications, strict=True)))  # type: ignore[assignment]
    run.finalizer.register("close-run-pulls", run.close_run_pulls)
    run.finalizer.register("delete-run-branches", run.delete_run_branches)
    run.finalizer.register("recover-publications", run.recover_publications)
    run.queries = queries  # type: ignore[attr-defined]
    return live_run


def _inventory(live_run: Live) -> dict[str, str]:
    return {
        row["kind"].removeprefix("inventory:"): row["status"]
        for row in live_run.ledger.cleanup_rows()
        if row["kind"].startswith("inventory:")
    }


def test_finalize_cleans_a_publication_the_run_failed_before_reading_back(
    tmp_path: Path,
) -> None:
    """Behavior: a branch and pull request the platform published are found through the
    agent's work items, closed and deleted at their own head, even when the run failed
    before read-back; a foreign branch and a branch that moved off our commit stay."""

    forge = FakeForge()
    forge.branches = {"curie/pub-1": OWN_HEAD, "curie/pub-2": MOVED_HEAD, "someone/else": OWN_HEAD}
    forge.open_pull(7, "curie/pub-1")
    forge.open_pull(8, "someone/else")
    live_run = _recovering_run(
        tmp_path,
        forge,
        live=True,
        publications=[
            {"branch": "curie/pub-1", "lineage_head_sha": OWN_HEAD},
            {"branch": "curie/pub-2", "lineage_head_sha": OWN_HEAD},
        ],
    )
    run = live_run.run
    assert run.publication_branches == {}
    assert run.finalize() is False
    assert "JOIN curie.work_items" in run.queries[0]  # type: ignore[attr-defined]
    assert forge.closed == [7] and forge.deleted == ["curie/pub-1"]
    assert forge.branches == {"curie/pub-2": MOVED_HEAD, "someone/else": OWN_HEAD}
    assert _inventory(live_run)["forge_prs"] == "absent"
    assert _inventory(live_run)["forge_branches"] == "present"
    live_run.ledger.close()


def test_finalize_reports_absent_forge_state_after_recovering_an_own_publication(
    tmp_path: Path,
) -> None:
    forge = FakeForge()
    forge.branches = {"curie/pub-1": OWN_HEAD, "someone/else": MOVED_HEAD}
    forge.open_pull(7, "curie/pub-1")
    live_run = _recovering_run(
        tmp_path,
        forge,
        live=True,
        publications=[{"branch": "curie/pub-1", "lineage_head_sha": OWN_HEAD}],
    )
    live_run.run.finalize()
    steps = {row["kind"]: row["status"] for row in live_run.ledger.cleanup_rows()}
    assert steps["step:recover-publications"] == "ok"
    assert steps["step:close-run-pulls"] == "ok" and steps["step:delete-run-branches"] == "ok"
    assert (_inventory(live_run)["forge_prs"], _inventory(live_run)["forge_branches"]) == (
        "absent",
        "absent",
    )
    assert forge.branches == {"someone/else": MOVED_HEAD}
    live_run.ledger.close()


def test_failed_publication_enumeration_after_going_live_is_uncertain(tmp_path: Path) -> None:
    """Behavior: when the attempt went live and the platform cannot list the run's
    publications, nothing on the forge is touched and the inventory is uncertain."""

    forge = FakeForge()
    forge.branches = {"curie/pub-1": OWN_HEAD}
    forge.open_pull(7, "curie/pub-1")
    live_run = _recovering_run(
        tmp_path,
        forge,
        live=True,
        publications=[{"branch": "curie/pub-1", "lineage_head_sha": OWN_HEAD}],
    )
    run = live_run.run

    def broken(_query: str) -> list[dict[str, Any]]:
        raise psk.StepFailed("the database is unreachable")

    run.sql = broken  # type: ignore[method-assign,assignment]
    assert run.finalize() is False
    inventory = _inventory(live_run)
    assert (inventory["forge_prs"], inventory["forge_branches"]) == ("uncertain", "uncertain")
    assert forge.closed == [] and forge.deleted == []
    steps = {row["kind"]: row["status"] for row in live_run.ledger.cleanup_rows()}
    assert steps["step:close-run-pulls"] == "failed" and steps["step:recover-publications"] == (
        "failed"
    )
    live_run.ledger.close()


def test_missing_api_client_after_going_live_is_uncertain_but_not_before(tmp_path: Path) -> None:
    forge = FakeForge()
    live_run = _recovering_run(
        tmp_path, forge, live=True, publications=[{"branch": "x", "lineage_head_sha": OWN_HEAD}]
    )
    live_run.run.api = None
    with pytest.raises(psk.StepFailed):
        live_run.run.pulls_closed()
    live_run.ledger.close()
    early = _recovering_run(tmp_path / "early", forge, live=False, publications=[])
    early.run.api = None
    assert early.run.pulls_closed() is True and early.run.branches_absent() is True
    early.ledger.close()


def test_a_publication_without_a_commit_head_is_tracked_but_never_touched(
    tmp_path: Path,
) -> None:
    forge = FakeForge()
    forge.branches = {"curie/pub-1": OWN_HEAD}
    forge.open_pull(7, "curie/pub-1")
    live_run = _recovering_run(
        tmp_path,
        forge,
        live=True,
        publications=[{"branch": "curie/pub-1", "lineage_head_sha": None}],
    )
    run = live_run.run
    run.recover_publications()
    assert run.publication_branches == {"curie/pub-1": ""}
    with pytest.raises(psk.StepFailed, match="not provably"):
        run.close_run_pulls()
    with pytest.raises(psk.StepFailed, match="not provably"):
        run.delete_run_branches()
    assert forge.closed == [] and forge.deleted == []
    assert run.pulls_closed() is False and run.branches_absent() is False
    live_run.ledger.close()


def test_harvest_stream_settles_only_after_a_post_exit_size_report(tmp_path: Path) -> None:
    """Behavior: the real in-pod harvester's stream proves a complete capture only once the
    native CLI exited and a size report followed; the grant's bytes never leave the pod."""

    files = _pod_files(
        _turns("msg_impl", IMPLEMENTER_MODEL, 3, 120, start=1),
        [("plan01", PLAN_KIND, _turns("msg_plan", REVIEWER_MODEL, 2, 200, start=5))],
    )
    tmp_root = tmp_path / "pod" / "tmp"
    for name, data in files.items():
        target = tmp_root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    proc = tmp_path / "pod" / "proc"
    (proc / "101").mkdir(parents=True)
    (proc / "stat").write_text("cpu  1 2 3 4\nbtime 1700000000\n")
    (proc / "101" / "cmdline").write_bytes(b"claude\0--print\0")
    (proc / "101" / "stat").write_text("101 (claude) S " + " ".join(["0"] * 18) + " 12345 0 0\n")
    (proc / "101" / "environ").write_bytes(f"{sks.GRANT_KEY}={GRANT_MARKER}\0".encode())
    config = sks.harvest_config(
        nonce="0f0f1e1e2d2d3c3c",
        tmp_root=str(tmp_root),
        review_dir=str(tmp_root / "dark-factory-review"),
        workspace=str(tmp_path / "pod" / "workspace"),
        proc_root=str(proc),
        once=True,
    )
    result = subprocess.run(
        [sys.executable, "-c", sks.HARVEST_PROGRAM, config],
        env={"PATH": "/usr/bin:/bin", sks.GRANT_KEY: GRANT_MARKER},
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert GRANT_MARKER.encode() not in result.stdout
    scrubber = sks.Scrubber()
    scrubber.hold(sks.HeldSecret(GRANT_MARKER)).clear()
    scan = sks.TokenScan(scrubber)
    harvest, watch, trace = sks.HarvestState(scan), sks.ProcessWatch(), sks.CaptureTrace()
    for line in result.stdout.splitlines():
        harvest.feed(line)
        watch.feed(line, now=T0 + 10)
        trace.feed_harvest(line)
    assert isinstance(scan.channels["harvest"], sks.ChannelScan) and sks.scan_clean(scan)
    assert (harvest.container_credential or {}).get("credential") == "present"
    assert harvest.transcript_files() == files
    assert harvest.complete and watch.ran and not watch.exited
    assert watch.running and watch.started == T0 + 10
    sizes = {name: len(data) for name, data in files.items() if name.endswith(".jsonl")}
    # The real program reports bytes read right after its read pass, then ends on its own
    # deadline (once): the trace holds that report and the deadline end.
    assert [reported for _at, reported in trace.reports] == [sizes]
    assert trace.harvester_ended is True
    failures = sks.capture_failures(
        trace,
        harvest.transcript_files(),
        delivered=harvest.complete,
        pod_gone=True,
        request_terminal=True,
        runner_pods=1,
    )
    assert {"harvester_deadline", "no_turn_end"} <= set(failures)
    facts = sks.attempt_facts([harvest.transcript_files()], not_before=T0 - 30)
    summary = {item["role"]: item for item in map(sks.session_summary, facts.sessions)}
    assert summary[sks.IMPLEMENTER]["output_tokens"] == 360
    assert summary[sks.PLAN_REVIEWER]["models"] == [REVIEWER_MODEL]


TURN_END = T0 + 181.0


def _final(
    message_id: str, model: str, tokens: int, at: float, *, stop: str | None = "end_turn"
) -> dict[str, Any]:
    """An assistant record that ends a turn; ``stop=None`` writes a null stop reason."""

    record = _assistant(message_id, model, tokens, at)
    record["message"]["stop_reason"] = stop
    return record


def _agent_call(use_id: str, kind: str, at: float) -> dict[str, Any]:
    record = _final(f"msg_{use_id}", IMPLEMENTER_MODEL, 40, at, stop="tool_use")
    record["message"]["content"] = [
        {"type": "tool_use", "id": use_id, "name": "Agent", "input": {"subagent_type": kind}}
    ]
    return record


def _agent_result(use_id: str, agent: str, at: float) -> dict[str, Any]:
    """The tool result the native CLI writes when a subagent finishes (toolUseResult)."""

    record = _user(at)
    record["message"]["content"] = [
        {"type": "tool_result", "tool_use_id": use_id, "content": "review done"}
    ]
    record["toolUseResult"] = {"agentId": agent, "status": "completed"}
    return record


def _capture_files(
    *, main_stop: str | None = "end_turn", plan_stop: str | None = "end_turn", diff: bool = True
) -> dict[str, bytes]:
    """Attempt 2's sanitized shape: the implementer's 17 turns and 7,024 output tokens, a
    plan review of 4 turns and 682 tokens and a diff review of 3 turns and 901 tokens,
    each transcript ending in its result frame."""

    main = [
        _user(0),
        *_turns("msg_impl", IMPLEMENTER_MODEL, 14, 480, start=1),
        _agent_call("toolu_plan", PLAN_KIND, 20),
        _agent_result("toolu_plan", "plan01", 35),
        _agent_call("toolu_diff", DIFF_KIND, 140),
        _agent_result("toolu_diff", "diff01", 174),
        _final("msg_impl_end", IMPLEMENTER_MODEL, 224, 180, stop=main_stop),
    ]
    plan = [
        *_turns("msg_plan", REVIEWER_MODEL, 3, 150, start=21),
        _final("msg_plan_end", REVIEWER_MODEL, 232, 34, stop=plan_stop),
    ]
    review = [
        *_turns("msg_diff", REVIEWER_MODEL, 2, 300, start=141),
        _final("msg_diff_end", REVIEWER_MODEL, 301, 173),
    ]
    reviewers = [("plan01", PLAN_KIND, plan), ("diff01", DIFF_KIND, review)]
    return _pod_files(main, reviewers if diff else reviewers[:1])


def _runner_log(kind: str, at: float) -> bytes:
    """The runner's own turn record (curie_runner.session), in attempt 2's recorded shape."""

    message = (
        f"turn start session=agent-{SESSION} user=slack:{SENDER_USER}"
        if kind == "start"
        else f"turn end session=agent-{SESSION} status=awaiting-approval duration_ms=182890"
    )
    return json.dumps(
        {
            "logger": "curie_runner.session",
            "message": message,
            "service.name": "curie-runner",
            "severity": "INFO",
            "timestamp": dt.datetime.fromtimestamp(at, dt.UTC).isoformat(),
        }
    ).encode()


def _report(at: float, sizes: Mapping[str, int]) -> bytes:
    """The harvester's size report (``sync`` in personal_profile_support.HARVEST_PROGRAM)."""

    return json.dumps({"kind": "sync", "sizes": dict(sizes), "at": round(at, 3)}).encode()


def _capture_trace(
    files: Mapping[str, bytes],
    *,
    after: Sequence[float] = (0.1, 0.2),
    late_start: bool = False,
    grow: bool = False,
    drop: bool = False,
    ended: bool = False,
) -> sks.CaptureTrace:
    """A runner pod's trace: turn start, reports while it ran, the turn end, then a report
    at each ``after`` offset; ``grow`` lengthens the last one and ``drop`` omits a file."""

    sizes = {name: len(data) for name, data in files.items() if name.endswith(".jsonl")}
    trace = sks.CaptureTrace()
    trace.feed_log(_runner_log("start", T0 + 1.0))
    trace.feed_harvest(_report(T0 + 60, {name: size // 2 for name, size in sizes.items()}))
    trace.feed_harvest(_report(TURN_END - 0.05, sizes))
    trace.feed_log(_runner_log("end", TURN_END))
    if late_start:
        trace.feed_log(_runner_log("start", TURN_END + 0.15))
    for index, offset in enumerate(after):
        report = dict(sizes)
        last = index == len(after) - 1
        if grow and last:
            report[next(iter(report))] += 120
        if drop and last:
            report.pop(next(iter(report)))
        trace.feed_harvest(_report(TURN_END + offset, report))
    if ended:
        trace.feed_harvest(json.dumps({"kind": "end"}).encode())
    return trace


def _affirm(files: Mapping[str, bytes], trace: sks.CaptureTrace, **evidence: Any) -> list[str]:
    values = {
        "delivered": True,
        "pod_gone": True,
        "request_terminal": True,
        "runner_pods": 1,
        **evidence,
    }
    return sks.capture_failures(trace, files, **values)


@pytest.mark.parametrize("stop", sorted(sks.FINAL_STOP_REASONS))
def test_capture_is_affirmed_after_the_runner_turn_end_in_attempt_twos_shape(stop: str) -> None:
    """Behavior: two matching size reports after the runner's turn-end record, whole
    transcripts ending in a stop reason that means the model stopped, every subagent's
    transcript, one deleted pod and a terminal request affirm the capture, and the usage
    comes from the transcripts."""

    files = _capture_files(main_stop=stop)
    trace = _capture_trace(files)
    assert _affirm(files, trace) == []
    facts = sks.attempt_facts([files], not_before=T0 - 30)
    observed = {
        item["role"]: (item["output_tokens"], item["assistant_turns"])
        for item in map(sks.session_summary, facts.sessions)
    }
    assert observed == {
        sks.IMPLEMENTER: (7024, 17),
        sks.PLAN_REVIEWER: (682, 4),
        sks.DIFF_REVIEWER: (901, 3),
    }
    sanitized = trace.sanitized()
    assert [run["count"] for run in sanitized["reports"]] == [1, 3]
    assert sanitized["turns"] == [["start", T0 + 1.0], ["end", TURN_END]]
    assert not any(name in json.dumps(sanitized) for name in files)


@pytest.mark.parametrize(
    ("files", "trace", "evidence", "failure"),
    [
        pytest.param(None, {"after": ()}, {}, "reports_after_turn_end", id="report-before-end"),
        pytest.param(
            None, {"late_start": True}, {}, "turn_started_after_turn_end", id="turn-after-end"
        ),
        pytest.param(None, {"grow": True}, {}, "growth_after_turn_end", id="growth-between"),
        pytest.param(None, {"after": (0.1,)}, {}, "reports_after_turn_end", id="one-report"),
        pytest.param(None, {"ended": True}, {}, "harvester_deadline", id="harvester-deadline"),
        pytest.param(None, {"drop": True}, {}, "report_dropped_a_file", id="report-drops-file"),
        pytest.param(
            {"main_stop": "tool_use"}, {}, {}, "result_frame_missing", id="final-is-tool-use"
        ),
        pytest.param({"main_stop": None}, {}, {}, "result_frame_missing", id="null-stop"),
        pytest.param({"plan_stop": None}, {}, {}, "result_frame_missing", id="subagent-incomplete"),
        pytest.param({"diff": False}, {}, {}, "subagent_transcript_missing", id="no-subagent"),
        pytest.param(
            None, {}, {"request_terminal": False}, "request_not_terminal", id="never-terminal"
        ),
        pytest.param(None, {}, {"runner_pods": 2}, "runner_pods", id="further-runner-pod"),
        pytest.param(None, {}, {"pod_gone": False}, "pod_not_deleted", id="pod-not-deleted"),
        pytest.param(None, {}, {"delivered": False}, "bytes_not_delivered", id="undelivered"),
    ],
)
def test_capture_is_not_affirmed_when_any_recorded_check_fails(
    files: dict[str, Any] | None,
    trace: dict[str, Any],
    evidence: dict[str, Any],
    failure: str,
) -> None:
    """Behavior: each check of the capture rule fails closed on its own."""

    harvested = _capture_files(**(files or {}))
    assert failure in _affirm(harvested, _capture_trace(harvested, **trace), **evidence)


def test_capture_is_not_affirmed_for_a_cut_missing_or_unanswered_transcript() -> None:
    """Behavior: a cut final line, a missing stop reason, an unanswered tool use and a
    subagent result with no agent id each fail the transcript checks."""

    files = _capture_files()
    main = next(name for name in files if name.endswith(f"{SESSION}.jsonl"))
    cut = {**files, main: files[main][:-1]}
    assert "transcript_truncated" in sks.transcript_failures(cut)
    records = [json.loads(line) for line in files[main].splitlines()]
    del records[-1]["message"]["stop_reason"]
    assert "result_frame_missing" in sks.transcript_failures({**files, main: _jsonl(records)})
    asking = [*records[:-1], _agent_call("toolu_more", DIFF_KIND, 179)]
    assert "tool_use_unanswered" in sks.transcript_failures({**files, main: _jsonl(asking)})
    records = [json.loads(line) for line in files[main].splitlines()]
    del records[-2]["toolUseResult"]
    assert "subagent_transcript_missing" in sks.transcript_failures(
        {**files, main: _jsonl(records)}
    )


def test_settlement_takes_native_usage_only_once_capture_is_affirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: settlement affirms the capture from one pod's recorded streams, takes each
    slot's usage from the transcripts and records the sanitized trace; the same streams
    without the second report after the turn end leave every slot unknown."""

    for after, sources in (
        ((0.1, 0.2), ["native", "not_started", "native", "native", "not_started"]),
        ((0.1,), ["unknown"] * 5),
    ):
        live = _live_run(tmp_path / str(len(after)))
        run, ledger = live.run, live.ledger
        ledger.begin_live(now=T0, grant_expires_at=T0 + 3 * 3600)
        run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=T0)
        run.attempt_live, run.attempt_started = True, T0
        pod = _runner_pod(run, "uid-1")
        run.pods[pod.uid] = pod
        files = _capture_files()
        _feed(pod, {"kind": "process", "count": 1, "processes": []})
        for name, data in files.items():
            if name.endswith(".meta.json"):
                encoded = base64.b64encode(data).decode()
                _feed(pod, {"kind": "meta", "path": name, "data": encoded})
            else:
                _feed(pod, _file_record(name, 0, data))
        reports = _capture_trace(files, after=after)
        pod.trace.turns = reports.turns
        for at, sizes in reports.reports:
            _feed(pod, {"kind": "sync", "sizes": sizes, "at": at})
        pod.eof.set()
        monkeypatch.setattr(run, "requests_terminal", lambda: True)
        monkeypatch.setattr(run, "pods_gone", lambda: {"uid-1": True})
        # Settlement charges native usage only for responses the guard has seen.
        live.clock.at = T0 + 190
        run.tick()
        run.settle()
        rows = ledger.sessions()
        assert [row["source"] for row in rows] == sources
        capture = ledger.observations("budget")["capture"]
        assert capture["affirmed"] is (len(after) == 2)
        assert capture["aci_usage"] == "not_carried"
        assert not any(name in json.dumps(capture) for name in files)
        if len(after) == 2:
            assert [(row["output_tokens"], row["assistant_turns"]) for row in rows] == [
                (7024, 17),
                (0, 0),
                (682, 4),
                (901, 3),
                (0, 0),
            ]
        else:
            assert capture["failures"] == ["reports_after_turn_end"]
        ledger.close()


def test_a_runner_pod_counts_as_gone_only_when_deleted_or_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: a pod being deleted or absent from the listing is gone, a listed pod
    without a deletion time is not, and an unreadable listing proves nothing."""

    live = _live_run(tmp_path)
    run = live.run
    for uid in ("uid-1", "uid-2", "uid-3"):
        run.pods[uid] = _runner_pod(run, uid, name=f"acme-claim-{uid}-runner")
    listing = {
        "items": [
            {"metadata": {"uid": "uid-1", "deletionTimestamp": "2026-11-13T00:08:42Z"}},
            {"metadata": {"uid": "uid-2"}},
        ]
    }
    monkeypatch.setattr(run.kube, "json", lambda *_args, **_kw: listing)
    assert run.pods_gone() == {"uid-1": True, "uid-2": False, "uid-3": True}
    monkeypatch.setattr(run.kube, "json", lambda *_args, **_kw: {"kind": "Status"})
    assert run.pods_gone() == {}
    live.ledger.close()


def test_capture_findings_name_the_file_and_field_and_the_skeleton_keeps_no_content() -> None:
    """Behavior: each capture failure names its file (a path digest) and the field it read,
    and the transcript skeleton keeps each record's shape and usage with no content."""

    files = _capture_files(main_stop=None)
    main = next(name for name in files if name.endswith(f"{SESSION}.jsonl"))
    findings = sks.capture_findings(
        _capture_trace(files, grow=True),
        files,
        delivered=True,
        pod_gone=True,
        request_terminal=True,
        runner_pods=1,
    )
    digest = sks.ref_digest(main)
    assert {
        "code": "result_frame_missing",
        "file": digest,
        "field": "message.stop_reason",
        "value": None,
    } in findings
    grown = [item for item in findings if item["code"] == "growth_after_turn_end"]
    assert [(item["file"], item["field"]) for item in grown] == [(digest, "sync.sizes")]
    skeleton = sks.transcript_skeleton(files)
    entry = skeleton[digest]
    assert entry["kind"] == "main" and entry["bytes"] == len(files[main])
    last = entry["records"][-1]
    assert (last["type"], last["stop_reason"], last["has_usage"]) == ("assistant", None, True)
    assert last["usage"] == {"input_tokens": 10, "output_tokens": 224}
    assert last["offset"] + len(files[main].splitlines()[-1]) + 1 == len(files[main])
    assert [r["result_agent_id"] for r in entry["records"] if r["result_agent_id"]] == [
        "plan01",
        "diff01",
    ]
    text = json.dumps(skeleton)
    assert not any(word in text for word in ("working", "review done", "work the issue"))


def _noid(model: str, tokens: int, at: float, *, stop: str | None = None) -> dict[str, Any]:
    """An assistant record that carries usage and has no message id."""

    record = _assistant("placeholder", model, tokens, at)
    del record["message"]["id"]
    if stop is not None:
        record["message"]["stop_reason"] = stop
    return record


def _budgeted(tmp_path: Path) -> Live:
    """A live attempt with the budget observations the judge reads, and one runner pod."""

    live = _live_run(tmp_path)
    ledger, run = live.ledger, live.run
    now = time.time()
    ledger.record_item_ledger(32768, 8607, ledger.ceiling())
    ledger.record("budget", "allowance_breach", [])
    ledger.record("budget", "tool_search_calls", 0)
    ledger.record("grant_probe", "started", {"at": now, "model": sks.GRANT_PROBE_MODEL})
    ledger.record("grant_probe", "model", sks.GRANT_PROBE_MODEL)
    ledger.record("grant_probe", "output_cap", sks.GRANT_PROBE_OUTPUT_CAP)
    ledger.record("grant_probe", "turn_cap", sks.GRANT_PROBE_TURNS)
    ledger.record(
        "grant_probe",
        "result",
        {"ok": True, "turns": 1, "output_tokens": 4, "models": [sks.GRANT_PROBE_MODEL]},
    )
    ledger.begin_live(now=now, grant_expires_at=now + 3 * 3600)
    run.reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=time.time() + 1)
    run.attempt_live, run.attempt_started = True, T0
    pod = _runner_pod(run, "uid-1")
    run.pods[pod.uid] = pod
    return live


def _feed_transcripts(pod: psk.RunnerPod, files: Mapping[str, bytes]) -> None:
    _feed(pod, {"kind": "process", "count": 1, "processes": []})
    for name, data in files.items():
        if name.endswith(".meta.json"):
            _feed(pod, {"kind": "meta", "path": name, "data": base64.b64encode(data).decode()})
        else:
            _feed(pod, _file_record(name, 0, data))


def _affirm_capture(
    run: psk.LiveRun,
    pod: psk.RunnerPod,
    files: Mapping[str, bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports = _capture_trace(files)
    pod.trace.turns = reports.turns
    for at, sizes in reports.reports:
        _feed(pod, {"kind": "sync", "sizes": sizes, "at": at})
    pod.eof.set()
    monkeypatch.setattr(run, "requests_terminal", lambda: True)
    monkeypatch.setattr(run, "pods_gone", lambda: {pod.uid: True})


def test_an_idless_usage_record_is_a_response_the_guard_must_see(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: an assistant record with usage and no id keeps a stable key, counts in the
    token total, and the ceiling passes once the guard has seen that key. A guard that
    never saw it leaves the session unknown."""

    synthetic = _assistant("msg_synth", "<synthetic>", 0, 0.5)
    identified = _assistant("msg_real", IMPLEMENTER_MODEL, 40, 1)
    missing = _noid(IMPLEMENTER_MODEL, 60, 2, stop="end_turn")
    records = [_user(0), synthetic, identified, missing]
    files = _pod_files(records)
    session = sks.attempt_facts([files], not_before=T0 - 30).sessions[0]
    path = next(name for name in files if name.endswith(".jsonl")).removesuffix(".jsonl")
    # The synthetic record takes ordinal 0 and the identified record ordinal 1.
    digest = hashlib.sha256(f"{path}\0{2}".encode()).hexdigest()[:16]
    assert session.message_ids == {f"noid:{digest}", "msg_real"}
    assert (session.output_tokens, session.assistant_turns, session.synthetic) == (100, 2, 1)
    longer = _pod_files([*records, _noid(IMPLEMENTER_MODEL, 10, 3)])
    again = sks.attempt_facts([longer], not_before=T0 - 30).sessions[0]
    assert f"noid:{digest}" in again.message_ids and len(again.message_ids) == 3

    live = _budgeted(tmp_path / "seen")
    run, ledger, pod = live.run, live.ledger, live.run.pods["uid-1"]
    _feed_transcripts(pod, files)
    live.clock.at = T0 + 10
    run.tick()
    assert run.guard_checks == 1 and session.message_ids <= run.guard_responses
    _affirm_capture(run, pod, files, monkeypatch)
    run.settle()
    guard = ledger.observations("budget")["guard_checks"]
    assert (guard["checks"], guard["responses"], guard["responses_checked"]) == (1, 2, 2)
    assert guard["sessions"][0]["turns"] == guard["sessions"][0]["responses"] == 2
    assert ledger.sessions()[0]["source"] == "native"
    assert ledger.sessions()[0]["output_tokens"] == 100
    assert sks.ceiling_enforcement_failures(ledger) == []
    assert sks.judge(ledger, "budget")["checks"]["budget.ceiling_enforced"] is True
    ledger.close()

    missed = _budgeted(tmp_path / "missed")
    run, ledger, pod = missed.run, missed.ledger, missed.run.pods["uid-1"]
    main = next(name for name in files if name.endswith(".jsonl"))
    head = _jsonl([_user(0), synthetic, identified])
    tail = _jsonl([missing])
    assert files[main] == head + tail
    _feed(pod, {"kind": "process", "count": 1, "processes": []}, _file_record(main, 0, head))
    missed.clock.at = T0 + 10
    run.tick()
    assert run.guard_checks == 1 and run.guard_responses == {"msg_real"}
    _feed(pod, _file_record(main, len(head), tail))
    _affirm_capture(run, pod, files, monkeypatch)
    run.settle()
    assert ledger.sessions()[0]["source"] == "unknown"
    assert ledger.sessions()[0]["output_tokens"] is None
    assert "guard_missed_response" in sks.ceiling_enforcement_failures(ledger)
    ledger.close()


def test_a_session_whose_responses_are_all_idless_passes_only_when_guarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behavior: a session whose every assistant record lacks an id still has one response
    per usage record, and the ceiling passes when the guard saw each of those keys."""

    records = [
        _user(0),
        _noid(IMPLEMENTER_MODEL, 30, 1),
        _noid(IMPLEMENTER_MODEL, 70, 2, stop="end_turn"),
    ]
    files = _pod_files(records)
    session = sks.attempt_facts([files], not_before=T0 - 30).sessions[0]
    assert session.assistant_turns == 2 and len(session.message_ids) == 2
    assert all(key.startswith("noid:") for key in session.message_ids)
    live = _budgeted(tmp_path)
    run, ledger, pod = live.run, live.ledger, live.run.pods["uid-1"]
    _feed_transcripts(pod, files)
    live.clock.at = T0 + 10
    run.tick()
    assert run.guard_checks == 1 and run.guard_responses == set(session.message_ids)
    _affirm_capture(run, pod, files, monkeypatch)
    run.settle()
    guard = ledger.observations("budget")["guard_checks"]
    assert (guard["checks"], guard["responses"], guard["responses_checked"]) == (1, 2, 2)
    assert guard["sessions"][0]["turns"] == 2 and guard["sessions"][0]["guard_after_first"] is True
    assert ledger.sessions()[0]["source"] == "native"
    assert ledger.sessions()[0]["output_tokens"] == 100
    assert sks.ceiling_enforcement_failures(ledger) == []
    assert sks.judge(ledger, "budget")["checks"]["budget.ceiling_enforced"] is True
    ledger.close()


def test_a_launched_session_with_no_response_is_not_enforced(tmp_path: Path) -> None:
    """Behavior: a slot the launched set names, whose harvest holds no response, fails
    the ceiling, including when a check was recorded against that empty harvest."""

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        files = _pod_files([_user(0)])
        facts = sks.attempt_facts([files], not_before=T0 - 30)
        assert facts.sessions[0].message_ids == frozenset()
        assert facts.sessions[0].assistant_turns == 0
        reservations = [
            sks.Reservation(
                int(row["seq"]),
                str(row["role"]),
                int(row["reserved_output"]),
                int(row["reserved_turns"]),
                float(row["reserved_seconds"]),
            )
            for row in ledger.sessions()
        ]
        launched = ledger.observations("budget")["launched_sessions"]
        ledger.record(
            "budget",
            "guard_checks",
            sks.guard_coverage(reservations, facts.sessions, (), checks=1, launched=launched),
        )
        result = sks.judge(ledger, "budget")
        assert result["checks"]["budget.ceiling_enforced"] is False
        assert "responses_absent" in result["observations"]["budget.ceiling_enforcement_failures"]
    finally:
        ledger.close()


def test_a_guard_check_on_an_empty_harvest_does_not_count(tmp_path: Path) -> None:
    """Behavior: watchdog ticks before any response do not count as checks. The pod
    watcher records the launch, and that session still having no response fails."""

    files = _pod_files([_user(0)])
    live = _budgeted(tmp_path)
    run, ledger, pod = live.run, live.ledger, live.run.pods["uid-1"]
    _feed_transcripts(pod, files)
    live.clock.at = T0 + 10
    run.tick()
    run.tick()
    assert run.guard_checks == 0
    run.settle()
    guard = ledger.observations("budget")["guard_checks"]
    assert guard["checks"] == 0 and guard["responses"] == 0
    assert ledger.observations("budget")["launched_sessions"] == [
        {"role": sks.IMPLEMENTER, "seq": 1}
    ]
    assert guard["sessions"][0]["launched"] is True and guard["sessions"][0]["responses"] == 0
    assert sks.ceiling_enforcement_failures(ledger) == ["responses_absent"]
    ledger.close()


def test_turn_starts_name_distinct_runner_sessions() -> None:
    """Behavior: each distinct turn-start session takes the next implementer slot, a
    repeated id does not, and a process that ran with no turn start takes one slot."""

    roles = [
        sks.Reservation(1, sks.IMPLEMENTER, 1, 1, 1),
        sks.Reservation(2, sks.IMPLEMENTER, 1, 1, 1),
        sks.Reservation(3, sks.PLAN_REVIEWER, 1, 1, 1),
    ]
    starts = [("agent-a", 1.0), ("agent-a", 2.0), ("agent-b", 3.0)]
    assert sks.launched_sessions(roles, [{"ran": True, "turn_starts": starts}]) == [
        {"role": sks.IMPLEMENTER, "seq": 1},
        {"role": sks.IMPLEMENTER, "seq": 2},
    ]
    assert sks.launched_sessions(roles, [{"ran": True, "turn_starts": ()}]) == [
        {"role": sks.IMPLEMENTER, "seq": 1}
    ]
    assert sks.launched_sessions(roles[:1], [{"ran": False, "turn_starts": starts}]) == [
        {"role": sks.IMPLEMENTER, "seq": 1},
        {"role": sks.IMPLEMENTER},
    ]


def _empty_guard() -> dict[str, Any]:
    return {"checks": 0, "responses": 0, "responses_checked": 0, "sessions": []}


def test_an_empty_harvest_on_a_completed_run_is_not_enforced(tmp_path: Path) -> None:
    """Behavior: a completed run whose harvest is empty and whose launched set names no
    implementer fails the ceiling. The empty guard record alone does not."""

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        assert ledger.observations("progress")["pull_request_exists"] is True
        ledger.record("budget", "launched_sessions", [])
        ledger.record("budget", "guard_checks", _empty_guard())
        result = sks.judge(ledger, "budget")
        assert result["checks"]["budget.ceiling_enforced"] is False
        assert result["observations"]["budget.ceiling_enforcement_failures"] == [
            "launched_set_empty"
        ]
    finally:
        ledger.close()


def test_a_launched_session_missing_from_the_harvest_is_not_enforced(
    tmp_path: Path,
) -> None:
    """Behavior: a runner turn start with no harvested row fails the ceiling, and the
    launch is recorded from that log rather than from the harvest."""

    live = _budgeted(tmp_path / "driver")
    run, ledger, pod = live.run, live.ledger, live.run.pods["uid-1"]
    pod.trace.feed_log(_runner_log("start", T0 + 1))
    run.settle()
    assert ledger.observations("budget")["launched_sessions"] == [
        {"role": sks.IMPLEMENTER, "seq": 1}
    ]
    assert ledger.observations("budget")["guard_checks"]["sessions"] == []
    assert sks.ceiling_enforcement_failures(ledger) == ["launched_unharvested"]
    ledger.close()

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    recorded = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        launched = recorded.observations("budget")["launched_sessions"]
        recorded.record(
            "budget",
            "launched_sessions",
            [*launched, {"seq": 99, "role": sks.IMPLEMENTER}],
        )
        result = sks.judge(recorded, "budget")
        assert result["checks"]["budget.ceiling_enforced"] is False
        assert result["observations"]["budget.ceiling_enforcement_failures"] == [
            "launched_unharvested"
        ]
    finally:
        recorded.close()


def test_a_missing_launched_set_is_not_enforced(tmp_path: Path) -> None:
    """Behavior: a budget ledger with no launched-set observation fails the ceiling,
    even when every harvested row is otherwise covered."""

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        with ledger.transaction() as db:
            db.execute(
                "DELETE FROM observations WHERE run_id=? AND criterion='budget' "
                "AND key='launched_sessions'",
                (RUN_ID,),
            )
        result = sks.judge(ledger, "budget")
        assert result["checks"]["budget.ceiling_enforced"] is False
        assert result["observations"]["budget.ceiling_enforcement_failures"] == [
            "launched_set_missing"
        ]
    finally:
        ledger.close()


def test_ceiling_is_enforced_when_the_watchdog_saw_every_recorded_response(
    tmp_path: Path,
) -> None:
    """Behavior: one non-empty watchdog check over the attempt's transcripts, then
    settlement, passes the ceiling. Capture is not affirmed, so every slot is charged
    its full share, and that charge stays inside the recorded item cap."""

    files = _attempt_files()
    parsed = sks.attempt_facts([files], not_before=T0 - 30)
    expected = sum(len(session.message_ids) for session in parsed.sessions)
    live = _budgeted(tmp_path)
    run, ledger, pod = live.run, live.ledger, live.run.pods["uid-1"]
    _feed_transcripts(pod, files)
    live.clock.at = T0 + 130
    run.tick()
    assert run.guard_checks == 1
    run.settle()
    guard = ledger.observations("budget")["guard_checks"]
    assert expected >= 1
    assert (guard["checks"], guard["responses"], guard["responses_checked"]) == (
        1,
        expected,
        expected,
    )
    assert ledger.observations("budget")["launched_sessions"] == [
        {"role": sks.IMPLEMENTER, "seq": 1}
    ]
    assert [row["launched"] for row in guard["sessions"]] == [True, False, False]
    assert all(row["responses"] >= 1 and row["guard_after_first"] for row in guard["sessions"])
    result = sks.judge(ledger, "budget")
    assert result["verdict"] == "pass", result["checks"]
    assert result["checks"]["budget.ceiling_enforced"] is True
    assert result["counts"]["budget.item_charged"] == 8607 + 16384
    assert result["observations"]["budget.usage_exact"]["exact"] is False
    assert result["observations"]["budget.usage_exact"]["unknown_slots"] == 5
    assert result["observations"]["budget.ceiling_enforcement_failures"] == []
    ledger.close()


def _attempt_three_ledger(state: Path) -> sks.Ledger:
    """Unknown full-share charges, with the guard recorded from the parsed transcripts."""

    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    with ledger.transaction() as db:
        db.execute(
            "UPDATE sessions SET output_tokens=NULL, assistant_turns=NULL, elapsed=NULL, "
            "source='unknown' WHERE run_id=?",
            (RUN_ID,),
        )
        db.execute("UPDATE runs SET budget_state='unknown' WHERE run_id=?", (RUN_ID,))
    codes = ["growth_after_turn_end", "result_frame_missing", "subagent_transcript_missing"]
    ledger.record("budget", "capture", {"affirmed": False, "failures": codes})
    return ledger


def _over_bound(ledger: sks.Ledger) -> None:
    with ledger.transaction() as db:
        db.execute("UPDATE sessions SET reserved_output=9000 WHERE run_id=? AND seq=1", (RUN_ID,))


def _over_cap(ledger: sks.Ledger) -> None:
    ledger.record("budget", "item_ledger", {"cap": 32768, "charged_before": 16385})


def _no_guard_events(ledger: sks.Ledger) -> None:
    with ledger.transaction() as db:
        db.execute(
            "DELETE FROM observations WHERE run_id=? AND criterion='budget' AND key='guard_checks'",
            (RUN_ID,),
        )


def _guard_missed_a_response(ledger: sks.Ledger) -> None:
    guard = ledger.observations("budget")["guard_checks"]
    sessions = [dict(row) for row in guard["sessions"]]
    sessions[0]["responses_checked"] -= 1
    sessions[0]["guard_after_first"] = False
    ledger.record(
        "budget",
        "guard_checks",
        {**guard, "responses_checked": guard["responses_checked"] - 1, "sessions": sessions},
    )


@pytest.mark.parametrize(
    ("mutate", "failure"),
    [
        pytest.param(_over_bound, "bound:session_output_tokens", id="full-share-over-bound"),
        pytest.param(_over_cap, "item_cap", id="item-over-cap"),
        pytest.param(_no_guard_events, "guard_events", id="no-guard-events"),
        pytest.param(_guard_missed_a_response, "guard_missed_response", id="guard-missed-response"),
    ],
)
def test_ceiling_is_not_enforced_when_a_recorded_condition_fails(
    tmp_path: Path, mutate: Callable[[sks.Ledger], None], failure: str
) -> None:
    """Behavior: a full-share charge over its bound, an item over its cap, or guard events
    that are missing or miss a response each fail ``budget.ceiling_enforced``."""

    ledger = _attempt_three_ledger(_private(tmp_path / "state"))
    try:
        mutate(ledger)
        result = sks.judge(ledger, "budget")
        assert result["checks"]["budget.ceiling_enforced"] is False
        assert failure in result["observations"]["budget.ceiling_enforcement_failures"]
        assert result["verdict"] == "fail"
    finally:
        ledger.close()


# --------------------------------------------------------------------------
# 5. The grant probe: one capped haiku turn, recorded outside the ceiling
# --------------------------------------------------------------------------


def _probe_cli(directory: Path, *, turns: int, tokens: int, model: str, failed: bool) -> Path:
    """A stand-in for the CLI's ``--print --output-format json`` result.

    The fields are the ones personal_profile_support.grant_probe_result reads
    (``subtype``, ``is_error``, ``num_turns``, ``usage``, ``modelUsage``,
    ``total_cost_usd``, ``terminal_reason``, ``api_error_status``). It reports
    what it was given: a digest of the grant, never the grant.
    """

    directory.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "type": "result",
        "subtype": "success",
        "is_error": failed,
        "duration_ms": 812,
        "num_turns": turns,
        "result": "ok",
        "session_id": SESSION,
        "total_cost_usd": 0.0001,
        "usage": {"input_tokens": 11, "output_tokens": tokens},
        "modelUsage": {model: {"inputTokens": 11, "outputTokens": tokens, "costUSD": 0.0001}},
    }
    if failed:
        result.update({"terminal_reason": "api_error", "api_error_status": 401})
    report = directory.parent / "probe-report.json"
    cli = directory / "claude"
    cli.write_text(
        f"#!{sys.executable}\n"
        "import hashlib, json, os, sys\n"
        "sys.stdin.read()\n"
        f"grant = os.environ.get({sks.GRANT_KEY!r}, '')\n"
        "report = {\n"
        "    'grant_sha256': hashlib.sha256(grant.encode()).hexdigest(),\n"
        "    'max_output': os.environ.get('CLAUDE_CODE_MAX_OUTPUT_TOKENS'),\n"
        "    'names': sorted(os.environ),\n"
        "    'home_entries': sorted(os.listdir(os.environ['HOME'])),\n"
        "    'cwd_is_home': os.path.realpath(os.getcwd())\n"
        "    == os.path.realpath(os.environ['HOME']),\n"
        "    'argv': sys.argv[1:],\n"
        "}\n"
        f"open({str(report)!r}, 'w').write(json.dumps(report))\n"
        f"print(json.dumps({result!r}))\n"
        f"sys.exit({1 if failed else 0})\n"
    )
    cli.chmod(0o755)
    return cli


@pytest.mark.parametrize(
    ("turns", "tokens", "model", "failed", "failures"),
    [
        pytest.param(1, 4, "claude-haiku-5-5", False, [], id="one-capped-haiku-turn"),
        pytest.param(1, 32, "claude-haiku-5-5-20261001", False, [], id="dated-haiku-at-cap"),
        pytest.param(1, 33, "claude-haiku-5-5", False, ["output_tokens"], id="over-the-cap"),
        pytest.param(2, 4, "claude-haiku-5-5", False, ["turns"], id="two-turns"),
        pytest.param(1, 4, "claude-sonnet-5-5", False, ["model"], id="another-model"),
        pytest.param(1, 4, "claude-haiku-5-5", True, ["not_ok"], id="refused-grant"),
    ],
)
def test_grant_probe_outside_the_ceiling_is_one_capped_haiku_turn(
    tmp_path: Path, turns: int, tokens: int, model: str, failed: bool, failures: list[str]
) -> None:
    """Behavior and liveness: the probe runs with an empty HOME and only the grant, its
    record is written before it runs, and anything but one haiku turn of at most 32 output
    tokens refuses the run before any reservation."""

    cli = _probe_cli(tmp_path / "cli", turns=turns, tokens=tokens, model=model, failed=failed)
    grant = _private_file(_private(tmp_path / "inputs") / "grant", GRANT_MARKER)
    live = _live_run(
        tmp_path, shell_env={"PATH": str(cli.parent)}, grant_file=grant, grant_probe_cli="claude"
    )
    if failures:
        with pytest.raises(sks.Refusal) as refused:
            live.run.run_grant_probe()
        assert refused.value.code == "grant_probe_refused"
    else:
        live.run.run_grant_probe()
    ledger = live.ledger
    probe = ledger.observations("grant_probe")
    assert probe["failures"] == failures
    assert probe["model"] == sks.GRANT_PROBE_MODEL == "claude-haiku-5-5"
    assert (probe["output_cap"], probe["turn_cap"]) == (32, 1)
    assert (probe["result"]["turns"], probe["result"]["output_tokens"]) == (turns, tokens)
    started, finished = (ledger.recorded_at("grant_probe", key) for key in ("started", "result"))
    assert started is not None and finished is not None and started <= finished
    assert ledger.sessions() == []  # outside the ceiling: nothing reserved or charged
    same = all(sks.same_model(name, sks.GRANT_PROBE_MODEL) for name in probe["result"]["models"])
    assert same is ("model" not in failures)
    report = json.loads((tmp_path / "probe-report.json").read_text())
    assert report["grant_sha256"] == hashlib.sha256(GRANT_MARKER.encode()).hexdigest()
    assert report["max_output"] == "32"
    assert report["home_entries"] == [] and report["cwd_is_home"] is True
    expected = {*sks.grant_probe_env(home="h", cli_dir="d", grant="g")}
    assert set(report["names"]) - {"LC_CTYPE"} == expected | {"CLAUDE_CODE_MAX_OUTPUT_TOKENS"}
    assert report["argv"] == sks.grant_probe_argv(str(cli))[1:]
    assert sks.grant_probe_result(0, b"", b"startup failure")["ok"] is False
    assert sks.load_grant(grant).reveal() == GRANT_MARKER
    assert GRANT_MARKER not in (live.run.inputs.state_dir / "commands.jsonl").read_text()
    assert GRANT_MARKER not in ledger.dump()
    ledger.close()


# --------------------------------------------------------------------------
# 6. Judge: every check from a recorded observation; a missing one fails
# --------------------------------------------------------------------------


def _passing_ledger(state: Path, *, rehearsal: bool = False) -> None:
    """Every observation a passing run records, in the shapes the driver records them."""

    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        now = time.time()
        ledger.ensure_run(CANDIDATE, phase="run", now=now)
        ledger.mark_live_kind("rehearse" if rehearsal else "run")
        observations: dict[tuple[str, str], Any] = {
            **{
                ("slack", key): True
                for key in (
                    "sender_auth_ok",
                    "curie_auth_ok",
                    "app_token_ok",
                    "same_team",
                    "sender_distinct",
                    "isolation_confirmed",
                    "curie_in_channel",
                    "ping_answered",
                    "ping_receipt_present",
                    "control_receipt_present",
                    "control_enqueue_logged",
                    "dispatcher_pods_gone",
                )
            },
            ("slack", "declared_driver"): DRIVER,
            ("slack", "sender_driver"): DRIVER,
            ("slack", "ping_messages"): 2,
            ("slack", "control_receipt_checks"): RECEIPT_CHECKS,
            ("refusal", "disconnect"): {"exit": 0, "state": "inactive"},
            ("refusal", "state_before_control"): "inactive",
            ("refusal", "teardown_settled"): True,
            ("refusal", "control_issue_distinct"): True,
            ("refusal", "control_reply_outcome"): sks.ENROLLMENT_INACTIVE,
            ("refusal", "expected_outcome"): sks.ENROLLMENT_INACTIVE,
            ("refusal", "before"): sks.counts_snapshot(
                work_items=1, requests=1, claims=0, runner_pods=0, sessions=4
            ),
            ("refusal", "after"): sks.counts_snapshot(
                work_items=1, requests=1, claims=0, runner_pods=0, sessions=4
            ),
            ("refusal", "control_native_sessions"): 0,
            ("cleanup", "registry_tags_absent"): True,
            ("cleanup", "token_scan"): {
                "stdout": {"scanned_bytes": 512, "token_bytes_found": False},
                "ledger": {"scanned_bytes": 2048, "token_bytes_found": False},
            },
        }
        for (section, key), value in observations.items():
            ledger.record(section, key, value)
        for step in ("helm-uninstall", "delete-slack-messages", "remove-registry-container"):
            ledger.record_cleanup(f"step:{step}", step, "ok")
        for kind in sks._INVENTORY:
            ledger.record_cleanup(f"inventory:{kind}", kind, "absent")
        if rehearsal:
            return
        ledger.record_ceiling(MODEL_CEILING, sks.parse_ceiling(MODEL_CEILING))
        ledger.record_item_ledger(32768, 8607, sks.parse_ceiling(MODEL_CEILING))
        ledger.begin_live(now=now, grant_expires_at=now + 3 * 3600)
        ledger.record("budget", "allowance_breach", [])
        ledger.record("budget", "tool_search_calls", 0)
        ledger.record("grant_probe", "started", {"at": now, "model": sks.GRANT_PROBE_MODEL})
        ledger.record("grant_probe", "model", sks.GRANT_PROBE_MODEL)
        ledger.record("grant_probe", "output_cap", sks.GRANT_PROBE_OUTPUT_CAP)
        ledger.record("grant_probe", "turn_cap", sks.GRANT_PROBE_TURNS)
        result = {"ok": True, "turns": 1, "output_tokens": 4, "models": [sks.GRANT_PROBE_MODEL]}
        ledger.record("grant_probe", "result", result)
        # After every record above, however long they took: the judge orders them by time.
        reservations = ledger.reserve(sks.ATTEMPT_ROLES, now=time.time() + 1)
        facts = sks.attempt_facts([_attempt_files()], not_before=T0 - 30)
        seen = [key for session in facts.sessions for key in session.message_ids]
        launched = sks.launched_sessions(reservations, [{"ran": True, "turn_starts": ()}])
        ledger.record("budget", "launched_sessions", launched)
        ledger.record(
            "budget",
            "guard_checks",
            sks.guard_coverage(reservations, facts.sessions, seen, checks=1, launched=launched),
        )
        sks.settle_attempt(ledger, reservations, facts.sessions, complete=True)
        request_id = str(sks.kickoff_request_id(sks.DEFAULT_IDENTITY, DELIVERY))
        run_observations: dict[tuple[str, str], Any] = {
            ("slack", "kickoff_at_root"): True,
            ("slack", "kickoff_receipt_present"): True,
            ("slack", "kickoff_receipt_checks"): RECEIPT_CHECKS,
            ("slack", "kickoff_enqueue_logged"): True,
            ("admission", "before"): {"work_items": 0, "requests": 0},
            ("admission", "work_items"): [
                {
                    "id": WORK_ITEM,
                    "tenant_id": TENANT,
                    "model_auth_mode": "personal",
                    "owner_principal_id": PRINCIPAL,
                    "enrollment_generation": 3,
                    "tracker_issue_id": "7",
                    "trigger_label": None,
                }
            ],
            ("admission", "requests"): [
                {
                    "id": request_id,
                    "work_item_id": WORK_ITEM,
                    "model_auth_mode": "personal",
                    "objective_digest": sks.ref_digest(ISSUE_URL),
                    "requester": f"slack:{SENDER_USER}",
                }
            ],
            ("admission", "expected_request_id"): request_id,
            ("admission", "expected_mode"): sks.PERSONAL_MODE,
            ("admission", "issue_url_digest"): sks.ref_digest(ISSUE_URL),
            ("admission", "issue_number"): 7,
            ("admission", "expected_requester"): f"slack:{SENDER_USER}",
            ("admission", "thread"): THREAD,
            ("admission", "expected_thread"): THREAD,
            ("owner", "expected"): OWNER,
            ("owner", "work_item_owner"): OWNER,
            ("owner", "request_owners"): [OWNER],
            ("owner", "agent_tenant"): TENANT,
            ("owner", "link_native_is_sender"): True,
            ("owner", "link_principal"): PRINCIPAL,
            ("owner", "enrollment_state_at_kickoff"): "active",
            ("owner", "enrollment_generation_at_kickoff"): 3,
            ("grant", "platform_images_are_candidate"): True,
            ("grant", "runner_pods"): [
                {
                    "image": {
                        "from_run_registry": True,
                        "digest_matches": True,
                        "revision_matches": True,
                    },
                    "grant_from_claim_tokens": True,
                    "claim_owner_labels": True,
                    "container_credential": "present",
                }
            ],
            ("grant", "sessions"): [sks.session_summary(s) for s in facts.sessions],
            ("grant", "role_models"): sks.role_models(facts.sessions),
            ("grant", "synthetic_records"): facts.synthetic_records,
            ("grant", "transcript_error_classes"): sks.transcript_error_classes(facts.sessions),
            ("grant", "stderr_error_classes"): [],
            ("grant", "terminal_cause"): None,
            ("progress", "thread"): THREAD,
            ("progress", "expected_thread"): THREAD,
            ("progress", "posts"): [
                {
                    "kind": kind,
                    "ref_digest": sks.ref_digest(ts),
                    "posted": True,
                    "failed": False,
                }
                for kind, ts in (
                    (sks.PULL_REQUEST_POST, REPLY_TS[1]),
                    (sks.TERMINAL_POST, REPLY_TS[2]),
                )
            ],
            ("progress", "thread_message_digests"): sorted(
                sks.ref_digest(ts) for ts in (KICKOFF_TS, *REPLY_TS)
            ),
            ("progress", "foreign_messages"): 0,
            ("progress", "root_posts"): 0,
            ("progress", "kickoff_reply_outcome"): sks.ADMITTED,
            ("progress", "pull_request_exists"): True,
        }
        for (section, key), value in run_observations.items():
            ledger.record(section, key, value)
    finally:
        ledger.close()


# What each criterion reads from the observations; deleting any one fails it.
READS: dict[str, tuple[tuple[str, str], ...]] = {
    "slack": tuple(
        ("slack", key)
        for key in (
            "sender_auth_ok",
            "curie_auth_ok",
            "app_token_ok",
            "same_team",
            "sender_distinct",
            "isolation_confirmed",
            "curie_in_channel",
            "declared_driver",
            "sender_driver",
            "ping_answered",
            "ping_receipt_present",
            "control_receipt_present",
            "control_receipt_checks",
            "control_enqueue_logged",
            "dispatcher_pods_gone",
            "kickoff_at_root",
            "kickoff_receipt_present",
            "kickoff_receipt_checks",
            "kickoff_enqueue_logged",
        )
    ),
    "admission": tuple(
        ("admission", key)
        for key in (
            "before",
            "work_items",
            "requests",
            "expected_request_id",
            "expected_mode",
            "issue_url_digest",
            "issue_number",
            "expected_requester",
            "thread",
            "expected_thread",
        )
    ),
    "owner": tuple(
        ("owner", key)
        for key in (
            "expected",
            "work_item_owner",
            "request_owners",
            "agent_tenant",
            "link_native_is_sender",
            "link_principal",
            "enrollment_state_at_kickoff",
            "enrollment_generation_at_kickoff",
        )
    ),
    "grant": (
        *(
            ("grant", key)
            for key in (
                "platform_images_are_candidate",
                "runner_pods",
                "sessions",
                "synthetic_records",
                "transcript_error_classes",
                "stderr_error_classes",
                "terminal_cause",
            )
        ),
        ("budget", "ceiling"),
    ),
    "progress": tuple(
        ("progress", key)
        for key in (
            "thread",
            "expected_thread",
            "posts",
            "thread_message_digests",
            "foreign_messages",
            "root_posts",
            "kickoff_reply_outcome",
            "pull_request_exists",
        )
    ),
    "refusal": tuple(
        ("refusal", key)
        for key in (
            "disconnect",
            "state_before_control",
            "teardown_settled",
            "control_issue_distinct",
            "control_reply_outcome",
            "expected_outcome",
            "before",
            "after",
            "control_native_sessions",
        )
    ),
    "budget": (
        ("budget", "approved_ceiling"),
        ("budget", "ceiling"),
        ("budget", "allowance_breach"),
        ("budget", "launched_sessions"),
        *(("grant_probe", key) for key in ("started", "result", "output_cap", "turn_cap", "model")),
    ),
    "cleanup": (("cleanup", "token_scan"), ("cleanup", "registry_tags_absent")),
}


@contextlib.contextmanager
def _without(ledger: sks.Ledger, table: str, where: str, *args: Any) -> Iterator[None]:
    """Remove rows for the duration of the block, then put the exact rows back."""

    rows = ledger.db.execute(f"SELECT * FROM {table} WHERE {where}", args).fetchall()
    assert rows, (table, where, args)
    ledger.db.execute(f"DELETE FROM {table} WHERE {where}", args)
    try:
        yield
    finally:
        marks = ",".join("?" * len(rows[0]))
        ledger.db.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)


def _verdict(ledger: sks.Ledger, criterion: str) -> str:
    return str(sks.judge(ledger, criterion)["verdict"])


def test_judge_fails_each_criterion_on_a_missing_observation(tmp_path: Path) -> None:
    """Behavior: the judge passes a complete ledger, and removing any one observation, the
    session rows, a cleanup step or an inventory row fails the criterion that reads it."""

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        result = sks.judge(ledger, "all")
        assert [name for name, ok in result["checks"].items() if not ok] == []
        assert result["role_models"] == {
            sks.IMPLEMENTER: [IMPLEMENTER_MODEL],
            sks.PLAN_REVIEWER: [REVIEWER_MODEL],
            sks.DIFF_REVIEWER: [REVIEWER_MODEL],
        }
        assert result["ceiling"]["models"] == {
            sks.DIFF_REVIEWER: REVIEWER_MODEL,
            sks.PLAN_REVIEWER: REVIEWER_MODEL,
        }
        where = "run_id=? AND criterion=? AND key=?"
        for criterion, reads in READS.items():
            assert _verdict(ledger, criterion) == "pass"
            for section, key in reads:
                with _without(ledger, "observations", where, RUN_ID, section, key):
                    assert _verdict(ledger, criterion) == "fail", (criterion, section, key)
                    assert _verdict(ledger, "all") == "fail"
        with _without(ledger, "sessions", "run_id=?", RUN_ID):
            assert _verdict(ledger, "budget") == "fail"
        with _without(ledger, "cleanup", "run_id=? AND kind LIKE 'step:%'", RUN_ID):
            assert _verdict(ledger, "cleanup") == "fail"
        with _without(ledger, "cleanup", "run_id=? AND kind=?", RUN_ID, "inventory:slack_messages"):
            assert _verdict(ledger, "cleanup") == "fail"
        ledger.db.execute(
            "UPDATE sessions SET output_tokens=NULL WHERE run_id=? AND seq=2", (RUN_ID,)
        )
        # Unknown usage is charged its full share (coordinator ruling of 2026-11-13): the
        # ceiling still holds, and the judge reports the usage as not exact.
        result = sks.judge(ledger, "budget")
        assert result["verdict"] == "pass"
        assert result["observations"]["budget.usage_exact"]["exact"] is False
    finally:
        ledger.close()


def test_judge_checks_observed_models_and_rehearsal_ledgers(tmp_path: Path) -> None:
    """Behavior: a reviewer on another model fails grant; an empty ledger fails every
    criterion; a rehearsal ledger is judged only on slack, refusal and cleanup."""

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        sessions = ledger.observations("grant")["sessions"]
        swapped = [
            {**item, "models": ["claude-sonnet-5-5"]} if item["role"] == sks.PLAN_REVIEWER else item
            for item in sessions
        ]
        ledger.record("grant", "sessions", swapped)
        checks = sks.judge(ledger, "grant")["checks"]
        assert [name for name, ok in checks.items() if not ok] == ["grant.expected_models"]
    finally:
        ledger.close()

    empty_state = _private(tmp_path / "empty")
    empty = sks.Ledger.open(empty_state, RUN_ID, scrubber=sks.Scrubber())
    try:
        empty.ensure_run(CANDIDATE, phase="run")
        empty.mark_live_kind("run")
        for criterion in sks.CRITERIA:
            assert _verdict(empty, criterion) == "fail", criterion
    finally:
        empty.close()

    rehearsal_state = _private(tmp_path / "rehearsal")
    _passing_ledger(rehearsal_state, rehearsal=True)
    rehearsal = sks.Ledger.open(rehearsal_state, RUN_ID, scrubber=sks.Scrubber())
    try:
        result = sks.judge(rehearsal, "all")
        assert result["verdict"] == "pass"
        assert {name.split(".", 1)[0] for name in result["checks"]} == set(sks.REHEARSAL_CRITERIA)
        assert not any("kickoff" in name for name in result["checks"])
        with pytest.raises(sks.Refusal) as refused:
            sks.judge(rehearsal, "grant")
        assert refused.value.code == "rehearsal_ledger"
    finally:
        rehearsal.close()


def test_judge_reads_only_the_ledger_through_main(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Behavior: judge passes a complete ledger with no tool on PATH, and refuses a
    candidate the ledger did not record or a run ID it does not hold."""

    state = _private(tmp_path / "state")
    _passing_ledger(state)
    environ = {"PATH": str(_private(tmp_path / "empty-path"))}

    def judge(candidate: str, run_id: str) -> tuple[int, dict[str, Any]]:
        argv = ["judge", "--candidate", candidate, "--state-dir", str(state)]
        code = psk.main([*argv, "--run-id", run_id, "--criterion", "all"], environ=environ)
        return code, _output(capsys)[0]

    code, payload = judge(CANDIDATE, RUN_ID)
    assert (code, payload["verdict"]) == (sks.EXIT_PASS, "pass")
    assert payload["counts"]["budget.sessions"] == len(sks.ATTEMPT_ROLES)
    code, payload = judge(OTHER_SHA, RUN_ID)
    assert (code, payload["error"]) == (sks.EXIT_REFUSED, "candidate_mismatch")
    code, payload = judge(CANDIDATE, "acme-kickoff-9")
    assert (code, payload["error"]) == (sks.EXIT_REFUSED, "ledger_missing")


# --------------------------------------------------------------------------
# 7. Read-back: every post in the kickoff's own thread
# --------------------------------------------------------------------------


def test_read_back_matcher_rejects_a_post_in_another_thread(tmp_path: Path) -> None:
    """Behavior: a post read back from another thread, an unposted row or an empty list
    is never evidence for the kickoff thread; the judge holds the same line."""

    # conversations.replies (https://docs.slack.dev/reference/methods/conversations.replies):
    # the parent carries thread_ts equal to its ts, each reply its parent's ts.
    root = {
        "type": "message",
        "user": SENDER_USER,
        "bot_id": SENDER_BOT,
        "text": sks.kickoff_text(CURIE_USER, ISSUE_URL),
        "ts": KICKOFF_TS,
        "thread_ts": KICKOFF_TS,
        "reply_count": 3,
    }

    def reply(ts: str, text: str, outcome: str | None = None) -> dict[str, Any]:
        message: dict[str, Any] = {
            "type": "message",
            "user": CURIE_USER,
            "bot_id": CURIE_BOT,
            "text": text,
            "ts": ts,
            "thread_ts": KICKOFF_TS,
            "parent_user_id": SENDER_USER,
        }
        if outcome:
            message["blocks"] = [
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Outcome: {outcome}"}]}
            ]
        return message

    thread = [
        root,
        reply(REPLY_TS[0], "Factory run started for this issue.", sks.ADMITTED),
        reply(
            REPLY_TS[1], "Pull request opened: <https://github.com/acme-corp/acme-fixture/pull/8>"
        ),
        reply(REPLY_TS[2], "Factory run finished."),
    ]
    elsewhere = {**thread[3], "ts": "1800000009.000009", "thread_ts": OTHER_ROOT_TS}
    assert sks.kickoff_text(CURIE_USER, ISSUE_URL) == f"<@{CURIE_USER}> [test action] {ISSUE_URL}"
    assert sks.posts_in_thread([REPLY_TS[1], REPLY_TS[2]], thread, KICKOFF_TS)
    assert not sks.posts_in_thread([REPLY_TS[1], elsewhere["ts"]], [*thread, elsewhere], KICKOFF_TS)
    assert not sks.posts_in_thread([], thread, KICKOFF_TS)
    assert not sks.posts_in_thread([None], thread, KICKOFF_TS)
    assert not sks.in_thread(
        {**thread[3], "ts": KICKOFF_TS, "thread_ts": OTHER_ROOT_TS}, KICKOFF_TS
    )
    assert sks.foreign_messages([*thread, elsewhere], KICKOFF_TS) == 1
    assert sks.thread_digests([*thread, elsewhere], KICKOFF_TS) == sorted(
        sks.ref_digest(str(message["ts"])) for message in thread
    )
    assert sks.reply_outcome(thread, author=CURIE_USER) == sks.ADMITTED
    assert sks.reply_outcome(thread, author=SENDER_USER) is None

    # The ping is answered only by Curie, inside the ping's thread, naming the sender.
    ping = {**root, "text": sks.ping_text(CURIE_USER)}
    answer = reply(REPLY_TS[0], f"This installation accepts test actions from <@{SENDER_USER}>.")
    assert sks.ping_answered(
        [ping, answer], curie_user=CURIE_USER, sender_user=SENDER_USER, root_ts=KICKOFF_TS
    )
    top_level = {key: value for key, value in answer.items() if key != "thread_ts"}
    for messages in ([ping, top_level], [ping, {**answer, "user": SENDER_USER}]):
        assert not sks.ping_answered(
            messages, curie_user=CURIE_USER, sender_user=SENDER_USER, root_ts=KICKOFF_TS
        )

    # The judge reads recorded digests only and fails a post outside the thread.
    state = _private(tmp_path / "state")
    _passing_ledger(state)
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    try:
        posts = ledger.observations("progress")["posts"]
        posts[0]["ref_digest"] = sks.ref_digest(str(elsewhere["ts"]))
        ledger.record("progress", "posts", posts)
        checks = sks.judge(ledger, "progress")["checks"]
        assert [name for name, ok in checks.items() if not ok] == [
            "progress.every_post_in_origin_thread"
        ]
    finally:
        ledger.close()


def test_receipts_are_matched_to_the_post_under_their_own_key() -> None:
    """Behavior: a declared driver's version 2 receipt, stored under its delivery's key,
    is the post's; a misfiled, malformed or person's receipt is not admission evidence."""

    # Version 2 evidence names the declared bot user, the slack_evidence shape
    # (packages/channel-gateway/src/curie_channel_gateway/slack/identity.py).
    evidence = {
        "user_id": SENDER_USER,
        "team_id": TEAM,
        "enterprise_ids": [],
        "is_ext_shared_channel": False,
    }
    receipt = sks.SlackMentionReceipt(
        version=2,
        delivery_id=DELIVERY,
        channel_identity=sks.DEFAULT_IDENTITY,
        channel=CHANNEL,
        root_ts=KICKOFF_TS,
        message_ts=KICKOFF_TS,
        user_id=SENDER_USER,
        body=sks.kickoff_text(CURIE_USER, ISSUE_URL),
        evidence=evidence,
        driver_bot_id=SENDER_BOT,
    )
    other = receipt.model_copy(
        update={"delivery_id": "Ev0EXAMPLE2", "root_ts": REPLY_TS[0], "message_ts": REPLY_TS[0]}
    )
    stored = {
        sks.receipt_key(DELIVERY): receipt.model_dump_json().encode(),
        sks.receipt_key("Ev0EXAMPLE2"): other.model_dump_json().encode(),
        sks.receipt_key("Ev0EXAMPLE3"): b"{not json",
    }
    sender = sks.SlackAuth(TEAM, SENDER_USER, SENDER_BOT)
    found = sks.find_receipt(stored, channel=CHANNEL, message_ts=KICKOFF_TS)
    assert found == receipt
    assert sks.receipt_checks(found, channel=CHANNEL, message_ts=KICKOFF_TS, sender=sender) == (
        RECEIPT_CHECKS
    )
    misfiled = {sks.receipt_key("Ev0EXAMPLE9"): receipt.model_dump_json().encode()}
    assert sks.find_receipt(misfiled, channel=CHANNEL, message_ts=KICKOFF_TS) is None
    person = sks.SlackMentionReceipt(
        **{**receipt.model_dump(), "version": 1, "driver_bot_id": None}
    )
    checks = sks.receipt_checks(person, channel=CHANNEL, message_ts=KICKOFF_TS, sender=sender)
    assert [name for name, ok in checks.items() if not ok] == ["version_2", "driver_bot_is_sender"]
    assert sks.receipt_checks(None, channel=CHANNEL, message_ts=KICKOFF_TS, sender=sender) == {
        "receipt_readable": False
    }
    assert sks.parse_receipt(b"{not json") is None
    assert sks.RECEIPT_PATTERN == "curie:slack-mention-receipt:*"
    # handlers.py _mint_turn: "enqueued %s %s as stream entry %s identity=%s slack_identity=%s".
    log = (
        b"INFO enqueued slack event Ev0EXAMPLE1 as stream entry 1800000001000-0 "
        b"identity=psk-acme slack_identity=default\n"
    )
    assert sks.enqueue_logged(log, DELIVERY)
    assert not sks.enqueue_logged(log, "Ev0EXAMPLE2")
    assert not sks.enqueue_logged(
        b"enqueued block action Ev0EXAMPLE1 as stream entry 1-0", DELIVERY
    )
    # The expected request id is the shared derivation the worker and the API admit under.
    assert sks.kickoff_request_id(sks.DEFAULT_IDENTITY, DELIVERY) == uuid.uuid5(
        uuid.NAMESPACE_URL, f"curie-slack://factory/kickoff/default/{DELIVERY}"
    )
    links = sks.find_issue_links(f"<{ISSUE_URL}>")
    assert [(link.kind, link.number) for link in links] == [("issue", 7)]


# --------------------------------------------------------------------------
# 8. No held secret reaches argv, logs, output or the ledger
# --------------------------------------------------------------------------


def test_no_held_secret_reaches_argv_logs_or_ledger(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Behavior: across a Slack probe and a run that loads every Slack token, no token's
    bytes (raw or base64) reach a child's argv, the command log, stdout, stderr, the ledger
    or a Slack request body; children get only the filtered environment."""

    kit = _kit(tmp_path)
    kit.slack.rate_limit_once.add("conversations.replies")
    assert kit.run("slack-probe") == sks.EXIT_PASS
    probe = capsys.readouterr()
    payload = json.loads(probe.out.splitlines()[-1])
    assert payload["checks"]["round_trip"] and payload["checks"]["probe_post_deleted"]
    assert kit.clock.waits == [3.0]  # Retry-After honored, then the same call retried
    assert kit.slack.writes() == [("chat.postMessage", SENDER_USER), ("chat.delete", SENDER_USER)]
    assert kit.slack.messages == []
    kit.values["--run-id"] = "acme-kickoff-2"
    assert kit.run("run") == sks.EXIT_FAIL
    run = capsys.readouterr()
    blobs = {
        "stdout": (probe.out + run.out).encode(),
        "stderr": (probe.err + run.err).encode(),
        "child argv": kit.log.read_bytes(),
        "slack request bodies": b"\n".join(kit.slack.bodies),
        **{
            f"state {path.name}": path.read_bytes()
            for path in kit.state.rglob("*")
            if path.is_file()
        },
    }
    assert "state acme-kickoff-2.commands.jsonl" in blobs
    scrubber = sks.Scrubber([sks.HeldSecret(marker) for marker in MARKERS])
    scan = sks.TokenScan(scrubber, tuple(blobs))
    found = [name for name, data in blobs.items() if scan.scan(name, data)]
    assert found == [] and sks.scan_clean(scan)
    ledger = sks.Ledger.open_existing(kit.state, "acme-kickoff-2")
    try:
        recorded = ledger.observations("cleanup")
        assert recorded["scan_clean"] is True
        assert not any(item["token_bytes_found"] for item in recorded["token_scan"].values())
    finally:
        ledger.close()
    ambient = {**kit.environ, "ANTHROPIC_API_KEY": "", "SLACK_BOT_TOKEN": CURIE_BOT_MARKER}
    assert sks.child_environment(ambient, kubeconfig=None, docker_context=DOCKER_CONTEXT) == {
        "PATH": kit.environ["PATH"],
        "HOME": kit.environ["HOME"],
        "DOCKER_CONTEXT": DOCKER_CONTEXT,
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    shell = psk.Shell(env={}, scrubber=scrubber, scan=scan, cwd=tmp_path)
    with pytest.raises(psk.StepFailed, match="child argument"):
        shell.guard(["kubectl", "create", "secret", f"--from-literal=t={SENDER_MARKER}"])


# --------------------------------------------------------------------------
# 9. Images, pods, install values and the OIDC split horizon
# --------------------------------------------------------------------------


def test_candidate_images_are_verified_by_digest_and_revision(tmp_path: Path) -> None:
    """Behavior and liveness: every image the run deploys, the runner layer included, is
    read back from the loopback registry with the candidate's revision label, else refused."""

    tag = sks.image_tag(CANDIDATE, RUN_ID)
    layer = "acme-bundle-runner"
    served = {(name, tag): _image(CANDIDATE) for name in psk.DOCKERFILES}
    served[(layer, tag)] = _image(CANDIDATE)
    state = _private(tmp_path / "state")
    ledger = sks.Ledger.open(state, RUN_ID, scrubber=sks.Scrubber())
    ledger.ensure_run(CANDIDATE, phase="images")
    scrubber = sks.Scrubber()

    def builder(port: int) -> psk.ImageBuilder:
        ledger.record("images", "layer_ref", f"localhost:{port}/{layer}:{tag}")
        return psk.ImageBuilder(
            inputs=psk.Inputs(CANDIDATE, RUN_ID, state, registry_port=port),
            shell=psk.Shell(env={}, scrubber=scrubber, scan=sks.TokenScan(scrubber), cwd=state),
            ledger=ledger,
            finalizer=psk.Finalizer(ledger),
            curie_bin="curie",
            gate_limits=sks.reviewer_limits(sks.ATTEMPT_ROLES),
        )

    with _registry(served) as port:
        verified = builder(port).verify()
        refs = sks.candidate_refs(CANDIDATE, RUN_ID, f"localhost:{port}/{layer}:{tag}")
        get = sks.loopback_get("127.0.0.1", port)
        direct = sks.verify_candidate_images(get, CANDIDATE, refs)
        image = sks.registry_image(get, "curie-api", tag)
    assert set(verified) == {*psk.DOCKERFILES, sks.LAYER_IMAGE}
    assert all(item.revision == CANDIDATE for item in verified.values())
    assert ledger.observations("images")["verified_digests"] == {
        name: item.digest for name, item in verified.items()
    }
    assert direct == verified
    assert refs[sks.LAYER_IMAGE] == (layer, tag)
    manifest, _config = _image(CANDIDATE)
    assert image is not None and image.digest == "sha256:" + hashlib.sha256(manifest).hexdigest()
    assert sks.split_image_ref(f"localhost:{port}/{layer}@{image.digest}") == (layer, image.digest)

    stale = {**served, ("curie-worker", tag): _image(OTHER_SHA)}
    with _registry(stale) as port, pytest.raises(sks.Refusal) as refused:
        builder(port).verify()
    assert (refused.value.code, refused.value.detail) == (
        "candidate_image_mismatch",
        "curie-worker",
    )
    ledger.close()


def test_the_agent_names_an_explicit_reviewer_model_for_personal_mode(tmp_path: Path) -> None:
    """Behavior: setup gives the agent the explicit Claude reviewer model personal mode
    requires, the approved one on a live run and the bundle's on a rehearsal, alongside
    the approved deadline; an approval whose reviewer models differ is refused. Without
    it the runner refuses every session before any provider call (runner_escalated)."""

    live_run = _live_run(tmp_path)
    run = live_run.run
    calls: list[tuple[str, ...]] = []

    class RecordingCli:
        def run(self, verb: str, *args: str, **_kwargs: Any) -> None:
            calls.append((verb, *args))

    run.curie = RecordingCli()  # type: ignore[assignment]
    run.agent, run.prefix = "acme-psk", "curie/acme"
    run.ceiling = sks.parse_ceiling(MODEL_CEILING)
    run.configure_agent()
    window = str(len(sks.ATTEMPT_ROLES) * 300)
    assert (
        "overrides",
        "acme-psk",
        "--reviewer-model",
        "claude-opus-5-5",
        "--execution-deadline",
        window,
    ) in calls
    assert calls[0][0] == "publication-policy"

    calls.clear()
    run.ceiling = None
    run.configure_agent()
    assert ("overrides", "acme-psk", "--reviewer-model", sks.REVIEWER_MODEL) in calls
    assert sks.reviewer_model(sks.parse_ceiling(CEILING)) == sks.REVIEWER_MODEL
    differing = (
        f"{CEILING},model.diff-reviewer=claude-opus-5-5,model.plan-reviewer=claude-sonnet-5-5"
    )
    with pytest.raises(sks.Refusal) as refused:
        sks.reviewer_model(sks.parse_ceiling(differing))
    assert refused.value.code == "live_unapproved"
    live_run.ledger.close()


def test_every_image_a_run_builds_carries_its_own_run_label() -> None:
    """Behavior: every candidate image and the runner layer carry the candidate revision
    and the run's own tag, so two runs at one revision never build the same image config.
    A node holding an identical image from an earlier run reports that run's digest as
    the running image ID, which the exact running-digest check rejects."""

    first, second = sks.image_tag(CANDIDATE, RUN_ID), sks.image_tag(CANDIDATE, "acme-kickoff-2")
    labels = sks.candidate_labels(CANDIDATE, first)
    assert labels == [f"{sks.REVISION_LABEL}={CANDIDATE}", f"{sks.RUN_LABEL}={first}"]
    assert labels != sks.candidate_labels(CANDIDATE, second)
    layer = sks.run_layer_dockerfile("FROM base\nLABEL a=b\n", first)
    assert layer == f"FROM base\nLABEL a=b\nLABEL {sks.RUN_LABEL}={first}\n"
    assert layer != sks.run_layer_dockerfile("FROM base\nLABEL a=b\n", second)

    # The node's report for a rebuilt identical image names the earlier run's digest.
    earlier, pushed = "sha256:" + "1" * 64, "sha256:" + "2" * 64
    registry = "localhost:25001"
    aliased = {
        "items": [
            _container_pod("acme-api", "api", f"{registry}/curie-api@{pushed}", f"x@{earlier}")
        ]
    }
    failures = sks.running_image_failures(
        aliased, {"curie-api": pushed}, registry=registry, required=("curie-api",)
    )
    assert "curie-api_digest_mismatch" in failures


def _container_pod(
    name: str, container: str, image: str, image_id: str, *, env: Sequence[Any] = ()
) -> dict[str, Any]:
    """A ``kubectl get pods -o json`` item with spec containers and container statuses.

    https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/
    """

    return {
        "metadata": {"name": name, "uid": f"uid-{name}"},
        "spec": {"containers": [{"name": container, "image": image, "env": list(env)}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": container,
                    "image": image,
                    "imageID": image_id,
                    "ready": True,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2027-01-15T08:00:00Z"}},
                }
            ],
        },
    }


def test_pod_audit_reads_digests_owner_labels_and_status_only(tmp_path: Path) -> None:
    """Behavior: running digests must be the pushed ones, the runner reads its grant from its
    own claim's ``-tokens`` Secret under the owner's labels, and rollout diagnostics read
    pod status and Warning events, never a container's environment."""

    registry = "localhost:5999"
    digests = {name: "sha256:" + str(index) * 64 for index, name in enumerate(psk.DOCKERFILES, 1)}

    def platform(name: str, digest: str) -> dict[str, Any]:
        ref = f"{registry}/{name}@{digests[name]}"
        return _container_pod(
            f"{name}-0", name.removeprefix("curie-"), ref, f"{registry}/{name}@{digest}"
        )

    required = ("curie-api", "curie-worker", "curie-dispatcher")
    expected = {name: digests[name] for name in required}
    pods = {"items": [platform(name, digests[name]) for name in required]}
    assert sks.running_image_failures(pods, expected, registry=registry, required=required) == []
    # A stale pod is a mismatch, and with no api container on the pushed digest the
    # candidate api is also not running; beside a matching pod only the mismatch remains.
    stale_pod = platform("curie-api", "sha256:" + "f" * 64)
    stale = {"items": [stale_pod, *pods["items"][1:]]}
    assert sks.running_image_failures(stale, expected, registry=registry, required=required) == [
        "curie-api_digest_mismatch",
        "curie-api_not_running",
    ]
    mixed = {"items": [stale_pod, *pods["items"]]}
    assert sks.running_image_failures(mixed, expected, registry=registry, required=required) == [
        "curie-api_digest_mismatch"
    ]
    missing = {"items": pods["items"][:2]}
    assert sks.running_image_failures(missing, expected, registry=registry, required=required) == [
        "curie-dispatcher_not_running"
    ]

    layer = "sha256:" + "9" * 64
    claim_name = "acme-claim-1"
    grant_ref = {"secretKeyRef": {"name": f"{claim_name}-tokens", "key": sks.GRANT_KEY}}
    runner = _container_pod(
        f"{claim_name}-runner",
        "runner",
        f"{registry}/acme-bundle-runner@{layer}",
        f"{registry}/acme-bundle-runner@{layer}",
        env=[{"name": sks.GRANT_KEY, "valueFrom": grant_ref}],
    )
    assert sks.runner_image_check(runner, registry=registry, digest=layer) == (
        {"from_run_registry": True, "digest_matches": True},
        layer,
    )
    assert sks.runner_image_check(runner, registry=registry, digest="sha256:" + "8" * 64)[0] == {
        "from_run_registry": True,
        "digest_matches": False,
    }
    owner = {"tenant_id": TENANT, "principal_id": PRINCIPAL, "generation": 3}
    labels = {
        sks.OWNER_TENANT_LABEL: TENANT,
        sks.OWNER_PRINCIPAL_LABEL: PRINCIPAL,
        sks.GENERATION_LABEL: "3",
    }
    claim = {"metadata": {"name": claim_name, "labels": labels}}
    assert sks.pod_grant_checks(runner, claim, owner) == {
        "grant_from_claim_tokens": True,
        "claim_owner_labels": True,
    }
    literal = json.loads(json.dumps(runner))
    literal["spec"]["containers"][0]["env"] = [{"name": sks.GRANT_KEY, "value": "x"}]
    stale_claim = {
        "metadata": {"name": claim_name, "labels": {**labels, sks.GENERATION_LABEL: "2"}}
    }
    assert sks.pod_grant_checks(literal, stale_claim, owner) == {
        "grant_from_claim_tokens": False,
        "claim_owner_labels": False,
    }

    # Diagnostics through the driver: pod status and Warning events only.
    crashed = platform("curie-api", digests["curie-api"])
    crashed["spec"]["containers"][0]["env"] = [{"name": "LEAK_PROBE", "value": GRANT_MARKER}]
    crashed["status"]["containerStatuses"][0].update(
        {
            "ready": False,
            "restartCount": 4,
            "state": {"waiting": {"reason": "CrashLoopBackOff", "message": "back-off 40s"}},
            "lastState": {"terminated": {"reason": "Error", "exitCode": 1}},
        }
    )
    listed = {"items": [crashed]}
    events = {
        "items": [
            {
                "type": "Warning",
                "reason": "BackOff",
                "message": "Back-off restarting failed container api",
                "involvedObject": {"kind": "Pod", "name": "curie-api-0"},
                "count": 4,
            },
            {"type": "Normal", "reason": "Pulled", "message": "pulled", "involvedObject": {}},
        ]
    }

    def answer(argv: list[str]) -> psk.Completed:
        body = events if "events" in argv else listed
        return psk.Completed(0, json.dumps(body).encode(), b"")

    live = _live_run(tmp_path, answer=answer)
    live.run.record_rollout_diagnostics()
    diagnostics = live.ledger.observations("install")["rollout_diagnostics"]
    assert diagnostics == sks.pod_diagnostics(listed, events)
    assert [item["reason"] for item in diagnostics["warnings"]] == ["BackOff"]
    assert GRANT_MARKER not in live.ledger.dump()
    live.ledger.close()


def test_install_values_and_oidc_split_horizon() -> None:
    """Behavior: the install carries no secret value and no service credential, declares the
    sender from its auth.test, and the OIDC login is routed only through the run's forwards."""

    fullname = sks.chart_fullname(RELEASE)
    assert fullname == "psk-acme-curie"
    origins = sks.oidc_origins(
        namespace=NAMESPACE,
        dex_service=f"{RELEASE}-dex",
        api_service=f"{fullname}-api",
        api_port=psk.API_PORT,
    )
    issuer_host = f"{RELEASE}-dex.{NAMESPACE}.svc.cluster.local:{sks.DEX_PORT}"
    assert origins.issuer == f"http://{issuer_host}/dex"
    assert sks.route_url(
        f"{origins.issuer}/auth?client_id=curie-console", origins, dex_local=41000, api_local=41001
    ) == ("http://127.0.0.1:41000/dex/auth?client_id=curie-console", {"Host": issuer_host})
    callback = f"{origins.redirect_uri}?code=c&state=s"
    assert sks.route_url(callback, origins, dex_local=41000, api_local=41001) == (
        "http://127.0.0.1:41001/console/oidc/callback?code=c&state=s",
        {},
    )
    local = "http://127.0.0.1:41001/console/oidc/login"
    assert sks.route_url(local, origins, dex_local=41000, api_local=41001) == (local, {})
    for outside in (
        origins.issuer.replace("http://", "https://"),
        "http://dex.example.com/dex/auth",
        "http://127.0.0.1:41002/console/oidc/login",
    ):
        assert sks.route_url(outside, origins, dex_local=41000, api_local=41001) is None

    client_marker = "dex-client-marker"
    secret, pod, service = sks.dex_manifests(
        name=f"{RELEASE}-dex",
        labels={sks.MANAGED_BY_LABEL: sks.MANAGED_BY_VALUE},
        config=sks.dex_config(origins, client_secret=client_marker),
        client_secret=client_marker,
        image="ghcr.io/dexidp/dex@sha256:" + "d" * 64,
    )
    assert [secret["kind"], pod["kind"], service["kind"]] == ["Secret", "Pod", "Service"]
    config = json.loads(base64.b64decode(secret["data"]["config.yaml"]))
    assert config["issuer"] == origins.issuer
    assert config["staticClients"][0]["redirectURIs"] == [origins.redirect_uri]
    assert pod["spec"]["volumes"][0]["secret"]["items"] == [
        {"key": "config.yaml", "path": "config.yaml"}
    ]
    assert client_marker not in json.dumps(pod) + json.dumps(service)

    digests = {name: "sha256:" + "a" * 64 for name in (*psk.DOCKERFILES, sks.LAYER_IMAGE)}
    plan = sks.InstallPlan(
        release=RELEASE,
        registry="localhost:5999",
        digests=digests,
        agent="acme-psk-00000001",
        repo=REPO,
        app_id="42",
        app_secret=f"{RELEASE}-github-app",
        slack_secret=f"{RELEASE}-slack-tokens",
        channel=CHANNEL,
        sender=sks.SlackAuth(TEAM, SENDER_USER, SENDER_BOT),
        grant_namespace=f"{NAMESPACE}-grants",
        origins=origins,
        dex_secret=f"{RELEASE}-dex",
        unused_label="psk-unused-00000001",
        priority_classes=None,
        controller_deploy=False,
        model_addresses=["192.0.2.11", "192.0.2.10", "192.0.2.10"],
        implementer_turns=32,
        response_cap=sks.RESPONSE_OUTPUT_CAP,
    )
    values = sks.install_values(plan)
    assert values["testInstallation"] == {"enabled": True, "drivers": [DRIVER]}
    assert values["dispatcher"]["deploy"] is True
    assert values["dispatcher"]["slack"]["appTokenExistingSecret"] == f"{RELEASE}-slack-tokens"
    assert values["personalClaude"] == {"grantNamespace": f"{NAMESPACE}-grants"}
    personal = {"name": "FACTORY_MODEL_AUTH_MODE", "value": sks.PERSONAL_MODE}
    assert personal in values["api"]["extraEnv"] and personal in values["worker"]["extraEnv"]
    egress = values["security"]["networkPolicy"]["allowedEgress"]
    assert [item["cidr"] for item in egress] == ["192.0.2.10/32", "192.0.2.11/32"]
    assert client_marker not in json.dumps(values)
    assert sks.service_credential_findings(values) == []
    leaked = json.loads(json.dumps(values))
    leaked["worker"]["extraEnv"].append({"name": sks.GRANT_KEY, "value": "x"})
    leaked["agentSandbox"]["runner"]["fakeModel"] = True
    assert sks.service_credential_findings(leaked) == [
        "CLAUDE_CODE_OAUTH_TOKEN",
        "agentSandbox.runner",
    ]

    def encoded(**data: str) -> dict[str, str]:
        return {key: base64.b64encode(value.encode()).decode() for key, value in data.items()}

    generated = encoded(**{key: f"{key}-{'g' * 24}" for key in sks.PUBLISHED_DEFAULTS})
    assert sks.generated_secrets_ok(generated)
    assert not sks.generated_secrets_ok(encoded(**sks.PUBLISHED_DEFAULTS))
    assert not sks.generated_secrets_ok(encoded(**dict.fromkeys(sks.PUBLISHED_DEFAULTS, "s" * 32)))
    assert not sks.generated_secrets_ok({})


def test_ceiling_is_the_exact_approval_and_the_gate_holds_parallel_starts(tmp_path: Path) -> None:
    """Behavior and liveness: the canonical approval parses with per-role turns and models,
    anything else is refused, shares never exceed it, and parallel reviewer starts each
    claim an independent slot file in the bundle's reservation gate."""

    ceiling = sks.parse_ceiling(CEILING)
    assert ceiling.canonical() == CEILING and ceiling.turns_for(sks.IMPLEMENTER) == 32
    assert sks.Ceiling.from_json(ceiling.as_json()) == ceiling
    per_role = CEILING.replace("session-turns=32", "session-turns=32,session-turns.implementer=24")
    parsed = sks.parse_ceiling(f"{per_role},model.plan-reviewer={REVIEWER_MODEL}")
    assert (parsed.turns_for(sks.IMPLEMENTER), parsed.turns_for(sks.PLAN_REVIEWER)) == (24, 32)
    assert parsed.models == {sks.PLAN_REVIEWER: REVIEWER_MODEL}
    for refused in (
        "",
        f"{CEILING},sessions=8",
        f"{CEILING},model.implementer=Claude Opus",
        f"{CEILING},model.reviewer={REVIEWER_MODEL}",
        CEILING.replace("session-turns=32", "session-turns=32,session-turns.implementer=33"),
        CEILING.replace("output-tokens=16384", "output-tokens=16385"),
        CEILING.replace(",paid-retries=0", ""),
    ):
        with pytest.raises(sks.Refusal) as error:
            sks.parse_ceiling(refused)
        assert error.value.code == "live_unapproved", refused
    assert sks.attempt_roles(ceiling) == sks.ATTEMPT_ROLES
    shares = sks.reservation_shares(ceiling, sks.ATTEMPT_ROLES)
    assert sum(share.output_tokens for share in shares) <= ceiling.output_tokens
    assert sum(share.turns for share in shares) <= ceiling.turns
    assert all(share.output_tokens <= ceiling.session_output_tokens for share in shares)
    assert sks.reviewer_limits(sks.ATTEMPT_ROLES) == {"plan": 1, "diff": 2}

    hooks = json.loads((psk.REPO_ROOT / psk.BUNDLE_SOURCE / "hooks" / "hooks.json").read_text())
    patched = sks.private_bundle_hooks(hooks, {"plan": 1, "diff": 2})
    assert patched["hooks"]["PreToolUse"][:-1] == hooks["hooks"].get("PreToolUse", [])
    added = patched["hooks"]["PreToolUse"][-1]
    assert added["matcher"] == "Agent|Task"
    assert added["hooks"][0]["command"].endswith(
        f'{sks.RESERVATION_GATE_FILE} \'{{"diff":2,"plan":1}}\''
    )
    assert {key: value for key, value in patched["hooks"].items() if key != "PreToolUse"} == {
        key: value for key, value in hooks["hooks"].items() if key != "PreToolUse"
    }

    gate = _private(tmp_path / "hooks")
    (gate / sks.RESERVATION_GATE_FILE).write_text(sks.RESERVATION_GATE_PROGRAM)
    shutil.copy(psk.REPO_ROOT / psk.BUNDLE_SOURCE / "hooks" / "review_gate.py", gate)
    slots = tmp_path / "reservations"
    environment = {"PATH": "/usr/bin:/bin", "PSK_RESERVATION_DIR": str(slots)}

    def start(tool: str, kind: str) -> subprocess.Popen[str]:
        event = {"tool_name": tool, "tool_input": {"subagent_type": kind, "prompt": "review"}}
        process = subprocess.Popen(
            [sys.executable, "-I", str(gate / sks.RESERVATION_GATE_FILE), '{"diff":2,"plan":1}'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        assert process.stdin is not None
        process.stdin.write(json.dumps(event))
        process.stdin.close()
        return process

    # Every start is launched before any is awaited, so the hooks run in parallel.
    parallel = [start("Agent", DIFF_KIND) for _ in range(4)] + [
        start("Task", PLAN_KIND) for _ in range(2)
    ]
    codes = [process.wait(timeout=30) for process in parallel]
    for process in parallel:
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
    assert sorted(codes[:4]) == [0, 0, 2, 2] and sorted(codes[4:]) == [0, 2]
    assert sorted(os.listdir(slots)) == ["diff-0", "diff-1", "plan-0"]
    other = start("Agent", "general-purpose")
    assert other.wait(timeout=30) == 2
    bash = subprocess.run(
        [sys.executable, "-I", str(gate / sks.RESERVATION_GATE_FILE), '{"diff":2,"plan":1}'],
        input=json.dumps({"tool_name": "Bash", "tool_input": {"command": "ls"}}),
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
        check=False,
    )
    assert bash.returncode == 0
    for stream in (other.stdout, other.stderr):
        if stream is not None:
            stream.close()


# --------------------------------------------------------------------------
# 10. Meta: borrowed symbols, sleeps and provisioning literals
# --------------------------------------------------------------------------

_IMAGES = "test_candidate_images_are_verified_by_digest_and_revision"
_INPUTS = "test_input_checks_accept_valid_inputs"
_HARVEST = "test_harvest_stream_settles_only_after_a_post_exit_size_report"
_PROBE = "test_grant_probe_outside_the_ceiling_is_one_capped_haiku_turn"
_PODS = "test_pod_audit_reads_digests_owner_labels_and_status_only"
_SECRETS = "test_no_held_secret_reaches_argv_logs_or_ledger"
EXERCISED = {
    "HARVEST_PROGRAM": _HARVEST,
    "ChannelScan": _HARVEST,
    "HarvestState": _HARVEST,
    "HeldSecret": _SECRETS,
    "Refusal": "test_reservations_persist_before_a_paid_start_and_unknown_usage_refuses_the_next",
    "Scrubber": _SECRETS,
    "TokenScan": _SECRETS,
    "candidate_refs": _IMAGES,
    "chart_fullname": "test_install_values_and_oidc_split_horizon",
    "check_grant_file": _INPUTS,
    "child_environment": _SECRETS,
    "grant_probe_argv": _PROBE,
    "grant_probe_env": _PROBE,
    "grant_probe_result": _PROBE,
    "harvest_config": _HARVEST,
    "image_tag": _IMAGES,
    "load_grant": _PROBE,
    "loopback_get": _IMAGES,
    "parse_expiry": _INPUTS,
    "parse_native_transcripts": "test_watchdog_charges_every_session_against_the_ceiling",
    "pod_diagnostics": _PODS,
    "ref_digest": "test_read_back_matcher_rejects_a_post_in_another_thread",
    "refuse_ambient_credentials": _INPUTS,
    "registry_image": _IMAGES,
    "require_app_kit": _INPUTS,
    "require_candidate": _INPUTS,
    "require_dns_name": _INPUTS,
    "require_lifetime": _INPUTS,
    "require_private_dir": _INPUTS,
    "require_registry": _INPUTS,
    "require_release": _INPUTS,
    "require_run_id": _INPUTS,
    "runner_image_check": _PODS,
    "running_image_failures": _PODS,
    "same_model": _PROBE,
    "session_summary": _HARVEST,
    "split_image_ref": _IMAGES,
    "verify_candidate_images": _IMAGES,
}


def test_every_borrowed_symbol_is_exercised() -> None:
    """Regression guard: one import block from the #5375 support module, every name in it
    used by the driver and exercised by a named test here; personal_profile.py is never
    imported."""

    support = ast.parse(Path(sks.__file__).read_text())
    blocks = [
        node
        for node in support.body
        if isinstance(node, ast.ImportFrom) and node.module == "personal_profile_support"
    ]
    assert len(blocks) == 1
    assert {alias.name for alias in blocks[0].names} == set(EXERCISED)
    driver = ast.parse(Path(psk.__file__).read_text())
    used = {node.id for node in ast.walk(support) if isinstance(node, ast.Name)} | {
        node.attr
        for node in ast.walk(driver)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "sks"
    }
    for name, test_name in sorted(EXERCISED.items()):
        assert name in used, f"{name} is borrowed but never used"
        assert f"sks.{name}" in inspect.getsource(globals()[test_name]), (name, test_name)
    for tree in (support, driver):
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        assert "personal_profile" not in imported


def test_this_module_has_no_sleeps_or_provisioning_literals() -> None:
    """Regression guard: time only moves through FakeClock, and nothing here provisions."""

    source = Path(__file__).read_text()
    for banned in (
        "time." + "sleep(",
        "asyncio." + "sleep(",
        "helm" + " install",
        "kind" + " create",
    ):
        assert banned not in source, banned

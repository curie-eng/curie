"""Drive the candidate Docker image through its default process entrypoint.

Only the two external APIs are fixtures. AgentMail is served over verified
HTTPS, and the platform API records every POST. No source is mounted into the
container and no released image can substitute for the current Docker build.

The shared MailHandler implements the four provider call shapes documented at:
https://docs.agentmail.to/api-reference/inboxes/messages/list
https://docs.agentmail.to/api-reference/inboxes/messages/get
https://docs.agentmail.to/api-reference/inboxes/threads/get
https://docs.agentmail.to/api-reference/inboxes/messages/reply
Its listing filtering and pagination follow those docs. Listing IDs are also
recorded after the response is written, so an empty or unreachable provider
cannot masquerade as successful authentication refusal.
"""

from __future__ import annotations

import json
import re
import shutil
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pytest
from _support import (
    AGENTMAIL_API_KEY,
    ALLOWED_SENDER,
    CHANNEL_TOKEN,
    EGRESS_SECRET,
    INBOX,
    IngressHandler,
    IngressState,
    MailHandler,
    MailState,
)

REPO = Path(__file__).resolve().parents[3]
IMAGE_LABEL = "io.curie.test.mail-proof-image"
CONTAINER_LABEL = "io.curie.test.mail-proof-container"
MESSAGE_IDS = {"msg-packaged-turn", "msg-packaged-approval"}


def _run(argv: list[str], *, timeout: float = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, cwd=REPO, capture_output=True, text=True, timeout=timeout, check=False
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode:
        raise RuntimeError(
            f"{purpose} failed with exit {result.returncode}:\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout


def _docker(*args: str, timeout: float = 30) -> str:
    return _require(_run(["docker", *args], timeout=timeout), f"Docker {args[0]}")


def _owned_containers(owner: str) -> list[str]:
    return _docker("ps", "-aq", "--filter", f"label={CONTAINER_LABEL}={owner}").split()


def _cleanup_containers(owner: str) -> None:
    owned = _owned_containers(owner)
    if owned:
        _docker("rm", "--force", *owned)
    if _owned_containers(owner):
        raise RuntimeError("packaged mail proof left an owned container behind")


@pytest.fixture(scope="module")
def packaged_image(request: pytest.FixtureRequest) -> str:
    if sys.platform != "linux":
        raise RuntimeError("the packaged mail proof requires Linux host networking")
    for tool in ("docker", "openssl"):
        if shutil.which(tool) is None:
            raise RuntimeError(f"the packaged mail proof requires {tool}")
    engine = _docker("info", "--format", "{{.OSType}}")
    if engine.strip() != "linux":
        raise RuntimeError("the packaged mail proof requires a Linux Docker engine")

    owner = uuid.uuid4().hex
    image = f"curie-mail-proof-3953:{owner}"

    def cleanup() -> None:
        owned = _docker("image", "ls", "-q", "--filter", f"label={IMAGE_LABEL}={owner}")
        if owned.strip():
            _docker("image", "rm", "--force", image)
        remaining = _docker("image", "ls", "-q", "--filter", f"label={IMAGE_LABEL}={owner}")
        if remaining.strip():
            raise RuntimeError("packaged mail proof left its candidate image behind")

    request.addfinalizer(cleanup)
    _docker(
        "build",
        "--file",
        str(REPO / "apps/mail-adapter/Dockerfile"),
        "--tag",
        image,
        "--label",
        f"{IMAGE_LABEL}={owner}",
        str(REPO),
        timeout=1200,
    )
    return image


class RecordedMailState(MailState):
    def __init__(self) -> None:
        super().__init__()
        self.record_lock = threading.Lock()
        self.served_listings: list[tuple[str, ...]] = []

    def served_ids(self) -> set[str]:
        with self.record_lock:
            return {message_id for listing in self.served_listings for message_id in listing}


class RecordingMailHandler(MailHandler):
    def _send(self, status: int, payload: dict[str, Any]) -> None:
        super()._send(status, payload)
        parts = self._parts()
        if status == 200 and len(parts) == 4 and parts[3] == "messages":
            state = cast(RecordedMailState, self.state)
            served = tuple(str(message["message_id"]) for message in payload["messages"])
            with state.record_lock:
                state.served_listings.append(served)


class RecordedIngressState(IngressState):
    def __init__(self) -> None:
        super().__init__()
        self.post_paths: list[str] = []


class RecordingIngressHandler(IngressHandler):
    def do_POST(self) -> None:
        cast(RecordedIngressState, self.state).post_paths.append(self.path)
        super().do_POST()


@dataclass
class ExternalApis:
    mail: RecordedMailState
    platform: RecordedIngressState
    certificate: Path


@pytest.fixture
def external_apis(request: pytest.FixtureRequest, tmp_path: Path) -> ExternalApis:
    certificate = tmp_path / "provider-ca.pem"
    private_key = tmp_path / "provider-key.pem"
    _require(
        _run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-keyout",
                str(private_key),
                "-out",
                str(certificate),
            ]
        ),
        "generating the local HTTPS fixture certificate",
    )
    certificate.chmod(0o644)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certificate, private_key)
    servers: list[tuple[ThreadingHTTPServer, threading.Thread]] = []

    def cleanup() -> None:
        errors = []
        for server, thread in reversed(servers):
            try:
                if thread.is_alive():
                    server.shutdown()
                server.server_close()
                if thread.ident is not None:
                    thread.join(timeout=5)
                if thread.is_alive():
                    errors.append("packaged mail proof left an external API fixture running")
            except Exception as exc:
                errors.append(str(exc))
        if errors:
            raise RuntimeError("; ".join(errors))

    request.addfinalizer(cleanup)

    def start(handler: type[BaseHTTPRequestHandler], state: Any, *, tls: bool) -> int:
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server.daemon_threads = True
        server.state = state  # type: ignore[attr-defined]
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        )
        servers.append((server, thread))
        if tls:
            server.socket = context.wrap_socket(server.socket, server_side=True)
        thread.start()
        return int(server.server_address[1])

    mail = RecordedMailState()
    platform = RecordedIngressState()
    mail.base_url = f"https://127.0.0.1:{start(RecordingMailHandler, mail, tls=True)}/v0"
    platform.url = f"http://127.0.0.1:{start(RecordingIngressHandler, platform, tls=False)}"
    return ExternalApis(mail, platform, certificate)


@dataclass
class PackagedProcess:
    container_id: str
    apis: ExternalApis
    base_url: str = ""

    def state(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(_docker("inspect", self.container_id))[0]["State"])

    def logs(self) -> str:
        result = _run(["docker", "logs", self.container_id])
        _require(result, "reading the packaged adapter logs")
        return result.stdout + result.stderr

    def stop(self) -> None:
        _docker("stop", "--time", "10", self.container_id, timeout=20)


@pytest.fixture
def start_packaged(
    request: pytest.FixtureRequest, packaged_image: str, external_apis: ExternalApis
) -> Callable[..., PackagedProcess]:
    owner = uuid.uuid4().hex
    request.addfinalizer(lambda: _cleanup_containers(owner))

    def start(*, senders: str, opt_in: bool = False) -> PackagedProcess:
        env = {
            "AGENTMAIL_API_KEY": AGENTMAIL_API_KEY,
            "AGENTMAIL_INBOX": INBOX,
            "AGENTMAIL_BASE_URL": external_apis.mail.base_url,
            "CURIE_API_URL": external_apis.platform.url,
            "CURIE_CHANNEL_TOKEN": CHANNEL_TOKEN,
            "CURIE_EGRESS_SECRET": EGRESS_SECRET,
            "CURIE_ADAPTER_PRINCIPAL": "adp.packaged.example",
            "ADAPTER_INGRESS_ENABLED": "true",
            "CURIE_MAIL_ALLOWED_SENDERS": senders,
            "CURIE_MAIL_POLL_INTERVAL_SECONDS": "0.05",
            "CURIE_MAIL_PORT": "0",
            "CURIE_MAIL_STATE_PATH": "/tmp/packaged-mail-state.sqlite3",
            "SSL_CERT_FILE": "/tmp/provider-ca.pem",
            "OTEL_SDK_DISABLED": "true",
        }
        if opt_in:
            env["CURIE_MAIL_ALLOW_ALL_SENDERS"] = "true"
        args = [
            "run",
            "--detach",
            "--network",
            "host",
            "--label",
            f"{CONTAINER_LABEL}={owner}",
            "--mount",
            f"type=bind,src={external_apis.certificate},dst=/tmp/provider-ca.pem,readonly",
        ]
        for name, value in env.items():
            args.extend(("--env", f"{name}={value}"))
        # No command or entrypoint override: the built image's CMD is the proof.
        container_id = _docker(*args, packaged_image).strip()
        return PackagedProcess(container_id, external_apis)

    return start


def _health(base_url: str, path: str) -> bool:
    try:
        with urllib.request.urlopen(base_url + path, timeout=0.5) as response:
            return cast(int, response.status) == 200
    except (OSError, urllib.error.URLError):
        return False


@pytest.fixture
def default_wildcard(start_packaged: Callable[..., PackagedProcess]) -> PackagedProcess:
    return start_packaged(senders="*")


@pytest.fixture(
    params=[(ALLOWED_SENDER, False), ("*", True)], ids=["allowlisted", "wildcard-opt-in"]
)
def polling_packaged(
    request: pytest.FixtureRequest, start_packaged: Callable[..., PackagedProcess]
) -> PackagedProcess:
    senders, opt_in = request.param
    process = start_packaged(senders=senders, opt_in=opt_in)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        logs = process.logs()
        port = re.search(r"mail adapter starting: kind=email port=(\d+)\b", logs)
        if port:
            process.base_url = f"http://127.0.0.1:{port.group(1)}"
            if (
                _health(process.base_url, "/healthz")
                and _health(process.base_url, "/readyz")
                and process.apis.mail.list_calls > 0
            ):
                return process
        if not process.state()["Running"]:
            raise RuntimeError(f"packaged adapter exited during fixture startup:\n{logs}")
        time.sleep(0.05)
    raise RuntimeError(f"packaged adapter did not become healthy and poll HTTPS:\n{process.logs()}")


def test_packaged_wildcard_sender_filter_refuses_without_opt_in(
    default_wildcard: PackagedProcess,
) -> None:
    deadline = time.monotonic() + 10
    while default_wildcard.state()["Running"] and time.monotonic() < deadline:
        time.sleep(0.05)
    state = default_wildcard.state()
    assert not state["Running"], "wildcard configuration must refuse the image's default boot"
    assert state["ExitCode"] != 0
    assert "CURIE_MAIL_ALLOW_ALL_SENDERS must be explicitly true" in default_wildcard.logs()
    assert default_wildcard.apis.mail.list_calls == 0
    assert default_wildcard.apis.platform.post_paths == []


def test_packaged_process_refuses_unverifiable_turn_and_approval_answer(
    polling_packaged: PackagedProcess,
) -> None:
    process = polling_packaged
    mail = process.apis.mail
    initial_lists = mail.list_calls
    # Unlabelled mail is visible under AgentMail's documented default filtering.
    # A generic Authentication-Results header has no guaranteed provenance:
    # https://docs.agentmail.to/api-reference/inboxes/messages/get
    # https://docs.agentmail.to/knowledge-base/inbound-emails-missing
    with mail.record_lock:
        mail.add_inbound(
            "msg-packaged-turn", "thr-packaged-turn", sender=ALLOWED_SENDER, text="Hello"
        )
        mail.add_inbound(
            "msg-packaged-approval",
            "thr-packaged-approval",
            sender=ALLOWED_SENDER,
            text="APPROVE",
            full_text="APPROVE\nApproval reference: curie-approval-" + "a" * 24,
            headers={
                "From": ALLOWED_SENDER,
                "Message-ID": "<approval-answer@example.com>",
                "Authentication-Results": "mx.example.com; dmarc=pass header.from=example.com",
            },
        )

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if (
            mail.served_ids() == MESSAGE_IDS
            and mail.list_calls >= initial_lists + 3
            and process.logs().count("reason=authentication_unverifiable") >= 2
        ):
            break
        time.sleep(0.05)
    assert mail.served_ids() == MESSAGE_IDS, (
        "both messages must actually be served in HTTPS listings"
    )
    assert mail.list_calls >= initial_lists + 3, "the running adapter must keep polling"
    assert _health(process.base_url, "/healthz")
    assert _health(process.base_url, "/readyz")
    process.stop()
    records = [json.loads(line) for line in process.logs().splitlines()]
    refusals = [
        record for record in records if "reason=authentication_unverifiable" in record["message"]
    ]
    assert len(refusals) == 2
    assert all(record["severity"] == "WARNING" for record in refusals)
    correlations = set()
    for record in refusals:
        match = re.search(r"rejected correlation=([a-f0-9]+):", record["message"])
        assert match is not None, "each refusal must carry its correlation token"
        correlations.add(match.group(1))
    assert len(correlations) == 2, "both served messages must produce distinct refusal records"
    assert process.apis.platform.post_paths == [], (
        "turn and approval resolution must never be posted"
    )
    assert process.apis.platform.requests == []
    assert process.apis.platform.resolves == []
    assert mail.body_calls == {}, "authentication refusal must precede body or approval parsing"
    assert mail.replies == []
    assert all(
        query.get(key) == "false"
        for query in mail.list_queries
        for key in ("include_spam", "include_blocked", "include_unauthenticated")
    )
    assert set(mail.list_authorization) == {f"Bearer {AGENTMAIL_API_KEY}"}

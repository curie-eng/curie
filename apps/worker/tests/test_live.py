"""Live proof that a real Slack PDF reaches the model through the worker.

The default tier proves that the production Slack transport refuses redirects.
The live tier requires ``CURIE_LIVE_PDF_PROOF=1`` and the platform, Slack,
and Valkey environment named in ``_REQUIRED_ENV``. Run both with
``uv run pytest apps/worker/tests/test_live.py -q``. The external driver owns
the isolated stack, Valkey database, worker, runner, and their teardown.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import httpx
import pytest
from aci_protocol import (
    RUNS_STREAM_DEFAULT,
    WORKER_GROUP_DEFAULT,
    Attachment,
    QueuedTurn,
    ReplyHandle,
    TurnSource,
)
from curie_dispatcher.queue import to_stream_fields
from curie_worker.attachments import SlackFileClient, SlackFileError
from redis import Redis
from redis.exceptions import RedisError

_SLACK_API = "https://slack.com/api"
_FILE_NAME = "proof.pdf"
_PROMPT = "Read the attached PDF and reply with exactly the hidden code printed inside it."
_PLACEHOLDER = "Reading the attached PDF."
_REQUIRED_ENV = (
    "CURIE_API_URL",
    "CURIE_API_KEY",
    "SLACK_BOT_TOKEN",
    "SLACK_TEST_CHANNEL",
    "CURIE_LIVE_PDF_AGENT",
    "VALKEY_HOST",
    "VALKEY_PORT",
)


class _ProofError(RuntimeError):
    """A failure safe to show without exposing a live identifier or body."""


@dataclass(frozen=True)
class _LiveConfig:
    api_url: str
    api_key: str = field(repr=False)
    slack_token: str = field(repr=False)
    slack_channel: str
    agent_name: str
    valkey_host: str
    valkey_port: int
    valkey_db: int
    valkey_password: str | None = field(repr=False)
    stream: str
    group: str
    deadline_s: float


def _live_config() -> _LiveConfig:
    if os.environ.get("CURIE_LIVE_PDF_PROOF") != "1":
        pytest.skip("set CURIE_LIVE_PDF_PROOF=1 to run the live PDF proof")

    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.fail(
            "live PDF proof prerequisites are incomplete: " + ", ".join(missing),
            pytrace=False,
        )
    try:
        port = int(os.environ["VALKEY_PORT"])
        database = int(os.environ.get("VALKEY_DB", "0"))
        requested_deadline = float(os.environ.get("CURIE_LIVE_PDF_TIMEOUT_SECONDS", "180"))
    except ValueError:
        pytest.fail("live PDF proof numeric prerequisites are invalid", pytrace=False)
    if port <= 0 or database < 0 or requested_deadline <= 0:
        pytest.fail(
            "live PDF proof numeric prerequisites are outside their valid range",
            pytrace=False,
        )

    return _LiveConfig(
        api_url=os.environ["CURIE_API_URL"].rstrip("/"),
        api_key=os.environ["CURIE_API_KEY"],
        slack_token=os.environ["SLACK_BOT_TOKEN"],
        slack_channel=os.environ["SLACK_TEST_CHANNEL"],
        agent_name=os.environ["CURIE_LIVE_PDF_AGENT"],
        valkey_host=os.environ["VALKEY_HOST"],
        valkey_port=port,
        valkey_db=database,
        valkey_password=os.environ.get("VALKEY_PASSWORD") or None,
        stream=os.environ.get("CURIE_STREAM", RUNS_STREAM_DEFAULT),
        group=os.environ.get("CURIE_CONSUMER_GROUP", WORKER_GROUP_DEFAULT),
        deadline_s=min(requested_deadline, 180.0),
    )


def _one_page_pdf(code: str) -> bytes:
    content = f"BT /F1 18 Tf 72 700 Td ({code}) Tj ET\n".encode("ascii")
    objects = (
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
        ),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(content)).encode("ascii") + b" >>\nstream\n"
        + content
        + b"endstream",
    )
    document = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for number, body in enumerate(objects, start=1):
        offsets.append(len(document))
        document.extend(f"{number} 0 obj\n".encode("ascii"))
        document.extend(body)
        document.extend(b"\nendobj\n")
    xref = len(document)
    document.extend(f"xref\n0 {len(offsets)}\n".encode("ascii"))
    document.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        document.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    document.extend(
        f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode(
            "ascii"
        )
    )
    return bytes(document)


def _slack_call(
    client: httpx.Client,
    token: str,
    method: str,
    *,
    timeout: float,
    data: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        response = client.post(
            f"{_SLACK_API}/{method}",
            headers={"Authorization": f"Bearer {token}"},
            data=data,
            json=json_body,
            timeout=max(0.1, timeout),
        )
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        raise _ProofError(f"Slack {method} did not return a usable response") from None
    if (
        response.status_code != 200
        or not isinstance(payload, dict)
        or payload.get("ok") is not True
    ):
        raise _ProofError(f"Slack {method} did not return a success envelope")
    return cast(dict[str, Any], payload)


def _api_get(
    client: httpx.Client, config: _LiveConfig, path: str, timeout: float
) -> list[dict[str, Any]]:
    try:
        response = client.get(
            config.api_url + path,
            headers={"X-API-Key": config.api_key},
            timeout=max(0.1, timeout),
        )
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        raise _ProofError("Curie API prerequisite check did not return a usable response") from None
    if response.status_code != 200 or not isinstance(payload, list):
        raise _ProofError("Curie API prerequisite check returned an unexpected shape")
    if not all(isinstance(row, dict) for row in payload):
        raise _ProofError("Curie API prerequisite check returned invalid rows")
    return cast(list[dict[str, Any]], payload)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _ProofError("live PDF proof exceeded its bounded deadline")
    return min(15.0, remaining)


def _preflight(
    web: httpx.Client, redis_client: Redis, config: _LiveConfig, deadline: float
) -> str:
    identity = _slack_call(
        web, config.slack_token, "auth.test", timeout=_remaining(deadline)
    )
    user_id = identity.get("user_id")
    if not isinstance(user_id, str) or not user_id:
        raise _ProofError("Slack identity prerequisite returned no bot identity")

    channel = _slack_call(
        web,
        config.slack_token,
        "conversations.info",
        timeout=_remaining(deadline),
        data={"channel": config.slack_channel},
    ).get("channel")
    if (
        not isinstance(channel, dict)
        or channel.get("id") != config.slack_channel
        or channel.get("is_member") is not True
    ):
        raise _ProofError("Slack channel prerequisite did not confirm exact membership")

    agents = _api_get(web, config, "/agents", _remaining(deadline))
    matches = [row for row in agents if row.get("name") == config.agent_name]
    if len(matches) != 1:
        raise _ProofError(f"Curie agent prerequisite found {len(matches)} matching rows")
    agent = matches[0]
    channels = agent.get("channels")
    if not isinstance(channels, list) or not any(
        isinstance(binding, dict)
        and binding.get("kind") == "slack"
        and binding.get("address") == config.slack_channel
        for binding in channels
    ):
        raise _ProofError("Curie agent prerequisite did not confirm the exact Slack binding")
    agent_id = agent.get("id")
    if not isinstance(agent_id, str) or not agent_id:
        raise _ProofError("Curie agent prerequisite returned no agent identity")
    deployments = _api_get(
        web, config, f"/deployments?agent_id={agent_id}", _remaining(deadline)
    )
    if not any(
        row.get("agent_id") == agent_id and row.get("status") == "active"
        for row in deployments
    ):
        raise _ProofError("Curie agent prerequisite found no active deployment")
    try:
        consumers = redis_client.xinfo_consumers(config.stream, config.group)
    except RedisError:
        raise _ProofError(
            "Valkey prerequisite check could not inspect the consumer group"
        ) from None
    if not consumers:
        raise _ProofError("Valkey prerequisite found no worker consumer")
    return user_id


def test_slack_transport_refuses_a_cross_origin_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("no_proxy", "*")
    monkeypatch.setenv("NO_PROXY", "*")

    observed: list[dict[str, str]] = []
    source_authorizations: list[str] = []

    class _Observer(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            observed.append(
                {"path": self.path, "authorization": self.headers.get("Authorization", "")}
            )
            body = b"observer ready"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    observer = ThreadingHTTPServer(("127.0.0.1", 0), _Observer)
    observer.daemon_threads = True
    observer_url = f"http://127.0.0.1:{observer.server_address[1]}/observe"

    class _Source(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            source_authorizations.append(self.headers.get("Authorization", ""))
            self.send_response(302)
            self.send_header("Location", observer_url)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), _Source)
    source.daemon_threads = True
    threads = [
        threading.Thread(target=observer.serve_forever, daemon=True),
        threading.Thread(target=source.serve_forever, daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        with urllib.request.urlopen(observer_url, timeout=5) as control:
            assert control.status == 200
            assert control.read() == b"observer ready"
        assert len(observed) == 1
        observed.clear()

        client = SlackFileClient(
            token="synthetic_pdf_proof",
            api_url=f"http://127.0.0.1:{source.server_address[1]}",
        )
        with pytest.raises(SlackFileError) as refusal:
            client.fetch("synthetic_file")
        assert observed == []
        assert source_authorizations == ["Bearer synthetic_pdf_proof"]
        assert "HTTP 302" in str(refusal.value)
    finally:
        source.shutdown()
        observer.shutdown()
        source.server_close()
        observer.server_close()
        for thread in threads:
            thread.join(timeout=5)


def test_live_slack_pdf_reaches_the_model(caplog: pytest.LogCaptureFixture) -> None:
    config = _live_config()
    for logger_name in ("httpx", "httpcore"):
        caplog.set_level(logging.WARNING, logger=logger_name)
    deadline = time.monotonic() + config.deadline_s
    web = httpx.Client(follow_redirects=False)
    redis_client = Redis(
        host=config.valkey_host,
        port=config.valkey_port,
        db=config.valkey_db,
        password=config.valkey_password,
        decode_responses=True,
        socket_connect_timeout=min(5.0, config.deadline_s),
        socket_timeout=min(5.0, config.deadline_s),
    )
    file_id: str | None = None
    placeholder_ts: str | None = None
    failure: str | None = None
    cleanup_failures: list[str] = []
    try:
        bot_user_id = _preflight(web, redis_client, config, deadline)
        hidden_code = secrets.token_hex(16)
        pdf = _one_page_pdf(hidden_code)
        if (
            not pdf.startswith(b"%PDF-1.4")
            or not pdf.endswith(b"%%EOF\n")
            or pdf.count(b"/Type /Page ") != 1
            or pdf.count(hidden_code.encode("ascii")) != 1
        ):
            raise _ProofError("generated PDF failed local validity checks")

        # Slack requires getUploadURLExternal, a raw upload to the returned URL,
        # then completeUploadExternal.
        # https://docs.slack.dev/reference/methods/files.getUploadURLExternal/
        upload_slot = _slack_call(
            web,
            config.slack_token,
            "files.getUploadURLExternal",
            timeout=_remaining(deadline),
            data={"filename": _FILE_NAME, "length": str(len(pdf))},
        )
        upload_url = upload_slot.get("upload_url")
        file_id_value = upload_slot.get("file_id")
        if (
            not isinstance(upload_url, str)
            or not upload_url.startswith("https://")
            or not isinstance(file_id_value, str)
        ):
            raise _ProofError("Slack upload slot returned an unexpected shape")
        file_id = file_id_value
        try:
            uploaded = web.post(
                upload_url,
                content=pdf,
                headers={"Content-Type": "application/pdf"},
                timeout=_remaining(deadline),
            )
        except httpx.HTTPError:
            raise _ProofError("Slack raw file upload failed") from None
        if uploaded.status_code != 200:
            raise _ProofError("Slack raw file upload returned a failure status")
        posted = _slack_call(
            web,
            config.slack_token,
            "chat.postMessage",
            timeout=_remaining(deadline),
            json_body={"channel": config.slack_channel, "text": _PLACEHOLDER},
        )
        timestamp = posted.get("ts")
        if not isinstance(timestamp, str) or not timestamp:
            raise _ProofError("Slack placeholder returned no timestamp")
        placeholder_ts = timestamp

        # Complete the upload into the owned root's thread with no comment or
        # mention. Slack documents channel_id and thread_ts on this method.
        # https://docs.slack.dev/reference/methods/files.completeUploadExternal/
        completed = _slack_call(
            web,
            config.slack_token,
            "files.completeUploadExternal",
            timeout=_remaining(deadline),
            json_body={
                "files": [{"id": file_id, "title": _FILE_NAME}],
                "channel_id": config.slack_channel,
                "thread_ts": placeholder_ts,
            },
        )
        completed_files = completed.get("files")
        if not isinstance(completed_files, list) or not any(
            isinstance(row, dict) and row.get("id") == file_id for row in completed_files
        ):
            raise _ProofError("Slack file completion returned an unexpected shape")

        # The worker resolves Slack's documented direct download field through
        # files.info before it stores the bytes for the sandbox.
        # https://docs.slack.dev/reference/objects/file-object/
        turn = QueuedTurn(
            event_id=secrets.token_hex(16),
            conversation_id=placeholder_ts,
            author=bot_user_id,
            text=_PROMPT,
            source=TurnSource.SLACK,
            reply_handle=ReplyHandle(
                kind="slack",
                channel=config.slack_channel,
                placeholder=placeholder_ts,
                adapter=None,
            ),
            received_at=datetime.now(UTC).isoformat(),
            attachments=[
                Attachment(
                    id=file_id,
                    name=_FILE_NAME,
                    mime_type="application/pdf",
                    size_bytes=len(pdf),
                )
            ],
        )
        fields = to_stream_fields(turn)
        if hidden_code in json.dumps(fields, sort_keys=True):
            raise _ProofError("hidden PDF code escaped into the queued turn")
        try:
            queued_id = redis_client.xadd(config.stream, fields)
        except RedisError:
            raise _ProofError("Valkey refused the live proof turn") from None
        if not isinstance(queued_id, str):
            raise _ProofError("Valkey returned an unexpected queue receipt")

        while time.monotonic() < deadline:
            history = _slack_call(
                web,
                config.slack_token,
                "conversations.history",
                timeout=_remaining(deadline),
                data={
                    "channel": config.slack_channel,
                    "oldest": placeholder_ts,
                    "latest": placeholder_ts,
                    "inclusive": "true",
                    "limit": "1",
                },
            )
            messages = history.get("messages")
            if isinstance(messages, list) and any(
                isinstance(message, dict)
                and message.get("ts") == placeholder_ts
                and hidden_code in str(message.get("text", ""))
                for message in messages
            ):
                break
            time.sleep(min(2.0, max(0.1, deadline - time.monotonic())))
        else:
            raise _ProofError("Slack root was not updated with the PDF code before the deadline")

        while time.monotonic() < deadline:
            try:
                pending = redis_client.xpending_range(
                    config.stream,
                    config.group,
                    min=queued_id,
                    max=queued_id,
                    count=1,
                )
            except RedisError:
                raise _ProofError("Valkey could not confirm worker acknowledgement") from None
            if not pending:
                break
            time.sleep(min(0.2, max(0.05, deadline - time.monotonic())))
        else:
            raise _ProofError("worker did not acknowledge the live proof before the deadline")
    except Exception as exc:
        failure = (
            str(exc)
            if isinstance(exc, _ProofError)
            else f"live PDF proof failed ({type(exc).__name__})"
        )
    finally:
        cleanup_timeout = 10.0
        if placeholder_ts is not None:
            try:
                _slack_call(
                    web,
                    config.slack_token,
                    "chat.delete",
                    timeout=cleanup_timeout,
                    data={"channel": config.slack_channel, "ts": placeholder_ts},
                )
            except Exception:
                cleanup_failures.append("Slack message cleanup")
        if file_id is not None:
            try:
                _slack_call(
                    web,
                    config.slack_token,
                    "files.delete",
                    timeout=cleanup_timeout,
                    data={"file": file_id},
                )
            except Exception:
                cleanup_failures.append("Slack file cleanup")
        # The external driver owns this isolated Valkey database. Leaving the
        # entry avoids racing the worker between its visible Slack edit and ACK.
        redis_client.close()
        web.close()

    problems = ([failure] if failure is not None else []) + cleanup_failures
    if problems:
        pytest.fail("; ".join(problems), pytrace=False)

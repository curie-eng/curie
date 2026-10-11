"""Exercise replay matching and recording through the Messages HTTP boundary.

Streaming follows https://platform.claude.com/docs/en/api/messages-streaming.
The recording provider is Anthropic compatible, as documented at
https://openrouter.ai/docs/api-reference/overview.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _module() -> Any:
    spec = importlib.util.spec_from_file_location("curie_model_script", ROOT / "model_script.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ms = _module()


def _message(name: str, text: str) -> dict[str, Any]:
    return {
        "id": "msg_recorded",
        "type": "message",
        "role": "assistant",
        "model": "scripted",
        "content": [
            {
                "type": "tool_use",
                "id": "toolu_recorded",
                "name": name,
                "input": {"text": text},
            }
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 3, "output_tokens": 2},
    }


def _request(
    system: str, user: str, *, ordinal: int = 0, stream: bool = False
) -> dict[str, Any]:
    messages: list[dict[str, Any]] = [{"role": "user", "content": user}]
    for index in range(ordinal):
        messages += [
            {"role": "assistant", "content": f"Earlier answer {index}"},
            {"role": "user", "content": f"Later result {index}"},
        ]
    return {
        "model": "scripted",
        "max_tokens": 32,
        "stream": stream,
        "system": system,
        "messages": messages,
    }


def _exchange(user: str, tool: str, *, ordinal: int = 0) -> dict[str, Any]:
    return {
        "lane_key": ms.scrub(user),
        "ordinal": ordinal,
        "message": _message(tool, "recorded response"),
    }


def _post(
    base: str, payload: dict[str, Any], *, headers: dict[str, str] | None = None
) -> tuple[int, bytes, str]:
    request = Request(
        base + "/v1/messages",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, response.read(), response.headers.get("Content-Type", "")
    except HTTPError as exc:
        return exc.code, exc.read(), exc.headers.get("Content-Type", "")


def test_replay_ignores_system_tools_model_and_later_messages(tmp_path: Path) -> None:
    path = tmp_path / "transcript.json"
    ms.dump_transcript(path, [_exchange("plan the change", "Plan", ordinal=1)])
    script = ms.ModelScript(path)
    script.start()
    try:
        payload = _request(
            "Today is 2027-01-02. Completely different system.", "plan the change", ordinal=1
        )
        payload["model"] = "another-model"
        payload["tools"] = [{"name": "DifferentTool", "input_schema": {"type": "object"}}]
        payload["messages"][-1]["content"] = "Ran 8 tests in 0.987s instead of 0.123s"
        status, body, _ = _post(script.base_url, payload)
        assert status == 200
        assert json.loads(body)["content"][0]["name"] == "Plan"
    finally:
        script.close()


@pytest.mark.parametrize(
    ("recorded", "replayed"),
    [
        ("Review on 2026-10-06", "Review on 2027-01-02"),
        ("Tests finished in 0.123s", "Tests finished in 12.987s"),
        ("Ran tests in 0.123 seconds", "Ran tests in 2.987 seconds"),
        (
            "Review https://github.com/acme-corp/acme-bot/issues/4 at "
            "2026-10-06T10:20:30Z with toolu_abc msg_def " + "a" * 40,
            "Review https://github.com/acme-corp/acme-bot/issues/9 at "
            "2027-01-02T11:22:33Z with toolu_xyz msg_zzz " + "b" * 40,
        ),
    ],
)
def test_replay_scrubs_first_user_dates_durations_and_ids(
    tmp_path: Path, recorded: str, replayed: str
) -> None:
    path = tmp_path / "transcript.json"
    ms.dump_transcript(path, [_exchange(recorded, "Edit")])
    script = ms.ModelScript(path)
    script.start()
    try:
        status, body, content_type = _post(
            script.base_url, _request("A new system prompt", replayed, stream=True)
        )
        assert status == 200
        assert "text/event-stream" in content_type
        assert b"Edit" in body
    finally:
        script.close()


def test_replay_selects_interleaved_lanes_and_assistant_ordinals(tmp_path: Path) -> None:
    path = tmp_path / "transcript.json"
    ms.dump_transcript(
        path,
        [
            _exchange("plan the change", "Plan"),
            _exchange("review the diff", "Diff"),
            _exchange("plan the change", "Finish", ordinal=1),
        ],
    )
    script = ms.ModelScript(path)
    script.start()
    try:
        for user, ordinal, tool in [
            ("review the diff", 0, "Diff"),
            ("plan the change", 1, "Finish"),
            ("plan the change", 0, "Plan"),
        ]:
            status, body, _ = _post(
                script.base_url, _request("ignored system", user, ordinal=ordinal)
            )
            assert status == 200
            assert json.loads(body)["content"][0]["name"] == tool
    finally:
        script.close()


@pytest.mark.parametrize(
    ("user", "ordinal"), [("a different first user message", 0), ("plan the change", 1)]
)
def test_replay_rejects_an_unrecorded_lane_or_ordinal(
    tmp_path: Path, user: str, ordinal: int
) -> None:
    path = tmp_path / "transcript.json"
    ms.dump_transcript(path, [_exchange("plan the change", "Plan")])
    script = ms.ModelScript(path)
    script.start()
    try:
        status, body, _ = _post(
            script.base_url, _request("same system", user, ordinal=ordinal)
        )
        assert status == 422
        assert json.loads(body)["error"]["type"] == "unexpected_request"
    finally:
        with pytest.raises(ms.UnexpectedRequest):
            script.close()


def test_replay_cannot_reuse_an_exchange(tmp_path: Path) -> None:
    path = tmp_path / "transcript.json"
    ms.dump_transcript(path, [_exchange("plan the change", "Plan")])
    script = ms.ModelScript(path)
    script.start()
    try:
        payload = _request("ignored system", "plan the change")
        assert _post(script.base_url, payload)[0] == 200
        assert _post(script.base_url, payload)[0] == 422
    finally:
        with pytest.raises(ms.UnexpectedRequest):
            script.close()


def test_record_injects_environment_key_and_saves_only_replay_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider_key = "example-provider-key"
    client_key = "example-cluster-key"
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", provider_key)
    seen: list[dict[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            seen.append(
                {
                    "key": self.headers.get("x-api-key", ""),
                    "authorization": self.headers.get("Authorization", ""),
                    "version": self.headers.get("anthropic-version", ""),
                }
            )
            message = _message("Edit", "recorded")
            if json.loads(raw).get("stream"):
                payload = ms.message_to_sse(message)
                content_type = "text/event-stream"
            else:
                payload = json.dumps(message).encode()
                content_type = "application/json"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    output = tmp_path / "recorded.json"
    recorder = ms.ModelScript(
        output, record=True, upstream=f"http://127.0.0.1:{server.server_address[1]}"
    )
    recorder.start()
    try:
        for ordinal, stream in [(0, True), (1, False)]:
            status, body, _ = _post(
                recorder.base_url,
                _request(
                    "private system request marker", "add Kelvin", ordinal=ordinal, stream=stream
                ),
                headers={
                    "x-api-key": client_key,
                    "Authorization": f"Bearer {client_key}",
                    "anthropic-version": "2023-06-01",
                },
            )
            assert status == 200
            assert b"Edit" in body
    finally:
        recorder.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert len(seen) == 2
    assert all(item["key"] == provider_key for item in seen)
    assert all(client_key not in item["authorization"] for item in seen)
    assert all(item["version"] == "2023-06-01" for item in seen)
    text = output.read_text()
    for excluded in (provider_key, client_key, "private system request marker", "sk-or-"):
        assert excluded not in text
    saved = json.loads(text)
    assert saved["version"] == 1
    assert len(saved["exchanges"]) == 2
    assert all(
        set(exchange) == {"lane_key", "ordinal", "message"} for exchange in saved["exchanges"]
    )
    assert [exchange["ordinal"] for exchange in saved["exchanges"]] == [0, 1]
    assert all(exchange["lane_key"] == "add Kelvin" for exchange in saved["exchanges"])

    monkeypatch.delenv("CURIE_FACTORY_RECORD_KEY")
    replay = ms.ModelScript(output)
    replay.start()
    try:
        status, body, _ = _post(
            replay.base_url, _request("another system", "add Kelvin", ordinal=1)
        )
        assert status == 200
        assert json.loads(body)["content"][0]["name"] == "Edit"
    finally:
        replay.close()


def test_record_close_waits_for_an_in_flight_exchange_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A request still waiting on the provider at close must reach the transcript."""

    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "example-provider-key")
    received = threading.Event()
    release = threading.Event()

    class SlowProvider(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:  # noqa: N802
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            received.set()
            release.wait(timeout=10)
            payload = json.dumps(_message("Edit", "recorded")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), SlowProvider)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    output = tmp_path / "recorded.json"
    recorder = ms.ModelScript(
        output, record=True, upstream=f"http://127.0.0.1:{server.server_address[1]}"
    )
    recorder.start()
    results: list[tuple[int, bytes, str]] = []
    client = threading.Thread(
        target=lambda: results.append(
            _post(recorder.base_url, _request("system", "add Kelvin"))
        ),
        daemon=True,
    )
    releaser = threading.Timer(0.5, release.set)
    try:
        client.start()
        assert received.wait(timeout=5)
        releaser.start()
        recorder.close()
        # close() returned, so the in-flight handler finished writing its
        # response; the client only has to read bytes already sent.
        client.join(timeout=2)
        assert not client.is_alive()
        assert release.is_set()
        assert results and results[0][0] == 200
        saved = json.loads(output.read_text())
        assert len(saved["exchanges"]) == 1
        assert saved["exchanges"][0]["lane_key"] == "add Kelvin"
        assert saved["exchanges"][0]["message"]["content"][0]["name"] == "Edit"
    finally:
        release.set()
        releaser.cancel()
        client.join(timeout=5)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)


def test_committed_transcripts_store_no_requests_or_provider_keys() -> None:
    for path in (ROOT / "transcripts").glob("*.json"):
        text = path.read_text()
        assert "sk-or-" not in text
        for exchange in json.loads(text)["exchanges"]:
            assert set(exchange) == {"lane_key", "ordinal", "message"}

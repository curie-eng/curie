"""The scripted Messages endpoint matches content and fails closed (#3814).

Streaming frames follow https://platform.claude.com/docs/en/api/messages-streaming.
Recording proxies an Anthropic-compatible base such as
https://openrouter.ai/api (https://openrouter.ai/docs/api-reference/overview).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
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


def _request(system: str, user: str, *, stream: bool = False) -> dict[str, Any]:
    return {
        "model": "scripted",
        "max_tokens": 32,
        "stream": stream,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }


def _exchange(system: str, tool: str) -> dict[str, Any]:
    body = _request(system, "review this diff" if "diff" in system else "plan the change")
    return {
        "match": ms.normalize({"method": "POST", "path": "/v1/messages", "body": body}),
        "message": _message(tool, system),
    }


def _post(base: str, payload: dict[str, Any]) -> tuple[int, bytes, str]:
    data = json.dumps(payload).encode()
    request = Request(
        base + "/v1/messages",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, response.read(), response.headers.get("Content-Type", "")
    except Exception as exc:
        if not hasattr(exc, "code"):
            raise
        return int(exc.code), exc.read(), exc.headers.get("Content-Type", "")


def test_normalize_ignores_stream_ids_and_urls() -> None:
    left = {
        "system": "plan reviewer https://github.com/acme/fixture/issues/4",
        "stream": True,
        "messages": [{"content": "toolu_abc msg_def " + "a" * 40}],
    }
    right = {
        "stream": False,
        "messages": [{"content": "toolu_xyz msg_zzz " + "b" * 40}],
        "system": "plan reviewer https://github.com/other/repo/issues/9",
    }
    assert ms.normalize(left) == ms.normalize(right)


def test_serve_matches_plan_and_diff_reviewers_and_rejects_unknown(tmp_path: Path) -> None:
    path = tmp_path / "transcript.json"
    ms.dump_transcript(
        path,
        [
            _exchange("You are the plan reviewer.", "Plan"),
            _exchange("You are the diff reviewer.", "Diff"),
        ],
    )
    script = ms.ModelScript(path)
    script.start()
    try:
        status, body, content_type = _post(
            script.base_url,
            _request("You are the diff reviewer.", "review this diff", stream=True),
        )
        assert status == 200
        assert "text/event-stream" in content_type
        assert b"Diff" in body
        status, body, _ = _post(
            script.base_url, _request("You are the plan reviewer.", "plan the change")
        )
        assert status == 200
        assert json.loads(body)["content"][0]["name"] == "Plan"
        status, body, _ = _post(script.base_url, _request("You are a stranger.", "hello"))
        assert status == 422
        assert json.loads(body)["error"]["type"] == "unexpected_request"
    finally:
        with pytest.raises(ms.UnexpectedRequest):
            script.close()


def test_record_proxies_and_replays(tmp_path: Path) -> None:
    seen: dict[str, str] = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            seen["key"] = self.headers.get("x-api-key", "")
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
    thread_started = __import__("threading").Thread(target=server.serve_forever, daemon=True)
    thread_started.start()
    output = tmp_path / "recorded.json"
    try:
        recorder = ms.ModelScript(
            output,
            record=True,
            upstream=f"http://127.0.0.1:{server.server_address[1]}",
        )
        recorder.start()
        status, body, _ = _post(
            recorder.base_url,
            _request("You are the implementer.", "add fahrenheit", stream=True),
        )
        assert status == 200
        assert b"Edit" in body
        request = Request(
            recorder.base_url + "/v1/messages",
            data=json.dumps(
                _request("You are the implementer.", "add fahrenheit", stream=True)
            ).encode(),
            headers={"Content-Type": "application/json", "x-api-key": "sk-or-test-secret"},
            method="POST",
        )
        with urlopen(request, timeout=5) as response:
            assert response.status == 200
        recorder.close()
    finally:
        server.shutdown()
        server.server_close()
    assert seen["key"] == "sk-or-test-secret"
    saved = json.loads(output.read_text())
    assert len(saved["exchanges"]) == 2
    replay = ms.ModelScript(output)
    replay.start()
    try:
        status, body, _ = _post(
            replay.base_url,
            _request("You are the implementer.", "add fahrenheit"),
        )
        assert status == 200
        assert json.loads(body)["content"][0]["name"] == "Edit"
    finally:
        replay.close()

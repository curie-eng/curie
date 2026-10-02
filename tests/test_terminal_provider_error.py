"""Real HTTP contract for the ladder's bounded external-provider failure fixture."""

from __future__ import annotations

import json
import selectors
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

FIXTURE = Path(__file__).resolve().parents[1] / "cli/scripts/fixtures/terminal-provider-error.py"


@contextmanager
def provider() -> Iterator[tuple[str, subprocess.Popen[str]]]:
    assert FIXTURE.is_file(), "the terminal provider fixture must be shipped with the ladder"
    process = subprocess.Popen(
        [sys.executable, str(FIXTURE), "--port", "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        with selectors.DefaultSelector() as reader:
            reader.register(process.stdout, selectors.EVENT_READ)
            assert reader.select(timeout=5), "provider did not report its listening port"
        startup = json.loads(process.stdout.readline())
        assert set(startup) == {"port"}
        yield f"http://127.0.0.1:{startup['port']}", process
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def health(url: str) -> dict[str, int]:
    with urlopen(url + "/health", timeout=5) as response:
        return json.load(response)  # type: ignore[no-any-return]


def test_terminal_provider_failure_is_nonretryable_and_never_logs_request_secrets() -> None:
    # Provider retry directive: official Anthropic SDK retry handling source.
    # https://github.com/anthropics/anthropic-sdk-typescript/blob/main/src/client.ts
    # Also observed with pinned claude-agent-sdk0.2.159: one POST, terminal
    # model-credential-rejected/classified-failure instead of repeated401 calls.
    secret = "EXAMPLE-provider-key-do-not-echo"
    prompt = "EXAMPLE-private-prompt-do-not-echo"
    with provider() as (url, process):
        assert health(url) == {"requests": 0}
        request = Request(
            url + "/v1/messages",
            data=json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode(),
            headers={"X-Api-Key": secret, "Content-Type": "application/json"},
        )
        try:
            urlopen(request, timeout=5)
        except HTTPError as error:
            assert error.code == 401
            assert error.headers["x-should-retry"] == "false"
            body = error.read().decode()
            result = json.loads(body)
            assert result["type"] == "error"
            assert result["error"]["type"] == "authentication_error"
            assert secret not in body and prompt not in body
        else:
            raise AssertionError("the negative provider returned success")
        assert health(url) == {"requests": 1}
        process.terminate()
        stdout, stderr = process.communicate(timeout=5)
        assert secret not in stdout + stderr and prompt not in stdout + stderr
        assert stdout == "" and stderr == "", "request metadata must not enter fixture logs"


def test_provider_health_and_unknown_reads_do_not_fabricate_model_requests() -> None:
    with provider() as (url, _process):
        assert health(url) == {"requests": 0}
        try:
            urlopen(url + "/unknown", timeout=5)
        except HTTPError as error:
            assert error.code == 404
        else:
            raise AssertionError("unknown provider reads must fail")
        assert health(url) == {"requests": 0}

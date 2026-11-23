import json
import logging
import threading
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from curie_channel_gateway.slack.files import FileResponse, SlackFileClient, SlackFileError

FILE = "F0EXAMPLE1"
UPLOADER = "U0EXAMPLE1"
TOKEN = "example-provider-credential"
DOWNLOAD = "https://files.slack.com/files-pri/T0EXAMPLE1-F0EXAMPLE1/download/example-clip.m4a"


def _file(**updates):
    # Slack documents user as the creator, size in bytes, and duration_ms in milliseconds.
    # https://docs.slack.dev/reference/objects/file-object/
    # https://docs.slack.dev/reference/methods/files.info/
    return {
        "id": FILE,
        "user": UPLOADER,
        "mode": "hosted",
        "is_external": False,
        "file_access": "visible",
        "mimetype": "audio/mp4",
        "size": 5,
        "duration_ms": 1000,
        "url_private_download": DOWNLOAD,
        **updates,
    }


class _Chunks:
    def __init__(self, *parts):
        self.parts = iter(parts)
        self.closed = False
        self.read = 0

    def __iter__(self):
        return self

    def __next__(self):
        part = next(self.parts)
        self.read += 1
        return part

    def close(self):
        self.closed = True


def _client(metadata, download=None):
    requests = []
    responses = iter([metadata, download])

    def transport(**request):
        requests.append(request)
        response = next(responses)
        assert response is not None, "unexpected request to download file bytes"
        return response

    return SlackFileClient(token=TOKEN, transport=transport), requests


def _info(file):
    return FileResponse(200, {}, _Chunks(json.dumps({"ok": True, "file": file}).encode()))


def _read(client):
    return client.fetch_voice(FILE, UPLOADER, max_bytes=5, max_duration_ms=1000)


@pytest.mark.parametrize("known", [True, False])
def test_reads_a_bounded_clip_for_the_receipt_uploader(known):
    file = _file()
    if not known:
        del file["duration_ms"], file["size"], file["file_access"]
    info = _info(file)
    stream = _Chunks(b"abc", b"de")
    client, calls = _client(
        info, FileResponse(200, {"content-type": "application/octet-stream"}, stream)
    )

    clip = _read(client)

    assert clip.data == b"abcde"
    assert clip.mime_type == "audio/mp4"
    assert clip.duration_ms == (1000 if known else None)
    assert [call["url"] for call in calls] == [
        f"https://slack.com/api/files.info?file={FILE}",
        DOWNLOAD,
    ]
    assert all(call["headers"]["Authorization"] == f"Bearer {TOKEN}" for call in calls)
    assert stream.closed and info.chunks.closed
    assert "abcde" not in repr(clip)


@pytest.mark.parametrize(
    "update",
    [
        {"id": "F0EXAMPLE2"},
        {"user": "U0EXAMPLE2"},
        {"user": None},
        {"bot_id": "B0EXAMPLE1"},
        {"bot_user_id": UPLOADER},
        {"is_external": True},
        {"is_external": 0},
        {"is_external": None},
        {"mode": "snippet"},
        {"file_access": "check_file_info"},
        {"is_tombstoned": True},
        {"is_deleted": True},
        {"is_hidden_by_limit": 1},
        {"mimetype": "video/mp4"},
        {"mimetype": "audio/"},
        {"size": 6},
        {"size": True},
        {"size": -1},
        {"duration_ms": 1001},
        {"duration_ms": -1},
        {"duration_ms": "1000"},
        {"duration_ms": False},
        {"url_private_download": "https://files.slack.com.example.com/private"},
        {"url_private_download": "https://user@files.slack.com/private"},
        {"url_private_download": None},
    ],
)
def test_refuses_unusable_file_before_downloading(update):
    info = _info(_file(**update))
    client, calls = _client(info)
    with pytest.raises(SlackFileError):
        _read(client)
    assert len(calls) == 1 and info.chunks.closed


@pytest.mark.parametrize(
    "payload",
    [
        b'{"ok":false,"error":"example-private-error"}',
        b'{"ok":"true","file":{}}',
        b"[]",
        b'{"ok":true,"file":[]}',
        b"<private-body>",
        b"x" * 65537,
    ],
    ids=["denied", "invalid-ok", "list", "no-file", "non-json", "oversized"],
)
def test_refuses_unusable_files_info_response_without_disclosing_it(payload):
    info = FileResponse(200, {}, _Chunks(payload))
    client, calls = _client(info)
    with pytest.raises(SlackFileError) as refused:
        _read(client)
    assert len(calls) == 1 and info.chunks.closed
    assert "private" not in "".join(traceback.format_exception(refused.value))


@pytest.mark.parametrize(
    "status,content_type,parts,read",
    [
        (302, "audio/mp4", [b"unused"], 0),
        (403, "audio/mp4", [b"unused"], 0),
        (200, "text/html; charset=UTF-8", [b"unused"], 0),
        (200, "application/json", [b"unused"], 0),
        (200, "application/octet-stream", [b"<html", b">"], 2),
        (200, "audio/mp4", [b"abcd", b"ef", b"never read"], 2),
        (200, "audio/mp4", [b""], 1),
    ],
)
def test_refused_response_closes_at_the_first_failure(status, content_type, parts, read):
    stream = _Chunks(*parts)
    client, calls = _client(
        _info(_file()), FileResponse(status, {"Content-Type": content_type}, stream)
    )
    with pytest.raises(SlackFileError):
        _read(client)
    assert len(calls) == 2 and stream.read == read and stream.closed


@pytest.mark.parametrize(
    "headers",
    [{"Content-Length": "6"}, {"content-Length": "invalid"}, {"content-Encoding": "gzip"}],
)
def test_refuses_oversized_or_encoded_download_before_reading(headers):
    stream = _Chunks(b"abcdef")
    client, _ = _client(
        _info(_file()), FileResponse(200, {"Content-Type": "audio/mp4", **headers}, stream)
    )
    with pytest.raises(SlackFileError):
        _read(client)
    assert stream.closed and stream.read == 0


def test_incomplete_download_is_refused():
    stream = _Chunks(b"abc")
    client, _ = _client(_info(_file()), FileResponse(200, {"Content-Type": "audio/mp4"}, stream))
    with pytest.raises(SlackFileError):
        _read(client)
    assert stream.closed


def test_read_failure_closes_stream_and_omits_provider_diagnostic():
    class Interrupted(_Chunks):
        def __next__(self):
            raise OSError(f"example-private-error {DOWNLOAD} {TOKEN}")

    stream = Interrupted()
    client, _ = _client(_info(_file()), FileResponse(200, {"Content-Type": "audio/mp4"}, stream))
    with pytest.raises(SlackFileError) as refused:
        _read(client)
    assert stream.closed
    diagnostics = "".join(traceback.format_exception(refused.value))
    assert "example-private-error" not in diagnostics and DOWNLOAD not in diagnostics
    assert TOKEN not in diagnostics


def test_html_sign_in_body_with_binary_content_type_is_refused():
    html = b" \n<HTML lang=en>"
    client, _ = _client(
        _info(_file(size=len(html))),
        FileResponse(200, {"Content-Type": "audio/mp4"}, _Chunks(html)),
    )
    with pytest.raises(SlackFileError):
        client.fetch_voice(FILE, UPLOADER, max_bytes=len(html), max_duration_ms=1000)


def test_default_transport_keeps_request_url_and_redirect_target_out_of_logs(caplog):
    class Slack(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(302)
            self.send_header("Location", DOWNLOAD)
            self.end_headers()

        def log_message(self, format, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Slack) as server:
        thread = threading.Thread(target=server.handle_request, daemon=True)
        server.timeout = 5
        thread.start()
        try:
            client = SlackFileClient(
                token=TOKEN, api_url=f"http://127.0.0.1:{server.server_port}/example-api"
            )
            with caplog.at_level(logging.DEBUG), pytest.raises(SlackFileError) as refused:
                _read(client)
        finally:
            thread.join(timeout=6)
        assert not thread.is_alive()
    assert refused.value.status == 302
    diagnostics = caplog.text + "".join(traceback.format_exception(refused.value))
    assert "example-api" not in diagnostics and DOWNLOAD not in diagnostics
    assert FILE not in diagnostics and TOKEN not in diagnostics

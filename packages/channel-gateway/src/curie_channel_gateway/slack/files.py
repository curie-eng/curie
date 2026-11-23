"""Slack file reads behind the gateway file-source port."""

import json
import logging
import re
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field

import httpx

from ..ports.reads import FileSource

_MAX_INFO_BYTES = 64 * 1024
_SLACK_API_URL = "https://slack.com/api"
_AUDIO_TYPE = re.compile(r"audio/[a-z0-9][a-z0-9!#$&^_.+-]{0,126}")
_HTML_PAGE = re.compile(rb"(?:\xef\xbb\xbf)?\s*<(?:!doctype\s+html|html\b)", re.IGNORECASE)
_VOICE_REQUEST = ContextVar("slack_voice_request", default=False)


def _public_http_log(record: logging.LogRecord) -> bool:
    return not _VOICE_REQUEST.get()


# HTTPX logs the request URL at INFO. HTTP Core logs response headers, including
# Location, at DEBUG. Filter only this synchronous request's execution context.
for _logger in (
    "httpx",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.connection",
    "httpcore.socks",
):
    logging.getLogger(_logger).addFilter(_public_http_log)


@dataclass(frozen=True)
class FileResponse:
    status: int
    headers: Mapping[str, str]
    chunks: Iterator[bytes]

    def close(self) -> None:
        close = getattr(self.chunks, "close", None)
        if close is not None:
            close()


@dataclass(frozen=True)
class SlackVoiceFile:
    data: bytes = field(repr=False)
    mime_type: str
    duration_ms: int | None


def file_transport(
    *, method: str, url: str, headers: Mapping[str, str], chunk_bytes: int
) -> FileResponse:
    client = httpx.Client(timeout=30.0, follow_redirects=False)
    try:
        response = client.send(client.build_request(method, url, headers=headers), stream=True)
    except httpx.HTTPError:
        client.close()
        raise SlackFileError("provider file request failed", status=503) from None

    class ResponseChunks:
        def __init__(self) -> None:
            self._chunks = response.iter_bytes(chunk_bytes)

        def __iter__(self) -> Iterator[bytes]:
            return self

        def __next__(self) -> bytes:
            try:
                return next(self._chunks)
            except StopIteration:
                self.close()
                raise
            except httpx.HTTPError:
                self.close()
                raise SlackFileError("provider file request failed", status=503) from None

        def close(self) -> None:
            response.close()
            client.close()

    return FileResponse(response.status_code, dict(response.headers), ResponseChunks())


class SlackFileError(RuntimeError):
    """Slack refused a file lookup or download at the application level.

    Slack answers ``200`` with ``{"ok": false, "error": "..."}`` for an
    application-level failure, so translating that envelope belongs to the port
    rather than to the resolver above it. ``status`` is the HTTP status when the
    refusal was one, so an earlier file's unavailability can be named.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        self.status = status
        super().__init__(message)


class SlackFileClient:
    """Download one Slack file's bytes with the bot token, and only here.

    The download reads ``url_private_download`` rather than ``url_private``:
    Slack answers a PDF's ``url_private`` with a 302 to ``slack-files.com``,
    and the transport below deliberately refuses to follow it (see
    ``_NoRedirect``), so the bearer token never reaches a host Slack chose.
    ``url_private_download`` is Slack's documented direct-download endpoint on
    the same file object and does not redirect
    (https://docs.slack.dev/reference/objects/file-object). It is NOT public,
    though: the download must carry ``Authorization: Bearer <token>``, or
    Slack answers with an HTML sign-in page rather than the bytes.

    The token authenticates exactly these two requests and reaches nothing
    downstream -- what the sandbox receives is a presigned one-object URL from
    the private object store.
    """

    def __init__(
        self,
        *,
        token: str,
        transport: Callable[..., FileResponse] = file_transport,
        api_url: str = _SLACK_API_URL,
        read_chunk_bytes: int = 1024 * 1024,
    ) -> None:
        if not token:
            raise ValueError("attachment resolution requires a Slack bot token")
        if read_chunk_bytes <= 0:
            raise ValueError("read_chunk_bytes must be positive")
        self._token = token
        self._transport = transport
        self._api_url = api_url.rstrip("/")
        self._read_chunk_bytes = read_chunk_bytes

    def fetch(self, file_id: str) -> Iterator[bytes]:
        file_obj = self._file_info(file_id)
        return self._download(file_obj).chunks

    def fetch_voice(
        self, file_id: str, uploader_id: str, *, max_bytes: int, max_duration_ms: int
    ) -> SlackVoiceFile:
        """Read a hosted audio file selected by an admitted human receipt."""
        if max_bytes <= 0 or max_duration_ms <= 0:
            raise ValueError("voice file limits must be positive")
        if not file_id.strip() or not uploader_id.strip():
            raise SlackFileError("voice file receipt is incomplete")

        token = _VOICE_REQUEST.set(True)
        try:
            file_obj = self._file_info(file_id, bounded=True)
            if (
                file_obj.get("id") != file_id
                or file_obj.get("user") != uploader_id
                or file_obj.get("bot_id")
                or file_obj.get("bot_user_id")
            ):
                raise SlackFileError("voice file uploader or identity refused")
            if (
                file_obj.get("mode") != "hosted"
                or file_obj.get("is_external") is not False
                or file_obj.get("file_access", "visible") != "visible"
                or file_obj.get("is_deleted")
                or file_obj.get("is_tombstoned")
                or file_obj.get("is_hidden_by_limit")
            ):
                raise SlackFileError("voice file unavailable")
            mime = file_obj.get("mimetype")
            if not isinstance(mime, str) or not _AUDIO_TYPE.fullmatch(mime.lower()):
                raise SlackFileError("voice file is not audio")
            expected_size = _optional_measure(file_obj, "size", max_bytes)
            duration_ms = _optional_measure(file_obj, "duration_ms", max_duration_ms)
            byte_limit = expected_size or max_bytes
            response = self._download(file_obj, byte_limit=byte_limit)
            data = self._bounded_body(response, byte_limit, "voice file is oversized")
            if expected_size is not None and len(data) != expected_size:
                raise SlackFileError("voice file download was incomplete")
            if not data or _HTML_PAGE.match(data):
                raise SlackFileError("voice file contains no usable audio")
            return SlackVoiceFile(data, mime.lower(), duration_ms)
        except (httpx.HTTPError, OSError):
            raise SlackFileError("voice file request failed", status=503) from None
        finally:
            _VOICE_REQUEST.reset(token)

    def _download(
        self, file_obj: Mapping[str, object], *, byte_limit: int | None = None
    ) -> FileResponse:
        response = self._request(self._download_url(file_obj), byte_limit=byte_limit)
        if response.status != 200:
            response.close()
            raise SlackFileError(
                f"slack file download failed: HTTP {response.status}", status=response.status
            )
        content_type = self._header(response.headers, "content-type").partition(";")[0].strip()
        if content_type.startswith("text/html"):
            response.close()
            raise SlackFileError(
                "slack file download returned an HTML page rather than file bytes; "
                "the bot token is likely missing the files:read scope"
            )
        if byte_limit is not None and not (
            _AUDIO_TYPE.fullmatch(content_type) or content_type == "application/octet-stream"
        ):
            response.close()
            raise SlackFileError("voice download content type refused")
        return response

    def _file_info(self, file_id: str, *, bounded: bool = False) -> Mapping[str, object]:
        response = self._request(
            f"{self._api_url}/files.info?{httpx.QueryParams({'file': file_id})}",
            byte_limit=_MAX_INFO_BYTES if bounded else None,
        )
        if response.status != 200:
            response.close()
            raise SlackFileError(
                f"slack files.info failed: HTTP {response.status}", status=response.status
            )
        body = self._bounded_body(
            response, _MAX_INFO_BYTES, "slack files.info response is oversized"
        )
        try:
            payload = json.loads(body)
        except (ValueError, RecursionError):
            raise SlackFileError("slack files.info returned an unusable envelope") from None
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            raise SlackFileError("slack file metadata request refused")
        file_obj = payload.get("file")
        if not isinstance(file_obj, dict):
            raise SlackFileError("slack files.info returned an unusable envelope")
        return file_obj

    def _request(self, url: str, *, byte_limit: int | None) -> FileResponse:
        headers = {"Authorization": f"Bearer {self._token}"}
        chunk_bytes = self._read_chunk_bytes
        if byte_limit is not None:
            headers["Accept-Encoding"] = "identity"
            chunk_bytes = min(chunk_bytes, byte_limit + 1)
        response = self._transport(method="GET", url=url, headers=headers, chunk_bytes=chunk_bytes)
        if byte_limit is not None:
            if self._header(response.headers, "content-encoding") not in ("", "identity"):
                response.close()
                raise SlackFileError("voice file transfer encoding refused")
            length = self._header(response.headers, "content-length")
            if length:
                try:
                    valid = length.isascii() and length.isdecimal() and int(length) <= byte_limit
                except ValueError:
                    valid = False
                if not valid:
                    response.close()
                    raise SlackFileError("voice transfer outside permitted bounds")
        return response

    @staticmethod
    def _download_url(file_obj: Mapping[str, object]) -> str:
        if "url_private_download" not in file_obj:
            raise SlackFileError("slack files.info's file object has no url_private_download field")
        download_url = file_obj["url_private_download"]
        if not isinstance(download_url, str):
            raise SlackFileError("provider file URL is invalid")
        try:
            parsed = httpx.URL(download_url)
        except (httpx.InvalidURL, UnicodeError):
            raise SlackFileError("provider file URL is invalid") from None
        if (
            parsed.scheme != "https"
            or parsed.host != "files.slack.com"
            or parsed.userinfo
            or parsed.port not in (None, 443)
        ):
            raise SlackFileError("provider file download URL refused")
        return download_url

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str:
        for key, value in headers.items():
            if key.lower() == name:
                return value.strip().lower()
        return ""

    @staticmethod
    def _bounded_body(response: FileResponse, limit: int, oversized: str) -> bytes:
        body = bytearray()
        try:
            for chunk in response.chunks:
                if len(chunk) > limit - len(body):
                    raise SlackFileError(oversized)
                body.extend(chunk)
            return bytes(body)
        finally:
            response.close()


def _optional_measure(file_obj: Mapping[str, object], key: str, maximum: int) -> int | None:
    if key not in file_obj:
        return None
    value = file_obj[key]
    if type(value) is not int or not 0 < value <= maximum:
        raise SlackFileError("voice file metadata outside permitted bounds")
    return value


_CONFORMS: type[FileSource] = SlackFileClient

"""Slack file reads behind the gateway file-source port."""

import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass

import httpx

from ..ports.reads import FileSource

_MAX_INFO_BYTES = 64 * 1024
_SLACK_API_URL = "https://slack.com/api"


@dataclass(frozen=True)
class FileResponse:
    status: int
    headers: Mapping[str, str]
    chunks: Iterator[bytes]


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
        download_url = self._download_url(file_id)
        response = self._transport(
            method="GET",
            url=download_url,
            headers=self._headers(),
            chunk_bytes=self._read_chunk_bytes,
        )
        if response.status != 200:
            close = getattr(response.chunks, "close", None)
            if close is not None:
                close()
            raise SlackFileError(
                f"slack file download failed: HTTP {response.status}", status=response.status
            )
        content_type = self._content_type(response.headers)
        if content_type.startswith("text/html"):
            close = getattr(response.chunks, "close", None)
            if close is not None:
                close()
            # Slack serves its sign-in page, with a 200, when the bearer token
            # is missing or lacks files:read. Storing that page as the
            # "attachment" would hand the agent a login screen to read.
            raise SlackFileError(
                "slack file download returned an HTML page rather than file bytes; "
                "the bot token is likely missing the files:read scope"
            )
        return response.chunks

    def _download_url(self, file_id: str) -> str:
        response = self._transport(
            method="GET",
            url=f"{self._api_url}/files.info?{httpx.QueryParams({'file': file_id})}",
            headers=self._headers(),
            chunk_bytes=self._read_chunk_bytes,
        )
        if response.status != 200:
            close = getattr(response.chunks, "close", None)
            if close is not None:
                close()
            raise SlackFileError(
                f"slack files.info failed: HTTP {response.status}", status=response.status
            )
        body = self._bounded_body(response.chunks)
        try:
            payload = json.loads(body)
            if not payload.get("ok"):
                raise SlackFileError("slack file metadata request refused")
            file_obj = payload["file"]
            if not isinstance(file_obj, Mapping):
                raise TypeError("file object is not a mapping")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SlackFileError("slack files.info returned an unusable envelope") from exc
        # The envelope parsed fine and was ok, but the field this module reads
        # instead of url_private (see the class docstring) may still be
        # absent. Named explicitly rather than folded into the except above,
        # since a missing field is not the same failure as a malformed
        # envelope and an operator needs to be able to tell them apart.
        if "url_private_download" not in file_obj:
            raise SlackFileError("slack files.info's file object has no url_private_download field")
        download_url = str(file_obj["url_private_download"])
        try:
            parsed = httpx.URL(download_url)
        except httpx.InvalidURL:
            raise SlackFileError("provider file URL is invalid") from None
        if (
            parsed.scheme != "https"
            or parsed.host != "files.slack.com"
            or parsed.userinfo
            or parsed.port not in (None, 443)
        ):
            raise SlackFileError("provider file download URL refused")
        return download_url

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    @staticmethod
    def _content_type(headers: Mapping[str, str]) -> str:
        for key, value in headers.items():
            if str(key).lower() == "content-type":
                return str(value).lower()
        return ""

    @staticmethod
    def _bounded_body(chunks: Iterator[bytes]) -> bytes:
        body = bytearray()
        try:
            for chunk in chunks:
                body.extend(chunk)
                if len(body) > _MAX_INFO_BYTES:
                    raise SlackFileError("slack files.info response is oversized")
            return bytes(body)
        finally:
            close = getattr(chunks, "close", None)
            if close is not None:
                close()


_CONFORMS: type[FileSource] = SlackFileClient

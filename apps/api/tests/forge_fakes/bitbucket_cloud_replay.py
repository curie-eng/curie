"""Record and replay of Bitbucket Cloud REST exchanges (#4401, decision 16).

A cassette is one recorded session against the test workspace, scrubbed by
``bitbucket_cloud_record.scrub`` before it is written:

    {"schema": "curie.bitbucket-cloud.recording/v1",
     "meta": {"branch": "curie-record-<UTC timestamp>", "repo_uuid": "{...}", ...},
     "exchanges": [{"request": {"method", "path", "query": [[k, v], ...], "body"},
                    "response": {"status", "headers": {...}, "body"}}]}

Paths keep the API prefix (``/2.0/...``) and drop the host. Only the response
headers in `RESPONSE_HEADERS` are kept at all.

`ReplayTransport` serves the exchanges strictly in order and fails on any
request whose method, path, query (as a multiset) or JSON body differs from
the recording, and on any request carrying ``If-None-Match`` (decision 13.4).
`RecordingTransport` wraps the real transport for a live run and refuses the
201st request (decision 16.3). That cap counts every request of the recorded
session, its own teardown included, and the cassette is exactly those
requests. A live run that fails cleans up through `AllowanceTransport`
instead: at most `CLEANUP_REQUESTS` more requests (find the pull request when
the run never read it back, decline it, delete the branch), outside the cap and never recorded, since a failed run
writes no cassette. So an exhausted budget cannot block the cleanup.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx

SCHEMA = "curie.bitbucket-cloud.recording/v1"
RECORDINGS = Path(__file__).resolve().parent / "bitbucket_cloud_recordings"
LIFECYCLE = RECORDINGS / "lifecycle.json"
API_PREFIX = "/2.0"
MAX_REQUESTS = 200
# A failed live run's cleanup: one decline and one branch deletion.
CLEANUP_REQUESTS = 3

# The response headers a recording keeps; every other one is dropped unread.
RESPONSE_HEADERS = frozenset({"content-type", "retry-after"})
RATE_LIMIT_HEADER_PREFIX = "x-ratelimit-"


def kept_header(name: str) -> bool:
    lowered = name.lower()
    return lowered in RESPONSE_HEADERS or lowered.startswith(RATE_LIMIT_HEADER_PREFIX)


@dataclass(frozen=True)
class RecordedRequest:
    method: str
    path: str
    query: tuple[tuple[str, str], ...]
    body: Any

    def to_json(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "path": self.path,
            "query": [list(pair) for pair in self.query],
            "body": self.body,
        }


@dataclass(frozen=True)
class RecordedResponse:
    status: int
    headers: dict[str, str]
    body: Any

    def to_json(self) -> dict[str, Any]:
        return {"status": self.status, "headers": dict(self.headers), "body": self.body}


@dataclass(frozen=True)
class Exchange:
    request: RecordedRequest
    response: RecordedResponse

    def to_json(self) -> dict[str, Any]:
        return {"request": self.request.to_json(), "response": self.response.to_json()}


@dataclass
class Cassette:
    meta: dict[str, Any]
    exchanges: list[Exchange] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "meta": dict(self.meta),
            "exchanges": [exchange.to_json() for exchange in self.exchanges],
        }

    @classmethod
    def from_json(cls, document: Mapping[str, Any]) -> Cassette:
        if document.get("schema") != SCHEMA:
            raise ValueError(f"not a {SCHEMA} recording")
        return cls(
            meta=dict(document["meta"]),
            exchanges=[
                Exchange(
                    RecordedRequest(
                        method=e["request"]["method"],
                        path=e["request"]["path"],
                        query=tuple((k, v) for k, v in e["request"]["query"]),
                        body=e["request"]["body"],
                    ),
                    RecordedResponse(
                        status=e["response"]["status"],
                        headers=dict(e["response"]["headers"]),
                        body=e["response"]["body"],
                    ),
                )
                for e in document["exchanges"]
            ],
        )

    @classmethod
    def load(cls, path: Path) -> Cassette:
        return cls.from_json(json.loads(path.read_text()))

    def dump(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json(), indent=2, sort_keys=True) + "\n")

    def find(self, method: str, path_suffix: str, **query: str) -> int:
        """The index of the first exchange with this method, a path ending in
        ``path_suffix``, and at least these query values."""

        for index, exchange in enumerate(self.exchanges):
            request = exchange.request
            if request.method != method or not request.path.endswith(path_suffix):
                continue
            if all((k, v) in request.query for k, v in query.items()):
                return index
        raise LookupError(f"no recorded {method} ...{path_suffix} {query}")


def committed_recordings() -> list[Path]:
    return sorted(RECORDINGS.glob("*.json")) if RECORDINGS.is_dir() else []


def request_body(request: httpx.Request) -> Any:
    """A request's body as recorded: its JSON, its text, or None when empty."""

    content = request.content
    if not content:
        return None
    try:
        return json.loads(content)
    except ValueError:
        return content.decode()


def request_query(request: httpx.Request) -> tuple[tuple[str, str], ...]:
    return tuple(parse_qsl(request.url.query.decode(), keep_blank_values=True))


def response_body(response: httpx.Response) -> Any:
    if not response.content:
        return None
    try:
        return json.loads(response.content)
    except ValueError:
        return response.text


Fault = tuple[int, Mapping[str, str], Any]


class ReplayTransport(httpx.AsyncBaseTransport):
    """Serves a cassette's exchanges in order, asserting each request matches.

    ``faults`` maps an exchange index to a ``(status, headers, body)`` served in
    place of the recorded response; the request must still match.
    """

    def __init__(self, cassette: Cassette, *, faults: Mapping[int, Fault] | None = None) -> None:
        self.cassette = cassette
        self.faults = dict(faults or {})
        self.position = 0
        self.seen: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        index = self.position
        self.seen.append(request)
        described = f"{request.method} {request.url.path}"
        if "if-none-match" in request.headers:
            raise AssertionError(f"exchange {index}: {described} sent If-None-Match")
        if index >= len(self.cassette.exchanges):
            raise AssertionError(f"exchange {index}: {described} was never recorded")
        recorded = self.cassette.exchanges[index].request
        actual = RecordedRequest(
            request.method, request.url.path, request_query(request), request_body(request)
        )
        if (actual.method, actual.path) != (recorded.method, recorded.path):
            raise AssertionError(
                f"exchange {index}: {described}, recorded {recorded.method} {recorded.path}"
            )
        if Counter(actual.query) != Counter(recorded.query):
            raise AssertionError(
                f"exchange {index}: {described} query {list(actual.query)},"
                f" recorded {list(recorded.query)}"
            )
        if actual.body != recorded.body:
            raise AssertionError(
                f"exchange {index}: {described} body {actual.body!r}, recorded {recorded.body!r}"
            )
        self.position += 1
        if index in self.faults:
            status, headers, body = self.faults[index]
            response = RecordedResponse(status, dict(headers), body)
        else:
            response = self.cassette.exchanges[index].response
        return _response(request, response)

    def assert_consumed(self, upto: int | None = None) -> None:
        """Every exchange was served, or exactly the first ``upto`` of them."""

        expected = len(self.cassette.exchanges) if upto is None else upto
        assert self.position == expected, f"served {self.position} of {expected} recorded exchanges"


def _response(request: httpx.Request, recorded: RecordedResponse) -> httpx.Response:
    headers = dict(recorded.headers)
    if recorded.body is None:
        content = b""
    elif isinstance(recorded.body, str):
        content = recorded.body.encode()
    else:
        content = json.dumps(recorded.body).encode()
        headers.setdefault("content-type", "application/json")
    return httpx.Response(recorded.status, headers=headers, content=content, request=request)


class RecordingTransport(httpx.AsyncBaseTransport):
    """Passes requests to ``inner`` and keeps each exchange, unscrubbed.

    Only the allowlisted response headers are kept, and no request header at
    all, so a token never reaches the captured exchanges.
    """

    def __init__(self, inner: httpx.AsyncBaseTransport, *, limit: int = MAX_REQUESTS) -> None:
        self.inner = inner
        self.limit = limit
        self.exchanges: list[Exchange] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if len(self.exchanges) >= self.limit:
            raise AssertionError(f"a recording run may make at most {self.limit} requests")
        await request.aread()
        response = await self.inner.handle_async_request(request)
        content = await response.aread()
        await response.aclose()
        # The body is already decoded, so the encoding headers no longer apply.
        headers = [
            (k, v)
            for k, v in response.headers.multi_items()
            if k.lower() not in {"content-encoding", "content-length", "transfer-encoding"}
        ]
        replayed = httpx.Response(
            response.status_code, headers=headers, content=content, request=request
        )
        self.exchanges.append(
            Exchange(
                RecordedRequest(
                    request.method,
                    request.url.path,
                    request_query(request),
                    request_body(request),
                ),
                RecordedResponse(
                    response.status_code,
                    {k.lower(): v for k, v in response.headers.items() if kept_header(k)},
                    response_body(replayed),
                ),
            )
        )
        return replayed

    async def aclose(self) -> None:
        await self.inner.aclose()


class AllowanceTransport(httpx.AsyncBaseTransport):
    """Passes at most ``limit`` requests to ``inner`` and records none of them.

    The reserved allowance a failed live recording cleans up with, apart from
    the recorded session's budget.
    """

    def __init__(self, inner: httpx.AsyncBaseTransport, *, limit: int = CLEANUP_REQUESTS) -> None:
        self.inner = inner
        self.limit = limit
        self.used = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.used >= self.limit:
            raise AssertionError(f"a recording's cleanup may make at most {self.limit} requests")
        self.used += 1
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()

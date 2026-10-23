"""Reference client for the shared connector resolver (ADR 0239).

ADR 0239 "Bundle, identity and request contract" 5 makes this one
dependency-free source file the reference SDK: adopters vendor it into their
own Docker build directory and run their copy against the executable wire
corpus (``wire_corpus.json`` with ``run_wire_corpus.py``). It is standard
library only and runs on Python 3.12, because a connector image never
installs the Python 3.13 worker package. Other languages implement the wire
directly; the corpus is the shared definition of conformance.

Why the shape is what it is:

1. Resolve transaction commit is the admission boundary, so every provider
   API call made through ``ProviderClient.request`` performs exactly one new
   resolve. Nothing here caches a ``Resolution`` or any authorization: a
   handle that was good for the previous call proves nothing about the next
   one (a disable, a membership change or a reimport may have committed in
   between).
2. Each resolve opens a fresh AF_UNIX connection (the resolver answers one
   request per connection with ``Connection: close``), and each provider call
   opens a fresh ``HTTPSConnection`` that is closed afterwards. Nothing is
   pooled, so no connection outlives the authorization it was made under.
3. The URL origin must equal one of the origins the resolver returned for
   this binding (scheme and host compared lower case, the https default port
   443 made explicit). Only https is accepted. An undeclared origin raises
   ``OriginRefused`` before a byte is sent to the provider.
4. A 3xx answer raises ``RedirectRefused``. Redirects are never followed, so
   the credential is never re-sent to a location the resolver did not
   declare.
5. Auth and cache state belongs to one (binding, account, revision)
   partition. ``PartitionedCache`` keys every entry by that partition, so a
   reimport (a new revision) or another member (another binding) starts
   empty instead of reading state built under different credentials.

6. The request authority comes only from the checked origin. A caller (or
   an ``authorize`` callback) that sets ``Host`` gets ``ValueError`` before
   the resolve, so no header can point the request at another authority.
7. Every header name and value is checked before anything is sent: names
   are HTTP tokens, values are Latin-1 with no control character other than
   horizontal tab (so no CR or LF). A bad header raises ``ValueError`` with a
   fixed message that never quotes the value; ``http.client`` would
   otherwise quote it, Authorization included. A transport failure
   (``http.client``, ``ssl``, ``socket``) raises ``ProviderTransportError``
   naming only the failure's class, with no chained exception.

Secrets stay out of diagnostics: no exception message, ``repr``, chained
exception or log line here carries the invocation handle or credential
material.
``Resolution.fields`` and every ``Field`` value are excluded from ``repr``.
There is no module-level mutable state; every client owns what it uses.
"""

from __future__ import annotations

import base64
import binascii
import http.client
import json
import os
import re
import socket
import ssl
import threading
import urllib.parse
from collections.abc import Callable, Hashable, Mapping
from types import MappingProxyType
from typing import Any, Protocol

SOCKET_ENV = "CURIE_SHARED_RESOLVER_SOCKET"
RESOLVE_PATH = "/v1/resolve"

ADMISSION_CREDENTIAL = "credential"
ADMISSION_IDENTITY = "identity"

# A resolver answer is a small JSON document; anything larger is not one.
MAX_RESPONSE_BYTES = 1 << 20

_HTTPS = "https"
_HTTPS_DEFAULT_PORT = 443
_REFUSAL_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
# A sendable header value: Latin-1, horizontal tab allowed, no other C0 or C1
# control character (so no CR or LF) and no DEL.
_HEADER_VALUE = re.compile(r"[\t\x20-\x7e\xa0-\xff]*")
# A sendable request target: printable ASCII without spaces.
_REQUEST_TARGET = re.compile(r"[\x21-\x7e]+")
# The canonical spelling of the run and work item UUIDs the resolver answers.
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_HOST_HEADER = "host"


class ResolverRefused(Exception):
    """The resolver refused this resolve; ``str(exc)`` is the refusal code."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


class ResolverProtocolError(Exception):
    """The resolver's answer did not follow the wire contract.

    The message names what was wrong, never a received value.
    """


class OriginRefused(Exception):
    """The URL origin is not declared for this binding; nothing was sent."""

    def __init__(self) -> None:
        super().__init__("the URL origin is not a declared https origin of this binding")


class RedirectRefused(Exception):
    """The provider answered 3xx; it was not followed and nothing was re-sent."""

    def __init__(self, status: int, location: str | None) -> None:
        super().__init__(f"provider answered {status}; redirects are never followed")
        self.status = status
        self.location = location


class ProviderTransportError(Exception):
    """The provider call failed in transport (connection, TLS, HTTP framing).

    The message names only the failure's class; the underlying exception is
    not chained, because ``http.client`` and ``ssl`` messages can quote
    request data.
    """

    def __init__(self, kind: str) -> None:
        super().__init__(f"the provider call failed in transport ({kind})")
        self.kind = kind


class _Immutable:
    """Value objects without ``dataclasses``.

    ``@dataclass`` resolves string annotations through
    ``sys.modules[cls.__module__]``, which does not exist when a vendored copy
    is loaded by path with ``importlib`` and not registered there. Plain
    classes load however an adopter imports the file.
    """

    __slots__ = ()

    def _set(self, **values: object) -> None:
        for name, value in values.items():
            object.__setattr__(self, name, value)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")


class Field(_Immutable):
    """One credential field: exactly one of ``text`` or ``data`` is set.

    Neither value appears in ``repr``.
    """

    __slots__ = ("text", "data")
    text: str | None
    data: bytes | None

    def __init__(self, text: str | None = None, data: bytes | None = None) -> None:
        if (text is None) == (data is None):
            raise ValueError("a Field holds exactly one of text or data")
        self._set(text=text, data=data)

    def __repr__(self) -> str:
        kind = "text" if self.text is not None else "data"
        return f"Field({kind}=<redacted>)"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return NotImplemented
        return (self.text, self.data) == (other.text, other.data)

    def __hash__(self) -> int:
        return hash((self.text, self.data))


class Resolution(_Immutable):
    """One admitted resolve. Use it for one provider call, then drop it.

    ``fields`` is a read-only mapping and is excluded from ``repr``. ``run``
    and ``work_item`` are the run context of the verified caller token the
    proxy admitted: both set, or both None when the caller carried no run.
    """

    __slots__ = (
        "admission",
        "binding",
        "account",
        "revision",
        "origins",
        "expires_at",
        "slot",
        "fields",
        "run",
        "work_item",
    )
    admission: str
    binding: str
    account: str
    revision: int
    origins: tuple[str, ...]
    expires_at: int
    slot: str
    fields: Mapping[str, Field]
    run: str | None
    work_item: str | None

    def __init__(
        self,
        admission: str,
        binding: str,
        account: str,
        revision: int,
        origins: tuple[str, ...],
        expires_at: int,
        slot: str,
        fields: Mapping[str, Field],
        run: str | None = None,
        work_item: str | None = None,
    ) -> None:
        if (run is None) != (work_item is None):
            raise ValueError("a Resolution carries run and work_item together or neither")
        self._set(
            admission=admission,
            binding=binding,
            account=account,
            revision=revision,
            origins=tuple(origins),
            expires_at=expires_at,
            slot=slot,
            fields=MappingProxyType(dict(fields)),
            run=run,
            work_item=work_item,
        )

    @property
    def partition(self) -> tuple[str, str, int]:
        """The (binding, account, revision) that auth and cache state belongs to."""

        return (self.binding, self.account, self.revision)

    def _identity(self) -> tuple[object, ...]:
        return (
            self.admission,
            self.binding,
            self.account,
            self.revision,
            self.origins,
            self.expires_at,
            self.slot,
            self.run,
            self.work_item,
        )

    def __repr__(self) -> str:
        return (
            f"Resolution(admission={self.admission!r}, binding={self.binding!r}, "
            f"account={self.account!r}, revision={self.revision!r}, origins={self.origins!r}, "
            f"expires_at={self.expires_at!r}, slot={self.slot!r}, run={self.run!r}, "
            f"work_item={self.work_item!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Resolution):
            return NotImplemented
        return self._identity() == other._identity() and dict(self.fields) == dict(other.fields)

    def __hash__(self) -> int:
        return hash(self._identity())


class _Partitioned(Protocol):
    @property
    def partition(self) -> tuple[str, str, int]: ...


class _UnixHTTPConnection(http.client.HTTPConnection):
    """``http.client`` over one AF_UNIX connection."""

    def __init__(self, path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._unix_path = path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout)
            sock.connect(self._unix_path)
        except BaseException:
            sock.close()
            raise
        self.sock = sock


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ResolverProtocolError("the resolver answer repeats a key")
        document[key] = value
    return document


def _decode(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, ValueError):
        raise ResolverProtocolError("the resolver answer is not JSON") from None


def _string(document: Mapping[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise ResolverProtocolError(f"the resolver answer has no string {key!r}")
    return value


def _integer(document: Mapping[str, Any], key: str) -> int:
    value = document.get(key)
    if type(value) is not int:
        raise ResolverProtocolError(f"the resolver answer has no integer {key!r}")
    return value


def _fields(credential: Any) -> Mapping[str, Field]:
    if not isinstance(credential, dict) or not isinstance(credential.get("fields"), dict):
        raise ResolverProtocolError("the credential admission carries no fields")
    raw: dict[str, Any] = credential["fields"]
    if not raw:
        raise ResolverProtocolError("the credential admission carries no fields")
    fields: dict[str, Field] = {}
    for name, value in raw.items():
        if not isinstance(value, dict) or len(value) != 1:
            raise ResolverProtocolError("a credential field is not {text} or {bytes}")
        if isinstance(value.get("text"), str):
            fields[name] = Field(text=value["text"])
        elif isinstance(value.get("bytes"), str):
            try:
                data = base64.b64decode(value["bytes"].encode("ascii"), validate=True)
            except (UnicodeEncodeError, binascii.Error):
                raise ResolverProtocolError("a bytes field is not standard base64") from None
            fields[name] = Field(data=data)
        else:
            raise ResolverProtocolError("a credential field is not {text} or {bytes}")
    return MappingProxyType(fields)


def _run_context(document: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """The answer's ``run`` and ``work_item``: both absent, or both canonical UUIDs."""

    if "run" not in document and "work_item" not in document:
        return None, None
    run = document.get("run")
    work_item = document.get("work_item")
    if (
        not isinstance(run, str)
        or not isinstance(work_item, str)
        or _UUID.fullmatch(run) is None
        or _UUID.fullmatch(work_item) is None
    ):
        raise ResolverProtocolError("the resolver answer's run context is not a uuid pair")
    return run, work_item


def _resolution(document: Any, slot: str) -> Resolution:
    if not isinstance(document, dict):
        raise ResolverProtocolError("the resolver answer is not a JSON object")
    admission = _string(document, "admission")
    origins = document.get("origins")
    if not isinstance(origins, list) or not all(isinstance(item, str) for item in origins):
        raise ResolverProtocolError("the resolver answer has no origin list")
    answered_slot = _string(document, "slot")
    if answered_slot != slot:
        raise ResolverProtocolError("the resolver answered for another slot")
    if admission == ADMISSION_CREDENTIAL:
        fields = _fields(document.get("credential"))
    elif admission == ADMISSION_IDENTITY:
        if "credential" in document:
            raise ResolverProtocolError("an identity admission carries a credential")
        fields = MappingProxyType({})
    else:
        raise ResolverProtocolError("the resolver answer names an unknown admission")
    run, work_item = _run_context(document)
    return Resolution(
        admission=admission,
        binding=_string(document, "binding"),
        account=_string(document, "account"),
        revision=_integer(document, "revision"),
        origins=tuple(origins),
        expires_at=_integer(document, "expires_at"),
        slot=answered_slot,
        fields=fields,
        run=run,
        work_item=work_item,
    )


def _refusal(document: Any, status: int) -> ResolverRefused:
    if not isinstance(document, dict) or set(document) != {"refusal"}:
        raise ResolverProtocolError(f"the resolver answered {status} without a refusal code")
    code = document["refusal"]
    if not isinstance(code, str) or _REFUSAL_CODE.fullmatch(code) is None:
        raise ResolverProtocolError(f"the resolver answered {status} without a refusal code")
    return ResolverRefused(code, status)


class ResolverClient:
    """Resolves invocation handles over the pod-local resolver socket.

    Every ``resolve`` is one new AF_UNIX connection carrying one request with
    a ``Content-Length`` body; nothing is kept between calls.
    """

    def __init__(self, socket_path: str | None = None, *, timeout: float = 10.0) -> None:
        path = socket_path if socket_path is not None else os.environ.get(SOCKET_ENV, "")
        if not path:
            raise ValueError(f"{SOCKET_ENV} is not set and no socket path was given")
        if not timeout > 0:
            raise ValueError("timeout must be positive")
        self.socket_path = path
        self.timeout = timeout

    def resolve(self, handle: str, slot: str, purpose: str) -> Resolution:
        """One resolve: a ``Resolution`` or ``ResolverRefused``.

        The empty ``slot`` asks for identity-only admission on a
        connector-custody application. ``purpose`` is audit metadata only.
        """

        for value in (handle, slot, purpose):
            if not isinstance(value, str):
                raise TypeError("handle, slot and purpose must be strings")
        body = json.dumps(
            {"handle": handle, "slot": slot, "purpose": purpose},
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        connection = _UnixHTTPConnection(self.socket_path, self.timeout)
        try:
            connection.request(
                "POST",
                RESOLVE_PATH,
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            status = response.status
            payload = response.read(MAX_RESPONSE_BYTES + 1)
        finally:
            connection.close()
        if len(payload) > MAX_RESPONSE_BYTES:
            raise ResolverProtocolError("the resolver answer is too large")
        document = _decode(payload)
        if status == 200:
            return _resolution(document, slot)
        raise _refusal(document, status)


def origin_key(url: str) -> tuple[str, str, int]:
    """(scheme, host, port) of an https URL: lower case, port 443 when absent.

    ``ValueError`` (never naming the URL) when it is not an absolute https URL
    with a host and no user information.
    """

    try:
        parts = urllib.parse.urlsplit(url)
        port = parts.port
    except ValueError:
        raise ValueError("not a valid URL") from None
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    if scheme != _HTTPS:
        raise ValueError("only https origins are accepted")
    if not host:
        raise ValueError("the URL names no host")
    if parts.username is not None or parts.password is not None:
        raise ValueError("the URL carries user information")
    return (scheme, host, _HTTPS_DEFAULT_PORT if port is None else port)


def _declared(resolution: Resolution) -> frozenset[tuple[str, str, int]]:
    keys: set[tuple[str, str, int]] = set()
    for origin in resolution.origins:
        try:
            keys.add(origin_key(origin))
        except ValueError:
            continue
    return frozenset(keys)


class PartitionedCache[V]:
    """Auth or cache state kept per (binding, account, revision) partition.

    Lookups take a ``Resolution`` or a ``ProviderResponse`` (anything with a
    ``partition``), so state built under one credential revision or binding
    is never read under another. Each instance has its own lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[tuple[tuple[str, str, int], Hashable], V] = {}

    def get(self, resolution: _Partitioned, key: Hashable, default: V | None = None) -> V | None:
        with self._lock:
            return self._entries.get((resolution.partition, key), default)

    def put(self, resolution: _Partitioned, key: Hashable, value: V) -> None:
        with self._lock:
            self._entries[(resolution.partition, key)] = value

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


class ProviderResponse(_Immutable):
    """A provider answer and the partition of the credential that obtained it.

    ``body`` is excluded from ``repr``.
    """

    __slots__ = ("status", "headers", "body", "partition")
    status: int
    headers: Mapping[str, str]
    body: bytes
    partition: tuple[str, str, int]

    def __init__(
        self,
        status: int,
        headers: Mapping[str, str],
        body: bytes,
        partition: tuple[str, str, int],
    ) -> None:
        self._set(
            status=status,
            headers=MappingProxyType(dict(headers)),
            body=body,
            partition=partition,
        )

    def __repr__(self) -> str:
        return (
            f"ProviderResponse(status={self.status!r}, partition={self.partition!r}, "
            f"body=<{len(self.body)} bytes>)"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProviderResponse):
            return NotImplemented
        return (self.status, dict(self.headers), self.body, self.partition) == (
            other.status,
            dict(other.headers),
            other.body,
            other.partition,
        )

    def __hash__(self) -> int:
        return hash((self.status, self.body, self.partition))


def bearer(resolution: Resolution, headers: Mapping[str, str]) -> dict[str, str]:
    """``headers`` plus ``Authorization: Bearer <the single text field>``.

    Any caller-supplied Authorization header is replaced.
    """

    if resolution.admission != ADMISSION_CREDENTIAL:
        raise ValueError("bearer needs a credential admission")
    values = list(resolution.fields.values())
    if len(values) != 1 or values[0].text is None:
        raise ValueError("bearer needs exactly one text field")
    token = values[0].text
    if not token or "\t" in token or _HEADER_VALUE.fullmatch(token) is None:
        raise ValueError("the credential field is not a valid bearer token")
    authorized = {name: value for name, value in headers.items() if name.lower() != "authorization"}
    authorized["Authorization"] = f"Bearer {token}"
    return authorized


def _checked_headers(headers: Mapping[str, str], source: str) -> dict[str, str]:
    """``headers`` if every one is sendable as is, else ``ValueError``.

    ``http.client`` rejects a bad value with a message (or a
    ``UnicodeEncodeError``) that quotes the value, Authorization included;
    checking first, with messages that name only ``source`` and the already
    validated header name, keeps the value out of every diagnostic. ``Host``
    is refused in any case: the authority comes only from the checked origin.
    """

    checked: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str) or _HEADER_NAME.fullmatch(name) is None:
            raise ValueError(f"a header from {source} has an invalid name")
        if name.lower() == _HOST_HEADER:
            raise ValueError(f"{source} may not set Host; it comes from the checked origin")
        if not isinstance(value, str) or _HEADER_VALUE.fullmatch(value) is None:
            raise ValueError(
                f"header {name} from {source} is not Latin-1 without control characters"
            )
        checked[name] = value
    return checked


def _authority(host: str, port: int) -> str:
    """The Host value for a checked origin: IPv6 bracketed, port 443 implied."""

    shown = f"[{host}]" if ":" in host else host
    return shown if port == _HTTPS_DEFAULT_PORT else f"{shown}:{port}"


type Authorize = Callable[[Resolution, Mapping[str, str]], dict[str, str]]


class ProviderClient:
    """Provider calls authorized by one fresh resolve each.

    ``request`` checks the caller's headers, resolves the handle, checks the
    URL origin against the origins that resolve returned, authorizes the
    headers, checks them again, and sends over a fresh ``HTTPSConnection``
    closed afterwards, with ``Host`` set from the checked origin. A 3xx
    raises ``RedirectRefused``; a transport failure raises
    ``ProviderTransportError``.
    """

    def __init__(
        self,
        resolver: ResolverClient,
        slot: str,
        *,
        authorize: Authorize = bearer,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 30.0,
    ) -> None:
        if not isinstance(slot, str):
            raise TypeError("slot must be a string")
        if not timeout > 0:
            raise ValueError("timeout must be positive")
        self.resolver = resolver
        self.slot = slot
        self._authorize = authorize
        self._ssl_context = ssl_context if ssl_context is not None else ssl.create_default_context()
        self._timeout = timeout

    def request(
        self,
        handle: str,
        method: str,
        url: str,
        *,
        purpose: str,
        headers: Mapping[str, str] | None = None,
        body: bytes | None = None,
    ) -> ProviderResponse:
        try:
            target = origin_key(url)
            parts = urllib.parse.urlsplit(url)
        except ValueError:
            raise OriginRefused() from None
        path = parts.path or "/"
        if parts.query:
            path = f"{path}?{parts.query}"
        if not isinstance(method, str) or _HEADER_NAME.fullmatch(method) is None:
            raise ValueError("the request method is not an HTTP token")
        if _REQUEST_TARGET.fullmatch(path) is None:
            raise ValueError("the URL path or query is not printable ASCII")
        # Refused before the resolve: a bad caller header never costs an admission.
        caller = _checked_headers(dict(headers or {}), "the caller")

        resolution = self.resolver.resolve(handle, self.slot, purpose)
        if target not in _declared(resolution):
            raise OriginRefused()
        prepared = _checked_headers(self._authorize(resolution, caller), "authorize")

        _, host, port = target
        prepared["Host"] = _authority(host, port)
        connection = http.client.HTTPSConnection(
            host, port, timeout=self._timeout, context=self._ssl_context
        )
        failure: str | None = None
        location: str | None = None
        status = 0
        payload = b""
        received: dict[str, str] = {}
        try:
            try:
                connection.request(method, path, body=body, headers=prepared)
                response = connection.getresponse()
                status = response.status
                if 300 <= status <= 399:
                    location = response.getheader("Location")
                else:
                    payload = response.read()
                    for name, value in response.getheaders():
                        key = name.lower()
                        received[key] = f"{received[key]}, {value}" if key in received else value
            except (OSError, http.client.HTTPException) as exc:
                failure = type(exc).__name__
        finally:
            connection.close()
        if failure is not None:
            # Raised outside the handler and ``from None``: no context or cause
            # carries the original message, which can quote request data.
            raise ProviderTransportError(failure) from None
        if 300 <= status <= 399:
            raise RedirectRefused(status, location)
        return ProviderResponse(
            status=status,
            headers=received,
            body=payload,
            partition=resolution.partition,
        )

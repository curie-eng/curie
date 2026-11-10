"""A bounded Docker Registry v2 client for image retention (#3246, ADR 0176 decision 6).

Teardown deletes a run's images by listing each repository the run recorded,
resolving every tag, and deleting each distinct digest. The repository names
come from namespace data, so every call is fenced:

* A repository must be exactly ``<prefix>/<namespace>/<name>`` on the
  configured registry host, judged with the distribution name grammar.
* Credentials go to the registry host, or to a Bearer token realm on the
  registry host or on a host listed in ``E2E_REGISTRY_TOKEN_HOSTS``.
* Redirects are never followed.
* Every transport, timeout or parse failure becomes a ``RegistryError`` that
  names only the operation, host, repository and status. No header, token,
  config text or response body is ever carried.

API: https://distribution.github.io/distribution/spec/api/
Token auth: https://distribution.github.io/distribution/spec/auth/token/
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import httpx

from curie_e2e_connector.contract import REFUSAL_MISCONFIGURED, REFUSAL_REGISTRY_DELETE
from curie_e2e_connector.kube import ClusterError

# distribution reference grammar: one path component. The grammar's `-*`
# separator is written `-+`: the same language, without exponential backtracking.
NAME_COMPONENT = re.compile(r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$")
_HOST = re.compile(r"^[A-Za-z0-9.-]+(:[0-9]{1,5})?$")
_TAG = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
# A run namespace is always the connector prefix plus the run uuid.
_RUN_NAMESPACE = re.compile(
    r"^[a-z0-9-]*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_LINK_NEXT = re.compile(r'<([^>]*)>\s*;\s*rel="?next"?')
_CHALLENGE_PARAM = re.compile(r'([A-Za-z_]+)="([^"]*)"')
_MAX_TAG_PAGES = 50
_MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)


_T = TypeVar("_T")
# The statuses a registry answers when the credential presented is not accepted.
_AUTH_REFUSALS = frozenset({401, 403})


class RegistryError(ClusterError):
    """A registry call failed. The message never includes a credential or a body.

    ``status`` is the HTTP status the registry answered, when one did.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class RegistrySettings:
    # "<host>[:port][/path...]", or "" when no registry is configured.
    prefix: str
    insecure: bool
    token_hosts: tuple[str, ...]

    def __post_init__(self) -> None:
        # The one normalization every reader of the prefix shares: surrounding
        # whitespace and a trailing "/" are not part of the repository path.
        object.__setattr__(self, "prefix", self.prefix.strip().rstrip("/"))


def valid_host(value: str) -> bool:
    """``host[:port]`` with a port in range. An IPv6 literal is not accepted."""

    if not _HOST.fullmatch(value):
        return False
    _host, _sep, port = value.partition(":")
    return not port or 0 < int(port) <= 65535


def host_port(host: str, *, insecure: bool) -> int:
    """The TCP port a registry host is reached on."""

    _name, sep, port = host.partition(":")
    if sep:
        return int(port)
    return 80 if insecure else 443


def split_repository(name: str) -> tuple[str, tuple[str, ...]]:
    """Return ``(host, path components)``; host is "" when the name has none.

    Raises ``RegistryError`` when any part breaks the distribution grammar.
    """

    segments = name.split("/")
    host = ""
    if len(segments) > 1 and (
        "." in segments[0] or ":" in segments[0] or segments[0] == "localhost"
    ):
        host = segments.pop(0)
        if not valid_host(host):
            raise RegistryError("repository host is not a valid registry host")
    if not segments or not all(NAME_COMPONENT.fullmatch(part) for part in segments):
        raise RegistryError("repository name breaks the distribution grammar")
    return host, tuple(segments)


def _prefix_parts(settings: RegistrySettings) -> tuple[str, tuple[str, ...]] | None:
    if not settings.prefix:
        return None
    segments = settings.prefix.split("/")
    host = segments[0]
    if not valid_host(host) or not ("." in host or ":" in host or host == "localhost"):
        return None
    path = tuple(segments[1:])
    if not all(NAME_COMPONENT.fullmatch(part) for part in path):
        return None
    return host, path


def validate_prefix(prefix: str) -> None:
    """Raise unless ``prefix`` is ``<registry host>[/<component>...]``."""

    if _prefix_parts(RegistrySettings(prefix=prefix, insecure=False, token_hosts=())) is None:
        raise RegistryError("registry prefix must be <host>[:port][/path] in the grammar")


def owns_repository(settings: RegistrySettings, namespace: str, repo: str) -> bool:
    """True only for ``<prefix>/<namespace>/<one component>`` on the prefix host."""

    parts = _prefix_parts(settings)
    if parts is None:
        return False
    try:
        host, components = split_repository(repo)
    except RegistryError:
        return False
    prefix_host, prefix_path = parts
    return (
        host == prefix_host
        and len(components) == len(prefix_path) + 2
        and components[: len(prefix_path)] == prefix_path
        and components[-2] == namespace
        and NAME_COMPONENT.fullmatch(namespace) is not None
    )


def namespace_repository(settings: RegistrySettings, namespace: str, name: str) -> str:
    """Build ``<prefix>/<namespace>/<name>`` and re-validate it."""

    repo = f"{settings.prefix}/{namespace}/{name}"
    if not owns_repository(settings, namespace, repo):
        raise RegistryError("the image name does not form a repository under the registry prefix")
    return repo


def _registry_path(settings: RegistrySettings, repo: str) -> tuple[str, str]:
    """``(host, path)`` for a repository under some run namespace of the prefix.

    The client is not told the namespace, so it requires the shape every run
    namespace has. ``retention`` also checks the exact namespace before calling.
    """

    parts = _prefix_parts(settings)
    if parts is None:
        raise RegistryError("registry is not configured")
    try:
        host, components = split_repository(repo)
    except RegistryError:
        raise RegistryError("repository is outside the registry prefix") from None
    prefix_host, prefix_path = parts
    if (
        host != prefix_host
        or len(components) != len(prefix_path) + 2
        or components[: len(prefix_path)] != prefix_path
        or _RUN_NAMESPACE.fullmatch(components[-2]) is None
        or len(components[-2]) > 63
    ):
        raise RegistryError("repository is outside the registry prefix")
    return host, "/".join(components)


def _config_refused(reason: str) -> RegistryError:
    return RegistryError(f"{REFUSAL_MISCONFIGURED}: {reason}")


def parse_docker_config(text: str | None) -> dict[str, tuple[str, str]]:
    """Map a registry host to ``(username, password)`` from a docker config.json.

    Only basic credentials are supported. An ``identitytoken`` or
    ``registrytoken`` entry, or an ``auth`` that is not base64 ``user:password``,
    is refused as misconfigured rather than read as anonymous, because crane
    would push with it while retention could not delete what it pushed.
    """

    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except ValueError:
        raise _config_refused("registry config is not JSON") from None
    if not isinstance(parsed, dict):
        raise _config_refused("registry config is not a JSON object")
    auths = parsed.get("auths") or {}
    if not isinstance(auths, dict):
        raise _config_refused("registry config auths is not an object")
    found: dict[str, tuple[str, str]] = {}
    for key, entry in auths.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            raise _config_refused("registry config has a malformed auths entry")
        host = _normalize_auth_key(key)
        for token_key in ("identitytoken", "registrytoken"):
            if entry.get(token_key):
                raise _config_refused(
                    f"registry config uses {token_key}; only user:password credentials "
                    "are supported"
                )
        auth = entry.get("auth")
        if auth is not None and not isinstance(auth, str):
            raise _config_refused("registry config auth is not base64 user:password")
        if auth:
            try:
                decoded = base64.b64decode(auth, validate=True).decode("utf-8")
            except (binascii.Error, ValueError):
                raise _config_refused("registry config auth is not base64 user:password") from None
            user, sep, password = decoded.partition(":")
            if not sep:
                raise _config_refused("registry config auth is not base64 user:password")
            found[host] = (user, password)
            continue
        user_value = entry.get("username")
        password_value = entry.get("password")
        if isinstance(user_value, str) and isinstance(password_value, str):
            found[host] = (user_value, password_value)
    return found


def _normalize_auth_key(key: str) -> str:
    bare = key.split("://", 1)[1] if "://" in key else key
    return bare.split("/", 1)[0]


def registry_client(timeout: float) -> httpx.Client:
    """The one factory for registry HTTP clients. Redirects are never followed."""

    return httpx.Client(timeout=timeout, follow_redirects=False)


class RegistryApi:
    """The three registry calls image retention makes."""

    settings: RegistrySettings

    def list_tags(self, repo: str) -> list[str]:
        raise NotImplementedError

    def resolve(self, repo: str, tag: str) -> str | None:
        raise NotImplementedError

    def delete_manifest(self, repo: str, digest: str) -> None:
        raise NotImplementedError


class DockerRegistry(RegistryApi):
    def __init__(
        self,
        auths: dict[str, tuple[str, str]],
        *,
        settings: RegistrySettings,
        client: httpx.Client,
    ) -> None:
        self.settings = settings
        self._auths = dict(auths)
        self._client = client

    # Public calls

    def list_tags(self, repo: str) -> list[str]:
        host, path = _registry_path(self.settings, repo)
        base = httpx.URL(f"{self._scheme}://{host}/v2/{path}/tags/list")
        url = base
        tags: list[str] = []
        for _page in range(_MAX_TAG_PAGES):
            response = self._send("GET", url, host, path, "tag list")
            if response.status_code == 404:
                return tags
            if response.status_code != 200:
                raise RegistryError(
                    f"tag list on {host} for {repo} answered {response.status_code}",
                    status=response.status_code,
                )
            tags.extend(self._page_tags(response, host, repo))
            following = self._next_page(response, base)
            if following is None:
                return tags
            url = following
        raise RegistryError(f"tag list on {host} for {repo} exceeded {_MAX_TAG_PAGES} pages")

    def resolve(self, repo: str, tag: str) -> str | None:
        host, path = _registry_path(self.settings, repo)
        if not _TAG.fullmatch(tag):
            raise RegistryError(f"tag for {repo} is not a valid tag")
        url = httpx.URL(f"{self._scheme}://{host}/v2/{path}/manifests/{tag}")
        response = self._send("HEAD", url, host, path, "manifest read", accept=_MANIFEST_ACCEPT)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise RegistryError(
                f"manifest read on {host} for {repo} answered {response.status_code}",
                status=response.status_code,
            )
        digest = str(response.headers.get("Docker-Content-Digest", ""))
        if not DIGEST.fullmatch(digest):
            raise RegistryError(f"manifest read on {host} for {repo} returned no sha256 digest")
        return digest

    def delete_manifest(self, repo: str, digest: str) -> None:
        host, path = _registry_path(self.settings, repo)
        if not DIGEST.fullmatch(digest):
            raise RegistryError(f"digest for {repo} is not a sha256 digest")
        url = httpx.URL(f"{self._scheme}://{host}/v2/{path}/manifests/{digest}")
        response = self._send("DELETE", url, host, path, "manifest delete")
        status = response.status_code
        if status in (200, 202, 404):
            return
        if status == 405:
            raise RegistryError(
                f"{REFUSAL_REGISTRY_DELETE}: {host} {repo}: registry does not allow deletes (405)"
            )
        raise RegistryError(
            f"{REFUSAL_REGISTRY_DELETE}: {host} {repo}: delete answered {status}", status=status
        )

    # Internals

    @property
    def _scheme(self) -> str:
        return "http" if self.settings.insecure else "https"

    def _page_tags(self, response: httpx.Response, host: str, repo: str) -> list[str]:
        try:
            payload = response.json()
        except ValueError:
            raise RegistryError(f"tag list on {host} for {repo} is not JSON") from None
        if not isinstance(payload, dict):
            raise RegistryError(f"tag list on {host} for {repo} is not an object")
        tags = payload.get("tags")
        if tags is None:
            return []
        if not isinstance(tags, list):
            raise RegistryError(f"tag list on {host} for {repo} has no tag array")
        for tag in tags:
            if not isinstance(tag, str) or not _TAG.fullmatch(tag):
                raise RegistryError(f"tag list on {host} for {repo} has an invalid tag")
        return list(tags)

    def _next_page(self, response: httpx.Response, base: httpx.URL) -> httpx.URL | None:
        link = response.headers.get("Link")
        if not link:
            return None
        match = _LINK_NEXT.search(link)
        if match is None:
            return None
        try:
            following = base.join(match.group(1))
        except (httpx.InvalidURL, ValueError):
            raise RegistryError(f"tag list on {base.host} has an unreadable next link") from None
        if (
            following.scheme != base.scheme
            or following.host != base.host
            or following.port != base.port
            or following.path != base.path
        ):
            raise RegistryError(f"tag list on {base.host} paginated outside the repository")
        return following

    def _send(
        self,
        method: str,
        url: httpx.URL,
        host: str,
        path: str,
        operation: str,
        *,
        accept: str | None = None,
    ) -> httpx.Response:
        headers: dict[str, str] = {}
        if accept:
            headers["Accept"] = accept
        credentials = self._auths.get(host)
        if credentials is not None:
            headers["Authorization"] = _basic(credentials)
        response = self._transport(method, url, headers, host, operation)
        challenge = response.headers.get("WWW-Authenticate", "")
        if response.status_code != 401 or not challenge.lower().startswith("bearer "):
            return response
        token = self._token(challenge, host, path, credentials, operation)
        headers["Authorization"] = f"Bearer {token}"
        return self._transport(method, url, headers, host, operation)

    def _transport(
        self,
        method: str,
        url: httpx.URL,
        headers: dict[str, str],
        host: str,
        operation: str,
    ) -> httpx.Response:
        try:
            return self._client.request(method, url, headers=headers)
        except httpx.TimeoutException:
            raise RegistryError(f"{operation} on {host} timed out") from None
        except httpx.HTTPError:
            raise RegistryError(f"{operation} on {host} is unreachable") from None

    def _token(
        self,
        challenge: str,
        host: str,
        path: str,
        credentials: tuple[str, str] | None,
        operation: str,
    ) -> str:
        params = dict(_CHALLENGE_PARAM.findall(challenge[len("bearer ") :]))
        realm = self._allowed_realm(params.get("realm", ""), host)
        query: dict[str, str] = {}
        if params.get("service"):
            query["service"] = params["service"]
        query["scope"] = params.get("scope") or f"repository:{path}:pull,delete"
        headers: dict[str, str] = {}
        if credentials is not None:
            headers["Authorization"] = _basic(credentials)
        response = self._transport(
            "GET", realm.copy_merge_params(query), headers, host, f"{operation} token"
        )
        if response.status_code != 200:
            raise RegistryError(
                f"{operation} token request for {host} answered {response.status_code}",
                status=response.status_code,
            )
        try:
            payload: Any = response.json()
            token = payload.get("token") or payload.get("access_token")
        except (ValueError, AttributeError, TypeError):
            raise RegistryError(f"{operation} token response for {host} is not JSON") from None
        if not isinstance(token, str) or not token:
            raise RegistryError(f"{operation} token response for {host} carries no token")
        return token

    def _allowed_realm(self, realm: str, host: str) -> httpx.URL:
        refused = RegistryError(f"token realm host not allowed for {host}")
        try:
            url = httpx.URL(realm)
        except (httpx.InvalidURL, ValueError):
            raise refused from None
        if url.scheme != "https" or url.userinfo or not url.host:
            raise refused
        port = url.port
        realm_host = url.host if port in (None, 443) else f"{url.host}:{port}"
        allowed = {
            _strip_https_port(host),
            *(_strip_https_port(entry) for entry in self.settings.token_hosts),
        }
        if realm_host not in allowed:
            raise refused
        return url


class CompositeRegistry(RegistryApi):
    """Several credentials for one registry, tried in order.

    Agents sharing a test cluster can push with different credentials, and the
    cluster is swept once. Each call goes to the first registry; only an auth
    refusal (401 or 403) moves it on to the next. Any other error is raised
    as is, so a credential never masks a real failure. When every credential
    is refused, the last refusal is raised.
    """

    def __init__(self, registries: Sequence[RegistryApi]) -> None:
        if not registries:
            raise ValueError("a composite registry needs at least one registry")
        self._registries = tuple(registries)
        self.settings = self._registries[0].settings

    def list_tags(self, repo: str) -> list[str]:
        return self._first(lambda registry: registry.list_tags(repo))

    def resolve(self, repo: str, tag: str) -> str | None:
        return self._first(lambda registry: registry.resolve(repo, tag))

    def delete_manifest(self, repo: str, digest: str) -> None:
        self._first(lambda registry: registry.delete_manifest(repo, digest))

    def _first(self, call: Callable[[RegistryApi], _T]) -> _T:
        refused: RegistryError | None = None
        for registry in self._registries:
            try:
                return call(registry)
            except RegistryError as exc:
                if exc.status not in _AUTH_REFUSALS:
                    raise
                refused = exc
        assert refused is not None
        raise refused


def _strip_https_port(entry: str) -> str:
    return entry[: -len(":443")] if entry.endswith(":443") else entry


def _basic(credentials: tuple[str, str]) -> str:
    user, password = credentials
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode("ascii")

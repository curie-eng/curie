"""The bounded registry client against a fake distribution registry (#3246, ADR 0176 decision 6).

The fake is an ``httpx.MockTransport`` shaped like the Docker Registry HTTP API
v2 as implemented by distribution:

* API: https://distribution.github.io/distribution/spec/api/
  (tags/list ``{"name", "tags"}`` with ``Link`` pagination, HEAD with
  ``Docker-Content-Digest``, DELETE answering 202, the ``errors`` body with
  codes ``NAME_UNKNOWN``, ``MANIFEST_UNKNOWN`` and ``UNSUPPORTED``).
* Token auth: https://distribution.github.io/distribution/spec/auth/token/
  (401 with ``WWW-Authenticate: Bearer realm=...,service=...,scope=...``, the
  token endpoint answering ``{"token": ...}`` or ``{"access_token": ...}``).
"""

from __future__ import annotations

import base64
import contextlib
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from curie_e2e_connector.kube import ClusterError
from curie_e2e_connector.registry import (
    DockerRegistry,
    RegistryError,
    RegistrySettings,
    namespace_repository,
    owns_repository,
    parse_docker_config,
    registry_client,
    split_repository,
)

HOST = "registry.test.example"
PREFIX = f"{HOST}/e2e"
NS = "curie-e2e-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
REPO = f"{PREFIX}/{NS}/app"
PATH = f"e2e/{NS}/app"
USER = "pusher"
PASSWORD = "push-pass-SENTINEL-9f3"
TOKEN = "registry-token-SENTINEL-77a"
DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32

ACCEPT_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}


def settings(*, insecure: bool = False, token_hosts: tuple[str, ...] = ()) -> RegistrySettings:
    return RegistrySettings(prefix=PREFIX, insecure=insecure, token_hosts=token_hosts)


def errors_body(code: str, message: str) -> dict[str, Any]:
    # distribution error envelope: {"errors": [{"code", "message", "detail"}]}
    return {"errors": [{"code": code, "message": message, "detail": {}}]}


class Recorder:
    """Records every request the client sends, then answers with ``handler``."""

    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self.handler = handler
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.handler(request)

    def hosts(self) -> set[str]:
        return {request.url.host for request in self.requests}


def make(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    auths: dict[str, tuple[str, str]] | None = None,
    insecure: bool = False,
    token_hosts: tuple[str, ...] = (),
) -> tuple[DockerRegistry, Recorder]:
    recorder = Recorder(handler)
    client = httpx.Client(transport=httpx.MockTransport(recorder), follow_redirects=False)
    registry = DockerRegistry(
        {HOST: (USER, PASSWORD)} if auths is None else auths,
        settings=settings(insecure=insecure, token_hosts=token_hosts),
        client=client,
    )
    return registry, recorder


def basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def assert_no_secret(exc: BaseException) -> None:
    text = str(exc)
    assert PASSWORD not in text
    assert TOKEN not in text
    assert base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode() not in text


# Repository grammar


def test_split_repository_separates_host_and_components() -> None:
    host, components = split_repository(REPO)
    assert host == HOST
    assert list(components) == ["e2e", NS, "app"]
    host, components = split_repository("localhost/e2e/app")
    assert host == "localhost"
    assert list(components) == ["e2e", "app"]
    host, components = split_repository("registry.test.example:5000/e2e/app")
    assert host == "registry.test.example:5000"


def test_namespace_repository_builds_prefix_namespace_name() -> None:
    assert namespace_repository(settings(), NS, "app") == REPO


@pytest.mark.parametrize("name", ["App", "../app", "a/b", "", "app?x"])
def test_namespace_repository_refuses_a_bad_name(name: str) -> None:
    with pytest.raises(ClusterError):
        namespace_repository(settings(), NS, name)


REFUSED_REPOSITORIES = [
    pytest.param(f"{PREFIX}/{NS}/../../victim/app", id="traversal"),
    pytest.param(f"{PREFIX}/{NS}/%2e%2e", id="percent"),
    pytest.param(f"{PREFIX}/{NS}/app?x", id="query"),
    pytest.param(f"{PREFIX}/{NS}/app#x", id="fragment"),
    pytest.param(f"{PREFIX}/{NS}/App", id="uppercase"),
    pytest.param(f"{PREFIX}/{NS}/app/extra", id="extra-level"),
    pytest.param(f"other.example/e2e/{NS}/app", id="other-host"),
    pytest.param(f"{HOST}:5000/e2e/{NS}/app", id="other-port"),
    pytest.param(f"{PREFIX}/curie-e2e-someone-else/app", id="other-namespace"),
    pytest.param(f"{PREFIX}/{NS}//app", id="empty-component"),
    pytest.param(f"{PREFIX}/{NS}", id="no-name"),
    pytest.param(f"{HOST}/{NS}/app", id="missing-prefix-path"),
]


@pytest.mark.parametrize("repo", REFUSED_REPOSITORIES)
def test_owns_repository_refuses_anything_outside_prefix_namespace_name(repo: str) -> None:
    assert owns_repository(settings(), NS, repo) is False


def test_owns_repository_accepts_the_namespace_repository() -> None:
    assert owns_repository(settings(), NS, REPO) is True
    assert owns_repository(settings(), NS, f"{PREFIX}/{NS}/my_app-2.web") is True


@pytest.mark.parametrize("repo", REFUSED_REPOSITORIES)
def test_a_refused_repository_sends_no_request(repo: str) -> None:
    registry, recorder = make(lambda request: httpx.Response(200, json={"tags": []}))
    with pytest.raises(RegistryError):
        registry.list_tags(repo)
    with pytest.raises(RegistryError):
        registry.resolve(repo, "build-0a1b2c3d")
    with pytest.raises(RegistryError):
        registry.delete_manifest(repo, DIGEST)
    assert recorder.requests == []


# Docker config parsing


def test_parse_docker_config_reads_auth_and_username_password() -> None:
    text = json.dumps(
        {
            "auths": {
                "https://registry.test.example/v1/": {
                    "auth": base64.b64encode(b"pusher:pw:with:colons").decode()
                },
                "cache.test.example": {"username": "cacher", "password": "cache-pw"},
            }
        }
    )
    assert parse_docker_config(text) == {
        "registry.test.example": ("pusher", "pw:with:colons"),
        "cache.test.example": ("cacher", "cache-pw"),
    }


@pytest.mark.parametrize("text", [None, ""])
def test_parse_docker_config_empty_is_no_credentials(text: str | None) -> None:
    assert parse_docker_config(text) == {}


def test_parse_docker_config_refuses_non_json_without_echoing_it() -> None:
    with pytest.raises(RegistryError, match="registry config is not JSON") as excinfo:
        parse_docker_config(f'{{"auths": {{"x": "{PASSWORD}"')
    assert_no_secret(excinfo.value)


def test_registry_error_is_a_cluster_error() -> None:
    assert issubclass(RegistryError, ClusterError)


def test_registry_client_never_follows_redirects() -> None:
    client = registry_client(7.0)
    try:
        assert isinstance(client, httpx.Client)
        assert client.follow_redirects is False
        assert client.timeout.read == 7.0
    finally:
        client.close()


# Basic auth


def test_basic_auth_comes_from_the_config() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"name": PATH, "tags": ["build-0a1b2c3d"]})

    registry, recorder = make(handler)
    assert registry.list_tags(REPO) == ["build-0a1b2c3d"]
    [request] = recorder.requests
    assert request.url == httpx.URL(f"https://{HOST}/v2/{PATH}/tags/list")
    assert request.headers["Authorization"] == basic(USER, PASSWORD)


def test_no_credentials_sends_no_authorization() -> None:
    registry, recorder = make(
        lambda request: httpx.Response(200, json={"name": PATH, "tags": []}), auths={}
    )
    assert registry.list_tags(REPO) == []
    assert "Authorization" not in recorder.requests[0].headers


def test_insecure_uses_plain_http() -> None:
    registry, recorder = make(
        lambda request: httpx.Response(200, json={"name": PATH, "tags": []}), insecure=True
    )
    registry.list_tags(REPO)
    assert recorder.requests[0].url.scheme == "http"


# Bearer token flow


def bearer_handler(
    realm: str,
    *,
    token_key: str = "token",
    scope: str = f"repository:{PATH}:pull",
    token_response: Callable[[httpx.Request], httpx.Response] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == HOST and request.url.path.startswith("/v2/"):
            if request.headers.get("Authorization") == f"Bearer {TOKEN}":
                return httpx.Response(200, json={"name": PATH, "tags": ["build-0a1b2c3d"]})
            return httpx.Response(
                401,
                headers={
                    "WWW-Authenticate": (f'Bearer realm="{realm}",service="{HOST}",scope="{scope}"')
                },
                json=errors_body("UNAUTHORIZED", "authentication required"),
            )
        if token_response is not None:
            return token_response(request)
        return httpx.Response(200, json={token_key: TOKEN, "expires_in": 300})

    return handler


@pytest.mark.parametrize("token_key", ["token", "access_token"])
def test_bearer_flow_against_a_separate_allowlisted_token_host(token_key: str) -> None:
    scope = f"repository:{PATH}:pull"
    registry, recorder = make(
        bearer_handler("https://auth.test.example/token", token_key=token_key, scope=scope),
        token_hosts=("auth.test.example",),
    )

    assert registry.list_tags(REPO) == ["build-0a1b2c3d"]

    token_requests = [r for r in recorder.requests if r.url.host == "auth.test.example"]
    assert len(token_requests) == 1
    token_request = token_requests[0]
    assert token_request.method == "GET"
    assert token_request.url.scheme == "https"
    assert token_request.url.path == "/token"
    # The challenge scope is used verbatim.
    assert token_request.url.params["scope"] == scope
    assert token_request.url.params["service"] == HOST
    assert token_request.headers["Authorization"] == basic(USER, PASSWORD)
    retry = recorder.requests[-1]
    assert retry.url.host == HOST
    assert retry.headers["Authorization"] == f"Bearer {TOKEN}"


def test_bearer_realm_on_the_registry_host_itself_is_allowed() -> None:
    registry, recorder = make(bearer_handler(f"https://{HOST}/token"))
    assert registry.list_tags(REPO) == ["build-0a1b2c3d"]
    assert any(r.url.path == "/token" for r in recorder.requests)


def test_bearer_without_credentials_requests_an_anonymous_token() -> None:
    registry, recorder = make(bearer_handler(f"https://{HOST}/token"), auths={})
    assert registry.list_tags(REPO) == ["build-0a1b2c3d"]
    token_request = next(r for r in recorder.requests if r.url.path == "/token")
    assert "Authorization" not in token_request.headers


def test_allowlisted_token_host_with_an_explicit_port() -> None:
    registry, _recorder = make(
        bearer_handler("https://auth.test.example:8443/token"),
        token_hosts=("auth.test.example:8443",),
    )
    assert registry.list_tags(REPO) == ["build-0a1b2c3d"]


@pytest.mark.parametrize(
    ("realm", "token_hosts"),
    [
        pytest.param("https://evil.example/token", ("auth.test.example",), id="other-host"),
        pytest.param("http://auth.test.example/token", ("auth.test.example",), id="http"),
        pytest.param(
            "https://user:pw@auth.test.example/token", ("auth.test.example",), id="userinfo"
        ),
        pytest.param(
            "https://auth.test.example:8443/token",
            ("auth.test.example",),
            id="portless-entry-matches-443-only",
        ),
        pytest.param("https://auth.test.example/token", (), id="not-allowlisted"),
        pytest.param(f"http://{HOST}/token", (), id="registry-host-over-http"),
    ],
)
def test_a_disallowed_token_realm_is_refused_without_sending_a_credential(
    realm: str, token_hosts: tuple[str, ...]
) -> None:
    registry, recorder = make(bearer_handler(realm), token_hosts=token_hosts)
    with pytest.raises(RegistryError, match="token realm host not allowed") as excinfo:
        registry.list_tags(REPO)
    assert_no_secret(excinfo.value)
    # The only requests went to the registry itself: no token request at all.
    assert {r.url.host for r in recorder.requests} == {HOST}
    assert all(r.url.path.startswith("/v2/") for r in recorder.requests)


def test_a_redirect_from_the_token_realm_is_not_followed() -> None:
    def token_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "https://elsewhere.example/steal"})

    registry, recorder = make(
        bearer_handler("https://auth.test.example/token", token_response=token_response),
        token_hosts=("auth.test.example",),
    )
    with pytest.raises(RegistryError) as excinfo:
        registry.list_tags(REPO)
    assert_no_secret(excinfo.value)
    assert "elsewhere.example" not in recorder.hosts()


def test_a_redirect_from_the_registry_is_not_followed() -> None:
    registry, recorder = make(
        lambda request: httpx.Response(
            307, headers={"Location": f"https://elsewhere.example/v2/{PATH}/tags/list"}
        )
    )
    with pytest.raises(RegistryError):
        registry.list_tags(REPO)
    assert recorder.hosts() == {HOST}


def test_a_non_json_token_body_is_a_sanitized_registry_error() -> None:
    def token_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=f"<html>{TOKEN} {PASSWORD}</html>")

    registry, _recorder = make(
        bearer_handler("https://auth.test.example/token", token_response=token_response),
        token_hosts=("auth.test.example",),
    )
    with pytest.raises(RegistryError) as excinfo:
        registry.list_tags(REPO)
    assert_no_secret(excinfo.value)


# Listing


def test_tags_list_pagination_follows_link_on_the_same_repository() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("last") == "build-b":
            return httpx.Response(200, json={"name": PATH, "tags": ["staging-c"]})
        return httpx.Response(
            200,
            headers={"Link": f'</v2/{PATH}/tags/list?n=2&last=build-b>; rel="next"'},
            json={"name": PATH, "tags": ["build-a", "build-b"]},
        )

    registry, recorder = make(handler)
    assert registry.list_tags(REPO) == ["build-a", "build-b", "staging-c"]
    assert [r.url.path for r in recorder.requests] == [f"/v2/{PATH}/tags/list"] * 2
    assert recorder.hosts() == {HOST}


@pytest.mark.parametrize(
    "link",
    [
        pytest.param("</v2/e2e/victim/app/tags/list?last=b>", id="other-repository"),
        pytest.param(f"<https://evil.example/v2/{PATH}/tags/list?last=b>", id="other-host"),
        pytest.param(f"</v2/{PATH}/manifests/latest>", id="other-endpoint"),
    ],
)
def test_tags_list_pagination_never_leaves_the_repository(link: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Link": f'{link}; rel="next"'},
            json={"name": PATH, "tags": ["build-a"]},
        )

    registry, recorder = make(handler)
    with contextlib.suppress(RegistryError):
        registry.list_tags(REPO)
    assert recorder.hosts() == {HOST}
    assert all(r.url.path == f"/v2/{PATH}/tags/list" for r in recorder.requests)
    assert len(recorder.requests) == 1


def test_tags_list_pagination_is_bounded() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        last = request.url.params.get("last", "0")
        following = str(int(last) + 1)
        return httpx.Response(
            200,
            headers={"Link": f'</v2/{PATH}/tags/list?last={following}>; rel="next"'},
            json={"name": PATH, "tags": [f"build-{following}"]},
        )

    registry, recorder = make(handler)
    with contextlib.suppress(RegistryError):
        registry.list_tags(REPO)
    assert len(recorder.requests) <= 50


def test_tags_list_name_unknown_is_empty() -> None:
    registry, _recorder = make(
        lambda request: httpx.Response(
            404, json=errors_body("NAME_UNKNOWN", "repository name not known to registry")
        )
    )
    assert registry.list_tags(REPO) == []


def test_tags_list_null_tags_is_empty() -> None:
    # distribution answers "tags": null for a repository whose tags were all deleted.
    registry, _recorder = make(
        lambda request: httpx.Response(200, json={"name": PATH, "tags": None})
    )
    assert registry.list_tags(REPO) == []


@pytest.mark.parametrize("tag", ["bad tag", "../x", "-leading", "a" * 129, 7])
def test_tags_list_refuses_an_invalid_tag(tag: Any) -> None:
    registry, _recorder = make(
        lambda request: httpx.Response(200, json={"name": PATH, "tags": ["build-a", tag]})
    )
    with pytest.raises(RegistryError):
        registry.list_tags(REPO)


# Resolve


def test_resolve_heads_the_manifest_with_every_accept_type() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "Docker-Content-Digest": DIGEST,
                "Content-Type": "application/vnd.oci.image.manifest.v1+json",
            },
        )

    registry, recorder = make(handler)
    assert registry.resolve(REPO, "build-0a1b2c3d") == DIGEST
    [request] = recorder.requests
    assert request.method == "HEAD"
    assert request.url.path == f"/v2/{PATH}/manifests/build-0a1b2c3d"
    accepted = {item.strip() for item in request.headers["Accept"].split(",")}
    assert ACCEPT_TYPES <= accepted


def test_resolve_manifest_unknown_is_none() -> None:
    # A HEAD response has no body; the 404 status alone means MANIFEST_UNKNOWN.
    registry, _recorder = make(lambda request: httpx.Response(404))
    assert registry.resolve(REPO, "build-gone") is None


@pytest.mark.parametrize("digest", ["sha256:XYZ", "sha512:" + "ab" * 64, ""])
def test_resolve_refuses_a_malformed_digest_header(digest: str) -> None:
    headers = {"Docker-Content-Digest": digest} if digest else {}
    registry, _recorder = make(lambda request: httpx.Response(200, headers=headers))
    with pytest.raises(RegistryError):
        registry.resolve(REPO, "build-0a1b2c3d")


# Delete


@pytest.mark.parametrize("status", [200, 202])
def test_delete_manifest_accepts_success(status: int) -> None:
    registry, recorder = make(lambda request: httpx.Response(status))
    registry.delete_manifest(REPO, DIGEST)
    [request] = recorder.requests
    assert request.method == "DELETE"
    assert request.url == httpx.URL(f"https://{HOST}/v2/{PATH}/manifests/{DIGEST}")


def test_delete_manifest_unknown_is_done() -> None:
    registry, _recorder = make(
        lambda request: httpx.Response(
            404, json=errors_body("MANIFEST_UNKNOWN", "manifest unknown")
        )
    )
    registry.delete_manifest(REPO, OTHER_DIGEST)


def test_delete_manifest_405_raises_unsupported() -> None:
    registry, _recorder = make(
        lambda request: httpx.Response(
            405, json=errors_body("UNSUPPORTED", "The operation is unsupported.")
        )
    )
    with pytest.raises(RegistryError) as excinfo:
        registry.delete_manifest(REPO, DIGEST)
    message = str(excinfo.value)
    assert "405" in message
    assert "e2e_registry_delete_refused" in message
    assert HOST in message


def test_delete_manifest_other_status_carries_no_body() -> None:
    registry, _recorder = make(
        lambda request: httpx.Response(500, text=f"internal SECRET-BODY-TEXT {PASSWORD}")
    )
    with pytest.raises(RegistryError) as excinfo:
        registry.delete_manifest(REPO, DIGEST)
    message = str(excinfo.value)
    assert "500" in message
    assert "SECRET-BODY-TEXT" not in message
    assert_no_secret(excinfo.value)


# Wrapped transport errors


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError(f"connect failed for {USER}:{PASSWORD}"),
        httpx.ReadTimeout(f"read timed out {PASSWORD}"),
    ],
    ids=["connect", "timeout"],
)
def test_transport_errors_become_sanitized_registry_errors(error: httpx.HTTPError) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    registry, _recorder = make(handler)
    for call in (
        lambda: registry.list_tags(REPO),
        lambda: registry.resolve(REPO, "build-a"),
        lambda: registry.delete_manifest(REPO, DIGEST),
    ):
        with pytest.raises(RegistryError) as excinfo:
            call()
        assert_no_secret(excinfo.value)


def test_a_non_json_tags_body_is_a_registry_error() -> None:
    registry, _recorder = make(lambda request: httpx.Response(200, text="not json"))
    with pytest.raises(RegistryError):
        registry.list_tags(REPO)


# Unsupported credential forms (review 1, finding 2)


def docker_config(entry: dict[str, Any]) -> str:
    return json.dumps({"auths": {HOST: entry}})


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param({"identitytoken": "idtok-SENTINEL"}, id="identitytoken"),
        pytest.param({"registrytoken": "regtok-SENTINEL"}, id="registrytoken"),
        pytest.param({"auth": "", "identitytoken": "idtok-SENTINEL"}, id="empty-auth-identity"),
        pytest.param({"auth": "!!not-base64!!"}, id="auth-not-base64"),
        pytest.param(
            {"auth": base64.b64encode(b"no-colon-here").decode()}, id="auth-without-colon"
        ),
    ],
)
def test_parse_docker_config_refuses_unsupported_credential_forms_as_misconfigured(
    entry: dict[str, Any],
) -> None:
    with pytest.raises(ClusterError, match=r"^e2e_connector_misconfigured") as excinfo:
        parse_docker_config(docker_config(entry))
    assert "SENTINEL" not in str(excinfo.value)
    assert "no-colon-here" not in str(excinfo.value)


def test_parse_docker_config_still_accepts_a_normal_auth_entry_and_an_anonymous_config() -> None:
    auth = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    assert parse_docker_config(docker_config({"auth": auth})) == {HOST: (USER, PASSWORD)}
    assert parse_docker_config("{}") == {}
    assert parse_docker_config(json.dumps({"auths": {}})) == {}


# Registry settings normalization (review 1, finding 3)


def test_registry_settings_strip_a_trailing_slash_from_the_prefix() -> None:
    normalized = RegistrySettings(
        prefix="registry.example.com/e2e/", insecure=False, token_hosts=()
    )
    assert normalized.prefix == "registry.example.com/e2e"
    namespace = "curie-e2e-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    assert owns_repository(normalized, namespace, f"registry.example.com/e2e/{namespace}/app")


# Same host realm with an explicit registry port (review 1, finding 5)


def test_a_registry_configured_with_port_443_accepts_a_realm_on_the_portless_host() -> None:
    explicit = RegistrySettings(prefix=f"{HOST}:443/e2e", insecure=False, token_hosts=())
    recorder = Recorder(bearer_handler(f"https://{HOST}/token"))
    client = httpx.Client(transport=httpx.MockTransport(recorder), follow_redirects=False)
    registry = DockerRegistry({}, settings=explicit, client=client)

    assert registry.list_tags(f"{HOST}:443/e2e/{NS}/app") == ["build-0a1b2c3d"]
    assert any(r.url.path == "/token" for r in recorder.requests)


# CompositeRegistry over several push configs sharing one cluster (review 1, finding 1)

OTHER_USER = "second-pusher"
OTHER_PASSWORD = "second-pass-SENTINEL"

OPERATIONS = [
    pytest.param(lambda r: r.list_tags(REPO), ["build-0a1b2c3d"], id="list_tags"),
    pytest.param(lambda r: r.resolve(REPO, "build-0a1b2c3d"), DIGEST, id="resolve"),
    pytest.param(lambda r: r.delete_manifest(REPO, DIGEST), None, id="delete_manifest"),
]


def composite_of_two(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[Any, Recorder]:
    from curie_e2e_connector.registry import CompositeRegistry

    recorder = Recorder(handler)
    client = httpx.Client(transport=httpx.MockTransport(recorder), follow_redirects=False)
    first = DockerRegistry({HOST: (USER, PASSWORD)}, settings=settings(), client=client)
    second = DockerRegistry(
        {HOST: (OTHER_USER, OTHER_PASSWORD)}, settings=settings(), client=client
    )
    return CompositeRegistry([first, second]), recorder


def answer_for_second_only(
    refused_status: int, refused_code: str
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Authorization") != basic(OTHER_USER, OTHER_PASSWORD):
            return httpx.Response(
                refused_status, json=errors_body(refused_code, "access to the resource is denied")
            )
        if request.method == "DELETE":
            return httpx.Response(202)
        if request.method == "HEAD":
            return httpx.Response(200, headers={"Docker-Content-Digest": DIGEST})
        return httpx.Response(200, json={"name": PATH, "tags": ["build-0a1b2c3d"]})

    return handler


@pytest.mark.parametrize(
    ("status", "code"),
    [pytest.param(401, "UNAUTHORIZED", id="401"), pytest.param(403, "DENIED", id="403")],
)
@pytest.mark.parametrize(("operation", "expected"), OPERATIONS)
def test_composite_falls_through_to_the_next_credential_on_an_auth_refusal(
    operation: Callable[[Any], Any], expected: Any, status: int, code: str
) -> None:
    registry, recorder = composite_of_two(answer_for_second_only(status, code))

    assert operation(registry) == expected

    sent = [r.headers.get("Authorization") for r in recorder.requests]
    assert sent[0] == basic(USER, PASSWORD)
    assert basic(OTHER_USER, OTHER_PASSWORD) in sent


@pytest.mark.parametrize(("operation", "_expected"), OPERATIONS)
def test_composite_raises_a_non_auth_error_instead_of_masking_it(
    operation: Callable[[Any], Any], _expected: Any
) -> None:
    registry, recorder = composite_of_two(
        lambda request: httpx.Response(500, json=errors_body("UNKNOWN", "boom"))
    )

    with pytest.raises(RegistryError, match="500"):
        operation(registry)

    assert basic(OTHER_USER, OTHER_PASSWORD) not in [
        r.headers.get("Authorization") for r in recorder.requests
    ]

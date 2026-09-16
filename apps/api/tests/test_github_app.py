"""The platform's GitHub identity (ADR-0092).

The happy path is two HTTP calls. What is worth asserting is the selection
between credential paths, the scoping that makes an App token safer than a PAT,
and the caching -- because a cache bug here means either a token exchange on
every push or a dead token served until restart.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import Settings
from curie_api.github_app import GitHubAppError, GitHubCredentials

REPO = "octo/agent-bot"

# Captured before any patching. `curie_api.github_app.httpx` is the SAME module
# object as this module's `httpx`, so patching `.Client` there patches it here
# too -- a replacement that calls `httpx.Client(...)` would call itself.
_REAL_CLIENT = httpx.Client


def serve(handler: Any) -> Any:
    """A replacement `httpx.Client` that answers from `handler`."""

    return lambda *a, **kw: _REAL_CLIENT(transport=httpx.MockTransport(handler))


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def app_settings(private_key: str, **over: Any) -> Settings:
    return Settings(github_app_id="12345", github_app_private_key=private_key, **over)


class Recorder:
    """Stands in for GitHub. Records what we sent so scoping can be asserted."""

    def __init__(self, expires_at: str = "2999-01-01T00:00:00Z"):
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.minted = 0
        self._expires_at = expires_at

    def handle(self, request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, str(request.url), body))
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": 4242})
        if request.url.path.endswith("/access_tokens"):
            self.minted += 1
            return httpx.Response(
                201, json={"token": f"ghs_minted_{self.minted}", "expires_at": self._expires_at}
            )
        return httpx.Response(404, json={"message": "Not Found"})


@pytest.fixture
def github(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder()
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    return recorder


# --------------------------------------------------------------------------- #
# Which credential
# --------------------------------------------------------------------------- #
def test_the_app_is_preferred_over_a_token(private_key: str, github: Recorder) -> None:
    creds = GitHubCredentials(settings=app_settings(private_key, github_token="ghp_the_pat"))
    assert creds.token_for(REPO) == "ghs_minted_1"


def test_the_token_is_used_when_no_app_is_configured(github: Recorder) -> None:
    creds = GitHubCredentials(settings=Settings(github_token="ghp_the_pat"))
    assert creds.token_for(REPO) == "ghp_the_pat"
    assert github.calls == [], "no App configured means GitHub is never called"


def test_no_credential_configured_is_not_an_error() -> None:
    # A public repository clones with no header at all, and refusing to boot
    # would break every installation that never deploys from a private repo.
    creds = GitHubCredentials(settings=Settings(github_token=""))
    assert creds.token_for(REPO) == ""


def test_a_half_configured_app_falls_back_rather_than_breaking(github: Recorder) -> None:
    # An id with no key cannot sign anything. Falling back beats failing every
    # clone with a signing error.
    creds = GitHubCredentials(
        settings=Settings(github_app_id="12345", github_app_private_key="", github_token="ghp_x")
    )
    assert creds.token_for(REPO) == "ghp_x"


@pytest.mark.parametrize(
    ("settings_kwargs", "expected"),
    [
        ({"github_token": "ghp_x"}, "personal access token"),
        ({}, "none (public repositories only)"),
    ],
)
def test_describe_names_the_path_and_never_a_secret(
    settings_kwargs: dict[str, Any], expected: str
) -> None:
    creds = GitHubCredentials(settings=Settings(**settings_kwargs))
    assert creds.describe() == expected


def test_describe_does_not_leak_the_private_key(private_key: str) -> None:
    described = GitHubCredentials(settings=app_settings(private_key)).describe()
    assert "PRIVATE KEY" not in described
    assert private_key[:40] not in described


# --------------------------------------------------------------------------- #
# Scoping -- the reason an App beats an org-wide PAT
# --------------------------------------------------------------------------- #
def test_the_minted_token_is_scoped_to_the_one_repository(
    private_key: str, github: Recorder
) -> None:
    # An installation may cover the whole org. Narrowing to the repository being
    # cloned is what makes a leaked token useless against the others.
    GitHubCredentials(settings=app_settings(private_key)).token_for(REPO)
    mint = [c for c in github.calls if c[1].endswith("/access_tokens")][0]
    assert mint[2] == {"repositories": ["agent-bot"]}


def test_the_installation_is_discovered_not_configured(private_key: str, github: Recorder) -> None:
    # No installation id in settings anywhere: onboarding a repo is a checkbox
    # in GitHub, not a values change.
    GitHubCredentials(settings=app_settings(private_key)).token_for(REPO)
    assert any(c[1].endswith(f"/repos/{REPO}/installation") for c in github.calls)
    assert "github_app_installation_id" not in Settings.model_fields


def test_a_valid_repository_is_preserved_in_app_lookup_and_token_scope(
    private_key: str, github: Recorder
) -> None:
    repo = "Octo-Corp/repo.name_with-parts"

    GitHubCredentials(settings=app_settings(private_key)).token_for(repo)

    lookup = [call for call in github.calls if call[1].endswith("/installation")]
    mint = [call for call in github.calls if call[1].endswith("/access_tokens")]
    assert lookup[0][1] == f"https://api.github.com/repos/{repo}/installation"
    assert mint[0][2] == {"repositories": ["repo.name_with-parts"]}


def test_invalid_repo_full_name_never_reaches_github_app_http(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = Recorder()
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    with pytest.raises(GitHubAppError):
        creds.token_for("octo/../escape?token=x")

    assert recorder.calls == []


# --------------------------------------------------------------------------- #
# Caching
# --------------------------------------------------------------------------- #
def test_a_second_push_reuses_the_cached_token(private_key: str, github: Recorder) -> None:
    creds = GitHubCredentials(settings=app_settings(private_key))
    first, second = creds.token_for(REPO), creds.token_for(REPO)
    assert first == second
    assert github.minted == 1, "a burst of pushes must not cost a token exchange each"


def test_a_near_expiry_token_is_reminted(private_key: str, monkeypatch) -> None:
    """A token inside the refresh margin is re-minted before it can expire.

    This used `1971-01-01`, already long past, so it proved only that an
    EXPIRED token is replaced -- deleting the 300s margin entirely left it
    green (#1263). The margin exists for a token that is still valid now and
    will not be by the time a clone finishes, so the expiry has to sit in the
    future and inside the margin for the assertion to mean anything.
    """

    from curie_api.github_app import _TOKEN_REFRESH_MARGIN_SECONDS

    soon = datetime.now(UTC) + timedelta(seconds=_TOKEN_REFRESH_MARGIN_SECONDS / 2)
    recorder = Recorder(expires_at=soon.strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))
    creds.token_for(REPO)
    creds.token_for(REPO)
    assert recorder.minted == 2


def test_a_token_comfortably_outside_the_margin_is_reused(private_key: str, monkeypatch) -> None:
    """The other side of the margin: still-fresh tokens are NOT re-minted.

    Without this, "re-mint whenever asked" also passes the test above, and the
    cache stops being a cache.
    """

    from curie_api.github_app import _TOKEN_REFRESH_MARGIN_SECONDS

    later = datetime.now(UTC) + timedelta(seconds=_TOKEN_REFRESH_MARGIN_SECONDS * 4)
    recorder = Recorder(expires_at=later.strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))
    creds.token_for(REPO)
    creds.token_for(REPO)
    assert recorder.minted == 1


def test_an_unparseable_expiry_is_not_treated_as_immortal(private_key: str, monkeypatch) -> None:
    """A garbled expiry must fall back to an hour, not to forever.

    Caching a token as never-expiring fails every clone after it dies, until
    someone restarts the pod -- and the log says "authentication", not
    "expired". Treating it as immortal survived the suite (#1263).
    """

    recorder = Recorder(expires_at="not-a-timestamp")
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))
    creds.token_for(REPO)
    cached = creds._tokens[REPO]
    assumed = cached.expires_at - time.time()
    assert 3000 < assumed < 4200, f"expected the documented ~1h fallback, got {assumed:.0f}s"


def test_repository_keyed_app_caches_are_bounded(
    private_key: str, monkeypatch
) -> None:
    from curie_api.github_app import _REPOSITORY_CACHE_LIMIT

    creds = GitHubCredentials(settings=app_settings(private_key))
    monkeypatch.setattr(
        creds,
        "_mint_installation_token",
        lambda repo: (f"token-{repo}", time.time() + 3600),
    )
    for index in range(_REPOSITORY_CACHE_LIMIT + 1):
        creds.token_for(f"acme-corp/repo-{index}")
    assert len(creds._tokens) == _REPOSITORY_CACHE_LIMIT
    assert "acme-corp/repo-0" not in creds._tokens

    monkeypatch.setattr(creds, "_get", lambda url: {"id": 42})
    for index in range(_REPOSITORY_CACHE_LIMIT + 1):
        creds._installation_id(f"acme-corp/install-{index}")
    assert len(creds._installations) == _REPOSITORY_CACHE_LIMIT
    assert "acme-corp/install-0" not in creds._installations


# --------------------------------------------------------------------------- #
# The JWT itself -- GitHub rejects a malformed one with a bare 401
# --------------------------------------------------------------------------- #
def _decode(token: str, private_key: str) -> dict[str, Any]:
    """Verify and decode, the way GitHub does."""
    from cryptography.hazmat.primitives import serialization as ser

    public = ser.load_pem_private_key(private_key.encode(), password=None).public_key()
    return jwt.decode(token, public, algorithms=["RS256"], options={"verify_aud": False})


def test_the_jwt_names_this_app_as_the_issuer(private_key: str) -> None:
    # `iss` is how GitHub knows which App is calling. Replacing it with "0"
    # survived every test (#1263); the symptom is a 401 naming nothing.
    creds = GitHubCredentials(settings=app_settings(private_key))
    assert _decode(creds._app_jwt(), private_key)["iss"] == "12345"


def test_the_jwt_is_backdated_and_not_yet_expired(private_key: str) -> None:
    """`iat` in the past, `exp` in the future, both within GitHub's bounds.

    GitHub rejects a JWT whose `iat` is in its future -- ordinary clock skew
    does it -- and one whose `exp` is more than 10 minutes out. Pushing `iat`
    600s forward, or setting `exp` already-past, both survived (#1263), and
    both produce a 401 that says nothing about time.
    """

    creds = GitHubCredentials(settings=app_settings(private_key))
    now = time.time()
    claims = _decode(creds._app_jwt(), private_key)
    assert claims["iat"] < now, "iat must be backdated for clock skew"
    assert claims["exp"] > now, "exp must be in the future"
    assert claims["exp"] - claims["iat"] <= 600, "exp must stay inside GitHub's 10-minute cap"


# --------------------------------------------------------------------------- #
# The shared-resolver cache key
# --------------------------------------------------------------------------- #
def test_rotating_the_private_key_gets_a_new_resolver(private_key: str) -> None:
    """The cache key includes the key, so a rotation is not served stale.

    Dropping the private key from the key survived (#1263): after a rotation
    the old resolver -- and its cached tokens minted with the retired key --
    would be handed back until the process restarted.
    """

    from curie_api.github_app import credentials_for

    first = credentials_for(app_settings(private_key))
    assert credentials_for(app_settings(private_key)) is first, "same config, same resolver"

    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    rotated = other.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    assert credentials_for(app_settings(rotated)) is not first


def test_two_repositories_do_not_share_a_token(private_key: str, github: Recorder) -> None:
    creds = GitHubCredentials(settings=app_settings(private_key))
    assert creds.token_for(REPO) != creds.token_for("octo/other-bot")


# --------------------------------------------------------------------------- #
# Failure modes an operator has to be able to act on
# --------------------------------------------------------------------------- #
def test_a_repository_the_app_cannot_see_says_so(private_key: str, monkeypatch) -> None:
    # The most likely setup mistake, and GitHub's own "Not Found" explains
    # nothing about how to fix it.
    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client", serve(lambda r: httpx.Response(404, json={}))
    )
    creds = GitHubCredentials(settings=app_settings(private_key))
    with pytest.raises(GitHubAppError, match="not installed on that repository"):
        creds.token_for(REPO)


def test_a_malformed_private_key_is_reported_as_a_config_error() -> None:
    creds = GitHubCredentials(
        settings=Settings(github_app_id="1", github_app_private_key="not-a-pem")
    )
    with pytest.raises(GitHubAppError, match="full PEM private key"):
        creds.token_for(REPO)


def test_an_unreachable_github_is_not_silently_a_missing_credential(
    private_key: str, monkeypatch
) -> None:
    # Returning "" here would downgrade an outage into "clone a private repo
    # anonymously", whose error names authentication and misleads the operator.
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(boom))
    creds = GitHubCredentials(settings=app_settings(private_key))
    with pytest.raises(GitHubAppError, match="could not reach GitHub"):
        creds.token_for(REPO)


# --------------------------------------------------------------------------- #
# The resolver has to outlive one push, or the cache above is decorative
# --------------------------------------------------------------------------- #
def test_the_resolver_is_shared_across_pushes(private_key: str) -> None:
    # A fresh resolver per webhook means two extra GitHub calls on every push
    # and a cache that is never once hit.
    from curie_api.github_app import credentials_for

    settings = app_settings(private_key)
    assert credentials_for(settings) is credentials_for(app_settings(private_key))


def test_a_different_configuration_gets_a_different_resolver(private_key: str) -> None:
    from curie_api.github_app import credentials_for

    assert credentials_for(app_settings(private_key)) is not credentials_for(
        Settings(github_token="ghp_other")
    )


# --------------------------------------------------------------------------- #
# Saying which credential is live (#1262)
# --------------------------------------------------------------------------- #
def test_the_startup_line_names_the_credential_path(caplog) -> None:
    """AC1/AC3. ADR-0092 promised this line and it was never emitted.

    `describe()` had two tests and no caller, so an operator could not tell an
    App install from a PAT install from one that 404s on every private clone.
    Removing the call from create_app must turn this red.
    """

    from curie_api.main import create_app

    with caplog.at_level(logging.INFO, logger="curie_api"):
        create_app()
    assert any("github credential path" in r.getMessage() for r in caplog.records), (
        "no startup line names the credential path"
    )


def test_a_half_configured_app_warns_rather_than_looking_unconfigured(caplog) -> None:
    """AC2. ID set, key empty -- the DEFAULT shape of a partly-applied Secret.

    `app_configured` needs both, and the chart always renders the key from a
    secretKeyRef defaulting to empty. So a sync still in flight, or a typo'd
    key name, silently yields a PAT-or-nothing platform and a 404 on the
    private clone -- with nothing in the log distinguishing it from an install
    that never configured an App at all.
    """

    from curie_api.github_app import log_credential_path

    settings = Settings(github_app_id="1234567", github_app_private_key="", github_token="ghp_x")
    with caplog.at_level(logging.INFO, logger="curie_api"):
        log_credential_path(settings)

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings, "a half-configured App must not look like an unconfigured one"
    assert "1234567" in warnings[0].getMessage()
    assert "EMPTY" in warnings[0].getMessage()


def test_a_fully_configured_app_does_not_warn(private_key: str, caplog) -> None:
    # Otherwise "always warn" satisfies the test above and the line becomes noise.
    from curie_api.github_app import log_credential_path

    with caplog.at_level(logging.INFO, logger="curie_api"):
        log_credential_path(app_settings(private_key))
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_the_startup_line_carries_no_secret(private_key: str, caplog) -> None:
    from curie_api.github_app import log_credential_path

    with caplog.at_level(logging.INFO, logger="curie_api"):
        log_credential_path(app_settings(private_key, github_token="ghp_supersecret"))
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "PRIVATE KEY" not in text
    assert "ghp_supersecret" not in text


# --------------------------------------------------------------------------- #
# The installation-id cache -- reinstalling the App used to need a restart (#1257)
# --------------------------------------------------------------------------- #
class ReinstallRecorder:
    """GitHub across an App reinstall: discovery moves, the retired id 404s.

    `installation_id` is what discovery reports; `valid_ids` is what
    `/access_tokens` will still mint for. Setting the two to different values is
    exactly the state an operator lands in the moment they reinstall the App.
    """

    def __init__(self, installation_id: int = 4242, valid_ids: set[int] | None = None):
        self.minted = 0
        self.discoveries = 0
        self.installation_id = installation_id
        self.valid_ids = {installation_id} if valid_ids is None else valid_ids

    def handle(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[-1] == "installation":
            self.discoveries += 1
            return httpx.Response(200, json={"id": self.installation_id})
        if parts[-1] == "access_tokens":
            requested = int(parts[-2])
            if requested not in self.valid_ids:
                # What GitHub answers for an installation that no longer exists.
                return httpx.Response(404, json={"message": "Not Found"})
            self.minted += 1
            return httpx.Response(
                201,
                json={"token": f"ghs_from_{requested}", "expires_at": "2999-01-01T00:00:00Z"},
            )
        return httpx.Response(404, json={"message": "Not Found"})


def _expire_cached_token(creds: GitHubCredentials, repo: str) -> None:
    """Age out the token cache so the next call actually mints.

    The token cache is the thing that hid this bug in production for an hour at
    a time; forcing it stale is setup, not the assertion.
    """

    creds._tokens[repo].expires_at = time.time() - 1


def test_a_reinstalled_app_recovers_without_restarting_the_api(private_key, monkeypatch) -> None:
    """AC1. `_installations` had no invalidation of any kind (#1257).

    Reinstalling the App retires the installation id. Every later mint POSTed to
    the retired id, took a 404, and never re-ran discovery -- so the only
    recovery was restarting the API, and the 404 told the operator to install an
    App they had just installed. Deleting the eviction turns this red: the
    second `token_for` raises instead of returning a token from 8484.
    """

    recorder = ReinstallRecorder()
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    assert creds.token_for(REPO) == "ghs_from_4242"
    discoveries_before = recorder.discoveries

    # The operator reinstalls the App.
    recorder.installation_id = 8484
    recorder.valid_ids = {8484}
    _expire_cached_token(creds, REPO)

    assert creds.token_for(REPO) == "ghs_from_8484", "a reinstall must not need a restart"
    assert recorder.discoveries == discoveries_before + 1, "the stale id must be re-discovered"


def test_the_re_discovered_installation_is_cached_again(private_key, monkeypatch) -> None:
    # Otherwise "always re-discover" also passes the test above and the
    # installation cache stops being a cache.
    recorder = ReinstallRecorder()
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    creds.token_for(REPO)
    recorder.installation_id = 8484
    recorder.valid_ids = {8484}
    _expire_cached_token(creds, REPO)
    creds.token_for(REPO)

    discoveries_after_recovery = recorder.discoveries
    _expire_cached_token(creds, REPO)
    assert creds.token_for(REPO) == "ghs_from_8484"
    assert recorder.discoveries == discoveries_after_recovery, "recovery must not disable the cache"


def test_a_freshly_discovered_installation_is_not_re_discovered(private_key, monkeypatch) -> None:
    """Exactly one retry, and only when the id came from the cache.

    If discovery just produced the id, re-running discovery can only produce the
    same id and the same 404 -- a second round trip on every genuine failure,
    and one 404 away from a retry loop.
    """

    recorder = ReinstallRecorder(valid_ids=set())
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    with pytest.raises(GitHubAppError):
        creds.token_for(REPO)

    assert recorder.discoveries == 1, "a fresh id that 404s must surface, not be re-discovered"
    assert recorder.minted == 0


def test_a_fresh_installation_404_never_tells_the_operator_to_install_the_app(
    private_key, monkeypatch
) -> None:
    """AC3, the no-cache half. Nothing was cached, so nothing may be called stale.

    Discovery 404 means "install the App" (asserted by
    `test_a_repository_the_app_cannot_see_says_so`). A mint 404 on an id
    discovery just resolved means the App IS installed -- repeating the install
    advice is what sent operators round the loop after a reinstall. But this
    path evicted nothing and re-discovered nothing, so the reinstall wording is
    simply false here and points at a cache bug that does not exist (#1257).
    """

    recorder = ReinstallRecorder(valid_ids=set())
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    with pytest.raises(GitHubAppError) as raised:
        creds.token_for(REPO)

    message = str(raised.value)
    assert "4242" in message, "the operator has to be told which installation failed"
    assert "not installed on that repository" not in message
    assert "install it" not in message
    assert "stale" not in message, "nothing was cached here, so nothing went stale"


def test_a_stale_installation_404_says_the_cached_one_was_re_discovered(
    private_key, monkeypatch
) -> None:
    """AC3, the eviction half. The reinstall recovery ran and still failed.

    The operator reinstalled, we evicted the retired id and re-discovered a new
    one, and that one cannot mint either. This is the one state where naming the
    stale cache is true -- and the id quoted has to be the RE-DISCOVERED one, or
    the operator goes and inspects an installation GitHub already retired.
    """

    recorder = ReinstallRecorder()
    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(recorder.handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    creds.token_for(REPO)  # caches installation 4242

    # The operator reinstalls, and the new installation is broken too.
    recorder.installation_id = 8484
    recorder.valid_ids = set()
    _expire_cached_token(creds, REPO)

    with pytest.raises(GitHubAppError) as raised:
        creds.token_for(REPO)

    message = str(raised.value)
    assert "8484" in message, "the re-discovered installation is the one to go and look at"
    assert "stale" in message
    assert "not installed on that repository" not in message
    assert "install it" not in message


# --------------------------------------------------------------------------- #
# Single-flight -- `clone_and_archive` runs under `run_in_threadpool`
# --------------------------------------------------------------------------- #
def test_concurrent_pushes_to_one_repo_mint_exactly_one_token(private_key, monkeypatch) -> None:
    """`token_for` was a check-then-act on a plain dict (#1257).

    Concurrent webhook pushes are real OS threads, so eight of them produced
    five distinct tokens from five mints. Every token was valid, so this is
    redundant token exchanges and rate-limit pressure rather than a correctness
    bug -- but the fix is a per-repo lock and a re-read of the cache.
    """

    workers = 8
    recorder = Recorder()
    barrier = threading.Barrier(workers)
    inner = recorder.handle

    def slow(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/access_tokens"):
            time.sleep(0.05)  # a real window for the losers to pile into
        return inner(request)

    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(slow))
    creds = GitHubCredentials(settings=app_settings(private_key))

    seen: list[str] = []
    guard = threading.Lock()

    def push() -> None:
        barrier.wait(timeout=10)
        token = creds.token_for(REPO)
        with guard:
            seen.append(token)

    threads = [threading.Thread(target=push) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert len(seen) == workers
    assert recorder.minted == 1, f"{workers} concurrent pushes cost {recorder.minted} exchanges"
    assert set(seen) == {"ghs_minted_1"}, "every caller must get the one minted token"


def test_two_repositories_do_not_serialize_on_one_lock(private_key, monkeypatch) -> None:
    """One slow repository must not stall a mint for a different repository.

    A single global lock also passes the single-flight test above, at the cost
    of holding it across the HTTP call: one slow repo would then stall every
    other push. Asserting only that `_mint_lock_for` hands back distinct objects
    does not catch that -- per-repo lock objects plus a separate global lock
    around `token_for` would pass while serializing every repository. So this
    overlaps two real mints and asserts the second one finished while the first
    was still in flight.
    """

    other = "octo/other-bot"
    recorder = Recorder()
    inner = recorder.handle
    slow_mint_started = threading.Event()
    release_slow_mint = threading.Event()

    def handle(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.content) if request.content else None
        blocks = request.url.path.endswith("/access_tokens") and (
            isinstance(body, dict) and body.get("repositories") == ["agent-bot"]
        )
        if blocks:
            slow_mint_started.set()
            # Bounded, and released by the main thread below: a regression has to
            # FAIL on the assertions, never hang the suite waiting for a deadlock.
            release_slow_mint.wait(timeout=30)
        return inner(request)

    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(handle))
    creds = GitHubCredentials(settings=app_settings(private_key))

    fast_finished = threading.Event()

    def fast_push() -> None:
        creds.token_for(other)
        fast_finished.set()

    slow = threading.Thread(target=creds.token_for, args=(REPO,))
    fast = threading.Thread(target=fast_push)
    overtook = False
    slow_still_in_flight = False
    slow.start()
    try:
        reached_mint = slow_mint_started.wait(timeout=10)
        if reached_mint:
            fast.start()
            overtook = fast_finished.wait(timeout=10)
            slow_still_in_flight = slow.is_alive()
    finally:
        release_slow_mint.set()
        slow.join(timeout=30)
        if fast.ident is not None:
            fast.join(timeout=30)

    assert reached_mint, "the slow repository never reached its own mint"
    assert overtook, f"{other} serialized behind {REPO}'s in-flight mint"
    assert slow_still_in_flight, "the slow mint had already finished; the overlap proved nothing"
    assert creds._mint_lock_for(REPO) is not creds._mint_lock_for(other)
    assert creds._mint_lock_for(REPO) is creds._mint_lock_for(REPO)


# --------------------------------------------------------------------------- #
# Expiry timezone -- latent, but the fallout is a seven-hour outage
# --------------------------------------------------------------------------- #
def test_a_naive_expiry_is_read_as_utc() -> None:
    """An expiry without `Z` must not be read in the container's local zone.

    `.timestamp()` on a naive datetime applies local time: under
    TZ=America/Los_Angeles a 3600s token caches as 28800s and every clone 401s
    for about seven hours, with the log saying "authentication". Asserting the
    naive and `Z` forms agree keeps this independent of the machine's own TZ.
    """

    naive = GitHubCredentials._expiry_seconds("2026-01-01T00:00:00")
    zulu = GitHubCredentials._expiry_seconds("2026-01-01T00:00:00Z")
    assert naive == zulu


def test_an_explicit_offset_expiry_is_still_honoured() -> None:
    # The UTC default must apply only when the value carries no zone at all.
    offset = GitHubCredentials._expiry_seconds("2026-01-01T00:00:00+02:00")
    assert offset == GitHubCredentials._expiry_seconds("2025-12-31T22:00:00Z")


def test_fresh_installation_cannot_change_during_token_mint(
    private_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A repository installation can disappear between discovery and mint. The
    # exact-identity review/publication path must not inherit the clone helper's
    # rediscovery fallback (REST installation/token endpoints documented above).
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": 4242 if len(calls) == 1 else 4343})
        if request.url.path == "/app/installations/4242/access_tokens":
            return httpx.Response(404, json={"message": "Not Found"})
        return httpx.Response(
            201, json={"token": "fixture-other-installation", "expires_at": "2999-01-01T00:00:00Z"}
        )

    monkeypatch.setattr("curie_api.github_app.httpx.Client", serve(handle))
    credentials = GitHubCredentials(settings=app_settings(private_key))
    with pytest.raises(GitHubAppError):
        credentials.token_for_verified_installation(REPO, 4242)
    assert calls == [f"/repos/{REPO}/installation", "/app/installations/4242/access_tokens"]

"""The platform's own GitHub identity (ADR-0092).

Answers one question: *what credential may this installation use to read
repository X?* Everything about how that credential is then applied — the
`x-access-token` basic header, keeping it out of `argv` and out of
`.git/config` — stays in `gitflow._clone_credential_env`, unchanged.

Two paths, in order:

1. **A GitHub App.** The platform holds a private key and mints a token scoped
   to ONE repository, valid for an hour. Nothing to rotate, no human owner, and
   a leak is useless against any other repository.
2. **A personal access token.** The fallback, and deliberately not deprecated:
   a GitHub Enterprise or air-gapped install may have no App, and a first-run
   operator should be able to prove the flow before registering one.

Neither configured is not an error. A public repository clones with no
credential at all, and failing at startup would make the platform refuse to boot
for installations that never deploy from a private repo.

The installation is DISCOVERED, not configured. An operator who has to look up
an opaque numeric id to onboard a repository has not been given the "tick a box
in GitHub" that ADR-0092 is buying.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

import httpx
import jwt

from .config import Settings
from .repo_full_name import (
    InvalidRepoFullName,
    normalize_repo_full_name,
    repo_url_path,
)

logger = logging.getLogger(__name__)

# GitHub rejects an App JWT with an `iat` in its future, and small clock skew
# between us and GitHub is normal. Backdating by a minute is the documented
# remedy. The `exp` stays well inside GitHub's 10-minute ceiling.
_JWT_BACKDATE_SECONDS = 60
_JWT_LIFETIME_SECONDS = 480

# Re-mint this long before a token actually expires, so a clone that starts just
# under the wire does not have its credential expire mid-transfer.
_TOKEN_REFRESH_MARGIN_SECONDS = 300
_REPOSITORY_CACHE_LIMIT = 256


# Resolvers are shared per credential configuration, because the token cache
# lives on the resolver. Constructing one per webhook -- the obvious thing --
# would mean two extra GitHub calls on every single push, and the cache would
# never once be hit. Keyed on the fields that determine identity, so a test
# passing its own Settings gets its own resolver rather than a poisoned one.
_RESOLVERS: dict[tuple[str, str, str, str], GitHubCredentials] = {}


def credentials_for(settings: Settings) -> GitHubCredentials:
    """The shared resolver for this credential configuration."""

    key = (
        settings.github_app_id,
        settings.github_app_private_key,
        settings.github_token,
        settings.github_api_url,
    )
    existing = _RESOLVERS.get(key)
    if existing is None:
        existing = GitHubCredentials(settings=settings)
        _RESOLVERS[key] = existing
    return existing


def log_credential_path(settings: Settings) -> None:
    """Say once, at startup, which credential the platform will clone with.

    ADR-0092 said this line existed. It did not (#1262): `describe()` was never
    called from anywhere, so an operator had no way to tell an App install from
    a PAT install from one that will 404 on every private clone -- and two
    tests covered the dead method, which is worse than none.
    """

    credentials = credentials_for(settings)
    logger.info("github credential path: %s", credentials.describe())
    warning = credentials.half_configured()
    if warning:
        # Above INFO deliberately: this is the state that looks configured and
        # is not, and it is the DEFAULT shape of a partly-applied BYO Secret.
        logger.warning("github app is half-configured: %s", warning)


class GitHubAppError(RuntimeError):
    """The App is configured but could not produce a token."""


class GitHubInstallationRefused(GitHubAppError):
    """Fresh App installation identity was deterministically refused."""


class _GitHubNotFound(GitHubAppError):
    """GitHub answered 404.

    A subclass rather than a `not_found_message` parameter because the caller,
    not `_request`, is the only thing that knows which 404 this is: the same
    status on `/repos/{repo}/installation` means "the App was never installed"
    while on `/access_tokens` it usually means "the id we cached was retired by
    a reinstall". Callers that do not catch it still see the identical
    `GitHubAppError` with the identical message, so `_request`'s contract is
    unchanged.

    Carries no extra state: the type IS the signal, and a `status_code`
    attribute nothing ever read only invited a second way to ask the same
    question.
    """


@dataclass
class _CachedToken:
    token: str
    expires_at: float

    def usable(self, now: float) -> bool:
        return bool(self.token) and now < self.expires_at - _TOKEN_REFRESH_MARGIN_SECONDS


@dataclass
class GitHubCredentials:
    """Resolves a read credential for one repository.

    Holds a token cache keyed by repository, so a burst of pushes to one repo
    costs one token exchange rather than one per push.

    `clone_and_archive` runs under `run_in_threadpool`, so concurrent webhook
    pushes hit this object from genuine OS threads. The per-repository mint lock
    below is what stops eight simultaneous pushes from doing eight token
    exchanges (measured: 8 threads, 5 distinct tokens) -- every one of those
    tokens is valid, so this is rate-limit pressure rather than a correctness
    bug, but it is free to avoid.
    """

    settings: Settings
    _tokens: OrderedDict[str, _CachedToken] = field(default_factory=OrderedDict)
    _installations: OrderedDict[str, int] = field(default_factory=OrderedDict)
    # Per-repository, never global: a mint for one repo must not serialize a
    # mint for another, and the HTTP call happens while this is held.
    _mint_locks: dict[str, threading.Lock] = field(default_factory=dict)
    # Guards only the dict above, and is never held across a network call.
    _mint_locks_guard: threading.Lock = field(default_factory=threading.Lock)

    @property
    def app_configured(self) -> bool:
        return bool(self.settings.github_app_id and self.settings.github_app_private_key)

    def describe(self) -> str:
        """Which path is in use, for one startup log line. Names no secret.

        Called by `log_credential_path` below. ADR-0092 promised this line and
        it was never emitted -- two tests covered a method nothing invoked
        (#1262).
        """

        if self.app_configured:
            return f"github app (app_id={self.settings.github_app_id})"
        if self.settings.github_token:
            return "personal access token"
        return "none (public repositories only)"

    def half_configured(self) -> str | None:
        """The App is partly set up, so the platform silently used something else.

        `app_configured` needs BOTH the id and the key, and the chart always
        renders GITHUB_APP_PRIVATE_KEY from a secretKeyRef whose default is
        empty -- so present-but-empty is the DEFAULT state, not an absent one.
        An operator whose external-secrets sync is still in flight, or who
        typo'd the key name against a Secret that carries it empty, gets a
        PAT-or-nothing platform and a 404 on the private clone. The 404 handler
        writes a good message, but it is unreachable: the App path was never
        entered (#1262).
        """

        if self.app_configured:
            return None
        if self.settings.github_app_id and not self.settings.github_app_private_key:
            return (
                f"github_app_id is set (app_id={self.settings.github_app_id}) but the "
                "private key is EMPTY, so the App is not in use. A BYO Secret that "
                "exists but is unpopulated looks exactly like this -- check the key "
                "name and whether the sync has completed."
            )
        if self.settings.github_app_private_key and not self.settings.github_app_id:
            return (
                "a GitHub App private key is set but github_app_id is empty, "
                "so the App is not in use."
            )
        return None

    def token_for(self, repo_full_name: str) -> str:
        """A credential able to read ``owner/repo``, or "" if none is configured.

        Returning "" rather than raising is deliberate: a public repository
        needs no credential, and `_clone_credential_env` already treats an empty
        token as "send no Authorization header".
        """

        try:
            repo_full_name = normalize_repo_full_name(repo_full_name)
        except InvalidRepoFullName as exc:
            raise GitHubAppError(str(exc)) from exc

        if not self.app_configured:
            return self.settings.github_token

        cached = self._tokens.get(repo_full_name)
        if cached is not None and cached.usable(time.time()):
            self._tokens.move_to_end(repo_full_name)
            return cached.token

        with self._mint_lock_for(repo_full_name):
            # Double-checked: the thread that held this lock has almost
            # certainly just populated the cache we lost the race to read.
            cached = self._tokens.get(repo_full_name)
            if cached is not None and cached.usable(time.time()):
                self._tokens.move_to_end(repo_full_name)
                return cached.token

            token, expires_at = self._mint_installation_token(repo_full_name)
            self._tokens[repo_full_name] = _CachedToken(token=token, expires_at=expires_at)
            self._tokens.move_to_end(repo_full_name)
            while len(self._tokens) > _REPOSITORY_CACHE_LIMIT:
                self._tokens.popitem(last=False)
            return token

    def fresh_installation_token(
        self, repo_full_name: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        """Revalidate a review sender's installation with this platform's App.

        A cached token or a user PAT proves neither which App sent an event nor
        whether its claimed installation still owns this repository. Discovery
        uses our App JWT on a repository-derived endpoint on every check; the
        caller must also read the current repository/PR with the returned token.
        """
        repo_full_name = normalize_repo_full_name(repo_full_name)
        if not self.app_configured:
            raise GitHubInstallationRefused("review feedback requires a configured GitHub App")
        url = (
            f"{self.settings.github_api_url.rstrip('/')}"
            f"/repos/{repo_url_path(repo_full_name)}/installation"
        )
        with self._mint_lock_for(repo_full_name):
            try:
                with httpx.Client(timeout=self.settings.github_app_timeout_seconds) as client:
                    response = client.get(url, headers=self._headers(), follow_redirects=False)
                if response.status_code != 200:
                    if response.status_code < 500 and response.status_code not in {
                        401,
                        403,
                        404,
                        429,
                    }:
                        raise GitHubInstallationRefused("current review installation was refused")
                    raise GitHubAppError("current review installation could not be verified")
                payload = response.json()
            except (httpx.HTTPError, ValueError):
                raise GitHubAppError("current review installation could not be verified") from None
            actual = payload.get("id") if isinstance(payload, dict) else None
            if (
                not isinstance(actual, int)
                or isinstance(actual, bool)
                or actual <= 0
                or (expected_installation_id is not None and actual != expected_installation_id)
            ):
                raise GitHubInstallationRefused(
                    "review installation differs from the current App installation"
                )
            if self._installations.get(repo_full_name) != actual:
                # A token is scoped to an installation, not just a repository.
                # Reinstallation invalidates even an otherwise unexpired cache.
                self._tokens.pop(repo_full_name, None)
            self._installations[repo_full_name] = actual
            self._installations.move_to_end(repo_full_name)
            while len(self._installations) > _REPOSITORY_CACHE_LIMIT:
                self._installations.popitem(last=False)
            cached = self._tokens.get(repo_full_name)
            if cached is not None and cached.usable(time.time()):
                self._tokens.move_to_end(repo_full_name)
                return actual, cached.token
            token, expires_at = self._mint_for_installation(repo_full_name, actual)
            self._tokens[repo_full_name] = _CachedToken(token=token, expires_at=expires_at)
            self._tokens.move_to_end(repo_full_name)
            while len(self._tokens) > _REPOSITORY_CACHE_LIMIT:
                self._tokens.popitem(last=False)
            return actual, token

    def token_for_verified_installation(
        self, repo_full_name: str, expected_installation_id: int
    ) -> str:
        """Return a token only for an independently rediscovered exact installation."""
        _, token = self.fresh_installation_token(repo_full_name, expected_installation_id)
        return token

    def _mint_lock_for(self, repo_full_name: str) -> threading.Lock:
        with self._mint_locks_guard:
            lock = self._mint_locks.get(repo_full_name)
            if lock is None:
                lock = threading.Lock()
                self._mint_locks[repo_full_name] = lock
            return lock

    # -- GitHub App plumbing -------------------------------------------------

    def _app_jwt(self) -> str:
        now = int(time.time())
        payload = {
            "iat": now - _JWT_BACKDATE_SECONDS,
            "exp": now + _JWT_LIFETIME_SECONDS,
            "iss": self.settings.github_app_id,
        }
        try:
            return jwt.encode(payload, self.settings.github_app_private_key, algorithm="RS256")
        except Exception as exc:  # a malformed key is an operator error, not a runtime one
            raise GitHubAppError(
                "could not sign a GitHub App JWT; check that github_app_private_key "
                f"is the App's full PEM private key: {type(exc).__name__}"
            ) from exc

    def _installation_id(self, repo_full_name: str) -> tuple[int, bool]:
        """The App's installation on this repository, and whether it was cached.

        The second element is what makes the retry in `_mint_installation_token`
        safe to bound: re-discovering an id that discovery just produced would
        only repeat the same 404.
        """

        known = self._installations.get(repo_full_name)
        if known is not None:
            self._installations.move_to_end(repo_full_name)
            return known, True
        return self._discover_installation_id(repo_full_name), False

    def _discover_installation_id(self, repo_full_name: str) -> int:
        """Ask GitHub which installation covers this repository, and cache it."""

        url = (
            f"{self.settings.github_api_url.rstrip('/')}"
            f"/repos/{repo_url_path(repo_full_name)}/installation"
        )
        data = self._get(url)
        installation_id = data.get("id")
        if not isinstance(installation_id, int):
            raise GitHubAppError(f"GitHub did not report an installation id for {repo_full_name!r}")
        self._installations[repo_full_name] = installation_id
        self._installations.move_to_end(repo_full_name)
        while len(self._installations) > _REPOSITORY_CACHE_LIMIT:
            self._installations.popitem(last=False)
        return installation_id

    def _mint_installation_token(self, repo_full_name: str) -> tuple[str, float]:
        installation_id, from_cache = self._installation_id(repo_full_name)
        try:
            return self._mint_for_installation(repo_full_name, installation_id)
        except _GitHubNotFound as exc:
            if not from_cache:
                raise self._mint_404_error(
                    repo_full_name, installation_id, re_discovered=False
                ) from exc
            # Reinstalling the App retires the old installation and issues a new
            # id. `_installations` had no expiry, so every later mint POSTed to
            # the retired id, 404'd, and never re-ran discovery -- recovery meant
            # restarting the API. Evict and re-discover ONCE; the id we just used
            # came from the cache, so a fresh one is genuinely new information.
            logger.info(
                "cached GitHub installation %s for %r 404'd on mint; re-discovering",
                installation_id,
                repo_full_name,
            )
            self._installations.pop(repo_full_name, None)

        # Deliberately back through `_installation_id`, which reads the cache
        # first, rather than calling `_discover_installation_id` directly: that
        # is what makes the eviction above load-bearing. We hold this repo's
        # mint lock, so nothing can repopulate the entry we just popped, and the
        # lookup therefore misses and discovers. Call discovery directly and the
        # eviction becomes decorative -- deleting it leaves every test green
        # (#1257), because discovery would overwrite the stale entry anyway.
        installation_id, _ = self._installation_id(repo_full_name)
        try:
            return self._mint_for_installation(repo_full_name, installation_id)
        except _GitHubNotFound as exc:
            # One retry, never a loop: discovery has now spoken twice.
            raise self._mint_404_error(repo_full_name, installation_id, re_discovered=True) from exc

    def _mint_for_installation(
        self, repo_full_name: str, installation_id: int
    ) -> tuple[str, float]:
        url = (
            f"{self.settings.github_api_url.rstrip('/')}"
            f"/app/installations/{installation_id}/access_tokens"
        )
        # Scoped to this one repository even though the installation may cover
        # more. A credential that leaks is then useless against the others --
        # the narrowing ADR-0092 buys over an org-wide PAT.
        _, repo = repo_full_name.split("/", 1)
        data = self._post(url, {"repositories": [repo]} if repo else None)

        token = data.get("token")
        if not isinstance(token, str) or not token:
            raise GitHubAppError(f"GitHub returned no installation token for {repo_full_name!r}")
        return token, self._expiry_seconds(data.get("expires_at"))

    @staticmethod
    def _mint_404_error(
        repo_full_name: str, installation_id: int, *, re_discovered: bool
    ) -> GitHubAppError:
        """The mint 404'd on an id discovery resolved. Two operator states, two texts.

        Deliberately NOT the discovery wording in either case. Discovery
        succeeded, so the App *is* registered on the repository, and telling the
        operator to install it is precisely the advice that wasted their time
        after a reinstall.

        The two states are genuinely different and one sentence cannot be true of
        both. On the fresh path nothing was ever cached, nothing was evicted and
        nothing was re-discovered, so claiming a cached installation went stale
        describes a sequence that did not happen and sends the operator hunting a
        cache bug instead of the App's repository permissions (#1257).
        """

        if re_discovered:
            middle = (
                "The previously cached installation was stale, so it was evicted and "
                "re-discovered -- and the re-discovered installation cannot mint either. "
            )
        else:
            middle = (
                "Discovery resolved that installation just now and nothing was cached, "
                "so the App is registered on the repository. "
            )
        return GitHubAppError(
            f"GitHub returned 404 minting an installation token for {repo_full_name!r} via "
            f"installation {installation_id}. "
            + middle
            + f"Check the App's repository access and permissions for {repo_full_name!r}."
        )

    @staticmethod
    def _expiry_seconds(raw: Any) -> float:
        """When the token dies, as a monotonic-ish epoch second.

        GitHub states an ISO timestamp. If it is missing or unparseable, assume
        the documented one hour rather than treating the token as immortal --
        caching a dead token would fail every clone until restart.
        """

        from datetime import UTC, datetime

        if isinstance(raw, str):
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                logger.warning("unparseable GitHub token expiry; assuming one hour")
            else:
                if parsed.tzinfo is None:
                    # GitHub documents these as UTC. A value without `Z` or an
                    # offset would otherwise be read in the container's local
                    # zone by `.timestamp()`: under TZ=America/Los_Angeles a
                    # 3600s token caches as 28800s and every clone 401s for
                    # about seven hours. github.com always sends `Z`, but this
                    # module already promises to survive one that does not.
                    parsed = parsed.replace(tzinfo=UTC)
                return parsed.timestamp()
        return time.time() + 3600

    # -- HTTP ----------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._app_jwt()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _get(self, url: str) -> dict[str, Any]:
        return self._request("GET", url, None)

    def _post(self, url: str, body: dict[str, Any] | None) -> dict[str, Any]:
        return self._request("POST", url, body)

    def _request(self, method: str, url: str, body: dict[str, Any] | None) -> dict[str, Any]:
        try:
            with httpx.Client(timeout=self.settings.github_app_timeout_seconds) as client:
                response = client.request(method, url, json=body, headers=self._headers())
        except httpx.HTTPError as exc:
            raise GitHubAppError(f"could not reach GitHub at {url}: {exc}") from exc

        if response.status_code == 404:
            # The single most likely operator mistake, and the one whose default
            # message ("Not Found") explains nothing. Raised as the subclass so
            # the mint path can tell a retired installation id apart from an App
            # that was never installed; uncaught, it reads identically.
            raise _GitHubNotFound(
                f"GitHub returned 404 for {url}. Usually this means the App is not "
                "installed on that repository -- install it, or add the repository "
                "to the existing installation."
            )
        if response.status_code >= 400:
            raise GitHubAppError(f"GitHub returned {response.status_code} for {url}")

        payload = response.json()
        if not isinstance(payload, dict):
            raise GitHubAppError(f"GitHub returned an unexpected body for {url}")
        return payload

"""Typed configuration of the code hosts, trackers and bindings the factory uses.

ADR 0197: a binding joins one tracker to the repositories its tickets may run
against. A native tracker (GitHub, GitLab) is bound to its own single
repository on the same forge and admits by that forge's write check. A Jira
binding lists repositories by alias on any code host, picks one per ticket
(`curie_api.forges.resolution`), and admits by an allowlist of Jira accounts
or a Jira group.

The model is pure: it validates shape and references and reads nothing. Every
refusal is a ``ValueError`` raised from a validator, so loading a bad config
fails with a ``pydantic.ValidationError`` naming the offending entry.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from curie_api.config import valid_base_branch
from curie_api.forges import types
from curie_api.forges.capabilities import NATIVE_FORGES, mandatory_capabilities
from curie_api.forges.errors import InvalidPairing
from curie_api.forges.paths import valid_repository_path
from curie_api.github_review_events import valid_github_login

# The kinds a deployment may configure. The in-memory kinds are test adapters
# and are not configurable. `test_config_model` pins these to `types`.
CodeHostKind = Literal["github", "gitlab", "bitbucket_cloud", "bitbucket_dc"]
TrackerKind = Literal["github", "gitlab", "jira_cloud"]

# A config name or repository alias. An alias appears in a `repo:<alias>`
# label, so it carries no whitespace and no colon.
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?")
_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]{1,5})?")
_LOOPBACK = frozenset({"localhost", "127.0.0.1"})
MAX_LABEL_LENGTH = 50


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _name(value: str, what: str) -> str:
    if not _NAME.fullmatch(value):
        raise ValueError(
            f"{what} {value!r} must be 1 to 64 lowercase letters, digits, '.', '_' or '-'"
        )
    return value


def _host(value: str) -> str:
    host = value.strip().lower()
    if not _HOST.fullmatch(host):
        raise ValueError(f"host {value!r} must be a bare hostname, optionally with a port")
    return host


def _api_url(value: str) -> str:
    url = value.strip().rstrip("/")
    parts = urlsplit(url)
    loopback_http = parts.scheme == "http" and parts.hostname in _LOOPBACK
    if (
        not (parts.scheme == "https" or loopback_http)
        or not parts.netloc
        or parts.query
        or parts.fragment
        or "?" in url
        or "#" in url
        or "@" in parts.netloc
    ):
        raise ValueError(
            f"api_url {value!r} must be an https URL (http only on localhost) with no"
            " credentials, query or fragment"
        )
    return url


def _duplicates(values: Iterable[str]) -> list[str]:
    return sorted(value for value, count in Counter(values).items() if count > 1)


# Credentials -----------------------------------------------------------------


class GitHubAppCredentialConfig(_Model):
    """A GitHub App: the platform re-mints one-hour installation tokens."""

    type: Literal["github_app"] = "github_app"
    app_id: str = Field(min_length=1)
    private_key: SecretStr
    timeout_s: float = Field(default=15.0, gt=0, allow_inf_nan=False)

    @field_validator("private_key")
    @classmethod
    def check_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("a GitHub App private key must be non-empty")
        return value


class OAuthClientCredentialConfig(_Model):
    """OAuth client credentials (Jira Cloud): the platform re-mints tokens."""

    type: Literal["oauth_client_credentials"] = "oauth_client_credentials"
    client_id: str = Field(min_length=1)
    client_secret: SecretStr
    # The operator-held client secret's own expiry, when the tracker reports one.
    secret_expires_at: AwareDatetime | None = None

    @field_validator("client_secret")
    @classmethod
    def check_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("an OAuth client secret must be non-empty")
        return value


class StaticTokenCredentialConfig(_Model):
    """A long-lived token an operator rotates.

    ``expires_at`` set: a known expiry. ``never_expires``: the token was issued
    without one. Neither: the forge does not report it, so the expiry is unknown.
    ``username`` is set for forges whose git credential is basic auth.
    """

    type: Literal["static_token"] = "static_token"
    token: SecretStr
    username: str | None = Field(default=None, min_length=1)
    expires_at: AwareDatetime | None = None
    never_expires: bool = False

    @model_validator(mode="after")
    def check_expiry(self) -> Self:
        if not self.token.get_secret_value().strip():
            raise ValueError("a static token must be non-empty")
        if self.expires_at is not None and self.never_expires:
            raise ValueError("a static token has an expires_at or never_expires, not both")
        return self


CredentialConfig = Annotated[
    GitHubAppCredentialConfig | OAuthClientCredentialConfig | StaticTokenCredentialConfig,
    Field(discriminator="type"),
]

# Which forge kinds may use a re-minting credential type. A static token is
# accepted everywhere.
_CREDENTIAL_KINDS: dict[str, frozenset[str]] = {
    "github_app": frozenset({types.GITHUB}),
    "oauth_client_credentials": frozenset({types.JIRA_CLOUD}),
}


def _check_credential(kind: str, credential: CredentialConfig) -> None:
    allowed = _CREDENTIAL_KINDS.get(credential.type)
    if allowed is not None and kind not in allowed:
        raise ValueError(f"a {credential.type} credential cannot authenticate to {kind}")


# Forges ----------------------------------------------------------------------


class CodeHostConfig(_Model):
    """One code host connection. ``host`` is the identity host repositories are
    bound under (ADR 0197 identity 2); ``api_url`` is where its API answers."""

    name: str
    kind: CodeHostKind
    host: str
    api_url: str
    credential: CredentialConfig

    @field_validator("name")
    @classmethod
    def check_name(cls, value: str) -> str:
        return _name(value, "code host name")

    @field_validator("host")
    @classmethod
    def check_host(cls, value: str) -> str:
        return _host(value)

    @field_validator("api_url")
    @classmethod
    def check_api_url(cls, value: str) -> str:
        return _api_url(value)

    @model_validator(mode="after")
    def check_credential(self) -> Self:
        _check_credential(self.kind, self.credential)
        return self


class TrackerConfig(_Model):
    """One tracker connection and its intake: the label that marks a ticket, the
    account a mention must name, and how often it is polled (ADR 0197 intake 1)."""

    name: str
    kind: TrackerKind
    host: str
    api_url: str
    credential: CredentialConfig
    label: str
    mention: str
    poll_interval_s: float = Field(default=45.0, gt=0, allow_inf_nan=False)

    @field_validator("name")
    @classmethod
    def check_name(cls, value: str) -> str:
        return _name(value, "tracker name")

    @field_validator("host")
    @classmethod
    def check_host(cls, value: str) -> str:
        return _host(value)

    @field_validator("api_url")
    @classmethod
    def check_api_url(cls, value: str) -> str:
        return _api_url(value)

    @field_validator("label")
    @classmethod
    def check_label(cls, value: str) -> str:
        if (
            not value
            or len(value) > MAX_LABEL_LENGTH
            or any(character.isspace() for character in value)
        ):
            raise ValueError(
                f"label {value!r} must be 1 to {MAX_LABEL_LENGTH} characters with no whitespace"
            )
        return value

    @model_validator(mode="after")
    def check_intake(self) -> Self:
        _check_credential(self.kind, self.credential)
        if self.kind == types.GITHUB:
            valid_mention = valid_github_login(self.mention)
        else:
            valid_mention = bool(self.mention) and not any(c.isspace() for c in self.mention)
        if not valid_mention:
            raise ValueError(f"mention {self.mention!r} is not a {self.kind} account")
        return self

    @property
    def native(self) -> bool:
        """A native tracker pairs only with its own forge and admits by its write check."""

        return self.kind in NATIVE_FORGES


# Repositories and bindings -----------------------------------------------------


class RequiredCheckConfig(_Model):
    """The check a change under ``paths`` must pass (today's per-repo Python CI).

    ``key`` and ``pending_key_prefix`` are normalized check keys
    (`types.NormalizedCheck.key`). A check whose key starts with the prefix is
    a shard that precedes ``key``, so its presence keeps the verdict waiting.
    """

    key: str = Field(min_length=1)
    paths: tuple[str, ...]
    pending_key_prefix: str | None = Field(default=None, min_length=1)

    @field_validator("key")
    @classmethod
    def check_key(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("a required check key must be non-blank")
        return value

    @field_validator("paths")
    @classmethod
    def check_paths(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or not all(
            path.strip() and not path.startswith("/") and not path.endswith("/") for path in value
        ):
            raise ValueError(
                "paths must be non-empty relative prefixes without a leading or trailing slash"
            )
        return value


class CiPolicyConfig(_Model):
    """Per-repository CI policy, keyed on normalized check keys (ADR 0197
    consequence 7). ``metadata_rerun_keys`` are the checks a metadata-only edit
    reruns, so only their fresh results judge a metadata revision."""

    required: RequiredCheckConfig | None = None
    metadata_rerun_keys: tuple[str, ...] = ()

    @field_validator("metadata_rerun_keys")
    @classmethod
    def check_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not all(key.strip() for key in value) or _duplicates(value):
            raise ValueError("metadata_rerun_keys must be unique non-blank check keys")
        return value


class RepositoryBindingConfig(_Model):
    """One repository a binding may run against.

    Identity is (code host kind, code host host, ``project_id``); ``path`` is
    for display and for the adapter's resolve-and-compare before cloning.
    ``bases`` are the branches a ticket may start from (a ``base:`` label picks
    one); empty means the repository's default branch only. ``default_base``,
    when set, is one of ``bases`` and is used when no label picks one.
    """

    alias: str
    code_host: str
    project_id: str = Field(min_length=1)
    path: str
    bases: tuple[str, ...] = ()
    default_base: str | None = None
    ci: CiPolicyConfig = Field(default_factory=CiPolicyConfig)

    @field_validator("alias")
    @classmethod
    def check_alias(cls, value: str) -> str:
        return _name(value, "repository alias")

    @field_validator("project_id")
    @classmethod
    def check_project_id(cls, value: str) -> str:
        if value != value.strip() or any(c.isspace() for c in value):
            raise ValueError(f"project_id {value!r} must carry no whitespace")
        return value

    @model_validator(mode="after")
    def check_bases(self) -> Self:
        if not all(valid_base_branch(base) for base in self.bases) or _duplicates(self.bases):
            raise ValueError(f"repository {self.alias!r} bases must be unique branch names")
        if self.default_base is not None and self.default_base not in self.bases:
            raise ValueError(f"repository {self.alias!r} default_base must be one of its bases")
        return self


class WriteCheck(_Model):
    """Admit when the tracker reports the actor may write (native trackers)."""

    type: Literal["write_check"] = "write_check"


class AccountAllowlist(_Model):
    """Admit tracker accounts by immutable account id, never by email
    (ADR 0197 alternative 3)."""

    type: Literal["allowlist"] = "allowlist"
    accounts: frozenset[str]

    @field_validator("accounts")
    @classmethod
    def check_accounts(cls, value: frozenset[str]) -> frozenset[str]:
        if not value or not all(account and account == account.strip() for account in value):
            raise ValueError("an allowlist names one or more non-blank account ids")
        if any("@" in account for account in value):
            raise ValueError("an allowlist holds account ids, not emails")
        return value


class GroupAuthority(_Model):
    """Admit members of one tracker group, named by its immutable group id."""

    type: Literal["group"] = "group"
    group_id: str = Field(min_length=1)


StartAuthority = Annotated[
    WriteCheck | AccountAllowlist | GroupAuthority, Field(discriminator="type")
]


class BindingConfig(_Model):
    """One tracker scope joined to the repositories its tickets run against.

    ``tracker_scope_id`` is the immutable scope the tracker's issue ids are
    unique within (ADR 0197 identity 1): the repository id on GitHub, the
    project id on GitLab, the site id on Jira.

    ``review_feedback_allowlist`` is keyed by code host name: the code-host
    account ids whose review feedback is acted on when that code host cannot
    report write access (ADR 0197 authority 3). Account ids are only unique
    within one code host, so the list is never shared across hosts.
    """

    name: str
    tracker: str
    tracker_scope_id: str = Field(min_length=1)
    repos: tuple[RepositoryBindingConfig, ...] = Field(min_length=1)
    default_repo: str | None = None
    components: dict[str, str] = Field(default_factory=dict)
    start_authority: StartAuthority = Field(default_factory=WriteCheck)
    review_feedback_allowlist: dict[str, frozenset[str]] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def check_name(cls, value: str) -> str:
        return _name(value, "binding name")

    @model_validator(mode="after")
    def check_repos(self) -> Self:
        aliases = [repo.alias for repo in self.repos]
        duplicated = _duplicates(aliases)
        if duplicated:
            raise ValueError(f"binding {self.name!r} repeats aliases {duplicated}")
        identities = [f"{repo.code_host}\0{repo.project_id}" for repo in self.repos]
        if _duplicates(identities):
            raise ValueError(f"binding {self.name!r} lists one repository under two aliases")
        if self.default_repo is not None and self.default_repo not in aliases:
            raise ValueError(
                f"binding {self.name!r} default_repo {self.default_repo!r} is not one of its repos"
            )
        unknown = sorted(
            f"{component}->{alias}"
            for component, alias in self.components.items()
            if alias not in aliases
        )
        if unknown:
            raise ValueError(f"binding {self.name!r} maps components to unknown aliases {unknown}")
        return self

    def repo(self, alias: str) -> RepositoryBindingConfig | None:
        return next((repo for repo in self.repos if repo.alias == alias), None)


class ForgesConfig(_Model):
    """Every code host, tracker and binding of one deployment.

    ``card_base_url`` is the public origin a forge's image proxy fetches the
    live status card from; empty omits the card image.
    """

    code_hosts: tuple[CodeHostConfig, ...] = ()
    trackers: tuple[TrackerConfig, ...] = ()
    bindings: tuple[BindingConfig, ...] = ()
    card_base_url: str = ""

    @field_validator("card_base_url")
    @classmethod
    def check_card_base_url(cls, value: str) -> str:
        value = value.strip()
        return _api_url(value) if value else ""

    @model_validator(mode="after")
    def check_references(self) -> Self:
        for what, names in (
            ("code host", [host.name for host in self.code_hosts]),
            ("tracker", [tracker.name for tracker in self.trackers]),
            ("binding", [binding.name for binding in self.bindings]),
        ):
            duplicated = _duplicates(names)
            if duplicated:
                raise ValueError(f"{what} names are not unique: {duplicated}")
        code_hosts = {host.name: host for host in self.code_hosts}
        trackers = {tracker.name: tracker for tracker in self.trackers}
        scopes: dict[tuple[str, str, str], str] = {}
        for binding in self.bindings:
            tracker = trackers.get(binding.tracker)
            if tracker is None:
                raise ValueError(
                    f"binding {binding.name!r} names unknown tracker {binding.tracker!r}"
                )
            scope = (tracker.kind, tracker.host, binding.tracker_scope_id)
            if scope in scopes:
                raise ValueError(
                    f"bindings {scopes[scope]!r} and {binding.name!r} claim the same tracker scope"
                )
            scopes[scope] = binding.name
            _check_binding(binding, tracker, code_hosts)
        return self

    def tracker(self, name: str) -> TrackerConfig:
        return next(tracker for tracker in self.trackers if tracker.name == name)

    def code_host(self, name: str) -> CodeHostConfig:
        return next(host for host in self.code_hosts if host.name == name)


def _check_binding(
    binding: BindingConfig, tracker: TrackerConfig, code_hosts: dict[str, CodeHostConfig]
) -> None:
    label = f"binding {binding.name!r}"
    used_hosts: set[str] = set()
    for repo in binding.repos:
        host = code_hosts.get(repo.code_host)
        if host is None:
            raise ValueError(
                f"{label} repo {repo.alias!r} names unknown code host {repo.code_host!r}"
            )
        used_hosts.add(host.name)
        if not valid_repository_path(host.kind, repo.path):
            shape = "owner/name" if host.kind == types.GITHUB else "two or more segments"
            raise ValueError(f"{label} repo {repo.alias!r} path {repo.path!r} is not {shape}")
        try:
            mandatory_capabilities(tracker.kind, host.kind)
        except InvalidPairing as refused:
            raise ValueError(f"{label}: {refused.reason}") from None
        if tracker.native and host.host != tracker.host:
            raise ValueError(
                f"{label}: a {tracker.kind} tracker on {tracker.host} pairs only with a code"
                f" host on {tracker.host}, not {host.host}"
            )
    unknown_hosts = sorted(set(binding.review_feedback_allowlist) - used_hosts)
    if unknown_hosts:
        raise ValueError(
            f"{label} review_feedback_allowlist names code hosts it binds no repo on: "
            f"{unknown_hosts}"
        )
    if any(not ids or not all(ids) for ids in binding.review_feedback_allowlist.values()):
        raise ValueError(f"{label} review_feedback_allowlist entries must name account ids")
    if tracker.native:
        _check_native(binding, tracker)
    elif binding.default_repo is None:
        raise ValueError(f"{label} must name a default_repo")
    elif isinstance(binding.start_authority, WriteCheck):
        raise ValueError(
            f"{label}: a {tracker.kind} tracker cannot check repository write access; "
            "start authority must be an allowlist or a group"
        )


def _check_native(binding: BindingConfig, tracker: TrackerConfig) -> None:
    """A native tracker's repository is its own project (ADR 0197 identity 3),
    and it keeps the forge write check (authority 1)."""

    label = f"binding {binding.name!r}"
    if len(binding.repos) != 1:
        raise ValueError(f"{label}: a {tracker.kind} tracker binds exactly its own repository")
    (repo,) = binding.repos
    if repo.project_id != binding.tracker_scope_id:
        raise ValueError(
            f"{label}: a {tracker.kind} tracker's scope is its repository, so"
            f" tracker_scope_id must equal project_id {repo.project_id!r}"
        )
    if binding.components:
        raise ValueError(f"{label}: a {tracker.kind} tracker has no component map")
    if not isinstance(binding.start_authority, WriteCheck):
        raise ValueError(f"{label}: a {tracker.kind} tracker admits by its write check only")

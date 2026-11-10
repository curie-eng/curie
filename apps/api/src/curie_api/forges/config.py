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
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    SecretStr,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    field_validator,
    model_validator,
)
from pydantic_core import InitErrorDetails, PydanticCustomError

from curie_api.branches import base_allowed, valid_base_pattern
from curie_api.forges import types
from curie_api.forges.capabilities import NATIVE_FORGES, mandatory_capabilities
from curie_api.forges.errors import CredentialUnreadable, InvalidPairing
from curie_api.forges.paths import valid_repository_path
from curie_api.forges.runner_config import DockerConfig, RunnerConfig
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
    # Only the outermost model's setting counts, and a refusal would otherwise
    # echo the raw input, secrets included, as ``input_value``.
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


def valid_config_name(value: str) -> bool:
    """Whether ``value`` is a config name: a binding, tracker or code host name,
    or a repository alias. A ``jira=`` surface names a binding by it."""

    return bool(_NAME.fullmatch(value))


def _name(value: str, what: str) -> str:
    if not valid_config_name(value):
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

# What surrounds a static token in a file or a pasted value: ASCII whitespace
# only, so a token is never altered by Unicode rules (#4623).
_ASCII_WHITESPACE = " \t\n\r\x0b\x0c"
_UNSAFE_IN_HEADER = re.compile(r"[\x00-\x1f\x7f]")
# The input a rebuilt credential refusal carries in place of the value.
_MASK = SecretStr("**********")


class StaticTokenRefused(ValueError):
    """A static token that cannot be sent. ``problem`` never names the value."""

    def __init__(self, problem: str) -> None:
        self.problem = problem
        super().__init__(f"a static token {problem}")


def header_safe(value: str) -> bool:
    """Whether ``value`` carries no line break, other ASCII control character or
    DEL, so it can go in an HTTP header or git credential as it is."""

    return _UNSAFE_IN_HEADER.search(value) is None


def normalize_static_token(value: str) -> str:
    """``value`` without its surrounding ASCII whitespace.

    Refuses an empty result and one that still carries a line break, another
    ASCII control character or DEL, which no HTTP header or git credential may.
    """

    token = value.strip(_ASCII_WHITESPACE)
    if not token:
        raise StaticTokenRefused("must be non-empty")
    if not header_safe(token):
        raise StaticTokenRefused("contains a line break or control character")
    return token


def read_static_token(path: str) -> str:
    """The static token in the file at ``path`` now, normalized.

    Read on every call, so a replaced file (a Kubernetes Secret volume's
    ``..data`` swap included) is what the next caller gets; nothing is cached.
    `CredentialUnreadable` when the file is missing, unreadable, empty or
    malformed; its text is fixed and never carries the content (#4630).
    """

    problem: str | None = None
    content = ""
    try:
        content = Path(path).read_text()
    except (OSError, UnicodeDecodeError):
        # Decided outside the handler: a decode error holds the file's bytes.
        problem = "cannot be read"
    if problem is None and not content.strip():
        problem = "is empty"
    if problem is None:
        try:
            return normalize_static_token(content)
        except StaticTokenRefused as refused:
            problem = refused.problem
    raise CredentialUnreadable(problem)


def _masked(refused: ValidationError, *, entry: str | None) -> ValidationError:
    """``refused`` rebuilt with every error's input masked.

    pydantic's ``errors()`` and ``json()`` carry the raw input whatever
    ``hide_input_in_errors`` says, and a model holding a secret has it in that
    input. Each error keeps its location, type and message. A static-token
    refusal is typed ``static_token``; given ``entry``, its message names it.
    Raise the result outside the ``except`` block, so no context keeps the
    original.
    """

    line_errors: list[InitErrorDetails] = []
    for error in refused.errors(include_url=False, include_input=False):
        context = error.get("ctx") or {}
        cause = context.get("error")
        if isinstance(cause, StaticTokenRefused):
            problem: str | None = cause.problem
            message = f"a static token {cause.problem}"
        elif error["type"] == "static_token":
            problem, message = context["problem"], error["msg"]
        else:
            problem, message = None, error["msg"]
        if problem is None:
            kind = PydanticCustomError(error["type"], "{message}", {"message": message})
        else:
            if entry is not None:
                message = f"{entry} static token {problem}"
            kind = PydanticCustomError(
                "static_token", "{message}", {"message": message, "problem": problem}
            )
        line_errors.append({"type": kind, "loc": error["loc"], "input": _MASK})
    return ValidationError.from_exception_data(refused.title, line_errors)


def _masking[M](value: object, handler: Callable[[object], M]) -> M:
    """``handler(value)``, any refusal re-raised with its input masked.

    A model holding a secret, or holding one that does, validates through this
    from a ``mode="wrap"`` model validator declared after its other validators,
    so it wraps all of them, also when the model is built on its own.
    """

    try:
        return handler(value)
    except ValidationError as refused:
        masked = _masked(refused, entry=None)
    # Raised outside the handler so no context keeps the original error.
    raise masked


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

    # Declared last, so it wraps every check above (see `_masking`).
    @model_validator(mode="wrap")
    @classmethod
    def mask_refusal(cls, value: object, handler: ModelWrapValidatorHandler[Self]) -> Self:
        return _masking(value, handler)


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

    # Declared last, so it wraps every check above (see `_masking`).
    @model_validator(mode="wrap")
    @classmethod
    def mask_refusal(cls, value: object, handler: ModelWrapValidatorHandler[Self]) -> Self:
        return _masking(value, handler)


class StaticTokenCredentialConfig(_Model):
    """A long-lived token an operator rotates.

    Exactly one source: ``token_file``, the file the chart mounts from a Secret
    and the only source ``CURIE_FORGES`` accepts, or ``token``, an inline value
    for programmatic use. The file is read again by every `current_token` call,
    so a rotated Secret is used at the next operation with no restart (#4630).

    ``expires_at`` set: a known expiry. ``never_expires``: the token was issued
    without one. Neither: the forge does not report it, so the expiry is unknown.
    ``username`` is set for forges whose git credential is basic auth.
    """

    type: Literal["static_token"] = "static_token"
    token: SecretStr | None = None
    token_file: str | None = Field(default=None, min_length=1)
    username: str | None = Field(default=None, min_length=1)
    expires_at: AwareDatetime | None = None
    never_expires: bool = False

    @field_validator("token")
    @classmethod
    def check_token(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        return SecretStr(normalize_static_token(value.get_secret_value()))

    @model_validator(mode="after")
    def check_source(self) -> Self:
        if (self.token is None) == (self.token_file is None):
            raise StaticTokenRefused("names exactly one of token or token_file")
        return self

    @model_validator(mode="after")
    def check_expiry(self) -> Self:
        if self.expires_at is not None and self.never_expires:
            raise StaticTokenRefused("has an expires_at or never_expires, not both")
        return self

    def current_token(self) -> str:
        """The token to send now: the inline value, or the file read again now.

        `CredentialUnreadable` when the file cannot be sent; never an earlier value.
        """

        if self.token is not None:
            return self.token.get_secret_value()
        assert self.token_file is not None
        return read_static_token(self.token_file)

    # Declared last, so it wraps every check above (see `_masking`).
    @model_validator(mode="wrap")
    @classmethod
    def mask_refusal(cls, value: object, handler: ModelWrapValidatorHandler[Self]) -> Self:
        return _masking(value, handler)


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


def _named_credential(
    what: str, value: object, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
) -> CredentialConfig:
    """Validate a credential. Every refusal comes out with its input masked,
    and a static-token refusal names the entry."""

    try:
        credential: CredentialConfig = handler(value)
        return credential
    except ValidationError as refused:
        name = info.data.get("name")
        masked = _masked(
            refused, entry=f"{what} {name!r}" if isinstance(name, str) else f"{what} unnamed"
        )
    # Raised outside the handler so the refusal keeps no inner error as context.
    raise masked


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

    @field_validator("credential", mode="wrap")
    @classmethod
    def name_credential(
        cls, value: object, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> CredentialConfig:
        return _named_credential("code host", value, handler, info)

    @model_validator(mode="after")
    def check_credential(self) -> Self:
        _check_credential(self.kind, self.credential)
        return self

    # Declared last, so it wraps every check above (see `_masking`).
    @model_validator(mode="wrap")
    @classmethod
    def mask_refusal(cls, value: object, handler: ModelWrapValidatorHandler[Self]) -> Self:
        return _masking(value, handler)


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

    @field_validator("credential", mode="wrap")
    @classmethod
    def name_credential(
        cls, value: object, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> CredentialConfig:
        return _named_credential("tracker", value, handler, info)

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

    # Declared last, so it wraps every check above (see `_masking`).
    @model_validator(mode="wrap")
    @classmethod
    def mask_refusal(cls, value: object, handler: ModelWrapValidatorHandler[Self]) -> Self:
        return _masking(value, handler)

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
    ``bases`` are case-sensitive branch globs (a ``base:`` label picks a literal
    branch); empty means the repository's default branch only. ``default_base``
    is a matching literal branch used when no label picks one.
    """

    alias: str
    code_host: str
    project_id: str = Field(min_length=1)
    path: str
    bases: tuple[str, ...] = ()
    default_base: str | None = None
    ci: CiPolicyConfig = Field(default_factory=CiPolicyConfig)
    runner: RunnerConfig | None = None
    docker: DockerConfig | None = None

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
        if not all(valid_base_pattern(base) for base in self.bases) or _duplicates(self.bases):
            raise ValueError(
                f"repository {self.alias!r} bases must be unique branch names or glob patterns"
            )
        if self.default_base is not None and not base_allowed(
            self.default_base, self.bases, default_branch=None
        ):
            raise ValueError(
                f"repository {self.alias!r} default_base must be one of its bases"
                " or match a base pattern"
            )
        if self.docker is not None and self.docker.enabled:
            if self.runner is None or "docker-cli" not in self.runner.tools:
                raise ValueError(
                    f"repository {self.alias!r} docker.enabled requires runner tool docker-cli"
                )
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
        _check_one_entry_per_code_host(self.code_hosts)
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
        _check_github_pairing(self.trackers, self.code_hosts)
        _check_one_github_app(self.trackers, self.code_hosts)
        _check_repository_runner_configs(self.bindings, code_hosts)
        return self

    # Declared last, so it wraps every check above (see `_masking`).
    @model_validator(mode="wrap")
    @classmethod
    def mask_refusal(cls, value: object, handler: ModelWrapValidatorHandler[Self]) -> Self:
        return _masking(value, handler)

    def tracker(self, name: str) -> TrackerConfig:
        return next(tracker for tracker in self.trackers if tracker.name == name)

    def code_host(self, name: str) -> CodeHostConfig:
        return next(host for host in self.code_hosts if host.name == name)


def _check_one_entry_per_code_host(code_hosts: tuple[CodeHostConfig, ...]) -> None:
    """One code host entry per (kind, host).

    A stored repository records only its code host's kind and host, so that
    pair must name exactly one entry and one credential (#4358 plan D14).
    """

    seen: dict[tuple[str, str], str] = {}
    for host in code_hosts:
        identity = (host.kind, host.host)
        if identity in seen:
            raise ValueError(
                f"code hosts {seen[identity]!r} and {host.name!r} are both {host.kind} on"
                f" {host.host}: configure one code host entry per host"
            )
        seen[identity] = host.name


def _check_repository_runner_configs(
    bindings: tuple[BindingConfig, ...], code_hosts: dict[str, CodeHostConfig]
) -> None:
    """One resolved repository identity has one effective runner declaration.

    Activated declarations also need an unambiguous repository path for the
    deferred worker pool map. Legacy bindings without those declarations keep
    their existing cross-host path behavior.
    """

    declarations: dict[tuple[str, str, str], tuple[str, RunnerConfig | None, DockerConfig]] = {}
    paths: dict[str, tuple[set[tuple[str, str, str]], bool, str]] = {}
    for binding in bindings:
        for repo in binding.repos:
            host = code_hosts[repo.code_host]
            identity = (host.kind, host.host, repo.project_id)
            label = f"binding {binding.name!r} repository {repo.alias!r}"
            docker = repo.docker or DockerConfig(enabled=False)
            previous = declarations.get(identity)
            if previous is not None and (previous[1], previous[2]) != (repo.runner, docker):
                raise ValueError(
                    f"{previous[0]} and {label} have conflicting runner or docker declarations"
                )
            declarations[identity] = (label, repo.runner, docker)
            path = repo.path.lower() if host.kind == types.GITHUB else repo.path
            active = repo.runner is not None or docker.enabled or bool(docker.egress_allowlist)
            prior_path = paths.get(path)
            if prior_path is None:
                paths[path] = ({identity}, active, label)
                continue
            identities, previously_active, prior_label = prior_path
            if identities != {identity} and (active or previously_active):
                raise ValueError(
                    f"{prior_label} and {label} share path {path!r} across different repository"
                    " identities; a runner or docker declaration requires an unambiguous path"
                )
            identities.add(identity)
            paths[path] = (identities, active or previously_active, prior_label)


def _check_github_pairing(
    trackers: tuple[TrackerConfig, ...], code_hosts: tuple[CodeHostConfig, ...]
) -> None:
    """A GitHub tracker pairs only with a GitHub code host on its own host.

    Intake reads a GitHub tracker's issues with the GitHub code host's
    installation token, whether or not a binding names the pair, so a code host
    on another host would send that token to the tracker's API.
    """

    for tracker in trackers:
        if tracker.kind != types.GITHUB:
            continue
        for host in code_hosts:
            if host.kind == types.GITHUB and host.host != tracker.host:
                raise ValueError(
                    f"tracker {tracker.name!r} on {tracker.host} and code host {host.name!r}"
                    f" on {host.host}: a github tracker pairs only with a github code host"
                    f" on {tracker.host}"
                )


def _check_one_github_app(
    trackers: tuple[TrackerConfig, ...], code_hosts: tuple[CodeHostConfig, ...]
) -> None:
    """A GitHub tracker and a GitHub code host on one host act as one App.

    The factory recognizes its own issue and pull request comments by the App
    that wrote them, so two App ids on one host would make each side's marked
    comments look foreign to the other and break their dedupe.
    """

    for tracker in trackers:
        if tracker.kind != types.GITHUB or not isinstance(
            tracker.credential, GitHubAppCredentialConfig
        ):
            continue
        for host in code_hosts:
            if (
                host.kind == types.GITHUB
                and host.host == tracker.host
                and isinstance(host.credential, GitHubAppCredentialConfig)
                and host.credential.app_id != tracker.credential.app_id
            ):
                raise ValueError(
                    f"tracker {tracker.name!r} (GitHub App {tracker.credential.app_id}) and"
                    f" code host {host.name!r} (GitHub App {host.credential.app_id}) are both"
                    f" on {tracker.host} and must use the same GitHub App"
                )


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

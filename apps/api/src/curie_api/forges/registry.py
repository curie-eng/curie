"""The forge registry: one table of forge kinds and the builder of a deployment's
connections (ADR 0197, #4397).

Each kind package under ``curie_api.forges`` registers itself here, either with
its tracker and code host connection factories or as reserved and not
installed. ``build_registry`` turns a `ForgesConfig` into one connection per
config entry; it is the only way a factory code path gets a tracker, a code
host, a credential, a tracker label or mention, per-repository policy or the
card base URL. A config naming a kind this build does not install is refused,
naming the kind and the entry, never skipped.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from curie_api.forges import types
from curie_api.forges.config import (
    BindingConfig,
    CodeHostConfig,
    ForgesConfig,
    RepositoryBindingConfig,
    TrackerConfig,
)
from curie_api.forges.connections import BindingTrackerConnection
from curie_api.forges.credential_providers import ExpiryReport, expiry_for
from curie_api.forges.errors import Unavailable

if TYPE_CHECKING:
    from curie_api.config import Settings
    from curie_api.forges.connections import CodeHostConnection, TrackerConnection
    from curie_api.forges.github.connection import (
        GitHubCodeHostConnection,
        GitHubTrackerConnection,
    )

TrackerFactory = Callable[..., "TrackerConnection"]
CodeHostFactory = Callable[..., "CodeHostConnection"]


@dataclass(frozen=True)
class _Kind:
    tracker: TrackerFactory | None
    code_host: CodeHostFactory | None
    installed: bool


_KINDS: dict[str, _Kind] = {}


class NotInstalled(ValueError):
    """A config entry names a forge kind this build ships no adapter for."""

    def __init__(self, kind: str, *, entry: str) -> None:
        super().__init__(f"{entry} uses kind {kind!r}, which is not installed in this build")
        self.kind = kind


def register(
    kind: str, *, tracker: TrackerFactory | None, code_host: CodeHostFactory | None
) -> None:
    """Install ``kind`` with its connection factories. A kind registers once."""

    if kind in _KINDS:
        raise ValueError(f"forge kind {kind!r} is already registered")
    _KINDS[kind] = _Kind(tracker, code_host, installed=True)


def register_not_installed(kind: str) -> None:
    """Reserve ``kind``: the config model accepts it, and this build refuses it."""

    if kind in _KINDS:
        raise ValueError(f"forge kind {kind!r} is already registered")
    _KINDS[kind] = _Kind(None, None, installed=False)


def _factory(kind: str, role: str, entry: str) -> Callable[..., Any]:
    registered = _KINDS.get(kind)
    factory = None
    if registered is not None and registered.installed:
        factory = registered.tracker if role == "tracker" else registered.code_host
    if factory is None:
        raise NotInstalled(kind, entry=entry)
    return factory


def check_installed(config: ForgesConfig) -> None:
    """Refuse the first entry whose kind is not installed for its role."""

    for host in config.code_hosts:
        _factory(host.kind, "code_host", f"code host {host.name!r}")
    for tracker in config.trackers:
        _factory(tracker.kind, "tracker", f"tracker {tracker.name!r}")


@dataclass(frozen=True)
class CredentialReport:
    """One configured credential's expiry, for status and doctor output."""

    section: str
    name: str
    credential_type: str
    expiry: ExpiryReport


def _single_github(
    entries: tuple[CodeHostConfig, ...] | tuple[TrackerConfig, ...], what: str
) -> None:
    # A `github=owner/name` surface carries no host, so two GitHub entries of
    # one role would make routing ambiguous.
    names = [entry.name for entry in entries if entry.kind == types.GITHUB]
    if len(names) > 1:
        raise ValueError(f"only one github {what} may be configured, got {', '.join(names)}")


class ForgeRegistry:
    """The connections built from one `ForgesConfig`, one per config entry."""

    def __init__(self, config: ForgesConfig, *, ca_bundle: str = "") -> None:
        check_installed(config)
        _single_github(config.trackers, "tracker")
        _single_github(config.code_hosts, "code host")
        self.config = config
        self._trackers: dict[str, TrackerConnection] = {
            tracker.name: _factory(tracker.kind, "tracker", tracker.name)(
                tracker, ca_bundle=ca_bundle
            )
            for tracker in config.trackers
        }
        self._code_hosts: dict[str, CodeHostConnection] = {
            host.name: _factory(host.kind, "code_host", host.name)(host, ca_bundle=ca_bundle)
            for host in config.code_hosts
        }

    @property
    def card_base_url(self) -> str:
        return self.config.card_base_url

    def tracker(self, name: str) -> TrackerConnection:
        return self._trackers[name]

    def binding_tracker(self, name: str) -> BindingTrackerConnection:
        """The tracker ``name`` when one binding scopes it (Jira Cloud, #4358 plan D13).

        A repository-scoped tracker (GitHub) is refused with
        ``Unavailable("tracker_not_binding_scoped")``: its trackers come from
        ``for_repository``.
        """

        connection = self._trackers[name]
        if not isinstance(connection, BindingTrackerConnection):
            raise Unavailable("tracker_not_binding_scoped")
        return connection

    def code_host(self, name: str) -> CodeHostConnection:
        return self._code_hosts[name]

    def code_host_for(self, kind: str, host: str) -> CodeHostConnection:
        """The code host entry of ``kind`` on ``host``, the pair a stored
        repository records (#4358 plan D14 item 1).

        `_single_github` makes the GitHub answer unique; with none configured
        the lookup is ``Unavailable("code_host_unconfigured")``.
        """

        found = next(
            (entry for entry in self.config.code_hosts if (entry.kind, entry.host) == (kind, host)),
            None,
        )
        if found is None:
            raise Unavailable("code_host_unconfigured")
        return self.code_host(found.name)

    def binding(self, name: str) -> BindingConfig:
        return next(binding for binding in self.config.bindings if binding.name == name)

    def github_tracker(self) -> GitHubTrackerConnection | None:
        """The one GitHub tracker, or None when none is configured."""

        found = next((t for t in self.config.trackers if t.kind == types.GITHUB), None)
        return None if found is None else cast("GitHubTrackerConnection", self.tracker(found.name))

    def github_code_host(self) -> GitHubCodeHostConnection | None:
        """The one GitHub code host, or None when none is configured."""

        found = next((h for h in self.config.code_hosts if h.kind == types.GITHUB), None)
        return (
            None if found is None else cast("GitHubCodeHostConnection", self.code_host(found.name))
        )

    def repository_policy(
        self, kind: str, host: str, path: str, project_id: str | None
    ) -> RepositoryBindingConfig | None:
        """The binding entry of one repository: matched by its immutable id
        first, so a renamed repository keeps its policy, then by path
        case-insensitively."""

        candidates = [
            repo
            for binding in self.config.bindings
            for repo in binding.repos
            if (code_host := self.config.code_host(repo.code_host)).kind == kind
            and code_host.host == host
        ]
        if project_id is not None:
            by_id = next((repo for repo in candidates if repo.project_id == project_id), None)
            if by_id is not None:
                return by_id
        folded = path.casefold()
        return next((repo for repo in candidates if repo.path.casefold() == folded), None)

    def github_repository_policy(
        self, path: str, project_id: str | None = None
    ) -> RepositoryBindingConfig | None:
        """`repository_policy` on the configured GitHub code host."""

        code_host = self.github_code_host()
        if code_host is None:
            return None
        return self.repository_policy(types.GITHUB, code_host.config.host, path, project_id)

    def credential_reports(self) -> list[CredentialReport]:
        return [
            CredentialReport(
                section, entry.name, entry.credential.type, expiry_for(entry.credential)
            )
            for section, entries in (
                ("code_host", self.config.code_hosts),
                ("tracker", self.config.trackers),
            )
            for entry in entries
        ]


def build_registry(config: ForgesConfig, *, ca_bundle: str = "") -> ForgeRegistry:
    """The registry of ``config``. ``ca_bundle`` is the install's code host CA."""

    return ForgeRegistry(config, ca_bundle=ca_bundle)


_REGISTRIES: OrderedDict[tuple[int, str], tuple[ForgesConfig, ForgeRegistry]] = OrderedDict()
_REGISTRY_CACHE_LIMIT = 32


def forges_registry(settings: Settings) -> ForgeRegistry:
    """The registry of ``settings.forges``, built once per config object.

    Keyed by the config's identity, which the cache holds, so the key cannot be
    reused by another object while the entry lives.
    """

    config = settings.forges
    key = (id(config), settings.code_host_ca_bundle)
    cached = _REGISTRIES.get(key)
    if cached is not None and cached[0] is config:
        _REGISTRIES.move_to_end(key)
        return cached[1]
    registry = build_registry(config, ca_bundle=settings.code_host_ca_bundle)
    _REGISTRIES[key] = (config, registry)
    while len(_REGISTRIES) > _REGISTRY_CACHE_LIMIT:
        _REGISTRIES.popitem(last=False)
    return registry


# Every kind package registers itself on import, so the table is complete
# whichever module imports the registry first.
from curie_api.forges import (  # noqa: E402, F401  registers each kind
    bitbucket_cloud,
    bitbucket_dc,
    github,
    gitlab,
    jira_cloud,
)

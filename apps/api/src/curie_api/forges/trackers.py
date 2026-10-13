"""Which tracker, code host, reply route and issue link a WorkItem has
(#4358 plan D4, D14).

The factory core asks here instead of reaching for a GitHub accessor. The
answer comes from the issue's tracker kind and the repository's stored code
host entry: GitHub keeps today's values byte for byte, a Jira issue is
addressed by the config binding serving its site scope, and a
repository-addressed kind (GitLab) needs only its connection registered.
"""

from __future__ import annotations

from collections.abc import Mapping

import httpx

from curie_api.config import Settings
from curie_api.forges import surfaces, types
from curie_api.forges.config import BindingConfig, RepositoryBindingConfig
from curie_api.forges.connections import RepositoryTrackerConnection
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.hosts import repository_ref
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.registry import ForgeRegistry
from curie_api.forges.types import RepositoryRef, TrackerIssueRef

_GITHUB_COMMENT_PREFIX = "#issuecomment-"
_FOCUSED_COMMENT_PREFIX = "?focusedCommentId="
_COMMENT_PREFIXES = {types.GITHUB: _GITHUB_COMMENT_PREFIX, types.GITLAB: "#note_"}
_GITLAB_NOTE_EVENT = "note:"


def binding_for_scope(registry: ForgeRegistry, issue: TrackerIssueRef) -> BindingConfig | None:
    """The config binding whose tracker has the issue's kind and host and whose
    scope is the issue's scope, or None."""

    config = registry.config
    for binding in config.bindings:
        tracker = config.tracker(binding.tracker)
        if (
            tracker.kind == issue.kind
            and tracker.host == issue.host
            and binding.tracker_scope_id == issue.scope_id
        ):
            return binding
    return None


def work_item_policy(
    registry: ForgeRegistry, issue: TrackerIssueRef, repository: RepositoryRef
) -> RepositoryBindingConfig | None:
    """The repository policy (bases, CI) a WorkItem on ``issue`` runs under
    (#4358 plan D15, r1 finding 4).

    A GitHub issue keeps today's answer, ``repository``'s entry on the GitHub
    code host. Any other issue reads ``repository``'s entry in the binding
    serving the issue's scope, on whichever code host the repository records;
    None when no binding serves the scope or the binding does not list it.
    """

    if issue.kind == types.GITHUB:
        return registry.github_repository_policy(repository.path)
    binding = binding_for_scope(registry, issue)
    if binding is None:
        return None
    return registry.binding_repository_policy(binding, repository)


def tracker_for_binding(
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    binding: BindingConfig,
    *,
    label: str | None = None,
) -> Tracker:
    """The tracker on ``binding``'s scope; ``label`` replaces the entry's label.

    Every non-GitHub tracker kind is binding scoped: the registry refuses at
    config load a kind whose connection is not (`registry.check_tracker_scope`).
    A GitHub binding raises ``Unavailable("tracker_not_binding_scoped")``.
    """

    return registry.binding_tracker(binding.tracker).for_binding(client, binding, label=label)


def _repository_tracker(
    registry: ForgeRegistry, issue: TrackerIssueRef
) -> RepositoryTrackerConnection | None:
    """The repository-scoped tracker entry of the issue's kind and host, if any."""

    for entry in registry.config.trackers:
        if entry.kind != issue.kind or entry.host != issue.host:
            continue
        connection = registry.tracker(entry.name)
        if isinstance(connection, RepositoryTrackerConnection):
            return connection
    return None


def tracker_for_work_item(
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    issue: TrackerIssueRef,
    repository: RepositoryRef,
    installation_id: int | None,
    *,
    timeout_s: float | None = None,
) -> Tracker:
    """The tracker a WorkItem's ``issue`` lives on.

    A repository-scoped issue (GitHub) gets the tracker on ``repository``,
    whose token is minted only when a request is made, for the recorded
    ``installation_id`` alone (`CodeHostConnection.installation_token`, the
    fence the issue read has always kept), with the provider timeout capped at
    ``timeout_s``. Naming the issue (`issue_url`, `closing_reference`) mints
    nothing. A WorkItem with no recorded installation raises
    ``Unavailable("installation")`` on first use. Any other kind gets the
    tracker of the binding serving the issue's scope, ``NotFound("binding")``
    when none does. ``NotFound("tracker")`` when no configured tracker has the
    issue's kind and host.
    """

    if not any(
        entry.kind == issue.kind and entry.host == issue.host for entry in registry.config.trackers
    ):
        raise NotFound("tracker")
    connection = _repository_tracker(registry, issue)
    if connection is not None:

        async def token() -> str:
            if installation_id is None:
                raise Unavailable("installation")
            code_host = registry.code_host_for(repository.kind, repository.host)
            return await code_host.installation_token(
                repository, installation_id, timeout_s=timeout_s
            )

        return connection.for_repository(
            client,
            repo_full_name=repository.path,
            repository_id=int(issue.scope_id),
            token=token,
        )
    binding = binding_for_scope(registry, issue)
    if binding is None:
        raise NotFound("binding")
    return tracker_for_binding(registry, client, binding)


def code_host_for_repository(
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    repository: RepositoryRef,
    *,
    installations: Mapping[str, int] | None = None,
) -> CodeHost:
    """The code host adapter of the entry ``repository`` records (its kind and
    host), with ``repository``'s pull request comments.
    ``Unavailable("code_host_unconfigured")`` when no entry matches."""

    connection = registry.code_host_for(repository.kind, repository.host)
    return connection.adapter(client, installations=installations, repository=repository)


def stored_repository(
    settings: Settings,
    *,
    kind: str | None,
    host: str | None,
    path: str,
    project_id: int | str | None,
) -> RepositoryRef:
    """A repository as a stored row records it (#4358 plan D14 item 5).

    A row with its typed code host columns set names that entry. A row with
    both NULL was written before typed code hosts, when the install's GitHub
    was the only code host, and names it.
    """

    stored = repository_ref(settings, path=path, project_id=project_id)
    if kind is None or host is None:
        return stored
    return RepositoryRef(kind, host, stored.project_id, path)


def issue_link(registry: ForgeRegistry, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
    """The issue's web link, with no request.

    A GitHub issue is on its repository, under the factory GitHub code host's
    web origin: the exact text GitHub admission has always written. Any other
    kind is linked by the tracker of the binding serving its scope.
    """

    if issue.kind == types.GITHUB:
        connection = registry.github_code_host()
        if connection is None:
            raise Unavailable("code_host_unconfigured")
        return f"{connection.html_base.rstrip('/')}/{repository.path}/issues/{issue.issue_id}"
    binding = binding_for_scope(registry, issue)
    if binding is None:
        raise NotFound("binding")
    return registry.binding_tracker(binding.tracker).issue_url(issue)


def comment_prefix(kind: str) -> str:
    """What joins an issue link and one of its comment ids, per tracker kind:
    GitHub's ``#issuecomment-``, GitLab's ``#note_`` anchor, else Jira's
    ``?focusedCommentId=``."""

    return _COMMENT_PREFIXES.get(kind, _FOCUSED_COMMENT_PREFIX)


def mention_comment_id(kind: str, event_id: str) -> str:
    """The comment id inside a mention notice's ``event_id``.

    A GitLab mention's event id is ``note:<id>`` (its tracker keys label events
    and notes apart); every other kind's is the comment id itself.
    """

    if kind == types.GITLAB:
        return event_id.removeprefix(_GITLAB_NOTE_EVENT)
    return event_id


def mention_objective(link: str, kind: str, comment_id: str) -> str:
    """The link to one comment on the issue at ``link``: a mention's objective."""

    return f"{link}{comment_prefix(kind)}{comment_id}"


def reply_route(
    registry: ForgeRegistry, issue: TrackerIssueRef, repository_path: str
) -> tuple[str, str, str]:
    """The reply kind, address and conversation id an issue replies on.

    GitHub: ``("github", <owner/name>, "issue-<n>")``; Jira: ``("jira",
    <binding name>, "issue-<id>")``; a repository-addressed kind its surface
    kind and project path.
    """

    kind, address = surfaces.surface_location(registry, issue, repository_path)
    return kind, address, f"issue-{issue.issue_id}"


def display_reference(issue: TrackerIssueRef, repository_path: str) -> str:
    """How an issue is named to a person: ``owner/name#<n>`` on GitHub, else its
    key (``PROJ-12``), else its id."""

    if issue.kind == types.GITHUB:
        return f"{repository_path}#{issue.issue_id}"
    return issue.display_key or issue.issue_id

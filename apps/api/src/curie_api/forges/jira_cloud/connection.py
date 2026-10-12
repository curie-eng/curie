"""The Jira Cloud tracker connection the forge registry builds (#4358).

One configured ``jira_cloud`` tracker entry: its site ``host``, its API
(``https://<site>.atlassian.net`` for a classic token or
``https://api.atlassian.com/ex/jira/<cloudId>`` for a scoped one, decision 2),
its ``static_token`` credential (the bot's Atlassian account email as
``username`` and its API token), label and ``mention`` (the bot's accountId).
It is binding scoped (plan D13): `for_binding` yields the tracker for one
binding's cloudId and start authority. The identity it verifies (decision 3)
is cached here, so it is read once per connection rather than per tracker.
"""

from __future__ import annotations

from urllib.parse import urlsplit

import httpx

from curie_api.forges.config import BindingConfig, StaticTokenCredentialConfig, TrackerConfig
from curie_api.forges.jira_cloud.tracker import IdentityCache, JiraTracker, browse_url
from curie_api.forges.jira_cloud.transport import JiraTransport
from curie_api.forges.types import TrackerIssueRef


def site_origin(api_url: str, host: str) -> str:
    """The site's web origin: the API's scheme when the API is the site itself
    (a classic token, or a local fake), else ``https://<host>``."""

    parts = urlsplit(api_url)
    scheme = parts.scheme if parts.netloc == host else "https"
    return f"{scheme}://{host}"


class JiraTrackerConnection:
    """One configured Jira Cloud tracker: yields a `JiraTracker` per binding."""

    def __init__(self, config: TrackerConfig, *, ca_bundle: str = "") -> None:
        # ``ca_bundle`` is the install's code host CA; the caller's HTTP client
        # carries TLS settings, so a Jira connection reads nothing from it.
        credential = config.credential
        if not isinstance(credential, StaticTokenCredentialConfig):
            raise ValueError("jira_cloud supports static_token credentials only")
        if credential.username is None:
            raise ValueError(
                f"tracker {config.name!r} needs a username on its static_token credential:"
                " the bot's Atlassian account email, sent with the API token as basic auth"
            )
        self.config = config
        self._credential = credential
        self._username = credential.username
        self.site_origin = site_origin(config.api_url, config.host)
        self._identity = IdentityCache()

    def for_binding(
        self, client: httpx.AsyncClient, binding: BindingConfig, *, label: str | None = None
    ) -> JiraTracker:
        """The tracker on ``binding``'s site scope. ``label`` replaces the entry's label."""

        if binding.tracker != self.config.name:
            raise ValueError(f"binding {binding.name!r} is not bound to {self.config.name!r}")
        transport = JiraTransport(
            client,
            api_url=self.config.api_url,
            username=self._username,
            token=self._credential.token,
        )
        return JiraTracker(
            transport,
            host=self.config.host,
            site_origin=self.site_origin,
            scope_id=binding.tracker_scope_id,
            label=self.config.label if label is None else label,
            mention=self.config.mention,
            start_authority=binding.start_authority,
            identity=self._identity,
        )

    def issue_url(self, issue: TrackerIssueRef) -> str:
        """Decision 18: ``https://<host>/browse/<display_key>``, no request.
        `NotFound` when the issue has no key or is on another tracker."""

        return browse_url(self.site_origin, issue, host=self.config.host)

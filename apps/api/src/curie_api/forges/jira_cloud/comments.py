"""Jira status comments behind the MarkedComments port (#4358 decision 15).

ADF has no hidden text, so the core's marker cannot ride in the visible body as
it does on GitHub. It is stored instead as the comment entity property
``curie.factory`` with value ``{"marker": ..., "body_sha256": ...}``
(https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comment-properties/).
Ownership is that property plus the bot's own ``accountId``: a person who
copies the property onto a comment of theirs is neither found nor edited. The
``accountId`` is the tracker's configured ``mention``, so before any comment is
selected the site's cloudId and the token's accountId are verified against the
binding (decision 3, `curie_api.forges.jira_cloud.identity`); a token that is
not the bot is `Unauthorized` and edits nothing.

The body is converted to ADF (`curie_api.forges.jira_cloud.adf`). A body over
30,000 characters is cut at the last line boundary under that length and ends
with a note, keeping it inside Jira's 32,767 character comment limit. The
digest is of the core's whole body, so an unchanged body writes nothing.
Only `ReplyTarget` kind ``issue`` lives here; pull request targets belong to
the code host.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from curie_api.forges.capabilities import MARKED_COMMENT_OPERATIONS, Operation, Support
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.jira_cloud import adf
from curie_api.forges.jira_cloud.identity import IdentityCache, verify_identity
from curie_api.forges.jira_cloud.transport import MAX_PAGES, JiraTransport
from curie_api.forges.types import JIRA_CLOUD, MarkedComment, ReplyTarget, UpsertResult

PROPERTY_KEY = "curie.factory"
BODY_LIMIT = 30_000
TRUNCATION_NOTE = "_Status truncated to fit Jira's comment limit._"
# https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-comment-list-post
_LIST_IDS_LIMIT = 1000


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode()).hexdigest()


def fit(body: str) -> str:
    """``body`` cut at the last line boundary under `BODY_LIMIT`, with a note."""

    if len(body) <= BODY_LIMIT:
        return body
    head = body[:BODY_LIMIT]
    cut = head.rfind("\n")
    kept = (head[:cut] if cut > 0 else head).rstrip("\n")
    return f"{kept}\n\n{TRUNCATION_NOTE}\n"


@dataclass(frozen=True)
class _Found:
    comment: MarkedComment
    digest: str | None


class JiraMarkedComments:
    """Status comments on the issues of one Jira site scope, as the bot."""

    kind = JIRA_CLOUD
    capabilities: Mapping[Operation, Support] = dict.fromkeys(
        MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
    )

    def __init__(
        self,
        transport: JiraTransport,
        *,
        host: str,
        site_origin: str,
        scope_id: str,
        mention: str,
        identity: IdentityCache,
        page_size: int = 100,
    ) -> None:
        self._transport = transport
        self.host = host
        self._site_origin = site_origin
        self._scope_id = scope_id
        self._mention = mention
        self._identity = identity
        self._page_size = page_size

    def _issue_id(self, target: ReplyTarget) -> str:
        issue = target.issue
        if (
            target.kind != "issue"
            or issue is None
            or issue.kind != JIRA_CLOUD
            or issue.host != self.host
            or issue.scope_id != self._scope_id
            or not issue.issue_id.isdigit()
        ):
            raise NotFound("issue")
        return issue.issue_id

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        found = await self._find(target, marker)
        return None if found is None else found.comment

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        issue_id = self._issue_id(target)
        digest = body_sha256(body)
        document = adf.from_markdown(fit(body))
        value = {"marker": marker, "body_sha256": digest}
        found = await self._find(target, marker)
        if found is not None:
            comment = MarkedComment(id=found.comment.id, target=target, body=body)
            if found.digest == digest:
                return UpsertResult(comment=comment, written=False)
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-id-put
            await self._transport.put(
                f"/issue/{issue_id}/comment/{comment.id}",
                what="comment_update",
                params={"notifyUsers": "false"},
                json={"body": document},
            )
            await self._transport.put(
                f"/comment/{comment.id}/properties/{PROPERTY_KEY}",
                what="comment_property",
                json=value,
            )
            return UpsertResult(comment=comment, written=True)
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-post
        created = await self._transport.post(
            f"/issue/{issue_id}/comment",
            what="comment_create",
            json={"body": document, "properties": [{"key": PROPERTY_KEY, "value": value}]},
        )
        comment_id = created.get("id") if isinstance(created, dict) else None
        if not isinstance(comment_id, str | int) or not str(comment_id):
            raise Unavailable("comment_create")
        return UpsertResult(
            comment=MarkedComment(id=str(comment_id), target=target, body=body), written=True
        )

    async def _find(self, target: ReplyTarget, marker: str) -> _Found | None:
        """Our own comment carrying ``marker``, from a complete listing, once the
        token is verified to be the bot ``mention`` names."""

        issue_id = self._issue_id(target)
        await verify_identity(
            self._transport,
            self._identity,
            site_origin=self._site_origin,
            scope_id=self._scope_id,
            mention=self._mention,
        )
        comments, _total = await self._transport.comments(
            issue_id, newest_first=False, page_size=self._page_size
        )
        own = [
            str(comment.get("id"))
            for comment in comments
            if _account_id(comment.get("author")) == self._mention
            and str(comment.get("id")).isdigit()
        ]
        for start in range(0, len(own), _LIST_IDS_LIMIT):
            for listed in await self._with_properties(own[start : start + _LIST_IDS_LIMIT]):
                if _account_id(listed.get("author")) != self._mention:
                    continue
                stored = _property(listed)
                if stored is None or stored.get("marker") != marker:
                    continue
                digest = stored.get("body_sha256")
                body = listed.get("body")
                return _Found(
                    comment=MarkedComment(
                        id=str(listed.get("id")),
                        target=target,
                        body=adf.to_markdown(body if isinstance(body, Mapping) else None),
                    ),
                    digest=digest if isinstance(digest, str) else None,
                )
        return None

    async def _with_properties(self, ids: list[str]) -> list[dict[str, Any]]:
        """The comments ``ids`` with their entity properties, every page or `Unavailable`."""

        listed: list[dict[str, Any]] = []
        start = 0
        for _ in range(MAX_PAGES):
            page = await self._transport.post(
                "/comment/list",
                what="comment_list",
                params={"expand": "properties", "startAt": start, "maxResults": len(ids)},
                json={"ids": [int(comment_id) for comment_id in ids]},
            )
            values = page.get("values") if isinstance(page, dict) else None
            if not isinstance(values, list):
                raise Unavailable("comment_list")
            listed.extend(value for value in values if isinstance(value, dict))
            if page.get("isLast") is True:
                return listed
            if not values:
                raise Unavailable("comment_list")
            start += len(values)
        raise Unavailable("comment_list")


def _account_id(user: Any) -> str | None:
    if not isinstance(user, dict):
        return None
    account_id = user.get("accountId")
    return account_id if isinstance(account_id, str) else None


def _property(comment: Mapping[str, Any]) -> Mapping[str, Any] | None:
    properties = comment.get("properties")
    if not isinstance(properties, list):
        return None
    for entry in properties:
        if isinstance(entry, dict) and entry.get("key") == PROPERTY_KEY:
            value = entry.get("value")
            return value if isinstance(value, Mapping) else None
    return None

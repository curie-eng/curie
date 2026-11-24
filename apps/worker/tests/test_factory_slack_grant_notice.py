"""One Slack direct message when a personal Claude plan is rejected (#5720).

The production progress loop materializes ``factory_grant_notices`` and posts
through the production Slack reply adapter. Only the Web API is a stand-in.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from curie_internal.factory_cause import cause_text
from curie_worker.factory_slack_card import CardRequest, CardSubject, render_card
from sqlalchemy import text

from apps.api.tests.conftest import _disposable_db as _disposable_db
from apps.api.tests.conftest import clean_db as clean_db
from apps.api.tests.conftest import identity_fence_backend as identity_fence_backend
from apps.api.tests.conftest import identity_fence_key as identity_fence_key
from apps.api.tests.conftest import migrated as migrated
from apps.api.tests.test_factory_slack_runtime import (
    RECONNECT_URL,
    REPO,
    TEAM,
    Stack,
    Worker,
    _enroll,
    _grant_rows,
    _personal_failure,
    _rows,
    _seed_principal,
    _seed_receiving_identity,
    _stamp_title,
)
from apps.api.tests.test_factory_slack_runtime import stack as stack

_BUTTON = "Reconnect your Claude plan"


def _direct_messages(stack: Stack, slack_user: str) -> list[Any]:
    return [
        call
        for call in stack.slack.delivered_posts(stack.factory_token)
        if call.params.get("channel") == slack_user and not call.params.get("thread_ts")
    ]


def _button_urls(call: Any) -> list[str]:
    blocks = call.params.get("blocks")
    if isinstance(blocks, str):
        blocks = json.loads(blocks)
    if not isinstance(blocks, list):
        return []
    return [
        str(element.get("url"))
        for block in blocks
        if isinstance(block, dict) and block.get("type") == "actions"
        for element in block.get("elements") or []
        if isinstance(element, dict) and element.get("url")
    ]


def test_grant_notice_posts_once_to_the_owner_and_survives_a_restart(
    stack: Stack, caplog: pytest.LogCaptureFixture
) -> None:
    """The DM goes to the owner's Slack user id, once, including across a restart.

    An owner with no Slack link gets no post and one ``grant_notice_no_recipient``
    log line.
    """

    caplog.set_level(logging.INFO, logger="curie_worker.factory_slack_progress")
    slack_user = f"U0{stack.tag.upper()}GD"
    owner, _link = _seed_principal(slack_user)
    _enroll(owner)
    _seed_receiving_identity(stack.identity)
    number = _personal_failure(stack, owner, title="Fix the greeting")
    stack.drain()

    (posted,) = _direct_messages(stack, slack_user)
    issue_url = f"https://github.com/{REPO}/issues/{number}"
    assert posted.params["text"] == (
        "Your Claude plan was rejected when Curie tried to use it, so your factory run for "
        f"#{number} Fix the greeting stopped. The work is kept. "
        "The owner can reconnect or replace the enrollment on the Claude plan page if needed. "
        "To resume, the owner can remove and reapply the issue's trigger label, or mention the app "
        f"in the run's Slack thread if the run has an open pull request.\n{issue_url}"
    )
    assert _button_urls(posted) == [RECONNECT_URL]
    blocks = posted.params["blocks"]
    blocks = json.loads(blocks) if isinstance(blocks, str) else blocks
    labels = [
        element["text"]["text"]
        for block in blocks
        if block.get("type") == "actions"
        for element in block["elements"]
    ]
    assert _BUTTON in labels
    assert _grant_rows(owner)[0]["posted_at"] is not None

    stack.drain(fresh=True)
    assert len(_direct_messages(stack, slack_user)) == 1

    unlinked, _none = _seed_principal(None)
    _enroll(unlinked)
    _personal_failure(stack, unlinked, title="No link")
    stack.drain()
    stack.drain(fresh=True)

    assert _direct_messages(stack, slack_user) == [posted]
    (row,) = _grant_rows(unlinked)
    assert row["recipient"] is None
    assert row["posted_at"] is None
    assert row["failed_at"] is not None
    assert row["last_error"] == "grant_notice_no_recipient"
    assert row["attempts"] == 0
    logged = [
        record.message for record in caplog.records if record.message == "grant_notice_no_recipient"
    ]
    assert logged == ["grant_notice_no_recipient"]


async def _materialize(worker: Worker) -> None:
    await worker.progress._store.materialize()


def _materialize_grant(stack: Stack) -> None:
    stack.client.portal.call(_materialize, stack.worker)


def _revoke(link_id: uuid.UUID) -> None:
    updated = _rows(
        "UPDATE curie.identity_links SET revoked_at = now() WHERE id = :id RETURNING id",
        {"id": link_id},
    )
    assert updated


def _relink(old_link_id: uuid.UUID, slack_user: str) -> None:
    updated = _rows(
        "INSERT INTO curie.identity_links "
        "(id, tenant_id, identity_namespace_id, provider_native_id, principal_id, "
        " created_by_principal_id, verification_source) "
        "SELECT :id, tenant_id, identity_namespace_id, :native, principal_id, "
        " created_by_principal_id, verification_source "
        "FROM curie.identity_links WHERE id = :old RETURNING id",
        {"id": uuid.uuid4(), "native": slack_user, "old": old_link_id},
    )
    assert updated


def _clear_title(principal_id: uuid.UUID) -> uuid.UUID:
    updated = _rows(
        "UPDATE curie.factory_terminal_notices AS n SET subject_title = NULL "
        "FROM curie.execution_requests AS q "
        "JOIN curie.work_items AS w ON w.id = q.work_item_id "
        "WHERE n.execution_request_id = q.id AND w.owner_principal_id = :owner "
        "RETURNING n.execution_request_id",
        {"owner": principal_id},
    )
    assert len(updated) == 1
    request_id = updated[0]["execution_request_id"]
    assert isinstance(request_id, uuid.UUID)
    return request_id


def _release_grant(principal_id: uuid.UUID, *, attempts: int | None = None) -> None:
    _rows(
        "UPDATE curie.factory_grant_notices SET not_before = NULL, "
        "attempts = COALESCE(:attempts, attempts) WHERE principal_id = :owner",
        {"owner": principal_id, "attempts": attempts},
    )


def test_grant_notice_send_reads_the_link_again(stack: Stack) -> None:
    """A cached Slack user is not used after the link is revoked or replaced.

    The row is materialized while the link is active. Revoking it sends
    nothing. Replacing it sends to the new user. A disabled identity or a
    disconnected installation also sends nothing.
    """

    tag = stack.tag.upper()
    _seed_receiving_identity(stack.identity)

    revoked_user = f"U0{tag}RV"
    revoked_owner, revoked_link = _seed_principal(revoked_user)
    assert revoked_link is not None
    _enroll(revoked_owner)
    _personal_failure(stack, revoked_owner, title="Revoked")
    _materialize_grant(stack)
    assert _grant_rows(revoked_owner)[0]["recipient"] == revoked_user
    _revoke(revoked_link)
    stack.drain()
    assert _direct_messages(stack, revoked_user) == []
    (revoked_row,) = _grant_rows(revoked_owner)
    assert revoked_row["posted_at"] is None
    assert revoked_row["failed_at"] is not None
    assert revoked_row["last_error"] == "grant_notice_no_recipient"
    assert revoked_row["attempts"] == 0

    old_user = f"U0{tag}OLD"
    new_user = f"U0{tag}NEW"
    moved_owner, old_link = _seed_principal(old_user)
    assert old_link is not None
    _enroll(moved_owner)
    _personal_failure(stack, moved_owner, title="Reassigned")
    _materialize_grant(stack)
    assert _grant_rows(moved_owner)[0]["recipient"] == old_user
    _revoke(old_link)
    _relink(old_link, new_user)
    stack.drain()
    assert _direct_messages(stack, old_user) == []
    (moved,) = _direct_messages(stack, new_user)
    assert "Reassigned" in moved.params["text"]
    assert _grant_rows(moved_owner)[0]["posted_at"] is not None

    disabled_user = f"U0{tag}DS"
    disabled_owner, _disabled_link = _seed_principal(disabled_user)
    _enroll(disabled_owner)
    _personal_failure(stack, disabled_owner, title="Disabled")
    _materialize_grant(stack)
    assert _grant_rows(disabled_owner)[0]["recipient"] == disabled_user
    updated = _rows(
        "UPDATE curie.channel_identities SET status = 'disabled' WHERE name = :name RETURNING id",
        {"name": stack.identity},
    )
    assert updated
    stack.drain()
    assert _direct_messages(stack, disabled_user) == []
    assert _grant_rows(disabled_owner)[0]["last_error"] == "grant_notice_no_recipient"
    _rows(
        "UPDATE curie.channel_identities SET status = 'active' WHERE name = :name",
        {"name": stack.identity},
    )

    disconnected_user = f"U0{tag}DC"
    disconnected_owner, _disconnected_link = _seed_principal(disconnected_user)
    _enroll(disconnected_owner)
    _personal_failure(stack, disconnected_owner, title="Disconnected")
    _materialize_grant(stack)
    assert _grant_rows(disconnected_owner)[0]["recipient"] == disconnected_user
    updated = _rows(
        "UPDATE curie.provider_installations "
        "SET status = 'disconnected', disconnected_at = now() "
        "WHERE provider = 'slack' AND external_account_id = :team RETURNING id",
        {"team": TEAM},
    )
    assert updated
    stack.drain()
    assert _direct_messages(stack, disconnected_user) == []
    assert _grant_rows(disconnected_owner)[0]["last_error"] == "grant_notice_no_recipient"
    _rows(
        "UPDATE curie.provider_installations "
        "SET status = 'connected', disconnected_at = NULL "
        "WHERE provider = 'slack' AND external_account_id = :team",
        {"team": TEAM},
    )


def test_grant_notice_reads_the_title_when_it_sends(stack: Stack) -> None:
    """A title filled after the snapshot is in the DM. An empty title waits, then sends.

    The sentence never keeps a hole where the title would have been.
    """

    tag = stack.tag.upper()
    _seed_receiving_identity(stack.identity)
    waiting_user = f"U0{tag}TW"
    waiting_owner, _link = _seed_principal(waiting_user)
    _enroll(waiting_owner)
    number = _personal_failure(stack, waiting_owner, title="Fix the greeting")
    request_id = _clear_title(waiting_owner)

    stack.drain()
    assert _direct_messages(stack, waiting_user) == []
    (waiting,) = _grant_rows(waiting_owner)
    assert waiting["posted_at"] is None
    assert waiting["failed_at"] is None
    assert waiting["last_error"] == "grant_notice_title_pending"
    assert waiting["attempts"] == 1
    assert "title" not in waiting["payload"]

    _stamp_title(request_id, "Fix the greeting")
    _release_grant(waiting_owner)
    stack.drain()
    (posted,) = _direct_messages(stack, waiting_user)
    assert f"#{number} Fix the greeting stopped" in posted.params["text"]

    bare_user = f"U0{tag}TB"
    bare_owner, _bare_link = _seed_principal(bare_user)
    _enroll(bare_owner)
    bare_number = _personal_failure(stack, bare_owner, title="Never filled")
    _clear_title(bare_owner)
    stack.drain()
    _release_grant(bare_owner, attempts=5)
    stack.drain()
    (bare,) = _direct_messages(stack, bare_user)
    line = str(bare.params["text"]).split("\n", 1)[0]
    assert f"#{bare_number} stopped" in line
    assert "  " not in line
    assert "Never filled" not in line


def test_grant_notice_revoked_during_the_title_read_is_not_sent(stack: Stack) -> None:
    """A link revoked while the title is read is not the recipient.

    The title query is the await before resolution. Revoking inside that await
    leaves no direct message and records ``grant_notice_no_recipient``.
    """

    tag = stack.tag.upper()
    _seed_receiving_identity(stack.identity)
    slack_user = f"U0{tag}TQ"
    owner, link = _seed_principal(slack_user)
    assert link is not None
    _enroll(owner)
    _personal_failure(stack, owner, title="During the title read")
    _materialize_grant(stack)
    store = stack.worker.progress._store
    original = store.grant_notice_title

    async def title_then_revoke(work: Any) -> str | None:
        title = await original(work)
        async with store._engine.begin() as connection:
            updated = await connection.execute(
                text(
                    "UPDATE curie.identity_links SET revoked_at = now() "
                    "WHERE id = :id RETURNING id"
                ),
                {"id": link},
            )
            assert updated.first() is not None
        return title

    store.grant_notice_title = title_then_revoke
    try:
        stack.drain()
    finally:
        store.grant_notice_title = original
    assert _direct_messages(stack, slack_user) == []
    (row,) = _grant_rows(owner)
    assert row["posted_at"] is None
    assert row["failed_at"] is not None
    assert row["last_error"] == "grant_notice_no_recipient"
    assert row["attempts"] == 0


def test_grant_notice_title_matches_the_outbox_issue(stack: Stack) -> None:
    """The send reads the title of the issue key the outbox stored.

    A later failure backdated ahead of that issue must not replace the title.
    """

    tag = stack.tag.upper()
    _seed_receiving_identity(stack.identity)
    slack_user = f"U0{tag}TI"
    owner, _link = _seed_principal(slack_user)
    _enroll(owner)
    number_b = _personal_failure(stack, owner, title="Placeholder")
    request_b = _clear_title(owner)
    _materialize_grant(stack)
    owed = _grant_rows(owner)[0]
    assert owed["payload"]["issue_key"] == f"#{number_b}"
    assert "title" not in owed["payload"]
    number_a = _personal_failure(stack, owner, title="Alpha", age_s=300)
    _stamp_title(request_b, "Beta")
    stack.drain()
    (posted,) = _direct_messages(stack, slack_user)
    text_value = str(posted.params["text"])
    assert f"#{number_b}" in text_value
    assert "Beta" in text_value
    assert "Alpha" not in text_value
    assert f"#{number_a}" not in text_value


def _needs_person_card(*, cause: str, personal: bool) -> str:
    now = datetime(2026, 11, 18, tzinfo=UTC)
    subject = CardSubject(
        repository="acme/widgets",
        issue_label="#7",
        issue_title="Fix the greeting",
        issue_url="https://github.com/acme/widgets/issues/7",
        requester_slack_user=None,
        requester_login=None,
    )
    request = CardRequest(
        status="failed",
        terminal_cause=cause,
        first=True,
        declaration=None,
        reports=(),
        started_at=now,
        terminal_at=now,
        published_at=None,
        pr_number=None,
        pr_url=None,
        pr_title=None,
        plan_still_connected=False,
        personal=personal,
    )
    return render_card(subject, request, now=now).message.text


def test_grant_notice_card_uses_the_recorded_cause() -> None:
    """Copy follows the recorded cause, including after the enrollment moves.

    ``plan_still_connected`` is false, which is the reconnect that used to flip
    a provider rejection onto the enrollment fence sentence.
    """

    rejection = "Your Claude plan was rejected."
    rejected = _needs_person_card(cause="authentication_required", personal=True)
    assert rejection in rejected
    fence = cause_text("enrollment_fenced")
    fenced = _needs_person_card(cause="enrollment_fenced", personal=True)
    assert fence[:1].upper() + fence[1:] in fenced
    assert "was rejected" not in fenced
    shared = cause_text("authentication_required")
    service = _needs_person_card(cause="authentication_required", personal=False)
    assert shared[:1].upper() + shared[1:] in service
    assert rejection not in service
    for card in (rejected, fenced):
        assert "remove and reapply the issue's trigger label" in card
        assert "Slack thread if the run has an open pull request" in card

"""A Slack mention that links one issue starts a personal factory run (#5380 AC1, AC3).

The boundary is ``test_factory_slack_runtime``'s steering stack through
``factory_slack_kickoff_harness``: the real API (``create_app()`` on uvicorn
over a disposable migrated Postgres, real Valkey and RustFS) and its
``POST /v1/internal/factory-slack/kickoff`` route with the real resolver,
bindings, admission, locks and progress store; for the native tests also the
dispatcher's Bolt app driven through ``SocketModeHandler`` and the worker as
``run.build`` composes it. Assertions read Postgres through independent
sessions. Only the Slack Web API (``FakeSlack``) and GitHub REST
(``GitHubTruth`` with per-issue answers, shapes cited in ``forge_fakes``) are
stand-ins. Event shapes follow
https://docs.slack.dev/reference/events/app_mention; a declared driver's
follows the shape observed over Socket Mode on a real test workspace,
2026-11-12. Placeholder identifiers only.

Every test here fails with the kickoff route reverted (it answers 404). The
label cancellation tests also fail with only the admitting-label rule
reverted, except the two regression guards named in their docstrings, which
pass both ways. Approval resolution never changes a WorkItem's owner tuple;
``test_personal_execution_approval.py`` covers that for every personal
WorkItem, kickoff ones included, so it is not repeated here.
"""

from __future__ import annotations

import hashlib
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.models import DEFAULT_TENANT_ID
from curie_api.personal_enrollments import EnrollmentStatus
from curie_internal.slack_mention_receipt import kickoff_request_id

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (  # noqa: E402
    BASE_SHA,
    INSTALLATION_ID,
    LABEL,
    REPO,
    PlantedIssue,
    _issue_event,
    _post,
    forges_document,
    forges_env_json,
    github_repository_binding,
    issue_event,
    listed_publication,
)

from apps.api.tests.factory_slack_kickoff_harness import (  # noqa: E402
    GENERATION,
    OTHER_REPO,
    Mention,
    ask,
    ask_over_http,
    branch_read,
    counts,
    decide,
    declare_driver,
    disconnect_owner,
    driver_mention,
    enroll,
    enrolled,
    fail_github,
    fresh_ts,
    held_root,
    issue_read,
    issue_url,
    kick,
    linking,
    next_issue,
    pause_github,
    person_mention,
    plant_issue,
    poll_pass,
    requests_of,
    start_refusal,
    threads_at,
    wait_blocked_by,
    work_items_of,
)
from apps.api.tests.factory_slack_kickoff_harness import kickoff as kickoff  # noqa: E402
from apps.api.tests.test_factory_slack_runtime import (  # noqa: E402
    _PULLS,
    CHANNEL,
    OTHER_TEAM,
    SteeringStack,
    _admitted,
    _api_refused,
    _assert_no_sandbox,
    _code,
    _disable,
    _rows,
    _seed_principal,
    owner_tuples,
    runnable_requests,
)

OTHER_CHANNEL = "C0EXAMPLE3"


def _personal(principal_id: uuid.UUID) -> tuple[Any, ...]:
    return (DEFAULT_TENANT_ID, "personal", principal_id, GENERATION)


def _assert_admitted(answer: dict[str, Any], number: int) -> tuple[uuid.UUID, uuid.UUID]:
    assert answer["outcome"] == "admitted", answer
    assert answer["issue_url"] == issue_url(number), answer
    return uuid.UUID(answer["work_item_id"]), uuid.UUID(answer["request_id"])


def _admit_one(stack: SteeringStack) -> None:
    """Liveness: a valid mention from an enrolled person still admits."""

    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    answer = decide(stack, person_mention(stack, person.slack_user, linking(number)))
    _assert_admitted(answer, number)


# --- AC1: admission ------------------------------------------------------------------


def test_an_enrolled_sender_at_root_admits_one_personal_work_item_and_request(
    kickoff: SteeringStack,
) -> None:
    """A person's top-level mention linking an issue admits one personal run they own.

    Native path: the dispatcher writes the receipt, the worker asks the route
    before any sandbox, and the reply names the outcome and the issue.
    """

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    before = counts()

    attempt = kick(stack, person.slack_user, linking(number))

    work_item_id, request_id = _assert_admitted(attempt.answer, number)
    assert attempt.answer["replayed"] is False
    assert request_id == kickoff_request_id(stack.identity, attempt.delivery.delivery_id)
    (item,) = work_items_of(number)
    assert item["id"] == work_item_id
    assert item["trigger_label"] is None
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "main",
        "default",
        BASE_SHA,
    )
    assert item["code_host_installation_id"] == INSTALLATION_ID
    (request,) = requests_of(work_item_id)
    assert request["id"] == request_id
    assert request["status"] == "waiting"
    assert request["objective"] == issue_url(number)
    assert request["requester"] == f"slack:{person.slack_user}"
    assert (
        request["reply_kind"],
        request["reply_address"],
        request["reply_conversation_id"],
    ) == ("github", REPO, f"issue-{number}")
    assert owner_tuples(work_item_id) == {
        work_item_id: _personal(person.principal_id),
        request_id: _personal(person.principal_id),
    }
    (thread,) = threads_at(attempt.delivery.message_ts)
    assert thread == {
        "work_item_id": work_item_id,
        "agent_id": uuid.UUID(stack.agent_id),
        "channel_identity": stack.identity,
        "channel": CHANNEL,
        "root_ts": attempt.delivery.message_ts,
        "first_sequence": request["sequence"],
        "origin": "kickoff",
    }
    assert counts() == {
        "work_items": before["work_items"] + 1,
        "requests": before["requests"] + 1,
        "threads": before["threads"] + 1,
    }
    assert attempt.edits, "the placeholder was not edited with the answer"
    assert "Outcome: admitted" in attempt.edits[-1].text
    assert issue_url(number) in attempt.edits[-1].text
    _assert_no_sandbox(stack)


@pytest.mark.parametrize("enabled", [False, True])
def test_kickoff_pins_the_verified_senders_coding_selection(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    monkeypatch.setenv("CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY", str(enabled).lower())
    get_settings.cache_clear()
    try:
        person = enrolled(kickoff)
        document = "EXAMPLE_SLACK_CODING_INSTRUCTIONS_5591\n"
        digest = hashlib.sha256(document.encode()).hexdigest()
        _rows(
            "INSERT INTO curie.requester_coding_profiles "
            "(tenant_id, principal_id, text, revision, sha256, format_version) "
            "VALUES (:tenant, :principal, :text, 1, :sha, 1)",
            {
                "tenant": DEFAULT_TENANT_ID,
                "principal": person.principal_id,
                "text": document,
                "sha": digest,
            },
        )
        number = next_issue()
        plant_issue(kickoff, number)
        response = ask_over_http(
            kickoff, person_mention(kickoff, person.slack_user, linking(number))
        ).result(timeout=60)
        assert response.status_code == 200, response.text
        work_item_id, _request_id = _assert_admitted(response.json(), number)
        (item,) = _rows(
            "SELECT requester_principal_id, owner_principal_id, coding_instructions_state, "
            "coding_instructions_revision, coding_instructions_sha256, "
            "coding_instructions_absence_reason FROM curie.work_items WHERE id = :id",
            {"id": work_item_id},
        )
        assert item["requester_principal_id"] == person.principal_id
        assert item["owner_principal_id"] == person.principal_id
        (request,) = _rows(
            "SELECT coding_instructions_selection_id, coding_instructions_evidence "
            "FROM curie.execution_requests WHERE work_item_id = :id",
            {"id": work_item_id},
        )
        assert request["coding_instructions_selection_id"] == work_item_id
        snapshots = _rows(
            "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :id",
            {"id": work_item_id},
        )
        if enabled:
            assert item["coding_instructions_state"] == "selected"
            assert item["coding_instructions_revision"] == 1
            assert item["coding_instructions_sha256"] == digest
            assert snapshots == [{"text": document}]
            assert request["coding_instructions_evidence"] == "pending"
        else:
            assert item["coding_instructions_absence_reason"] == "application_unavailable"
            assert snapshots == []
            assert request["coding_instructions_evidence"] == "none"
        assert document not in response.text
    finally:
        get_settings.cache_clear()


def test_an_unmapped_thread_reply_admits_under_its_thread_root(kickoff: SteeringStack) -> None:
    """A mention in a thread no factory run maps admits with the thread's root as its root."""

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    thread_ts = fresh_ts()

    attempt = kick(stack, person.slack_user, linking(number), thread_ts=thread_ts)

    work_item_id, _request_id = _assert_admitted(attempt.answer, number)
    (thread,) = threads_at(thread_ts)
    assert (thread["work_item_id"], thread["origin"]) == (work_item_id, "kickoff")
    assert threads_at(attempt.delivery.message_ts) == []
    _assert_no_sandbox(stack)


def test_a_redelivery_answers_the_same_admission_once(kickoff: SteeringStack) -> None:
    """A delivery whose answer was lost is answered with its committed request, also
    after the owner disconnected; a new delivery from them is then refused."""

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    mention = person_mention(stack, person.slack_user, linking(number))

    first = decide(stack, mention)
    work_item_id, request_id = _assert_admitted(first, number)
    assert first["replayed"] is False
    after_first = counts()

    again = decide(stack, mention)
    assert again["replayed"] is True, again
    assert (again["work_item_id"], again["request_id"]) == (str(work_item_id), str(request_id))
    assert counts() == after_first

    status = disconnect_owner(stack, person.principal_id).result(timeout=30)
    assert isinstance(status, EnrollmentStatus), status
    replayed = decide(stack, mention)
    assert replayed["outcome"] == "admitted" and replayed["replayed"] is True, replayed
    assert replayed["request_id"] == str(request_id)
    assert [row["id"] for row in requests_of(work_item_id)] == [request_id]
    assert counts() == after_first

    later = next_issue()
    plant_issue(stack, later)
    refused = decide(stack, person_mention(stack, person.slack_user, linking(later)))
    assert refused["outcome"] == "enrollment_inactive", refused
    assert counts() == after_first


def test_later_steering_never_transfers_ownership(kickoff: SteeringStack) -> None:
    """Steering a kickoff run never changes its owner: another enrolled writer is
    refused ``owner_mismatch``, the owner's own revision copies the owner tuple."""

    stack = kickoff
    owner = enrolled(stack, github=True)
    writer = enrolled(stack, github=True)
    number = next_issue()
    plant_issue(stack, number)
    mention = person_mention(stack, owner.slack_user, linking(number))
    work_item_id, request_id = _assert_admitted(decide(stack, mention), number)
    stack.start(request_id)
    pull = next(_PULLS)
    _lineage, branch = stack.own_pull_request(work_item_id, pull)
    stack.github.open_pull(pull, branch)

    _api_refused(
        stack.steer(writer.slack_user, "Make it faster.", thread_ts=mention.root_ts),
        "admission_refused",
        "owner_mismatch",
    )
    _admitted(
        stack.steer(owner.slack_user, "Also update the changelog.", thread_ts=mention.root_ts)
    )

    tuples = owner_tuples(work_item_id)
    assert len(tuples) == 3, tuples
    assert set(tuples.values()) == {_personal(owner.principal_id)}


# --- AC1: every refusal fails closed ----------------------------------------------------


REFUSALS: dict[str, tuple[str, str | None]] = {
    "tampered_receipt": ("receipt_mismatch", None),
    "undeclared_driver": ("test_driver_refused", None),
    "test_installation_off_at_api": ("test_driver_refused", None),
    "factory_disabled": ("not_factory", None),
    "unserved_repository": ("not_factory", None),
    "unbound_channel": ("binding_missing", None),
    "binding_in_another_tenant": ("binding_missing", None),
    "two_identities_in_channel": ("binding_ambiguous", None),
    "no_link": ("link_missing", None),
    "two_links": ("link_multiple", None),
    "malformed_link": ("link_malformed", None),
    "another_host": ("link_host_unsupported", None),
    "pull_request_link": ("link_pull_request", None),
    "pull_request_number": ("link_pull_request", None),
    "unlinked_sender": ("sender_unverified", "no_link"),
    "wrong_workspace": ("sender_unverified", "namespace_unknown"),
    "agent_in_another_tenant": ("sender_unverified", "identity_not_found"),
    "disabled_principal": ("sender_unverified", "principal_inactive"),
    "not_enrolled": ("enrollment_inactive", None),
    "inactive_enrollment": ("enrollment_inactive", None),
    "issue_missing": ("issue_inaccessible", None),
    "issue_closed": ("issue_closed", None),
    "publication_exists": ("publication_exists", None),
    "two_base_labels": ("base_refused", "base_conflict"),
    "base_branch_missing": ("base_refused", "base_missing"),
    "service_work_item_on_issue": ("admission_refused", "owner_mismatch"),
    "mapped_root": ("thread_conflict", None),
}


def _new_tenant() -> uuid.UUID:
    tenant_id = uuid.uuid4()
    _rows(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :deployment, 'active')",
        {"id": tenant_id, "deployment": f"acme-kickoff-{tenant_id}"},
    )
    return tenant_id


def _move_agent(stack: SteeringStack, tenant_id: uuid.UUID, *, whole: bool) -> None:
    """Re-home the agent's bindings, or with ``whole`` the agent and every row its
    write paths scope, in one statement so composite keys stay consistent."""

    params = {"tenant": tenant_id, "agent": uuid.UUID(stack.agent_id)}
    if not whole:
        _rows("UPDATE curie.agent_channels SET tenant_id = :tenant WHERE agent_id = :agent", params)
        return
    _rows(
        "WITH a AS (UPDATE curie.agents SET tenant_id = :tenant WHERE id = :agent), "
        "c AS (UPDATE curie.agent_channels SET tenant_id = :tenant WHERE agent_id = :agent), "
        "v AS (UPDATE curie.agent_versions SET tenant_id = :tenant WHERE agent_id = :agent) "
        "UPDATE curie.deployments SET tenant_id = :tenant WHERE agent_id = :agent",
        params,
    )


def _refusal_case(
    stack: SteeringStack, monkeypatch: pytest.MonkeyPatch, case: str
) -> tuple[Mention, dict[str, Any], Callable[[], None]]:
    """The mention for ``case``, payload overrides, and how to undo its setup."""

    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    text_ = linking(number)
    overrides: dict[str, Any] = {}

    def nothing() -> None:
        return None

    undo: Callable[[], None] = nothing
    if case == "tampered_receipt":
        mention = person_mention(stack, person.slack_user, text_)
        overrides["receipt"] = {**mention.receipt, "root_ts": fresh_ts()}
        return mention, overrides, undo
    if case == "undeclared_driver":
        declare_driver(stack, monkeypatch)
        return driver_mention(stack, text_, bot_id="B0EXAMPLE6"), overrides, undo
    if case == "test_installation_off_at_api":
        declare_driver(stack, monkeypatch, api=False)
        return driver_mention(stack, text_), overrides, undo
    if case == "factory_disabled":
        settings = get_settings()
        monkeypatch.setattr(settings, "factory_enabled", False)
        return (
            person_mention(stack, person.slack_user, text_),
            overrides,
            lambda: monkeypatch.setattr(settings, "factory_enabled", True),
        )
    if case == "unserved_repository":
        return (
            person_mention(stack, person.slack_user, linking(number, OTHER_REPO)),
            overrides,
            undo,
        )
    if case == "unbound_channel":
        return (
            person_mention(stack, person.slack_user, text_, channel=OTHER_CHANNEL),
            overrides,
            undo,
        )
    if case in ("binding_in_another_tenant", "agent_in_another_tenant"):
        tenant_id = _new_tenant()
        whole = case == "agent_in_another_tenant"
        _move_agent(stack, tenant_id, whole=whole)

        def restore() -> None:
            _move_agent(stack, DEFAULT_TENANT_ID, whole=whole)
            _rows("DELETE FROM curie.tenants WHERE id = :id", {"id": tenant_id})

        return person_mention(stack, person.slack_user, text_), overrides, restore
    if case == "two_identities_in_channel":
        _rows(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
            "VALUES (:id, :agent, 'slack', :channel, 'default')",
            {"id": uuid.uuid4(), "agent": uuid.UUID(stack.agent_id), "channel": CHANNEL},
        )

        def unbind() -> None:
            _rows(
                "DELETE FROM curie.agent_channels WHERE agent_id = :agent AND kind = 'slack' "
                "AND adapter = 'default'",
                {"agent": uuid.UUID(stack.agent_id)},
            )

        return person_mention(stack, person.slack_user, text_), overrides, unbind
    texts = {
        "no_link": "Please start the factory on the login bug.",
        "two_links": f"{text_} and {linking(number + 1)}",
        "malformed_link": f"Please start <https://github.com/{REPO}/issues/abc>",
        "another_host": "Please start <https://gitlab.example.com/acme-corp/acme-bot/issues/7>",
        "pull_request_link": f"Please start <https://github.com/{REPO}/pull/7>",
    }
    if case in texts:
        return person_mention(stack, person.slack_user, texts[case]), overrides, undo
    if case == "unlinked_sender":
        stranger = stack.person(slack=False, github=False)
        enroll(stranger.principal_id)
        return person_mention(stack, stranger.slack_user, text_), overrides, undo
    if case == "wrong_workspace":
        return person_mention(stack, person.slack_user, text_, team=OTHER_TEAM), overrides, undo
    if case == "disabled_principal":
        _disable(person)
        return person_mention(stack, person.slack_user, text_), overrides, undo
    if case in ("not_enrolled", "inactive_enrollment"):
        other = stack.person(github=False)
        if case == "inactive_enrollment":
            enroll(other.principal_id, active=False)
        return person_mention(stack, other.slack_user, text_), overrides, undo
    planted = {
        "pull_request_number": PlantedIssue(pull_request=True),
        "issue_missing": PlantedIssue(missing=True),
        "issue_closed": PlantedIssue(state="closed"),
        "two_base_labels": PlantedIssue(labels=("base:main", "base:next")),
    }
    if case in planted:
        stack.github.planted_issues[number] = planted[case]
        return person_mention(stack, person.slack_user, text_), overrides, undo
    if case == "publication_exists":
        stack.github.listed_pulls.append(listed_publication(number, pull=next(_PULLS)))
        return (
            person_mention(stack, person.slack_user, text_),
            overrides,
            stack.github.listed_pulls.clear,
        )
    if case == "base_branch_missing":
        stack.github.missing_branches.add("main")
        return (
            person_mention(stack, person.slack_user, text_),
            overrides,
            stack.github.missing_branches.clear,
        )
    if case == "service_work_item_on_issue":
        stack.admit(number)
        return person_mention(stack, person.slack_user, text_), overrides, undo
    if case == "mapped_root":
        run = stack.open_steered_run()
        return (
            person_mention(stack, person.slack_user, text_, thread_ts=run.root_ts),
            overrides,
            undo,
        )
    raise AssertionError(f"unknown case {case}")


@pytest.mark.parametrize("case", sorted(REFUSALS))
def test_each_refusal_leaves_no_runnable_state_beside_a_valid_liveness_admission(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """Each refusal answers its named outcome and writes no WorkItem, request or
    thread and nothing runnable; a valid mention in the same stack then admits."""

    stack = kickoff
    mention, overrides, undo = _refusal_case(stack, monkeypatch, case)
    before = counts()
    runnable = runnable_requests()

    answer = decide(stack, mention, **overrides)

    outcome, reason = REFUSALS[case]
    assert answer["outcome"] == outcome, answer
    if reason is not None:
        assert answer["reason"] == reason, answer
    assert answer["request_id"] is None and answer["work_item_id"] is None, answer
    assert counts() == before
    assert runnable_requests() == runnable
    undo()
    _admit_one(stack)


def test_an_existing_association_cannot_be_hijacked_by_a_second_root_or_owner(
    kickoff: SteeringStack,
) -> None:
    """One issue keeps its first root and owner; one root keeps its first issue,
    and the loser of a race for a root rolls its whole admission back."""

    stack = kickoff
    owner = enrolled(stack)
    intruder = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    first = person_mention(stack, owner.slack_user, linking(number))
    _assert_admitted(decide(stack, first), number)
    settled = counts()

    for user in (owner.slack_user, intruder.slack_user):
        again = decide(stack, person_mention(stack, user, linking(number)))
        assert again["outcome"] == "thread_conflict", again
    other = next_issue()
    plant_issue(stack, other)
    at_root = person_mention(stack, intruder.slack_user, linking(other), thread_ts=first.root_ts)
    assert decide(stack, at_root)["outcome"] == "thread_conflict"
    assert counts() == settled
    assert work_items_of(other) == []

    # Two issues race for one root: the other admission's thread row is in
    # flight at the root when this one inserts, and commits first.
    _decoy_request, decoy = stack.admit(next_issue())
    raced = next_issue()
    plant_issue(stack, raced)
    root = fresh_ts()
    racing = person_mention(stack, owner.slack_user, linking(raced), thread_ts=root)
    with held_root(stack, root, decoy) as held:
        answered = ask_over_http(stack, racing)
        wait_blocked_by(held.pid)
        held.commit = True
    response = answered.result(timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["outcome"] == "thread_conflict", response.json()
    assert work_items_of(raced) == []
    assert [row["work_item_id"] for row in threads_at(root)] == [decoy]
    _admit_one(stack)


def test_a_declared_driver_resolves_only_to_its_prelinked_principal(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ADR 0202: a declared driver's version 2 receipt admits as the principal its
    declared bot user is linked to, whatever the event claims its user is; the
    same bot in a channel it is not declared in is refused."""

    stack = kickoff
    declare_driver(stack, monkeypatch)
    principal_id, _link = _seed_principal("U0EXAMPLE5")
    enroll(principal_id)
    claimed = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)

    answer = decide(stack, driver_mention(stack, linking(number), event_user=claimed.slack_user))

    work_item_id, request_id = _assert_admitted(answer, number)
    assert owner_tuples(work_item_id) == {
        work_item_id: _personal(principal_id),
        request_id: _personal(principal_id),
    }
    (request,) = requests_of(work_item_id)
    assert request["requester"] == "slack:U0EXAMPLE5"

    elsewhere = next_issue()
    plant_issue(stack, elsewhere)
    refused = decide(stack, driver_mention(stack, linking(elsewhere), channel="C0EXAMPLE2"))
    assert refused["outcome"] == "test_driver_refused", refused
    assert work_items_of(elsewhere) == []


def test_owner_text_in_the_body_never_chooses_the_owner(kickoff: SteeringStack) -> None:
    """Names, ids and requester text in the message never select the owner."""

    stack = kickoff
    sender = enrolled(stack)
    named = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    text_ = (
        f"{linking(number)} for <@{named.slack_user}>, owner={named.principal_id}, "
        f"requester=slack:{named.slack_user}"
    )

    answer = decide(stack, person_mention(stack, sender.slack_user, text_))

    work_item_id, _request_id = _assert_admitted(answer, number)
    assert set(owner_tuples(work_item_id).values()) == {_personal(sender.principal_id)}
    (request,) = requests_of(work_item_id)
    assert request["requester"] == f"slack:{sender.slack_user}"
    assert request["objective"] == issue_url(number)


# --- interleavings -----------------------------------------------------------------------


def test_identity_or_enrollment_changed_between_reads_refuses(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Slack link revoked while GitHub is read answers ``authority_changed``; an
    enrollment disconnected then answers ``enrollment_inactive``. Neither admits."""

    stack = kickoff
    for change in ("revoke", "disconnect"):
        person = enrolled(stack)
        number = next_issue()
        plant_issue(stack, number)
        before = counts()
        pause = pause_github(stack, monkeypatch, branch_read)
        answered = ask_over_http(stack, person_mention(stack, person.slack_user, linking(number)))
        try:
            assert pause.entered.wait(20), "the base read never started"
            if change == "revoke":
                assert person.slack_link_id is not None
                stack.revoke(person.slack_link_id)
            else:
                status = disconnect_owner(stack, person.principal_id).result(timeout=30)
                assert isinstance(status, EnrollmentStatus), status
        finally:
            pause.release.set()
        response = answered.result(timeout=30)
        assert response.status_code == 200, response.text
        expected = "authority_changed" if change == "revoke" else "enrollment_inactive"
        assert response.json()["outcome"] == expected, response.json()
        assert counts() == before
    _admit_one(stack)


def test_a_close_between_the_unlocked_read_and_the_lock_admits_nothing(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The issue is closed, and the close webhook handled, after the first issue
    read: the decisive read under the issue lock refuses ``issue_closed``."""

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    before = counts()
    pause = pause_github(stack, monkeypatch, branch_read)
    answered = ask_over_http(stack, person_mention(stack, person.slack_user, linking(number)))
    try:
        assert pause.entered.wait(20), "the base read never started"
        stack.github.planted_issues[number] = PlantedIssue(state="closed")
        closed = _post(
            stack.client,
            "issues",
            _issue_event(
                "closed",
                number,
                issue={"number": number, "state": "closed", "body": "do not store"},
            ),
        )
        assert closed.status_code == 200, closed.text
    finally:
        pause.release.set()
    response = answered.result(timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["outcome"] == "issue_closed", response.json()
    assert counts() == before
    assert work_items_of(number) == []


def test_a_disconnect_racing_a_kickoff_completes_without_a_stale_admission(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both interleavings finish within the bound. A disconnect committed while the
    kickoff holds the issue lock leaves no admission; a disconnect that waits on
    the kickoff's enrollment share lock fences the request it then commits."""

    stack = kickoff

    # 1. The disconnect commits while the kickoff waits on its decisive read.
    early = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    before = counts()
    pause = pause_github(stack, monkeypatch, issue_read(number), skip=1)
    answered = ask_over_http(stack, person_mention(stack, early.slack_user, linking(number)))
    try:
        assert pause.entered.wait(20), "the decisive issue read never started"
        status = disconnect_owner(stack, early.principal_id).result(timeout=30)
        assert isinstance(status, EnrollmentStatus), status
    finally:
        pause.release.set()
    response = answered.result(timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["outcome"] == "enrollment_inactive", response.json()
    assert counts() == before

    # 2. The kickoff holds the enrollment share lock (admission ran) while its
    # thread insert waits on a held root key; the disconnect queues behind it.
    late = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    _decoy_request, decoy = stack.admit(next_issue())
    mention = person_mention(stack, late.slack_user, linking(number))
    with held_root(stack, mention.root_ts, decoy) as held:
        answered = ask_over_http(stack, mention)
        kickoff_pid = wait_blocked_by(held.pid)
        disconnecting = disconnect_owner(stack, late.principal_id)
        wait_blocked_by(kickoff_pid)
    response = answered.result(timeout=30)
    assert response.status_code == 200, response.text
    work_item_id, request_id = _assert_admitted(response.json(), number)
    status = disconnecting.result(timeout=30)
    assert isinstance(status, EnrollmentStatus), status
    assert start_refusal(stack, request_id) in {
        "enrollment_inactive",
        "enrollment_generation_mismatch",
    }
    assert [row["status"] for row in requests_of(work_item_id)] == ["waiting"]


def test_a_nondefault_base_label_is_persisted_and_the_request_starts(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An issue's allowed ``base:`` label pins the WorkItem's base at admission, and
    the request is acquired and started from it."""

    stack = kickoff
    document = forges_document(
        bindings=[github_repository_binding(bases=["main", "next"], default_base="main")]
    )
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path / "bases"))
    get_settings.cache_clear()
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number, labels=("base:next",))

    answer = decide(stack, person_mention(stack, person.slack_user, linking(number)))

    work_item_id, request_id = _assert_admitted(answer, number)
    (item,) = work_items_of(number)
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "next",
        "label",
        BASE_SHA,
    )
    assert item["trigger_label"] is None
    assert start_refusal(stack, request_id) is None
    assert [row["status"] for row in requests_of(work_item_id)] == ["running"]


def test_a_code_host_outage_answers_503_and_admits_once_after(
    kickoff: SteeringStack, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GitHub unavailable answers 503 with ``Retry-After`` and writes nothing; the
    same delivery then admits once, and again replays."""

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    mention = person_mention(stack, person.slack_user, linking(number))
    before = counts()
    failed = fail_github(stack, monkeypatch, issue_read(number))

    unavailable = ask(stack, mention)

    assert failed, "the issue read was never made"
    assert unavailable.status_code == 503, unavailable.text
    assert _code(unavailable) == "code_host_unavailable"
    assert unavailable.headers.get("retry-after") == "10"
    assert unavailable.headers.get("cache-control") == "no-store"
    assert counts() == before

    admitted = decide(stack, mention)
    _assert_admitted(admitted, number)
    assert admitted["replayed"] is False
    replayed = decide(stack, mention)
    assert replayed["replayed"] is True and replayed["request_id"] == admitted["request_id"]
    assert counts()["requests"] == before["requests"] + 1


# --- AC3: progress reaches the originating root -----------------------------------------


def test_progress_reaches_the_originating_root_end_to_end(kickoff: SteeringStack) -> None:
    """With no factory destination configured, the run's phase, pull request and
    terminal posts land in the kickoff's own root, and no root is posted."""

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    attempt = kick(stack, person.slack_user, linking(number))
    work_item_id, request_id = _assert_admitted(attempt.answer, number)
    root = attempt.delivery.message_ts

    epoch = stack.start(request_id)
    stack.report(request_id, "plan", "Drafting the plan")
    pull = next(_PULLS)
    stack.own_pull_request(work_item_id, pull)
    stack.finish(request_id, epoch, published=True)
    stack.drain()

    posts = [
        call.haystack
        for call in stack.slack.thread_posts(stack.factory_token, root)
        if "Outcome:" not in call.haystack
    ]
    assert len(posts) == 3, posts
    assert "Drafting the plan" in posts[0]
    assert f"https://github.com/{REPO}/pull/{pull}" in posts[1]
    assert stack.slack.roots(stack.factory_token) == []
    (thread,) = threads_at(root)
    assert (thread["work_item_id"], thread["origin"]) == (work_item_id, "kickoff")
    assert len(stack.threads()) == 1


# --- label cancellation (#4361 decision 10 with kickoff) ---------------------------------


def test_the_stale_sweep_never_cancels_a_kickoff_work_item_on_a_label_removal(
    kickoff: SteeringStack,
) -> None:
    """The issue once had the factory label, removed by a person before the kickoff:
    the poll's stale sweep keeps the kickoff run. Closing the issue still cancels
    it (that half is a regression guard, passing both ways)."""

    stack = kickoff
    stack.github.answer_listings = True
    person = enrolled(stack)
    number = next_issue()
    history = [
        issue_event("labeled", event_id=7_100_001, label=LABEL),
        issue_event("unlabeled", event_id=7_100_002, label=LABEL),
    ]
    plant_issue(stack, number, events=list(history))
    work_item_id, request_id = _assert_admitted(
        decide(stack, person_mention(stack, person.slack_user, linking(number))), number
    )

    poll_pass(stack)

    (item,) = work_items_of(number)
    assert item["cancelled_at"] is None
    assert [row["status"] for row in requests_of(work_item_id)] == ["waiting"]

    stack.github.planted_issues[number] = PlantedIssue(
        state="closed", events=[*history, issue_event("closed", event_id=7_100_003)]
    )
    poll_pass(stack)

    (item,) = work_items_of(number)
    assert item["cancelled_at"] is not None
    assert request_id not in runnable_requests()


def test_webhook_label_removal_never_cancels_a_kickoff_work_item_and_closing_does(
    kickoff: SteeringStack,
) -> None:
    """An ``unlabeled`` webhook leaves a kickoff run alone, while it still cancels a
    label-admitted run (a regression guard), and closing the kickoff's issue
    cancels it (a regression guard)."""

    stack = kickoff
    person = enrolled(stack)
    number = next_issue()
    plant_issue(stack, number)
    _assert_admitted(
        decide(stack, person_mention(stack, person.slack_user, linking(number))), number
    )

    unlabeled = _post(
        stack.client, "issues", _issue_event("unlabeled", number, label={"name": LABEL})
    )
    assert unlabeled.status_code == 200, unlabeled.text
    assert "label_not_admitting" in unlabeled.text
    (item,) = work_items_of(number)
    assert item["cancelled_at"] is None

    labelled = next_issue()
    plant_issue(stack, labelled, labels=(LABEL,))
    admitted = _post(
        stack.client, "issues", _issue_event("labeled", labelled, label={"name": LABEL})
    )
    assert admitted.status_code == 200, admitted.text
    (by_label,) = work_items_of(labelled)
    assert by_label["trigger_label"] == LABEL and by_label["cancelled_at"] is None
    plant_issue(stack, labelled)
    removed = _post(
        stack.client, "issues", _issue_event("unlabeled", labelled, label={"name": LABEL})
    )
    assert removed.status_code == 200, removed.text
    (by_label,) = work_items_of(labelled)
    assert by_label["cancelled_at"] is not None

    plant_issue(stack, number, state="closed")
    closed = _post(
        stack.client,
        "issues",
        _issue_event("closed", number, issue={"number": number, "state": "closed", "body": "x"}),
    )
    assert closed.status_code == 200, closed.text
    (item,) = work_items_of(number)
    assert item["cancelled_at"] is not None

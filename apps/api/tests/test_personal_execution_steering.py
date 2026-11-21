"""Slack steering keeps a personal WorkItem's owner (#5378 AC5, Decision 8, ADR 0268).

A threaded Slack mention in a factory run's root asks the internal steer route
for one more revision of its pull request (#5414). For a personal WorkItem the
route names the resolved sender as the owner, at the sender's current
enrollment generation, and the existing owner admission compares that with the
locked WorkItem's tuple. Only the WorkItem's owner steers it: another
principal is refused even with write access to the repository, and so are a
Slack account that now resolves to someone else, an inactive enrollment, an
identity that changes while the decision is made, and a WorkItem of another
tenant. When the owner's enrollment advances, an owner mention can rebind the
WorkItem after teardown is confirmed. A live runtime causes a ``runtime_live``
refusal. Each refusal leaves the runnable requests and owner tuples unchanged.
An admitted revision copies the locked WorkItem's tuple in either installation
mode, and a redelivery admits it once.

The boundary is ``test_factory_slack_runtime``'s steering stack: the real API
served over HTTP and the worker as ``run.build`` composes it, over real Postgres
and Valkey. Owner checks start with the production receipt writer and enqueue.
The cross-tenant case also runs through the dispatcher's Bolt app. Only the Slack
Web API, GitHub REST and the Kubernetes API are stand-ins. Enrollment headers
are seeded with raw SQL, standing in for #5377's enrollment writes. The other tenant comes from
``personal_execution_support``. Every check reads the durable rows, and the
owner each request runs as is projected through a newly constructed worker
``BindingResolver``. Placeholder identifiers only.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import personal_execution_support as support
import pytest
import test_factory_slack_runtime as runtime
from curie_api.models import DEFAULT_TENANT_ID
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from test_factory_slack_runtime import OwnerTuple, Person, SteeringStack, SteerRun
from test_personal_execution_revalidation import _confirm_resume_teardown, _resume_requests

# The owner's enrollment generation when its WorkItem is admitted. Not 1, so a
# tuple that names a default generation instead of the bound one is caught.
GENERATION = 2


@pytest.fixture
def steering(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[SteeringStack]:
    support.cleanup_owned()
    with runtime._steering_stack(monkeypatch, tmp_path) as built:
        yield built
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


# --- enrollment headers, standing in for #5377 ------------------------------------------


def _enroll(person: Person, *, generation: int = GENERATION) -> None:
    runtime._rows(
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, :generation, true, 1)",
        {
            "id": uuid.uuid4(),
            "tenant": DEFAULT_TENANT_ID,
            "principal": person.principal_id,
            "generation": generation,
        },
    )


def _advance(person: Person) -> int:
    """The person replaces their subscription: the generation and CAS version advance."""

    (row,) = runtime._rows(
        "UPDATE curie.personal_claude_enrollments "
        "SET generation = generation + 1, version = version + 1, updated_at = now() "
        "WHERE tenant_id = :tenant AND principal_id = :principal RETURNING generation",
        {"tenant": DEFAULT_TENANT_ID, "principal": person.principal_id},
    )
    return int(row["generation"])


def _disconnect(person: Person) -> None:
    (row,) = runtime._rows(
        "UPDATE curie.personal_claude_enrollments "
        "SET active = false, version = version + 1, updated_at = now() "
        "WHERE tenant_id = :tenant AND principal_id = :principal RETURNING active",
        {"tenant": DEFAULT_TENANT_ID, "principal": person.principal_id},
    )
    assert row["active"] is False


def _owner_context(person: Person, generation: int = GENERATION) -> dict[str, Any]:
    return {
        "tenant_id": str(DEFAULT_TENANT_ID),
        "principal_id": str(person.principal_id),
        "enrollment_generation": generation,
    }


def _personal(person: Person, generation: int = GENERATION) -> OwnerTuple:
    return (DEFAULT_TENANT_ID, "personal", person.principal_id, generation)


def _relink_slack(slack_user: str, principal_id: uuid.UUID) -> None:
    """Link ``slack_user``'s Slack account to ``principal_id`` (its old link is revoked)."""

    from curie_api.identity.service import find_or_create_namespace
    from curie_api.models import IdentityLink

    async def go(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            namespace = await find_or_create_namespace(
                session,
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                authority="",
                kind="slack_workspace",
                key=runtime.TEAM,
            )
            session.add(
                IdentityLink(
                    tenant_id=DEFAULT_TENANT_ID,
                    identity_namespace_id=namespace.id,
                    provider_native_id=slack_user,
                    principal_id=principal_id,
                    created_by_principal_id=principal_id,
                    verification_source="admin_mapped",
                )
            )
            await session.commit()

    runtime._db(go)


def _assert_projected(
    stack: SteeringStack, run: SteerRun, expected: OwnerTuple, request_ids: list[uuid.UUID]
) -> None:
    """A newly constructed worker resolver sees ``expected`` on every request."""

    tenant_id = expected[0]
    assert tenant_id is not None
    projected = support.resolve_execution_owners(request_ids, tenant_id=tenant_id)
    for request_id in request_ids:
        owner = projected[request_id]
        assert owner is not None, request_id
        assert (
            owner.work_item_id,
            owner.request_id,
            owner.agent_id,
            owner.tenant_id,
            owner.model_auth_mode,
            owner.owner_principal_id,
            owner.enrollment_generation,
        ) == (run.work_item_id, request_id, uuid.UUID(stack.agent_id), *expected), request_id


# --- the owner steers ------------------------------------------------------------------


@pytest.mark.parametrize("installation", ["service", "personal"])
def test_the_owner_steers_a_personal_work_item_and_its_revision_keeps_the_owner_tuple(
    steering: SteeringStack, monkeypatch: pytest.MonkeyPatch, installation: str
) -> None:
    """AC5 and the 2026-11-10 lifecycle addition: the revision keeps the WorkItem's tuple.

    The owner's mention admits a revision whose tenant, mode, owner principal
    and enrollment generation equal the locked WorkItem's, on a service and a
    personal installation alike: the mode comes from the WorkItem, never the
    installation. The first attempt's answer is lost, and the redelivery
    replays the same revision. Once the live run ends the revision is promoted
    to run next with the same tuple, and a reconstructed worker resolver sees
    the owner on both requests.

    Liveness for every refusal in this suite.
    """

    stack = steering
    owner = stack.person()
    _enroll(owner)
    run = stack.open_steered_run(owner=_owner_context(owner))
    personal = _personal(owner)
    assert runtime.owner_tuples(run.work_item_id) == {
        run.work_item_id: personal,
        run.request_id: personal,
    }
    runtime.installation_mode(monkeypatch, installation)

    delivery = runtime._lose_one_admission(stack, owner, run)
    runtime._replay_completes_once(stack, stack.steerer, run, delivery)
    revision_id = stack.expected_request_id(delivery)
    owned = {run.work_item_id: personal, run.request_id: personal, revision_id: personal}
    assert runtime.owner_tuples(run.work_item_id) == owned
    (revision,) = [row for row in stack.requests() if row["id"] == revision_id]
    assert (revision["status"], revision["requester"]) == ("queued", owner.requester)
    _assert_projected(stack, run, personal, [run.request_id, revision_id])

    # The live run ends and the revision is promoted to run next.
    stack.finish(run.request_id, run.epoch)
    runtime._promote(run.work_item_id)
    statuses = {row["id"]: row["status"] for row in stack.requests()}
    assert statuses[revision_id] == "waiting", statuses
    assert runtime.owner_tuples(run.work_item_id) == owned
    assert runtime.runnable_requests() == {revision_id}
    _assert_projected(stack, run, personal, [run.request_id, revision_id])
    runtime._assert_no_sandbox(stack)


# --- refusals, each beside the owner's admission ---------------------------------------

REFUSALS = [
    "another_enrolled_writer",
    "another_writer_without_enrollment",
    "slack_account_relinked",
    "advanced_generation",
    "disconnected_enrollment",
    "authority_changed_mid_decision",
]


@pytest.mark.parametrize("case", REFUSALS)
def test_each_steer_by_anyone_but_the_current_owner_is_refused_beside_the_owners_admission(
    steering: SteeringStack, case: str
) -> None:
    """AC5: wrong owner, changed identity and live runtime queue nothing.

    Every refused mention answers ``admission_refused`` with its named owner
    code (``authority_changed`` for the identity changed mid-decision and
    ``runtime_live`` when the generation advanced while the run is live), creates
    no request, leaves no runnable request beside the owner's, and moves no
    owner tuple. The owner's own mention, in the same root, is admitted.

    Falsifiable: a steering route that admits under service mode with no owner
    admits every refused mention here except ``authority_changed_mid_decision``,
    which the route refuses before admission either way.
    """

    stack = steering
    owner = stack.person()
    _enroll(owner)
    run = stack.open_steered_run(owner=_owner_context(owner))
    personal = _personal(owner)
    root = run.root_ts

    def live(body: str = "Please also update the changelog.") -> None:
        runtime._admitted(stack.steer_queued(owner.slack_user, body, thread_ts=root))

    def refused(user: str, reason: str) -> None:
        runtime._api_refused(
            stack.steer_queued(user, "Please rename the helper.", thread_ts=root),
            "admission_refused",
            reason,
        )

    if case == "another_enrolled_writer":
        # Write access to the repository and an enrollment of their own confer
        # nothing on another person's WorkItem.
        other = stack.person()
        _enroll(other, generation=1)
        refused(other.slack_user, "owner_mismatch")
        live()
    elif case == "another_writer_without_enrollment":
        other = stack.person()
        refused(other.slack_user, "enrollment_inactive")
        live()
    elif case == "slack_account_relinked":
        # The owner's Slack account now resolves to another enrolled writer.
        live()
        other = stack.person(slack=False)
        _enroll(other, generation=GENERATION)
        assert owner.slack_link_id is not None
        stack.revoke(owner.slack_link_id)
        _relink_slack(owner.slack_user, other.principal_id)
        refused(owner.slack_user, "owner_mismatch")
    elif case == "advanced_generation":
        # The previous run is still live after the owner replaces enrollment.
        live()
        assert _advance(owner) == GENERATION + 1
        refused(owner.slack_user, "runtime_live")
    elif case == "disconnected_enrollment":
        live()
        _disconnect(owner)
        refused(owner.slack_user, "enrollment_inactive")
    elif case == "authority_changed_mid_decision":
        runtime._admitted(
            stack.steer_paused(
                owner.slack_user, "Paused, then released.", thread_ts=root, during=lambda: None
            )
        )
        runtime._api_refused(
            stack.steer_paused(
                owner.slack_user,
                "Paused, then revoked.",
                thread_ts=root,
                during=lambda: stack.revoke(owner.github_link_ids[0]),
            ),
            "authority_changed",
        )
    else:  # pragma: no cover - the parameter list is closed
        raise AssertionError(case)

    # Exactly the owner's one revision was admitted, and it is the only
    # runnable request; every tuple is the WorkItem's.
    admitted = [row for row in stack.requests() if row["id"] != run.request_id]
    assert len(admitted) == 1, admitted
    (revision,) = admitted
    assert revision["requester"] == owner.requester
    assert runtime.runnable_requests() == {revision["id"]}
    assert runtime.owner_tuples(run.work_item_id) == {
        run.work_item_id: personal,
        run.request_id: personal,
        revision["id"]: personal,
    }
    _assert_projected(stack, run, personal, [revision["id"]])
    runtime._assert_no_sandbox(stack)


def test_a_personal_work_item_of_another_tenant_refuses_a_steer_resolved_in_the_default_tenant(
    steering: SteeringStack,
) -> None:
    """AC5: a sender resolved in one tenant never steers another tenant's personal run.

    Slack senders resolve in the default tenant. The agent and its personal
    WorkItem belong to another tenant, owned there. A default-tenant writer
    with an active enrollment and write access mentions the run (handed to the
    worker at the queue boundary) and is refused ``owner_tenant_mismatch``,
    with no request and the foreign tuple unchanged. Liveness: the owner
    admission of a default-tenant WorkItem
    (``test_the_owner_steers_a_personal_work_item_and_its_revision_keeps_the_owner_tuple``).

    Falsifiable: a steering route that admits under service mode with no owner
    queues a revision of the foreign WorkItem.
    """

    stack = steering
    world = support.seed_world()
    writer = stack.person()
    _enroll(writer)
    support.move_agent_to_tenant(uuid.UUID(stack.agent_id), world.tenant_id)
    run = stack.open_steered_run(owner=world.owner())
    foreign: OwnerTuple = world.personal()
    owned = {run.work_item_id: foreign, run.request_id: foreign}
    assert runtime.owner_tuples(run.work_item_id) == owned

    runtime._api_refused(
        stack.steer_queued(writer.slack_user, "Please rename the helper.", thread_ts=run.root_ts),
        "admission_refused",
        "owner_tenant_mismatch",
    )
    assert runtime.owner_tuples(run.work_item_id) == owned
    assert runtime.runnable_requests() == set()
    runtime._assert_no_sandbox(stack)


def test_owner_slack_steer_resumes_at_current_generation_after_teardown(
    steering: SteeringStack,
) -> None:
    owner = steering.person()
    other = steering.person()
    _enroll(owner, generation=1)
    _enroll(other)
    run = steering.open_steered_run(owner=runtime._owner(owner.principal_id))
    support.finish_failed(run.request_id, run.epoch, cause="authentication_required")
    runtime._set_enrollment(owner.principal_id, generation=2, active=True)
    old: OwnerTuple = (DEFAULT_TENANT_ID, "personal", owner.principal_id, 1)
    before = _resume_requests(run.work_item_id)

    another = steering.steer_queued(other.slack_user, "Please resume.", thread_ts=run.root_ts)
    runtime._api_refused(another, "admission_refused", "owner_mismatch")
    refused = steering.steer_queued(owner.slack_user, "Please resume.", thread_ts=run.root_ts)
    runtime._api_refused(refused, "admission_refused", "runtime_live")
    assert "try again shortly" in refused.edits[-1].text.lower()
    assert runtime.owner_tuples(run.work_item_id) == {run.work_item_id: old, run.request_id: old}
    assert _resume_requests(run.work_item_id) == before

    _confirm_resume_teardown(run.request_id)
    [before] = _resume_requests(run.work_item_id)
    resumed = steering.steer_queued(owner.slack_user, "Please resume.", thread_ts=run.root_ts)
    runtime._admitted(resumed)
    predecessor, successor = _resume_requests(run.work_item_id)
    assert predecessor == before
    assert (successor["enrollment_generation"], successor["status"]) == (2, "waiting")
    new: OwnerTuple = (DEFAULT_TENANT_ID, "personal", owner.principal_id, 2)
    assert runtime.owner_tuples(run.work_item_id) == {
        run.work_item_id: new,
        run.request_id: old,
        successor["id"]: new,
    }
    runtime._assert_no_sandbox(steering)

"""Another tenant's personal WorkItem refuses a default-tenant steer
(#5378 AC5, Decision 8, ADR 0268).

A threaded Slack mention in a factory run's root asks the internal steer route
for one more revision of its pull request (#5414). Slack senders resolve in
the default tenant. When the agent and its personal WorkItem belong to another
tenant, a default-tenant writer is refused ``owner_tenant_mismatch`` and the
foreign owner tuple stays.

The boundary is ``test_factory_slack_runtime``'s steering stack: the real API
served over HTTP, the dispatcher's Bolt app, and the worker as ``run.build``
composes it, over real Postgres and Valkey. Only the Slack Web API, GitHub REST
and the Kubernetes API are stand-ins. Enrollment headers are seeded with raw
SQL, standing in for #5377's enrollment writes; the other tenant comes from
``personal_execution_support``. Every check reads the durable rows.
Placeholder identifiers only.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import personal_execution_support as support
import pytest
import test_factory_slack_runtime as runtime
from curie_api.models import DEFAULT_TENANT_ID
from test_factory_slack_runtime import OwnerTuple, Person, SteeringStack
from test_personal_execution_revalidation import _confirm_resume_teardown, _resume_requests

# Enrollment generation for the default-tenant writer. Not 1, so it is not a
# default generation.
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


def test_a_personal_work_item_of_another_tenant_refuses_a_steer_resolved_in_the_default_tenant(
    steering: SteeringStack,
) -> None:
    """AC5: a sender resolved in one tenant never steers another tenant's personal run.

    Slack senders resolve in the default tenant. The agent and its personal
    WorkItem belong to another tenant, owned there. A default-tenant writer
    with an active enrollment and write access mentions the run (handed to the
    worker at the queue boundary) and is refused ``owner_tenant_mismatch``,
    with no request and the foreign tuple unchanged.

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

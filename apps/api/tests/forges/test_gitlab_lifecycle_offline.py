"""AC 8 of #4399: a gitlab+gitlab install, end to end, offline.

The GitLab fake (`forge_fakes.gitlab.GitLabAPI`) is the only fake. It is
served over a socket and the install's ``CURIE_FORGES`` names only a GitLab
tracker, code host and binding on it (`forge_fakes.gitlab_lifecycle`), built
through the production registry. Nothing in the factory core is replaced: unlike
``forge_fakes/bitbucket_lifecycle.py`` no ``forges.hosts`` or
``code_host_for_repository`` seam is patched, so every code host step must
resolve the GitLab code host from the stored WorkItem or lineage itself.

The passes are the real ones: `WorkItemReconciler.run_once` (tracker poll,
CI gate, review feedback) and the status note sync (`_reconcile`), and the
worker's and publication Job's internal routes. The review feedback pass is
expected inside ``run_once``, beside the tracker poll, so a developer's diff
comment becomes a revision on the next reconcile with no GitHub webhook.

The steps are the ``run_*`` functions in `forge_fakes.gitlab_lifecycle`; the
live fixture suite (``gitlab_fixture/test_gitlab_lifecycle_fixture.py``) runs
the same ones against real GitLab CE.

Run with ``uv run pytest apps/api/tests -k gitlab_lifecycle``.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.factory_review_feedback import cursor_key
from curie_api.forges.registry import forges_registry
from curie_api.forges.trackers import reply_route
from curie_api.models import DEFAULT_TENANT_ID
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from forge_fakes import gitlab_lifecycle as gl
from forge_fakes.github_comments import _rows
from forge_fakes.gitlab_lifecycle import FAST, GitLabInstall

pytestmark = pytest.mark.usefixtures("clean_db")


@pytest.fixture
def install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path)


@pytest.fixture
def two_surfaces(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path, labels=(None, FAST))


def test_gitlab_lifecycle_label_to_merge_request_ci_review_and_cancel(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    gl.run_label_to_merge_request_ci_review_and_cancel(install, caplog)


def test_gitlab_lifecycle_a_reporter_label_admits_nothing_and_sets_no_state_label(
    install: GitLabInstall,
) -> None:
    gl.run_reporter_label_admits_nothing(install)


def test_gitlab_lifecycle_two_surfaces_admit_only_their_own_label(
    two_surfaces: GitLabInstall,
) -> None:
    gl.run_two_surfaces_admit_only_their_own_label(two_surfaces)


def test_gitlab_lifecycle_an_issue_mention_links_the_note_and_counts_as_a_follow_up(
    install: GitLabInstall,
) -> None:
    gl.run_issue_mention_links_the_note(install)


def test_gitlab_lifecycle_failures_log_and_answer_no_token(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    gl.run_failures_log_and_answer_no_token(install, caplog)


# --- #5378: review feedback admits under the installation's mode ------------------------
#
# The review feedback pass (`factory_review_feedback`, inside `run_once`) builds
# its revision facts with the installation's `FACTORY_MODEL_AUTH_MODE` and never
# an owner, as tracker intake does. Each WorkItem's merge request and lineage
# are the fixture state a factory publication leaves behind (seeded, as the
# GitHub review suite seeds its lineage); the merge request lives on the fake
# and the review is a developer's diff discussion on it.

OWNER_MARK = "test-gitlab-review-owner-"
# The owner's enrollment generation; not 1, so a default generation is caught.
GENERATION = 2
_TUPLE = "tenant_id, model_auth_mode, owner_principal_id, enrollment_generation"


def _seed_owner() -> uuid.UUID:
    """An active default-tenant principal enrolled at ``GENERATION`` (standing in for #5377)."""

    principal_id = uuid.uuid4()
    gl.execute(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, display_name) "
        "VALUES (:id, :tenant, :subject, 'human', 'active', 'Acme Owner')",
        {"id": principal_id, "tenant": DEFAULT_TENANT_ID, "subject": f"{OWNER_MARK}{principal_id}"},
    )
    gl.execute(
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, :generation, true, 1)",
        {
            "id": uuid.uuid4(),
            "tenant": DEFAULT_TENANT_ID,
            "principal": principal_id,
            "generation": GENERATION,
        },
    )
    return principal_id


@pytest.fixture
def owners() -> Iterator[Callable[[], uuid.UUID]]:
    """Seeds owners; afterwards removes them with every row naming them."""

    yield _seed_owner
    gl.execute(
        "TRUNCATE curie.execution_requests, curie.work_items, "
        "curie.personal_claude_enrollments CASCADE",
        {},
    )
    gl.execute(
        "DELETE FROM curie.principals WHERE idp_subject LIKE :mark", {"mark": f"{OWNER_MARK}%"}
    )


def _installation_mode(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    """Switch the installation's ``FACTORY_MODEL_AUTH_MODE``, as an operator's
    restart does; each reconciler pass reads its settings anew."""

    monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", mode)
    get_settings.cache_clear()


def _owner_tuples(work_item_id: uuid.UUID) -> dict[uuid.UUID, tuple[Any, ...]]:
    """The owner tuple of the WorkItem and of every one of its requests, by id."""

    found = _rows(
        f"SELECT id, {_TUPLE} FROM curie.work_items WHERE id = :id "
        f"UNION ALL SELECT id, {_TUPLE} FROM curie.execution_requests WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return {
        row["id"]: (
            row["tenant_id"],
            row["model_auth_mode"],
            row["owner_principal_id"],
            row["enrollment_generation"],
        )
        for row in found
    }


def _open_merge_request(install: GitLabInstall, work_item_id: uuid.UUID) -> tuple[int, uuid.UUID]:
    """An open merge request on the fake and the lineage a publication leaves for it."""

    fake = install.fake
    (item,) = _rows(
        "SELECT agent_id, conversation_id FROM curie.work_items WHERE id = :id",
        {"id": work_item_id},
    )
    branch = f"curie/review-acme-{uuid.uuid4().hex[:8]}"
    mr = fake.add_merge_request(branch, title="Fix the widget parser")
    lineage_id = uuid.uuid4()
    gl.execute(
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
        "pr_number, pr_url, head_sha, status, version, latest_revision, code_host_kind, "
        "code_host_host, repository_project_id, code_host_pr_id, base_ref) VALUES "
        "(:id, :agent, :deployment, :conversation, :repo, :base, :branch, :pr, :url, :head, "
        "'open', 1, 1, 'gitlab', :host, :project, :pr_id, 'main')",
        {
            "id": lineage_id,
            "agent": item["agent_id"],
            "deployment": uuid.UUID(gl.deployment(install, item["agent_id"])),
            "conversation": item["conversation_id"],
            "repo": install.backend.project_path,
            "base": install.backend.default_head(),
            "branch": branch,
            "pr": mr,
            "url": install.mr_url(mr),
            "head": fake.branches[branch],
            "host": install.host,
            "project": str(install.backend.project_id),
            "pr_id": str(fake.merge_requests[mr].id),
        },
    )
    gl.execute(
        "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
        "version = version + 1 WHERE id = :id",
        {"lineage": lineage_id, "id": work_item_id},
    )
    return mr, lineage_id


def _forget_cursor(lineage_id: uuid.UUID) -> None:
    """Remove the review pass's shared Valkey cursor of one lineage."""

    client = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        client.delete(cursor_key(lineage_id))
    finally:
        client.close()


def _admit_personal(install: GitLabInstall, owner: uuid.UUID) -> uuid.UUID:
    """A personal WorkItem on an unlabelled issue, through the trusted owner-bearing
    admission (#5376), on the route the issue replies on."""

    backend = install.backend
    iid = backend.add_issue(
        "Widget parser drops a quoted field", "Quoted commas split.", author=backend.developer
    )
    issue = install.issue_ref(iid)
    kind, address, conversation = reply_route(
        forges_registry(get_settings()), issue, backend.project_path
    )
    admitted = install.call(
        "POST",
        "/v1/internal/work-items/admissions",
        json={
            "agent_id": str(install.agent_id),
            "kind": kind,
            "address": address,
            "reply_conversation_id": conversation,
            "tracker": {
                "kind": issue.kind,
                "host": issue.host,
                "scope_id": issue.scope_id,
                "issue_id": issue.issue_id,
                "display_key": issue.display_key,
            },
            "repository": {
                "code_host_kind": "gitlab",
                "host": install.host,
                "project_id": str(backend.project_id),
                "path": backend.project_path,
            },
            "objective": install.issue_url(iid),
            "requester": f"gitlab:{backend.developer.id}:{backend.developer.login}",
            "request_id": str(uuid.uuid4()),
            "model_auth_mode": "personal",
            "owner": {
                "tenant_id": str(DEFAULT_TENANT_ID),
                "principal_id": str(owner),
                "enrollment_generation": GENERATION,
            },
        },
    )
    assert admitted.status_code == 200, admitted.text
    (item,) = gl.work_items(iid)
    return uuid.UUID(str(item["id"]))


def test_gitlab_review_feedback_on_a_personal_installation_refuses_a_legacy_service_work_item(
    owners: Callable[[], uuid.UUID], install: GitLabInstall, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#5378: the review pass supplies the installation's mode, as tracker intake does.

    A WorkItem the label admitted as service before the installation switched
    to personal mode has no owner. On the personal installation a developer's
    review discussion on its merge request is refused
    (``personal_owner_required``): no revision, no owner invented, every tuple
    still service. Liveness: back on the service setting a new discussion
    admits a service revision.

    Falsifiable: a review pass that admits under service mode whatever the
    installation's mode queues the first discussion's revision.
    """

    backend = install.backend
    iid = gl.admit(install)
    (item,) = gl.work_items(iid)
    work_item_id = uuid.UUID(str(item["id"]))
    (first,) = gl.requests_of(work_item_id)
    service = (DEFAULT_TENANT_ID, "service", None, None)
    legacy = {work_item_id: service, first["id"]: service}
    assert _owner_tuples(work_item_id) == legacy
    mr, lineage_id = _open_merge_request(install, work_item_id)
    try:
        _installation_mode(monkeypatch, "personal")
        backend.mr_discussion(mr, backend.developer, gl.review_body(install))
        gl.reconcile()
        assert [row["id"] for row in gl.requests_of(work_item_id)] == [first["id"]]
        assert _owner_tuples(work_item_id) == legacy

        _installation_mode(monkeypatch, "service")
        backend.mr_discussion(mr, backend.developer, f"{gl.review_body(install)} and the docs")
        gl.reconcile()
        rows = gl.requests_of(work_item_id)
        assert len(rows) == 2, rows
        assert _owner_tuples(work_item_id) == {**legacy, rows[1]["id"]: service}
    finally:
        _forget_cursor(lineage_id)


@pytest.mark.parametrize("installation", ["service", "personal"])
def test_gitlab_review_feedback_on_a_personal_work_item_keeps_its_owner_tuple(
    owners: Callable[[], uuid.UUID],
    install: GitLabInstall,
    monkeypatch: pytest.MonkeyPatch,
    installation: str,
) -> None:
    """#5378: a reviewer's revision of a personal WorkItem runs as the WorkItem's owner.

    A developer who is not the owner leaves a review discussion. On a service
    and a personal installation alike the revision is admitted with the
    developer as its requester and the locked WorkItem's tuple as its owner,
    and a reconstructed worker resolver projects that owner. Reviewers never
    supply ownership. Liveness for the refusal above on a personal installation.
    """

    import personal_execution_support as support

    backend = install.backend
    owner = owners()
    work_item_id = _admit_personal(install, owner)
    (first,) = gl.requests_of(work_item_id)
    personal = (DEFAULT_TENANT_ID, "personal", owner, GENERATION)
    assert _owner_tuples(work_item_id) == {work_item_id: personal, first["id"]: personal}
    mr, lineage_id = _open_merge_request(install, work_item_id)
    try:
        _installation_mode(monkeypatch, installation)
        backend.mr_discussion(mr, backend.developer, gl.review_body(install))
        gl.reconcile()
        rows = gl.requests_of(work_item_id)
        assert len(rows) == 2, rows
        revision = rows[1]
        assert revision["requester"].startswith(f"gitlab:{backend.developer.id}:")
        assert _owner_tuples(work_item_id) == {
            work_item_id: personal,
            first["id"]: personal,
            revision["id"]: personal,
        }
        projected = support.resolve_execution_owners([revision["id"]], tenant_id=DEFAULT_TENANT_ID)
        resolved = projected[revision["id"]]
        assert resolved is not None
        assert (
            resolved.work_item_id,
            resolved.tenant_id,
            resolved.model_auth_mode,
            resolved.owner_principal_id,
            resolved.enrollment_generation,
        ) == (work_item_id, *personal)
    finally:
        _forget_cursor(lineage_id)

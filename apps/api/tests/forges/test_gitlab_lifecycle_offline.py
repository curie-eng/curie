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

Run with ``uv run pytest apps/api/tests -k gitlab_lifecycle``.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from curie_api.forges import types
from forge_fakes import gitlab_lifecycle as gl
from forge_fakes.gitlab_lifecycle import (
    BOT,
    DEVELOPER,
    FAST,
    LABEL,
    PROJECT_ID,
    PROJECT_PATH,
    REPORTER,
    TOKEN,
    GitLabInstall,
)

pytestmark = pytest.mark.usefixtures("clean_db")

REVIEW_BODY = f"@{BOT.login} please rename the helper before merge"
MENTION_BODY = f"@{BOT.login} please also handle tab separated lines"
BASIC_OAUTH2 = "Basic " + base64.b64encode(f"oauth2:{TOKEN}".encode()).decode()


def _reconcile() -> None:
    # Imported here: test modules are siblings on sys.path, not a package.
    from test_factory_terminus import _reconcile as reconcile

    reconcile()


@pytest.fixture
def install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path)


@pytest.fixture
def two_surfaces(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path, labels=(None, FAST))


def _admit(install: GitLabInstall, title: str = "Widget parser drops the last field") -> int:
    """A developer's issue, labelled by the developer and admitted by the poll."""

    iid = install.fake.add_issue(
        title, "Parsing a three-field line yields two fields.", author=DEVELOPER
    )
    install.fake.label(iid, LABEL, DEVELOPER)
    _reconcile()
    assert len(gl.work_items(iid)) == 1, gl.work_items(iid)
    return iid


def _no_token_anywhere(install: GitLabInstall, caplog: pytest.LogCaptureFixture) -> None:
    assert TOKEN not in caplog.text
    for response in install.responses:
        assert TOKEN not in response.text, response.request.url
    assert TOKEN not in json.dumps(gl.turns())
    assert all(TOKEN not in note.body for note in install.fake.notes.values())
    assert all(TOKEN not in mr.description for mr in install.fake.merge_requests.values())


# --- the lifecycle ----------------------------------------------------------------------------


def test_gitlab_lifecycle_label_to_merge_request_ci_review_and_cancel(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    fake = install.fake
    iid = fake.add_issue(
        "Widget parser drops the last field",
        "Parsing a three-field line yields two fields.",
        author=DEVELOPER,
    )

    # 1. The developer labels the issue; the real poll pass admits one WorkItem
    #    on the bound project, replying on the gitlab surface.
    fake.label(iid, LABEL, DEVELOPER)
    _reconcile()
    [item] = gl.work_items(iid)
    assert (
        item["tracker_host"],
        item["tracker_scope_id"],
        item["tracker_display_key"],
        item["trigger_label"],
    ) == (install.host, str(PROJECT_ID), f"{PROJECT_PATH}#{iid}", LABEL)
    assert (
        item["code_host_kind"],
        item["code_host_host"],
        item["repository_project_id"],
        item["repository_path"],
    ) == (types.GITLAB, install.host, str(PROJECT_ID), PROJECT_PATH)
    assert item["code_host_installation_id"] is None
    assert item["base_branch"] == "main"
    assert item["base_commit"] == fake.branches["main"]
    assert item["agent_id"] == install.agent_id
    [first] = gl.requests_of(item["id"])
    assert first["objective"].splitlines()[0] == install.issue_url(iid)
    assert (first["reply_kind"], first["reply_address"], first["reply_conversation_id"]) == (
        "gitlab",
        PROJECT_PATH,
        f"issue-{iid}",
    )
    assert first["requester"] == f"gitlab:{DEVELOPER.id}:{DEVELOPER.login}"
    assert first["status"] == "waiting"
    assert [t for t in gl.turns() if str(first["id"]) in t["event_id"]]
    [note] = install.bot_notes(iid)
    assert install.state_labels(iid) == {"curie-factory:queued"}
    posted = note.body

    # 2. The run starts: the same note is edited in place, the state label moves.
    epoch = gl.start_request(first["id"])
    _reconcile()
    assert gl.requests_of(item["id"])[0]["status"] == "running"
    [same] = install.bot_notes(iid)
    assert same.id == note.id and same.body != posted
    assert install.state_labels(iid) == {"curie-factory:running"}

    # 3. The workspace: the worker selects the WorkItem's path (no kind) and is
    #    issued the GitLab code host's clone credential, Basic oauth2.
    deployment_id = gl.deployment(install, item["agent_id"])
    selected = gl.select_workspace(install, deployment_id, item, first["requester"])
    assert selected.status_code == 200, selected.text
    assert selected.json()["repo_full_name"] == PROJECT_PATH
    clone = gl.workspace_credential(install, deployment_id, item)
    assert clone.status_code == 200, clone.text
    issued = clone.json()
    assert issued["clone_url"] == f"{install.origin}/{PROJECT_PATH}.git"
    assert issued["origin"] == install.origin
    assert issued["header_form"] == types.CredentialHeader.AUTHORIZATION_BASIC.value
    assert issued["authorization_header"] == BASIC_OAUTH2
    assert (issued["base_branch"], issued["base_commit"]) == ("main", item["base_commit"])

    # 4. The publication carries the closing reference; its push credential is
    #    the GitLab code host's; the merge request opens through the recover route
    #    and the lineage records the GitLab MR URL.
    request_row = {**gl.requests_of(item["id"])[0], "runtime_epoch": epoch}
    created = gl.publish(install, deployment_id, item, request_row)
    assert created.status_code == 201, created.text
    publication = created.json()
    assert publication["body"].count(f"Closes #{iid}") == 1
    gl.approve(publication["id"])
    push = gl.publication_credential(install, publication["id"])
    assert push.status_code == 200, push.text
    assert push.json()["clone_url"] == f"{install.origin}/{PROJECT_PATH}.git"
    assert push.json()["authorization_header"] == BASIC_OAUTH2
    head = fake.push(publication["branch"], "Fix the widget parser")
    recovered = gl.recover_pull_request(install, publication["id"], head)
    assert recovered.status_code == 200, recovered.text
    pull = recovered.json()
    mr = install.merge_request()
    assert (pull["number"], pull["url"], pull["head_sha"]) == (mr.iid, install.mr_url(mr.iid), head)
    assert (mr.source_branch, mr.target_branch) == (publication["branch"], "main")
    assert f"Closes #{iid}" in mr.description
    advanced = gl.advance_lineage(
        install,
        publication,
        pr_number=mr.iid,
        pr_url=pull["url"],
        head_sha=head,
        expected_head_sha=None,
    )
    assert advanced.status_code == 200, advanced.text
    lineage = gl.lineage_of(item["id"])
    assert (lineage["pr_number"], lineage["pr_url"], lineage["head_sha"], lineage["status"]) == (
        mr.iid,
        install.mr_url(mr.iid),
        head,
        "open",
    )
    # The lineage records the GitLab identity, which is what links it to the
    # WorkItem (installation is GitHub's alone).
    assert (
        lineage["code_host_kind"],
        lineage["code_host_host"],
        lineage["repository_project_id"],
        lineage["base_ref"],
    ) == (types.GITLAB, install.host, str(PROJECT_ID), "main")
    assert lineage["code_host_pr_id"]
    # The worker's lineage read refreshes the stored MR through the GitLab code host.
    read = gl.read_lineage(install, deployment_id, item)
    assert read.status_code == 200, read.text
    assert (read.json()["pr_url"], read.json()["head_sha"]) == (install.mr_url(mr.iid), head)

    # 5. CI goes red on the head. GitLab can rerun a failed job, so the gate
    #    retries it once; when the rerun fails too, a fix round is asked for.
    failed_job = fake.report_job(head, "test", "failed")
    _reconcile()
    assert [
        fake.api_path(r) for r in fake.calls("POST", "/projects/*") if "retry" in str(r.url)
    ] == [f"/projects/{PROJECT_ID}/jobs/{failed_job}/retry"]
    assert gl.ci_turns(first["id"]) == []
    fake.report_job(head, "test", "failed")
    _reconcile()
    assert gl.requests_of(item["id"])[0]["status"] == "running"
    fix_turns = gl.ci_turns(first["id"])
    assert [t["event_id"] for t in fix_turns] == [f"work-item-{first['id']}-ci-2"]
    assert install.mr_url(mr.iid) in fix_turns[0]["text"]
    assert head in fix_turns[0]["text"]

    #    The fix round publishes a revision of the same MR the way the worker does:
    #    lineage read, precheck mint (read through the GitLab code host), the
    #    revision publication, the push, the lineage advance. Then CI is green.
    assert gl.read_lineage(install, deployment_id, item).status_code == 200
    minted = gl.mint_precheck(
        install, deployment_id, item, first["id"], epoch, fix_turns[0]["event_id"]
    )
    assert minted.status_code == 200, minted.text
    context = minted.json()
    assert context["observed_title"] == publication["title"]
    gl.outcome_durable(publication["id"])
    revised = gl.publish(
        install,
        deployment_id,
        item,
        request_row,
        title="Fix the widget parser off-by-one",
        base_sha=head,
        observed={
            "observed_title": context["observed_title"],
            "observed_body_sha256": context["observed_body_sha256"],
            "observed_lineage_id": context["lineage_id"],
            "observed_lineage_version": context["lineage_version"],
        },
    )
    assert revised.status_code == 201, revised.text
    revision_publication = revised.json()
    gl.approve(revision_publication["id"])
    fix_head = fake.push(publication["branch"], "Fix the widget parser off-by-one")
    fixed = gl.advance_lineage(
        install,
        revision_publication,
        pr_number=mr.iid,
        pr_url=install.mr_url(mr.iid),
        head_sha=fix_head,
        expected_head_sha=head,
    )
    assert fixed.status_code == 200, fixed.text
    fake.report_job(fix_head, "test", "success")
    _reconcile()
    assert gl.requests_of(item["id"])[0]["status"] == "completed"
    assert len(gl.ci_turns(first["id"])) == 1
    [final] = install.bot_notes(iid)
    assert final.id == note.id
    assert install.mr_url(mr.iid) in final.body
    assert install.state_labels(iid) == {"curie-factory:pr-open"}

    # 6. A developer's diff comment becomes a revision, answered by exactly one
    #    reply in its discussion.
    discussion = fake.mr_discussion(mr.iid, DEVELOPER, REVIEW_BODY, diff=True)
    _reconcile()
    rows = gl.requests_of(item["id"])
    assert len(rows) == 2, rows
    revision = rows[1]
    assert (revision["reply_kind"], revision["reply_address"]) == ("gitlab", PROJECT_PATH)
    assert revision["reply_target_kind"] == "review_thread"
    assert revision["reply_target_pr_number"] == str(mr.iid)
    assert revision["status"] == "waiting"
    assert install.mr_url(mr.iid) in revision["objective"]
    assert revision["requester"].startswith(f"gitlab:{DEVELOPER.id}:")
    _reconcile()  # the same note again admits nothing new
    assert len(gl.requests_of(item["id"])) == 2
    assert len(gl.work_items(iid)) == 1
    gl.start_request(revision["id"])
    _reconcile()
    _reconcile()
    replies = [n for n in fake.thread_notes(discussion)[1:] if not n.system]
    assert [n.author for n in replies] == [int(BOT.id)]

    # 7. Removing the label cancels the WorkItem.
    fake.label(iid, LABEL, DEVELOPER, add=False)
    _reconcile()
    [cancelled] = gl.work_items(iid)
    assert cancelled["cancelled_at"] is not None
    assert all(r["status"] not in {"waiting", "running"} for r in gl.requests_of(item["id"]))

    # The token never left the API: not in a log line, a raised error's log, a
    # route answer, a turn, a note or the MR.
    _no_token_anywhere(install, caplog)


def test_gitlab_lifecycle_a_reporter_label_admits_nothing_and_sets_no_state_label(
    install: GitLabInstall,
) -> None:
    fake = install.fake
    iid = fake.add_issue("Widget parser drops a quoted field", "Quoted commas split.")

    fake.label(iid, LABEL, REPORTER)
    _reconcile()
    _reconcile()

    assert gl.work_items(iid) == []
    assert install.state_labels(iid) == set()
    assert install.bot_notes(iid) == []

    # Liveness: the developer's label on the next issue is admitted.
    other = _admit(install, "Widget parser drops a trailing field")
    assert install.state_labels(other) == {"curie-factory:queued"}


def test_gitlab_lifecycle_two_surfaces_admit_only_their_own_label(
    two_surfaces: GitLabInstall,
) -> None:
    install = two_surfaces
    fake = install.fake
    plain = fake.add_issue("Widget parser drops the last field")
    quick = fake.add_issue("Widget parser drops a quoted field")

    fake.label(plain, LABEL, DEVELOPER)
    fake.label(quick, FAST, DEVELOPER)
    _reconcile()
    _reconcile()

    [plain_item] = gl.work_items(plain)
    [quick_item] = gl.work_items(quick)
    assert (plain_item["agent_id"], plain_item["trigger_label"]) == (install.agents[None], LABEL)
    assert (quick_item["agent_id"], quick_item["trigger_label"]) == (install.agents[FAST], FAST)

    # A second surface with the same project and label is refused, whether the
    # label is named or is the tracker's own; another label binds.
    for label in (None, LABEL, FAST):
        refused = gl.create_agent(install, label)
        assert refused.status_code == 409, (label, refused.text)
    assert gl.create_agent(install, "factory-slow").status_code == 201


def test_gitlab_lifecycle_an_issue_mention_links_the_note_and_counts_as_a_follow_up(
    install: GitLabInstall,
) -> None:
    from curie_api.factory_reply_target import is_issue_follow_up
    from curie_api.forges.trackers import comment_prefix

    fake = install.fake
    iid = _admit(install)
    [item] = gl.work_items(iid)

    # A reporter's mention starts nothing.
    fake.issue_note(iid, REPORTER, MENTION_BODY)
    _reconcile()
    assert len(gl.requests_of(item["id"])) == 1

    note_id = fake.issue_note(iid, DEVELOPER, MENTION_BODY)
    _reconcile()
    rows = gl.requests_of(item["id"])
    assert len(rows) == 2, rows
    link = install.issue_url(iid)
    assert rows[1]["objective"].splitlines()[0] == f"{link}#note_{note_id}"
    assert is_issue_follow_up(
        rows[1]["objective"], issue_link=link, comment_prefix=comment_prefix(types.GITLAB)
    )


def test_gitlab_lifecycle_failures_log_and_answer_no_token(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    fake = install.fake
    iid = _admit(install)
    [item] = gl.work_items(iid)
    [first] = gl.requests_of(item["id"])
    gl.start_request(first["id"])
    deployment_id = gl.deployment(install, item["agent_id"])
    assert gl.select_workspace(install, deployment_id, item, first["requester"]).status_code == 200

    # Every GitLab error body now quotes the presented token and URL, and the
    # token stops working: each pass and route fails, and none repeats it.
    fake.echo_token_in_errors = True
    fake.tokens[TOKEN].revoked = True
    other = fake.add_issue("Widget parser drops a trailing field")
    fake.label(other, LABEL, DEVELOPER)
    _reconcile()
    refused = gl.workspace_credential(install, deployment_id, item)
    assert refused.status_code == 502, refused.text

    assert gl.work_items(other) == []
    _no_token_anywhere(install, caplog)
    # Liveness of the probe: the fake really did quote it to the API.
    assert any(TOKEN in r.headers.get("private-token", "") for r in fake.requests[-5:]), (
        "the API stopped calling GitLab"
    )

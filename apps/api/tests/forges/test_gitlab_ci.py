"""GitLab CI behind the code host port (#4399 AC 4; decisions 18 and 19).

Checks are read on exactly one head: the head's pipelines, plus a merged
results pipeline only when its merge commit's parents include the head. Jobs
are native ``RUN`` checks keyed by name; external commit statuses are
``STATUS`` checks keyed ``status:<name>``.
"""

from __future__ import annotations

import httpx
import pytest
from curie_api.forges.types import (
    CheckSource,
    CheckState,
    RerunOutcome,
    RollupState,
    check_run_key,
    failed_native_jobs,
    status_key,
)
from curie_telemetry.redact import redact_text
from forge_fakes.contract_harness import WRITER
from forge_fakes.gitlab import PROJECT_ID, WEB, GitLabAPI, gitlab_code_host

PROJECT = f"/projects/{PROJECT_ID}"
# Assembled at runtime so no token-shaped literal sits in the tree; the
# "fixture" word marks it a placeholder for the recordings hygiene scan.
TOKEN_SHAPED = "glpat" + "-" + "fixture" * 3


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fake() -> GitLabAPI:
    fake = GitLabAPI()
    fake.add_user(WRITER)
    fake.set_member(WRITER, 30)
    return fake


def _head(fake: GitLabAPI, branch: str = "factory/ci") -> tuple[int, str]:
    sha = fake.push(branch)
    return fake.add_merge_request(branch), sha


# Decision 18: the state map ---


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "allow_failure", "state"),
    [
        ("success", False, CheckState.SUCCESS),
        ("failed", False, CheckState.FAILURE),
        ("failed", True, CheckState.NEUTRAL),
        ("canceled", False, CheckState.CANCELLED),
        ("skipped", False, CheckState.SKIPPED),
        ("manual", False, CheckState.SKIPPED),
        ("created", False, CheckState.PENDING),
        ("waiting_for_resource", False, CheckState.PENDING),
        ("preparing", False, CheckState.PENDING),
        ("pending", False, CheckState.PENDING),
        ("running", False, CheckState.PENDING),
        ("scheduled", False, CheckState.PENDING),
        # A word decision 18 does not name waits; it never passes.
        ("canceling", False, CheckState.PENDING),
    ],
)
async def test_every_job_status_maps_as_decided(
    fake: GitLabAPI, status: str, allow_failure: bool, state: CheckState
) -> None:
    _, head = _head(fake)
    job = fake.report_job(head, "unit", status, allow_failure=allow_failure)

    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), head)

    (check,) = rollup.checks
    assert (check.key, check.name, check.state, check.reported_state) == (
        "unit",
        "unit",
        state,
        status,
    )
    assert (check.source, check.native, check.check_id) == (CheckSource.RUN, True, str(job))
    assert check.head_sha == head
    assert check.url == f"{WEB}/acme/platform/widgets/-/jobs/{job}"


@pytest.mark.anyio
async def test_jobs_are_read_without_retried_ones_and_the_newest_per_name_wins(
    fake: GitLabAPI,
) -> None:
    _, head = _head(fake)
    fake.report_job(head, "unit", "failed")
    fake.report_job(head, "unit", "running")  # a retry: the failed job is now retried
    later = fake.add_pipeline(head, jobs={"unit": "success", "lint": "success"})
    newest = next(j.id for j in fake.jobs.values() if j.pipeline == later and j.name == "unit")

    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), head)

    assert sorted((c.key, c.state) for c in rollup.checks) == [
        ("lint", CheckState.SUCCESS),
        ("unit", CheckState.SUCCESS),
    ]
    assert next(c.check_id for c in rollup.checks if c.key == "unit") == str(newest)
    assert rollup.state is RollupState.SUCCESS
    for listing in fake.calls("GET", f"{PROJECT}/pipelines/*"):
        if fake.api_path(listing).endswith("/jobs"):
            assert fake.params(listing).get("include_retried") == "false"


@pytest.mark.anyio
async def test_a_job_named_like_a_status_key_is_escaped(fake: GitLabAPI) -> None:
    _, head = _head(fake)
    fake.report_job(head, "status:docs", "success")

    (check,) = (await gitlab_code_host(fake).observe_ci(fake.repository(), head)).checks

    assert check.key == check_run_key("status:docs") == "check:status:docs"


@pytest.mark.anyio
async def test_an_external_status_is_a_status_check_and_jobs_are_not_counted_twice(
    fake: GitLabAPI,
) -> None:
    _, head = _head(fake)
    fake.report_job(head, "unit", "success")
    fake.add_status(head, "docs", "failed")

    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), head)

    assert sorted((c.key, c.source, c.state) for c in rollup.checks) == [
        (status_key("docs"), CheckSource.STATUS, CheckState.FAILURE),
        ("unit", CheckSource.RUN, CheckState.SUCCESS),
    ]
    docs = next(c for c in rollup.checks if c.source is CheckSource.STATUS)
    assert (docs.native, docs.reported_state) == (False, "failed")
    assert rollup.state is RollupState.FAILURE


def _pipeline_lands_after(fake: GitLabAPI, head: str, read: str) -> httpx.AsyncClient:
    """A client whose fake gets a new push pipeline (job ``test`` pending) on
    ``head`` right after the first answered GET whose path ends with ``read``:
    a push pipeline GitLab creates while the adapter is between two reads."""

    landed: list[int] = []

    def handle(request: httpx.Request) -> httpx.Response:
        response = fake.handle(request)
        if not landed and request.method == "GET" and fake.api_path(request).endswith(read):
            landed.append(fake.add_pipeline(head, jobs={"test": "pending"}))
        return response

    return httpx.AsyncClient(transport=httpx.MockTransport(handle))


@pytest.mark.anyio
@pytest.mark.parametrize(
    "read", ["/version", "/statuses", "/pipelines", "/merge_requests", "/jobs"]
)
async def test_a_push_pipeline_created_mid_read_never_shows_its_job_as_a_status(
    fake: GitLabAPI, read: str
) -> None:
    _, head = _head(fake)
    fake.report_job(head, "unit", "success")
    fake.add_status(head, "docs", "success")
    # GitLab attaches an external status to the commit's push pipeline.
    assert [s.pipeline for s in fake.statuses] == [
        next(p.id for p in fake.pipelines.values() if p.sha == head)
    ]

    client = _pipeline_lands_after(fake, head, read)
    rollup = await gitlab_code_host(fake, client=client).observe_ci(fake.repository(), head)

    assert len([p for p in fake.pipelines.values() if p.sha == head]) == 2
    statuses = [c for c in rollup.checks if c.source is CheckSource.STATUS]
    assert [(c.key, c.state) for c in statuses] == [(status_key("docs"), CheckState.SUCCESS)]
    assert status_key("test") not in {c.key for c in rollup.checks}
    # The new job is either not seen yet or seen as the native job it is.
    assert {c.key for c in rollup.checks} - {"unit", status_key("docs")} <= {"test"}


@pytest.mark.anyio
async def test_checks_on_another_commit_never_appear(fake: GitLabAPI) -> None:
    _, old = _head(fake)
    fake.report_job(old, "unit", "success")
    fake.add_status(old, "docs", "success")
    new = fake.push("factory/ci")

    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), new)

    assert (rollup.state, rollup.checks) == (RollupState.NONE, ())


# Decision 18: merged results pipelines ---


@pytest.mark.anyio
async def test_a_merged_results_pipeline_counts_when_its_parents_include_the_head(
    fake: GitLabAPI,
) -> None:
    iid, head = _head(fake)
    fake.add_merged_results_pipeline(
        iid, parents=(fake.branches["main"], head), jobs={"unit": "failed"}
    )

    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), head)

    assert [(c.key, c.state, c.head_sha) for c in rollup.checks] == [
        ("unit", CheckState.FAILURE, head)
    ]
    assert fake.calls("GET", f"{PROJECT}/repository/commits/{head}/merge_requests")


@pytest.mark.anyio
async def test_a_merged_results_pipeline_of_another_head_does_not_count(fake: GitLabAPI) -> None:
    iid, old = _head(fake)
    fake.add_merged_results_pipeline(
        iid, parents=(fake.branches["main"], old), jobs={"unit": "success"}
    )
    new = fake.push("factory/ci")

    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), new)

    assert (rollup.state, rollup.checks) == (RollupState.NONE, ())


# Decision 19: diagnostics ---


@pytest.mark.anyio
async def test_a_failing_jobs_excerpt_is_the_last_200_lines_redacted(fake: GitLabAPI) -> None:
    _, head = _head(fake)
    lines = [f"line-{n:03d}" for n in range(1, 251)]
    lines[-2] = f"export GITLAB_TOKEN={TOKEN_SHAPED}"
    pipeline = fake.add_pipeline(head)
    job = fake.add_job(pipeline, "unit", "failed", trace="\n".join(lines) + "\n")
    fake.add_job(pipeline, "lint", "success")

    report = await gitlab_code_host(fake).ci_diagnostics(fake.repository(), head, base_ref=None)

    assert report.rollup.state is RollupState.FAILURE
    (diagnostic,) = report.diagnostics
    assert (diagnostic.check_key, diagnostic.check_id) == ("unit", str(job))
    assert diagnostic.log is not None and not diagnostic.log_unavailable
    assert "line-050" not in diagnostic.log
    assert "line-051" in diagnostic.log and "line-250" in diagnostic.log
    assert TOKEN_SHAPED not in diagnostic.log and TOKEN_SHAPED not in diagnostic.excerpt
    assert redact_text(lines[-2]) in diagnostic.log
    assert fake.calls("GET", f"{PROJECT}/jobs/{job}/trace")
    assert report.base is None


@pytest.mark.anyio
async def test_an_unreadable_trace_is_log_unavailable(fake: GitLabAPI) -> None:
    _, head = _head(fake)
    job = fake.report_job(head, "unit", "failed")
    fake.script(500, path=f"{PROJECT}/jobs/{job}/trace")

    report = await gitlab_code_host(fake).ci_diagnostics(fake.repository(), head, base_ref=None)

    (diagnostic,) = report.diagnostics
    assert diagnostic.log_unavailable is True


@pytest.mark.anyio
async def test_a_failing_external_status_reports_its_description(fake: GitLabAPI) -> None:
    # Parity with the GitHub adapter's commit statuses: an external gate says
    # what it wants only in its description, so dropping it left the factory
    # with no CI findings and a metadata-only republish (live #4399 run).
    _, head = _head(fake)
    said = f"Add a CHANGELOG.md line. GITLAB_TOKEN={TOKEN_SHAPED}"
    gate = fake.add_status(head, "loop-gate", "failed", description=said)
    quiet = fake.add_status(head, "other-gate", "failed")
    fake.add_status(head, "passing-gate", "success", description="fine")

    report = await gitlab_code_host(fake).ci_diagnostics(fake.repository(), head, base_ref=None)

    assert report.rollup.state is RollupState.FAILURE
    by_id = {d.check_id: d for d in report.diagnostics}
    assert set(by_id) == {str(gate), str(quiet)}
    loud = by_id[str(gate)]
    assert loud.summary == redact_text(said) and loud.excerpt == redact_text(said)
    assert "CHANGELOG.md" in loud.excerpt and TOKEN_SHAPED not in loud.excerpt
    assert (loud.log, loud.log_unavailable) == (None, False)
    assert (by_id[str(quiet)].summary, by_id[str(quiet)].excerpt) == (None, "")


@pytest.mark.anyio
async def test_an_unreadable_status_listing_still_reports_the_failing_status(
    fake: GitLabAPI,
) -> None:
    _, head = _head(fake)
    gate = fake.add_status(head, "loop-gate", "failed", description="Add a line.")
    ci = gitlab_code_host(fake)._ci(fake.repository())
    rollup = await ci.observe(head)
    fake.script(500, path=f"{PROJECT}/repository/commits/{head}/statuses")

    (diagnostic,) = await ci.diagnostics(rollup)

    assert (diagnostic.check_id, diagnostic.summary, diagnostic.excerpt) == (str(gate), None, "")


@pytest.mark.anyio
async def test_a_head_failure_reads_the_base_branchs_checks(fake: GitLabAPI) -> None:
    base = fake.branches["main"]
    fake.report_job(base, "unit", "failed")
    _, head = _head(fake)
    fake.report_job(head, "unit", "failed")

    report = await gitlab_code_host(fake).ci_diagnostics(fake.repository(), head, base_ref="main")

    assert report.base is not None
    assert (report.base.head_sha, report.base.state) == (base, RollupState.FAILURE)


@pytest.mark.anyio
async def test_a_passing_head_reads_no_base(fake: GitLabAPI) -> None:
    fake.report_job(fake.branches["main"], "unit", "failed")
    _, head = _head(fake)
    fake.report_job(head, "unit", "success")

    report = await gitlab_code_host(fake).ci_diagnostics(fake.repository(), head, base_ref="main")

    assert (report.rollup.state, report.base, report.diagnostics) == (
        RollupState.SUCCESS,
        None,
        (),
    )


# Decision 19: rerun ---


async def _failed_jobs(fake: GitLabAPI, head: str) -> tuple:
    rollup = await gitlab_code_host(fake).observe_ci(fake.repository(), head)
    return failed_native_jobs(rollup.checks)


@pytest.mark.anyio
async def test_rerun_failed_retries_each_failed_job_once(fake: GitLabAPI) -> None:
    _, head = _head(fake)
    unit = fake.report_job(head, "unit", "failed")
    lint = fake.report_job(head, "lint", "canceled")
    fake.report_job(head, "docs", "success")
    code_host = gitlab_code_host(fake)
    jobs = await _failed_jobs(fake, head)

    record = await code_host.rerun_failed(fake.repository(), jobs)

    retried = [fake.api_path(r) for r in fake.requests if r.method == "POST"]
    assert sorted(retried) == sorted(
        [f"{PROJECT}/jobs/{unit}/retry", f"{PROJECT}/jobs/{lint}/retry"]
    )
    assert sorted(a.unit for a in record.attempts) == sorted([str(unit), str(lint)])
    assert all(a.outcome is RerunOutcome.ACCEPTED for a in record.attempts)
    assert (await code_host.observe_ci(fake.repository(), head)).state is RollupState.PENDING

    again = await code_host.rerun_failed(
        fake.repository(), record.jobs, settled={a.unit for a in record.attempts}
    )
    assert again.attempts == ()
    assert len([r for r in fake.requests if r.method == "POST"]) == 2


@pytest.mark.anyio
async def test_a_server_error_on_retry_stops_the_call(fake: GitLabAPI) -> None:
    _, head = _head(fake)
    fake.report_job(head, "unit", "failed")
    fake.report_job(head, "lint", "failed")
    jobs = await _failed_jobs(fake, head)
    fake.script(502, path=f"{PROJECT}/jobs/*", method="POST")

    record = await gitlab_code_host(fake).rerun_failed(fake.repository(), jobs)

    assert [a.outcome for a in record.attempts] == [RerunOutcome.RETRY]
    assert len([r for r in fake.requests if r.method == "POST"]) == 1


@pytest.mark.anyio
async def test_a_refused_retry_moves_on_to_the_next_job(fake: GitLabAPI) -> None:
    _, head = _head(fake)
    fake.report_job(head, "unit", "failed")
    fake.report_job(head, "lint", "failed")
    jobs = await _failed_jobs(fake, head)
    fake.script(403, path=f"{PROJECT}/jobs/*", method="POST")

    record = await gitlab_code_host(fake).rerun_failed(fake.repository(), jobs)

    assert [a.outcome for a in record.attempts] == [RerunOutcome.REFUSED, RerunOutcome.ACCEPTED]

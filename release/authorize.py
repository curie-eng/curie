#!/usr/bin/env python3
"""Decide whether a pushed tag is authorized to publish a release (issue #628).

`release.yaml` triggers its full publish pipeline on any `v*` tag push. A tag is
just a ref; pushing one proves nothing about the commit it points at. This
script is the gate `authorize-release` runs before any other job in that
pipeline, and it fails closed on either of two questions:

  ancestry  Is the tagged commit reachable from an explicitly supplied
            reviewed branch? A tag on a feature branch, a rebased commit, or
            anything that bypassed a merged PR is refused here, before any
            image builds. A final semver tag must be reachable from the
            primary reviewed ref (the first `--reviewed-ref`, `origin/main`
            in the release workflow). A prerelease may come from any
            reviewed ref so `next` RCs still publish (issues #1476, #1560).

  checks    Does that commit's check-runs list show every REQUIRED_CHECK_NAMES
            entry successful (or neutral)? An explicit allowlist,
            not "some checks, all green" (issue #733): a commit with only an
            unrelated passing check-run and no sign its real CI ever ran
            must be refused, the same as one that is on a reviewed branch but
            was never checked, or was checked and failed. Zero check-runs is
            also a failure -- absence of checks is not evidence they passed. A
            required check-run that concluded `skipped` is refused too
            (issue #1470): a job-level `if:` on the job, or a failed job in
            its `needs:`, both make GitHub record that required check as
            `skipped`, and a chart that was never rendered, linted, or
            kubeconform-validated must not authorize a release. A skipped
            check-run whose name is NOT required is still ignored, like any
            other non-required check. The
            gate's own workflow run is excluded from that list (issue #732):
            it is itself an in-progress check-run on the tagged SHA and
            would otherwise wait on itself forever.

Both live as separately-testable functions so the negative case -- an
unreviewed or check-less commit is refused -- is an ordinary pytest assertion
against a constructed fixture, not a manual demonstration against the real
repo. Only `main()` needs the network (`gh api` for the live check-run list);
see `release/integrity.py` for the same manifest/verify split rationale.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

PASSING_CONCLUSIONS = {"success", "neutral"}

# The check-run names that must be present and green before a tag may
# publish (issue #733). These are the job `name:` fields from
# `.github/workflows/ci.yaml`, the workflow that gates every PR and push to
# a reviewed branch, restricted to jobs that speak to release confidence: the
# three language/test jobs, the generated-artifact drift checks, the
# release-compose validation, every first-party image actually building
# (including the worker-local overlay and the dispatcher's own import
# smoke-test), and the two behavioral gates (eval falsifiability, the E2E
# parity ladder) that ci.yaml's own comments describe as catching bug
# classes no unit test does. Checks from other workflows (CodeQL, the
# dependency/secret scanners, release.yaml's own jobs) are deliberately
# excluded -- they matter, but are not what this gate is asserting about
# *this* commit's CI.
# The chart check is included despite living in another workflow because it
# validates the release chart itself.
#
# This list is a plain constant, not derived from ci.yaml or helm-ci.yaml at
# runtime. It is the simpler of the two options ADR-0058 left open (issue
# #733). Tests pin its required names to jobs in both workflows, so a job
# rename or removal requires a matching edit here.
REQUIRED_CHECK_NAMES = frozenset(
    {
        "Python (ruff + mypy + pytest)",
        "Rust (fmt + clippy + test)",
        "Contracts (generated TypeScript compiles)",
        "UI (lint + vitest + build + Playwright)",
        "Compose (release stack validates)",
        "Build runner image (no push)",
        "Build api image (no push)",
        "Build dispatcher image (no push)",
        "Build worker image (no push)",
        "Build ui image (no push)",
        "Build sre-bot-tempo image (no push)",
        "Build worker-local overlay image (no push)",
        "Dispatcher image imports resolve",
        "Eval falsifiability gate (fake model, offline)",
        "E2E parity ladder (skill + local, fake model)",
        "E2E parity ladder (local-release, fake model)",
        "Chart (lint + template + kubeconform)",
    }
)


class AuthorizationError(Exception):
    """A tagged commit is not authorized to publish a release."""


# Semver 2.0.0 with an optional leading `v`. A non-empty prerelease
# identifier is the tag class; build metadata does not make a tag a
# prerelease.
_SEMVER_TAG = re.compile(
    r"^v?(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
    r"(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*)?$"
)


def classify_release_tag(tag: str) -> str:
    """Return `final` or `prerelease` for a semver `v*` tag.

    Strips a `refs/tags/` prefix so either `GITHUB_REF` or `GITHUB_REF_NAME`
    can be passed. Raises AuthorizationError on a non-semver name so a tag
    that the workflow's `v*` glob accepted but that has no prerelease
    component we can trust cannot publish as a final release by accident.
    """
    raw = tag.strip()
    if raw.startswith("refs/tags/"):
        raw = raw.removeprefix("refs/tags/")
    match = _SEMVER_TAG.fullmatch(raw)
    if match is None:
        raise AuthorizationError(
            f"tag {tag!r} is not a semver v* tag; refusing to authorize this tag"
        )
    if match.group("prerelease"):
        return "prerelease"
    return "final"


def commit_is_on_reviewed_branch(
    sha: str, reviewed_ref: str, *, cwd: Path | None = None
) -> bool:
    """Is `sha` reachable from `reviewed_ref` in the repo at `cwd`?

    Requires full history (`fetch-depth: 0` in the workflow checkout); a
    shallow clone would make an ancestor commit unreachable and read as absent.
    """
    try:
        result = subprocess.run(
            ["git", "merge-base", "--is-ancestor", sha, reviewed_ref],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise AuthorizationError(
            f"could not determine whether commit {sha} is reachable from "
            f"{reviewed_ref}: {exc}"
        ) from exc

    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False

    detail = result.stderr.strip()
    suffix = f": {detail}" if detail else ""
    raise AuthorizationError(
        f"could not determine whether commit {sha} is reachable from "
        f"{reviewed_ref}; git merge-base returned {result.returncode}{suffix}"
    )


def missing_required_checks(
    check_runs: list[dict[str, object]],
    required_names: frozenset[str] | None = None,
) -> set[str]:
    """Which `required_names` have no passing check-run for this commit?

    A name only counts as satisfied if some check-run with that exact `name`
    concluded `success`/`neutral` AND no check-run with that name is
    still running, failed, skipped, or otherwise non-passing. `skipped` is not
    a pass for a required name (issue #1470): GitHub records that conclusion
    both when a job-level `if:` excludes the job and when a job in its
    `needs:` failed, so treating it as passing authorized releases whose chart
    was never rendered, linted, or kubeconform-validated, and whose ladder
    jobs never ran because their upstream build failed. A required name with two
    check-runs -- one success and one failure (a re-run with mixed states) --
    is left in the returned set: for a fail-closed release gate any non-passing
    run of a required name masks nothing and blocks the tag. A same-named entry
    that is still running, failed, or never ran at all thus leaves its name in
    the returned set. Names outside `required_names` are ignored entirely -- an
    unrelated check-run, passing or not, has no bearing on this gate (issue
    #733): the point is asserting the checks that matter actually ran and
    passed, not that everything present happened to be green. That still holds
    for a `skipped` NON-required check-run, which is ignored like any other
    non-required entry; only a skipped *required* name blocks.

    `required_names` defaults to the module-level `REQUIRED_CHECK_NAMES`,
    looked up here rather than bound as the parameter's default value so that
    tests can override the module constant and have every caller (including
    `main()`, which never passes this through explicitly) pick it up.
    """
    if required_names is None:
        required_names = REQUIRED_CHECK_NAMES
    passed_names = {
        run.get("name") for run in check_runs if run.get("conclusion") in PASSING_CONCLUSIONS
    }
    non_passing_required = {
        name
        for name in required_names
        if any(
            run.get("name") == name and run.get("conclusion") not in PASSING_CONCLUSIONS
            for run in check_runs
        )
    }
    return (set(required_names) - passed_names) | non_passing_required


def exclude_current_workflow_run(
    check_runs: list[dict[str, object]], run_id: str | None
) -> list[dict[str, object]]:
    """Drop the check-runs belonging to the workflow run doing the asking.

    On a tag push the gate's own job is itself a check-run on the tagged SHA,
    with `status: in_progress` and `conclusion: null`, so it would refuse every
    legitimate release by waiting on itself. Only the current run is excluded --
    a blanket "ignore every null conclusion" would let a genuinely stuck
    unrelated required check through, which is the opposite of failing closed.

    A check-run's `details_url` is its job URL, observed live as
    `https://github.com/curie-eng/curie/actions/runs/<run_id>/job/<job_id>`,
    so the run id embedded in that path is the ownership signal. An entry with
    no `details_url` (an external app's check) can never be ours, so it stays.
    """
    if not run_id:
        return check_runs
    marker = f"/actions/runs/{run_id}/"
    return [
        run for run in check_runs if marker not in str(run.get("details_url") or "")
    ]


def authorize(
    sha: str,
    check_runs: list[dict[str, object]],
    reviewed_refs: Sequence[str],
    *,
    cwd: Path | None = None,
    exclude_run_id: str | None = None,
    required_names: frozenset[str] | None = None,
    tag: str | None = None,
) -> None:
    """Raise AuthorizationError unless `sha` may publish a release.

    `tag` is the pushed tag name (`v0.7.0`, `v0.7.0-rc.1`). A missing tag
    is treated as final (fail closed) so omitting the class cannot widen
    the reviewed-ref set. `main()` always passes `--tag`. A final tag must
    be reachable from the first reviewed ref (the stable line); a
    prerelease may use any reviewed ref (issue #1560).
    """
    if not reviewed_refs:
        raise AuthorizationError(
            "at least one reviewed ref is required; refusing to authorize this tag"
        )

    reachability = [
        commit_is_on_reviewed_branch(sha, reviewed_ref, cwd=cwd)
        for reviewed_ref in reviewed_refs
    ]
    if not any(reachability):
        checked_refs = ", ".join(reviewed_refs)
        raise AuthorizationError(
            f"commit {sha} is not reachable from any reviewed ref "
            f"({checked_refs}); refusing to authorize this tag"
        )
    tag_class = "final" if tag is None else classify_release_tag(tag)
    if tag_class == "final":
        primary_ref = reviewed_refs[0]
        if not reachability[0]:
            tag_label = tag if tag is not None else "unspecified"
            raise AuthorizationError(
                f"commit {sha} is not reachable from {primary_ref}; "
                f"a final tag ({tag_label}) requires the merge-then-tag sequence: "
                f"merge the reviewed branch into {primary_ref}, then tag. "
                "Refusing to authorize this tag"
            )
    other_runs = exclude_current_workflow_run(check_runs, exclude_run_id)
    missing = missing_required_checks(other_runs, required_names)
    if missing:
        raise AuthorizationError(
            f"commit {sha} is missing {len(missing)} required check-run(s) "
            f"({len(other_runs)} check-runs found for the commit, excluding "
            "this workflow run's own): "
            f"{', '.join(sorted(missing))}. Refusing to authorize this tag "
            "until its required checks are current and green."
        )


def fetch_check_runs(
    sha: str, repo: str, *, per_page: int = 100
) -> list[dict[str, object]]:
    """The commit's check-runs via `gh api`, which carries GITHUB_TOKEN auth.

    Paginates explicitly rather than assuming one page covers it (issue
    #733): a real commit on this repo's `main` was measured with dozens of
    check-runs (CodeQL, dependency/secret scanners, and every ci.yaml job,
    several of them matrixed), comfortably past the endpoint's own default
    page size of 30, and there is no ceiling on that growing further as more
    workflows land. `per_page=100` shrinks the common case to one request,
    but the loop below keeps requesting subsequent pages -- using the
    response's own `total_count` as the stopping point -- until every
    check-run has been collected, so correctness does not depend on staying
    under any particular count. This endpoint's top level is an object, not
    an array (`total_count` + `check_runs`), so `gh api --paginate` would not
    auto-merge it even if used; paging by hand and concatenating `check_runs`
    across responses is the simpler route.

    `-X GET` is load-bearing, not decoration: `gh api` defaults to GET but
    switches to POST as soon as any `-f`/`-F` flag is present, and
    `POST /repos/{owner}/{repo}/commits/{sha}/check-runs` does not exist
    (issue #732). The live observation pinning this is cited in
    `release/tests/test_authorize.py::TestFetchCheckRuns`.
    """
    runs: list[dict[str, object]] = []
    total_count: int | None = None
    page = 1
    while total_count is None or len(runs) < total_count:
        result = subprocess.run(
            [
                "gh",
                "api",
                "-X",
                "GET",
                f"repos/{repo}/commits/{sha}/check-runs",
                "-f",
                f"per_page={per_page}",
                "-f",
                f"page={page}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(result.stdout)
        if total_count is None:
            total_count = payload["total_count"]
        page_runs = payload["check_runs"]
        if not page_runs:
            # A page reporting nothing ends the loop even if total_count
            # implied more remained -- a stale/wrong total_count must not
            # spin this forever.
            break
        runs.extend(page_runs)
        page += 1
    return runs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sha", help="the tagged commit to authorize")
    parser.add_argument("--repo", required=True, help="owner/name, e.g. curie-eng/curie")
    parser.add_argument(
        "--reviewed-ref",
        action="append",
        required=True,
        help="a reviewed branch ref; repeat for every allowed branch",
    )
    parser.add_argument(
        "--tag",
        required=True,
        help="the pushed tag name, e.g. v0.7.0 or v0.7.0-rc.1",
    )
    parser.add_argument(
        "--run-id",
        default=os.environ.get("GITHUB_RUN_ID"),
        help="this workflow run's id; its own check-runs are excluded from the gate",
    )
    args = parser.parse_args(argv)

    try:
        check_runs = fetch_check_runs(args.sha, args.repo)
        authorize(
            args.sha,
            check_runs,
            args.reviewed_ref,
            exclude_run_id=args.run_id,
            tag=args.tag,
        )
    except AuthorizationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
        # A lookup that did not complete is not an authorization verdict. Fail
        # closed either way, but say which of the two happened -- an opaque
        # traceback here is what kept issue #732's `gh api` defect unreadable.
        detail = getattr(exc, "stderr", None)
        suffix = f": {str(detail).strip()}" if detail else ""
        print(
            f"ERROR: could not retrieve check-runs for {args.sha} from "
            f"{args.repo} -- the lookup failed with "
            f"{type(exc).__name__}{suffix}. Refusing to authorize this tag "
            "because its check status is unknown.",
            file=sys.stderr,
        )
        return 1
    checked_refs = ", ".join(args.reviewed_ref)
    print(
        f"OK: {args.sha} is reachable from a reviewed ref "
        f"({checked_refs}) and checked; authorized"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

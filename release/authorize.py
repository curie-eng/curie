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

            One exception (ADR-0195, issue #3858): a release-preparation
            commit that changes only version identity may borrow an
            ancestor's green CI. When the tagged commit's own required checks
            are absent or still running, and none of them concluded
            non-passing, the gate walks the commit's first-parent ancestors,
            nearest first and at most VERSION_ONLY_PROOF_DEPTH deep. Each
            candidate is considered only if the whole diff from it to the
            tagged commit stays inside `release/atlas.py`'s
            `version_only_paths` for this tag; the first path outside that set
            ends the walk. The first candidate whose own required checks are
            all green is the proof commit. Any contrary evidence on the tagged
            commit itself, any other path in the delta, or no green candidate
            within the depth refuses exactly as before.

            ADR-0247
            (docs/adr/0247-a-tree-identical-parent-or-a-schema-window-registration-reuses-checks.md,
            issue #4862) amends that exception with two rules under the same
            precondition. First, before the walk, every parent of the tagged
            commit (in parent order, a merge's second parent included) whose
            tree is byte-identical to the tagged commit's tree is a proof
            candidate: its CI ran on these exact bytes, so the first such
            parent with all required checks green is the proof commit. A
            release-PR merge on an unmoved base is proved by its PR head this
            way. A parent with a different tree is never consulted by this
            rule. Second, inside the walk, a delta that also touches
            SCHEMA_WINDOWS_PATH still counts as version-only when the only
            change to that JSON document is registering this tag's schema
            window: a new `windows` key (the tag without its `v`) whose value
            equals the candidate's `candidate` entry, with every other key and
            window unchanged. Any other edit to that file ends the walk.

Both live as separately-testable functions so the negative case -- an
unreviewed or check-less commit is refused -- is an ordinary pytest assertion
against a constructed fixture, not a manual demonstration against the real
repo. Only `main()` needs the network (`gh api` for the live check-run list);
see `release/integrity.py` for the same manifest/verify split rationale.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType
from typing import NamedTuple


def _load_sibling(filename: str, module_name: str) -> ModuleType:
    """Load a module that sits next to this file, by path.

    The release workflows run this script as `python3 release/authorize.py`
    with no package on the import path, so siblings are loaded by file.
    """
    path = Path(__file__).resolve().parent / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_nightly = _load_sibling("nightly.py", "release_nightly")
_atlas = _load_sibling("atlas.py", "release_atlas")

PASSING_CONCLUSIONS = {"success", "neutral"}

# How many first-parent ancestors the version-only proof walk may consult
# (ADR-0195, issue #3858). A release preparation is normally one commit, or a
# merge of one, on top of green CI; ten leaves room for a few stacked bumps
# without letting the walk wander arbitrarily far back.
VERSION_ONLY_PROOF_DEPTH = 10

# The schema-window catalog `bump-version` edits in the same release
# preparation commit (ADR-0247, issue #4862). Registering this tag's window
# there is version identity; any other change to it is not.
SCHEMA_WINDOWS_PATH = "cli/src/application_schema_windows.json"

# The two rules that may stand a proof commit's checks in for the tagged
# commit's own, as `Proof.path` labels (ADR-0195 as amended by ADR-0247).
TREE_IDENTICAL_PARENT = "tree-identical parent"
VERSION_ONLY_ANCESTOR = "version-only ancestor"

# The check-run names that must be present and green before a tag may
# publish (issue #733). These are the job `name:` fields from
# `.github/workflows/ci.yaml`, the workflow that gates every PR and push to
# a reviewed branch, restricted to jobs that speak to release confidence: the
# three language/test jobs, the generated-artifact drift checks, the
# release-compose validation, every first-party image actually building
# (ci-images, including the worker-local overlay, and the example connector
# builds), and the behavioral gates (image-proofs, eval falsifiability, and the
# E2E parity ladder) that ci.yaml's own
# comments describe as catching bug classes no unit test does. Checks
# from other workflows (CodeQL, the
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
#
# EVERY first-party connector image build is required here, not just the
# bundle's read-only `tempo` server (issue #1951). The `examples/` rows of
# ci.yaml's `images` job are release-load-bearing in a way the one-sided
# version of this list could not see. `release.yaml`'s `build` matrix rebuilds
# those same Dockerfiles for real, and its `merge` job -- the job that
# assembles the multi-arch manifest every published tag actually resolves to --
# is gated `if: always() && needs.build.result == 'success'`. So one connector
# image going red on `next` and not named here authorizes the tag, fails a
# single `build` leg, and takes the manifest merge for EVERY image down with
# it, while the CLI binaries and the GitHub Release -- which hang off
# `authorize-release` alone -- publish regardless. The release ships binaries
# whose images have per-arch blobs pushed by digest and no pullable manifest
# tag anywhere. The per-run Docker daemon image (`apps/docker-daemon`, ADR
# 0217) is a `build` matrix row in exactly the same position, so its ci.yaml
# group in "Image proofs (smoke, Docker daemon, toolchain)" is required too.
#
# The subset test below cannot see that direction: it asserts every required
# name is a real job, so ADDING a connector build to ci.yaml and forgetting it
# here leaves the subset perfectly intact. That direction is now pinned by
# `test_every_connector_image_build_is_a_required_check`, which derives the
# connector rows from ci.yaml itself -- a new connector image is guarded by a
# failing test, not by whoever remembers to read this comment.
REQUIRED_CHECK_NAMES = frozenset(
    {
        "Python (ruff + mypy + pytest)",
        "Rust (fmt + clippy + test)",
        "Contracts (generated TypeScript compiles)",
        "UI (lint + vitest + build + Playwright)",
        "Compose (release stack validates)",
        "Build CI images (no push)",
        "Build sre-bot-tempo image (no push)",
        "Build sre-bot-self-upgrade image (no push)",
        "Build sre-bot-scaler image (no push)",
        "Build github-activity-github image (no push)",
        "Image proofs (smoke, Docker daemon, toolchain)",
        "Eval falsifiability gate (fake model, offline)",
        "E2E parity ladder (skill + local, fake model)",
        "E2E parity ladder (local-release, fake model)",
        "Chart (lint + template + kubeconform)",
    }
)


class AuthorizationError(Exception):
    """A tagged commit is not authorized to publish a release."""


class Proof(NamedTuple):
    """A commit whose green required checks stand in for the tagged commit's.

    `path` names the rule that admitted it: TREE_IDENTICAL_PARENT or
    VERSION_ONLY_ANCESTOR.
    """

    commit: str
    path: str


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


def _current_attempts(
    runs: list[dict[str, object]],
) -> list[dict[str, object]] | None:
    """The latest attempts of one name, or None when they cannot be ordered.

    None means at least one payload has no `completed_at` and no
    `started_at`. Callers then fail closed instead of guessing which
    attempt is current.
    """
    recencies = [_check_run_recency(run) for run in runs]
    ordered = [recency for recency in recencies if recency is not None]
    if len(ordered) != len(recencies):
        return None
    latest = max(ordered)
    return [
        run
        for run, recency in zip(runs, recencies, strict=True)
        if recency == latest
    ]


def _check_run_recency(run: dict[str, object]) -> str | None:
    """When this check-run became the current attempt, if the payload says.

    A finished run is ordered by `completed_at`. A run that has not finished
    is ordered by `started_at`, so a later attempt that is still going blocks
    an earlier success, and an earlier unfinished run does not outrank a
    later finished one. Payloads with neither field cannot be ordered.
    """
    completed = run.get("completed_at")
    if isinstance(completed, str) and completed:
        return completed
    started = run.get("started_at")
    if isinstance(started, str) and started:
        return started
    return None


def missing_required_checks(
    check_runs: list[dict[str, object]],
    required_names: frozenset[str] | None = None,
) -> set[str]:
    """Which `required_names` are not satisfied by their current check-run?

    The current check-run for a name is the latest one that payload can
    order, by `completed_at` or else `started_at`. That run must have
    concluded `success` or `neutral`. An earlier failure, cancellation, or
    skip does not block once a later run of the same name passed: GitHub
    keeps every attempt, and a release push re-runs CI on the same commit.
    The latest attempt is what "current and green" means. A later failure
    after an earlier success still blocks (issue #811). `skipped` is not a
    pass when it is the current conclusion (issue #1470): a job-level `if:`
    or a failed `needs:` job records that conclusion, and a chart that was
    never rendered must not authorize a release.

    When any run of a required name has no `completed_at` and no
    `started_at`, that name cannot be ordered. It then fails closed: any
    non-passing run of the name blocks, and so does a name with no passing
    run at all.

    Names outside `required_names` are ignored entirely. An unrelated
    check-run, passing or not, has no bearing on this gate (issue #733).
    A `skipped` non-required check-run is ignored the same way. Only a
    skipped required name blocks, and only when that skip is current or
    the runs cannot be ordered.

    `required_names` defaults to the module-level `REQUIRED_CHECK_NAMES`,
    looked up here rather than bound as the parameter's default value so that
    tests can override the module constant and have every caller (including
    `main()`, which never passes this through explicitly) pick it up.
    """
    if required_names is None:
        required_names = REQUIRED_CHECK_NAMES
    grouped: dict[str, list[dict[str, object]]] = {name: [] for name in required_names}
    for run in check_runs:
        name = run.get("name")
        if isinstance(name, str) and name in grouped:
            grouped[name].append(run)

    missing: set[str] = set()
    for name, runs in grouped.items():
        if not runs:
            missing.add(name)
            continue
        current = _current_attempts(runs)
        considered = runs if current is None else current
        if any(run.get("conclusion") not in PASSING_CONCLUSIONS for run in considered):
            missing.add(name)
    return missing


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


def _git_output(args: Sequence[str], *, cwd: Path | None, purpose: str) -> str:
    """Run a read-only git command for the proof walk; any failure refuses."""
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise AuthorizationError(f"could not {purpose}: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip()
        suffix = f": {detail}" if detail else ""
        raise AuthorizationError(
            f"could not {purpose}; git returned {result.returncode}{suffix}"
        )
    return result.stdout


def _has_contrary_required_evidence(
    check_runs: list[dict[str, object]], required_names: frozenset[str]
) -> bool:
    """Did the current attempt of a required name conclude non-passing?

    Uses the same attempt selection as `missing_required_checks`. An earlier
    failure, cancellation, or skip is not contrary once a later attempt of
    that name passed, so a version-only proof walk can still borrow an
    ancestor when some other required name is absent or still running. A
    later non-passing attempt is contrary and blocks that walk (issue #811).
    A null conclusion (still running) is not evidence either way. `skipped`
    counts as contrary when it is current, or when the attempts cannot be
    ordered (issue #1470).
    """
    grouped: dict[str, list[dict[str, object]]] = {name: [] for name in required_names}
    for run in check_runs:
        name = run.get("name")
        if isinstance(name, str) and name in grouped:
            grouped[name].append(run)
    for runs in grouped.values():
        if not runs:
            continue
        current = _current_attempts(runs)
        considered = runs if current is None else current
        if any(
            run.get("conclusion") is not None
            and run.get("conclusion") not in PASSING_CONCLUSIONS
            for run in considered
        ):
            return True
    return False


def find_tree_identical_parent_proof(
    sha: str,
    fetch_check_runs: Callable[[str], list[dict[str, object]]],
    *,
    cwd: Path | None = None,
    exclude_run_id: str | None = None,
    required_names: frozenset[str] | None = None,
) -> str | None:
    """The first parent of `sha` with the same tree and green required checks.

    Parents are consulted in parent order, a merge's second parent included
    (ADR-0247, issue #4862). Only a parent whose tree object equals `sha`'s is
    fetched; identical trees mean its CI ran on these exact bytes. Returns
    None when no such parent is green. Git failures raise AuthorizationError.
    """
    tree = _git_output(
        ["rev-parse", f"{sha}^{{tree}}"],
        cwd=cwd,
        purpose=f"resolve the tree of {sha}",
    ).strip()
    parents = _git_output(
        ["rev-list", "--parents", "-n", "1", sha],
        cwd=cwd,
        purpose=f"list the parents of {sha}",
    ).split()[1:]
    for parent in parents:
        parent_tree = _git_output(
            ["rev-parse", f"{parent}^{{tree}}"],
            cwd=cwd,
            purpose=f"resolve the tree of {parent}",
        ).strip()
        if parent_tree != tree:
            continue
        runs = exclude_current_workflow_run(fetch_check_runs(parent), exclude_run_id)
        if not missing_required_checks(runs, required_names):
            return parent
    return None


def _schema_windows_at(commit: str, *, cwd: Path | None) -> object:
    """The parsed schema-window catalog at `commit`, or None if unreadable.

    A missing file or malformed JSON is not an error here: it only means the
    delta cannot be a registration, so the walk refuses on its own terms.
    """
    try:
        result = subprocess.run(
            ["git", "show", f"{commit}:{SCHEMA_WINDOWS_PATH}"],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise AuthorizationError(
            f"could not read {SCHEMA_WINDOWS_PATH} at {commit}: {exc}"
        ) from exc
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def is_schema_window_registration(
    base: str, sha: str, version: str, *, cwd: Path | None = None
) -> bool:
    """Is the catalog change from `base` to `sha` only registering `version`?

    True only when both documents are objects with an object `windows`,
    `version` is a new key in `windows` whose value equals `base`'s
    `candidate` (what bump-version promotes), and every other top-level key
    and every other window is unchanged (ADR-0247, issue #4862).
    """
    old = _schema_windows_at(base, cwd=cwd)
    new = _schema_windows_at(sha, cwd=cwd)
    if not isinstance(old, dict) or not isinstance(new, dict):
        return False
    old_windows = old.get("windows")
    new_windows = new.get("windows")
    if not isinstance(old_windows, dict) or not isinstance(new_windows, dict):
        return False
    if version in old_windows or version not in new_windows:
        return False
    if "candidate" not in old or new_windows[version] != old["candidate"]:
        return False
    if {k: v for k, v in new_windows.items() if k != version} != old_windows:
        return False
    return {k: v for k, v in new.items() if k != "windows"} == {
        k: v for k, v in old.items() if k != "windows"
    }


def find_version_only_proof(
    sha: str,
    tag: str,
    fetch_check_runs: Callable[[str], list[dict[str, object]]],
    *,
    cwd: Path | None = None,
    exclude_run_id: str | None = None,
    required_names: frozenset[str] | None = None,
) -> str | None:
    """The nearest first-parent ancestor whose green CI also proves `sha`.

    Walks at most VERSION_ONLY_PROOF_DEPTH first-parent ancestors, nearest
    first. The diff from each candidate to `sha` is checked against
    `version_only_paths` for `tag` before that candidate's check-runs are
    fetched; the first candidate outside the set ends the walk. A delta that
    also touches SCHEMA_WINDOWS_PATH stays inside the set only when that
    change registers this tag's schema window (ADR-0247). Returns None when
    no candidate qualifies. Git failures raise AuthorizationError.
    """
    version = tag.strip().removeprefix("refs/tags/")
    allowed = _atlas.version_only_paths(version)
    window_key = version.removeprefix("v")
    ancestors = _git_output(
        [
            "rev-list",
            "--first-parent",
            f"--max-count={VERSION_ONLY_PROOF_DEPTH + 1}",
            sha,
        ],
        cwd=cwd,
        purpose=f"list the first-parent ancestors of {sha}",
    ).split()
    for candidate in ancestors[1:]:
        changed = _git_output(
            ["diff", "--no-renames", "--name-only", candidate, sha],
            cwd=cwd,
            purpose=f"diff {candidate} against {sha}",
        ).splitlines()
        extra = {path for path in changed if path} - allowed
        if extra and not (
            extra == {SCHEMA_WINDOWS_PATH}
            and is_schema_window_registration(candidate, sha, window_key, cwd=cwd)
        ):
            return None
        runs = exclude_current_workflow_run(fetch_check_runs(candidate), exclude_run_id)
        if not missing_required_checks(runs, required_names):
            return candidate
    return None


def authorize(
    sha: str,
    check_runs: list[dict[str, object]],
    reviewed_refs: Sequence[str],
    *,
    cwd: Path | None = None,
    exclude_run_id: str | None = None,
    required_names: frozenset[str] | None = None,
    nightly_conclusion: str | None = None,
    allow_red_nightly: bool = False,
    require_nightly: bool = False,
    tag: str | None = None,
    fetch_proof_check_runs: Callable[[str], list[dict[str, object]]] | None = None,
) -> Proof | None:
    """Raise AuthorizationError unless `sha` may publish a release.

    Returns the Proof whose commit's checks stood in for `sha`'s own, or None
    when `sha`'s own required checks were green. A tree-identical parent is
    tried first, then the version-only walk (see the module docstring,
    ADR-0195 as amended by ADR-0247). Either needs both `tag` and
    `fetch_proof_check_runs`; without either the gate is unchanged.

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
    if required_names is None:
        required_names = REQUIRED_CHECK_NAMES
    other_runs = exclude_current_workflow_run(check_runs, exclude_run_id)
    missing = missing_required_checks(other_runs, required_names)
    proof: Proof | None = None
    if (
        missing
        and tag is not None
        and fetch_proof_check_runs is not None
        and not _has_contrary_required_evidence(other_runs, required_names)
    ):
        # Identical bytes are the stronger evidence, so they are tried first.
        parent = find_tree_identical_parent_proof(
            sha,
            fetch_proof_check_runs,
            cwd=cwd,
            exclude_run_id=exclude_run_id,
            required_names=required_names,
        )
        if parent is not None:
            proof = Proof(parent, TREE_IDENTICAL_PARENT)
        else:
            ancestor = find_version_only_proof(
                sha,
                tag,
                fetch_proof_check_runs,
                cwd=cwd,
                exclude_run_id=exclude_run_id,
                required_names=required_names,
            )
            if ancestor is not None:
                proof = Proof(ancestor, VERSION_ONLY_ANCESTOR)
    if missing and proof is None:
        raise AuthorizationError(
            f"commit {sha} is missing {len(missing)} required check-run(s) "
            f"({len(other_runs)} check-runs found for the commit, excluding "
            "this workflow run's own): "
            f"{', '.join(sorted(missing))}. Refusing to authorize this tag "
            "until its required checks are current and green."
        )
    if require_nightly or nightly_conclusion is not None or allow_red_nightly:
        reason = _nightly.nightly_refusal_reason(
            nightly_conclusion, allow_red=allow_red_nightly
        )
        if reason:
            raise AuthorizationError(reason)
    return proof


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
        default=None,
        help="the pushed tag name, e.g. v0.7.0 or v0.7.0-rc.1",
    )
    parser.add_argument(
        "--run-id",
        default=os.environ.get("GITHUB_RUN_ID"),
        help="this workflow run's id; its own check-runs are excluded from the gate",
    )
    args = parser.parse_args(argv)
    branch = "main"
    nightly_conclusion: str | None = None
    proof: Proof | None = None
    proof_runs: dict[str, list[dict[str, object]]] = {}

    def fetch_proof_check_runs(candidate: str) -> list[dict[str, object]]:
        # Both authorize() calls below consult the same commits; fetch each once.
        if candidate not in proof_runs:
            proof_runs[candidate] = fetch_check_runs(candidate, args.repo)
        return proof_runs[candidate]

    try:
        check_runs = fetch_check_runs(args.sha, args.repo)
        # Establish reachability, tag class, and required CI before the nightly
        # network lookup. This keeps a deterministic authorization refusal from
        # being masked by an unrelated lookup failure.
        authorize(
            args.sha,
            check_runs,
            args.reviewed_ref,
            exclude_run_id=args.run_id,
            tag=args.tag,
            fetch_proof_check_runs=fetch_proof_check_runs,
        )
        matching = [
            ref
            for ref in args.reviewed_ref
            if commit_is_on_reviewed_branch(args.sha, ref)
        ]
        branch = _nightly.nightly_branch_from_refs(matching)
        try:
            nightly_conclusion = _nightly.fetch_latest_nightly_conclusion(
                args.repo, branch
            )
            bodies = _nightly.fetch_associated_pr_bodies(args.sha, args.repo)
        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
            detail = getattr(exc, "stderr", None)
            suffix = f": {str(detail).strip()}" if detail else ""
            print(
                f"ERROR: could not retrieve the nightly conclusion or associated "
                f"pull request bodies for {args.sha} on {branch} from {args.repo} "
                f"-- the lookup failed with {type(exc).__name__}{suffix}. "
                "Refusing to authorize this tag because its nightly status is "
                "unknown.",
                file=sys.stderr,
            )
            return 1
        proof = authorize(
            args.sha,
            check_runs,
            args.reviewed_ref,
            exclude_run_id=args.run_id,
            tag=args.tag,
            fetch_proof_check_runs=fetch_proof_check_runs,
            nightly_conclusion=nightly_conclusion,
            allow_red_nightly=_nightly.allow_red_nightly_from_bodies(bodies),
            require_nightly=True,
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
    if nightly_conclusion == "success":
        nightly_note = f"the latest nightly on {branch} concluded success"
    else:
        nightly_note = (
            f"the latest nightly on {branch} concluded {nightly_conclusion!r} "
            "with --allow-red-nightly recorded in an associated pull request body"
        )
    if proof is None:
        checked_note = "checked"
    else:
        checked_note = (
            f"checked via {proof.path} {proof.commit} "
            "(its required checks stand in for this commit's, ADR-0195 as "
            "amended by ADR-0247)"
        )
    print(
        f"OK: {args.sha} is reachable from a reviewed ref "
        f"({checked_refs}), {checked_note}, and {nightly_note}; authorized"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

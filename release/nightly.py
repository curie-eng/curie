#!/usr/bin/env python3
"""Nightly ladder conclusion gate and per-signature issue filing (#2245).

The release authorizer consults `nightly_refusal_reason` so a tag cannot
publish while the latest completed nightly on its branch is not `success`,
unless a merged PR body for the tagged commit records `--allow-red-nightly`.

A failed nightly run files (or comments on) one issue per extracted failure
signature, labelled `nightly-ladder`, deduplicated by a stable hash marker
in the issue body.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

NIGHTLY_WORKFLOW = "nightly-graded-ladder.yaml"
NIGHTLY_LABEL = "nightly-ladder"
ALLOW_RED_TOKEN = "--allow-red-nightly"
SIGNATURE_MARKER_PREFIX = "nightly-ladder-signature:"

# CSI/OSC and single-character escapes GitHub runners emit into job logs.
_ANSI_ESCAPE = re.compile(
    r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-?]*[ -/]*[@-~]|\x1b[@-Z\\-_]"
)

_ERROR_LINE = re.compile(
    r"(?:^|\n)(?:error: |AssertionError: |cluster: |local: |skill: ).+",
    re.IGNORECASE,
)
_KNOWN = (
    re.compile(
        r"image '[^']+' is required by compose\.release\.yaml[^\n]*"
    ),
    re.compile(
        r"message after repeated eval timed out at 45s without a finalized reply"
    ),
    re.compile(
        r"no new worker-to-runner trace carried ERROR on "
        r"turn\.process \+ agent\.run and classified_failure"
    ),
    re.compile(r"=== curie skill up \(fake model, offline\) ==="),
)


def nightly_refusal_reason(
    conclusion: str | None, *, allow_red: bool
) -> str | None:
    """Why a tag must not publish, or None if the nightly does not block."""
    if allow_red:
        return None
    if conclusion == "success":
        return None
    if conclusion is None:
        return (
            "no completed nightly graded ladder run was found on the base "
            "branch; refusing to authorize this tag until one concludes "
            f"success (or a merged PR body records {ALLOW_RED_TOKEN})"
        )
    return (
        f"the latest completed nightly graded ladder concluded {conclusion!r}, "
        "not success; refusing to authorize this tag until the nightly is "
        f"green (or a merged PR body records {ALLOW_RED_TOKEN})"
    )


def allow_red_nightly_from_bodies(bodies: Sequence[str]) -> bool:
    return any(ALLOW_RED_TOKEN in (body or "") for body in bodies)


def nightly_branch_from_refs(
    matching_refs: Sequence[str], *, default: str = "main"
) -> str:
    """Prefer `main` when the commit is on more than one reviewed branch."""
    shorts = [ref.rsplit("/", 1)[-1] for ref in matching_refs]
    if "main" in shorts:
        return "main"
    if shorts:
        return shorts[0]
    return default


def fetch_latest_nightly_conclusion(repo: str, branch: str) -> str | None:
    """The latest completed nightly workflow run's conclusion, or None."""
    result = subprocess.run(
        [
            "gh",
            "api",
            "-X",
            "GET",
            f"repos/{repo}/actions/workflows/{NIGHTLY_WORKFLOW}/runs",
            "-f",
            f"branch={branch}",
            "-f",
            "status=completed",
            "-f",
            "per_page=1",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    runs = payload.get("workflow_runs") or []
    if not runs:
        return None
    conclusion = runs[0].get("conclusion")
    return str(conclusion) if conclusion is not None else None


def fetch_associated_pr_bodies(sha: str, repo: str) -> list[str]:
    """Bodies of PRs associated with `sha` (merged or open)."""
    result = subprocess.run(
        [
            "gh",
            "api",
            "-X",
            "GET",
            f"repos/{repo}/commits/{sha}/pulls",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    return merged_pr_bodies(payload)


def merged_pr_bodies(payload: object) -> list[str]:
    """Bodies of merged PRs only; an open PR cannot authorize a tag."""
    if not isinstance(payload, list):
        return []
    return [
        str(pr.get("body") or "")
        for pr in payload
        if pr.get("merged_at")
    ]


def signature_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def signature_marker(sig_id: str) -> str:
    return f"<!-- {SIGNATURE_MARKER_PREFIX} {sig_id} -->"


@dataclass(frozen=True)
class Signature:
    job: str
    text: str
    note: str = ""

    @property
    def signature_id(self) -> str:
        return signature_id(self.text)


@dataclass(frozen=True)
class IssueAction:
    kind: Literal["create", "comment"]
    body: str
    title: str = ""
    labels: tuple[str, ...] = ()
    number: int | None = None


def extract_signatures(jobs: Sequence[dict[str, object]]) -> list[Signature]:
    """One signature per failed job, preferring known stable phrases."""
    found: list[Signature] = []
    seen: set[str] = set()
    for job in jobs:
        if job.get("conclusion") != "failure":
            continue
        name = str(job.get("name") or "job")
        text = job_signature_text(job)
        if text in seen:
            continue
        seen.add(text)
        found.append(Signature(job=name, text=text, note=_log_note(job)))
    return found


def job_signature_text(job: dict[str, object]) -> str:
    """The signature a failed job files under.

    A job whose log could not be fetched still gets a stable, per-rung
    signature so the red rung is filed instead of crashing the filer (#2868).
    """
    if job.get("log_error"):
        return f"{job.get('name') or 'job'}: failed, job log unavailable"
    return _signature_text(str(job.get("log") or ""))


def _log_note(job: dict[str, object]) -> str:
    error = str(job.get("log_error") or "")
    if not error:
        return ""
    return f"Log omitted: the job log could not be fetched ({error})."


def _signature_text(log: str) -> str:
    for pattern in _KNOWN:
        match = pattern.search(log)
        if not match:
            continue
        text = " ".join(match.group(0).split())
        if "skill up" in text.lower():
            later = list(_ERROR_LINE.finditer(log[match.end() :]))
            if later:
                return " ".join(later[-1].group(0).split())
        return text
    matches = list(_ERROR_LINE.finditer(log))
    if matches:
        return " ".join(matches[-1].group(0).split())
    return "ladder job failed with no recognized error line"


def plan_issue_actions(
    signatures: Sequence[Signature],
    existing_issues: Sequence[dict[str, object]],
    *,
    run_url: str = "",
) -> list[IssueAction]:
    by_marker: dict[str, dict[str, object]] = {}
    for issue in existing_issues:
        body = str(issue.get("body") or "")
        for sig in signatures:
            marker = signature_marker(sig.signature_id)
            if marker in body:
                by_marker[sig.signature_id] = issue
    actions: list[IssueAction] = []
    for sig in signatures:
        existing = by_marker.get(sig.signature_id)
        snippet = (
            f"Run: {run_url}\nJob: {sig.job}\nSignature: `{sig.text}`\n"
            if run_url
            else f"Job: {sig.job}\nSignature: `{sig.text}`\n"
        )
        if sig.note:
            snippet += f"\n{sig.note}\n"
        if existing is not None:
            actions.append(
                IssueAction(
                    kind="comment",
                    number=int(existing["number"]),
                    body=f"Recurrence on the nightly graded ladder.\n\n{snippet}",
                )
            )
            continue
        marker = signature_marker(sig.signature_id)
        title = f"nightly-ladder: {sig.text[:80]}"
        body = (
            f"{marker}\n\n"
            "Auto-filed from a failed nightly graded parity ladder run. "
            "Deduplicated by the signature marker above.\n\n"
            f"{snippet}"
        )
        actions.append(
            IssueAction(
                kind="create",
                title=title,
                body=body,
                labels=(NIGHTLY_LABEL,),
            )
        )
    return actions


def _gh(args: list[str]) -> str:
    result = subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _ensure_label(repo: str) -> None:
    listing = _gh(
        [
            "label",
            "list",
            "--repo",
            repo,
            "--search",
            NIGHTLY_LABEL,
            "--json",
            "name",
        ]
    )
    names = {item.get("name") for item in json.loads(listing)}
    if NIGHTLY_LABEL in names:
        return
    subprocess.run(
        [
            "gh",
            "label",
            "create",
            NIGHTLY_LABEL,
            "--repo",
            repo,
            "--description",
            "Deduplicated nightly graded ladder failure",
            "--color",
            "0E8A16",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def strip_ansi(text: str) -> str:
    """Drop terminal escape sequences so signatures stay stable (#2819)."""
    return _ANSI_ESCAPE.sub("", text)


def job_log(repo: str, job_id: object) -> str:
    """One failed job's log, with terminal escape sequences removed.

    Since gh 2.76 the CLI refuses to print a response carrying terminal
    escape sequences unless `--allow-escape-sequences` is passed, which
    killed nightly issue filing from 2026-09-03 (#2819). Ask for the raw
    bytes, fall back when an older gh does not know the flag, and strip
    the escapes ourselves either way.
    """
    endpoint = ["api", "-X", "GET", f"repos/{repo}/actions/jobs/{job_id}/logs"]
    try:
        raw = _gh([*endpoint, "--allow-escape-sequences"])
    except subprocess.CalledProcessError as exc:
        if "unknown flag" not in (exc.stderr or ""):
            raise
        raw = _gh(endpoint)
    return strip_ansi(raw)


def _failed_job_logs(repo: str, run_id: str) -> list[dict[str, object]]:
    payload = json.loads(
        _gh(["api", "-X", "GET", f"repos/{repo}/actions/runs/{run_id}/jobs"])
    )
    jobs: list[dict[str, object]] = []
    for job in payload.get("jobs") or []:
        if job.get("conclusion") != "failure":
            continue
        entry: dict[str, object] = {
            "name": job.get("name") or "job",
            "conclusion": "failure",
            "log": "",
        }
        # One unreadable log must not cost every rung its issue (#2868).
        try:
            entry["log"] = job_log(repo, job.get("id"))
        except subprocess.CalledProcessError as exc:
            entry["log_error"] = _gh_detail(exc)
        jobs.append(entry)
    return jobs


def _open_nightly_issues(repo: str) -> list[dict[str, object]]:
    listing = _gh(
        [
            "issue",
            "list",
            "--repo",
            repo,
            "--label",
            NIGHTLY_LABEL,
            "--state",
            "open",
            "--json",
            "number,title,body",
            "--limit",
            "100",
        ]
    )
    payload = json.loads(listing)
    return payload if isinstance(payload, list) else []


def _filing_error(message: str) -> int:
    """Annotate and fail so a run nobody watches still shows red (#2819)."""
    print(f"::error title=nightly-ladder issue filing failed::{message}")
    return 1


def _gh_detail(exc: subprocess.CalledProcessError) -> str:
    return " ".join(((exc.stderr or exc.stdout or "").strip() or str(exc)).split())


def file_issues(repo: str, run_id: str, run_url: str) -> int:
    try:
        return _file_issues(repo, run_id, run_url)
    except subprocess.CalledProcessError as exc:
        detail = _gh_detail(exc)
        return _filing_error(
            f"gh failed while filing nightly-ladder issues for run {run_id}: "
            f"{detail}"
        )


def _file_issues(repo: str, run_id: str, run_url: str) -> int:
    _ensure_label(repo)
    jobs = _failed_job_logs(repo, run_id)
    signatures = extract_signatures(jobs)
    if not signatures:
        return _filing_error(
            f"run {run_id} failed but no failed job produced a signature; "
            "no nightly-ladder issue was filed"
        )
    actions = plan_issue_actions(
        signatures, _open_nightly_issues(repo), run_url=run_url
    )
    filed: set[str] = set()
    failures: list[str] = []
    for sig, action in zip(signatures, actions, strict=True):
        try:
            _apply_action(repo, action)
        except subprocess.CalledProcessError as exc:
            failures.append(f"{sig.job}: {_gh_detail(exc)}")
            continue
        filed.add(sig.text)
    # Every red rung must end up with an issue created or updated (#2868).
    unfiled = [
        str(job.get("name") or "job")
        for job in jobs
        if job.get("conclusion") == "failure"
        and job_signature_text(job) not in filed
    ]
    if unfiled:
        detail = f"; gh errors: {'; '.join(failures)}" if failures else ""
        return _filing_error(
            f"red rung(s) with no nightly-ladder issue created or updated "
            f"for run {run_id}: {', '.join(unfiled)}{detail}"
        )
    return 0


def _apply_action(repo: str, action: IssueAction) -> None:
    if action.kind == "create":
        label_args: list[str] = []
        for label in action.labels:
            label_args.extend(["--label", label])
        _gh(
            [
                "issue",
                "create",
                "--repo",
                repo,
                "--title",
                action.title,
                "--body",
                action.body,
                *label_args,
            ]
        )
        print(f"created issue for {action.title!r}")
        return
    _gh(
        [
            "issue",
            "comment",
            str(action.number),
            "--repo",
            repo,
            "--body",
            action.body,
        ]
    )
    print(f"commented on issue #{action.number}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    file_cmd = sub.add_parser("file-issues", help="file one issue per failure signature")
    file_cmd.add_argument("--repo", required=True)
    file_cmd.add_argument("--run-id", required=True)
    file_cmd.add_argument(
        "--run-url",
        default="",
        help="workflow run URL to cite in the issue body",
    )
    args = parser.parse_args(argv)
    if args.cmd == "file-issues":
        run_url = args.run_url or (
            f"https://github.com/{args.repo}/actions/runs/{args.run_id}"
        )
        try:
            return file_issues(args.repo, args.run_id, run_url)
        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
            detail = getattr(exc, "stderr", None)
            suffix = f": {str(detail).strip()}" if detail else ""
            print(
                f"ERROR: could not file nightly-ladder issues "
                f"({type(exc).__name__}{suffix})",
                file=sys.stderr,
            )
            return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

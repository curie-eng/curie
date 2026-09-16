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
        log = str(job.get("log") or "")
        name = str(job.get("name") or "job")
        text = _signature_text(log)
        if text in seen:
            continue
        seen.add(text)
        found.append(Signature(job=name, text=text))
    return found


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


def _failed_job_logs(repo: str, run_id: str) -> list[dict[str, object]]:
    payload = json.loads(
        _gh(["api", "-X", "GET", f"repos/{repo}/actions/runs/{run_id}/jobs"])
    )
    jobs: list[dict[str, object]] = []
    for job in payload.get("jobs") or []:
        if job.get("conclusion") != "failure":
            continue
        job_id = job.get("id")
        log = _gh(
            [
                "api",
                "-X",
                "GET",
                f"repos/{repo}/actions/jobs/{job_id}/logs",
            ]
        )
        jobs.append(
            {
                "name": job.get("name") or "job",
                "conclusion": "failure",
                "log": log,
            }
        )
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


def file_issues(repo: str, run_id: str, run_url: str) -> int:
    _ensure_label(repo)
    signatures = extract_signatures(_failed_job_logs(repo, run_id))
    if not signatures:
        print("no failure signatures extracted; nothing to file")
        return 0
    actions = plan_issue_actions(
        signatures, _open_nightly_issues(repo), run_url=run_url
    )
    for action in actions:
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
        else:
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
    return 0


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

"""ADR number uniqueness across the working tree, release trains, and open PRs.

Local uniqueness (#541) catches two files in the same tree claiming one
``NNNN`` prefix. That is not enough: ``main`` and ``next`` (or an open PR
against either) can each merge a different ``docs/adr/NNNN-*.md`` with the
same number, and git will not conflict. Issue #1957 extends the gate to the
union of:

- the working tree,
- ``origin/main`` after an explicit fetch,
- ``origin/next`` after an explicit fetch when that train exists,
- open pull requests that **add** a numbered ADR file.

A scan that cannot fetch the trains or list open PR files fails closed when
this candidate adds or renames an ADR (authors must not allocate blindly).
With no candidate ADR claim, scan failures and remote-only collisions are
warnings so unrelated PRs are not blocked by strangers' collisions or by
network gaps. Required failures are limited to collisions the candidate's
added or renamed ADR filenames participate in.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from .finding import Finding, Severity
from .generate import adr_number, iter_adr_docs

_ADR_REL = "docs/adr"
_ADR_PATH_RE = re.compile(r"^docs/adr/(\d+)-[^/]+\.md$")
_TRAIN_REFS = ("main", "next")
_SCAN_FAIL_PREFIX = (
    "cannot verify open PRs to determine a new unique ADR number"
)
# Environment GitHub Actions sets on pull_request jobs; used to pick the PR
# base when discovering which ADR files this candidate added or renamed.
_GITHUB_BASE_REF_ENV = "GITHUB_BASE_REF"


@dataclass(frozen=True)
class AdrClaim:
    """One allocation of an ADR number by a concrete filename at a source."""

    number: str
    filename: str
    source: str


class AdrScanError(Exception):
    """The remote allocation scan could not run to completion."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)

    def as_finding(self) -> Finding:
        return Finding(_ADR_REL, "remote-scan", f"{_SCAN_FAIL_PREFIX}: {self.detail}")


Runner = Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]


def default_runner(cmd: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run ``cmd`` in ``cwd``, capturing text stdout/stderr."""
    return subprocess.run(
        list(cmd),
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def claims_from_filenames(
    filenames: Sequence[str], source: str
) -> list[AdrClaim]:
    """Build claims from basename or repo-relative ADR paths."""
    claims: list[AdrClaim] = []
    for raw in filenames:
        name = Path(raw).name
        number = adr_number(Path(name))
        if number is None:
            continue
        claims.append(AdrClaim(number=number, filename=name, source=source))
    return claims


def working_tree_claims(repo_root: Path) -> list[AdrClaim]:
    """Every numbered ADR file in the working tree."""
    return [
        AdrClaim(number=number, filename=doc.name, source="working-tree")
        for doc in iter_adr_docs(repo_root)
        if (number := adr_number(doc)) is not None
    ]


def next_free_number(numbers: set[str]) -> str:
    """One past the highest allocated number, zero-padded to at least four digits.

    Matches the claim rule in ``docs/adr/README.md``: the next free number is
    one past the last claimed value in the combined allocation set, not the
    first gap.
    """
    if not numbers:
        return "0001"
    width = max(4, max(len(n) for n in numbers))
    highest = max(int(n) for n in numbers)
    return f"{highest + 1:0{width}d}"


def collision_groups(claims: Sequence[AdrClaim]) -> dict[str, list[AdrClaim]]:
    """Numbers claimed by more than one distinct filename, with every holder."""
    by_number: dict[str, list[AdrClaim]] = {}
    for claim in claims:
        by_number.setdefault(claim.number, []).append(claim)

    collisions: dict[str, list[AdrClaim]] = {}
    for number, holders in by_number.items():
        filenames = {claim.filename for claim in holders}
        if len(filenames) > 1:
            collisions[number] = holders
    return collisions


def format_collision_reason(number: str, holders: Sequence[AdrClaim], free: str) -> str:
    """Human reason naming every holder and the next free number (#1957)."""
    # Stable order: filename then source, so the message is byte-stable.
    ordered = sorted(holders, key=lambda c: (c.filename, c.source))
    holder_text = ", ".join(f"{c.filename} ({c.source})" for c in ordered)
    return (
        f"ADR number {number} is claimed by {len({c.filename for c in holders})} "
        f"distinct files: {holder_text}; next free number is {free}"
    )


def local_collision_findings(repo_root: Path) -> list[Finding]:
    """In-tree duplicate prefixes only (#541). Same filename twice cannot happen."""
    findings: list[Finding] = []
    collisions = collision_groups(working_tree_claims(repo_root))
    for number, holders in sorted(collisions.items()):
        names = sorted({claim.filename for claim in holders})
        findings.append(
            Finding(
                _ADR_REL,
                number,
                f"ADR number {number} is claimed by {len(names)} files: "
                f"{', '.join(names)}; renumber all but one to the next "
                "free number and update every inbound reference",
            )
        )
    return findings


def _run_checked(
    runner: Runner, cwd: Path, cmd: Sequence[str], what: str
) -> str:
    result = runner(cmd, cwd)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip() or f"exit {result.returncode}"
        raise AdrScanError(f"{what} failed: {detail}")
    return result.stdout


def fetch_release_trains(
    repo_root: Path, *, runner: Runner = default_runner
) -> tuple[str, ...]:
    """Fetch ``origin/main`` and ``origin/next`` (when present); return fetched refs.

    ``main`` is required. ``next`` is included when the remote advertises it so
    a temporarily retired integration branch does not brick the docs gate.
    """
    remote_heads = _run_checked(
        runner,
        repo_root,
        ["git", "ls-remote", "--heads", "origin", "main", "next"],
        "listing origin release-train heads",
    )
    available = set()
    for line in remote_heads.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        ref = parts[1]
        if ref.endswith("/main"):
            available.add("main")
        elif ref.endswith("/next"):
            available.add("next")

    if "main" not in available:
        raise AdrScanError("origin/main is not advertised by the remote")

    fetched: list[str] = []
    for name in _TRAIN_REFS:
        if name not in available:
            continue
        _run_checked(
            runner,
            repo_root,
            ["git", "fetch", "--no-tags", "origin", f"{name}:refs/remotes/origin/{name}"],
            f"fetching origin/{name}",
        )
        fetched.append(name)
    return tuple(fetched)


def claims_at_ref(
    repo_root: Path, ref: str, *, runner: Runner = default_runner
) -> list[AdrClaim]:
    """Numbered ADR basenames at ``ref`` (typically ``origin/main``)."""
    stdout = _run_checked(
        runner,
        repo_root,
        ["git", "ls-tree", "-r", "--name-only", ref, "--", _ADR_REL],
        f"listing ADR files at {ref}",
    )
    paths = [line.strip() for line in stdout.splitlines() if line.strip()]
    return claims_from_filenames(paths, source=ref)


_GITHUB_REMOTE_RE = re.compile(
    r"(?:github\.com[:/]|github\.com/)(?P<owner>[^/]+)/(?P<repo>[^/.]+?)(?:\.git)?$"
)


def _repo_slug(repo_root: Path, runner: Runner) -> str:
    """Owner/name from ``origin``, so slug resolution does not need the GitHub API."""
    stdout = _run_checked(
        runner,
        repo_root,
        ["git", "remote", "get-url", "origin"],
        "reading the origin remote URL",
    ).strip()
    match = _GITHUB_REMOTE_RE.search(stdout)
    if match is None:
        raise AdrScanError(
            f"origin remote URL is not a github.com remote (got {stdout!r})"
        )
    return f"{match.group('owner')}/{match.group('repo')}"


def open_pr_adr_claims(
    repo_root: Path, *, runner: Runner = default_runner
) -> list[AdrClaim]:
    """ADR files **added** by open pull requests (GitHub via ``gh``)."""
    slug = _repo_slug(repo_root, runner)
    # Paginate open PRs, then each PR's files. Fail closed on any API error so
    # a partial list cannot bless a colliding number as free.
    pr_json = _run_checked(
        runner,
        repo_root,
        [
            "gh",
            "api",
            "--paginate",
            f"repos/{slug}/pulls?state=open&per_page=100",
        ],
        "listing open pull requests",
    )
    try:
        # --paginate concatenates JSON arrays into a stream gh joins; gh api
        # --paginate returns a single JSON array when the client supports it,
        # otherwise concatenated arrays. Accept both shapes.
        pulls = _parse_json_array_stream(pr_json)
    except json.JSONDecodeError as exc:
        raise AdrScanError(f"open pull request list was not JSON: {exc}") from exc

    claims: list[AdrClaim] = []
    for pull in pulls:
        if not isinstance(pull, dict):
            continue
        number = pull.get("number")
        if not isinstance(number, int):
            continue
        source = f"PR #{number}"
        files_json = _run_checked(
            runner,
            repo_root,
            [
                "gh",
                "api",
                "--paginate",
                f"repos/{slug}/pulls/{number}/files?per_page=100",
            ],
            f"listing files for PR #{number}",
        )
        try:
            files = _parse_json_array_stream(files_json)
        except json.JSONDecodeError as exc:
            raise AdrScanError(f"PR #{number} file list was not JSON: {exc}") from exc
        for entry in files:
            if not isinstance(entry, dict):
                continue
            status = entry.get("status")
            filename = entry.get("filename")
            if not isinstance(filename, str):
                continue
            # Added and renamed-into paths claim a number; modified/removed do
            # not allocate a new identity.
            if status not in ("added", "renamed"):
                continue
            if _ADR_PATH_RE.match(filename) is None:
                continue
            claims.extend(claims_from_filenames([filename], source=source))
    return claims


def _parse_json_array_stream(raw: str) -> list[object]:
    """Parse ``gh api --paginate`` output that may be one array or many abutting."""
    text = raw.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Concatenated arrays: `][`
        combined: list[object] = []
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(text):
            while idx < len(text) and text[idx].isspace():
                idx += 1
            if idx >= len(text):
                break
            chunk, offset = decoder.raw_decode(text, idx)
            if not isinstance(chunk, list):
                raise
            combined.extend(chunk)
            idx = offset
        return combined
    if isinstance(parsed, list):
        return parsed
    raise json.JSONDecodeError("expected a JSON array", text, 0)


def collect_remote_claims(
    repo_root: Path, *, runner: Runner = default_runner
) -> list[AdrClaim]:
    """Fetch trains and gather claims from refs plus open PR additions."""
    fetched = fetch_release_trains(repo_root, runner=runner)
    claims: list[AdrClaim] = []
    for name in fetched:
        claims.extend(claims_at_ref(repo_root, f"origin/{name}", runner=runner))
    claims.extend(open_pr_adr_claims(repo_root, runner=runner))
    return claims


def allocation_findings(
    claims: Sequence[AdrClaim],
    *,
    candidate_adr_filenames: set[str] | None = None,
) -> list[Finding]:
    """Findings for cross-source number collisions, naming holders and next free.

    The full union is always reported. A collision is a hard error only when one
    of its distinct filenames is an ADR this candidate **added or renamed into
    place**; every other collision is a warning so a PR with no ADR claim, or a
    repair that renumbers away from a train collision, is not blocked by
    strangers' open PRs or by the pre-repair train state (#1957 review).
    """
    collisions = collision_groups(claims)
    if not collisions:
        return []
    free = next_free_number({claim.number for claim in claims})
    participating = candidate_adr_filenames or set()
    findings: list[Finding] = []
    for number, holders in sorted(collisions.items()):
        holder_names = {claim.filename for claim in holders}
        severity: Severity = (
            "error" if holder_names & participating else "warning"
        )
        findings.append(
            Finding(
                _ADR_REL,
                number,
                format_collision_reason(number, holders, free),
                severity=severity,
            )
        )
    return findings


def adr_diff_base_ref() -> str:
    """Remote ref to diff ADR adds/renames against (PR base, else ``origin/main``)."""
    base = (os.environ.get(_GITHUB_BASE_REF_ENV) or "").strip()
    if base:
        return f"origin/{base.removeprefix('refs/heads/')}"
    return "origin/main"


def discover_candidate_adr_filenames(
    repo_root: Path, *, runner: Runner = default_runner, base_ref: str | None = None
) -> set[str]:
    """Basenames of numbered ADRs this candidate added or renamed into place.

    Uses ``git diff --name-status`` against the PR base (or ``origin/main``).
    Only ``A`` and ``R`` destinations count: an in-place edit of an existing
    ADR does not make inherited train collisions the candidate's fault.
    """
    ref = base_ref if base_ref is not None else adr_diff_base_ref()
    result = runner(
        ["git", "diff", "--name-status", f"{ref}...HEAD", "--", _ADR_REL],
        repo_root,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip() or (
            f"exit {result.returncode}"
        )
        raise AdrScanError(f"diffing ADR files against {ref} failed: {detail}")

    names: set[str] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        status = parts[0]
        if status.startswith("A") and len(parts) >= 2:
            path = parts[1]
        elif status.startswith("R") and len(parts) >= 3:
            path = parts[2]
        else:
            continue
        if _ADR_PATH_RE.match(path) is None:
            continue
        names.add(Path(path).name)
    return names


def check_adr_uniqueness(
    repo_root: Path,
    *,
    require_remote: bool,
    runner: Runner = default_runner,
    candidate_adr_filenames: set[str] | None = None,
) -> list[Finding]:
    """Local uniqueness always; remote union when ``require_remote`` is true.

    Remote collisions that this candidate's added/renamed ADRs participate in
    fail the gate; other remote collisions are warnings. A failed candidate
    ADR ``git diff`` always fails closed. When the candidate adds or renames
    an ADR, a failed remote scan also fails closed so authors cannot allocate
    blindly. With a successful empty candidate claim set, a later scan failure
    is a warning so unrelated PRs are not blocked by network or auth gaps.
    """
    local = local_collision_findings(repo_root)
    if not require_remote:
        return local

    # claimed stays None until discover succeeds so a failed git diff cannot be
    # mistaken for "no candidate ADR claim" and soft-warned.
    claimed: set[str] | None = candidate_adr_filenames

    # Local collisions already make the number ambiguous; still attempt the
    # remote scan so authors see the next free number over the full set when
    # the network path works. A scan failure is reported alongside local
    # findings only when there is no local collision yet -- otherwise the
    # local remedy is the one to take first.
    try:
        if claimed is None:
            claimed = discover_candidate_adr_filenames(repo_root, runner=runner)
        remote_claims = collect_remote_claims(repo_root, runner=runner)
    except AdrScanError as exc:
        if local:
            return local
        finding = exc.as_finding()
        if claimed is not None and not claimed:
            return [
                Finding(
                    finding.doc,
                    finding.citation,
                    finding.reason,
                    severity="warning",
                )
            ]
        return [finding]

    all_claims = working_tree_claims(repo_root) + remote_claims
    remote_findings = allocation_findings(
        all_claims, candidate_adr_filenames=claimed
    )
    # Prefer the richer cross-source message when both fire for the same
    # number; keep any local-only duplicates that the remote set somehow
    # did not restate (should not happen for working-tree pairs).
    if remote_findings:
        return remote_findings
    return local


def repo_has_git_metadata(repo_root: Path) -> bool:
    """True when ``repo_root`` is a git work tree (``.git`` file or directory)."""
    return (repo_root / ".git").exists()

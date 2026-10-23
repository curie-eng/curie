"""Reuse complete kind CI proof for a release tree, or dispatch and await CI."""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import cast
from urllib.parse import quote, urlencode


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("CI deadline exceeded")
    return remaining


def _pause(seconds: float, deadline: float) -> None:
    time.sleep(min(seconds, _remaining(deadline)))


def _execute(
    command: list[str],
    deadline: float,
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=_remaining(deadline),
    )
    _remaining(deadline)
    return result


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return cast(dict[str, object], value)


def _sha(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def _positive_id(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
        return None
    return int(value) if value > 0 else None


def required_job_names(repo_root: Path, *, deadline: float) -> frozenset[str]:
    environment = dict(os.environ)
    environment.pop("CURIE_E2E_SHARDS_OVERRIDE", None)
    result = _execute(
        ["bash", "cli/scripts/cluster-upgrade-matrix.sh", "--list-shards", "--json"],
        deadline,
        cwd=repo_root,
        env=environment,
    )
    if result.returncode:
        raise RuntimeError(f"Canonical shard listing failed: {result.stderr.strip()}")
    shards = _object(json.loads(result.stdout)).get("shards")
    if not isinstance(shards, list) or not shards:
        raise ValueError("Canonical shard listing must contain a nonempty shards list")
    ids: set[str] = set()
    for shard in shards:
        shard_id = _object(shard).get("id")
        if not isinstance(shard_id, str) or not re.fullmatch(r"s[0-9]{2}", shard_id):
            raise ValueError("Invalid canonical shard ID")
        if shard_id in ids:
            raise ValueError(f"Duplicate canonical shard ID: {shard_id}")
        ids.add(shard_id)
    return frozenset(
        [
            "Rust convergence regressions (kind)",
            "E2E parity ladder (cluster rung on kind, fake model)",
            "E2E approval resume after worker and store restarts (kind)",
            "E2E released chart upgrade negative control (nil unsafe helpers)",
            "E2E cluster upgrade matrix shards",
            "E2E required",
            *(
                f"E2E chart runtime regressions (kind, Calico, {leg})"
                for leg in ("chart", "workload")
            ),
            *(
                f"E2E scripted factory (kind, scripted model, {suite})"
                for suite in (
                    "scenario",
                    "initial-binding",
                    "static-token-rotation",
                    "fresh-install",
                )
            ),
            *(f"Admitted agent runtime regressions (kind, shard {n}/5)" for n in range(1, 6)),
            *(f"E2E cluster upgrade matrix ({shard_id})" for shard_id in ids),
        ]
    )


def _run_reasons(run: Mapping[str, object]) -> list[str]:
    reasons = []
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        reasons.append("run did not complete successfully")
    if run.get("path") != ".github/workflows/ci.yaml":
        reasons.append("run is not ci.yaml")
    if _positive_id(run.get("id")) is None or not _sha(run.get("head_sha")):
        reasons.append("run identity is invalid")
    if not isinstance(run.get("html_url"), str) or not run["html_url"]:
        reasons.append("run URL is missing")
    return reasons


def qualification_reasons(
    run: Mapping[str, object],
    jobs: Sequence[Mapping[str, object]],
    *,
    candidate_tree: str,
    target_tree: str,
    required_names: frozenset[str],
) -> list[str]:
    reasons = _run_reasons(run)
    if not _sha(candidate_tree) or not _sha(target_tree) or candidate_tree != target_tree:
        reasons.append("commit trees differ or are invalid")
    evidence: dict[str, list[Mapping[str, object]]] = {name: [] for name in required_names}
    for job in jobs:
        name = job.get("name")
        if isinstance(name, str) and name in evidence:
            evidence[name].append(job)
    for name, entries in evidence.items():
        if not entries or any(
            job.get("status") != "completed" or job.get("conclusion") != "success"
            for job in entries
        ):
            reasons.append(f"required job lacks complete success: {name}")
    return reasons


class _GitHub:
    def __init__(self, repo: str, deadline: float) -> None:
        self.prefix = f"repos/{repo}"
        self.deadline = deadline
        self.commits: dict[str, tuple[str, str]] = {}

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        fields: Sequence[str] = (),
    ) -> tuple[dict[str, object], dict[str, str]]:
        while True:
            result = _execute(
                ["gh", "api", "--include", "--method", method, path, *fields],
                self.deadline,
            )
            header, separator, body = result.stdout.partition("\n\n")
            status = re.match(r"HTTP/\S+\s+(\d{3})\b", header)
            if not separator or not status:
                raise RuntimeError(
                    f"{method} {path}: missing HTTP response: {result.stderr.strip()}"
                )
            code = int(status[1])
            if method == "GET" and 500 <= code < 600:
                print(f"GET {path}: HTTP {code}; retrying", file=sys.stderr)
                _pause(2, self.deadline)
                continue
            if result.returncode or not 200 <= code < 300:
                raise RuntimeError(f"{method} {path}: HTTP {code}: {result.stderr.strip()}")
            headers = {}
            for line in header.splitlines()[1:]:
                name, colon, value = line.partition(":")
                if colon:
                    headers[name.lower()] = value.strip()
            return _object(json.loads(body)), headers

    def pages(self, path: str, key: str, **query: str) -> Iterator[dict[str, object]]:
        endpoint = f"{path}?{urlencode({'per_page': 100, 'page': 1, **query})}"
        while True:
            body, headers = self.request(endpoint)
            entries = body.get(key)
            if not isinstance(entries, list):
                raise ValueError(f"GET {endpoint}: missing or malformed {key}")
            for entry in entries:
                yield _object(entry)
            next_page = re.search(r'<([^>]+)>;\s*rel="next"', headers.get("link", ""))
            if not next_page:
                return
            endpoint = next_page[1]

    def commit(self, ref: str) -> tuple[str, str]:
        if ref not in self.commits:
            body, _ = self.request(f"{self.prefix}/commits/{quote(ref, safe='')}")
            sha = body.get("sha")
            tree = _object(_object(body.get("commit")).get("tree")).get("sha")
            if not _sha(sha) or not _sha(tree):
                raise ValueError(f"Commit {ref} lacks a valid SHA and tree")
            resolved = cast(str, sha), cast(str, tree)
            self.commits[ref] = self.commits[resolved[0]] = resolved
        return self.commits[ref]

    def select(self, ref: str, required_names: frozenset[str]) -> dict[str, object] | None:
        target_sha, target_tree = self.commit(ref)
        seen: set[int] = set()
        for query in ({"head_sha": target_sha}, {}):
            for run in self.pages(
                f"{self.prefix}/actions/workflows/ci.yaml/runs",
                "workflow_runs",
                status="success",
                **query,
            ):
                if _run_reasons(run):
                    continue
                run_id = cast(int, _positive_id(run["id"]))
                if run_id in seen:
                    continue
                seen.add(run_id)
                _, candidate_tree = self.commit(cast(str, run["head_sha"]))
                if candidate_tree != target_tree:
                    continue
                jobs = list(
                    self.pages(
                        f"{self.prefix}/actions/runs/{run_id}/jobs",
                        "jobs",
                        filter="latest",
                    )
                )
                if not qualification_reasons(
                    run,
                    jobs,
                    candidate_tree=candidate_tree,
                    target_tree=target_tree,
                    required_names=required_names,
                ):
                    return run
        return None

    def wait(self, run_id: int, poll_seconds: float) -> None:
        while True:
            run, _ = self.request(f"{self.prefix}/actions/runs/{run_id}")
            status = run.get("status")
            if status == "completed":
                if run.get("conclusion") != "success":
                    raise RuntimeError(f"CI run {run_id} concluded: {run.get('conclusion')}")
                return
            if status not in ("queued", "in_progress", "requested", "waiting", "pending"):
                raise ValueError(f"CI run {run_id} has invalid status: {status}")
            _pause(poll_seconds, self.deadline)


def main(argv: Sequence[str] | None = None) -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("select", "run"):
        subparser = commands.add_parser(command)
        subparser.add_argument("--repo", required=True)
        subparser.add_argument("--commit", required=True)
        subparser.add_argument(
            "--timeout-seconds",
            type=float,
            default=300 if command == "select" else 20400,
        )
        if command == "run":
            subparser.add_argument("--ref", required=True)
            subparser.add_argument("--poll-seconds", type=float, default=120)
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive and finite")
    if args.command == "run" and (not math.isfinite(args.poll_seconds) or args.poll_seconds < 0):
        parser.error("--poll-seconds must be nonnegative and finite")
    deadline = started + args.timeout_seconds
    try:
        required = required_job_names(Path(__file__).resolve().parents[1], deadline=deadline)
        github = _GitHub(args.repo, deadline)
        selected = github.select(args.commit, required)
        _remaining(deadline)
        if selected:
            print(f"Reusing complete CI proof: {selected['html_url']}", file=sys.stderr)
        if args.command == "select":
            print(
                json.dumps(
                    {
                        "selected_run_id": selected["id"] if selected else None,
                        "selected_run_url": selected["html_url"] if selected else None,
                    }
                )
            )
            return 0 if selected else 3
        if selected:
            mode, run_id, run_url = "reused", selected["id"], selected["html_url"]
        else:
            print(f"No qualifying CI run; dispatching full CI for {args.ref}", file=sys.stderr)
            dispatch, _ = github.request(
                f"{github.prefix}/actions/workflows/ci.yaml/dispatches",
                method="POST",
                fields=("-f", f"ref={args.ref}", "-F", "return_run_details=true"),
            )
            dispatched_id = _positive_id(dispatch.get("workflow_run_id"))
            if dispatched_id is None:
                raise ValueError("Dispatch response lacks a positive integral workflow_run_id")
            mode, run_id = "dispatched", dispatched_id
            run_url = f"https://github.com/{args.repo}/actions/runs/{run_id}"
            print(f"Waiting for dispatched CI run: {run_url}", file=sys.stderr)
            github.wait(dispatched_id, args.poll_seconds)
        _remaining(deadline)
        print(json.dumps({"mode": mode, "run_id": run_id, "run_url": run_url}))
        return 0
    except (OSError, subprocess.SubprocessError, ValueError, RuntimeError) as error:
        print(f"Kind CI error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

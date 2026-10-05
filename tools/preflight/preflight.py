#!/usr/bin/env python3
"""Run the service-free PR gates selected by a source checkout's diff."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Never

import yaml


@dataclass(frozen=True)
class Check:
    workflow: str
    job: str
    step: str
    command: str
    cwd: str
    group: str
    ci_command: str


def exact(job: str, step: str, command: str, cwd: str, group: str) -> Check:
    return Check("ci.yaml", job, step, command, cwd, group, command)


# These commands are declarations, independent of the workflows read by the
# drift guard. Updating CI requires an intentional update here and its tests.
ACTION_PINS_COMMAND = (
    r"""uv run --no-project --with pyyaml==6.0.3 python3 - <<'PY'
import re
from pathlib import Path

import yaml

def uses_nodes(node, seen):
    if id(node) in seen:
        return
    seen.add(id(node))
    if isinstance(node, yaml.MappingNode):
        for key, value in node.value:
            if isinstance(key, yaml.ScalarNode) and key.value == "uses":
                yield value
        children = (value for key, value in node.value)
    elif isinstance(node, yaml.SequenceNode):
        children = iter(node.value)
    else:
        children = ()
    for child in children:
        yield from uses_nodes(child, seen)

errors = []
checked = 0
directory = Path(".github/workflows")
for path in sorted((*directory.glob("*.yaml"), *directory.glob("*.yml"))):
    source = path.read_text()
    # Token gaps contain comments, unlike quoted scalars containing '#'.
    # YAML nodes also cover quoted keys, flow mappings, and aliases.
    comments = {}
    previous = 0
    for token in yaml.scan(source):
        gap = source[previous:token.start_mark.index]
        for match in re.finditer(r"#[^\n]*", gap):
            position = previous + match.start()
            comments[source.count("\n", 0, position)] = match.group()
        previous = token.end_mark.index
    root = yaml.compose(source, Loader=yaml.SafeLoader)
    for node in uses_nodes(root, set()):
        line = node.start_mark.line + 1
        if not isinstance(node, yaml.ScalarNode):
            errors.append((path, line, "uses must be a scalar action reference"))
            continue
        value = node.value
        # Local actions use this repository's checked out code.
        if not value.startswith(("actions/", "./")):
            checked += 1
            if not re.fullmatch(r"[^/\s@]+/[^\s@]+@[0-9a-fA-F]{40}", value):
                errors.append((path, line, "third party action requires a full 40 hex commit SHA"))
            comment = comments.get(node.end_mark.line, "")
            if not re.search(r"#\s*v[0-9]+(?:\.[0-9]+)*\b", comment):
                errors.append((path, line, "third party action requires a version comment"""
    r""" on its uses line"))
for path, line, message in errors:
    print(f"::error file={path},line={line}::{message}")
if errors:
    raise SystemExit(1)
print(f"Verified {checked} third party action references")
PY"""
)
COMMIT_COMMAND = r'''bash scripts/check-commit-messages.sh \
  "${{ github.event.pull_request.base.sha }}..${{ github.event.pull_request.head.sha }}"'''
GITLEAKS_COMMAND = r"""docker run --rm -v "${{ github.workspace }}:/repo" \
  "$GITLEAKS_IMAGE" \
  detect --source /repo --redact --verbose --exit-code 1 \
  --report-path /repo/gitleaks-report.json \
  ${GITLEAKS_LOG_OPTS:+"$GITLEAKS_LOG_OPTS"}"""
COMPILE_CI_COMMAND = "cargo test --locked"
COMPILE_FAST_COMMAND = "cargo check --locked --tests"
TYPESCRIPT_COMMAND = r"""npx --yes json-schema-to-typescript@15.0.4 \
  packages/aci-protocol/schema/aci-protocol.schema.json \
  --unreachableDefinitions \
  -o packages/aci-protocol/generated/ts/aci-protocol.ts
git diff --exit-code -- packages/aci-protocol/generated/ts/aci-protocol.ts"""
MANIFEST_COMMAND = (
    r"""pnpm gen:manifest
git diff --exit-code src/generated/commandManifest.ts \
  || (echo "commandManifest.ts is stale; run 'pnpm gen:manifest' in apps/ui"""
    r""" and commit." && exit 1)"""
)
# A fresh hook worktree has no node_modules. This is dependency setup from CI,
# not another selected gate, and --dry-run never runs it.
UI_SETUP = exact("ui", "Install dependencies", "pnpm install --frozen-lockfile", "apps/ui", "setup")
CHECKS = (
    exact(
        "action-pins",
        "Require third party action SHA pins and version comments",
        ACTION_PINS_COMMAND,
        ".",
        "always",
    ),
    exact("commit-messages", "Check the PR's commit messages", COMMIT_COMMAND, ".", "always"),
    Check(
        "gitleaks.yaml",
        "gitleaks",
        "Run gitleaks",
        GITLEAKS_COMMAND,
        ".",
        "always",
        GITLEAKS_COMMAND,
    ),
    exact("python", "Lockfile is up to date", "uv lock --check", ".", "python"),
    exact(
        "python",
        "Alembic revision gate",
        "uv run python scripts/check-alembic-revisions.py",
        ".",
        "python",
    ),
    exact(
        "python",
        "Schema window gate",
        "uv run python scripts/check-schema-window.py",
        ".",
        "python",
    ),
    exact("python", "Ruff", "uv run ruff check .", ".", "python"),
    exact("python", "Mypy", "uv run mypy", ".", "python"),
    exact(
        "python",
        "Import boundaries (harness SDK containment)",
        "uv run lint-imports",
        ".",
        "python",
    ),
    exact(
        "rust-lint", "Version consistency", "bash scripts/check-version-consistency.sh", ".", "rust"
    ),
    exact("rust-lint", "Fmt", "cargo fmt --check", "cli", "rust"),
    exact(
        "rust-lint", "Clippy", "cargo clippy --locked --all-targets -- -D warnings", "cli", "rust"
    ),
    exact(
        "rust-lint",
        "Schema baseline",
        "bash scripts/refresh-schema-baseline.sh --check",
        "cli",
        "rust",
    ),
    exact(
        "rust-lint",
        "Schema window gate",
        "uv run python scripts/check-schema-window.py",
        ".",
        "rust",
    ),
    Check(
        "ci.yaml",
        "rust-lint",
        "Generated ACI protocol crate compiles",
        COMPILE_FAST_COMMAND,
        "packages/aci-protocol/generated/rust",
        "rust",
        COMPILE_CI_COMMAND,
    ),
    exact(
        "contracts-ts",
        "Regenerate TypeScript from the committed schema and check for drift",
        TYPESCRIPT_COMMAND,
        ".",
        "contracts",
    ),
    exact(
        "contracts-ts",
        "tsc --noEmit on generated ACI types",
        "npx --yes -p typescript@5.9.3 tsc --noEmit -p "
        "packages/aci-protocol/generated/ts/tsconfig.json",
        ".",
        "contracts",
    ),
    exact("ui", "Command manifest is current", MANIFEST_COMMAND, "apps/ui", "cli"),
    exact("ui", "Lint", "pnpm lint", "apps/ui", "ui"),
)


def load_workflow(root: Path, filename: str) -> dict[str, Any]:
    try:
        workflow = yaml.safe_load((root / ".github/workflows" / filename).read_text())
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"Cannot read workflow {filename}: {error}") from error
    if not isinstance(workflow, dict) or not isinstance(workflow.get("jobs"), dict):
        raise ValueError(f"Workflow {filename} has no jobs mapping")
    return workflow


def validate_workflows(root: Path) -> None:
    declarations = (*CHECKS, UI_SETUP)
    workflows = {
        name: load_workflow(root, name) for name in {check.workflow for check in declarations}
    }
    for check in declarations:
        label = f"{check.workflow}:{check.job}: {check.step}"
        job = workflows[check.workflow]["jobs"].get(check.job)
        if not isinstance(job, dict) or not isinstance(job.get("steps"), list):
            raise ValueError(f"Mirrored step is missing: {label}")
        steps = [
            step
            for step in job["steps"]
            if isinstance(step, dict) and step.get("name") == check.step
        ]
        if len(steps) != 1:
            raise ValueError(f"Mirrored step is missing or duplicated: {label}")
        actual = steps[0].get("run")
        if not isinstance(actual, str) or actual.strip() != check.ci_command.strip():
            raise ValueError(f"Mirrored CI command changed: {label}")
        if check.step == "Generated ACI protocol crate compiles":
            # Decision 10 pins both sides separately. A caller cannot change
            # the fast command merely by retaining the same CI declaration.
            if (
                check.ci_command != "cargo test --locked"
                or check.command != "cargo check --locked --tests"
            ):
                raise ValueError(f"Compile-only mapping changed: {label}")
        elif check.command.strip() != check.ci_command.strip():
            raise ValueError(f"Mirrored preflight command changed: {label}")
        defaults = job.get("defaults", {}).get("run", {})
        cwd = steps[0].get("working-directory", defaults.get("working-directory", "."))
        if cwd == "${{ github.workspace }}":
            cwd = "."
        if cwd != check.cwd:
            raise ValueError(f"Mirrored working directory changed: {label}")
    image = workflows["gitleaks.yaml"]["jobs"]["gitleaks"].get("env", {}).get("GITLEAKS_IMAGE")
    if not isinstance(image, str) or not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("gitleaks.yaml: GITLEAKS_IMAGE must name a digest-pinned image")


def selected_checks(paths: list[str]) -> list[Check]:
    groups = {"always"}
    for path in paths:
        parts = Path(path).parts
        if (
            path.endswith(".py")
            or Path(path).name in {"pyproject.toml", "uv.lock"}
            or any(part.startswith("alembic") for part in parts)
        ):
            groups.add("python")
        if path.startswith(("cli/", "packages/aci-protocol/")):
            groups.add("rust")
        if path.startswith("packages/aci-protocol/"):
            groups.add("contracts")
        if path.startswith("cli/"):
            groups.add("cli")
        if path.startswith("apps/ui/"):
            groups.add("ui")
    return [check for check in CHECKS if check.group in groups]


def git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=root, text=True, capture_output=True, check=False)
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def render_command(root: Path, check: Check, base: str, head: str) -> str:
    command = check.command
    range_ = f"origin/{base}..{head}"
    command = command.replace(
        '"${{ github.event.pull_request.base.sha }}..${{ github.event.pull_request.head.sha }}"',
        shlex.quote(range_),
    )
    if check.job == "gitleaks":
        workflow = load_workflow(root, "gitleaks.yaml")
        image = workflow["jobs"]["gitleaks"]["env"]["GITLEAKS_IMAGE"]
        command = command.replace('"${{ github.workspace }}:/repo"', shlex.quote(f"{root}:/repo"))
        command = command.replace('"$GITLEAKS_IMAGE"', shlex.quote(image))
        command = command.replace(
            '${GITLEAKS_LOG_OPTS:+"$GITLEAKS_LOG_OPTS"}', shlex.quote(f"--log-opts={range_}")
        )
        # A linked worktree's .git file points outside its checkout. Preserve
        # those paths inside the scanner container so Git reads the same refs.
        git_dir = Path(git(root, "rev-parse", "--absolute-git-dir"))
        common_dir = Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir"))
        if git_dir != root / ".git":
            mounts = " ".join(
                f"-v {shlex.quote(f'{path}:{path}:ro')}" for path in sorted({git_dir, common_dir})
            )
            command = command.replace("docker run --rm", f"docker run --rm {mounts}", 1)
    return command


@contextmanager
def committed_checkout(root: Path, head: str) -> Iterator[Path]:
    """Run against the selected commit without changing the caller's files."""
    scratch = root / ".projects" / "preflight"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=scratch) as directory:
        checkout = Path(directory) / "checkout"
        registered = False
        try:
            git(root, "worktree", "add", "--quiet", "--detach", str(checkout), head)
            registered = True
            yield checkout
        finally:
            if registered:
                git(root, "worktree", "remove", "--force", str(checkout))
                if checkout.exists():
                    raise ValueError("The owned preflight worktree was not removed")


def execute_checks(
    root: Path, base: str, head: str, dry_run: bool, report: dict[str, Any]
) -> None:
    validate_workflows(root)
    paths = git(root, "diff", "--name-only", f"origin/{base}...{head}").splitlines()
    for check in selected_checks(paths):
        command = render_command(root, check, base, head)
        report["checks"].append(
            {
                "workflow": check.workflow,
                "job": check.job,
                "step": check.step,
                "command": command,
                "cwd": check.cwd,
                "group": check.group,
            }
        )
        if dry_run:
            continue
        label = f"{check.workflow}:{check.job}: {check.step}"
        print(f"preflight: {label}", file=sys.stderr, flush=True)
        if check.job == "ui" and check.step == "Lint":
            print(
                f"preflight: prerequisite {UI_SETUP.workflow}:{UI_SETUP.job}: {UI_SETUP.step}",
                file=sys.stderr,
                flush=True,
            )
            command = f"{UI_SETUP.command}\n{command}"
        try:
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", command],
                cwd=root / check.cwd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )
            exit_code, output = result.returncode, result.stdout
        except OSError as error:
            exit_code, output = 127, str(error)
        if exit_code:
            tail = "\n".join(output.splitlines()[-30:])[-12000:]
            report["failures"].append(
                {"check": label, "exit_code": exit_code, "output_tail": tail}
            )
            print(f"preflight: failed {label}\n{tail}", file=sys.stderr)
        else:
            print(f"preflight: passed {label}", file=sys.stderr)


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ValueError(message)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    parser = Parser(description=__doc__)
    parser.add_argument("--fast", action="store_true", help="Run only the service-free PR gates")
    parser.add_argument("--base", default="main", help="Compare against origin/<base>")
    parser.add_argument("--head", default="HEAD", help="Commit at the end of the comparison")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print checks without executing them"
    )
    parser.add_argument("--json", action="store_true", help="Emit one JSON result")
    json_output = "--json" in arguments
    report: dict[str, Any] = {"passed": False, "checks": [], "failures": []}
    try:
        args = parser.parse_args(arguments)
        if not args.fast:
            raise ValueError("Select --fast; only the fast tier is implemented")
        if args.base.startswith("-") or args.head.startswith("-"):
            raise ValueError("Base and head must be Git references, not options")
        root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel"))
        git(root, "rev-parse", "--verify", f"origin/{args.base}^{{commit}}")
        head = git(root, "rev-parse", "--verify", f"{args.head}^{{commit}}")
        report.update(base=args.base, head=args.head, dry_run=args.dry_run)
        if args.dry_run:
            execute_checks(root, args.base, args.head, True, report)
        else:
            with committed_checkout(root, head) as checkout:
                execute_checks(checkout, args.base, "HEAD", False, report)
        report["passed"] = not report["failures"]
        if report["failures"]:
            report.update(
                error="Fast preflight checks failed",
                fix="Fix every named check, then rerun curie dev preflight --fast",
            )
        exit_code = 0 if report["passed"] else 1
    except (ValueError, OSError) as error:
        report.update(
            error=str(error),
            fix="Check the repository toolchain and fetch the requested "
            "origin base, then rerun curie dev preflight --fast",
        )
        exit_code = 2
    if json_output:
        print(json.dumps(report))
    elif "error" in report:
        print(f"preflight: {report['error']}\nFix: {report['fix']}", file=sys.stderr)
    elif report.get("dry_run"):
        for check in report["checks"]:
            print(
                f"{check['workflow']}:{check['job']}: {check['step']} "
                f"(cwd {check['cwd']})\n{check['command']}"
            )
    else:
        print(f"Fast preflight passed ({len(report['checks'])} checks)")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

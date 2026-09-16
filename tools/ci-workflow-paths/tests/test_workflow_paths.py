"""Workspace path gate over the real GitHub Actions workflows (issue #1350).

The rule this file encodes: a `run:` step may only reference a workspace path
whose directory either exists in the repository or is created earlier in the
same job. A step that reads or writes a directory nothing produces is a defect
that no local run can catch, because nothing outside GitHub Actions executes a
workflow file.

Issue #1350 is the instance: `release.yaml` asserted the glibc floor against
`cli/dist/...` while the producing step wrote to `dist/...`, so the assert failed
on every tag and no GitHub Release was published at all. The gate encodes the
invariant rather than the string, so the next workflow that acquires the same
defect elsewhere reds too.

Issue #1713 is intentionally co-located here because the `pr-body.yaml`
workflow wiring is itself a repository-path contract: its PR-body checks must
invoke the checked-in gate from the workspace it runs in. Keeping that wiring
under the same real-workflow path sweep catches a moved or missing test path
where CI would otherwise fail only after the workflow starts.

The gate parses `.github/workflows/*.yaml` and `*.yml` directly and takes the
tracked path set from `git ls-files`. There are no fixtures and no mocks: the
whole value of this gate is that it reads what CI actually runs.

Deliberately out of scope: action inputs in `with:` blocks. They name repository
paths too, and modelling each action's per input path semantics (which inputs are
paths, which are created versus read, whether the action honours
`working-directory`, which it does not) is a different and much larger gate.

Two residual limits on the other side of the rule, where the gate reds a step
that is correct. Both were measured rather than estimated: thirteen plausible
and correct future workflow edits were run through the gate, and one of the
thirteen reds.

First, creator spellings. `CREATE` recognises the common ways a `run:` body makes
a directory, and nothing else. A directory created by a project script
(`make dist`, a `curie` subcommand, a `package.json` script), by a tool flag not
listed there (`helm package -d`, `rsync`, `cp --parents`), by a composite action
other than `actions/download-artifact`, or through a shell variable holding the
name, is invisible: the gate sees the read and not the creation, and reds a step
that is fine. The fix for that failure is to add the spelling to `CREATE`, NOT to
allowlist the path, and the rule test's message says so. The trade is deliberate:
recognising fewer spellings costs false positives, and recognising a command that
does not actually create (a bare `-C`, which is `git -C` and `make -C` too) costs
real coverage, so the flag forms that name an output are generic and the ones
that merely name a directory are anchored to the command that creates.

Second, prose inside a message. `echo "results land in out/summary.json"` is
structurally identical to a path reference and the gate reds it. This is the one
row of the thirteen that still reds, and it is accepted rather than fixed: no
rule distinguishes a path in a message from a path in a command without a shell
and English parser. `PROSE_EXEMPT` names each such token per step, which keeps
the exemption narrow and reds when the message is reworded.
"""

from __future__ import annotations

import functools
import posixpath
import re
import subprocess
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, NamedTuple

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

# Build output directories: gitignored, so the tracked path condition can never
# cover them, and populated by a tool rather than by a `mkdir` the gate can see.
# `cli/target` is cargo's, filled by the `cargo build` / `cargo zigbuild` step
# that precedes every read of it (release.yaml `Strip and name the binary`,
# ci.yaml `cli-portability`). Adding an entry here is a deliberate decision: it
# says a tool guarantees this directory, so the gate stops asking.
BUILD_OUTPUT_DIRS = frozenset({"cli/target"})

# Tokens that are prose inside a message, not paths the step touches. The `ui`
# job runs with `working-directory: apps/ui`, so the words "in apps/ui" inside
# the failure message of `Command manifest is current` resolve to the nonexistent
# `apps/ui/apps/ui`. Structurally indistinguishable from a path reference, so it
# is named rather than pattern matched away. Keyed on the resolved prefix as well
# as the step, so rewording the message reds this entry instead of silently
# widening it.
PROSE_EXEMPT = frozenset(
    {
        ("ci.yaml", "ui", "Command manifest is current", "apps/ui/apps/ui"),
    }
)

# Steps whose effective working directory is an expression the gate cannot
# resolve, so their tokens are not checked. Both are the buildx digest merge,
# which runs in ${{ runner.temp }}/digests, outside the workspace entirely, so no
# token in them could refer to a repository path anyway. Asserted as an exact
# set: a new unresolvable working directory must be a deliberate decision, not a
# silent hole.
UNRESOLVABLE_WORKING_DIR = frozenset(
    {
        ("release.yaml", "merge", "Create manifest list and push"),
        ("release.yaml", "worker-local-merge", "Create manifest list and push"),
    }
)

# `${{ github.workspace }}` IS the repository root, so it normalises to no prefix
# rather than counting as unresolvable. This is ci.yaml's `Version consistency`
# step, which overrides the `rust` job's `cli` default precisely to get back to
# the root; without the normalisation that one real, correct token is invisible.
WORKSPACE_EXPRESSION = "${{ github.workspace }}"

# One path segment: ordinary path characters, or a whole `${{ ... }}` / `${VAR}`
# / `$VAR` expression consumed intact, so the static prefix walk recognises the
# segment as dynamic rather than splitting through it.
SEGMENT = r"(?:[A-Za-z0-9_.*+@~-]|\$\{\{[^}]*\}\}|\$\{[^}]*\}|\$[A-Za-z_][A-Za-z0-9_]*)+"

# Anything that makes a segment dynamic, so the static prefix stops before it.
# `${{ ... }}` needs no branch of its own: it starts with `${`, so the `\$\{`
# branch already matches it.
DYNAMIC_SEGMENT = re.compile(r"\$\{|\$[A-Za-z_]|\*")

# `run:` bodies here are heavily commented and the comments name paths in prose
# ("Scanning the cli/ tree ...", "compose.release.yaml is generated ..."). A
# comment is not a path the step touches. Stripping can only lose coverage in a
# case the shell makes impossible, since a real command cannot live after a `#`
# on the same line. Only a `#` at line start or after whitespace is stripped, so
# a `#` inside a word (a URL fragment, a sed expression) survives.
COMMENT = re.compile(r"(?m)(?:^|(?<=\s))#.*$")

# A leading `./` is the most idiomatic way to write a workspace relative path in
# a shell, and the token lookbehind excludes `.` to keep mid path matches out, so
# without this normalisation `"./cli/dist/curie-x"` yields no token at all and
# issue #1350 written that way sails straight through. The lookbehind here is the
# token one, so `../dist` is left alone (the character before the matched `./` is
# a `.`), as are `$PWD/./x` and any `://` inside a URL.
LEADING_DOT_SLASH = re.compile(r"(?<![A-Za-z0-9_./$:-])\./")

# First segments the tracked tree can never supply, chosen by hand. Every one is
# a name CI conventionally writes its output to, and none is ever a tracked top
# level, so without this set they would never trigger extraction at all. `dist`
# is precisely the directory issue #1350 is about. Being untracked also means the
# tracked path condition can never cover a reference to one of these: it must
# come from a creator, which is exactly the property that catches this defect
# class.
#
# The list is hand written rather than derived, and no rule generates it. Three
# of the five (`build`, `dist`, `target`) do happen to sit in `.gitignore` and
# two (`artifacts`, `out`) do not, so do not read the set as `.gitignore`'s build
# output entries. Deriving it from `.gitignore` was rejected in both directions:
# it would miss `artifacts` and `out`, which exist only on the runner and so are
# never ignored here, and it would drag in `node_modules`, `.venv` and `agents`,
# which are not build output and would trigger extraction on tokens the gate can
# say nothing useful about.
UNTRACKED_TRIGGER_SEGMENTS = frozenset({"artifacts", "build", "dist", "out", "target"})

# One command line operand: ordinary non whitespace characters, with a quoted
# string or a `${{ ... }}` expression consumed whole. Splitting a creator's
# operands on bare whitespace would tear `"${{ runner.temp }}/digests"` into
# three pieces and register `runner.temp` as a created directory, which is the
# exact widening the empty static prefix guard exists to prevent.
OPERAND = r"(?:\$\{\{[^}]*\}\}|\"[^\"]*\"|'[^']*'|[^\s;&|)\"'])+"
OPERANDS = re.compile(OPERAND)

# Directory creation inside a `run:` body. Only two spellings were recognised
# originally (`mkdir` and `--dir`), which made every other idiomatic creator a
# false positive source now that five build output names trigger extraction: a
# step that plainly creates `out` two lines above reading it would red the gate
# and the message would tell its author to allowlist. A gate people route around
# protects nothing, so the common creator spellings are recognised here.
#
# `mkdir` captures its whole operand region rather than one argument, because
# `mkdir -p dist out` is completely ordinary and registering only `dist` from it
# is the single most likely way this gate produces a baffling failure. Its flags
# are not stripped here and do not need to be: the region is split into operands
# and `_creator_paths` drops every operand beginning with `-`, which is what
# handles a flag in any position, including the trailing `mkdir dist -p`.
#
# The flag forms are deliberately generic rather than anchored to the command
# that carries them (`--dir` is `gh release download`'s, `--target-dir` cargo's,
# `--outdir` python build's, `--output-dir` helm template's): a false creator can
# only weaken the gate slightly, whereas anchoring to one command invites the
# next one to slip past. `-d` and `-C` are the reverse case and ARE anchored to
# their commands, because a bare `-C` is `git -C` and `make -C` too, and those
# name a directory to run in rather than one to create.
CREATE = re.compile(
    rf"""
      (?<![\w./-])mkdir[ \t]+(?P<made>[^;&|)\n]+)
    | --(?:dir|outdir|output-dir|target-dir)[= ](?P<flagged>{OPERAND})
    | (?<![\w./-])(?:install|unzip)[^;&|)\n]*?[ \t]-d[ \t]+(?P<into>{OPERAND})
    | (?<![\w./-])tar[^;&|)\n]*?[ \t]-C[ \t]+(?P<extracted>{OPERAND})
    | (?<![\w./-])git[ \t]+clone[ \t]+(?:-\S+[ \t]+)*{OPERAND}[ \t]+(?P<cloned>{OPERAND})
    | (?<![\w./-])docker[ \t]+cp[ \t]+{OPERAND}[ \t]+(?P<copied>{OPERAND})
    """,
    re.VERBOSE,
)

# The third creator form: an action that materialises a directory. Without it the
# whole `release` job (`path: dist`) and ci.yaml's `e2e-ladder-cluster` (`path:
# cli/target/release`) would be false positive clusters.
DOWNLOAD_ARTIFACT = "actions/download-artifact"

Identity = tuple[str, str, str]


class Reference(NamedTuple):
    """One path-like token, resolved to the directory prefix the gate can check."""

    filename: str
    job_id: str
    step_name: str
    token: str
    prefix: str


class ResolvedStep(NamedTuple):
    """One step of one job, with its effective working directory already resolved."""

    identity: Identity
    step: dict[str, Any]
    prefix: str
    resolvable: bool


@functools.cache
def _tracked_files() -> frozenset[str]:
    """Every path `git ls-files` reports, relative to the repository root."""
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked = frozenset(entry for entry in completed.stdout.split("\0") if entry)
    assert tracked, f"`git ls-files` reported nothing under {REPO_ROOT}"
    return tracked


@functools.cache
def _tracked_entries() -> frozenset[str]:
    """Tracked files plus every parent directory of one.

    A reference to `scripts` or `charts/curie` names a directory git does not
    list on its own, so the parents have to be expanded or every directory
    reference in every workflow would read as unresolved.
    """
    entries: set[str] = set()
    for path in _tracked_files():
        entries.add(path)
        segments = path.split("/")
        for depth in range(1, len(segments)):
            entries.add("/".join(segments[:depth]))
    return frozenset(entries)


@functools.cache
def _trigger_segments() -> frozenset[str]:
    """First segments that make a token worth extracting.

    Derived from the tracked tree rather than hardcoded, so a top level directory
    added tomorrow is covered without anyone editing this file.

    `UNTRACKED_TRIGGER_SEGMENTS` is unioned in because build output directory
    names can never be tracked top levels and would otherwise never trigger. See
    that constant for why each name is there and why the set is not derived.

    Residual limit, and it is real: a token whose first segment is neither a
    tracked top level nor one of those hand added names is never extracted, and a
    token the extractor never yields is a token the rule can never check. A step
    that reads `coverage/report.xml` or `staging/bundle.tgz` is issue #1350's
    defect class exactly, and this gate is blind to it until the name is added
    here. Do not read a green sweep as proof that every path in a `run:` body was
    resolved; it proves only that every extracted one was.

    `.github` is subtracted because nothing in CI writes into `.github/`, so a
    missing directory defect is not reachable there, and the only `run:` body
    occurrences in this repository are not filesystem paths at all. There are two,
    both in ci.yaml: the `changes` job's path filter regex, where a backslash
    escape makes the token read `.github/workflows/ci`, and (excluded separately,
    by the token lookbehind) `--signer-workflow "$GITHUB_REPOSITORY/.github/...`.
    The first step is unnamed, so an identity keyed exemption for it would be
    keyed on a positional index and would rot on any reordering. Subtracting the
    directory is the honest fix.
    """
    tops = {path.split("/", 1)[0] for path in _tracked_files() if "/" in path}
    return frozenset((tops - {".github"}) | UNTRACKED_TRIGGER_SEGMENTS)


@functools.cache
def _path_token() -> re.Pattern[str]:
    """The token regex, built over the derived trigger segments.

    The negative lookbehind is the single most important piece of false positive
    control and every character in it is load bearing:

    * `/` and `.` exclude mid path matches, which keeps `$PWD/cli/target/...`
      (ci.yaml) and `"$GITHUB_REPOSITORY/.github/workflows/release.yaml"`
      (release.yaml) out. Both are correct paths the gate cannot resolve and must
      not guess at.
    * `:` excludes URLs, so `https://host/cli/...` never matches.
    * `$` and `-` exclude a trigger word appearing as the tail of a shell variable
      or of a flag.
    * `A-Za-z0-9_` excludes a trigger word appearing as the tail of a longer
      identifier.

    Requiring at least one `/` is what keeps the bare `charts/|cli/|` alternatives
    inside the `changes` job's grep pattern from matching: they have nothing after
    the slash.
    """
    alternation = "|".join(re.escape(segment) for segment in sorted(_trigger_segments()))
    return re.compile(rf"(?<![A-Za-z0-9_./$:-])(?:{alternation})(?:/{SEGMENT})+")


def _strip_comments(body: str) -> str:
    return COMMENT.sub("", body)


def _extract_tokens(body: str) -> list[str]:
    """Every path-like token in a `run:` body, comments stripped first.

    Quoted strings are deliberately NOT stripped. The #1350 defect is inside
    double quotes, so a quote stripping pass would delete the only violation the
    gate exists to find.

    A leading `./` is normalised off after comment stripping, so `./dist/x` is
    seen as the `dist/x` it names. See `LEADING_DOT_SLASH`.
    """
    return _path_token().findall(LEADING_DOT_SLASH.sub("", _strip_comments(body)))


def _static_prefix(token: str) -> str:
    """The longest leading run of segments containing no expression and no glob.

    The gate checks only this much. `dist/${{ matrix.target }}/foo` is checked as
    far as `dist` and no further, because the gate cannot know what a matrix
    expands to and guessing would be worse than not checking. The #1350 defect is
    in the static prefix, which is the general shape of this class: a wrong
    directory, not a wrong filename.
    """
    static: list[str] = []
    for segment in token.split("/"):
        if DYNAMIC_SEGMENT.search(segment):
            break
        static.append(segment)
    return "/".join(static)


@functools.cache
def _load_workflows() -> dict[str, dict[str, Any]]:
    """Parse every workflow file. Also a free structural check on the YAML.

    Both extensions: GitHub Actions honours `.yml` exactly as it honours `.yaml`,
    so a `.yaml`-only glob would let a whole workflow file opt out of this gate by
    being named `ci.yml`.
    """
    paths = sorted([*WORKFLOWS_DIR.glob("*.yaml"), *WORKFLOWS_DIR.glob("*.yml")])
    assert paths, f"no workflow files found under {WORKFLOWS_DIR}"
    loaded: dict[str, dict[str, Any]] = {}
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(document, dict), f"{path.name} did not parse as a YAML mapping"
        # Note: the `on:` key parses as the boolean True under the YAML 1.1
        # loader pyyaml uses. Nothing here reads it, so that is harmless.
        loaded[path.name] = document
    return loaded


def _iter_jobs() -> Iterator[tuple[str, dict[str, Any], str, dict[str, Any], list[Any]]]:
    """Yield (filename, document, job id, job, steps) for every job with steps."""
    for filename, document in _load_workflows().items():
        jobs = document.get("jobs") or {}
        for job_id, job in jobs.items():
            if not isinstance(job, dict):
                continue
            steps = job.get("steps")
            if not steps:
                # A job that delegates to a reusable workflow has `uses:` and no
                # `steps:` at all.
                continue
            yield filename, document, job_id, job, steps


def _declared_working_directory(node: dict[str, Any]) -> str | None:
    """A workflow level or job level `defaults: run: working-directory:`."""
    defaults = node.get("defaults")
    if not isinstance(defaults, dict):
        return None
    run = defaults.get("run")
    if not isinstance(run, dict):
        return None
    value = run.get("working-directory")
    return value if isinstance(value, str) else None


def _resolve_working_directory(raw: str | None) -> tuple[str, bool]:
    """Return (prefix relative to the repository root, resolvable).

    Precedence is applied by the caller, lowest to highest: workflow defaults,
    job defaults, step `working-directory`. A workflow level `defaults` exists
    nowhere today; the precedence is implemented anyway so adding one later cannot
    silently invalidate every resolution in this gate.
    """
    if raw is None:
        return "", True
    value = raw.strip()
    if value in {"", ".", "./", WORKSPACE_EXPRESSION}:
        return "", True
    if DYNAMIC_SEGMENT.search(value):
        return value, False
    return value.strip("/"), True


def _join(prefix: str, token: str) -> str:
    return posixpath.normpath(f"{prefix}/{token}") if prefix else token


def _step_name(step: dict[str, Any], index: int) -> str:
    name = step.get("name")
    return name if isinstance(name, str) else f"<unnamed step at index {index}>"


def _creator_paths(step: dict[str, Any], prefix: str) -> Iterator[str]:
    """Directories this step creates, resolved and reduced to a static prefix.

    A creator whose own path is an expression (`mkdir -p "${{ runner.temp }}/..."`)
    reduces to an empty static prefix and is dropped: registering it would grant
    permission to every path in the job.

    Each `CREATE` alternative carries exactly one group, and that group's operand
    region is split rather than taken whole, because `mkdir` accepts any number of
    directories. An operand beginning with `-` is a flag, not a directory.
    """
    candidates: list[tuple[str, str]] = []
    body = step.get("run")
    if isinstance(body, str):
        for match in CREATE.finditer(_strip_comments(body)):
            region = next(group for group in match.groups() if group is not None)
            candidates.extend(
                (prefix, operand)
                for operand in OPERANDS.findall(region)
                if not operand.startswith("-")
            )
    uses = step.get("uses")
    if isinstance(uses, str) and uses.split("@", 1)[0] == DOWNLOAD_ARTIFACT:
        with_block = step.get("with")
        if isinstance(with_block, dict):
            path = with_block.get("path")
            if isinstance(path, str):
                # An action input is relative to the workspace and does NOT honour
                # `working-directory`, so it resolves with no prefix at all.
                candidates.append(("", path))
    for creator_prefix, raw in candidates:
        static = _static_prefix(_join(creator_prefix, raw.strip("\"'")))
        if static:
            yield static


def _covered_by(prefix: str, directories: frozenset[str] | set[str]) -> bool:
    """True when `prefix` is one of `directories` or sits underneath one."""
    return any(prefix == entry or prefix.startswith(entry + "/") for entry in directories)


def _disposition(reference: Reference, created: set[str]) -> str | None:
    """How this reference resolves, or None when nothing accounts for it."""
    prefix = reference.prefix
    if prefix in _tracked_entries():
        return "tracked"
    if _covered_by(prefix, created):
        return "created"
    if _covered_by(prefix, BUILD_OUTPUT_DIRS):
        return "build-output"
    if (reference.filename, reference.job_id, reference.step_name, prefix) in PROSE_EXEMPT:
        return "prose-exempt"
    return None


def _iter_resolved_steps() -> Iterator[ResolvedStep]:
    """Every step of every job, with the working directory precedence applied.

    The single place the precedence chain is walked, lowest to highest: workflow
    defaults, job defaults, step `working-directory`. Both the sweep and the
    unresolvable-step set read it, so a fix to the precedence cannot land in one
    walk and be silently absent from the other.

    Steps with no `run:` body are yielded too. They are not path references, but
    an `actions/download-artifact` step is a creator, so the sweep needs to see
    them; consumers that only care about `run:` steps filter on the body.
    """
    for filename, document, job_id, job, steps in _iter_jobs():
        workflow_default = _declared_working_directory(document)
        job_default = _declared_working_directory(job)
        for index, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            declared = step.get("working-directory")
            raw = declared if isinstance(declared, str) else (job_default or workflow_default)
            prefix, resolvable = _resolve_working_directory(raw)
            identity = (filename, job_id, _step_name(step, index))
            yield ResolvedStep(identity, step, prefix, resolvable)


def _dispose(steps: Iterable[ResolvedStep]) -> Iterator[tuple[Reference, str | None]]:
    """Resolve every path-like token of the given steps against their creators.

    Ordering rule, and it matters: a step's own creators are registered BEFORE its
    own tokens are checked. `Strip and name the binary` does `mkdir -p dist` and
    `cp "$bin" "dist/..."` in the same body two lines apart, so checking tokens
    first would make that correct step a false positive. Within body line ordering
    is not modelled; `mkdir -p` is idempotent and conventionally first, and
    modelling intra body order would be a shell interpreter, which this is not.

    Creators are accumulated per job and keyed by `(filename, job id)`, because
    the rule is "created earlier in the SAME job": one job's `mkdir` grants
    nothing to the next, and a job id repeated in another workflow file is a
    different job entirely.

    Split out of `_sweep` so that keying can be pinned over constructed steps.
    The real tree cannot pin it: every job there that reads a created directory
    also creates it, so a single global creator set resolves the whole tree
    identically.
    """
    created: dict[tuple[str, str], set[str]] = {}
    for identity, step, prefix, resolvable in steps:
        filename, job_id, name = identity
        job_created = created.setdefault((filename, job_id), set())
        if resolvable:
            job_created.update(_creator_paths(step, prefix))
        body = step.get("run")
        if not resolvable or not isinstance(body, str):
            continue
        for token in _extract_tokens(body):
            reference = Reference(
                filename, job_id, name, token, _static_prefix(_join(prefix, token))
            )
            yield reference, _disposition(reference, job_created)


@functools.cache
def _sweep() -> tuple[tuple[Reference, str | None], ...]:
    """Resolve every path-like token in every `run:` step of every workflow."""
    return tuple(_dispose(_iter_resolved_steps()))


@functools.cache
def _unresolvable_steps() -> frozenset[Identity]:
    """Every `run:` step whose effective working directory the gate cannot resolve."""
    return frozenset(
        resolved.identity
        for resolved in _iter_resolved_steps()
        if not resolved.resolvable and isinstance(resolved.step.get("run"), str)
    )


def _tokens_of(filename: str, job_id: str, step_name: str) -> list[str]:
    return [
        reference.token
        for reference, _disposition in _sweep()
        if (reference.filename, reference.job_id, reference.step_name)
        == (filename, job_id, step_name)
    ]


def _pull_request_body_workflow_steps() -> list[dict[str, Any]]:
    """Return the dedicated PR-body workflow's steps, rejecting a CI transplant.

    This guard is intentionally rooted in the separate workflow GitHub runs for
    pull-request body edits. `ci.yaml` only runs on commits, so placing the
    checker there would leave a post-CI body edit unchecked.
    """
    workflows = _load_workflows()
    assert "pr-body.yaml" in workflows, (
        "the dedicated PR-body workflow is missing. Do not move this guard into "
        "ci.yaml: GitHub can edit a pull request body without creating a commit."
    )
    assert "check-pr-body.sh" not in (
        WORKFLOWS_DIR / "ci.yaml"
    ).read_text(encoding="utf-8"), (
        "the PR-body guard must remain independent of ci.yaml, whose commit-driven "
        "runs cannot observe a body-only edit."
    )
    jobs = workflows["pr-body.yaml"].get("jobs")
    assert isinstance(jobs, dict), "pr-body.yaml must declare the PR-body guard job"
    job = jobs.get("pr-body")
    assert isinstance(job, dict), "pr-body.yaml must retain its dedicated `pr-body` job"
    steps = job.get("steps")
    assert isinstance(steps, list), "the dedicated `pr-body` job must have steps"
    return [step for step in steps if isinstance(step, dict)]


def test_pr_body_guard_workflow_handles_body_only_pull_request_edits() -> None:
    """Pin #1713's runner wiring over the real workflow GitHub executes.

    A unit test of the shell matcher is not enough: the historical failure was a
    PR body containing literal ``\\n`` characters, and GitHub can create that
    failure through `edited` without starting commit-triggered CI. The test
    therefore checks the dedicated event wiring, safe github-script byte write,
    checker self-test, and the event-payload invocation as one executable
    contract. Removing any link makes this test red.
    """
    workflow = _load_workflows()["pr-body.yaml"]
    trigger = workflow.get(True, workflow.get("on"))
    assert isinstance(trigger, dict), "pr-body.yaml must be triggered by `pull_request`"
    pull_request = trigger.get("pull_request")
    assert isinstance(pull_request, dict), "pr-body.yaml must configure pull_request events"
    assert set(pull_request.get("types") or []) == {
        "opened",
        "reopened",
        "synchronize",
        "edited",
    }, (
        "the PR-body guard must run on opened, reopened, synchronize, and edited. "
        "In particular, `edited` closes the body-only bypass that ci.yaml cannot see."
    )

    steps = _pull_request_body_workflow_steps()
    script_steps = [
        step
        for step in steps
        if isinstance(step.get("uses"), str)
        and step["uses"].split("@", 1)[0] == "actions/github-script"
    ]
    assert len(script_steps) == 1, "the PR payload must be serialized by one github-script step"
    script_step = script_steps[0]
    assert script_step.get("env", {}).get("PR_BODY_FILE") == (
        "${{ runner.temp }}/curie-pr-body.md"
    ), "github-script must write only to a runner-owned temporary file"
    assert script_step.get("env", {}).get("PR_TITLE_FILE") == (
        "${{ runner.temp }}/curie-pr-title.txt"
    ), "github-script must write the untrusted title to a runner-owned temporary file"
    script = script_step.get("with", {}).get("script")
    assert isinstance(script, str), "github-script must contain the payload serialization script"
    assert "context.payload.pull_request?.body ?? \"\"" in script
    assert "context.payload.pull_request?.title ?? \"\"" in script
    assert "fs.writeFileSync(process.env.PR_BODY_FILE, body" in script
    assert "fs.writeFileSync(process.env.PR_TITLE_FILE, title" in script
    assert "github.event.pull_request.body" not in script, (
        "untrusted PR text must be read from context.payload, not interpolated into script source"
    )
    assert "github.event.pull_request.title" not in script, (
        "untrusted PR title must be read from context.payload, not interpolated into script source"
    )

    run_bodies = [step.get("run") for step in steps if isinstance(step.get("run"), str)]
    assert "bash scripts/check-pr-body.sh --self-test" in run_bodies, (
        "the workflow must execute the matcher's self-test before guarding event data"
    )
    guard_runs = [
        body
        for body in run_bodies
        if isinstance(body, str) and 'bash scripts/check-pr-body.sh "$PR_BODY_FILE"' in body
    ]
    assert len(guard_runs) == 1, "the event check must pass the runner temp file to the guard"
    assert "--title-file \"$PR_TITLE_FILE\"" in guard_runs[0], (
        "the event check must pass the title file so patch-release Trigger/Live proof can be gated"
    )
    assert "${{ github.event.pull_request.body }}" not in guard_runs[0], (
        "the guard must receive the temporary file, never an interpolated PR body"
    )
    assert "${{ github.event.pull_request.title }}" not in guard_runs[0], (
        "the guard must receive the title file, never an interpolated PR title"
    )


def _render(entries: list[Identity]) -> str:
    return "\n".join(f"  {filename} :: {job} :: {name}" for filename, job, name in sorted(entries))


def test_no_run_step_references_an_uncreated_directory() -> None:
    """The rule itself, swept over every step of every workflow file.

    This is deliberately a sweep over the present and future contents of
    `.github/workflows/`, with the trigger segments derived from `git ls-files`,
    rather than a list of the paths we happen to know about. A workflow added
    tomorrow, or a new top level directory, is covered without anyone editing a
    list.
    """
    unresolved = [
        f"{reference.filename} :: {reference.job_id} :: {reference.step_name}\n"
        f"      token  = {reference.token!r}\n"
        f"      prefix = {reference.prefix!r}"
        for reference, disposition in _sweep()
        if disposition is None
    ]
    assert not unresolved, (
        "these `run:` steps reference a workspace path whose directory is neither "
        "tracked in the repository nor created by an earlier or same step of the "
        "same job, so the step reads or writes a directory nothing produces. That "
        "is issue #1350's defect class: it cannot fail locally, only on the runner.\n"
        "Two things produce this failure and they need opposite fixes. Either the "
        "workflow path is wrong, and the fix is in the workflow; or this gate "
        "failed to see a creator that is right there, and the fix is in this file. "
        "The second is the likelier one when the directory is visibly created "
        "nearby, or when you did not touch the reported path: check `CREATE` "
        "against the exact spelling that makes the directory, and check that the "
        "creating step resolves under the same working directory as the reader. "
        "Allowlist only after ruling both out, and only with a why-comment saying "
        "what guarantees the directory that this gate cannot see. An allowlist "
        "entry added to silence a gate bug widens the gate permanently:\n"
        + "\n".join(f"  {entry}" for entry in sorted(unresolved))
    )


def test_the_rule_rejects_a_reference_nothing_accounts_for() -> None:
    """The rule's own outcome, demonstrated by execution rather than by history.

    Every other test here asserts what the real tree does, and the real tree is
    clean, so a `_disposition` that failed open would leave this whole file green
    while the gate protected nothing. This is the committed form of "revert the
    fix and watch it red", pinned over constructed input so it keeps holding after
    the workflows move on.
    """
    violating = Reference(
        "release.yaml",
        "cli-binaries",
        "Assert the glibc floor",
        "cli/dist/curie-${{ matrix.target }}",
        "cli/dist",
    )
    assert _disposition(violating, set()) is None, (
        "the gate no longer rejects issue #1350's own reference. `cli/dist` is not "
        "tracked, is created by nothing, and is on no allowlist, so it must resolve "
        "to a violation. If this passes, the rule test above is green because "
        "nothing can fail it, not because the workflows are correct."
    )
    covered = violating._replace(token="dist/curie-${{ matrix.target }}", prefix="dist")
    assert _disposition(covered, {"dist"}) == "created", (
        "the same reference under the directory the job actually creates must "
        "resolve, or the gate would red on the correct path too and the rejection "
        "above would be indiscriminate rather than a real decision."
    )


def test_every_allowlist_entry_is_actually_used() -> None:
    """No stale entry may sit on an allowlist granting permission to nothing.

    An unused entry is worse than no entry: it reads as a documented exception
    while it exempts nothing, and it silently widens the gate the day a token
    drifts under it.
    """
    used_build_output: set[str] = set()
    used_prose: set[tuple[str, str, str, str]] = set()
    for reference, disposition in _sweep():
        if disposition == "build-output":
            used_build_output.update(
                entry for entry in BUILD_OUTPUT_DIRS if _covered_by(reference.prefix, {entry})
            )
        elif disposition == "prose-exempt":
            used_prose.add(
                (reference.filename, reference.job_id, reference.step_name, reference.prefix)
            )
    assert used_build_output == set(BUILD_OUTPUT_DIRS), (
        "these BUILD_OUTPUT_DIRS entries cover no token in any workflow, so they "
        "grant permission to nothing. Either the build output moved and the entry "
        "rotted, or it was never needed:\n"
        + "\n".join(f"  {entry}" for entry in sorted(BUILD_OUTPUT_DIRS - used_build_output))
    )
    assert used_prose == set(PROSE_EXEMPT), (
        "these PROSE_EXEMPT entries match no token in any workflow. The entry is "
        "keyed on the resolved prefix as well as the step, so a reworded message or "
        "a changed working directory reds here rather than silently widening the "
        "exemption:\n" + "\n".join(f"  {entry}" for entry in sorted(PROSE_EXEMPT - used_prose))
    )


def test_unresolvable_working_directory_steps_are_exactly_the_known_set() -> None:
    """Presence guard on the gate's one blind spot, asserted in both directions.

    A step whose effective working directory is an expression has all of its
    tokens skipped. That hole must not grow silently, and it must not quietly
    close either: a shrinking set means an exemption is now dead weight.
    """
    actual = _unresolvable_steps()
    assert actual == UNRESOLVABLE_WORKING_DIR, (
        "the set of steps whose effective working directory the gate cannot "
        "resolve has changed. Every such step has ALL of its path tokens skipped, "
        "so an addition is a new hole in the gate and must be a deliberate "
        "decision, not a side effect.\n"
        "  newly unresolvable: "
        + _render(sorted(actual - UNRESOLVABLE_WORKING_DIR)).strip()
        + "\n  no longer unresolvable: "
        + _render(sorted(UNRESOLVABLE_WORKING_DIR - actual)).strip()
    )


def test_the_glibc_floor_asserts_are_still_seen_by_the_extractor() -> None:
    """Presence guard tying this gate to the defect it was written for.

    Without it, blinding the token regex would make the rule test pass vacuously
    on an empty token set: every sweep would be clean because nothing was ever
    extracted. Both glibc floor asserts, the per PR one in ci.yaml and the release
    one in release.yaml, must still produce their tokens.

    The release assertion is phrased on the suffix rather than the whole token
    because the token is exactly what issue #1350 changes: it reads
    `cli/dist/curie-...` before the fix and `dist/curie-...` after it, and this
    guard must hold across that change while the rule test is what moves.
    """
    portability = _tokens_of("ci.yaml", "cli-portability", "Assert the glibc floor")
    assert "scripts/check-glibc-floor.sh" in portability, (
        "the extractor no longer yields the `scripts/check-glibc-floor.sh` token "
        "from ci.yaml's per PR glibc floor assert. Either the step moved or the "
        "token regex has gone blind, in which case every sweep in this file is "
        f"passing vacuously. Tokens seen: {portability!r}"
    )
    assert "cli/target/x86_64-unknown-linux-gnu/release/curie" in portability, (
        "the extractor no longer yields ci.yaml's build output binary path, which "
        "is the one token exercising BUILD_OUTPUT_DIRS on the per PR side. Tokens "
        f"seen: {portability!r}"
    )
    release = _tokens_of("release.yaml", "cli-binaries", "Assert the glibc floor")
    assert "scripts/check-glibc-floor.sh" in release, (
        "the extractor no longer yields the `scripts/check-glibc-floor.sh` token "
        "from release.yaml's shipped artifact assert, the step issue #1350 is "
        f"about. Tokens seen: {release!r}"
    )
    artifact = [token for token in release if token.endswith("dist/curie-${{ matrix.target }}")]
    assert len(artifact) == 1, (
        "release.yaml's `Assert the glibc floor` no longer yields exactly one "
        "artifact token ending in `dist/curie-${{ matrix.target }}`. That token is "
        "the subject of issue #1350: the rule test is what decides whether its "
        f"directory resolves, and this guard is what proves it is still seen at "
        f"all. Tokens seen: {release!r}"
    )


def test_path_token_extraction_over_constructed_bodies() -> None:
    """Direct unit test over `_extract_tokens` and `_static_prefix`.

    The sweep exercises the extractor only through whatever the workflows happen
    to contain today, so the lookbehind's false positive control and the
    expression segment handling have no targeted coverage there at all: blinding
    either one changes the sweep's outcome in ways a reader cannot attribute. This
    builds bodies directly and asserts both directions, which is what makes those
    two pieces testable.
    """
    extracted: dict[str, tuple[str, str]] = {
        "a plain repository path": (
            "bash scripts/check-glibc-floor.sh",
            "scripts/check-glibc-floor.sh",
        ),
        "a double quoted path with a matrix expression": (
            'cp "$bin" "dist/curie-${{ matrix.target }}"',
            "dist/curie-${{ matrix.target }}",
        ),
        "a path with a shell variable segment": (
            "cp cli/target/${TARGET}/release/curie .",
            "cli/target/${TARGET}/release/curie",
        ),
        "a glob tail": ("for f in dist/*; do :; done", "dist/*"),
        "a path after an equals sign": ("--out=dist/checksums.txt", "dist/checksums.txt"),
        "a workspace relative path written with a leading dot slash": (
            'bash ./scripts/check-glibc-floor.sh "./cli/dist/foo"',
            "cli/dist/foo",
        ),
    }
    for label, (body, expected) in extracted.items():
        assert expected in _extract_tokens(body), (
            f"`_extract_tokens` no longer yields {expected!r} from {label}. A token "
            "the extractor cannot see is a token the rule cannot check, so every "
            f"sweep over a body of this shape passes vacuously. Body: {body!r}"
        )
    ignored: dict[str, str] = {
        # Excluded by the `/` and `.` in the lookbehind. An absolute host path
        # that the gate cannot resolve and must not guess at.
        "a $PWD anchored mount": (
            "docker run --rm -v "
            '"$PWD/cli/target/x86_64-unknown-linux-gnu/release/curie:/curie:ro" img'
        ),
        "a path under another shell variable": (
            '--signer-workflow "$GITHUB_REPOSITORY/.github/workflows/release.yaml"'
        ),
        # Excluded by the leading `./` normalisation declining to fire: the
        # character before the `./` is a `.`, so `..` stays intact and the token
        # lookbehind then rejects the `dist` after the slash. A parent directory
        # escape is outside the workspace and the gate must not resolve it.
        "a parent directory escape": 'cp "../dist/foo" .',
        # Excluded by the `:` in the lookbehind.
        "a URL containing a trigger segment": "curl -sSf https://example.invalid/cli/install.sh",
        # Excluded by the `A-Za-z0-9_` and `-` in the lookbehind.
        "a trigger word as the tail of an identifier": "echo $UPLOAD_dist/nothing",
        "a trigger word as the tail of a flag": "some-tool --release/candidate",
        # Excluded by requiring at least one segment after the slash, which is
        # what keeps the `changes` job's grep alternation out.
        "a bare alternation in a path filter regex": "grep -E '^(charts/|cli/|docs/)'",
        # Excluded by comment stripping.
        "a path named in prose inside a comment": (
            "# Scanning the cli/tree would drag in build output\ntrue"
        ),
    }
    unexpected = {
        label: tokens for label, body in ignored.items() if (tokens := _extract_tokens(body))
    }
    assert not unexpected, (
        "`_extract_tokens` matched these bodies, which contain no workspace path "
        "the gate can resolve. Each one reopens a false positive the lookbehind or "
        "the comment stripping was written to avoid:\n"
        + "\n".join(f"  {label} -> {tokens!r}" for label, tokens in sorted(unexpected.items()))
    )
    prefixes: dict[str, str] = {
        "dist/curie-${{ matrix.target }}": "dist",
        "cli/dist/curie-${{ matrix.target }}": "cli/dist",
        "cli/target/x86_64-unknown-linux-gnu/release/curie": (
            "cli/target/x86_64-unknown-linux-gnu/release/curie"
        ),
        "dist/*": "dist",
        "cli/target/${TARGET}/release/curie": "cli/target",
        "docs/adr/0075-agent-proxy.md": "docs/adr/0075-agent-proxy.md",
    }
    wrong = {
        token: actual
        for token, expected in prefixes.items()
        if (actual := _static_prefix(token)) != expected
    }
    assert not wrong, (
        "`_static_prefix` no longer stops where it should. It must stop at the "
        "first segment carrying an expression or a glob, because the gate cannot "
        "know what a matrix expands to, and it must consume a fully static path "
        "whole. Getting this wrong either checks a directory that does not exist "
        f"or checks nothing at all: {wrong!r}"
    )


def test_covered_by_stops_at_a_directory_boundary() -> None:
    """`_covered_by`'s `/` boundary, asserted in both directions.

    One expression decides every `created` and every `BUILD_OUTPUT_DIRS`
    resolution in the gate. Simplifying it to a bare `prefix.startswith(entry)`
    leaves the whole real tree sweep clean, because no workflow today references a
    sibling directory that shares a leading string with a covered one. The widening
    is total and silent: `cli/target-old` would resolve as build output, `dist`
    would cover `distribution`, and issue #1350's defect class goes green wherever
    the wrong directory happens to start with a right one.

    The empty entry case is the other half of that. An entry of `""` covering
    nothing is what stops a creator with no resolvable path from granting
    permission to the whole job, so it belongs on the same boundary as the rest.
    """
    inside: dict[str, tuple[str, set[str]]] = {
        "a file under a created directory": ("dist/curie-x86_64", {"dist"}),
        "a deeper path under a build output directory": (
            "cli/target/x86_64-unknown-linux-gnu/release/curie",
            {"cli/target"},
        ),
        "the covered directory named exactly": ("cli/target", {"cli/target"}),
        "one entry among several": ("dist/checksums.txt", {"cli/target", "dist"}),
    }
    missed = {
        label: (prefix, sorted(directories))
        for label, (prefix, directories) in inside.items()
        if not _covered_by(prefix, directories)
    }
    assert not missed, (
        "`_covered_by` no longer covers a prefix that genuinely sits under one of "
        "the directories. A prefix rejected wrongly is a correct step reported as "
        "issue #1350's defect, so the gate reds on paths that are fine and the next "
        f"reader learns to allowlist their way past it: {missed!r}"
    )
    outside: dict[str, tuple[str, set[str]]] = {
        "a sibling sharing a leading string": ("cli/target-old/release/curie", {"cli/target"}),
        "a longer top level sharing a leading string": ("distribution/x", {"dist"}),
        "a covering entry with no path at all": ("dist/anything", {""}),
    }
    widened = {
        label: (prefix, sorted(directories))
        for label, (prefix, directories) in outside.items()
        if _covered_by(prefix, directories)
    }
    assert not widened, (
        "`_covered_by` now covers a prefix that is NOT under any of the "
        "directories, so the gate has been silently widened for every created "
        "directory and every BUILD_OUTPUT_DIRS entry at once. A step reading a "
        "directory nothing produces resolves clean as soon as its name shares a "
        f"leading string with one that is produced: {widened!r}"
    )


def test_creator_registration_over_constructed_steps() -> None:
    """Direct unit test over `_creator_paths`, the gate's permission granting half.

    Three of its decisions are invisible to the real tree sweep. The empty static
    prefix guard is reached today (both `Export digest` steps in release.yaml do
    `mkdir -p` on an expression), but dropping it is masked by `_covered_by`'s
    boundary, so only the two mutations together show. The working directory
    prefixing and the "an action input does not honour `working-directory`" rule
    are latent: no creator sits under a working directory today, so both mutations
    are free until the day someone adds one, at which point permission is granted
    at the wrong level, which is issue #1350's own shape inverted. The `--dir=`
    spelling is simply written nowhere in this repository.

    Every creator spelling beyond `mkdir` and `--dir` is likewise written nowhere
    in this repository, so the real tree sweep cannot pin any of them and this
    table is the whole coverage. Each is asserted as an exact list, which is what
    pins a flag's VALUE as the thing that registers rather than the flag word:
    `--target-dir target/ci` must yield `target/ci` alone, and a regex that
    captured the flag word instead would yield the trigger segment `target-dir`.
    The same exactness pins `docker cp c:/o out` on the destination rather than
    the source, since registering a source would grant permission to a directory
    the step only reads.
    """
    creators: dict[str, tuple[dict[str, Any], str, list[str]]] = {
        "a mkdir at the repository root": ({"run": "mkdir -p dist"}, "", ["dist"]),
        "a mkdir inside a working directory": ({"run": "mkdir -p dist"}, "cli", ["cli/dist"]),
        "the space spelling of the download directory flag": (
            {"run": "gh release download --dir dist"},
            "",
            ["dist"],
        ),
        "the equals spelling of the download directory flag": (
            {"run": "gh release download --dir=dist"},
            "",
            ["dist"],
        ),
        "a creator whose own path is an expression": (
            {"run": 'mkdir -p "${{ runner.temp }}/digests"'},
            "",
            [],
        ),
        "a creator named only inside a comment": ({"run": "# mkdir -p dist\ntrue"}, "", []),
        # Every entry below is a creator spelling the gate was blind to while the
        # directory it creates was already a trigger segment, so each one was a
        # measured false positive: a correct step reading what it had just made.
        "a mkdir with several directory operands": (
            {"run": "mkdir -p dist out"},
            "",
            ["dist", "out"],
        ),
        "an expression operand alongside a real one": (
            {"run": 'mkdir -p "${{ runner.temp }}/digests" dist'},
            "",
            ["dist"],
        ),
        "a flag written after the operands": ({"run": "mkdir dist -p"}, "", ["dist"]),
        "install in its make-a-directory mode": ({"run": "install -d out"}, "", ["out"]),
        "the helm template output flag": (
            {"run": "helm template curie charts/curie --output-dir out"},
            "",
            ["out"],
        ),
        "the python build output flag": ({"run": "python -m build --outdir dist"}, "", ["dist"]),
        "the cargo target directory flag": (
            {"run": "cargo build --target-dir target/ci"},
            "",
            ["target/ci"],
        ),
        "the unzip destination flag": ({"run": "unzip -d build pkg.zip"}, "", ["build"]),
        "the tar change-directory flag": ({"run": "tar -xf pkg.tgz -C dist"}, "", ["dist"]),
        "a clone into a directory": (
            {"run": "git clone https://example.invalid/vendor.git build/vendor"},
            "",
            ["build/vendor"],
        ),
        # The near misses. Each of these registers the wrong operand under a
        # plausible mis-widening, and the wrong operand is a permission grant.
        "a copy out of a container, which creates the destination not the source": (
            {"run": "docker cp c:/o out"},
            "",
            ["out"],
        ),
        "a change-directory flag on a command that does not create it": (
            {"run": "git -C cli status && make -C docs html"},
            "",
            [],
        ),
        "an action input, which is workspace relative and ignores working-directory": (
            {"uses": f"{DOWNLOAD_ARTIFACT}@v4", "with": {"path": "dist"}},
            "cli",
            ["dist"],
        ),
    }
    wrong = {
        label: actual
        for label, (step, prefix, expected) in creators.items()
        if (actual := list(_creator_paths(step, prefix))) != expected
    }
    assert not wrong, (
        "`_creator_paths` no longer registers the directories a step creates the "
        "way the gate's rule requires. Registering too little turns a correct step "
        "into a false positive; registering too much grants permission the job "
        "never earned, and an empty prefix grants it to every path in the job at "
        "once. Each entry here is a form the gate claims to handle:\n"
        + "\n".join(f"  {label} -> {actual!r}" for label, actual in sorted(wrong.items()))
    )


def test_a_creator_grants_permission_only_inside_its_own_job() -> None:
    """The "same job" half of the rule, pinned over constructed steps.

    The accumulator keyed by `(filename, job id)` is what makes a `mkdir` grant
    permission to its own job and to nothing else. Collapsing it to one set shared
    by every job of every file leaves the whole real tree sweep clean, because
    every job there that reads a created directory also creates it, so the
    invariant has to be pinned on input built here. Without it any `mkdir`
    anywhere in a workflow would cover every read in every job of that file, and
    issue #1350's defect class goes green whenever some other job happens to make
    a directory of the same name.

    Both halves of the key are exercised: the same read in a second job of the
    same file, and the same read in a job of the same NAME in a different file.
    """
    read = {"run": "cat dist/thing.txt"}
    steps = [
        ResolvedStep(("w.yaml", "maker", "Make the directory"), {"run": "mkdir -p dist"}, "", True),
        ResolvedStep(("w.yaml", "maker", "Read it"), read, "", True),
        ResolvedStep(("w.yaml", "reader", "Read it"), read, "", True),
        ResolvedStep(("other.yaml", "maker", "Read it"), read, "", True),
    ]
    expected = {
        ("w.yaml", "maker"): "created",
        ("w.yaml", "reader"): None,
        ("other.yaml", "maker"): None,
    }
    actual = {
        (reference.filename, reference.job_id): disposition
        for reference, disposition in _dispose(iter(steps))
    }
    assert actual == expected, (
        "a creator no longer grants permission to its own job and only its own "
        "job. `mkdir -p dist` in `w.yaml`'s `maker` job must resolve that job's "
        "own read of `dist/thing.txt` and must leave the identical read unresolved "
        "in `w.yaml`'s `reader` job and in `other.yaml`'s own `maker` job. If a "
        "read outside the creating job now resolves, the creator accumulator has "
        "stopped being keyed per job or per file, and one `mkdir` anywhere grants "
        f"permission everywhere: {actual!r}"
    )


def test_working_directory_precedence_is_applied_in_order() -> None:
    """Workflow defaults, overridden by job defaults, overridden by the step itself.

    The chain has no intentional test otherwise. Making the gate ignore job level
    `defaults.run.working-directory` reds only the vacuity guard, and only as a
    side effect of one PROSE_EXEMPT entry ceasing to match, which aims the reader
    at an allowlist rather than at the resolution that actually broke. Delete that
    entry as well and every job level working directory in every workflow silently
    resolves to the repository root with the suite fully green.

    A workflow level `defaults` block exists nowhere today, so it is pinned on the
    reader both levels share rather than on a real workflow. The job and step
    levels are pinned on the resolved `Reference.prefix` of two real steps, which
    is the only place the precedence is actually applied, and which no allowlist
    entry can affect.
    """
    declared: dict[str, tuple[dict[str, Any], str | None]] = {
        "a defaults block naming a directory": (
            {"defaults": {"run": {"working-directory": "cli"}}},
            "cli",
        ),
        "a node carrying no defaults at all": ({}, None),
        "a defaults block with no run": ({"defaults": {"shell": "bash"}}, None),
        "a run block with no working-directory": ({"defaults": {"run": {"shell": "bash"}}}, None),
    }
    misread = {
        label: actual
        for label, (node, expected) in declared.items()
        if (actual := _declared_working_directory(node)) != expected
    }
    assert not misread, (
        "`_declared_working_directory` no longer reads a `defaults: run: "
        "working-directory:` block the way GitHub Actions does. Both the workflow "
        "level and the job level default come through this one reader, so getting "
        "it wrong silently resolves whole jobs at the repository root:\n"
        + "\n".join(f"  {label} -> {actual!r}" for label, actual in sorted(misread.items()))
    )
    resolved: dict[str, tuple[str | None, tuple[str, bool]]] = {
        "nothing declared anywhere": (None, ("", True)),
        "a plain directory": ("cli", ("cli", True)),
        "the workspace expression, which IS the repository root": (
            WORKSPACE_EXPRESSION,
            ("", True),
        ),
        "a bare dot": (".", ("", True)),
        "an expression the gate cannot resolve": (
            "${{ runner.temp }}/digests",
            ("${{ runner.temp }}/digests", False),
        ),
    }
    misresolved = {
        label: actual
        for label, (raw, expected) in resolved.items()
        if (actual := _resolve_working_directory(raw)) != expected
    }
    assert not misresolved, (
        "`_resolve_working_directory` no longer turns a declared working directory "
        "into the prefix the gate joins tokens against. Resolving an expression as "
        "if it were a directory checks paths that do not exist; failing to "
        "normalise the workspace expression hides real tokens behind a skipped "
        "step:\n"
        + "\n".join(f"  {label} -> {actual!r}" for label, actual in sorted(misresolved.items()))
    )
    swept = {
        (
            reference.filename,
            reference.job_id,
            reference.step_name,
            reference.token,
        ): reference.prefix
        for reference, _disposition in _sweep()
    }
    job_level = ("ci.yaml", "ui", "Command manifest is current", "apps/ui")
    assert swept.get(job_level) == "apps/ui/apps/ui", (
        "a token in ci.yaml's `ui` job no longer resolves under that job's "
        "`defaults.run.working-directory: apps/ui`. Either the step moved, or the "
        "gate has stopped applying job level defaults, in which case every job "
        "level working directory in every workflow now resolves at the repository "
        "root and the sweep is checking the wrong directories everywhere. This is "
        "asserted on the resolved prefix, not on a disposition, so no allowlist "
        f"entry can satisfy it. Prefix seen: {swept.get(job_level)!r}"
    )
    step_level = (
        "ci.yaml",
        "rust",
        "Version consistency",
        "scripts/check-version-consistency.sh",
    )
    assert swept.get(step_level) == "scripts/check-version-consistency.sh", (
        "ci.yaml's `Version consistency` step sets its own `working-directory` "
        "precisely to override the `rust` job's `cli` default and get back to the "
        "repository root. Its token no longer resolves there, so a step level "
        "declaration is no longer winning over the job level one, and every such "
        "override is now being resolved at the wrong level. Prefix seen: "
        f"{swept.get(step_level)!r}"
    )

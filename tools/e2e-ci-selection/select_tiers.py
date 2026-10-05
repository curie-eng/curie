"""Select end to end CI tiers from changed repository paths."""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import yaml

BASE_TIERS = ("skill", "local", "local-release", "cluster")
TIERS = (*BASE_TIERS, "released-upgrade")
OUTPUT_KEYS = {
    "skill": "skill",
    "local": "local",
    "local-release": "local_release",
    "cluster": "cluster",
    "released-upgrade": "released_upgrade",
}
# Jobs behind these tiers each boot a kind cluster. Callers omit them when
# the run should not pay for that.
KIND_TIERS = frozenset({"cluster", "released-upgrade"})
# The kind-rung factory scenario runs only when one of these paths changes.
FACTORY_PREFIXES = (
    "apps/api/src/curie_api/factory_",
    "apps/api/src/curie_api/routers/publications",
    "apps/worker/src/curie_worker/factory",
    "runner/src/curie_runner/verification.py",
    "runner/src/curie_runner/preflight",
    "examples/dark-factory",
    "tools/factory-e2e",
    "tools/model-script",
    "tools/github-stub",
)
# The kind approval resume scenario (#4016) runs only when a worker or approval
# path changes. Prefixes without a .py suffix match by raw string prefix, so
# "curie_api/approval" covers every approval*.py module.
APPROVAL_RESUME_PREFIXES = (
    "apps/worker/src/curie_worker",
    "apps/api/src/curie_api/resumequeue.py",
    "apps/api/src/curie_api/resumereconciler.py",
    "apps/api/src/curie_api/sweeper.py",
    "apps/api/src/curie_api/routers/approvals.py",
    "apps/api/src/curie_api/routers/approval_recovery.py",
    "apps/api/src/curie_api/approval",
    "runner/src/curie_runner/approval.py",
    "cli/scripts/e2e-cluster-approval-resume-restarts.sh",
)
UPGRADE_WORKFLOW_JOBS = frozenset(
    {
        "e2e-released-upgrade",
        "e2e-released-upgrade-negative",
        "e2e-cluster-upgrade-matrix-shards",
        "e2e-cluster-upgrade-matrix",
    }
)
WORKFLOW_PATH = ".github/workflows/ci.yaml"
REPO_ROOT = Path(__file__).resolve().parents[2]
CARGO_MANIFEST = "cli/Cargo.toml"
# A version-only diff (#3858) still runs this rung: it proves the new version
# identity end to end on the new commit.
VERSION_ONLY_TIERS = frozenset({"local-release"})
HUNK_HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


class RegistryError(ValueError):
    """The selection registry is malformed or ambiguous."""


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: yaml.SafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise RegistryError("registry contains an unhashable mapping key") from exc
        if duplicate:
            raise RegistryError(f"duplicate registry key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class Registry:
    fallback: tuple[str, ...]
    exact: dict[str, tuple[str, ...]]
    prefixes: dict[str, tuple[str, ...]]
    ignored_exact: tuple[str, ...]
    ignored_prefixes: tuple[str, ...]


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RegistryError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise RegistryError(f"{label} keys must be strings")
    return cast(dict[str, object], value)


def _tiers(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise RegistryError(f"{label} must be a tier list")
    tiers = tuple(item for item in value if isinstance(item, str))
    if len(set(tiers)) != len(tiers):
        raise RegistryError(f"{label} contains a duplicate tier")
    unknown = sorted(set(tiers).difference(TIERS))
    if unknown:
        raise RegistryError(f"{label} contains unknown tiers: {', '.join(unknown)}")
    return tiers


def _tier_rules(value: object, label: str) -> dict[str, tuple[str, ...]]:
    rules = _mapping(value, label)
    selected: dict[str, tuple[str, ...]] = {}
    for path, tiers in rules.items():
        if not path or path.startswith("/") or path.endswith("/"):
            raise RegistryError(f"{label} contains an invalid path")
        selected_tiers = _tiers(tiers, f"{label}.{path}")
        if not selected_tiers:
            raise RegistryError(f"{label}.{path} must select at least one tier")
        selected[path] = selected_tiers
    return selected


def _matches_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


# Fail-closed pytest set. Ignore rules may skip compose+pytest, but never
# for these Python or runtime paths even when a more-specific ignore exists
# (packages/test-support, apps/dispatcher, apps/ui).
MUST_RUN_PYTEST_EXACT = frozenset(
    {
        "uv.lock",
        "pyproject.toml",
        ".github/e2e-selection.yaml",
    }
)
MUST_RUN_PYTEST_PREFIXES = (
    "packages",
    "apps",
    "runner",
    "examples/tests",
    "cli",
    "tools",
    "release",
    ".github/workflows",
)
IMAGE_LOCKFILES = frozenset({"uv.lock", "pyproject.toml"})


def _is_must_run_pytest(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    if path in MUST_RUN_PYTEST_EXACT or name in MUST_RUN_PYTEST_EXACT:
        return True
    return any(_matches_prefix(path, prefix) for prefix in MUST_RUN_PYTEST_PREFIXES)


def _needs_pytest(registry: Registry, paths: list[str]) -> bool:
    if not paths:
        return True
    for path in paths:
        if _is_must_run_pytest(path):
            return True
        if not _is_ignored(registry, path):
            return True
    return False


def _needs_images(paths: list[str]) -> bool:
    if not paths:
        return True
    for path in paths:
        name = path.rsplit("/", 1)[-1]
        if path in IMAGE_LOCKFILES or name in IMAGE_LOCKFILES:
            return True
        if "Dockerfile" in name or name.endswith(".dockerfile"):
            return True
    return False


RUNTIME_ASSERTION_DIR = "charts/curie/ci/runtime"


def _is_runtime_assertion(path: str) -> bool:
    parent, _, name = path.rpartition("/")
    return parent == RUNTIME_ASSERTION_DIR and name.endswith(".sh")


def _matches_scenario(path: str, prefixes: tuple[str, ...]) -> bool:
    for prefix in prefixes:
        if path == prefix or path.startswith(f"{prefix}/"):
            return True
        if not prefix.endswith((".py", ".sh")) and path.startswith(prefix):
            return True
    return False


def _needs_factory(paths: list[str]) -> bool:
    if not paths:
        return True
    return any(_matches_scenario(path, FACTORY_PREFIXES) for path in paths)


def _needs_approval_resume(paths: list[str]) -> bool:
    if not paths:
        return True
    return any(_matches_scenario(path, APPROVAL_RESUME_PREFIXES) for path in paths)


def _needs_cli_release(paths: list[str]) -> bool:
    if not paths:
        return True
    return any(path == "cli" or path.startswith("cli/") for path in paths)


def _load_registry(path: Path) -> Registry:
    with path.open(encoding="utf-8") as stream:
        document = yaml.load(stream, Loader=UniqueKeyLoader)
    root = _mapping(document, "registry")
    if type(root.get("version")) is not int or root["version"] != 1:
        raise RegistryError("registry version must be 1")

    fallback = _tiers(root.get("fallback"), "fallback")
    if fallback != BASE_TIERS:
        raise RegistryError("fallback must contain every base tier in canonical order")

    rules = _mapping(root.get("rules"), "rules")
    if set(rules) != {"exact", "prefixes", "ignored_exact", "ignored_prefixes"}:
        raise RegistryError(
            "rules must define exact, prefixes, ignored_exact, and ignored_prefixes"
        )
    exact = _tier_rules(rules["exact"], "rules.exact")
    prefixes = _tier_rules(rules["prefixes"], "rules.prefixes")
    ignored_exact_rules = _mapping(rules["ignored_exact"], "rules.ignored_exact")
    ignored_exact: list[str] = []
    for ignored, value in ignored_exact_rules.items():
        if not ignored or ignored.startswith("/") or ignored.endswith("/"):
            raise RegistryError("rules.ignored_exact contains an invalid path")
        if value != []:
            raise RegistryError("ignored exact values must be empty lists")
        if ignored in exact or ignored in prefixes:
            raise RegistryError(f"ignored exact path overlaps a selected rule: {ignored}")
        ignored_exact.append(ignored)

    ignored_rules = _mapping(rules["ignored_prefixes"], "rules.ignored_prefixes")

    ignored_prefixes: list[str] = []
    for ignored, value in ignored_rules.items():
        if not ignored or ignored.startswith("/") or ignored.endswith("/"):
            raise RegistryError("rules.ignored_prefixes contains an invalid path")
        if value != []:
            raise RegistryError("ignored prefix values must be empty lists")
        ignored_prefixes.append(ignored)

    for ignored in ignored_prefixes:
        # Reject only when this ignore would hide a selected exact path or prefix.
        # A more-specific ignored child of a selected prefix is a hole, not an overlap.
        if any(_matches_prefix(path, ignored) for path in exact):
            raise RegistryError(f"ignored prefix overlaps a selected rule: {ignored}")
        if any(_matches_prefix(prefix, ignored) for prefix in prefixes):
            raise RegistryError(f"ignored prefix overlaps a selected rule: {ignored}")

    return Registry(fallback, exact, prefixes, tuple(ignored_exact), tuple(ignored_prefixes))


def _is_ignored(registry: Registry, path: str) -> bool:
    return path in registry.ignored_exact or any(
        _matches_prefix(path, prefix) for prefix in registry.ignored_prefixes
    )


def _select_path(registry: Registry, path: str) -> set[str]:
    if _is_ignored(registry, path):
        return set()

    selected: set[str] = set()
    matched = False
    if path in registry.exact:
        matched = True
        selected.update(registry.exact[path])
    for prefix, tiers in registry.prefixes.items():
        if _matches_prefix(path, prefix):
            matched = True
            selected.update(tiers)
    return selected if matched else set(registry.fallback)


def _changed_paths(base: str, head: str) -> list[str]:
    completed = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", f"{base}...{head}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in completed.stdout.splitlines() if line]


def _workflow_job_spans(content: str) -> dict[str, tuple[int, int]]:
    try:
        root = yaml.compose(content)
    except yaml.YAMLError as exc:
        raise RegistryError("workflow YAML is malformed") from exc
    if not isinstance(root, yaml.nodes.MappingNode):
        raise RegistryError("workflow root must be a mapping")

    root_keys: set[str] = set()
    jobs_node: yaml.nodes.MappingNode | None = None
    for key, value in root.value:
        if not isinstance(key, yaml.nodes.ScalarNode):
            raise RegistryError("workflow root keys must be strings")
        if key.value in root_keys:
            raise RegistryError(f"duplicate workflow root key: {key.value}")
        root_keys.add(key.value)
        if key.value == "jobs":
            if not isinstance(value, yaml.nodes.MappingNode):
                raise RegistryError("workflow jobs must be a mapping")
            jobs_node = value
        elif isinstance(value, yaml.nodes.MappingNode):
            if any(
                isinstance(nested_key, yaml.nodes.ScalarNode)
                and nested_key.value in UPGRADE_WORKFLOW_JOBS
                for nested_key, _ in value.value
            ):
                raise RegistryError("upgrade job moved outside workflow jobs")
    if jobs_node is None:
        raise RegistryError("workflow jobs mapping is missing")

    spans: dict[str, tuple[int, int]] = {}
    seen_jobs: set[str] = set()
    for index, (key, value) in enumerate(jobs_node.value):
        if not isinstance(key, yaml.nodes.ScalarNode):
            raise RegistryError("workflow job keys must be strings")
        if key.value in seen_jobs:
            raise RegistryError(f"duplicate workflow job: {key.value}")
        seen_jobs.add(key.value)
        if key.value in UPGRADE_WORKFLOW_JOBS:
            if not isinstance(value, yaml.nodes.MappingNode):
                raise RegistryError(f"workflow job {key.value} must be a mapping")
            beginning = key.start_mark.line + 1
            if index + 1 < len(jobs_node.value):
                next_key, _ = jobs_node.value[index + 1]
                end = next_key.start_mark.line + 1
            else:
                end = len(content.splitlines()) + 1
            # Two flow-style job keys can share a physical line. Both own it.
            spans[key.value] = (beginning, max(beginning + 1, end))

    # PyYAML resolves aliases to the anchor's source node and keeps the anchor's
    # marks. Until we track anchor dependencies, refuse an aliased target job
    # instead of silently treating its external definition as unrelated.
    try:
        alias_lines = (
            event.start_mark.line + 1
            for event in yaml.parse(content)
            if isinstance(event, yaml.events.AliasEvent) and event.start_mark is not None
        )
        if any(
            beginning <= line < end
            for line in alias_lines
            for beginning, end in spans.values()
        ):
            raise RegistryError("released-upgrade job uses a YAML alias")
    except yaml.YAMLError as exc:
        raise RegistryError("workflow YAML is malformed") from exc
    return spans


def _changes_upgrade_workflow_jobs(base: str, head: str) -> bool:
    merge_base = subprocess.run(
        ["git", "merge-base", base, head],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    old_content = subprocess.run(
        ["git", "show", f"{merge_base}:{WORKFLOW_PATH}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    new_content = subprocess.run(
        ["git", "show", f"{head}:{WORKFLOW_PATH}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    old_spans = _workflow_job_spans(old_content)
    new_spans = _workflow_job_spans(new_content)
    if not (old_spans or new_spans):
        raise RegistryError("workflow has no released-upgrade job anchors")

    diff = subprocess.run(
        ["git", "diff", "--no-renames", "--unified=0", merge_base, head, "--", WORKFLOW_PATH],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    hunk_lines = [line for line in diff.splitlines() if line.startswith("@@")]
    if not hunk_lines:
        raise RegistryError("changed workflow has no text hunks")
    for line in hunk_lines:
        match = HUNK_HEADER.match(line)
        if match is None:
            raise RegistryError(f"malformed workflow diff hunk: {line}")
        old_start, old_count, new_start, new_count = match.groups()
        for start, count, spans in (
            (int(old_start), int(old_count or "1"), old_spans),
            (int(new_start), int(new_count or "1"), new_spans),
        ):
            if count and any(
                start < end and start + count > beginning
                for beginning, end in spans.values()
            ):
                return True
    return False


def _load_atlas() -> ModuleType:
    """Load release/atlas.py by path; it owns the version-only path set."""
    path = REPO_ROOT / "release" / "atlas.py"
    spec = importlib.util.spec_from_file_location("release_atlas", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_atlas = _load_atlas()


def _head_version(head: str | None) -> str | None:
    """The CLI version at `head` (revision mode) or in this checkout (path mode).

    None when the manifest cannot be read, which keeps today's selection.
    """
    if head is None:
        try:
            manifest = (REPO_ROOT / CARGO_MANIFEST).read_text(encoding="utf-8")
        except OSError:
            return None
    else:
        completed = subprocess.run(
            ["git", "show", f"{head}:{CARGO_MANIFEST}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return None
        manifest = completed.stdout
    try:
        version = tomllib.loads(manifest)["package"]["version"]
    except (tomllib.TOMLDecodeError, KeyError, TypeError):
        return None
    return version if isinstance(version, str) and version else None


def _is_version_only(paths: list[str], head: str | None) -> bool:
    """Does the diff change only version identity for the head's release (#3858)?"""
    if not paths:
        return False
    version = _head_version(head)
    if version is None:
        return False
    allowed: frozenset[str] = _atlas.version_only_paths(f"v{version}")
    return set(paths) <= allowed


def _render(
    selected: set[str],
    pytest_needed: bool,
    images_needed: bool,
    cli_release_needed: bool,
    released_upgrade_full: bool,
    version_only: bool,
    factory_needed: bool,
    approval_resume_needed: bool,
) -> str:
    lines = [f"{OUTPUT_KEYS[tier]}={'true' if tier in selected else 'false'}" for tier in TIERS]
    skill_local = ",".join(tier for tier in TIERS[:2] if tier in selected)
    lines.append(f"skill_local_tiers={skill_local}")
    lines.append(f"pytest={'true' if pytest_needed else 'false'}")
    lines.append(f"images={'true' if images_needed else 'false'}")
    lines.append(f"cli_release={'true' if cli_release_needed else 'false'}")
    lines.append(
        f"released_upgrade_full={'true' if released_upgrade_full else 'false'}"
    )
    lines.append(f"version_only={'true' if version_only else 'false'}")
    lines.append(f"factory={'true' if factory_needed else 'false'}")
    lines.append(
        f"approval_resume={'true' if approval_resume_needed else 'false'}"
    )
    return "\n".join(lines) + "\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--push", action="store_true")
    parser.add_argument(
        "--omit-kind",
        action="store_true",
        help="Drop the cluster and released-upgrade tiers.",
    )
    return parser


def _run() -> None:
    args = _parser().parse_args()
    registry = _load_registry(args.registry)

    paths: list[str] = []
    workflow_upgrade_changed = False
    version_only = False
    factory_needed = False
    approval_resume_needed = False
    if args.push:
        if args.path or args.base or args.head:
            raise RegistryError("push cannot be combined with paths or revisions")
        selected = set(TIERS)
        pytest_needed = True
        images_needed = True
        cli_release_needed = True
        factory_needed = True
        approval_resume_needed = True
    else:
        if args.path and (args.base or args.head):
            raise RegistryError("paths cannot be combined with revisions")
        if args.path:
            paths = args.path
        elif args.base and args.head:
            paths = _changed_paths(args.base, args.head)
            if WORKFLOW_PATH in paths:
                workflow_upgrade_changed = _changes_upgrade_workflow_jobs(
                    args.base, args.head
                )
        else:
            raise RegistryError("provide paths, push, or both base and head revisions")
        selected = set().union(*(_select_path(registry, path) for path in paths))
        pytest_needed = _needs_pytest(registry, paths)
        images_needed = _needs_images(paths)
        cli_release_needed = _needs_cli_release(paths)
        version_only = _is_version_only(paths, args.head if not args.path else None)
        factory_needed = _needs_factory(paths)
        if factory_needed:
            selected.add("cluster")
        approval_resume_needed = _needs_approval_resume(paths)
        if approval_resume_needed:
            selected.add("cluster")

    if args.omit_kind:
        selected.difference_update(KIND_TIERS)
        # An added or changed cluster runtime assertion must run on the enforcing
        # cluster rung before `E2E required` can pass (#3391), so omitting kind
        # never drops the tier that proves it.
        if any(_is_runtime_assertion(path) for path in paths):
            selected.add("cluster")
        factory_needed = False
        approval_resume_needed = False
    if workflow_upgrade_changed:
        selected.add("released-upgrade")
    if version_only:
        # A release bump changes only version identity, so the proof the base
        # already has still holds (#3858). Run the rung that exercises the new
        # version, and leave pytest to the required job's release tests.
        selected = set(VERSION_ONLY_TIERS)
        pytest_needed = False

    # Ordinary pull requests run one upgrade matrix smoke shard. A change to
    # the upgrade jobs themselves runs the full matrix and released chart jobs
    # before merge, as do pushes and dispatches (the nightly).
    released_upgrade_full = (
        args.push or workflow_upgrade_changed
    ) and "released-upgrade" in selected

    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise RegistryError("GITHUB_OUTPUT is required")
    with Path(output_path).open("a", encoding="utf-8") as stream:
        stream.write(
            _render(
                selected,
                pytest_needed,
                images_needed,
                cli_release_needed,
                released_upgrade_full,
                version_only,
                factory_needed,
                approval_resume_needed,
            )
        )


def main() -> int:
    _run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

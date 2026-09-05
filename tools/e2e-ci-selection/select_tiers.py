"""Select end to end CI tiers from changed repository paths."""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
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


# Fail-closed pytest set. ignored_prefixes may skip compose+pytest, but never
# for these Python or runtime paths even when a more-specific ignore exists
# (packages/test-support, apps/dispatcher, apps/ui).
MUST_RUN_PYTEST_EXACT = frozenset({"uv.lock", "pyproject.toml"})
MUST_RUN_PYTEST_PREFIXES = (
    "packages",
    "apps",
    "runner",
    "examples/tests",
    "cli",
)


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
        if not any(_matches_prefix(path, prefix) for prefix in registry.ignored_prefixes):
            return True
    return False


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
    if set(rules) != {"exact", "prefixes", "ignored_prefixes"}:
        raise RegistryError("rules must define exact, prefixes, and ignored_prefixes")
    exact = _tier_rules(rules["exact"], "rules.exact")
    prefixes = _tier_rules(rules["prefixes"], "rules.prefixes")
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

    return Registry(fallback, exact, prefixes, tuple(ignored_prefixes))


def _select_path(registry: Registry, path: str) -> set[str]:
    if any(_matches_prefix(path, prefix) for prefix in registry.ignored_prefixes):
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


def _render(selected: set[str], pytest_needed: bool) -> str:
    lines = [f"{OUTPUT_KEYS[tier]}={'true' if tier in selected else 'false'}" for tier in TIERS]
    skill_local = ",".join(tier for tier in TIERS[:2] if tier in selected)
    lines.append(f"skill_local_tiers={skill_local}")
    lines.append(f"pytest={'true' if pytest_needed else 'false'}")
    return "\n".join(lines) + "\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--push", action="store_true")
    return parser


def _run() -> None:
    args = _parser().parse_args()
    registry = _load_registry(args.registry)

    if args.push:
        if args.path or args.base or args.head:
            raise RegistryError("push cannot be combined with paths or revisions")
        selected = set(TIERS)
        pytest_needed = True
    else:
        if args.path and (args.base or args.head):
            raise RegistryError("paths cannot be combined with revisions")
        if args.path:
            paths = args.path
        elif args.base and args.head:
            paths = _changed_paths(args.base, args.head)
        else:
            raise RegistryError("provide paths, push, or both base and head revisions")
        selected = set().union(*(_select_path(registry, path) for path in paths))
        pytest_needed = _needs_pytest(registry, paths)

    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise RegistryError("GITHUB_OUTPUT is required")
    with Path(output_path).open("a", encoding="utf-8") as stream:
        stream.write(_render(selected, pytest_needed))


def main() -> int:
    _run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

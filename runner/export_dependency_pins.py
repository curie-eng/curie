"""Export the runner's recursive registry dependency closure from uv.lock.

Each line is `name==version [; marker] --hash=sha256:...` with every wheel and
sdist hash the lock records, for `pip install --require-hashes`. A registry
record without valid sha256 hashes is refused.
"""

from __future__ import annotations

import re
import sys
import tomllib
from typing import Any


class InvalidLock(ValueError):
    """Raised when the runner dependency records cannot be trusted."""


def _normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _require_package_records(lock: dict[str, Any]) -> list[dict[str, Any]]:
    packages = lock.get("package")
    if not isinstance(packages, list) or not all(
        isinstance(package, dict) for package in packages
    ):
        raise InvalidLock("uv.lock must contain package records")
    return packages


def _package_name(package: dict[str, Any], context: str) -> str:
    name = package.get("name")
    if not isinstance(name, str) or not (normalized := _normalize_name(name)):
        raise InvalidLock(f"{context} must declare a package name")
    return normalized


_SHA256 = re.compile(r"sha256:[0-9a-f]{64}")


def _registry_hashes(package: dict[str, Any], name: str) -> list[str]:
    """Return the sorted, unique sha256 of every wheel and sdist of a package."""
    records: list[Any] = []
    wheels = package.get("wheels", [])
    if not isinstance(wheels, list):
        raise InvalidLock(f"uv.lock registry package {name} wheels must be a list")
    records.extend(wheels)
    if "sdist" in package:
        records.append(package["sdist"])

    hashes: set[str] = set()
    for record in records:
        value = record.get("hash") if isinstance(record, dict) else None
        if not isinstance(value, str) or not _SHA256.fullmatch(value):
            raise InvalidLock(
                f"uv.lock registry package {name} has a malformed sha256 hash"
            )
        hashes.add(value)
    if not hashes:
        raise InvalidLock(f"uv.lock registry package {name} must record hashes")
    return sorted(hashes)


def _runner_dependency_pins(lock: dict[str, Any]) -> list[str]:
    packages = _require_package_records(lock)
    package_by_name: dict[str, dict[str, Any]] = {}
    for package in packages:
        name = _package_name(package, "uv.lock package record")
        if name in package_by_name:
            raise InvalidLock(f"uv.lock resolves {name} more than once")
        package_by_name[name] = package

    runner = package_by_name.get("curie-runner")
    if runner is None:
        raise InvalidLock("uv.lock must contain exactly one curie-runner record")

    pins: dict[str, str] = {}
    pin_hashes: dict[str, list[str]] = {}
    reachable_conditions: dict[str, set[frozenset[str]]] = {}
    traversed_states: dict[
        str, set[tuple[frozenset[str], frozenset[str]]]
    ] = {}

    def visit(
        package: dict[str, Any],
        path_conditions: frozenset[str],
        requested_extras: frozenset[str],
    ) -> None:
        name = _package_name(package, "reachable uv.lock package record")
        known_conditions = reachable_conditions.setdefault(name, set())
        if not any(known <= path_conditions for known in known_conditions):
            known_conditions.difference_update(
                {known for known in known_conditions if path_conditions <= known}
            )
            known_conditions.add(path_conditions)

        known_states = traversed_states.setdefault(name, set())
        if any(
            known_conditions <= path_conditions and known_extras >= requested_extras
            for known_conditions, known_extras in known_states
        ):
            return
        known_states.difference_update(
            {
                (known_path, known_extras)
                for known_path, known_extras in known_states
                if path_conditions <= known_path and requested_extras >= known_extras
            }
        )
        known_states.add((path_conditions, requested_extras))

        source = package.get("source")
        if not isinstance(source, dict):
            raise InvalidLock(f"uv.lock package {name} must declare its source")

        if "registry" in source:
            version = package.get("version")
            if not isinstance(version, str) or not version:
                raise InvalidLock(
                    f"uv.lock registry package {name} must declare a version"
                )
            pins[name] = version
            pin_hashes[name] = _registry_hashes(package, name)

        def visit_dependencies(
            dependencies: Any, dependency_context: str
        ) -> None:
            if not isinstance(dependencies, list):
                raise InvalidLock(f"{dependency_context} must contain records")

            dependency_names: set[str] = set()
            for dependency in dependencies:
                if not isinstance(dependency, dict):
                    raise InvalidLock(
                        f"each {dependency_context} entry must be a record"
                    )
                dependency_name = _package_name(
                    dependency, f"each {dependency_context} entry"
                )
                if dependency_name in dependency_names:
                    raise InvalidLock(
                        f"{dependency_context} dependency {dependency_name} "
                        "is duplicated"
                    )
                dependency_names.add(dependency_name)

                marker = dependency.get("marker")
                if marker is not None and (
                    not isinstance(marker, str)
                    or not marker.strip()
                    or any(character in marker for character in "\r\n;")
                ):
                    raise InvalidLock(
                        f"{dependency_context} dependency {dependency_name} "
                        "must declare a valid marker"
                    )

                extras = dependency.get("extra", [])
                if not isinstance(extras, list) or not all(
                    isinstance(extra, str) and extra for extra in extras
                ):
                    raise InvalidLock(
                        f"{dependency_context} dependency {dependency_name} "
                        "extras must be names"
                    )

                resolved = package_by_name.get(dependency_name)
                if resolved is None:
                    raise InvalidLock(
                        f"uv.lock does not resolve {name} dependency "
                        f"{dependency_name}"
                    )
                dependency_conditions = path_conditions
                if marker is not None:
                    dependency_conditions = path_conditions | {marker}
                visit(resolved, dependency_conditions, frozenset(extras))

        visit_dependencies(
            package.get("dependencies", []), f"uv.lock package {name} dependencies"
        )

        if requested_extras:
            optional_dependencies = package.get("optional-dependencies", {})
            if not isinstance(optional_dependencies, dict):
                raise InvalidLock(
                    f"uv.lock package {name} optional dependencies must be tables"
                )
            for extra in sorted(requested_extras):
                extra_dependencies = optional_dependencies.get(extra)
                if extra_dependencies is None:
                    raise InvalidLock(
                        f"uv.lock package {name} does not declare selected extra {extra}"
                    )
                visit_dependencies(
                    extra_dependencies,
                    f"uv.lock package {name} extra {extra} dependencies",
                )

    runner_dependencies = runner.get("dependencies")
    if not isinstance(runner_dependencies, list):
        raise InvalidLock("curie-runner must declare dependency records")
    visit(runner, frozenset(), frozenset())
    pins.pop("curie-runner", None)

    requirements: list[str] = []
    for name, version in sorted(pins.items()):
        conditions = reachable_conditions[name]
        hash_options = "".join(f" --hash={value}" for value in pin_hashes[name])
        if frozenset() in conditions:
            requirements.append(f"{name}=={version}{hash_options}")
            continue

        path_markers: list[str] = []
        for condition in sorted(conditions, key=lambda item: tuple(sorted(item))):
            markers = sorted(condition)
            if len(markers) == 1:
                path_markers.append(markers[0])
            else:
                path_markers.append(
                    " and ".join(f"({marker})" for marker in markers)
                )
        combined_marker = path_markers[0]
        if len(path_markers) > 1:
            combined_marker = " or ".join(
                f"({path_marker})" for path_marker in path_markers
            )
        requirements.append(f"{name}=={version} ; {combined_marker}{hash_options}")

    return requirements


def main() -> int:
    try:
        lock = tomllib.loads(sys.stdin.read())
        print("\n".join(_runner_dependency_pins(lock)))
    except (InvalidLock, tomllib.TOMLDecodeError) as error:
        print(f"invalid uv.lock: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

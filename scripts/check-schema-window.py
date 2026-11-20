import argparse
import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[1]


def _fail(message: str) -> int:
    print(f"Schema window gate failed: {message}", file=sys.stderr)
    return 1


def _down_revision_ids(value: ast.expr) -> list[str]:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return [value.value]
    if isinstance(value, ast.Tuple):
        ids: list[str] = []
        for elt in value.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                ids.append(elt.value)
        return ids
    return []


def _revision_fields(path: Path) -> tuple[str | None, list[str]]:
    """Return module-level ``revision`` and ``down_revision`` string ids.

    Reads the values statically so the gate does not import (and therefore
    execute) the migration module. ``down_revision`` may be None, a string, or
    a tuple of strings; only string ids are returned as parents.
    """
    try:
        module = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, ValueError):
        return None, []
    revision: str | None = None
    down_ids: list[str] = []
    for node in module.body:
        if isinstance(node, ast.AnnAssign):
            targets: list[ast.expr] = [node.target]
            value = node.value
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        else:
            continue
        if value is None:
            continue
        names = {
            target.id for target in targets if isinstance(target, ast.Name)
        }
        if "revision" in names and isinstance(value, ast.Constant):
            if isinstance(value.value, str):
                revision = value.value
        if "down_revision" in names:
            down_ids = _down_revision_ids(value)
    return revision, down_ids


def _chart_app_version(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("appVersion:"):
            continue
        raw = stripped[len("appVersion:") :].strip()
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
            raw = raw[1:-1].strip()
        else:
            raw = raw.strip('"').strip()
        if raw[:1] in {"v", "V"}:
            raw = raw[1:].strip()
        return raw or None
    return None


def _load_catalog(path: Path) -> tuple[object, str | None]:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"catalog is not valid JSON: {exc}"
    return payload, None


def _semantic_version_key(version: str) -> tuple[int, int, int, int, int] | None:
    match = re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.(0|[1-9][0-9]*))?",
        version,
    )
    if match is None:
        return None
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), int(rc is None), int(rc or 0)


def _git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str] | None:
    """Run git inside ``repo_root`` without climbing into a parent repository."""
    env = dict(os.environ)
    # A non-git repo root must not resolve an enclosing repository.
    env["GIT_CEILING_DIRECTORIES"] = str(repo_root.parent)
    try:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
    except OSError:
        return None


def _catalog_window(text: str, app_version: str) -> dict | None:
    try:
        payload: object = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    windows = payload.get("windows")
    if not isinstance(windows, dict):
        return None
    window = windows.get(app_version)
    return window if isinstance(window, dict) else None


def _released_window(repo_root: Path, app_version: str) -> dict | None:
    """Return ``windows[app_version]`` as recorded at the ``v<app_version>`` tag.

    The release tag is the source because release tags are ancestors of main
    and the architecture atlas does not record schema windows. None means the
    tag is not readable, which happens between the release commit that
    registers the atlas and the tag push, or in a clone without tags.
    """
    result = _git(
        repo_root,
        "show",
        f"refs/tags/v{app_version}:cli/src/application_schema_windows.json",
    )
    if result is None or result.returncode != 0:
        return None
    return _catalog_window(result.stdout, app_version)


def _history_readable(repo_root: Path) -> bool:
    """False for a non-git root or a shallow clone, whose log cannot be trusted."""
    inside = _git(repo_root, "rev-parse", "--is-inside-work-tree")
    if inside is None or inside.returncode != 0 or inside.stdout.strip() != "true":
        return False
    shallow = _git(repo_root, "rev-parse", "--is-shallow-repository")
    if shallow is None or shallow.returncode != 0:
        return False
    return shallow.stdout.strip() == "false"


def _atlas_ids(repo_root: Path, rev: str) -> set[str] | None:
    """Atlas version ids at ``rev``.

    An empty set means the manifest is absent. None means it cannot be read.
    """
    spec = f"{rev}:docs/architecture-atlas/versions.json"
    present = _git(repo_root, "cat-file", "-e", spec)
    if present is None:
        return None
    if present.returncode != 0:
        # A missing path is an empty manifest. Any other git failure is unreadable.
        if "does not exist" in present.stderr or "not in" in present.stderr:
            return set()
        return None
    shown = _git(repo_root, "show", spec)
    if shown is None or shown.returncode != 0:
        return None
    try:
        payload: object = json.loads(shown.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    versions = payload.get("versions")
    if not isinstance(versions, list):
        return None
    ids: set[str] = set()
    for item in versions:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            return None
        ids.add(item["id"])
    return ids


def _commit_adds_version(repo_root: Path, commit: str, version_id: str) -> bool:
    child = _atlas_ids(repo_root, commit)
    if child is None or version_id not in child:
        return False
    parent = _git(repo_root, "rev-parse", "--verify", "--quiet", f"{commit}^")
    if parent is None or parent.returncode != 0 or not parent.stdout.strip():
        return True
    parent_ids = _atlas_ids(repo_root, parent.stdout.strip())
    if parent_ids is None:
        return False
    return version_id not in parent_ids


def _registered_window(repo_root: Path, app_version: str) -> dict | None:
    """Return the window frozen when ``v<app_version>`` was added to the atlas.

    The registration commit is the oldest commit on HEAD that added that id to
    ``docs/architecture-atlas/versions.json``. None means that commit cannot be
    read, including a non-git root and a shallow clone.
    """
    if not _history_readable(repo_root):
        return None
    version_id = f"v{app_version}"
    log = _git(
        repo_root,
        "log",
        "--reverse",
        f"-S{version_id}",
        "--format=%H",
        "--",
        "docs/architecture-atlas/versions.json",
    )
    if log is None or log.returncode != 0:
        return None
    for commit in log.stdout.split():
        if not _commit_adds_version(repo_root, commit, version_id):
            continue
        shown = _git(
            repo_root,
            "show",
            f"{commit}:cli/src/application_schema_windows.json",
        )
        if shown is None or shown.returncode != 0:
            return None
        return _catalog_window(shown.stdout, app_version)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the candidate schema window against the Alembic "
            "head and verify that Chart.yaml appVersion is catalogued."
        )
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=DEFAULT_REPO_ROOT,
        help="Repository root containing charts/, cli/, and apps/api/alembic.",
    )
    args = parser.parse_args()
    repo_root: Path = args.repo_root.resolve()

    chart_path = repo_root / "charts" / "curie" / "Chart.yaml"
    if not chart_path.is_file():
        return _fail(f"Chart.yaml does not exist: {chart_path}")
    app_version = _chart_app_version(chart_path)
    if app_version is None:
        return _fail(f"Chart.yaml has no appVersion: {chart_path}")

    catalog_path = repo_root / "cli" / "src" / "application_schema_windows.json"
    if not catalog_path.is_file():
        return _fail(f"catalog does not exist: {catalog_path}")
    payload, catalog_error = _load_catalog(catalog_path)
    if catalog_error is not None:
        return _fail(catalog_error)
    if not isinstance(payload, dict):
        return _fail("catalog missing candidate/windows/revisions")
    revisions = payload.get("revisions")
    candidate = payload.get("candidate")
    windows = payload.get("windows")
    if (
        not isinstance(revisions, list)
        or not isinstance(candidate, dict)
        or not isinstance(windows, dict)
    ):
        return _fail("catalog missing candidate/windows/revisions")
    catalog_ids = {item for item in revisions if isinstance(item, str)}

    versions = repo_root / "apps" / "api" / "alembic" / "versions"
    if not versions.is_dir():
        return _fail(f"versions directory does not exist: {versions}")

    graph: dict[str, list[str]] = {}
    try:
        for path in versions.iterdir():
            if (
                not path.is_file()
                or path.suffix != ".py"
                or path.name == "__init__.py"
            ):
                continue
            revision, down_ids = _revision_fields(path)
            if revision is None:
                continue
            graph[revision] = down_ids
    except OSError as exc:
        return _fail(f"could not scan versions directory {versions}: {exc}")

    parent_ids = {dep for deps in graph.values() for dep in deps}
    heads = sorted(revision for revision in graph if revision not in parent_ids)
    if len(heads) != 1:
        rendered_heads = ", ".join(heads) if heads else "none"
        return _fail(
            f"expected exactly one Alembic head, found {len(heads)}: "
            f"{rendered_heads}"
        )
    head = heads[0]

    missing = sorted(revision for revision in graph if revision not in catalog_ids)
    if missing:
        return _fail("catalog revisions missing alembic id " + ", ".join(missing))

    chart_window = windows.get(app_version)
    if not isinstance(chart_window, dict):
        return _fail(f"catalog has no window for appVersion {app_version}")
    chart_min = chart_window.get("schema_min")
    chart_head = chart_window.get("schema_head")
    if not isinstance(chart_min, str) or chart_min not in catalog_ids:
        return _fail(
            f"windows[{app_version!r}].schema_min is not a catalog revision: "
            f"{chart_min!r}"
        )
    if not isinstance(chart_head, str) or chart_head not in catalog_ids:
        return _fail(
            f"windows[{app_version!r}].schema_head is not a catalog revision: "
            f"{chart_head!r}"
        )
    if revisions.index(chart_min) > revisions.index(chart_head):
        return _fail(
            f"windows[{app_version!r}].schema_min is after its schema_head"
        )

    for version in windows:
        if _semantic_version_key(version) is None:
            return _fail(
                f"catalog window key is not a supported semantic version: {version!r}"
            )

    schema_min = candidate.get("schema_min")
    schema_head = candidate.get("schema_head")
    if not isinstance(schema_min, str) or schema_min not in catalog_ids:
        return _fail(f"candidate.schema_min is not a catalog revision: {schema_min!r}")
    if not isinstance(schema_head, str) or schema_head not in catalog_ids:
        return _fail(f"candidate.schema_head is not a catalog revision: {schema_head!r}")
    if revisions.index(schema_min) > revisions.index(schema_head):
        return _fail("candidate.schema_min is after candidate.schema_head")
    if schema_head != head:
        return _fail(
            f"candidate.schema_head is {schema_head!r} "
            f"but alembic head is {head}"
        )

    # @spec PROTECTED-HOOK-SOURCE-2: compare the installed owner to actual inputs.
    serving_path = repo_root / (
        "packages/protected-hooks/src/curie_protected_hooks/schema_serving.json"
    )
    try:
        serving = json.loads(serving_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return _fail(f"cannot read shared serving metadata: {exc}")
    if serving != {
        "schema_min": schema_min, "schema_head": schema_head, "revision_parents": graph
    }:
        return _fail("shared serving metadata differs from candidate or actual graph")

    api_window_path = (
        repo_root / "apps" / "api" / "src" / "curie_api" / "schema_compat.json"
    )
    try:
        api_window = json.loads(api_window_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return _fail(f"cannot read API schema compatibility window: {exc}")
    if not isinstance(api_window, dict):
        return _fail("API schema compatibility window is not an object")
    if (
        api_window.get("schema_min") != schema_min
        or api_window.get("schema_head") != schema_head
    ):
        return _fail(
            "candidate schema bounds do not match "
            "apps/api/src/curie_api/schema_compat.json"
        )

    atlas_path = repo_root / "docs" / "architecture-atlas" / "versions.json"
    try:
        atlas = json.loads(atlas_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return _fail(f"cannot read architecture atlas versions: {exc}")
    atlas_versions = atlas.get("versions") if isinstance(atlas, dict) else None
    if not isinstance(atlas_versions, list) or not all(
        isinstance(item, dict) and isinstance(item.get("id"), str)
        for item in atlas_versions
    ):
        return _fail("architecture atlas versions manifest is malformed")
    released = any(item["id"] == f"v{app_version}" for item in atlas_versions)
    tagged_window = _released_window(repo_root, app_version) if released else None
    matches_candidate = chart_min == schema_min and chart_head == schema_head
    if tagged_window is not None:
        # A released window is immutable; only the candidate may move.
        if chart_window != tagged_window:
            return _fail(
                f"windows[{app_version!r}] differs from its v{app_version} "
                "release tag; a released window is immutable, so move only "
                "the candidate"
            )
    elif released:
        # The atlas names this version before its tag is pushed. Freeze the
        # window at the commit that added the atlas id. When that commit
        # cannot be read, keep the candidate-equality rule.
        registered_window = _registered_window(repo_root, app_version)
        if registered_window is not None:
            if chart_window != registered_window:
                return _fail(
                    f"windows[{app_version!r}] differs from the window recorded "
                    f"when v{app_version} was registered; a released window is "
                    "immutable, so move only the candidate"
                )
        elif not matches_candidate:
            return _fail(
                f"windows[{app_version!r}] differs from candidate and the "
                f"v{app_version} release tag is not readable; "
                "run git fetch --tags origin"
            )
    elif not matches_candidate:
        return _fail(
            f"windows[{app_version!r}] differs from candidate; "
            f"rerun curie dev bump-version {app_version} for the same version"
        )

    print(
        "schema-window OK: "
        f"chart appVersion {app_version} "
        f"candidate schema_head {head}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

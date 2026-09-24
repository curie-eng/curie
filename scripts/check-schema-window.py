import argparse
import ast
import json
import re
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the newest catalog schema window against the Alembic "
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
        return _fail("catalog missing windows/revisions")
    revisions = payload.get("revisions")
    windows = payload.get("windows")
    if not isinstance(revisions, list) or not isinstance(windows, dict):
        return _fail("catalog missing windows/revisions")
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

    newest_app_version: str | None = None
    newest_version_key: tuple[int, int, int, int, int] | None = None
    chart_is_release_candidate = False
    for version in windows:
        version_key = _semantic_version_key(version)
        if version_key is None:
            return _fail(
                f"catalog window key is not a supported semantic version: {version!r}"
            )
        if version == app_version:
            chart_is_release_candidate = version_key[3] == 0
        if newest_version_key is None or version_key > newest_version_key:
            newest_app_version = version
            newest_version_key = version_key

    if newest_app_version is None:
        return _fail("catalog has no schema windows")
    newest_window = windows[newest_app_version]
    if not isinstance(newest_window, dict):
        return _fail(
            f"catalog window for newest appVersion {newest_app_version} is not an object"
        )
    schema_head = newest_window.get("schema_head")
    if schema_head != head:
        return _fail(
            f"windows[{newest_app_version!r}].schema_head is {schema_head!r} "
            f"but alembic head is {head}"
        )
    if chart_is_release_candidate and chart_window.get("schema_head") != head:
        return _fail(
            f"windows[{app_version!r}].schema_head is "
            f"{chart_window.get('schema_head')!r} but alembic head is {head}"
        )

    print(
        "schema-window OK: "
        f"chart appVersion {app_version} "
        f"catalog appVersion {newest_app_version} schema_head {head}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

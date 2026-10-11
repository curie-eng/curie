import argparse
import ast
import hashlib
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from alembic.script import ScriptDirectory

FILENAME_PATTERN = re.compile(r"^(\d+[a-z]?)_.+\.py$")
DEFAULT_REPO = Path(__file__).resolve().parents[1]
DEFAULT_SCRIPT_LOCATION = (
    Path(__file__).resolve().parents[1] / "apps" / "api" / "alembic"
)
DEFAULT_KINDS_FILE = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "src"
    / "curie_api"
    / "revision_kinds.json"
)
DEFAULT_WINDOW_FILE = DEFAULT_KINDS_FILE.with_name("schema_compat.json")
DEFAULT_SERVING_FILE = (
    Path(__file__).resolve().parents[1]
    / "packages/protected-hooks/src/curie_protected_hooks/schema_serving.json"
)
DEFAULT_CATALOG_FILE = (
    Path(__file__).resolve().parents[1] / "cli/src/application_schema_windows.json"
)
DEFAULT_CHART_METADATA = (
    Path(__file__).resolve().parents[1] / "charts/curie/files/schema-compat.json"
)
_VALID_KINDS = {"expand", "contract", "irreversible"}


def _revision_id(path: Path) -> str | None:
    """Return a migration's module-level ``revision`` value.

    Reads the value statically so a duplicate is reported without importing
    (and therefore executing) the migration module. Returns None when the value
    is absent or not a literal string; the graph load below reports those.
    """
    try:
        module = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, ValueError):
        return None
    for node in module.body:
        if isinstance(node, ast.AnnAssign):
            targets: list[ast.expr] = [node.target]
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
        else:
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "revision"
            for target in targets
        ):
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
    return None


def _revision_lineage(
    source: str, filename: str
) -> tuple[str, str | tuple[str, ...] | None]:
    """Read literal module assignments without executing a migration."""
    values: dict[str, object] = {}
    for node in ast.parse(source, filename=filename).body:
        if isinstance(node, ast.AnnAssign):
            targets: list[ast.expr] = [node.target]
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
        else:
            continue
        for target in targets:
            if (
                isinstance(target, ast.Name)
                and target.id in {"revision", "down_revision"}
                and node.value is not None
            ):
                values[target.id] = ast.literal_eval(node.value)

    revision = values.get("revision")
    if not isinstance(revision, str) or "down_revision" not in values:
        raise ValueError(f"{filename}: expected literal revision and down_revision assignments")
    parent = values["down_revision"]
    if parent is None:
        parents: str | tuple[str, ...] | None = None
    elif isinstance(parent, str):
        parents = parent
    elif isinstance(parent, (list, tuple)) and all(isinstance(item, str) for item in parent):
        parents = tuple(sorted(parent))
    else:
        raise ValueError(f"{filename}: down_revision must be None, a string, or string parents")
    return revision, parents


def _cross_train_shared_count(versions: Path, repo: Path, ref: str) -> int:
    local: dict[str, tuple[str, str | tuple[str, ...] | None]] = {}
    for path in sorted(versions.glob("*.py")):
        if not path.is_file() or path.name == "__init__.py":
            continue
        revision, parents = _revision_lineage(path.read_text(encoding="utf-8"), path.name)
        local[revision] = (path.name, parents)

    versions_path = "apps/api/alembic/versions"
    listing = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "--name-only", "-z", f"{ref}:{versions_path}"],
        check=True,
        capture_output=True,
        text=True,
    )
    other: dict[str, tuple[str, str | tuple[str, ...] | None]] = {}
    for filename in sorted(listing.stdout.split("\0")):
        if not filename.endswith(".py") or filename == "__init__.py":
            continue
        source = subprocess.run(
            ["git", "-C", str(repo), "show", f"{ref}:{versions_path}/{filename}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        revision, parents = _revision_lineage(source, filename)
        if revision in other:
            raise ValueError(
                f"{ref}: duplicate revision id {revision!r} in "
                f"{other[revision][0]} and {filename}"
            )
        other[revision] = (filename, parents)

    shared = sorted(local.keys() & other.keys())
    mismatches = []
    for revision in shared:
        if local[revision] != other[revision]:
            local_filename, local_parents = local[revision]
            other_filename, other_parents = other[revision]
            mismatches.append(
                f"revision {revision!r}: local {local_filename} "
                f"down_revision={local_parents!r}; {ref} {other_filename} "
                f"down_revision={other_parents!r}"
            )
    if mismatches:
        raise ValueError("\n".join(mismatches))
    return len(shared)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Alembic revision numbers and graph heads."
    )
    parser.add_argument(
        "--script-location",
        type=Path,
        help="Alembic script directory to validate (default: <repo>/apps/api/alembic).",
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=DEFAULT_REPO,
        help="Repository for local migrations and Git reads (default: repository root).",
    )
    parser.add_argument(
        "--other-train-ref",
        help="Git ref whose shared revision filenames and parents must match the local tree.",
    )
    parser.add_argument(
        "--write-upgrade-metadata",
        action="store_true",
        help="Regenerate the chart's schema metadata from the API window and graph.",
    )
    args = parser.parse_args()
    script_location: Path = args.script_location or args.repo / "apps/api/alembic"
    if (
        args.write_upgrade_metadata
        and script_location.resolve() != DEFAULT_SCRIPT_LOCATION.resolve()
    ):
        parser.error("--write-upgrade-metadata requires the authoritative API migration tree")
    versions = script_location / "versions"

    if not script_location.is_dir():
        print(
            f"Alembic revision gate failed: script location does not exist: "
            f"{script_location}",
            file=sys.stderr,
        )
        return 1

    if not versions.is_dir():
        print(
            f"Alembic revision gate failed: versions directory does not exist: "
            f"{versions}",
            file=sys.stderr,
        )
        return 1

    filenames_by_token: dict[str, list[str]] = defaultdict(list)
    unrecognized_filenames: list[str] = []
    try:
        for path in versions.iterdir():
            if (
                not path.is_file()
                or path.suffix != ".py"
                or path.name == "__init__.py"
            ):
                continue
            match = FILENAME_PATTERN.fullmatch(path.name)
            if match is None:
                unrecognized_filenames.append(path.name)
                continue
            filenames_by_token[match.group(1)].append(path.name)
    except OSError as exc:
        print(
            f"Alembic revision gate failed: could not scan versions directory "
            f"{versions}: {exc}",
            file=sys.stderr,
        )
        return 1

    if unrecognized_filenames:
        print(
            "Alembic revision gate failed: unrecognized migration filenames "
            "found:",
            file=sys.stderr,
        )
        for filename in sorted(unrecognized_filenames):
            print(f"  {filename}", file=sys.stderr)
        print(
            "Name every migration <digits><optional lowercase letter>_"
            "<description>.py.",
            file=sys.stderr,
        )
        return 1

    duplicates = {
        token: sorted(filenames)
        for token, filenames in filenames_by_token.items()
        if len(filenames) > 1
    }
    if duplicates:
        print(
            "Alembic revision gate failed: duplicate numeric revision or "
            "suffixed revision filename tokens found:",
            file=sys.stderr,
        )
        for token in sorted(duplicates):
            print(
                f"  {token}: {', '.join(duplicates[token])}",
                file=sys.stderr,
            )
        print(
            "Rename migrations so every leading revision token is unique.",
            file=sys.stderr,
        )
        return 1

    filenames_by_revision: dict[str, list[str]] = defaultdict(list)
    for filenames in filenames_by_token.values():
        for filename in filenames:
            revision_id = _revision_id(versions / filename)
            if revision_id is not None:
                filenames_by_revision[revision_id].append(filename)

    duplicate_revisions = {
        revision_id: sorted(filenames)
        for revision_id, filenames in filenames_by_revision.items()
        if len(filenames) > 1
    }
    if duplicate_revisions:
        print(
            "Alembic revision gate failed: duplicate revision ids found:",
            file=sys.stderr,
        )
        for revision_id in sorted(duplicate_revisions):
            print(
                f"  {revision_id}: "
                f"{', '.join(duplicate_revisions[revision_id])}",
                file=sys.stderr,
            )
        print(
            "Give every migration a unique revision id, then repoint the "
            "down_revision of whatever followed it.",
            file=sys.stderr,
        )
        return 1

    shared_revision_count = None
    if args.other_train_ref is not None:
        try:
            shared_revision_count = _cross_train_shared_count(
                versions, args.repo, args.other_train_ref
            )
        except (OSError, SyntaxError, ValueError, subprocess.CalledProcessError) as exc:
            detail = (
                exc.stderr.strip()
                if isinstance(exc, subprocess.CalledProcessError)
                else str(exc)
            )
            print(
                f"Alembic cross-train lineage failed against {args.other_train_ref}: {detail}",
                file=sys.stderr,
            )
            return 1

    try:
        heads = sorted(ScriptDirectory(str(script_location)).get_heads())
    except Exception as exc:
        print(
            f"Alembic revision gate failed: could not load the revision graph "
            f"from {script_location}: {exc}",
            file=sys.stderr,
        )
        print(
            "Fix malformed revision modules and graph references, then rerun "
            "the checker.",
            file=sys.stderr,
        )
        return 1

    if len(heads) != 1:
        rendered_heads = ", ".join(heads) if heads else "none"
        print(
            f"Alembic revision gate failed: expected exactly one Alembic head, "
            f"found {len(heads)}: {rendered_heads}",
            file=sys.stderr,
        )
        print(
            "Create a merge revision so the migration tree has one head.",
            file=sys.stderr,
        )
        return 1

    if script_location.resolve() == DEFAULT_SCRIPT_LOCATION.resolve():
        try:
            kinds_payload = json.loads(DEFAULT_KINDS_FILE.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(
                f"Alembic revision gate failed: could not read {DEFAULT_KINDS_FILE}: {exc}",
                file=sys.stderr,
            )
            return 1
        graph_ids = {
            rev.revision
            for rev in ScriptDirectory(str(script_location)).walk_revisions()
        }
        declared = set(kinds_payload)
        missing = sorted(graph_ids - declared)
        extra = sorted(declared - graph_ids)
        invalid = sorted(
            f"{rev}={kind}"
            for rev, kind in kinds_payload.items()
            if kind not in _VALID_KINDS
        )
        if missing or extra or invalid:
            print(
                "Alembic revision gate failed: revision_kinds.json must classify "
                "every revision as expand, contract, or irreversible (#2300).",
                file=sys.stderr,
            )
            if missing:
                print(f"  missing kinds: {', '.join(missing)}", file=sys.stderr)
            if extra:
                print(f"  extra kinds: {', '.join(extra)}", file=sys.stderr)
            if invalid:
                print(f"  invalid kinds: {', '.join(invalid)}", file=sys.stderr)
            return 1

        # @spec PROTECTED-HOOK-SOURCE-2: installed serving metadata owns its mirror.
        try:
            window = json.loads(DEFAULT_CATALOG_FILE.read_text(encoding="utf-8"))["candidate"]
            if window["schema_head"] != heads[0] or window["schema_min"] not in graph_ids:
                raise ValueError("candidate compatibility window does not name the current graph")
            revisions = []
            for revision in ScriptDirectory(str(script_location)).walk_revisions():
                parent = revision.down_revision
                parents = (
                    list(parent)
                    if isinstance(parent, (list, tuple))
                    else [parent] if parent else []
                )
                revisions.append(
                    {
                        "revision": revision.revision,
                        "parents": parents,
                        "kind": kinds_payload[revision.revision],
                        "sha256": hashlib.sha256(Path(revision.path).read_bytes()).hexdigest(),
                    }
                )
            serving = {
                "schema_min": window["schema_min"],
                "schema_head": window["schema_head"],
                "revision_parents": {
                    row["revision"]: row["parents"] for row in revisions
                },
            }
            serving_text = json.dumps(serving, indent=2, sort_keys=True) + "\n"
            window_text = json.dumps(window, indent=2, sort_keys=True) + "\n"
            if args.write_upgrade_metadata:
                DEFAULT_SERVING_FILE.parent.mkdir(parents=True, exist_ok=True)
                DEFAULT_SERVING_FILE.write_text(serving_text, encoding="utf-8")
                DEFAULT_WINDOW_FILE.write_text(window_text, encoding="utf-8")
            if DEFAULT_SERVING_FILE.read_text(encoding="utf-8") != serving_text:
                raise ValueError("shared serving metadata differs from candidate or actual graph")
            if json.loads(DEFAULT_WINDOW_FILE.read_text(encoding="utf-8")) != window:
                raise ValueError("API window mirror differs from shared serving metadata")
            metadata = {
                "schema_min": window["schema_min"],
                "schema_head": window["schema_head"],
                "revisions": sorted(revisions, key=lambda revision: revision["revision"]),
            }
            expected = json.dumps(metadata, indent=2, sort_keys=True) + "\n"
            if args.write_upgrade_metadata:
                DEFAULT_CHART_METADATA.parent.mkdir(parents=True, exist_ok=True)
                DEFAULT_CHART_METADATA.write_text(expected, encoding="utf-8")
            if DEFAULT_CHART_METADATA.read_text(encoding="utf-8") != expected:
                raise ValueError("packaged copy differs from the API window or revision graph")
        except (OSError, ValueError, KeyError) as error:
            print(
                f"Alembic revision gate failed: schema compatibility metadata: {error}. "
                "Run uv run python scripts/check-alembic-revisions.py --write-upgrade-metadata.",
                file=sys.stderr,
            )
            return 1

    if shared_revision_count is not None:
        print(
            f"Alembic cross-train lineage passed: {shared_revision_count} shared "
            f"revision ids match {args.other_train_ref}."
        )
    print(f"Alembic revision gate passed with head {heads[0]}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

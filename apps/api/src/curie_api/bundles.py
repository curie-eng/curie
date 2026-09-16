"""Plugin bundle intake: detect archive format, extract safely, validate.

The upload path is: bytes -> detect zip/tar(.gz) -> extract into a temp dir
(guarding against path traversal) -> locate the bundle root -> validate via the
frozen ``plugin_format.validate_bundle``. Storage and DB wiring live in the
router; this module is pure intake logic.
"""

import io
import json
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from plugin_format import (
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBERS,
    DEFAULT_MAX_UNCOMPRESSED_BYTES,
    MANIFEST_LOCATIONS,
    TOOL_POLICY_ENFORCEMENT,
    ApprovalPolicy,
    PluginManifest,
    UnsupportedArchive,
    ValidationResult,
    bundle_root,
    connector_render,
    resolve_manifest,
    safe_extract,
    validate_bundle,
)
from plugin_format.connector_lock import (
    CONNECTOR_LOCK_FILE,
    ConnectorLockFile,
    validate_connector_lock,
)
from plugin_format.connectors import CONNECTORS_FILE, ConnectorsFile, validate_connectors
from plugin_format.deploy_targets import DeployTargetsFile, validate_deploy_targets
from plugin_format.validate import DEPLOY_FILE
from plugin_format.yaml_loader import safe_load_unique

# Re-exported so existing catchers (gitflow.py, routers/bundles.py, tests) keep
# resolving ``bundles.UnsupportedArchive`` after the extraction logic moved to
# plugin_format; safe_extract raises this single error for unsafe/unrecognized
# archives.
__all__ = [
    "UnsupportedArchive",
    "bundle_root",
    "declared_approval_routes",
    "detect_format",
    "extract_and_validate",
    "extract_stored_bundle",
    "read_bundle_text_files",
]

# Detected format -> (stored key extension, content type). Detection sniffs the
# bytes rather than trusting the upload filename.
_ZIP = (".zip", "application/zip")
_TAR_GZ = (".tar.gz", "application/gzip")
_TAR = (".tar", "application/x-tar")


def detect_format(data: bytes) -> tuple[str, str]:
    """Return (extension, content_type) for the archive, sniffing the bytes."""

    if zipfile.is_zipfile(io.BytesIO(data)):
        return _ZIP
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz"):
            return _TAR_GZ
    except tarfile.TarError:
        pass
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:"):
            return _TAR
    except tarfile.TarError:
        pass
    raise UnsupportedArchive("upload is not a zip or tar(.gz) archive")


def extract_and_validate(
    data: bytes,
    dest: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
    max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    max_members: int = DEFAULT_MAX_MEMBERS,
) -> tuple[str, str, ValidationResult]:
    """Detect, extract, and validate. Returns (extension, content_type, result).

    Extraction (with the traversal/symlink/special-file, size/ratio, and
    member-count guards) and the single-wrapper-dir unwrap live in
    ``plugin_format``; this only adds the storage-key/content-type detection the
    upload path needs. The caps default to ``plugin_format``'s generous
    fallbacks; ``deploy.py`` passes the operator-configured ``Settings`` values
    instead.
    """

    extension, content_type = detect_format(data)
    extract_stored_bundle(
        data,
        dest,
        max_uncompressed_bytes=max_uncompressed_bytes,
        max_compression_ratio=max_compression_ratio,
        max_members=max_members,
    )
    # Ingestion states the PLATFORM's contract, not this process's: the API does
    # not run tool calls, the runner does, and the runner in this release applies
    # a declared `toolPolicy` at both of its interception points. Refusing here
    # instead would mean no policy-bearing bundle could ever be stored, which
    # makes the extension undeployable rather than safe.
    #
    # Safe under skew, and only in one direction: a runner without the
    # classification code passes no enforcement id to its own `validate_bundle`,
    # so it refuses to BOOT a policy-bearing bundle. The failure mode of an old
    # runner meeting a new bundle is "will not start", never "starts unfenced".
    result = validate_bundle(
        bundle_root(dest), enforces_tool_policy=TOOL_POLICY_ENFORCEMENT
    )
    return extension, content_type, result


def extract_stored_bundle(
    data: bytes,
    dest: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
    max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    max_members: int = DEFAULT_MAX_MEMBERS,
) -> None:
    """Extract bytes that already passed ``validate_bundle`` at store time.

    Bounded extraction under the caller's caps, and nothing else: no
    ``detect_format``, no ``validate_bundle``. Re-validation is not so much
    skipped here as already done -- the storage key is immutable and write-once
    (see ``storage.ObjectStore``), so the object cannot have changed since it
    was validated on the delivery that stored it. The part that CAN have moved
    since is the operator's caps, and ``safe_extract`` re-applies them here:
    the same backward-compatibility commitment ``deploy.revalidate_stored_bundle``
    makes (ADR-0059 decision 3).

    Raises ``UnsupportedArchive`` when a cap is exceeded. Mapping that onto the
    caller's own error contract is the caller's job -- ``gitflow.process_push``
    re-raises it as ``deploy.BundleTooLarge`` so an over-cap legacy bundle keeps
    reporting ``bundle.too_large`` rather than the vaguer ``bundle.unsupported``.

    Named sibling of ``read_bundle_text_files``, which does the same
    bounded-extract-without-validate for the bundle-files read endpoint, and the
    shared bounded-extract primitive ``extract_and_validate`` itself calls, so
    the caps argument list has one home rather than two copies. The "stored" in
    the name describes the caller whose contract this docstring records, not a
    property this function re-checks.
    """

    safe_extract(
        data,
        dest,
        max_uncompressed_bytes=max_uncompressed_bytes,
        max_compression_ratio=max_compression_ratio,
        max_members=max_members,
    )


def declared_approval_routes(root: Path) -> set[str] | None:
    """The approval routes a bundle DECLARES, or ``None`` when it declares poison.

    The deploy-time half of the declared/bound approval-route join (#2436). The
    runtime counterpart is
    ``runner/src/curie_runner/approval.py::resolve_approval_policy``, and this
    reader reproduces it step for step. Code cannot be shared across that seam --
    ``packages/plugin-format`` is a frozen interface and ``curie_api`` must not
    import ``curie_runner`` -- so the rule is frozen in
    ``tests/vectors/approval-route-normalization.json`` and EXECUTED from both
    sides. Drift here is the #453/#544 fail-open shape: a bundle passes the
    configuration gate and then boots with a different set of gates.

    The declared set is ``set(route_by_tool.values())`` of the loader's LAST-WINS
    ``{stripped gate: stripped route}`` map, deliberately NOT the union of every
    gate's route value. Two gates naming one tool are a last-wins duplicate
    ``validate_bundle`` accepts and the runner boots, and the earlier route can
    never be raised at runtime -- so unioning would refuse a deploy over a route
    that does not exist.

    ``None`` is the poison value, returned on exactly the condition the runner
    raises ``ApprovalPolicyError`` and refuses to boot: a declared gate name that
    arms no tool, which is what a blank stripped ``gate`` or a blank stripped
    ``route`` produces. An unreadable manifest or an ``approvalPolicy`` that does
    not parse is poison too, for the same reason ``resolve_approval_policy``
    treats it as one -- once a policy is declared, a parse error cannot revoke
    the intent, and reading it as "declares nothing" would accept, at
    configuration time, a bundle the runner will not run. The caller turns
    ``None`` into a refusal (``deploy.check_routes_from_bytes``).

    An empty set is reserved for the honest cases AC4 rests on: no manifest, no
    ``approvalPolicy``, or an explicitly empty ``gates`` list.
    """

    manifest_path = resolve_manifest(bundle_root(root))
    if manifest_path is None:
        # `validate_bundle` already refuses a manifestless bundle at every
        # storage entry point, so this is the defensive branch, not a real one.
        return set()
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        # A manifest that will not parse cannot prove it declares no policy.
        return None
    if not isinstance(raw, dict) or raw.get("approvalPolicy") is None:
        return set()
    try:
        manifest = PluginManifest.model_validate(raw)
        policy = ApprovalPolicy.model_validate(manifest.approvalPolicy)
    except (ValueError, TypeError):
        return None
    routes = {
        gate.gate.strip(): gate.route.strip()
        for gate in policy.gates
        if gate.gate and gate.gate.strip() and gate.route and gate.route.strip()
    }
    # Compare DISTINCT declared names against armed names, not counts, exactly as
    # the loader does: two entries for one tool are the last-wins duplicate
    # above, while a gate whose name survives here but armed nothing is the
    # partially armed policy the runner refuses to boot.
    declared_names = {gate.gate.strip() for gate in policy.gates if isinstance(gate.gate, str)}
    if declared_names - set(routes):
        return None
    return set(routes.values())


def read_connectors(root: Path) -> ConnectorsFile:
    """Parse a validated bundle's ``connectors.yaml``, or an empty set.

    Safe to call only after ``validate_bundle`` has passed: every malformed
    shape is already rejected there, so this cannot be the place a bad
    declaration first surfaces.
    """

    path = bundle_root(root) / CONNECTORS_FILE
    if not path.is_file():
        return ConnectorsFile()
    parsed, errors = validate_connectors(safe_load_unique(path.read_text(encoding="utf-8")))
    if errors or parsed is None:  # pragma: no cover -- validate_bundle gates this
        return ConnectorsFile()
    return parsed


def read_connector_lock(root: Path) -> ConnectorLockFile | None:
    """Parse a validated bundle's ``connectors.lock.yaml``, or None (ADR 0113).

    None is not an error and is the common case: an ordinary ``image:`` bundle
    carries no lock and never will. Safe to call only after ``validate_bundle``
    has passed, exactly like ``read_connectors`` -- a malformed lock, a lockless
    ``build:`` bundle, and a stale digest are all already rejected there, so this
    cannot be the place a bad lock first surfaces.
    """

    path = bundle_root(root) / CONNECTOR_LOCK_FILE
    if not path.is_file():
        return None
    parsed, errors = validate_connector_lock(safe_load_unique(path.read_text(encoding="utf-8")))
    if errors or parsed is None:  # pragma: no cover -- validate_bundle gates this
        return None
    return parsed


def read_deploy_targets(root: Path) -> DeployTargetsFile | None:
    """Parse a validated bundle's ``deploy.yaml``, or None when the file is ABSENT.

    None is not an error. A bundle without ``deploy.yaml`` predates ADR-0089
    and still deploys to the single agent its repository binds -- the caller
    must keep working for it, not reject it. A present file that declares no
    targets is a different case: it returns a non null ``DeployTargetsFile``
    with an empty ``targets`` map, not None.
    """

    path = bundle_root(root) / DEPLOY_FILE
    if not path.is_file():
        return None
    parsed, errors = validate_deploy_targets(
        safe_load_unique(path.read_text(encoding="utf-8"))
    )
    if errors or parsed is None:  # pragma: no cover -- validate_bundle gates this
        return None
    return parsed


def render_connector_manifests(
    connectors: ConnectorsFile,
    *,
    release: str,
    agent: str,
    namespace: str,
    app_name: str,
    secret_name: str,
) -> list[dict[str, Any]]:
    """Kubernetes objects for a bundle's hosted connectors (ADR-0086, #1063).

    The API renders but never applies. Rendering is a pure function, so it needs
    no cluster access, and the API's RBAC stays the deliberately read-only
    `pods: list` + `pods/log: get` it has today -- which matters because this is
    the component that receives webhooks from the internet. The CLI applies the
    result with the operator's own kubectl credentials, so cluster-write
    authority stays where it already was.
    """

    objects: list[dict[str, Any]] = []
    for name, spec in sorted(connectors.connectors.items()):
        objects.extend(
            connector_render.render(
                release=release,
                agent=agent,
                namespace=namespace,
                app_name=app_name,
                connector=name,
                spec=spec,
                secret_name=secret_name,
            )
        )
    return objects


def owned_secret_keys(connectors: ConnectorsFile) -> list[str]:
    """Declared secrets whose VALUE the caller must supply (#1163).

    Excludes any that reference a Secret provisioned out of band: resolving
    those would defeat the purpose, and the caller may well not have access to
    them -- which is exactly why the reference form exists (ADR-0090).
    """

    keys: list[str] = []
    for spec in connectors.connectors.values():
        for name in spec.resolved_secrets():
            if name not in keys:
                keys.append(name)
    return sorted(keys)


def connector_mcp_entries(
    connectors: ConnectorsFile, *, release: str, agent: str, namespace: str
) -> dict[str, Any]:
    """The `.mcp.json` entries for declared connectors, keyed by name.

    Derived from the Service the manifests define, so an author never writes a
    URL that resolves in one tier and not another.
    """

    return {
        name: connector_render.mcp_entry(release, agent, namespace, name, spec)
        for name, spec in sorted(connectors.connectors.items())
    }


def _collect_text_files(root: Path) -> list[tuple[str, str]]:
    """The bundle's known text files as (bundle-relative posix path, content).

    Deliberately an allowlist of the bundle's structured text surfaces -- the
    manifest, the skill docs, and the eval cases -- so binaries (and anything
    else) are skipped, not just filtered by a guessed encoding. Paths are
    relative to the bundle root and posix so the UI reads a stable shape.
    """

    candidates: list[Path] = []
    for fixed in (*MANIFEST_LOCATIONS, Path("evals/cases.json")):
        if (root / fixed).is_file():
            candidates.append(root / fixed)
    candidates.extend(p for p in root.glob("skills/**/SKILL.md") if p.is_file())

    files: list[tuple[str, str]] = []
    for path in candidates:
        files.append((path.relative_to(root).as_posix(), path.read_text("utf-8")))
    return sorted(files, key=lambda item: item[0])


def read_bundle_text_files(
    data: bytes,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
    max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    max_members: int = DEFAULT_MAX_MEMBERS,
) -> list[tuple[str, str]]:
    """Extract an archive's bytes and return its known text files.

    Mirrors the upload path (detect -> extract into a temp dir, guarding path
    traversal -> unwrap to the bundle root) but reads the text surfaces instead of
    validating. Returns (path, content) pairs; the caller shapes the response.
    The caps default to ``plugin_format``'s generous fallbacks; the router passes
    the operator-configured ``Settings`` values so this path honors the same
    bounds as ingestion (#815) rather than the library defaults.
    """

    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp)
        safe_extract(
            data,
            dest,
            max_uncompressed_bytes=max_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_members=max_members,
        )
        return _collect_text_files(bundle_root(dest))

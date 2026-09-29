"""``connectors.lock.yaml``: what a declared build resolved to (ADR 0113).

The declaration says where the source is; the lock says which image that source
produced on this install. It is written by ``curie build``, packed into the
bundle like any other file, and read back by the bundle validator, the API and
the CLI -- so the platform holds the exact digest a version deployed, rather
than that fact living only in local CLI state.

    version: 1
    connectors:
      k8s-write:
        image: ghcr.io/acme-corp/sre-bot-k8s-write-mcp@sha256:<64 hex>
        delivery: registry            # or: local-daemon, the dev fast path
        platforms: [linux/amd64, linux/arm64]
        source_digest: sha256:<64 hex>

``apply_lock`` is the single enforcement point for ADR 0113's "the resolved
digest is the only identity rendered into a Deployment or used to start the
local connector": it returns a derived ``ConnectorsFile`` in which every
``build:`` connector has become an ordinary ``image:`` connector, so ``render``,
``mcp_entry``, ``is_hosted`` and the worker's reconcile plan all work unchanged
and there is exactly one place a mutable tag can be refused.

Models and pure functions only. No registry access, no build, no ``docker`` --
that is what lets the API call this and stay a pure renderer under ADR-0087.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field

from .connectors import ConnectorBuild, ConnectorsFile

# The file this module parses. Defined here, with the parser, for the same
# reason CONNECTORS_FILE lives beside `validate_connectors`: the CLI writes it,
# the packer must not exclude it, the validator reads it and the API reads it
# again, and four hand-typed copies of a filename is how one of them ends up
# plural.
CONNECTOR_LOCK_FILE = "connectors.lock.yaml"

# The only shape this reader understands. Refusing an unknown version is what
# lets a future shape change be rejected by an older reader instead of silently
# misread.
LOCK_VERSION = 1

# An image reference is well formed in exactly two shapes and nothing else:
# `<repo>@sha256:<64 lowercase hex>`, the OCI content-addressable manifest
# digest a registry pull resolves
# (https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests),
# and a bare `sha256:<64 lowercase hex>`, the image ID `docker image inspect
# --format {{.Id}}` reports for a locally built image. A tag is refused because
# it can be repointed at a different artifact after review and between tier
# runs, which is the failure this platform has already hit.
_HEX = "[0-9a-f]{64}"


class ConnectorLockEntry(BaseModel):
    """The resolved identity of one built connector.

    Strict for the same reason ``ConnectorSpec`` is: this is Curie's own file
    with no external producer, so an unrecognised key is a mistake, and a
    plausible one -- an operator who writes `digest:` beside `image:` believes
    they pinned something they did not.
    """

    model_config = ConfigDict(extra="forbid")

    # `<repo>@sha256:...` for registry delivery, bare `sha256:...` for
    # local-daemon. Checked in `apply_lock`, not here, so a malformed reference
    # is refused at the one place an image reaches a Deployment.
    image: str
    # Control-bearing: it decides whether the cluster preflight refuses, so an
    # unmodelled value is rejected loudly rather than degraded to a default
    # (the rule ADR-0036 sets for control-bearing enums).
    delivery: Literal["registry", "local-daemon"]
    # What the build targeted, recorded so an operator reading the lock can see
    # whether the artifact covers the nodes they are deploying to.
    platforms: list[str]
    # The content-derived identity of the build INPUT (see `source_digest_of`).
    # Bundle intake recomputes it and refuses a lock that no longer matches.
    source_digest: str


class RunnerLockEntry(BaseModel):
    """The resolved identity of the bundle's runner layer (ADR 0173).

    ``image`` is the built layer and ``base`` the platform runner it was built
    on; both must be digests of ``delivery``'s shape, checked in
    ``resolve_runner_image``.
    """

    model_config = ConfigDict(extra="forbid")

    image: str
    base: str
    delivery: Literal["registry", "local-daemon"]
    platforms: list[str]
    source_digest: str


class ConnectorLockFile(BaseModel):
    """The parsed ``connectors.lock.yaml``."""

    model_config = ConfigDict(extra="forbid")

    version: int
    connectors: dict[str, ConnectorLockEntry] = Field(default_factory=dict)
    # Present exactly when the declaration has a `runner:` block.
    runner: RunnerLockEntry | None = None


def validate_connector_lock(data: Any) -> tuple[ConnectorLockFile | None, list[tuple[str, str]]]:
    """Validate parsed ``connectors.lock.yaml`` content.

    Returns ``(parsed, errors)`` in the same shape ``validate_connectors`` uses,
    so a malformed lock is rejected at bundle intake rather than stored and hit
    at render time, where the failure is an opaque parse error inside the API on
    a version that already looks deployed.
    """

    errors: list[tuple[str, str]] = []
    if not isinstance(data, dict):
        return None, [
            (
                "connectors.lock_invalid",
                f"{CONNECTOR_LOCK_FILE} must be a mapping with `version` and `connectors`",
            )
        ]

    try:
        parsed = ConnectorLockFile.model_validate(data)
    except Exception as exc:  # pydantic ValidationError -- surface it verbatim
        return None, [("connectors.lock_invalid", f"{CONNECTOR_LOCK_FILE}: {str(exc)[:400]}")]

    if parsed.version != LOCK_VERSION:
        errors.append(
            (
                "connectors.lock_unsupported_version",
                f"{CONNECTOR_LOCK_FILE}: version {parsed.version} is not a shape this build "
                f"understands (expected {LOCK_VERSION}). Rebuild the lock with `curie build "
                "--plugin-dir <dir>` rather than reading it with the wrong rules",
            )
        )

    return (parsed if not errors else None), errors


def _image_matches_delivery(entry: ConnectorLockEntry) -> bool:
    """Does the recorded reference match the delivery it claims?

    Delivery and image shape must agree or the two fields are decoration: a bare
    image ID carries no repository, so nothing can pull it, and a repository
    reference means nothing to the local daemon.
    """

    return _reference_matches_delivery(entry.image, entry.delivery)


def _reference_matches_delivery(reference: str, delivery: str) -> bool:
    if delivery == "registry":
        return bool(re.fullmatch(rf"[^@\s]+@sha256:{_HEX}", reference))
    return bool(re.fullmatch(rf"sha256:{_HEX}", reference))


# The build argument a runner Dockerfile takes its base from (ADR 0173).
RUNNER_BASE_ARG = "CURIE_RUNNER_IMAGE"


def resolve_runner_image(
    connectors: ConnectorsFile, lock: ConnectorLockFile | None, *, portable: bool
) -> str | None:
    """The runner layer image a renderer uses, or None for the platform runner.

    The single runner resolver (ADR 0173 decision 2): None when the bundle
    declares no ``runner:``; otherwise exactly the recorded digest. Raises
    ``ValueError`` when the runner is declared but unlocked, when its image or
    base is not a digest of its delivery's shape, or when ``portable`` and the
    layer lives only in a local daemon. ``portable`` is keyword-only for the
    same reason it is on ``apply_lock``.
    """

    if connectors.runner is None:
        return None
    entry = lock.runner if lock is not None else None
    if entry is None:
        raise ValueError(
            f"connectors.yaml declares a runner layer but {CONNECTOR_LOCK_FILE} has no runner "
            "entry for it. Run `curie build --plugin-dir <dir>` to build it and record what it "
            "resolved to."
        )
    for field, reference in (("image", entry.image), ("base", entry.base)):
        if not _reference_matches_delivery(reference, entry.delivery):
            raise ValueError(
                f"runner: {CONNECTOR_LOCK_FILE} records {field} {reference!r} for delivery "
                f"{entry.delivery!r}, which is not a digest of that delivery's shape. A mutable "
                "tag is never rendered: it can be repointed at a different artifact after review."
            )
    if portable and entry.delivery == "local-daemon":
        raise ValueError(
            f"runner: {CONNECTOR_LOCK_FILE} records a local-daemon runner image, which names "
            "nothing a Kubernetes node can pull. Rebuild with `curie build --plugin-dir <dir> "
            "--registry <ref>`."
        )
    return entry.image


def _dockerfile_instructions(text: str) -> list[str]:
    """A Dockerfile's instructions: comments and blanks dropped, ``\\`` joined."""

    out: list[str] = []
    current = ""
    for line in text.splitlines():
        stripped = line.strip()
        if not current and (not stripped or stripped.startswith("#")):
            continue
        if stripped.endswith("\\"):
            current += stripped[:-1] + " "
            continue
        out.append(current + stripped)
        current = ""
    if current.strip():
        out.append(current)
    return out


def check_runner_dockerfile(text: str) -> str | None:
    """Why this runner Dockerfile is refused, or None when its base is the argument.

    The first ``FROM`` must be ``${CURIE_RUNNER_IMAGE}`` (or ``$CURIE_RUNNER_IMAGE``),
    optionally ``AS name``, with ``ARG CURIE_RUNNER_IMAGE`` declared before it:
    ``curie build`` passes the digest-pinned platform runner through that
    argument, so a literal base builds on something the lock does not record.
    Frozen against ``tests/vectors/runner-dockerfile-base.json``; the Rust twin
    is ``connector_build::check_runner_dockerfile``.
    """

    declared = False
    for instruction in _dockerfile_instructions(text):
        words = instruction.split()
        if not words:
            continue
        keyword = words[0].upper()
        if keyword == "ARG":
            declared = declared or any(
                word.split("=", 1)[0] == RUNNER_BASE_ARG for word in words[1:]
            )
            continue
        if keyword != "FROM":
            continue
        rest = [word for word in words[1:] if not word.startswith("--")]
        image = rest[0] if rest else ""
        is_arg = image in {f"${{{RUNNER_BASE_ARG}}}", f"${RUNNER_BASE_ARG}"}
        tail = rest[1:]
        tail_ok = not tail or (len(tail) == 2 and tail[0].upper() == "AS")
        if not is_arg or not tail_ok:
            return (
                f"the runner Dockerfile's first FROM is {instruction.strip()!r}; it must be "
                f"`FROM ${{{RUNNER_BASE_ARG}}}` so `curie build` can build on the digest-pinned "
                f"platform runner it records in {CONNECTOR_LOCK_FILE}"
            )
        if not declared:
            return (
                f"the runner Dockerfile uses ${{{RUNNER_BASE_ARG}}} without declaring it: add "
                f"`ARG {RUNNER_BASE_ARG}` before the first FROM"
            )
        return None
    return (
        f"the runner Dockerfile has no FROM; it must start from `ARG {RUNNER_BASE_ARG}` and "
        f"`FROM ${{{RUNNER_BASE_ARG}}}`"
    )


def apply_lock(
    connectors: ConnectorsFile, lock: ConnectorLockFile | None, *, portable: bool
) -> ConnectorsFile:
    """Resolve every ``build:`` connector to its locked digest.

    ``portable`` is keyword-only because two callers pass different values and
    the wrong one silently deploys an unpullable image to a cluster: the
    Kubernetes manifest route passes ``True``, because every consumer of that
    route applies its result to a cluster. Intake validation passes ``False``,
    because a local-tier version is a legitimate stored artifact until
    something asks to apply it.

    Raises ``ValueError`` rather than returning errors: every condition here
    means the caller is about to render an object that cannot start, so there is
    no partial answer worth returning.
    """

    resolved: dict[str, Any] = {}
    entries = lock.connectors if lock is not None else {}
    for name, spec in connectors.connectors.items():
        if spec.build is None:
            # An `image:` or `url:` connector is carried through byte-identical.
            # Every bundle in the wild is one of these, and `apply_lock` runs on
            # every version the API renders.
            resolved[name] = spec
            continue
        entry = entries.get(name)
        if entry is None:
            raise ValueError(
                f"connector {name!r} declares `build:` but {CONNECTOR_LOCK_FILE} has no entry "
                "for it. Run `curie build --plugin-dir <dir>` to build it and record what it "
                "resolved to."
            )
        if not _image_matches_delivery(entry):
            raise ValueError(
                f"connector {name!r}: {CONNECTOR_LOCK_FILE} records image {entry.image!r} for "
                f"delivery {entry.delivery!r}, which is not a digest of that delivery's shape. "
                "A mutable tag is never rendered into a Deployment: it can be repointed at a "
                "different artifact after review."
            )
        if portable and entry.delivery == "local-daemon":
            raise ValueError(
                f"connector {name!r}: {CONNECTOR_LOCK_FILE} records a local-daemon image, which "
                "names nothing a Kubernetes node can pull -- applying it yields "
                "ImagePullBackOff on every node, long after the deploy reported success. "
                "Rebuild with `curie build --plugin-dir <dir> --registry <ref>`."
            )
        resolved[name] = spec.model_copy(update={"image": entry.image, "build": None})
    return ConnectorsFile(connectors=resolved)


# --------------------------------------------------------------------------- #
# source_digest: the content-derived identity of a build INPUT
#
# The full algorithm, and why each rule exists, is stated in
# tests/vectors/connector-source-digest.json, which freezes it against a corpus
# both this function and the Rust port in cli/src/connector_build.rs are read
# against. It lives here rather than in the CLI so the validator that refuses a
# stale lock and the builder that writes one cannot compute two different values.
#
# The `.dockerignore` half is ORDERED, Docker's own semantics: the LAST rule
# that matches decides, and a `!` rule re-includes. The first version of this
# function returned on the first matching exclusion and had no `!` at all, which
# broke the one property the digest exists to have -- it changes exactly when
# the bytes Docker builds from change -- in both directions: `*.log` plus
# `!keep.log` shipped keep.log to the daemon while hashing nothing of it (a
# stale lock passing review), and `*` alone emptied the digest entirely, so the
# lock could never go stale. That is a change to a frozen cross-language
# algorithm, taken because the old one contradicted its own specification and no
# stored digest depends on it (a lock is regenerated by the next `curie build`).
# --------------------------------------------------------------------------- #
class _IgnoreRule(NamedTuple):
    """One `.dockerignore` line: the pattern, and whether it re-includes."""

    negated: bool
    pattern: str


def _dockerignore_rules(context: Path, dockerfile: str) -> list[_IgnoreRule]:
    """The context's own exclusion set, in file order, or an empty one.

    Docker's file, so honoring it means the digest covers exactly what the
    daemon receives. ``.curieignore`` governs bundle packing, which is a
    different question, and is deliberately not consulted.

    Order is preserved because it is load-bearing: ``*`` followed by
    ``!Dockerfile`` and ``!Dockerfile`` followed by ``*`` are different
    exclusion sets, and the last matching rule is the one that decides.

    The two trailing rules mirror the docker CLI's own
    ``TrimBuildFilesFromExcludes``: the daemon always receives the Dockerfile
    and ``.dockerignore`` even when the patterns exclude them, so a digest that
    dropped them would not move when the file that decides what is built is
    edited.
    """

    path = context / ".dockerignore"
    if not path.is_file():
        return []
    rules: list[_IgnoreRule] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("!"):
            # The `!` is removed and the remainder used verbatim, as Docker
            # does; a bare `!` names nothing and is skipped rather than being
            # read as a re-include of everything.
            remainder = stripped[1:]
            if remainder:
                rules.append(_IgnoreRule(True, remainder))
            continue
        rules.append(_IgnoreRule(False, stripped))
    for build_file in (".dockerignore", dockerfile):
        if _is_excluded(rules, build_file):
            rules.append(_IgnoreRule(True, build_file))
    return rules


def _matches(pattern: str, candidates: list[str]) -> bool:
    """Go ``filepath.Match`` semantics, the matcher Docker's own client uses.

    Matched segment by segment against the path and each of its ancestor
    directory paths, so ``*`` and ``?`` never cross a ``/`` and a pattern naming
    a directory excludes everything beneath it.
    """

    parts = pattern.split("/")
    for candidate in candidates:
        against = candidate.split("/")
        if len(parts) != len(against):
            continue
        if all(fnmatch.fnmatchcase(b, a) for a, b in zip(parts, against, strict=True)):
            return True
    return False


def _is_excluded(rules: list[_IgnoreRule], relative: str) -> bool:
    """Is this path excluded once every rule has had its say, in file order?

    Docker's rule, not "the first exclusion wins": every rule is evaluated and
    the last one that matches decides, so a later ``!`` re-includes what an
    earlier pattern excluded and a later pattern excludes what an earlier ``!``
    re-included.
    """

    segments = relative.split("/")
    candidates = ["/".join(segments[: i + 1]) for i in range(len(segments))]
    excluded = False
    for rule in rules:
        if _matches(rule.pattern, candidates):
            excluded = not rule.negated
    return excluded


def resolve_context(bundle_root: Path, context: str) -> Path:
    """The canonical build context, required to sit inside the bundle.

    The Python twin of ``cli/src/connector_build.rs``'s ``resolve_context``, and
    it must stay its twin: whatever bytes this returns are the bytes
    ``source_digest_of`` hashes, so a resolver that admits a directory the CLI
    refuses lets an author steer the digest with content the bundle does not
    contain -- and one that refuses a directory the CLI admits reports every such
    bundle as unbuildable at intake.

    ``connectors._escapes`` is the textual half and already refuses an absolute,
    empty or ``..``-climbing declaration; only the filesystem can see the rest.
    A symlinked context is REFUSED rather than dereferenced (the rule
    ``cli/src/bundle.rs``'s packer applies, for the same reason), and the
    resolved path must still be inside the resolved bundle root, so a chain of
    links cannot walk out of it. Files INSIDE the context are handled the same
    way by both walks: a symlink is never followed and never hashed.

    Raises ``ValueError`` so the caller can turn it into one validator error
    rather than deciding what a half-resolved path means.
    """

    root = Path(bundle_root).resolve()
    candidate = root / context
    if candidate.is_symlink():
        raise ValueError(
            f"`build.context` is {context!r}, which is a symlink. Curie refuses to follow one "
            "rather than hash and build whatever it points at, which may be nothing this bundle "
            "contains."
        )
    resolved = candidate.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(
            f"`build.context` is {context!r}, which resolves to {resolved} -- outside the bundle. "
            "The build input must be content the bundle carries, or the digest pins bytes no "
            "reviewer of this bundle ever saw."
        )
    return resolved


def source_digest_of(context_dir: Path, build: ConnectorBuild) -> str:
    """Hash a build context and its declaration into one stable identity.

    Content only: no mtime, uid or gid, deliberately unlike
    ``cli/src/bundle.rs``'s archive digest, which embeds all three because it
    identifies an ARCHIVE and not a source tree. Reusing that computation here
    would report every fresh checkout as a changed source.

    Content and ONE bit of mode: whether the file is executable by its owner.
    Docker's build context tar carries each file's mode and BuildKit keys its
    cache on it, so flipping the exec bit on an entrypoint changes the build
    input; a digest blind to it leaves the lock looking fresh while a stale image
    deploys.

    The declared ``build:`` block is hashed alongside the files, so editing
    ``platforms`` or ``dockerfile`` alone still invalidates the lock. The
    generated ``connectors.lock.yaml`` is excluded so that writing the lock does
    not invalidate the digest it records -- but ONLY when the declared context is
    the bundle root, which is the one place `curie build` writes it. Under a
    subdirectory context a file of that name is authored input the daemon
    receives like any other, and so is a file of that name nested deeper under
    any context.
    """

    root = Path(context_dir)
    rules = _dockerignore_rules(root, build.dockerfile)
    # Decided from the DECLARATION, not the tree: `curie build` writes the
    # generated lock at the bundle root only, so it is this context's own
    # generated file exactly when the declared context names that root.
    exclude_lock = build.context.rstrip("/") in {".", ""}
    included: list[str] = []
    for path in root.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root).as_posix()
        if exclude_lock and relative == CONNECTOR_LOCK_FILE:
            continue
        if _is_excluded(rules, relative):
            continue
        included.append(relative)

    stream = bytearray()
    for relative in sorted(included, key=lambda r: r.encode("utf-8")):
        path = root / relative
        content = hashlib.sha256(path.read_bytes()).hexdigest()
        executable = b"1" if path.stat().st_mode & 0o100 else b"0"
        stream += (
            relative.encode("utf-8") + b"\0" + content.encode("ascii") + b"\0" + executable + b"\n"
        )
    canonical = json.dumps(
        {
            "context": build.context,
            "dockerfile": build.dockerfile,
            # Declared order, not sorted: a lane that sorts platforms and one
            # that does not would silently compute two digests for one build.
            "platforms": list(build.platforms),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    stream += b"build\0" + canonical.encode("utf-8") + b"\n"
    return "sha256:" + hashlib.sha256(bytes(stream)).hexdigest()

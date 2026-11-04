"""Shared UI evidence fixtures: the corpus, real git checkouts and PNG bytes.

``tests/vectors/ui-evidence-cases.json`` drives the runner, worker and API
evidence boundaries. These builders turn a case ``fixture`` into a real git
checkout (prepared base P, one committed change, optional uncommitted changes,
gitignored ``.curie/ui-evidence/``) and build PNGs with zlib and struct only.
The candidate tree C is computed with plain git, never by runner code, so every
boundary checks the runner against an independent value.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import struct
import subprocess
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO = "acme-corp/acme-bot"
GITHUB = "https://github.com"
EVIDENCE_DIR = Path(".curie") / "ui-evidence"
IDENTITY = (
    "-c",
    "user.name=Acme Bot",
    "-c",
    "user.email=acme-bot@example.com",
    "-c",
    "commit.gpgsign=false",
)

REPO_ROOT = Path(__file__).resolve().parents[4]
CORPUS_PATH = REPO_ROOT / "tests" / "vectors" / "ui-evidence-cases.json"
CORPUS: dict[str, Any] = json.loads(CORPUS_PATH.read_text())
CASES: dict[str, dict[str, Any]] = {case["id"]: case for case in CORPUS["cases"]}
LIMITS: Mapping[str, int] = CORPUS["limits"]
MAX_IMAGE_BYTES = LIMITS["max_image_bytes"]
MAX_MANIFEST_BYTES = LIMITS["max_manifest_bytes"]
MAX_TOTAL_BYTES = LIMITS["max_total_bytes"]
OIDS: Mapping[str, str] = CORPUS["conventions"]["oids"]

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


# --- PNG builders (zlib/struct only) -------------------------------------------


def png_chunk(kind: bytes, data: bytes, *, crc: int | None = None) -> bytes:
    """One PNG chunk: length, type, data and the CRC32 over type and data."""

    checksum = zlib.crc32(kind + data) & 0xFFFFFFFF if crc is None else crc
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", checksum)


def _ihdr_and_idat(seed: str) -> tuple[bytes, bytes]:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    width, height = 4, 3
    ihdr = png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    rows = b"".join(
        b"\x00" + bytes(digest[(row + column) % 32] for column in range(width * 3))
        for row in range(height)
    )
    return ihdr, png_chunk(b"IDAT", zlib.compress(rows))


def png(seed: str, kind: str = "valid", *, size: int | None = None) -> bytes:
    """A PNG of ``kind`` whose pixels derive from ``seed``.

    ``valid`` is a real image (signature, IHDR, zlib IDAT, IEND, correct CRCs).
    ``size`` pads a valid image with a ``tEXt`` chunk to exactly that many bytes.
    ``oversized`` is a valid image padded to one byte over the image limit.
    """

    ihdr, idat = _ihdr_and_idat(seed)
    iend = png_chunk(b"IEND", b"")
    if kind == "oversized":
        return png(seed, size=MAX_IMAGE_BYTES + 1)
    if kind == "valid":
        if size is None:
            return PNG_SIGNATURE + ihdr + idat + iend
        keyword = b"Comment\x00"
        padding = size - len(PNG_SIGNATURE + ihdr + idat + iend) - 12 - len(keyword)
        assert padding >= 0, size
        text = png_chunk(b"tEXt", keyword + b"x" * padding)
        built = PNG_SIGNATURE + ihdr + text + idat + iend
        assert len(built) == size
        return built
    if kind == "crc":
        good = zlib.crc32(idat[4:-4]) & 0xFFFFFFFF
        broken = png_chunk(b"IDAT", idat[8:-4], crc=good ^ 0x1)
        return PNG_SIGNATURE + ihdr + broken + iend
    if kind == "ihdr_not_first":
        return PNG_SIGNATURE + png_chunk(b"tEXt", b"Comment\x00first") + ihdr + idat + iend
    if kind == "iend_not_last":
        return PNG_SIGNATURE + ihdr + idat + iend + png_chunk(b"tEXt", b"Comment\x00after")
    if kind == "not_png":
        return b"GIF89a\x04\x00\x03\x00\x80\x00\x00" + hashlib.sha256(seed.encode()).digest()
    raise AssertionError(f"unknown png kind {kind!r}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- Checkout builders ---------------------------------------------------------


def git(repo: Path, *args: str, env: Mapping[str, str] | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        env=None if env is None else dict(env),
        check=True,
        capture_output=True,
    )
    return result.stdout.decode("utf-8", errors="replace").strip()


def independent_candidate_tree(repo: Path, scratch: Path) -> str:
    """C from the checkout's final files with plain git, never runner code."""

    scratch.mkdir(parents=True, exist_ok=True)
    index = scratch / f"candidate-{len(list(scratch.iterdir()))}.index"
    env = {**os.environ, "GIT_INDEX_FILE": str(index)}
    git(repo, "read-tree", "HEAD", env=env)
    git(repo, "add", "-A", env=env)
    return git(repo, "write-tree", env=env)


def make_checkout(root: Path, *, dirty: bool) -> tuple[Path, str, str]:
    """A checkout with prepared base P, one committed change on top, and optionally
    uncommitted changes. Returns (repo, P, HEAD)."""

    repo = root / "workspace"
    repo.mkdir(parents=True)
    git(repo, "init", "--quiet")
    git(repo, "remote", "add", "origin", f"{GITHUB}/{REPO}.git")
    (repo / ".gitignore").write_bytes(b".curie/ui-evidence/\n")
    (repo / "README.md").write_bytes(b"acme bot\n")
    (repo / "src").mkdir()
    (repo / "src" / "app.py").write_bytes(b"x = 1\n")
    git(repo, "add", "-A")
    git(repo, *IDENTITY, "commit", "--quiet", "-m", "Prepared base")
    base = git(repo, "rev-parse", "HEAD")
    (repo / "src" / "app.py").write_bytes(b"x = 2\n")
    git(repo, "add", "-A")
    git(repo, *IDENTITY, "commit", "--quiet", "-m", "Agent change")
    head = git(repo, "rev-parse", "HEAD")
    if dirty:
        (repo / "README.md").write_bytes(b"acme bot, edited\n")
        (repo / "src" / "feature.py").write_bytes(b"feature = True\n")
    return repo, base, head


@dataclass
class CaseCheckout:
    repo: Path
    base: str
    head: str
    head_tree: str
    candidate_tree: str
    originals: dict[str, bytes]

    @property
    def evidence(self) -> Path:
        return self.repo / EVIDENCE_DIR

    def values(self) -> dict[str, str]:
        return {
            "$P": self.base,
            "$HEAD": self.head,
            "$HEAD_TREE": self.head_tree,
            "$C": self.candidate_tree,
            "$OTHER_BASE": OIDS["other_base"],
            "$OTHER_TREE": OIDS["other_tree"],
        }


def _write_evidence_file(evidence: Path, path: str, spec: Mapping[str, Any]) -> bytes | None:
    target = evidence / path
    target.parent.mkdir(parents=True, exist_ok=True)
    if "png" in spec:
        data = png(path, spec["png"], size=spec.get("size"))
        target.write_bytes(data)
        return data
    if "symlink" in spec:
        os.symlink(spec["symlink"], target)
        return None
    if "dir_symlink" in spec:
        os.symlink(spec["dir_symlink"], target, target_is_directory=True)
        return None
    raise AssertionError(f"unknown file spec {spec!r}")


def build_case_checkout(root: Path, case: Mapping[str, Any]) -> CaseCheckout:
    """The real checkout a corpus case's ``fixture`` describes."""

    fixture = case["fixture"]
    dirty = fixture["checkout"] == "dirty"
    repo, base, head = make_checkout(root, dirty=dirty)
    head_tree = git(repo, "rev-parse", f"{head}^{{tree}}")
    candidate = independent_candidate_tree(repo, root / "candidate-index")
    # The fixture's own premise: a clean checkout's C is HEAD's tree, a dirty one's is not.
    assert (candidate == head_tree) is (not dirty)

    evidence = repo / EVIDENCE_DIR
    evidence.mkdir(parents=True)
    originals: dict[str, bytes] = {}
    for path, spec in fixture["files"].items():
        data = _write_evidence_file(evidence, path, spec)
        if data is not None:
            originals[path] = data

    raw = fixture.get("manifest_raw")
    manifest = fixture.get("manifest")
    if isinstance(raw, str):
        (evidence / "manifest.json").write_text(raw)
    elif manifest is not None:
        document = copy.deepcopy(manifest)
        selectors = {
            "base_sha": {"P": base, "other": OIDS["other_base"]}.get(fixture.get("base")),
            "head_sha": {"HEAD": head}.get(fixture.get("head")),
            "candidate_dirty": fixture.get("candidate_dirty"),
            "candidate_tree": {"C": candidate, "other": OIDS["other_tree"]}.get(
                fixture.get("tree")
            ),
        }
        for key, value in selectors.items():
            if value is not None:
                document[key] = value
        (evidence / "manifest.json").write_text(json.dumps(document))

    # Evidence is gitignored, so writing it never moves C.
    assert independent_candidate_tree(repo, root / "candidate-index") == candidate
    return CaseCheckout(repo, base, head, head_tree, candidate, originals)


def resolve(value: Any, checkout: CaseCheckout) -> Any:
    """A corpus expectation with every placeholder replaced by this checkout's value."""

    values = checkout.values()

    def walk(node: Any) -> Any:
        if isinstance(node, str) and node in values:
            return values[node]
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, dict):
            resolved = {key: walk(item) for key, item in node.items()}
            if "path" in node and "ordinal" in node:
                data = checkout.originals.get(str(node["path"]))
                if resolved.get("size") == "$size":
                    assert data is not None, node
                    resolved["size"] = len(data)
                if resolved.get("sha256") == "$sha256":
                    assert data is not None, node
                    resolved["sha256"] = sha256_hex(data)
            return resolved
        return node

    return walk(value)

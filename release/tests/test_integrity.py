"""Contract tests for the release integrity gate (release/integrity.py).

The gate is the deterministic check behind issue #629: a release must not publish
an asset that lacks a checksum, a signature, provenance, or an SBOM. These tests
drive the two entry points the release workflow calls -- `manifest` (build
checksums.txt, refusing to build one over an incomplete dist) and `verify`
(re-check a published dist) -- against synthetic dist directories.

The gate is deliberately CLOSED-WORLD: it is not enough that the assets we know
about today are covered. Any *unrecognized* file that lands in dist must also be
covered, so adding a new release asset without its SBOM fails the release instead
of shipping an unattested artifact.
"""

import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "release" / "integrity.py"

VERSION = "1.2.3"


def load_module():
    """Import the standalone script by path (release/ is not on sys.path)."""
    spec = importlib.util.spec_from_file_location("release_integrity", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


integrity = load_module()

# Read from integrity.py rather than restated. A second copy drifts silently:
# the release that failed on an undeclared arm64 binary had the workflow matrix
# and integrity.py disagreeing, and a test with its own third list could not
# have noticed.
BINARIES = integrity.BINARY_ASSETS
CHART = f"curie-{VERSION}.tgz"
COMPOSE = "compose.release.yaml"
ASSETS = (*BINARIES, CHART, COMPOSE)


def write_sbom(path: Path, subject: str) -> None:
    """Write a minimal but structurally valid SPDX document for `subject`."""
    path.write_text(
        json.dumps(
            {
                "spdxVersion": "SPDX-2.3",
                "name": subject,
                "SPDXID": "SPDXRef-DOCUMENT",
                "packages": [{"name": "example-dep", "SPDXID": "SPDXRef-Package-0"}],
            }
        )
    )


def make_dist(tmp_path: Path, assets=ASSETS, sboms=None) -> Path:
    """Build a dist dir holding `assets` plus an SBOM for each of `sboms`."""
    dist = tmp_path / "dist"
    dist.mkdir(exist_ok=True)
    for name in assets:
        (dist / name).write_bytes(f"contents of {name}".encode())
    for name in ASSETS if sboms is None else sboms:
        write_sbom(dist / f"{name}.spdx.json", name)
    return dist


def sign(dist: Path) -> None:
    """Stand in for the cosign keyless bundle the workflow writes next to the manifest."""
    (dist / "checksums.txt.sigstore.json").write_text(json.dumps({"mediaType": "example"}))


def seal(dist: Path, version: str = VERSION) -> Path:
    """Complete, signed dist: the state `verify` must accept."""
    (dist / "checksums.txt").write_text(integrity.build_manifest(dist, version))
    sign(dist)
    return dist


class TestBuildManifest:
    def test_covers_every_file_including_the_sboms(self, tmp_path):
        dist = make_dist(tmp_path)

        manifest = integrity.build_manifest(dist, VERSION)

        listed = {line.split("  ", 1)[1] for line in manifest.splitlines()}
        assert listed == {p.name for p in dist.iterdir()}
        # The SBOMs are assets in their own right: an unsigned SBOM is a tampering
        # surface, so they must be inside the signed manifest too.
        assert f"{CHART}.spdx.json" in listed

    def test_lines_are_sha256sum_check_compatible(self, tmp_path):
        dist = make_dist(tmp_path)

        (dist / "checksums.txt").write_text(integrity.build_manifest(dist, VERSION))

        # The documented verification command is `sha256sum --check checksums.txt`;
        # if our format drifts from coreutils', the docs lie.
        done = subprocess.run(
            ["sha256sum", "--check", "--strict", "checksums.txt"],
            cwd=dist,
            capture_output=True,
            text=True,
        )
        assert done.returncode == 0, done.stdout + done.stderr

    def test_hashes_are_the_real_digests(self, tmp_path):
        dist = make_dist(tmp_path)

        manifest = integrity.build_manifest(dist, VERSION)

        expected = hashlib.sha256((dist / COMPOSE).read_bytes()).hexdigest()
        assert f"{expected}  {COMPOSE}" in manifest.splitlines()

    def test_is_deterministic_and_sorted(self, tmp_path):
        dist = make_dist(tmp_path)

        manifest = integrity.build_manifest(dist, VERSION)

        names = [line.split("  ", 1)[1] for line in manifest.splitlines()]
        assert names == sorted(names)
        assert manifest == integrity.build_manifest(dist, VERSION)

    @pytest.mark.parametrize("missing", ASSETS)
    def test_refuses_to_build_over_a_missing_required_asset(self, tmp_path, missing):
        dist = make_dist(tmp_path, assets=[a for a in ASSETS if a != missing])

        with pytest.raises(integrity.IntegrityError, match=missing):
            integrity.build_manifest(dist, VERSION)

    def test_rejects_a_chart_packaged_at_the_wrong_version(self, tmp_path):
        dist = make_dist(tmp_path, assets=(*BINARIES, "curie-9.9.9.tgz", COMPOSE))

        # A stale chart tgz would otherwise sail through: it matches curie-*.tgz.
        with pytest.raises(integrity.IntegrityError, match=CHART):
            integrity.build_manifest(dist, VERSION)

    @pytest.mark.parametrize("uncovered", ASSETS)
    def test_refuses_when_a_known_asset_has_no_sbom(self, tmp_path, uncovered):
        dist = make_dist(tmp_path, sboms=[a for a in ASSETS if a != uncovered])

        with pytest.raises(integrity.IntegrityError, match="SBOM"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_undeclared_asset(self, tmp_path):
        # The closed-world clause: a NEW asset someone adds to the release must be
        # declared here too, or the gate silently stops meaning anything.
        dist = make_dist(tmp_path)
        (dist / "curie-installer.sh").write_bytes(b"#!/bin/sh\n")

        with pytest.raises(integrity.IntegrityError, match="curie-installer.sh"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_undeclared_asset_even_when_it_brings_an_sbom(self, tmp_path):
        # `files: dist/*` publishes whatever is staged, so a stray file must not be
        # able to buy its way into a signed release just by carrying an SBOM.
        dist = make_dist(tmp_path)
        (dist / "curie-backdoor").write_bytes(b"pwned")
        write_sbom(dist / "curie-backdoor.spdx.json", "curie-backdoor")

        with pytest.raises(integrity.IntegrityError, match="not declared"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_empty_sbom(self, tmp_path):
        dist = make_dist(tmp_path)
        (dist / f"{COMPOSE}.spdx.json").write_text("")

        with pytest.raises(integrity.IntegrityError, match="SBOM"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_sbom_that_is_not_valid_json(self, tmp_path):
        dist = make_dist(tmp_path)
        (dist / f"{CHART}.spdx.json").write_text("<html>404: not found</html>")

        with pytest.raises(integrity.IntegrityError, match="SBOM"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_sbom_that_is_valid_json_but_not_an_sbom(self, tmp_path):
        # `{}` parses. A generator that silently degrades to an empty document
        # would otherwise satisfy the gate while inventorying nothing.
        dist = make_dist(tmp_path)
        (dist / f"{CHART}.spdx.json").write_text("{}")

        with pytest.raises(integrity.IntegrityError, match="SPDX"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_sbom_that_inventories_nothing(self, tmp_path):
        dist = make_dist(tmp_path)
        (dist / f"{CHART}.spdx.json").write_text(
            json.dumps({"spdxVersion": "SPDX-2.3", "packages": []})
        )

        with pytest.raises(integrity.IntegrityError, match="no packages"):
            integrity.build_manifest(dist, VERSION)

    def test_refuses_an_sbom_with_no_asset(self, tmp_path):
        # The closed-world rule is "an SBOM *for one*": an orphan SBOM means either
        # an asset went missing or the SBOM is misnamed. Both are release bugs.
        dist = make_dist(tmp_path)
        write_sbom(dist / "ghost-asset.spdx.json", "ghost-asset")

        with pytest.raises(integrity.IntegrityError, match="ghost-asset"):
            integrity.build_manifest(dist, VERSION)


class TestVerify:
    def test_accepts_a_complete_signed_dist(self, tmp_path):
        dist = seal(make_dist(tmp_path))

        integrity.verify(dist, VERSION)  # does not raise

    def test_rejects_a_missing_checksum_manifest(self, tmp_path):
        dist = seal(make_dist(tmp_path))
        (dist / "checksums.txt").unlink()

        with pytest.raises(integrity.IntegrityError, match="checksums.txt"):
            integrity.verify(dist, VERSION)

    def test_rejects_a_missing_signature(self, tmp_path):
        dist = seal(make_dist(tmp_path))
        (dist / "checksums.txt.sigstore.json").unlink()

        with pytest.raises(integrity.IntegrityError, match="sigstore"):
            integrity.verify(dist, VERSION)

    def test_rejects_a_tampered_asset(self, tmp_path):
        dist = seal(make_dist(tmp_path))
        (dist / COMPOSE).write_bytes(b"image: evil/backdoor:latest")

        with pytest.raises(integrity.IntegrityError, match="sha256 mismatch"):
            integrity.verify(dist, VERSION)

    def test_rejects_a_file_the_manifest_does_not_list(self, tmp_path):
        # A declared asset published outside the signature: the manifest was built
        # over a smaller dist than the one shipped, so the signature says nothing
        # about this file.
        dist = seal(make_dist(tmp_path))
        manifest = dist / "checksums.txt"
        kept = [ln for ln in manifest.read_text().splitlines() if not ln.endswith(f"  {COMPOSE}")]
        manifest.write_text("".join(f"{ln}\n" for ln in kept))

        with pytest.raises(integrity.IntegrityError, match="not listed"):
            integrity.verify(dist, VERSION)

    def test_rejects_an_asset_listed_but_not_delivered(self, tmp_path):
        dist = seal(make_dist(tmp_path))
        (dist / BINARIES[1]).unlink()

        with pytest.raises(integrity.IntegrityError, match=BINARIES[1]):
            integrity.verify(dist, VERSION)


class TestCli:
    def run(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True)

    def test_manifest_writes_the_file_and_exits_zero(self, tmp_path):
        dist = make_dist(tmp_path)

        done = self.run("manifest", "--dist", str(dist), "--version", VERSION)

        assert done.returncode == 0, done.stderr
        assert (dist / "checksums.txt").read_text() == integrity.build_manifest(dist, VERSION)

    def test_manifest_exits_nonzero_and_writes_nothing_when_incomplete(self, tmp_path):
        dist = make_dist(tmp_path, assets=BINARIES)

        done = self.run("manifest", "--dist", str(dist), "--version", VERSION)

        assert done.returncode == 1
        assert COMPOSE in done.stderr
        # A half-written manifest is worse than none: it would be signed as truth.
        assert not (dist / "checksums.txt").exists()

    def test_verify_exits_zero_on_a_complete_dist(self, tmp_path):
        dist = seal(make_dist(tmp_path))

        done = self.run("verify", "--dist", str(dist), "--version", VERSION)

        assert done.returncode == 0, done.stderr

    def test_verify_exits_nonzero_on_a_missing_integrity_artifact(self, tmp_path):
        dist = seal(make_dist(tmp_path))
        (dist / f"{BINARIES[0]}.spdx.json").unlink()

        done = self.run("verify", "--dist", str(dist), "--version", VERSION)

        assert done.returncode == 1
        assert "SBOM" in done.stderr


def test_declared_binaries_match_the_workflow_matrix() -> None:
    """The declaration and the build matrix cannot drift apart.

    v0.6.0's first release build failed at the very last job -- after every
    image, binary and SBOM had been built -- with:

        ERROR: file(s) staged for release but not declared in integrity.py:
        curie-aarch64-unknown-linux-gnu

    The arm64 Linux target had been added to the workflow matrix and not to
    `BINARY_ASSETS`. A comment said "keep in lockstep with its matrix", and a
    comment is not a check: nothing compared the two until a tagged release
    tried to publish, which is the most expensive possible moment to find out.

    This reads both and fails in milliseconds instead.
    """

    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yaml").read_text()
    # The matrix entries, e.g. "- target: aarch64-unknown-linux-gnu". Anchored
    # to the list-item form so a `target:` appearing in prose or another job
    # does not count as a build target.
    matrix = {
        f"curie-{m}" for m in re.findall(r"^\s*-\s*target:\s*([A-Za-z0-9_.-]+)\s*$", workflow, re.M)
    }
    assert matrix, "found no build targets in release.yaml; the pattern has drifted"

    declared = set(integrity.BINARY_ASSETS)
    assert declared == matrix, (
        "release.yaml's matrix and integrity.py's BINARY_ASSETS disagree.\n"
        f"  built but not declared: {sorted(matrix - declared)}\n"
        f"  declared but not built: {sorted(declared - matrix)}\n"
        "A target added to one and not the other fails the release at its last "
        "job. Add it to BINARY_ASSETS (and give it an SBOM) or drop it from the "
        "matrix."
    )

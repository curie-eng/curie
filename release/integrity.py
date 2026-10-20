#!/usr/bin/env python3
"""Gate the integrity artifacts of a release dist directory (issue #629).

A GitHub release publishes CLI binaries, a Helm chart, and a compose file. A user
who downloads one has no way to tell what they received unless every asset is
covered by a checksum, a signature over that checksum manifest, provenance tied to
the release commit, and an SBOM. This script is the deterministic check that the
coverage is actually complete, and it is the only place that decides what
"complete" means. `.github/workflows/release.yaml` calls it twice:

  manifest  Before publishing: refuse to build checksums.txt over an incomplete
            dist, then write it. Signing an incomplete manifest would launder a
            gap into an attestation, so the gate runs BEFORE cosign, not after.

  verify    After publishing: re-download the release and re-check it, so the
            published bytes -- not the ones we hoped we uploaded -- are what the
            gate passed. Signature and provenance verification (cosign
            verify-blob, gh attestation verify) run alongside it in the workflow;
            those need the network and a trust root, so they stay there.

The check is CLOSED-WORLD by design. Requiring only the assets we know about today
would mean the next asset someone adds to the release ships uncovered while the
gate still reports green. So every file in dist must be accounted for: a
recognized asset, an SBOM for one, or the manifest and its signature. Anything
else fails.

Fails loud: this runs unattended at publish time, where a silent pass ships an
unverifiable artifact.
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

CHECKSUM_FILE = "checksums.txt"
SIGNATURE_FILE = "checksums.txt.sigstore.json"
SBOM_SUFFIX = ".spdx.json"

# The CLI targets built by the `cli-binaries` job. Keep in lockstep with its matrix.
BINARY_ASSETS = (
    "curie-x86_64-unknown-linux-gnu",
    "curie-aarch64-unknown-linux-gnu",
    "curie-aarch64-apple-darwin",
)
COMPOSE_ASSET = "compose.release.yaml"


class IntegrityError(Exception):
    """A release asset is missing, uncovered, or does not match its checksum."""


def chart_asset(version: str) -> str:
    """The chart tgz `helm package --version <version>` produces."""
    return f"curie-{version}.tgz"


def required_assets(version: str) -> list[str]:
    """Every asset the release must publish, in a stable order."""
    return [*BINARY_ASSETS, chart_asset(version), COMPOSE_ASSET]


def _is_integrity_artifact(name: str) -> bool:
    return name in (CHECKSUM_FILE, SIGNATURE_FILE) or name.endswith(SBOM_SUFFIX)


def assets_in(dist: Path) -> list[str]:
    """Every file in dist that is a release asset rather than an artifact about one."""
    return sorted(
        p.name for p in dist.iterdir() if p.is_file() and not _is_integrity_artifact(p.name)
    )


def _check_required_present(dist: Path, version: str) -> None:
    missing = [name for name in required_assets(version) if not (dist / name).is_file()]
    if missing:
        raise IntegrityError(
            f"release asset(s) missing from {dist}: {', '.join(missing)}. "
            "The release must not publish a partial asset set."
        )


def _check_sbom_is_usable(sbom: Path) -> None:
    """An SBOM must actually be an SPDX document that inventories something.

    Presence is too weak a check: a generator that degrades to `{}` or to a
    zero-package document still writes a file, and the gate would call that
    covered while the SBOM says nothing at all.
    """
    raw = sbom.read_text().strip()
    if not raw:
        raise IntegrityError(f"SBOM {sbom.name} is empty; the generator produced nothing.")
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise IntegrityError(f"SBOM {sbom.name} is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict) or "spdxVersion" not in doc:
        raise IntegrityError(f"SBOM {sbom.name} is not an SPDX document (no spdxVersion).")
    if not doc.get("packages"):
        raise IntegrityError(
            f"SBOM {sbom.name} lists no packages; it inventories nothing. "
            "Check what the generator was pointed at."
        )


def _check_no_undeclared_assets(dist: Path, version: str) -> None:
    """Nothing gets published that this file does not name.

    `files: dist/*` publishes whatever is in dist, and the steps above sign and
    attest it. So a stray file -- a leftover build artifact, or anything a
    compromised step drops in -- would otherwise ship signed and attested purely
    because it brought an SBOM along. Declaring the release set here means adding
    an asset is a reviewed edit to this list, not a side effect of a job writing
    to dist.
    """
    declared = set(required_assets(version))
    undeclared = [name for name in assets_in(dist) if name not in declared]
    if undeclared:
        raise IntegrityError(
            f"file(s) staged for release but not declared in {Path(__file__).name}: "
            f"{', '.join(undeclared)}. If this is a new release asset, add it to "
            "required_assets() and generate an SBOM for it; if it is not, keep it "
            "out of dist."
        )


def _check_sbom_coverage(dist: Path) -> None:
    """Every asset carries a usable SBOM, and every SBOM belongs to an asset."""
    assets = assets_in(dist)
    for name in assets:
        sbom = dist / f"{name}{SBOM_SUFFIX}"
        if not sbom.is_file():
            raise IntegrityError(
                f"no SBOM for release asset '{name}': expected {sbom.name}. "
                "Every published asset needs a dependency inventory (#629); add one "
                "in the job that builds the asset."
            )
        _check_sbom_is_usable(sbom)

    # An SBOM naming an asset that is not here means the asset went missing or the
    # SBOM is misnamed; either way the release is not what it claims to be.
    for path in sorted(dist.iterdir()):
        if not path.name.endswith(SBOM_SUFFIX):
            continue
        subject = path.name[: -len(SBOM_SUFFIX)]
        if subject not in assets:
            raise IntegrityError(
                f"SBOM {path.name} describes '{subject}', which is not in the release."
            )


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest_entries(dist: Path) -> list[str]:
    """Files the checksum manifest covers: everything but the manifest and its signature."""
    skip = (CHECKSUM_FILE, SIGNATURE_FILE)
    return sorted(p.name for p in dist.iterdir() if p.is_file() and p.name not in skip)


def build_manifest(dist: Path, version: str) -> str:
    """Return checksums.txt content for a complete dist, or raise IntegrityError.

    The format is coreutils `sha256sum` output ("<hex>  <name>"), because the
    documented verification step is `sha256sum --check checksums.txt`.
    """
    _check_required_present(dist, version)
    _check_no_undeclared_assets(dist, version)
    _check_sbom_coverage(dist)
    lines = [f"{sha256_of(dist / name)}  {name}" for name in manifest_entries(dist)]
    return "".join(f"{line}\n" for line in lines)


def parse_manifest(text: str) -> dict[str, str]:
    listed: dict[str, str] = {}
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        digest, _, name = line.partition("  ")
        if not name:
            raise IntegrityError(f"{CHECKSUM_FILE} line {lineno} is not sha256sum format: {line!r}")
        listed[name] = digest
    return listed


def verify(dist: Path, version: str) -> None:
    """Re-check a published dist end to end, raising IntegrityError on any gap."""
    _check_required_present(dist, version)
    _check_no_undeclared_assets(dist, version)
    _check_sbom_coverage(dist)

    manifest = dist / CHECKSUM_FILE
    if not manifest.is_file():
        raise IntegrityError(f"{CHECKSUM_FILE} was not published with the release.")
    if not (dist / SIGNATURE_FILE).is_file():
        raise IntegrityError(
            f"{SIGNATURE_FILE} was not published: the checksum manifest carries no "
            "cosign signature, so nothing binds it to this workflow."
        )

    listed = parse_manifest(manifest.read_text())
    present = set(manifest_entries(dist))

    unlisted = sorted(present - set(listed))
    if unlisted:
        raise IntegrityError(
            f"file(s) published but not listed in {CHECKSUM_FILE}: {', '.join(unlisted)}. "
            "An unlisted file is outside the signature."
        )
    undelivered = sorted(set(listed) - present)
    if undelivered:
        raise IntegrityError(
            f"file(s) listed in {CHECKSUM_FILE} but not published: {', '.join(undelivered)}."
        )

    for name, expected in sorted(listed.items()):
        actual = sha256_of(dist / name)
        if actual != expected:
            raise IntegrityError(
                f"sha256 mismatch for '{name}': manifest says {expected}, file is {actual}."
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="action", required=True)
    for name, help_text in (
        ("manifest", f"gate the dist, then write {CHECKSUM_FILE}"),
        ("verify", "re-check a published dist against its manifest"),
    ):
        action = sub.add_parser(name, help=help_text)
        action.add_argument(
            "--dist", type=Path, default=Path("dist"), help="release dist directory"
        )
        action.add_argument(
            "--version", required=True, help="release version, without the leading v"
        )

    args = parser.parse_args(argv)

    try:
        if args.action == "manifest":
            text = build_manifest(args.dist, args.version)
            (args.dist / CHECKSUM_FILE).write_text(text)
            print(f"OK: {len(text.splitlines())} file(s) covered by {CHECKSUM_FILE}")
        else:
            verify(args.dist, args.version)
            print("OK: every published asset is checksummed, signed, and has an SBOM")
    except IntegrityError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

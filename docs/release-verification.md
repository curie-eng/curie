# Verifying a release

Every GitHub release publishes the `curie` CLI binaries, the Helm chart, and
`compose.release.yaml`. Installing the CLI means running a downloaded binary as
root and pointing it at a cluster, so verify what you received before you run it.

This page is the reference and owns the download-verify-install flow. The
[README quickstart](../README.md#quickstart), [`QUICKSTART.md`](../QUICKSTART.md),
and the [local dev onboarding](onboarding.md) point here for it.

These artifacts are produced by the release workflow, so they exist on every
release published *after* v0.4.0. Nothing below works against v0.4.0 or earlier,
which shipped bare binaries.

## Patch releases name their trigger and live proof

A patch release (`vX.Y.Z` where Z is not 0) names the defects that triggered it
and the live surface each defect was re-verified on. The four v0.8.x patch PRs
(#2117, #2157, #2181, #2218) are the negative examples: they described release
mechanics, marked every product end-to-end tier n/a, and moved unfinished
issues out of the milestone so it read as empty. Only v0.8.4 had a real
trigger, and even that cut did not record live proof on the PR.

The release pull request therefore carries two required sections, which the
default [PR template](../.github/PULL_REQUEST_TEMPLATE.md) includes:

- **Trigger** lists the issue numbers of the defects that caused the cut.
- **Live proof** names a CI or ladder run URL that re-verified each trigger on
  a live surface, or an explicit `waiver:` line with the reason no live run
  was possible.

`scripts/check-pr-body.sh` (the PR-body gate in
[`.github/workflows/pr-body.yaml`](../.github/workflows/pr-body.yaml)) rejects a
patch release PR whose title matches `Prepare the vX.Y.Z release` when either
section is missing, comment-only, or empty of those contents. Feature releases
(`vX.Y.0`) are not this gate.

When an open issue is moved out of the milestone within 24 hours of the cut,
comment on that issue naming the release it moved from. That is a process
rule: the PR-body gate does not observe milestone moves. A milestone that
reads "zero open issues" is not itself a reason to cut.

## What each release publishes

| Asset | What it is |
|---|---|
| `curie-x86_64-unknown-linux-gnu` | CLI binary, Linux x86_64 |
| `curie-aarch64-unknown-linux-gnu` | CLI binary, Linux aarch64 |
| `curie-aarch64-apple-darwin` | CLI binary, macOS Apple silicon |
| `curie-<version>.tgz` | packaged Helm chart |
| `compose.release.yaml` | the self-contained local stack |
| `<asset>.spdx.json` | SPDX SBOM, one per asset |
| `checksums.txt` | sha256 of every file above |
| `checksums.txt.sigstore.json` | cosign signature over `checksums.txt` |

Every asset also carries [SLSA (Supply-chain Levels for Software Artifacts)
build provenance](https://slsa.dev/), naming the repository, the workflow, and
the commit it was built from.

The trust chain is: provenance and the cosign signature establish that
`checksums.txt` came from this repo's release workflow, and `checksums.txt`
establishes that each file is the one that workflow produced. So verifying the
manifest's signature and then checking a file against the manifest is enough --
you do not need a separate signature per asset.
[ADR-0052](adr/0052-release-asset-trust-model.md) (Architecture Decision
Record) records why the trust model is shaped this way.

## Verify the CLI before installing it

Set the version you are installing and download the binary alongside the manifest
and its signature:

```bash
VERSION=vX.Y.Z          # the release you are installing
REPO=curie-eng/curie
# Resolve the right asset for this machine. The release ships Linux x86_64,
# Linux aarch64, and macOS Apple silicon; anything else builds from source (see cli/).
ASSET=
case "$(uname -s)/$(uname -m)" in
  Linux/x86_64)                 ASSET=curie-x86_64-unknown-linux-gnu ;;
  Linux/aarch64)                ASSET=curie-aarch64-unknown-linux-gnu ;;
  Darwin/arm64|Darwin/aarch64)  ASSET=curie-aarch64-apple-darwin ;;
esac
: "${ASSET:?no prebuilt binary for this platform; build the CLI from source in cli/}"
BASE="https://github.com/$REPO/releases/download/$VERSION"

curl -fsSLO "$BASE/$ASSET"
curl -fsSLO "$BASE/checksums.txt"
curl -fsSLO "$BASE/checksums.txt.sigstore.json"
```

**Step 1 -- the manifest is genuinely ours.** Requires
[cosign](https://docs.sigstore.dev/system_config/installation/). The identity is
pinned to the exact workflow and tag, so a manifest signed by any other workflow,
repo, or ref fails:

```bash
cosign verify-blob \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity "https://github.com/$REPO/.github/workflows/release.yaml@refs/tags/$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
```

**Step 2 -- the binary matches the manifest.** `--ignore-missing` checks only the
files you actually downloaded:

```bash
sha256sum --check --ignore-missing checksums.txt   # macOS: shasum -a 256 --check --ignore-missing checksums.txt
```

**Step 3 -- only now, install it.** Both checks must print OK first:

```bash
chmod +x "$ASSET" && sudo mv "$ASSET" /usr/local/bin/curie
curie --version
```

## Verify provenance with the GitHub CLI

If you have `gh` and would rather not install cosign, `gh attestation verify`
checks any single asset in one command, no manifest needed. It fails unless the
asset was built by this repo's release workflow:

```bash
gh attestation verify "$ASSET" \
  --repo curie-eng/curie \
  --signer-workflow curie-eng/curie/.github/workflows/release.yaml \
  --source-ref "refs/tags/$VERSION"
```

`--source-ref` is doing real work, so do not drop it: `--signer-workflow` alone
accepts any artifact this workflow ever built, on any ref, so a file swapped in
from another tag would still verify. Add `--format json` to read the full
provenance statement, including the commit the build ran from.

## Verify the chart and the compose file

Same two steps, different asset. The chart and compose file are data rather than
executables, but a tampered chart deploys tampered images:

```bash
curl -fsSLO "$BASE/compose.release.yaml"
curl -fsSLO "$BASE/curie-${VERSION#v}.tgz"
sha256sum --check --ignore-missing checksums.txt
```

Run the cosign step above first if you have not already: checking a file against
an unverified manifest proves only that it downloaded intact.

A release binary fetches these two assets itself when you run `curie cluster
up` or `curie local up`, caching them under `~/.cache/curie/`. That fetch
does not verify them today: it is protected by HTTPS to GitHub, not by the
signature. Verify them by hand as above if you need the stronger guarantee.

## SBOMs (Software Bill of Materials)

Each asset ships an SPDX (Software Package Data Exchange) 2.3 SBOM at
`<asset>.spdx.json`, covered by `checksums.txt` like any other file, so verify
it the same way before trusting it:

- **CLI binaries** -- cataloged from `cli/Cargo.lock`, so the SBOM is the full
  crate dependency graph the build pinned. This is the one to feed to a
  vulnerability scanner.
- **Chart and compose** -- these are deployment manifests with no dependencies of
  their own, so their SBOMs inventory the packaged artifact itself and little
  else. The dependency graph of what they deploy belongs to the `curie-*`
  container images, and those do **not** carry SBOMs or provenance yet: that is
  issue #62, still open. Do not read a verified chart as a verified stack.

Scan one with any SPDX-aware tool, for example
[grype](https://github.com/anchore/grype):

```bash
grype "sbom:./$ASSET.spdx.json"
```

## What happens if an asset is not covered

The release fails, before and after publishing.

`release/integrity.py` is the gate and the single definition of what "covered"
means. `.github/workflows/release.yaml` calls it twice: once before signing, to
refuse to build a checksum manifest over an incomplete asset set (signing an
incomplete manifest would launder the gap into a valid signature), and once
against the published bytes.

The release is created as a **draft**, whose assets are not publicly
downloadable. The `verify-and-publish` job then re-downloads it and re-runs the
whole documented path -- checksums, cosign, and `gh attestation verify` -- and
only promotes the draft to a published release if all of it passes. Publication
is the consequence of verification, so an unverifiable release never reaches
anyone.

The check is closed-world: every file in the release must be an asset
`release/integrity.py` declares, a usable SBOM for one of them, or the manifest
and its signature. Anything else fails, including a stray file that brought an
SBOM along -- `files: dist/*` publishes whatever is staged, so the declared list
is what stops an unreviewed artifact from being signed and shipped. Adding a
release asset therefore means declaring it there and generating an SBOM for it,
which is a reviewed edit rather than a side effect.

## Not covered yet

- **Apple notarization.** The macOS binary is unsigned and un-notarized, pending
  an Apple developer account decision. Gatekeeper quarantines a browser-downloaded
  copy; the `curl` download above avoids this (curl does not set the quarantine
  flag), or clear it once with
  `xattr -d com.apple.quarantine ./curie-aarch64-apple-darwin` (or right-click
  the binary in Finder and choose Open). Verify it with cosign or
  `gh attestation verify` as above -- that is the real check regardless.
- **Container images.** GHCR (GitHub Container Registry) image signing,
  provenance, and SBOMs are issue #62.
- **The SBOM generator's own supply chain.** `anchore/sbom-action` is pinned to a
  commit SHA, but on Linux and macOS it fetches `install.sh` from the `anchore/syft`
  `main` branch at run time, so the SBOM step still executes mutable upstream code
  before the artifacts are signed. Pinning the action does not pin that. Replacing
  it with a checksum-pinned syft binary would close the gap, at the cost of
  hand-managing syft upgrades that Dependabot handles today.

#!/usr/bin/env bash
# One-command installer for curie. Two modes, auto-detected:
#
#   * From a source checkout (this script sits at the repo root next to
#     cli/Cargo.toml):
#         ./get-curie.sh
#     builds the CLI from source (cargo install --path cli) and hands off to
#     `curie install` for the rest (uv sync, pnpm install, runner image).
#     This is the contributor bootstrap: it solves the chicken-and-egg where
#     the CLI cannot install itself before it is built. On an existing install
#     it reuses heavyweight artifacts (`curie install --update`) but always
#     rebuilds and reinstalls the CLI from this checkout.
#
#   * Anywhere else, including piped straight from the web:
#         curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | bash
#     downloads the latest released binary for this platform (or
#     CURIE_VERSION=vX.Y.Z), ALWAYS verifies the sha256, runs
#     `cosign verify-blob` with the pinned certificate identity when cosign is
#     on PATH (set CURIE_REQUIRE_COSIGN=1 to make its absence a hard failure),
#     and installs the binary to a PATH location. No checkout, no toolchain. It
#     never calls sudo; run it under sudo yourself for a root-owned dir. This
#     automates docs/release-verification.md without weakening it.
#
# The mode is auto-detected (a real script file next to cli/Cargo.toml => source,
# otherwise download). Force it with CURIE_INSTALL_MODE=source|download.
set -euo pipefail

REPO=curie-eng/curie

# --- mode detection -------------------------------------------------------
# A source checkout is one where this script is a real file on disk sitting at
# the repo root next to cli/Cargo.toml. When piped via `curl | bash` there is
# no file on disk (BASH_SOURCE is unset or not a path), so we fall through to
# the download path.
SELF_DIR=
if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
  SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
MODE="${CURIE_INSTALL_MODE:-}"
if [ -z "$MODE" ]; then
  if [ -n "$SELF_DIR" ] && [ -f "$SELF_DIR/cli/Cargo.toml" ]; then
    MODE=source
  else
    MODE=download
  fi
fi

# ==========================================================================
# SOURCE MODE -- build the CLI from this checkout, then hand off to `curie
# install` for deps + runner image.
# ==========================================================================
if [ "$MODE" = source ]; then
  if [ -z "$SELF_DIR" ] || [ ! -f "$SELF_DIR/cli/Cargo.toml" ]; then
    echo "error: source install mode needs a source checkout -- this script must be a" >&2
    echo "file sitting next to cli/Cargo.toml. To install the released binary instead," >&2
    echo "run in download mode (unset CURIE_INSTALL_MODE, or pipe this from the web)." >&2
    exit 1
  fi
  cd "$SELF_DIR"

  # Invoke the freshly installed binary by its absolute path so this works even
  # when ~/.cargo/bin is not yet on PATH in the current shell (a new clone on a
  # machine whose shell rc has not been re-sourced).
  CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
  CURIE_BIN="$CARGO_BIN/curie"

  # Always (re)build and install the CLI from this checkout. A file-mtime
  # heuristic cannot tell whether the checked-out source matches the installed
  # binary -- a `git checkout`/branch switch changes content without reliably
  # bumping mtimes, so a stale binary (missing commands that exist in the
  # source) would silently survive. cargo's own caching keeps this to a few
  # seconds when nothing changed; --force just re-links and copies the binary.
  UPDATE_ARGS=()
  [[ -x "$CURIE_BIN" ]] && UPDATE_ARGS=(--update)
  echo "==> cargo install --path cli --force (build curie and install it to ~/.cargo/bin)"
  cargo install --path cli --force

  # `${a[@]+...}` rather than a bare `"${UPDATE_ARGS[*]}"`/`"${UPDATE_ARGS[@]}"`:
  # expanding an EMPTY array under `set -u` is an "unbound variable" error until
  # bash 4.4, and macOS ships 3.2 as /bin/bash. UPDATE_ARGS is empty on exactly
  # the FIRST run (no installed binary yet, so no --update), which is the run
  # this script exists for -- a fresh Mac clone aborted here before installing.
  echo "==> curie install ${UPDATE_ARGS[*]+${UPDATE_ARGS[*]}} (deps + runner image as needed)"
  "$CURIE_BIN" install ${UPDATE_ARGS[@]+"${UPDATE_ARGS[@]}"}

  echo
  echo "Done. If 'curie' is not found in this shell, add ~/.cargo/bin to PATH"
  echo "(rustup writes ~/.cargo/env for this) and open a new shell."
  echo
  echo "For future changes you don't need this script -- run 'curie update' to"
  echo "rebuild and reinstall the CLI on PATH ('curie update --image' also rebuilds"
  echo "the runner image)."
  exit 0
fi

# ==========================================================================
# DOWNLOAD MODE -- fetch, verify, and install the released binary.
# ==========================================================================

# Resolve the asset for this machine. The release ships Linux binaries for
# x86_64 and arm64, plus macOS Apple silicon, so this case statement is the whole
# platform contract. Reset ASSET first so an unmatched platform is empty, not
# stale, and name neither asset literally anywhere else (issues #746, #752).
ASSET=
case "$(uname -s)/$(uname -m)" in
  Linux/x86_64)                ASSET=curie-x86_64-unknown-linux-gnu ;;
  Linux/arm64|Linux/aarch64)   ASSET=curie-aarch64-unknown-linux-gnu ;;
  Darwin/arm64|Darwin/aarch64) ASSET=curie-aarch64-apple-darwin ;;
esac
if [ -z "$ASSET" ]; then
  echo "error: no prebuilt curie binary for $(uname -s)/$(uname -m)." >&2
  echo "Supported: Linux x86_64/arm64 and macOS Apple silicon (Darwin arm64)." >&2
  echo "On anything else, build the CLI from source: clone the repo and run" >&2
  echo "./get-curie.sh from the checkout (see cli/ and docs/release-verification.md)." >&2
  exit 1
fi

# Pick the sha256 tool up front: sha256sum on Linux, shasum -a 256 on macOS.
sha256_check() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check --ignore-missing "$@"
  else
    shasum -a 256 --check --ignore-missing "$@"
  fi
}
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "error: need sha256sum or shasum on PATH to verify the download." >&2
  exit 1
fi

# Resolve the release tag: CURIE_VERSION wins, else the GitHub API's latest.
VERSION="${CURIE_VERSION:-}"
if [ -z "$VERSION" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d'"' -f4)"
fi
if [ -z "$VERSION" ]; then
  echo "error: could not resolve the latest release tag. Set CURIE_VERSION=vX.Y.Z." >&2
  exit 1
fi

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
cd "$WORKDIR"

BASE="https://github.com/$REPO/releases/download/$VERSION"
echo "==> downloading $ASSET ($VERSION)"
curl -fsSLO "$BASE/$ASSET"
curl -fsSLO "$BASE/checksums.txt"
curl -fsSLO "$BASE/checksums.txt.sigstore.json"

# Step 1: prove checksums.txt is genuinely this repo's release output. Keep this
# and the sha256 step as separate statements so `set -e` still trips on either;
# do not chain them with && (that would swallow the second under set -e).
if command -v cosign >/dev/null 2>&1; then
  echo "==> cosign verify-blob (signed checksums manifest)"
  cosign verify-blob \
    --bundle checksums.txt.sigstore.json \
    --certificate-identity "https://github.com/$REPO/.github/workflows/release.yaml@refs/tags/$VERSION" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    checksums.txt
elif [ "${CURIE_REQUIRE_COSIGN:-0}" = "1" ]; then
  echo "error: CURIE_REQUIRE_COSIGN=1 but cosign is not on PATH." >&2
  echo "Install cosign: https://docs.sigstore.dev/system_config/installation/" >&2
  exit 1
else
  echo "WARNING: cosign not found -- skipping signature check of checksums.txt." >&2
  echo "  The sha256 check below still runs, but cannot prove the manifest is genuine." >&2
  echo "  Verify the signature by hand once cosign is installed:" >&2
  echo "    cosign verify-blob --bundle checksums.txt.sigstore.json \\" >&2
  echo "      --certificate-identity https://github.com/$REPO/.github/workflows/release.yaml@refs/tags/$VERSION \\" >&2
  echo "      --certificate-oidc-issuer https://token.actions.githubusercontent.com checksums.txt" >&2
  echo "  Or re-run with CURIE_REQUIRE_COSIGN=1 to make this a hard failure." >&2
fi

# Step 2: prove the binary matches the (now verified) manifest.
echo "==> verifying sha256"
sha256_check checksums.txt

# Step 3: install. /usr/local/bin when writable without sudo, else ~/.local/bin.
if [ -w /usr/local/bin ]; then
  INSTALL_DIR=/usr/local/bin
else
  INSTALL_DIR="$HOME/.local/bin"
  mkdir -p "$INSTALL_DIR"
fi
# Set an explicit 0755 rather than chmod +x: under a permissive umask the
# downloaded file can start group/world-writable, and +x would preserve those
# write bits into the installed binary (mv keeps the mode), letting another
# local user replace the CLI later run as you.
chmod 755 "$ASSET"
mv -f "$ASSET" "$INSTALL_DIR/curie"
echo "==> installed curie to $INSTALL_DIR/curie"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "WARNING: $INSTALL_DIR is not on your PATH. Add it, e.g.:" >&2
    echo "    export PATH=\"$INSTALL_DIR:\$PATH\"" >&2
    ;;
esac

"$INSTALL_DIR/curie" --version

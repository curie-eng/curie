#!/usr/bin/env bash
#
# Render-assertion test for the Helm release Secret size ceiling. Proves:
#
#   1. BUDGET: the record `helm` would store for a default install of this
#      chart fits inside the Kubernetes Secret data limit (1 MiB), with
#      headroom left for the chart to keep growing.
#   2. ACCOUNTING: the `ci/` assertion scripts are excluded from the packaged
#      chart, so adding one costs nothing against that budget.
#
# `helm` keeps the whole release record -- every chart file plus the rendered
# manifest -- in a single Secret, so the chart's own bulk is spent against a
# hard 1 MiB API limit. Nothing warns as that fills: the chart renders, lints
# and templates fine right up to the byte that breaks it, and then EVERY
# install fails at once with `Secret ... data: Too long`. That is how it broke
# in September 2026: the `ci/` scripts had grown to 1.2 MiB of chart payload
# that no installed release ever reads, and the parity ladder and released
# chart upgrade both went red on `main` at the same commit.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== Packaging the chart as helm would store it ==="
helm package "$CHART" -d "$TMP" >/dev/null

echo "=== Rendering the default manifest ==="
helm template rel "$CHART" > "$TMP/manifest.yaml"

python3 - "$TMP" <<'PY'
import base64, gzip, io, json, os, sys, tarfile

tmp = sys.argv[1]
LIMIT = 1024 * 1024          # Kubernetes Secret data ceiling, in bytes.
BUDGET_FRACTION = 0.75       # Fail while there is still room to fix it.

tgz = next(os.path.join(tmp, f) for f in os.listdir(tmp) if f.endswith(".tgz"))

files = {}
with tarfile.open(tgz) as tar:
    for member in tar.getmembers():
        if member.isfile():
            files[member.name] = tar.extractfile(member).read()

packaged_ci = sorted(n for n in files if "/ci/" in n)
if packaged_ci:
    raise SystemExit(
        "the chart package still carries ci/ assertion scripts, which no "
        "installed release reads; add `ci/` to charts/curie/.helmignore. "
        f"Found {len(packaged_ci)}, e.g. {packaged_ci[:3]}"
    )
print("  ok: ci/ assertion scripts are excluded from the packaged chart")

manifest = open(os.path.join(tmp, "manifest.yaml"), "rb").read()

# Mirror how helm builds the record it stores: chart files base64-encoded
# inside the release JSON alongside the rendered manifest, gzipped, then
# base64-encoded into the Secret's data value.
#
# This is a model, not helm's own serializer, so it is deliberately built to
# err high -- a budget guard that underestimates is worthless. It differs
# from helm in two places, and both are accounted for:
#
#   OVER by ~110 KB. helm stores values as a parsed map, dropping comments;
#   this counts values.yaml raw, which is 84% comments. That inflates the
#   estimate, so comment growth can trip the budget early. Safe direction,
#   and the remedy (trim comments, or raise the budget deliberately) is the
#   same conversation the guard exists to start.
#
#   UNDER by the rendered NOTES, which helm keeps in info.notes. Rendering
#   NOTES needs an API server (`helm install --dry-run=client` still dials
#   one), so CI cannot produce it. Instead, allow for it: charge the record a
#   second copy of the NOTES.txt template source. The rendered output is that
#   template with its conditional branches resolved, so a whole extra copy is
#   a generous ceiling for it.
notes_allowance = next(
    (d for n, d in files.items() if n.endswith("templates/NOTES.txt")), b""
)
if not notes_allowance:
    raise SystemExit(
        "templates/NOTES.txt is not in the packaged chart, so the rendered "
        "NOTES this model allows for cannot be bounded; update this script."
    )

release = {
    "chart": {"files": [
        {"name": n, "data": base64.b64encode(d).decode()} for n, d in files.items()
    ]},
    "manifest": manifest.decode("utf-8", "replace"),
    "info": {"notes": notes_allowance.decode("utf-8", "replace")},
    "config": {},
}
buf = io.BytesIO()
with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
    gz.write(json.dumps(release).encode())
stored = len(base64.b64encode(buf.getvalue()))

budget = int(LIMIT * BUDGET_FRACTION)
pct = stored / LIMIT * 100
print(f"  release Secret payload: {stored:,} bytes ({pct:.1f}% of the {LIMIT:,} byte limit)")

if stored >= LIMIT:
    raise SystemExit(
        f"the release record is {stored:,} bytes, over the {LIMIT:,} byte "
        "Secret limit: EVERY `curie cluster up` against this chart will fail "
        "with `Secret ... data: Too long`. Shrink the chart payload."
    )
if stored > budget:
    raise SystemExit(
        f"the release record is {stored:,} bytes, past the {budget:,} byte "
        f"budget ({BUDGET_FRACTION:.0%} of the {LIMIT:,} byte Secret limit). "
        "It still installs, but there is little room left; shrink the chart "
        "payload or exclude files no installed release reads."
    )
print(f"  ok: within the {budget:,} byte budget")
PY

echo
echo "PASS: the Helm release Secret for a default install fits the 1 MiB ceiling with headroom, and ci/ scripts cost nothing against it."

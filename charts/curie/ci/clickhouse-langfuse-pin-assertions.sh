#!/usr/bin/env bash
#
# Issue #2210: keep the chart ClickHouse default and the Langfuse pin as one
# supported pair, and fail CI when either side drifts.
#
# Langfuse 3.225.x ClickHouse migration 39 cannot parse on 24.8 or 25.8. The
# chart default must be on the 25.12 line (the version Langfuse's own compose
# pins at v3.225.7) so the Langfuse pin can be advanced, and AVX is a
# chart-default requirement because 25.12 is not SSE4.2-safe. Since #2319 the
# expected value is the 25.12.11.4 patch build rather than the moving 25.12
# alias. Comments on both values.yaml keys must name the other pin so they are
# not moved independently.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
VALUES="$CHART/values.yaml"
EXPECTED_LANGFUSE="3.225.5"
EXPECTED_CLICKHOUSE="25.12.11.4"
EXPECTED_CLICKHOUSE_IMAGE="clickhouse/clickhouse-server:${EXPECTED_CLICKHOUSE}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RENDER="$TMP/chart.yaml"
CHECKER="$TMP/check.py"

helm template curie "$CHART" >"$RENDER"

cat >"$CHECKER" <<'PY'
import os
import pathlib
import subprocess
import sys
import tempfile

import yaml

(
    chart_path,
    values_path,
    expected_langfuse,
    expected_clickhouse,
    expected_clickhouse_image,
) = sys.argv[1:]

documents = [
    document
    for document in yaml.safe_load_all(pathlib.Path(chart_path).read_text())
    if document
]

problems = []
clickhouse_images = []
langfuse_images = {}
preflight_env = {}
preflight_script = None

for document in documents:
    kind = document.get("kind")
    name = document.get("metadata", {}).get("name", "")
    spec = document.get("spec", {})
    template_spec = spec.get("template", {}).get("spec", {})
    containers = template_spec.get("containers") or []

    if kind == "StatefulSet":
        for container in containers:
            if container.get("name") == "clickhouse":
                clickhouse_images.append(container.get("image"))

    if kind == "Deployment":
        for container in containers:
            cname = container.get("name")
            if cname in ("langfuse-web", "langfuse-worker"):
                langfuse_images[cname] = container.get("image")

    if kind == "Job" and str(name).endswith("-preflight-avx"):
        for container in containers:
            if container.get("name") != "avx-check":
                continue
            for item in container.get("env") or []:
                preflight_env[item.get("name")] = item.get("value")
            command = container.get("command") or []
            if command:
                preflight_script = command[-1]

if clickhouse_images != [expected_clickhouse_image]:
    problems.append(
        f"chart clickhouse must use reviewed image {expected_clickhouse_image}; "
        f"found {clickhouse_images!r}"
    )

expected_langfuse_images = {
    "langfuse-web": f"langfuse/langfuse:{expected_langfuse}",
    "langfuse-worker": f"langfuse/langfuse-worker:{expected_langfuse}",
}
for name, wanted in expected_langfuse_images.items():
    actual = langfuse_images.get(name)
    if actual != wanted:
        problems.append(
            f"chart {name} must use reviewed image {wanted}; found {actual!r}"
        )

if preflight_env.get("CLICKHOUSE_TAG") != expected_clickhouse:
    problems.append(
        "AVX preflight CLICKHOUSE_TAG must match the chart ClickHouse pin "
        f"{expected_clickhouse}; found {preflight_env.get('CLICKHOUSE_TAG')!r}"
    )
if preflight_env.get("CLICKHOUSE_IMAGE") != expected_clickhouse_image:
    problems.append(
        "AVX preflight CLICKHOUSE_IMAGE must match the chart ClickHouse pin "
        f"{expected_clickhouse_image}; found {preflight_env.get('CLICKHOUSE_IMAGE')!r}"
    )

safe_tags = (preflight_env.get("SSE42_SAFE_TAGS") or "").split()
if expected_clickhouse in safe_tags:
    problems.append(
        f"chart-default ClickHouse {expected_clickhouse} must stay out of "
        "sse42SafeTags so AVX remains required for the default pair; "
        f"found {safe_tags!r}"
    )
if "24.8" not in safe_tags:
    problems.append(
        "sse42SafeTags must still list 24.8 as the explicit AVX-less override; "
        f"found {safe_tags!r}"
    )

values_text = pathlib.Path(values_path).read_text()
langfuse_idx = values_text.find('tag: "' + expected_langfuse + '"')
clickhouse_idx = values_text.find('tag: "' + expected_clickhouse + '"')
if langfuse_idx < 0:
    problems.append(
        f"values.yaml langfuse.image.tag must be {expected_langfuse}"
    )
if clickhouse_idx < 0:
    problems.append(
        f"values.yaml clickhouse.image.tag must be {expected_clickhouse}"
    )

def preceding_comment(text, idx, window=900):
    start = max(0, idx - window)
    return text[start:idx]


if langfuse_idx >= 0:
    langfuse_comment = preceding_comment(values_text, langfuse_idx)
    if "clickhouse.image.tag" not in langfuse_comment or expected_clickhouse not in langfuse_comment:
        problems.append(
            "langfuse.image.tag comment must name clickhouse.image.tag "
            f"{expected_clickhouse} as its constraint"
        )
if clickhouse_idx >= 0:
    clickhouse_comment = preceding_comment(values_text, clickhouse_idx)
    if "langfuse.image.tag" not in clickhouse_comment or expected_langfuse not in clickhouse_comment:
        problems.append(
            "clickhouse.image.tag comment must name langfuse.image.tag "
            f"{expected_langfuse} as its constraint"
        )

if preflight_script is None:
    problems.append("AVX preflight Job script was not rendered")
else:
    script_path = pathlib.Path(tempfile.mkdtemp()) / "avx-check.sh"
    script_path.write_text(preflight_script)
    env = os.environ.copy()
    env.update({
        "CLICKHOUSE_TAG": preflight_env.get("CLICKHOUSE_TAG") or "",
        "CLICKHOUSE_IMAGE": preflight_env.get("CLICKHOUSE_IMAGE") or "",
        "SSE42_SAFE_TAGS": preflight_env.get("SSE42_SAFE_TAGS") or "",
        "FORCE_NO_AVX": "true",
    })
    completed = subprocess.run(
        ["/bin/sh", str(script_path)],
        env=env,
        text=True,
        capture_output=True,
    )
    combined = completed.stdout + completed.stderr
    if completed.returncode == 0:
        problems.append(
            "AVX preflight must FAIL without AVX on the chart-default "
            f"ClickHouse {expected_clickhouse}; it passed:\n{combined}"
        )
    elif "FAIL" not in combined:
        problems.append(
            "AVX preflight failed without AVX but did not report FAIL for "
            f"chart-default ClickHouse {expected_clickhouse}:\n{combined}"
        )

if problems:
    raise SystemExit("\n".join(problems))

print(
    f"chart pins Langfuse {expected_langfuse} with ClickHouse {expected_clickhouse}, "
    "comments name the pair, and AVX is required for the default tag"
)
PY

python3 "$CHECKER" "$RENDER" "$VALUES" "$EXPECTED_LANGFUSE" "$EXPECTED_CLICKHOUSE" "$EXPECTED_CLICKHOUSE_IMAGE"

# helm template does not emit NOTES.txt. Render it through tpl so the
# operator-facing label is asserted on the same consumer path as `helm install`.
NOTES_CHART="$TMP/notes-chart"
cp -a "$CHART" "$NOTES_CHART"
cp "$CHART/templates/NOTES.txt" "$NOTES_CHART/NOTES.txt"
cat >"$NOTES_CHART/templates/notes-check.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-check
data:
  notes: |
{{ tpl (.Files.Get "NOTES.txt") . | nindent 4 }}
EOF

NOTES="$TMP/notes.yaml"
helm template curie "$NOTES_CHART" --show-only templates/notes-check.yaml >"$NOTES"
if ! grep -q '\[AVX required\]' "$NOTES"; then
  echo "FAIL: default helm NOTES must label ClickHouse [AVX required]; got:" >&2
  cat "$NOTES" >&2
  exit 1
fi
if grep -q 'SSE4.2-pinned' "$NOTES"; then
  echo "FAIL: default helm NOTES still claim SSE4.2-pinned" >&2
  exit 1
fi

OVERRIDE_NOTES="$TMP/notes-24.8.yaml"
helm template curie "$NOTES_CHART" --show-only templates/notes-check.yaml \
  --set clickhouse.image.tag=24.8 >"$OVERRIDE_NOTES"
if ! grep -q '\[SSE4.2-safe override\]' "$OVERRIDE_NOTES"; then
  echo "FAIL: 24.8 override NOTES must label ClickHouse [SSE4.2-safe override]; got:" >&2
  cat "$OVERRIDE_NOTES" >&2
  exit 1
fi
echo "NOTES: default ClickHouse is [AVX required]; 24.8 override is [SSE4.2-safe override]"

OVERRIDE_RENDER="$TMP/chart-24.8.yaml"
helm template curie "$CHART" --set clickhouse.image.tag=24.8 >"$OVERRIDE_RENDER"
python3 - "$OVERRIDE_RENDER" <<'PY'
import os
import pathlib
import subprocess
import sys
import tempfile

import yaml

documents = [
    document
    for document in yaml.safe_load_all(pathlib.Path(sys.argv[1]).read_text())
    if document
]
preflight_env = {}
preflight_script = None
clickhouse_images = []
for document in documents:
    kind = document.get("kind")
    name = document.get("metadata", {}).get("name", "")
    spec = document.get("spec", {}).get("template", {}).get("spec", {})
    containers = spec.get("containers") or []
    if kind == "StatefulSet":
        for container in containers:
            if container.get("name") == "clickhouse":
                clickhouse_images.append(container.get("image"))
    if kind == "Job" and str(name).endswith("-preflight-avx"):
        for container in containers:
            if container.get("name") != "avx-check":
                continue
            for item in container.get("env") or []:
                preflight_env[item.get("name")] = item.get("value")
            command = container.get("command") or []
            if command:
                preflight_script = command[-1]
if clickhouse_images != ["clickhouse/clickhouse-server:24.8"]:
    raise SystemExit(f"24.8 override render must use ClickHouse 24.8; found {clickhouse_images!r}")
if preflight_env.get("CLICKHOUSE_TAG") != "24.8":
    raise SystemExit(
        "24.8 override must flow clickhouse.image.tag into AVX preflight "
        f"CLICKHOUSE_TAG; found {preflight_env.get('CLICKHOUSE_TAG')!r}"
    )
if preflight_script is None:
    raise SystemExit("24.8 override did not render the AVX preflight Job")
script_path = pathlib.Path(tempfile.mkdtemp()) / "avx-check.sh"
script_path.write_text(preflight_script)
env = os.environ.copy()
env.update({
    "CLICKHOUSE_TAG": preflight_env.get("CLICKHOUSE_TAG") or "",
    "CLICKHOUSE_IMAGE": preflight_env.get("CLICKHOUSE_IMAGE") or "",
    "SSE42_SAFE_TAGS": preflight_env.get("SSE42_SAFE_TAGS") or "",
    "FORCE_NO_AVX": "true",
})
completed = subprocess.run(["/bin/sh", str(script_path)], env=env, text=True, capture_output=True)
out = completed.stdout + completed.stderr
if completed.returncode != 0:
    raise SystemExit(
        "AVX preflight must PASS without AVX on a helm-rendered 24.8 override; "
        f"it failed:\n{out}"
    )
print("override: helm-rendered clickhouse.image.tag=24.8 passes AVX preflight without AVX")
PY

MUTANT_RENDER="$TMP/chart-old-clickhouse.yaml"
python3 - "$RENDER" "$MUTANT_RENDER" "$EXPECTED_CLICKHOUSE" <<'PY'
import pathlib
import sys

render, mutant, version = sys.argv[1:]
pathlib.Path(mutant).write_text(
    pathlib.Path(render).read_text().replace(f":{version}", ":24.8")
)
PY

negative_output=""
if negative_output="$(python3 "$CHECKER" "$MUTANT_RENDER" "$VALUES" "$EXPECTED_LANGFUSE" "$EXPECTED_CLICKHOUSE" "$EXPECTED_CLICKHOUSE_IMAGE" 2>&1)"; then
  echo "FAIL: ClickHouse 24.8 mutation passed the Langfuse/ClickHouse pair contract" >&2
  exit 1
fi
if [[ "$negative_output" != *"must use reviewed image ${EXPECTED_CLICKHOUSE_IMAGE}"* ]]; then
  echo "FAIL: ClickHouse 24.8 mutation failed unexpectedly: $negative_output" >&2
  exit 1
fi

echo "negative: replacing ClickHouse ${EXPECTED_CLICKHOUSE} with 24.8 is rejected"

MUTANT_LANGFUSE="$TMP/chart-new-langfuse.yaml"
python3 - "$RENDER" "$MUTANT_LANGFUSE" "$EXPECTED_LANGFUSE" <<'PY'
import pathlib
import sys

render, mutant, version = sys.argv[1:]
pathlib.Path(mutant).write_text(
    pathlib.Path(render).read_text().replace(f":{version}", ":3")
)
PY

if negative_output="$(python3 "$CHECKER" "$MUTANT_LANGFUSE" "$VALUES" "$EXPECTED_LANGFUSE" "$EXPECTED_CLICKHOUSE" "$EXPECTED_CLICKHOUSE_IMAGE" 2>&1)"; then
  echo "FAIL: floating Langfuse :3 mutation passed the Langfuse/ClickHouse pair contract" >&2
  exit 1
fi
if [[ "$negative_output" != *"must use reviewed image langfuse/langfuse:${EXPECTED_LANGFUSE}"* ]]; then
  echo "FAIL: floating Langfuse :3 mutation failed unexpectedly: $negative_output" >&2
  exit 1
fi

echo "negative: replacing Langfuse ${EXPECTED_LANGFUSE} with :3 is rejected"

MUTANT_SSE42="$TMP/chart-sse42.yaml"
python3 - "$RENDER" "$MUTANT_SSE42" "$EXPECTED_CLICKHOUSE" <<'PY'
import pathlib
import sys

render, mutant, version = sys.argv[1:]
text = pathlib.Path(render).read_text()
old = 'value: "24.8 24.3 23.8"'
new = f'value: "{version} 24.8 24.3 23.8"'
if old not in text:
    raise SystemExit(f"could not find AVX preflight SSE42_SAFE_TAGS assignment {old}")
pathlib.Path(mutant).write_text(text.replace(old, new, 1))
PY

if negative_output="$(python3 "$CHECKER" "$MUTANT_SSE42" "$VALUES" "$EXPECTED_LANGFUSE" "$EXPECTED_CLICKHOUSE" "$EXPECTED_CLICKHOUSE_IMAGE" 2>&1)"; then
  echo "FAIL: adding ${EXPECTED_CLICKHOUSE} to sse42SafeTags passed the pair contract" >&2
  exit 1
fi
if [[ "$negative_output" != *"must stay out of sse42SafeTags"* ]]; then
  echo "FAIL: sse42SafeTags mutation failed unexpectedly: $negative_output" >&2
  exit 1
fi

echo "negative: adding ClickHouse ${EXPECTED_CLICKHOUSE} to sse42SafeTags is rejected"

MUTANT_VALUES="$TMP/values-no-coupling.yaml"
python3 - "$VALUES" "$MUTANT_VALUES" <<'PY'
import pathlib
import sys

src, dest = sys.argv[1:]
text = pathlib.Path(src).read_text().replace("clickhouse.image.tag", "image.tag").replace(
    "langfuse.image.tag", "image.tag"
)
pathlib.Path(dest).write_text(text)
PY

if negative_output="$(python3 "$CHECKER" "$RENDER" "$MUTANT_VALUES" "$EXPECTED_LANGFUSE" "$EXPECTED_CLICKHOUSE" "$EXPECTED_CLICKHOUSE_IMAGE" 2>&1)"; then
  echo "FAIL: stripping pin-coupling comments passed the pair contract" >&2
  exit 1
fi
if [[ "$negative_output" != *"comment must name"* ]]; then
  echo "FAIL: comment-stripping mutation failed unexpectedly: $negative_output" >&2
  exit 1
fi

echo "negative: stripping the pin-coupling comments is rejected"
echo "PASS: chart Langfuse ${EXPECTED_LANGFUSE} and ClickHouse ${EXPECTED_CLICKHOUSE} stay a documented pair"

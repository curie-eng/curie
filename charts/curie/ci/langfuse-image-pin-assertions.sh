#!/usr/bin/env bash
#
# Issue #2190: keep the shipped Langfuse runtime on one reviewed version.
#
# On 2026-09-01T14:50:59Z upstream shipped v3.225.6 by rebuilding the
# floating `:3` tag and the minor tag `3.225` in place onto its new digest,
# rather than publishing a `3.225.6` tag — so a minor-tag pin would have
# moved too (the exact patch tag `3.225.5` was unaffected). In CI, the
# langfuse-web container reported healthy but never served
# /api/public/health, timing out the bring-up gate. This gate renders both
# user-facing consumers, requires one exact version, and proves a
# floating-tag mutation is rejected.
#
# The chart runs Langfuse images in initContainers as well as application
# containers (the wait-for-clickhouse helper reuses the same image), and both
# are pulled and EXECUTED before the application container starts. Both
# initContainers are named `wait-for-clickhouse`, in both Deployments, so a
# name-keyed expectation cannot address them. The chart-side rule is therefore
# positional-free: the enclosing Deployment decides which reviewed image
# applies, and EVERY container and initContainer in it whose image is a
# `langfuse/` image must equal that image exactly.
#
# Issue #2332: the same three images must also be pinnable by digest. The
# templates used to concatenate `repo` and `tag` with a colon, so an operator
# who set `repo@sha256:...` got `repo@sha256:...:tag` and the kubelet refused it
# with InvalidImageName. The second half of this file renders the chart with
# digests set and proves every one of those images -- including the
# wait-for-clickhouse init container that gates them -- resolves to a bare
# `repo@sha256:...` and never to the `@sha256:...:tag` shape.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
EXPECTED_VERSION="3.225.5"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RENDER="$TMP/chart.yaml"
COMPOSE_JSON="$TMP/compose.json"
CHECKER="$TMP/check.py"

helm template curie "$CHART" >"$RENDER"
docker compose --profile full -f "$REPO_ROOT/compose.dev.yaml" config --format json >"$COMPOSE_JSON"

cat >"$CHECKER" <<'PY'
import json
import pathlib
import sys

import yaml

chart_path, compose_path, expected_version = sys.argv[1:]
expected = {
    "langfuse-web": f"langfuse/langfuse:{expected_version}",
    "langfuse-worker": f"langfuse/langfuse-worker:{expected_version}",
}
LANGFUSE_IMAGE_PREFIX = "langfuse/"
SECTIONS = ("initContainers", "containers")

documents = [
    document
    for document in yaml.safe_load_all(pathlib.Path(chart_path).read_text())
    if document
]

problems = []
# name -> {"deployment": str, "initContainers": int, "containers": int}. Counts
# are the anti-vacuity ledger: a scan that finds nothing must not report PASS.
found = {}

for document in documents:
    if document.get("kind") != "Deployment":
        continue
    deployment = document.get("metadata", {}).get("name")
    pod = document.get("spec", {}).get("template", {}).get("spec", {})
    sections = {section: pod.get(section) or [] for section in SECTIONS}
    langfuse_refs = [
        (section, container)
        for section in SECTIONS
        for container in sections[section]
        if str(container.get("image", "")).startswith(LANGFUSE_IMAGE_PREFIX)
    ]
    # The application container's name is what identifies which reviewed image
    # governs this Deployment; the initContainers inherit that decision.
    owners = [
        container.get("name")
        for container in sections["containers"]
        if container.get("name") in expected
    ]
    if len(owners) != 1:
        if langfuse_refs or owners:
            problems.append(
                f"chart Deployment {deployment} carries {len(langfuse_refs)} Langfuse image "
                f"reference(s) but {len(owners)} known Langfuse application container(s) "
                f"{owners!r}, so no reviewed image can be attributed to it "
                "(floating Langfuse tags are forbidden)"
            )
        continue

    owner = owners[0]
    wanted = expected[owner]
    if owner in found:
        problems.append(
            f"chart application container {owner} rendered twice "
            f"({found[owner]['deployment']} and {deployment})"
        )
        continue
    seen = found.setdefault(
        owner, {"deployment": deployment, "initContainers": 0, "containers": 0}
    )

    # Positive, name-keyed assertion on the application container itself, so a
    # non-`langfuse/` image there cannot slip past the prefix rule below. Any
    # `langfuse/` image is already covered (and counted) by that rule.
    owner_container = next(
        container for container in sections["containers"] if container.get("name") == owner
    )
    owner_image = owner_container.get("image")
    if not str(owner_image).startswith(LANGFUSE_IMAGE_PREFIX):
        seen["containers"] += 1
        problems.append(
            f"chart {deployment} container {owner} must use reviewed image {wanted}; "
            f"found {owner_image!r} (floating Langfuse tags are forbidden)"
        )

    for section, container in langfuse_refs:
        seen[section] += 1
        actual = container.get("image")
        if actual != wanted:
            problems.append(
                f"chart {deployment} {section[:-1]} {container.get('name')} must use "
                f"reviewed image {wanted}; found {actual!r} "
                "(floating Langfuse tags are forbidden)"
            )

for name, wanted in expected.items():
    if name not in found:
        problems.append(
            f"chart renders no {name} container at all; expected {wanted} "
            "(floating Langfuse tags are forbidden)"
        )
        continue
    for section in SECTIONS:
        if not found[name][section]:
            problems.append(
                f"chart {found[name]['deployment']} has no {section[:-1]} running a "
                f"{LANGFUSE_IMAGE_PREFIX}* image, so the pin check over that section is "
                f"vacuous; expected {wanted} (floating Langfuse tags are forbidden)"
            )

compose = json.loads(pathlib.Path(compose_path).read_text())
compose_images = {
    name: compose.get("services", {}).get(name, {}).get("image")
    for name in expected
}
for name, wanted in expected.items():
    actual = compose_images.get(name)
    if actual != wanted:
        problems.append(
            f"compose {name} must use reviewed image {wanted}; found {actual!r} "
            "(floating Langfuse tags are forbidden)"
        )

if problems:
    raise SystemExit("\n".join(problems))

checked = sum(found[name][section] for name in expected for section in SECTIONS)
print(
    f"chart pins {checked} Langfuse image references (containers + initContainers) "
    f"and Compose pins web/worker to {expected_version}"
)
PY

python3 "$CHECKER" "$RENDER" "$COMPOSE_JSON" "$EXPECTED_VERSION"

# Negative control 1: a floating tag on every surface is rejected.
MUTANT_RENDER="$TMP/chart-floating.yaml"
MUTANT_COMPOSE="$TMP/compose-floating.json"
python3 - "$RENDER" "$COMPOSE_JSON" "$MUTANT_RENDER" "$MUTANT_COMPOSE" "$EXPECTED_VERSION" <<'PY'
import pathlib
import sys

render, compose, mutant_render, mutant_compose, version = sys.argv[1:]
pathlib.Path(mutant_render).write_text(pathlib.Path(render).read_text().replace(f":{version}", ":3"))
pathlib.Path(mutant_compose).write_text(pathlib.Path(compose).read_text().replace(f":{version}", ":3"))
PY

negative_output=""
if negative_output="$(python3 "$CHECKER" "$MUTANT_RENDER" "$MUTANT_COMPOSE" "$EXPECTED_VERSION" 2>&1)"; then
  echo "FAIL: floating-tag mutation passed the Langfuse image contract" >&2
  exit 1
fi
if [[ "$negative_output" != *"floating Langfuse tags are forbidden"* ]]; then
  echo "FAIL: floating-tag mutation failed unexpectedly: $negative_output" >&2
  exit 1
fi

echo "negative: replacing the reviewed version with :3 is rejected"

# Negative control 2: the initContainer coverage is not decorative. Float ONLY
# the initContainer images, leaving both application containers and the whole
# Compose surface correctly pinned, and require the gate to still fail.
INIT_MUTANT_RENDER="$TMP/chart-floating-init.yaml"
python3 - "$RENDER" "$INIT_MUTANT_RENDER" <<'PY'
import pathlib
import sys

import yaml

render, mutant_render = sys.argv[1:]
documents = [
    document
    for document in yaml.safe_load_all(pathlib.Path(render).read_text())
    if document
]
mutated = 0
for document in documents:
    if document.get("kind") != "Deployment":
        continue
    pod = document.get("spec", {}).get("template", {}).get("spec", {})
    for container in pod.get("initContainers") or []:
        if str(container.get("image", "")).startswith("langfuse/"):
            container["image"] = container["image"].rsplit(":", 1)[0] + ":3"
            mutated += 1
if mutated != 2:
    raise SystemExit(
        f"expected 2 Langfuse initContainer images to mutate, mutated {mutated}"
    )
pathlib.Path(mutant_render).write_text(yaml.safe_dump_all(documents))
PY

init_negative_output=""
if init_negative_output="$(python3 "$CHECKER" "$INIT_MUTANT_RENDER" "$COMPOSE_JSON" "$EXPECTED_VERSION" 2>&1)"; then
  echo "FAIL: floating initContainer image passed the Langfuse image contract" >&2
  exit 1
fi
if [[ "$init_negative_output" != *" initContainer "* ]]; then
  echo "FAIL: initContainer mutation failed for the wrong reason: $init_negative_output" >&2
  exit 1
fi
if [[ "$init_negative_output" == *" container langfuse-"* ]]; then
  echo "FAIL: initContainer mutation also tripped an application-container check, so it does not isolate the initContainer coverage: $init_negative_output" >&2
  exit 1
fi

echo "negative: a floating :3 image in an initContainer position is rejected"

# ---------------------------------------------------------------------------
# Issue #2332: digest-pinnability. Chart render only -- no Compose -- so this
# section stays runnable wherever `docker compose` is unavailable.
# ---------------------------------------------------------------------------
WEB_DIGEST="sha256:$(printf 'aa%.0s' $(seq 32))"
WORKER_DIGEST="sha256:$(printf 'bb%.0s' $(seq 32))"
CLICKHOUSE_DIGEST="sha256:$(printf 'cc%.0s' $(seq 32))"
CLICKHOUSE_TAG="25.12.11.4"  # charts/curie/values.yaml clickhouse.image.tag

DIGEST_RENDER="$TMP/chart-digest.yaml"
DIGEST_CHECKER="$TMP/check-digest.py"

helm template curie "$CHART" \
  --set-string "langfuse.image.webDigest=$WEB_DIGEST" \
  --set-string "langfuse.image.workerDigest=$WORKER_DIGEST" \
  --set-string "clickhouse.image.digest=$CLICKHOUSE_DIGEST" \
  >"$DIGEST_RENDER"

cat >"$DIGEST_CHECKER" <<'PY'
import pathlib
import re
import sys

import yaml

render_path, web_digest, worker_digest, clickhouse_digest = sys.argv[1:]

expected = {
    "langfuse-web": f"langfuse/langfuse@{web_digest}",
    "langfuse-worker": f"langfuse/langfuse-worker@{worker_digest}",
    "clickhouse": f"clickhouse/clickhouse-server@{clickhouse_digest}",
}

documents = [
    document
    for document in yaml.safe_load_all(pathlib.Path(render_path).read_text())
    if document
]

app_images = {}
# Workload container name -> (app container image, wait-for-clickhouse image).
gate_pairs = {}
all_images = []
preflight_env = None  # {"CLICKHOUSE_IMAGE": ..., "CLICKHOUSE_TAG": ...} from the AVX preflight Job.
for document in documents:
    spec = document.get("spec")
    pod_spec = spec.get("template", {}).get("spec", {}) if isinstance(spec, dict) else {}
    containers = pod_spec.get("containers") or []
    init_containers = pod_spec.get("initContainers") or []
    for container in containers + init_containers:
        all_images.append(container.get("image"))
    gate = next((c for c in init_containers if c.get("name") == "wait-for-clickhouse"), None)
    for container in containers:
        name = container.get("name")
        if name in expected:
            app_images[name] = container.get("image")
            if gate is not None:
                gate_pairs[name] = (container.get("image"), gate.get("image"))
        env_pairs = {e.get("name"): e.get("value") for e in (container.get("env") or [])}
        if "CLICKHOUSE_IMAGE" in env_pairs:
            preflight_env = env_pairs

problems = []
for name, wanted in expected.items():
    actual = app_images.get(name)
    if actual != wanted:
        problems.append(
            f"{name} must render digest reference {wanted}; found {actual!r} "
            "(digest pinning is broken)"
        )

for name in ("langfuse-web", "langfuse-worker"):
    if name not in gate_pairs:
        problems.append(
            f"{name} has no wait-for-clickhouse init container to compare "
            "(digest pinning is broken)"
        )
        continue
    app_image, gate_image = gate_pairs[name]
    if gate_image != app_image:
        problems.append(
            f"{name} wait-for-clickhouse init container must use the same bytes as the "
            f"app container {app_image!r}; found {gate_image!r} (digest pinning is broken)"
        )

if preflight_env is None:
    problems.append(
        "no Job container env carries CLICKHOUSE_IMAGE to compare against the AVX "
        "preflight (digest pinning is broken)"
    )
else:
    clickhouse_app_image = app_images.get("clickhouse")
    if preflight_env.get("CLICKHOUSE_IMAGE") != clickhouse_app_image:
        problems.append(
            "AVX preflight Job env CLICKHOUSE_IMAGE must use the same bytes as the "
            f"ClickHouse container {clickhouse_app_image!r}; found "
            f"{preflight_env.get('CLICKHOUSE_IMAGE')!r} (digest pinning is broken)"
        )
    if "@sha256:" in (preflight_env.get("CLICKHOUSE_TAG") or ""):
        problems.append(
            "AVX preflight Job env CLICKHOUSE_TAG must stay a plain version string "
            f"for the SSE4.2 prefix match, not a digest; found "
            f"{preflight_env.get('CLICKHOUSE_TAG')!r} (digest pinning is broken)"
        )

invalid = sorted(
    {image for image in all_images if image and re.search(r"@sha256:[0-9a-f]+:", image)}
)
if invalid:
    problems.append(
        "rendered images use the InvalidImageName shape @sha256:...:tag: "
        + ", ".join(invalid)
        + " (digest pinning is broken)"
    )

if problems:
    raise SystemExit("\n".join(problems))

print("chart renders every pinnable image as a bare repo@sha256 reference")
PY

python3 "$DIGEST_CHECKER" "$DIGEST_RENDER" "$WEB_DIGEST" "$WORKER_DIGEST" "$CLICKHOUSE_DIGEST"
echo "digest: langfuse web/worker, clickhouse and the clickhouse gate all pin by digest"
echo "digest: AVX preflight Job CLICKHOUSE_IMAGE matches the ClickHouse digest while CLICKHOUSE_TAG stays a plain version string"

MUTANT_DIGEST_RENDER="$TMP/chart-digest-suffixed.yaml"
python3 - "$DIGEST_RENDER" "$MUTANT_DIGEST_RENDER" "$EXPECTED_VERSION" "$CLICKHOUSE_TAG" <<'PY'
import pathlib
import re
import sys

render, mutant, langfuse_version, clickhouse_tag = sys.argv[1:]

# Reproduce exactly what the broken templates rendered: repository, digest, then
# a concatenated `:tag` the kubelet rejects as InvalidImageName.
text = pathlib.Path(render).read_text()
text = re.sub(
    r"(langfuse/langfuse(?:-worker)?@sha256:[0-9a-f]+)",
    lambda match: f"{match.group(1)}:{langfuse_version}",
    text,
)
text = re.sub(
    r"(clickhouse/clickhouse-server@sha256:[0-9a-f]+)",
    lambda match: f"{match.group(1)}:{clickhouse_tag}",
    text,
)
pathlib.Path(mutant).write_text(text)
PY

digest_negative_output=""
if digest_negative_output="$(python3 "$DIGEST_CHECKER" "$MUTANT_DIGEST_RENDER" "$WEB_DIGEST" "$WORKER_DIGEST" "$CLICKHOUSE_DIGEST" 2>&1)"; then
  echo "FAIL: an @sha256:...:tag render passed the digest-pin contract" >&2
  exit 1
fi
if [[ "$digest_negative_output" != *"digest pinning is broken"* ]]; then
  echo "FAIL: digest mutation failed unexpectedly: $digest_negative_output" >&2
  exit 1
fi

echo "negative: appending :<tag> after a digest is rejected"

python3 - "$RENDER" "$CLICKHOUSE_TAG" <<'PY'
import pathlib
import sys

import yaml

render_path, clickhouse_tag = sys.argv[1:]
wanted = f"clickhouse/clickhouse-server:{clickhouse_tag}"

actual = None
for document in yaml.safe_load_all(pathlib.Path(render_path).read_text()):
    if not document or not isinstance(document.get("spec"), dict):
        continue
    containers = document["spec"].get("template", {}).get("spec", {}).get("containers", [])
    for container in containers:
        if container.get("name") == "clickhouse":
            actual = container.get("image")

if actual != wanted:
    raise SystemExit(
        f"with no digest set the clickhouse container must stay on {wanted}; found {actual!r}"
    )

print(f"default render keeps ClickHouse on {wanted}")
PY

echo "default: no digest set leaves the reviewed ClickHouse tag path unchanged"
echo "PASS: chart and Compose share one reviewed Langfuse version, reject floating tags, and every pinnable image renders a valid digest reference"

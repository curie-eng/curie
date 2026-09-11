#!/usr/bin/env bash
# The dev interaction harness (packages/test-support/src/curie_test_support/
# interaction/) reaches no shipped artifact.
#
# Two independent controls, in the order of their strength:
#
#   1. The resolved --no-dev dependency closure of every production image. This
#      is the PRIMARY control and it is about the DISTRIBUTION, not a name: a
#      distribution that is not installed cannot be imported however the harness
#      is spelled, where a module-name sweep only ever finds today's spelling.
#      The five workspace-context app Dockerfiles (api, dispatcher, worker,
#      mail-adapter and adapters/discord) install `uv sync --frozen --no-dev
#      --no-editable --package curie-<app>`, so `uv export --frozen --no-dev
#      --package <p>` is the same set; runner/Dockerfile installs
#      export_dependency_pins.py's output with --no-deps, so that exporter is
#      run for the sixth image. Building the images is deliberately NOT made a
#      CI job (plan decision 3); where an image is already present locally it is
#      additionally probed.
#   1b. The Dockerfile POSTURE, per instruction. The export is not the image:
#      the app images `COPY . .` and ship the builder's /app/.venv, so a builder
#      line that installs a dev-only workspace member by path, or a post-copy
#      `uv sync` with --no-dev dropped, changes nothing control 1 can see. Both
#      shapes are seeded against the checker at the end of this script.
#   2. A sealed-chart-render sweep for the module name, the distribution name and
#      their base64 encodings, over EVERY rendered document and EVERY pod-spec
#      container kind (containers, initContainers, ephemeralContainers) plus
#      decoded Secret `data` -- the sibling
#      ci/approval-principal-wiring-assertions.sh's method, for the same reason:
#      a check that reads containers[0] of four named Deployments is not a claim
#      about the render.
#
# A typo'd path in an assertion script exits 0 and proves nothing, so both
# controls are exercised against a SEEDED VIOLATION at the end of this script
# and must reject it. Removing that self-check removes the only evidence that
# this script is capable of failing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "ASSERTION FAILED: $1" >&2; exit 1; }

HARNESS_MODULE="curie_test_support"
HARNESS_DISTRIBUTION="curie-test-support"
HARNESS_PATH="packages/test-support"
APP_PACKAGES=(curie-api curie-dispatcher curie-worker curie-mail-adapter curie-discord-adapter)
# Every Dockerfile whose build context is the WORKSPACE. adapters/discord was a
# production recipe of exactly the app shape (COPY . . then a --package uv sync)
# and was on none of these lists, so "every production image" excluded an image.
PROD_DOCKERFILES=(
  apps/api/Dockerfile
  apps/dispatcher/Dockerfile
  apps/worker/Dockerfile
  apps/mail-adapter/Dockerfile
  adapters/discord/Dockerfile
  runner/Dockerfile
)

# --- Assertion 1: the resolved --no-dev closures -----------------------------
# Written to files first so the closure checker below runs over the same shape
# for the real closures and for the seeded one.
assert_closure_excludes_harness() {
  local label="$1" file="$2"
  local lines
  lines="$(grep -cve '^[[:space:]]*$' "$file" || true)"
  # Guard the guard: an empty export would make the absence check vacuous.
  [ "$lines" -gt 20 ] || fail "$label closure has only $lines lines; the exclusion check is vacuous"
  if grep -qiE "(^|[/[:space:]])(${HARNESS_DISTRIBUTION}|${HARNESS_MODULE})([[:space:]=<>!~]|$)|${HARNESS_PATH}" "$file"; then
    fail "$label installs the dev interaction harness: $(grep -inE "${HARNESS_DISTRIBUTION}|${HARNESS_MODULE}|${HARNESS_PATH}" "$file" | head -3)"
  fi
}

for package in "${APP_PACKAGES[@]}"; do
  NO_COLOR=1 uv export --frozen --no-dev --package "$package" \
    --directory "$REPO_ROOT" >"$TMP/$package.txt" \
    || fail "uv export failed for $package"
  assert_closure_excludes_harness "$package" "$TMP/$package.txt"
done

python3 "$REPO_ROOT/runner/export_dependency_pins.py" <"$REPO_ROOT/uv.lock" >"$TMP/curie-runner.txt" \
  || fail "runner/export_dependency_pins.py failed"
assert_closure_excludes_harness "curie-runner" "$TMP/curie-runner.txt"

# runner/Dockerfile installs by explicit path, so its exclusion is also a
# negative assertion on the Dockerfile itself: an added COPY of the package
# would not show up in the pins above.
if grep -q "$HARNESS_PATH" "$REPO_ROOT/runner/Dockerfile"; then
  fail "runner/Dockerfile references $HARNESS_PATH"
fi
# The `uv export` closure above is NOT the image. App images `COPY . .` into the
# builder and ship that stage's /app/.venv, so a single builder line -- `uv pip
# install ./packages/test-support`, or a post-copy `uv sync` with --no-dev
# dropped -- lands the harness in the shipped venv while the export is
# byte-identical. `grep -q -- "--no-dev" <file>`, which is what this used to be,
# passes on that file: the pre-copy dependency-layer sync still carries the flag.
#
# So posture is judged per INSTRUCTION (continuations joined), with three
# properties: no install verb names a dev-only workspace member; every
# `uv sync` carries --no-dev; and the FINAL sync, the one after `COPY . .`
# whose result is what ships, carries --no-dev and --package.
cat >"$TMP/dockerfile_posture.py" <<'PY'
"""Report dev-harness posture violations in one Dockerfile.

In a file, not inline, so the real Dockerfiles and the seeded violations at the
bottom of this script are judged by exactly the same code.
"""

import pathlib
import re
import shlex
import sys
import tomllib

REPO_ROOT = pathlib.Path(sys.argv[1])
# `uv sync --package <member>` IS an install: it resolves that member into the
# builder venv the runtime stage ships (round-2 review finding A).
INSTALL_VERBS = (
    ("pip", "install"),
    ("pip3", "install"),
    ("uv", "pip", "install"),
    ("uv", "add"),
    ("poetry", "add"),
    ("uv", "sync"),
)
SYNC_VERB = ("uv", "sync")
SHELL_OPERATORS = frozenset({"&&", "||", ";", "|", "&", "(", ")", "{", "}"})
# Flags that put a non-runtime dependency group back into a `--no-dev` sync.
DEV_GROUP_FLAGS = ("--dev", "--all-groups", "--group", "--only-group", "--only-dev")
WORKSPACE_COPY = re.compile(r"COPY\s+\.\s+\./?$", re.IGNORECASE)
HARNESS_PATH = "packages/test-support"


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).strip().lower()


def dev_only_members():
    """Workspace members only the dev dependency group installs -- derived, not listed."""
    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    def names(entries):
        return {
            normalize(re.split(r"[<>=!~\[; ]", entry, maxsplit=1)[0])
            for entry in entries or []
            if isinstance(entry, str)
        }

    runtime = names((root.get("project") or {}).get("dependencies"))
    grouped = set()
    for entries in (root.get("dependency-groups") or {}).values():
        grouped |= names(entries)
    members = {}
    workspace = ((root.get("tool") or {}).get("uv") or {}).get("workspace") or {}
    for relative in workspace.get("members") or []:
        pyproject = REPO_ROOT / relative / "pyproject.toml"
        if not pyproject.is_file():
            continue
        declared = (tomllib.loads(pyproject.read_text()).get("project") or {}).get("name")
        name = normalize(declared) if isinstance(declared, str) else ""
        if name and name in grouped and name not in runtime:
            members[name] = relative
    if not members:
        raise SystemExit("no dev-only workspace member resolved; this checker is vacuous")
    return members


def commands(text):
    """One string per instruction, shell continuations joined, comments dropped."""
    out = []
    buffer = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.lstrip().startswith("#") or (not buffer and not line.strip()):
            continue
        if line.endswith("\\"):
            buffer += line[:-1] + " "
            continue
        out.append(" ".join(f"{buffer}{line}".split()))
        buffer = ""
    if buffer.strip():
        out.append(" ".join(buffer.split()))
    return out


def tokenize(command):
    """Shell tokens of one instruction, end-of-line comments removed (finding B)."""
    try:
        return shlex.split(command, comments=True)
    except ValueError:
        return [token for token in command.split() if not token.startswith("#")]


def segments(command):
    """The shell commands inside one RUN, each as its own token list.

    Splitting on the shell operators is what keeps `|| echo --no-dev` from
    reading as a flag of the sync (finding B, second shape).
    """
    tokens = tokenize(command)
    if not tokens or tokens[0] != "RUN":
        return []
    tokens = tokens[1:]
    while tokens and tokens[0].startswith("--mount"):
        tokens = tokens[1:]
    split = []
    current = []
    for token in tokens:
        if token in SHELL_OPERATORS:
            split.append(current)
            current = []
        else:
            current.append(token)
    split.append(current)
    found = []
    for segment in split:
        while segment and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", segment[0]):
            segment = segment[1:]
        if (
            len(segment) > 2
            and pathlib.PurePath(segment[0]).name in ("python", "python3")
            and segment[1] == "-m"
        ):
            segment = segment[2:]
        if segment:
            found.append([pathlib.PurePath(segment[0]).name, *segment[1:]])
    return found


def starts_with(segment, verb):
    return tuple(segment[: len(verb)]) == verb


def has_flag(segment, flag):
    return any(token == flag or token.startswith(f"{flag}=") for token in segment)


def flag_value(token):
    """The value half of `--flag=value`; the token itself otherwise."""
    if token.startswith("-") and "=" in token:
        return token.split("=", 1)[1]
    return token


def names_path(token, target):
    cleaned = flag_value(token).strip("'\"")
    cleaned = cleaned[2:] if cleaned.startswith("./") else cleaned
    cleaned = cleaned.rstrip("/")
    target = target[2:] if target.startswith("./") else target
    target = target.rstrip("/")
    return bool(target) and (cleaned == target or cleaned.startswith(f"{target}/"))


def names_distribution(token, distribution):
    head = re.split(r"[<>=!~\[;]", flag_value(token).strip("'\""), maxsplit=1)[0]
    return normalize(head) == distribution


def copy_aliases(instructions, member):
    """Destinations a COPY gave the dev-only member's tree (finding C)."""
    aliases = set()
    for command in instructions:
        tokens = tokenize(command)
        if not tokens or tokens[0] != "COPY":
            continue
        operands = [token for token in tokens[1:] if not token.startswith("--")]
        if len(operands) < 2:
            continue
        for source in operands[:-1]:
            if names_path(source, member):
                aliases.add(operands[-1])
    return aliases


def posture(path, text):
    problems = []
    instructions = commands(text)
    parsed = [(command, segments(command)) for command in instructions]
    for distribution, member in dev_only_members().items():
        targets = {member, *copy_aliases(instructions, member)}
        for command, command_segments in parsed:
            for segment in command_segments:
                if not any(starts_with(segment, verb) for verb in INSTALL_VERBS):
                    continue
                if any(
                    any(names_path(token, target) for target in targets)
                    or names_distribution(token, distribution)
                    for token in segment[1:]
                ):
                    problems.append(
                        f"{path} installs the dev-only workspace member {member} "
                        f"({distribution}) into the image venv: {command}"
                    )
    if path == "runner/Dockerfile":
        if HARNESS_PATH in text:
            problems.append(f"{path} references {HARNESS_PATH}")
        return problems
    syncs = [
        (command, segment)
        for command, command_segments in parsed
        for segment in command_segments
        if starts_with(segment, SYNC_VERB)
    ]
    if not syncs:
        problems.append(f"{path} has no uv sync line; the posture assertion would be vacuous")
    for command, segment in syncs:
        if not has_flag(segment, "--no-dev"):
            problems.append(f"{path} installs without --no-dev: {command}")
        for flag in DEV_GROUP_FLAGS:
            if has_flag(segment, flag):
                problems.append(
                    f"{path} re-admits a non-runtime dependency group with {flag}: {command}"
                )
    copies = [i for i, command in enumerate(instructions) if WORKSPACE_COPY.match(command)]
    if not copies:
        problems.append(f"{path} has no `COPY . .`; this checker cannot judge its final layer")
        return problems
    final = [
        (command, segment)
        for command, command_segments in parsed[copies[-1] :]
        for segment in command_segments
        if starts_with(segment, SYNC_VERB)
    ]
    if not final:
        problems.append(f"{path} runs no uv sync after `COPY . .`")
    for command, segment in final:
        if not has_flag(segment, "--no-dev"):
            problems.append(f"{path} post-`COPY . .` sync omits --no-dev: {command}")
        if not has_flag(segment, "--package"):
            problems.append(f"{path} post-`COPY . .` sync omits --package: {command}")
    return problems


found = posture(sys.argv[2], pathlib.Path(sys.argv[3]).read_text())
for problem in found:
    print(problem)
raise SystemExit(1 if found else 0)
PY

for dockerfile in "${PROD_DOCKERFILES[@]}"; do
  python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$REPO_ROOT/$dockerfile" \
    || fail "$dockerfile fails the dev-harness posture check (see above)"
done

# Opportunistic image probe: only where the image already exists locally, so
# this script stays runnable without a five-image build.
if command -v docker >/dev/null 2>&1; then
  for app in api dispatcher worker mail-adapter discord-adapter runner; do
    image="ghcr.io/curie-eng/curie-$app:dev"
    docker image inspect "$image" >/dev/null 2>&1 || continue
    if docker run --rm --entrypoint /app/.venv/bin/python "$image" \
      -c "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('$HARNESS_MODULE') is None else 1)"; then
      echo "  probed $image: $HARNESS_MODULE absent"
    else
      fail "$image has $HARNESS_MODULE importable inside the production venv"
    fi
  done
fi

# --- Assertion 2: the sealed chart render ------------------------------------
cat >"$TMP/sweep.py" <<'PY'
"""Sweep a rendered chart directory for any dev-harness reference.

Kept in a file rather than inline so the real render and the seeded-violation
render below are judged by exactly the same code.
"""

import base64
import pathlib
import sys

import yaml

MODULE = "curie_test_support"
DISTRIBUTION = "curie-test-support"
PACKAGE_PATH = "packages/test-support"
NEEDLES = [MODULE, DISTRIBUTION, PACKAGE_PATH]
ENCODED = {base64.b64encode(needle.encode()).decode(): needle for needle in NEEDLES}


def load(root):
    docs = []
    for path in pathlib.Path(root).rglob("*.yaml"):
        with path.open() as stream:
            docs.extend(doc for doc in yaml.safe_load_all(stream) if doc)
    return docs


def pod_specs(doc):
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        yield spec
    elif kind == "CronJob":
        job = (spec.get("jobTemplate") or {}).get("spec") or {}
        yield (job.get("template") or {}).get("spec") or {}
    elif kind in ("Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"):
        yield (spec.get("template") or {}).get("spec") or {}


docs = load(sys.argv[1])
if not docs:
    raise SystemExit("render produced no documents; the harness sweep is vacuous")

for doc in docs:
    kind = doc.get("kind")
    name = doc.get("metadata", {}).get("name")
    dumped = yaml.safe_dump(doc)
    for needle in NEEDLES:
        if needle in dumped:
            raise SystemExit(f"render {kind}/{name} references the dev harness ({needle})")
    for encoded, needle in ENCODED.items():
        if encoded in dumped:
            raise SystemExit(f"render {kind}/{name} carries base64 {needle}")
    # A base64 `data:` payload hides its plaintext from the sweep above.
    if kind == "Secret":
        for key, value in (doc.get("data") or {}).items():
            try:
                decoded = base64.b64decode(str(value), validate=True).decode("utf-8", "replace")
            except Exception:
                continue
            for needle in NEEDLES:
                if needle in decoded:
                    raise SystemExit(f"Secret {name} data.{key} decodes to {needle}")
    # Every container kind, not containers[0] of a few named workloads: a dev
    # signer or harness sidecar would most plausibly arrive as an initContainer.
    for pod in pod_specs(doc):
        if not pod:
            continue
        containers = []
        for field in ("containers", "initContainers", "ephemeralContainers"):
            containers.extend(pod.get(field) or [])
        for container in containers:
            label = f"{kind}/{name} container {container.get('name')}"
            rendered = yaml.safe_dump(container)
            for needle in NEEDLES:
                if needle in rendered:
                    raise SystemExit(f"{label} references the dev harness ({needle})")
            for entry in container.get("env") or []:
                blob = yaml.safe_dump(entry)
                for needle in NEEDLES:
                    if needle in blob:
                        raise SystemExit(f"{label} env {entry.get('name')} names the dev harness")
PY

sealed_render="$TMP/sealed-render"
mkdir -p "$sealed_render"
helm template curie "$CHART" --output-dir "$sealed_render" \
  --set dispatcher.slack.appToken=xapp-assert \
  --set dispatcher.slack.botToken=xoxb-assert >/dev/null \
  || fail "sealed chart render failed"
python3 "$TMP/sweep.py" "$sealed_render" || fail "sealed render references the dev interaction harness"

# --- Self-check: both controls must reject a seeded violation ----------------
# Without this, a typo'd path or a needle that never matches leaves every
# assertion above passing for the wrong reason.
printf -- '-e ./apps/api\n-e ./%s\n' "$HARNESS_PATH" >"$TMP/seeded-closure.txt"
for i in $(seq 1 40); do echo "filler-package-$i==1.0.0" >>"$TMP/seeded-closure.txt"; done
if (assert_closure_excludes_harness "seeded" "$TMP/seeded-closure.txt") >/dev/null 2>&1; then
  fail "the closure check accepted a closure that installs $HARNESS_DISTRIBUTION"
fi

# The seed above is an editable PATH install, so on its own it only ever proves
# the HARNESS_PATH alternative of the grep fires: a broken DISTRIBUTION/MODULE
# branch would still look green while a real `uv export` line
# `curie-test-support==...` slipped through. Seed that exact shape too, with no
# path anywhere in the file, so the named-requirement branch is proven
# independently.
printf '%s==0.0.0\n' "$HARNESS_DISTRIBUTION" >"$TMP/seeded-named.txt"
for i in $(seq 1 40); do echo "filler-package-$i==1.0.0" >>"$TMP/seeded-named.txt"; done
if grep -q "$HARNESS_PATH" "$TMP/seeded-named.txt"; then
  fail "the named-requirement seed contains $HARNESS_PATH; it cannot prove the distribution branch"
fi
if (assert_closure_excludes_harness "seeded-named" "$TMP/seeded-named.txt") >/dev/null 2>&1; then
  fail "the closure check accepted a closure pinning $HARNESS_DISTRIBUTION==0.0.0"
fi

seeded_render="$TMP/seeded-render"
cp -r "$sealed_render" "$seeded_render"
cat >"$seeded_render/seeded-violation.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: curie-seeded-violation
spec:
  template:
    spec:
      initContainers:
        - name: dev-signer
          image: curie-seeded
          command: ["python", "-m", "${HARNESS_MODULE}.interaction"]
      containers:
        - name: app
          image: curie-seeded
YAML
if python3 "$TMP/sweep.py" "$seeded_render" >/dev/null 2>&1; then
  fail "the render sweep accepted a pod spec running the dev harness"
fi

seeded_sealed_dir="$TMP/seeded-secret"
cp -r "$sealed_render" "$seeded_sealed_dir"
{
  echo "apiVersion: v1"
  echo "kind: Secret"
  echo "metadata:"
  echo "  name: curie-seeded-secret"
  echo "data:"
  echo "  harness: $(printf '%s' "$HARNESS_MODULE" | base64 | tr -d '\n')"
} >"$seeded_sealed_dir/seeded-secret.yaml"
if python3 "$TMP/sweep.py" "$seeded_sealed_dir" >/dev/null 2>&1; then
  fail "the render sweep accepted a Secret whose data decodes to the dev harness"
fi

# The Dockerfile posture checker must reject the two shapes finding 1 named --
# proven by running it, not by reading it. Both seeds leave `uv export` output
# byte-identical, so nothing above this line could ever see them.
for dockerfile in "${PROD_DOCKERFILES[@]}"; do
  # Seed A: a path-install of the dev-only harness in the builder.
  cp "$REPO_ROOT/$dockerfile" "$TMP/seeded-dockerfile"
  printf 'RUN uv pip install ./%s\n' "$HARNESS_PATH" >>"$TMP/seeded-dockerfile"
  if python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$TMP/seeded-dockerfile" \
    >/dev/null 2>&1; then
    fail "the posture checker accepted $dockerfile with a path-install of $HARNESS_PATH"
  fi

  # Seed B: --no-dev dropped from the FINAL, post-`COPY . .` sync only. On the
  # four images with a pre-copy dependency-layer sync the flag is still present
  # in the file, which is exactly what the old `grep -q -- "--no-dev"` read.
  python3 - "$REPO_ROOT/$dockerfile" "$TMP/seeded-final-sync" <<'PY'
import pathlib
import sys

source, destination = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
head, separator, tail = source.read_text().rpartition("uv sync")
if not separator:
    raise SystemExit(0)
destination.write_text(f"{head}{separator}{tail.replace(' --no-dev', '', 1)}")
PY
  if [ -f "$TMP/seeded-final-sync" ]; then
    if python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$TMP/seeded-final-sync" \
      >/dev/null 2>&1; then
      fail "the posture checker accepted $dockerfile with --no-dev dropped from its final sync"
    fi
    rm -f "$TMP/seeded-final-sync"
  fi

  # Seed A (round-2 review): `uv sync --package <dev-only member>`. --no-dev and
  # --package are both present and `uv export` is byte-identical, so assertion 1
  # and the old flag checks all read clean while the member lands in the venv.
  cp "$REPO_ROOT/$dockerfile" "$TMP/seeded-dockerfile"
  printf 'RUN uv sync --frozen --no-dev --no-editable --package curie-api --package %s\n' \
    "$HARNESS_DISTRIBUTION" >>"$TMP/seeded-dockerfile"
  if python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$TMP/seeded-dockerfile" \
    >/dev/null 2>&1; then
    fail "the posture checker accepted $dockerfile with a uv sync --package $HARNESS_DISTRIBUTION"
  fi

  # Seed C (round-2 review): the harness COPYed to a renamed path and installed
  # from there. The RUN names neither the member path nor the distribution.
  cp "$REPO_ROOT/$dockerfile" "$TMP/seeded-dockerfile"
  printf 'COPY %s /opt/vendored\nRUN uv pip install /opt/vendored\n' \
    "$HARNESS_PATH" >>"$TMP/seeded-dockerfile"
  if python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$TMP/seeded-dockerfile" \
    >/dev/null 2>&1; then
    fail "the posture checker accepted $dockerfile installing the harness from a renamed path"
  fi

  # runner/Dockerfile runs no uv sync, so the sync-flag seeds below do not apply.
  if [ "$dockerfile" = "runner/Dockerfile" ]; then
    continue
  fi

  # Seed B (round-2 review): a --no-dev uv never receives. Both shapes satisfy a
  # substring match on the joined instruction; neither reaches uv.
  for decoy in '# --no-dev' '|| echo --no-dev'; do
    python3 - "$REPO_ROOT/$dockerfile" "$TMP/seeded-decoy" "$decoy" <<'SEED_B'
import pathlib
import sys

source, destination, decoy = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
head, separator, tail = source.read_text().rpartition("uv sync")
if not separator:
    raise SystemExit(0)
# Only the REST OF THAT LINE is rewritten: appending at EOF would leave the
# real sync untouched and the seed would prove nothing.
line, newline, rest = tail.partition("\n")
mutated = f"{head}{separator}{line.replace(' --no-dev', '', 1)} {decoy}{newline}{rest}"
if "--no-dev" not in mutated.rpartition("uv sync")[2].partition("\n")[0]:
    raise SystemExit("the decoy seed lost its --no-dev text; it proves nothing")
destination.write_text(mutated)
SEED_B
    if [ -f "$TMP/seeded-decoy" ]; then
      if python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$TMP/seeded-decoy" \
        >/dev/null 2>&1; then
        fail "the posture checker accepted $dockerfile whose final sync only says '$decoy'"
      fi
      rm -f "$TMP/seeded-decoy"
    fi
  done

  # Seed D (found while closing A-C): --no-dev is present and honest, and a
  # later --group dev puts the dev group straight back.
  cp "$REPO_ROOT/$dockerfile" "$TMP/seeded-dockerfile"
  printf 'RUN uv sync --frozen --no-dev --group dev --no-editable --package curie-api\n' \
    >>"$TMP/seeded-dockerfile"
  if python3 "$TMP/dockerfile_posture.py" "$REPO_ROOT" "$dockerfile" "$TMP/seeded-dockerfile" \
    >/dev/null 2>&1; then
    fail "the posture checker accepted $dockerfile re-admitting the dev group with --group dev"
  fi
done

echo "OK: dev interaction harness exclusion assertions passed"

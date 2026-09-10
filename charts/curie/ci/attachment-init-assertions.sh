#!/usr/bin/env bash
#
# Render-assertion test for the inbound-attachment init container (#2567, S4).
#
# The worker parks a person's uploaded file in the private object store and
# mints a short-lived one-object read capability. `attachments-init` is the only
# thing in the cluster that redeems it, and the whole feature is invisible if it
# is absent, unmounted, or handed nothing: the pod boots healthy and the agent
# says it cannot see a file the person can see in the thread. So the presence,
# the ORDER, the mount and the env delivery are each asserted here rather than
# left to be discovered by a human reading a rendered manifest.
#
# Four properties are load-bearing and each has a reason it is not merely style:
#
#  1. The reference reaches ONLY the init container. It is a presigned URL, and
#     the runner container runs prompt-injectable model code that can echo any
#     plain env var into a channel. Same reasoning as
#     apps/worker/tests/sandbox/test_k8s_claim.py's workspace-capability test,
#     one layer down: the claim scopes the env by containerName, and the pod
#     template must not hand it out again unscoped.
#
#  2. The sha256 is VERIFIED. Without it the "signed one-object capability" is
#     just a URL, and whatever answers it becomes the attachment the agent
#     reads. The digest is minted from the bytes that actually landed in the
#     store (see AttachmentRef), so it is the only thing in the pod that can
#     tell those bytes from any others.
#
#  3. The size cap is enforced, and it AGREES with the worker's. The worker
#     refuses a file over its cap while streaming; an init container with a
#     smaller cap silently drops files the worker accepted, and one with no cap
#     at all lets a hostile store fill the pod's emptyDir. Presence alone would
#     pass the day the two numbers diverge, so this asserts equality against
#     the worker Deployment's rendered env.
#
#  4. Switching the lane off leaves the sandbox pod exactly as it is today: no
#     init container, no volume, no mount. The feature has to be absent when
#     absent, not present-and-empty.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="${CHART:-$(cd "$SCRIPT_DIR/.." && pwd)}"

# Capture Helm output in Python so a large manifest cannot be truncated by a
# shell command substitution.
python3 - "$CHART" <<'PY'
import copy
import io
import re
import subprocess
import sys
from contextlib import redirect_stdout

import yaml

CHART = sys.argv[1]

INIT_NAME = "attachments-init"
REF_ENV = "CURIE_ATTACHMENTS_REF"
WORKER_CAP_ENV = "CURIE_ATTACHMENT_MAX_FILE_BYTES"
WORKER_TTL_ENVS = (
    "CURIE_ATTACHMENT_REFERENCE_TTL_SECONDS",
    "CURIE_ATTACHMENT_RETENTION_TTL_SECONDS",
)


def render(*sets):
    command = ["helm", "template", "curie", CHART, "--namespace", "dev"]
    for assignment in sets:
        command += ["--set", assignment]
    try:
        rendered = subprocess.run(command, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as error:
        print("FAIL: helm template could not render the chart", file=sys.stderr)
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        raise SystemExit(error.returncode)
    return [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]


def sandbox_template(docs):
    """The generic runner SandboxTemplate, selected by its component label.

    The label is read off the object, falling back to the pod template, for the
    same reason worker-object-store-assertions.sh does it: the SandboxTemplate
    labels the two differently (`agent-sandbox` on the object, `runner-sandbox`
    on the pod) and a name suffix match would also select a per-agent template.
    """

    for doc in docs:
        if doc.get("kind") != "SandboxTemplate":
            continue
        pod = doc["spec"]["podTemplate"]
        labelled = doc["metadata"].get("labels", {}).get(
            "app.kubernetes.io/component"
        ) or pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
        if labelled == "agent-sandbox":
            return doc
    raise SystemExit("FAIL: no generic SandboxTemplate rendered")


def pod_spec(docs):
    return sandbox_template(docs)["spec"]["podTemplate"]["spec"]


def worker_env(docs):
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        if doc["metadata"].get("labels", {}).get("app.kubernetes.io/component") != "worker":
            continue
        containers = doc["spec"]["template"]["spec"]["containers"]
        worker = next(c for c in containers if c["name"] == "worker")
        return {entry["name"]: entry for entry in worker.get("env", [])}
    raise SystemExit("FAIL: no curie worker Deployment rendered")


def named(containers, name):
    matches = [c for c in containers if c.get("name") == name]
    if len(matches) > 1:
        raise SystemExit(f"FAIL: {len(matches)} containers named {name!r}")
    return matches[0] if matches else None


def env_names(container):
    return {entry["name"] for entry in (container or {}).get("env", [])}


def mount_paths(container):
    return {
        mount["name"]: mount["mountPath"]
        for mount in (container or {}).get("volumeMounts", [])
    }


# Numeric literals, matched so a digit inside an identifier is not one. The
# comparison below has to be NUMERIC: helm renders a large YAML integer through
# `{{ .Values... }}` as `2.68435456e+08` (the existing workspace-init shows the
# same shape), which python reads as the right number and a string compare
# against "268435456" does not.
_NUMBER = re.compile(r"(?<![\w.])\d[\d_]*(?:\.\d+)?(?:[eE][+-]?\d+)?")


def numeric_literals(script):
    found = {}
    for raw in _NUMBER.findall(script):
        try:
            found.setdefault(float(raw.replace("_", "")), raw)
        except ValueError:
            continue
    return found


def script_of(container):
    """The init container's program, as one string.

    Read from the whole command list rather than a fixed index so a
    `sh -c` wrapper, a `python -c` body, or an argv reshuffle is all the same
    to the digest/cap assertions below.
    """

    return "\n".join(str(part) for part in (container or {}).get("command", []))


def assert_contract(docs, label="default"):
    failures = []
    spec = pod_spec(docs)
    inits = spec.get("initContainers") or []
    mains = spec.get("containers") or []

    init = named(inits, INIT_NAME)
    runner = named(mains, "runner")
    if runner is None:
        raise SystemExit("FAIL: the SandboxTemplate renders no runner container")

    if init is None:
        failures.append(
            f"the sandbox pod renders no {INIT_NAME!r} init container. Nothing in "
            "the cluster then redeems the worker's minted capability, and the "
            "agent reports it cannot see an attachment the person can see."
        )
        report(label, failures, init, runner, spec, docs)
        return

    # (1) It is an INIT container, so it completes before the runner starts. A
    # sidecar would race the first turn: the agent could read a half-written
    # file, or none at all, and either looks like a model failure.
    if named(mains, INIT_NAME) is not None:
        failures.append(f"{INIT_NAME} is also a main container; it must run to completion first")

    # Ordered after workspace-init, which deletes every child of its own root on
    # entry. The two use separate volumes today, and this keeps that a choice
    # rather than a coincidence a reorder could silently break.
    order = [c.get("name") for c in inits]
    if "workspace-init" in order and order.index(INIT_NAME) < order.index("workspace-init"):
        failures.append(
            f"{INIT_NAME} runs before workspace-init, which wipes its own root on entry"
        )

    # (2) The shared volume: the init container writes it and the runner reads
    # it, at the SAME path, or the agent is told about a path nothing mounted.
    init_mounts = mount_paths(init)
    runner_mounts = mount_paths(runner)
    shared = set(init_mounts) & set(runner_mounts)
    attachment_volumes = {
        name for name in shared if init_mounts[name] == runner_mounts[name]
    }
    # Identify the attachment volume as the one the init container writes that
    # is not the bundle or workspace volume.
    candidates = sorted(attachment_volumes - {"bundles", "workspace", "aws-config"})
    if not candidates:
        failures.append(
            f"{INIT_NAME} and the runner share no volume at a common path: "
            f"init={init_mounts} runner={runner_mounts}. Files materialized by "
            "the init container are then unreachable from the session."
        )
    else:
        volume_name = candidates[0]
        volumes = {v["name"]: v for v in (spec.get("volumes") or [])}
        volume = volumes.get(volume_name)
        if volume is None:
            failures.append(f"volume {volume_name!r} is mounted but never declared")
        elif "emptyDir" not in volume:
            failures.append(f"volume {volume_name!r} is not an emptyDir")
        elif not (volume.get("emptyDir") or {}).get("sizeLimit"):
            failures.append(
                f"emptyDir {volume_name!r} has no sizeLimit (ADR-0059 decision 2 "
                "requires one on every writable emptyDir in the sandbox pod)"
            )

    # (3) The capability is delivered to the init container and NOWHERE else.
    if REF_ENV not in env_names(init):
        failures.append(
            f"{INIT_NAME} has no {REF_ENV} entry. The claim's Overrides injection "
            "only wins over env the pod template already declares, so an absent "
            "baked entry means the reference never arrives."
        )
    for container in inits + mains:
        if container.get("name") == INIT_NAME:
            continue
        if REF_ENV in env_names(container):
            failures.append(
                f"{container.get('name')} also receives {REF_ENV}. It is a presigned "
                "URL; the runner runs prompt-injectable model code that can echo a "
                "plain env var into a channel."
            )

    # (4) The digest is verified, not merely computed.
    script = script_of(init)
    if "sha256" not in script:
        failures.append(
            f"{INIT_NAME} never mentions sha256: the minted digest is not verified, "
            "so whatever answers the URL becomes the file the agent reads"
        )
    elif not any(token in script for token in ("hexdigest", "digest()")):
        failures.append(
            f"{INIT_NAME} computes no digest of the bytes it fetched (no hexdigest); "
            "a digest never computed cannot be compared"
        )
    elif "!=" not in script and "==" not in script:
        failures.append(
            f"{INIT_NAME} computes a digest but never compares it against the "
            "reference; the verification is a no-op"
        )

    # (5) The size cap is present AND agrees with the worker's own.
    env = worker_env(docs)
    cap_entry = env.get(WORKER_CAP_ENV)
    if cap_entry is None or not cap_entry.get("value"):
        failures.append(
            f"the worker Deployment has no {WORKER_CAP_ENV}; the chart's new "
            "attachment values are not threaded to the process that enforces them"
        )
    else:
        cap = float(str(cap_entry["value"]))
        if cap not in numeric_literals(script):
            failures.append(
                f"{INIT_NAME} does not enforce the worker's cap of {cap:.0f} bytes. A "
                "smaller cap silently drops files the worker accepted; no cap lets "
                "a hostile store fill the pod's emptyDir."
            )
    for name in WORKER_TTL_ENVS:
        entry = env.get(name)
        if entry is None or not entry.get("value"):
            failures.append(f"the worker Deployment has no {name}")

    report(label, failures, init, runner, spec, docs)


def report(label, failures, init, runner, spec, docs):
    if failures:
        print(f"FAIL: {label}: the attachment init contract is not satisfied\n")
        for failure in failures:
            print("  - " + failure)
        raise SystemExit(1)
    order = [c.get("name") for c in (spec.get("initContainers") or [])]
    print(f"ok: {label}: init order {order} -> runner")
    print(f"      {INIT_NAME} env      = {sorted(env_names(init))}")
    print(f"      {INIT_NAME} mounts   = {mount_paths(init)}")
    print(f"      runner mounts        = {mount_paths(runner)}")


def expect_failure(docs, messages, label="probe"):
    captured = io.StringIO()
    try:
        with redirect_stdout(captured):
            assert_contract(docs, label)
    except SystemExit as error:
        assert error.code == 1, f"probe exited {error.code}, expected 1"
    else:
        raise AssertionError("the attachment init contract probe unexpectedly passed")
    output = captured.getvalue()
    for message in messages:
        assert message in output, f"missing diagnostic {message!r} in:\n{output}"


default_docs = render()
assert_contract(default_docs, "default")

# --- mutation honesty: each property, removed, must fail the gate above -----
#
# Without these the assertions could be satisfied by a manifest that merely
# mentions the right words, and a future refactor could weaken one of them
# without this file noticing.

no_init = copy.deepcopy(default_docs)
spec = pod_spec(no_init)
spec["initContainers"] = [
    c for c in spec["initContainers"] if c.get("name") != INIT_NAME
]
expect_failure(no_init, (f"renders no {INIT_NAME!r} init container",), "no-init probe")

leaked = copy.deepcopy(default_docs)
runner = named(pod_spec(leaked)["containers"], "runner")
runner.setdefault("env", []).append({"name": REF_ENV, "value": ""})
expect_failure(leaked, (f"runner also receives {REF_ENV}",), "leaked-ref probe")

no_digest = copy.deepcopy(default_docs)
init = named(pod_spec(no_digest)["initContainers"], INIT_NAME)
init["command"] = [part.replace("sha256", "nodigest") for part in init["command"]]
expect_failure(no_digest, ("never mentions sha256",), "no-digest probe")

no_cap = copy.deepcopy(default_docs)
init = named(pod_spec(no_cap)["initContainers"], INIT_NAME)
cap = float(str(worker_env(no_cap)[WORKER_CAP_ENV]["value"]))
rendered_cap = numeric_literals(script_of(init))[cap]
init["command"] = [
    str(part).replace(rendered_cap, "999999999999") for part in init["command"]
]
expect_failure(no_cap, ("does not enforce the worker's cap",), "no-cap probe")

unmounted = copy.deepcopy(default_docs)
runner = named(pod_spec(unmounted)["containers"], "runner")
runner["volumeMounts"] = [
    m for m in runner.get("volumeMounts", []) if m["name"] in {"bundles", "workspace"}
]
expect_failure(unmounted, ("share no volume at a common path",), "unmounted probe")

unbounded = copy.deepcopy(default_docs)
spec = pod_spec(unbounded)
init = named(spec["initContainers"], INIT_NAME)
runner = named(spec["containers"], "runner")
shared = {m["name"] for m in init.get("volumeMounts", [])} & {
    m["name"] for m in runner.get("volumeMounts", [])
}
target = sorted(shared - {"bundles", "workspace", "aws-config"})[0]
for volume in spec["volumes"]:
    if volume["name"] == target:
        volume["emptyDir"].pop("sizeLimit", None)
expect_failure(unbounded, ("has no sizeLimit",), "unbounded-emptydir probe")

print("ok: removing the init container, the digest check, the cap, the mount,")
print("    the emptyDir sizeLimit, or scoping the reference to the runner each")
print("    fails the contract above")

# --- switched off leaves the pod exactly as it is today ---------------------

off_docs = render("agentSandbox.runner.attachments.enabled=false")
off_spec = pod_spec(off_docs)
off_inits = off_spec.get("initContainers") or []
off_mains = off_spec.get("containers") or []
assert named(off_inits, INIT_NAME) is None, (
    "attachments.enabled=false still renders the init container"
)
for container in off_inits + off_mains:
    assert REF_ENV not in env_names(container), (
        f"attachments.enabled=false still hands {REF_ENV} to {container.get('name')}"
    )

on_spec = pod_spec(default_docs)
on_volume_names = {v["name"] for v in (on_spec.get("volumes") or [])}
off_volume_names = {v["name"] for v in (off_spec.get("volumes") or [])}
new_volumes = on_volume_names - off_volume_names
assert new_volumes, "enabling attachments adds no volume, so nothing is materialized"

on_runner_mounts = mount_paths(named(on_spec["containers"], "runner"))
off_runner_mounts = mount_paths(named(off_mains, "runner"))
assert set(off_runner_mounts) == set(on_runner_mounts) - new_volumes, (
    "attachments.enabled=false leaves an attachment mount on the runner: "
    f"off={off_runner_mounts} on={on_runner_mounts}"
)

print(f"ok: attachments.enabled=false renders no {INIT_NAME}, no {sorted(new_volumes)}")
print("    volume and no runner mount -- the sandbox pod is what it is today")

print()
print(
    "PASS: attachments-init runs before the runner, shares one size-limited "
    "emptyDir with it, receives the presigned reference that the runner never "
    "sees, verifies the minted sha256, enforces the worker's own size cap, and "
    "disappears entirely when the lane is switched off."
)
PY

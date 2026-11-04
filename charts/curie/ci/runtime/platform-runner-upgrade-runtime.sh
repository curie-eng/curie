#!/usr/bin/env bash
# #4767: deployed platform-runner agents survive both upgrade entry points.
# fix-pin-provision: candidate-stack
set +x
set -euo pipefail
umask 077
export CURIE_UPGRADE_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
python3 - <<'PYTHON'
import base64
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request

import yaml


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def interrupted(_signum, _frame):
    raise InterruptedError("scenario interrupted")


signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)


root = Path(os.environ["CURIE_UPGRADE_REPO_ROOT"])
for name in ("CURIE_BIN", "CURIE_SCENARIO_CONTEXT", "CURIE_SCENARIO_RUN_ID",
             "CURIE_SCENARIO_API_IMAGE", "CURIE_SCENARIO_WORKER_IMAGE", "CURIE_SCENARIO_RUNNER_IMAGE"):
    require(bool(os.environ.get(name)), f"{name} is required")
context = os.environ["CURIE_SCENARIO_CONTEXT"]
run_id = os.environ["CURIE_SCENARIO_RUN_ID"]
require(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,24}", run_id), "invalid caller run ID")
require(not context.startswith("-") and not re.search(r"[\s\x00-\x1f\x7f]", context), "invalid caller context")
binary = str(Path(os.environ["CURIE_BIN"]).resolve())
require(os.access(binary, os.X_OK), "candidate CLI is unavailable")
for name in ("helm", "kubectl", "python3"):
    require(shutil.which(name), f"{name} is required")
images = {part: os.environ[f"CURIE_SCENARIO_{part.upper()}_IMAGE"] for part in ("api", "worker", "runner")}
for image in images.values():
    require(re.fullmatch(r"[a-z0-9][a-z0-9./:_-]*:[A-Za-z0-9_.-]+", image), "invalid candidate image")
env = os.environ.copy()
for name in ("CURIE_API_URL", "CURIE_API_KEY", "CURIE_VALKEY_PASSWORD", "CURIE_NAMESPACE", "CURIE_STREAM",
             "CURIE_CREDENTIALS", "CURIE_MODEL_CREDENTIALS", "CURIE_MODEL", "CURIE_MODEL_BASE_URL",
             "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "OPENAI_API_KEY",
             "CURIE_GITHUB_TOKEN", "GITHUB_TOKEN", "GH_TOKEN", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN",
             "CURIE_FAKE_MODEL", "CURIE_FAKE_SCRIPT", "CURIE_FAKE_SCRIPT_FILE"):
    env.pop(name, None)
work = Path(tempfile.mkdtemp(prefix="curie-platform-runner-upgrade-"))
env["CURIE_CONFIG_DIR"] = str(work / "config")
diagnostic_redactions = set()


def run(argv, description, *, stdin=None, timeout=120, check=True):
    # CLI output may carry generated credentials. Retain it privately; never replay it.
    process = subprocess.Popen([str(arg) for arg in argv], stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, start_new_session=True)
    try:
        stdout, stderr = process.communicate(stdin, timeout=timeout)
    except BaseException:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=10)
        raise
    result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    if check and result.returncode:
        try:
            payload = json.loads(stdout)
            detail = payload.get("fail_forward", {}).get("reason") if isinstance(payload.get("fail_forward"), dict) else payload.get("error", "")
            detail = str(detail)
            categories = [word for word in ("schema", "forward-only", "runner", "canary", "compatibility", "chart", "hook", "drain", "template", "ownership") if word in detail.lower()]
            print("FAIL categories: " + ", ".join(categories), flush=True)
            detail = re.sub(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s]+", "[redacted URL]", detail)
            detail = re.sub(r"/(?:home|tmp|var|Users)/[^\s]+", "[redacted path]", detail)
            for value in sorted(diagnostic_redactions, key=len, reverse=True):
                if value:
                    detail = detail.replace(value, "[redacted]")
            print("FAIL detail: " + detail[:1800], flush=True)
        except (ValueError, AttributeError):
            pass
        raise RuntimeError(f"{description} failed (exit {result.returncode}); output withheld because it may contain credentials")
    return result


def kc(namespace, *args, **kwargs):
    return run(["kubectl", "--context", context, "-n", namespace, *args], "kubectl " + args[0], **kwargs)


def read(namespace, resource):
    return json.loads(kc(namespace, "get", resource, "-o", "json").stdout)


def cli(executable, namespace, release, chart, *args, **kwargs):
    return run([executable, "--json", "cluster", "--context", context, *args,
                "--namespace", namespace, "--release", release, "--chart", chart], "cluster " + args[0], **kwargs)


def patch(namespace, kind, name, value):
    kc(namespace, "patch", kind, name, "--type=merge", "-p", json.dumps(value))


def clean(namespace, release):
    worker = kc(namespace, "get", "deployment/" + release + "-curie-worker", "--ignore-not-found", "-o", "name").stdout.strip()
    if worker:
        kc(namespace, "scale", worker, "--replicas=0")
        kc(namespace, "delete", "pods", "-l",
           f"app.kubernetes.io/instance={release},app.kubernetes.io/component=worker",
           "--grace-period=1", "--wait=true", "--timeout=120s", timeout=140)
    for kind in ("sandboxwarmpools", "sandboxclaims", "sandboxes"):
        kc(namespace, "delete", kind, "--all", "--ignore-not-found", "--wait=true", "--timeout=120s", timeout=140)
    run(["helm", "--kube-context", context, "-n", namespace, "uninstall", release,
         "--ignore-not-found", "--wait", "--timeout", "120s"], "owned release uninstall", timeout=150)
    kc(namespace, "delete", "namespace", namespace, "--ignore-not-found", "--wait=true", "--timeout=180s", timeout=200)
    # Chart cluster objects outlive Helm when annotated keep. Delete only this release's objects.
    for kind in ("clusterroles", "clusterrolebindings", "validatingadmissionpolicies", "validatingadmissionpolicybindings", "priorityclasses"):
        for item in read(namespace, kind)["items"]:
            annotations = item["metadata"].get("annotations", {})
            if annotations.get("meta.helm.sh/release-name") == release and annotations.get("meta.helm.sh/release-namespace") == namespace:
                name = item["metadata"]["name"]
                kc(namespace, "delete", kind, name, "--wait=true", "--timeout=60s", timeout=80)
                require(not kc(namespace, "get", kind, name, "--ignore-not-found", "-o", "name").stdout.strip(),
                        "owned cluster object remains")
    controller_ns = json.loads(kc(namespace, "get", "namespace/agent-sandbox-system", "--ignore-not-found", "-o", "json").stdout or "null")
    if controller_ns and controller_ns["metadata"].get("annotations", {}).get("meta.helm.sh/release-name") == release:
        kc(namespace, "delete", "namespace/agent-sandbox-system", "--wait=true", "--timeout=90s", timeout=110)
    require(not kc(namespace, "get", "namespace", namespace, "--ignore-not-found", "-o", "name").stdout.strip(),
            "owned namespace remains")
    print("OK: owned release, namespace, PVCs and cluster objects removed", flush=True)


def turn(executable, namespace, release, chart, expected, since, runner_slots):
    thread = f"{time.time_ns() / 1e9:.6f}"
    started = time.monotonic()
    result = cli(executable, namespace, release, chart, "message", "--agent", "acme-bot", "--channel", "C0EXAMPLE1",
                 "--thread", thread, "--timeout-secs", "180", "Confirm the upgrade probe.", timeout=195)
    body = json.loads(result.stdout)
    require(body.get("finalized") is True, "cold turn did not finalize")
    require(time.monotonic() - started <= 180, "cold turn exceeded 180 seconds")
    pods = read(namespace, "pods")["items"]
    runners = [pod for pod in pods if pod["metadata"]["creationTimestamp"] >= since
               and any(c.get("name") == "runner" for c in pod["spec"]["containers"])]
    require(bool(runners), "no new runner pod observed")
    for pod in runners:
        for kind, name in runner_slots:
            container = next(c for c in pod["spec"].get(kind, []) if c["name"] == name)
            require(container["image"] == expected, f"new pod {kind} {name} used a forbidden runner image")
        require(any(c["name"] == "runner" and c.get("imageID") for c in pod["status"].get("containerStatuses", [])),
                "new runner never started its image")
    print("OK: a new cold turn finalized on the expected runner within 180 seconds", flush=True)


def snapshot(namespace, fullname):
    primary = read(namespace, "secret/" + fullname + "-secrets")
    owner = primary["metadata"]["annotations"]["meta.helm.sh/release-name"]
    credentials = {}
    for secret in read(namespace, "secrets")["items"]:
        if secret["metadata"].get("annotations", {}).get("meta.helm.sh/release-name") != owner:
            continue
        for key, value in secret.get("data", {}).items():
            if key == "installationId":
                continue
            credentials[secret["metadata"]["name"] + "/" + key] = hashlib.sha256(value.encode()).hexdigest()
            diagnostic_redactions.add(value)
            diagnostic_redactions.add(base64.b64decode(value).decode(errors="replace"))
    require(bool(credentials), "release has no recorded credential hashes")
    return {"credentials": credentials,
            "pvcs": {item["metadata"]["name"]: item["metadata"]["uid"] for item in read(namespace, "pvc")["items"]}}


def scenario(entry, baseline, old_chart, chart, version):
    namespace = f"test-4767-{entry}-{run_id}"
    release = f"acme-{entry}-{run_id}"
    fullname = release + "-curie"
    require(not kc(namespace, "get", "namespace", namespace, "--ignore-not-found", "-o", "name").stdout.strip(),
            "scenario namespace already exists")
    owned = False
    try:
        owned = True
        kc(namespace, "create", "namespace", namespace)
        values = {
            "security": {"gvisor": {"mode": "off"}},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False}, "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "ui": {"deploy": False}, "dispatcher": {"deploy": False}, "mailAdapter": {"deploy": False},
            "worker": {"replicas": 1}, "resourceQuota": {"enabled": False},
            "priorityClasses": {kind: {"create": True, "name": f"{release}-{kind}"} for kind in ("platform", "sandbox")},
            "agentSandbox": {"controller": {"deploy": not bool(kc("agent-sandbox-system", "get", "deployment/agent-sandbox-controller", "--ignore-not-found", "-o", "name").stdout.strip())}, "platformPool": {"replicas": 0},
                             "poolAgents": ["acme-layer"],
                             "runnerImages": {"acme-layer": "ghcr.io/curie-eng/curie-runner@sha256:3b69e0ab26c8d717306b0214a34575a17fdb53d7352412140d175f088c68b4bc"},
                             "runner": {"fakeModel": True, "prewarm": {"enabled": True}}},
        }
        values_path = work / f"{entry}-values.yaml"
        values_path.write_text(yaml.safe_dump(values))
        cli(baseline, namespace, release, old_chart, "up", "--fake-model", "--no-expose",
            "--values-file", values_path, timeout=900)
        print("OK: published v0.13.0 baseline installed", flush=True)
        bundle = work / f"{entry}-bundle"
        run([baseline, "example", "sre-bot", "render", "--out", bundle,
             "--namespace", namespace, "--release", release], "render maintained baseline bundle", timeout=180)
        kc(namespace, "create", "secret", "generic", "curie-grafana-connector",
           "--from-literal=GRAFANA_SERVICE_ACCOUNT_TOKEN=acme-test-token")
        kc(namespace, "create", "serviceaccount", "acme-probe")
        token = kc(namespace, "create", "token", "acme-probe").stdout.strip()
        diagnostic_redactions.add(token)
        env["K8S_KUBECONFIG"] = yaml.safe_dump({"apiVersion": "v1", "kind": "Config", "current-context": "probe",
            "clusters": [{"name": "probe", "cluster": {"server": "https://kubernetes.default.svc", "insecure-skip-tls-verify": True}}],
            "contexts": [{"name": "probe", "context": {"cluster": "probe", "user": "probe"}}],
            "users": [{"name": "probe", "user": {"token": token}}]})
        # Use the real API and its machine identity to bind the maintained bundle's approval route.
        kc(namespace, "exec", "-i", "deployment/" + fullname + "-api", "--", "python3", "-c", """
import json,os,urllib.request
payload={"name":"acme-bot","channel":{"kind":"slack","address":"C0EXAMPLE1"},
         "approval_routes":{"sre-approvals":{"resolution":{"kind":"slack","address":"C0EXAMPLE1"},"approvers":{"users":["U0EXAMPLE1"]}}}}
request=urllib.request.Request("http://127.0.0.1:8000/agents", data=json.dumps(payload).encode(),
    headers={"X-API-Key":os.environ["API_KEY"],"Content-Type":"application/json"},method="POST")
with urllib.request.urlopen(request,timeout=30) as response:
    assert response.status == 201
""")
        cli(baseline, namespace, release, old_chart, "deploy", "--plugin-dir", bundle,
            "--agent", "acme-bot", "--env", "dev", "--slack-channel", "C0EXAMPLE1", timeout=300)
        source_name = fullname + "-agent-acme-bot-runner"
        source = read(namespace, "sandboxtemplate/" + source_name)
        require(not source["metadata"].get("annotations", {}).get("meta.helm.sh/release-name"),
                "baseline deploy did not create a CLI-owned template")
        spec = source["spec"]["podTemplate"]["spec"]
        old_image = next(c["image"] for c in spec["containers"] if c["name"] == "runner")
        runner_slots = [(kind, container["name"]) for kind in ("containers", "initContainers")
                        for container in spec.get(kind, []) if container["image"] == old_image]
        require(("initContainers", "workspace-init") in runner_slots,
                "published baseline is missing its runner-backed workspace init container")
        extra_secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "acme-runner-env"},
                        "stringData": {"ACME_UPGRADE_SENTINEL": "preserved"}}
        kc(namespace, "create", "-f", "-", stdin=json.dumps(extra_secret))
        for container in spec["containers"]:
            if container["name"] == "runner":
                container.setdefault("envFrom", []).append({"secretRef": {"name": "acme-runner-env"}})
                container["resources"]["requests"]["memory"] = "256Mi"
                container["resources"]["limits"]["memory"] = "768Mi"
        patch(namespace, "sandboxtemplate", source_name, {"spec": source["spec"]})
        source = read(namespace, "sandboxtemplate/" + source_name)
        turn(baseline, namespace, release, old_chart, old_image, "1970-01-01T00:00:00Z", runner_slots)
        layer_name = fullname + "-agent-acme-layer-runner"
        layer_before = read(namespace, "sandboxtemplate/" + layer_name)
        before = snapshot(namespace, fullname)
        foreign = copy.deepcopy(source)
        foreign_name = fullname + "-agent-acme-unrelated-runner"
        foreign["metadata"] = {"name": foreign_name, "labels": {"app.kubernetes.io/instance": "other-release"}}
        foreign["spec"]["podTemplate"]["metadata"]["labels"]["curietech.ai/agent"] = "acme-unrelated"
        kc(namespace, "create", "-f", "-", stdin=json.dumps(foreign))
        foreign_before = read(namespace, "sandboxtemplate/" + foreign_name)
        pool_before = read(namespace, "sandboxwarmpool/" + source_name + "-pool")["spec"]
        for part in ("api", "worker"):
            repository, tag = images[part].rsplit(":", 1)
            values.setdefault(part, {})["image"] = {"repository": repository, "tag": tag, "digest": "", "pullPolicy": "Never"}
        # Preserve the baseline layer in retained values; the dedicated lifecycle owns its clear.
        values["agentSandbox"].pop("runnerImages")
        repository, tag = images["runner"].rsplit(":", 1)
        values["agentSandbox"]["runner"].update({"image": repository, "tag": tag, "digest": "", "imagePullPolicy": "Never"})
        values["agentSandbox"]["runner"]["prewarm"]["imagePullPolicy"] = "Never"
        values_path.write_text(yaml.safe_dump(values))
        since = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        if entry == "upgrade":
            result = cli(binary, namespace, release, chart, "upgrade", "--to", version,
                         "--values-file", values_path, "--yes", timeout=900)
            body = json.loads(result.stdout)
            require(body["status"] == "succeeded" and body["canary"]["passed"], "upgrade did not pass its canary")
        else:
            cli(binary, namespace, release, chart, "up", "--fake-model", "--no-expose",
                "--values-file", values_path, timeout=900)
        retained = snapshot(namespace, fullname)
        for kind in ("credentials", "pvcs"):
            changed = [key for key, value in before[kind].items() if retained[kind].get(key) != value]
            require(not changed, "pre-existing " + kind + " changed: " + ", ".join(changed))
        layer_after = read(namespace, "sandboxtemplate/" + layer_name)
        expected_layer = images["runner"] if entry == "upgrade" else layer_before["spec"]["podTemplate"]["spec"]["containers"][0]["image"]
        require(layer_after["spec"]["podTemplate"]["spec"]["containers"][0]["image"] == expected_layer,
                "existing layered-runner upgrade behavior changed")
        after = read(namespace, "sandboxtemplate/" + source_name)
        expected_spec = copy.deepcopy(source["spec"])
        for kind, name in runner_slots:
            container = next(c for c in expected_spec["podTemplate"]["spec"][kind] if c["name"] == name)
            container["image"] = images["runner"]
        require(after["spec"] == expected_spec and after["metadata"]["labels"] == source["metadata"]["labels"],
                "deployment-created template retained a stale runner app/init image or lost agent settings")
        require(read(namespace, "sandboxwarmpool/" + source_name + "-pool")["spec"] == pool_before, "pool selection changed")
        require(read(namespace, "sandboxtemplate/" + foreign_name) == foreign_before, "unrelated template changed")
        kc(namespace, "delete", "sandboxclaims", "-l", "curietech.ai/agent=acme-bot", "--wait=true", "--timeout=90s")
        turn(binary, namespace, release, chart, images["runner"], since, runner_slots)
        print(f"PASS: {entry} reconciled the deployed platform runner and preserved release and agent state", flush=True)
        if entry == "up":
            history = json.loads(run(["helm", "--kube-context", context, "-n", namespace, "history", release, "-o", "json"], "read rollback target history").stdout)
            revision = str(history[-1]["revision"])
            run(["helm", "--kube-context", context, "-n", namespace, "upgrade", release, chart,
                 "--reuse-values", "--wait", "--timeout", "300s"], "prepare compatible rollback revision", timeout=330)
            stale = copy.deepcopy(after["spec"])
            for kind, name in runner_slots:
                next(c for c in stale["podTemplate"]["spec"][kind] if c["name"] == name)["image"] = old_image
            patch(namespace, "sandboxtemplate", source_name, {"spec": stale})
            rollback_since = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            result = cli(binary, namespace, release, chart, "rollback", "--revision", revision, "--yes", timeout=900)
            require(json.loads(result.stdout)["rolled_back"] is True, "rollback did not succeed")
            restored = read(namespace, "sandboxtemplate/" + source_name)
            require(restored["spec"] == expected_spec, "rollback left a stale runner image or changed agent settings")
            require(read(namespace, "sandboxwarmpool/" + source_name + "-pool")["spec"] == pool_before, "rollback changed pool selection")
            retained = snapshot(namespace, fullname)
            for kind in ("credentials", "pvcs"):
                require(all(retained[kind].get(key) == value for key, value in before[kind].items()),
                        "rollback changed existing " + kind)
            kc(namespace, "delete", "sandboxclaims", "-l", "curietech.ai/agent=acme-bot", "--wait=true", "--timeout=90s")
            turn(binary, namespace, release, chart, images["runner"], rollback_since, runner_slots)
            print("PASS: rollback repaired the stale deployed runner template and completed a fresh turn", flush=True)
    finally:
        env.pop("K8S_KUBECONFIG", None)
        if owned:
            clean(namespace, release)


crds = ("sandboxes.agents.x-k8s.io", "sandboxclaims.extensions.agents.x-k8s.io",
        "sandboxtemplates.extensions.agents.x-k8s.io", "sandboxwarmpools.extensions.agents.x-k8s.io")
absent_crds = [name for name in crds if not kc("default", "get", "crd/" + name, "--ignore-not-found", "-o", "name").stdout.strip()]
try:
    # Published baseline bytes are checked independently of the candidate checkout.
    for name in ("curie-x86_64-unknown-linux-gnu", "curie-0.13.0.tgz", "checksums.txt"):
        url = "https://github.com/curie-eng/curie/releases/download/v0.13.0/" + name
        with urllib.request.urlopen(url, timeout=120) as response, (work / name).open("wb") as output:
            shutil.copyfileobj(response, output)
    checks = {line.split()[-1].lstrip("*"): line.split()[0] for line in (work / "checksums.txt").read_text().splitlines()}
    for name in ("curie-x86_64-unknown-linux-gnu", "curie-0.13.0.tgz"):
        require(hashlib.sha256((work / name).read_bytes()).hexdigest() == checks[name], "published baseline checksum mismatch")
    baseline = work / "curie-x86_64-unknown-linux-gnu"
    baseline.chmod(0o700)
    old_chart = work / "curie-0.13.0.tgz"
    version = yaml.safe_load((root / "charts/curie/Chart.yaml").read_text())["version"]
    for entry in ("upgrade", "up"):
        scenario(entry, baseline, old_chart, root / "charts/curie", str(version))
finally:
    try:
        for name in absent_crds:
            kc("default", "delete", "crd/" + name, "--ignore-not-found", "--wait=true", "--timeout=60s", timeout=80)
            require(not kc("default", "get", "crd/" + name, "--ignore-not-found", "-o", "name").stdout.strip(),
                    "owned CRD remains")
    finally:
        shutil.rmtree(work)
PYTHON

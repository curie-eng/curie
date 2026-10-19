#!/usr/bin/env bash
# #4716: retry a failed sealed first install against real StatefulSet stores.
# fix-pin-provision: candidate-stack
# The caller supplies CURIE_BIN, CURIE_SCENARIO_CONTEXT, CURIE_SCENARIO_RUN_ID,
# and CURIE_SCENARIO_{API,WORKER,RUNNER}_IMAGE, already loaded into the cluster.
set +x
set -euo pipefail
umask 077

export CURIE_RETRY_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
command -v python3 >/dev/null || { echo 'FAIL: python3 is required' >&2; exit 1; }
python3 - <<'PY'
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time


class ScenarioError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise ScenarioError(message)


def timestamp(value):
    require(isinstance(value, str) and value.endswith("Z"), "missing API-server timestamp")
    return datetime.datetime.fromisoformat(value[:-1] + "+00:00")


def main():
    try:
        import yaml
    except ImportError:
        raise ScenarioError("Python 3 with PyYAML is required") from None
    names = (
        "CURIE_BIN", "CURIE_SCENARIO_CONTEXT", "CURIE_SCENARIO_RUN_ID",
        "CURIE_SCENARIO_API_IMAGE", "CURIE_SCENARIO_WORKER_IMAGE", "CURIE_SCENARIO_RUNNER_IMAGE",
    )
    for name in names:
        require(bool(os.environ.get(name)), f"{name} is required")
    binary = Path(os.environ["CURIE_BIN"]).absolute()
    require(binary.is_file() and os.access(binary, os.X_OK), "CURIE_BIN must be executable")
    context = os.environ["CURIE_SCENARIO_CONTEXT"]
    require(not context.startswith("-") and not re.search(r"[\s\x00-\x1f\x7f]", context),
            "CURIE_SCENARIO_CONTEXT must name one kubeconfig context")
    run_id = os.environ["CURIE_SCENARIO_RUN_ID"]
    require(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,23}[a-z0-9])?", run_id),
            "CURIE_SCENARIO_RUN_ID must be 1 to 25 lowercase DNS-label characters")
    component = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
    host_component = r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
    host = rf"(?:{host_component}(?:\.{host_component})*|\[[A-Fa-f0-9:]+\])(?::[0-9]+)?"
    repository = rf"(?:{host}/)?{component}(?:/{component})*"
    images = {}
    for component_name in ("api", "worker", "runner"):
        variable = f"CURIE_SCENARIO_{component_name.upper()}_IMAGE"
        image = os.environ[variable]
        image_name, separator, tag = image.rpartition(":")
        require(separator and re.fullmatch(repository, image_name)
                and re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag),
                f"{variable} must be a repository:tag image reference")
        images[component_name] = (image_name, tag)
    contexts = {}
    kubeconfig = os.environ.get("KUBECONFIG") or str(Path.home() / ".kube/config")
    try:
        for filename in dict.fromkeys(filter(None, kubeconfig.split(os.pathsep))):
            path = Path(filename)
            if not path.exists():
                continue
            config = yaml.safe_load(path.read_text()) or {}
            entries = config.get("contexts", [])
            require(isinstance(entries, list), "cannot read kubeconfig contexts")
            for entry in entries:
                require(isinstance(entry, dict) and isinstance(entry.get("name"), str),
                        "cannot read kubeconfig contexts")
                contexts.setdefault(entry["name"], entry.get("context"))
    except (OSError, ValueError, AttributeError, yaml.YAMLError):
        raise ScenarioError("cannot read the caller's kubeconfig") from None
    require(isinstance(contexts.get(context), dict), "context is absent from the caller's kubeconfig")
    for executable in ("kubectl", "helm"):
        require(shutil.which(executable), f"required executable is unavailable: {executable}")
    require(os.environ.get("HELM_DRIVER", "secret") == "secret", "Helm Secret storage is required")
    root = Path(os.environ["CURIE_RETRY_REPO_ROOT"])
    source_chart = root / "charts/curie"
    defaults = yaml.safe_load((source_chart / "values.yaml").read_text())
    require(re.fullmatch(r"postgres:[\w.-]+@sha256:[a-f0-9]{64}", defaults["postgres"]["image"]),
            "the delay hook requires the chart's digest-pinned Postgres image")
    namespace = f"test-4716-retry-{run_id}"
    release = f"retry-{run_id}"
    fullname = f"{release}-curie"
    unrelated = {"pre": f"unrelated-pre-{release}", "post": f"unrelated-post-{release}"}
    bad_image = f"registry.invalid/curie-api:missing-{run_id}"
    environment = os.environ.copy()
    for name in (
        "CURIE_CREDENTIALS", "CURIE_MODEL_CREDENTIALS", "CURIE_MODEL", "CURIE_MODEL_BASE_URL",
        "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "OPENAI_API_KEY",
        "CURIE_GITHUB_TOKEN", "GITHUB_TOKEN", "GH_TOKEN", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN",
        "CURIE_API_URL", "CURIE_API_KEY", "CURIE_VALKEY_PASSWORD", "CURIE_NAMESPACE", "CURIE_STREAM",
        "CURIE_FAKE_MODEL", "CURIE_FAKE_SCRIPT", "CURIE_FAKE_SCRIPT_FILE",
    ):
        environment.pop(name, None)

    # Captured subprocess output can include generated credentials. Never print it,
    # including in errors, assertion messages, or cleanup diagnostics.
    def run(arguments, description, *, timeout=60, check=True, stdin=None):
        process = None
        completed = False
        try:
            process = subprocess.Popen(arguments, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                       env=environment, start_new_session=True)
            stdout, stderr = process.communicate(input=stdin, timeout=timeout)
            completed = True
        except subprocess.TimeoutExpired:
            raise ScenarioError(f"{description} timed out") from None
        except OSError:
            raise ScenarioError(f"{description} could not start") from None
        finally:
            if process is not None and not completed:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.communicate(timeout=5)
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.communicate(timeout=5)
        result = subprocess.CompletedProcess(arguments, process.returncode, stdout, stderr)
        require(not check or result.returncode == 0,
                f"{description} failed (exit {result.returncode})")
        return result

    def kc(*arguments, **options):
        return run(["kubectl", "--context", context, "--namespace", namespace, *arguments],
                   "kubectl " + arguments[0], **options)

    def helm(*arguments, **options):
        return run(["helm", "--kube-context", context, "--namespace", namespace, *arguments],
                   "Helm " + arguments[0], **options)

    def read(resource):
        return json.loads(kc("get", resource, "-o", "json").stdout)

    require(not kc("get", f"namespace/{namespace}", "--ignore-not-found", "-o", "name").stdout.strip(),
            "scenario namespace already exists")
    crds = (
        "sandboxes.agents.x-k8s.io", "sandboxclaims.extensions.agents.x-k8s.io",
        "sandboxtemplates.extensions.agents.x-k8s.io", "sandboxwarmpools.extensions.agents.x-k8s.io",
    )
    absent_crds = [name for name in crds if not kc(
        "get", f"crd/{name}", "--ignore-not-found", "-o", "name").stdout.strip()]
    workdir = Path(tempfile.mkdtemp(prefix="curie-failed-first-install-retry-"))
    environment["CURIE_CONFIG_DIR"] = str(workdir / "curie-config")
    namespace_owned = False

    def cleanup():
        failures = []

        def attempt(action, message):
            try:
                action()
            except Exception:
                failures.append(message)

        if namespace_owned:
            attempt(lambda: helm("uninstall", release, "--ignore-not-found", "--wait", "--timeout", "60s",
                                 timeout=75), "owned release uninstall failed")
            attempt(lambda: kc("delete", "pods", "--all", "--ignore-not-found", "--wait=true", "--timeout=60s",
                               timeout=75), "owned pod drain failed")
            attempt(lambda: kc("delete", "namespace", namespace, "--ignore-not-found", "--wait=false"),
                    "owned namespace deletion failed")
            attempt(lambda: kc("wait", "--for=delete", f"namespace/{namespace}", "--timeout=90s", timeout=105),
                    "owned namespace did not disappear")
            for name in absent_crds:
                attempt(lambda name=name: kc("delete", f"crd/{name}", "--ignore-not-found", "--wait=true",
                                             "--timeout=60s", timeout=75), "new chart CRD deletion failed")
            for resource in (f"namespace/{namespace}", *(f"crd/{name}" for name in absent_crds)):
                attempt(lambda resource=resource: require(not kc(
                    "get", resource, "--ignore-not-found", "-o", "name").stdout.strip(),
                    "owned object remains"), "owned object teardown could not be verified")
            attempt(lambda: require(not helm("list", "--all", "--short", "--filter", f"^{release}$").stdout.strip(),
                                    "owned release remains"), "release teardown could not be verified")
        attempt(lambda: shutil.rmtree(workdir), "private fixture cleanup failed")
        require(not failures, "; ".join(failures))
        if namespace_owned:
            print("OK: owned namespace, release, pods, PVCs, and new chart CRDs are gone")

    def interrupt(_signum, _frame):
        raise ScenarioError("scenario interrupted")

    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    try:
        # This is the only added chart template. Production templates remain intact.
        chart = workdir / "chart"
        shutil.copytree(source_chart, chart)
        (chart / "templates/failed-first-install-delay.yaml").write_text('''apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "curie.fullname" . }}-retry-delay
  annotations:
    helm.sh/hook: pre-install
    helm.sh/hook-weight: "-100"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 60
  template:
    spec:
      automountServiceAccountToken: false
      restartPolicy: Never
      containers:
        - name: delay
          image: {{ .Values.postgres.image | quote }}
          imagePullPolicy: IfNotPresent
          command: ["sleep", "2"]
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {cpu: 100m, memory: 32Mi}
''')
        values = {
            "fullnameOverride": fullname,
            "global": {"imagePullPolicy": "IfNotPresent"},
            "security": {"allowDevDefaults": False, "gvisor": {"mode": "off"}},
            "api": {"image": {"repository": images["api"][0], "tag": images["api"][1], "pullPolicy": "Never"}},
            "worker": {"image": {"repository": images["worker"][0], "tag": images["worker"][1], "pullPolicy": "Never"}},
            "agentSandbox": {"deploy": False, "controller": {"deploy": False}, "runner": {
                "image": images["runner"][0], "tag": images["runner"][1], "imagePullPolicy": "Never",
                "fakeModel": True, "prewarm": {"enabled": False}}},
            "langfuse": {"deploy": True, "modelPricing": {"enabled": True}},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "priorityClasses": {kind: {"create": False, "name": ""} for kind in ("platform", "sandbox")},
            "resourceQuota": {"enabled": False}, "limitRange": {"enabled": False},
            "preflights": {"controllerReady": {"enabled": False}, "networkPolicyProbe": {"enabled": False}},
        }
        for component_name in ("ui", "dispatcher", "mailAdapter", "inference"):
            values[component_name] = {"deploy": False}
        for component_name in ("postgres", "clickhouse", "rustfs", "valkey"):
            values[component_name] = {"deploy": True, "persistence": {"enabled": True}}
        values_file = workdir / "values.yaml"
        values_file.write_text(yaml.safe_dump(values))
        namespace_owned = True
        kc("create", "namespace", namespace)

        def create_unrelated(when):
            manifest = {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {
                "name": unrelated[when], "namespace": namespace, "labels": {
                    "app.kubernetes.io/instance": release, "app.kubernetes.io/component": "valkey"}},
                "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Mi"}}}}
            claim = json.loads(kc("create", "-f", "-", "-o", "json", stdin=json.dumps(manifest)).stdout)
            require(bool(claim["metadata"].get("uid")), "unrelated PVC has no UID")
            return claim

        kept = {"pre": create_unrelated("pre")}
        command = [str(binary), "cluster", "--context", context, "up", "--namespace", namespace,
                   "--release", release, "--chart", str(chart), "--values-file", str(values_file),
                   "--adopt", "--fake-model", "--no-expose"]
        first = run([*command, "--set-string", f"api.image.repository={bad_image.rsplit(':', 1)[0]}",
                     "--set-string", f"api.image.tag={bad_image.rsplit(':', 1)[1]}"],
                    "first cluster up", timeout=660, check=False)
        require(first.returncode != 0, "first cluster up unexpectedly succeeded with an unavailable API image")
        history = json.loads(helm("history", release, "--output", "json").stdout)
        require(len(history) == 1 and history[0]["revision"] == 1
                and all(row["status"].lower() == "failed" for row in history),
                "the first attempt must leave only failed revision 1")
        pods = read("pods")["items"]
        require(any(pod["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "api"
                    and container.get("image") == bad_image
                    and container.get("state", {}).get("waiting", {}).get("reason")
                    in {"ErrImageNeverPull", "ErrImagePull", "ImagePullBackOff"}
                    for pod in pods for container in pod.get("status", {}).get("containerStatuses", [])),
                "the first failure did not leave the unavailable API image waiting")
        anchor = read(f"secret/sh.helm.release.v1.{release}.v1")
        anchor_time = timestamp(anchor["metadata"].get("creationTimestamp"))
        secret = read(f"secret/{fullname}-secrets")

        def init_key_digest(secret):
            key = base64.b64decode(secret["data"]["langfuseInitProjectSecretKey"], validate=True)
            require(key and key != defaults["langfuse"]["init"]["projectSecretKey"].encode(),
                    "the install did not generate a sealed Langfuse init key")
            return hashlib.sha256(key).digest()

        first_key_digest = init_key_digest(secret)
        first_secret_uid = secret["metadata"]["uid"]
        print("OK: failed revision 1 used a generated Langfuse init key")
        claims = {}
        statefulsets = read("statefulsets")["items"]
        for component_name in ("postgres", "clickhouse", "rustfs", "valkey"):
            matches = [item for item in statefulsets if item["metadata"]["name"] == f"{fullname}-{component_name}"]
            require(len(matches) == 1, f"missing production {component_name} StatefulSet")
            statefulset = matches[0]
            metadata = statefulset["metadata"]
            annotations = metadata.get("annotations", {})
            require(annotations.get("meta.helm.sh/release-name") == release
                    and annotations.get("meta.helm.sh/release-namespace") == namespace,
                    f"{component_name} StatefulSet is not owned by this release")
            require(statefulset["spec"]["replicas"] == 1, "expected one replica per data StatefulSet")
            templates = statefulset["spec"]["volumeClaimTemplates"]
            require(len(templates) == 1 and templates[0]["metadata"]["name"] == "data",
                    "production data claim-template name changed")
            name = f"{templates[0]['metadata']['name']}-{metadata['name']}-0"
            claim = read(f"pvc/{name}")
            require(timestamp(claim["metadata"].get("creationTimestamp")) > anchor_time,
                    f"data PVC {name} is not strictly newer than revision 1")
            labels = claim["metadata"].get("labels", {})
            require(labels.get("app.kubernetes.io/instance") == release
                    and labels.get("app.kubernetes.io/component") == component_name,
                    f"data PVC {name} is missing the production selector labels")
            claims[name] = claim["metadata"]["uid"]
        require(len(claims) == 4, "expected exactly four data PVCs")
        kc("rollout", "status", f"deployment/{fullname}-langfuse-web", "--timeout=300s", timeout=315)
        deadline = time.monotonic() + 300
        seeded = False
        while time.monotonic() < deadline:
            result = kc("exec", f"statefulset/{fullname}-postgres", "--", "sh", "-ec", '''
export PGPASSWORD="$POSTGRES_PASSWORD" PGCONNECT_TIMEOUT=5 PGOPTIONS='-c statement_timeout=5000'
exec psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" \
    --no-psqlrc --tuples-only --no-align --command='SELECT count(*) FROM api_keys;'
''', timeout=min(15, max(1, deadline - time.monotonic())), check=False)
            if result.returncode == 0 and result.stdout.strip().isdigit() and int(result.stdout.strip()) > 0:
                seeded = True
                break
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        require(seeded, "Langfuse did not seed a real api_keys row within 300 seconds")
        print("OK: the first Langfuse install seeded api_keys in real Postgres")
        kept["post"] = create_unrelated("post")
        require(timestamp(kept["pre"]["metadata"]["creationTimestamp"]) <= anchor_time,
                "the pre-install unrelated claim was created after revision 1")
        require(timestamp(kept["post"]["metadata"]["creationTimestamp"]) > anchor_time,
                "the post-install unrelated claim is not newer than revision 1")
        retry = run(command, "candidate retry cluster up", timeout=900)
        output = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", retry.stdout + retry.stderr)
        require("removing data volumes of the failed first revision" in output,
                "retry did not report the data-volume cleanup checklist step")
        for name, old_uid in claims.items():
            require(name in output, f"retry did not name deleted data PVC {name}")
            replacement = read(f"pvc/{name}")
            new_uid = replacement["metadata"]["uid"]
            require(new_uid and new_uid != old_uid, f"data PVC {name} retained its first-install UID")
            print(f"OK: deleted and recreated {name}: UID {old_uid} -> {new_uid}")
        for when, original in kept.items():
            name = unrelated[when]
            require(name in output, f"retry did not name kept unrelated PVC {name}")
            require(any(name in line and re.search(r"\b(?:keep|kept|keeping|retain|retained|retaining)\b", line, re.I)
                        for line in output.splitlines()), f"retry did not report {name} as kept")
            current = read(f"pvc/{name}")
            require(current["metadata"]["uid"] == original["metadata"]["uid"],
                    f"unrelated {when}-install PVC {name} was replaced")
            print(f"OK: kept unrelated {when}-install {name}: UID {original['metadata']['uid']}")
        replacement_secret = read(f"secret/{fullname}-secrets")
        require(replacement_secret["metadata"]["uid"] != first_secret_uid,
                "retry did not recreate the chart Secret")
        require(init_key_digest(replacement_secret) != first_key_digest,
                "retry did not generate a new Langfuse init key")
        history = json.loads(helm("history", release, "--output", "json").stdout)
        require(len(history) == 1 and history[0]["revision"] == 1 and history[0]["status"] == "deployed",
                "retry did not converge to a clean deployed revision 1")
        for component_name in ("api", "worker"):
            deployment = read(f"deployment/{fullname}-{component_name}")
            container = next(item for item in deployment["spec"]["template"]["spec"]["containers"]
                             if item["name"] == component_name)
            require(container["image"] == os.environ[f"CURIE_SCENARIO_{component_name.upper()}_IMAGE"]
                    and container["imagePullPolicy"] == "Never", f"{component_name} is not using the imported candidate")
        kc("wait", "--for=condition=complete", f"job/{fullname}-langfuse-model-pricing", "--timeout=300s", timeout=315)
        pricing = read(f"job/{fullname}-langfuse-model-pricing")
        require(pricing["status"].get("succeeded") == 1, "Langfuse model-pricing hook did not succeed")
        deployment = read(f"deployment/{fullname}-langfuse-web")
        container = next(item for item in deployment["spec"]["template"]["spec"]["containers"]
                         if item["name"] == "langfuse-web")
        langfuse_env = {item["name"]: item for item in container["env"]}
        require(langfuse_env["LANGFUSE_INIT_PROJECT_SECRET_KEY"]["valueFrom"]["secretKeyRef"] == {
            "name": f"{fullname}-secrets", "key": "langfuseInitProjectSecretKey"},
            "Langfuse is not reading the chart's generated init key")
        authentication = {
            "public": langfuse_env["LANGFUSE_INIT_PROJECT_PUBLIC_KEY"]["value"],
            "secret": base64.b64decode(replacement_secret["data"]["langfuseInitProjectSecretKey"], validate=True).decode(),
            "url": f"http://{fullname}-langfuse-web:{defaults['langfuse']['web']['service']['port']}/api/public/projects",
        }
        # Langfuse documents Basic auth (public key username, secret key password)
        # and GET /api/public/projects: https://api.reference.langfuse.com/#tag/projects
        # Credentials travel over exec stdin, never process arguments or output.
        probe = kc("exec", "-i", f"deployment/{fullname}-api", "--", "python3", "-c", '''
import base64, json, sys, urllib.error, urllib.request
credentials = json.load(sys.stdin)
token = base64.b64encode((credentials["public"] + ":" + credentials["secret"]).encode()).decode()
request = urllib.request.Request(credentials["url"], headers={"Authorization": "Basic " + token})
try:
    with urllib.request.urlopen(request, timeout=20) as response:
        print(response.status)
except urllib.error.HTTPError as error:
    print(error.code)
except Exception:
    raise SystemExit("Langfuse projects request could not complete") from None
''', stdin=json.dumps(authentication), timeout=30)
        require(probe.stdout.strip() == "200", "generated chart credentials did not authenticate to Langfuse projects")
        print("OK: Langfuse model-pricing hook succeeded and authenticated projects API returned 200")
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        cleanup()
    print("PASS: failed first install retry regenerated sealed credentials and data while retaining unrelated PVCs")


try:
    main()
except ScenarioError as error:
    print(f"FAIL: {error}", file=sys.stderr)
    sys.exit(1)
except Exception as error:
    print(f"FAIL: scenario infrastructure or response error ({type(error).__name__})", file=sys.stderr)
    sys.exit(1)
PY

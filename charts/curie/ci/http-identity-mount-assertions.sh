#!/usr/bin/env bash
# HTTP identity operator files reach only their own workload (@spec HTTP-INVOCATION-EXECUTION-20).
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)"
python3 - "$CHART" <<'PY'
import pathlib
import subprocess
import sys
import tempfile

import yaml

chart = pathlib.Path(sys.argv[1])
base = {"dispatcher": {"slack": {"appToken": "xapp-example", "botToken": "xoxb-example"}}}
API_SECRET = "acme-api-http-identity"
RECEIVER_SECRET = "acme-dispatcher-http-receiver"
API_DIR = "/var/run/curie/http-identity"
RECEIVER_DIR = "/var/run/curie/http-receiver"
count = 0


def render(*args, ok=True):
    global count
    count += 1
    with tempfile.TemporaryDirectory() as tmp:
        values = pathlib.Path(tmp) / "values.yaml"
        values.write_text(yaml.safe_dump(base))
        result = subprocess.run(
            # values-dev keeps published, deterministic secrets so renders compare.
            ["helm", "template", "acme", str(chart), "-f", str(chart / "values-dev.yaml"),
             "-f", str(values), *args],
            text=True, capture_output=True,
        )
    if ok:
        assert result.returncode == 0, result.stderr
    return result


def docs(result):
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def workload(documents, component):
    matches = [
        doc for doc in documents
        if doc.get("kind") == "Deployment"
        and doc["metadata"].get("labels", {}).get("app.kubernetes.io/component") == component
    ]
    assert len(matches) == 1, f"expected one {component} Deployment"
    return matches[0]["spec"]["template"]["spec"]


def container(pod, name):
    matches = [c for c in pod["containers"] if c["name"] == name]
    assert len(matches) == 1, f"expected one {name} container"
    return matches[0]


def env_value(item, name):
    return [e for e in item.get("env", []) if e["name"] == name]


def mentions(documents, needle):
    """Every rendered object whose text names ``needle``."""
    return sorted({
        f"{doc['kind']}/{doc['metadata']['name']}"
        for doc in documents if needle in yaml.safe_dump(doc)
    })


# 1. Default render: nothing selected, nothing mounted, no chart-owned env.
default = render()
assert "CURIE_HTTP_IDENTITY_DIR" not in default.stdout
assert "CURIE_HTTP_RECEIVER_DIR" not in default.stdout
assert "http-identity" not in default.stdout and "http-receiver" not in default.stdout
# A release that predates the keys replays them as nil; the render is unchanged.
retained = render("--set", "api.httpIdentity=null", "--set", "dispatcher.httpIdentity=null")
assert retained.stdout == default.stdout, "nil httpIdentity changed the default render"
empty = render("--set-string", "api.httpIdentity.existingSecret=",
               "--set-string", "dispatcher.httpIdentity.existingSecret=")
assert empty.stdout == default.stdout, "empty existingSecret changed the default render"

# 2. API selection: one 0440 read-only file in the API container alone.
selected = render("--set-string", f"api.httpIdentity.existingSecret={API_SECRET}")
documents = docs(selected)
api = workload(documents, "api")
api_container = container(api, "api")
assert env_value(api_container, "CURIE_HTTP_IDENTITY_DIR") == [
    {"name": "CURIE_HTTP_IDENTITY_DIR", "value": API_DIR}
], [e["name"] for e in api_container.get("env", [])]
assert {"name": "http-identity", "mountPath": API_DIR, "readOnly": True} in api_container[
    "volumeMounts"
], api_container.get("volumeMounts")
volumes = [v for v in api["volumes"] if v["name"] == "http-identity"]
assert volumes == [{
    "name": "http-identity",
    "secret": {
        "secretName": API_SECRET,
        "defaultMode": 0o440,
        "items": [{"key": "api-runtime.json", "path": "api-runtime.json"}],
    },
}], volumes
assert api.get("securityContext", {}).get("fsGroup") == 1000, api.get("securityContext")
for init in api.get("initContainers", []):
    assert not env_value(init, "CURIE_HTTP_IDENTITY_DIR"), init["name"]
    assert all(m["name"] != "http-identity" for m in init.get("volumeMounts", [])), init["name"]
assert mentions(documents, API_SECRET) == ["Deployment/acme-curie-api"], mentions(
    documents, API_SECRET
)
dispatcher = container(workload(documents, "dispatcher"), "dispatcher")
assert not env_value(dispatcher, "CURIE_HTTP_RECEIVER_DIR")

# 2b. The optional provider trust anchor joins the same API-only volume.
anchored = docs(render("--set-string", f"api.httpIdentity.existingSecret={API_SECRET}",
                       "--set", "api.httpIdentity.providerCaBundle=true"))
volumes = [v for v in workload(anchored, "api")["volumes"] if v["name"] == "http-identity"]
assert volumes[0]["secret"]["items"] == [
    {"key": "api-runtime.json", "path": "api-runtime.json"},
    {"key": "provider-ca.pem", "path": "provider-ca.pem"},
], volumes
assert mentions(anchored, "provider-ca.pem") == ["Deployment/acme-curie-api"]
assert "provider-ca.pem" not in selected.stdout, "the anchor mounted without being selected"
orphan = render("--set", "api.httpIdentity.providerCaBundle=true", ok=False)
assert orphan.returncode != 0 and "api.httpIdentity.existingSecret" in orphan.stderr, orphan.stderr
assert render("--set-string", f"api.httpIdentity.existingSecret={API_SECRET}",
              "--set-string", "api.httpIdentity.providerCaBundle=true", ok=False).returncode != 0

# 3. Dispatcher selection: its own file, never the API's, never worker or runner.
both = render(
    "--set-string", f"api.httpIdentity.existingSecret={API_SECRET}",
    "--set-string", f"dispatcher.httpIdentity.existingSecret={RECEIVER_SECRET}",
)
documents = docs(both)
pod = workload(documents, "dispatcher")
dispatcher = container(pod, "dispatcher")
assert env_value(dispatcher, "CURIE_HTTP_RECEIVER_DIR") == [
    {"name": "CURIE_HTTP_RECEIVER_DIR", "value": RECEIVER_DIR}
], [e["name"] for e in dispatcher.get("env", [])]
assert {"name": "http-receiver", "mountPath": RECEIVER_DIR, "readOnly": True} in dispatcher[
    "volumeMounts"
]
assert pod["volumes"] == [{
    "name": "http-receiver",
    "secret": {
        "secretName": RECEIVER_SECRET,
        "defaultMode": 0o440,
        "items": [{"key": "receivers.json", "path": "receivers.json"}],
    },
}], pod["volumes"]
assert pod.get("securityContext", {}).get("fsGroup") == 1000, pod.get("securityContext")
assert not env_value(dispatcher, "CURIE_HTTP_IDENTITY_DIR")
assert mentions(documents, RECEIVER_SECRET) == ["Deployment/acme-curie-dispatcher"]
assert mentions(documents, API_SECRET) == ["Deployment/acme-curie-api"]
for needle in ("CURIE_HTTP_IDENTITY_DIR", "CURIE_HTTP_RECEIVER_DIR", "api-runtime.json",
               "receivers.json"):
    owners = mentions(documents, needle)
    assert set(owners) <= {"Deployment/acme-curie-api", "Deployment/acme-curie-dispatcher"}, (
        needle, owners,
    )

# 4. A receiver without the API registrations cannot be rendered.
lonely = render("--set-string", f"dispatcher.httpIdentity.existingSecret={RECEIVER_SECRET}",
                ok=False)
assert lonely.returncode != 0 and "api.httpIdentity.existingSecret" in lonely.stderr, lonely.stderr

# 5. Both environment names are chart-owned even while unselected.
for workload_name, name, key in (
    ("api", "CURIE_HTTP_IDENTITY_DIR", "api.httpIdentity.existingSecret"),
    ("dispatcher", "CURIE_HTTP_RECEIVER_DIR", "dispatcher.httpIdentity.existingSecret"),
):
    result = render("--set", f"{workload_name}.extraEnv[0].name={name}",
                    "--set-string", f"{workload_name}.extraEnv[0].value=/tmp/elsewhere", ok=False)
    assert result.returncode != 0 and name in result.stderr and key in result.stderr, result.stderr

# 6. A Secret name that Kubernetes cannot accept is refused by the schema.
invalid = render("--set-string", "api.httpIdentity.existingSecret=Not_A_Name", ok=False)
assert invalid.returncode != 0, "accepted an invalid Secret name"

print(f"OK: {count} Helm renders; default render unchanged; API and dispatcher files mount only into their own containers")
PY

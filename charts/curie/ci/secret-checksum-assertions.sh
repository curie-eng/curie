#!/usr/bin/env bash
# Render-only proof for #1135. Derive each pod's consumed chart-owned Secret
# keys from its manifest, then recompute sha256(toJson(key -> plaintext value)).
# Rendered Secrets and Helm values stay in memory and never enter CI output.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
for tool in helm python3; do
  command -v "$tool" >/dev/null || { echo "FAIL: $tool is required" >&2; exit 1; }
done

python3 - "$CHART" <<'PY'
import base64
import copy
import hashlib
import json
import subprocess
import sys

try:
    import yaml
except ImportError:
    raise SystemExit("FAIL: python3 requires PyYAML") from None

CHART = sys.argv[1]
RELEASE = "acme-checksum"
ANNOTATION = "checksum/secret"
LONG_RUNNING = {"Deployment", "StatefulSet", "DaemonSet"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def merge(left, right):
    result = copy.deepcopy(left)
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def render(label, values=None):
    try:
        result = subprocess.run(
            ["helm", "template", RELEASE, CHART, "--values", "-"],
            input=yaml.safe_dump(values or {}),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise AssertionError(f"{label}: Helm render could not complete") from None
    require(result.returncode == 0, f"{label}: Helm render failed; output withheld to protect credentials")
    try:
        docs = [doc for doc in yaml.safe_load_all(result.stdout) if isinstance(doc, dict)]
    except yaml.YAMLError:
        raise AssertionError(f"{label}: invalid rendered YAML; output withheld to protect credentials") from None
    require(bool(docs), f"{label}: Helm rendered no resources")
    return docs


def secret_content(doc):
    try:
        content = {
            key: base64.b64decode(value, validate=True).decode("utf-8")
            for key, value in (doc.get("data") or {}).items()
        }
        content.update(doc.get("stringData") or {})
    except (ValueError, UnicodeError, TypeError):
        raise AssertionError("chart-owned Secret has invalid encoded data; values withheld") from None
    require(all(isinstance(value, str) for value in content.values()), "chart-owned Secret has a non-string value")
    return content


def chart_secret(docs):
    # installationId distinguishes the chart-owned Secret from runner tokens,
    # connector credentials and other separately rendered Secrets.
    secrets = [
        doc for doc in docs
        if doc.get("kind") == "Secret"
        and "installationId" in {**(doc.get("data") or {}), **(doc.get("stringData") or {})}
    ]
    require(len(secrets) == 1, "render must contain exactly one chart-owned Secret")
    return secrets[0]["metadata"]["name"], secret_content(secrets[0])


def consumed_keys(spec, secret_name, all_keys, include_optional=True):
    keys = set()
    for container in list(spec.get("containers") or []) + list(spec.get("initContainers") or []):
        for env in container.get("env") or []:
            ref = (env.get("valueFrom") or {}).get("secretKeyRef") or {}
            if ref.get("name") == secret_name and (include_optional or not ref.get("optional")):
                keys.add(ref["key"])
        for env_from in container.get("envFrom") or []:
            ref = env_from.get("secretRef") or {}
            if ref.get("name") == secret_name and (include_optional or not ref.get("optional")):
                keys.update(all_keys)
    for volume in spec.get("volumes") or []:
        sources = [(volume.get("secret") or {}, "secretName")]
        sources.extend(
            (source.get("secret") or {}, "name")
            for source in (volume.get("projected") or {}).get("sources") or []
        )
        for source, name_field in sources:
            if source.get(name_field) == secret_name and (include_optional or not source.get("optional")):
                items = source.get("items") or []
                if items:
                    keys.update(item["key"] for item in items)
                else:
                    keys.update(all_keys)
    return keys


def checksum(content, keys):
    # Helm's toJson uses Go encoding/json: sorted map keys, compact separators,
    # UTF-8, and escaped HTML plus U+2028/U+2029. Hash decoded Secret values,
    # never their base64 transport representation or the complete manifest.
    encoded = json.dumps({key: content.get(key) for key in keys}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    for char, escaped in (("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        encoded = encoded.replace(char, escaped)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def carries_annotation(value):
    if isinstance(value, dict):
        if ANNOTATION in (value.get("annotations") or {}):
            return True
        return any(carries_annotation(child) for child in value.values())
    if isinstance(value, list):
        return any(carries_annotation(child) for child in value)
    return False


def audit(docs, label):
    secret_name, content = chart_secret(docs)
    workloads = {}
    for doc in docs:
        kind = doc.get("kind")
        name = doc.get("metadata", {}).get("name", "<unnamed>")
        identity = (kind, name)
        if kind in {"Job", "SandboxTemplate"}:
            require(not carries_annotation(doc), f"{label}: {kind}/{name} must not carry {ANNOTATION}")
        if kind not in LONG_RUNNING:
            continue
        template = doc.get("spec", {}).get("template") or {}
        spec = template.get("spec") or {}
        keys = consumed_keys(spec, secret_name, set(content))
        required_keys = consumed_keys(spec, secret_name, set(content), include_optional=False)
        annotation = (template.get("metadata", {}).get("annotations") or {}).get(ANNOTATION)
        require(required_keys <= set(content), f"{label}: {kind}/{name} references an absent required chart-owned Secret key")
        if keys:
            require(annotation is not None, f"{label}: {kind}/{name} lacks {ANNOTATION}")
            require(annotation == checksum(content, keys), f"{label}: {kind}/{name} checksum disagrees with its rendered consumed keys")
        else:
            require(annotation is None, f"{label}: {kind}/{name} hashes a Secret it does not consume")
        workloads[identity] = {
            "keys": keys,
            "checksum": annotation,
            "component": (
                (doc.get("metadata", {}).get("labels") or {}).get("app.kubernetes.io/component")
                or (template.get("metadata", {}).get("labels") or {}).get("app.kubernetes.io/component")
            ),
        }
    require(any(workload["keys"] for workload in workloads.values()), f"{label}: no long-running chart Secret consumers were audited")
    return workloads


def checksums(workloads):
    return {identity: workload["checksum"] for identity, workload in workloads.items()}


def components(workloads, identities):
    return {workloads[identity]["component"] for identity in identities}


def expect_rejected(docs, label, needle):
    try:
        audit(docs, label)
    except AssertionError as error:
        require(needle in str(error), f"{label}: negative control failed for an unintended reason")
    else:
        raise AssertionError(f"{label}: negative control was accepted")


def main():
    default_docs = render("default sealed")
    audit(default_docs, "default sealed")
    sealed_repeat = render("second sealed install")
    audit(sealed_repeat, "second sealed install")
    identity = chart_secret(default_docs)[1]["installationId"]
    require(chart_secret(sealed_repeat)[1]["installationId"] != identity, "separate sealed installs must mint distinct installation identities")
    string_false = render("string false sealed install", {"security": {"allowDevDefaults": "false"}})
    audit(string_false, "string false sealed install")
    require(chart_secret(string_false)[1]["installationId"] not in {identity, chart_secret(sealed_repeat)[1]["installationId"]}, "string false must preserve a distinct sealed installation identity")
    print("OK: default sealed checksums match the emitted Secret and rendered consumption")
    print("OK: sealed installs retain distinct identities, including string false")

    enabled = {
        "dispatcher": {"deploy": True, "slack": {"appToken": "xapp-assert", "botToken": "xoxb-assert"}},
        "mailAdapter": {
            "deploy": True,
            "channelToken": "channel-assert",
            "egressSecret": "egress-assert",
            "agentmail": {"apiKey": "provider-assert", "httpsCidrs": ["203.0.113.0/24"]},
        },
    }
    enabled_workloads = audit(render("enabled sealed", enabled), "enabled sealed")
    require({"dispatcher", "mail-adapter"} <= components(enabled_workloads, enabled_workloads), "enabled render must contain dispatcher and mail-adapter")
    print("OK: dispatcher and mail adapter sealed checksums match their consumed keys")

    dev = {"security": {"allowDevDefaults": True}}
    dev_docs = render("dev", dev)
    base = audit(dev_docs, "dev")
    null_passthrough = merge(dev, {
        "agentSandbox": {"runner": {"credentials": None}},
        "dispatcher": {"slack": {"appToken": None, "botToken": None, "signingSecret": None}},
        "mailAdapter": {"channelToken": None, "egressSecret": None, "agentmail": {"apiKey": None}},
    })
    null_docs = render("null passthrough credentials", null_passthrough)
    null_content = chart_secret(null_docs)[1]
    for key in (
        "agentCredentials", "slackAppToken", "slackBotToken", "slackSigningSecret",
        "mailAgentmailApiKey", "mailChannelToken", "mailEgressSecret",
    ):
        require(null_content.get(key) == "", f"null passthrough credentials: {key} must emit an empty string")
    audit(null_docs, "null passthrough credentials")
    print("OK: null passthrough credentials emit empty strings with matching workload hashes")
    for label, overlay in (
        ("identical input", {}),
        ("API replica change", {"api": {"replicas": 2}}),
        ("API image change", {"api": {"image": {"tag": "checksum-assert"}}}),
    ):
        compared = audit(render(label, merge(dev, overlay)), label)
        require(checksums(compared) == checksums(base), f"{label}: checksum changed without a Secret content change")
    print("OK: identical input, replicas and image tag leave every checksum unchanged")

    rotations = (
        ("dispatcher.slack.botToken", "slackBotToken", {"dispatcher": {"slack": {"botToken": "xoxb-rotated"}}}),
        ("api.apiKey", "apiKey", {"api": {"apiKey": "api-rotated"}}),
        ("postgres.auth.password", "postgresPassword", {"postgres": {"auth": {"password": "postgres-rotated"}}}),
        ("valkey.password", "valkeyPassword", {"valkey": {"password": "valkey-rotated"}}),
        ("clickhouse.auth.password", "clickhousePassword", {"clickhouse": {"auth": {"password": "clickhouse-rotated"}}}),
        ("rustfs.auth.secretKey", "rustfsSecretKey", {"rustfs": {"auth": {"secretKey": "rustfs-rotated"}}}),
    )
    for profile, values, baseline in (("default", dev, base), ("enabled", merge(dev, enabled), None)):
        baseline = baseline or audit(render("enabled dev", values), "enabled dev")
        for value_key, secret_key, overlay in rotations:
            label = f"{profile}: {value_key} rotation"
            rotated = audit(render(label, merge(values, overlay)), label)
            require(set(rotated) == set(baseline), f"{label}: workload membership unexpectedly changed")
            expected = {identity for identity, workload in baseline.items() if secret_key in workload["keys"]}
            changed = {identity for identity in baseline if rotated[identity]["checksum"] != baseline[identity]["checksum"]}
            require(bool(expected), f"{label}: the key has no audited consumer")
            require(changed == expected, f"{label}: changed workloads differ from rendered key consumers")
            if profile == "default" and secret_key == "slackBotToken":
                require(components(baseline, changed) == {"api", "worker"}, "Slack bot token rotation must change only API and worker")
            if secret_key == "valkeyPassword":
                require(any(identity[0] == "StatefulSet" and baseline[identity]["component"] == "valkey" for identity in changed), "Valkey password rotation must change the Valkey StatefulSet")
            print(f"OK: {label} changes exactly its {len(expected)} rendered consumer(s)")

    postgres_byo = audit(render("Postgres existingSecret", merge(dev, {"postgres": {"existingSecret": "acme-postgres-credentials"}})), "Postgres existingSecret")
    postgres = [workload for identity, workload in postgres_byo.items() if identity[0] == "StatefulSet" and workload["component"] == "postgres"]
    require(len(postgres) == 1 and postgres[0]["checksum"] is None, "Postgres existingSecret must remove the Postgres checksum")
    stores = {store: {"existingSecret": f"acme-{store}-credentials"} for store in ("postgres", "valkey", "clickhouse", "rustfs", "langfuse")}
    audit(render("all stores existingSecret", merge(dev, stores)), "all stores existingSecret")
    store_keys = {
        "postgres": {"postgresPassword"},
        "valkey": {"valkeyPassword"},
        "clickhouse": {"clickhousePassword"},
        "rustfs": {"rustfsSecretKey"},
        "langfuse": {
            "langfuseSalt", "langfuseEncryptionKey", "langfuseNextauthSecret",
            "langfuseInitProjectSecretKey", "langfuseInitUserPassword", "otlpAuthHeader",
        },
    }
    for store, keys in store_keys.items():
        label = f"individual {store} existingSecret"
        workloads = audit(render(label, merge(dev, {store: stores[store]})), label)
        require(all(not (keys & workload["keys"]) for workload in workloads.values()), f"{label}: a workload still consumes the replaced chart-owned keys")
    print("OK: existingSecret paths hash only retained chart-owned references")

    label = "explicit OTLP auth header"
    otlp = audit(render(label, merge(dev, {"otelCollector": {"otlpAuthHeader": "Bearer checksum-assert"}})), label)
    expected = {identity for identity, workload in base.items() if "otlpAuthHeader" in workload["keys"]}
    changed = {identity for identity in base if otlp[identity]["checksum"] != base[identity]["checksum"]}
    require(bool(expected) and changed == expected, "explicit OTLP auth header must change exactly its rendered consumers")
    explicit_byo = audit(render("explicit OTLP auth header with Langfuse existingSecret", merge(dev, {
        "langfuse": stores["langfuse"],
        "otelCollector": {"otlpAuthHeader": "Bearer checksum-assert"},
    })), "explicit OTLP auth header with Langfuse existingSecret")
    require(any("otlpAuthHeader" in workload["keys"] for workload in explicit_byo.values()), "explicit OTLP header must retain its chart-owned reference with BYO Langfuse")
    print("OK: an explicit OTLP auth header changes only its rendered consumers")

    worker_override = merge(dev, {"worker": {"extraEnv": [{"name": "CURIE_API_KEY", "value": "worker-override-assert"}]}})
    overridden = audit(render("worker API key override", worker_override), "worker API key override")
    worker = [workload for workload in overridden.values() if workload["component"] == "worker"]
    require(len(worker) == 1 and "apiKey" not in worker[0]["keys"], "worker API key override must remove the chart-owned apiKey reference")
    rotated = audit(render("rotation with worker API key override", merge(worker_override, {"api": {"apiKey": "api-override-rotated"}})), "rotation with worker API key override")
    expected = {identity for identity, workload in overridden.items() if "apiKey" in workload["keys"]}
    changed = {identity for identity in overridden if rotated[identity]["checksum"] != overridden[identity]["checksum"]}
    require(bool(expected) and changed == expected, "worker API key override must exclude worker from chart-owned apiKey rotation")
    print("OK: a worker CURIE_API_KEY override excludes the unused chart-owned key")

    real_model = merge(dev, {"agentSandbox": {"runner": {
        "fakeModel": False,
        "credentials": "credential-assert",
    }}})
    credential_workloads = audit(render("real-model credentials", real_model), "real-model credentials")
    credential_rotated = audit(render("real-model credential rotation", merge(real_model, {
        "agentSandbox": {"runner": {"credentials": "credential-rotated"}},
    })), "real-model credential rotation")
    require(set(credential_rotated) == set(credential_workloads), "real-model credential rotation unexpectedly changed workload membership")
    expected = {identity for identity, workload in credential_workloads.items() if "agentCredentials" in workload["keys"]}
    changed = {identity for identity in credential_workloads if credential_rotated[identity]["checksum"] != credential_workloads[identity]["checksum"]}
    require(bool(expected) and changed == expected, "real-model credential rotation must change exactly its rendered consumers")
    require("worker" in components(credential_workloads, expected), "real-model credentials must participate in the worker checksum")
    for label, runner in (
        ("BYO model credentials", {"credentialsExistingSecret": "acme-model-credentials"}),
        ("fake-model credential omission", {"fakeModel": True}),
    ):
        workloads = audit(render(label, merge(real_model, {"agentSandbox": {"runner": runner}})), label)
        require(all("agentCredentials" not in workload["keys"] for workload in workloads.values()), f"{label}: unused chart-owned model credentials still participate in a checksum")
    print("OK: real-model credentials rotate only their consumers; BYO and fake-model paths omit the chart-owned key")

    key_free = {
        "rustfs": {
            "deploy": False,
            "host": "s3.example.com",
            "port": 443,
            "auth": {"accessKey": ""},
            "egress": [{"cidr": "192.0.2.10/32", "ports": [{"protocol": "TCP", "port": 443}]}],
            "stsEgress": [{"cidr": "192.0.2.11/32", "ports": [{"protocol": "TCP", "port": 443}]}],
        },
    }
    key_free_workloads = audit(render("key-free object store", merge(dev, key_free)), "key-free object store")
    require(all("rustfsSecretKey" not in workload["keys"] for workload in key_free_workloads.values()), "key-free object store must not hash unused static credentials")
    print("OK: the key-free object-store path omits the unused static Secret key")

    escaped_value = "checksum<>&" + chr(0x2028) + chr(0x2029)
    escaped_docs = render("Go JSON escaping", merge(dev, {"api": {"apiKey": escaped_value}}))
    require(chart_secret(escaped_docs)[1]["apiKey"] == escaped_value, "JSON escaping profile did not preserve its input")
    audit(escaped_docs, "Go JSON escaping")
    print("OK: hashes match Go JSON escaping for HTML and Unicode separators")

    secret_name, _ = chart_secret(dev_docs)
    header_values = merge(merge(dev, enabled), {
        "otelCollector": {
            "deploy": False,
            "endpoint": "https://otel.example.com:4318",
            "headersExistingSecret": secret_name,
            "headersSecretKey": "mailChannelToken",
            "egress": [{"cidr": "192.0.2.40/32", "ports": [{"protocol": "TCP", "port": 4318}]}],
        },
        "mailAdapter": {"otelEgress": {"httpsCidrs": ["192.0.2.40/32"], "port": 4318}},
    })
    header_workloads = audit(render("managed telemetry header reference", header_values), "managed telemetry header reference")
    header_consumers = {workload["component"] for workload in header_workloads.values() if "mailChannelToken" in workload["keys"]}
    require({"api", "worker", "dispatcher", "mail-adapter"} <= header_consumers, "managed telemetry header references must enter each consuming pod checksum")
    print("OK: telemetry headers can reference arbitrary emitted chart-owned keys")
    extras = {
        workload: {"extraEnv": [{"name": "CHECKSUM_ASSERTION_EXTRA", "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "mailChannelToken"}}}]}
        for workload in ("api", "worker", "dispatcher", "otelCollector")
    }
    extra_workloads = audit(render("extraEnv Secret reference", merge(merge(dev, enabled), extras)), "extraEnv Secret reference")
    extra_consumers = {workload["component"] for workload in extra_workloads.values() if "mailChannelToken" in workload["keys"]}
    require({"api", "worker", "dispatcher", "otel-collector"} <= extra_consumers, "extraEnv references must be included in workload checksums")
    print("OK: rendered extraEnv Secret references participate in the checksum")

    optional = merge(dev, {"api": {"extraEnv": [{
        "name": "CHECKSUM_ASSERTION_OPTIONAL",
        "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "checksumAssertionMissing", "optional": True}},
    }]}})
    optional_docs = render("optional missing Secret key", optional)
    require("checksumAssertionMissing" not in chart_secret(optional_docs)[1], "optional missing-key profile unexpectedly emitted its key")
    optional_workloads = audit(optional_docs, "optional missing Secret key")
    optional_api = [workload for workload in optional_workloads.values() if workload["component"] == "api"]
    require(len(optional_api) == 1 and "checksumAssertionMissing" in optional_api[0]["keys"], "optional missing-key reference must participate with a null memo value")
    repeated_optional = audit(render("repeated optional missing Secret key", optional), "repeated optional missing Secret key")
    require(checksums(repeated_optional) == checksums(optional_workloads), "optional missing-key checksums must remain stable")
    print("OK: an optional missing Secret key hashes as stable null")

    # Negative controls start from a real render. Gaining any supported Secret
    # reference without changing the checksum must fail the same audit as CI.
    target = next(doc for doc in dev_docs if doc.get("kind") == "Deployment" and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "api")
    missing = copy.deepcopy(dev_docs)
    mutated = next(doc for doc in missing if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == target["metadata"]["name"])
    del mutated["spec"]["template"]["metadata"]["annotations"][ANNOTATION]
    expect_rejected(missing, "missing annotation", "lacks checksum/secret")
    required_missing = copy.deepcopy(dev_docs)
    mutated = next(doc for doc in required_missing if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == target["metadata"]["name"])
    mutated["spec"]["template"]["spec"]["containers"][0].setdefault("env", []).append({
        "name": "CHECKSUM_ASSERTION_REQUIRED",
        "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "checksumAssertionMissing"}},
    })
    expect_rejected(required_missing, "required missing Secret key", "absent required chart-owned Secret key")
    for source in ("init env", "envFrom", "volume items", "whole volume", "projected volume"):
        altered = copy.deepcopy(dev_docs)
        mutated = next(doc for doc in altered if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == target["metadata"]["name"])
        spec = mutated["spec"]["template"]["spec"]
        if source == "init env":
            spec.setdefault("initContainers", []).append({"name": "checksum-assert", "env": [{"name": "ADDITIONAL_KEY", "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "mailChannelToken"}}}]})
        elif source == "envFrom":
            spec["containers"][0].setdefault("envFrom", []).append({"secretRef": {"name": secret_name}})
        elif source in {"volume items", "whole volume"}:
            secret = {"secretName": secret_name}
            if source == "volume items":
                secret["items"] = [{"key": "mailChannelToken", "path": "token"}]
            spec.setdefault("volumes", []).append({"name": "checksum-assert", "secret": secret})
        else:
            spec.setdefault("volumes", []).append({"name": "checksum-assert", "projected": {"sources": [{"secret": {"name": secret_name, "items": [{"key": "mailChannelToken", "path": "token"}]}}]}})
        expect_rejected(altered, source, "checksum disagrees")
    print("OK: missing annotations and unaccounted init/envFrom/volume references are rejected")
    print("Secret checksum render assertions passed.")


try:
    main()
except AssertionError as error:
    raise SystemExit(f"FAIL: {error}") from None
except Exception:
    # YAML errors, subprocess diagnostics and tracebacks can quote credentials.
    raise SystemExit("FAIL: assertion script could not inspect the render; diagnostic content withheld") from None
PY

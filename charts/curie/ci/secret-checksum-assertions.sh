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


def consumed_keys(spec, secret_name, all_keys):
    keys = set()
    for container in list(spec.get("containers") or []) + list(spec.get("initContainers") or []):
        for env in container.get("env") or []:
            ref = (env.get("valueFrom") or {}).get("secretKeyRef") or {}
            if ref.get("name") == secret_name:
                keys.add(ref["key"])
        for env_from in container.get("envFrom") or []:
            if (env_from.get("secretRef") or {}).get("name") == secret_name:
                keys.update(all_keys)
    for volume in spec.get("volumes") or []:
        sources = [(volume.get("secret") or {}, "secretName")]
        sources.extend(
            (source.get("secret") or {}, "name")
            for source in (volume.get("projected") or {}).get("sources") or []
        )
        for source, name_field in sources:
            if source.get(name_field) == secret_name:
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
    encoded = json.dumps({key: content[key] for key in keys}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
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
        keys = consumed_keys(template.get("spec") or {}, secret_name, set(content))
        annotation = (template.get("metadata", {}).get("annotations") or {}).get(ANNOTATION)
        require(keys <= set(content), f"{label}: {kind}/{name} references an absent chart-owned Secret key")
        if keys:
            require(annotation is not None, f"{label}: {kind}/{name} lacks {ANNOTATION}")
            require(annotation == checksum(content, keys), f"{label}: {kind}/{name} checksum disagrees with its rendered consumed keys")
        else:
            require(annotation is None, f"{label}: {kind}/{name} hashes a Secret it does not consume")
        workloads[identity] = {
            "keys": keys,
            "checksum": annotation,
            "component": (doc.get("metadata", {}).get("labels") or {}).get("app.kubernetes.io/component"),
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
    print("OK: default sealed checksums match the emitted Secret and rendered consumption")

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
    print("OK: existingSecret paths hash only retained chart-owned references")

    secret_name, _ = chart_secret(dev_docs)
    extras = {
        workload: {"extraEnv": [{"name": "CHECKSUM_ASSERTION_EXTRA", "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "mailChannelToken"}}}]}
        for workload in ("api", "worker", "dispatcher", "otelCollector")
    }
    extra_workloads = audit(render("extraEnv Secret reference", merge(merge(dev, enabled), extras)), "extraEnv Secret reference")
    extra_consumers = {workload["component"] for workload in extra_workloads.values() if "mailChannelToken" in workload["keys"]}
    require({"api", "worker", "dispatcher", "otel-collector"} <= extra_consumers, "extraEnv references must be included in workload checksums")
    print("OK: rendered extraEnv Secret references participate in the checksum")

    # Negative controls start from a real render. Gaining any supported Secret
    # reference without changing the checksum must fail the same audit as CI.
    target = next(doc for doc in dev_docs if doc.get("kind") == "Deployment" and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "api")
    missing = copy.deepcopy(dev_docs)
    mutated = next(doc for doc in missing if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == target["metadata"]["name"])
    del mutated["spec"]["template"]["metadata"]["annotations"][ANNOTATION]
    expect_rejected(missing, "missing annotation", "lacks checksum/secret")
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

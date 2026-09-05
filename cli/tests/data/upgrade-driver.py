#!/usr/bin/python3
"""Recording external process boundary for the upgrade driver's CLI tests."""

import base64
import copy
import hashlib
import json
import os
import pathlib
import sys
import time

root = pathlib.Path(os.environ["UPGRADE_DRIVER_ROOT"])
scenario = os.environ["UPGRADE_DRIVER_SCENARIO"]
program = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]


def api_image(values):
    api = values.get("api", {})
    api = api if isinstance(api, dict) else {}
    image = api.get("image", {})
    image = image if isinstance(image, dict) else {}
    return (image.get("repository") or "ghcr.io/curie-eng/curie-api") + ":0.9.0"


# Capture and target-pinning recording boundary. No real credentials are used.
if program == "kubectl" and args[:2] == ["config", "view"]:
    view = {
        "apiVersion": "v1",
        "kind": "Config",
        "current-context": "acme-context",
        "clusters": [
            {
                "name": "acme-cluster",
                "cluster": {
                    "server": os.environ.get(
                        "UPGRADE_DRIVER_SERVER", "https://cluster.example.com"
                    ),
                    "certificate-authority-data": "Zml4dHVyZS1jYQ==",
                },
            }
        ],
        "contexts": [
            {"name": "acme-context", "context": {"cluster": "acme-cluster", "user": "acme-user"}}
        ],
        "users": [{"name": "acme-user", "user": {"token": "fixture-kubeconfig-token"}}],
    }
    if scenario == "target-config-forbidden":
        print("fixture-kubeconfig-token", file=sys.stderr)
        sys.exit(1)
    if scenario == "target-config-malformed":
        print("fixture-kubeconfig-token")
        sys.exit(0)
    if scenario == "target-config-unbound":
        view["current-context"] = "different-context"
    if scenario == "target-config-ambiguous":
        view["clusters"].append(copy.deepcopy(view["clusters"][0]))
    print(json.dumps(view))
    (root / "ambient-context-changed").write_text("https://changed.example.com")
    sys.exit(0)

if (scenario == "context-drift" or scenario.startswith("alias-image-")) and (
    root / "ambient-context-changed"
).exists():
    config_path = pathlib.Path(os.environ.get("KUBECONFIG", "/missing-snapshot"))
    config = json.loads(config_path.read_text())
    with (root / "snapshot-observations.jsonl").open("a") as log:
        log.write(
            json.dumps(
                {
                    "path": str(config_path),
                    "mode": config_path.stat().st_mode & 0o777,
                    "server": config["clusters"][0]["cluster"]["server"],
                }
            )
            + "\n"
        )
    assert config["clusters"][0]["cluster"]["server"] == "https://cluster.example.com"

if program == "kubectl" and args[:2] == ["get", "namespace"]:
    if scenario == "target-namespace-forbidden":
        print("fixture-kubeconfig-token", file=sys.stderr)
        sys.exit(1)
    if scenario in ("target-namespace-wrong", "target-namespace-missing-uid"):
        print(
            json.dumps(
                {
                    "kind": "Namespace",
                    "metadata": {
                        "name": "other" if scenario == "target-namespace-wrong" else args[2]
                    },
                }
            )
        )
        sys.exit(0)
    print(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": args[2],
                    "uid": os.environ.get("UPGRADE_DRIVER_NAMESPACE_UID", "acme-namespace-uid"),
                },
            }
        )
    )
    sys.exit(0)

retained_values = json.loads((root / "values.json").read_text())

workload = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "name": "acme-bot-api",
        "namespace": "upgrade-test",
        "generation": 2,
        "labels": {"app.kubernetes.io/instance": "acme-bot", "app.kubernetes.io/component": "api"},
    },
    "spec": {
        "replicas": 1,
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/instance": "acme-bot",
                "app.kubernetes.io/component": "api",
            }
        },
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "api",
                        "image": api_image(retained_values),
                        "env": [{"name": "TEST", "value": "retained"}],
                    }
                ]
            }
        },
    },
}
if scenario.startswith("init-image-"):
    workload["spec"]["template"]["spec"]["initContainers"] = [
        {"name": "init", "image": "busybox:1.36"}
    ]
database = {
    "apiVersion": "apps/v1",
    "kind": "StatefulSet",
    "metadata": {
        "name": "acme-bot-postgres",
        "generation": 1,
        "labels": {"app.kubernetes.io/instance": "acme-bot"},
    },
    "spec": {
        "replicas": 1,
        "serviceName": "acme-bot-postgres",
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/instance": "acme-bot",
                "app.kubernetes.io/component": "postgres",
            }
        },
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "postgres",
                        "image": "postgres:16-alpine",
                        "env": [
                            {
                                "name": "POSTGRES_DB",
                                "value": "other"
                                if scenario == "recovery-db-mismatch"
                                else "postgres",
                            },
                            {"name": "POSTGRES_USER", "value": "postgres"},
                        ],
                    }
                ]
            }
        },
    },
    "status": {"readyReplicas": 1, "updatedReplicas": 1, "observedGeneration": 1},
}
image_parity = scenario.startswith(("pinned-image-", "alias-image-"))
if scenario.startswith("pinned-image-"):
    database["spec"]["template"]["spec"]["containers"][0]["image"] = (
        "postgres:16-alpine@sha256:" + "a" * 64
    )
with (root / "calls.jsonl").open("a") as log:
    log.write(json.dumps([program, *args]) + "\n")


def recovery_hooks(values):
    marker = values.get("upgradeRecovery", {}).get("operationId", "")
    return [
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "acme-bot-" + component,
                "namespace": "upgrade-test",
                "labels": {
                    "app.kubernetes.io/component": component,
                    "curie-upgrade-operation": marker,
                },
                "annotations": {
                    "helm.sh/hook": events,
                    "helm.sh/hook-delete-policy": "before-hook-creation",
                    "helm.sh/hook-weight": weight,
                },
            },
            "spec": {
                "template": {
                    "metadata": {"labels": {"curie-upgrade-operation": marker}},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {"name": component, "image": "example.com/acme-phase:0.9.0"}
                        ],
                    },
                }
            },
        }
        for component, events, weight in [
            ("upgrade-drain", "pre-upgrade,pre-rollback", "-10"),
            ("schema-migrate", "post-install,pre-upgrade,pre-rollback", "-5"),
            ("upgrade-drain-release", "post-upgrade,post-rollback", "-10"),
        ]
    ]


pending = (root / "late-pending").exists()
if program == "helm" and args[0] == "status" and pending:
    hooks = [
        {
            "manifest": json.dumps(hook),
            "events": hook["metadata"]["annotations"]["helm.sh/hook"].split(","),
            "last_run": {"phase": "Succeeded"},
        }
        for hook in recovery_hooks(retained_values)
    ]
    if scenario == "unknown-hook":
        hooks.append(copy.deepcopy(hooks[0]))
    if scenario in ("test-only-hook", "extra-upgrade-hook", "extra-rollback-hook"):
        event = (
            "test"
            if scenario == "test-only-hook"
            else "post-upgrade"
            if scenario == "extra-upgrade-hook"
            else "pre-rollback"
        )
        hooks.append(
            {
                "events": [event],
                "manifest": json.dumps(
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "acme-readonly-probe",
                            "namespace": "upgrade-test",
                            "annotations": {"helm.sh/hook": event},
                        },
                    }
                ),
            }
        )
    print(
        json.dumps(
            {
                "name": "acme-bot",
                "namespace": "upgrade-test",
                "version": 3 if scenario == "wrong-revision" else 2,
                "info": {
                    "status": "pending-rollback"
                    if scenario == "pending-rollback"
                    else "pending-upgrade"
                },
                "hooks": hooks,
            }
        )
    )
    sys.exit(0)
if program == "helm" and args[0] == "rollback":
    assert args[2] == "2"
    (root / "late-pending").unlink()
    (root / "rollback-complete").write_text("3")
    sys.exit(0)
if program == "kubectl" and args[:2] == ["get", "secret"]:
    revision = args[2].split(".v")[-1]
    marker = (
        (root / "operation-label").read_text()
        if (root / "operation-label").exists()
        else "old-operation"
    )
    metadata = {
        "name": args[2],
        "namespace": "upgrade-test",
        "uid": "release-uid-" + revision,
        "resourceVersion": revision,
        "labels": {
            "owner": "helm",
            "name": "acme-bot",
            "version": revision,
            "status": "pending-upgrade"
            if pending and revision == "2"
            else ("failed" if scenario == "helm-failed" else "deployed"),
            "curie-upgrade-operation": marker,
        },
    }
    if scenario == "wrong-release-uid":
        metadata["uid"] = "replacement-uid"
    if scenario == "replaced-source-release" and revision == "1":
        metadata["uid"] = "replacement-source-uid"
    if scenario == "wrong-marker":
        metadata["labels"]["curie-upgrade-operation"] = "inherited-marker"
    print(json.dumps([metadata]))
    sys.exit(0)
if program == "kubectl" and args[:2] == ["get", "jobs,pods"]:
    objects = []
    for hook in recovery_hooks(retained_values):
        job = copy.deepcopy(hook)
        job["metadata"]["uid"] = job["metadata"]["name"] + "-uid"
        job["status"] = {
            "active": 0,
            "succeeded": 1,
            "conditions": [{"type": "Complete", "status": "True"}],
        }
        pod = {
            "kind": "Pod",
            "metadata": {
                "name": job["metadata"]["name"] + "-pod",
                "namespace": "upgrade-test",
                "labels": dict(job["spec"]["template"]["metadata"]["labels"]),
                "ownerReferences": [
                    {
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": job["metadata"]["name"],
                        "uid": job["metadata"]["uid"],
                        "controller": True,
                    }
                ],
            },
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{"name": "phase", "state": {"terminated": {"exitCode": 0}}}],
            },
        }
        objects.extend([job, pod])
    if scenario == "active-hook":
        objects[0]["status"]["active"] = 1
    if scenario == "malformed-hook-active":
        objects[0]["status"]["active"] = "1"
    if scenario == "replacement-hook-uid":
        objects[0]["metadata"]["uid"] = "replacement-job-uid"
        objects[1]["metadata"]["ownerReferences"][0]["uid"] = "replacement-job-uid"
    if scenario == "ephemeral-hook-running":
        objects[1]["status"]["ephemeralContainerStatuses"] = [
            {"name": "debug", "state": {"running": {}}}
        ]
    if scenario == "missing-hook":
        objects.pop(0)
    if scenario == "terminating-hook":
        objects[0]["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    if scenario == "wrong-pod-owner":
        objects[1]["metadata"]["ownerReferences"][0]["uid"] = "foreign-uid"
    if scenario == "orphan-hook-pod":
        orphan = copy.deepcopy(objects[1])
        orphan["metadata"]["ownerReferences"] = []
        objects.append(orphan)
    print(json.dumps({"items": objects}))
    sys.exit(0)

if program == "helm":
    if args[0] in ("status", "list"):
        installed = (
            (root / "installed-version").read_text()
            if (root / "installed-version").exists()
            else "0.8.5"
        )
        if scenario == "helm-forbidden":
            print("Error: Kubernetes API forbidden", file=sys.stderr)
            sys.exit(1)
        if args[0] == "list":
            print(
                json.dumps(
                    [
                        {
                            "name": "acme-bot",
                            "chart": f"curie-{installed}",
                            "app_version": installed,
                            "status": "deployed",
                        }
                    ]
                )
            )
        else:
            if scenario.startswith("source-status-"):
                status = {
                    "name": "acme-bot",
                    "namespace": "upgrade-test",
                    "version": 1,
                    "info": {"status": "deployed"},
                }
                if scenario == "source-status-wrong-name":
                    status["name"] = "acme-other"
                elif scenario == "source-status-wrong-namespace":
                    status["namespace"] = "other-test"
                elif scenario == "source-status-missing-revision":
                    status.pop("version")
                elif scenario == "source-status-zero-revision":
                    status["version"] = 0
                elif scenario == "source-status-string-revision":
                    status["version"] = "1"
                print(json.dumps(status))
                sys.exit(0)
            print(
                json.dumps(
                    {
                        "name": "acme-bot",
                        "namespace": "upgrade-test",
                        "version": 3
                        if (root / "rollback-complete").exists()
                        else (2 if (root / "installed-version").exists() else 1),
                        "info": {"status": "failed" if scenario == "helm-failed" else "deployed"},
                        "hooks": []
                        if scenario == "missing-hooks"
                        else [
                            {
                                "events": [event],
                                "last_run": {"phase": "Succeeded"},
                                "manifest": json.dumps(
                                    {
                                        "metadata": {
                                            "labels": {"app.kubernetes.io/component": component}
                                        }
                                    }
                                ),
                            }
                            for component, event in [
                                ("schema-migrate", "pre-upgrade"),
                                ("upgrade-drain", "pre-upgrade"),
                                ("upgrade-drain-release", "post-upgrade"),
                            ]
                        ],
                    }
                )
            )
    elif args[:2] == ["get", "metadata"]:
        # Helm3.16.4 status deliberately strips Chart; get metadata is separate.
        # https://github.com/helm/helm/blob/v3.16.4/cmd/helm/status.go
        # https://github.com/helm/helm/blob/v3.16.4/pkg/action/get_metadata.go
        if scenario == "source-metadata-hung":
            (root / "metadata-pid").write_text(str(os.getpid()))
            print("synthetic-source-metadata-secret-sentinel", file=sys.stderr, flush=True)
            time.sleep(60)
        if scenario == "source-metadata-denied":
            print("synthetic-source-metadata-secret-sentinel", file=sys.stderr)
            sys.exit(1)
        if scenario == "source-metadata-malformed":
            print("synthetic-source-metadata-secret-sentinel")
            sys.exit(0)
        installed = (
            (root / "installed-version").read_text()
            if (root / "installed-version").exists()
            else "0.8.5"
        )
        metadata = {
            "name": "acme-bot",
            "namespace": "upgrade-test",
            "revision": int(args[args.index("--revision") + 1]),
            "chart": "curie",
            "version": installed,
            "appVersion": installed,
            "status": "pending-upgrade" if pending else "deployed",
        }
        changes = {
            "source-metadata-wrong-name": ("name", "acme-other"),
            "source-metadata-wrong-namespace": ("namespace", "other-test"),
            "source-metadata-wrong-revision": ("revision", 99),
            "source-metadata-wrong-chart": ("chart", "other-chart"),
            "source-metadata-wrong-version": ("version", "0.7.0"),
            "source-metadata-failed": ("status", "failed"),
        }
        if scenario in changes:
            key, value = changes[scenario]
            metadata[key] = value
        if scenario == "source-metadata-missing-revision":
            metadata.pop("revision")
        print(json.dumps(metadata))
    elif args[:2] == ["show", "chart"]:
        version = "0.8.5" if scenario == "wrong-chart" else "0.9.0"
        print(f"name: curie\nversion: {version}\nappVersion: {version}")
    elif args[0] == "template":
        rendered_values = json.loads(pathlib.Path(args[args.index("-f") + 1]).read_text())
        rendered_image = api_image(rendered_values)
        if "templates/worker-upgrade-drain.yaml" in args or "templates/schema-migrate.yaml" in args:
            print(
                "\n---\n".join(
                    json.dumps(hook)
                    for hook in recovery_hooks(rendered_values)
                    if ("schema-migrate" in hook["metadata"]["name"])
                    == ("templates/schema-migrate.yaml" in args)
                )
            )
            sys.exit(0)
        if "templates/api.yaml" in args:
            # The actual api.yaml emits a Service followed by the Deployment.
            print(json.dumps({"apiVersion": "v1", "kind": "Service"}))
            if scenario == "rendered-api-missing":
                sys.exit(0)
            print("---")
            rendered = copy.deepcopy(workload)
            rendered["spec"]["template"]["spec"]["containers"][0]["image"] = (
                "example.com/acme-other-api:0.9.0"
                if scenario == "rendered-api-image-mismatch"
                else rendered_image
            )
            print(json.dumps(rendered))
            if scenario == "rendered-api-duplicate":
                print("---")
                print(json.dumps(rendered))
            sys.exit(0)
        metadata = {
            "schema_min": "0040",
            "schema_head": "0040",
            "revisions": [
                {"revision": "0039", "parents": [], "kind": "expand", "sha256": "a" * 64},
                {
                    "revision": "0040",
                    "parents": ["0039"],
                    "kind": "contract" if scenario == "schema-contract" else "expand",
                    "sha256": "b" * 64,
                },
            ],
        }
        data = {
            "application-version": "0.8.5" if scenario == "schema-metadata-mismatch" else "0.9.0",
            "compatibility.json": json.dumps(metadata),
            "api-image": "example.com/acme-other-api:0.9.0"
            if scenario == "metadata-image-mismatch"
            else rendered_image,
        }
        if scenario == "metadata-image-missing":
            data.pop("api-image")
        elif scenario == "metadata-image-empty":
            data["api-image"] = ""
        elif scenario == "metadata-image-invalid":
            data["api-image"] = ["invalid"]
        print(
            json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "acme-bot-schema-compat"},
                    "data": data,
                }
            )
        )
    elif args[:2] == ["get", "values"]:
        print((root / "values.json").read_text())
    elif args[:2] == ["get", "manifest"]:
        if scenario == "changed-pending-manifest":
            workload["spec"]["replicas"] = 2
        print(json.dumps(workload))
        if scenario.startswith("recovery-") or image_parity:
            if scenario == "recovery-foreign-namespace":
                database["metadata"]["namespace"] = "other-namespace"
            print("---")
            print(json.dumps(database))
            print("---")
            print(
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "kind": "Service",
                        "metadata": {"name": "acme-bot-postgres"},
                        "spec": {"ports": [{"name": "postgres", "port": 5432}]},
                    }
                )
            )
            if scenario == "recovery-duplicate-db":
                second = copy.deepcopy(database)
                second["metadata"]["name"] = "acme-bot-other-postgres"
                print("---")
                print(json.dumps(second))
        if scenario == "secret-string-data":
            print("---")
            print(
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "kind": "Secret",
                        "metadata": {"name": "acme-bot-secret"},
                        "stringData": {"key": "fixture-secret-value"},
                    }
                )
            )
    elif args[0] == "upgrade":
        if scenario == "owner-death":
            witness = root / "helm-owner.pending"
            witness.write_text(json.dumps({"pid": os.getpid(), "parent": os.getppid()}))
            witness.rename(root / "helm-owner.json")
            deadline = time.monotonic() + 15
            while not (root / "release-helm").exists():
                if time.monotonic() > deadline:
                    sys.exit(70)
                time.sleep(0.01)
            (root / "after-owner-mutation").write_text("mutation after owner termination")
        if scenario == "helm-hook-fails":
            sys.exit(1)
        if "-f" in args:
            values = pathlib.Path(args[args.index("-f") + 1]).read_text()
            (root / "applied-values.json").write_text(values)
            (root / "values.json").write_text(values)
        (root / "installed-version").write_text("0.9.0")
        (root / "api-unavailable").unlink(missing_ok=True)
        if scenario in ("late-pending", "zero-exit-pending"):
            (root / "operation-label").write_text(
                args[args.index("--labels") + 1].split("=", 1)[1]
                if "--labels" in args
                else "missing"
            )
            (root / "late-pending").write_text("pending")
            sys.exit(0 if scenario == "zero-exit-pending" else 1)
        if scenario == "helm-success-reply-lost":
            sys.exit(1)
    else:
        print("unsupported recording Helm command", file=sys.stderr)
        sys.exit(64)
elif program == "kubectl":
    if args[:2] == ["get", "configmap"]:
        record = root / "record.json"
        if record.exists():
            if "json" in args:
                version = (
                    (root / "record-version").read_text()
                    if (root / "record-version").exists()
                    else "1"
                )
                print(
                    json.dumps(
                        {
                            "metadata": {
                                "resourceVersion": version,
                                "uid": "wrong-checkpoint"
                                if scenario == "wrong-checkpoint-uid"
                                else "checkpoint-uid",
                            },
                            "data": {"record": record.read_text()},
                        }
                    )
                )
                if scenario == "checkpoint-conflict":
                    (root / "record-version").write_text(str(int(version) + 1))
            else:
                print(record.read_text())
        elif "--ignore-not-found" not in args:
            print("Error from server (NotFound): configmaps not found", file=sys.stderr)
            sys.exit(1)
    elif args[0] in ("apply", "create", "replace"):
        if scenario == "checkpoint-fails":
            print("Error from server (Forbidden): checkpoint write denied", file=sys.stderr)
            sys.exit(1)
        manifest = json.loads(pathlib.Path(args[args.index("-f") + 1]).read_text())
        version = (
            int((root / "record-version").read_text()) if (root / "record-version").exists() else 0
        )
        if args[0] == "replace" and manifest["metadata"].get("resourceVersion") != str(version):
            print("Error from server (Conflict): stale resource version", file=sys.stderr)
            sys.exit(1)
        if args[0] == "create" and (root / "record.json").exists():
            sys.exit(1)
        version += 1
        (root / "record-version").write_text(str(version))
        (root / "record.json").write_text(manifest["data"]["record"])
        record = json.loads(manifest["data"]["record"])
        if scenario.startswith("interrupt-after-") and record["completed"][
            -1
        ] == scenario.removeprefix("interrupt-after-"):
            sys.exit(1)
        print(json.dumps({"metadata": {"resourceVersion": str(version), "uid": "checkpoint-uid"}}))
    elif args[:2] == ["get", "deploy,sts,ds"] or (args[0] == "get" and "-f" in args):
        if scenario == "pause-after-apply" and not (root / "release-observation").exists():
            record = json.loads((root / "record.json").read_text())
            if record["completed"][-1] == "apply":
                marker = root / "observation-owner.pending"
                marker.write_text(json.dumps({"pid": os.getpid(), "parent": os.getppid()}))
                marker.rename(root / "observation-owner.json")
                deadline = time.monotonic() + 15
                while not (root / "release-observation").exists():
                    assert time.monotonic() < deadline, "observation not released"
                    time.sleep(0.01)
        workload["status"] = {
            "readyReplicas": 1,
            "updatedReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 2,
        }
        if scenario == "stale-generation":
            workload["status"]["observedGeneration"] = 1
        if scenario == "wrong-image" or (root / "resume-image-drift").exists():
            workload["spec"]["template"]["spec"]["containers"][0]["image"] = (
                "ghcr.io/curie-eng/curie-api:0.8.5"
            )
        if scenario == "wrong-manifest":
            workload["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = "drift"
        if scenario == "missing-object":
            if "--ignore-not-found" not in args:
                sys.exit(1)
            print(json.dumps({"items": []}))
            sys.exit(0)
        objects = [workload]
        if scenario.startswith("recovery-") or image_parity:
            objects.append(database)
            objects.append(
                {
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"name": "acme-bot-postgres"},
                    "spec": {"ports": [{"name": "postgres", "port": 5432}]},
                }
            )
        if scenario == "secret-string-data":
            objects.append(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": "acme-bot-secret"},
                    "data": {"key": base64.b64encode(b"fixture-secret-value").decode()},
                }
            )
        print(json.dumps({"items": objects}))
    elif args[:2] == ["get", "deployment"]:
        print(
            json.dumps(
                {
                    "status": {
                        "readyReplicas": 1 if scenario == "recovery-running-api-probe-fails" else 0
                    }
                }
            )
        )
    elif args[:2] == ["get", "statefulset"]:
        database["metadata"]["namespace"] = "upgrade-test"
        database["metadata"]["annotations"] = {
            "meta.helm.sh/release-name": "acme-bot",
            "meta.helm.sh/release-namespace": "upgrade-test",
        }
        print(json.dumps(database))
    elif args[:2] == ["get", "node"]:
        if scenario == "alias-image-denied":
            print("fixture-private-node-denial", file=sys.stderr)
            sys.exit(1)
        names = [
            "docker.io/library/postgres:16-alpine",
            "docker.io/library/acme-postgres-alias:fixture",
            "docker.io/library/import-fixture@sha256:" + "a" * 64,
        ]
        images = [{"names": names}]
        if scenario == "alias-image-split":
            images = [{"names": names[:1]}, {"names": names[1:]}]
        if scenario == "alias-image-ambiguous":
            images.append({"names": names})
        if scenario == "alias-image-missing":
            images = []
        print(
            json.dumps(
                {
                    "kind": "Node",
                    "metadata": {
                        "name": "other-node"
                        if scenario == "alias-image-wrong-node"
                        else "acme-node"
                    },
                    "status": {"images": images},
                }
            )
        )
    elif args[:2] == ["get", "pods"]:
        pods = []
        workloads = [workload] + (
            [database] if scenario.startswith("recovery-") or image_parity else []
        )
        for item in workloads:
            containers = copy.deepcopy(item["spec"]["template"]["spec"]["containers"])
            statuses = [
                {
                    "name": container["name"],
                    "image": (
                        "docker.io/library/" + container["image"]
                        if container["name"] == "postgres"
                        else container["image"]
                    ),
                    "imageID": "containerd://sha256:" + "d" * 64,
                    "ready": True,
                    "state": {"running": {}},
                }
                for container in containers
            ]
            pods.append(
                {
                    "metadata": {
                        "name": item["metadata"]["name"] + "-pod",
                        "labels": item["spec"]["selector"]["matchLabels"],
                    },
                    "spec": {"containers": containers},
                    "status": {"phase": "Running", "containerStatuses": statuses},
                }
            )
        if image_parity:
            postgres_pod = pods[1]
            postgres_pod["spec"]["nodeName"] = "acme-node"
            status = postgres_pod["status"]["containerStatuses"][0]
            # Containerd may report a config SHA in image and RepoDigest in imageID.
            # https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#ContainerStatus
            status["image"] = "sha256:" + "b" * 64
            status["imageID"] = "docker.io/library/postgres@sha256:" + "a" * 64
            if scenario.startswith("alias-image-"):
                status["image"] = "docker.io/library/acme-postgres-alias:fixture"
                status["imageID"] = "docker.io/library/import-fixture@sha256:" + "a" * 64
            if scenario.endswith("wrong-digest"):
                status["imageID"] = status["imageID"].replace("a" * 64, "c" * 64)
            if scenario.endswith("wrong-repository"):
                status["imageID"] = "docker.io/library/foreign@sha256:" + "a" * 64
            if scenario.endswith("opaque"):
                status["imageID"] = "containerd://sha256:" + "a" * 64
            if scenario.endswith("pod-drift"):
                postgres_pod["spec"]["containers"][0]["image"] = "postgres:15-alpine"
        if scenario.startswith("init-image-"):
            pods[0]["spec"]["initContainers"] = copy.deepcopy(
                workload["spec"]["template"]["spec"]["initContainers"]
            )
            pods[0]["status"]["initContainerStatuses"] = [
                {
                    "name": "init",
                    "image": "docker.io/library/busybox:1.36",
                    "imageID": ""
                    if scenario == "init-image-missing-id"
                    else "containerd://sha256:" + "e" * 64,
                    "state": {"terminated": {"exitCode": 0}},
                }
            ]
        if scenario == "wrong-running-image":
            pods[0]["status"]["containerStatuses"][0]["image"] = "ghcr.io/curie-eng/curie-api:0.8.5"
        if scenario == "missing-running-image-id":
            pods[0]["status"]["containerStatuses"][0]["imageID"] = ""
        if scenario == "missing-running-pod":
            pods = []
        if scenario == "stale-extra-pod":
            old = copy.deepcopy(pods[0])
            old["metadata"]["name"] = "acme-bot-api-old"
            old["spec"]["containers"][0]["image"] = "ghcr.io/curie-eng/curie-api:0.8.5"
            old["status"]["containerStatuses"][0]["image"] = "ghcr.io/curie-eng/curie-api:0.8.5"
            pods.append(old)
        print(json.dumps({"items": pods}))
    elif args[0] == "exec":
        if "upgrade-database-recovery" in args:
            if scenario == "recovery-db-fails":
                sys.exit(1)
            print(
                json.dumps(
                    {
                        "current_revision": "0040"
                        if scenario == "recovery-db-advanced"
                        else "9999"
                        if scenario == "recovery-db-unknown"
                        else "0039",
                        "database_name": "other"
                        if scenario == "recovery-live-catalog-mismatch"
                        else "postgres",
                    }
                )
            )
            sys.exit(0)
        if "upgrade-schema" in args:
            if (root / "api-unavailable").exists():
                sys.exit(1)
            if scenario == "schema-probe-fails":
                sys.exit(1)
            print(
                json.dumps(
                    {
                        "current_revision": None
                        if scenario == "schema-null"
                        else "unknown"
                        if scenario == "schema-unknown"
                        else (
                            "0039"
                            if scenario == "schema-not-target"
                            else ("0040" if (root / "installed-version").exists() else "0039")
                        ),
                        "source_head": "0039",
                        "database_endpoint_fingerprint": hashlib.sha256(
                            json.dumps(
                                ["acme-bot-postgres", 5432, "postgres"], separators=(",", ":")
                            ).encode()
                        ).hexdigest(),
                        "source_revisions": {
                            "0039": ("c" if scenario == "schema-content-mismatch" else "a") * 64,
                            "0040": "b" * 64,
                        },
                    }
                )
            )
            sys.exit(0)
        if scenario == "canary-fails" and "upgrade-canary" in args:
            sys.exit(1)
        if "upgrade-canary" in args or "upgrade-source-canary" in args:
            print(
                json.dumps(
                    {
                        "passed": True,
                        "agents_fingerprint": (
                            "b"
                            if (scenario == "lost-agents" and "upgrade-canary" in args)
                            or scenario == "additional-agents"
                            else "a"
                        )
                        * 64,
                    }
                )
            )
        elif "upgrade-queue-probe" in args:
            print(json.dumps({"queues_drained": True}))
        else:
            sys.exit(64)
    elif args[:2] == ["get", "deploy"]:
        pass
    else:
        print("unsupported recording kubectl command", file=sys.stderr)
        sys.exit(64)

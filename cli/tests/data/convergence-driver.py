#!/usr/bin/python3
"""Recording external process boundary; never claims a real Kubernetes run."""

import copy
import json
import os
import sys
import time
from pathlib import Path

root = Path(os.environ["CONVERGENCE_DRIVER_ROOT"])
scenario = os.environ["CONVERGENCE_DRIVER_SCENARIO"]
program = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / "calls.jsonl").open("a") as calls:
    calls.write(json.dumps([program, *args]) + "\n")

mixed = json.loads((root / "mixed-rollout.json").read_text())
deployment = copy.deepcopy(mixed)
deployment["status"].update(replicas=1, unavailableReplicas=0)
deployment["status"]["conditions"] = [{"type": "Available", "status": "True"}]
statefulset = copy.deepcopy(deployment)
statefulset["kind"] = "StatefulSet"
statefulset["metadata"]["name"] = "acme-bot-valkey"
statefulset["spec"]["selector"]["matchLabels"]["component"] = "valkey"
statefulset["spec"]["template"]["spec"]["containers"] = [{"name": "valkey", "image": "valkey:8"}]
statefulset["status"].update(currentRevision="current", updateRevision="current")
if scenario == "foreign-hook":
    statefulset["metadata"]["namespace"] = "another-managed-namespace"
# Containerd may report a config digest in ContainerStatus.image even though
# imageID names the requested repository manifest digest. Observed on the real
# September 2026 cluster run; Kubernetes allows runtime-resolved image spellings:
# https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#ContainerStatus
pinned_image = "acme-postgres:16@sha256:" + "a" * 64
pinned_id = "docker.io/library/acme-postgres@sha256:" + "a" * 64
if scenario.startswith("pinned-"):
    statefulset["spec"]["template"]["spec"]["containers"][0]["image"] = pinned_image
    deployment["spec"]["template"]["spec"]["initContainers"] = [
        {"name": "wait-for-postgres", "image": pinned_image}
    ]
alias_image = "example.com/custom-api:sibling"
alias_id = "example.com/imported-api@sha256:" + "d" * 64
if scenario.startswith("alias-"):
    deployment["spec"]["template"]["spec"]["initContainers"] = [
        {"name": "init-api", "image": "example.com/custom-api:target"}
    ]
expected = [copy.deepcopy(deployment), copy.deepcopy(statefulset)]
for item in expected:
    item.pop("status", None)
    item["metadata"].pop("generation", None)

if scenario == "empty-target":
    expected = []


def emit(value):
    print(json.dumps(value))
    sys.exit(0)


if program == "helm":
    if args[:2] == ["get", "values"]:
        emit({"security": {"allowDevDefaults": True}})
    if args[:2] == ["get", "manifest"]:
        # JSON documents are YAML documents too; no optional YAML dependency.
        print("\n---\n".join(json.dumps(item) for item in expected))
        sys.exit(0)
    if args[0] == "status":
        if "json" in args:
            if scenario == "degraded-then-hung":
                counter = root / "status-observations"
                count = int(counter.read_text()) + 1 if counter.exists() else 1
                counter.write_text(str(count))
                if count >= 3:
                    (root / "hung-pid").write_text(str(os.getpid()))
                    time.sleep(30)
            if scenario == "hung-read":
                (root / "hung-pid").write_text(str(os.getpid()))
                time.sleep(30)
            hooks = []
            if scenario in ["hook-failed", "helm-hook-fails", "foreign-hook"]:
                hooks = [
                    {
                        "name": "acme-bot-migrate",
                        "kind": "Job",
                        "events": ["pre-upgrade"],
                        "last_run": {
                            "phase": "Succeeded" if scenario == "foreign-hook" else "Failed"
                        },
                        "manifest": json.dumps(
                            {
                                "kind": "Job",
                                "metadata": {
                                    "name": "acme-bot-migrate",
                                    "namespace": "convergence-test",
                                },
                            }
                        ),
                    }
                ]
            emit(
                {
                    "version": 2,
                    "info": {"status": "failed" if scenario == "release-failed" else "deployed"},
                    "hooks": hooks,
                }
            )
        print("STATUS: deployed\nREVISION: 2")
        sys.exit(0)
    if args[0] == "template":
        print(
            "Error: could not find template " + args[args.index("--show-only") + 1] + " in chart",
            file=sys.stderr,
        )
        sys.exit(1)
    if args[0] == "upgrade":
        if scenario == "helm-hook-fails":
            print("Error: pre-upgrade hook failed", file=sys.stderr)
            sys.exit(1)
        print("Release accepted")
        sys.exit(0)
    if args[:2] == ["show", "chart"]:
        print("name: curie\nversion: 0.8.5\nappVersion: 0.8.5")
        sys.exit(0)

if program == "kubectl":
    if args[:2] == ["get", "node"]:
        if scenario == "alias-forbidden":
            print("Forbidden PRIVATE_MESSAGE_SENTINEL", file=sys.stderr)
            sys.exit(1)
        names = ["example.com/custom-api:target", alias_image, alias_id]
        images = [{"names": names}]
        if scenario == "alias-split-entry":
            images = [{"names": names[:2]}, {"names": names[2:]}]
        if scenario == "alias-ambiguous":
            images.append({"names": [names[0], "example.com/imported-api@sha256:" + "e" * 64]})
        if scenario == "alias-missing-entry":
            images = []
        if scenario == "alias-no-reported-alias":
            images = [{"names": [names[0], names[2]]}]
        emit(
            {
                "kind": "Node",
                "metadata": {
                    "name": "acme-other-node" if scenario == "alias-wrong-node" else args[2]
                },
                "status": {"images": images},
            }
        )
    if "namespace" in args or "namespaces" in args:
        emit({"kind": "Namespace", "metadata": {"name": args[-1]}})
    if "config" in args:
        print("https://127.0.0.1:6443")
        sys.exit(0)
    if "label" in args:
        sys.exit(0)
    if any(
        a in args
        for a in ["service", "services", "svc", "nodes", "priorityclass", "priorityclasses"]
    ):
        emit({"items": []})
    if scenario == "mixed":
        deployment = mixed
    if scenario == "stale-generation":
        deployment["status"]["observedGeneration"] = 1
    if scenario == "updated-short":
        deployment["status"]["updatedReplicas"] = 0
    if scenario == "ready-short":
        deployment["status"]["readyReplicas"] = 0
    if scenario == "unavailable":
        deployment["status"]["unavailableReplicas"] = 1
    if scenario == "statefulset-old":
        statefulset["status"]["currentRevision"] = "old"
    if scenario == "target-drift":
        deployment["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "example.com/custom-api:other"
        )
    if scenario == "converges-later":
        counter = root / "observations"
        count = int(counter.read_text()) if counter.exists() else 0
        if any("deployment" in arg for arg in args):
            count += 1
            counter.write_text(str(count))
        if count <= 1:
            deployment["status"]["observedGeneration"] = 1

    if scenario in ["scaled", "scaled-surplus"]:
        deployment["spec"]["replicas"] = 3
        deployment["status"].update(replicas=3, readyReplicas=3, updatedReplicas=3)
    workloads = [deployment, statefulset]
    pods = []
    for workload in workloads:
        containers = copy.deepcopy(workload["spec"]["template"]["spec"]["containers"])
        statuses = [
            {
                "name": c["name"],
                "image": "docker.io/library/valkey:8" if c["name"] == "valkey" else c["image"],
                "imageID": "containerd://sha256:example",
                "ready": True,
                "state": {"running": {}},
            }
            for c in containers
        ]
        pods.append(
            {
                "kind": "Pod",
                "metadata": {
                    "name": workload["metadata"]["name"] + "-new",
                    "namespace": workload["metadata"]["namespace"],
                    "labels": workload["spec"]["selector"]["matchLabels"],
                },
                "spec": {"containers": containers},
                "status": {"phase": "Running", "containerStatuses": statuses},
            }
        )
    if scenario.startswith("pinned-"):
        pods[0]["spec"]["initContainers"] = copy.deepcopy(
            deployment["spec"]["template"]["spec"]["initContainers"]
        )
        pods[0]["status"]["initContainerStatuses"] = [
            {
                "name": "wait-for-postgres",
                "image": "sha256:" + "c" * 64,
                "imageID": pinned_id,
                "ready": True,
                "state": {"terminated": {"exitCode": 0, "reason": "Completed"}},
            }
        ]
        pods[1]["status"]["containerStatuses"][0].update(
            image="sha256:" + "c" * 64, imageID=pinned_id
        )
        pinned_statuses = [
            pods[0]["status"]["initContainerStatuses"][0],
            pods[1]["status"]["containerStatuses"][0],
        ]
        if scenario == "pinned-pullable":
            for status in pinned_statuses:
                status["imageID"] = "docker-pullable://" + pinned_id
        if scenario == "pinned-wrong-digest":
            for status in pinned_statuses:
                status["image"] = pinned_image  # Name alone cannot override a wrong digest.
                status["imageID"] = pinned_id.replace("a" * 64, "b" * 64)
        if scenario == "pinned-wrong-repository":
            for status in pinned_statuses:
                status["imageID"] = pinned_id.replace("acme-postgres", "other-postgres")
        if scenario == "pinned-opaque-id":
            for status in pinned_statuses:
                status["imageID"] = "containerd://sha256:" + "c" * 64
        if scenario == "pinned-pod-drift":
            pods[1]["spec"]["containers"][0]["image"] = pinned_image.replace("a" * 64, "b" * 64)
        if scenario == "pinned-init-failed":
            pinned_statuses[0]["state"]["terminated"]["exitCode"] = 1
    if scenario.startswith("alias-"):
        pods[0]["spec"]["nodeName"] = "acme-node"
        pods[0]["status"]["containerStatuses"][0].update(image=alias_image, imageID=alias_id)
        pods[0]["spec"]["initContainers"] = copy.deepcopy(
            deployment["spec"]["template"]["spec"]["initContainers"]
        )
        pods[0]["status"]["initContainerStatuses"] = [
            {
                "name": "init-api",
                "image": alias_image,
                "imageID": alias_id,
                "state": {"terminated": {"exitCode": 0}},
            }
        ]
        if scenario == "alias-wrong-digest":
            pods[0]["status"]["containerStatuses"][0]["imageID"] = alias_id.replace(
                "d" * 64, "e" * 64
            )
        if scenario == "alias-wrong-repository":
            pods[0]["status"]["containerStatuses"][0]["imageID"] = alias_id.replace(
                "imported-api", "other-api"
            )
        if scenario == "alias-opaque-id":
            pods[0]["status"]["containerStatuses"][0]["imageID"] = "containerd://sha256:" + "d" * 64
        if scenario == "alias-pod-drift":
            pods[0]["spec"]["containers"][0]["image"] = "example.com/custom-api:old"
        if scenario == "alias-init-failed":
            pods[0]["status"]["initContainerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    if scenario in ["healthy-sidecar", "unready-sidecar"]:
        pods[0]["spec"]["containers"].append(
            {"name": "mesh-proxy", "image": "example.com/proxy:custom"}
        )
        pods[0]["status"]["containerStatuses"].append(
            {"name": "mesh-proxy", "ready": scenario == "healthy-sidecar", "state": {"running": {}}}
        )
    if scenario in ["scaled", "scaled-surplus"]:
        for index in range(2 if scenario == "scaled" else 3):
            pod = copy.deepcopy(pods[0])
            pod["metadata"]["name"] = "acme-bot-api-scaled-" + str(index)
            pods.append(pod)
    if scenario in ["mixed", "surplus"]:
        old = copy.deepcopy(pods[0])
        old["metadata"]["name"] = "acme-bot-api-old"
        old["spec"]["containers"][0]["image"] = "example.com/custom-api:old"
        old["status"]["containerStatuses"][0]["image"] = "example.com/custom-api:old"
        pods.append(old)
    if scenario == "mixed":
        pods[0]["status"]["containerStatuses"][0].update(
            ready=False,
            state={
                "waiting": {"reason": "CrashLoopBackOff", "message": "PRIVATE_MESSAGE_SENTINEL"}
            },
        )
    if scenario == "degraded-then-hung":
        pods[0]["status"]["containerStatuses"][0].update(
            ready=False,
            state={
                "waiting": {"reason": "ImagePullBackOff", "message": "PRIVATE_MESSAGE_SENTINEL"}
            },
        )
    if scenario == "wrong-image":
        pods[0]["status"]["containerStatuses"][0]["image"] = "example.com/custom-api:old"
    if scenario == "missing-image-status":
        pods[0]["status"]["containerStatuses"][0]["imageID"] = ""
    if scenario == "init-failed":
        pods[0]["status"]["initContainerStatuses"] = [
            {
                "name": "migrate",
                "state": {
                    "waiting": {"reason": "CrashLoopBackOff", "message": "PRIVATE_MESSAGE_SENTINEL"}
                },
            }
        ]
    if scenario == "container-failed":
        pods[0]["status"]["containerStatuses"][0]["state"] = {
            "terminated": {
                "reason": "OOMKilled",
                "exitCode": 137,
                "message": "PRIVATE_MESSAGE_SENTINEL",
            }
        }
    if scenario == "private-pod-reason":
        pods[0]["status"]["reason"] = "PRIVATE_MESSAGE_SENTINEL"
    jobs = (
        [
            {
                "kind": "Job",
                "metadata": {
                    "name": "acme-bot-migrate",
                    "namespace": "another-managed-namespace"
                    if scenario == "foreign-hook"
                    else "convergence-test",
                },
                "status": {
                    "failed": 1,
                    "conditions": [
                        {
                            "type": "Failed",
                            "status": "True",
                            "reason": "BackoffLimitExceeded",
                            "message": "PRIVATE_MESSAGE_SENTINEL",
                        }
                    ],
                },
            }
        ]
        if scenario in ["hook-failed", "helm-hook-fails", "foreign-hook"]
        else []
    )
    if any(a.startswith("deployment") for a in args):
        if scenario == "missing-workload":
            workloads = [statefulset]
        target_namespace = args[args.index("-n") + 1]
        emit(
            {
                "items": [
                    item
                    for item in [*workloads, *pods, *jobs]
                    if item["metadata"].get("namespace", "convergence-test") == target_namespace
                ]
            }
        )
    if "pods" in args:
        emit({"items": pods})
    if "jobs" in args or any(a.startswith("job/") for a in args):
        emit({"items": jobs})
    if "statefulset" in args:
        emit({"items": []})

print("unhandled recording command: " + repr([program, *args]), file=sys.stderr)
sys.exit(64)

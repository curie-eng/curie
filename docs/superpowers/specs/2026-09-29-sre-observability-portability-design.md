# SRE observability portability and log-collection failure design

Status: design for #1912. Target: `main`.

## Purpose and current state

An operator should be able to install the example observability stack on a
supported non-k3s cluster and see a useful failure if storage or log collection
does not work. The four PVC values already defer to the cluster's default
StorageClass; that part of the report is complete in current `main` and must
stay so. The remaining source values still mount only CRI log paths and run
`stage.cri`, while the manual path does not explain the required Grafana admin
Secret. Alloy can be Ready while reading no files.

## Runtime selection and configuration

Before any cluster mutation, the three SRE observability install paths inspect
all nodes on which the Alloy DaemonSet can run, including cordoned and NotReady
nodes, using `kubectl get nodes -o json`. Capacity planning still considers
only Ready, schedulable nodes. The Kubernetes Node
API reports `status.nodeInfo.containerRuntimeVersion` (for example,
`containerd://...` or `docker://...`). A uniform containerd or CRI-O runtime
keeps the existing CRI configuration. A uniform Docker runtime renders the
embedded Alloy values with the Docker container-log mount and `stage.docker`.
Missing, unsupported, or mixed runtimes fail with the offending node names and
runtime types before Helm starts; do not silently choose one format for a mixed
DaemonSet. `--dry-run`, including the full bot install path, does not read node
runtimes or mutate the cluster and states that runtime
selection occurs on the live run. Manual values-file installation remains
possible, with the two Docker edits and the supported runtime assumption
documented beside the example.

## Named failures

On a Helm wait timeout or Tempo StatefulSet rollout timeout, inspect Pending
PVCs and Warning events in the target namespace. The reported failure may be
a Pod `FailedScheduling` event rather than an event on the PVC: correlate Pods
that reference a Pending claim, then accept only a matching Pod UID event
whose message names an unbound claim. Bound these diagnostic reads by
wall-clock time and match PVC events by namespace and UID where available.
Include the PVC name and relevant event reason/message in the CLI failure.
Only present Helm pending-upgrade Secret cleanup as a conditional step after
verifying that release state; it is not a PVC remedy. If the read fails or no
PVC is Pending, keep the original command error and provide the exact
`kubectl get/describe pvc` diagnostic command. This is diagnostic only:
do not delete or mutate a PVC automatically. Document that manual Grafana
installation requires Secret `grafana-admin` with keys `admin-user` and
`admin-password`; the installer preserves or creates it itself and must never
print either value.

## Make silent collection observable

Annotate Alloy DaemonSet pods for the shipped Prometheus pod scrape job. Add
alerts keyed per Alloy pod, not a load-balanced Service: (1) an Alloy pod is
scrapeable but its `loki.source.file` component reports zero active files, or
omits that metric, for 10 minutes; (2) it read log lines in a 15-minute window
but sent none to Loki, sustained for 5 minutes. A quiet pod with active files
and no new lines is not a failure. Alert
annotations name the likely mount/runtime or downstream delivery checks, not
just "no logs". The metric and label contract is checked against the pinned
Alloy chart/image, rather than inferred from a text fixture.

## Verification and limits

Test runtime selection with containerd, CRI-O, Docker, mixed, missing,
cordoned, and NotReady-node inputs. Test both rendered values and the real Helm
chart output. Test Helm and Tempo PVC diagnostics with a matching Pending PVC
Warning event, the reported Pod `FailedScheduling` event, a stale UID, and a
non-PVC timeout. Use `promtool` to prove the
zero-file and read-without-send
alerts fire, and that a healthy/quiet collector does not. Run CLI lint/tests,
the observability chart assertions, and the required local, local-release,
cluster and external-integration tiers on the final candidate. No PR or issue
closure is justified if a required tier remains unverified.

## Source contracts

- Kubernetes Node `containerRuntimeVersion`:
  https://kubernetes.io/docs/reference/kubernetes-api/core/node-v1/
- Alloy file-source active/read metrics:
  https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.file/
- Alloy write sent-entry metric:
  https://grafana.com/docs/alloy/latest/reference/components/loki/loki.write/
- Alloy CRI and Docker processing stages:
  https://grafana.com/docs/alloy/latest/reference/components/loki/loki.process/

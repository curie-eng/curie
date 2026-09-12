#!/usr/bin/python3
"""Recording helm/kubectl boundary for `curie cluster upgrade`.

Installed under both names; dispatch is on ``sys.argv[0]``. Every invocation is
appended to ``$UPGRADE_DRIVER_ROOT/argv.log`` as one JSON array per line, so a
test can assert on the commands that were actually issued (and, for a refusal,
on the commands that were *not*). Never claims a real Kubernetes run.

Payload capture:
  * every ``-f <file>`` helm receives is copied to ``values-<n>.yaml``
  * every ``kubectl apply -f <file>`` manifest is copied to ``applied-<n>.json``

``helm get values`` returns the most recently APPLIED overlay when one exists,
falling back to ``retained.json``. A real release retains what was last handed
to it, and that is what makes a second run a resume of the first rather than a
replay of the same input.

Behaviour is selected by ``$UPGRADE_DRIVER_SCENARIO`` from SCENARIOS below.
Optional per-test inputs read from the root directory:
  * ``retained.json`` -- the document ``helm get values`` returns (JSON is YAML)
  * ``checkpoint.json`` -- the record served as the upgrade checkpoint ConfigMap

Three observable moments drive the fixtures, derived from the argv log rather
than from a call ordinal (the number of `helm status` reads is an
implementation detail of the observer):

  before    -- nothing has been mutated yet
  applied   -- a ``helm upgrade`` has been issued
  converged -- the convergence observation has read the live workloads

`convergence.observe` compares live containers against Helm's INSTALLED
manifest, never against ``--to``. So ``target`` (what ``helm get manifest``
renders) and ``served_*`` (what the live pods run) are separate knobs: a
fixture that moves them together can never make ``images`` false.
"""

import copy
import json
import os
import shutil
import sys
from pathlib import Path

RELEASE = "rel"
NAMESPACE = "ns"
REPO = "ghcr.io/curie-eng/curie-api"

# The exact resource selector `convergence::workloads_command` issues. The old
# `cluster upgrade` stub read `deploy,sts,ds`; serving only this string means a
# fixture cannot be satisfied by that narrower stub.
WORKLOADS = "deployments,statefulsets,daemonsets,pods,jobs"

# One dict, not a pile of branches. Keys:
#   before/after      chart version `helm status` reports pre/post `helm upgrade`
#   after_converge    chart version `helm status` reports once convergence has
#                     observed the live workloads; defaults to `after`. Only a
#                     version that moves BETWEEN Apply and Canary can prove the
#                     canary re-reads it instead of trusting its own bookkeeping.
#   served_before/after  container image tag actually running pre/post upgrade
#   target            image tag the rendered target manifest asks for
#   show_chart        version `helm show chart <local path>` reports
#   ready/updated/unavailable  replica counts on the live Deployment
#   generation_drift  live `status.observedGeneration` lags `metadata.generation`
#   workloads_fail    the `kubectl get <WORKLOADS>` read exits non-zero, so the
#                     observation cannot be MADE at all (distinct from a
#                     terminal condition, which is an observation with a verdict)
#   values_fail       `helm get values` exits non-zero with stderr that merely
#                     CONTAINS "not found" -- the release state is unknown, not
#                     positively absent
#   values_drift      the SECOND and later `helm get values` return a different
#                     document, so a re-read anywhere after Validate is visible
#   failed_hook       ""                    both pre-upgrade hooks succeed
#                     "upgrade-drain"       the #2010 drain gate Job fails
#                     "schema-migrate"      a NON-drain pre-upgrade hook fails
#                     Ruling 13: `queues_drained` binds to the upgrade-drain
#                     hook alone and `hooks_healthy` to every other hook, so the
#                     two are only provably distinct if the fixture can fail
#                     either one while the other stays healthy.
#   selector_drift    live workload selector differs from the target manifest
#   terminal          stamp ProgressDeadlineExceeded so an observer stops at
#                     once instead of retrying to its 300s deadline. This is a
#                     Rollout-facet issue: it must not be the reason any named
#                     convergence sub-flag goes false.
#   apply_fails       False | "always" | "after-converge"  -- when `kubectl
#                     apply` (the checkpoint write) exits non-zero
#   alembic_current   stdout of `kubectl exec ... alembic current`
#   alembic_fail      that exec exits 1 (unreadable live revision)
#   compat_metadata   object served as ConfigMap data.compatibility.json from
#                     `helm template --show-only templates/schema-compat.yaml`
DEFAULT_COMPAT_METADATA = {
    "schema_min": "0043",
    "schema_head": "0043",
    "revisions": [
        {
            "revision": "0043",
            "parents": ["0042"],
            "kind": "expand",
            "sha256": "ab",
        }
    ],
}

# Live 0039, head 0043, pending 0041 is contract. Used by schema-contract.
CONTRACT_COMPAT_METADATA = {
    "schema_min": "0043",
    "schema_head": "0043",
    "revisions": [
        {"revision": "0039", "parents": ["0038"], "kind": "expand", "sha256": "ab"},
        {"revision": "0040", "parents": ["0039"], "kind": "expand", "sha256": "ab"},
        {"revision": "0041", "parents": ["0040"], "kind": "contract", "sha256": "ab"},
        {"revision": "0042", "parents": ["0041"], "kind": "expand", "sha256": "ab"},
        {"revision": "0043", "parents": ["0042"], "kind": "expand", "sha256": "ab"},
    ],
}

BASE = {
    "before": "0.8.6",
    "after": "0.9.0",
    "after_converge": None,
    "served_before": "0.8.6",
    "served_after": "0.9.0",
    "target": "0.9.0",
    "show_chart": "0.9.0",
    "ready": 1,
    "updated": 1,
    "unavailable": 0,
    "failed_hook": "",
    "status_missing_before": False,
    "generation_drift": False,
    "workloads_fail": False,
    "values_fail": False,
    "values_drift": False,
    "selector_drift": False,
    "terminal": False,
    "apply_fails": False,
    "alembic_current": "0043 (head)",
    "alembic_fail": False,
    "compat_metadata": None,
}

SCENARIOS = {
    "healthy": {},
    # Helm exits 0 and the workloads converge, but the release never leaves the
    # old chart version. `helm show chart` still reports 0.9.0, so Validate
    # passes and Apply is genuinely reached; the post-condition read is the
    # only thing that can catch this.
    "stale-version": {"after": "0.8.6"},
    # Apply's post-condition read sees 0.9.0 and convergence is exact, then the
    # release slips back to 0.8.6 before the canary. Isolates the canary's own
    # version read from Apply's.
    "canary-version-drift": {"after_converge": "0.8.6"},
    # A resume whose Apply already happened: release and workloads are on 0.9.0.
    "resumed-applied": {"before": "0.9.0", "served_before": "0.9.0"},
    # The release reports the new revision and `helm get manifest` renders the
    # 0.9.0 images, but the live pods still run 0.8.6 at full ready counts.
    # Every other facet is healthy, so `images` is the only sub-flag that may
    # go false.
    "stale-images": {"served_after": "0.8.6", "terminal": True},
    # A non-drain pre-upgrade hook fails: hooks_healthy only.
    "failed-hook": {"failed_hook": "schema-migrate"},
    # The #2010 drain gate Job fails: queues_drained only. This is the live
    # observation `live_drain`'s `Ok(true)` stub never had -- the gate is a Helm
    # pre-upgrade hook that fires during Apply, so Converge is the only phase
    # that can see its verdict.
    "failed-drain-hook": {"failed_hook": "upgrade-drain"},
    "selector-drift": {"selector_drift": True, "terminal": True},
    # `observedGeneration` lags `generation`: the controller has not yet acted
    # on the target spec. Images, replicas, hooks and selectors all agree, so
    # `generations` is the only sub-flag that may go false.
    "stale-generation": {"generation_drift": True, "terminal": True},
    # `updatedReplicas` lags `desired` while `unavailableReplicas` is 0: the
    # replica facet is false and the unavailable facet is NOT, which is only
    # expressible because the two are observed separately.
    "stale-replicas": {"updated": 0, "terminal": True},
    # The mirror image: updated == ready == total == desired, but a replica is
    # unavailable. `replicas` stays true and `unavailable_zero` alone goes
    # false, so neither flag can be an alias of the other.
    "unavailable-replicas": {"unavailable": 1, "terminal": True},
    # The workloads read itself fails. No observation can be MADE, so no named
    # facet was determined -- every one must read false, and the read's own
    # error text must reach the operator.
    "workloads-unreadable": {"workloads_fail": True},
    # `helm get values` fails with stderr that merely contains "not found".
    # The retained overlay is UNKNOWN, not empty.
    "values-read-fails": {"values_fail": True},
    # A second `helm get values` would return a different document.
    "values-drift": {"values_drift": True},
    # Every checkpoint write fails, starting with the first one before any
    # mutation.
    "persist-fails": {"apply_fails": "always"},
    # Only the checkpoint write that FOLLOWS a failed Converge fails. The
    # convergence failure is the real verdict; the persist error must travel
    # beside it rather than replace it.
    "converge-then-persist-fails": {
        "served_after": "0.8.6",
        "terminal": True,
        "apply_fails": "after-converge",
    },
    # A local chart directory whose own metadata is not the requested --to.
    # No release yet: `helm status` fails until something is installed, so the
    # Drain phase is skipped for having nothing in flight.
    "fresh-install": {"status_missing_before": True},
    "local-chart-mismatch": {"show_chart": "0.8.7"},
    # Live revision 0099 is not in the target graph: Validate must refuse
    # before `helm upgrade`.
    "schema-incompatible": {"alembic_current": "0099"},
    # Live 0039, target head 0043, pending 0041 is contract.
    "schema-contract": {
        "alembic_current": "0039 (head)",
        "compat_metadata": CONTRACT_COMPAT_METADATA,
    },
    # Live already at target head. Chart upgrade may still proceed.
    "schema-compatible": {"alembic_current": "0043 (head)"},
    # Existing release, but the alembic probe fails. Fail closed; this is
    # not an empty-DB shortcut.
    "schema-probe-fails": {"alembic_fail": True},
    # No Helm release and the API probe also fails. Retained for the
    # empty-DB vs missing-API distinction; live tests do not drive it yet.
    "schema-fresh-empty": {"status_missing_before": True, "alembic_fail": True},
}

root = Path(os.environ["UPGRADE_DRIVER_ROOT"])
scenario = dict(BASE)
scenario.update(SCENARIOS[os.environ["UPGRADE_DRIVER_SCENARIO"]])
if scenario["after_converge"] is None:
    scenario["after_converge"] = scenario["after"]
program = Path(sys.argv[0]).name
args = sys.argv[1:]

log = root / "argv.log"
previous = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
with log.open("a") as handle:
    handle.write(json.dumps([program, *args]) + "\n")

upgraded = any(call[:2] == ["helm", "upgrade"] for call in previous)
converged = any(call[:1] == ["kubectl"] and WORKLOADS in call for call in previous)

if upgraded and converged:
    version = scenario["after_converge"]
elif upgraded:
    version = scenario["after"]
else:
    version = scenario["before"]
served = scenario["served_after"] if upgraded else scenario["served_before"]
image = f"{REPO}:{served}"
target_image = f"{REPO}:{scenario['target']}"


def emit(value):
    print(json.dumps(value))
    sys.exit(0)


def captured(prefix, suffix):
    """Captured payloads in capture order (index is 1-based and monotonic)."""
    return sorted(root.glob(f"{prefix}-*{suffix}"), key=lambda p: int(p.stem.split("-")[-1]))


def capture(prefix, suffix, source):
    index = len(captured(prefix, suffix)) + 1
    shutil.copyfile(source, root / f"{prefix}-{index}{suffix}")


def flag_value(name):
    return args[args.index(name) + 1] if name in args else None


expected = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {"name": f"{RELEASE}-api", "namespace": NAMESPACE},
    "spec": {
        "replicas": 1,
        "selector": {"matchLabels": {"app": f"{RELEASE}-api"}},
        "template": {
            "metadata": {"labels": {"app": f"{RELEASE}-api"}},
            "spec": {"containers": [{"name": "api", "image": target_image}]},
        },
    },
}

live = copy.deepcopy(expected)
live["metadata"]["generation"] = 3
live["spec"]["template"]["spec"]["containers"][0]["image"] = image
live["status"] = {
    "observedGeneration": 2 if scenario["generation_drift"] else 3,
    "replicas": 1,
    "readyReplicas": scenario["ready"],
    "updatedReplicas": scenario["updated"],
    "unavailableReplicas": scenario["unavailable"],
    "conditions": [{"type": "Available", "status": "True"}],
}
if scenario["selector_drift"]:
    live["spec"]["selector"]["matchLabels"] = {"app": f"{RELEASE}-api", "tier": "drifted"}
if scenario["terminal"]:
    live["status"]["conditions"].append(
        {"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}
    )

pod = {
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {
        "name": f"{RELEASE}-api-0",
        "namespace": NAMESPACE,
        "labels": live["spec"]["selector"]["matchLabels"],
    },
    "spec": {"containers": [{"name": "api", "image": image}]},
    "status": {
        "phase": "Running",
        "containerStatuses": [
            {
                "name": "api",
                "image": image,
                "imageID": "containerd://sha256:" + "a" * 64,
                "ready": True,
                "state": {"running": {}},
            }
        ],
    },
}

# Both pre-upgrade hooks the chart installs, by their real rendered names
# (`charts/curie/templates/worker-upgrade-drain.yaml` and `schema-migrate.yaml`).
# Both are always present; only `failed_hook` decides which one refused. A
# fixture that published just one hook could not tell the drain facet apart
# from the general hook facet.
# The two documents the `values-drift` scenario serves, distinguished by an
# extraEnv entry the migration carries through untouched. Only the FIRST is a
# legitimate input: it is what Validate read and migrated.
FIRST_VALUES = {
    "config": {"schemaVersion": "0.8.4"},
    "api": {"extraEnv": [{"name": "FIRST_READ_ONLY", "value": "1"}]},
}
DRIFTED_VALUES = {
    "config": {"schemaVersion": "0.8.4"},
    "api": {"extraEnv": [{"name": "SECOND_READ_MUST_NOT_WIN", "value": "1"}]},
}

HOOK_NAMES = {
    "upgrade-drain": f"{RELEASE}-upgrade-drain",
    "schema-migrate": f"{RELEASE}-schema-migrate",
}

hooks = []
jobs = []
for key, name in HOOK_NAMES.items():
    manifest = {"kind": "Job", "metadata": {"name": name, "namespace": NAMESPACE}}
    failed = scenario["failed_hook"] == key
    hooks.append(
        {
            "name": name,
            "kind": "Job",
            "events": ["pre-upgrade"],
            "last_run": {"phase": "Failed" if failed else "Succeeded"},
            "manifest": json.dumps(manifest),
        }
    )
    jobs.append(
        {
            "kind": "Job",
            "metadata": manifest["metadata"],
            "status": {"failed": 1, "conditions": [
                {"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}
            ]}
            if failed
            else {"succeeded": 1, "conditions": [{"type": "Complete", "status": "True"}]},
        }
    )

if program == "helm":
    if args[0] == "status":
        if scenario["status_missing_before"] and not upgraded:
            print('Error: release: not found', file=sys.stderr)
            sys.exit(1)
        if "json" in args:
            emit(
                {
                    "version": 2,
                    "info": {"status": "deployed"},
                    "chart": {"metadata": {"name": "curie", "version": version}},
                    "hooks": hooks,
                }
            )
        print("STATUS: deployed\nREVISION: 2")
        sys.exit(0)
    if args[:2] == ["get", "values"]:
        if scenario["values_fail"]:
            print('Error from server (NotFound): namespaces "ns" not found', file=sys.stderr)
            sys.exit(1)
        if scenario["values_drift"]:
            reads = sum(1 for call in previous if call[:3] == ["helm", "get", "values"])
            print(json.dumps(DRIFTED_VALUES if reads else FIRST_VALUES))
            sys.exit(0)
        applied_values = captured("values", ".yaml")
        if applied_values:
            # The release retains the overlay it was last handed. A second run
            # therefore migrates the FIRST run's own output, which is the only
            # shape that can prove idempotence rather than repeat one input.
            print(applied_values[-1].read_text())
            sys.exit(0)
        retained = root / "retained.json"
        print(retained.read_text() if retained.exists() else "{}")
        sys.exit(0)
    if args[:2] == ["get", "manifest"]:
        # JSON documents are YAML documents; no optional YAML dependency here.
        print(json.dumps(expected))
        sys.exit(0)
    if args[:2] == ["show", "chart"]:
        print(
            "name: curie\nversion: {v}\nappVersion: {v}".format(v=scenario["show_chart"])
        )
        sys.exit(0)
    if args[0] == "template":
        show_only = flag_value("--show-only")
        if show_only is None:
            for arg in args:
                if arg.startswith("--show-only="):
                    show_only = arg.split("=", 1)[1]
                    break
        if show_only == "templates/schema-compat.yaml":
            metadata = scenario["compat_metadata"] or DEFAULT_COMPAT_METADATA
            payload = (
                metadata
                if isinstance(metadata, str)
                else json.dumps(metadata, sort_keys=True)
            )
            print(
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": f"{RELEASE}-curie-schema-compat",
                            "labels": {
                                "app.kubernetes.io/component": "schema-compat"
                            },
                        },
                        "data": {"compatibility.json": payload},
                    }
                )
            )
            sys.exit(0)
        print(
            f"Error: could not find template {show_only or '<template>'} in chart",
            file=sys.stderr,
        )
        sys.exit(1)
    if args[0] == "upgrade":
        values = flag_value("-f")
        if values:
            capture("values", ".yaml", values)
        print("Release accepted")
        sys.exit(0)

if program == "kubectl":
    if scenario["workloads_fail"] and args[:3] == ["get", WORKLOADS, "-n"]:
        print("Error from server (Forbidden): workloads is forbidden", file=sys.stderr)
        sys.exit(1)
    if args[0] == "apply":
        manifest = flag_value("-f")
        if manifest:
            capture("applied", ".json", manifest)
        fails = scenario["apply_fails"]
        if fails == "always" or (fails == "after-converge" and converged):
            print("error: could not apply the checkpoint ConfigMap", file=sys.stderr)
            sys.exit(1)
        print("configmap/checkpoint configured")
        sys.exit(0)
    if args[:2] == ["get", "configmap"]:
        if args[2].endswith("-upgrade-checkpoint"):
            checkpoint = root / "checkpoint.json"
            if checkpoint.exists():
                sys.stdout.write(checkpoint.read_text().strip())
            sys.exit(0)
        print(f'Error from server (NotFound): configmaps "{args[2]}" not found', file=sys.stderr)
        sys.exit(1)
    if args[:2] == ["get", "node"]:
        emit({"kind": "Node", "metadata": {"name": args[2]}, "status": {"images": []}})
    if args[:3] == ["get", WORKLOADS, "-n"]:
        emit({"items": [live, pod, *jobs]})
    if args[:2] == ["get", "deploy"] and len(args) > 2 and not args[2].startswith("-"):
        # The drain probe reads one named Deployment; its output is not parsed.
        print(f"{args[2]}   1/1")
        sys.exit(0)
    if args[0] == "exec" and "alembic" in args and "current" in args:
        if scenario["alembic_fail"]:
            print(
                "Error from server (NotFound): deployments.apps "
                f'"{RELEASE}-curie-api" not found',
                file=sys.stderr,
            )
            sys.exit(1)
        print(scenario["alembic_current"])
        sys.exit(0)

print("unhandled recording command: " + repr([program, *args]), file=sys.stderr)
sys.exit(64)

#!/usr/bin/python3
"""Recording helm/kubectl boundary for `curie cluster upgrade`.

Installed under both names; dispatch is on ``sys.argv[0]``. Every invocation is
appended to ``$UPGRADE_DRIVER_ROOT/argv.log`` as one JSON array per line, so a
test can assert on the commands that were actually issued (and, for a refusal,
on the commands that were *not*). Never claims a real Kubernetes run.

Payload capture:
  * every ``-f <file>`` helm receives is copied to ``values-<n>.yaml``
  * every ``kubectl create -f <file>`` object is copied to ``created-<n>.json``
  * every ``kubectl patch --patch-file <file>`` document is copied to
    ``patch-<n>.json``

``helm get values`` returns the most recently APPLIED overlay when one exists,
falling back to ``retained.json``. A real release retains what was last handed
to it, and that is what makes a second run a resume of the first rather than a
replay of the same input.

Behaviour is selected by ``$UPGRADE_DRIVER_SCENARIO`` from SCENARIOS below.
Optional per-test inputs read from the root directory:
  * ``retained.json`` -- the document ``helm get values`` returns (JSON is YAML)
  * ``checkpoint.json`` -- the complete upgrade checkpoint ConfigMap

Three observable moments drive the fixtures, derived from the argv log rather
than from a call ordinal (the number of `helm get metadata` reads is an
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
#   before/after      chart version `helm get metadata` reports before and after
#                     `helm upgrade`
#   after_converge    chart version `helm get metadata` reports once
#                     convergence has observed the live workloads; defaults to
#                     `after`. Only a
#                     version that moves BETWEEN Apply and Canary can prove the
#                     canary re-reads it instead of trusting its own bookkeeping.
#   metadata_numeric_version  return the Helm revision number in metadata's
#                             `version` field instead of a chart version string
#   metadata_missing_before  metadata reports no release before Apply
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
#   checkpoint_patch_fails  False | "always" | "after-converge". A record
#                           patch exits nonzero at the selected moment.
#   acquire_conflict  None | "create" | "patch". The selected ownership write
#                     loses to a deterministic external holder.
#   stale_record_cas  an external writer advances resourceVersion and installs
#                     a sentinel immediately before the first record patch.
#   release_conflict  an external writer replaces the holder immediately before
#                     the normal release patch.
#   namespace_absent  checkpoint lookup reports the namespace absent.
#   namespace_disappears  checkpoint lookup reports the ConfigMap absent, then
#                         create reports the namespace absent.
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
    "metadata_numeric_version": False,
    "served_before": "0.8.6",
    "served_after": "0.9.0",
    "target": "0.9.0",
    "show_chart": "0.9.0",
    "ready": 1,
    "updated": 1,
    "unavailable": 0,
    "failed_hook": "",
    "metadata_missing_before": False,
    "metadata_after_shape": "valid",
    "generation_drift": False,
    "workloads_fail": False,
    "values_fail": False,
    "values_drift": False,
    "selector_drift": False,
    "terminal": False,
    "checkpoint_patch_fails": False,
    "acquire_conflict": None,
    "stale_record_cas": False,
    "release_conflict": False,
    "namespace_absent": False,
    "namespace_disappears": False,
    "alembic_current": "0043 (head)",
    "alembic_fail": False,
    "compat_metadata": None,
}

SCENARIOS = {
    "healthy": {},
    # Keep the release cache target distinct from the running CLI version so
    # the resolver test can prove that --to owns the cache key.
    "release-cache-prior": {
        "after": "0.8.9",
        "served_after": "0.8.9",
        "target": "0.8.9",
        "show_chart": "0.8.9",
    },
    # Helm exits 0 and the workloads converge, but the release never leaves the
    # old chart version. `helm show chart` still reports 0.9.0, so Validate
    # passes and Apply is genuinely reached; the post-condition read is the
    # only thing that can catch this.
    "stale-version": {"after": "0.8.6"},
    # Apply's post-condition read sees 0.9.0 and convergence is exact, then the
    # release slips back to 0.8.6 before the canary. Isolates the canary's own
    # version read from Apply's.
    "canary-version-drift": {"after_converge": "0.8.6"},
    "metadata-malformed-after": {"metadata_after_shape": "malformed"},
    "metadata-missing-after": {"metadata_after_shape": "missing"},
    "metadata-numeric-after": {"metadata_after_shape": "numeric"},
    # A resume whose Apply already happened: release and workloads are on 0.9.0.
    "resumed-applied": {"before": "0.9.0", "served_before": "0.9.0"},
    # The release metadata reports the target chart version and `helm get
    # manifest` renders the 0.9.0 images, but the live pods still run 0.8.6 at
    # full ready counts.
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
    "persist-fails": {"checkpoint_patch_fails": "always"},
    # Only the checkpoint write that FOLLOWS a failed Converge fails. The
    # convergence failure is the real verdict; the persist error must travel
    # beside it rather than replace it.
    "converge-then-persist-fails": {
        "served_after": "0.8.6",
        "terminal": True,
        "checkpoint_patch_fails": "after-converge",
    },
    "acquire-create-conflict": {"acquire_conflict": "create"},
    "acquire-patch-conflict": {"acquire_conflict": "patch"},
    "stale-record-cas": {"stale_record_cas": True},
    "release-conflict": {"release_conflict": True},
    "converge-then-release-conflict": {
        "served_after": "0.8.6",
        "terminal": True,
        "release_conflict": True,
    },
    "namespace-absent": {"namespace_absent": True},
    "namespace-disappears": {"namespace_disappears": True},
    # A local chart directory whose own metadata is not the requested --to.
    # No release yet: `helm get metadata` fails until something is installed,
    # so the Drain phase is skipped for having nothing in flight.
    "fresh-install": {"metadata_missing_before": True},
    "local-chart-mismatch": {"show_chart": "0.8.7"},
    # Helm metadata's top-level `version` is the numeric revision rather than
    # the chart version string. The CLI must treat the chart version as absent.
    "numeric-metadata-version": {"metadata_numeric_version": True},
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
    "schema-fresh-empty": {
        "metadata_missing_before": True,
        "alembic_fail": True,
    },
    # Drain's worker Deployment probe fails with a non-NotFound error, so
    # live_drain retries until the phase budget expires and returns false.
    "undrained-deploy": {},
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
    chart_version = scenario["after_converge"]
elif upgraded:
    chart_version = scenario["after"]
else:
    chart_version = scenario["before"]
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


CHECKPOINT = f"{RELEASE}-upgrade-checkpoint"
HOLDER_ANNOTATION = "curietech.ai/upgrade-holder"
ACTION_ANNOTATION = "curietech.ai/upgrade-action"
CREATE_WINNER = "00000000-0000-4000-8000-000000000701"
PATCH_WINNER = "00000000-0000-4000-8000-000000000702"
RELEASE_WINNER = "00000000-0000-4000-8000-000000000703"
WINNING_ACTION = "upgrade to 0.9.0"
SENTINEL_RECORD = json.dumps(
    {
        "target_version": "9.9.9",
        "from_version": "9.9.8",
        "known_good_version": "9.9.8",
        "completed": ["plan"],
        "status": "external_sentinel",
        "plan": ["external writer sentinel"],
        "drain_completed": False,
        "convergence": None,
        "canary": None,
        "fail_forward": None,
        "resumed": False,
    },
    separators=(",", ":"),
)


def checkpoint_path():
    return root / "checkpoint.json"


def read_checkpoint():
    path = checkpoint_path()
    return json.loads(path.read_text()) if path.exists() else None


def normalize_config_map(config_map):
    metadata = config_map.setdefault("metadata", {})
    if metadata.get("annotations") == {}:
        metadata.pop("annotations")
    if config_map.get("data") == {}:
        config_map.pop("data")
    return config_map


def save_checkpoint(config_map):
    normalized = normalize_config_map(copy.deepcopy(config_map))
    checkpoint_path().write_text(json.dumps(normalized, separators=(",", ":")))
    return normalized


def next_resource_version(config_map):
    current = config_map.get("metadata", {}).get("resourceVersion", "0")
    try:
        return str(int(current) + 1)
    except (TypeError, ValueError):
        return f"{current}.next"


def winning_checkpoint(holder, resource_version, record=SENTINEL_RECORD):
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": CHECKPOINT,
            "namespace": NAMESPACE,
            "resourceVersion": resource_version,
            "labels": {
                "app.kubernetes.io/managed-by": "curie",
                "curietech.ai/upgrade": "checkpoint",
            },
            "annotations": {
                HOLDER_ANNOTATION: holder,
                ACTION_ANNOTATION: WINNING_ACTION,
            },
        },
        "data": {"record": record},
    }


def decode_pointer(path):
    if not isinstance(path, str) or not path.startswith("/"):
        raise ValueError(f"invalid JSON Pointer {path!r}")
    return [part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")]


def pointer_parent(document, path):
    parts = decode_pointer(path)
    if not parts:
        raise ValueError("the document root is not a mutable checkpoint field")
    current = document
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current:
            raise KeyError(path)
        current = current[part]
    if not isinstance(current, dict):
        raise KeyError(path)
    return current, parts[-1]


def pointer_value(document, path):
    current = document
    for part in decode_pointer(path):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(path)
        current = current[part]
    return current


def apply_patch(document, operations):
    changed = copy.deepcopy(document)
    for operation in operations:
        op = operation.get("op")
        path = operation.get("path")
        if op == "test":
            if pointer_value(changed, path) != operation.get("value"):
                raise ValueError(f"test failed at {path}")
            continue
        parent, key = pointer_parent(changed, path)
        if op == "add":
            parent[key] = copy.deepcopy(operation.get("value"))
        elif op == "replace":
            if key not in parent:
                raise KeyError(path)
            parent[key] = copy.deepcopy(operation.get("value"))
        elif op == "remove":
            if key not in parent:
                raise KeyError(path)
            del parent[key]
        else:
            raise ValueError(f"unsupported JSON Patch operation {op!r}")
    return changed


def patch_adds_holder(operation):
    if operation.get("op") != "add":
        return False
    if operation.get("path") == "/metadata/annotations/curietech.ai~1upgrade-holder":
        return True
    return operation.get("path") == "/metadata/annotations" and isinstance(
        operation.get("value"), dict
    ) and HOLDER_ANNOTATION in operation["value"]


def patch_kind(operations):
    paths = {operation.get("path") for operation in operations}
    if any(
        operation.get("op") == "remove"
        and operation.get("path") == "/metadata/annotations/curietech.ai~1upgrade-holder"
        for operation in operations
    ):
        return "release"
    if "/data" in paths or "/data/record" in paths:
        return "record"
    if any(patch_adds_holder(operation) for operation in operations):
        return "acquire"
    return "unknown"


def conflict(message):
    print(
        f"Error from server (Conflict): Operation cannot be fulfilled on "
        f"configmaps \"{CHECKPOINT}\": {message}",
        file=sys.stderr,
    )
    sys.exit(1)


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
    # Helm v3.20 removes rel.Chart before serializing status and keeps the
    # numeric release revision at version:
    # https://github.com/helm/helm/blob/v3.20.0/cmd/helm/status.go
    if args[0] == "history":
        print("Error: release: not found", file=sys.stderr)
        sys.exit(1)
    if args[0] == "status":
        if "json" in args:
            emit(
                {
                    "config": {},
                    "hooks": hooks,
                    "info": {"status": "deployed"},
                    "manifest": json.dumps(expected),
                    "name": RELEASE,
                    "namespace": NAMESPACE,
                    "version": 2,
                }
            )
        print("STATUS: deployed\nREVISION: 2")
        sys.exit(0)
    if args[:2] == ["get", "metadata"]:
        # Helm v3.20 maps this string from rel.Chart.Metadata.Version:
        # https://github.com/helm/helm/blob/v3.20.0/pkg/action/get_metadata.go
        if scenario["metadata_missing_before"] and not upgraded:
            print('Error: release: not found', file=sys.stderr)
            sys.exit(1)
        shape = scenario["metadata_after_shape"] if upgraded else "valid"
        if shape == "malformed":
            print("{")
            sys.exit(0)
        if scenario["metadata_numeric_version"]:
            shape = "numeric"
        metadata = {
            "name": RELEASE,
            "chart": "curie",
            "version": chart_version,
            "appVersion": chart_version,
            "namespace": NAMESPACE,
            "revision": 2,
            "status": "deployed",
            "deployedAt": "2026-09-12T00:00:00Z",
        }
        if shape == "missing":
            del metadata["version"]
        elif shape == "numeric":
            metadata["version"] = 2
        emit(metadata)
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
    if args[0] == "create":
        manifest = flag_value("-f")
        if manifest:
            capture("created", ".json", manifest)
        if scenario["namespace_absent"] or scenario["namespace_disappears"]:
            print(
                f'Error from server (NotFound): error when creating "{manifest}": '
                f'namespaces "{NAMESPACE}" not found',
                file=sys.stderr,
            )
            sys.exit(1)
        if scenario["acquire_conflict"] == "create":
            save_checkpoint(winning_checkpoint(CREATE_WINNER, "701"))
            conflict("the object has been modified")
        if read_checkpoint() is not None:
            print(
                f'Error from server (AlreadyExists): configmaps "{CHECKPOINT}" already exists',
                file=sys.stderr,
            )
            sys.exit(1)
        if manifest is None:
            print("error: create requires -f", file=sys.stderr)
            sys.exit(64)
        created = json.loads(Path(manifest).read_text())
        metadata = created.setdefault("metadata", {})
        metadata.setdefault("name", CHECKPOINT)
        metadata.setdefault("namespace", NAMESPACE)
        metadata["resourceVersion"] = "100"
        emit(save_checkpoint(created))
    if args[0] == "patch" and args[1:3] == ["configmap", CHECKPOINT]:
        patch_file = flag_value("--patch-file")
        if patch_file is None:
            print("error: patch requires --patch-file", file=sys.stderr)
            sys.exit(64)
        capture("patch", ".json", patch_file)
        operations = json.loads(Path(patch_file).read_text())
        kind = patch_kind(operations)
        current = read_checkpoint()
        if current is None:
            print(
                f'Error from server (NotFound): configmaps "{CHECKPOINT}" not found',
                file=sys.stderr,
            )
            sys.exit(1)
        if kind == "acquire" and scenario["acquire_conflict"] == "patch":
            save_checkpoint(winning_checkpoint(PATCH_WINNER, next_resource_version(current)))
            conflict("the object has been modified")
        if kind == "record" and scenario["stale_record_cas"]:
            external = copy.deepcopy(current)
            external.setdefault("data", {})["record"] = SENTINEL_RECORD
            external["metadata"]["resourceVersion"] = next_resource_version(current)
            save_checkpoint(external)
            (root / "sentinel-record.txt").write_text(SENTINEL_RECORD)
            conflict("the object has been modified")
        fails = scenario["checkpoint_patch_fails"]
        if kind == "record" and (
            fails == "always" or (fails == "after-converge" and converged)
        ):
            print("error: could not patch the checkpoint ConfigMap", file=sys.stderr)
            sys.exit(1)
        if kind == "release" and scenario["release_conflict"]:
            external = copy.deepcopy(current)
            external["metadata"]["resourceVersion"] = next_resource_version(current)
            external["metadata"]["annotations"] = {
                HOLDER_ANNOTATION: RELEASE_WINNER,
                ACTION_ANNOTATION: WINNING_ACTION,
            }
            save_checkpoint(external)
            conflict("the object has been modified")
        try:
            patched = apply_patch(current, operations)
        except (KeyError, TypeError, ValueError) as error:
            conflict(str(error))
        patched["metadata"]["resourceVersion"] = next_resource_version(current)
        emit(save_checkpoint(patched))
    if args[:2] == ["get", "configmap"]:
        if args[2] == CHECKPOINT:
            if scenario["namespace_absent"]:
                print(
                    f'Error from server (NotFound): namespaces "{NAMESPACE}" not found',
                    file=sys.stderr,
                )
                sys.exit(1)
            checkpoint = read_checkpoint()
            if checkpoint is not None:
                emit(checkpoint)
            print(
                f'Error from server (NotFound): configmaps "{CHECKPOINT}" not found',
                file=sys.stderr,
            )
            sys.exit(1)
        print(f'Error from server (NotFound): configmaps "{args[2]}" not found', file=sys.stderr)
        sys.exit(1)
    if args[:2] == ["get", "node"]:
        emit({"kind": "Node", "metadata": {"name": args[2]}, "status": {"images": []}})
    if args[:3] == ["get", WORKLOADS, "-n"]:
        emit({"items": [live, pod, *jobs]})
    if args[:2] == ["get", "deploy"] and len(args) > 2 and not args[2].startswith("-"):
        # The drain probe reads one named Deployment; replica output is not parsed.
        name = args[2]
        if os.environ["UPGRADE_DRIVER_SCENARIO"] == "undrained-deploy":
            print(f'deployment "{name}" has not drained', file=sys.stderr)
            sys.exit(1)
        print(f"{name}   1/1")
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

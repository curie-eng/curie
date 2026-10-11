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

``helm get values`` selects the requested revision, defaulting to the newest.
Scenarios can give each revision distinct values; otherwise it returns the
most recently APPLIED overlay when one exists, falling back to ``retained.json``.
A real release retains what was last handed to it, and that is what makes a
second run a resume of the first rather than a replay of the same input.

Behaviour is selected by ``$UPGRADE_DRIVER_SCENARIO`` from SCENARIOS below.
Optional per-test inputs read from the root directory:
  * ``retained.json`` -- the document ``helm get values`` returns (JSON is YAML)
  * ``checkpoint.json`` -- the complete upgrade checkpoint ConfigMap
  * ``chart-values.json`` -- the target chart's own values, which
    ``helm show values`` prints. Absent, that read exits 64 as it always has,
    so no existing test starts resolving a runner tag over the network.
  * ``retirement.json`` -- per-agent claim lists under ``agents``, one
    namespace-wide ``pods`` list (platform-pool pods carry no agent label, so
    the CLI must read pods unlabelled and match them by name), plus a
    selected retirement read failure, malformed response, or ``fail_delete``.

``kubectl get sandboxtemplates...`` renders the SandboxTemplates the chart
would produce from the values Helm was last handed (the most recent
``values-<n>.yaml``, else ``retained.json``) over ``chart-values.json``, the way
``curie.sandboxTemplate`` and ``curie.agentSandboxPoolAgents`` in
``charts/curie/templates/agent-sandbox.yaml`` do. It never reads the CLI's own
plan back, so a canary that compares against it compares against the chart.

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
#   before/after      deployed chart version in history and revision-pinned
#                     metadata before and after `helm upgrade`
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
#                     "upgrade-drain-attest" the attest Job fails
#                     "schema-migrate"      a NON-drain pre-upgrade hook fails
#                     Ruling 13: `queues_drained` binds to the upgrade-drain
#                     hook and the upgrade-drain-attest hook. `hooks_healthy`
#                     binds to every other hook, so the facets are only
#                     provably distinct if the fixture can fail one of those
#                     drain hooks while the other hooks stay healthy.
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
#   template_ignores_runner_images  the rendered per-agent SandboxTemplates run
#                     the platform runner even where `runnerImages` binds a
#                     layer, so a canary that checks the plan against the
#                     rendered templates has something to catch (#4321)
#   stale_agent_templates  {agent: image}. Each named agent's per-agent
#                     SandboxTemplate exists and renders that image whatever the
#                     values say, the way a template a failed or partial render
#                     left behind survives (#4321)
#   pending_status    orphaned newest revision status above serving revision 4
#   no_serving_revision  history contains only the pending revision
#   hook_jobs_shape   "valid" | "failed" | "malformed" | "not-list"
#   active_hook       a selected hook Job still has active pods
#   active_plain_job  a selected non-hook Job has active pods
#   takeover_conflict  a writer replaces the holder before the takeover patch
#   rollback_fails    Helm rollback's wait fails after creating a failed revision
#   revision_values   retained values per revision, independent of chart version
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
    "drain_annotation": "auto",
    "drain_render_fails": False,
    "upgrade_fails": False,
    "history_after_upgrade": None,
    "revision_versions": None,
    "revision_values": None,
    "pending_status": None,
    "no_serving_revision": False,
    "hook_jobs_shape": "valid",
    "active_hook": False,
    "active_plain_job": False,
    "takeover_conflict": False,
    "rollback_fails": False,
    "template_ignores_runner_images": False,
    "stale_agent_templates": {},
}

SCENARIOS = {
    "healthy": {},
    "pending-revision": {"pending_status": "pending-upgrade"},
    "pending-divergent-values": {
        "pending_status": "pending-upgrade",
        "revision_values": {
            "4": {
                "config": {"schemaVersion": "0.8.6"},
                "worker": {
                    "extraEnv": [
                        {"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": "120"},
                        {"name": "SERVING_REVISION_ONLY", "value": "keep"},
                    ],
                },
                "connectorCaller": {"existingSecret": "acme-caller-pair"},
            },
            "5": {
                "config": {"schemaVersion": "0.9.0"},
                "worker": {
                    "runnerTotalTimeoutSeconds": 999,
                    "extraEnv": [{"name": "PENDING_REVISION_ONLY", "value": "must-not-win"}],
                },
                "connectorCaller": {"existingSecret": "acme-caller-pair"},
            },
        },
    },
    "take-over-running-hook": {
        "pending_status": "pending-upgrade",
        "active_hook": True,
    },
    "take-over-active-plain-job": {
        "pending_status": "pending-upgrade",
        "active_plain_job": True,
    },
    "take-over-jobs-failed": {"hook_jobs_shape": "failed"},
    "take-over-jobs-malformed": {"hook_jobs_shape": "malformed"},
    "take-over-jobs-not-list": {"hook_jobs_shape": "not-list"},
    "take-over-cas-conflict": {
        "pending_status": "pending-upgrade",
        "takeover_conflict": True,
    },
    "take-over-rollback-failed": {
        "pending_status": "pending-upgrade",
        "rollback_fails": True,
    },
    "pending-no-serving": {
        "pending_status": "pending-upgrade",
        "no_serving_revision": True,
    },
    "failed-no-serving": {
        "pending_status": "failed",
        "no_serving_revision": True,
    },
    "pending-install": {"pending_status": "pending-install"},
    "pending-rollback": {"pending_status": "pending-rollback"},
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
    "failed-attest-hook": {"failed_hook": "upgrade-drain-attest"},
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
    "drain-annotation-missing": {"drain_annotation": None},
    "drain-annotation-invalid": {"drain_annotation": "many"},
    "drain-render-fails": {"drain_render_fails": True},
    # Helm exits 1 with the chart's nil digest refusal and creates no revision.
    "helm-upgrade-fails": {"upgrade_fails": True},
    # Helm exits 1 after recording a failed revision. The previous revision
    # stays deployed, which is what the worker still runs.
    "upgrade-fails-previous-deployed": {
        "upgrade_fails": True,
        "history_after_upgrade": [
            {"revision": 1, "status": "deployed", "chart": "curie-0.8.6", "app_version": "0.8.6"},
            {"revision": 2, "status": "failed", "chart": "curie-0.9.0", "app_version": "0.9.0"},
        ],
        "revision_versions": {"1": "0.8.6", "2": "0.9.0"},
    },
    # Helm exits 1 but the deployed revision is already the target.
    "upgrade-fails-target-deployed": {
        "upgrade_fails": True,
        "history_after_upgrade": [
            {"revision": 1, "status": "superseded", "chart": "curie-0.8.6", "app_version": "0.8.6"},
            {"revision": 2, "status": "deployed", "chart": "curie-0.9.0", "app_version": "0.9.0"},
        ],
        "revision_versions": {"1": "0.8.6", "2": "0.9.0"},
    },
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
    # No release yet: `helm history` fails until something is installed,
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
    # Helm accepts the rebound layer, but the per-agent SandboxTemplate still
    # renders the platform runner. Only the canary's template read sees it.
    "stock-layer-dropped": {"template_ignores_runner_images": True},
    # Run as the RESUME of a `healthy` run interrupted after Apply cleared
    # acme-bot's owner-built layer (the argv log already holds that run's
    # `helm upgrade`), while acme-bot's per-agent SandboxTemplate still renders
    # the old layer. Only a canary that remembers the original plan names
    # acme-bot at all.
    "stale-cleared-template": {
        "stale_agent_templates": {
            "acme-bot": "ghcr.io/acme/acme-bot-runner@sha256:"
            + "a" * 64,
        },
    },
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
rollback_attempted = any(call[:2] == ["helm", "rollback"] for call in previous)
rolled_back = rollback_attempted and not scenario["rollback_fails"]
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


def history_row(revision, status, version, description):
    """Helm's history formatter exposes chart version separately from revision.

    https://github.com/helm/helm/blob/v3.20.0/cmd/helm/history.go
    Rollback creates a new revision rather than changing the old one in place:
    https://github.com/helm/helm/blob/v3.20.0/pkg/action/rollback.go
    """
    return {
        "revision": revision,
        "updated": "2026-09-12T00:00:00Z",
        "status": status,
        "chart": f"curie-{version}",
        "app_version": version,
        "description": description,
    }


def release_history():
    if upgraded and scenario["history_after_upgrade"] is not None:
        return copy.deepcopy(scenario["history_after_upgrade"])
    pending = scenario["pending_status"]
    if scenario["metadata_missing_before"] and not upgraded:
        return None
    if pending:
        rows = [] if scenario["no_serving_revision"] else [
            history_row(4, "deployed", scenario["before"], "Upgrade complete")
        ]
        rows.append(history_row(5, pending, scenario["target"], "Preparing upgrade"))
        if rollback_attempted:
            # Helm leaves the old pending row unchanged. A successful
            # rollback supersedes deployed rows; a wait failure stores the
            # new rollback revision as failed and leaves the old deployed
            # revision serving.
            # https://github.com/helm/helm/blob/v3.20.0/pkg/action/rollback.go
            if rolled_back:
                for row in rows:
                    if row["status"] == "deployed":
                        row["status"] = "superseded"
            status = "deployed" if rolled_back else "failed"
            rows.append(history_row(6, status, scenario["before"], "Rollback to 4"))
        if upgraded and not scenario["upgrade_fails"]:
            for row in rows:
                if row["status"] == "deployed":
                    row["status"] = "superseded"
            revision = 7 if rollback_attempted else 6
            rows.append(history_row(revision, "deployed", chart_version, "Upgrade complete"))
        return rows
    # #3849: Helm can fail before it stores a revision. Keep the original
    # deployed row in that case, including its old chart metadata.
    if scenario["metadata_missing_before"]:
        return [history_row(1, "deployed", chart_version, "Install complete")]
    rows = [history_row(1, "deployed", scenario["before"], "Install complete")]
    if upgraded and not scenario["upgrade_fails"]:
        rows[0]["status"] = "superseded"
        rows.append(history_row(2, "deployed", chart_version, "Upgrade complete"))
    return rows


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
    if any(
        operation.get("op") == "replace"
        and operation.get("path") == "/metadata/annotations/curietech.ai~1upgrade-holder"
        for operation in operations
    ):
        return "takeover"
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

# The pre-upgrade hooks the chart installs, by their real rendered names
# (`charts/curie/templates/worker-upgrade-drain.yaml` and `schema-migrate.yaml`).
# The drain Job, the attest Job, and schema-migrate are always present; only
# `failed_hook` decides which one refused. A fixture that published just one
# hook could not tell the drain facet apart from the general hook facet.
# The two documents the `values-drift` scenario serves, distinguished by an
# extraEnv entry the migration carries through untouched. Only the FIRST is a
# legitimate input: it is what Validate read and migrated.
FIRST_VALUES = {
    "config": {"schemaVersion": "0.8.4"},
    "api": {"extraEnv": [{"name": "FIRST_READ_ONLY", "value": "1"}]},
    "connectorCaller": {"existingSecret": "acme-caller-pair"},
}
DRIFTED_VALUES = {
    "config": {"schemaVersion": "0.8.4"},
    "api": {"extraEnv": [{"name": "SECOND_READ_MUST_NOT_WIN", "value": "1"}]},
    "connectorCaller": {"existingSecret": "acme-caller-pair"},
}

# The SandboxTemplate CRD the chart renders one runner template per agent into.
SANDBOX_TEMPLATES = "sandboxtemplates.extensions.agents.x-k8s.io"
# `charts/curie/values.yaml` `agentSandbox.runner.image`.
DEFAULT_RUNNER_IMAGE = "ghcr.io/curie-eng/curie-runner"
# `.Chart.Name` of `charts/curie`.
CHART_NAME = "curie"
# The per-agent maps `curie.agentSandboxPoolAgents` unions; `poolAgents` is a
# list and handled beside them.
POOL_AGENT_MAPS = ("connectorSecrets", "registryEgress", "runnerImages", "workspaceSizeLimits")


def chart_values():
    path = root / "chart-values.json"
    return json.loads(path.read_text()) if path.exists() else {}


def release_values(revision=None):
    """Revision-pinned retained values, or Helm's newest revision by default.

    https://github.com/helm/helm/blob/v3.20.0/pkg/action/get_values.go
    """
    revision_values = scenario["revision_values"]
    if revision_values is not None:
        history = release_history() or []
        row = history[-1] if revision is None and history else next(
            (row for row in history if str(row["revision"]) == revision), None
        )
        if row is None:
            raise ValueError(f"release revision {revision} not found")
        key = str(row["revision"])
        if key in revision_values:
            return copy.deepcopy(revision_values[key])
        if row["description"].startswith("Rollback to "):
            return copy.deepcopy(revision_values[row["description"].split()[-1]])
    applied = captured("values", ".yaml")
    source = applied[-1] if applied else root / "retained.json"
    text = source.read_text() if source.exists() else "{}"
    return json.loads(text or "{}")


def helm_trunc_trim(text):
    """`trunc 63 | trimSuffix "-"`: sprig's trimSuffix removes exactly one dash."""
    text = text[:63]
    return text[:-1] if text.endswith("-") else text


def effective_name_value(values, key):
    """`.Values.<key>` as Helm sees it: merged by key presence.

    A release value that is present wins even when it is "" or None (Helm
    deletes a null key, and an empty string renders as empty); only an absent
    key falls back to the chart default.
    """
    if key in values:
        return values[key]
    return chart_values().get(key)


def fullname(values):
    """`curie.fullname` in `charts/curie/templates/_helpers.tpl` for RELEASE.

    Reads the effective values: a `fullnameOverride` or `nameOverride` set only
    in the target chart's defaults (`chart-values.json`) still names the
    templates, the way Helm coalesces chart defaults beneath release values.
    """
    override = effective_name_value(values, "fullnameOverride")
    if override:
        return helm_trunc_trim(str(override))
    name = effective_name_value(values, "nameOverride") or CHART_NAME
    if name in RELEASE:
        return helm_trunc_trim(RELEASE)
    return helm_trunc_trim(f"{RELEASE}-{name}")


def sandbox_template(name, image):
    return {
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
        "kind": "SandboxTemplate",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/name": "curie",
                "app.kubernetes.io/instance": RELEASE,
                "app.kubernetes.io/component": "agent-sandbox",
            },
        },
        "spec": {"podTemplate": {"spec": {"containers": [{"name": "runner", "image": image}]}}},
    }


def sandbox_templates():
    """The SandboxTemplates `charts/curie/templates/agent-sandbox.yaml` renders."""
    values = release_values()
    sandbox = values.get("agentSandbox") or {}
    prefix = fullname(values)
    runner = {}
    for source in (
        (chart_values().get("agentSandbox") or {}).get("runner") or {},
        sandbox.get("runner") or {},
    ):
        for key, value in source.items():
            if value is None:
                runner.pop(key, None)
            else:
                runner[key] = value
    # `curie.image`: a digest wins, else the tag, else the chart appVersion.
    image = runner.get("image") or DEFAULT_RUNNER_IMAGE
    if runner.get("digest"):
        platform = f"{image}@{runner['digest']}"
    else:
        platform = f"{image}:{runner.get('tag') or scenario['show_chart']}"
    agents = set()
    for key in POOL_AGENT_MAPS:
        if isinstance(sandbox.get(key), dict):
            agents.update(sandbox[key])
    if isinstance(sandbox.get("poolAgents"), list):
        agents.update(sandbox["poolAgents"])
    layers = sandbox.get("runnerImages") or {}
    stale = scenario["stale_agent_templates"]
    agents.update(stale)
    items = [sandbox_template(f"{prefix}-runner", platform)]
    for agent in sorted(agents):
        layer = layers.get(agent)
        agent_image = platform if scenario["template_ignores_runner_images"] or not layer else layer
        agent_image = stale.get(agent, agent_image)
        items.append(sandbox_template(f"{prefix}-agent-{agent}-runner", agent_image))
    return items


def matches_selector(item, selector):
    labels = item["metadata"].get("labels", {})
    for term in filter(None, (selector or "").split(",")):
        key, _, value = term.partition("=")
        if labels.get(key) != value:
            return False
    return True


def retirement_inputs():
    path = root / "retirement.json"
    return json.loads(path.read_text()) if path.exists() else {}


def retirement_failure(resource):
    if retirement_inputs().get("fail_read") == resource:
        print(f"Error from server (Forbidden): {resource} is forbidden", file=sys.stderr)
        sys.exit(1)


def emit_retirement_payload(payload):
    if isinstance(payload, str):
        print(payload)
        sys.exit(0)
    emit(payload)


def retirement_claims(agent):
    retirement_failure("claims")
    payload = retirement_inputs().get("agents", {}).get(agent, {}).get("claims")
    if payload is None:
        # The vendored CRD prints .status.sandbox.name as the claim's sandbox
        # (charts/curie/crds/crd-sandboxclaims.yaml), the bound pod's name.
        item = {
            "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
            "kind": "SandboxClaim",
            "metadata": {
                "name": f"{agent}-old-claim",
                "labels": {"curietech.ai/agent": agent},
            },
            "status": {"sandbox": {"name": f"{agent}-old-sandbox"}},
        }
        payload = {"apiVersion": "v1", "kind": "List", "items": [item]}
    emit_retirement_payload(payload)


def retirement_pods():
    retirement_failure("pods")
    inputs = retirement_inputs()
    payload = inputs.get("pods")
    if payload is None:
        # One old-layer pod per bound agent. Like a platform-pool pod, it
        # carries no curietech.ai/agent label: claim labels never reach pods.
        retained = json.loads((root / "retained.json").read_text())
        layers = (retained.get("agentSandbox") or {}).get("runnerImages") or {}
        items = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": f"{agent}-old-sandbox"},
                "spec": {"containers": [{"name": "runner", "image": image}]},
            }
            for agent, image in sorted(layers.items())
            if isinstance(image, str)
        ]
        payload = {"apiVersion": "v1", "kind": "List", "items": items}
    emit_retirement_payload(payload)


HOOK_NAMES = {
    "upgrade-drain": f"{RELEASE}-upgrade-drain",
    "upgrade-drain-attest": f"{RELEASE}-upgrade-drain-attest",
    "schema-migrate": f"{RELEASE}-schema-migrate",
}

hooks = []
jobs = []
for key, name in HOOK_NAMES.items():
    manifest = {
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/instance": RELEASE,
                "app.kubernetes.io/managed-by": "Helm",
            },
            "annotations": {"helm.sh/hook": "pre-upgrade"},
        },
    }
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

# Hook membership comes from Helm's documented annotation, and JobStatus.active
# counts pending and running pods. A selected active Job without the annotation
# must not block takeover.
# https://helm.sh/docs/topics/charts_hooks/
# https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/job-v1/#JobStatus
ownership_jobs = copy.deepcopy(jobs)
if scenario["active_hook"]:
    ownership_jobs[0]["status"] = {"active": 1}
if scenario["active_plain_job"]:
    plain_job = copy.deepcopy(ownership_jobs[0])
    plain_job["metadata"]["name"] = f"{RELEASE}-ordinary-job"
    plain_job["metadata"].pop("annotations")
    plain_job["status"] = {"active": 1}
    ownership_jobs.append(plain_job)

if program == "helm":
    # Helm v3.20 removes rel.Chart before serializing status and keeps the
    # numeric release revision at version:
    # https://github.com/helm/helm/blob/v3.20.0/cmd/helm/status.go
    if args[0] == "history":
        history = release_history()
        if history is None:
            print("Error: release: not found", file=sys.stderr)
            sys.exit(1)
        emit(history)
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
        history = release_history()
        if history is None:
            print('Error: release: not found', file=sys.stderr)
            sys.exit(1)
        shape = scenario["metadata_after_shape"] if upgraded else "valid"
        revision = flag_value("--revision")
        # Without --revision Helm reads the newest release, even while pending.
        # Keep that behavior so a missing pin exposes the actual bug.
        # https://github.com/helm/helm/blob/v3.20.0/pkg/action/action.go
        row = history[-1] if revision is None else next(
            (row for row in history if str(row["revision"]) == revision), None
        )
        if row is None:
            print(f"Error: release revision {revision} not found", file=sys.stderr)
            sys.exit(1)
        chart_version = row["app_version"]
        revision_versions = scenario.get("revision_versions") or {}
        if revision and revision in revision_versions:
            chart_version = revision_versions[revision]
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
            "revision": row["revision"],
            "status": row["status"],
            "deployedAt": "2026-09-12T00:00:00Z",
        }
        if shape == "missing":
            del metadata["version"]
        elif shape == "numeric":
            metadata["version"] = 2
        emit(metadata)
    if args[0] == "rollback":
        history = release_history() or []
        requested = args[2] if len(args) > 2 else ""
        serving = next((row for row in history if row["status"] == "deployed"), None)
        if serving is None or requested != str(serving["revision"]):
            print("Error: rollback must name the serving revision", file=sys.stderr)
            sys.exit(1)
        if scenario["rollback_fails"]:
            print("Error: rollback failed: password=acme-rollback-token", file=sys.stderr)
            sys.exit(1)
        print(f'Rollback to {requested} was a success')
        sys.exit(0)
    if args[:2] == ["get", "values"]:
        # An absent Helm release cannot have retained values. GetValues.Run
        # uses the same revision lookup as GetMetadata.Run:
        # https://github.com/helm/helm/blob/v3.20.0/pkg/action/get_values.go
        if scenario["metadata_missing_before"] and not upgraded:
            print("Error: release: not found", file=sys.stderr)
            sys.exit(1)
        if scenario["values_fail"]:
            print('Error from server (NotFound): namespaces "ns" not found', file=sys.stderr)
            sys.exit(1)
        if scenario["values_drift"]:
            reads = sum(1 for call in previous if call[:3] == ["helm", "get", "values"])
            print(json.dumps(DRIFTED_VALUES if reads else FIRST_VALUES))
            sys.exit(0)
        if scenario["revision_values"] is not None:
            try:
                emit(release_values(flag_value("--revision")))
            except ValueError as error:
                print(f"Error: {error}", file=sys.stderr)
                sys.exit(1)
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
    if args[:2] == ["show", "values"] and (root / "chart-values.json").exists():
        # JSON is YAML. Without the file this falls through to exit 64 below.
        print((root / "chart-values.json").read_text())
        sys.exit(0)
    if args[0] == "template":
        # @spec CLUSTER-VALUES-FILES c2: outside-boundary Helm parsing is real.
        if args[1] == "curie-values-lint" and os.environ.get("VALUES_FILES_REAL_HELM"):
            import subprocess
            sys.exit(subprocess.call([os.environ["VALUES_FILES_REAL_HELM"], *args]))
        show_only = flag_value("--show-only")
        if show_only is None:
            for arg in args:
                if arg.startswith("--show-only="):
                    show_only = arg.split("=", 1)[1]
                    break
        if show_only == "templates/schema-compat.yaml":
            values_file = flag_value("-f")
            if values_file:
                capture("schema-values", ".json", values_file)
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
        if show_only is None:
            if scenario["drain_render_fails"]:
                print("Error: target render failed", file=sys.stderr)
                sys.exit(1)
            values_file = flag_value("-f")
            values = json.loads(Path(values_file).read_text()) if values_file else {}
            if values_file:
                capture("render-values", ".json", values_file)
            worker = values.get("worker", {})
            if (
                worker.get("deploy", True) is False
                or worker.get("upgradeDrain", {}).get("enabled", True) is False
            ):
                emit({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "no-drain"}})
            budget = int(worker.get("deliveryBudgetSeconds", 600)) + int(
                worker.get("deliveryShutdownReserveSeconds", 60)
            )
            drain = max(int(worker.get("upgradeDrain", {}).get("timeoutSeconds", 900)), budget)
            grace = max(int(worker.get("terminationGracePeriodSeconds", 1860)), budget)
            minimum = str(drain + 120 + grace + 60)
            annotation = scenario["drain_annotation"]
            if annotation == "auto":
                annotation = minimum
            annotations = {"helm.sh/hook": "pre-upgrade"}
            if annotation is not None:
                annotations["curie.ai/minimum-helm-timeout-seconds"] = annotation
            emit({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": f"{RELEASE}-upgrade-drain",
                    "labels": {"app.kubernetes.io/component": "upgrade-drain"},
                    "annotations": annotations,
                },
            })
        print(
            f"Error: could not find template {show_only or '<template>'} in chart",
            file=sys.stderr,
        )
        sys.exit(1)
    if args[0] == "upgrade":
        values = flag_value("-f")
        if values:
            capture("values", ".yaml", values)
        if scenario["upgrade_fails"]:
            print(
                "Error: execution error at (curie/templates/agent-sandbox.yaml:870:4): "
                'agentSandbox.runnerImages.acme-bot must be a digest reference, got "<nil>"',
                file=sys.stderr,
            )
            sys.exit(1)
        print("Release accepted")
        sys.exit(0)

if program == "kubectl":
    if args[:2] == ["get", "jobs"]:
        shape = scenario["hook_jobs_shape"]
        if shape == "failed":
            print("Error from server (Forbidden): jobs is forbidden", file=sys.stderr)
            sys.exit(1)
        if shape == "malformed":
            print("{")
            sys.exit(0)
        if shape == "not-list":
            emit({"apiVersion": "v1", "kind": "List", "items": {"unexpected": "map"}})
        selector = flag_value("-l")
        emit({
            "apiVersion": "v1",
            "kind": "List",
            "items": [item for item in ownership_jobs if matches_selector(item, selector)],
        })
    if args[:1] == ["-n"] and "delete" in args and "sandboxclaim" in args:
        if retirement_inputs().get("fail_delete"):
            print("Error from server (Forbidden): sandboxclaims is forbidden", file=sys.stderr)
            sys.exit(1)
        print("sandboxclaim deleted")
        sys.exit(0)
    if args[:2] == ["-n", NAMESPACE] and args[2:4] == ["get", SANDBOX_TEMPLATES]:
        inputs = retirement_inputs()
        prior_reads = sum(
            call[:5] == ["kubectl", "-n", NAMESPACE, "get", SANDBOX_TEMPLATES]
            for call in previous
        )
        # Only retirement's first read fails; the later canary still sees
        # the healthy rendered templates and can complete the upgrade.
        if upgraded and prior_reads == 0:
            if inputs.get("fail_read") == "templates":
                print("Error from server (Forbidden): templates is forbidden", file=sys.stderr)
                sys.exit(1)
            if "templates" in inputs:
                payload = inputs["templates"]
                if isinstance(payload, str):
                    print(payload)
                    sys.exit(0)
                emit(payload)
        selector = flag_value("-l")
        emit(
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [item for item in sandbox_templates() if matches_selector(item, selector)],
            }
        )
    if args[:2] == ["-n", NAMESPACE] and args[2:4] == ["get", "pods"]:
        # Pods are read namespace-wide and matched to claims by name only. A
        # selector would miss platform-pool pods, which carry no agent label.
        if args != ["-n", NAMESPACE, "get", "pods", "-o", "json"]:
            print(
                "agent retirement requires an unlabelled namespace-wide JSON pod read",
                file=sys.stderr,
            )
            sys.exit(1)
        retirement_pods()
    if args[:2] == ["-n", NAMESPACE] and args[2:4] == ["get", "sandboxclaim"]:
        selector = flag_value("-l") or ""
        prefix = "curietech.ai/agent="
        if not selector.startswith(prefix) or args[-2:] != ["-o", "json"]:
            print("agent retirement requires a labelled JSON claim read", file=sys.stderr)
            sys.exit(1)
        retirement_claims(selector[len(prefix):])
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
        if kind == "takeover" and scenario["takeover_conflict"]:
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
        # @spec CLUSTER-VALUES-FILES c3: invalidate only this test-owned temp path.
        removed_tmp = os.environ.get("VALUES_FILES_REMOVE_TMP_AFTER_SCHEMA_PROBE")
        if removed_tmp:
            os.rmdir(removed_tmp)
        print(scenario["alembic_current"])
        sys.exit(0)

print("unhandled recording command: " + repr([program, *args]), file=sys.stderr)
sys.exit(64)

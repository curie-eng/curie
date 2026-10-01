"""Deciding what a connector reconcile changes (ADR-0090, #1184).

The decision is the dangerous part, not the API call: a plan that names one
object too many takes down a live agent's tools. These assert the two
properties that keep that from happening, and the drift-comparison behaviour
that keeps the loop from rewriting the world forever.
"""

from __future__ import annotations

from typing import Any

from curie_worker.connector_agent import own
from curie_worker.connector_reconcile import OWNER_LABEL, plan, stamp_hash


def obj(kind: str, name: str, owner: str | None = None, spec: Any = None) -> dict[str, Any]:
    labels = {OWNER_LABEL: owner} if owner else {}
    return {
        "apiVersion": "v1",
        "kind": kind,
        "metadata": {"name": name, "labels": labels},
        "spec": spec if spec is not None else {"replicas": 1},
    }


def live(o: dict[str, Any], **server_fields: Any) -> dict[str, Any]:
    """The same object as the cluster would return it after Curie applied it.

    Carries the digest, because everything the applier writes is stamped. An
    object WITHOUT one is a different case -- something the CLI applier created
    -- and reads as drift so the reconciler adopts it.
    """

    out = stamp_hash(o)
    out["metadata"] = {
        **out["metadata"],
        "uid": "abc-123",
        "resourceVersion": "4711",
        "creationTimestamp": "2026-07-31T00:00:00Z",
        "namespace": "curie",
        **server_fields,
    }
    return out


# --------------------------------------------------------------------------- #
# Never touch what Curie did not create
# --------------------------------------------------------------------------- #
def test_an_unlabelled_object_is_invisible() -> None:
    # The first adopting agent repo ran a hand-written connector beside a
    # Curie-managed one through its entire migration. Deleting it would have
    # taken the bot's only working Grafana path with it.
    handwritten = obj("Deployment", "grafana-mcp")  # no owner label
    p = plan(desired=[], live=[handwritten], agent="acme-bot")
    assert p.delete == []
    assert p.is_noop


def test_another_agents_connector_is_never_pruned() -> None:
    # Two agents in one release each declare `grafana` (#1116). One removing it
    # must not delete the other's.
    mine = obj("Deployment", "rel-acme-bot-mcp-grafana", owner="acme-bot")
    theirs = obj("Deployment", "rel-acme-dev-mcp-grafana", owner="acme-dev")
    p = plan(desired=[], live=[live(mine), live(theirs)], agent="acme-bot")
    assert p.delete == [("Deployment", "rel-acme-bot-mcp-grafana")]


# --------------------------------------------------------------------------- #
# Converge
# --------------------------------------------------------------------------- #
def test_a_declared_object_that_does_not_exist_is_applied() -> None:
    want = obj("Service", "svc", owner="a")
    p = plan(desired=[want], live=[], agent="a")
    assert p.apply == [stamp_hash(want)]
    assert p.delete == []


def test_an_undeclared_owned_object_is_deleted() -> None:
    # Removing a connector from connectors.yaml must remove it. Without this a
    # pod keeps running with a credential mounted and nothing referencing it.
    p = plan(desired=[], live=[live(obj("Service", "svc", owner="a"))], agent="a")
    assert p.delete == [("Service", "svc")]


def test_an_object_that_already_matches_is_left_alone() -> None:
    want = obj("Service", "svc", owner="a")
    p = plan(desired=[want], live=[live(want)], agent="a")
    assert p.apply == []
    assert p.unchanged == [("Service", "svc")]
    assert p.is_noop


# --------------------------------------------------------------------------- #
# Drift: the behaviour that overrides a human
# --------------------------------------------------------------------------- #
def test_a_drifted_object_is_reapplied_and_reported_as_drift() -> None:
    # This is the one action that reverts someone's `kubectl edit`. Correct for
    # a declared system, and it must be visible rather than silent.
    want = obj("Deployment", "dep", owner="a", spec={"replicas": 1})
    edited = live(obj("Deployment", "dep", owner="a", spec={"replicas": 5}))
    p = plan(desired=[want], live=[edited], agent="a")
    assert p.apply == [stamp_hash(want)]
    assert p.drifted == [("Deployment", "dep")]


def test_server_added_metadata_is_not_drift() -> None:
    # uid, resourceVersion, creationTimestamp and friends exist on every live
    # object and on no rendered one. Counting them as drift would rewrite every
    # object every loop -- a hot loop wearing a reconciler's clothes.
    want = obj("Service", "svc", owner="a")
    p = plan(desired=[want], live=[live(want, managedFields=[{"manager": "kubectl"}])], agent="a")
    assert p.apply == []
    assert p.drifted == []


def test_a_status_block_is_not_drift() -> None:
    want = obj("Deployment", "dep", owner="a")
    running = live(want)
    running["status"] = {"readyReplicas": 1}
    p = plan(desired=[want], live=[running], agent="a")
    assert p.is_noop


# --------------------------------------------------------------------------- #
# The whole set at once
# --------------------------------------------------------------------------- #
def test_a_mixed_state_converges_in_one_plan() -> None:
    keep = obj("Service", "keep", owner="a")
    drift = obj("Deployment", "drift", owner="a", spec={"replicas": 1})
    p = plan(
        desired=[keep, drift, obj("NetworkPolicy", "new", owner="a")],
        live=[
            live(keep),
            live(obj("Deployment", "drift", owner="a", spec={"replicas": 9})),
            live(obj("Secret", "stale", owner="a")),
            live(obj("Deployment", "someone-elses", owner="other")),
            obj("Service", "handwritten"),
        ],
        agent="a",
    )
    assert [o["metadata"]["name"] for o in p.apply] == ["drift", "new"]
    assert p.delete == [("Secret", "stale")]
    assert p.unchanged == [("Service", "keep")]
    assert p.drifted == [("Deployment", "drift")]


# --------------------------------------------------------------------------- #
# The reconciler inherits the locked digest -- it never resolves a build
# itself (ADR 0113, #1690)
# --------------------------------------------------------------------------- #
def test_a_built_connectors_desired_object_carries_the_locked_digest() -> None:
    """`apply_lock` is the single resolution point (Section 3,
    `connector_agent.py:62`: the worker "consumes what the API returns ...
    inherits the digest from the API render"). `connector_reconcile.plan`
    itself never touches a `build:` declaration or a lock -- it only ever sees
    whatever `desired` the caller hands it -- so this drives the REAL
    `apply_lock` -> `render` -> `own` -> `plan` path end to end rather than
    poking a Deployment's `image` field by hand, and proves the object `plan`
    decides to apply carries the resolved digest, not the source declaration.
    """
    from plugin_format import connector_render
    from plugin_format.connector_lock import ConnectorLockFile, apply_lock
    from plugin_format.connectors import validate_connectors

    digest = "ghcr.io/acme-corp/acme-bot-k8s-write-mcp@sha256:" + "3" * 64
    declared, errors = validate_connectors(
        {
            "connectors": {
                "k8s-write": {
                    "build": {
                        "context": "connectors/k8s-write",
                        "platforms": ["linux/amd64"],
                    }
                }
            }
        }
    )
    assert errors == [], errors
    lock = ConnectorLockFile.model_validate(
        {
            "version": 1,
            "connectors": {
                "k8s-write": {
                    "image": digest,
                    "delivery": "registry",
                    "platforms": ["linux/amd64"],
                    "source_digest": "sha256:" + "4" * 64,
                }
            },
        }
    )
    resolved = apply_lock(declared, lock, portable=True)
    rendered = connector_render.render(
        release="acme-bot",
        agent="acme-bot",
        namespace="acme-bot",
        app_name="acme-bot",
        connector="k8s-write",
        spec=resolved.connectors["k8s-write"],
        secret_name="conn-secrets",
        proxy=connector_render.ConnectorProxy(
            image="ghcr.io/curie-eng/curie-worker:0.0.0",
            public_keys=("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=",),
        ),
    )
    desired = [own(o, "acme-bot") for o in rendered]

    p = plan(desired=desired, live=[], agent="acme-bot")

    deployment = next(o for o in p.apply if o["kind"] == "Deployment")
    assert deployment["spec"]["template"]["spec"]["containers"][0]["image"] == digest
    # The unresolved declaration never reaches the plan at all: only
    # apply_lock's OUTPUT does. A caller that skipped apply_lock would either
    # have `render` raise (its own guard) or, if that regressed, apply a
    # Deployment with no image rather than the locked digest.
    assert "build" not in str(rendered)

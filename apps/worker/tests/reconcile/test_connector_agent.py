"""Reconciling one agent (ADR-0090, #1184).

Most of these are about the operator-supplied Secret, because that is the only
way this step destroys something. Everything else it can get wrong is recoverable
on the next pass.
"""

from __future__ import annotations

from typing import Any

import pytest
from curie_worker.connector_agent import (
    RenderedConnectors,
    own,
    reconcile_agent,
)
from curie_worker.connector_reconcile import OWNER_LABEL, stamp_hash

AGENT = "acme-bot"
NS = "curie"
SECRET_NAME = "curie-acme-bot-connector-secrets"


def manifest(kind: str, name: str) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": kind, "metadata": {"name": name}, "spec": {"x": 1}}


def live_copy(obj: dict[str, Any], agent: str = AGENT) -> dict[str, Any]:
    """As the cluster would return it after the reconciler applied it."""

    stored = stamp_hash(own(obj, agent))
    stored["metadata"]["uid"] = "u-1"
    return stored


class Source:
    def __init__(self, rendered: RenderedConnectors) -> None:
        self._rendered = rendered
        self.calls: list[tuple[str, str]] = []

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        self.calls.append((agent_id, version_id))
        return self._rendered


class FakeClient:
    def __init__(self, live: list[dict[str, Any]] | None = None) -> None:
        self._live = live or []
        self.applied: list[str] = []
        self.deleted: list[tuple[str, str]] = []

    def list_owned(self, namespace: str, owner: str) -> list[dict[str, Any]]:
        return [
            o for o in self._live if (o["metadata"].get("labels") or {}).get(OWNER_LABEL) == owner
        ]

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        self.applied.append(obj["metadata"]["name"])

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.deleted.append((kind, name))


def run(source: Source, client: FakeClient):
    return reconcile_agent(
        source, client, agent=AGENT, agent_id="a-1", version_id="v-1", namespace=NS
    )


# --------------------------------------------------------------------------- #
# The credential hazard
# --------------------------------------------------------------------------- #
def test_it_never_prunes_the_operator_supplied_secret() -> None:
    # THE bug this module exists for. The CLI mints that Secret from values the
    # cluster does not hold, stamps the same owner label the reconciler prunes
    # on, and the API's render never emits it. A plain "own it and no longer
    # declare it -> delete it" pass therefore deletes the credential, and every
    # connector pod fails on its next restart.
    secret = live_copy({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": SECRET_NAME}})
    dep = manifest("Deployment", "curie-acme-bot-mcp-grafana")
    client = FakeClient([secret, live_copy(dep)])
    source = Source(
        RenderedConnectors(
            manifests=[dep], owned_secret_name=SECRET_NAME, owned_secret_keys=["GRAFANA_TOKEN"]
        )
    )

    outcome = run(source, client)

    assert client.deleted == [], "the operator's credential must never be pruned"
    assert outcome.skipped is None
    assert outcome.ok


def test_an_agent_whose_credential_was_never_provisioned_is_skipped() -> None:
    # Applying a Deployment whose secretKeyRef points at nothing yields pods
    # stuck rather than failing loudly, several layers from the cause.
    client = FakeClient([])
    source = Source(
        RenderedConnectors(
            manifests=[manifest("Deployment", "dep")],
            owned_secret_name=SECRET_NAME,
            owned_secret_keys=["GRAFANA_TOKEN"],
        )
    )

    outcome = run(source, client)

    assert outcome.skipped is not None
    assert "curie cluster deploy" in outcome.skipped, "the message must say what to do"
    # Deliberately NOT the Secret's name or its keys. Both are identifiers
    # rather than values, but they are of no use here -- the operator's action
    # is the same either way -- and leaving them out keeps a credential name out
    # of a log stream that may be shipped somewhere less protected.
    assert SECRET_NAME not in outcome.skipped
    assert "GRAFANA_TOKEN" not in outcome.skipped
    # Vacuous on the delete half: this FakeClient starts with nothing live, so
    # deleted == [] proves nothing about pruning. The delete-half assertion
    # that would actually catch a regression lives in
    # test_an_unprovisioned_agent_still_prunes_what_it_no_longer_declares below.
    assert client.applied == [] and client.deleted == []


def test_an_unprovisioned_agent_still_prunes_what_it_no_longer_declares() -> None:
    # #1214: the skip above exists to stop a Deployment being applied with a
    # secretKeyRef pointing at nothing. It does NOT need to stop a delete --
    # removing an object is never harmed by a missing Secret. The old
    # `return AgentOutcome(agent=agent, skipped=reason)` stopped both, so an
    # agent that removed a connector while also missing its operator secret
    # leaked that connector's objects until someone happened to run
    # `curie cluster deploy` for an unrelated reason. `kept` stays declared
    # (so the still-owed pass must leave it alone); `new_dep` is declared but
    # not yet live, so if applies were not actually suppressed it would show
    # up in `client.applied` -- proving the suppression rather than assuming it.
    kept = manifest("Deployment", "kept-dep")
    new_dep = manifest("Deployment", "new-dep")
    stale_dep = live_copy(manifest("Deployment", "stale-dep"))
    stale_netpol = live_copy(manifest("NetworkPolicy", "stale-netpol"))
    stale_svc = live_copy(manifest("Service", "stale-svc"))
    client = FakeClient([live_copy(kept), stale_dep, stale_netpol, stale_svc])
    source = Source(
        RenderedConnectors(
            manifests=[kept, new_dep],
            owned_secret_name=SECRET_NAME,
            owned_secret_keys=["TOKEN"],
        )
    )

    outcome = run(source, client)

    assert outcome.skipped is not None
    assert "curie cluster deploy" in outcome.skipped
    assert SECRET_NAME not in outcome.skipped and "TOKEN" not in outcome.skipped

    assert outcome.report is not None
    assert outcome.report.applied == []
    assert outcome.report.deleted == [
        ("Deployment", "stale-dep"),
        ("NetworkPolicy", "stale-netpol"),
        ("Service", "stale-svc"),
    ]
    assert client.applied == [], "no values exist for the missing secret -- applying stays off"
    assert ("Deployment", "kept-dep") not in client.deleted, "still declared, not this pass's job"


def test_no_secret_of_any_name_is_pruned_on_the_unprovisioned_branch() -> None:
    # Why no Secret of any name is manageable here: the `connector_agent` module
    # docstring. `stale-svc` keeps the test honest -- pruning still happens on
    # this branch (#1214), it is Secrets specifically that are held back.
    stale_secret = live_copy(
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "old-connector-secret"}}
    )
    stale_svc = live_copy(manifest("Service", "stale-svc"))
    client = FakeClient([stale_secret, stale_svc])
    source = Source(
        RenderedConnectors(manifests=[], owned_secret_name=SECRET_NAME, owned_secret_keys=["TOKEN"])
    )

    outcome = run(source, client)

    assert outcome.skipped is not None
    pruned = client.deleted
    assert [d for d in pruned if d[0] == "Secret"] == [], "no Secret may be pruned on this branch"
    assert ("Service", "stale-svc") in pruned, "non-Secret objects must still prune (#1214)"


def test_a_failed_prune_on_the_skip_path_is_reported_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The skip branch runs deletes, so it can FAIL deletes -- a finalizer or an
    # RBAC gap leaves the object live and the same failure recurs every pass.
    # Returning straight off `execute()` makes that invisible: the only record
    # of the pass is the skip reason, which names neither the object nor the
    # error, so the operator sees a routine skip while a prune silently never
    # lands. It stays a skip and it stays non-raising -- one agent's stuck
    # delete must not abort the pass for the rest.
    class ExplodingDelete(FakeClient):
        def delete(self, namespace: str, kind: str, name: str) -> None:
            raise RuntimeError("apiserver refused: forbidden")

    stale_svc = live_copy(manifest("Service", "stale-svc"))
    client = ExplodingDelete([stale_svc])
    source = Source(
        RenderedConnectors(manifests=[], owned_secret_name=SECRET_NAME, owned_secret_keys=["TOKEN"])
    )

    with caplog.at_level("WARNING", logger="curie_worker.connector_agent"):
        outcome = run(source, client)

    assert outcome.skipped is not None, "a failed prune does not turn the skip into a normal pass"
    assert outcome.report is not None and outcome.report.failures
    assert not outcome.ok
    assert any(
        "Service" in r.getMessage()
        and "stale-svc" in r.getMessage()
        and "apiserver refused: forbidden" in r.getMessage()
        for r in caplog.records
    ), "the failing object and its error must reach a log stream, not just the outcome"


def test_a_reference_form_bundle_has_no_exception_at_all() -> None:
    # `secrets: [{from_secret: ...}]` (#1163) points at a Secret someone else
    # provisioned, so no key needs resolving and nothing is protected. This is
    # the path ADR-0090 calls the prerequisite.
    client = FakeClient([])
    source = Source(RenderedConnectors(manifests=[manifest("Deployment", "dep")]))

    outcome = run(source, client)

    assert outcome.skipped is None
    assert client.applied == ["dep"]


def test_a_stale_secret_is_still_pruned_when_no_keys_are_owned() -> None:
    # The protection is scoped to the credential the operator supplies. An
    # owned Secret with no owned keys is ordinary garbage and must still go, or
    # removing a connector leaves its credential behind forever.
    stale = live_copy({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "stale"}})
    client = FakeClient([stale])
    source = Source(RenderedConnectors(manifests=[]))

    run(source, client)
    assert client.deleted == [("Secret", "stale")]


def test_the_secret_exclusion_does_not_reach_the_provisioned_path() -> None:
    # Pins the SCOPE of the by-kind exclusion, which the test above cannot: that
    # one has no owned keys, so `needs_operator_credentials` is False and
    # widening the guard to `unprovisioned or rendered.needs_operator_credentials`
    # survives it. Here the render-named Secret IS live, so the branch is not
    # taken and a stale-named owned Secret is ordinary garbage that must still
    # go -- under the widened guard it would be held back and leak forever.
    keeper = live_copy({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": SECRET_NAME}})
    stale = live_copy(
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "old-connector-secret"}}
    )
    client = FakeClient([keeper, stale])
    source = Source(
        RenderedConnectors(manifests=[], owned_secret_name=SECRET_NAME, owned_secret_keys=["TOKEN"])
    )

    outcome = run(source, client)

    assert outcome.skipped is None, "the render-named Secret is live -- this is the normal path"
    # Exact list: the stale name went, and the render-named one did not.
    assert client.deleted == [("Secret", "old-connector-secret")]


# --------------------------------------------------------------------------- #
# Ownership
# --------------------------------------------------------------------------- #
def test_rendered_objects_are_stamped_with_the_owner() -> None:
    # The renderer does not set it -- it is a deployment fact, not a bundle
    # fact. Unstamped, the applier refuses to write the object, and rightly so.
    client = FakeClient([])
    run(Source(RenderedConnectors(manifests=[manifest("Service", "svc")])), client)
    assert client.applied == ["svc"]


def test_own_does_not_mutate_the_rendered_object() -> None:
    # The rendered set may be reused across agents; stamping one must not label
    # it for another.
    obj = manifest("Service", "svc")
    own(obj, AGENT)
    assert "labels" not in obj["metadata"]


def test_another_agents_objects_are_never_visible() -> None:
    theirs = live_copy(manifest("Deployment", "theirs"), agent="other-bot")
    client = FakeClient([theirs])
    run(Source(RenderedConnectors(manifests=[])), client)
    assert client.deleted == []


# --------------------------------------------------------------------------- #
# Ordinary operation
# --------------------------------------------------------------------------- #
def test_a_converged_agent_does_no_work() -> None:
    dep = manifest("Deployment", "dep")
    client = FakeClient([live_copy(dep)])
    outcome = run(Source(RenderedConnectors(manifests=[dep])), client)

    assert outcome.plan is not None and outcome.plan.is_noop
    assert client.applied == [] and client.deleted == []
    assert outcome.ok


def test_the_version_asked_for_is_the_one_rendered() -> None:
    source = Source(RenderedConnectors(manifests=[]))
    run(source, FakeClient([]))
    assert source.calls == [("a-1", "v-1")]


def test_a_failed_apply_is_reported_not_raised() -> None:
    # One agent failing must not abort the pass for every other agent.
    class Exploding(FakeClient):
        def apply(self, namespace: str, obj: dict[str, Any]) -> None:
            raise RuntimeError("apiserver said no")

    outcome = run(Source(RenderedConnectors(manifests=[manifest("Service", "svc")])), Exploding([]))
    assert not outcome.ok
    assert outcome.report is not None and outcome.report.failures


@pytest.mark.parametrize("keys", [[], ["TOKEN"]])
def test_a_missing_owned_secret_name_is_not_treated_as_a_protected_object(keys) -> None:
    # An empty name would protect ("Secret", "") -- matching nothing, but also
    # skipping every agent if the absence check were written carelessly.
    client = FakeClient([])
    outcome = run(
        Source(
            RenderedConnectors(
                manifests=[manifest("Service", "svc")], owned_secret_name="", owned_secret_keys=keys
            )
        ),
        client,
    )
    assert outcome.skipped is None
    assert client.applied == ["svc"]

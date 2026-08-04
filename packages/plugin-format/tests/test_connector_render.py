"""Deriving Kubernetes objects from a declared connector (ADR-0086, #1063).

The value of deriving rather than documenting is that specific defects become
unrepresentable. These tests pin the two that were actually hit by hand, so a
refactor cannot quietly reintroduce them.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from plugin_format import connector_render as r
from plugin_format.connectors import ConnectorSpec

HOSTED = ConnectorSpec(
    image="grafana/mcp-grafana:0.17.2",
    args=["-t", "streamable-http", "-disable-write"],
    env={"GRAFANA_URL": "https://g.example.com"},
    secrets=["GRAFANA_TOKEN"],
)
REMOTE = ConnectorSpec(url="https://mcp.internal/mcp", headers={"Authorization": "Bearer ${T}"})


def _objs(release: str = "acme-bot", app: str = "acme-bot") -> list[dict]:
    return r.render(release, "acme-bot", "acme-bot", app, "grafana", HOSTED, "conn-secrets")


# --------------------------------------------------------------------------- #
# The ClusterIP trap -- the defect this renderer exists to prevent
# --------------------------------------------------------------------------- #
def test_egress_rule_uses_a_podselector_never_an_ipblock() -> None:
    # A NetworkPolicy naming a Service ClusterIP can NEVER match: kube-proxy
    # DNATs the destination to a pod IP before the policy is evaluated. The
    # symptom is a bare connection refused, and on a CNI that ignores
    # NetworkPolicy (minikube's default) the broken rule looks identical to a
    # correct one -- so it survives local testing and fails in a real cluster.
    np = next(o for o in _objs() if o["kind"] == "NetworkPolicy")
    to = np["spec"]["egress"][0]["to"][0]
    assert "podSelector" in to
    assert "ipBlock" not in to


def test_egress_selects_exactly_the_pods_rail_1_denies() -> None:
    # Too narrow and the allow widens nothing (NetworkPolicy is additive, it
    # cannot narrow -- ADR-0067) so the sandbox still cannot reach the
    # connector. Too broad -- e.g. only `component` -- and it also grants egress
    # to every OTHER release's sandboxes in the namespace. Both fail silently.
    np = next(
        o
        for o in r.render("relA", "a", "ns", "acme-bot", "g", HOSTED, "s")
        if o["kind"] == "NetworkPolicy"
    )
    assert np["spec"]["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "acme-bot",
        "app.kubernetes.io/instance": "relA",
        "app.kubernetes.io/component": "runner-sandbox",
    }


def test_two_releases_do_not_select_each_others_sandboxes() -> None:
    a = next(
        o
        for o in r.render("relA", "a", "ns", "app", "g", HOSTED, "s")
        if o["kind"] == "NetworkPolicy"
    )
    b = next(
        o
        for o in r.render("relB", "a", "ns", "app", "g", HOSTED, "s")
        if o["kind"] == "NetworkPolicy"
    )
    assert a["spec"]["podSelector"] != b["spec"]["podSelector"]


# --------------------------------------------------------------------------- #
# The host-header trap
# --------------------------------------------------------------------------- #
def test_host_aliases_cover_every_name_the_sandbox_could_dial() -> None:
    # Servers that guard against DNS rebinding default their allowlist to
    # loopback, so an in-cluster caller reaching them by Service DNS gets
    # `forbidden: host not allowed`. Curie named the Service, so Curie can
    # supply the full set; an author would have to guess it.
    aliases = r.host_aliases("acme-bot", "a", "grafana", "ns", 8000)
    assert "acme-bot-a-mcp-grafana:8000" in aliases
    assert "acme-bot-a-mcp-grafana.ns:8000" in aliases
    assert "acme-bot-a-mcp-grafana.ns.svc.cluster.local:8000" in aliases


def test_injected_url_matches_the_service_that_was_rendered() -> None:
    # Hand-writing this URL is how a bundle ends up with an address that does
    # not resolve in the tier it is deployed to.
    svc = next(o for o in _objs() if o["kind"] == "Service")
    url = r.mcp_entry("acme-bot", "acme-bot", "acme-bot", "grafana", HOSTED)["url"]
    assert svc["metadata"]["name"] in url
    assert url.endswith("/mcp")


# --------------------------------------------------------------------------- #
# Hardening the author never writes, and so cannot forget
# --------------------------------------------------------------------------- #
def test_container_is_hardened_by_construction() -> None:
    dep = next(o for o in _objs() if o["kind"] == "Deployment")
    pod = dep["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["resources"]["limits"]["memory"]


def test_secrets_travel_by_reference_never_as_a_literal() -> None:
    dep = next(o for o in _objs() if o["kind"] == "Deployment")
    env = dep["spec"]["template"]["spec"]["containers"][0]["env"]
    entry = next(e for e in env if e["name"] == "GRAFANA_TOKEN")
    assert entry["valueFrom"]["secretKeyRef"]["name"] == "conn-secrets"
    assert "value" not in entry, "a secret must never be inlined into the manifest"


def test_plain_env_is_passed_through() -> None:
    dep = next(o for o in _objs() if o["kind"] == "Deployment")
    env = dep["spec"]["template"]["spec"]["containers"][0]["env"]
    assert {"name": "GRAFANA_URL", "value": "https://g.example.com"} in env


# --------------------------------------------------------------------------- #
# Remote connectors own no objects
# --------------------------------------------------------------------------- #
def test_remote_connector_renders_nothing_to_run() -> None:
    assert r.render("acme-bot", "a", "ns", "app", "internal", REMOTE, "s") == []


def test_remote_connector_keeps_its_own_url_and_headers() -> None:
    entry = r.mcp_entry("acme-bot", "a", "ns", "internal", REMOTE)
    assert entry["url"] == "https://mcp.internal/mcp"
    assert entry["headers"]["Authorization"] == "Bearer ${T}"


@pytest.mark.parametrize("kind", ["Service", "Deployment", "NetworkPolicy"])
def test_hosted_connector_renders_the_full_set(kind: str) -> None:
    assert any(o["kind"] == kind for o in _objs())


# --------------------------------------------------------------------------- #
# Anti-drift: the selector is only correct if it matches the CHART's
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_selector_matches_what_the_chart_actually_renders() -> None:
    # The two failure modes are both silent, so asserting against my own belief
    # about the labels proves nothing. Render the real chart and compare.
    chart = Path(__file__).resolve().parents[3] / "charts" / "curie"
    if not chart.is_dir():  # package tested outside the monorepo
        pytest.skip("chart not present")
    out = subprocess.run(
        ["helm", "template", "myrel", str(chart), "--set", "nameOverride=acme-bot"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    chart_selector = None
    for doc in yaml.safe_load_all(out):
        if (
            doc
            and doc.get("kind") == "NetworkPolicy"
            and "runner-default-deny-egress" in doc["metadata"]["name"]
        ):
            chart_selector = doc["spec"]["podSelector"]["matchLabels"]
    assert chart_selector, "could not find Rail 1's default-deny egress policy"
    assert r.sandbox_selector("myrel", "acme-bot") == chart_selector


# --------------------------------------------------------------------------- #
# Cross-AGENT collision -- #1116
# --------------------------------------------------------------------------- #
DEV = ConnectorSpec(image="grafana/mcp-grafana:0.17.2", env={"GRAFANA_URL": "https://dev.g"})
PROD = ConnectorSpec(image="grafana/mcp-grafana:0.17.2", env={"GRAFANA_URL": "https://prod.g"})


def test_two_agents_in_one_release_do_not_share_object_names() -> None:
    # Curie runs many agents per release. Release-scoped names meant acme-dev and
    # acme-prod both rendered `curie-mcp-grafana`, so deploying prod silently
    # repointed the DEV agent at the prod endpoint -- and, because the Secret was
    # release-scoped too, handed it the prod token. Nothing errored.
    dev = [
        o["metadata"]["name"]
        for o in r.render("curie", "acme-dev", "ns", "curie", "grafana", DEV, "s")
    ]
    prod = [
        o["metadata"]["name"]
        for o in r.render("curie", "acme-prod", "ns", "curie", "grafana", PROD, "s")
    ]
    assert not set(dev) & set(prod), f"agents share object names: {dev} vs {prod}"


def test_two_agents_do_not_share_pod_labels() -> None:
    # The Service selector is these labels. Sharing them would route one agent's
    # traffic to the other's pods even with distinct object names.
    dev = next(
        o
        for o in r.render("curie", "acme-dev", "ns", "curie", "grafana", DEV, "s")
        if o["kind"] == "Service"
    )
    prod = next(
        o
        for o in r.render("curie", "acme-prod", "ns", "curie", "grafana", PROD, "s")
        if o["kind"] == "Service"
    )
    assert dev["spec"]["selector"] != prod["spec"]["selector"]


def test_each_agent_gets_its_own_url() -> None:
    dev = r.mcp_entry("curie", "acme-dev", "ns", "grafana", DEV)["url"]
    prod = r.mcp_entry("curie", "acme-prod", "ns", "grafana", PROD)["url"]
    assert dev != prod


def test_over_long_names_stay_valid_dns_labels() -> None:
    name = r.object_name("a" * 30, "b" * 30, "c" * 40)
    assert len(name) <= 63
    assert name[0].isalnum() and name[-1].isalnum()


def test_over_long_names_that_share_a_prefix_still_differ() -> None:
    # Clipping alone would map these onto one object, reintroducing the very
    # collision the agent scoping exists to prevent.
    a = r.object_name("release", "agent-with-a-very-long-name-number-one", "grafana")
    b = r.object_name("release", "agent-with-a-very-long-name-number-two", "grafana")
    assert len(a) <= 63 and len(b) <= 63
    assert a != b


# --------------------------------------------------------------------------- #
# Placeholders: values only Curie can know -- #1156
# --------------------------------------------------------------------------- #
HOSTED_WITH_HOSTS = ConnectorSpec(
    image="grafana/mcp-grafana:0.17.2",
    args=["-t", "streamable-http", "-allowed-hosts", "${CURIE_ALLOWED_HOSTS}"],
)


def _dep(agent: str = "acme-dev", spec: ConnectorSpec = HOSTED_WITH_HOSTS) -> dict:
    return next(
        o
        for o in r.render("acme-bot", agent, "acme-bot", "acme-bot", "grafana", spec, "s")
        if o["kind"] == "Deployment"
    )


def test_allowed_hosts_expands_to_every_name_the_sandbox_could_dial() -> None:
    # Servers that guard against DNS rebinding default their allowlist to
    # loopback, so without this the connector starts and answers every in-cluster
    # call with `forbidden: host not allowed` -- healthy in `kubectl get pods`,
    # working for nobody.
    args = _dep()["spec"]["template"]["spec"]["containers"][0]["args"]
    value = args[args.index("-allowed-hosts") + 1]
    assert "${" not in value, "placeholder reached the container unsubstituted"
    for alias in r.host_aliases("acme-bot", "acme-dev", "grafana", "acme-bot", 8000):
        assert alias in value


def test_each_agent_gets_its_own_allowlist() -> None:
    # Since #1116 the Service name is agent-scoped, so one hardcoded allowlist
    # cannot serve two agents built from the same bundle.
    def hosts(agent: str) -> str:
        a = _dep(agent)["spec"]["template"]["spec"]["containers"][0]["args"]
        return a[a.index("-allowed-hosts") + 1]

    assert hosts("acme-dev") != hosts("acme-prod")


def test_placeholders_expand_in_env_too() -> None:
    spec = ConnectorSpec(image="x:1", env={"SELF_URL": "${CURIE_CONNECTOR_URL}"})
    env = _dep(spec=spec)["spec"]["template"]["spec"]["containers"][0]["env"]
    entry = next(e for e in env if e["name"] == "SELF_URL")
    assert entry["value"].startswith("http://acme-bot-acme-dev-mcp-grafana.acme-bot")


def test_text_without_placeholders_is_untouched() -> None:
    spec = ConnectorSpec(image="x:1", args=["-t", "streamable-http"], env={"A": "b"})
    c = _dep(spec=spec)["spec"]["template"]["spec"]["containers"][0]
    assert c["args"] == ["-t", "streamable-http"]
    assert {"name": "A", "value": "b"} in c["env"]


# --------------------------------------------------------------------------- #
# What an unhostable tier mounts -- #1160
# --------------------------------------------------------------------------- #
def test_a_hosted_connector_with_a_fallback_is_reachable_where_it_cannot_be_hosted() -> None:
    spec = ConnectorSpec(image="x:1", unhosted_url="http://host.docker.internal:8765/mcp")
    assert r.unhosted_mcp_entry(spec) == {
        "type": "http",
        "url": "http://host.docker.internal:8765/mcp",
    }


def test_a_hosted_connector_with_no_fallback_mounts_nothing_rather_than_a_dead_url() -> None:
    # None is a real answer: "declared but not exercisable here" (#1093). A URL
    # that resolves nowhere would turn that into a connection refused mid-turn.
    assert r.unhosted_mcp_entry(ConnectorSpec(image="x:1")) is None


def test_a_remote_connector_needs_no_fallback_to_stay_reachable() -> None:
    entry = r.unhosted_mcp_entry(REMOTE)
    assert entry is not None
    assert entry["url"] == "https://mcp.internal/mcp"


def test_the_fallback_never_displaces_the_derived_url_where_curie_hosts() -> None:
    # The whole point is that `cluster` keeps hosting it. A fallback that won
    # everywhere would silently repoint a production agent at someone's laptop.
    spec = ConnectorSpec(image="x:1", unhosted_url="http://host.docker.internal:8765/mcp")
    hosted = r.mcp_entry("curie", "acme-dev", "curie", "grafana", spec)
    assert "svc.cluster.local" in hosted["url"]
    assert "8765" not in hosted["url"]


# --------------------------------------------------------------------------- #
# A referenced Secret renders the same shape as an owned one -- #1163
# --------------------------------------------------------------------------- #
def test_a_referenced_secret_points_at_the_secret_the_author_named() -> None:
    from plugin_format.connectors import SecretRef

    spec = ConnectorSpec(image="x:1", secrets=[SecretRef(name="TOKEN", from_secret="grafana-mcp")])
    dep = next(
        o
        for o in r.render("rel", "ag", "ns", "app", "g", spec, "curie-owned")
        if o["kind"] == "Deployment"
    )
    entry = next(
        e for e in dep["spec"]["template"]["spec"]["containers"][0]["env"] if e["name"] == "TOKEN"
    )
    ref = entry["valueFrom"]["secretKeyRef"]
    assert ref["name"] == "grafana-mcp", "must point at the out-of-band Secret, not Curie's"
    assert ref["key"] == "TOKEN"
    assert "value" not in entry


def test_owned_and_referenced_secrets_are_indistinguishable_to_the_container() -> None:
    # Both render a secretKeyRef and never a literal. The container cannot tell
    # which is which, so nothing downstream needs to care.
    from plugin_format.connectors import SecretRef

    spec = ConnectorSpec(image="x:1", secrets=["OWNED", SecretRef(name="REFD", from_secret="ext")])
    dep = next(
        o
        for o in r.render("rel", "ag", "ns", "app", "g", spec, "curie-owned")
        if o["kind"] == "Deployment"
    )
    env = {e["name"]: e for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["OWNED"]["valueFrom"]["secretKeyRef"]["name"] == "curie-owned"
    assert env["REFD"]["valueFrom"]["secretKeyRef"]["name"] == "ext"
    for name in ("OWNED", "REFD"):
        assert "value" not in env[name], f"{name} must never be inlined"


def test_a_referenced_secret_is_not_optional() -> None:
    # A missing referenced Secret must stop the pod, not start it credential-less
    # and 401 on every call -- which reads as "the tool is broken".
    from plugin_format.connectors import SecretRef

    spec = ConnectorSpec(image="x:1", secrets=[SecretRef(name="T", from_secret="ext")])
    dep = next(
        o for o in r.render("rel", "ag", "ns", "app", "g", spec, "s") if o["kind"] == "Deployment"
    )
    entry = next(
        e for e in dep["spec"]["template"]["spec"]["containers"][0]["env"] if e["name"] == "T"
    )
    assert entry["valueFrom"]["secretKeyRef"]["optional"] is False

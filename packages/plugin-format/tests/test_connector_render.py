"""Deriving Kubernetes objects from a declared connector (ADR-0086, #1063).

The value of deriving rather than documenting is that specific defects become
unrepresentable. These tests pin the two that were actually hit by hand, so a
refactor cannot quietly reintroduce them.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml
from plugin_format import connector_render as r
from plugin_format.connectors import ConnectorSpec, SecretRef, validate_connectors

HOSTED = ConnectorSpec(
    image="grafana/mcp-grafana:0.17.2",
    args=["-t", "streamable-http", "-disable-write"],
    env={"GRAFANA_URL": "https://g.example.com"},
    secrets=["GRAFANA_TOKEN"],
)
REMOTE = ConnectorSpec(url="https://mcp.internal/mcp", headers={"Authorization": "Bearer ${T}"})


def _objs(
    release: str = "acme-bot",
    app: str = "acme-bot",
    spec: ConnectorSpec = HOSTED,
) -> list[dict]:
    return r.render(
        release=release,
        agent="acme-bot",
        namespace="acme-bot",
        app_name=app,
        connector="grafana",
        spec=spec,
        secret_name="conn-secrets",
    )


# Two NetworkPolicies ship per connector now, so selecting "the NetworkPolicy"
# by kind picks whichever happens to be first and silently tests the wrong
# object. Select by direction.
def _egress_np(objs: list) -> dict:
    return next(
        o for o in objs if o["kind"] == "NetworkPolicy" and o["spec"]["policyTypes"] == ["Egress"]
    )


def _ingress_np(objs: list) -> dict:
    return next(
        o for o in objs if o["kind"] == "NetworkPolicy" and o["spec"]["policyTypes"] == ["Ingress"]
    )


# --------------------------------------------------------------------------- #
# The ClusterIP trap -- the defect this renderer exists to prevent
# --------------------------------------------------------------------------- #
def test_egress_rule_uses_a_podselector_never_an_ipblock() -> None:
    # A NetworkPolicy naming a Service ClusterIP can NEVER match: kube-proxy
    # DNATs the destination to a pod IP before the policy is evaluated. The
    # symptom is a bare connection refused, and on a CNI that ignores
    # NetworkPolicy (minikube's default) the broken rule looks identical to a
    # correct one -- so it survives local testing and fails in a real cluster.
    np = _egress_np(_objs())
    to = np["spec"]["egress"][0]["to"][0]
    assert "podSelector" in to
    assert "ipBlock" not in to


def test_egress_selects_exactly_the_pods_rail_1_denies() -> None:
    # Too narrow and the allow widens nothing (NetworkPolicy is additive, it
    # cannot narrow -- ADR-0067) so the sandbox still cannot reach the
    # connector. Too broad -- e.g. only `component` -- and it also grants egress
    # to every OTHER release's sandboxes in the namespace. Both fail silently.
    np = _egress_np(
        r.render(
            release="relA",
            agent="a",
            namespace="ns",
            app_name="acme-bot",
            connector="g",
            spec=HOSTED,
            secret_name="s",
        )
    )
    assert np["spec"]["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "acme-bot",
        "app.kubernetes.io/instance": "relA",
        "app.kubernetes.io/component": "runner-sandbox",
    }


def test_two_releases_do_not_select_each_others_sandboxes() -> None:
    a = _egress_np(
        r.render(
            release="relA",
            agent="a",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=HOSTED,
            secret_name="s",
        )
    )
    b = _egress_np(
        r.render(
            release="relB",
            agent="a",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=HOSTED,
            secret_name="s",
        )
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
    assert (
        r.render(
            release="acme-bot",
            agent="a",
            namespace="ns",
            app_name="app",
            connector="internal",
            spec=REMOTE,
            secret_name="s",
        )
        == []
    )


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
    # sre-prod both rendered `curie-mcp-grafana`, so deploying prod silently
    # repointed the DEV agent at the prod endpoint -- and, because the Secret was
    # release-scoped too, handed it the prod token. Nothing errored.
    dev = [
        o["metadata"]["name"]
        for o in r.render(
            release="curie",
            agent="acme-dev",
            namespace="ns",
            app_name="curie",
            connector="grafana",
            spec=DEV,
            secret_name="s",
        )
    ]
    prod = [
        o["metadata"]["name"]
        for o in r.render(
            release="curie",
            agent="sre-prod",
            namespace="ns",
            app_name="curie",
            connector="grafana",
            spec=PROD,
            secret_name="s",
        )
    ]
    assert not set(dev) & set(prod), f"agents share object names: {dev} vs {prod}"


def test_two_agents_do_not_share_pod_labels() -> None:
    # The Service selector is these labels. Sharing them would route one agent's
    # traffic to the other's pods even with distinct object names.
    dev = next(
        o
        for o in r.render(
            release="curie",
            agent="acme-dev",
            namespace="ns",
            app_name="curie",
            connector="grafana",
            spec=DEV,
            secret_name="s",
        )
        if o["kind"] == "Service"
    )
    prod = next(
        o
        for o in r.render(
            release="curie",
            agent="sre-prod",
            namespace="ns",
            app_name="curie",
            connector="grafana",
            spec=PROD,
            secret_name="s",
        )
        if o["kind"] == "Service"
    )
    assert dev["spec"]["selector"] != prod["spec"]["selector"]


def test_each_agent_gets_its_own_url() -> None:
    dev = r.mcp_entry("curie", "acme-dev", "ns", "grafana", DEV)["url"]
    prod = r.mcp_entry("curie", "sre-prod", "ns", "grafana", PROD)["url"]
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
# Forged join -- #1446
#
# #1116 made the object name agent-scoped by joining the two names with the
# literal `-mcp-`. That literal is a bare substring INSIDE a single DNS label,
# not a structural separator, so the join point is not recoverable from the
# rendered string: `curie-a-mcp-b-mcp-c` reads equally well as
# (agent=`a-mcp-b`, connector=`c`) and (agent=`a`, connector=`b-mcp-c`). Two
# different agents therefore render byte-identical Services, Deployments,
# NetworkPolicies AND `app.kubernetes.io/name` pod selectors.
#
# The connector is deliberately unauthenticated (ADR-0086: "the network is not
# one layer of the access control here, it is the whole of it"), so the object
# name is the ONLY thing binding a sandbox to a credential. A collision hands
# one agent another agent's production token with nothing logged and nothing
# failing. These tests pin the refusal.
# --------------------------------------------------------------------------- #


def _render_connector(agent: str, connector: str, release: str = "curie") -> list[dict]:
    """Render one connector's objects, holding everything but the names fixed.

    The one render call both the refusal tests and their controls go through.
    The namespace, app name, spec and secret name are incidental to #1446, and
    pinning them in a single place is what lets a refusal and the control it is
    paired with differ in nothing but the agent and connector names -- which is
    the entire claim those pairs make.
    """

    return r.render(
        release=release,
        agent=agent,
        namespace="ns",
        app_name="curie",
        connector=connector,
        spec=DEV,
        secret_name="s",
    )


def _rendered_names(release: str, agent: str, connector: str) -> dict[str, str]:
    """The four object names one connector renders, keyed by what they are.

    Used as the CONTROL for the refusal tests below. A test that only asserts
    `render()` raises passes vacuously if a future change stops rendering an
    object at all, so every refusal test is paired with a render of a
    non-forging name that proves all four kinds are still produced -- and that
    all four take their name from `object_name`, which is where the guard lives.
    """

    objs = _render_connector(agent, connector, release)
    # Select the policies by direction, never by kind: two NetworkPolicies ship
    # per connector, so `kind == "NetworkPolicy"` silently picks whichever is
    # first and tests the wrong object.
    return {
        "Service": next(o for o in objs if o["kind"] == "Service")["metadata"]["name"],
        "Deployment": next(o for o in objs if o["kind"] == "Deployment")["metadata"]["name"],
        "egress": _egress_np(objs)["metadata"]["name"],
        "ingress": _ingress_np(objs)["metadata"]["name"],
    }


def test_the_issue_pair_cannot_render_the_same_objects() -> None:
    # The exact pair from #1446. Both tuples build the identical base string
    # `curie-a-mcp-b-mcp-c`, so before this guard the DEV agent `a` declaring a
    # connector `b-mcp-c` and the PROD agent `a-mcp-b` declaring a connector `c`
    # rendered the same Service, the same Deployment, both the same
    # NetworkPolicies and the same pod selector. Whichever deployed last owned
    # every object, and the other agent's sandbox reached a connector holding a
    # credential that was never issued to it. Nothing errored -- that silence is
    # the whole defect, and it is why the refusal is fail-closed rather than a
    # warning.
    assert f"curie-{'a-mcp-b'}-mcp-{'c'}" == f"curie-{'a'}-mcp-{'b-mcp-c'}", (
        "if this ever stops holding the pair below is no longer the issue's pair"
    )

    with pytest.raises(r.AmbiguousObjectName) as first:
        _render_connector(agent="a-mcp-b", connector="c")
    with pytest.raises(r.AmbiguousObjectName) as second:
        _render_connector(agent="a", connector="b-mcp-c")

    assert "a-mcp-b" in str(first.value), "the error must name the offending agent"
    assert "b-mcp-c" in str(second.value), "the error must name the offending connector"


def test_every_rendered_object_kind_is_refused() -> None:
    # AC3: the check covers all FOUR rendered objects, not just the Deployment.
    # The guard sits in `object_name`, so `render()` refuses before it produces
    # any of them -- but "raises" alone would still pass if `render` quietly
    # stopped emitting the ingress policy #1443 added. The control below pins
    # the full set and its names, so dropping an object fails here too.
    with pytest.raises(r.AmbiguousObjectName):
        _render_connector(agent="a-mcp-b", connector="c")

    control = _rendered_names("curie", "acme-dev", "grafana")
    assert control == {
        "Service": "curie-acme-dev-mcp-grafana",
        "Deployment": "curie-acme-dev-mcp-grafana",
        "egress": "curie-acme-dev-mcp-grafana-allow",
        "ingress": "curie-acme-dev-mcp-grafana-allow-ingress",
    }


def test_a_substring_ban_would_miss_this_pair() -> None:
    # This is the test that separates the shipped rule from the fix the issue
    # itself suggested. `-mcp-` is NOT a substring of either offending name:
    #
    #     "-mcp-" in "x-mcp"  ->  False
    #     "-mcp-" in "mcp-c"  ->  False
    #
    # yet agent `x-mcp` + connector `c` and agent `x` + connector `mcp-c` both
    # render `curie-x-mcp-mcp-c`. A ban on the literal substring therefore
    # leaves this collision fully live while looking like a fix. The rule has to
    # be "would this name FORGE a second join once the concatenation happens",
    # which is `-mcp-` in f"{agent}-" on the left and in f"-{connector}" on the
    # right -- each side tested against the half of the delimiter it abuts.
    assert "-mcp-" not in "x-mcp"
    assert "-mcp-" not in "mcp-c"
    assert f"curie-{'x-mcp'}-mcp-{'c'}" == f"curie-{'x'}-mcp-{'mcp-c'}"

    with pytest.raises(r.AmbiguousObjectName):
        _render_connector(agent="x-mcp", connector="c")
    with pytest.raises(r.AmbiguousObjectName):
        _render_connector(agent="x", connector="mcp-c")


def test_a_forging_pair_is_refused_even_past_the_digest_boundary() -> None:
    # The truncate-with-digest branch does NOT close this hole on its own, and
    # believing it does is the easy wrong conclusion: it looks like a
    # disambiguator because it exists to stop two long names clipping together.
    # It cannot help here. The digest is taken over `base`, and a forging pair
    # produces the SAME `base` from both tuples -- so it produces the same
    # sha256, the same truncation, and reproduces the collision byte for byte.
    # The guard has to run BEFORE the length check; this test fails if someone
    # moves it after.
    long_agent = "a" * 25 + "-mcp-" + "b" * 25
    short_connector = "c" * 10
    short_agent = "a" * 25
    long_connector = "b" * 25 + "-mcp-" + "c" * 10

    left = f"curie-{long_agent}-mcp-{short_connector}"
    right = f"curie-{short_agent}-mcp-{long_connector}"
    assert left == right, "the two tuples must build one base for this test to mean anything"
    assert len(left) > 63, "this pair must reach the truncate-with-digest branch"

    with pytest.raises(r.AmbiguousObjectName):
        _render_connector(agent=long_agent, connector=short_connector)
    with pytest.raises(r.AmbiguousObjectName):
        _render_connector(agent=short_agent, connector=long_connector)


_FORGING_AGENT = "a-mcp-b"

# Every accessor that derives anything from the object name. They all funnel
# through `object_name` today, which is why one guard is enough -- but that is a
# property of the current code, not a guarantee. If a refactor gives any of
# these its own copy of the `-mcp-` concatenation, the guard stops covering it
# and the collision comes back through that one path alone, silently. This list
# is the pin, and it includes `_labels` deliberately: those labels ARE the
# Service selector, the Deployment matchLabels and both policies' podSelectors,
# so a collision there cross-wires traffic even when the object names differ.
_DERIVATIONS: list[tuple[str, Callable[[], object]]] = [
    ("object_name", lambda: r.object_name("curie", _FORGING_AGENT, "c")),
    ("service_dns", lambda: r.service_dns("curie", _FORGING_AGENT, "c", "ns")),
    ("host_aliases", lambda: r.host_aliases("curie", _FORGING_AGENT, "c", "ns", 8000)),
    ("substitutions", lambda: r.substitutions("curie", _FORGING_AGENT, "c", "ns", 8000)),
    ("mcp_entry", lambda: r.mcp_entry("curie", _FORGING_AGENT, "ns", "c", DEV)),
    ("_labels", lambda: r._labels("curie", _FORGING_AGENT, "c")),
    ("render_service", lambda: r.render_service("curie", _FORGING_AGENT, "c", DEV)),
    (
        "render_deployment",
        lambda: r.render_deployment("curie", _FORGING_AGENT, "ns", "c", DEV, "s"),
    ),
    (
        "render_networkpolicy",
        lambda: r.render_networkpolicy("curie", _FORGING_AGENT, "curie", "c", DEV),
    ),
    (
        "render_ingress_networkpolicy",
        lambda: r.render_ingress_networkpolicy("curie", _FORGING_AGENT, "curie", "c", DEV),
    ),
    ("render", lambda: _render_connector(agent=_FORGING_AGENT, connector="c")),
]


@pytest.mark.parametrize("call", [pytest.param(fn, id=name) for name, fn in _DERIVATIONS])
def test_the_refusal_reaches_every_derivation(call: Callable[[], object]) -> None:
    # A guard that only covers `render()` leaves `mcp_entry` handing the sandbox
    # a URL for the OTHER agent's connector, and `substitutions` baking that
    # same host into CURIE_ALLOWED_HOSTS -- both without rendering a single
    # object. Every derivation has to refuse, or the credential still leaks
    # through the path that skipped the renderer.
    with pytest.raises(r.AmbiguousObjectName):
        call()


def test_the_error_names_the_offending_side() -> None:
    # An operator hits this on an install that worked yesterday (an agent named
    # `grafana-mcp` was legal before this fix). "invalid name" would send them
    # into the source; the message has to say WHICH of the two names offends and
    # WHAT ITS VALUE IS, because the agent comes from deploy.yaml and the
    # connector from connectors.yaml -- two different files to go edit.
    with pytest.raises(r.AmbiguousObjectName) as agent_side:
        r.object_name("curie", "aa-mcp-bb", "grafana")
    assert "aa-mcp-bb" in str(agent_side.value)

    with pytest.raises(r.AmbiguousObjectName) as connector_side:
        r.object_name("curie", "acme", "mcp-zzqq")
    assert "mcp-zzqq" in str(connector_side.value)

    # Both sides offending reports the AGENT, deterministically -- the guard
    # checks the agent first. Pinned so the message is reproducible rather than
    # incidentally ordered.
    with pytest.raises(r.AmbiguousObjectName) as both:
        r.object_name("curie", "aa-mcp-bb", "mcp-zzqq")
    assert "aa-mcp-bb" in str(both.value)
    assert "mcp-zzqq" not in str(both.value)


# The asymmetry below looks like a bug until the derivation is done, so it is
# stated here once rather than in each case:
#
#   agent     is followed by the join  ->  refused iff `-mcp-` in f"{agent}-"
#   connector is preceded by the join  ->  refused iff `-mcp-` in f"-{connector}"
#
# So a TRAILING `-mcp` is fatal on the agent side (`grafana-mcp` + `-mcp-c...`
# completes a second join) but harmless on the connector side (nothing follows
# the connector, so `c-mcp` cannot complete anything). A LEADING `mcp-` is the
# mirror: fatal on the connector side, harmless on the agent side (an
# alternative split of `curie-mcp-x-mcp-c` would leave an EMPTY agent, which is
# not a name any bundle can declare). Each side sits against a different half of
# the delimiter, so a symmetric rule would be wrong in both directions -- it
# would over-reject `c-mcp` and `mcp-x`, breaking installs for no security gain.
_REFUSED_AGENTS = ["a-mcp-b", "grafana-mcp", "x-mcp"]
_ALLOWED_AGENTS = ["mcp-x", "mcp", "acme-dev", "kubernetes"]
_REFUSED_CONNECTORS = ["b-mcp-c", "mcp-c"]
_ALLOWED_CONNECTORS = ["c-mcp", "grafana-mcp", "mcp", "grafana", "kubernetes", "netpol-probe"]


@pytest.mark.parametrize("agent", _REFUSED_AGENTS)
def test_agent_forges_join_is_true_for_a_name_that_completes_the_delimiter(agent: str) -> None:
    assert r.agent_forges_join(agent) is True


@pytest.mark.parametrize("agent", _ALLOWED_AGENTS)
def test_agent_forges_join_is_false_for_a_name_that_only_looks_like_it(agent: str) -> None:
    # Over-rejection is not the safe direction here. Every name refused is an
    # install that must be renamed before its next deploy, so a rule that is
    # merely conservative breaks working agents for nothing.
    assert r.agent_forges_join(agent) is False


@pytest.mark.parametrize("connector", _REFUSED_CONNECTORS)
def test_connector_forges_join_is_true_for_a_name_that_completes_the_delimiter(
    connector: str,
) -> None:
    assert r.connector_forges_join(connector) is True


@pytest.mark.parametrize("connector", _ALLOWED_CONNECTORS)
def test_connector_forges_join_is_false_for_a_name_that_only_looks_like_it(
    connector: str,
) -> None:
    # `kubernetes` and `netpol-probe` are the connector names actually shipped
    # in examples/, and `grafana-mcp` is the shape an author reaches for first.
    # If any of these start being refused, this fix has broken the repo's own
    # bundles.
    assert r.connector_forges_join(connector) is False


@pytest.mark.parametrize(
    ("release", "agent", "connector", "expected"),
    [
        ("curie", "acme-dev", "grafana-mcp", "curie-acme-dev-mcp-grafana-mcp"),
        ("curie", "acme-dev", "c-mcp", "curie-acme-dev-mcp-c-mcp"),
        ("curie", "acme-dev", "mcp", "curie-acme-dev-mcp-mcp"),
        ("curie", "mcp-x", "grafana", "curie-mcp-x-mcp-grafana"),
        ("curie", "mcp", "grafana", "curie-mcp-mcp-grafana"),
        ("grafana-mcp", "acme", "grafana", "grafana-mcp-acme-mcp-grafana"),
    ],
    ids=[
        "connector_grafana_mcp",
        "connector_c_mcp",
        "connector_mcp",
        "agent_mcp_x",
        "agent_mcp",
        "release_grafana_mcp",
    ],
)
def test_names_that_only_look_like_the_join_still_render(
    release: str, agent: str, connector: str, expected: str
) -> None:
    # The over-rejection guard, and the test that fails if someone "simplifies"
    # the rule to a substring ban or to `base.count("-mcp-") == 1`. Each of
    # these renders an UNAMBIGUOUS name: no other valid (agent, connector) split
    # of the result exists, because the alternative split either leaves one side
    # empty or fails to preserve the release prefix.
    assert r.object_name(release, agent, connector) == expected
    names = _rendered_names(release, agent, connector)
    assert names == {
        "Service": expected,
        "Deployment": expected,
        "egress": f"{expected}-allow",
        "ingress": f"{expected}-allow-ingress",
    }


def test_the_release_is_deliberately_not_guarded() -> None:
    # The release side is NOT checked, and that is a decision rather than an
    # oversight. A `-mcp-` inside the release cannot create an
    # (agent, connector) ambiguity: any alternative split of
    # `grafana-mcp-acme-mcp-grafana` either leaves an empty agent or stops
    # preserving the release prefix. Guarding the whole rendered base -- the
    # obvious-looking `base.count("-mcp-") == 1` -- would refuse every deploy of
    # a release literally named `grafana-mcp`, which is a real break of working
    # installs in exchange for no security gain at all.
    assert r.object_name("grafana-mcp", "acme", "grafana") == "grafana-mcp-acme-mcp-grafana"
    assert r.agent_forges_join("acme") is False
    assert r.connector_forges_join("grafana") is False


def _name_as_it_rendered_before_1446(release: str, agent: str, connector: str) -> str:
    """The pre-#1446 derivation, restated so the pin is independent of the code.

    Deliberately a second copy of the formula: a pin that called `object_name`
    to compute its own expectation could not detect a rename at all.
    """

    base = f"{release}-{agent}-mcp-{connector}"
    if len(base) <= 63:
        return base
    digest = hashlib.sha256(base.encode()).hexdigest()[:8]
    return f"{base[: 63 - 8 - 1].rstrip('-')}-{digest}"


@pytest.mark.parametrize(
    ("release", "agent", "connector"),
    [
        ("curie", "acme-dev", "grafana"),
        ("curie", "acme-bot", "kubernetes"),
        ("curie", "sre-prod", "netpol-probe"),
        ("acme-bot", "driftcheck", "grafana"),
        # Over the 63-character ceiling, so this one pins the
        # truncate-with-digest branch as well as the plain concatenation.
        ("release-with-a-long-name", "agent-with-a-very-long-name-number-one", "grafana"),
    ],
)
def test_allowed_names_render_exactly_the_name_they_render_today(
    release: str, agent: str, connector: str
) -> None:
    # #1116's contract has two halves and this fix may only touch one. Names
    # must be DISTINCT per (release, agent, connector) -- that is what the guard
    # above buys -- and they must stay "stable and derivable", which means every
    # name that deploys today must render byte-identically tomorrow. A rename
    # would not fail loudly: it would leave every live Service, Deployment and
    # NetworkPolicy orphaned under a name nothing reconciles any more, still
    # running, still holding its credential, while a fresh set comes up beside
    # it. This test is the churn pin.
    assert r.object_name(release, agent, connector) == _name_as_it_rendered_before_1446(
        release, agent, connector
    )


def test_an_unhosted_connector_still_derives_nothing() -> None:
    # `unhosted_mcp_entry` calls `mcp_entry("", "", "", "", spec)` with four
    # empty strings for a remote connector. Empty names must not trip the new
    # guard -- for two independent reasons: the spec is not hosted so
    # `service_dns` is never reached, and `-mcp-` is not in `"-"` on either side
    # anyway. If this starts raising, every remote connector in every bundle
    # stops resolving.
    entry = r.unhosted_mcp_entry(REMOTE)
    assert entry is not None
    assert entry["url"] == "https://mcp.internal/mcp"


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
        for o in r.render(
            release="acme-bot",
            agent=agent,
            namespace="acme-bot",
            app_name="acme-bot",
            connector="grafana",
            spec=spec,
            secret_name="s",
        )
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

    assert hosts("acme-dev") != hosts("sre-prod")


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
        for o in r.render(
            release="rel",
            agent="ag",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=spec,
            secret_name="curie-owned",
        )
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
        for o in r.render(
            release="rel",
            agent="ag",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=spec,
            secret_name="curie-owned",
        )
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
        o
        for o in r.render(
            release="rel",
            agent="ag",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=spec,
            secret_name="s",
        )
        if o["kind"] == "Deployment"
    )
    entry = next(
        e for e in dep["spec"]["template"]["spec"]["containers"][0]["env"] if e["name"] == "T"
    )
    assert entry["valueFrom"]["secretKeyRef"]["optional"] is False


# -- sealed_secrets: the ADR-0094 contract slice ------------------------------


def test_sealed_secrets_is_optional_and_absent_by_default() -> None:
    """Additive: an existing bundle must be unaffected by the new field."""

    spec = ConnectorSpec(image="grafana/mcp-grafana:0.17.2")
    assert spec.sealed_secrets == {}
    assert spec.secret_names() == []


def test_sealed_secret_names_join_the_other_forms() -> None:
    """All three holders answer the same question, so one list answers it."""

    spec = ConnectorSpec(
        image="x",
        secrets=["PLAIN"],
        sealed_secrets={"SEALED": "AgBv3n2K"},
    )
    assert spec.secret_names() == ["PLAIN", "SEALED"]
    # Only the plain one is Curie's to resolve a value for.
    assert spec.resolved_secrets() == ["PLAIN"]


def test_a_declared_sealed_secret_is_refused_until_decryption_exists() -> None:
    parsed, errors = validate_connectors(
        {"connectors": {"grafana": {"image": "x", "sealed_secrets": {"TOKEN": "AgB"}}}}
    )
    assert "connectors.sealed_secrets_unsupported" in [code for code, _ in errors]
    assert parsed is None
    diagnostic = next(
        message for code, message in errors if code == "connectors.sealed_secrets_unsupported"
    )
    assert "secrets:" in diagnostic


def test_an_empty_sealed_blob_is_reported_on_its_own() -> None:
    """A distinct diagnostic, so the eventual decrypt path inherits the check."""

    _, errors = validate_connectors(
        {"connectors": {"grafana": {"image": "x", "sealed_secrets": {"TOKEN": "  "}}}}
    )
    assert "connectors.empty_sealed_value" in [code for code, _ in errors]


# --- secret_files (#1402) ---------------------------------------------------
#
# `secrets:` renders a secretKeyRef and nothing else, so a server that
# authenticates from a FILE -- a kubeconfig, a TLS client cert, GCP
# service-account JSON -- had no way to receive its credential at all. These
# pin the projection, and the guards that keep it from becoming a second, worse
# way to leak one.

FILE_SPEC = ConnectorSpec(
    image="ghcr.io/containers/kubernetes-mcp-server:latest",
    args=["--kubeconfig", "/secrets/kubeconfig", "--read-only"],
    secret_files={"K8S_READONLY_KUBECONFIG": "/secrets/kubeconfig"},
)


def _file_dep(spec: ConnectorSpec) -> dict:
    return r.render_deployment("acme-bot", "acme-bot", "acme-bot", "k8s", spec, "conn-secrets")


def test_secret_file_lands_at_the_declared_path_not_a_directory() -> None:
    # subPath is what makes the mount a FILE at exactly the declared path.
    # Without it Kubernetes mounts a DIRECTORY there, and `--kubeconfig
    # /secrets/kubeconfig` would open a directory and fail in a way that reads
    # like a malformed credential.
    mount = _file_dep(FILE_SPEC)["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][0]
    assert mount["mountPath"] == "/secrets/kubeconfig"
    assert mount["subPath"] == "kubeconfig"
    assert mount["readOnly"] is True


def test_secret_file_reads_the_same_per_agent_secret_as_env_secrets() -> None:
    # Same store, same Secret, same resolution -- only the delivery differs.
    vol = _file_dep(FILE_SPEC)["spec"]["template"]["spec"]["volumes"][0]["secret"]
    assert vol["secretName"] == "conn-secrets"
    assert vol["items"] == [{"key": "K8S_READONLY_KUBECONFIG", "path": "kubeconfig"}]


def test_secret_file_is_readable_by_the_nonroot_user_that_must_open_it() -> None:
    # Regression: this shipped as 0400 and the server died with
    #   open /secrets/kubeconfig: permission denied
    # on a file that was mounted correctly. Secret volume files are owned by
    # root:fsGroup, NOT by runAsUser, so owner-only is unreadable by the 65532
    # the container runs as. 0440 + a matching fsGroup is the narrowest pair
    # that actually opens.
    pod = _file_dep(FILE_SPEC)["spec"]["template"]["spec"]
    assert pod["volumes"][0]["secret"]["defaultMode"] == 0o440
    assert pod["securityContext"]["fsGroup"] == pod["securityContext"]["runAsUser"]


def test_secret_file_is_not_optional() -> None:
    # Matches `secrets:`: a missing key must stop the pod rather than start a
    # server that 401s on every call.
    vol = _file_dep(FILE_SPEC)["spec"]["template"]["spec"]["volumes"][0]["secret"]
    assert vol["optional"] is False


def test_fsgroup_is_absent_when_nothing_is_projected() -> None:
    # fsGroup applies to EVERY volume in the pod, so it is set only when a
    # secret is actually projected as a file.
    assert "fsGroup" not in _file_dep(HOSTED)["spec"]["template"]["spec"]["securityContext"]


def test_each_secret_file_gets_its_own_volume() -> None:
    # One volume per file, so each carries only its own key. A single volume
    # with several items would put every credential in one directory, where a
    # server that reads a directory would see the others.
    spec = ConnectorSpec(
        image="x:1",
        secret_files={"A": "/secrets/a.pem", "B": "/secrets/b.json"},
    )
    pod = _file_dep(spec)["spec"]["template"]["spec"]
    assert len(pod["volumes"]) == 2
    keys = sorted(v["secret"]["items"][0]["key"] for v in pod["volumes"])
    assert keys == ["A", "B"]
    for v in pod["volumes"]:
        assert len(v["secret"]["items"]) == 1


def test_a_connector_without_secret_files_is_unchanged() -> None:
    # Additive to a frozen contract: a bundle that does not use this must
    # render exactly what it rendered before, with no empty volumes key.
    pod = _file_dep(HOSTED)["spec"]["template"]["spec"]
    assert "volumes" not in pod
    assert "volumeMounts" not in pod["containers"][0]


def test_secret_files_are_reported_as_both_declared_and_resolved() -> None:
    # `secret_names()` feeds the duplicate-name validator; `resolved_secrets()`
    # is what the deploy path is told to WRITE into the per-agent Secret. The
    # second was missing, so the volume pointed at a key nobody wrote.
    assert "K8S_READONLY_KUBECONFIG" in FILE_SPEC.secret_names()
    assert "K8S_READONLY_KUBECONFIG" in FILE_SPEC.resolved_secrets()


def test_every_rendered_volume_key_is_one_resolved_secrets_claims() -> None:
    # Why this invariant matters, and what #1424 violated. A secret volume is
    # rendered `optional: false`, so a key the deploy path never resolved is
    # not a degraded connector: the kubelet cannot mount it and the pod never
    # starts, stuck on `FailedMount ... secret not found`. Pinned as the class
    # -- every projected key must be one `resolved_secrets()` claims -- so the
    # fix cannot be satisfied by handling only this one spec's shape.
    # Boundary: this pins the renderer against the accessor in process; the
    # deploy path itself is covered by the API-level test in
    # `apps/api/tests/test_bundle_connectors.py`.
    spec = ConnectorSpec(
        image="x:1",
        secrets=["ENV_TOKEN"],
        secret_files={"KUBECONFIG": "/secrets/kubeconfig", "CA_BUNDLE": "/secrets/ca.pem"},
    )
    resolved = spec.resolved_secrets()
    projected = [
        item["key"]
        for vol in _file_dep(spec)["spec"]["template"]["spec"]["volumes"]
        for item in vol["secret"]["items"]
    ]
    assert projected, "expected the spec to project at least one key"
    for key in projected:
        assert key in resolved, f"volume projects {key!r}, absent from resolved_secrets()"


@pytest.mark.parametrize(
    "data,code",
    [
        (
            {"image": "x:1", "secret_files": {"A": "secrets/x"}},
            "connectors.secret_file_relative_path",
        ),
        ({"image": "x:1", "secret_files": {"A": "/tmp/x"}}, "connectors.secret_file_in_tmp"),
        (
            {"image": "x:1", "secret_files": {"A": "/s/x", "B": "/s/x"}},
            "connectors.secret_file_path_collision",
        ),
        (
            {"image": "x:1", "secrets": ["A"], "secret_files": {"A": "/s/x"}},
            "connectors.secret_both_env_and_file",
        ),
        (
            {"url": "https://m/mcp", "secret_files": {"A": "/s/x"}},
            "connectors.remote_has_secret_files",
        ),
    ],
)
def test_secret_file_misuse_is_refused(data: dict, code: str) -> None:
    _, errors = validate_connectors({"connectors": {"k": data}})
    assert code in [c for c, _ in errors]


def test_a_well_formed_secret_file_validates() -> None:
    parsed, errors = validate_connectors(
        {"connectors": {"k": {"image": "x:1", "secret_files": {"A": "/secrets/kubeconfig"}}}}
    )
    assert errors == []
    assert parsed is not None


# --------------------------------------------------------------------------- #
# Ingress: who may REACH the connector
# --------------------------------------------------------------------------- #
def test_connector_accepts_traffic_only_from_the_sandbox() -> None:
    # The egress policy governs where the sandbox may go; it places no limit on
    # who may arrive. Without an ingress rule every pod in the namespace can
    # call a connector that holds a production credential and authenticates
    # nobody -- because the sandbox has no credential to authenticate WITH, so
    # the network is the entire access control.
    np = _ingress_np(
        r.render(
            release="release-r",
            agent="agent-a",
            namespace="namespace-n",
            app_name="app-name",
            connector="connector-c",
            spec=HOSTED,
            secret_name="connector-secret",
        )
    )
    src = np["spec"]["ingress"][0]["from"]
    assert len(src) == 1, "exactly one source: the sandbox"
    assert src[0]["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "app-name",
        "app.kubernetes.io/instance": "release-r",
        "app.kubernetes.io/component": "runner-sandbox",
    }
    assert set(src[0]) == {"podSelector"}
    assert src[0]["podSelector"]["matchLabels"] == r.sandbox_selector("release-r", "app-name")


# This sits BESIDE the test above, which already goes red on the same mutation
# via its `set(src[0]) == {"podSelector"}` line. That coverage is incidental:
# it lives inside a test named for pod-scope narrowness, and its failure points
# a reader at label identity, not at namespace scope. A future edit could
# reasonably relax or split it without any signal that one line of it was the
# only thing standing between the repo and the #1502 widening. This test's
# whole stated purpose IS the namespace axis, so it cannot be relaxed by
# accident. Keep both; they are complements, not duplicates (#1450, #1502).
def test_ingress_source_peer_is_namespace_scoped_by_omission() -> None:
    """The ingress `from` peer must stay a BARE podSelector -- no namespaceSelector.

    The widening this exists to catch is `namespaceSelector: {}` merged INTO
    the existing peer, rather than added as a second peer:

        from:
          - podSelector: {matchLabels: {...sandbox labels...}}
            namespaceSelector: {}            # <-- the mutation

    A bare podSelector peer is implicitly scoped to the policy's own namespace,
    so this merge relaxes the NAMESPACE axis while leaving the pod axis exactly
    as narrow as it was: every sandbox-labelled pod in EVERY namespace is then
    admitted to a connector that holds a production credential and
    authenticates nobody.

    The cluster gate cannot see it. `scripts/check-netpol-enforcement.sh`'s
    deny prober `netpol-probe-outside` is unlabelled and lives in the
    connectors' own namespace, so it differs from a sandbox on the POD axis
    only -- the merged peer still denies it and the gate stays green while the
    boundary is gone. That script's `netpol-probe-foreign` (sandbox labels,
    different namespace) is the cluster-side complement; this test needs no
    cluster at all.
    """
    src = _ingress_np(_objs(release="release-r", app="app-name"))["spec"]["ingress"][0]["from"]
    assert len(src) == 1, (
        "exactly one source peer; a second peer widens the source set as surely "
        "as widening this one does"
    )
    assert set(src[0]) == {"podSelector"}, (
        "the peer must carry podSelector and NOTHING else: a bare podSelector peer is "
        "namespace-local, and any sibling key here widens the source beyond this namespace"
    )
    assert "namespaceSelector" not in src[0], (
        "a bare podSelector peer is namespace-local; a namespaceSelector here would admit "
        "sandbox-labelled pods in EVERY namespace, which the unlabelled same-namespace "
        "cluster probe cannot observe (#1502)"
    )
    assert src[0]["podSelector"]["matchLabels"] == r.sandbox_selector("release-r", "app-name"), (
        "the peer must still name exactly this release's sandbox on the pod axis; "
        "the namespace axis is held closed by omission, not by these labels"
    )


def test_ingress_policy_selects_the_connector_not_the_sandbox() -> None:
    # Getting this backwards yields a policy that parses, applies, and protects
    # the wrong pod -- the sandbox gains an ingress restriction it does not need
    # while the connector keeps none.
    objs = r.render(
        release="release-r",
        agent="agent-a",
        namespace="namespace-n",
        app_name="app-name",
        connector="connector-c",
        spec=HOSTED,
        secret_name="connector-secret",
    )
    ing = _ingress_np(objs)
    egr = _egress_np(objs)
    # The two policies must select OPPOSITE ends of the same hop: egress is
    # attached to the sandbox, ingress to the connector. Swapping them still
    # parses and still applies -- it just protects the wrong pod.
    assert ing["spec"]["podSelector"] != egr["spec"]["podSelector"]
    assert (
        ing["spec"]["podSelector"]["matchLabels"]
        == egr["spec"]["egress"][0]["to"][0]["podSelector"]["matchLabels"]
    ), "ingress must select the pod the egress rule points AT"
    assert (
        egr["spec"]["podSelector"]["matchLabels"]
        == ing["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]
    ), "ingress must admit the pod the egress rule is attached to"
    dep = next(o for o in objs if o["kind"] == "Deployment")
    assert ing["spec"]["podSelector"]["matchLabels"] == dep["spec"]["template"]["metadata"][
        "labels"
    ]


def test_ingress_is_port_scoped_to_the_connector_port() -> None:
    ports = _ingress_np(_objs())["spec"]["ingress"][0]["ports"]
    assert ports == [{"protocol": "TCP", "port": HOSTED.port}]


def test_ingress_uses_a_podselector_never_an_ipblock() -> None:
    # Same trap as the egress rule: a ClusterIP ipBlock can never match, and it
    # looks correct on a CNI that ignores NetworkPolicy.
    src = _ingress_np(_objs())["spec"]["ingress"][0]["from"][0]
    assert "podSelector" in src and "ipBlock" not in src


def test_two_releases_do_not_admit_each_others_sandboxes() -> None:
    # A too-broad source selector would let another release's sandbox read
    # production through this connector, silently.
    a = _ingress_np(
        r.render(
            release="relA",
            agent="a",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=HOSTED,
            secret_name="s",
        )
    )
    b = _ingress_np(
        r.render(
            release="relB",
            agent="a",
            namespace="ns",
            app_name="app",
            connector="g",
            spec=HOSTED,
            secret_name="s",
        )
    )
    assert a["spec"]["ingress"][0]["from"] != b["spec"]["ingress"][0]["from"]


def test_the_two_policies_do_not_collide_on_name() -> None:
    names = [o["metadata"]["name"] for o in _objs() if o["kind"] == "NetworkPolicy"]
    assert len(names) == len(set(names)) == 2


def test_a_remote_connector_renders_no_policies() -> None:
    # Nothing is hosted, so there is nothing in-cluster to admit traffic to.
    assert not [
        o
        for o in r.render(
            release="rel",
            agent="a",
            namespace="ns",
            app_name="app",
            connector="x",
            spec=REMOTE,
            secret_name="s",
        )
    ]


# --------------------------------------------------------------------------- #
# `spec.port` is read in six places, and every one of them has to read the
# DECLARED port. The default is 8000, so a reader that hardcodes it keeps
# working for every connector that never sets `port:` and breaks only for the
# ones that do -- silently, and with a different symptom per reader. These
# render at TWO different NON-default ports for exactly that reason: asserting
# at 8000 would pin nothing, and asserting at one other port would pin nothing
# against a reader that hardcodes THAT value instead.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("port", [9876, 9999])
def test_a_wrong_container_port_leaves_the_service_routing_to_a_refused_port(port: int) -> None:
    # The Service names its targetPort "http", so the container's port NAME is
    # what resolves it. A containerPort that is not the declared port makes
    # "http" resolve to a port nothing listens on, and every call is refused.
    spec = ConnectorSpec(image="grafana/mcp-grafana:0.17.2", port=port)
    container = next(o for o in _objs(spec=spec) if o["kind"] == "Deployment")["spec"]["template"][
        "spec"
    ]["containers"][0]
    assert container["ports"] == [{"name": "http", "containerPort": port}]


@pytest.mark.parametrize("port", [9876, 9999])
def test_a_wrong_egress_port_leaves_rail_1_denying_the_port_the_connector_listens_on(
    port: int,
) -> None:
    # Rail 1 is default-deny, so this rule is the only thing that opens the hop.
    # Opened on the wrong port it denies the real one, and the sandbox's call
    # times out with no policy error anywhere to say why.
    spec = ConnectorSpec(image="grafana/mcp-grafana:0.17.2", port=port)
    egress = _egress_np(_objs(spec=spec))["spec"]["egress"][0]
    assert egress["ports"] == [{"protocol": "TCP", "port": port}]


@pytest.mark.parametrize("port", [9876, 9999])
def test_a_wrong_url_port_makes_the_sandbox_dial_a_port_nothing_serves(port: int) -> None:
    # The author never writes this URL, so a hardcoded port here is not
    # correctable from the bundle: the sandbox dials the wrong port on the right
    # pod and the MCP server never answers.
    spec = ConnectorSpec(image="grafana/mcp-grafana:0.17.2", port=port)
    url = r.mcp_entry("acme-rel", "acme-bot", "acme-ns", "grafana", spec)["url"]
    assert url == f"http://acme-rel-acme-bot-mcp-grafana.acme-ns.svc.cluster.local:{port}/mcp"


@pytest.mark.parametrize("port", [9876, 9999])
def test_connector_env_port_placeholders_use_the_declared_port(port: int) -> None:
    spec = ConnectorSpec(
        image="grafana/mcp-grafana:0.17.2",
        port=port,
        env={
            "ALLOWED_HOSTS": "${CURIE_ALLOWED_HOSTS}",
            "CONNECTOR_PORT": "${CURIE_CONNECTOR_PORT}",
            "CONNECTOR_URL": "${CURIE_CONNECTOR_URL}",
        },
    )
    env = next(o for o in _objs(spec=spec) if o["kind"] == "Deployment")["spec"]["template"][
        "spec"
    ]["containers"][0]["env"]
    assert {entry["name"]: entry["value"] for entry in env} == {
        "ALLOWED_HOSTS": (
            f"acme-bot-acme-bot-mcp-grafana:{port},"
            f"acme-bot-acme-bot-mcp-grafana.acme-bot:{port},"
            f"acme-bot-acme-bot-mcp-grafana.acme-bot.svc.cluster.local:{port}"
        ),
        "CONNECTOR_PORT": str(port),
        "CONNECTOR_URL": (
            f"http://acme-bot-acme-bot-mcp-grafana.acme-bot.svc.cluster.local:{port}"
        ),
    }


# --------------------------------------------------------------------------- #
# An unresolved `build:` connector never renders -- ADR 0113
# --------------------------------------------------------------------------- #
def _build_spec(**overrides: object) -> ConnectorSpec:
    payload: dict = {
        "build": {
            "context": "connectors/k8s-write",
            "platforms": ["linux/amd64", "linux/arm64"],
        }
    }
    payload.update(overrides)
    return ConnectorSpec.model_validate(payload)


def test_rendering_an_unresolved_build_raises_instead_of_emitting_a_null_image() -> None:
    # The alternative is render_deployment emitting `"image": None`, which
    # applies as an invalid Deployment and surfaces as an opaque Kubernetes
    # error a long way from its cause. The message must name the artifact that
    # is missing, because the fix is to produce it, not to edit the bundle.
    with pytest.raises(ValueError) as exc:
        _objs(spec=_build_spec())
    assert "connectors.lock.yaml" in str(exc.value)


def test_the_guard_fires_before_any_object_is_produced() -> None:
    # A guard that ran per-object would emit a Service and a NetworkPolicy for a
    # connector that can never start, and a caller applying the partial list
    # leaves them orphaned in the namespace.
    with pytest.raises(ValueError):
        r.render(
            release="acme-rel",
            agent="acme-bot",
            namespace="acme-ns",
            app_name="curie",
            connector="k8s-write",
            spec=_build_spec(),
            secret_name="conn-secrets",
        )


def test_a_remote_connector_still_renders_nothing_rather_than_raising() -> None:
    # The guard is scoped to hosted-ness. A `url` connector has no image by
    # design and must keep returning an empty object list, not an error.
    assert _objs(spec=REMOTE) == []


# --------------------------------------------------------------------------- #
# Hosted-ness is image-independent: the derivations a build connector inherits
# --------------------------------------------------------------------------- #
def test_a_build_connector_with_no_unhosted_url_is_declared_but_not_exercisable() -> None:
    # Matching the `image:` form exactly. Reverting `is_hosted` to
    # `self.image is not None` makes this return the REMOTE-form entry built
    # from a `url` that is None, so the runner mounts an entry whose URL is
    # null instead of reporting the connector as unavailable here (#1093).
    assert r.unhosted_mcp_entry(_build_spec()) is None
    assert r.unhosted_mcp_entry(HOSTED) is None


def test_a_build_connector_may_declare_where_to_reach_it_when_unhosted() -> None:
    spec = _build_spec(unhosted_url="http://127.0.0.1:8765/mcp")
    assert r.unhosted_mcp_entry(spec) == {"type": "http", "url": "http://127.0.0.1:8765/mcp"}


def test_the_mcp_entry_for_a_build_connector_is_the_service_derived_url() -> None:
    # The URL derivation reads the Service name, never the image, so it is
    # identical before and after the lock is applied. That is what lets the
    # skill, local and cluster tiers mount one byte-identical entry.
    entry = r.mcp_entry("acme-rel", "acme-bot", "acme-ns", "k8s-write", _build_spec())
    assert entry == {
        "type": "http",
        "url": "http://acme-rel-acme-bot-mcp-k8s-write.acme-ns.svc.cluster.local:8000/mcp",
    }
    assert entry == r.mcp_entry(
        "acme-rel",
        "acme-bot",
        "acme-ns",
        "k8s-write",
        ConnectorSpec(image="ghcr.io/acme-corp/acme-bot-k8s-write-mcp@sha256:" + "0" * 64),
    )


# --------------------------------------------------------------------------- #
# The frozen Python/Rust object-name and Service-DNS seam -- ADR 0113 Decision 2
# --------------------------------------------------------------------------- #
_DNS_VECTORS = json.loads(
    (Path(__file__).parents[3] / "tests" / "vectors" / "connector-service-dns.json").read_text(
        encoding="utf-8"
    )
)

_DNS_VECTOR_KEYS = {
    "name",
    "why",
    "release",
    "agent",
    "connector",
    "namespace",
    "object_name",
    "service_dns",
}


def test_every_service_dns_vector_declares_only_modelled_keys() -> None:
    for vector in _DNS_VECTORS["vectors"]:
        extra = set(vector) - _DNS_VECTOR_KEYS
        assert not extra, f"{vector['name']}: unmodelled vector keys {sorted(extra)}"


@pytest.mark.parametrize("vector", _DNS_VECTORS["vectors"], ids=lambda v: v["name"])
def test_service_dns_vectors(vector: dict) -> None:
    # This string is the Docker network alias the CLI starts a skill-tier and
    # local-tier connector container under, and the runner derives the URL it
    # dials from THIS function. The two lanes cannot share code, so a change to
    # either derivation that is not made to both has to fail both suites --
    # otherwise the runner dials a DNS name no container owns and the failure is
    # a bare connection timeout with nothing logged at either end. Same
    # mechanism as tests/vectors/approval-action-ids.json.
    assert (
        r.object_name(vector["release"], vector["agent"], vector["connector"])
        == vector["object_name"]
    )
    assert (
        r.service_dns(
            vector["release"], vector["agent"], vector["connector"], vector["namespace"]
        )
        == vector["service_dns"]
    )


def test_the_dns_corpus_covers_the_truncation_branch() -> None:
    # Without an over-long name in the corpus the vectors freeze only the
    # concatenation, and the truncation-with-digest branch -- the half a hand
    # port gets wrong -- stays unpinned in both languages.
    bases = [
        f"{v['release']}-{v['agent']}-mcp-{v['connector']}" for v in _DNS_VECTORS["vectors"]
    ]
    assert any(len(base) > 63 for base in bases)
    truncated = [
        v
        for v in _DNS_VECTORS["vectors"]
        if len(f"{v['release']}-{v['agent']}-mcp-{v['connector']}") > 63
    ]
    # A truncated name must still be a legal DNS label, and two long names
    # sharing a prefix must not collapse onto one object.
    assert all(len(v["object_name"]) <= 63 for v in truncated)
    assert all(not v["object_name"].endswith("-") for v in truncated)
    assert len({v["object_name"] for v in truncated}) == len(truncated)


# --------------------------------------------------------------------------- #
# The Authorization header a hosted HTTP MCP server needs -- #2503
#
# `ghcr.io/github/github-mcp-server` over HTTP authenticates the CLIENT: per
# GitHub's docs its streamable-HTTP transport requires `Authorization: Bearer
# <PAT>` on every request, and an unauthenticated `GET /mcp` is a 401. Until
# now the hosted entry carried a URL and nothing else, so the probe failed and
# the agent simply listed no `mcp__github__*` tools -- a silent no-tools, not
# an error. The header is DERIVED from ``bearer_secret`` (or the single
# declared secret) for the same reason the URL is derived (ADR-0086): the
# author writes neither.
# --------------------------------------------------------------------------- #
GITHUB = ConnectorSpec(
    image="ghcr.io/github/github-mcp-server:v0.20.1",
    secrets=["GITHUB_PERSONAL_ACCESS_TOKEN"],
)


def _github_entry(spec: ConnectorSpec = GITHUB) -> dict:
    return r.mcp_entry("acme-rel", "acme-bot", "acme-ns", "github", spec)


def test_a_hosted_connector_carries_a_bearer_header_for_its_declared_secret() -> None:
    # Without this the mounted entry is a bare URL, github-mcp-server answers
    # 401, and the failure surfaces only as an agent with no tools (#2503).
    entry = _github_entry()
    assert entry["headers"] == {"Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"}
    # The URL derivation is untouched: the header rides alongside it.
    assert entry["url"] == (
        "http://acme-rel-acme-bot-mcp-github.acme-ns.svc.cluster.local:8000/mcp"
    )


def test_a_secret_ref_contributes_its_env_var_name_not_the_secret_it_points_at() -> None:
    # `secrets:` is `list[str | SecretRef]`. The header names the ENV VAR the
    # MCP client expands, which for a SecretRef is `.name` -- `from_secret` is
    # a Kubernetes Secret name and would expand to nothing in the sandbox.
    spec = ConnectorSpec(
        image="ghcr.io/github/github-mcp-server:v0.20.1",
        secrets=[SecretRef(name="GITHUB_PERSONAL_ACCESS_TOKEN", from_secret="gh-pat")],
    )
    assert _github_entry(spec)["headers"] == {
        "Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }


def test_a_hosted_connector_with_no_secrets_renders_exactly_as_it_did_before() -> None:
    # Regression guard on the byte-identical claim: a connector that declares
    # no credential has no env var to reference, so emitting an empty or
    # literal header would change the mounted entry of every existing bundle.
    spec = ConnectorSpec(image="ghcr.io/github/github-mcp-server:v0.20.1")
    assert _github_entry(spec) == {
        "type": "http",
        "url": "http://acme-rel-acme-bot-mcp-github.acme-ns.svc.cluster.local:8000/mcp",
    }


def test_a_remote_connector_keeps_the_headers_its_author_wrote() -> None:
    # Derivation is scoped to the hosted form. A `url:` connector's headers are
    # author input, so deriving over them would silently replace a hand-written
    # Authorization (or drop a second header) on every existing remote bundle.
    assert r.mcp_entry("acme-rel", "acme-bot", "acme-ns", "internal", REMOTE) == {
        "type": "http",
        "url": "https://mcp.internal/mcp",
        "headers": {"Authorization": "Bearer ${T}"},
    }


def test_only_the_placeholder_is_emitted_never_a_resolved_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `${VAR}` is expanded by the MCP client from the sandbox environment. If
    # the renderer ever read the value instead, the credential would land in
    # the mounted `.mcp.json` -- a plaintext copy on disk in every tier, and in
    # anything that logs the rendered entry.
    monkeypatch.setenv("GITHUB_PERSONAL_ACCESS_TOKEN", "ghp_sentinel")
    assert "ghp_sentinel" not in json.dumps(_github_entry())


def test_bearer_secret_names_the_header_when_several_secrets_are_declared() -> None:
    # #2559: secrets[0] was the Bearer only because it came first. A hosted
    # connector that also declares a pod-side credential must name the header
    # secret, or the sandbox would bind the wrong name (or every name).
    spec = ConnectorSpec(
        image="ghcr.io/github/github-mcp-server:v0.20.1",
        secrets=["POD_ONLY", "GITHUB_PERSONAL_ACCESS_TOKEN"],
        bearer_secret="GITHUB_PERSONAL_ACCESS_TOKEN",
    )
    assert _github_entry(spec)["headers"] == {
        "Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }


def test_a_single_declared_secret_still_derives_the_header_without_bearer_secret() -> None:
    # The github-mcp-server shape. Requiring the new field on every existing
    # one-secret bundle would be a break for no security gain: there is only
    # one name that could be the Bearer.
    spec = ConnectorSpec(
        image="ghcr.io/github/github-mcp-server:v0.20.1",
        secrets=["GITHUB_PERSONAL_ACCESS_TOKEN"],
    )
    assert spec.bearer_secret is None
    assert _github_entry(spec)["headers"] == {
        "Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }

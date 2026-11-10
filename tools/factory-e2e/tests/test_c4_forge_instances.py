"""C4 real-cluster registration regressions for #4620.

WARNING: root pytest collection includes this module and provisions real kind
clusters. Explicit selection always runs. Required executables are cargo, git,
Docker, kind, kubectl, Helm 3 and openssl. Inherit the configured Docker daemon.
There are no skips, model turns, live credentials or production test hooks.

The bare Fix pin is deliberately unparametrized. Four tests own four fresh,
sequential clusters; candidate artifacts are built once per pytest invocation.
Both Fix pin phases independently build their actual invocation working tree.
External HTTP fixtures establish regression behavior, not live-provider proof
tracked by #4400.
"""

from __future__ import annotations

import base64
import copy
import dataclasses
import importlib.util
import json
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest


def load_cluster_helpers() -> Any:
    name = "c4_forge_instance_cluster"
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name("forge_instance_cluster.py")
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("C4 cluster helper could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


helpers = load_cluster_helpers()
RELEASE = helpers.RELEASE
# The trigger label, then each state label, as the GitLab quickstart ensures them.
HELD_LABELS = (
    "curie-factory",
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
)
Candidate, Cluster, Peer = helpers.Candidate, helpers.Cluster, helpers.Peer
binding_for, diagnostic, entries = helpers.binding_for, helpers.diagnostic, helpers.entries
named, object_state = helpers.named, helpers.object_state
private_write, refused_for, same_secret = (
    helpers.private_write,
    helpers.refused_for,
    helpers.same_secret,
)


@pytest.fixture(scope="session")
def invocation_candidate(request: pytest.FixtureRequest) -> Candidate:
    source = Path.cwd().resolve()
    if not (source / "cli/Cargo.toml").is_file():
        pytest.fail("invoke pytest from the candidate repository root", pytrace=False)
    candidate = Candidate(source)
    request.addfinalizer(candidate.close)
    candidate.prepare()
    return candidate


@pytest.fixture(autouse=True)
def candidate_curie(invocation_candidate: Candidate, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Override the inherited shell-stub fixture by its exact name."""
    binary = invocation_candidate.binary
    if binary is None:
        invocation_candidate.fail("real candidate executable unavailable")
    monkeypatch.setenv("CURIE_BIN", str(binary))
    return binary


@pytest.fixture
def cluster_factory(
    request: pytest.FixtureRequest,
    invocation_candidate: Candidate,
) -> Callable[[str], Cluster]:
    def create(group: str) -> Cluster:
        cluster = Cluster(invocation_candidate, group)
        request.addfinalizer(cluster.close)
        cluster.start()
        cluster.seed_gitlab()
        return cluster

    return create


@pytest.fixture
def gitlab_cluster(cluster_factory: Callable[[str], Cluster]) -> Cluster:
    return cluster_factory("gitlab")


@pytest.fixture
def jira_cluster(cluster_factory: Callable[[str], Cluster]) -> Cluster:
    return cluster_factory("jira")


@pytest.fixture
def stale_cluster(cluster_factory: Callable[[str], Cluster]) -> Cluster:
    return cluster_factory("stale")


@pytest.fixture
def interrupted_cluster(cluster_factory: Callable[[str], Cluster]) -> Cluster:
    return cluster_factory("interrupted")


def credential_pair(values: dict[str, Any], peer: Peer) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = binding_for(values, peer)
    return (
        named(values, "codeHosts", binding["repos"][0]["codeHost"]),
        named(values, "trackers", binding["tracker"]),
    )


def clean_object(obj: dict[str, Any]) -> dict[str, Any]:
    result = object_state(obj, metadata=False)
    result.pop("status", None)
    result["metadata"].pop("deletionTimestamp", None)
    result["metadata"].pop("deletionGracePeriodSeconds", None)
    return result


def replace_object(cluster: Cluster, obj: dict[str, Any]) -> None:
    current = json.loads(
        cluster.kubectl("get", obj["kind"], obj["metadata"]["name"], "-o", "json").stdout
    )
    replacement = clean_object(obj)
    replacement["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
    cluster.kubectl("replace", "-f", "-", input_text=json.dumps(replacement))


def remove_object(cluster: Cluster, obj: dict[str, Any]) -> None:
    cluster.kubectl("delete", obj["kind"], obj["metadata"]["name"], "--wait=true")


def trust_object(cluster: Cluster, values: dict[str, Any]) -> dict[str, Any]:
    ref = values["codeHostTrust"]["caBundle"]["configMapRef"]
    return json.loads(cluster.kubectl("get", "configmap", ref["name"], "-o", "json").stdout)


def certificates(pem: str) -> set[bytes]:
    import ssl

    result = set()
    for chunk in pem.split("-----BEGIN CERTIFICATE-----")[1:]:
        body = chunk.split("-----END CERTIFICATE-----")[0]
        result.add(
            ssl.PEM_cert_to_DER_cert(
                "-----BEGIN CERTIFICATE-----" + body + "-----END CERTIFICATE-----"
            )
        )
    return result


def token_matches(secret: dict[str, Any], key: str, file: Path) -> bool:
    return base64.b64decode(secret["data"][key]) == file.read_bytes().strip()


def values_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    # Helm user values include signing keys. Keep pytest's recursive equality
    # explanation away from the raw document on a failing restoration.
    return left == right


def resource_names(snapshot: Any) -> set[str]:
    return {
        name
        for name in snapshot.objects
        if not name.startswith(f"Secret/sh.helm.release.v1.{RELEASE}.v")
    }


def observed_token(cluster: Cluster, peer: Peer, *, operation: str, ordinal: int = 0) -> bool:
    return any(
        r["operation"] == operation and r["credential"] == ordinal
        for r in cluster.receipts(peer)["requests"]
    )


def rename_gitlab_records(cluster: Cluster, peer: Peer) -> tuple[str, str, str, str]:
    values = cluster.values()
    binding = binding_for(values, peer)
    host, tracker = credential_pair(values, peer)
    secret = clean_object(cluster.secret(host))
    secret["metadata"] = {"name": "operator-forge-credential", "namespace": cluster.namespace}
    secret["data"] = {"operator-key": next(iter(secret["data"].values()))}
    cluster.apply(secret)
    old_host, old_tracker, old_binding = host["name"], tracker["name"], binding["name"]
    host["name"], tracker["name"], binding["name"] = (
        "operator-code-host",
        "gitlab",
        "operator-scope",
    )
    for entry in (host, tracker):
        entry["credential"].update(
            existingSecret=secret["metadata"]["name"], existingSecretKey="operator-key"
        )
    for item in values["factory"]["bindings"]:
        if item["tracker"] == old_tracker:
            item["tracker"] = tracker["name"]
        for repo in item["repos"]:
            if repo["codeHost"] == old_host:
                repo["codeHost"] = host["name"]
    ref = values["codeHostTrust"]["caBundle"]["configMapRef"]
    trust = clean_object(trust_object(cluster, values))
    trust["metadata"] = {"name": "operator-roots", "namespace": cluster.namespace}
    trust["data"] = {"roots.pem": trust["data"][ref["key"]]}
    cluster.apply(trust)
    ref.update(name="operator-roots", key="roots.pem")
    cluster.replace_values(values)
    return host["name"], tracker["name"], binding["name"], old_binding


def drop_peer(values: dict[str, Any], peer: Peer) -> dict[str, Any]:
    result = copy.deepcopy(values)
    binding = binding_for(result, peer)
    host, tracker = credential_pair(result, peer)
    result["factory"]["codeHosts"] = [
        e for e in result["factory"]["codeHosts"] if e["name"] != host["name"]
    ]
    result["factory"]["trackers"] = [
        e for e in result["factory"]["trackers"] if e["name"] != tracker["name"]
    ]
    result["factory"]["bindings"] = [
        e for e in result["factory"]["bindings"] if e["name"] != binding["name"]
    ]
    return result


def test_second_gitlab_registration_preserves_first(gitlab_cluster: Cluster) -> None:
    cluster = gitlab_cluster
    first, second = cluster.peers["gitlab-a"], cluster.peers["gitlab-b"]
    original = cluster.values()
    first_host, first_tracker = credential_pair(original, first)
    first_binding = copy.deepcopy(binding_for(original, first))
    first_secret = cluster.secret(first_host)
    trust_ref = copy.deepcopy(original["codeHostTrust"]["caBundle"]["configMapRef"])

    second_run = cluster.quickstart(second, agent="acme-second", label="second-agent-only")
    current = cluster.values()
    hosts = entries(current, "codeHosts", "gitlab")
    trackers = entries(current, "trackers", "gitlab")
    # AC1 is first: the old candidate reaches this assertion with a real Helm
    # release and replaces A. No refusal, rotation or trust vector precedes it.
    assert len(hosts) == 2, "the second quickstart replaced the first GitLab code host"
    assert len(trackers) == 2, "the second quickstart replaced the first GitLab tracker"
    assert {h["apiUrl"] for h in hosts} == {first.origin + "/api/v4", second.origin + "/api/v4"}
    assert {t["apiUrl"] for t in trackers} == {first.origin + "/api/v4", second.origin + "/api/v4"}
    assert named(current, "codeHosts", first_host["name"]) == first_host
    assert named(current, "trackers", first_tracker["name"]) == first_tracker
    assert binding_for(current, first) == first_binding
    assert same_secret(first_secret, cluster.secret(first_host)), "A Secret bytes/UID/RV changed"
    second_host, second_tracker = credential_pair(current, second)
    second_binding = binding_for(current, second)
    assert first_binding["name"] != second_binding["name"]
    assert first_binding["trackerScopeId"] == second_binding["trackerScopeId"] == "41"
    assert first_binding["repos"][0]["path"] != second_binding["repos"][0]["path"]
    assert first_binding["repos"][0]["projectId"] == second_binding["repos"][0]["projectId"] == "41"
    assert first_host["credential"]["existingSecret"] != second_host["credential"]["existingSecret"]
    assert second_host["credential"] == second_tracker["credential"]
    assert second_tracker["label"] == "curie-factory", (
        "an agent label became a new tracker's default"
    )
    assert token_matches(
        cluster.secret(second_host),
        second_host["credential"]["existingSecretKey"],
        second.token_file,
    ), "B did not retain its own credential"
    assert second_run.returncode == 0, "second registration did not complete"

    serving_polls = cluster.poll_receipts((first, second))
    assert set(serving_polls) == {first.name, second.name}, "serving API did not poll both servers"
    assert all(any(r["credential"] == 0 for r in rows) for rows in serving_polls.values()), (
        "serving API polling used another instance's credential"
    )
    for peer in (first, second):
        observation = cluster.probe("binding", binding=binding_for(current, peer)["name"])
        assert observation.get("content", "").count(peer.config["content"]) >= 1
        assert observation["repositories"][0]["path"] == peer.path
        assert observation["repositories"][0]["id"] == "41"
        assert observed_token(cluster, peer, operation="ticket")

    # AC2: an explicit surface label does not become the shared default.
    values = cluster.values()
    named(values, "trackers", first_tracker["name"])["label"] = "operator-default"
    cluster.replace_values(values)
    before = cluster.probe("labels")["surfaces"]
    old_surface = next(s for s in before if s["agent"] == "acme-first" and s["kind"] == "gitlab")
    assert old_surface["stored"] is None and old_surface["effective"] == "operator-default"
    labelled = cluster.quickstart(first, agent="acme-labelled", label="only-this-agent")
    assert labelled.returncode == 0
    after = cluster.probe("labels")["surfaces"]
    assert (
        next(s for s in after if s["agent"] == "acme-first" and s["kind"] == "gitlab")
        == old_surface
    )
    assert next(s for s in after if s["agent"] == "acme-labelled" and s["kind"] == "gitlab") == {
        "agent": "acme-labelled",
        "kind": "gitlab",
        "address": first.path,
        "stored": "only-this-agent",
        "effective": "only-this-agent",
    }
    assert credential_pair(cluster.values(), first)[1]["label"] == "operator-default"
    rerun = cluster.quickstart(first)
    assert rerun.returncode == 0
    assert "operator-default" in diagnostic(rerun)
    neighbor = cluster.quickstart(
        first, agent="acme-third", repo=first.config["projects"][1]["path"]
    )
    assert neighbor.returncode == 0
    assert len(entries(cluster.values(), "trackers", "gitlab")) == 2
    assert (
        len(
            [
                b
                for b in cluster.values()["factory"]["bindings"]
                if b["tracker"] == first_tracker["name"]
            ]
        )
        == 2
    )

    for kwargs, message in (
        ({"token": first.alternate_token_file}, r"secret|token|credential"),
        ({"url": first.origin + "/different-context"}, r"api|identity|host"),
    ):
        before_refusal = cluster.snapshot()
        refused = cluster.quickstart(first, **kwargs)
        after_refusal = cluster.snapshot()
        assert refused_for(
            refused, message, r"conflict|different|mismatch|rotat|refus|incompatible"
        )
        assert before_refusal == after_refusal, "refusal wrote protected state or peer labels"
    equivalent = cluster.quickstart(first, url=first.origin + "/")
    assert equivalent.returncode == 0

    # Missing/malformed credential state is arranged and completely restored
    # before the following independent refusal vector.
    first_host = credential_pair(cluster.values(), first)[0]
    saved = cluster.secret(first_host)
    secret_name = saved["metadata"]["name"]
    secret_key = first_host["credential"]["existingSecretKey"]
    for data in ({"wrong-key": "b3BlcmF0b3I="}, {secret_key: ""}):

        def arrange(data: dict[str, str] = data) -> None:
            altered = copy.deepcopy(saved)
            altered["data"] = data
            replace_object(cluster, altered)

        with cluster.operator_vector(
            arrange,
            lambda: replace_object(cluster, saved),
            changed_objects={f"Secret/{secret_name}"},
        ):
            before_refusal = cluster.snapshot()
            refused = cluster.quickstart(first)
            assert refused_for(refused, r"secret|key|token|credential")
            assert before_refusal == cluster.snapshot()
    with cluster.operator_vector(
        lambda: remove_object(cluster, saved),
        lambda: cluster.apply(clean_object(saved)),
        changed_objects={f"Secret/{secret_name}"},
    ):
        before_missing = cluster.snapshot()
        refused = cluster.quickstart(first)
        assert refused_for(refused, r"secret|credential", r"missing|not found|read|absent")
        assert before_missing == cluster.snapshot()
    unchanged = cluster.quickstart(first)
    assert unchanged.returncode == 0

    # A new registration must not adopt a foreign object at its deterministic
    # name, including an object whose bytes happen to match the supplied token.
    populated = cluster.values()
    second_host = credential_pair(populated, second)[0]
    second_secret = cluster.secret(second_host)
    without_second = drop_peer(populated, second)

    def foreign_arrangement() -> None:
        cluster.replace_values(without_second)
        foreign = clean_object(second_secret)
        foreign["metadata"] = {
            "name": second_secret["metadata"]["name"],
            "namespace": cluster.namespace,
            "annotations": {"meta.helm.sh/release-name": "acme-other"},
        }
        replace_object(cluster, foreign)

    def foreign_restoration() -> None:
        replace_object(cluster, second_secret)
        cluster.replace_values(populated)

    with cluster.operator_vector(
        foreign_arrangement,
        foreign_restoration,
        changed_objects={f"Secret/{second_secret['metadata']['name']}"},
        changes_helm=True,
    ):
        before_refusal = cluster.snapshot()
        refused = cluster.quickstart(second, agent="acme-second")
        assert refused_for(refused, r"secret|own|release", r"foreign|own|conflict|refus")
        assert before_refusal == cluster.snapshot()
    assert cluster.quickstart(second, agent="acme-second").returncode == 0

    # Occupy B's binding name with an independently valid scope from A.
    populated = cluster.values()
    occupied = copy.deepcopy(populated)
    b_binding = binding_for(occupied, second)
    b_binding.update(tracker=first_tracker["name"], trackerScopeId="43")
    b_binding["repos"][0].update(
        codeHost=first_host["name"], projectId="43", path="acme-corp/operator-occupied"
    )
    with cluster.operator_vector(
        lambda: cluster.replace_values(occupied),
        lambda: cluster.replace_values(populated),
        changes_helm=True,
    ):
        before_refusal = cluster.snapshot()
        refused = cluster.quickstart(second, agent="acme-second")
        assert refused_for(
            refused, r"binding|scope|name", r"conflict|occupied|identity|refus|different"
        )
        assert before_refusal == cluster.snapshot()
    assert cluster.quickstart(second, agent="acme-second").returncode == 0

    # Arbitrary recorded names and a fixed literal are equally canonical.
    canonical = rename_gitlab_records(cluster, first)
    before_reuse = cluster.snapshot()
    old_secret = cluster.secret(credential_pair(cluster.values(), first)[0])
    reuse = cluster.quickstart(first)
    assert reuse.returncode == 0
    reused = cluster.values()
    host, tracker = credential_pair(reused, first)
    assert (host["name"], tracker["name"], binding_for(reused, first)["name"]) == canonical[:3]
    assert host["name"] != tracker["name"]
    assert host["credential"]["existingSecretKey"] == "operator-key"
    assert resource_names(before_reuse) == resource_names(cluster.snapshot())
    assert before_reuse.values["factory"] == reused["factory"]
    assert same_secret(old_secret, cluster.secret(host)), (
        "equivalent rerun wrote the canonical Secret"
    )

    before_rotation = cluster.snapshot()
    refused = cluster.quickstart(first, token=first.rotated_token_file)
    assert refused_for(refused, r"secret|credential|token", r"different|rotat|refus|mismatch")
    assert before_rotation == cluster.snapshot()
    rotated = copy.deepcopy(old_secret)
    rotated["data"]["operator-key"] = base64.b64encode(
        first.rotated_token_file.read_bytes().strip()
    ).decode()
    replace_object(cluster, rotated)
    operator_secret = cluster.secret(host)
    rotation = cluster.quickstart(first, token=first.rotated_token_file)
    assert rotation.returncode == 0
    assert same_secret(operator_secret, cluster.secret(host)), (
        "quickstart rewrote the operator rotation"
    )
    assert credential_pair(cluster.values(), first)[0]["name"] == canonical[0]
    observation = cluster.probe("binding", binding=canonical[2])
    assert first.config["content"] in observation.get("content", "")
    assert observed_token(cluster, first, operation="ticket", ordinal=2)
    before_old = cluster.snapshot()
    refused = cluster.quickstart(first, token=first.token_file)
    assert refused_for(refused, r"secret|credential|token", r"different|rotat|refus|mismatch")
    assert before_old == cluster.snapshot()

    # AC3 trust: canonical custom key, both roots, no duplication, unknown CA.
    values = cluster.values()
    ref = values["codeHostTrust"]["caBundle"]["configMapRef"]
    assert ref == {"name": "operator-roots", "key": "roots.pem"}
    bundle_before = trust_object(cluster, values)
    expected_roots = certificates(first.ca.read_text()) | certificates(second.ca.read_text())
    assert certificates(bundle_before["data"][ref["key"]]) == expected_roots
    for peer, token in ((first, first.rotated_token_file), (second, second.token_file)):
        repeated = cluster.quickstart(
            peer, token=token, agent=("acme-first" if peer is first else "acme-second")
        )
        assert repeated.returncode == 0
        assert cluster.values()["codeHostTrust"]["caBundle"]["configMapRef"] == ref
        observation = cluster.probe("binding", binding=binding_for(cluster.values(), peer)["name"])
        assert peer.config["content"] in observation.get("content", "")
    bundle_after = trust_object(cluster, cluster.values())
    assert bundle_after["metadata"]["uid"] == bundle_before["metadata"]["uid"]
    assert (
        bundle_after["metadata"]["resourceVersion"] == bundle_before["metadata"]["resourceVersion"]
    )
    assert bundle_after["data"] == bundle_before["data"]
    assert bundle_after["data"][ref["key"]].count("-----BEGIN CERTIFICATE-----") == len(
        expected_roots
    )
    untrusted = cluster.probe(
        "untrusted", url=cluster.peers["gitlab-unknown"].origin + "/api/v4/version"
    )
    assert untrusted.get("tls_refused") is True, "unknown private root was trusted"
    malformed_ca = cluster.root / "malformed-ca.pem"
    private_write(malformed_ca, first.ca.read_text() + "\nnot a certificate\n")
    before_malformed = cluster.snapshot()
    refused = cluster.quickstart(first, token=first.rotated_token_file, ca=malformed_ca)
    assert refused_for(refused, r"ca|certificate|pem", r"invalid|malformed|unexpected|junk|refus")
    assert before_malformed == cluster.snapshot()

    with cluster.operator_vector(
        lambda: remove_object(cluster, bundle_after),
        lambda: cluster.apply(clean_object(bundle_after)),
        changed_objects={f"ConfigMap/{ref['name']}"},
    ):
        before_missing = cluster.snapshot()
        refused = cluster.quickstart(first, token=first.rotated_token_file)
        assert refused_for(refused, r"configmap|bundle|trust|ca", r"missing|not found|read|absent")
        assert before_missing == cluster.snapshot()
    assert cluster.quickstart(first, token=first.rotated_token_file).returncode == 0
    assert trust_ref["name"] != ref["name"], (
        "the custom-key vector did not exercise a new canonical bundle"
    )


def test_jira_sites_coexist_and_conflicts_preserve_all_routes(jira_cluster: Cluster) -> None:
    cluster = jira_cluster
    first, second = cluster.peers["jira-a"], cluster.peers["jira-b"]
    before_jira = cluster.values()
    gitlab = copy.deepcopy(before_jira["factory"])
    expected_env = copy.deepcopy(before_jira.get("agentSandbox", {}).get("runnerEnv", {}))
    history_before = json.loads(
        cluster.helm("history", RELEASE, "-o", "json", "--max", "256").stdout
    )
    assert isinstance(history_before, list)
    installed = cluster.quickstart(first, app_flags=True)
    assert installed.returncode == 0
    history_after = json.loads(
        cluster.helm("history", RELEASE, "-o", "json", "--max", "256").stdout
    )
    assert isinstance(history_after, list)
    assert len(history_after) - len(history_before) == 2, (
        "first Jira registration must write GitHub intake and the Jira tracker with its runner "
        "environment, without a standalone environment upgrade"
    )
    initial = cluster.values()
    expected_env.setdefault("acme-first", {})["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = "16000"
    assert initial["agentSandbox"]["runnerEnv"] == expected_env
    first_binding = copy.deepcopy(binding_for(initial, first))
    first_tracker = named(initial, "trackers", first_binding["tracker"])
    first_secret = cluster.secret(first_tracker)
    second_install = cluster.quickstart(second, app_flags=True, agent="acme-second")
    assert second_install.returncode == 0
    current = cluster.values()
    expected_env.setdefault("acme-second", {})["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = "16000"
    assert current["agentSandbox"]["runnerEnv"] == expected_env
    jira_trackers = entries(current, "trackers", "jira_cloud")
    assert len(jira_trackers) == 2
    assert {t["apiUrl"] for t in jira_trackers} == {first.origin, second.origin}
    assert binding_for(current, first) == first_binding
    assert same_secret(first_secret, cluster.secret(first_tracker))
    assert len({t["credential"]["existingSecret"] for t in jira_trackers}) == 2
    assert (
        binding_for(current, first)["trackerScopeId"]
        != binding_for(current, second)["trackerScopeId"]
    )
    for section in ("codeHosts", "trackers"):
        assert [e for e in current["factory"][section] if e["kind"] == "gitlab"] == gitlab[section]
    assert all(b in current["factory"]["bindings"] for b in gitlab["bindings"])
    for peer in (first, second):
        binding = binding_for(current, peer)
        observation = cluster.probe("binding", binding=binding["name"])
        assert peer.config["content"] in observation.get("content", "")
        assert observed_token(cluster, peer, operation="ticket")
        agent = "acme-first" if peer is first else "acme-second"
        surface = next(
            s
            for a in cluster.api("GET", "/agents")
            if a["name"] == agent
            for s in a["channels"]
            if s["kind"] == "jira"
        )
        assert surface["address"] == binding["name"]

    # Nonliteral names must survive the recorded AddAgent guard.
    renamed = cluster.values()
    tracker = named(renamed, "trackers", first_binding["tracker"])
    tracker["name"] = "operator-jira-site"
    canonical_jira_secret = clean_object(cluster.secret(first_tracker))
    canonical_jira_secret["metadata"] = {
        "name": "curie-jira-token",
        "namespace": cluster.namespace,
    }
    canonical_jira_secret["data"] = {
        "operator-jira-key": canonical_jira_secret["data"][
            first_tracker["credential"]["existingSecretKey"]
        ],
    }
    cluster.apply(canonical_jira_secret)
    tracker["credential"].update(
        existingSecret="curie-jira-token",
        existingSecretKey="operator-jira-key",
        neverExpires=False,
    )
    binding = binding_for(current, first)
    renamed_binding = next(
        b for b in renamed["factory"]["bindings"] if b["name"] == binding["name"]
    )
    renamed_binding.update(name="operator-jira-binding", tracker=tracker["name"])
    renamed_binding["startAuthority"]["accounts"] = ["acme-starter", "acme-second-starter"]
    tracker["pollIntervalSeconds"] = 45.0
    renamed_binding["components"] = {}
    renamed_binding["repos"][0].update(bases=[], ci={"metadataRerunKeys": []})
    cluster.replace_values(renamed)
    for agent in cluster.api("GET", "/agents"):
        for surface in agent["channels"]:
            if surface["kind"] == "jira" and surface["address"] == binding["name"]:
                query = f"?kind=jira&address={quote(binding['name'], safe='')}"
                cluster.api(
                    "PATCH",
                    f"/agents/{agent['id']}/channels{query}",
                    {"kind": "jira", "address": "operator-jira-binding"},
                )
    accounts = ("acme-second-starter", "acme-starter")
    # A second agent on one binding needs its own trigger label; the tracker
    # entry keeps the label it was recorded with.
    canonical_jira_before = cluster.secret(tracker)
    same_site = cluster.quickstart(
        first, agent="acme-labelled", accounts=accounts, label="only-this-agent"
    )
    assert same_site.returncode == 0
    expected_env.setdefault("acme-labelled", {})["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = "16000"
    assert cluster.values()["agentSandbox"]["runnerEnv"] == expected_env
    assert binding_for(cluster.values(), first)["name"] == "operator-jira-binding"
    assert same_secret(canonical_jira_before, cluster.secret(tracker))
    assert named(cluster.values(), "trackers", "operator-jira-site")["egress"] == tracker["egress"]
    assert (
        cluster.quickstart(
            first, agent="acme-labelled", accounts=accounts, label="only-this-agent"
        ).returncode
        == 0
    )

    for kwargs, cause in (
        ({"token": first.alternate_token_file}, r"token|credential|secret"),
        ({"username": "acme-other@example.com"}, r"username|credential|account"),
        ({"repo": "acme-corp/acme-other"}, r"repo|binding|scope"),
        ({"accounts": ("acme-starter",)}, r"authority|account|binding"),
        ({"url": first.origin + "/ex/jira/" + first.config["cloud_id"]}, r"api|identity|site"),
    ):
        arguments: dict[str, Any] = {"agent": "acme-third", "accounts": accounts, **kwargs}
        before_refusal = cluster.snapshot()
        refused = cluster.quickstart(first, **arguments)
        assert refused_for(refused, cause, r"different|mismatch|conflict|refus|rotat")
        assert before_refusal == cluster.snapshot()
        # The Install route must perform the same guard before GitHub Intake.
        before_install = cluster.snapshot()
        refused = cluster.quickstart(first, **arguments, app_flags=True)
        assert refused_for(refused, cause, r"different|mismatch|conflict|refus|rotat")
        assert before_install == cluster.snapshot()
    original_values = cluster.values()
    alternate_default = copy.deepcopy(original_values)
    default_binding = binding_for(alternate_default, first)
    github_host = default_binding["repos"][0]["codeHost"]
    default_binding["repos"].append(
        {
            "alias": "acme-other",
            "codeHost": github_host,
            "projectId": "78",
            "path": "acme-corp/acme-other",
        }
    )
    default_binding["defaultRepo"] = "acme-other"
    with cluster.operator_vector(
        lambda: cluster.replace_values(alternate_default),
        lambda: cluster.replace_values(original_values),
        changes_helm=True,
    ):
        cluster.check_bridges()
        before_default = cluster.snapshot()
        argv = cluster.quickstart_args(first, agent="acme-third", accounts=accounts)
        refused = cluster.run([*argv, "--repo", "acme-corp/acme-other"], check=False, timeout=360)
        assert refused_for(refused, r"default.?repo|binding", r"different|mismatch|conflict|refus")
        assert before_default == cluster.snapshot()
    original_labels = cluster.receipts(first)["labels"]
    with cluster.operator_vector(
        lambda: cluster.control(first, "/__fixture/identity", {"bot": "acme-other-bot"}),
        lambda: cluster.control(first, "/__fixture/restore", {"labels": original_labels}),
    ):
        before_identity = cluster.snapshot()
        refused = cluster.quickstart(first, agent="acme-third", accounts=accounts)
        assert refused_for(
            refused, r"mention|bot|account|identity", r"different|mismatch|conflict|refus"
        )
        assert before_identity == cluster.snapshot()
        before_install_identity = cluster.snapshot()
        refused = cluster.quickstart(first, agent="acme-third", accounts=accounts, app_flags=True)
        assert refused_for(
            refused, r"mention|bot|account|identity", r"different|mismatch|conflict|refus"
        )
        assert before_install_identity == cluster.snapshot()
    assert (
        cluster.quickstart(
            first, agent="acme-labelled", accounts=accounts, label="only-this-agent"
        ).returncode
        == 0
    )

    # A real DC route follows the GitHub Install and recorded AddAgent routes.
    # Its dedicated site gives the repository binding a distinct immutable scope.
    dc, dc_site = cluster.peers["dc"], cluster.peers["jira-dc"]
    before_dc = cluster.values()
    installed_dc = cluster.quickstart(dc, jira=dc_site, agent="acme-dc")
    assert installed_dc.returncode == 0
    after_dc = cluster.values()
    dc_hosts = entries(after_dc, "codeHosts", "bitbucket_dc")
    assert len(dc_hosts) == 1
    assert dc_hosts[0]["host"] == "127.0.0.1"
    dc_binding = binding_for(after_dc, dc_site)
    assert dc_binding["repos"][0]["codeHost"] == dc_hosts[0]["name"]
    assert dc_binding["repos"][0]["projectId"] == "88"
    assert dc_binding["reviewFeedbackAllowlist"] == {dc_hosts[0]["name"]: ["17"]}
    for peer in (first, second):
        assert binding_for(before_dc, peer) == binding_for(after_dc, peer)
    observation = cluster.probe("binding", binding=dc_binding["name"])
    assert dc_site.config["content"] in observation.get("content", "")
    assert observation["repositories"][0] == {
        "kind": "bitbucket_dc",
        "host": "127.0.0.1",
        "id": "88",
        "path": "ACME/widgets",
    }
    before_conflict = cluster.snapshot()
    refused_dc = cluster.quickstart(
        dc, jira=dc_site, agent="acme-dc", token=dc_site.alternate_token_file
    )
    assert refused_for(
        refused_dc, r"jira|secret|credential", r"different|mismatch|rotat|conflict|refus"
    )
    assert before_conflict == cluster.snapshot(), (
        "conflicting Jira input changed DC or Jira resources"
    )
    assert cluster.quickstart(dc, jira=dc_site, agent="acme-dc").returncode == 0


def test_registration_stale_observation_refuses(stale_cluster: Cluster) -> None:
    cluster = stale_cluster
    peer = cluster.peers["gitlab-a"]
    # A missing label POST is after ownership observation, unlike preflight
    # GETs. Only an external fixture blocks; product code has no timing hook.
    # Each held POST answers inside the CLI's HTTP timeout, so the operator's
    # window is the holds over every missing label, not one request.
    for lane in ("secret", "trust", "helm"):
        original_snapshot = cluster.snapshot()
        values = cluster.values()
        host = credential_pair(values, peer)[0]
        secret = cluster.secret(host)
        trust = trust_object(cluster, values)
        original_labels = cluster.receipts(peer)["labels"]
        cluster.control(peer, "/__fixture/arm", {"labels": list(HELD_LABELS)})
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(cluster.quickstart, peer)
            try:
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    if cluster.receipts(peer)["barrier_entered"]:
                        break
                    if future.done():
                        cluster.candidate.fail(
                            "quickstart never reached the post-observation label barrier"
                        )
                    time.sleep(0.2)
                else:
                    cluster.candidate.fail("post-observation label barrier timed out")
                if lane in ("secret", "trust"):
                    target = secret if lane == "secret" else trust
                    cluster.kubectl(
                        "annotate",
                        target["kind"],
                        target["metadata"]["name"],
                        "acme.example.com/operator-observation=newer",
                        "--overwrite",
                    )
                else:
                    changed = copy.deepcopy(values)
                    changed["api"]["orgName"] = "Acme operator update"
                    # Record the revision inside the label window and settle
                    # after the CLI finishes.
                    cluster.replace_values(changed, settle=False)
                after_operator = cluster.snapshot()
                cluster.control(peer, "/__fixture/release", {})
                result = future.result(timeout=120)
                if lane == "helm":
                    cluster.settle_release()
                assert refused_for(
                    result, r"stale|changed|conflict|concurrent|revision|resourceversion"
                )
                # Label writes are idempotent and precede the fence; every other
                # observed surface must stay exactly as the operator left it.
                finished = cluster.snapshot()
                assert after_operator == dataclasses.replace(
                    finished, label_writes=after_operator.label_writes
                ), "stale registration overwrote newer operator state"
            finally:
                cluster.control(peer, "/__fixture/release", {})
                # Always finish the command before restoring state or teardown.
                future.result(timeout=180)
                if lane in ("secret", "trust"):
                    replace_object(cluster, secret if lane == "secret" else trust)
                else:
                    cluster.replace_values(values)
                cluster.control(peer, "/__fixture/restore", {"labels": original_labels})
        changed_objects = set()
        if lane in ("secret", "trust"):
            target = secret if lane == "secret" else trust
            changed_objects.add(f"{target['kind']}/{target['metadata']['name']}")
        cluster.require_restoration(
            original_snapshot,
            changed_objects=changed_objects,
            changes_helm=lane == "helm",
            label_writes={peer.name: len(HELD_LABELS)},
        )
        restored = cluster.values()
        assert values_match(restored, values), "operator values were not restored"
        if lane == "secret":
            assert same_secret(secret, cluster.secret(host), version=False)
        elif lane == "trust":
            assert trust_object(cluster, restored)["data"] == trust["data"]
        assert cluster.quickstart(peer).returncode == 0, (
            "unchanged observation neighbor did not succeed"
        )


def test_interrupted_owned_resources_are_reused_and_foreign_resources_refuse(
    interrupted_cluster: Cluster,
) -> None:
    cluster = interrupted_cluster
    peer = cluster.peers["gitlab-b"]
    established = cluster.quickstart(peer, agent="acme-second")
    assert established.returncode == 0
    populated = cluster.values()
    host, tracker = credential_pair(populated, peer)
    secret = cluster.secret(host)
    original_binding = copy.deepcopy(binding_for(populated, peer))
    without_peer = drop_peer(populated, peer)
    # Retain the exact same-owner credential object as an interrupted run.
    cluster.replace_values(without_peer)
    before_retry_secret = cluster.secret(host)
    retry = cluster.quickstart(peer, agent="acme-second")
    assert retry.returncode == 0
    assert same_secret(before_retry_secret, cluster.secret(host))
    assert credential_pair(cluster.values(), peer) == (host, tracker)
    assert binding_for(cluster.values(), peer) == original_binding

    populated = cluster.values()
    without_peer = drop_peer(populated, peer)
    for foreign in (False, True):

        def arrange(foreign: bool = foreign) -> None:
            cluster.replace_values(without_peer)
            interrupted = copy.deepcopy(secret)
            if foreign:
                interrupted["metadata"]["annotations"] = {
                    "meta.helm.sh/release-name": "acme-foreign",
                    "meta.helm.sh/release-namespace": cluster.namespace,
                }
            else:
                interrupted["data"][host["credential"]["existingSecretKey"]] = base64.b64encode(
                    peer.alternate_token_file.read_bytes().strip()
                ).decode()
            replace_object(cluster, interrupted)

        def restore() -> None:
            replace_object(cluster, secret)
            cluster.replace_values(populated)

        with cluster.operator_vector(
            arrange,
            restore,
            changed_objects={f"Secret/{secret['metadata']['name']}"},
            changes_helm=True,
        ):
            before_refusal = cluster.snapshot()
            refused = cluster.quickstart(peer, agent="acme-second")
            assert refused_for(refused, r"secret", r"foreign|own|different|mismatch|conflict|refus")
            assert secret["metadata"]["name"] in diagnostic(refused)
            assert (
                "operator" in diagnostic(refused).lower()
                or "remove" in diagnostic(refused).lower()
                or "update" in diagnostic(refused).lower()
            )
            assert before_refusal == cluster.snapshot()
        assert cluster.quickstart(peer, agent="acme-second").returncode == 0

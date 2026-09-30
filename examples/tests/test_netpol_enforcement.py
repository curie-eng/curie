"""Black box coverage for the cluster NetworkPolicy enforcement gate."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import yaml
from plugin_format.connectors import validate_connectors

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check-netpol-enforcement.sh"
WEATHER_CONNECTORS = REPO_ROOT / "examples" / "weather" / "connectors.yaml"


@dataclass(frozen=True)
class GateRun:
    stdout: str
    stderr: str
    returncode: int
    kubectl_calls: list[list[str]]


def _run_gate(
    tmp_path: Path,
    *,
    connectors: tuple[str, ...],
    sandbox_unreachable: tuple[str, ...] = (),
    outside_reachable: tuple[str, ...] = (),
    sandbox_dns_unreachable: bool = False,
    outside_dns_unreachable: bool = False,
    sandbox_to_deny_target_reachable: bool = False,
    outside_to_deny_target_reachable: bool = True,
    connector_not_ready: tuple[str, ...] = (),
    foreign_reachable: tuple[str, ...] = (),
    foreign_dns_unreachable: bool = False,
    foreign_to_deny_target_reachable: bool = True,
    foreign_namespace_uncreatable: bool = False,
    foreign_namespace_policies: tuple[str, ...] = (),
    foreign_namespace_policies_unreadable: bool = False,
    foreign_pod_labels: str | None = None,
    foreign_fqdn_unresolvable: tuple[str, ...] = (),
    foreign_namespace_env: str | None = None,
    foreign_namespace_exists: bool = False,
    foreign_namespace_create_races: bool = False,
    direct_services: bool = False,
) -> GateRun:
    fake = tmp_path / "kubectl"
    log = tmp_path / "kubectl.jsonl"
    fake.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
from urllib.parse import urlparse

args = sys.argv[1:]
with open(os.environ["FAKE_KUBECTL_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(args) + "\\n")

connectors = tuple(filter(None, os.environ.get("FAKE_CONNECTORS", "").split(",")))
# With a caller proxy each connector also has a direct Service. It is named
# `<connector>-direct` and carries the connector's own name label.
direct_services = os.environ.get("FAKE_DIRECT_SERVICES") == "1"
sandbox_unreachable = set(
    filter(None, os.environ.get("FAKE_SANDBOX_UNREACHABLE", "").split(","))
)
outside_reachable = set(
    filter(None, os.environ.get("FAKE_OUTSIDE_REACHABLE", "").split(","))
)
sandbox_dns_unreachable = os.environ.get("FAKE_SANDBOX_DNS_UNREACHABLE") == "1"
outside_dns_unreachable = os.environ.get("FAKE_OUTSIDE_DNS_UNREACHABLE") == "1"
sandbox_to_deny_target_reachable = (
    os.environ.get("FAKE_SANDBOX_TO_DENY_TARGET_REACHABLE") == "1"
)
outside_to_deny_target_reachable = (
    os.environ.get("FAKE_OUTSIDE_TO_DENY_TARGET_REACHABLE") == "1"
)
connector_not_ready = set(
    filter(None, os.environ.get("FAKE_CONNECTOR_NOT_READY", "").split(","))
)
foreign_reachable = set(
    filter(None, os.environ.get("FAKE_FOREIGN_REACHABLE", "").split(","))
)
foreign_dns_unreachable = os.environ.get("FAKE_FOREIGN_DNS_UNREACHABLE") == "1"
foreign_to_deny_target_reachable = (
    os.environ.get("FAKE_FOREIGN_TO_DENY_TARGET_REACHABLE") == "1"
)
foreign_namespace_uncreatable = (
    os.environ.get("FAKE_FOREIGN_NAMESPACE_UNCREATABLE") == "1"
)
foreign_namespace_policies = tuple(
    filter(None, os.environ.get("FAKE_FOREIGN_NAMESPACE_POLICIES", "").split(","))
)
foreign_namespace_policies_unreadable = (
    os.environ.get("FAKE_FOREIGN_NAMESPACE_POLICIES_UNREADABLE") == "1"
)
foreign_pod_labels = os.environ.get("FAKE_FOREIGN_POD_LABELS", "")
foreign_fqdn_unresolvable = set(
    filter(None, os.environ.get("FAKE_FOREIGN_FQDN_UNRESOLVABLE", "").split(","))
)
foreign_namespace_exists = os.environ.get("FAKE_FOREIGN_NAMESPACE_EXISTS") == "1"
foreign_namespace_create_races = (
    os.environ.get("FAKE_FOREIGN_NAMESPACE_CREATE_RACES") == "1"
)
# Derived exactly as the script derives it, so the fake answers as the CLUSTER
# would rather than as the script hopes. A script that talked to some other
# namespace would reach no modelled arm and exit 90 instead of quietly passing.
foreign_ns = os.environ.get("CURIE_NETPOL_FOREIGN_NS") or "curie-netpol-foreign"

GENERIC_DNS_CONTROL = "kubernetes.default.svc.cluster.local"


def namespace() -> str | None:
    return args[args.index("-n") + 1] if "-n" in args else None


def namespace_get_ordinal() -> int:
    # This invocation is already in the log, so the first `get namespace` sees 1.
    # The ensure path is get -> create -> get, and only the SECOND get may answer
    # differently from the first; without an ordinal a create race and a plain
    # unreachable namespace are the same fake, and the race arm would be untested.
    seen = 0
    with open(os.environ["FAKE_KUBECTL_LOG"], encoding="utf-8") as stream:
        for line in stream:
            call = json.loads(line)
            if "get" in call and "namespace" in call:
                seen += 1
    return seen


def connector_deployment() -> str | None:
    for connector in connectors:
        if f"deployment/{connector}" in args or f"deploy/{connector}" in args:
            return connector
        for resource in ("deployment", "deploy"):
            if resource in args and args.index(resource) + 1 < len(args):
                if args[args.index(resource) + 1] == connector:
                    return connector
    return None


def service_name(host: str) -> str:
    # The foreign probe addresses a connector by FQDN -- e.g.
    # curie-mcp-alpha.curie.svc.cluster.local -- because the short Service name
    # does not resolve from another namespace. If this fake did not strip the
    # .<ns>.svc.cluster.local suffix, that host would match no connector, the
    # fake would invent a denial of its own, every foreign leg would "pass"
    # regardless of what the policy says, and the widening could never be shown
    # red. That is a test proving nothing -- the exact defect class #1502 was
    # filed about, one level up.
    return host.split(".")[0]

if "delete" in args or "apply" in args:
    raise SystemExit(0)

if "wait" in args or ("rollout" in args and "status" in args):
    deployment = connector_deployment()
    if deployment is not None:
        raise SystemExit(1 if deployment in connector_not_ready else 0)
    raise SystemExit(0)

if "get" in args and "networkpolicy" in args:
    # Two different questions share this verb, and answering both with a silent
    # exit 0 is what makes the policy-vacuum check untestable: the listing would
    # report "no policies" no matter what the namespace held. The named get is
    # the chart-installed check in $NS; the -o jsonpath listing is the vacuum
    # check in $FOREIGN_NS, and only the latter has an answer worth faking.
    if "-o" in args and namespace() == foreign_ns:
        if foreign_namespace_policies_unreadable:
            raise SystemExit(1)
        for name in foreign_namespace_policies:
            print(name, end=" ")
        raise SystemExit(0)
    if "-o" not in args:
        raise SystemExit(0)

# Both namespace verbs are matched BEFORE the looser get/pod and get/svc
# branches so `get namespace` is never swallowed by one of them.
if "get" in args and "namespace" in args:
    # Absent by default: that is the honest first-run state, and it is the only
    # arm that exercises `create namespace`, so a script that assumes the
    # namespace already exists cannot pass here.
    if foreign_namespace_exists:
        raise SystemExit(0)
    if foreign_namespace_create_races and namespace_get_ordinal() >= 2:
        raise SystemExit(0)
    raise SystemExit(1)

if "create" in args and "namespace" in args:
    if foreign_namespace_uncreatable or foreign_namespace_create_races:
        raise SystemExit(1)
    raise SystemExit(0)

if "get" in args and "pod" in args:
    # The deny target is read for its pod IP and the foreign probe for its
    # labels. One arm answering both would hand the label check an IP address,
    # so the label comparison could never be satisfied -- and the test that says
    # a stripped label is fatal would go red for the wrong reason, which is the
    # same thing as not testing it at all.
    pod_name = args[args.index("pod") + 1]
    if pod_name == "netpol-probe-deny-target":
        print("192.0.2.10", end="")
        raise SystemExit(0)
    if pod_name == "netpol-probe-foreign":
        print(foreign_pod_labels, end="")
        raise SystemExit(0)

if "get" in args and "svc" in args:
    if "-l" in args:
        listed = list(connectors)
        if direct_services:
            by_label = "labels" in args[-1]
            listed += [c if by_label else f"{c}-direct" for c in connectors]
        if listed:
            print("\\n".join(listed))
        raise SystemExit(0)
    print("8000", end="")
    raise SystemExit(0)

if "exec" in args:
    exec_index = args.index("exec")
    pod = args[exec_index + 1]
    command = args[args.index("--") + 1 :]
    if command and command[0] == "getent":
        resolved = command[-1]
        if pod == "netpol-probe-sandbox":
            raise SystemExit(1 if sandbox_dns_unreachable else 0)
        if pod == "netpol-probe-outside":
            raise SystemExit(1 if outside_dns_unreachable else 0)
        if pod == "netpol-probe-foreign":
            # Keyed on the NAME asked about, not just the pod. The generic
            # control and the per-connector FQDN check are separate hazards: a
            # CoreDNS view or a Cilium FQDN policy can serve kubernetes.default
            # and refuse one connector, so a fake that answers both from one
            # switch cannot show the per-connector check failing on its own.
            if resolved == GENERIC_DNS_CONTROL:
                raise SystemExit(1 if foreign_dns_unreachable else 0)
            raise SystemExit(1 if service_name(resolved) in foreign_fqdn_unresolvable else 0)
        raise SystemExit(1)
    urls = [value for value in command if value.startswith("http://")]
    if urls:
        host = urlparse(urls[0]).hostname or ""
        # The deny target is addressed by raw pod IP, so it must be matched
        # BEFORE the FQDN suffix is stripped -- 192.0.2.10 would otherwise
        # normalize to "192" and fall through to the connector arms.
        if host == "192.0.2.10":
            if pod == "netpol-probe-sandbox":
                raise SystemExit(0 if sandbox_to_deny_target_reachable else 1)
            if pod == "netpol-probe-outside":
                raise SystemExit(0 if outside_to_deny_target_reachable else 1)
            if pod == "netpol-probe-foreign":
                raise SystemExit(0 if foreign_to_deny_target_reachable else 1)
            raise SystemExit(1)
        service = service_name(host)
        if service not in connectors:
            raise SystemExit(1)
        if pod == "netpol-probe-sandbox":
            raise SystemExit(1 if service in sandbox_unreachable else 0)
        if pod == "netpol-probe-outside":
            raise SystemExit(0 if service in outside_reachable else 1)
        if pod == "netpol-probe-foreign":
            raise SystemExit(0 if service in foreign_reachable else 1)

print(f"unexpected kubectl invocation: {args!r}", file=sys.stderr)
raise SystemExit(90)
""",
        encoding="utf-8",
    )
    fake.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{tmp_path}{os.pathsep}{env['PATH']}",
            "FAKE_KUBECTL_LOG": str(log),
            "FAKE_CONNECTORS": ",".join(connectors),
            "FAKE_SANDBOX_UNREACHABLE": ",".join(sandbox_unreachable),
            "FAKE_OUTSIDE_REACHABLE": ",".join(outside_reachable),
            "FAKE_SANDBOX_DNS_UNREACHABLE": ("1" if sandbox_dns_unreachable else "0"),
            "FAKE_OUTSIDE_DNS_UNREACHABLE": ("1" if outside_dns_unreachable else "0"),
            "FAKE_SANDBOX_TO_DENY_TARGET_REACHABLE": (
                "1" if sandbox_to_deny_target_reachable else "0"
            ),
            "FAKE_OUTSIDE_TO_DENY_TARGET_REACHABLE": (
                "1" if outside_to_deny_target_reachable else "0"
            ),
            "FAKE_CONNECTOR_NOT_READY": ",".join(connector_not_ready),
            "FAKE_FOREIGN_REACHABLE": ",".join(foreign_reachable),
            "FAKE_FOREIGN_DNS_UNREACHABLE": ("1" if foreign_dns_unreachable else "0"),
            "FAKE_FOREIGN_TO_DENY_TARGET_REACHABLE": (
                "1" if foreign_to_deny_target_reachable else "0"
            ),
            "FAKE_FOREIGN_NAMESPACE_UNCREATABLE": ("1" if foreign_namespace_uncreatable else "0"),
            "FAKE_FOREIGN_NAMESPACE_POLICIES": ",".join(foreign_namespace_policies),
            "FAKE_FOREIGN_NAMESPACE_POLICIES_UNREADABLE": (
                "1" if foreign_namespace_policies_unreadable else "0"
            ),
            # Default to the labels a correct apply produces, so every other test
            # exercises the read-back on its passing path rather than skipping it.
            "FAKE_FOREIGN_POD_LABELS": (
                "curie curie runner-sandbox" if foreign_pod_labels is None else foreign_pod_labels
            ),
            "FAKE_FOREIGN_FQDN_UNRESOLVABLE": ",".join(foreign_fqdn_unresolvable),
            "FAKE_FOREIGN_NAMESPACE_EXISTS": ("1" if foreign_namespace_exists else "0"),
            "FAKE_FOREIGN_NAMESPACE_CREATE_RACES": ("1" if foreign_namespace_create_races else "0"),
            "FAKE_DIRECT_SERVICES": ("1" if direct_services else "0"),
        }
    )
    # Popped rather than left alone: an ambient CURIE_NETPOL_FOREIGN_NS on the
    # developer's shell would silently move the probe's namespace for every test
    # in this file, and the override tests would then prove nothing.
    if foreign_namespace_env is None:
        env.pop("CURIE_NETPOL_FOREIGN_NS", None)
    else:
        env["CURIE_NETPOL_FOREIGN_NS"] = foreign_namespace_env
    result = subprocess.run(
        ["bash", str(SCRIPT), "curie", "curie", "curie"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    return GateRun(result.stdout, result.stderr, result.returncode, calls)


def _service_name(host: str) -> str:
    # The foreign probe must use the FQDN (a short Service name does not resolve
    # from another namespace), while the same-namespace probes use the short
    # name. Both must bucket to the same connector key here, or a foreign
    # attempt would be counted as a different target than the sandbox and
    # outside attempts against the very same Service.
    return host.split(".")[0]


def _probe_exec_calls(calls: list[list[str]], pod: str) -> list[list[str]]:
    return [
        call
        for call in calls
        if "exec" in call and "--" in call and call[call.index("exec") + 1] == pod
    ]


def _exec_urls(call: list[str]) -> list[str]:
    command = call[call.index("--") + 1 :]
    return [value for value in command if value.startswith("http://")]


def _connector_curl_calls(calls: list[list[str]]) -> set[tuple[str, str]]:
    attempts: set[tuple[str, str]] = set()
    for call in calls:
        if "exec" not in call or "--" not in call:
            continue
        urls = _exec_urls(call)
        if not urls:
            continue
        service = _service_name(urlparse(urls[0]).hostname or "")
        if "-mcp-" in service:
            attempts.add((call[call.index("exec") + 1], service))
    return attempts


def _curl_events(calls: list[list[str]]) -> list[tuple[int, str, str]]:
    events: list[tuple[int, str, str]] = []
    probe_roles = {
        "netpol-probe-sandbox": "sandbox",
        "netpol-probe-outside": "outside",
        "netpol-probe-foreign": "foreign",
    }
    for index, call in enumerate(calls):
        if "exec" not in call or "--" not in call:
            continue
        urls = _exec_urls(call)
        if not urls:
            continue
        pod = call[call.index("exec") + 1]
        role = probe_roles.get(pod, pod)
        host = urlparse(urls[0]).hostname or ""
        target = "deny_target" if host == "192.0.2.10" else _service_name(host)
        events.append((index, role, target))
    return events


def _resolution_events(calls: list[list[str]]) -> list[tuple[int, str, str]]:
    # Keeps the NAME asked about, not just the pod. The ordering pin needs to
    # know which resolution preceded which curl, and every foreign getent looks
    # identical without the name.
    events: list[tuple[int, str, str]] = []
    for index, call in enumerate(calls):
        if "exec" not in call or "--" not in call:
            continue
        command = call[call.index("--") + 1 :]
        if command[:2] == ["getent", "hosts"]:
            events.append((index, call[call.index("exec") + 1], command[-1]))
    return events


def _dns_events(calls: list[list[str]]) -> list[tuple[int, str]]:
    return [(index, pod) for index, pod, _ in _resolution_events(calls)]


def _namespace_verb_calls(calls: list[list[str]], verb: str) -> list[list[str]]:
    return [call for call in calls if verb in call and "namespace" in call]


def _deployment_readiness_events(
    calls: list[list[str]], connectors: tuple[str, ...]
) -> list[tuple[int, str]]:
    events: list[tuple[int, str]] = []
    for index, call in enumerate(calls):
        if "wait" not in call and not ("rollout" in call and "status" in call):
            continue
        for connector in connectors:
            direct_resource = any(
                value in call for value in (f"deployment/{connector}", f"deploy/{connector}")
            )
            separate_resource = any(
                resource in call
                and call.index(resource) + 1 < len(call)
                and call[call.index(resource) + 1] == connector
                for resource in ("deployment", "deploy")
            )
            if direct_resource or separate_resource:
                events.append((index, connector))
    return events


def test_weather_ladder_bundle_declares_a_hosted_connector() -> None:
    assert WEATHER_CONNECTORS.is_file(), "the fixed weather ladder bundle has no connectors.yaml"
    parsed, errors = validate_connectors(yaml.safe_load(WEATHER_CONNECTORS.read_text()))

    assert errors == []
    assert parsed is not None
    assert any(connector.is_hosted for connector in parsed.connectors.values())


def test_zero_connector_services_fails(tmp_path: Path) -> None:
    result = _run_gate(tmp_path, connectors=())

    assert result.returncode != 0
    assert "found 0 connector Services" in result.stdout + result.stderr


def test_non_enforcing_cni_fails_before_connector_checks(tmp_path: Path) -> None:
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha",),
        sandbox_to_deny_target_reachable=True,
    )

    assert result.returncode != 0
    assert "CNI is not enforcing NetworkPolicy" in result.stderr
    assert not any(
        target.startswith("curie-mcp-") for _, _, target in _curl_events(result.kubectl_calls)
    )


def test_outside_probe_must_reach_known_listener_before_connector_denial(tmp_path: Path) -> None:
    connector = "curie-mcp-alpha"
    result = _run_gate(
        tmp_path,
        connectors=(connector,),
        outside_to_deny_target_reachable=False,
    )

    assert result.returncode != 0
    assert "outside probe" in result.stderr
    assert "deny target" in result.stderr
    assert ("outside", "deny_target") in {
        (role, target) for _, role, target in _curl_events(result.kubectl_calls)
    }
    assert ("outside", connector) not in {
        (role, target) for _, role, target in _curl_events(result.kubectl_calls)
    }


def test_outside_probe_must_resolve_dns_before_connector_denial(tmp_path: Path) -> None:
    connector = "curie-mcp-alpha"
    result = _run_gate(
        tmp_path,
        connectors=(connector,),
        outside_dns_unreachable=True,
    )

    assert result.returncode != 0
    assert "outside probe cannot resolve DNS" in result.stderr
    dns_events = _dns_events(result.kubectl_calls)
    assert any(pod == "netpol-probe-outside" for _, pod in dns_events)
    outside_connector_curls = [
        index
        for index, role, target in _curl_events(result.kubectl_calls)
        if role == "outside" and target == connector
    ]
    assert outside_connector_curls == []


def test_every_connector_checks_sandbox_allow_outside_deny_and_foreign_deny(
    tmp_path: Path,
) -> None:
    # An exact set, not a subset: a connector silently skipped by one of the
    # three legs is the failure this catches. Two connectors is what makes
    # "asserted per connector" testable at all -- with one, a leg run once for
    # the whole run is indistinguishable from a leg run per connector.
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(tmp_path, connectors=connectors)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "connector Service count: 2" in result.stdout
    assert _connector_curl_calls(result.kubectl_calls) == {
        ("netpol-probe-sandbox", "curie-mcp-alpha"),
        ("netpol-probe-outside", "curie-mcp-alpha"),
        ("netpol-probe-foreign", "curie-mcp-alpha"),
        ("netpol-probe-sandbox", "curie-mcp-beta"),
        ("netpol-probe-outside", "curie-mcp-beta"),
        ("netpol-probe-foreign", "curie-mcp-beta"),
    }


def test_a_connector_s_direct_service_is_not_a_second_connector(tmp_path: Path) -> None:
    # ADR-0168 decision 7: the direct Service lands on the server's own port,
    # which only an operator policy opens, so a sandbox is refused there by
    # design. It fronts the same connector and must not be probed as another.
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(tmp_path, connectors=connectors, direct_services=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "connector Service count: 2" in result.stdout
    assert "-direct" not in json.dumps(result.kubectl_calls)


def test_connector_denials_follow_outside_control_and_deployment_readiness(
    tmp_path: Path,
) -> None:
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(tmp_path, connectors=connectors)

    assert result.returncode == 0, result.stdout + result.stderr
    curl_indexes = {
        (role, target): index for index, role, target in _curl_events(result.kubectl_calls)
    }
    readiness_indexes = {
        connector: index
        for index, connector in _deployment_readiness_events(result.kubectl_calls, connectors)
    }
    assert ("sandbox", "deny_target") in curl_indexes
    assert ("outside", "deny_target") in curl_indexes
    assert set(readiness_indexes) == set(connectors)
    for connector in connectors:
        assert readiness_indexes[connector] < curl_indexes[("sandbox", connector)]
        assert readiness_indexes[connector] < curl_indexes[("outside", connector)]
        assert curl_indexes[("outside", "deny_target")] < curl_indexes[("outside", connector)]


def test_connector_readiness_failure_is_not_reported_as_policy_failure(tmp_path: Path) -> None:
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(
        tmp_path,
        connectors=connectors,
        connector_not_ready=("curie-mcp-beta",),
    )

    assert result.returncode != 0
    assert "Deployment curie-mcp-beta" in result.stderr
    assert "ready" in result.stderr
    assert "sandbox cannot reach connector Service curie-mcp-beta" not in result.stderr
    assert ("sandbox", "curie-mcp-beta") not in {
        (role, target) for _, role, target in _curl_events(result.kubectl_calls)
    }


def test_outside_access_to_any_connector_fails(tmp_path: Path) -> None:
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(
        tmp_path,
        connectors=connectors,
        outside_reachable=("curie-mcp-beta",),
    )

    assert result.returncode != 0
    assert "outside probe reached connector Service curie-mcp-beta:8000" in result.stderr


def test_sandbox_access_loss_fails(tmp_path: Path) -> None:
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(
        tmp_path,
        connectors=connectors,
        sandbox_unreachable=("curie-mcp-beta",),
    )

    assert result.returncode != 0
    assert "sandbox cannot reach connector Service curie-mcp-beta:8000" in result.stderr


def test_foreign_probe_runs_in_a_different_namespace_than_the_connectors(
    tmp_path: Path,
) -> None:
    # The foreign probe earns its keep by differing from the sandbox on the
    # NAMESPACE axis and nothing else. Move it back into the connectors' own
    # namespace -- an easy "simplification", since it would then need no
    # namespace RBAC and no second apply -- and it silently degrades into a
    # second copy of the outside probe, observing only the pod axis that is
    # already covered. The gate would stay green while #1502's widening walked
    # straight back in.
    result = _run_gate(tmp_path, connectors=("curie-mcp-alpha", "curie-mcp-beta"))

    assert result.returncode == 0, result.stdout + result.stderr
    foreign_calls = _probe_exec_calls(result.kubectl_calls, "netpol-probe-foreign")
    assert foreign_calls, "the foreign probe never ran"
    for call in foreign_calls:
        assert "-n" in call, f"foreign exec carries no namespace flag: {call!r}"
        namespace = call[call.index("-n") + 1]
        assert namespace != "curie", (
            "the foreign probe executed in the connectors' own namespace; it then "
            "differs from the sandbox on the pod axis only and cannot observe a "
            f"namespace-axis widening: {call!r}"
        )


def test_foreign_probe_addresses_the_connector_by_fqdn(tmp_path: Path) -> None:
    # A short Service name resolves only inside the Service's own namespace.
    # From the foreign namespace it fails DNS, curl exits non-zero, and the
    # script reads that as "policy denied me" -- a green leg that never
    # attempted the connection at all. The FQDN forces the connection to be
    # made, so its failure is attributable to the ingress policy.
    result = _run_gate(tmp_path, connectors=("curie-mcp-alpha", "curie-mcp-beta"))

    assert result.returncode == 0, result.stdout + result.stderr
    hosts = [
        urlparse(url).hostname or ""
        for call in _probe_exec_calls(result.kubectl_calls, "netpol-probe-foreign")
        for url in _exec_urls(call)
    ]
    connector_hosts = [host for host in hosts if "-mcp-" in host]
    assert connector_hosts, "the foreign probe never curled a connector"
    for host in connector_hosts:
        assert host.endswith(".curie.svc.cluster.local"), (
            "the foreign probe must address the connector by FQDN; the short name "
            f"does not resolve from another namespace: {host!r}"
        )


def test_foreign_probe_reaching_a_connector_fails_the_gate(tmp_path: Path) -> None:
    # The harness-level analogue of the live widening: `namespaceSelector: {}`
    # merged into the ingress `from` peer admits sandbox-labelled pods from
    # every namespace while still denying the unlabelled same-namespace outside
    # probe. Before the foreign leg existed this run was green (#1502).
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(
        tmp_path,
        connectors=connectors,
        foreign_reachable=("curie-mcp-beta",),
    )

    assert result.returncode != 0
    lowered = result.stderr.lower()
    assert "reached connector service curie-mcp-beta:8000" in lowered
    assert "every namespace" in lowered, (
        "the failure must say what the widening IS -- an ingress peer admitting "
        "sandbox-labelled pods in every namespace -- not merely that a curl "
        f"worked: {result.stderr!r}"
    )
    assert "1502" in result.stderr


def test_foreign_probe_must_reach_deny_target_before_connector_denial(
    tmp_path: Path,
) -> None:
    # Sibling of test_outside_probe_must_reach_known_listener_before_connector_denial.
    # A foreign probe with no working cross-namespace network denies every
    # connector for reasons that have nothing to do with the ingress policy, so
    # its denial certifies nothing. That must be fatal, and it must be caught
    # before any connector is curled from it.
    connector = "curie-mcp-alpha"
    result = _run_gate(
        tmp_path,
        connectors=(connector,),
        foreign_to_deny_target_reachable=False,
    )

    assert result.returncode != 0
    assert "foreign" in result.stderr.lower()
    assert "deny target" in result.stderr
    events = {(role, target) for _, role, target in _curl_events(result.kubectl_calls)}
    assert ("foreign", "deny_target") in events
    assert ("foreign", connector) not in events


def test_foreign_probe_must_resolve_dns_before_connector_denial(tmp_path: Path) -> None:
    # Sibling of test_outside_probe_must_resolve_dns_before_connector_denial,
    # and the sharper hazard of the two here: the foreign probe reaches the
    # connector by NAME, so broken DNS makes every foreign denial vacuous while
    # looking exactly like enforcement.
    connector = "curie-mcp-alpha"
    result = _run_gate(
        tmp_path,
        connectors=(connector,),
        foreign_dns_unreachable=True,
    )

    assert result.returncode != 0
    lowered = result.stderr.lower()
    assert "foreign" in lowered
    assert "resolve dns" in lowered
    dns_events = _dns_events(result.kubectl_calls)
    assert any(pod == "netpol-probe-foreign" for _, pod in dns_events)
    foreign_connector_curls = [
        index
        for index, role, target in _curl_events(result.kubectl_calls)
        if role == "foreign" and target == connector
    ]
    assert foreign_connector_curls == []


def test_foreign_namespace_creation_failure_is_fatal(tmp_path: Path) -> None:
    # A skipped deny leg is worse than no deny leg, because a green run reads as
    # proof. If the foreign namespace cannot be created the script must stop,
    # not carry on with two legs and print the same banner -- and the message
    # must hand the operator the way out, since a restricted kubectl context
    # with no cluster-scoped namespace RBAC is a legitimate way to arrive here.
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_namespace_uncreatable=True,
    )

    assert result.returncode != 0
    assert "CURIE_NETPOL_FOREIGN_NS" in result.stderr, (
        "the failure must name the override an operator without namespace-create "
        f"RBAC needs: {result.stderr!r}"
    )
    assert "RBAC" in result.stderr
    assert _connector_curl_calls(result.kubectl_calls) == set(), (
        "no connector was probed at all, so no leg may be reported as having passed"
    )


def test_foreign_namespace_carrying_a_networkpolicy_is_fatal(tmp_path: Path) -> None:
    # A denial observed from a namespace that has policies of its own is
    # unattributable. An injected egress policy allowing the pod CIDR and DNS but
    # not the Service CIDR leaves BOTH foreign positive controls passing -- they
    # use a pod IP and DNS -- while the connector curl fails on the probe's own
    # egress. The leg would print ok with connector ingress wide open.
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_namespace_policies=("injected-egress",),
    )

    assert result.returncode != 0
    assert "curie-netpol-foreign" in result.stderr
    assert "injected-egress" in result.stderr, (
        "the failure must name the policy that made the namespace unusable, or an "
        f"operator cannot tell what to remove: {result.stderr!r}"
    )
    assert _connector_curl_calls(result.kubectl_calls) == set(), (
        "no connector was probed at all, so no leg may be reported as having passed"
    )


def test_foreign_namespace_policy_list_failure_is_fatal(tmp_path: Path) -> None:
    # Unreadable is not the same as empty. Treating a failed listing as "no
    # policies" would restore exactly the vacuum assumption the listing exists to
    # replace, and would do it silently on any context lacking the read.
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_namespace_policies_unreadable=True,
    )

    assert result.returncode != 0
    assert "NetworkPolicies" in result.stderr
    assert "curie-netpol-foreign" in result.stderr
    assert _connector_curl_calls(result.kubectl_calls) == set()


def test_foreign_probe_without_sandbox_labels_is_fatal(tmp_path: Path) -> None:
    # Wearing the three runner-sandbox labels is the entire reason this probe can
    # see the namespace axis. A mutating webhook that strips
    # app.kubernetes.io/component leaves an UNLABELLED probe, which a correct
    # connector policy and a namespace-widened one deny alike -- so the leg goes
    # green while the widening stays invisible. That is #1502 one level up: a
    # check that cannot fail.
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_pod_labels="curie curie something-else",
    )

    assert result.returncode != 0
    assert "runner-sandbox" in result.stderr
    assert "curie curie something-else" in result.stderr, (
        "the failure must show the labels actually read back off the live object, "
        f"not merely say they were wrong: {result.stderr!r}"
    )
    assert ("netpol-probe-foreign", "curie-mcp-alpha") not in _connector_curl_calls(
        result.kubectl_calls
    )


def test_foreign_probe_must_resolve_each_connector_fqdn(tmp_path: Path) -> None:
    # A DNS failure inside the curl is indistinguishable from a policy denial:
    # curl exits non-zero either way and the script reads that as enforcement.
    # The generic kubernetes.default control does not cover this -- a Cilium FQDN
    # policy or a CoreDNS view can serve that name and refuse this one -- so the
    # connector's own FQDN must be proven resolvable before its unreachability
    # means anything.
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(
        tmp_path,
        connectors=connectors,
        foreign_fqdn_unresolvable=("curie-mcp-beta",),
    )

    assert result.returncode != 0
    assert "curie-mcp-beta.curie.svc.cluster.local" in result.stderr, (
        f"the failure must name the FQDN that would not resolve: {result.stderr!r}"
    )
    assert ("netpol-probe-foreign", "curie-mcp-beta") not in _connector_curl_calls(
        result.kubectl_calls
    ), "the denial curl ran anyway, so an unresolvable name would still read as enforcement"


def test_explicit_foreign_namespace_makes_no_cluster_scoped_calls(tmp_path: Path) -> None:
    # The documented escape hatch is for the operator who lacks cluster-scoped
    # namespace RBAC. A guard that trips on the very permission it is escaping is
    # a guard in name only, so this asserts the override is real: not created,
    # not even read, and the probe genuinely lands in the supplied namespace.
    foreign_ns = "preprovisioned-probe-ns"
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_namespace_env=foreign_ns,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert _namespace_verb_calls(result.kubectl_calls, "get") == [], (
        "`kubectl get namespace` is cluster-scoped; calling it defeats the override"
    )
    assert _namespace_verb_calls(result.kubectl_calls, "create") == []
    foreign_applies = [
        call
        for call in result.kubectl_calls
        if "apply" in call and "-n" in call and call[call.index("-n") + 1] == foreign_ns
    ]
    assert foreign_applies, (
        "the foreign probe was never applied into the supplied namespace, so the "
        "override changed the message and nothing else"
    )


def test_existing_foreign_namespace_is_reused_not_created(tmp_path: Path) -> None:
    # The steady state after the first run. A script that created unconditionally
    # would fail on AlreadyExists on every subsequent run, and a script that
    # treated that failure as fatal would take the whole deny leg down with it.
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_namespace_exists=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert _namespace_verb_calls(result.kubectl_calls, "create") == [], (
        "an existing namespace was created again rather than reused"
    )


def test_foreign_namespace_create_race_is_survivable(tmp_path: Path) -> None:
    # Two runs against one cluster both miss on `get`, one create wins and the
    # loser sees AlreadyExists -- for a namespace that by then exists and is
    # perfectly usable. Trusting the create's exit status turns that race into a
    # dropped deny leg, which reads as proof that ingress is narrow.
    result = _run_gate(
        tmp_path,
        connectors=("curie-mcp-alpha", "curie-mcp-beta"),
        foreign_namespace_create_races=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert len(_namespace_verb_calls(result.kubectl_calls, "get")) >= 2, (
        "the create failure was not re-checked, so the run survived by luck"
    )


def test_foreign_connector_denial_follows_its_own_controls(tmp_path: Path) -> None:
    # Sibling of test_connector_denials_follow_outside_control_and_deployment_readiness,
    # for the foreign leg. Every one of these orderings is load-bearing: a
    # reordered script still exits 0 on a healthy cluster, so nothing but an
    # ordering pin catches a control that has drifted BEHIND the denial it is
    # supposed to qualify.
    connectors = ("curie-mcp-alpha", "curie-mcp-beta")
    result = _run_gate(tmp_path, connectors=connectors)

    assert result.returncode == 0, result.stdout + result.stderr
    curl_indexes = {
        (role, target): index for index, role, target in _curl_events(result.kubectl_calls)
    }
    readiness_indexes = {
        connector: index
        for index, connector in _deployment_readiness_events(result.kubectl_calls, connectors)
    }
    fqdn_indexes = {
        host: index
        for index, pod, host in _resolution_events(result.kubectl_calls)
        if pod == "netpol-probe-foreign"
    }
    assert ("foreign", "deny_target") in curl_indexes
    for connector in connectors:
        denial = curl_indexes[("foreign", connector)]
        fqdn = f"{connector}.curie.svc.cluster.local"
        assert fqdn in fqdn_indexes, (
            f"the foreign probe never proved {fqdn} resolves, so its denial of "
            "that connector could be a DNS failure"
        )
        assert fqdn_indexes[fqdn] < denial
        assert curl_indexes[("foreign", "deny_target")] < denial
        assert readiness_indexes[connector] < denial

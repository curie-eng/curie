"""Offline checks for the docker scenario harness (ADR 0216, ADR 0217)."""

from __future__ import annotations

import copy
import json
import os
import re
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import docker_scenario
import factory_e2e as fe
import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
FIXTURES = ROOT / "fixtures" / "docker-scenario"
RAW = FIXTURES / "raw"
GITHUB = FIXTURES / "github"
LIFECYCLE = ROOT / "fixtures" / "daemon-lifecycle" / "passing-envelope.json"
SCRIPT = ROOT / "docker_scenario.py"
FACTORY = ROOT / "factory_e2e.py"

NONCE = "e8ce1a8c"
IMAGE = "alpine:3.20"
HOLDER = "2e5ce74007fdb30a9f4ffc414ade4ba0e7aa2f4d421bb1f068e2aaf2f7ba3427"
DAEMON_ID = "c1c8410e-1137-4ebf-9eb9-586d0a2b134b"
DAEMON_NAME = "7bd5b54461f4"
FIRST_HEAD = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c"
LATER_HEAD = "1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
CATALOG = "acme-corp/acme-bot"
CUSTOM = "acme-corp/acme-tools"
CUSTOM_REPLY = FIXTURES / "custom-reply.txt"
SECTIONS = ("runs", "cleanup", "control", "upgrade", "ci")
EXPECTED_RULES = {
    "userns-host": "host-namespace",
    "privileged": "privileged",
    "cap-add": "cap-add",
    "host-bind": "host-bind",
    "mount-bind": "host-bind",
    "volume-driver-bind": "volume-driver-options",
    "seccomp-unconfined": "security-opt",
    "exec-privileged": "exec-privileged",
}


def _raw(name: str) -> str:
    return (RAW / f"{name}.out").read_text()


def _rc(name: str) -> int:
    return int((RAW / f"{name}.rc").read_text().strip())


@pytest.fixture
def envelope() -> dict[str, Any]:
    return copy.deepcopy(json.loads((FIXTURES / "passing-envelope.json").read_text()))


@pytest.fixture
def lifecycle() -> dict[str, Any]:
    return json.loads(LIFECYCLE.read_text())


def _assert_prefix(envelope: Any, prefix: str) -> list[str]:
    reasons = docker_scenario.judge(envelope)
    assert any(reason.startswith(f"{prefix}: ") for reason in reasons), (prefix, reasons)
    return reasons


def _escape(envelope: dict[str, Any], run: str, name: str) -> dict[str, Any]:
    return next(e for e in envelope["runs"][run]["probe"]["escapes"] if e["name"] == name)


def _target(envelope: dict[str, Any], run: str, name: str) -> dict[str, Any]:
    targets = envelope["runs"][run]["probe"]["egress"]["targets"]
    return next(t for t in targets if t["name"] == name)


def _check(envelope: dict[str, Any], name: str) -> dict[str, Any]:
    return next(c for c in envelope["ci"]["checks"] if c["name"] == name)


def _run(*argv: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *argv], capture_output=True, text=True, cwd=REPO, env=env, check=False
    )


# passing envelope


def test_passing_envelope_judges_clean(envelope: dict[str, Any]) -> None:
    assert docker_scenario.judge(envelope) == []


def test_constants_match_the_contract() -> None:
    assert docker_scenario.SCHEMA == "docker-scenario/v1"
    assert docker_scenario.NONCE_LABEL == "curie.fixture.nonce"
    assert docker_scenario.LIMIT_SECONDS == 300
    assert docker_scenario.ESCAPES == tuple(EXPECTED_RULES.items())
    assert docker_scenario.EGRESS_TARGETS == ("cluster-api", "curie-api", "metadata")
    assert docker_scenario.METADATA == "169.254.169.254:80"
    assert docker_scenario.CLEANUP_PATHS == ("success", "cancel", "worker_kill", "orphan_sweep")
    assert docker_scenario.DAEMON_KINDS == ("Pod", "Service", "NetworkPolicy", "Secret")


def test_wrong_schema(envelope: dict[str, Any]) -> None:
    envelope["schema"] = "docker-scenario/v0"
    _assert_prefix(envelope, "schema")


# one mutation per judge reason


def _drop_escape(env: dict[str, Any], name: str) -> None:
    probe = env["runs"]["a"]["probe"]
    probe["escapes"] = [e for e in probe["escapes"] if e["name"] != name]


def _drop_holder_event(env: dict[str, Any], action: str) -> None:
    daemon = env["runs"]["a"]["daemon"]
    holder = env["runs"]["a"]["probe"]["holder"]["id"]
    daemon["events"] = [
        e for e in daemon["events"] if not (e["Actor"]["ID"] == holder and e["Action"] == action)
    ]


def _share_daemon_id(env: dict[str, Any]) -> None:
    shared = env["runs"]["a"]["daemon"]["info"]["id"]
    env["runs"]["b"]["daemon"]["info"]["id"] = shared
    env["runs"]["b"]["probe"]["info"]["id"] = shared


def _separate_windows(env: dict[str, Any]) -> None:
    t1 = env["runs"]["a"]["probe"]["window"][1]
    env["runs"]["b"]["probe"]["window"] = [t1 + 10, t1 + 20]


def _drop_kind(env: dict[str, Any], kind: str) -> None:
    path = env["cleanup"]["success"]
    path["objects"] = [o for o in path["objects"] if o["kind"] != kind]


def _custom_rebuild_line(env: dict[str, Any]) -> None:
    env["upgrade"]["plan_lines"].append(
        f"repository runner rebuild: build catalog repository {CUSTOM} in "
        "registry.example.com/acme-corp on registry.example.com/curie/curie-runner during Apply, "
        "then apply its receipt in the single Helm upgrade"
    )


def _drop_lines(env: dict[str, Any], start: str) -> None:
    lines = env["upgrade"]["plan_lines"]
    env["upgrade"]["plan_lines"] = [line for line in lines if not line.startswith(start)]


def _foreign_nonce(env: dict[str, Any]) -> None:
    env["runs"]["a"]["daemon"]["events"][3]["Actor"]["Attributes"][docker_scenario.NONCE_LABEL] = (
        "ffffffff"
    )


def _blank_ids(env: dict[str, Any]) -> None:
    env["runs"]["a"]["probe"]["info"]["id"] = ""
    env["runs"]["a"]["daemon"]["info"]["id"] = ""


def _set(path: tuple[Any, ...], value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(env: dict[str, Any]) -> None:
        node = env
        for key in path[:-1]:
            node = node[key]
        node[path[-1]] = value

    return mutate


MUTATIONS: list[tuple[str, str, Callable[[dict[str, Any]], None]]] = [
    (
        "escape-exit-zero",
        "escape_allowed",
        lambda e: _escape(e, "a", "privileged").update(exit_code=0),
    ),
    (
        "escape-wrong-rule",
        "escape_wrong_rule",
        lambda e: _escape(e, "a", "cap-add").update(rule="privileged"),
    ),
    (
        "escape-no-rule",
        "escape_wrong_rule",
        lambda e: _escape(e, "b", "host-bind").update(rule=None),
    ),
    ("escape-missing", "missing_observation", lambda e: _drop_escape(e, "mount-bind")),
    (
        "holder-not-run",
        "missing_observation",
        _set(("runs", "a", "probe", "holder", "exit_code"), 125),
    ),
    ("event-foreign-nonce", "events_foreign", _foreign_nonce),
    ("holder-no-start", "events_missing", lambda e: _drop_holder_event(e, "start")),
    ("holder-no-create", "events_missing", lambda e: _drop_holder_event(e, "create")),
    (
        "identity-id",
        "identity_mismatch",
        _set(("runs", "a", "daemon", "info", "id"), "00000000-0000-4000-8000-000000000000"),
    ),
    (
        "identity-name",
        "identity_mismatch",
        _set(("runs", "a", "probe", "info", "name"), "someone-else"),
    ),
    (
        "identity-daemon-pod",
        "identity_mismatch",
        _set(("runs", "b", "daemon_pod"), "curie-thread-other"),
    ),
    ("identity-empty-id", "identity_mismatch", _blank_ids),
    (
        "egress-reachable",
        "egress_reachable",
        lambda e: _target(e, "a", "metadata").update(exit_code=0, reachable=True),
    ),
    (
        "egress-probe-failed",
        "egress_probe_failed",
        lambda e: _target(e, "b", "cluster-api").update(exit_code=125),
    ),
    (
        "pull-failed",
        "egress_pull_failed",
        _set(("runs", "a", "probe", "egress", "pull", "exit_code"), 1),
    ),
    *[
        (
            f"remaining-{path}",
            "incomplete_cleanup",
            _set(("cleanup", path, "remaining"), [{"kind": "Pod", "name": "left"}]),
        )
        for path in ("success", "cancel", "worker_kill", "orphan_sweep")
    ],
    ("absent-false", "incomplete_cleanup", _set(("cleanup", "cancel", "absent"), False)),
    (
        "settle-over-limit",
        "incomplete_cleanup",
        _set(("cleanup", "orphan_sweep", "settle_seconds"), 300.1),
    ),
    ("settle-negative", "incomplete_cleanup", _set(("cleanup", "success", "settle_seconds"), -1)),
    ("missing-kind", "incomplete_cleanup", lambda e: _drop_kind(e, "NetworkPolicy")),
    (
        "object-without-uid",
        "incomplete_cleanup",
        lambda e: e["cleanup"]["worker_kill"]["objects"][0].pop("uid"),
    ),
    ("shared-daemon-id", "concurrency_shared_daemon", _share_daemon_id),
    (
        "shared-claim",
        "concurrency_shared_daemon",
        lambda e: e["runs"]["b"].update(claim=e["runs"]["a"]["claim"]),
    ),
    ("windows-apart", "not_concurrent", _separate_windows),
    ("control-docker-env", "control_docker", _set(("control", "docker_env"), True)),
    ("control-daemon-objects", "control_docker", _set(("control", "daemon_objects"), 1)),
    ("control-client-files", "control_docker", _set(("control", "client_docker_files"), True)),
    (
        "catalog-unchanged",
        "upgrade_catalog_not_rebuilt",
        lambda e: e["upgrade"]["catalog"].update(after=e["upgrade"]["catalog"]["before"]),
    ),
    (
        "catalog-tag-not-digest",
        "upgrade_catalog_not_rebuilt",
        _set(
            ("upgrade", "catalog", "after"), "registry.example.com/acme-corp/runner-acme-bot:latest"
        ),
    ),
    (
        "catalog-no-rebuild-line",
        "upgrade_catalog_not_rebuilt",
        lambda e: _drop_lines(e, "repository runner rebuild:"),
    ),
    (
        "custom-changed",
        "upgrade_custom_rebuilt",
        lambda e: e["upgrade"]["custom"].update(after=e["upgrade"]["catalog"]["after"]),
    ),
    ("custom-rebuild-line", "upgrade_custom_not_refused", _custom_rebuild_line),
    (
        "custom-no-preserved-line",
        "upgrade_custom_not_refused",
        lambda e: _drop_lines(e, "custom repository runner preserved:"),
    ),
    (
        "required-check-red",
        "ci_not_green_first_head",
        lambda e: _check(e, "E2E required").update(conclusion="failure"),
    ),
]


@pytest.mark.parametrize(
    ("prefix", "mutate"), [(p, m) for _, p, m in MUTATIONS], ids=[i for i, _, _ in MUTATIONS]
)
def test_mutation_fails_with_reason(
    envelope: dict[str, Any], prefix: str, mutate: Callable[[dict[str, Any]], None]
) -> None:
    mutate(envelope)
    _assert_prefix(envelope, prefix)


def test_every_contract_prefix_has_a_mutation() -> None:
    covered = {prefix for _, prefix, _ in MUTATIONS} | {"schema"}
    assert covered == {
        "schema", "missing_observation", "identity_mismatch", "events_missing",
        "events_foreign", "escape_allowed", "escape_wrong_rule", "egress_reachable",
        "egress_probe_failed", "egress_pull_failed", "incomplete_cleanup",
        "concurrency_shared_daemon", "not_concurrent", "control_docker",
        "upgrade_catalog_not_rebuilt", "upgrade_custom_rebuilt",
        "upgrade_custom_not_refused", "ci_not_green_first_head",
    }  # fmt: skip


def test_escape_reason_names_run_and_escape(envelope: dict[str, Any]) -> None:
    _escape(envelope, "b", "privileged")["exit_code"] = 0
    reasons = _assert_prefix(envelope, "escape_allowed")
    assert any("privileged" in r and re.search(r"\bb\b", r) for r in reasons), reasons


def test_cleanup_reason_names_the_path(envelope: dict[str, Any]) -> None:
    envelope["cleanup"]["worker_kill"]["absent"] = False
    reasons = _assert_prefix(envelope, "incomplete_cleanup")
    assert any("worker_kill" in r for r in reasons), reasons


def test_ci_reason_names_the_check(envelope: dict[str, Any]) -> None:
    _check(envelope, "Fix pin verification")["conclusion"] = "failure"
    reasons = docker_scenario.judge(envelope)
    assert (
        "ci_not_green_first_head: required check 'Fix pin verification' concluded 'failure' "
        "on the first head"
    ) in reasons


# review round 1 amendments (A1 to A4)


@pytest.mark.parametrize(
    "target",
    ["kubernetes.default.svc:443", "10.96.0.1", "10.96.0.1:0", "10.96.0.1:65536", "::1:80", ""],
)
def test_judge_egress_target_must_be_an_ip_port(envelope: dict[str, Any], target: str) -> None:
    _target(envelope, "a", "cluster-api")["target"] = target
    reasons = _assert_prefix(envelope, "egress_probe_failed")
    assert any("cluster-api" in r for r in reasons), reasons


def test_judge_accepts_bracketed_ipv6_egress_target(envelope: dict[str, Any]) -> None:
    _target(envelope, "a", "cluster-api")["target"] = "[fd00::1]:443"
    assert docker_scenario.judge(envelope) == []


@pytest.mark.parametrize("field", ["holder_removed", "volume_absent"])
def test_judge_incomplete_probe_cleanup_names_the_run(envelope: dict[str, Any], field: str) -> None:
    envelope["runs"]["b"]["probe"]["cleanup"][field] = False
    reasons = _assert_prefix(envelope, "incomplete_cleanup")
    assert any("run b probe" in r for r in reasons), reasons


def test_judge_missing_probe_cleanup_fails(envelope: dict[str, Any]) -> None:
    del envelope["runs"]["a"]["probe"]["cleanup"]
    assert docker_scenario.judge(envelope) != []


def test_judge_sub_second_gap_between_windows_is_not_concurrent(
    envelope: dict[str, Any],
) -> None:
    envelope["runs"]["a"]["probe"]["window"] = [90.0, 100.2]
    envelope["runs"]["b"]["probe"]["window"] = [100.3, 110.0]
    _assert_prefix(envelope, "not_concurrent")


def test_judge_sub_second_overlap_is_concurrent(envelope: dict[str, Any]) -> None:
    envelope["runs"]["a"]["probe"]["window"] = [90.0, 100.4]
    envelope["runs"]["b"]["probe"]["window"] = [100.3, 110.0]
    assert docker_scenario.judge(envelope) == []


@pytest.mark.parametrize("nonce", [None, "", 7, ["e8ce1a8c"]], ids=repr)
def test_judge_requires_a_nonempty_string_nonce(envelope: dict[str, Any], nonce: Any) -> None:
    envelope["nonce"] = nonce
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith("missing_observation: nonce") for r in reasons), reasons


def test_judge_missing_nonce_key_is_missing_observation(envelope: dict[str, Any]) -> None:
    del envelope["nonce"]
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith("missing_observation: nonce") for r in reasons), reasons


def test_judge_event_without_the_label_key_is_foreign(envelope: dict[str, Any]) -> None:
    del envelope["runs"]["a"]["daemon"]["events"][3]["Actor"]["Attributes"][
        docker_scenario.NONCE_LABEL
    ]
    _assert_prefix(envelope, "events_foreign")


def test_judge_nonce_and_every_label_removed_together_is_foreign(
    envelope: dict[str, Any],
) -> None:
    del envelope["nonce"]
    for run in envelope["runs"].values():
        for event in run["daemon"]["events"]:
            event["Actor"]["Attributes"].pop(docker_scenario.NONCE_LABEL, None)
    _assert_prefix(envelope, "events_foreign")


def test_empty_events_fail(envelope: dict[str, Any]) -> None:
    envelope["runs"]["a"]["daemon"]["events"] = []
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith(("events_missing: ", "missing_observation: ")) for r in reasons), (
        reasons
    )


def test_missing_egress_target_fails(envelope: dict[str, Any]) -> None:
    probe = envelope["runs"]["a"]["probe"]
    probe["egress"]["targets"] = [t for t in probe["egress"]["targets"] if t["name"] != "curie-api"]
    reasons = docker_scenario.judge(envelope)
    assert any("curie-api" in r for r in reasons), reasons


def test_missing_run_b(envelope: dict[str, Any]) -> None:
    del envelope["runs"]["b"]
    _assert_prefix(envelope, "missing_observation")


@pytest.mark.parametrize("section", SECTIONS)
def test_missing_section(envelope: dict[str, Any], section: str) -> None:
    del envelope[section]
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith(f"missing_observation: {section}") for r in reasons), reasons


@pytest.mark.parametrize("garbage", [None, [], ["x"], "text", 7], ids=repr)
@pytest.mark.parametrize("section", SECTIONS)
def test_garbage_section_never_raises(envelope: dict[str, Any], section: str, garbage: Any) -> None:
    envelope[section] = garbage
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith(f"missing_observation: {section}") for r in reasons), reasons


@pytest.mark.parametrize(
    "path",
    [
        ("runs", "a"),
        ("runs", "a", "probe"),
        ("runs", "a", "daemon"),
        ("runs", "a", "probe", "info"),
        ("runs", "a", "probe", "holder"),
        ("runs", "a", "probe", "escapes"),
        ("runs", "a", "probe", "egress"),
        ("runs", "a", "probe", "egress", "targets"),
        ("runs", "a", "probe", "window"),
        ("runs", "a", "daemon", "events"),
        ("cleanup", "success"),
        ("cleanup", "success", "objects"),
        ("upgrade", "plan_lines"),
        ("upgrade", "catalog"),
        ("ci", "checks"),
    ],
    ids=lambda p: ".".join(p),
)
@pytest.mark.parametrize("garbage", [None, "text", 3, [None]], ids=repr)
def test_nested_garbage_never_raises(
    envelope: dict[str, Any], path: tuple[str, ...], garbage: Any
) -> None:
    _set(path, garbage)(envelope)
    assert docker_scenario.judge(envelope) != []


@pytest.mark.parametrize("garbage", [None, [], "text", 0])
def test_garbage_envelope_never_raises(garbage: Any) -> None:
    assert docker_scenario.judge(garbage) != []


# liveness: the fail-closed branches still pass real-looking data


def test_other_holder_actions_still_pass(envelope: dict[str, Any]) -> None:
    events = envelope["runs"]["a"]["daemon"]["events"]
    extra = copy.deepcopy(next(e for e in events if e["Action"] == "start"))
    extra["Action"] = "exec_create: true"
    events.append(extra)
    assert docker_scenario.judge(envelope) == []


def test_recorded_events_include_non_holder_containers(envelope: dict[str, Any]) -> None:
    events = envelope["runs"]["a"]["daemon"]["events"]
    assert {e["Actor"]["ID"] for e in events} - {HOLDER}
    assert docker_scenario.judge(envelope) == []


def test_neutral_and_skipped_required_checks_pass(envelope: dict[str, Any]) -> None:
    _check(envelope, "E2E required")["conclusion"] = "neutral"
    _check(envelope, "Fix pin verification")["conclusion"] = "skipped"
    assert docker_scenario.judge(envelope) == []


def test_red_check_on_another_head_is_ignored(envelope: dict[str, Any]) -> None:
    envelope["ci"]["checks"].append(
        {
            "name": "E2E required",
            "head_sha": LATER_HEAD,
            "status": "completed",
            "conclusion": "failure",
        }
    )
    assert docker_scenario.judge(envelope) == []


def test_settle_at_the_limit_passes(envelope: dict[str, Any]) -> None:
    envelope["cleanup"]["orphan_sweep"]["settle_seconds"] = 300
    envelope["cleanup"]["success"]["settle_seconds"] = 0
    assert docker_scenario.judge(envelope) == []


def test_pending_combined_status_with_green_check_runs_passes() -> None:
    # status.json is GitHub's combined status for a fully green head: state
    # "pending" with zero statuses, because the head only has check runs.
    combined = json.loads((GITHUB / "status.json").read_text())
    assert combined["state"] == "pending" and combined["statuses"] == []
    ci = docker_scenario.ci_section(
        FIRST_HEAD,
        json.loads((GITHUB / "rules-branches-next.json").read_text()),
        json.loads((GITHUB / "check-runs.json").read_text()),
        combined["statuses"],
    )
    assert fe.judge_first_head_ci(ci) == []


# parsers on the raw recordings


def test_parse_info_on_recordings() -> None:
    runner = docker_scenario.parse_info(_raw("info-runner"))
    daemon = docker_scenario.parse_info(_raw("info-daemon"))
    assert runner == {"id": DAEMON_ID, "name": DAEMON_NAME}
    assert daemon == runner


@pytest.mark.parametrize(
    "text", ["", "\n", "onlyone", "a b c", "Cannot connect to the Docker daemon"]
)
def test_parse_info_rejects_other_shapes(text: str) -> None:
    assert docker_scenario.parse_info(text) == {"id": "", "name": ""}


@pytest.mark.parametrize(("name", "rule"), list(EXPECTED_RULES.items()))
def test_parse_refusal_on_recordings(name: str, rule: str) -> None:
    assert docker_scenario.parse_refusal(_raw(f"escape-{name}")) == rule


@pytest.mark.parametrize("name", ["holder", "pull", "info-runner", "egress-public"])
def test_parse_refusal_none_without_refusal(name: str) -> None:
    assert docker_scenario.parse_refusal(_raw(name)) is None


def test_parse_events_on_recording() -> None:
    events = docker_scenario.parse_events(_raw("events"))
    assert len(events) == 20
    assert all(e["Actor"]["Attributes"]["curie.fixture.nonce"] == NONCE for e in events)
    holder_actions = [e["Action"] for e in events if e["Actor"]["ID"] == HOLDER]
    assert holder_actions == ["create", "start", "kill", "die", "destroy"]


def test_parse_events_skips_blank_lines() -> None:
    text = "\n" + _raw("events").replace("\n", "\n\n") + "\n  \n"
    assert len(docker_scenario.parse_events(text)) == 20
    assert docker_scenario.parse_events("") == []


@pytest.mark.parametrize("line", ["not json", "[1, 2]", '"text"', "{truncated"])
def test_parse_events_rejects_non_object_lines(line: str) -> None:
    with pytest.raises(ValueError):
        docker_scenario.parse_events(_raw("events") + line + "\n")


def test_escape_result_on_recording() -> None:
    output = _raw("escape-privileged")
    assert docker_scenario.escape_result("privileged", _rc("escape-privileged"), output) == {
        "name": "privileged",
        "exit_code": 125,
        "rule": "privileged",
        "output_tail": output,
    }


def test_escape_result_keeps_the_last_400_characters() -> None:
    output = "x" * 1000 + _raw("escape-exec-privileged")
    result = docker_scenario.escape_result("exec-privileged", 1, output)
    assert result["output_tail"] == output[-400:]
    assert result["rule"] == "exec-privileged"


def test_egress_result_shapes() -> None:
    blocked = docker_scenario.egress_result(
        "metadata", "169.254.169.254:80", _rc("egress-metadata"), _raw("egress-metadata")
    )
    assert blocked == {
        "name": "metadata",
        "target": "169.254.169.254:80",
        "exit_code": 1,
        "reachable": False,
        "output_tail": _raw("egress-metadata")[-400:],
    }
    public = docker_scenario.egress_result(
        "public", "192.0.2.80:443", _rc("egress-public"), _raw("egress-public")
    )
    assert public["reachable"] is True and public["exit_code"] == 0
    assert docker_scenario.egress_result("metadata", "169.254.169.254:80", 1)["output_tail"] == ""


def test_egress_result_keeps_the_last_400_characters() -> None:
    output = "y" * 1000 + "nc: connect timed out\n"
    result = docker_scenario.egress_result("metadata", "169.254.169.254:80", 1, output)
    assert result["output_tail"] == output[-400:]


def test_passing_envelope_run_a_is_the_recording(envelope: dict[str, Any]) -> None:
    run = envelope["runs"]["a"]
    assert run["probe"]["info"] == docker_scenario.parse_info(_raw("info-runner"))
    assert run["daemon"]["info"] == docker_scenario.parse_info(_raw("info-daemon"))
    assert run["daemon"]["events"] == docker_scenario.parse_events(_raw("events"))
    assert run["probe"]["holder"] == {"id": _raw("holder").strip(), "exit_code": 0}
    t0, t1 = (int(v) for v in (RAW / "window").read_text().split())
    assert run["probe"]["window"] == [t0, t1]
    assert run["probe"]["escapes"] == [
        docker_scenario.escape_result(name, _rc(f"escape-{name}"), _raw(f"escape-{name}"))
        for name in EXPECTED_RULES
    ]


# escape battery argv (plan table)


def test_escape_commands_match_plan_table() -> None:
    commands = docker_scenario.escape_commands(IMAGE, NONCE, HOLDER)
    assert commands == [
        (
            "userns-host",
            ["docker", "run", "--rm", "--userns=host", IMAGE, "true"],
            "host-namespace",
        ),
        ("privileged", ["docker", "run", "--rm", "--privileged", IMAGE, "true"], "privileged"),
        ("cap-add", ["docker", "run", "--rm", "--cap-add", "SYS_ADMIN", IMAGE, "true"], "cap-add"),
        ("host-bind", ["docker", "run", "--rm", "-v", "/:/host", IMAGE, "true"], "host-bind"),
        (
            "mount-bind",
            ["docker", "run", "--rm", "--mount", "type=bind,source=/,target=/host", IMAGE, "true"],
            "host-bind",
        ),
        (
            "volume-driver-bind",
            [
                "docker",
                "volume",
                "create",
                "--opt",
                "type=none",
                "--opt",
                "o=bind",
                "--opt",
                "device=/",
                f"curie-escape-{NONCE}",
            ],
            "volume-driver-options",
        ),
        (
            "seccomp-unconfined",
            ["docker", "run", "--rm", "--security-opt", "seccomp=unconfined", IMAGE, "true"],
            "security-opt",
        ),
        ("exec-privileged", ["docker", "exec", "--privileged", HOLDER, "true"], "exec-privileged"),
    ]


# from_lifecycle and assemble


def test_from_lifecycle_matches_the_envelope(
    envelope: dict[str, Any], lifecycle: dict[str, Any]
) -> None:
    assert docker_scenario.from_lifecycle(lifecycle) == {
        "cleanup": envelope["cleanup"],
        "control": envelope["control"],
        "lifecycle": envelope["lifecycle"],
    }


def test_from_lifecycle_drops_the_client_secret(lifecycle: dict[str, Any]) -> None:
    cleanup = docker_scenario.from_lifecycle(lifecycle)["cleanup"]
    for path in docker_scenario.CLEANUP_PATHS:
        assert [o["kind"] for o in cleanup[path]["objects"]] == list(docker_scenario.DAEMON_KINDS)


@pytest.mark.parametrize("schema", ["daemon-lifecycle/v0", "docker-scenario/v1", None])
def test_from_lifecycle_rejects_wrong_schema(lifecycle: dict[str, Any], schema: Any) -> None:
    lifecycle = copy.deepcopy(lifecycle)
    lifecycle["schema"] = schema
    with pytest.raises(ValueError):
        docker_scenario.from_lifecycle(lifecycle)


def test_assemble_rebuilds_the_envelope(
    envelope: dict[str, Any], lifecycle: dict[str, Any]
) -> None:
    assembled = docker_scenario.assemble(
        NONCE, envelope["runs"], lifecycle, envelope["upgrade"], envelope["ci"]
    )
    assert assembled == envelope


# upgrade


def _values(catalog_image: str, custom_image: str) -> dict[str, Any]:
    return {
        "factory": {
            "bindings": [
                {
                    "repos": [
                        {
                            "path": "acme-corp/other",
                            "runner": {"image": "unrelated@sha256:" + "0" * 64},
                        }
                    ]
                },
                {
                    "repos": [
                        {"path": CATALOG, "runner": {"image": catalog_image, "tools": ["node"]}},
                        {
                            "path": CUSTOM,
                            "runner": {"image": custom_image, "dockerfile": "Dockerfile"},
                        },
                    ]
                },
            ]
        }
    }


def test_runner_image_finds_the_repo(envelope: dict[str, Any]) -> None:
    upgrade = envelope["upgrade"]
    values = _values(upgrade["catalog"]["before"], upgrade["custom"]["before"])
    assert docker_scenario.runner_image(values, CATALOG) == upgrade["catalog"]["before"]
    assert docker_scenario.runner_image(values, CUSTOM) == upgrade["custom"]["before"]
    assert docker_scenario.runner_image(values, "acme-corp/absent") == ""
    assert docker_scenario.runner_image({}, CATALOG) == ""


def test_upgrade_section_from_plan_and_values(envelope: dict[str, Any]) -> None:
    upgrade = envelope["upgrade"]
    plan = "\n\n".join(f"  {line}  " for line in upgrade["plan_lines"]) + "\n\n"
    section = docker_scenario.upgrade_section(
        plan,
        _values(upgrade["catalog"]["before"], upgrade["custom"]["before"]),
        _values(upgrade["catalog"]["after"], upgrade["custom"]["after"]),
        CATALOG,
        CUSTOM,
        custom_reply=f"\n  {upgrade['custom']['reply']}  \n",
    )
    assert section == upgrade


def test_upgrade_section_custom_reply_defaults_to_empty() -> None:
    section = docker_scenario.upgrade_section("", _values("", ""), _values("", ""), CATALOG, CUSTOM)
    assert section["custom"]["reply"] == ""


def test_cli_upgrade_carries_the_custom_reply(tmp_path: Path, envelope: dict[str, Any]) -> None:
    upgrade = envelope["upgrade"]
    plan = tmp_path / "plan.txt"
    plan.write_text("\n".join(upgrade["plan_lines"]) + "\n")
    before, after = tmp_path / "before.json", tmp_path / "after.json"
    before.write_text(
        json.dumps(_values(upgrade["catalog"]["before"], upgrade["custom"]["before"]))
    )
    after.write_text(json.dumps(_values(upgrade["catalog"]["after"], upgrade["custom"]["after"])))
    args = [
        str(SCRIPT), "upgrade", "--plan", str(plan), "--before", str(before),
        "--after", str(after), "--catalog-repo", CATALOG, "--custom-repo", CUSTOM,
    ]  # fmt: skip
    missing = _run(*args)
    assert missing.returncode == 2
    assert "--custom-reply" in missing.stderr
    out = tmp_path / "upgrade.json"
    done = _run(*args, "--custom-reply", str(CUSTOM_REPLY), "--out", str(out))
    assert done.returncode == 0, done.stderr
    assert json.loads(out.read_text()) == upgrade


# custom runner refusal reply (A7)


def test_custom_reply_fixture_is_the_envelope_reply(envelope: dict[str, Any]) -> None:
    assert CUSTOM_REPLY.read_text().strip() == envelope["upgrade"]["custom"]["reply"]


def _other_repo_reply(reply: str) -> str:
    return reply.replace(CUSTOM, CATALOG)


MISSING_POOL_REPLY = (
    f"This agent cannot start: repository {CUSTOM} is bound to runner pool runner-acme-tools, "
    "but SandboxWarmPool runner-acme-tools was not rendered (missing_pool); redeploy its "
    f"runner with `curie cluster factory --repo {CUSTOM}`. An operator has to make that change."
)


@pytest.mark.parametrize(
    "mutate",
    [
        _other_repo_reply,
        lambda reply: MISSING_POOL_REPLY,
        lambda reply: "",
        lambda reply: reply + " Please retry later.",
        lambda reply: "Sorry. " + reply,
    ],
    ids=["other-repo", "missing-pool", "empty", "trailing-text", "leading-text"],
)
def test_judge_custom_reply_must_be_the_stale_base_refusal(
    envelope: dict[str, Any], mutate: Callable[[str], str]
) -> None:
    envelope["upgrade"]["custom"]["reply"] = mutate(envelope["upgrade"]["custom"]["reply"])
    _assert_prefix(envelope, "upgrade_custom_not_refused")


@pytest.mark.parametrize("missing", [True, False], ids=["absent", "null"])
def test_judge_missing_custom_reply_is_not_refused(envelope: dict[str, Any], missing: bool) -> None:
    if missing:
        del envelope["upgrade"]["custom"]["reply"]
    else:
        envelope["upgrade"]["custom"]["reply"] = None
    _assert_prefix(envelope, "upgrade_custom_not_refused")


# lifecycle evidence (A8)


def test_from_lifecycle_reports_the_lifecycle_judge(lifecycle: dict[str, Any]) -> None:
    assert docker_scenario.from_lifecycle(lifecycle)["lifecycle"] == {
        "schema": "daemon-lifecycle/v1",
        "failures": [],
    }


def _failing_lifecycle(lifecycle: dict[str, Any]) -> dict[str, Any]:
    lifecycle = copy.deepcopy(lifecycle)
    lifecycle["turns"]["cancel"]["request_state"] = "completed"
    return lifecycle


def test_from_lifecycle_lists_the_lifecycle_failures(lifecycle: dict[str, Any]) -> None:
    result = docker_scenario.from_lifecycle(_failing_lifecycle(lifecycle))["lifecycle"]
    assert result["schema"] == "daemon-lifecycle/v1"
    assert result["failures"]
    assert all(isinstance(f, str) for f in result["failures"])


def test_judge_assembled_failing_lifecycle_is_incomplete_cleanup(
    envelope: dict[str, Any], lifecycle: dict[str, Any]
) -> None:
    assembled = docker_scenario.assemble(
        NONCE, envelope["runs"], _failing_lifecycle(lifecycle), envelope["upgrade"], envelope["ci"]
    )
    reasons = _assert_prefix(assembled, "incomplete_cleanup")
    assert any(r.startswith("incomplete_cleanup: ") and "lifecycle" in r for r in reasons)


def test_judge_lifecycle_failures_are_incomplete_cleanup(envelope: dict[str, Any]) -> None:
    envelope["lifecycle"]["failures"] = ["turns.cancel: request ended completed"]
    reasons = _assert_prefix(envelope, "incomplete_cleanup")
    assert any("lifecycle" in r for r in reasons if r.startswith("incomplete_cleanup: "))


@pytest.mark.parametrize(
    "value", [None, "text", [], {}, {"schema": "daemon-lifecycle/v1"}], ids=repr
)
def test_judge_non_dict_or_failureless_lifecycle_is_missing_observation(
    envelope: dict[str, Any], value: Any
) -> None:
    envelope["lifecycle"] = value
    _assert_prefix(envelope, "missing_observation")
    assert any(
        r.startswith("missing_observation: lifecycle") for r in docker_scenario.judge(envelope)
    )


def test_judge_absent_lifecycle_is_missing_observation(envelope: dict[str, Any]) -> None:
    del envelope["lifecycle"]
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith("missing_observation: lifecycle") for r in reasons), reasons


@pytest.mark.parametrize("failures", [None, "none", {"a": 1}, 0], ids=repr)
def test_judge_non_list_lifecycle_failures_is_missing_observation(
    envelope: dict[str, Any], failures: Any
) -> None:
    envelope["lifecycle"]["failures"] = failures
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith("missing_observation: lifecycle") for r in reasons), reasons


# probe windows (A9)


@pytest.mark.parametrize(
    "window",
    [
        [20, 10],
        [float("nan"), 1],
        [1, float("inf")],
        [True, 2],
        [1],
        [1, 2, 3],
        "10-20",
        None,
    ],
    ids=repr,
)
@pytest.mark.parametrize("run", ["a", "b"])
def test_judge_invalid_window_is_missing_observation(
    envelope: dict[str, Any], run: str, window: Any
) -> None:
    envelope["runs"][run]["probe"]["window"] = window
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith(f"missing_observation: run {run} window") for r in reasons), reasons
    assert not any(r.startswith("not_concurrent") for r in reasons), reasons


def test_judge_zero_length_window_is_valid(envelope: dict[str, Any]) -> None:
    envelope["runs"]["a"]["probe"]["window"] = [5, 5]
    envelope["runs"]["b"]["probe"]["window"] = [0, 10]
    assert docker_scenario.judge(envelope) == []


# ci section


def _github(name: str) -> Any:
    return json.loads((GITHUB / name).read_text())


def test_ci_section_from_github_fixtures(envelope: dict[str, Any]) -> None:
    ci = docker_scenario.ci_section(
        FIRST_HEAD, _github("rules-branches-next.json"), _github("check-runs.json"), []
    )
    assert ci == envelope["ci"]
    assert ci["required"] == [
        "Compose (release stack validates)",
        "Contracts (generated TypeScript compiles)",
        "Python (ruff + mypy + pytest)",
        "Rust (fmt + clippy + test)",
        "UI (lint + vitest + build + Playwright)",
        "gitleaks (full history)",
        "E2E required",
        "Fix pin verification",
        "Chart (lint + template + kubeconform)",
    ]
    assert len(ci["checks"]) == 65


def test_ci_section_explicit_required_wins() -> None:
    ci = docker_scenario.ci_section(
        FIRST_HEAD,
        _github("rules-branches-next.json"),
        _github("check-runs.json"),
        [],
        required=["E2E required"],
    )
    assert ci["required"] == ["E2E required"]
    ci = docker_scenario.ci_section(FIRST_HEAD, None, _github("check-runs.json"), [], ["CodeQL"])
    assert ci["required"] == ["CodeQL"]


def test_ci_section_normalizes_statuses() -> None:
    statuses = [
        {"context": "ok", "state": "success"},
        {"context": "red", "state": "failure"},
        {"context": "broken", "state": "error"},
        {"context": "waiting", "state": "pending"},
    ]
    ci = docker_scenario.ci_section(FIRST_HEAD, [], {"total_count": 0, "check_runs": []}, statuses)
    assert ci["checks"] == [
        {"name": "ok", "head_sha": FIRST_HEAD, "status": "completed", "conclusion": "success"},
        {"name": "red", "head_sha": FIRST_HEAD, "status": "completed", "conclusion": "failure"},
        {"name": "broken", "head_sha": FIRST_HEAD, "status": "completed", "conclusion": "failure"},
        {"name": "waiting", "head_sha": FIRST_HEAD, "status": "queued", "conclusion": None},
    ]
    assert ci["required"] == []


def test_ci_section_without_rules_reads_no_required_checks() -> None:
    empty: dict[str, Any] = {"check_runs": []}
    assert docker_scenario.ci_section("h", None, empty, []) == {
        "first_head": "h",
        "required": [],
        "checks": [],
    }
    assert docker_scenario.ci_section("h", None, empty, [], required=["x"])["required"] == ["x"]


# first-head CI judge (selected by -k first_head)


def test_first_head_ci_survives_unhashable_conclusion(envelope: dict[str, Any]) -> None:
    checks = [
        {"name": "a", "head_sha": "h", "status": "completed", "conclusion": "failure"},
        {"name": "a", "head_sha": "h", "status": "completed", "conclusion": []},
    ]
    obs = {"first_head": "h", "required": ["a"], "checks": checks}
    assert fe.judge_first_head_ci(obs) == [
        "required check 'a' concluded 'failure' on the first head"
    ]
    envelope["ci"]["checks"] = checks
    reasons = docker_scenario.judge(envelope)
    assert any(r.startswith("ci_not_green_first_head: ") for r in reasons), reasons
    assert not any(r.startswith("missing_observation: ci") for r in reasons), reasons


def _green(name: str, head: str = FIRST_HEAD, conclusion: str = "success") -> dict[str, Any]:
    return {"name": name, "head_sha": head, "status": "completed", "conclusion": conclusion}


def test_first_head_ci_passes_when_required_checks_are_green() -> None:
    obs = {"first_head": FIRST_HEAD, "required": ["a", "b"], "checks": [_green("a"), _green("b")]}
    assert fe.judge_first_head_ci(obs) == []


def test_first_head_ci_missing_check() -> None:
    obs = {"first_head": FIRST_HEAD, "required": ["a", "b"], "checks": [_green("a")]}
    assert fe.judge_first_head_ci(obs) == ["required check 'b' has no result on the first head"]


def test_first_head_ci_failed_check() -> None:
    obs = {
        "first_head": FIRST_HEAD,
        "required": ["a"],
        "checks": [_green("a", conclusion="failure")],
    }
    assert fe.judge_first_head_ci(obs) == [
        "required check 'a' concluded 'failure' on the first head"
    ]


def test_first_head_ci_pending_check() -> None:
    check = {"name": "a", "head_sha": FIRST_HEAD, "status": "in_progress", "conclusion": None}
    obs = {"first_head": FIRST_HEAD, "required": ["a"], "checks": [check]}
    assert fe.judge_first_head_ci(obs) == ["required check 'a' concluded None on the first head"]


def test_first_head_ci_green_only_on_later_head_does_not_count() -> None:
    checks = [_green("a", conclusion="failure"), _green("a", head=LATER_HEAD)]
    obs = {
        "first_head": FIRST_HEAD,
        "required": ["a", "b"],
        "checks": checks + [_green("b", head=LATER_HEAD)],
    }
    assert fe.judge_first_head_ci(obs) == [
        "required check 'a' concluded 'failure' on the first head",
        "required check 'b' has no result on the first head",
    ]


def test_first_head_ci_empty_required() -> None:
    obs = {"first_head": FIRST_HEAD, "required": [], "checks": [_green("a")]}
    assert fe.judge_first_head_ci(obs) == ["no required checks were recorded"]


@pytest.mark.parametrize("head", [None, ""])
def test_first_head_ci_no_first_head(head: Any) -> None:
    obs = {"first_head": head, "required": ["a"], "checks": [_green("a")]}
    assert fe.judge_first_head_ci(obs) == ["no first published head was recorded"]


def test_first_head_ci_neutral_and_skipped_pass() -> None:
    checks = [_green("a", conclusion="neutral"), _green("b", conclusion="skipped")]
    obs = {"first_head": FIRST_HEAD, "required": ["a", "b"], "checks": checks}
    assert fe.judge_first_head_ci(obs) == []


def test_first_head_ci_success_and_failure_on_the_first_head_fails() -> None:
    for checks in (
        [_green("a", conclusion="failure"), _green("a")],
        [_green("a"), _green("a", conclusion="failure")],
    ):
        obs = {"first_head": FIRST_HEAD, "required": ["a"], "checks": checks}
        assert fe.judge_first_head_ci(obs) == [
            "required check 'a' concluded 'failure' on the first head"
        ]


def test_first_head_ci_names_the_first_non_green_conclusion() -> None:
    checks = [_green("a"), _green("a", conclusion="timed_out"), _green("a", conclusion="failure")]
    obs = {"first_head": FIRST_HEAD, "required": ["a"], "checks": checks}
    assert fe.judge_first_head_ci(obs) == [
        "required check 'a' concluded 'timed_out' on the first head"
    ]


def test_first_head_ci_section_keeps_the_newest_status_per_context() -> None:
    empty = {"total_count": 0, "check_runs": []}
    newest_first = [
        {"context": "legacy/status", "state": "success"},
        {"context": "legacy/status", "state": "failure"},
    ]
    ci = docker_scenario.ci_section(FIRST_HEAD, None, empty, newest_first, ["legacy/status"])
    assert [c["conclusion"] for c in ci["checks"]] == ["success"]
    assert fe.judge_first_head_ci(ci) == []
    ci = docker_scenario.ci_section(FIRST_HEAD, None, empty, newest_first[::-1], ["legacy/status"])
    assert [c["conclusion"] for c in ci["checks"]] == ["failure"]
    assert fe.judge_first_head_ci(ci) == [
        "required check 'legacy/status' concluded 'failure' on the first head"
    ]


def test_first_head_ci_section_dedupes_statuses_across_contexts() -> None:
    empty = {"total_count": 0, "check_runs": []}
    statuses = [
        {"context": "one", "state": "pending"},
        {"context": "two", "state": "success"},
        {"context": "one", "state": "success"},
        {"context": "two", "state": "error"},
    ]
    ci = docker_scenario.ci_section(FIRST_HEAD, None, empty, statuses, ["one", "two"])
    assert [(c["name"], c["status"], c["conclusion"]) for c in ci["checks"]] == [
        ("one", "queued", None),
        ("two", "completed", "success"),
    ]


def test_first_head_ci_status_context_counts() -> None:
    ci = docker_scenario.ci_section(
        FIRST_HEAD,
        None,
        {"total_count": 0, "check_runs": []},
        [{"context": "legacy/status", "state": "success"}],
        required=["legacy/status"],
    )
    assert fe.judge_first_head_ci(ci) == []


def test_first_head_ci_on_recorded_github_shapes(envelope: dict[str, Any]) -> None:
    assert fe.judge_first_head_ci(envelope["ci"]) == []


# CLI through the real entrypoints


def _write(tmp_path: Path, envelope: Any) -> Path:
    path = tmp_path / "envelope.json"
    path.write_text(json.dumps(envelope))
    return path


def test_help_names_every_section_and_the_w2_waypoint() -> None:
    result = _run(str(SCRIPT), "--help")
    assert result.returncode == 0, result.stderr
    text = " ".join(result.stdout.split())
    for section in SECTIONS:
        assert section in text, section
    assert "w2 waypoint" in text


def test_cli_judge_passing(tmp_path: Path) -> None:
    result = _run(str(SCRIPT), "judge", str(FIXTURES / "passing-envelope.json"))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"result": "passed", "failures": []}


def test_cli_judge_failing(tmp_path: Path, envelope: dict[str, Any]) -> None:
    envelope["control"]["docker_env"] = True
    result = _run(str(SCRIPT), "judge", str(_write(tmp_path, envelope)))
    assert result.returncode == 1, result.stderr
    out = json.loads(result.stdout)
    assert out["result"] == "failed"
    assert any(f.startswith("control_docker: ") for f in out["failures"])


def test_cli_judge_missing_file(tmp_path: Path) -> None:
    result = _run(str(SCRIPT), "judge", str(tmp_path / "absent.json"))
    assert result.returncode == 2


def test_cli_collect_requires_context() -> None:
    result = _run(
        str(SCRIPT), "collect", "--namespace", "ns", "--pod", "p", "--nonce", NONCE,
        "--since", "1", "--until", "2",
    )  # fmt: skip
    assert result.returncode == 2
    assert "--context" in result.stderr


def test_factory_e2e_judge_docker(tmp_path: Path, envelope: dict[str, Any]) -> None:
    passing = _run(
        str(FACTORY), "judge-docker", "--envelope", str(FIXTURES / "passing-envelope.json")
    )
    assert passing.returncode == 0, passing.stderr
    assert json.loads(passing.stdout)["result"] == "passed"
    _escape(envelope, "a", "seccomp-unconfined")["exit_code"] = 0
    failing = _run(str(FACTORY), "judge-docker", "--envelope", str(_write(tmp_path, envelope)))
    assert failing.returncode == 1, failing.stderr
    assert any(f.startswith("escape_allowed: ") for f in json.loads(failing.stdout)["failures"])


# probe and collect against fakes that replay the raw recordings

# The fake replays the recorded bytes from fixtures/docker-scenario/raw: it
# picks the recording by argv, writes the .out content (stderr on a nonzero
# exit, as the real CLI does), logs its argv, and exits with the .rc code.
FAKE_DOCKER = r"""
import json, os, sys
from pathlib import Path
raw = Path(os.environ["FAKE_RAW"])
args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
joined = " ".join(args)
if args[:1] == ["info"]:
    name = "info-runner"
elif args[:1] == ["pull"]:
    name = "pull"
elif args[:2] == ["volume", "create"]:
    name = "escape-volume-driver-bind"
elif args[:2] == ["volume", "rm"]:
    name = "volume-rm"
elif args[:2] == ["volume", "ls"]:
    sys.stdout.write(os.environ.get("FAKE_VOLUME_LS", ""))
    sys.exit(0)
elif args[:1] == ["ps"]:
    sys.stdout.write(os.environ.get("FAKE_PS_OUT", ""))
    sys.exit(0)
elif args[:1] == ["exec"]:
    name = "escape-exec-privileged"
elif args[:1] == ["rm"]:
    name = "holder-rm"
elif args[:1] == ["run"] and "-d" in args:
    if os.environ.get("FAKE_HOLDER_FAIL"):
        sys.stderr.write("docker: Error response from daemon: holder create failed\n")
        sys.exit(125)
    name = "holder"
elif args[:1] == ["run"] and "--userns=host" in args:
    name = "escape-userns-host"
elif args[:1] == ["run"] and "--privileged" in args:
    name = "escape-privileged"
elif args[:1] == ["run"] and "--cap-add" in args:
    name = "escape-cap-add"
elif args[:1] == ["run"] and "-v" in args:
    name = "escape-host-bind"
elif args[:1] == ["run"] and "--mount" in args:
    name = "escape-mount-bind"
elif args[:1] == ["run"] and "--security-opt" in args:
    name = "escape-seccomp-unconfined"
elif args[:1] == ["run"] and "169.254.169.254" in joined:
    name = "egress-metadata"
elif args[:1] == ["run"]:
    name = "egress-testnet"
else:
    sys.stderr.write("fake docker: unexpected argv " + joined + "\n")
    sys.exit(99)
code = int((raw / (name + ".rc")).read_text().strip())
(sys.stdout if code == 0 else sys.stderr).write((raw / (name + ".out")).read_text())
sys.exit(code)
"""

FAKE_KUBECTL = r"""
import json, os, sys
from pathlib import Path
raw = Path(os.environ["FAKE_RAW"])
args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if "get" in args and "pod" in args:
    pod = args[args.index("pod") + 1]
    print(json.dumps({"kind": "Pod", "metadata": {"name": pod, "uid": os.environ["FAKE_UID"]}}))
    sys.exit(0)
if "exec" in args and "--" in args:
    docker = args[args.index("--") + 1:]
    name = {"info": "info-daemon", "events": "events"}.get(docker[1] if len(docker) > 1 else "")
    if docker[:1] == ["docker"] and name:
        sys.stdout.write((raw / (name + ".out")).read_text())
        sys.exit(int((raw / (name + ".rc")).read_text().strip()))
sys.stderr.write("fake kubectl: unexpected argv " + " ".join(args) + "\n")
sys.exit(99)
"""


@pytest.fixture
def fake_bin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    for name, body in (("docker", FAKE_DOCKER), ("kubectl", FAKE_KUBECTL)):
        exe = bin_dir / name
        exe.write_text(f"#!{sys.executable}\n{body}")
        exe.chmod(0o755)
    log = tmp_path / "argv.log"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RAW", str(RAW))
    monkeypatch.setenv("FAKE_LOG", str(log))
    monkeypatch.setenv("FAKE_UID", "8a3f2c1e-4b7d-4e90-9a61-2f5c8d0b7e14")
    yield log


def _logged(log: Path) -> list[list[str]]:
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_probe_replays_recordings(tmp_path: Path, fake_bin: Path, envelope: dict[str, Any]) -> None:
    out = tmp_path / "probe.json"
    code = docker_scenario.main(
        [
            "probe", "--nonce", NONCE, "--image", IMAGE,
            "--cluster-api", "10.96.0.1:443", "--api", "10.96.112.40:8080",
            "--out", str(out),
        ]
    )  # fmt: skip
    assert code == 0
    probe = json.loads(out.read_text())
    recorded = envelope["runs"]["a"]["probe"]
    assert probe["info"] == recorded["info"]
    assert probe["holder"] == recorded["holder"]
    assert [(e["name"], e["exit_code"], e["rule"]) for e in probe["escapes"]] == [
        (e["name"], e["exit_code"], e["rule"]) for e in recorded["escapes"]
    ]
    assert probe["egress"]["pull"] == recorded["egress"]["pull"]
    targets = [
        {k: v for k, v in t.items() if k != "output_tail"} for t in probe["egress"]["targets"]
    ]
    assert targets == recorded["egress"]["targets"]
    for target in probe["egress"]["targets"]:
        raw = "egress-metadata" if target["name"] == "metadata" else "egress-testnet"
        assert target["output_tail"] == _raw(raw)[-400:], target["name"]
    t0, t1 = probe["window"]
    assert isinstance(t0, float) and isinstance(t1, float) and t0 <= t1
    assert probe["cleanup"] == {"holder_removed": True, "volume_absent": True}

    calls = _logged(fake_bin)
    for _, argv, _ in docker_scenario.escape_commands(IMAGE, NONCE, HOLDER):
        assert argv[1:] in calls, argv
    assert ["info", "--format", "{{.ID}} {{.Name}}"] in calls
    holder_run = next(c for c in calls if c[:1] == ["run"] and "-d" in c)
    assert f"curie.fixture.nonce={NONCE}" in holder_run and holder_run[-2:] == ["sleep", "300"]
    name = holder_run[holder_run.index("--name") + 1]
    assert re.fullmatch(rf"curie-probe-{NONCE}-[0-9a-f]{{6}}", name), name
    assert ["rm", "-f", name] in calls
    assert ["volume", "rm", "-f", f"curie-escape-{NONCE}"] in calls
    listings = [c for c in calls if c[:1] == ["ps"] or c[:2] == ["volume", "ls"]]
    assert any(c[:1] == ["ps"] and f"name=^{name}$" in c for c in listings), listings
    assert any(f"name=^curie-escape-{NONCE}$" in c for c in listings if c[:2] == ["volume", "ls"])


def _probe_args(out: Path, **targets: str) -> list[str]:
    given = {"cluster-api": "10.96.0.1:443", "api": "10.96.112.40:8080", **targets}
    argv = ["probe", "--nonce", NONCE, "--image", IMAGE, "--out", str(out)]
    for flag, value in given.items():
        argv += [f"--{flag}", value]
    return argv


def test_probe_cleans_up_by_name_when_the_holder_never_starts(
    tmp_path: Path, fake_bin: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_HOLDER_FAIL", "1")
    docker_scenario.main(_probe_args(tmp_path / "probe.json"))
    calls = _logged(fake_bin)
    holder_run = next(c for c in calls if c[:1] == ["run"] and "-d" in c)
    name = holder_run[holder_run.index("--name") + 1]
    assert ["rm", "-f", name] in calls
    assert ["volume", "rm", "-f", f"curie-escape-{NONCE}"] in calls


@pytest.mark.parametrize(
    ("env", "field"), [("FAKE_PS_OUT", "holder_removed"), ("FAKE_VOLUME_LS", "volume_absent")]
)
def test_probe_exits_1_when_cleanup_is_not_verified(
    tmp_path: Path,
    fake_bin: Path,
    monkeypatch: pytest.MonkeyPatch,
    env: str,
    field: str,
) -> None:
    monkeypatch.setenv(env, "9f8e7d6c5b4a\n")
    out = tmp_path / "probe.json"
    assert docker_scenario.main(_probe_args(out)) == 1
    cleanup = json.loads(out.read_text())["cleanup"]
    assert cleanup[field] is False
    assert [v for k, v in cleanup.items() if k != field] == [True]


@pytest.mark.parametrize("flag", ["cluster-api", "api", "metadata"])
@pytest.mark.parametrize(
    "bad",
    [
        "kubernetes.default.svc:443",
        "10.0.0.1",
        "10.0.0.1:0",
        "10.0.0.1:65536",
        "10.0.0.1:x",
        "::1:80",
    ],
)
def test_probe_rejects_non_ip_targets(tmp_path: Path, fake_bin: Path, flag: str, bad: str) -> None:
    with pytest.raises(SystemExit) as exc:
        docker_scenario.main(_probe_args(tmp_path / "probe.json", **{flag: bad}))
    assert exc.value.code == 2
    assert not (tmp_path / "probe.json").exists()
    assert not fake_bin.exists() or not _logged(fake_bin)


def test_probe_accepts_ipv4_and_bracketed_ipv6_targets(tmp_path: Path, fake_bin: Path) -> None:
    out = tmp_path / "probe.json"
    argv = _probe_args(out, **{"cluster-api": "[fd00::1]:443", "metadata": "169.254.169.254:80"})
    assert docker_scenario.main(argv) == 0
    targets = {t["name"]: t["target"] for t in json.loads(out.read_text())["egress"]["targets"]}
    assert targets["cluster-api"] == "[fd00::1]:443"


def test_collect_replays_recordings(
    tmp_path: Path, fake_bin: Path, envelope: dict[str, Any]
) -> None:
    out = tmp_path / "daemon.json"
    code = docker_scenario.main(
        [
            "collect", "--context", "scratch-ctx", "--namespace", "fd-proof-d-e8ce1a8c",
            "--pod", DAEMON_NAME, "--nonce", NONCE,
            "--since", "1792669432", "--until", "1792669451", "--out", str(out),
        ]
    )  # fmt: skip
    assert code == 0
    assert json.loads(out.read_text()) == envelope["runs"]["a"]["daemon"]

    calls = _logged(fake_bin)
    assert calls
    for call in calls:
        assert call[call.index("--context") + 1] == "scratch-ctx", call
        assert call[call.index("-n") + 1] == "fd-proof-d-e8ce1a8c", call
    execs = [c for c in calls if "exec" in c]
    assert len(execs) >= 2
    for call in execs:
        assert call[call.index("-c") + 1] == "dockerd", call
        assert DAEMON_NAME in call[: call.index("--")]
    events = next(c for c in execs if "events" in c)
    assert events[events.index("--since") + 1] == "1792669432"
    assert events[events.index("--until") + 1] == "1792669451"
    assert f"label=curie.fixture.nonce={NONCE}" in events
    assert any("get" in c and "pod" in c and DAEMON_NAME in c for c in calls)


# ci against a local GitHub stand-in serving the recorded REST shapes


@pytest.fixture
def github_server() -> Iterator[tuple[str, list[str]]]:
    requested: list[str] = []
    base = f"/repos/{CATALOG}"
    routes = {
        f"{base}/rules/branches/next": (GITHUB / "rules-branches-next.json").read_bytes(),
        f"{base}/commits/{FIRST_HEAD}/check-runs?per_page=100&filter=latest": (
            GITHUB / "check-runs.json"
        ).read_bytes(),
        # The statuses list for the head; the recorded combined status carries
        # the same (empty) list.
        f"{base}/commits/{FIRST_HEAD}/statuses?per_page=100": json.dumps(
            json.loads((GITHUB / "status.json").read_text())["statuses"]
        ).encode(),
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            requested.append(self.path)
            body = routes.get(self.path)
            self.send_response(200 if body is not None else 404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body if body is not None else b'{"message": "Not Found"}')

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", requested
    finally:
        server.shutdown()
        server.server_close()


def test_ci_collects_from_github(
    tmp_path: Path,
    github_server: tuple[str, list[str]],
    envelope: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    url, requested = github_server
    out = tmp_path / "ci.json"
    code = docker_scenario.main(
        [
            "ci",
            "--api-url",
            url,
            "--repo",
            CATALOG,
            "--base",
            "next",
            "--sha",
            FIRST_HEAD,
            "--out",
            str(out),
        ]
    )
    assert code == 0
    assert json.loads(out.read_text()) == envelope["ci"]
    assert sorted(requested) == sorted(
        [
            f"/repos/{CATALOG}/rules/branches/next",
            f"/repos/{CATALOG}/commits/{FIRST_HEAD}/check-runs?per_page=100&filter=latest",
            f"/repos/{CATALOG}/commits/{FIRST_HEAD}/statuses?per_page=100",
        ]
    )


def test_ci_with_explicit_required_skips_rules(
    tmp_path: Path, github_server: tuple[str, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    url, requested = github_server
    out = tmp_path / "ci.json"
    code = docker_scenario.main(
        [
            "ci", "--api-url", url, "--repo", CATALOG, "--base", "next", "--sha", FIRST_HEAD,
            "--required", "E2E required", "--required", "CodeQL", "--out", str(out),
        ]
    )  # fmt: skip
    assert code == 0
    assert json.loads(out.read_text())["required"] == ["E2E required", "CodeQL"]
    assert not any("/rules/" in path for path in requested)


@pytest.fixture
def paged_github_server() -> Iterator[tuple[str, list[str]]]:
    """Check runs and statuses each split over two pages joined by Link headers."""

    requested: list[str] = []
    routes: dict[str, tuple[bytes, str]] = {}
    commit = f"/repos/{CATALOG}/commits/{FIRST_HEAD}"

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            requested.append(self.path)
            body, link = routes.get(self.path, (b'{"message": "Not Found"}', ""))
            self.send_response(200 if self.path in routes else 404)
            self.send_header("Content-Type", "application/json")
            if link:
                self.send_header("Link", link)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    url = f"http://127.0.0.1:{server.server_address[1]}"
    runs = json.loads((GITHUB / "check-runs.json").read_text())["check_runs"]

    def page(rel_path: str, number: int) -> str:
        return f"<{url}{rel_path}&page={number}>"

    runs_path = f"{commit}/check-runs?per_page=100&filter=latest"
    statuses_path = f"{commit}/statuses?per_page=100"
    for path, first, second in (
        (
            runs_path,
            {"total_count": len(runs), "check_runs": runs[:30]},
            {"total_count": len(runs), "check_runs": runs[30:]},
        ),
        (
            statuses_path,
            [{"context": "legacy/one", "state": "success"}],
            [{"context": "legacy/two", "state": "success"}],
        ),
    ):
        routes[path] = (
            json.dumps(first).encode(),
            f'{page(path, 2)}; rel="next", {page(path, 2)}; rel="last"',
        )
        routes[f"{path}&page=2"] = (
            json.dumps(second).encode(),
            f'{page(path, 1)}; rel="prev", {page(path, 1)}; rel="first"',
        )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield url, requested
    finally:
        server.shutdown()
        server.server_close()


def test_ci_follows_link_headers_for_check_runs_and_statuses(
    tmp_path: Path,
    paged_github_server: tuple[str, list[str]],
    envelope: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    url, requested = paged_github_server
    out = tmp_path / "ci.json"
    code = docker_scenario.main(
        [
            "ci", "--api-url", url, "--repo", CATALOG, "--base", "next", "--sha", FIRST_HEAD,
            "--required", "E2E required", "--required", "legacy/two", "--out", str(out),
        ]
    )  # fmt: skip
    assert code == 0
    names = [c["name"] for c in json.loads(out.read_text())["checks"]]
    expected = [c["name"] for c in envelope["ci"]["checks"]]
    assert names == expected + ["legacy/one", "legacy/two"]
    assert len(requested) == 4, requested
    assert sum("page=2" in path for path in requested) == 2, requested

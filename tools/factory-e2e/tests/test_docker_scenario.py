"""Offline checks for the docker scenario harness (ADR 0216, ADR 0217)."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import docker_scenario
import factory_e2e as fe
import pytest
import yaml

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
SECTIONS = ("runs", "cleanup", "control", "upgrade", "ci", "compose", "skill_rung")
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
        NONCE,
        envelope["runs"],
        lifecycle,
        envelope["upgrade"],
        envelope["ci"],
        envelope["compose"],
        envelope["skill_rung"],
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
        NONCE,
        envelope["runs"],
        _failing_lifecycle(lifecycle),
        envelope["upgrade"],
        envelope["ci"],
        envelope["compose"],
        envelope["skill_rung"],
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


def test_cli_collect_requires_the_runner_namespace_and_pod() -> None:
    result = _run(
        str(SCRIPT), "collect", "--context", "ctx", "--namespace", "ns", "--pod", "p",
        "--nonce", NONCE, "--since", "1", "--until", "2",
    )  # fmt: skip
    assert result.returncode == 2
    assert "--runner-namespace" in result.stderr and "--runner-pod" in result.stderr


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


# The kubectl fake replays the recorded objects in raw/ (daemon-pod.json,
# daemon-service.json, runner-pod.json) by namespace and name, serves the client
# Secret one jsonpath key at a time from the test's FAKE_SECRET file, and runs every
# `exec ... -- sh -c SCRIPT` for real with the dockerd shims below first on PATH and
# TMPDIR under the test's tmp_path, so the staging, the private mount and the
# removal scripts execute and their effects can be observed. It appends kubectl's
# "command terminated" line on a nonzero exec, as the real CLI does.
FAKE_KUBECTL = r"""
import io, json, os, subprocess, sys, tarfile
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
ns = args[args.index("-n") + 1]
rest = args[args.index("-n") + 2:]
objects = {}
for env in ("FAKE_DAEMON_POD", "FAKE_SERVICE", "FAKE_RUNNER_POD"):
    obj = json.loads(Path(os.environ[env]).read_text())
    meta = obj["metadata"]
    objects[(obj["kind"].lower(), meta["namespace"], meta["name"])] = obj
secret = json.loads(Path(os.environ["FAKE_SECRET"]).read_text())
def fail(text, code=1):
    sys.stderr.write(text + "\n")
    sys.exit(code)
if rest[:1] == ["get"] and rest[1] in ("pod", "service"):
    found = objects.get((rest[1], ns, rest[2]))
    if found is None or rest[3:] != ["-o", "json"]:
        fail(f'Error from server (NotFound): {rest[1]}s "{rest[2]}" not found')
    print(json.dumps(found))
    sys.exit(0)
if rest[:2] == ["get", "secret"]:
    if (ns, rest[2]) != (secret["namespace"], secret["name"]):
        fail(f'Error from server (NotFound): secrets "{rest[2]}" not found')
    path = rest[4] if rest[3:4] == ["-o"] and len(rest) == 5 else ""
    if not (path.startswith("jsonpath={.data.") and path.endswith("}")):
        fail("fake kubectl: only one jsonpath data key may be read", 98)
    key = path[len("jsonpath={.data."):-1].replace("\\.", ".")
    sys.stdout.write(secret["data"].get(key, ""))
    sys.exit(0)
if rest[:1] == ["exec"] and "--" in rest:
    head, command = rest[1:rest.index("--")], rest[rest.index("--") + 1:]
    stdin = "-i" in head
    pod = [t for t in head if t not in ("-i", "-c", "dockerd")]
    if ("pod", ns, pod[0]) not in objects or head[head.index("-c") + 1] != "dockerd":
        fail(f'Error from server (NotFound): pods "{pod[0]}" not found')
    data = sys.stdin.buffer.read() if stdin else b""
    if stdin:
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            members = [[m.name, m.mode, m.isfile()] for m in tar.getmembers()]
        with open(os.environ["FAKE_DOCKERD_LOG"], "a") as log:
            log.write(json.dumps({"staged": members}) + "\n")
    if os.environ.get("FAKE_CLEANUP_FAIL") and command[2].startswith("rm -rf"):
        busy = f"rm: can't remove '{command[-1]}': Resource busy"
        fail(busy + "\ncommand terminated with exit code 1")
    env = dict(os.environ, TMPDIR=os.environ["FAKE_DOCKERD_TMP"])
    env["PATH"] = os.environ["FAKE_DOCKERD_BIN"] + os.pathsep + env["PATH"]
    proc = subprocess.run(command, input=data, capture_output=True, env=env)
    sys.stdout.buffer.write(proc.stdout)
    sys.stderr.buffer.write(proc.stderr)
    if proc.returncode:
        sys.stderr.write(f"command terminated with exit code {proc.returncode}\n")
    sys.exit(proc.returncode)
fail("fake kubectl: unexpected argv " + " ".join(args), 99)
"""

# Shims for the dockerd container's own tools. unshare drops -m; mount logs the
# bind of the staged hosts file over /etc/hosts; docker checks that the Service name
# it dials is mapped to 127.0.0.1 by that file, then plays the daemon: it trusts only
# the claim's CA and client certificate and serves only the claim's Service name
# (FAKE_TRUST), replaying the recorded TLS refusal for anything else, and otherwise
# replays info-daemon or events.
DOCKERD_SHIMS = {
    "unshare": r"""
import os, sys
args = sys.argv[1:]
if args[:1] != ["-m"]:
    sys.exit("unshare: fake expects -m")
os.execvp(args[1], args[1:])
""",
    "mount": r"""
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if len(args) != 3 or args[0] != "--bind" or args[2] != "/etc/hosts":
    sys.exit("mount: fake expects --bind SRC /etc/hosts")
with open(os.environ["FAKE_DOCKERD_LOG"], "a") as log:
    log.write(json.dumps({"mount": args, "hosts": Path(args[1]).read_text()}) + "\n")
""",
    "docker": r"""
import hashlib, json, os, sys
from pathlib import Path
raw = Path(os.environ["FAKE_RAW"])
args = sys.argv[1:]
flags = ["-H", "--tlsverify", "--tlscacert", "--tlscert", "--tlskey"]
if [args[0], args[2], args[3], args[5], args[7]] != flags:
    sys.exit("docker: fake expects -H HOST --tlsverify and the three TLS files")
target = args[1].removeprefix("tcp://")
files = {"ca": args[4], "cert": args[6], "key": args[8]}
seen = {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in files.items()}
modes = {k: oct(os.stat(p).st_mode & 0o777) for k, p in files.items()}
with open(os.environ["FAKE_DOCKERD_LOG"], "a") as log:
    log.write(json.dumps({"docker": args, "sha256": seen, "modes": modes}) + "\n")
hosts = (Path(files["ca"]).parent / "hosts").read_text().split("\n")
if f"127.0.0.1 {target.rsplit(':', 1)[0]}" not in hosts:
    sys.exit(f"error during connect: dial tcp: lookup {target}: no such host")
trust = json.loads(Path(os.environ["FAKE_TRUST"]).read_text())
if target != trust["server"]:
    name = "tls-server-name"
elif seen["ca"] != trust["ca"]:
    name = "tls-foreign-ca"
elif seen["cert"] != trust["client"]:
    name = "tls-foreign-client"
else:
    name = {"info": "info-daemon", "events": "events"}[args[9]]
code = int((raw / (name + ".rc")).read_text().strip())
(sys.stdout if code == 0 else sys.stderr).write((raw / (name + ".out")).read_text())
sys.exit(code)
""",
}

DAEMON_NAMESPACE = "fd-proof-d-e8ce1a8c"
RUNNER_NAMESPACE = "fd-proof-e8ce1a8c"
RUNNER_POD = "curie-runner-e8ce1a8c-a"
CLIENT_SECRET = f"{DAEMON_NAME}-tokens"
DAEMON_SERVER = f"{DAEMON_NAME}.{DAEMON_NAMESPACE}.svc:2376"
OTHER_CLAIM = "3f9a0d2c7e15"
CLIENT_KEYS = ("ca.pem", "cert.pem", "key.pem")


def _issue(claim: str) -> dict[str, str]:
    from curie_worker.sandbox import tool_daemon

    return dict(tool_daemon.issue(claim, DAEMON_NAMESPACE, 3600).client_files)


@pytest.fixture(scope="module")
def client_pems() -> dict[str, str]:
    """The claim's client files, minted the way the worker mints them."""

    return _issue(DAEMON_NAME)


@pytest.fixture(scope="module")
def foreign_pems() -> dict[str, str]:
    """Another claim's client files: a CA and client certificate the daemon never saw."""

    return _issue(OTHER_CLAIM)


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class FakeCluster:
    """Points the kubectl fake at its objects, Secret and daemon trust, one file each."""

    def __init__(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pems: dict[str, str]
    ) -> None:
        self.dir = tmp_path / "cluster"
        self.dir.mkdir()
        self.tmp = tmp_path / "dockerd-tmp"
        self.tmp.mkdir()
        self.log = tmp_path / "dockerd.log"
        self.log.touch()
        self.pems = pems
        self.monkeypatch = monkeypatch
        monkeypatch.setenv("FAKE_DOCKERD_TMP", str(self.tmp))
        monkeypatch.setenv("FAKE_DOCKERD_LOG", str(self.log))
        trust = {"server": DAEMON_SERVER, "ca": _sha(pems["ca.pem"])}
        trust["client"] = _sha(pems["cert.pem"])
        monkeypatch.setenv("FAKE_TRUST", str(self._write("trust.json", trust)))
        self.reset()

    def _write(self, name: str, obj: Any) -> Path:
        path = self.dir / name
        path.write_text(json.dumps(obj))
        return path

    def reset(self) -> None:
        """The recorded objects, the claim's own Secret, and no fault."""

        for env, name in (
            ("FAKE_DAEMON_POD", "daemon-pod.json"),
            ("FAKE_SERVICE", "daemon-service.json"),
            ("FAKE_RUNNER_POD", "runner-pod.json"),
        ):
            self.monkeypatch.setenv(env, str(RAW / name))
        self.secret({key: _b64(text) for key, text in self.pems.items()})
        self.monkeypatch.delenv("FAKE_CLEANUP_FAIL", raising=False)

    def secret(self, data: dict[str, str]) -> None:
        body = {"namespace": RUNNER_NAMESPACE, "name": CLIENT_SECRET, "data": data}
        self.monkeypatch.setenv("FAKE_SECRET", str(self._write("secret.json", body)))

    def edit(self, name: str, change: Callable[[dict[str, Any]], object]) -> None:
        """Serve an edited copy of a recorded object: daemon-pod, daemon-service, runner-pod."""

        obj = json.loads((RAW / f"{name}.json").read_text())
        change(obj)
        env = {"daemon-pod": "FAKE_DAEMON_POD", "daemon-service": "FAKE_SERVICE"}
        path = self._write(f"{name}.json", obj)
        self.monkeypatch.setenv(env.get(name, "FAKE_RUNNER_POD"), str(path))

    def records(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def leftovers(self) -> list[str]:
        return sorted(p.name for p in self.tmp.iterdir())


@pytest.fixture
def fake_bin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    bin_dir = tmp_path / "fakebin"
    dockerd_bin = tmp_path / "dockerd-bin"
    for directory, fakes in (
        (bin_dir, {"docker": FAKE_DOCKER, "kubectl": FAKE_KUBECTL}),
        (dockerd_bin, DOCKERD_SHIMS),
    ):
        directory.mkdir()
        for name, body in fakes.items():
            exe = directory / name
            exe.write_text(f"#!{sys.executable}\n{body}")
            exe.chmod(0o755)
    log = tmp_path / "argv.log"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RAW", str(RAW))
    monkeypatch.setenv("FAKE_LOG", str(log))
    monkeypatch.setenv("FAKE_DOCKERD_BIN", str(dockerd_bin))
    yield log


@pytest.fixture
def cluster(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_bin: Path, client_pems: dict[str, str]
) -> FakeCluster:
    return FakeCluster(tmp_path, monkeypatch, client_pems)


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


def _collect_argv(out: Path) -> list[str]:
    flags = {
        "context": "scratch-ctx", "namespace": DAEMON_NAMESPACE, "pod": DAEMON_NAME,
        "runner-namespace": RUNNER_NAMESPACE, "runner-pod": RUNNER_POD, "nonce": NONCE,
        "since": "1792669432", "until": "1792669451",
    }  # fmt: skip
    argv = ["collect", "--out", str(out)]
    for flag, value in flags.items():
        argv += [f"--{flag}", value]
    return argv


def _material(*pem_sets: dict[str, str]) -> list[str]:
    """Every PEM, its base64 form and each of its body lines: none may surface."""

    found: list[str] = ["-----BEGIN", "PRIVATE KEY"]
    for pems in pem_sets:
        for text in pems.values():
            found += [text, _b64(text), _b64(text)[:40]]
            found += [line for line in text.splitlines() if len(line) >= 16 and "-----" not in line]
    return found


def _no_material(texts: list[str], *pem_sets: dict[str, str]) -> None:
    for text in texts:
        for secret in _material(*pem_sets):
            assert secret not in text, "client credential material leaked"


def _refused(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    *pem_sets: dict[str, str],
) -> str:
    """Run collect, expect a refusal, and return its stderr after the leak checks."""

    out = tmp_path / "daemon.json"
    assert docker_scenario.main(_collect_argv(out)) == 1
    assert not out.exists()
    captured = capsys.readouterr()
    _no_material([captured.out, captured.err], *pem_sets)
    return captured.err


def _secret_reads(calls: list[list[str]]) -> list[list[str]]:
    return [c for c in calls if "secret" in c]


def _execs(calls: list[list[str]]) -> list[list[str]]:
    return [c for c in calls if "exec" in c]


def test_collect_replays_recordings(
    tmp_path: Path,
    fake_bin: Path,
    cluster: FakeCluster,
    envelope: dict[str, Any],
    client_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    out = tmp_path / "daemon.json"
    assert docker_scenario.main(_collect_argv(out)) == 0
    section = json.loads(out.read_text())
    assert section == envelope["runs"]["a"]["daemon"]
    captured = capsys.readouterr()
    _no_material([out.read_text(), captured.out, captured.err], client_pems)

    calls = _logged(fake_bin)
    runner_side = [["get", "pod", RUNNER_POD, "-o", "json"]] + [
        ["get", "secret", CLIENT_SECRET, "-o", "jsonpath={.data." + key.replace(".", "\\.") + "}"]
        for key in CLIENT_KEYS
    ]
    for call in calls:
        assert call[call.index("--context") + 1] == "scratch-ctx", call
        namespace = call[call.index("-n") + 1]
        rest = call[call.index("-n") + 2 :]
        assert namespace == (RUNNER_NAMESPACE if rest in runner_side else DAEMON_NAMESPACE), call
    assert _secret_reads(calls) == [
        ["--context", "scratch-ctx", "-n", RUNNER_NAMESPACE, *read] for read in runner_side[1:]
    ]
    assert ["get", "service", DAEMON_NAME, "-o", "json"] in [c[4:] for c in calls]
    assert ["get", "pod", DAEMON_NAME, "-o", "json"] in [c[4:] for c in calls]
    execs = _execs(calls)
    assert len(execs) == 4  # stage, info, events, removal
    for call in execs:
        assert call[call.index("-c") + 1] == "dockerd", call
        assert DAEMON_NAME in call[: call.index("--")]
    assert "-i" in execs[0][: execs[0].index("--")]
    events = next(c for c in execs if "events" in c)
    assert events[events.index("--since") + 1] == "1792669432"
    assert events[events.index("--until") + 1] == "1792669451"
    assert f"label=curie.fixture.nonce={NONCE}" in events

    records = cluster.records()
    staged = [r["staged"] for r in records if "staged" in r]
    assert staged == [[[key, 0o600, True] for key in CLIENT_KEYS]]
    host = DAEMON_SERVER.rsplit(":", 1)[0]
    mounts = [r for r in records if "mount" in r]
    assert len(mounts) == 2
    assert all(m["hosts"] == f"127.0.0.1 {host}\n" for m in mounts)
    dockers = [r for r in records if "docker" in r]
    assert [d["docker"][9] for d in dockers] == ["info", "events"]
    for record in dockers:
        argv = record["docker"]
        directory = mounts[0]["mount"][1].removesuffix("/hosts")
        assert argv[:9] == [
            "-H", f"tcp://{DAEMON_SERVER}", "--tlsverify",
            "--tlscacert", f"{directory}/ca.pem", "--tlscert", f"{directory}/cert.pem",
            "--tlskey", f"{directory}/key.pem",
        ]  # fmt: skip
        assert record["sha256"] == {
            "ca": _sha(client_pems["ca.pem"]),
            "cert": _sha(client_pems["cert.pem"]),
            "key": _sha(client_pems["key.pem"]),
        }
        assert set(record["modes"].values()) == {"0o600"}
    assert cluster.leftovers() == []


def test_collect_accepts_a_client_secret_mounted_without_items(
    tmp_path: Path, cluster: FakeCluster, envelope: dict[str, Any]
) -> None:
    def whole_secret(pod: dict[str, Any]) -> None:
        del _volume(pod)["secret"]["items"]

    cluster.edit("runner-pod", whole_secret)
    out = tmp_path / "daemon.json"
    assert docker_scenario.main(_collect_argv(out)) == 0
    assert json.loads(out.read_text()) == envelope["runs"]["a"]["daemon"]
    assert cluster.leftovers() == []


def _endpoint(claim: str, namespace: str, port: int) -> Callable[[dict[str, Any]], object]:
    """An edit setting the runner's DOCKER_HOST to another daemon address."""

    value = f"tcp://{claim}.{namespace}.svc:{port}"
    return lambda pod: _docker_host(pod).update({"value": value})


def _dockerd(pod: dict[str, Any]) -> dict[str, Any]:
    return next(c for c in pod["spec"]["containers"] if c["name"] == "dockerd")


def _runner(pod: dict[str, Any]) -> dict[str, Any]:
    return next(c for c in pod["spec"]["containers"] if c["name"] == "runner")


def _docker_host(pod: dict[str, Any]) -> dict[str, Any]:
    return next(e for e in _runner(pod)["env"] if e["name"] == "DOCKER_HOST")


def _volume(pod: dict[str, Any]) -> dict[str, Any]:
    return next(v for v in pod["spec"]["volumes"] if v["name"] == "curie-docker-tls")


def _host_arg(pod: dict[str, Any], value: str | None) -> None:
    args = _dockerd(pod)["args"]
    index = args.index("--host=tcp://0.0.0.0:2376")
    if value is None:
        del args[index]
    else:
        args[index] = value


CLAIM = docker_scenario.CLAIM_LABEL
# (served object, edit, expected refusal text): each must refuse before any Secret read.
MISMATCHES: dict[str, tuple[str, Callable[[dict[str, Any]], object], str]] = {
    "runner-claim-differs": (
        "runner-pod", lambda o: o["metadata"]["labels"].update({CLAIM: OTHER_CLAIM}),
        "belongs to another claim",
    ),
    "runner-claim-missing": (
        "runner-pod", lambda o: o["metadata"]["labels"].pop(CLAIM), "no usable",
    ),
    "daemon-claim-missing": (
        "daemon-pod", lambda o: o["metadata"]["labels"].pop(CLAIM), "no usable",
    ),
    "dockerd-port-not-declared": (
        "daemon-pod", lambda o: _host_arg(o, "--host=tcp://0.0.0.0:2377"), "no such containerPort",
    ),
    "container-port-differs": (
        "daemon-pod", lambda o: _dockerd(o)["ports"][0].update({"containerPort": 2377}),
        "no such containerPort",
    ),
    "dockerd-second-host": (
        "daemon-pod", lambda o: _dockerd(o)["args"].append("--host=unix:///var/run/docker.sock"),
        "exactly one --host",
    ),
    "dockerd-no-tcp-host": ("daemon-pod", lambda o: _host_arg(o, None), "exactly one --host"),
    "service-port-differs": (
        "daemon-service", lambda o: o["spec"]["ports"][0].update({"port": 2377}), "does not route",
    ),
    "service-target-port-differs": (
        "daemon-service", lambda o: o["spec"]["ports"][0].update({"targetPort": 2377}),
        "does not route",
    ),
    "service-port-unnamed": (
        "daemon-service", lambda o: o["spec"]["ports"][0].update({"name": "tls"}), "does not route",
    ),
    "runner-endpoint-port-differs": (
        "runner-pod", _endpoint(DAEMON_NAME, DAEMON_NAMESPACE, 2377), "does not name",
    ),
    "runner-names-another-claim": (
        "runner-pod", _endpoint(OTHER_CLAIM, DAEMON_NAMESPACE, 2376), "does not name",
    ),
    "runner-names-another-namespace": (
        "runner-pod", _endpoint(DAEMON_NAME, RUNNER_NAMESPACE, 2376), "does not name",
    ),
    "runner-dials-a-bare-ip": (
        "runner-pod", lambda o: _docker_host(o).update({"value": "tcp://10.42.0.17:2376"}),
        "does not name",
    ),
    "runner-without-docker-host": (
        "runner-pod", lambda o: _runner(o)["env"].remove(_docker_host(o)), "does not name",
    ),
    "secret-projects-no-key": (
        "runner-pod", lambda o: _volume(o)["secret"]["items"].pop(), "does not project",
    ),
    "no-client-mount": (
        "runner-pod", lambda o: _runner(o)["volumeMounts"].clear(), "mounts no client Secret",
    ),
    "no-runner-container": (
        "runner-pod", lambda o: _runner(o).update({"name": "sidecar"}), "no runner container",
    ),
}  # fmt: skip


@pytest.mark.parametrize("case", sorted(MISMATCHES))
def test_collect_refuses_a_mismatched_claim_endpoint_before_reading_credentials(
    tmp_path: Path,
    fake_bin: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    case: str,
) -> None:
    name, edit, reason = MISMATCHES[case]
    cluster.edit(name, edit)
    err = _refused(tmp_path, capsys, client_pems)
    assert reason in err
    calls = _logged(fake_bin)
    assert _secret_reads(calls) == []
    assert _execs(calls) == []
    assert cluster.leftovers() == []


def _without(data: dict[str, str], key: str) -> None:
    del data[key]


# (key, change to the served base64 data): each refused before anything is staged.
BAD_CREDENTIALS: dict[str, tuple[str, Callable[[dict[str, str]], object], str]] = {
    "missing-key": ("key.pem", lambda d: _without(d, "key.pem"), "missing or empty"),
    "empty-key": ("cert.pem", lambda d: d.update({"cert.pem": ""}), "missing or empty"),
    "malformed-base64": (
        "ca.pem", lambda d: d.update({"ca.pem": d["ca.pem"][:-3] + "!*#"}), "base64",
    ),
    "not-pem": ("cert.pem", lambda d: d.update({"cert.pem": _b64("not a certificate\n")}), "PEM"),
    "key-in-cert-slot": ("cert.pem", lambda d: d.update({"cert.pem": d["key.pem"]}), "PEM"),
    "cert-in-key-slot": ("key.pem", lambda d: d.update({"key.pem": d["cert.pem"]}), "PEM"),
}  # fmt: skip


@pytest.mark.parametrize("case", sorted(BAD_CREDENTIALS))
def test_collect_refuses_a_missing_or_malformed_client_credential(
    tmp_path: Path,
    fake_bin: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    case: str,
) -> None:
    key, change, reason = BAD_CREDENTIALS[case]
    data = {k: _b64(text) for k, text in client_pems.items()}
    change(data)
    cluster.secret(data)
    err = _refused(tmp_path, capsys, client_pems)
    assert f"secret {CLIENT_SECRET}: {key}" in err and reason in err
    assert _execs(_logged(fake_bin)) == []
    assert cluster.leftovers() == []


@pytest.mark.parametrize(
    ("foreign", "recording"),
    [
        (("cert.pem", "key.pem"), "tls-foreign-client"),
        (("ca.pem",), "tls-foreign-ca"),
        (CLIENT_KEYS, "tls-foreign-ca"),
    ],
)
def test_collect_fails_closed_on_a_foreign_client_credential(
    tmp_path: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    foreign_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    foreign: tuple[str, ...],
    recording: str,
) -> None:
    served = {k: foreign_pems[k] if k in foreign else client_pems[k] for k in CLIENT_KEYS}
    cluster.secret({k: _b64(text) for k, text in served.items()})
    err = _refused(tmp_path, capsys, client_pems, foreign_pems)
    assert _raw(recording).strip() in err
    assert [r["docker"][9] for r in cluster.records() if "docker" in r] == ["info"]
    assert cluster.leftovers() == []


def test_collect_fails_closed_when_the_daemon_certificate_names_another_service(
    tmp_path: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    trust = json.loads(Path(os.environ["FAKE_TRUST"]).read_text())
    trust["server"] = f"{OTHER_CLAIM}.{DAEMON_NAMESPACE}.svc:2376"
    Path(os.environ["FAKE_TRUST"]).write_text(json.dumps(trust))
    err = _refused(tmp_path, capsys, client_pems)
    assert _raw("tls-server-name").strip() in err
    assert cluster.leftovers() == []


def test_collect_reports_a_failed_removal_without_material(
    tmp_path: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FAKE_CLEANUP_FAIL", "1")
    err = _refused(tmp_path, capsys, client_pems)
    assert "staged credentials not removed" in err and "Resource busy" in err
    assert len(cluster.leftovers()) == 1


def test_collect_reports_both_a_refusal_and_a_failed_removal(
    tmp_path: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    foreign_pems: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    served = {**client_pems, **{k: foreign_pems[k] for k in ("cert.pem", "key.pem")}}
    cluster.secret({k: _b64(text) for k, text in served.items()})
    monkeypatch.setenv("FAKE_CLEANUP_FAIL", "1")
    err = _refused(tmp_path, capsys, client_pems, foreign_pems)
    assert _raw("tls-foreign-client").strip() in err
    assert "staged credentials not removed" in err


@pytest.mark.parametrize("fault", ["malformed", "foreign", "mismatch", "removal"])
def test_collect_recovers_after_a_refused_run(
    tmp_path: Path,
    cluster: FakeCluster,
    client_pems: dict[str, str],
    foreign_pems: dict[str, str],
    envelope: dict[str, Any],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    data = {k: _b64(text) for k, text in client_pems.items()}
    if fault == "malformed":
        cluster.secret({**data, "key.pem": "%%%"})
    elif fault == "foreign":
        cluster.secret({**data, "cert.pem": _b64(foreign_pems["cert.pem"])})
    elif fault == "mismatch":
        cluster.edit("runner-pod", MISMATCHES["runner-names-another-claim"][1])
    else:
        monkeypatch.setenv("FAKE_CLEANUP_FAIL", "1")
    _refused(tmp_path, capsys, client_pems, foreign_pems)
    left = cluster.leftovers()

    cluster.reset()
    out = tmp_path / "daemon.json"
    assert docker_scenario.main(_collect_argv(out)) == 0
    assert json.loads(out.read_text()) == envelope["runs"]["a"]["daemon"]
    assert cluster.leftovers() == left


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


# compose and skill_rung collectors against a fake _command

# The fake outputs replay shapes observed through the hardened daemon (built from
# apps/docker-daemon with curie-authz loaded) on the recording run:
# `docker compose port server 8080` printed `0.0.0.0:32768`;
# `docker compose ps --all --format json` printed one JSON object per line with keys
# including "Service" and "State"; `docker info --format '{{.ID}} {{.Name}}'` printed
# `275deac0-10bc-4cf6-857e-3f51f70c36de ded1c42cd936`; `docker compose down` and the
# label-filtered `docker ps -aq`, `network ls -q`, `volume ls -q` and `image ls -q`
# listings printed nothing on success.

COMPOSE_FILES = ("compose.yaml", "server/Dockerfile", "server/index.html", "client/Dockerfile")
LADDER_FILES = ("cli/scripts/e2e-ladder.sh", "cli/scripts/e2e.sh", "cli/scripts/gnu-process.py")
LADDER_BUNDLE = "examples/weather"
LADDER_PASS = "LADDER PASS (tiers: skill)"
COMPOSE_INFO = "275deac0-10bc-4cf6-857e-3f51f70c36de ded1c42cd936\n"
COMPOSE_DAEMON = {"id": "275deac0-10bc-4cf6-857e-3f51f70c36de", "name": "ded1c42cd936"}
COMPOSE_IMAGE = "python:3.12-alpine"
PUBLISHED_HOST = "10.240.1.2"
DOCKER_HOST = "tcp://10.240.1.2:2375"
SOURCE_REVISION = "4f1c2b9a7d3e5f60718293a4b5c6d7e8f9012345"
RUNNER_ID = "sha256:3b9f0c1d2e4a5b6c7d8e9f00112233445566778899aabbccddeeff0011223344"
LISTING = "9f8e7d6c5b4a\n"
SNAPSHOT_PS = ["docker", "ps", "-aq", "--no-trunc"]
SNAPSHOT_NETWORKS = ["docker", "network", "ls", "-q", "--no-trunc"]
SNAPSHOT_VOLUMES = ["docker", "volume", "ls", "-q"]
# The daemon's default bridge, host and none networks, present in every network listing.
DEFAULT_NETWORKS = [
    "5f1b0c2a9d8e7f60" + "1" * 48,
    "7a2c1d3b0e9f8a71" + "2" * 48,
    "9b3d2e4c1f0a9b82" + "3" * 48,
]
PREEXISTING_CONTAINER = "c0ffee00" + "4" * 56
PREEXISTING_VOLUME = "curie-preexisting-cache"
NEW_CONTAINER = "d00dfeed" + "5" * 56
NEW_NETWORK = "e1e1e1e1" + "6" * 56
NEW_VOLUME = "curie-ladder-workspace"
DAEMON_KINDS_BY_KEY = {"ps": "containers", "network ls": "networks", "volume ls": "volumes"}
CREDENTIALS = (
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CURIE_CREDENTIALS",
    "CURIE_MODEL_CREDENTIALS",
    "CURIE_MODEL_BASE_URL",
    "ANTHROPIC_BASE_URL",
    "CURIE_MODEL",
)
COMPOSE_KINDS = (
    ("containers", "ps"),
    ("networks", "network ls"),
    ("volumes", "volume ls"),
    ("images", "image ls"),
)
HEX64 = re.compile(r"[0-9a-f]{64}")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tree_digest(root: Path) -> str:
    """The plan's tree digest, computed independently of docker_scenario."""

    digest = hashlib.sha256()
    files = sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())
    for rel in files:
        digest.update(f"{rel}\0{_sha256((root / rel).read_bytes())}\n".encode())
    return digest.hexdigest()


def _marker() -> str:
    return f"curie-compose {NONCE}"


def _ps_object(project: str, service: str, state: str = "running") -> dict[str, Any]:
    return {
        "Command": "python3 -m http.server 8080 -d /srv"
        if service == "server"
        else "sleep infinity",
        "CreatedAt": "2026-10-24 10:15:02 +0000 UTC",
        "ExitCode": 0,
        "Health": "",
        "ID": "4c1d9e2f7a3b" if service == "server" else "8e2a6b0c5d1f",
        "Image": f"{project}-{service}",
        "Name": f"{project}-{service}-1",
        "Project": project,
        "Service": service,
        "State": state,
        "Status": "Up 3 seconds",
    }


class FakeCommands:
    """Stands in for docker_scenario._command; answers by argv shape and records calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], int, dict[str, str] | None]] = []
        self.answers: dict[str, tuple[int, str, str]] = {}
        self.raises: dict[str, BaseException] = {}
        self.hooks: dict[str, Callable[[list[str], dict[str, str] | None], None]] = {}
        self.http: list[tuple[int | None, str]] = []
        self.urls: list[str] = []
        self.sleeps: list[float] = []
        # Daemon objects the unfiltered skill_rung listings report; removals drop ids
        # unless they are sticky (a removal that leaves the object behind).
        self.daemon: dict[str, list[str]] = {
            "containers": [],
            "networks": list(DEFAULT_NETWORKS),
            "volumes": [],
        }
        self.sticky: set[str] = set()

    def add(self, kind: str, ident: str) -> None:
        self.daemon[kind].append(ident)

    @staticmethod
    def key(argv: list[str]) -> str:
        if argv[:2] == ["docker", "compose"] and "-f" in argv:
            rest = argv[argv.index("-f") + 2 :]
            return f"compose {rest[0]}" if rest else "compose"
        if argv[:1] == ["docker"]:
            if argv[1:2] in (["info"], ["build"], ["ps"], ["rm"]):
                return argv[1]
            if argv[1:2] in (["network"], ["volume"], ["image"]):
                return " ".join(argv[1:3])
        if argv[:1] == ["bash"]:
            return "ladder"
        if argv[0].endswith("/curie") and argv[1:] == ["--version"]:
            return "version"
        return "unknown"

    def default(self, key: str, argv: list[str]) -> tuple[int, str, str]:
        if key in DAEMON_KINDS_BY_KEY and "--filter" not in argv:
            ids = self.daemon[DAEMON_KINDS_BY_KEY[key]]
            return 0, "".join(f"{i}\n" for i in ids), ""
        removals = {"rm": "containers", "network rm": "networks", "volume rm": "volumes"}
        if key in removals:
            kind = removals[key]
            for ident in argv[3:]:
                if not ident.startswith("-") and ident not in self.sticky:
                    self.daemon[kind] = [i for i in self.daemon[kind] if i != ident]
            return 0, "", ""
        if key == "compose ps":
            project = argv[argv.index("-p") + 1]
            lines = [json.dumps(_ps_object(project, s)) for s in ("server", "client")]
            return 0, "\n".join(lines) + "\n", ""
        answers = {
            "info": (0, COMPOSE_INFO, ""),
            "compose build": (0, "", "#9 naming to docker.io/library/server done\n"),
            "compose up": (0, "", " Container server-1  Started\n"),
            "compose exec": (0, _marker() + "\n", ""),
            "compose port": (0, "0.0.0.0:32768\n", ""),
            "compose down": (0, "", ""),
            "ps": (0, "", ""),
            "network ls": (0, "", ""),
            "volume ls": (0, "", ""),
            "image ls": (0, "", ""),
            "build": (0, "", "#12 naming to docker.io/library/curie-runner:latest done\n"),
            "image inspect": (0, f"{RUNNER_ID} {NONCE}\n", ""),
            "ladder": (
                0,
                "skill tier: weather bundle ok\n" + LADDER_PASS + "\n",
                "",
            ),
            "image rm": (0, f"Untagged: curie-runner:latest\nDeleted: {RUNNER_ID}\n", ""),
            "version": (0, "curie 0.14.1\n", ""),
        }
        return answers.get(key, (99, "", "fake: unexpected argv " + " ".join(argv)))

    def __call__(
        self, argv: list[str], timeout: int = 120, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        argv = [str(a) for a in argv]
        self.calls.append((argv, timeout, None if env is None else dict(env)))
        key = self.key(argv)
        if key in self.hooks:
            self.hooks[key](argv, env)
        if key in self.raises:
            raise self.raises[key]
        if key in self.answers:
            return self.answers[key]
        return self.default(key, argv)

    def http_get(self, url: str, timeout: int = 5) -> tuple[int | None, str]:
        self.urls.append(url)
        return self.http.pop(0) if self.http else (200, _marker() + "\n")

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    def keys(self) -> list[str]:
        return [self.key(argv) for argv, _, _ in self.calls]

    def argvs(self, key: str) -> list[list[str]]:
        return [argv for argv, _, _ in self.calls if self.key(argv) == key]

    def call(self, key: str) -> tuple[list[str], int, dict[str, str] | None]:
        found = [c for c in self.calls if self.key(c[0]) == key]
        assert len(found) == 1, (key, self.keys())
        return found[0]


@pytest.fixture
def fake_commands(monkeypatch: pytest.MonkeyPatch) -> FakeCommands:
    fake = FakeCommands()
    monkeypatch.setattr(docker_scenario, "_command", fake)
    monkeypatch.setattr(docker_scenario, "_http_get", fake.http_get)
    monkeypatch.setattr(docker_scenario.time, "sleep", fake.sleep)
    monkeypatch.setenv("DOCKER_HOST", DOCKER_HOST)
    return fake


def _compose_base(fake: FakeCommands) -> list[str]:
    argv = next(a for a, _, _ in fake.calls if a[:2] == ["docker", "compose"])
    return argv[: argv.index("-f") + 2]


def _run_compose(host: str = PUBLISHED_HOST) -> dict[str, Any]:
    return docker_scenario.compose(NONCE, COMPOSE_IMAGE, host)


# compose: pure helpers


def test_compose_constants_match_the_plan() -> None:
    assert docker_scenario.COMPOSE_PORT == 8080
    assert docker_scenario.COMPOSE_SERVICES == ("server", "client")
    assert docker_scenario.COMPOSE_FILES == COMPOSE_FILES
    assert docker_scenario.COMPOSE_LABEL == "com.docker.compose.project"
    assert docker_scenario.compose_marker(NONCE) == _marker()


def test_compose_files_build_from_the_image_without_volumes() -> None:
    files = docker_scenario.compose_files(NONCE, COMPOSE_IMAGE)
    assert set(files) == set(COMPOSE_FILES)
    assert files["server/index.html"] == _marker() + "\n"
    server = files["server/Dockerfile"]
    assert server.splitlines()[0] == f"FROM {COMPOSE_IMAGE}"
    assert "COPY index.html /srv/index.html" in server
    assert '["python3", "-m", "http.server", "8080", "-d", "/srv"]' in server
    client = files["client/Dockerfile"]
    assert client.splitlines()[0] == f"FROM {COMPOSE_IMAGE}"
    assert '["sleep", "infinity"]' in client
    spec = yaml.safe_load(files["compose.yaml"])
    services = spec["services"]
    assert set(services) == {"server", "client"}
    assert services["server"]["build"] == "./server"
    assert [str(p) for p in services["server"]["ports"]] == ["8080"]
    assert services["client"]["build"] == "./client"
    assert "volumes" not in spec
    for name, service in services.items():
        assert "volumes" not in service, name
    for rel, text in files.items():
        assert "volumes" not in text and "bind" not in text, rel


@pytest.mark.parametrize(
    ("text", "port"),
    [
        ("0.0.0.0:32768\n", 32768),
        ("[::]:32768\n", 32768),
        ("0.0.0.0:32768\n[::]:32768\n", 32768),
        ("0.0.0.0:1", 1),
        ("0.0.0.0:65535", 65535),
        ("", None),
        ("\n", None),
        ("0.0.0.0:0", None),
        ("0.0.0.0:65536", None),
        ("0.0.0.0:x", None),
        ("no such service: server", None),
    ],
)
def test_compose_parse_port(text: str, port: int | None) -> None:
    assert docker_scenario.parse_port(text) == port


# compose: collector


def test_compose_runs_build_up_exec_port_and_publishes(fake_commands: FakeCommands) -> None:
    fake = fake_commands
    written: dict[str, str] = {}

    def capture(argv: list[str], env: dict[str, str] | None) -> None:
        project_dir = Path(argv[argv.index("-f") + 1]).parent
        for path in project_dir.rglob("*"):
            if path.is_file():
                written[path.relative_to(project_dir).as_posix()] = path.read_text()

    fake.hooks["compose build"] = capture
    section = _run_compose()

    assert written == docker_scenario.compose_files(NONCE, COMPOSE_IMAGE)
    for rel, text in written.items():
        assert "volumes:" not in text and "bind" not in text, rel
    base = _compose_base(fake)
    project = base[base.index("-p") + 1]
    assert re.fullmatch(rf"curie-compose-{NONCE}-[0-9a-f]{{6}}", project), project
    assert base == ["docker", "compose", "-p", project, "-f", base[-1]]
    assert Path(base[-1]).name == "compose.yaml"
    assert not Path(base[-1]).exists()
    for argv, _, _ in fake.calls:
        if argv[:2] == ["docker", "compose"]:
            assert argv[: len(base)] == base, argv

    keys = fake.keys()
    assert keys.index("compose build") < keys.index("compose up")
    assert ["docker", "info", "--format", docker_scenario.INFO_FORMAT] in fake.argvs("info")
    build, build_timeout, _ = fake.call("compose build")
    assert build == [*base, "build"]
    assert build_timeout == docker_scenario.BUILD_TIMEOUT
    assert fake.argvs("compose up") == [[*base, "up", "-d"]]
    assert fake.argvs("compose ps") == [[*base, "ps", "--all", "--format", "json"]]
    exec_argv = fake.call("compose exec")[0]
    assert exec_argv[: len(base) + 5] == [*base, "exec", "-T", "client", "python3", "-c"]
    assert "http://server:8080/index.html" in exec_argv[-1]
    assert fake.argvs("compose port") == [[*base, "port", "server", "8080"]]
    assert fake.urls == [f"http://{PUBLISHED_HOST}:32768/index.html"]
    label = f"label=com.docker.compose.project={project}"
    assert fake.argvs("compose down") == [
        [*base, "down", "--volumes", "--rmi", "local", "--remove-orphans"]
    ]
    assert fake.argvs("ps") == [["docker", "ps", "-aq", "--filter", label]]
    assert fake.argvs("network ls") == [["docker", "network", "ls", "-q", "--filter", label]]
    assert fake.argvs("volume ls") == [["docker", "volume", "ls", "-q", "--filter", label]]
    assert fake.argvs("image ls") == [["docker", "image", "ls", "-q", "--filter", label]]
    assert max(keys.index(k) for k in ("compose exec", "compose port")) < keys.index("compose down")
    assert keys.index("compose down") < min(keys.index(k) for _, k in COMPOSE_KINDS)

    assert section["project"] == project
    assert section["docker_host"] == DOCKER_HOST
    assert section["published_host"] == PUBLISHED_HOST
    assert section["info"] == COMPOSE_DAEMON
    assert section["image"] == COMPOSE_IMAGE
    assert section["expected"] == _marker()
    assert section["files"] == {
        rel: _sha256(text.encode())
        for rel, text in docker_scenario.compose_files(NONCE, COMPOSE_IMAGE).items()
    }
    assert section["build"]["exit_code"] == 0
    assert section["up"]["exit_code"] == 0
    assert section["down"]["exit_code"] == 0
    assert section["services"] == [
        {"service": "server", "state": "running"},
        {"service": "client", "state": "running"},
    ]
    assert section["reach"] == {
        "from": "client",
        "to": "server:8080",
        "exit_code": 0,
        "body": _marker() + "\n",
    }
    assert section["published"] == {
        "port": 32768,
        "url": f"http://{PUBLISHED_HOST}:32768/index.html",
        "status": 200,
        "body": _marker() + "\n",
    }
    assert section["cleanup"] == {
        "containers": [],
        "networks": [],
        "volumes": [],
        "images": [],
        "verified": True,
    }
    t0, t1 = section["window"]
    assert isinstance(t0, float) and isinstance(t1, float) and t0 <= t1


def test_compose_happy_path_judges_clean(fake_commands: FakeCommands) -> None:
    assert docker_scenario.judge_compose(_run_compose()) == []


def test_compose_published_check_retries_until_200(fake_commands: FakeCommands) -> None:
    fake_commands.http = [(None, "Connection refused"), (502, "Bad Gateway")]
    section = _run_compose()
    assert len(fake_commands.urls) == 3
    assert fake_commands.sleeps and all(s == 1 for s in fake_commands.sleeps)
    assert section["published"]["status"] == 200
    assert section["published"]["body"] == _marker() + "\n"
    assert docker_scenario.judge_compose(section) == []


def test_compose_published_check_gives_up_after_the_attempts(
    fake_commands: FakeCommands,
) -> None:
    fake_commands.http = [(None, "Connection refused")] * (docker_scenario.HTTP_ATTEMPTS + 5)
    section = _run_compose()
    assert len(fake_commands.urls) == docker_scenario.HTTP_ATTEMPTS
    assert section["published"]["status"] is None
    reasons = docker_scenario.judge_compose(section)
    assert any(r.startswith("compose_published_port_unreachable: ") for r in reasons), reasons


def test_compose_brackets_an_ipv6_published_host(fake_commands: FakeCommands) -> None:
    section = _run_compose("fd00::5")
    assert fake_commands.urls == ["http://[fd00::5]:32768/index.html"]
    assert section["published"]["url"] == "http://[fd00::5]:32768/index.html"


def test_compose_without_a_port_skips_the_published_check(fake_commands: FakeCommands) -> None:
    fake_commands.answers["compose port"] = (1, "", "no port 8080/tcp published\n")
    section = _run_compose()
    assert fake_commands.urls == []
    assert section["published"]["port"] is None
    assert section["published"]["status"] is None
    assert section["published"]["url"] == ""
    reasons = docker_scenario.judge_compose(section)
    assert any(r.startswith("compose_published_port_unreachable: ") for r in reasons), reasons


def test_compose_ps_accepts_a_json_array(fake_commands: FakeCommands) -> None:
    def array(argv: list[str], env: dict[str, str] | None) -> None:
        project = argv[argv.index("-p") + 1]
        objects = [_ps_object(project, "server"), _ps_object(project, "client", "exited")]
        fake_commands.answers["compose ps"] = (0, json.dumps(objects) + "\n", "")

    fake_commands.hooks["compose ps"] = array
    section = _run_compose()
    assert section["services"] == [
        {"service": "server", "state": "running"},
        {"service": "client", "state": "exited"},
    ]
    reasons = docker_scenario.judge_compose(section)
    assert any(r.startswith("compose_service_not_running: client") for r in reasons), reasons


def _assert_compose_cleanup_ran(fake: FakeCommands) -> None:
    base = _compose_base(fake)
    project = base[base.index("-p") + 1]
    label = f"label=com.docker.compose.project={project}"
    assert [*base, "down", "--volumes", "--rmi", "local", "--remove-orphans"] in fake.argvs(
        "compose down"
    )
    for _, key in COMPOSE_KINDS:
        assert any(label in argv for argv in fake.argvs(key)), (key, fake.keys())


def test_compose_cleans_up_when_up_fails(fake_commands: FakeCommands) -> None:
    fake_commands.answers["compose up"] = (1, "", "Error response from daemon: up failed\n")
    try:
        section: dict[str, Any] | None = _run_compose()
    except docker_scenario.CommandError:
        section = None
    _assert_compose_cleanup_ran(fake_commands)
    if section is not None:
        assert section["up"]["exit_code"] == 1
        reasons = docker_scenario.judge_compose(section)
        assert any(r.startswith("compose_up_failed: ") for r in reasons), reasons


class _Boom(Exception):
    pass


@pytest.mark.parametrize("step", ["compose up", "compose exec", "compose port"])
def test_compose_cleans_up_when_a_step_raises(fake_commands: FakeCommands, step: str) -> None:
    fake_commands.raises[step] = _Boom(step)
    with pytest.raises(_Boom):
        _run_compose()
    _assert_compose_cleanup_ran(fake_commands)


@pytest.mark.parametrize(("kind", "key"), COMPOSE_KINDS)
@pytest.mark.parametrize("answer", [(0, LISTING, ""), (1, "", "Error response from daemon\n")])
def test_compose_unverified_cleanup_is_recorded(
    fake_commands: FakeCommands, kind: str, key: str, answer: tuple[int, str, str]
) -> None:
    fake_commands.answers[key] = answer
    section = _run_compose()
    cleanup = section["cleanup"]
    assert cleanup["verified"] is False
    if answer[1]:
        assert LISTING.strip() in cleanup[kind]
    reasons = docker_scenario.judge_compose(section)
    assert any(r.startswith("incomplete_cleanup: ") for r in reasons), reasons


def test_compose_requires_a_published_host(fake_commands: FakeCommands) -> None:
    with pytest.raises(docker_scenario.CommandError):
        _run_compose("")
    assert fake_commands.calls == []


# skill_rung: fake source tree and collector


SOURCE_FILES = {
    "cli/scripts/e2e-ladder.sh": "#!/usr/bin/env bash\nset -euo pipefail\necho ladder\n",
    "cli/scripts/e2e.sh": "#!/usr/bin/env bash\nset -euo pipefail\necho e2e\n",
    "cli/scripts/gnu-process.py": "import sys\nsys.exit(0)\n",
    "examples/weather/curie.yaml": "name: weather\n",
    "examples/weather/skills/forecast/SKILL.md": "# Forecast\nRead the forecast.\n",
    "runner/Dockerfile": "FROM python:3.12-slim\nRUN true\n",
    "README.md": "not provisioned\n",
}


def _skill_source(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "source"
    for rel, text in SOURCE_FILES.items():
        path = source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    curie_bin = tmp_path / "built" / "curie"
    curie_bin.parent.mkdir()
    curie_bin.write_text("#!/bin/sh\necho 'curie 0.14.1'\n")
    curie_bin.chmod(0o755)
    return source, curie_bin


def _run_skill_rung(source: Path, curie_bin: Path, host: str = PUBLISHED_HOST) -> dict[str, Any]:
    return docker_scenario.skill_rung(NONCE, source, curie_bin, host, SOURCE_REVISION)


def test_skill_rung_constants_match_the_plan() -> None:
    assert docker_scenario.LADDER_FILES == LADDER_FILES
    assert docker_scenario.LADDER_BUNDLE == LADDER_BUNDLE
    assert docker_scenario.RUNNER_TAG == "curie-runner"
    assert docker_scenario.RUNNER_DOCKERFILE == "runner/Dockerfile"
    assert docker_scenario.LADDER_PASS == LADDER_PASS
    assert docker_scenario.MANAGED_LABEL == "curietech.ai/managed-by=curie-cli"


def test_skill_rung_tree_digest_and_sha256_file(tmp_path: Path) -> None:
    source, curie_bin = _skill_source(tmp_path)
    bundle = source / "examples/weather"
    assert docker_scenario.tree_digest(bundle) == _tree_digest(bundle)
    assert docker_scenario.sha256_file(curie_bin) == _sha256(curie_bin.read_bytes())
    before = docker_scenario.tree_digest(bundle)
    (bundle / "curie.yaml").write_text("name: other\n")
    assert docker_scenario.tree_digest(bundle) != before


def test_skill_rung_tree_digest_refuses_symlinks(tmp_path: Path) -> None:
    source, _ = _skill_source(tmp_path)
    bundle = source / "examples/weather"
    (bundle / "link.md").symlink_to(bundle / "curie.yaml")
    with pytest.raises(ValueError):
        docker_scenario.tree_digest(bundle)


def test_skill_rung_provisions_runs_the_ladder_and_cleans_up(
    tmp_path: Path, fake_commands: FakeCommands, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = fake_commands
    source, curie_bin = _skill_source(tmp_path)
    for name in CREDENTIALS:
        monkeypatch.setenv(name, "not-a-real-credential")
    monkeypatch.setenv("CURIE_BIN", str(tmp_path / "elsewhere" / "curie"))
    seen: dict[str, Any] = {}

    def capture(argv: list[str], env: dict[str, str] | None) -> None:
        script = Path(argv[1])
        repo, work = script.parents[2], script.parents[3]
        bundle = repo / LADDER_BUNDLE
        curie = work / "bin" / "curie"
        seen.update(
            work=work,
            files={rel: (repo / rel).read_text() for rel in LADDER_FILES},
            bundle={
                p.relative_to(bundle).as_posix(): p.read_text()
                for p in bundle.rglob("*")
                if p.is_file()
            },
            readme=(repo / "README.md").exists(),
            curie=curie.read_bytes(),
            executable=os.access(curie, os.X_OK),
            mode=curie.stat().st_mode & 0o777,
        )

    fake.hooks["ladder"] = capture
    section = _run_skill_rung(source, curie_bin)

    work = seen["work"]
    ladder_argv, ladder_timeout, env = fake.call("ladder")
    assert ladder_argv == ["bash", str(work / "repo" / "cli" / "scripts" / "e2e-ladder.sh")]
    assert ladder_timeout == docker_scenario.LADDER_TIMEOUT
    assert seen["files"] == {rel: SOURCE_FILES[rel] for rel in LADDER_FILES}
    assert seen["bundle"] == {
        "curie.yaml": SOURCE_FILES["examples/weather/curie.yaml"],
        "skills/forecast/SKILL.md": SOURCE_FILES["examples/weather/skills/forecast/SKILL.md"],
    }
    assert seen["readme"] is False
    assert seen["curie"] == curie_bin.read_bytes()
    assert seen["executable"] is True and seen["mode"] == 0o755
    assert not work.exists()

    assert env is not None
    assert env["CURIE_E2E_TIERS"] == "skill"
    assert env["CURIE_E2E_LIVE"] == "0"
    assert env["CURIE_BIN"] == str(work / "bin" / "curie")
    assert env["DOCKER_PUBLISHED_HOST"] == PUBLISHED_HOST
    assert env["DOCKER_HOST"] == DOCKER_HOST
    assert env.get("PATH") == os.environ["PATH"]
    for name in CREDENTIALS:
        assert name not in env, name

    build, build_timeout, _ = fake.call("build")
    assert build == [
        "docker", "build", "-f", str(source / "runner" / "Dockerfile"), "-t", "curie-runner",
        "--label", f"curie.fixture.nonce={NONCE}", str(source),
    ]  # fmt: skip
    assert build_timeout == docker_scenario.BUILD_TIMEOUT
    assert fake.argvs("image inspect") == [
        [
            "docker", "image", "inspect", "--format",
            '{{.Id}} {{index .Config.Labels "curie.fixture.nonce"}}', "curie-runner",
        ]
    ]  # fmt: skip
    assert fake.argvs("image rm") == [["docker", "image", "rm", "-f", "curie-runner"]]
    image_ls = ["docker", "image", "ls", "-q", "curie-runner"]
    assert fake.argvs("image ls").count(image_ls) == 2
    assert fake.argvs("ps") == [SNAPSHOT_PS] * 3
    assert fake.argvs("network ls") == [SNAPSHOT_NETWORKS] * 3
    assert fake.argvs("volume ls") == [SNAPSHOT_VOLUMES] * 3
    for removal in ("rm", "network rm", "volume rm"):
        assert removal not in fake.keys(), removal
    keys = fake.keys()
    first_ls = keys.index("image ls")
    last_ls = len(keys) - 1 - keys[::-1].index("image ls")
    order = [
        first_ls,
        keys.index("build"),
        keys.index("image inspect"),
        keys.index("ladder"),
        keys.index("image rm"),
        last_ls,
    ]
    assert order == sorted(order), keys
    for listing in DAEMON_KINDS_BY_KEY:
        assert keys.index(listing) < keys.index("build"), listing
        assert keys.index("image rm") < len(keys) - 1 - keys[::-1].index(listing), listing
    assert "unknown" not in keys, fake.calls

    assert section["docker_host"] == DOCKER_HOST
    assert section["published_host"] == PUBLISHED_HOST
    assert section["nonce"] == NONCE
    assert section["info"] == COMPOSE_DAEMON
    inputs = section["inputs"]
    assert inputs["curie_bin"] == {
        "sha256": _sha256(curie_bin.read_bytes()),
        "version": "curie 0.14.1",
    }
    assert inputs["files"] == {rel: _sha256(SOURCE_FILES[rel].encode()) for rel in LADDER_FILES}
    assert inputs["bundle"] == {
        "path": "examples/weather",
        "digest": _tree_digest(source / "examples/weather"),
    }
    assert inputs["source_revision"] == SOURCE_REVISION
    image = section["image"]
    assert image["tag"] == "curie-runner"
    assert image["id"] == RUNNER_ID
    assert image["label_nonce"] == NONCE
    assert image["dockerfile"] == "runner/Dockerfile"
    assert image["dockerfile_sha256"] == _sha256(SOURCE_FILES["runner/Dockerfile"].encode())
    assert image["daemon_id"] == COMPOSE_DAEMON["id"]
    assert image["exit_code"] == 0
    ladder = section["ladder"]
    assert ladder["tiers"] == "skill" and ladder["live"] == "0"
    assert ladder["exit_code"] == 0 and ladder["passed"] is True
    assert docker_scenario.LADDER_PASS in ladder["output_tail"].splitlines()
    assert section["cleanup"] == {
        "image_removed": True,
        "containers": [],
        "networks": [],
        "volumes": [],
        "verified": True,
    }
    t0, t1 = section["window"]
    assert isinstance(t0, float) and isinstance(t1, float) and t0 <= t1


def test_skill_rung_happy_path_judges_clean(tmp_path: Path, fake_commands: FakeCommands) -> None:
    source, curie_bin = _skill_source(tmp_path)
    assert docker_scenario.judge_skill_rung(_run_skill_rung(source, curie_bin)) == []


def test_skill_rung_ladder_output_tail_keeps_the_last_4000_characters(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    out = "z" * 5000 + "\n" + LADDER_PASS + "\n"
    fake_commands.answers["ladder"] = (0, out, "")
    ladder = _run_skill_rung(source, curie_bin)["ladder"]
    assert ladder["output_tail"] == out[-4000:]
    assert ladder["passed"] is True


@pytest.mark.parametrize(
    ("code", "out", "passed"),
    [
        (0, "skill tier ok\nLADDER PASS (tiers: skill)\n", True),
        (1, "skill tier ok\nLADDER PASS (tiers: skill)\n", False),
        (0, "skill tier ok\n", False),
        (0, "LADDER PASS (tiers: skill, live)\n", False),
        (0, "not yet LADDER PASS (tiers: skill)\n", False),
        (0, "LADDER FAIL (tiers: skill)\n", False),
    ],
    ids=["pass", "nonzero", "no-line", "other-tiers", "embedded", "fail-line"],
)
def test_skill_rung_passed_needs_exit_0_and_the_exact_line(
    tmp_path: Path, fake_commands: FakeCommands, code: int, out: str, passed: bool
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    fake_commands.answers["ladder"] = (code, out, "")
    ladder = _run_skill_rung(source, curie_bin)["ladder"]
    assert ladder["passed"] is passed
    assert ladder["exit_code"] == code


def _assert_skill_cleanup_ran(fake: FakeCommands) -> None:
    keys = fake.keys()
    rm = keys.index("image rm")
    assert keys.index("ladder") < rm
    after = fake.calls[rm + 1 :]
    assert ["docker", "image", "ls", "-q", "curie-runner"] in [a for a, _, _ in after]
    for listing in (SNAPSHOT_PS, SNAPSHOT_NETWORKS, SNAPSHOT_VOLUMES):
        assert listing in [a for a, _, _ in after], listing


def test_skill_rung_cleans_up_when_the_ladder_fails(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    fake_commands.answers["ladder"] = (1, "", "LADDER FAIL (tiers: skill)\n")
    try:
        section: dict[str, Any] | None = _run_skill_rung(source, curie_bin)
    except docker_scenario.CommandError:
        section = None
    _assert_skill_cleanup_ran(fake_commands)
    if section is not None:
        assert section["ladder"]["passed"] is False
        reasons = docker_scenario.judge_skill_rung(section)
        assert any(r.startswith("skill_rung_ladder_failed: ") for r in reasons), reasons


def test_skill_rung_cleans_up_when_the_ladder_raises(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    seen: list[Path] = []
    fake_commands.hooks["ladder"] = lambda argv, env: seen.append(Path(argv[1]).parents[3])
    fake_commands.raises["ladder"] = _Boom("ladder")
    with pytest.raises(_Boom):
        _run_skill_rung(source, curie_bin)
    _assert_skill_cleanup_ran(fake_commands)
    assert seen and not seen[0].exists()


@pytest.mark.parametrize(
    "answer", [(0, LISTING, ""), (1, "", "Cannot connect to the Docker daemon\n")]
)
def test_skill_rung_refuses_when_curie_runner_cannot_be_proven_absent(
    tmp_path: Path, fake_commands: FakeCommands, answer: tuple[int, str, str]
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    fake_commands.answers["image ls"] = answer
    with pytest.raises(docker_scenario.CommandError):
        _run_skill_rung(source, curie_bin)
    keys = fake_commands.keys()
    assert "build" not in keys
    assert "image rm" not in keys
    assert "ladder" not in keys


def _leave_behind(fake: FakeCommands) -> None:
    """The ladder leaves a new container, network and volume on the daemon."""

    def ladder(argv: list[str], env: dict[str, str] | None) -> None:
        fake.add("containers", NEW_CONTAINER)
        fake.add("networks", NEW_NETWORK)
        fake.add("volumes", NEW_VOLUME)

    fake.hooks["ladder"] = ladder


def _with_preexisting(fake: FakeCommands) -> None:
    fake.add("containers", PREEXISTING_CONTAINER)
    fake.add("volumes", PREEXISTING_VOLUME)


def test_skill_rung_removes_what_the_failed_ladder_left(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    fake = fake_commands
    source, curie_bin = _skill_source(tmp_path)
    _with_preexisting(fake)
    _leave_behind(fake)
    fake.answers["ladder"] = (1, "", "LADDER FAIL (tiers: skill)\n")
    section = _run_skill_rung(source, curie_bin)

    assert fake.argvs("rm") == [["docker", "rm", "-f", "-v", NEW_CONTAINER]]
    assert fake.argvs("network rm") == [["docker", "network", "rm", NEW_NETWORK]]
    assert fake.argvs("volume rm") == [["docker", "volume", "rm", "-f", NEW_VOLUME]]
    preexisting = {PREEXISTING_CONTAINER, PREEXISTING_VOLUME, *DEFAULT_NETWORKS}
    for key in ("rm", "network rm", "volume rm"):
        for argv in fake.argvs(key):
            assert not preexisting & set(argv), argv
    assert fake.daemon["containers"] == [PREEXISTING_CONTAINER]
    assert fake.daemon["networks"] == DEFAULT_NETWORKS
    assert fake.daemon["volumes"] == [PREEXISTING_VOLUME]

    keys = fake.keys()
    for listing in DAEMON_KINDS_BY_KEY:
        assert keys.index(listing) < keys.index("build"), listing
    last_rm = max(keys.index(k) for k in ("rm", "network rm", "volume rm"))
    assert keys.index("ladder") < min(keys.index(k) for k in ("rm", "network rm", "volume rm"))
    assert last_rm < keys.index("image rm")
    for listing in DAEMON_KINDS_BY_KEY:
        assert keys.index("image rm") < len(keys) - 1 - keys[::-1].index(listing), listing
        assert keys.count(listing) == 3, (listing, keys)
    assert section["cleanup"] == {
        "image_removed": True,
        "containers": [],
        "networks": [],
        "volumes": [],
        "verified": True,
    }
    reasons = docker_scenario.judge_skill_rung(section)
    assert _reasons_with(reasons, "skill_rung_ladder_failed"), reasons
    assert not _reasons_with(reasons, "incomplete_cleanup"), reasons


@pytest.mark.parametrize(
    ("kind", "ident"),
    [("containers", NEW_CONTAINER), ("networks", NEW_NETWORK), ("volumes", NEW_VOLUME)],
)
def test_skill_rung_removal_that_leaves_one_behind_is_not_verified(
    tmp_path: Path, fake_commands: FakeCommands, kind: str, ident: str
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    _with_preexisting(fake_commands)
    _leave_behind(fake_commands)
    fake_commands.sticky.add(ident)
    section = _run_skill_rung(source, curie_bin)
    cleanup = section["cleanup"]
    assert cleanup[kind] == [ident]
    assert [cleanup[k] for k in ("containers", "networks", "volumes") if k != kind] == [[], []]
    assert cleanup["verified"] is False
    for preexisting in (PREEXISTING_CONTAINER, PREEXISTING_VOLUME, *DEFAULT_NETWORKS):
        assert preexisting not in cleanup[kind]
    reasons = _reasons_with(docker_scenario.judge_skill_rung(section), "incomplete_cleanup")
    assert any(kind in r and ident in r for r in reasons), reasons


def test_skill_rung_never_removes_snapshot_objects(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    _with_preexisting(fake_commands)
    section = _run_skill_rung(source, curie_bin)
    keys = fake_commands.keys()
    for removal in ("rm", "network rm", "volume rm"):
        assert removal not in keys, removal
    assert section["cleanup"]["verified"] is True
    assert docker_scenario.judge_skill_rung(section) == []


@pytest.mark.parametrize("key", list(DAEMON_KINDS_BY_KEY))
def test_skill_rung_failed_snapshot_listing_builds_nothing(
    tmp_path: Path, fake_commands: FakeCommands, key: str
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    fake_commands.answers[key] = (1, "", "Cannot connect to the Docker daemon\n")
    with pytest.raises(docker_scenario.CommandError):
        _run_skill_rung(source, curie_bin)
    keys = fake_commands.keys()
    assert "build" not in keys
    assert "ladder" not in keys
    assert "image rm" not in keys


@pytest.mark.parametrize("key", list(DAEMON_KINDS_BY_KEY))
def test_skill_rung_failed_final_listing_is_not_verified(
    tmp_path: Path, fake_commands: FakeCommands, key: str
) -> None:
    source, curie_bin = _skill_source(tmp_path)

    def fail_later(argv: list[str], env: dict[str, str] | None) -> None:
        fake_commands.answers[key] = (1, "", "Cannot connect to the Docker daemon\n")

    fake_commands.hooks["ladder"] = fail_later
    section = _run_skill_rung(source, curie_bin)
    assert section["cleanup"]["verified"] is False


def test_skill_rung_reason_names_leftover_networks(skill_section: dict[str, Any]) -> None:
    skill_section["cleanup"]["networks"] = [NEW_NETWORK]
    reasons = _reasons_with(docker_scenario.judge_skill_rung(skill_section), "incomplete_cleanup")
    assert any(r.startswith("incomplete_cleanup: skill rung left networks") for r in reasons), (
        reasons
    )


# docker env: DOCKER_CONTEXT and BUILDX_BUILDER never reach the collectors' commands


def test_compose_and_skill_rung_docker_env_drops_context_and_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DOCKER_CONTEXT", "elsewhere")
    monkeypatch.setenv("BUILDX_BUILDER", "remote-builder")
    monkeypatch.setenv("DOCKER_HOST", DOCKER_HOST)
    env = docker_scenario._docker_env()
    assert "DOCKER_CONTEXT" not in env and "BUILDX_BUILDER" not in env
    assert env["DOCKER_HOST"] == DOCKER_HOST
    assert env == {
        k: v for k, v in os.environ.items() if k not in ("DOCKER_CONTEXT", "BUILDX_BUILDER")
    }
    env["DOCKER_HOST"] = "tcp://changed:2375"
    assert os.environ["DOCKER_HOST"] == DOCKER_HOST
    assert os.environ["DOCKER_CONTEXT"] == "elsewhere"


@pytest.mark.parametrize("collector", ["compose", "skill_rung"])
def test_compose_and_skill_rung_commands_never_see_context_or_builder(
    tmp_path: Path, fake_commands: FakeCommands, monkeypatch: pytest.MonkeyPatch, collector: str
) -> None:
    monkeypatch.setenv("DOCKER_CONTEXT", "elsewhere")
    monkeypatch.setenv("BUILDX_BUILDER", "remote-builder")
    if collector == "compose":
        _run_compose()
    else:
        source, curie_bin = _skill_source(tmp_path)
        _leave_behind(fake_commands)
        _run_skill_rung(source, curie_bin)
    assert fake_commands.calls
    for argv, _, env in fake_commands.calls:
        assert env is not None, argv
        assert "DOCKER_CONTEXT" not in env and "BUILDX_BUILDER" not in env, argv
        assert env.get("DOCKER_HOST") == DOCKER_HOST, argv
    if collector == "skill_rung":
        ladder_env = fake_commands.call("ladder")[2]
        assert ladder_env is not None
        assert ladder_env["CURIE_E2E_TIERS"] == "skill"
        assert ladder_env["CURIE_E2E_LIVE"] == "0"


def test_skill_rung_image_still_present_after_removal_is_not_verified(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)

    def present_after_rm(argv: list[str], env: dict[str, str] | None) -> None:
        fake_commands.answers["image ls"] = (0, LISTING, "")

    fake_commands.hooks["image rm"] = present_after_rm
    section = _run_skill_rung(source, curie_bin)
    assert section["cleanup"]["verified"] is False


def _drop(rel: str) -> Callable[[Path, Path], None]:
    def mutate(source: Path, curie_bin: Path) -> None:
        path = source / rel
        if path.is_dir():
            for child in sorted(path.rglob("*"), reverse=True):
                child.unlink() if child.is_file() else child.rmdir()
            path.rmdir()
        else:
            path.unlink()

    return mutate


SKILL_INPUT_GAPS: list[tuple[str, Callable[[Path, Path], None]]] = [
    ("curie-bin-absent", lambda source, curie_bin: curie_bin.unlink()),
    ("curie-bin-not-executable", lambda source, curie_bin: curie_bin.chmod(0o644)),
    *[(f"missing-{rel}", _drop(rel)) for rel in LADDER_FILES],
    ("missing-bundle", _drop("examples/weather")),
    ("missing-dockerfile", _drop("runner/Dockerfile")),
]


@pytest.mark.parametrize(
    "mutate", [m for _, m in SKILL_INPUT_GAPS], ids=[i for i, _ in SKILL_INPUT_GAPS]
)
def test_skill_rung_refuses_missing_inputs_before_any_command(
    tmp_path: Path, fake_commands: FakeCommands, mutate: Callable[[Path, Path], None]
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    mutate(source, curie_bin)
    with pytest.raises(docker_scenario.CommandError):
        _run_skill_rung(source, curie_bin)
    assert fake_commands.calls == []


def test_skill_rung_refuses_a_directory_as_curie_bin(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    with pytest.raises(docker_scenario.CommandError):
        _run_skill_rung(source, curie_bin.parent)
    assert fake_commands.calls == []


def test_skill_rung_requires_a_published_host(tmp_path: Path, fake_commands: FakeCommands) -> None:
    source, curie_bin = _skill_source(tmp_path)
    with pytest.raises(docker_scenario.CommandError):
        _run_skill_rung(source, curie_bin, "")
    assert fake_commands.calls == []


@pytest.mark.parametrize("value", [None, ""], ids=["unset", "empty"])
@pytest.mark.parametrize("collector", ["compose", "skill_rung"])
def test_compose_and_skill_rung_require_docker_host(
    tmp_path: Path,
    fake_commands: FakeCommands,
    monkeypatch: pytest.MonkeyPatch,
    collector: str,
    value: str | None,
) -> None:
    if value is None:
        monkeypatch.delenv("DOCKER_HOST", raising=False)
    else:
        monkeypatch.setenv("DOCKER_HOST", value)
    source, curie_bin = _skill_source(tmp_path)
    with pytest.raises(docker_scenario.CommandError):
        if collector == "compose":
            _run_compose()
        else:
            _run_skill_rung(source, curie_bin)
    assert fake_commands.calls == []


# recorded sections (fixtures/docker-scenario, recorded by the docker scenario driver)


def _recorded(name: str) -> dict[str, Any]:
    section = json.loads((FIXTURES / name).read_text())
    assert isinstance(section, dict), name
    return section


@pytest.fixture
def compose_section() -> dict[str, Any]:
    return _recorded("compose-passing.json")


@pytest.fixture
def skill_section() -> dict[str, Any]:
    return _recorded("skill-rung-passing.json")


def _reasons_with(reasons: list[str], prefix: str) -> list[str]:
    return [r for r in reasons if r.startswith(f"{prefix}: ")]


def test_recorded_compose_passing_judges_clean(compose_section: dict[str, Any]) -> None:
    assert docker_scenario.judge_compose(compose_section) == []


def test_recorded_compose_failing_is_an_unreachable_published_port(
    compose_section: dict[str, Any],
) -> None:
    failing = _recorded("compose-failing.json")
    reasons = docker_scenario.judge_compose(failing)
    assert _reasons_with(reasons, "compose_published_port_unreachable"), reasons
    assert failing["published_host"] != compose_section["published_host"]
    assert failing["published"]["status"] != 200
    assert failing["reach"]["exit_code"] == 0
    assert failing["cleanup"]["verified"] is True


def test_recorded_skill_rung_passing_judges_clean(skill_section: dict[str, Any]) -> None:
    assert docker_scenario.judge_skill_rung(skill_section) == []


def test_recorded_skill_rung_failing_is_a_failed_ladder() -> None:
    failing = _recorded("skill-rung-failing.json")
    reasons = docker_scenario.judge_skill_rung(failing)
    assert _reasons_with(reasons, "skill_rung_ladder_failed"), reasons
    assert failing["ladder"]["exit_code"] != 0
    assert failing["ladder"]["passed"] is False
    assert failing["cleanup"]["verified"] is True


def test_recorded_compose_passing_shapes(compose_section: dict[str, Any]) -> None:
    section = compose_section
    expected = section["expected"]
    assert re.fullmatch(r"curie-compose [0-9a-f]+", expected), expected
    assert expected.split()[1] in section["project"]
    assert expected in section["reach"]["body"]
    assert expected in section["published"]["body"]
    assert section["reach"]["from"] == "client" and section["reach"]["to"] == "server:8080"
    port = section["published"]["port"]
    assert isinstance(port, int) and 1 <= port <= 65535
    assert section["published"]["url"] == (f"http://{section['published_host']}:{port}/index.html")
    assert section["published"]["status"] == 200
    assert sorted(s["service"] for s in section["services"]) == ["client", "server"]
    assert section["info"]["id"] and section["info"]["name"]
    assert set(section["files"]) == set(COMPOSE_FILES)
    assert all(HEX64.fullmatch(d) for d in section["files"].values())
    assert section["cleanup"]["verified"] is True


def test_recorded_skill_rung_passing_shapes(skill_section: dict[str, Any]) -> None:
    section = skill_section
    assert docker_scenario.LADDER_PASS in section["ladder"]["output_tail"].splitlines()
    assert section["ladder"]["exit_code"] == 0 and section["ladder"]["passed"] is True
    assert section["ladder"]["tiers"] == "skill" and section["ladder"]["live"] == "0"
    image = section["image"]
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", image["id"]), image["id"]
    assert image["tag"] == "curie-runner"
    assert image["label_nonce"] == section["nonce"]
    assert image["daemon_id"] == section["info"]["id"]
    inputs = section["inputs"]
    assert inputs["bundle"]["path"] == "examples/weather"
    assert set(inputs["files"]) == set(LADDER_FILES)
    assert inputs["curie_bin"]["version"]
    assert re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", inputs["source_revision"])
    assert section["cleanup"]["image_removed"] is True
    assert section["cleanup"]["networks"] == []
    assert section["cleanup"]["verified"] is True


def test_recorded_passing_envelope_embeds_the_recorded_sections(
    envelope: dict[str, Any], compose_section: dict[str, Any], skill_section: dict[str, Any]
) -> None:
    identity = envelope["runs"]["a"]["daemon"]["info"]
    compose = copy.deepcopy(compose_section)
    compose["info"] = dict(identity)
    skill = copy.deepcopy(skill_section)
    skill["info"] = dict(identity)
    skill["image"]["daemon_id"] = identity["id"]
    assert envelope["compose"] == compose
    assert envelope["skill_rung"] == skill


# compose and skill_rung judges: one mutation per reason and per required observation


def _delete(path: tuple[Any, ...]) -> Callable[[dict[str, Any]], None]:
    def mutate(section: dict[str, Any]) -> None:
        node = section
        for key in path[:-1]:
            node = node[key]
        del node[path[-1]]

    return mutate


def _service(section: dict[str, Any], name: str) -> dict[str, Any]:
    return next(s for s in section["services"] if s["service"] == name)


def _drop_service(name: str) -> Callable[[dict[str, Any]], None]:
    def mutate(section: dict[str, Any]) -> None:
        section["services"] = [s for s in section["services"] if s["service"] != name]

    return mutate


def _upper_digest(section: dict[str, Any]) -> None:
    section["files"]["compose.yaml"] = "ABCDEF" + section["files"]["compose.yaml"][6:]


def _other_url_host(section: dict[str, Any]) -> None:
    port = section["published"]["port"]
    section["published"]["url"] = f"http://198.51.100.7:{port}/index.html"


Mutation = tuple[str, str, Callable[[dict[str, Any]], None]]

COMPOSE_MUTATIONS: list[Mutation] = [
    *[
        (f"blank-{field}", "missing_observation", _set((field,), ""))
        for field in ("docker_host", "published_host", "project")
    ],
    *[
        (f"absent-{field}", "missing_observation", _delete((field,)))
        for field in ("docker_host", "published_host", "project", "window", "info")
    ],
    ("blank-info-id", "missing_observation", _set(("info", "id"), "")),
    ("blank-info-name", "missing_observation", _set(("info", "name"), "")),
    ("absent-info-id", "missing_observation", _delete(("info", "id"))),
    ("window-reversed", "missing_observation", _set(("window",), [20.0, 10.0])),
    ("window-nan", "missing_observation", _set(("window",), [float("nan"), 1.0])),
    *[
        (f"digest-not-hex-{rel}", "missing_observation", _set(("files", rel), "x"))
        for rel in COMPOSE_FILES
    ],
    *[
        (f"digest-absent-{rel}", "missing_observation", _delete(("files", rel)))
        for rel in COMPOSE_FILES
    ],
    ("digest-uppercase", "missing_observation", _upper_digest),
    ("build-failed", "compose_build_failed", _set(("build", "exit_code"), 1)),
    ("up-failed", "compose_up_failed", _set(("up", "exit_code"), 1)),
    (
        "server-exited",
        "compose_service_not_running",
        lambda s: _service(s, "server").update(state="exited"),
    ),
    ("client-absent", "compose_service_not_running", _drop_service("client")),
    (
        "server-twice",
        "compose_service_not_running",
        lambda s: s["services"].append(copy.deepcopy(_service(s, "server"))),
    ),
    ("reach-exit", "compose_service_unreachable", _set(("reach", "exit_code"), 1)),
    ("reach-no-marker", "compose_service_unreachable", _set(("reach", "body"), "")),
    *[
        (
            f"port-{value!r}",
            "compose_published_port_unreachable",
            _set(("published", "port"), value),
        )
        for value in (None, 0, 65536, "32768", True)
    ],
    ("status-502", "compose_published_port_unreachable", _set(("published", "status"), 502)),
    ("status-none", "compose_published_port_unreachable", _set(("published", "status"), None)),
    ("published-no-marker", "compose_published_port_unreachable", _set(("published", "body"), "")),
    ("url-other-host", "compose_published_port_unreachable", _other_url_host),
    ("down-failed", "compose_down_failed", _set(("down", "exit_code"), 1)),
    *[
        (f"left-{kind}", "incomplete_cleanup", _set(("cleanup", kind), [LISTING.strip()]))
        for kind, _ in COMPOSE_KINDS
    ],
    ("cleanup-unverified", "incomplete_cleanup", _set(("cleanup", "verified"), False)),
    ("cleanup-verified-text", "incomplete_cleanup", _set(("cleanup", "verified"), "true")),
]


def _bad_daemon(section: dict[str, Any]) -> None:
    section["image"]["daemon_id"] = "00000000-0000-4000-8000-000000000000"


SKILL_RUNG_MUTATIONS: list[Mutation] = [
    *[
        (f"blank-{field}", "missing_observation", _set((field,), ""))
        for field in ("docker_host", "published_host")
    ],
    *[
        (f"absent-{field}", "missing_observation", _delete((field,)))
        for field in ("docker_host", "published_host", "info", "window")
    ],
    ("blank-info-id", "missing_observation", _set(("info", "id"), "")),
    ("window-reversed", "missing_observation", _set(("window",), [20.0, 10.0])),
    ("curie-bin-sha-blank", "missing_observation", _set(("inputs", "curie_bin", "sha256"), "")),
    ("curie-bin-sha-not-hex", "missing_observation", _set(("inputs", "curie_bin", "sha256"), "x")),
    ("curie-bin-absent", "missing_observation", _delete(("inputs", "curie_bin"))),
    ("version-blank", "missing_observation", _set(("inputs", "curie_bin", "version"), "")),
    ("version-absent", "missing_observation", _delete(("inputs", "curie_bin", "version"))),
    *[
        (f"digest-not-hex-{rel}", "missing_observation", _set(("inputs", "files", rel), "x"))
        for rel in LADDER_FILES
    ],
    *[
        (f"digest-absent-{rel}", "missing_observation", _delete(("inputs", "files", rel)))
        for rel in LADDER_FILES
    ],
    (
        "bundle-other-path",
        "missing_observation",
        _set(("inputs", "bundle", "path"), "examples/other"),
    ),
    ("bundle-digest-not-hex", "missing_observation", _set(("inputs", "bundle", "digest"), "x")),
    ("bundle-absent", "missing_observation", _delete(("inputs", "bundle"))),
    ("revision-short", "missing_observation", _set(("inputs", "source_revision"), "4f1c2b9")),
    ("revision-absent", "missing_observation", _delete(("inputs", "source_revision"))),
    ("image-exit", "skill_rung_image_not_built", _set(("image", "exit_code"), 1)),
    ("image-tag", "skill_rung_image_not_built", _set(("image", "tag"), "curie-runner:other")),
    ("image-id-blank", "skill_rung_image_not_built", _set(("image", "id"), "")),
    ("image-id-short", "skill_rung_image_not_built", _set(("image", "id"), "sha256:3b9f")),
    ("image-id-bare", "skill_rung_image_not_built", _set(("image", "id"), RUNNER_ID[7:])),
    ("image-dockerfile", "skill_rung_image_not_built", _set(("image", "dockerfile"), "Dockerfile")),
    (
        "image-dockerfile-sha",
        "skill_rung_image_not_built",
        _set(("image", "dockerfile_sha256"), "x"),
    ),
    ("image-label-nonce", "skill_rung_image_not_built", _set(("image", "label_nonce"), "ffffffff")),
    ("image-daemon-id", "skill_rung_image_not_built", _bad_daemon),
    ("tiers-all", "skill_rung_wrong_mode", _set(("ladder", "tiers"), "skill,local")),
    ("live-1", "skill_rung_wrong_mode", _set(("ladder", "live"), "1")),
    ("live-int", "skill_rung_wrong_mode", _set(("ladder", "live"), 0)),
    ("ladder-exit", "skill_rung_ladder_failed", _set(("ladder", "exit_code"), 1)),
    ("ladder-not-passed", "skill_rung_ladder_failed", _set(("ladder", "passed"), False)),
    ("ladder-passed-text", "skill_rung_ladder_failed", _set(("ladder", "passed"), "true")),
    ("image-not-removed", "incomplete_cleanup", _set(("cleanup", "image_removed"), False)),
    ("left-containers", "incomplete_cleanup", _set(("cleanup", "containers"), ["4c1d9e2f7a3b"])),
    ("left-volumes", "incomplete_cleanup", _set(("cleanup", "volumes"), ["curie-volume"])),
    ("left-networks", "incomplete_cleanup", _set(("cleanup", "networks"), [NEW_NETWORK])),
    ("cleanup-networks-absent", "missing_observation", _delete(("cleanup", "networks"))),
    ("cleanup-networks-text", "missing_observation", _set(("cleanup", "networks"), "none")),
    ("cleanup-networks-null", "missing_observation", _set(("cleanup", "networks"), None)),
    ("cleanup-unverified", "incomplete_cleanup", _set(("cleanup", "verified"), False)),
]


def _assert_section_prefix(
    judge: Callable[[Any], list[str]], section: Any, prefix: str, where: str
) -> list[str]:
    reasons = judge(section)
    assert isinstance(reasons, list) and all(isinstance(r, str) for r in reasons), reasons
    found = _reasons_with(reasons, prefix)
    if prefix == "missing_observation":
        found = [r for r in found if r.startswith(f"missing_observation: {where}")]
    assert found, (prefix, reasons)
    return reasons


@pytest.mark.parametrize(
    ("prefix", "mutate"),
    [(p, m) for _, p, m in COMPOSE_MUTATIONS],
    ids=[i for i, _, _ in COMPOSE_MUTATIONS],
)
def test_compose_mutation_fails_with_reason(
    compose_section: dict[str, Any], prefix: str, mutate: Callable[[dict[str, Any]], None]
) -> None:
    mutate(compose_section)
    _assert_section_prefix(docker_scenario.judge_compose, compose_section, prefix, "compose")


@pytest.mark.parametrize(
    ("prefix", "mutate"),
    [(p, m) for _, p, m in SKILL_RUNG_MUTATIONS],
    ids=[i for i, _, _ in SKILL_RUNG_MUTATIONS],
)
def test_skill_rung_mutation_fails_with_reason(
    skill_section: dict[str, Any], prefix: str, mutate: Callable[[dict[str, Any]], None]
) -> None:
    mutate(skill_section)
    _assert_section_prefix(docker_scenario.judge_skill_rung, skill_section, prefix, "skill_rung")


def test_compose_and_skill_rung_every_prefix_has_a_mutation() -> None:
    assert {p for _, p, _ in COMPOSE_MUTATIONS} == {
        "missing_observation", "compose_build_failed", "compose_up_failed",
        "compose_service_not_running", "compose_service_unreachable",
        "compose_published_port_unreachable", "compose_down_failed", "incomplete_cleanup",
    }  # fmt: skip
    assert {p for _, p, _ in SKILL_RUNG_MUTATIONS} == {
        "missing_observation", "skill_rung_image_not_built", "skill_rung_wrong_mode",
        "skill_rung_ladder_failed", "incomplete_cleanup",
    }  # fmt: skip


def test_compose_and_skill_rung_reasons_reach_the_envelope_judge(
    envelope: dict[str, Any],
) -> None:
    envelope["compose"]["build"]["exit_code"] = 1
    envelope["skill_rung"]["ladder"]["exit_code"] = 1
    reasons = _assert_prefix(envelope, "compose_build_failed")
    assert _reasons_with(reasons, "skill_rung_ladder_failed"), reasons


def _paths(node: Any, prefix: tuple[Any, ...] = ()) -> Iterator[tuple[Any, ...]]:
    """Every key path into the section, dict keys and the first element of each list."""

    if isinstance(node, dict):
        for key, value in node.items():
            yield (*prefix, key)
            yield from _paths(value, (*prefix, key))
    elif isinstance(node, list) and node:
        yield (*prefix, 0)
        yield from _paths(node[0], (*prefix, 0))


GARBAGE: list[Any] = [None, [], "x", 3]


@pytest.mark.parametrize("garbage", GARBAGE, ids=repr)
@pytest.mark.parametrize(
    ("judge", "name"),
    [
        (lambda s: docker_scenario.judge_compose(s), "compose-passing.json"),
        (lambda s: docker_scenario.judge_skill_rung(s), "skill-rung-passing.json"),
    ],
    ids=["compose", "skill_rung"],
)
def test_compose_and_skill_rung_garbage_never_raises(
    judge: Callable[[Any], list[str]], name: str, garbage: Any
) -> None:
    reasons = judge(garbage)
    assert reasons and all(isinstance(r, str) for r in reasons), reasons
    passing = _recorded(name)
    for path in _paths(passing):
        section = copy.deepcopy(passing)
        _set(path, garbage)(section)
        reasons = judge(section)
        assert isinstance(reasons, list) and all(isinstance(r, str) for r in reasons), path


# envelope integration


IMPOSTOR = "00000000-0000-4000-8000-000000000000"


def _set_daemon(envelope: dict[str, Any], section: str, daemon_id: str) -> None:
    envelope[section]["info"]["id"] = daemon_id
    if section == "skill_rung":
        envelope[section]["image"]["daemon_id"] = daemon_id


@pytest.mark.parametrize("section", ["compose", "skill_rung"])
def test_compose_and_skill_rung_daemon_must_be_a_runs_daemon(
    envelope: dict[str, Any], section: str
) -> None:
    _set_daemon(envelope, section, IMPOSTOR)
    reasons = _assert_prefix(envelope, "identity_mismatch")
    assert any(section in r and IMPOSTOR in r for r in _reasons_with(reasons, "identity_mismatch"))


@pytest.mark.parametrize("section", ["compose", "skill_rung"])
def test_compose_and_skill_rung_on_run_b_daemon_pass(
    envelope: dict[str, Any], section: str
) -> None:
    _set_daemon(envelope, section, envelope["runs"]["b"]["daemon"]["info"]["id"])
    assert docker_scenario.judge(envelope) == []


def test_assemble_embeds_compose_and_skill_rung_verbatim(
    envelope: dict[str, Any], lifecycle: dict[str, Any]
) -> None:
    compose = {"project": "sentinel-compose"}
    skill = {"nonce": "sentinel-skill"}
    assembled = docker_scenario.assemble(
        NONCE, envelope["runs"], lifecycle, envelope["upgrade"], envelope["ci"], compose, skill
    )
    assert assembled["compose"] == compose
    assert assembled["skill_rung"] == skill


def test_cli_assemble_embeds_compose_and_skill_rung(
    tmp_path: Path, envelope: dict[str, Any]
) -> None:
    files = {}
    for name, value in (
        ("probe-a", envelope["runs"]["a"]["probe"]),
        ("daemon-a", envelope["runs"]["a"]["daemon"]),
        ("probe-b", envelope["runs"]["b"]["probe"]),
        ("daemon-b", envelope["runs"]["b"]["daemon"]),
        ("upgrade", envelope["upgrade"]),
        ("ci", envelope["ci"]),
        ("compose", envelope["compose"]),
        ("skill-rung", envelope["skill_rung"]),
    ):
        files[name] = tmp_path / f"{name}.json"
        files[name].write_text(json.dumps(value))
    out = tmp_path / "envelope.json"
    args = [
        str(SCRIPT), "assemble", "--nonce", NONCE,
        "--run", f"a={files['probe-a']},{files['daemon-a']}",
        "--run", f"b={files['probe-b']},{files['daemon-b']}",
        "--claim", f"a={envelope['runs']['a']['claim']}",
        "--claim", f"b={envelope['runs']['b']['claim']}",
        "--lifecycle", str(LIFECYCLE), "--upgrade", str(files["upgrade"]),
        "--ci", str(files["ci"]), "--out", str(out),
    ]  # fmt: skip
    for missing in ("--compose", "--skill-rung"):
        given = {"--compose": files["compose"], "--skill-rung": files["skill-rung"]}
        del given[missing]
        extra = [part for flag, path in given.items() for part in (flag, str(path))]
        result = _run(*args, *extra)
        assert result.returncode == 2, result.stderr
        assert missing in result.stderr
    done = _run(*args, "--compose", str(files["compose"]), "--skill-rung", str(files["skill-rung"]))
    assert done.returncode == 0, done.stderr
    assert json.loads(out.read_text()) == envelope


def test_help_names_compose_skill_rung_and_the_docker_scenario_driver() -> None:
    result = _run(str(SCRIPT), "--help")
    assert result.returncode == 0, result.stderr
    text = " ".join(result.stdout.split())
    for word in ("compose", "skill_rung", "skill-rung", "docker scenario driver", "w2 waypoint"):
        assert word in text, word
    for command in ("compose", "skill-rung"):
        sub = _run(str(SCRIPT), command, "--help")
        assert sub.returncode == 0, sub.stderr


def test_cli_compose_writes_the_section_with_the_env_published_host(
    tmp_path: Path, fake_commands: FakeCommands, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DOCKER_PUBLISHED_HOST", PUBLISHED_HOST)
    out = tmp_path / "compose.json"
    code = docker_scenario.main(
        ["compose", "--nonce", NONCE, "--image", COMPOSE_IMAGE, "--out", str(out)]
    )
    assert code == 0
    section = json.loads(out.read_text())
    assert section["published_host"] == PUBLISHED_HOST
    assert docker_scenario.judge_compose(section) == []


def test_cli_compose_exits_1_after_writing_when_cleanup_is_not_verified(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    fake_commands.answers["volume ls"] = (0, LISTING, "")
    out = tmp_path / "compose.json"
    code = docker_scenario.main(
        [
            "compose", "--nonce", NONCE, "--image", COMPOSE_IMAGE,
            "--published-host", PUBLISHED_HOST, "--out", str(out),
        ]
    )  # fmt: skip
    assert code == 1
    assert json.loads(out.read_text())["cleanup"]["verified"] is False


def _skill_rung_args(source: Path, out: Path) -> list[str]:
    return [
        "skill-rung", "--nonce", NONCE, "--source", str(source),
        "--source-revision", SOURCE_REVISION, "--published-host", PUBLISHED_HOST,
        "--out", str(out),
    ]  # fmt: skip


def test_cli_skill_rung_curie_bin_defaults_to_env(
    tmp_path: Path, fake_commands: FakeCommands, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    monkeypatch.setenv("CURIE_BIN", str(curie_bin))
    provisioned: list[bytes] = []
    fake_commands.hooks["ladder"] = lambda argv, env: provisioned.append(
        Path((env or {})["CURIE_BIN"]).read_bytes()
    )
    out = tmp_path / "skill-rung.json"
    assert docker_scenario.main(_skill_rung_args(source, out)) == 0
    section = json.loads(out.read_text())
    assert section["inputs"]["curie_bin"]["sha256"] == _sha256(curie_bin.read_bytes())
    assert provisioned == [curie_bin.read_bytes()]
    assert docker_scenario.judge_skill_rung(section) == []


def test_cli_skill_rung_exits_1_after_writing_when_cleanup_is_not_verified(
    tmp_path: Path, fake_commands: FakeCommands
) -> None:
    source, curie_bin = _skill_source(tmp_path)
    _leave_behind(fake_commands)
    fake_commands.sticky.add(NEW_CONTAINER)
    out = tmp_path / "skill-rung.json"
    code = docker_scenario.main([*_skill_rung_args(source, out), "--curie-bin", str(curie_bin)])
    assert code == 1
    assert json.loads(out.read_text())["cleanup"]["verified"] is False


# review round 2: ladder env and process-group timeouts


def test_skill_rung_ladder_env_carries_only_its_own_curie_e2e_keys(
    tmp_path: Path, fake_commands: FakeCommands, monkeypatch: pytest.MonkeyPatch
) -> None:
    inherited = {
        "CURIE_E2E_IMAGE": "ghcr.io/example/other-runner:dev",
        "CURIE_E2E_PORT": "18080",
        "CURIE_E2E_NETWORK": "other-net",
        "CURIE_E2E_BUNDLE": "examples/other",
        "CURIE_E2E_CONNECTOR_BUNDLE": "examples/connector",
        "CURIE_E2E_HOOK_APPROVAL": "1",
        "CURIE_E2E_TIERS": "skill,local",
        "CURIE_E2E_LIVE": "1",
    }
    for name, value in inherited.items():
        monkeypatch.setenv(name, value)
    source, curie_bin = _skill_source(tmp_path)
    _run_skill_rung(source, curie_bin)
    env = fake_commands.call("ladder")[2]
    assert env is not None
    assert {k: v for k, v in env.items() if k.startswith("CURIE_E2E_")} == {
        "CURIE_E2E_TIERS": "skill",
        "CURIE_E2E_LIVE": "0",
        "CURIE_E2E_IMAGE": "curie-runner",
    }


def _gone(pid: int) -> bool:
    """True once pid no longer exists or is a zombie awaiting its reaper."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return True
    return state == "Z"


def test_skill_rung_command_timeout_kills_the_process_group(tmp_path: Path) -> None:
    pid_file = tmp_path / "pid"
    started = time.monotonic()
    code, _, err = docker_scenario._command(
        ["bash", "-c", f"sleep 30 >/dev/null 2>&1 & echo $! > {pid_file}; wait"], timeout=1
    )
    assert code == 124, err
    pid = int(pid_file.read_text().strip())
    deadline = time.monotonic() + 2
    while not _gone(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    try:
        assert _gone(pid), f"background sleep {pid} outlived the timed-out command"
    finally:
        if not _gone(pid):
            os.kill(pid, 9)
    assert time.monotonic() - started < 5


def test_skill_rung_command_within_timeout_keeps_code_and_output() -> None:
    code, out, err = docker_scenario._command(
        ["bash", "-c", "echo out; echo err >&2; exit 3"], timeout=10
    )
    assert (code, out, err) == (3, "out\n", "err\n")


def test_skill_rung_command_missing_binary_is_127() -> None:
    code, out, _ = docker_scenario._command(["curie-no-such-binary-for-tests"], timeout=5)
    assert (code, out) == (127, "")

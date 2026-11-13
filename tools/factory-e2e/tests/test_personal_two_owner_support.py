"""Hermetic contract for the two-owner scenario's pure core (#5379).

The live scenario needs a throwaway cluster, two independently enrolled grants
and an owned forge, so it is proved by ``personal_profile.py --scenario
two-owner run``. These tests pin what must hold before anything paid: every
inputs-file refusal with its authorized control, the start manifest and the
unpaid schedule check, the owner-aware ledger's reservations and guards, the
gate decision and the in-pod gate programs (run here as real processes), the
two-owner install values and grant scan, the OIDC login flow against scripted
Dex and API redirects, the issue fixtures and their CI acknowledgement test,
and the judges against complete and incomplete synthetic ledgers.

Nothing here touches a cluster, GitHub, Docker or a network, and no value is a
real credential or identifier.
"""

from __future__ import annotations

import ast
import atexit
import base64
import contextlib
import copy
import dataclasses
import functools
import hashlib
import json
import os
import queue
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import urllib.parse
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import personal_profile_support as pps
import personal_two_owner_support as pto
import pytest
from two_owner_shared import _refused_record

HERE = Path(pto.__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
T0 = 1_800_000_000.0
CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
RUN_ID = "acme-two-owner-1"
GRANT_A = "synthetic-owner-a-grant-not-a-token"
GRANT_B = "synthetic-owner-b-grant-not-a-token"
SLACK_BOT = "xoxb-synthetic-gateway-bot-not-a-token"
CHANNEL = "C0EXAMPLE1"
REPO = "acme-corp/acme-fixture"
# A GKE-shaped context name: long, lower case, digits; it must not read as a token.
CONTEXT = "gke_acme-project-123_us-central1-a_acme-cluster"
WINDOW = T0 + pto.GRANT_MIN_LIFETIME + 3600


# --------------------------------------------------------------------------
# Builders (no provisioning fixtures)
# --------------------------------------------------------------------------


def _private(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _file(path: Path, content: bytes, mode: int = 0o600) -> Path:
    path.write_bytes(content)
    os.chmod(path, mode)
    return path


def _kit(tmp_path: Path) -> dict[str, Any]:
    """A valid live inputs document whose every named file exists."""

    kit = _private(tmp_path / "kit")
    app = _private(kit / "app")
    (app / "app.json").write_text(json.dumps({"id": 42, "installation_id": 7, "repo": REPO}))
    (app / "app.pem").write_text("placeholder, never read here\n")
    kubeconfig = _file(kit / "kubeconfig", b"apiVersion: v1\nkind: Config\n")
    slack = _file(kit / "slack-bot", SLACK_BOT.encode() + b"\n")
    grant_a = _file(kit / "grant-a", GRANT_A.encode() + b"\n")
    grant_b = _file(kit / "grant-b", GRANT_B.encode() + b"\n")
    return {
        "target": {
            "context": CONTEXT,
            "namespace": "pto-acme-1",
            "release": "pto-acme",
            "registry": "localhost:5999",
            "docker_context": "acme-docker",
            "kubeconfig": str(kubeconfig),
        },
        "forge": {"app_dir": str(app)},
        "gateway": {"slack_bot_token_file": str(slack), "resolution_channel": CHANNEL},
        "owners": {
            "a": {"grant_file": str(grant_a), "usable_until": int(WINDOW)},
            "b": {"grant_file": str(grant_b), "usable_until": "2027-01-15T10:00:00+00:00"},
        },
        "independent_accounts": True,
        "prior_item_output_tokens": 0,
        "probe_cli": "claude",
    }


def _setup_document(tmp_path: Path) -> dict[str, Any]:
    document = _kit(tmp_path)
    for key in ("owners", "independent_accounts"):
        document.pop(key)
    return document


def _inputs_file(tmp_path: Path, document: Any, mode: int = 0o600) -> Path:
    folder = _private(tmp_path / "inputs")
    text = document if isinstance(document, str) else json.dumps(document)
    return _file(folder / "two-owner.json", text.encode(), mode)


def _load(
    tmp_path: Path, document: Any, mode: Literal["setup", "live"] = "live"
) -> pto.ScenarioInputs:
    path = _inputs_file(tmp_path, document)
    return pto.load_inputs_file(path, mode=mode, now=T0)


def _refused(code: str, call: Callable[[], Any]) -> pps.Refusal:
    with pytest.raises(pps.Refusal) as caught:
        call()
    assert caught.value.code == code, (caught.value.code, caught.value.detail)
    return caught.value


def _set(document: dict[str, Any], dotted: str, value: Any) -> None:
    *path, last = dotted.split(".")
    node = document
    for part in path:
        node = node[part]
    node[last] = value


def _drop(document: dict[str, Any], dotted: str) -> None:
    *path, last = dotted.split(".")
    node = document
    for part in path:
        node = node[part]
    node.pop(last)


# --------------------------------------------------------------------------
# A. Ceilings and approval
# --------------------------------------------------------------------------


def test_ceilings_are_the_approved_numbers() -> None:
    assert (pto.MAX_SESSIONS, pto.MAX_TURNS, pto.SESSION_TURNS) == (16, 192, 32)
    assert (pto.MAX_OUTPUT_TOKENS, pto.SESSION_OUTPUT_TOKENS, pto.OWNER_OUTPUT_TOKENS) == (
        32768,
        8192,
        16384,
    )
    assert (pto.SESSION_SECONDS, pto.LIVE_SECONDS, pto.CLEANUP_SECONDS) == (300, 4800, 600)
    assert (pto.PAID_RETRIES, pto.PROBE_OUTPUT_TOKENS, pto.PROBE_SECONDS) == (0, 32, 30)
    assert pto.PROBE_MODEL == "claude-haiku-5-5" == pps.GRANT_PROBE_MODEL
    assert pto.RESPONSE_OUTPUT_TOKENS == 2048
    assert (pto.EXECUTION_DEADLINE, pto.HOLD_SECONDS, pto.HOOK_TIMEOUT) == (3600, 240, 250)
    assert pto.HOLD_SECONDS < pto.HOOK_TIMEOUT
    assert pto.OWNERS == ("a", "b")


def test_approved_ceiling_is_canonical_and_accepted() -> None:
    text = pto.approved_ceiling()
    assert text == (
        "sessions=16,turns=192,session-turns=32,output-tokens=32768,"
        "session-output-tokens=8192,owner-output-tokens=16384,session-seconds=300,"
        "live-seconds=4800,cleanup-seconds=600,paid-retries=0,probe-model=claude-haiku-5-5,"
        "probe-output-tokens=32,probe-turns=1,probe-seconds=30"
    )
    pto.require_live_approval(text)


@pytest.mark.parametrize(
    "text",
    [
        None,
        "",
        pps.approved_ceiling(),
        pto.approved_ceiling().replace("sessions=16", "sessions=17"),
        pto.approved_ceiling() + ",",
        " " + pto.approved_ceiling(),
    ],
)
def test_live_approval_refuses_anything_else(text: str | None) -> None:
    _refused("live_not_approved", lambda: pto.require_live_approval(text))


def test_new_refusal_codes_have_fix_texts() -> None:
    for code in pto.FIXES:
        assert pps.Refusal(code).payload()["fix"] == pps.FIXES[code]
    # The single-profile texts are kept, never overwritten.
    assert pps.FIXES["lifetime_insufficient"].startswith("use a grant")


def test_module_has_no_assert_statements() -> None:
    tree = ast.parse(Path(pto.__file__).read_text())
    assert not [node for node in ast.walk(tree) if isinstance(node, ast.Assert)]


# --------------------------------------------------------------------------
# B. Inputs file
# --------------------------------------------------------------------------


def test_live_inputs_load_both_owners_without_grant_bytes(tmp_path: Path) -> None:
    inputs = _load(tmp_path, _kit(tmp_path))
    assert inputs.mode == "live" and inputs.independent_accounts is True
    assert [item.owner for item in inputs.owners] == ["a", "b"]
    assert inputs.owner("a").usable_until == WINDOW
    # T0 is 2027-01-15T08:00:00Z, so the ISO bound is two hours later.
    assert inputs.owner("b").usable_until == T0 + 7200
    assert inputs.target.context == CONTEXT
    assert inputs.target.registry == "localhost:5999" and inputs.target.registry_port == 5999
    assert inputs.target.grant_namespace == "pto-acme-1-grants"
    assert inputs.gateway.resolution_channel == CHANNEL
    assert inputs.prior_item_output_tokens == 0 and inputs.probe_cli == "claude"
    held = str(dataclasses.asdict(inputs))
    assert GRANT_A not in held and GRANT_B not in held
    for shown in (repr(inputs), repr(inputs.target), repr(inputs.owners), repr(inputs.forge)):
        assert str(tmp_path) not in shown and CONTEXT not in shown


def test_setup_inputs_load_without_owners(tmp_path: Path) -> None:
    inputs = _load(tmp_path, _setup_document(tmp_path), mode="setup")
    assert inputs.mode == "setup" and inputs.owners == () and not inputs.independent_accounts
    _refused("inputs_invalid", lambda: inputs.owner("a"))


def test_lifetime_boundary_is_accepted(tmp_path: Path) -> None:
    document = _kit(tmp_path)
    _set(document, "owners.a.usable_until", int(T0 + pto.GRANT_MIN_LIFETIME))
    _set(document, "owners.b.usable_until", int(T0 + pto.GRANT_MIN_LIFETIME))
    inputs = _load(tmp_path, document)
    assert inputs.owner("b").usable_until - T0 == pto.GRANT_MIN_LIFETIME


def _unsafe_mode(path: Path) -> None:
    os.chmod(path, 0o640)


def _unsafe_world(path: Path) -> None:
    os.chmod(path, 0o644)


def _unsafe_readonly(path: Path) -> None:
    os.chmod(path, 0o400)


def _unsafe_symlink(path: Path) -> None:
    real = path.with_name("real.json")
    path.rename(real)
    path.symlink_to(real)


def _unsafe_large(path: Path) -> None:
    path.write_bytes(b" " * (pto.INPUTS_LIMIT + 1))
    os.chmod(path, 0o600)


def _unsafe_parent(path: Path) -> None:
    os.chmod(path.parent, 0o770)


def _unsafe_directory(path: Path) -> None:
    path.unlink()
    path.mkdir(mode=0o700)


def _unsafe_missing(path: Path) -> None:
    path.unlink()


@pytest.mark.parametrize(
    "unsafe",
    [
        _unsafe_mode,
        _unsafe_world,
        _unsafe_readonly,
        _unsafe_symlink,
        _unsafe_large,
        _unsafe_parent,
        _unsafe_directory,
        _unsafe_missing,
    ],
)
def test_unsafe_inputs_file_is_refused(tmp_path: Path, unsafe: Callable[[Path], None]) -> None:
    path = _inputs_file(tmp_path, _kit(tmp_path))
    unsafe(path)
    _refused("inputs_unsafe", lambda: pto.load_inputs_file(path, mode="live", now=T0))


def test_foreign_owned_inputs_file_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _inputs_file(tmp_path, _kit(tmp_path))
    real = os.getuid()
    monkeypatch.setattr(pto.os, "getuid", lambda: real + 1)
    _refused("inputs_unsafe", lambda: pto.load_inputs_file(path, mode="live", now=T0))


def _anthropic_shaped() -> str:
    return "sk-" + "ant-" + "oat01-" + "synthetic"


INLINE_CORPUS: list[tuple[str, Any]] = [
    ("owners.a.grant", "inline value"),
    ("owners.b.token", "inline value"),
    ("owners.a.oauth_token", "inline value"),
    ("gateway.slack_bot_token", "xoxb-" + "synthetic"),
    ("target.password", "value"),
    ("target.api_key", "value"),
    ("forge.private_key", "value"),
    ("owner_session", "value"),
    ("target.docker_context", _anthropic_shaped()),
    ("target.context", "Bearer abc"),
    ("forge.app_dir", "/kit/" + "Ab1" * 12),
    ("probe_cli", "-----BEGIN " + "PRIVATE KEY-----"),
    ("gateway.resolution_channel", "gh" + "p_" + "synthetic"),
]


@pytest.mark.parametrize(("field", "value"), INLINE_CORPUS)
def test_inline_grant_values_are_refused(tmp_path: Path, field: str, value: Any) -> None:
    document = _kit(tmp_path)
    _set(document, field, value)
    _refused("inline_grant_refused", lambda: _load(tmp_path, document))


def test_inline_grant_refused_in_setup_too(tmp_path: Path) -> None:
    document = _setup_document(tmp_path)
    _set(document, "target.docker_context", _anthropic_shaped())
    _refused("inline_grant_refused", lambda: _load(tmp_path, document, mode="setup"))


@pytest.mark.parametrize(
    "text",
    [
        "sk-" + "ant-" + "x",
        "xox" + "b-1",
        "Bearer token-value",
        "Ab1" * 11,
        "eyJ" + "a" * 10 + ".body",
    ],
)
def test_token_shapes(text: str) -> None:
    assert pto.token_shaped(text)


@pytest.mark.parametrize(
    "text",
    [CONTEXT, "/home/acme/.kube/config", "localhost:5999", "claude", "C0EXAMPLE1", "a" * 64],
)
def test_names_and_paths_are_not_token_shaped(text: str) -> None:
    assert not pto.token_shaped(text)


INVALID_CORPUS: list[tuple[str, Callable[[dict[str, Any]], None]]] = [
    ("top-level key", lambda doc: _set(doc, "extra", 1)),
    ("target key", lambda doc: _set(doc, "target.extra", "x")),
    ("forge key", lambda doc: _set(doc, "forge.extra", "x")),
    ("gateway key", lambda doc: _set(doc, "gateway.extra", "x")),
    ("third owner", lambda doc: _set(doc, "owners.c", {})),
    ("owner key", lambda doc: _set(doc, "owners.a.extra", "x")),
    ("owner missing", lambda doc: _drop(doc, "owners.b")),
    ("owner field missing", lambda doc: _drop(doc, "owners.a.usable_until")),
    ("target field missing", lambda doc: _drop(doc, "target.kubeconfig")),
    ("section not an object", lambda doc: _set(doc, "forge", "x")),
    ("relative path", lambda doc: _set(doc, "target.kubeconfig", "kubeconfig")),
    ("missing kubeconfig file", lambda doc: _set(doc, "target.kubeconfig", "/nonexistent/kc")),
    ("bad namespace", lambda doc: _set(doc, "target.namespace", "Not_A_Label")),
    ("long namespace", lambda doc: _set(doc, "target.namespace", "n" * 57)),
    ("bad context", lambda doc: _set(doc, "target.context", "has space")),
    ("prior missing", lambda doc: _drop(doc, "prior_item_output_tokens")),
    ("prior negative", lambda doc: _set(doc, "prior_item_output_tokens", -1)),
    ("prior too large", lambda doc: _set(doc, "prior_item_output_tokens", 32769)),
    ("prior bool", lambda doc: _set(doc, "prior_item_output_tokens", True)),
    ("prior string", lambda doc: _set(doc, "prior_item_output_tokens", "0")),
    ("probe cli space", lambda doc: _set(doc, "probe_cli", "claude --x")),
    ("list value", lambda doc: _set(doc, "probe_cli", ["claude"])),
]


@pytest.mark.parametrize(("case", "mutate"), INVALID_CORPUS, ids=[c for c, _ in INVALID_CORPUS])
def test_invalid_inputs_are_refused(
    tmp_path: Path, case: str, mutate: Callable[[dict[str, Any]], None]
) -> None:
    document = _kit(tmp_path)
    mutate(document)
    _refused("inputs_invalid", lambda: _load(tmp_path, document))


@pytest.mark.parametrize(
    "text", ['{"target": {}, "target": {}}', "[1, 2]", "not json", '"string"', "\xff"]
)
def test_unparseable_or_duplicate_json_is_refused(tmp_path: Path, text: str) -> None:
    path = _inputs_file(tmp_path, "{}")
    path.write_bytes(text.encode("latin-1"))
    _refused("inputs_invalid", lambda: pto.load_inputs_file(path, mode="live", now=T0))


def test_validators_name_the_field(tmp_path: Path) -> None:
    document = _kit(tmp_path)
    _set(document, "target.release", "curie")
    assert _refused("release_reserved", lambda: _load(tmp_path, document)).detail == (
        "target.release"
    )
    document = _kit(tmp_path / "2")
    _set(document, "target.registry", "192.0.2.10:5000")
    _refused("registry_invalid", lambda: _load(tmp_path / "2", document))
    document = _kit(tmp_path / "3")
    _set(document, "gateway.resolution_channel", "general")
    _refused("slack_kit_invalid", lambda: _load(tmp_path / "3", document))
    document = _kit(tmp_path / "4")
    os.unlink(document["forge"]["app_dir"] + "/app.pem")
    _refused("app_kit_invalid", lambda: _load(tmp_path / "4", document))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda doc: _set(doc, "owners", {}),
        lambda doc: _set(doc, "independent_accounts", True),
        lambda doc: _set(doc, "independent_accounts", False),
        lambda doc: _set(doc, "target.grant_file", "/kit/grant-a"),
    ],
)
def test_setup_inputs_carrying_grants_are_refused(
    tmp_path: Path, mutate: Callable[[dict[str, Any]], None]
) -> None:
    document = _setup_document(tmp_path)
    mutate(document)
    _refused("setup_inputs_carry_grants", lambda: _load(tmp_path, document, mode="setup"))


def test_live_document_is_refused_in_setup_mode(tmp_path: Path) -> None:
    _refused("setup_inputs_carry_grants", lambda: _load(tmp_path, _kit(tmp_path), mode="setup"))


@pytest.mark.parametrize("value", ["missing", False, 1, "true", None])
def test_independence_must_be_asserted_true(tmp_path: Path, value: Any) -> None:
    document = _kit(tmp_path)
    if value == "missing":
        document.pop("independent_accounts")
    else:
        document["independent_accounts"] = value
    _refused("independence_unasserted", lambda: _load(tmp_path, document))


def _same_path(document: dict[str, Any]) -> None:
    document["owners"]["b"]["grant_file"] = document["owners"]["a"]["grant_file"]


def _hard_link(document: dict[str, Any]) -> None:
    linked = Path(document["owners"]["a"]["grant_file"]).with_name("grant-a-link")
    os.link(document["owners"]["a"]["grant_file"], linked)
    document["owners"]["b"]["grant_file"] = str(linked)


def _same_content(document: dict[str, Any]) -> None:
    Path(document["owners"]["b"]["grant_file"]).write_bytes(GRANT_A.encode() + b"\n")


def _same_content_without_newline(document: dict[str, Any]) -> None:
    Path(document["owners"]["b"]["grant_file"]).write_bytes(GRANT_A.encode())


def _dotted_path(document: dict[str, Any]) -> None:
    grant = Path(document["owners"]["a"]["grant_file"])
    document["owners"]["b"]["grant_file"] = str(grant.parent / "." / grant.name)


@pytest.mark.parametrize(
    "duplicate",
    [_same_path, _hard_link, _same_content, _same_content_without_newline, _dotted_path],
)
def test_duplicate_owner_grants_are_refused(
    tmp_path: Path, duplicate: Callable[[dict[str, Any]], None]
) -> None:
    document = _kit(tmp_path)
    duplicate(document)
    _refused("duplicate_owner_grant", lambda: _load(tmp_path, document))


@pytest.mark.parametrize(
    ("owner", "value"),
    [
        ("a", int(T0 + pto.GRANT_MIN_LIFETIME - 1)),
        ("b", int(T0 + 60)),
        ("a", int(T0 - 1)),
        ("b", "2027-01-15T09:00:00+00:00"),
    ],
)
def test_insufficient_lifetime_is_refused(tmp_path: Path, owner: str, value: Any) -> None:
    document = _kit(tmp_path)
    _set(document, f"owners.{owner}.usable_until", value)
    error = _refused("lifetime_insufficient", lambda: _load(tmp_path, document))
    assert error.detail == f"owners.{owner}.usable_until"


@pytest.mark.parametrize("value", ["tomorrow", "2027-01-15T10:00:00", True, 1.5, None])
def test_unknown_expiry_is_refused(tmp_path: Path, value: Any) -> None:
    document = _kit(tmp_path)
    _set(document, "owners.a.usable_until", value)
    _refused("expiry_unknown", lambda: _load(tmp_path, document))


@pytest.mark.parametrize(
    ("content", "mode", "code"),
    [
        (GRANT_A.encode(), 0o644, "grant_unsafe"),
        (b"", 0o600, "grant_missing"),
        (b"two words\n", 0o600, "grant_unsafe"),
    ],
)
def test_unsafe_grant_file_is_refused_with_its_field(
    tmp_path: Path, content: bytes, mode: int, code: str
) -> None:
    document = _kit(tmp_path)
    _file(Path(document["owners"]["b"]["grant_file"]), content, mode)
    assert _refused(code, lambda: _load(tmp_path, document)).detail == "owners.b.grant_file"


# --------------------------------------------------------------------------
# C. Manifest, transitions and the schedule check
# --------------------------------------------------------------------------


def test_manifest_is_the_planned_sixteen_starts() -> None:
    table = {group.name: (group.owner, group.roles) for group in pto.MANIFEST}
    three = (pto.IMPLEMENTER, pto.PLAN_REVIEWER, pto.DIFF_REVIEWER)
    assert table == {
        "a-probe": ("a", (pto.PROBE,)),
        "a1": ("a", three),
        "a2": ("a", three),
        "a3": ("a", (pto.IMPLEMENTER,)),
        "b-probe": ("b", (pto.PROBE,)),
        "b1": ("b", three),
        "b-r1": ("b", (pto.IMPLEMENTER,)),
        "b-r2": ("b", (pto.IMPLEMENTER, pto.DIFF_REVIEWER, pto.DIFF_REVIEWER)),
    }
    assert sum(len(group.roles) for group in pto.MANIFEST) == 16
    for owner in pto.OWNERS:
        names = [group.name for group in pto.MANIFEST if group.owner == owner]
        assert len(names) == 4 and sum(len(pto.GROUPS[name].roles) for name in names) == 8, owner
    assert (pto.PLAN_REVIEWER, pto.DIFF_REVIEWER) == (pps.PLAN_REVIEWER, pps.DIFF_REVIEWER)
    assert pto.PLANNING_USAGE == {
        "implementer": 2700,
        "plan-reviewer": 1000,
        "diff-reviewer": 800,
        "repair-implementer": 2700,
        "interrupted": 0,
        "probe": 32,
    }


def test_every_transition_maps_to_a_group_or_a_sentinel() -> None:
    names = set(pto.GROUPS)
    assert set(pto.TRANSITIONS.values()) <= names | {pto.STARTS_NOTHING, pto.FENCED}
    for name in names:
        assert list(pto.TRANSITIONS.values()).count(name) == 1, name
    fenced = [key for key, value in pto.TRANSITIONS.items() if value == pto.FENCED]
    assert any("owner-lost successor" in key for key in fenced)
    assert any("round-3" in key for key in fenced)


def test_schedule_check_passes_the_approved_plan() -> None:
    result = pto.schedule_check(0)
    shares = {row["name"]: (row["share"], row["turn_share"]) for row in result["groups"]}
    assert shares == {
        "a-probe": (32, 1),
        "a1": (5450, 32),
        "b-probe": (32, 1),
        "b1": (5450, 32),
        "a2": (3950, 32),
        "b-r1": (8192, 32),
        "a3": (7352, 32),
        "b-r2": (3950, 24),
    }
    assert result["sessions"] == 16
    assert result["owner_totals"] == {"a": 11732, "b": 8832}
    assert result["aggregate"] == 20564 and result["aggregate_limit"] == 32768
    assert result["verdict"] == "pass"


def test_schedule_check_accepts_the_largest_prior_spend() -> None:
    assert pto.schedule_check(8312)["aggregate_limit"] == 32768 - 8312


def _extra_group() -> dict[str, Any]:
    extra = pto.Group("b-r3", "b", (pto.IMPLEMENTER,), "round-3 repair", "repair")
    return {
        "manifest": (*pto.MANIFEST, extra),
        "transitions": {**pto.TRANSITIONS, "round-3 repair": "b-r3"},
    }


def _reviewer_only_group() -> dict[str, Any]:
    manifest = tuple(
        pto.Group(g.name, g.owner, (pto.DIFF_REVIEWER,), g.trigger, "repair")
        if g.name == "b-r1"
        else g
        for g in pto.MANIFEST
    )
    return {"manifest": manifest, "usage": {**pto.PLANNING_USAGE, "diff-reviewer": 800}}


IMPOSSIBLE: list[tuple[str, int, Callable[[], dict[str, Any]], str]] = [
    ("seventeen starts", 0, _extra_group, "17 starts"),
    ("fifteen starts", 0, lambda: {"manifest": pto.MANIFEST[:-1]}, "13 starts"),
    (
        "implementer share under 2700",
        0,
        lambda: {"usage": {**pto.PLANNING_USAGE, "implementer": 6500}},
        "a2: share 2684",
    ),
    (
        "owner over 16384",
        0,
        lambda: {"usage": {**pto.PLANNING_USAGE, "implementer": 4400, "repair-implementer": 4400}},
        "owner a plans 16832",
    ),
    ("prior spend leaves the aggregate short", 8313, dict, "a3: share 2699"),
    ("prior spend far over", 20000, dict, "a2: share 1234"),
    ("prior spend over the ceiling", 32769, dict, "prior spend"),
    (
        "turn share under 16",
        0,
        lambda: {"turns": {**pto.PLANNING_TURNS, "implementer": 40}},
        "a3: turn share 2",
    ),
    (
        "unmapped transition",
        0,
        lambda: {"transitions": {**pto.TRANSITIONS, "worker restart": "nowhere"}},
        "transition unmapped: worker restart",
    ),
    (
        "missing transition",
        0,
        lambda: {"transitions": {k: v for k, v in pto.TRANSITIONS.items() if v != "a3"}},
        "transition unmapped",
    ),
    (
        "group started twice",
        0,
        lambda: {
            "transitions": {k: ("a2" if v == "a3" else v) for k, v in pto.TRANSITIONS.items()}
        },
        "group a2 needs exactly one",
    ),
    (
        "same-owner open group",
        0,
        lambda: {"open_during": {"a3": ("a2",)}},
        "same owner",
    ),
    (
        "open group reserved later",
        0,
        lambda: {"open_during": {"a1": ("b1",)}},
        "not reserved before",
    ),
    (
        "planned session over its share",
        0,
        lambda: {"usage": {**pto.PLANNING_USAGE, "probe": 40}},
        "a-probe: a planned probe session exceeds its share",
    ),
    (
        "reviewer-only share under 1000",
        0,
        lambda: {"usage": {**pto.PLANNING_USAGE, "implementer": 5400, "repair-implementer": 400}},
        "under",
    ),
]


@pytest.mark.parametrize(
    ("case", "prior", "overrides", "reason"), IMPOSSIBLE, ids=[c[0] for c in IMPOSSIBLE]
)
def test_schedule_check_refuses_impossible_plans(
    case: str, prior: int, overrides: Callable[[], dict[str, Any]], reason: str
) -> None:
    error = _refused("session_plan_impossible", lambda: pto.schedule_check(prior, **overrides()))
    assert reason in error.detail, error.detail


def test_reviewer_only_group_needs_one_thousand() -> None:
    plan = _reviewer_only_group()
    result = pto.schedule_check(0, **plan)
    assert {row["name"]: row["share"] for row in result["groups"]}["b-r1"] >= pto.MIN_SHARE
    starved = {**plan, "usage": {**plan["usage"], "implementer": 3600, "plan-reviewer": 3300}}
    error = _refused("session_plan_impossible", lambda: pto.schedule_check(0, **starved))
    assert "under" in error.detail


@pytest.mark.parametrize(
    ("group", "limits"),
    [
        ("a1", {"plan": 1, "diff": 1}),
        ("a2", {"plan": 1, "diff": 1}),
        ("b1", {"plan": 1, "diff": 1}),
        ("a3", {"plan": 0, "diff": 0}),
        ("b-r1", {"plan": 0, "diff": 0}),
        ("b-r2", {"plan": 0, "diff": 2}),
        ("a-probe", {"plan": 0, "diff": 0}),
    ],
)
def test_subagent_limits_follow_the_manifest(group: str, limits: dict[str, int]) -> None:
    assert pto.subagent_limits(group) == limits


def test_subagent_limits_refuse_an_unknown_group() -> None:
    _refused("arguments_invalid", lambda: pto.subagent_limits("b-r3"))


# --------------------------------------------------------------------------
# D. Ledger
# --------------------------------------------------------------------------


def _ledger(
    tmp_path: Path,
    *,
    prior: int = 0,
    windows: Mapping[str, float] | None = None,
    live: bool = True,
    rehearsal: bool = False,
) -> pto.TwoOwnerLedger:
    ledger = pto.TwoOwnerLedger.open(_private(tmp_path / "state"), RUN_ID)
    ledger.db.execute("PRAGMA synchronous=OFF")
    ledger.ensure_run(CANDIDATE, phase="run", now=T0)
    if rehearsal:
        ledger.mark_rehearsal()
        return ledger
    ledger.set_prior_spend(prior)
    for owner, until in (windows or {"a": WINDOW, "b": WINDOW}).items():
        ledger.set_owner_window(owner, until)
    if live:
        ledger.begin_live(now=T0, grant_expires_at=WINDOW)
    return ledger


def _planned(group: pto.Group, role: str) -> pps.SessionUsage:
    key = pto.planned_key(group, role)
    model = pto.PROBE_MODEL if role == pto.PROBE else pps.IMPLEMENTER_MODEL
    return pps.SessionUsage(pto.PLANNING_USAGE[key], pto.PLANNING_TURNS[key], 30.0, model)


def _settle(
    ledger: pto.TwoOwnerLedger,
    reservations: Sequence[pps.Reservation],
    usage: pps.SessionUsage | None = None,
) -> None:
    for item in reservations:
        group = pto.group_of_attempt(item.attempt)
        assert group is not None
        source = "probe" if group.kind == "probe" else "native"
        ledger.settle(item.seq, usage or _planned(group, item.role), source=source)


def _reserve_through(ledger: pto.TwoOwnerLedger, last: str, now: float = T0 + 10) -> None:
    for group in pto.MANIFEST:
        _settle(ledger, ledger.reserve_group(group.name, now=now))
        if group.name == last:
            return


def test_reserve_group_matches_the_schedule_and_settles(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    plan = {
        row["name"]: (row["share"], row["turn_share"]) for row in pto.schedule_check(0)["groups"]
    }
    seen: dict[str, tuple[int, int]] = {}
    open_b_r1: list[pps.Reservation] = []
    for index, group in enumerate(pto.MANIFEST):
        if group.name == "b-r2":
            # B-r1 settles at zero from its prompt-hold harvest before B-r2 is reserved.
            _settle(ledger, open_b_r1)
        reservations = ledger.reserve_group(group.name, now=T0 + 10 * index)
        assert [item.role for item in reservations] == list(group.roles)
        assert {item.attempt for item in reservations} == {pto.group_index(group.name)}
        assert len({(item.output_tokens, item.turns) for item in reservations}) == 1
        seen[group.name] = (reservations[0].output_tokens, reservations[0].turns)
        if group.kind == "probe":
            assert reservations[0].seconds == pto.PROBE_SECONDS
        else:
            assert reservations[0].seconds == pto.SESSION_SECONDS
        for item in reservations:
            assert ledger.owner_of(item.seq) == group.owner
        if group.name == "b-r1":
            open_b_r1 = reservations
        else:
            _settle(ledger, reservations)
    assert seen == plan
    assert ledger.owner_spent("a") == 11732 and ledger.owner_spent("b") == 8832
    assert ledger.aggregate_spent() == 20564
    assert ledger.budget_state() == "ok"
    assert [item.seq for item in ledger.reservations_of("b-r2")] == [14, 15, 16]


REFUSALS: list[tuple[str, str, Callable[[Path], Callable[[], Any]]]] = []


def _case(code: str, name: str) -> Callable[[Callable[[Path], Callable[[], Any]]], Any]:
    def register(build: Callable[[Path], Callable[[], Any]]) -> Any:
        REFUSALS.append((name, code, build))
        return build

    return register


@_case("arguments_invalid", "unknown group")
def _unknown_group(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    return lambda: ledger.reserve_group("b-r3", now=T0)


@_case("arguments_invalid", "base reserve")
def _base_reserve(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    return lambda: ledger.reserve([pto.IMPLEMENTER], attempt=1, now=T0)


@_case("group_reserved", "group twice")
def _group_twice(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    return lambda: ledger.reserve_group("a-probe", now=T0)


@_case("group_out_of_order", "out of owner order")
def _out_of_order(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    return lambda: ledger.reserve_group("a2", now=T0)


@_case("rehearsal_run_id", "rehearsal ledger")
def _rehearsal(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path, rehearsal=True)
    return lambda: ledger.reserve_group("a-probe", now=T0)


@_case("budget_exceeded", "budget exceeded")
def _exceeded(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    ledger.record_unreserved_session("a", "a1", pto.IMPLEMENTER, None)
    return lambda: ledger.reserve_group("a-probe", now=T0)


@_case("allowance_exhausted", "no live start")
def _no_live(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path, live=False)
    return lambda: ledger.reserve_group("a-probe", now=T0)


@_case("usage_unknown", "unknown usage")
def _unknown(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    probe = ledger.reserve_group("a-probe", now=T0)
    ledger.settle(probe[0].seq, None, source="unknown")
    return lambda: ledger.reserve_group("b-probe", now=T0)


@_case("owner_session_open", "owner session open")
def _owner_open(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    ledger.reserve_group("a-probe", now=T0)
    return lambda: ledger.reserve_group("a1", now=T0)


@_case("allowance_exhausted", "live seconds")
def _live_seconds(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    return lambda: ledger.reserve_group(
        "a-probe", now=T0 + pto.LIVE_SECONDS - pto.SESSION_SECONDS + 1
    )


@_case("lifetime_insufficient", "owner window short")
def _window_short(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path, windows={"a": T0 + pto.LIVE_SECONDS - 1, "b": WINDOW})
    return lambda: ledger.reserve_group("a-probe", now=T0)


@_case("lifetime_insufficient", "owner window missing")
def _window_missing(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path, windows={"b": WINDOW})
    return lambda: ledger.reserve_group("a-probe", now=T0)


@_case("allowance_exhausted", "owner allocation")
def _owner_allocation(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    heavy = pps.SessionUsage(5000, 10, 60.0, pps.IMPLEMENTER_MODEL)
    _settle(ledger, ledger.reserve_group("a1", now=T0), heavy)
    return lambda: ledger.reserve_group("a2", now=T0)


@_case("allowance_exhausted", "aggregate guard with prior spend")
def _aggregate(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path, prior=30000)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    return lambda: ledger.reserve_group("a1", now=T0)


@_case("allowance_exhausted", "turns")
def _turns(tmp_path: Path) -> Callable[[], Any]:
    ledger = _ledger(tmp_path)
    many = pps.SessionUsage(1000, 31, 60.0, pps.IMPLEMENTER_MODEL)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    _settle(ledger, ledger.reserve_group("a1", now=T0), many)
    _settle(ledger, ledger.reserve_group("b-probe", now=T0))
    _settle(ledger, ledger.reserve_group("b1", now=T0), many)
    return lambda: ledger.reserve_group("a2", now=T0)


@pytest.mark.parametrize(("case", "code", "build"), REFUSALS, ids=[r[0] for r in REFUSALS])
def test_reserve_group_refusals(
    tmp_path: Path, case: str, code: str, build: Callable[[Path], Callable[[], Any]]
) -> None:
    _refused(code, build(tmp_path))


def test_reserve_group_boundaries_are_admitted(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, windows={"a": T0 + pto.LIVE_SECONDS, "b": WINDOW})
    now = T0 + pto.LIVE_SECONDS - pto.SESSION_SECONDS
    assert len(ledger.reserve_group("a-probe", now=now)) == 1


def test_open_session_of_the_other_owner_is_charged_at_its_share(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _reserve_through(ledger, "a2")
    b_r1 = ledger.reserve_group("b-r1", now=T0 + 20)
    assert b_r1[0].output_tokens == 8192
    assert ledger.owner_spent("b") == 4532 + 8192
    assert ledger.owner_spent("b", live={b_r1[0].seq: 0}) == 4532
    assert ledger.aggregate_spent(live={b_r1[0].seq: 5}) == 13564 + 5
    a3 = ledger.reserve_group("a3", now=T0 + 30)
    assert a3[0].output_tokens == 7352
    _refused("owner_session_open", lambda: ledger.reserve_group("b-r2", now=T0 + 40))
    _settle(ledger, b_r1)
    _settle(ledger, a3)
    assert ledger.reserve_group("b-r2", now=T0 + 50)[0].output_tokens == 3950


def test_unknown_usage_is_charged_at_its_share_and_blocks(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, prior=100)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    a1 = ledger.reserve_group("a1", now=T0)
    ledger.settle(a1[0].seq, None, source="unknown")
    assert ledger.owner_spent("a") == 32 + 3 * a1[0].output_tokens
    assert ledger.aggregate_spent() == 100 + 32 + 3 * a1[0].output_tokens
    _refused("usage_unknown", lambda: ledger.reserve_group("b-probe", now=T0))


def test_unreserved_session_exceeds_the_budget(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    usage = pps.SessionUsage(100, 2, 5.0, pps.REVIEWER_MODEL)
    seq = ledger.record_unreserved_session("b", "b1", pto.DIFF_REVIEWER, usage)
    assert ledger.owner_of(seq) == "b" and ledger.budget_state() == "exceeded"
    assert ledger.reservations_of("b1") == []
    _refused("arguments_invalid", lambda: ledger.record_unreserved_session("c", "b1", "x", None))


def test_settle_attempt_routes_an_extra_session_to_its_owner(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    a1 = ledger.reserve_group("a1", now=T0)

    def facts(role: str, tokens: int) -> pps.SessionFacts:
        return pps.SessionFacts(
            path=role,
            role=role,
            parent="s",
            models=(pps.REVIEWER_MODEL,),
            assistant_turns=2,
            output_tokens=tokens,
            synthetic=0,
            first=1.0,
            last=2.0,
            elapsed=1.0,
            aci_tool_calls=0,
            usage_known=True,
        )

    transcript = pps.TranscriptFacts(
        sessions=[
            facts(pto.IMPLEMENTER, 900),
            facts(pto.PLAN_REVIEWER, 300),
            facts(pto.DIFF_REVIEWER, 200),
            facts(pto.DIFF_REVIEWER, 150),
        ],
        malformed_lines=0,
        synthetic_records=0,
        aci_tool_calls=0,
    )
    outcome = pps.settle_attempt(ledger, a1, transcript, complete=True)
    assert outcome == {"settled": 3, "unknown": 0, "unreserved": 1}
    extra = [row for row in ledger.session_rows() if row["source"] == "unreserved"]
    assert [(row["owner"], row["grp"]) for row in extra] == [("a", "a1")]
    assert ledger.budget_state() == "exceeded"


EXCEEDING: list[tuple[str, Callable[[pto.TwoOwnerLedger], None]]] = []


def _exceeds(name: str) -> Callable[[Callable[[pto.TwoOwnerLedger], None]], Any]:
    def register(build: Callable[[pto.TwoOwnerLedger], None]) -> Any:
        EXCEEDING.append((name, build))
        return build

    return register


@_exceeds("session output tokens")
def _over_tokens(ledger: pto.TwoOwnerLedger) -> None:
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    over = pps.SessionUsage(pto.SESSION_OUTPUT_TOKENS + 1, 3, 30.0, pps.IMPLEMENTER_MODEL)
    _settle(ledger, ledger.reserve_group("a1", now=T0)[:1], over)


@_exceeds("session turns")
def _over_turns(ledger: pto.TwoOwnerLedger) -> None:
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    over = pps.SessionUsage(100, pto.SESSION_TURNS + 1, 30.0, pps.IMPLEMENTER_MODEL)
    _settle(ledger, ledger.reserve_group("a1", now=T0)[:1], over)


@_exceeds("session seconds")
def _over_seconds(ledger: pto.TwoOwnerLedger) -> None:
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    over = pps.SessionUsage(100, 3, pto.SESSION_SECONDS + 0.5, pps.IMPLEMENTER_MODEL)
    _settle(ledger, ledger.reserve_group("a1", now=T0)[:1], over)


@_exceeds("probe output tokens")
def _over_probe(ledger: pto.TwoOwnerLedger) -> None:
    over = pps.SessionUsage(pto.PROBE_OUTPUT_TOKENS + 1, 1, 3.0, pto.PROBE_MODEL)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0), over)


@_exceeds("probe turns")
def _over_probe_turns(ledger: pto.TwoOwnerLedger) -> None:
    over = pps.SessionUsage(5, 2, 3.0, pto.PROBE_MODEL)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0), over)


@_exceeds("owner allocation")
def _over_owner(ledger: pto.TwoOwnerLedger) -> None:
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    _settle(
        ledger,
        ledger.reserve_group("a1", now=T0),
        pps.SessionUsage(5500, 3, 30.0, pps.IMPLEMENTER_MODEL),
    )


def _over_aggregate(tmp_path: Path) -> pto.TwoOwnerLedger:
    ledger = _ledger(tmp_path, prior=20000)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    _settle(
        ledger,
        ledger.reserve_group("a1", now=T0),
        pps.SessionUsage(4300, 3, 30.0, pps.IMPLEMENTER_MODEL),
    )
    return ledger


@pytest.mark.parametrize(("case", "build"), EXCEEDING, ids=[e[0] for e in EXCEEDING])
def test_settle_past_a_bound_marks_the_budget_exceeded(
    tmp_path: Path, case: str, build: Callable[[pto.TwoOwnerLedger], None]
) -> None:
    ledger = _ledger(tmp_path)
    build(ledger)
    assert ledger.budget_state() == "exceeded"


def test_aggregate_guard_counts_prior_spend(tmp_path: Path) -> None:
    ledger = _over_aggregate(tmp_path)
    assert ledger.owner_spent("a") == 32 + 3 * 4300 <= pto.OWNER_OUTPUT_TOKENS
    assert ledger.budget_state() == "exceeded"


def test_settle_within_bounds_keeps_the_budget(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, prior=12000)
    _reserve_through(ledger, "b1")
    assert ledger.budget_state() == "ok"
    exact = pps.SessionUsage(pto.PROBE_OUTPUT_TOKENS, 1, float(pto.PROBE_SECONDS), pto.PROBE_MODEL)
    other = _ledger(tmp_path / "other")
    _settle(other, other.reserve_group("a-probe", now=T0), exact)
    assert other.budget_state() == "ok"


@pytest.mark.parametrize(("extra", "state"), [(0, "ok"), (1, "exceeded")])
def test_settled_usage_at_every_ceiling_keeps_the_budget(
    tmp_path: Path, extra: int, state: str
) -> None:
    # Inclusive ceilings: a session at 8,192 tokens, 32 turns and 300 s is within;
    # the owner then reaches exactly 16,384 output tokens, and one more is over.
    ledger = _ledger(tmp_path)
    _settle(ledger, ledger.reserve_group("a-probe", now=T0))
    a1 = ledger.reserve_group("a1", now=T0)
    limits = pps.SessionUsage(
        pto.SESSION_OUTPUT_TOKENS,
        pto.SESSION_TURNS,
        float(pto.SESSION_SECONDS),
        pps.IMPLEMENTER_MODEL,
    )
    _settle(ledger, a1[:1], limits)
    assert ledger.budget_state() == "ok"
    rest = pto.OWNER_OUTPUT_TOKENS - ledger.owner_spent("a", live={a1[1].seq: 0, a1[2].seq: 0})
    _settle(ledger, a1[1:2], pps.SessionUsage(rest + extra, 6, 30.0, pps.REVIEWER_MODEL))
    assert ledger.budget_state() == state


def test_prior_spend_and_windows_are_recorded_once(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, prior=10)
    ledger.set_prior_spend(10)
    _refused("arguments_invalid", lambda: ledger.set_prior_spend(11))
    ledger.set_owner_window("a", WINDOW)
    _refused("arguments_invalid", lambda: ledger.set_owner_window("a", WINDOW + 1))
    assert ledger.prior_spend() == 10 and ledger.owner_window("b") == WINDOW
    for bad in (-1, 32769, True, 1.0):
        _refused("arguments_invalid", lambda bad=bad: ledger.set_prior_spend(bad))
    for owner, until in (("c", WINDOW), ("a", float("nan")), ("b", True), ("b", 0)):
        _refused("arguments_invalid", lambda o=owner, u=until: ledger.set_owner_window(o, u))
    fresh = pto.TwoOwnerLedger.open(_private(tmp_path / "fresh"), RUN_ID)
    fresh.ensure_run(CANDIDATE, phase="run", now=T0)
    fresh.set_owner_window("a", WINDOW)
    fresh.set_owner_window("b", WINDOW)
    fresh.begin_live(now=T0, grant_expires_at=WINDOW)
    fresh.reserve_group("a-probe", now=T0)
    _refused("arguments_invalid", lambda: fresh.set_prior_spend(0))
    assert fresh.prior_spend() == 0


def test_open_existing_requires_a_two_owner_ledger(tmp_path: Path) -> None:
    state = _private(tmp_path / "single")
    single = pps.Ledger.open(state, RUN_ID)
    single.ensure_run(CANDIDATE, phase="run", now=T0)
    single.close()
    _refused("ledger_missing", lambda: pto.TwoOwnerLedger.open_existing(state, RUN_ID))
    ledger = _ledger(tmp_path)
    ledger.close()
    reopened = pto.TwoOwnerLedger.open_existing(tmp_path / "state", RUN_ID)
    assert reopened.candidate() == CANDIDATE
    reopened.close()


# --------------------------------------------------------------------------
# E. Gate decision, hooks merge and the in-pod programs
# --------------------------------------------------------------------------

RESERVATION = pps.Reservation(
    seq=2, attempt=2, role="implementer", output_tokens=5000, turns=32, seconds=300.0
)
FULL_SHARE = dataclasses.replace(RESERVATION, output_tokens=8192)


def _decide(**changes: Any) -> tuple[str, str]:
    arguments: dict[str, Any] = {
        "session_usage": pps.SessionUsage(1000, 10, 60.0, pps.IMPLEMENTER_MODEL),
        "reservation": RESERVATION,
        "owner_spent": 6000,
        "aggregate_spent": 12000,
        "session_started_at": T0,
        "now": T0 + 60,
        "final": False,
    }
    arguments.update(changes)
    return pto.gate_decision(**arguments)


def _usage(tokens: int, turns: int = 10) -> pps.SessionUsage:
    return pps.SessionUsage(tokens, turns, 60.0, pps.IMPLEMENTER_MODEL)


def test_gate_decision_releases_within_every_bound() -> None:
    assert _decide() == ("go", "")
    assert _decide(session_usage=_usage(4999)) == ("go", "")
    assert _decide(session_usage=_usage(1000, 31)) == ("go", "")
    assert _decide(now=T0 + 299.9) == ("go", "")
    assert _decide(owner_spent=pto.OWNER_OUTPUT_TOKENS - pto.RESPONSE_OUTPUT_TOKENS) == ("go", "")
    assert _decide(aggregate_spent=pto.MAX_OUTPUT_TOKENS - 2 * pto.RESPONSE_OUTPUT_TOKENS) == (
        "go",
        "",
    )
    assert _decide(reservation=FULL_SHARE, session_usage=_usage(6144)) == ("go", "")


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"session_usage": None}, "usage_unknown"),
        ({"owner_spent": None}, "usage_unknown"),
        ({"aggregate_spent": None}, "usage_unknown"),
        ({"owner_spent": True}, "usage_unknown"),
        ({"session_usage": _usage(-1)}, "usage_unknown"),
        ({"session_started_at": None}, "session_start_unknown"),
        ({"session_usage": _usage(5000)}, "session_share"),
        ({"reservation": FULL_SHARE, "session_usage": _usage(6145)}, "session_output_tokens"),
        ({"session_usage": _usage(1000, 32)}, "session_turns"),
        (
            {
                "reservation": dataclasses.replace(RESERVATION, turns=12),
                "session_usage": _usage(9, 12),
            },
            "session_turns",
        ),
        ({"now": T0 + 300}, "session_seconds"),
        (
            {"owner_spent": pto.OWNER_OUTPUT_TOKENS - pto.RESPONSE_OUTPUT_TOKENS + 1},
            "owner_allocation",
        ),
        (
            {"aggregate_spent": pto.MAX_OUTPUT_TOKENS - 2 * pto.RESPONSE_OUTPUT_TOKENS + 1},
            "aggregate_guard",
        ),
    ],
)
def test_gate_decision_stops(changes: dict[str, Any], reason: str) -> None:
    assert _decide(**changes) == ("stop", reason)


#: A share above the per-session cap, so the cap's own check is reached.
UNCAPPED_SHARE = dataclasses.replace(RESERVATION, output_tokens=pto.SESSION_OUTPUT_TOKENS + 1)
TWELVE_TURNS = dataclasses.replace(RESERVATION, turns=12)
#: (case, a final hold exactly at the ceiling, the same hold one above it, the reason).
FINAL_BOUNDARIES: list[tuple[str, dict[str, Any], dict[str, Any], str]] = [
    ("share", {"session_usage": _usage(5000)}, {"session_usage": _usage(5001)}, "session_share"),
    (
        "full share",
        {"reservation": FULL_SHARE, "session_usage": _usage(pto.SESSION_OUTPUT_TOKENS)},
        {"reservation": FULL_SHARE, "session_usage": _usage(pto.SESSION_OUTPUT_TOKENS + 1)},
        "session_share",
    ),
    (
        "session output tokens",
        {"reservation": UNCAPPED_SHARE, "session_usage": _usage(pto.SESSION_OUTPUT_TOKENS)},
        {"reservation": UNCAPPED_SHARE, "session_usage": _usage(pto.SESSION_OUTPUT_TOKENS + 1)},
        "session_output_tokens",
    ),
    (
        "session turns",
        {"session_usage": _usage(1000, pto.SESSION_TURNS)},
        {"session_usage": _usage(1000, pto.SESSION_TURNS + 1)},
        "session_turns",
    ),
    (
        "reserved turns",
        {"reservation": TWELVE_TURNS, "session_usage": _usage(9, 12)},
        {"reservation": TWELVE_TURNS, "session_usage": _usage(9, 13)},
        "session_turns",
    ),
    (
        "session seconds",
        {"now": T0 + pto.SESSION_SECONDS},
        {"now": T0 + pto.SESSION_SECONDS + 1},
        "session_seconds",
    ),
    (
        "owner allocation",
        {"owner_spent": pto.OWNER_OUTPUT_TOKENS},
        {"owner_spent": pto.OWNER_OUTPUT_TOKENS + 1},
        "owner_allocation",
    ),
    (
        "aggregate guard",
        {"aggregate_spent": pto.MAX_OUTPUT_TOKENS},
        {"aggregate_spent": pto.MAX_OUTPUT_TOKENS + 1},
        "aggregate_guard",
    ),
]


@pytest.mark.parametrize(
    ("case", "at", "over", "reason"), FINAL_BOUNDARIES, ids=[b[0] for b in FINAL_BOUNDARIES]
)
def test_a_final_hold_passes_at_a_ceiling_and_stops_above_it(
    case: str, at: dict[str, Any], over: dict[str, Any], reason: str
) -> None:
    # Stop and SubagentStop ask for no further response: every ceiling is inclusive.
    assert _decide(final=True, **at) == ("go", "")
    assert _decide(final=True, **over) == ("stop", reason)


def _dark_factory_hooks() -> dict[str, Any]:
    raw = json.loads((REPO_ROOT / "examples/dark-factory/hooks/hooks.json").read_text())
    return pps.private_bundle_hooks(raw)


def test_bundle_hooks_merge_the_gate_and_keep_every_hook() -> None:
    patched = pps.private_bundle_hooks(
        json.loads((REPO_ROOT / "examples/dark-factory/hooks/hooks.json").read_text())
    )
    before = copy.deepcopy(patched)
    merged = pto.two_owner_bundle_hooks(patched)
    assert patched == before
    for event, entries in before["hooks"].items():
        assert merged["hooks"][event][: len(entries)] == entries
    gate = {
        event: [
            hook
            for entry in merged["hooks"][event]
            for hook in entry["hooks"]
            if pto.GATE_FILE in hook["command"]
        ]
        for event in ("UserPromptSubmit", "PreToolUse", "Stop", "SubagentStop")
    }
    assert {
        event: [hook["command"].rsplit(" ", 1)[1] for hook in hooks]
        for event, hooks in gate.items()
    } == {
        "UserPromptSubmit": ["prompt"],
        "PreToolUse": ["response"],
        "Stop": ["stop"],
        "SubagentStop": ["subagent-stop"],
    }
    assert all(hook["timeout"] == pto.HOOK_TIMEOUT for hooks in gate.values() for hook in hooks)
    pre = [entry for entry in merged["hooks"]["PreToolUse"] if pto.GATE_FILE in json.dumps(entry)]
    assert pre[0]["matcher"] == "*"


def test_bundle_hooks_refuse_unpatched_or_doubly_gated_hooks() -> None:
    raw = json.loads((REPO_ROOT / "examples/dark-factory/hooks/hooks.json").read_text())
    _refused("arguments_invalid", lambda: pto.two_owner_bundle_hooks(raw))
    merged = pto.two_owner_bundle_hooks(_dark_factory_hooks())
    _refused("arguments_invalid", lambda: pto.two_owner_bundle_hooks(merged))


def test_gate_programs_compile() -> None:
    compile(pto.GATE_HOOK_PROGRAM, pto.GATE_FILE, "exec")
    compile(pto.GATE_STREAM_PROGRAM, "gate_stream", "exec")
    for program in (pto.GATE_HOOK_PROGRAM, pto.GATE_STREAM_PROGRAM):
        assert "@HOLD_SECONDS@" not in program and "@GATE_DIR@" not in program
        assert "@LIMITS_FILE@" not in program
    assert pto.GATE_DIR in pto.GATE_HOOK_PROGRAM and pto.GATE_DIR in pto.GATE_STREAM_PROGRAM
    assert f"HOLD_SECONDS = {pto.HOLD_SECONDS}" in pto.GATE_HOOK_PROGRAM


@dataclasses.dataclass
class GateBench:
    """The gate hook installed in a bundle-shaped directory, run under a fake ``claude``."""

    root: Path
    hook: Path
    claude: Path
    state: Path
    transcript: Path

    def env(self, **extra: str) -> dict[str, str]:
        env = {"PATH": os.environ.get("PATH", ""), "PTO_GATE_DIR": str(self.state)}
        env.update(extra)
        return env

    def start(self, mode: str, event: Mapping[str, Any], **extra: str) -> subprocess.Popen[str]:
        # The parent's argv[0] names ``claude``, as the native CLI's does, so the
        # hook's parent walk finds it; the parent runs the hook as its child.
        parent = (
            "import subprocess, sys\n"
            "code = subprocess.run([sys.argv[1], sys.argv[2], sys.argv[3]]).returncode\n"
            "sys.exit(code)\n"
        )
        process = subprocess.Popen(
            [str(self.claude), "-I", "-c", parent, sys.executable, str(self.hook), mode],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self.env(**extra),
            text=True,
        )
        assert process.stdin is not None
        process.stdin.write(json.dumps(dict(event)))
        process.stdin.close()
        return process

    def run(self, mode: str, event: Mapping[str, Any], **extra: str) -> tuple[int, str, str]:
        process = self.start(mode, event, **extra)
        stdout, stderr = process.communicate(timeout=60)
        return process.returncode, stdout, stderr


def _bench(tmp_path: Path) -> GateBench:
    hooks = tmp_path / "bundle" / "hooks"
    hooks.mkdir(parents=True)
    hook = hooks / pto.GATE_FILE
    hook.write_text(pto.GATE_HOOK_PROGRAM)
    shutil.copyfile(
        REPO_ROOT / "examples/dark-factory/hooks/review_gate.py", hooks / "review_gate.py"
    )
    bin_dir = tmp_path / "fake-cli"
    bin_dir.mkdir()
    claude = bin_dir / "claude"
    claude.symlink_to(sys.executable)
    transcript = tmp_path / "projects" / "s" / "session.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_bytes(b'{"type": "user"}\n' * 3)
    return GateBench(tmp_path, hook, claude, tmp_path / "gate", transcript)


class Stream:
    """GATE_STREAM_PROGRAM as a real process; lines are read by a thread into a queue."""

    def __init__(self, bench: GateBench) -> None:
        self.process = subprocess.Popen(
            [sys.executable, "-I", "-c", pto.GATE_STREAM_PROGRAM],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=bench.env(),
            text=True,
        )
        self.lines: queue.Queue[dict[str, Any]] = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()
        self.expect(lambda item: item.get("ready") is True)

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.lines.put(json.loads(line))

    def expect(self, predicate: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
        while True:
            item = self.lines.get(timeout=30)
            if predicate(item):
                return item

    def send(self, command: Mapping[str, Any] | str) -> None:
        assert self.process.stdin is not None
        text = command if isinstance(command, str) else json.dumps(dict(command))
        self.process.stdin.write(text + "\n")
        self.process.stdin.flush()

    def close(self) -> int:
        assert self.process.stdin is not None
        self.process.stdin.close()
        return self.process.wait(timeout=30)


def _event(bench: GateBench, **fields: Any) -> dict[str, Any]:
    return {
        "session_id": "5d0c7f3e-0000-4000-8000-00000000a001",
        "transcript_path": str(bench.transcript),
        "hook_event_name": "UserPromptSubmit",
        **fields,
    }


def test_gate_holds_until_the_stream_releases_go(tmp_path: Path) -> None:
    bench = _bench(tmp_path)
    stream = Stream(bench)
    hook = bench.start("prompt", _event(bench))
    held = stream.expect(lambda item: "hold" in item)
    record = held["hold"]
    assert held["key"] == record["key"] and pto.GATE_KEY.fullmatch(record["key"])
    assert record["mode"] == "prompt"
    assert record["cli_pid"] == hook.pid and isinstance(record["cli_start"], int)
    assert record["hook_pid"] not in (hook.pid, None)
    assert record["transcript_size"] == bench.transcript.stat().st_size
    assert record["session_id"] == "5d0c7f3e-0000-4000-8000-00000000a001"
    stream.send({"release": record["key"], "verdict": "go"})
    stream.expect(lambda item: item.get("applied") == record["key"])
    stdout, stderr = hook.communicate(timeout=60)
    assert hook.returncode == 0 and stdout == "" and stderr == ""
    stream.expect(lambda item: item.get("gone") == record["key"])
    assert stream.close() == 0
    assert not list(bench.state.glob("holding-*")) and not list(bench.state.glob("release-*"))


@pytest.mark.parametrize(
    ("mode", "fields", "exit_code"),
    [
        ("prompt", {}, 2),
        ("response", {"tool_name": "Bash", "tool_input": {"command": "ls"}}, 2),
        ("stop", {}, 0),
        ("subagent-stop", {"agent_id": "a1"}, 0),
    ],
)
def test_gate_stop_verdict_fails_closed(
    tmp_path: Path, mode: str, fields: dict[str, Any], exit_code: int
) -> None:
    bench = _bench(tmp_path)
    stream = Stream(bench)
    hook = bench.start(mode, _event(bench, **fields))
    held = stream.expect(lambda item: "hold" in item)
    assert held["hold"]["mode"] == mode
    assert held["hold"]["tool_name"] == fields.get("tool_name")
    stream.send({"release": held["key"], "verdict": "stop"})
    stdout, stderr = hook.communicate(timeout=60)
    assert hook.returncode == exit_code
    if exit_code == 2:
        assert "stopped this session" in stderr and stdout == ""
    else:
        # A Stop hook that exits 2 makes the model continue; the gate ends it instead.
        assert json.loads(stdout) == {
            "continue": False,
            "stopReason": "two-owner gate: the acceptance driver stopped this session at its "
            "budget gate",
        }
    assert stream.close() == 0


@pytest.mark.parametrize(("mode", "exit_code"), [("prompt", 2), ("response", 2), ("stop", 0)])
def test_gate_times_out_closed(tmp_path: Path, mode: str, exit_code: int) -> None:
    bench = _bench(tmp_path)
    code, stdout, stderr = bench.run(mode, _event(bench), PTO_GATE_HOLD_SECONDS="0.2")
    assert code == exit_code
    text = stderr if exit_code == 2 else json.loads(stdout)["stopReason"]
    assert "did not release this hold in time" in text
    assert not list(bench.state.glob("holding-*"))


def test_gate_hold_override_only_shortens() -> None:
    assert "min(limit, max(0.0, float(override)))" in pto.GATE_HOOK_PROGRAM


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        ("not json", "unreadable hook input"),
        ("[1]", "unreadable hook input"),
    ],
)
def test_gate_refuses_unreadable_input(tmp_path: Path, event: str, expected: str) -> None:
    bench = _bench(tmp_path)
    result = subprocess.run(
        [sys.executable, "-I", str(bench.hook), "prompt"],
        input=event,
        capture_output=True,
        text=True,
        env=bench.env(),
        timeout=60,
    )
    assert result.returncode == 2 and expected in result.stderr


def test_gate_refuses_an_unknown_mode(tmp_path: Path) -> None:
    bench = _bench(tmp_path)
    code, _stdout, stderr = bench.run("launch", _event(bench))
    assert code == 2 and "unknown gate mode" in stderr


def _agent(bench: GateBench, subagent_type: str) -> dict[str, Any]:
    return _event(
        bench,
        hook_event_name="PreToolUse",
        tool_name="Agent",
        tool_input={"subagent_type": subagent_type, "description": "review", "prompt": "x"},
    )


def test_gate_refuses_subagents_without_limits(tmp_path: Path) -> None:
    bench = _bench(tmp_path)
    code, _stdout, stderr = bench.run("response", _agent(bench, "dark-factory:plan-reviewer"))
    assert code == 2 and "limits are not written" in stderr
    assert not list(bench.state.glob("holding-*"))


def test_gate_admits_reserved_reviewers_once_per_slot(tmp_path: Path) -> None:
    bench = _bench(tmp_path)
    stream = Stream(bench)
    stream.send({"limits": pto.subagent_limits("a1")})
    stream.expect(lambda item: item.get("limits_applied") == {"plan": 1, "diff": 1})
    first = bench.start("response", _agent(bench, "dark-factory:plan-reviewer"))
    held = stream.expect(lambda item: "hold" in item)
    assert held["hold"]["subagent_kind"] == "plan" and held["hold"]["tool_name"] == "Agent"
    stream.send({"release": held["key"], "verdict": "go"})
    first.communicate(timeout=60)
    assert first.returncode == 0
    code, _stdout, stderr = bench.run("response", _agent(bench, "dark-factory:plan-reviewer"))
    assert code == 2 and "plan reviewer allowance for this pod is spent" in stderr
    code, _stdout, stderr = bench.run("response", _agent(bench, "general-purpose"))
    assert code == 2 and "only the reserved plan and diff reviewers" in stderr
    stream.send({"limits": pto.subagent_limits("a3")})
    stream.expect(lambda item: item.get("limits_applied") == {"plan": 0, "diff": 0})
    code, _stdout, stderr = bench.run("response", _agent(bench, "diff-reviewer"))
    assert code == 2 and "diff reviewer allowance for this pod is spent" in stderr
    assert stream.close() == 0


def test_gate_slot_taken_by_a_timed_out_hold_stays_spent(tmp_path: Path) -> None:
    bench = _bench(tmp_path)
    bench.state.mkdir(mode=0o700)
    (bench.state / pto.GATE_LIMITS_FILE).write_text(json.dumps({"plan": 0, "diff": 1}))
    code, _stdout, stderr = bench.run(
        "response", _agent(bench, "dark-factory:diff-reviewer"), PTO_GATE_HOLD_SECONDS="0.1"
    )
    assert code == 2 and "did not release" in stderr
    code, _stdout, stderr = bench.run("response", _agent(bench, "dark-factory:diff-reviewer"))
    assert code == 2 and "allowance for this pod is spent" in stderr


@pytest.mark.parametrize(
    ("command", "error"),
    [
        ({"release": "../escape-1-1", "verdict": "go"}, "invalid release"),
        ({"release": "prompt-1-1", "verdict": "maybe"}, "invalid release"),
        ({"release": "launch-1-1", "verdict": "go"}, "invalid release"),
        ({"limits": {"plan": -1}}, "invalid limits"),
        ({"limits": {"other": 1}}, "invalid limits"),
        ({"limits": {"plan": True}}, "invalid limits"),
        ({"limits": [1]}, "invalid limits"),
        ({"release": "prompt-1-1"}, "unknown command"),
        ("not json", "unreadable command"),
        ("[1]", "unreadable command"),
    ],
)
def test_gate_stream_refuses_malformed_commands(
    tmp_path: Path, command: Mapping[str, Any] | str, error: str
) -> None:
    bench = _bench(tmp_path)
    stream = Stream(bench)
    stream.send(command)
    assert stream.expect(lambda item: "error" in item) == {"error": error}
    assert stream.close() == 0
    assert sorted(path.name for path in bench.state.iterdir()) == []
    assert not (tmp_path / "escape-1-1").exists()


def test_gate_stream_reports_a_heartbeat(tmp_path: Path) -> None:
    bench = _bench(tmp_path)
    stream = Stream(bench)
    beat = stream.expect(lambda item: "heartbeat" in item)
    assert beat["holds"] == [] and isinstance(beat["heartbeat"], float)
    assert stream.close() == 0


# --------------------------------------------------------------------------
# F. Install values, Dex and the installation-grant scan
# --------------------------------------------------------------------------

RELEASE = "pto-acme"
NAMESPACE = "pto-acme-1"
ISSUER = pto.dex_issuer("pto-dex", NAMESPACE)
REDIRECT = pto.api_redirect_uri(RELEASE, NAMESPACE)


def _base_values() -> dict[str, Any]:
    return pps.install_values(
        release=RELEASE,
        registry="localhost:5999",
        digests={name: "sha256:" + "4" * 64 for name in pps.REQUIRED_IMAGES},
        agent="acme-pto-0a0b0c0d",
        repo=REPO,
        app_id="42",
        app_secret="pto-acme-github-app",
        trigger_label="pto-unused-0a0b0c0d",
        priority_classes=None,
        controller_deploy=False,
        model_addresses=["192.0.2.10"],
        slack_bot_secret="pto-acme-slack-bot",
    )


def _values(base: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return pto.two_owner_install_values(
        base if base is not None else _base_values(),
        release=RELEASE,
        namespace=NAMESPACE,
        grant_namespace=f"{NAMESPACE}-grants",
        issuer=ISSUER,
        redirect_uri=REDIRECT,
        client_id=pto.OIDC_CLIENT_ID,
        client_secret_ref=("pto-acme-oidc", "clientSecret"),
    )


def _env_of(entries: Any) -> dict[str, Any]:
    return {entry["name"]: entry.get("value", entry.get("valueFrom")) for entry in entries or []}


def test_two_owner_values_deliver_grants_per_claim_only() -> None:
    base = _base_values()
    before = copy.deepcopy(base)
    values = _values(base)
    assert base == before
    assert pto.installation_grant_findings([values]) == []
    runner = _env_of(values["agentSandbox"]["runner"]["extraEnv"])
    assert pto.MODEL_AUTH_MODE_ENV not in runner and pps.PERSONAL_GRANT_ENV not in runner
    assert runner == {
        "CURIE_MAX_TURNS": "32",
        "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(pto.RESPONSE_OUTPUT_TOKENS),
        "ENABLE_TOOL_SEARCH": "false",
    }
    assert "personal-grant" not in json.dumps(values)
    assert values["personalClaude"] == {"grantNamespace": f"{NAMESPACE}-grants"}
    worker = values["worker"]
    assert _env_of(worker["extraEnv"]) == {"FACTORY_MODEL_AUTH_MODE": "personal"}
    assert worker["maxConcurrency"] == 3 and worker["workItemMaxTurns"] == 32
    assert values["resourceQuota"] == {"hard": {"requestsMemory": "16Gi"}}
    api = _env_of(values["api"]["extraEnv"])
    assert api == {
        "FACTORY_MODEL_AUTH_MODE": "personal",
        "CURIE_OIDC_ISSUER": ISSUER,
        "CURIE_OIDC_AUDIENCE": pto.OIDC_CLIENT_ID,
        "CURIE_OIDC_JWKS_URL": ISSUER + "/keys",
        "CURIE_OIDC_CLIENT_SECRET": {
            "secretKeyRef": {"name": "pto-acme-oidc", "key": "clientSecret"}
        },
        "CURIE_OIDC_REDIRECT_URI": REDIRECT,
        "CURIE_OIDC_SCOPES": "openid email profile",
        "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED": "true",
    }
    assert values["agentSandbox"]["runner"]["fakeModel"] is False
    assert ISSUER == "http://pto-dex.pto-acme-1.svc.cluster.local:5556/dex"
    assert REDIRECT == (
        "http://pto-acme-curie-api.pto-acme-1.svc.cluster.local:8000/console/oidc/callback"
    )


def test_two_owner_values_are_idempotent_over_their_own_output() -> None:
    once = _values()
    assert _values(once) == once


@pytest.mark.parametrize(
    "changes",
    [
        {"grant_namespace": NAMESPACE},
        {"grant_namespace": "Bad_Name"},
        {"issuer": ISSUER + "/"},
        {"issuer": "ftp://dex/dex"},
        {"redirect_uri": "not a url"},
        {"client_id": ""},
        {"client_secret_ref": ("Bad_Name", "clientSecret")},
        {"client_secret_ref": ("pto-acme-oidc", "")},
    ],
)
def test_two_owner_values_refuse_bad_arguments(changes: dict[str, Any]) -> None:
    arguments: dict[str, Any] = {
        "release": RELEASE,
        "namespace": NAMESPACE,
        "grant_namespace": f"{NAMESPACE}-grants",
        "issuer": ISSUER,
        "redirect_uri": REDIRECT,
        "client_id": pto.OIDC_CLIENT_ID,
        "client_secret_ref": ("pto-acme-oidc", "clientSecret"),
    }
    arguments.update(changes)
    _refused("arguments_invalid", lambda: pto.two_owner_install_values(_base_values(), **arguments))


def test_two_owner_values_refuse_a_base_with_a_service_credential() -> None:
    base = _base_values()
    base["worker"]["extraEnv"] = [{"name": "ANTHROPIC_API_KEY", "value": "placeholder"}]
    _refused("deployed_service_credential", lambda: _values(base))


CLAIM = "curie-thread-0a0b0c0d0e-abcd"


def _grant_env(
    secret: str = f"{CLAIM}-tokens", key: str = pps.PERSONAL_GRANT_ENV
) -> dict[str, Any]:
    return {
        "name": pps.PERSONAL_GRANT_ENV,
        "valueFrom": {"secretKeyRef": {"name": secret, "key": key, "optional": False}},
    }


def _pod(
    *, claim: str | None = CLAIM, container: str = "runner", env: Sequence[Any] = ()
) -> dict[str, Any]:
    labels = {pto.CLAIM_LABEL: claim} if claim else {}
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "runner-pod", "labels": labels},
        "spec": {
            "containers": [
                {"name": container, "image": "localhost:5999/curie-runner", "env": list(env)}
            ]
        },
    }


def _template(*, claim: str | None, env: Sequence[Any]) -> dict[str, Any]:
    metadata = {"labels": {pto.CLAIM_LABEL: claim}} if claim else {}
    return {
        "kind": "SandboxTemplate",
        "metadata": {"name": "pool-template"},
        "spec": {
            "podTemplate": {
                "metadata": metadata,
                "spec": {"containers": [{"name": "runner", "image": "runner", "env": list(env)}]},
            }
        },
    }


def _secret(name: str, labels: Mapping[str, str], keys: Sequence[str]) -> dict[str, Any]:
    return {
        "kind": "Secret",
        "metadata": {"name": name, "labels": dict(labels)},
        "data": {name: "AA==" for name in keys},
    }


MODE_ENV = {"name": "CURIE_MODEL_AUTH_MODE", "value": "personal"}
GRANT_FINDINGS: list[tuple[str, Any, list[str]]] = [
    (
        "release-wide grant on the pool template",
        _template(claim=None, env=[_grant_env("pto-acme-personal-grant", "token")]),
        ["CLAUDE_CODE_OAUTH_TOKEN", "grant_secret_ref"],
    ),
    (
        "another claim's tokens",
        _pod(env=[_grant_env("curie-other-tokens")]),
        ["CLAUDE_CODE_OAUTH_TOKEN", "grant_secret_ref"],
    ),
    (
        "non-runner container",
        _pod(container="sidecar", env=[_grant_env()]),
        ["CLAUDE_CODE_OAUTH_TOKEN", "grant_secret_ref"],
    ),
    (
        "unlabelled pod",
        _pod(claim=None, env=[_grant_env()]),
        ["CLAUDE_CODE_OAUTH_TOKEN", "grant_secret_ref"],
    ),
    (
        "literal grant value",
        _pod(env=[{"name": pps.PERSONAL_GRANT_ENV, "value": "placeholder"}]),
        ["CLAUDE_CODE_OAUTH_TOKEN"],
    ),
    (
        "personal marker on the pool",
        _template(claim=None, env=[MODE_ENV]),
        ["CURIE_MODEL_AUTH_MODE"],
    ),
    (
        "grant key under another name",
        _pod(env=[{**_grant_env(), "name": "SOMETHING_ELSE"}]),
        ["grant_secret_ref"],
    ),
    ("unlabelled grant Secret", _secret("loose", {}, [pps.PERSONAL_GRANT_ENV]), ["grant_secret"]),
    ("release grant Secret", _secret("pto-acme-personal-grant", {}, ["token"]), ["grant_secret"]),
    (
        "tokens Secret of another claim",
        _secret(f"{CLAIM}-tokens", {pto.CLAIM_LABEL: "other"}, [pps.PERSONAL_GRANT_ENV]),
        ["grant_secret"],
    ),
    (
        "envFrom of the release grant",
        {
            "containers": [
                {
                    "name": "runner",
                    "image": "r",
                    "envFrom": [{"secretRef": {"name": "pto-acme-personal-grant"}}],
                }
            ]
        },
        ["grant_secret_ref"],
    ),
    (
        "volume of the release grant",
        {"volumes": [{"name": "grant", "secret": {"secretName": "pto-acme-personal-grant"}}]},
        ["grant_secret_ref"],
    ),
    (
        "service credential",
        {"worker": {"extraEnv": [{"name": "ANTHROPIC_API_KEY", "value": "placeholder"}]}},
        ["ANTHROPIC_API_KEY"],
    ),
]


@pytest.mark.parametrize(
    ("case", "document", "findings"), GRANT_FINDINGS, ids=[g[0] for g in GRANT_FINDINGS]
)
def test_installation_grant_findings(case: str, document: Any, findings: list[str]) -> None:
    assert pto.installation_grant_findings([document]) == findings


@pytest.mark.parametrize(
    "document",
    [
        _pod(env=[_grant_env(), MODE_ENV, {"name": "CURIE_MAX_TURNS", "value": "32"}]),
        _template(claim=CLAIM, env=[_grant_env(), MODE_ENV]),
        _secret(f"{CLAIM}-tokens", {pto.CLAIM_LABEL: CLAIM}, [pps.PERSONAL_GRANT_ENV]),
        _secret("curie-pe-0a0b-g1", {pto.CUSTODY_LABEL: "true"}, [pps.PERSONAL_GRANT_ENV]),
        _pod(claim=None, env=[{"name": "CURIE_MODEL_AUTH_MODE", "value": "service"}]),
        {"items": [_pod(env=[_grant_env()])]},
    ],
)
def test_per_claim_grant_delivery_is_allowed(document: Any) -> None:
    assert pto.installation_grant_findings([document]) == []


def test_dex_image_is_the_oidc_harness_pin() -> None:
    text = (REPO_ROOT / "tools/oidc-e2e/oidc_e2e.py").read_text()
    name, _, digest = pto.DEX_IMAGE.partition("@")
    assert f'"{name}"' in text and f'"@{digest}"' in text


def test_dex_config_has_two_distinct_password_users() -> None:
    config = pto.dex_config(
        issuer=ISSUER,
        client_id=pto.OIDC_CLIENT_ID,
        client_secret="client-marker",
        redirect_uri=REDIRECT,
        users=[pto.OWNER_EMAILS["a"], pto.OWNER_EMAILS["b"]],
    )
    assert config["issuer"] == ISSUER and config["enablePasswordDB"] is True
    assert config["staticClients"] == [
        {
            "id": pto.OIDC_CLIENT_ID,
            "name": "Curie console (two-owner acceptance)",
            "secret": "client-marker",
            "redirectURIs": [REDIRECT],
        }
    ]
    users = config["staticPasswords"]
    assert [user["email"] for user in users] == ["owner-a@example.com", "owner-b@example.com"]
    assert [user["userID"] for user in users] == [
        str(uuid.uuid5(uuid.NAMESPACE_URL, user["email"])) for user in users
    ]
    assert len({user["userID"] for user in users}) == 2
    assert config["web"] == {"http": "0.0.0.0:5556"} and "connectors" not in config


@pytest.mark.parametrize(
    "changes",
    [
        {"users": []},
        {"users": ["owner-a@example.com", "owner-a@example.com"]},
        {"users": ["Not An Email"]},
        {"issuer": ISSUER + "/"},
        {"client_secret": ""},
    ],
)
def test_dex_config_refusals(changes: dict[str, Any]) -> None:
    arguments: dict[str, Any] = {
        "issuer": ISSUER,
        "client_id": pto.OIDC_CLIENT_ID,
        "client_secret": "client-marker",
        "redirect_uri": REDIRECT,
        "users": list(pto.OWNER_EMAILS.values()),
    }
    arguments.update(changes)
    _refused("arguments_invalid", lambda: pto.dex_config(**arguments))


DEX_LABELS = {"app.kubernetes.io/name": "pto-dex", "curie.dev/personal-profile-run": "acme"}


def _dex(labels: Mapping[str, str] = DEX_LABELS) -> list[dict[str, Any]]:
    return pto.dex_manifests(
        namespace=NAMESPACE,
        name="pto-dex",
        image_ref="localhost:5999/dex@sha256:" + "5" * 64,
        config_secret="pto-dex-config",
        labels=labels,
        config={"issuer": ISSUER},
    )


def test_dex_manifests_shape() -> None:
    secret, deployment, service = _dex()
    assert [item["kind"] for item in (secret, deployment, service)] == [
        "Secret",
        "Deployment",
        "Service",
    ]
    assert json.loads(secret["stringData"][pto.DEX_CONFIG_KEY]) == {"issuer": ISSUER}
    assert all(item["metadata"]["namespace"] == NAMESPACE for item in (secret, deployment, service))
    template = deployment["spec"]["template"]
    assert (
        deployment["spec"]["selector"]["matchLabels"]
        == DEX_LABELS
        == template["metadata"]["labels"]
    )
    container = template["spec"]["containers"][0]
    # The image ENTRYPOINT (docker-entrypoint) templates config and needs a writable root
    # filesystem, so under readOnlyRootFilesystem the pod crash-loops. Running dex directly
    # with a writable emptyDir at /tmp starts and listens on 5556.
    assert container["command"] == ["dex", "serve", "/etc/dex/config.yaml"]
    assert "args" not in container
    assert {"name": "tmp", "mountPath": "/tmp"} in container["volumeMounts"]
    assert {"name": "tmp", "emptyDir": {}} in template["spec"]["volumes"]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert template["spec"]["automountServiceAccountToken"] is False
    assert service["spec"] == {
        "type": "ClusterIP",
        "selector": DEX_LABELS,
        "ports": [{"name": "http", "port": 5556, "targetPort": 5556}],
    }
    # No chart selector (name plus instance of the release) can match the Dex pod.
    assert "app.kubernetes.io/instance" not in template["metadata"]["labels"]


@pytest.mark.parametrize(
    "labels",
    [
        {**DEX_LABELS, "app.kubernetes.io/instance": RELEASE},
        {**DEX_LABELS, "app.kubernetes.io/component": "api"},
        {"app.kubernetes.io/name": "curie"},
        {**DEX_LABELS, pto.CLAIM_LABEL: CLAIM},
        {**DEX_LABELS, "sandbox.users.io/curie-admission-claim": CLAIM},
        {},
    ],
)
def test_dex_labels_must_not_match_chart_selectors(labels: Mapping[str, str]) -> None:
    _refused("arguments_invalid", lambda: _dex(labels))


# --------------------------------------------------------------------------
# G. OIDC login against scripted Dex and API redirects
# --------------------------------------------------------------------------

API_LOCAL = "http://127.0.0.1:41001"
DEX_LOCAL = "http://127.0.0.1:41002"
ISSUER_ORIGIN = "http://pto-dex.pto-acme-1.svc.cluster.local:5556"
REDIRECT_ORIGIN = "http://pto-acme-curie-api.pto-acme-1.svc.cluster.local:8000"
STATE_MARKER = "state-marker-0001"
SESSION_MARKER = "session-marker-0001"


@pytest.mark.parametrize(
    ("url", "base", "expected"),
    [
        (
            ISSUER_ORIGIN + "/dex/auth?client_id=curie-console&state=s1",
            None,
            DEX_LOCAL + "/dex/auth?client_id=curie-console&state=s1",
        ),
        (
            "/dex/auth/local/login?back=&state=r1",
            DEX_LOCAL + "/dex/auth?x=1",
            DEX_LOCAL + "/dex/auth/local/login?back=&state=r1",
        ),
        (
            "/dex/approval?req=r1",
            ISSUER_ORIGIN + "/dex/auth/local",
            DEX_LOCAL + "/dex/approval?req=r1",
        ),
        ("login?back=", DEX_LOCAL + "/dex/auth/local/", DEX_LOCAL + "/dex/auth/local/login?back="),
        (
            REDIRECT_ORIGIN + "/console/oidc/callback?code=c1&state=s1",
            DEX_LOCAL + "/dex/approval",
            API_LOCAL + "/console/oidc/callback?code=c1&state=s1",
        ),
        (
            "HTTP://PTO-DEX.pto-acme-1.svc.cluster.local:5556/dex#frag",
            None,
            DEX_LOCAL + "/dex#frag",
        ),
        ("http://example.test/dex", None, "http://example.test/dex"),
        (
            "https://pto-dex.pto-acme-1.svc.cluster.local:5556/dex",
            None,
            "https://pto-dex.pto-acme-1.svc.cluster.local:5556/dex",
        ),
        (
            "http://pto-dex.pto-acme-1.svc.cluster.local/dex",
            None,
            "http://pto-dex.pto-acme-1.svc.cluster.local/dex",
        ),
    ],
)
def test_rewrite_origin(url: str, base: str | None, expected: str) -> None:
    mapping = {ISSUER_ORIGIN: DEX_LOCAL, REDIRECT_ORIGIN: API_LOCAL}
    assert pto.rewrite_origin(url, mapping, base=base) == expected


def test_rewrite_origin_makes_default_ports_explicit() -> None:
    mapping = {"http://dex.test": "http://127.0.0.1:9"}
    assert pto.rewrite_origin("http://dex.test:80/a?b=1", mapping) == "http://127.0.0.1:9/a?b=1"


Response = tuple[int, list[tuple[str, str]], bytes]


class ScriptedOpener:
    """Replays the Dex and API redirect shapes, recording every request."""

    def __init__(
        self, script: list[tuple[str, str, Callable[[Mapping[str, str], bytes | None], Response]]]
    ) -> None:
        self.script = list(script)
        self.requests: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> Response:
        self.requests.append((method, url, dict(headers), body))
        assert self.script, f"unexpected request {method} {url}"
        expected_method, expected_url, answer = self.script.pop(0)
        assert (method, url) == (expected_method, expected_url)
        return answer(headers, body)


def _redirect(location: str, *cookies: str, status: int = 302) -> Callable[..., Response]:
    headers = [("Location", location), *(("Set-Cookie", cookie) for cookie in cookies)]
    return lambda _headers, _body: (status, headers, b"")


def _page(html: str) -> Callable[..., Response]:
    return lambda _headers, _body: (200, [("Content-Type", "text/html")], html.encode())


AUTHORIZE = (
    "/dex/auth?client_id=curie-console&redirect_uri="
    + urllib.parse.quote(REDIRECT_ORIGIN + pto.CALLBACK_PATH, safe="")
    + "&response_type=code&scope=openid+email+profile&state=s1&nonce=n1"
    "&code_challenge=c1&code_challenge_method=S256"
)
CALLBACK_QUERY = "?code=code-marker&state=s1"
LOGIN_FORM = (
    '<form method="post" action="/dex/auth/local/login?back=&amp;state=r1">'
    '<input name="login"><input type="password" name="password"></form>'
)


def _password(headers: Mapping[str, str], body: bytes | None) -> Response:
    form = urllib.parse.parse_qs((body or b"").decode())
    assert form == {"login": [pto.OWNER_EMAILS["a"]], "password": [pto.DEX_PASSWORD]}
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert headers["Cookie"] == "dex_session=dex-cookie-marker"
    return 303, [("Location", "/dex/approval?req=r1&hmac=h1")], b""


def _callback(headers: Mapping[str, str], _body: bytes | None) -> Response:
    assert headers == {"Cookie": f"{pto.STATE_COOKIE}={STATE_MARKER}"}
    return (
        303,
        [
            ("Location", "/"),
            (
                "Set-Cookie",
                f"{pto.SESSION_COOKIE}={SESSION_MARKER}; HttpOnly; Path=/; SameSite=lax; Secure",
            ),
            ("Set-Cookie", f"{pto.STATE_COOKIE}=; Max-Age=0; Path=/; Secure"),
        ],
        b"",
    )


def _start() -> tuple[str, str, Callable[..., Response]]:
    return (
        "GET",
        API_LOCAL + pto.LOGIN_PATH,
        _redirect(
            ISSUER_ORIGIN + AUTHORIZE,
            f"{pto.STATE_COOKIE}={STATE_MARKER}; HttpOnly; Path=/; SameSite=lax; Secure",
        ),
    )


def _login(opener: ScriptedOpener) -> pps.HeldSecret:
    return pto.oidc_login(
        opener,
        api_origin=API_LOCAL,
        issuer_origin=ISSUER_ORIGIN,
        dex_local_origin=DEX_LOCAL,
        redirect_origin=REDIRECT_ORIGIN,
        email=pto.OWNER_EMAILS["a"],
        password=pto.DEX_PASSWORD,
    )


def test_oidc_login_follows_path_only_hops_and_a_chooser() -> None:
    opener = ScriptedOpener(
        [
            _start(),
            (
                "GET",
                DEX_LOCAL + AUTHORIZE,
                _page('<a href="/dex/auth/local?req=r1&amp;x=1">Log in</a>'),
            ),
            (
                "GET",
                DEX_LOCAL + "/dex/auth/local?req=r1&x=1",
                _redirect(
                    "/dex/auth/local/login?back=&state=r1",
                    "dex_session=dex-cookie-marker; Path=/dex",
                ),
            ),
            ("GET", DEX_LOCAL + "/dex/auth/local/login?back=&state=r1", _page(LOGIN_FORM)),
            ("POST", DEX_LOCAL + "/dex/auth/local/login?back=&state=r1", _password),
            (
                "GET",
                DEX_LOCAL + "/dex/approval?req=r1&hmac=h1",
                _redirect(REDIRECT_ORIGIN + pto.CALLBACK_PATH + CALLBACK_QUERY, status=303),
            ),
            ("GET", API_LOCAL + pto.CALLBACK_PATH + CALLBACK_QUERY, _callback),
        ]
    )
    session = _login(opener)
    assert session.reveal() == SESSION_MARKER and SESSION_MARKER not in repr(session)
    assert not opener.script
    origins = {pto._origin(url) for _method, url, _headers, _body in opener.requests}
    assert origins == {"http://127.0.0.1:41001", "http://127.0.0.1:41002"}
    posts = [url for method, url, _headers, _body in opener.requests if method == "POST"]
    assert all(url.startswith(DEX_LOCAL) for url in posts) and len(posts) == 1


def test_oidc_login_rewrites_an_absolute_dex_hop() -> None:
    opener = ScriptedOpener(
        [
            _start(),
            (
                "GET",
                DEX_LOCAL + AUTHORIZE,
                _redirect(
                    ISSUER_ORIGIN + "/dex/auth/local/login?back=&state=r1",
                    "dex_session=dex-cookie-marker",
                ),
            ),
            (
                "GET",
                DEX_LOCAL + "/dex/auth/local/login?back=&state=r1",
                _page('<input name="password">'),
            ),
            ("POST", DEX_LOCAL + "/dex/auth/local/login?back=&state=r1", _password),
            (
                "GET",
                DEX_LOCAL + "/dex/approval?req=r1&hmac=h1",
                _redirect(REDIRECT_ORIGIN + pto.CALLBACK_PATH + CALLBACK_QUERY),
            ),
            ("GET", API_LOCAL + pto.CALLBACK_PATH + CALLBACK_QUERY, _callback),
        ]
    )
    assert _login(opener).reveal() == SESSION_MARKER


LOGIN_FAILURES: list[tuple[str, Callable[[], list[Any]], str]] = [
    (
        "start not redirected",
        lambda: [("GET", API_LOCAL + pto.LOGIN_PATH, _page("no"))],
        "login_start",
    ),
    (
        "no state cookie",
        lambda: [("GET", API_LOCAL + pto.LOGIN_PATH, _redirect(ISSUER_ORIGIN + AUTHORIZE))],
        "state_cookie",
    ),
    (
        "authorize elsewhere",
        lambda: [
            (
                "GET",
                API_LOCAL + pto.LOGIN_PATH,
                _redirect("http://idp.example.test/auth", f"{pto.STATE_COOKIE}={STATE_MARKER}"),
            )
        ],
        "authorize_origin",
    ),
    (
        "dex hop to a foreign origin",
        lambda: [
            _start(),
            ("GET", DEX_LOCAL + AUTHORIZE, _redirect("http://idp.example.test/dex/auth/local")),
        ],
        "unexpected_origin",
    ),
    (
        "form posting to a foreign origin",
        lambda: [
            _start(),
            (
                "GET",
                DEX_LOCAL + AUTHORIZE,
                _page('<form action="http://idp.example.test/x"><input name="password"></form>'),
            ),
        ],
        "unexpected_origin",
    ),
    (
        "redirect without location",
        lambda: [_start(), ("GET", DEX_LOCAL + AUTHORIZE, lambda _h, _b: (302, [], b""))],
        "dex_location",
    ),
    (
        "unexpected page",
        lambda: [_start(), ("GET", DEX_LOCAL + AUTHORIZE, _page("<p>error</p>"))],
        "dex_unexpected_response",
    ),
    (
        "redirect loop",
        lambda: (
            [_start()] + [("GET", DEX_LOCAL + AUTHORIZE, _redirect(AUTHORIZE))] * pto.LOGIN_HOPS
        ),
        "dex_hops_exhausted",
    ),
    (
        "callback without a session",
        lambda: [
            _start(),
            (
                "GET",
                DEX_LOCAL + AUTHORIZE,
                _redirect(REDIRECT_ORIGIN + pto.CALLBACK_PATH + CALLBACK_QUERY),
            ),
            ("GET", API_LOCAL + pto.CALLBACK_PATH + CALLBACK_QUERY, _redirect("/?error=oidc")),
        ],
        "callback",
    ),
]


@pytest.mark.parametrize(
    ("case", "script", "step"), LOGIN_FAILURES, ids=[f[0] for f in LOGIN_FAILURES]
)
def test_oidc_login_failures(case: str, script: Callable[[], list[Any]], step: str) -> None:
    opener = ScriptedOpener(script())
    with pytest.raises(pto.LoginFailed) as caught:
        _login(opener)
    assert caught.value.step == step
    assert not opener.script
    sent = [url for _method, url, _headers, _body in opener.requests]
    assert not any("example.test" in url for url in sent)
    assert not any(body for method, _url, _headers, body in opener.requests if method == "POST")


def test_oidc_login_refuses_overlapping_origins() -> None:
    with pytest.raises(pto.LoginFailed) as caught:
        pto.oidc_login(
            ScriptedOpener([]),
            api_origin=API_LOCAL,
            issuer_origin=API_LOCAL,
            dex_local_origin=DEX_LOCAL,
            redirect_origin=REDIRECT_ORIGIN,
            email="owner-a@example.com",
            password=pto.DEX_PASSWORD,
        )
    assert caught.value.step == "origins"


# --------------------------------------------------------------------------
# H. Issue fixtures and the CI acknowledgement test
# --------------------------------------------------------------------------

NONCE = "0a1b2c3d4e5f6a7b"


def test_owner_a_issue_keeps_the_single_profile_shape() -> None:
    title, body = pto.render_issue("a", NONCE)
    single = (HERE / "fixtures" / "personal-profile-issue.md").read_text()
    assert body == single.replace("{nonce}", NONCE)
    assert title == "Record the acceptance nonce"
    assert f"acceptance nonce: {NONCE}" in body and "{" not in body


def test_owner_b_issue_carries_the_exact_ci_ack_test() -> None:
    title, body = pto.render_issue("b", NONCE)
    assert title == "Add the CI acknowledgement test"
    assert "unitconv/tests/test_ci_ack.py" in body and "NONCE-B.md" in body
    assert f"acceptance nonce: {NONCE}" in body
    block = body.split("```python\n", 1)[1].split("\n```", 1)[0] + "\n"
    assert block == pto.CI_ACK_TEST
    assert "checks must pass" in body and "For reviewers" in body
    assert "acknowledgement check" in body and "{nonce}" not in body


@pytest.mark.parametrize(
    ("owner", "nonce"), [("c", NONCE), ("a", ""), ("b", "../x"), ("a", "short")]
)
def test_render_issue_refusals(owner: str, nonce: str) -> None:
    _refused("arguments_invalid", lambda: pto.render_issue(owner, nonce))


def _ci_ack(
    tmp_path: Path, head_ref: str | None, ack: str | None
) -> subprocess.CompletedProcess[str]:
    root = tmp_path / "repo"
    tests = root / "unitconv" / "tests"
    tests.mkdir(parents=True, exist_ok=True)
    (tests / "test_ci_ack.py").write_text(pto.CI_ACK_TEST)
    if ack is not None:
        (root / "CI_ACK.md").write_text(ack)
    env = {"PATH": os.environ.get("PATH", "")}
    if head_ref is not None:
        env["GITHUB_HEAD_REF"] = head_ref
    return subprocess.run(
        [sys.executable, "-I", "-m", "unittest", "discover", "-s", "unitconv/tests", "-v"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


HEAD_REF = "curie/publication-" + "0123456789abcdef" * 2


def test_ci_ack_test_is_skipped_without_a_head_ref(tmp_path: Path) -> None:
    result = _ci_ack(tmp_path, None, None)
    assert result.returncode == 0 and "skipped" in result.stderr and "::error" not in result.stdout


@pytest.mark.parametrize("ack", [None, "", "ci ack: curie/publication-other\n", "ci ack:\n"])
def test_ci_ack_test_fails_without_the_line(tmp_path: Path, ack: str | None) -> None:
    result = _ci_ack(tmp_path, HEAD_REF, ack)
    assert result.returncode == 1
    first = result.stdout.splitlines()[0]
    assert first == (
        f"::error file=CI_ACK.md,line=1::CI_ACK.md must contain the line 'ci ack: {HEAD_REF}'"
    )


@pytest.mark.parametrize("ack", [f"ci ack: {HEAD_REF}\n", f"notes\n  ci ack: {HEAD_REF}  \n"])
def test_ci_ack_test_passes_with_the_line(tmp_path: Path, ack: str) -> None:
    result = _ci_ack(tmp_path, HEAD_REF, ack)
    assert result.returncode == 0 and "::error" not in result.stdout


# --------------------------------------------------------------------------
# I. Evidence contract and judges
# --------------------------------------------------------------------------


def _passing(criterion: str, key: str) -> Any:
    rule = pto.COMPARISONS.get(criterion, {}).get(key)
    return True if rule is None else rule[1]


#: Every card a complete run delivers: the two drill cards and the four scenario cards.
CARDS = ("drill_reject", "drill_approve", "card1", "card2", "card3", "card4")


def _record_complete(ledger: pto.TwoOwnerLedger) -> None:
    for criterion, keys in pto.EXPECTED.items():
        for key in keys:
            ledger.record(criterion, key, _passing(criterion, key))
    ledger.record("budget", "cleanup_elapsed", 120.0)
    ledger.record(
        "cleanup",
        "token_scan",
        {
            name: {"scanned_bytes": 10, "token_bytes_found": False}
            for name in pto.LIVE_SCAN_CHANNELS
        },
    )
    ledger.record(
        "cleanup",
        "capture_audit",
        {name: {**entry, "sources": 2} for name, entry in pto.capture_receipts().items()},
    )
    ledger.record("cleanup", "traces_backend", "in_cluster")
    for card in CARDS:
        state = {"delivered": True, "attempts": 1, "error": False}
        ledger.record("approval", f"{card}_card", state)
    residue = pto.card_residue(
        {card: f"ref-{card}" for card in CARDS},
        ledger.observations("approval"),
        channel="ref-channel",
    )
    ledger.record("cleanup", "card_residue", residue)
    for step in ("fence", "inventory", "forge", "namespace"):
        ledger.record_cleanup(f"step:{step}", step, "ok")
    for kind in pto.INVENTORY_KINDS:
        ledger.record_cleanup(f"inventory:{kind}", kind, "absent")


@functools.lru_cache(maxsize=1)
def _template_ledger() -> Path:
    """A complete live ledger built once, then copied into each test's directory."""

    folder = Path(tempfile.mkdtemp(prefix="pto-judge-"))
    atexit.register(shutil.rmtree, folder, True)
    ledger = _ledger(folder)
    open_b_r1: list[pps.Reservation] = []
    for index, group in enumerate(pto.MANIFEST):
        if group.name == "b-r2":
            _settle(ledger, open_b_r1)
        reservations = ledger.reserve_group(group.name, now=T0 + 60 * index)
        if group.name == "b-r1":
            open_b_r1 = reservations
        else:
            _settle(ledger, reservations)
    ledger.end_live(now=T0 + 4000)
    _record_complete(ledger)
    ledger.close()
    return folder / "state"


def _complete(tmp_path: Path) -> pto.TwoOwnerLedger:
    state = _private(tmp_path / "state")
    target = state / pps.LEDGER_FILE
    shutil.copyfile(_template_ledger() / pps.LEDGER_FILE, target)
    os.chmod(target, 0o600)
    ledger = pto.TwoOwnerLedger.open(state, RUN_ID)
    ledger.db.execute("PRAGMA synchronous=OFF")
    return ledger


def test_judges_pass_on_a_complete_ledger(tmp_path: Path) -> None:
    ledger = _complete(tmp_path)
    for criterion in pto.CRITERIA:
        result = pto.judge(ledger, criterion)
        assert result["verdict"] == "pass", (criterion, result["checks"])
        assert set(pto.EXPECTED[criterion]) <= set(result["checks"])
    every = pto.judge(ledger, "all")
    assert every["verdict"] == "pass"
    assert every["not_met"] == ["uncommitted workspace restoration: Not met, tracked in #5541"]
    assert every["observed"] == {}
    assert pto.judge(ledger, "restarts")["not_met"] == every["not_met"]
    budget = pto.judge(ledger, "budget")["counts"]
    assert budget["sessions"] == 16 and budget["output_tokens"] == 20564
    assert (budget["output_tokens_a"], budget["output_tokens_b"]) == (11732, 8832)


EXPECTED_KEYS = [(criterion, key) for criterion, keys in pto.EXPECTED.items() for key in keys]


@pytest.mark.parametrize(
    ("criterion", "key"), EXPECTED_KEYS, ids=[f"{c}.{k}" for c, k in EXPECTED_KEYS]
)
def test_each_expected_key_is_required(tmp_path: Path, criterion: str, key: str) -> None:
    ledger = _complete(tmp_path)
    good = _passing(criterion, key)
    wrong = [False, None, "true", 1 if good is True else True]
    if good is not True:
        wrong.append(good + 1)
    for value in wrong:
        ledger.record(criterion, key, value)
        result = pto.judge(ledger, criterion)
        assert result["verdict"] == "fail" and result["checks"][key] is False, value
    ledger.db.execute(
        "DELETE FROM observations WHERE run_id=? AND criterion=? AND key=?",
        (RUN_ID, criterion, key),
    )
    result = pto.judge(ledger, criterion)
    assert result["verdict"] == "fail" and result["checks"][key] is False
    assert pto.judge(ledger, "all")["checks"][f"{criterion}.{key}"] is False
    ledger.record(criterion, key, good)
    assert pto.judge(ledger, criterion)["verdict"] == "pass"


def _session(ledger: pto.TwoOwnerLedger, sql: str, *args: Any) -> None:
    ledger.db.execute(sql, args)


LEDGER_BREAKS: list[tuple[str, Callable[[pto.TwoOwnerLedger], None], str]] = [
    (
        "unknown usage",
        lambda led: _session(
            led, "UPDATE sessions SET output_tokens=NULL, source='unknown' WHERE seq=2"
        ),
        "no_unknown_usage",
    ),
    (
        "unreserved session",
        lambda led: led.record_unreserved_session(
            "a", "a1", pto.DIFF_REVIEWER, pps.SessionUsage(1, 1, 1.0, "m")
        ),
        "no_unreserved_sessions",
    ),
    (
        "budget exceeded",
        lambda led: _session(led, "UPDATE runs SET budget_state='exceeded'"),
        "budget_not_exceeded",
    ),
    (
        "second live start",
        lambda led: _session(led, "UPDATE runs SET live_starts=2"),
        "no_paid_retry",
    ),
    ("paid retry recorded", lambda led: led.record("budget", "paid_retry", True), "no_paid_retry"),
    (
        "live window overrun",
        lambda led: _session(led, "UPDATE runs SET live_ended_at=?", T0 + 4801),
        "live_elapsed_within",
    ),
    (
        "live never ended",
        lambda led: _session(led, "UPDATE runs SET live_ended_at=NULL"),
        "live_elapsed_within",
    ),
    (
        "cleanup overrun",
        lambda led: led.record("budget", "cleanup_elapsed", 601.0),
        "cleanup_elapsed_within",
    ),
    (
        "cleanup unknown",
        lambda led: led.record("budget", "cleanup_elapsed", True),
        "cleanup_elapsed_within",
    ),
    (
        "missing group",
        lambda led: _session(led, "DELETE FROM sessions WHERE seq IN (14, 15, 16)"),
        "all_groups_reserved",
    ),
    (
        "session over its bound",
        lambda led: _session(led, "UPDATE sessions SET elapsed=301 WHERE seq=2"),
        "session_bounds",
    ),
    (
        "probe over its bound",
        lambda led: _session(led, "UPDATE sessions SET output_tokens=33 WHERE seq=1"),
        "session_bounds",
    ),
    (
        "probe settled as native",
        lambda led: _session(led, "UPDATE sessions SET source='native' WHERE seq=1"),
        "settled_from_native",
    ),
    (
        "role settled as probe",
        lambda led: _session(led, "UPDATE sessions SET source='probe' WHERE seq=2"),
        "settled_from_native",
    ),
    (
        "owner over its allocation",
        lambda led: _session(led, "UPDATE sessions SET output_tokens=8000 WHERE seq IN (2, 3, 4)"),
        "owner_output_within_a",
    ),
    (
        "aggregate over with prior spend",
        lambda led: _session(
            led,
            "UPDATE observations SET value_json='12205' WHERE criterion='budget' AND key=?",
            pto.PRIOR_SPEND_KEY,
        ),
        "aggregate_output_within",
    ),
    (
        "turns over",
        lambda led: _session(
            led, "UPDATE sessions SET assistant_turns=32 WHERE seq <= 9 AND role != 'probe'"
        ),
        "total_turns_within",
    ),
    (
        "owner window short",
        lambda led: _session(
            led,
            "UPDATE observations SET value_json=? WHERE criterion='budget' "
            "AND key='usable_until_b'",
            json.dumps(T0 + 3999),
        ),
        "owner_windows_covered",
    ),
]


@pytest.mark.parametrize(
    ("case", "corrupt", "check"), LEDGER_BREAKS, ids=[b[0] for b in LEDGER_BREAKS]
)
def test_budget_judge_reads_the_ledger(
    tmp_path: Path, case: str, corrupt: Callable[[pto.TwoOwnerLedger], None], check: str
) -> None:
    ledger = _complete(tmp_path)
    corrupt(ledger)
    result = pto.judge(ledger, "budget")
    assert result["verdict"] == "fail" and result["checks"][check] is False, result["checks"]


def _group_row(ledger: pto.TwoOwnerLedger, group: str) -> dict[str, Any]:
    return next(row for row in ledger.session_rows() if row["grp"] == group)


def _set_session(ledger: pto.TwoOwnerLedger, group: str, column: str, value: Any) -> None:
    seq = _group_row(ledger, group)["seq"]
    _session(ledger, f"UPDATE sessions SET {column}=? WHERE seq=?", value, seq)


def _owner_a_at(ledger: pto.TwoOwnerLedger, extra: int) -> None:
    spent = sum(int(row["output_tokens"]) for row in ledger.session_rows() if row["owner"] == "a")
    tokens = int(_group_row(ledger, "a3")["output_tokens"])
    _set_session(ledger, "a3", "output_tokens", tokens + pto.OWNER_OUTPUT_TOKENS - spent + extra)


def _aggregate_at(ledger: pto.TwoOwnerLedger, extra: int) -> None:
    total = sum(int(row["output_tokens"]) for row in ledger.session_rows())
    _session(
        ledger,
        "UPDATE observations SET value_json=? WHERE criterion='budget' AND key=?",
        json.dumps(pto.MAX_OUTPUT_TOKENS - total + extra),
        pto.PRIOR_SPEND_KEY,
    )


def _turns_at(ledger: pto.TwoOwnerLedger, extra: int) -> None:
    rows = ledger.session_rows()
    need = pto.MAX_TURNS - sum(int(row["assistant_turns"]) for row in rows) + extra
    for row in rows:
        if pto.GROUPS[row["grp"]].kind == "probe" or need <= 0:
            continue
        more = min(need, pto.SESSION_TURNS - int(row["assistant_turns"]))
        _session(
            ledger,
            "UPDATE sessions SET assistant_turns=? WHERE seq=?",
            int(row["assistant_turns"]) + more,
            row["seq"],
        )
        need -= more
    assert need == 0


#: (case, budget check, how to put the complete ledger at that ceiling plus ``extra``).
BUDGET_CEILINGS: list[tuple[str, str, Callable[[pto.TwoOwnerLedger, int], None]]] = [
    (
        "session output tokens",
        "session_bounds",
        lambda led, extra: _set_session(
            led, "b-r1", "output_tokens", pto.SESSION_OUTPUT_TOKENS + extra
        ),
    ),
    (
        "session turns",
        "session_bounds",
        lambda led, extra: _set_session(led, "a1", "assistant_turns", pto.SESSION_TURNS + extra),
    ),
    (
        "session seconds",
        "session_bounds",
        lambda led, extra: _set_session(led, "a1", "elapsed", pto.SESSION_SECONDS + extra),
    ),
    ("owner output tokens", "owner_output_within_a", _owner_a_at),
    ("aggregate output tokens", "aggregate_output_within", _aggregate_at),
    ("total turns", "total_turns_within", _turns_at),
    (
        "live seconds",
        "live_elapsed_within",
        lambda led, extra: _session(
            led, "UPDATE runs SET live_ended_at=?", T0 + pto.LIVE_SECONDS + extra
        ),
    ),
    (
        "cleanup seconds",
        "cleanup_elapsed_within",
        lambda led, extra: led.record(
            "budget", "cleanup_elapsed", float(pto.CLEANUP_SECONDS + extra)
        ),
    ),
]


@pytest.mark.parametrize(
    ("case", "check", "place"), BUDGET_CEILINGS, ids=[c[0] for c in BUDGET_CEILINGS]
)
def test_budget_ceilings_are_inclusive(
    tmp_path: Path, case: str, check: str, place: Callable[[pto.TwoOwnerLedger, int], None]
) -> None:
    at = _complete(tmp_path / "at")
    place(at, 0)
    assert pto.judge(at, "budget")["checks"][check] is True
    over = _complete(tmp_path / "over")
    place(over, 1)
    assert pto.judge(over, "budget")["checks"][check] is False


CLEANUP_BREAKS: list[tuple[str, Callable[[pto.TwoOwnerLedger], None], str]] = [
    (
        "residual inventory",
        lambda led: led.record_cleanup("inventory:claims", "claims", "present"),
        "inventory_absent",
    ),
    (
        "missing inventory kind",
        lambda led: _session(
            led, "DELETE FROM cleanup WHERE resource_kind='inventory:custody_secrets'"
        ),
        "inventory_complete",
    ),
    ("failed step", lambda led: led.record_cleanup("step:forge", "forge", "failed"), "steps_ok"),
    (
        "no steps",
        lambda led: _session(led, "DELETE FROM cleanup WHERE resource_kind LIKE 'step:%'"),
        "steps_recorded",
    ),
    (
        "token found",
        lambda led: led.record(
            "cleanup",
            "token_scan",
            {
                name: {"scanned_bytes": 10, "token_bytes_found": name == "harvest"}
                for name in pto.LIVE_SCAN_CHANNELS
            },
        ),
        "token_bytes_absent",
    ),
    (
        "channel not scanned",
        lambda led: led.record(
            "cleanup", "token_scan", {"stdout": {"scanned_bytes": 10, "token_bytes_found": False}}
        ),
        "token_scan_complete",
    ),
    (
        "capture channel scanned zero bytes",
        lambda led: _set_scan(led, "pod_logs", {"scanned_bytes": 0, "token_bytes_found": False}),
        "token_scan_complete",
    ),
    (
        "events never scanned",
        lambda led: _set_scan(led, "events", None),
        "token_scan_complete",
    ),
    (
        "capture read failed",
        lambda led: _set_capture(led, "pod_logs", _pod_logs(sources=9, failures=1)),
        "capture_audit_complete",
    ),
    (
        "pod removals unrecorded",
        lambda led: _set_capture(led, "pod_logs", {"sources": 2, "failures": 0}),
        "capture_audit_complete",
    ),
    (
        "removed pod unnamed",
        lambda led: _set_capture(
            led, "pod_logs", _pod_logs(uncaptured=[_removal("", "runner", "emergency-stop")])
        ),
        "capture_audit_complete",
    ),
    (
        "stop-path removal claimed captured",
        lambda led: _set_capture(
            led,
            "pod_logs",
            _pod_logs(before_deletion=[_removal("pod-a1", "runner", "emergency-fence")]),
        ),
        "capture_audit_complete",
    ),
    (
        "removed pod of an unknown workload",
        lambda led: _set_capture(
            led, "pod_logs", _pod_logs(before_deletion=[_removal("api-0", "api", "fence")])
        ),
        "capture_audit_complete",
    ),
    (
        "capture read nothing",
        lambda led: _set_capture(led, "publication_captures", {"sources": 0, "failures": 0}),
        "capture_audit_complete",
    ),
    (
        "capture receipt missing",
        lambda led: _set_capture(led, "api_captures", None),
        "capture_audit_complete",
    ),
    (
        "traces sent to an unreadable backend",
        lambda led: led.record("cleanup", "traces_backend", "external"),
        "traces_audited",
    ),
    (
        "trace backend unrecorded",
        lambda led: led.record("cleanup", "traces_backend", None),
        "traces_audited",
    ),
    (
        "card claimed removed",
        lambda led: _set_residue(led, removed=True),
        "card_residue_recorded",
    ),
    (
        "delivered card unnamed",
        lambda led: _set_residue(led, drop="drill_approve"),
        "card_residue_recorded",
    ),
    (
        "card residue status unexplained",
        lambda led: _set_residue(led, status=("card2", "deleted")),
        "card_residue_recorded",
    ),
    (
        "delivered card recorded as undelivered",
        lambda led: _set_residue(led, status=("card4", "not_delivered")),
        "card_residue_recorded",
    ),
    (
        "cleanup past its window",
        lambda led: led.record("cleanup", "cleanup_within_window", False),
        "cleanup_within_window",
    ),
]


def _set_scan(ledger: pto.TwoOwnerLedger, channel: str, entry: Any) -> None:
    scans = dict(ledger.observations("cleanup")["token_scan"])
    if entry is None:
        scans.pop(channel)
    else:
        scans[channel] = entry
    ledger.record("cleanup", "token_scan", scans)


def _removal(pod: str, workload: str, step: str) -> dict[str, str]:
    return {"pod": pps.ref_digest(pod) if pod else "", "workload": workload, "step": step}


def _pod_logs(**fields: Any) -> dict[str, Any]:
    """A pod_logs receipt read from two sources with no failure, ``fields`` overriding."""

    return {**pto.capture_receipts()["pod_logs"], "sources": 2, **fields}


def _set_capture(ledger: pto.TwoOwnerLedger, channel: str, entry: Any) -> None:
    receipts = dict(ledger.observations("cleanup")["capture_audit"])
    if entry is None:
        receipts.pop(channel)
    else:
        receipts[channel] = entry
    ledger.record("cleanup", "capture_audit", receipts)


def _set_residue(
    ledger: pto.TwoOwnerLedger,
    *,
    removed: bool = False,
    drop: str = "",
    status: tuple[str, str] | None = None,
) -> None:
    residue = json.loads(json.dumps(ledger.observations("cleanup")["card_residue"]))
    residue["removed"] = removed
    residue["cards"].pop(drop, None)
    if status is not None:
        card, value = status
        residue["cards"][card]["residue"] = value
    ledger.record("cleanup", "card_residue", residue)


@pytest.mark.parametrize(
    ("case", "corrupt", "check"), CLEANUP_BREAKS, ids=[b[0] for b in CLEANUP_BREAKS]
)
def test_cleanup_judge_reads_the_inventory(
    tmp_path: Path, case: str, corrupt: Callable[[pto.TwoOwnerLedger], None], check: str
) -> None:
    ledger = _complete(tmp_path)
    corrupt(ledger)
    result = pto.judge(ledger, "cleanup")
    assert result["verdict"] == "fail" and result["checks"][check] is False


def test_removals_recorded_requires_both_lists_and_each_pod_named() -> None:
    captured = _removal("pod-a1", "runner", "remove-pod")
    stopped = _removal("pod-a2", "runner", "emergency-stop")
    worker = _removal("worker-0", "worker", "stop-worker")
    assert pto.removals_recorded(_pod_logs())
    both = _pod_logs(before_deletion=[captured, worker], uncaptured=[stopped])
    assert pto.removals_recorded(both)
    # A pod a planned step missed is named uncaptured under that step.
    assert pto.removals_recorded(_pod_logs(uncaptured=[_removal("pod-a3", "runner", "fence")]))
    for broken in (
        {"sources": 2, "failures": 0},
        _pod_logs(uncaptured=None),
        _pod_logs(before_deletion={}),
        _pod_logs(before_deletion=[stopped]),
        _pod_logs(uncaptured=[{**stopped, "pod": ""}]),
        _pod_logs(uncaptured=[{**stopped, "pod": 7}]),
        _pod_logs(uncaptured=[{**stopped, "workload": "api"}]),
        _pod_logs(uncaptured=[{**stopped, "step": "teardown"}]),
        _pod_logs(uncaptured=["pod-a2"]),
        None,
    ):
        assert pto.removals_recorded(broken) is False, broken


def test_a_stop_path_removal_is_named_and_counted_without_failing_the_audit(
    tmp_path: Path,
) -> None:
    ledger = _complete(tmp_path)
    receipt = _pod_logs(
        before_deletion=[_removal("worker-0", "worker", "stop-worker")],
        uncaptured=[
            _removal("pod-a1", "runner", "emergency-stop"),
            _removal("worker-1", "worker", "emergency-fence"),
        ],
    )
    _set_capture(ledger, "pod_logs", receipt)
    result = pto.judge(ledger, "cleanup")
    assert result["verdict"] == "pass", result["checks"]
    assert result["checks"]["capture_audit_complete"] is True
    assert result["counts"]["pods_before_deletion"] == 1
    assert result["counts"]["pods_uncaptured"] == 2


def test_deleted_pods_reads_the_names_kubectl_delete_printed() -> None:
    printed = b"pod/pod-a1\npod/pod-b1\n\nclaim.x/claim-a1\npod/\npod/a/b\n"
    assert pto.deleted_pods(printed) == ["pod-a1", "pod-b1"]
    assert pto.deleted_pods(b"") == []


def test_trace_backend_follows_the_rendered_values() -> None:
    # The scenario's install keeps the chart's Langfuse, ClickHouse and collector.
    assert pto.trace_backend(_values()) == "in_cluster"
    assert pto.trace_backend({}) == "in_cluster"
    assert pto.trace_backend({"otelCollector": {"telemetryDisabled": True}}) == "absent"
    for values in (
        {"langfuse": {"deploy": False}},
        {"clickhouse": {"deploy": False}},
        {"otelCollector": {"deploy": False}},
        {"otelCollector": {"deploy": False, "endpoint": "https://otel.example.com:4318"}},
    ):
        assert pto.trace_backend(values) == "external", values


def test_trace_queries_name_only_the_trace_tables() -> None:
    assert [pto.trace_query(table) for table in pto.TRACE_TABLES] == [
        "SELECT * FROM traces FORMAT JSON",
        "SELECT * FROM observations FORMAT JSON",
        "SELECT * FROM scores FORMAT JSON",
    ]
    with pytest.raises(ValueError):
        pto.trace_query("system.users")
    # The ClickHouse login comes from the container's own environment, never argv.
    assert "$CLICKHOUSE_PASSWORD" in pto.CLICKHOUSE_EXEC and '"$1"' in pto.CLICKHOUSE_EXEC


def test_container_log_reads_cover_every_instance_that_ran() -> None:
    def status(name: str, state: dict[str, Any], **extra: Any) -> dict[str, Any]:
        return {"name": name, "state": state, "lastState": {}, "restartCount": 0, **extra}

    pods = {
        "items": [
            {
                "metadata": {"name": "api-1"},
                "status": {
                    "initContainerStatuses": [status("migrate", {"terminated": {"exitCode": 0}})],
                    "containerStatuses": [
                        status(
                            "api",
                            {"running": {"startedAt": "2027-01-15T08:00:00Z"}},
                            lastState={"terminated": {"exitCode": 1}},
                            restartCount=1,
                        ),
                        status(
                            "sidecar",
                            {"waiting": {"reason": "CrashLoopBackOff"}},
                            restartCount=3,
                        ),
                    ],
                },
            },
            {
                "metadata": {"name": "pending-1"},
                "status": {"containerStatuses": [status("web", {"waiting": {}})]},
            },
            {"metadata": {"name": "unscheduled-1"}, "status": {"phase": "Pending"}},
        ]
    }
    assert pto.container_log_reads(pods) == [
        ("api-1", "migrate", False),
        ("api-1", "api", False),
        ("api-1", "api", True),
        ("api-1", "sidecar", True),
    ]
    assert pto.container_log_reads(None) == []
    assert pto.container_log_reads({"items": "unreadable"}) == []


def _scan_entry(size: int = 10, found: bool = False) -> dict[str, Any]:
    return {"scanned_bytes": size, "token_bytes_found": found}


def test_scan_checks_require_bytes_receipts_and_traces_on_every_channel() -> None:
    scans = {name: _scan_entry() for name in pto.LIVE_SCAN_CHANNELS}
    receipts = {name: {**entry, "sources": 1} for name, entry in pto.capture_receipts().items()}
    passing = pto.scan_checks(scans, receipts, rehearsal=False, traces_backend="in_cluster")
    assert passing == {
        "token_scan_complete": True,
        "capture_audit_complete": True,
        "traces_audited": True,
        "token_bytes_absent": True,
    }
    # A channel initialized and never fed is not a scan.
    empty = {**scans, "events": _scan_entry(0)}
    verdict = pto.scan_checks(empty, receipts, rehearsal=False, traces_backend="in_cluster")
    assert verdict["token_scan_complete"] is False
    boolean = {**scans, "events": {"scanned_bytes": True, "token_bytes_found": False}}
    verdict = pto.scan_checks(boolean, receipts, rehearsal=False, traces_backend="in_cluster")
    assert verdict["token_scan_complete"] is False
    failed = {**receipts, "traces": {"sources": 3, "failures": 1}}
    verdict = pto.scan_checks(scans, failed, rehearsal=False, traces_backend="in_cluster")
    assert verdict["capture_audit_complete"] is False and verdict["traces_audited"] is False
    # Without traces, only an explicit absence passes.
    bare = {name: entry for name, entry in scans.items() if name != "traces"}
    unread = {name: entry for name, entry in receipts.items() if name != "traces"}
    absent = pto.scan_checks(bare, unread, rehearsal=False, traces_backend="absent")
    assert all(absent.values()), absent
    for backend in ("external", None, "in_cluster"):
        verdict = pto.scan_checks(bare, unread, rehearsal=False, traces_backend=backend)
        assert verdict["traces_audited"] is False, backend
    # A rehearsal has no harvest and no forge file; a live run must have both.
    rehearsal = {name: scans[name] for name in pto.REHEARSAL_SCAN_CHANNELS}
    assert pto.scan_checks(rehearsal, receipts, rehearsal=True, traces_backend="in_cluster")[
        "token_scan_complete"
    ]
    assert not pto.scan_checks(rehearsal, receipts, rehearsal=False, traces_backend="in_cluster")[
        "token_scan_complete"
    ]
    assert "harvest" not in pto.REHEARSAL_SCAN_CHANNELS
    assert set(pps.SCAN_CHANNELS) < set(pto.LIVE_SCAN_CHANNELS)


def test_card_residue_records_each_card_and_its_status() -> None:
    approval = {
        "drill_reject_card": {"delivered": True},
        "card1_card": {"delivered": False},
        "card1_delivered": True,
        "drill_reject_card_on_route_channel": True,
    }
    residue = pto.card_residue(
        {"drill_reject": "ref-1", "card1": "ref-2"}, approval, channel="ref-channel"
    )
    assert residue == {
        "channel": "ref-channel",
        "removed": False,
        "cards": {
            "card1": {"publication": "ref-2", "delivered": False, "residue": "not_delivered"},
            "drill_reject": {
                "publication": "ref-1",
                "delivered": True,
                "residue": "left_in_channel",
            },
        },
    }
    assert pto.delivered_cards(approval) == {"drill_reject"}
    assert pto.card_residue_recorded(residue, approval) is True
    # A delivered card the residue does not name, a claimed removal, a bare count.
    assert not pto.card_residue_recorded(residue, {**approval, "card2_card": {"delivered": True}})
    assert not pto.card_residue_recorded({**residue, "removed": True}, approval)
    assert not pto.card_residue_recorded(
        {"cards": 2, "channel": "ref-channel", "removed": False}, approval
    )
    assert not pto.card_residue_recorded({**residue, "channel": ""}, approval)


def test_context_versions_read_the_personal_context_shape() -> None:
    body = {
        "work_item_id": "w",
        "tenant_id": "t",
        "owner_principal_id": "p",
        "model_auth_mode": "personal",
        "enrollment_generation": 1,
        "work_item_version": 3,
        "request": {
            "id": "r",
            "sequence": 1,
            "status": "queued",
            "version": 5,
            "enrollment_generation": 1,
        },
        "enrollment": {"generation": 1, "active": True, "version": 2},
    }
    assert pto.context_versions(body) == {
        "request_id": "r",
        "expected_work_item_version": 3,
        "expected_request_version": 5,
        "expected_enrollment_version": 2,
    }
    binding = pto.context_binding(body)
    assert binding is not None and binding["owner_principal_id"] == "p"
    assert binding["enrollment_generation"] == 1 and binding["request_generation"] == 1
    for broken in (
        {**body, "request": None},
        {**body, "enrollment": None},
        {**body, "work_item_version": True},
        {**body, "request": {**body["request"], "id": ""}},
        [],
    ):
        assert pto.context_versions(broken) is None
        assert pto.context_binding(broken) is None


def test_a_disabled_trace_export_passes_on_its_recorded_absence(tmp_path: Path) -> None:
    ledger = _complete(tmp_path)
    ledger.record("cleanup", "traces_backend", "absent")
    _set_scan(ledger, "traces", None)
    _set_capture(ledger, "traces", None)
    result = pto.judge(ledger, "cleanup")
    assert result["verdict"] == "pass", result["checks"]
    assert result["checks"]["traces_audited"] is True


def test_a_rehearsal_needs_no_harvest_or_forge_scan_and_a_live_run_does(tmp_path: Path) -> None:
    for channel in ("harvest", "forge"):
        rehearsal = _ledger(tmp_path / f"rehearsal-{channel}", rehearsal=True)
        _record_complete(rehearsal)
        _set_scan(rehearsal, channel, None)
        assert pto.judge(rehearsal, "cleanup")["verdict"] == "pass"
        live = _complete(tmp_path / f"live-{channel}")
        _set_scan(live, channel, None)
        assert pto.judge(live, "cleanup")["checks"]["token_scan_complete"] is False


def test_rehearsal_ledger_passes_no_live_criterion(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, rehearsal=True)
    _record_complete(ledger)
    for key in pto.REHEARSAL_EXPECTED:
        ledger.record("rehearsal", key, 403 if key == "card_unlisted_status" else True)
    for criterion in pto.CRITERIA:
        result = pto.judge(ledger, criterion)
        if criterion == "cleanup":
            assert result["verdict"] == "pass"
        else:
            assert result == {
                "criterion": criterion,
                "verdict": "fail",
                "checks": {"live_ledger": False},
                "counts": {},
                "not_met": pto.NOT_MET.get(criterion, []),
                "observed": {},
            }
    rehearsal = pto.judge(ledger, "rehearsal")
    assert rehearsal["verdict"] == "pass" and rehearsal["checks"]["no_native_sessions"] is True
    every = pto.judge(ledger, "all")
    assert every["verdict"] == "fail" and every["checks"]["rehearsal.rehearsal_ledger"] is True
    ledger.record("rehearsal", "card_unlisted_status", True)
    assert pto.judge(ledger, "rehearsal")["verdict"] == "fail"


def test_rehearsal_summary_fails_on_a_live_ledger(tmp_path: Path) -> None:
    ledger = _complete(tmp_path)
    result = pto.judge(ledger, "rehearsal")
    assert result["verdict"] == "fail" and result["checks"]["rehearsal_ledger"] is False
    assert result["checks"]["no_native_sessions"] is False


def test_judge_refuses_an_unknown_criterion(tmp_path: Path) -> None:
    _refused("arguments_invalid", lambda: pto.judge(_complete(tmp_path), "pod-restart"))


def test_check_value_accepts_only_exact_values() -> None:
    assert pto.check_value(True, None) and not pto.check_value(1, None)
    assert not pto.check_value("True", None) and not pto.check_value(None, None)
    assert pto.check_value(2, ("==", 2)) and not pto.check_value(True, ("==", 1))
    assert pto.check_value(5, (">=", 2)) and pto.check_value(1, ("<=", 2))
    assert not pto.check_value(2.0, ("==", 2)) and not pto.check_value(2, ("!=", 2))


def test_every_criterion_has_expected_keys_and_meanings() -> None:
    assert set(pto.EXPECTED) == set(pto.CRITERIA)
    for criterion, keys in pto.EXPECTED.items():
        assert keys, criterion
        for key, meaning in keys.items():
            assert isinstance(meaning, str) and meaning and "\n" not in meaning, key
            assert "<owner>" not in meaning, key
    for criterion, rules in pto.COMPARISONS.items():
        assert set(rules) <= set(pto.EXPECTED[criterion])
    for owner in pto.OWNERS:
        for key in (
            "delivery_own_grant",
            "delivery_not_other",
            "claim_labels",
            "runner_images_candidate",
        ):
            assert f"{key}_{owner}" in pto.EXPECTED["owners"]
    for kind in ("pod", "worker", "process"):
        for key in (
            "lineage_before",
            "marker_before",
            "old_runtime_absent",
            "termination_epoch",
            "fresh_runtime",
            "marker_after",
            "committed_head_restored",
            "committed_files_restored",
        ):
            assert f"{kind}_{key}" in pto.EXPECTED["restarts"]


def test_role_checks_from_session_summaries() -> None:
    def summary(role: str, model: str, synthetic: int = 0) -> dict[str, Any]:
        return {"role": role, "models": [model], "assistant_turns": 3, "synthetic": synthetic}

    sessions = [
        summary(pto.IMPLEMENTER, pps.IMPLEMENTER_MODEL),
        summary(pto.PLAN_REVIEWER, pps.REVIEWER_MODEL),
        summary(pto.DIFF_REVIEWER, pps.REVIEWER_MODEL + "-20261101"),
    ]
    assert set(pto.role_checks(sessions).values()) == {True}
    wrong = pto.role_checks([*sessions[:2], summary(pto.DIFF_REVIEWER, pps.IMPLEMENTER_MODEL)])
    assert wrong["reviewer_model"] is False and wrong["diff_reviewer_session"] is True
    missing = pto.role_checks(sessions[:1])
    assert missing["plan_reviewer_session"] is False
    synthetic = pto.role_checks([*sessions, summary(pto.IMPLEMENTER, "<synthetic>", 1)])
    assert synthetic["no_synthetic_records"] is False
    assert pto.role_checks([])["no_synthetic_records"] is False


def test_ledger_rows_are_never_rewritten_by_judging(tmp_path: Path) -> None:
    ledger = _complete(tmp_path)
    before = ledger.db.execute("SELECT COUNT(*), SUM(output_tokens) FROM sessions").fetchone()
    pto.judge(ledger, "all")
    assert (
        ledger.db.execute("SELECT COUNT(*), SUM(output_tokens) FROM sessions").fetchone() == before
    )
    path = tmp_path / "state" / pps.LEDGER_FILE
    ledger.close()
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM session_groups").fetchone()[0] == 16


# --------------------------------------------------------------------------
# J. Live evidence readers
# --------------------------------------------------------------------------

PROJECT = "curie-claude-config-t1/projects/-workspace"


def _stamp(offset: float) -> str:
    import datetime as dt

    return dt.datetime.fromtimestamp(T0 + offset, dt.UTC).isoformat().replace("+00:00", "Z")


def _jsonl(records: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def _reply(message_id: str, at: float, tokens: int, content: list[Any]) -> dict[str, Any]:
    """An assistant record in the native CLI transcript shape."""

    return {
        "type": "assistant",
        "timestamp": _stamp(at),
        "message": {
            "id": message_id,
            "model": pps.IMPLEMENTER_MODEL,
            "role": "assistant",
            "content": content,
            "usage": {"input_tokens": 10, "output_tokens": tokens},
        },
    }


def _result(tool_id: str, at: float) -> dict[str, Any]:
    return {
        "type": "user",
        "timestamp": _stamp(at),
        "message": {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": tool_id, "content": "ok"}],
        },
    }


def _tool(tool_id: str, name: str = "Bash") -> dict[str, Any]:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": {}}


def test_hold_relpath_names_only_transcripts_under_the_runner_tmp() -> None:
    good = f"/tmp/{PROJECT}/session-1.jsonl"
    assert pto.hold_relpath(good) == f"{PROJECT}/session-1.jsonl"
    sub = f"/tmp/{PROJECT}/session-1/subagents/agent-x1.jsonl"
    assert pto.hold_relpath(sub) == f"{PROJECT}/session-1/subagents/agent-x1.jsonl"
    for bad in (
        None,
        f"/var/{PROJECT}/session-1.jsonl",
        f"/tmp/{PROJECT}/../session-1.jsonl",
        f"/tmp/{PROJECT}/session-1.json",
        "/tmp/other/projects/-workspace/session-1.jsonl",
        f"/tmp/{PROJECT}//session-1.jsonl",
    ):
        assert pto.hold_relpath(bad) is None


def test_probe_argv_uses_the_probe_model() -> None:
    argv = pto.probe_argv("/opt/cli/claude")
    assert argv[argv.index("--model") + 1] == pto.PROBE_MODEL
    assert argv[argv.index("--max-turns") + 1] == "1"
    assert "--no-session-persistence" in argv


def _probe_json(**body: Any) -> bytes:
    base: dict[str, Any] = {
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "num_turns": 1,
        "result": "ok",
        "total_cost_usd": 0.0001,
        "usage": {"output_tokens": 5},
        "modelUsage": {pto.PROBE_MODEL: {}},
    }
    base.update(body)
    return json.dumps(base).encode()


def test_probe_outcome_reads_turns_model_and_limits() -> None:
    passed = pto.probe_outcome(0, _probe_json())
    assert passed["ok"] and passed["on_probe_model"] and passed["within_limits"]
    assert passed["turns"] == 1 and passed["output_tokens"] == 5
    over = pto.probe_outcome(0, _probe_json(usage={"output_tokens": pto.PROBE_OUTPUT_TOKENS + 1}))
    assert over["within_limits"] is False
    turns = pto.probe_outcome(0, _probe_json(num_turns=2))
    assert turns["within_limits"] is False
    unread = pto.probe_outcome(0, _probe_json(usage={}))
    assert unread["within_limits"] is False and unread["output_tokens"] is None
    other = pto.probe_outcome(0, _probe_json(modelUsage={pps.IMPLEMENTER_MODEL: {}}))
    assert other["on_probe_model"] is False


def test_probe_outcome_classifies_a_usage_limit_refusal() -> None:
    refused = pto.probe_outcome(
        1,
        _probe_json(
            is_error=True,
            terminal_reason="api_error",
            api_error_status=429,
            result='API Error: 429 {"type":"error","error":{"type":"rate_limit_error"}}',
            usage={"output_tokens": 0},
            modelUsage={},
        ),
    )
    assert refused["ok"] is False
    assert pps.error_summary(refused["error"]) == "api_error 429 rate_limit_error"


def test_native_tool_calls_and_first_tool_after() -> None:
    main = f"{PROJECT}/session-1.jsonl"
    sub = f"{PROJECT}/session-1/subagents/agent-x1.jsonl"
    files = {
        main: _jsonl(
            [
                _reply("m1", 1, 10, [_tool("t1")]),
                _result("t1", 2),
                _reply("m2", 5, 10, [_tool("t2")]),
            ]
        ),
        sub: _jsonl([_reply("s1", 3, 10, [_tool("t3")]), _result("t3", 4)]),
    }
    calls = pto.native_tool_calls(files)
    assert [(call["id"], call["subagent"], call["result"]) for call in calls] == [
        ("t1", False, True),
        ("t3", True, True),
        ("t2", False, False),
    ]
    assert pto.first_tool_after(calls, T0) == T0 + 1
    # A subagent call or a call without a result never counts; nothing after is None.
    assert pto.first_tool_after(calls, T0 + 1) is None
    replayed = pto.native_tool_calls(files, exclude_ids=frozenset({"m1"}))
    assert [call["id"] for call in replayed] == ["t3", "t2"]
    assert pto.native_tool_calls(files, not_before=T0 + 4) == [
        {"id": "t2", "name": "Bash", "at": T0 + 5, "subagent": False, "result": False}
    ]


def test_claim_labels_match_needs_every_owner_label() -> None:
    labels = {
        pto.CLAIM_OWNER_LABELS["principal"]: "p-1",
        pto.CLAIM_OWNER_LABELS["tenant"]: "t-1",
        pto.CLAIM_OWNER_LABELS["generation"]: "1",
        pto.CLAIM_OWNER_LABELS["mode"]: pps.PERSONAL_MODE,
    }
    assert pto.claim_labels_match(labels, tenant_id="t-1", principal_id="p-1", generation=1)
    for name in pto.CLAIM_OWNER_LABELS.values():
        partial = {key: value for key, value in labels.items() if key != name}
        assert not pto.claim_labels_match(
            partial, tenant_id="t-1", principal_id="p-1", generation=1
        )
    assert not pto.claim_labels_match(labels, tenant_id="t-1", principal_id="p-2", generation=1)
    assert not pto.claim_labels_match(labels, tenant_id="t-1", principal_id="p-1", generation=2)
    assert not pto.claim_labels_match(None, tenant_id="t-1", principal_id="p-1", generation=1)


def test_marker_turn_is_a_turn_record() -> None:
    marker = "pto-a-synthetic"
    turn = pto.marker_turn(marker, "r" * 32, _stamp(0))
    assert turn["type"] == "turn" and turn["record_id"] == "r" * 32
    assert marker in turn["user"] and turn["status"] == "done"
    assert [item["role"] for item in turn["messages"]] == ["user", "assistant"]


def test_stream_has_event_reads_the_payload_field() -> None:
    event = "work-item-5d0c7f3e-0000-4000-8000-0000000c0001-ci-2"
    entries = [
        ("1-0", {pto.STREAM_PAYLOAD_FIELD: json.dumps({"event_id": "other"})}),
        ("2-0", {pto.STREAM_PAYLOAD_FIELD: "not json"}),
        ("3-0", {pto.STREAM_PAYLOAD_FIELD: json.dumps({"event_id": event})}),
    ]
    assert pto.stream_has_event(entries, event)
    assert not pto.stream_has_event(entries[:2], event)
    assert not pto.stream_has_event([("4-0", {"other": json.dumps({"event_id": event})})], event)


def _check_run(run_id: int, name: str, status: str, conclusion: str | None, app: str) -> Any:
    return {
        "id": run_id,
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "app": {"slug": app},
    }


def test_failing_actions_runs_and_rerun_count() -> None:
    runs = {
        "check_runs": [
            _check_run(1, "unit", "completed", "failure", pto.ACTIONS_APP),
            _check_run(2, "lint", "completed", "success", pto.ACTIONS_APP),
            _check_run(3, "other", "completed", "failure", "another-app"),
            _check_run(4, "unit", "in_progress", None, pto.ACTIONS_APP),
        ]
    }
    assert pto.failing_actions_runs(runs) == [{"id": 1, "name": "unit"}]
    assert pto.failing_actions_runs({"check_runs": runs["check_runs"][1:2]}) == []
    assert pto.rerun_count(runs, "unit") == (2, False)
    done = {"check_runs": [runs["check_runs"][0], {**runs["check_runs"][3], "status": "completed"}]}
    assert pto.rerun_count(done, "unit") == (2, True)
    assert pto.rerun_count(runs, "missing") == (0, False)


def test_annotations_name_the_ack_file() -> None:
    assert pto.annotations_name([{"path": pto.CI_ACK_FILE, "message": "x"}], pto.CI_ACK_FILE)
    assert pto.annotations_name([{"path": "t.py", "message": "CI_ACK.md must..."}], "CI_ACK.md")
    assert not pto.annotations_name([{"path": "t.py", "message": "other"}], pto.CI_ACK_FILE)
    assert not pto.annotations_name(None, pto.CI_ACK_FILE)


def test_required_check_names_from_rules_and_protection() -> None:
    rules = [
        {"type": "pull_request"},
        {
            "type": "required_status_checks",
            "parameters": {"required_status_checks": [{"context": "unit"}]},
        },
    ]
    branch = {"protection": {"required_status_checks": {"contexts": ["lint"], "checks": []}}}
    assert pto.required_check_names(rules, branch) == ["lint", "unit"]
    assert pto.required_check_names([], {}) == []


def test_checks_present_needs_a_success_for_every_name() -> None:
    runs = {"check_runs": [_check_run(1, "unit", "completed", "success", pto.ACTIONS_APP)]}
    statuses = {"statuses": [{"context": "lint", "state": "success"}]}
    assert pto.checks_present(["unit", "lint"], runs, statuses)
    pending = {"statuses": [{"context": "lint", "state": "pending"}]}
    assert not pto.checks_present(["unit", "lint"], runs, pending)
    assert not pto.checks_present(["unit", "missing"], runs, statuses)
    assert not pto.checks_present([], runs, statuses)


def test_ci_ack_present_names_the_branch() -> None:
    branch = "curie/publication-" + "0" * 32
    assert pto.ci_ack_present(f"# ack\nci ack: {branch}\n".encode(), branch)
    assert not pto.ci_ack_present(b"ci ack: curie/publication-other\n", branch)
    assert not pto.ci_ack_present(b"ci ack: \n", "")


def _facts(files: Mapping[str, bytes]) -> pps.TranscriptFacts:
    return pps.parse_native_transcripts(files)


def _container_pod(state: Mapping[str, Any], name: str = pps.RUNNER_CONTAINER) -> dict[str, Any]:
    """``kubectl get pod -o json`` with one container status in ``state``."""

    return {"status": {"containerStatuses": [{"name": name, "restartCount": 0, "state": state}]}}


def test_runner_started_at_reads_the_running_runner_container() -> None:
    running = {"running": {"startedAt": _stamp(-20)}}
    assert pto.runner_started_at(_container_pod(running)) == T0 - 20
    pod = _container_pod(running)
    pod["status"]["containerStatuses"].insert(
        0, {"name": "sidecar", "state": {"running": {"startedAt": _stamp(-90)}}}
    )
    assert pto.runner_started_at(pod) == T0 - 20
    for unread in (
        _container_pod({"waiting": {"reason": "ContainerCreating"}}),
        _container_pod({"running": {"startedAt": "soon"}}),
        _container_pod(running, name="sidecar"),
        {"status": {}},
        {},
        None,
    ):
        assert pto.runner_started_at(unread) is None, unread


def test_a_native_clock_starts_at_its_earliest_observation() -> None:
    assert pto.native_clock_start(monotonic=50.0, wall=T0, container_started=None) == 50.0
    # The runner container started 20 s before the driver first saw it.
    assert pto.native_clock_start(monotonic=50.0, wall=T0, container_started=T0 - 20) == 30.0
    # A container start reported after the observation moves nothing.
    assert pto.native_clock_start(monotonic=50.0, wall=T0, container_started=T0 + 5) == 50.0


def test_hold_usage_is_zero_only_at_the_initial_prompt_hold() -> None:
    main = f"{PROJECT}/session-1.jsonl"
    zero = pps.SessionUsage(0, 0, 0.0, "")
    assert pto.hold_usage(None, initial_prompt=True) == zero
    assert pto.hold_usage(None, initial_prompt=False) is None
    prompt_only = _facts(
        {main: _jsonl([{"type": "user", "timestamp": _stamp(0), "message": {"content": "go"}}])}
    )
    assert pto.hold_usage(prompt_only.sessions[0], initial_prompt=True) == zero
    assert pto.hold_usage(prompt_only.sessions[0], initial_prompt=False) is None
    replied = _facts({main: _jsonl([_reply("m1", 0, 42, [])])})
    for initial in (True, False):
        usage = pto.hold_usage(replied.sessions[0], initial_prompt=initial)
        assert usage is not None and usage.output_tokens == 42 and usage.assistant_turns == 1
    cut = _facts({main: _jsonl([_reply("m1", 0, 42, [])]) + b'{"type": "assist'})
    assert pto.hold_usage(cut.sessions[0], initial_prompt=False) is None
    # An API error after real usage: the failed request may have spent what is not shown.
    errored = _facts({main: _jsonl([_reply("m1", 0, 42, []), _refused_record(_stamp(5))])})
    (session,) = errored.sessions
    assert session.usage_known and session.errors
    for initial in (True, False):
        assert pto.hold_usage(session, initial_prompt=initial) is None


def test_zero_usage_class_keeps_the_error_class_or_the_planned_name() -> None:
    error = {"cli_error": "api_error", "http_status": 429, "provider_error_type": None}
    assert pto.zero_usage_class({"error_classes": [error]}) == "api_error 429"
    assert pto.zero_usage_class({"error_classes": [], "synthetic": 1}) is None
    assert pto.zero_usage_class({"error_classes": []}, planned="prompt_hold") == "prompt_hold"
    assert pto.zero_usage_class({"error_classes": []}) is None


# --------------------------------------------------------------------------
# K. The native-process interruption and per-claim grant delivery
# --------------------------------------------------------------------------

needs_proc = pytest.mark.skipif(
    not Path("/proc/self/stat").exists(), reason="reads /proc as the runner container does"
)
WITNESS = {"action": "signal_processes", "native_identity_matched": True, "native_gone": True}
UNMATCHED = {"action": "signal_processes", "native_identity_matched": False, "native_gone": False}


def _starttime(pid: int) -> int:
    """/proc stat field 22, as GATE_HOOK_PROGRAM records ``cli_start``."""

    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])


@contextlib.contextmanager
def _sleepers(count: int) -> Iterator[list[subprocess.Popen[bytes]]]:
    """Harmless stand-ins blocked on stdin; an EOF ends each one with status 0."""

    processes = [
        subprocess.Popen(
            [sys.executable, "-I", "-c", "import sys; sys.stdin.read()"], stdin=subprocess.PIPE
        )
        for _ in range(count)
    ]
    try:
        yield processes
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=30)
            if process.stdin is not None:
                process.stdin.close()


def _ended(process: subprocess.Popen[bytes]) -> int:
    """Close the stand-in's stdin and reap it: 0 means no signal ever reached it."""

    assert process.stdin is not None
    process.stdin.close()
    return process.wait(timeout=30)


def _interrupt(target: int, start: int | str, runner: int) -> dict[str, Any] | None:
    """INTERRUPT_PROGRAM as a real process; ``runner`` always stands in for PID 1."""

    result = subprocess.run(
        [sys.executable, "-I", "-c", pto.INTERRUPT_PROGRAM, str(target), str(start)],
        env={"PATH": os.environ.get("PATH", ""), pto.INTERRUPT_RUNNER_ENV: str(runner)},
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return pto.interrupt_witness(result.stdout)


def test_interrupt_program_signals_only_the_held_cli_and_the_runner() -> None:
    compile(pto.INTERRUPT_PROGRAM, "interrupt", "exec")
    assert "@" not in pto.INTERRUPT_PROGRAM
    tree = ast.parse(pto.INTERRUPT_PROGRAM)
    senders = {"kill", "killpg", "pidfd_send_signal", "raise_signal", "pthread_kill"}
    signals = sorted(
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and ast.unparse(node.func).split(".")[-1] in senders
    )
    # Two signals in all: SIGKILL to the matched target, SIGTERM to the runner.
    assert signals == ["os.kill(RUNNER, signal.SIGTERM)", "os.kill(target, signal.SIGKILL)"]
    assert "listdir" not in pto.INTERRUPT_PROGRAM


@needs_proc
def test_interrupt_program_kills_the_matched_cli_then_terms_the_runner() -> None:
    with _sleepers(3) as (cli, runner, bystander):
        witness = _interrupt(cli.pid, _starttime(cli.pid), runner.pid)
        # The killed CLI is this test's unreaped child, so the program saw a zombie.
        assert witness == WITNESS
        assert cli.wait(timeout=30) == -signal.SIGKILL
        assert _ended(runner) == -signal.SIGTERM
        assert _ended(bystander) == 0


@needs_proc
def test_interrupt_program_signals_nothing_without_an_identity_match() -> None:
    with _sleepers(2) as (cli, runner):
        start = _starttime(cli.pid)
        for target, ticks in (
            (cli.pid, start + 1),
            (cli.pid, "not-a-tick"),
            (runner.pid, _starttime(runner.pid)),
        ):
            assert _interrupt(target, ticks, runner.pid) == UNMATCHED, (target, ticks)
        assert _ended(cli) == 0 and _ended(runner) == 0


def test_interrupt_witness_reads_exactly_one_witness_line() -> None:
    line = json.dumps(WITNESS).encode()
    assert pto.interrupt_witness(line + b"\n") == WITNESS
    assert pto.interrupt_witness(b"\n" + json.dumps(UNMATCHED).encode() + b"\n") == UNMATCHED
    for bad in (
        b"",
        line + b"\n" + line,
        b"not json",
        b"[]",
        json.dumps({**WITNESS, "extra": 1}).encode(),
        json.dumps({**WITNESS, "action": "delete_pod"}).encode(),
        json.dumps({**WITNESS, "native_gone": "true"}).encode(),
        json.dumps({**UNMATCHED, "native_gone": True}).encode(),
        json.dumps({key: WITNESS[key] for key in ("action", "native_gone")}).encode(),
    ):
        assert pto.interrupt_witness(bad) is None, bad


def _tokens_secret(
    value: str | None, *, name: str = "claim-a-tokens", key: str = pto.GRANT_KEY
) -> bytes:
    """A ``<claim>-tokens`` Secret as ``kubectl get secret -o json`` prints it."""

    data = {} if value is None else {key: base64.b64encode(value.encode()).decode()}
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name}, "data": data}
    return json.dumps({**secret, "type": "Opaque"}).encode()


def test_claim_grant_delivery_compares_digests_only() -> None:
    own, other = pto.grant_digest(GRANT_A), pto.grant_digest(GRANT_B)
    check = functools.partial(
        pto.claim_grant_delivery, secret="claim-a-tokens", own=own, other=other
    )
    refused = {"own_grant": False, "not_other": False}
    assert check(_tokens_secret(GRANT_A)) == {"own_grant": True, "not_other": True}
    assert check(_tokens_secret(GRANT_B)) == refused
    marker = "synthetic-unrelated-marker"
    assert check(_tokens_secret(marker)) == {"own_grant": False, "not_other": True}
    invalid = {"metadata": {"name": "claim-a-tokens"}, "data": {pto.GRANT_KEY: "!!not base64"}}
    for document in (
        _tokens_secret(None),
        _tokens_secret(""),
        _tokens_secret(GRANT_A, key="token"),
        _tokens_secret(GRANT_A, name="claim-b-tokens"),
        json.dumps(invalid).encode(),
        b"{",
        b"[]",
    ):
        assert check(document) == refused, document
    # Equal digests prove nothing about "not the other owner's".
    same = pto.claim_grant_delivery(
        _tokens_secret(GRANT_A), secret="claim-a-tokens", own=own, other=own
    )
    assert same == refused


def _runner_pod(env: list[dict[str, Any]]) -> dict[str, Any]:
    containers = [{"name": "runner", "env": env}, {"name": "sidecar", "env": []}]
    return {"metadata": {"name": "pod-a"}, "spec": {"containers": containers}}


def test_claim_grant_ref_needs_exactly_the_claim_secret_and_key() -> None:
    def ref(**source: Any) -> dict[str, Any]:
        selector = {"name": "claim-a-tokens", "key": pto.GRANT_KEY, **source}
        return {"name": pto.GRANT_KEY, "valueFrom": {"secretKeyRef": selector}}

    plain = {"name": "CURIE_SETTING", "value": "marker"}
    assert pto.claim_grant_ref(
        {"items": [_runner_pod([ref()]), _runner_pod([plain, ref()])]}, "claim-a-tokens"
    )
    literal = {"name": pto.GRANT_KEY, "value": "marker"}
    config = {"name": pto.GRANT_KEY, "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}
    both = {**ref(), "value": "marker"}
    cases: list[dict[str, Any]] = [
        {},
        {"items": []},
        {"items": [_runner_pod([])]},
        {"items": [_runner_pod([ref(), ref()])]},
        {"items": [_runner_pod([ref(name="claim-b-tokens")])]},
        {"items": [_runner_pod([ref(key="token")])]},
        {"items": [_runner_pod([ref(optional=True)])]},
        {"items": [_runner_pod([literal])]},
        {"items": [_runner_pod([config])]},
        {"items": [_runner_pod([both])]},
        {"items": [_runner_pod([ref()]), {"spec": {"containers": []}}]},
    ]
    for pods in cases:
        assert not pto.claim_grant_ref(pods, "claim-a-tokens"), pods


ENROLLMENT = uuid.UUID("5d0c7f3e-0000-4000-8000-0000000e0a01")
GRANT_NAMESPACE = "pto-acme-1-grants"
CUSTODY_NAME = f"curie-pe-{ENROLLMENT.hex}-g1"


def _custody_row(**changes: Any) -> dict[str, Any]:
    """One joined custody row as ``sql_rows`` returns it (row_to_json shapes)."""

    row = {
        "enrollment_id": str(ENROLLMENT),
        "generation": 1,
        "state": "published",
        "secret_namespace": GRANT_NAMESPACE,
        "secret_name": CUSTODY_NAME,
        "secret_key": pto.GRANT_KEY,
        "secret_uid": "uid-custody-a",
        "header_generation": 1,
        "active": True,
    }
    return {**row, **changes}


def test_custody_secret_matches_the_published_row_name_uid_and_labels() -> None:
    assert ".data" not in pto.CUSTODY_METADATA_JSONPATH
    listed = f"{CUSTODY_NAME}|uid-custody-a|curie|true|{ENROLLMENT.hex}|1".encode()
    row = _custody_row()
    assert pto.custody_row_secret(row, namespace=GRANT_NAMESPACE) == CUSTODY_NAME
    assert pto.custody_secret_matches(row, listed, namespace=GRANT_NAMESPACE)
    for changes in (
        {"state": "retiring"},
        {"active": False},
        {"header_generation": 2},
        {"secret_namespace": "pto-acme-1"},
        {"secret_name": "curie-pe-other-g1"},
        {"secret_key": "token"},
        {"secret_uid": None},
        {"secret_uid": " "},
        {"enrollment_id": "not-a-uuid"},
        {"generation": True},
    ):
        changed = _custody_row(**changes)
        assert pto.custody_row_secret(changed, namespace=GRANT_NAMESPACE) is None, changes
        assert not pto.custody_secret_matches(changed, listed, namespace=GRANT_NAMESPACE)
    for line in (
        b"",
        listed.replace(b"uid-custody-a", b"uid-other"),
        listed.replace(b"|curie|", b"|helm|"),
        listed.replace(b"|true|", b"||"),
        listed[: -len(b"|1")] + b"|2",
        listed + b"\n" + listed,
    ):
        assert not pto.custody_secret_matches(row, line, namespace=GRANT_NAMESPACE), line


# --------------------------------------------------------------------------
# L. Recovery evidence: the workspace, the replayed history and the epoch
# --------------------------------------------------------------------------

#: The PATH WORKSPACE_PROGRAM gives git, as in the runner image.
PROGRAM_PATH = "/usr/local/bin:/usr/bin:/bin"
needs_git = pytest.mark.skipif(
    shutil.which("git", path=PROGRAM_PATH) is None, reason="runs git as the runner image does"
)
NONCE = "acme-nonce-0123456789"
HEAD = "c" * 40


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git(repo: Path, *args: str) -> str:
    env = {"PATH": PROGRAM_PATH, "HOME": str(repo.parent), "GIT_CONFIG_NOSYSTEM": "1"}
    identity = ["-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid"]
    done = subprocess.run(
        ["git", *identity, "-C", str(repo), *args],
        capture_output=True,
        env=env,
        timeout=60,
        check=True,
    )
    return done.stdout.decode().strip()


def _read_workspace(repo: Path) -> dict[str, Any] | None:
    done = subprocess.run(
        [sys.executable, "-I", "-c", pto.WORKSPACE_PROGRAM, str(repo), NONCE],
        capture_output=True,
        timeout=60,
        check=True,
    )
    return pto.workspace_state(done.stdout)


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


@needs_git
def test_workspace_program_reads_a_real_checkout_and_writes_nothing(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    readme, core = b"acme fixture\n", b"def convert() -> int:\n    return 1\n"
    (repo / "README.md").write_bytes(readme)
    (repo / "unitconv").mkdir()
    (repo / "unitconv" / "core.py").write_bytes(core)
    (repo / "link").symlink_to("README.md")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "fixture")
    head = _git(repo, "rev-parse", "HEAD")
    committed = {"README.md": _digest(readme), "link": _digest(b"README.md")}
    committed["unitconv/core.py"] = _digest(core)
    assert _read_workspace(repo) == {
        "head": head,
        "files": committed,
        "committed": committed,
        "status": [],
        "uncommitted": {},
        "nonce_paths": [],
    }
    # A staged rename, an unstaged edit carrying the nonce and an untracked file.
    _git(repo, "mv", "README.md", "DOCS.md")
    edited = core + f"# {NONCE}\n".encode()
    (repo / "unitconv" / "core.py").write_bytes(edited)
    (repo / "notes").mkdir()
    (repo / "notes" / "NONCE.md").write_bytes(f"nonce {NONCE}\n".encode())
    before = _tree_bytes(repo)
    state = _read_workspace(repo)
    assert _tree_bytes(repo) == before, "the program wrote into the workspace"
    assert state is not None and state["head"] == head
    # HEAD's tree is unchanged; the tracked files are the index's.
    assert state["committed"] == committed
    assert state["files"] == {
        "DOCS.md": _digest(readme),
        "link": _digest(b"README.md"),
        "unitconv/core.py": _digest(edited),
    }
    # Both sides of the rename are named, however this git reports it.
    named = {"DOCS.md", "README.md", "notes/NONCE.md", "unitconv/core.py"}
    assert pto.changed_paths(state) == named
    assert state["uncommitted"] == {
        "DOCS.md": _digest(readme),
        "README.md": None,
        "notes/NONCE.md": _digest(f"nonce {NONCE}\n".encode()),
        "unitconv/core.py": _digest(edited),
    }
    assert state["nonce_paths"] == ["notes/NONCE.md", "unitconv/core.py"]
    source = {"head": head, "files": committed}
    # The committed state holds under the edits, which are only observed (#5541).
    clean = pto.workspace_state(json.dumps({**state, "status": [], "uncommitted": {}}).encode())
    assert pto.workspace_restoration(source, state, state) == {
        "committed_head_restored": True,
        "committed_files_restored": True,
    }
    assert pto.uncommitted_restoration(state, clean) == "not_restored"


@needs_git
def test_workspace_program_outside_a_repository_reads_no_head(tmp_path: Path) -> None:
    # A .git file naming no repository stops git's search in the parents.
    (tmp_path / ".git").write_text("gitdir: missing\n")
    state = _read_workspace(tmp_path)
    assert state == {
        "head": None,
        "files": {},
        "committed": {},
        "status": None,
        "uncommitted": {},
        "nonce_paths": [],
    }
    source = {"head": HEAD, "files": {"README.md": _digest(b"x")}}
    assert set(pto.workspace_restoration(source, state, state).values()) == {False}


def _state(**changes: Any) -> dict[str, Any]:
    files = {"README.md": _digest(b"readme"), "unitconv/core.py": _digest(b"core")}
    state = {
        "head": HEAD,
        "files": dict(files),
        "committed": dict(files),
        "status": [],
        "uncommitted": {},
        "nonce_paths": [],
    }
    return {**state, **changes}


def _line(value: Any) -> bytes:
    return json.dumps(value).encode() + b"\n"


def test_workspace_state_reads_exactly_the_program_line() -> None:
    assert pto.workspace_state(_line(_state())) == _state()
    assert pto.workspace_state(b"\n" + _line(_state()) + b"\n") == _state()
    unread = _state(head=None, files={}, committed={}, status=None)
    assert pto.workspace_state(_line(unread)) == unread
    for value in (
        b"",
        b"not json\n",
        _line(_state()) * 2,
        _line([_state()]),
        _line({**_state(), "extra": 1}),
        _line({key: value for key, value in _state().items() if key != "nonce_paths"}),
        _line(_state(head="main")),
        _line(_state(head="C" * 40)),
        _line(_state(files={"README.md": "abc"})),
        _line(_state(committed={"": _digest(b"x")})),
        _line(_state(uncommitted=[])),
        _line(_state(status=[["??"]])),
        _line(_state(status=[["??", ""]])),
        _line(_state(status="clean")),
        _line(_state(nonce_paths="NONCE.md")),
    ):
        assert pto.workspace_state(value) is None, value


def test_workspace_restoration_holds_the_forge_head_and_blobs_on_both_sides() -> None:
    source = {"head": HEAD, "files": _state()["committed"]}
    good = {"committed_head_restored": True, "committed_files_restored": True}
    assert pto.workspace_restoration(source, _state(), _state()) == good
    # The worker stop: no runner existed, so only the fresh checkout is read.
    assert pto.workspace_restoration(source, None, _state()) == good
    # An uncommitted edit is observed, never counted for or against.
    edited = _state(
        files={**_state()["files"], "unitconv/core.py": _digest(b"edit")},
        status=[[" M", "unitconv/core.py"], ["??", "NONCE.md"]],
        uncommitted={"unitconv/core.py": _digest(b"edit"), "NONCE.md": _digest(b"nonce")},
    )
    assert pto.workspace_restoration(source, edited, edited) == good
    other = "d" * 40
    tracked = _state()["files"]
    # (source, before, after, HEAD restored, committed files restored)
    cases: list[tuple[Any, Any, Any, bool, bool]] = [
        (source, _state(), None, False, False),
        (source, _state(head=other), _state(), False, True),
        (source, _state(), _state(head=other), False, True),
        (source, _state(committed={}), _state(), True, False),
        (source, _state(), _state(committed={"README.md": _digest(b"readme")}), True, False),
        # A tracked file off its committed bytes that the status does not name.
        (source, _state(), _state(files={**tracked, "README.md": _digest(b"x")}), True, False),
        # A tracked path the forge does not hold and the status does not name.
        (source, _state(), _state(files={**tracked, "extra.py": _digest(b"x")}), True, False),
        (source, _state(), _state(status=None), True, False),
        # An unreadable forge source compares equal to nothing.
        ({"head": None, "files": {}}, _state(), _state(), False, False),
    ]
    for case_source, before, after, head_ok, files_ok in cases:
        result = pto.workspace_restoration(case_source, before, after)
        assert result == {
            "committed_head_restored": head_ok,
            "committed_files_restored": files_ok,
        }, (before, after)


def test_uncommitted_restoration_is_an_observation_of_5541() -> None:
    edits = {"NONCE.md": _digest(b"nonce")}
    with_edits = _state(status=[["??", "NONCE.md"]], uncommitted=edits)
    assert pto.uncommitted_restoration(None, _state()) == "none_before"
    assert pto.uncommitted_restoration(_state(), _state()) == "none_before"
    assert pto.uncommitted_restoration(with_edits, with_edits) == "restored"
    assert pto.uncommitted_restoration(with_edits, _state()) == "not_restored"
    assert pto.uncommitted_restoration(with_edits, None) == "not_restored"
    other = _state(uncommitted={"NONCE.md": _digest(b"other")})
    assert pto.uncommitted_restoration(with_edits, other) == "not_restored"


def test_forge_tree_paths_needs_the_whole_recursive_tree() -> None:
    # GET /repos/{repo}/git/trees/{sha}?recursive=1, GitHub's documented shape.
    tree = {
        "sha": HEAD,
        "url": "https://api.github.com/repos/acme-corp/acme-fixture/git/trees/" + HEAD,
        "truncated": False,
        "tree": [
            {"path": "unitconv", "mode": "040000", "type": "tree", "sha": "a" * 40},
            {"path": "unitconv/core.py", "mode": "100644", "type": "blob", "sha": "b" * 40},
            {"path": "README.md", "mode": "100644", "type": "blob", "sha": "d" * 40},
            {"path": "vendor/lib", "mode": "160000", "type": "commit", "sha": "e" * 40},
        ],
    }
    assert pto.forge_tree_paths(tree) == ["README.md", "unitconv/core.py"]
    for broken in (
        {**tree, "truncated": True},
        {key: value for key, value in tree.items() if key != "truncated"},
        {**tree, "tree": None},
        {**tree, "tree": [*tree["tree"], {"type": "blob"}]},
        {**tree, "tree": tree["tree"][:1]},
        [tree],
        None,
    ):
        assert pto.forge_tree_paths(broken) is None, broken


def test_marker_replayed_reads_only_main_native_transcripts() -> None:
    marker = "pto-b-0123456789ab"
    record = json.dumps({"type": "user", "message": {"content": f"Remember {marker}."}})
    main = "curie-claude-config-t2/projects/-workspace/session-2.jsonl"
    agent = "curie-claude-config-t2/projects/-workspace/session-2/subagents/agent-1.jsonl"
    assert pto.marker_replayed({main: record.encode()}, marker)
    assert not pto.marker_replayed({main: record.encode()}, "")
    assert not pto.marker_replayed({main: b"{}\n"}, marker)
    assert not pto.marker_replayed({agent: record.encode()}, marker)
    assert not pto.marker_replayed({main.removesuffix(".jsonl") + ".txt": record.encode()}, marker)
    assert not pto.marker_replayed({}, marker)


WORK = "5d0c7f3e-0000-4000-8000-0000000c0001"
OLD_ROW = {
    "id": "5d0c7f3e-0000-4000-8000-0000000d0a01",
    "work_item_id": WORK,
    "sequence": 1,
    "status": "failed",
    "terminal_cause": "publication_denied",
    "started": True,
    "runtime_epoch": 2,
}
NEW_ROW = {
    "id": "5d0c7f3e-0000-4000-8000-0000000d0a02",
    "work_item_id": WORK,
    "sequence": 2,
    "status": "running",
    "started": True,
    "runtime_claim_name": "claim-a2",
    "runtime_epoch": 1,
    "runtime_owner": "curie-worker-1-7",
}


def test_successor_epoch_confirmed_needs_a_successor_bound_on_the_fresh_claim() -> None:
    def confirmed(old: Mapping[str, Any], new: Mapping[str, Any], held: Any = 2) -> bool:
        return pto.successor_epoch_confirmed(old, new, held_epoch=held, claim="claim-a2")

    assert confirmed(OLD_ROW, NEW_ROW)
    assert confirmed({**OLD_ROW, "status": "completed", "runtime_epoch": 3}, NEW_ROW)
    # A terminal old request alone never confirms it: no successor bound.
    waiting = {**NEW_ROW, "status": "waiting", "started": False, "runtime_claim_name": None}
    assert not confirmed(OLD_ROW, waiting)
    for old in (
        {**OLD_ROW, "status": status}
        for status in ("waiting", "running", "cancellation_requested", "queued", None)
    ):
        assert not confirmed(old, NEW_ROW), old
    for changes in ({"started": False}, {"runtime_epoch": 1}, {"runtime_epoch": None}):
        assert not confirmed({**OLD_ROW, **changes}, NEW_ROW), changes
    for changes in (
        {"id": OLD_ROW["id"]},
        {"work_item_id": "5d0c7f3e-0000-4000-8000-0000000c0002"},
        {"sequence": 1},
        {"sequence": None},
        {"started": False},
        {"runtime_claim_name": "claim-a1"},
        {"runtime_epoch": 0},
        {"runtime_epoch": True},
        {"runtime_owner": None},
        {"runtime_owner": ""},
    ):
        assert not confirmed(OLD_ROW, {**NEW_ROW, **changes}), changes
    for held in (None, 0, True, "2"):
        assert not confirmed(OLD_ROW, NEW_ROW, held), held
    assert not pto.successor_epoch_confirmed(OLD_ROW, NEW_ROW, held_epoch=2, claim="")


def test_current_epoch_confirmed_needs_the_same_running_request_at_the_held_epoch() -> None:
    row = {"id": NEW_ROW["id"], "status": "running", "runtime_epoch": 2}

    def confirmed(value: Mapping[str, Any], held: Any = 2, request: str = NEW_ROW["id"]) -> bool:
        return pto.current_epoch_confirmed(value, request_id=request, held_epoch=held)

    assert confirmed(row)
    # A termination claim or an owner-lost successor moves the epoch on.
    assert not confirmed({**row, "runtime_epoch": 3})
    assert not confirmed({**row, "status": "failed"})
    assert not confirmed({**row, "id": OLD_ROW["id"]})
    assert not confirmed(row, request="")
    for held in (None, 0, True):
        assert not confirmed({**row, "runtime_epoch": held}, held), held


def test_restart_observations_are_reported_and_never_required(tmp_path: Path) -> None:
    observations = pto.OBSERVATIONS["restarts"]
    assert not set(observations) & set(pto.EXPECTED["restarts"])
    for key, meaning in observations.items():
        assert isinstance(meaning, str) and meaning and "\n" not in meaning, key
    for kind in ("pod", "process"):
        assert "#5541" in observations[f"{kind}_uncommitted_restored"]
    assert "worker_uncommitted_restored" not in observations
    assert any("#5541" in line for line in pto.NOT_MET["restarts"])
    ledger = _complete(tmp_path)
    recorded = {
        "process_recovery": "new_pod",
        "process_same_pod": False,
        "process_container_restarted": False,
        "process_uncommitted_restored": "none_before",
        "pod_uncommitted_restored": "not_restored",
        "pod_termination_recorded": False,
        "worker_runtime_owner_moved": False,
        "worker_git_status_after": [],
    }
    for key, value in recorded.items():
        ledger.record("restarts", key, value)
    ledger.record("restarts", "unlisted_key", True)
    result = pto.judge(ledger, "restarts")
    assert result["verdict"] == "pass" and result["observed"] == recorded
    assert not set(recorded) & set(result["checks"])
    every = pto.judge(ledger, "all")
    assert every["verdict"] == "pass"
    assert every["observed"] == {f"restarts.{key}": value for key, value in recorded.items()}

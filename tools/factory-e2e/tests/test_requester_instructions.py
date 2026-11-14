"""Hermetic contract for the requester coding instructions proof driver (#5592).

The rehearsal needs a throwaway cluster and the live run needs the fixture
repository, the second test account and the live model endpoint, so both are
proved by running ``requester_instructions.py``. These tests pin only the pure
parts: argument parsing, preflight composition (a missing prerequisite is a
refusal, never a skip), the ledger's slot bookkeeping, the spend caps, the two
programs that run outside the driver (each fails closed on bad input, a crash
and its internal deadline), the per-slot assertions over recorded evidence, the
forge cleanup bookkeeping and the judge, where a proof that did not run is
missing, never passed. Nothing here touches a cluster, Docker or a network
beyond a loopback socket, and every value is an obvious synthetic sentinel.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import personal_owner_enforcement as pod
import personal_owner_enforcement_support as poe
import personal_profile_support as pps
import pytest
import requester_instructions as driver
import requester_instructions_support as ris
from curie_internal.requester_coding_instructions import text_sha256

CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
TOKEN = "synthetic_user_token_sentinel_not_a_token_0001"
T0 = 1_800_000_000.0
NONCE = "a1b2c3d4"
KUBE_CONTEXT = "throwaway"


# --------------------------------------------------------------------------
# Helpers built in the test body
# --------------------------------------------------------------------------


def _private(path: Path, text: str, mode: int = 0o600) -> Path:
    path.write_text(text)
    path.chmod(mode)
    return path


def _state(tmp_path: Path) -> Path:
    state = tmp_path / "state"
    state.mkdir(parents=True)
    state.chmod(0o700)
    return state


def _app_dir(tmp_path: Path, **overrides: Any) -> Path:
    directory = tmp_path / "app"
    directory.mkdir(exist_ok=True)
    directory.chmod(0o700)
    body = {"id": "12345", "installation_id": "67890", "repo": "acme-corp/acme-fixture"}
    body.update(overrides)
    _private(directory / "app.json", json.dumps(body))
    _private(directory / "app.pem", "synthetic pem sentinel\n")
    return directory


def _conflict(tmp_path: Path, **overrides: Any) -> Path:
    body = {
        "preference": "Name every new Python module in camelCase.",
        "forbidden_added_line_regex": r"camelCase",
        "rule_path": "AGENTS.md",
    }
    body.update(overrides)
    return _private(tmp_path / "conflict.json", json.dumps(body))


def _ledger(tmp_path: Path, mode: str = "live", run_id: str = "run-1") -> ris.RequesterLedger:
    state = tmp_path / "state" if (tmp_path / "state").exists() else _state(tmp_path)
    ledger = ris.RequesterLedger.open(state, run_id, create=True)
    ledger.begin(CANDIDATE, mode, T0)
    return ledger


def _run(program: str, *argv: str, stdin: bytes = b"", timeout: float = 30) -> tuple[int, bytes]:
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, *argv],
        input=stdin,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    return result.returncode, result.stdout


def _passing_record(criterion: str, **overrides: Any) -> ris.RunRecord:
    mode = ris.PROOF_MODE[criterion]
    observations: dict[str, dict[str, Any]] = {name: {} for name in ris.CRITERIA}
    observations[criterion] = dict(ris.EXPECTED[criterion])
    cleanup = [
        {"kind": kind, "ref": "x", "status": "absent", "detail": ""}
        for kind in ris.CLEANUP_KINDS[criterion]
    ]
    cleanup.append({"kind": "step", "ref": "fence", "status": "ok", "detail": ""})
    slots = (
        [
            {
                "slot": n,
                "actor": "owner",
                "issue": str(n),
                "recorded_at": T0 + n,
                "labeled_at": T0 + n + 1,
                "launches": 2,
            }
            for n in (1, 2, 3)
        ]
        if criterion == "10"
        else []
    )
    fields: dict[str, Any] = {
        "run_id": f"{mode}-1",
        "candidate": CANDIDATE,
        "mode": mode,
        "outcome": "completed",
        "observations": observations,
        "cleanup": cleanup,
        "slots": slots,
    }
    fields.update(overrides)
    return ris.RunRecord(**fields)


def _evidence(slot: int, document: ris.Document, **overrides: Any) -> ris.SlotEvidence:
    marker = [] if slot == 3 else [f"+{document.marker}"]
    files = ({"filename": "notes/a.md", "patch": "\n".join(["+one sentence", *marker])},)
    body = "Implements the issue."
    if slot != 3:
        body += "\n\n" + ris.expected_annotation(document.sha256)
    fields: dict[str, Any] = {
        "recorded_before_label": True,
        "requester_matches": True,
        "evidence": "none" if slot == 3 else "applied",
        "pull_request_found": True,
        "body": body,
        "files": files,
        "reviewer": ris.ReviewerScan(files=1, records=4, hits=0, error=None),
    }
    fields.update(overrides)
    return ris.SlotEvidence(**fields)


# --------------------------------------------------------------------------
# Arguments and preflight composition
# --------------------------------------------------------------------------


def _common(tmp_path: Path) -> list[str]:
    return [
        "--candidate",
        CANDIDATE,
        "--context",
        KUBE_CONTEXT,
        "--namespace",
        "rqp-test",
        "--docker-context",
        "scratch",
        "--registry",
        "127.0.0.1:5000",
        "--input-file",
        str(tmp_path / "input.json"),
        "--state-dir",
        str(tmp_path / "state"),
        "--run-id",
        "run-1",
    ]


def test_live_flags_are_required_for_preflight_and_run_only(tmp_path: Path) -> None:
    parser = driver.build_parser()
    rehearse = parser.parse_args(["rehearse", *_common(tmp_path)])
    assert rehearse.app_dir == "" and rehearse.slot_seconds == driver.DEFAULT_SLOT_SECONDS
    for command in ("preflight", "run"):
        with pytest.raises(SystemExit):
            parser.parse_args([command, *_common(tmp_path)])
    live = [
        "--app-dir",
        "/a",
        "--owner-token-file",
        "/o",
        "--second-token-file",
        "/s",
        "--model-credential-file",
        "/m",
        "--conflict-file",
        "/c",
        "--priority-class",
        "proof-critical",
    ]
    args = parser.parse_args(["run", *_common(tmp_path), *live])
    assert args.second_token_file == "/s" and args.priority_class == "proof-critical"
    forwarded = driver.forwarded(args)
    assert forwarded[forwarded.index("--conflict-file") + 1] == "/c"
    assert "--grant-file" not in forwarded


def test_judge_takes_repeatable_run_ids_and_only_known_criteria() -> None:
    parser = driver.build_parser()
    args = parser.parse_args(
        ["judge", "--state-dir", "/s", "--run-id", "a", "--run-id", "b", "--criterion", "all"]
    )
    assert args.run_id == ["a", "b"]
    with pytest.raises(SystemExit):
        parser.parse_args(["judge", "--state-dir", "/s", "--run-id", "a", "--criterion", "11"])


def test_preflight_plan_runs_live_input_checks_only_for_live_commands() -> None:
    assert ris.preflight_plan("rehearse") == ris.BASE_PREFLIGHT
    for command in ("preflight", "run"):
        plan = ris.preflight_plan(command)
        assert plan[: len(ris.BASE_PREFLIGHT)] == ris.BASE_PREFLIGHT
        assert {"second_token_file", "priority_class"} <= set(plan)
    # The live model and App probes run immediately before the start, never here.
    assert not set(ris.LIVE_START_PROBES) & set(ris.preflight_plan("run"))
    with pytest.raises(ValueError):
        ris.preflight_plan("judge")


def _live_args(tmp_path: Path, **overrides: str) -> Any:
    owner = _private(tmp_path / "owner-token", TOKEN + "\n")
    second = _private(tmp_path / "second-token", TOKEN + "\n")
    model = _private(
        tmp_path / "model.json",
        json.dumps({"base_url": "https://model.example.com", "api_key": "k" * 20}),
    )
    values = {
        "app_dir": str(_app_dir(tmp_path)),
        "owner_token_file": str(owner),
        "second_token_file": str(second),
        "model_credential_file": str(model),
        "conflict_file": str(_conflict(tmp_path)),
        "priority_class": "proof-critical",
        "model": poe.MODEL,
        "slot_seconds": "1500",
    }
    values.update(overrides)
    argv = ["preflight", *_common(tmp_path)]
    for key, value in values.items():
        argv += ["--" + key.replace("_", "-"), value]
    return driver.build_parser().parse_args(argv)


def _stub_owner_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, priority_class: int = 0
) -> list[list[str]]:
    state = _state(tmp_path)
    flags = poe.Flags(
        CANDIDATE, KUBE_CONTEXT, "rqp-test", "scratch", "127.0.0.1:5000", 5000, state, "run-1"
    )
    inputs = poe.InputFile(str(tmp_path / "kubeconfig"), "/bin/true", "10.0.0.1")
    monkeypatch.setattr(
        pod,
        "preflight",
        lambda args, rt, grant: {"flags": flags, "inputs": inputs, "checks": {"rbac": True}},
    )
    probes: list[list[str]] = []

    def probe(argv: Any, env: Any, rt: Any) -> subprocess.CompletedProcess[bytes]:
        probes.append(list(argv))
        return subprocess.CompletedProcess(list(argv), priority_class, b"", b"")

    monkeypatch.setattr(pod, "probe", probe)
    return probes


def _rt() -> pod.Runtime:
    scrubber = pps.Scrubber()
    scan = pps.TokenScan(scrubber, ("stdout", "stderr", "children"))
    return pod.Runtime({}, pod.Output(scrubber, scan), scrubber, scan)


def test_preflight_composes_owner_checks_ledger_and_live_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probes = _stub_owner_preflight(monkeypatch, tmp_path)
    args = _live_args(tmp_path)
    found = driver.preflight(args, _rt(), "preflight")
    assert found["checks"] == {
        "rbac": True,
        "run_id_unused": True,
        "app_kit": True,
        "owner_token_file": True,
        "second_token_file": True,
        "model_credential_file": True,
        "conflict_file": True,
        "priority_class": True,
    }
    assert probes == [
        [
            "kubectl",
            "--context",
            KUBE_CONTEXT,
            "get",
            "priorityclass",
            "proof-critical",
            "-o",
            "name",
        ]
    ]


def test_preflight_refuses_a_missing_priority_class(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_owner_preflight(monkeypatch, tmp_path, priority_class=1)
    with pytest.raises(ris.Refusal) as refused:
        driver.preflight(_live_args(tmp_path), _rt(), "preflight")
    assert refused.value.code == "priority_class_invalid"


def test_preflight_refuses_a_missing_second_token_and_a_used_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_owner_preflight(monkeypatch, tmp_path)
    with pytest.raises(ris.Refusal) as refused:
        driver.preflight(
            _live_args(tmp_path, second_token_file=str(tmp_path / "absent")), _rt(), "preflight"
        )
    assert refused.value.code == "token_file_missing"
    ledger = ris.RequesterLedger.open(tmp_path / "state", "run-1", create=True)
    ledger.begin(CANDIDATE, "rehearse", T0)
    ledger.close()
    with pytest.raises(ris.Refusal) as used:
        driver.preflight(_live_args(tmp_path), _rt(), "preflight")
    assert used.value.code == "run_id_used"


def test_rehearse_preflight_skips_live_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probes = _stub_owner_preflight(monkeypatch, tmp_path)
    args = driver.build_parser().parse_args(["rehearse", *_common(tmp_path)])
    found = driver.preflight(args, _rt(), "rehearse")
    assert found["checks"] == {"rbac": True, "run_id_unused": True}
    assert probes == []


def test_main_refuses_an_ambient_model_credential_before_anything(
    tmp_path: Path, capsys: Any
) -> None:
    code = driver.main(
        ["rehearse", *_common(tmp_path)], environ={"ANTHROPIC_API_KEY": "synthetic-sentinel"}
    )
    assert code == poe.EXIT_REFUSED
    out = capsys.readouterr().out
    assert json.loads(out)["error"] == "service_credentials"
    assert "synthetic-sentinel" not in out


def test_live_shape_bounds_slot_seconds(tmp_path: Path) -> None:
    with pytest.raises(ris.Refusal) as refused:
        driver.require_live_shape(_live_args(tmp_path, slot_seconds="60"))
    assert refused.value.code == "live_arguments_invalid"
    driver.require_live_shape(_live_args(tmp_path))


# --------------------------------------------------------------------------
# Live inputs: stat only in preflight, strict shapes when read
# --------------------------------------------------------------------------


def test_token_file_is_stat_only_and_private(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = _private(tmp_path / "token", TOKEN + "\n")

    def unread(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("the preflight read a token file")

    with monkeypatch.context() as patched:
        patched.setattr(Path, "read_text", unread)
        patched.setattr(Path, "read_bytes", unread)
        ris.stat_token_file(token, "--owner-token-file")
    for path, code in (
        (None, "token_file_missing"),
        (tmp_path / "absent", "token_file_missing"),
        (_private(tmp_path / "loose", TOKEN, 0o644), "token_file_unsafe"),
        (_private(tmp_path / "empty", ""), "token_file_unsafe"),
        (_private(tmp_path / "large", "x" * 600), "token_file_unsafe"),
    ):
        with pytest.raises(ris.Refusal) as refused:
            ris.stat_token_file(path, "--owner-token-file")
        assert refused.value.code == code
    link = tmp_path / "link"
    link.symlink_to(token)
    with pytest.raises(ris.Refusal):
        ris.stat_token_file(link, "--owner-token-file")


def test_read_token_file_holds_one_token_line(tmp_path: Path) -> None:
    held = ris.read_token_file(_private(tmp_path / "token", TOKEN + "\n"), "--second-token-file")
    assert held.reveal() == TOKEN and TOKEN not in repr(held)
    for text in (f"{TOKEN}\n{TOKEN}\n", "has spaces in it, not a token at all\n"):
        with pytest.raises(ris.Refusal):
            ris.read_token_file(_private(tmp_path / "bad", text), "--second-token-file")


def test_app_kit_reads_identifiers_and_stats_the_key(tmp_path: Path) -> None:
    assert ris.load_app_kit(_app_dir(tmp_path))["repo"] == "acme-corp/acme-fixture"
    for name, overrides in (
        ("bad-id", {"id": "x"}),
        ("bad-repo", {"repo": "no-slash"}),
        ("extra", {"secret": "nope"}),
    ):
        root = tmp_path / name
        root.mkdir()
        with pytest.raises(ris.Refusal) as refused:
            ris.load_app_kit(_app_dir(root, **overrides))
        assert refused.value.code == "app_kit_invalid"
    missing = tmp_path / "missing"
    missing.mkdir()
    (_app_dir(missing) / "app.pem").unlink()
    with pytest.raises(ris.Refusal):
        ris.load_app_kit(missing / "app")
    loose = tmp_path / "loose"
    loose.mkdir()
    _app_dir(loose).chmod(0o755)
    with pytest.raises(ris.Refusal):
        ris.load_app_kit(loose / "app")
    with pytest.raises(ris.Refusal):
        ris.load_app_kit(Path("relative/app"))


def test_conflict_file_shape(tmp_path: Path) -> None:
    conflict = ris.load_conflict(_conflict(tmp_path))
    assert conflict.rule_path == "AGENTS.md"
    for overrides in (
        {"preference": "two\nlines here"},
        {"forbidden_added_line_regex": "("},
        {"rule_path": "../outside"},
        {"extra": "x"},
    ):
        with pytest.raises(ris.Refusal) as refused:
            ris.load_conflict(_conflict(tmp_path, **overrides))
        assert refused.value.code == "conflict_file_invalid"


def test_model_credential_shape(tmp_path: Path) -> None:
    good = {"base_url": "https://model.example.com/", "api_key": "k" * 20}
    base, key = ris.load_model_credential(_private(tmp_path / "m.json", json.dumps(good)))
    assert base.reveal() == "https://model.example.com" and key.reveal() == "k" * 20
    for bad in (
        {**good, "base_url": "http://model.example.com"},
        {**good, "api_key": "has space in the key"},
        {"base_url": good["base_url"]},
    ):
        with pytest.raises(ris.Refusal):
            ris.load_model_credential(_private(tmp_path / "m.json", json.dumps(bad)))
    with pytest.raises(ris.Refusal):
        ris.stat_model_credential(None)


# --------------------------------------------------------------------------
# Documents
# --------------------------------------------------------------------------


def test_documents_are_distinct_and_digested_as_the_api_stores_them() -> None:
    first = ris.make_document("first", NONCE)
    second = ris.make_document("second", NONCE)
    assert first.sha256 == text_sha256(first.text) != second.sha256
    assert first.canary in first.text and first.marker in first.text
    assert set(first.needles()).isdisjoint(second.needles())
    # The bare marker is legitimately visible to a diff reviewer, so it is no needle.
    assert first.marker not in first.needles()
    assert first.text not in repr(first)
    conflicted = ris.make_document("owner", NONCE, conflict="Use tabs everywhere.")
    assert "Use tabs everywhere." in conflicted.needles()
    for role, nonce, conflict in (("BAD", NONCE, ""), ("ok", "xyz", ""), ("ok", NONCE, "a\nb")):
        with pytest.raises(ValueError):
            ris.make_document(role, nonce, conflict=conflict)


def test_hits_finds_raw_and_json_escaped_needles() -> None:
    document = ris.make_document("first", NONCE)
    needles = document.needles()
    assert ris.hits(b"nothing here", needles) == 0
    assert ris.hits(document.canary.encode(), needles) == 1
    escaped = json.dumps({"text": document.preferences[0]}).encode()
    assert ris.hits(escaped, needles) == 1
    assert ris.hits(document.text.encode(), needles) == len(needles)


# --------------------------------------------------------------------------
# Ledger, slots and spend caps
# --------------------------------------------------------------------------


def test_slots_are_recorded_before_labels_and_capped(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    with pytest.raises(ris.Refusal) as unrecorded:
        ledger.mark_labeled(1, T0)
    assert unrecorded.value.code == "slot_unrecorded"
    for slot in (1, 2, 3):
        ledger.record_slot(slot, "owner", str(slot), T0 + slot)
        ledger.mark_labeled(slot, T0 + slot + 1)
    with pytest.raises(ris.Refusal) as twice:
        ledger.record_slot(2, "owner", "9", T0)
    assert twice.value.code == "slot_recorded"
    with pytest.raises(ris.Refusal) as relabel:
        ledger.mark_labeled(2, T0)
    assert relabel.value.code == "slot_recorded"
    for slot in (0, 4, True):
        with pytest.raises(ris.Refusal) as capped:
            ledger.record_slot(slot, "owner", "9", T0)
        assert capped.value.code == "slot_cap"
    assert ledger.count_launch(1) == 1 and ledger.count_launch(1) == 2
    assert ledger.launches(2) == 0 and ledger.launches(9) == 0
    assert [row["slot"] for row in ledger.slots()] == [1, 2, 3]
    record = ris.record_of(ledger)
    assert record.mode == "live" and len(record.slots) == 3


def test_a_rehearsal_records_no_slot(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, mode="rehearse")
    with pytest.raises(ris.Refusal) as refused:
        ledger.record_slot(1, "owner", "1", T0)
    assert refused.value.code == "live_mode_required"


def test_ledger_is_private_and_run_ids_are_never_reused(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.close()
    path = tmp_path / "state" / ris.LEDGER_FILE
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    ris.run_id_unused(tmp_path / "state", "run-2")
    with pytest.raises(ris.Refusal) as used:
        ris.run_id_unused(tmp_path / "state", "run-1")
    assert used.value.code == "run_id_used"
    with pytest.raises(poe.Refusal) as missing:
        ris.RequesterLedger.open(tmp_path / "state", "run-9", create=False)
    assert missing.value.code == "ledger_missing"
    path.chmod(0o644)
    with pytest.raises(poe.Refusal):
        ris.RequesterLedger.open(tmp_path / "state", "run-1", create=False)


@pytest.mark.parametrize(
    ("slot", "labeled", "launches", "live_open", "permitted"),
    [
        (1, True, 0, True, True),
        (1, True, 1, True, True),
        (1, True, 2, True, False),
        (1, False, 0, True, False),
        (1, True, 0, False, False),
        (None, True, 0, True, False),
        (4, True, 0, True, False),
        (1, True, -1, True, False),
    ],
)
def test_launch_gate(
    slot: Any, labeled: bool, launches: int, live_open: bool, permitted: bool
) -> None:
    assert (
        ris.launch_permitted(slot, labeled=labeled, launches_so_far=launches, live_open=live_open)
        is permitted
    )


def test_caps_match_precondition_five() -> None:
    assert ris.caps() == {
        "max_slots": 3,
        "session_usd": 5,
        "max_continuations_per_slot": 1,
        "launches_per_slot": 2,
    }
    assert ris.GATE_DEADLINE_SECONDS < 60


# --------------------------------------------------------------------------
# The reviewer transcript scan program: fails closed
# --------------------------------------------------------------------------


def _transcripts(root: Path, document: ris.Document, *, leak: bool) -> Path:
    project = root / "home" / ".claude" / "projects" / "p"
    side = project / "s1" / "subagents"
    side.mkdir(parents=True)
    main = [json.dumps({"type": "user", "message": document.text})]
    (project / "s1.jsonl").write_text("\n".join(main) + "\n")
    reviewer = {"type": "user", "isSidechain": True, "message": "Review this diff."}
    lines = [json.dumps(reviewer)]
    if leak:
        lines.append(json.dumps({"type": "user", "message": document.preferences[0]}))
    (side / "agent-1.jsonl").write_text("\n".join(lines) + "\n")
    return root / "home"


def test_reviewer_scan_counts_reviewer_records_only(tmp_path: Path) -> None:
    document = ris.make_document("first", NONCE)
    root = _transcripts(tmp_path / "clean", document, leak=False)
    code, out = _run(
        ris.REVIEWER_SCAN_PROGRAM, "20", str(root), stdin=json.dumps(document.needles()).encode()
    )
    scan = ris.reviewer_scan_result(code, out)
    # The parent transcript carries the whole document; it is not a reviewer record.
    assert scan == ris.ReviewerScan(files=1, records=1, hits=0, error=None)


@pytest.mark.parametrize("fault", ["unreadable_transcript", "missing_root"])
def test_reviewer_scan_fails_closed_on_an_incomplete_read(tmp_path: Path, fault: str) -> None:
    document = ris.make_document("first", NONCE)
    # One clean, readable reviewer transcript...
    root = _transcripts(tmp_path / "mixed", document, leak=False)
    roots = [str(root)]
    if fault == "unreadable_transcript":
        # ...beside one that cannot be opened (a dangling link names it).
        side = next(path for path in root.rglob("subagents") if path.is_dir())
        (side / "agent-unreadable.jsonl").symlink_to(tmp_path / "gone.jsonl")
    else:
        roots.append(str(tmp_path / "absent"))
    code, out = _run(
        ris.REVIEWER_SCAN_PROGRAM, "20", *roots, stdin=json.dumps(document.needles()).encode()
    )
    assert code == 4
    assert ris.reviewer_scan_result(code, out).error == "incomplete"


def test_reviewer_scan_fails_closed_on_an_unparsable_parent_line(tmp_path: Path) -> None:
    document = ris.make_document("first", NONCE)
    root = _transcripts(tmp_path / "torn", document, leak=False)
    parent = next(path for path in root.rglob("*.jsonl") if "subagents" not in path.parts)
    with parent.open("ab") as stream:
        stream.write(b'{"isSidechain": tru\n')
    code, out = _run(
        ris.REVIEWER_SCAN_PROGRAM, "20", str(root), stdin=json.dumps(document.needles()).encode()
    )
    assert ris.reviewer_scan_result(code, out).error == "incomplete"


def test_reviewer_scan_finds_a_leaked_preference(tmp_path: Path) -> None:
    document = ris.make_document("first", NONCE)
    root = _transcripts(tmp_path / "leak", document, leak=True)
    code, out = _run(
        ris.REVIEWER_SCAN_PROGRAM, "20", str(root), stdin=json.dumps(document.needles()).encode()
    )
    scan = ris.reviewer_scan_result(code, out)
    assert scan.error is None and scan.hits == 1 and scan.records == 2


@pytest.mark.parametrize(
    ("argv", "stdin"),
    [
        (["20", "/tmp"], b"not json"),
        (["20", "/tmp"], b"[]"),
        (["20"], b'["x"]'),
        (["75", "/tmp"], b'["x"]'),
        (["soon", "/tmp"], b'["x"]'),
    ],
)
def test_reviewer_scan_refuses_bad_input(argv: list[str], stdin: bytes) -> None:
    code, out = _run(ris.REVIEWER_SCAN_PROGRAM, *argv, stdin=stdin)
    assert code == 2
    assert ris.reviewer_scan_result(code, out).error == "input"


def test_reviewer_scan_stops_at_its_deadline(tmp_path: Path) -> None:
    side = tmp_path / "subagents"
    side.mkdir()
    # Opening a FIFO with no writer blocks, so only the deadline ends the scan.
    os.mkfifo(side / "agent-1.jsonl")
    code, out = _run(ris.REVIEWER_SCAN_PROGRAM, "0.3", str(tmp_path), stdin=b'["x"]')
    assert code == 3
    assert ris.reviewer_scan_result(code, out) == ris.ReviewerScan(error="deadline")


def test_a_crashed_or_unreadable_scan_is_never_clean() -> None:
    code, out = _run("raise RuntimeError('crash')")
    assert code != 0
    assert ris.reviewer_scan_result(code, out).error == "unreadable"
    assert ris.reviewer_scan_result(0, b'{"error": null, "files": 1}').error == "unreadable"
    assert ris.reviewer_scan_result(0, b"[1]").error == "unreadable"
    assert ris.reviewer_scan_result(1, b'{"error": null}').error == "failed"
    combined = ris.combine_scans(
        [ris.ReviewerScan(1, 2, 0, None), ris.ReviewerScan(error="deadline")]
    )
    assert combined.error == "deadline"
    assert ris.combine_scans([]).error == "unscanned"
    assert ris.combine_scans(
        [ris.ReviewerScan(1, 2, 0, None), ris.ReviewerScan(1, 3, 1, None)]
    ) == ris.ReviewerScan(2, 5, 1, None)


# --------------------------------------------------------------------------
# The model probe program: fails closed
# --------------------------------------------------------------------------


def test_model_probe_refuses_bad_input() -> None:
    for argv, stdin in (
        (["20"], b"not json"),
        (["20"], json.dumps({"base_url": "http://x", "api_key": "k", "model": "m"}).encode()),
        (["90"], json.dumps({"base_url": "https://x", "api_key": "k", "model": "m"}).encode()),
    ):
        code, out = _run(ris.MODEL_PROBE_PROGRAM, *argv, stdin=stdin)
        assert code == 2
        assert ris.model_probe_result(code, out) == {"ok": False, "status": None, "error": "input"}


def test_model_probe_stops_at_its_deadline() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        # The connection is accepted by the backlog and never answered.
        request = {"base_url": f"https://127.0.0.1:{port}", "api_key": "k", "model": "m"}
        code, out = _run(ris.MODEL_PROBE_PROGRAM, "0.5", stdin=json.dumps(request).encode())
    assert code == 3
    assert ris.model_probe_result(code, out)["error"] == "deadline"


def test_model_probe_fails_on_a_refused_connection_or_crash() -> None:
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port = unused.getsockname()[1]
    request = {"base_url": f"https://127.0.0.1:{port}", "api_key": "k", "model": "m"}
    code, out = _run(ris.MODEL_PROBE_PROGRAM, "10", stdin=json.dumps(request).encode())
    assert code == 1 and ris.model_probe_result(code, out)["ok"] is False
    assert ris.model_probe_result(0, b"Traceback") == {
        "ok": False,
        "status": None,
        "error": "unreadable",
    }
    assert ris.model_probe_result(1, b'{"ok": true}')["ok"] is False


def test_user_probe_requires_identity_and_write_permission() -> None:
    user = {"id": 42, "login": "acme-second"}
    assert ris.user_probe_result(200, user, 200, {"permission": "write"}) == {
        "ok": True,
        "account_id": 42,
        "login": "acme-second",
        "write": True,
    }
    assert ris.user_probe_result(200, user, 200, {"permission": "read", "role_name": "maintain"})[
        "ok"
    ]
    for status, body, permission in (
        (401, user, {"permission": "write"}),
        (200, {"login": "x"}, {"permission": "write"}),
        (200, {"id": True, "login": "x"}, {"permission": "write"}),
        (200, user, {"permission": "read"}),
    ):
        assert ris.user_probe_result(status, body, 200, permission)["ok"] is False
    assert ris.user_probe_result(200, user, 404, None)["account_id"] is None


# --------------------------------------------------------------------------
# Per-slot assertions over recorded evidence
# --------------------------------------------------------------------------


def test_annotation_is_the_last_line_only() -> None:
    digest = "0" * 64
    line = ris.expected_annotation(digest)
    assert ris.annotation_of(f"Body.\n\n{line}\n\n") == (line, 1)
    assert ris.annotation_of(f"{line}\nMore text after.") == (None, 1)
    assert ris.annotation_of(None) == (None, 0)
    assert ris.added_lines([{"patch": "@@\n+new\n-old\n+++ b/x\n context"}, {"patch": None}]) == [
        "new"
    ]


def test_slot_one_passes_on_applied_annotation_marker_and_repository_rule() -> None:
    document = ris.make_document("owner", NONCE, conflict="Name modules in camelCase.")
    observed = ris.slot_observations(
        1, _evidence(1, document), document=document, conflict_regex="camelCase"
    )
    expected = {key: value for key, value in ris.EXPECTED["10"].items() if key.startswith("slot1.")}
    assert observed == expected


def test_slot_one_fails_when_the_conflicting_preference_won() -> None:
    document = ris.make_document("owner", NONCE, conflict="Name modules in camelCase.")
    files = ({"patch": f"+def camelCaseThing():\n+{document.marker}"},)
    observed = ris.slot_observations(
        1, _evidence(1, document, files=files), document=document, conflict_regex="camelCase"
    )
    assert observed["slot1.repository_rule_won"] is False
    assert observed["slot1.marker_in_diff"] is True


def test_slot_two_fails_on_another_digest_or_a_second_annotation() -> None:
    document = ris.make_document("second", NONCE)
    other = ris.make_document("owner", NONCE)
    assert ris.slot_observations(2, _evidence(2, document), document=document) == {
        key: value for key, value in ris.EXPECTED["10"].items() if key.startswith("slot2.")
    }
    wrong = _evidence(2, other)
    assert ris.slot_observations(2, wrong, document=document)["slot2.annotation_matches"] is False
    doubled = _evidence(
        2,
        document,
        body=f"{ris.expected_annotation(document.sha256)}\n\n"
        f"{ris.expected_annotation(document.sha256)}",
    )
    assert ris.slot_observations(2, doubled, document=document)["slot2.annotation_matches"] is False


def test_slot_three_control_must_lack_annotation_and_marker() -> None:
    cleared = ris.make_document("owner", NONCE)
    assert ris.slot_observations(3, _evidence(3, cleared), document=cleared) == {
        key: value for key, value in ris.EXPECTED["10"].items() if key.startswith("slot3.")
    }
    annotated = _evidence(3, cleared, body=f"Body\n\n{ris.expected_annotation(cleared.sha256)}")
    assert ris.slot_observations(3, annotated, document=cleared)["slot3.annotation_absent"] is False
    marked = _evidence(3, cleared, files=({"patch": f"+{cleared.marker}"},))
    assert ris.slot_observations(3, marked, document=cleared)["slot3.marker_absent"] is False
    missing = _evidence(3, cleared, pull_request_found=False)
    observed = ris.slot_observations(3, missing, document=cleared)
    assert observed["slot3.annotation_absent"] is False and observed["slot3.marker_absent"] is False


def test_reviewer_evidence_must_be_seen_and_clean() -> None:
    document = ris.make_document("second", NONCE)
    leaked = _evidence(2, document, reviewer=ris.ReviewerScan(1, 3, 2, None))
    assert ris.slot_observations(2, leaked, document=document)["slot2.reviewer_hits"] == 2
    unseen = _evidence(2, document, reviewer=ris.ReviewerScan(0, 0, 0, None))
    observed = ris.slot_observations(2, unseen, document=document)
    assert observed["slot2.reviewer_transcripts_seen"] is False
    failed = _evidence(2, document, reviewer=ris.ReviewerScan(error="deadline"))
    observed = ris.slot_observations(2, failed, document=document)
    assert observed["slot2.reviewer_hits"] is None
    assert observed["slot2.reviewer_transcripts_seen"] is False


# --------------------------------------------------------------------------
# Forge cleanup bookkeeping
# --------------------------------------------------------------------------


def test_forge_cleanup_rows_count_only_confirmed_removals() -> None:
    rows = ris.forge_cleanup_rows(
        issues={"7": "closed", "8": "open"},
        pulls={"9": None},
        branches={"rqp-a": False, "rqp-b": None},
        label=False,
    )
    assert rows == [
        ("forge_issues", "#7", "absent"),
        ("forge_issues", "#8", "present"),
        ("forge_pulls", "#9", "present"),
        ("forge_branches", "rqp-a", "absent"),
        ("forge_branches", "rqp-b", "present"),
        ("forge_label", "run", "absent"),
    ]
    empty = ris.forge_cleanup_rows(issues={}, pulls={}, branches={}, label=None)
    assert ("forge_label", "run", "present") in empty
    assert {kind for kind, _, _ in empty} == {
        "forge_issues",
        "forge_pulls",
        "forge_branches",
        "forge_label",
    }


# --------------------------------------------------------------------------
# The judge
# --------------------------------------------------------------------------


def test_judge_passes_complete_records_for_both_criteria() -> None:
    verdict = ris.judge([_passing_record("9"), _passing_record("10")], "all")
    assert verdict["verdict"] == "pass"
    assert [item["verdict"] for item in verdict["criteria"]] == ["pass", "pass"]
    assert ris.exit_code(verdict["verdict"]) == poe.EXIT_PASS


def test_a_proof_that_did_not_run_is_missing_never_passed() -> None:
    verdict = ris.judge([_passing_record("9")], "all")
    assert verdict["verdict"] == "missing"
    assert verdict["criteria"][1]["verdict"] == "missing"
    assert ris.exit_code(verdict["verdict"]) == poe.EXIT_FAIL
    unfinished = ris.judge([_passing_record("10", outcome=None)], "10")
    assert unfinished["verdict"] == "missing"
    assert ris.judge([], "9")["verdict"] == "missing"


def test_one_missing_observation_or_wrong_value_fails() -> None:
    record = _passing_record("9")
    observations = dict(record.observations["9"])
    del observations["logs.canary_hits"]
    dropped = _passing_record("9", observations={"9": observations, "10": {}})
    result = ris.judge([dropped], "9")
    assert result["verdict"] == "fail"
    assert any(c["status"] == "missing" for c in result["criteria"][0]["checks"])
    leaked = {**record.observations["9"], "logs.canary_hits": 1}
    assert ris.judge([_passing_record("9", observations={"9": leaked})], "9")["verdict"] == "fail"
    # A True where the count 1 is expected is not a pass.
    typed = {**record.observations["9"], "logs.canary_hits": False}
    assert ris.judge([_passing_record("9", observations={"9": typed})], "9")["verdict"] == "fail"


def test_cleanup_and_slot_order_are_judged() -> None:
    record = _passing_record("10")
    dirty = [*record.cleanup, {"kind": "foreign_untouched", "ref": "s", "status": "present"}]
    assert ris.judge([_passing_record("10", cleanup=dirty)], "10")["verdict"] == "fail"
    no_forge = [row for row in record.cleanup if row["kind"] != "forge_label"]
    assert ris.judge([_passing_record("10", cleanup=no_forge)], "10")["verdict"] == "fail"
    failed_step = [*record.cleanup, {"kind": "step", "ref": "absence", "status": "failed"}]
    assert ris.judge([_passing_record("10", cleanup=failed_step)], "10")["verdict"] == "fail"
    late = [dict(row) for row in record.slots]
    late[0]["recorded_at"] = late[0]["labeled_at"] + 1
    assert ris.judge([_passing_record("10", slots=late)], "10")["verdict"] == "fail"
    overspent = [dict(row) for row in record.slots]
    overspent[1]["launches"] = 3
    assert ris.judge([_passing_record("10", slots=overspent)], "10")["verdict"] == "fail"
    failed = ris.judge([_passing_record("10", outcome="failed")], "10")
    assert failed["verdict"] == "fail"


def test_a_rehearsal_never_proves_the_live_criterion() -> None:
    rehearsal = _passing_record("9")
    live_keys = {"9": {}, "10": dict(ris.EXPECTED["10"])}
    mislabeled = _passing_record("9", observations=live_keys)
    assert ris.judge([rehearsal, mislabeled], "10")["verdict"] == "missing"
    with pytest.raises(ris.Refusal):
        ris.judge([rehearsal], "11")


def test_judge_command_reads_the_ledger_only_and_prints_sanitized(
    tmp_path: Path, capsys: Any
) -> None:
    ledger = _ledger(tmp_path, mode="rehearse", run_id="rehearse-1")
    for key, value in ris.EXPECTED["9"].items():
        ledger.record("9", key, value, T0)
    for kind in ris.CLEANUP_KINDS["9"]:
        ledger.record_cleanup(kind, "x", "absent", "", T0)
    ledger.record_cleanup("step", "fence", "ok", "", T0)
    ledger.finish("completed", T0)
    ledger.close()
    state = str(tmp_path / "state")
    code = driver.main(
        ["judge", "--state-dir", state, "--run-id", "rehearse-1", "--criterion", "9"]
    )
    out = capsys.readouterr().out
    assert code == poe.EXIT_PASS and json.loads(out)["verdict"] == "pass"
    code = driver.main(
        ["judge", "--state-dir", state, "--run-id", "rehearse-1", "--criterion", "all"]
    )
    assert code == poe.EXIT_FAIL
    assert json.loads(capsys.readouterr().out)["criteria"][1]["verdict"] == "missing"
    code = driver.main(["judge", "--state-dir", state, "--run-id", "absent", "--criterion", "9"])
    assert code == poe.EXIT_REFUSED
    assert json.loads(capsys.readouterr().out)["error"] == "ledger_missing"


def test_refusal_payloads_name_codes_and_flags_never_values() -> None:
    payload = ris.Refusal("token_file_unsafe", "--second-token-file").payload()
    assert payload["error"] == "token_file_unsafe" and "--second-token-file" in payload["fix"]
    inherited = ris.Refusal("cluster_unready", "--context").payload()
    assert inherited["fix"].startswith(poe.FIXES["cluster_unready"])

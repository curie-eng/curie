"""A failed run must not disclose a private value (ADR 0271 review fix 2).

Covers ``scrub_private``, ``Evidence.hold_private`` / ``Evidence.finish`` and the
scrub ``require_private_run`` does before it raises, through ``identity_scenarios.run``
and the ``prove`` output path of ``identity_e2e.main``. Synthetic values only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import identity_e2e
import identity_scenarios as scenarios
import identity_scenarios_attestation as att
import pytest
from identity_scenarios import Evidence, StepFailed

SEG = "A1b2C3d4E5f6G7h8_-9z"
ENV = f"{SEG}.{SEG}"
TICKET = f"cat1.{ENV}"
GRANT = f"ccg.{ENV}"
SECRET = "synthetic-secret-value-0001"
SUBJECT = "synthetic-subject-0002"
ISSUER = "https://idp.identity-e2e.example"
NAME = "zz-privacy-emission"


def principal() -> dict[str, str]:
    return {"tenant_id": "tenant-0001", "issuer": ISSUER, "subject": SUBJECT}


# --- scrub_private ----------------------------------------------------------------


def test_scrub_replaces_a_principal_nested_in_a_list() -> None:
    data = {"steps": [{}, {"observed": {"who": principal()}}]}
    assert att.scrub_private(data) == 1
    assert data["steps"][1]["observed"]["who"] == "[redacted: principal]"


def test_scrub_replaces_an_envelope_inside_a_sentence() -> None:
    data = {"note": f"the proxy refused {GRANT} on retry", "keep": "fine"}
    assert att.scrub_private(data) == 1
    assert data == {"note": "[redacted]", "keep": "fine"}


def test_scrub_replaces_a_held_secret_in_a_failure_string_and_a_tuple() -> None:
    data = {"steps": [{"failure": f"{SECRET} was not the approver"}], "seen": ("ok", f"x-{SECRET}")}
    assert att.scrub_private(data, secrets=(SECRET,)) == 2
    assert data["steps"][0]["failure"] == "[redacted]"
    assert data["seen"][1] == "[redacted]"


def test_scrub_replaces_a_key_equal_to_a_secret_and_keeps_its_value() -> None:
    data = {"by": {SECRET: {"note": f"has {SECRET}"}}}
    assert att.scrub_private(data, secrets=(SECRET,)) == 2
    assert data == {"by": {"[redacted key 1]": {"note": "[redacted]"}}}


def test_scrub_ignores_a_short_secret() -> None:
    data = {"note": "contains abcdefg here"}
    assert att.scrub_private(data, secrets=("abcdefg",)) == 0
    assert data == {"note": "contains abcdefg here"}


def test_scrub_of_a_clean_tree_changes_nothing() -> None:
    data = {"steps": [{"id": "a", "observed": {"n": 3, "ok": True, "x": None}}], "s": ("a", "b")}
    before = json.dumps(data, sort_keys=True)
    assert att.scrub_private(data, secrets=(SECRET,)) == 0
    assert json.dumps(data, sort_keys=True) == before


def test_scrubbed_tree_passes_the_guard() -> None:
    data = {
        "a": [{"x": principal()}],
        "b": f"see {TICKET}",
        "c": {SECRET: f"{SECRET}!"},
        "d": (f"{SECRET}", 1),
    }
    assert att.scrub_private(data, secrets=(SECRET,)) >= 4
    assert att.require_private_evidence(data, secrets=(SECRET,)) > 0


# --- Evidence.hold_private / finish -----------------------------------------------


def test_finish_without_anything_private_changes_only_passed_and_finished_at() -> None:
    evidence = Evidence(NAME)
    evidence.hold_private([SECRET, 7, None])
    with evidence.step("a", "clean") as seen:
        seen["count"] = 2
    before = json.loads(json.dumps(evidence.data))
    done = evidence.finish()
    assert "privacy" not in done
    assert done.pop("passed") is True
    assert done.pop("finished_at")
    assert done == before


def test_finish_scrubs_steps_failure_and_aborted_and_fails_the_run() -> None:
    evidence = Evidence(NAME)
    evidence.hold_private([SECRET])
    evidence.hold_private([SUBJECT])
    with evidence.step("a", "observe") as seen:
        seen["value"] = SECRET
    # Evidence.step records a StepFailed as the step's failure and swallows it.
    with evidence.step("b", "fail"):
        raise StepFailed(f"no {SECRET}")
    assert evidence.data["steps"][-1]["passed"] is False
    evidence.data["aborted"] = f"gone {SUBJECT}"
    done = evidence.finish()
    flat = json.dumps(done)
    assert SECRET not in flat
    assert SUBJECT not in flat
    assert done["passed"] is False
    assert done["privacy"] == {"redactions": 3}


# --- require_private_run ----------------------------------------------------------


def test_require_private_run_scrubs_evidence_and_seen_before_raising() -> None:
    evidence = Evidence(NAME)
    with evidence.step("a", "earlier") as earlier:
        earlier["ticket"] = f"minted {TICKET}"
    seen: dict[str, Any] = {"echo": f"again {TICKET}"}
    with pytest.raises(StepFailed) as caught:
        att.require_private_run(evidence, seen, secrets=())
    assert "steps[0].observed.ticket" in str(caught.value)
    for held in (str(caught.value), json.dumps(evidence.data), json.dumps(seen)):
        assert SEG not in held


# --- the failed run, through run() and the prove output path ----------------------


class StubHarness:
    def __init__(self, *_: Any, **__: Any) -> None:
        self.state = self

    def load(self) -> None:
        return None


def leaky(_harness: Any, _args: Any, evidence: Evidence) -> None:
    evidence.hold_private([SECRET])
    with evidence.step("observe", "records a held value") as seen:
        seen["value"] = SECRET
        seen["who"] = principal()
    with evidence.step("fail", "fails with an envelope"):
        raise StepFailed(f"refused {GRANT}")
    raise identity_e2e.HarnessError(f"lost {SECRET}")


def clean(_harness: Any, _args: Any, evidence: Evidence) -> None:
    evidence.hold_private([SECRET])
    with evidence.step("ok", "records only counts") as seen:
        seen["count"] = 1


def prove(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    func: Any,
) -> tuple[int, str, str]:
    monkeypatch.setitem(scenarios.SCENARIOS, NAME, func)
    monkeypatch.setattr(identity_e2e, "Harness", StubHarness)
    out = tmp_path / "evidence.json"
    code = identity_e2e.main(
        [
            "prove",
            "--context", "ctx",
            "--namespace", "ns",
            "--release", "rel",
            "--state-dir", str(tmp_path / "state"),
            "--scenario", NAME,
            "--evidence", str(out),
        ]
    )  # fmt: skip
    return code, out.read_text(), capsys.readouterr().out


def test_failed_run_discloses_nothing_in_the_file_or_stdout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, written, printed = prove(monkeypatch, tmp_path, capsys, leaky)
    assert code == 1
    for text in (written, printed):
        assert SECRET not in text
        assert SUBJECT not in text
        assert SEG not in text
        assert json.loads(text)["passed"] is False
        assert json.loads(text)["privacy"]["redactions"] >= 3


def test_clean_run_passes_with_no_privacy_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, written, printed = prove(monkeypatch, tmp_path, capsys, clean)
    assert code == 0
    for text in (written, printed):
        assert json.loads(text)["passed"] is True
        assert "privacy" not in json.loads(text)

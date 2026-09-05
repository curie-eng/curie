"""The wire-lock gate: a version gate that actually gates.

The gate must distinguish "the wire changed" from "someone bumped the version".
Its fingerprint therefore normalizes the version out of the schema, and it fails
a wire change that is not accompanied by a version bump -- with a message saying
which to do. Every model mutation here is done IN MEMORY (a mutated ``_MODELS``
tuple via ``pydantic.create_model``); no file is touched, so the repo stays clean.
"""

import json
from pathlib import Path

import pytest
from aci_protocol import PROTOCOL_VERSION, schema_export, wire_lock
from aci_protocol.schema_export import _MODELS
from aci_protocol.turn import QueuedTurn
from aci_protocol.wire_lock import (
    WireLockError,
    check_against_base,
    check_wire_lock,
    wire_fingerprint,
    write_lock,
)
from pydantic import Field, create_model

# A model set that differs from the shipped one by exactly one added model, so a
# correct fingerprint changes while the version does not.
_MUTATED_MODELS = _MODELS + (create_model("Injected", x=(str, ...)),)

# A model set identical on the wire to the shipped one but carrying a different
# docstring and a new field ``description`` -- a doc-only edit. ``create_model``
# (not a subclass) keeps the ``$defs`` key "QueuedTurn" so the sole schema delta
# is documentation.
#
# The swapped model MUST be a pure root -- one no other model in the set $refs.
# Replacing a $ref'd model makes pydantic disambiguate the name collision by
# module-qualifying BOTH defs ("__main____X" / "aci_protocol__session__X") and
# retargeting its referrer's $ref, which moves structural keys the fingerprint
# deliberately keeps. That is a fixture artifact, not a doc-only edit, and it
# would make this test assert the opposite of what it means.
#
# This was ``SessionConfig`` until #488, whose ``BootEnv`` composes it as a field
# and so ended its pure-root status. ``QueuedTurn`` is the structural analogue:
# nothing $refs it, and it $refs another model (``ReplyHandle``) just as
# SessionConfig $refs Budget/OtelConfig.
_DOC_ONLY_MODELS = tuple(
    create_model(
        "QueuedTurn",
        __base__=QueuedTurn,
        __doc__="A deliberately different docstring: documentation only.",
        text=(str, Field(description="the inbound message text")),
    )
    if model is QueuedTurn
    else model
    for model in _MODELS
)


def test_gate_fires_when_wire_changes_without_a_bump() -> None:
    # AC: CI fails a wire change that does not bump the version, with a message
    # saying which to do.
    h_real = wire_fingerprint()
    h_mutated = wire_fingerprint(_MUTATED_MODELS)
    assert h_mutated != h_real  # precondition: the mutation is observable
    lock = {"protocol_version": "0.2.0", "wire_sha256": h_real}
    with pytest.raises(WireLockError) as excinfo:
        check_wire_lock(fingerprint=h_mutated, version="0.2.0", lock=lock)
    message = str(excinfo.value)
    assert "bump" in message.lower()
    assert "version" in message.lower()


def test_gate_passes_when_wire_changes_with_a_bump() -> None:
    h_real = wire_fingerprint()
    h_mutated = wire_fingerprint(_MUTATED_MODELS)
    lock = {"protocol_version": "0.2.0", "wire_sha256": h_real}
    # A bumped version legitimizes the wire change; no raise.
    check_wire_lock(fingerprint=h_mutated, version="0.3.0", lock=lock)


def test_gate_passes_on_a_version_only_bump() -> None:
    # AC: CI still passes when only the version is bumped (wire unchanged).
    h_real = wire_fingerprint()
    lock = {"protocol_version": "0.2.0", "wire_sha256": h_real}
    check_wire_lock(fingerprint=h_real, version="0.2.1", lock=lock)


def test_fingerprint_is_invariant_under_a_version_bump(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Decision 4's whole point: the fingerprint normalizes the version out, so a
    # version-only bump does not move the hash. Without this the gate is a
    # tautology and nobody would notice.
    baseline = wire_fingerprint()
    monkeypatch.setattr(schema_export, "PROTOCOL_VERSION", "0.9.9")
    monkeypatch.setattr("aci_protocol.version.PROTOCOL_VERSION", "0.9.9")
    assert wire_fingerprint() == baseline


def test_fingerprint_changes_when_a_field_is_added() -> None:
    # The inverse guard: a fingerprint that normalizes too aggressively would be
    # blind to a real wire change. Also kills wire_fingerprint -> return "".
    assert wire_fingerprint(_MUTATED_MODELS) != wire_fingerprint()


def test_fingerprint_is_invariant_under_a_doc_only_change() -> None:
    # Change 2b: documentation is not wire-compatibility surface. A new docstring
    # or Field(description=...) renders into the schema as title/description keys
    # but changes nothing a consumer decodes, so it must not move the hash (the
    # change-class table's "Doc/description only -> none"). Without the recursive
    # title/description strip this would force a spurious version bump.
    assert wire_fingerprint(_DOC_ONLY_MODELS) == wire_fingerprint()


def test_check_against_base_skips_a_missing_base_lock() -> None:
    # The base predates the lock (this PR and all pre-lock history): a None base
    # lock is not a failure, it returns quietly.
    check_against_base(None)


def test_check_against_base_fails_a_wire_move_at_the_same_base_version() -> None:
    # The gate the committed-lock test cannot express: the current wire differs
    # from the base lock's hash at the SAME version, i.e. a wire change was landed
    # without a bump relative to base.
    stale = {"protocol_version": PROTOCOL_VERSION, "wire_sha256": "0" * 64}
    with pytest.raises(WireLockError):
        check_against_base(stale)


def test_check_against_base_passes_when_base_matches_current_wire() -> None:
    # An unchanged wire against a base lock that already records it: no raise.
    base = {"protocol_version": PROTOCOL_VERSION, "wire_sha256": wire_fingerprint()}
    check_against_base(base)


def test_write_lock_refuses_an_unbumped_wire_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Closes the escape hatch (decision 4): the regenerator enforces the same
    # rule as the gate, so an author cannot silently rewrite the lock without a
    # bump.
    #
    # write_lock targets the REAL committed schema/wire.lock via _lock_path(),
    # so this test points it at a tmp copy first (#627). Without that, during a
    # PROTOCOL_VERSION bump -- version bumped, lock not yet regenerated -- the
    # mutated fingerprint differs from the stale lock at a *different* version,
    # which passes check_wire_lock and rewrites the real lock to a poisoned
    # half-state, breaking the next check-contracts.sh run and this test's own
    # precondition.
    lock_file = tmp_path / "wire.lock"
    original = (
        json.dumps(
            {"protocol_version": PROTOCOL_VERSION, "wire_sha256": wire_fingerprint()},
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    lock_file.write_text(original, encoding="utf-8")
    monkeypatch.setattr(wire_lock, "_lock_path", lambda: lock_file)

    with pytest.raises(WireLockError):
        write_lock(_MUTATED_MODELS)

    # The refusal fires before any write, so even the tmp lock is untouched.
    assert lock_file.read_text(encoding="utf-8") == original


def test_committed_lock_matches_the_current_wire() -> None:
    # The live CI assertion, wiring the real fingerprint against the committed
    # wire.lock.
    lock_path = schema_export.schema_path().parent / "wire.lock"
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    assert lock["protocol_version"] == PROTOCOL_VERSION
    assert lock["wire_sha256"] == wire_fingerprint()


def test_bootstrap_token_contract_uses_patch_version_0_4_6() -> None:
    # Session identity landed as 0.4.4 (optional session_id/history_ref), the
    # adoption credential and its ack as 0.4.5. This patch adds the optional
    # BootEnv.runner_bootstrap_token key and nothing on the event wire.
    lock_path = schema_export.schema_path().parent / "wire.lock"
    lock = json.loads(lock_path.read_text(encoding="utf-8"))

    assert PROTOCOL_VERSION == "0.4.6"
    assert lock["protocol_version"] == "0.4.6"

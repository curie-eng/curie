"""Round-trip and decision-pinning tests for the shared eval/approval wire models.

These models cross a Valkey Stream between the API and the worker. They carry no
``version`` field (the ``QueuedTurn`` exemplar carries none either), so the
properties asserted here are the whole contract: round-trip identity, the
strict-producer/tolerant-consumer split from ``_AciModel``, and the per-field
optionality and constraint decisions the shared models resolve.

The decision-pinning tests below are the point of the exercise. Each one fails if
the constraint it names is silently loosened out of the model:

    EvalJob.bundle_ref            required but nullable (adopt the API's side)
    ApprovalRequest string fields min_length=1 (adopt the API's strict side)
    ApprovalRequest.expires_in_seconds   gt=0, default None
    ApprovalRequest.gate_kind     GateKind | None -- the value domain tightens,
                                  but it MUST stay nullable: an older runner
                                  emits neither gate field during a rolling
                                  deploy and the durable row's columns stay NULL.
"""

import uuid

import pytest
from aci_protocol import (
    RUNS_STREAM_DEFAULT,
    STREAM_PAYLOAD_FIELD,
    WORKER_GROUP_DEFAULT,
    ApprovalRequest,
    EvalJob,
    EvalReport,
    GateKind,
)
from aci_protocol.events import _READER_CONTEXT_KEY
from pydantic import ValidationError

_AGENT_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_VERSION_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _full_eval_job() -> EvalJob:
    return EvalJob(
        agent_id=_AGENT_ID,
        version_id=_VERSION_ID,
        sha="abc123",
        suite="smoke",
        bundle_ref="s3://bundles/abc123.zip",
        target_url="https://example.test/status",
        model="claude-opus-4-8",
        requested_at="2026-01-01T00:00:00Z",
    )


def _minimal_eval_job_fields() -> dict[str, object]:
    # The minimal legal payload: bundle_ref is required, so it is present and
    # explicitly None. target_url and model are the only omittable fields.
    return {
        "agent_id": _AGENT_ID,
        "version_id": _VERSION_ID,
        "sha": "abc123",
        "suite": "smoke",
        "bundle_ref": None,
        "requested_at": "2026-01-01T00:00:00Z",
    }


def _full_eval_report() -> EvalReport:
    return EvalReport(
        repo_full_name="acme/widgets",
        sha="abc123",
        passed_count=3,
        total=4,
        target_url="https://example.test/status",
    )


def _full_approval_request() -> ApprovalRequest:
    return ApprovalRequest(
        agent_id=_AGENT_ID,
        conversation_id="c1",
        author="u1",
        summary="deploy the thing",
        reply_kind="email",
        reply_channel="C1",
        reply_placeholder="1.0",
        reply_endpoint="https://example.test/api",
        reply_adapter="agentmail-sandbox",
        dedupe_key="e1",
        route="ops",
        card_channel="C2",
        gate_kind=GateKind.PERMISSION,
        granted_tool="Bash",
        granted_arguments={"command": "printf ok", "options": {"flags": ["a"]}},
        expires_in_seconds=3600,
    )


def _minimal_approval_request_fields() -> dict[str, object]:
    return {
        "conversation_id": "c1",
        "author": "u1",
        "summary": "deploy the thing",
        # Required alongside `reply_channel` (plan EB-A15): the durable record's
        # routing half. Its presence here is what makes the "minimal" payload
        # minimal rather than incomplete.
        "reply_kind": "slack",
        "reply_channel": "C1",
        "reply_placeholder": "1.0",
        "dedupe_key": "e1",
    }


# --- Round-trip identity ------------------------------------------------------


def test_eval_job_round_trips_identically() -> None:
    original = _full_eval_job()
    restored = EvalJob.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.agent_id == _AGENT_ID
    assert restored.version_id == _VERSION_ID
    assert restored.sha == "abc123"
    assert restored.suite == "smoke"
    assert restored.bundle_ref == "s3://bundles/abc123.zip"
    assert restored.target_url == "https://example.test/status"
    assert restored.model == "claude-opus-4-8"
    assert restored.requested_at == "2026-01-01T00:00:00Z"


def test_eval_report_round_trips_identically() -> None:
    original = _full_eval_report()
    restored = EvalReport.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.repo_full_name == "acme/widgets"
    assert restored.sha == "abc123"
    assert restored.passed_count == 3
    assert restored.total == 4
    assert restored.target_url == "https://example.test/status"


def test_approval_request_round_trips_identically() -> None:
    original = _full_approval_request()
    restored = ApprovalRequest.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.agent_id == _AGENT_ID
    assert restored.conversation_id == "c1"
    assert restored.author == "u1"
    assert restored.summary == "deploy the thing"
    assert restored.reply_kind == "email"
    assert restored.reply_channel == "C1"
    assert restored.reply_placeholder == "1.0"
    assert restored.reply_endpoint == "https://example.test/api"
    assert restored.reply_adapter == "agentmail-sandbox"
    assert restored.dedupe_key == "e1"
    assert restored.route == "ops"
    assert restored.card_channel == "C2"
    assert restored.gate_kind == GateKind.PERMISSION
    assert restored.granted_tool == "Bash"
    assert restored.granted_arguments == {"command": "printf ok", "options": {"flags": ["a"]}}
    assert restored.expires_in_seconds == 3600


# --- Round-trip with optionals omitted ----------------------------------------


def test_eval_job_minimal_payload_lands_on_documented_defaults() -> None:
    original = EvalJob(**_minimal_eval_job_fields())  # type: ignore[arg-type]
    restored = EvalJob.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.bundle_ref is None
    assert restored.target_url is None
    assert restored.model is None


def test_eval_report_minimal_payload_lands_on_documented_defaults() -> None:
    original = EvalReport(repo_full_name="acme/widgets", sha="abc123", passed_count=0, total=0)
    restored = EvalReport.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.target_url is None


def test_approval_request_minimal_payload_lands_on_documented_defaults() -> None:
    original = ApprovalRequest(**_minimal_approval_request_fields())  # type: ignore[arg-type]
    restored = ApprovalRequest.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.agent_id is None
    assert restored.reply_endpoint is None
    # Nullable because `slack` legitimately has no adapter: its route is the
    # worker's configured Slack origin (plan D4.4, EB-A7).
    assert restored.reply_adapter is None
    assert restored.route is None
    assert restored.card_channel is None
    assert restored.gate_kind is None
    assert restored.granted_tool is None
    assert restored.expires_in_seconds is None


# --- Strict producer ----------------------------------------------------------


def test_constructing_eval_job_with_unknown_field_is_strict() -> None:
    with pytest.raises(ValidationError):
        EvalJob(**_minimal_eval_job_fields(), bogus=1)  # type: ignore[arg-type]


def test_constructing_eval_report_with_unknown_field_is_strict() -> None:
    with pytest.raises(ValidationError):
        EvalReport(
            repo_full_name="acme/widgets",
            sha="abc123",
            passed_count=0,
            total=0,
            bogus=1,
        )


def test_constructing_approval_request_with_unknown_field_is_strict() -> None:
    with pytest.raises(ValidationError):
        ApprovalRequest(**_minimal_approval_request_fields(), bogus=1)  # type: ignore[arg-type]


# --- T-A12: the durable record carries the routing half too -------------------


def test_approval_request_without_reply_kind_is_rejected() -> None:
    """T-A12 / AC2 (plan EB-A15, finding 2).

    `ApprovalRequest` is the model approvals actually use (`schemas.py` re-exports
    it as `ApprovalCreate`), so this is where the durable record's routing half is
    made non-optional. Required, not optional, for D1's reason: a `reply_kind`
    that can be absent puts the silent misroute back on the RESUME path, where it
    is least observable -- the reply lands in the wrong channel days later.

    Note this model's own docstring calls `gate_kind`/`granted_tool` "optional for
    the rolling-deploy window". Phase 2 deliberately does not follow that
    precedent; the quiescent cutover (plan section 6.1) is what buys the right to
    reject it.
    """

    fields = _minimal_approval_request_fields()
    del fields["reply_kind"]

    with pytest.raises(ValidationError):
        ApprovalRequest(**fields)  # type: ignore[arg-type]


def test_approval_request_rejects_an_empty_reply_kind() -> None:
    """T-A12, the min_length half. `reply_kind` mirrors `reply_channel`'s
    `Field(min_length=1)`, so a producer cannot satisfy "required" with `""` --
    an empty kind resolves to no binding and would drop every resume silently.
    """

    with pytest.raises(ValidationError):
        ApprovalRequest(**{**_minimal_approval_request_fields(), "reply_kind": ""})  # type: ignore[arg-type]


# --- Tolerant consumer --------------------------------------------------------
#
# Tolerance is read-only and reader-context gated (the ``_AciModel`` contract):
# the sanctioned consumer decode threads the flag, so a payload from a newer
# producer carrying a field this build does not model still decodes. This is the
# forward-compat property that makes a patch bump honest.


def test_eval_job_consumer_ignores_unknown_field() -> None:
    payload = _full_eval_job().model_dump_json()[:-1] + ', "future_field": 1}'
    job = EvalJob.model_validate_json(payload, context={_READER_CONTEXT_KEY: True})
    assert job == _full_eval_job()


def test_eval_report_consumer_ignores_unknown_field() -> None:
    payload = _full_eval_report().model_dump_json()[:-1] + ', "future_field": 1}'
    report = EvalReport.model_validate_json(payload, context={_READER_CONTEXT_KEY: True})
    assert report == _full_eval_report()


def test_approval_request_consumer_ignores_unknown_field() -> None:
    payload = _full_approval_request().model_dump_json()[:-1] + ', "future_field": 1}'
    request = ApprovalRequest.model_validate_json(payload, context={_READER_CONTEXT_KEY: True})
    assert request == _full_approval_request()


# --- Decision 1: bundle_ref is required, but nullable -------------------------


def test_eval_job_without_bundle_ref_raises() -> None:
    # Omitting the key entirely fails: the API is the only producer and it always
    # emits it. The worker's default of None was drift, not a designed tolerance.
    fields = _minimal_eval_job_fields()
    del fields["bundle_ref"]
    with pytest.raises(ValidationError):
        EvalJob(**fields)  # type: ignore[arg-type]


def test_eval_job_with_explicit_null_bundle_ref_succeeds() -> None:
    # Still nullable, so every payload the API has ever produced decodes.
    job = EvalJob(**_minimal_eval_job_fields())  # type: ignore[arg-type]
    assert job.bundle_ref is None


# --- Decision 2: the API's strict constraints ---------------------------------


def test_approval_request_empty_conversation_id_raises() -> None:
    fields = _minimal_approval_request_fields()
    fields["conversation_id"] = ""
    with pytest.raises(ValidationError):
        ApprovalRequest(**fields)  # type: ignore[arg-type]


def test_approval_request_without_reply_placeholder_raises() -> None:
    fields = _minimal_approval_request_fields()
    del fields["reply_placeholder"]
    with pytest.raises(ValidationError):
        ApprovalRequest(**fields)  # type: ignore[arg-type]


def test_approval_request_with_null_reply_placeholder_succeeds() -> None:
    fields = _minimal_approval_request_fields()
    fields["reply_placeholder"] = None
    request = ApprovalRequest.model_validate(fields)
    assert request.reply_placeholder is None


def test_approval_request_with_empty_reply_placeholder_raises() -> None:
    fields = _minimal_approval_request_fields()
    fields["reply_placeholder"] = ""
    with pytest.raises(ValidationError):
        ApprovalRequest(**fields)  # type: ignore[arg-type]


def test_approval_request_zero_expires_in_seconds_raises() -> None:
    with pytest.raises(ValidationError):
        ApprovalRequest(**_minimal_approval_request_fields(), expires_in_seconds=0)  # type: ignore[arg-type]


def test_approval_request_negative_expires_in_seconds_raises() -> None:
    with pytest.raises(ValidationError):
        ApprovalRequest(**_minimal_approval_request_fields(), expires_in_seconds=-1)  # type: ignore[arg-type]


def test_approval_request_positive_expires_in_seconds_succeeds() -> None:
    request = ApprovalRequest(**_minimal_approval_request_fields(), expires_in_seconds=1)  # type: ignore[arg-type]
    assert request.expires_in_seconds == 1


# --- Decision 2: the GateKind value domain ------------------------------------


def test_approval_request_unknown_gate_kind_raises() -> None:
    # gate_kind is authority-bearing (it decides whether a gate may grant), so an
    # unrecognized value is rejected, never degraded to None.
    with pytest.raises(ValidationError):
        ApprovalRequest(**_minimal_approval_request_fields(), gate_kind="nonsense")  # type: ignore[arg-type]


def test_approval_request_permission_gate_kind_succeeds() -> None:
    request = ApprovalRequest(**_minimal_approval_request_fields(), gate_kind="permission")  # type: ignore[arg-type]
    assert request.gate_kind == GateKind.PERMISSION


def test_approval_request_policy_gate_kind_succeeds() -> None:
    request = ApprovalRequest(**_minimal_approval_request_fields(), gate_kind="policy")  # type: ignore[arg-type]
    assert request.gate_kind == GateKind.POLICY


def test_approval_request_null_gate_kind_succeeds() -> None:
    # Load-bearing for the rolling-deploy window: an older runner emits no gate
    # provenance and the durable row's columns stay NULL. Never make this required.
    request = ApprovalRequest(**_minimal_approval_request_fields(), gate_kind=None)  # type: ignore[arg-type]
    assert request.gate_kind is None


# --- Transport constants ------------------------------------------------------
#
# These pin the wire literals the lanes used to hand-mirror, so a silent rename
# of any one of them is caught here rather than at runtime on a live stream.


def test_transport_constants_have_their_wire_values() -> None:
    assert RUNS_STREAM_DEFAULT == "curie:runs"
    assert WORKER_GROUP_DEFAULT == "curie-workers"
    assert STREAM_PAYLOAD_FIELD == "payload"

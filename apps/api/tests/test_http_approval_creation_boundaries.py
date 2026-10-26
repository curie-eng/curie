"""Creation value, retry and failure boundaries over the actual API and real stores.

@spec HTTP-APPROVAL-CONTINUITY-2/3/5 HTTP-INVOCATION-EXECUTION-16. Synthetic inputs only.
"""

from __future__ import annotations

import copy
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api.crud import approvals as approvals_crud
from sqlalchemy.exc import IntegrityError, OperationalError

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    accepted,
    canonical,
    digest,
    opened,
    post,
    refused,
    rows,
)
from apps.api.tests.test_http_approval_creation import (
    creation_case as creation_case,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import (
    assert_response,
    delivery_state,
    post_check,
    queued_invocation,
    request_for,
)


def with_arguments(case: SimpleNamespace, arguments: dict[str, Any]) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-4: an explicit permission scope with these arguments."""
    request = copy.deepcopy(case.request)
    request["approval"]["granted_arguments"] = arguments
    return request


def rotated_runtime(scope: dict[str, Any]) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-5: same keys, a later registration without this binding."""
    module = membership._runtime_api()
    providers = membership.build_provider_credential_registry(
        ((scope["configuration"], membership.ProviderBearer(membership.BEARER)),)
    )
    registration = module.ReceiverRegistrationV1(
        schema_version=1, receiver_id=scope["receiver_id"], generation=1,
        tenant_id=scope["tenant"], channel_identity_id=scope["identity"],
        channel_identity_name=scope["adapter"], provider_installation_id=scope["installation"],
        namespace=scope["namespace"], installation_account=membership.ACCOUNT,
        bot_user_id="U0EXAMPLEBOT", binding_ids=(str(uuid.uuid4()),),
        human_native_ids=(membership.SENDER,),
    )
    return module.build_http_invocation_runtime(
        providers, ((registration, scope["receiver_key"]),),
        (("synthetic-context", 1, scope["context_key"]),),
        signing_key=("synthetic-context", 1),
    )


@pytest.mark.parametrize(
    "arguments",
    [
        {"threshold": 0.5, "nested": {"ratio": -2.25}, "items": [1.5, 2]},
        {"large": 1e16, "unit": 1.0, "small": 1e-7},
    ],
    ids=["decimals", "storage-normalized"],
)
def test_finite_decimal_arguments_create_and_retry_against_the_stored_row(
    creation_case: SimpleNamespace, arguments: dict[str, Any],
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2/5: only nonfinite numbers refuse; seal the stored row."""
    case = creation_case
    request = with_arguments(case, arguments)
    first = accepted(post(case, request), request)
    stored = rows(request["approval"]["dedupe_key"])
    assert len(stored) == 1 and str(stored[0]["id"]) == first["id"]
    _, _, capsule = opened(case, stored[0])
    assert capsule.approval_record_sha256 == digest(
        canonical({key: stored[0][key] for key in RECORD_FIELDS})
    )
    retry = copy.deepcopy(request)
    retry["request_id"] = str(uuid.uuid4())
    assert accepted(post(case, retry), retry, duplicate=True)["id"] == first["id"]
    assert rows(request["approval"]["dedupe_key"]) == stored


def test_identical_retry_is_historical_after_the_receiver_registration_changes(
    creation_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5: a retry never reasserts current permission."""
    case = creation_case
    first = accepted(post(case), case.request)
    before = rows(case.request["approval"]["dedupe_key"])
    calls = len(case.provider.requests)
    case.client.app.state.http_identity_runtime = rotated_runtime(case.scope)
    retry = copy.deepcopy(case.request)
    retry["request_id"] = str(uuid.uuid4())
    assert accepted(post(case, retry), retry, duplicate=True)["id"] == first["id"]
    assert len(case.provider.requests) == calls
    assert rows(case.request["approval"]["dedupe_key"]) == before
    fresh = copy.deepcopy(case.request)
    fresh["approval"]["summary"] = "A different synthetic operation"
    refused(post(case, fresh), 409, "approval_provenance_conflict")


@pytest.mark.parametrize("fault", ["no-winner", "reread-fails"])
def test_uniqueness_race_failures_are_unavailable_not_definitive(
    creation_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, fault: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3: only a proven conflict is a definitive refusal."""
    case = creation_case

    async def insert(*_args: Any, **_kwargs: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-3: a constraint failure that is not a duplicate row."""
        raise IntegrityError("INSERT", {}, Exception("synthetic constraint"))

    original = approvals_crud.get_approval_by_dedupe_key
    reads = {"count": 0}

    async def lookup(*args: Any, **kwargs: Any) -> Any:
        """@spec HTTP-APPROVAL-CONTINUITY-3: the post-race reread is the third lookup."""
        reads["count"] += 1
        if fault == "reread-fails" and reads["count"] >= 3:
            raise OperationalError("SELECT", {}, Exception("synthetic database fault"))
        return await original(*args, **kwargs)

    monkeypatch.setattr(approvals_crud, "insert_approval", insert)
    monkeypatch.setattr(approvals_crud, "get_approval_by_dedupe_key", lookup)
    refused(post(case), 503, "authority_unavailable")
    assert reads["count"] == 3
    assert rows(case.request["approval"]["dedupe_key"]) == []


def test_unregistered_receiver_with_an_approval_id_keeps_the_invalid_context_answer(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: registration refusal precedes the resume refusal."""
    provider = synthetic_provider
    with queued_invocation(provider, valkey, runs_stream) as invocation:
        invocation.client.app.state.http_identity_runtime = rotated_runtime(invocation.scope)
        request = request_for(invocation, stage="claim_or_resume")
        request["approval_id"] = str(uuid.uuid4())
        before = delivery_state(valkey, runs_stream, invocation.scope["binding"])
        assert_response(
            post_check(invocation.client, request), request,
            decision="refuse", reason="invalid_context",
        )
        assert len(provider.requests) == 1
        assert delivery_state(valkey, runs_stream, invocation.scope["binding"]) == before

"""Creation value, retry and failure boundaries over the actual API and real stores.

@spec HTTP-APPROVAL-CONTINUITY-2/3/5 HTTP-INVOCATION-EXECUTION-16. Synthetic inputs only.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import redis

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_creation import (
    creation_case as creation_case,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import (
    assert_response,
    delivery_state,
    post_check,
    request_for,
)


def test_resume_claim_rejects_the_receiver_that_lost_its_binding(
    creation_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    case = creation_case
    state = case.client.app.state
    served = state.http_identity_runtime
    stream = delivery_state(valkey, runs_stream, case.scope["binding"])
    request = request_for(case, stage="claim_or_resume")
    before = len(case.provider.requests)
    state.http_identity_runtime = membership._runtime(
        membership._runtime_api(), {**case.scope, "binding": str(uuid.uuid4())},
    )
    try:
        request["approval_id"] = str(uuid.uuid4())
        assert_response(post_check(case.client, request), request,
                        decision="refuse", reason="invalid_context")
        assert len(case.provider.requests) == before
        assert delivery_state(valkey, runs_stream, case.scope["binding"]) == stream
    finally:
        state.http_identity_runtime = served
    request["approval_id"] = None
    assert_response(post_check(case.client, request), request,
                    decision="continue", reason="current_authority")
    assert len(case.provider.requests) == before + 1
    assert delivery_state(valkey, runs_stream, case.scope["binding"]) == stream

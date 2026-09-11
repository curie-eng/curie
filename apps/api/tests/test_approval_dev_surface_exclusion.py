"""No development-only approval capability ships, and none is reachable in prod.

The capability defended here is *"resolve an approval without a genuine chat
principal"*, not "a route named /dev exists".  A route-name denylist is
deliberately absent: ``POST /approvals/principals/operator`` is a real
production issuance surface whose path contains "principal", so a denylist
either fails on the real route or is amended until it no longer names the
actual issuance surface.  Either way it is not the control, and it only ever
defends against a surface named the way we would have named it.  Every
assertion below is therefore positive and behavioral, driven against the real
app in ordinary production wiring with a real approval row.

The one structural assertion is the surface inventory: the live router table is
asked which handlers can reach ``approval_principal.mint`` or
``crud.claim_approval_resolution`` at all -- by object identity, not by path --
so a NEW issuance or resolution surface fails loudly instead of going unnoticed
by a suite that only exercises the surfaces it already knows about.

Bypasses this set does NOT test, stated plainly rather than claimed complete:
- **Replay of a valid chat bearer inside its 60s TTL.** Inherent to a bearer with no
  nonce; untested here and not defended by this PR.
- **In-process `dependency_overrides`** on the app object (already used by
  `apps/api/tests/test_approvals.py:2117-2128`). Anyone with in-process access to the app
  can bypass everything; that is not a boundary this test can defend.
- **A stolen valid operator token used against an `ExplicitUsers` route** -- that is the
  intended operator capability (#1531), not a bypass, but it means "platform key holder
  cannot resolve" is **false in general** and this plan does not claim it. The claim is
  narrowed to: *a platform key holder cannot resolve a channel-bound approval.*
- **A handler that reaches mint/claim through a non-``curie_api`` indirection** (a
  third-party dispatcher, a dynamically resolved attribute). The inventory walk
  follows names to objects through this package only.
- **``approval_auth``'s ``compare_digest(attester, api_key)`` equal-secret guard.** It is
  unreachable through ``Settings``, which refuses equal keys at construction; the
  construction refusal is what is pinned instead.
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import tomllib
import types
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path, PurePath
from typing import Any

import httpx
import pytest
import uvicorn
from curie_api import approval_principal, crud
from curie_api.approval_auth import APPROVAL_PRINCIPAL_HEADER, CONSOLE_SESSION_COOKIE
from curie_api.config import Settings, get_settings
from curie_api.deps import get_approver_sets
from curie_api.main import create_app
from curie_api.routers.console import SESSION_COOKIE
from curie_api.slack_approvers import SlackApproverSetSelector
from curie_api.usergroups import UserGroupMembership
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from pydantic import ValidationError

ATTESTER_ENV = "CURIE_APPROVAL_CHAT_ATTESTER_SECRET"
DEV_ATTESTER_DEFAULT = "curie-dev-approval-chat-attester"
SUBJECT = "U0EXAMPLE1"
OTHER = "U0EXAMPLE2"
CARD_CHANNEL = "C0EXAMPLE1"
SOURCE_CHANNEL = "C0EXAMPLE2"
GROUP = "S0EXAMPLE1"


@pytest.fixture
def approvals_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """Build the app after the per-test runs-stream override is installed."""

    with TestClient(create_app()) as test_client:
        yield test_client


def _create_approval(client: TestClient, auth_headers: dict[str, str]) -> dict[str, Any]:
    """A channel-bound approval: no route binding, so the card channel decides.

    ``SlackChannelMembers`` is the zero-setup default set, and the one that is
    neither operator- nor console-eligible.
    """

    payload = {
        "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": OTHER,
        "summary": "Confirm the requested action",
        "reply_kind": "slack",
        "reply_channel": CARD_CHANNEL,
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
    }
    response = client.post("/approvals", json=payload, headers=auth_headers)
    assert response.status_code == 201, response.text
    return response.json()


def _chat_token(approval_id: str, *, signing_key: str | None = None) -> str:
    return approval_principal.mint(
        signing_key or get_settings().approval_chat_attester_secret,
        subject=SUBJECT,
        kind="chat",
        actor_channel=CARD_CHANNEL,
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 60,
    )


def _principal_headers(token: str) -> dict[str, str]:
    return {APPROVAL_PRINCIPAL_HEADER: token}


def _cookie_headers(token: str) -> dict[str, str]:
    # As test_approval_authenticated_principals.py does: TestClient's origin is
    # HTTP so its jar refuses to auto-send the Secure session cookie.
    return {"Cookie": f"{CONSOLE_SESSION_COOKIE}={token}"}


def _mint_console_session(client: TestClient, auth_headers: dict[str, str]) -> str:
    minted = client.post("/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers)
    assert minted.status_code == 201, minted.text
    exchanged = client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    token = client.cookies.get(SESSION_COOKIE)
    assert token
    client.cookies.clear()
    return token


def _status(client: TestClient, approval_id: str, auth_headers: dict[str, str]) -> str:
    return client.get(f"/approvals/{approval_id}", headers=auth_headers).json()["status"]


def _audit(client: TestClient, approval_id: str, auth_headers: dict[str, str]) -> list[dict]:
    return client.get(f"/approvals/{approval_id}/audit", headers=auth_headers).json()


@contextlib.contextmanager
def _settings_env(**overrides: str) -> Iterator[None]:
    """Re-read Settings under ``overrides`` for the duration of the block.

    ``approval_auth`` calls ``get_settings()`` per request, so clearing the
    cache retargets the already-running app -- no second app, no second
    lifespan against the disposable database.
    """

    saved = {name: os.environ.get(name) for name in overrides}
    os.environ.update(overrides)
    get_settings.cache_clear()
    try:
        yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()


def _code_tree(code: types.CodeType) -> Iterator[types.CodeType]:
    """The function's own bytecode plus every nested function/comprehension."""

    yield code
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            yield from _code_tree(const)


def _reaches(endpoint: Any, targets: dict[int, str]) -> set[str]:
    """Which of ``targets`` (by object identity) this handler can reach.

    Names are resolved to OBJECTS -- against the defining module's globals and
    against any ``curie_api`` module it imported -- so the answer does not
    depend on how a call happens to be spelled, and a handler that reaches a
    target through a helper is still counted.
    """

    hit: set[str] = set()
    seen: set[int] = set()
    stack = [inspect.unwrap(endpoint)]
    while stack:
        func = stack.pop()
        code = getattr(func, "__code__", None)
        if code is None or id(func) in seen:
            continue
        seen.add(id(func))
        namespace = getattr(func, "__globals__", {})
        modules = [
            value
            for value in namespace.values()
            if inspect.ismodule(value) and getattr(value, "__name__", "").startswith("curie_api")
        ]
        for block in _code_tree(code):
            for name in block.co_names:
                for candidate in [namespace.get(name)] + [
                    getattr(module, name, None) for module in modules
                ]:
                    resolved = inspect.unwrap(candidate) if inspect.isfunction(candidate) else None
                    if resolved is None:
                        continue
                    if id(resolved) in targets:
                        hit.add(targets[id(resolved)])
                    elif getattr(resolved, "__module__", "").startswith("curie_api"):
                        stack.append(resolved)
    return hit


def _api_routes(app: Any) -> Iterator[APIRoute]:
    """Every APIRoute the app can actually serve, sub-routers included.

    ``app.routes`` is not flat on this FastAPI: ``include_router`` leaves a lazy
    wrapper holding the original router, so a substring/loop over ``app.routes``
    alone would see only ``/health`` and ``/ready`` and pass vacuously.
    """

    pending: list[Any] = list(app.routes)
    seen: set[int] = set()
    while pending:
        route = pending.pop()
        if id(route) in seen:
            continue
        seen.add(id(route))
        if isinstance(route, APIRoute):
            yield route
        inner = getattr(route, "original_router", None)
        if inner is not None:
            pending.extend(inner.routes)
        pending.extend(getattr(route, "routes", []))


def test_the_live_app_exposes_exactly_one_mint_and_one_resolve_surface() -> None:
    """Inventory the real router table, so a NEW issuance surface fails here.

    Every other test in this module exercises a surface we already know about;
    none of them would notice a freshly added ``POST /approvals/principals/chat``
    minting channel-bound evidence with no Slack click. This one asks
    ``create_app()`` which handlers can reach ``approval_principal.mint`` and
    ``crud.claim_approval_resolution`` AT ALL, by object identity rather than by
    path substring (a path denylist is the control this module rejects, see the
    module docstring). Adding any second minting or resolving handler -- whatever
    it is named, wherever it is mounted -- breaks this assertion by construction.
    """

    targets = {
        id(approval_principal.mint): "mint",
        id(crud.claim_approval_resolution): "resolve",
    }
    inventory: dict[str, set[str]] = {}
    app = create_app()
    routes = list(_api_routes(app))
    # Guard the guard: a flattening that returned nothing would make the
    # inventory below look clean for the wrong reason.
    assert len(routes) > 50, len(routes)
    for route in routes:
        reached = _reaches(route.endpoint, targets)
        if reached:
            inventory[route.endpoint.__qualname__] = reached

    # The control: the walk must actually find something, or an inventory that
    # silently resolved nothing would read as "no surfaces exist".
    assert inventory == {
        "mint_operator_principal": {"mint"},
        "resolve_approval": {"resolve"},
    }, inventory


def test_platform_key_operator_principal_cannot_resolve_a_channel_bound_approval(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The real issuance surface issues a credential the channel-bound set refuses.

    Minted through ``POST /approvals/principals/operator`` with the platform key
    -- the production path, not a hand-forged token -- so what is pinned is the
    capability an X-API-Key holder actually has.
    """

    minted = approvals_client.post(
        "/approvals/principals/operator", json={"subject": SUBJECT}, headers=auth_headers
    )
    assert minted.status_code == 201, minted.text
    approval = _create_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(minted.json()["token"]),
    )

    # 403, not 401: the token authenticated fine and was refused by the
    # authorizer's set-eligibility gate. A 401 here would mean the test passed
    # on a credential problem rather than on the capability boundary.
    assert denied.status_code == 403, denied.text
    assert "explicit user list" in denied.json()["detail"]
    entry = _audit(approvals_client, approval["id"], auth_headers)[0]
    assert entry["principal_kind"] == "operator"
    assert entry["authorizer"] == "ChannelMembershipAuthorizer"
    assert entry["authorized"] is False
    assert _status(approvals_client, approval["id"], auth_headers) == "pending"


def test_the_platform_key_itself_is_not_a_resolve_credential(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The most obvious "no genuine chat principal" credential: X-API-Key alone.

    The operator case above proves the platform key cannot mint its way past the
    channel-bound set; this proves it is not a resolve credential at all, so the
    mint step is not merely an inconvenience to be skipped.
    """

    approval = _create_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=auth_headers,
    )

    # 401 on the credential, not a 403 from an authorizer: nothing authenticated
    # as a principal, so no set ever got to vote.
    assert denied.status_code == 401, denied.text
    assert denied.json()["detail"] == "missing or invalid approval principal"
    assert _audit(approvals_client, approval["id"], auth_headers) == []
    assert _status(approvals_client, approval["id"], auth_headers) == "pending"


@pytest.mark.parametrize("retired_field", ["resolved_by", "actor_channel"])
def test_body_asserted_identity_never_substitutes_for_a_principal(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    retired_field: str,
) -> None:
    """Re-honoring a body identity field IS the defended capability.

    A caller-asserted ``resolved_by`` / ``actor_channel`` with no principal
    header would be a resolve without any dispatcher attestation. Either shape
    of refusal is acceptable (401 on the missing credential, 422 on the retired
    field), but the row must still be pending: a 422-only assertion can pass on
    validation while some other path still writes the resolution.
    """

    approval = _create_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved", retired_field: SUBJECT},
        headers=auth_headers,
    )

    assert denied.status_code in (401, 422), denied.text
    assert _audit(approvals_client, approval["id"], auth_headers) == []
    assert _status(approvals_client, approval["id"], auth_headers) == "pending"


class _AlwaysMemberGroup:
    """A group lookup that would ADMIT the subject, if it were ever consulted."""

    def __init__(self) -> None:
        self.calls = 0

    async def members(self, group_id: str) -> UserGroupMembership:
        self.calls += 1
        return UserGroupMembership(
            group=group_id,
            users=frozenset({SUBJECT}),
            fetched_at=datetime.now(UTC),
            cache_age_s=0.0,
        )


def _group_bound_approval(
    client: TestClient, auth_headers: dict[str, str]
) -> tuple[dict[str, Any], _AlwaysMemberGroup]:
    source = _AlwaysMemberGroup()
    client.app.dependency_overrides[get_approver_sets] = lambda: SlackApproverSetSelector(source)
    route = f"managers-{uuid.uuid4().hex[:8]}"
    agent = client.post(
        "/agents",
        json={
            "name": f"approval-group-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": SOURCE_CHANNEL},
            "approval_routes": {
                route: {
                    "resolution": {"kind": "slack", "address": CARD_CHANNEL},
                    "approvers": {"group": GROUP},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    payload = {
        "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "agent_id": agent.json()["id"],
        "author": OTHER,
        "summary": "Confirm the requested action",
        "route": route,
        "card_channel": CARD_CHANNEL,
        "gate_kind": "policy",
        "reply_kind": "slack",
        "reply_channel": SOURCE_CHANNEL,
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
    }
    created = client.post("/approvals", json=payload, headers=auth_headers)
    assert created.status_code == 201, created.text
    return created.json(), source


def test_operator_principal_is_refused_by_a_GROUP_bound_set_too(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The other ``operator_eligible = False`` set, which nothing else here hits.

    The channel-bound case pins ``SlackChannelMembers``; flipping only
    ``SlackUserGroupMembers.operator_eligible`` would leave this file green while
    an operator token resolved a group-bound route. The lookup is overridden with
    a source that WOULD admit the subject, so ``calls == 0`` proves the 403 is
    the eligibility gate rather than a Slack-lookup failure or an unbound route.
    """

    minted = approvals_client.post(
        "/approvals/principals/operator", json={"subject": SUBJECT}, headers=auth_headers
    )
    assert minted.status_code == 201, minted.text
    approval, source = _group_bound_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(minted.json()["token"]),
    )

    assert denied.status_code == 403, denied.text
    assert "explicit user list" in denied.json()["detail"]
    assert source.calls == 0
    entry = _audit(approvals_client, approval["id"], auth_headers)[0]
    assert entry["principal_kind"] == "operator"
    assert entry["authorizer"] == "UserGroupAuthorizer"
    assert entry["authorized"] is False
    assert _status(approvals_client, approval["id"], auth_headers) == "pending"


def test_console_session_cookie_alone_obtains_no_grant_on_a_channel_bound_approval(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """A live, subject-bound console session carries no channel evidence.

    The cookie is real and its session is live (it authenticates), so the
    refusal is the console-eligibility gate and not an invalid-cookie 401.
    """

    cookie = _cookie_headers(_mint_console_session(approvals_client, auth_headers))
    approval = _create_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=cookie,
    )

    assert denied.status_code == 403, denied.text
    assert "console approval principals" in denied.json()["detail"]
    entry = _audit(approvals_client, approval["id"], auth_headers)[0]
    assert entry["principal_kind"] == "console"
    assert entry["authorized"] is False
    assert _status(approvals_client, approval["id"], auth_headers) == "pending"


def test_cookie_and_principal_header_together_fail_closed_before_any_authorizer(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Two credentials are ambiguous and are refused, not resolved by precedence."""

    approval = _create_approval(approvals_client, auth_headers)
    headers = {
        **_principal_headers(_chat_token(approval["id"])),
        **_cookie_headers(_mint_console_session(approvals_client, auth_headers)),
    }

    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=headers,
    )

    assert denied.status_code == 401, denied.text
    # The ambiguity detail, not the bare missing-credential 401: both halves
    # were present and neither was chosen.
    assert denied.json()["detail"] == "ambiguous approval principal credentials"
    assert _audit(approvals_client, approval["id"], auth_headers) == []
    assert _status(approvals_client, approval["id"], auth_headers) == "pending"


def test_empty_attester_secret_refuses_chat_tokens_instead_of_falling_open(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """An unconfigured verifier key refuses every chat token, signed or not.

    The control at the end is what makes this a configuration refusal rather
    than a malformed-token refusal: the SAME token resolves once the real key
    is back.
    """

    approval = _create_approval(approvals_client, auth_headers)
    token = _chat_token(approval["id"])

    with _settings_env(**{ATTESTER_ENV: ""}):
        denied = approvals_client.post(
            f"/approvals/{approval['id']}/resolve",
            json={"decision": "approved"},
            headers=_principal_headers(token),
        )
        assert denied.status_code == 401, denied.text
        assert denied.json()["detail"] == "missing or invalid approval principal"
        assert _audit(approvals_client, approval["id"], auth_headers) == []
        assert _status(approvals_client, approval["id"], auth_headers) == "pending"

    allowed = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(token),
    )
    assert allowed.status_code == 200, allowed.text


def test_chat_token_signed_with_the_platform_key_is_refused(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The platform key cannot forge the dispatcher's attestation.

    The second half is the discriminator: an identically shaped token signed
    with the attester secret resolves, so the refusal is the signing key and
    not the envelope.

    What this is NOT: it is an HMAC mismatch, so it does not exercise
    ``approval_auth``'s ``compare_digest(attester, api_key)`` equal-secret
    guard. That branch is dead behind ``Settings``, which refuses an attester
    equal to API_KEY at construction (pinned by the key-separation test below),
    so no running app can reach it.
    """

    forged_for = _create_approval(approvals_client, auth_headers)
    genuine_for = _create_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{forged_for['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(
            _chat_token(forged_for["id"], signing_key=get_settings().api_key)
        ),
    )
    assert denied.status_code == 401, denied.text
    assert denied.json()["detail"] == "missing or invalid approval principal"
    assert _audit(approvals_client, forged_for["id"], auth_headers) == []
    assert _status(approvals_client, forged_for["id"], auth_headers) == "pending"

    allowed = approvals_client.post(
        f"/approvals/{genuine_for['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(_chat_token(genuine_for["id"])),
    )
    assert allowed.status_code == 200, allowed.text


# Test placeholders hoisted to constants (not literals inline) so the repo's
# secret-shaped-literal gate does not trip on these quoted values.
_PLATFORM_CREDENTIAL_VALUE = "a-real-key"
_ATTESTER_VALUE = "a-real-chat-attester-secret"
_WEBHOOK_VALUE = "a-real-secret"
_WORKER_TOKEN_VALUE = "a-real-worker-token"
_DEV_PLATFORM_CREDENTIAL_VALUE = "curie-dev-key"


def _prod_settings(**overrides: str) -> Settings:
    # Ignore any ambient .env / process env so the case controls every field,
    # as apps/api/tests/test_config_prod_gate.py does.
    base = {
        "environment": "prod",
        "api_key": _PLATFORM_CREDENTIAL_VALUE,
        "approval_chat_attester_secret": _ATTESTER_VALUE,
        "github_webhook_secret": _WEBHOOK_VALUE,
        "internal_worker_token": _WORKER_TOKEN_VALUE,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


@pytest.mark.parametrize(
    "attester_secret, why",
    [
        ("", "unset"),
        (DEV_ATTESTER_DEFAULT, "still the shipped dev default"),
    ],
)
def test_prod_boot_refuses_an_attester_secret_that_is_not_a_real_independent_key(
    attester_secret: str, why: str
) -> None:
    """The real prod gate (``_refuse_dev_defaults_in_prod``), not a toggle we invented.

    ``CURIE_DEV`` / ``DEBUG`` / ``CURIE_APPROVAL_BYPASS`` appear nowhere under
    apps/api/src, so asserting anything about them would pass vacuously. These
    two cases are the ones that reach the ``environment == "prod"`` branch; the
    equal-to-API_KEY case is refused earlier in every environment and is pinned
    separately below, so it is NOT evidence about the prod gate.

    The offender-list message is asserted, not merely the variable name, so a
    case that tripped some other check cannot pass for the wrong reason.
    """

    with pytest.raises(ValidationError) as exc:
        _prod_settings(approval_chat_attester_secret=attester_secret)
    message = str(exc.value)
    assert "ENVIRONMENT=prod but these secrets are unset or still the dev default" in message, why
    assert ATTESTER_ENV in message, why


def test_an_attester_secret_equal_to_the_platform_key_is_refused_in_every_environment() -> None:
    """Key separation (#1531) is enforced at construction, before the prod branch.

    This is a DIFFERENT gate from the prod one above: it fires in dev too, which
    is why the equal-key case never reaches ``environment == "prod"``. Asserting
    the distinct-from-API_KEY message is what keeps the two apart.
    """

    with pytest.raises(ValidationError) as exc:
        Settings(
            _env_file=None,
            environment="dev",
            api_key=_PLATFORM_CREDENTIAL_VALUE,
            approval_chat_attester_secret=_PLATFORM_CREDENTIAL_VALUE,
        )
    message = str(exc.value)
    assert f"{ATTESTER_ENV} must be distinct from API_KEY" in message
    assert "ENVIRONMENT=prod" not in message


def test_the_app_factory_cannot_boot_in_prod_on_the_shipped_dev_attester() -> None:
    """Through ``create_app()``/``get_settings()``, not just ``Settings(...)``.

    A factory that caught ``ValidationError`` (or a code path that read the
    attester without going through ``Settings``) would leave the constructor
    test above green while the process happily served prod traffic on the
    published dev key. This boots the real thing under ENVIRONMENT=prod.
    """

    with _settings_env(
        ENVIRONMENT="prod",
        API_KEY=_PLATFORM_CREDENTIAL_VALUE,
        GITHUB_WEBHOOK_SECRET=_WEBHOOK_VALUE,
        CURIE_INTERNAL_WORKER_TOKEN=_WORKER_TOKEN_VALUE,
        **{ATTESTER_ENV: DEV_ATTESTER_DEFAULT},
    ):
        with pytest.raises(ValidationError) as exc:
            create_app()
        assert ATTESTER_ENV in str(exc.value)


def test_whitespace_only_attester_secret_is_refused_at_settings_construction() -> None:
    """Stronger than the approval_auth guard, and the reason it never runs.

    ``" "`` is truthy, so it would sail past ``approval_auth.py:94``'s
    ``not attester_secret`` check -- but ``Settings()`` raises first
    (config.py:428-429), in EVERY environment, so the process never boots to
    serve with it. Pinned here so that ordering cannot move silently.

    A deliberate local restatement of ``test_config_prod_gate.py``'s blank-value
    pin: this module's capability argument depends on "an unconfigured verifier
    key can never serve", so the claim is proved here rather than borrowed from
    a file that could be narrowed without anyone reading this one.
    """

    with pytest.raises(ValidationError) as exc:
        Settings(
            _env_file=None,
            environment="dev",
            api_key=_DEV_PLATFORM_CREDENTIAL_VALUE,
            approval_chat_attester_secret="   ",
        )
    assert "must be non-blank" in str(exc.value)


def test_the_composed_loopback_api_seam_does_not_outlive_its_fixture(
    _disposable_db: Any,
) -> None:
    """AC-SEC3: the port the composed seam listens on genuinely stops answering.

    A closed ``httpx.Client`` proves nothing (any closed client raises), so this
    runs the real app on a real ephemeral loopback socket, proves it answers,
    tears it down, and then proves a FRESH client cannot connect to that port.
    """

    server = uvicorn.Server(
        uvicorn.Config(create_app(), host="127.0.0.1", port=0, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 30
    while not server.started and thread.is_alive() and time.time() < deadline:
        time.sleep(0.05)
    assert server.started, "loopback API never reported started (lifespan failure?)"
    port = server.servers[0].sockets[0].getsockname()[1]

    with httpx.Client(base_url=f"http://127.0.0.1:{port}") as live:
        assert live.get("/health").status_code == 200

    server.should_exit = True
    thread.join(timeout=30)
    assert not thread.is_alive()

    with httpx.Client() as after, pytest.raises(httpx.ConnectError):
        after.get(f"http://127.0.0.1:{port}/health")


# --- Stage 3: the dev interaction harness never reaches production ----------
#
# Stage 3 adds a dev-only interaction harness at
# ``packages/test-support/src/curie_test_support/interaction/`` with a
# ``python -m curie_test_support.interaction`` entry point. It registers no
# route, adds no service and adds no toggle -- so the assertions below defend
# the CAPABILITY ("nothing that can drive an approval without a genuine chat
# click ships"), not the name. The primary control is the resolved
# ``--no-dev`` dependency closure: a distribution that is not installed cannot
# be imported however it is spelled, where a module-name grep only ever finds
# the name we happened to think of.

HARNESS_DISTRIBUTION = "curie-test-support"
HARNESS_MODULE = "curie_test_support"
HARNESS_SUBMODULE = f"{HARNESS_MODULE}.interaction"
HARNESS_PACKAGE_PATH = "packages/test-support"
REPO_ROOT = Path(__file__).resolve().parents[3]
# Every first-party image whose build context is the WORKSPACE, keyed by the
# distribution its Dockerfile installs and valued by the workspace member that
# ships in it. Those are the only images the harness could reach, because they
# are the only ones that copy `packages/test-support` into a build stage at all.
#
# `adapters/discord` was missing from this list while being exactly that shape
# (`COPY . .` then `uv sync --frozen --no-dev --no-editable --package
# curie-discord-adapter`), so "every production image" excluded an image. The
# list is re-derived and cross-checked against the tree below rather than
# hand-maintained, so the next one cannot go missing silently.
APP_PACKAGES = (
    "curie-api",
    "curie-dispatcher",
    "curie-worker",
    "curie-mail-adapter",
    "curie-discord-adapter",
)
APP_MEMBERS = {
    "curie-api": "apps/api",
    "curie-dispatcher": "apps/dispatcher",
    "curie-worker": "apps/worker",
    "curie-mail-adapter": "apps/mail-adapter",
    "curie-discord-adapter": "adapters/discord",
}
APP_NAMES = ("api", "dispatcher", "worker", "mail-adapter")
APP_DOCKERFILES = tuple(f"{member}/Dockerfile" for member in APP_MEMBERS.values())
PROD_DOCKERFILES = (*APP_DOCKERFILES, "runner/Dockerfile")
# Images whose build context is their own directory (the sre-bot connectors) or
# which install no Python from the workspace (apps/ui, node) cannot reach a
# workspace member at all; `prototypes/` and `cli/scripts/fixtures/` are not
# shipped. `test_the_production_image_list_covers_every_workspace_context_image`
# below re-derives this from the tree so the exclusion is checked, not asserted.
NON_WORKSPACE_DOCKERFILES = frozenset(
    {
        "apps/ui/Dockerfile",
        "examples/sre-bot/connectors/tempo/Dockerfile",
        "examples/sre-bot/connectors/self-upgrade/Dockerfile",
        "prototypes/runner/Dockerfile",
        "cli/scripts/fixtures/mcp-receipt/Dockerfile",
        "charts/curie/ci/postgres-readiness-delay.Dockerfile",
        "compose/worker-local.Dockerfile",
    }
)


def _normalize_distribution(name: str) -> str:
    """PEP 503 normalization, so ``curie_test_support`` and ``Curie-Test-Support`` match."""

    return re.sub(r"[-_.]+", "-", name).strip().lower()


def _editable_distribution(relative: str) -> str:
    """The distribution a ``-e <path>`` entry actually installs.

    Reality check, corrected after this parser first shipped: ``uv export``
    writes workspace members as PATHS (``-e ./packages/test-support``), and the
    directory basename is NOT the distribution name -- that path installs
    ``curie-test-support``. Deriving the name from the basename made every
    editable member unrecognisable, so both the harness assertion and its own
    sanity guard were looking for names that can never appear. The name is read
    from the member's ``[project] name`` instead, with the basename kept only as
    a fallback for a path that carries no pyproject.
    """

    pyproject = REPO_ROOT / relative / "pyproject.toml"
    if pyproject.is_file():
        declared = (tomllib.loads(pyproject.read_text()).get("project") or {}).get("name")
        if isinstance(declared, str) and declared:
            return _normalize_distribution(declared)
    return _normalize_distribution(Path(relative).name)


def _closure_distributions(exported: str) -> set[str]:
    """Distribution names in a ``uv export`` / pip-requirements document.

    Parses both shapes the production images install: pinned registry
    requirements (``name==1.2.3``) and workspace members exported as editable
    paths (``-e ./packages/foo``). The editable form is the one that matters
    here -- the harness would arrive that way, and a scan that only understood
    ``==`` lines would be blind to exactly the case under test.
    """

    found: set[str] = set()
    for raw in exported.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            if line.startswith("-e "):
                found.add(_editable_distribution(line[3:].strip()))
            continue
        found.add(_normalize_distribution(re.split(r"[<>=!~\[; ]", line, maxsplit=1)[0]))
    return {name for name in found if name}


def _export_closure(package: str) -> str:
    """The exact closure the image installs: ``uv export --frozen --no-dev --package <p>``."""

    completed = subprocess.run(
        ["uv", "export", "--frozen", "--no-dev", "--package", package],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "NO_COLOR": "1"},
        timeout=600,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


def _runner_pins() -> str:
    """The runner image's own pinned requirements, produced by its real exporter.

    ``runner/Dockerfile`` does not use ``uv sync`` at all: it pipes ``uv.lock``
    through ``runner/export_dependency_pins.py`` and installs the result with
    ``--no-deps``. Asserting on that exporter's actual output is the only way
    this covers the fifth image rather than assuming it resembles the other four.
    """

    completed = subprocess.run(
        [sys.executable, str(REPO_ROOT / "runner" / "export_dependency_pins.py")],
        cwd=REPO_ROOT,
        input=(REPO_ROOT / "uv.lock").read_text(),
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")
def test_no_production_dependency_closure_installs_the_harness_distribution() -> None:
    """PRIMARY control: the harness distribution is in no production closure.

    Decision 3 of the plan: assert on the resolved ``--no-dev`` closure rather
    than building five images. The four app Dockerfiles install with
    ``uv sync --frozen --no-dev --no-editable --package curie-<app>`` and the
    runner installs ``export_dependency_pins.py``'s output, so these are the
    same sets those images end up with -- not a proxy for them.

    This is deliberately an assertion about the DISTRIBUTION, not about a module
    name: it is complete against any harness surface however it is renamed,
    where the name sweep below is only complete against today's spelling.
    """

    closures = {
        package: _closure_distributions(_export_closure(package)) for package in APP_PACKAGES
    }
    closures["curie-runner"] = _closure_distributions(_runner_pins())

    for package, distributions in closures.items():
        # Guard the guard: an export that silently produced nothing would make
        # the absence assertion below pass for the wrong reason.
        assert len(distributions) > 20, (package, sorted(distributions))
        assert _normalize_distribution(package) in distributions or package == "curie-runner", (
            f"{package}'s own closure does not contain {package}; the parse is wrong, "
            "so its absence assertion proves nothing"
        )
        assert _normalize_distribution(HARNESS_DISTRIBUTION) not in distributions, (
            f"{package}'s --no-dev closure installs {HARNESS_DISTRIBUTION}; the dev "
            "interaction harness would be importable inside the production image"
        )

    # Seeded violation: the same parser, handed a closure that DOES carry the
    # harness, must report it. Without this the assertion above could be passing
    # because the parser never recognises the distribution at all.
    seeded = _closure_distributions(
        "\n".join(["-e ./apps/api", "-e ./packages/test-support", "httpx==0.27.0"])
    )
    assert _normalize_distribution(HARNESS_DISTRIBUTION) in seeded, seeded


def _harness_declaration_sites(pyproject: dict[str, Any]) -> dict[str, list[str]]:
    """Where ``curie-test-support`` is declared in a parsed pyproject document.

    Returns a map of site -> the declaring list, so a failure names the exact
    table a future author added it to rather than only saying "somewhere".
    """

    target = _normalize_distribution(HARNESS_DISTRIBUTION)

    def declares(entries: Any) -> bool:
        return isinstance(entries, list) and any(
            isinstance(entry, str)
            and _normalize_distribution(re.split(r"[<>=!~\[; ]", entry, maxsplit=1)[0]) == target
            for entry in entries
        )

    sites: dict[str, list[str]] = {}
    project = pyproject.get("project") or {}
    if declares(project.get("dependencies")):
        sites["project.dependencies"] = list(project["dependencies"])
    for extra, entries in (project.get("optional-dependencies") or {}).items():
        if declares(entries):
            sites[f"project.optional-dependencies.{extra}"] = list(entries)
    for group, entries in (pyproject.get("dependency-groups") or {}).items():
        if declares(entries):
            sites[f"dependency-groups.{group}"] = list(entries)
    return sites


def test_the_harness_distribution_is_declared_only_in_the_dev_dependency_group() -> None:
    """``--no-dev`` is sufficient ONLY while ``dev`` is the single declaration.

    The image assertion above is downstream of this one: the moment
    ``curie-test-support`` also appears in an app's ``[project] dependencies``,
    ``--no-dev`` stops excluding it and every production image gains the
    harness. A future author who adds it there fails here, at the declaration,
    with the reason attached -- rather than shipping it.

    ``[tool.uv.sources]`` and ``[tool.uv.workspace] members`` are NOT
    declarations of a dependency; they only say where the workspace member
    lives, so they are expected and are not counted as sites.
    """

    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    assert _harness_declaration_sites(root) == {
        "dependency-groups.dev": root["dependency-groups"]["dev"]
    }, "curie-test-support must be declared only in [dependency-groups] dev"

    # Every workspace member that ships in an image must not declare it either:
    # a per-app dependency would reach the image through --package resolution.
    for relative in (*APP_MEMBERS.values(), "runner"):
        member = tomllib.loads((REPO_ROOT / relative / "pyproject.toml").read_text())
        assert _harness_declaration_sites(member) == {}, (
            f"{relative}/pyproject.toml declares {HARNESS_DISTRIBUTION}; it would then be "
            "installed by --package resolution regardless of --no-dev"
        )

    # Seeded violation: a second declaration in a runtime dependency list must
    # be reported by the very helper the assertions above trust.
    seeded = dict(root)
    seeded["project"] = {
        **root["project"],
        "dependencies": [*root["project"]["dependencies"], HARNESS_DISTRIBUTION],
    }
    assert "project.dependencies" in _harness_declaration_sites(seeded)


def _dev_only_workspace_members() -> dict[str, str]:
    """Workspace members that ONLY the dev dependency group installs.

    Derived from the root pyproject rather than listed here: a member is
    dev-only when the root declares its distribution in a
    ``[dependency-groups]`` table and never in the root's runtime
    ``[project] dependencies``. Today that is the harness plus the two dev
    tools; a future dev-only member is covered without an edit.
    """

    root = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    def names(entries: Any) -> set[str]:
        return {
            _normalize_distribution(re.split(r"[<>=!~\[; ]", entry, maxsplit=1)[0])
            for entry in entries or []
            if isinstance(entry, str)
        }

    runtime = names((root.get("project") or {}).get("dependencies"))
    grouped: set[str] = set()
    for entries in (root.get("dependency-groups") or {}).values():
        grouped |= names(entries)

    members: dict[str, str] = {}
    workspace = ((root.get("tool") or {}).get("uv") or {}).get("workspace") or {}
    for relative in workspace.get("members") or []:
        pyproject = REPO_ROOT / relative / "pyproject.toml"
        if not pyproject.is_file():
            continue
        declared = (tomllib.loads(pyproject.read_text()).get("project") or {}).get("name")
        name = _normalize_distribution(declared) if isinstance(declared, str) else ""
        if name and name in grouped and name not in runtime:
            members[name] = relative
    return members


# ``uv sync --package <member>`` IS an install: it resolves that member into the
# builder venv the runtime stage ships. Leaving it out of this tuple was how a
# ``uv sync --frozen --no-dev --package curie-api --package curie-test-support``
# passed every control (round-2 review finding A).
_INSTALL_VERBS = (
    ("pip", "install"),
    ("pip3", "install"),
    ("uv", "pip", "install"),
    ("uv", "add"),
    ("poetry", "add"),
    ("uv", "sync"),
)
_SYNC_VERB = ("uv", "sync")
_SHELL_OPERATORS = frozenset({"&&", "||", ";", "|", "&", "(", ")", "{", "}"})
# Flags that put a non-runtime dependency group back into a ``--no-dev`` sync.
_DEV_GROUP_FLAGS = ("--dev", "--all-groups", "--group", "--only-group", "--only-dev")
_WORKSPACE_COPY = re.compile(r"COPY\s+\.\s+\./?$", re.IGNORECASE)


def _dockerfile_commands(text: str) -> list[str]:
    """One string per Dockerfile instruction, continuations joined, comments dropped.

    A per-LINE reading is what let finding 1 through: a shell continuation
    splits ``uv sync`` from its flags, and a line-oriented check then judges a
    fragment. Joining first is what makes "the flags of THIS instruction" a
    question the checker can actually answer.
    """

    commands: list[str] = []
    buffer = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.lstrip().startswith("#") or (not buffer and not line.strip()):
            continue
        if line.endswith("\\"):
            buffer += line[:-1] + " "
            continue
        commands.append(" ".join(f"{buffer}{line}".split()))
        buffer = ""
    if buffer.strip():
        commands.append(" ".join(buffer.split()))
    return commands


def _tokenize(command: str) -> list[str]:
    """Shell tokens of one instruction, with end-of-line comments removed.

    Round-2 finding B: ``"--no-dev" in command`` is satisfied by text uv never
    receives -- ``uv sync ... # --no-dev``. ``shlex`` with ``comments=True``
    drops what the shell drops, so flags are judged as ARGUMENTS.
    """

    try:
        return shlex.split(command, comments=True)
    except ValueError:
        return [token for token in command.split() if not token.startswith("#")]


def _segments(command: str) -> list[list[str]]:
    """The shell commands inside one ``RUN``, each as its own token list.

    Round-2 finding B again, second shape: ``uv sync ... || echo --no-dev``.
    Splitting on the shell operators is what keeps a flag belonging to `echo`
    from being read as a flag of the sync. The argv head is reduced to its
    basename (``/app/.venv/bin/pip`` -> ``pip``) and leading ``VAR=value``
    assignments and ``python -m`` are stripped, so the verb match is on the
    program actually run.
    """

    tokens = _tokenize(command)
    if not tokens or tokens[0] != "RUN":
        return []
    tokens = tokens[1:]
    while tokens and tokens[0].startswith("--mount"):
        tokens = tokens[1:]
    split: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        if token in _SHELL_OPERATORS:
            split.append(current)
            current = []
        else:
            current.append(token)
    split.append(current)
    found: list[list[str]] = []
    for segment in split:
        while segment and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", segment[0]):
            segment = segment[1:]
        if (
            len(segment) > 2
            and PurePath(segment[0]).name in ("python", "python3")
            and segment[1] == "-m"
        ):
            segment = segment[2:]
        if segment:
            found.append([PurePath(segment[0]).name, *segment[1:]])
    return found


def _starts_with(segment: list[str], verb: tuple[str, ...]) -> bool:
    return tuple(segment[: len(verb)]) == verb


def _has_flag(segment: list[str], flag: str) -> bool:
    return any(token == flag or token.startswith(f"{flag}=") for token in segment)


def _flag_value(token: str) -> str:
    """The value half of ``--flag=value``; the token itself otherwise."""

    if token.startswith("-") and "=" in token:
        return token.split("=", 1)[1]
    return token


def _names_path(token: str, target: str) -> bool:
    cleaned = _flag_value(token).strip("'\"")
    cleaned = cleaned[2:] if cleaned.startswith("./") else cleaned
    cleaned = cleaned.rstrip("/")
    target = target[2:] if target.startswith("./") else target
    target = target.rstrip("/")
    return bool(target) and (cleaned == target or cleaned.startswith(f"{target}/"))


def _names_distribution(token: str, distribution: str) -> bool:
    head = re.split(r"[<>=!~\[;]", _flag_value(token).strip("'\""), maxsplit=1)[0]
    return _normalize_distribution(head) == distribution


def _copy_aliases(commands: list[str], member: str) -> set[str]:
    """Destinations a ``COPY`` gave the dev-only member's tree.

    Round-2 finding C: ``COPY packages/test-support /opt/x`` then
    ``RUN uv pip install /opt/x`` names neither the member path nor the
    distribution, so a name match alone cannot see it. Following the COPY is
    what keeps the renamed path recognisable as the harness.
    """

    aliases: set[str] = set()
    for command in commands:
        tokens = _tokenize(command)
        if not tokens or tokens[0] != "COPY":
            continue
        operands = [token for token in tokens[1:] if not token.startswith("--")]
        if len(operands) < 2:
            continue
        for source in operands[:-1]:
            if _names_path(source, member):
                aliases.add(operands[-1])
    return aliases


def _dockerfile_posture(path: str, text: str) -> list[str]:
    """Violations of the no-dev / no-dev-member posture in one Dockerfile.

    A list rather than an assertion so the seeded-violation cases below can run
    the SAME check against mutated text; a checker that is only ever called on
    the real files cannot be shown to be capable of failing.

    Four independent controls, because ``uv export`` sees none of them. The
    app images ``COPY . .`` into the builder and ship that stage's
    ``/app/.venv``, so a single builder line can install a dev-only member into
    the shipped venv while every export-based control still reads clean:

    1. no install verb (``pip install`` / ``uv pip install`` / ``uv add`` /
       ``uv sync``) may name a dev-only workspace member -- by distribution, by
       path, or by a path a ``COPY`` renamed it to;
    2. every ``uv sync`` carries ``--no-dev`` as a real argument, and none
       re-admits a group with ``--group`` / ``--all-groups`` / ``--dev``;
    3. the FINAL sync -- the one after ``COPY . .``, the only one whose result
       is what ships -- carries ``--no-dev`` and ``--package``. "Some line has
       the flag" is not this property: the pre-copy dependency-layer sync
       already carries it, so dropping it from the post-copy sync passes any
       any-line check.
    """

    problems: list[str] = []
    commands = _dockerfile_commands(text)
    parsed = [(command, _segments(command)) for command in commands]

    for distribution, member in _dev_only_workspace_members().items():
        targets = {member, *_copy_aliases(commands, member)}
        for command, command_segments in parsed:
            for segment in command_segments:
                if not any(_starts_with(segment, verb) for verb in _INSTALL_VERBS):
                    continue
                if any(
                    any(_names_path(token, target) for target in targets)
                    or _names_distribution(token, distribution)
                    for token in segment[1:]
                ):
                    problems.append(
                        f"{path} installs the dev-only workspace member {member} "
                        f"({distribution}) into the image venv: {command}"
                    )

    if path == "runner/Dockerfile":
        if HARNESS_PACKAGE_PATH in text:
            problems.append(f"{path} references {HARNESS_PACKAGE_PATH}")
        return problems

    installs = [
        (command, segment)
        for command, command_segments in parsed
        for segment in command_segments
        if _starts_with(segment, _SYNC_VERB)
    ]
    if not installs:
        problems.append(f"{path} has no uv sync line; the posture assertion would be vacuous")
    for command, segment in installs:
        if not _has_flag(segment, "--no-dev"):
            problems.append(f"{path} installs without --no-dev: {command}")
        for flag in _DEV_GROUP_FLAGS:
            if _has_flag(segment, flag):
                problems.append(
                    f"{path} re-admits a non-runtime dependency group with {flag}: {command}"
                )

    copies = [index for index, command in enumerate(commands) if _WORKSPACE_COPY.match(command)]
    if not copies:
        problems.append(
            f"{path} has no `COPY . .`; this checker assumes the workspace-copy image shape "
            "and cannot judge the final install layer of a different one"
        )
        return problems
    final = [
        (command, segment)
        for command, command_segments in parsed[copies[-1] :]
        for segment in command_segments
        if _starts_with(segment, _SYNC_VERB)
    ]
    if not final:
        problems.append(
            f"{path} runs no uv sync after `COPY . .`; the shipped venv is then whatever "
            "an earlier layer left, which this checker cannot bound"
        )
    for command, segment in final:
        if not _has_flag(segment, "--no-dev"):
            problems.append(f"{path} post-`COPY . .` sync omits --no-dev: {command}")
        if not _has_flag(segment, "--package"):
            problems.append(
                f"{path} post-`COPY . .` sync omits --package, so it resolves the whole "
                f"workspace rather than one member: {command}"
            )
    return problems


def test_the_production_image_list_covers_every_workspace_context_image() -> None:
    """Nothing in the tree builds from the workspace without being on the list.

    ``adapters/discord/Dockerfile`` was a production recipe of exactly the app
    shape and was on no list in this module -- so "every production image" was
    an unchecked claim about a hand-maintained tuple. This re-derives the
    Dockerfile set from the tree and forces every one of them to be either a
    production workspace image (checked above) or an explicitly named
    non-workspace one.
    """

    ignored = {".git", ".venv", "node_modules", "target", ".worktrees", "dist", ".mypy_cache"}
    # Filter on the path RELATIVE to the repo root: this checkout is itself
    # under a `.worktrees/` directory, so filtering absolute parts discards
    # every file in the tree and the sweep passes vacuously.
    found = {
        str(path.relative_to(REPO_ROOT))
        for path in REPO_ROOT.rglob("*Dockerfile")
        if not ignored & set(path.relative_to(REPO_ROOT).parts)
    }
    assert set(PROD_DOCKERFILES) <= found, sorted(set(PROD_DOCKERFILES) - found)
    unaccounted = found - set(PROD_DOCKERFILES) - NON_WORKSPACE_DOCKERFILES
    assert unaccounted == set(), (
        f"{sorted(unaccounted)} is neither on the production workspace-image list nor "
        "declared as building outside the workspace; if it copies the workspace it can "
        "install the dev harness and must be added to APP_MEMBERS/PROD_DOCKERFILES"
    )


def test_every_production_image_installs_without_the_dev_dependency_group() -> None:
    """The Dockerfile posture that makes the closure assertion true in the image.

    The closure test proves ``--no-dev`` EXCLUDES the harness; this proves the
    images actually pass ``--no-dev`` -- and, since ``uv export`` cannot see a
    Dockerfile-level install at all, that no builder line puts a dev-only
    workspace member into the venv that ships.
    """

    for relative in PROD_DOCKERFILES:
        text = (REPO_ROOT / relative).read_text()
        assert _dockerfile_posture(relative, text) == [], (
            relative,
            _dockerfile_posture(relative, text),
        )

    # Seeded violation: --no-dev stripped everywhere.
    for relative in APP_DOCKERFILES:
        mutated = (REPO_ROOT / relative).read_text().replace("--no-dev", "")
        assert _dockerfile_posture(relative, mutated) != [], relative

    # Seeded violation (finding 1): --no-dev dropped from the FINAL, post-
    # `COPY . .` sync ONLY. Every earlier line keeps the flag, so an any-line
    # check reads the file as clean while the shipped venv gains the dev group.
    for relative in APP_DOCKERFILES:
        text = (REPO_ROOT / relative).read_text()
        head, sep, tail = text.rpartition("uv sync")
        assert sep, relative
        mutated = f"{head}{sep}{tail.replace(' --no-dev', '', 1)}"
        # Where the image has a pre-copy dependency-layer sync (four of the
        # five), the flag survives on that earlier line -- which is the whole
        # point: an any-line check still sees "--no-dev" here.
        if text.count("uv sync") > 1:
            assert "--no-dev" in mutated, relative
        problems = _dockerfile_posture(relative, mutated)
        assert any("post-`COPY . .` sync omits --no-dev" in problem for problem in problems), (
            relative,
            problems,
        )

    # Seeded violation (finding 1): a path- or name-install of the dev-only
    # harness in the builder. `uv export` is byte-identical under this edit and
    # every sync keeps --no-dev, so this is exactly the shape that no
    # closure-based control -- here or in the chart script -- could ever see.
    for relative in PROD_DOCKERFILES:
        for line in (
            f"RUN uv pip install ./{HARNESS_PACKAGE_PATH}",
            f"RUN pip install {HARNESS_PACKAGE_PATH}",
            f"RUN uv add {HARNESS_DISTRIBUTION}",
            f"RUN uv pip install \\\n    ./{HARNESS_PACKAGE_PATH}",
        ):
            mutated = f"{(REPO_ROOT / relative).read_text()}\n{line}\n"
            problems = _dockerfile_posture(relative, mutated)
            assert any("dev-only workspace member" in problem for problem in problems), (
                relative,
                line,
                problems,
            )

    # Seeded violation A (round-2 review): `uv sync --package <dev-only member>`.
    # `--no-dev` and `--package` are both present and `uv export --frozen
    # --no-dev --package curie-api` is byte-identical, so every other control
    # here reads clean while the member lands in the builder venv that ships.
    for relative in APP_DOCKERFILES:
        mutated = (REPO_ROOT / relative).read_text() + (
            f"\nRUN uv sync --frozen --no-dev --no-editable --package curie-api "
            f"--package {HARNESS_DISTRIBUTION}\n"
        )
        problems = _dockerfile_posture(relative, mutated)
        assert any("dev-only workspace member" in problem for problem in problems), (
            relative,
            problems,
        )

    # Seeded violation B (round-2 review): a `--no-dev` uv never receives. Both
    # shapes -- an end-of-line comment and an `|| echo` the shell only reaches
    # on failure -- satisfy a substring match on the joined instruction.
    for relative in APP_DOCKERFILES:
        text = (REPO_ROOT / relative).read_text()
        head, sep, tail = text.rpartition("uv sync")
        assert sep, relative
        # Only the REST OF THAT LINE is rewritten: appending at EOF would leave
        # the real final sync untouched and the seed would prove nothing.
        line, newline, rest = tail.partition("\n")
        for suffix in ("# --no-dev", "|| echo --no-dev"):
            mutated = f"{head}{sep}{line.replace(' --no-dev', '', 1)} {suffix}{newline}{rest}"
            # The decoy text IS on the sync line: a substring check still sees it.
            assert "--no-dev" in mutated.rpartition("uv sync")[2].partition("\n")[0], relative
            problems = _dockerfile_posture(relative, mutated)
            assert any("post-`COPY . .` sync omits --no-dev" in problem for problem in problems), (
                relative,
                suffix,
                problems,
            )

    # Seeded violation C (round-2 review): the harness copied to a renamed path
    # and installed from there. The RUN names neither `packages/test-support`
    # nor `curie-test-support`, so only following the COPY can see it.
    for relative in APP_DOCKERFILES:
        mutated = (REPO_ROOT / relative).read_text() + (
            f"\nCOPY {HARNESS_PACKAGE_PATH} /opt/vendored\nRUN uv pip install /opt/vendored\n"
        )
        assert HARNESS_DISTRIBUTION not in "RUN uv pip install /opt/vendored"
        problems = _dockerfile_posture(relative, mutated)
        assert any("dev-only workspace member" in problem for problem in problems), (
            relative,
            problems,
        )

    # Seeded violation D (found while closing A-C): `--no-dev` is present and
    # honest, and a later `--group dev` puts the dev group straight back.
    for relative in APP_DOCKERFILES:
        mutated = (REPO_ROOT / relative).read_text() + (
            "\nRUN uv sync --frozen --no-dev --group dev --no-editable --package curie-api\n"
        )
        problems = _dockerfile_posture(relative, mutated)
        assert any("re-admits a non-runtime dependency group" in problem for problem in problems), (
            relative,
            problems,
        )

    runner_mutated = (
        REPO_ROOT / "runner" / "Dockerfile"
    ).read_text() + f"\nCOPY {HARNESS_PACKAGE_PATH} ./{HARNESS_PACKAGE_PATH}\n"
    assert _dockerfile_posture("runner/Dockerfile", runner_mutated) != []


def _python_sources(*relatives: str) -> Iterator[Path]:
    for relative in relatives:
        yield from sorted((REPO_ROOT / relative).rglob("*.py"))


def _imports_harness(source: str) -> bool:
    """Does this module import ``curie_test_support`` (either import form)?

    SECONDARY to the distribution assertion above, by design: a renamed harness
    module would evade this and not the closure probe. It is kept because it
    fails at edit time, in the file that introduced the coupling, where the
    closure probe fails later and further away.
    """

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            if any(alias.name.split(".")[0] == HARNESS_MODULE for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] == HARNESS_MODULE:
                return True
    return False


def test_no_production_module_imports_the_harness() -> None:
    """No shipped source tree reaches the harness, by AST rather than by grep.

    A substring scan would hit this test file, every docstring mentioning the
    module, and nothing that matters; the AST answers the real question, which
    is whether a production module would fail to import inside an image that
    (correctly) does not install the distribution.
    """

    trees = (*(f"{member}/src" for member in APP_MEMBERS.values()), "runner/src")
    scanned = 0
    for path in _python_sources(*trees):
        scanned += 1
        assert not _imports_harness(path.read_text()), (
            f"{path.relative_to(REPO_ROOT)} imports {HARNESS_MODULE}, which no production "
            "image installs; the module would ImportError at boot"
        )
    assert scanned > 50, scanned

    # Seeded violations: both import spellings must be detected.
    assert _imports_harness(f"import {HARNESS_SUBMODULE}\n")
    assert _imports_harness(f"from {HARNESS_SUBMODULE} import harness\n")


def _harness_toggle_fields(source: str) -> list[str]:
    """Settings fields whose name or default could switch a harness on.

    The threat is not "a field named after the harness" -- it is "a field whose
    VALUE can enable it". So this looks at both: any assignment whose target
    name or whose literal default text mentions the harness, interaction
    harness, or a synthetic-principal switch.
    """

    needles = (HARNESS_MODULE, HARNESS_DISTRIBUTION, "interaction_harness", "synthetic_principal")
    found: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        targets: list[str] = []
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target.id]
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if not targets:
            continue
        rendered = f"{' '.join(targets)} {ast.unparse(node)}".lower()
        if any(needle in rendered for needle in needles):
            found.extend(targets)
    return found


def test_no_production_config_carries_a_toggle_that_could_enable_the_harness() -> None:
    """There is no production switch, so there is nothing an operator can flip.

    Decision 2 of the plan says the harness adds no production toggle. That is
    a claim about config surface, so it is asserted over the real config
    modules -- and the seeded violation proves the scan can see such a field,
    which a scan over four clean files otherwise could not demonstrate.
    """

    configs = sorted(
        {
            *REPO_ROOT.glob("apps/*/src/**/config.py"),
            *REPO_ROOT.glob("adapters/*/src/**/config.py"),
        }
    )
    assert len(configs) >= 5, [str(path) for path in configs]
    for path in configs:
        assert _harness_toggle_fields(path.read_text()) == [], path.relative_to(REPO_ROOT)

    seeded = "class Settings:\n    enable_interaction_harness: bool = False\n"
    assert _harness_toggle_fields(seeded) == ["enable_interaction_harness"]
    seeded_by_value = f'class Settings:\n    extra_module: str = "{HARNESS_SUBMODULE}"\n'
    assert _harness_toggle_fields(seeded_by_value) == ["extra_module"]


def _mint_resolve_inventory(app: Any) -> dict[str, set[str]]:
    """The live-router inventory PR2 pins, extracted so a seeded app can use it.

    PR2 asserted this inline, which made the walk itself unfalsifiable: an
    inventory that resolved nothing reads identically to "no surfaces exist".
    Sharing the function with a deliberately-violating app is what proves it
    would notice a new surface named after the harness.
    """

    targets = {
        id(approval_principal.mint): "mint",
        id(crud.claim_approval_resolution): "resolve",
    }
    inventory: dict[str, set[str]] = {}
    for route in _api_routes(app):
        reached = _reaches(route.endpoint, targets)
        if reached:
            inventory[route.endpoint.__qualname__] = reached
    return inventory


def test_a_harness_shaped_synthetic_surface_would_fail_the_live_app_inventory() -> None:
    """The inventory catches a NEW mint/resolve surface, harness-named or not.

    The production assertion (exactly one mint, one resolve) already exists
    above. What it could not show is that it would fail -- so here the same
    walk runs against an app carrying a synthetic route named after the
    harness, and must report it. If a future harness ever grew an HTTP surface
    to drive approvals, this is the assertion that stops it.
    """

    app = create_app()
    assert _mint_resolve_inventory(app) == {
        "mint_operator_principal": {"mint"},
        "resolve_approval": {"resolve"},
    }

    seeded = create_app()

    async def mint_interaction_harness_principal() -> dict[str, str]:
        """A synthetic-principal surface of exactly the shape being excluded."""
        return {
            "token": approval_principal.mint(
                "k",
                subject=SUBJECT,
                kind="chat",
                actor_channel=CARD_CHANNEL,
                approval_id="a",
                scope=approval_principal.APPROVE_SCOPE,
                exp=0,
            )
        }

    seeded.post("/curie-test-support/principals")(mint_interaction_harness_principal)
    # Reality check: the inventory is keyed by ``__qualname__``, so a handler
    # defined inside a test function is keyed ``<test>.<locals>.<name>``, never
    # by its bare name. Match the final segment -- asserting the bare name made
    # this fail even though the seeded surface WAS detected.
    reached = _mint_resolve_inventory(seeded)
    seeded_keys = [
        key for key in reached if key.split(".")[-1] == "mint_interaction_harness_principal"
    ]
    assert seeded_keys, reached
    assert reached[seeded_keys[0]] == {"mint"}, reached


def test_synthetic_action_and_identity_probes_in_prod_are_refused_and_change_nothing(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The harness's two verbs, aimed at a genuinely prod-booted app, move nothing.

    A refusal alone is not the property worth pinning -- a 403 returned AFTER
    the approval was resolved, or after an audit row was written, is the exact
    failure this test exists to catch. So the status and the full audit trail
    are captured before and compared after, and the comparison happens outside
    the prod block so it is made with the ordinary platform credential rather
    than depending on the prod key the probes ran under.

    Corrected after review: this used to reuse ``approvals_client``, whose
    ``create_app()`` had already run under the TEST environment, and merely
    clear the settings cache under ``ENVIRONMENT=prod``. Routers were never
    rebuilt, so an ``environment == "prod"`` branch in ``create_app()`` that
    mounted a synthetic mint/resolve router was invisible to this test AND to
    the dev-time live-app inventory -- which is exactly the surface AC8 claims
    does not exist. A SECOND app is now built inside the prod block, from the
    real factory under real prod settings, the mint/resolve inventory is taken
    against THAT app, and the probes are driven against it.

    What this now proves: the prod-configured router table contains exactly one
    mint surface and one resolve surface, both the production ones, and neither
    harness verb moves the row or the audit trail.
    What it still does NOT prove: anything about a surface introduced by
    something other than ``create_app()`` under these settings -- a reverse
    proxy, a sidecar, or in-process ``dependency_overrides`` (see the module
    docstring's stated bypasses). The inventory walk's own blind spot
    (indirection outside ``curie_api``, a ``getattr``-resolved target) is
    unchanged by this and is stated there too.
    """

    approval = _create_approval(approvals_client, auth_headers)
    before_status = _status(approvals_client, approval["id"], auth_headers)
    before_audit = _audit(approvals_client, approval["id"], auth_headers)
    assert before_status == "pending"
    assert before_audit == []

    with _settings_env(
        ENVIRONMENT="prod",
        API_KEY=_PLATFORM_CREDENTIAL_VALUE,
        GITHUB_WEBHOOK_SECRET=_WEBHOOK_VALUE,
        CURIE_INTERNAL_WORKER_TOKEN=_WORKER_TOKEN_VALUE,
        **{ATTESTER_ENV: _ATTESTER_VALUE},
    ):
        # The real factory, under real prod settings. `create_app()` reads
        # `get_settings()` at build time, and the cache was just cleared, so
        # every router this app mounts is the set a prod process mounts.
        prod_app = create_app()
        # Guard the guard: if the settings cache had not actually retargeted,
        # this would be a second dev app wearing a prod label.
        assert get_settings().environment == "prod"

        # Take the inventory against the PROD app, not the dev one: a
        # `if settings.environment == "prod"` router is precisely the shape the
        # dev-time inventory cannot see.
        prod_routes = list(_api_routes(prod_app))
        assert len(prod_routes) > 50, len(prod_routes)
        assert _mint_resolve_inventory(prod_app) == {
            "mint_operator_principal": {"mint"},
            "resolve_approval": {"resolve"},
        }, _mint_resolve_inventory(prod_app)

        with TestClient(prod_app) as prod_client:
            # Probe 1 -- synthetic ACTION: "approve this" with no chat principal
            # at all, the shape a harness verb would take if it ever leaked.
            action_probe = prod_client.post(
                f"/approvals/{approval['id']}/resolve",
                json={"decision": "approved", "actor": SUBJECT, "principal": HARNESS_SUBMODULE},
            )
            # Probe 2 -- synthetic IDENTITY: a principal header that is a harness
            # string rather than a minted, signed chat principal.
            identity_probe = prod_client.post(
                f"/approvals/{approval['id']}/resolve",
                json={"decision": "approved"},
                headers=_principal_headers(f"{HARNESS_DISTRIBUTION}-synthetic-principal"),
            )

    for label, response in (("action", action_probe), ("identity", identity_probe)):
        assert response.status_code in (401, 403, 422), (
            f"{label} probe: {response.status_code} {response.text}"
        )

    assert _status(approvals_client, approval["id"], auth_headers) == before_status
    assert _audit(approvals_client, approval["id"], auth_headers) == before_audit

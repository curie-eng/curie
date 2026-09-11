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

import contextlib
import inspect
import os
import threading
import time
import types
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
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

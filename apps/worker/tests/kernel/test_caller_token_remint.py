"""The kernel's deadline re-mint keeps the caller identity (ADR 0239, #3823).

When a factory run's committed execution deadline is earlier than the boot
caller token's expiry, the kernel signs a replacement capped to the deadline
and replaces the runner. ADR 0239 identity contract 2 adds ``installation`` and
``agent_id`` to the token, so the replacement must carry every identity claim
of the token the binding minted, byte for byte, and only shorten ``exp``. A
token the kernel cannot read (the legacy ``cct.`` wire, another prefix, or
garbage) is never re-minted.

Tokens are decoded here independently (split, base64url, json), not through
the proxy's verifier.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from aci_protocol import Final, SessionStatus
from curie_worker.binding import (
    CONNECTOR_CALLER_TOKEN_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
    BindingResolver,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from curie_worker.kernel import claim
from curie_worker.sandbox.types import SandboxHandle
from nacl.signing import SigningKey

from apps.worker.tests.kernel.test_work_item_workspace import (
    AGENT_ID,
    ISSUE_URL,
    _Binding,
    _NoExistingPublication,
    _turn,
    _WorkItems,
    _Workspace,
)

if TYPE_CHECKING:
    from curie_worker.kernel.core import Kernel
    from curie_worker.workitem_dispatch import WorkItemRun

# The first seed of tests/vectors/connector-caller-token.json.
_SEED = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
_SCOPED = {"connector_release": "curie", "connector_namespace": "curie"}
_AGENT_NAME = "acme-bot"
_THREAD = "work-item-thread"
# A cluster's generated id, a Kubernetes Secret UID (kept uppercase), and a
# dedicated standalone worker's blank.
_INSTALLATIONS = [
    "Zq8mX2vN7kP4tR9wL3sB6yH1cF5gJ0dA",
    "9F1C2A7E-3B4D-4E5F-8A6B-7C8D9E0F1A2B",
    "",
]
_INSTALLATION_IDS = ["generated", "secret_uid", "blank"]


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _signed(body: dict[str, object], *, prefix: str) -> str:
    """A token signed with PyNaCl directly, not through the minter under test."""

    payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("ascii")
    head = f"{prefix}.{_b64url(payload)}"
    signature = SigningKey(base64.b64decode(_SEED)).sign(head.encode("ascii")).signature
    return f"{head}.{_b64url(signature)}"


def _claims(token: str) -> dict[str, object]:
    prefix, payload, signature = token.split(".")
    assert prefix == "cct2"
    SigningKey(base64.b64decode(_SEED)).verify_key.verify(
        f"{prefix}.{payload}".encode("ascii"), _b64url_decode(signature)
    )
    parsed: dict[str, object] = json.loads(_b64url_decode(payload))
    return parsed


def _identity(claims: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in claims.items() if key != "exp"}


def _deployment(agent_id: uuid.UUID = AGENT_ID) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_name=_AGENT_NAME,
        agent_id=agent_id,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


def _config(installation: str) -> WorkerConfig:
    return WorkerConfig(
        connector_caller_signing_key=_SEED,
        installation_id=installation,
        connector_release=_SCOPED["connector_release"],
        connector_namespace=_SCOPED["connector_namespace"],
    )


def _binding_boot_env(
    config: WorkerConfig, *, run: uuid.UUID, work_item: uuid.UUID, ceiling: int | None = None
) -> dict[str, str]:
    """The boot env the real binding renders, caller token and all."""

    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = config
    return resolver.boot_env(
        _deployment(),
        _THREAD,
        caller_run=str(run),
        caller_work_item=str(work_item),
        caller_exp_ceiling=ceiling,
    )


def _handle(name: str) -> SandboxHandle:
    return SandboxHandle(
        thread_key=_THREAD,
        claim_name=f"claim-{name}",
        sandbox_name=f"sandbox-{name}",
        namespace="curie",
        service_fqdn="sandbox.curie.svc",
        port=8080,
        session_id="session",
        carries_caller_token=True,
    )


class _KernelSlice:
    """The two members ``_cap_caller_token_to_deadline`` reads off the kernel."""

    def __init__(self, config: WorkerConfig) -> None:
        self._config = config
        self.claims: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self.replacement = _handle("replacement")

    async def _claim_or_resume(
        self, thread_key: str, boot_env: dict[str, str], **kwargs: Any
    ) -> SandboxHandle:
        self.claims.append((thread_key, dict(boot_env), kwargs))
        return self.replacement


def _run(*, request_id: uuid.UUID, work_item: uuid.UUID, deadline: datetime) -> SimpleNamespace:
    # The WorkItemRun members the re-mint reads and writes; a real run needs a
    # dispatch client and starts a renew task on construction.
    return SimpleNamespace(
        request_id=request_id,
        work_item_id=work_item,
        agent_id=AGENT_ID,
        execution_deadline=deadline,
        claim_name="claim-original",
        sandbox_name="sandbox-original",
    )


async def _cap(
    kernel: _KernelSlice, boot_env: dict[str, str], handle: SandboxHandle, run: SimpleNamespace
) -> SandboxHandle:
    return await claim._cap_caller_token_to_deadline(
        cast("Kernel", kernel),
        _THREAD,
        boot_env,
        handle,
        cast("WorkItemRun", run),
        agent_name=_AGENT_NAME,
        workspace_deployment_id=None,
        workspace_repo=None,
        lineage_branch=None,
        lineage_head=None,
        lineage_base_sha=None,
        publication_visible_outcome_revision=None,
        runner_resources=None,
        remaining_s=None,
    )


# --- _caller_token_exp ---------------------------------------------------------


def test_the_exp_of_a_cct2_token_is_read() -> None:
    body: dict[str, object] = {
        "agent": _AGENT_NAME,
        "agent_id": str(AGENT_ID),
        "exp": 1790000000,
        "installation": _INSTALLATIONS[0],
    }
    assert claim._caller_token_exp(_signed(body, prefix="cct2")) == 1790000000


@pytest.mark.parametrize(
    "token",
    [
        _signed({"agent": _AGENT_NAME, "exp": 1790000000}, prefix="cct"),
        _signed(
            {
                "agent": _AGENT_NAME,
                "agent_id": str(AGENT_ID),
                "exp": 1790000000,
                "installation": _INSTALLATIONS[0],
            },
            prefix="cct",
        ),
        _signed(
            {
                "agent": _AGENT_NAME,
                "agent_id": str(AGENT_ID),
                "exp": 1790000000,
                "installation": _INSTALLATIONS[0],
            },
            prefix="cct3",
        ),
        "cct2.payload.signature",
        "not a token",
        "",
    ],
    ids=["legacy_v1", "legacy_prefix_v2_claims", "cct3", "cct2_garbage", "garbage", "empty"],
)
def test_a_token_that_is_not_cct2_is_unreadable(token: str) -> None:
    assert claim._caller_token_exp(token) is None


# --- _cap_caller_token_to_deadline ---------------------------------------------


# @spec ADR-0239
@pytest.mark.parametrize("installation", _INSTALLATIONS, ids=_INSTALLATION_IDS)
def test_a_remint_keeps_every_identity_claim_and_only_shortens_exp(installation: str) -> None:
    config = _config(installation)
    request_id, work_item = uuid.uuid4(), uuid.uuid4()
    boot_env = _binding_boot_env(config, run=request_id, work_item=work_item)
    original = boot_env[CONNECTOR_CALLER_TOKEN_ENV]
    original_claims = _claims(original)
    # The boot token the binding minted: the 24 hour cap, no deadline yet.
    assert original_claims["installation"] == installation
    assert original_claims["agent_id"] == str(AGENT_ID)
    assert original_claims["agent"] == _AGENT_NAME
    assert original_claims["run"] == str(request_id)
    assert original_claims["work_item"] == str(work_item)

    deadline = datetime.now(UTC) + timedelta(hours=1)
    deadline_unix = int(deadline.timestamp())
    kernel = _KernelSlice(config)
    handle = _handle("original")
    run = _run(request_id=request_id, work_item=work_item, deadline=deadline)
    before = int(time.time())

    replaced = asyncio.run(_cap(kernel, boot_env, handle, run))
    after = int(time.time())

    reminted = boot_env[CONNECTOR_CALLER_TOKEN_ENV]
    assert reminted != original
    claims = _claims(reminted)
    assert _identity(claims) == _identity(original_claims)
    assert claims["installation"] == installation
    exp = claims["exp"]
    original_exp = original_claims["exp"]
    assert type(exp) is int and type(original_exp) is int
    assert exp <= deadline_unix < original_exp
    assert min(before + SANDBOX_TOKEN_TTL_SECONDS, deadline_unix) <= exp
    assert exp <= min(after + SANDBOX_TOKEN_TTL_SECONDS, deadline_unix)

    [(thread_key, claimed_env, kwargs)] = kernel.claims
    assert thread_key == _THREAD
    assert claimed_env[CONNECTOR_CALLER_TOKEN_ENV] == reminted
    assert kwargs["replace_handle"] is handle
    assert kwargs["caller_run"] == str(request_id)
    assert kwargs["agent_name"] == _AGENT_NAME
    assert replaced is kernel.replacement
    assert run.claim_name == kernel.replacement.claim_name
    assert run.sandbox_name == kernel.replacement.sandbox_name


@pytest.mark.parametrize("margin", [0, 60], ids=["at_the_deadline", "under_the_deadline"])
def test_a_token_already_within_the_deadline_is_not_reminted(margin: int) -> None:
    config = _config(_INSTALLATIONS[1])
    request_id, work_item = uuid.uuid4(), uuid.uuid4()
    deadline = datetime.now(UTC) + timedelta(hours=1)
    deadline_unix = int(deadline.timestamp())
    boot_env = _binding_boot_env(
        config, run=request_id, work_item=work_item, ceiling=deadline_unix - margin
    )
    before = dict(boot_env)
    assert _claims(boot_env[CONNECTOR_CALLER_TOKEN_ENV])["exp"] == deadline_unix - margin
    kernel = _KernelSlice(config)
    handle = _handle("original")
    run = _run(request_id=request_id, work_item=work_item, deadline=deadline)

    assert asyncio.run(_cap(kernel, boot_env, handle, run)) is handle
    assert boot_env == before
    assert kernel.claims == []
    assert run.claim_name == "claim-original"


@pytest.mark.parametrize(
    "token",
    [
        _signed({"agent": _AGENT_NAME, "exp": 4102444800}, prefix="cct"),
        "cct2.payload.signature",
        "not a token",
    ],
    ids=["legacy_cct", "cct2_garbage", "garbage"],
)
def test_an_unreadable_token_is_never_reminted(token: str) -> None:
    # A far-future legacy exp: the old reader would have re-minted it.
    config = _config(_INSTALLATIONS[0])
    boot_env = {CONNECTOR_CALLER_TOKEN_ENV: token, "CURIE_SESSION_ID": "session"}
    before = dict(boot_env)
    kernel = _KernelSlice(config)
    handle = _handle("original")
    run = _run(
        request_id=uuid.uuid4(),
        work_item=uuid.uuid4(),
        deadline=datetime.now(UTC) + timedelta(hours=1),
    )

    assert asyncio.run(_cap(kernel, boot_env, handle, run)) is handle
    assert boot_env == before
    assert kernel.claims == []


# --- Driven through the kernel (real Valkey, fake runner and K8s client) -------


class _DeadlineBinding(_Binding):
    """The workspace test's binding, with the caller token from the real binding."""

    def __init__(self, config: WorkerConfig) -> None:
        self._resolver = BindingResolver.__new__(BindingResolver)
        self._resolver._config = config.model_copy(update=_SCOPED)
        # Each caller token as issued. The kernel writes a re-minted token back
        # onto the same boot env dict, so the claim's env cannot show the original.
        self.issued: list[str] = []

    def boot_env(self, _resolved: object, thread_key: str, **kwargs: object) -> dict[str, str]:
        env = super().boot_env(_resolved, thread_key, **kwargs)
        rendered = self._resolver.boot_env(
            _deployment(),
            thread_key,
            caller_run=cast("str | None", kwargs.get("caller_run")),
            caller_work_item=cast("str | None", kwargs.get("caller_work_item")),
            caller_exp_ceiling=cast("int | None", kwargs.get("caller_exp_ceiling")),
        )
        env[CONNECTOR_CALLER_TOKEN_ENV] = rendered[CONNECTOR_CALLER_TOKEN_ENV]
        self.issued.append(rendered[CONNECTOR_CALLER_TOKEN_ENV])
        return env

    async def execution_deadline_seconds_for(self, _agent_id: uuid.UUID) -> int:
        # Two hours at boot; the API commits one hour at start, so the boot
        # token outlives the committed deadline and the kernel must re-mint.
        return 7200


# @spec ADR-0239
@pytest.mark.parametrize("installation", _INSTALLATIONS[:2], ids=_INSTALLATION_IDS[:2])
def test_a_factory_run_replaces_its_runner_with_the_same_identity_capped_to_the_deadline(
    make_harness: Callable[..., contextlib.AbstractAsyncContextManager[Any]],
    installation: str,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding_factory=_DeadlineBinding,
            workspace_factory=_Workspace,
            publication_creator=_NoExistingPublication(),
            connector_caller_signing_key=_SEED,
            installation_id=installation,
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = [env or {} for env in h.fake_k8s.claim_envs]
            assert len(envs) == 2, "the boot runner is replaced once, at start"
            binding = cast(_DeadlineBinding, h.kernel._binding)
            boot = binding.issued[0]
            replacement = envs[1][CONNECTOR_CALLER_TOKEN_ENV]
            assert replacement != boot
            boot_claims, replacement_claims = _claims(boot), _claims(replacement)
            assert _identity(replacement_claims) == _identity(boot_claims)
            assert replacement_claims["installation"] == installation
            assert replacement_claims["agent_id"] == str(AGENT_ID)
            assert replacement_claims["agent"] == _AGENT_NAME
            assert replacement_claims["run"] == str(request_id)
            assert replacement_claims["work_item"] == str(
                work_items.acquisitions[request_id].work_item_id
            )
            deadline = int(work_items.starts[request_id].execution_deadline.timestamp())
            exp, boot_exp = replacement_claims["exp"], boot_claims["exp"]
            assert type(exp) is int and type(boot_exp) is int
            assert exp <= deadline < boot_exp

    asyncio.run(exercise())

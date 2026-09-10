"""The inbound-attachment lane is WIRED into the production worker (#2567, S4).

S1-S3 built the lane and left it unreachable: ``Kernel.__init__`` accepts an
``attachments=`` keyword, ``run.build`` never passes it, and every unit test in
``test_attachments.py`` / ``test_attachment_retention.py`` still passes because
they construct the coordinator themselves.  That is the failure mode this file
exists for -- a feature whose whole test suite is green while no deployment can
reach it.  So these tests drive the REAL ``run.build`` and read what the kernel
was actually handed.

``build`` is drivable here even though ``test_run.py`` says it is not: the one
thing in it that touches the world is ``KubernetesSandboxClient.__init__``,
which loads a kubeconfig, and ``_sandbox_client`` is the seam that produces it.
Stub that one function and the rest of ``build`` is pure construction -- boto3
clients, aiohttp/httpx sessions and a SQLAlchemy engine all connect lazily. The
kernel is spied rather than replaced, so the assertions read a real ``Kernel``
built from real config and not a mock's recorded call.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from aci_protocol import Attachment
from curie_worker import run
from curie_worker.attachments import (
    AttachmentCoordinator,
    AttachmentLimits,
    AttachmentRef,
    encode_attachment_refs,
)
from curie_worker.config import WorkerConfig
from curie_worker.workspace import WorkspaceObjectStore

# A non-secret placeholder. The lane only needs a bot token to be PRESENT --
# nothing in build() calls Slack -- so keep it an obvious placeholder behind a
# named constant, as test_run.py does for the model credential.
_FAKE_BOT_TOKEN = "xoxb-PLACEHOLDER"


class _NoS3Client:
    """Stands in for every boto3 S3 client ``build`` would construct.

    Not an optimization. ``boto3.client`` resolves credentials through the
    module-level default session, which CACHES the result process-wide, so a
    real construction here would resolve the developer's ambient identity (an
    SSO profile, say) and then hand that cached credential to
    ``test_credential_resolution.py``, whose whole subject is which provider the
    chain reaches. That test would fail with ``sso`` instead of
    ``assume-role-with-web-identity`` and blame the worker's config.

    Nothing in ``build`` calls the client, and which identity signs is not this
    file's subject -- ``test_credential_resolution.py`` and
    ``charts/curie/ci/worker-object-store-assertions.sh`` own that from both
    ends.
    """


class _KernelSpy:
    """Records build()'s kwargs and still constructs the real kernel.

    Not a mock: a mock would let the lane be "wired" to something the real
    ``Kernel.__init__`` would reject, and would not prove the kernel ends up
    holding it.
    """

    instances: list[_KernelSpy] = []
    #: The real ``Kernel`` class, installed by the fixture below.
    real: Any = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.inner = _KernelSpy.real(**kwargs)
        _KernelSpy.instances.append(self)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> Any:
    """A factory returning the kwargs ``run.build`` handed the kernel.

    Everything is disposed inside the same event loop that created it, so a
    leaked aiohttp/httpx session cannot leak into another test's warnings.
    """

    monkeypatch.setattr(_KernelSpy, "real", run.Kernel)
    monkeypatch.setattr(run, "Kernel", _KernelSpy)
    # See _NoS3Client: a real client construction here leaks a cached ambient
    # credential into every later test that reads the provider chain.
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: _NoS3Client())
    # The calls in build() that reach outside the process, both of which load a
    # kubeconfig at __init__ and so must be stubbed for this to be hermetic.
    # A developer box usually HAS a kubeconfig, which is why leaving either of
    # these live passes locally and fails in CI with "Invalid kube-config file"
    # -- a machine-shaped green, and exactly how this test first shipped broken.
    monkeypatch.setattr(run, "_sandbox_client", lambda config, env, sub: object())
    # `_build_publication_loop` constructs a KubernetesPublicationCluster; its
    # own docstring says it is built at boot precisely so a bad kubeconfig fails
    # here. `None` is a value it legitimately returns when publication is off,
    # so stubbing it changes nothing this test asserts -- the attachment lane is
    # built from config and Slack credentials, never from the publication path.
    monkeypatch.setattr(run, "_build_publication_loop", lambda *_a, **_k: None)

    def _build(**config_overrides: Any) -> dict[str, Any]:
        _KernelSpy.instances.clear()

        async def _drive() -> dict[str, Any]:
            config = WorkerConfig(**config_overrides)
            runtime = run.build(config, {})
            try:
                assert len(_KernelSpy.instances) == 1
                return _KernelSpy.instances[0].kwargs
            finally:
                await runtime.runner.close()
                await runtime.eval_http.aclose()
                await runtime.async_redis.aclose()
                await runtime.eval_redis.aclose()
                await runtime.engine.dispose()

        return asyncio.run(_drive())

    return _build


def _lane(kwargs: dict[str, Any]) -> AttachmentCoordinator:
    lane = kwargs.get("attachments")
    assert lane is not None, (
        "run.build did not pass attachments= to the kernel. The whole inbound "
        "attachment lane is then dead code: Kernel._attachments stays None, "
        "_resolve_attachments is never called, and every attachment test still "
        "passes because they construct the coordinator themselves."
    )
    assert isinstance(lane, AttachmentCoordinator)
    return lane


# --- the lane reaches the kernel -------------------------------------------


def test_build_hands_the_kernel_an_attachment_coordinator(built: Any) -> None:
    # THE test for S4. revert: drop `attachments=` from the Kernel(...) call in
    # run.build -> this fails and nothing else in the suite does.
    assert isinstance(_lane(built(slack_bot_token=_FAKE_BOT_TOKEN)), AttachmentCoordinator)


def test_the_kernel_actually_holds_the_lane_it_was_handed(built: Any) -> None:
    # Passing the keyword is not enough: `attachments=None` would satisfy the
    # signature and leave the turn path untouched. Read the constructed kernel's
    # own state, which is what _handle_event branches on.
    kwargs = built(slack_bot_token=_FAKE_BOT_TOKEN)
    kernel = _KernelSpy.instances[0].inner
    assert kernel._attachments is _lane(kwargs)  # noqa: SLF001 -- the wiring IS the subject


def test_the_lane_downloads_through_the_bot_token_and_parks_in_the_private_store(
    built: Any,
) -> None:
    # The two ports that make the lane useful rather than merely present: the
    # channel download (the worker's bot token, ADR-0075) and the PRIVATE
    # object store. Wiring it to the public bundle bucket would publish every
    # inbound file to anything holding a bundle URL.
    lane = _lane(built(slack_bot_token=_FAKE_BOT_TOKEN, workspace_bucket="curie-workspaces"))
    assert lane.files is not None
    assert isinstance(lane.objects, WorkspaceObjectStore)
    assert lane.objects._bucket == "curie-workspaces"  # noqa: SLF001
    assert lane.objects._prefix.startswith("private/")  # noqa: SLF001


def test_no_bot_token_leaves_the_lane_unwired_rather_than_half_wired(built: Any) -> None:
    # The lane cannot resolve anything without the channel credential, and the
    # kernel treats a wired lane as authoritative. A deployment with no bot
    # token (compose smoke, a mail-only install) must run every turn exactly as
    # it does today rather than failing on the first message with a file.
    assert built(slack_bot_token="").get("attachments") is None


# --- the configured envelope reaches the lane ------------------------------


def test_the_configured_limits_reach_the_wired_coordinator(built: Any) -> None:
    # A cap in WorkerConfig that the coordinator never receives is the same
    # dead wiring one level down: the operator's value changes nothing.
    lane = _lane(
        built(
            slack_bot_token=_FAKE_BOT_TOKEN,
            attachment_max_file_bytes=7 * 1024 * 1024,
            attachment_reference_ttl_seconds=120,
            attachment_retention_ttl_seconds=900,
        )
    )
    assert lane.limits.max_file_bytes == 7 * 1024 * 1024
    assert lane.limits.reference_ttl_seconds == 120
    assert lane.limits.retention_ttl_seconds == 900


def test_an_unconfigured_deployment_gets_the_lane_defaults(built: Any) -> None:
    # The shipped envelope, read off the lane the kernel holds rather than off
    # the dataclass, so a wiring that quietly substitutes its own numbers fails.
    lane = _lane(built(slack_bot_token=_FAKE_BOT_TOKEN))
    default = AttachmentLimits()
    assert lane.limits.max_file_bytes == default.max_file_bytes
    assert lane.limits.reference_ttl_seconds == default.reference_ttl_seconds
    assert lane.limits.retention_ttl_seconds == default.retention_ttl_seconds


# --- the resolved turn's capability shape is the chart's contract ----------


def test_the_encoded_reference_field_names_are_the_chart_contract() -> None:
    """The init container decodes this by hand; the names are a cross-language seam.

    ``charts/curie/ci/attachment-init-behavior-assertions.sh`` builds a fixture
    reference with these exact keys because a chart CI script must not import
    the worker package. Renaming a key here without changing the init container
    makes every real attachment fail to decode inside the pod, where the only
    symptom is an init-container crash loop. Pinned on both sides so one side
    cannot move alone.
    """

    ref = AttachmentRef(
        name="report.csv",
        url="https://store.example/object",
        sha256="a" * 64,
        size_bytes=11,
        expires_at_epoch=2_000_000_000,
        mime_type="text/csv",
    )
    encoded = encode_attachment_refs((ref,))
    entries = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))

    assert entries == [
        {
            "n": "report.csv",
            "u": "https://store.example/object",
            "s": "a" * 64,
            "b": 11,
            "e": 2_000_000_000,
            "m": "text/csv",
        }
    ]


def test_a_resolved_turn_delivers_the_capability_under_the_init_containers_key(
    built: Any,
) -> None:
    # The last link: the lane the kernel holds must contribute the env key the
    # k8s substrate scopes to attachments-init. Driven through the WIRED lane's
    # own resolve so a build that hands over a differently-configured
    # coordinator cannot pass.
    lane = _lane(built(slack_bot_token=_FAKE_BOT_TOKEN))

    class _Objects:
        def put_stream(self, key: str, chunks: Any) -> None:
            for _ in chunks:
                pass

        def presign_get(self, key: str, *, expires_seconds: int) -> str:
            return f"https://store.example/{key}"

    class _Files:
        def fetch(self, file_id: str) -> Iterator[bytes]:
            yield b"hello"

    lane.files = _Files()  # type: ignore[assignment]
    lane.objects = _Objects()  # type: ignore[assignment]

    prepared = lane.resolve(
        thread_key="C1:1.0",
        agent_id="11111111-1111-1111-1111-111111111111",
        attachments=[Attachment(id="F1", name="report.csv")],
    )

    assert list(prepared.claim_env()) == ["CURIE_ATTACHMENTS_REF"]

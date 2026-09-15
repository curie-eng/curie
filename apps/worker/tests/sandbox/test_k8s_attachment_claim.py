"""The attachment capability's place in the emitted SandboxClaim env (#2567, S3).

Its own file rather than an addition to ``test_k8s_claim.py``: the constants
under test do not exist yet, so a module-level import of them there would turn
that file's whole (green) suite into one collection error.

The rule this pins is the same one ``test_k8s_claim.py`` pins for the bundle and
workspace refs. The agent-sandbox controller injects per-claim env with no
``containerName`` into only the FIRST MAIN container, so an init container sees a
value only when the entry names it -- and the runner sees any UNNAMED entry. The
attachment reference must therefore be named at the attachment init container and
must NOT appear unnamed, for two separate reasons:

1. Without the named entry the init container has no capability and the runner
   boots an empty attachment dir -- the file silently never arrives.
2. With an unnamed entry the presigned URL is also handed to the model's own
   process, which is a capability the agent has no need for and, being a plain
   env var, one it could echo into a channel.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest
from curie_worker.sandbox.k8s import (
    BUNDLE_INIT_CONTAINERS,
    WORKSPACE_INIT_CONTAINERS,
    KubernetesSandboxClient,
)

ATTACHMENT_REF_VALUE = "opaque-presigned-attachment-reference"


class _FakeApi:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []

    def create_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, body: dict[str, Any]
    ) -> None:
        self.created.append(body)


def _client(api: _FakeApi) -> KubernetesSandboxClient:
    client = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    client._api = api  # type: ignore[attr-defined]
    client._namespace = "test-ns"  # type: ignore[attr-defined]
    return client


def _new_symbol(name: str) -> Any:
    """The not-yet-added k8s constant, or a failure that names what is missing."""

    module = importlib.import_module("curie_worker.sandbox.k8s")
    try:
        return getattr(module, name)
    except AttributeError as exc:  # pragma: no cover - the red state
        pytest.fail(f"curie_worker.sandbox.k8s does not define {name} yet: {exc}")


def test_the_attachment_reference_targets_only_the_attachment_init_container() -> None:
    attachments_ref_env = _new_symbol("ATTACHMENTS_REF_ENV")
    attachment_init_containers = _new_symbol("ATTACHMENT_INIT_CONTAINERS")
    assert attachment_init_containers, "the attachment lane needs at least one init container"

    api = _FakeApi()
    _client(api).create_claim(
        "claim-attachments",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            attachments_ref_env: ATTACHMENT_REF_VALUE,
        },
    )
    entries = api.created[0]["spec"]["env"]

    unnamed = {entry["name"] for entry in entries if "containerName" not in entry}
    assert attachments_ref_env not in unnamed, (
        "the runner container must not receive the attachment capability"
    )
    assert "CURIE_BUDGET" in unnamed, "the rest of the boot env is still written"

    named = {
        (entry["containerName"], entry["name"]): entry["value"]
        for entry in entries
        if "containerName" in entry
    }
    for container in attachment_init_containers:
        assert named[(container, attachments_ref_env)] == ATTACHMENT_REF_VALUE

    # And nowhere else. The workspace and bundle init containers fetch different
    # object classes with their own capabilities; handing them this one would
    # widen three containers' reach for the benefit of none.
    for container in (*WORKSPACE_INIT_CONTAINERS, *BUNDLE_INIT_CONTAINERS):
        if container in attachment_init_containers:
            continue
        assert (container, attachments_ref_env) not in named


def test_a_claim_with_no_attachments_emits_no_named_attachment_env() -> None:
    """The regression guard: today's turn shape is untouched.

    A turn carrying no files must produce exactly the claim it produces now --
    no extra entry, and in particular no empty-valued one, which would make an
    init container think it had work to do.
    """

    attachments_ref_env = _new_symbol("ATTACHMENTS_REF_ENV")

    api = _FakeApi()
    _client(api).create_claim(
        "claim-plain", pool="pool", env={"CURIE_BUDGET": "{}", "CURIE_SESSION_ID": "s"}
    )
    entries = api.created[0]["spec"]["env"]

    assert entries
    assert all(entry["name"] != attachments_ref_env for entry in entries)
    assert all("containerName" not in entry for entry in entries)

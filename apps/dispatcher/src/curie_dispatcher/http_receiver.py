"""Selected native Slack intake. @spec HTTP-INVOCATION-EXECUTION-1/2/3/19."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Annotated, Any, Literal, Never, Self, SupportsIndex
from urllib.parse import urlsplit

import httpx
from curie_internal import identity_approval as approval_wire
from curie_internal import identity_context as codec
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator
from slack_sdk.web import WebClient

from .approval_actions import ResolveOutcome, parse_decision_time
from .approval_principal import mint_chat_principal
from .identities import bot_ids_from_auth_test
from .identity import slack_evidence
from .inbound_attachments import derive_attachments
from .inbound_text import derive_text, strip_self_mention
from .installation_report import auth_test_report
from .relevance import NON_CONTENT_SUBTYPES


class _Frozen(BaseModel):
    """@spec HTTP-INVOCATION-EXECUTION-19."""

    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", revalidate_instances="always",
    )

    def __bool__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise TypeError("receiver_state_is_not_permission")


def _credential(value: object) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    if type(value) is not str or not 1 <= len(value) <= 16_384:
        raise ValueError("invalid_receiver_credential")
    value.encode("ascii")
    return value


class HttpReceiverScopeV1(_Frozen):
    """@spec HTTP-INVOCATION-EXECUTION-19."""

    schema_version: codec.Version
    receiver_id: codec.CanonicalUUID
    receiver_generation: codec.Generation
    channel_identity_id: codec.CanonicalUUID
    channel_identity_name: codec.Identifier
    provider_installation_id: codec.CanonicalUUID
    namespace: codec.ContextNamespaceV1
    installation_account: codec.Identifier
    bot_user_id: codec.Identifier

    @property
    def grid(self) -> bool:
        """A workspace installation inside an Enterprise Grid organisation (ADR 0252).

        @spec HTTP-INVOCATION-EXECUTION-21.
        """
        return self.namespace.kind == "slack_enterprise"

    @model_validator(mode="after")
    def ordinary_workspace(self) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-19/21."""
        namespace, account = self.namespace, self.installation_account
        ordinary = namespace.kind == "slack_workspace" and namespace.key == account
        grid = namespace.kind == "slack_enterprise" and namespace.key != account
        if namespace.authority != "" or not (ordinary or grid):
            raise ValueError("invalid_http_receiver_scope")
        return self


class HttpReceiverBindingV1(_Frozen):
    """@spec HTTP-INVOCATION-EXECUTION-19."""

    schema_version: codec.Version
    native_channel_id: codec.Identifier
    binding_id: codec.CanonicalUUID
    binding_generation: codec.ChannelGeneration
    channel_credential: Annotated[str, BeforeValidator(_credential)] = Field(repr=False)


class ReceiverSubmission(_Frozen):
    """@spec HTTP-INVOCATION-EXECUTION-19."""

    status: Literal["accepted", "pending", "refused", "unavailable"]
    stream_id: codec.Identifier | None


class _TurnAccepted(_Frozen):
    """The existing API receipt, not a second turn. @spec HTTP-INVOCATION-EXECUTION-19."""

    event_id: codec.Identifier
    stream_id: codec.Identifier | None
    duplicate: bool


_REFUSED = ReceiverSubmission(status="refused", stream_id=None)
_UNAVAILABLE = ReceiverSubmission(status="unavailable", stream_id=None)
_AMBIGUOUS = (
    httpx.ReadTimeout, httpx.ReadError, httpx.WriteTimeout, httpx.WriteError,
    httpx.RemoteProtocolError,
)


def _qualified(scope: HttpReceiverScopeV1, client: WebClient) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/19/21."""
    if not isinstance(client, WebClient):
        raise ValueError("invalid_http_receiver_runtime")
    observed = client.auth_test()
    report = auth_test_report(scope.channel_identity_name, observed)
    ids = bot_ids_from_auth_test(observed)
    # A Grid workspace install reports its organisation; an ordinary one none.
    organisation = scope.namespace.key if scope.grid else None
    if (
        report is None or ids is None or report["team_id"] != scope.installation_account
        or ids.bot_user_id != scope.bot_user_id or report["enterprise_id"] != organisation
        or report["is_enterprise_install"] is not False
    ):
        raise ValueError("invalid_http_receiver_runtime")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("invalid_receiver_receipt")
        result[key] = value
    return result


def _constant(value: str) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    raise ValueError("invalid_receiver_receipt")


async def _receipt(response: httpx.Response) -> ReceiverSubmission:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    if response.status_code in {401, 403, 409, 413, 422}:
        return _REFUSED
    if response.status_code not in {200, 202}:
        return _UNAVAILABLE
    chunks, length = [], 0
    async for chunk in response.aiter_bytes():
        length += len(chunk)
        if length > 65_536:
            return _UNAVAILABLE
        chunks.append(chunk)
    try:
        value = json.loads(
            b"".join(chunks).decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant,
        )
        receipt = _TurnAccepted.model_validate(value)
        if response.status_code == 202:
            return (
                ReceiverSubmission(status="pending", stream_id=None)
                if receipt.stream_id is None and receipt.duplicate is True else _UNAVAILABLE
            )
        if receipt.stream_id is None or re.fullmatch(r"[0-9]+-[0-9]+", receipt.stream_id) is None:
            return _UNAVAILABLE
        return ReceiverSubmission(status="accepted", stream_id=receipt.stream_id)
    except (ValueError, TypeError, RecursionError):
        return _UNAVAILABLE


# A refused answer sends nothing; an unknown one is never retried or resolved
# another way. Both render as the existing generic failure.
_ANSWER_REFUSED = ResolveOutcome(status_code=401)
_ANSWER_UNKNOWN = ResolveOutcome(status_code=0)
_INTERACTIONS = frozenset({"block_actions", "view_submission"})
# The API's fixed refusal codes, worded for the person who clicked. A code
# outside this table renders the legacy class-neutral refusal, never itself.
# @spec HTTP-APPROVAL-CONTINUITY-7.
_REFUSAL_WORDING = {
    (403, "caller_not_allowed"): "You are not allowed to answer this approval.",
    (403, "unsupported_approval"): "This approval can't be answered here.",
    (503, "authority_unavailable"): (
        "Couldn't confirm your permission right now. Try again in a moment."
    ),
}


async def _answer_outcome(response: httpx.Response) -> ResolveOutcome:
    """The bounded resolve verdict, strictly decoded. @spec HTTP-APPROVAL-CONTINUITY-7."""
    chunks, length = [], 0
    async for chunk in response.aiter_bytes():
        length += len(chunk)
        if length > 65_536:
            return _ANSWER_UNKNOWN
        chunks.append(chunk)
    try:
        value = json.loads(
            b"".join(chunks).decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant,
        )
    except (ValueError, TypeError, RecursionError):
        value = None
    if type(value) is not dict:
        return _ANSWER_UNKNOWN if response.status_code == 200 else ResolveOutcome(
            status_code=response.status_code,
        )
    detail, resolved_by, status = value.get("detail"), value.get("resolved_by"), value.get("status")
    if response.status_code in {403, 503}:
        detail = _REFUSAL_WORDING.get((response.status_code, str(detail)), "")
    if response.status_code == 200 and status not in {"approved", "rejected"}:
        return _ANSWER_UNKNOWN
    return ResolveOutcome(
        status_code=response.status_code,
        detail=detail if type(detail) is str else "",
        resolved_by=resolved_by if type(resolved_by) is str and resolved_by else None,
        decision=status if type(status) is str else None,
        resolved_at=parse_decision_time(value.get("resolved_at")),
    )


class HttpReceiverRuntime:
    """Opaque operator initialization. @spec HTTP-INVOCATION-EXECUTION-1/19."""

    __slots__ = ("_scope", "_bindings", "_keys", "_url", "_api_base", "_client_factory")
    _scope: HttpReceiverScopeV1
    _bindings: Mapping[str, HttpReceiverBindingV1]
    _keys: codec.IdentityKeyring
    _url: str
    _api_base: str
    _client_factory: Callable[[], httpx.AsyncClient]

    def __new__(cls, *args: object, **kwargs: object) -> Self:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise TypeError("http_receiver_runtime_not_public")

    def __setattr__(self, name: str, value: object) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise AttributeError("immutable_http_receiver_runtime")

    def __delattr__(self, name: str) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise AttributeError("immutable_http_receiver_runtime")

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise TypeError("http_receiver_runtime_not_serializable")

    def __getstate__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise TypeError("http_receiver_runtime_not_serializable")

    def __repr__(self) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        return "HttpReceiverRuntime()"

    def __bool__(self) -> Never:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        raise TypeError("http_receiver_runtime_is_not_permission")

    @property
    def identity_name(self) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        return self._scope.channel_identity_name

    def validate_connection(self, identity_name: str, web_client: WebClient) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        try:
            if identity_name != self.identity_name:
                raise ValueError
            _qualified(self._scope, web_client)
        except Exception:  # noqa: BLE001 - startup errors exclude provider and secret bytes.
            raise ValueError("invalid_http_receiver_runtime") from None

    def submit(
        self, *, body: dict[str, Any], event: dict[str, Any], slack_identity: str,
        bot_user_id: str | None,
    ) -> ReceiverSubmission:
        """@spec HTTP-INVOCATION-EXECUTION-2/3/19."""
        scope = self._scope
        try:
            if (
                type(body) is not dict or type(event) is not dict
                or body.get("event") != event or body.get("type") != "event_callback"
                or body.get("team_id") != scope.installation_account
                or event.get("type") != "app_mention" or event.get("bot_id")
                or event.get("subtype") in NON_CONTENT_SUBTYPES
                or slack_identity != scope.channel_identity_name or bot_user_id != scope.bot_user_id
            ):
                return _REFUSED
            channel = event.get("channel")
            if type(channel) is not str or channel not in self._bindings:
                return _REFUSED
            binding = self._bindings[channel]
            timestamp = event.get("thread_ts") or event.get("ts")
            sender = event.get("user")
            if type(timestamp) is not str or not timestamp or type(sender) is not str or not sender:
                return _REFUSED
            delivery = body.get("event_id")
            if type(delivery) is not str or not delivery:
                return _REFUSED
            evidence = slack_evidence(body)
            if evidence is None:
                return _REFUSED
            evidence = {
                "delivery": None, "user_id": None, "event_user_team": None, "event_team": None,
                "interaction_user_team_id": None, "is_ext_shared_channel": None,
                **evidence,
            }
            evidence["enterprise_ids"] = tuple(evidence.get("enterprise_ids", ()))
            conversation = timestamp
            payload = json.dumps({
                "kind": "slack", "address": channel, "adapter": scope.channel_identity_name,
                "delivery_id": delivery, "conversation_id": conversation, "author": sender,
                "text": strip_self_mention(derive_text(event), bot_user_id), "reply_ref": "",
                "attachments": [item.model_dump(mode="json") for item in derive_attachments(event)],
            }, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            proof = codec.ReceiverProofV1(
                schema_version=1, receiver_id=scope.receiver_id,
                receiver_generation=scope.receiver_generation,
                channel_identity_id=scope.channel_identity_id,
                provider_installation_id=scope.provider_installation_id,
                binding_id=binding.binding_id, binding_generation=binding.binding_generation,
                delivery_id=delivery, native_channel_id=channel, conversation_id=conversation,
                body_sha256=hashlib.sha256(payload).hexdigest(),
                slack=codec.ReceiverSlackEvidenceV1.model_validate(evidence),
            )
            token = codec.sign_receiver_proof(proof, self._keys)
        except (ValueError, TypeError, AttributeError, RecursionError):
            return _REFUSED
        return asyncio.run(self._submit(payload, token, binding.channel_credential))

    def submit_answer(
        self, *, approval_id: str, decision: str, note: str | None, user: str, channel: str,
        slack: dict[str, Any] | None, attester_secret: str,
    ) -> ResolveOutcome:
        """One bounded, proved answer from the authenticated Bolt listener.

        The decision and note are serialized once; the existing chat attestation
        and a receiver-signed proof bind those exact bytes. Called only after the
        listener acked. @spec HTTP-APPROVAL-CONTINUITY-7.
        """
        scope = self._scope
        try:
            if (
                type(approval_id) is not str or str(uuid.UUID(approval_id)) != approval_id
                or decision not in {"approved", "rejected"} or type(slack) is not dict
                or slack.get("delivery") not in _INTERACTIONS or slack.get("user_id") != user
                or type(slack.get("interaction_user_team_id")) is not str
                or not slack["interaction_user_team_id"]
                or type(channel) is not str or not channel
                or (note is not None and type(note) is not str)
            ):
                return _ANSWER_REFUSED
            content: dict[str, str] = {"decision": decision}
            if note:
                content["note"] = note
            body = json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            attestation = mint_chat_principal(
                attester_secret, subject=user, actor_channel=channel, approval_id=approval_id,
                slack=slack,
            )
            evidence = {
                "delivery": None, "user_id": None, "event_user_team": None, "event_team": None,
                "interaction_user_team_id": None, "is_ext_shared_channel": None,
                **slack,
            }
            evidence["enterprise_ids"] = tuple(evidence.get("enterprise_ids", ()))
            proof = approval_wire.ApprovalAnswerProofV1(
                schema_version=1, purpose="approval_answer", operation="resolve",
                receiver_id=scope.receiver_id, receiver_generation=scope.receiver_generation,
                channel_identity_id=scope.channel_identity_id,
                provider_installation_id=scope.provider_installation_id,
                approval_id=approval_id, body_sha256=hashlib.sha256(body).hexdigest(),
                attestation_sha256=hashlib.sha256(attestation.encode("ascii")).hexdigest(),
                native_channel_id=channel,
                slack=codec.ReceiverSlackEvidenceV1.model_validate(evidence),
            )
            token = approval_wire.sign_approval_answer_proof(proof, self._keys)
        except (ValueError, TypeError, AttributeError, RecursionError):
            return _ANSWER_REFUSED
        return asyncio.run(self._answer(approval_id, body, attestation, token))

    async def _answer(
        self, approval_id: str, body: bytes, attestation: str, proof: str,
    ) -> ResolveOutcome:
        """Fresh owned client, five seconds total, one attempt. @spec HTTP-APPROVAL-CONTINUITY-7."""
        try:
            async with asyncio.timeout(5):
                async with self._client_factory() as client:
                    request = httpx.Request(
                        "POST", f"{self._api_base}/approvals/{approval_id}/resolve",
                        content=body, headers={
                            "Content-Type": "application/json",
                            "X-Curie-Approval-Principal": attestation,
                            "X-Curie-Approval-Receiver-Proof": proof,
                        },
                    )
                    response = await client.send(
                        request, stream=True, auth=None, follow_redirects=False,
                    )
                    try:
                        return await _answer_outcome(response)
                    finally:
                        await response.aclose()
        except Exception:  # noqa: BLE001 - an unknown outcome is never a decision.
            return _ANSWER_UNKNOWN

    async def _submit(self, payload: bytes, proof: str, credential: str) -> ReceiverSubmission:
        """@spec HTTP-INVOCATION-EXECUTION-3/19."""
        try:
            async with asyncio.timeout(5):
                async with self._client_factory() as client:
                    for attempt in range(2):
                        request = httpx.Request("POST", self._url, content=payload, headers={
                            "Content-Type": "application/json", "X-API-Key": credential,
                            "X-Curie-Receiver-Proof": proof,
                        })
                        complete = False
                        try:
                            response = await client.send(
                                request, stream=True, auth=None, follow_redirects=False,
                            )
                            try:
                                result = await _receipt(response)
                                complete = True
                                return result
                            finally:
                                await response.aclose()
                        except _AMBIGUOUS:
                            if attempt or complete:
                                return _UNAVAILABLE
                    return _UNAVAILABLE
        except Exception:  # noqa: BLE001 - ordinary transport/cleanup errors expose no inputs.
            return _UNAVAILABLE


class SelectedApprovalAnswers:
    """The resolver the approval listeners use on a selected receiver.

    It never reaches the legacy client: every answer is the runtime's bounded,
    proved submission. Declared test drivers are not a supported answer source.
    @spec HTTP-APPROVAL-CONTINUITY-7.
    """

    def __init__(self, runtime: HttpReceiverRuntime, attester_secret: str) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-7."""
        self._runtime = runtime
        self._attester_secret = attester_secret

    def resolve(
        self, approval_id: str, *, decision: str, attested_user: str, attested_channel: str,
        note: str | None = None, test_driver: bool = False, slack: dict[str, Any] | None = None,
    ) -> ResolveOutcome:
        """@spec HTTP-APPROVAL-CONTINUITY-7."""
        if test_driver:
            return _ANSWER_REFUSED
        return self._runtime.submit_answer(
            approval_id=approval_id, decision=decision, note=note, user=attested_user,
            channel=attested_channel, slack=slack, attester_secret=self._attester_secret,
        )


def build_http_receiver_runtime(
    scope: HttpReceiverScopeV1, bindings: tuple[HttpReceiverBindingV1, ...], *,
    receiver_key: bytes, web_client: WebClient, api_base_url: str,
    http_client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> HttpReceiverRuntime:
    """@spec HTTP-INVOCATION-EXECUTION-1/2/19."""
    try:
        if type(scope) is not HttpReceiverScopeV1 or type(bindings) is not tuple:
            raise ValueError
        scope = HttpReceiverScopeV1.model_validate(scope.model_dump())
        if not 1 <= len(bindings) <= 256 or type(receiver_key) is not bytes:
            raise ValueError
        by_channel: dict[str, HttpReceiverBindingV1] = {}
        ids: set[str] = set()
        for supplied in bindings:
            if type(supplied) is not HttpReceiverBindingV1:
                raise ValueError
            binding = HttpReceiverBindingV1.model_validate(supplied.model_dump())
            if binding.native_channel_id in by_channel or binding.binding_id in ids:
                raise ValueError
            by_channel[binding.native_channel_id] = binding
            ids.add(binding.binding_id)
        keys = codec.build_identity_keyring(
            "receiver", ((scope.receiver_id, scope.receiver_generation, receiver_key),),
        )
        if type(api_base_url) is not str or api_base_url != api_base_url.strip():
            raise ValueError
        url = urlsplit(api_base_url)
        if (
            url.scheme not in {"http", "https"} or not url.hostname or url.username is not None
            or url.password is not None or url.query or url.fragment
        ):
            raise ValueError
        _ = url.port
        if http_client_factory is not None and not callable(http_client_factory):
            raise ValueError
        _qualified(scope, web_client)
        runtime = object.__new__(HttpReceiverRuntime)
        object.__setattr__(runtime, "_scope", scope)
        object.__setattr__(runtime, "_bindings", MappingProxyType(by_channel))
        object.__setattr__(runtime, "_keys", keys)
        object.__setattr__(runtime, "_url", api_base_url.rstrip("/") + "/channels/turns")
        object.__setattr__(runtime, "_api_base", api_base_url.rstrip("/"))
        object.__setattr__(runtime, "_client_factory", http_client_factory or httpx.AsyncClient)
        return runtime
    except Exception:  # noqa: BLE001 - initializer failures never expose supplied material.
        raise ValueError("invalid_http_receiver_runtime") from None

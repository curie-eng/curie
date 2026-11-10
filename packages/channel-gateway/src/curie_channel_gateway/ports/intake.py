"""Gateway-owned submission and approval transports to existing API endpoints."""

from collections.abc import Mapping
from typing import ClassVar, Literal, Protocol


class TurnSubmission[Turn, Receipt](Protocol):
    """Submit to /channels/turns using the lane's configured channel credential.

    Turn and Receipt retain the lane's existing typed payload and result. The
    gateway authenticates the upstream delivery, constructs the payload, fixes
    kind as an adapter constant and signs the receiver proof over exact bytes.
    Preserve delivery IDs, reply slots, duplicate receipts and pending/refused/
    unavailable outcomes. Mail retains its accepted/refused/retry classification.
    The API owns admission, dedupe and enqueue on this HTTP path; this does not
    replace the stock Slack admission-then-Valkey path.
    """

    kind: ClassVar[str]

    def submit(self, turn: Turn) -> Receipt: ...


class ApprovalResolution[Outcome](Protocol):
    """Resolve through /approvals/{id}/resolve; Outcome retains the API verdict.

    The gateway owns upstream authentication, minting the chat-attester principal
    and signing the receiver answer proof over exact resolve bytes and principal.
    The API verifies their signatures, evidence shape and binding, derives a
    principal and authorizes resolution. It cannot independently confirm that a
    Slack delivery occurred: holders of the platform key and attester secret can
    assert any user. Inputs are authenticated by the gateway before this call,
    never accepted as proof merely because an actor string was supplied.
    Mail retains its existing adapter principal and verified sender evidence.
    API resolve-once, fresh membership checks and worker resume enqueue stay put.
    """

    def resolve(
        self,
        approval_id: str,
        *,
        decision: Literal["approved", "rejected"],
        actor: str,
        actor_channel: str | None,
        note: str | None = None,
        evidence: Mapping[str, object] | None = None,
    ) -> Outcome: ...

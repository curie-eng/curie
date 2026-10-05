"""Capability declarations and the pairing rules between trackers and code hosts.

Each adapter declares every operation of its port as supported, no-op or
unsupported (ADR 0197, "Two ports" item 5). Mandatory operations must be
supported. A no-op is allowed only where doing nothing is a correct answer.
An unsupported operation raises `Unsupported` and the caller falls back.
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from curie_api.forges import types
from curie_api.forges.errors import InvalidPairing


class Support(enum.StrEnum):
    SUPPORTED = "supported"
    NOOP = "noop"
    UNSUPPORTED = "unsupported"


class Operation(enum.StrEnum):
    """Every port operation, by port."""

    # Tracker
    POLL_MARKED = "tracker.poll_marked"
    VERIFY_CURRENT = "tracker.verify_current"
    MARKING_ACTOR = "tracker.marking_actor"
    MAY_START = "tracker.may_start"
    READ_TICKET = "tracker.read_ticket"
    SET_STATE_LABEL = "tracker.set_state_label"
    CLOSING_REFERENCE = "tracker.closing_reference"
    LINK_PULL_REQUEST = "tracker.link_pull_request"
    DEPENDENCIES = "tracker.dependencies"
    # CodeHost
    RESOLVE_REPOSITORY = "code_host.resolve_repository"
    CREDENTIAL = "code_host.credential"
    BRANCH_HEAD = "code_host.branch_head"
    READ_COMMIT = "code_host.read_commit"
    FIND_PULL_REQUEST = "code_host.find_pull_request"
    OPEN_PULL_REQUEST = "code_host.open_pull_request"
    UPDATE_PULL_REQUEST = "code_host.update_pull_request"
    READ_PULL_REQUEST = "code_host.read_pull_request"
    OBSERVE_CI = "code_host.observe_ci"
    LIST_REVIEW_FEEDBACK = "code_host.list_review_feedback"
    VERIFY_FEEDBACK = "code_host.verify_feedback"
    CI_DIAGNOSTICS = "code_host.ci_diagnostics"
    RERUN_FAILED = "code_host.rerun_failed"
    USER_CAN_WRITE = "code_host.user_can_write"
    # MarkedComments, exposed by both ports
    FIND_MARKED = "marked_comments.find_marked"
    UPSERT_MARKED = "marked_comments.upsert_marked"


class CapabilityTier(enum.StrEnum):
    MANDATORY = "mandatory"
    OPTIONAL = "optional"


TRACKER_OPERATIONS: frozenset[Operation] = frozenset(
    op for op in Operation if op.value.startswith("tracker.")
)
CODE_HOST_OPERATIONS: frozenset[Operation] = frozenset(
    op for op in Operation if op.value.startswith("code_host.")
)
MARKED_COMMENT_OPERATIONS: frozenset[Operation] = frozenset(
    op for op in Operation if op.value.startswith("marked_comments.")
)

OPTIONAL_OPERATIONS: frozenset[Operation] = frozenset(
    {
        Operation.LINK_PULL_REQUEST,
        Operation.DEPENDENCIES,
        Operation.CI_DIAGNOSTICS,
        Operation.RERUN_FAILED,
        Operation.USER_CAN_WRITE,
    }
)

# Doing nothing is a correct answer only here: a tracker whose closing reference
# already links the pull request, a tracker with no dependency notion (no
# dependencies), a code host with nothing it can rerun. A permission or a log
# read cannot be answered by doing nothing, so those are supported or not.
NOOP_ELIGIBLE: frozenset[Operation] = frozenset(
    {Operation.LINK_PULL_REQUEST, Operation.DEPENDENCIES, Operation.RERUN_FAILED}
)


def tier(operation: Operation) -> CapabilityTier:
    if operation in OPTIONAL_OPERATIONS:
        return CapabilityTier.OPTIONAL
    return CapabilityTier.MANDATORY


# A native forge implements both ports, and its tracker pairs only with itself:
# its write check says nothing about a repository on another forge.
NATIVE_FORGES: frozenset[str] = frozenset({types.GITHUB, types.GITLAB, types.MEMORY})
# A tracker-only kind pairs with any code host and authorizes from its binding.
TRACKER_ONLY: frozenset[str] = frozenset({types.JIRA, types.MEMORY_TRACKER_ONLY})
CODE_HOST_ONLY: frozenset[str] = frozenset({types.BITBUCKET_CLOUD, types.BITBUCKET_DATA_CENTER})


@dataclass(frozen=True)
class MandatorySet:
    """The operations each side of one pairing must declare SUPPORTED."""

    tracker: frozenset[Operation]
    code_host: frozenset[Operation]
    marked_comments: frozenset[Operation]


def mandatory_capabilities(tracker_kind: str, code_host_kind: str) -> MandatorySet:
    """The mandatory set for a pairing; refuses a pairing ADR 0197 forbids.

    On a native pairing the code host must also report write access: the native
    tracker admits on that same forge's write check, so a forge that cannot
    report it cannot admit at all. Elsewhere a code host that cannot report it
    falls back to the binding's allowlist (`curie_api.forges.authority`).
    """

    if tracker_kind not in NATIVE_FORGES | TRACKER_ONLY:
        raise InvalidPairing(f"{tracker_kind!r} is not a tracker kind")
    if code_host_kind not in NATIVE_FORGES | CODE_HOST_ONLY:
        raise InvalidPairing(f"{code_host_kind!r} is not a code host kind")
    native = tracker_kind in NATIVE_FORGES
    if native and tracker_kind != code_host_kind:
        raise InvalidPairing(f"a {tracker_kind} tracker pairs only with a {tracker_kind} code host")
    tracker = TRACKER_OPERATIONS - OPTIONAL_OPERATIONS
    code_host = CODE_HOST_OPERATIONS - OPTIONAL_OPERATIONS
    if native:
        code_host = code_host | {Operation.USER_CAN_WRITE}
    return MandatorySet(tracker, code_host, MARKED_COMMENT_OPERATIONS)


class _Declares(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def capabilities(self) -> Mapping[Operation, Support]: ...


class _DeclaresComments(_Declares, Protocol):
    @property
    def marked_comments(self) -> _Declares: ...


def validate_declaration(
    capabilities: Mapping[Operation, Support], operations: frozenset[Operation]
) -> None:
    """Every operation of the port is declared, and no-op only where eligible."""

    missing = sorted(op.value for op in operations if op not in capabilities)
    if missing:
        raise InvalidPairing(f"undeclared operations: {', '.join(missing)}")
    extra = sorted(op.value for op in capabilities if op not in operations)
    if extra:
        raise InvalidPairing(f"operations of another port declared: {', '.join(extra)}")
    for op, support in capabilities.items():
        if support is Support.NOOP and op not in NOOP_ELIGIBLE:
            raise InvalidPairing(f"{op.value} cannot be a no-op")


def _require(side: str, declared: Mapping[Operation, Support], ops: frozenset[Operation]) -> None:
    short = sorted(op.value for op in ops if declared.get(op) is not Support.SUPPORTED)
    if short:
        raise InvalidPairing(f"{side} must support {', '.join(short)}")


def validate_pairing(tracker: _DeclaresComments, code_host: _DeclaresComments) -> MandatorySet:
    """Refuse a binding whose pairing or declarations ADR 0197 does not allow."""

    required = mandatory_capabilities(tracker.kind, code_host.kind)
    validate_declaration(tracker.capabilities, TRACKER_OPERATIONS)
    validate_declaration(code_host.capabilities, CODE_HOST_OPERATIONS)
    for side in (tracker, code_host):
        validate_declaration(side.marked_comments.capabilities, MARKED_COMMENT_OPERATIONS)
    _require("tracker", tracker.capabilities, required.tracker)
    _require("code host", code_host.capabilities, required.code_host)
    _require("tracker comments", tracker.marked_comments.capabilities, required.marked_comments)
    _require("code host comments", code_host.marked_comments.capabilities, required.marked_comments)
    return required


def supports(capabilities: Mapping[Operation, Support], operation: Operation) -> bool:
    return capabilities.get(operation) is Support.SUPPORTED

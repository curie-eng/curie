"""Per-agent publication approval policy (ADR 0147, issue 2575).

The default is human approval. ``auto`` does not skip the approval row. The
platform resolves that row under the operator's recorded policy, and a later
credential redemption refuses the row when the policy version no longer matches.
"""

from __future__ import annotations

import re
from typing import Any

POLICY_APPROVE = "approve"
POLICY_AUTO = "auto"
POLICY_IDENTITY = "publication:auto"
PLATFORM_ACTOR = "platform:publication-policy"
PLATFORM_AUTHORIZER = "publication-policy"
DEFAULT_BRANCH_PREFIX = "curie/"

_BRANCH_PREFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}/$")


class PublicationPolicyConflict(Exception):
    """A publication-policy write lost a compare-and-set on the policy version."""


def validate_branch_prefix(value: str) -> str:
    """Return a slash-terminated git ref prefix, or raise ValueError.

    Git rejects a component that ends in ``.lock`` or ``.`` (``git
    check-ref-format``). A prefix that can only produce those branches is
    refused here, before a publication is created.
    """

    if not isinstance(value, str) or _BRANCH_PREFIX.fullmatch(value) is None:
        raise ValueError("publication_branch_prefix must be a slash-terminated git ref prefix")
    component = value[:-1]
    if ".." in component or component.endswith(".") or component.endswith(".lock"):
        raise ValueError("publication_branch_prefix must be a slash-terminated git ref prefix")
    return value


def publication_row_prefix(branch: str, *, operator_prefix: str | None, auto: bool) -> str | None:
    """Prefix snapshot for one publication row.

    Automatic policy records the operator prefix. A lineage that already left
    the historical ``curie/`` prefix keeps that branch's own prefix, so a later
    human revision is not renamed and is not rejected as an unbound branch.
    """

    if auto and operator_prefix:
        return operator_prefix
    if branch.startswith(DEFAULT_BRANCH_PREFIX):
        return None
    slash = branch.find("/")
    if slash <= 0:
        return None
    try:
        return validate_branch_prefix(branch[: slash + 1])
    except ValueError:
        return None


def publication_branch_name(lineage_id_hex: str, *, prefix: str | None, auto: bool) -> str:
    """Name one new lineage branch. Human policy keeps the historical prefix."""

    name_prefix = prefix if auto and prefix else DEFAULT_BRANCH_PREFIX
    return f"{name_prefix}publication-{lineage_id_hex}"


def policy_still_authorizes(agent: Any, approval: Any) -> bool:
    """Whether a policy-resolved approval may still redeem a write credential.

    A human resolution has no policy identity and is unchanged. A policy
    resolution matches only the current ``auto`` policy at the same version.
    """

    identity = getattr(approval, "policy_identity", None)
    if identity is None:
        return True
    if agent is None:
        return False
    return bool(
        identity == POLICY_IDENTITY
        and getattr(approval, "resolved_by", None) == PLATFORM_ACTOR
        and getattr(agent, "publication_policy", None) == POLICY_AUTO
        and getattr(approval, "policy_version", None)
        == getattr(agent, "publication_policy_version", None)
    )

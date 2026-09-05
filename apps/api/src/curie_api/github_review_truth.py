"""Fresh GitHub authority checks for thread-bound human review feedback.

The HTTP ingress and the before-model recheck use the same verifier. It only
reads provider state; publication still belongs to the existing trusted writer.
"""

from dataclasses import dataclass, replace
from typing import Any

import httpx
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .github_review_events import (
    FeedbackIgnored,
    FeedbackUnavailable,
    UnverifiedFeedback,
    parse_feedback,
)
from .repo_full_name import repo_url_path


@dataclass(frozen=True)
class BoundReviewLineage:
    """Identity read from one unambiguous persisted publication lineage."""

    repo_full_name: str
    pr_number: int
    branch: str
    head_sha: str
    repository_id: int
    installation_id: int
    pr_node_id: str
    base_ref: str


async def verify_feedback_truth(
    feedback: UnverifiedFeedback,
    lineage: BoundReviewLineage,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> str:
    """Return the exact current bound PR head, or a payload-free refusal.

    Resource URLs are derived from persisted identity. No claimed webhook or
    REST response URL is fetched, and redirects cannot carry the App token to
    another endpoint. All credential-bearing requests stay inside the API.
    """
    if (
        feedback.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or feedback.pr_number != lineage.pr_number
        or feedback.repository_id != lineage.repository_id
        or feedback.installation_id != lineage.installation_id
    ):
        raise FeedbackIgnored("lineage_mismatch")
    try:
        token = await run_in_threadpool(
            credentials_for(settings).token_for_verified_installation,
            lineage.repo_full_name,
            lineage.installation_id,
        )
    except (GitHubInstallationRefused, ValueError):
        raise FeedbackIgnored("installation_unverified") from None
    except GitHubAppError:
        raise FeedbackUnavailable("installation_unavailable") from None

    api = settings.github_api_url.rstrip("/")
    repo_path = f"/repos/{repo_url_path(lineage.repo_full_name)}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async def read(path: str, refusal: str) -> dict[str, Any]:
        try:
            response = await client.get(
                f"{api}{path}",
                headers=headers,
                follow_redirects=False,
            )
        except httpx.HTTPError:
            raise FeedbackUnavailable(refusal) from None
        # A 404 can conceal missing App permissions; it cannot distinguish a
        # deleted comment from a temporary inability to prove current authority.
        # Neither case may execute. Operators can redeliver after correcting
        # setup; GitHub does not automatically retry failed webhook deliveries.
        if response.status_code in {401, 403, 404, 429} or response.status_code >= 500:
            raise FeedbackUnavailable(refusal)
        if response.status_code != 200:
            raise FeedbackIgnored(refusal)
        try:
            result = response.json()
        except ValueError:
            raise FeedbackIgnored(refusal) from None
        if not isinstance(result, dict):
            raise FeedbackIgnored(refusal)
        return result

    repository = await read(repo_path, "repository_unavailable")

    def same_repository(value: Any) -> bool:
        return (
            isinstance(value, dict)
            and type(value.get("id")) is int
            and value["id"] == lineage.repository_id
            and isinstance(value.get("full_name"), str)
            and value["full_name"].casefold() == lineage.repo_full_name.casefold()
        )

    if not same_repository(repository):
        raise FeedbackIgnored("repository_mismatch")
    pr = await read(f"{repo_path}/pulls/{lineage.pr_number}", "pull_request_unavailable")
    head, base = pr.get("head"), pr.get("base")
    if (
        not isinstance(head, dict)
        or not isinstance(base, dict)
        or type(pr.get("number")) is not int
        or pr["number"] != lineage.pr_number
        or pr.get("node_id") != lineage.pr_node_id
        or base.get("ref") != lineage.base_ref
        or not isinstance(pr.get("html_url"), str)
        or pr["html_url"].casefold()
        != f"https://github.com/{lineage.repo_full_name}/pull/{lineage.pr_number}".casefold()
        or head.get("ref") != lineage.branch
    ):
        raise FeedbackIgnored("pull_request_mismatch")
    if not same_repository(head.get("repo")) or not same_repository(base.get("repo")):
        raise FeedbackIgnored("repository_mismatch")
    if pr.get("state") != "open" or pr.get("merged") is not False:
        raise FeedbackIgnored("terminal_pull_request")
    if head.get("sha") != lineage.head_sha or (
        feedback.head_sha is not None and feedback.head_sha != lineage.head_sha
    ):
        raise FeedbackIgnored("stale_feedback_head")

    if feedback.event == "issue_comment":
        path = f"{repo_path}/issues/comments/{feedback.feedback_id}"
    elif feedback.event == "pull_request_review_comment":
        path = f"{repo_path}/pulls/comments/{feedback.feedback_id}"
    else:
        path = f"{repo_path}/pulls/{lineage.pr_number}/reviews/{feedback.feedback_id}"
    current = await read(path, "feedback_unavailable")
    if feedback.event == "issue_comment" and current.get("issue_url") != (
        f"{api}{repo_path}/issues/{lineage.pr_number}"
    ):
        raise FeedbackIgnored("feedback_target_mismatch")
    if feedback.event == "pull_request_review_comment" and current.get("pull_request_url") != (
        f"{api}{repo_path}/pulls/{lineage.pr_number}"
    ):
        raise FeedbackIgnored("feedback_target_mismatch")

    # Reuse the strict body/author/time/URL/context checks on the freshly read
    # provider object. The wrapper supplies only independently verified facts.
    canonical: dict[str, Any] = {
        "installation": {"id": feedback.installation_id},
        "repository": repository,
        "sender": current.get("user"),
        "action": "submitted" if feedback.event == "pull_request_review" else "created",
    }
    if feedback.event == "issue_comment":
        canonical.update(
            {
                "comment": current,
                "issue": {
                    "number": lineage.pr_number,
                    "state": "open",
                    "pull_request": {},
                },
            }
        )
    else:
        canonical["pull_request"] = pr
        canonical["review" if feedback.event == "pull_request_review" else "comment"] = current
    observed = parse_feedback(feedback.event, canonical, str(feedback.delivery_id))
    # Repository spelling may differ in a signed payload; it is case-insensitive
    # identity. The canonical URL retained for the model comes from our lineage.
    expected = replace(feedback, repo_full_name=observed.repo_full_name, url=observed.url)
    if observed != expected:
        raise FeedbackIgnored("feedback_changed")
    if observed.author_association == "CONTRIBUTOR":
        # Association does not establish effective repository permission. Keep
        # the existing OWNER/MEMBER/COLLABORATOR policy, and authorize this
        # fallback only through the current, installation-verified App token.
        # Permission results are read again on every pre-model verification.
        # https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
        permission = await read(
            f"{repo_path}/collaborators/{observed.sender_login}/permission",
            "sender_permission_unavailable",
        )
        user = permission.get("user")
        if (
            not isinstance(user, dict)
            or type(user.get("id")) is not int
            or user["id"] != observed.sender_id
        ):
            raise FeedbackIgnored("sender_permission_identity_mismatch")
        # The documented legacy field folds maintain into write. Descriptive
        # role_name and unexpected values cannot grant authority.
        if permission.get("permission") not in ("write", "admin"):
            raise FeedbackIgnored("sender_permission_refused")
    return lineage.head_sha

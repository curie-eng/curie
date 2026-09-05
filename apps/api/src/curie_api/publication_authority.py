"""Read immutable GitHub identities during the existing publication advance.

No GitHub writes occur here. Historical NULL lineages cannot acquire authority
from a repository name that may have been deleted and recreated meanwhile.
"""

from dataclasses import dataclass
from typing import Any

import httpx
from starlette.concurrency import run_in_threadpool

from .config import Settings
from .github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from .models import ThreadPublicationLineage
from .repo_full_name import repo_url_path
from .schemas import PublicationLineageAdvance


class AuthorityRefused(RuntimeError):
    """Fresh provider identity disagrees with the exact publication outcome."""


class AuthorityUnavailable(RuntimeError):
    """The configured product App could not establish current provider truth."""


@dataclass(frozen=True)
class VerifiedPublicationIdentity:
    repository_id: int
    installation_id: int
    pr_node_id: str
    base_ref: str


def _positive_id(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value < 2**63


def validated_identity(
    repository: Any,
    pull_request: Any,
    *,
    installation_id: int,
    repo_full_name: str,
    pr_number: int,
    branch: str,
    head_sha: str,
    state: str,
) -> VerifiedPublicationIdentity:
    """Validate provider-owned fields, never a webhook's purported authority."""
    if not isinstance(repository, dict) or not isinstance(pull_request, dict):
        raise AuthorityRefused("publication GitHub identity was refused")
    repository_id = repository.get("id")
    node_id = pull_request.get("node_id")
    head = pull_request.get("head")
    base = pull_request.get("base")
    if (
        not _positive_id(repository_id)
        or not _positive_id(installation_id)
        or str(repository.get("full_name", "")).casefold() != repo_full_name.casefold()
        or not _positive_id(pull_request.get("number"))
        or pull_request.get("number") != pr_number
        or not isinstance(node_id, str)
        or not node_id
        or len(node_id) > 256
        or str(pull_request.get("html_url", "")).casefold()
        != f"https://github.com/{repo_full_name}/pull/{pr_number}".casefold()
        or not isinstance(head, dict)
        or not isinstance(base, dict)
        or head.get("ref") != branch
        or not isinstance(head.get("sha"), str)
        or head["sha"].lower() != head_sha.lower()
    ):
        raise AuthorityRefused("publication GitHub identity was refused")
    for side in (head, base):
        remote_repo = side.get("repo")
        if (
            not isinstance(remote_repo, dict)
            or not _positive_id(remote_repo.get("id"))
            or remote_repo.get("id") != repository_id
            or str(remote_repo.get("full_name", "")).casefold() != repo_full_name.casefold()
        ):
            raise AuthorityRefused("publication GitHub repository identity was refused")
    merged = pull_request.get("merged")
    remote_state = pull_request.get("state")
    if not isinstance(merged, bool) or remote_state not in {"open", "closed"}:
        raise AuthorityRefused("publication GitHub state was refused")
    actual_state = "merged" if merged else remote_state
    if actual_state != state or (merged and remote_state != "closed"):
        raise AuthorityRefused("publication GitHub state was refused")
    base_ref = base.get("ref")
    if not isinstance(base_ref, str) or not base_ref or len(base_ref) > 1024:
        raise AuthorityRefused("publication GitHub base reference was refused")
    assert isinstance(repository_id, int)
    return VerifiedPublicationIdentity(repository_id, installation_id, node_id, base_ref)


async def verify_publication_identity(
    lineage: ThreadPublicationLineage,
    data: PublicationLineageAdvance,
    settings: Settings,
    client: httpx.AsyncClient,
) -> VerifiedPublicationIdentity | None:
    # A newly captured binding proves this producer created the lineage. Existing
    # PRs without immutable IDs remain ineligible even if an App is added later.
    if lineage.binding_id is None or (
        lineage.pr_number is not None and lineage.github_repository_id is None
    ):
        return None
    resolver = credentials_for(settings)
    if not resolver.app_configured:
        if lineage.github_repository_id is not None:
            raise AuthorityUnavailable("publication App identity is not configured")
        return None
    try:
        installation_id, token = await run_in_threadpool(
            resolver.fresh_installation_token,
            lineage.repo_full_name,
            lineage.github_installation_id,
        )
    except GitHubInstallationRefused:
        raise AuthorityRefused("publication App installation identity was refused") from None
    except (GitHubAppError, ValueError):
        raise AuthorityUnavailable("publication App identity could not be verified") from None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    root = settings.github_api_url.rstrip("/")
    path = f"/repos/{repo_url_path(lineage.repo_full_name)}"
    payloads = []
    try:
        for suffix in (path, f"{path}/pulls/{data.pr_number}"):
            response = await client.get(
                root + suffix,
                headers=headers,
                timeout=settings.github_app_timeout_seconds,
                follow_redirects=False,
            )
            if response.status_code in {401, 403, 404, 429} or response.status_code >= 500:
                raise AuthorityUnavailable("publication GitHub identity could not be verified")
            if response.status_code != 200:
                raise AuthorityRefused("publication GitHub identity was refused")
            payloads.append(response.json())
    except (httpx.HTTPError, ValueError):
        raise AuthorityUnavailable("publication GitHub identity could not be verified") from None
    return validated_identity(
        *payloads,
        installation_id=installation_id,
        repo_full_name=lineage.repo_full_name,
        pr_number=data.pr_number,
        branch=lineage.branch,
        head_sha=data.head_sha,
        state=data.state,
    )

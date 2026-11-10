"""Server-derived GitHub authorization for trusted worker-only redemption."""

import base64
from typing import Any
from urllib.parse import urlsplit

from .repo_full_name import repo_url_path


def resolve_repository_credential(
    repo_full_name: str, *, html_base: str, credentials: Any
) -> tuple[str, str]:
    """Return a clean origin and its ephemeral Authorization header.

    The origin is the code host's canonical HTTPS origin for the repository,
    ``<html_base>/<owner>/<name>.git``, derived from the code host's configured
    host and the stored repository path. ``credentials`` is the code host's
    `GitHubCredentials`: an App installation token when an App is configured,
    else its operator token.
    """

    base = html_base.rstrip("/")
    clone_url = f"{base}/{repo_url_path(repo_full_name)}.git"
    origin = urlsplit(clone_url)
    if (
        clone_url != f"{base}/{repo_full_name}.git"
        or origin.scheme != "https"
        or not origin.hostname
        or origin.username is not None
        or origin.password is not None
    ):
        raise ValueError("managed repository origin is not canonical configured GitHub HTTPS")
    token = credentials.token_for(repo_full_name)
    if not token:
        raise ValueError("operator GitHub credential is not configured")
    # The same `x-access-token` Basic header `gitflow.clone_credential_env` sends.
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    return clone_url, f"Basic {basic}"

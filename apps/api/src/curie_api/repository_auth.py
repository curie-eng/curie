"""Server-derived GitHub authorization for trusted worker-only redemption."""

from urllib.parse import urlsplit

from curie_api.gitflow import clone_credential_env

from .config import Settings
from .gitflow import trusted_clone_url
from .github_app import credentials_for


def resolve_repository_credential(
    repo_full_name: str, settings: Settings
) -> tuple[str, str]:
    """Return a clean origin and its ephemeral Authorization header.

    The origin comes only from stored server facts and configured clone base.
    App installation-token minting remains preferred by ``GitHubCredentials``;
    the raw operator token remains its fallback.
    """

    clone_url = trusted_clone_url(repo_full_name, settings)
    expected_url = f"{settings.github_html_base}/{repo_full_name}.git"
    origin = urlsplit(clone_url)
    if (
        clone_url != expected_url
        or origin.scheme != "https"
        or not origin.hostname
        or origin.username is not None
        or origin.password is not None
    ):
        raise ValueError("managed repository origin is not canonical configured GitHub HTTPS")
    env = clone_credential_env(
        clone_url,
        settings,
        repo_full_name=repo_full_name,
        credentials=credentials_for(settings),
    )
    value = env.get("GIT_CONFIG_VALUE_0", "")
    prefix = "Authorization: "
    authorization_header = value[len(prefix) :] if value.startswith(prefix) else ""
    if not authorization_header:
        raise ValueError("operator GitHub credential is not configured")
    return clone_url, authorization_header

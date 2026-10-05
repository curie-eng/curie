"""Issue a repository credential to the trusted worker through CodeHost (ADR 0197).

The workspace clone and the publication push both receive the same facts as
data: the clean clone URL, the header value git sends and the header form that
names it, the origin the header is scoped to, and a reference to the CA bundle
mounted where the git command runs. The worker derives none of them; it holds
no forge code (ADR 0197, "Two ports" item 6).
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from .config import Settings
from .forges.hosts import code_host_for, repository_ref
from .forges.types import CredentialHeader, CredentialScope


@dataclass(frozen=True)
class IssuedCredential:
    clone_url: str
    authorization_header: str
    origin: str
    header_form: CredentialHeader
    ca_bundle_ref: str | None


async def issue_repository_credential(
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    repo_full_name: str,
    project_id: int | str | None,
    scope: CredentialScope,
) -> IssuedCredential:
    """The code host's credential for one stored repository.

    Raises `curie_api.forges.errors.ForgeError` when the code host cannot issue
    one. The header value is what follows the header name in
    ``Credential.git_header()``, so GitHub's stays exactly ``Basic <base64>``.
    """

    code_host = code_host_for(settings, client)
    repository = repository_ref(settings, path=repo_full_name, project_id=project_id)
    credential = await code_host.credential(repository, scope)
    _, _, value = credential.git_header().partition(": ")
    return IssuedCredential(
        clone_url=f"{credential.origin}/{repository.path}.git",
        authorization_header=value,
        origin=credential.origin,
        header_form=credential.header,
        ca_bundle_ref=settings.code_host_ca_bundle.strip() or None,
    )

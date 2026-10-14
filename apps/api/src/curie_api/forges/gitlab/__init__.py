"""GitLab (gitlab.com and self-managed) as the factory's code host and tracker (ADR 0197, #4399)."""

from curie_api.forges.gitlab.connection import GitLabCodeHostConnection, GitLabTrackerConnection
from curie_api.forges.registry import register

register("gitlab", tracker=GitLabTrackerConnection, code_host=GitLabCodeHostConnection)

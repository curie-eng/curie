"""GitHub as the factory's code host and tracker (ADR 0197)."""

from curie_api.forges.github.connection import GitHubCodeHostConnection, GitHubTrackerConnection
from curie_api.forges.registry import register

register("github", tracker=GitHubTrackerConnection, code_host=GitHubCodeHostConnection)

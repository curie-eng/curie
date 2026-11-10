"""Jira Cloud as a tracker (ADR 0197, #4358).

A tracker-only kind: it pairs with any code host and admits by its binding's
account allowlist or group. It has no code host side.
"""

from curie_api.forges.jira_cloud.connection import JiraTrackerConnection
from curie_api.forges.registry import register

register("jira_cloud", tracker=JiraTrackerConnection, code_host=None)

"""Bitbucket Cloud as a code host (ADR 0197, #4401)."""

from curie_api.forges.bitbucket_cloud.connection import BitbucketCloudConnection
from curie_api.forges.registry import register

register("bitbucket_cloud", tracker=None, code_host=BitbucketCloudConnection)

"""Bitbucket Data Center as a code host (ADR 0197, #4401)."""

from curie_api.forges.bitbucket_dc.connection import BitbucketDcConnection
from curie_api.forges.registry import register

register("bitbucket_dc", tracker=None, code_host=BitbucketDcConnection)

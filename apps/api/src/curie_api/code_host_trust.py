"""The TLS trust the API's code host HTTP clients use (ADR 0197, #3831).

A self-managed code host may present a certificate no public root covers. The
operator mounts one PEM bundle (``codeHostTrust.caBundle`` in the chart) and the
API names its path in ``CURIE_CODE_HOST_CA_BUNDLE``. Each code host client then
verifies against the public roots plus that bundle, so an install that also
reaches public GitHub keeps working with a bundle holding only its private CA.
Unset, the clients verify exactly as before.
"""

from __future__ import annotations

import ssl
from functools import lru_cache

import certifi

from .config import Settings


@lru_cache(maxsize=4)
def _context(bundle: str) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=certifi.where())
    context.load_verify_locations(cafile=bundle)
    return context


def code_host_verify(settings: Settings) -> ssl.SSLContext | bool:
    """``verify=`` for an httpx client that reaches a code host."""

    bundle = settings.code_host_ca_bundle.strip()
    return _context(bundle) if bundle else True

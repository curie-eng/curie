"""Private gateway trust is scoped to its configured origin."""

import os
import ssl
from functools import lru_cache
from urllib.parse import urlsplit

import httpx


@lru_cache(maxsize=4)
def _context(ca_file: str) -> ssl.SSLContext:
    context = ssl.create_default_context()
    try:
        context.load_verify_locations(cafile=ca_file)
    except FileNotFoundError:
        # A publicly trusted TLS Secret may omit the optional CA projection.
        pass
    return context


def gateway_verify() -> ssl.SSLContext | bool:
    ca_file = os.environ.get("CURIE_CHANNEL_GATEWAY_CA_FILE", "")
    return _context(ca_file) if ca_file else True


def gateway_http_mounts(endpoint: str) -> dict[str, httpx.AsyncHTTPTransport]:
    if not endpoint or not os.environ.get("CURIE_CHANNEL_GATEWAY_CA_FILE"):
        return {}
    origin = urlsplit(endpoint)
    return {f"{origin.scheme}://{origin.netloc}": httpx.AsyncHTTPTransport(verify=gateway_verify())}


def gateway_file_context(url: str) -> ssl.SSLContext | None:
    endpoint = os.environ.get("CURIE_CHANNEL_GATEWAY_URL", "")
    if not endpoint or not os.environ.get("CURIE_CHANNEL_GATEWAY_CA_FILE"):
        return None
    expected, actual = urlsplit(endpoint), urlsplit(url)
    if (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc):
        return None
    return _context(os.environ["CURIE_CHANNEL_GATEWAY_CA_FILE"])

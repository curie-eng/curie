"""The ObjectStore port: BundleStore satisfies it.

These are pure unit tests -- no RustFS/S3 required. They pin the extracted port
(#282, ADR-0026): the existing BundleStore is an ObjectStore, and the shared S3
client factory stays path-style so a second S3-backed site cannot drift.
"""

from curie_api.config import Settings
from curie_api.storage import BundleStore, ObjectStore, build_s3_client


def _settings() -> Settings:
    return Settings(
        s3_endpoint_url="http://localhost:29000",
        s3_access_key="rustfs",
        s3_secret_key="rustfssecret",
        s3_region="us-east-1",
        bundle_bucket="bundles",
    )


def test_bundle_store_satisfies_port() -> None:
    # boto3.client construction is offline; no network call is made here.
    store = BundleStore(_settings())
    assert isinstance(store, ObjectStore)


def test_build_s3_client_is_path_style() -> None:
    client = build_s3_client(_settings())
    # Path-style addressing is the alignment RustFS requires; assert the shared
    # factory pins it so a second S3-backed site cannot silently drift.
    assert client.meta.config.s3["addressing_style"] == "path"

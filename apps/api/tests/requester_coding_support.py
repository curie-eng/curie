"""Real application, owner sessions and Postgres for coding profile proofs."""

from collections.abc import Iterator
from typing import Any

import personal_enrollment_support as enrollment
import personal_execution_support as pe
import pytest
from fastapi.testclient import TestClient
from oidc_test_idp import TestIdP
from personal_enrollment_support import FakeCustodyStore, Owner

PATH = "/coding-instructions"
TEXT = "Prefer explicit names.\n"
SENTINEL = "EXAMPLE_CODING_TEXT_SENTINEL_982314"


@pytest.fixture
def owned(clean_db: None) -> Iterator[None]:
    pe.cleanup_owned()
    yield
    assert pe.cleanup_owned() == 0


@pytest.fixture
def idp(owned: None) -> Iterator[TestIdP]:
    with enrollment.identity_provider() as provider:
        yield provider


@pytest.fixture
def store() -> FakeCustodyStore:
    return FakeCustodyStore()


@pytest.fixture
def api(idp: TestIdP, store: FakeCustodyStore) -> Iterator[TestClient]:
    with enrollment.enrollment_app(store) as client:
        yield client


@pytest.fixture
def owner(api: TestClient, idp: TestIdP) -> Owner:
    return enrollment.seed_owner(enrollment.seed_tenant(), issuer=idp.issuer)


def get(api: TestClient, owner: Owner) -> Any:
    return api.get(PATH, headers=owner.headers(origin=None))


def put(api: TestClient, owner: Owner, value: str = TEXT, revision: int = 0) -> Any:
    return api.put(
        PATH, json={"text": value, "expected_revision": revision}, headers=owner.headers()
    )


def clear(api: TestClient, owner: Owner, revision: int) -> Any:
    return api.delete(PATH, params={"expected_revision": revision}, headers=owner.headers())


def body(response: Any) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    assert response.headers["Cache-Control"] == "no-store"
    found: dict[str, Any] = response.json()
    assert found["application"] == "unavailable"
    assert set(found) == {
        "revision",
        "sha256",
        "format_version",
        "byte_length",
        "text",
        "updated_at",
        "tenant_enabled",
        "application",
    }
    return found


def snapshot(owner: Owner) -> list[dict[str, Any]]:
    return pe.rows(
        "SELECT * FROM curie.requester_coding_profiles "
        "WHERE tenant_id = :tenant AND principal_id = :principal",
        {"tenant": owner.tenant_id, "principal": owner.principal_id},
    )

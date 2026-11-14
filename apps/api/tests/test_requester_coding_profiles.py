"""Owner profile lifecycle, isolation, redaction and policy through the real API."""

# ruff: noqa: F811

from __future__ import annotations

import base64
import hashlib
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import personal_enrollment_support as enrollment
import personal_execution_support as pe
import pytest
import requester_coding_support as support
from curie_api.config import get_settings
from curie_api.crud import console
from curie_internal.requester_coding_instructions import MAX_BYTES, MAX_REVISION
from fastapi.testclient import TestClient
from oidc_test_idp import TestIdP
from personal_enrollment_support import Owner
from requester_coding_support import api, idp, owned, owner, store  # noqa: F401
from sqlalchemy.ext.asyncio import AsyncSession

VECTORS = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests/vectors/requester-coding-instructions.json"
    ).read_text()
)["cases"]
MAX_REQUEST_BYTES = 131_072


@pytest.mark.parametrize("shape", ["declared", "chunked", "understated", "unparseable"])
def test_body_cap_precedes_decoding_and_authentication(api: TestClient, shape: str) -> None:
    oversized = b"\xff" * (MAX_REQUEST_BYTES + 1)
    headers = {"Content-Type": "application/json"}
    if shape in {"chunked", "understated"}:
        content: Any = iter((oversized[:MAX_REQUEST_BYTES], oversized[MAX_REQUEST_BYTES:]))
        if shape == "understated":
            headers["Content-Length"] = "1"
    else:
        content = oversized
        if shape == "unparseable":
            headers["Content-Length"] = "invalid"
    response = api.put(support.PATH, content=content, headers=headers)
    assert response.status_code == 413
    assert response.json() == {"detail": {"code": "too_large"}}
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize(
    "document", ["x" * MAX_BYTES, "\x01" * MAX_BYTES], ids=["plain", "escaped"]
)
def test_body_at_cap_preserves_the_document_and_requires_a_session(
    api: TestClient, owner: Owner, chunked: bool, document: str
) -> None:
    encoded = json.dumps({"text": document, "expected_revision": 0}).encode()
    encoded += b" " * (MAX_REQUEST_BYTES - len(encoded))
    headers = {"Content-Type": "application/json"}
    content: Any = iter((encoded[:MAX_BYTES], encoded[MAX_BYTES:])) if chunked else encoded
    assert api.put(support.PATH, content=content, headers=headers).status_code == 401
    content = iter((encoded[:MAX_BYTES], encoded[MAX_BYTES:])) if chunked else encoded
    saved = support.body(
        api.put(support.PATH, content=content, headers={**owner.headers(), **headers})
    )
    assert saved["text"] == document
    assert saved["byte_length"] == MAX_BYTES


def test_create_replace_clear_retains_revision(api: TestClient, owner: Owner) -> None:
    assert support.body(support.get(api, owner)) == {
        "revision": 0,
        "sha256": None,
        "format_version": 1,
        "byte_length": 0,
        "text": None,
        "updated_at": None,
        "tenant_enabled": True,
        "application": "unavailable",
    }
    first = support.body(support.put(api, owner, "first\r\nsecond\r  \t\n"))
    assert first["text"] == "first\nsecond\n  \t\n"
    assert first["revision"] == 1 and first["format_version"] == 1
    assert first["sha256"] == hashlib.sha256(first["text"].encode()).hexdigest()
    assert first["byte_length"] == len(first["text"].encode())
    assert first["updated_at"] is not None
    assert support.body(support.get(api, owner)) == first
    replaced = support.body(support.put(api, owner, "second\n", 1))
    assert replaced["revision"] == 2 and replaced["text"] == "second\n"
    cleared = support.body(support.clear(api, owner, 2))
    assert cleared["revision"] == 3
    assert cleared["text"] is None and cleared["sha256"] is None and cleared["byte_length"] == 0
    assert support.body(support.get(api, owner)) == cleared
    assert support.body(support.put(api, owner, "third", 3))["revision"] == 4
    assert support.body(support.put(api, owner, "", 4))["revision"] == 5
    assert support.body(support.clear(api, owner, 5))["revision"] == 6


@pytest.mark.parametrize("case", VECTORS, ids=lambda case: case["name"])
def test_shared_vectors_through_api_parser(
    api: TestClient, owner: Owner, case: dict[str, Any]
) -> None:
    raw = base64.b64decode(case["input_base64"])
    if "input_text" in case:
        response = support.put(api, owner, raw.decode("utf-8"))
    else:
        response = api.put(
            support.PATH,
            content=b'{"text":"' + raw + b'","expected_revision":0}',
            headers={**owner.headers(), "Content-Type": "application/json"},
        )
    if case["error"] is not None:
        assert response.status_code == 422
        assert response.json() == {"detail": {"code": case["error"]}}
        assert support.snapshot(owner) == []
    else:
        found = support.body(response)
        assert found["revision"] == 1
        assert found["text"] == (case["normalized_text"] or None)
        assert found["byte_length"] == case["byte_length"]
        assert found["sha256"] == case["sha256"]


@pytest.mark.parametrize(
    "body",
    [
        {"text": support.SENTINEL},
        {"text": support.SENTINEL, "expected_revision": True},
        {"text": support.SENTINEL, "expected_revision": -1},
        {"text": support.SENTINEL, "expected_revision": MAX_REVISION + 1},
        {"text": support.SENTINEL, "expected_revision": "0"},
        {"text": support.SENTINEL, "expected_revision": 0, "principal_id": str(uuid.uuid4())},
        {"text": [support.SENTINEL], "expected_revision": 0},
        {"text": None, "expected_revision": 0},
        [support.SENTINEL],
    ],
)
def test_shape_failures_are_redacted(
    api: TestClient, owner: Owner, body: Any, caplog: pytest.LogCaptureFixture
) -> None:
    before = support.snapshot(owner)
    response = api.put(support.PATH, json=body, headers=owner.headers())
    assert response.status_code == 422
    assert response.json() == {"detail": {"code": "invalid_text"}}
    assert support.SENTINEL not in response.text + caplog.text
    assert support.snapshot(owner) == before


@pytest.mark.parametrize(
    "raw",
    [
        b'{"text":"EXAMPLE_CODING_TEXT_SENTINEL_982314",',
        b'{"text":"EXAMPLE_CODING_TEXT_SENTINEL_982314\xff","expected_revision":0}',
        b'{"text":"EXAMPLE_CODING_TEXT_SENTINEL_982314\\ud800","expected_revision":0}',
        '{"text":"EXAMPLE_CODING_TEXT_SENTINEL_982314","expected_revision":0}'.encode("utf-16-le"),
    ],
)
def test_raw_json_and_surrogates_are_redacted(
    api: TestClient, owner: Owner, raw: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    response = api.put(
        support.PATH,
        content=raw,
        headers={**owner.headers(), "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": {"code": "invalid_text"}}
    assert support.SENTINEL not in response.text + caplog.text
    assert support.snapshot(owner) == []


@pytest.mark.parametrize("other_tenant", [False, True])
def test_another_owner_cannot_target_the_document(
    api: TestClient, owner: Owner, idp: TestIdP, other_tenant: bool
) -> None:
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    colleague = enrollment.seed_owner(
        enrollment.seed_tenant() if other_tenant else owner.tenant_id, issuer=idp.issuer
    )
    assert support.body(support.get(api, colleague))["revision"] == 0
    for response in (support.put(api, colleague, "foreign", 1), support.clear(api, colleague, 1)):
        assert response.status_code == 409
        assert response.json() == {"detail": {"code": "stale_revision", "current_revision": 0}}
    assert support.snapshot(owner) == before
    assert support.body(support.put(api, colleague, "colleague"))["text"] == "colleague"
    assert support.snapshot(owner) == before


@pytest.mark.parametrize(
    "header",
    [
        "X-API-Key",
        "X-Curie-Approval-Principal",
        "X-Curie-Adapter-Principal",
        "X-Curie-Worker-Token",
        "Authorization",
    ],
)
def test_foreign_credentials_are_never_owner_authority(
    api: TestClient, owner: Owner, header: str
) -> None:
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    value = get_settings().api_key if header == "X-API-Key" else "example-credential"
    for headers in (
        {header: value, "Origin": enrollment.ORIGIN},
        {**owner.headers(), header: value},
    ):
        for method in ("GET", "PUT", "DELETE"):
            kwargs: dict[str, Any] = {}
            if method == "PUT":
                kwargs["json"] = {"text": "foreign", "expected_revision": 1}
            if method == "DELETE":
                kwargs["params"] = {"expected_revision": 1}
            assert api.request(method, support.PATH, headers=headers, **kwargs).status_code == 401
    assert support.snapshot(owner) == before


@pytest.mark.parametrize("refusal", ["revoked", "disabled", "login_code"])
def test_only_live_active_principal_sessions_authenticate(
    api: TestClient, owner: Owner, refusal: str
) -> None:
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    if refusal == "disabled":
        pe.rows(
            "UPDATE curie.principals SET status = 'disabled' WHERE id = :p",
            {"p": owner.principal_id},
        )
    elif refusal == "revoked":
        pe.rows(
            "UPDATE curie.console_sessions SET revoked_at = now() WHERE principal_id = :p",
            {"p": owner.principal_id},
        )
    else:

        async def login_code(session: AsyncSession) -> str:
            code, _ = await console.create_console_login_code(session, subject="operator")
            exchanged = await console.exchange_console_login_code(session, code)
            assert exchanged is not None
            return exchanged[0]

        owner = Owner(owner.tenant_id, owner.principal_id, pe.with_session(login_code))
    for response in (
        support.get(api, owner),
        support.put(api, owner, "rejected", 1),
        support.clear(api, owner, 1),
    ):
        assert response.status_code == 401
    assert support.snapshot(owner) == before


@pytest.mark.parametrize(
    "origin", [None, "https://other.example.com", "https://sibling.testserver"]
)
def test_mutation_requires_origin(api: TestClient, owner: Owner, origin: str | None) -> None:
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    for response in (
        api.put(
            support.PATH,
            json={"text": "rejected", "expected_revision": 1},
            headers=owner.headers(origin=origin),
        ),
        api.delete(
            support.PATH, params={"expected_revision": 1}, headers=owner.headers(origin=origin)
        ),
    ):
        assert response.status_code == 403
    assert support.snapshot(owner) == before
    assert support.body(support.get(api, owner))["revision"] == 1


def test_stale_revision_and_bigint_exhaustion(api: TestClient, owner: Owner) -> None:
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    for response in (support.put(api, owner, "rejected", 0), support.clear(api, owner, 0)):
        assert response.status_code == 409
        assert response.json() == {"detail": {"code": "stale_revision", "current_revision": 1}}
    assert support.snapshot(owner) == before
    pe.rows(
        "UPDATE curie.requester_coding_profiles SET revision = :maximum WHERE principal_id = :p",
        {"maximum": MAX_REVISION, "p": owner.principal_id},
    )
    before = support.snapshot(owner)
    for response in (
        support.put(api, owner, "rejected", MAX_REVISION),
        support.clear(api, owner, MAX_REVISION),
    ):
        assert response.status_code == 409
        assert response.json() == {"detail": {"code": "revision_exhausted"}}
    assert support.snapshot(owner) == before


@pytest.mark.parametrize("initial", [False, True])
def test_same_revision_writers_advance_once(api: TestClient, owner: Owner, initial: bool) -> None:
    revision = 0
    if initial:
        revision = support.body(support.put(api, owner))["revision"]
    barrier = threading.Barrier(2, timeout=20)

    def writer(value: str) -> Any:
        barrier.wait()
        return support.put(api, owner, value, revision)

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(writer, ["first", "second"]))
    responses.sort(key=lambda response: response.status_code)
    assert [response.status_code for response in responses] == [200, 409]
    assert responses[1].json() == {
        "detail": {"code": "stale_revision", "current_revision": revision + 1}
    }
    found = support.body(support.get(api, owner))
    assert found == responses[0].json()
    assert found["text"] in {"first", "second"}


def test_policy_default_toggle_owner_management_and_authority(
    api: TestClient, owner: Owner
) -> None:
    path = f"/identity/admin/{owner.tenant_id}/coding-instructions-policy"
    headers = {"X-API-Key": get_settings().api_key}
    assert api.get(path, headers=headers).json() == {"enabled": True}
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    for method in ("GET", "PUT"):
        response = api.request(
            method,
            path,
            headers=owner.headers(),
            **({"json": {"enabled": False}} if method == "PUT" else {}),
        )
        assert response.status_code == 401
    assert support.snapshot(owner) == before
    for value in (False, True):
        response = api.put(path, json={"enabled": value}, headers=headers)
        assert response.status_code == 200 and response.json() == {"enabled": value}
        read = api.get(path, headers=headers)
        assert read.status_code == 200 and read.json() == {"enabled": value}
        revision = support.body(support.get(api, owner))["revision"]
        changed = support.body(support.put(api, owner, "still editable", revision))
        assert changed["tenant_enabled"] is value
        cleared = support.body(support.clear(api, owner, changed["revision"]))
        assert cleared["tenant_enabled"] is value and cleared["text"] is None
    unknown = f"/identity/admin/{uuid.uuid4()}/coding-instructions-policy"
    assert api.get(unknown, headers=headers).status_code == 404
    assert api.put(unknown, headers=headers, json={"enabled": False}).status_code == 404
    assert api.put(path, headers=headers, json={"enabled": "false"}).status_code == 422
    assert api.get(support.PATH, headers=headers).status_code == 401


def test_enrollment_replacement_reconnect_disconnect_preserve_document(
    api: TestClient, owner: Owner
) -> None:
    support.body(support.put(api, owner))
    before = support.snapshot(owner)
    assert enrollment.put(api, owner, "enroll").status_code == 200
    assert enrollment.put(api, owner, "replace", version=1).status_code == 200
    assert enrollment.disconnect(api, owner, 2).status_code == 200
    assert enrollment.put(api, owner, "reconnect", version=3).status_code == 200
    assert enrollment.disconnect(api, owner, 4).status_code == 200
    assert support.snapshot(owner) == before
    assert support.body(support.get(api, owner))["text"] == support.TEXT


def test_database_failure_never_exposes_text(
    api: TestClient, owner: Owner, caplog: pytest.LogCaptureFixture
) -> None:
    name = "coding_profile_failure_" + uuid.uuid4().hex
    pe.rows(
        f"CREATE FUNCTION curie.{name}() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN RAISE EXCEPTION 'rejected instruction: %', NEW.text; END $$"
    )
    try:
        pe.rows(
            f"CREATE TRIGGER {name} BEFORE INSERT OR UPDATE ON "
            f"curie.requester_coding_profiles FOR EACH ROW EXECUTE FUNCTION curie.{name}()"
        )
        response = support.put(api, owner, support.SENTINEL)
        assert response.status_code == 503
        assert response.json() == {"detail": {"code": "coding_instructions_unavailable"}}
        assert support.SENTINEL not in response.text + caplog.text
        assert support.snapshot(owner) == []
    finally:
        pe.rows(f"DROP TRIGGER IF EXISTS {name} ON curie.requester_coding_profiles")
        pe.rows(f"DROP FUNCTION curie.{name}()")

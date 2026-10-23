"""Shared connector declarations at API intake and in every post-intake reader (cp5, AC1).

``curie_internal.shared_declarations.parse_declarations`` is the one strict
parse API and worker share: an entry carrying ``shared`` goes through the
frozen ``ConnectorSpec`` (its ``exclusive_shared`` refuses any mixing) and the
dedicated subset through the frozen ``validate_connectors``. Intake wraps the
frozen ``validate_bundle``: it validates the ORIGINAL ``deploy.yaml`` first so
every target refusal keeps its own code, validates the bundle in place with
``connectors.yaml`` rewritten to the dedicated subset, restores both files byte
for byte, then resolves each shared alias the deploying agent's target runs
against the shared store: the application exists in this installation, is
enabled, and the agent is a member under exactly that alias. Every intake
site (upload, ``POST /deployments``, git-flow) resolves; a caller that does not
opt in keeps the frozen ``connectors.shared_connector_requires_application``.

Post-intake readers never fall back to an empty connector set: a stored bundle
that does not parse fails closed with a code, and a shared alias is handled
explicitly (no custody, no digest authority, never an independent verifier).

Real Postgres and RustFS throughout; nothing internal is faked. The git-flow
case pushes to a local bare repository, as ``test_gitflow_integration.py``
does, with the webhook signed exactly as GitHub signs it.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from _migration_support import sql_dicts
from _shared_connector_support import (
    HOSTED_DEDICATED,
    PLUGIN_NAME,
    REMOTE_DEDICATED,
    SharedCleanup,
    SharedStore,
    assert_declaration_refused,
    bundle_archive,
    bundle_files,
    bundle_ref,
    capture_debug_logs,
    captured_log_text,
    connectors_yaml,
    deployment_count,
    detail_code,
    operator_headers,
    random_installation,
    restore_installation,
    shared_app,
    shared_entry,
    tree_digest,
    write_tree,
)
from curie_api import bundles, deploy
from curie_api.action_undoable import sealing_custody
from curie_api.config import get_settings
from curie_api.crud import versions as crud_versions
from curie_api.remediation_forward import in_force_connector_digest
from curie_api.remediation_verifier import NOT_INDEPENDENT, independence_refusal
from curie_api.shared_connectors.declarations import DeclarationRefused, read_declarations
from curie_api.storage import BundleStore, build_s3_client
from curie_internal.shared_declarations import (
    Declarations,
    SharedEntry,
    dedicated_subset,
    has_shared,
    parse_declarations,
)
from plugin_format import TOOL_POLICY_ENFORCEMENT, bundle_root, validate_bundle
from plugin_format.connectors import ConnectorsFile, validate_connectors
from plugin_format.deploy_targets import validate_deploy_targets
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ALIAS = "tracker"
REQUIRES_APPLICATION = "connectors.shared_connector_requires_application"
ACT_DIGEST = "sha256:" + "ab" * 32
ACT_IMAGE = f"ghcr.io/example/k8s-restorer@{ACT_DIGEST}"
READ_IMAGE = "ghcr.io/example/metrics-mcp@sha256:" + "ef" * 32
# The acting connector holds the sealing key as a SecretRef (its accepted
# form); the read connector's header expands a different secret name.
SEALED_ACTING = (
    "  k8s:\n"
    f"    image: {ACT_IMAGE}\n"
    "    secrets:\n"
    "      - name: SNAPSHOT_SEALING_KEY\n"
    "        from_secret: k8s-restorer-seal\n"
)
METRICS_READER = f"  metrics:\n    image: {READ_IMAGE}\n    secrets: [METRICS_TOKEN]\n"
WIKI_REMOTE = "  wiki:\n    url: https://wiki.example/mcp\n"


@pytest.fixture
def cleanup(migrated: None) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def store(cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


# --- helpers ----------------------------------------------------------------


def _reference(application_id: str) -> dict[str, Any]:
    return {"application": application_id, "contract": 1}


def _mixed(application_id: str, *dedicated: str, alias: str = ALIAS) -> str:
    return connectors_yaml(shared_entry(alias, application_id), *(dedicated or (HOSTED_DEDICATED,)))


def _invalid(response: httpx.Response) -> list[tuple[str, str]]:
    """The ``(code, message)`` pairs of an upload's 422 ``bundle failed validation``."""

    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert detail["detail"] == "bundle failed validation", response.text
    return [(error["code"], error["message"]) for error in detail["errors"]]


def _refused(response: httpx.Response, status: int, code: str) -> None:
    """A resolution refusal: the exact status and code, in either body form."""

    assert response.status_code == status, response.text
    assert detail_code(response) == code, response.text


def _stored_objects(agent_id: str) -> int:
    settings = get_settings()
    listed = build_s3_client(settings).list_objects_v2(
        Bucket=settings.bundle_bucket, Prefix=f"bundles/{agent_id}/"
    )
    return int(listed.get("KeyCount", 0))


def _session_call(factory: Callable[[Any], Any]) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await factory(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _store_directly(agent_id: str, version_id: str, data: bytes) -> None:
    """Attach bytes to a version with no intake at all: a bundle stored before cp5."""

    async def run(session: Any) -> None:
        version = await crud_versions.get_version(session, uuid.UUID(version_id))
        assert version is not None
        await deploy.store_bundle(
            BundleStore(get_settings()),
            session,
            uuid.UUID(agent_id),
            version,
            data,
            ".tar.gz",
            "application/gzip",
        )

    _session_call(run)


def _extracted(data: bytes, dest: Path) -> Path:
    bundles.extract_stored_bundle(data, dest)
    return bundle_root(dest)


def _frozen_result(data: bytes) -> Any:
    """What intake answered before cp5: frozen ``validate_bundle`` plus custody."""

    with tempfile.TemporaryDirectory() as tmp:
        root = _extracted(data, Path(tmp))
        result = validate_bundle(root, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
        custody = bundles.sealing_key_custody_issues(root)
    if custody:
        result = type(result)(
            valid=False, errors=[*result.errors, *custody], warnings=result.warnings
        )
    return result


# --- 1 to 3. the parser ------------------------------------------------------


def test_parse_shared_entry_strictly_and_keep_dedicated_validation() -> None:
    application_id = str(uuid.uuid4())
    hosted = {"image": "grafana/mcp-grafana:0.17.2", "secrets": ["GRAFANA_TOKEN"]}
    data = {"connectors": {ALIAS: {"shared": _reference(application_id)}, "grafana": hosted}}
    assert has_shared(data)
    assert dedicated_subset(data) == {"connectors": {"grafana": hosted}}

    declarations, errors = parse_declarations(data)
    assert errors == []
    assert isinstance(declarations, Declarations)
    assert declarations.shared == (SharedEntry(name=ALIAS, application=application_id),)
    frozen, frozen_errors = validate_connectors({"connectors": {"grafana": hosted}})
    assert frozen_errors == [] and frozen is not None
    assert declarations.dedicated.model_dump() == frozen.model_dump()
    assert declarations.names() == frozenset({ALIAS, "grafana"})
    # The target restriction applies to both halves.
    narrowed = declarations.restricted(frozenset({"grafana"}))
    assert narrowed.shared == () and set(narrowed.dedicated.connectors) == {"grafana"}
    assert declarations.restricted(None) == declarations

    # Negative: a dedicated error beside a valid shared entry is the frozen one.
    broken = {"connectors": {ALIAS: {"shared": _reference(application_id)}, "bad name": hosted}}
    refused, refusal = parse_declarations(broken)
    assert refused is None
    assert refusal == validate_connectors(dedicated_subset(broken))[1]
    assert [code for code, _ in refusal] == ["connectors.bad_name"]

    # No file, and a file that is not a mapping, behave as the frozen validator does.
    empty, none_errors = parse_declarations(None)
    assert none_errors == [] and empty is not None
    assert empty.shared == () and empty.dedicated == ConnectorsFile()
    assert parse_declarations(["not", "a", "mapping"]) == (
        None,
        validate_connectors(["not", "a", "mapping"])[1],
    )
    assert not has_shared({"connectors": {"grafana": hosted}})


_APPLICATION = str(uuid.uuid4())
MIXED_ENTRIES: dict[str, dict[str, Any]] = {
    "image": {"shared": _reference(_APPLICATION), "image": "grafana/mcp-grafana:0.17.2"},
    "build": {"shared": _reference(_APPLICATION), "build": {"context": "."}},
    "url": {"shared": _reference(_APPLICATION), "url": "https://mcp.example.com/mcp"},
    "secrets": {"shared": _reference(_APPLICATION), "secrets": ["TOKEN"]},
    "secret_files": {"shared": _reference(_APPLICATION), "secret_files": {"a.json": "A"}},
    "env": {"shared": _reference(_APPLICATION), "env": {"A": "b"}},
    "headers": {"shared": _reference(_APPLICATION), "headers": {"X": "y"}},
    "bearer_secret": {"shared": _reference(_APPLICATION), "bearer_secret": "TOKEN"},
    "proxy_upstream_host": {"shared": _reference(_APPLICATION), "proxy_upstream_host": "h"},
    "null": {"shared": None},
    "uppercase-uuid": {"shared": {"application": _APPLICATION.upper(), "contract": 1}},
    "nil-uuid": {"shared": {"application": str(uuid.UUID(int=0)), "contract": 1}},
    "contract-true": {"shared": {"application": _APPLICATION, "contract": True}},
    "contract-two": {"shared": {"application": _APPLICATION, "contract": 2}},
    "extra-key": {"shared": {"application": _APPLICATION, "contract": 1, "region": "x"}},
}


@pytest.mark.parametrize("case", sorted(MIXED_ENTRIES))
def test_mixed_shared_entry_refuses_by_field(case: str) -> None:
    data = {"connectors": {ALIAS: MIXED_ENTRIES[case]}}
    declarations, errors = parse_declarations(data)
    assert declarations is None
    assert errors, case
    code, message = errors[0]
    assert code == "connectors.shared_invalid", errors
    assert message.startswith(f"connectors.{ALIAS}: "), message
    assert len(message) <= len(f"connectors.{ALIAS}: ") + 400
    # Never the frozen "requires application" refusal and never an empty success.
    assert all(code != REQUIRES_APPLICATION for code, _ in errors)

    # Liveness: the same connector with only ``shared`` parses.
    alone, alone_errors = parse_declarations(
        {"connectors": {ALIAS: {"shared": _reference(_APPLICATION)}}}
    )
    assert alone_errors == []
    assert alone is not None and alone.shared == (SharedEntry(ALIAS, _APPLICATION),)


NAME_CORPUS = (
    "a",
    "tracker",
    "my-tracker",
    "x1",
    "1x",
    "a" * 40,
    "a" * 41,
    "Tracker",
    "bad_name",
    "bad name",
    "-lead",
    "trail-",
    "dot.name",
    "",
    "curie",
    "curie-state",
    "curie-slack",
    "curie-docs",
    "mcp-github",
    "github-direct",
)


def test_shared_name_rule_matches_frozen_dedicated_rule() -> None:
    accepted: list[str] = []
    for name in NAME_CORPUS:
        _, dedicated = validate_connectors(
            {"connectors": {name: {"url": "https://example.invalid/mcp"}}}
        )
        _, shared = parse_declarations({"connectors": {name: {"shared": _reference(_APPLICATION)}}})
        assert {code for code, _ in shared} == {code for code, _ in dedicated}, name
        if not shared:
            accepted.append(name)
    # The corpus exercises both sides of the rule.
    assert "tracker" in accepted and "a" * 40 in accepted
    assert "a" * 41 not in accepted and "curie" not in accepted and "Tracker" not in accepted


# --- 4 to 6. upload ------------------------------------------------------------


def test_upload_accepts_member_bundle_and_stores_it(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, ALIAS)
    version_id = store.create_version(agent_id)

    uploaded = store.upload(agent_id, version_id, bundle_archive(_mixed(application_id)))
    assert uploaded.status_code == 201, uploaded.text
    assert bundle_ref(version_id) == uploaded.json()["bundle_ref"]
    assert _stored_objects(agent_id) == 1

    # Secondary: a broken dedicated sibling still answers its frozen code.
    ambiguous = (
        "  grafana:\n    image: grafana/mcp-grafana:0.17.2\n    url: https://g.example.com/mcp\n"
    )
    broken = store.create_version(agent_id)
    archive = bundle_archive(_mixed(application_id, ambiguous))
    errors = _invalid(store.upload(agent_id, broken, archive))
    assert "connectors.ambiguous" in {code for code, _ in errors}
    assert REQUIRES_APPLICATION not in {code for code, _ in errors}
    assert bundle_ref(broken) is None


def test_upload_refuses_each_resolution_failure(
    store: SharedStore, cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent_id = store.create_agent()

    def attempt(application_id: str, alias: str = ALIAS) -> tuple[str, httpx.Response]:
        version_id = store.create_version(agent_id)
        archive = bundle_archive(_mixed(application_id, alias=alias))
        return version_id, store.upload(agent_id, version_id, archive)

    unknown = str(uuid.uuid4())
    version_id, response = attempt(unknown)
    _refused(response, 409, "shared_application_unknown")
    assert bundle_ref(version_id) is None

    foreign_installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, foreign_installation) as client:
        foreign = SharedStore(client, foreign_installation, cleanup)
        foreign_id = foreign.register_application()["application_id"]
        foreign.member(foreign_id, agent_id, ALIAS)
    restore_installation(monkeypatch, store.installation)
    version_id, response = attempt(foreign_id)
    _refused(response, 409, "shared_application_unknown")
    assert bundle_ref(version_id) is None

    disabled = store.register_application()["application_id"]
    store.member(disabled, agent_id, ALIAS)
    assert store.disable_application(disabled).status_code == 200
    version_id, response = attempt(disabled)
    _refused(response, 409, "shared_application_disabled")
    assert bundle_ref(version_id) is None

    application_id = store.register_application()["application_id"]
    version_id, response = attempt(application_id)
    _refused(response, 409, "shared_application_not_member")
    assert bundle_ref(version_id) is None

    store.member(application_id, agent_id, "renamed")
    version_id, response = attempt(application_id)
    _refused(response, 409, "shared_application_not_member")
    assert bundle_ref(version_id) is None

    for installation, observed, code in (
        (None, "true", "shared_installation_unavailable"),
        (store.installation, "false", "shared_installation_unobserved"),
    ):
        with shared_app(monkeypatch, installation, observed=observed) as client:
            other = SharedStore(client, installation or "", cleanup)
            version_id = other.create_version(agent_id)
            response = other.upload(agent_id, version_id, bundle_archive(_mixed(application_id)))
            _refused(response, 409, code)
            assert bundle_ref(version_id) is None
    restore_installation(monkeypatch, store.installation)

    # Liveness: with the alias the bundle declares, the same upload passes.
    store.member(application_id, agent_id, ALIAS)
    version_id, response = attempt(application_id)
    assert response.status_code == 201, response.text
    assert bundle_ref(version_id) is not None


def test_upload_refuses_mixing_and_never_rewrites(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, ALIAS)
    store.uploaded(agent_id, bundle_archive(_mixed(application_id)))
    before = _stored_objects(agent_id)

    mixed = connectors_yaml(
        f"  {ALIAS}:\n    shared:\n      application: {application_id}\n      contract: 1\n"
        "    url: https://mcp.example.com/mcp\n"
    )
    version_id = store.create_version(agent_id)
    errors = _invalid(store.upload(agent_id, version_id, bundle_archive(mixed)))
    codes = {code for code, _ in errors}
    assert "connectors.shared_invalid" in codes, errors
    assert REQUIRES_APPLICATION not in codes
    assert bundle_ref(version_id) is None
    assert _stored_objects(agent_id) == before


# --- 7 and 8. the wrapper ----------------------------------------------------


def _dedicated_corpus() -> dict[str, bytes]:
    sealed_plain = (
        "  k8s:\n    image: grafana/mcp-grafana:0.17.2\n    secrets: [SNAPSHOT_SEALING_KEY]\n"
    )
    return {
        "no-connectors": bundle_archive(None),
        "hosted": bundle_archive(connectors_yaml(HOSTED_DEDICATED)),
        "remote": bundle_archive(connectors_yaml(REMOTE_DEDICATED)),
        "bad-name": bundle_archive(connectors_yaml("  Bad:\n    url: https://x.example/mcp\n")),
        "ambiguous": bundle_archive(
            connectors_yaml("  g:\n    image: a/b:1\n    url: https://x.example/mcp\n")
        ),
        "not-a-mapping": bundle_archive("- just\n- a list\n"),
        "custody": bundle_archive(connectors_yaml(sealed_plain)),
        "targets-valid": bundle_archive(
            connectors_yaml(HOSTED_DEDICATED),
            deploy="targets:\n  dev:\n    agent: acme-bot\n    connectors: [grafana]\n",
        ),
        "targets-unknown": bundle_archive(
            connectors_yaml(HOSTED_DEDICATED),
            deploy="targets:\n  dev:\n    agent: acme-bot\n    connectors: [ghost]\n",
        ),
        "targets-null": bundle_archive(
            connectors_yaml(HOSTED_DEDICATED),
            deploy="targets:\n  dev:\n    agent: acme-bot\n    connectors:\n",
        ),
    }


def test_dedicated_bundle_intake_unchanged(tmp_path: Path) -> None:
    corpus = _dedicated_corpus()
    valid = {name for name, data in corpus.items() if _frozen_result(data).valid}
    # The corpus has both outcomes.
    assert {"hosted", "remote", "no-connectors", "targets-valid"} <= valid
    assert {"bad-name", "ambiguous", "custody", "targets-unknown"}.isdisjoint(valid)
    for name, data in corpus.items():
        expected = _frozen_result(data)
        for allow_shared in (True, False):
            dest = tmp_path / f"{name}-{allow_shared}"
            dest.mkdir()
            _, _, result, shared = bundles.extract_and_validate(
                data, dest, allow_shared=allow_shared
            )
            assert result.model_dump() == expected.model_dump(), (name, allow_shared)
            assert shared == (), name
        if expected.valid:
            assert deploy.validate_archive(data, allow_shared=True) == (
                ".tar.gz",
                "application/gzip",
                (),
            )
        else:
            with pytest.raises(deploy.BundleInvalid) as refused:
                deploy.validate_archive(data, allow_shared=True)
            assert refused.value.errors == [error.model_dump() for error in expected.errors]


def test_validate_with_shared_restores_the_tree(tmp_path: Path) -> None:
    application_id = str(uuid.uuid4())
    entry = shared_entry(ALIAS, application_id)
    cases = {
        "valid-with-targets": bundle_files(
            connectors_yaml(entry, HOSTED_DEDICATED),
            deploy="targets:\n  dev:\n    agent: acme-bot\n    connectors: [tracker, grafana]\n",
        ),
        "valid-without-targets": bundle_files(connectors_yaml(entry, HOSTED_DEDICATED)),
        "invalid-targets": bundle_files(
            connectors_yaml(entry, HOSTED_DEDICATED),
            deploy="targets:\n  dev:\n    agent: acme-bot\n    connectors: [tracker, tracker]\n",
        ),
        "malformed-shared": bundle_files(
            connectors_yaml(
                f"  {ALIAS}:\n    shared:\n      application: {application_id}\n"
                "      contract: 1\n    image: a/b:1\n"
            ),
            deploy="targets:\n  dev:\n    agent: acme-bot\n    connectors: [tracker]\n",
        ),
        "broken-dedicated": bundle_files(
            connectors_yaml(entry, "  Bad:\n    url: https://x.example/mcp\n")
        ),
    }
    outcomes: dict[str, tuple[bool, set[str]]] = {}
    for name, files in cases.items():
        root = write_tree(tmp_path / name, files)
        before = tree_digest(root)
        result, declarations = bundles.validate_with_shared(root)
        assert tree_digest(root) == before, name
        codes = {error.code for error in result.errors}
        assert REQUIRES_APPLICATION not in codes, (name, codes)
        outcomes[name] = (result.valid, codes)
        if name.startswith("valid"):
            assert declarations is not None
            assert declarations.shared == (SharedEntry(ALIAS, application_id),)
        if name == "malformed-shared":
            assert declarations is None
    assert outcomes["valid-with-targets"] == (True, set())
    assert outcomes["valid-without-targets"] == (True, set())
    assert outcomes["invalid-targets"][0] is False
    assert "deploy.duplicate_connector" in outcomes["invalid-targets"][1]
    assert outcomes["malformed-shared"][0] is False
    assert "connectors.shared_invalid" in outcomes["malformed-shared"][1]
    assert outcomes["broken-dedicated"][0] is False
    assert "connectors.bad_name" in outcomes["broken-dedicated"][1]


# --- 9. deploy targets ---------------------------------------------------------


def test_deploy_target_alias_and_restriction(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    excluded, included = store.create_agent(), store.create_agent()

    # The target runs only the dedicated connector: no membership is needed.
    targets = (
        f"targets:\n  dev:\n    agent: {store.agent_name(excluded)}\n    connectors: [grafana]\n"
    )
    version_id = store.uploaded(excluded, bundle_archive(_mixed(application_id), deploy=targets))
    assert bundle_ref(version_id) is not None

    targets = (
        f"targets:\n  dev:\n    agent: {store.agent_name(included)}\n"
        f"    connectors: [grafana, {ALIAS}]\n"
    )
    archive = bundle_archive(_mixed(application_id), deploy=targets)
    version_id = store.create_version(included)
    _refused(store.upload(included, version_id, archive), 409, "shared_application_not_member")
    assert bundle_ref(version_id) is None
    # Liveness: once a member, the target listing the shared alias validates and stores.
    store.member(application_id, included, ALIAS)
    assert store.upload(included, version_id, archive).status_code == 201


def test_original_deploy_targets_refusals_survive(store: SharedStore, tmp_path: Path) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    name = store.agent_name(agent_id)
    store.member(application_id, agent_id, ALIAS)

    def targets(prod: str, dev: str) -> str:
        return (
            f"targets:\n  prod:\n    agent: {name}\n    env: prod\n{prod}"
            f"  dev:\n    agent: {name}\n{dev}"
        )

    cases = {
        "deploy.conflicting_connectors": targets(
            f"    connectors: [{ALIAS}]\n", "    connectors: []\n"
        ),
        "deploy.duplicate_connector": targets(
            f"    connectors: [{ALIAS}, {ALIAS}]\n", f"    connectors: [{ALIAS}, {ALIAS}]\n"
        ),
        "deploy.null_connectors": targets("    connectors:\n", "    connectors:\n"),
        "deploy.unknown_connector": targets(
            f"    connectors: [{ALIAS}, ghost]\n", f"    connectors: [{ALIAS}, ghost]\n"
        ),
    }
    for code, deploy_yaml in cases.items():
        if code == "deploy.unknown_connector":
            # The frozen validator's own message for the ghost, from the same
            # targets over a dedicated-only bundle. The shared bundle declares
            # the alias, so only the ghost may be unknown there.
            dedicated_only = connectors_yaml(HOSTED_DEDICATED, REMOTE_DEDICATED)
            root = write_tree(
                tmp_path / "unknown", bundle_files(dedicated_only, deploy=deploy_yaml)
            )
            frozen = [
                (error.code, error.message)
                for error in validate_bundle(
                    root, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT
                ).errors
                if error.code == code
            ]
            frozen = [(c, m) for c, m in frozen if "ghost" in m]
        else:
            frozen = [
                (c, m)
                for c, m in validate_deploy_targets(yaml.safe_load(deploy_yaml))[1]
                if c == code
            ]
        assert frozen, code
        version_id = store.create_version(agent_id)
        errors = _invalid(
            store.upload(
                agent_id, version_id, bundle_archive(_mixed(application_id), deploy=deploy_yaml)
            )
        )
        for pair in frozen:
            assert pair in errors, (code, errors)
        # Reported once, never twice through the rewritten copy.
        assert len([pair for pair in errors if pair[0] == code]) == len(frozen), errors
        assert REQUIRES_APPLICATION not in {c for c, _ in errors}
        assert bundle_ref(version_id) is None, code

    # Liveness: the alias on both targets passes.
    liveness = targets(f"    connectors: [{ALIAS}]\n", f"    connectors: [{ALIAS}]\n")
    version_id = store.uploaded(agent_id, bundle_archive(_mixed(application_id), deploy=liveness))
    assert bundle_ref(version_id) is not None


def test_stored_invalid_targets_never_unrestricted(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    name = store.agent_name(agent_id)
    store.member(application_id, agent_id, ALIAS)
    conflicting = (
        f"targets:\n  prod:\n    agent: {name}\n    env: prod\n    connectors: [{ALIAS}]\n"
        f"  dev:\n    agent: {name}\n    connectors: []\n"
    )
    shared_version = store.create_version(agent_id)
    _store_directly(
        agent_id,
        shared_version,
        bundle_archive(_mixed(application_id, REMOTE_DEDICATED), deploy=conflicting),
    )
    assert_declaration_refused(
        store.render(agent_id, shared_version), 422, "deploy.conflicting_connectors"
    )

    # Secondary: a dedicated-only stored bundle with the same invalid file
    # renders exactly as before cp5 (dedicated behavior unchanged).
    dedicated = conflicting.replace(f"[{ALIAS}]", "[wiki]")
    dedicated_version = store.create_version(agent_id)
    _store_directly(
        agent_id,
        dedicated_version,
        bundle_archive(connectors_yaml(REMOTE_DEDICATED, WIKI_REMOTE), deploy=dedicated),
    )
    rendered = store.render(agent_id, dedicated_version)
    assert rendered.status_code == 200, rendered.text
    # Invalid targets read as unrestricted for a dedicated-only bundle, as before.
    assert sorted(rendered.json()["mcp_entries"]) == ["internal", "wiki"]
    assert rendered.json()["shared"] == []


# --- 10 and 11. the other intake sites -------------------------------------


def test_post_deployments_rechecks_membership(store: SharedStore) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, ALIAS)
    version_id = store.uploaded(agent_id, bundle_archive(_mixed(application_id)))
    assert store.remove_member(application_id, agent_id).status_code == 200

    _refused(store.deploy(agent_id, version_id), 409, "shared_application_not_member")
    assert deployment_count(agent_id) == 0

    # Liveness: back as a member, the same deployment is created.
    store.member(application_id, agent_id, ALIAS)
    created = store.deploy(agent_id, version_id)
    assert created.status_code == 201, created.text
    assert deployment_count(agent_id) == 1


def _git(*args: str, cwd: Path | None = None) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    out = subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
    )
    return out.stdout.strip()


def _bare_repo(base: Path, repo: str, files: dict[str, str]) -> tuple[str, str]:
    """``<base>/<repo>.git`` holding one commit of ``files`` on ``dev``; (url, sha)."""

    work = base / "_work" / repo
    write_tree(work, files)
    _git("init", "-q", "-b", "dev", cwd=work)
    _git("add", "-A", cwd=work)
    _git("commit", "-q", "-m", "init", cwd=work)
    sha = _git("rev-parse", "HEAD", cwd=work)
    bare = base / f"{repo}.git"
    bare.parent.mkdir(parents=True, exist_ok=True)
    _git("clone", "--quiet", "--bare", str(work), str(bare))
    return f"file://{bare}", sha


def _push(client: Any, repo: str, sha: str, clone_url: str) -> dict[str, Any]:
    body = json.dumps(
        {
            "ref": "refs/heads/dev",
            "after": sha,
            "repository": {"full_name": repo, "clone_url": clone_url},
        }
    ).encode()
    secret = get_settings().github_webhook_secret
    signature = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    response = client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 200, response.text
    result: dict[str, Any] = response.json()
    return result


def test_gitflow_push_refuses_non_member_target(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base = tmp_path / "remotes"
    base.mkdir()
    monkeypatch.setenv("GITHUB_CLONE_BASE", f"file://{base}")
    get_settings.cache_clear()
    repo = f"octo/shared-{uuid.uuid4().hex[:10]}"
    created = store.call(
        "POST",
        "/agents",
        json={
            "name": f"shared-git-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": f"C{uuid.uuid4().hex[:10].upper()}"},
            "repo_full_name": repo,
        },
        headers=operator_headers(),
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    application_id = store.register_application()["application_id"]
    clone_url, sha = _bare_repo(base, repo, bundle_files(_mixed(application_id)))

    rejected = _push(store.client, repo, sha, clone_url)
    assert rejected["status"] == "rejected", rejected
    assert [error["code"] for error in rejected["errors"]] == ["shared_application_not_member"]
    assert all(
        row["bundle_ref"] is None
        for row in sql_dicts(
            "SELECT bundle_ref FROM curie.agent_versions WHERE agent_id = :id",
            {"id": uuid.UUID(agent_id)},
        )
    )
    assert deployment_count(agent_id) == 0

    # Liveness: once the agent is a member, a new delivery of the same commit deploys.
    store.member(application_id, agent_id, ALIAS)
    deployed = _push(store.client, repo, sha, clone_url)
    assert deployed["status"] == "deployed", deployed
    assert deployment_count(agent_id) == 1


# --- 12. an intake caller that does not opt in -----------------------------


def test_intake_without_allow_shared_fails_closed(tmp_path: Path) -> None:
    application_id = str(uuid.uuid4())
    data = bundle_archive(_mixed(application_id))
    with pytest.raises(deploy.BundleInvalid) as refused:
        deploy.validate_archive(data)
    assert REQUIRES_APPLICATION in {error["code"] for error in refused.value.errors}
    _, _, result, _ = bundles.extract_and_validate(data, tmp_path / "closed")
    assert not result.valid
    assert REQUIRES_APPLICATION in {error.code for error in result.errors}

    # Liveness: opting in parses the entry (resolution happens at the site).
    extension, content_type, shared = deploy.validate_archive(data, allow_shared=True)
    assert (extension, content_type) == (".tar.gz", "application/gzip")
    assert shared == (SharedEntry(ALIAS, application_id),)


# --- 13 and 14. post-intake readers ------------------------------------------


def test_post_intake_readers_handle_shared_explicitly(
    store: SharedStore, caplog: pytest.LogCaptureFixture
) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, ALIAS)
    version_id = store.uploaded(
        agent_id, bundle_archive(_mixed(application_id, SEALED_ACTING, METRICS_READER))
    )
    assert store.deploy(agent_id, version_id).status_code == 201
    agent = uuid.UUID(agent_id)
    objects = BundleStore(get_settings())

    with capture_debug_logs(caplog):
        custody = _session_call(lambda session: sealing_custody(session, objects, [agent]))
    # The dedicated holder keeps custody; the shared alias never has it.
    assert custody == {agent: frozenset({"k8s"})}
    assert "shared_connector_custody_unsupported" in captured_log_text(caplog.records)

    def digest(connector: str) -> Any:
        return _session_call(
            lambda session: in_force_connector_digest(session, objects, agent, connector)
        )

    assert digest(ALIAS) is None
    assert digest("k8s") == ACT_DIGEST

    def independence(acting: str, reading: str) -> Any:
        action = {"name": "restart", "connector": acting, "verifier": {"connector": reading}}
        return _session_call(
            lambda session: independence_refusal(session, agent, action, store=objects)
        )

    assert independence(ALIAS, "metrics") == NOT_INDEPENDENT
    assert independence("k8s", ALIAS) == NOT_INDEPENDENT
    # Liveness: two dedicated connectors with disjoint header secrets are independent.
    assert independence("k8s", "metrics") is None


def test_read_declarations_never_returns_empty_on_error(
    store: SharedStore, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    application_id = store.register_application()["application_id"]
    agent_id = store.create_agent()
    store.member(application_id, agent_id, ALIAS)
    malformed = connectors_yaml(
        f"  {ALIAS}:\n    shared:\n      application: {application_id}\n      contract: 1\n"
        "    image: a/b:1\n",
        SEALED_ACTING,
        METRICS_READER,
    )
    # The reader itself raises with the code instead of answering an empty file.
    root = write_tree(tmp_path / PLUGIN_NAME, bundle_files(malformed))
    with pytest.raises(DeclarationRefused) as raised:
        read_declarations(root)
    assert raised.value.status_code == 422
    detail = raised.value.detail
    assert (detail["code"] if isinstance(detail, dict) else detail) == "connectors.shared_invalid"

    # Deployed while bundleless, then the malformed bytes are attached with no
    # intake: the shape of a bundle stored before cp5.
    version_id = store.create_version(agent_id)
    deployed = store.deploy(agent_id, version_id)
    assert deployed.status_code == 201, deployed.text
    _store_directly(agent_id, version_id, bundle_archive(malformed))

    assert_declaration_refused(
        store.render(agent_id, version_id), 422, "connectors.shared_invalid", declared=None
    )

    agent = uuid.UUID(agent_id)
    objects = BundleStore(get_settings())
    with capture_debug_logs(caplog):
        custody = _session_call(lambda session: sealing_custody(session, objects, [agent]))
    assert custody == {agent: frozenset()}
    # Through the fail-closed branch, never a silent empty read.
    logged = captured_log_text(caplog.records)
    assert "in-force bundle unreadable" in logged
    assert "DeclarationRefused" in logged

    action = {"name": "restart", "connector": "k8s", "verifier": {"connector": "metrics"}}
    refusal = _session_call(
        lambda session: independence_refusal(session, agent, action, store=objects)
    )
    # Read as an empty file this would be None (neither connector found).
    assert refusal == NOT_INDEPENDENT
    digest = _session_call(
        lambda session: in_force_connector_digest(session, objects, agent, "k8s")
    )
    assert digest is None

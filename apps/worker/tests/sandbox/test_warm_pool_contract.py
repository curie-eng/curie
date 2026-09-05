"""Warm-pool capability projection (#1492 D1): deterministic, secret-free, cold-honest.

The projection is the version-stable subset of the SAME boot env the cold path
renders (``BindingResolver.boot_env``), so a new boot key cannot silently drift
between a cold claim and a warm template: an unclassified key refuses the
projection rather than being guessed into or out of the hash. Credentials are
identity only -- a ``secretKeyRef`` name/key or a nonsecret generation/expiry --
and no token byte may reach the hash, ``repr`` or a log line.

Nothing here touches Kubernetes, a Secret, Postgres or Valkey.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import replace

import pytest
from aci_protocol import BootEnv
from curie_worker import sandbox_token
from curie_worker.binding import (
    DECISION_ENV,
    FALSE_COMPLETION_CHECK_ENV,
    GRANT_TOOL_ENV,
    RESUMED_KIND_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
    BindingResolver,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from curie_worker.sandbox.warm_pool_contract import (
    DEFAULT_CREDENTIAL_ADMISSION_MARGIN_SECONDS,
    PROJECTION_THREAD_SENTINEL,
    STATE_APP_SCOPE,
    STATE_SCOPE,
    CapabilityProjection,
    ColdReason,
    CredentialGeneration,
    CredentialRevision,
    ProjectionError,
    SecretKeyRef,
    classify_claim,
    mint_credential_generation,
    project_boot_env,
    warm_boot_projection,
    worker_env_classes,
)
from curie_worker.workspace import WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV

PLATFORM_KEY = "unit-test-platform-key"
AGENT_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
VERSION_ID = uuid.UUID("00000000-0000-4000-8000-0000000000a1")
DEPLOYMENT_ID = uuid.UUID("00000000-0000-4000-8000-0000000000d1")
BASELINE_CREDENTIAL = SecretKeyRef(name="curie-runner-credentials", key="agentCredentials")
CONNECTOR_SECRET_NAME = "curie-agent-sre-connector-secrets"
GENERATION = CredentialGeneration.for_window(str(AGENT_ID), issued_at=1_800_000_000)

SESSION_ENV = BootEnv.env_key("session_id")
HISTORY_REF_ENV = BootEnv.env_key("history_ref")
RUNNER_TOKEN_ENV = BootEnv.env_key("runner_token")
HISTORY_TOKEN_ENV = BootEnv.env_key("history_token")
MEMORY_TOKEN_ENV = BootEnv.env_key("memory_token")
STATE_TOKEN_ENV = BootEnv.env_key("state_token")
STATE_URL_ENV = BootEnv.env_key("state_url")
CREDENTIALS_ENV = BootEnv.env_key("credentials_ref")
MODEL_ENV = BootEnv.env_key("model")
MEMORY_REF_ENV = BootEnv.env_key("memory_ref")
CONNECTOR_SECRET_KEYS_ENV = BootEnv.env_key("connector_secret_keys")

# Explicit synthetic API observations for this pure helper test. They do not
# claim a production Secret watcher, fresh read, or rotation lifecycle exists.
OBSERVED_CREDENTIAL_REVISIONS = {
    CREDENTIALS_ENV: CredentialRevision(BASELINE_CREDENTIAL, "example-model-uid", "11"),
    "GH_TOKEN": CredentialRevision(
        SecretKeyRef(CONNECTOR_SECRET_NAME, "GH_TOKEN"), "example-connector-uid", "12"
    ),
    "OTHER_TOKEN": CredentialRevision(
        SecretKeyRef(CONNECTOR_SECRET_NAME, "OTHER_TOKEN"), "example-connector-uid", "12"
    ),
    **{
        key: CredentialRevision(
            SecretKeyRef("curie-platform-credentials", "apiKey"), "example-signing-uid", "13"
        )
        for key in (HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, STATE_TOKEN_ENV)
    },
}


def _resolved(**overrides: object) -> ResolvedDeployment:
    base: dict[str, object] = {
        "agent_id": AGENT_ID,
        "agent_name": "sre",
        "deployment_id": DEPLOYMENT_ID,
        "version_id": VERSION_ID,
        "version_label": "v3",
        "bundle_ref": "bundles/sre/v3.tgz",
        "max_usd_per_day": None,
        "max_output_tokens_per_run": None,
        "model": "claude-opus-5",
        "thinking": None,
        "approval_required_tools": ["Bash"],
        "secrets": None,
        "memory": True,
    }
    base.update(overrides)
    return ResolvedDeployment.model_validate(base)


def _config(**overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "api_key": PLATFORM_KEY,
        "credentials": "sk-live-model-credential-value",
        "connector_release": "curie",
        "connector_namespace": "curie",
        "model": "claude-sonnet-5",
    }
    base.update(overrides)
    return WorkerConfig(**base)  # type: ignore[arg-type]


def _resolver(config: WorkerConfig | None = None) -> BindingResolver:
    # boot_env never touches the engine; the projection is a pure render.
    return BindingResolver(object(), config or _config())  # type: ignore[arg-type]


def _project(
    resolved: ResolvedDeployment | None = None,
    config: WorkerConfig | None = None,
    **overrides: object,
) -> CapabilityProjection:
    cfg = config or _config()
    resolved = resolved or _resolved()
    generation = (
        CredentialGeneration.for_window(str(resolved.agent_id), issued_at=GENERATION.issued_at)
        if cfg.api_key
        else None
    )
    kwargs: dict[str, object] = {
        "bundle_sha256": "ab" * 32,
        "model_credential_ref": BASELINE_CREDENTIAL,
        "connector_secret_name": CONNECTOR_SECRET_NAME,
        "credential_generation": generation,
        "credential_revisions": OBSERVED_CREDENTIAL_REVISIONS,
    }
    kwargs.update(overrides)
    return warm_boot_projection(_resolver(cfg), resolved, **kwargs)  # type: ignore[arg-type]


# --- projection derives from the real cold render -------------------------------


def test_projection_is_the_version_stable_subset_of_the_cold_boot_env() -> None:
    resolver = _resolver()
    resolved = _resolved()
    cold = resolver.boot_env(resolved, "1234.5678", kind="slack", address="C0EXAMPLE1")
    projection = _project(resolved)

    for key in (
        BootEnv.env_key("plugin_dir"),
        BootEnv.env_key("budget"),
        MEMORY_REF_ENV,
        BootEnv.env_key("bundle_ref"),
        BootEnv.env_key("bundle_version"),
        MODEL_ENV,
        BootEnv.env_key("approval_required_tools"),
        BootEnv.env_key("connector_release"),
        BootEnv.env_key("connector_agent"),
        BootEnv.env_key("connector_namespace"),
        STATE_URL_ENV,
    ):
        assert projection.env[key] == cold[key], key
    for key in (
        SESSION_ENV,
        HISTORY_REF_ENV,
        RUNNER_TOKEN_ENV,
        HISTORY_TOKEN_ENV,
        MEMORY_TOKEN_ENV,
        STATE_TOKEN_ENV,
        CREDENTIALS_ENV,
    ):
        assert key not in projection.env, key
    assert projection.credential_keys == (HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, STATE_TOKEN_ENV)
    assert projection.secret_refs[CREDENTIALS_ENV] == BASELINE_CREDENTIAL
    assert projection.credential_generation == GENERATION
    assert PROJECTION_THREAD_SENTINEL not in projection.canonical_json()
    assert projection.bundle_sha256 == "ab" * 32
    assert projection.version_id == str(VERSION_ID)
    assert projection.deployment_id == str(DEPLOYMENT_ID)


def test_generation_is_deterministic_for_identical_inputs() -> None:
    assert _project().capability_generation == _project().capability_generation
    assert len(_project().capability_generation) == 64


@pytest.mark.parametrize(
    "mutation",
    [
        {"model": "claude-sonnet-5"},
        {"thinking": "high"},
        {"max_usd_per_day": 3.5},
        {"max_output_tokens_per_run": 4096},
        {"approval_required_tools": ["Bash", "Write"]},
        {"agent_id": uuid.UUID("00000000-0000-4000-8000-000000000002")},
        {"bundle_ref": "bundles/sre/v4.tgz"},
        {"version_label": "v4"},
        {"version_id": uuid.UUID("00000000-0000-4000-8000-0000000000a2")},
    ],
    ids=lambda m: next(iter(m)),
)
def test_mutable_agent_or_version_change_mints_a_new_generation(mutation: dict) -> None:
    assert _project().capability_generation != _project(_resolved(**mutation)).capability_generation


def test_worker_default_model_change_mints_a_new_generation_when_agent_pins_none() -> None:
    resolved = _resolved(model=None)
    a = _project(resolved, _config(model="claude-sonnet-5"))
    b = _project(resolved, _config(model="claude-opus-5"))
    assert a.env[MODEL_ENV] == "claude-sonnet-5"
    assert a.capability_generation != b.capability_generation


def test_bundle_sha256_and_credential_window_are_part_of_the_generation() -> None:
    base = _project()
    assert base.capability_generation != _project(bundle_sha256="cd" * 32).capability_generation
    renewed = CredentialGeneration.for_window(str(AGENT_ID), issued_at=1_800_000_000 + 3600)
    assert (
        base.capability_generation != _project(credential_generation=renewed).capability_generation
    )


def test_memory_ref_is_included_and_omitting_it_is_refused() -> None:
    projection = _project()
    assert projection.env[MEMORY_REF_ENV].endswith(f"/agents/{AGENT_ID}/state/memory")
    env = dict(_resolver().boot_env(_resolved(), PROJECTION_THREAD_SENTINEL))
    env.pop(MEMORY_REF_ENV)
    with pytest.raises(ProjectionError, match="memory_ref"):
        project_boot_env(env, **_identity(), credential_generation=GENERATION)


# --- per-conversation and credential material never reach the hash --------------


def _identity(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "agent_id": str(AGENT_ID),
        "version_id": str(VERSION_ID),
        "deployment_id": str(DEPLOYMENT_ID),
        "version_label": "v3",
        "bundle_ref": "bundles/sre/v3.tgz",
        "bundle_sha256": "ab" * 32,
        "memory": True,
        "model_credential_ref": BASELINE_CREDENTIAL,
        "connector_secret_name": CONNECTOR_SECRET_NAME,
        "credential_revisions": OBSERVED_CREDENTIAL_REVISIONS,
    }
    base.update(overrides)
    return base


def _cold_env(**overrides: str) -> dict[str, str]:
    env = dict(_resolver().boot_env(_resolved(), "thread-a", kind="slack", address="C0EXAMPLE1"))
    env.update(overrides)
    return env


def test_per_conversation_inputs_do_not_change_the_generation() -> None:
    a = project_boot_env(_cold_env(), **_identity(), credential_generation=GENERATION)
    b = project_boot_env(
        _cold_env(
            **{
                SESSION_ENV: "agent-x-thread-other",
                HISTORY_REF_ENV: "http://api/agents/x/state/transcript/other",
                RUNNER_TOKEN_ENV: "another-per-claim-token",
                HISTORY_TOKEN_ENV: "sbx.other.sig",
                MEMORY_TOKEN_ENV: "sbx.other.sig",
                STATE_TOKEN_ENV: "sbx.other2.sig",
            }
        ),
        **_identity(),
        credential_generation=GENERATION,
    )
    assert a.capability_generation == b.capability_generation


def test_token_and_credential_values_never_reach_json_repr_str_or_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    material = {
        RUNNER_TOKEN_ENV: "RUNNER-MATERIAL-7f3a",
        HISTORY_TOKEN_ENV: "HISTORY-MATERIAL-7f3a",
        MEMORY_TOKEN_ENV: "MEMORY-MATERIAL-7f3a",
        STATE_TOKEN_ENV: "STATE-MATERIAL-7f3a",
        CREDENTIALS_ENV: "CREDENTIAL-MATERIAL-7f3a",
        "GH_TOKEN": "CONNECTOR-MATERIAL-7f3a",
        CONNECTOR_SECRET_KEYS_ENV: "GH_TOKEN",
    }
    projection = project_boot_env(
        _cold_env(**material), **_identity(), credential_generation=GENERATION
    )
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("test").info("projection %s %r", projection, projection)
    rendered = "\n".join(
        [projection.canonical_json(), repr(projection), str(projection), caplog.text]
    )
    for key, value in material.items():
        if key == CONNECTOR_SECRET_KEYS_ENV:
            continue
        assert value not in rendered, key
    assert projection.env[CONNECTOR_SECRET_KEYS_ENV] == "GH_TOKEN"
    assert "GH_TOKEN" not in projection.env
    assert projection.secret_refs["GH_TOKEN"] == SecretKeyRef(
        name=CONNECTOR_SECRET_NAME, key="GH_TOKEN"
    )


def test_a_credential_value_leaking_through_another_key_is_refused() -> None:
    env = _cold_env(**{CREDENTIALS_ENV: "LEAK-ME", "CURIE_MODEL": "LEAK-ME"})
    with pytest.raises(ProjectionError, match="credential value"):
        project_boot_env(env, **_identity(), credential_generation=GENERATION)


def test_unclassified_boot_key_refuses_the_projection() -> None:
    env = _cold_env(CURIE_FUTURE_KNOB="1")
    with pytest.raises(ProjectionError, match="CURIE_FUTURE_KNOB"):
        project_boot_env(env, **_identity(), credential_generation=GENERATION)


def test_every_worker_boot_key_is_classified() -> None:
    classes = worker_env_classes()
    expected = set(BootEnv.env_keys("worker")) | {
        GRANT_TOOL_ENV,
        RESUMED_KIND_ENV,
        DECISION_ENV,
        FALSE_COMPLETION_CHECK_ENV,
        WORKSPACE_REF_ENV,
        WORKSPACE_SHA256_ENV,
    }
    assert expected <= set(classes)
    assert BootEnv.env_key("runner_bootstrap_token") not in classes  # substrate-only, never worker


def test_model_credential_without_a_secret_ref_is_refused() -> None:
    with pytest.raises(ProjectionError, match=CREDENTIALS_ENV):
        project_boot_env(
            _cold_env(),
            **_identity(model_credential_ref=None),
            credential_generation=GENERATION,
        )


def test_fake_model_install_without_credential_needs_no_secret_ref() -> None:
    projection = _project(
        config=_config(credentials="", fake_model=True), model_credential_ref=None
    )
    assert CREDENTIALS_ENV not in projection.secret_refs
    assert projection.env[BootEnv.env_key("fake_model")] == "1"


def test_connector_secret_without_a_secret_name_is_refused() -> None:
    resolved = _resolved(secrets={"GH_TOKEN": "value"})
    with pytest.raises(ProjectionError, match="GH_TOKEN"):
        _project(resolved, connector_secret_name=None)


def test_state_tokens_and_credential_generation_must_agree() -> None:
    with pytest.raises(ProjectionError, match="credential generation"):
        _project(credential_generation=None)
    with pytest.raises(ProjectionError, match="credential generation"):
        _project(config=_config(api_key=""), credential_generation=GENERATION)
    no_key = _project(config=_config(api_key=""), credential_generation=None)
    assert no_key.credential_keys == ()
    assert no_key.credential_generation is None


def test_generation_for_another_agent_is_refused() -> None:
    other = CredentialGeneration.for_window("not-this-agent", issued_at=1_800_000_000)
    with pytest.raises(ProjectionError, match="agent"):
        _project(credential_generation=other)


# --- memory=false: binding-scoped state cannot be pre-baked ------------------------


def test_memory_false_drops_the_binding_scoped_state_url_from_the_template() -> None:
    projection = _project(_resolved(memory=False))
    assert STATE_URL_ENV not in projection.env
    assert STATE_TOKEN_ENV not in projection.credential_keys
    assert projection.memory is False


def test_memory_true_keeps_only_the_agent_wide_state_url() -> None:
    projection = _project()
    assert projection.env[STATE_URL_ENV].endswith(f"/agents/{AGENT_ID}/state")
    assert STATE_TOKEN_ENV in projection.credential_keys


# --- credential generation identity and the existing codec ------------------------


def test_credential_generation_is_nonsecret_identity_only() -> None:
    gen = CredentialGeneration.for_window(str(AGENT_ID), issued_at=100)
    assert gen == CredentialGeneration.for_window(str(AGENT_ID), issued_at=100)
    assert gen.expires_at == 100 + SANDBOX_TOKEN_TTL_SECONDS
    assert gen.scopes == (STATE_SCOPE, STATE_APP_SCOPE)
    assert gen.admits(now=gen.expires_at - 1)
    assert not gen.admits(now=gen.expires_at)
    assert not gen.admits(now=gen.expires_at - 60, margin_seconds=60)
    with pytest.raises(ValueError):
        CredentialGeneration.for_window(str(AGENT_ID), issued_at=100, ttl_seconds=0)


def test_minted_material_uses_the_existing_scoped_codec_and_is_redacted() -> None:
    gen = GENERATION
    material = mint_credential_generation(PLATFORM_KEY, gen)
    projection = _project()
    assert tuple(material.keys()) == projection.credential_keys
    agent = str(AGENT_ID)
    now = gen.issued_at + 1
    assert sandbox_token.verify(
        material.value(HISTORY_TOKEN_ENV), PLATFORM_KEY, agent=agent, scope=STATE_SCOPE, now=now
    )
    assert sandbox_token.verify(
        material.value(MEMORY_TOKEN_ENV), PLATFORM_KEY, agent=agent, scope=STATE_SCOPE, now=now
    )
    assert sandbox_token.verify(
        material.value(STATE_TOKEN_ENV), PLATFORM_KEY, agent=agent, scope=STATE_APP_SCOPE, now=now
    )
    # Causal negatives: wrong agent, wrong scope, expired, wrong key.
    assert not sandbox_token.verify(
        material.value(HISTORY_TOKEN_ENV), PLATFORM_KEY, agent="other", scope=STATE_SCOPE, now=now
    )
    assert not sandbox_token.verify(
        material.value(STATE_TOKEN_ENV), PLATFORM_KEY, agent=agent, scope=STATE_SCOPE, now=now
    )
    assert not sandbox_token.verify(
        material.value(HISTORY_TOKEN_ENV),
        PLATFORM_KEY,
        agent=agent,
        scope=STATE_SCOPE,
        now=gen.expires_at,
    )
    assert not sandbox_token.verify(
        material.value(HISTORY_TOKEN_ENV), "other-key", agent=agent, scope=STATE_SCOPE, now=now
    )
    rendered = repr(material) + str(material) + json.dumps(material.identity())
    for key in material.keys():
        assert material.value(key) not in rendered
    assert material.value(HISTORY_TOKEN_ENV) != PLATFORM_KEY
    assert material.identity()["expires_at"] == gen.expires_at


def test_minting_refuses_an_empty_key() -> None:
    with pytest.raises(ValueError):
        mint_credential_generation("", GENERATION)


# --- cold eligibility --------------------------------------------------------------


def _claim_env(resolved: ResolvedDeployment | None = None, **overrides: str) -> dict[str, str]:
    env = dict(
        _resolver().boot_env(
            resolved or _resolved(), "1234.5678", kind="slack", address="C0EXAMPLE1"
        )
    )
    env.update(overrides)
    return env


def test_fresh_matching_turn_is_warm_eligible() -> None:
    projection = _project()
    verdict = classify_claim(
        _claim_env(),
        projection,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert verdict.warm, verdict
    assert verdict.reasons == ()


@pytest.mark.parametrize("rotated", [False, True])
def test_unverified_credential_revision_never_reuses_an_existing_pool(rotated: bool) -> None:
    """Fixed Secret names cannot attest which credential a warm process loaded."""

    resolved = _resolved(secrets={"GH_TOKEN": "example-old-connector"})
    projection = _project(resolved)
    claim = _claim_env(resolved)
    if rotated:
        claim["GH_TOKEN"] = "example-new-connector"
        claim[CREDENTIALS_ENV] = "example-new-model"
    # No fresh authoritative Secret observation is available. Neither equal
    # material nor a changed value under an unchanged key can prove warm parity.
    verdict = classify_claim(claim, projection, now=GENERATION.issued_at + 1)
    assert not verdict.warm
    assert ColdReason.CREDENTIAL_AUTHORITY_UNVERIFIED in verdict.reasons


@pytest.mark.parametrize("key", [CREDENTIALS_ENV, "GH_TOKEN", HISTORY_TOKEN_ENV])
@pytest.mark.parametrize("field", ["uid", "resource_version"])
def test_changed_authoritative_revision_refuses_the_old_generation(key: str, field: str) -> None:
    # Kubernetes ObjectMeta defines UID as object identity and resourceVersion
    # as opaque: https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/
    # These are synthetic observations, not proof of a production Secret reader.
    resolved = _resolved(secrets={"GH_TOKEN": "example-connector"})
    old = _project(resolved)
    current = dict(OBSERVED_CREDENTIAL_REVISIONS)
    changed_keys = (
        (HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, STATE_TOKEN_ENV)
        if key == HISTORY_TOKEN_ENV
        else (key,)
    )
    for changed_key in changed_keys:
        current[changed_key] = replace(current[changed_key], **{field: "example-new-revision"})
    new = _project(resolved, credential_revisions=current)
    assert old.capability_generation != new.capability_generation
    refused = classify_claim(
        _claim_env(resolved),
        old,
        current_credential_revisions=current,
        now=GENERATION.issued_at + 1,
    )
    assert not refused.warm
    assert ColdReason.GENERATION_MISMATCH in refused.reasons
    assert key in refused.mismatched_keys
    # Same observed source revisions on a newly constructed generation are a
    # pure healthy control; creating/rotating actual pods remains unproved.
    assert classify_claim(
        _claim_env(resolved),
        new,
        current_credential_revisions=current,
        now=GENERATION.issued_at + 1,
    ).warm


@pytest.mark.parametrize("key", [CREDENTIALS_ENV, "GH_TOKEN", HISTORY_TOKEN_ENV])
def test_missing_current_credential_authority_is_cold(key: str) -> None:
    resolved = _resolved(secrets={"GH_TOKEN": "example-connector"})
    current = dict(OBSERVED_CREDENTIAL_REVISIONS)
    current.pop(key)
    verdict = classify_claim(
        _claim_env(resolved),
        _project(resolved),
        current_credential_revisions=current,
        now=GENERATION.issued_at + 1,
    )
    assert not verdict.warm
    assert ColdReason.CREDENTIAL_AUTHORITY_UNVERIFIED in verdict.reasons


def test_unknown_projected_authority_cannot_be_replaced_by_a_current_claim() -> None:
    projection = _project(credential_revisions=None)
    assert not projection.credential_authority_complete
    verdict = classify_claim(
        _claim_env(),
        projection,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
        now=GENERATION.issued_at + 1,
    )
    assert not verdict.warm
    assert ColdReason.CREDENTIAL_AUTHORITY_UNVERIFIED in verdict.reasons


def test_revision_of_a_different_secret_does_not_attest_the_projected_ref() -> None:
    current = dict(OBSERVED_CREDENTIAL_REVISIONS)
    current[CREDENTIALS_ENV] = replace(
        current[CREDENTIALS_ENV], source=SecretKeyRef("different-secret", "agentCredentials")
    )
    projection = _project(credential_revisions=current)
    assert not projection.credential_authority_complete
    assert not classify_claim(
        _claim_env(),
        projection,
        current_credential_revisions=current,
        now=GENERATION.issued_at + 1,
    ).warm


def test_mixed_state_signer_snapshot_cannot_form_a_warm_generation() -> None:
    current = dict(OBSERVED_CREDENTIAL_REVISIONS)
    current[HISTORY_TOKEN_ENV] = replace(current[HISTORY_TOKEN_ENV], resource_version="other")
    projection = _project(credential_revisions=current)
    assert not projection.credential_authority_complete
    assert not classify_claim(
        _claim_env(),
        projection,
        current_credential_revisions=current,
        now=GENERATION.issued_at + 1,
    ).warm


def test_generation_mismatch_is_cold_rather_than_another_pool() -> None:
    projection = _project()
    verdict = classify_claim(
        _claim_env(_resolved(model="claude-sonnet-5")),
        projection,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert not verdict.warm
    assert ColdReason.GENERATION_MISMATCH in verdict.reasons
    assert MODEL_ENV in verdict.mismatched_keys


def test_memory_false_binding_turn_is_cold() -> None:
    resolved = _resolved(memory=False)
    projection = _project(resolved)
    verdict = classify_claim(
        _claim_env(resolved),
        projection,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert not verdict.warm
    assert verdict.reasons == (ColdReason.MEMORY_FALSE_BINDING_STATE,)


@pytest.mark.parametrize(
    ("key", "reason"),
    [
        (GRANT_TOOL_ENV, ColdReason.APPROVAL_GRANT),
        (RESUMED_KIND_ENV, ColdReason.APPROVAL_RESUMED_KIND),
        (DECISION_ENV, ColdReason.APPROVAL_DECISION),
        (WORKSPACE_REF_ENV, ColdReason.WORKSPACE_ENV),
        (WORKSPACE_SHA256_ENV, ColdReason.WORKSPACE_ENV),
    ],
)
def test_per_claim_overlay_env_is_cold(key: str, reason: ColdReason) -> None:
    verdict = classify_claim(
        _claim_env(**{key: "x"}),
        _project(),
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert not verdict.warm
    assert verdict.reasons == (reason,)


def test_resume_eval_and_workspace_flags_are_cold() -> None:
    projection = _project()
    now = GENERATION.issued_at + 1
    assert classify_claim(
        _claim_env(),
        projection,
        resume=True,
        now=now,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).reasons == (ColdReason.RESUME_HISTORY,)
    assert classify_claim(
        _claim_env(),
        projection,
        eval_lane=True,
        now=now,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).reasons == (ColdReason.EVAL_LANE,)
    assert classify_claim(
        _claim_env(),
        projection,
        workspace_stage=True,
        now=now,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).reasons == (ColdReason.WORKSPACE_STAGE,)


def test_unknown_extra_claim_env_is_cold_not_guessed() -> None:
    verdict = classify_claim(
        _claim_env(CURIE_FUTURE_KNOB="1"),
        _project(),
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert not verdict.warm
    assert verdict.reasons == (ColdReason.EXTRA_CLAIM_ENV,)
    assert verdict.mismatched_keys == ("CURIE_FUTURE_KNOB",)


def test_expired_credential_generation_is_cold_until_a_new_generation() -> None:
    projection = _project()
    verdict = classify_claim(
        _claim_env(),
        projection,
        now=GENERATION.expires_at,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert not verdict.warm
    assert verdict.reasons == (ColdReason.CREDENTIAL_GENERATION_EXPIRED,)


def test_a_generation_near_expiry_is_cold_by_the_admission_margin() -> None:
    projection = _project()
    edge = GENERATION.expires_at - DEFAULT_CREDENTIAL_ADMISSION_MARGIN_SECONDS
    assert not classify_claim(
        _claim_env(),
        projection,
        now=edge,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).warm
    assert classify_claim(
        _claim_env(),
        projection,
        now=edge - 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).warm
    # A caller with a longer worst-case turn widens the margin.
    assert not classify_claim(
        _claim_env(),
        projection,
        now=edge - 1,
        margin_seconds=3600,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).warm


def test_missing_version_stable_key_on_the_claim_is_a_mismatch() -> None:
    env = _claim_env()
    env.pop(BootEnv.env_key("approval_required_tools"))
    verdict = classify_claim(
        env,
        _project(),
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert verdict.reasons == (ColdReason.GENERATION_MISMATCH,)


def test_a_secret_ref_the_template_carries_but_the_claim_lacks_is_a_mismatch() -> None:
    projection = _project()
    env = _claim_env()
    env.pop(CREDENTIALS_ENV)
    verdict = classify_claim(
        env,
        projection,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert verdict.reasons == (ColdReason.GENERATION_MISMATCH,)
    assert CREDENTIALS_ENV in verdict.mismatched_keys


def test_connector_secret_set_drift_is_a_mismatch_in_both_directions() -> None:
    with_secret = _resolved(secrets={"GH_TOKEN": "value"})
    projection = _project(with_secret)
    now = GENERATION.issued_at + 1
    # The template references GH_TOKEN; a claim rendered without it is skew.
    verdict = classify_claim(
        _claim_env(),
        projection,
        now=now,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert ColdReason.GENERATION_MISMATCH in verdict.reasons
    assert "GH_TOKEN" in verdict.mismatched_keys
    # The claim renders GH_TOKEN; a template without it is skew.
    verdict = classify_claim(
        _claim_env(with_secret),
        _project(),
        now=now,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert ColdReason.GENERATION_MISMATCH in verdict.reasons
    assert "GH_TOKEN" in verdict.mismatched_keys
    # Same secrets on both sides: warm.
    assert classify_claim(
        _claim_env(with_secret),
        projection,
        now=now,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    ).warm


def test_state_token_presence_must_match_the_generation_keys() -> None:
    projection = _project()
    env = _claim_env()
    env.pop(HISTORY_TOKEN_ENV)
    verdict = classify_claim(
        env,
        projection,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert verdict.reasons == (ColdReason.GENERATION_MISMATCH,)
    assert HISTORY_TOKEN_ENV in verdict.mismatched_keys


def test_a_no_key_claim_against_a_keyed_generation_is_cold() -> None:
    """A no-key render mints no state tokens; a keyed template is different authority."""

    keyed = _project()
    unkeyed_env = dict(
        _resolver(_config(api_key="")).boot_env(
            _resolved(), "1234.5678", kind="slack", address="C0EXAMPLE1"
        )
    )
    verdict = classify_claim(
        unkeyed_env,
        keyed,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert verdict.reasons == (ColdReason.GENERATION_MISMATCH,)
    assert set(verdict.mismatched_keys) >= {HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, STATE_TOKEN_ENV}


def test_a_keyed_claim_against_a_credential_free_projection_is_cold() -> None:
    unkeyed = _project(config=_config(api_key="", credentials=""), model_credential_ref=None)
    verdict = classify_claim(
        _claim_env(),
        unkeyed,
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert ColdReason.GENERATION_MISMATCH in verdict.reasons
    assert CREDENTIALS_ENV in verdict.mismatched_keys
    assert HISTORY_TOKEN_ENV in verdict.mismatched_keys


def test_a_claim_rendered_without_the_model_credential_is_cold() -> None:
    env = dict(
        _resolver(_config(credentials="")).boot_env(
            _resolved(), "1234.5678", kind="slack", address="C0EXAMPLE1"
        )
    )
    verdict = classify_claim(
        env,
        _project(),
        now=GENERATION.issued_at + 1,
        current_credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    assert verdict.reasons == (ColdReason.GENERATION_MISMATCH,)
    assert verdict.mismatched_keys == (CREDENTIALS_ENV,)

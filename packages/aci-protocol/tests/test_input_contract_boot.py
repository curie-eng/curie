"""Optional worker authority for generic input enforcement survives boot wiring."""

from aci_protocol import BootEnv, Budget, SessionConfig

ENFORCEMENT = "curie/input-contracts@1"
ENV_KEY = "CURIE_INPUT_CONTRACT_ENFORCEMENT"


def session() -> SessionConfig:
    return SessionConfig(
        plugin_dir="/plugins/demo",
        session_id="fixture-session",
        sandbox_id="fixture-sandbox",
        budget=Budget(max_output_tokens_per_run=4096, max_usd_per_day=5),
    )


def test_legacy_boot_does_not_claim_input_enforcement() -> None:
    boot = BootEnv(session=session())
    assert boot.input_contract_enforcement is None
    assert ENV_KEY not in boot.to_env()
    assert BootEnv.from_env(boot.to_env()).input_contract_enforcement is None


def test_input_enforcement_roundtrips_exact_identifier() -> None:
    boot = BootEnv(session=session(), input_contract_enforcement=ENFORCEMENT)
    assert boot.to_env()[ENV_KEY] == ENFORCEMENT
    assert BootEnv.from_env(boot.to_env()) == boot
    assert BootEnv.env_key("input_contract_enforcement") == ENV_KEY
    assert ENV_KEY in BootEnv.env_keys("worker")
    assert ENV_KEY not in BootEnv.env_keys("substrate")


def test_worker_render_explicitly_transports_enforcement_authority() -> None:
    env = BootEnv.render_worker(
        plugin_dir="/plugins/demo",
        session_id="fixture-session",
        budget=Budget(max_output_tokens_per_run=4096, max_usd_per_day=5),
        memory_ref="http://api.example/memory",
        history_ref="http://api.example/history",
        input_contract_enforcement=ENFORCEMENT,
    )
    assert env[ENV_KEY] == ENFORCEMENT


def test_boot_retains_unknown_identifier_for_runtime_to_reject() -> None:
    # Boot wiring transports authority; the consumer must compare the exact
    # contract instead of dropping an unfamiliar value into legacy behavior.
    boot = BootEnv(session=session(), input_contract_enforcement="curie/input-contracts@99")
    assert BootEnv.from_env(boot.to_env()).input_contract_enforcement == "curie/input-contracts@99"


def test_legacy_boot_does_not_invent_input_context() -> None:
    boot = BootEnv(session=session())
    assert boot.input_contract_context is None
    assert "CURIE_INPUT_CONTRACT_CONTEXT" not in boot.to_env()


def test_explicit_input_context_roundtrips_and_worker_render_preserves_it() -> None:
    for context in ["standalone", "tracked-implementation"]:
        boot = BootEnv(session=session(), input_contract_context=context)
        assert BootEnv.from_env(boot.to_env()).input_contract_context == context
        env = BootEnv.render_worker(
            plugin_dir="/plugins/demo",
            session_id="fixture-session",
            budget=Budget(max_output_tokens_per_run=4096, max_usd_per_day=5),
            memory_ref="http://api.example/memory",
            history_ref="http://api.example/history",
            input_contract_context=context,
        )
        assert env["CURIE_INPUT_CONTRACT_CONTEXT"] == context
    assert "CURIE_INPUT_CONTRACT_CONTEXT" in BootEnv.env_keys("worker")
    assert "CURIE_INPUT_CONTRACT_CONTEXT" not in BootEnv.env_keys("substrate")


def test_unknown_input_context_is_rejected_instead_of_legacy_fallback() -> None:
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BootEnv(session=session(), input_contract_context="unknown-context")
    env = BootEnv(session=session()).to_env()
    env["CURIE_INPUT_CONTRACT_CONTEXT"] = "unknown-context"
    with pytest.raises(ValidationError):
        BootEnv.from_env(env)

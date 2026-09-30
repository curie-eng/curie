//! Integration: the missing-release recovery command uses the provider inferred
//! by the same credential-prefix map as `curie cluster up`, and names the
//! namespace and release doctor was actually invoked with (#1358 D1). The
//! provider inference (#1813) must survive that rewrite unchanged: an absent or
//! unrecognized credential still leaves egress sealed.

use curie::doctor::{evaluate, Facts};
use curie::ops::provider_from_credential_prefix;

fn missing_release_recovery(credential: Option<&str>) -> String {
    let facts = Facts {
        model_credential: credential.map(|_| "CURIE_CREDENTIALS".into()),
        model_credential_source: credential.map(|_| "environment".into()),
        model_credential_provider: credential.and_then(provider_from_credential_prefix),
        kube_context: Some("minikube".into()),
        target: Some(("acme".into(), "acme-bot".into())),
        ..Default::default()
    };

    evaluate(&facts)
        .into_iter()
        .find(|check| check.id == "release")
        .and_then(|check| check.fix)
        .expect("a missing release must offer a recovery command")
}

#[test]
fn missing_release_recovery_infers_egress_from_credential_prefix() {
    let openrouter = missing_release_recovery(Some("sk-or-PLACEHOLDER"));
    assert!(
        openrouter.starts_with("curie cluster up --namespace acme --release acme-bot"),
        "recovery command must be runnable against the invoked target: {openrouter}"
    );
    assert!(
        openrouter.contains("--allow-egress-host openrouter"),
        "OpenRouter credential must select OpenRouter egress: {openrouter}"
    );
    assert!(
        !openrouter.contains("--allow-egress-host anthropic"),
        "OpenRouter credential must not select Anthropic egress: {openrouter}"
    );

    let anthropic = missing_release_recovery(Some("sk-ant-PLACEHOLDER"));
    assert!(
        anthropic.contains("--allow-egress-host anthropic"),
        "Anthropic credential must select Anthropic egress: {anthropic}"
    );

    for credential in [None, Some("unknown-PLACEHOLDER")] {
        assert_eq!(
            missing_release_recovery(credential),
            "curie cluster up --namespace acme --release acme-bot",
            "no unambiguous provider means egress remains sealed"
        );
    }
}

/// The placeholders the recovery command used to print were not runnable, and
/// worse, they hid that doctor had inspected a different release than the one
/// the operator was about to act on.
#[test]
fn missing_release_recovery_never_prints_a_placeholder_target() {
    for credential in [None, Some("sk-ant-PLACEHOLDER")] {
        let command = missing_release_recovery(credential);
        assert!(
            !command.contains("<ns>") && !command.contains("<name>"),
            "the real target replaces the placeholders: {command}"
        );
    }
}

#[test]
fn file_backed_model_pin_recovery_uses_curie_apply_and_the_file_key() {
    let facts = Facts {
        model_release_default: Some("claude-sonnet-5".into()),
        target: Some(("acme".into(), "acme-bot".into())),
        declared_installation: true,
        ..Default::default()
    };

    let fix = evaluate(&facts)
        .into_iter()
        .find(|check| check.id == "model-pin")
        .and_then(|check| check.fix)
        .expect("a floating file backed release model must offer a fix");

    assert!(
        fix.contains("curie apply") && fix.contains("agentSandbox.runner.model"),
        "the fix must direct the operator to apply and name the file key: {fix}"
    );
    assert!(
        !fix.contains("cluster up --set"),
        "the file backed fix must not return to the cluster up override shape: {fix}"
    );
}

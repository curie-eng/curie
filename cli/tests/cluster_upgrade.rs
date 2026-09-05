//! `curie cluster upgrade` (#2301): one resumable lifecycle with exact
//! convergence and a target-version canary before success.
//!
//! Sibling issues own other slices and are not reimplemented here:
//! - #2299 versioned configuration migrations
//! - #2300 database compatibility windows
//! - #2097 the kind released-install upgrade CI rung
//!
//! Drain reuse is the #2010 gate: one drain per attempt, resume after drain
//! does not drain accepted work again.

use curie::ops::{
    run_lifecycle, ClusterUpgradeOutput, CommonOpts, FakeUpgradeHost, UpgradeOpts, UpgradePhase,
};
use curie::ui::CliOutput;

fn opts(to: &str) -> UpgradeOpts {
    UpgradeOpts {
        common: CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        },
        to: to.into(),
        chart: None,
        yes: true,
        forward_only: false,
    }
}

fn dry_opts(to: &str) -> UpgradeOpts {
    let mut o = opts(to);
    o.common.dry_run = true;
    o
}

fn output_json(out: &ClusterUpgradeOutput) -> serde_json::Value {
    out.to_json()
}

#[tokio::test]
async fn dry_run_emits_a_redacted_plan_and_does_not_mutate() {
    let secret = "credential-LEAK-2301-secret";
    let mut host = FakeUpgradeHost::installed("0.8.6").with_secret(secret);
    let out = run_lifecycle(dry_opts("0.9.0"), &mut host)
        .await
        .expect("dry-run plan");
    match &out {
        ClusterUpgradeOutput::DryRun(plan) => {
            assert!(
                plan.lines.iter().any(|l| l.contains("plan")),
                "plan must name the inspect/plan phase: {:?}",
                plan.lines
            );
            assert!(
                plan.lines
                    .iter()
                    .all(|l| !l.contains("--reuse-values")
                        && !l.contains("--reset-then-reuse-values")),
                "operator must not choose a Helm merge flag: {:?}",
                plan.lines
            );
            let joined = plan.lines.join("\n");
            assert!(!joined.contains(secret), "plan leaked credential: {joined}");
            assert!(
                joined.contains(&curie::ops::mask_secret(secret)),
                "plan must mask the credential it carried: {joined}"
            );
        }
        other => panic!("dry-run must not mutate: {other:?}"),
    }
    assert_eq!(host.mutate_calls, 0);
    assert_eq!(host.drain_calls, 0);
}

#[tokio::test]
async fn n_to_n_plus_one_succeeds_only_with_converge_and_canary() {
    let mut host = FakeUpgradeHost::installed("0.8.6");
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("upgrade");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["phase"], "commit");
    assert_eq!(json["target_version"], "0.9.0");
    assert_eq!(json["from_version"], "0.8.6");
    assert_eq!(json["known_good_version"], "0.9.0");
    assert_eq!(json["convergence"]["exact"], true);
    assert_eq!(json["canary"]["passed"], true);
    assert_eq!(host.drain_calls, 1);
    assert!(host.mutate_calls >= 1);
    assert_eq!(host.current_version(), "0.9.0");
}

#[tokio::test]
async fn fresh_install_to_n_skips_drain_and_commits_known_good() {
    let mut host = FakeUpgradeHost::empty();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("fresh install");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["known_good_version"], "0.9.0");
    assert_eq!(
        host.drain_calls, 0,
        "a first install has nothing in flight; the #2010 hook is pre-upgrade only"
    );
    assert_eq!(host.current_version(), "0.9.0");
}

#[tokio::test]
async fn same_version_rerun_is_idempotent_and_still_proves_canary() {
    let mut host = FakeUpgradeHost::installed("0.9.0").with_known_good("0.9.0");
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("same-version rerun");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["unchanged"], true);
    assert_eq!(json["convergence"]["exact"], true);
    assert_eq!(json["canary"]["passed"], true);
    assert_eq!(
        host.mutate_calls, 0,
        "a same-version rerun must not apply a new Helm revision"
    );
}

#[tokio::test]
async fn validate_failure_does_not_begin_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").refuse_schema();
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("schema refuse");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("compatibility") || msg.contains("schema") || msg.contains("validate"),
        "error must name the validate refusal: {msg}"
    );
    assert_eq!(host.mutate_calls, 0);
    assert_eq!(host.drain_calls, 0);
    assert_eq!(host.current_version(), "0.8.6");
}

#[tokio::test]
async fn config_refuse_does_not_begin_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").refuse_config();
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("config refuse");
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("config") || msg.to_lowercase().contains("validate"),
        "error must name the config refusal: {msg}"
    );
    assert_eq!(host.mutate_calls, 0);
}

#[tokio::test]
async fn drain_is_exactly_once_across_resume() {
    let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::Drain);
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("interrupted after drain");
    assert!(format!("{err:#}").contains("interrupted"));
    assert_eq!(host.drain_calls, 1);

    host.clear_interrupt();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("resume");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["resumed"], true);
    assert_eq!(
        host.drain_calls, 1,
        "resume after drain must not drain accepted work again"
    );
}

#[tokio::test]
async fn interruption_at_every_durable_phase_can_resume() {
    for phase in UpgradePhase::ALL {
        let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(phase);
        let err = run_lifecycle(opts("0.9.0"), &mut host)
            .await
            .expect_err("interrupted");
        assert!(
            format!("{err:#}").contains("interrupted"),
            "phase {phase:?} must persist then interrupt"
        );
        host.clear_interrupt();
        let out = run_lifecycle(opts("0.9.0"), &mut host)
            .await
            .expect("resume after interrupt");
        let json = output_json(&out);
        assert_eq!(
            json["status"], "succeeded",
            "resume after {phase:?} must complete"
        );
        assert_eq!(json["canary"]["passed"], true);
        assert_eq!(json["convergence"]["exact"], true);
        assert_eq!(json["known_good_version"], "0.9.0");
    }
}

#[tokio::test]
async fn success_is_refused_when_canary_fails() {
    let mut host = FakeUpgradeHost::installed("0.8.6").canary_fails();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("failed canary is a completed failure payload");
    let json = output_json(&out);
    assert_ne!(json["status"], "succeeded");
    assert_eq!(json["status"], "failed");
    assert_eq!(json["canary"]["passed"], false);
    assert!(json["fail_forward"].is_object());
    assert_eq!(json["known_good_version"], "0.8.6");
}

#[tokio::test]
async fn success_is_refused_when_convergence_is_not_exact() {
    let mut host = FakeUpgradeHost::installed("0.8.6").converge_incomplete();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("incomplete converge is a failure payload");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["convergence"]["exact"], false);
    assert!(json.get("canary").is_none() || json["canary"].is_null());
}

#[tokio::test]
async fn manifest_mismatch_blocks_known_good_commit() {
    let mut host = FakeUpgradeHost::installed("0.8.6").manifest_mismatch();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("manifest mismatch");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["convergence"]["manifest_matches"], false);
    assert_eq!(json["known_good_version"], "0.8.6");
}

#[tokio::test]
async fn failure_before_apply_leaves_previous_version_serving() {
    let mut host = FakeUpgradeHost::installed("0.8.6").fail_at(UpgradePhase::Migrate);
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("failed migrate");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["previous_serving"], true);
    assert_eq!(json["known_good_version"], "0.8.6");
    assert_eq!(host.current_version(), "0.8.6");
    assert_eq!(host.mutate_calls, 0);
}

#[tokio::test]
async fn mixed_versions_return_one_fail_forward_path() {
    let mut host = FakeUpgradeHost::installed("0.8.6")
        .fail_at(UpgradePhase::Converge)
        .mixed_versions_on_fail();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("mixed fail");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["previous_serving"], false);
    let ff = json["fail_forward"].as_object().expect("fail_forward");
    assert!(
        ff["command"]
            .as_str()
            .unwrap_or("")
            .contains("curie cluster upgrade"),
        "one bounded fail-forward command: {ff:?}"
    );
    assert!(
        !ff["command"].as_str().unwrap_or("").contains("helm "),
        "operator must not be sent to a raw Helm command: {ff:?}"
    );
}

#[tokio::test]
async fn persisted_record_is_redacted() {
    let secret = "credential-LEAK-2301-persist";
    let mut host = FakeUpgradeHost::installed("0.8.6").with_secret(secret);
    let _ = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("upgrade");
    let dumped = host.persisted_json();
    assert!(
        !dumped.contains(secret),
        "checkpoint leaked credential: {dumped}"
    );
}

#[tokio::test]
async fn in_flight_drain_refusal_does_not_mutate() {
    let mut host = FakeUpgradeHost::installed("0.8.6").in_flight(&["runs/curie/1-0"]);
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("drain refusal");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["phase"], "drain");
    assert_eq!(json["previous_serving"], true);
    assert_eq!(host.mutate_calls, 0);
    assert_eq!(host.current_version(), "0.8.6");
}

#[tokio::test]
async fn cluster_status_reports_phase_and_known_good() {
    let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::Checkpoint);
    let _ = run_lifecycle(opts("0.9.0"), &mut host).await;
    let view = host.status_view();
    assert_eq!(view.phase.as_deref(), Some("checkpoint"));
    assert_eq!(view.status, "in_progress");
    assert_eq!(view.known_good_version.as_deref(), Some("0.8.6"));
    assert_eq!(view.target_version.as_deref(), Some("0.9.0"));
}

//! `curie cluster upgrade` (#2301): one resumable lifecycle with exact
//! convergence and a target-version canary before success.
//!
//! Sibling issues own other slices and are not reimplemented here:
//! - #2299 versioned configuration migrations
//! - #2300 database compatibility windows
//! - #2097 the kind released-install upgrade CI rung
//!
//! DrainPreflight reuse: one worker-reachability check per attempt, resume
//! after it does not repeat it. It is a preflight, not the #2010 drain gate
//! itself (issue #2830); that gate is the chart's pre-upgrade Helm hook Job,
//! observed at Converge.

use curie::ops::{
    run_lifecycle, ClusterUpgradeOutput, CommonOpts, FakeUpgradeHost, UpgradeChart, UpgradeOpts,
    UpgradePhase,
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
        chart: UpgradeChart::AvailableLocal("charts/curie".into()),
        yes: true,
        forward_only: false,
    }
}

fn dry_opts(to: &str) -> UpgradeOpts {
    let mut o = opts(to);
    o.common.dry_run = true;
    o
}

#[tokio::test]
async fn real_upgrade_refuses_pending_release_before_tool_lookup() {
    let mut pending = opts("0.9.0");
    pending.chart = UpgradeChart::PendingRelease {
        source_url: "https://example.com/curie-0.9.0.tgz".into(),
        cache_path: "/cache/curie/v0.9.0/curie-0.9.0.tgz".into(),
    };

    let error = curie::ops::upgrade(pending)
        .await
        .expect_err("a real upgrade must never accept an unmaterialized release chart");
    assert_eq!(
        format!("{error:#}"),
        "a pending release chart is only valid for a dry run; download the chart before starting a real upgrade"
    );
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

// #2861: a same-version rerun runs no drain preflight, checkpoint, migrate or
// Helm upgrade. The persisted record and the plan must say so instead of
// listing those phases as completed.
#[tokio::test]
async fn same_version_rerun_records_skipped_phases_not_completed_ones() {
    let mut host = FakeUpgradeHost::installed("0.9.0").with_known_good("0.9.0");
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("same-version rerun");
    let record: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        record["completed"],
        serde_json::json!(["plan", "validate", "converge", "canary", "commit"]),
        "only phases that executed are completed: {record}"
    );
    assert_eq!(
        record["skipped"],
        serde_json::json!(["drain_preflight", "checkpoint", "migrate", "apply"]),
        "{record}"
    );
    let json = output_json(&out);
    let plan: Vec<String> = serde_json::from_value(json["plan"].clone()).unwrap();
    assert!(
        plan.iter().any(|l| l.contains("0.9.0 is already installed")
            && l.contains("no helm upgrade runs")),
        "the plan must say the apply is skipped: {plan:?}"
    );
    assert!(
        !plan.iter().any(|l| l.starts_with("helm upgrade")
            || l.starts_with("phase drain_preflight:")
            || l.starts_with("phase checkpoint:")
            || l.starts_with("phase migrate:")),
        "a skipped apply must not be planned as a helm upgrade: {plan:?}"
    );
}

// A fresh install has nothing in flight, so DrainPreflight is skipped, not run.
// An interrupted install must still not replay it on resume.
#[tokio::test]
async fn fresh_install_records_drain_preflight_as_skipped_across_resume() {
    let mut host = FakeUpgradeHost::empty().interrupt_after(UpgradePhase::Apply);
    run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("interrupted");
    host.clear_interrupt();
    run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("resume");
    let record: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        record["skipped"],
        serde_json::json!(["drain_preflight"]),
        "{record}"
    );
    assert!(
        !record["completed"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("drain_preflight")),
        "{record}"
    );
    assert_eq!(
        host.drain_calls, 0,
        "resume must not replay a skipped preflight"
    );
}

// Schema refusal at Validate must not begin mutation. `--forward-only` is a
// clap/LiveHost flag; FakeUpgradeHost keeps a boolean `refuse_schema`.
// The live binary tests in `cluster_upgrade_live.rs` own that AC:
// `pending_contract_refuses_and_names_forward_only` and
// `forward_only_allows_pending_contract_to_reach_helm_upgrade`.
#[tokio::test]
async fn validate_failure_does_not_begin_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").refuse_schema();
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("schema refuse");
    let msg = format!("{err:#}");
    assert!(
        msg.contains(
            "database/application compatibility check refused the target schema before mutation"
        ),
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
async fn sequential_same_target_resume_does_not_redrain() {
    let mut host =
        FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::DrainPreflight);
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("interrupted after drain preflight");
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
        "resume after drain preflight must not repeat it"
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
        .expect("drain preflight refusal");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["phase"], "drain_preflight");
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

#[tokio::test]
async fn sequential_different_target_is_refused_without_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::Plan);
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("first run interrupted, leaving an in-progress checkpoint");
    assert!(format!("{err:#}").contains("interrupted"));
    host.clear_interrupt();

    let err = run_lifecycle(opts("0.9.1"), &mut host)
        .await
        .expect_err("a different target is refused while one is in progress");
    assert!(
        format!("{err:#}").contains("already in progress"),
        "{err:#}"
    );
    assert_eq!(
        host.mutate_calls, 0,
        "the refused run must not have mutated"
    );
}

#[test]
fn upgrade_phase_parse_matches_as_str_and_rejects_unknown() {
    for phase in UpgradePhase::ALL {
        assert_eq!(UpgradePhase::parse(phase.as_str()), Some(phase));
        assert_eq!(
            UpgradePhase::parse(&format!("  {}  ", phase.as_str())),
            Some(phase),
            "parse must trim {}",
            phase.as_str()
        );
    }
    assert_eq!(UpgradePhase::parse("not-a-phase"), None);
    assert_eq!(UpgradePhase::parse(""), None);
}

/// #2862: a dry run exists to be checked before the real run, so a plan that
/// ends in a Validate refusal must fail exactly like the real run does, still
/// without mutating anything.
#[tokio::test]
async fn dry_run_whose_plan_refuses_validation_is_an_error() {
    for (label, host) in [
        (
            "schema",
            FakeUpgradeHost::installed("0.8.6").refuse_schema(),
        ),
        (
            "config",
            FakeUpgradeHost::installed("0.8.6").refuse_config(),
        ),
    ] {
        let mut host = host;
        let err = run_lifecycle(dry_opts("0.9.0"), &mut host)
            .await
            .expect_err(label);
        assert!(
            format!("{err:#}").contains("before mutation"),
            "{label}: the dry-run error must carry the Validate refusal: {err:#}"
        );
        assert_eq!(host.mutate_calls, 0, "{label}");
        assert_eq!(host.drain_calls, 0, "{label}");
        assert_eq!(host.current_version(), "0.8.6", "{label}");
    }
}

/// #2863: the printed apply line carries `--install` and the retained values
/// argument exactly when Apply passes them, with a placeholder for the path.
#[tokio::test]
async fn dry_run_apply_line_carries_install_and_retained_values_only_when_passed() {
    let apply_line = |out: ClusterUpgradeOutput| match out {
        ClusterUpgradeOutput::DryRun(plan) => plan
            .lines
            .into_iter()
            .find(|l| l.starts_with("helm upgrade "))
            .expect("plan has an apply line"),
        other => panic!("dry-run must not mutate: {other:?}"),
    };

    let mut fresh = FakeUpgradeHost::empty().with_retained_values();
    let line = apply_line(run_lifecycle(dry_opts("0.9.0"), &mut fresh).await.unwrap());
    assert_eq!(
        line,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m --install -f <retained-values>"
    );

    let mut existing = FakeUpgradeHost::installed("0.8.6").with_retained_values();
    let line = apply_line(
        run_lifecycle(dry_opts("0.9.0"), &mut existing)
            .await
            .unwrap(),
    );
    assert_eq!(
        line,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m -f <retained-values>"
    );

    let mut bare = FakeUpgradeHost::installed("0.8.6");
    let line = apply_line(run_lifecycle(dry_opts("0.9.0"), &mut bare).await.unwrap());
    assert_eq!(
        line,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m"
    );
}

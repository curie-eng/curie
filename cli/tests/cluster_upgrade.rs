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
    run_lifecycle, ClusterUpgradeOutput, CommonOpts, FakeUpgradeHost, LayerClearReason,
    RunnerLayerPlan, UpgradeChart, UpgradeOpts, UpgradePhase,
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
        take_over: None,
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

/// #4332: a same-version known-good rerun skips Apply, so its dry-run plan
/// must not promise to retire any agent's SandboxClaims.
#[tokio::test]
async fn same_version_dry_run_drops_runner_layer_retire_lines() {
    let mut host = FakeUpgradeHost::installed("0.11.0")
        .with_known_good("0.11.0")
        .with_retained_values()
        .with_runner_layer_clears(&["factory", "sre-bot"]);
    let out = run_lifecycle(dry_opts("0.11.0"), &mut host)
        .await
        .expect("dry-run plan");
    let ClusterUpgradeOutput::DryRun(plan) = &out else {
        panic!("dry-run must not mutate: {out:?}");
    };
    assert!(
        plan.lines.iter().any(
            |l| l.contains("0.11.0 is already installed") && l.contains("no helm upgrade runs")
        ),
        "{:?}",
        plan.lines
    );
    assert!(
        plan.lines
            .iter()
            .all(|l| !l.starts_with("retire SandboxClaims ") && !l.starts_with("helm upgrade ")),
        "a skipped Apply retires nothing: {:?}",
        plan.lines
    );
    assert_eq!(host.mutate_calls, 0);
    assert!(host.retired_claims.is_empty());
}

/// #3218: an upgrade that changes the platform runner names every layered
/// agent before it upgrades and clears each one's runner image in the same
/// `helm upgrade`, after the retained values so the clear wins.
#[tokio::test]
async fn dry_run_names_stale_runner_layers_and_clears_them_in_the_apply() {
    let mut host = FakeUpgradeHost::installed("0.10.0")
        .with_retained_values()
        .with_runner_layer_clears(&["factory", "sre-bot"]);
    let out = run_lifecycle(dry_opts("0.11.0"), &mut host)
        .await
        .expect("dry-run plan");
    let ClusterUpgradeOutput::DryRun(plan) = &out else {
        panic!("dry-run must not mutate: {out:?}");
    };
    let apply = plan
        .lines
        .iter()
        .find(|l| l.starts_with("helm upgrade "))
        .expect("plan has an apply line");
    assert_eq!(
        apply,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m -f <retained-values>"
    );
    assert!(
        !apply.contains("=null"),
        "a null runnerImages override is the render refusal: {apply}"
    );
    let notice = plan
        .lines
        .iter()
        .find(|l| l.starts_with("runner layers:"))
        .expect("plan names the stale runner layers");
    assert!(notice.contains("factory, sre-bot"), "{notice}");
    assert!(notice.contains("WITHOUT their layer"), "{notice}");
    assert!(notice.contains("curie build --plugin-dir"), "{notice}");
    // Claim names are observed after Apply, so the plan describes selection.
    let apply_at = plan.lines.iter().position(|l| l == apply).unwrap();
    assert_eq!(
        &plan.lines[apply_at + 1..apply_at + 3],
        &[
            "retire SandboxClaims of agent factory whose runner is not the planned layer",
            "retire SandboxClaims of agent sre-bot whose runner is not the planned layer",
        ]
    );
    assert!(
        notice.contains("it retires their SandboxClaims whose runner is not the planned layer"),
        "{notice}"
    );
    let json = output_json(&out).to_string();
    assert!(
        !json.contains("=null"),
        "the plan must not pass a null digest: {json}"
    );
    assert!(json.contains("sre-bot"), "{json}");
    assert_eq!(host.mutate_calls, 0);

    let mut untouched = FakeUpgradeHost::installed("0.10.0").with_retained_values();
    let ClusterUpgradeOutput::DryRun(plan) = run_lifecycle(dry_opts("0.11.0"), &mut untouched)
        .await
        .unwrap()
    else {
        panic!("dry-run must not mutate");
    };
    assert!(plan.lines.iter().all(|l| !l.contains("runnerImages")
        && !l.starts_with("runner layers:")
        && !l.contains("SandboxClaims")));
}

/// #3849: a Helm apply error is a terminal failed checkpoint at apply.
/// The previous known-good version stays the one that was serving.
#[tokio::test]
async fn apply_error_records_a_failed_checkpoint_and_keeps_the_previous_release() {
    let mut host = FakeUpgradeHost::installed("0.11.1").apply_error(
        "agentSandbox.runnerImages.acme-bot must be a digest reference, got \"<nil>\"",
    );
    let out = run_lifecycle(opts("0.11.2"), &mut host)
        .await
        .expect("apply failure is a completed failure payload");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "apply", "{json}");
    assert_eq!(json["previous_serving"], true, "{json}");
    assert_eq!(json["known_good_version"], "0.11.1", "{json}");
    assert_eq!(host.current_version(), "0.11.1");
    assert_eq!(host.mutate_calls, 1, "apply is attempted once");
    let view = host.status_view();
    assert_eq!(view.status, "failed");
    assert_eq!(view.phase.as_deref(), Some("apply"));
    let record: serde_json::Value = serde_json::from_str(&host.persisted_json()).unwrap();
    assert_eq!(record["status"], "failed");
    assert_eq!(record["failed_phase"], "apply");
    assert!(record["completed"]
        .as_array()
        .unwrap()
        .iter()
        .any(|phase| phase == "migrate"));
    assert!(!record["completed"]
        .as_array()
        .unwrap()
        .iter()
        .any(|phase| phase == "apply"));
    assert!(
        json["fail_forward"]["reason"]
            .as_str()
            .unwrap_or("")
            .contains("<nil>"),
        "{json}"
    );
}

/// #3422: a real upgrade that clears layered agents' runner images retires
/// their SandboxClaims after Apply, so live threads leave the old layer the
/// way `cluster deploy` makes them (#3300). An upgrade clearing nothing
/// retires nothing.
#[tokio::test]
async fn upgrade_retires_claims_of_every_cleared_runner_layer_after_apply() {
    let mut host =
        FakeUpgradeHost::installed("0.10.0").with_runner_layer_clears(&["factory", "sre-bot"]);
    let out = run_lifecycle(opts("0.11.0"), &mut host)
        .await
        .expect("upgrade");
    assert_eq!(output_json(&out)["status"], "succeeded", "{out:?}");
    assert_eq!(host.retired_claims, vec!["factory", "sre-bot"]);

    let mut plain = FakeUpgradeHost::installed("0.10.0");
    run_lifecycle(opts("0.11.0"), &mut plain)
        .await
        .expect("upgrade");
    assert!(plain.retired_claims.is_empty());
}

const STOCK_LAYER: &str = "ghcr.io/curie-eng/curie-dark-factory-runner@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const PLATFORM_RUNNER: &str = "ghcr.io/curie-eng/curie-runner@sha256:2222222222222222222222222222222222222222222222222222222222222222";

/// One stock agent rebound to the layer published for `--to` (#4321).
fn stock_rebind_plan() -> RunnerLayerPlan {
    RunnerLayerPlan {
        rebinds: vec![("dark-factory".into(), STOCK_LAYER.into())],
        ..RunnerLayerPlan::default()
    }
}

/// #4321 AC3: the plan lists rebound agents apart from cleared ones, the
/// owner-built line keeps its v0.12.3 text for owner-built agents only, and
/// every changed agent's claims are retired right after Apply.
#[tokio::test]
async fn dry_run_shows_rebinds_and_stock_clears_apart() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_retained_values()
        .with_runner_layer_plan(RunnerLayerPlan {
            rebinds: vec![("dark-factory".into(), STOCK_LAYER.into())],
            clears: vec![
                ("acme-bot".into(), LayerClearReason::OwnerBuilt),
                (
                    "night-factory".into(),
                    LayerClearReason::StockLayerUnpublished,
                ),
            ],
            kept: Vec::new(),
        });
    let out = run_lifecycle(dry_opts("0.12.3"), &mut host)
        .await
        .expect("dry-run plan");
    let ClusterUpgradeOutput::DryRun(plan) = &out else {
        panic!("dry-run must not mutate: {out:?}");
    };
    let at = |prefix: &str| {
        plan.lines
            .iter()
            .position(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("no line starting {prefix:?}: {:?}", plan.lines))
    };
    let rebound_at = at("runner layers rebound:");
    let owner_at = at("runner layers:");
    let stock_at = at("stock runner layers:");
    assert!(
        rebound_at < owner_at && owner_at < stock_at,
        "rebind line, owner notice, stock notice: {:?}",
        plan.lines
    );
    let rebound = &plan.lines[rebound_at];
    assert!(
        rebound.contains(&format!("dark-factory={STOCK_LAYER}")),
        "{rebound}"
    );
    assert!(
        !rebound.contains("acme-bot") && !rebound.contains("night-factory"),
        "{rebound}"
    );
    let owner = &plan.lines[owner_at];
    assert!(owner.contains("agent(s) acme-bot will stop"), "{owner}");
    assert!(!owner.contains("factory"), "{owner}");
    let stock = &plan.lines[stock_at];
    assert!(stock.contains("night-factory"), "{stock}");
    assert!(stock.contains("curie cluster factory"), "{stock}");
    assert!(!stock.contains("curie build --plugin-dir"), "{stock}");
    assert!(!stock.contains("acme-bot"), "{stock}");

    let apply_at = at("helm upgrade ");
    assert_eq!(
        &plan.lines[apply_at + 1..apply_at + 4],
        &[
            "retire SandboxClaims of agent acme-bot whose runner is not the planned layer",
            "retire SandboxClaims of agent dark-factory whose runner is not the planned layer",
            "retire SandboxClaims of agent night-factory whose runner is not the planned layer",
        ]
    );
    assert_eq!(host.mutate_calls, 0);
    assert!(host.retired_claims.is_empty());

    let json = output_json(&out);
    let lines: Vec<&str> = json["plan"]
        .as_array()
        .expect("--json plan")
        .iter()
        .filter_map(|l| l.as_str())
        .collect();
    for prefix in [
        "runner layers rebound:",
        "runner layers:",
        "stock runner layers:",
    ] {
        assert!(
            lines.iter().any(|l| l.starts_with(prefix)),
            "--json plan lacks {prefix:?}: {json}"
        );
    }
}

/// #4321 AC6: rendered templates that match the plan let the run commit
/// known-good, and a rebound agent's claims are retired after Apply (AC2).
#[tokio::test]
async fn canary_commits_when_templates_match_the_plan() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_plan(stock_rebind_plan())
        .with_template_images(PLATFORM_RUNNER, &[("dark-factory", STOCK_LAYER)]);
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("upgrade");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded", "{json}");
    assert_eq!(json["phase"], "commit", "{json}");
    assert_eq!(json["canary"]["passed"], true, "{json}");
    assert_eq!(json["known_good_version"], "0.12.3", "{json}");
    assert_eq!(host.retired_claims, vec!["dark-factory"]);
}

/// #4321 AC6: a template that renders something other than the plan fails
/// the canary, the run does not commit known-good, and the reason names the
/// agent, what the upgrade planned and what the template renders.
#[tokio::test]
async fn canary_fails_and_keeps_known_good_when_a_template_is_off_plan() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_plan(stock_rebind_plan())
        .with_template_images(PLATFORM_RUNNER, &[("dark-factory", PLATFORM_RUNNER)]);
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("failed canary is a completed failure payload");
    let json = output_json(&out);
    assert_eq!(json["canary"]["passed"], false, "{json}");
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "canary", "{json}");
    assert_eq!(json["known_good_version"], "0.12.2", "{json}");
    let reason = json["fail_forward"]["reason"].as_str().unwrap_or("");
    assert!(reason.contains("dark-factory"), "{json}");
    assert!(reason.contains(STOCK_LAYER), "the planned image: {json}");
    assert!(
        reason.contains(PLATFORM_RUNNER),
        "the rendered image: {json}"
    );
    let view = host.status_view();
    assert_eq!(view.status, "failed");
    assert_eq!(view.known_good_version.as_deref(), Some("0.12.2"));
}

const OWNER_LAYER: &str = "ghcr.io/acme/acme-bot-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

/// #4321 review P1: the canary expectations are recorded in the checkpoint
/// when the record is created, and a resumed run checks the recorded ones.
/// After Apply cleared acme-bot, re-planning from the modified values no
/// longer sees acme-bot at all (an empty host plan here), so only the
/// recorded expectation can catch its stale per-agent template.
#[tokio::test]
async fn resumed_canary_uses_the_recorded_expectations_not_the_replanned_ones() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_clears(&["acme-bot"])
        .with_template_images(PLATFORM_RUNNER, &[("acme-bot", OWNER_LAYER)])
        .interrupt_after(UpgradePhase::Apply);
    let err = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect_err("interrupted after apply");
    assert!(format!("{err:#}").contains("interrupted"), "{err:#}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", "PlatformRunner"]]),
        "the fresh record carries the plan's canary expectations: {persisted}"
    );

    // The resumed process re-plans from values Apply already changed.
    let mut host = host.with_runner_layer_plan(RunnerLayerPlan::default());
    host.clear_interrupt();
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("failed canary is a completed failure payload");
    let json = output_json(&out);
    assert_eq!(json["resumed"], true, "{json}");
    assert_eq!(json["canary"]["passed"], false, "{json}");
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "canary", "{json}");
    assert_eq!(json["known_good_version"], "0.12.2", "{json}");
    let reason = json["fail_forward"]["reason"].as_str().unwrap_or("");
    assert!(reason.contains("acme-bot"), "{json}");
    assert!(reason.contains(OWNER_LAYER), "the stale layer: {json}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", "PlatformRunner"]]),
        "a resumed run keeps the recorded expectations: {persisted}"
    );
}

/// #4321 review round 2 P2: when a run stops before Apply, the plan Apply
/// will actually execute is the one the resumed process computes. Here the
/// first attempt could only clear acme-bot (say the registry was down), and
/// the resumed one rebinds it to a published layer. The resumed run refreshes
/// and persists the canary expectations from that plan, so the correctly
/// rebound template passes. The post-Apply resume test above is the inverse
/// control: once Apply has run, the recorded expectations win.
#[tokio::test]
async fn resume_before_apply_refreshes_the_canary_expectations_from_the_executed_plan() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_clears(&["acme-bot"])
        .interrupt_after(UpgradePhase::Checkpoint);
    let err = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect_err("interrupted before apply");
    assert!(format!("{err:#}").contains("interrupted"), "{err:#}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", "PlatformRunner"]]),
        "the first attempt records its own plan: {persisted}"
    );
    assert!(
        !persisted["completed"]
            .as_array()
            .expect("completed phases")
            .iter()
            .any(|phase| phase == "apply"),
        "Apply is still outstanding: {persisted}"
    );

    let mut host = host
        .with_runner_layer_plan(RunnerLayerPlan {
            rebinds: vec![("acme-bot".into(), STOCK_LAYER.into())],
            ..RunnerLayerPlan::default()
        })
        .with_template_images(PLATFORM_RUNNER, &[("acme-bot", STOCK_LAYER)]);
    host.clear_interrupt();
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("upgrade");
    let json = output_json(&out);
    assert_eq!(json["resumed"], true, "{json}");
    assert_eq!(json["status"], "succeeded", "{json}");
    assert_eq!(json["phase"], "commit", "{json}");
    assert_eq!(json["canary"]["passed"], true, "{json}");
    assert_eq!(json["known_good_version"], "0.12.3", "{json}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", {"Image": STOCK_LAYER}]]),
        "the resumed run persists the expectations of the plan Apply executed: {persisted}"
    );
}

use super::{
    absent_container_note, check_deploy_routes_bound, declared_approval_routes,
    github_repo_allowlist_is_empty, merge_secret_env, model_credential_summary,
    parse_credential_env_file, parse_manifest_gates, plan_recorded_state, plan_recorded_teardown,
    plan_skill_down, promote_candidate_window, recorded_ids_match, replace_first_line,
    report_sweep, resolve_cases_path, resolve_env_file_credentials, route_write_refusal,
    routing_warning, seed_env_if_missing, select_in_force_deployment, select_passthrough_env,
    sweep_json_row, sweep_table_row, unbound_approval_routes, validate_channel_binding,
    validate_notification_target, ApprovalGateDecl, DeclaringVersion, DeployTier, DownPlan,
    EnvSeed, RecordedStatePlan, RecordedStateQuery, RecordedTeardown, SweepRow,
};
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};

// @spec ADR-0168 d3
#[test]
fn a_slack_notification_names_an_identity_and_no_transport() {
    let named = crate::api::NotificationTargetWrite {
        kind: "slack".into(),
        address: "C0EXAMPLE2".into(),
        endpoint: None,
        adapter: Some("ops-bot".into()),
    };
    validate_notification_target("finance", &named).expect("an identity alone is complete");
    let transport = crate::api::NotificationTargetWrite {
        endpoint: Some("https://adapter.example.com/replies".into()),
        ..named
    };
    let err = validate_notification_target("finance", &transport).unwrap_err();
    assert!(err.to_string().contains("no endpoint"), "{err}");
}

#[test]
fn github_repo_allowlist_is_empty_for_missing_null_and_empty_values() {
    assert!(github_repo_allowlist_is_empty(&json!({})));
    assert!(github_repo_allowlist_is_empty(&json!({"api": {}})));
    assert!(github_repo_allowlist_is_empty(
        &json!({"api": {"githubRepoAllowlist": serde_json::Value::Null}})
    ));
    assert!(github_repo_allowlist_is_empty(
        &json!({"api": {"githubRepoAllowlist": []}})
    ));
    assert!(github_repo_allowlist_is_empty(
        &json!({"api": {"githubRepoAllowlist": ["", "  "]}})
    ));
    assert!(!github_repo_allowlist_is_empty(
        &json!({"api": {"githubRepoAllowlist": ["acme-corp/acme-bot"]}})
    ));
    assert!(!github_repo_allowlist_is_empty(
        &json!({"api": {"githubRepoAllowlist": ["acme-labs/*"]}})
    ));
}

// --- the eval exit-code contract (#2007) --------------------------------
//
// The exit-0/exit-1 halves of the contract are proven end-to-end in
// `cli/tests/eval_case_selector.rs`, against the real binary's process exit
// code and the requests the runner actually received. A unit test here
// could only re-derive the verdict rule from a row vector, which stays
// green under a mutation that runs the UNFILTERED suite.

#[tokio::test]
async fn a_mistyped_case_id_fails_a_skill_eval_model_sweep_rather_than_greening_it() {
    // #2007: the skill-tier `--model` sweep boots a transient LOCAL runner
    // per model and grades in-CLI via `run_suite_cases`, so a `--case-id`
    // selection reaches it (unlike the local/cluster sweep, which is the
    // platform plane and only ever sees a suite NAME). `select_cases` runs
    // before `eval_sweep` boots anything, and an explicit --cases path
    // skips cwd-dependent resolution, so this reaches the exit-2 gate with
    // no Docker daemon and no `.curie/runner.json` needed.
    let dir = tempfile::tempdir().expect("tempdir");
    let cases = dir.path().join("cases.json");
    std::fs::write(
        &cases,
        r#"{"name":"smoke","cases":[{"id":"greets-the-user","input":"hi","grader":{"kind":"contains","expected":"hi"}}]}"#,
    )
    .expect("write suite");
    let err = super::eval(
        Some(cases),
        vec!["greets-the-usr".to_string()],
        None,
        vec!["opus".to_string()],
        Vec::new(),
        "curie-runner:test".to_string(),
        crate::eval_sampling::SampleConfig::default(),
    )
    .await
    .expect_err("a mistyped --case-id must fail the sweep, not silently sweep everything");
    assert_eq!(
        crate::exit::classify(&err).0,
        crate::exit::ExitClass::Usage,
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("greets-the-usr"), "{err:#}");
}

/// The platform's answer for a repository two agents bind with no declared
/// targets -- built by deserializing the WIRE shape, so the test cannot
/// drift from what the endpoint actually sends (#1221).
fn unroutable_check() -> crate::api::RoutingCheck {
    serde_json::from_str(
        r#"{
            "repo_full_name": "octo/shared-repo",
            "agent_count": 2,
            "agents": ["acme-bot", "acme-dev"],
            "resolvable": false,
            "unresolvable": [
                {
                    "environment": "dev",
                    "code": "deploy.no_targets",
                    "message": "2 agents are built from this repository but the bundle has no deploy.yaml, so there is nothing to say which one this branch deploys to. Declare a target (ADR-0089)."
                }
            ]
        }"#,
    )
    .expect("the routing-check wire shape should decode")
}

#[test]
fn the_routing_warning_names_the_repository_and_every_bound_agent() {
    // The operator just deployed ONE of these agents. Naming only that one
    // would hide the actual damage: the sibling that was working stops
    // deploying without anyone touching it.
    let warning = routing_warning(&unroutable_check());
    assert!(warning.contains("octo/shared-repo"), "was {warning}");
    assert!(warning.contains("acme-bot"), "was {warning}");
    assert!(warning.contains("acme-dev"), "was {warning}");
}

#[test]
fn the_routing_warning_carries_the_resolvers_own_words_verbatim() {
    // Paraphrasing here would put a second statement of the routing rule in
    // the client, free to drift from the one a push enforces (#1212).
    let check = unroutable_check();
    let warning = routing_warning(&check);
    assert!(
        warning.contains(&check.unresolvable[0].message),
        "was {warning}"
    );
    assert!(warning.contains("deploy.no_targets"), "was {warning}");
    assert!(warning.contains("dev"), "was {warning}");
}

#[test]
fn the_routing_warning_states_the_blast_radius_it_was_told_about() {
    let warning = routing_warning(&unroutable_check());
    // The damage is real and must read as such, but scoped to the
    // environment the resolver actually named.
    assert!(warning.contains("dev"), "was {warning}");
    assert!(
        warning.contains("no longer deploy anything"),
        "was {warning}"
    );
    assert!(
        warning.contains("did not touch"),
        "the warning must say untouched agents are affected too: {warning}"
    );
    // The remedy is the resolver's, carried in its own message. A second,
    // hardcoded one is what made this warning misleading for a bundle that
    // already declares targets.
    assert!(
        !warning.contains("Fix: declare a target"),
        "the client must not append its own remedy: {warning}"
    );
}

/// One environment broken and one fine -- a bundle with a good `dev` target
/// and a `prod` target naming an agent that does not exist. Dev pushes
/// still deploy, so the warning must not say otherwise (#1221).
fn prod_only_unroutable_check() -> crate::api::RoutingCheck {
    serde_json::from_str(
        r#"{
            "repo_full_name": "octo/shared-repo",
            "agent_count": 2,
            "agents": ["acme-bot", "acme-dev"],
            "resolvable": false,
            "unresolvable": [
                {
                    "environment": "prod",
                    "code": "deploy.unknown_agent",
                    "message": "The prod target names agent 'acme-prod', which does not exist."
                }
            ]
        }"#,
    )
    .expect("the routing-check wire shape should decode")
}

#[test]
fn the_routing_warning_does_not_widen_one_broken_environment_to_all() {
    let check = prod_only_unroutable_check();
    let warning = routing_warning(&check);
    assert!(warning.contains("prod"), "was {warning}");
    assert!(
        warning.contains(&check.unresolvable[0].message),
        "was {warning}"
    );
    // Nothing may suggest the dev lane broke: it did not, and telling the
    // operator it did sends them to fix working configuration.
    assert!(
        !warning.contains("dev environment"),
        "dev still routes and must not be named as broken: {warning}"
    );
    assert!(
        !warning.contains("every push"),
        "only the reported environments are affected: {warning}"
    );
}

#[test]
fn env_file_resolver_respects_shell_and_vault_precedence() {
    let parsed = vec![
        (
            "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
            "oat-file".to_string(),
        ),
        ("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string()),
    ];
    // Nothing higher present -> the file fills both SDK names.
    let none_present = |_: &str| false;
    assert_eq!(
        resolve_env_file_credentials(&parsed, &none_present),
        vec![
            (
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "oat-file".to_string()
            ),
            ("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string()),
        ]
    );
    // A higher source (shell env or vault) already has the OAuth token, so
    // the file fills only the still-missing API key: shell > vault > file.
    let oauth_present = |name: &str| name == "CLAUDE_CODE_OAUTH_TOKEN";
    assert_eq!(
        resolve_env_file_credentials(&parsed, &oauth_present),
        vec![("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string())]
    );
}

#[test]
fn env_file_curie_credential_dominates_the_sdk_pair() {
    let parsed = vec![
        ("CURIE_CREDENTIALS".to_string(), "byo-file".to_string()),
        ("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string()),
    ];
    // The file's CURIE_CREDENTIALS wins and is returned alone.
    let none = |_: &str| false;
    assert_eq!(
        resolve_env_file_credentials(&parsed, &none),
        vec![("CURIE_CREDENTIALS".to_string(), "byo-file".to_string())]
    );
    // A BYO credential from a higher source dominates -> nothing from the
    // file (the SDK pair never rides alongside CURIE_CREDENTIALS).
    let byo_present = |name: &str| name == "CURIE_CREDENTIALS";
    assert!(resolve_env_file_credentials(&parsed, &byo_present).is_empty());
}

#[test]
fn parse_credential_env_file_reads_only_recognized_nonempty_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".env");
    std::fs::write(
        &path,
        "ANTHROPIC_API_KEY=sk-real\n\
         CURIE_CREDENTIALS=\n\
         UNRELATED=leaked\n\
         CLAUDE_CODE_OAUTH_TOKEN=oat-real\n",
    )
    .unwrap();
    let parsed = parse_credential_env_file(&path).unwrap();
    // Recognized + non-empty only: the empty CURIE_CREDENTIALS and the
    // UNRELATED key are dropped, never absorbed (#749/#540).
    assert_eq!(
        parsed,
        vec![
            ("ANTHROPIC_API_KEY".to_string(), "sk-real".to_string()),
            (
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "oat-real".to_string()
            ),
        ]
    );
    assert!(!parsed.iter().any(|(key, _)| key == "UNRELATED"));
}

#[test]
fn parse_credential_env_file_errors_on_a_missing_file() {
    let err = parse_credential_env_file(Path::new("/no/such/curie/env/file")).unwrap_err();
    assert!(err.to_string().contains("--env-file"), "{err}");
}

fn row(model: &str, passed: usize, completed: usize, total: usize) -> SweepRow {
    SweepRow {
        model: model.into(),
        passed,
        completed,
        total,
        plumbing: 0,
    }
}

#[test]
fn never_completed_requires_a_nonempty_row_with_zero_completions() {
    // The distinct #622 outcome: cases ran, none completed.
    assert!(row("bogus", 0, 0, 5).never_completed());
    // A real 0% (every case completed and lost on the grader) is NOT this
    // outcome -- the negative control the acceptance criteria calls out.
    assert!(!row("opus", 0, 5, 5).never_completed());
    // A model that completed and passed some cases is obviously not this
    // outcome either.
    assert!(!row("opus", 3, 5, 5).never_completed());
    // An empty row (no cases at all) must not be misread as never-completed;
    // there is nothing to have failed to complete.
    assert!(!row("opus", 0, 0, 0).never_completed());
}

#[test]
fn a_real_zero_percent_model_still_exits_ok_and_reports_zero_percent() {
    // Negative control (acceptance criterion 4): a model that legitimately
    // scores 0% -- every case completed, the grader just disagreed -- must
    // still report 0% and exit 0. A sweep stays a comparison, not a gate.
    let rows = vec![row("opus", 0, 5, 5), row("sonnet", 2, 5, 5)];
    assert!(report_sweep(&rows, None).is_ok());
}

#[test]
fn a_model_with_zero_completed_turns_fails_the_sweep_loudly() {
    // The bug this issue fixes: an unresolvable model must not read as an
    // indistinguishable 0%. `report_sweep` returns `Err` (never
    // `std::process::exit` itself, so a caller's port-forward guard still
    // drops via normal unwind) and the message names the model and a likely
    // cause instead of the eval consumer.
    let rows = vec![row("bogus-model-xyz", 0, 0, 5), row("opus", 3, 5, 5)];
    let err = report_sweep(&rows, None).expect_err("a never-completed row must fail the sweep");
    let msg = err.to_string();
    assert!(msg.contains("bogus-model-xyz"), "{msg}");
    assert!(!msg.contains("eval consumer"), "{msg}");
    assert!(
        msg.contains("never resolved") || msg.contains("zero completed turns"),
        "{msg}"
    );
    let (class, _fix) = crate::exit::classify(&err);
    assert_eq!(class, crate::exit::ExitClass::Failure);
}

#[test]
fn every_model_never_completed_still_names_every_one() {
    let rows = vec![row("model-alpha", 0, 0, 3), row("model-beta", 0, 0, 3)];
    let err = report_sweep(&rows, None).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("model-alpha"), "{msg}");
    assert!(msg.contains("model-beta"), "{msg}");
}

fn recorded_query<'a>(
    recorded: Option<&'a str>,
    target: &'a str,
    replace: bool,
) -> RecordedStateQuery<'a> {
    RecordedStateQuery {
        recorded_name: recorded,
        target_name: target,
        replace,
        recorded_id: None,
        live_id: None,
        same_bundle_dir: true,
        recorded_digest: None,
        current_digest: None,
    }
}

#[test]
fn replace_clears_a_stale_record_for_the_very_container_it_replaces() {
    // A bundle holding both a stale runner.json and a live container of that
    // name was unrecoverable with --replace: the record refused the boot
    // before the preflight could remove anything (#747).
    assert_eq!(
        plan_recorded_state(recorded_query(
            Some("curie-runner-local"),
            "curie-runner-local",
            true
        )),
        RecordedStatePlan::ClearAndProceed
    );
    assert_eq!(
        plan_recorded_state(recorded_query(None, "curie-runner-local", false)),
        RecordedStatePlan::Proceed
    );
}

#[test]
fn replace_does_not_clear_a_record_naming_a_different_runner() {
    // Removing one container is no reason to forget another: a record for a
    // different, still-live runner keeps refusing, with or without --replace.
    assert_eq!(
        plan_recorded_state(recorded_query(
            Some("curie-runner-local"),
            "curie-example-42",
            true
        )),
        RecordedStatePlan::Refuse
    );
    assert_eq!(
        plan_recorded_state(recorded_query(
            Some("curie-runner-local"),
            "curie-runner-local",
            false
        )),
        RecordedStatePlan::Refuse
    );
}

fn verified_query<'a>(
    replace: bool,
    recorded_digest: Option<&'a str>,
    current_digest: Option<&'a str>,
) -> RecordedStateQuery<'a> {
    RecordedStateQuery {
        recorded_name: Some("curie-runner-local"),
        target_name: "curie-runner-local",
        replace,
        recorded_id: Some("deadbeef00001111"),
        live_id: Some("deadbeef0000"),
        same_bundle_dir: true,
        recorded_digest,
        current_digest,
    }
}

#[test]
fn plain_up_replaces_a_verified_same_bundle_when_the_snapshot_differs() {
    assert_eq!(
        plan_recorded_state(verified_query(false, Some("aaa"), Some("bbb"))),
        RecordedStatePlan::ClearAndProceed
    );
}

#[test]
fn plain_up_reports_already_running_when_the_verified_snapshot_matches() {
    assert_eq!(
        plan_recorded_state(verified_query(false, Some("aaa"), Some("aaa"))),
        RecordedStatePlan::AlreadyRunning
    );
}

#[test]
fn replace_still_restarts_a_verified_unchanged_bundle() {
    assert_eq!(
        plan_recorded_state(verified_query(true, Some("aaa"), Some("aaa"))),
        RecordedStatePlan::ClearAndProceed
    );
}

#[test]
fn plain_up_refuses_when_the_snapshot_cannot_be_compared() {
    assert_eq!(
        plan_recorded_state(verified_query(false, None, Some("aaa"))),
        RecordedStatePlan::Refuse
    );
    assert_eq!(
        plan_recorded_state(verified_query(false, Some("aaa"), None)),
        RecordedStatePlan::Refuse
    );
}

#[test]
fn plain_up_refuses_a_verified_name_whose_container_id_does_not_match() {
    let mut q = verified_query(false, Some("aaa"), Some("bbb"));
    q.live_id = Some("cccccccccccc");
    assert_eq!(plan_recorded_state(q), RecordedStatePlan::Refuse);
}

#[test]
fn plain_up_refuses_when_the_record_is_not_this_bundle_directory() {
    let mut q = verified_query(false, Some("aaa"), Some("bbb"));
    q.same_bundle_dir = false;
    assert_eq!(plan_recorded_state(q), RecordedStatePlan::Refuse);
}

#[test]
fn recorded_ids_match_across_short_and_long_docker_ids() {
    assert!(recorded_ids_match(
        "9f2c1d3e4b5a6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f",
        "9f2c1d3e4b5a"
    ));
    assert!(recorded_ids_match("9f2c1d3e4b5a", "9f2c1d3e4b5a6c7d"));
    assert!(!recorded_ids_match("", "9f2c1d3e4b5a"));
    assert!(!recorded_ids_match("aaaa", "bbbb"));
}

#[test]
fn an_absent_container_note_says_the_stale_state_was_cleared() {
    // Only the recorded path clears `.curie/runner.json`, and this sentence
    // is the user's only signal that it did, so the two notes are NOT
    // interchangeable (#747).
    assert_eq!(
        absent_container_note("curie-runner-local", true),
        "container 'curie-runner-local' was already gone; cleared stale state"
    );
    // The --name paths clear nothing, so they must not claim to.
    assert_eq!(
        absent_container_note("curie-example-42", false),
        "container 'curie-example-42' was already gone"
    );
}

#[test]
fn recorded_teardown_removes_the_container_it_actually_recorded() {
    // `docker ps` reports a short id and `docker run` a full one, so the same
    // container must still be recognized across that truncation.
    // And the removal targets the PROBED id, not the recorded one and never
    // the name: a name can change hands between the check and the removal.
    assert_eq!(
        plan_recorded_teardown(
            "9f2c1d3e4b5a6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f",
            "curie-runner-local",
            Some("9f2c1d3e4b5a")
        ),
        RecordedTeardown::Remove {
            id: "9f2c1d3e4b5a".into()
        }
    );
    // Nothing holds the name: no removal to claim, the record still clears.
    assert_eq!(
        plan_recorded_teardown("9f2c1d3e4b5a", "curie-runner-local", None),
        RecordedTeardown::AlreadyGone
    );
}

#[test]
fn recorded_teardown_refuses_a_container_that_merely_reuses_the_name() {
    // Bundle B booted a NEW container under the same name (its own
    // `skill up --replace`). A plain `skill down` in bundle A must not
    // destroy it just because the name still matches (#747).
    let plan = plan_recorded_teardown(
        "aaaa1111bbbb2222",
        "curie-runner-local",
        Some("cccc3333dddd"),
    );
    let RecordedTeardown::Hijacked { message } = plan else {
        panic!("a different container holding the recorded name must not be removed");
    };
    assert!(message.contains("curie-runner-local"), "{message}");
    assert!(message.contains("cccc3333dddd"), "{message}");
    assert!(message.contains("nothing was removed"), "{message}");
    assert!(
        message.contains("curie skill down --name curie-runner-local"),
        "{message}"
    );
}

#[test]
fn skill_down_removes_the_recorded_runner() {
    assert_eq!(
        plan_skill_down(Some("curie-runner-local"), None, false),
        DownPlan::Recorded {
            container: "curie-runner-local".into()
        }
    );
}

#[test]
fn skill_down_targets_a_name_that_is_not_the_recorded_runner() {
    // Only `Recorded` clears `.curie/runner.json`. An explicit --name that
    // disagrees with the record is a targeted removal, so the still-running
    // recorded runner keeps its state file, ollama container, and network
    // instead of being silently orphaned (#747).
    assert_eq!(
        plan_skill_down(Some("curie-runner-local"), Some("curie-example-42"), true),
        DownPlan::Targeted {
            container: "curie-example-42".into()
        }
    );
}

#[test]
fn skill_down_does_not_claim_a_removal_of_an_absent_targeted_container() {
    // `docker rm -f <missing>` exits 0, so the removal itself cannot tell a
    // real teardown from a no-op. Absence has to come from the probe, or the
    // verb reports "stopped and removed" for a container that was never
    // there (#747). Still not an error -- just not a removal.
    let plan = plan_skill_down(Some("curie-runner-local"), Some("curie-747-absent"), false);
    assert_eq!(
        plan,
        DownPlan::TargetedAbsent {
            container: "curie-747-absent".into()
        }
    );
    // The only variants that report a removal are the ones that do one.
    assert!(!matches!(
        plan,
        DownPlan::Targeted { .. } | DownPlan::Recorded { .. } | DownPlan::Orphan { .. }
    ));
}

#[test]
fn skill_down_with_the_recorded_name_is_the_full_recorded_teardown() {
    // Naming the recorded container explicitly is the state-clearing
    // teardown, not a targeted removal that would strand the record.
    assert_eq!(
        plan_skill_down(
            Some("curie-runner-local"),
            Some("curie-runner-local"),
            false
        ),
        DownPlan::Recorded {
            container: "curie-runner-local".into()
        }
    );
}

#[test]
fn skill_down_falls_back_to_container_identity_without_state() {
    // The reported wedge (#747): an orphaned container and no runner.json.
    // `skill down` must be able to clear it.
    assert_eq!(
        plan_skill_down(None, None, true),
        DownPlan::Orphan {
            container: "curie-runner-local".into()
        }
    );
    assert_eq!(
        plan_skill_down(None, Some("curie-example-42"), true),
        DownPlan::Orphan {
            container: "curie-example-42".into()
        }
    );
}

#[test]
fn skill_down_with_nothing_to_remove_names_the_container_and_the_remedy() {
    let DownPlan::Nothing { message } = plan_skill_down(None, None, false) else {
        panic!("no state and no container is nothing to remove");
    };
    assert!(message.contains("curie-runner-local"), "{message}");
    assert!(message.contains(".curie/runner.json"), "{message}");
    assert!(message.contains("--name"), "{message}");

    let DownPlan::Nothing { message } = plan_skill_down(None, Some("curie-eval-sweep-0"), false)
    else {
        panic!("no state and no container is nothing to remove");
    };
    assert!(message.contains("curie-eval-sweep-0"), "{message}");
}

#[test]
fn replace_first_line_rewrites_only_the_first_anchored_line() {
    // The [package] version, not a dependency `version = ` line below it.
    let cargo = "[package]\nname = \"curie\"\nversion = \"0.4.0\"\n\n[dependencies]\nserde = { version = \"1\" }\n";
    let out = replace_first_line(cargo, "version = ", "version = \"0.5.0\"").unwrap();
    assert!(out.contains("version = \"0.5.0\""));
    // The dependency's inline version is untouched.
    assert!(out.contains("serde = { version = \"1\" }"));
    assert_eq!(out.matches("0.5.0").count(), 1);
    assert!(out.ends_with('\n'));
}

#[test]
fn replace_first_line_preserves_indentation_and_reports_absence() {
    let chart = "apiVersion: v2\nname: curie\nappVersion: \"0.4.0\"\n";
    let out = replace_first_line(chart, "appVersion:", "appVersion: \"0.5.0\"").unwrap();
    assert!(out.contains("appVersion: \"0.5.0\""));
    assert!(replace_first_line(chart, "nonexistent:", "x").is_none());
}

const RELEASE_CATALOG: &str = r#"{
    "revisions": ["0045", "0058", "0059"],
    "candidate": {"schema_min": "0045", "schema_head": "0059"},
    "windows": {
        "0.10.0": {"schema_min": "0045", "schema_head": "0058"},
        "0.10.1": {"schema_min": "0045", "schema_head": "0058"}
    }
}"#;

#[test]
fn bump_version_promotes_candidate_and_preserves_prior_release_windows() {
    let promoted = promote_candidate_window(RELEASE_CATALOG, "0.10.2", false)
        .expect("new release can claim the candidate window");
    let catalog: serde_json::Value = serde_json::from_str(&promoted).unwrap();
    assert_eq!(catalog["windows"]["0.10.2"], catalog["candidate"]);
    assert_eq!(catalog["windows"]["0.10.2"]["schema_head"], "0059");
    for version in ["0.10.0", "0.10.1"] {
        assert_eq!(catalog["windows"][version]["schema_min"], "0045");
        assert_eq!(catalog["windows"][version]["schema_head"], "0058");
    }
}

#[test]
fn bump_version_repeating_the_same_promotion_is_idempotent() {
    let once = promote_candidate_window(RELEASE_CATALOG, "0.10.2", false).unwrap();
    let twice = promote_candidate_window(&once, "0.10.2", true).unwrap();
    let once: serde_json::Value = serde_json::from_str(&once).unwrap();
    let twice: serde_json::Value = serde_json::from_str(&twice).unwrap();
    assert_eq!(once, twice);
}

#[test]
fn bump_version_refuses_to_rewrite_a_registered_window() {
    let error = promote_candidate_window(RELEASE_CATALOG, "0.10.1", true)
        .expect_err("registered 0.10.1 must keep its published schema head");
    assert!(format!("{error:#}").contains("0.10.1"));
    assert!(format!("{error:#}").contains("next version"));
}

#[test]
fn bump_version_refuses_to_create_a_missing_registered_window() {
    let error = promote_candidate_window(RELEASE_CATALOG, "0.10.2", true)
        .expect_err("registered 0.10.2 must already have a catalog window");
    assert!(format!("{error:#}").contains("0.10.2"));
    assert!(format!("{error:#}").contains("no catalog window"));
}

#[test]
fn bump_version_can_repromote_an_unregistered_window() {
    let promoted = promote_candidate_window(RELEASE_CATALOG, "0.10.1", false)
        .expect("unregistered version can be prepared again");
    let catalog: serde_json::Value = serde_json::from_str(&promoted).unwrap();
    assert_eq!(catalog["windows"]["0.10.1"], catalog["candidate"]);
    assert_eq!(catalog["windows"]["0.10.1"]["schema_head"], "0059");
    assert_eq!(catalog["windows"]["0.10.0"]["schema_head"], "0058");
}

#[tokio::test]
async fn bump_version_refuses_leading_zero_components() {
    for version in ["00.10.2", "0.010.2", "0.10.02", "0.10.2-rc.01"] {
        let error = super::bump_version(version, true)
            .await
            .expect_err("noncanonical version must be refused before reading the checkout");
        assert_eq!(
            crate::exit::classify(&error).0,
            crate::exit::ExitClass::Usage,
            "{version}: {error:#}"
        );
    }
}

/// Scaffold a bundle at `dir` under `name`, then overwrite its manifest's
/// `secrets` policy with `secrets`. Shared setup for tests exercising the
/// declared-secrets gate in `deploy()`.
fn scaffold_with_secrets(dir: &Path, name: &str, secrets: &[&str]) {
    crate::scaffold::scaffold(dir, name).unwrap();
    let manifest_path = dir.join(".claude-plugin/plugin.json");
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest["secrets"] = serde_json::json!(secrets);
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

#[test]
fn default_channel_passes_local_validation() {
    assert!(validate_channel_binding("slack", crate::api::DEFAULT_SLACK_CHANNEL).is_ok());
}

#[test]
fn parse_check_report_accepts_declared_authed_flag() {
    // The frozen check-report contract gains `authed` on each declared server
    // so the CLI/UI can flag credential-gated servers the offline check never
    // exercised. It must round-trip through parse_check_report.
    let json = r#"{
        "check": "mcp-load",
        "version": 1,
        "plugin_dir": "/x",
        "declared": [
            {"name": "github", "source": ".mcp.json", "form": "bare_file", "authed": true}
        ],
        "registered": [],
        "matches": [],
        "verdict": "green",
        "reasons": [],
        "hints": []
    }"#;
    let report = super::parse_check_report(json).expect("authed report must parse");
    assert!(
        report.declared[0].authed,
        "declared[].authed must round-trip true"
    );
}

#[test]
fn parse_check_report_defaults_authed_false_when_absent() {
    // Backward compat: a report from an older runner has no `authed` key. It
    // must still parse and default to false (#[serde(default)]), never fail
    // the contract on the missing field.
    let json = r#"{
        "check": "mcp-load",
        "version": 1,
        "plugin_dir": "/x",
        "declared": [
            {"name": "plain", "source": "plugin.json", "form": "inline"}
        ],
        "registered": [],
        "matches": [],
        "verdict": "green",
        "reasons": [],
        "hints": []
    }"#;
    let report = super::parse_check_report(json).expect("report without authed must parse");
    assert!(
        !report.declared[0].authed,
        "absent authed must default to false"
    );
}

#[test]
fn install_preserves_existing_local_config() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join(".env"), "USER_SETTING=keep-me\n").unwrap();
    std::fs::write(
        root.path().join(".env.example"),
        "USER_SETTING=new-default\n",
    )
    .unwrap();

    assert_eq!(
        seed_env_if_missing(root.path()).unwrap(),
        EnvSeed::Preserved
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join(".env")).unwrap(),
        "USER_SETTING=keep-me\n"
    );
}

#[test]
fn explicit_cases_path_wins() {
    let snapshot = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(snapshot.path().join("evals")).unwrap();
    std::fs::write(snapshot.path().join("evals/cases.json"), "[]").unwrap();
    let path = resolve_cases_path(
        Some(PathBuf::from("/x/cases.json")),
        std::path::Path::new("/nowhere"),
        Some(snapshot.path()),
        None,
    )
    .unwrap();
    assert_eq!(path, PathBuf::from("/x/cases.json"));
}

#[test]
fn missing_recorded_snapshot_cases_do_not_fall_back_to_cwd() {
    let cwd = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(cwd.path().join("evals")).unwrap();
    std::fs::write(cwd.path().join("evals/cases.json"), "[]").unwrap();
    let snapshot = tempfile::tempdir().unwrap();

    let err = resolve_cases_path(None, cwd.path(), Some(snapshot.path()), None).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("no eval cases found in the running snapshot"),
        "{message}"
    );
    assert!(message.contains("--cases"), "{message}");
}

#[test]
fn falls_back_from_cwd_to_the_recorded_bundle_dir() {
    let cwd = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(bundle.path().join("evals")).unwrap();
    std::fs::write(bundle.path().join("evals/cases.json"), "[]").unwrap();

    // cwd has no cases: resolve into the bundle dir from the state file.
    let resolved = resolve_cases_path(None, cwd.path(), None, Some(bundle.path())).unwrap();
    assert_eq!(resolved, bundle.path().join("evals/cases.json"));

    // cwd cases take precedence once present.
    std::fs::create_dir_all(cwd.path().join("evals")).unwrap();
    std::fs::write(cwd.path().join("evals/cases.json"), "[]").unwrap();
    let resolved = resolve_cases_path(None, cwd.path(), None, Some(bundle.path())).unwrap();
    assert_eq!(resolved, cwd.path().join("evals/cases.json"));
}

#[test]
fn errors_when_nothing_is_found() {
    let cwd = tempfile::tempdir().unwrap();
    let err = resolve_cases_path(None, cwd.path(), None, None).unwrap_err();
    assert!(err.to_string().contains("--cases"), "{err}");
}

#[test]
fn rejects_hash_prefixed_channel_name() {
    let err = validate_channel_binding("slack", "#testing")
        .unwrap_err()
        .to_string();
    assert!(err.contains("channel ID"), "{err}");
}

#[test]
fn accepts_channel_id() {
    assert!(validate_channel_binding("slack", "C0EXAMPLE4").is_ok());
}

#[test]
fn rejects_leading_whitespace_hash() {
    assert!(validate_channel_binding("slack", "  #testing").is_err());
}

#[test]
fn a_kind_with_no_local_rule_passes_locally() {
    // No local shape rule for a non-slack kind; the API is the authoritative
    // gate for it.
    assert!(validate_channel_binding("webhook", "#anything").is_ok());
}

/// A fully-credentialed host, for the cases below that are not about which
/// ambient names happen to be exported.
fn all_ambient_present(_name: &str) -> bool {
    true
}

#[test]
fn fake_model_forwards_nothing_even_with_byo() {
    // A fake model run needs no credential: forward none, even when an
    // explicit BYO reference is present, so a real token never leaks into
    // the untrusted runner.
    assert_eq!(
        select_passthrough_env(true, false, Some("sk-or-x"), &all_ambient_present),
        Vec::<String>::new()
    );
}

#[test]
fn explicit_byo_credential_forwarded_alone() {
    // A non-empty BYO credential is forwarded alone -- the ambient SDK vars
    // must not shadow the operator's chosen credential.
    assert_eq!(
        select_passthrough_env(false, false, Some("sk-or-x"), &all_ambient_present),
        vec!["CURIE_CREDENTIALS".to_string()]
    );
}

#[test]
fn oauth_shaped_byo_dropped_under_base_url_override() {
    // An sk-ant-oat OAuth token authenticates nothing behind a base-URL
    // override, so it is dropped rather than left inert in /proc/1/environ
    // (issue #603). The ambient fallback is also suppressed under the override.
    assert_eq!(
        select_passthrough_env(false, true, Some("sk-ant-oat-x"), &all_ambient_present),
        Vec::<String>::new()
    );
}

#[test]
fn provider_byo_kept_under_base_url_override() {
    // A non-OAuth provider key (sk-or- OpenRouter) is routed into
    // ANTHROPIC_API_KEY even behind a preset base URL, so it is still
    // forwarded -- dropping it would break BYO OpenRouter (issue #603).
    assert_eq!(
        select_passthrough_env(false, true, Some("sk-or-x"), &all_ambient_present),
        vec!["CURIE_CREDENTIALS".to_string()]
    );
}

#[test]
fn oauth_shaped_byo_kept_without_override() {
    // The OAuth drop is gated on the override: on the legacy real-Anthropic
    // path an sk-ant-oat token is a valid credential and is forwarded alone.
    assert_eq!(
        select_passthrough_env(false, false, Some("sk-ant-oat-x"), &all_ambient_present),
        vec!["CURIE_CREDENTIALS".to_string()]
    );
}

#[test]
fn empty_byo_credential_falls_back_to_sdk_vars() {
    // An empty CURIE_CREDENTIALS (a blank line in .env) is treated as unset,
    // so the ambient SDK vars carry the legacy real-Anthropic credential.
    assert_eq!(
        select_passthrough_env(false, false, Some(""), &all_ambient_present),
        vec![
            "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
            "ANTHROPIC_API_KEY".to_string()
        ]
    );
}

#[test]
fn no_byo_credential_falls_back_to_sdk_vars() {
    assert_eq!(
        select_passthrough_env(false, false, None, &all_ambient_present),
        vec![
            "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
            "ANTHROPIC_API_KEY".to_string()
        ]
    );
}

/// The state that used to boot silently and fail one command later.
#[test]
fn no_resolved_credential_warns_and_says_none() {
    let (row, warning) = model_credential_summary(false, None, &[]);
    assert_eq!(row, "none");
    let warning = warning.expect("a runner that cannot reach a model must say so at boot");
    assert!(
        warning.contains("model-credential-rejected"),
        "the warning must name the error the next command will actually print, \
         so the two are recognizably the same problem: {warning}"
    );
    for way_out in ["CURIE_CREDENTIALS", "--fake-model"] {
        assert!(
            warning.contains(way_out),
            "the warning must offer {way_out} as a way forward: {warning}"
        );
    }
}

/// The warning has to stay rare to stay meaningful: every configured path
/// reports itself and stays quiet.
#[test]
fn each_configured_model_path_reports_itself_without_warning() {
    let cases = [
        (true, None, None, "fake (offline, scripted replies)"),
        (false, Some("llama3"), None, "local ollama (llama3)"),
        (false, None, Some("CURIE_CREDENTIALS"), "CURIE_CREDENTIALS"),
        (false, None, Some("ANTHROPIC_API_KEY"), "ANTHROPIC_API_KEY"),
    ];
    for (fake, local, name, expected) in cases {
        let names: Vec<String> = name.into_iter().map(String::from).collect();
        let (row, warning) = model_credential_summary(fake, local, &names);
        assert_eq!(row, expected, "row for fake={fake} local={local:?}");
        assert!(
            warning.is_none(),
            "a configured model path must not warn (fake={fake} local={local:?})"
        );
    }
}

/// `--local-model` beats `--fake-model` at the call site, and the panel has
/// to describe the runner that actually booted.
#[test]
fn local_model_wins_over_fake_in_the_summary() {
    let (row, _) = model_credential_summary(true, Some("llama3"), &[]);
    assert_eq!(row, "local ollama (llama3)");
}

/// Names, never values -- the row is printed.
#[test]
fn summary_reports_names_only() {
    let (row, _) = model_credential_summary(false, None, &["CURIE_CREDENTIALS".to_string()]);
    assert!(
        !row.contains("sk-"),
        "the panel must never carry a credential value: {row}"
    );
}

/// One row of the committed cross-language forwarding matrix. The five
/// inputs are booleans: the rule keys on presence, never on a credential's
/// content.
///
/// `deny_unknown_fields` makes an unrecognized key a hard parse failure
/// rather than a silently ignored input: a row that grows a sixth input
/// this lane cannot see would otherwise pass vacuously, which is the exact
/// drift the gate exists to catch. A new input must be taught to this
/// struct, to the Python lane's expected key set
/// (apps/worker/tests/sandbox/test_vector_credential_forwarding.py), and to
/// the vector file itself.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForwardingVector {
    name: String,
    /// Documentation carried by the vector file; parsed so the row's own
    /// rationale is not an unknown field, and read back into the assertion
    /// message so a failing vector explains itself.
    why: String,
    fake_model: bool,
    base_url_override: bool,
    byo_credential: bool,
    byo_oauth_shaped: bool,
    ambient_oauth: bool,
    ambient_api_key: bool,
    expected: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForwardingVectors {
    /// The file-level rationale; parsed so it is not an unknown field.
    /// Underscore-prefixed so rustc's dead_code lint skips it; the serde
    /// rename keeps the JSON key it matches on as `comment`.
    #[serde(rename = "comment")]
    _comment: String,
    vectors: Vec<ForwardingVector>,
}

#[test]
fn cli_matches_every_forwarding_vector() {
    // The Rust half of the cross-language gate (#495). The Python worker lane
    // (apps/worker/tests/sandbox/test_vector_credential_forwarding.py) reads
    // this same file, so a rule changed in one language without the other
    // fails that language's test. The rule is not restated here.
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/vectors/model-credential-forwarding.json"
    ))
    .expect("read tests/vectors/model-credential-forwarding.json");
    let parsed: ForwardingVectors = serde_json::from_str(&raw).unwrap_or_else(|err| {
        panic!(
            "parse tests/vectors/model-credential-forwarding.json: {err}\n\
             An unknown field is rejected on purpose: a new input this lane cannot see \
             would pass vacuously. Teach the new key to ForwardingVector here, to \
             _EXPECTED_VECTOR_KEYS in \
             apps/worker/tests/sandbox/test_vector_credential_forwarding.py, and to both \
             implementations of the rule."
        )
    });
    // Guards against a rename or a truncated file making this loop vacuously pass.
    assert!(!parsed.vectors.is_empty(), "no vectors parsed");

    for vector in &parsed.vectors {
        let ambient_present = |name: &str| match name {
            "CLAUDE_CODE_OAUTH_TOKEN" => vector.ambient_oauth,
            "ANTHROPIC_API_KEY" => vector.ambient_api_key,
            _ => false,
        };
        // An OAuth-shaped BYO is sk-ant-oat; a provider key is sk-or-. Both are
        // placeholders and forwarded by NAME, so neither value enters the argv.
        let byo = vector.byo_credential.then_some(if vector.byo_oauth_shaped {
            "sk-ant-oat-PLACEHOLDER-byo"
        } else {
            "sk-or-PLACEHOLDER-byo"
        });
        assert_eq!(
            select_passthrough_env(
                vector.fake_model,
                vector.base_url_override,
                byo,
                &ambient_present
            ),
            vector.expected,
            "{}: {}",
            vector.name,
            vector.why
        );
    }
}

#[test]
fn secret_env_appends_after_the_model_credential() {
    // --secret names ride alongside the model credential, in order, so an
    // authed MCP server gets its token next to the model token.
    assert_eq!(
        merge_secret_env(
            select_passthrough_env(false, false, None, &all_ambient_present),
            &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
        ),
        vec![
            "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
            "ANTHROPIC_API_KEY".to_string(),
            "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
        ]
    );
}

#[test]
fn secret_env_forwarded_even_when_model_credential_suppressed() {
    // A fake/local model suppresses the model credential but a bundle's MCP
    // secret must still reach the sandbox.
    assert_eq!(
        merge_secret_env(
            select_passthrough_env(true, false, None, &all_ambient_present),
            &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
        ),
        vec!["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
    );
}

#[test]
fn secret_env_deduplicates_against_the_credential_vars() {
    // Passing a model-credential var as --secret must not duplicate it.
    assert_eq!(
        merge_secret_env(
            select_passthrough_env(false, false, None, &all_ambient_present),
            &["ANTHROPIC_API_KEY".to_string()]
        ),
        vec![
            "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
            "ANTHROPIC_API_KEY".to_string(),
        ]
    );
}

#[tokio::test]
async fn deploy_names_the_remediation_when_api_is_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    crate::scaffold::scaffold(dir.path(), "test-agent").unwrap();
    // Exercise local state guidance with its default URL while port 1 below
    // remains the deterministic connection refusal target.
    let hint = crate::local::deploy_unreachable_hint(
        crate::message::DEFAULT_LOCAL_API_URL,
        Some("curie-api-1   curie-api:dev   curie-api   Up 8 seconds (health: starting)"),
    );
    let opts = super::DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        identity: None,
        plugin_dir: dir.path().to_path_buf(),
        // port 1 is reserved/closed -> deterministic connection refused
        api_url: "http://127.0.0.1:1".to_string(),
        api_key: "k".to_string(),
        slack_channel: None,
        repo: None,
        workspace: super::WorkspaceIntent::Preserve,
        tier: super::DeployTier::Local,
        env: Some(super::DeployEnv::Dev),
        label: Some("v0".to_string()),
        secret: vec![],
        secret_binding_supported: true,
        connect_hint: hint.clone(),
    };
    let err = super::deploy(opts).await.unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("local stack is still starting"),
        "the connection error must retain the state aware recovery: {rendered}"
    );
    assert!(
        rendered.contains("curie local status"),
        "the connection error must direct the operator to inspect local state: {rendered}"
    );
    assert!(
        !rendered.contains("curie local up"),
        "a starting stack must not be told to start again: {rendered}"
    );
}

#[test]
fn unbound_declared_secrets_diffs_declared_against_bound() {
    // All declared names bound -> nothing unbound.
    assert!(
        super::unbound_declared_secrets(&["GH_TOKEN".to_string()], &["GH_TOKEN".to_string()])
            .is_empty()
    );
    // A declared name not in the bound set is returned.
    assert_eq!(
        super::unbound_declared_secrets(
            &["GH_TOKEN".to_string(), "SLACK".to_string()],
            &["GH_TOKEN".to_string()]
        ),
        vec!["SLACK".to_string()]
    );
    // Nothing declared -> nothing unbound (even with bound extras).
    assert!(super::unbound_declared_secrets(&[], &["GH_TOKEN".to_string()]).is_empty());
    // The #464 mismatch: declared the connector name, bound a different one.
    assert_eq!(
        super::unbound_declared_secrets(
            &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()],
            &["GH_TOKEN".to_string()]
        ),
        vec!["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
    );
    // A MALFORMED declared name (not env-var syntax) is excluded from the
    // gap: it is the plugin-format validator's job to reject it server-side,
    // so the gate must not preempt that with a misleading `--secret` message.
    assert!(super::unbound_declared_secrets(&["github-token".to_string()], &[]).is_empty());
    // A well-formed unbound name alongside a malformed one: only the
    // well-formed one is a gap.
    assert_eq!(
        super::unbound_declared_secrets(
            &["github-token".to_string(), "GITHUB_TOKEN".to_string()],
            &[]
        ),
        vec!["GITHUB_TOKEN".to_string()]
    );
}

#[test]
fn gate_accepts_a_declared_name_that_connectors_yaml_auto_binds() {
    // #2503 x #464: the gate diffs the declared names against the
    // EFFECTIVE bind set, not the `--secret` flags alone. A name Curie
    // itself resolves from connectors.yaml is bound, so a deploy that
    // needs no flag must not be refused.
    let declared = vec!["GH".to_string()];
    let effective = super::merge_secret_env(vec![], &["GH".to_string()]);
    assert!(
        super::unbound_declared_secrets(&declared, &effective).is_empty(),
        "an auto-bound connector secret is not a gap"
    );
}

#[test]
fn gate_still_reports_a_name_bound_by_neither_source() {
    // The gate must not go vacuous: a manifest secret that neither a
    // `--secret` flag nor connectors.yaml binds still fails the deploy.
    let declared = vec!["GH".to_string(), "SLACK".to_string()];
    let effective = super::merge_secret_env(vec![], &["GH".to_string()]);
    assert_eq!(
        super::unbound_declared_secrets(&declared, &effective),
        vec!["SLACK".to_string()]
    );
}

#[test]
fn secret_env_binds_owned_connector_name_with_no_explicit_flag() {
    // #2503: a hosted connector's declared GITHUB_PERSONAL_ACCESS_TOKEN
    // must reach the sandbox env with zero `--secret` flags, so the
    // derived `Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}` header (ADR-0009 /
    // #1488 delivery path) expands instead of sending a literal `${...}`.
    assert_eq!(
        merge_secret_env(vec![], &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]),
        vec!["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
    );
}

#[tokio::test]
async fn deploy_fails_when_declared_secret_is_not_bound() {
    // AC3: a declared secret NAME with no matching --secret binding fails the
    // deploy BEFORE any network attempt -- a true deploy-time error, not a
    // runtime/connection failure.
    let dir = tempfile::tempdir().unwrap();
    // Declares a NAME we will bind under the wrong key.
    scaffold_with_secrets(dir.path(), "test-agent", &["GITHUB_PERSONAL_ACCESS_TOKEN"]);

    let opts = super::DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        identity: None,
        plugin_dir: dir.path().to_path_buf(),
        api_url: "http://127.0.0.1:1".to_string(),
        api_key: "k".to_string(),
        slack_channel: None,
        repo: None,
        workspace: super::WorkspaceIntent::Preserve,
        env: Some(super::DeployEnv::Dev),
        label: Some("v0".to_string()),
        secret: vec!["GH_TOKEN".to_string()],
        secret_binding_supported: true,
        connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        tier: super::DeployTier::Local,
    };
    let err = super::deploy(opts).await.unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("GITHUB_PERSONAL_ACCESS_TOKEN"),
        "error must name the missing secret: {rendered}"
    );
    assert!(
        !rendered.contains("UNREACHABLE-HINT-SENTINEL"),
        "gate must fire before any network attempt (no connect hint): {rendered}"
    );
}

#[tokio::test]
async fn deploy_local_tier_auto_binds_and_resolves_a_connectors_yaml_bearer_secret() {
    // #2518: local now auto-binds the connectors.yaml Bearer name, like
    // cluster (#2503). The #464 gate must pass with no `--secret`, and the
    // name must then be resolved for delivery on the agent record, so an
    // unset value is refused by name before any network attempt.
    let dir = tempfile::tempdir().unwrap();
    scaffold_with_secrets(dir.path(), "test-agent", &["GH_2518_UNSET_BEARER"]);
    write_manifest(
        dir.path(),
        "connectors.yaml",
        "connectors:\n  gh:\n    image: ghcr.io/example/gh:1\n    secrets:\n      - GH_2518_UNSET_BEARER\n",
    );

    let opts = super::DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        identity: None,
        plugin_dir: dir.path().to_path_buf(),
        api_url: "http://127.0.0.1:1".to_string(),
        api_key: "k".to_string(),
        slack_channel: None,
        repo: None,
        workspace: super::WorkspaceIntent::Preserve,
        env: Some(super::DeployEnv::Dev),
        label: Some("v0".to_string()),
        secret: vec![],
        secret_binding_supported: true,
        connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        tier: super::DeployTier::Local,
    };
    let err = super::deploy(opts).await.unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        !rendered.contains("declares connector secret(s) that were not bound on deploy"),
        "local's effective bind set now includes the connectors.yaml name: {rendered}"
    );
    assert!(
        rendered.contains("connector secret GH_2518_UNSET_BEARER"),
        "the auto-bound name must be resolved for record delivery: {rendered}"
    );
    assert!(
        !rendered.contains("UNREACHABLE-HINT-SENTINEL"),
        "resolution must fail before any network attempt: {rendered}"
    );
}

#[tokio::test]
async fn deploy_local_tier_refuses_a_reserved_auto_bound_bearer_before_reading_it() {
    // #2518 review: an `unhosted_url` connector is skipped by
    // `refuse_reserved_secret_names`, yet its Bearer name is auto-bound, so
    // local would otherwise read the operator's model credential onto the
    // agent record.
    let dir = tempfile::tempdir().unwrap();
    scaffold_with_secrets(dir.path(), "test-agent", &[]);
    write_manifest(
        dir.path(),
        "connectors.yaml",
        "connectors:\n  gh:\n    image: ghcr.io/example/gh:1\n    secrets:\n      - ANTHROPIC_API_KEY\n    unhosted_url: http://127.0.0.1:1/mcp\n",
    );
    let opts = super::DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        identity: None,
        plugin_dir: dir.path().to_path_buf(),
        api_url: "http://127.0.0.1:1".to_string(),
        api_key: "k".to_string(),
        slack_channel: None,
        repo: None,
        workspace: super::WorkspaceIntent::Preserve,
        env: Some(super::DeployEnv::Dev),
        label: Some("v0".to_string()),
        secret: vec![],
        secret_binding_supported: true,
        connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        tier: super::DeployTier::Local,
    };
    let rendered = format!("{:#}", super::deploy(opts).await.unwrap_err());
    assert!(
        rendered.contains("`ANTHROPIC_API_KEY` is a reserved"),
        "a reserved Bearer name must be refused before resolution: {rendered}"
    );
}

#[tokio::test]
async fn deploy_cluster_tier_passes_the_gate_on_a_connectors_yaml_auto_bound_secret() {
    // The paired case: the SAME bundle (GH declared in both plugin.json and
    // connectors.yaml, no `--secret` flag) reaches the network on Cluster,
    // because the effective bind set there is the union with
    // `hosted_env_secret_names`, not `opts.secret` alone.
    let dir = tempfile::tempdir().unwrap();
    scaffold_with_secrets(dir.path(), "test-agent", &["GH"]);
    write_manifest(
        dir.path(),
        "connectors.yaml",
        "connectors:\n  gh:\n    image: ghcr.io/example/gh:1\n    secrets:\n      - GH\n",
    );

    let opts = super::DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        identity: None,
        plugin_dir: dir.path().to_path_buf(),
        api_url: "http://127.0.0.1:1".to_string(),
        api_key: "k".to_string(),
        slack_channel: None,
        repo: None,
        workspace: super::WorkspaceIntent::Preserve,
        env: Some(super::DeployEnv::Dev),
        label: Some("v0".to_string()),
        secret: vec![],
        secret_binding_supported: true,
        connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        tier: super::DeployTier::Cluster,
    };
    let err = super::deploy(opts).await.unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        !rendered.contains("declares connector secret(s) that were not bound on deploy"),
        "cluster's effective bind set auto-includes GH via connectors.yaml, \
         so the gate must pass and the error must be the network path: {rendered}"
    );
}

#[tokio::test]
async fn deploy_skips_secrets_gate_when_binding_unsupported() {
    // AC2: a tier that cannot bind `--secret` skips the declared-secrets
    // gate so a secrets-declaring bundle is not preempted with a
    // remediation that does not exist. Cluster now binds (#1488); this
    // pin is the skip itself.
    let dir = tempfile::tempdir().unwrap();
    scaffold_with_secrets(dir.path(), "test-agent", &["GITHUB_PERSONAL_ACCESS_TOKEN"]);

    let opts = super::DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        identity: None,
        plugin_dir: dir.path().to_path_buf(),
        // port 1 is reserved/closed -> deterministic connection refused
        api_url: "http://127.0.0.1:1".to_string(),
        api_key: "k".to_string(),
        slack_channel: None,
        repo: None,
        workspace: super::WorkspaceIntent::Preserve,
        env: Some(super::DeployEnv::Dev),
        label: Some("v0".to_string()),
        secret: vec![],
        secret_binding_supported: false,
        connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        tier: super::DeployTier::Cluster,
    };
    let err = super::deploy(opts).await.unwrap_err();
    let rendered = format!("{err:#}");
    // The error is the network/connect path, not the secrets gate.
    assert!(
        rendered.contains("UNREACHABLE-HINT-SENTINEL"),
        "gate should be skipped, so deploy reaches the network: {rendered}"
    );
    assert!(
        !rendered.contains("GITHUB_PERSONAL_ACCESS_TOKEN"),
        "the skipped gate must not name the declared secret: {rendered}"
    );
}

// --- skill approvals (tier parity, issue #459) --------------------------

/// Write a plugin manifest at `rel` under `dir`, creating parent dirs.
fn write_manifest(dir: &std::path::Path, rel: &str, body: &str) {
    let path = dir.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

/// A minimal valid manifest, declaring no `approvalPolicy`.
///
/// The set/clear path validates the bundle the same way the view path does,
/// so every env-path test needs a real bundle on disk. Declaring no gates is
/// the legitimate no-policy case, which that path must still accept.
const MINIMAL_MANIFEST: &str = r#"{"name":"x","version":"1"}"#;

/// Give `dir` the minimal valid bundle manifest the env path requires.
fn write_minimal_manifest(dir: &std::path::Path) {
    write_manifest(dir, ".claude-plugin/plugin.json", MINIMAL_MANIFEST);
}

/// The gate names listed by a `skill approvals` view output's JSON.
fn gate_names(json: &serde_json::Value) -> Vec<String> {
    json["gates"]
        .as_array()
        .expect("view JSON exposes a `gates` array")
        .iter()
        .map(|g| {
            g["gate"]
                .as_str()
                .expect("gate name is a string")
                .to_string()
        })
        .collect()
}

fn usage_class(err: &anyhow::Error) -> crate::exit::ExitClass {
    crate::exit::classify(err).0
}

fn deployment(env: &str, status: &str, version: &str, ts: &str) -> crate::api::Deployment {
    crate::api::Deployment {
        id: format!("dep-{version}"),
        environment: env.into(),
        status: status.into(),
        version_id: Some(version.into()),
        deployed_at: Some(ts.into()),
        workspace_enabled: false,
    }
}

#[test]
fn select_in_force_deployment_prefers_prod_then_most_recent() {
    // Oldest-first, mixed envs/statuses. prod outranks dev; among a rank the
    // most recent (last) active row wins; inactive rows are ignored (#546).
    let deps = vec![
        deployment("dev", "active", "v1", "2026-07-01"),
        deployment("prod", "superseded", "v2", "2026-07-02"),
        deployment("prod", "active", "v3", "2026-07-03"),
        deployment("dev", "active", "v4", "2026-07-04"),
    ];
    assert_eq!(
        select_in_force_deployment(&deps).and_then(|d| d.version_id.clone()),
        Some("v3".to_string()),
        "active prod wins over a newer active dev"
    );
    // No prod: newest active dev.
    let dev_only = vec![
        deployment("dev", "active", "a", "2026-07-01"),
        deployment("dev", "active", "b", "2026-07-05"),
    ];
    assert_eq!(
        select_in_force_deployment(&dev_only).and_then(|d| d.version_id.clone()),
        Some("b".to_string())
    );
    // No active deployment at all -> nothing in force.
    let none = vec![deployment("dev", "superseded", "x", "2026-07-01")];
    assert!(select_in_force_deployment(&none).is_none());
    assert!(select_in_force_deployment(&[]).is_none());
}

#[test]
fn approvals_summary_line_never_claims_ungated_when_the_manifest_is_unreadable() {
    // The whole point of the three-state split (#607): "no gates found" and
    // "could not look" are different answers, and only the first one licenses
    // the affirmative claim. A failed manifest fetch used to collapse into the
    // second branch here and report the agent as running without approval.
    let ungated = super::approvals_summary_line("weather", &[], None);
    assert!(
        ungated.contains("no tools are gated (calls run without approval)"),
        "a genuinely readable, gate-free agent still gets the affirmative claim: {ungated}"
    );

    let blind = super::approvals_summary_line("weather", &[], Some("the deploy list failed"));
    assert!(
        !blind.contains("no tools are gated"),
        "an unreadable manifest must not render as an affirmative un-gated claim: {blind}"
    );
    assert!(
        blind.contains("could not be read") && blind.contains("the deploy list failed"),
        "the reason we could not look is disclosed: {blind}"
    );

    // Gates found from the platform field while the manifest was unreadable:
    // the list is real but partial, and silence about that implies complete.
    let partial =
        super::approvals_summary_line("weather", &["Bash".into()], Some("the fetch failed"));
    assert!(
        partial.contains("incomplete") && partial.contains("could not be read"),
        "a partial list discloses that more gates may be armed: {partial}"
    );

    let complete = super::approvals_summary_line("weather", &["Bash".into()], None);
    assert!(
        !complete.contains("incomplete") && complete.contains("1 gated tool(s)"),
        "a fully-read gate list makes no incompleteness caveat: {complete}"
    );
}

// --- the approval-route pre-check (#2448) ------------------------------

/// AC4: the CLI's declared-route reader executes the same frozen vector the
/// API reader and the runner loader execute (#2436), so the three readers of
/// one manifest cannot drift apart.
#[test]
fn cli_declared_route_reader_executes_the_frozen_vector() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/vectors/approval-route-normalization.json"
    );
    let vector: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read the frozen route vector"))
            .expect("the frozen route vector is JSON");
    let cases = vector["cases"].as_array().expect("the vector has cases");
    assert!(
        !cases.is_empty(),
        "the frozen vector must carry cases, or this test proves nothing"
    );

    for case in cases {
        let id = case["id"].as_str().expect("every case has an id");
        let mut manifest = json!({"name": "route-vector", "version": "0.1.0"});
        if !case["gates"].is_null() {
            manifest["approvalPolicy"] = json!({ "gates": case["gates"].clone() });
        }
        let parsed = parse_manifest_gates(&manifest.to_string(), &format!("vector case {id}"));

        if case["expected"] == json!("rejected") {
            assert!(
                parsed.is_err(),
                "case {id:?}: the vector says rejected, but the reader accepted {parsed:?}"
            );
            continue;
        }
        let expected: Vec<String> = serde_json::from_value(case["expected"].clone())
            .unwrap_or_else(|e| panic!("case {id:?}: expected must be a route list: {e}"));
        let gates = parsed
            .unwrap_or_else(|e| panic!("case {id:?}: the vector declares {expected:?}: {e:#}"));
        let declared: Vec<String> = declared_approval_routes(&gates).into_iter().collect();
        assert_eq!(
            declared, expected,
            "case {id:?}: the CLI reader drifted from the frozen vector"
        );
    }
}

fn route_set(names: &[&str]) -> std::collections::BTreeSet<String> {
    names.iter().map(|n| n.to_string()).collect()
}

#[test]
fn unbound_approval_routes_is_a_verbatim_case_sensitive_sorted_difference() {
    let bound = ["finance".to_string(), "extra".to_string()];
    assert_eq!(
        unbound_approval_routes(&route_set(&["ops", "finance"]), bound.iter()),
        vec!["ops".to_string()]
    );

    let padded = [" ops ".to_string()];
    assert_eq!(
        unbound_approval_routes(&route_set(&["ops"]), padded.iter()),
        vec!["ops".to_string()],
        "a padded bound key does not bind the trimmed declared route"
    );

    let cased = ["Ops".to_string()];
    assert_eq!(
        unbound_approval_routes(&route_set(&["ops"]), cased.iter()),
        vec!["ops".to_string()],
        "route names compare case-sensitively"
    );

    assert!(unbound_approval_routes(&route_set(&[]), bound.iter()).is_empty());

    let none: [String; 0] = [];
    assert_eq!(
        unbound_approval_routes(&route_set(&["zeta", "alpha", "mid"]), none.iter()),
        vec!["alpha".to_string(), "mid".to_string(), "zeta".to_string()],
        "the unbound routes come back sorted"
    );
}

fn precheck_agent(routes: &[&str]) -> crate::api::Agent {
    let routes: Option<serde_json::Map<String, serde_json::Value>> = if routes.is_empty() {
        None
    } else {
        Some(
            routes
                .iter()
                .map(|r| {
                    (
                        r.to_string(),
                        json!({"resolution": {"kind": "slack", "address": "C0EXAMPLE1"}}),
                    )
                })
                .collect(),
        )
    };
    serde_json::from_value(json!({
        "id": "ag_2448",
        "name": "deal-desk",
        "channels": [{"kind": "slack", "address": "C0EXAMPLE0"}],
        "approval_routes": routes,
        "memory": false,
    }))
    .expect("the fixture is a valid AgentOut")
}

fn deploy_refusal(result: anyhow::Result<()>) -> (String, String) {
    let err = result.expect_err("an unbound declared route must refuse");
    let (class, fix) = crate::exit::classify(&err);
    assert_eq!(class, crate::exit::ExitClass::Usage, "{err:#}");
    (
        format!("{err:#}"),
        fix.unwrap_or_else(|| panic!("the refusal must carry a fix: {err:#}")),
    )
}

#[test]
fn check_deploy_routes_bound_refuses_only_an_unbound_declared_route() {
    use crate::api::ChannelOutcome;
    let created = ChannelOutcome::Created("C0EXAMPLE0".to_string());
    let unchanged = ChannelOutcome::Unchanged {
        channels: vec!["C0EXAMPLE0".to_string()],
        passed: false,
    };
    let unbound_agent = precheck_agent(&[]);

    // Unreadable (None) or empty declared sets never refuse: fail-open.
    assert!(check_deploy_routes_bound(
        None,
        "deal-desk",
        &unbound_agent,
        &created,
        DeployTier::Local
    )
    .is_ok());
    assert!(check_deploy_routes_bound(
        Some(&route_set(&[])),
        "deal-desk",
        &unbound_agent,
        &created,
        DeployTier::Local
    )
    .is_ok());
    // Everything declared is bound.
    assert!(check_deploy_routes_bound(
        Some(&route_set(&["ops"])),
        "deal-desk",
        &precheck_agent(&["ops", "legacy"]),
        &unchanged,
        DeployTier::Cluster
    )
    .is_ok());

    // A just-created agent with no bindings, local tier.
    let (message, fix) = deploy_refusal(check_deploy_routes_bound(
        Some(&route_set(&["ops"])),
        "deal-desk",
        &unbound_agent,
        &created,
        DeployTier::Local,
    ));
    assert!(message.contains("\"ops\""), "{message}");
    assert!(
        message.contains("this agent binds no approval routes"),
        "{message}"
    );
    assert!(message.contains("was created by this deploy"), "{message}");
    assert!(
        fix.contains(
            "curie local approvals deal-desk --route-resolution ops=<channel> \
             --route-approvers ops=users:<user-id>"
        ),
        "{fix}"
    );
    assert!(
        fix.contains("operator principal"),
        "the fix says why an explicit user list matters (#2902): {fix}"
    );
    assert!(!fix.contains("curie cluster approvals"), "{fix}");
    assert!(
        !fix.contains("--namespace/--release/--api-url"),
        "only the cluster tier repeats connection flags: {fix}"
    );
    assert!(
        !fix.contains("--routes-from"),
        "nothing is bound, so nothing needs preserving: {fix}"
    );

    // The same agent, not created by this deploy, on the cluster tier.
    let (message, fix) = deploy_refusal(check_deploy_routes_bound(
        Some(&route_set(&["ops"])),
        "deal-desk",
        &unbound_agent,
        &unchanged,
        DeployTier::Cluster,
    ));
    assert!(
        message.contains("No version, bundle, or deployment was created"),
        "{message}"
    );
    assert!(!message.contains("was created by this deploy"), "{message}");
    assert!(fix.contains("curie cluster approvals deal-desk"), "{fix}");
    assert!(!fix.contains("curie local approvals"), "{fix}");
    assert!(fix.contains("--namespace/--release/--api-url"), "{fix}");

    // A route already bound is repeated in the one full-replacement write.
    let (message, fix) = deploy_refusal(check_deploy_routes_bound(
        Some(&route_set(&["ops", "finance"])),
        "deal-desk",
        &precheck_agent(&["finance"]),
        &unchanged,
        DeployTier::Local,
    ));
    assert!(
        message.contains("bound routes are \"finance\""),
        "{message}"
    );
    assert!(fix.contains("--route-resolution ops=<channel>"), "{fix}");
    assert!(
        fix.contains("--route-resolution finance=<channel>"),
        "{fix}"
    );
    assert!(
        fix.contains("--route-approvers finance=users:<user-id>"),
        "{fix}"
    );
    assert!(fix.contains("--routes-from"), "{fix}");
}

#[test]
fn route_write_refusal_names_every_deployment_declaring_a_removed_route() {
    use std::collections::BTreeMap;
    let write = |routes: &[&str]| -> BTreeMap<String, crate::api::ApprovalRouteBindingWrite> {
        routes
            .iter()
            .map(|r| {
                (
                    r.to_string(),
                    crate::api::ApprovalRouteBindingWrite {
                        resolution: crate::api::ApprovalResolutionWrite::slack("C0EXAMPLE1"),
                        notification: None,
                        approvers: None,
                    },
                )
            })
            .collect()
    };
    let declaring = vec![
        DeclaringVersion {
            version_id: "ver-a".to_string(),
            deployments: vec![("dep-dev".to_string(), "dev".to_string())],
            routes: route_set(&["ops", "finance"]),
        },
        DeclaringVersion {
            version_id: "ver-b".to_string(),
            deployments: vec![("dep-prod".to_string(), "prod".to_string())],
            routes: route_set(&["ops"]),
        },
    ];

    assert!(route_write_refusal("deal-desk", &write(&["ops", "finance"]), &declaring).is_none());
    assert!(route_write_refusal("deal-desk", &write(&[]), &[]).is_none());

    let err = route_write_refusal("deal-desk", &write(&["finance"]), &declaring)
        .expect("dropping a declared route must refuse");
    let message = format!("{err:#}");
    let (class, fix) = crate::exit::classify(&err);
    assert_eq!(class, crate::exit::ExitClass::Usage, "{message}");
    for needle in [
        "deal-desk",
        "\"ops\"",
        "dep-dev",
        "dep-prod",
        "ver-a",
        "ver-b",
    ] {
        assert!(message.contains(needle), "must name {needle:?}: {message}");
    }
    assert!(
        !message.contains("\"finance\""),
        "a route the write keeps is not reported: {message}"
    );
    let fix = fix.expect("the refusal carries a fix");
    assert!(fix.contains("DELETE /deployments/"), "{fix}");
    assert!(
        fix.contains("deploying a version that declares no gates is not enough"),
        "{fix}"
    );
}

#[test]
fn parse_manifest_gates_extracts_gate_route_pairs() {
    // The shared parser (#546) recovers approvalPolicy gates from raw manifest
    // text, the same shape `local`/`cluster approvals` union into the report.
    let gates = parse_manifest_gates(
        r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"mcp__plugin_gh_github__create_issue","route":"eng"}]}}"#,
        "test manifest",
    )
    .expect("valid manifest parses");
    assert_eq!(
        gates,
        vec![(
            "mcp__plugin_gh_github__create_issue".to_string(),
            "eng".to_string()
        )]
    );
    // A manifest missing the required `name` disarms every gate -> surfaced.
    assert!(parse_manifest_gates(
        r#"{"version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"}]}}"#,
        "bad manifest",
    )
    .is_err());
}

#[test]
fn parse_manifest_gates_refuses_a_gate_the_runner_cannot_arm() {
    // NEGATIVE CONTROL for the #520 CLI mirror. The runner refuses to boot
    // on a declared gate it cannot arm, so reporting `Bash` as armed here
    // would name a gate that in fact stops the runner. Restoring the old
    // `continue` (drop the empty gate, keep its siblings) makes this fail.
    for body in [
        // Present but empty/whitespace: parses, keys nothing once trimmed.
        r#"{"name":"x","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"   ","route":"eng"}]}}"#,
        r#"{"name":"x","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"Write","route":""}]}}"#,
        // Required key missing entirely.
        r#"{"name":"x","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"Write"}]}}"#,
    ] {
        assert!(
            parse_manifest_gates(body, "partial manifest").is_err(),
            "reported gates for a manifest the runner refuses: {body}"
        );
    }
    // An explicitly empty gates list declares nothing: no gates, no error.
    assert_eq!(
        parse_manifest_gates(r#"{"name":"x","approvalPolicy":{"gates":[]}}"#, "empty")
            .expect("an empty gates list is a valid declaration of no gates"),
        Vec::new()
    );
}

#[test]
fn parse_manifest_gates_refuses_a_manifest_invalid_in_an_unrelated_field() {
    // NEGATIVE CONTROL for issue #701 (sibling of #691, ADR-0041's formerly
    // "known limitation"). `ManifestApprovals` reads only `name` +
    // `approvalPolicy`, so a manifest with a well-formed policy but a
    // TYPE-INVALID unrelated modeled field (`commands` must be a string, a
    // list of strings, or null per `plugin_format.models.PluginManifest`)
    // used to parse straight through: this view would report `Bash` as
    // armed while the runner's own `PluginManifest.model_validate` raises
    // and refuses to boot with ANY of the declared gates armed -- the
    // exact silent-drift class #691 closed on the api.rs seam, reproduced
    // here on the plugin_format seam. Deleting
    // `validate_against_plugin_format_schema`'s call in
    // `parse_manifest_gates` makes this test fail (back to `Ok(vec![("Bash",
    // "eng")])`).
    let body = r#"{
        "name": "deal-desk",
        "commands": 123,
        "approvalPolicy": {"gates": [{"gate": "Bash", "route": "eng"}]}
    }"#;
    let err = parse_manifest_gates(body, "test manifest").expect_err(
        "a manifest invalid in an unrelated modeled field must not report gates as armed",
    );
    assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
}

#[test]
fn parse_manifest_gates_tolerates_an_unmodeled_extra_field() {
    // Positive control paired with the test above: `plugin_format`'s models
    // are deliberately lenient (`extra="allow"`) so a real bundle carrying a
    // field this schema does not model yet (e.g. a future Claude Code key)
    // must still validate and report its gates -- the gate here is on TYPE
    // validity of MODELED fields, never on the presence of an unmodeled one.
    let body = r#"{
        "name": "deal-desk",
        "someFutureClaudeCodeKey": {"nested": true},
        "approvalPolicy": {"gates": [{"gate": "Bash", "route": "eng"}]}
    }"#;
    assert_eq!(
        parse_manifest_gates(body, "test manifest")
            .expect("an unmodeled extra field must not be rejected"),
        vec![("Bash".to_string(), "eng".to_string())]
    );
}

#[test]
fn parse_manifest_gates_skips_full_validation_when_no_policy_is_declared() {
    // A manifest with no `approvalPolicy` (or an explicit `null`) never
    // reaches the runner's full-`PluginManifest` validation either (see
    // `resolve_approval_policy`'s early return), so an unrelated
    // type-invalid field must not be surfaced here -- there is no policy to
    // falsely report as armed, so the honest answer stays the empty list.
    for body in [
        r#"{"name": "deal-desk", "commands": 123}"#,
        r#"{"name": "deal-desk", "commands": 123, "approvalPolicy": null}"#,
    ] {
        assert_eq!(
            parse_manifest_gates(body, "test manifest")
                .expect("no declared policy must not trip the full-manifest schema gate"),
            Vec::new()
        );
    }
}

#[test]
fn skill_approvals_list_resolve_reported_unavailable_not_absent() {
    // ADR-0077 / ADR-0041: --list/--resolve are answered-as-unavailable at
    // the skill tier (exit 4, carrying a cross-tier fix), with the reason and
    // the local/cluster alternative -- not silently broken.
    let err = super::skill_approvals_list_unavailable();
    let (class, fix) = crate::exit::classify(&err);
    assert_eq!(class, crate::exit::ExitClass::Unsupported);
    assert!(
        fix.expect("an unsupported error carries the cross-tier fix")
            .contains("approvals"),
        "the fix must point at the local/cluster alternative"
    );
    let shown = format!("{err:#}");
    assert!(shown.contains("durable approval record"), "reason: {shown}");
    assert!(shown.contains("cluster approvals"), "alternative: {shown}");
}

#[test]
fn approval_gate_decl_parses_grantable_via_policy() {
    // #558: the operator opt-in on a manifest gate. Absent -> defaults false
    // (old manifests keep the no-grant baseline); present true -> parses.
    let without: ApprovalGateDecl =
        serde_json::from_str(r#"{"gate":"close_issue","route":"deal-desk"}"#)
            .expect("a gate without grantableViaPolicy parses");
    assert!(!without.grantable_via_policy);

    let with: ApprovalGateDecl = serde_json::from_str(
        r#"{"gate":"close_issue","route":"deal-desk","grantableViaPolicy":true}"#,
    )
    .expect("a gate with grantableViaPolicy:true parses");
    assert!(with.grantable_via_policy);

    let templated: ApprovalGateDecl = serde_json::from_str(
        r#"{"gate":"Bash","route":"managers","summary":"Run {command}. Approve?"}"#,
    )
    .expect("a gate with a summary template parses");
    assert_eq!(
        templated.summary.as_deref(),
        Some("Run {command}. Approve?")
    );
}

#[tokio::test]
async fn skill_approvals_view_lists_bundle_gates() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_manifest(
        dir.path(),
        ".claude-plugin/plugin.json",
        r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"mcp__x__y","route":"eng"}]}}"#,
    );
    let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
        .await
        .unwrap();
    let names = gate_names(&out.to_json());
    assert!(names.contains(&"Bash".to_string()), "{names:?}");
    assert!(names.contains(&"mcp__x__y".to_string()), "{names:?}");
}

#[tokio::test]
async fn skill_approvals_view_reads_fallback_plugin_json() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_manifest(
        dir.path(),
        "plugin.json",
        r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"}]}}"#,
    );
    let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
        .await
        .unwrap();
    assert_eq!(gate_names(&out.to_json()), vec!["Bash".to_string()]);
}

#[tokio::test]
async fn skill_approvals_view_empty_when_no_policy() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_manifest(
        dir.path(),
        ".claude-plugin/plugin.json",
        r#"{"name":"x","version":"1"}"#,
    );
    let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
        .await
        .unwrap();
    assert!(gate_names(&out.to_json()).is_empty());
}

#[tokio::test]
async fn skill_approvals_view_refuses_an_incomplete_gate() {
    let dir = tempfile::tempdir().unwrap();
    // The second gate has an empty route, so it keys nothing and the runner
    // refuses to boot on it (#520). Mirror that refusal: reporting `Bash` as
    // armed while the runner will not start is the drift this test pins.
    write_manifest(
        dir.path(),
        ".claude-plugin/plugin.json",
        r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"NoRoute","route":""}]}}"#,
    );
    assert!(
        super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .is_err(),
        "reported gates for a manifest the runner refuses to boot on"
    );
}

#[tokio::test]
async fn skill_approvals_view_duplicate_gate_collapses_to_last_route() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    // The runner keys a dict by trimmed gate name, so `Bash` declared twice
    // arms ONCE with the LAST route. Reporting both would name a gate the
    // runner never arms and a route it never fires.
    write_manifest(
        dir.path(),
        ".claude-plugin/plugin.json",
        r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"stale"},{"gate":"Other","route":"ops"},{"gate":" Bash ","route":"eng"}]}}"#,
    );
    let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
        .await
        .unwrap();
    let json = out.to_json();
    let gates = json["gates"].as_array().unwrap();
    assert_eq!(
        gates.len(),
        2,
        "a gate declared twice must collapse to one entry, as the runner's dict does: {gates:?}"
    );
    let bash: Vec<&serde_json::Value> = gates.iter().filter(|g| g["gate"] == "Bash").collect();
    assert_eq!(bash.len(), 1, "exactly one Bash entry: {gates:?}");
    assert_eq!(
        bash[0]["route"], "eng",
        "the LAST declaration must win the route, mirroring the runner's dict comprehension: {gates:?}"
    );
    // First declaration fixes position, as Python dict insertion order does.
    assert_eq!(
        gates[0]["gate"], "Bash",
        "order must stay stable: {gates:?}"
    );
}

#[test]
fn skill_approvals_render_never_claims_calls_are_ungated() {
    // The bundle is not the effective policy: CURIE_APPROVAL_REQUIRED_TOOLS
    // is resolved at container boot and cannot be seen from here, so neither
    // branch may imply the listed gates are the complete set.
    let empty = super::gates_summary_line(&[]);
    assert!(
        !empty.contains("without approval"),
        "an empty bundle policy must not claim calls run without approval -- an env override may gate them: {empty}"
    );
    assert!(
        empty.contains("CURIE_APPROVAL_REQUIRED_TOOLS"),
        "the empty render must name the override it cannot see: {empty}"
    );
    let listed = super::gates_summary_line(&[("Bash".into(), "eng".into())]);
    assert!(
        listed.contains("CURIE_APPROVAL_REQUIRED_TOOLS"),
        "the non-empty render must not imply the listed gates are the complete effective set: {listed}"
    );
}

// --- tier 1: a REQUIRED key missing disarms the WHOLE policy in the runner.
// `plugin_format.models.ApprovalGate` declares `gate: str` / `route: str`
// with no default, so `model_validate` raises and `load_approval_policy`
// returns {} -- zero gates armed. Reporting the well-formed sibling as armed
// would claim a safety control the runner never arms.

// Also covers `--gate`/`--clear` argument misuse, and the set path, which
// must not be more credulous than the view path: both emit an answer ABOUT
// a specific bundle, so a missing or invalid manifest is a usage error on
// either path (previously `--plugin-dir /does/not/exist` exited 0 on set).
#[tokio::test]
async fn skill_approvals_invalid_input_is_usage_error() {
    struct Case {
        name: &'static str,
        manifest: Option<&'static str>,
        gates: &'static [&'static str],
        clear: bool,
        must_not_contain: Option<&'static str>,
    }
    const CASES: &[Case] = &[
        Case {
            name: "view_gate_missing_route_key",
            manifest: Some(
                r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"NoRoute"}]}}"#,
            ),
            gates: &[],
            clear: false,
            // The sibling must not be reported as armed anywhere in the message.
            must_not_contain: Some("Bash -> eng"),
        },
        Case {
            name: "view_gate_missing_gate_key",
            manifest: Some(
                r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"route":"eng"}]}}"#,
            ),
            gates: &[],
            clear: false,
            must_not_contain: None,
        },
        Case {
            // `PluginManifest` requires `name`; without it the runner's parse
            // raises and it arms zero gates, so listing `Bash` would be false.
            name: "view_manifest_without_name",
            manifest: Some(
                r#"{"version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"}]}}"#,
            ),
            gates: &[],
            clear: false,
            must_not_contain: None,
        },
        Case {
            name: "view_malformed_json",
            manifest: Some(r#"{"name":"x",,}"#),
            gates: &[],
            clear: false,
            must_not_contain: None,
        },
        Case {
            name: "view_without_manifest",
            manifest: None,
            gates: &[],
            clear: false,
            must_not_contain: None,
        },
        Case {
            name: "clear_with_gate",
            manifest: None,
            gates: &["X"],
            clear: true,
            must_not_contain: None,
        },
        Case {
            // A comma cannot round-trip through the CSV env encoding.
            name: "comma_in_gate",
            manifest: None,
            gates: &["a,b"],
            clear: false,
            must_not_contain: None,
        },
        Case {
            name: "whitespace_gate",
            manifest: None,
            gates: &["  "],
            clear: false,
            must_not_contain: None,
        },
        Case {
            name: "set_without_manifest",
            manifest: None,
            gates: &["A"],
            clear: false,
            must_not_contain: None,
        },
        Case {
            name: "clear_without_manifest",
            manifest: None,
            gates: &[],
            clear: true,
            must_not_contain: None,
        },
        Case {
            // The view path rejects a manifest the runner's parse would
            // reject; the set path names the same bundle, so it must too.
            name: "set_with_invalid_manifest",
            manifest: Some(r#"{"name":"x",,}"#),
            gates: &["A"],
            clear: false,
            must_not_contain: None,
        },
    ];
    for case in CASES {
        let dir = tempfile::tempdir().unwrap();
        if let Some(body) = case.manifest {
            write_manifest(dir.path(), ".claude-plugin/plugin.json", body);
        }
        let gates = case.gates.iter().map(|g| g.to_string()).collect();
        let err = super::skill_approvals(dir.path().to_path_buf(), gates, case.clear)
            .await
            .unwrap_err();
        assert_eq!(
            usage_class(&err),
            crate::exit::ExitClass::Usage,
            "case {}: {err:#}",
            case.name
        );
        if let Some(fragment) = case.must_not_contain {
            assert!(
                !format!("{err:#}").contains(fragment),
                "case {}: a key-missing gate disarms every gate: {err:#}",
                case.name
            );
        }
    }
}

#[tokio::test]
async fn skill_approvals_set_emits_env_assignment() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_minimal_manifest(dir.path());
    let out = super::skill_approvals(
        dir.path().to_path_buf(),
        vec!["A".into(), "B".into()],
        false,
    )
    .await
    .unwrap();
    let json = out.to_json();
    assert_eq!(
        json["env"].as_str().unwrap(),
        "CURIE_APPROVAL_REQUIRED_TOOLS=A,B"
    );
    let restart = json["restart"].as_str().unwrap();
    assert!(
        restart.contains("--secret CURIE_APPROVAL_REQUIRED_TOOLS"),
        "the restart caveat must name the --secret forwarding that actually applies the env, not a bare `skill up` (which forwards only model credentials): {restart}"
    );
    assert!(
        restart.contains("boot"),
        "the restart caveat must still say the env resolves once at container boot: {restart}"
    );
    assert!(
        restart.contains(&dir.path().display().to_string()),
        "the restart caveat must carry the caller's --plugin-dir so the re-boot targets the bundle whose approvals were read, not whatever bundle happens to be in the CWD: {restart}"
    );
    assert!(
        restart.contains("curie skill down"),
        "the restart caveat must name the stop-first step: `start` hard-errors when a runner is already recorded for the dir: {restart}"
    );
    let bundle_note = json["bundle_note"].as_str().unwrap();
    assert!(
        bundle_note.contains("adds to") && bundle_note.contains("cannot remove"),
        "the set path's bundle note must state the add-only semantics (the runner unions the bundle gates with the override): {bundle_note}"
    );
}

#[tokio::test]
async fn skill_approvals_clear_emits_empty_env_assignment() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_minimal_manifest(dir.path());
    let out = super::skill_approvals(dir.path().to_path_buf(), vec![], true)
        .await
        .unwrap();
    let json = out.to_json();
    assert_eq!(
        json["env"].as_str().unwrap(),
        "CURIE_APPROVAL_REQUIRED_TOOLS="
    );
    let restart = json["restart"].as_str().unwrap();
    assert!(
        restart.contains("--secret CURIE_APPROVAL_REQUIRED_TOOLS"),
        "the clear path's restart caveat must name the --secret forwarding too: a bare `skill up` never forwards the cleared assignment either: {restart}"
    );
    assert!(
        restart.contains(&dir.path().display().to_string()),
        "the clear path's restart caveat must carry the caller's --plugin-dir too, or the re-boot clears the override on the wrong bundle: {restart}"
    );
    assert!(
        restart.contains("curie skill down"),
        "the clear path's restart caveat must name the stop-first step too: {restart}"
    );
    let bundle_note = json["bundle_note"].as_str().unwrap();
    assert!(
        bundle_note.contains("only the env override") && bundle_note.contains("stay armed"),
        "the clear path's bundle note must state that it clears only the override and leaves the bundle-declared gates armed: {bundle_note}"
    );
}

/// `skill approvals` reads only the bundle on disk, so it cannot know which
/// of `skill up`'s flags (image, port, name, network, otel-endpoint, budget,
/// model, local-model, fake-model, repeatable --secret) the caller passed.
/// A synthesized `skill up --secret ...` presented as the command to run is
/// therefore actively destructive: following it re-boots the runner on
/// defaults, switching model provider and dropping every other connector
/// `--secret`. The guidance must point at the caller's OWN invocation.
#[tokio::test]
async fn skill_approvals_restart_points_at_the_callers_own_up_invocation() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_minimal_manifest(dir.path());
    for (gate, clear) in [(vec!["A".to_string()], false), (vec![], true)] {
        let out = super::skill_approvals(dir.path().to_path_buf(), gate, clear)
            .await
            .unwrap();
        let json = out.to_json();
        let restart = json["restart"].as_str().unwrap();
        assert!(
            !restart.contains("`curie skill up --secret"),
            "the guidance must not synthesize a `skill up --secret ...` command line: this command cannot reconstruct the caller's original flags, so pasting it re-boots on defaults and drops their other --secret credentials (clear={clear}): {restart}"
        );
        assert!(
            restart.contains("your own original `curie skill up` invocation"),
            "the guidance must direct the caller to re-run their own original invocation with the flag added (clear={clear}): {restart}"
        );
    }
}

#[tokio::test]
async fn skill_approvals_restart_shell_quotes_a_bundle_path_with_a_space() {
    use crate::ui::CliOutput;
    // The guidance names the bundle dir inside shell-facing text, so a path
    // the shell would split must travel quoted or it names a different dir.
    let dir = tempfile::tempdir().unwrap();
    let spaced = dir.path().join("my bundle");
    std::fs::create_dir(&spaced).unwrap();
    write_minimal_manifest(&spaced);
    let out = super::skill_approvals(spaced.clone(), vec!["A".to_string()], false)
        .await
        .unwrap();
    let json = out.to_json();
    let restart = json["restart"].as_str().unwrap();
    assert!(
        restart.contains(&format!("'{}'", spaced.display())),
        "a bundle path containing a space must be emitted single-quoted: {restart}"
    );
}

/// The guidance says to export the assignment, so the human line is read as
/// shell text. `--gate` rejects only commas and whitespace-only names, so a
/// gate with a space reaches this line; unquoted, bash word-splits it and the
/// runner is handed a different gate than the one printed.
#[tokio::test]
async fn skill_approvals_human_render_shell_quotes_an_assignment_with_a_space() {
    use crate::ui::CliOutput;
    let dir = tempfile::tempdir().unwrap();
    write_minimal_manifest(dir.path());
    let out = super::skill_approvals(dir.path().to_path_buf(), vec!["Foo Bar".into()], false)
        .await
        .unwrap();
    // The --json field stays the raw assignment: a machine consumer wants the
    // value, not a shell literal it would have to unquote.
    assert_eq!(
        out.to_json()["env"].as_str().unwrap(),
        "CURIE_APPROVAL_REQUIRED_TOOLS=Foo Bar"
    );
    assert_eq!(
        super::human_env_line("CURIE_APPROVAL_REQUIRED_TOOLS=Foo Bar"),
        "CURIE_APPROVAL_REQUIRED_TOOLS='Foo Bar'"
    );
    assert_eq!(
        super::human_env_line("CURIE_APPROVAL_REQUIRED_TOOLS=$(cmd)"),
        "CURIE_APPROVAL_REQUIRED_TOOLS='$(cmd)'",
        "shell syntax in a gate name must be quoted, not left to be substituted on paste"
    );
    // The cleared assignment still renders as an assignment to an empty value.
    assert_eq!(
        super::human_env_line("CURIE_APPROVAL_REQUIRED_TOOLS="),
        "CURIE_APPROVAL_REQUIRED_TOOLS=''"
    );
}

#[test]
fn shell_quote_escapes_an_embedded_single_quote() {
    // The one byte single-quoting cannot carry literally. Closing, escaping,
    // and reopening is what keeps the rest of the path inside the quotes.
    assert_eq!(super::shell_quote("/tmp/it's here"), r"'/tmp/it'\''s here'");
    assert_eq!(super::shell_quote("/tmp/plain"), "'/tmp/plain'");
}

// --- the set path must not be more credulous than the view path ----------
// Both emit an answer ABOUT a specific bundle. The view path errors when the
// bundle has no manifest; the set path emitted export-then-reboot guidance
// naming a directory it had never opened, so `--plugin-dir /does/not/exist`
// exited 0 and the guidance failed later at `skill up`.

#[tokio::test]
async fn skill_approvals_set_with_valid_manifest_and_no_policy_succeeds() {
    use crate::ui::CliOutput;
    // Regression guard on the two-tier semantics: a manifest that parses but
    // declares no `approvalPolicy` is the legitimate no-gates case, not an
    // invalid bundle. Setting an env override for it must still work -- the
    // validation may only reject a missing, unreadable, or invalid manifest.
    let dir = tempfile::tempdir().unwrap();
    write_minimal_manifest(dir.path());
    let out = super::skill_approvals(dir.path().to_path_buf(), vec!["A".into()], false)
        .await
        .unwrap();
    assert_eq!(
        out.to_json()["env"].as_str().unwrap(),
        "CURIE_APPROVAL_REQUIRED_TOOLS=A"
    );
}

/// AC2: an unavailable verb must name the concept's absence AND point at the
/// tier that answers it. The normal human path renders only the outer message
/// and does not emit this error's fix, so both halves must remain in Display.
#[test]
fn skill_versions_unavailable_message_names_reason_and_alternative() {
    let shown = format!("{:#}", super::skill_versions_unavailable());
    assert!(
        shown.contains(super::VERSIONS_REASON),
        "the human message must carry the reason: {shown}"
    );
    assert!(
        shown.contains(super::VERSIONS_ALT),
        "the human message must carry the cross-tier redirect: {shown}"
    );
}

#[test]
fn skill_memory_unavailable_message_names_reason_and_alternative() {
    let shown = format!("{:#}", super::skill_memory_unavailable());
    assert!(
        shown.contains(super::MEMORY_REASON),
        "the human message must carry the reason: {shown}"
    );
    assert!(
        shown.contains(super::MEMORY_ALT),
        "the human message must carry the cross-tier redirect: {shown}"
    );
}

#[test]
fn skill_versions_unavailable_is_unsupported() {
    let err = super::skill_versions_unavailable();
    assert_eq!(
        crate::exit::classify(&err).0,
        crate::exit::ExitClass::Unsupported
    );
    let json = crate::exit::error_json(&err);
    assert!(
        json["error"].as_str().unwrap().contains("versions"),
        "error names the concept: {}",
        json["error"]
    );
    let fix = json["fix"].as_str().unwrap();
    assert!(
        fix.contains("cluster") || fix.contains("local"),
        "fix names a cross-tier alternative: {fix}"
    );
}

#[test]
fn skill_memory_unavailable_is_unsupported() {
    let err = super::skill_memory_unavailable();
    assert_eq!(
        crate::exit::classify(&err).0,
        crate::exit::ExitClass::Unsupported
    );
    let json = crate::exit::error_json(&err);
    assert!(
        json["error"].as_str().unwrap().contains("memory"),
        "error names the concept: {}",
        json["error"]
    );
    let fix = json["fix"].as_str().unwrap();
    assert!(
        fix.contains("cluster") || fix.contains("local"),
        "fix names a cross-tier alternative: {fix}"
    );
}

// ─── #700: plumbing rows render distinctly from real rows in a sweep ─────

fn real_row(model: &str, passed: usize, total: usize) -> SweepRow {
    SweepRow {
        model: model.to_string(),
        passed,
        // Every case in these plumbing-focused fixtures actually completed;
        // that axis is #622's concern, not this one's.
        completed: total,
        total,
        plumbing: 0,
    }
}

fn plumbing_only_row(model: &str, plumbing: usize) -> SweepRow {
    SweepRow {
        model: model.to_string(),
        passed: 0,
        completed: 0,
        total: 0,
        plumbing,
    }
}

#[test]
fn plumbing_only_row_is_detected_and_a_real_row_is_not() {
    assert!(plumbing_only_row("fake", 3).is_plumbing_only());
    assert!(!real_row("opus", 3, 3).is_plumbing_only());
    // A real row that never ran any case (no cases assigned this model, no
    // plumbing either) is 0/0 but NOT plumbing-only -- there is nothing to
    // distinguish it from, so it must not get the plumbing marker.
    assert!(!real_row("idle", 0, 0).is_plumbing_only());
}

#[test]
fn sweep_json_row_carries_the_plumbing_count_and_a_filterable_boolean() {
    let real = sweep_json_row(&real_row("opus", 2, 3));
    assert_eq!(real["model"], "opus");
    assert_eq!(real["passed"], 2);
    assert_eq!(real["total"], 3);
    assert_eq!(real["plumbing"], 0);
    assert_eq!(real["plumbing_only"], false);

    let plumbing = sweep_json_row(&plumbing_only_row("fake", 5));
    assert_eq!(plumbing["model"], "fake");
    assert_eq!(plumbing["passed"], 0);
    assert_eq!(plumbing["total"], 0);
    assert_eq!(plumbing["plumbing"], 5);
    assert_eq!(
        plumbing["plumbing_only"], true,
        "a scripted consumer filters on this flag to drop fixture rows: {plumbing}"
    );
}

#[test]
fn sweep_table_row_marks_a_plumbing_only_row_distinctly_from_a_real_one() {
    let real = sweep_table_row(&real_row("opus", 3, 3));
    assert_eq!(real[0], "opus");
    assert_eq!(real[1], "3/3");
    assert_eq!(real[2], "100%");
    assert_eq!(real[3], "-");

    let plumbing = sweep_table_row(&plumbing_only_row("fake", 4));
    assert_eq!(
        plumbing[0], "fake (plumbing)",
        "the model name must be marked so it cannot be skimmed as a real row"
    );
    assert_eq!(plumbing[1], "0/0");
    assert_eq!(
        plumbing[2], "n/a",
        "a plumbing-only row must not read as a real 0% failure"
    );
    assert_eq!(plumbing[3], "4");
}

#[test]
fn sweep_table_rows_for_a_mixed_sweep_stay_distinguishable_side_by_side() {
    // A sweep containing both a plumbing row and a real row (#700 AC): the two
    // must render differently enough that scanning the table cannot mistake
    // one for the other.
    let rows = [real_row("opus", 2, 3), plumbing_only_row("fake-model", 3)];
    let table: Vec<Vec<String>> = rows.iter().map(sweep_table_row).collect();
    assert_eq!(table[0], vec!["opus", "2/3", "67%", "-"]);
    assert_eq!(table[1], vec!["fake-model (plumbing)", "0/0", "n/a", "3"]);
    assert_ne!(table[0][2], table[1][2], "pass-rate columns must differ");
}

// ───────────────────────────────────────────────────────────────────────
// #1087 AC2: `skill message` and `skill eval` execute the SAME recorded
// bundle. The mount decision is pure, so the parity seam is provable in CI
// rather than only by the live sweep; each assertion terminates in the argv
// the Docker daemon receives, not in a struct field or an enum variant.
// ───────────────────────────────────────────────────────────────────────

use super::{
    eval_runner_spec, recorded_bundle_digest, resolve_sweep_mount, sweep_snapshot, EvalBundle,
    SweepMount,
};
use crate::docker::StartSpec;
use crate::state::RunnerState;

const RECORDED_SOURCE: &str = "/src";
const RECORDED_SNAPSHOT: &str = "/src/.curie/snapshots/abc";
const RECORDED_DIGEST: &str = "abc";
const RECORDED_URL: &str = "http://localhost:7245";

/// A recorded runner, parameterized by the two #1087 fields so the
/// pre-#1087 and half-written shapes are the SAME fixture with different
/// values rather than three hand-built structs that can drift apart.
fn recorded_runner(digest: Option<&str>, snapshot_dir: Option<&str>) -> RunnerState {
    RunnerState {
        container_id: "abc123".into(),
        container_name: "curie-runner-local".into(),
        image: "curie-runner".into(),
        port: 7245,
        base_url: RECORDED_URL.into(),
        session_id: "local-1".into(),
        plugin_dir: RECORDED_SOURCE.into(),
        fake_model: false,
        ollama_container: None,
        network: None,
        model_base_url: None,
        bundle_digest: digest.map(str::to_string),
        bundle_snapshot_dir: snapshot_dir.map(str::to_string),
        connector_containers: Vec::new(),
        connector_network: None,
    }
}

/// The eval sweep's runner spec, mounting `dir`. Mirrors what
/// `boot_eval_runner` builds, so these tests end where the user's Docker
/// daemon does: in `run_args()`.
fn sweep_spec(dir: &Path) -> StartSpec {
    StartSpec {
        image: "curie-runner".into(),
        container_name: "curie-eval-sweep-0".into(),
        host_port: 7345,
        plugin_dir: dir.to_path_buf(),
        session_id: "eval-1".into(),
        sandbox_id: "local".into(),
        budget_json: r#"{"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#.into(),
        fake_model: false,
        network: None,
        otel_endpoint: None,
        model_base_url: None,
        model: Some("opus".into()),
        passthrough_env: vec![],
        docker_env: vec![],
    }
}

fn mounts(spec: &StartSpec) -> Vec<String> {
    spec.run_args()
        .windows(2)
        .filter(|pair| pair[0] == "-v")
        .map(|pair| pair[1].clone())
        .collect()
}

// #1087 AC2's honesty rule, tested where it now lives. Both `skill status`
// and `skill eval` call this, and both of their own paths need either a cwd
// with recorded state or a live Docker run to reach, so these are the only
// tests that can red when the rule is broken.

#[test]
fn recorded_bundle_digest_reports_the_digest_of_the_recorded_runner() {
    let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

    assert_eq!(
        recorded_bundle_digest(Some(&saved), RECORDED_URL),
        Some(RECORDED_DIGEST.to_string()),
        "the record IS the runner being reported on, so its digest applies"
    );
}

#[test]
fn recorded_bundle_digest_is_none_for_a_runner_the_record_is_not_about() {
    let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

    assert_eq!(
        recorded_bundle_digest(Some(&saved), "http://localhost:9999"),
        None,
        "an explicit --url elsewhere must not be married to this bundle's digest"
    );
}

#[test]
fn recorded_bundle_digest_is_none_when_the_record_predates_the_feature() {
    let saved = recorded_runner(None, None);

    assert_eq!(
        recorded_bundle_digest(Some(&saved), RECORDED_URL),
        None,
        "a pre-#1087 record has no digest to report, and that is not an error"
    );
}

#[test]
fn recorded_bundle_digest_is_none_without_a_record() {
    assert_eq!(recorded_bundle_digest(None, RECORDED_URL), None);
}

#[test]
fn sweep_snapshot_reuses_the_recorded_runners_snapshot() {
    let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

    let (dir, digest) = sweep_snapshot(Some(&saved))
        .expect("a recorded runner's snapshot is what the sweep must mount");

    assert_eq!(dir, PathBuf::from(RECORDED_SNAPSHOT));
    assert_eq!(
        digest, RECORDED_DIGEST,
        "the sweep reports the SAME digest the messaging path recorded"
    );
    assert_eq!(
        mounts(&sweep_spec(&dir)),
        vec![format!("{RECORDED_SNAPSHOT}:/plugin:ro")],
        "the sweep's runner must boot on the recorded snapshot"
    );
}

#[test]
fn sweep_snapshot_is_none_without_a_recorded_snapshot() {
    // No runner recorded at all.
    assert!(sweep_snapshot(None).is_none());
    // A record written before #1087: both fields absent.
    assert!(sweep_snapshot(Some(&recorded_runner(None, None))).is_none());
    // Half written (digest recorded, directory missing): there is nothing to
    // mount, so the sweep packs its own rather than guessing a path.
    assert!(sweep_snapshot(Some(&recorded_runner(Some(RECORDED_DIGEST), None))).is_none());
    // ...and the mirror-image half: a directory with no digest to report.
    assert!(sweep_snapshot(Some(&recorded_runner(None, Some(RECORDED_SNAPSHOT)))).is_none());
}

#[test]
fn sweep_snapshot_never_returns_the_source_dir() {
    let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

    let (dir, _) = sweep_snapshot(Some(&saved)).expect("a snapshot is recorded");

    assert_ne!(
        dir,
        PathBuf::from(RECORDED_SOURCE),
        "reading plugin_dir would remount the editable source the ticket removes"
    );
    assert!(dir.starts_with(format!("{RECORDED_SOURCE}/.curie/snapshots")));
    assert!(
        !mounts(&sweep_spec(&dir)).contains(&format!("{RECORDED_SOURCE}:/plugin:ro")),
        "the mutable source must never reach the sweep's argv"
    );
}

#[test]
fn resolve_sweep_mount_returns_the_recorded_snapshot_when_one_exists() {
    // A source dir is supplied too: reusing the recorded snapshot regardless
    // is the decision `eval_sweep` must not re-make in its own body.
    let resolved = resolve_sweep_mount(
        Some((
            PathBuf::from(RECORDED_SNAPSHOT),
            RECORDED_DIGEST.to_string(),
        )),
        Some(Path::new(RECORDED_SOURCE)),
    );

    match resolved {
        SweepMount::Recorded { dir, digest } => {
            assert_eq!(dir, PathBuf::from(RECORDED_SNAPSHOT));
            assert_eq!(digest, RECORDED_DIGEST);
            assert_eq!(
                mounts(&sweep_spec(&dir)),
                vec![format!("{RECORDED_SNAPSHOT}:/plugin:ro")],
                "the resolved mount is what the sweep's docker run receives"
            );
        }
        SweepMount::PackEphemeral { source } => panic!(
            "a recorded snapshot must be reused, not repacked from {}",
            source.display()
        ),
    }
}

#[test]
fn resolve_sweep_mount_packs_ephemeral_when_nothing_is_recorded() {
    // With a recorded bundle source but no snapshot: pack from that source.
    match resolve_sweep_mount(None, Some(Path::new(RECORDED_SOURCE))) {
        SweepMount::PackEphemeral { source } => {
            assert_eq!(source, PathBuf::from(RECORDED_SOURCE))
        }
        SweepMount::Recorded { dir, .. } => {
            panic!(
                "nothing is recorded, so {} cannot be mounted",
                dir.display()
            )
        }
    }
    // With nothing recorded at all: pack from the cwd. The fall-back is
    // always "pack" -- `SweepMount` has no variant that mounts mutable
    // source, which is the hole this ticket closes.
    match resolve_sweep_mount(None, None) {
        SweepMount::PackEphemeral { source } => assert_eq!(source, PathBuf::from(".")),
        SweepMount::Recorded { dir, .. } => {
            panic!(
                "nothing is recorded, so {} cannot be mounted",
                dir.display()
            )
        }
    }
}

/// A minimal on-disk bundle source, returned with its owning tempdir so the
/// caller keeps it alive. `EvalBundle::materialize` canonicalizes and packs
/// for real, so these cases need real files rather than the string paths the
/// pure-resolver tests above use.
fn bundle_source() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let source = tmp.path().join("bundle");
    std::fs::create_dir_all(source.join("skills/demo")).unwrap();
    std::fs::write(source.join("skills/demo/SKILL.md"), "# demo\n").unwrap();
    (tmp, source)
}

/// The argv-terminating helper for the two `EvalBundle` tests: what
/// `boot_eval_runner` would hand the Docker daemon for this bundle.
fn eval_mounts(bundle: &EvalBundle) -> Vec<String> {
    mounts(&eval_runner_spec(
        bundle,
        "curie-runner",
        7345,
        "curie-eval-sweep-0",
        "opus",
        vec![],
        vec![],
    ))
}

/// #1087 AC2's wiring guard. The pure-resolver tests above prove
/// `resolve_sweep_mount` decides correctly; they cannot prove the eval path
/// OBEYS it, because `eval_sweep` needs a Docker daemon to run at all. This
/// closes that gap from the other end: the eval path's only mountable value
/// is an `EvalBundle`, whose fields are private to its module, so
/// re-introducing the source-directory mount is a compile error in
/// `eval_sweep` and can only be written inside `materialize` -- where this
/// test sees it. Mutating the `PackEphemeral` arm to return `source` reds
/// this test.
#[test]
fn the_eval_bundle_packs_a_snapshot_and_never_mounts_the_mutable_source() {
    let (_tmp, source) = bundle_source();

    let bundle = EvalBundle::materialize(SweepMount::PackEphemeral {
        source: source.clone(),
    })
    .expect("packing an ephemeral snapshot from a real bundle source");

    let canonical_source = source.canonicalize().unwrap();
    assert_ne!(
        bundle.dir(),
        canonical_source,
        "the sweep must execute a snapshot, never the editable source"
    );
    assert!(
        bundle
            .dir()
            .starts_with(canonical_source.join(".curie/snapshots")),
        "a packed snapshot lives under the source's snapshot root, got {}",
        bundle.dir().display()
    );
    assert!(
        !bundle.digest().is_empty(),
        "a packed snapshot reports its own digest for the --json payload"
    );
    // The source it must release when the sweep ends -- and the only reason
    // this variant carries one.
    assert_eq!(bundle.ephemeral_source(), Some(canonical_source.as_path()));

    let mounts = eval_mounts(&bundle);
    assert_eq!(
        mounts,
        vec![format!("{}:/plugin:ro", bundle.dir().display())],
        "the snapshot is what reaches the Docker daemon"
    );
    assert!(
        !mounts.contains(&format!("{}:/plugin:ro", canonical_source.display())),
        "the mutable source must never reach the eval runner's argv"
    );
}

/// The recorded arm of the same guard: the recorded runner's snapshot is
/// mounted as-is and its digest is carried through unchanged, which is what
/// makes `skill message` and `skill eval` report the SAME value rather than
/// two independently recomputed ones. It is also not this run's to delete.
#[test]
fn the_eval_bundle_reuses_the_recorded_snapshot_and_its_digest() {
    let (_tmp, source) = bundle_source();
    let recorded = source.join(".curie/snapshots/abc");
    std::fs::create_dir_all(&recorded).unwrap();

    let bundle = EvalBundle::materialize(SweepMount::Recorded {
        dir: recorded.clone(),
        digest: RECORDED_DIGEST.to_string(),
    })
    .expect("a recorded snapshot on disk resolves");

    assert_eq!(bundle.dir(), recorded.canonicalize().unwrap());
    assert_eq!(
        bundle.digest(),
        RECORDED_DIGEST,
        "the recorded digest is reused, not recomputed"
    );
    assert_eq!(
        bundle.ephemeral_source(),
        None,
        "the recorded runner owns this snapshot; the sweep must not release it"
    );
    assert_eq!(
        eval_mounts(&bundle),
        vec![format!("{}:/plugin:ro", bundle.dir().display())]
    );
}

/// An aborted boot releases the credentials it staged, not just the
/// containers. Deleting the wipe from `release_boot_scaffolding` fails here.
///
/// Nothing else will ever collect them: no state was recorded, so no `skill
/// down` can find the bundle (#1087). Driven with no sidecar, no connector
/// and no network so it reaches no Docker daemon -- what is under test is
/// the release list, not the removals.
#[tokio::test]
async fn an_aborted_boot_releases_the_staged_credentials() {
    let dir = tempfile::tempdir().unwrap();
    crate::connector_build::stage_secret_file(
        dir.path(),
        "kubernetes",
        "/secrets/kubeconfig",
        "creds",
    )
    .unwrap();
    let root = crate::connector_build::connector_secrets_root(dir.path());
    assert!(root.exists());

    super::release_boot_scaffolding(
        None,
        &[],
        None,
        &dir.path().join(".curie/snapshots/never-packed"),
        dir.path(),
    )
    .await;

    assert!(
        !root.exists(),
        "a resolved credential must not outlive the boot that staged it"
    );
}

// @spec ADR-0168 d8
#[test]
fn identity_names_follow_the_deploy_yaml_rule() {
    let longest = "a".repeat(40);
    for ok in ["default", "ops-bot", "a", "b2", longest.as_str()] {
        assert!(
            super::validate_identity_name(ok).is_ok(),
            "{ok:?} is a valid name"
        );
    }
    let too_long = "a".repeat(41);
    for bad in [
        "",
        "Ops",
        "ops_bot",
        "-ops",
        "ops-",
        "ops bot",
        too_long.as_str(),
    ] {
        let err = super::validate_identity_name(bad).unwrap_err();
        assert_eq!(crate::exit::classify(&err).0.code(), 2, "{bad:?}");
        assert!(err.to_string().contains("--identity"), "{err}");
    }
}

/// @spec ACTION-EXECUTOR-23: the deploy preflight both tiers share refuses a
/// plain `SNAPSHOT_SEALING_KEY` with the API's reason, as a usage error, before
/// it reaches the API (port 1 would be a transient error). The reason is read
/// from `tests/vectors/sealing-key-custody.json`, which the API's half reads too.
#[tokio::test]
async fn prepare_deploy_refuses_a_plain_sealing_key_on_both_tiers() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/vectors/sealing-key-custody.json"
    ))
    .expect("parse tests/vectors/sealing-key-custody.json");
    let reason = vector["reasons"]["SNAPSHOT_SEALING_KEY"]
        .as_str()
        .expect("reasons.SNAPSHOT_SEALING_KEY");
    for tier in [super::DeployTier::Local, super::DeployTier::Cluster] {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "sealer").unwrap();
        std::fs::write(
            dir.path().join("connectors.yaml"),
            "connectors:\n  k8s:\n    image: ghcr.io/example/k8s-restorer@sha256:\
             abababababababababababababababababababababababababababababababab\n    \
             env:\n      SNAPSHOT_SEALING_KEY: SEALVALUE-placeholder-7f3c9e1b\n",
        )
        .unwrap();
        let opts = super::DeployOpts {
            delivery: None,
            agent: None,
            target: None,
            identity: None,
            plugin_dir: dir.path().to_path_buf(),
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            repo: None,
            workspace: super::WorkspaceIntent::Preserve,
            tier,
            env: Some(super::DeployEnv::Dev),
            label: Some("v0".to_string()),
            secret: vec![],
            secret_binding_supported: true,
            connect_hint: String::new(),
        };
        let err = match super::prepare_deploy(opts).await {
            Ok(_) => panic!("{tier:?}: a plain SNAPSHOT_SEALING_KEY must be refused"),
            Err(err) => err,
        };
        let rendered = format!("{err:#}");
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Usage,
            "{tier:?}: a custody refusal is a usage error: {rendered}"
        );
        assert!(
            rendered.contains(reason),
            "{tier:?}: the refusal must carry the API's reason verbatim: {rendered}"
        );
        assert!(
            rendered.contains("connectors.yaml (connectors.k8s.env)"),
            "{tier:?}: the refusal names where the key is declared: {rendered}"
        );
        assert!(
            !rendered.contains("SEALVALUE-placeholder"),
            "{tier:?}: the refusal must not echo the key's value: {rendered}"
        );
    }
}

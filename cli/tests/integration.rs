//! The one integration test crate root for the curie CLI. Every file
//! cli/tests/<name>.rs is a module declared below. With `autotests = false`
//! an undeclared file never compiles, which the guard test below catches.

mod support;

mod actions_cli;
mod agent_update_body;
mod api_deploy;
mod api_emit_parity;
mod api_field_parity;
mod api_lifecycle;
mod approval_hint_channel;
mod approval_pending_approvers;
mod approval_presentation;
mod approval_principal;
mod approval_recovery_cli;
mod approval_route_bindings;
mod approval_route_precheck;
mod approvals_list_truncation;
mod approvals_manifest_unreadable;
mod artifacts_fetch;
mod boot_env_contract;
mod callers_verb;
mod channel_token_verb;
mod chart_check;
mod chart_fullname_parity;
mod chat_enqueue;
mod claim_input_reaping;
mod cli_output_variants;
mod cluster_connection_transport;
mod cluster_convergence;
mod cluster_deploy_no_docker;
mod cluster_down_failforward;
mod cluster_down_namespace_bound;
mod cluster_kube_context;
mod cluster_lint_values;
mod cluster_namespace_ownership;
mod cluster_rollback;
mod cluster_rollback_live_schema;
mod cluster_rollback_schema;
mod cluster_secret_hardening;
mod cluster_secret_scope;
mod cluster_secret_scope_kind;
mod cluster_stub_trust;
mod cluster_target;
mod cluster_up_admission_fast_fail;
mod cluster_up_capacity_preflight;
mod cluster_up_e2e_connector_identity;
mod cluster_up_gvisor_preflight;
mod cluster_up_inference;
mod cluster_up_priorityclass_preflight;
mod cluster_up_retained_runner_values;
mod cluster_upgrade;
mod cluster_upgrade_live;
mod cluster_upgrade_matrix;
mod cluster_values_files;
mod command_surface;
mod completion_outbox_health;
mod config_migrate;
mod connector_build;
mod connector_build_decl_vectors;
mod connector_caller_dev_pair;
mod connector_caller_keys;
mod connector_compose;
mod connector_hosted_caller_key;
mod connector_lock;
mod connector_mount_visibility;
mod connector_rollout;
mod connector_rollout_live;
mod connector_selection;
mod connector_start_wait;
mod connector_startup_events;
mod console_login;
mod credential_resolution;
mod cron_trigger_warning;
mod curie_yaml_starter;
mod delivery_diagnostics;
mod delivery_observation;
mod delivery_pin;
mod deploy_environment;
mod deploy_transport;
mod dev_model_credit;
mod docker_no_host_binds;
mod docker_subpath_preflight;
mod doctor_api;
mod doctor_recovery;
mod doctor_release_health;
mod doctor_slack_ready;
mod doctor_target;
mod eval_case_selector;
mod eval_concurrency;
mod eval_empty_model;
mod eval_falsifiability;
mod eval_portforward_cleanup;
mod eval_sampling;
mod eval_thread_reset;
mod example_builtin_coding_tools;
mod example_dark_factory_render;
mod example_sre_bot_install;
mod exit_codes;
mod factory_github_app;
mod factory_quickstart_context;
mod factory_quickstart_forges;
mod factory_quickstart_gitlab;
mod factory_quickstart_gitlab_fixture;
mod factory_quickstart_jira;
mod factory_quickstart_output;
mod factory_toolchain;
mod factory_tracker_scope;
mod fake_tier_plumbing;
mod forge_expiry;
mod forge_fixture;
mod github_app_clap_wiring;
mod github_app_identity;
mod github_app_input_validation;
mod github_app_sandbox_reconciliation;
mod guide;
mod hook_fire;
mod hooks_operator;
mod installation_plan_parity;
mod json_contract;
mod json_emit_contract;
mod kube_spawn_inventory;
mod lease_expiry_cluster_proof;
mod local_connectedness_probe;
mod local_resources;
mod local_stack_keys;
mod local_up_build_channel_guard;
mod local_up_fake_warning;
mod mail_channel_health;
mod memory_add;
mod memory_facts;
mod memory_guidance_clap_wiring;
mod message_argv;
mod message_ephemeral_ports;
mod model_provider_registry;
mod nightly_graded_ladder;
mod non_interactive;
mod observability_query;
mod oci_registry;
mod openrouter_credit;
mod overrides_clap_wiring;
mod platform_image_builder;
mod plugin_format_field_parity;
mod pre_push_hook;
mod probe_fanout;
mod recovery_drill;
mod release_accept;
mod remediation_policy_cli;
mod remediation_receipts_cli;
mod remediation_vectors;
mod remote_dev_workspace;
mod repo_runner;
mod resume_wait;
mod runner_layer_build;
mod runner_stream;
mod schedules;
mod schema_inventory;
mod sealing_key_custody;
mod sealing_key_custody_cli;
mod shipped_bundle_runner_layers;
mod skill_bundle_snapshot;
mod skill_check;
mod skill_up_auto_reload;
mod skill_up_replace;
mod spec_scaffold;
mod sre_observability_provision;
mod surfaces_trigger_label;
mod surfaces_verb;
mod trajectory_eval;
mod trajectory_platform_eval;
mod tui_parity;
mod two_release_approval_e2e;
mod unique_stream;
mod upgrade_drill;
mod valkey_required_gate;
mod verify_fix_pin;
mod work_items;
mod worker_claims;

use std::fs;
use std::path::Path;

/// File names (sorted) ending in `.rs`, other than `integration.rs`, whose stem
/// has no line in `root_source` that equals `mod <stem>;` after trimming.
fn undeclared_test_files(file_names: &[String], root_source: &str) -> Vec<String> {
    let declared: Vec<&str> = root_source.lines().map(str::trim).collect();
    let mut missing: Vec<String> = file_names
        .iter()
        .filter(|name| name.as_str() != "integration.rs")
        .filter_map(|name| name.strip_suffix(".rs").map(|stem| (name, stem)))
        .filter(|(_, stem)| !declared.contains(&format!("mod {stem};").as_str()))
        .map(|(name, _)| name.clone())
        .collect();
    missing.sort();
    missing
}

#[test]
fn every_test_file_is_declared_in_the_integration_crate() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut names = Vec::new();
    for entry in fs::read_dir(&tests_dir).expect("read cli/tests") {
        let entry = entry.expect("read cli/tests entry");
        if !entry.file_type().expect("file type").is_file() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if name.ends_with(".rs") {
                names.push(name.to_string());
            }
        }
    }
    let root_source =
        fs::read_to_string(tests_dir.join("integration.rs")).expect("read tests/integration.rs");
    let missing = undeclared_test_files(&names, &root_source);
    assert!(
        missing.is_empty(),
        "these test files are not declared, so they never compile: {}. \
         Add `mod <name>;` for each to cli/tests/integration.rs",
        missing.join(", ")
    );
}

#[test]
fn every_test_file_is_declared_guard_names_an_undeclared_file() {
    let names: Vec<String> = ["alpha.rs", "zz_probe.rs", "integration.rs", "notes.txt"]
        .iter()
        .map(|name| name.to_string())
        .collect();
    let missing = undeclared_test_files(&names, "mod alpha;\n// mod zz_probe;\n");
    assert_eq!(missing, vec!["zz_probe.rs"]);
}

#[test]
fn every_test_file_is_declared_guard_accepts_a_fully_declared_tree() {
    let names: Vec<String> = ["alpha.rs", "beta.rs", "integration.rs"]
        .iter()
        .map(|name| name.to_string())
        .collect();
    let missing = undeclared_test_files(&names, "mod support;\nmod alpha;\n    mod beta;\n");
    assert!(missing.is_empty(), "{missing:?}");
}

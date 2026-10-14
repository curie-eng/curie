//! Operator fact flags validate and describe every intended operation offline.
//! Storage behavior is exercised by the real local Python test at both tiers.

use std::process::{Command, Output};

const FACT: &str = "fact-0123456789abcdef0123456789abcdef";
const CHANNEL: &str = "webhook=inbox=operator@example.com?view=all#retained%25";

fn run(tier: &str, flags: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            tier,
            "memory",
            "acme-bot",
            "--api-url",
            "http://127.0.0.1:9",
            "--api-key",
            "example-key",
        ])
        .args(flags)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run source CLI")
}

fn dry_plan(tier: &str, flags: &[&str]) -> Vec<String> {
    let mut argv = flags.to_vec();
    argv.extend(["--dry-run", "--json"]);
    let output = run(tier, &argv);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout: {stdout}; stderr: {stderr}"
    );
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("one JSON result");
    assert_eq!(value["dry_run"], true);
    value["plan"]
        .as_array()
        .expect("dry run plan array")
        .iter()
        .map(|line| line.as_str().expect("plan line").to_string())
        .collect()
}

fn refused(tier: &str, flags: &[&str], evidence: &str) {
    let output = run(tier, flags);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "unexpected success: {text}");
    assert!(text.contains(evidence), "expected {evidence:?}: {text}");
    assert!(
        !text.contains("Connection refused"),
        "validation must precede HTTP: {text}"
    );
}

#[test]
fn plain_fact_listing_plans_legacy_agent_and_channel_reads_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let plan = dry_plan(tier, &[]);
        assert!(
            plan.iter()
                .any(|line| line.starts_with("GET ") && line.contains("/agents/<id>/memory")),
            "legacy log must remain in the plan: {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|line| line.starts_with("GET ") && line.contains("/state/memory")),
            "agent facts must be read: {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|line| line.starts_with("GET ") && line.contains("/state/bindings/")),
            "bound channel facts must be read: {plan:?}"
        );
        assert!(plan.iter().all(|line| !line.starts_with("DELETE ")));
    }
}

#[test]
fn selected_channel_listing_preserves_equals_and_encodes_transport_characters() {
    for tier in ["local", "cluster"] {
        let plan = dry_plan(tier, &["--channel", CHANNEL]);
        let reads: Vec<_> = plan
            .iter()
            .filter(|line| line.starts_with("GET "))
            .collect();
        assert!(!reads.is_empty(), "channel listing must read: {plan:?}");
        let selected = reads
            .iter()
            .find(|line| line.contains("/state/bindings/webhook/"))
            .unwrap_or_else(|| panic!("channel facts must be read: {plan:?}"));
        assert!(
            selected.contains("inbox") && selected.contains("operator"),
            "{selected}"
        );
        assert!(
            selected.contains("%3Fview") && selected.contains("%23retained"),
            "{selected}"
        );
        assert!(
            selected.contains("%2525"),
            "literal percent must be encoded: {selected}"
        );
        assert!(
            !selected.contains("?view") && !selected.contains("#retained"),
            "{selected}"
        );
        assert!(!reads
            .iter()
            .any(|line| line.contains("/agents/<id>/memory ")));
        assert!(!reads.iter().any(|line| line.contains("/state/memory")));
    }
}

#[test]
fn deletion_plans_the_selected_fact_read_and_versioned_delete_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for channel in [None, Some("slack=C0EXAMPLE1")] {
            let mut flags = vec!["--delete", FACT];
            if let Some(channel) = channel {
                flags.extend(["--channel", channel]);
            }
            let plan = dry_plan(tier, &flags);
            let read = plan
                .iter()
                .position(|line| line.starts_with("GET ") && line.contains(FACT))
                .unwrap_or_else(|| panic!("read current row before deleting: {plan:?}"));
            let delete = plan
                .iter()
                .position(|line| line.starts_with("DELETE ") && line.contains(FACT))
                .unwrap_or_else(|| panic!("delete selected row: {plan:?}"));
            assert!(read < delete, "the read must precede the delete: {plan:?}");
            assert!(
                plan[delete].contains("expected_version"),
                "CAS must be explicit: {plan:?}"
            );
            if channel.is_some() {
                assert!(plan[read].contains("/state/bindings/slack/C0EXAMPLE1/memory/"));
                assert!(plan[delete].contains("/state/bindings/slack/C0EXAMPLE1/memory/"));
            } else {
                assert!(plan[read].contains("/state/memory/"));
                assert!(plan[delete].contains("/state/memory/"));
                assert!(!plan[delete].contains("/bindings/"));
            }
        }
    }
}

#[test]
fn canonical_fact_ids_require_an_exact_full_match_before_any_http() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--delete", FACT]);
        for invalid in [
            "log",
            "guidance",
            "fact-short",
            "fact-0123456789ABCDEF0123456789abcdef",
            "fact-0123456789abcdef0123456789abcdef0",
            "../guidance",
            "fact-0123456789abcdef0123456789abcdef/log",
            "fact-0123456789abcdef0123456789abcdef\n",
        ] {
            refused(tier, &["--delete", invalid, "--dry-run", "--json"], "fact");
        }
    }
}

#[test]
fn malformed_channel_pairs_are_refused_before_any_http() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--channel", "slack=C0EXAMPLE1"]);
        for invalid in ["slack", "=C0EXAMPLE1", "slack="] {
            refused(
                tier,
                &["--channel", invalid, "--dry-run", "--json"],
                "channel",
            );
            refused(
                tier,
                &[
                    "--delete",
                    FACT,
                    "--channel",
                    invalid,
                    "--dry-run",
                    "--json",
                ],
                "channel",
            );
        }
    }
}

#[test]
fn delete_and_channel_selection_conflict_with_add_and_every_guidance_flag() {
    for tier in ["local", "cluster"] {
        for action in [
            vec!["--add", "operator note"],
            vec!["--guidance"],
            vec!["--guidance-from", "missing-guidance.md"],
            vec!["--reset-guidance"],
        ] {
            for selection in [
                vec!["--delete", FACT],
                vec!["--channel", "slack=C0EXAMPLE1"],
            ] {
                for (first, second) in [(&action, &selection), (&selection, &action)] {
                    let mut flags = first.clone();
                    flags.extend(second.iter().copied());
                    flags.extend(["--dry-run", "--json"]);
                    refused(tier, &flags, "cannot be used");
                }
            }
        }
    }
}

#[test]
fn operators_cannot_supply_an_expected_version_flag() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--delete", FACT]);
        refused(
            tier,
            &["--delete", FACT, "--expected-version", "1", "--dry-run"],
            "unexpected argument",
        );
    }
}

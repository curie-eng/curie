//! Schema compatibility gate for `curie cluster rollback` (#2296).
//!
//! The v0.8.5 -> v0.8.4 incident: Helm marked 0.8.4 `superseded` (status-safe
//! under #1899) while the live database sat at Alembic revision 0039, which
//! 0.8.4's migrate init container does not know. This file pins the additional
//! pre-mutation gate. Status filtering stays in `cluster_rollback.rs`.

use crate::support::executable as test_executable;

use std::fs;
use std::path::Path;
use std::process::Output;

use curie::exit::classify;
use curie::ops::{
    parse_helm_history, rollback, select_rollback_revision, ClusterRollbackOutput, CommonOpts,
    HelmRevision, RollbackOpts,
};
use curie::schema_compat::{pending_revisions, plan_upgrade, TargetMetadata};
use curie::schema_window::{live_in_window, newest_fail_forward, window_for};

fn catalog_marks_artifact_identity_ambiguous(version: &str) -> bool {
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("../src/application_schema_windows.json"))
            .expect("application schema catalog parses");
    catalog["windows"][version]["artifact_identity_ambiguous"]
        .as_bool()
        .unwrap_or(false)
}

/// Keep each release's catalog window and accepted boundaries in one table.
#[test]
fn release_catalog_windows_match_their_revision_boundaries() {
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("../src/application_schema_windows.json"))
            .expect("application schema catalog parses");

    type ReleaseCase<'a> = (
        &'a str,
        &'a str,
        &'a str,
        &'a [&'a str],
        &'a [&'a str],
        bool,
        bool,
    );
    let cases: &[ReleaseCase<'_>] = &[
        ("0.8.7", "0001", "0039", &["0039"], &["0040"], false, false),
        ("0.8.8", "0001", "0039", &["0039"], &["0044"], true, false),
        ("0.8.9", "0001", "0039", &["0039"], &["0044"], true, false),
        (
            "0.9.0",
            "0001",
            "0044",
            &["0039", "0044"],
            &["0045"],
            false,
            false,
        ),
        (
            "0.9.1",
            "0001",
            "0044",
            &["0039", "0044"],
            &["0045"],
            false,
            false,
        ),
        (
            "0.9.2",
            "0045",
            "0045",
            &["0045"],
            &["0044", "0046"],
            false,
            false,
        ),
        (
            "0.10.0-rc.1",
            "0045",
            "0057",
            &["0045", "0056", "0057"],
            &["0044"],
            false,
            true,
        ),
        (
            "0.10.0-rc.2",
            "0045",
            "0057",
            &["0045", "0056", "0057"],
            &["0044"],
            false,
            true,
        ),
        (
            "0.12.3",
            "0076",
            "0093",
            &["0076", "0081", "0091", "0092", "0093"],
            &["0082", "0091a", "0098", "0099"],
            false,
            false,
        ),
        (
            "0.13.0",
            "0108",
            "0108",
            &["0108"],
            &[
                "0081", "0091", "0092", "0093", "0082", "0091a", "0092a", "0093a", "0098", "0099",
                "0100",
            ],
            false,
            false,
        ),
    ];

    for (version, schema_min, schema_head, accepted, refused, ambiguous, prefixed_alias) in cases {
        let window = window_for(version).unwrap_or_else(|| panic!("{version} is catalogued"));
        assert_eq!(window.schema_min, *schema_min, "{version}");
        assert_eq!(window.schema_head, *schema_head, "{version}");
        for revision in *accepted {
            assert!(
                live_in_window(revision, &window),
                "{version} accepts {revision}"
            );
        }
        for revision in *refused {
            assert!(
                !live_in_window(revision, &window),
                "{version} refuses {revision}"
            );
        }
        assert_eq!(
            catalog_marks_artifact_identity_ambiguous(version),
            *ambiguous,
            "{version} artifact identity ambiguity"
        );
        if *prefixed_alias {
            assert!(catalog["windows"].get(*version).is_some());
            assert_eq!(
                window_for(&format!("v{version}"))
                    .expect("prefixed release candidate is catalogued")
                    .schema_head,
                window.schema_head,
                "{version} prefixed alias"
            );
        }
    }
}

#[test]
fn stable_v0100_sorts_after_its_release_candidate_for_fail_forward() {
    assert_eq!(
        newest_fail_forward(["0.10.0-rc.1"], "0057").as_deref(),
        Some("0.10.0-rc.1")
    );
    assert_eq!(
        newest_fail_forward(["0.10.0-rc.1", "0.10.0"], "0057").as_deref(),
        Some("0.10.0")
    );
    assert_eq!(
        newest_fail_forward(["0.10.0-rc.1", "0.10.0-rc.2"], "0057").as_deref(),
        Some("0.10.0-rc.2")
    );
}

/// @spec PROTECTED-HOOK-SOURCE-1
// @spec PROTECTED-HOOK-SOURCE-10
// @spec DEPLOY-NOTICE-RELEASE-1.
/// Released 0.9.1 reports catalog head 0044. The packaged chart applies
/// expansions through 0059, contracts 0060 and 0063, feature-train expansions
/// through 0069, route-identity contract 0070, then memory-writes expansion
/// 0071, work item base expansion 0072, polling cursor expansion 0073, hook
/// source policy/operation expansions 0075/0076, deploy notice expansion
/// 0082, provider-installations/channel-identities expansion 0083, channel
/// canvas edits expansion 0084, action executions expansion 0085, OIDC login
/// expansion 0100, tenant-scope expansion 0101, reply target expansion 0102, the
/// tracker identity contract 0103, trigger label expansion 0105, and identity
/// links expansion 0107 and train join 0108, then team sync state 0120 and
/// agent access policies 0121, directory identity 0122 and the attachment
/// catalog 0130, so the upgrade is forward-only.
#[test]
fn v091_source_upgrades_through_the_packaged_chart_graph() {
    // @spec HTTP-PRINCIPAL-ADOPTION-V1-13: candidate graph keeps published windows unchanged.
    // @spec AGENT-ROLE-PERSISTENCE-V1-9: role configuration 0133 precedes the head.
    // @spec HTTP-APPROVAL-CONTINUITY-5: approval provenance 0137 precedes the head.
    // @spec AUTOMATED-REMEDIATION-15: 0138 only widens a CHECK.
    // @spec HTTP-INVOCATION-EXECUTION-21: Grid namespaces 0139 precedes factory base expansion 0143.
    let source = window_for("0.9.1").expect("0.9.1 is catalogued");
    let target: TargetMetadata =
        serde_json::from_str(include_str!("../../charts/curie/files/schema-compat.json"))
            .expect("packaged chart schema compatibility metadata parses");

    assert_eq!(source.schema_head, "0044");

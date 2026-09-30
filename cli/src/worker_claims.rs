//! Bounded, read-only observation of the worker claim gate.
//!
//! The worker owns the marker and its wire shape. The CLI executes the worker's
//! status mode in the selected runtime, accepts only that narrow schema, and
//! turns every selection, process, timeout, or parse failure into `Unknown`.
//! External command output is never carried into an operator report.

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use time::{format_description::well_known::Rfc3339, OffsetDateTime, UtcOffset};
use tokio::time::Instant;

use crate::ops::{plain, run_capture, OpsCommand};

const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(10);
const COMPOSE_WORKER_SERVICE: &str = "curie-worker";

/// The pre-#3127 status invocation, kept as its own constant because a mixed
/// version worker (older image than this CLI) refuses an unrecognized
/// `--with-ttl` flag with a non-zero argparse exit. Callers retry with this
/// once, within the same deadline, when the primary `STATUS_ARGS` exec ran
/// but did not succeed.
const LEGACY_STATUS_ARGS: [&str; 6] = [
    "python",
    "-m",
    "curie_worker.upgrade_drain",
    "--mode",
    "status",
    "--json",
];

const STATUS_ARGS: [&str; 7] = [
    "python",
    "-m",
    "curie_worker.upgrade_drain",
    "--mode",
    "status",
    "--json",
    "--with-ttl",
];

/// The only claim-gate states an operator surface may act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimsState {
    ClaimsEnabled,
    Quiescing {
        since: String,
        revision: u64,
        ttl_seconds: Option<u64>,
    },
    QuiescingMetadataUnavailable {
        ttl_seconds: Option<u64>,
    },
    Unknown,
}

impl ClaimsState {
    /// Authored reason shared by doctor and message. Unknown state has no
    /// proven marker to report.
    pub(crate) fn wait_reason(&self) -> Option<String> {
        match self {
            Self::Quiescing {
                since,
                revision,
                ttl_seconds,
            } => {
                let mut reason = format!("waiting for upgrade revision {revision} since {since}");
                if let Some(ttl_seconds) = ttl_seconds {
                    reason.push_str(&format!("; marker expires in {ttl_seconds}s"));
                }
                Some(reason)
            }
            Self::QuiescingMetadataUnavailable { ttl_seconds } => {
                let mut reason = "waiting for upgrade; marker metadata unavailable".to_string();
                if let Some(ttl_seconds) = ttl_seconds {
                    reason.push_str(&format!("; marker expires in {ttl_seconds}s"));
                }
                Some(reason)
            }
            Self::ClaimsEnabled | Self::Unknown => None,
        }
    }

    /// Read-only diagnosis used by status surfaces.
    pub(crate) fn status_diagnosis(&self) -> String {
        match self {
            Self::ClaimsEnabled => "worker claims enabled".to_string(),
            Self::Quiescing { .. } => format!(
                "worker {}",
                self.wait_reason()
                    .expect("a quiescing claim state has a wait reason")
            ),
            Self::QuiescingMetadataUnavailable { ttl_seconds } => {
                let mut detail =
                    "worker quiescing for upgrade; marker metadata unavailable".to_string();
                if let Some(ttl_seconds) = ttl_seconds {
                    detail.push_str(&format!("; marker expires in {ttl_seconds}s"));
                }
                detail
            }
            Self::Unknown => "worker claim state unknown".to_string(),
        }
    }
}

/// Cluster observation retains the selected pod only so `cluster status` can
/// annotate the matching existing row. The pod name is never inferred from an
/// image or resource name pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterObservation {
    pub(crate) state: ClaimsState,
    pub(crate) worker_pod: Option<String>,
}

impl ClusterObservation {
    fn unknown() -> Self {
        Self {
            state: ClaimsState::Unknown,
            worker_pod: None,
        }
    }
}

/// A selected cluster target carrying the original observation deadline into
/// the dependent exec stage.
pub(crate) struct ClusterProbe {
    namespace: String,
    worker_pod: Option<String>,
    deadline: Instant,
}

impl ClusterProbe {
    pub(crate) fn selected_pod(&self) -> Option<&str> {
        self.worker_pod.as_deref()
    }

    /// Execute the worker reader with only the time left from selection. This
    /// method is separate so callers can overlap exec with their other
    /// dependent reads without resetting the observer budget.
    pub(crate) async fn observe(self) -> ClusterObservation {
        let Some(pod) = self.worker_pod else {
            return ClusterObservation::unknown();
        };
        let observation = async {
            let (mut executed, mut stdout, _) =
                run_capture(&cluster_exec_command(&self.namespace, &pod, &STATUS_ARGS))
                    .await
                    .ok()?;
            if !executed {
                // The exec process ran but exited non-zero: a mixed version
                // worker's argparse rejects an unrecognized `--with-ttl`.
                // Retry once with the pre-#3127 args, within the same
                // deadline, before giving up on this pod (#3127).
                (executed, stdout, _) = run_capture(&cluster_exec_command(
                    &self.namespace,
                    &pod,
                    &LEGACY_STATUS_ARGS,
                ))
                .await
                .ok()?;
            }
            if !executed {
                return None;
            }
            Some(ClusterObservation {
                state: parse_status(&stdout),
                worker_pod: Some(pod),
            })
        };

        tokio::time::timeout_at(self.deadline, observation)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(ClusterObservation::unknown)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusDocument {
    state: String,
    since: Value,
    revision: Value,
    #[serde(default)]
    ttl_seconds: Value,
}

/// `null`/absent -> no TTL claim. Any non-negative integer is a valid TTL.
/// Anything else (negative, fractional, string, bool) is unparseable and the
/// caller must fall back to `Unknown`.
fn parse_ttl_seconds(value: &Value) -> Result<Option<u64>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    value.as_u64().map(Some).ok_or(())
}

fn valid_utc_rfc3339(value: &str) -> bool {
    // This also prevents a syntactically valid prefix followed by terminal
    // control data from becoming an authored operator line.
    if value.len() > 64 || value.chars().any(char::is_control) {
        return false;
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|timestamp| timestamp.offset() == UtcOffset::UTC)
        .unwrap_or(false)
}

fn parse_status(stdout: &str) -> ClaimsState {
    let Ok(document) = serde_json::from_str::<StatusDocument>(stdout) else {
        return ClaimsState::Unknown;
    };

    let Ok(ttl_seconds) = parse_ttl_seconds(&document.ttl_seconds) else {
        return ClaimsState::Unknown;
    };

    match document.state.as_str() {
        "claims_enabled" if document.since.is_null() && document.revision.is_null() => {
            ClaimsState::ClaimsEnabled
        }
        "unknown" if document.since.is_null() && document.revision.is_null() => {
            ClaimsState::Unknown
        }
        "quiescing" if document.since.is_null() && document.revision.is_null() => {
            ClaimsState::QuiescingMetadataUnavailable { ttl_seconds }
        }
        "quiescing" => {
            let Some(since) = document.since.as_str() else {
                return ClaimsState::Unknown;
            };
            let Some(revision) = document.revision.as_u64().filter(|revision| *revision > 0) else {
                return ClaimsState::Unknown;
            };
            if !valid_utc_rfc3339(since) {
                return ClaimsState::Unknown;
            }
            ClaimsState::Quiescing {
                since: since.to_string(),
                revision,
                ttl_seconds,
            }
        }
        _ => ClaimsState::Unknown,
    }
}

fn running_worker_pod(stdout: &str, release: &str) -> Option<String> {
    let document = serde_json::from_str::<Value>(stdout).ok()?;
    let items = document.get("items")?.as_array()?;
    let mut matches: Vec<String> = items
        .iter()
        .filter_map(|pod| {
            let metadata = pod.get("metadata")?;
            let labels = metadata.get("labels")?.as_object()?;
            let correct_release = labels
                .get("app.kubernetes.io/instance")
                .and_then(Value::as_str)
                == Some(release);
            let worker_component = labels
                .get("app.kubernetes.io/component")
                .and_then(Value::as_str)
                == Some("worker");
            let running = pod.pointer("/status/phase").and_then(Value::as_str) == Some("Running");
            let terminating = metadata
                .get("deletionTimestamp")
                .is_some_and(|timestamp| !timestamp.is_null());
            if !correct_release || !worker_component || !running || terminating {
                return None;
            }
            metadata
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
        })
        .collect();
    matches.sort_unstable();
    matches.into_iter().next()
}

fn cluster_selection_command(namespace: &str, release: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("pods"),
            plain("-n"),
            plain(namespace),
            plain("-l"),
            plain(crate::ops::worker_deployment_selector(release)),
            plain("-o"),
            plain("json"),
        ],
    )
}

fn cluster_exec_command(namespace: &str, pod: &str, status_args: &[&str]) -> OpsCommand {
    let mut args = vec![
        plain("exec"),
        plain("-n"),
        plain(namespace),
        plain(pod),
        plain("--"),
    ];
    args.extend(status_args.iter().copied().map(plain));
    OpsCommand::new("kubectl", args)
}

fn local_exec_command(project: &str, compose_files: &[String], status_args: &[&str]) -> OpsCommand {
    let mut args = vec![plain("compose"), plain("-p"), plain(project)];
    let files = if compose_files.is_empty() {
        vec![crate::local::DEFAULT_COMPOSE_FILE.to_string()]
    } else {
        compose_files.to_vec()
    };
    for file in files {
        args.extend([plain("-f"), plain(file)]);
    }
    args.extend([plain("exec"), plain("-T"), plain(COMPOSE_WORKER_SERVICE)]);
    args.extend(status_args.iter().copied().map(plain));
    OpsCommand::new("docker", args)
}

/// Select one Running worker from this exact release. The returned handle owns
/// the absolute deadline that also bounds its later exec.
pub(crate) async fn select_cluster(namespace: &str, release: &str) -> ClusterProbe {
    let deadline = Instant::now() + OBSERVATION_TIMEOUT;
    let selection = async {
        let (selected, stdout, _) = run_capture(&cluster_selection_command(namespace, release))
            .await
            .ok()?;
        if !selected {
            return None;
        }
        running_worker_pod(&stdout, release)
    };
    let worker_pod = tokio::time::timeout_at(deadline, selection)
        .await
        .ok()
        .flatten();
    ClusterProbe {
        namespace: namespace.to_string(),
        worker_pod,
        deadline,
    }
}

/// Complete a cluster observation without exposing its two stages to callers
/// that have no independent work to overlap.
pub(crate) async fn observe_cluster(namespace: &str, release: &str) -> ClusterObservation {
    select_cluster(namespace, release).await.observe().await
}

/// Execute status in the fixed local Compose project and worker service. The
/// entire child lifetime is covered by one deadline.
pub(crate) async fn observe_local(project: &str, compose_files: &[String]) -> ClaimsState {
    let observation = async {
        let (mut executed, mut stdout, _) =
            run_capture(&local_exec_command(project, compose_files, &STATUS_ARGS))
                .await
                .ok()?;
        if !executed {
            // Same mixed-version retry as the cluster path: an older worker
            // rejects `--with-ttl` (#3127).
            (executed, stdout, _) = run_capture(&local_exec_command(
                project,
                compose_files,
                &LEGACY_STATUS_ARGS,
            ))
            .await
            .ok()?;
        }
        executed.then(|| parse_status(&stdout))
    };

    tokio::time::timeout(OBSERVATION_TIMEOUT, observation)
        .await
        .ok()
        .flatten()
        .unwrap_or(ClaimsState::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SINCE: &str = "2026-09-25T10:00:00+00:00";

    #[test]
    fn status_args_ask_the_worker_for_ttl() {
        assert_eq!(
            STATUS_ARGS,
            [
                "python",
                "-m",
                "curie_worker.upgrade_drain",
                "--mode",
                "status",
                "--json",
                "--with-ttl",
            ]
        );
    }

    #[test]
    fn legacy_status_args_omit_with_ttl_and_are_a_prefix_of_status_args() {
        assert_eq!(
            LEGACY_STATUS_ARGS,
            [
                "python",
                "-m",
                "curie_worker.upgrade_drain",
                "--mode",
                "status",
                "--json",
            ]
        );
        assert_eq!(
            &STATUS_ARGS[..LEGACY_STATUS_ARGS.len()],
            LEGACY_STATUS_ARGS.as_slice()
        );
        assert_eq!(STATUS_ARGS.len(), LEGACY_STATUS_ARGS.len() + 1);
        assert_eq!(STATUS_ARGS[LEGACY_STATUS_ARGS.len()], "--with-ttl");
    }

    #[test]
    fn a_quiescing_document_carries_the_remaining_marker_ttl() {
        let state = parse_status(&format!(
            r#"{{"state":"quiescing","since":"{SINCE}","revision":7,"ttl_seconds":120}}"#
        ));
        assert_eq!(
            state,
            ClaimsState::Quiescing {
                since: SINCE.to_string(),
                revision: 7,
                ttl_seconds: Some(120),
            }
        );
        assert_eq!(
            state.wait_reason().as_deref(),
            Some(
                "waiting for upgrade revision 7 since 2026-09-25T10:00:00+00:00; \
                 marker expires in 120s"
            )
        );
        assert!(state.status_diagnosis().contains("marker expires in 120s"));
    }

    #[test]
    fn an_old_worker_document_without_ttl_still_parses() {
        let state = parse_status(&format!(
            r#"{{"state":"quiescing","since":"{SINCE}","revision":7}}"#
        ));
        assert_eq!(
            state,
            ClaimsState::Quiescing {
                since: SINCE.to_string(),
                revision: 7,
                ttl_seconds: None,
            }
        );
        let reason = state.wait_reason().unwrap();
        assert_eq!(
            reason,
            "waiting for upgrade revision 7 since 2026-09-25T10:00:00+00:00"
        );
        assert_eq!(
            parse_status(r#"{"state":"quiescing","since":null,"revision":null}"#),
            ClaimsState::QuiescingMetadataUnavailable { ttl_seconds: None }
        );
        assert_eq!(
            parse_status(r#"{"state":"claims_enabled","since":null,"revision":null}"#),
            ClaimsState::ClaimsEnabled
        );
    }

    #[test]
    fn metadata_unavailable_quiescing_reports_the_ttl() {
        let state =
            parse_status(r#"{"state":"quiescing","since":null,"revision":null,"ttl_seconds":45}"#);
        assert_eq!(
            state,
            ClaimsState::QuiescingMetadataUnavailable {
                ttl_seconds: Some(45)
            }
        );
        assert!(state
            .wait_reason()
            .unwrap()
            .contains("marker expires in 45s"));
    }

    #[test]
    fn a_negative_or_non_integer_ttl_is_unknown() {
        for ttl in ["-1", "1.5", "\"120\"", "true"] {
            let doc = format!(
                r#"{{"state":"quiescing","since":"{SINCE}","revision":7,"ttl_seconds":{ttl}}}"#
            );
            assert_eq!(parse_status(&doc), ClaimsState::Unknown, "ttl {ttl}");
            let doc = format!(
                r#"{{"state":"quiescing","since":null,"revision":null,"ttl_seconds":{ttl}}}"#
            );
            assert_eq!(parse_status(&doc), ClaimsState::Unknown, "ttl {ttl}");
        }
    }
}

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

const STATUS_ARGS: [&str; 6] = [
    "python",
    "-m",
    "curie_worker.upgrade_drain",
    "--mode",
    "status",
    "--json",
];

/// The only claim-gate states an operator surface may act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimsState {
    ClaimsEnabled,
    Quiescing { since: String, revision: u64 },
    QuiescingMetadataUnavailable,
    Unknown,
}

impl ClaimsState {
    /// Authored reason shared by doctor and message. Unknown state has no
    /// proven marker to report.
    pub(crate) fn wait_reason(&self) -> Option<String> {
        match self {
            Self::Quiescing { since, revision } => Some(format!(
                "waiting for upgrade revision {revision} since {since}"
            )),
            Self::QuiescingMetadataUnavailable => {
                Some("waiting for upgrade; marker metadata unavailable".to_string())
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
            Self::QuiescingMetadataUnavailable => {
                "worker quiescing for upgrade; marker metadata unavailable".to_string()
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
            let (executed, stdout, _) = run_capture(&cluster_exec_command(&self.namespace, &pod))
                .await
                .ok()?;
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

    match document.state.as_str() {
        "claims_enabled" if document.since.is_null() && document.revision.is_null() => {
            ClaimsState::ClaimsEnabled
        }
        "unknown" if document.since.is_null() && document.revision.is_null() => {
            ClaimsState::Unknown
        }
        "quiescing" if document.since.is_null() && document.revision.is_null() => {
            ClaimsState::QuiescingMetadataUnavailable
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

fn cluster_exec_command(namespace: &str, pod: &str) -> OpsCommand {
    let mut args = vec![
        plain("exec"),
        plain("-n"),
        plain(namespace),
        plain(pod),
        plain("--"),
    ];
    args.extend(STATUS_ARGS.into_iter().map(plain));
    OpsCommand::new("kubectl", args)
}

fn local_exec_command(project: &str, compose_file: &str) -> OpsCommand {
    let mut args = vec![
        plain("compose"),
        plain("-p"),
        plain(project),
        plain("-f"),
        plain(compose_file),
        plain("exec"),
        plain("-T"),
        plain(COMPOSE_WORKER_SERVICE),
    ];
    args.extend(STATUS_ARGS.into_iter().map(plain));
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
pub(crate) async fn observe_local(project: &str, compose_file: &str) -> ClaimsState {
    let observation = async {
        let (executed, stdout, _) = run_capture(&local_exec_command(project, compose_file))
            .await
            .ok()?;
        executed.then(|| parse_status(&stdout))
    };

    tokio::time::timeout(OBSERVATION_TIMEOUT, observation)
        .await
        .ok()
        .flatten()
        .unwrap_or(ClaimsState::Unknown)
}

//! Command handlers behind the `curie` subcommands.
//!
//! The binary's `args` modules own the clap surface and `dispatch` routes each
//! parsed command here. Each submodule owns one command group and speaks only
//! through the library modules (docker, runner, api, scaffold, state, evals,
//! render); this module re-exports them so callers name `commands::<handler>`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::ValueEnum;
use curie_aci_protocol::{Budget, EventType, OutboundEvent, SessionStatus};
use serde::{Deserialize, Serialize};

use crate::api::{ApiClient, ChannelOutcome, RoutingCheck};
use crate::bundle::{git_status_is_clean_for_pack, pack_tar_gz};
use crate::docker::{self, CheckSpec, StartSpec};
use crate::evals::{
    eval_author, graded_answer, load_eval, outcome_label, rollup_line, score_turn, turn_completed,
    CaseOutcome, EvalSuite, TrajectoryScorer,
};
use crate::render::{boxed_summary, status_str, TurnPart, TurnPrinter};
use crate::runner::RunnerClient;
use crate::scaffold::{
    derive_plugin_name, read_declared_secrets, read_manifest, refuse_unowned_nonempty_dir,
    scaffold, scaffold_from_spec,
};
use crate::state::{self, RunnerState};

mod actions;
mod agent_actions;
mod agents;
mod approvals;
mod channels;
mod connector_images;
mod credential_env;
mod deploy;
mod dev;
mod eval;
mod init;
mod overrides;
mod skill;
mod skill_approvals;
mod skill_unavailable;
mod work;

pub use actions::*;
pub use agent_actions::*;
pub use agents::*;
pub use approvals::*;
pub use channels::*;
pub use connector_images::*;
pub use credential_env::*;
pub use deploy::*;
pub use dev::*;
pub use eval::*;
pub use init::*;
pub use overrides::*;
pub use skill::*;
pub use skill_approvals::*;
pub use skill_unavailable::*;
pub use work::*;

pub const DEFAULT_PORT: u16 = 7245; // the design canon's local bot port
pub const DEFAULT_BUDGET: &str = r#"{"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#;
pub const DEFAULT_LOCAL_MODEL: &str = "qwen3:4b";
pub const DEFAULT_OLLAMA_IMAGE: &str = "ollama/ollama:0.24.0";
pub const OLLAMA_PORT: u16 = 11434;

#[derive(Clone, Copy, ValueEnum)]
pub enum SendType {
    Message,
    Job,
    EvalCase,
}

impl From<SendType> for EventType {
    fn from(value: SendType) -> Self {
        match value {
            SendType::Message => EventType::Message,
            SendType::Job => EventType::Job,
            SendType::EvalCase => EventType::EvalCase,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
pub enum DeployEnv {
    Dev,
    Prod,
}

impl DeployEnv {
    pub fn as_str(self) -> &'static str {
        match self {
            DeployEnv::Dev => "dev",
            DeployEnv::Prod => "prod",
        }
    }
}

/// `local observability`: print the local platform's observability surfaces --
/// the Curie Console, the Langfuse UI, and the API base -- resolved through the
/// shared tier-aware endpoint seam (`crate::observability`).
///
/// `ObservabilityOutput` moved to `crate::observability` (#460) so both tiers
/// return one type; the hardcoded URL array that used to live here is replaced
/// by `observability::local_endpoints()`, whose consts `local.rs::ENDPOINTS`
/// also references (one source of truth for the port literals).
///
/// Agent-first: a browser is opened only when the human passes `--open`, and
/// never under `--json` (gated by `observability::should_open`). A missing
/// opener (headless/CI) is not an error -- the URLs are printed either way.
pub async fn observability(open: bool) -> Result<crate::observability::ObservabilityOutput> {
    let ui = crate::ui::ui();
    let surfaces = crate::observability::local_endpoints();
    crate::observability::open_endpoints(&surfaces, open, ui.json()).await;
    // A hint, not payload: `observability` never checks whether the stack is
    // up, so this is stderr guidance rather than a claim about what happened.
    ui.note("start these surfaces with `curie local up` if they are unreachable");
    Ok(crate::observability::ObservabilityOutput::Surfaces(
        surfaces,
    ))
}

/// Reject a channel binding this CLI can cheaply prove is wrong before a round
/// trip. Dispatches on `kind`, mirroring the API's kind-dispatched
/// `validate_channel_binding`: a kind with no local rule passes here and is
/// answered authoritatively by the API.
///
/// The `slack` arm rejects a `#name` rather than a channel ID: real Slack
/// events carry the channel **ID** (e.g. `C0EXAMPLE1`), and the worker's
/// binding resolver matches on that ID, so a `#name` value is stored verbatim
/// and never routes -- a silently dead binding. Fail the deploy up front
/// instead.
fn validate_channel_binding(kind: &str, address: &str) -> Result<()> {
    if channel_binding_never_resolves(kind, address) {
        return Err(crate::exit::usage(format!(
            "slack channel {address:?} is a name, not an ID: real Slack events carry the \
             channel ID (e.g. C0EXAMPLE1) and the worker routes on it, so a #name binding \
             never receives messages. Pass the channel ID instead -- find it in the \
             channel's About tab, or the channel URL (.../archives/C0EXAMPLE1)."
        )));
    }
    Ok(())
}

fn resolve_url(explicit: Option<String>) -> Result<String> {
    if let Some(url) = explicit {
        return Ok(url);
    }
    if let Some(saved) = state::load(Path::new("."))? {
        return Ok(saved.base_url);
    }
    Ok(format!("http://localhost:{DEFAULT_PORT}"))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_secs()
}

/// Short git SHA of the plugin dir's checkout, for the version line.
async fn git_short_sha(dir: &Path) -> Option<String> {
    let output = tokio::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(dir)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!sha.is_empty()).then_some(sha)
}

// --- skill tier parity (issue #459) -----------------------------------------

#[cfg(test)]
mod tests;

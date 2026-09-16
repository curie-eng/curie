//! Tier-aware observability endpoint seam (issue #460).
//!
//! One shared [`Endpoint`] value type and one [`ObservabilityOutput`] that both
//! tiers return: `commands::observability` (local) and `ops::observability`
//! (cluster) each resolve their surfaces and hand the output back to `main.rs`,
//! which `emit()`s it. The json-vs-human choice is made once, in `Ui::emit`
//! (#456) -- this module never bypasses `CliOutput`.
//!
//! This module is a **leaf**: it must never import `ops` or `local`. The cluster
//! resolver needs ops-private kubectl helpers, so it lives in `ops.rs` and
//! yields these same `Endpoint` values.

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::api::{ApiClient, MetricSeries, MetricsSummary, TraceListRow, TraceTree};
use crate::ui::{CliOutput, Ui};

/// A single observability surface.
///
/// The wire key is `name` (not `label`) and the payload key is `surfaces`: this
/// is an **additive superset** of the shipped `{"surfaces":[{"name","url"}]}`
/// contract (PR#503/#456) that agents already consume. `note` and `browsable`
/// are the new fields.
///
/// `url` carries a real, parseable URL when one exists; `note` carries a hint or
/// degraded/error message. They are never overloaded onto one field -- an agent
/// consuming `--json` must be able to parse `url` as a URL, so a degraded
/// endpoint sets `url: None` and puts its message in `note`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub name: String,
    pub url: Option<String>,
    pub note: Option<String>,
    pub browsable: bool,
}

/// Output of `<tier> observability`: the observability surfaces to print, or the
/// `--dry-run` plan of the discovery commands the cluster tier would run. The
/// browser-open side effect happens in the handler (gated by [`should_open`]);
/// this type only carries what `emit` renders.
///
/// An enum rather than a struct because `--dry-run` needs a home: this matches
/// the house pattern of `VersionsOutput`/`KillOutput`, whose dry-run variant
/// delegates to the shared `DryRunPlan` rather than re-deriving its JSON. Only
/// the cluster tier has a `--dry-run` (it shells kubectl); `local observability`
/// is network-free and always yields `Surfaces`.
pub enum ObservabilityOutput {
    /// The `cluster observability --dry-run` plan: `{"dry_run":true,"plan":[..]}`.
    DryRun(crate::ui::DryRunPlan),
    /// The resolved observability surfaces.
    Surfaces(Vec<Endpoint>),
}

impl crate::ui::CliOutput for ObservabilityOutput {
    /// `{"surfaces":[{"name","url":<string|null>,"note":<string|null>,"browsable":bool}]}`,
    /// or the `DryRunPlan`'s own `{"dry_run":true,"plan":[..]}`.
    ///
    /// All four surface keys are ALWAYS emitted, with explicit nulls -- never
    /// conditionally omitted. That is the repo convention pinned by
    /// `kill_output_json_shape_is_pinned` ("the false case must emit
    /// `killed: false`, not omit the key").
    fn to_json(&self) -> Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Surfaces(surfaces) => {
                let rows: Vec<Value> = surfaces
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "name": e.name,
                            "url": e.url,
                            "note": e.note,
                            "browsable": e.browsable,
                        })
                    })
                    .collect();
                serde_json::json!({ "surfaces": rows })
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Surfaces(surfaces) => {
                for e in surfaces {
                    // A row carries a url or a note, never both meanings in one
                    // field. URLs keep the PR#34 `ui.url` styling.
                    match (&e.url, &e.note) {
                        (Some(url), _) => ui.kv(&e.name, &ui.url(url)),
                        (None, Some(note)) => ui.kv(&e.name, note),
                        (None, None) => ui.kv(&e.name, "unavailable"),
                    }
                }
            }
        }
    }
}

/// One bounded newest-first trace-list result. `count` is the number actually
/// returned after defensive client-side truncation, not a backend total.
#[derive(Debug, Serialize)]
pub struct ObservabilityRunsOutput {
    pub limit: usize,
    pub count: usize,
    pub runs: Vec<TraceListRow>,
}

impl CliOutput for ObservabilityRunsOutput {
    fn to_json(&self) -> Value {
        serde_json::to_value(self)
            .unwrap_or_else(|_| serde_json::json!({"limit": self.limit, "count": 0, "runs": []}))
    }

    fn render(&self, ui: &Ui) {
        if self.runs.is_empty() {
            ui.payload("No runs found.");
            return;
        }
        for row in &self.runs {
            ui.kv(
                &row.id,
                &format!(
                    "{}  {}",
                    row.timestamp,
                    row.name.as_deref().unwrap_or("(unnamed trace)")
                ),
            );
        }
    }
}

/// One complete API `TraceTree`, serialized wholesale so correlation fields
/// and recursively typed observation nodes cannot be projected away.
pub struct ObservabilityRunOutput(pub TraceTree);

impl CliOutput for ObservabilityRunOutput {
    fn to_json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn render(&self, ui: &Ui) {
        let rendered = serde_json::to_string_pretty(&self.0).unwrap_or_else(|_| "{}".to_string());
        ui.payload_plain(&rendered);
    }
}

/// Metrics queries return either of the existing API DTOs directly. The enum
/// gives `Ui::emit` one typed result while retaining the response's direct JSON
/// shape (there is no CLI wrapper or projection around either DTO).
pub enum ObservabilityMetricsOutput {
    Summary(MetricsSummary),
    Series(MetricSeries),
}

impl CliOutput for ObservabilityMetricsOutput {
    fn to_json(&self) -> Value {
        match self {
            Self::Summary(summary) => {
                serde_json::to_value(summary).unwrap_or_else(|_| serde_json::json!({}))
            }
            Self::Series(series) => {
                serde_json::to_value(series).unwrap_or_else(|_| serde_json::json!({}))
            }
        }
    }

    fn render(&self, ui: &Ui) {
        match self {
            Self::Summary(summary) => {
                ui.kv("window", &format!("{} to {}", summary.start, summary.end));
                ui.kv("runs", &summary.runs.to_string());
                ui.kv("latency p95", &format!("{:.2} ms", summary.latency_p95_ms));
                ui.kv("tokens", &summary.tokens.to_string());
                let cost = if summary.cost_known {
                    format!("${:.6}", summary.cost_usd)
                } else {
                    "unknown (model pricing unavailable)".to_string()
                };
                ui.kv("cost", &cost);
                ui.kv("error rate", &format!("{:.2}%", summary.error_rate * 100.0));
            }
            Self::Series(series) => {
                ui.kv("metric", &series.metric);
                ui.kv("granularity", &series.granularity);
                ui.kv("window", &format!("{} to {}", series.start, series.end));
                for point in &series.points {
                    ui.kv(&point.ts, &point.value.to_string());
                }
            }
        }
    }
}

/// Shared local/cluster query request. Tier-specific connection discovery is
/// completed in `main.rs`; from here onward both siblings execute this one API
/// client path.
pub enum ObservabilityQuery {
    Runs {
        limit: usize,
        agent_id: Option<String>,
    },
    Run {
        trace_id: String,
    },
    Metrics {
        metric: Option<String>,
        granularity: String,
        start: Option<String>,
        end: Option<String>,
        environment: Option<String>,
        agent: Option<String>,
    },
}

/// Validate a present-but-empty agent filter value (#1948). An empty string is
/// not an absent one: forwarded verbatim it reaches FastAPI as a falsey query
/// value the API treats as no filter at all, so a "scoped" export silently
/// returns every agent's data. A whitespace-only value is the same refusal.
/// A non-empty value passes through VERBATIM: agent names are exact-match
/// identifiers and the API intentionally accepts names with leading or
/// trailing whitespace, so trimming here would resolve a different agent.
/// `accepted` names the value forms the flag takes, so the refusal's fix stays
/// complete per flag.
fn non_empty_filter(raw: Option<&str>, flag: &str, accepted: &str) -> Result<Option<String>> {
    match raw {
        None => Ok(None),
        Some(value) => {
            if value.trim().is_empty() {
                return Err(anyhow::Error::from(
                    crate::exit::CliError::usage(format!(
                        "--{flag} was given an empty value; a present-but-empty filter would \
                         drop the scope and query every agent"
                    ))
                    .with_fix(format!(
                        "--{flag} accepts {accepted}; pass a non-empty value or omit \
                         --{flag} entirely"
                    )),
                ));
            }
            Ok(Some(value.to_string()))
        }
    }
}

/// Execute one read-only observability query through the Curie platform API.
/// The returned trait object still flows through the single `Ui::emit` success
/// decision in `main.rs`; this function never writes a success payload itself.
pub async fn query(
    tier: &str,
    api_url: &str,
    api_key: &str,
    query: ObservabilityQuery,
) -> Result<Box<dyn CliOutput>> {
    let client = ApiClient::new(api_url, api_key)?;
    let output: Box<dyn CliOutput> = match query {
        ObservabilityQuery::Runs { limit, agent_id } => {
            let agent_id =
                non_empty_filter(agent_id.as_deref(), "agent-id", "a platform agent id")?;
            let mut runs = client
                .list_observability_runs(limit, agent_id.as_deref())
                .await
                .map_err(|error| classify_api_error(error, tier))?;
            runs.truncate(limit);
            let count = runs.len();
            crate::ui::ui().note(&format!(
                "Inspect one result with `curie {tier} observability run <trace-id>`."
            ));
            Box::new(ObservabilityRunsOutput { limit, count, runs })
        }
        ObservabilityQuery::Run { trace_id } => {
            let run = client
                .observability_run(&trace_id)
                .await
                .map_err(|error| classify_api_error(error, tier))?
                .ok_or_else(|| {
                    crate::exit::CliError::failure(format!(
                        "observability trace {trace_id:?} was not found"
                    ))
                    .with_fix(format!(
                        "list recent traces with `curie {tier} observability runs --limit 20`"
                    ))
                })?;
            Box::new(ObservabilityRunOutput(run))
        }
        ObservabilityQuery::Metrics {
            metric,
            granularity,
            start,
            end,
            environment,
            agent,
        } => {
            // #1948: the API's `agent` filter is a trace-name contains token
            // `agent-<agent_id>` (apps/api/src/curie_api/metrics.py::
            // agent_trace_filter), because production traces are named
            // `curie-run:agent-<agent_id>-thread-<ts>`. Forwarding the
            // human-readable name matched nothing and read as false zeroes, so
            // the name is resolved to its agent record first, by name or by
            // id, reusing the deploy flow's one resolution path.
            let agent = non_empty_filter(
                agent.as_deref(),
                "agent",
                "a platform agent's name or agent id",
            )?;
            let agent_filter = match agent {
                None => None,
                Some(name) => {
                    let resolved =
                        client
                            .find_agent_observability(&name)
                            .await
                            .map_err(|error| {
                                // Only a genuine no-match is operator input error. A
                                // transport or availability failure keeps its own
                                // transient/failure class instead of masquerading as
                                // "no such agent" (#1948 review).
                                if crate::api::is_agent_lookup_not_found(&error) {
                                    anyhow::Error::from(
                                        crate::exit::CliError::usage(format!(
                                    "--agent {name:?} does not match any platform agent by \
                                     name or agent id"
                                ))
                                        .with_fix(format!(
                                    "--agent accepts a deployed agent's name or agent id; list \
                                     agents with `curie {tier} versions`"
                                )),
                                    )
                                } else {
                                    classify_api_error(error, tier)
                                }
                            })?;
                    Some(format!("agent-{}", resolved.id))
                }
            };
            match metric {
                Some(metric) => Box::new(ObservabilityMetricsOutput::Series(
                    client
                        .observability_metric_series(
                            &metric,
                            &granularity,
                            start.as_deref(),
                            end.as_deref(),
                            environment.as_deref(),
                            agent_filter.as_deref(),
                        )
                        .await
                        .map_err(|error| classify_api_error(error, tier))?,
                )),
                None => Box::new(ObservabilityMetricsOutput::Summary(
                    client
                        .observability_metrics_summary(
                            start.as_deref(),
                            end.as_deref(),
                            environment.as_deref(),
                            agent_filter.as_deref(),
                        )
                        .await
                        .map_err(|error| classify_api_error(error, tier))?,
                )),
            }
        }
    };
    Ok(output)
}

fn classify_api_error(error: anyhow::Error, tier: &str) -> anyhow::Error {
    if !crate::exit::is_transient_reqwest(&error)
        && !crate::api::is_observability_api_unavailable(&error)
    {
        let (class, fix) = crate::exit::classify(&error);
        if class != crate::exit::ExitClass::Failure || fix.is_some() {
            return error;
        }
        return anyhow::Error::from(
            crate::exit::CliError::failure(format!("{error:#}")).with_fix(format!(
                "verify the {tier} platform release exposes the current observability API DTOs"
            )),
        );
    }
    let fix = if tier == "local" {
        "start the local API with `curie local up`, or pass a reachable --api-url"
    } else {
        "verify the cluster API endpoint, or pass a reachable --api-url"
    };
    // Keep the tagged error as the anyhow chain head. `exit::classify` walks
    // concrete causes to recover `CliError`; attaching it as anyhow context
    // would instead leave a `ContextError` head and fall through to the generic
    // reqwest retry hint, losing this tier's actionable remediation.
    anyhow::Error::from(
        crate::exit::CliError::transient(format!(
            "the Curie API is unavailable for this query: {error:#}"
        ))
        .with_fix(fix),
    )
}

/// Local Curie Console (browsable). Port literal lives once, here;
/// `local.rs::ENDPOINTS` references it.
pub const LOCAL_CONSOLE_URL: &str = "http://localhost:28080/?api=1";
/// Local Langfuse UI (browsable).
pub const LOCAL_LANGFUSE_URL: &str = "http://localhost:23000";
/// Local platform API base (not browsable -- an agent target, not a webapp).
pub const LOCAL_API_URL: &str = "http://localhost:28000";

/// The local tier's three observability surfaces: Console + Langfuse
/// (browsable) and the API base (not browsable).
pub fn local_endpoints() -> Vec<Endpoint> {
    vec![
        Endpoint {
            name: "Curie Console".to_string(),
            url: Some(LOCAL_CONSOLE_URL.to_string()),
            note: None,
            browsable: true,
        },
        Endpoint {
            name: "Langfuse UI (traces / cost / evals)".to_string(),
            url: Some(LOCAL_LANGFUSE_URL.to_string()),
            note: None,
            browsable: true,
        },
        Endpoint {
            name: "Curie API".to_string(),
            url: Some(LOCAL_API_URL.to_string()),
            note: None,
            browsable: false,
        },
    ]
}

/// Whether the handler should open `e` in a browser:
/// `e.browsable && open && !json`.
///
/// The `!json` term is belt-and-suspenders: `--open` is already an explicit
/// opt-in, but a machine consumer must never have tabs spawned at it. The base
/// already guaranteed "never open under `--json`"; #460 PRESERVES that across
/// both tiers rather than re-deriving it.
pub fn should_open(e: &Endpoint, open: bool, json: bool) -> bool {
    e.browsable && open && !json
}

/// Best-effort browser open of every surface [`should_open`] selects, shared by
/// both tiers so the opener choice and the swallow-the-failure policy cannot
/// drift. A missing opener (headless host, CI) is NOT an error: the URLs are
/// printed by `render` either way.
pub async fn open_endpoints(endpoints: &[Endpoint], open: bool, json: bool) {
    if open && json {
        // Surfaced rather than silently dropped, on stderr so the `--json`
        // stdout payload stays a single clean JSON line.
        crate::ui::ui().warn("--json suppresses --open: no browser was opened");
    }
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    for e in endpoints {
        if !should_open(e, open, json) {
            continue;
        }
        let Some(url) = e.url.as_deref() else {
            continue;
        };
        let _ = tokio::process::Command::new(opener)
            .arg(url)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(name: &str, url: Option<&str>, browsable: bool) -> Endpoint {
        Endpoint {
            name: name.to_string(),
            url: url.map(str::to_string),
            note: None,
            browsable,
        }
    }

    #[test]
    fn local_endpoints_yields_console_langfuse_and_api() {
        let eps = local_endpoints();
        assert_eq!(eps.len(), 3, "local tier yields exactly three surfaces");

        // Console: browsable, exact URL, no note. The name matches the shipped
        // handler's so the local payload stays a superset, not a rename.
        assert_eq!(eps[0].name, "Curie Console");
        assert_eq!(eps[0].url.as_deref(), Some(LOCAL_CONSOLE_URL));
        assert_eq!(eps[0].url.as_deref(), Some("http://localhost:28080/?api=1"));
        assert_eq!(eps[0].note, None);
        assert!(eps[0].browsable);

        // Langfuse: browsable, exact URL, no note.
        assert_eq!(eps[1].name, "Langfuse UI (traces / cost / evals)");
        assert_eq!(eps[1].url.as_deref(), Some(LOCAL_LANGFUSE_URL));
        assert_eq!(eps[1].url.as_deref(), Some("http://localhost:23000"));
        assert_eq!(eps[1].note, None);
        assert!(eps[1].browsable);

        // API base: has a URL but is NOT browsable (an agent target).
        assert_eq!(eps[2].name, "Curie API");
        assert_eq!(eps[2].url.as_deref(), Some(LOCAL_API_URL));
        assert_eq!(eps[2].url.as_deref(), Some("http://localhost:28000"));
        assert_eq!(eps[2].note, None);
        assert!(
            !eps[2].browsable,
            "the API base is never opened in a browser"
        );
    }

    /// The full browsable x open x json truth table: `browsable && open && !json`.
    /// Exactly one of the eight combinations may open a browser.
    #[test]
    fn should_open_truth_table() {
        let yes = ep("Curie Console", Some(LOCAL_CONSOLE_URL), true);
        let no = ep("Curie API", Some(LOCAL_API_URL), false);

        // browsable = true: only --open without --json opens.
        assert!(should_open(&yes, true, false), "browsable+open+!json opens");
        assert!(!should_open(&yes, true, true), "--json wins over --open");
        assert!(!should_open(&yes, false, false), "no --open, no browser");
        assert!(!should_open(&yes, false, true), "neither open nor json");

        // browsable = false: never, in any combination.
        assert!(!should_open(&no, true, false));
        assert!(!should_open(&no, true, true));
        assert!(!should_open(&no, false, false));
        assert!(!should_open(&no, false, true));
    }
}

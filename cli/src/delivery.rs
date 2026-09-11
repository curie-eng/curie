//! Truthful git-flow *delivery* diagnostics (#2496).
//!
//! Binding and delivery are two different facts. `cluster deploy --repo`
//! historically printed `git-flow pushes to <owner>/<name> deploy this agent`
//! from binding evidence alone -- a delivery promise nothing had observed.
//! This module owns the single statement of the delivery rule, so `commands.rs`
//! (the deploy bind line) and `doctor.rs` (the `webhook` check) cannot drift
//! apart the way they already did: #1239 shipped `api.commitPollIntervalSeconds`
//! as the no-ingress delivery path and never taught doctor about it.
//!
//! Everything here is pure. The observation happens at the call sites, which
//! have a namespace/release to read COMPUTED helm values from (#1950: a
//! user-supplied read cannot see a chart default nobody set, and this key's
//! default is `0`).
//!
//! The contract is pinned by `cli/tests/delivery_diagnostics.rs`; the renderer
//! strings asserted there are the operator-facing wording and are load-bearing
//! -- in particular, nothing here may claim a push was PROVEN to deploy, since
//! every fact this module judges is configuration state, never a delivery.

/// What was observed about this release's push-delivery paths.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeliveryFacts {
    /// How the API is reachable from outside, per doctor's single exposure
    /// decision function. `None` = neither ingress nor NodePort observed.
    /// NOT proof of unreachability: a load balancer or tunnel in front is
    /// invisible here, which is why no wording says a push *cannot* arrive.
    pub exposure: Option<String>,
    /// `api.commitPollIntervalSeconds` read from COMPUTED helm values (#1950).
    pub poll_interval_seconds: Option<f64>,
    /// Why discovery failed. `Some` forces [`Delivery::Unknown`] regardless of
    /// the fields above, so a failed read never reads as "not armed".
    pub discovery_failure: Option<String>,
}

/// The four -- and only four -- delivery states.
#[derive(Debug, Clone, PartialEq)]
pub enum Delivery {
    /// Nothing observed that would carry a push to this install.
    NotArmed,
    /// The API polls the repo for commits itself. Configuration evidence only.
    Polling { seconds: f64 },
    /// The API is reachable, but Curie does not create the GitHub webhook, so
    /// exposure alone is not delivery.
    ExposedUnverified { how: String },
    /// Discovery failed. Deliberately NOT [`Delivery::NotArmed`]: conflating
    /// them is this bug's own shape.
    Unknown { reason: String },
}

/// One rendered operator-facing line.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryLine {
    /// `true` -> `ui.warn`, `false` -> `ui.note`. Only `Polling` is a note.
    pub warning: bool,
    pub text: String,
}

/// Judge observed facts. Precedence: `discovery_failure`, then a positive poll
/// interval, then exposure, else `NotArmed`. A non-finite or negative interval
/// is not polling, matching the API's own `> 0` gate (`main.py:186`).
pub fn assess(facts: &DeliveryFacts) -> Delivery {
    // First, and unconditionally: a failed read outranks every other field,
    // including a stale-looking exposure or interval that came from a partial
    // observation. Reporting "not armed" about something nobody could see is
    // the same defect this ticket is about, one level up.
    if let Some(reason) = &facts.discovery_failure {
        return Delivery::Unknown {
            reason: reason.clone(),
        };
    }
    // Polling before exposure: it is the only path Curie itself drives, so it
    // is the strongest configuration evidence available and does not depend on
    // a webhook nobody created. `> 0` and finite mirrors the API's own gate
    // (`apps/api/.../main.py`), so an interval the API ignores is not reported
    // as armed here either.
    if let Some(seconds) = facts
        .poll_interval_seconds
        .filter(|s| s.is_finite() && *s > 0.0)
    {
        return Delivery::Polling { seconds };
    }
    match &facts.exposure {
        Some(how) => Delivery::ExposedUnverified { how: how.clone() },
        None => Delivery::NotArmed,
    }
}

/// Render an interval the way an operator typed it: helm records `60`, and
/// `60s` reads as a setting while `60.0s` reads as a computed number.
fn seconds(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

/// Render one assessment. Wording is pinned by the integration test.
pub fn render(delivery: &Delivery) -> DeliveryLine {
    match delivery {
        // Scoped to what was observed: "will not deploy this agent", never
        // "cannot reach this install". A load balancer or tunnel in front of
        // the API is invisible to every reader feeding this module, so the
        // stronger claim would be the same unobserved assertion in reverse.
        // Both arming paths are named, not just the one Curie drives (#2496).
        // An operator told only about polling reads the ingress/NodePort clause
        // as diagnosis and has no idea the other path exists; naming only the
        // webhook half is the ingress-only advice this ticket is fixing. The
        // webhook path is stated as two steps on purpose -- Curie exposes the
        // API but never creates the webhook, so "expose it" alone would be the
        // same unearned delivery promise in a new place.
        Delivery::NotArmed => DeliveryLine {
            warning: true,
            text: "push delivery is NOT armed: no webhook exposure (no ingress, no NodePort) \
                   and api.commitPollIntervalSeconds is 0, so a git push will not deploy this \
                   agent. Arm it by exposing the API where GitHub can reach it (ingress or \
                   NodePort) and creating a webhook there, or by setting \
                   api.commitPollIntervalSeconds."
                .to_string(),
        },
        // A note, never a warning: an armed poller is a working install, and
        // warning at one is the false alarm that teaches operators to ignore
        // this line entirely.
        Delivery::Polling { seconds: s } => DeliveryLine {
            warning: false,
            text: format!(
                "push delivery: commit polling is configured every {}s (configuration \
                 evidence only; no push has been observed)",
                seconds(*s)
            ),
        },
        // Exposure is neither armed nor broken. Curie never creates the GitHub
        // webhook, so an exposed API says only that a webhook COULD reach it.
        Delivery::ExposedUnverified { how } => DeliveryLine {
            warning: true,
            text: format!(
                "push delivery is UNCONFIRMED: the API is exposed ({how}), but Curie does not \
                 create the GitHub webhook, so exposure alone does not mean a push deploys \
                 this agent. Set api.commitPollIntervalSeconds for a path that needs no webhook."
            ),
        },
        Delivery::Unknown { reason } => DeliveryLine {
            warning: true,
            text: format!(
                "push delivery is UNKNOWN: {reason}, so whether a git push deploys this agent \
                 could not be determined"
            ),
        },
    }
}

/// Render one assessment as a doctor CHECK DETAIL (#2496).
///
/// The same four cases, in the phrasing a check row wants: a row already
/// carries its own state word (`OK` / `MISS` / `SKIP`) and its title, so the
/// line-oriented [`render`] would restate both. This is deliberately a second
/// RENDERER rather than a second RULE -- doctor had grown its own sentences for
/// three of these four cases, which is the drift this module exists to stop, so
/// both wordings now live here where a change to one is visible next to the
/// other.
///
/// Doctor still owns the STATE mapping and the fix command: which case is `ok`
/// versus `missing` versus `skipped` depends on doctor's cry-wolf policy, and a
/// recovery command needs a namespace and release this module has no business
/// knowing.
/// The clause that makes an `ExposedUnverified` row honest, named so doctor's
/// one-line verdict can recognise that row without re-deriving the rule (#2496).
/// `summary()` sees only `(id, state, detail)`, and this case is `Ok` for
/// cry-wolf reasons while being the one `Ok` that must NOT read as wired --
/// matching this constant is how those two facts stay one fact.
pub const EXPOSED_UNVERIFIED_MARKER: &str = "not a confirmed push path";

pub fn render_detail(delivery: &Delivery) -> String {
    match delivery {
        Delivery::NotArmed => "no ingress and no NodePort, and api.commitPollIntervalSeconds is \
                               0 — no observed path carries a push to this install. If a load \
                               balancer or tunnel fronts the API, this check cannot see it and \
                               you can ignore this"
            .to_string(),
        // The honesty clause travels with the fact here too: an OK row that
        // says only "polling every 60s" reads as an observed delivery, which is
        // precisely the claim nothing in this module is entitled to make.
        Delivery::Polling { seconds: s } => format!(
            "commit polling every {}s (api.commitPollIntervalSeconds); configuration evidence \
             only, no push has been observed",
            seconds(*s)
        ),
        Delivery::ExposedUnverified { how } => format!(
            "{how} — Curie does not create the GitHub webhook, so this is an exposed API, \
             {EXPOSED_UNVERIFIED_MARKER}"
        ),
        // A skipped row states the observation failure and nothing else, so the
        // line-oriented wording is already exactly right.
        Delivery::Unknown { .. } => render(delivery).text,
    }
}

/// The local tier never assesses delivery and must print nothing at all.
/// `None` (not assessed) is deliberately distinct from `Some(Unknown)`.
pub fn render_optional(delivery: Option<&Delivery>) -> Option<DeliveryLine> {
    delivery.map(render)
}

/// The binding-fact line that replaces the old delivery promise.
pub fn bind_note(repo: &str) -> String {
    // #1212's intent survives -- a successful bind is still visibly confirmed
    // and still names the repository. What it no longer does is promise the
    // deploy, which is a separate fact the delivery line answers.
    format!("repo binding: this agent is bound to {repo}; git-flow pushes to it route here")
}

// `polling_fix_suffix` now lives beside doctor's other fix-command builders
// (#2496): it takes an already-built `curie cluster up` string and no
// `Delivery` at all, so it is fix presentation, not observation. Re-exported
// here so its pinned test path keeps resolving.
pub use crate::doctor::polling_fix_suffix;

/// What a read of `api.commitPollIntervalSeconds` actually established (#2496).
///
/// Three outcomes, not two. `Option<f64>` had only "a number" and "no number",
/// and folding ABSENT together with PRESENT-BUT-UNREADABLE is this ticket's own
/// defect one level down: an absent key on a readable document is the ordinary
/// ClusterIP install the chart renders as `0`, while a null / bool / array /
/// object / unparseable-string value is a document nobody could read. Rendering
/// `NotArmed` for the second is a verdict about something never observed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PollReading {
    /// The key is genuinely absent from an otherwise readable document. The
    /// chart default is `0`, so this assesses exactly like an observed `0`.
    Absent,
    /// A number (or a `--set-string` numeric string) was read.
    Observed(f64),
    /// The document, or this key within it, could not be read. Must become a
    /// [`DeliveryFacts::discovery_failure`], never a `NotArmed` verdict.
    Unreadable,
}

/// Read `api.commitPollIntervalSeconds` out of a COMPUTED helm values document.
///
/// The whole document is the input, not the extracted node, because two of the
/// three outcomes are properties of the document rather than of the value: a
/// computed payload that is not an object at all (helm can answer `null` for an
/// empty body, `ops::fetch_helm_values` passes that through as `Ok(Some(null))`,
/// and that "successful" read establishes nothing), and an `api` node that is
/// not an object, both of which a node-level reader would report as a plain
/// absent key. Callers therefore cannot accidentally lose the distinction.
///
/// Helm records `--set-string api.commitPollIntervalSeconds="60"` as a JSON
/// *string*, so a number-only read would report an armed install as not armed.
pub fn read_poll_interval(computed: &serde_json::Value) -> PollReading {
    let Some(root) = computed.as_object() else {
        return PollReading::Unreadable;
    };
    let api = match root.get("api") {
        // No `api` key at all on a readable document: nothing set it, which is
        // the chart-default shape, not an unreadable one.
        None | Some(serde_json::Value::Null) => return PollReading::Absent,
        Some(api) => api,
    };
    let Some(api) = api.as_object() else {
        return PollReading::Unreadable;
    };
    match api.get("commitPollIntervalSeconds") {
        None => PollReading::Absent,
        Some(serde_json::Value::Number(n)) => n
            .as_f64()
            .map_or(PollReading::Unreadable, PollReading::Observed),
        Some(serde_json::Value::String(s)) => s
            .trim()
            .parse::<f64>()
            .map_or(PollReading::Unreadable, PollReading::Observed),
        // Null, bool, array, object, unparseable string: the key is THERE and
        // says something this reader cannot interpret. Coercing any of them to
        // `0.0` -- or to a plain absence -- turns an unreadable key into a "not
        // armed" verdict, the conflation `discovery_failure` exists to prevent.
        Some(_) => PollReading::Unreadable,
    }
}

/// The reason string a caller records when [`read_poll_interval`] could not
/// read the key. Stated once so both observers word the skip identically.
pub const UNREADABLE_INTERVAL_REASON: &str =
    "this release's computed Helm values recorded an unreadable \
     api.commitPollIntervalSeconds";

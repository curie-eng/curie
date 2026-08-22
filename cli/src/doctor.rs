//! `curie doctor`: what is set up, what is missing, and the command that fixes it.
//!
//! A first-time user learns the required inputs one failure at a time --
//! `skill up` succeeds and `skill message` fails on a credential; a deploy works
//! and the next `git push` does nothing because no ingress exists. The list is
//! about ten items long, and nothing states it up front.
//!
//! Documentation is the obvious answer and the wrong one: a checklist in a
//! README goes stale the moment a flag changes, and it cannot tell an operator
//! which items THEY are missing. This can, and it reports what is actually
//! observable rather than what a doc claims.
//!
//! Two rules the checks follow:
//!
//! - **Names, never values.** A credential is reported by the variable that
//!   holds it. This output is pasted into issues and chat.
//! - **Absent is not broken.** Someone on the laptop rung has no cluster and is
//!   not misconfigured. Cluster checks report `NotApplicable` rather than
//!   failing, so the output stays readable at every rung.

use serde::Serialize;

use crate::modelpin::{classify, PinStatus};

/// What one check found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Configured and usable.
    Ok,
    /// Genuinely missing, and something the user is trying to do needs it.
    Missing,
    /// Not needed at the rung this install is on.
    NotApplicable,
}

impl State {
    pub fn glyph(self) -> &'static str {
        match self {
            State::Ok => "ok  ",
            State::Missing => "MISS",
            State::NotApplicable => "--  ",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    /// Stable identifier, for a consumer gating on a specific check.
    pub id: &'static str,
    pub title: &'static str,
    pub state: State,
    /// What was observed. Never a credential value.
    pub detail: String,
    /// The exact command that fixes it, when it is fixable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

/// Everything the checks reason about, gathered once.
///
/// Separating observation from judgement is what makes the judgement testable:
/// every check below is a pure function of this struct, so the interesting
/// cases -- half-configured, laptop-only, fully wired -- are unit tests rather
/// than cluster fixtures.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    /// NAME of the model credential found, never its value.
    pub model_credential: Option<String>,
    /// Where it came from, for the detail line.
    pub model_credential_source: Option<String>,
    /// The configured model id, verbatim. A value here is not a claim that the
    /// id is valid, only that something set it.
    pub model_pin: Option<String>,
    pub docker_ok: bool,
    /// Plugin name from `.claude-plugin/plugin.json` in the working directory.
    pub bundle_name: Option<String>,
    pub kube_context: Option<String>,
    /// `(release, chart)` when a Curie release is installed.
    pub release: Option<(String, String)>,
    /// Whether the release has non-empty Slack tokens recorded.
    pub slack_configured: bool,
    /// Which clone credential the release carries, if any.
    pub clone_credential: Option<String>,
    /// Every agent and its repository binding. `None` means the platform API
    /// was not reached, which is a fact to report rather than a failure -- the
    /// other checks need only kubectl and helm.
    pub agents: Option<Vec<(String, Option<String>)>>,
    /// How the API is reachable from outside, if it is. `None` means neither
    /// mechanism the chart knows about is in place -- which is NOT proof it is
    /// unreachable, since a load balancer or tunnel in front is invisible here.
    pub api_exposure: Option<String>,
}

fn ok(id: &'static str, title: &'static str, detail: impl Into<String>) -> Check {
    Check {
        id,
        title,
        state: State::Ok,
        detail: detail.into(),
        fix: None,
    }
}

fn missing(
    id: &'static str,
    title: &'static str,
    detail: impl Into<String>,
    fix: impl Into<String>,
) -> Check {
    Check {
        id,
        title,
        state: State::Missing,
        detail: detail.into(),
        fix: Some(fix.into()),
    }
}

fn skipped(id: &'static str, title: &'static str, detail: impl Into<String>) -> Check {
    Check {
        id,
        title,
        state: State::NotApplicable,
        detail: detail.into(),
        fix: None,
    }
}

/// Judge the gathered facts. Pure.
pub fn evaluate(f: &Facts) -> Vec<Check> {
    let mut out = Vec::new();

    out.push(match (&f.model_credential, &f.model_credential_source) {
        (Some(name), Some(src)) => ok(
            "model-credential",
            "Model credential",
            format!("{name} ({src})"),
        ),
        (Some(name), None) => ok("model-credential", "Model credential", name.clone()),
        _ => missing(
            "model-credential",
            "Model credential",
            "none found",
            "export CURIE_CREDENTIALS=sk-ant-...   (or `curie secrets set CURIE_CREDENTIALS`; \
             `curie skill up --fake-model` needs none)",
        ),
    });

    out.push(match classify(f.model_pin.as_deref()) {
        PinStatus::Unset => skipped(
            "model-pin",
            "Model pin",
            "no CURIE_MODEL set, so the platform default applies",
        ),
        PinStatus::Pinned { id, date } => {
            ok("model-pin", "Model pin", format!("{id} (snapshot {date})"))
        }
        // Ok rather than Missing, deliberately: a floating name works, and a
        // working install must not report as unready. What is at risk is
        // reproducibility, so this carries a fix without failing the check.
        PinStatus::Floating { id } => Check {
            id: "model-pin",
            title: "Model pin",
            state: State::Ok,
            detail: format!(
                "{id} is a floating name; the provider can repoint it at new \
                 weights with no change here, and no gate would see it"
            ),
            fix: Some(
                "export CURIE_MODEL=<id>-YYYYMMDD   (pin the dated snapshot, so a \
                 graded bundle keeps the weights it was graded on)"
                    .into(),
            ),
        },
    });

    out.push(if f.docker_ok {
        ok("docker", "Docker", "running")
    } else {
        missing(
            "docker",
            "Docker",
            "not reachable",
            "start Docker Desktop, or install it: https://docs.docker.com/get-docker/",
        )
    });

    out.push(match &f.bundle_name {
        Some(name) => ok("bundle", "Bundle in this directory", name.clone()),
        None => missing(
            "bundle",
            "Bundle in this directory",
            "no .claude-plugin/plugin.json",
            "curie init my-agent && cd my-agent",
        ),
    });

    // Everything below needs a cluster. Without one this install is on the
    // laptop rung, which is a complete way to use Curie -- so these report as
    // not-applicable rather than as failures.
    let Some(context) = &f.kube_context else {
        out.push(skipped(
            "cluster",
            "Cluster",
            "no kube context — laptop rung only, which is fine",
        ));
        for (id, title) in [
            ("release", "Curie release"),
            ("slack", "Slack"),
            ("clone-credential", "Clone credential"),
            ("webhook", "Webhook exposure"),
            ("repo-binding", "Repo binding"),
        ] {
            out.push(skipped(id, title, "needs a cluster"));
        }
        return out;
    };
    out.push(ok("cluster", "Cluster", context.clone()));

    let Some((release, chart)) = &f.release else {
        out.push(missing(
            "release",
            "Curie release",
            "not installed in this namespace",
            "curie cluster up --namespace <ns> --release <name> --allow-egress-host anthropic",
        ));
        for (id, title) in [
            ("slack", "Slack"),
            ("clone-credential", "Clone credential"),
            ("webhook", "Webhook exposure"),
            ("repo-binding", "Repo binding"),
        ] {
            out.push(skipped(id, title, "needs a release"));
        }
        return out;
    };
    out.push(ok(
        "release",
        "Curie release",
        format!("{release} ({chart})"),
    ));

    out.push(if f.slack_configured {
        ok("slack", "Slack", "app and bot tokens recorded")
    } else {
        missing(
            "slack",
            "Slack",
            "no tokens recorded",
            "curie cluster comms --slack --app-token xapp-... --bot-token xoxb-...",
        )
    });

    out.push(match &f.clone_credential {
        Some(which) => ok("clone-credential", "Clone credential", which.clone()),
        None => missing(
            "clone-credential",
            "Clone credential",
            "none — a private repo cannot be cloned, so git-push deploys will fail",
            "curie cluster github-app --app-id <id> --private-key ./key.pem",
        ),
    });

    // Exposure has more than one shape, and asserting otherwise cries wolf on a
    // working install: sre-bot serves its webhook on a NodePort with no ingress
    // at all, and an early version of this check called that broken. A doctor
    // that is wrong about a working setup is worse than no doctor, because it
    // teaches people to ignore it.
    out.push(match &f.api_exposure {
        Some(how) => ok("webhook", "Webhook exposure", how.clone()),
        None => missing(
            "webhook",
            "Webhook exposure",
            "no ingress and no NodePort — if a load balancer or tunnel fronts the API, \
             this check cannot see it and you can ignore this",
            "curie cluster up --set api.ingress.enabled=true --set api.ingress.host=<host>",
        ),
    });

    // The binding decides whether a push reaches this agent at all. A push for
    // an agent with none matches nothing and is answered `ignored` -- nothing is
    // logged, so the only symptom is a green delivery in GitHub and no deploy.
    out.push(match &f.agents {
        None => skipped(
            "repo-binding",
            "Repo binding",
            "platform API not reached — pass --api-url/--api-key to include this",
        ),
        Some(agents) if agents.is_empty() => {
            skipped("repo-binding", "Repo binding", "no agents deployed yet")
        }
        Some(agents) => {
            let unbound: Vec<&str> = agents
                .iter()
                .filter(|(_, repo)| repo.is_none())
                .map(|(name, _)| name.as_str())
                .collect();
            if unbound.is_empty() {
                ok(
                    "repo-binding",
                    "Repo binding",
                    format!("{} agent(s), all bound", agents.len()),
                )
            } else {
                missing(
                    "repo-binding",
                    "Repo binding",
                    format!(
                        "unbound: {} — a push for these matches no agent and is \
                         silently ignored",
                        unbound.join(", ")
                    ),
                    "curie cluster deploy --plugin-dir . --repo <owner>/<name>   \
                     (binds an agent that has none; it will NOT rebind one already \
                     pointing at a different repository)",
                )
            }
        }
    });

    out
}

/// Point at the guided path when there is more than one thing to fix.
///
/// A single missing item needs no signpost: the `→ fix` line beside it is the
/// whole answer. Several is different -- the fixes have an order, some depend
/// on values the operator has not collected yet, and reading eight lines and
/// sequencing them is exactly the work the guided workflow already does.
///
/// `curie` with no arguments opens that workflow. It is discoverable in
/// principle and invisible in practice, because a first-time user reaching for
/// help types `curie --help` and gets an alphabetical list of eighteen verbs
/// with `interactive` eleventh.
pub fn guidance(checks: &[Check]) -> Option<String> {
    let missing = checks.iter().filter(|c| c.state == State::Missing).count();
    if missing < 2 {
        return None;
    }
    Some(format!(
        "{missing} things to set up. Run `curie` with no arguments for a guided \
         walkthrough, or fix them one at a time with the commands above."
    ))
}

/// The one-line verdict: what this install can do right now.
///
/// Deliberately capability-shaped rather than a count. "3 of 8 checks passed"
/// tells an operator nothing; "you can run locally but not deploy" tells them
/// where they are.
pub fn summary(checks: &[Check]) -> String {
    let state = |id: &str| checks.iter().find(|c| c.id == id).map(|c| c.state);
    let has = |id: &str| state(id) == Some(State::Ok);

    if !has("bundle") {
        return "No bundle here. Start with `curie init my-agent`.".to_string();
    }
    if !has("docker") {
        return "Docker is not reachable, so nothing can run locally yet.".to_string();
    }
    if !has("model-credential") {
        return "Ready to run offline (`curie skill up --fake-model`). A model \
                credential is needed for real replies."
            .to_string();
    }
    if !has("release") {
        return "Ready to run locally. No cluster release yet, so no Slack and no \
                deploys."
            .to_string();
    }
    if !has("slack") {
        return "Deployable to the cluster. Slack is not wired, so the agent has no \
                way to be reached."
            .to_string();
    }
    if !has("clone-credential") || !has("webhook") || state("repo-binding") == Some(State::Missing)
    {
        return "Answering in Slack. Git-push deploys are not wired yet -- see the \
                missing items above."
            .to_string();
    }
    // repo-binding reads NotApplicable in two situations that both reach this
    // point: the platform API was never consulted (no --api-url/--api-key, an
    // unreachable API, or a rejected key -- indistinguishable from here) or it
    // was reached and found no agents deployed yet. Neither is evidence a git
    // push deploys anything, so claiming "Fully wired" here asserted the one
    // capability the run did not check (#1354).
    if !has("repo-binding") {
        return "Answering in Slack. Git-push deploys are unverified -- see the \
                Repo binding line above."
            .to_string();
    }
    "Fully wired: local runs, Slack, and git-push deploys.".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn laptop() -> Facts {
        Facts {
            docker_ok: true,
            bundle_name: Some("my-agent".into()),
            ..Default::default()
        }
    }

    fn checks_with(missing: usize) -> Vec<Check> {
        (0..missing)
            .map(|i| Check {
                id: "x",
                title: "t",
                state: State::Missing,
                detail: format!("{i}"),
                fix: None,
            })
            .collect()
    }

    /// One gap needs no signpost -- the `-> fix` beside it is the whole answer.
    /// Sending someone to a TUI to set one environment variable is worse than
    /// telling them the variable.
    #[test]
    fn a_single_gap_does_not_advertise_the_walkthrough() {
        assert_eq!(guidance(&checks_with(0)), None);
        assert_eq!(guidance(&checks_with(1)), None);
    }

    /// Several gaps have an ORDER and depend on values not yet collected, which
    /// is the work the guided workflow exists to do.
    #[test]
    fn several_gaps_name_the_guided_path() {
        let hint = guidance(&checks_with(3)).expect("should advertise");
        assert!(hint.contains("3 things"), "{hint}");
        assert!(hint.contains("`curie`"), "must name the command: {hint}");
        assert!(
            hint.contains("one at a time"),
            "must leave the manual path open: {hint}"
        );
    }

    /// A check that is NotApplicable is not a gap. Counting it would advertise
    /// a walkthrough for a cluster the operator does not have.
    #[test]
    fn checks_that_do_not_apply_are_not_counted_as_gaps() {
        let mut checks = checks_with(1);
        for i in 0..4 {
            checks.push(Check {
                id: "n",
                title: "t",
                state: State::NotApplicable,
                detail: format!("{i}"),
                fix: None,
            });
        }
        assert_eq!(
            guidance(&checks),
            None,
            "one real gap plus four n/a is one gap"
        );
    }

    /// Cluster, release, Slack and clone credential all in place.
    fn wired() -> Facts {
        Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("minikube".into()),
            release: Some(("acme".into(), "curie-0.6.0".into())),
            slack_configured: true,
            clone_credential: Some("github app".into()),
            api_exposure: Some("NodePort 30799".into()),
            ..laptop()
        }
    }

    fn find<'a>(checks: &'a [Check], id: &str) -> &'a Check {
        checks.iter().find(|c| c.id == id).expect(id)
    }

    /// The laptop rung is a complete way to use Curie. Reporting five failures
    /// at someone who has not asked for a cluster is how a doctor command
    /// becomes noise people stop reading.
    #[test]
    fn no_cluster_is_not_a_failure() {
        let checks = evaluate(&laptop());
        for id in ["cluster", "release", "slack", "clone-credential", "webhook"] {
            assert_eq!(
                find(&checks, id).state,
                State::NotApplicable,
                "{id} must not read as broken on the laptop rung"
            );
        }
        assert!(
            find(&checks, "cluster").detail.contains("which is fine"),
            "the detail should reassure, not accuse"
        );
    }

    /// The failure a first-time user actually hits: boot succeeds, the next
    /// command fails. The fix has to name the variable AND the offline escape.
    #[test]
    fn a_missing_credential_names_both_ways_forward() {
        let checks = evaluate(&laptop());
        let c = find(&checks, "model-credential");
        assert_eq!(c.state, State::Missing);
        let fix = c.fix.as_deref().expect("must offer a fix");
        assert!(fix.contains("CURIE_CREDENTIALS"), "{fix}");
        assert!(
            fix.contains("--fake-model"),
            "the offline path matters: {fix}"
        );
    }

    /// This output gets pasted into issues and chat.
    #[test]
    fn no_check_can_carry_a_credential_value() {
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            model_credential_source: Some("environment".into()),
            clone_credential: Some("github app (app_id=4475970)".into()),
            slack_configured: true,
            ..laptop()
        };
        let rendered = format!("{:?}", evaluate(&f));
        for leaked in ["sk-ant-", "xoxb-", "xapp-", "ghp_", "-----BEGIN"] {
            assert!(!rendered.contains(leaked), "{leaked} leaked: {rendered}");
        }
    }

    /// Each rung's summary has to say what you can DO, not how many checks
    /// passed -- a count tells an operator nothing about where they are.
    #[test]
    fn the_summary_reports_capability_at_each_rung() {
        let cases = [
            (Facts::default(), "curie init"),
            (laptop(), "fake-model"),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    ..laptop()
                },
                "No cluster release",
            ),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    kube_context: Some("minikube".into()),
                    release: Some(("acme".into(), "curie-0.6.0".into())),
                    ..laptop()
                },
                "Slack is not wired",
            ),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    kube_context: Some("minikube".into()),
                    release: Some(("acme".into(), "curie-0.6.0".into())),
                    slack_configured: true,
                    ..laptop()
                },
                "Git-push deploys are not wired",
            ),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    kube_context: Some("minikube".into()),
                    release: Some(("acme".into(), "curie-0.6.0".into())),
                    slack_configured: true,
                    clone_credential: Some("github app".into()),
                    api_exposure: Some("NodePort 30799".into()),
                    agents: Some(vec![("bot".into(), Some("acme/bot".into()))]),
                    ..laptop()
                },
                "Fully wired",
            ),
        ];
        for (facts, expected) in cases {
            let s = summary(&evaluate(&facts));
            assert!(s.contains(expected), "expected {expected:?} in {s:?}");
        }
    }

    /// A floating model name is reported without failing the install: it works
    /// today, so `Missing` would make a usable setup look broken. The fix is
    /// what carries the advice.
    #[test]
    fn a_floating_model_reports_ok_with_a_fix() {
        let f = Facts {
            model_pin: Some("gpt-4o".into()),
            ..Default::default()
        };
        let c = evaluate(&f)
            .into_iter()
            .find(|c| c.id == "model-pin")
            .expect("model-pin check");
        assert_eq!(c.state, State::Ok);
        assert!(c.detail.contains("gpt-4o"), "{}", c.detail);
        assert!(
            c.fix.as_deref().unwrap_or("").contains("CURIE_MODEL"),
            "the fix must name the variable to set"
        );
    }

    /// A dated snapshot is clean: no advice, nothing to do.
    #[test]
    fn a_pinned_snapshot_carries_no_fix() {
        let f = Facts {
            model_pin: Some("claude-haiku-4-5-20251001".into()),
            ..Default::default()
        };
        let c = evaluate(&f)
            .into_iter()
            .find(|c| c.id == "model-pin")
            .expect("model-pin check");
        assert_eq!(c.state, State::Ok);
        assert!(c.fix.is_none(), "a pinned snapshot needs no fix");
        assert!(c.detail.contains("20251001"), "{}", c.detail);
    }

    /// No model configured is not a gap: the platform default is a valid way to
    /// run, so this must not count against readiness.
    #[test]
    fn an_unset_model_is_not_applicable() {
        let c = evaluate(&Facts::default())
            .into_iter()
            .find(|c| c.id == "model-pin")
            .expect("model-pin check");
        assert_eq!(c.state, State::NotApplicable);
    }

    /// Every failing check must be actionable. A report that says "missing"
    /// without saying what to run is the checklist problem restated.
    #[test]
    fn every_missing_check_carries_a_command() {
        let facts = [Facts::default(), laptop()];
        for f in facts {
            for c in evaluate(&f).iter().filter(|c| c.state == State::Missing) {
                let fix = c.fix.as_deref().unwrap_or("");
                assert!(
                    fix.contains("curie ") || fix.contains("export ") || fix.contains("http"),
                    "{} must name a command, got {fix:?}",
                    c.id
                );
            }
        }
    }

    /// The binding is what makes a push reach an agent. An unbound one fails
    /// SILENTLY: the webhook returns 200, GitHub shows a green delivery, and
    /// nothing is logged -- so the check has to say that out loud. This also
    /// pins the Missing versus NotApplicable distinction the verdict turns
    /// on: a KNOWN unbound agent must reach the actionable "not wired yet"
    /// summary, not the "unverified" hedge reserved for never having checked.
    #[test]
    fn an_unbound_agent_is_reported_with_what_it_costs() {
        let f = Facts {
            agents: Some(vec![
                ("bot".into(), Some("acme/bot".into())),
                ("bot-dev".into(), None),
            ]),
            ..wired()
        };
        let checks = evaluate(&f);
        let c = find(&checks, "repo-binding").clone();
        assert_eq!(c.state, State::Missing);
        assert!(c.detail.contains("bot-dev"), "must name it: {}", c.detail);
        assert!(
            c.detail.contains("silently ignored"),
            "must say the failure is silent: {}",
            c.detail
        );
        assert!(
            summary(&checks).contains("Git-push deploys are not wired yet"),
            "a KNOWN unbound agent must route to the actionable verdict, not \
             the unverified hedge: {}",
            summary(&checks)
        );
    }

    /// The advice has to be right about what CAN be fixed. An agent with no
    /// binding is bindable by a later deploy (#1194); only one already pointing
    /// at a DIFFERENT repository is left alone. Telling someone to delete and
    /// recreate an unbound agent would destroy its version history for nothing.
    #[test]
    fn the_fix_distinguishes_unbound_from_misbound() {
        let f = Facts {
            agents: Some(vec![("bot".into(), None)]),
            ..wired()
        };
        let fix = find(&evaluate(&f), "repo-binding")
            .fix
            .clone()
            .expect("must offer a fix");
        assert!(fix.contains("--repo"), "{fix}");
        assert!(
            fix.contains("NOT rebind"),
            "must be explicit that a wrong binding is not fixed this way: {fix}"
        );
    }

    /// Not reaching the API is a fact, not a failure -- doctor needs only
    /// kubectl and helm for everything else.
    #[test]
    fn an_unreachable_api_is_not_a_failure() {
        let c = find(&evaluate(&wired()), "repo-binding").clone();
        assert_eq!(c.state, State::NotApplicable);
        assert!(c.detail.contains("--api-url"), "{}", c.detail);
    }

    #[test]
    fn all_bound_agents_pass() {
        let f = Facts {
            agents: Some(vec![("bot".into(), Some("acme/bot".into()))]),
            ..wired()
        };
        assert_eq!(find(&evaluate(&f), "repo-binding").state, State::Ok);
    }

    /// Found on a real install (#1354): every other check passed, the platform
    /// API was never reached (no --api-url/--api-key), and the summary still
    /// said "Fully wired" -- asserting the one capability the run did not
    /// check. `wired()` has no `agents` set, so repo-binding is NotApplicable
    /// here, not Ok.
    #[test]
    fn unreached_api_does_not_claim_fully_wired() {
        let checks = evaluate(&wired());
        let s = summary(&checks);
        assert!(!s.contains("Fully wired"), "{s}");
        assert!(s.contains("Git-push deploys are unverified"), "{s}");
    }

    /// The sibling NotApplicable path: the platform API WAS reached but no
    /// agents are deployed yet. A different reason, the same lack of evidence
    /// that a git push deploys anything, so both paths must land on the same
    /// hedge rather than one of them slipping through to "Fully wired".
    #[test]
    fn no_agents_deployed_does_not_claim_fully_wired() {
        let f = Facts {
            agents: Some(vec![]),
            ..wired()
        };
        let checks = evaluate(&f);
        let s = summary(&checks);
        assert!(!s.contains("Fully wired"), "{s}");
        assert!(s.contains("Git-push deploys are unverified"), "{s}");
    }

    /// Found by running this against a real install. sre-bot serves its webhook
    /// on a NodePort with no ingress, and the first version of this check called
    /// that broken -- on a cluster where git-push deploys demonstrably work.
    #[test]
    fn a_nodeport_counts_as_exposure() {
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("default".into()),
            release: Some(("sre-bot".into(), "curie-0.6.0".into())),
            slack_configured: true,
            clone_credential: Some("github app".into()),
            api_exposure: Some("NodePort 30799".into()),
            agents: Some(vec![("sre-bot".into(), Some("acme/sre-bot".into()))]),
            ..laptop()
        };
        let checks = evaluate(&f);
        assert_eq!(find(&checks, "webhook").state, State::Ok);
        assert!(
            summary(&checks).contains("Fully wired"),
            "{}",
            summary(&checks)
        );
    }

    /// And when nothing is found, it must not claim the API is unreachable --
    /// a load balancer or tunnel in front is invisible to this check.
    #[test]
    fn no_known_exposure_is_hedged_not_asserted() {
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("minikube".into()),
            release: Some(("acme".into(), "curie-0.6.0".into())),
            slack_configured: true,
            clone_credential: Some("pat".into()),
            ..laptop()
        };
        let c = find(&evaluate(&f), "webhook").clone();
        assert_eq!(c.state, State::Missing);
        assert!(c.detail.contains("you can ignore this"), "{}", c.detail);
    }

    /// A half-wired release is the state that looks fine and is not: the bot
    /// answers, and every push silently does nothing.
    #[test]
    fn slack_without_deploy_wiring_is_reported_precisely() {
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("minikube".into()),
            release: Some(("acme".into(), "curie-0.6.0".into())),
            slack_configured: true,
            ..laptop()
        };
        let checks = evaluate(&f);
        assert_eq!(find(&checks, "slack").state, State::Ok);
        assert_eq!(find(&checks, "webhook").state, State::Missing);
        assert!(
            find(&checks, "webhook")
                .detail
                .contains("no ingress and no NodePort"),
            "must name what it looked for: {}",
            find(&checks, "webhook").detail
        );
    }
}

// -- observation --------------------------------------------------------------

/// Gather the facts. Every probe is read-only and failure-tolerant: a missing
/// tool or an unreachable cluster is a fact to report, never an error to raise.
pub async fn gather(namespace: &str, release: &str, api: Option<(&str, &str)>) -> Facts {
    let mut f = Facts {
        docker_ok: probe_ok("docker", &["info"]).await,
        bundle_name: bundle_name(),
        ..Default::default()
    };

    // Names, never values: the id is the configuration, not a secret.
    f.model_pin = std::env::var(curie_aci_protocol::env_keys::CURIE_MODEL).ok();

    for name in crate::commands::MODEL_CREDENTIAL_ENV_NAMES {
        if std::env::var(name).map(|v| !v.is_empty()).unwrap_or(false) {
            f.model_credential = Some(name.to_string());
            f.model_credential_source = Some("environment".into());
            break;
        }
        if crate::secrets::is_saved(name).unwrap_or(false) {
            f.model_credential = Some(name.to_string());
            f.model_credential_source = Some("curie secrets".into());
            break;
        }
    }

    // Optional: everything else needs only kubectl and helm, so an absent or
    // unreachable API narrows the report rather than failing it.
    if let Some((url, key)) = api {
        if let Ok(client) = crate::api::ApiClient::new(url, key) {
            if let Ok(agents) = client.list_agents().await {
                f.agents = Some(
                    agents
                        .into_iter()
                        .map(|a| (a.name, a.repo_full_name))
                        .collect(),
                );
            }
        }
    }

    let (ok, ctx, _) = capture("kubectl", &["config", "current-context"]).await;
    if !ok || ctx.trim().is_empty() {
        return f;
    }
    f.kube_context = Some(ctx.trim().to_string());

    let common = crate::ops::CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    let Ok(Some(chart)) = crate::ops::fetch_release_chart(&common).await else {
        return f;
    };
    f.release = Some((release.to_string(), chart));

    if let Ok(Some(values)) = crate::ops::fetch_release_values(&common).await {
        let at = |path: &[&str]| -> Option<String> {
            let mut node = &values;
            for k in path {
                node = node.get(k)?;
            }
            node.as_str().map(str::to_string).filter(|s| !s.is_empty())
        };
        f.slack_configured = at(&["dispatcher", "slack", "botToken"]).is_some();
        let ingress_on = values
            .get("api")
            .and_then(|a| a.get("ingress"))
            .and_then(|i| i.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        f.api_exposure = if ingress_on {
            Some(match at(&["api", "ingress", "host"]) {
                Some(host) => format!("ingress ({host})"),
                None => "ingress".to_string(),
            })
        } else {
            api_nodeport(namespace, release)
                .await
                .map(|p| format!("NodePort {p}"))
        };
        // NAMES and shapes only -- never the key or token itself.
        f.clone_credential = match at(&["api", "githubAppId"]) {
            Some(id) => Some(format!("github app (app_id={id})")),
            None => at(&["api", "githubToken"]).map(|_| "personal access token".to_string()),
        };
    }
    f
}

/// The API Service's nodePort, when it is exposed that way.
async fn api_nodeport(namespace: &str, release: &str) -> Option<String> {
    let (ok, out, _) = capture(
        "kubectl",
        &[
            "get",
            "svc",
            &format!("{release}-api"),
            "-n",
            namespace,
            "-o",
            "jsonpath={.spec.ports[?(@.nodePort)].nodePort}",
        ],
    )
    .await;
    let port = out.trim().to_string();
    (ok && !port.is_empty()).then_some(port)
}

fn bundle_name() -> Option<String> {
    let raw = std::fs::read_to_string(".claude-plugin/plugin.json").ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    v.get("name")?.as_str().map(str::to_string)
}

async fn probe_ok(program: &str, args: &[&str]) -> bool {
    capture(program, args).await.0
}

async fn capture(program: &str, args: &[&str]) -> (bool, String, String) {
    let cmd = crate::ops::OpsCommand::new(
        program,
        args.iter().map(|a| crate::ops::plain(*a)).collect(),
    );
    crate::ops::run_capture(&cmd)
        .await
        .unwrap_or((false, String::new(), String::new()))
}

/// What `curie doctor` reports.
#[derive(Debug)]
pub struct DoctorOutput {
    pub checks: Vec<Check>,
    pub summary: String,
}

impl crate::ui::CliOutput for DoctorOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "summary": self.summary,
            "ready": self.checks.iter().all(|c| c.state != State::Missing),
            "checks": self.checks,
            "guidance": guidance(&self.checks),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        for c in &self.checks {
            ui.payload_plain(&format!(
                "{}  {:<26} {}",
                c.state.glyph(),
                c.title,
                c.detail
            ));
            if let Some(fix) = &c.fix {
                ui.payload_plain(&format!("      → {fix}"));
            }
        }
        ui.payload_plain("");
        ui.payload_plain(&self.summary);
        if let Some(hint) = guidance(&self.checks) {
            ui.payload_plain(&hint);
        }
    }
}

pub async fn doctor(namespace: &str, release: &str, api: Option<(&str, &str)>) -> DoctorOutput {
    let checks = evaluate(&gather(namespace, release, api).await);
    let summary = summary(&checks);
    DoctorOutput { checks, summary }
}

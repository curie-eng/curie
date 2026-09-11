//! Integration: truthful git-flow delivery diagnostics (#2496).
//!
//! `curie cluster deploy --repo` printed `repo binding: git-flow pushes to
//! <owner>/<name> deploy this agent` and exited 0 even when NO push-delivery
//! path was armed (no ingress/NodePort AND `api.commitPollIntervalSeconds` is
//! 0), and doctor's `webhook` MISS fix named only `--set
//! api.ingress.enabled=true`. Binding is observed; delivery was assumed.
//!
//! These tests pin the pure surface the implementer will add to
//! `cli/src/delivery.rs` (imported here from the `curie` lib):
//!
//!   pub struct DeliveryFacts {
//!       pub exposure: Option<String>,
//!       pub poll_interval_seconds: Option<f64>,
//!       pub discovery_failure: Option<String>,
//!   }
//!   pub enum Delivery { NotArmed, Polling { seconds: f64 },
//!                       ExposedUnverified { how: String },
//!                       Unknown { reason: String } }
//!   pub struct DeliveryLine { pub warning: bool, pub text: String }
//!
//!   pub fn assess(facts: &DeliveryFacts) -> Delivery
//!   pub fn render(delivery: &Delivery) -> DeliveryLine
//!   pub fn render_optional(delivery: Option<&Delivery>) -> Option<DeliveryLine>
//!   pub fn bind_note(repo: &str) -> String
//!   pub fn polling_fix_suffix(targeted_up: &str) -> String
//!   pub fn read_poll_interval(computed: &serde_json::Value) -> PollReading
//!
//! plus two new `curie::doctor::Facts` fields the doctor cases below construct:
//!
//!   pub commit_poll_interval_seconds: Option<f64>
//!   pub delivery_discovery_failure: Option<String>
//!
//! Until all of those exist this test target fails to compile: that is the
//! intended RED, isolated to this file because it imports from the lib rather
//! than adding inline lib tests. It is also the fix-pin selector file for this
//! `bug`-labelled issue -- `tools/fix-pin-ci/check.py` accepts only
//! `cli/tests/<file>.rs::<test_fn>`, which an in-module `#[cfg(test)]` test
//! cannot satisfy, so every AC-bearing assertion lives here.

use curie::delivery::{
    assess, bind_note, polling_fix_suffix, read_poll_interval, render, render_optional, Delivery,
    DeliveryFacts, PollReading,
};
use curie::doctor::{evaluate, summary, Check, Facts, ReleaseProbe, State};

fn facts(exposure: Option<&str>, interval: Option<f64>, failure: Option<&str>) -> DeliveryFacts {
    DeliveryFacts {
        exposure: exposure.map(str::to_string),
        poll_interval_seconds: interval,
        discovery_failure: failure.map(str::to_string),
    }
}

fn find<'a>(checks: &'a [Check], id: &str) -> &'a Check {
    checks
        .iter()
        .find(|c| c.id == id)
        .unwrap_or_else(|| panic!("no check with id {id:?}; got {:?}", ids(checks)))
}

fn ids(checks: &[Check]) -> Vec<&str> {
    checks.iter().map(|c| c.id).collect()
}

/// Every string this module can put in front of an operator, for the wording
/// guard and the "no proven-delivery claim" sweep.
fn every_rendered_string() -> Vec<String> {
    [
        Delivery::NotArmed,
        Delivery::Polling { seconds: 60.0 },
        Delivery::ExposedUnverified {
            how: "ingress curie.example.com".to_string(),
        },
        Delivery::Unknown {
            reason: "could not read this release's computed Helm values".to_string(),
        },
    ]
    .iter()
    .map(|d| render(d).text)
    .chain(std::iter::once(bind_note("acme/widgets")))
    .collect()
}

// ---------------------------------------------------------------------------
// The four assessment cases (plan tests 1, 2, 4, 6)
// ---------------------------------------------------------------------------

/// AC1 / plan test 1 -- THE FIX PIN. No exposure and no poller means no
/// observed path carries a push to this install, and the operator must be told
/// so instead of being promised a deploy that will never happen.
#[test]
fn no_exposure_and_no_poller_warns_that_push_delivery_is_not_armed() {
    let d = assess(&facts(None, Some(0.0), None));
    assert_eq!(
        d,
        Delivery::NotArmed,
        "no ingress/NodePort and api.commitPollIntervalSeconds=0 is the ticket's case 1"
    );

    let line = render(&d);
    assert!(
        line.warning,
        "NotArmed is the operator's action item and must render as a warning, got {line:?}"
    );
    assert!(
        line.text.contains("push delivery is NOT armed"),
        "the not-armed warning must say so plainly, got {:?}",
        line.text
    );
    assert!(
        line.text.contains("api.commitPollIntervalSeconds"),
        "the warning must name the key that arms the no-webhook path, got {:?}",
        line.text
    );
    assert!(
        line.text.contains("will not deploy this agent"),
        "the warning must state the consequence in the same terms the old false \
         promise used, got {:?}",
        line.text
    );
}

/// AC2 / plan test 2. A positive interval is the strongest configuration
/// evidence there is -- Curie itself fetches the commits -- and needs no
/// exposure at all. This is the path #1239 shipped and doctor never learned.
#[test]
fn positive_interval_is_polling_even_without_exposure() {
    let d = assess(&facts(None, Some(60.0), None));
    assert_eq!(
        d,
        Delivery::Polling { seconds: 60.0 },
        "a private install with the poller armed is delivering, got {d:?}"
    );

    let line = render(&d);
    assert!(
        !line.warning,
        "an armed poller is not a problem and must be a note, not a warning: {line:?}"
    );
    assert!(
        line.text.contains("commit polling is configured"),
        "the polling note must name the mechanism, got {:?}",
        line.text
    );
    assert!(
        line.text.contains("configuration evidence only")
            && line.text.contains("no push has been observed"),
        "polling is configuration evidence, never a proven delivery, got {:?}",
        line.text
    );
}

/// AC3 / plan test 4. Curie does not create the GitHub webhook, so an exposed
/// API is UNCONFIRMED delivery -- neither armed nor broken.
#[test]
fn exposure_without_polling_is_unconfirmed_not_armed() {
    let d = assess(&facts(Some("ingress curie.example.com"), Some(0.0), None));
    assert_eq!(
        d,
        Delivery::ExposedUnverified {
            how: "ingress curie.example.com".to_string()
        },
        "exposure with no poller is case 3, got {d:?}"
    );

    let line = render(&d);
    assert!(
        line.text.contains("push delivery is UNCONFIRMED"),
        "exposure must be reported as unconfirmed, got {:?}",
        line.text
    );
    assert!(
        line.text
            .contains("Curie does not create the GitHub webhook"),
        "AC3 requires naming WHY exposure is not delivery, got {:?}",
        line.text
    );
    assert!(
        line.text.contains("ingress curie.example.com"),
        "the observed exposure must be carried into the line, got {:?}",
        line.text
    );
    assert!(
        !line.text.contains("NOT armed"),
        "an exposed install must not be told delivery is not armed, got {:?}",
        line.text
    );
}

/// AC4 / plan test 6 -- THE SINGLE MOST IMPORTANT NEGATIVE. A failed discovery
/// must never collapse into "not armed": asserting a verdict about something
/// nobody could observe is the exact defect this ticket is about, one level up.
#[test]
fn discovery_failure_is_unknown_and_never_not_armed() {
    let d = assess(&facts(
        None,
        None,
        Some("could not read this release's computed Helm values"),
    ));

    assert_ne!(
        d,
        Delivery::NotArmed,
        "a failed read must NOT be reported as not-armed -- that is the bug's own shape"
    );
    assert_eq!(
        d,
        Delivery::Unknown {
            reason: "could not read this release's computed Helm values".to_string()
        },
        "case 4 is Unknown carrying the reason, got {d:?}"
    );

    // A discovery failure outranks every other field, including facts that
    // would otherwise read as armed.
    let masked = assess(&facts(
        Some("nodePort 30080"),
        Some(60.0),
        Some("helm read failed"),
    ));
    assert!(
        matches!(masked, Delivery::Unknown { .. }),
        "discovery_failure must take precedence over stale exposure/interval reads, got {masked:?}"
    );

    let line = render(&d);
    assert!(
        line.text.contains("push delivery is UNKNOWN"),
        "the unknown line must say so, got {:?}",
        line.text
    );
    assert!(
        line.text
            .contains("could not read this release's computed Helm values"),
        "the reason must be carried so the operator can act on it, got {:?}",
        line.text
    );
    assert!(
        !line.text.contains("NOT armed"),
        "UNKNOWN must not leak the not-armed verdict, got {:?}",
        line.text
    );
}

// ---------------------------------------------------------------------------
// Negatives and boundaries (plan tests 3, 5, 7)
// ---------------------------------------------------------------------------

/// Plan test 3. The false alarm the ticket explicitly forbids: an install with
/// commit polling armed must never be told push delivery is not armed.
#[test]
fn polling_never_renders_the_not_armed_warning() {
    for seconds in [1.0, 30.0, 60.0, 3600.0] {
        let line = render(&Delivery::Polling { seconds });
        assert!(
            !line.text.contains("NOT armed"),
            "an armed poller ({seconds}s) must never render the not-armed warning, got {:?}",
            line.text
        );
        assert!(
            !line.warning,
            "an armed poller ({seconds}s) must not warn at all, got {line:?}"
        );
    }
}

/// Plan test 5. Polling outranks exposure: it is the path Curie itself drives,
/// so an install with both is delivering, not merely unconfirmed.
#[test]
fn polling_wins_over_exposure() {
    let d = assess(&facts(Some("ingress curie.example.com"), Some(60.0), None));
    assert_eq!(
        d,
        Delivery::Polling { seconds: 60.0 },
        "polling is stronger evidence than exposure and must win, got {d:?}"
    );
    assert!(
        !matches!(d, Delivery::ExposedUnverified { .. }),
        "an install with the poller armed must not be reported as merely exposed"
    );
}

/// Plan test 7. The boundary against the API's own `> 0` gate. `None` is the
/// key never observed on a successful read, which is chart-default 0.
#[test]
fn zero_negative_nan_and_absent_intervals_are_not_polling() {
    for interval in [
        Some(0.0),
        Some(-1.0),
        Some(-0.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ] {
        let d = assess(&facts(None, interval, None));
        assert_eq!(
            d,
            Delivery::NotArmed,
            "interval {interval:?} must not count as polling (the API gates on `> 0`), got {d:?}"
        );
    }
}

/// Helm records `--set-string api.commitPollIntervalSeconds="60"` as a JSON
/// string. A number-only read would report an armed install as not armed, so
/// both encodings must parse identically and feed the same assessment.
///
/// #2496 fixer: the reader now answers with three outcomes rather than
/// `Option<f64>`. ABSENT and PRESENT-BUT-UNREADABLE were folded together, and
/// both then rendered `NotArmed` at the call sites -- a verdict about a value
/// nobody could read, which is this ticket's own defect one level down. The
/// null / unparseable expectations below therefore changed from "not observed,
/// same as absent" to `Unreadable`, which the observers turn into a discovery
/// failure. The number/string parity and the absent case are unchanged.
#[test]
fn string_and_number_intervals_parse_and_assess_identically() {
    let doc = |value: serde_json::Value| serde_json::json!({ "api": { "commitPollIntervalSeconds": value } });
    let as_number = read_poll_interval(&doc(serde_json::json!(60)));
    let as_string = read_poll_interval(&doc(serde_json::json!("60")));
    assert_eq!(
        as_number,
        PollReading::Observed(60.0),
        "a numeric interval must parse, got {as_number:?}"
    );
    assert_eq!(
        as_string, as_number,
        "--set-string records the interval as a JSON string and must parse the same \
         as a number, got {as_string:?} vs {as_number:?}"
    );
    let PollReading::Observed(from_string) = as_string else {
        panic!("a string-recorded interval must be observed, got {as_string:?}")
    };
    assert_eq!(
        assess(&facts(None, Some(from_string), None)),
        Delivery::Polling { seconds: 60.0 },
        "a string-recorded interval must arm polling, not read as not-armed"
    );

    assert_eq!(
        read_poll_interval(&serde_json::json!({ "api": {} })),
        PollReading::Absent,
        "an absent key on a readable document is the chart-default shape, not zero"
    );
    assert_eq!(
        read_poll_interval(&serde_json::json!({})),
        PollReading::Absent,
        "no api block at all is still a readable document with the key absent"
    );
    for unreadable in [
        serde_json::Value::Null,
        serde_json::json!(true),
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!("not-a-number"),
    ] {
        let reading = read_poll_interval(&doc(unreadable.clone()));
        assert_eq!(
            reading,
            PollReading::Unreadable,
            "a present but uninterpretable value ({unreadable}) must not collapse into \
             absent -- it becomes a discovery failure, got {reading:?}"
        );
    }
    // A "successful" helm read can still hand back a document nobody can read:
    // `fetch_helm_values` passes a null body through as `Ok(Some(null))`.
    for not_a_document in [
        serde_json::Value::Null,
        serde_json::json!([]),
        serde_json::json!(7),
    ] {
        assert_eq!(
            read_poll_interval(&not_a_document),
            PollReading::Unreadable,
            "a computed payload that is not an object establishes nothing ({not_a_document})"
        );
    }
    assert_eq!(
        read_poll_interval(&serde_json::json!({ "api": 3 })),
        PollReading::Unreadable,
        "an api node that is not an object is unreadable, not an absent key"
    );
}

// ---------------------------------------------------------------------------
// Wording guard and the bind line (plan tests 8, 9)
// ---------------------------------------------------------------------------

/// Plan test 8 / AC7's honesty rule. Nothing this module renders may claim a
/// push was PROVEN to deploy -- Curie observes configuration, never delivery.
#[test]
fn no_renderer_claims_a_push_was_proven() {
    const FORBIDDEN: &[&str] = &[
        "will deploy",
        "pushes deploy this agent",
        "pushes to it deploy this agent",
        "verified",
        "confirmed delivery",
        "guaranteed",
    ];
    for text in every_rendered_string() {
        let lower = text.to_ascii_lowercase();
        for needle in FORBIDDEN {
            assert!(
                !lower.contains(needle),
                "rendered string claims proven delivery via {needle:?}: {text:?}"
            );
        }
    }
}

/// Plan test 9. The bind line states a BINDING fact. It keeps #1212's intent
/// (a successful bind is still visibly confirmed) while dropping the delivery
/// promise `deploy this agent` that this ticket is about.
#[test]
fn bind_note_states_binding_not_delivery() {
    let note = bind_note("acme/widgets");
    assert!(
        note.contains("acme/widgets"),
        "#1212: a successful bind must still name the repo it bound, got {note:?}"
    );
    assert!(
        note.contains("bound"),
        "the line must state the binding fact, got {note:?}"
    );
    assert!(
        !note.contains("deploy this agent"),
        "the bind line must no longer promise a deploy -- that is the reported bug, got {note:?}"
    );
}

/// AC5, as far as a pure surface can pin it. The local tier passes `None`
/// (delivery was never assessed, because there is no release to read) and must
/// therefore print NOTHING -- not an Unknown, not a caveat. `Some(Unknown)`
/// stays a real, distinct state.
#[test]
fn local_tier_none_delivery_emits_no_line() {
    assert_eq!(
        render_optional(None),
        None,
        "the local tier must emit no delivery line at all"
    );
    assert!(
        render_optional(Some(&Delivery::Unknown {
            reason: "no release".to_string()
        }))
        .is_some(),
        "Some(Unknown) is assessed-but-undetermined and MUST still speak -- collapsing it \
         into silence would recreate the not-assessed/undetermined conflation"
    );
    for d in [
        Delivery::NotArmed,
        Delivery::Polling { seconds: 60.0 },
        Delivery::ExposedUnverified {
            how: "nodePort 30080".to_string(),
        },
    ] {
        assert_eq!(
            render_optional(Some(&d)),
            Some(render(&d)),
            "an assessed delivery must render exactly the same line as render(), got {d:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Doctor (plan tests 12, 13, 14) -- the `webhook` check becomes the delivery
// check; the id is deliberately unchanged so no skipped-list loop drifts.
// ---------------------------------------------------------------------------

/// A reachable cluster whose release is installed and serving.
///
/// This has to describe a serving release, not just a `target`: doctor
/// deliberately SKIPS the delivery row on the laptop rung (no kube context) and
/// on a release it could not confirm is serving, because it must not render a
/// verdict about an install nobody could see. A fixture missing these facts
/// would assert against those skip paths instead of the delivery rule these
/// tests are about.
fn doctor_facts() -> Facts {
    Facts {
        kube_context: Some("minikube".to_string()),
        target: Some(("curie-2496".to_string(), "curie".to_string())),
        release: ReleaseProbe::Installed {
            chart: "curie-0.6.0".to_string(),
        },
        release_status: Some("deployed".to_string()),
        ready_workloads: Some(1),
        ..Default::default()
    }
}

/// Plan test 12 / AC2 in doctor. A private install with the poller armed is
/// delivering. Calling it broken is the false alarm that teaches operators to
/// ignore doctor -- the same argument the existing NodePort comment makes.
#[test]
fn webhook_check_is_ok_when_polling_is_configured() {
    let f = Facts {
        api_exposure: None,
        commit_poll_interval_seconds: Some(60.0),
        ..doctor_facts()
    };
    let checks = evaluate(&f);
    let check = find(&checks, "webhook");
    assert_eq!(
        check.state,
        State::Ok,
        "an armed commit poller is a working delivery path, got {check:?}"
    );
    assert!(
        check.fix.is_none(),
        "an OK check must offer no fix, got {:?}",
        check.fix
    );
    assert!(
        !check.detail.contains("no ingress and no NodePort"),
        "doctor must not report a polling install as unexposed-and-broken, got {:?}",
        check.detail
    );
}

/// Plan test 13 / AC6 -- the reported doctor half of the bug. The MISS fix
/// named ingress only, which is unusable advice for a private install.
#[test]
fn webhook_miss_fix_names_commit_poll_interval_seconds() {
    let f = Facts {
        api_exposure: None,
        commit_poll_interval_seconds: Some(0.0),
        ..doctor_facts()
    };
    let checks = evaluate(&f);
    let check = find(&checks, "webhook");
    assert_eq!(
        check.state,
        State::Missing,
        "no exposure and no poller is a genuine MISS, got {check:?}"
    );
    let fix = check
        .fix
        .as_deref()
        .expect("a MISS delivery check must carry a fix");
    assert!(
        fix.contains("api.commitPollIntervalSeconds"),
        "AC6: the fix must offer the polling path for a private install, got {fix:?}"
    );
    assert!(
        fix.contains("api.ingress.enabled=true"),
        "the existing ingress path must survive alongside it, got {fix:?}"
    );
    assert!(
        fix.contains("curie-2496") && fix.contains("-n "),
        "the fix must stay targeted at the diagnosed namespace/release (#1358), got {fix:?}"
    );
}

/// Plan test 14 / AC4 in doctor. An unreadable computed-values read is case 4:
/// doctor states observations, never a verdict about something nobody saw.
#[test]
fn webhook_check_is_skipped_not_missing_when_values_unreadable() {
    let f = Facts {
        api_exposure: None,
        commit_poll_interval_seconds: None,
        delivery_discovery_failure: Some(
            "could not read this release's computed Helm values".to_string(),
        ),
        ..doctor_facts()
    };
    let checks = evaluate(&f);
    let check = find(&checks, "webhook");
    assert_ne!(
        check.state,
        State::Missing,
        "a failed values read must not be reported as a missing delivery path, got {check:?}"
    );
    assert_eq!(
        check.state,
        State::NotApplicable,
        "an unobservable delivery path is skipped, not judged, got {check:?}"
    );
    assert!(
        check
            .detail
            .contains("could not read this release's computed Helm values"),
        "the skip must carry the reason, got {:?}",
        check.detail
    );
    assert!(
        check.fix.is_none(),
        "doctor must not hand out a fix for something it could not observe, got {:?}",
        check.fix
    );
}

/// #2496 fixer, findings 1 and 3. The check row respected all four cases; the
/// one-line VERDICT did not. It keyed off `has("webhook")`, so a SKIPPED row
/// (case 4, discovery failed) produced the same "not wired yet" sentence as a
/// genuinely unarmed install, and an `Ok` row carrying the exposed-but-
/// unverified caveat (case 3) produced `Fully wired: ... git-push deploys` --
/// a delivery promise from public exposure alone, which is exactly what this
/// ticket forbids. The rest of the install is wired in both fixtures, so the
/// delivery state is the only thing moving the verdict.
#[test]
fn the_summary_verdict_respects_all_four_delivery_cases() {
    let wired = || Facts {
        model_credential: Some("CURIE_CREDENTIALS".to_string()),
        bundle_name: Some("my-agent".to_string()),
        docker_ok: true,
        slack_app_token: true,
        slack_bot_token: true,
        clone_credential: Some("github app".to_string()),
        agents: Some(vec![("bot".to_string(), Some("acme/bot".to_string()))]),
        ..doctor_facts()
    };

    let skipped = summary(&evaluate(&Facts {
        delivery_discovery_failure: Some(
            "could not read this release's computed Helm values".to_string(),
        ),
        ..wired()
    }));
    assert!(
        !skipped.contains("not wired yet"),
        "a skipped delivery row is not a missing one, got {skipped}"
    );
    assert!(
        skipped.contains("could not be checked"),
        "a failed read must say it could not tell, got {skipped}"
    );

    let exposed = summary(&evaluate(&Facts {
        api_exposure: Some("NodePort 30799".to_string()),
        ..wired()
    }));
    assert!(
        !exposed.contains("Fully wired"),
        "exposure alone is not a delivery path and must not read as wired, got {exposed}"
    );
    assert!(
        exposed.contains("unverified"),
        "exposure alone must be hedged, got {exposed}"
    );

    let polling = summary(&evaluate(&Facts {
        commit_poll_interval_seconds: Some(60.0),
        ..wired()
    }));
    assert!(
        polling.contains("Fully wired"),
        "an armed poller IS a delivery path and must still read as wired, got {polling}"
    );

    let not_armed = summary(&evaluate(&wired()));
    assert!(
        not_armed.contains("not wired yet"),
        "a genuinely unarmed install keeps the not-wired verdict, got {not_armed}"
    );
}

/// The check id is load-bearing: a NEW id would have to be threaded through
/// four skipped-list loops, `summary` and the ready logic. Retitling is fine;
/// renumbering the surface is not.
#[test]
fn delivery_reuses_the_webhook_check_id_and_adds_no_sibling() {
    let checks = evaluate(&doctor_facts());
    assert_eq!(
        checks.iter().filter(|c| c.id == "webhook").count(),
        1,
        "exactly one delivery check, keeping id `webhook`; got ids {:?}",
        ids(&checks)
    );
    assert!(
        find(&checks, "repo-binding").id == "repo-binding",
        "the sibling repo-binding check answers BINDING and must survive unchanged"
    );
    assert_eq!(
        find(&checks, "webhook").title,
        "Push delivery",
        "the check now answers delivery, not exposure"
    );
}

/// The doctor fix suffix is built from the caller's already-targeted `up`
/// command, so the namespace/release targeting rule is stated in exactly one
/// place (AC7) rather than rebuilt here.
#[test]
fn polling_fix_suffix_reuses_the_targeted_up_command() {
    let suffix = polling_fix_suffix("curie cluster up -n curie-2496 --release curie");
    assert!(
        suffix.contains("curie cluster up -n curie-2496 --release curie"),
        "the suffix must reuse the targeted base verbatim, got {suffix:?}"
    );
    assert!(
        suffix.contains("api.commitPollIntervalSeconds"),
        "the suffix exists to name the polling key, got {suffix:?}"
    );
}

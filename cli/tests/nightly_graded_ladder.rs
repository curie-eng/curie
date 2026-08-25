//! ADR-0081 / issue #872: the nightly graded parity ladder is the workflow
//! that runs the cold-start parity ladder (`curie dev e2e-ladder`) LIVE
//! against a real model, not the sealed fake-model install `ci.yaml` runs on
//! every PR. `ci.yaml`'s cluster ladder job installs with `--fake-model`
//! (ADR-0055 bounds what that fake green means: plumbing only, never a graded
//! turn). The nightly workflow sits on the opposite side of that seam: it
//! must arm `CURIE_E2E_LIVE`, never seal the install, and carry the
//! OpenRouter credential only as the one env key the runner reads.
//!
//! Grounding for the OpenRouter/env-key claims asserted below:
//! - `docs/interfaces/model-provider/INTERFACE.md:73-76`: an `sk-or-` credential
//!   auto-selects `OPENROUTER_BASE_URL`; no `ANTHROPIC_BASE_URL` is set for this
//!   provider, and the real key travels as `ANTHROPIC_API_KEY` inside the
//!   runner -- the CLI-facing input for that credential is `CURIE_CREDENTIALS`.
//! - `cli/src/ops.rs:529-532`: `--allow-egress-host` takes the provider
//!   KEYWORD `openrouter` (resolved to `openrouter.ai` at install time), never
//!   a bare hostname like `openrouter.ai` itself.
//!
//! This file is a text-contract test against the workflow YAML AS TEXT (no
//! `serde_yaml`, no new dependency -- std `fs` only), so it fails today
//! because `.github/workflows/nightly-graded-ladder.yaml` does not exist yet.
//! Each assertion targets a user-visible CI contract (arms live, opens
//! egress, never seals, never leaks the secret), not Rust internals, so it
//! survives a rename or reformat of the workflow as long as the contract
//! holds. Deleting the workflow fails every assertion below except the CI
//! sibling anchor (assertion group 3), which stays green against the
//! existing `ci.yaml` and only breaks if someone arms `ci.yaml`'s fake seal
//! off, proving the two workflows are pinned to opposite sides of the seam.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;

/// Read a workflow file's raw text, or an empty string when it does not exist
/// yet. Assertions on an empty string fail with their own readable messages
/// rather than panicking on a missing file, so a missing nightly workflow
/// surfaces as a normal test failure naming the violated contract.
fn workflow_text(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../.github/workflows")
        .join(name);
    fs::read_to_string(path).unwrap_or_default()
}

fn nightly() -> String {
    workflow_text("nightly-graded-ladder.yaml")
}

fn ci() -> String {
    workflow_text("ci.yaml")
}

fn ladder() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/e2e-ladder.sh");
    fs::read_to_string(path).unwrap_or_default()
}

fn chart_runtime_e2e() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../scripts/chart-runtime-e2e.sh");
    fs::read_to_string(path).unwrap_or_default()
}

fn count_lines_containing(text: &str, needle: &str) -> usize {
    text.lines().filter(|line| line.contains(needle)).count()
}

#[test]
fn chart_runtime_observes_each_refusal_series_and_keeps_healthy_failures_zero() {
    let text = chart_runtime_e2e();
    let healthy = text
        .split_once("banner \"ASSERT OTel healthy reception and delivery\"")
        .and_then(|(_, tail)| {
            tail.split_once("banner \"ASSERT backend outage queues and reports failure\"")
        })
        .map(|(case, _)| case)
        .expect("chart runtime must retain bounded healthy and outage controls");
    let overflow = text
        .split_once(
            "banner \"ASSERT tiny persistent queue overflows loudly and loses bounded excess\"",
        )
        .map(|(_, case)| case)
        .expect("chart runtime must retain the bounded overflow control");

    for metric in [
        "otelcol_receiver_refused_spans",
        "otelcol_receiver_refused_log_records",
        "otelcol_receiver_refused_metric_points",
    ] {
        assert!(text.contains(metric), "chart runtime omitted {metric}");
    }
    assert!(healthy.contains(
        "assert_metrics_zero \"$OTEL_COLLECTOR_SERVICE\" \"\" \"${OTELCOL_REFUSED_METRICS[@]}\""
    ));
    assert!(
        healthy.contains(
            "assert_metrics_zero \"$OTEL_COLLECTOR_SERVICE\" \"$OTEL_EXPORTER\" \\\n  \"${OTELCOL_FAILED_METRICS[@]}\""
        ),
        "the healthy path must require every Collector send_failed series to be present and zero"
    );
    assert!(
        !healthy.contains("OTELCOL_ENQUEUE_FAILED_METRICS"),
        "Collector 0.119 does not expose enqueue_failed series before an enqueue failure occurs"
    );
    assert!(
        overflow.contains(
            "wait_metrics_positive \"$OTEL_COLLECTOR_SERVICE\" \"$OTEL_EXPORTER\" \"${OTELCOL_ENQUEUE_FAILED_METRICS[@]}\""
        ),
        "the bounded overflow path must still require every enqueue_failed signal series to move"
    );
}

#[test]
fn chart_runtime_sustains_a_ready_bounded_outage_before_restart_and_overflow() {
    let text = chart_runtime_e2e();
    let bounded_function = text
        .split_once("assert_sustained_outage_bounded()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must retain the sustained-outage assertion function");
    let storage_observer = text
        .split_once("create_otlp_storage_observer()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must create a task-owned Collector PVC storage observer");
    let bounded = text
        .find("assert_sustained_outage_bounded \"$OUTAGE_COLLECTOR_POD\"")
        .expect("outage path must execute the bounded readiness control");
    let restart = text
        .find("ASSERT Collector restart retains the queued signals")
        .expect("chart runtime must retain the restart proof");
    let overflow = text
        .find("ASSERT tiny persistent queue overflows loudly")
        .expect("chart runtime must retain the overflow proof");
    assert!(bounded < restart && restart < overflow);
    assert!(text.contains("stopped being Ready during sustained exporter outage"));
    assert!(text.contains("Collector restarted during sustained exporter outage"));
    assert!(text.contains("Collector was OOMKilled during sustained exporter outage"));
    assert!(text.contains(".status.containerStatuses[0].restartCount"));
    assert!(text.contains("0 < value <= capacity"));
    assert!(text.contains("OTEL_STORAGE_OBSERVER=\"e2e-otel-storage-observer\""));
    assert!(text.contains("\ncreate_otlp_storage_observer\n"));
    assert!(storage_observer.contains("name: $OTEL_STORAGE_OBSERVER"));
    assert!(storage_observer.contains("app.kubernetes.io/component: e2e-otel-storage-observer"));
    assert!(storage_observer.contains("app.kubernetes.io/instance: $RELEASE"));
    assert!(storage_observer.contains("claimName: $COLLECTOR_PVC"));
    assert!(storage_observer.contains("mountPath: /var/lib/otelcol"));
    assert!(storage_observer.contains("readOnly: true"));
    let observer_exec = bounded_function
        .find("kubectl exec \"$OTEL_STORAGE_OBSERVER\"")
        .expect("the sustained-outage path must measure storage through its PVC observer");
    let usage = bounded_function
        .find("du -sk /var/lib/otelcol")
        .expect("the sustained-outage path must keep its durable PVC usage measurement");
    assert!(
        observer_exec < usage,
        "the durable PVC usage measurement must run through the task-owned storage observer"
    );
    assert!(
        !bounded_function.contains("kubectl exec \"$pod\" -n \"$NAMESPACE\" -- sh"),
        "the distroless Collector pod must not be used to run sh for PVC measurement"
    );
    assert!(
        !bounded_function.contains("kubectl exec \"$pod\" -n \"$NAMESPACE\" -- du"),
        "the distroless Collector pod must not be used to run du for PVC measurement"
    );
    let observer_delete = text
        .find("kubectl delete pod \"$OTEL_STORAGE_OBSERVER\"")
        .expect("the PVC observer must release its read-only mount before Collector restart");
    let collector_delete = text
        .find("kubectl delete pod \"$OLD_COLLECTOR_POD\"")
        .expect("chart runtime must retain the explicit Collector pod restart");
    assert!(
        bounded < observer_delete && observer_delete < collector_delete,
        "the storage observer must be deleted after bounded outage measurement and before the Collector pod restart"
    );
    assert!(text.contains("beyond declared ${bound_kib}Ki bound"));
    assert!(text.contains("while retrying a fixed bounded queue"));
}

#[test]
fn chart_runtime_otel_probe_and_metrics_observer_use_release_identity() {
    let text = chart_runtime_e2e();
    let sender = text
        .split("send_otlp_triplet()")
        .nth(1)
        .expect("OTLP triplet sender must remain in the chart runtime harness");
    assert!(sender.contains(
        "template:\n    metadata:\n      labels:\n        app.kubernetes.io/name: curie\n        app.kubernetes.io/instance: $RELEASE"
    ));
    assert!(sender.contains("app.kubernetes.io/component: e2e-otel-probe"));

    assert!(text.contains("ASSERT OTel healthy reception and delivery"));

    assert!(text.contains(
        "name: $OTEL_OBSERVER\n  labels:\n    app.kubernetes.io/name: curie\n    app.kubernetes.io/instance: $RELEASE"
    ));
    assert!(text.contains("metrics_snapshot \"$OTEL_COLLECTOR_SERVICE\""));
}

#[test]
fn chart_runtime_falsifies_collector_metrics_ingress_policy() {
    let text = chart_runtime_e2e();
    assert!(
        text.contains("OTEL_UNSELECTED_OBSERVER=\"e2e-otel-unselected-observer\""),
        "chart runtime must create a distinct same-namespace observer that the configured peer does not select"
    );
    assert!(
        text.contains("OTEL_METRICS_TEST_ALLOW=\"e2e-otel-metrics-test-allow\""),
        "chart runtime must own a temporary allow policy for the falsifiability control"
    );
    assert!(
        text.contains("security:\n  otelCollectorNetworkPolicy:\n    metricsIngress:"),
        "the runtime overlay must configure the selected observer through the chart's standard NetworkPolicyPeer surface"
    );

    let resources = text
        .split_once("create_otlp_probe_resources()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must retain its task-owned OTLP probe resources");
    assert!(resources.contains("name: $OTEL_UNSELECTED_OBSERVER"));
    assert!(resources.contains("app.kubernetes.io/instance: $RELEASE"));
    assert!(resources.contains("app.kubernetes.io/component: e2e-otel-unselected-observer"));

    let proof = text
        .split_once("assert_collector_metrics_network_policy()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must retain a bounded Collector metrics policy proof");
    let selected_positive = proof
        .find("metrics_scrape_from \"$OTEL_OBSERVER\"")
        .expect("selected observer must positively scrape Collector self-metrics");
    let unselected_negative = proof
        .find("assert_metrics_scrape_denied \"$OTEL_UNSELECTED_OBSERVER\"")
        .expect("unselected same-namespace observer must be denied before the control allow");
    let temporary_allow = proof
        .find("name: $OTEL_METRICS_TEST_ALLOW")
        .expect("proof must apply a temporary targeted NetworkPolicy allow");
    let unselected_positive = proof[temporary_allow..]
        .find("metrics_scrape_from \"$OTEL_UNSELECTED_OBSERVER\"")
        .map(|offset| temporary_allow + offset)
        .expect("the denied observer must scrape after only its targeted allow is applied");
    let remove_allow = proof
        .find("kubectl delete networkpolicy \"$OTEL_METRICS_TEST_ALLOW\"")
        .expect("proof must remove its temporary targeted allow");
    assert!(
        selected_positive < unselected_negative
            && unselected_negative < temporary_allow
            && temporary_allow < unselected_positive
            && unselected_positive < remove_allow,
        "metrics policy proof must observe allow, deny, targeted re-allow, then cleanup in causal order"
    );
    assert!(proof.contains("podSelector:"));
    assert!(proof.contains("app.kubernetes.io/component: otel-collector"));
    assert!(proof.contains("from:"));
    assert!(proof.contains("app.kubernetes.io/component: e2e-otel-unselected-observer"));
    assert!(proof.contains("port: 8888"));
    assert!(text.contains("\nassert_collector_metrics_network_policy\n"));
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write harness executable");
    let mut permissions = fs::metadata(path)
        .expect("read harness metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("mark harness executable");
}

// --- Assertion group 1: arms the GRADED path -------------------------------

/// The nightly workflow must arm live grading with the exact double-quoted
/// form, never a bare or unquoted `1` that a YAML parser could read as an
/// integer instead of the string the ladder script compares against.
#[test]
fn nightly_arms_live_grading_with_the_exact_quoted_form() {
    let text = nightly();
    assert!(
        text.contains("CURIE_E2E_LIVE: \"1\""),
        "the nightly workflow must arm CURIE_E2E_LIVE with the exact quoted \
         form `CURIE_E2E_LIVE: \"1\"` so the ladder runs live, not fake; \
         file contents:\n{text}"
    );
}

/// Every line referencing the OpenRouter secret must also assign
/// `CURIE_CREDENTIALS:` on that same line, so the secret reaches the ladder
/// only through the one env key the CLI reads (never bare, never aliased to
/// a differently-named env var that could accidentally land on a `run:` line
/// elsewhere).
#[test]
fn nightly_secret_reaches_the_ladder_only_as_curie_credentials() {
    let text = nightly();
    assert!(
        text.contains("secrets.OPENROUTER_API_KEY"),
        "the nightly workflow must reference secrets.OPENROUTER_API_KEY at \
         least once to supply the live model credential; file contents:\n{text}"
    );
    for line in text.lines() {
        if line.contains("secrets.OPENROUTER_API_KEY") {
            assert!(
                line.contains("CURIE_CREDENTIALS:"),
                "every line referencing secrets.OPENROUTER_API_KEY must also \
                 assign CURIE_CREDENTIALS: on that same line, so the secret \
                 reaches the ladder only as that one env key: {line}"
            );
        }
    }
}

/// The model default `z-ai/glm-5.2` must appear on a line that also names
/// `CURIE_MODEL`, so the graded default is wired as the model the ladder
/// actually calls, not just mentioned in a comment.
#[test]
fn nightly_wires_the_glm_default_model_via_curie_model() {
    let text = nightly();
    let has_model_line = text
        .lines()
        .any(|line| line.contains("z-ai/glm-5.2") && line.contains("CURIE_MODEL"));
    assert!(
        has_model_line,
        "the nightly workflow must have a line containing both the model id \
         z-ai/glm-5.2 and the CURIE_MODEL env key, wiring the default model \
         into the ladder; file contents:\n{text}"
    );
}

/// The nightly ladder must cover both tier sets: the fast `skill,local` rungs
/// and the separate `cluster` rung. Accept either quoted or unquoted YAML
/// scalars by checking substrings rather than exact-matching the whole line.
#[test]
fn nightly_covers_both_the_skill_local_and_cluster_tier_sets() {
    let text = nightly();
    assert!(
        text.contains("skill,local"),
        "the nightly workflow must set CURIE_E2E_TIERS to a value containing \
         `skill,local` so the fast rungs run graded too; file contents:\n{text}"
    );
    let has_cluster_tiers = text.lines().any(|line| {
        let normalized: String = line.split_whitespace().collect();
        normalized.contains("CURIE_E2E_TIERS:cluster")
            || normalized.contains("CURIE_E2E_TIERS:\"cluster\"")
    });
    assert!(
        has_cluster_tiers,
        "the nightly workflow must have a CURIE_E2E_TIERS: cluster (quoted or \
         unquoted) line covering the cluster rung separately from \
         skill,local; file contents:\n{text}"
    );
}

// --- Assertion group 2: cluster graded install -----------------------------

/// The cluster install must open egress to the `openrouter` provider keyword
/// (resolved to `openrouter.ai` at install time by `cli/src/ops.rs:529-532`),
/// and the workflow must never contain the sealed-install flag anywhere --
/// proving the cluster rung is graded, not fake.
#[test]
fn nightly_cluster_install_opens_openrouter_egress_and_never_seals() {
    let text = nightly();
    assert!(
        text.contains("--allow-egress-host openrouter"),
        "the nightly workflow's cluster install must open egress with \
         `--allow-egress-host openrouter` (the provider keyword, not a bare \
         hostname) so the graded model call is reachable; file contents:\n{text}"
    );
    assert!(
        !text.contains("--fake-model"),
        "the nightly workflow must never seal the install with --fake-model \
         anywhere in the file (including comments); a sealed install cannot \
         be the graded ladder; file contents:\n{text}"
    );
}

// --- Assertion group 3: sibling / negative parity (mandatory) --------------

/// `ci.yaml`'s cluster ladder job DOES seal its install with `--fake-model`.
/// This is the sibling anchor: it pins the two workflows on opposite sides of
/// the fake/graded seam. This assertion passes today against the existing
/// `ci.yaml` and fails only if someone arms ci.yaml graded (removing its
/// `--fake-model`) or de-arms the nightly ladder, collapsing the seam this
/// whole test file exists to guard.
#[test]
fn ci_yaml_still_seals_its_cluster_install_with_fake_model() {
    let text = ci();
    assert!(
        text.contains("--fake-model"),
        "ci.yaml must still contain --fake-model in its cluster ladder job; \
         if this ever fails, either ci.yaml was accidentally armed graded or \
         the fake/graded seam this test guards has collapsed; file \
         contents:\n{text}"
    );
}

// --- Assertion group 4: #632 secret posture --------------------------------

/// The workflow must declare least-privilege `permissions:` with
/// `contents: read`, so the nightly job cannot write back to the repo.
#[test]
fn nightly_declares_least_privilege_contents_read_permissions() {
    let text = nightly();
    assert!(
        text.contains("permissions:"),
        "the nightly workflow must declare a permissions: block (least \
         privilege, #632); file contents:\n{text}"
    );
    assert!(
        text.contains("contents: read"),
        "the nightly workflow's permissions: block must include \
         `contents: read`; file contents:\n{text}"
    );
}

/// Every `actions/checkout` use must be paired with
/// `persist-credentials: false`, so the checkout-injected token cannot
/// override a differently-scoped credential used later in the job (the same
/// class of hazard the checkout-credentials learning documents). Counting
/// occurrences (rather than requiring exact adjacency) keeps the assertion
/// robust to step reordering while still catching a checkout step that lacks
/// the setting entirely.
#[test]
fn nightly_pairs_every_checkout_with_persist_credentials_false() {
    let text = nightly();
    let checkout_count = count_lines_containing(&text, "uses: actions/checkout");
    assert!(
        checkout_count > 0,
        "the nightly workflow must use actions/checkout at least once; file \
         contents:\n{text}"
    );
    let persist_false_count = count_lines_containing(&text, "persist-credentials: false");
    assert!(
        persist_false_count >= checkout_count,
        "every actions/checkout use ({checkout_count}) must be paired with \
         persist-credentials: false ({persist_false_count} found); a bare \
         checkout leaves the job token in global git config for later steps \
         to pick up; file contents:\n{text}"
    );
}

/// The OpenRouter secret must never be echoed on a `run:` line. Combined with
/// assertion group 1's "only as CURIE_CREDENTIALS:" check, this closes off
/// the one remaining way the secret could leak into job logs.
#[test]
fn nightly_never_echoes_the_openrouter_secret_on_a_run_line() {
    let text = nightly();
    for line in text.lines() {
        if line.contains("secrets.OPENROUTER_API_KEY") {
            assert!(
                !line.contains("run:"),
                "the OpenRouter secret must never appear on a `run:` line \
                 (it would be echoed into job logs): {line}"
            );
        }
    }
}

// --- Assertion group 5: the eval-block TEXT contracts ----------------------
//
// The four tests below are exact-substring assertions on the ladder script's
// source text, and they are therefore WEAK BY CONSTRUCTION: a substring match
// proves no behavior at all, and a reformat that preserves behavior breaks
// them. They are kept, not deleted, because they encode a real CI contract --
// that the graded nightly path still fires the LIVE tier eval at every rung --
// and deleting them would remove the only guard on it. The real coverage for
// parity lives in the EXECUTING controls at the bottom of this file (assertion
// group 6), which run the script rather than read it.
//
// Each region now also carries the `--dry-run` suite-parity check, which runs
// in fake mode as well as live (no turn, no grading, no stack: the dry-run plan
// line is resolved by the tier's own frozen suite loader). At the cluster rung
// the dry-run and the live eval share one `eval_args` array so `--listen-host`
// is forwarded to BOTH, which is why the two calls sit adjacent. `--json` is the
// one flag NOT in that array: both calls pass it themselves, for different
// reasons (#1602's auditable green, and a machine-readable dry-run plan).
//
// The cluster rung's grade is REPORT ONLY (#1603), so its text contract here
// asserts the eval still RUNS and still runs under `--json`; the non-fatal half
// is proved by the executing control at the bottom of this file, which runs the
// ladder against a stub evaluator that exits 42.

#[test]
fn live_local_rung_grades_the_deployed_weather_cases() {
    let text = ladder();
    let finalized = text
        .find(r#"assert_finalized_reply "local" "$out""#)
        .expect("the local rung must assert its finalized reply");
    let telemetry = text[finalized..]
        .find(r#"assert_local_otel_healthy_turn "$healthy_before""#)
        .map(|offset| finalized + offset)
        .expect("the local rung must prove healthy turn telemetry after the reply");
    let contract = r#"echo "=== curie local eval --dry-run (suite parity) ==="
    local eval_args=(local eval)
    if [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle/evals/cases.json")
    fi
    assert_suite "local" "$(cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval ==="
        (cd "$WORKDIR/bundle" && "$BIN" "${eval_args[@]}")
    fi"#;
    let eval = text[telemetry..]
        .find(contract)
        .map(|offset| telemetry + offset)
        .expect("the local rung must run its suite check and live eval");
    assert!(
        finalized < telemetry && telemetry < eval,
        "the live local rung must prove telemetry after its plumbing assertion, \
         then run local eval against the deployed weather bundle cases with the \
         suite-parity dry-run check in front of it"
    );
}

#[test]
fn live_local_release_rung_grades_its_own_weather_cases_copy() {
    let text = ladder();
    let contract = r#"assert_finalized_reply "local-release" "$out"

    echo
    echo "=== curie local eval --dry-run (suite parity, release compose stack) ==="
    local eval_args=(local eval)
    if [[ ! -f "$WORKDIR/bundle-release/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle-release/evals/cases.json")
    fi
    assert_suite "local-release" "$(cd "$WORKDIR/bundle-release" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval (release compose stack) ==="
        (cd "$WORKDIR/bundle-release" && "$BIN" "${eval_args[@]}")
    fi"#;
    assert!(
        text.contains(contract),
        "the live local release rung must run local eval after its plumbing \
         assertion against its own deployed weather bundle cases copy, with \
         the suite-parity dry-run check in front of it; \
         ladder contents:\n{text}"
    );
}

#[test]
fn live_cluster_rung_runs_weather_cases_with_the_message_listen_host() {
    let text = ladder();
    // The shared `eval_args` array and the dry-run call it feeds. Scoped to the
    // array plus that one call rather than the whole block, because the block
    // carries the #1602/#1603 rationale comments and a reworded comment must not
    // red this contract.
    let contract = r#"local eval_args=(cluster eval)
    if [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle/evals/cases.json")
    fi
    if [[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
        eval_args+=(--listen-host "$CURIE_E2E_LISTEN_HOST")
    fi
    assert_suite "cluster" "$(cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}" --dry-run)""#;
    assert!(
        text.contains(r#"assert_finalized_reply "cluster" "$out""#),
        "the live cluster rung must keep its plumbing assertion; ladder \
         contents:\n{text}"
    );
    assert!(
        text.contains(contract),
        "the live cluster rung must resolve its suite through one shared \
         eval_args array and forward the message listen host to the \
         suite-parity dry-run; ladder contents:\n{text}"
    );
    assert!(
        text.contains(r#"if ! (cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}"); then"#),
        "the live cluster rung must still RUN cluster eval against the deployed \
         weather bundle cases and forward the message listen host -- through the \
         SAME array as the dry-run, so neither call can lose it -- and report \
         only (#1603) means the grade is non-fatal, never that the step was \
         deleted; ladder contents:\n{text}"
    );
}

/// The cluster rung's eval must run in `--json` mode. The human table prints a
/// reply only for a RED case, so a green would carry no evidence of HOW it was
/// earned. The weather case now requires the fetch capability through its
/// trajectory sidecar; its regex grades answer formatting only and cannot pass
/// the full case alone. The json payload carries `output` for every case, pass
/// included, which is what keeps the report only grade auditable in
/// the job log. Asserted for the cluster rung alone, since the sibling
/// assertions above pin the other rungs to the plain table.
///
/// The second half is the reconciliation of #1602 with the suite-parity dry-run
/// that shares this rung's `eval_args`: BOTH calls need `--json` (auditability of
/// a green here, a machine-readable plan there), so it is passed at each call
/// site and must stay OUT of the shared array -- inside it, every invocation
/// would carry `--json` twice.
#[test]
fn live_cluster_rung_emits_the_graded_reply_for_passing_cases() {
    let text = ladder();
    assert!(
        text.contains(r#"if ! (cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}"); then"#),
        "the live cluster rung must run its eval with `--json` so a PASSING \
         case's reply text lands in the job log; without it a dishonest green \
         (a fabricated temperature) is indistinguishable from an honest one; \
         ladder contents:\n{text}"
    );
    assert!(
        !text.contains(r#"local eval_args=(--json cluster eval"#),
        "`--json` must be passed at each cluster eval call site, never stored in \
         the shared eval_args array: in the array both the dry-run and the live \
         grade would pass it twice, and neither invocation would be the one the \
         executing controls below drive; ladder contents:\n{text}"
    );
}

// --- Assertion group 6: the EXECUTING parity controls -----------------------
//
// Everything below runs the real `cli/scripts/e2e-ladder.sh` against a stub
// `curie`, `docker` and `kubectl`, so it exercises the ladder's own parity
// helpers rather than reading its source. No Docker daemon, no cluster, no
// credential (the one live-mode control supplies a dummy CURIE_CREDENTIALS,
// which apply_model_mode only checks for presence).
//
// The design that makes each negative control attributable: ONE stub body,
// configured entirely through STUB_* env vars, so every negative control is the
// positive control's configuration with exactly ONE knob moved. A non-zero exit
// therefore cannot come from an unrelated breakage -- the positive control
// proves the un-moved configuration is green -- and each control additionally
// requires the operator-facing message to name the divergence it injected.
//
// All of these assert on exit codes and operator-facing message text, never on
// the ladder's internal shell identifiers, so renaming `PARITY_DIGEST` or
// `assert_bundle_identity` leaves them passing.

/// The ids the stub deploy receipt reports. They are also what the stubbed
/// `GET /deployments` read must agree with, which is the whole point of the
/// sole-active-deployment control.
const AGENT_ID: &str = "6f3d1c2a-0000-4000-8000-000000000001";
const VERSION_ID: &str = "6f3d1c2a-0000-4000-8000-000000000002";
const DEPLOYMENT_ID: &str = "6f3d1c2a-0000-4000-8000-000000000003";
/// The stale `prod` row the second-active-deployment control injects. `prod`
/// because the worker's runtime binding prefers it over recency, so this is the
/// exact row that would silently serve the turn while the ladder reported the
/// digest of the `dev` bundle it just uploaded.
const STALE_PROD_DEPLOYMENT_ID: &str = "6f3d1c2a-0000-4000-8000-000000000099";

/// A digest value pinned by the digest-divergence control at the local rung.
const PINNED_DIGEST: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
/// The divergent digest the cluster rung reports in the digest-divergence
/// control.
const DIVERGENT_DIGEST: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

/// The stub `curie`, `docker` and `kubectl` the executing controls drive the
/// ladder with. One body for all of them: behavior comes from STUB_* env vars
/// so the tests differ by configuration, not by a second stub.
///
/// Every arm matches only the argv shape the scripts actually issue. An
/// invocation variant nobody issues falls through to `exit 97`, deliberately: an
/// arm that tolerates a shape the script does not use is a compatibility path,
/// and it would keep a control green while hiding the regression that started
/// issuing that shape (the `--json`-less deploy is the exact case -- the stub
/// answers with JSON either way, so tolerating it would hide a deploy that
/// stopped asking for a receipt).
fn write_ladder_stubs(dir: &Path) {
    fs::write(
        dir.join("deploy-provider-wire.json"),
        include_str!("data/deploy-provider-wire.json"),
    )
    .expect("write deploy provider fixture");

    write_executable(
        &dir.join("curie"),
        r#"#!/bin/sh
set -u

if [ -n "${STUB_INVOCATION_LOG:-}" ]; then
    printf '%s\n' "$*" >> "$STUB_INVOCATION_LOG"
fi

# `--plugin-dir <dir>` is the last pair on every deploy the ladder makes, so the
# final argument is the bundle directory that rung packed.
bundle_dir=""
for arg in "$@"; do bundle_dir="$arg"; done

# The `--name <name>` value, read off argv: the #747 leftover-runner case asserts
# that `skill up`'s refusal names the exact container an operator must clear.
name=""
prev=""
for arg in "$@"; do
    if [ "$prev" = "--name" ]; then name="$arg"; fi
    prev="$arg"
done

# A CONTENT-derived digest, not a canned one: the default makes two rungs that
# packed the same tree agree by construction and two rungs that packed different
# trees disagree by construction. That is what lets the case-ids-only control
# prove the digest carries the case-id claim, and what lets the positive control
# compare three independently derived digests instead of one canned constant.
#
# Hashes path plus bytes for every regular file, mtimes excluded (the ladder
# normalizes those itself) and `.curie` excluded (the real packer excludes it,
# cli/src/bundle.rs). The Rust side computes the same value for the pristine
# weather bundle in weather_bundle_sha256(); the two must stay in step.
sha_of_bundle() {
    python3 -c 'import hashlib, os, sys
root = sys.argv[1]
digest = hashlib.sha256()
for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = sorted(d for d in dirnames if d != ".curie")
    for entry in sorted(filenames):
        path = os.path.join(dirpath, entry)
        digest.update(os.path.relpath(path, root).encode())
        digest.update(open(path, "rb").read())
print(digest.hexdigest())' "$1"
}

# The `deploy_result` branch of cli/schema/deploy.schema.json, with every field
# the ladder reads: bundle.sha256, agent.id, version.id, and
# deployment.{id,environment,status}.
emit_deploy() {
    python3 -c 'import json, sys
fixture = json.load(open(sys.argv[1]))
fixture["agent"]["id"] = sys.argv[2]
fixture["version"]["id"] = sys.argv[3]
fixture["deployment"]["id"] = sys.argv[4]
fixture["bundle"]["sha256"] = sys.argv[5]
print(json.dumps(fixture, separators=(",", ":")))' \
        "$STUB_STATE/deploy-provider-wire.json" \
        "$STUB_AGENT_ID" \
        "$STUB_VERSION_ID" \
        "$STUB_DEPLOYMENT_ID" \
        "$1"
}

# The DryRunPlan shape (cli/src/ui.rs: {"dry_run":true,"plan":[lines]}) carrying
# the one plan line the tier's suite loader emits.
emit_plan() {
    printf '{"dry_run":true,"plan":["grade %s case(s) from suite \\"%s\\" against the %s tier"]}\n' "$2" "$3" "$1"
}

case "$*" in
    "--version")
        echo "curie test harness"
        ;;
    "--json try")
        # The keyless first run is deliberately disposable: its finalized fake
        # reply is the only observable result, so no project may be retained
        # in the caller's clean directory.
        printf '%s\n' '{"status":"done","finalized":true,"reply":"all done"}'
        ;;
    "try")
        # The credential-discovery E2E supplies a known-invalid credential.
        # Name its source without ever expanding its value, and create neither
        # a scaffold nor a runner before the rejection.
        echo "error: discovered credential source ANTHROPIC_API_KEY was rejected" >&2
        exit 1
        ;;
    "try --keep")
        # Graduation keeps the standard plugin shape, then the normal skill
        # commands below operate on this directory exactly as they do for a
        # user-created project. A second graduation must refuse before writing
        # the retained manifest.
        if [ -e curie-demo/.claude-plugin/plugin.json ]; then
            echo "error: curie-demo already exists" >&2
            exit 1
        fi
        mkdir -p curie-demo/.claude-plugin
        printf '%s\n' '{"name":"curie-demo"}' > curie-demo/.claude-plugin/plugin.json
        echo "stub try all done"
        ;;
    "--json cluster status")
        # The case-ids-only control's single injection point: after the local
        # rung deployed and before the cluster rung does, rewrite ONLY the case
        # ids in the bundle both rungs deploy. Suite name and case count are
        # untouched, so every dry-run plan line still matches exactly and the
        # digest is the only thing that moves.
        if [ "${STUB_MUTATE_CASE_IDS:-0}" = "1" ] && [ -f "$STUB_STATE/last_plugin_dir" ]; then
            python3 -c 'import json,sys
p = sys.argv[1]
d = json.load(open(p))
for i, c in enumerate(d["cases"]):
    c["id"] = "drifted-case-%d" % i
json.dump(d, open(p, "w"))' "$(cat "$STUB_STATE/last_plugin_dir")/evals/cases.json"
        fi
        printf '%s\n' '{"release_found":true}'
        ;;
    "skill up --plugin-dir . --image curie-runner --port 7245 --name curie-e2e-runner --fake-model")
        # The skill rung's identity surface. `skill up` packs an IMMUTABLE
        # snapshot of the source as it stands now and the runner keeps it for its
        # whole lifetime, so the digest is recorded here and replayed by
        # `skill status --json` below. That is what makes e2e.sh's two #1087 legs
        # resolve the way the real CLI resolves them: a host edit after boot is
        # invisible to the running runner, and a re-up on the edited source packs
        # a NEW digest.
        printf '%s' "$(sha_of_bundle "$PWD")" > "$STUB_STATE/skill_digest"
        echo "stub: runner up"
        # e2e.sh asserts the boot panel NAMES the model path it resolved, and
        # that it does not cry wolf about a missing credential. This arm is the
        # sealed argv, so the sealed row is the honest answer to it.
        echo "  Model    fake (offline, no credential)"
        ;;
    "skill up --fake-model")
        # The graduated demo uses the unadorned normal command. It records a
        # bundle digest and runner state so its status and teardown checks
        # exercise the same state transition as a regular project.
        mkdir -p .curie
        printf '%s' "$(sha_of_bundle "$PWD")" > "$STUB_STATE/skill_digest"
        printf '%s\n' '{"runner":"stub"}' > .curie/runner.json
        echo "stub: runner up"
        echo "  Model    fake (offline, no credential)"
        ;;
    "skill up --fake-model --plugin-dir "*"/bundle-hermetic --name curie-ladder-hermetic-"*)
        # The hermetic negative (ADR 0113): a scratch bundle declaring no
        # hosted connector boots cleanly. Must sit ABOVE the #747 arm, whose
        # pattern also matches this argv; the case then reads the session off
        # `docker inspect` and sweeps for connector containers, both answered
        # by the docker stub below.
        echo "stub: hermetic runner up"
        ;;
    "skill up --fake-model --plugin-dir "*" --name "*)
        # The #747 leftover-runner case: a taken container name must be a usage
        # refusal (exit 2) carrying the operator's own remedy, never docker's raw
        # "name is already in use" text.
        echo "error: container name conflict: a container named '$name' already exists." >&2
        echo "fix: curie skill down --name $name, then re-run." >&2
        exit 2
        ;;
    "skill status")
        echo "stub: runner running"
        ;;
    "skill status --json")
        printf '{"bundle_digest":"%s"}\n' "$(cat "$STUB_STATE/skill_digest")"
        ;;
    "skill message "*)
        echo "stub skill all done reply"
        ;;
    "--json skill eval")
        # The bundle's OWN suite, read off the bundle e2e.sh cd'd into, because
        # the skill rung is the one rung that reads its case ids back directly.
        # Sealed shape only (ADR-0055: a fake turn is reported as the non-graded
        # plumbing_ok and nothing is graded), which is what the one control that
        # drives this rung runs as; a live skill run would fall on e2e.sh's own
        # live-posture assertion rather than be silently accepted here.
        python3 -c 'import json, sys
suite = json.load(open(sys.argv[1]))
ids = [case["id"] for case in suite["cases"]]
print(json.dumps({
    "suite": suite["name"],
    "total": len(ids),
    "passed": 0,
    "failed": 0,
    "plumbing_ok": len(ids),
    "cases": [{"id": case_id, "status": "plumbing_ok"} for case_id in ids],
}))' "$PWD/evals/cases.json"
        ;;
    "skill down")
        rm -f .curie/runner.json
        echo "stub: runner down"
        ;;
    "skill down --name "*)
        echo "stub: removed the leftover runner named '$name'"
        ;;
    "local up --minimal")
        echo "stub: compose stack up"
        ;;
    "local up")
        echo "stub: full compose stack up"
        ;;
    "--json local deploy --plugin-dir "*)
        printf '%s' "$bundle_dir" > "$STUB_STATE/last_plugin_dir"
        emit_deploy "${STUB_LOCAL_SHA256:-$(sha_of_bundle "$bundle_dir")}"
        ;;
    "--json cluster deploy --plugin-dir "*)
        printf '%s' "$bundle_dir" > "$STUB_STATE/last_plugin_dir"
        emit_deploy "${STUB_CLUSTER_SHA256:-$(sha_of_bundle "$bundle_dir")}"
        ;;
    "--json local message "*)
        printf '%s\n' '{"finalized":true,"reply":"stub local weather reply"}'
        ;;
    "--json cluster message "*)
        printf '%s\n' '{"finalized":true,"reply":"stub cluster weather reply"}'
        ;;
    "--json local eval --cases "*--dry-run*)
        emit_plan local 1 weather
        ;;
    "--json local eval --dry-run")
        emit_plan local 1 weather
        ;;
    "--json cluster eval --cases "*--dry-run*)
        # Only the cluster count is an override: it is the knob the suite
        # divergence control moves. Everything else is the constant the weather
        # bundle declares.
        emit_plan cluster "${STUB_CLUSTER_PLAN_COUNT:-1}" weather
        ;;
    "--json cluster eval --dry-run")
        emit_plan cluster "${STUB_CLUSTER_PLAN_COUNT:-1}" weather
        ;;
    # The two LIVE grades, and they are deliberately asymmetric: the local rung
    # runs the plain human table, the cluster rung runs `--json` so a passing
    # case's reply is auditable in the job log (#1602). The dry-run arms above
    # must stay above this one, since `--json cluster eval --cases *` matches a
    # dry-run argv too and the first matching arm wins.
    "local eval --cases "*|"--json cluster eval --cases "*|"local eval"|"--json cluster eval")
        if [ -n "${STUB_EVAL_MARKER:-}" ]; then
            printf '%s\n' called > "$STUB_EVAL_MARKER"
        fi
        exit "${STUB_EVAL_EXIT:-0}"
        ;;
    "local down")
        echo "stub: compose stack down"
        ;;
    *)
        echo "unexpected curie invocation: $*" >&2
        exit 97
        ;;
esac
"#,
    );

    // The ladder's raw-docker uses are all either "is a stack already running"
    // or "assert nothing survived", so an unrecognized invocation returning
    // nothing is the honest default here; the two reads that carry a real
    // answer (the compose-worker selection and its env inspect) get explicit
    // arms above it.
    write_executable(
        &dir.join("docker"),
        r#"#!/bin/sh
set -u
case "$*" in
    "inspect curie-runner-local")
        # The first-run credential check refuses to touch an existing shared
        # local runner. Its harness begins with no such runner, while all
        # other inspect shapes retain their existing configured behavior.
        echo "stub docker: no such object: curie-runner-local" >&2
        exit 1
        ;;
    "inspect curie-ladder-hermetic-"*)
        # The hermetic negative's session read: assert_no_connector_containers
        # refuses an empty project scope by design, so the runner env must
        # carry a session id for the sweep to be non-vacuous.
        echo "CURIE_SESSION_ID=local-stub-hermetic"
        ;;
    "inspect "*)
        # A failed env read, on its own knob: an inspect that dies (the worker
        # exited since the `docker ps`, or a daemon blip) prints nothing, which
        # is indistinguishable from a worker carrying no CURIE_FAKE_MODEL unless
        # the probe checks the status. Under a live run that reads as "live", so
        # the unread probe would certify the mode it never managed to read.
        if [ "${STUB_DOCKER_INSPECT_EXIT:-0}" != "0" ]; then
            echo "stub docker: no such object: the container is gone" >&2
            exit "$STUB_DOCKER_INSPECT_EXIT"
        fi
        if [ -n "${STUB_FAKE_MODEL:-}" ]; then
            echo "CURIE_FAKE_MODEL=$STUB_FAKE_MODEL"
        fi
        # The default local rungs read the connector scope off the same worker
        # env: the stock weather bundle declares the netpol-probe fixture, so
        # the dual assertion derives its identity label from this release.
        echo "CURIE_RELEASE=curie"
        echo "PATH=/usr/local/bin:/usr/bin:/bin"
        ;;
    *"curietech.ai/connector=curie-weather-mcp-netpol-probe"*)
        # The dual assertion's hosted read: the stock weather bundle declares
        # the netpol-probe fixture, and this stubbed tier reports it running
        # under exactly the identity label the reconciler stamps.
        echo "curie-curie-weather-mcp-netpol-probe-1"
        ;;
    *"com.docker.compose.service=curie-worker"*)
        # Exactly one worker: the probe must refuse to guess when the scoping
        # assumption breaks, so returning one id is the only shape that lets the
        # mode assertion be reached at all.
        echo "stub-curie-worker"
        ;;
    *)
        ;;
esac
"#,
    );

    write_executable(
        &dir.join("kubectl"),
        r#"#!/bin/sh
set -u
case "$*" in
    *"CURIE_FAKE_MODEL"*)
        printf '%s' "${STUB_FAKE_MODEL:-}"
        ;;
    *)
        ;;
esac
"#,
    );
}

/// Serve one canned JSON body to every request on an ephemeral loopback port,
/// and return its base URL for `CURIE_API_URL`.
///
/// `CURIE_API_URL` is not a test-only knob: it is the documented env fallback of
/// every local verb's `--api-url` (`cli/src/main.rs:1193`), with the same
/// default the CLI itself applies, so pointing the ladder's deployments read at
/// a different API is production behavior that happens to also be the seam this
/// control needs.
fn spawn_deployments_stub(body: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind the deployments stub");
    let address = listener.local_addr().expect("read the stub address");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            // The ladder only GETs, so the request is one small buffer and its
            // content is irrelevant; it is drained only so the client sees a
            // complete exchange before the response.
            let mut request = [0u8; 2048];
            let _ = stream.read(&mut request);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    format!("http://{address}")
}

/// The `DeploymentOut` list shape (`apps/api/src/curie_api/schemas.py:654-660`)
/// for the healthy case: exactly one active row, and it is this run's.
fn one_active_deployment() -> String {
    format!(
        "[{{\"id\":\"{DEPLOYMENT_ID}\",\"agent_id\":\"{AGENT_ID}\",\
         \"version_id\":\"{VERSION_ID}\",\"environment\":\"dev\",\
         \"commit_sha\":null,\"status\":\"active\",\
         \"deployed_at\":\"2026-08-17T00:00:00Z\"}}]"
    )
}

/// This run's `dev` row plus a stale active `prod` row for the same agent: the
/// wrong-artifact-green shape.
fn two_active_deployments() -> String {
    format!(
        "[{{\"id\":\"{DEPLOYMENT_ID}\",\"agent_id\":\"{AGENT_ID}\",\
         \"version_id\":\"{VERSION_ID}\",\"environment\":\"dev\",\
         \"commit_sha\":null,\"status\":\"active\",\
         \"deployed_at\":\"2026-08-17T00:00:00Z\"}},\
         {{\"id\":\"{STALE_PROD_DEPLOYMENT_ID}\",\"agent_id\":\"{AGENT_ID}\",\
         \"version_id\":\"{VERSION_ID}\",\"environment\":\"prod\",\
         \"commit_sha\":null,\"status\":\"active\",\
         \"deployed_at\":\"2026-01-01T00:00:00Z\"}}]"
    )
}

/// Refuse to run a control that drives the local rung while something holds the
/// local reply stub's port.
///
/// `assert_stub_port_free` (`cli/scripts/e2e-ladder.sh`) is a real connect
/// probe against a HOST-GLOBAL port that the ladder deliberately does not let a
/// caller override, so no stub can satisfy it. On a box with several checkouts,
/// a concurrent `local message` or `local eval` holds it and reds every
/// local-rung control for a reason that has nothing to do with parity. Failing
/// here, with the ladder's own fix line, keeps that red diagnosable instead of
/// arriving as a confusing parity failure.
fn require_local_stub_port_free() {
    let address = "127.0.0.1:8155"
        .parse()
        .expect("parse the stub port address");
    if std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(250)).is_ok()
    {
        panic!(
            "port 8155 is already in use, so the ladder's local rung cannot \
             start and this control cannot run. This is a host collision, not \
             a parity failure. fix: stop the process holding it (another ladder \
             run, or a stale local message/eval), then re-run."
        );
    }
}

/// Run the real ladder script against the stubs in `harness`, with `envs`
/// carrying the tier selection and the STUB_* knobs this control moves.
///
/// Every variable the ladder or `e2e.sh` reads is REMOVED before `envs` is
/// applied, so the child environment is fixed by the control rather than by
/// whatever the developer happens to have exported. Two classes, both of which
/// have already bitten:
/// - `CURIE_API_KEY` / `CURIE_API_URL`: the cluster rung's runtime-binding read
///   arms itself only when a key is present, so an exported key made a
///   cluster-only control reach the default `localhost:28000` and fail
///   unreachable -- green in CI, red on one box.
/// - `CURIE_E2E_IMAGE` / `_PORT` / `_NETWORK` / `_OTEL`: these change the argv
///   `e2e.sh` builds for `skill up`, which the stub matches exactly, so an
///   exported value turns the skill rung into an `exit 97`.
fn run_ladder(harness: &Path, envs: &[(&str, &str)]) -> Output {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/e2e-ladder.sh");
    run_ladder_script(&script, harness, envs)
}

fn run_ladder_script(script: &Path, harness: &Path, envs: &[(&str, &str)]) -> Output {
    let path = format!(
        "{}:{}",
        harness.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut command = Command::new("bash");
    command
        .arg(script)
        .env("CURIE_BIN", harness.join("curie"))
        .env("PATH", path)
        .env("STUB_STATE", harness)
        .env("STUB_AGENT_ID", AGENT_ID)
        .env("STUB_VERSION_ID", VERSION_ID)
        .env("STUB_DEPLOYMENT_ID", DEPLOYMENT_ID)
        .env_remove("CURIE_E2E_TIERS")
        .env_remove("CURIE_E2E_LIVE")
        .env_remove("CURIE_E2E_LISTEN_HOST")
        .env_remove("CURIE_E2E_BUNDLE")
        .env_remove("CURIE_E2E_IMAGE")
        .env_remove("CURIE_E2E_PORT")
        .env_remove("CURIE_E2E_NETWORK")
        .env_remove("CURIE_E2E_OTEL")
        .env_remove("CURIE_FAKE_MODEL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_API_URL");
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("run the real ladder script")
}

fn run_eval_argument_control(trajectory: bool) -> (Output, String) {
    require_local_stub_port_free();
    let root = tempfile::tempdir().expect("create isolated ladder root");
    let scripts = root.path().join("cli/scripts");
    let evals = root.path().join("examples/weather/evals");
    fs::create_dir_all(&scripts).expect("create script directory");
    fs::create_dir_all(&evals).expect("create eval directory");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/e2e-ladder.sh");
    let script = scripts.join("e2e-ladder.sh");
    fs::copy(source, &script).expect("copy ladder script");
    fs::write(
        evals.join("cases.json"),
        r#"{"name":"weather","cases":[{"id":"weather","input":"weather","grader":{"kind":"contains","expected":"sunny"}}]}"#,
    )
    .expect("write cases");
    if trajectory {
        fs::write(
            evals.join("trajectory.json"),
            r#"{"specs":[{"case_id":"weather","expected":["WebSearch"],"mode":"exact","threshold":1.0}]}"#,
        )
        .expect("write trajectory sidecar");
    }

    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let invocation_log = harness.path().join("invocations.log");
    let invocation_log_value = invocation_log.display().to_string();
    let output = run_ladder_script(
        &script,
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            ("CURIE_API_URL", &api_url),
            ("STUB_FAKE_MODEL", ""),
            ("STUB_INVOCATION_LOG", &invocation_log_value),
        ],
    );
    let invocations = fs::read_to_string(invocation_log).unwrap_or_default();
    (output, invocations)
}

/// Whether some ONE line of the transcript carries every one of `needles`.
///
/// Line-scoped rather than whole-transcript, and that is the load-bearing part:
/// the stub's raw `deploy --json` receipt already echoes a digest into the
/// transcript, so a whole-transcript `contains` check for that digest passes
/// with no parity logic in the ladder at all. Requiring the digest to share a
/// line with a rung label, or two digests to share a line with each other,
/// demands an actual operator-facing report instead.
fn has_line_with(transcript: &str, needles: &[&str]) -> bool {
    transcript
        .lines()
        .any(|line| needles.iter().all(|needle| line.contains(needle)))
}

/// Both streams together, because a control asserts on the operator-facing
/// message without caring which stream carried it.
fn transcript(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The content digest of the weather bundle as it sits in the tree, which is the
/// digest the content-derived stub reports for every rung before anything
/// mutates the copy. Computed with the same path-plus-bytes walk the stub's
/// `sha_of_bundle` runs, so the two agree by construction.
fn weather_bundle_sha256() -> String {
    let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../examples/weather");
    let output = Command::new("python3")
        .arg("-c")
        .arg(
            "import hashlib, os, sys\n\
             root = sys.argv[1]\n\
             digest = hashlib.sha256()\n\
             for dirpath, dirnames, filenames in os.walk(root):\n\
             \x20   dirnames[:] = sorted(d for d in dirnames if d != '.curie')\n\
             \x20   for entry in sorted(filenames):\n\
             \x20       path = os.path.join(dirpath, entry)\n\
             \x20       digest.update(os.path.relpath(path, root).encode())\n\
             \x20       digest.update(open(path, 'rb').read())\n\
             print(digest.hexdigest())",
        )
        .arg(bundle)
        .output()
        .expect("hash the weather bundle tree");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[test]
fn ladder_selects_platform_trajectory_eval_without_overriding_deployed_cases() {
    let (trajectory_output, trajectory_invocations) = run_eval_argument_control(true);
    let trajectory_transcript = transcript(&trajectory_output);
    assert_eq!(
        trajectory_output.status.code(),
        Some(0),
        "trajectory ladder failed:\n{trajectory_transcript}"
    );
    let trajectory_evals = trajectory_invocations
        .lines()
        .filter(|line| line.contains("local eval") || line.contains("cluster eval"))
        .collect::<Vec<_>>();
    assert_eq!(trajectory_evals.len(), 4, "{trajectory_invocations}");
    assert!(
        trajectory_evals
            .iter()
            .all(|line| !line.contains("--cases")),
        "trajectory eval must use the deployed bundle instead of an explicit cases override: \
         {trajectory_invocations}"
    );
    let trajectory_starts = trajectory_invocations
        .lines()
        .filter(|line| line.starts_with("local up"))
        .collect::<Vec<_>>();
    assert_eq!(trajectory_starts, ["local up"], "{trajectory_invocations}");
    assert!(
        trajectory_starts
            .iter()
            .all(|line| !line.contains("--minimal")),
        "trajectory scoring requires the full local profile: {trajectory_invocations}"
    );
    for tier in ["local", "cluster"] {
        assert!(
            has_line_with(
                &trajectory_transcript,
                &[tier, r#"grade 1 case(s) from suite "weather""#,],
            ),
            "trajectory dry run must expose the standard suite and case count for {tier}: \
             {trajectory_transcript}"
        );
    }

    let (ordinary_output, ordinary_invocations) = run_eval_argument_control(false);
    let ordinary_transcript = transcript(&ordinary_output);
    assert_eq!(
        ordinary_output.status.code(),
        Some(0),
        "ordinary ladder failed:\n{ordinary_transcript}"
    );
    let ordinary_evals = ordinary_invocations
        .lines()
        .filter(|line| line.contains("local eval") || line.contains("cluster eval"))
        .collect::<Vec<_>>();
    assert_eq!(ordinary_evals.len(), 4, "{ordinary_invocations}");
    assert!(
        ordinary_evals.iter().all(|line| line.contains("--cases")),
        "ordinary eval must retain its explicit cases path: {ordinary_invocations}"
    );
    let ordinary_starts = ordinary_invocations
        .lines()
        .filter(|line| line.starts_with("local up"))
        .collect::<Vec<_>>();
    assert_eq!(
        ordinary_starts,
        ["local up --minimal"],
        "ordinary grading must retain the minimal local profile: {ordinary_invocations}"
    );
}

/// POSITIVE CONTROL. Every rung -- skill, local and cluster -- reports the same
/// bundle digest, a matching dry-run plan line, and a model mode consistent with
/// the run, so the ladder must pass and must NAME that one digest against each of
/// the three rungs.
///
/// Why it exists: without it, the five negative controls below are unanchored --
/// a ladder that failed unconditionally would satisfy all of them. This is the
/// test that makes "one knob moved" mean something, and it is also the only test
/// that catches a parity assertion so strict it can never pass.
///
/// The skill rung is in the tier list deliberately, and it is the only executing
/// coverage of that leg: it runs `cli/scripts/e2e.sh` as a child, so it is what
/// fails if the `CURIE_E2E_BUNDLE` handoff breaks, if the `tee`d
/// `initial bundle digest:` line stops being parsed, or if the skill rung stops
/// comparing its identity with the deployed rungs. With local and cluster alone,
/// every one of those regressions stayed green.
///
/// No digest is pinned through a STUB_* override here: all three rungs report the
/// content-derived digest of the tree they actually packed, so their agreement is
/// three independent derivations landing on one value rather than one canned
/// constant echoed three times.
#[test]
fn parity_passes_when_every_rung_reports_the_same_identity() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let digest = weather_bundle_sha256();

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "skill,local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a ladder run whose rungs all report the same digest, the same suite \
         and a mode matching the run must pass; transcript:\n{transcript}"
    );
    for rung in ["skill", "local", "cluster"] {
        assert!(
            has_line_with(&transcript, &[&digest, rung]),
            "the ladder must report the common bundle digest against the {rung} \
             rung by name, since the transcript IS the parity evidence an \
             operator reads; transcript:\n{transcript}"
        );
    }
    assert!(
        transcript.contains("reports-a-temperature"),
        "the ladder must name the common suite's case ids, so a reader can see \
         WHICH case set every rung shared rather than taking `parity` on \
         trust; transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: bundle identity. The cluster rung's deploy receipt reports
/// a different `bundle.sha256` than the local rung's.
///
/// Prevents shipping silently: two tiers running two different artifacts while
/// the ladder reports a green "same bundle at every tier", which is the false
/// claim in the script header this whole change repairs.
#[test]
fn parity_fails_when_a_rung_reports_a_different_bundle_digest() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_LOCAL_SHA256", PINNED_DIGEST),
            // The one knob moved off the positive control.
            ("STUB_CLUSTER_SHA256", DIVERGENT_DIGEST),
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a rung reporting a different bundle digest must fail the ladder; \
         transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a digest divergence; \
         transcript:\n{transcript}"
    );
    assert!(
        has_line_with(&transcript, &[PINNED_DIGEST, DIVERGENT_DIGEST]),
        "the failure must put the pinned digest and the divergent one on one \
         line, so an operator sees the mismatch itself rather than two \
         unrelated receipts; transcript:\n{transcript}"
    );
    assert!(
        has_line_with(&transcript, &[DIVERGENT_DIGEST, "cluster"]),
        "the failure must name WHICH rung shipped the different artifact; \
         transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: suite and case count. The cluster tier's own suite loader
/// reports two cases from the weather suite when the bundle the ladder packed
/// carries one.
///
/// Prevents shipping silently: a tier grading a different suite than the one the
/// ladder deployed -- the second half of the divergence this change repairs
/// (skill graded `introduces-itself`, local and cluster graded
/// `reports-a-temperature`, and nothing compared them).
#[test]
fn parity_fails_when_a_rung_resolves_a_different_case_set() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_LOCAL_SHA256", PINNED_DIGEST),
            ("STUB_CLUSTER_SHA256", PINNED_DIGEST),
            ("STUB_FAKE_MODEL", "1"),
            // The one knob moved off the positive control.
            ("STUB_CLUSTER_PLAN_COUNT", "2"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a tier whose suite loader resolves a different case count must fail \
         the ladder; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a suite divergence; \
         transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("cluster")
            && transcript
                .contains(r#"grade 2 case(s) from suite "weather" against the cluster tier"#),
        "the failure must name the rung and print the raw plan line the tier \
         reported, so a harness red is distinguishable from a parity red; \
         transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: fake-versus-live mode. The run asks for live, and the
/// deployed cluster worker reports `CURIE_FAKE_MODEL=1`.
///
/// Prevents shipping silently: a "graded" nightly rung actually running sealed
/// against the fake model. The cluster rung is the target on purpose -- it is
/// the rung whose mode the script used to DISCLAIM rather than verify.
#[test]
fn parity_fails_when_the_deployed_model_mode_contradicts_the_run() {
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "cluster"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            // The one knob moved: a live run against a sealed deployment.
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a live run against a deployment whose worker reports the fake model \
         must fail the ladder, not warn; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a mode contradiction; \
         transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("cluster") && transcript.contains("CURIE_FAKE_MODEL"),
        "the failure must name the rung and the observed CURIE_FAKE_MODEL value \
         read off the running artifact; transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: a mode probe that could not read anything. The run asks for
/// live and `docker inspect` on the compose worker exits non-zero.
///
/// The sibling of the control above, on the failure mode it cannot reach: there
/// the probe read a contradiction, here it read NOTHING. An absent
/// CURIE_FAKE_MODEL is legitimately live, so a probe that swallowed the failed
/// read would hand assert_model_mode an empty string and the live rung would
/// certify a mode nobody observed. LIVE is the only mode where this hides, since
/// a sealed run already refuses an empty value.
#[test]
fn parity_fails_when_the_local_mode_probe_cannot_read_the_worker_env() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            ("CURIE_API_URL", &api_url),
            // No fake model, which is what a live run wants to observe.
            ("STUB_FAKE_MODEL", ""),
            // The one knob moved: the env read itself fails.
            ("STUB_DOCKER_INSPECT_EXIT", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a live run whose worker env read failed must fail the ladder rather \
         than treat the unread probe as live; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a mode it never read; \
         transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("local mode probe") && transcript.contains("docker inspect"),
        "the failure must say the worker env read is what failed, so an operator \
         is not sent hunting a parity divergence; transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: case ids only. Between the local rung's deploy and the
/// cluster rung's, the bundle's suite file has every case id rewritten while its
/// suite NAME and case COUNT stay identical. Both rungs' dry-run plan lines
/// therefore match the expectation exactly, and the digest is the only signal
/// that moved.
///
/// This is the control that proves the digest, not the dry-run plan line, is
/// what carries the case-id claim: the plan line carries a name and a count and
/// no ids at all, so if the ladder's case-set evidence rested on it, this run
/// would go green while the two tiers graded different cases. Deleting this test
/// makes that transitive argument unfalsifiable and lets a decorative case-id
/// claim ship.
#[test]
fn parity_fails_when_only_the_case_ids_differ() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let unmutated_digest = weather_bundle_sha256();

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_FAKE_MODEL", "1"),
            // The one knob moved off the positive control. No SHA overrides
            // here: the stub reports a content-derived digest, so the
            // divergence is CAUSED by the id rewrite rather than asserted.
            ("STUB_MUTATE_CASE_IDS", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a case-id-only divergence must still fail the ladder, through bundle \
         identity; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass when two rungs graded different \
         case ids; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains(r#"grade 1 case(s) from suite "weather" against the cluster tier"#),
        "the cluster tier's plan line must still have matched the expected \
         suite and count -- that is what makes this a case-ids-only \
         divergence rather than a suite divergence; transcript:\n{transcript}"
    );
    assert!(
        has_line_with(&transcript, &[&unmutated_digest, "cluster"]),
        "the failure must be an identity failure naming the cluster rung and \
         the pinned digest of the tree the ladder packed, not a suite failure; \
         transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: runtime binding. A stale active `prod` deployment exists
/// for the same agent alongside the `dev` row this run just created.
///
/// Prevents shipping silently the worst failure in this area: a deploy receipt
/// proves what was UPLOADED, not what the following turn RAN. The worker's
/// binding prefers `prod` over recency over the active set, so a leftover active
/// prod row serves the turn while the ladder reports the dev bundle's digest --
/// a green ladder proving the wrong artifact. Deleting this test makes the
/// sole-active-deployment assertion unfalsifiable.
#[test]
fn parity_fails_when_a_second_active_deployment_exists() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    // The one knob moved off the positive control: the deployments read.
    let api_url = spawn_deployments_stub(&two_active_deployments());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_LOCAL_SHA256", PINNED_DIGEST),
            ("STUB_CLUSTER_SHA256", PINNED_DIGEST),
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a second active deployment for the same agent must stop the ladder \
         before the turn, since the turn may bind to the wrong artifact; \
         transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass while a stale active deployment \
         could have served the turn; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains(DEPLOYMENT_ID) && transcript.contains(STALE_PROD_DEPLOYMENT_ID),
        "the failure must name every active deployment row, so the operator \
         can see which one is the shadow; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("local down --wipe --yes"),
        "the failure must carry the actionable fix line, matching the ladder's \
         other preflight-style hard failures; transcript:\n{transcript}"
    );
}

/// The inverse of the local/local-release assertions above, which pin those
/// rungs to a bare `"$BIN" local eval ...` that `set -e` makes fatal. On the
/// cluster rung the grade is REPORT ONLY (#1603): the eval still runs and still
/// prints, but a red case must not fail the rung while cluster fetch success is
/// unproved. The trajectory sidecar records requested tool identity, not
/// successful execution. The model routing defect in #1709 was fixed by #1715
/// on main and awaits its forward merge to next.
///
/// This is the successor of the #872 coverage (commit 1d266a73's third bullet),
/// which asserted the ladder EXITED 42 on a red deployed evaluator. #1603
/// deliberately reversed that half for this one rung, so what survives from #872
/// is the other half, and it is the half that mattered: the evaluator must
/// actually have been REACHED. Report only means non-fatal, never skipped.
///
/// Driven through the shared stub so the cluster rung's deploy-receipt, suite and
/// mode reads resolve deterministically instead of falling through to `exit 97`
/// or reaching the host's real `kubectl`. `STUB_EVAL_EXIT=42` keeps the injected
/// failure the same one #872 used, so the only thing that changed is what the
/// ladder is allowed to do with it.
#[test]
fn live_cluster_rung_reports_but_does_not_propagate_evaluator_failure() {
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let eval_marker = harness.path().join("eval_called");

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "cluster"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            // A live run needs the deployed worker to report NO fake model, or
            // the mode assertion would red the run before the evaluator ran.
            ("STUB_FAKE_MODEL", ""),
            (
                "STUB_EVAL_MARKER",
                eval_marker.to_str().expect("marker path"),
            ),
            ("STUB_EVAL_EXIT", "42"),
        ],
    );

    // Stream-scoped, not the merged transcript: the tolerated-grade notice is a
    // stderr diagnostic while LADDER PASS is the stdout verdict, and a control
    // that accepted either on either stream would pass with the two swapped.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        eval_marker.exists(),
        "the deployed evaluator must still run after the cluster message \
         plumbing succeeds -- report only means non-fatal, not skipped; \
         status: {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "the cluster rung's eval grade is report only (#1603), so an evaluator \
         failure must NOT fail the ladder; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("LADDER PASS"),
        "the ladder must run to completion past a red cluster eval; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("report only (#1603)"),
        "a red cluster eval must say IN THE LOG that it was tolerated, and cite \
         the ticket, so a silent tolerance is never mistaken for a pass; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

//! `curie cluster status | down` plus the release, secret, service-URL and
//! observability discovery those verbs (and the rest of the CLI) read through.

use anyhow::{bail, Context, Result};

#[allow(unused_imports)]
use super::{command::*, convergence, providers::*, up::*};

pub struct DownOpts {
    pub common: CommonOpts,
    pub yes: bool,
}

pub struct RollbackOpts {
    pub common: CommonOpts,
    /// An operator-named revision. `None` lets [`select_rollback_revision`] pick
    /// the newest `deployed`/`superseded` revision below the current one, which
    /// is the whole point of the verb (#1899).
    pub revision: Option<u32>,
    /// Admit a `--revision` whose status is not `deployed`/`superseded`. Refused
    /// without this flag, since helm never finished applying such a revision.
    pub allow_failed_revision: bool,
    pub yes: bool,
}

/// The version declared by the chart at `chart`, read from its own `Chart.yaml`.
///
/// `helm show chart` accepts every reference form `--chart` does -- a
/// directory, a `.tgz`, a repo ref -- so this is the version of the chart that
/// was actually rendered rather than one guessed from the reference's spelling.
///
/// A non-zero exit is fatal on purpose: this feeds the CHART VERSION MISMATCH
/// comparison, and failing open to a guessed version would raise (or suppress)
/// that warning about a chart nothing looked at (#1352).
pub async fn chart_version(chart: &str) -> Result<String> {
    let cmd = OpsCommand::new("helm", vec![plain("show"), plain("chart"), plain(chart)]);
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        bail!("could not read the chart version of {chart}: {err}");
    }
    let chart_yaml: serde_json::Value = serde_norway::from_str(&out)
        .with_context(|| format!("could not parse the Chart.yaml of {chart}"))?;
    chart_yaml
        .get("version")
        .and_then(|version| version.as_str())
        .map(str::to_string)
        .with_context(|| format!("the Chart.yaml of {chart} declares no version"))
}

/// The read-only commands `curie cluster status` runs (and prints under `--dry-run`).
///
/// Pure: the caller resolves the release's [`ReleaseFullname`] and passes it in
/// (live for a real run, [`chart_fullname`] under `--dry-run`), so this builder
/// still makes no cluster call of its own.
pub fn status_commands(o: &CommonOpts, fullname: &ReleaseFullname) -> Vec<OpsCommand> {
    let mut commands = vec![
        helm_status_cmd(o),
        pods_cmd(o),
        svc_cmd(o, fullname, "ui"),
        svc_cmd(o, fullname, "langfuse-web"),
        kubeconfig_host_cmd(),
    ];
    commands.extend(convergence::dry_run_commands(o));
    commands
}

fn helm_status_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("status"),
            plain(&o.release),
            plain("-n"),
            plain(&o.namespace),
        ],
    )
}

fn pods_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("pods"),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// `kubectl get svc <fullname>-<suffix> -n <ns> -o json`.
///
/// The choke point for every release Service the CLI reads. It takes a resolved
/// [`ReleaseFullname`], never the release name: the chart names each Service
/// `{{ include "curie.fullname" . }}-<component>`, which equals
/// `<release>-<component>` only when the release name happens to contain the
/// chart name (#1533).
fn svc_cmd(o: &CommonOpts, fullname: &ReleaseFullname, suffix: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("svc"),
            plain(fullname.resource(suffix)),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// `kubectl config view --minify -o jsonpath={...server}`: the current-context
/// API-server URL. The one canonical builder (#497) shared by the ops egress-IP
/// resolution and the message driver's advertise-host detection.
pub(crate) fn kubeconfig_host_cmd() -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("config"),
            plain("view"),
            plain("--minify"),
            plain("-o"),
            plain("jsonpath={.clusters[0].cluster.server}"),
        ],
    )
}

pub(crate) fn nodes_cmd() -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![plain("get"), plain("nodes"), plain("-o"), plain("json")],
    )
}

/// `helm uninstall` then a namespace sweep of only the namespaces THIS release
/// created (runtime sandboxes, PVCs and job pods Helm does not own). #707: the
/// sweep is scoped by the ownership labels `up` stamped rather than a hardcoded
/// namespace pair, so a pre-existing (unlabeled) namespace is never deleted.
/// `--ignore-not-found` keeps a partial teardown re-runnable and the label
/// selector tolerates zero matches. CRDs are never targeted (retention is
/// by-construction).
///
/// #1654: the selector is the CONJUNCTION of both ownership labels
/// (`curietech.ai/created-by=<release>,curietech.ai/created-in=<namespace>`),
/// because a release name alone is not an identity on a shared cluster. Two
/// independent installs normally both take the default release name `curie`
/// while living in different install namespaces, so a `created-by`-only
/// selector matched the OTHER install's namespaces and one `cluster down`
/// deleted them (observed live: `agent-sandbox-system` stamped
/// `created-by=curie` but annotated `meta.helm.sh/release-namespace: curie-other`,
/// i.e. owned by a different release, swept anyway). The identity is therefore
/// the PAIR (release name, install namespace), and both terms are required.
///
/// A namespace stamped by an older CLI carries only `created-by` and so does
/// NOT match this selector: that is deliberate. The sweep fails safe toward
/// retention rather than deleting a namespace whose owner cannot be
/// established; there is no fallback selector, since a fallback is exactly the
/// cross-release delete #1654 reports.
pub fn down_commands(o: &CommonOpts) -> Vec<OpsCommand> {
    vec![
        OpsCommand::new(
            "helm",
            vec![
                plain("uninstall"),
                plain(&o.release),
                plain("-n"),
                plain(&o.namespace),
            ],
        ),
        OpsCommand::new(
            "kubectl",
            vec![
                plain("delete"),
                plain("namespace"),
                plain("-l"),
                plain(format!(
                    "curietech.ai/created-by={},curietech.ai/created-in={}",
                    o.release, o.namespace
                )),
                plain("--ignore-not-found"),
            ],
        ),
    ]
}

/// Outcome of the `helm uninstall` teardown step. `Absent` is the existing
/// already-absent ("not found") case, which counts as done, never outstanding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelmOutcome {
    Removed,
    Absent,
    Failed,
}

/// Outcome of the label-scoped namespace sweep step. `NoMatch` (#768) is the
/// zero-match case: the selector's `kubectl delete` exits 0 (success) but
/// deleted nothing, because it printed nothing to stdout. This happens by
/// design when Curie was installed into a PRE-EXISTING namespace (#707
/// deliberately leaves that namespace unlabeled, so it is never a sweep
/// target). `NoMatch` counts as a completed step, same as `Removed`
/// (`outstanding_steps` never re-queues it), but it is NOT the same as
/// `Removed` for messaging: a `NoMatch` sweep stopped no compute, so
/// `teardown_result` must not describe it as "swept".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepOutcome {
    Removed,
    NoMatch,
    Failed,
}

/// A teardown step that can remain outstanding after a fail-forward `down`.
#[derive(Debug, PartialEq, Eq)]
enum TeardownStep {
    HelmUninstall,
    NamespaceSweep,
}

/// Pure decision (#767): which teardown steps did NOT complete. A `Removed` or
/// `Absent` helm is done; a `Failed` helm leaves `HelmUninstall` outstanding. A
/// `Removed` sweep is done; a `Failed` sweep leaves `NamespaceSweep`
/// outstanding. Order matches `down_commands` (helm before sweep).
fn outstanding_steps(helm: HelmOutcome, sweep: SweepOutcome) -> Vec<TeardownStep> {
    let mut out = Vec::new();
    if matches!(helm, HelmOutcome::Failed) {
        out.push(TeardownStep::HelmUninstall);
    }
    if matches!(sweep, SweepOutcome::Failed) {
        out.push(TeardownStep::NamespaceSweep);
    }
    out
}

/// Pure builder (#767, aggregation #768): the exact copy-pasteable resume
/// command for the outstanding steps, mapping each back to its
/// `down_commands(o)` entry (index 0 = HelmUninstall, 1 = NamespaceSweep).
///
/// When BOTH steps are outstanding the emitted ORDER matches `down()`'s own
/// execution order: the HELM UNINSTALL FIRST, then the namespace sweep. Helm
/// first because Helm stores its release metadata as Secrets INSIDE the
/// release namespace, and this chart owns cluster-scoped resources
/// (ClusterRole/ClusterRoleBinding). Sweeping the namespace first would destroy
/// that metadata, the following `helm uninstall` would report "not found"
/// (which this code reads as already-absent success), and the cluster-scoped
/// resources would be orphaned with no cleanup path.
///
/// #768: a plain "; " join runs both commands unconditionally (the compute-
/// stopping sweep is never skipped by a repeated helm failure), but a shell
/// only returns the LAST command's exit status, so an agent or CI executing
/// the resume line verbatim could read exit 0 even though helm failed. A
/// naive "&&" join would fix the status but reintroduce the fail-hard hazard
/// this whole feature exists to remove (a repeated helm failure would again
/// skip the sweep). Instead the two-step remainder is wrapped so each
/// command's status is captured into its own shell variable, both commands
/// still run unconditionally, and the final expression is nonzero UNLESS both
/// succeeded -- runs-both-aggregates-nonzero, not runs-both-or-skips-second.
///
/// The argv itself comes from `down_commands`, which stays the single source of
/// truth for the teardown commands, so the resume line cannot drift from what
/// would actually finish the job. A single-step remainder is one command with
/// no wrapper; an empty remainder yields the empty string.
fn resume_command(remaining: &[TeardownStep], o: &CommonOpts) -> String {
    let cmds = down_commands(o);
    let mut steps: Vec<&TeardownStep> = remaining.iter().collect();
    // Helm before sweep, matching `down_commands` execution order.
    steps.sort_by_key(|step| match step {
        TeardownStep::HelmUninstall => 0,
        TeardownStep::NamespaceSweep => 1,
    });
    let lines: Vec<String> = steps
        .iter()
        .map(|step| {
            let idx = match step {
                TeardownStep::HelmUninstall => 0,
                TeardownStep::NamespaceSweep => 1,
            };
            cmds[idx].display()
        })
        .collect();
    match lines.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [first, second] => {
            format!("{first}; s1=$?; {second}; s2=$?; [ \"$s1\" -eq 0 ] && [ \"$s2\" -eq 0 ]")
        }
        // `remaining` only ever holds HelmUninstall and/or NamespaceSweep, so a
        // third element is unreachable; stay defensive rather than panic.
        _ => lines.join("; "),
    }
}

/// Classifies whether a shelled-out `helm`/`kubectl` subprocess's stderr names a
/// transient connectivity failure (the API server was unreachable) rather than a
/// permanent one (RBAC, authz, invalid context, a failed hook). Subprocess-stderr
/// sibling of `crate::exit::is_transient_reqwest`, the HTTP-client side's single
/// definition of unreachable; the two cover the two transports and should stay
/// conceptually aligned. Only CONCRETE network signatures count: Helm wraps
/// permanent auth/exec-plugin/kubeconfig errors as `Kubernetes cluster
/// unreachable: ...`, so a bare `unreachable`/`timeout` prefix is not a reliable
/// transient signal and is deliberately excluded (a permanent error wearing that
/// generic prefix must classify false).
///
/// A host RESOLUTION failure is a permanent override checked BEFORE the marker
/// scan: `dial tcp: lookup bad-host: no such host` carries the `dial tcp`
/// connectivity marker but names a kubeconfig hostname that does not resolve,
/// a deterministic configuration error that retrying can never fix. The
/// override list is kept to the single `no such host` signature (the Go
/// resolver's permanent NXDOMAIN wording that both helm and kubectl surface);
/// broader DNS wordings such as `temporary failure in name resolution` name a
/// DNS-SERVER problem, which really is transient, so they stay out. `no route
/// to host` is a routing/connectivity failure, not name resolution, and is
/// deliberately not matched by the override.
pub(super) fn is_connectivity_failure(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    const PERMANENT_RESOLUTION: &[&str] = &["no such host"];
    if PERMANENT_RESOLUTION
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return false;
    }
    const MARKERS: &[&str] = &[
        "connection refused",
        "tls handshake",
        "no route to host",
        "i/o timeout",
        "network is unreachable",
        "could not connect",
        "dial tcp",
        "connection reset",
        "context deadline exceeded",
        // kubectl's own refusal wording, added when a kubectl call site started
        // classifying with this (#1351). client-go prints "The connection to
        // the server <host> was refused - did you specify the right host or
        // port?", which carries none of the Go-client signatures above: the
        // words are separated, so the literal "connection refused" never
        // appears. Without this marker every unreachable-apiserver kubectl read
        // classified as a permanent Failure. Kept to that one phrasing, which
        // client-go emits only for a refused connection.
        "connection to the server",
    ];
    MARKERS.iter().any(|marker| lower.contains(marker))
}

/// A usage-block header, e.g. docker's `Usage:  docker [OPTIONS] COMMAND ...`.
/// Matched on the `usage:` prefix only: a diagnosis line that happens to talk
/// about usage (`usage limit exceeded`) never starts with the colon form, while
/// every CLI that renders help on a bad invocation does.
///
/// Checked over bytes rather than `&str` slicing: a fixed byte index is not
/// guaranteed to fall on a char boundary, which panicked on third-party
/// stderr containing multi-byte characters (#1251).
fn is_usage_header(line: &str) -> bool {
    line.as_bytes()
        .get(..6)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"usage:"))
}

/// A trailing help pointer a CLI appends instead of, or after, a usage block --
/// docker's `Run 'docker --help' for more information`, kubectl's
/// `See 'kubectl get --help' for usage.`. Matched on SHAPE (a `Run '`/`See '`
/// opener that mentions `--help`), not on a tool name, so this does not need a
/// new marker per tool.
fn is_help_pointer(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let opens = ["run '", "run \"", "see '", "see \""]
        .iter()
        .any(|opener| lower.starts_with(opener));
    opens && lower.contains("--help")
}

/// One-line reason drawn from a captured stderr: the last non-empty trimmed
/// line that is part of the DIAGNOSIS, or a short default when the stderr is
/// empty. The single implementation behind both a failing teardown's Display
/// message and the `run_step` failure line, so neither drops the stderr to
/// `--debug` plumbing and the two cannot drift apart.
///
/// Last-non-empty is the base rule because `helm` and `kubectl` print warnings
/// BEFORE their `Error:` line, so the tail is where their diagnosis lives. What
/// #1230 fixed is that a CLI rejecting its own invocation inverts that: the
/// diagnosis comes first and a usage block comes last, so the raw base rule
/// surfaced `Run 'docker --help' for more information` and threw away
/// `unknown flag: --profile`. Trailing help text is therefore cut before the
/// base rule is applied.
///
/// Safety property: a stderr that is ONLY help text has no diagnosis to
/// recover, so the cut falls back to the base rule's answer rather than to an
/// empty string. This can improve the surfaced reason, never blank it.
pub(super) fn failure_reason(stderr: &str) -> &str {
    let lines: Vec<&str> = stderr.lines().map(str::trim).collect();
    let base = lines.iter().rev().find(|l| !l.is_empty()).copied();
    // Everything from the last usage header on is the tool's own help text.
    let diagnosis_end = lines
        .iter()
        .rposition(|l| is_usage_header(l))
        .unwrap_or(lines.len());
    lines[..diagnosis_end]
        .iter()
        .rev()
        .find(|l| !l.is_empty() && !is_help_pointer(l))
        .copied()
        .or(base)
        .unwrap_or("command failed")
}

/// Pure decision (#767, #768): turn the two teardown-step outcomes plus their
/// stderr into the `cluster down` result. An empty remainder is success.
/// Otherwise it is a fail-forward error carrying the exact resume command in
/// BOTH the human Display message (P1: `main` renders Display and drops the
/// fix) and the `fix` (for `--json`). The exit class is Transient (exit 3, safe
/// to retry) IFF every outstanding failed step's stderr is a connectivity
/// failure; any permanent outstanding failure makes it a plain Failure (exit 1,
/// P2). #768: a `SweepOutcome::NoMatch` (the label selector matched nothing,
/// e.g. a pre-existing namespace #707 never stamped) is a distinct case from
/// `Removed` -- it must never be worded as having swept/removed compute, since
/// nothing was actually deleted.
fn teardown_result(
    helm: HelmOutcome,
    sweep: SweepOutcome,
    helm_err: &str,
    sweep_err: &str,
    o: &CommonOpts,
) -> anyhow::Result<ClusterDownOutput> {
    let remaining = outstanding_steps(helm, sweep);
    if remaining.is_empty() {
        return Ok(ClusterDownOutput::Down {
            release_was_absent: matches!(helm, HelmOutcome::Absent),
        });
    }
    let cmd = resume_command(&remaining, o);

    // Transient only when every OUTSTANDING failed step is a connectivity failure;
    // a non-failed step never blocks retryability, a permanent failed step always
    // does.
    let helm_retryable = !matches!(helm, HelmOutcome::Failed) || is_connectivity_failure(helm_err);
    let sweep_retryable =
        !matches!(sweep, SweepOutcome::Failed) || is_connectivity_failure(sweep_err);
    let transient = helm_retryable && sweep_retryable;

    // The one-line reason drawn from the failed step that DETERMINES the class,
    // composed into the message so the human Display (P1) names WHY teardown
    // failed. On a non-transient result at least one failed step is permanent,
    // and that is the actionable one the operator must fix, so it wins over a
    // merely transient sibling (helm preferred when both are permanent). On a
    // transient result every failed step is connectivity, so prefer helm.
    let reason = if transient {
        if matches!(helm, HelmOutcome::Failed) {
            failure_reason(helm_err)
        } else {
            failure_reason(sweep_err)
        }
    } else if !helm_retryable {
        failure_reason(helm_err)
    } else {
        failure_reason(sweep_err)
    };

    let message = if matches!(sweep, SweepOutcome::Removed) {
        // Sweep succeeded (compute stopped); only the stale helm record remains.
        format!(
            "helm uninstall failed ({reason}) but the run-created namespaces were swept; the release record remains. Resume with: {cmd}"
        )
    } else if matches!(sweep, SweepOutcome::NoMatch) {
        // #768: the sweep's label selector matched nothing -- this release never
        // created (or was installed into a pre-existing) namespace, so nothing
        // was actually removed. This is NOT the swept case above: do not claim
        // compute was stopped, since it may still be running.
        format!(
            "helm uninstall failed ({reason}); no run-created namespaces matched the sweep, so no compute was stopped (this release may be running in a pre-existing namespace); the release record remains. Resume with: {cmd}"
        )
    } else if transient {
        format!(
            "cluster down could not complete; the API server is unreachable. Resume with: {cmd}"
        )
    } else {
        format!(
            "cluster down could not complete; teardown did not finish ({reason}). Resume with: {cmd}"
        )
    };

    let err = if transient {
        crate::exit::CliError::transient(message).with_fix(cmd)
    } else {
        crate::exit::CliError::failure(message).with_fix(cmd)
    };
    Err(err.into())
}

/// #707 ownership stamp. Returns the single `kubectl label namespace` step that
/// records THIS release as the creator of the target namespace `o.namespace`
/// (callers retarget `o` with `ns_common`, so this is the namespace being
/// labelled, NOT the release's install namespace), but ONLY when `up` actually
/// created it (`namespace_existed == false`); an empty vec when the namespace
/// pre-existed, so a namespace `up` merely adopted is never stamped and
/// therefore never swept by a later `down`. A release-scoped label (not a
/// per-invocation run-id) is what lets a separate `down` invocation match what
/// `up` created. `--overwrite` keeps a re-run idempotent, so an `up` interrupted
/// after create but before stamp fails safe toward retention.
///
/// #1654: the stamp carries TWO labels,
/// `curietech.ai/created-by=<release>` plus
/// `curietech.ai/created-in=<release_namespace>`, written in a single
/// `kubectl label` invocation so the step and checklist accounting is
/// unchanged. `release_namespace` is passed explicitly rather than read off
/// `o.namespace`, which the `ns_common` retarget has already pointed at the
/// namespace being labelled rather than at the install namespace. See
/// `down_commands` for why the PAIR (release name, install namespace) is the
/// identity and why a namespace stamped by an older, single-label CLI is
/// deliberately left unswept.
pub(super) fn ownership_label_commands(
    o: &CommonOpts,
    release_namespace: &str,
    namespace_existed: bool,
) -> Vec<OpsCommand> {
    if namespace_existed {
        return Vec::new();
    }
    vec![OpsCommand::new(
        "kubectl",
        vec![
            plain("label"),
            plain("namespace"),
            plain(&o.namespace),
            plain(format!("curietech.ai/created-by={}", o.release)),
            plain(format!("curietech.ai/created-in={release_namespace}")),
            plain("--overwrite"),
        ],
    )]
}

/// Whether `up` should attempt the ownership stamp for a candidate namespace,
/// given whether it existed BEFORE the install and whether it exists AFTER.
/// A namespace that pre-existed is never stamped (adopted, not created).
/// A namespace that did not exist before AND still does not exist after is
/// also never stamped: `agent-sandbox-system` is chart-conditional (created
/// only when `agentSandbox.controller.deploy` is true), so under
/// `--set agentSandbox.controller.deploy=false` it stays absent through the
/// whole install and `kubectl label namespace <missing>` would fail. Only a
/// namespace this run actually brought into existence gets stamped.
pub(super) fn should_stamp_ownership(existed_before: bool, exists_after: bool) -> bool {
    !existed_before && exists_after
}

/// Re-targets `opts` at namespace `ns` with an explicit `dry_run`, for the two
/// `up()` ownership-stamping call sites (`--dry-run` preview and the post-helm
/// stamp attempt) that otherwise duplicate the same `CommonOpts` construction.
pub(super) fn ns_common(opts: &CommonOpts, ns: &str, dry_run: bool) -> CommonOpts {
    CommonOpts {
        namespace: ns.to_string(),
        release: opts.release.clone(),
        dry_run,
    }
}

/// `kubectl get namespace <ns>`: the pre-existence probe `up` runs before the
/// install so it stamps ownership only on namespaces it creates.
fn namespace_get_cmd(namespace: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![plain("get"), plain("namespace"), plain(namespace)],
    )
}

/// Whether `namespace` already exists on the cluster. A nonzero `kubectl get`
/// (typically NotFound) reads as absent, so `up` treats it as fresh and stamps
/// it; any other transport error surfaces later on the install itself.
pub(super) async fn namespace_exists(namespace: &str) -> Result<bool> {
    let (ok, _out, _err) = run_capture(&namespace_get_cmd(namespace)).await?;
    Ok(ok)
}

/// Parse the hostname out of a kubeconfig `cluster.server` URL
/// (`https://host:6443` -> `host`). Delegates to the shared parser in
/// `message::split_server_url` so IPv6 and scheme/path handling stay in one place.
pub fn host_from_server_url(server: &str) -> Option<String> {
    crate::message::split_server_url(server).map(|(host, _)| host.to_string())
}

/// Output of `cluster status`: the dry-run plan, or the release/pod/URL summary.
/// `--json` emits one JSON object; the real path formerly printed via
/// `ui.payload`/scattered helpers (all suppressed under `--json`, #485).
#[derive(Debug)]
pub enum ClusterStatusOutput {
    DryRun(crate::ui::DryRunPlan),
    Status(Box<ClusterStatus>),
}

/// The assembled `cluster status` reading. Owns its data so `to_json`/`render`
/// can run after every capture completes.
#[derive(Debug)]
pub struct ClusterStatus {
    pub namespace: String,
    pub revision: String,
    pub release_state: String,
    pub release_found: bool,
    pub release_missing_note: Option<String>,
    pub pods: Vec<PodRow>,
    pub ready: usize,
    pub total: usize,
    pub unhealthy: Vec<String>,
    pub pods_listed: bool,
    pub urls: Vec<ServiceUrl>,
    pub upgrade: super::upgrade::UpgradeStatusView,
}

impl crate::ui::CliOutput for ClusterStatusOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterStatusOutput::DryRun(plan) => plan.to_json(),
            ClusterStatusOutput::Status(s) => {
                let healthy = s.total > 0 && s.ready == s.total && s.unhealthy.is_empty();
                serde_json::json!({
                    "namespace": s.namespace,
                    "revision": s.revision,
                    "release_state": s.release_state,
                    "release_found": s.release_found,
                    "pods": {
                        "ready": s.ready,
                        "total": s.total,
                        "unhealthy": s.unhealthy,
                        "rows": s.pods.iter().map(PodRow::to_json).collect::<Vec<_>>(),
                    },
                    "urls": s.urls.iter().map(ServiceUrl::to_json).collect::<Vec<_>>(),
                    "healthy": healthy,
                    "upgrade": s.upgrade.to_json(),
                })
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterStatusOutput::DryRun(plan) => plan.render(ui),
            ClusterStatusOutput::Status(s) => {
                ui.payload(&format!(
                    "curie · namespace {} · revision {} · {}",
                    s.namespace, s.revision, s.release_state
                ));
                if let Some(note) = &s.release_missing_note {
                    ui.note(note);
                }
                render_pod_table(ui, &s.pods);
                if !s.pods_listed {
                    ui.warn(&format!("could not list pods in namespace {}", s.namespace));
                }
                for url in &s.urls {
                    url.render(ui);
                }
                ui.payload(&format!(
                    "upgrade {} · phase {} · known-good {}",
                    s.upgrade.status,
                    s.upgrade.phase.as_deref().unwrap_or("idle"),
                    s.upgrade.known_good_version.as_deref().unwrap_or("none")
                ));
                if s.total > 0 && s.ready == s.total && s.unhealthy.is_empty() {
                    ui.success(&format!("healthy ({}/{} pods ready)", s.ready, s.total));
                } else if s.total == 0 {
                    ui.warn("no pods running");
                } else {
                    let mut msg = format!("{}/{} pods ready", s.ready, s.total);
                    if !s.unhealthy.is_empty() {
                        msg.push_str(&format!("; not ready: {}", s.unhealthy.join(", ")));
                    }
                    ui.warn(&msg);
                }
            }
        }
    }
}

pub async fn status(opts: CommonOpts) -> Result<ClusterStatusOutput> {
    if opts.dry_run {
        // A dry run makes no cluster call, so the release's fullname cannot be
        // discovered and the chart's no-override rule is the honest best guess.
        // `dry_run_fullname` computes it and emits the caveat that says so.
        let fullname = dry_run_fullname(&opts.release);
        return Ok(ClusterStatusOutput::DryRun(crate::ui::DryRunPlan {
            lines: std::iter::once(convergence::DRY_RUN_NOTE.to_owned())
                .chain(
                    status_commands(&opts, &fullname)
                        .iter()
                        .map(OpsCommand::display),
                )
                .collect(),
        }));
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;

    // (a) Helm release state -> a bright header line.
    let (helm_ok, helm_out, helm_err) = run_capture(&helm_status_cmd(&opts)).await?;
    let field = |name: &str, default: &str| -> String {
        helm_out
            .lines()
            .find(|l| l.trim_start().starts_with(name))
            .and_then(|l| l.split_once(':'))
            .map(|(_, v)| v.trim().to_string())
            .unwrap_or_else(|| default.to_string())
    };
    let (release_state, revision) = if helm_ok {
        (field("STATUS:", "unknown"), field("REVISION:", "?"))
    } else {
        ("not found".to_string(), "none".to_string())
    };
    let release_missing_note = (!helm_ok).then(|| {
        format!(
            "release {} not found: {}",
            opts.release,
            helm_err.trim().lines().next().unwrap_or("no such release")
        )
    });

    // (b) Pod health.
    let (ok, out, _) = run_capture(&pods_cmd(&opts)).await?;
    let (pods, ready, total, mut unhealthy) = if ok {
        let items: Vec<serde_json::Value> = serde_json::from_str::<serde_json::Value>(&out)
            .ok()
            .and_then(|v| v.get("items").and_then(|i| i.as_array()).cloned())
            .unwrap_or_default();
        collect_pod_summary(&items)
    } else {
        (Vec::new(), 0, 0, Vec::new())
    };

    // The same target-manifest check gates up/apply and this read surface.
    // Keep the existing JSON schema: diagnoses use its unhealthy string list.
    match convergence::observe(&opts).await {
        Ok(observation) => unhealthy.extend(observation.issues),
        Err(error) => unhealthy.push(format!("convergence could not be verified: {error}")),
    }
    if !ok {
        unhealthy.push("could not list release pods".to_string());
    }

    // (c) URL discovery. Resolve the release's rendered fullname once, here on
    // the live branch -- `--dry-run` returned above without touching kubectl.
    // The host lookup does not depend on the fullname, so the two run
    // concurrently rather than paying for each other's round-trip; the service
    // reads below need both and fan out after.
    let (fullname, host) = tokio::join!(
        release_fullname(&opts.namespace, &opts.release),
        discover_host(),
    );
    let urls = vec![
        resolve_service_url(&opts, &fullname, "ui", "UI", &host, true).await,
        resolve_service_url(&opts, &fullname, "langfuse-web", "Langfuse", &host, false).await,
    ];

    let chart_version = helm_ok.then(|| field("CHART:", "").trim().to_string());
    let upgrade = super::upgrade::load_upgrade_status(
        &opts.namespace,
        &opts.release,
        chart_version.filter(|v| !v.is_empty()),
    )
    .await;

    let output = ClusterStatusOutput::Status(Box::new(ClusterStatus {
        namespace: opts.namespace.clone(),
        revision,
        release_state,
        release_found: helm_ok,
        release_missing_note,
        pods,
        ready,
        total,
        unhealthy,
        pods_listed: ok,
        urls,
        upgrade,
    }));
    let json = crate::ui::CliOutput::to_json(&output);
    if json["healthy"] != true {
        return Err(crate::ui::ui().failed_report(
            &output,
            crate::exit::CliError::failure(format!(
                "target release has not converged: {}",
                json["pods"]["unhealthy"].as_array().map(|reasons| reasons.iter().filter_map(serde_json::Value::as_str).collect::<Vec<_>>().join("; ")).unwrap_or_default()
            ))
                .with_fix("inspect the unhealthy rollout reasons, correct the target configuration and rerun `curie cluster up`")
                .into(),
        ));
    }
    Ok(output)
}

/// Output of `cluster down`: the dry-run plan, an operator abort, or the removed
/// release. `--json` emits a JSON object; the real path formerly ended in
/// `ui.payload` (suppressed under `--json`, #485).
#[derive(Debug)]
pub enum ClusterDownOutput {
    DryRun(crate::ui::DryRunPlan),
    Aborted,
    Down { release_was_absent: bool },
}

impl crate::ui::CliOutput for ClusterDownOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterDownOutput::DryRun(plan) => plan.to_json(),
            ClusterDownOutput::Aborted => serde_json::json!({"down": false, "aborted": true}),
            ClusterDownOutput::Down { release_was_absent } => serde_json::json!({
                "down": true,
                "release_was_absent": release_was_absent,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterDownOutput::DryRun(plan) => plan.render(ui),
            ClusterDownOutput::Aborted => ui.note("aborted"),
            ClusterDownOutput::Down { .. } => {
                ui.payload("curie is down");
                ui.note("The agents.x-k8s.io CRDs are left in place intentionally.");
            }
        }
    }
}

pub async fn down(opts: DownOpts) -> Result<ClusterDownOutput> {
    let ui = crate::ui::ui();
    let cmds = down_commands(&opts.common);
    if opts.common.dry_run {
        return Ok(ClusterDownOutput::DryRun(crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        }));
    }
    ui.warn(&format!(
        "this uninstalls release '{0}' in namespace '{1}' and deletes only the namespaces that release created (labeled curietech.ai/created-by={0} AND curietech.ai/created-in={1}, so a namespace another release created is out of reach), leaving any pre-existing namespaces untouched",
        opts.common.release, opts.common.namespace
    ));
    if !opts.yes
        && !confirm(&format!(
            "This uninstalls release '{0}' in namespace '{1}' and deletes only the namespaces that release created (labeled curietech.ai/created-by={0} AND curietech.ai/created-in={1}, so a namespace another release created is out of reach). Continue? [y/N] ",
            opts.common.release, opts.common.namespace
        ))?
    {
        return Ok(ClusterDownOutput::Aborted);
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;

    let cl = ui.checklist();

    // helm uninstall, tolerating an already-absent release. On any OTHER failure
    // (e.g. a transient API-server blip) we do NOT bail: keep the stderr and fall
    // through to the sweep, so the run-created namespaces are never orphaned
    // (#767 fail-forward).
    let uninstall = &cmds[0];
    ui.plumbing(&format!("+ {}", uninstall.display()));
    let step = cl.step("uninstalling release");
    let (ok, out, helm_err) = run_capture(uninstall).await?;
    let helm_outcome = if ok {
        step.done("removed");
        HelmOutcome::Removed
    } else if helm_err.contains("not found") || out.contains("not found") {
        step.done("already absent");
        HelmOutcome::Absent
    } else {
        step.fail("failed");
        HelmOutcome::Failed
    };
    for line in out.lines().chain(helm_err.lines()) {
        ui.plumbing(line);
    }

    // Namespace sweep (runtime artifacts Helm does not own). Runs
    // UNCONDITIONALLY: it is Helm-independent by design (#707) and is what
    // actually stops compute, so a failed helm uninstall must not skip it. Not
    // `run_step`, which bails on a nonzero exit; here we tolerate and classify.
    let sweep = &cmds[1];
    ui.plumbing(&format!("+ {}", sweep.display()));
    let step = cl.step("sweeping namespaces");
    let (ok, out, sweep_err) = run_capture(sweep).await?;
    // #768: `--ignore-not-found` makes a zero-match selector exit 0 with EMPTY
    // stdout (no "namespace ... deleted" line), the same exit code an actual
    // removal gets. That is exactly the pre-existing-namespace case (#707
    // deliberately never stamps it), so a bare `ok` check cannot tell "nothing
    // to do" apart from "removed it"; only the stdout content can. Distinguish
    // them into their own outcome so `teardown_result` never claims compute was
    // stopped when the sweep matched nothing.
    let sweep_outcome = if ok {
        if out.trim().is_empty() {
            step.done("no matching namespaces");
            SweepOutcome::NoMatch
        } else {
            step.done("removed");
            SweepOutcome::Removed
        }
    } else {
        step.fail("failed");
        SweepOutcome::Failed
    };
    for line in out.lines().chain(sweep_err.lines()) {
        ui.plumbing(line);
    }

    // Pure decision (#767): success on a complete teardown, else a fail-forward
    // error whose exit class and message follow from the outcomes plus stderr.
    teardown_result(
        helm_outcome,
        sweep_outcome,
        &helm_err,
        &sweep_err,
        &opts.common,
    )
}

// ---------------------------------------------------------------------------
// `cluster rollback` (#1899)
// ---------------------------------------------------------------------------

/// One row of `helm history -o json`. Only the fields the rollback decision
/// needs are modelled; helm adds others (`updated`, `app_version`) that are
/// deliberately ignored so a helm version bump cannot break parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelmRevision {
    pub revision: u32,
    pub status: String,
    pub chart: String,
    pub description: String,
}

/// The revision statuses it is safe to roll back TO. Everything else
/// (`failed`, the four `pending-*` states, `uninstalling`, `uninstalled`,
/// `unknown`) names a revision helm never finished putting on the cluster, so
/// rolling back to it re-applies a manifest that was never known good.
const ROLLBACK_ELIGIBLE_STATUSES: [&str; 2] = ["deployed", "superseded"];

fn is_eligible_rollback_status(status: &str) -> bool {
    ROLLBACK_ELIGIBLE_STATUSES.contains(&status.trim().to_ascii_lowercase().as_str())
}

/// The revision the release is on right now.
///
/// Normally that is the one helm marks `deployed`. The fallback to the highest
/// revision is the real recovery case this verb exists for: when the newest
/// revision FAILED there is no `deployed` row at all, and treating the failed
/// revision as "current" is what lets the selector look below it.
fn current_revision(history: &[HelmRevision]) -> Option<u32> {
    history
        .iter()
        .filter(|r| r.status.trim().eq_ignore_ascii_case("deployed"))
        .map(|r| r.revision)
        .max()
        .or_else(|| history.iter().map(|r| r.revision).max())
}

/// [`current_revision`], or the error every entry point owes an operator whose
/// history came back empty. One copy on purpose: the wording here has already
/// been reworked twice, and a second hand-written copy would silently keep the
/// old text the next time it changes.
fn require_current_revision(history: &[HelmRevision]) -> Result<u32> {
    match current_revision(history) {
        Some(current) => Ok(current),
        None => Err(crate::exit::CliError::failure(
            "`helm history` returned no revisions for this release, so there is nothing to roll back to",
        )
        .with_fix("confirm the release name and namespace with `helm list -n <namespace>`")
        .into()),
    }
}

/// The INELIGIBLE revisions between `to` and `from`. The status filter is not
/// redundant: it is only the auto-select path that leaves nothing eligible in
/// the gap. An operator-named `--revision` can step over revisions that were
/// perfectly good, and reporting those as "not deployed/superseded" is a false
/// claim in both the note and the `--json` `skipped` field, so the list is
/// computed rather than assumed. Reporting the rest is the whole point of AC2.
///
/// Deduplicated defensively: [`parse_helm_history`] already collapses a corrupt
/// history to one row per revision, but this is pure over any slice a caller
/// hands it, and a number must never surface twice in the report.
fn skipped_between(history: &[HelmRevision], to: u32, from: u32) -> Vec<u32> {
    let mut skipped: Vec<u32> = history
        .iter()
        .filter(|r| r.revision > to && r.revision < from)
        .filter(|r| !is_eligible_rollback_status(&r.status))
        .map(|r| r.revision)
        .collect();
    skipped.sort_unstable();
    skipped.dedup();
    skipped
}

/// A resolved rollback: where the release is now, where it is going, and what
/// was stepped over on the way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackChoice {
    pub from_revision: u32,
    pub to_revision: u32,
    pub skipped: Vec<u32>,
    /// True only when `--allow-failed-revision` admitted a revision whose status
    /// is not `deployed`/`superseded`.
    pub forced: bool,
}

/// The outcome of the pure selection. `NoEligible` is a legitimate reading of a
/// real history (a first install, or a release whose every prior revision
/// failed), not a parse error, so it is carried as a value and turned into the
/// operator-facing refusal by [`RollbackTarget::require_eligible`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackTarget {
    Eligible(RollbackChoice),
    NoEligible { current: u32, skipped: Vec<u32> },
}

impl RollbackTarget {
    /// The chosen revision, or the AC4 refusal: never a silent no-op, and never
    /// a rollback to a revision helm never finished applying.
    pub fn require_eligible(self) -> Result<RollbackChoice> {
        match self {
            RollbackTarget::Eligible(choice) => Ok(choice),
            RollbackTarget::NoEligible { current, skipped } => {
                let detail = if skipped.is_empty() {
                    format!("revision {current} is the only revision in its history")
                } else {
                    format!(
                        "every revision below {current} ({}) has a status helm never finished applying",
                        skipped
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                // The next step rides in the message as well as the fix: a bare
                // `CliError` shows only its message to a human presenter, so an
                // operator on a TTY would otherwise never learn there is one.
                Err(crate::exit::CliError::failure(format!(
                    "no revision is safe to roll back to: {detail}; inspect \
                     `helm history <release> -n <namespace>` and, if you accept the risk, name one \
                     explicitly with --revision <n> --allow-failed-revision"
                ))
                .with_fix(
                    "inspect `helm history <release> -n <namespace>` and, if you accept the risk, \
                     name one explicitly with --revision <n> --allow-failed-revision",
                )
                .into())
            }
        }
    }
}

/// Pick the rollback target: the NEWEST revision strictly below the current one
/// whose status is `deployed` or `superseded`.
///
/// This is the whole fix for #1899. A `cluster up` against a cluster with no
/// `runsc` RuntimeClass records a FAILED revision before its successful retry,
/// so the history alternates failed/superseded and the immediately preceding
/// revision -- the one bare `helm rollback` targets -- is a failed one. Skipping
/// ineligible statuses is what stops an operator having to know that.
///
/// Pure by construction so the decision is unit-testable with no cluster.
pub fn select_rollback_revision(history: &[HelmRevision]) -> Result<RollbackTarget> {
    let current = require_current_revision(history)?;

    match history
        .iter()
        .filter(|r| r.revision < current && is_eligible_rollback_status(&r.status))
        .map(|r| r.revision)
        .max()
    {
        Some(to_revision) => Ok(RollbackTarget::Eligible(RollbackChoice {
            from_revision: current,
            to_revision,
            skipped: skipped_between(history, to_revision, current),
            forced: false,
        })),
        None => Ok(RollbackTarget::NoEligible {
            current,
            skipped: skipped_between(history, 0, current),
        }),
    }
}

/// Validate an operator-named `--revision`. A revision that is not in the
/// history is always refused (helm would otherwise fail obscurely mid-rollback);
/// an ineligible status is refused unless `allow_failed` was passed, and the
/// refusal names both the status and the override so the operator does not have
/// to guess (AC3).
pub fn resolve_explicit_revision(
    history: &[HelmRevision],
    revision: u32,
    allow_failed: bool,
) -> Result<RollbackChoice> {
    let current = require_current_revision(history)?;

    let row = history.iter().find(|r| r.revision == revision).ok_or_else(|| {
        let known: Vec<String> = history.iter().map(|r| r.revision.to_string()).collect();
        crate::exit::CliError::usage(format!(
            "revision {revision} is not in this release's Helm history (it has {})",
            known.join(", ")
        ))
        .with_fix("run `curie cluster rollback` with no --revision to let it pick the newest safe revision")
    })?;

    let eligible = is_eligible_rollback_status(&row.status);
    if !eligible && !allow_failed {
        return Err(crate::exit::CliError::usage(format!(
            "revision {revision} has status `{}`, not `deployed` or `superseded`; \
             helm never finished applying it, so rolling back to it re-applies a manifest \
             that was never known good; pass --allow-failed-revision to roll back to it anyway, \
             or omit --revision to let Curie pick the newest safe one",
            row.status
        ))
        .with_fix("pass --allow-failed-revision to roll back to it anyway, or omit --revision to let Curie pick the newest safe one")
        .into());
    }

    Ok(RollbackChoice {
        from_revision: current,
        to_revision: revision,
        skipped: skipped_between(history, revision, current),
        forced: !eligible,
    })
}

/// Parse `helm history -o json`: an array of objects. Unknown fields are
/// tolerated (helm adds them across versions) and `revision` is accepted as a
/// JSON number or a string, since that shape has moved between helm releases.
/// Never panics on operator-visible input -- a malformed payload becomes a
/// `CliError` naming the command to run by hand.
pub fn parse_helm_history(json: &str) -> Result<Vec<HelmRevision>> {
    let malformed = |detail: String| {
        crate::exit::CliError::failure(format!(
            "could not read `helm history -o json` output: {detail}"
        ))
        .with_fix(
            "run `helm history <release> -n <namespace> -o json` by hand to see what helm returned",
        )
    };

    let rows: serde_json::Value =
        serde_json::from_str(json).map_err(|e| malformed(e.to_string()))?;
    let rows = rows
        .as_array()
        .ok_or_else(|| malformed("expected a JSON array of revisions".to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let revision = row
            .get("revision")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            })
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| {
                malformed(format!("a revision has no usable `revision` field: {row}"))
            })?;
        let field = |name: &str| {
            row.get(name)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        // A row with no `status` is treated as `unknown`, which is ineligible:
        // the safe reading of a field we could not see is "not known good".
        let status = match field("status").as_str() {
            "" => "unknown".to_string(),
            s => s.to_string(),
        };
        out.push(HelmRevision {
            revision,
            status,
            chart: field("chart"),
            description: field("description"),
        });
    }
    // The invariant every consumer below relies on: ascending by revision, and
    // exactly ONE row per revision number. Eligibility is a property of the
    // REVISION, not of a row, so both the auto-selector (which filters rows by
    // status) and `--revision` (which looks a row up) must read the same answer
    // for the same number. A truncated or corrupt helm secret can yield two
    // rows for one revision that disagree -- say 20 `superseded` and 20
    // `failed` -- and before collapsing them the two paths contradicted each
    // other: `--revision 20` refused it as failed while a bare rollback happily
    // picked it. A revision whose own history contradicts itself was never
    // known good, so it collapses to its ineligible reading: sort ineligible
    // first within a revision number (`false` orders before `true`), then keep
    // the first row of each run.
    out.sort_by_key(|r| (r.revision, is_eligible_rollback_status(&r.status)));
    out.dedup_by_key(|r| r.revision);
    Ok(out)
}

/// `helm history` for the release, as JSON. `--max 256` because helm's default
/// of 10 silently truncates the history, and a truncated history would make the
/// selector pick a revision that merely looks like the newest safe one.
pub fn helm_history_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("history"),
            plain(&o.release),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
            plain("--max"),
            plain("256"),
        ],
    )
}

/// `helm rollback` to an explicit revision. Always explicit: the whole point of
/// the verb is that helm's own default target (the immediately preceding
/// revision) is the wrong one on a failed/superseded history.
pub fn helm_rollback_cmd(o: &CommonOpts, revision: u32) -> OpsCommand {
    helm_rollback_cmd_to(o, revision.to_string())
}

/// The revision slot of the rollback argv, as printed by `--dry-run` before the
/// history that decides it has been read.
const SELECTED_REVISION: &str = "<selected-revision>";

/// [`helm_rollback_cmd`] with the revision slot left as text, so the plan a dry
/// run prints and the command a live run executes are built by one function
/// rather than a builder and a `format!` that drift apart.
fn helm_rollback_cmd_to(o: &CommonOpts, revision: String) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("rollback"),
            plain(&o.release),
            plain(revision),
            plain("-n"),
            plain(&o.namespace),
        ],
    )
}

/// The commands `curie cluster rollback` runs (and prints under `--dry-run`):
/// the history read that decides the target, then the rollback itself. A `None`
/// revision is one the caller has not resolved yet, and stands in the argv as
/// [`SELECTED_REVISION`].
pub fn rollback_commands(o: &CommonOpts, revision: Option<u32>) -> Vec<OpsCommand> {
    let target = revision.map_or_else(|| SELECTED_REVISION.to_string(), |r| r.to_string());
    vec![helm_history_cmd(o), helm_rollback_cmd_to(o, target)]
}

/// One printed plan line. `display()` everywhere, except that it shell-quotes
/// [`SELECTED_REVISION`] (the angle brackets are not shell-safe) into something
/// that reads like a literal argument. The placeholder is a prompt to the
/// reader, not a value, so it is unquoted back out; everything else in the line
/// keeps `display()`'s quoting and masking.
fn plan_line(cmd: &OpsCommand) -> String {
    cmd.display()
        .replace(&shell_quote(SELECTED_REVISION), SELECTED_REVISION)
}

/// Output of `cluster rollback`: the dry-run plan, an operator abort, or the
/// completed rollback. `skipped` is carried into `--json` so an agent sees the
/// revisions bare `helm rollback` would have landed on.
#[derive(Debug)]
pub enum ClusterRollbackOutput {
    DryRun(crate::ui::DryRunPlan),
    Aborted,
    RolledBack {
        from_revision: u32,
        to_revision: u32,
        skipped: Vec<u32>,
        forced: bool,
    },
}

impl crate::ui::CliOutput for ClusterRollbackOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterRollbackOutput::DryRun(plan) => plan.to_json(),
            ClusterRollbackOutput::Aborted => {
                serde_json::json!({"rolled_back": false, "aborted": true})
            }
            ClusterRollbackOutput::RolledBack {
                from_revision,
                to_revision,
                skipped,
                forced,
            } => serde_json::json!({
                "rolled_back": true,
                "from_revision": from_revision,
                "to_revision": to_revision,
                "skipped": skipped,
                "forced": forced,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterRollbackOutput::DryRun(plan) => plan.render(ui),
            ClusterRollbackOutput::Aborted => ui.note("aborted"),
            ClusterRollbackOutput::RolledBack {
                from_revision,
                to_revision,
                skipped,
                forced,
            } => {
                ui.payload(&format!(
                    "curie rolled back from revision {from_revision} to revision {to_revision}"
                ));
                if let Some(note) = skipped_note(skipped, *from_revision) {
                    ui.note(&note);
                }
                if *forced {
                    ui.warn(&format!(
                        "revision {to_revision} was admitted by --allow-failed-revision; helm never finished applying it, so verify the release with `curie cluster status`"
                    ));
                }
            }
        }
    }
}

/// The AC2 line: which revisions were passed over, and -- only when it is
/// actually so -- which of them a bare `helm rollback` would have targeted.
///
/// helm's own default target is always the revision immediately below `from`,
/// so that half of the sentence is true only when the highest skipped revision
/// IS `from - 1`. On an explicit `--revision` the gap can contain eligible
/// revisions that are not in this list, and naming the highest ineligible one
/// as helm's target would be a fabrication. `None` when nothing was skipped, so
/// the common case stays quiet.
fn skipped_note(skipped: &[u32], from: u32) -> Option<String> {
    let highest = *skipped.iter().max()?;
    let list = skipped
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    Some(if from.checked_sub(1) == Some(highest) {
        format!(
            "skipped revision(s) {list} (not deployed/superseded); a bare `helm rollback` would have targeted {highest}"
        )
    } else {
        format!("skipped revision(s) {list} (not deployed/superseded)")
    })
}

pub async fn rollback(opts: RollbackOpts) -> Result<ClusterRollbackOutput> {
    let ui = crate::ui::ui();
    let history_cmd = helm_history_cmd(&opts.common);

    if opts.common.dry_run {
        // The target revision is a function of the live history, so a dry run
        // that has not read it can only name the revision when the operator did.
        return Ok(ClusterRollbackOutput::DryRun(crate::ui::DryRunPlan {
            lines: rollback_commands(&opts.common, opts.revision)
                .iter()
                .map(plan_line)
                .collect(),
        }));
    }
    require_on_path("helm")?;

    ui.plumbing(&format!("+ {}", history_cmd.display()));
    let (ok, history_out, history_err) = run_capture(&history_cmd).await?;
    if !ok {
        // A missing release fails HERE, with helm's own words, rather than
        // downstream as a misleading "no eligible revision" (AC6).
        let detail = history_err
            .trim()
            .lines()
            .next()
            .unwrap_or("helm exited nonzero with no message");
        return Err(crate::exit::CliError::failure(format!(
            "could not read the Helm history of release '{}' in namespace '{}': {detail}",
            opts.common.release, opts.common.namespace
        ))
        .with_fix(format!(
            "confirm the release exists with `helm list -n {}`",
            opts.common.namespace
        ))
        .into());
    }
    let history = parse_helm_history(&history_out)?;

    let choice = match opts.revision {
        Some(revision) => {
            resolve_explicit_revision(&history, revision, opts.allow_failed_revision)?
        }
        None => select_rollback_revision(&history)?.require_eligible()?,
    };

    // Disclosed BEFORE the prompt, and on stderr like `cluster down` does it:
    // the operator confirms knowing both the target and what was passed over,
    // which is the difference this verb exists to make. It cannot go through
    // `payload` -- ADR-0021 (#474) reserves that channel for `CliOutput::render`,
    // and routing it here told an operator who then declined the prompt that the
    // release was rolling back, on stdout, while printing it twice on success.
    ui.note(&format!(
        "rolling release '{}' back from revision {} to revision {}",
        opts.common.release, choice.from_revision, choice.to_revision
    ));
    if let Some(note) = skipped_note(&choice.skipped, choice.from_revision) {
        ui.warn(&note);
    }

    if !opts.yes
        && !confirm(&format!(
            "This rolls back release '{}' in namespace '{}' from revision {} to revision {}. Continue? [y/N] ",
            opts.common.release, opts.common.namespace, choice.from_revision, choice.to_revision
        ))?
    {
        return Ok(ClusterRollbackOutput::Aborted);
    }

    let cl = ui.checklist();
    let rollback_cmd = helm_rollback_cmd(&opts.common, choice.to_revision);
    ui.plumbing(&format!("+ {}", rollback_cmd.display()));
    let step = cl.step("rolling back release");
    let (ok, out, err) = run_capture(&rollback_cmd).await?;
    for line in out.lines().chain(err.lines()) {
        ui.plumbing(line);
    }
    if !ok {
        step.fail("failed");
        let detail = err
            .trim()
            .lines()
            .next()
            .unwrap_or("helm exited nonzero with no message");
        return Err(crate::exit::CliError::failure(format!(
            "helm could not roll release '{}' back to revision {}: {detail}",
            opts.common.release, choice.to_revision
        ))
        .with_fix(format!(
            "inspect the release with `curie cluster status --release {} --namespace {}`",
            opts.common.release, opts.common.namespace
        ))
        .into());
    }
    step.done("rolled back");

    Ok(ClusterRollbackOutput::RolledBack {
        from_revision: choice.from_revision,
        to_revision: choice.to_revision,
        skipped: choice.skipped,
        forced: choice.forced,
    })
}

/// Read a y/N confirmation from stderr/stdin for `down` when `--yes` is absent.
/// Prompt on stderr for a y/N confirmation of a destructive action, returning
/// whether the operator affirmed. The one canonical implementation (#497): every
/// destructive verb passes its own fully-formed prompt (ending in `[y/N] `). A
/// non-interactive session (piped/agent stdin) can never answer, so it refuses
/// with the `--yes` remediation instead of blocking on a read that never returns.
pub(crate) fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{IsTerminal, Write};
    if !std::io::stdin().is_terminal() {
        return Err(crate::exit::CliError::usage(
            "refusing to prompt for confirmation in a non-interactive session; re-run with --yes to proceed",
        )
        .with_fix("pass --yes")
        .into());
    }
    eprint!("{prompt}");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading confirmation from stdin")?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "Yes"))
}

/// One pod's row in the `cluster status` table.
#[derive(Debug)]
pub struct PodRow {
    pub name: String,
    pub ready: String,
    pub status: String,
}

impl PodRow {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"pod": self.name, "ready": self.ready, "status": self.status})
    }
}

/// Render the collected pod rows as a borderless table to stdout (human path).
fn render_pod_table(ui: &crate::ui::Ui, pods: &[PodRow]) {
    if pods.is_empty() {
        return;
    }
    let rows: Vec<Vec<String>> = pods
        .iter()
        .map(|p| vec![p.name.clone(), p.ready.clone(), p.status.clone()])
        .collect();
    ui.payload_plain(&crate::ui::table(&["pod", "ready", "status"], &rows, &[]));
}

/// Summarise `kubectl get pods` output into (rows, ready count, steady-state
/// total, names of pods not Running) WITHOUT printing, so the caller can render
/// or serialize it (#485). Terminal and terminating pods stay in the rows but are
/// excluded from the tally.
fn collect_pod_summary(pods: &[serde_json::Value]) -> (Vec<PodRow>, usize, usize, Vec<String>) {
    let mut ready = 0usize;
    let mut total = 0usize;
    let mut unhealthy: Vec<String> = Vec::new();
    let mut table_rows: Vec<PodRow> = Vec::new();
    for pod in pods {
        let name = pod
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("?")
            .to_string();
        let terminating = pod
            .get("metadata")
            .and_then(|m| m.get("deletionTimestamp"))
            .is_some();
        let phase = pod
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
            .unwrap_or("");
        let reason = pod
            .get("status")
            .and_then(|s| s.get("reason"))
            .and_then(|r| r.as_str())
            .unwrap_or("");
        let containers = pod
            .get("status")
            .and_then(|s| s.get("containerStatuses"))
            .and_then(|c| c.as_array());
        let (ready_n, total_m) = match containers {
            Some(cs) => {
                let m = cs.len();
                let n = cs
                    .iter()
                    .filter(|c| c.get("ready").and_then(|r| r.as_bool()) == Some(true))
                    .count();
                (n, m)
            }
            None => (0, 0),
        };
        let ready_col = format!("{ready_n}/{total_m}");
        let display_status = if terminating {
            "Terminating"
        } else if !reason.is_empty() {
            convergence::reason(reason)
        } else {
            phase
        };
        table_rows.push(PodRow {
            name: name.clone(),
            ready: ready_col,
            status: display_status.to_string(),
        });
        if phase == "Succeeded" || reason == "Completed" || terminating {
            continue;
        }
        total += 1;
        let all_ready = total_m > 0 && ready_n == total_m;
        if all_ready {
            ready += 1;
        }
        if phase != "Running" {
            unhealthy.push(name);
        }
    }
    (table_rows, ready, total, unhealthy)
}

/// Resolve a routable node host: kubeconfig cluster server hostname, falling
/// back to the first node's InternalIP. None when neither is available.
async fn resolve_node_host() -> Option<String> {
    if let Ok((true, out, _)) = run_capture(&kubeconfig_host_cmd()).await {
        if let Some(host) = host_from_server_url(out.trim()) {
            return Some(host);
        }
    }
    if let Ok((true, out, _)) = run_capture(&nodes_cmd()).await {
        if let Some(ip) = node_internal_ip(&out) {
            return Some(ip);
        }
    }
    None
}

/// Resolve the node host: the kubeconfig cluster server hostname, falling back
/// to the first node's InternalIP, then to the literal `localhost`.
async fn discover_host() -> String {
    resolve_node_host()
        .await
        .unwrap_or_else(|| "localhost".to_string())
}

/// Format an `http://host:port<path>` URL for a node, bracketing an IPv6 host
/// literal so the authority is valid (`::1` -> `[::1]`). `host` is expected
/// unbracketed (as `resolve_node_host` returns it); `path` is appended verbatim
/// (`/api`, `/?api=1`, or `""`).
fn node_http_url(host: &str, port: u16, path: &str) -> String {
    if host.contains(':') {
        format!("http://[{host}]:{port}{path}")
    } else {
        format!("http://{host}:{port}{path}")
    }
}

/// A usage error (exit 2) whose fix hint points the operator at `--api-url`,
/// the escape hatch for every UI-proxy discovery failure.
fn api_url_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("pass --api-url")
        .into()
}

/// A usage error (exit 2) whose fix hint points the operator at `--api-key`,
/// the escape hatch when the release's API key cannot be read from its Secret.
fn api_key_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("pass --api-key")
        .into()
}

/// A release-state failure while discovering an API key. The release cannot be
/// authenticated until its state is known, so do not suggest an API key as a
/// remedy for an unreadable Helm inspection.
fn api_key_state_err(namespace: &str, release: &str, msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix(format!(
            "run `curie cluster status --namespace {namespace} --release {release}` and retry"
        ))
        .into()
}

fn helm_release_entries(output: &str) -> Result<Vec<serde_json::Value>> {
    let releases: serde_json::Value =
        serde_json::from_str(output.trim()).context("malformed Helm release list JSON")?;
    releases
        .as_array()
        .cloned()
        .context("malformed Helm release list JSON: expected an array")
}

/// Find the requested release's Helm status in a `helm list -o json` result.
/// Missing releases are a valid result; malformed Helm output is not.
fn helm_release_status(output: &str, release: &str) -> Result<Option<String>> {
    let releases = helm_release_entries(output)?;
    let Some(found) = releases
        .iter()
        .find(|entry| entry.get("name").and_then(|name| name.as_str()) == Some(release))
    else {
        return Ok(None);
    };
    found
        .get("status")
        .and_then(|status| status.as_str())
        .map(|status| Some(status.to_string()))
        .context("malformed Helm release list JSON: release has no status")
}

/// Find a deployed release with the requested name in another namespace.
/// The all-namespace Helm listing makes a same-name alternate explicit rather
/// than guessing that a credential override can repair a failed release.
fn deployed_release_namespace(
    output: &str,
    release: &str,
    requested_namespace: &str,
) -> Result<Option<String>> {
    let releases = helm_release_entries(output)?;

    for entry in releases {
        if entry.get("name").and_then(|name| name.as_str()) != Some(release)
            || !entry
                .get("status")
                .and_then(|status| status.as_str())
                .is_some_and(|status| status.eq_ignore_ascii_case("deployed"))
        {
            continue;
        }
        let namespace = entry
            .get("namespace")
            .and_then(|namespace| namespace.as_str())
            .context("malformed Helm release list JSON: deployed release has no namespace")?;
        if namespace != requested_namespace {
            return Ok(Some(namespace.to_string()));
        }
    }
    Ok(None)
}

/// Discover a Helm release's platform API key by reading it out of the chart
/// Secret (data key `apiKey`), whose name is discovered by label selector
/// rather than computed -- see [`release_secret_name`] -- decoded server-side
/// by kubectl's `base64decode` so the plaintext never lands in argv (#524). The
/// governance verbs use this so they authenticate against a REAL release whose
/// `api.apiKey` was randomized at `cluster up`, instead of silently sending the
/// dev sentinel `curie-dev-key` and 401-ing. An explicit `--api-key`/env still
/// wins (the caller only reaches here when neither was supplied). The value is
/// never printed — it flows straight into the `X-API-Key` header.
pub async fn discover_api_key(namespace: &str, release: &str) -> Result<String> {
    if let Some(api_key) = read_release_secret(namespace, release, "apiKey").await {
        return Ok(api_key);
    }

    let requested_cmd = OpsCommand::new(
        "helm",
        vec![
            plain("list"),
            plain("-n"),
            plain(namespace),
            plain("--all"),
            plain("-o"),
            plain("json"),
        ],
    );
    let (requested_ok, requested_output, requested_error) = run_capture(&requested_cmd)
        .await
        .map_err(|error| {
            api_key_state_err(
                namespace,
                release,
                format!("could not inspect Helm state for release {release} in namespace {namespace}: {error}"),
            )
        })?;
    if !requested_ok {
        return Err(api_key_state_err(
            namespace,
            release,
            format!(
                "could not inspect Helm state for release {release} in namespace {namespace}: {}",
                failure_reason(&requested_error)
            ),
        ));
    }
    let requested_status = helm_release_status(&requested_output, release).map_err(|error| {
        api_key_state_err(
            namespace,
            release,
            format!("could not inspect Helm state for release {release} in namespace {namespace}: {error}"),
        )
    })?;

    if requested_status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case("deployed"))
    {
        return Err(api_key_usage_err(format!(
            "release {release} in namespace {namespace} is deployed, but its chart Secret API key could not be read; \
             pass --api-key or set CURIE_API_KEY to the release's api.apiKey"
        )));
    }

    let all_cmd = OpsCommand::new(
        "helm",
        vec![
            plain("list"),
            plain("-A"),
            plain("--all"),
            plain("-o"),
            plain("json"),
        ],
    );
    let deployed_alternate = match run_capture(&all_cmd).await {
        Ok((true, all_output, _)) => {
            deployed_release_namespace(&all_output, release, namespace).unwrap_or(None)
        }
        _ => None,
    };

    let status_command =
        format!("`curie cluster status --namespace {namespace} --release {release}`");
    let (message, fix) = match (requested_status, deployed_alternate) {
        (Some(status), Some(alternate_namespace)) => (
            format!(
                "release {release} in namespace {namespace} is {status}, not deployed; a deployed release named {release} is available in namespace {alternate_namespace}. Inspect the failed release with {status_command} or retry this command with `--namespace {alternate_namespace}`"
            ),
            format!("retry this command with `--namespace {alternate_namespace}`"),
        ),
        (Some(status), None) => (
            format!(
                "release {release} in namespace {namespace} is {status}, not deployed; inspect its state with {status_command}"
            ),
            format!("run {status_command} and repair or redeploy the release"),
        ),
        (None, Some(alternate_namespace)) => (
            format!(
                "no release named {release} was found in namespace {namespace}; a deployed release with that name is available in namespace {alternate_namespace}. Retry this command with `--namespace {alternate_namespace}`"
            ),
            format!("retry this command with `--namespace {alternate_namespace}`"),
        ),
        (None, None) => (
            format!(
                "no deployed release named {release} was found in namespace {namespace}; inspect the target with {status_command}"
            ),
            format!("run {status_command} and deploy the release before retrying"),
        ),
    };
    Err(crate::exit::CliError::usage(message).with_fix(fix).into())
}

#[cfg(test)]
mod api_key_discovery_tests {

    // --- #1030: the worker Deployment lookup ---------------------------------

    #[test]
    fn the_worker_selector_does_not_guess_the_deployment_name() {
        // The chart names it `{{ curie.fullname }}-worker`, which equals
        // `<release>-worker` only when the release name contains the chart name.
        // `acme-prod` renders `acme-prod-curie-worker`, and nameOverride moves it
        // again. Selecting on labels is what `release_secret_name` already does.
        let selector = worker_deployment_selector("acme-prod");
        assert!(selector.contains("app.kubernetes.io/instance=acme-prod"));
        assert!(selector.contains("app.kubernetes.io/component=worker"));
        assert!(
            !selector.contains("acme-prod-worker"),
            "the selector must not encode a guessed name: {selector}"
        );
    }

    #[test]
    fn a_failed_lookup_is_unknown_and_never_reads_as_real_slack() {
        // The distinction that keeps #1030 from returning in another shape. A
        // kubectl failure is not evidence that the worker talks to real Slack, and
        // treating it as such posts a real token wherever real Slack is while the
        // worker edits through a proxy the CLI never saw.
        assert_eq!(parse_slack_api_base(false, ""), SlackApiBase::Unknown);
        assert_eq!(
            parse_slack_api_base(false, "https://proxy.example/api"),
            SlackApiBase::Unknown
        );
    }

    #[test]
    fn an_empty_successful_lookup_means_real_slack() {
        // The chart renders SLACK_API_BASE_URL only when worker.slackApiBaseUrl is
        // non-empty, so a clean empty result is the ordinary case, not a failure.
        assert_eq!(parse_slack_api_base(true, ""), SlackApiBase::RealSlack);
        assert_eq!(parse_slack_api_base(true, "  \n "), SlackApiBase::RealSlack);
    }

    #[test]
    fn a_configured_base_is_returned_trimmed() {
        assert_eq!(
            parse_slack_api_base(true, "  https://proxy.example/api \n"),
            SlackApiBase::Configured("https://proxy.example/api".to_string())
        );
    }

    #[test]
    fn two_containers_reporting_a_base_is_unknown_not_a_coin_flip() {
        // Cannot happen in this chart today. If it ever does, picking one half is
        // exactly the ambiguity this issue is about, so say so instead.
        assert_eq!(
            parse_slack_api_base(true, "https://a/api\nhttps://b/api\n"),
            SlackApiBase::Unknown
        );
    }
    use super::*;

    struct EnvRestore {
        path: Option<std::ffi::OsString>,
        requested: Option<std::ffi::OsString>,
        requested_default: Option<std::ffi::OsString>,
        all: Option<std::ffi::OsString>,
        all_default: Option<std::ffi::OsString>,
        all_forbidden: Option<std::ffi::OsString>,
        log: Option<std::ffi::OsString>,
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (name, previous) in [
                ("PATH", &self.path),
                ("CURIE_TEST_HELM_REQUESTED", &self.requested),
                ("CURIE_TEST_HELM_REQUESTED_DEFAULT", &self.requested_default),
                ("CURIE_TEST_HELM_ALL", &self.all),
                ("CURIE_TEST_HELM_ALL_DEFAULT", &self.all_default),
                ("CURIE_TEST_HELM_ALL_FORBIDDEN", &self.all_forbidden),
                ("CURIE_TEST_HELM_LOG", &self.log),
            ] {
                match previous {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    fn write_executable(path: &std::path::Path, body: &str) {
        std::fs::write(path, body).expect("write fake cluster executable");
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(path)
            .expect("read fake cluster executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("make fake cluster executable runnable");
    }

    fn install_cluster_diagnosis_tools(tools: &std::path::Path) -> EnvRestore {
        write_executable(
            &tools.join("kubectl"),
            r#"#!/bin/sh
case "$*" in
  *"get secret -l app.kubernetes.io/instance=curie"*)
    printf '%s\n' 'curie-secrets'
    ;;
  *"get secret curie-secrets"*)
    printf '%s\n' 'Error from server (NotFound): secrets "curie-secrets" not found' >&2
    exit 1
    ;;
  *)
    printf 'unexpected kubectl invocation: %s\n' "$*" >&2
    exit 64
    ;;
esac
"#,
        );
        write_executable(
            &tools.join("helm"),
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_HELM_LOG"
case "$*" in
  *"-n curie"*)
    case "$*" in
      *"--all"*) printf '%s\n' "$CURIE_TEST_HELM_REQUESTED" ;;
      *) printf '%s\n' "${CURIE_TEST_HELM_REQUESTED_DEFAULT:-$CURIE_TEST_HELM_REQUESTED}" ;;
    esac
    ;;
  *)
    if [ "${CURIE_TEST_HELM_ALL_FORBIDDEN:-}" = 1 ]; then
      printf '%s\n' 'forbidden: cannot list releases across namespaces' >&2
      exit 1
    fi
    case "$*" in
      *"--all"*) printf '%s\n' "$CURIE_TEST_HELM_ALL" ;;
      *) printf '%s\n' "${CURIE_TEST_HELM_ALL_DEFAULT:-$CURIE_TEST_HELM_ALL}" ;;
    esac
    ;;
esac
"#,
        );

        let restore = EnvRestore {
            path: std::env::var_os("PATH"),
            requested: std::env::var_os("CURIE_TEST_HELM_REQUESTED"),
            requested_default: std::env::var_os("CURIE_TEST_HELM_REQUESTED_DEFAULT"),
            all: std::env::var_os("CURIE_TEST_HELM_ALL"),
            all_default: std::env::var_os("CURIE_TEST_HELM_ALL_DEFAULT"),
            all_forbidden: std::env::var_os("CURIE_TEST_HELM_ALL_FORBIDDEN"),
            log: std::env::var_os("CURIE_TEST_HELM_LOG"),
        };
        let mut path = vec![tools.to_path_buf()];
        path.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        std::env::set_var("PATH", std::env::join_paths(path).expect("join test PATH"));
        restore
    }

    fn assert_state_was_read(log: &std::path::Path) {
        let invocations = std::fs::read_to_string(log).expect("read Helm invocation log");
        assert!(
            invocations
                .lines()
                .any(|line| line == "list -n curie --all -o json"),
            "the requested release state was not read: {invocations}"
        );
        assert!(
            invocations
                .lines()
                .any(|line| line == "list -A --all -o json"),
            "the all namespace release state was not read: {invocations}"
        );
    }

    #[tokio::test]
    async fn api_key_failure_names_a_deployed_same_name_release_in_another_namespace() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );
        std::env::set_var(
            "CURIE_TEST_HELM_ALL",
            r#"[{"name":"curie","namespace":"curie","status":"failed"},{"name":"curie","namespace":"healthy","status":"deployed"}]"#,
        );

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("an unreadable secret must not yield an API key");
        let message = error.to_string();

        assert_eq!(
            crate::exit::classify(&error).0,
            crate::exit::ExitClass::Usage,
            "release state guidance must preserve the command's usage exit"
        );
        assert!(
            message.contains("failed"),
            "missing requested state: {message}"
        );
        assert!(
            message.contains("healthy"),
            "missing deployed alternate namespace: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a failed release cannot be repaired by supplying its key: {message}"
        );
        assert_state_was_read(&log);
    }

    #[tokio::test]
    async fn api_key_failure_without_a_deployed_alternate_does_not_offer_a_key_remedy() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );
        std::env::set_var(
            "CURIE_TEST_HELM_ALL",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("a failed release with no healthy alternate must fail");
        let message = error.to_string();

        assert!(
            message.contains("failed"),
            "missing requested state: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a failed release cannot be repaired by supplying its key: {message}"
        );
        assert_state_was_read(&log);
    }

    #[tokio::test]
    async fn deployed_release_key_failure_does_not_require_all_namespace_access() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"deployed"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_ALL", "[]");

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("an unreadable secret must not yield an API key");
        let message = error.to_string();

        assert_eq!(
            crate::exit::classify(&error).0,
            crate::exit::ExitClass::Usage,
            "a missing deployed release key remains a usage error"
        );
        assert!(
            message.contains("--api-key"),
            "missing flag remedy: {message}"
        );
        assert!(
            message.contains("CURIE_API_KEY"),
            "missing environment remedy: {message}"
        );

        let invocations = std::fs::read_to_string(&log).expect("read Helm invocation log");
        assert!(
            invocations
                .lines()
                .any(|line| line == "list -n curie --all -o json"),
            "the requested release state was not read: {invocations}"
        );
        assert!(
            !invocations
                .lines()
                .any(|line| line == "list -A --all -o json"),
            "cluster wide Helm access is forbidden once the requested release is deployed: {invocations}"
        );
    }

    #[tokio::test]
    async fn pending_upgrade_is_read_instead_of_reported_as_a_missing_release() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"pending-upgrade"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_REQUESTED_DEFAULT", "[]");
        std::env::set_var(
            "CURIE_TEST_HELM_ALL",
            r#"[{"name":"curie","namespace":"curie","status":"pending-upgrade"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_ALL_DEFAULT", "[]");

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("a pending release must not yield an API key");
        let message = error.to_string();

        assert!(
            message.contains("pending-upgrade"),
            "pending Helm state was hidden: {message}"
        );
        assert!(
            !message.contains("no deployed release named")
                && !message.contains("deploy the release"),
            "pending state was mistaken for a missing release: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a pending release cannot be repaired by configuring its key: {message}"
        );
        assert_state_was_read(&log);
    }

    #[tokio::test]
    async fn failed_release_state_survives_a_forbidden_all_namespace_scan() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_ALL", "[]");
        std::env::set_var("CURIE_TEST_HELM_ALL_FORBIDDEN", "1");

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("a failed release must not yield an API key");
        let message = error.to_string();
        let (class, fix) = crate::exit::classify(&error);

        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(
            message.contains("failed"),
            "known requested release state was discarded: {message}"
        );
        assert!(
            !message.contains("could not inspect Helm state across namespaces"),
            "the optional namespace scan replaced known state: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a failed release cannot be repaired by configuring its key: {message}"
        );
        assert!(
            fix.as_deref()
                .is_some_and(|guidance| guidance.contains("curie cluster status")),
            "missing cluster status guidance: {fix:?}"
        );
    }
}

/// A usage error (exit 2) whose fix hint points the operator at
/// `--valkey-password`, the escape hatch when the release's Valkey password
/// cannot be read from its Secret.
fn valkey_password_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("pass --valkey-password")
        .into()
}

/// Discover a Helm release's Valkey password from the same chart Secret
/// (name discovered by label selector -- see [`release_secret_name`] -- data
/// key `valkeyPassword`). `cluster message` enqueues
/// onto the release's Valkey, whose password `cluster up` randomizes, so without
/// this the dev sentinel `valkeypass` reaches a strong-secrets install and the
/// connection fails authentication (#786). An explicit
/// `--valkey-password`/`CURIE_VALKEY_PASSWORD` still wins (the caller only
/// reaches here when neither was supplied); the value is never printed.
pub async fn discover_valkey_password(namespace: &str, release: &str) -> Result<String> {
    read_release_secret(namespace, release, "valkeyPassword")
        .await
        .ok_or_else(|| {
            valkey_password_usage_err(format!(
                "could not read the Valkey password from the chart Secret for release {release} in namespace \
                 {namespace}; pass --valkey-password or set CURIE_VALKEY_PASSWORD to the \
                 release's valkey.password"
            ))
        })
}

/// A usage error whose fix hint points at the Slack bot-token escape hatch.
fn slack_bot_token_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("set CURIE_SLACK_BOT_TOKEN, or connect the workspace with `curie cluster comms --slack`")
        .into()
}

/// Discover a Helm release's Slack bot token from the chart Secret
/// (name discovered by label selector -- see [`release_secret_name`] -- data
/// key `slackBotToken`), or from the operator's own
/// `dispatcher.slack.botTokenExistingSecret` when one is configured (#1759).
/// In connected mode `cluster message` posts a real placeholder to the
/// workspace with this token so the approval card and resumed reply ride the
/// connected transport, instead of the throwaway stub (#770/ADR-0078). Only
/// reached when the release's dispatcher Deployment is present (a workspace IS
/// connected), so the token is expected to be set; an empty or unreadable
/// value is an actionable error. The value is never printed -- it flows only
/// into the `chat.postMessage` auth header.
pub async fn discover_slack_bot_token(namespace: &str, release: &str) -> Result<String> {
    read_direct_passthrough_secret(
        namespace,
        release,
        "slackBotToken",
        "dispatcher.slack.botTokenExistingSecret",
        "dispatcher.slack.botTokenExistingSecretKey",
    )
    .await
    .filter(|token| !token.is_empty())
    .ok_or_else(|| {
            slack_bot_token_usage_err(format!(
                "could not read a Slack bot token from the chart Secret for release {release} in namespace \
                 {namespace}; the workspace may not be connected (run `curie cluster comms \
                 --slack`), or set CURIE_SLACK_BOT_TOKEN"
            ))
        })
}

/// What a Slack API base lookup found for a release.
///
/// The three outcomes are deliberately distinct because they demand different
/// behavior, and collapsing them is how #1030 could come back wearing a different
/// hat. "Configured nothing" is the ordinary case and means real Slack. "Could not
/// look" is not evidence of anything and must not be read as the ordinary case,
/// because the CLI would then post a real token wherever real Slack is while the
/// worker edits through a proxy the CLI never saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlackApiBase {
    /// The worker renders `SLACK_API_BASE_URL` with this value.
    Configured(String),
    /// The worker renders no `SLACK_API_BASE_URL`, so it talks to real Slack.
    RealSlack,
    /// The lookup itself could not run or could not find the worker.
    Unknown,
}

/// The label selector that finds a release's worker Deployment whatever it is named.
///
/// The name is `{{ include "curie.fullname" . }}-worker`, which equals
/// `<release>-worker` only when the release name happens to contain the chart
/// name. A release named `acme-prod` renders `acme-prod-curie-worker`, and
/// `nameOverride`/`fullnameOverride` move it again. Guessing the name is the
/// defect `release_secret_name` already avoids by selecting on labels, and this
/// selects the same way for the same reason.
fn worker_deployment_selector(release: &str) -> String {
    component_selector(release, "worker")
}

/// The label pair that finds one component of a release whatever it is named.
///
/// `curie.selectorLabels` (`charts/curie/templates/_helpers.tpl:40-44`) emits
/// `app.kubernetes.io/instance: {{ .Release.Name }}` and
/// `app.kubernetes.io/component: {{ .component }}` and reads NEITHER
/// `nameOverride` nor `fullnameOverride`, so these labels are stable on exactly
/// the installs whose rendered NAMES are not. One spelling, because three
/// hand-built copies of the same pair is three places for it to drift from the
/// chart.
///
/// Not the shape [`release_secret_name`] uses: that one deliberately selects on
/// `instance` alone and filters by name suffix, because the chart Secret
/// carries no component label.
pub(super) fn component_selector(release: &str, component: &str) -> String {
    format!("app.kubernetes.io/instance={release},app.kubernetes.io/component={component}")
}

/// Parse `kubectl get deployment -o jsonpath=...` output into an outcome.
///
/// Pure, so every branch is unit-testable without a cluster. `ok=false` is the
/// case that must not read as "nothing configured": see [`SlackApiBase`].
fn parse_slack_api_base(ok: bool, out: &str) -> SlackApiBase {
    if !ok {
        return SlackApiBase::Unknown;
    }
    let value = out.trim();
    if value.is_empty() {
        return SlackApiBase::RealSlack;
    }
    // The jsonpath ranges over containers, so two containers each rendering the
    // var would concatenate. That cannot happen in this chart (the worker
    // Deployment has one container), and if it ever does, guessing which half is
    // the base is exactly the ambiguity this issue is about.
    if value.lines().count() > 1 {
        return SlackApiBase::Unknown;
    }
    SlackApiBase::Configured(value.to_string())
}

/// The Slack API base the release's own worker is configured with.
///
/// Read from the SAME release the bot token is read from (#1030). The base used to
/// come from `SLACK_API_BASE_URL` in the CLI's own process environment while the
/// token came from the release Secret, so a developer with a stub URL exported
/// from earlier testing sent a production workspace token to their local stub.
/// The two halves now share one source.
pub async fn discover_slack_api_base_url(namespace: &str, release: &str) -> SlackApiBase {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("deployment"),
            plain("-l"),
            plain(worker_deployment_selector(release)),
            plain("-o"),
            plain(
                "jsonpath={range .items[*].spec.template.spec.containers[*]}                 {.env[?(@.name=='SLACK_API_BASE_URL')].value}{\"\\n\"}{end}",
            ),
        ],
    );
    match run_capture(&cmd).await {
        Ok((ok, out, _)) => parse_slack_api_base(ok, &out),
        Err(_) => SlackApiBase::Unknown,
    }
}

/// Whether the release's dispatcher Deployment exists in `namespace` -- i.e. a
/// real Slack workspace is connected (via `curie cluster comms --slack`). In
/// that case `cluster message` posts a real placeholder and routes the approval
/// card + resumed reply over that connected transport rather than a throwaway
/// stub (#770/ADR-0078). A kubectl failure (cluster unreachable, no such
/// namespace) reads as NOT connected, so the caller safely falls back to the
/// stub path instead of failing the whole command. `--ignore-not-found` makes an
/// absent Deployment an empty success, so "connected" is exactly "non-empty
/// output on a zero exit".
pub async fn dispatcher_connected(namespace: &str, release: &str) -> bool {
    // The chart renders `{{ curie.fullname }}-dispatcher`, so the name must be
    // resolved and not computed from the release. `--ignore-not-found` turns a
    // wrong name into a confident empty success, which reads as "no workspace
    // connected" -- a silent wrong answer rather than a visible failure (#1533).
    let fullname = release_fullname(namespace, release).await;
    let dispatcher = fullname.resource("dispatcher");
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("deployment"),
            plain(&dispatcher),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("name"),
        ],
    );
    match run_capture(&cmd).await {
        Ok((true, out, _)) => !out.trim().is_empty(),
        // A probe that could not run is NOT evidence of "no workspace connected"
        // (#957 mode C). Silently downgrading to the stub path here means the
        // operator asked for the connected mode, got the other one, and was told
        // nothing. Still fall back -- the stub path is the safe direction, and
        // failing the command on a flaky kubectl would be worse -- but say so.
        Ok((false, _, err)) => {
            crate::ui::ui().warn(&format!(
                "could not determine whether a Slack workspace is connected \
                 (kubectl probe for {dispatcher} failed: {}); assuming \
                 NOT connected and using the local reply stub",
                err.trim().lines().next().unwrap_or("no stderr")
            ));
            false
        }
        Err(exc) => {
            crate::ui::ui().warn(&format!(
                "could not determine whether a Slack workspace is connected \
                 ({exc}); assuming NOT connected and using the local reply stub"
            ));
            false
        }
    }
}

/// The name of the release's chart Secret, discovered rather than computed.
///
/// It was computed as `<release>-secrets`, which is only right when the release
/// name happens to contain the chart name. The chart uses helm's standard
/// `fullname`, so a default install renders `<release>-curie-secrets` and every
/// read silently found nothing. It went unnoticed because the installs that
/// exercise these paths set `nameOverride` to the release name, which collapses
/// the two forms.
///
/// Discovered because it cannot be computed from what the CLI knows: both
/// `nameOverride` and `fullnameOverride` change the answer, and neither is
/// visible from the release name alone. Selecting on the instance label works
/// whatever the operator set.
///
/// The `-connector-secrets` exclusion is load-bearing: per-agent connector
/// Secrets carry the same release labels, and one of those would be a
/// confidently wrong answer rather than an empty one.
async fn release_secret_name(namespace: &str, release: &str) -> Option<String> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("secret"),
            plain("-l"),
            plain(format!("app.kubernetes.io/instance={release}")),
            plain("-o"),
            plain("jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"),
        ],
    );
    let (ok, out, _err) = run_capture(&cmd).await.ok()?;
    if !ok {
        return None;
    }
    pick_release_secret(&out)
}

/// The chart Secret among a release's Secrets, or `None` if it is not there.
///
/// Pure, so the selection rule is testable without a cluster -- which is how
/// the `-connector-secrets` collision was caught before it could pick one.
pub fn pick_release_secret(names: &str) -> Option<String> {
    names
        .lines()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .find(|n| n.ends_with("-secrets") && !n.ends_with("-connector-secrets"))
        .map(str::to_string)
}

/// The chart Secret's name, falling back to the chart's own naming rule.
///
/// A caller that must NAME the Secret (rather than read through it) still needs
/// a string when the cluster is unreachable -- a `--dry-run` plan, for
/// instance. That fallback used to be `format!("{release}-secrets")`, the raw
/// chart-resource form this sweep exists to delete: for an ordinary `platform`
/// install the chart renders `platform-curie-secrets`, so a transient discovery
/// failure had `migrate-store` stage a pod against a Secret that does not
/// exist. It now goes through [`chart_fullname`], which is the chart's
/// no-override rule and byte-identical for the default `curie` release.
///
/// Only the FALLBACK changed. Live discovery still selects on the instance
/// label alone and filters by suffix afterwards ([`pick_release_secret`]),
/// which is what makes it correct under `nameOverride`/`fullnameOverride`.
pub async fn release_secret_name_or_default(namespace: &str, release: &str) -> String {
    release_secret_name(namespace, release)
        .await
        .unwrap_or_else(|| chart_fullname(release).resource("secrets"))
}

#[cfg(test)]
mod release_secret_name_tests {
    use super::*;

    /// The bug: `<release>-secrets` is only right when the release name
    /// contains the chart name. A default install renders
    /// `<release>-curie-secrets`, and every read silently found nothing.
    #[test]
    fn a_default_install_secret_is_found() {
        let listed = "t-curie-secrets\nsh.helm.release.v1.t.v1\n";
        assert_eq!(
            pick_release_secret(listed),
            Some("t-curie-secrets".to_string())
        );
    }

    /// The shape that hid the bug: with `nameOverride` equal to the release
    /// name, both forms collapse to the same string.
    #[test]
    fn a_name_override_install_secret_is_found() {
        assert_eq!(
            pick_release_secret("acme-bot-secrets\n"),
            Some("acme-bot-secrets".to_string())
        );
    }

    /// The collision the exclusion exists for. Per-agent connector Secrets
    /// carry the same release labels, so without it the selector could return
    /// one -- a confidently WRONG answer, which is worse than an empty one.
    #[test]
    fn a_connector_secret_is_never_mistaken_for_the_chart_secret() {
        let listed = "acme-bot-acme-bot-connector-secrets\n                      acme-bot-acme-dev-connector-secrets\n                      acme-bot-secrets\n";
        assert_eq!(
            pick_release_secret(listed),
            Some("acme-bot-secrets".to_string())
        );
    }

    /// Ordering must not decide it: the connector Secret sorting first is the
    /// realistic case, since kubectl lists alphabetically.
    #[test]
    fn ordering_does_not_change_the_answer() {
        let connector_first = "a-connector-secrets\nz-curie-secrets\n";
        assert_eq!(
            pick_release_secret(connector_first),
            Some("z-curie-secrets".to_string())
        );
    }

    /// An absent release must yield nothing, not a guess. The callers turn
    /// `None` into an actionable error naming their escape-hatch flag.
    #[test]
    fn no_matching_secret_yields_none() {
        assert_eq!(pick_release_secret(""), None);
        assert_eq!(pick_release_secret("sh.helm.release.v1.t.v1\n"), None);
        assert_eq!(pick_release_secret("only-connector-secrets\n"), None);
    }
}

/// A release's rendered `curie.fullname` -- the prefix every Curie-owned object
/// in the chart is named from.
///
/// A newtype rather than a bare `String` on purpose. Every affected call site
/// used to build `format!("{release}-{component}")` from a raw release name, so
/// a `&str` parameter merely RENAMED to `fullname` would still compile when a
/// caller passed the release name -- the original defect, type-checked as
/// correct. This value is constructible only by [`chart_fullname`] and
/// [`release_fullname`], so a raw release name cannot reach a resource name at
/// all, and the compiler finds every site that has not been routed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseFullname(String);

impl ReleaseFullname {
    /// The chart's name for one of the release's components:
    /// `{{ include "curie.fullname" . }}-<component>`
    /// (`charts/curie/templates/_helpers.tpl:16-26`).
    ///
    /// The suffix is appended AFTER the fullname's own `trunc 63`, exactly as
    /// the chart templates do, so a rendered object name can legitimately
    /// exceed 63 characters. Truncating the joined string instead would name an
    /// object helm never created; `truncation_happens_before_the_component_suffix`
    /// pins the ordering.
    pub fn resource(&self, component: &str) -> String {
        format!("{}-{component}", self.0)
    }

    /// The fullname itself, for a caller that needs the prefix rather than a
    /// component's name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The chart's `curie.fullname` computed from the release name alone.
///
/// **Offline fallback only.** This is the chart's NO-OVERRIDE path
/// (`charts/curie/templates/_helpers.tpl:16-26`):
///
/// ```text
/// fullnameOverride                 if set
/// .Release.Name                    if it contains the chart name
/// "{.Release.Name}-curie"          otherwise
///                                  ... then | trunc 63 | trimSuffix "-"
/// ```
///
/// It cannot see `nameOverride` or `fullnameOverride`, and both move the
/// rendered name somewhere this rule computes WRONGLY: `helm template platform
/// charts/curie --set fullnameOverride=platform` renders `platform-api` while
/// this rule says `platform-curie-api`. So a live path calls
/// [`release_fullname`], which asks the cluster and only falls back here when
/// the cluster cannot answer (`--dry-run`, no kubectl, no release yet).
/// `cli/tests/chart_fullname_parity.rs` pins both the rule and its limit
/// against the chart's own render.
pub fn chart_fullname(release: &str) -> ReleaseFullname {
    const CHART_NAME: &str = "curie";

    let fullname = if release.contains(CHART_NAME) {
        release.to_string()
    } else {
        format!("{release}-{CHART_NAME}")
    };
    // `trunc 63` first, then `trimSuffix "-"`, with sprig's exact semantics:
    // `trimSuffix` removes EXACTLY ONE trailing dash where
    // `str::trim_end_matches('-')` removes all of them. Confirmed against the
    // chart rather than against a reading of sprig -- `--set
    // fullnameOverride=<61 a's>--<10 z's>` renders the api Service as
    // `<61 a's>--api`, so one dash survives for the component suffix to join to.
    let truncated: String = fullname.chars().take(63).collect();
    let trimmed = truncated.strip_suffix('-').unwrap_or(truncated.as_str());
    ReleaseFullname(trimmed.to_string())
}

/// The fullname a `--dry-run` plan prints, plus the caveat that goes with it.
///
/// A dry run makes no cluster call, so the release's rendered name cannot be
/// discovered and [`chart_fullname`]'s no-override rule is the honest best
/// guess. Every dry-run branch owes the reader the same caveat, so the note
/// lives with the value rather than being restated at each verb.
fn dry_run_fullname(release: &str) -> ReleaseFullname {
    let fullname = chart_fullname(release);
    crate::ui::ui().note(&format!(
        "dry run: service names assume the chart's default naming ({}); an install \
         using nameOverride/fullnameOverride renders them differently",
        fullname.resource("<component>")
    ));
    fullname
}

#[cfg(test)]
mod chart_fullname_tests {
    use super::*;

    /// Every component the CLI derives from a release name. The chart names
    /// each one `{{ include "curie.fullname" . }}-<component>`
    /// (`charts/curie/templates/_helpers.tpl:16-26`), so this is the full set
    /// the sweep has to keep byte-identical for the default release.
    const COMPONENTS: [&str; 7] = [
        "api",
        "ui",
        "langfuse-web",
        "valkey",
        "dispatcher",
        "worker",
        "secrets",
    ];

    fn opts(release: &str, namespace: &str) -> CommonOpts {
        CommonOpts {
            namespace: namespace.into(),
            release: release.into(),
            dry_run: false,
        }
    }

    // --- the negative control -------------------------------------------------

    /// THE regression guard for the whole sweep.
    ///
    /// `"curie".contains("curie")` is true, so the chart's fullname for the
    /// default release is the release name itself and every derived resource
    /// name is exactly what the CLI built before this change. Every install
    /// anyone runs locally, in CI, and on the parity ladder uses `--release
    /// curie`; if this goes red the fix broke all of them.
    ///
    /// The expectation is a literal, never a second call to the rule under
    /// test -- comparing the rule against itself would pass whatever the rule
    /// happened to be.
    #[test]
    fn the_default_release_is_a_byte_identical_no_op() {
        for component in COMPONENTS {
            assert_eq!(
                chart_fullname("curie").resource(component),
                format!("curie-{component}"),
                "the default release must derive the same name it always did"
            );
        }
    }

    // --- the chart's naming rule ----------------------------------------------

    /// `contains`, not `starts_with` and not an exact match: the chart tests
    /// `contains $name .Release.Name`, so a release that merely embeds the
    /// chart name anywhere takes no suffix. A "stricter" reading here would
    /// diverge from what helm actually renders.
    #[test]
    fn a_release_containing_curie_takes_no_suffix() {
        assert_eq!(chart_fullname("curie").as_str(), "curie");
        assert_eq!(chart_fullname("curieish").as_str(), "curieish");
        assert_eq!(chart_fullname("my-curie-prod").as_str(), "my-curie-prod");
        assert_eq!(chart_fullname("curieish").resource("api"), "curieish-api");
    }

    /// The reported bug: `helm template platform charts/curie` renders
    /// `platform-curie-api`, and the CLI used to ask for `platform-api`.
    #[test]
    fn a_release_not_containing_curie_takes_the_chart_suffix() {
        assert_eq!(chart_fullname("platform").as_str(), "platform-curie");
        assert_eq!(chart_fullname("acme-prod").as_str(), "acme-prod-curie");
        assert_eq!(
            chart_fullname("platform").resource("api"),
            "platform-curie-api"
        );
        assert_eq!(
            chart_fullname("acme-prod").resource("worker"),
            "acme-prod-curie-worker"
        );
    }

    /// Helm applies `trunc 63` to the FULLNAME -- `printf "%s-%s" .Release.Name
    /// $name | trunc 63` -- and the template appends `-<component>` after that,
    /// so a rendered object name can exceed 63 characters. Truncating the
    /// joined string instead yields 63 and names an object the chart never
    /// created. This test is what pins that ordering.
    #[test]
    fn truncation_happens_before_the_component_suffix() {
        let release = "a".repeat(70);
        let fullname = chart_fullname(&release);

        assert_eq!(fullname.as_str(), "a".repeat(63));
        assert_eq!(fullname.as_str().len(), 63);

        let resource = fullname.resource("api");
        assert_eq!(resource, format!("{}-api", "a".repeat(63)));
        assert_eq!(
            resource.len(),
            67,
            "the component suffix is appended after truncation, so 63 + \"-api\""
        );
    }

    /// `trimSuffix "-"` runs after `trunc`, so a cut that lands on a dash
    /// leaves the object named `<...>-api`, never `<...>--api`.
    #[test]
    fn a_trailing_dash_after_truncation_is_trimmed() {
        // 62 `a`s and a dash: the fullname is `<62 a>--curie`, and the 63rd
        // character is the release's own trailing dash.
        let release = format!("{}-", "a".repeat(62));
        let fullname = chart_fullname(&release);

        assert_eq!(fullname.as_str(), "a".repeat(62));
        assert_eq!(fullname.resource("api"), format!("{}-api", "a".repeat(62)));
        assert!(
            !fullname.resource("api").contains("--"),
            "a doubled dash means the trim ran before the truncation"
        );
    }

    /// Sprig's `trimSuffix "-"` removes exactly ONE trailing dash;
    /// `str::trim_end_matches('-')` removes all of them. A release whose
    /// truncation boundary lands after a `--` is where the two diverge, and the
    /// CLI must follow the chart.
    ///
    /// Confirmed against helm rather than against a reading of Sprig. The
    /// release-name path cannot be rendered directly: helm rejects any release
    /// name longer than 53 characters, so no release can reach the 63-character
    /// cut. The `fullnameOverride` branch runs the byte-identical
    /// `| trunc 63 | trimSuffix "-"` pipeline and has no such limit, so
    ///
    ///   helm template platform charts/curie \
    ///     --set fullnameOverride=<61 a's>--<10 z's>
    ///
    /// renders the api Service as `<61 a's>--api` (66 characters): the cut left
    /// `<61 a's>--`, and exactly one dash was removed, leaving one behind for
    /// the component suffix to join to. That is the behavior pinned here.
    #[test]
    fn only_one_trailing_dash_is_trimmed_matching_helms_trimsuffix() {
        // 61 `a`s, then `--`, then filler so the fullname overruns 63. The
        // release does not contain "curie", so the chart appends the suffix and
        // the cut lands exactly on the second of the two dashes.
        let release = format!("{}--{}", "a".repeat(61), "z".repeat(10));
        let fullname = chart_fullname(&release);

        assert_eq!(
            fullname.as_str(),
            format!("{}-", "a".repeat(61)),
            "one dash is trimmed, not both: trimming both would name an object \
             the chart never rendered"
        );
        assert_eq!(fullname.as_str().len(), 62);
        assert!(
            !fullname.as_str().ends_with("--"),
            "the truncation must still have trimmed one dash"
        );

        // helm rendered `<61 a's>--api`: the retained dash plus the template's
        // own `-api`. A `trim_end_matches` implementation gives `<61 a's>-api`.
        assert_eq!(fullname.resource("api"), format!("{}--api", "a".repeat(61)));
        assert_eq!(fullname.resource("api").len(), 66);
    }
    /// Helm's `printf "%s-%s" "" "curie"` is `-curie`. Kubernetes rejects that
    /// name, which is the right place for the failure -- pin what the chart
    /// does rather than "improving" it here and diverging from the render.
    #[test]
    fn an_empty_release_still_produces_the_chart_name() {
        assert_eq!(chart_fullname("").as_str(), "-curie");
        assert_eq!(chart_fullname("").resource("api"), "-curie-api");
    }

    // --- the discovery parse --------------------------------------------------

    /// Discovery reads back the name of a Service selected by label, so the
    /// fullname is whatever precedes the component suffix. Both shapes are
    /// real: `platform-api` is an override install, `platform-curie-api` is
    /// the no-override chart rule.
    #[test]
    fn a_discovered_api_service_name_yields_the_fullname() {
        assert_eq!(
            fullname_from_resource_name("platform-api", "api"),
            Some("platform".to_string())
        );
        assert_eq!(
            fullname_from_resource_name("platform-curie-api", "api"),
            Some("platform-curie".to_string())
        );
    }

    /// A name that does not carry the suffix we asked for is not ours to
    /// truncate. Blind stripping would mint a confidently wrong fullname,
    /// which is worse than falling through to the chart rule.
    #[test]
    fn a_name_without_the_expected_suffix_is_rejected_not_stripped() {
        assert_eq!(fullname_from_resource_name("platform-ui", "api"), None);
        assert_eq!(fullname_from_resource_name("platformapi", "api"), None);
        assert_eq!(fullname_from_resource_name("api", "api"), None);
    }

    /// The jsonpath yields an empty string when the selector matches nothing.
    /// That must be `None` so the caller falls through to the worker probe and
    /// then to `chart_fullname`, rather than resolving to the empty fullname.
    #[test]
    fn empty_discovery_output_yields_none() {
        assert_eq!(fullname_from_resource_name("", "api"), None);
        assert_eq!(fullname_from_resource_name("", "worker"), None);
    }

    /// Step 2 of discovery: with `api.deploy=false` there is no api Service,
    /// but the worker Deployment still carries the release labels. It strips
    /// its OWN suffix, and must not accept a name carrying another one.
    #[test]
    fn the_worker_fallback_strips_its_own_suffix() {
        assert_eq!(
            fullname_from_resource_name("platform-worker", "worker"),
            Some("platform".to_string())
        );
        assert_eq!(
            fullname_from_resource_name("platform-curie-worker", "worker"),
            Some("platform-curie".to_string())
        );
        assert_eq!(fullname_from_resource_name("platform-api", "worker"), None);
    }

    // --- discovery cardinality ------------------------------------------------

    /// The ordinary case: one labelled object, one answer.
    #[test]
    fn exactly_one_match_yields_the_fullname() {
        assert_eq!(
            component_discovery("platform-curie-api\n", "api"),
            ComponentDiscovery::Found(chart_fullname("platform"))
        );
        assert_eq!(
            component_discovery("platform-api", "api"),
            ComponentDiscovery::Found(ReleaseFullname("platform".to_string()))
        );
    }

    /// Zero matches is absence, not failure: the caller falls through to the
    /// worker probe and then to the chart rule, and says nothing, because a
    /// not-yet-installed release is a supported state.
    #[test]
    fn zero_matches_is_not_present() {
        assert_eq!(
            component_discovery("", "api"),
            ComponentDiscovery::NotPresent
        );
        assert_eq!(
            component_discovery("\n  \n", "api"),
            ComponentDiscovery::NotPresent
        );

        let absent = ComponentDiscovery::NotPresent;
        let fallback = chart_fullname("platform");
        assert_eq!(
            absent.fallback_warning("platform-ns", "platform", &fallback),
            None,
            "an absent release must degrade quietly; only a FAILED probe warns"
        );
    }

    /// THE finding. Kubernetes does not enforce label uniqueness, so `items[0]`
    /// could name a workload that is not ours -- a stray `unexpected-api`
    /// carrying the release labels would resolve the fullname to `unexpected`
    /// and point `cluster message`/`comms`/`eval` at it. Two matches are
    /// refused, never resolved to the first.
    #[test]
    fn two_matches_are_refused_not_resolved_to_the_first() {
        let outcome = component_discovery("platform-curie-api\nunexpected-api\n", "api");
        match &outcome {
            ComponentDiscovery::Ambiguous { component, names } => {
                let listed: Vec<&str> = names.iter().map(String::as_str).collect();
                assert_eq!(component.as_str(), "api");
                assert_eq!(listed, ["platform-curie-api", "unexpected-api"]);
            }
            other => panic!("two matches must be refused, not resolved: {other:?}"),
        }
    }

    /// A single match that does not carry the component suffix is rejected, not
    /// blind-stripped -- the rule `fullname_from_resource_name` pins, asserted
    /// here at the level that actually decides what discovery returns.
    #[test]
    fn a_single_match_without_the_suffix_is_rejected_not_stripped() {
        assert_eq!(
            component_discovery("platform-ui\n", "api"),
            ComponentDiscovery::NotPresent
        );
        assert_eq!(
            component_discovery("api\n", "api"),
            ComponentDiscovery::NotPresent,
            "stripping `-api` off `api` leaves an empty fullname"
        );
    }

    /// A FAILED probe is not an absent release. It must warn, say why, and say
    /// that the name in use is a guess -- otherwise `cluster status` reports
    /// the Service as "not found" and masks an RBAC denial.
    #[test]
    fn a_failed_probe_warns_that_the_name_is_a_guess() {
        let denied = ComponentDiscovery::ProbeFailed {
            component: "api".to_string(),
            detail: "Error from server (Forbidden): services is forbidden".to_string(),
        };
        let fallback = chart_fullname("platform");
        let warning = denied
            .fallback_warning("platform-ns", "platform", &fallback)
            .expect("a failed probe must warn");

        assert!(warning.contains("FAILED"), "{warning}");
        assert!(warning.contains("Forbidden"), "{warning}");
        assert!(warning.contains("platform-ns"), "the namespace: {warning}");
        assert!(warning.contains("COMPUTED GUESS"), "{warning}");
        assert!(warning.contains("platform-curie-<component>"), "{warning}");
        assert!(
            warning.contains("nameOverride/fullnameOverride"),
            "{warning}"
        );
    }

    /// The ambiguity warning has to be actionable: the namespace, the selector,
    /// and every candidate name, so the operator can see the collision it has
    /// to resolve.
    #[test]
    fn the_ambiguity_warning_names_the_selector_and_the_candidates() {
        let ambiguous = ComponentDiscovery::Ambiguous {
            component: "api".to_string(),
            names: vec![
                "platform-curie-api".to_string(),
                "unexpected-api".to_string(),
            ],
        };
        let fallback = chart_fullname("platform");
        let warning = ambiguous
            .fallback_warning("platform-ns", "platform", &fallback)
            .expect("an ambiguous match must warn");

        assert!(warning.contains("platform-ns"), "{warning}");
        assert!(
            warning.contains(&component_selector("platform", "api")),
            "{warning}"
        );
        assert!(warning.contains("platform-curie-api"), "{warning}");
        assert!(warning.contains("unexpected-api"), "{warning}");
        assert!(warning.contains("COMPUTED GUESS"), "{warning}");
    }

    /// A resolved name wins from either probe; otherwise a PROBLEM outranks
    /// absence, so a denied api probe is not buried by an absent worker.
    #[test]
    fn a_probe_problem_outranks_an_absent_second_probe() {
        let denied = ComponentDiscovery::ProbeFailed {
            component: "api".to_string(),
            detail: "forbidden".to_string(),
        };
        let absent = ComponentDiscovery::NotPresent;
        let found = ComponentDiscovery::Found(chart_fullname("platform"));

        assert_eq!(
            preferred_probe_outcome(denied.clone(), absent.clone()),
            denied
        );
        assert_eq!(
            preferred_probe_outcome(absent.clone(), denied.clone()),
            denied
        );
        // A real answer from either probe still wins.
        assert_eq!(preferred_probe_outcome(denied, found.clone()), found);
        assert_eq!(
            preferred_probe_outcome(absent.clone(), absent.clone()),
            absent
        );
    }

    // --- the chart Secret's offline name --------------------------------------

    /// `release_secret_name_or_default` fell back to `format!("{release}-secrets")`,
    /// the raw chart-resource form #1533 names explicitly. A `platform` install
    /// renders `platform-curie-secrets`, so the old fallback had `migrate-store`
    /// stage a pod against a Secret that does not exist. The fallback is now the
    /// `secrets` resource of `chart_fullname`, pinned here as a literal rather
    /// than by re-deriving it.
    #[test]
    fn the_secret_fallback_uses_the_chart_rule_for_a_non_default_release() {
        assert_eq!(
            chart_fullname("platform").resource("secrets"),
            "platform-curie-secrets"
        );
        assert_eq!(
            chart_fullname("acme-prod").resource("secrets"),
            "acme-prod-curie-secrets"
        );
    }

    /// The negative control for that change: `"curie".contains("curie")`, so
    /// the default release's Secret name is byte-identical to the computed
    /// `format!("{release}-secrets")` it replaced. Every local, CI, and
    /// parity-ladder install uses `--release curie`.
    #[test]
    fn the_secret_fallback_is_byte_identical_for_the_default_release() {
        assert_eq!(chart_fullname("curie").resource("secrets"), "curie-secrets");
        assert_eq!(
            chart_fullname("my-curie-prod").resource("secrets"),
            "my-curie-prod-secrets"
        );
    }

    // --- rendered argv for a non-default release ------------------------------

    /// `cluster status` under a release that does not contain the chart name.
    /// The literals are what `helm template platform charts/curie` actually
    /// renders; before this change the CLI asked for `platform-ui` and
    /// `platform-langfuse-web`, which do not exist.
    #[test]
    fn status_queries_the_chart_rendered_services_for_a_non_default_release() {
        let o = opts("platform", "platform-ns");
        let lines: Vec<String> = status_commands(&o, &chart_fullname("platform"))
            .iter()
            .map(OpsCommand::display)
            .collect();

        assert!(
            lines.contains(&"kubectl get svc platform-curie-ui -n platform-ns -o json".to_string()),
            "status must query the chart-rendered ui Service: {lines:#?}"
        );
        assert!(
            lines.contains(
                &"kubectl get svc platform-curie-langfuse-web -n platform-ns -o json".to_string()
            ),
            "status must query the chart-rendered langfuse Service: {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("svc platform-ui ")
                || line.contains("svc platform-langfuse-web ")),
            "the raw release name must not reach a service query: {lines:#?}"
        );
    }

    /// `cluster observability` resolves the same two Services and had the same
    /// defect.
    #[test]
    fn observability_queries_the_chart_rendered_services_for_a_non_default_release() {
        let o = opts("platform", "platform-ns");
        let lines: Vec<String> = observability_commands(&o, &chart_fullname("platform"))
            .iter()
            .map(OpsCommand::display)
            .collect();

        assert!(
            lines.contains(&"kubectl get svc platform-curie-ui -n platform-ns -o json".to_string()),
            "observability must query the chart-rendered ui Service: {lines:#?}"
        );
        assert!(
            lines.contains(
                &"kubectl get svc platform-curie-langfuse-web -n platform-ns -o json".to_string()
            ),
            "observability must query the chart-rendered langfuse Service: {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("svc platform-ui ")
                || line.contains("svc platform-langfuse-web ")),
            "the raw release name must not reach a service query: {lines:#?}"
        );
    }

    /// The default release renders the same argv it always has. The sibling
    /// `status_lists_the_readonly_commands` pins this at the argv level too;
    /// this is the observability half of that control.
    #[test]
    fn the_default_release_renders_the_same_service_argv_it_always_did() {
        let o = opts("curie", "curie");
        let lines: Vec<String> = observability_commands(&o, &chart_fullname("curie"))
            .iter()
            .map(OpsCommand::display)
            .collect();

        assert!(
            lines.contains(&"kubectl get svc curie-ui -n curie -o json".to_string()),
            "{lines:#?}"
        );
        assert!(
            lines.contains(&"kubectl get svc curie-langfuse-web -n curie -o json".to_string()),
            "{lines:#?}"
        );
    }
}

/// The fullname implied by a discovered object name, or `None` when that name
/// does not carry the component suffix we selected on.
///
/// Pure, extracted for the same reason [`pick_release_secret`] is: the part
/// that can be wrong is the selection rule, and it has to be testable with no
/// cluster. Rejecting rather than blind-stripping is load-bearing -- a name
/// that does not end in `-<component>` is not ours to truncate, and a
/// confidently wrong fullname is worse than falling through to the chart rule.
pub fn fullname_from_resource_name(name: &str, component: &str) -> Option<String> {
    let fullname = name.trim().strip_suffix(&format!("-{component}"))?;
    if fullname.is_empty() {
        return None;
    }
    Some(fullname.to_string())
}

/// The outcome of one discovery probe, kept as four distinct cases on purpose.
///
/// The probe used to collapse "this release is not installed", "kubectl is not
/// on PATH", "RBAC denied the read" and "two objects matched" into a single
/// `None`, and the caller then silently computed a name. The distinction that
/// matters: falling back for a genuinely ABSENT or not-yet-installed release is
/// defensible -- `doctor` and a fresh namespace must still work -- while
/// falling back because the probe FAILED is a guess dressed as an answer, and
/// doing it silently is the defect. Each case is its own variant so
/// [`release_fullname`] can say which one happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComponentDiscovery {
    /// Exactly one labelled object matched and its name carries the component
    /// suffix: the release's rendered fullname, read off the cluster.
    Found(ReleaseFullname),
    /// The probe ran and answered: this release has no such object.
    NotPresent,
    /// The probe did not answer -- kubectl missing, unreachable API server,
    /// RBAC denial, malformed request. Carries the first line of stderr.
    ProbeFailed { component: String, detail: String },
    /// More than one object carried the release's labels for this component.
    /// Kubernetes does not enforce label uniqueness, so this is reachable with
    /// a hand-applied or copy-pasted object, and taking `items[0]` would
    /// silently point every downstream verb at a workload that is not ours.
    Ambiguous {
        component: String,
        names: Vec<String>,
    },
}

impl ComponentDiscovery {
    /// The warning an operator must see before this outcome degrades into a
    /// COMPUTED name, or `None` when degrading is the honest, normal thing.
    ///
    /// Pure, so the wording -- the part that has to be actionable -- is
    /// testable with no cluster.
    pub fn fallback_warning(
        &self,
        namespace: &str,
        release: &str,
        fallback: &ReleaseFullname,
    ) -> Option<String> {
        let guess = format!(
            "the name being used, `{}`, is a COMPUTED GUESS from the chart's default \
             naming and is WRONG on an install using nameOverride/fullnameOverride",
            fallback.resource("<component>")
        );
        match self {
            // A real answer needs no caveat, and an absent release is the
            // documented reason this path has a fallback at all.
            Self::Found(_) | Self::NotPresent => None,
            Self::ProbeFailed { component, detail } => Some(format!(
                "could not discover release `{release}` resource names in namespace \
                 `{namespace}`: the kubectl probe for the `{component}` object FAILED \
                 ({detail}). Continuing, but {guess}. Check kubectl access and RBAC \
                 (get/list on services and deployments in `{namespace}`)."
            )),
            Self::Ambiguous { component, names } => Some(format!(
                "refusing to choose: {} objects in namespace `{namespace}` match \
                 `{}` ({}). Curie cannot tell which one belongs to release `{release}`, \
                 so it targets NONE of them. Continuing, but {guess}. Relabel or remove \
                 the objects that are not part of release `{release}`.",
                names.len(),
                component_selector(release, component),
                names.join(", ")
            )),
        }
    }
}

/// The outcome implied by the names a probe matched.
///
/// Pure, extracted for the same reason [`fullname_from_resource_name`] is: the
/// cardinality rule is the part that can be wrong, and it has to be testable
/// with no cluster.
///
/// EXACTLY ONE match resolves. Zero is absence. Two or more is
/// [`ComponentDiscovery::Ambiguous`] and never `names[0]`: those labels are not
/// unique by construction, so a stray Service named `unexpected-api` carrying
/// the release's labels would otherwise resolve the fullname to `unexpected`
/// and send `cluster message`/`comms`/`eval` at an unrelated workload.
///
/// Two legitimate releases are NOT this case: the selector pins
/// `app.kubernetes.io/instance=<release>`, so another release's objects never
/// match in the first place.
pub fn component_discovery(names: &str, component: &str) -> ComponentDiscovery {
    let matched: Vec<String> = names
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect();

    match matched.len() {
        0 => ComponentDiscovery::NotPresent,
        1 => match fullname_from_resource_name(&matched[0], component) {
            Some(fullname) => ComponentDiscovery::Found(ReleaseFullname(fullname)),
            // Labelled for the component but not NAMED for it: not ours to
            // truncate. Fall through to the next probe rather than mint a
            // confidently wrong fullname.
            None => ComponentDiscovery::NotPresent,
        },
        _ => ComponentDiscovery::Ambiguous {
            component: component.to_string(),
            names: matched,
        },
    }
}

/// One discovery probe: select the release's `<component>` objects by label and
/// read their names back.
///
/// The jsonpath ranges over ALL matches rather than reading `.items[0]`, so
/// cardinality is observable at all -- a single-item jsonpath cannot tell one
/// match from three.
async fn discover_component_fullname(
    namespace: &str,
    release: &str,
    kind: &str,
    component: &str,
) -> ComponentDiscovery {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain(kind),
            plain("-l"),
            plain(component_selector(release, component)),
            plain("-o"),
            plain("jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"),
        ],
    );
    match run_capture(&cmd).await {
        Ok((true, out, _err)) => component_discovery(&out, component),
        Ok((false, _out, err)) => {
            let detail = err.trim().lines().next().unwrap_or("no stderr");
            ComponentDiscovery::ProbeFailed {
                component: component.to_string(),
                detail: detail.to_string(),
            }
        }
        Err(exc) => ComponentDiscovery::ProbeFailed {
            component: component.to_string(),
            detail: exc.to_string(),
        },
    }
}

/// Which of the two probes' outcomes to report.
///
/// A resolved name wins from either probe. Otherwise a PROBLEM (a failed probe,
/// an ambiguous match) outranks absence, because absence is the one case that
/// degrades silently and it must not mask the other two.
fn preferred_probe_outcome(
    api: ComponentDiscovery,
    worker: ComponentDiscovery,
) -> ComponentDiscovery {
    if matches!(api, ComponentDiscovery::Found(_)) {
        return api;
    }
    if matches!(worker, ComponentDiscovery::Found(_)) {
        return worker;
    }
    if matches!(api, ComponentDiscovery::NotPresent) {
        return worker;
    }
    api
}

/// The release's rendered fullname, read off the cluster.
///
/// Discovered rather than computed for the same reason [`release_secret_name`]
/// is: `nameOverride` and `fullnameOverride` both change the rendered name and
/// neither is visible from the release name alone. It works because
/// `curie.selectorLabels` (`charts/curie/templates/_helpers.tpl:40-44`) emits
/// `app.kubernetes.io/instance: {{ .Release.Name }}` and
/// `app.kubernetes.io/component: {{ .component }}` and reads NEITHER override,
/// so the labels stay stable on exactly the installs whose NAMES do not.
/// Verified against both override renders, and pinned by
/// `overrides_preserve_the_discovery_labels`.
///
/// Two probes, deliberately: the api Service first, then the worker Deployment.
/// `api.deploy=false` is a supported install with no api Service, and the
/// worker Deployment still carries the release labels.
///
/// Neither probe picks among matches. `app.kubernetes.io/instance=<release>`
/// separates two legitimate releases, but nothing in Kubernetes stops a stray
/// object from carrying the same pair of labels, so a set of size two or more
/// is refused rather than resolved -- see [`component_discovery`].
async fn discover_release_fullname(namespace: &str, release: &str) -> ComponentDiscovery {
    let api = discover_component_fullname(namespace, release, "svc", "api").await;
    if matches!(api, ComponentDiscovery::Found(_)) {
        return api;
    }
    let worker = discover_component_fullname(namespace, release, "deployment", "worker").await;
    preferred_probe_outcome(api, worker)
}

/// The release's fullname: discovered from the cluster, falling back to the
/// chart's no-override rule.
///
/// THE live entry point. Every path that can reach a cluster resolves here, and
/// [`chart_fullname`] is what it degrades to. Discovery finding nothing is
/// normal rather than an error -- `doctor` and a not-yet-installed release must
/// still work -- so this never fails.
///
/// It is not, however, silent about WHY it degraded. A failed probe (RBAC
/// denial, no kubectl, unreachable API server) and an ambiguous match both warn
/// before the computed name is used, because on an install using
/// `nameOverride`/`fullnameOverride` that name is known to be wrong: without
/// the warning `cluster status` reports "not found" for a Service that exists
/// and a self-plumbed deploy fails against a name helm never rendered. Control
/// flow is deliberately unchanged -- the fallback still happens, loudly.
/// Failing mutating verbs closed on a failed probe is the stronger fix and is
/// left as a follow-up policy decision.
///
/// Resolve LAZILY, on the branch that actually needs a cluster-derived name.
/// Resolving at a verb's entry point fires kubectl on the explicit-`--api-url`
/// and `--dry-run` paths, which are contractually cluster-offline
/// (`cli/tests/cluster_connection_transport.rs`). Under `--dry-run`, call
/// [`chart_fullname`] directly and make no cluster call at all.
pub async fn release_fullname(namespace: &str, release: &str) -> ReleaseFullname {
    match discover_release_fullname(namespace, release).await {
        ComponentDiscovery::Found(fullname) => fullname,
        outcome => {
            let fallback = chart_fullname(release);
            if let Some(warning) = outcome.fallback_warning(namespace, release, &fallback) {
                crate::ui::ui().warn(&warning);
            }
            fallback
        }
    }
}

/// The release's sealing keys (ADR-0094): current first, then the previous one
/// if a rotation is in progress. Follows the operator's own
/// `sealing.{privateKey,previousPrivateKey}ExistingSecret` when configured
/// (#1759), otherwise the chart's own Secret.
///
/// Returns whatever is present. An empty vector means this release has no
/// sealing key, which the caller must report rather than work around -- sealing
/// against nothing, or "decrypting" without a key, both produce a connector
/// that starts and then fails every call.
pub async fn read_sealing_keys(namespace: &str, release: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for (default_data_key, existing_secret_key, existing_secret_key_key) in [
        (
            "sealingPrivateKey",
            "sealing.privateKeyExistingSecret",
            "sealing.privateKeyExistingSecretKey",
        ),
        (
            "sealingPreviousPrivateKey",
            "sealing.previousPrivateKeyExistingSecret",
            "sealing.previousPrivateKeyExistingSecretKey",
        ),
    ] {
        if let Some(value) = read_direct_passthrough_secret(
            namespace,
            release,
            default_data_key,
            existing_secret_key,
            existing_secret_key_key,
        )
        .await
        {
            if !value.trim().is_empty() {
                keys.push(value);
            }
        }
    }
    keys
}

/// Read one data key out of a release's chart Secret, decoded server-side by
/// kubectl's `base64decode` so the plaintext never lands in argv (#524). `None`
/// when the Secret, the key, or the cluster is unreachable; the caller turns
/// that into an actionable error naming its own escape-hatch flag.
async fn read_release_secret(namespace: &str, release: &str, data_key: &str) -> Option<String> {
    let secret = release_secret_name(namespace, release).await?;
    read_secret_key(namespace, &secret, data_key).await
}

/// Read one data key out of a NAMED Secret -- not necessarily the release's own
/// chart Secret, since a direct-passthrough credential's `existingSecret` names
/// an operator-managed Secret elsewhere in the namespace. Decoded server-side by
/// kubectl's `base64decode` so the plaintext never lands in argv (#524). `None`
/// when the Secret, the key, or the cluster is unreachable.
async fn read_secret_key(namespace: &str, secret_name: &str, data_key: &str) -> Option<String> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("secret"),
            plain(secret_name),
            plain("-o"),
            plain(format!(
                "go-template={{{{ index .data \"{data_key}\" | base64decode }}}}"
            )),
        ],
    );
    match run_capture(&cmd).await {
        Ok((true, out, _)) if !out.trim().is_empty() => Some(out.trim().to_string()),
        _ => None,
    }
}

/// Read a direct-passthrough credential (issue #1759), following the operator's
/// own `existingSecret` when one is configured for this key and otherwise
/// falling back to the chart's own Secret and its published key name -- the
/// same BYO-wins precedence the chart templates themselves use.
///
/// Without this, a CLI verb that reads the credential straight from the
/// chart's own Secret (as every one of these did before the BYO escape
/// existed) silently reads nothing for anyone who adopts it, even though the
/// deployed workload resolves correctly from the operator's Secret.
async fn read_direct_passthrough_secret(
    namespace: &str,
    release: &str,
    default_data_key: &str,
    existing_secret_key: &str,
    existing_secret_key_key: &str,
) -> Option<String> {
    let common = CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    let existing = fetch_existing_values(&common).await.ok().flatten();
    match resolve_existing_secret_ref(
        existing.as_ref(),
        existing_secret_key,
        existing_secret_key_key,
        default_data_key,
    ) {
        Some((secret_name, data_key)) => read_secret_key(namespace, &secret_name, &data_key).await,
        None => read_release_secret(namespace, release, default_data_key).await,
    }
}

/// Build the UI `/api` proxy base URL (`http://<host>:<ui-nodeport>/api`) from
/// the UI service JSON and a resolved node host, or an actionable usage error.
/// `cluster deploy` reaches the platform API through this proxy (the UI pod
/// serves `/api`), so it never falls back to a port-forward.
fn ui_api_url_from_parts(ui_svc_json: &str, host: Option<&str>) -> Result<String> {
    match parse_service(ui_svc_json) {
        Some((svc_type, node_port, _)) if svc_type == "NodePort" => {
            let np = node_port.ok_or_else(|| {
                api_url_usage_err(
                    "the UI service is NodePort but has not been assigned a nodePort yet; wait for the release to settle or pass --api-url to target the API directly",
                )
            })?;
            let host = host.ok_or_else(|| {
                api_url_usage_err(
                    "could not determine a node host to reach the UI /api proxy; pass --api-url to target the API directly",
                )
            })?;
            Ok(node_http_url(host, np, "/api"))
        }
        Some(_) => Err(api_url_usage_err(
            "the UI service is not NodePort-exposed (installed with --no-expose?); re-run `cluster up` without --no-expose or pass --api-url to target the API directly",
        )),
        None => Err(api_url_usage_err(
            "could not read the UI service to discover the platform API URL; pass --api-url to target the API directly",
        )),
    }
}

/// Build a direct platform-API base URL from the api service, used when the UI
/// is not deployed. No `/api` suffix: this is the API itself, not the UI proxy.
fn api_url_from_parts(api_svc_json: &str, host: Option<&str>) -> Option<String> {
    match parse_service(api_svc_json) {
        Some((svc_type, Some(np), _)) if svc_type == "NodePort" => {
            host.map(|h| node_http_url(h, np, ""))
        }
        _ => None,
    }
}

/// Discover the platform API URL for a release.
///
/// Prefers the UI's `/api` proxy, which is how a default install is reached
/// with no port-forward. Falls back to the api service directly when the UI is
/// absent: `ui.deploy=false` is a legitimate way to run a Slack-only bot with a
/// smaller footprint, and it used to break EVERY `cluster` verb -- deploy,
/// versions, kill, delete -- with an error naming only the UI, which reads like
/// a broken release rather than a supported configuration (#1068).
pub async fn discover_api_url(namespace: &str, release: &str) -> Result<String> {
    let common = CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    // This function is only reached when no `--api-url` was supplied, so it is
    // already a cluster path: resolving the fullname here keeps the explicit
    // `--api-url` and `--dry-run` routes free of any kubectl call.
    let fullname = release_fullname(namespace, release).await;
    let ui_svc = fullname.resource("ui");
    let api_svc = fullname.resource("api");
    let host = resolve_node_host().await;

    if let Ok((true, ui_json, _)) = run_capture(&svc_cmd(&common, &fullname, "ui")).await {
        return ui_api_url_from_parts(&ui_json, host.as_deref());
    }

    // No UI. The api service may still be reachable on its own NodePort.
    if let Ok((true, api_json, _)) = run_capture(&svc_cmd(&common, &fullname, "api")).await {
        if let Some(url) = api_url_from_parts(&api_json, host.as_deref()) {
            return Ok(url);
        }
        // The port-forward hint names 8000:8000, not the old 8123: `cluster
        // deploy` no longer binds 8123 for its own tunnel, and a hint naming
        // that exact port would send the operator at the thing #1533 fixed.
        return Err(api_url_usage_err(format!(
            "the {ui_svc} service is absent (ui.deploy=false?) and {api_svc} is not NodePort-exposed, so there is no reachable platform API URL; expose it with --set api.service.type=NodePort, or pass --api-url (e.g. via `kubectl port-forward svc/{api_svc} 8000:8000`)"
        )));
    }

    Err(api_url_usage_err(format!(
        "could not read the {ui_svc} or {api_svc} service in namespace {namespace} to discover the platform API URL; pass --api-url to target the API directly"
    )))
}

/// First node InternalIP from `kubectl get nodes -o json`.
fn node_internal_ip(nodes_json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(nodes_json).ok()?;
    for node in v.get("items")?.as_array()? {
        let addrs = node.get("status")?.get("addresses")?.as_array()?;
        for a in addrs {
            if a.get("type").and_then(|t| t.as_str()) == Some("InternalIP") {
                if let Some(ip) = a.get("address").and_then(|s| s.as_str()) {
                    return Some(ip.to_string());
                }
            }
        }
    }
    None
}

/// Print one service's access URL: a NodePort URL when exposed, else the
/// port-forward command to reach a ClusterIP service.
/// One resolved service URL row for `cluster status`. Owns its data so the
/// status reading can be rendered (byte-identical to the prior `ui.kv` output)
/// or serialized under `--json` (#485), instead of printing inline.
#[derive(Debug)]
pub struct ServiceUrl {
    label: String,
    name: String,
    namespace: String,
    api: bool,
    kind: ServiceUrlKind,
}

#[derive(Debug)]
enum ServiceUrlKind {
    NotFound,
    NodePortUrl(String),
    UnassignedNodePort,
    PortForward { local: u16, port: u16 },
    Unreadable,
}

impl ServiceUrl {
    /// Build the shared port-forward text after the caller chooses whether the
    /// URL target is plain (JSON) or styled (human output).
    fn port_forward_hint(&self, local: u16, port: u16, target: &str) -> String {
        port_forward_hint_with(&self.namespace, &self.name, local, port, target)
    }

    fn to_json(&self) -> serde_json::Value {
        let (url, note): (Option<String>, Option<String>) = match &self.kind {
            ServiceUrlKind::NodePortUrl(url) => (Some(url.clone()), None),
            ServiceUrlKind::NotFound => (None, Some(format!("service {} not found", self.name))),
            ServiceUrlKind::UnassignedNodePort => (
                None,
                Some(format!(
                    "service {} is NodePort but exposes no nodePort yet",
                    self.name
                )),
            ),
            ServiceUrlKind::PortForward { local, port } => {
                let suffix_path = api_suffix_path(self.api);
                (
                    None,
                    Some(self.port_forward_hint(
                        *local,
                        *port,
                        &format!("http://localhost:{local}{suffix_path}"),
                    )),
                )
            }
            ServiceUrlKind::Unreadable => {
                (None, Some(format!("could not read service {}", self.name)))
            }
        };
        serde_json::json!({"name": self.label, "url": url, "note": note})
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match &self.kind {
            ServiceUrlKind::NodePortUrl(url) => ui.kv(&self.label, &ui.url(url)),
            ServiceUrlKind::NotFound => {
                ui.kv(&self.label, &format!("service {} not found", self.name))
            }
            ServiceUrlKind::UnassignedNodePort => ui.kv(
                &self.label,
                &format!(
                    "service {} is NodePort but exposes no nodePort yet",
                    self.name
                ),
            ),
            ServiceUrlKind::PortForward { local, port } => {
                let suffix_path = api_suffix_path(self.api);
                ui.kv(
                    &self.label,
                    &self.port_forward_hint(
                        *local,
                        *port,
                        &ui.url(&format!("http://localhost:{local}{suffix_path}")),
                    ),
                )
            }
            ServiceUrlKind::Unreadable => ui.kv(
                &self.label,
                &format!("could not read service {}", self.name),
            ),
        }
    }
}

/// One `cluster status` URL row.
///
/// The displayed `name` and the name `svc_cmd` QUERIES come from the same
/// resolved fullname on purpose: they diverged before, so status could report a
/// service it had never asked about.
async fn resolve_service_url(
    o: &CommonOpts,
    fullname: &ReleaseFullname,
    suffix: &str,
    label: &str,
    host: &str,
    api: bool,
) -> ServiceUrl {
    let name = fullname.resource(suffix);
    let mk = |kind| ServiceUrl {
        label: label.to_string(),
        name: name.clone(),
        namespace: o.namespace.clone(),
        api,
        kind,
    };
    let (ok, out, _) = match run_capture(&svc_cmd(o, fullname, suffix)).await {
        Ok(res) => res,
        Err(_) => return mk(ServiceUrlKind::NotFound),
    };
    if !ok {
        return mk(ServiceUrlKind::NotFound);
    }
    // Same discovery core as `cluster observability` (#460); this owns the
    // wording, so the status output stays byte-identical.
    let kind = match resolve_service_endpoint(&out, host, api) {
        ServiceEndpoint::NodePortUrl(url) => ServiceUrlKind::NodePortUrl(url),
        ServiceEndpoint::UnassignedNodePort => ServiceUrlKind::UnassignedNodePort,
        ServiceEndpoint::PortForwardHint { local, port } => {
            ServiceUrlKind::PortForward { local, port }
        }
        ServiceEndpoint::Unreadable => ServiceUrlKind::Unreadable,
    };
    mk(kind)
}

/// From `kubectl get svc -o json`, return (type, first nodePort, first port).
fn parse_service(svc_json: &str) -> Option<(String, Option<u16>, u16)> {
    let v: serde_json::Value = serde_json::from_str(svc_json).ok()?;
    let spec = v.get("spec")?;
    let svc_type = spec.get("type").and_then(|t| t.as_str())?.to_string();
    let first_port = spec.get("ports")?.as_array()?.first()?;
    let node_port = first_port
        .get("nodePort")
        .and_then(|p| p.as_u64())
        .map(|p| p as u16);
    let port = first_port.get("port").and_then(|p| p.as_u64()).unwrap_or(0) as u16;
    Some((svc_type, node_port, port))
}

// ---------------------------------------------------------------------------
// Observability twin (issue #460).
// ---------------------------------------------------------------------------

/// Structured result of resolving one service's access endpoint.
///
/// A pure, structured value rather than a pre-formatted string: the caller owns
/// all formatting, because `cluster status`'s notes embed the service **name**
/// and its ClusterIP hint embeds **namespace + name** plus a styled `ui.url(..)`
/// mid-string. Pre-formatting that into a plain URL would break the PR#34
/// "status output visually unchanged" prior intent.
///
/// The four variants map the exact `parse_service` match arms in
/// `print_service_url`; the svc-fetch-failure / `!ok` "service not found" arms
/// stay in the async wrapper, before any JSON exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceEndpoint {
    /// NodePort-exposed: a fully built URL via `node_http_url(host, np, path)`.
    NodePortUrl(String),
    /// Type NodePort but no nodePort assigned yet.
    UnassignedNodePort,
    /// ClusterIP/other: reachable only via a port-forward.
    /// `local` is non-privileged: service ports below 1024 are offset by
    /// 18000, while an absent service port falls back to 8080.
    PortForwardHint { local: u16, port: u16 },
    /// `parse_service` returned None (malformed/unreadable JSON).
    Unreadable,
}

/// The path suffix that selects the console's API-backed view.
fn api_suffix_path(api: bool) -> &'static str {
    if api {
        "/?api=1"
    } else {
        ""
    }
}

/// Pure discovery core shared by `cluster status` and `cluster observability`:
/// map a service's JSON + a resolved node host to a structured endpoint.
/// `api` appends the Console's `/?api=1` suffix path.
fn resolve_service_endpoint(svc_json: &str, host: &str, api: bool) -> ServiceEndpoint {
    let path = api_suffix_path(api);
    match parse_service(svc_json) {
        Some((svc_type, node_port, _)) if svc_type == "NodePort" => match node_port {
            Some(np) => ServiceEndpoint::NodePortUrl(node_http_url(host, np, path)),
            None => ServiceEndpoint::UnassignedNodePort,
        },
        Some((_, _, port)) => ServiceEndpoint::PortForwardHint {
            local: match port {
                0 => 8080,
                1..=1023 => port + 18000,
                _ => port,
            },
            port,
        },
        None => ServiceEndpoint::Unreadable,
    }
}

/// The port-forward hint wording. `target` is the already-rendered URL text --
/// plain for machine payloads, styled for human output -- so the wording cannot
/// drift between the two callers.
fn port_forward_hint_with(ns: &str, name: &str, local: u16, port: u16, target: &str) -> String {
    format!("kubectl -n {ns} port-forward svc/{name} {local}:{port}  then {target}")
}

/// Plain, machine-safe hint (no ANSI): used for the observability `Endpoint.note`
/// that `--json` serializes.
fn port_forward_hint(ns: &str, name: &str, local: u16, port: u16, path: &str) -> String {
    port_forward_hint_with(
        ns,
        name,
        local,
        port,
        &format!("http://localhost:{local}{path}"),
    )
}

/// The platform API service's port (`<fullname>-api`, `api.service.port` in the
/// chart). Owned here so the port-forward hint carries no bare literal.
const API_SERVICE_PORT: u16 = 8000;

/// Map the UI service JSON + node host to the cluster's **API base** endpoint:
/// the UI `/api` proxy URL (the in-cluster way to reach the platform API, #360),
/// which is never browsable. Degrades to a `note` endpoint on any error.
///
/// The notes are minted here rather than borrowed from `ui_api_url_from_parts`
/// on purpose: that helper speaks `cluster deploy`'s error vocabulary, where
/// `--api-url` is a real escape hatch. `cluster observability` has no such flag,
/// so its rows must never name it. Instead the row reports the true condition
/// (`ui` service missing) or hands back an actionable port-forward for the API
/// service -- plain text, since `--json` serializes this note.
fn api_base_endpoint(
    o: &CommonOpts,
    fullname: &ReleaseFullname,
    ui_svc_json: Option<&str>,
    host: Option<&str>,
) -> crate::observability::Endpoint {
    let row = |url, note| crate::observability::Endpoint {
        name: "Curie API".to_string(),
        url,
        note,
        browsable: false,
    };
    let Some(ui_svc_json) = ui_svc_json else {
        return row(
            None,
            Some(format!("service {} not found", fullname.resource("ui"))),
        );
    };
    match ui_api_url_from_parts(ui_svc_json, host) {
        Ok(url) => row(Some(url), None),
        // Any other failure -- ClusterIP / `--no-expose` (a supported install
        // mode), an unassigned nodePort, an unreadable service, or an
        // unresolvable host -- still leaves a way in: port-forward the API
        // service directly. The operator copies and runs that line, so it must
        // name the object the chart actually rendered.
        Err(_) => row(
            None,
            Some(port_forward_hint(
                &o.namespace,
                &fullname.resource("api"),
                API_SERVICE_PORT,
                API_SERVICE_PORT,
                "",
            )),
        ),
    }
}

/// Map one release service to an observability [`Endpoint`], degrading to a
/// `note` row (never a hard failure, never a message smuggled into `url`) when
/// the service is missing, unsettled, unreadable, or reachable only by a
/// port-forward.
fn service_surface(
    o: &CommonOpts,
    fullname: &ReleaseFullname,
    suffix: &str,
    name: &str,
    svc_json: Option<&str>,
    host: Option<&str>,
    api: bool,
) -> crate::observability::Endpoint {
    let svc_name = fullname.resource(suffix);
    let degraded = |note: String| crate::observability::Endpoint {
        name: name.to_string(),
        url: None,
        note: Some(note),
        browsable: false,
    };
    let Some(svc_json) = svc_json else {
        return degraded(format!("service {svc_name} not found"));
    };
    let Some(host) = host else {
        return degraded(format!(
            "could not determine a node host to reach service {svc_name}"
        ));
    };
    match resolve_service_endpoint(svc_json, host, api) {
        ServiceEndpoint::NodePortUrl(url) => crate::observability::Endpoint {
            name: name.to_string(),
            url: Some(url),
            note: None,
            browsable: true,
        },
        ServiceEndpoint::UnassignedNodePort => degraded(format!(
            "service {svc_name} is NodePort but exposes no nodePort yet"
        )),
        ServiceEndpoint::PortForwardHint { local, port } => degraded(port_forward_hint(
            &o.namespace,
            &svc_name,
            local,
            port,
            api_suffix_path(api),
        )),
        ServiceEndpoint::Unreadable => degraded(format!("could not read service {svc_name}")),
    }
}

/// Fetch one release service's JSON, or None when kubectl cannot read it.
async fn fetch_service(o: &CommonOpts, fullname: &ReleaseFullname, suffix: &str) -> Option<String> {
    match run_capture(&svc_cmd(o, fullname, suffix)).await {
        Ok((true, out, _)) => Some(out),
        _ => None,
    }
}

/// The cluster tier's three observability surfaces (payload parity with local):
/// Console via the `ui` service, Langfuse via `langfuse-web`, and the API base
/// via the UI `/api` proxy. Degrades per endpoint; never hard-fails.
pub async fn cluster_observability_endpoints(
    opts: &CommonOpts,
) -> Vec<crate::observability::Endpoint> {
    // Deliberately `resolve_node_host()` (Option -> a degraded note), NOT
    // `cluster status`'s `discover_host()` (which fabricates `localhost` when
    // neither the kubeconfig server URL nor a node InternalIP is readable).
    // This twin's primary consumer is a coding agent reading `--json`
    // (ADR-0021/0038), and a `localhost` URL that will not resolve is worse for
    // it than an explicit note saying the host could not be determined. It also
    // matches the `resolve_node_host()`+Option pattern #360 set for every
    // URL-producing path (`discover_api_url`) and the `api_base_endpoint`
    // row. `cluster status` stays human-facing and keeps its display
    // convenience.
    //
    // Resolved once, before the fan-out: both service reads and all three rows
    // must agree on the release's rendered name. This is a live path only --
    // `observability`'s `--dry-run` branch returns before reaching here.
    // `resolve_node_host()` needs no fullname, so it runs alongside the
    // resolution instead of behind it; only the service reads have to wait.
    let (fullname, host) = tokio::join!(
        release_fullname(&opts.namespace, &opts.release),
        resolve_node_host(),
    );
    let (ui_svc, langfuse_svc) = tokio::join!(
        fetch_service(opts, &fullname, "ui"),
        fetch_service(opts, &fullname, "langfuse-web"),
    );
    vec![
        service_surface(
            opts,
            &fullname,
            "ui",
            "Curie Console",
            ui_svc.as_deref(),
            host.as_deref(),
            true,
        ),
        service_surface(
            opts,
            &fullname,
            "langfuse-web",
            "Langfuse UI (traces / cost / evals)",
            langfuse_svc.as_deref(),
            host.as_deref(),
            false,
        ),
        api_base_endpoint(opts, &fullname, ui_svc.as_deref(), host.as_deref()),
    ]
}

/// The read-only commands `curie cluster observability` runs (and prints under
/// `--dry-run`).
///
/// A superset of what actually runs, not a 1:1 trace: `resolve_node_host` only
/// falls through to `nodes_cmd()` when `kubeconfig_host_cmd()` yields no host.
pub fn observability_commands(o: &CommonOpts, fullname: &ReleaseFullname) -> Vec<OpsCommand> {
    vec![
        kubeconfig_host_cmd(),
        nodes_cmd(),
        svc_cmd(o, fullname, "ui"),
        svc_cmd(o, fullname, "langfuse-web"),
    ]
}

/// `cluster observability`: resolve the release's observability surfaces with
/// the same discovery `cluster status` does, and return them for `emit`.
///
/// Agent-first: a browser is opened only when the human passes `--open`, and
/// never under `--json`.
pub async fn observability(
    opts: CommonOpts,
    open: bool,
) -> Result<crate::observability::ObservabilityOutput> {
    if opts.dry_run {
        // No cluster call, so no discovery: the printed names follow the
        // chart's no-override rule and an override install renders them
        // differently. `dry_run_fullname` emits that caveat with the value.
        let fullname = dry_run_fullname(&opts.release);
        return Ok(crate::observability::ObservabilityOutput::DryRun(
            crate::ui::DryRunPlan {
                lines: observability_commands(&opts, &fullname)
                    .iter()
                    .map(|cmd| cmd.display())
                    .collect(),
            },
        ));
    }
    require_on_path("kubectl")?;
    let surfaces = cluster_observability_endpoints(&opts).await;
    let ui = crate::ui::ui();
    crate::observability::open_endpoints(&surfaces, open, ui.json()).await;
    // The cluster counterpart of the local tier's hint: stderr guidance, not
    // payload, since resolving a service says nothing about whether the release
    // is actually serving.
    ui.note("start these surfaces with `curie cluster up` if they are unreachable");
    Ok(crate::observability::ObservabilityOutput::Surfaces(
        surfaces,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops::testsupport::*;

    // #707 ownership-aware teardown. `down` deletes only the namespaces THIS
    // release created (carrying the release-scoped ownership label `up` stamped),
    // instead of the old hardcoded `curie agent-sandbox-system` literal sweep.
    // A pre-existing (unlabeled) namespace is left untouched.
    #[test]
    fn down_deletes_only_release_owned_namespaces_by_label() {
        let cmds = down_commands(&common_distinct_release());
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0].display(), "helm uninstall prod-release -n agent-ns");
        let sweep = cmds[1].display();
        // Label-selector-scoped delete keyed on THIS release's ownership labels.
        // #1654: the selector is the conjunction of release name AND install
        // namespace, so it cannot reach another release's namespaces.
        assert_eq!(
            sweep,
            "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found"
        );
        // Negative case: the pre-existing shared namespace is no longer an
        // unconditional delete target (that would strand pre-existing state).
        assert!(!sweep.contains("agent-sandbox-system"), "{sweep}");
        // ignore-not-found preserved so a partial teardown stays re-runnable.
        assert!(sweep.contains("--ignore-not-found"), "{sweep}");
    }

    // #1654 cross-release teardown scope. Two independent Curie installs on one
    // cluster normally BOTH take the default release name `curie` and differ
    // only in their install namespace, so a sweep selector keyed on the release
    // name alone matched the OTHER install's namespaces and deleted them
    // (observed live: `agent-sandbox-system` stamped `created-by=curie` while
    // annotated `meta.helm.sh/release-namespace: curie-other`, swept by an
    // unrelated release's `cluster down`, killing a running bot).
    //
    // Modeled BEHAVIORALLY rather than by string comparison: stamp release B's
    // namespace, build release A's sweep selector, then evaluate that selector
    // against those labels the way kubectl would. A FAILURE on the first
    // assertion means release A's teardown can delete a namespace release B
    // created -- the #1654 defect itself, which is what dropping `created-in`
    // from either the stamp or the selector reintroduces. The second assertion
    // is the anti-vacuity control: a selector that matched nothing at all would
    // also pass the first one, so release A must still sweep its OWN namespace.
    #[test]
    fn down_selector_never_matches_another_releases_namespace() {
        // Release B: name `curie`, installed into `curie-other`, and it created
        // the shared-looking `agent-sandbox-system`.
        let b = CommonOpts {
            namespace: "curie-other".into(),
            release: "curie".into(),
            dry_run: false,
        };
        let b_stamp = ownership_label_commands(
            &ns_common(&b, "agent-sandbox-system", false),
            &b.namespace,
            false,
        );
        assert_eq!(b_stamp.len(), 1);
        let b_labels = parse_stamped_labels(&b_stamp[0]);

        // Release A: the SAME release name, installed into a different
        // namespace. Its teardown sweep must not reach release B's namespace.
        let a = CommonOpts {
            namespace: "curie-a".into(),
            release: "curie".into(),
            dry_run: false,
        };
        let a_sweep = down_commands(&a);
        let a_terms = parse_selector_terms(&a_sweep[1]);
        assert!(
            !selector_matches(&a_terms, &b_labels),
            "release A's sweep selector {a_terms:?} must NOT match release B's namespace labels \
             {b_labels:?}; matching means one install's `cluster down` deletes another live \
             install's namespaces (#1654)"
        );

        // Anti-vacuity: release A must still sweep a namespace A itself created.
        let a_stamp = ownership_label_commands(
            &ns_common(&a, "agent-sandbox-system", false),
            &a.namespace,
            false,
        );
        assert_eq!(a_stamp.len(), 1);
        let a_labels = parse_stamped_labels(&a_stamp[0]);
        assert!(
            selector_matches(&a_terms, &a_labels),
            "release A's sweep selector {a_terms:?} must still match its OWN stamp {a_labels:?}; \
             a selector that matches nothing is not a fix"
        );
    }

    // #707 code-reviewer finding A2: `agent-sandbox-system` is chart-conditional
    // (created only when `agentSandbox.controller.deploy` is true), so under a
    // `--set agentSandbox.controller.deploy=false` release it is absent both
    // BEFORE and AFTER the helm install. `up()` gates the actual stamp attempt
    // on `should_stamp_ownership(existed_before, exists_after)`, re-probed AFTER
    // `cmds` executes -- this is the pure decision table that gate encodes, unit
    // tested directly since `up()` itself needs a live cluster to exercise.
    #[test]
    fn should_stamp_ownership_only_when_created_by_this_run() {
        // Created by this run: absent before, present after -> stamp.
        assert!(
            should_stamp_ownership(false, true),
            "a namespace this run created must be stamped"
        );
        // Pre-existing: present before (and therefore still present after) ->
        // never stamp, regardless of the post-install probe.
        assert!(
            !should_stamp_ownership(true, true),
            "a pre-existing namespace must never be stamped"
        );
        // controller.deploy=false: absent before AND still absent after -- the
        // chart never created it (e.g. agent-sandbox-system with the sandbox
        // controller subchart disabled). Must not stamp: `kubectl label
        // namespace <missing>` would fail and break `up`.
        assert!(
            !should_stamp_ownership(false, false),
            "a namespace the chart never created must not be stamped"
        );
    }

    // #767 fail-forward teardown (aggregation hardened by #768). PRODUCTION
    // SYMBOLS: two pure functions plus three small enums so `cluster down` runs
    // both teardown steps to completion and then decides the exit from the
    // combined result, instead of bailing the instant `helm uninstall` returns a
    // non-"not found" nonzero exit:
    //
    //   enum HelmOutcome { Removed, Absent, Failed }
    //   enum SweepOutcome { Removed, NoMatch, Failed }
    //   enum TeardownStep { HelmUninstall, NamespaceSweep }  // derive Debug, PartialEq, Eq
    //   fn outstanding_steps(helm: HelmOutcome, sweep: SweepOutcome) -> Vec<TeardownStep>
    //   fn resume_command(remaining: &[TeardownStep], o: &CommonOpts) -> String
    //
    // `outstanding_steps` is the pure decision function: Removed/Absent helm is
    // done, Failed helm leaves HelmUninstall outstanding; Removed/NoMatch sweep
    // is done, Failed sweep leaves NamespaceSweep outstanding; order is
    // HelmUninstall before NamespaceSweep (matching `down_commands` order).
    // `resume_command` maps the outstanding steps back to the matching
    // `down_commands(o)` entries. When BOTH steps are outstanding it emits the
    // HELM UNINSTALL FIRST, then the namespace sweep: helm first because Helm's
    // release metadata lives as Secrets inside the release namespace, so
    // sweeping first would destroy it and orphan the chart's cluster-scoped
    // resources. #768: the two commands are still run unconditionally (never
    // gated behind "&&", which would let a repeated helm failure block the
    // sweep), but each command's own exit status is now captured into a shell
    // variable ($?), and the resume line ends with a boolean expression that is
    // nonzero unless BOTH captured statuses were 0 -- fixing the old "; " join's
    // silent-exit-0-on-helm-failure bug without reintroducing the "&&" fail-hard
    // hazard.

    // AC: an exact resumable cleanup command is surfaced. `resume_command`
    // renders the exact copy-pasteable line for a helm-only remainder.
    #[test]
    fn resume_command_helm_only_is_the_exact_uninstall_line() {
        let o = common_distinct_release();
        let cmd = resume_command(&[TeardownStep::HelmUninstall], &o);
        assert_eq!(cmd, "helm uninstall prod-release -n agent-ns");
    }

    // AC (preserves #707): a sweep-only remainder renders the exact
    // ownership-label-scoped delete, never an unscoped `delete namespace`.
    #[test]
    fn resume_command_sweep_only_is_label_scoped() {
        let o = common_distinct_release();
        let cmd = resume_command(&[TeardownStep::NamespaceSweep], &o);
        assert_eq!(
            cmd,
            "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found"
        );
        // #707 ownership-scope invariant: the sweep stays keyed on THIS release's
        // label and is never widened to an unconditional namespace delete.
        assert!(
            cmd.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "{cmd}"
        );
        assert!(
            !cmd.contains("delete namespace prod-release"),
            "must never be an unscoped delete: {cmd}"
        );
        // ignore-not-found preserved so the resume stays re-runnable.
        assert!(cmd.contains("--ignore-not-found"), "{cmd}");
    }

    // AC + review (#768 aggregation): both steps outstanding -> the HELM
    // UNINSTALL FIRST, then the namespace sweep, both run unconditionally, with
    // each captured exit status aggregated into a nonzero-unless-both-succeeded
    // trailing expression. Ordering rationale: Helm stores its release metadata
    // as Secrets inside the release namespace, and the chart owns cluster-scoped
    // resources (ClusterRole/ClusterRoleBinding); sweeping the namespace first
    // would destroy that metadata, the subsequent helm uninstall would report
    // "not found", and those cluster-scoped resources would be orphaned.
    // Aggregation rationale: a plain "; " join runs both commands unconditionally
    // but only returns the LAST command's exit status, so a helm failure
    // followed by a successful sweep silently reads as exit 0. A " && " join
    // would fix the status but let a repeated helm failure short-circuit and
    // block the compute-stopping sweep, so it is explicitly rejected. The
    // wrapper here keeps both commands unconditional and only combines their
    // CAPTURED statuses afterward.
    #[test]
    fn resume_command_both_runs_both_steps_helm_first() {
        let o = common_distinct_release();
        let cmd = resume_command(
            &[TeardownStep::HelmUninstall, TeardownStep::NamespaceSweep],
            &o,
        );
        let helm_cmd = "helm uninstall prod-release -n agent-ns";
        let sweep_cmd =
            "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found";
        assert_eq!(
            cmd,
            format!(
                "{helm_cmd}; s1=$?; {sweep_cmd}; s2=$?; [ \"$s1\" -eq 0 ] && [ \"$s2\" -eq 0 ]"
            )
        );
        // The helm uninstall must appear BEFORE the sweep, so Helm's release
        // metadata (in the release namespace) survives long enough for helm to
        // remove the chart-owned cluster-scoped resources.
        let helm_at = cmd.find("helm uninstall").expect("helm present");
        let sweep_at = cmd.find("kubectl delete namespace").expect("sweep present");
        assert!(
            helm_at < sweep_at,
            "helm uninstall must run before the sweep: {cmd}"
        );
        // The two commands themselves are joined by a plain "; " immediately
        // followed by capturing their own exit status -- NOT " && " -- so a
        // repeated helm failure can never short-circuit and block the sweep.
        assert!(
            cmd.starts_with(&format!("{helm_cmd}; s1=$?; {sweep_cmd}; s2=$?;")),
            "helm and the sweep must both run unconditionally, not gated behind &&: {cmd}"
        );
        // The trailing "&&" combines two `[ ... -eq 0 ]` tests over the ALREADY
        // captured statuses; it does not gate whether the sweep runs, only
        // whether the whole line's own exit status reports both as successful.
        assert!(
            cmd.ends_with("[ \"$s1\" -eq 0 ] && [ \"$s2\" -eq 0 ]"),
            "the resume line's own exit status must aggregate both captured statuses: {cmd}"
        );
        // The sweep half stays label-scoped even when combined with helm.
        assert!(
            cmd.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "{cmd}"
        );
        assert!(cmd.contains("--ignore-not-found"), "{cmd}");
    }

    // Nothing remaining -> nothing to resume -> the empty string.
    #[test]
    fn resume_command_empty_remainder_is_empty_string() {
        let o = common_distinct_release();
        assert_eq!(resume_command(&[], &o), "");
    }

    // AC: helm-uninstall failure no longer aborts before the sweep. The decision
    // table proves the sweep runs (and is scored) even when helm failed, and
    // that a swept-but-helm-stale run surfaces only the helm step as outstanding.
    #[test]
    fn outstanding_steps_decision_table() {
        use TeardownStep::*;

        // Happy path: helm removed, sweep clean -> nothing outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Removed, SweepOutcome::Removed),
            Vec::<TeardownStep>::new()
        );
        // Already-absent release, sweep clean -> still nothing outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Absent, SweepOutcome::Removed),
            Vec::<TeardownStep>::new()
        );
        // Fail-forward win: helm failed but the sweep removed the namespaces
        // (compute stopped); only the stale helm release record remains.
        assert_eq!(
            outstanding_steps(HelmOutcome::Failed, SweepOutcome::Removed),
            vec![HelmUninstall]
        );
        // Nothing could be removed; the API server is still unreachable.
        assert_eq!(
            outstanding_steps(HelmOutcome::Failed, SweepOutcome::Failed),
            vec![HelmUninstall, NamespaceSweep]
        );
        // Helm removed but the sweep failed -> only the sweep is outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Removed, SweepOutcome::Failed),
            vec![NamespaceSweep]
        );
        // #768: a zero-match sweep (pre-existing namespace, never labeled by
        // #707) is a completed step, same as an actual removal -- there is
        // nothing left for THIS step to do, so it must not be outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Removed, SweepOutcome::NoMatch),
            Vec::<TeardownStep>::new()
        );
        // #768: helm failed and the sweep matched nothing -> only the helm
        // record is outstanding; the sweep is done (there was nothing to sweep),
        // but critically it did NOT stop any compute, unlike the Removed case
        // above.
        assert_eq!(
            outstanding_steps(HelmOutcome::Failed, SweepOutcome::NoMatch),
            vec![HelmUninstall]
        );
    }

    // #767 fail-forward HARDENING. PRODUCTION SYMBOLS: a connectivity
    // classifier and the pure teardown-result decision that `down()` calls
    // AFTER running both teardown steps and capturing their outcomes plus
    // stderr:
    //
    //   fn is_connectivity_failure(stderr: &str) -> bool
    //   fn teardown_result(
    //       helm: HelmOutcome,
    //       sweep: SweepOutcome,
    //       helm_err: &str,
    //       sweep_err: &str,
    //       o: &CommonOpts,
    //   ) -> anyhow::Result<ClusterDownOutput>
    //
    // `is_connectivity_failure` lower-cases stderr and matches only concrete
    // network signatures (connection refused, tls handshake, no route to
    // host, i/o timeout, network is unreachable, could not connect, dial tcp,
    // connection reset, context deadline exceeded, and -- since a kubectl call
    // site started classifying with it in #1351 -- client-go's own "connection
    // to the server" refusal wording). Bare "unreachable" and
    // "timeout" are deliberately excluded: Helm wraps permanent
    // auth/exec-plugin/kubeconfig errors as `Kubernetes cluster unreachable:
    // ...`, so that generic prefix alone is not a reliable transient signal.
    // Permanent errors (forbidden, rbac, unauthorized, invalid, not
    // authorized) stay false so they are never mislabeled retryable.
    // `teardown_result` is the pure decision: an empty remainder returns
    // Ok(Down{release_was_absent}); otherwise it builds the resume command (via
    // resume_command), composes it INTO the message so the human Display carries
    // it (P1: `main` renders Display and drops the fix), attaches it as the fix for
    // `--json`, and tags the exit class Transient IFF an outstanding failed step's
    // stderr is a connectivity failure, else a plain Failure (P2).

    // P2: recognized connectivity stderrs are transient (retryable).
    #[test]
    fn is_connectivity_failure_true_for_unreachable_markers() {
        assert!(is_connectivity_failure(
            "Kubernetes cluster unreachable: Get \"https://h:6443/version\": net/http: TLS handshake timeout"
        ));
        assert!(is_connectivity_failure(
            "dial tcp 1.2.3.4:6443: connect: connection refused"
        ));
        assert!(is_connectivity_failure("i/o timeout"));
        assert!(is_connectivity_failure("no route to host"));
    }

    // #1351: kubectl's own refusal wording, verbatim as kubectl v1.36.2 wrote it
    // to stderr against an unreachable apiserver during this ticket's live
    // reproduction. client-go separates the words, so the "connection refused"
    // marker above never appears in it and every unreadable-cluster kubectl read
    // classified as a permanent Failure until the "connection to the server"
    // marker was added.
    #[test]
    fn is_connectivity_failure_true_for_kubectls_own_refusal_wording() {
        assert!(is_connectivity_failure(
            "The connection to the server localhost:8080 was refused - did you specify the right host or port?"
        ));
    }

    // P2: permanent failures (RBAC, authz, invalid) are NOT connectivity, so they
    // must classify as a plain Failure, never a retryable transient.
    #[test]
    fn is_connectivity_failure_false_for_permanent_errors() {
        assert!(!is_connectivity_failure(
            "Error: query: failed to query with labels: namespaces is forbidden: User cannot list resource"
        ));
        assert!(!is_connectivity_failure("Error: rbac: access denied"));
        assert!(!is_connectivity_failure("error: You must be logged in"));
        assert!(!is_connectivity_failure(""));
    }

    // Codex P2: Helm wraps permanent errors (auth, exec-plugin, kubeconfig) as
    // "Kubernetes cluster unreachable: ...", so the bare "unreachable"/"timeout"
    // prefix is NOT a reliable transient signal. Only concrete network signatures
    // (connection refused, tls handshake, no route to host, i/o timeout, dial tcp,
    // network is unreachable, connection reset, context deadline exceeded) count;
    // a permanent error wearing Helm's generic prefix must classify FALSE.
    #[test]
    fn is_connectivity_failure_false_for_helm_wrapped_permanent_errors() {
        // Auth exec-plugin failure wrapped by Helm's generic prefix.
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable: Get \"https://h:6443/version\": getting credentials: exec plugin: exec: \"gke-gcloud-auth-plugin\": executable file not found in $PATH"
        ));
        // RBAC/authz failure wrapped by Helm's generic prefix.
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable: namespaces is forbidden: User cannot list resource \"namespaces\""
        ));
        // Bare unreachable with no concrete network signature at all.
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable"
        ));
    }

    // Review: a host that does not RESOLVE is a deterministic configuration error
    // (a bad kubeconfig hostname), not a transient network blip. Retrying cannot
    // fix it, so it must classify FALSE even though the stderr also carries the
    // "dial tcp" marker; otherwise automation retries forever instead of fixing
    // the context.
    #[test]
    fn is_connectivity_failure_false_for_permanent_host_resolution_errors() {
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable: Get \"https://bad-host:6443/version\": dial tcp: lookup bad-host: no such host"
        ));
        assert!(!is_connectivity_failure(
            "dial tcp: lookup bad-host on 127.0.0.53:53: no such host"
        ));
    }

    // #1230: a CLI that rejects its invocation prints the diagnosis FIRST and
    // its usage block last, so "last non-empty line" surfaces the boilerplate
    // and drops the only actionable line. This is the transcript that provoked
    // the issue -- `curie local up` reported "Run 'docker --help' for more
    // information" while the real cause sat four lines above it.
    #[test]
    fn failure_reason_skips_a_trailing_usage_block() {
        let stderr = "unknown flag: --profile\n\
                      \n\
                      Usage:  docker [OPTIONS] COMMAND [ARG...]\n\
                      \n\
                      Run 'docker --help' for more information\n";
        assert_eq!(failure_reason(stderr), "unknown flag: --profile");
    }

    // The same shape without a usage block: some rejections print only the
    // diagnosis plus a bare help pointer.
    #[test]
    fn failure_reason_skips_a_bare_help_pointer() {
        let stderr = "docker: 'compos' is not a docker command.\nSee 'docker --help'\n";
        assert_eq!(
            failure_reason(stderr),
            "docker: 'compos' is not a docker command."
        );
        let kubectl = "error: unknown flag: --foo\nSee 'kubectl get --help' for usage.\n";
        assert_eq!(failure_reason(kubectl), "error: unknown flag: --foo");
    }

    // REGRESSION GUARD, and the reason the naive inverse ("take the first
    // line") is wrong: helm prints warnings BEFORE its `Error:` line, so the
    // last line is the right answer there and must stay the right answer.
    #[test]
    fn failure_reason_keeps_the_last_line_when_a_warning_precedes_the_error() {
        let stderr = "WARNING: Kubernetes configuration file is group-readable\n\
                      Error: INSTALLATION FAILED: cannot re-use a name that is still in use\n";
        assert_eq!(
            failure_reason(stderr),
            "Error: INSTALLATION FAILED: cannot re-use a name that is still in use"
        );
    }

    // The unchanged base cases: one line is that line, nothing is the default.
    #[test]
    fn failure_reason_is_unchanged_for_single_line_and_empty_stderr() {
        assert_eq!(
            failure_reason("Error: release: not found\n"),
            "Error: release: not found"
        );
        assert_eq!(failure_reason(""), "command failed");
        assert_eq!(failure_reason("   \n\n  \n"), "command failed");
    }

    // The safety property: stripping must never blank the reason. A stderr that
    // is ONLY a usage block has no diagnosis to recover, so the result falls
    // back to today's last-line answer rather than to an empty string.
    #[test]
    fn failure_reason_falls_back_rather_than_blanking_on_a_pure_usage_block() {
        let stderr = "Usage:  docker [OPTIONS] COMMAND [ARG...]\n\
                      \n\
                      Run 'docker --help' for more information\n";
        assert_eq!(
            failure_reason(stderr),
            "Run 'docker --help' for more information"
        );
    }

    // Both teardown steps completed: success, release present.
    #[test]
    fn teardown_result_all_removed_is_success() {
        let o = common_distinct_release();
        let res = teardown_result(HelmOutcome::Removed, SweepOutcome::Removed, "", "", &o)
            .expect("a complete teardown is Ok");
        assert!(matches!(
            res,
            ClusterDownOutput::Down {
                release_was_absent: false
            }
        ));
    }

    // Already-absent release, sweep clean: success, release_was_absent true.
    #[test]
    fn teardown_result_absent_release_is_success_absent() {
        let o = common_distinct_release();
        let res = teardown_result(HelmOutcome::Absent, SweepOutcome::Removed, "", "", &o)
            .expect("an already-absent release still completes");
        assert!(matches!(
            res,
            ClusterDownOutput::Down {
                release_was_absent: true
            }
        ));
    }

    // P1 + P2: both steps failed on an unreachable API server. Transient (exit 3),
    // and the label-scoped resume command rides in BOTH the human Display message
    // (P1: `main` renders Display, so a no-json operator must still see it) and the
    // fix (for `--json`).
    #[test]
    fn teardown_result_connectivity_both_failed_is_transient_with_resume_in_message_and_fix() {
        let o = common_distinct_release();
        let helm_err =
            "Kubernetes cluster unreachable: Get \"https://h:6443/version\": net/http: TLS handshake timeout";
        let sweep_err = "Kubernetes cluster unreachable: connection refused";
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Failed,
            helm_err,
            sweep_err,
            &o,
        )
        .expect_err("an incomplete teardown is an error");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Transient);
        assert_eq!(class.code(), 3);

        // P1: the resume command is IN the human Display message, not only the fix.
        let shown = err.to_string();
        assert!(
            shown.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "the human message must carry the label-scoped resume command: {shown}"
        );

        // --json path: the fix carries the same label-scoped resume command.
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert!(
            fix.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "fix must carry the label-scoped resume command: {fix}"
        );
    }

    // Fail-forward win: helm failed (unreachable) but the sweep removed the
    // namespaces, so only the stale helm release record remains. Transient, the
    // message distinguishes the swept case (stable substring "swept"), and the
    // resume command is the helm-only line in both message and fix.
    #[test]
    fn teardown_result_connectivity_helm_only_failed_surfaces_swept_and_helm_resume() {
        let o = common_distinct_release();
        let helm_err = "Kubernetes cluster unreachable: net/http: TLS handshake timeout";
        let err = teardown_result(HelmOutcome::Failed, SweepOutcome::Removed, helm_err, "", &o)
            .expect_err("a stale helm record is still an incomplete teardown");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Transient);

        let shown = err.to_string();
        // Distinguishes the swept-but-helm-stale case.
        assert!(shown.contains("swept"), "{shown}");
        // Only the helm step is outstanding, so the helm-only resume line rides in
        // the message (P1) ...
        assert!(
            shown.contains("helm uninstall prod-release -n agent-ns"),
            "the message must carry the helm-only resume line: {shown}"
        );
        // ... and must not drag in the (completed) sweep command.
        assert!(
            !shown.contains("delete namespace"),
            "a swept run must not list the sweep as outstanding: {shown}"
        );
        // ... and the fix is exactly the helm-only line for `--json`.
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert_eq!(fix, "helm uninstall prod-release -n agent-ns");
    }

    // #768 core anti-regression: a zero-match sweep is NOT the same as an
    // actual removal. When Curie was installed into a pre-existing namespace
    // (#707 never labels it), the label-scoped sweep exits 0 with nothing
    // matched. Before #768 this was mapped to the same `SweepOutcome::Removed`
    // as a real deletion, so a failed helm uninstall paired with a zero-match
    // sweep produced the exact same "the run-created namespaces were swept"
    // message as a real removal, even though the pre-existing namespace's
    // workloads (and the failed release's compute) may still be running. This
    // test locks the fix: `SweepOutcome::NoMatch` must NEVER be worded as
    // "swept", and the message must say plainly that no compute was stopped.
    #[test]
    fn teardown_result_zero_match_sweep_never_claims_compute_was_removed() {
        let o = common_distinct_release();
        let helm_err = "Kubernetes cluster unreachable: net/http: TLS handshake timeout";
        let err = teardown_result(HelmOutcome::Failed, SweepOutcome::NoMatch, helm_err, "", &o)
            .expect_err("a stale helm record is still an incomplete teardown");

        let shown = err.to_string();
        // The core anti-regression: must NOT claim the run-created namespaces
        // were swept, since nothing actually matched the selector.
        assert!(
            !shown.contains("were swept"),
            "a zero-match sweep must never be worded as an actual removal: {shown}"
        );
        // Must plainly say no compute was stopped, so an operator does not
        // mistakenly believe the failed release's workloads are gone.
        assert!(
            shown.to_lowercase().contains("no compute was stopped")
                || shown.to_lowercase().contains("no run-created namespaces matched"),
            "the message must not imply compute was removed when the sweep matched nothing: {shown}"
        );
        // The sweep itself is done (nothing to sweep), so only the helm step is
        // outstanding -- the resume command is the helm-only line, exactly like
        // the real-removal ("swept") case, in both the message and the fix.
        assert!(
            shown.contains("helm uninstall prod-release -n agent-ns"),
            "the message must carry the helm-only resume line: {shown}"
        );
        assert!(
            !shown.contains("delete namespace"),
            "a zero-match sweep has nothing outstanding, so the sweep must not be listed as a resume step: {shown}"
        );
        let (_, fix) = crate::exit::classify(&err);
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert_eq!(fix, "helm uninstall prod-release -n agent-ns");
    }

    // #768: a zero-match sweep still counts as a COMPLETED step when helm itself
    // succeeds -- the pre-existing namespace was correctly left untouched (#707),
    // so `cluster down` overall succeeds exactly as it would if the release had
    // created and then swept its own namespace.
    #[test]
    fn teardown_result_zero_match_sweep_with_helm_removed_is_still_success() {
        let o = common_distinct_release();
        let res = teardown_result(HelmOutcome::Removed, SweepOutcome::NoMatch, "", "", &o)
            .expect("a zero-match sweep alongside a clean helm uninstall is a complete teardown");
        assert!(matches!(
            res,
            ClusterDownOutput::Down {
                release_was_absent: false
            }
        ));
    }

    // P2: permanent failures (RBAC, authz) are NOT retryable. Both steps failed
    // with a forbidden error, so the class is a plain Failure (exit 1), NOT a
    // transient, while still failing forward: the resume command rides in message
    // and fix.
    #[test]
    fn teardown_result_permanent_failure_is_plain_failure_not_transient() {
        let o = common_distinct_release();
        let forbidden =
            "Error: query: failed to query with labels: namespaces is forbidden: User cannot list resource \"namespaces\"";
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Failed,
            forbidden,
            forbidden,
            &o,
        )
        .expect_err("an incomplete teardown is an error");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Failure,
            "an RBAC/permanent failure must classify as Failure, not Transient"
        );
        assert_eq!(class.code(), 1);
        assert_ne!(class, crate::exit::ExitClass::Transient);

        // Fail-forward still surfaces the resume command in message and fix.
        let shown = err.to_string();
        assert!(
            shown.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "even a permanent failure surfaces the label-scoped resume command: {shown}"
        );
        // Codex P2: the permanent-failure message must surface the underlying
        // reason drawn from the failed step's stderr, not a generic line that
        // drops it to --debug plumbing. The operator must see WHY teardown failed.
        assert!(
            shown.contains("forbidden"),
            "the permanent-failure message must surface the underlying stderr reason: {shown}"
        );
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert!(
            fix.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "{fix}"
        );
    }

    // Codex P2: in a MIXED failure the surfaced reason must be the failure that
    // DETERMINES the exit class (the permanent, actionable problem the operator
    // must fix), not merely the first failed step. Here helm fails transiently
    // (connectivity) but the sweep then fails permanently (RBAC): the permanent
    // sweep failure blocks retry, so the class is Failure (exit 1), and the
    // message must name the permanent sweep reason (`forbidden`), not hide it
    // behind the transient helm connectivity reason.
    #[test]
    fn teardown_result_mixed_failure_surfaces_the_determining_permanent_reason() {
        let o = common_distinct_release();
        let helm_err =
            "Error: Kubernetes cluster unreachable: Get \"https://h:6443/version\": dial tcp 10.0.0.1:6443: connect: connection refused";
        let sweep_err =
            "Error: namespaces is forbidden: User \"sa\" cannot delete resource \"namespaces\" in API group";
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Failed,
            helm_err,
            sweep_err,
            &o,
        )
        .expect_err("an incomplete teardown is an error");

        let (class, _fix) = crate::exit::classify(&err);
        // The permanent sweep failure blocks retry: Failure (exit 1), not Transient.
        assert_eq!(
            class,
            crate::exit::ExitClass::Failure,
            "the permanent sweep failure must classify as Failure, not Transient"
        );
        assert_eq!(class.code(), 1);
        assert_ne!(class, crate::exit::ExitClass::Transient);

        // The message must surface the DETERMINING permanent reason (`forbidden`),
        // not just the transient helm connectivity reason. It is acceptable if the
        // message also includes the helm reason, but `forbidden` must be present.
        let shown = err.to_string();
        assert!(
            shown.contains("forbidden"),
            "the determining permanent sweep reason must be surfaced: {shown}"
        );
    }

    // #1251: `is_usage_header` slices `line[..6]` guarded only by a BYTE length
    // check, but the char-boundary requirement is not a byte count -- a
    // diagnosis line where the 7th byte lands inside a multi-byte character
    // panics with "byte index 6 is not a char boundary" instead of returning a
    // reason. Reverting the fix turns this RED.
    #[test]
    fn failure_reason_does_not_panic_on_multibyte_diagnosis_line() {
        let stderr = "abcdeé: dépôt manquant\n";
        assert_eq!(failure_reason(stderr), "abcdeé: dépôt manquant");
    }

    // #1251, the exact production path: `run_capture` decodes subprocess
    // stderr with `String::from_utf8_lossy`, so an invalid byte from a real
    // CLI becomes a U+FFFD replacement character (3 bytes) rather than a
    // valid multi-byte character typed by hand. Built via the same lossy
    // conversion so the test documents the real path instead of hardcoding
    // the glyph. Reverting the fix turns this RED (this is the transcript
    // from the issue: `printf "abcde\xe9fghij\n"` on a fake docker stderr).
    #[test]
    fn failure_reason_does_not_panic_on_lossy_replacement_character() {
        let stderr = String::from_utf8_lossy(b"abcde\xe9fghij\n").into_owned();
        let expected = stderr.trim().to_string();
        assert_eq!(failure_reason(&stderr), expected);
    }

    // Guard that the fix does not also break detection: a genuine `Usage:`
    // header, followed by multi-byte content the header wraps, must still be
    // recognized and cut so the diagnosis above it is what gets returned.
    #[test]
    fn failure_reason_still_cuts_a_multibyte_usage_header() {
        let stderr = "erreur: dépôt manquant\n\
                      Usage: café [OPTIONS] COMMAND\n\
                      Run 'café --help' for more information\n";
        assert_eq!(failure_reason(stderr), "erreur: dépôt manquant");
    }

    // Guard that the fix does not also break detection: a line shorter than
    // six bytes is excluded by the length check outright, and a line that is
    // exactly six bytes but is not the `usage:` prefix must not be
    // misclassified as a usage header either. The candidate line sits above a
    // real diagnosis line so a misclassification is observable through the
    // cut: an always-true predicate would treat the candidate as the usage
    // header, truncate the diagnosis after it, and return the candidate line
    // instead of the diagnosis.
    #[test]
    fn failure_reason_does_not_misclassify_short_or_nonmatching_multibyte_lines() {
        // "café!" is exactly six bytes but is not the usage prefix, so it must not
        // truncate the diagnosis that follows it.
        assert_eq!(
            failure_reason("café!\nerreur: dépôt manquant\n"),
            "erreur: dépôt manquant"
        );
        assert_eq!(
            failure_reason("café\nerreur: dépôt manquant\n"),
            "erreur: dépôt manquant"
        );
    }

    // #1251, same defect class via `teardown_result`: `helm_err`/`sweep_err`
    // come from `run_capture`'s lossy stderr too, so a failed helm step whose
    // stderr trips the same byte-boundary case must return a result carrying
    // a reason rather than panicking through `teardown_result` ->
    // `failure_reason`. Reverting the fix turns this RED.
    #[test]
    fn teardown_result_helm_failure_with_multibyte_stderr_does_not_panic() {
        let o = common_distinct_release();
        let helm_err = String::from_utf8_lossy(b"abcde\xe9fghij\n").into_owned();
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Removed,
            &helm_err,
            "",
            &o,
        )
        .expect_err("a failed helm step is an incomplete teardown");
        let shown = err.to_string();
        assert!(
            shown.contains(helm_err.trim()),
            "the message must surface the helm stderr reason: {shown}"
        );
    }

    #[test]
    fn status_lists_the_readonly_commands() {
        let cmds = status_commands(&common(), &fullname());
        let lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        assert_eq!(lines[0], "helm status curie -n curie");
        assert_eq!(lines[1], "kubectl get pods -n curie -o json");
        assert_eq!(lines[2], "kubectl get svc curie-ui -n curie -o json");
        assert_eq!(
            lines[3],
            "kubectl get svc curie-langfuse-web -n curie -o json"
        );
        assert!(
            lines[4].starts_with("kubectl config view --minify -o "),
            "{}",
            lines[4]
        );
    }

    #[test]
    fn host_from_server_url_strips_scheme_and_port() {
        assert_eq!(
            host_from_server_url("https://10.1.2.3:6443").as_deref(),
            Some("10.1.2.3")
        );
        assert_eq!(
            host_from_server_url("https://k3s.local:6443").as_deref(),
            Some("k3s.local")
        );
        assert_eq!(
            host_from_server_url("https://host").as_deref(),
            Some("host")
        );
        assert_eq!(host_from_server_url(""), None);
    }

    #[test]
    fn host_from_server_url_parses_bracketed_ipv6() {
        assert_eq!(
            host_from_server_url("https://[::1]:6443").as_deref(),
            Some("::1")
        );
        assert_eq!(
            host_from_server_url("https://[2001:db8::1]:8443").as_deref(),
            Some("2001:db8::1")
        );
        assert_eq!(
            host_from_server_url("https://[::1]").as_deref(),
            Some("::1")
        );
    }

    #[test]
    fn parse_service_reads_type_and_ports() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        assert_eq!(
            parse_service(json),
            Some(("NodePort".into(), Some(31234), 80))
        );
        let cluster = r#"{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}"#;
        assert_eq!(
            parse_service(cluster),
            Some(("ClusterIP".into(), None, 3000))
        );
    }

    #[test]
    fn node_internal_ip_finds_first_internal_address() {
        let json = r#"{"items":[{"status":{"addresses":[
            {"type":"Hostname","address":"node1"},
            {"type":"InternalIP","address":"192.168.1.5"}
        ]}}]}"#;
        assert_eq!(node_internal_ip(json).as_deref(), Some("192.168.1.5"));
    }

    #[test]
    fn pod_summary_does_not_panic_on_empty() {
        // No items: empty items array.
        let items: Vec<serde_json::Value> = Vec::new();
        let _ = collect_pod_summary(&items);
    }

    #[test]
    fn pod_summary_excludes_completed_and_terminating() {
        let json = r#"[
            {"metadata":{"name":"api0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"api1"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"worker0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"worker1"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"dispatcher0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"ui0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"postgres0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"valkey0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"langfuse0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"otel0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"runnerold","deletionTimestamp":"2024-01-01T00:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"preflight0"},"status":{"phase":"Succeeded","reason":"Completed","containerStatuses":[{"ready":false,"restartCount":0}]}},
            {"metadata":{"name":"preflight1"},"status":{"phase":"Succeeded","reason":"Completed","containerStatuses":[{"ready":false,"restartCount":0}]}},
            {"metadata":{"name":"job0"},"status":{"phase":"Succeeded","containerStatuses":[{"ready":false,"restartCount":0}]}}
        ]"#;

        let items: Vec<serde_json::Value> = serde_json::from_str(json).unwrap();
        let (_, ready, total, unhealthy) = collect_pod_summary(&items);
        assert_eq!((ready, total, unhealthy), (10, 10, vec![]));
    }

    #[test]
    fn pod_summary_flags_genuinely_unhealthy_steady_state_pod() {
        let json = r#"[
            {"metadata":{"name":"api0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"worker0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"dispatcher0"},"status":{"phase":"Pending","containerStatuses":[]}}
        ]"#;

        let items: Vec<serde_json::Value> = serde_json::from_str(json).unwrap();
        let (_, ready, total, unhealthy) = collect_pod_summary(&items);
        assert_eq!(
            (ready, total, unhealthy),
            (2, 3, vec!["dispatcher0".to_string()])
        );
    }

    #[test]
    fn ui_api_url_nodeport_with_host_builds_proxy_url() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        let url = ui_api_url_from_parts(json, Some("10.0.0.5")).expect("should build a proxy URL");
        assert_eq!(url, "http://10.0.0.5:31234/api");
    }

    #[test]
    fn node_http_url_brackets_ipv6_and_appends_path() {
        assert_eq!(
            node_http_url("10.0.0.5", 31234, "/api"),
            "http://10.0.0.5:31234/api"
        );
        assert_eq!(
            node_http_url("::1", 31234, "/api"),
            "http://[::1]:31234/api"
        );
        assert_eq!(
            node_http_url("node.local", 30080, "/?api=1"),
            "http://node.local:30080/?api=1"
        );
        assert_eq!(
            node_http_url("10.0.0.5", 30080, ""),
            "http://10.0.0.5:30080"
        );
    }

    #[test]
    fn ui_api_url_ipv6_host_is_bracketed() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        let url = ui_api_url_from_parts(json, Some("::1")).expect("should build a proxy URL");
        assert_eq!(url, "http://[::1]:31234/api");
    }

    #[test]
    fn ui_api_url_nodeport_without_host_errs_mentioning_api_url() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        let err = ui_api_url_from_parts(json, None).expect_err("a missing host must error");
        assert!(err.to_string().contains("--api-url"), "{err}");
    }

    #[test]
    fn ui_api_url_nodeport_without_assigned_nodeport_errs_mentioning_api_url() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80}]}}"#;
        let err = ui_api_url_from_parts(json, Some("10.0.0.5"))
            .expect_err("an unassigned nodePort must error");
        assert!(err.to_string().contains("--api-url"), "{err}");
    }

    #[test]
    fn ui_api_url_clusterip_errs_mentioning_no_expose_and_api_url() {
        let json = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        let err = ui_api_url_from_parts(json, Some("10.0.0.5"))
            .expect_err("a non-NodePort service must error");
        let msg = err.to_string();
        assert!(msg.contains("--no-expose"), "{msg}");
        assert!(msg.contains("--api-url"), "{msg}");
    }

    #[test]
    fn api_url_falls_back_to_the_api_service_nodeport() {
        // ui.deploy=false is a supported way to run a Slack-only bot; when the
        // UI is absent the api service's own NodePort is still a valid target.
        // No /api suffix -- that path is the UI's proxy, not the API itself.
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":8000,"nodePort":30799}]}}"#;
        let url = api_url_from_parts(json, Some("10.0.0.5")).expect("should build a direct URL");
        assert_eq!(url, "http://10.0.0.5:30799");
    }

    #[test]
    fn api_url_fallback_declines_a_clusterip_api_service() {
        // ClusterIP is unreachable from outside the cluster, so there is no URL
        // to return; the caller turns this into an actionable error.
        let json = r#"{"spec":{"type":"ClusterIP","ports":[{"port":8000}]}}"#;
        assert!(api_url_from_parts(json, Some("10.0.0.5")).is_none());
    }

    #[test]
    fn api_url_fallback_declines_without_a_host() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":8000,"nodePort":30799}]}}"#;
        assert!(api_url_from_parts(json, None).is_none());
    }

    #[test]
    fn ui_api_url_malformed_json_errs_mentioning_api_url() {
        let err =
            ui_api_url_from_parts("", Some("10.0.0.5")).expect_err("malformed JSON must error");
        assert!(err.to_string().contains("--api-url"), "{err}");
    }

    #[test]
    fn resolve_service_endpoint_nodeport_builds_the_node_url() {
        // api=true appends the Console's `/?api=1` suffix path.
        assert_eq!(
            resolve_service_endpoint(NODEPORT_SVC, "10.0.0.5", true),
            ServiceEndpoint::NodePortUrl("http://10.0.0.5:31234/?api=1".to_string())
        );
        // api=false yields the bare node URL -- no `?api=1`.
        assert_eq!(
            resolve_service_endpoint(NODEPORT_SVC, "10.0.0.5", false),
            ServiceEndpoint::NodePortUrl("http://10.0.0.5:31234".to_string())
        );
        // An IPv6 host is bracketed so the authority stays valid (via node_http_url).
        assert_eq!(
            resolve_service_endpoint(NODEPORT_SVC, "::1", true),
            ServiceEndpoint::NodePortUrl("http://[::1]:31234/?api=1".to_string())
        );
    }

    #[test]
    fn resolve_service_endpoint_clusterip_yields_a_port_forward_hint() {
        // ClusterIP: not node-exposed, so the caller must port-forward. Each
        // privileged service port maps to a deterministic non-privileged local port.
        let http_clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        let http_endpoint = resolve_service_endpoint(http_clusterip, "10.0.0.5", true);
        let ServiceEndpoint::PortForwardHint {
            local: http_local,
            port: http_port,
        } = http_endpoint
        else {
            panic!("ClusterIP services must yield a port-forward hint");
        };
        assert_eq!(http_local, 18080);
        assert_eq!(http_port, 80);
        assert!(
            http_local >= 1024,
            "the local port must be bindable by a non-root user"
        );

        let https_clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":443}]}}"#;
        let https_endpoint = resolve_service_endpoint(https_clusterip, "10.0.0.5", true);
        let ServiceEndpoint::PortForwardHint {
            local: https_local,
            port: https_port,
        } = https_endpoint
        else {
            panic!("ClusterIP services must yield a port-forward hint");
        };
        assert_eq!(https_local, 18443);
        assert_eq!(https_port, 443);
        assert!(
            https_local >= 1024,
            "the local port must be bindable by a non-root user"
        );

        // An absent port parses as 0, which falls back to local port 8080.
        let no_port = r#"{"spec":{"type":"ClusterIP","ports":[{}]}}"#;
        assert_eq!(
            resolve_service_endpoint(no_port, "10.0.0.5", true),
            ServiceEndpoint::PortForwardHint {
                local: 8080,
                port: 0
            }
        );
    }

    #[test]
    fn resolve_service_endpoint_boundary_variants_do_not_panic() {
        // NodePort type but the nodePort is not assigned yet (release settling).
        let unassigned = r#"{"spec":{"type":"NodePort","ports":[{"port":80}]}}"#;
        assert_eq!(
            resolve_service_endpoint(unassigned, "10.0.0.5", true),
            ServiceEndpoint::UnassignedNodePort
        );
        // Malformed / empty JSON is unreadable, never a panic.
        assert_eq!(
            resolve_service_endpoint("", "10.0.0.5", true),
            ServiceEndpoint::Unreadable
        );
        assert_eq!(
            resolve_service_endpoint("{not json", "10.0.0.5", true),
            ServiceEndpoint::Unreadable
        );
        // Well-formed JSON with no spec is also unreadable.
        assert_eq!(
            resolve_service_endpoint(r#"{"metadata":{"name":"ui"}}"#, "10.0.0.5", true),
            ServiceEndpoint::Unreadable
        );
    }

    #[test]
    fn port_forward_hint_reproduces_the_status_hint_text() {
        // The exact hint `cluster status` prints today for a ClusterIP service
        // (PR#34 visual-parity guard): two spaces before `then`.
        assert_eq!(
            port_forward_hint("curie", "curie-ui", 18080, 80, "/?api=1"),
            "kubectl -n curie port-forward svc/curie-ui 18080:80  then http://localhost:18080/?api=1"
        );
        // The 0-port fallback surfaces local 8080 while still forwarding to 0.
        assert_eq!(
            port_forward_hint("curie", "curie-langfuse-web", 8080, 0, ""),
            "kubectl -n curie port-forward svc/curie-langfuse-web 8080:0  then http://localhost:8080"
        );
    }

    #[test]
    fn service_url_json_uses_the_privileged_port_forward_hint() {
        let clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        let endpoint = resolve_service_endpoint(clusterip, "10.0.0.5", true);
        let ServiceEndpoint::PortForwardHint { local, port } = endpoint else {
            panic!("ClusterIP services must yield a port-forward hint");
        };
        let service_url = ServiceUrl {
            label: "UI".to_string(),
            name: "curie-ui".to_string(),
            namespace: "curie".to_string(),
            api: true,
            kind: ServiceUrlKind::PortForward { local, port },
        };

        assert_eq!(
            service_url.to_json(),
            serde_json::json!({
                "name": "UI",
                "url": null,
                "note": "kubectl -n curie port-forward svc/curie-ui 18080:80  then http://localhost:18080/?api=1",
            })
        );
    }

    #[test]
    fn service_url_port_forward_hint_preserves_a_human_identity_target() {
        let service_url = ServiceUrl {
            label: "UI".to_string(),
            name: "curie-ui".to_string(),
            namespace: "curie".to_string(),
            api: true,
            kind: ServiceUrlKind::PortForward {
                local: 18080,
                port: 80,
            },
        };
        let ui = crate::ui::Ui::resolve(
            crate::ui::ColorFlag::Never,
            false,
            false,
            false,
            &crate::ui::UiEnv {
                no_color: false,
                clicolor_zero: false,
                clicolor_force: false,
                term_dumb: false,
                ci: false,
                stderr_tty: true,
                stdout_tty: true,
                utf8: true,
                truecolor: false,
            },
        );
        let target = ui.url("http://localhost:18080/?api=1");
        assert_eq!(target, "http://localhost:18080/?api=1");
        assert_eq!(
            service_url.port_forward_hint(18080, 80, &target),
            "kubectl -n curie port-forward svc/curie-ui 18080:80  then http://localhost:18080/?api=1"
        );
    }

    #[test]
    fn api_base_endpoint_maps_ui_service_to_a_non_browsable_api_endpoint() {
        // A NodePort ui service resolves to the UI /api proxy URL (#360) and is
        // NEVER browsable -- it is an agent target, not a webapp.
        let ep = api_base_endpoint(&common(), &fullname(), Some(NODEPORT_SVC), Some("10.0.0.5"));
        assert_eq!(ep.name, "Curie API");
        assert_eq!(ep.url.as_deref(), Some("http://10.0.0.5:31234/api"));
        assert_eq!(ep.note, None);
        assert!(!ep.browsable);
    }

    #[test]
    fn api_base_endpoint_degrades_to_a_note_when_the_ui_service_is_unreadable() {
        // Unreadable ui service: degrade to a note endpoint rather than failing
        // the whole command, and never smuggle the message into `url`.
        let ep = api_base_endpoint(&common(), &fullname(), Some(""), Some("10.0.0.5"));
        assert_eq!(ep.name, "Curie API");
        assert_eq!(ep.url, None, "a degraded endpoint must not carry a url");
        assert!(
            ep.note.is_some(),
            "a degraded endpoint must explain itself in `note`"
        );
        assert!(!ep.browsable);
    }

    #[test]
    fn api_base_endpoint_reports_a_missing_ui_service_as_not_found() {
        // Not "could not read" (the deploy-path wording): the true condition is
        // not-found, and this row must agree with the `ui` row from
        // `service_surface`.
        let ep = api_base_endpoint(&common(), &fullname(), None, Some("10.0.0.5"));
        assert_eq!(ep.url, None);
        assert_eq!(ep.note.as_deref(), Some("service curie-ui not found"));
        assert!(!ep.browsable);
        assert_no_api_url_hint(&ep);
    }

    #[test]
    fn api_base_endpoint_hints_a_port_forward_for_a_clusterip_ui_service() {
        // `--no-expose` is a supported install mode, so this is a real path,
        // not an error: hand back an actionable port-forward for the API
        // service instead of deploy's dead --api-url hint.
        let ep = api_base_endpoint(
            &common(),
            &fullname(),
            Some(CLUSTERIP_SVC),
            Some("10.0.0.5"),
        );
        assert_eq!(ep.url, None);
        assert_eq!(
            ep.note.as_deref(),
            Some(
                "kubectl -n curie port-forward svc/curie-api 8000:8000  then http://localhost:8000"
            )
        );
        assert!(!ep.browsable);
        assert_no_api_url_hint(&ep);
    }

    #[test]
    fn api_base_endpoint_notes_stay_plain_for_the_json_payload() {
        // `Ui::emit_json` documents the payload as machine-consumed: no ANSI.
        for ep in [
            api_base_endpoint(&common(), &fullname(), None, Some("10.0.0.5")),
            api_base_endpoint(
                &common(),
                &fullname(),
                Some(CLUSTERIP_SVC),
                Some("10.0.0.5"),
            ),
            api_base_endpoint(&common(), &fullname(), Some(""), Some("10.0.0.5")),
            api_base_endpoint(&common(), &fullname(), Some(NODEPORT_SVC), None),
        ] {
            let note = ep.note.as_deref().unwrap_or("");
            assert!(
                !note.contains('\u{1b}'),
                "note must carry no ANSI: {note:?}"
            );
            assert_no_api_url_hint(&ep);
        }
    }

    #[test]
    fn api_base_endpoint_hints_a_port_forward_when_the_host_is_unresolvable() {
        let ep = api_base_endpoint(&common(), &fullname(), Some(NODEPORT_SVC), None);
        assert_eq!(ep.url, None);
        assert_eq!(
            ep.note.as_deref(),
            Some(
                "kubectl -n curie port-forward svc/curie-api 8000:8000  then http://localhost:8000"
            )
        );
        assert!(!ep.browsable);
        assert_no_api_url_hint(&ep);
    }

    // ---- service_surface: the whole cluster-tier ServiceEndpoint -> Endpoint
    // mapper. It decides url-vs-note and owns `browsable`, the --open gate.

    #[test]
    fn service_surface_maps_a_nodeport_service_to_a_browsable_url_row() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some(NODEPORT_SVC),
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.name, "Curie Console");
        assert_eq!(ep.url.as_deref(), Some("http://10.0.0.5:31234/?api=1"));
        assert_eq!(ep.note, None);
        assert!(ep.browsable, "a resolved NodePort URL is the --open target");
    }

    #[test]
    fn service_surface_degrades_when_the_service_is_not_found() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            None,
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.url, None, "a degraded row must never carry a url");
        assert_eq!(ep.note.as_deref(), Some("service curie-ui not found"));
        assert!(!ep.browsable, "--open must not fire on a degraded row");
    }

    #[test]
    fn service_surface_degrades_when_the_node_host_is_unresolvable() {
        // Pins the deliberate divergence from `cluster status`: this twin does
        // NOT inherit `discover_host()`'s `localhost` fallback, so an
        // unresolvable host is an explicit note, never a fabricated URL.
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some(NODEPORT_SVC),
            None,
            true,
        );
        assert_eq!(ep.url, None, "must not fabricate a localhost URL");
        assert_eq!(
            ep.note.as_deref(),
            Some("could not determine a node host to reach service curie-ui")
        );
        assert!(!ep.browsable);
    }

    #[test]
    fn service_surface_degrades_an_unassigned_nodeport_to_a_note() {
        let unassigned = r#"{"spec":{"type":"NodePort","ports":[{"port":80}]}}"#;
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some(unassigned),
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.url, None);
        assert_eq!(
            ep.note.as_deref(),
            Some("service curie-ui is NodePort but exposes no nodePort yet")
        );
        assert!(!ep.browsable);
    }

    #[test]
    fn service_surface_maps_a_clusterip_service_to_a_plain_port_forward_note() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "langfuse-web",
            "Langfuse UI",
            Some(CLUSTERIP_SVC),
            Some("10.0.0.5"),
            false,
        );
        assert_eq!(ep.url, None);
        let note = ep.note.as_deref().expect("a port-forward hint");
        assert_eq!(
            note,
            "kubectl -n curie port-forward svc/curie-langfuse-web 3000:3000  then http://localhost:3000"
        );
        // Serialized into the --json payload, which is machine-consumed.
        assert!(
            !note.contains('\u{1b}'),
            "note must carry no ANSI: {note:?}"
        );
        assert!(!ep.browsable, "a port-forward row is not a browser target");
    }

    #[test]
    fn service_surface_degrades_an_unreadable_service_to_a_note() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some("{not json"),
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.url, None);
        assert_eq!(ep.note.as_deref(), Some("could not read service curie-ui"));
        assert!(!ep.browsable);
    }

    #[test]
    fn observability_dry_run_plan_lists_the_read_only_lookups() {
        let lines: Vec<String> = observability_commands(&common(), &fullname())
            .iter()
            .map(|c| c.display())
            .collect();
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert!(
            lines.iter().any(|l| l.contains("get svc curie-ui")),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("get svc curie-langfuse-web")),
            "{lines:?}"
        );
    }
}

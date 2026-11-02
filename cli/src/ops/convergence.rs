//! Read-only convergence against Helm's exact installed target manifest.
//! Shared by up/apply and status; this does not perform transactional recovery.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;

use super::command::command_io_error;
use super::{plain, CommonOpts, OpsCommand, UpInvocation};

/// Which convergence property an observed issue actually disproves.
///
/// `cluster upgrade` reports named sub-flags (`images`, `generations`, ...) and
/// must not derive them by substring-matching reason text, which would flip a
/// gate to green on a reworded message. Every issue therefore carries the facet
/// the observing code genuinely inspected.
///
/// `Replicas` and `Unavailable` are separate: a workload can be mid-rollout
/// (`updated != desired`) while `unavailableReplicas` is genuinely `0`, so
/// deriving one from the other reports a value the observer never saw.
///
/// `Rollout` is the honest home for issues that no named property covers: a
/// stalled rollout, a crash-looping or terminating container, a revision that
/// is still pending or settled in a non-deployed status, an empty target
/// manifest, a StatefulSet mid-revision, or a Helm revision that moved under
/// the observation. It fails `exact` without lying about which specific
/// property was disproved. A same-revision status transition within one sample
/// is not a Rollout verdict: that sample is discarded and tags every facet,
/// because none of its facts belong to one consistent release state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Facet {
    Image,
    Generation,
    Replicas,
    Unavailable,
    Manifest,
    Hook,
    Drain,
    Rollout,
}

/// The Drain facet covers the worker drain, attest, hold, and post-upgrade
/// release Jobs (`charts/curie/templates/worker-upgrade-drain.yaml`). A refusal
/// means accepted work had not settled, no successful drain is recorded, or
/// this revision's pause authority could not be renewed. `cluster upgrade`
/// reports these failures separately from every other hook. Schema-migrate
/// stays [`Facet::Hook`].
fn hook_facet(name: &str) -> Facet {
    if name.ends_with("-upgrade-drain")
        || name.ends_with("-upgrade-drain-release")
        || name.ends_with("-upgrade-drain-attest")
        || name.ends_with("-upgrade-drain-hold")
    {
        Facet::Drain
    } else {
        Facet::Hook
    }
}

#[derive(Default)]
pub(super) struct Observation {
    pub issues: Vec<String>,
    /// The same issues, each tagged with the property it disproves. Written
    /// only by [`Observation::issue`] alongside `issues`, so the two cannot
    /// drift apart.
    pub facets: Vec<(Facet, String)>,
    pub terminal: bool,
    /// The Helm revision the sample's first status read reported. The waiters
    /// pin their target to the first sample's value and never adopt another.
    pub revision: Option<u64>,
}

impl Observation {
    fn issue(&mut self, facet: Facet, resource: &str, reason: impl std::fmt::Display) {
        let text = format!("{resource}: {reason}");
        self.facets.push((facet, text.clone()));
        self.issues.push(text);
    }

    /// One issue that disproves several facets at once, recorded as a single
    /// operator-visible line so the facet tagging cannot duplicate output.
    fn issue_multi(&mut self, facets: &[Facet], resource: &str, reason: impl std::fmt::Display) {
        let text = format!("{resource}: {reason}");
        for facet in facets {
            self.facets.push((*facet, text.clone()));
        }
        self.issues.push(text);
    }

    /// True when nothing observed disproves `facet`.
    pub fn holds(&self, facet: Facet) -> bool {
        !self.facets.iter().any(|(observed, _)| *observed == facet)
    }
}

/// Only standardized reason codes are printed. Kubernetes message fields can
/// contain arbitrary application/configuration data and never enter output.
pub(super) fn reason(value: &str) -> &'static str {
    match value {
        "ProgressDeadlineExceeded" => "ProgressDeadlineExceeded",
        "BackoffLimitExceeded" => "BackoffLimitExceeded",
        "DeadlineExceeded" => "DeadlineExceeded",
        "CrashLoopBackOff" => "CrashLoopBackOff",
        "ImagePullBackOff" => "ImagePullBackOff",
        "ErrImagePull" => "ErrImagePull",
        "InvalidImageName" => "InvalidImageName",
        "CreateContainerConfigError" => "CreateContainerConfigError",
        "CreateContainerError" => "CreateContainerError",
        "RunContainerError" => "RunContainerError",
        "OOMKilled" => "OOMKilled",
        "Error" => "Error",
        "Evicted" => "Evicted",
        "ContainerCannotRun" => "ContainerCannotRun",
        "Unschedulable" => "Unschedulable",
        "Completed" => "Completed",
        _ => "UnrecognizedReason",
    }
}

fn array<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn text<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value.pointer(pointer).and_then(Value::as_str).unwrap_or("")
}

fn count(value: &Value, pointer: &str) -> u64 {
    value.pointer(pointer).and_then(Value::as_u64).unwrap_or(0)
}

fn namespace<'a>(value: &'a Value, opts: &'a CommonOpts) -> &'a str {
    value
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .unwrap_or(&opts.namespace)
}

/// Helm v3 status JSON marshals `pkg/time.Time` as RFC3339Nano; Kubernetes
/// `creationTimestamp` is RFC3339 truncated to whole seconds. Zero-year
/// sentinel values are missing.
/// https://github.com/helm/helm/blob/v3.20.0/pkg/time/time.go
/// https://github.com/kubernetes/apimachinery/blob/v0.34.1/pkg/apis/meta/v1/time.go
fn rfc3339(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .filter(|stamp| stamp.year() > 1)
}

/// Issue #2858: a leftover hook Job from an earlier release of the same name
/// is older than the current revision. Helm hook Jobs typically have no owner
/// UID pointing at the current revision Secret, so creation time is the
/// observable. Compare whole seconds so a Helm RFC3339Nano `last_deployed`
/// cannot mark a same-second current Job stale. Unparseable timestamps fail
/// closed and still count. Helm `last_run.phase=Failed` stays unfiltered:
/// that is this revision's own hook record, even when a leftover Job is older.
fn hook_job_predates_revision(job: &Value, last_deployed: Option<OffsetDateTime>) -> bool {
    match (
        rfc3339(text(job, "/metadata/creationTimestamp")),
        last_deployed,
    ) {
        (Some(created), Some(deployed)) => created.unix_timestamp() < deployed.unix_timestamp(),
        _ => false,
    }
}

#[derive(Clone, Copy)]
struct Deadlines {
    observation: Instant,
    convergence: Option<Instant>,
}

impl Deadlines {
    fn outer(self) -> (Instant, &'static str) {
        match self.convergence {
            Some(deadline) if deadline <= self.observation => (
                deadline,
                "post-Helm convergence timed out after 300 seconds",
            ),
            _ => (
                self.observation,
                "convergence observation timed out after 30 seconds",
            ),
        }
    }
}

async fn capture(command: OpsCommand, description: &str, deadlines: Deadlines) -> Result<String> {
    let inner = Instant::now() + Duration::from_secs(10);
    let (outer, message) = deadlines.outer();
    let (deadline, message) = if outer <= inner {
        (outer, message.to_owned())
    } else {
        (inner, format!("{description} timed out after 10 seconds"))
    };
    let (command, _secret_files) = command
        .materialize_secret_files()
        .with_context(|| format!("could not {description}"))?;
    let mut child = command
        .tokio_command()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| command_io_error(&command.program, error))
        .with_context(|| format!("could not {description}"))?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let result = tokio::time::timeout_at(deadline, async {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let (status, _, _) = tokio::try_join!(
            child.wait(),
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err),
        )?;
        Ok::<_, std::io::Error>((status.success(), String::from_utf8_lossy(&out).into_owned()))
    })
    .await;
    if !matches!(&result, Ok(Ok(_))) {
        let stopped = child.start_kill();
        let reaped = child.wait().await;
        stopped
            .and(reaped.map(|_| ()))
            .map_err(|error| command_io_error(&command.program, error))
            .with_context(|| {
                format!("could not stop and reap child while attempting to {description}")
            })?;
    }
    let (ok, out) = result
        .context(message)?
        .map_err(|error| command_io_error(&command.program, error))
        .with_context(|| format!("could not {description}"))?;
    if !ok {
        bail!("could not {description}; inspect Helm/Kubernetes access and retry");
    }
    Ok(out)
}

fn helm_status_command(opts: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("status"),
            plain(&opts.release),
            plain("-n"),
            plain(&opts.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

fn manifest_command(opts: &CommonOpts, revision: &str) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("manifest"),
            plain(&opts.release),
            plain("-n"),
            plain(&opts.namespace),
            plain("--revision"),
            plain(revision),
        ],
    )
}

fn workloads_command(namespace: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("deployments,statefulsets,daemonsets,pods,jobs"),
            plain("-n"),
            plain(namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

fn node_images_command(node: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("node"),
            plain(node),
            plain("-o"),
            plain("json"),
        ],
    )
}

pub(super) const DRY_RUN_NOTE: &str = "# Convergence plan only: <revision> and each <manifest-namespace> are resolved at runtime from Helm status and the target workload/hook manifests; placeholders are not executable arguments. <pod-node> is read only if a tagged image reports a different alias, requiring get-node access for that serving Pod's node. Recheck Helm revision after each observation.";

/// Preview the same pure command builders the observer executes. Dynamic
/// arguments are explicitly identified by DRY_RUN_NOTE in the caller's plan.
pub(super) fn dry_run_commands(opts: &CommonOpts) -> Vec<OpsCommand> {
    vec![
        helm_status_command(opts),
        manifest_command(opts, "<revision>"),
        workloads_command("<manifest-namespace>"),
        node_images_command("<pod-node>"),
        helm_status_command(opts),
    ]
}

async fn helm_status(opts: &CommonOpts, deadlines: Deadlines) -> Result<Value> {
    let raw = capture(
        helm_status_command(opts),
        "read Helm release state",
        deadlines,
    )
    .await?;
    serde_json::from_str(&raw).context("Helm release state is malformed")
}

/// The revision and status of one Helm status read. Only the numeric revision
/// and a recognized Helm status name are ever printed; Helm descriptions and
/// notes can carry arbitrary data and never enter output.
fn helm_identity(status: &Value) -> Result<(u64, &'static str)> {
    let revision = status
        .get("version")
        .and_then(Value::as_u64)
        .filter(|version| *version > 0)
        .context("Helm release has no verifiable revision")?;
    let state = match text(status, "/info/status") {
        "deployed" => "deployed",
        "pending-install" => "pending-install",
        "pending-upgrade" => "pending-upgrade",
        "pending-rollback" => "pending-rollback",
        "failed" => "failed",
        "superseded" => "superseded",
        "uninstalled" => "uninstalled",
        "uninstalling" => "uninstalling",
        "unknown" => "unknown",
        _ => "unrecognized",
    };
    Ok((revision, state))
}

fn normalize_image(image: &str) -> String {
    let mut image = image.to_owned();
    let first = image.split('/').next().unwrap_or("");
    if !image.contains('/') {
        image = format!("docker.io/library/{image}");
    } else if !first.contains('.') && !first.contains(':') && first != "localhost" {
        image = format!("docker.io/{image}");
    }
    if !image.contains('@') && !image.rsplit('/').next().unwrap_or("").contains(':') {
        image.push_str(":latest");
    }
    image
}

/// Registry-normalized repository, with tag and digest stripped the same way
/// manifest_identity strips a tag from the repository side of repo@digest.
fn repository(image: &str) -> String {
    let image = normalize_image(image);
    let repository = if let Some((repo, _)) = image.split_once('@') {
        repo
    } else {
        image.as_str()
    };
    let last_segment = repository.rsplit('/').next().unwrap_or(repository);
    if let Some((_, tag)) = last_segment.split_once(':') {
        repository[..repository.len() - tag.len() - 1].to_owned()
    } else {
        repository.to_owned()
    }
}

/// A digest-qualified runtime reference can omit the requested tag. Compare
/// the canonical repository and the exact manifest digest, never a config ID
/// or an assumed equivalence between different registry digests.
fn manifest_identity(image: &str) -> Option<String> {
    let image = normalize_image(image);
    let (repository, digest) = image.split_once('@')?;
    if digest.is_empty() || digest.contains('@') {
        return None;
    }
    let last_segment = repository.rsplit('/').next()?;
    let repository = if let Some((_, tag)) = last_segment.split_once(':') {
        &repository[..repository.len() - tag.len() - 1]
    } else {
        repository
    };
    Some(format!("{repository}@{digest}"))
}

fn needs_node_identity(expected: &str, status: &Value) -> bool {
    manifest_identity(expected).is_none()
        && normalize_image(text(status, "/image")) != normalize_image(expected)
}

/// Identities named on Node.status.images entries whose names list contains
/// `reference`. None means the name is absent from the inventory.
/// https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/#NodeStatus
fn matching_inventory_identities(node: &Value, reference: &str) -> Option<BTreeSet<String>> {
    let wanted = normalize_image(reference);
    let mut found = false;
    let mut identities = BTreeSet::new();
    for image in array(node, "/status/images") {
        let names: Vec<&str> = array(image, "/names")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        if !names
            .iter()
            .copied()
            .any(|name| normalize_image(name) == wanted)
        {
            continue;
        }
        found = true;
        for name in names {
            if let Some(identity) = manifest_identity(name) {
                identities.insert(identity);
            }
        }
    }
    found.then_some(identities)
}

/// Unique repo@digest bound to `reference` in this node's image inventory.
/// containerd/kubelet may list each tag of one loaded image as its own
/// Node.status.images entry; only an exact digest identity combines them.
/// A missing name, a digest-less entry, or two distinct digests fail closed.
fn unique_inventory_identity(node: &Value, reference: &str) -> Option<String> {
    let mut identities = matching_inventory_identities(node, reference)?.into_iter();
    let identity = identities.next()?;
    identities.next().is_none().then_some(identity)
}

/// Unique repo@digest on this node whose repository equals `repo`.
fn unique_repository_identity(node: &Value, repo: &str) -> Option<String> {
    let mut identities = BTreeSet::new();
    for image in array(node, "/status/images") {
        for name in array(image, "/names").iter().filter_map(Value::as_str) {
            let Some(identity) = manifest_identity(name) else {
                continue;
            };
            if repository(&identity) == repo {
                identities.insert(identity);
            }
        }
    }
    let mut identities = identities.into_iter();
    let identity = identities.next()?;
    identities.next().is_none().then_some(identity)
}

/// Kubelet's nodeStatusMaxImages default. A Node.status.images list this long
/// may have dropped any smaller image, so an absent name is unknown there.
/// https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/
const NODE_STATUS_MAX_IMAGES: usize = 50;

fn inventory_truncated(node: &Value) -> bool {
    array(node, "/status/images").len() >= NODE_STATUS_MAX_IMAGES
}

/// Bind a tagged request or kubelet alias to the running imageID digest.
/// Prefer a unique digest on matching inventory entries; if those entries are
/// digest-less, a unique same-repository digest on the node that equals the
/// running identity. A missing name is unbound unless the inventory is at the
/// kubelet cap, where the running digest in the same repository binds it (#3352).
/// Do not infer across repositories.
/// https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/#NodeStatus
fn resolve_reference_identity(node: &Value, reference: &str, running: &str) -> Option<String> {
    match matching_inventory_identities(node, reference) {
        Some(identities) if identities.len() > 1 => None,
        Some(identities) if identities.is_empty() => {
            let identity = unique_repository_identity(node, &repository(reference))?;
            (identity == running).then_some(identity)
        }
        Some(_) => unique_inventory_identity(node, reference),
        None => (inventory_truncated(node) && repository(running) == repository(reference))
            .then(|| running.to_owned()),
    }
}

fn observed_image_matches(expected: &str, status: &Value, node: Option<&Value>) -> bool {
    let image_id = text(status, "/imageID");
    if let Some(expected) = manifest_identity(expected) {
        // Containerd can report a config SHA in status.image for a digest-pinned
        // request. Only the qualified imageID supplies manifest identity then.
        let image_id = image_id
            .strip_prefix("docker-pullable://")
            .unwrap_or(image_id);
        return manifest_identity(image_id).as_ref() == Some(&expected);
    }
    // A tag is a requested reference, not immutable content authority.
    if !needs_node_identity(expected, status) {
        return !image_id.is_empty();
    }
    let Some(node) = node else { return false };
    let Some(identity) = manifest_identity(
        image_id
            .strip_prefix("docker-pullable://")
            .unwrap_or(image_id),
    ) else {
        return false;
    };
    let Some(expected_identity) = resolve_reference_identity(node, expected, &identity) else {
        return false;
    };
    let Some(alias_identity) = resolve_reference_identity(node, text(status, "/image"), &identity)
    else {
        return false;
    };
    expected_identity == identity && alias_identity == identity
}

fn selected(workload: &Value, pod: &Value) -> bool {
    let selector = &workload["spec"]["selector"];
    let labels = &pod["metadata"]["labels"];
    let matches = selector.get("matchLabels").and_then(Value::as_object);
    let expressions = array(selector, "/matchExpressions");
    if matches.is_none_or(|labels| labels.is_empty()) && expressions.is_empty() {
        return false;
    }
    matches.is_none_or(|expected| {
        expected
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
    }) && expressions.iter().all(|expression| {
        let Some(key) = expression.get("key").and_then(Value::as_str) else {
            return false;
        };
        let actual = labels.get(key);
        let values = array(expression, "/values");
        match text(expression, "/operator") {
            "In" => actual.is_some_and(|actual| values.contains(actual)),
            "NotIn" => actual.is_none_or(|actual| !values.contains(actual)),
            "Exists" => actual.is_some(),
            "DoesNotExist" => actual.is_none(),
            _ => false,
        }
    })
}

/// A terminal pod (`Succeeded` or `Failed`) that is not being deleted. A
/// kubelet-initiated termination (node-pressure eviction, graceful node
/// shutdown) leaves that pod object behind and its ReplicaSet never deletes it,
/// while the controller's own counts already cover active pods only (#4496).
/// A terminal pod with a deletionTimestamp is still terminating and still counts.
fn left_terminal(pod: &Value) -> bool {
    matches!(text(pod, "/status/phase"), "Succeeded" | "Failed")
        && pod
            .pointer("/metadata/deletionTimestamp")
            .is_none_or(Value::is_null)
}

/// A Pod the workload selects and that counts as a replica: terminal pods
/// dropped by `left_terminal` are excluded. Node discovery and `workload()`
/// share this one predicate so a pod that is not counted never triggers a Node
/// read (an evicted pod's Node may be gone or unreadable).
fn counted_pod(expected: &Value, item: &Value) -> bool {
    text(item, "/kind") == "Pod" && selected(expected, item) && !left_terminal(item)
}

fn pod_reasons(pod: &Value, result: &mut Observation) {
    let name = text(pod, "/metadata/name");
    let pod_reason = text(pod, "/status/reason");
    if !pod_reason.is_empty() && pod_reason != "Completed" {
        result.issue(Facet::Rollout, name, reason(pod_reason));
    }
    for (field, init) in [
        ("/status/initContainerStatuses", true),
        ("/status/containerStatuses", false),
    ] {
        for container in array(pod, field) {
            let id = format!(
                "{name}/{}{}",
                if init { "init:" } else { "" },
                text(container, "/name")
            );
            if let Some(waiting) = container.pointer("/state/waiting") {
                result.issue(Facet::Rollout, &id, reason(text(waiting, "/reason")));
            }
            if let Some(terminated) = container.pointer("/state/terminated") {
                if !init || count(terminated, "/exitCode") != 0 {
                    result.issue(
                        Facet::Rollout,
                        &id,
                        format!(
                            "{} (exit {})",
                            reason(text(terminated, "/reason")),
                            count(terminated, "/exitCode")
                        ),
                    );
                }
            }
        }
    }
}

fn compare_containers(
    expected: &Value,
    actual: &Value,
    pod: bool,
    nodes: &BTreeMap<String, Value>,
    result: &mut Observation,
) {
    let id = text(actual, "/metadata/name");
    for (field, status_field, init) in [
        ("containers", "containerStatuses", false),
        ("initContainers", "initContainerStatuses", true),
    ] {
        let wanted = array(expected, &format!("/spec/template/spec/{field}"));
        let actual_path = if pod {
            format!("/spec/{field}")
        } else {
            format!("/spec/template/spec/{field}")
        };
        let actual_containers = array(actual, &actual_path);
        for container in wanted {
            let name = text(container, "/name");
            let image = text(container, "/image");
            let actual_container = actual_containers
                .iter()
                .find(|item| text(item, "/name") == name);
            if image.is_empty()
                || actual_container.is_none_or(|item| {
                    normalize_image(text(item, "/image")) != normalize_image(image)
                })
            {
                result.issue(
                    Facet::Image,
                    id,
                    format!("container {name} does not match target image"),
                );
            }
            if !pod {
                continue;
            }
            let status = array(actual, &format!("/status/{status_field}"))
                .iter()
                .find(|item| text(item, "/name") == name);
            let image_matches = status.is_some_and(|status| {
                observed_image_matches(image, status, nodes.get(text(actual, "/spec/nodeName")))
            });
            let valid = image_matches
                && status.is_some_and(|status| {
                    if init
                        && container.get("restartPolicy").and_then(Value::as_str) != Some("Always")
                    {
                        status
                            .pointer("/state/terminated/exitCode")
                            .and_then(Value::as_u64)
                            == Some(0)
                    } else {
                        status.get("ready").and_then(Value::as_bool) == Some(true)
                            && status.pointer("/state/running").is_some()
                    }
                });
            if !valid {
                // A container whose image already matches the target but is not
                // ready is a stalled rollout (CrashLoopBackOff, failed probe),
                // not an image mismatch. Tagging it `Image` would report
                // `images: false` about an image that is in fact correct.
                result.issue(
                    if image_matches {
                        Facet::Rollout
                    } else {
                        Facet::Image
                    },
                    id,
                    if !image_matches && status.is_some_and(|status| needs_node_identity(image, status)) {
                        format!("container {name} tagged alias has no unique same-node image binding; inspect the serving Node image inventory or select a digest-pinned target image")
                    } else {
                        format!("container {name} has no ready target image observation")
                    },
                );
            }
        }
        // Admission-injected containers have no Helm image authority. Require
        // their health without comparing them to an invented chart image.
        if pod && !init {
            for container in actual_containers {
                if wanted
                    .iter()
                    .any(|wanted| text(wanted, "/name") == text(container, "/name"))
                {
                    continue;
                }
                let status = array(actual, &format!("/status/{status_field}"))
                    .iter()
                    .find(|status| text(status, "/name") == text(container, "/name"));
                if status.is_none_or(|status| {
                    status.get("ready").and_then(Value::as_bool) != Some(true)
                        || status.pointer("/state/running").is_none()
                }) {
                    result.issue(
                        Facet::Rollout,
                        id,
                        "admission-injected container is not ready",
                    );
                }
            }
        }
    }
}

fn workload(
    expected: &Value,
    actual: &Value,
    items: &[Value],
    nodes: &BTreeMap<String, Value>,
    result: &mut Observation,
) {
    let id = text(expected, "/metadata/name");
    let kind = text(expected, "/kind");
    let generation = actual
        .pointer("/metadata/generation")
        .and_then(Value::as_u64);
    if generation.is_none()
        || generation
            != actual
                .pointer("/status/observedGeneration")
                .and_then(Value::as_u64)
    {
        result.issue(
            Facet::Generation,
            id,
            "target generation has not been observed",
        );
    }
    let desired = if kind == "DaemonSet" {
        count(actual, "/status/desiredNumberScheduled")
    } else {
        actual
            .pointer("/spec/replicas")
            .and_then(Value::as_u64)
            .unwrap_or(1)
    };
    let (updated, ready, total, unavailable) = if kind == "DaemonSet" {
        (
            count(actual, "/status/updatedNumberScheduled"),
            count(actual, "/status/numberReady"),
            count(actual, "/status/currentNumberScheduled"),
            count(actual, "/status/numberUnavailable"),
        )
    } else {
        (
            count(actual, "/status/updatedReplicas"),
            count(actual, "/status/readyReplicas"),
            count(actual, "/status/replicas"),
            count(actual, "/status/unavailableReplicas"),
        )
    };
    let counts_off = updated != desired || ready != desired || total != desired;
    if counts_off || unavailable != 0 {
        let mut facets = Vec::new();
        if counts_off {
            facets.push(Facet::Replicas);
        }
        if unavailable != 0 {
            facets.push(Facet::Unavailable);
        }
        result.issue_multi(&facets, id, format!("replicas desired={desired} updated={updated} ready={ready} total={total} unavailable={unavailable}"));
    }
    if kind == "StatefulSet"
        && desired > 0
        && (text(actual, "/status/currentRevision").is_empty()
            || text(actual, "/status/currentRevision") != text(actual, "/status/updateRevision"))
    {
        result.issue(
            Facet::Rollout,
            id,
            "StatefulSet current revision does not match target revision",
        );
    }
    for condition in array(actual, "/status/conditions") {
        if text(condition, "/type") == "Progressing"
            && text(condition, "/status") == "False"
            && text(condition, "/reason") == "ProgressDeadlineExceeded"
        {
            result.issue(Facet::Rollout, id, "ProgressDeadlineExceeded");
            result.terminal = true;
        }
    }
    if expected.pointer("/spec/selector") != actual.pointer("/spec/selector") {
        result.issue(Facet::Manifest, id, "workload selector differs from target");
    }
    compare_containers(expected, actual, false, nodes, result);
    let pods: Vec<_> = items
        .iter()
        .filter(|item| counted_pod(expected, item))
        .collect();
    if pods.len() as u64 != desired {
        result.issue(
            Facet::Replicas,
            id,
            format!(
                "selected pods={} desired={desired}; surplus or missing target replicas",
                pods.len()
            ),
        );
    }
    for pod in pods {
        if text(pod, "/status/phase") != "Running"
            || pod
                .pointer("/metadata/deletionTimestamp")
                .is_some_and(|value| !value.is_null())
        {
            result.issue(
                Facet::Rollout,
                text(pod, "/metadata/name"),
                "target pod is not steadily running",
            );
        }
        pod_reasons(pod, result);
        compare_containers(expected, pod, true, nodes, result);
    }
}

/// Every facet, for a sample in which nothing about the target was observed.
const UNOBSERVED: [Facet; 8] = [
    Facet::Image,
    Facet::Generation,
    Facet::Replicas,
    Facet::Unavailable,
    Facet::Manifest,
    Facet::Hook,
    Facet::Drain,
    Facet::Rollout,
];

/// One sample against the target revision. `expected` pins the revision a
/// waiter adopted from its first sample; a different latest revision is a
/// replacement, refused before its manifest is read. A pending status on the
/// target revision is retryable; every other non-deployed status is terminal.
async fn observe_inner(
    opts: &CommonOpts,
    pinned: Option<u64>,
    deadlines: Deadlines,
) -> Result<Observation> {
    let status = helm_status(opts, deadlines).await?;
    let (revision, state) = helm_identity(&status)?;
    let mut result = Observation {
        revision: Some(revision),
        ..Observation::default()
    };
    if let Some(expected) = pinned.filter(|expected| *expected != revision) {
        result.issue_multi(
            &UNOBSERVED,
            "Helm release",
            format!("target revision changed between convergence observations; expected revision={expected}, observed revision={revision} status={state}"),
        );
        result.terminal = true;
        return Ok(result);
    }
    let last_deployed = rfc3339(text(&status, "/info/last_deployed"));
    match state {
        "deployed" => {}
        "pending-install" | "pending-upgrade" | "pending-rollback" => result.issue(
            Facet::Rollout,
            "Helm release",
            format!("latest revision={revision} status={state} is not deployed yet"),
        ),
        _ => {
            result.issue(
                Facet::Rollout,
                "Helm release",
                format!("latest revision={revision} status={state} is not deployed"),
            );
            result.terminal = true;
        }
    }
    let manifest = capture(
        manifest_command(opts, &revision.to_string()),
        "read target release manifest",
        deadlines,
    )
    .await?;
    let mut expected = Vec::new();
    for document in serde_norway::Deserializer::from_str(&manifest) {
        let value = Value::deserialize(document).context("target release manifest is malformed")?;
        if matches!(
            text(&value, "/kind"),
            "Deployment" | "StatefulSet" | "DaemonSet"
        ) {
            expected.push(value);
        }
    }
    if expected.is_empty() {
        result.issue(
            Facet::Rollout,
            "Helm release",
            "target manifest contains no managed workloads",
        );
        result.terminal = true;
    }
    let mut namespaces: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for object in &expected {
        namespaces
            .entry(namespace(object, opts).to_owned())
            .or_default();
    }
    for hook in array(&status, "/hooks") {
        if !array(hook, "/events").iter().any(|event| {
            matches!(
                event.as_str(),
                Some("pre-install" | "post-install" | "pre-upgrade" | "post-upgrade")
            )
        }) {
            continue;
        }
        if text(hook, "/last_run/phase") == "Failed" {
            let name = text(hook, "/name");
            result.issue(hook_facet(name), name, "Helm hook failed");
            result.terminal = true;
        }
        if text(hook, "/kind") == "Job" {
            let hook_manifest: Value = serde_norway::from_str(text(hook, "/manifest"))
                .context("hook manifest is malformed")?;
            namespaces
                .entry(namespace(&hook_manifest, opts).to_owned())
                .or_default();
        }
    }
    for (namespace, items) in &mut namespaces {
        let raw = capture(
            workloads_command(namespace),
            "read target workloads and pods",
            deadlines,
        )
        .await?;
        let value: Value =
            serde_json::from_str(&raw).context("target workloads and pods are malformed")?;
        *items = value
            .get("items")
            .and_then(Value::as_array)
            .context("target workload response has no items")?
            .clone();
    }
    let mut needed_nodes = BTreeSet::new();
    for object in &expected {
        for pod in namespaces[namespace(object, opts)]
            .iter()
            .filter(|item| counted_pod(object, item))
        {
            for (field, statuses) in [
                ("containers", "containerStatuses"),
                ("initContainers", "initContainerStatuses"),
            ] {
                for container in array(object, &format!("/spec/template/spec/{field}")) {
                    if array(pod, &format!("/status/{statuses}"))
                        .iter()
                        .any(|status| {
                            text(status, "/name") == text(container, "/name")
                                && needs_node_identity(text(container, "/image"), status)
                        })
                    {
                        let node = text(pod, "/spec/nodeName");
                        if !node.is_empty() {
                            needed_nodes.insert(node.to_owned());
                        }
                    }
                }
            }
        }
    }
    let mut nodes = BTreeMap::new();
    for name in needed_nodes {
        let raw = capture(node_images_command(&name), "read serving Node image inventory", deadlines).await
            .map_err(|error| crate::exit::CliError::failure(format!(
                "tagged image alias requires get-node read access for the serving Pod; inspect that access or select a digest-pinned target image: {error}"
            )).with_fix("allow read access to the serving Node or select a digest-pinned target image, then rerun the same cluster command"))?;
        let node: Value =
            serde_json::from_str(&raw).context("serving Node image inventory is malformed")?;
        if text(&node, "/kind") != "Node" || text(&node, "/metadata/name") != name {
            bail!("image inventory did not identify the serving Pod's Node; retry with a verifiable node observation");
        }
        nodes.insert(name, node);
    }
    for object in &expected {
        let items = &namespaces[namespace(object, opts)];
        if let Some(actual) = items.iter().find(|item| {
            text(item, "/kind") == text(object, "/kind")
                && text(item, "/metadata/name") == text(object, "/metadata/name")
        }) {
            workload(object, actual, items, &nodes, &mut result);
        } else {
            result.issue(
                Facet::Manifest,
                text(object, "/metadata/name"),
                "target workload is absent",
            );
        }
    }
    for hook in array(&status, "/hooks") {
        if text(hook, "/kind") != "Job"
            || !array(hook, "/events").iter().any(|event| {
                matches!(
                    event.as_str(),
                    Some("pre-install" | "post-install" | "pre-upgrade" | "post-upgrade")
                )
            })
        {
            continue;
        }
        let hook_manifest: Value = serde_norway::from_str(text(hook, "/manifest"))
            .context("hook manifest is malformed")?;
        for job in namespaces[namespace(&hook_manifest, opts)]
            .iter()
            .filter(|item| {
                text(item, "/kind") == "Job"
                    && text(item, "/metadata/name") == text(hook, "/name")
                    && !hook_job_predates_revision(item, last_deployed)
            })
        {
            for condition in array(job, "/status/conditions") {
                if text(condition, "/type") == "Failed" && text(condition, "/status") == "True" {
                    let name = text(job, "/metadata/name");
                    result.issue(hook_facet(name), name, reason(text(condition, "/reason")));
                    result.terminal = true;
                }
            }
        }
    }
    let final_status = helm_status(opts, deadlines).await?;
    let (final_revision, final_state) = helm_identity(&final_status)?;
    if final_revision != revision {
        let expected = pinned.unwrap_or(revision);
        result.issue(
            Facet::Rollout,
            "Helm release",
            format!("revision changed during convergence observation; expected revision={expected}, before revision={revision} status={state}, after revision={final_revision} status={final_state}"),
        );
        result.terminal = true;
    } else if final_state != state {
        // The same revision moved status mid-sample, so the facts read in
        // between belong to no single release state. Discard them all and let
        // the waiter reread; this is a transition, not a replacement.
        let mut discarded = Observation {
            revision: Some(revision),
            ..Observation::default()
        };
        discarded.issue_multi(
            &UNOBSERVED,
            "Helm release",
            format!("status changed during convergence observation; before revision={revision} status={state}, after revision={final_revision} status={final_state}; sample discarded"),
        );
        return Ok(discarded);
    }
    Ok(result)
}

async fn observe_pinned(
    opts: &CommonOpts,
    expected: Option<u64>,
    convergence: Option<Instant>,
) -> Result<Observation> {
    let deadlines = Deadlines {
        observation: Instant::now() + Duration::from_secs(30),
        convergence,
    };
    let result = observe_inner(opts, expected, deadlines).await;
    let (deadline, message) = deadlines.outer();
    if Instant::now() >= deadline {
        return tokio::time::timeout_at(deadline, std::future::pending())
            .await
            .context(message);
    }
    result
}

/// One unpinned sample of whatever revision Helm currently reports.
pub(super) async fn observe(opts: &CommonOpts) -> Result<Observation> {
    observe_pinned(opts, None, None).await
}

pub(super) async fn installation_failure(
    opts: &CommonOpts,
    original: anyhow::Error,
    invocation: UpInvocation,
) -> anyhow::Error {
    if let Ok(observation) = observe(opts).await {
        if !observation.issues.is_empty() {
            return crate::exit::CliError::failure(format!(
                "Helm installation failed; observed rollout reasons: {}",
                observation.issues.join("; ")
            ))
            .with_fix(match invocation {
                UpInvocation::ClusterUp => "run `curie cluster status`, correct the failed hook or workload and retry",
                UpInvocation::Apply => "run `curie cluster status`, correct the failed hook or workload configuration in `curie.yaml`, and rerun `curie apply`",
            })
            .into();
        }
    }
    original
}

/// Wait up to 300 seconds, sampling every two seconds, until the target
/// release has exactly converged. A pending revision or a sample whose Helm
/// status moved while it was read is retried; a stable failure is refused.
///
/// The target revision is pinned to the one the first sample's first status
/// read observes, and any later sample naming another revision is refused. A
/// replacement that lands after the initiating Helm command returns but before
/// that first read is not detected here.
pub(super) async fn wait(opts: &CommonOpts, invocation: UpInvocation) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut last_issues: Vec<String> = Vec::new();
    let fix = match invocation {
        UpInvocation::ClusterUp => "run `curie cluster status` to inspect the failed rollout; correct the target configuration and rerun `curie cluster up`",
        UpInvocation::Apply => "run `curie cluster status` to inspect the failed rollout; correct the target configuration in `curie.yaml` and rerun `curie apply`",
    };
    let mut expected = None;
    loop {
        let observation = observe_pinned(opts, expected, Some(deadline)).await;
        let result = match observation {
            Ok(result) => result,
            Err(error) if last_issues.is_empty() => return Err(error),
            Err(error) => {
                return Err(crate::exit::CliError::failure(format!(
                    "{error}; last observed rollout reasons: {}",
                    last_issues.join("; ")
                ))
                .with_fix(fix)
                .into());
            }
        };
        // Pin the first sample's revision, even when that sample was
        // discarded, and never adopt a later one.
        expected = expected.or(result.revision);
        if result.issues.is_empty() {
            return Ok(());
        }
        if result.terminal || tokio::time::Instant::now() + Duration::from_secs(2) >= deadline {
            return Err(crate::exit::CliError::failure(format!(
                "target release has not converged: {}",
                result.issues.join("; ")
            ))
            .with_fix(fix)
            .into());
        }
        last_issues = result.issues;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Like [`wait`], but hands the caller the final [`Observation`] instead of an
/// error, so a caller that owes the operator a structured verdict (the
/// `cluster upgrade` Converge phase, which must still emit its convergence
/// payload, its failing phase and one fail-forward path) keeps the facts
/// instead of losing them to `?`.
///
/// Keeps its verdict separate from [`wait`], which renders the fix for the
/// command that started the installation.
///
/// A read that fails outright, or a rollout that never settles, comes back as a
/// terminal observation rather than an `Err`: an unreadable cluster has not
/// converged, and that is a verdict, not a crash. The target revision is
/// pinned exactly as in [`wait`].
pub(super) async fn wait_for_observation(opts: &CommonOpts) -> Observation {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut carried = Observation::default();
    let mut expected = None;
    loop {
        let observed = observe_pinned(opts, expected, Some(deadline)).await;
        let result = match observed {
            Ok(result) => result,
            Err(error) => {
                // The observation could not be made, so NO named property was
                // determined. `holds` is closed-world, so leaving the facets
                // untagged would report images/generations/replicas/... as
                // observed-true off a read that never happened. Tag every facet
                // with the one failure, and keep any facet an earlier pass had
                // already genuinely disproved.
                let mut result = carried;
                let fix = crate::exit::classify(&error).1;
                let detail = match fix {
                    Some(fix) => format!("{error:#}; {fix}"),
                    None => format!("{error:#}"),
                };
                result.issue_multi(&UNOBSERVED, "Helm release", detail);
                result.terminal = true;
                return result;
            }
        };
        expected = expected.or(result.revision);
        if result.issues.is_empty()
            || result.terminal
            || tokio::time::Instant::now() + Duration::from_secs(2) >= deadline
        {
            return result;
        }
        carried = result;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hook_facet_reports_a_failed_hold_as_a_drain_failure() {
        let name = "acme-curie-upgrade-drain-hold";
        let mut observation = Observation::default();
        observation.issue(hook_facet(name), name, "Helm hook failed");
        assert!(!observation.holds(Facet::Drain));
        assert!(observation.holds(Facet::Hook));
        assert_eq!(observation.issues, [format!("{name}: Helm hook failed")]);

        for suffix in [
            "upgrade-drain",
            "upgrade-drain-attest",
            "upgrade-drain-release",
        ] {
            assert_eq!(hook_facet(&format!("acme-curie-{suffix}")), Facet::Drain);
        }
        assert_eq!(hook_facet("acme-curie-schema-migrate"), Facet::Hook);
        assert_eq!(
            hook_facet("acme-curie-upgrade-drain-hold-other"),
            Facet::Hook
        );
    }

    #[test]
    fn rfc3339_parses_helm_and_kubernetes_stamps() {
        assert!(rfc3339("2026-09-21T12:00:00Z").is_some());
        assert!(rfc3339("2026-09-21T12:00:00.123456789Z").is_some());
        assert!(rfc3339("2026-09-21T12:00:00+00:00").is_some());
        assert!(rfc3339("0001-01-01T00:00:00Z").is_none());
        assert!(rfc3339("").is_none());
    }

    fn deployed(stamp: &str) -> Option<OffsetDateTime> {
        rfc3339(stamp)
    }

    #[test]
    fn older_job_predates_the_current_revision() {
        let stale = json!({"metadata": {"creationTimestamp": "2026-09-21T11:53:16Z"}});
        assert!(hook_job_predates_revision(
            &stale,
            deployed("2026-09-21T12:00:00Z")
        ));
        let current = json!({"metadata": {"creationTimestamp": "2026-09-21T12:00:01Z"}});
        assert!(!hook_job_predates_revision(
            &current,
            deployed("2026-09-21T12:00:00Z")
        ));
        assert!(!hook_job_predates_revision(&stale, None));
        assert!(!hook_job_predates_revision(
            &json!({}),
            deployed("2026-09-21T12:00:00Z")
        ));
    }

    #[test]
    fn fractional_last_deployed_does_not_mark_a_same_second_job_stale() {
        let same_second = json!({"metadata": {"creationTimestamp": "2026-09-21T12:00:00Z"}});
        assert!(!hook_job_predates_revision(
            &same_second,
            deployed("2026-09-21T12:00:00.217175126Z")
        ));
        let previous_second = json!({"metadata": {"creationTimestamp": "2026-09-21T11:59:59Z"}});
        assert!(hook_job_predates_revision(
            &previous_second,
            deployed("2026-09-21T12:00:00.217175126Z")
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Output, Stdio};
    use std::thread::{self, JoinHandle};
    use std::time::Instant;

    const CHILD: &str = "CURIE_CONVERGENCE_TEST_CHILD";
    const IMAGE: &str = "docker.io/library/busybox:1.36.1";
    const NODE_IMAGE: &str = "kindest/node:v1.31.0@sha256:25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93";

    const WORKLOAD: &str = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}-workload
spec:
  replicas: 1
  selector:
    matchLabels: {app: {{ .Release.Name }}}
  template:
    metadata:
      labels: {app: {{ .Release.Name }}}
    spec:
      terminationGracePeriodSeconds: 0
      containers:
        - name: probe
          image: {{ .Values.image }}
          imagePullPolicy: Never
          command: [sh, -c]
          args: ["{{ if .Values.ready }}touch /tmp/ready; {{ end }}exec sleep 900"]
          readinessProbe:
            exec:
              command: [sh, -c, "test -f /tmp/ready"]
            periodSeconds: 1
            failureThreshold: 1
"#;

    const HOOK: &str = r#"apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .Release.Name }}-{{ .Values.hookName }}
  annotations:
    helm.sh/hook: post-install,pre-upgrade,pre-rollback
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 900
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: gate
          image: {{ .Values.image }}
          imagePullPolicy: Never
          command: [sh, -c]
          args: ["{{ if .Values.gated }}until test -f /tmp/release; do sleep 0.1; done; {{ end }}exit {{ .Values.exitCode }}"]
"#;

    // Helm status exposes version, info.status and hooks[].last_run.phase:
    // https://helm.sh/docs/helm/helm_status/ . The fixture records real tuples
    // (1, pending-install) and (1, deployed); no response bytes are replaced.
    const FORWARDER: &str = r#"#!/usr/bin/env python3
import os
import socket
import subprocess
import sys
import time

tool = os.path.basename(sys.argv[0])
args = sys.argv[1:]
started = time.monotonic()
event = ''
if tool == 'helm' and args[:1] == ['status']:
    event = 'status'
if tool == 'kubectl' and args[:2] == ['get', 'deployments,statefulsets,daemonsets,pods,jobs']:
    event = 'workloads'

def barrier(phase):
    if event:
        host, port = os.environ['CURIE_CONVERGENCE_BARRIER'].split(':')
        with socket.create_connection((host, int(port)), timeout=7) as sock:
            remaining = max(0.001, 8 - (time.monotonic() - started))
            sock.settimeout(min(7, remaining))
            sock.sendall((event + ':' + phase + ':' + str(remaining) + '\n').encode())
            if sock.recv(1) != b'+':
                raise RuntimeError('convergence barrier closed')

barrier('before')
result = subprocess.run([os.environ['CURIE_CONVERGENCE_REAL_' + tool.upper()], *args],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
barrier('after')
sys.stdout.buffer.write(result.stdout)
sys.stderr.buffer.write(result.stderr)
sys.exit(result.returncode)
"#;

    struct Process {
        child: Child,
        stdout: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
        stderr: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
        output: Option<Output>,
    }

    fn drain(mut source: impl Read + Send + 'static) -> JoinHandle<std::io::Result<Vec<u8>>> {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            source.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
    }

    impl Process {
        fn spawn(command: Command) -> Result<Self> {
            Self::spawn_with_input(command, None)
        }

        fn spawn_with_input(mut command: Command, input: Option<&[u8]>) -> Result<Self> {
            command
                .process_group(0)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if input.is_some() {
                command.stdin(Stdio::piped());
            }
            let mut child = command
                .spawn()
                .context("could not start required fixture executable")?;
            if let Some(input) = input {
                // Dropping the handle after the write closes stdin so the child sees EOF.
                let mut stdin = child.stdin.take().context("missing process stdin")?;
                stdin
                    .write_all(input)
                    .context("could not write fixture process stdin")?;
            }
            let stdout = Some(drain(
                child.stdout.take().context("missing process stdout")?,
            ));
            let stderr = Some(drain(
                child.stderr.take().context("missing process stderr")?,
            ));
            Ok(Self {
                child,
                stdout,
                stderr,
                output: None,
            })
        }

        fn kill(&mut self) {
            // Every fixture client owns its process group, including forwarders.
            unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };
        }

        fn wait(&mut self, budget: Duration) -> Result<&Output> {
            if self.output.is_none() {
                let deadline = Instant::now() + budget;
                let mut timed_out = false;
                let status = loop {
                    if let Some(status) = self.child.try_wait()? {
                        break status;
                    }
                    if Instant::now() >= deadline {
                        timed_out = true;
                        self.kill();
                        break self.child.wait()?;
                    }
                    thread::sleep(Duration::from_millis(10));
                };
                let stdout = self
                    .stdout
                    .take()
                    .unwrap()
                    .join()
                    .map_err(|_| anyhow::anyhow!("fixture stdout reader panicked"))??;
                let stderr = self
                    .stderr
                    .take()
                    .unwrap()
                    .join()
                    .map_err(|_| anyhow::anyhow!("fixture stderr reader panicked"))??;
                let tail = timed_out.then(|| stderr_tail(&stderr));
                self.output = Some(Output {
                    status,
                    stdout,
                    stderr,
                });
                if let Some(tail) = tail {
                    bail!("required fixture process exceeded its wall-clock budget: {tail}");
                }
            }
            Ok(self.output.as_ref().unwrap())
        }

        fn stop(&mut self) -> Result<()> {
            if self.output.is_none() {
                self.kill();
                self.wait(Duration::from_secs(10))?;
            }
            Ok(())
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            drop(self.stop());
        }
    }

    fn stderr_tail(stderr: &[u8]) -> String {
        let text = String::from_utf8_lossy(stderr);
        let lines: Vec<&str> = text.trim().lines().collect();
        let tail = lines[lines.len().saturating_sub(20)..].join("\n");
        let mut start = tail.len().saturating_sub(4000);
        while !tail.is_char_boundary(start) {
            start += 1;
        }
        tail[start..].trim().to_owned()
    }

    fn run(command: Command, budget: Duration) -> Result<String> {
        run_with_input(command, None, budget)
    }

    fn run_with_input(command: Command, input: Option<&[u8]>, budget: Duration) -> Result<String> {
        let program = Path::new(command.get_program())
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
        let mut process = Process::spawn_with_input(command, input)?;
        let output = process.wait(budget)?;
        if !output.status.success() {
            bail!(
                "required fixture command {program} failed with {}: {}",
                output.status,
                stderr_tail(&output.stderr)
            );
        }
        String::from_utf8(output.stdout.clone()).context("fixture output is not UTF-8")
    }

    fn executable(name: &str) -> Result<PathBuf> {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|dir| dir.join(name))
            .find(|path| {
                path.metadata().is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            })
            .with_context(|| format!("mandatory {name} executable is unavailable"))
    }

    #[derive(Clone, Copy)]
    enum Operation {
        Install,
        Upgrade,
        Rollback,
    }

    impl Operation {
        fn pending(self) -> &'static str {
            match self {
                Self::Install => "pending-install",
                Self::Upgrade => "pending-upgrade",
                Self::Rollback => "pending-rollback",
            }
        }

        fn revision(self) -> u64 {
            match self {
                Self::Install => 1,
                Self::Upgrade => 2,
                Self::Rollback => 3,
            }
        }
    }

    struct Target {
        release: String,
        hook: String,
        revision: u64,
        operation: usize,
    }

    struct Fixture {
        root: tempfile::TempDir,
        programs: BTreeMap<String, PathBuf>,
        original_path: std::ffi::OsString,
        cluster: String,
        context: String,
        image: String,
        image_built: bool,
        release_prefix: String,
        releases: Vec<String>,
        processes: Vec<Process>,
        cluster_requested: bool,
        cleaned: bool,
        started: Instant,
    }

    impl Fixture {
        fn new() -> Result<Self> {
            let mut programs = BTreeMap::new();
            for name in ["docker", "kind", "kubectl", "helm", "python3"] {
                programs.insert(name.to_owned(), executable(name)?);
            }
            let storage = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .context("CLI has no checkout root")?
                .join(".projects/convergence");
            std::fs::create_dir_all(&storage)?;
            let root = tempfile::tempdir_in(storage)?;
            let cluster = format!(
                "acme-convergence-{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            );
            let context = format!("kind-{cluster}");
            let image = format!("acme-convergence-busybox:{cluster}");
            Ok(Self {
                root,
                programs,
                original_path: std::env::var_os("PATH").context("PATH is required")?,
                release_prefix: cluster.clone(),
                cluster,
                context,
                image,
                image_built: false,
                releases: Vec::new(),
                processes: Vec::new(),
                cluster_requested: false,
                cleaned: false,
                started: Instant::now(),
            })
        }

        fn command(&self, program: &str) -> Command {
            let mut command = Command::new(&self.programs[program]);
            command
                .env("KUBECONFIG", self.root.path().join("kubeconfig"))
                .env("HELM_KUBECONTEXT", &self.context)
                .env("HELM_DRIVER", "secret");
            if program == "helm" {
                command.args(["--kube-context", &self.context]);
            } else if program == "kubectl" {
                command.args(["--context", &self.context, "--request-timeout=5s"]);
            }
            command
        }

        fn invoke(&self, program: &str, args: &[&str], budget: Duration) -> Result<String> {
            let mut command = self.command(program);
            command.args(args);
            run(command, budget).with_context(|| format!("{program} fixture operation failed"))
        }

        fn containers(&self, cluster: &str) -> Result<BTreeSet<String>> {
            let mut command = self.command("docker");
            command.args([
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                &format!("label=io.x-k8s.kind.cluster={cluster}"),
            ]);
            Ok(run(command, Duration::from_secs(15))?
                .lines()
                .map(str::to_owned)
                .collect())
        }

        fn start(&mut self) -> Result<()> {
            self.invoke(
                "docker",
                &["info", "--format", "{{.ServerVersion}}"],
                Duration::from_secs(20),
            )?;
            self.invoke("python3", &["--version"], Duration::from_secs(5))?;
            self.invoke("helm", &["version", "--short"], Duration::from_secs(10))?;
            let clusters = self.invoke("kind", &["get", "clusters"], Duration::from_secs(10))?;
            anyhow::ensure!(
                !clusters.lines().any(|name| name == self.cluster),
                "fixture cluster name is occupied"
            );
            self.cluster_requested = true;
            let kubeconfig = self.root.path().join("kubeconfig");
            self.invoke(
                "kind",
                &[
                    "create",
                    "cluster",
                    "--name",
                    &self.cluster,
                    "--image",
                    NODE_IMAGE,
                    "--kubeconfig",
                    kubeconfig.to_str().context("non-UTF-8 kubeconfig path")?,
                    "--wait",
                    "120s",
                ],
                Duration::from_secs(180),
            )?;
            println!("convergence cluster created: {}", self.cluster);
            let contexts: Value = serde_json::from_str(&self.invoke(
                "kubectl",
                &["config", "view", "-o", "json"],
                Duration::from_secs(10),
            )?)?;
            anyhow::ensure!(
                array(&contexts, "/contexts").len() == 1
                    && text(&contexts, "/current-context") == self.context,
                "fixture kubeconfig must contain only its owned context"
            );
            self.invoke("docker", &["pull", IMAGE], Duration::from_secs(120))?;
            // The pulled image can be a multi-platform index whose other platforms are
            // absent under the containerd image store, which kind cannot load. An owned
            // single-platform image built from it loads on every engine.
            self.image_built = true;
            let mut build = self.command("docker");
            build.args(["build", "-q", "-t", &self.image, "-"]);
            run_with_input(
                build,
                Some(format!("FROM {IMAGE}\n").as_bytes()),
                Duration::from_secs(60),
            )
            .context("docker fixture operation failed")?;
            self.invoke(
                "kind",
                &["load", "docker-image", &self.image, "--name", &self.cluster],
                Duration::from_secs(90),
            )?;
            self.configure_case()
        }

        fn for_case(shared: &Self) -> Result<Self> {
            let mut fixture = Self::new()?;
            fixture.cluster.clone_from(&shared.cluster);
            fixture.context.clone_from(&shared.context);
            fixture.image.clone_from(&shared.image);
            std::fs::copy(
                shared.root.path().join("kubeconfig"),
                fixture.root.path().join("kubeconfig"),
            )?;
            fixture.configure_case()?;
            Ok(fixture)
        }

        fn configure_case(&self) -> Result<()> {
            let chart = self.root.path().join("chart");
            std::fs::create_dir_all(chart.join("templates"))?;
            std::fs::write(
                chart.join("Chart.yaml"),
                "apiVersion: v2\nname: acme-convergence\nversion: 0.1.0\n",
            )?;
            std::fs::write(
                chart.join("values.yaml"),
                format!(
                    "ready: true\ngated: false\nhookName: schema-migrate\nexitCode: 0\nimage: {}\n",
                    self.image
                ),
            )?;
            std::fs::write(chart.join("templates/workload.yaml"), WORKLOAD)?;
            std::fs::write(chart.join("templates/hook.yaml"), HOOK)?;
            std::fs::create_dir(self.root.path().join("bin"))?;
            for name in ["helm", "kubectl"] {
                let path = self.root.path().join("bin").join(name);
                std::fs::write(&path, FORWARDER)?;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
            }
            Ok(())
        }

        fn release(&mut self) -> Result<String> {
            let release = format!("{}-{}", self.release_prefix, self.releases.len());
            self.releases.push(release.clone());
            self.invoke(
                "kubectl",
                &["create", "namespace", &release],
                Duration::from_secs(10),
            )?;
            Ok(release)
        }

        fn helm_change(
            &mut self,
            verb: &str,
            release: &str,
            gated: bool,
            ready: bool,
            hook: &str,
            exit: u8,
        ) -> Result<usize> {
            let mut command = self.command("helm");
            command
                .args([verb, release])
                .arg(self.root.path().join("chart"));
            command.args(["-n", release, "--timeout", "600s", "--set"]);
            command.arg(format!(
                "gated={gated},ready={ready},hookName={hook},exitCode={exit}"
            ));
            self.spawn(command)
        }

        fn spawn(&mut self, command: Command) -> Result<usize> {
            let index = self.processes.len();
            self.processes.push(Process::spawn(command)?);
            Ok(index)
        }

        fn finish_helm(&mut self, process: usize, success: bool, budget: Duration) -> Result<()> {
            let output = self.processes[process].wait(budget)?;
            anyhow::ensure!(
                output.status.success() == success,
                "Helm operation returned unexpected {}",
                output.status
            );
            Ok(())
        }

        fn status(&self, release: &str) -> Result<(u64, String)> {
            let status: Value = serde_json::from_str(&self.invoke(
                "helm",
                &["status", release, "-n", release, "-o", "json"],
                Duration::from_secs(8),
            )?)?;
            let revision = status["version"]
                .as_u64()
                .context("real Helm status has no numeric revision")?;
            let state = text(&status, "/info/status");
            anyhow::ensure!(
                matches!(
                    state,
                    "pending-install"
                        | "pending-upgrade"
                        | "pending-rollback"
                        | "deployed"
                        | "failed"
                        | "uninstalled"
                ),
                "unexpected real Helm fixture status"
            );
            println!(
                "UTC={} elapsed={:.3}s release={} revision={} status={}",
                OffsetDateTime::now_utc().format(&Rfc3339)?,
                self.started.elapsed().as_secs_f64(),
                release,
                revision,
                state
            );
            Ok((revision, state.to_owned()))
        }

        fn pending(&self, target: &Target, state: &str) -> Result<()> {
            let deadline = Instant::now() + Duration::from_secs(45);
            loop {
                if let Ok((revision, observed)) = self.status(&target.release) {
                    if revision == target.revision && observed == state {
                        break;
                    }
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "Helm did not enter the required pending state"
                );
                thread::sleep(Duration::from_millis(100));
            }
            let deadline = Instant::now() + Duration::from_secs(45);
            loop {
                let pods = self.invoke(
                    "kubectl",
                    &[
                        "get",
                        "pods",
                        "-n",
                        &target.release,
                        "-l",
                        &format!("job-name={}", target.hook),
                        "-o",
                        "name",
                    ],
                    Duration::from_secs(8),
                )?;
                if !pods.trim().is_empty() {
                    break;
                }
                anyhow::ensure!(Instant::now() < deadline, "real hook Pod was not created");
                thread::sleep(Duration::from_millis(100));
            }
            self.invoke(
                "kubectl",
                &[
                    "wait",
                    "pod",
                    "-n",
                    &target.release,
                    "-l",
                    &format!("job-name={}", target.hook),
                    "--for=condition=Ready",
                    "--timeout=60s",
                ],
                Duration::from_secs(70),
            )?;
            Ok(())
        }

        fn workload_ready(&self, release: &str, budget: Duration) -> Result<()> {
            self.invoke(
                "kubectl",
                &[
                    "rollout",
                    "status",
                    &format!("deployment/{release}-workload"),
                    "-n",
                    release,
                    "--timeout=60s",
                ],
                budget,
            )?;
            Ok(())
        }

        fn open_hook(&self, target: &Target, budget: Duration) -> Result<()> {
            self.invoke(
                "kubectl",
                &[
                    "exec",
                    "-n",
                    &target.release,
                    &format!("job/{}", target.hook),
                    "--",
                    "touch",
                    "/tmp/release",
                ],
                budget,
            )?;
            Ok(())
        }

        fn open_workload(&self, release: &str, budget: Duration) -> Result<()> {
            let deadline = Instant::now() + budget;
            self.invoke(
                "kubectl",
                &[
                    "exec",
                    "-n",
                    release,
                    &format!("deployment/{release}-workload"),
                    "--",
                    "touch",
                    "/tmp/ready",
                ],
                budget,
            )?;
            self.workload_ready(release, deadline.saturating_duration_since(Instant::now()))
        }

        fn prepare_pending(
            &mut self,
            operation: Operation,
            ready: bool,
            hook: &str,
            exit: u8,
        ) -> Result<Target> {
            let release = self.release()?;
            if !matches!(operation, Operation::Install) {
                let gated = matches!(operation, Operation::Rollback);
                let seed = self.helm_change("install", &release, gated, true, hook, 0)?;
                if gated {
                    let initial = Target {
                        release: release.clone(),
                        hook: format!("{release}-{hook}"),
                        revision: 1,
                        operation: seed,
                    };
                    self.pending(&initial, "pending-install")?;
                    self.open_hook(&initial, Duration::from_secs(10))?;
                }
                self.finish_helm(seed, true, Duration::from_secs(60))?;
                self.workload_ready(&release, Duration::from_secs(65))?;
                if matches!(operation, Operation::Rollback) {
                    let seed = self.helm_change("upgrade", &release, false, true, hook, 0)?;
                    self.finish_helm(seed, true, Duration::from_secs(60))?;
                }
            }
            let process = match operation {
                Operation::Install => {
                    self.helm_change("install", &release, true, ready, hook, exit)?
                }
                Operation::Upgrade => {
                    self.helm_change("upgrade", &release, true, ready, hook, exit)?
                }
                Operation::Rollback => {
                    let mut command = self.command("helm");
                    command.args([
                        "rollback",
                        &release,
                        "1",
                        "-n",
                        &release,
                        "--timeout",
                        "600s",
                    ]);
                    self.spawn(command)?
                }
            };
            let target = Target {
                release,
                hook: format!("{}-{hook}", self.releases.last().unwrap()),
                revision: operation.revision(),
                operation: process,
            };
            self.pending(&target, operation.pending())?;
            if ready {
                self.workload_ready(&target.release, Duration::from_secs(65))?;
            }
            Ok(target)
        }

        fn child(
            &mut self,
            test: &str,
            release: &str,
            contract: &str,
            expectation: &str,
            revision: u64,
            gate: Option<&Barrier>,
        ) -> Result<usize> {
            let full_name = "ops::convergence::tests::shared_cluster_convergence_cases";
            let mut command = Command::new(std::env::current_exe()?);
            command
                .args(["--exact", full_name, "--nocapture", "--test-threads=1"])
                .env(CHILD, test)
                .env("CURIE_CONVERGENCE_RELEASE", release)
                .env("CURIE_CONVERGENCE_CONTRACT", contract)
                .env("CURIE_CONVERGENCE_EXPECT", expectation)
                .env("CURIE_CONVERGENCE_REVISION", revision.to_string())
                .env("KUBECONFIG", self.root.path().join("kubeconfig"))
                .env("HELM_KUBECONTEXT", &self.context)
                .env("HELM_DRIVER", "secret")
                .env_remove("CURIE_CONVERGENCE_BARRIER")
                .env("PATH", &self.original_path);
            if let Some(gate) = gate {
                let mut paths = vec![self.root.path().join("bin")];
                paths.extend(std::env::split_paths(&self.original_path));
                command.env("PATH", std::env::join_paths(paths)?).env(
                    "CURIE_CONVERGENCE_BARRIER",
                    gate.listener.local_addr()?.to_string(),
                );
                for program in ["helm", "kubectl"] {
                    command.env(
                        format!("CURIE_CONVERGENCE_REAL_{}", program.to_uppercase()),
                        &self.programs[program],
                    );
                }
            }
            self.spawn(command)
        }

        fn finish_child(
            &mut self,
            process: usize,
            mut gate: Option<&mut Barrier>,
            budget: Duration,
        ) -> Result<()> {
            let deadline = Instant::now() + budget;
            while self.processes[process].child.try_wait()?.is_none() && Instant::now() < deadline {
                if let Some(gate) = gate.as_mut() {
                    if let Some((_, stream)) = gate.event()? {
                        acknowledge(stream)?;
                    }
                }
                thread::sleep(Duration::from_millis(5));
            }
            let output =
                self.processes[process].wait(deadline.saturating_duration_since(Instant::now()))?;
            print!("{}", String::from_utf8_lossy(&output.stdout));
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
            anyhow::ensure!(output.status.success(), "private convergence child failed");
            Ok(())
        }

        fn history(&self, target: &Target) -> Result<()> {
            let history: Value = serde_json::from_str(&self.invoke(
                "helm",
                &[
                    "history",
                    &target.release,
                    "-n",
                    &target.release,
                    "-o",
                    "json",
                ],
                Duration::from_secs(10),
            )?)?;
            let entries = history.as_array().context("Helm history is not an array")?;
            anyhow::ensure!(
                entries.len() == target.revision as usize,
                "waiter must not trigger another Helm operation"
            );
            let last = entries.last().context("Helm history is empty")?;
            anyhow::ensure!(
                last["revision"].as_u64() == Some(target.revision) && last["status"] == "deployed",
                "target revision must be deployed"
            );
            anyhow::ensure!(
                self.status(&target.release)? == (target.revision, "deployed".to_owned()),
                "unexpected final Helm identity"
            );
            Ok(())
        }

        fn diagnostics(&self, case: &str) {
            for release in &self.releases {
                for (program, args) in [
                    (
                        "kubectl",
                        vec!["get", "all,events", "-n", release, "-o", "yaml"],
                    ),
                    (
                        "helm",
                        vec!["status", release, "-n", release, "--show-desc"],
                    ),
                ] {
                    match self.invoke(program, &args, Duration::from_secs(10)) {
                        Ok(output) => eprintln!("case={case} {program} diagnostics: {output}"),
                        Err(error) => eprintln!("case={case} diagnostics failed: {error:#}"),
                    }
                }
            }
        }

        fn cleanup(&mut self) -> Result<()> {
            if self.cleaned {
                return Ok(());
            }
            let mut errors = Vec::new();
            for process in &mut self.processes {
                if process.stop().is_err() {
                    errors.push("could not stop and join a fixture process");
                }
            }
            for release in &self.releases {
                if self
                    .invoke(
                        "helm",
                        &[
                            "uninstall",
                            release,
                            "-n",
                            release,
                            "--ignore-not-found",
                            "--no-hooks",
                            "--timeout",
                            "30s",
                        ],
                        Duration::from_secs(40),
                    )
                    .is_err()
                {
                    errors.push("owned release uninstall failed");
                }
                if self
                    .invoke(
                        "kubectl",
                        &[
                            "delete",
                            "namespace",
                            release,
                            "--ignore-not-found",
                            "--wait=true",
                            "--timeout=60s",
                        ],
                        Duration::from_secs(70),
                    )
                    .is_err()
                {
                    errors.push("owned namespace deletion failed");
                }
                match self.invoke(
                    "kubectl",
                    &[
                        "get",
                        "namespace",
                        release,
                        "--ignore-not-found",
                        "-o",
                        "name",
                    ],
                    Duration::from_secs(10),
                ) {
                    Ok(names) if names.trim().is_empty() => {}
                    _ => errors.push("owned namespace deletion was not verified"),
                }
            }
            if self.cluster_requested {
                if self
                    .invoke(
                        "kind",
                        &["delete", "cluster", "--name", &self.cluster],
                        Duration::from_secs(90),
                    )
                    .is_err()
                {
                    errors.push("owned kind cluster deletion failed");
                }
                match self.invoke("kind", &["get", "clusters"], Duration::from_secs(15)) {
                    Ok(names) if !names.lines().any(|name| name == self.cluster) => {}
                    _ => errors.push("owned kind cluster deletion was not verified"),
                }
                match self.containers(&self.cluster) {
                    Ok(remaining) => {
                        if !remaining.is_empty() {
                            errors.push("owned Docker node containers remain");
                        }
                    }
                    Err(_) => errors.push("Docker cleanup could not be verified"),
                }
            }
            if self.image_built {
                // A failed removal is tolerated only when the tag is already absent.
                drop(self.invoke(
                    "docker",
                    &["image", "rm", &self.image],
                    Duration::from_secs(30),
                ));
                match self.invoke(
                    "docker",
                    &["image", "ls", "-q", &self.image],
                    Duration::from_secs(15),
                ) {
                    Ok(ids) if ids.trim().is_empty() => {}
                    _ => errors.push("owned fixture image removal was not verified"),
                }
            }
            self.cleaned = errors.is_empty();
            anyhow::ensure!(
                errors.is_empty(),
                "fixture cleanup failed: {}",
                errors.join("; ")
            );
            println!("fixture cleanup verified");
            Ok(())
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if !self.cleaned && self.cleanup().is_err() {
                eprintln!("fixture cleanup guard could not verify teardown");
            }
        }
    }

    struct Barrier {
        listener: TcpListener,
        counts: BTreeMap<String, usize>,
    }

    struct Permit {
        stream: Option<TcpStream>,
        deadline: Instant,
    }

    fn acknowledge(mut stream: TcpStream) -> Result<()> {
        stream.write_all(b"+")?;
        Ok(())
    }

    impl Permit {
        fn remaining(&self) -> Duration {
            self.deadline.saturating_duration_since(Instant::now())
        }

        fn resume(mut self) -> Result<()> {
            anyhow::ensure!(
                Instant::now() < self.deadline,
                "fixture barrier exceeded 6.5 seconds; preserve the ten-second capture budget"
            );
            acknowledge(self.stream.take().unwrap())
        }
    }

    impl Drop for Permit {
        fn drop(&mut self) {
            if let Some(stream) = self.stream.take() {
                drop(acknowledge(stream));
            }
        }
    }

    impl Barrier {
        fn new() -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            Ok(Self {
                listener,
                counts: BTreeMap::new(),
            })
        }

        fn event(&mut self) -> Result<Option<(String, TcpStream)>> {
            let (stream, _) = match self.listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            // macOS inherits the listener's nonblocking mode on accept. The
            // event protocol waits for one bounded line after connecting.
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut event = String::new();
            BufReader::new(&stream).read_line(&mut event)?;
            Ok(Some((event.trim().to_owned(), stream)))
        }

        fn pause(&mut self, kind: &str, ordinal: usize, phase: &str) -> Result<Permit> {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some((event, stream)) = self.event()? {
                    let mut fields = event.split(':');
                    let observed_kind = fields.next().context("missing barrier kind")?;
                    let observed_phase = fields.next().context("missing barrier phase")?;
                    let remaining: f64 =
                        fields.next().context("missing capture budget")?.parse()?;
                    let count = self.counts.entry(observed_kind.to_owned()).or_default();
                    if observed_phase == "before" {
                        *count += 1;
                    }
                    if observed_kind == kind && *count == ordinal && observed_phase == phase {
                        return Ok(Permit {
                            stream: Some(stream),
                            deadline: Instant::now() + Duration::from_secs_f64(remaining.min(6.5)),
                        });
                    }
                    acknowledge(stream)?;
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "observer did not reach the required real-executable barrier"
                );
                thread::sleep(Duration::from_millis(5));
            }
        }
    }

    #[test]
    fn barrier_reads_an_event_sent_after_the_connection_is_accepted() -> Result<()> {
        let mut barrier = Barrier::new()?;
        let address = barrier.listener.local_addr()?;
        let (connected, ready) = std::sync::mpsc::channel();
        let sender = thread::spawn(move || -> std::io::Result<()> {
            let mut stream = TcpStream::connect(address)?;
            connected.send(()).unwrap();
            thread::sleep(Duration::from_millis(100));
            stream.write_all(b"status:before:9.0\n")
        });
        ready.recv_timeout(Duration::from_secs(2))?;
        let event = barrier.event();
        sender.join().unwrap()?;
        let (event, _) = event?.context("connected peer was not accepted")?;
        assert_eq!(event, "status:before:9.0");
        Ok(())
    }

    fn healthy(observation: &Observation) {
        assert!(
            observation.issues.is_empty(),
            "unexpected issues: {}",
            observation.issues.join("; ")
        );
        assert!(!observation.terminal);
        for facet in UNOBSERVED {
            assert!(
                observation.holds(facet),
                "healthy sample must establish {facet:?}"
            );
        }
    }

    fn failed_facets(observation: &Observation, failed: &[Facet]) {
        for facet in UNOBSERVED {
            assert_eq!(
                observation.holds(facet),
                !failed.contains(&facet),
                "unexpected {facet:?} verdict: {}",
                observation.issues.join("; ")
            );
        }
    }

    fn child_scenario() -> Result<()> {
        let release = std::env::var("CURIE_CONVERGENCE_RELEASE")?;
        let opts = CommonOpts {
            namespace: release.clone(),
            release,
            dry_run: false,
        };
        let contract = std::env::var("CURIE_CONVERGENCE_CONTRACT")?;
        let expectation = std::env::var("CURIE_CONVERGENCE_EXPECT")?;
        let revision: u64 = std::env::var("CURIE_CONVERGENCE_REVISION")?.parse()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            if expectation == "discard" {
                let observed = observe(&opts).await?;
                assert!(!observed.terminal, "a same-revision status transition must retry");
                assert_eq!(observed.issues.len(), 1, "discard every inconsistent first-sample fact");
                let issue = &observed.issues[0];
                assert!(issue.contains("status changed during convergence observation"));
                assert!(issue.contains(&format!("before revision={revision} status=pending-install")));
                assert!(issue.contains(&format!("after revision={revision} status=deployed")));
                assert!(issue.contains("sample discarded"));
                assert!(!issue.contains("revision changed"));
                for facet in UNOBSERVED { assert!(!observed.holds(facet), "discarded sample cannot establish {facet:?}"); }
                println!("discarded observation: {issue}");
                healthy(&observe(&opts).await?);
            }
            if expectation.starts_with("terminal-") {
                let observed = observe(&opts).await?;
                assert!(observed.terminal);
                assert!(!observed.holds(Facet::Rollout));
                match expectation.as_str() {
                    "terminal-hook" | "terminal-drain" => {
                        let (failed, other) = if expectation == "terminal-drain" { (Facet::Drain, Facet::Hook) } else { (Facet::Hook, Facet::Drain) };
                        assert!(!observed.holds(failed));
                        assert!(observed.holds(other));
                        failed_facets(&observed, &[failed, Facet::Rollout]);
                        assert!(observed.issues.iter().any(|issue| issue.contains("Helm hook failed")));
                    }
                    "terminal-uninstalled" => {
                        assert!(observed.issues.iter().any(|issue| issue.contains("status=uninstalled")));
                        failed_facets(&observed, &[Facet::Manifest, Facet::Rollout]);
                    }
                    _ => bail!("unknown terminal test expectation"),
                }
            }
            if expectation == "unreadable" {
                assert!(observe(&opts).await.is_err(), "absent Helm storage must fail closed");
            }
            if expectation == "budget" {
                let observed = observe(&opts).await?;
                assert!(!observed.terminal, "stable pending must remain retryable");
                assert!(observed.issues.iter().any(|issue| issue.contains("pending-")));
                failed_facets(&observed, &[Facet::Rollout]);
            }
            let started = Instant::now();
            let (success, issues, terminal) = match contract.as_str() {
                "wait" => match wait(&opts, UpInvocation::ClusterUp).await {
                    Ok(()) => (true, String::new(), false),
                    Err(error) => (false, format!("{error:#}"), true),
                },
                "structured" => {
                    let observed = wait_for_observation(&opts).await;
                    let success = observed.issues.is_empty();
                    if success { healthy(&observed); } else { assert!(!observed.holds(Facet::Rollout)); }
                    if expectation == "terminal-hook" { failed_facets(&observed, &[Facet::Hook, Facet::Rollout]); }
                    if expectation == "terminal-drain" { failed_facets(&observed, &[Facet::Drain, Facet::Rollout]); }
                    if expectation == "terminal-uninstalled" { failed_facets(&observed, &[Facet::Manifest, Facet::Rollout]); }
                    if expectation == "unreadable" { failed_facets(&observed, &UNOBSERVED); }
                    (success, observed.issues.join("; "), observed.terminal)
                }
                _ => bail!("unknown private waiter contract"),
            };
            let elapsed = started.elapsed();
            println!("contract={contract} elapsed={:.3}s success={success} terminal={terminal} issues={issues}", elapsed.as_secs_f64());
            match expectation.as_str() {
                "success" | "discard" | "discard-wait" => assert!(success, "pending revision must converge without a second Helm operation: {issues}"),
                "budget" => {
                    assert!(!success, "closed gate must not converge");
                    assert!((295.0..=310.0).contains(&elapsed.as_secs_f64()), "pending must use the original 300-second budget, elapsed={elapsed:?}: {issues}");
                    assert!(issues.contains("pending-") && !issues.contains("revision changed"));
                }
                "within" | "between" | "discard-then-replace" => {
                    assert!(!success && terminal, "replacement revision must be terminal");
                    assert!(elapsed < Duration::from_secs(20));
                    assert!(issues.contains(&format!("expected revision={revision}")));
                    if expectation == "within" {
                        assert!(issues.contains("revision changed during convergence observation"));
                        assert!(issues.contains(&format!("before revision={revision} status=deployed")));
                        assert!(issues.contains(&format!("after revision={} status=deployed", revision + 1)));
                    } else {
                        assert!(issues.contains("target revision changed between convergence observations"));
                        assert!(issues.contains(&format!("observed revision={} status=deployed", revision + 1)));
                    }
                }
                "terminal-hook" | "terminal-drain" | "terminal-uninstalled" | "unreadable" => {
                    assert!(!success && terminal);
                    assert!(elapsed < Duration::from_secs(15), "stable failure must refuse promptly");
                }
                _ => bail!("unknown convergence test expectation"),
            }
            Ok(())
        })
    }

    fn fixture_test(
        name: &str,
        shared: &Fixture,
        scenario: impl FnOnce(&mut Fixture) -> Result<()>,
    ) -> Result<()> {
        let mut fixture = Fixture::for_case(shared)?;
        println!("case={name} started cluster={}", fixture.cluster);
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scenario(&mut fixture)))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("case {name} panicked")));
        if outcome.is_err() {
            fixture.diagnostics(name);
        }
        let cleanup = fixture.cleanup();
        println!(
            "case={name} completed elapsed={:.3}s",
            fixture.started.elapsed().as_secs_f64()
        );
        match (outcome, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error.context(name.to_owned())),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("case {name}: teardown also failed: {cleanup:#}")))
            }
        }
    }

    #[test]
    fn shared_cluster_convergence_cases() -> Result<()> {
        if std::env::var_os(CHILD).is_some() {
            return child_scenario();
        }
        let _environment = crate::PROCESS_ENV_LOCK.blocking_lock();
        if executable("docker").is_err() && std::env::var_os("CI_REQUIRE_DOCKER_TESTS").is_none() {
            eprintln!("skipping convergence cases: docker executable is unavailable");
            return Ok(());
        }
        let mut shared = Fixture::new()?;
        let outcome = shared.start().and_then(|()| {
            type Case = fn(&Fixture) -> Result<()>;
            let cases: [Case; 6] = [
                pending_states_exhaust_the_original_budget,
                same_revision_pending_install_converges,
                same_revision_pending_upgrade_and_rollback_converge,
                status_only_transition_discards_the_sample,
                stable_hook_failure_and_uninstalled_release_are_terminal,
                replacement_revision_is_refused_within_and_between_samples,
            ];
            let barrier = std::sync::Barrier::new(cases.len());
            thread::scope(|scope| {
                let handles: Vec<_> = cases
                    .into_iter()
                    .map(|case| {
                        let shared = &shared;
                        let barrier = &barrier;
                        scope.spawn(move || {
                            barrier.wait();
                            case(shared)
                        })
                    })
                    .collect();
                let mut errors = Vec::new();
                for handle in handles {
                    match handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => errors.push(format!("{error:#}")),
                        Err(_) => errors.push("convergence case thread panicked".to_owned()),
                    }
                }
                anyhow::ensure!(errors.is_empty(), "{}", errors.join("; "));
                Ok(())
            })
        });
        let cleanup = shared.cleanup();
        match (outcome, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("shared cluster teardown also failed: {cleanup:#}")))
            }
        }
    }

    fn release_at_barrier(fixture: &mut Fixture, target: &Target, permit: Permit) -> Result<()> {
        fixture.open_hook(target, permit.remaining())?;
        fixture.finish_helm(target.operation, true, permit.remaining())?;
        permit.resume()
    }

    fn pending_success(
        fixture: &mut Fixture,
        test: &str,
        operation: Operation,
        contract: &str,
    ) -> Result<()> {
        let target = fixture.prepare_pending(operation, true, "schema-migrate", 0)?;
        let mut barrier = Barrier::new()?;
        let child = fixture.child(
            test,
            &target.release,
            contract,
            "success",
            target.revision,
            Some(&barrier),
        )?;
        // The second real status has already captured pending, so the first
        // sample must retry even if Helm finishes before those bytes return.
        let permit = barrier.pause("status", 2, "after")?;
        release_at_barrier(fixture, &target, permit)?;
        fixture.finish_child(child, Some(&mut barrier), Duration::from_secs(45))?;
        fixture.history(&target)
    }

    fn same_revision_pending_install_converges(shared: &Fixture) -> Result<()> {
        fixture_test(
            "same_revision_pending_install_converges",
            shared,
            |fixture| {
                for contract in ["wait", "structured"] {
                    pending_success(
                        fixture,
                        "same_revision_pending_install_converges",
                        Operation::Install,
                        contract,
                    )?;
                }
                Ok(())
            },
        )
    }

    fn same_revision_pending_upgrade_and_rollback_converge(shared: &Fixture) -> Result<()> {
        fixture_test(
            "same_revision_pending_upgrade_and_rollback_converge",
            shared,
            |fixture| {
                for operation in [Operation::Upgrade, Operation::Rollback] {
                    for contract in ["wait", "structured"] {
                        pending_success(
                            fixture,
                            "same_revision_pending_upgrade_and_rollback_converge",
                            operation,
                            contract,
                        )?;
                    }
                }
                Ok(())
            },
        )
    }

    fn status_only_transition_discards_the_sample(shared: &Fixture) -> Result<()> {
        fixture_test(
            "status_only_transition_discards_the_sample",
            shared,
            |fixture| {
                for (contract, expectation) in [
                    ("wait", "discard"),
                    ("wait", "discard-wait"),
                    ("structured", "discard-wait"),
                ] {
                    let target =
                        fixture.prepare_pending(Operation::Install, false, "schema-migrate", 0)?;
                    let mut barrier = Barrier::new()?;
                    let child = fixture.child(
                        "status_only_transition_discards_the_sample",
                        &target.release,
                        contract,
                        expectation,
                        1,
                        Some(&barrier),
                    )?;
                    // Capture a genuine unready workload, then finish the same
                    // Helm operation before the final status read.
                    let permit = barrier.pause("workloads", 1, "after")?;
                    release_at_barrier(fixture, &target, permit)?;
                    let permit = barrier.pause("workloads", 2, "before")?;
                    fixture.open_workload(&target.release, permit.remaining())?;
                    permit.resume()?;
                    fixture.finish_child(child, Some(&mut barrier), Duration::from_secs(45))?;
                    fixture.history(&target)?;
                }
                Ok(())
            },
        )
    }

    fn stable_hook_failure_and_uninstalled_release_are_terminal(shared: &Fixture) -> Result<()> {
        fixture_test(
            "stable_hook_failure_and_uninstalled_release_are_terminal",
            shared,
            |fixture| {
                for (hook, expectation) in [
                    ("schema-migrate", "terminal-hook"),
                    ("upgrade-drain-hold", "terminal-drain"),
                ] {
                    let target = fixture.prepare_pending(Operation::Install, true, hook, 17)?;
                    fixture.open_hook(&target, Duration::from_secs(10))?;
                    fixture.finish_helm(target.operation, false, Duration::from_secs(45))?;
                    anyhow::ensure!(
                        fixture.status(&target.release)? == (1, "failed".to_owned()),
                        "Helm must record a genuine hook failure"
                    );
                    for contract in ["wait", "structured"] {
                        let child = fixture.child(
                            "stable_hook_failure_and_uninstalled_release_are_terminal",
                            &target.release,
                            contract,
                            expectation,
                            1,
                            None,
                        )?;
                        fixture.finish_child(child, None, Duration::from_secs(20))?;
                    }
                }
                let release = fixture.release()?;
                let install =
                    fixture.helm_change("install", &release, false, true, "schema-migrate", 0)?;
                fixture.finish_helm(install, true, Duration::from_secs(60))?;
                fixture.invoke(
                    "helm",
                    &[
                        "uninstall",
                        &release,
                        "-n",
                        &release,
                        "--keep-history",
                        "--no-hooks",
                    ],
                    Duration::from_secs(30),
                )?;
                let remaining = fixture.invoke(
                    "kubectl",
                    &[
                        "get",
                        &format!("deployment/{release}-workload"),
                        "-n",
                        &release,
                        "--ignore-not-found",
                        "-o",
                        "name",
                    ],
                    Duration::from_secs(10),
                )?;
                anyhow::ensure!(
                    remaining.trim().is_empty(),
                    "uninstalled workload must be absent"
                );
                anyhow::ensure!(
                    fixture.status(&release)? == (1, "uninstalled".to_owned()),
                    "uninstall must retain real Helm history"
                );
                for contract in ["wait", "structured"] {
                    let child = fixture.child(
                        "stable_hook_failure_and_uninstalled_release_are_terminal",
                        &release,
                        contract,
                        "terminal-uninstalled",
                        1,
                        None,
                    )?;
                    fixture.finish_child(child, None, Duration::from_secs(20))?;
                }
                let absent = fixture.release()?;
                for contract in ["wait", "structured"] {
                    let child = fixture.child(
                        "stable_hook_failure_and_uninstalled_release_are_terminal",
                        &absent,
                        contract,
                        "unreadable",
                        1,
                        None,
                    )?;
                    fixture.finish_child(child, None, Duration::from_secs(20))?;
                }
                Ok(())
            },
        )
    }

    fn replace(fixture: &mut Fixture, release: &str, permit: Permit) -> Result<()> {
        let mut command = fixture.command("helm");
        command
            .args(["upgrade", release])
            .arg(fixture.root.path().join("chart"));
        command.args([
            "-n",
            release,
            "--no-hooks",
            "--set",
            "gated=false,ready=true",
        ]);
        let process = fixture.spawn(command)?;
        fixture.finish_helm(process, true, permit.remaining())?;
        permit.resume()
    }

    fn replacement_revision_is_refused_within_and_between_samples(shared: &Fixture) -> Result<()> {
        fixture_test(
            "replacement_revision_is_refused_within_and_between_samples",
            shared,
            |fixture| {
                for contract in ["wait", "structured"] {
                    for expectation in ["within", "between", "discard-then-replace"] {
                        let mut barrier = Barrier::new()?;
                        let release = if expectation == "discard-then-replace" {
                            let target = fixture.prepare_pending(
                                Operation::Install,
                                false,
                                "schema-migrate",
                                0,
                            )?;
                            let child = fixture.child(
                                "replacement_revision_is_refused_within_and_between_samples",
                                &target.release,
                                contract,
                                expectation,
                                1,
                                Some(&barrier),
                            )?;
                            let permit = barrier.pause("workloads", 1, "after")?;
                            release_at_barrier(fixture, &target, permit)?;
                            let permit = barrier.pause("status", 3, "before")?;
                            replace(fixture, &target.release, permit)?;
                            fixture.finish_child(
                                child,
                                Some(&mut barrier),
                                Duration::from_secs(30),
                            )?;
                            continue;
                        } else {
                            let release = fixture.release()?;
                            let install = fixture.helm_change(
                                "install",
                                &release,
                                false,
                                false,
                                "schema-migrate",
                                0,
                            )?;
                            fixture.finish_helm(install, true, Duration::from_secs(60))?;
                            release
                        };
                        let child = fixture.child(
                            "replacement_revision_is_refused_within_and_between_samples",
                            &release,
                            contract,
                            expectation,
                            1,
                            Some(&barrier),
                        )?;
                        let permit = if expectation == "within" {
                            barrier.pause("workloads", 1, "after")?
                        } else {
                            barrier.pause("status", 3, "before")?
                        };
                        replace(fixture, &release, permit)?;
                        fixture.finish_child(child, Some(&mut barrier), Duration::from_secs(30))?;
                    }
                }
                Ok(())
            },
        )
    }

    fn pending_states_exhaust_the_original_budget(shared: &Fixture) -> Result<()> {
        fixture_test(
            "pending_states_exhaust_the_original_budget",
            shared,
            |fixture| {
                let mut cases = Vec::new();
                for operation in [Operation::Install, Operation::Upgrade, Operation::Rollback] {
                    for contract in ["wait", "structured"] {
                        let target =
                            fixture.prepare_pending(operation, true, "schema-migrate", 0)?;
                        cases.push((target, contract));
                    }
                }
                let mut children = Vec::new();
                for (target, contract) in &cases {
                    children.push(fixture.child(
                        "pending_states_exhaust_the_original_budget",
                        &target.release,
                        contract,
                        "budget",
                        target.revision,
                        None,
                    )?);
                }
                let deadline = Instant::now() + Duration::from_secs(315);
                for child in children {
                    fixture.finish_child(
                        child,
                        None,
                        deadline.saturating_duration_since(Instant::now()),
                    )?;
                }
                for ((target, _), operation) in cases.iter().zip([
                    Operation::Install,
                    Operation::Install,
                    Operation::Upgrade,
                    Operation::Upgrade,
                    Operation::Rollback,
                    Operation::Rollback,
                ]) {
                    anyhow::ensure!(
                        fixture.status(&target.release)?
                            == (target.revision, operation.pending().to_owned()),
                        "closed gate must remain at its original pending revision"
                    );
                }
                Ok(())
            },
        )
    }
}

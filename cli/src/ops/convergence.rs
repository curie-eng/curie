//! Read-only convergence against Helm's exact installed target manifest.
//! Shared by up/apply and status; this does not perform transactional recovery.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::{plain, run_capture, CommonOpts, OpsCommand, UpInvocation};

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
/// stalled rollout, a crash-looping or terminating container, an undeployed
/// revision, an empty target manifest, a StatefulSet mid-revision, or a Helm
/// revision that moved under the observation. It fails `exact` without lying
/// about which specific property was disproved.
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

/// The #2010 worker drain gate, by the hook names
/// `charts/curie/templates/worker-upgrade-drain.yaml` renders (the pre-upgrade
/// quiesce Job and its post-upgrade release). A refusal there means accepted
/// work had not settled when the roll began, which `cluster upgrade` reports
/// separately from every other hook.
fn hook_facet(name: &str) -> Facet {
    if name.ends_with("-upgrade-drain") || name.ends_with("-upgrade-drain-release") {
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

async fn capture(command: OpsCommand, description: &str) -> Result<String> {
    let (ok, out, _) = tokio::time::timeout(Duration::from_secs(10), run_capture(&command))
        .await
        .with_context(|| format!("{description} timed out after 10 seconds"))?
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

async fn helm_status(opts: &CommonOpts) -> Result<Value> {
    let raw = capture(helm_status_command(opts), "read Helm release state").await?;
    serde_json::from_str(&raw).context("Helm release state is malformed")
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

/// Bind a tagged request or kubelet alias to the running imageID digest.
/// Prefer a unique digest on matching inventory entries; if those entries are
/// digest-less, a unique same-repository digest on the node that equals the
/// running identity. A missing name is unbound. Do not infer across repositories.
/// https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/#NodeStatus
fn resolve_reference_identity(node: &Value, reference: &str, running: &str) -> Option<String> {
    match matching_inventory_identities(node, reference) {
        Some(identities) if identities.len() > 1 => None,
        Some(identities) if identities.is_empty() => {
            let identity = unique_repository_identity(node, &repository(reference))?;
            (identity == running).then_some(identity)
        }
        Some(_) => unique_inventory_identity(node, reference),
        None => None,
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
        .filter(|item| text(item, "/kind") == "Pod" && selected(expected, item))
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

async fn observe_inner(opts: &CommonOpts) -> Result<Observation> {
    let status = helm_status(opts).await?;
    let revision = status
        .get("version")
        .and_then(Value::as_u64)
        .filter(|version| *version > 0)
        .context("Helm release has no verifiable revision")?;
    let mut result = Observation::default();
    let last_deployed = rfc3339(text(&status, "/info/last_deployed"));
    if text(&status, "/info/status") != "deployed" {
        result.issue(
            Facet::Rollout,
            "Helm release",
            "latest revision is not deployed",
        );
        result.terminal = true;
    }
    let manifest = capture(
        manifest_command(opts, &revision.to_string()),
        "read target release manifest",
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
            .filter(|item| text(item, "/kind") == "Pod" && selected(object, item))
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
        let raw = capture(node_images_command(&name), "read serving Node image inventory").await
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
    let final_status = helm_status(opts).await?;
    if final_status.get("version") != status.get("version")
        || final_status.pointer("/info/status") != status.pointer("/info/status")
    {
        result.issue(
            Facet::Rollout,
            "Helm release",
            "revision changed during convergence observation",
        );
        result.terminal = true;
    }
    Ok(result)
}

pub(super) async fn observe(opts: &CommonOpts) -> Result<Observation> {
    tokio::time::timeout(Duration::from_secs(30), observe_inner(opts))
        .await
        .context("convergence observation timed out after 30 seconds")?
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

pub(super) async fn wait(opts: &CommonOpts, invocation: UpInvocation) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut last_issues: Vec<String> = Vec::new();
    let fix = match invocation {
        UpInvocation::ClusterUp => "run `curie cluster status` to inspect the failed rollout; correct the target configuration and rerun `curie cluster up`",
        UpInvocation::Apply => "run `curie cluster status` to inspect the failed rollout; correct the target configuration in `curie.yaml` and rerun `curie apply`",
    };
    loop {
        let observation = tokio::time::timeout_at(deadline, observe(opts))
            .await
            .context("post-Helm convergence timed out after 300 seconds")
            .and_then(|result| result);
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
/// terminal observation rather than an `Err` — an unreadable cluster has not
/// converged, and that is a verdict, not a crash.
/// Every facet, for the one case where nothing at all could be observed.
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

pub(super) async fn wait_for_observation(opts: &CommonOpts) -> Observation {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut carried = Observation::default();
    loop {
        let observed = tokio::time::timeout_at(deadline, observe(opts))
            .await
            .context("post-Helm convergence timed out after 300 seconds")
            .and_then(|result| result);
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

//! Read-only convergence against Helm's exact installed target manifest.
//! Shared by up/apply and status; this does not perform transactional recovery.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use super::{plain, run_capture, CommonOpts, OpsCommand};

#[derive(Default)]
pub(super) struct Observation {
    pub issues: Vec<String>,
    pub terminal: bool,
}

impl Observation {
    fn issue(&mut self, resource: &str, reason: impl std::fmt::Display) {
        self.issues.push(format!("{resource}: {reason}"));
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
    // Kubelet's per-node inventory groups aliases for one loaded image. Never
    // combine separate entries or infer equivalence from tag/repository text.
    // Missing/truncated/ambiguous inventory cannot establish this binding.
    let matches: Vec<_> = array(node, "/status/images")
        .iter()
        .filter(|image| {
            array(image, "/names")
                .iter()
                .filter_map(Value::as_str)
                .any(|name| normalize_image(name) == normalize_image(expected))
        })
        .collect();
    matches.len() == 1
        && array(matches[0], "/names")
            .iter()
            .filter_map(Value::as_str)
            .any(|name| normalize_image(name) == normalize_image(text(status, "/image")))
        && array(matches[0], "/names")
            .iter()
            .filter_map(Value::as_str)
            .any(|name| manifest_identity(name).as_ref() == Some(&identity))
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
        result.issue(name, reason(pod_reason));
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
                result.issue(&id, reason(text(waiting, "/reason")));
            }
            if let Some(terminated) = container.pointer("/state/terminated") {
                if !init || count(terminated, "/exitCode") != 0 {
                    result.issue(
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
                result.issue(id, format!("container {name} does not match target image"));
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
                result.issue(
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
                    result.issue(id, "admission-injected container is not ready");
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
        result.issue(id, "target generation has not been observed");
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
    if updated != desired || ready != desired || total != desired || unavailable != 0 {
        result.issue(id, format!("replicas desired={desired} updated={updated} ready={ready} total={total} unavailable={unavailable}"));
    }
    if kind == "StatefulSet"
        && desired > 0
        && (text(actual, "/status/currentRevision").is_empty()
            || text(actual, "/status/currentRevision") != text(actual, "/status/updateRevision"))
    {
        result.issue(
            id,
            "StatefulSet current revision does not match target revision",
        );
    }
    for condition in array(actual, "/status/conditions") {
        if text(condition, "/type") == "Progressing"
            && text(condition, "/status") == "False"
            && text(condition, "/reason") == "ProgressDeadlineExceeded"
        {
            result.issue(id, "ProgressDeadlineExceeded");
            result.terminal = true;
        }
    }
    if expected.pointer("/spec/selector") != actual.pointer("/spec/selector") {
        result.issue(id, "workload selector differs from target");
    }
    compare_containers(expected, actual, false, nodes, result);
    let pods: Vec<_> = items
        .iter()
        .filter(|item| text(item, "/kind") == "Pod" && selected(expected, item))
        .collect();
    if pods.len() as u64 != desired {
        result.issue(
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
    if text(&status, "/info/status") != "deployed" {
        result.issue("Helm release", "latest revision is not deployed");
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
            result.issue(text(hook, "/name"), "Helm hook failed");
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
            result.issue(text(object, "/metadata/name"), "target workload is absent");
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
                text(item, "/kind") == "Job" && text(item, "/metadata/name") == text(hook, "/name")
            })
        {
            for condition in array(job, "/status/conditions") {
                if text(condition, "/type") == "Failed" && text(condition, "/status") == "True" {
                    result.issue(
                        text(job, "/metadata/name"),
                        reason(text(condition, "/reason")),
                    );
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
) -> anyhow::Error {
    if let Ok(observation) = observe(opts).await {
        if !observation.issues.is_empty() {
            return crate::exit::CliError::failure(format!(
                "Helm installation failed; observed rollout reasons: {}",
                observation.issues.join("; ")
            ))
            .with_fix("run `curie cluster status`, correct the failed hook or workload and retry")
            .into();
        }
    }
    original
}

pub(super) async fn wait(opts: &CommonOpts) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut last_issues: Vec<String> = Vec::new();
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
                .with_fix("run `curie cluster status` to inspect the failed rollout; correct the target configuration and rerun `curie cluster up`")
                .into());
            }
        };
        if result.issues.is_empty() {
            return Ok(());
        }
        if result.terminal || tokio::time::Instant::now() + Duration::from_secs(2) >= deadline {
            return Err(crate::exit::CliError::failure(format!("target release has not converged: {}", result.issues.join("; "))).with_fix("run `curie cluster status` to inspect the failed rollout; correct the target configuration and rerun `curie cluster up`").into());
        }
        last_issues = result.issues;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

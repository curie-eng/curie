//! Install or reuse External Secrets during `curie apply`.
//!
//! The chart does not render these objects. A provider file installs the pinned
//! chart when nothing is there, reuses a compatible controller, and refuses an
//! incompatible one before any write. Teardown removes the controller only when
//! this process installed it.

use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::eso::{
    apply, render_secret_store, render_service_account, Kubectl, KubectlOutput, StoreSpec,
    SystemKubectl, ESO_VERSION, EXTERNAL_SECRET_API, PUSH_SECRET_API,
};

pub const ESO_RELEASE: &str = "external-secrets";
pub const ESO_NAMESPACE: &str = "external-secrets";
pub const ESO_CHART: &str = "oci://ghcr.io/external-secrets/charts/external-secrets";
pub const ESO_DEPLOYMENT: &str = "external-secrets";
pub const OWNERSHIP_CONFIGMAP: &str = "curie-eso-install";
pub const INSTALLED_BY: &str = "curie";
const PINNED_MINOR: (u64, u64) = (2, 11);
const READY_TIMEOUT: Duration = Duration::from_secs(180);

/// What a read of the cluster showed. Writes have not happened yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallationView {
    pub helm_chart: Option<String>,
    pub image: Option<String>,
    pub controller_present: bool,
    pub external_secret_served: Vec<String>,
    pub push_secret_served: Vec<String>,
    /// `None` watches every namespace.
    pub watch_namespace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Install,
    Reuse,
    Refuse(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureOutcome {
    Installed,
    Reused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerTeardown {
    Removed,
    Retained,
    /// The ownership marker could not be read. The controller stays.
    Unproven,
}

/// Helm, so tests can script it. Stdout and stderr only.
pub trait Helm {
    fn run(&self, args: &[String]) -> Result<KubectlOutput>;
}

/// `helm` from PATH. The process environment supplies kubeconfig and context.
#[derive(Debug, Default, Clone)]
pub struct SystemHelm;

impl Helm for SystemHelm {
    fn run(&self, args: &[String]) -> Result<KubectlOutput> {
        let output = std::process::Command::new("helm")
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .context("could not start helm")?;
        Ok(KubectlOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// `major.minor` of a chart version or image reference. A leading `v` is ignored.
pub fn minor_version(raw: &str) -> Option<(u64, u64)> {
    let trimmed = raw.trim();
    let without_digest = trimmed.split('@').next().unwrap_or(trimmed);
    let tag = without_digest
        .rsplit_once(':')
        .map(|(_, tag)| tag)
        .unwrap_or(without_digest);
    let version = tag.trim_start_matches('v');
    let version = version.rsplit('-').next().unwrap_or(version);
    if !version.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Install when nothing is present. Reuse only a 2.11 controller that serves
/// ExternalSecret `v1` and PushSecret `v1alpha1` and watches this namespace.
pub fn decide(view: &InstallationView, install_namespace: &str) -> Decision {
    let present = view.helm_chart.is_some()
        || view.image.is_some()
        || view.controller_present
        || !view.external_secret_served.is_empty()
        || !view.push_secret_served.is_empty();
    if !present {
        return Decision::Install;
    }
    let mut problems = Vec::new();
    match &view.helm_chart {
        Some(chart) if minor_version(chart) == Some(PINNED_MINOR) => {}
        Some(chart) => problems.push(format!(
            "helm chart {chart} is not External Secrets {ESO_VERSION}"
        )),
        None => problems.push(format!(
            "helm release {ESO_NAMESPACE}/{ESO_RELEASE} is absent"
        )),
    }
    match &view.image {
        Some(image) if minor_version(image) == Some(PINNED_MINOR) => {}
        Some(image) => problems.push(format!(
            "controller image {image} is not External Secrets {ESO_VERSION}"
        )),
        None => problems.push("External Secrets controller image is absent".to_string()),
    }
    if !view
        .external_secret_served
        .iter()
        .any(|version| version == "v1")
    {
        problems.push(format!(
            "ExternalSecret CRD does not serve {EXTERNAL_SECRET_API}"
        ));
    }
    if !view
        .push_secret_served
        .iter()
        .any(|version| version == "v1alpha1")
    {
        problems.push(format!("PushSecret CRD does not serve {PUSH_SECRET_API}"));
    }
    if let Some(scope) = &view.watch_namespace {
        if scope != install_namespace {
            problems.push(format!(
                "External Secrets watches namespace {scope}, not {install_namespace}"
            ));
        }
    }
    if problems.is_empty() {
        Decision::Reuse
    } else {
        Decision::Refuse(problems.join("; "))
    }
}

/// Commands a dry run reports. Nothing is executed.
pub fn dry_run_lines(spec: &StoreSpec) -> Vec<String> {
    vec![
        format!(
            "helm upgrade --install {ESO_RELEASE} {ESO_CHART} --version {ESO_VERSION} --namespace {ESO_NAMESPACE} --create-namespace --set installCRDs=true --wait --timeout 5m"
        ),
        format!(
            "kubectl apply serviceaccount {} in {} annotated for {}",
            spec.service_account, spec.namespace, spec.role_arn
        ),
        format!(
            "kubectl apply secretstore {} in {} region {}",
            spec.name, spec.namespace, spec.region
        ),
    ]
}

pub fn ensure(kubectl: &dyn Kubectl, helm: &dyn Helm, spec: &StoreSpec) -> Result<EnsureOutcome> {
    let view = inspect(kubectl, helm)?;
    let outcome = match decide(&view, &spec.namespace) {
        Decision::Install => {
            install_controller(helm)?;
            mark_owned(kubectl)?;
            EnsureOutcome::Installed
        }
        Decision::Reuse => EnsureOutcome::Reused,
        Decision::Refuse(reason) => {
            bail!(
                "refusing to change External Secrets: {reason}. Nothing was changed. Install External Secrets {ESO_VERSION} with ExternalSecret v1 and PushSecret v1alpha1, watching this namespace, or remove the other controller first."
            )
        }
    };
    ensure_namespace(kubectl, &spec.namespace)?;
    let objects = vec![render_service_account(spec), render_secret_store(spec)];
    apply(kubectl, &spec.namespace, &objects)?;
    wait_until_ready(kubectl, &spec.namespace, &spec.name, READY_TIMEOUT)?;
    Ok(outcome)
}

pub fn ensure_system(spec: &StoreSpec) -> Result<EnsureOutcome> {
    crate::ops::require_on_path("helm")?;
    crate::ops::require_on_path("kubectl")?;
    ensure(&SystemKubectl::default(), &SystemHelm, spec)
}

/// Uninstall the controller only when the ownership marker says Curie installed it.
pub fn remove_owned_controller(
    kubectl: &dyn Kubectl,
    helm: &dyn Helm,
) -> Result<ControllerTeardown> {
    match read_ownership(kubectl) {
        Ownership::Absent | Ownership::Foreign => Ok(ControllerTeardown::Retained),
        Ownership::Unreadable => Ok(ControllerTeardown::Unproven),
        Ownership::Owned => {
            let args = argv(&["uninstall", ESO_RELEASE, "-n", ESO_NAMESPACE]);
            let out = helm.run(&args)?;
            if !out.success
                && !out.stderr.contains("not found")
                && !out.stdout.contains("not found")
            {
                bail!(
                    "could not uninstall External Secrets release {ESO_NAMESPACE}/{ESO_RELEASE}: {}",
                    out.stderr.trim()
                );
            }
            Ok(ControllerTeardown::Removed)
        }
    }
}

pub fn remove_owned_controller_system() -> Result<ControllerTeardown> {
    crate::ops::require_on_path("helm")?;
    crate::ops::require_on_path("kubectl")?;
    remove_owned_controller(&SystemKubectl::default(), &SystemHelm)
}

enum Ownership {
    Owned,
    Absent,
    Foreign,
    Unreadable,
}

fn inspect(kubectl: &dyn Kubectl, helm: &dyn Helm) -> Result<InstallationView> {
    let listed = helm.run(&argv(&[
        "list",
        "-n",
        ESO_NAMESPACE,
        "-o",
        "json",
        "-f",
        ESO_RELEASE,
    ]))?;
    let helm_chart = if listed.success {
        chart_version(&listed.stdout)
    } else if listed.stderr.contains("NotFound") || listed.stderr.contains("not found") {
        None
    } else {
        bail!(
            "could not list helm releases in {ESO_NAMESPACE}: {}",
            listed.stderr.trim()
        )
    };
    let deployment = kubectl.run(
        &argv(&[
            "-n",
            ESO_NAMESPACE,
            "get",
            "deploy",
            ESO_DEPLOYMENT,
            "-o",
            "json",
        ]),
        None,
    )?;
    let (controller_present, image, watch_namespace) = if deployment.success {
        let body: Value = serde_json::from_str(&deployment.stdout)
            .context("External Secrets deployment returned invalid JSON")?;
        (true, controller_image(&body), watch_namespace(&body))
    } else if is_missing(&deployment.stderr) {
        (false, None, None)
    } else {
        bail!(
            "could not read External Secrets deployment: {}",
            deployment.stderr.trim()
        )
    };
    Ok(InstallationView {
        helm_chart,
        image,
        controller_present,
        external_secret_served: served_crd(kubectl, "externalsecrets.external-secrets.io")?,
        push_secret_served: served_crd(kubectl, "pushsecrets.external-secrets.io")?,
        watch_namespace,
    })
}

fn chart_version(stdout: &str) -> Option<String> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    let releases = value.as_array()?;
    releases.iter().find_map(|release| {
        let name = release["name"].as_str()?;
        if name != ESO_RELEASE {
            return None;
        }
        release["chart"].as_str().map(str::to_string)
    })
}

fn controller_image(deployment: &Value) -> Option<String> {
    let containers = deployment["spec"]["template"]["spec"]["containers"].as_array()?;
    let container = containers
        .iter()
        .find(|container| container["name"].as_str() == Some(ESO_DEPLOYMENT))
        .or_else(|| containers.first())?;
    container["image"].as_str().map(str::to_string)
}

fn watch_namespace(deployment: &Value) -> Option<String> {
    let containers = deployment["spec"]["template"]["spec"]["containers"].as_array()?;
    for container in containers {
        let mut tokens = Vec::new();
        for field in ["command", "args"] {
            if let Some(values) = container[field].as_array() {
                for value in values {
                    if let Some(token) = value.as_str() {
                        tokens.push(token.to_string());
                    }
                }
            }
        }
        for (index, token) in tokens.iter().enumerate() {
            if let Some(value) = token.strip_prefix("--scoped-namespace=") {
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
            if token == "--scoped-namespace" {
                if let Some(next) = tokens.get(index + 1) {
                    if !next.starts_with('-') {
                        return Some(next.clone());
                    }
                }
            }
        }
        if let Some(env) = container["env"].as_array() {
            for item in env {
                let name = item["name"].as_str().unwrap_or("");
                if name == "WATCH_NAMESPACE" || name == "SCOPE_NAMESPACE" {
                    if let Some(value) = item["value"].as_str() {
                        if !value.is_empty() {
                            return Some(value.to_string());
                        }
                    }
                }
            }
        }
    }
    None
}

fn served_crd(kubectl: &dyn Kubectl, name: &str) -> Result<Vec<String>> {
    let out = kubectl.run(&argv(&["get", "crd", name, "-o", "json"]), None)?;
    if is_missing(&out.stderr) || (!out.success && out.stderr.contains("NotFound")) {
        return Ok(Vec::new());
    }
    if !out.success {
        bail!("could not read CRD {name}: {}", out.stderr.trim());
    }
    let body: Value = serde_json::from_str(&out.stdout)
        .with_context(|| format!("CRD {name} returned invalid JSON"))?;
    let versions = body["spec"]["versions"]
        .as_array()
        .map(|versions| {
            versions
                .iter()
                .filter(|version| version["served"].as_bool().unwrap_or(true))
                .filter_map(|version| version["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Ok(versions)
}

fn install_controller(helm: &dyn Helm) -> Result<()> {
    let args = argv(&[
        "upgrade",
        "--install",
        ESO_RELEASE,
        ESO_CHART,
        "--version",
        ESO_VERSION,
        "--namespace",
        ESO_NAMESPACE,
        "--create-namespace",
        "--set",
        "installCRDs=true",
        "--wait",
        "--timeout",
        "5m",
    ]);
    let out = helm.run(&args)?;
    if !out.success {
        bail!(
            "could not install External Secrets {ESO_VERSION}: {}",
            out.stderr.trim()
        );
    }
    Ok(())
}

fn mark_owned(kubectl: &dyn Kubectl) -> Result<()> {
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": OWNERSHIP_CONFIGMAP,
            "namespace": ESO_NAMESPACE,
        },
        "data": {
            "installed-by": INSTALLED_BY,
            "chart-version": ESO_VERSION,
        },
    });
    apply_raw(kubectl, None, &body)
}

fn ensure_namespace(kubectl: &dyn Kubectl, namespace: &str) -> Result<()> {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": namespace },
    });
    apply_raw(kubectl, None, &body)
}

fn apply_raw(kubectl: &dyn Kubectl, namespace: Option<&str>, body: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(body)?;
    let mut parts = Vec::new();
    if let Some(namespace) = namespace {
        parts.extend(["-n", namespace]);
    }
    parts.extend(["apply", "-f", "-"]);
    let args = argv_owned(parts);
    let out = kubectl.run(&args, Some(&bytes))?;
    if !out.success {
        bail!("kubectl apply failed: {}", out.stderr.trim());
    }
    Ok(())
}

fn wait_until_ready(
    kubectl: &dyn Kubectl,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let out = kubectl.run(
            &argv(&["-n", namespace, "get", "secretstore", name, "-o", "json"]),
            None,
        )?;
        if out.success {
            if let Ok(body) = serde_json::from_str::<Value>(&out.stdout) {
                if store_ready(&body) {
                    return Ok(());
                }
            }
        } else if !is_missing(&out.stderr) && Instant::now() >= deadline {
            bail!(
                "could not read SecretStore {namespace}/{name}: {}",
                out.stderr.trim()
            );
        }
        if Instant::now() >= deadline {
            bail!(
                "SecretStore {namespace}/{name} did not become Ready within {}s",
                timeout.as_secs()
            );
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(Duration::from_secs(2).min(remaining));
    }
}

fn store_ready(body: &Value) -> bool {
    body["status"]["conditions"]
        .as_array()
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition["type"] == json!("Ready") && condition["status"] == json!("True")
            })
        })
}

fn read_ownership(kubectl: &dyn Kubectl) -> Ownership {
    let out = match kubectl.run(
        &argv(&[
            "-n",
            ESO_NAMESPACE,
            "get",
            "configmap",
            OWNERSHIP_CONFIGMAP,
            "-o",
            "json",
        ]),
        None,
    ) {
        Ok(out) => out,
        Err(_) => return Ownership::Unreadable,
    };
    if !out.success {
        if is_missing(&out.stderr) {
            return Ownership::Absent;
        }
        return Ownership::Unreadable;
    }
    let Ok(body) = serde_json::from_str::<Value>(&out.stdout) else {
        return Ownership::Unreadable;
    };
    if body["data"]["installed-by"].as_str() == Some(INSTALLED_BY) {
        Ownership::Owned
    } else {
        Ownership::Foreign
    }
}

fn is_missing(stderr: &str) -> bool {
    stderr.contains("NotFound")
        || stderr.contains("the server doesn't have a resource type")
        || stderr.contains("not found")
}

fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_string()).collect()
}

fn argv_owned(parts: Vec<&str>) -> Vec<String> {
    parts.into_iter().map(str::to_string).collect()
}

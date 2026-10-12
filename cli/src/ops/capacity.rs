//! Advisory platform requests accounting from the final Helm render.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use super::{plain, run_capture, CommonOpts, OpsCommand};
use crate::ui::Ui;

const MIB: u128 = 1024 * 1024;

#[derive(Clone, Copy, Default)]
struct Requests {
    cpu: u128,
    memory: u128,
}

impl Requests {
    fn add(self, other: Self) -> Result<Self> {
        Ok(Self {
            cpu: self
                .cpu
                .checked_add(other.cpu)
                .context("CPU request total overflowed")?,
            memory: self
                .memory
                .checked_add(other.memory)
                .context("memory request total overflowed")?,
        })
    }

    fn multiply(self, count: u128) -> Result<Self> {
        Ok(Self {
            cpu: self
                .cpu
                .checked_mul(count)
                .context("CPU request total overflowed")?,
            memory: self
                .memory
                .checked_mul(count)
                .context("memory request total overflowed")?,
        })
    }

    fn max(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.max(other.cpu),
            memory: self.memory.max(other.memory),
        }
    }
}

struct Projection {
    amount: u128,
    percentage: String,
    shortfall: Option<u128>,
}

impl Projection {
    fn new(amount: u128, allocatable: u128) -> Result<Self> {
        if allocatable == 0 {
            bail!("eligible nodes have zero allocatable capacity");
        }
        let weighted = amount
            .checked_mul(5)
            .context("projected request total overflowed")?;
        let threshold = allocatable
            .checked_mul(4)
            .context("allocatable capacity total overflowed")?;
        let tenths = amount
            .checked_mul(1000)
            .context("projected request percentage overflowed")?
            / allocatable;
        Ok(Self {
            amount,
            percentage: if tenths % 10 == 0 {
                (tenths / 10).to_string()
            } else {
                format!("{}.{}", tenths / 10, tenths % 10)
            },
            shortfall: (weighted > threshold).then(|| weighted.div_ceil(4) - allocatable),
        })
    }
}

struct Report {
    install: Requests,
    allocatable: Requests,
    existing: Requests,
    eligible_nodes: usize,
    gke: bool,
    cpu: Projection,
    memory: Projection,
}

impl Report {
    fn messages(&self) -> (String, Vec<String>, Option<&'static str>) {
        let note = format!(
            "capacity: install requests {}m CPU and {}Mi memory across {} eligible nodes; already requested {}m CPU and {}Mi memory; projected CPU {}% and memory {}% of allocatable",
            self.install.cpu,
            self.install.memory.div_ceil(MIB),
            self.eligible_nodes,
            self.existing.cpu,
            self.existing.memory.div_ceil(MIB),
            self.cpu.percentage,
            self.memory.percentage,
        );
        let mut warnings = Vec::new();
        if let Some(shortfall) = self.cpu.shortfall {
            warnings.push(format!(
                "capacity: projected CPU {}% ({}m requested / {}m allocatable) exceeds 80%; additional allocatable shortfall: {}m",
                self.cpu.percentage, self.cpu.amount, self.allocatable.cpu, shortfall,
            ));
        }
        if let Some(shortfall) = self.memory.shortfall {
            warnings.push(format!(
                "capacity: projected memory {}% ({}Mi requested / {}Mi allocatable) exceeds 80%; additional allocatable shortfall: {}Mi",
                self.memory.percentage,
                self.memory.amount.div_ceil(MIB),
                self.allocatable.memory.div_ceil(MIB),
                shortfall.div_ceil(MIB),
            ));
        }
        let recommendation = (!warnings.is_empty()).then_some(if self.gke {
            "On GKE, use a node pool of e2-standard-4 or larger with three nodes."
        } else {
            "Add nodes or use larger nodes until allocatable grows by the shortfall."
        });
        (note, warnings, recommendation)
    }
}

fn quantity(value: &Value, cpu: bool) -> Result<u128> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => bail!("unsupported resource quantity"),
    };
    if cpu {
        parse_cpu_quantity(&text).map_err(|_| anyhow!("unsupported CPU quantity"))
    } else {
        // The shared parser retains the example installer's original error
        // details. Never expose those details from this advisory calculation.
        parse_memory_quantity(&text).map_err(|_| anyhow!("unsupported memory quantity"))
    }
}

fn resource_requests(resources: &Value) -> Result<Requests> {
    if !resources.is_object() {
        bail!("malformed resource requests");
    }
    Ok(Requests {
        cpu: resources
            .get("cpu")
            .map_or(Ok(0), |value| quantity(value, true))?,
        memory: resources
            .get("memory")
            .map_or(Ok(0), |value| quantity(value, false))?,
    })
}

fn container_requests(container: &Value) -> Result<Requests> {
    if !container.is_object() {
        bail!("malformed pod container");
    }
    let Some(resources) = container.get("resources").filter(|value| !value.is_null()) else {
        return Ok(Requests::default());
    };
    if !resources.is_object() {
        bail!("malformed container resources");
    }
    for key in ["requests", "limits"] {
        if resources
            .get(key)
            .is_some_and(|value| !value.is_null() && !value.is_object())
        {
            bail!("malformed container resource requests or limits");
        }
    }
    let request = |name, cpu| {
        resources["requests"]
            .get(name)
            .or_else(|| resources["limits"].get(name))
            .map_or(Ok(0), |value| quantity(value, cpu))
    };
    Ok(Requests {
        cpu: request("cpu", true)?,
        memory: request("memory", false)?,
    })
}

fn optional_array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(&[]),
        Some(Value::Array(items)) => Ok(items),
        _ => bail!("malformed resource list"),
    }
}

fn pod_requests(spec: &Value) -> Result<Requests> {
    let containers = spec["containers"]
        .as_array()
        .filter(|containers| !containers.is_empty())
        .context("malformed pod container list")?;
    let mut steady = Requests::default();
    for container in containers {
        steady = steady.add(container_requests(container)?)?;
    }
    let mut sidecars = Requests::default();
    let mut init_max = Requests::default();
    for container in optional_array(spec, "initContainers")? {
        let requests = container_requests(container)?;
        // Restartable init containers remain running in later init stages.
        // https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/#resource-sharing-within-containers
        if container["restartPolicy"].as_str() == Some("Always") {
            sidecars = sidecars.add(requests)?;
            init_max = init_max.max(sidecars);
        } else {
            init_max = init_max.max(sidecars.add(requests)?);
        }
    }
    let requests = steady.add(sidecars)?.max(init_max);
    match spec.get("overhead").filter(|value| !value.is_null()) {
        Some(overhead) => requests.add(resource_requests(overhead)?),
        None => Ok(requests),
    }
}

fn replicas(spec: &Value, key: &str) -> Result<u128> {
    match spec.get(key) {
        None => Ok(1),
        Some(value) => value
            .as_u64()
            .map(u128::from)
            .context("malformed workload replica count"),
    }
}

fn namespace_of<'a>(document: &'a Value, namespace: &'a str) -> &'a str {
    document["metadata"]["namespace"]
        .as_str()
        .unwrap_or(namespace)
}

fn rendered_requests(
    rendered: &str,
    eligible_nodes: usize,
    upgrade: bool,
    namespace: &str,
) -> Result<Requests> {
    let mut documents = Vec::new();
    for document in serde_norway::Deserializer::from_str(rendered) {
        let value = Value::deserialize(document).map_err(|_| anyhow!("malformed Helm render"))?;
        if value.is_null() {
            continue;
        }
        if !value.is_object() {
            bail!("malformed Helm resource");
        }
        documents.push(value);
    }
    let mut templates = BTreeMap::new();
    for document in &documents {
        if document["kind"].as_str() == Some("SandboxTemplate") {
            let name = document["metadata"]["name"]
                .as_str()
                .context("malformed SandboxTemplate identity")?;
            let key = (namespace_of(document, namespace), name);
            if templates.insert(key, document).is_some() {
                bail!("duplicate SandboxTemplate identity");
            }
        }
    }
    let phases = if upgrade {
        ["pre-upgrade", "post-upgrade"]
    } else {
        ["pre-install", "post-install"]
    };
    let mut total = Requests::default();
    let mut hook_max = Requests::default();
    for document in &documents {
        let spec = &document["spec"];
        let requests = match document["kind"].as_str() {
            Some("Deployment" | "StatefulSet") => {
                pod_requests(&spec["template"]["spec"])?.multiply(replicas(spec, "replicas")?)?
            }
            Some("DaemonSet") => {
                pod_requests(&spec["template"]["spec"])?.multiply(eligible_nodes as u128)?
            }
            Some("Job") => {
                if let Some(hook) = document["metadata"]["annotations"].get("helm.sh/hook") {
                    let hook = hook.as_str().context("malformed Helm hook annotation")?;
                    if hook.split(',').any(|phase| phases.contains(&phase.trim())) {
                        hook_max = hook_max.max(pod_requests(&spec["template"]["spec"])?);
                    }
                    continue;
                }
                pod_requests(&spec["template"]["spec"])?.multiply(replicas(spec, "parallelism")?)?
            }
            Some("Pod") => {
                if document["metadata"]["annotations"]
                    .get("helm.sh/hook")
                    .is_some()
                {
                    continue;
                }
                pod_requests(spec)?.multiply(replicas(spec, "parallelism")?)?
            }
            Some("SandboxWarmPool") => {
                let name = spec["sandboxTemplateRef"]["name"]
                    .as_str()
                    .context("malformed SandboxWarmPool template reference")?;
                let template = templates
                    .get(&(namespace_of(document, namespace), name))
                    .context("SandboxWarmPool template is missing from the render")?;
                pod_requests(&template["spec"]["podTemplate"]["spec"])?
                    .multiply(replicas(spec, "replicas")?)?
            }
            _ => continue,
        };
        total = total.add(requests)?;
    }
    total.add(hook_max)
}

fn calculate(
    rendered: &str,
    nodes: &Value,
    pods: &Value,
    upgrade: bool,
    namespace: &str,
    release: &str,
) -> Result<Report> {
    let nodes = nodes["items"].as_array().context("malformed node list")?;
    let pods = pods["items"].as_array().context("malformed pod list")?;
    let mut eligible = BTreeSet::new();
    let mut allocatable = Requests::default();
    let mut gke = false;
    for node in nodes {
        gke |= node["metadata"]["labels"]
            .get("cloud.google.com/gke-nodepool")
            .is_some();
        let ready = optional_array(&node["status"], "conditions")?
            .iter()
            .any(|condition| {
                condition["type"].as_str() == Some("Ready")
                    && condition["status"].as_str() == Some("True")
            });
        let hard_taint = optional_array(&node["spec"], "taints")?
            .iter()
            .any(|taint| {
                matches!(taint["effect"].as_str(), Some("NoSchedule" | "NoExecute"))
                    && !is_transient_condition_taint(taint["key"].as_str().unwrap_or(""))
            });
        if !ready || node["spec"]["unschedulable"].as_bool() == Some(true) || hard_taint {
            continue;
        }
        let name = node["metadata"]["name"]
            .as_str()
            .context("malformed node identity")?;
        if !eligible.insert(name) {
            bail!("duplicate node identity");
        }
        let resources = &node["status"]["allocatable"];
        if resources.get("cpu").is_none() || resources.get("memory").is_none() {
            bail!("eligible node allocatable CPU or memory is missing");
        }
        allocatable = allocatable.add(resource_requests(resources)?)?;
    }
    if eligible.is_empty() {
        bail!("no eligible nodes");
    }
    let mut existing = Requests::default();
    for pod in pods {
        if matches!(
            pod["status"]["phase"].as_str(),
            Some("Succeeded" | "Failed")
        ) {
            continue;
        }
        if !pod["spec"]["nodeName"]
            .as_str()
            .is_some_and(|name| eligible.contains(name))
        {
            continue;
        }
        if pod["metadata"]["namespace"].as_str() == Some(namespace)
            && pod["metadata"]["labels"]["app.kubernetes.io/instance"].as_str() == Some(release)
        {
            continue;
        }
        existing = existing.add(pod_requests(&pod["spec"])?)?;
    }
    let install = rendered_requests(rendered, eligible.len(), upgrade, namespace)?;
    let projected = existing.add(install)?;
    Ok(Report {
        install,
        allocatable,
        existing,
        eligible_nodes: eligible.len(),
        gke,
        cpu: Projection::new(projected.cpu, allocatable.cpu)?,
        memory: Projection::new(projected.memory, allocatable.memory)?,
    })
}

async fn read_cluster(args: Vec<super::CmdArg>, context: &str, resource: &str) -> Result<Value> {
    let mut args = args;
    args.push(plain("--context"));
    args.push(plain(context));
    let command = OpsCommand::new("kubectl", args);
    let capture = tokio::time::timeout(Duration::from_secs(20), run_capture(&command))
        .await
        .map_err(|_| anyhow!("{resource} read exceeded 20 seconds"))?
        .map_err(|_| anyhow!("could not read {resource}"))?;
    let (ok, output, stderr) = capture;
    if !ok {
        if stderr.contains("Forbidden") {
            bail!("Forbidden while listing {resource}");
        }
        bail!("could not list {resource}");
    }
    serde_json::from_str(&output).map_err(|_| anyhow!("malformed {resource} JSON"))
}

pub(super) async fn preflight(render: OpsCommand, common: &CommonOpts, upgrade: bool, ui: &Ui) {
    let result = async {
        let (ok, rendered, _) = tokio::time::timeout(Duration::from_secs(20), run_capture(&render))
            .await
            .map_err(|_| anyhow!("chart render exceeded 20 seconds"))?
            .map_err(|_| anyhow!("could not render chart requests"))?;
        if !ok {
            bail!("could not render chart requests");
        }
        let context = std::env::var("HELM_KUBECONTEXT")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(crate::kube_context::current_context_name)
            .context("no pinned Kubernetes context")?;
        let nodes = read_cluster(
            vec![plain("get"), plain("nodes"), plain("-o"), plain("json")],
            &context,
            "nodes",
        )
        .await?;
        let pods = read_cluster(
            vec![
                plain("get"),
                plain("pods"),
                plain("--all-namespaces"),
                plain("--field-selector=status.phase!=Succeeded,status.phase!=Failed"),
                plain("-o"),
                plain("json"),
            ],
            &context,
            "pods",
        )
        .await?;
        calculate(
            &rendered,
            &nodes,
            &pods,
            upgrade,
            &common.namespace,
            &common.release,
        )
    }
    .await;
    match result {
        Ok(report) => {
            let (note, warnings, recommendation) = report.messages();
            ui.note(&note);
            for warning in warnings {
                ui.warn(&warning);
            }
            if let Some(recommendation) = recommendation {
                ui.note(recommendation);
            }
        }
        Err(error) => ui.note(&format!("capacity preflight skipped: {error}")),
    }
}

fn parse_cpu_quantity(quantity: &str) -> Result<u128> {
    let quantity = quantity.trim();
    let (number, precision, scale) = match quantity.strip_suffix('m') {
        Some(number) => (number, 0, 1),
        None => (quantity, 3, 1000),
    };
    let number = number.strip_prefix('+').unwrap_or(number);
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole.bytes().all(|digit| digit.is_ascii_digit())
        || !fraction.bytes().all(|digit| digit.is_ascii_digit())
    {
        bail!("unsupported Kubernetes CPU quantity");
    }
    let mut millicores = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<u128>()
            .map_err(|_| anyhow!("CPU quantity overflowed"))?
    }
    .checked_mul(scale)
    .context("CPU quantity overflowed")?;
    let mut decimal = 0u128;
    for offset in 0..precision {
        decimal = decimal * 10
            + fraction
                .as_bytes()
                .get(offset)
                .map_or(0, |digit| u128::from(digit - b'0'));
    }
    millicores = millicores
        .checked_add(decimal)
        .context("CPU quantity overflowed")?;
    if fraction.bytes().skip(precision).any(|digit| digit != b'0') {
        millicores = millicores
            .checked_add(1)
            .context("CPU quantity overflowed")?;
    }
    Ok(millicores)
}

pub(crate) fn is_transient_condition_taint(key: &str) -> bool {
    matches!(
        key,
        "node.kubernetes.io/not-ready"
            | "node.kubernetes.io/unreachable"
            | "node.kubernetes.io/network-unavailable"
            | "node.cloudprovider.kubernetes.io/uninitialized"
    )
}

pub(crate) fn parse_memory_quantity(quantity: &str) -> Result<u128> {
    let quantity = quantity.trim();
    let (number, multiplier) = [
        ("Ei", 1024f64.powi(6)),
        ("Pi", 1024f64.powi(5)),
        ("Ti", 1024f64.powi(4)),
        ("Gi", 1024f64.powi(3)),
        ("Mi", 1024f64.powi(2)),
        ("Ki", 1024f64),
        ("E", 1000f64.powi(6)),
        ("P", 1000f64.powi(5)),
        ("T", 1000f64.powi(4)),
        ("G", 1000f64.powi(3)),
        ("M", 1000f64.powi(2)),
        ("K", 1000f64),
        ("k", 1000f64),
        ("m", 0.001f64),
        ("u", 0.000_001f64),
        ("n", 0.000_000_001f64),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        quantity
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .unwrap_or((quantity, 1f64));
    let number = number
        .parse::<f64>()
        .with_context(|| format!("unsupported Kubernetes memory quantity {quantity:?}"))?;
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes < 0.0 || bytes > u128::MAX as f64 {
        bail!("unsupported Kubernetes memory quantity {quantity:?}");
    }
    Ok(bytes.ceil() as u128)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const MIB: u128 = 1024 * 1024;
    const NAMESPACE: &str = "acme-space";
    const RELEASE: &str = "acme-release";

    fn node(name: &str, cpu: &str, memory: &str) -> Value {
        // Kubernetes exposes allocatable separately from capacity and names the
        // scheduling condition Ready. Use allocatable for the denominator.
        // https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/
        json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name},
            "spec": {},
            "status": {
                "capacity": {"cpu": "100", "memory": "100Gi"},
                "allocatable": {"cpu": cpu, "memory": memory},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        })
    }

    fn two_nodes() -> Value {
        json!({"items": [node("node-a", "2", "4Gi"), node("node-b", "2", "4Gi")]})
    }

    fn container(name: &str, cpu: &str, memory: &str) -> Value {
        json!({
            "name": name, "image": "example.com/acme:test",
            "resources": {"requests": {"cpu": cpu, "memory": memory}}
        })
    }

    fn pod_spec(cpu: &str, memory: &str) -> Value {
        json!({"containers": [container("app", cpu, memory)]})
    }

    fn pod_document(name: &str, spec: Value) -> Value {
        json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name}, "spec": spec})
    }

    fn workload(kind: &str, name: &str, spec: Value) -> Value {
        json!({
            "apiVersion": "apps/v1", "kind": kind,
            "metadata": {"name": name},
            "spec": {"template": {"spec": spec}}
        })
    }

    fn job(name: &str, cpu: &str, memory: &str, hook: Option<&str>) -> Value {
        let mut document = workload("Job", name, pod_spec(cpu, memory));
        document["apiVersion"] = json!("batch/v1");
        if let Some(hook) = hook {
            document["metadata"]["annotations"] = json!({"helm.sh/hook": hook});
        }
        document
    }

    fn render(documents: &[Value]) -> String {
        // JSON is valid YAML. Separators exercise the same multi-document
        // boundary as helm template without writing a fixture to disk.
        documents
            .iter()
            .map(|document| serde_json::to_string(document).unwrap())
            .collect::<Vec<_>>()
            .join("\n---\n")
    }

    fn no_pods() -> Value {
        json!({"items": []})
    }

    fn calculate_documents(documents: &[Value], upgrade: bool) -> Report {
        calculate(
            &render(documents),
            &two_nodes(),
            &no_pods(),
            upgrade,
            NAMESPACE,
            RELEASE,
        )
        .expect("valid capacity fixture")
    }

    fn complete_render() -> String {
        let mut deployment = workload("Deployment", "acme-app", pod_spec("150m", "128Mi"));
        deployment["spec"]["replicas"] = json!(2);
        let statefulset = workload("StatefulSet", "acme-state", pod_spec("200m", "256Mi"));
        let daemonset = workload("DaemonSet", "acme-prewarm", pod_spec("30m", "48Mi"));
        let mut sidecar = container("sidecar", "50m", "32Mi");
        sidecar["restartPolicy"] = json!("Always");
        let mut initialized = pod_spec("100m", "128Mi");
        initialized["initContainers"] = json!([sidecar, container("initialize", "300m", "256Mi")]);
        initialized["overhead"] = json!({"cpu": "10m", "memory": "16Mi"});
        let limits_only = pod_document(
            "acme-limits",
            json!({"containers": [{
                "name": "app", "image": "example.com/acme:test",
                "resources": {"limits": {"cpu": "250m", "memory": "64Mi"}}
            }]}),
        );
        let mut ordinary_job = job("acme-worker-job", "40m", "16Mi", None);
        ordinary_job["spec"]["parallelism"] = json!(3);
        render(&[
            deployment,
            statefulset,
            daemonset,
            pod_document("acme-initialized", initialized),
            limits_only,
            ordinary_job,
            job("acme-pre-install", "400m", "100Mi", Some("pre-install")),
            job("acme-post-install", "100m", "500Mi", Some("post-install")),
            job("acme-pre-upgrade", "800m", "200Mi", Some("pre-upgrade")),
            job("acme-post-upgrade", "200m", "700Mi", Some("post-upgrade")),
            job(
                "acme-both-phases",
                "50m",
                "32Mi",
                Some("pre-install, pre-upgrade"),
            ),
            job("acme-test", "90", "90Gi", Some("test")),
            json!({
                "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
                "kind": "SandboxWarmPool", "metadata": {"name": "acme-pool"},
                "spec": {"replicas": 2, "sandboxTemplateRef": {"name": "acme-sandbox"}}
            }),
            json!({
                "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
                "kind": "SandboxTemplate", "metadata": {"name": "acme-sandbox"},
                "spec": {"podTemplate": {"spec": pod_spec("80m", "96Mi")}}
            }),
            json!({
                "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "acme-config"},
                "data": {"cpu": "not-a-quantity", "memory": "not-a-quantity"}
            }),
        ])
    }

    #[test]
    fn complete_render_counts_install_workloads_and_per_resource_hook_maximum() {
        let report = calculate(
            &complete_render(),
            &two_nodes(),
            &no_pods(),
            false,
            NAMESPACE,
            RELEASE,
        )
        .unwrap();
        // Ordinary workloads: 1450m and 1216Mi. Concurrent hook reserve:
        // pre-install supplies the CPU maximum; post-install supplies memory.
        assert_eq!(report.install.cpu, 1850);
        assert_eq!(report.install.memory, 1716 * MIB);
        assert_eq!(report.eligible_nodes, 2);
        assert_eq!(report.allocatable.cpu, 4000);
        assert_eq!(report.allocatable.memory, 8 * 1024 * MIB);
        assert_eq!(report.existing.cpu, 0);
        assert_eq!(report.existing.memory, 0);
    }

    #[test]
    fn complete_render_uses_upgrade_hooks_and_excludes_install_and_test_hooks() {
        let report = calculate(
            &complete_render(),
            &two_nodes(),
            &no_pods(),
            true,
            NAMESPACE,
            RELEASE,
        )
        .unwrap();
        assert_eq!(report.install.cpu, 2250);
        assert_eq!(report.install.memory, 1916 * MIB);
    }

    #[test]
    fn init_stages_add_only_sidecars_already_started_and_take_resource_maxima() {
        // Restartable init containers remain live through subsequent stages.
        // https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/#resource-sharing-within-containers
        // Stage order is specified by AggregateContainerRequests:
        // https://github.com/kubernetes/kubernetes/blob/v1.34.0/staging/src/k8s.io/component-helpers/resource/helpers.go
        let mut first_sidecar = container("first-sidecar", "50m", "80Mi");
        first_sidecar["restartPolicy"] = json!("Always");
        let mut second_sidecar = container("second-sidecar", "20m", "30Mi");
        second_sidecar["restartPolicy"] = json!("Always");
        let mut spec = pod_spec("100m", "100Mi");
        spec["initContainers"] = json!([
            first_sidecar,
            container("cpu-initializer", "300m", "20Mi"),
            second_sidecar,
            container("memory-initializer", "90m", "300Mi")
        ]);
        // Overhead is added after comparing steady-state and init requests.
        // https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/
        spec["overhead"] = json!({"cpu": "10m", "memory": "16Mi"});
        let report = calculate_documents(&[pod_document("acme-stages", spec)], false);
        // CPU stage: 50+300, not 50+300+20. Memory stage: 80+30+300.
        assert_eq!(report.install.cpu, 360);
        assert_eq!(report.install.memory, 426 * MIB);
    }

    #[test]
    fn app_container_sum_can_exceed_every_init_stage() {
        let mut sidecar = container("sidecar", "50m", "32Mi");
        sidecar["restartPolicy"] = json!("Always");
        let report = calculate_documents(
            &[pod_document(
                "acme-app-sum",
                json!({
                    "containers": [container("first", "200m", "128Mi"), container("second", "300m", "256Mi")],
                    "initContainers": [container("classic", "100m", "16Mi"), sidecar]
                }),
            )],
            false,
        );
        assert_eq!(report.install.cpu, 550);
        assert_eq!(report.install.memory, 416 * MIB);
    }

    #[test]
    fn limits_default_missing_requests_per_resource_but_preserve_explicit_zero() {
        // API defaulting copies a limit only when that resource has no request.
        // https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits
        let report = calculate_documents(
            &[pod_document(
                "acme-resource-defaults",
                json!({"containers": [
                    {"name": "cpu-request", "resources": {
                        "requests": {"cpu": "100m"}, "limits": {"cpu": "900m", "memory": "64Mi"}
                    }},
                    {"name": "memory-request", "resources": {
                        "requests": {"memory": "32Mi"}, "limits": {"cpu": "200m", "memory": "128Mi"}
                    }},
                    {"name": "zero-request", "resources": {
                        "requests": {"cpu": "0", "memory": "0"}, "limits": {"cpu": "1", "memory": "1Gi"}
                    }},
                    {"name": "best-effort"}
                ]}),
            )],
            false,
        );
        assert_eq!(report.install.cpu, 300);
        assert_eq!(report.install.memory, 96 * MIB);
    }

    #[test]
    fn replica_and_parallelism_defaults_and_explicit_zero_are_respected() {
        let mut zero_deployment = workload("Deployment", "acme-zero", pod_spec("10", "10Gi"));
        zero_deployment["spec"]["replicas"] = json!(0);
        let mut zero_job = job("acme-zero-job", "10", "10Gi", None);
        zero_job["spec"]["parallelism"] = json!(0);
        let report = calculate_documents(
            &[
                workload("Deployment", "acme-default", pod_spec("10m", "10Mi")),
                workload("StatefulSet", "acme-state-default", pod_spec("20m", "20Mi")),
                job("acme-job-default", "30m", "30Mi", None),
                pod_document("acme-pod-default", pod_spec("40m", "40Mi")),
                zero_deployment,
                zero_job,
            ],
            false,
        );
        assert_eq!(report.install.cpu, 100);
        assert_eq!(report.install.memory, 100 * MIB);
    }

    #[test]
    fn combined_hook_annotation_counts_a_job_once_and_nonmatching_hooks_count_nothing() {
        for upgrade in [false, true] {
            let report = calculate_documents(
                &[
                    job(
                        "acme-multiple-hooks",
                        "123m",
                        "45Mi",
                        Some("pre-install, post-install, pre-upgrade, post-upgrade, test"),
                    ),
                    job("acme-delete", "90", "90Gi", Some("pre-delete, post-delete")),
                    job(
                        "acme-test-only",
                        "not-a-quantity",
                        "not-a-quantity",
                        Some("test"),
                    ),
                ],
                upgrade,
            );
            assert_eq!(report.install.cpu, 123);
            assert_eq!(report.install.memory, 45 * MIB);
        }
    }

    #[test]
    fn warm_pool_resolves_a_template_before_or_after_it_and_ignores_unused_templates() {
        let template = json!({
            "apiVersion": "extensions.agents.x-k8s.io/v1alpha1", "kind": "SandboxTemplate",
            "metadata": {"name": "acme-template"},
            "spec": {"podTemplate": {"spec": pod_spec("125m", "48Mi")}}
        });
        let pool = json!({
            "apiVersion": "extensions.agents.x-k8s.io/v1alpha1", "kind": "SandboxWarmPool",
            "metadata": {"name": "acme-pool"},
            "spec": {"replicas": 2, "sandboxTemplateRef": {"name": "acme-template"}}
        });
        let unused_template = json!({
            "kind": "SandboxTemplate", "metadata": {"name": "acme-unused"},
            "spec": {"podTemplate": {"spec": pod_spec("not-a-quantity", "not-a-quantity")}}
        });
        for documents in [
            vec![pool.clone(), template.clone(), unused_template.clone()],
            vec![template.clone(), pool.clone(), unused_template],
        ] {
            let report = calculate_documents(&documents, false);
            assert_eq!(report.install.cpu, 250);
            assert_eq!(report.install.memory, 96 * MIB);
        }
    }

    fn existing_pod(
        name: &str,
        node_name: Option<&str>,
        namespace: &str,
        release: &str,
        phase: &str,
        requests: (&str, &str),
    ) -> Value {
        let mut spec = pod_spec(requests.0, requests.1);
        if let Some(node_name) = node_name {
            spec["nodeName"] = json!(node_name);
        }
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": namespace,
                "labels": {"app.kubernetes.io/instance": release}},
            "spec": spec, "status": {"phase": phase}
        })
    }

    #[test]
    fn eligible_nodes_and_bound_active_pods_share_the_same_node_set() {
        let ready = node("node-ready", "1", "2Gi");
        let mut not_ready = node("node-not-ready", "50", "50Gi");
        not_ready["status"]["conditions"][0]["status"] = json!("False");
        let mut cordoned = node("node-cordoned", "50", "50Gi");
        cordoned["spec"]["unschedulable"] = json!(true);
        let mut tainted = node("node-tainted", "50", "50Gi");
        tainted["spec"]["taints"] = json!([{"key": "dedicated", "effect": "NoSchedule"}]);
        let mut transient = node("node-transient", "1", "2Gi");
        transient["spec"]["taints"] =
            json!([{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}]);
        let nodes = json!({"items": [ready, not_ready, cordoned, tainted, transient]});
        let pods = json!({"items": [
            existing_pod("acme-system", Some("node-ready"), "kube-system", "system", "Running", ("100m", "64Mi")),
            existing_pod("acme-other-release", Some("node-transient"), NAMESPACE, "acme-other", "Running", ("200m", "128Mi")),
            existing_pod("acme-current-release", Some("node-ready"), NAMESPACE, RELEASE, "Running", ("900m", "900Mi")),
            existing_pod("acme-other-namespace", Some("node-ready"), "acme-other-space", RELEASE, "Running", ("50m", "32Mi")),
            existing_pod("acme-completed", Some("node-ready"), NAMESPACE, "acme-other", "Succeeded", ("900m", "900Mi")),
            existing_pod("acme-failed", Some("node-ready"), NAMESPACE, "acme-other", "Failed", ("900m", "900Mi")),
            existing_pod("acme-not-ready", Some("node-not-ready"), NAMESPACE, "acme-other", "Running", ("900m", "900Mi")),
            existing_pod("acme-cordoned", Some("node-cordoned"), NAMESPACE, "acme-other", "Running", ("900m", "900Mi")),
            existing_pod("acme-tainted", Some("node-tainted"), NAMESPACE, "acme-other", "Running", ("900m", "900Mi")),
            existing_pod("acme-unbound", None, NAMESPACE, "acme-other", "Pending", ("900m", "900Mi")),
            existing_pod("acme-unknown-node", Some("node-unknown"), NAMESPACE, "acme-other", "Running", ("900m", "900Mi")),
            existing_pod("acme-bound-pending", Some("node-transient"), NAMESPACE, "acme-other", "Pending", ("40m", "16Mi"))
        ]});
        for upgrade in [false, true] {
            let report = calculate("", &nodes, &pods, upgrade, NAMESPACE, RELEASE).unwrap();
            assert_eq!(report.eligible_nodes, 2);
            assert_eq!(report.allocatable.cpu, 2000);
            assert_eq!(report.allocatable.memory, 4 * 1024 * MIB);
            assert_eq!(report.existing.cpu, 390);
            assert_eq!(report.existing.memory, 240 * MIB);
        }
    }

    #[test]
    fn permanent_taints_exclude_nodes_but_prefer_no_schedule_and_transient_taints_do_not() {
        // NoSchedule and NoExecute are hard effects; PreferNoSchedule is soft.
        // https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
        for (key, effect, eligible) in [
            ("dedicated", "NoSchedule", false),
            ("dedicated", "NoExecute", false),
            ("dedicated", "PreferNoSchedule", true),
            ("node.kubernetes.io/memory-pressure", "NoSchedule", false),
            ("node.kubernetes.io/not-ready", "NoSchedule", true),
            ("node.kubernetes.io/unreachable", "NoExecute", true),
            ("node.kubernetes.io/network-unavailable", "NoSchedule", true),
            (
                "node.cloudprovider.kubernetes.io/uninitialized",
                "NoSchedule",
                true,
            ),
        ] {
            let mut candidate = node("node-candidate", "500m", "512Mi");
            candidate["spec"]["taints"] = json!([{"key": key, "effect": effect}]);
            let nodes = json!({"items": [node("node-ready", "1", "1Gi"), candidate]});
            let report = calculate("", &nodes, &no_pods(), false, NAMESPACE, RELEASE).unwrap();
            assert_eq!(
                report.eligible_nodes,
                if eligible { 2 } else { 1 },
                "{key}/{effect}"
            );
            assert_eq!(report.allocatable.cpu, if eligible { 1500 } else { 1000 });
            assert_eq!(
                report.allocatable.memory,
                if eligible { 1536 * MIB } else { 1024 * MIB }
            );
        }
    }

    #[test]
    fn ready_condition_must_explicitly_be_true() {
        for condition in [
            json!([]),
            json!([{"type": "Ready", "status": "Unknown"}]),
            json!([{"type": "MemoryPressure", "status": "True"}]),
        ] {
            let mut candidate = node("node-candidate", "100", "100Gi");
            candidate["status"]["conditions"] = condition;
            let report = calculate(
                "",
                &json!({"items": [node("node-ready", "1", "1Gi"), candidate]}),
                &no_pods(),
                false,
                NAMESPACE,
                RELEASE,
            )
            .unwrap();
            assert_eq!(report.eligible_nodes, 1);
            assert_eq!(report.allocatable.cpu, 1000);
        }
    }

    #[test]
    fn existing_pods_use_init_sidecars_overhead_and_limits_too() {
        let mut pod = existing_pod(
            "acme-existing",
            Some("node-a"),
            NAMESPACE,
            "acme-other",
            "Running",
            ("100m", "128Mi"),
        );
        let mut sidecar = container("sidecar", "50m", "32Mi");
        sidecar["restartPolicy"] = json!("Always");
        pod["spec"]["initContainers"] = json!([sidecar, {
            "name": "classic-init", "resources": {"limits": {"cpu": "300m", "memory": "256Mi"}}
        }]);
        pod["spec"]["overhead"] = json!({"cpu": "10m", "memory": "16Mi"});
        let report = calculate(
            "",
            &two_nodes(),
            &json!({"items": [pod]}),
            false,
            NAMESPACE,
            RELEASE,
        )
        .unwrap();
        assert_eq!(report.existing.cpu, 360);
        assert_eq!(report.existing.memory, 304 * MIB);
    }

    fn percentages(message: &str) -> Vec<f64> {
        message
            .split('%')
            .filter_map(|prefix| {
                let start = prefix
                    .rfind(|character: char| !character.is_ascii_digit() && character != '.')
                    .map_or(0, |offset| offset + 1);
                prefix[start..].parse().ok()
            })
            .collect()
    }

    fn report_at(cpu: &str, memory: &str, nodes: Value, existing: Value) -> Report {
        calculate(
            &render(&[pod_document("acme-platform", pod_spec(cpu, memory))]),
            &nodes,
            &existing,
            false,
            NAMESPACE,
            RELEASE,
        )
        .unwrap()
    }

    #[test]
    fn exactly_eighty_percent_prints_note_without_warning_or_recommendation() {
        let report = report_at(
            "800m",
            "80Mi",
            json!({"items": [node("node-a", "1", "100Mi")]}),
            no_pods(),
        );
        let (note, warnings, recommendation) = report.messages();
        assert!(note.starts_with("capacity:"), "{note}");
        assert!(note.contains("800m"), "{note}");
        assert!(note.contains("80Mi"), "{note}");
        assert!(note.contains('1'), "eligible node count: {note}");
        assert_eq!(percentages(&note), vec![80.0, 80.0]);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(recommendation, None);
    }

    #[test]
    fn cpu_warning_uses_projected_existing_load_and_rounds_shortfall_up() {
        let nodes = json!({"items": [node("node-a", "1", "100Mi")]});
        let existing = json!({"items": [existing_pod("acme-system", Some("node-a"), "kube-system", "system", "Running", ("510m", "20Mi"))]});
        let report = report_at("300m", "30Mi", nodes, existing);
        let (note, warnings, recommendation) = report.messages();
        assert!(note.starts_with("capacity:"), "{note}");
        assert!(
            note.contains("300m") && note.contains("30Mi"),
            "install requests: {note}"
        );
        assert_eq!(percentages(&note), vec![81.0, 50.0]);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].starts_with("capacity:"), "{}", warnings[0]);
        assert!(
            warnings[0].to_lowercase().contains("cpu"),
            "{}",
            warnings[0]
        );
        assert!(percentages(&warnings[0]).contains(&81.0));
        for amount in ["810m", "1000m", "13m"] {
            assert!(
                warnings[0].contains(amount),
                "missing {amount}: {}",
                warnings[0]
            );
        }
        // ceil(810*5/4)-1000 = 13m, independently specified by the issue.
        assert_eq!(
            recommendation,
            Some("Add nodes or use larger nodes until allocatable grows by the shortfall.")
        );
    }

    #[test]
    fn memory_warning_rounds_additional_allocatable_up_to_whole_mib() {
        let report = report_at(
            "500m",
            "81Mi",
            json!({"items": [node("node-a", "1", "100Mi")]}),
            no_pods(),
        );
        let (note, warnings, recommendation) = report.messages();
        assert_eq!(percentages(&note), vec![50.0, 81.0]);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].starts_with("capacity:"), "{}", warnings[0]);
        assert!(
            warnings[0].to_lowercase().contains("memory"),
            "{}",
            warnings[0]
        );
        assert!(percentages(&warnings[0]).contains(&81.0));
        // Required extra allocatable is 1.25MiB, displayed as 2MiB.
        for amount in ["81Mi", "100Mi", "2Mi"] {
            assert!(
                warnings[0].contains(amount),
                "missing {amount}: {}",
                warnings[0]
            );
        }
        assert_eq!(
            recommendation,
            Some("Add nodes or use larger nodes until allocatable grows by the shortfall.")
        );
    }

    #[test]
    fn each_over_threshold_resource_warns_and_recommendation_appears_once() {
        let report = report_at(
            "900m",
            "90Mi",
            json!({"items": [node("node-a", "1", "100Mi")]}),
            no_pods(),
        );
        let (_, warnings, recommendation) = report.messages();
        assert_eq!(warnings.len(), 2);
        assert!(warnings
            .iter()
            .all(|warning| warning.starts_with("capacity:")));
        assert!(warnings
            .iter()
            .any(|warning| warning.to_lowercase().contains("cpu") && warning.contains("125m")));
        assert!(warnings
            .iter()
            .any(|warning| warning.to_lowercase().contains("memory") && warning.contains("13Mi")));
        assert_eq!(
            recommendation,
            Some("Add nodes or use larger nodes until allocatable grows by the shortfall.")
        );
    }

    #[test]
    fn quantities_just_above_eighty_percent_warn_even_when_percentages_round_down() {
        let report = report_at(
            "8001m",
            &(80 * MIB + 1).to_string(),
            json!({"items": [node("node-a", "10001m", "100Mi")]}),
            no_pods(),
        );
        let (_, warnings, recommendation) = report.messages();
        // The CPU numerator exceeds the integer threshold by one millicore.
        // Memory exceeds it by one byte. Rounded display must not decide it.
        assert_eq!(warnings.len(), 2);
        assert!(warnings
            .iter()
            .any(|warning| warning.to_lowercase().contains("cpu") && warning.contains("1m")));
        assert!(warnings
            .iter()
            .any(|warning| warning.to_lowercase().contains("memory") && warning.contains("1Mi")));
        assert_eq!(
            recommendation,
            Some("Add nodes or use larger nodes until allocatable grows by the shortfall.")
        );
    }

    #[test]
    fn any_gke_node_selects_the_gke_recommendation_even_when_that_node_is_ineligible() {
        let mut gke_node = node("node-gke", "1", "100Mi");
        gke_node["metadata"]["labels"] = json!({"cloud.google.com/gke-nodepool": "acme-pool"});
        gke_node["spec"]["unschedulable"] = json!(true);
        let nodes = json!({"items": [node("node-a", "1", "100Mi"), gke_node]});
        let report = report_at("810m", "50Mi", nodes, no_pods());
        assert!(report.gke);
        let (_, warnings, recommendation) = report.messages();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            recommendation,
            Some("On GKE, use a node pool of e2-standard-4 or larger with three nodes.")
        );
    }

    #[test]
    fn cpu_quantities_accept_millicores_and_plain_or_decimal_cores() {
        // CPU cores and milliCPU are absolute units; 0.1 equals 100m.
        // https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#cpu-resource-units
        for (quantity, expected) in [
            ("100m", 100),
            ("0.5", 500),
            ("2", 2000),
            ("1500m", 1500),
            ("1.125", 1125),
            ("0", 0),
        ] {
            assert_eq!(
                parse_cpu_quantity(quantity).unwrap(),
                expected,
                "{quantity}"
            );
        }
    }

    #[test]
    fn memory_quantities_accept_binary_decimal_and_exponent_forms() {
        // Memory quantities are bytes; decimal M and binary Mi differ.
        // https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#memory-resource-units
        for (quantity, expected) in [
            ("128Mi", 128 * MIB),
            ("1Gi", 1024 * MIB),
            ("1e9", 1_000_000_000),
            ("512M", 512_000_000),
            ("1.5Gi", 1536 * MIB),
            ("129e6", 129_000_000),
            ("400m", 1),
            ("0", 0),
        ] {
            assert_eq!(
                parse_memory_quantity(quantity).unwrap(),
                expected,
                "{quantity}"
            );
        }
    }

    #[test]
    fn malformed_negative_nonfinite_and_overflowing_quantities_are_errors() {
        for quantity in ["", "bogus", "-1", "NaN", "inf", "1e999", "128Mi"] {
            assert!(parse_cpu_quantity(quantity).is_err(), "CPU {quantity:?}");
        }
        for quantity in ["", "bogus", "-1", "NaN", "inf", "1e999", "128MiB"] {
            assert!(
                parse_memory_quantity(quantity).is_err(),
                "memory {quantity:?}"
            );
        }
    }

    #[test]
    fn malformed_quantity_returns_a_reason_for_the_wrappers_skip_note() {
        // calculate returns an error; the advisory wrapper turns it into exactly
        // one capacity preflight skipped note, covered by the binary tests.
        for (cpu, memory, resource) in [
            ("not-a-quantity", "128Mi", "cpu"),
            ("100m", "not-a-quantity", "memory"),
        ] {
            let error = calculate(
                &render(&[pod_document("acme-invalid", pod_spec(cpu, memory))]),
                &two_nodes(),
                &no_pods(),
                false,
                NAMESPACE,
                RELEASE,
            )
            .err()
            .expect("invalid quantity must skip the capacity calculation");
            let reason = format!("{error:#}").to_lowercase();
            assert!(reason.contains(resource), "{reason}");
            assert!(
                reason.contains("quantity") || reason.contains("request"),
                "{reason}"
            );
        }
    }

    #[test]
    fn malformed_render_list_shapes_and_zero_eligible_nodes_are_errors() {
        assert!(calculate(
            "kind: [",
            &two_nodes(),
            &no_pods(),
            false,
            NAMESPACE,
            RELEASE
        )
        .is_err());
        for invalid_list in [json!(null), json!({}), json!({"items": {}})] {
            assert!(calculate("", &invalid_list, &no_pods(), false, NAMESPACE, RELEASE).is_err());
            assert!(calculate("", &two_nodes(), &invalid_list, false, NAMESPACE, RELEASE).is_err());
        }
        let mut cordoned = node("node-cordoned", "1", "1Gi");
        cordoned["spec"]["unschedulable"] = json!(true);
        for nodes in [json!({"items": []}), json!({"items": [cordoned]})] {
            let error = calculate("", &nodes, &no_pods(), false, NAMESPACE, RELEASE)
                .err()
                .expect("zero eligible nodes must skip");
            assert!(format!("{error:#}").contains("node"));
        }
    }

    #[test]
    fn invalid_quantities_in_eligible_nodes_and_counted_pods_are_errors() {
        for (cpu, memory) in [("invalid", "1Gi"), ("1", "invalid")] {
            assert!(calculate(
                "",
                &json!({"items": [node("node-a", cpu, memory)]}),
                &no_pods(),
                false,
                NAMESPACE,
                RELEASE
            )
            .is_err());
            let pods = json!({"items": [existing_pod("acme-invalid", Some("node-a"), NAMESPACE, "acme-other", "Running", (cpu, memory))]});
            assert!(calculate("", &two_nodes(), &pods, false, NAMESPACE, RELEASE).is_err());
        }
    }

    #[test]
    fn excluded_pods_do_not_make_the_calculation_fail_on_their_quantities() {
        let pods = json!({"items": [
            existing_pod("acme-release", Some("node-a"), NAMESPACE, RELEASE, "Running", ("invalid", "invalid")),
            existing_pod("acme-completed", Some("node-a"), NAMESPACE, "acme-other", "Succeeded", ("invalid", "invalid")),
            existing_pod("acme-unbound", None, NAMESPACE, "acme-other", "Pending", ("invalid", "invalid"))
        ]});
        let report = calculate("", &two_nodes(), &pods, true, NAMESPACE, RELEASE).unwrap();
        assert_eq!(report.existing.cpu, 0);
        assert_eq!(report.existing.memory, 0);
    }

    #[test]
    fn missing_warm_pool_template_is_an_error_instead_of_an_undercount() {
        let pool = json!({
            "kind": "SandboxWarmPool", "metadata": {"name": "acme-pool"},
            "spec": {"replicas": 2, "sandboxTemplateRef": {"name": "acme-missing"}}
        });
        assert!(calculate(
            &render(&[pool]),
            &two_nodes(),
            &no_pods(),
            false,
            NAMESPACE,
            RELEASE
        )
        .is_err());
    }
}

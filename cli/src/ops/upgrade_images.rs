//! Observe the actual containers selected by an upgraded workload. Image IDs
//! are runtime evidence, not registry manifest/digest equivalence assertions.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedImage {
    pub workload: String,
    pub pod: String,
    pub container: String,
    pub image: String,
    pub image_id: String,
}

use super::convergence::{needs_node_identity, normalize_image, observed_image_matches};
use std::collections::{BTreeMap, BTreeSet};

fn selected(expected: &Value, pod: &Value) -> bool {
    expected
        .pointer("/spec/selector/matchLabels")
        .and_then(Value::as_object)
        .filter(|selector| !selector.is_empty())
        .is_some_and(|selector| {
            pod.pointer("/metadata/labels")
                .and_then(Value::as_object)
                .is_some_and(|labels| {
                    selector
                        .iter()
                        .all(|(key, value)| labels.get(key) == Some(value))
                })
        })
}

pub(super) fn required_nodes(objects: &[Value], pods: &[Value]) -> BTreeSet<String> {
    let mut nodes = BTreeSet::new();
    for expected in objects {
        for pod in pods.iter().filter(|pod| selected(expected, pod)) {
            for (field, status_field) in [
                ("containers", "containerStatuses"),
                ("initContainers", "initContainerStatuses"),
            ] {
                for container in expected
                    .pointer(&format!("/spec/template/spec/{field}"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let status = pod
                        .pointer(&format!("/status/{status_field}"))
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .find(|status| status.get("name") == container.get("name"));
                    if status.is_some_and(|status| {
                        needs_node_identity(
                            container.get("image").and_then(Value::as_str).unwrap_or(""),
                            status,
                        )
                    }) {
                        if let Some(node) = pod
                            .pointer("/spec/nodeName")
                            .and_then(Value::as_str)
                            .filter(|node| !node.is_empty())
                        {
                            nodes.insert(node.to_owned());
                        }
                    }
                }
            }
        }
    }
    nodes
}

pub(super) fn observe(
    expected: &Value,
    pods: &[Value],
    desired: Option<u64>,
    nodes: &BTreeMap<String, Value>,
) -> (bool, Vec<ObservedImage>) {
    let mut observations = Vec::new();
    if expected
        .pointer("/spec/selector/matchLabels")
        .and_then(Value::as_object)
        .is_none_or(|selector| selector.is_empty())
    {
        return (false, observations);
    }
    let selected: Vec<_> = pods.iter().filter(|pod| selected(expected, pod)).collect();
    let mut exact = desired.is_some_and(|desired| selected.len() as u64 == desired);
    for pod in selected {
        exact &= pod.pointer("/status/phase").and_then(Value::as_str) == Some("Running")
            && pod
                .pointer("/metadata/deletionTimestamp")
                .is_none_or(Value::is_null);
        for (field, status_field, must_run) in [
            ("containers", "containerStatuses", true),
            ("initContainers", "initContainerStatuses", false),
        ] {
            let expected_containers = expected
                .pointer(&format!("/spec/template/spec/{field}"))
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let actual_containers = pod
                .pointer(&format!("/spec/{field}"))
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let statuses = pod
                .pointer(&format!("/status/{status_field}"))
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            exact &= actual_containers.len() == expected_containers.len();
            for container in expected_containers {
                let Some(name) = container.get("name").and_then(Value::as_str) else {
                    exact = false;
                    continue;
                };
                let wanted = container.get("image").and_then(Value::as_str).unwrap_or("");
                let actual = actual_containers
                    .iter()
                    .find(|item| item.get("name").and_then(Value::as_str) == Some(name));
                let status = statuses
                    .iter()
                    .find(|item| item.get("name").and_then(Value::as_str) == Some(name));
                // Kubernetes documents image and imageID as runtime observations:
                // https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#ContainerStatus
                // Runtime-resolved names may include the default Docker registry.
                let observed = status
                    .and_then(|item| item.get("image"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let image_id = status
                    .and_then(|item| item.get("imageID"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                exact &= !wanted.is_empty()
                    && !observed.is_empty()
                    && !image_id.trim().is_empty()
                    && actual
                        .and_then(|item| item.get("image"))
                        .and_then(Value::as_str)
                        .is_some_and(|image| normalize_image(image) == normalize_image(wanted))
                    && status.is_some_and(|status| {
                        observed_image_matches(
                            wanted,
                            status,
                            nodes.get(
                                pod.pointer("/spec/nodeName")
                                    .and_then(Value::as_str)
                                    .unwrap_or(""),
                            ),
                        )
                    })
                    && (!must_run
                        || status.is_some_and(|status| {
                            status.get("ready").and_then(Value::as_bool) == Some(true)
                                && status.pointer("/state/running").is_some()
                        }));
                observations.push(ObservedImage {
                    workload: expected
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    pod: pod
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    container: name.to_owned(),
                    image: observed.to_owned(),
                    image_id: image_id.to_owned(),
                });
            }
        }
    }
    (exact, observations)
}

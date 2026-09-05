//! Narrow late-completion recovery authority. A local lock alone authorizes no
//! remote mutation; every original hook must actually have completed first.
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::command::{plain, CommonOpts, OpsCommand};

pub(super) const LABEL: &str = "curie-upgrade-operation";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub id: String,
    pub source_revision: u64,
    pub source_uid: String,
    pub expected_revision: u64,
    pub checkpoint_uid: String,
    pub target_identity: String,
    pub hooks_identity: String,
    pub pending_uid: Option<String>,
    pub original_hook_uids: std::collections::BTreeMap<String, String>,
    pub pending_manifest_identity: Option<String>,
    pub completed_revision_uid: Option<String>,
    pub rollback_started: bool,
}

pub(super) fn refusal(message: &str) -> anyhow::Error {
    crate::exit::CliError::failure(message)
        .with_fix("preserve the upgrade checkpoint and local ownership file; inspect the exact Helm revision and all three retained hook Jobs/Pods. Only the same owned late-completion attempt can resume; active, missing, foreign or interrupted rollback evidence requires the operator's separately reviewed recovery procedure")
        .into()
}

pub(super) fn metadata_command(opts: &CommonOpts, revision: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("secret"),
            plain(format!("sh.helm.release.v1.{}.v{revision}", opts.release)),
            plain("-n"),
            plain(&opts.namespace),
            plain("-o"),
            plain("jsonpath-as-json={.metadata}"),
        ],
    )
}

pub(super) fn terminal_command(opts: &CommonOpts) -> OpsCommand {
    // All namespace Jobs/Pods are read so unlabeled orphans cannot hide behind a
    // selector. Only the exact three named Jobs and their related Pods count.
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("jobs,pods"),
            plain("-n"),
            plain(&opts.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

pub(super) fn metadata(
    raw: &Value,
    opts: &CommonOpts,
    revision: u64,
    state: &str,
    marker: Option<&str>,
) -> Result<String> {
    let entries = raw
        .as_array()
        .filter(|entries| entries.len() == 1)
        .ok_or_else(|| refusal("Helm storage metadata projection is missing or ambiguous"))?;
    let entry = &entries[0];
    let labels = &entry["labels"];
    let revision_label = revision.to_string();
    if entry["name"] != format!("sh.helm.release.v1.{}.v{revision}", opts.release)
        || entry["namespace"] != opts.namespace
        || labels["owner"] != "helm"
        || labels["name"] != opts.release
        || labels["version"].as_str() != Some(revision_label.as_str())
        || labels["status"] != state
        || entry["resourceVersion"].as_str().is_none_or(str::is_empty)
        || !entry["deletionTimestamp"].is_null()
        || marker.is_some_and(|marker| labels[LABEL] != marker)
    {
        return Err(refusal(
            "Helm storage metadata does not match the exact owned revision",
        ));
    }
    entry["uid"]
        .as_str()
        .filter(|uid| !uid.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| refusal("Helm storage metadata has no UID"))
}

pub(super) fn digest(value: &impl Serialize) -> Result<String> {
    use sha2::{Digest, Sha256};
    Ok(Sha256::digest(serde_json::to_vec(value)?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) fn hook_manifests(status: &Value, marker: &str) -> Result<Vec<Value>> {
    let mut selected = Vec::new();
    for hook in status["hooks"]
        .as_array()
        .ok_or_else(|| refusal("pending release has no exact hook manifests"))?
    {
        let manifest = hook["manifest"]
            .as_str()
            .ok_or_else(|| refusal("pending hook manifest is absent"))?;
        let object: Value = serde_norway::from_str(manifest)
            .map_err(|_| refusal("pending hook manifest is malformed"))?;
        let events = hook["events"]
            .as_array()
            .filter(|events| !events.is_empty())
            .ok_or_else(|| refusal("pending hook events are absent"))?;
        let component = object
            .pointer("/metadata/labels/app.kubernetes.io~1component")
            .and_then(Value::as_str);
        if matches!(
            component,
            Some("upgrade-drain" | "schema-migrate" | "upgrade-drain-release")
        ) {
            let declared: Vec<_> = object["metadata"]["annotations"]["helm.sh/hook"]
                .as_str()
                .unwrap_or_default()
                .split(',')
                .collect();
            if events.len() != declared.len()
                || events.iter().any(|event| {
                    event
                        .as_str()
                        .is_none_or(|event| !declared.contains(&event))
                })
            {
                return Err(refusal("pending hook event metadata is inconsistent"));
            }
            selected.push(object);
        } else if object["metadata"]["labels"][LABEL] == marker
            || events.iter().any(|event| event.as_str() != Some("test"))
        {
            // Additional original upgrade hooks may still have in-flight work.
            // Never ignore them merely because they do not run on rollback.
            return Err(refusal(
                "additional executable hooks require a separately verified recovery prerequisite",
            ));
        }
    }
    Ok(selected)
}

pub(super) fn validate_hooks(hooks: &[Value], marker: &str, namespace: &str) -> Result<()> {
    if hooks.len() != 3 {
        return Err(refusal(
            "recovery requires exactly the three known target hook Jobs",
        ));
    }
    for (component, events, weight) in [
        ("upgrade-drain", "pre-upgrade,pre-rollback", "-10"),
        (
            "schema-migrate",
            "post-install,pre-upgrade,pre-rollback",
            "-5",
        ),
        ("upgrade-drain-release", "post-upgrade,post-rollback", "-10"),
    ] {
        let selected: Vec<_> = hooks
            .iter()
            .filter(|hook| {
                hook.pointer("/metadata/labels/app.kubernetes.io~1component")
                    .and_then(Value::as_str)
                    == Some(component)
            })
            .collect();
        if selected.len() != 1 {
            return Err(refusal("recovery hook set is missing or ambiguous"));
        }
        let hook = selected[0];
        let metadata = &hook["metadata"];
        // Helm may omit namespace from a namespaced manifest; its release
        // namespace supplies the effective namespace in that case.
        if hook["apiVersion"] != "batch/v1"
            || hook["kind"] != "Job"
            || metadata["name"].as_str().is_none_or(str::is_empty)
            || metadata["namespace"]
                .as_str()
                .is_some_and(|ns| ns != namespace)
            || metadata["labels"][LABEL] != marker
            || hook["spec"]["template"]["metadata"]["labels"][LABEL] != marker
            || metadata["annotations"]["helm.sh/hook"] != events
            || metadata["annotations"]["helm.sh/hook-weight"] != weight
            || metadata["annotations"]["helm.sh/hook-delete-policy"] != "before-hook-creation"
        {
            return Err(refusal(
                "target chart does not provide the exact opt-in recovery hooks",
            ));
        }
    }
    Ok(())
}

pub(super) fn all_terminal(
    hooks: &[Value],
    observation: &Value,
    marker: &str,
    namespace: &str,
) -> Result<()> {
    let items = observation["items"]
        .as_array()
        .ok_or_else(|| refusal("hook Job/Pod observation is missing"))?;
    let mut known_uids = Vec::new();
    let mut names = Vec::new();
    for hook in hooks {
        let name = hook["metadata"]["name"]
            .as_str()
            .ok_or_else(|| refusal("hook name is absent"))?;
        names.push(name);
        let matches: Vec<_> = items
            .iter()
            .filter(|item| {
                item["kind"] == "Job"
                    && item["metadata"]["name"] == name
                    && item["metadata"]["namespace"] == namespace
            })
            .collect();
        if matches.len() != 1 {
            return Err(refusal(
                "all original hook Jobs must be retained and terminal before recovery",
            ));
        }
        let job = matches[0];
        let uid = job["metadata"]["uid"]
            .as_str()
            .filter(|uid| !uid.is_empty())
            .ok_or_else(|| refusal("retained hook Job has no UID"))?;
        let conditions = job["status"]["conditions"].as_array();
        let complete = conditions.is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c["type"] == "Complete" && c["status"] == "True")
        });
        let failed = conditions.is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c["type"] == "Failed" && c["status"] == "True")
        });
        let invalid_count = ["active", "terminating", "succeeded"].iter().any(|field| {
            job["status"]
                .get(*field)
                .is_some_and(|value| value.as_u64().is_none())
        });
        if invalid_count
            || job["metadata"]["labels"][LABEL] != marker
            || !job["metadata"]["deletionTimestamp"].is_null()
            || job["status"]["active"].as_u64().unwrap_or(0) != 0
            || job["status"]["terminating"].as_u64().unwrap_or(0) != 0
            || !complete
            || failed
            || job["status"]["succeeded"].as_u64().unwrap_or(0) == 0
            || job["status"]["uncountedTerminatedPods"]
                .as_object()
                .is_some_and(|counts| {
                    counts
                        .values()
                        .any(|value| value.as_array().is_none_or(|a| !a.is_empty()))
                })
        {
            return Err(refusal(
                "an original hook Job is active, failed, terminating or incompletely accounted",
            ));
        }
        let pods: Vec<_> = items
            .iter()
            .filter(|pod| {
                pod["kind"] == "Pod"
                    && pod["metadata"]["namespace"] == namespace
                    && pod["metadata"]["ownerReferences"]
                        .as_array()
                        .is_some_and(|owners| owners.iter().any(|owner| owner["uid"] == uid))
            })
            .collect();
        if pods.is_empty() {
            return Err(refusal("retained hook Job has no terminal Pod witness"));
        }
        for pod in pods {
            let owners = pod["metadata"]["ownerReferences"].as_array().unwrap();
            if owners.len() != 1
                || owners[0]["kind"] != "Job"
                || owners[0]["name"] != name
                || owners[0]["controller"] != true
                || pod["metadata"]["labels"][LABEL] != marker
                || !pod["metadata"]["deletionTimestamp"].is_null()
                || !matches!(
                    pod["status"]["phase"].as_str(),
                    Some("Succeeded" | "Failed")
                )
                || pod["status"]["containerStatuses"]
                    .as_array()
                    .is_none_or(|statuses| {
                        statuses.is_empty()
                            || statuses.iter().any(|status| {
                                status["state"]["terminated"]["exitCode"].as_i64().is_none()
                            })
                    })
                || pod["status"]["ephemeralContainerStatuses"]
                    .as_array()
                    .is_some_and(|statuses| {
                        statuses.iter().any(|status| {
                            status["state"]["terminated"]["exitCode"].as_i64().is_none()
                        })
                    })
                || pod["status"]["initContainerStatuses"]
                    .as_array()
                    .is_some_and(|statuses| {
                        statuses.iter().any(|status| {
                            status["state"]["terminated"]["exitCode"].as_i64().is_none()
                        })
                    })
            {
                return Err(refusal(
                    "an original hook Pod has uncertain ownership or is not conclusively terminal",
                ));
            }
        }
        known_uids.push(uid);
    }
    for pod in items
        .iter()
        .filter(|item| item["kind"] == "Pod" && item["metadata"]["namespace"] == namespace)
    {
        let owners = pod["metadata"]["ownerReferences"].as_array();
        let related = pod["metadata"]["labels"][LABEL] == marker
            || owners.is_some_and(|owners| {
                owners
                    .iter()
                    .any(|o| o["name"].as_str().is_some_and(|n| names.contains(&n)))
            });
        if related
            && !owners.is_some_and(|owners| {
                owners.len() == 1
                    && owners[0]["uid"]
                        .as_str()
                        .is_some_and(|uid| known_uids.contains(&uid))
            })
        {
            return Err(refusal("an orphan or foreign hook Pod prevents recovery"));
        }
    }
    Ok(())
}

pub(super) fn job_uids(
    hooks: &[Value],
    observation: &Value,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut uids = std::collections::BTreeMap::new();
    for hook in hooks {
        let name = hook["metadata"]["name"]
            .as_str()
            .ok_or_else(|| refusal("hook name absent"))?;
        let job = observation["items"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["kind"] == "Job" && item["metadata"]["name"] == name)
            })
            .ok_or_else(|| refusal("hook Job absent"))?;
        uids.insert(
            name.to_owned(),
            job["metadata"]["uid"]
                .as_str()
                .ok_or_else(|| refusal("hook Job UID absent"))?
                .to_owned(),
        );
    }
    Ok(uids)
}

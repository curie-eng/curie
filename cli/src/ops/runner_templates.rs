use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::{plain, run_capture, CommonOpts, OpsCommand};

struct AgentTemplate<'a> {
    object: &'a Value,
    name: &'a str,
    images: Vec<ImageField<'a>>,
}

struct ImageField<'a> {
    path: String,
    kind: &'static str,
    name: &'a str,
    image: &'a str,
}

fn image_fields(object: &Value) -> Result<Vec<ImageField<'_>>> {
    let mut fields = Vec::new();
    for kind in ["containers", "initContainers"] {
        let Some(containers) = object.pointer(&format!("/spec/podTemplate/spec/{kind}")) else {
            continue;
        };
        for (index, container) in containers
            .as_array()
            .context("SandboxTemplate container list is malformed")?
            .iter()
            .enumerate()
        {
            let name = container
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .context("SandboxTemplate container has no name")?;
            let image = container
                .get("image")
                .and_then(Value::as_str)
                .filter(|image| !image.is_empty())
                .context("SandboxTemplate container has no image")?;
            fields.push(ImageField {
                path: format!("/spec/podTemplate/spec/{kind}/{index}/image"),
                kind,
                name,
                image,
            });
        }
    }
    Ok(fields)
}

fn runner(object: &Value) -> Result<(usize, &str)> {
    let containers = object
        .pointer("/spec/podTemplate/spec/containers")
        .and_then(Value::as_array)
        .context("SandboxTemplate has no runner containers")?;
    let mut runners = containers
        .iter()
        .enumerate()
        .filter(|(_, container)| container.get("name").and_then(Value::as_str) == Some("runner"));
    let (index, container) = runners
        .next()
        .context("SandboxTemplate has no named runner")?;
    if runners.next().is_some() {
        bail!("SandboxTemplate has multiple named runners");
    }
    let image = container
        .get("image")
        .and_then(Value::as_str)
        .filter(|image| !image.is_empty())
        .context("SandboxTemplate has no runner image")?;
    Ok((index, image))
}

fn platform_templates<'a>(
    list: &'a Value,
    values: &Value,
    release: &str,
) -> Result<(&'a str, Vec<AgentTemplate<'a>>)> {
    let fullname = super::release_fullname_from_values(release, values);
    let items = list
        .get("items")
        .and_then(Value::as_array)
        .context("SandboxTemplate list has no items")?;
    let owns = |object: &Value| {
        object
            .pointer("/metadata/labels/app.kubernetes.io~1instance")
            .and_then(Value::as_str)
            == Some(release)
    };
    let generic_name = format!("{fullname}-runner");
    let generic = items
        .iter()
        .find(|item| {
            owns(item)
                && item.pointer("/metadata/name").and_then(Value::as_str) == Some(&generic_name)
        })
        .context("the release's platform SandboxTemplate is missing")?;
    if generic
        .pointer("/metadata/annotations/meta.helm.sh~1release-name")
        .and_then(Value::as_str)
        != Some(release)
    {
        bail!("the platform SandboxTemplate has ambiguous release ownership");
    }
    let (_, target) = runner(generic)?;
    let platform_fields = image_fields(generic)?;
    let runner_slots: Vec<_> = platform_fields
        .iter()
        .filter(|field| field.image == target)
        .collect();
    let prefix = format!("{fullname}-agent-");
    let layers = values
        .pointer("/agentSandbox/runnerImages")
        .and_then(Value::as_object);
    let mut agents = Vec::new();
    for object in items.iter().filter(|item| owns(item)) {
        let name = object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .context("release SandboxTemplate has no name")?;
        let Some(agent) = name
            .strip_prefix(&prefix)
            .and_then(|name| name.strip_suffix("-runner"))
        else {
            continue;
        };
        let owner = object
            .pointer("/metadata/annotations/meta.helm.sh~1release-name")
            .and_then(Value::as_str);
        if owner.is_some() {
            continue;
        }
        if layers
            .and_then(|layers| layers.get(agent))
            .is_some_and(|layer| !layer.is_null())
        {
            continue;
        }
        if agent.is_empty()
            || object
                .pointer("/spec/podTemplate/metadata/labels/curietech.ai~1agent")
                .and_then(Value::as_str)
                != Some(agent)
        {
            bail!("SandboxTemplate {name} has ambiguous agent ownership; refusing to adopt it");
        }
        let managed_by = object
            .pointer("/metadata/labels/app.kubernetes.io~1managed-by")
            .and_then(Value::as_str);
        if !matches!(managed_by, Some("curie-cli" | "Helm")) {
            bail!("SandboxTemplate {name} has ambiguous manager ownership; refusing to adopt it");
        }
        let (_, image) = runner(object).with_context(|| format!("SandboxTemplate {name}"))?;
        let images = image_fields(object)?
            .into_iter()
            .filter(|field| {
                field.image == image
                    || runner_slots
                        .iter()
                        .any(|slot| slot.kind == field.kind && slot.name == field.name)
            })
            .collect();
        agents.push(AgentTemplate {
            object,
            name,
            images,
        });
    }
    Ok((target, agents))
}

async fn read_json(command: &OpsCommand) -> Result<Value> {
    let (ok, stdout, stderr) = run_capture(command).await?;
    if !ok {
        bail!(
            "SandboxTemplate reconciliation read failed: {}",
            crate::schema_window::redact_probe_text(stderr.trim())
        );
    }
    serde_json::from_str(&stdout)
        .context("SandboxTemplate reconciliation read returned malformed JSON")
}

async fn observe(common: &CommonOpts) -> Result<Option<(Value, Value)>> {
    let history = read_json(&super::helm_history_cmd(common)).await?;
    let revision = crate::cluster_secrets::serving_revision(&history, &common.release)
        .map_err(anyhow::Error::msg)?;
    let values = read_json(&crate::cluster_secrets::helm_get_json(
        common, "values", true, revision,
    ))
    .await?;
    if values
        .pointer("/agentSandbox/deploy")
        .and_then(Value::as_bool)
        == Some(false)
    {
        return Ok(None);
    }
    let list = read_json(&OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(&common.namespace),
            plain("get"),
            plain(crate::connectors::SANDBOX_TEMPLATE_KIND),
            plain("-l"),
            plain(format!("app.kubernetes.io/instance={}", common.release)),
            plain("-o"),
            plain("json"),
        ],
    ))
    .await?;
    Ok(Some((values, list)))
}

pub(super) async fn verify(common: &CommonOpts) -> Result<()> {
    let Some((values, list)) = observe(common).await? else {
        return Ok(());
    };
    let (target, templates) = platform_templates(&list, &values, &common.release)?;
    for template in templates {
        for field in template.images {
            if field.image != target {
                bail!(
                    "SandboxTemplate {} {} {} still renders {}, expected the platform runner {}",
                    template.name,
                    field.kind,
                    field.name,
                    field.image,
                    target
                );
            }
        }
    }
    Ok(())
}

pub(super) async fn reconcile(common: &CommonOpts) -> Result<()> {
    let Some((values, list)) = observe(common).await? else {
        return Ok(());
    };
    let (target, templates) = platform_templates(&list, &values, &common.release)?;
    for template in templates {
        let replacements: Vec<_> = template
            .images
            .iter()
            .filter(|field| field.image != target)
            .collect();
        if replacements.is_empty() {
            continue;
        }
        let version = template
            .object
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str)
            .context("deployment-created SandboxTemplate has no resourceVersion")?;
        let mut patch =
            vec![json!({"op": "test", "path": "/metadata/resourceVersion", "value": version})];
        patch.extend(replacements.into_iter().map(|field| {
            json!({
                "op": "replace", "path": field.path, "value": target
            })
        }));
        let patch = Value::Array(patch);
        let command = OpsCommand::new(
            "kubectl",
            vec![
                plain("-n"),
                plain(&common.namespace),
                plain("patch"),
                plain(crate::connectors::SANDBOX_TEMPLATE_KIND),
                plain(template.name),
                plain("--type=json"),
                plain("-p"),
                plain(patch.to_string()),
            ],
        );
        let (ok, _, stderr) = run_capture(&command).await?;
        if !ok {
            bail!(
                "could not reconcile SandboxTemplate {}: {}",
                template.name,
                crate::schema_window::redact_probe_text(stderr.trim())
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Metadata and named-container shapes follow the Kubernetes object APIs:
    // https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/
    // https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#Container
    fn object(
        name: &str,
        agent: Option<&str>,
        manager: &str,
        owner: Option<&str>,
        image: &str,
    ) -> Value {
        let mut value = json!({
            "metadata": {"name": name, "resourceVersion": "3", "labels": {
                "app.kubernetes.io/instance": "acme", "app.kubernetes.io/managed-by": manager}},
            "spec": {"podTemplate": {"metadata": {"labels": {}}, "spec": {
                "containers": [{"name": "sidecar", "image": "acme-sidecar:stable"},
                               {"name": "runner", "image": image}]}}}
        });
        if let Some(agent) = agent {
            value["spec"]["podTemplate"]["metadata"]["labels"]["curietech.ai/agent"] = json!(agent);
        }
        if let Some(owner) = owner {
            value["metadata"]["annotations"] = json!({"meta.helm.sh/release-name": owner});
        }
        value
    }

    #[test]
    fn platform_agent_template_canary_selects_cli_and_legacy_clones_only() {
        let items = json!({"items": [
            object("acme-curie-runner", None, "Helm", Some("acme"), "acme-runner:new"),
            object("acme-curie-agent-acme-bot-runner", Some("acme-bot"), "curie-cli", None, "acme-runner:old"),
            object("acme-curie-agent-acme-legacy-runner", Some("acme-legacy"), "Helm", None, "acme-runner:old"),
            object("acme-curie-agent-acme-chart-runner", Some("acme-chart"), "Helm", Some("acme"), "acme-runner:new"),
            object("acme-curie-agent-acme-foreign-runner", Some("acme-foreign"), "Helm", Some("other"), "acme-runner:old"),
            object("acme-curie-agent-acme-layer-runner", Some("acme-layer"), "curie-cli", None, "acme-layer:old")
        ]});
        let (target, selected) = platform_templates(
            &items,
            &json!({"agentSandbox": {"runnerImages": {
            "acme-layer": "acme-layer:old"}}}),
            "acme",
        )
        .unwrap();
        assert_eq!(target, "acme-runner:new");
        assert_eq!(
            selected
                .iter()
                .map(|template| template.name)
                .collect::<Vec<_>>(),
            vec![
                "acme-curie-agent-acme-bot-runner",
                "acme-curie-agent-acme-legacy-runner"
            ]
        );
        assert!(selected.iter().all(|template| template.images.len() == 1
            && template.images[0].path == "/spec/podTemplate/spec/containers/1/image"));
    }

    #[test]
    fn platform_agent_template_canary_rejects_a_foreign_generic_template() {
        let list = json!({"items": [object("acme-curie-runner", None, "Helm", Some("other"), "acme-runner:new")]});
        assert!(platform_templates(&list, &json!({}), "acme").is_err());
    }

    #[test]
    fn platform_agent_template_canary_rejects_ambiguous_ownership_and_missing_runner() {
        let generic = object(
            "acme-curie-runner",
            None,
            "Helm",
            Some("acme"),
            "acme-runner:new",
        );
        for invalid in [
            object(
                "acme-curie-agent-acme-bot-runner",
                None,
                "curie-cli",
                None,
                "acme-runner:old",
            ),
            object(
                "acme-curie-agent-acme-bot-runner",
                Some("another-agent"),
                "curie-cli",
                None,
                "acme-runner:old",
            ),
            object(
                "acme-curie-agent-acme-bot-runner",
                Some("acme-bot"),
                "another-manager",
                None,
                "acme-runner:old",
            ),
            object(
                "acme-curie-agent-acme-bot-runner",
                Some("acme-bot"),
                "curie-cli",
                None,
                "",
            ),
        ] {
            let list = json!({"items": [generic, invalid]});
            assert!(platform_templates(&list, &json!({}), "acme").is_err());
        }
    }
}

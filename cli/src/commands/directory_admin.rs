//! Tenant directory administration through the platform API (ADR 0220).
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

pub enum DirectoryOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        tenant_id: String,
        resource: String,
        result: Value,
    },
}
impl crate::ui::CliOutput for DirectoryOutput {
    fn to_json(&self) -> Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Done {
                tenant_id,
                resource,
                result,
            } => json!({"tenant_id":tenant_id,"resource":resource,"result":result}),
        }
    }
    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Done { .. } => {
                ui.payload(&serde_json::to_string_pretty(&self.to_json()).unwrap_or_default())
            }
        }
    }
}

fn target(tenant: &str, resource: &str, write: bool) -> Result<(reqwest::Method, String)> {
    uuid::Uuid::parse_str(tenant).context("--tenant must be a UUID")?;
    Ok(match (resource, write) {
        ("config", false) => (reqwest::Method::GET, format!("/directory/config/{tenant}")),
        ("config", true) => (reqwest::Method::PUT, format!("/directory/config/{tenant}")),
        ("namespaces" | "teams", true) => (reqwest::Method::POST, format!("/directory/{resource}")),
        ("teams", false) => (reqwest::Method::GET, format!("/teams?tenant_id={tenant}")),
        ("people" | "links" | "namespaces", false) => (
            reqwest::Method::GET,
            format!("/identity/admin/{tenant}/{resource}"),
        ),
        _ => bail!("resource is read-only or unknown"),
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn directory_admin(
    api_url: String,
    api_key: String,
    tenant: String,
    resource: String,
    apply: Option<std::path::PathBuf>,
    after: Option<uuid::Uuid>,
    dry_run: bool,
) -> Result<DirectoryOutput> {
    let (method, mut path) = target(&tenant, &resource, apply.is_some())?;
    let body: Option<Value> = apply
        .map(|file| -> Result<Value> {
            let raw = std::fs::read(file).context("reading directory request JSON")?;
            let mut value: Value =
                serde_json::from_slice(&raw).context("parsing directory request JSON")?;
            let object = value
                .as_object_mut()
                .context("directory request must be a JSON object")?;
            if resource != "config" {
                if object
                    .get("tenant_id")
                    .is_some_and(|value| value.as_str() != Some(&tenant))
                {
                    bail!("request tenant_id differs from --tenant");
                }
                object.insert("tenant_id".into(), json!(tenant));
            }
            Ok(value)
        })
        .transpose()?;
    if let Some(cursor) = after {
        if method != reqwest::Method::GET
            || !matches!(resource.as_str(), "people" | "links" | "namespaces")
        {
            bail!("--after applies only to read-only people, links or namespaces");
        }
        path.push_str(&format!("?after={cursor}"));
    }
    if dry_run {
        return Ok(DirectoryOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!("{method} {api_url}{path}")],
        }));
    }
    let client = crate::api::ApiClient::with_resolved_key(&api_url, &api_key)?;
    let result = if let Some(body) = body {
        match resource.as_str() {
            "config" => {
                client
                    .put_directory_config(&tenant, &serde_json::from_value(body)?)
                    .await?
            }
            "namespaces" => {
                client
                    .create_directory_namespace(&serde_json::from_value(body)?)
                    .await?
            }
            "teams" => {
                client
                    .create_directory_team(&serde_json::from_value(body)?)
                    .await?
            }
            _ => bail!("resource is read-only or unknown"),
        }
    } else {
        client.directory_admin_read(&path).await?
    };
    Ok(DirectoryOutput::Done {
        tenant_id: tenant,
        resource,
        result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn directory_routes_never_offer_manual_link_writes() {
        let tenant = "00000000-0000-0000-0000-000000000001";
        assert!(target(tenant, "links", true).is_err());
        assert!(target("invalid", "config", false).is_err());
        assert_eq!(
            target(tenant, "config", true).unwrap().0,
            reqwest::Method::PUT
        );
        assert_eq!(
            target(tenant, "people", false).unwrap().1,
            format!("/identity/admin/{tenant}/people")
        );
    }

    #[test]
    fn directory_request_mirrors_preserve_cas_and_server_namespace_authority() {
        use crate::api_requests::{DirectoryConfigIn, DirectoryNamespaceIn, DirectoryTeamIn};
        let tenant = "00000000-0000-0000-0000-000000000001";
        let config = json!({"issuer":"https://idp.example.com","managed_by":"directory",
            "governed_domains":["example.com"],"expected_version":7});
        let typed: DirectoryConfigIn = serde_json::from_value(config.clone()).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), config);
        let namespace = json!({"tenant_id":tenant,"provider":"email",
            "kind":"email_domain","key":"example.com"});
        let typed: DirectoryNamespaceIn = serde_json::from_value(namespace.clone()).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), namespace);
        let mut null_authority = namespace;
        null_authority["authority"] = Value::Null;
        assert!(serde_json::from_value::<DirectoryNamespaceIn>(null_authority).is_err());
        let team = json!({"tenant_id":tenant,"external_id":"group-1","name":"Operators"});
        let typed: DirectoryTeamIn = serde_json::from_value(team.clone()).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), team);
        let mut unknown = config;
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<DirectoryConfigIn>(unknown).is_err());
    }
}

use anyhow::{bail, Context, Result};
use reqwest::Method;
use serde_json::Value;
use uuid::Uuid;

pub fn authority_target(
    action: &str,
    tenant: Option<Uuid>,
    after: Option<Uuid>,
) -> Result<(Method, String)> {
    if action == "list" {
        if tenant.is_some() {
            bail!("list does not accept --tenant; use get for one tenant");
        }
        let path = after.map_or("/identity/authorities".to_owned(), |cursor| {
            format!("/identity/authorities?after={cursor}")
        });
        return Ok((Method::GET, path));
    }
    if after.is_some() {
        bail!("--after applies only to list");
    }
    let tenant = tenant.context("--tenant is required for this action")?;
    let path = format!("/identity/authorities/{tenant}");
    Ok(match action {
        "get" => (Method::GET, path),
        "adopt" => (Method::PUT, path),
        "replace" | "retire" => (Method::POST, format!("{path}/{action}")),
        _ => bail!("unknown authority action"),
    })
}

pub fn role_filter_target(
    action: &str,
    tenant: Uuid,
    agent: Uuid,
    role: Option<&str>,
) -> Result<(Method, String)> {
    let path = format!("/identity/roles/{tenant}/{agent}");
    if action == "list" {
        if role.is_some() {
            bail!("list returns both roles and does not accept --role");
        }
        return Ok((Method::GET, path));
    }
    let role = role.context("--role is required for set and remove")?;
    if !matches!(role, "invocation" | "approver") {
        bail!("unknown role");
    }
    Ok(match action {
        "set" => (Method::PUT, format!("{path}/{role}")),
        "remove" => (Method::DELETE, format!("{path}/{role}")),
        _ => bail!("unknown role filter action"),
    })
}

pub async fn identity_operator(
    api_url: String,
    api_key: String,
    method: Method,
    path: String,
    apply: Option<std::path::PathBuf>,
    dry_run: bool,
) -> Result<super::DirectoryOutput> {
    if (method == Method::GET) == apply.is_some() {
        bail!("mutations require --apply JSON; reads do not accept --apply");
    }
    let body: Option<Value> = apply
        .map(|file| -> Result<Value> {
            let raw = std::fs::read(file).context("reading identity operator request")?;
            let body: Value = serde_json::from_slice(&raw).context("invalid request JSON")?;
            if !body.is_object() {
                bail!("request must be a JSON object");
            }
            Ok(body)
        })
        .transpose()?;
    if dry_run {
        return Ok(super::DirectoryOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!("{method} {path}")],
        }));
    }
    let client = crate::api::ApiClient::with_resolved_key(&api_url, &api_key)?;
    let result = client
        .identity_operator_request(method, &path, body.as_ref())
        .await?;
    Ok(super::DirectoryOutput::Done {
        tenant_id: result["configuration"]["tenant_id"]
            .as_str()
            .or_else(|| result["tenant_id"].as_str())
            .unwrap_or_default()
            .to_owned(),
        resource: "identity".to_owned(),
        result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::CliOutput;

    #[tokio::test]
    async fn dry_run_never_emits_credentials_or_request_contents() {
        let tenant = Uuid::nil();
        let (method, path) = authority_target("adopt", Some(tenant), None).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("request.json");
        std::fs::write(&file, r#"{"bearer":"PRIVATE_BODY_MARKER"}"#).unwrap();
        let output = identity_operator(
            "https://user:PRIVATE_URL_MARKER@example.com".into(),
            "PRIVATE_API_MARKER".into(),
            method,
            path,
            Some(file),
            true,
        )
        .await
        .unwrap()
        .to_json()
        .to_string();
        assert!(output.contains("PUT /identity/authorities/"));
        assert!(!output.contains("PRIVATE_"));
    }

    #[test]
    fn actions_require_their_scope_and_use_versioned_operator_routes() {
        assert!(authority_target("get", None, None).is_err());
        assert!(authority_target("retire", Some(Uuid::nil()), Some(Uuid::nil())).is_err());
        assert!(role_filter_target("set", Uuid::nil(), Uuid::nil(), None).is_err());
        assert_eq!(
            role_filter_target("remove", Uuid::nil(), Uuid::nil(), Some("approver"))
                .unwrap()
                .0,
            Method::DELETE
        );
    }
}

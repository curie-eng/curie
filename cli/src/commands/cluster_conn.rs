//! The one way a cluster command reaches the release API (#4362 decision 6):
//! the `cluster` verbs and `factory quickstart` both resolve the URL, the key,
//! and the loopback tunnel here.

use anyhow::{bail, Result};

/// Resolve a cluster verb's API URL, key, and optional loopback tunnel. Explicit
/// values win. An omitted URL self-plumbs the release API service, while an
/// omitted key is read from the release Secret.
pub async fn resolve_cluster_conn(
    api_url: Option<String>,
    api_key: Option<String>,
    namespace: &str,
    release: &str,
    dry_run: bool,
) -> Result<(String, String, Option<tokio::process::Child>)> {
    let api_key = super::normalize_deploy_api_key(api_key);
    let key_auto_discovered = api_key.is_none();
    let local_port = 0;
    if dry_run {
        return Ok((
            api_url.unwrap_or_else(|| format!("http://localhost:{local_port}")),
            api_key.unwrap_or_default(),
            None,
        ));
    }
    let api_key = match api_key {
        Some(key) => key,
        None => crate::ops::discover_api_key(namespace, release).await?,
    };
    let tunnel = super::deploy_api_tunnel(
        api_url.as_deref(),
        namespace,
        release,
        local_port,
        crate::message::API_REMOTE_PORT,
    )
    .await;
    let (api_url, port_forward) = match tunnel {
        Some((_fullname, pf_cmd)) => {
            let (child, effective_port) =
                crate::message::start_port_forward(&pf_cmd, local_port, "cluster api").await?;
            (format!("http://127.0.0.1:{effective_port}"), Some(child))
        }
        None => {
            let url = api_url.expect("explicit url when no port-forward");
            if key_auto_discovered && crate::api::is_insecure_endpoint(&url) {
                bail!(
                    "refusing to send the auto-discovered release key over cleartext \
                     HTTP to {url}: the strong key would leak on the wire. Pass \
                     --api-key explicitly to acknowledge, use an https:// URL, or omit \
                     --api-url to reach the release over the loopback port-forward."
                );
            }
            (url, None)
        }
    };
    Ok((api_url, api_key, port_forward))
}

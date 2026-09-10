//! Apply the connector objects the API rendered, and prune what is no longer
//! declared (ADR-0086, #1063).
//!
//! The API computes these manifests; this applies them. That split is
//! deliberate: rendering is a pure function, so the API needs no cluster access
//! and keeps its deliberately read-only RBAC even though it is the service that
//! receives internet webhooks. Applying happens here, under the operator's own
//! kubectl credentials -- where cluster-write authority already lived.
//!
//! Pruning is the reason this is not a bare `kubectl apply`. Every object
//! carries a label naming the agent that declared it, so removing a connector
//! from `connectors.yaml` removes its Deployment, Service, and NetworkPolicies
//! on the next deploy. Without that, a deleted connector leaves a pod running
//! with a credential mounted and nothing referencing it -- the kind of leak
//! nobody notices because nothing breaks.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::secrets::{
    keys_being_replaced, replacement_warning_line, write_intent_line, SecretScope,
};

/// Marks every object this command owns, so pruning can find them again.
pub const OWNER_LABEL: &str = "curie.dev/connector-owner";

/// The label value for an agent's connector objects.
pub fn owner_value(agent_name: &str) -> String {
    agent_name.to_string()
}

/// Stamp the owner label onto each rendered object.
///
/// Applied here rather than in the renderer because ownership is a property of
/// *this deploy*, not of the declaration -- the same bundle deployed as two
/// agents must not have one prune the other's objects.
pub fn label_objects(manifests: &[Value], agent_name: &str) -> Vec<Value> {
    manifests
        .iter()
        .cloned()
        .map(|mut obj| {
            if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let labels = meta
                    .entry("labels")
                    .or_insert_with(|| Value::Object(Default::default()));
                if let Some(map) = labels.as_object_mut() {
                    map.insert(
                        OWNER_LABEL.to_string(),
                        Value::String(owner_value(agent_name)),
                    );
                }
            }
            obj
        })
        .collect()
}

/// `kubectl apply` argv for a JSON document supplied on stdin.
///
/// kubectl accepts JSON wherever it accepts YAML, so the manifests travel from
/// the API as JSON and never need a YAML serializer on this side.
pub fn apply_args(namespace: &str) -> Vec<String> {
    vec![
        "kubectl".into(),
        "-n".into(),
        namespace.into(),
        "apply".into(),
        "-f".into(),
        "-".into(),
    ]
}

/// `kubectl delete` argv for owned objects of `kind` that are no longer declared.
///
/// Scoped by the owner label AND by name, so it can only ever remove objects
/// this command created for this agent. A prune that selected on the label
/// alone would delete a concurrently-deploying agent's objects.
pub fn prune_args(namespace: &str, agent_name: &str, keep: &[String]) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "kubectl".into(),
        "-n".into(),
        namespace.into(),
        "delete".into(),
        // `secret` is in here deliberately. Dropping a connector from
        // connectors.yaml must take its credential with it -- a Secret left
        // behind is a live token nothing references and nobody notices.
        "deployment,service,networkpolicy,secret".into(),
        "-l".into(),
        format!("{}={}", OWNER_LABEL, owner_value(agent_name)),
        "--ignore-not-found".into(),
    ];
    // `kubectl delete -l` has no "except these" flag, so exclusion rides as a
    // field selector on name. It MUST be one comma-separated flag: kubectl
    // takes the last --field-selector and silently discards earlier ones, so
    // repeating the flag would exclude only the final object and delete the
    // rest -- pruning away the connectors just applied.
    if !keep.is_empty() {
        let excluded: Vec<String> = keep.iter().map(|n| format!("metadata.name!={n}")).collect();
        args.push(format!("--field-selector={}", excluded.join(",")));
    }
    args
}

/// Object names in a rendered set, used to build the prune exclusion.
pub fn object_names(manifests: &[Value]) -> Vec<String> {
    manifests
        .iter()
        .filter_map(|o| {
            o.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(str::to_string)
        })
        .collect()
}

/// Keys whose VALUES this command must resolve, and the Secret to put them in.
///
/// Taken from what the API DECLARED, not inferred from the manifests. Since
/// #1163 a connector may reference a Secret provisioned out of band, and its
/// key appears in the rendered `secretKeyRef` exactly like an owned one --
/// indistinguishable by shape. Inferring would try to resolve a credential
/// this caller may not have, and by design should not: the whole point of the
/// reference form is that the deploy path never handles it.
pub fn owned_secret(name: &str, keys: &[String]) -> Option<(String, Vec<String>)> {
    if name.is_empty() || keys.is_empty() {
        return None;
    }
    let mut keys = keys.to_vec();
    keys.sort();
    Some((name.to_string(), keys))
}

/// The Secret carrying resolved connector credentials.
///
/// `stringData` so kubectl does the base64; values reach it over stdin, never
/// argv. `kubectl create secret --from-literal` would put every credential in
/// the process table, where any local user can read it off `ps`.
pub fn render_secret(
    name: &str,
    namespace: &str,
    values: &std::collections::BTreeMap<String, String>,
) -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": name, "namespace": namespace},
        "stringData": values,
    })
}

/// `kubectl` argv reading the release's rendered `app.kubernetes.io/name`.
///
/// Read from the cluster, not re-derived from chart values. That label is what
/// Rail 1's default-deny egress selects on, so taking it from the objects the
/// chart actually rendered means the connector's allow-rule matches by
/// construction -- a second derivation could disagree, and a NetworkPolicy that
/// selects nothing fails silently (ADR-0067).
pub fn app_name_args(namespace: &str, release: &str) -> Vec<String> {
    vec![
        "kubectl".into(),
        "-n".into(),
        namespace.into(),
        "get".into(),
        "deployment".into(),
        "-l".into(),
        format!("app.kubernetes.io/instance={release}"),
        "-o".into(),
        r"jsonpath={.items[0].metadata.labels.app\.kubernetes\.io/name}".into(),
    ]
}

/// `kubectl` argv for the current kubeconfig's cluster identity material.
pub fn kubeconfig_view_args() -> Vec<String> {
    vec![
        "kubectl".into(),
        "config".into(),
        "view".into(),
        "--minify".into(),
        "--raw".into(),
        "-o".into(),
        "json".into(),
    ]
}

/// `kubectl` argv that lists a Secret without decoding its values on this side.
pub fn get_secret_args(namespace: &str, name: &str) -> Vec<String> {
    vec![
        "kubectl".into(),
        "-n".into(),
        namespace.into(),
        "get".into(),
        "secret".into(),
        name.into(),
        "-o".into(),
        "json".into(),
    ]
}

/// A captured kubeconfig target. Connector reads and writes always name this
/// snapshot explicitly, never a subsequently changed ambient context.
#[derive(Clone, Debug)]
pub struct ClusterTarget {
    pub scope: SecretScope,
    kubeconfig: Arc<tempfile::NamedTempFile>,
}

impl ClusterTarget {
    fn args(&self, argv: &[String]) -> Vec<String> {
        let mut result = vec![
            "kubectl".into(),
            "--kubeconfig".into(),
            self.kubeconfig.path().display().to_string(),
        ];
        result.extend(argv.iter().skip(1).cloned());
        result
    }

    async fn revalidate_ambient_binding(&self) -> Result<()> {
        if discover_cluster_identity().await? != self.scope.cluster_identity {
            anyhow::bail!(
                "current kubectl context no longer matches the connector target {}; refusing to mutate the captured target",
                self.scope.describe()
            );
        }
        Ok(())
    }
}

/// Fingerprint of the kube-apiserver this kubeconfig currently points at.
///
/// SHA-256 of `server` plus CA material so two clusters with the same release
/// name still differ. The digest is the stored identity; the raw CA PEM is never
/// retained as the key.
pub fn cluster_identity_from_kubeconfig_view(view: &Value) -> Result<String> {
    let cluster = view
        .get("clusters")
        .and_then(|clusters| clusters.as_array())
        .and_then(|clusters| clusters.first())
        .and_then(|entry| entry.get("cluster"))
        .ok_or_else(|| {
            crate::exit::usage(
                "kubectl config view returned no cluster; cannot scope connector secrets",
            )
        })?;
    let server = cluster.get("server").and_then(Value::as_str).unwrap_or("");
    let ca = cluster
        .get("certificate-authority-data")
        .and_then(Value::as_str)
        .or_else(|| cluster.get("certificate-authority").and_then(Value::as_str))
        .unwrap_or("");
    if server.is_empty() && ca.is_empty() {
        return Err(crate::exit::usage(
            "kubectl config view has no server or certificate authority; cannot scope connector secrets",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(server.as_bytes());
    hasher.update(b"\n");
    hasher.update(ca.as_bytes());
    let digest = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok(format!("ca:{digest}"))
}

/// Key names present on a live Secret object. Values (base64 or plaintext) are
/// discarded so replacement detection cannot leak them into logs.
pub fn secret_key_names(obj: &Value) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for field in ["data", "stringData"] {
        if let Some(map) = obj.get(field).and_then(Value::as_object) {
            keys.extend(map.keys().cloned());
        }
    }
    keys
}

/// Serialize a manifest list as a single JSON `List` for one apply invocation.
pub fn as_list_document(manifests: &[Value]) -> Result<String> {
    let doc = serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": manifests,
    });
    serde_json::to_string(&doc).context("serializing connector manifests")
}

/// Whether an mcp_entry URL names this Deployment's Service.
///
/// Same host match #2350 uses for rollout wait (`connector_workloads`): the
/// Service DNS is `<deployment>.<namespace>.svc.cluster.local`. Sibling: share
/// this helper with #2350 when that PR merges. Do not treat a missing match as
/// a capability-probe failure (#2519).
fn url_names_deployment(url: &str, deployment: &str) -> bool {
    let rest = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let hostport = rest.split('/').next().unwrap_or(rest);
    let host = hostport.rsplit('@').next().unwrap_or(hostport);
    let host = host.split(':').next().unwrap_or(host);
    host == deployment || host.starts_with(&format!("{deployment}."))
}

fn mcp_join_prefix(release: &str, agent: &str) -> String {
    format!("{release}-{agent}-mcp-")
}

/// Hosted Deployments whose Service is not in `mcp_entries` (#2352).
///
/// This is the mounted-*declaration* check, not tool-surface discovery: a
/// matching URL means Curie derived the MCP entry the runner will inject.
/// Empty Bearer expansion and probe failure stay #2519. Hosted *readiness*
/// (rollout) stays #2350. A hosted connector with a matching entry must not
/// warn: that is the shipped 0.8.6+ mount path, not the 0.8.5 silence.
pub fn hosted_unmounted_connectors(
    manifests: &[Value],
    mcp_entries: &BTreeMap<String, Value>,
    release: &str,
    agent: &str,
) -> Vec<String> {
    let prefix = mcp_join_prefix(release, agent);
    let mut names = Vec::new();
    for obj in manifests {
        if obj.get("kind").and_then(Value::as_str) != Some("Deployment") {
            continue;
        }
        let Some(name) = obj
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        if mcp_entries.iter().any(|(_, entry)| {
            entry
                .get("url")
                .and_then(Value::as_str)
                .is_some_and(|url| url_names_deployment(url, name))
        }) {
            continue;
        }
        let connector = name.strip_prefix(&prefix).unwrap_or(name);
        names.push(connector.to_string());
    }
    names.sort();
    names.dedup();
    names
}

/// Loud warning for connectors that were hosted but not declared mounted.
pub fn hosted_unmounted_warning(names: &[String]) -> Option<String> {
    match names {
        [] => None,
        [one] => Some(format!(
            "connector {one} was hosted but not mounted into the agent's MCP declaration"
        )),
        many => Some(format!(
            "connectors {} were hosted but not mounted into the agent's MCP declaration",
            many.join(", ")
        )),
    }
}

/// Human deploy report: warnings first, then URL notes for mounted entries.
pub fn connector_report_lines(synced: &ConnectorSync) -> (Vec<String>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut notes = Vec::new();
    if let Some(msg) = hosted_unmounted_warning(&synced.hosted_unmounted) {
        warnings.push(msg);
    }
    for (name, url) in &synced.urls {
        if synced.hosted_unmounted.iter().any(|n| n == name) {
            continue;
        }
        notes.push(format!("connector {name}: {url}"));
    }
    (warnings, notes)
}

/// Emit the deploy-time hosted vs mounted-declaration report.
pub fn report_connector_sync(synced: &ConnectorSync) {
    let ui = crate::ui::ui();
    let (warnings, notes) = connector_report_lines(synced);
    for msg in warnings {
        ui.warn(&msg);
    }
    for msg in notes {
        ui.note(&msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn objs() -> Vec<Value> {
        vec![
            json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"r-mcp-g"}}),
            json!({"apiVersion":"apps/v1","kind":"Deployment",
                   "metadata":{"name":"r-mcp-g","labels":{"existing":"kept"}}}),
        ]
    }

    #[test]
    fn owner_label_is_stamped_without_dropping_existing_labels() {
        let out = label_objects(&objs(), "acme-bot");
        let dep = &out[1]["metadata"]["labels"];
        assert_eq!(dep[OWNER_LABEL], "acme-bot");
        assert_eq!(dep["existing"], "kept", "renderer labels must survive");
    }

    #[test]
    fn objects_with_no_labels_map_still_get_the_owner() {
        let out = label_objects(&objs(), "acme-bot");
        assert_eq!(out[0]["metadata"]["labels"][OWNER_LABEL], "acme-bot");
    }

    #[test]
    fn prune_is_scoped_to_this_agent_not_all_connectors() {
        // Selecting on the label alone would delete a concurrently-deploying
        // agent's objects -- both are "connector objects".
        let args = prune_args("ns", "acme-bot", &[]);
        assert!(args.contains(&format!("{OWNER_LABEL}=acme-bot")));
        assert!(!args.iter().any(|a| a == OWNER_LABEL));
    }

    #[test]
    fn prune_excludes_every_applied_object_in_one_flag() {
        // kubectl keeps only the LAST --field-selector and silently drops the
        // rest, so repeating the flag would exclude one object and prune the
        // others -- deleting the connectors this deploy just applied.
        let keep = vec!["a".to_string(), "b".to_string()];
        let args = prune_args("ns", "acme-bot", &keep);
        let selectors: Vec<&String> = args
            .iter()
            .filter(|a| a.starts_with("--field-selector"))
            .collect();
        assert_eq!(selectors.len(), 1, "must be a single flag: {args:?}");
        assert_eq!(
            selectors[0],
            "--field-selector=metadata.name!=a,metadata.name!=b"
        );
    }

    #[test]
    fn prune_with_nothing_declared_removes_everything_owned() {
        // The connector was deleted from connectors.yaml: its Deployment,
        // Service, and two NetworkPolicies must go, or a pod keeps running with a
        // credential mounted and nothing referencing it.
        let args = prune_args("ns", "acme-bot", &[]);
        assert!(!args.iter().any(|a| a.starts_with("--field-selector")));
        assert!(args.contains(&"deployment,service,networkpolicy,secret".to_string()));
    }

    #[test]
    fn manifests_serialize_as_one_list_document() {
        let doc = as_list_document(&objs()).unwrap();
        let parsed: Value = serde_json::from_str(&doc).unwrap();
        assert_eq!(parsed["kind"], "List");
        assert_eq!(parsed["items"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn prune_removes_the_secret_too_when_a_connector_is_dropped() {
        // A Secret left behind is a live credential nothing references. Deleting
        // the Deployment but keeping its token is the leak this whole prune
        // exists to close, so `secret` must be in the kind list.
        let args = prune_args("ns", "acme-bot", &[]);
        let kinds = args.iter().find(|a| a.contains("deployment")).unwrap();
        assert!(kinds.contains("secret"), "kinds were: {kinds}");
    }

    #[test]
    fn only_curie_owned_keys_are_resolved_locally() {
        // Since #1163 a referenced Secret's key appears in the rendered
        // secretKeyRef exactly like an owned one. Resolving it here would try
        // to read a credential this caller may not have -- and by design
        // should not, since the reference form exists so the deploy path never
        // handles it. The API says which keys are ours; we do not guess.
        let (name, keys) = owned_secret("curie-owned", &["OWNED".to_string()]).unwrap();
        assert_eq!(name, "curie-owned");
        assert_eq!(keys, vec!["OWNED".to_string()]);
    }

    #[test]
    fn a_connector_whose_secrets_are_all_referenced_needs_no_secret_object() {
        // Every credential lives in a Secret someone else provisioned, so this
        // deploy creates none -- which is the property that lets a reconciler
        // apply the same connector without holding any credential (ADR-0090).
        assert!(owned_secret("curie-owned", &[]).is_none());
    }

    #[test]
    fn a_connector_with_no_secrets_at_all_needs_no_secret_object() {
        assert!(owned_secret("", &[]).is_none());
    }

    #[test]
    fn secret_values_ride_string_data_never_argv() {
        // `kubectl create secret --from-literal` puts every credential in the
        // process table, readable by any local user off `ps`. This travels on
        // stdin instead, so the value appears in no argv at all.
        let mut v = std::collections::BTreeMap::new();
        v.insert("TOKEN".to_string(), "s3cret".to_string());
        let obj = render_secret("r-a-connector-secrets", "ns", &v);
        assert_eq!(obj["stringData"]["TOKEN"], "s3cret");
        assert!(!apply_args("ns").iter().any(|a| a.contains("s3cret")));
    }

    #[test]
    fn the_secret_is_kept_not_pruned_by_the_deploy_that_wrote_it() {
        let mut v = std::collections::BTreeMap::new();
        v.insert("TOKEN".to_string(), "x".to_string());
        let objs = vec![render_secret("r-a-connector-secrets", "ns", &v)];
        let keep = object_names(&label_objects(&objs, "a"));
        let args = prune_args("ns", "a", &keep);
        assert!(args
            .iter()
            .any(|a| a.contains("metadata.name!=r-a-connector-secrets")));
    }

    #[test]
    fn app_name_is_read_from_the_release_not_guessed() {
        // Rail 1's default-deny selects on the label the chart rendered. Taking
        // it from the cluster makes the connector's allow-rule match by
        // construction; a second derivation could disagree, and a NetworkPolicy
        // that selects nothing fails silently.
        let args = app_name_args("ns", "myrel");
        assert!(args.contains(&"app.kubernetes.io/instance=myrel".to_string()));
        assert!(args
            .iter()
            .any(|a| a.contains("app\\.kubernetes\\.io/name")));
    }

    #[test]
    fn apply_reads_from_stdin_so_nothing_touches_disk() {
        let args = apply_args("ns");
        assert_eq!(args.last().unwrap(), "-");
    }

    #[test]
    fn cluster_identity_changes_when_the_ca_changes() {
        let a = json!({
            "clusters": [{"cluster": {
                "server": "https://cluster-a.example.com",
                "certificate-authority-data": "Y2EtYQ=="
            }}]
        });
        let b = json!({
            "clusters": [{"cluster": {
                "server": "https://cluster-b.example.com",
                "certificate-authority-data": "Y2EtYg=="
            }}]
        });
        let id_a = cluster_identity_from_kubeconfig_view(&a).unwrap();
        let id_b = cluster_identity_from_kubeconfig_view(&b).unwrap();
        assert!(id_a.starts_with("ca:"));
        assert_ne!(id_a, id_b);
        assert!(!id_a.contains("example.com"));
        assert!(!id_a.contains("Y2Et"));
    }

    #[test]
    fn secret_key_names_ignore_values() {
        let obj = json!({
            "data": {"K8S_WRITE_KUBECONFIG": "dG9rZW4tYQ=="},
            "stringData": {"OTHER": "should-not-appear-as-a-logged-value"}
        });
        let keys = secret_key_names(&obj);
        assert!(keys.contains("K8S_WRITE_KUBECONFIG"));
        assert!(keys.contains("OTHER"));
        let rendered = format!("{keys:?}");
        assert!(!rendered.contains("dG9rZW4"));
        assert!(!rendered.contains("should-not-appear"));
    }

    #[test]
    fn get_secret_args_do_not_decode_values() {
        let args = get_secret_args("curie", "acme-bot-connector-secrets");
        assert!(args.contains(&"json".to_string()));
        assert!(!args.iter().any(|a| a.contains("go-template")));
    }

    fn with_isolated_store<T>(body: impl FnOnce() -> T) -> T {
        let _lock = crate::PROCESS_ENV_LOCK.blocking_lock();
        let dir = tempfile::tempdir().unwrap();
        let previous = std::env::var_os("CURIE_CONFIG_DIR");
        std::env::set_var("CURIE_CONFIG_DIR", dir.path());
        let result = body();
        match previous {
            Some(value) => std::env::set_var("CURIE_CONFIG_DIR", value),
            None => std::env::remove_var("CURIE_CONFIG_DIR"),
        }
        result
    }

    #[test]
    fn prepare_writes_the_matching_cluster_secret_and_refuses_the_other() {
        with_isolated_store(|| {
            let a = SecretScope {
                cluster_identity: "ca:a".into(),
                release: "curie".into(),
                namespace: "curie-test".into(),
            };
            let b = SecretScope {
                cluster_identity: "ca:b".into(),
                release: "curie".into(),
                namespace: "curie".into(),
            };
            crate::secrets::save_scoped_value("K8S_WRITE_KUBECONFIG", &a, "token-a", None).unwrap();
            let prepared = prepare(
                &[],
                &BTreeMap::new(),
                "acme-bot-connector-secrets",
                &["K8S_WRITE_KUBECONFIG".to_string()],
                &a,
                "acme-bot",
                &BTreeMap::new(),
            )
            .unwrap();
            let intent = prepared.write_intent().unwrap();
            assert!(intent.contains("K8S_WRITE_KUBECONFIG"));
            assert!(!intent.contains("token-a"));
            assert_eq!(
                prepared.source_lines(),
                vec!["K8S_WRITE_KUBECONFIG: scoped stored secret (version 1)".to_string()]
            );
            let err = prepare(
                &[],
                &BTreeMap::new(),
                "acme-bot-connector-secrets",
                &["K8S_WRITE_KUBECONFIG".to_string()],
                &b,
                "acme-bot",
                &BTreeMap::new(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("refusing to inject"));
            assert!(!err.contains("token-a"));
        });
    }
}

// --------------------------------------------------------------------------- #
// Execution
// --------------------------------------------------------------------------- #

/// Run a kubectl argv, optionally writing `stdin` to it, capturing output.
///
/// Separate from `ops::run_capture` because that has no stdin channel, and
/// stdin is the whole point here: the applied document carries credentials, so
/// it must not become a `-f <file>` on disk or a `--from-literal` in argv.
async fn run(argv: &[String], stdin: Option<&str>) -> Result<(bool, String, String)> {
    use tokio::io::AsyncWriteExt;
    let (program, args) = argv.split_first().context("empty command")?;
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(if stdin.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        });
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to invoke `{program}`; is it on PATH?"))?;
    if let Some(doc) = stdin {
        let mut pipe = child.stdin.take().context("stdin pipe missing")?;
        pipe.write_all(doc.as_bytes()).await?;
        pipe.shutdown().await?;
    }
    let out = child.wait_with_output().await?;
    Ok((
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    ))
}

/// What a connector sync did, for the deploy summary.
#[derive(Debug, Default)]
pub struct ConnectorSync {
    pub applied: Vec<String>,
    pub urls: BTreeMap<String, String>,
    /// Connector secret keys this deploy wrote. Names only, never values.
    pub written_keys: Vec<String>,
    /// Live Secret keys this deploy is replacing. Names only, never values.
    pub replaced_keys: Vec<String>,
    /// Hosted Deployments with no matching mcp_entry URL (#2352).
    ///
    /// Declaration-layer only: not rollout readiness (#2350) and not MCP
    /// capability discovery (#2519). Empty means every hosted connector was
    /// declared mounted.
    pub hosted_unmounted: Vec<String>,
}

/// Discover the release's rendered app name (the chart's nameOverride).
pub async fn discover_app_name(target: &ClusterTarget) -> Result<String> {
    let (ok, out, err) = run(
        &target.args(&app_name_args(
            &target.scope.namespace,
            &target.scope.release,
        )),
        None,
    )
    .await?;
    let name = out.trim();
    if !ok || name.is_empty() {
        anyhow::bail!(
            "could not read the app name for release {} in namespace {}: no \
             Deployment carries app.kubernetes.io/instance={}. Connectors need it to \
             target the sandbox pods Rail 1 denies by default. Confirm the release is installed \
             with `curie cluster status`.{}",
            target.scope.release,
            target.scope.namespace,
            target.scope.release,
            if err.trim().is_empty() {
                String::new()
            } else {
                format!(" kubectl said: {}", err.trim())
            }
        );
    }
    Ok(name.to_string())
}

/// Resolve each declared credential against the cluster target.
///
/// A matching scoped store entry is required once anything is stored under that
/// name. Process environment is a first-run fallback only when the store has no
/// entry at all, and the source is returned so deploy can say so without printing
/// the value. Fails naming every gap at once rather than one per run.
fn resolve_secret_values(
    keys: &[String],
    target: &SecretScope,
    overrides: &BTreeMap<String, String>,
) -> Result<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let mut values = BTreeMap::new();
    let mut sources = BTreeMap::new();
    let mut missing = Vec::new();
    for k in keys {
        if let Some(value) = overrides.get(k) {
            if value.is_empty() {
                missing.push(k.clone());
            } else {
                sources.insert(k.clone(), "explicit deploy input".to_string());
                values.insert(k.clone(), value.clone());
            }
            continue;
        }
        match crate::secrets::resolve_cluster_secret(k, target)? {
            Some(resolved) => {
                sources.insert(k.clone(), resolved.source_label());
                values.insert(k.clone(), resolved.value);
            }
            None => missing.push(k.clone()),
        }
    }
    if !missing.is_empty() {
        return Err(crate::exit::usage(format!(
            "connectors.yaml declares secret(s) with no value available for {}: {}. Export each in \
             the environment, or store it for this target with `curie secrets set <NAME> \
             --from-env <NAME> --cluster-identity {} --release {} --namespace {}`.",
            target.describe(),
            missing.join(", "),
            target.cluster_identity,
            target.release,
            target.namespace
        )));
    }
    Ok((values, sources))
}

/// An agent's fully resolved connector synchronization plan.
#[derive(Debug)]
pub struct PreparedConnectorSync {
    namespace: String,
    agent_name: String,
    keep: Vec<String>,
    apply_document: Option<String>,
    result: ConnectorSync,
    secret_name: Option<String>,
    secret_keys: Vec<String>,
    /// The connector-owned secret VALUES as resolved for THIS cluster scope
    /// (#1913). Retained so `cluster deploy` binds the sandbox with the very
    /// same credential the connector pod receives rather than re-resolving the
    /// name from the environment or unscoped host storage, which can differ.
    /// Private: nothing outside this module may take ownership of a value.
    secret_values: BTreeMap<String, String>,
    secret_sources: BTreeMap<String, String>,
    target: SecretScope,
    bound_target: Option<ClusterTarget>,
}

impl PreparedConnectorSync {
    pub fn bind_target(mut self, target: ClusterTarget) -> Result<Self> {
        if self.target != target.scope {
            anyhow::bail!("prepared connector scope does not match captured Kubernetes target");
        }
        self.bound_target = Some(target);
        Ok(self)
    }

    /// The connector-owned secrets, keyed by NAME, with the cluster-scoped
    /// value already resolved for the deploy's target. Read by `cluster deploy`
    /// to bind those names into the agent sandbox (#2503) with the scoped value
    /// -- one cluster, one credential (#1913) -- instead of resolving the name
    /// a second time from a possibly stale environment.
    pub fn owned_secret_values(&self) -> &BTreeMap<String, String> {
        &self.secret_values
    }

    pub fn write_intent(&self) -> Option<String> {
        let secret_name = self.secret_name.as_deref()?;
        Some(write_intent_line(
            secret_name,
            &self.secret_keys,
            &self.target,
        ))
    }

    pub fn source_lines(&self) -> Vec<String> {
        self.secret_sources
            .iter()
            .map(|(key, source)| format!("{key}: {source}"))
            .collect()
    }

    /// Hosted connectors with no matching mcp_entry, recorded at prepare.
    pub fn hosted_unmounted(&self) -> &[String] {
        &self.result.hosted_unmounted
    }
}

/// Discover the current kubeconfig's cluster identity.
pub async fn discover_cluster_identity() -> Result<String> {
    let (ok, out, err) = run(&kubeconfig_view_args(), None).await?;
    if !ok {
        anyhow::bail!(
            "could not read the current kubeconfig cluster identity: {}",
            err.trim()
        );
    }
    let view: Value = serde_json::from_str(&out)
        .context("parsing `kubectl config view --minify --raw -o json`")?;
    cluster_identity_from_kubeconfig_view(&view)
}

/// Snapshot the selected kubeconfig at prepare time. The private temporary file
/// lives exactly as long as the connector plan that consumes it.
pub async fn bind_current_cluster(namespace: &str, release: &str) -> Result<ClusterTarget> {
    let (ok, out, err) = run(&kubeconfig_view_args(), None).await?;
    if !ok {
        anyhow::bail!("could not capture the current kubeconfig: {}", err.trim());
    }
    let view: Value = serde_json::from_str(&out)
        .context("parsing `kubectl config view --minify --raw -o json`")?;
    let cluster_identity = cluster_identity_from_kubeconfig_view(&view)?;
    let mut kubeconfig = tempfile::NamedTempFile::new()
        .context("creating private kubeconfig snapshot for connector deploy")?;
    kubeconfig.write_all(out.as_bytes())?;
    kubeconfig.flush()?;
    Ok(ClusterTarget {
        scope: SecretScope {
            cluster_identity,
            release: release.into(),
            namespace: namespace.into(),
        },
        kubeconfig: Arc::new(kubeconfig),
    })
}

/// Resolve and render an agent's connector objects without writing to kubectl.
pub fn prepare(
    manifests: &[Value],
    mcp_entries: &BTreeMap<String, Value>,
    owned_secret_name: &str,
    owned_secret_keys: &[String],
    target: &SecretScope,
    agent_name: &str,
    secret_overrides: &std::collections::BTreeMap<String, String>,
) -> Result<PreparedConnectorSync> {
    let mut objects = Vec::new();
    let mut secret_name = None;
    let mut secret_keys = Vec::new();
    let mut secret_values = BTreeMap::new();
    let mut secret_sources = BTreeMap::new();

    if let Some((name, keys)) = owned_secret(owned_secret_name, owned_secret_keys) {
        let (values, sources) = resolve_secret_values(&keys, target, secret_overrides)?;
        objects.push(render_secret(&name, &target.namespace, &values));
        secret_name = Some(name);
        secret_keys = keys;
        secret_values = values;
        secret_sources = sources;
    }
    objects.extend_from_slice(manifests);

    let mut result = ConnectorSync {
        written_keys: secret_keys.clone(),
        hosted_unmounted: hosted_unmounted_connectors(
            manifests,
            mcp_entries,
            &target.release,
            agent_name,
        ),
        ..Default::default()
    };
    for (name, entry) in mcp_entries {
        if let Some(url) = entry.get("url").and_then(|u| u.as_str()) {
            result.urls.insert(name.clone(), url.to_string());
        }
    }

    let labelled = label_objects(&objects, agent_name);
    let keep = object_names(&labelled);
    let apply_document = if labelled.is_empty() {
        None
    } else {
        Some(as_list_document(&labelled)?)
    };

    Ok(PreparedConnectorSync {
        namespace: target.namespace.clone(),
        agent_name: agent_name.to_string(),
        keep,
        apply_document,
        result,
        secret_name,
        secret_keys,
        secret_values,
        secret_sources,
        target: target.clone(),
        bound_target: None,
    })
}

/// Apply a prepared connector plan, and prune what it no longer declares.
///
/// Called after the bundle is deployed, so the objects exist before the next
/// turn reaches for them.
pub async fn sync(prepared: PreparedConnectorSync) -> Result<ConnectorSync> {
    let ui = crate::ui::ui();
    let PreparedConnectorSync {
        namespace,
        agent_name,
        keep,
        apply_document,
        mut result,
        secret_name,
        secret_keys,
        secret_values: _,
        secret_sources,
        target,
        bound_target,
    } = prepared;
    let bound_target = bound_target.context("connector sync has no captured Kubernetes target")?;

    if let Some(name) = secret_name.as_deref() {
        ui.note(&write_intent_line(name, &secret_keys, &target));
        for (key, source) in &secret_sources {
            ui.note(&format!("{key}: {source}"));
        }
        let existing = inspect_secret_keys(&bound_target, &namespace, name).await?;
        let replaced = keys_being_replaced(&existing, &secret_keys);
        if !replaced.is_empty() {
            ui.warn(&replacement_warning_line(name, &replaced));
        }
        result.replaced_keys = replaced;
    }

    if let Some(doc) = apply_document {
        bound_target.revalidate_ambient_binding().await?;
        let (ok, _out, err) = run(&bound_target.args(&apply_args(&namespace)), Some(&doc)).await?;
        if !ok {
            anyhow::bail!("applying connectors failed: {}", err.trim());
        }
        result.applied = keep.clone();
        ui.note(&format!(
            "connectors: applied {} object(s) for {agent_name}",
            keep.len()
        ));
    }

    // Runs even with nothing declared -- that is the case where a connector was
    // REMOVED, and the whole reason this is not a bare `kubectl apply`.
    bound_target.revalidate_ambient_binding().await?;
    let (ok, _out, err) = run(
        &bound_target.args(&prune_args(&namespace, &agent_name, &keep)),
        None,
    )
    .await?;
    if !ok {
        ui.warn(&format!(
            "connectors: pruning stale objects for {agent_name} failed: {}",
            err.trim()
        ));
    }
    Ok(result)
}
/// Render and reconcile the connectors declared by one deployed version.
///
/// The API owns rendering and the CLI owns applying under the operator's
/// kubectl identity. Callers must always state their locally owned secret
/// overrides explicitly, including the normal deploy path's empty map.
pub async fn sync_deployed_version(
    api_url: &str,
    api_key: &str,
    namespace: &str,
    release: &str,
    deployed: &crate::commands::DeployOutput,
    secret_overrides: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let target = bind_current_cluster(namespace, release).await?;
    let app_name = discover_app_name(&target).await?;
    let client = crate::api::ApiClient::new(api_url, api_key)?;
    let rendered = client
        .version_connectors(
            &deployed.agent_id,
            &deployed.version_id,
            release,
            namespace,
            &app_name,
        )
        .await?;
    let prepared = prepare(
        &rendered.manifests,
        &rendered.mcp_entries,
        &rendered.owned_secret_name,
        &rendered.owned_secret_keys,
        &target.scope,
        &deployed.agent_name,
        secret_overrides,
    )?
    .bind_target(target)?;
    let synced = sync(prepared).await?;
    report_connector_sync(&synced);
    Ok(())
}

async fn inspect_secret_keys(
    target: &ClusterTarget,
    namespace: &str,
    name: &str,
) -> Result<BTreeSet<String>> {
    let (ok, out, err) = run(&target.args(&get_secret_args(namespace, name)), None).await?;
    if !ok {
        if err.contains("NotFound") || err.contains("not found") {
            return Ok(BTreeSet::new());
        }
        anyhow::bail!(
            "could not inspect existing connector Secret {name}: {}; refusing to overwrite it",
            err.trim()
        );
    }
    let obj = serde_json::from_str::<Value>(&out)
        .context("parsing existing connector Secret JSON; refusing to overwrite it")?;
    Ok(secret_key_names(&obj))
}

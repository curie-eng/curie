//! Explicit Kubernetes context for `curie cluster` (#2723).
//!
//! Every `curie cluster` verb spawns `helm` and `kubectl` with the process env. This
//! module resolves the target context once, at dispatch, and pins it for every child:
//! a small kubeconfig file that only sets `current-context` goes FIRST in `KUBECONFIG`
//! (kubectl and helm take current-context from the first file that sets it), and
//! `HELM_KUBECONTEXT` is set to the same name so an ambient value cannot split helm
//! from kubectl.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};

/// The resolved context and the cluster it points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubeTarget {
    pub context: String,
    pub cluster: String,
}

/// Choose the target from a merged kubeconfig in `kubectl config view -o json` shape.
///
/// An explicit name must exist in `.contexts[].name`; otherwise the error names it and
/// lists the available contexts. Without one, the current-context is used, and `None`
/// means there is nothing to pin.
pub fn select_target(
    view: &serde_json::Value,
    explicit: Option<&str>,
) -> Result<Option<KubeTarget>> {
    let contexts: Vec<&serde_json::Value> = view
        .get("contexts")
        .and_then(|c| c.as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    let cluster_of = |name: &str| {
        contexts
            .iter()
            .find(|c| c.get("name").and_then(|n| n.as_str()) == Some(name))
            .map(|c| {
                c.pointer("/context/cluster")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            })
    };
    match explicit {
        Some(name) => match cluster_of(name) {
            Some(cluster) => Ok(Some(KubeTarget {
                context: name.to_string(),
                cluster,
            })),
            None => {
                let names: Vec<&str> = contexts
                    .iter()
                    .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
                    .collect();
                bail!(
                    "Kubernetes context {name:?} is not in the kubeconfig; available contexts: {}",
                    if names.is_empty() {
                        "(none)".to_string()
                    } else {
                        names.join(", ")
                    }
                )
            }
        },
        None => {
            let current = view
                .get("current-context")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if current.is_empty() {
                return Ok(None);
            }
            Ok(Some(KubeTarget {
                context: current.to_string(),
                cluster: cluster_of(current).unwrap_or_default(),
            }))
        }
    }
}

/// The env every helm/kubectl child needs: `KUBECONFIG` with the pin file first, then
/// the existing list (or `$HOME/.kube/config` when unset or empty), and
/// `HELM_KUBECONTEXT` set to the context, overriding any ambient value.
pub fn pinned_kubeconfig_env(
    pin_file: &Path,
    target: &KubeTarget,
    existing_kubeconfig: Option<OsString>,
    home: Option<&Path>,
) -> Result<Vec<(String, OsString)>> {
    let mut paths = vec![pin_file.to_path_buf()];
    match existing_kubeconfig.filter(|v| !v.is_empty()) {
        Some(list) => paths.extend(std::env::split_paths(&list)),
        None => {
            if let Some(home) = home {
                paths.push(home.join(".kube").join("config"));
            }
        }
    }
    let kubeconfig = std::env::join_paths(paths).context("building the pinned KUBECONFIG list")?;
    Ok(vec![
        ("KUBECONFIG".to_string(), kubeconfig),
        (
            "HELM_KUBECONTEXT".to_string(),
            OsString::from(&target.context),
        ),
    ])
}

/// The pin kubeconfig body. The context name is written as a JSON string, which is a
/// valid YAML double-quoted scalar for any name.
pub fn pin_file_contents(context: &str) -> String {
    let quoted = serde_json::to_string(context).unwrap_or_else(|_| "\"\"".to_string());
    format!("apiVersion: v1\nkind: Config\ncurrent-context: {quoted}\n")
}

/// Deterministic pin path, so repeated runs reuse one file per context.
pub fn pin_file_path(temp_dir: &Path, uid: u32, context: &str) -> PathBuf {
    let digest = Sha256::digest(context.as_bytes());
    let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
    temp_dir
        .join(format!("curie-kube-context-{uid}"))
        .join(format!("{hex}.yaml"))
}

fn current_uid() -> u32 {
    use std::os::unix::fs::MetadataExt;
    // `/proc/self` is Linux only; on macOS the owner of `$HOME` names the same user.
    std::fs::metadata("/proc/self")
        .or_else(|_| std::fs::metadata(std::env::var_os("HOME").unwrap_or_default()))
        .map(|m| m.uid())
        .unwrap_or(0)
}

fn write_pin_file(path: &Path, context: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("pin file has no parent directory"))?;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let tmp = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("pin"),
        std::process::id()
    ));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("writing {}", tmp.display()))?;
        f.write_all(pin_file_contents(context).as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("installing {}", path.display()))?;
    Ok(())
}

/// The kubeconfig files kubectl would load: the `KUBECONFIG` list, or
/// `$HOME/.kube/config` when it is unset or empty.
pub fn kubeconfig_paths(existing: Option<OsString>, home: Option<&Path>) -> Vec<PathBuf> {
    match existing.filter(|v| !v.is_empty()) {
        Some(list) => std::env::split_paths(&list)
            .filter(|p| !p.as_os_str().is_empty())
            .collect(),
        None => home
            .map(|h| vec![h.join(".kube").join("config")])
            .unwrap_or_default(),
    }
}

/// Merge kubeconfig documents the way client-go does for the fields this module reads:
/// the first file that sets `current-context` wins, and the first file that defines a
/// context name wins. The result has the `kubectl config view -o json` shape.
pub fn merge_kubeconfigs(documents: &[serde_json::Value]) -> serde_json::Value {
    let mut current = String::new();
    let mut contexts: Vec<serde_json::Value> = Vec::new();
    for doc in documents {
        if current.is_empty() {
            if let Some(c) = doc.get("current-context").and_then(|v| v.as_str()) {
                current = c.to_string();
            }
        }
        for ctx in doc
            .get("contexts")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let name = ctx.get("name").and_then(|n| n.as_str());
            let seen = contexts
                .iter()
                .any(|c| c.get("name").and_then(|n| n.as_str()) == name);
            if name.is_some() && !seen {
                contexts.push(ctx.clone());
            }
        }
    }
    serde_json::json!({ "current-context": current, "contexts": contexts })
}

/// Read the kubeconfig without spawning anything, so offline and fully explicit
/// commands still never invoke kubectl. Missing files are skipped, as kubectl does.
fn read_kubeconfig() -> Result<serde_json::Value> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let mut documents = Vec::new();
    for path in kubeconfig_paths(std::env::var_os("KUBECONFIG"), home.as_deref()) {
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        let doc: serde_json::Value = if raw.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_norway::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?
        };
        documents.push(doc);
    }
    Ok(merge_kubeconfigs(&documents))
}

/// Context names in the operator's kubeconfig, for a remedy that names them.
/// Best effort: an unreadable kubeconfig yields an empty list.
pub fn available_contexts() -> Vec<String> {
    read_kubeconfig()
        .ok()
        .and_then(|view| view.get("contexts").and_then(|c| c.as_array()).cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(str::to_string))
        .collect()
}

/// Resolve and pin the Kubernetes context for this `curie cluster` process.
///
/// With an explicit name, any failure to read the kubeconfig or an unknown name is an
/// error. Without one, a failure or an absent current-context returns `Ok(None)` and
/// leaves the env untouched.
pub fn pin_for_cluster_command(explicit: Option<&str>) -> Result<Option<KubeTarget>> {
    let view = match read_kubeconfig() {
        Ok(v) => v,
        Err(e) => match explicit {
            Some(name) => {
                return Err(e.context(format!(
                    "cannot resolve Kubernetes context {name:?}; available contexts: unknown"
                )))
            }
            None => return Ok(None),
        },
    };
    let Some(target) = select_target(&view, explicit)? else {
        return Ok(None);
    };
    let pin = pin_file_path(&std::env::temp_dir(), current_uid(), &target.context);
    write_pin_file(&pin, &target.context)?;
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let env = pinned_kubeconfig_env(
        &pin,
        &target,
        std::env::var_os("KUBECONFIG"),
        home.as_deref(),
    )?;
    for (key, value) in env {
        // Called once at dispatch, before the verb spawns any child or starts any task
        // that reads the environment.
        std::env::set_var(key, value);
    }
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn view() -> serde_json::Value {
        json!({
            "current-context": "prod-ctx",
            "contexts": [
                {"name": "prod-ctx", "context": {"cluster": "prod-cluster"}},
                {"name": "test-ctx", "context": {"cluster": "test-cluster"}}
            ]
        })
    }

    #[test]
    fn merge_takes_the_first_current_context_and_first_context_definition() {
        let pin = json!({"current-context": "test-ctx"});
        let a = json!({"current-context": "prod-ctx", "contexts": [
            {"name": "test-ctx", "context": {"cluster": "first"}}]});
        let b = json!({"contexts": [
            {"name": "test-ctx", "context": {"cluster": "second"}},
            {"name": "prod-ctx", "context": {"cluster": "prod-cluster"}}]});
        let merged = merge_kubeconfigs(&[pin, a, b]);
        let t = select_target(&merged, None).unwrap().unwrap();
        assert_eq!(
            (t.context.as_str(), t.cluster.as_str()),
            ("test-ctx", "first")
        );
        assert!(select_target(&merged, Some("prod-ctx")).unwrap().is_some());
    }

    #[test]
    fn kubeconfig_paths_fall_back_to_home_only_when_unset_or_empty() {
        let home = Path::new("/h");
        assert_eq!(
            kubeconfig_paths(None, Some(home)),
            vec![PathBuf::from("/h/.kube/config")]
        );
        assert_eq!(
            kubeconfig_paths(Some(OsString::new()), Some(home)),
            vec![PathBuf::from("/h/.kube/config")]
        );
        let list = std::env::join_paths(["/a", "/b"]).unwrap();
        assert_eq!(
            kubeconfig_paths(Some(list), Some(home)),
            vec![PathBuf::from("/a"), PathBuf::from("/b")]
        );
    }

    #[test]
    fn explicit_known_context_is_selected() {
        let t = select_target(&view(), Some("test-ctx")).unwrap().unwrap();
        assert_eq!(t.context, "test-ctx");
        assert_eq!(t.cluster, "test-cluster");
    }

    #[test]
    fn explicit_unknown_context_errors_and_lists_available() {
        let err = select_target(&view(), Some("nope"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("\"nope\""), "{err}");
        assert!(err.contains("prod-ctx, test-ctx"), "{err}");
    }

    #[test]
    fn implicit_uses_current_context() {
        let t = select_target(&view(), None).unwrap().unwrap();
        assert_eq!(t.context, "prod-ctx");
        assert_eq!(t.cluster, "prod-cluster");
    }

    #[test]
    fn implicit_without_current_context_is_none() {
        let mut v = view();
        v["current-context"] = json!("");
        assert_eq!(select_target(&v, None).unwrap(), None);
        v.as_object_mut().unwrap().remove("current-context");
        assert_eq!(select_target(&v, None).unwrap(), None);
    }

    fn target() -> KubeTarget {
        KubeTarget {
            context: "test-ctx".into(),
            cluster: "test-cluster".into(),
        }
    }

    #[test]
    fn env_prepends_pin_to_existing_list_and_sets_helm_context() {
        let existing = std::env::join_paths(["/a/config", "/b/config"]).unwrap();
        let env = pinned_kubeconfig_env(
            Path::new("/pin.yaml"),
            &target(),
            Some(existing),
            Some(Path::new("/home/u")),
        )
        .unwrap();
        let kc: Vec<PathBuf> = std::env::split_paths(&env[0].1).collect();
        assert_eq!(env[0].0, "KUBECONFIG");
        assert_eq!(
            kc,
            vec![
                PathBuf::from("/pin.yaml"),
                PathBuf::from("/a/config"),
                PathBuf::from("/b/config")
            ]
        );
        assert_eq!(env[1], ("HELM_KUBECONTEXT".into(), "test-ctx".into()));
    }

    #[test]
    fn env_defaults_to_home_kubeconfig_when_unset_or_empty() {
        for existing in [None, Some(OsString::new())] {
            let env = pinned_kubeconfig_env(
                Path::new("/pin.yaml"),
                &target(),
                existing,
                Some(Path::new("/home/u")),
            )
            .unwrap();
            let kc: Vec<PathBuf> = std::env::split_paths(&env[0].1).collect();
            assert_eq!(
                kc,
                vec![
                    PathBuf::from("/pin.yaml"),
                    PathBuf::from("/home/u/.kube/config")
                ]
            );
        }
    }

    #[test]
    fn helm_kubecontext_is_the_target_not_ambient() {
        // The builder never consults ambient HELM_KUBECONTEXT; the target always wins.
        let env = pinned_kubeconfig_env(Path::new("/p"), &target(), None, None).unwrap();
        let helm = env.iter().find(|(k, _)| k == "HELM_KUBECONTEXT").unwrap();
        assert_eq!(helm.1, OsString::from("test-ctx"));
    }

    #[test]
    fn pin_contents_quote_the_name_and_path_is_stable() {
        let body = pin_file_contents("we: ird\"ctx");
        assert!(
            body.contains("current-context: \"we: ird\\\"ctx\"\n"),
            "{body}"
        );
        let a = pin_file_path(Path::new("/tmp"), 7, "ctx");
        assert_eq!(a, pin_file_path(Path::new("/tmp"), 7, "ctx"));
        assert_ne!(a, pin_file_path(Path::new("/tmp"), 7, "ctx2"));
        assert!(a.starts_with("/tmp/curie-kube-context-7"));
    }
}

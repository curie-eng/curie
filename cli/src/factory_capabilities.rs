//! Observed, bundle-bound capabilities for factory admission (#4485).

use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const INPUT_PATH: &str = "verification/capability-inputs.json";
pub const MANIFEST_PATH: &str = "verification/capabilities.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeSpec {
    pub name: String,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInputs {
    pub required_executables: Vec<ProbeSpec>,
    pub registry_hosts: Vec<String>,
    pub services: Vec<String>,
    pub egress_class: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Executable {
    pub name: String,
    pub version: String,
    pub path: String,
    pub command: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Verification {
    source: String,
    checks: Vec<serde_json::Value>,
    lockfile_installs: bool,
}

#[derive(Debug, Serialize)]
pub struct Publication {
    github_directory_writable: bool,
    code_host_credential_in_sandbox: bool,
}

#[derive(Debug, Serialize)]
pub struct CapabilityManifest {
    schema_version: u32,
    runner_image: String,
    bundle_input_sha256: String,
    executables: Vec<Executable>,
    registry_hosts: Vec<String>,
    services: Vec<String>,
    connectors: Vec<String>,
    verification: Verification,
    egress_class: String,
    publication: Publication,
}

/// Digest the actual files the uploader packs. The manifest itself is excluded
/// to avoid a self-reference; the API binds the finished archive separately.
pub fn bundle_input_digest(dir: &Path) -> Result<String> {
    let archive = crate::bundle::pack_tar_gz(dir)?;
    let mut entries = Vec::new();
    for item in tar::Archive::new(GzDecoder::new(archive.as_slice())).entries()? {
        let mut item = item?;
        if !item.header().entry_type().is_file() {
            continue;
        }
        let name = item
            .path()?
            .to_string_lossy()
            .trim_start_matches("./")
            .to_string();
        if name == MANIFEST_PATH {
            continue;
        }
        let mut bytes = Vec::new();
        item.read_to_end(&mut bytes)?;
        entries.push((name, bytes));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut sha = Sha256::new();
    for (name, bytes) in entries {
        sha.update((name.len() as u64).to_be_bytes());
        sha.update(name.as_bytes());
        sha.update((bytes.len() as u64).to_be_bytes());
        sha.update(bytes);
    }
    Ok(sha
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub fn manifest_from_observations(
    inputs: &CapabilityInputs,
    observed: Vec<Executable>,
    runner_image: &str,
    bundle_input_sha256: String,
    connectors: Vec<String>,
    bundle_checks: Option<(Vec<serde_json::Value>, bool)>,
) -> Result<CapabilityManifest> {
    if !runner_image.contains("@sha256:") && !runner_image.starts_with("sha256:") {
        bail!("runner image is not digest-pinned: {runner_image}");
    }
    let mut names = BTreeSet::new();
    for probe in &inputs.required_executables {
        if probe.name.trim().is_empty() || probe.command.is_empty() || !names.insert(&probe.name) {
            bail!(
                "invalid or duplicate required executable probe {:?}",
                probe.name
            );
        }
        if !observed.iter().any(|tool| {
            tool.name == probe.name
                && tool.command == probe.command
                && !tool.version.is_empty()
                && !tool.path.is_empty()
        }) {
            bail!(
                "required tool {} could not be observed with command {}",
                probe.name,
                probe.command.join(" ")
            );
        }
    }
    let verification = match bundle_checks {
        Some((checks, lockfile_installs)) if !checks.is_empty() => Verification {
            source: "bundle".into(),
            checks,
            lockfile_installs,
        },
        Some((_, lockfile_installs)) => Verification {
            source: "repository".into(),
            checks: Vec::new(),
            lockfile_installs,
        },
        None => Verification {
            source: "repository".into(),
            checks: Vec::new(),
            lockfile_installs: false,
        },
    };
    Ok(CapabilityManifest {
        schema_version: 1,
        runner_image: runner_image.into(),
        bundle_input_sha256,
        executables: observed,
        registry_hosts: inputs.registry_hosts.clone(),
        services: inputs.services.clone(),
        connectors,
        verification,
        egress_class: inputs.egress_class.clone(),
        publication: Publication {
            github_directory_writable: false,
            code_host_credential_in_sandbox: false,
        },
    })
}

pub const PROBE_SCRIPT: &str = r#"import json, re, shutil, subprocess, sys
out = []
for probe in json.loads(sys.argv[1]):
    command = probe['command']
    path = shutil.which(command[0])
    if path is None:
        print(f"required tool {probe['name']} ({' '.join(command)}) is not on PATH", file=sys.stderr)
        sys.exit(1)
    try:
        done = subprocess.run([path, *command[1:]], capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"required tool {probe['name']} ({' '.join(command)}) failed: {error}", file=sys.stderr)
        sys.exit(1)
    if done.returncode != 0:
        print(f"required tool {probe['name']} ({' '.join(command)}) failed with exit {done.returncode}", file=sys.stderr)
        sys.exit(1)
    output = done.stdout + '\n' + done.stderr
    match = re.search(r'\d+(?:\.\d+)+', output)
    if match is None:
        print(f"required tool {probe['name']} ({' '.join(command)}) printed no version", file=sys.stderr)
        sys.exit(1)
    out.append({'name': probe['name'], 'version': match.group(0), 'path': path, 'command': command})
print(json.dumps(out, sort_keys=True))"#;

pub async fn probe_image(image: &str, probes: &[ProbeSpec]) -> Result<Vec<Executable>> {
    let encoded = serde_json::to_string(probes)?;
    let args = vec![
        "run".into(),
        "--rm".into(),
        "--network".into(),
        "none".into(),
        "--entrypoint".into(),
        "python3".into(),
        image.into(),
        "-c".into(),
        PROBE_SCRIPT.into(),
        encoded,
    ];
    let (status, stdout, stderr) = crate::docker::docker_capture(&args).await?;
    if !status.success() {
        bail!("runner image capability probe failed: {stderr}");
    }
    serde_json::from_str(&stdout).context("parse runner image capability probe")
}

pub async fn generate(dir: &Path, image: &str) -> Result<()> {
    let input_path = dir.join(INPUT_PATH);
    let inputs: CapabilityInputs = serde_json::from_slice(
        &std::fs::read(&input_path).with_context(|| format!("read {}", input_path.display()))?,
    )
    .with_context(|| format!("parse {}", input_path.display()))?;
    let observed = probe_image(image, &inputs.required_executables).await?;
    let decl = crate::connector_build::load(dir)?;
    let connectors = decl.connectors.keys().cloned().collect();
    let checks_path = dir.join("verification/checks.json");
    let bundle_checks = if checks_path.exists() {
        let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(&checks_path)?)?;
        let checks = raw
            .get("checks")
            .and_then(serde_json::Value::as_array)
            .context("verification/checks.json checks must be an array")?
            .clone();
        let lockfile_installs = raw
            .get("lockfile_installs")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Some((checks, lockfile_installs))
    } else {
        None
    };
    let digest = bundle_input_digest(dir)?;
    let manifest =
        manifest_from_observations(&inputs, observed, image, digest, connectors, bundle_checks)?;
    let path = dir.join(MANIFEST_PATH);
    std::fs::create_dir_all(path.parent().context("manifest has no parent")?)?;
    std::fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(&manifest)?),
    )
    .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

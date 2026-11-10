//! `curie seal`: encrypt a connector credential to a cluster (ADR-0094).
//!
//! The author-facing half of sealed secrets. Everything about the format lives
//! in [`crate::sealing`]; this module is the ergonomics — find the cluster's
//! public key, take the value without it touching a shell history, and hand
//! back something that can be pasted into `connectors.yaml`.
//!
//! Two ways to reach a public key, and the second is not a convenience:
//!
//! - **From the cluster** (default). Reads the private key from the release and
//!   derives the public half, so the two can never disagree.
//! - **From `--public-key`**. An agent author sealing a credential need not have
//!   cluster access at all — that is rather the point of publishing a public
//!   key. Requiring kubectl would put every author in the operator's shoes and
//!   quietly undo the separation ADR-0094 is buying.

use std::io::{self, IsTerminal, Read, Write};

use anyhow::{bail, Context, Result};

/// Where a sealed value is written in `connectors.yaml`, for the paste snippet.
fn snippet(env_name: &str, blob: &str) -> String {
    format!("    sealed_secrets:\n      {env_name}: {blob}")
}

pub struct SealOpts {
    pub connector: String,
    pub env_name: String,
    pub namespace: String,
    pub release: String,
    /// Seal against this public key instead of reading one from a cluster.
    pub public_key: Option<String>,
    /// Read the value from this environment variable instead of prompting.
    pub from_env: Option<String>,
}

#[derive(Debug)]
pub struct SealOutput {
    pub connector: String,
    pub env_name: String,
    pub sealed: String,
    /// The public key it was sealed to, so the author can confirm which cluster
    /// can open it. Not a secret: publishing it is the entire design.
    pub public_key: String,
}

impl crate::ui::CliOutput for SealOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "connector": self.connector,
            "env_name": self.env_name,
            "sealed": self.sealed,
            "public_key": self.public_key,
            "yaml": snippet(&self.env_name, &self.sealed),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload_plain(&snippet(&self.env_name, &self.sealed));
        ui.note(&format!(
            "sealed to public key {}. Only a cluster holding the matching private \
             key can read it, so this is safe to commit. The block above belongs under \
             connectors.{} in connectors.yaml, but connector validation rejects \
             sealed_secrets until #1434 lands, so it cannot be used yet.",
            self.public_key, self.connector
        ));
    }
}

/// Exact connector names Curie keeps for its own platform MCP servers: `curie`
/// is the approval server, `curie-state` the durable state server, and
/// `curie-slack` the granted Slack capability server.
///
/// Shared connector-name cases exercise both the plugin-format validator and
/// this CLI validator through their production boundaries.
const RESERVED_CONNECTOR_NAMES: [&str; 3] = ["curie", "curie-state", "curie-slack"];

/// A connector name becomes a Kubernetes resource name component.
fn validate_connector(name: &str) -> Result<()> {
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let valid = !name.is_empty()
        && name.len() <= 40
        && name
            .bytes()
            .all(|byte| is_alphanumeric(byte) || byte == b'-')
        && name
            .as_bytes()
            .first()
            .is_some_and(|byte| is_alphanumeric(*byte))
        && name
            .as_bytes()
            .last()
            .is_some_and(|byte| is_alphanumeric(*byte));
    if !valid {
        return Err(crate::exit::CliError::usage(format!(
            "connector name `{name}` must be a lower case RFC 1123 label of at most 40 characters"
        ))
        .with_fix("use lower case letters, digits, and internal hyphens, for example grafana")
        .into());
    }
    if RESERVED_CONNECTOR_NAMES.contains(&name) {
        return Err(crate::exit::CliError::usage(format!(
            "connector name `{name}` is reserved for Curie's own platform MCP server"
        ))
        .with_fix(format!("rename the connector, for example `{name}-tools`"))
        .into());
    }
    Ok(())
}

/// An env var name, held to the same shape `curie secrets set` requires.
///
/// The value becomes an environment variable inside the connector container, so
/// a name the shell cannot export is a deploy-time failure discovered long
/// after the seal looked fine.
fn validate_env_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit());
    if !ok {
        bail!(
            "`{name}` is not a usable environment variable name; use upper case, digits \
             and underscores, e.g. GRAFANA_SERVICE_ACCOUNT_TOKEN"
        );
    }
    Ok(())
}

/// Take the credential without it reaching a shell history or the process table.
///
/// Order matters: an explicit `--from-env` wins, then a piped stdin, then a
/// hidden prompt. The stdin path is what makes this usable from a script
/// without inventing a flag that takes the value as an argument -- which would
/// put a live credential in `ps` output and every shell history on the machine.
fn read_value(opts: &SealOpts) -> Result<String> {
    if let Some(var) = &opts.from_env {
        let value = std::env::var(var).with_context(|| format!("reading the value from ${var}"))?;
        if value.is_empty() {
            bail!("${var} is set but empty; there is nothing to seal");
        }
        return Ok(value);
    }
    if !io::stdin().is_terminal() {
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .context("reading the value from stdin")?;
        let value = buf.trim_end_matches(['\n', '\r']).to_string();
        if value.is_empty() {
            bail!("stdin was empty; there is nothing to seal");
        }
        return Ok(value);
    }
    print!("{}: ", opts.env_name);
    io::stdout().flush().ok();
    let value = rpassword::read_password().context("reading the value from the terminal")?;
    if value.is_empty() {
        bail!("no value entered; there is nothing to seal");
    }
    Ok(value)
}

pub async fn seal(opts: SealOpts) -> Result<SealOutput> {
    validate_connector(&opts.connector)?;
    validate_env_name(&opts.env_name)?;

    let public_key = match &opts.public_key {
        Some(key) => {
            // Validate by deriving nothing -- just check it decodes to a key of
            // the right size, so a truncated paste fails here rather than
            // producing a blob no cluster can open.
            crate::sealing::seal(key, "probe")
                .context("--public-key is not a usable sealing public key")?;
            key.trim().to_string()
        }
        None => {
            let keys = crate::ops::read_sealing_keys(&opts.namespace, &opts.release).await;
            let Some(current) = keys.first() else {
                bail!(
                    "release {} in namespace {} has no sealing key, so nothing can be sealed \
                     to it. Run `curie cluster up` to generate one, or pass --public-key to \
                     seal against a cluster you cannot reach.",
                    opts.release,
                    opts.namespace
                );
            };
            // Derived, never read from a second field: a stored public key can
            // drift from its private half, and the mismatch only surfaces when
            // a deploy fails to decrypt.
            crate::sealing::public_key_of(current)
                .context("the release's sealing private key is malformed")?
        }
    };

    let value = read_value(&opts)?;
    let sealed = crate::sealing::seal(&public_key, &value)?;

    Ok(SealOutput {
        connector: opts.connector,
        env_name: opts.env_name,
        sealed,
        public_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealing;

    #[test]
    fn connector_name_vectors_match_the_plugin_format_behavior() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/vectors/connector-fields.json"))
                .expect("connector parity vectors must be valid JSON");
        let cases = vectors["connector_names"]
            .as_array()
            .expect("connector name cases must be present");
        assert!(!cases.is_empty(), "connector name cases must not be empty");
        for case in cases {
            assert_eq!(case.as_object().expect("case must be an object").len(), 2);
            let name = case["name"]
                .as_str()
                .expect("connector name must be a string");
            let allowed = case["allowed"]
                .as_bool()
                .expect("expected outcome must be boolean");
            let result = validate_connector(name);
            assert_eq!(result.is_ok(), allowed, "connector {name}: {result:?}");
            if let Err(error) = result {
                assert!(
                    format!("{error:#}").contains(name),
                    "refusal must name {name}"
                );
            }
        }
    }

    #[test]
    fn validate_connector_refuses_reserved_names_and_keeps_prefixed_names_legal() {
        for name in RESERVED_CONNECTOR_NAMES {
            let err = validate_connector(name).expect_err("reserved name must be refused");
            assert!(
                format!("{err:#}").contains(name),
                "the refusal must name the connector: {err:#}"
            );
        }
        assert!(validate_connector("curie-docs").is_ok());
    }

    #[test]
    fn the_snippet_merges_under_a_connector_without_replacing_siblings() {
        let fragment = snippet("GRAFANA_TOKEN", "AgBv3n2K");
        assert_eq!(
            fragment, "    sealed_secrets:\n      GRAFANA_TOKEN: AgBv3n2K",
            "the paste guidance must contain only the connector child block"
        );

        let document = format!(
            "connectors:\n  grafana:\n    image: grafana/example-connector:1.0\n{fragment}\n  slack:\n    image: slack/example-connector:1.0\n"
        );
        let parsed: serde_json::Value = serde_norway::from_str(&document)
            .expect("the connector document with the pasted fragment must be valid YAML");
        assert_eq!(
            parsed["connectors"]["grafana"]["sealed_secrets"]["GRAFANA_TOKEN"],
            serde_json::json!("AgBv3n2K"),
            "the fragment must nest where the connector reads sealed values"
        );
        assert_eq!(
            parsed["connectors"]["grafana"]["image"],
            serde_json::json!("grafana/example-connector:1.0"),
            "pasting the fragment must preserve the connector image"
        );
        assert_eq!(
            parsed["connectors"]["slack"]["image"],
            serde_json::json!("slack/example-connector:1.0"),
            "pasting the fragment must preserve sibling connectors"
        );
    }

    #[test]
    fn an_unusable_env_var_name_is_refused() {
        for bad in ["", "lower", "HAS-DASH", "1LEADING", "HAS SPACE"] {
            assert!(validate_env_name(bad).is_err(), "{bad:?} must be refused");
        }
        validate_env_name("GRAFANA_SERVICE_ACCOUNT_TOKEN").expect("a normal name is fine");
    }

    /// The round trip that matters: what `seal` emits must open with the
    /// cluster's private key, or the whole feature is decorative.
    #[test]
    fn a_sealed_value_opens_with_the_cluster_key() {
        let kp = sealing::generate_keypair();
        let blob = sealing::seal(&kp.public_key, "grafana-token").expect("seals");
        assert_eq!(
            sealing::open_with_any(&[kp.private_key], &blob).expect("opens"),
            "grafana-token"
        );
    }

    /// A truncated or mistyped `--public-key` must fail at seal time. Producing
    /// a blob no cluster can open would be discovered at deploy, by which point
    /// it is committed and pushed.
    #[test]
    fn a_malformed_public_key_is_rejected_up_front() {
        for bad in ["not base64!!", "c2hvcnQ=", ""] {
            assert!(
                sealing::seal(bad, "value").is_err(),
                "{bad:?} must not produce a blob"
            );
        }
    }

    /// The payload carries the public key so an author can tell WHICH cluster
    /// can open what they just committed.
    #[test]
    fn the_payload_names_the_key_it_sealed_to() {
        use crate::ui::CliOutput;
        let kp = sealing::generate_keypair();
        let out = SealOutput {
            connector: "grafana".into(),
            env_name: "GRAFANA_TOKEN".into(),
            sealed: "AgBv3n2K".into(),
            public_key: kp.public_key.clone(),
        };
        let json = out.to_json();
        assert_eq!(json["public_key"], serde_json::json!(kp.public_key));
        assert_eq!(json["connector"], serde_json::json!("grafana"));
        assert!(json["yaml"].as_str().unwrap().contains("sealed_secrets"));
    }
}

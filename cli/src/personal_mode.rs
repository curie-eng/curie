//! Personal Claude subscription mode is Kubernetes only (ADR 0268).
//!
//! The Docker-backed verbs read the operator's own shell for the request and
//! refuse it before any Docker, compose, network, volume or image call. The
//! variables are read here, never emitted to a runner, so they are not boot
//! env keys.

use anyhow::Result;

/// Environment variables through which the CLI's own shell requests personal
/// Claude subscription mode. Only the exact value `personal` requests it.
const PERSONAL_MODE_VARS: [&str; 2] = ["CURIE_MODEL_AUTH_MODE", "FACTORY_MODEL_AUTH_MODE"];

/// Pure decision for [`refuse_personal_mode`]: the first variable whose value is
/// exactly `personal`, read through `get`.
fn personal_mode_request(get: impl Fn(&str) -> Option<String>) -> Option<&'static str> {
    PERSONAL_MODE_VARS
        .into_iter()
        .find(|name| get(name).as_deref() == Some("personal"))
}

/// Refuse a Docker-backed start (`skill up`, `skill eval --model`, `local up`,
/// `local rebuild`) when the environment requests personal mode. Personal
/// subscription mode runs only on Kubernetes, so this runs before any Docker,
/// compose, network, volume or image call, including under `--dry-run`.
///
/// Exit class is usage (2): the argv and environment are well formed but the
/// requested configuration is deterministically unsupported at this tier, so
/// retrying fails identically until the input (the variable) changes.
pub fn refuse_personal_mode() -> Result<()> {
    refuse_personal_mode_from(|name| std::env::var(name).ok())
}

fn refuse_personal_mode_from(get: impl Fn(&str) -> Option<String>) -> Result<()> {
    match personal_mode_request(get) {
        None => Ok(()),
        Some(name) => Err(anyhow::Error::from(
            crate::exit::CliError::usage(format!(
                "personal_mode_requires_kubernetes: {name}=personal requests personal Claude \
                 subscription mode, which runs only on Kubernetes (`curie cluster ...`), not in \
                 Docker-backed `skill` or `local` runs"
            ))
            .with_fix(format!(
                "unset {name}, or run on a cluster with `curie cluster up`"
            )),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personal_mode_is_requested_only_by_the_exact_value_on_either_variable() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert_eq!(
            personal_mode_request(env(&[("CURIE_MODEL_AUTH_MODE", "personal")])),
            Some("CURIE_MODEL_AUTH_MODE")
        );
        assert_eq!(
            personal_mode_request(env(&[("FACTORY_MODEL_AUTH_MODE", "personal")])),
            Some("FACTORY_MODEL_AUTH_MODE")
        );
        for live in ["service", "", "Personal", "personal ", "PERSONAL"] {
            let pairs: &'static [(&'static str, &'static str)] =
                Box::leak(Box::new([("CURIE_MODEL_AUTH_MODE", live)]));
            assert_eq!(personal_mode_request(env(pairs)), None, "{live:?}");
        }
        assert_eq!(personal_mode_request(env(&[])), None);
        assert!(refuse_personal_mode_from(env(&[])).is_ok());
        assert!(refuse_personal_mode_from(env(&[("CURIE_MODEL_AUTH_MODE", "service")])).is_ok());
    }

    #[test]
    fn the_personal_mode_refusal_is_a_coded_usage_error_with_a_fix() {
        let err = refuse_personal_mode_from(|name| {
            (name == "FACTORY_MODEL_AUTH_MODE").then(|| "personal".to_string())
        })
        .unwrap_err();
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(err
            .to_string()
            .starts_with("personal_mode_requires_kubernetes"));
        assert!(err.to_string().contains("Kubernetes"));
        assert!(fix.unwrap().contains("unset FACTORY_MODEL_AUTH_MODE"));
    }
}

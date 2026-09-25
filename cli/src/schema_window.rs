//! Declared application schema ranges for `curie cluster rollback` (#2296).
//!
//! v0.8.x API images run `alembic upgrade head` at startup and refuse a live
//! database revision they do not know. Helm `deployed`/`superseded` is not
//! enough: a status-eligible older application can still be unable to start.
//! This catalog is the CLI-side declaration of each released application's
//! schema range. The v0.9.0 replacement (#2300) ships the window inside the
//! API image; this module is the fail-closed repair for already-released
//! v0.8.x images that do not.

use std::sync::OnceLock;

use regex::Regex;
use serde::Deserialize;

const CATALOG_JSON: &str = include_str!("application_schema_windows.json");

#[derive(Debug, Deserialize)]
struct Catalog {
    revisions: Vec<String>,
    windows: std::collections::BTreeMap<String, Window>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Window {
    pub schema_min: String,
    pub schema_head: String,
    #[serde(default)]
    pub artifact_identity_ambiguous: bool,
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(CATALOG_JSON).expect("application_schema_windows.json is valid")
    })
}

/// Strip an optional `v` prefix so Helm `app_version` and catalog keys match.
pub fn normalize_app_version(raw: &str) -> String {
    raw.trim().trim_start_matches('v').trim().to_string()
}

/// Chart field `curie-0.8.4` -> `0.8.4`. `None` when the field is not that shape.
pub fn version_from_chart(chart: &str) -> Option<String> {
    let rest = chart.trim().strip_prefix("curie-")?;
    let first = rest.chars().next()?;
    if first.is_ascii_digit() {
        Some(normalize_app_version(rest))
    } else {
        None
    }
}

pub fn window_for(app_version: &str) -> Option<Window> {
    catalog()
        .windows
        .get(&normalize_app_version(app_version))
        .cloned()
}

fn revision_index(revision: &str) -> Option<usize> {
    catalog().revisions.iter().position(|item| item == revision)
}

/// Build the serving window declared by retained candidate metadata.
/// Both bounds must be known catalog revisions so an unrecognized value can
/// never expand rollback eligibility.
pub fn candidate_window(schema_min: &str, schema_head: &str) -> Result<Window, String> {
    let Some(min_idx) = revision_index(schema_min) else {
        return Err(format!(
            "candidate schema minimum revision {schema_min} is not in the application schema catalog"
        ));
    };
    let Some(head_idx) = revision_index(schema_head) else {
        return Err(format!(
            "candidate schema head revision {schema_head} is not in the application schema catalog"
        ));
    };
    if min_idx > head_idx {
        return Err(format!(
            "candidate schema minimum revision {schema_min} is after its head revision {schema_head}"
        ));
    }
    Ok(Window {
        schema_min: schema_min.to_string(),
        schema_head: schema_head.to_string(),
        artifact_identity_ambiguous: false,
    })
}

/// Live revision is inside the application's declared range on the linear
/// catalog chain. Unknown live revisions (newer than the catalog) are outside.
pub fn live_in_window(live: &str, window: &Window) -> bool {
    let Some(live_idx) = revision_index(live) else {
        return false;
    };
    let Some(min_idx) = revision_index(&window.schema_min) else {
        return false;
    };
    let Some(head_idx) = revision_index(&window.schema_head) else {
        return false;
    };
    live_idx >= min_idx && live_idx <= head_idx
}

type VersionKey = (u64, u64, u64, u8, u64);

fn version_key(version: &str) -> Option<VersionKey> {
    let normalized = normalize_app_version(version);
    let (core, release_candidate) = match normalized.split_once("-rc.") {
        Some((core, rc)) => (core, Some(rc)),
        None => (normalized.as_str(), None),
    };
    let parse_number = |part: &str| {
        if part.is_empty()
            || !part.bytes().all(|byte| byte.is_ascii_digit())
            || (part.len() > 1 && part.starts_with('0'))
        {
            return None;
        }
        part.parse::<u64>().ok()
    };
    let mut parts = core.split('.');
    let major = parse_number(parts.next()?)?;
    let minor = parse_number(parts.next()?)?;
    let patch = parse_number(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    let (stable, rc) = match release_candidate {
        Some(value) => (0, parse_number(value)?),
        None => (1, 0),
    };
    Some((major, minor, patch, stable, rc))
}

/// Newest catalogued application version in `candidates` whose window contains
/// `live`. That is the fail-forward target a refused rollback must name.
pub fn newest_fail_forward<'a>(
    candidates: impl IntoIterator<Item = &'a str>,
    live: &str,
) -> Option<String> {
    let mut best: Option<String> = None;
    for raw in candidates {
        let version = normalize_app_version(raw);
        if version.is_empty() {
            continue;
        }
        let Some(key) = version_key(&version) else {
            continue;
        };
        let Some(window) = window_for(&version) else {
            continue;
        };
        if !live_in_window(live, &window) {
            continue;
        }
        match &best {
            None => best = Some(version),
            Some(current) if Some(key) > version_key(current) => {
                best = Some(version);
            }
            Some(_) => {}
        }
    }
    best
}

#[derive(Debug)]
pub struct SchemaRefusal {
    pub message: String,
    pub fix: String,
}

fn fail_forward_fix(forward: Option<String>, live: &str) -> String {
    match forward {
        Some(version) => format!(
            "fail forward to application {version}, which can start against revision {live}"
        ),
        None => format!(
            "no catalogued application version in this release history can start against revision {live}; stay on the current revision"
        ),
    }
}

/// Check that `target_app` can start against `live` using its resolved artifact
/// window. `history_apps` supplies fail forward candidates from Helm history.
pub fn check_target_schema(
    target_app: &str,
    window: &Window,
    live: &str,
    history_apps: &[String],
) -> Result<(), SchemaRefusal> {
    let target = normalize_app_version(target_app);
    let candidates: Vec<&str> = history_apps.iter().map(String::as_str).collect();
    if live_in_window(live, window) {
        return Ok(());
    }
    Err(SchemaRefusal {
        message: format!(
            "refusing rollback to application {target}: live database revision {live} is outside its declared schema range (head {})",
            window.schema_head
        ),
        fix: fail_forward_fix(newest_fail_forward(candidates, live), live),
    })
}

/// Refusal for an application version that has no static catalog entry.
pub fn missing_target_window_refusal(
    target_app: &str,
    live: &str,
    history_apps: &[String],
) -> SchemaRefusal {
    let target = normalize_app_version(target_app);
    let candidates: Vec<&str> = history_apps.iter().map(String::as_str).collect();
    SchemaRefusal {
        message: format!(
            "refusing rollback to application {target}: no declared schema range for that version"
        ),
        fix: fail_forward_fix(newest_fail_forward(candidates, live), live),
    }
}

/// Last non-log token of `alembic current` stdout: `0039 (head)` -> `0039`.
pub fn parse_alembic_current_output(stdout: &str) -> Option<String> {
    for line in stdout.lines().rev() {
        let line = line.trim();
        if line.is_empty()
            || line.starts_with("INFO ")
            || line.starts_with("DEBUG ")
            || line.starts_with("WARNING ")
        {
            continue;
        }
        let token = line.split_whitespace().next()?;
        if !token.is_empty() && token.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Some(token.to_string());
        }
    }
    None
}

/// Strip connection strings and password-shaped tokens from probe output so a
/// refusal never carries database contents or credentials.
pub fn redact_probe_text(text: &str) -> String {
    static PATTERNS: OnceLock<[Regex; 3]> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        [
            Regex::new(r"(?i)(?:postgres(?:ql)?(?:\+asyncpg)?)://\S+").expect("dsn regex"),
            Regex::new(r"(?i)database_url=\S+").expect("database_url regex"),
            Regex::new(r"(?i)password=\S+").expect("password regex"),
        ]
    });
    let mut out = text.to_string();
    for re in patterns {
        out = re.replace_all(&out, "<redacted>").into_owned();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v084_cannot_start_against_0039() {
        let window = window_for("0.8.4").expect("0.8.4 is catalogued");
        assert_eq!(window.schema_head, "0038");
        assert!(!live_in_window("0039", &window));
        assert!(live_in_window("0038", &window));
        assert!(live_in_window("0001", &window));
    }

    #[test]
    fn v085_can_start_against_0039() {
        let window = window_for("0.8.5").expect("0.8.5 is catalogued");
        assert_eq!(window.schema_head, "0039");
        assert!(live_in_window("0039", &window));
        assert!(live_in_window("0038", &window));
    }

    #[test]
    fn incident_refusal_names_fail_forward_v085() {
        let history = ["0.8.4".to_string(), "0.8.5".to_string()];
        let window = window_for("0.8.4").expect("0.8.4 window");
        let err =
            check_target_schema("0.8.4", &window, "0039", &history).expect_err("incompatible");
        assert!(err.message.contains("0039") && err.message.contains("0.8.4"));
        assert!(err.message.contains("0038"));
        assert!(err.fix.contains("0.8.5"), "{}", err.fix);
        assert!(!err.message.contains("postgresql://") && !err.fix.contains("postgresql://"));
    }

    #[test]
    fn compatible_v085_to_v085_is_allowed() {
        let window = window_for("0.8.5").expect("0.8.5 window");
        check_target_schema("0.8.5", &window, "0039", &["0.8.5".to_string()]).expect("compatible");
    }

    #[test]
    fn unknown_app_version_is_refused() {
        let err = missing_target_window_refusal(
            "0.7.3",
            "0039",
            &["0.7.3".to_string(), "0.8.5".to_string()],
        );
        assert!(err.message.contains("0.7.3"));
        assert!(err.fix.contains("0.8.5"));
    }

    #[test]
    fn unknown_live_revision_is_outside_every_window() {
        let window = window_for("0.8.6").unwrap();
        assert!(!live_in_window("0099", &window));
        assert!(
            !live_in_window("0040", &window),
            "0.8.6's declared window ends at 0039; 0040 is this tree's later head"
        );
    }

    #[test]
    fn alembic_current_parses_head_annotation() {
        let stdout =
            "INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.\n0039 (head)\n";
        assert_eq!(
            parse_alembic_current_output(stdout).as_deref(),
            Some("0039")
        );
        assert_eq!(
            parse_alembic_current_output("0038\n").as_deref(),
            Some("0038")
        );
        assert_eq!(parse_alembic_current_output("INFO only\n"), None);
    }

    #[test]
    fn probe_redaction_strips_a_dsn() {
        let redacted = redact_probe_text(
            "could not connect to postgresql://curie:secret-password@postgres:5432/curie",
        );
        assert!(!redacted.contains("secret-password"));
        assert!(!redacted.contains("postgresql://"));
        assert!(redacted.contains("<redacted>"));
    }

    #[test]
    fn version_from_chart_reads_curie_chart_field() {
        assert_eq!(version_from_chart("curie-0.8.4").as_deref(), Some("0.8.4"));
        assert_eq!(version_from_chart("other-1.0.0"), None);
    }

    #[test]
    fn released_v090_and_v091_remain_at_0044_and_v091_refuses_0046() {
        let v090 = window_for("0.9.0").expect("0.9.0 is catalogued");
        let v091 = window_for("0.9.1").expect("0.9.1 is catalogued");
        assert_eq!(v090.schema_head, "0044");
        assert_eq!(v091.schema_head, "0044");

        let err = check_target_schema(
            "0.9.1",
            &v091,
            "0046",
            &["0.9.1".to_string(), "0.10.0".to_string()],
        )
        .expect_err("published 0.9.1 must refuse live revision 0046");
        assert!(err.message.contains("0.9.1"));
        assert!(err.message.contains("0046"));
        assert!(err.message.contains("0044"));
    }

    #[test]
    fn released_v092_accepts_adapter_0045_and_refuses_later_live_revisions() {
        let v092 = window_for("0.9.2").expect("0.9.2 is catalogued");
        assert_eq!(v092.schema_min, "0045");
        assert_eq!(v092.schema_head, "0045");

        check_target_schema(
            "0.9.2",
            &v092,
            "0045",
            &["0.9.2".to_string(), "0.10.0".to_string()],
        )
        .expect("published 0.9.2 accepts its adapter schema");

        for live in ["0046", "0047", "0048", "0049", "0050"] {
            let err = check_target_schema(
                "0.9.2",
                &v092,
                live,
                &["0.9.2".to_string(), "0.10.0".to_string()],
            )
            .expect_err("published 0.9.2 must refuse later live revisions");
            assert!(err.message.contains("0.9.2"));
            assert!(err.message.contains(live));
            assert!(err.message.contains("0045"));
        }
    }

    #[test]
    fn packaged_n_and_n1_share_this_tree_head_so_rollback_is_compatible() {
        let n = window_for("0.10.0").expect("0.10.0 is catalogued for the next train matrix");
        let n1 = window_for("0.10.1").expect("0.10.1 is catalogued for the next train matrix");
        assert_eq!(n.schema_min, "0045");
        assert_eq!(n.schema_head, "0059");
        assert_eq!(n.schema_min, n1.schema_min);
        assert_eq!(n.schema_head, n1.schema_head);
        check_target_schema(
            "0.10.0",
            &n,
            "0059",
            &["0.10.0".to_string(), "0.10.1".to_string()],
        )
        .expect("N+1 to N is the same schema window");
        let err = check_target_schema(
            "0.8.7",
            &window_for("0.8.7").expect("0.8.7 window"),
            &n.schema_head,
            &["0.8.7".to_string(), "0.10.0".to_string()],
        )
        .expect_err("0.8.7 cannot start on this tree's head");
        assert!(
            err.message.contains("0.8.7") && err.message.contains("schema"),
            "{}",
            err.message
        );
    }

    #[test]
    fn unreleased_patch_093_keeps_the_092_schema_window() {
        let previous = window_for("0.9.2").expect("0.9.2 schema window");
        let patch = window_for("0.9.3").expect("0.9.3 schema window");
        assert_eq!(patch.schema_min, previous.schema_min);
        assert_eq!(patch.schema_head, previous.schema_head);
        assert_eq!(patch.schema_min, "0045");
        assert_eq!(patch.schema_head, "0045");
    }

    #[test]
    fn catalog_head_matches_this_tree() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cli is not the repo root");
        let chart =
            std::fs::read_to_string(root.join("charts/curie/Chart.yaml")).expect("Chart.yaml");
        let app_version = chart
            .lines()
            .find_map(|line| {
                line.strip_prefix("appVersion:")
                    .map(|rest| normalize_app_version(rest.trim().trim_matches('"')))
            })
            .expect("appVersion");
        let chart_window = window_for(&app_version)
            .unwrap_or_else(|| panic!("catalog missing window for {app_version}"));
        assert!(
            !chart_window.artifact_identity_ambiguous,
            "Chart.yaml appVersion {app_version} must have one unambiguous artifact identity"
        );

        let (newest_catalog_version, newest_window) = catalog()
            .windows
            .iter()
            .max_by_key(|(version, _)| version_key(version))
            .expect("catalog has at least one schema window");

        let mut found = Vec::new();
        let mut down_of = Vec::new();
        let versions = root.join("apps/api/alembic/versions");
        for entry in std::fs::read_dir(&versions).expect("alembic versions") {
            let path = entry.expect("entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("py") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read migration");
            let mut revision = None;
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix("revision: str = \"") {
                    revision = rest.strip_suffix('"').map(str::to_string);
                }
                if let Some(rest) = line.strip_prefix("down_revision: str | None = ") {
                    let token = rest.trim().trim_matches('"');
                    if token != "None" && !token.is_empty() {
                        down_of.push(token.to_string());
                    }
                }
            }
            if let Some(id) = revision {
                found.push(id);
            }
        }
        for id in &found {
            assert!(
                catalog().revisions.iter().any(|item| item == id),
                "catalog revisions missing alembic id {id}"
            );
        }
        let mut heads: Vec<&String> = found
            .iter()
            .filter(|id| !down_of.iter().any(|down| down == *id))
            .collect();
        heads.sort();
        assert_eq!(heads.len(), 1, "expected one alembic head, got {heads:?}");
        let tree_head = heads[0];
        assert!(
            catalog().revisions.iter().any(|item| item == tree_head),
            "catalog revisions missing this tree's alembic head {tree_head}"
        );
        assert_eq!(
            newest_window.schema_head,
            *tree_head,
            "newest catalog appVersion {newest_catalog_version} window head must exactly match this tree's Alembic head {tree_head}; update the application schema window when the catalog revision list advances"
        );
    }
}

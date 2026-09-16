use anyhow::{anyhow, Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PARTIAL_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Release,
    Dev,
}

impl Channel {
    /// Read the build-stamped channel. Uses option_env! so it compiles even
    /// before build.rs exists; defaults to Dev when unset or unrecognized.
    pub fn current() -> Channel {
        #[cfg(debug_assertions)]
        if std::env::var("CURIE_TEST_ARTIFACT_CHANNEL").as_deref() == Ok("release") {
            return Channel::Release;
        }
        match option_env!("CURIE_BUILD_CHANNEL") {
            Some("release") => Channel::Release,
            _ => Channel::Dev,
        }
    }
}

/// The resolution outcome for one artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// Use this local path verbatim (override, or dev-channel local candidate).
    Local(PathBuf),
    /// Download `url` into `cache_path`.
    Fetch { url: String, cache_path: PathBuf },
}

impl Resolved {
    /// The argv path token for this artifact WITHOUT fetching (dry-run display):
    /// a local path as-is, or the would-be cache path for a Fetch.
    pub fn planned_target(&self) -> PathBuf {
        match self {
            Resolved::Local(path) => path.clone(),
            Resolved::Fetch { cache_path, .. } => cache_path.clone(),
        }
    }
}

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The full repository commit stamped by `build.rs`, when this binary was
/// built from a Git checkout. Source archives and other no-Git builds return
/// `None` rather than inventing provenance.
pub fn commit_sha() -> Option<&'static str> {
    option_env!("CURIE_BUILD_COMMIT")
        .filter(|commit| commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// XDG_CACHE_HOME or $HOME/.cache, joined with "curie". Err (never panic) when
/// neither env var is set, with a message naming the vars + the override flags.
pub fn cache_root() -> Result<PathBuf> {
    if let Some(value) = env::var_os("XDG_CACHE_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(value).join("curie"));
    }

    if let Some(value) = env::var_os("HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(value).join(".cache").join("curie"));
    }

    Err(anyhow!(
        "could not determine cache directory: XDG_CACHE_HOME and HOME are unset or empty; set HOME or pass -f/--chart override"
    ))
}

pub fn resolve_compose(
    override_: Option<&str>,
    channel: Channel,
    version: &str,
    cache_root: impl FnOnce() -> Result<PathBuf>,
    local_exists: bool,
) -> Result<Resolved> {
    if let Some(value) = override_ {
        return Ok(Resolved::Local(PathBuf::from(value)));
    }

    match channel {
        Channel::Release => Ok(Resolved::Fetch {
            url: format!(
                "https://github.com/curie-eng/curie/releases/download/v{version}/compose.release.yaml"
            ),
            cache_path: cache_root()?
                .join(format!("v{version}"))
                .join("compose.release.yaml"),
        }),
        Channel::Dev if local_exists => Ok(Resolved::Local(PathBuf::from("compose.dev.yaml"))),
        // compose.dev.yaml is not self-contained (it builds curie-worker from
        // ./compose, which is absent from a fetched copy), so there is nothing
        // safe to fall back to. Error instead of fetching a file that cannot run.
        Channel::Dev => Err(anyhow!(
            "dev build with no local compose.dev.yaml in cwd; pass -f <compose>, or use a released binary (which resolves the version-pinned release compose and so cannot serve --build)"
        )),
    }
}

pub fn resolve_chart(
    override_: Option<&str>,
    channel: Channel,
    version: &str,
    cache_root: impl FnOnce() -> Result<PathBuf>,
    local_exists: bool,
) -> Result<Resolved> {
    if let Some(value) = override_ {
        return Ok(Resolved::Local(PathBuf::from(value)));
    }

    match channel {
        Channel::Release => {
            if version.is_empty()
                || !version.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+')
                })
            {
                return Err(anyhow!(
                    "default release chart requires a safe target version component containing only ASCII letters, digits, '.', '-', and '+'; pass --chart <path-or-ref> to use an explicit chart"
                ));
            }
            Ok(Resolved::Fetch {
                url: format!(
                    "https://github.com/curie-eng/curie/releases/download/v{version}/curie-{version}.tgz"
                ),
                cache_path: cache_root()?
                    .join(format!("v{version}"))
                    .join(format!("curie-{version}.tgz")),
            })
        }
        Channel::Dev if local_exists => Ok(Resolved::Local(PathBuf::from("charts/curie"))),
        Channel::Dev => Err(anyhow!(
            "dev build with no charts/curie in cwd; pass --chart <path-or-tgz> or use a released binary"
        )),
    }
}

pub fn resolve_image(override_: Option<&str>, channel: Channel, version: &str) -> String {
    if let Some(value) = override_ {
        return value.to_string();
    }

    match channel {
        Channel::Release => {
            format!(
                "ghcr.io/curie-eng/{}:{version}",
                crate::docker::RUNNER_IMAGE
            )
        }
        Channel::Dev => crate::docker::RUNNER_IMAGE.to_string(),
    }
}

/// Perform the fetch described by a `Resolved::Fetch`, returning the local path.
/// - `Resolved::Local(p)` returns `p` unchanged (no network).
/// - `Resolved::Fetch`: cache hit (final file exists) returns it with no network;
///   else download.
/// - Downloads write to a PER-PROCESS-UNIQUE temp file in the cache dir, then
///   atomically rename to `cache_path`. Never a shared fixed `.partial` name.
pub async fn ensure_cached(resolved: &Resolved) -> Result<PathBuf> {
    match resolved {
        Resolved::Local(path) => Ok(path.clone()),
        Resolved::Fetch { url, cache_path } => {
            if cache_path.exists() {
                return Ok(cache_path.clone());
            }

            download_to_cache(url, cache_path).await?;
            Ok(cache_path.clone())
        }
    }
}

async fn download_to_cache(url: &str, cache_path: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .context("building the artifact fetch client")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to fetch {url}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("failed to fetch {url}: HTTP {status}: {body}"));
    }

    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;
    let parent = cache_path.parent().ok_or_else(|| {
        anyhow!(
            "failed to cache {url}: cache path has no parent: {}",
            cache_path.display()
        )
    })?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create cache directory for {url}: {}",
            parent.display()
        )
    })?;

    let partial_id = NEXT_PARTIAL_ID.fetch_add(1, Ordering::Relaxed);
    let partial_path = parent.join(format!(
        ".curie.{}.{}.partial",
        std::process::id(),
        partial_id
    ));

    if let Err(err) = fs::write(&partial_path, &bytes).with_context(|| {
        format!(
            "failed to write temporary cache file for {url}: {}",
            partial_path.display()
        )
    }) {
        let _ = fs::remove_file(&partial_path);
        return Err(err);
    }

    if let Err(err) = fs::rename(&partial_path, cache_path).with_context(|| {
        format!(
            "failed to install cached artifact from {url} at {}",
            cache_path.display()
        )
    }) {
        let _ = fs::remove_file(&partial_path);
        return Err(err);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION: &str = "0.1.0";

    #[test]
    fn release_compose_fetches_release_remote_and_ignores_local_candidate() {
        let expected = Resolved::Fetch {
            url: "https://github.com/curie-eng/curie/releases/download/v0.1.0/compose.release.yaml"
                .to_string(),
            cache_path: PathBuf::from("/tmp/xdgcache/curie/v0.1.0/compose.release.yaml"),
        };

        assert_eq!(
            resolve_compose(
                None,
                Channel::Release,
                VERSION,
                || Ok(PathBuf::from("/tmp/xdgcache/curie")),
                false,
            )
            .unwrap(),
            expected
        );
        assert_eq!(
            resolve_compose(
                None,
                Channel::Release,
                VERSION,
                || Ok(PathBuf::from("/tmp/xdgcache/curie")),
                true,
            )
            .unwrap(),
            expected
        );
    }

    #[test]
    fn release_chart_fetches_release_archive() {
        assert_eq!(
            resolve_chart(
                None,
                Channel::Release,
                VERSION,
                || Ok(PathBuf::from("/tmp/xdgcache/curie")),
                false,
            )
            .unwrap(),
            Resolved::Fetch {
                url: "https://github.com/curie-eng/curie/releases/download/v0.1.0/curie-0.1.0.tgz"
                    .to_string(),
                cache_path: PathBuf::from("/tmp/xdgcache/curie/v0.1.0/curie-0.1.0.tgz"),
            }
        );
    }

    #[test]
    fn release_chart_rejects_unsafe_version_components_before_cache_resolution() {
        for version in [
            "",
            "../outside",
            r"0.9.0\outside",
            "0.9.0?asset",
            "0.9.0#asset",
            "0.9.0%2Fasset",
            "0.9.0 target",
            "0.9.0\ncontrol",
            "0.9.0-é",
        ] {
            let cache_called = std::cell::Cell::new(false);
            let result = resolve_chart(
                None,
                Channel::Release,
                version,
                || {
                    cache_called.set(true);
                    Ok(PathBuf::from("/tmp/xdgcache/curie"))
                },
                false,
            );

            assert!(
                !cache_called.get(),
                "unsafe version reached cache resolution: {version:?}"
            );
            let message = result.unwrap_err().to_string();
            assert!(
                message.contains("safe target version component"),
                "error was {message:?}"
            );
            assert!(
                message.contains("ASCII letters, digits, '.', '-', and '+'"),
                "error was {message:?}"
            );
            assert!(message.contains("--chart"), "error was {message:?}");
            assert!(
                !message.chars().any(char::is_control),
                "error echoed a control byte: {message:?}"
            );
        }
    }

    #[test]
    fn release_chart_accepts_prerelease_and_build_version_components() {
        let version = "1.2.3-rc.1+build.7";

        assert_eq!(
            resolve_chart(
                None,
                Channel::Release,
                version,
                || Ok(PathBuf::from("/tmp/xdgcache/curie")),
                false,
            )
            .unwrap(),
            Resolved::Fetch {
                url: format!(
                    "https://github.com/curie-eng/curie/releases/download/v{version}/curie-{version}.tgz"
                ),
                cache_path: PathBuf::from(
                    "/tmp/xdgcache/curie/v1.2.3-rc.1+build.7/curie-1.2.3-rc.1+build.7.tgz",
                ),
            }
        );
    }

    #[test]
    fn unused_version_does_not_change_explicit_override_or_dev_resolution() {
        let unsafe_version = "../../outside?asset#fragment%2Fpath";

        assert_eq!(
            resolve_chart(
                Some("charts/curie"),
                Channel::Release,
                unsafe_version,
                || panic!("cache_root should not be called for a chart override"),
                false,
            )
            .unwrap(),
            Resolved::Local(PathBuf::from("charts/curie"))
        );
        assert_eq!(
            resolve_chart(
                None,
                Channel::Dev,
                unsafe_version,
                || panic!("cache_root should not be called for a local dev chart"),
                true,
            )
            .unwrap(),
            Resolved::Local(PathBuf::from("charts/curie"))
        );
    }

    #[test]
    fn release_image_uses_versioned_runner_ref_without_v_prefix() {
        assert_eq!(
            resolve_image(None, Channel::Release, VERSION),
            "ghcr.io/curie-eng/curie-runner:0.1.0".to_string()
        );
    }

    #[test]
    fn overrides_short_circuit_resolution_in_both_channels() {
        for channel in [Channel::Release, Channel::Dev] {
            for value in ["anything", "charts/curie", "compose.dev.yaml"] {
                assert_eq!(
                    resolve_compose(
                        Some(value),
                        channel,
                        VERSION,
                        || panic!("cache_root should not be called for compose overrides"),
                        false,
                    )
                    .unwrap(),
                    Resolved::Local(PathBuf::from(value))
                );
                assert_eq!(
                    resolve_chart(
                        Some(value),
                        channel,
                        VERSION,
                        || panic!("cache_root should not be called for chart overrides"),
                        false,
                    )
                    .unwrap(),
                    Resolved::Local(PathBuf::from(value))
                );
                assert_eq!(
                    resolve_image(Some(value), channel, VERSION),
                    value.to_string()
                );
            }
        }
    }

    #[test]
    fn dev_uses_local_artifacts_when_present() {
        assert_eq!(
            resolve_compose(
                None,
                Channel::Dev,
                VERSION,
                || panic!("cache_root should not be called for local dev compose"),
                true,
            )
            .unwrap(),
            Resolved::Local(PathBuf::from("compose.dev.yaml"))
        );
        assert_eq!(
            resolve_chart(
                None,
                Channel::Dev,
                VERSION,
                || panic!("cache_root should not be called for local dev chart"),
                true,
            )
            .unwrap(),
            Resolved::Local(PathBuf::from("charts/curie"))
        );
    }

    #[test]
    fn dev_missing_local_compose_and_chart_require_override() {
        let compose_err = resolve_compose(
            None,
            Channel::Dev,
            VERSION,
            || Ok(PathBuf::from("/tmp/xdgcache/curie")),
            false,
        )
        .unwrap_err();
        let compose_message = compose_err.to_string();
        assert!(
            compose_message.contains("-f"),
            "error was {compose_message}"
        );

        let err = resolve_chart(
            None,
            Channel::Dev,
            VERSION,
            || Ok(PathBuf::from("/tmp/xdgcache/curie")),
            false,
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("--chart"), "error was {message}");
    }

    #[test]
    fn dev_image_keeps_bare_runner_default() {
        assert_eq!(
            resolve_image(None, Channel::Dev, VERSION),
            "curie-runner".to_string()
        );
    }

    #[test]
    fn current_channel_defaults_to_dev_in_tests() {
        assert_eq!(Channel::current(), Channel::Dev);
    }

    #[test]
    fn planned_target_returns_file_path_without_fetching() {
        assert_eq!(
            Resolved::Fetch {
                url: "https://example.test/artifact".to_string(),
                cache_path: PathBuf::from("/x/y"),
            }
            .planned_target(),
            PathBuf::from("/x/y")
        );
        assert_eq!(
            Resolved::Local(PathBuf::from("z")).planned_target(),
            PathBuf::from("z")
        );
    }
}

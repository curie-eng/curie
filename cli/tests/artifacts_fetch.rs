mod support;

use curie::artifacts::{ensure_cached, Resolved};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use support::{serve, Response};

#[tokio::test]
async fn cache_hit_skips_network_for_pinned_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let cache_path = dir.path().join("compose.dev.yaml");
    fs::write(&cache_path, b"cached compose").unwrap();
    let resolved = Resolved::Fetch {
        url: "http://127.0.0.1:1/compose.dev.yaml".to_string(),
        cache_path: cache_path.clone(),
    };

    let path = ensure_cached(&resolved).await.unwrap();

    assert_eq!(path, cache_path);
    assert_eq!(fs::read(&cache_path).unwrap(), b"cached compose".to_vec());
}

#[tokio::test]
async fn cache_miss_downloads_pinned_and_second_call_uses_cache() {
    let expected = b"downloaded compose".to_vec();
    let body = expected.clone();
    let server = serve(move |req| {
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/compose.dev.yaml");
        Response {
            status: 200,
            content_type: "application/octet-stream".into(),
            body: body.clone(),
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let cache_path = dir.path().join("compose.dev.yaml");
    let resolved = Resolved::Fetch {
        url: format!("{}/compose.dev.yaml", server.base_url),
        cache_path: cache_path.clone(),
    };

    let first = ensure_cached(&resolved).await.unwrap();

    assert_eq!(first, cache_path);
    assert_eq!(fs::read(&cache_path).unwrap(), expected);
    assert_eq!(server.recorded().len(), 1);

    let second = ensure_cached(&resolved).await.unwrap();

    assert_eq!(second, cache_path);
    assert_eq!(server.recorded().len(), 1);
}

#[tokio::test]
async fn pinned_http_error_without_cache_reports_url_and_cleans_temp_files() {
    let server = serve(|_req| Response {
        status: 404,
        content_type: "text/plain".into(),
        body: b"not found".to_vec(),
    });
    let dir = tempfile::tempdir().unwrap();
    let cache_path = dir.path().join("compose.dev.yaml");
    let url = format!("{}/compose.dev.yaml", server.base_url);
    let resolved = Resolved::Fetch {
        url: url.clone(),
        cache_path: cache_path.clone(),
    };

    let err = ensure_cached(&resolved).await.unwrap_err();
    let message = err.to_string();
    let partials: Vec<PathBuf> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(".partial"))
        })
        .collect();

    assert!(message.contains(&url), "error was {message}");
    assert!(message.contains("not found"), "error was {message}");
    assert!(!cache_path.exists());
    assert!(partials.is_empty(), "leftover partial files: {partials:?}");
}

#[tokio::test]
async fn local_resolution_returns_path_without_network() {
    let path = PathBuf::from("compose.dev.yaml");
    let resolved = Resolved::Local(path.clone());

    assert_eq!(ensure_cached(&resolved).await.unwrap(), path);
}

#[test]
fn cluster_upgrade_rejects_unsafe_release_target_before_cache_or_helm() {
    let dir = tempfile::tempdir().unwrap();
    let empty_path = dir.path().join("empty-path");
    let cache_home = dir.path().join("cache");
    fs::create_dir(&empty_path).unwrap();
    let executable = fs::canonicalize(env!("CARGO_BIN_EXE_curie")).unwrap();
    assert!(executable.is_absolute());

    let output = Command::new(executable)
        .args([
            "--color=never",
            "cluster",
            "upgrade",
            "--to",
            "../../../outside?asset#fragment%2Fpath",
            "--yes",
            "--dry-run",
        ])
        .current_dir(dir.path())
        .env("CURIE_TEST_ARTIFACT_CHANNEL", "release")
        .env("XDG_CACHE_HOME", &cache_home)
        .env("PATH", &empty_path)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        message.contains("safe target version component"),
        "error was {message:?}"
    );
    assert!(message.contains("--chart"), "error was {message:?}");
    assert!(
        !cache_home.exists(),
        "invalid target must be rejected before cache materialization"
    );
}

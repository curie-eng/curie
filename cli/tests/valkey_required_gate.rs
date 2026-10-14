use crate::support;

fn restore_var(name: &str, value: Option<std::ffi::OsString>) {
    match value {
        Some(value) => std::env::set_var(name, value),
        None => std::env::remove_var(name),
    }
}

#[tokio::test]
async fn required_valkey_panics_when_valkey_is_unreachable() {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("reserve an unused localhost port");
    let port = listener.local_addr().expect("read reserved port").port();
    drop(listener);

    let _env = support::ENV_LOCK.lock().await;
    let previous_url = std::env::var_os("TEST_VALKEY_URL");
    let previous_required = std::env::var_os("CI_REQUIRE_VALKEY_TESTS");
    std::env::set_var("TEST_VALKEY_URL", format!("redis://127.0.0.1:{port}"));
    std::env::remove_var("CI_REQUIRE_VALKEY_TESTS");

    let optional = support::valkey_or_skip("optional Valkey gate probe").await;
    assert!(
        optional.is_none(),
        "unreachable optional Valkey did not skip"
    );

    std::env::set_var("CI_REQUIRE_VALKEY_TESTS", "1");

    let result =
        tokio::spawn(async { support::valkey_or_skip("required Valkey gate probe").await }).await;

    restore_var("TEST_VALKEY_URL", previous_url);
    restore_var("CI_REQUIRE_VALKEY_TESTS", previous_required);

    match result {
        Err(error) => assert!(error.is_panic(), "required Valkey failure was not a panic"),
        Ok(_) => panic!("unreachable required Valkey did not panic"),
    }
}

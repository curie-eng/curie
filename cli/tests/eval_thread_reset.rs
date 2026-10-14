//! Integration: eval-owned sandbox release queues onto the frozen thread-reset
//! SET against real Valkey (#1534).

use curie::queue::{queue_thread_reset, THREAD_RESET_SET};

use crate::support;
use support::{unique_stream, valkey_or_skip};

#[tokio::test]
async fn queue_thread_reset_sadds_the_frozen_set() {
    let Some(mut conn) = valkey_or_skip("queue_thread_reset_sadds_the_frozen_set").await else {
        return;
    };
    let thread_key = unique_stream("eval-thread-");
    queue_thread_reset(&mut conn, &thread_key)
        .await
        .expect("SADD thread-reset request");

    let is_member: bool = redis::cmd("SISMEMBER")
        .arg(THREAD_RESET_SET)
        .arg(&thread_key)
        .query_async(&mut conn)
        .await
        .expect("SISMEMBER");
    assert!(
        is_member,
        "eval-owned conversation {thread_key} must land on {THREAD_RESET_SET}"
    );

    let _: i32 = redis::cmd("SREM")
        .arg(THREAD_RESET_SET)
        .arg(&thread_key)
        .query_async(&mut conn)
        .await
        .expect("cleanup SREM");
}

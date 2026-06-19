use crate::pending_result::PendingResultState;

#[tokio::test]
async fn pending_result_blocks_idle_until_stored_ack() {
    let mut pending = PendingResultState::default();
    pending
        .set_ready("shard-1".to_string(), b"id,value\n1,a\n".to_vec())
        .unwrap();

    assert_eq!(pending.status(), "RESULT_READY");
    assert_eq!(pending.current_shard_id(), Some("shard-1"));
    assert!(pending.try_take_assignment("shard-2").is_err());

    let result_data = pending.to_result_data("shard-1").unwrap();
    assert_eq!(result_data.shard_id, "shard-1");
    assert_eq!(result_data.output_csv, b"id,value\n1,a\n".to_vec());

    pending.mark_stored("shard-1").unwrap();

    assert_eq!(pending.status(), "IDLE");
    assert_eq!(pending.current_shard_id(), None);
}

#[tokio::test]
async fn pending_result_allows_repeated_fetch_before_stored_ack() {
    let mut pending = PendingResultState::default();
    pending
        .set_ready("shard-1".to_string(), b"id,value\n1,a\n".to_vec())
        .unwrap();

    let first = pending.to_result_data("shard-1").unwrap();
    let second = pending.to_result_data("shard-1").unwrap();

    assert_eq!(first.shard_id, "shard-1");
    assert_eq!(second.shard_id, "shard-1");
    assert_eq!(first.output_csv, second.output_csv);
    assert_eq!(pending.status(), "RESULT_READY");
    assert_eq!(pending.current_shard_id(), Some("shard-1"));
}

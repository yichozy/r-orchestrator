#[cfg(test)]
mod tests {
    use crate::pending_result::PendingResultState;

    #[tokio::test]
    async fn pending_result_blocks_idle_until_stored_ack() {
        let mut pending = PendingResultState::default();
        pending.set_ready("shard-1".to_string()).unwrap();

        assert_eq!(pending.status(), "RESULT_READY");
        assert_eq!(pending.current_shard_id(), Some("shard-1"));
        assert!(pending.try_take_assignment("shard-2").is_err());

        pending.mark_stored("shard-1").unwrap();

        assert_eq!(pending.status(), "IDLE");
        assert_eq!(pending.current_shard_id(), None);
    }

    #[tokio::test]
    async fn pending_result_allows_new_assignment_after_stored_ack() {
        let mut pending = PendingResultState::default();

        pending.set_ready("shard-1".to_string()).unwrap();
        pending.mark_stored("shard-1").unwrap();

        assert_eq!(pending.status(), "IDLE");
        assert!(pending.try_take_assignment("shard-2").is_ok());
    }
}

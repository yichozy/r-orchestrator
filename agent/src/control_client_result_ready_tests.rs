#[cfg(test)]
mod tests {
    use crate::pending_result::PendingResultState;

    #[tokio::test]
    async fn pending_result_blocks_idle_until_stored_ack() {
        let mut pending = PendingResultState::default();
        pending
            .set_ready(
                "shard-1".to_string(),
                "tasks/abc/output.zip".to_string(),
                "sha256hash".to_string(),
            )
            .unwrap();

        assert_eq!(pending.status(), "RUNNING");
        assert_eq!(pending.current_shard_id(), Some("shard-1"));
        assert!(pending.try_take_assignment("shard-2").is_err());

        pending.mark_stored("shard-1").unwrap();

        assert_eq!(pending.status(), "IDLE");
        assert_eq!(pending.current_shard_id(), None);
    }

    #[tokio::test]
    async fn pending_result_allows_new_assignment_after_stored_ack() {
        let mut pending = PendingResultState::default();

        pending
            .set_ready(
                "shard-1".to_string(),
                "tasks/abc/output.zip".to_string(),
                "sha256hash".to_string(),
            )
            .unwrap();
        pending.mark_stored("shard-1").unwrap();

        assert_eq!(pending.status(), "IDLE");
        assert!(pending.try_take_assignment("shard-2").is_ok());
    }

    #[tokio::test]
    async fn pending_result_take_extracts_and_clears() {
        let mut pending = PendingResultState::default();
        pending
            .set_ready(
                "shard-1".to_string(),
                "tasks/abc/output.zip".to_string(),
                "sha256hash".to_string(),
            )
            .unwrap();

        let taken = pending.take();
        assert_eq!(
            taken,
            Some((
                "shard-1".to_string(),
                "tasks/abc/output.zip".to_string(),
                "sha256hash".to_string(),
            ))
        );

        // State is cleared after take.
        assert_eq!(pending.status(), "IDLE");
        assert!(pending.current_shard_id().is_none());
        assert!(pending.take().is_none());
        assert!(pending.try_take_assignment("shard-2").is_ok());
    }

    #[tokio::test]
    async fn pending_result_take_returns_none_when_idle() {
        let mut pending = PendingResultState::default();
        assert!(pending.take().is_none());
    }
}

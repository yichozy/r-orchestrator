#[derive(Default, Clone, Debug)]
pub struct PendingResultState {
    shard_id: Option<String>,
    output_oss_key: String,
    sha256: String,
}

impl PendingResultState {
    pub fn set_ready(
        &mut self,
        shard_id: String,
        output_oss_key: String,
        sha256: String,
    ) -> Result<(), String> {
        if self.shard_id.is_some() {
            return Err("pending result already exists".to_string());
        }
        self.shard_id = Some(shard_id);
        self.output_oss_key = output_oss_key;
        self.sha256 = sha256;
        Ok(())
    }

    /// Returns the heartbeat status to report to the server.
    /// While a result is pending, the shard is still RUNNING from the server's
    /// perspective (ShardResultReady has not been acknowledged yet).
    pub fn status(&self) -> &'static str {
        if self.shard_id.is_some() {
            "RUNNING"
        } else {
            "IDLE"
        }
    }

    pub fn current_shard_id(&self) -> Option<&str> {
        self.shard_id.as_deref()
    }

    /// Extracts the pending result data and clears internal state.
    /// Returns None if no result is pending. Used during reconnection to
    /// re-send ShardResultReady so the server can record the already-uploaded output.
    pub fn take(&mut self) -> Option<(String, String, String)> {
        let shard_id = self.shard_id.take()?;
        let oss_key = std::mem::take(&mut self.output_oss_key);
        let sha = std::mem::take(&mut self.sha256);
        Some((shard_id, oss_key, sha))
    }

    pub fn try_take_assignment(&self, shard_id: &str) -> Result<(), String> {
        if let Some(current) = self.current_shard_id() {
            return Err(format!(
                "cannot assign shard {shard_id} while pending result exists for shard {current}"
            ));
        }
        Ok(())
    }

    pub fn mark_stored(&mut self, shard_id: &str) -> Result<(), String> {
        if self.shard_id.as_deref() != Some(shard_id) {
            return Err("stored ack shard mismatch".to_string());
        }
        self.shard_id = None;
        self.output_oss_key.clear();
        self.sha256.clear();
        Ok(())
    }
}

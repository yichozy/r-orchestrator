#[derive(Default, Clone, Debug)]
pub struct PendingResultState {
    shard_id: Option<String>,
}

impl PendingResultState {
    pub fn set_ready(&mut self, shard_id: String) -> Result<(), String> {
        if self.shard_id.is_some() {
            return Err("pending result already exists".to_string());
        }
        self.shard_id = Some(shard_id);
        Ok(())
    }

    pub fn status(&self) -> &'static str {
        if self.shard_id.is_some() {
            "RESULT_READY"
        } else {
            "IDLE"
        }
    }

    pub fn current_shard_id(&self) -> Option<&str> {
        self.shard_id.as_deref()
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
        Ok(())
    }
}

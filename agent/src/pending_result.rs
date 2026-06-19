use crate::control_client::controlv1;

#[derive(Default, Clone, Debug)]
pub struct PendingResultState {
    shard_id: Option<String>,
    output_csv: Option<Vec<u8>>,
}

impl PendingResultState {
    pub fn set_ready(&mut self, shard_id: String, output_csv: Vec<u8>) -> Result<(), String> {
        if self.shard_id.is_some() {
            return Err("pending result already exists".to_string());
        }
        self.shard_id = Some(shard_id);
        self.output_csv = Some(output_csv);
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

    pub fn to_result_data(&self, shard_id: &str) -> Result<controlv1::ShardResultData, String> {
        if self.shard_id.as_deref() != Some(shard_id) {
            return Err("fetch shard mismatch for pending result".to_string());
        }
        let output_csv = self
            .output_csv
            .clone()
            .ok_or_else(|| "pending result bytes missing".to_string())?;
        Ok(controlv1::ShardResultData {
            shard_id: shard_id.to_string(),
            output_csv,
        })
    }

    pub fn mark_stored(&mut self, shard_id: &str) -> Result<(), String> {
        if self.shard_id.as_deref() != Some(shard_id) {
            return Err("stored ack shard mismatch".to_string());
        }
        self.shard_id = None;
        self.output_csv = None;
        Ok(())
    }
}

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

pub mod controlv1 {
    tonic::include_proto!("rorchestrator.control.v1");
}

async fn set_status_if_current_shard(
    status: &Arc<Mutex<(String, String)>>,
    expected_shard_id: &str,
    next_status: &str,
    next_shard_id: &str,
) {
    let mut current = status.lock().await;
    if current.1 == expected_shard_id {
        current.0 = next_status.to_string();
        current.1 = next_shard_id.to_string();
    }
}

#[derive(Clone)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

pub async fn connect(
    addr: &str,
) -> Result<controlv1::control_service_client::ControlServiceClient<Channel>, tonic::transport::Error>
{
    let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };
    controlv1::control_service_client::ControlServiceClient::connect(endpoint).await
}

pub async fn run_callback_loop(
    mut client: controlv1::control_service_client::ControlServiceClient<Channel>,
    agent_id: String,
    tenant_id: String,
    backend_name: String,
    token: String,
    heartbeat_interval_secs: u64,
    pending_result: Arc<Mutex<crate::pending_result::PendingResultState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (tx, rx) = mpsc::channel::<controlv1::AgentMessage>(32);
    tracing::info!(agent_id, tenant_id, backend_name, "registering agent");
    tx.send(controlv1::AgentMessage {
        payload: Some(controlv1::agent_message::Payload::Register(
            controlv1::Register {
                agent_id: agent_id.clone(),
                tenant_id,
                backend_name,
                token: token.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        )),
    })
    .await?;

    let outbound = ReceiverStream::new(rx);
    let response = client.open_control_stream(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();

    let initial_status = {
        let pending = pending_result.lock().await;
        (
            pending.status().to_string(),
            pending.current_shard_id().unwrap_or_default().to_string(),
        )
    };
    let status: Arc<Mutex<(String, String)>> = Arc::new(Mutex::new(initial_status.clone()));
    let cancel_tokens: Arc<Mutex<HashMap<String, CancelToken>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
        heartbeat_interval_secs.max(1),
    ));

    // On reconnect, if there's a pending result (output uploaded but not yet
    // acknowledged), re-send ShardResultReady so the server can record it.
    // Clear the pending state so the agent can accept new assignments — the
    // server resets reconnecting agents to IDLE regardless.
    {
        let mut pending = pending_result.lock().await;
        if let Some((shard_id, output_oss_key, sha256)) = pending.take() {
            tracing::info!(shard_id = %shard_id, "re-sending pending ShardResultReady on reconnect");
            tx.send(controlv1::AgentMessage {
                payload: Some(controlv1::agent_message::Payload::ShardResultReady(
                    controlv1::ShardResultReady {
                        shard_id,
                        output_oss_key,
                        sha256,
                    },
                )),
            })
            .await?;
        }
    }

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let (ref s, ref sid) = *status.lock().await;
                tracing::debug!(status = s.as_str(), current_shard_id = sid.as_str(), "sending heartbeat");
                if tx.send(controlv1::AgentMessage {
                    payload: Some(controlv1::agent_message::Payload::Heartbeat(controlv1::Heartbeat {
                        agent_id: agent_id.clone(),
                        status: s.clone(),
                        current_shard_id: sid.clone(),
                    })),
                }).await.is_err() {
                    return Ok(());
                }
                tracing::debug!("heartbeat sent");
            }
            msg = inbound.message() => {
                tracing::debug!("checking inbound message from server");
                let msg = msg?;
                let Some(message) = msg else {
                    tracing::info!("server closed stream (end of messages)");
                    return Ok(());
                };

                if let Some(payload) = message.payload {
                    match payload {
                        controlv1::server_message::Payload::AssignShard(assign) => {
                            {
                                let pending = pending_result.lock().await;
                                pending.try_take_assignment(&assign.shard_id)?;
                            }
                            tracing::info!(shard_id = %assign.shard_id, task_id = %assign.task_id, script_name = %assign.script_name, "shard assigned");
                            {
                                let mut s = status.lock().await;
                                if !s.1.is_empty() {
                                    return Err(format!(
                                        "received shard assignment {} while already processing {}",
                                        assign.shard_id, s.1
                                    )
                                    .into());
                                }
                                s.0 = "RUNNING".to_string();
                                s.1 = assign.shard_id.clone();
                            }

                            let cancel_token = CancelToken::new();
                            {
                                let mut tokens = cancel_tokens.lock().await;
                                tokens.insert(assign.shard_id.clone(), cancel_token.clone());
                            }

                            tx.send(controlv1::AgentMessage {
                                payload: Some(controlv1::agent_message::Payload::ShardAccepted(
                                    controlv1::ShardAccepted {
                                        shard_id: assign.shard_id.clone(),
                                    },
                                )),
                            })
                            .await?;

                            let exec_tx = tx.clone();
                            let exec_shard_id = assign.shard_id.clone();
                            let exec_script_name = assign.script_name.clone();
                            let exec_bundle_url = assign.bundle_download_url.clone();
                            let exec_output_url = assign.output_upload_url.clone();
                            let exec_output_key = assign.output_oss_key.clone();
                            let exec_status = status.clone();
                            let exec_pending_result = pending_result.clone();
                            let exec_cancel_tokens = cancel_tokens.clone();

                            // Send ShardStarted immediately so the shard
                            // transitions to RUNNING before we attempt execution.
                            let _ = tx
                                .send(controlv1::AgentMessage {
                                    payload: Some(controlv1::agent_message::Payload::ShardStarted(
                                        controlv1::ShardStarted {
                                            shard_id: assign.shard_id.clone(),
                                        },
                                    )),
                                })
                                .await;

                            tokio::spawn(async move {
                                let result = crate::executor::execute_shard(
                                    &exec_bundle_url,
                                    &exec_output_url,
                                    &exec_output_key,
                                    &exec_shard_id,
                                    &exec_script_name,
                                    &cancel_token,
                                )
                                .await;

                                {
                                    let mut tokens = exec_cancel_tokens.lock().await;
                                    tokens.remove(&exec_shard_id);
                                }

                                match result {
                                    Err(err) => {
                                        tracing::error!(shard_id = %exec_shard_id, error = %err, "shard execution failed");
                                        let _ = exec_tx
                                            .send(controlv1::AgentMessage {
                                                payload: Some(
                                                    controlv1::agent_message::Payload::ShardFailed(
                                                        controlv1::ShardFailed {
                                                            shard_id: exec_shard_id.clone(),
                                                            error_message: format!("executor error: {err}"),
                                                        },
                                                    ),
                                                ),
                                            })
                                            .await;
                                        set_status_if_current_shard(
                                            &exec_status,
                                            &exec_shard_id,
                                            "IDLE",
                                            "",
                                        )
                                        .await;
                                    }
                                    Ok(crate::executor::ExecutionOutcome::Cancelled) => {
                                            tracing::info!(shard_id = %exec_shard_id, "shard cancelled");
                                            let _ = exec_tx
                                                .send(controlv1::AgentMessage {
                                                    payload: Some(
                                                        controlv1::agent_message::Payload::ShardFailed(
                                                            controlv1::ShardFailed {
                                                                shard_id: exec_shard_id.clone(),
                                                                error_message: "cancelled".to_string(),
                                                            },
                                                        ),
                                                    ),
                                                })
                                                .await;
                                        set_status_if_current_shard(
                                            &exec_status,
                                            &exec_shard_id,
                                            "IDLE",
                                            "",
                                        )
                                        .await;
                                    }
                                    Ok(crate::executor::ExecutionOutcome::ResultReady { shard_id, output_oss_key, sha256 }) => {
                                        let set_ready_result = {
                                            let mut pending = exec_pending_result.lock().await;
                                            pending.set_ready(shard_id.clone(), output_oss_key.clone(), sha256.clone())
                                        };
                                        if let Err(err) = set_ready_result {
                                            tracing::error!(shard_id = %exec_shard_id, error = %err, "failed to store pending result");
                                            let _ = exec_tx
                                                .send(controlv1::AgentMessage {
                                                    payload: Some(
                                                        controlv1::agent_message::Payload::ShardFailed(
                                                            controlv1::ShardFailed {
                                                                shard_id: exec_shard_id.clone(),
                                                                error_message: format!("pending result error: {err}"),
                                                            },
                                                        ),
                                                    ),
                                                })
                                                .await;
                                            set_status_if_current_shard(
                                                &exec_status,
                                                &exec_shard_id,
                                                "IDLE",
                                                "",
                                            )
                                            .await;
                                            return;
                                        }

                                        // Keep status as RUNNING — the shard is still
                                        // RUNNING from the server's perspective until
                                        // ShardResultReady is processed and acknowledged.
                                        let _ = exec_tx
                                            .send(controlv1::AgentMessage {
                                                payload: Some(
                                                    controlv1::agent_message::Payload::ShardResultReady(
                                                        controlv1::ShardResultReady {
                                                            shard_id,
                                                            output_oss_key,
                                                            sha256,
                                                        },
                                                    ),
                                                ),
                                            })
                                            .await;
                                    }
                                }
                            });
                        }
                        controlv1::server_message::Payload::CancelShard(cancel) => {
                            tracing::info!(shard_id = %cancel.shard_id, "received cancel shard");
                            let tokens = cancel_tokens.lock().await;
                            if let Some(token) = tokens.get(&cancel.shard_id) {
                                token.cancel();
                            }
                        }
                        controlv1::server_message::Payload::Drain(_) => {
                            tracing::info!("received drain signal from server");
                            return Ok(());
                        }
                        controlv1::server_message::Payload::Shutdown(_) => {
                            tracing::info!("received shutdown signal from server");
                            return Ok(());
                        }
                        controlv1::server_message::Payload::ShardResultStored(stored) => {
                            tracing::info!(shard_id = %stored.shard_id, "server acknowledged stored shard result");
                            {
                                let mut pending = pending_result.lock().await;
                                if let Err(e) = pending.mark_stored(&stored.shard_id) {
                                    // Pending result may have been cleared on reconnect
                                    // after re-sending ShardResultReady. The ack is stale —
                                    // safe to ignore since the result was already processed.
                                    tracing::debug!(shard_id = %stored.shard_id, error = %e, "ShardResultStored with no pending result, ignoring");
                                }
                            }
                            set_status_if_current_shard(&status, &stored.shard_id, "IDLE", "").await;
                        }
                    }
                }
            }
        }
    }
}

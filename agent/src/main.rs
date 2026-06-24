mod config;
mod control_client;
#[cfg(test)]
mod control_client_result_ready_tests;
mod executor;
mod oss;
mod pending_result;
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .init();
}

async fn health_server(port: u16) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(port, error = %e, "failed to bind health port");
            return;
        }
    };
    tracing::info!(%addr, "health server listening");
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let _ = tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let _ = stream.shutdown().await;
                });
            }
            Err(e) => tracing::warn!(error = %e, "health accept error"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let cfg = config::Config::from_env();
    if cfg.agent_token.is_empty() {
        return Err("RORCHESTRATOR_AGENT_TOKEN (or AGENT_TOKEN) is required".into());
    }
    if cfg.tenant_id.is_empty() {
        return Err("RORCHESTRATOR_TENANT_ID is required".into());
    }

    tracing::info!(
        server_addr = %cfg.server_grpc_addr,
        tenant_id = %cfg.tenant_id,
        agent_id = %cfg.agent_id,
        "agent starting"
    );

    // Start health check TCP server in background
    tokio::spawn(health_server(cfg.health_port));

    // Start periodic cache cleanup in background
    tokio::spawn(executor::cleanup_cache_loop());

    let mut backoff_secs = cfg.reconnect_initial_backoff_secs.max(1);
    let max_backoff_secs = cfg
        .reconnect_max_backoff_secs
        .max(cfg.reconnect_initial_backoff_secs.max(1));
    let pending_result = Arc::new(Mutex::new(pending_result::PendingResultState::default()));

    loop {
        tracing::info!(
            server_addr = %cfg.server_grpc_addr,
            backoff_secs,
            "connecting to server"
        );
        match control_client::connect(&cfg.server_grpc_addr).await {
            Ok(client) => {
                tracing::info!(server_addr = %cfg.server_grpc_addr, "connected to server");

                backoff_secs = cfg.reconnect_initial_backoff_secs.max(1);
                let result = control_client::run_callback_loop(
                    client,
                    cfg.agent_id.clone(),
                    cfg.tenant_id.clone(),
                    cfg.backend_name.clone(),
                    cfg.agent_token.clone(),
                    cfg.heartbeat_interval_secs,
                    pending_result.clone(),
                )
                .await;
                if let Err(err) = result {
                    tracing::warn!(error = %err, "disconnected from server");
                } else {
                    tracing::info!("stream closed by server, reconnecting");
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "connect to server failed");
            }
        }

        tracing::info!(backoff_secs, "retrying connection");
        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs.saturating_mul(2)).min(max_backoff_secs);
    }
}

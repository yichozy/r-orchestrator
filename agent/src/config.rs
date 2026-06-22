pub struct Config {
    pub server_grpc_addr: String,
    pub agent_token: String,
    pub tenant_id: String,
    pub backend_name: String,
    pub agent_id: String,
    pub heartbeat_interval_secs: u64,
    pub reconnect_initial_backoff_secs: u64,
    pub reconnect_max_backoff_secs: u64,
    pub health_port: u16,
}

impl Config {
    pub fn from_env() -> Self {
        let server_grpc_addr = std::env::var("RORCHESTRATOR_SERVER_GRPC_ADDR")
            .or_else(|_| std::env::var("SERVER_ADDR"))
            .unwrap_or_else(|_| "127.0.0.1:9090".to_string());

        let agent_token = std::env::var("RORCHESTRATOR_AGENT_TOKEN")
            .or_else(|_| std::env::var("AGENT_TOKEN"))
            .unwrap_or_default();
        let tenant_id = std::env::var("RORCHESTRATOR_TENANT_ID").unwrap_or_default();
        let backend_name =
            std::env::var("RORCHESTRATOR_BACKEND_NAME").unwrap_or_else(|_| "skypilot".to_string());

        let agent_id = std::env::var("HOSTNAME").unwrap_or_else(|_| "agent".to_string());

        let heartbeat_interval_secs = std::env::var("RORCHESTRATOR_HEARTBEAT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(3);
        let reconnect_initial_backoff_secs =
            std::env::var("RORCHESTRATOR_RECONNECT_INITIAL_BACKOFF_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(1);
        let reconnect_max_backoff_secs = std::env::var("RORCHESTRATOR_RECONNECT_MAX_BACKOFF_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(30);

        let health_port: u16 = std::env::var("RORCHESTRATOR_HEALTH_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(9091);

        Self {
            server_grpc_addr,
            agent_token,
            tenant_id,
            backend_name,
            agent_id,
            heartbeat_interval_secs,
            reconnect_initial_backoff_secs,
            reconnect_max_backoff_secs,
            health_port,
        }
    }
}

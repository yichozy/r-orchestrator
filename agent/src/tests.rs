#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn config_reads_rorchestrator_env_vars() {
        let _guard = ENV_LOCK.lock().unwrap();

        let prev_server = std::env::var("RORCHESTRATOR_SERVER_GRPC_ADDR").ok();
        let prev_token = std::env::var("RORCHESTRATOR_AGENT_TOKEN").ok();
        let prev_tenant = std::env::var("RORCHESTRATOR_TENANT_ID").ok();
        let prev_backend = std::env::var("RORCHESTRATOR_BACKEND_NAME").ok();
        let prev_hb = std::env::var("RORCHESTRATOR_HEARTBEAT_INTERVAL_SECS").ok();
        let prev_reconnect_initial =
            std::env::var("RORCHESTRATOR_RECONNECT_INITIAL_BACKOFF_SECS").ok();
        let prev_reconnect_max = std::env::var("RORCHESTRATOR_RECONNECT_MAX_BACKOFF_SECS").ok();

        unsafe {
            std::env::set_var("RORCHESTRATOR_SERVER_GRPC_ADDR", "server:9090");
            std::env::set_var("RORCHESTRATOR_AGENT_TOKEN", "token");
            std::env::set_var("RORCHESTRATOR_TENANT_ID", "tenant-a");
            std::env::set_var("RORCHESTRATOR_BACKEND_NAME", "skypilot");
            std::env::set_var("RORCHESTRATOR_HEARTBEAT_INTERVAL_SECS", "5");
            std::env::set_var("RORCHESTRATOR_RECONNECT_INITIAL_BACKOFF_SECS", "2");
            std::env::set_var("RORCHESTRATOR_RECONNECT_MAX_BACKOFF_SECS", "10");
        }

        let cfg = crate::config::Config::from_env();
        assert_eq!(cfg.server_grpc_addr, "server:9090");
        assert_eq!(cfg.agent_token, "token");
        assert_eq!(cfg.tenant_id, "tenant-a");
        assert_eq!(cfg.backend_name, "skypilot");
        assert_eq!(cfg.heartbeat_interval_secs, 5);
        assert_eq!(cfg.reconnect_initial_backoff_secs, 2);
        assert_eq!(cfg.reconnect_max_backoff_secs, 10);

        unsafe {
            if let Some(v) = prev_server {
                std::env::set_var("RORCHESTRATOR_SERVER_GRPC_ADDR", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_SERVER_GRPC_ADDR");
            }
            if let Some(v) = prev_token {
                std::env::set_var("RORCHESTRATOR_AGENT_TOKEN", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_AGENT_TOKEN");
            }
            if let Some(v) = prev_tenant {
                std::env::set_var("RORCHESTRATOR_TENANT_ID", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_TENANT_ID");
            }
            if let Some(v) = prev_backend {
                std::env::set_var("RORCHESTRATOR_BACKEND_NAME", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_BACKEND_NAME");
            }
            if let Some(v) = prev_hb {
                std::env::set_var("RORCHESTRATOR_HEARTBEAT_INTERVAL_SECS", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_HEARTBEAT_INTERVAL_SECS");
            }
            if let Some(v) = prev_reconnect_initial {
                std::env::set_var("RORCHESTRATOR_RECONNECT_INITIAL_BACKOFF_SECS", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_RECONNECT_INITIAL_BACKOFF_SECS");
            }
            if let Some(v) = prev_reconnect_max {
                std::env::set_var("RORCHESTRATOR_RECONNECT_MAX_BACKOFF_SECS", v);
            } else {
                std::env::remove_var("RORCHESTRATOR_RECONNECT_MAX_BACKOFF_SECS");
            }
        }
    }
}

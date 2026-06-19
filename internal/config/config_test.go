package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("SERVER_HTTP_ADDR", "")
	t.Setenv("SERVER_GRPC_ADDR", "")
	t.Setenv("AGENT_TOKEN", "token-1")
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "tester")
	t.Setenv("DB_PASSWORD", "secret")
	t.Setenv("DB_NAME", "scheduler")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.Server.HTTPAddr != ":8089" {
		t.Fatalf("expected default http addr :8089, got %q", cfg.Server.HTTPAddr)
	}
	if cfg.Server.GRPCAddr != ":9090" {
		t.Fatalf("expected default grpc addr :9090, got %q", cfg.Server.GRPCAddr)
	}
	if cfg.K8s.AgentToken != "token-1" {
		t.Fatalf("expected agent token token-1, got %q", cfg.K8s.AgentToken)
	}
	if cfg.Database.URL != "postgres://tester:secret@localhost:5432/scheduler" {
		t.Fatalf("expected database url built from postgres envs, got %q", cfg.Database.URL)
	}
	if cfg.Server.GRPCPublicAddr != "" {
		t.Fatalf("expected empty server grpc public addr by default, got %q", cfg.Server.GRPCPublicAddr)
	}
	if cfg.K8s.AgentImage != "r-orchestrator/agent:latest" {
		t.Fatalf("expected default agent image, got %q", cfg.K8s.AgentImage)
	}
	if cfg.K8s.Namespace != "r-agents" {
		t.Fatalf("expected default agent namespace r-agents, got %q", cfg.K8s.Namespace)
	}
	if cfg.K8s.BillingCycleSeconds != 3600 {
		t.Fatalf("expected default billing cycle 3600, got %d", cfg.K8s.BillingCycleSeconds)
	}
	if cfg.K8s.AgentLogLevel != "info" {
		t.Fatalf("expected default agent log level info, got %q", cfg.K8s.AgentLogLevel)
	}
	if cfg.K8s.AgentParallelism != "1" {
		t.Fatalf("expected default agent parallelism 1, got %q", cfg.K8s.AgentParallelism)
	}
	if cfg.Server.LogLevel != "info" {
		t.Fatalf("expected default server log level info, got %q", cfg.Server.LogLevel)
	}
}

func TestLoadFromEnvRequiresPostgresSettings(t *testing.T) {
	t.Setenv("SERVER_HTTP_ADDR", "")
	t.Setenv("SERVER_GRPC_ADDR", "")
	t.Setenv("AGENT_TOKEN", "token-1")
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "tester")
	t.Setenv("DB_PASSWORD", "secret")
	t.Setenv("DB_NAME", "")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatalf("expected missing postgres setting to fail")
	}
}

func TestLoadFromEnvRequiresAgentToken(t *testing.T) {
	t.Setenv("SERVER_HTTP_ADDR", "")
	t.Setenv("SERVER_GRPC_ADDR", "")
	t.Setenv("AGENT_TOKEN", "")
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "tester")
	t.Setenv("DB_PASSWORD", "secret")
	t.Setenv("DB_NAME", "scheduler")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatalf("expected missing agent token to fail")
	}
}

func TestLoadFromEnvLoadsAgentSettings(t *testing.T) {
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "postgres")
	t.Setenv("DB_PASSWORD", "secret")
	t.Setenv("DB_NAME", "r_scheduler")
	t.Setenv("AGENT_TOKEN", "token-1")
	t.Setenv("AGENT_IMAGE", "ghcr.io/example/agent:0.2.0")
	t.Setenv("AGENT_NAMESPACE", "r-scheduler")
	t.Setenv("KUBECONFIG_PATH", "/path/to/kubeconfig")
	t.Setenv("SERVER_GRPC_PUBLIC_ADDR", "server.default.svc.cluster.local:9090")
	t.Setenv("CLUSTER_BILLING_CYCLE_SECONDS", "7200")
	t.Setenv("AGENT_LOG_LEVEL", "debug")
	t.Setenv("AGENT_PARALLELISM", "4")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.K8s.AgentImage != "ghcr.io/example/agent:0.2.0" {
		t.Fatalf("unexpected agent image: %q", cfg.K8s.AgentImage)
	}
	if cfg.K8s.Namespace != "r-scheduler" {
		t.Fatalf("unexpected agent namespace: %q", cfg.K8s.Namespace)
	}
	if cfg.K8s.KubeConfigPath != "/path/to/kubeconfig" {
		t.Fatalf("unexpected kubeconfig path: %q", cfg.K8s.KubeConfigPath)
	}
	if cfg.Server.GRPCPublicAddr != "server.default.svc.cluster.local:9090" {
		t.Fatalf("unexpected server grpc public addr: %q", cfg.Server.GRPCPublicAddr)
	}
	if cfg.K8s.BillingCycleSeconds != 7200 {
		t.Fatalf("unexpected billing cycle: %d", cfg.K8s.BillingCycleSeconds)
	}
	if cfg.K8s.AgentLogLevel != "debug" {
		t.Fatalf("unexpected agent log level: %q", cfg.K8s.AgentLogLevel)
	}
	if cfg.K8s.AgentParallelism != "4" {
		t.Fatalf("unexpected agent parallelism: %q", cfg.K8s.AgentParallelism)
	}
	if cfg.Server.LogLevel != "debug" {
		t.Fatalf("unexpected server log level: %q", cfg.Server.LogLevel)
	}
}

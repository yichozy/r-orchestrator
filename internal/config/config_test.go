package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("SERVER_HTTP_ADDR", "")
	t.Setenv("SERVER_GRPC_ADDR", "")
	t.Setenv("CLUSTER_AGENT_TOKEN", "token-1")
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
	if cfg.Cluster.AgentToken != "token-1" {
		t.Fatalf("expected agent token token-1, got %q", cfg.Cluster.AgentToken)
	}
	if cfg.Database.URL != "postgres://tester:secret@localhost:5432/scheduler" {
		t.Fatalf("expected database url built from postgres envs, got %q", cfg.Database.URL)
	}
	if cfg.Server.GRPCPublicAddr != "" {
		t.Fatalf("expected empty server grpc public addr by default, got %q", cfg.Server.GRPCPublicAddr)
	}
	if cfg.Cluster.AgentImage != "r-orchestrator/agent:latest" {
		t.Fatalf("expected default agent image, got %q", cfg.Cluster.AgentImage)
	}
	if cfg.Cluster.Kubernetes.Namespace != "r-agents" {
		t.Fatalf("expected default agent namespace r-agents, got %q", cfg.Cluster.Kubernetes.Namespace)
	}
	if cfg.Cluster.BillingCycleSeconds != 3600 {
		t.Fatalf("expected default billing cycle 3600, got %d", cfg.Cluster.BillingCycleSeconds)
	}
	if cfg.Cluster.AgentLogLevel != "info" {
		t.Fatalf("expected default agent log level info, got %q", cfg.Cluster.AgentLogLevel)
	}
	if cfg.Server.LogLevel != "info" {
		t.Fatalf("expected default server log level info, got %q", cfg.Server.LogLevel)
	}
}

func TestLoadFromEnvRequiresPostgresSettings(t *testing.T) {
	t.Setenv("SERVER_HTTP_ADDR", "")
	t.Setenv("SERVER_GRPC_ADDR", "")
	t.Setenv("CLUSTER_AGENT_TOKEN", "token-1")
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
	t.Setenv("CLUSTER_AGENT_TOKEN", "")
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
	t.Setenv("CLUSTER_AGENT_TOKEN", "token-1")
	t.Setenv("CLUSTER_AGENT_IMAGE", "ghcr.io/example/agent:0.2.0")
	t.Setenv("CLUSTER_KUBERNETES_NAMESPACE", "r-scheduler")
	t.Setenv("CLUSTER_KUBERNETES_KUBECONFIG_PATH", "/path/to/kubeconfig")
	t.Setenv("SERVER_GRPC_PUBLIC_ADDR", "server.default.svc.cluster.local:9090")
	t.Setenv("CLUSTER_BILLING_CYCLE_SECONDS", "7200")
	t.Setenv("CLUSTER_AGENT_LOG_LEVEL", "debug")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.AgentImage != "ghcr.io/example/agent:0.2.0" {
		t.Fatalf("unexpected agent image: %q", cfg.Cluster.AgentImage)
	}
	if cfg.Cluster.Kubernetes.Namespace != "r-scheduler" {
		t.Fatalf("unexpected agent namespace: %q", cfg.Cluster.Kubernetes.Namespace)
	}
	if cfg.Cluster.Kubernetes.KubeConfigPath != "/path/to/kubeconfig" {
		t.Fatalf("unexpected kubeconfig path: %q", cfg.Cluster.Kubernetes.KubeConfigPath)
	}
	if cfg.Server.GRPCPublicAddr != "server.default.svc.cluster.local:9090" {
		t.Fatalf("unexpected server grpc public addr: %q", cfg.Server.GRPCPublicAddr)
	}
	if cfg.Cluster.BillingCycleSeconds != 7200 {
		t.Fatalf("unexpected billing cycle: %d", cfg.Cluster.BillingCycleSeconds)
	}
	if cfg.Cluster.AgentLogLevel != "debug" {
		t.Fatalf("unexpected agent log level: %q", cfg.Cluster.AgentLogLevel)
	}
	if cfg.Server.LogLevel != "debug" {
		t.Fatalf("unexpected server log level: %q", cfg.Server.LogLevel)
	}
}

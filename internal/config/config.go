package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"
)

var (
	GlobalConfig            Config
	globalConfigMu          sync.RWMutex
	globalConfigInitialized bool
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Cluster  ClusterConfig
}

type ServerConfig struct {
	HTTPAddr       string
	GRPCAddr       string
	GRPCPublicAddr string
	PublicURL      string
	LogLevel       string
}

type DatabaseConfig struct {
	Host     string
	Port     string
	DBName   string
	Username string
	Password string
	URL      string
}

type ClusterConfig struct {
	BillingCycleSeconds   int
	BillingAdvanceSeconds int
	IdleThresholdSeconds  int
	AgentToken            string
	AgentImage            string
	AgentLogLevel         string
	AgentHeartbeatTimeout time.Duration
	AgentDisconnectGrace  time.Duration
	Kubernetes            KubernetesBackendConfig
	OSS                   OSSConfig
}

type OSSConfig struct {
	Endpoint     string
	Bucket       string
	AccessKey    string
	AccessSecret string
}

type KubernetesBackendConfig struct {
	Namespace        string
	ImagePullSecrets []string
	KubeConfigPath   string
}

func LoadFromEnv() (Config, error) {
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	if dbHost == "" {
		return Config{}, fmt.Errorf("DB_HOST is required")
	}
	if dbPort == "" {
		return Config{}, fmt.Errorf("DB_PORT is required")
	}
	if dbUser == "" {
		return Config{}, fmt.Errorf("DB_USER is required")
	}
	if dbPass == "" {
		return Config{}, fmt.Errorf("DB_PASSWORD is required")
	}
	if dbName == "" {
		return Config{}, fmt.Errorf("DB_NAME is required")
	}

	dbURL := (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(dbUser, dbPass),
		Host:   fmt.Sprintf("%s:%s", dbHost, dbPort),
		Path:   dbName,
	}).String()

	agentToken := os.Getenv("CLUSTER_AGENT_TOKEN")
	if agentToken == "" {
		return Config{}, fmt.Errorf("CLUSTER_AGENT_TOKEN is required")
	}

	billingCycleSeconds := envOrInt("CLUSTER_BILLING_CYCLE_SECONDS", 3600)
	if billingCycleSeconds <= 0 {
		billingCycleSeconds = 3600
	}
	billingAdvanceSeconds := envOrInt("CLUSTER_BILLING_ADVANCE_SECONDS", 180)
	if billingAdvanceSeconds <= 0 {
		billingAdvanceSeconds = 180
	}

	cfg := Config{
		Server: ServerConfig{
			HTTPAddr:       envOr("SERVER_HTTP_ADDR", ":8089"),
			GRPCAddr:       envOr("SERVER_GRPC_ADDR", ":9090"),
			GRPCPublicAddr: os.Getenv("SERVER_GRPC_PUBLIC_ADDR"),
			PublicURL:      os.Getenv("SERVER_PUBLIC_URL"),
			LogLevel:       envOr("LOG_LEVEL", "info"),
		},
		Database: DatabaseConfig{
			Host:     dbHost,
			Port:     dbPort,
			DBName:   dbName,
			Username: dbUser,
			Password: dbPass,
			URL:      dbURL,
		},
		Cluster: ClusterConfig{
			BillingCycleSeconds:   billingCycleSeconds,
			BillingAdvanceSeconds: billingAdvanceSeconds,
			IdleThresholdSeconds:  envOrInt("CLUSTER_IDLE_THRESHOLD_SECONDS", 600),
			AgentToken:            agentToken,
			AgentImage:            envOr("CLUSTER_AGENT_IMAGE", "r-orchestrator/agent:latest"),
			AgentLogLevel:         envOr("CLUSTER_AGENT_LOG_LEVEL", "info"),
			AgentHeartbeatTimeout: envOrDuration("CLUSTER_AGENT_HEARTBEAT_TIMEOUT", 90*time.Second),
			AgentDisconnectGrace:  envOrDuration("CLUSTER_AGENT_DISCONNECT_GRACE", 5*time.Minute),
			OSS: OSSConfig{
				Endpoint:     os.Getenv("ALIYUN_OSS_ENDPOINT"),
				Bucket:       os.Getenv("ALIYUN_OSS_BUCKET"),
				AccessKey:    os.Getenv("ALIYUN_OSS_ACCESS_KEY"),
				AccessSecret: os.Getenv("ALIYUN_OSS_ACCESS_SECRET"),
			},
			Kubernetes: KubernetesBackendConfig{
				Namespace:        envOr("CLUSTER_KUBERNETES_NAMESPACE", "r-agents"),
				ImagePullSecrets: parseImagePullSecrets(os.Getenv("CLUSTER_KUBERNETES_IMAGE_PULL_SECRETS")),
				KubeConfigPath:   os.Getenv("CLUSTER_KUBERNETES_KUBECONFIG_PATH"),
			},
		},
	}
	return cfg, nil
}

func InitGlobalConfig() error {
	globalConfigMu.Lock()
	defer globalConfigMu.Unlock()

	if globalConfigInitialized {
		return fmt.Errorf("global config already initialized")
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		return err
	}

	GlobalConfig = cfg
	globalConfigInitialized = true
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	s := os.Getenv(key)
	if s == "" {
		return fallback
	}
	v := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			v = v*10 + int(c-'0')
		} else {
			return fallback
		}
	}
	if v == 0 {
		return fallback
	}
	return v
}

func parseImagePullSecrets(raw string) []string {
	if raw == "" {
		return nil
	}
	var items []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil
	}
	secrets := make([]string, 0, len(items))
	for _, item := range items {
		if item.Name != "" {
			secrets = append(secrets, item.Name)
		}
	}
	return secrets
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
	if s := os.Getenv(key); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

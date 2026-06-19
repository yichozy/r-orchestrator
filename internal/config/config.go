package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
)

var (
	GlobalConfig            Config
	globalConfigMu          sync.RWMutex
	globalConfigInitialized bool
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	K8s      K8sConfig
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

type K8sConfig struct {
	AgentToken          string
	Namespace           string
	AgentImage          string
	ImagePullSecrets    []string
	KubeConfigPath      string
	AgentLogLevel       string
	AgentParallelism    string
	BillingCycleSeconds int
	BillingAdvanceSeconds int
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

	agentToken := os.Getenv("AGENT_TOKEN")
	if agentToken == "" {
		return Config{}, fmt.Errorf("AGENT_TOKEN is required")
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
		K8s: K8sConfig{
			AgentToken:          agentToken,
			Namespace:           envOr("AGENT_NAMESPACE", "r-agents"),
			AgentImage:          envOr("AGENT_IMAGE", "r-orchestrator/agent:latest"),
			ImagePullSecrets:    parseImagePullSecrets(os.Getenv("IMAGE_PULL_SECRET")),
			KubeConfigPath:      os.Getenv("KUBECONFIG_PATH"),
			AgentLogLevel:       envOr("AGENT_LOG_LEVEL", "info"),
			AgentParallelism:    envOr("AGENT_PARALLELISM", "1"),
			BillingCycleSeconds:   envOrInt("CLUSTER_BILLING_CYCLE_SECONDS", 3600),
			BillingAdvanceSeconds: envOrInt("CLUSTER_BILLING_ADVANCE_SECONDS", 180),
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

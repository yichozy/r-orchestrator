package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvVariableSkipsDotEnvInProd(t *testing.T) {
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, ".env"), []byte("CLUSTER_AGENT_TOKEN=from-dotenv\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
	t.Setenv("ENV", "prod")
	t.Setenv("CLUSTER_AGENT_TOKEN", "")

	if err := LoadEnvVariable(); err != nil {
		t.Fatalf("expected prod mode to skip .env loading, got %v", err)
	}
	if got := os.Getenv("CLUSTER_AGENT_TOKEN"); got != "" {
		t.Fatalf("expected prod mode to leave AGENT_TOKEN untouched, got %q", got)
	}
}

func TestLoadEnvVariableDoesNotRequireDotEnvWhenEnvironmentIsComplete(t *testing.T) {
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
	t.Setenv("ENV", "")
	t.Setenv("SERVER_HTTP_ADDR", "")
	t.Setenv("SERVER_GRPC_ADDR", "")
	t.Setenv("CLUSTER_AGENT_TOKEN", "token-1")
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "tester")
	t.Setenv("DB_PASSWORD", "secret")
	t.Setenv("DB_NAME", "scheduler")

	if err := LoadEnvVariable(); err != nil {
		t.Fatalf("expected missing .env to be ignored, got %v", err)
	}
	if _, err := LoadFromEnv(); err != nil {
		t.Fatalf("expected complete environment to load without .env, got %v", err)
	}
}

func TestLoadEnvVariablePrefersExplicitEnvironmentOverDotEnv(t *testing.T) {
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, ".env"), []byte("CLUSTER_AGENT_TOKEN=from-dotenv\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
	t.Setenv("ENV", "")
	t.Setenv("CLUSTER_AGENT_TOKEN", "from-env")

	if err := LoadEnvVariable(); err != nil {
		t.Fatalf("expected .env load to succeed, got %v", err)
	}
	if got := os.Getenv("CLUSTER_AGENT_TOKEN"); got != "from-env" {
		t.Fatalf("expected explicit environment variable to win, got %q", got)
	}
}

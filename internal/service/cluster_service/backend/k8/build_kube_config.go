package k8

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// BuildKubeConfig 返回 *rest.Config。
// 优先级：
//   1. 显式指定的 kubeConfigPath
//   2. 集群内配置（在 pod 中运行时）
//   3. 默认 kubeconfig (~/.kube/config)
func BuildKubeConfig(kubeConfigPath string) (*rest.Config, error) {
	if kubeConfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	}

	// 尝试集群内配置
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}

	// 回退到默认 kubeconfig
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("get home dir: %w", err)
	}

	defaultPath := filepath.Join(home, ".kube", "config")
	if _, err := os.Stat(defaultPath); err == nil {
		return clientcmd.BuildConfigFromFlags("", defaultPath)
	}

	return nil, fmt.Errorf("no kubeconfig found: set CLUSTER_K8S_KUBECONFIG_PATH or run in-cluster")
}

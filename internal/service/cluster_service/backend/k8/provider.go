package k8

import (
	"fmt"

	"github.com/google/uuid"
	"k8s.io/client-go/kubernetes"
)

const (
	labelApp    = "r-orchestrator-agent"
	labelTenant = "r-orchestrator-tenant"
)

// Config 保存 K8s provider 配置信息
type Config struct {
	Namespace        string
	AgentImage       string
	ImagePullSecrets []string
	ServerGRPCAddr   string
	AgentToken       string
	ServerPublicURL  string
	KubeConfigPath   string
	AgentLogLevel    string
	AgentParallelism string
}

// K8sProvider 通过 Kubernetes API 管理 agent 部署
type K8sProvider struct {
	client kubernetes.Interface
	cfg    Config
}

// NewK8sProvider 创建 K8s provider 实例
func NewK8sProvider(cfg Config) (*K8sProvider, error) {
	kubeConfig, err := BuildKubeConfig(cfg.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("build kube config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	return &K8sProvider{
		client: clientset,
		cfg:    cfg,
	}, nil
}

// SetClient 覆盖 kubernetes client，用于测试场景
func (p *K8sProvider) SetClient(client kubernetes.Interface) {
	p.client = client
}

// clusterName 根据租户 ID 生成集群名称
func clusterName(tenantID uuid.UUID) string {
	return fmt.Sprintf("r-orch-%s-agents", tenantID.String()[:8])
}

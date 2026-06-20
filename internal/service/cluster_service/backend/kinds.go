package backend

// BackendKind 定义已知的后端类型常量。
type BackendKind string

const (
	// BackendKindKubernetes Kubernetes 后端（StatefulSet 管理 agent Pod）
	BackendKindKubernetes BackendKind = "kubernetes"
)

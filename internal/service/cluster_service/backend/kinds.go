package backend

// BackendKind 定义已知的后端类型常量。
type BackendKind string

const (
	// BackendKindKubernetes Kubernetes 后端（StatefulSet 管理 agent Pod）
	BackendKindKubernetes BackendKind = "kubernetes"
)

// IsValid 判断字符串是否为已知后端类型。
func IsValid(kind string) bool {
	switch kind {
	case string(BackendKindKubernetes):
		return true
	default:
		return false
	}
}

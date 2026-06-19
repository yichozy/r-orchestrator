package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/yichozy/r-orchestrator/internal/model"
)

// Provider 定义集群资源提供者的接口。
type Provider interface {
	EnsureCluster(ctx context.Context, tenant model.Tenant) error
	ScaleCluster(ctx context.Context, tenant model.Tenant, replicas int) error
	DestroyCluster(ctx context.Context, tenant model.Tenant) error
}

// Registry 按后端名称路由到对应的 Provider 实现。
type Registry struct {
	mu        sync.Mutex
	providers map[string]Provider
}

// NewRegistry 创建空的 Provider 注册表。
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register 注册一个后端的 Provider 实现。
// 如果同名后端已存在，会覆盖旧的实现。
func (r *Registry) Register(kind string, p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[kind] = p
}

// Get 按 backendName 返回对应的 Provider 实现。
// 如果未注册该后端，返回错误。
func (r *Registry) Get(backendName string) (Provider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.providers[backendName]
	if !ok {
		return nil, fmt.Errorf("no provider registered for backend %q", backendName)
	}
	return p, nil
}

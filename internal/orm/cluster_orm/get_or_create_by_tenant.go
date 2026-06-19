package cluster_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// GetOrCreateByTenant 按 tenant_id 查询最新的非 TERMINATED Cluster 记录，
// 若不存在则创建一条新记录。
func GetOrCreateByTenant(ctx context.Context, db *gorm.DB, cluster model.Cluster) (model.Cluster, error) {
	existing, err := GetByTenant(ctx, db, cluster.TenantID)
	if err == nil {
		return existing, nil
	}

	return Create(ctx, db, cluster)
}

package cluster_orm

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// GetByTenant 按 tenant_id 查询最新的非 TERMINATED Cluster 记录。
func GetByTenant(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) (model.Cluster, error) {
	var cluster model.Cluster
	err := db.WithContext(ctx).
		Where("tenant_id = ? AND status <> ?", tenantID, model.ClusterStatusTerminated).
		Order("created_at DESC").
		First(&cluster).Error

	if err != nil {
		return model.Cluster{}, err
	}
	return cluster, nil
}

package cluster_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// GetActiveClusterList 列出所有 ACTIVE 和 PROVISIONING 状态的 Cluster。
func GetActiveClusterList(ctx context.Context, db *gorm.DB) ([]model.Cluster, error) {
	var clusters []model.Cluster
	err := db.WithContext(ctx).
		Where("status IN ?", []model.ClusterStatus{model.ClusterStatusActive, model.ClusterStatusProvisioning}).
		Find(&clusters).Error

	if err != nil {
		return nil, err
	}
	return clusters, nil
}

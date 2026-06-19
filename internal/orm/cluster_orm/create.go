package cluster_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// Create 在数据库中创建一条 Cluster 记录。
func Create(ctx context.Context, db *gorm.DB, cluster model.Cluster) (model.Cluster, error) {
	if err := db.WithContext(ctx).Create(&cluster).Error; err != nil {
		return model.Cluster{}, err
	}
	return cluster, nil
}

package cluster_orm

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// UpdateStatus 更新 Cluster 状态。
func UpdateStatus(ctx context.Context, db *gorm.DB, id uuid.UUID, status string) error {
	return db.WithContext(ctx).
		Model(&model.Cluster{}).
		Where("id = ?", id).
		Update("status", status).Error
}

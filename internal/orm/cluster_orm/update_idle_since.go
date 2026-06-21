package cluster_orm

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// UpdateIdleSince 更新 Cluster 的空闲起始时间。
func UpdateIdleSince(ctx context.Context, db *gorm.DB, clusterID uuid.UUID, idleSince *time.Time) error {
	return db.WithContext(ctx).
		Model(&model.Cluster{}).
		Where("id = ?", clusterID).
		Update("idle_since", idleSince).Error
}
